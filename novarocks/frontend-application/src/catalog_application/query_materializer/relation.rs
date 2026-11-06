// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Ordinary relation facts and their original statement memo. Provider I/O
//! runs only on the caller's existing serialized capture path.

use super::*;
use crate::catalog_application::query_bindings::{MemoizedQueryRelation, QueryRelationRequestKey};
use crate::catalog_application::query_catalog::relation::ConnectorQueryRelationMaterialization;
use novarocks_sql::compiler::{
    CatalogLookupTarget, CatalogRelationFact, CatalogRelationNeed, SqlCompileError,
};

fn fact_error(error: impl std::fmt::Display) -> SqlCompileError {
    SqlCompileError::Compilation(format!("build captured relation fact: {error}"))
}

fn original_relation_error(
    error: CatalogResolutionError,
) -> crate::mv::domain::application::MvApplicationError {
    use crate::mv::domain::application::MvApplicationError;
    match error.original_provider() {
        Some((provider, source)) => {
            let control = source
                .downcast_ref::<novarocks_type_contract::CompileControlError>()
                .cloned();
            MvApplicationError::new(
                crate::query_execution::mv_assembly::refresh_preparation::io_gate::provider_error_kind(provider),
                error.message(),
            ).with_original_failure(source.clone()).with_compile_control(control)
        }
        None => MvApplicationError::from_compile(error.into_compile()),
    }
}

impl CatalogServiceMaterializer<'_> {
    /// The original MV reader may suspend only at its named I/O gate. The
    /// gate prepares neutral facts; selection, schema creation, and binding
    /// publication resume on that same reader's original CPU source.
    pub(crate) async fn resolve_original_relation_fact(
        &self,
        need: &CatalogRelationNeed,
        controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
        context: novarocks_spi::connector::ConnectorRequestContext,
        io: &crate::query_execution::mv_assembly::refresh_preparation::io_gate::MvRefreshPreparationIo,
    ) -> Result<CatalogRelationFact, crate::mv::domain::application::MvApplicationError> {
        use crate::catalog_application::query_catalog::relation as projection;
        use novarocks_spi::connector::{ConnectorErrorKind, ConnectorTableResolution};
        use novarocks_sql::compiler::CatalogTimeTravelResolution;

        if !matches!(need.target(), CatalogLookupTarget::Table { .. }) {
            return Err(
                crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error(
                    "ordinary original relation capture cannot answer a metadata alias",
                ),
            );
        }
        if let Some(fact) = self.local_view_fact(need) {
            return fact.map_err(crate::mv::domain::application::MvApplicationError::from_compile);
        }
        if let Some(result) = self.original_system_relation_fact(need, io).await
            && let Some(fact) = result?
        {
            return Ok(fact);
        }
        if let Some(fact) = self.local_relation_fact(need) {
            return fact.map_err(crate::mv::domain::application::MvApplicationError::from_compile);
        }
        let relation = need.relation();
        let observation = self.original_catalog_admission(&relation.catalog)?;
        let key = QueryRelationRequestKey::from_need(need)
            .map_err(crate::mv::domain::application::MvApplicationError::from_compile)?;
        let result = if let Some(memo) = self.cached_relation(&key) {
            self.relation_answer_typed(need, memo)
        } else if let Some(fact) = self.completed_numeric_snapshot(need) {
            fact.map_err(CatalogResolutionError::from)
        } else if let Some(fact) = self.completed_current_table(need) {
            fact.map_err(CatalogResolutionError::from)
        } else {
            self.bindings
                .require_catalog_observation_allowed()
                .map_err(
                crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error,
            )?;
            let observed = async {
                let (planning_pin, identity, context) = projection::begin(controls, context, need)?;
                let selection = if need.version().is_some() {
                    let references = io
                        .reference_facts(&planning_pin, identity.clone(), context.clone())
                        .await
                        .map_err(|error| {
                            super::super::query_catalog::connector_table_resolution_error(
                                error,
                                &identity.namespace,
                                &identity.table,
                            )
                        })?;
                    match projection::project_reference(need, references)? {
                        CatalogTimeTravelResolution::Selected(binding) => Some(binding),
                        CatalogTimeTravelResolution::Rejected(fact) => {
                            return Ok(ConnectorQueryRelationMaterialization::SelectionRefused(
                                fact,
                            ));
                        }
                    }
                } else {
                    None
                };
                let resolution = selection
                    .as_ref()
                    .map_or(ConnectorTableResolution::StrictBaseTable, |binding| {
                        ConnectorTableResolution::HistoricalSnapshot(binding.snapshot_id())
                    });
                match io
                    .metadata(
                        &planning_pin,
                        context.clone(),
                        &identity.namespace,
                        &identity.table,
                        resolution,
                    )
                    .await
                {
                    Ok(metadata) => {
                        projection::finish_table(metadata, planning_pin, selection, &identity)
                    }
                    Err(error)
                        if selection.is_none() && error.kind() == ConnectorErrorKind::NotFound =>
                    {
                        let view = io
                            .view_metadata(&planning_pin, identity.clone(), context)
                            .await
                            .map_err(|error| {
                                super::super::query_catalog::connector_table_resolution_error(
                                    error,
                                    &identity.namespace,
                                    &identity.table,
                                )
                            })?;
                        projection::project_view(view, planning_pin, &identity)
                            .map(ConnectorQueryRelationMaterialization::View)
                    }
                    Err(error) => Err(
                        super::super::query_catalog::connector_table_resolution_error(
                            error,
                            &identity.namespace,
                            &identity.table,
                        ),
                    ),
                }
            }
            .await;
            self.retain_relation_observation(need, key, observed)
        };
        let admission = self.original_catalog_admission(&relation.catalog).and_then(|current| {
            if current == observation { Ok(()) } else {
                Err(crate::query_execution::mv_assembly::refresh_preparation::io_gate::contract_error(
                    "catalog attachment generation changed while acquiring its planning pin"))
            }
        });
        let fact = result.map_err(original_relation_error)?;
        admission?;
        Ok(fact)
    }

    fn relation_table_for_need(
        &self,
        need: &CatalogRelationNeed,
        binding: SqlTableBindingId,
        snapshot: Option<i64>,
    ) -> Result<ResolvedAnalyzerTable, SqlCompileError> {
        use crate::catalog_application::query_bindings::parse_time_travel_overlay_identity;
        use novarocks_sql::planning::catalog;
        let table = self
            .bindings
            .binding(binding)
            .map_err(SqlCompileError::Compilation)?
            .resolved
            .clone();
        let actual = catalog::materialization_identity_facts(&table);
        let requested = need.relation();
        let requested_overlay = parse_time_travel_overlay_identity(&requested.table);
        if actual.matches(&requested.catalog, &requested.namespace, &requested.table) {
            return match snapshot.or_else(|| requested_overlay.map(|(_, snapshot)| snapshot)) {
                Some(snapshot) => {
                    catalog::project_frozen_snapshot_sql_name(table, requested, snapshot)
                        .map_err(SqlCompileError::Compilation)
                }
                None => Ok(table),
            };
        }
        let original_overlay = parse_time_travel_overlay_identity(actual.table());
        let snapshot = snapshot
            .or_else(|| requested_overlay.map(|(_, snapshot)| snapshot))
            .ok_or_else(|| {
                SqlCompileError::Compilation(
                    "captured Current table cannot answer another SQL identity".into(),
                )
            })?;
        let original_base = original_overlay.map_or(actual.table(), |(base, _)| base);
        let requested_base = requested_overlay.map_or(requested.table.as_str(), |(base, _)| base);
        if actual.catalog() != requested.catalog
            || actual.namespace() != requested.namespace
            || original_base != requested_base
            || original_overlay.is_some_and(|(_, id)| id != snapshot)
            || requested_overlay.is_some_and(|(_, id)| id != snapshot)
        {
            return Err(SqlCompileError::Compilation(
                "SQL snapshot alias does not name the original canonical input".into(),
            ));
        }
        catalog::project_frozen_snapshot_sql_name(table, requested, snapshot)
            .map_err(SqlCompileError::Compilation)
    }

    fn memoized_relation_fact(
        &self,
        need: &CatalogRelationNeed,
        memo: MemoizedQueryRelation,
    ) -> Result<CatalogRelationFact, SqlCompileError> {
        match memo {
            MemoizedQueryRelation::Table { binding, selection } => {
                let table = self.relation_table_for_need(
                    need,
                    binding,
                    selection.as_ref().map(|binding| binding.snapshot_id()),
                )?;
                match selection {
                    Some(selection) => {
                        CatalogRelationFact::resolved_selected(need, table, &selection)
                    }
                    None => CatalogRelationFact::resolved(need, table),
                }
                .map_err(fact_error)
            }
            MemoizedQueryRelation::View(view) => {
                if view
                    .planning_pin
                    .binding()
                    .descriptor()
                    .instance_id
                    .as_str()
                    != view.definition.identity.catalog
                {
                    return Err(SqlCompileError::Compilation(
                        "view memo and its retained generation name another catalog".into(),
                    ));
                }
                CatalogRelationFact::view(need, view.definition).map_err(fact_error)
            }
            MemoizedQueryRelation::SelectionRefused(fact) => {
                fact.selection_refusal_for(need).map_err(fact_error)
            }
        }
    }

    fn relation_answer_typed(
        &self,
        need: &CatalogRelationNeed,
        memo: CatalogResolutionResult<MemoizedQueryRelation>,
    ) -> CatalogResolutionResult<CatalogRelationFact> {
        match memo {
            Ok(memo) => self.memoized_relation_fact(need, memo).map_err(Into::into),
            Err(CatalogResolutionError::Missing { reason }) => {
                CatalogRelationFact::missing(need, reason)
                    .map_err(fact_error)
                    .map_err(Into::into)
            }
            Err(error) => Err(error),
        }
    }

    fn relation_answer(
        &self,
        need: &CatalogRelationNeed,
        memo: CatalogResolutionResult<MemoizedQueryRelation>,
    ) -> Result<CatalogRelationFact, SqlCompileError> {
        self.relation_answer_typed(need, memo)
            .map_err(CatalogResolutionError::into_compile)
    }

    fn cached_relation(
        &self,
        key: &QueryRelationRequestKey,
    ) -> Option<CatalogResolutionResult<MemoizedQueryRelation>> {
        self.bindings
            .relations
            .lock()
            .expect("statement relation memo lock")
            .get(key)
            .cloned()
    }

    fn retain_relation_observation(
        &self,
        need: &CatalogRelationNeed,
        key: QueryRelationRequestKey,
        observed: CatalogResolutionResult<ConnectorQueryRelationMaterialization>,
    ) -> CatalogResolutionResult<CatalogRelationFact> {
        let memo = observed.and_then(|observed| match observed {
            ConnectorQueryRelationMaterialization::Table {
                materialization,
                selection,
            } => {
                let relation = need.relation();
                let key = selection.as_ref().map_or_else(
                    || {
                        QueryTableBindingKey::analysis_lookup(
                            &relation.catalog,
                            &relation.namespace,
                            &relation.table,
                        )
                    },
                    |selection| {
                        QueryTableBindingKey::snapshot(
                            &relation.catalog,
                            &relation.namespace,
                            &relation.table,
                            selection.snapshot_id(),
                        )
                    },
                );
                let binding = self.bind_for_sql(key, |binding| {
                    PreparedQueryTableBinding {
                        materialization: *materialization,
                        catalog: relation.catalog.clone(),
                        namespace: relation.namespace.clone(),
                        table: relation.table.clone(),
                        metadata_kind: None,
                    }
                    .create(binding)
                })?;
                Ok(MemoizedQueryRelation::Table { binding, selection })
            }
            ConnectorQueryRelationMaterialization::View(view) => {
                Ok(MemoizedQueryRelation::View(view))
            }
            ConnectorQueryRelationMaterialization::SelectionRefused(fact) => {
                Ok(MemoizedQueryRelation::SelectionRefused(Arc::from(fact)))
            }
        });
        {
            let mut entries = self
                .bindings
                .relations
                .lock()
                .expect("statement relation memo lock");
            if entries.contains_key(&key) {
                return Err(CatalogResolutionError::failed(
                    "serialized relation source attempted another observation of a completed request",
                ));
            }
            entries.insert(key, memo.clone());
        }
        self.relation_answer_typed(need, memo)
    }

    fn completed_current_table(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<CatalogRelationFact, SqlCompileError>> {
        if need.version().is_some() {
            return None;
        }
        let relation = need.relation();
        let key = QueryTableBindingKey::analysis_lookup(
            &relation.catalog,
            &relation.namespace,
            &relation.table,
        );
        let Some(Ok(binding)) = self.bindings.resolved_lookup(&key) else {
            return None;
        };
        Some(self.memoized_relation_fact(
            need,
            MemoizedQueryRelation::Table {
                binding,
                selection: None,
            },
        ))
    }

    fn completed_numeric_snapshot(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<CatalogRelationFact, SqlCompileError>> {
        use novarocks_sql::planning::catalog::{
            SqlTimeTravelSelection, parse_time_travel_selection,
        };
        let version = need.version()?;
        let selection = match parse_time_travel_selection(version, &need.relation().fqn()) {
            Ok(selection) => selection,
            Err(error) => return Some(Err(error.into())),
        };
        let SqlTimeTravelSelection::SnapshotId(snapshot) = selection else {
            return None;
        };
        let relation = need.relation();
        let key = QueryTableBindingKey::snapshot(
            &relation.catalog,
            &relation.namespace,
            &relation.table,
            snapshot,
        );
        let Some(Ok(binding)) = self.bindings.resolved_lookup(&key) else {
            return None;
        };
        Some(
            self.relation_table_for_need(need, binding, Some(snapshot))
                .and_then(|table| {
                    CatalogRelationFact::resolved_captured_snapshot(need, table).map_err(fact_error)
                }),
        )
    }

    pub(crate) fn local_view_fact(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<CatalogRelationFact, SqlCompileError>> {
        let relation = need.relation();
        if need.version().is_some()
            || self
                .query_overlay(
                    Some(&relation.catalog),
                    &relation.namespace,
                    &relation.table,
                )
                .is_some()
            || !(need.allows_local_view_shadow() || relation.catalog == "default_catalog")
        {
            return None;
        }
        let Some(snapshot) = self.local_views.as_ref() else {
            return Some(Err(SqlCompileError::Compilation(
                "local view snapshot is not installed for this catalog capture".into(),
            )));
        };
        let local = novarocks_types::naming::TableIdentity::new(
            "default_catalog",
            &relation.namespace,
            &relation.table,
        );
        snapshot
            .get(&local)
            .map(|view| CatalogRelationFact::view(need, view.clone()).map_err(fact_error))
    }

    fn local_relation_fact(
        &self,
        need: &CatalogRelationNeed,
    ) -> Option<Result<CatalogRelationFact, SqlCompileError>> {
        let relation = need.relation();
        let overlay = self.query_overlay(
            Some(&relation.catalog),
            &relation.namespace,
            &relation.table,
        );
        if relation.catalog != "default_catalog" && overlay.is_none() {
            return None;
        }
        if let Some(version) = need.version() {
            let selection = match novarocks_sql::planning::catalog::parse_time_travel_selection(
                version,
                &relation.fqn(),
            ) {
                Ok(selection) => selection,
                Err(error) => return Some(Err(error.into())),
            };
            // A captured source already owns this exact numeric snapshot.
            // Keep its original binding and validate the actual selector;
            // an overlay cannot resolve another snapshot or a named ref.
            if let novarocks_sql::planning::catalog::SqlTimeTravelSelection::SnapshotId(snapshot) =
                selection
                && overlay.as_ref().is_some_and(|overlay| {
                    overlay.key
                        == QueryTableBindingKey::snapshot(
                            &relation.catalog,
                            &relation.namespace,
                            &relation.table,
                            snapshot,
                        )
                })
            {
                return Some(
                    self.resolve_table_for_analysis_once(
                        Some(&relation.catalog),
                        &relation.namespace,
                        &relation.table,
                    )
                    .map_err(CatalogResolutionError::into_compile)
                    .and_then(|table| {
                        CatalogRelationFact::resolved_captured_snapshot(need, table)
                            .map_err(fact_error)
                    }),
                );
            }
            return Some(Err(
                novarocks_sql::analyze_error::AnalyzeError::unsupported_query_shape(
                    "historical selection is not supplied by a local or generated relation",
                    version.span,
                )
                .into(),
            ));
        }
        Some(crate::query_execution::completion_facts::catalog_fact_from(
            need,
            self.resolve_table_for_analysis_once(
                Some(&relation.catalog),
                &relation.namespace,
                &relation.table,
            ),
        ))
    }

    pub(crate) fn resolve_relation_fact(
        &self,
        need: &CatalogRelationNeed,
    ) -> Result<CatalogRelationFact, SqlCompileError> {
        let relation = need.relation();
        if let CatalogLookupTarget::IcebergMetadata { kind } = need.target() {
            return crate::query_execution::completion_facts::catalog_fact_from(
                need,
                self.resolve_metadata_table_typed(
                    Some(&relation.catalog),
                    &relation.namespace,
                    &relation.table,
                    kind,
                ),
            );
        }
        if let Some(fact) = self.local_view_fact(need) {
            return fact;
        }
        if let Some(result) = self.system_relation_fact(need)
            && let Some(fact) = result?
        {
            return Ok(fact);
        }
        if let Some(fact) = self.local_relation_fact(need) {
            return fact;
        }
        let observation = match self.require_catalog_admission(&relation.catalog) {
            Ok(observation) => observation,
            Err(CatalogResolutionError::Missing { reason }) => {
                return CatalogRelationFact::missing(need, reason).map_err(fact_error);
            }
            Err(error) => return Err(error.into_compile()),
        };
        let key = QueryRelationRequestKey::from_need(need)?;
        let result = if let Some(memo) = self.cached_relation(&key) {
            self.relation_answer(need, memo)
        } else if let Some(fact) = self.completed_numeric_snapshot(need) {
            fact
        } else if let Some(fact) = self.completed_current_table(need) {
            fact
        } else {
            self.bindings
                .require_catalog_observation_allowed()
                .map_err(SqlCompileError::Compilation)?;
            self.retain_relation_observation(need, key, self.loader.prepare_relation(need))
                .map_err(CatalogResolutionError::into_compile)
        };
        let admission = self
            .verify_catalog_admission(&relation.catalog, observation.as_ref())
            .map_err(CatalogResolutionError::into_compile);
        let fact = result?;
        admission?;
        Ok(fact)
    }

    pub(crate) async fn resolve_relation_fact_async(
        &self,
        need: &CatalogRelationNeed,
    ) -> Result<CatalogRelationFact, SqlCompileError> {
        let relation = need.relation();
        if let CatalogLookupTarget::IcebergMetadata { kind } = need.target() {
            return crate::query_execution::completion_facts::catalog_fact_from(
                need,
                self.resolve_metadata_table_async(
                    Some(&relation.catalog),
                    &relation.namespace,
                    &relation.table,
                    kind,
                )
                .await,
            );
        }
        if let Some(fact) = self.local_view_fact(need) {
            return fact;
        }
        if self.system_source(need).is_some() {
            // The caller must drive this source with its original CPU path.
            // A completed None is the original reader's explicit decline.
            let memo = self
                .bindings
                .system_relations
                .lock()
                .expect("statement system relation memo")
                .get(need.relation())
                .cloned();
            if !matches!(memo, Some(Ok(None))) {
                return Err(SqlCompileError::Compilation(
                    "definition system relation requires its original CPU fact path".into(),
                ));
            }
        }
        if let Some(fact) = self.local_relation_fact(need) {
            return fact;
        }
        let observation = match self.require_catalog_admission(&relation.catalog) {
            Ok(observation) => observation,
            Err(CatalogResolutionError::Missing { reason }) => {
                return CatalogRelationFact::missing(need, reason).map_err(fact_error);
            }
            Err(error) => return Err(error.into_compile()),
        };
        let key = QueryRelationRequestKey::from_need(need)?;
        let result = if let Some(memo) = self.cached_relation(&key) {
            self.relation_answer(need, memo)
        } else if let Some(fact) = self.completed_numeric_snapshot(need) {
            fact
        } else if let Some(fact) = self.completed_current_table(need) {
            fact
        } else {
            self.bindings
                .require_catalog_observation_allowed()
                .map_err(SqlCompileError::Compilation)?;
            self.retain_relation_observation(
                need,
                key,
                self.loader.prepare_relation_async(need).await,
            )
            .map_err(CatalogResolutionError::into_compile)
        };
        let admission = self
            .verify_catalog_admission(&relation.catalog, observation.as_ref())
            .map_err(CatalogResolutionError::into_compile);
        let fact = result?;
        admission?;
        Ok(fact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RefusingLoader {
        calls: Arc<AtomicUsize>,
        error: CatalogResolutionError,
    }
    impl QueryTableBindingLoader for RefusingLoader {
        fn prepare_relation(
            &self,
            _: &CatalogRelationNeed,
        ) -> CatalogResolutionResult<ConnectorQueryRelationMaterialization> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err(self.error.clone())
        }
        fn prepare_relation_async<'a>(
            &'a self,
            _: &'a CatalogRelationNeed,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = CatalogResolutionResult<ConnectorQueryRelationMaterialization>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                Err(self.error.clone())
            })
        }
        fn load_strict_base_table(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            panic!("ordinary relation facts must not use the table-only loader")
        }
        fn load_metadata_table(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: novarocks_sql::planning::catalog::MetadataTableKind,
            _: SqlTableBindingId,
        ) -> CatalogResolutionResult<QueryTableBinding> {
            panic!("ordinary relation facts must not use a metadata alias")
        }
    }

    struct ObservedCatalogAdmission {
        answers: Vec<novarocks_catalog_application::CatalogAdmission>,
        calls: AtomicUsize,
    }
    impl novarocks_catalog_application::CatalogApplicationPort for ObservedCatalogAdmission {
        fn create_catalog(
            &self,
            _: novarocks_catalog_application::CatalogCreateCommand,
        ) -> Result<
            novarocks_catalog_application::CatalogRuntimeObservation,
            novarocks_catalog_application::CatalogApplicationError,
        > {
            unreachable!("catalog mutation is not part of admission")
        }
        fn drop_catalog(
            &self,
            _: novarocks_catalog_application::CatalogDropCommand,
        ) -> Result<(), novarocks_catalog_application::CatalogApplicationError> {
            unreachable!("catalog mutation is not part of admission")
        }
        fn admit_catalog(
            &self,
            _: &novarocks_spi::connector::ConnectorInstanceId,
        ) -> novarocks_catalog_application::CatalogAdmission {
            let index = self.calls.fetch_add(1, Ordering::Relaxed);
            self.answers
                .get(index)
                .cloned()
                .expect("one original admission per observation phase")
        }
    }

    fn query_source(
        sql: &str,
        catalog: Option<&str>,
        database: &str,
    ) -> (
        novarocks_parser::ast::Query,
        novarocks_sql::compiler::SqlCapturedCatalogPreparation,
    ) {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        let source = novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            catalog,
            database,
            novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        (query.clone(), source)
    }

    fn assert_missing_reaches_original_analysis(
        query: novarocks_parser::ast::Query,
        source: novarocks_sql::compiler::SqlCapturedCatalogPreparation,
        fact: CatalogRelationFact,
        database: &str,
        missing_name: &str,
    ) {
        let source = source
            .continue_catalog(vec![fact].into_boxed_slice())
            .unwrap();
        assert!(source.needs().is_empty());
        let catalog = source.capture(&[]).unwrap();
        let result = novarocks_sql::compiler::analyze_mv_refresh_input(
            novarocks_sql::compiler::SqlMvRefreshAnalysisContext {
                query: Box::new(query),
                current_database: database.into(),
                catalog: &catalog,
                functions: novarocks_sql::compiler::builtin_sql_function_catalog(),
                constant_policy: crate::application::test_constant_policy(),
                control: novarocks_sql::compiler::SqlCompileControl::unbounded(),
            },
        );
        let Err(SqlCompileError::Analyze(error)) = result else {
            panic!("the original SQL analyzer must author the missing relation error")
        };
        assert_eq!(
            error.kind(),
            novarocks_sql::analyze_error::AnalyzeErrorKind::UnknownTable
        );
        assert_eq!(error.code().as_str(), "sql.analyze.unknown_table");
        assert!(error.message().contains(missing_name));
        assert!(error.span().is_some());
    }

    #[tokio::test]
    async fn initial_absent_catalog_answers_the_exact_need_without_entering_a_provider() {
        for asynchronous in [false, true] {
            let service = crate::catalog_application::query_catalog::new_query_catalog_service();
            let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
            let calls = Arc::new(AtomicUsize::new(0));
            let application = ObservedCatalogAdmission {
                answers: vec![novarocks_catalog_application::CatalogAdmission::Absent],
                calls: AtomicUsize::new(0),
            };
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                bindings.clone(),
                Box::new(RefusingLoader {
                    calls: calls.clone(),
                    error: CatalogResolutionError::failed(
                        "Provider must not observe an absent catalog",
                    ),
                }),
            )
            .with_catalog_application(Some(&application));
            let (query, source) = query_source("SELECT id FROM ice.db.orders", Some("ice"), "db");
            let [need] = source.needs() else {
                panic!("one real SQL relation need")
            };
            let fact = if asynchronous {
                materializer.resolve_relation_fact_async(need).await
            } else {
                materializer.resolve_relation_fact(need)
            }
            .unwrap();
            assert_eq!(fact.id(), need.id());
            assert_eq!(fact.relation(), need.relation());
            assert!(matches!(
                fact.outcome(),
                novarocks_sql::compiler::CatalogRelationOutcome::Missing { reason }
                    if reason.contains("unknown catalog `ice`")
            ));
            assert_eq!(application.calls.load(Ordering::Relaxed), 1);
            assert_eq!(calls.load(Ordering::Relaxed), 0);
            assert!(bindings.captured_bindings().is_empty());
            assert_missing_reaches_original_analysis(query, source, fact, "db", "ice");
        }
    }

    #[tokio::test]
    async fn unavailable_catalog_is_a_typed_source_and_never_a_missing_fact() {
        for asynchronous in [false, true] {
            let service = crate::catalog_application::query_catalog::new_query_catalog_service();
            let calls = Arc::new(AtomicUsize::new(0));
            let application = ObservedCatalogAdmission {
                answers: vec![
                    novarocks_catalog_application::CatalogAdmission::Unavailable {
                        reason: "original unavailable projection".into(),
                    },
                ],
                calls: AtomicUsize::new(0),
            };
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                Arc::new(QueryTableBindingStore::try_new().unwrap()),
                Box::new(RefusingLoader {
                    calls: calls.clone(),
                    error: CatalogResolutionError::failed(
                        "Provider must not observe an unavailable catalog",
                    ),
                }),
            )
            .with_catalog_application(Some(&application));
            let need = orders_need("SELECT x FROM ice.db.orders");
            let result = if asynchronous {
                materializer.resolve_relation_fact_async(&need).await
            } else {
                materializer.resolve_relation_fact(&need)
            };
            let Err(SqlCompileError::CompilationSource(source)) = result else {
                panic!("unavailable admission must retain the original typed source")
            };
            let original = source
                .downcast_ref::<novarocks_catalog_application::CatalogApplicationError>()
                .unwrap();
            assert_eq!(
                original.kind(),
                novarocks_catalog_application::CatalogApplicationErrorKind::Unavailable
            );
            assert!(
                original
                    .to_string()
                    .contains("original unavailable projection")
            );
            assert_eq!(application.calls.load(Ordering::Relaxed), 1);
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn post_observation_absence_or_unavailability_keeps_the_original_admission_failure() {
        use novarocks_catalog_application::{CatalogAdmission, CatalogRuntimeObservation};
        use novarocks_failure_contract::FailureProjection;
        let id = novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap();
        for answer in [
            CatalogAdmission::Absent,
            CatalogAdmission::Unavailable {
                reason: "original projection changed".into(),
            },
        ] {
            let expected = answer.clone().require_ready(&id).unwrap_err();
            let observation = CatalogRuntimeObservation {
                attachment_id: uuid::Uuid::from_u128(1),
                instance_id: id.clone(),
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                generation: 7,
            };
            let application = ObservedCatalogAdmission {
                answers: vec![CatalogAdmission::Ready(observation.clone()), answer],
                calls: AtomicUsize::new(0),
            };
            let service = crate::catalog_application::query_catalog::new_query_catalog_service();
            let calls = Arc::new(AtomicUsize::new(0));
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                Arc::new(QueryTableBindingStore::try_new().unwrap()),
                Box::new(RefusingLoader {
                    calls: calls.clone(),
                    error: CatalogResolutionError::failed("this fixture only observes admission"),
                }),
            )
            .with_catalog_application(Some(&application));
            let admitted = materializer.require_catalog_admission("ice").unwrap();
            assert_eq!(admitted.as_ref(), Some(&observation));
            assert_eq!(application.calls.load(Ordering::Relaxed), 1);
            let error = materializer
                .verify_catalog_admission("ice", admitted.as_ref())
                .unwrap_err();
            let compile = error.clone().into_compile();
            assert_eq!(compile, error.into_compile());
            let SqlCompileError::CompilationSource(source) = compile else {
                panic!("the later absence is an original failure, not a missing relation")
            };
            assert_eq!(source.failure_fact(), expected.failure_fact());
            assert_eq!(
                source.downcast_ref::<novarocks_catalog_application::CatalogApplicationError>(),
                Some(&expected)
            );
            assert_eq!(application.calls.load(Ordering::Relaxed), 2);
            assert_eq!(calls.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn empty_local_view_snapshot_keeps_the_local_probe_missing_fact() {
        use novarocks_query_application::view::ViewService;
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let application = ObservedCatalogAdmission {
            answers: Vec::new(),
            calls: AtomicUsize::new(0),
        };
        let views = novarocks_query_application::view::QueryViewService::new();
        let materializer = CatalogServiceMaterializer::new(
            None,
            &service,
            bindings.clone(),
            Box::new(RefusingLoader {
                calls: calls.clone(),
                error: CatalogResolutionError::failed("local absence cannot acquire a Provider"),
            }),
        )
        .with_catalog_application(Some(&application))
        .with_local_views(views.catalog_snapshot().unwrap());
        let database = novarocks_types::naming::DEFAULT_DATABASE;
        let (query, source) = query_source("SELECT * FROM local_probe", None, database);
        let fact = materializer
            .resolve_relation_fact_async(&source.needs()[0])
            .await
            .unwrap();
        assert!(matches!(
            fact.outcome(),
            novarocks_sql::compiler::CatalogRelationOutcome::Missing { reason }
                if reason.contains("local_probe")
        ));
        assert_eq!(application.calls.load(Ordering::Relaxed), 0);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(bindings.captured_bindings().is_empty());
        assert_missing_reaches_original_analysis(query, source, fact, database, "local_probe");
    }

    fn orders_need(sql: &str) -> CatalogRelationNeed {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        let source = novarocks_sql::compiler::SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some("ice"),
            "db",
            novarocks_sql::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        source
            .needs()
            .iter()
            .find(|need| need.relation().table == "orders")
            .unwrap()
            .clone()
    }

    #[tokio::test]
    async fn the_original_statement_memo_preserves_typed_failures_across_materializer_rounds() {
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let failure = CatalogResolutionError::from_connector(
            novarocks_spi::connector::ConnectorError::compile_interruption(
                CompileControlError::from_domain(LayoutResourceError::SourceModel),
            ),
        );
        let expected = failure.clone().into_compile();
        for (round, sql) in [
            "SELECT x FROM ice.db.orders",
            "SELECT b.x FROM ice.db.another a JOIN ice.db.orders b ON 1=1",
        ]
        .iter()
        .enumerate()
        {
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                bindings.clone(),
                Box::new(RefusingLoader {
                    calls: calls.clone(),
                    error: failure.clone(),
                }),
            );
            let need = orders_need(sql);
            assert_eq!(need.id().get(), round as u32);
            let actual = if round == 0 {
                materializer.resolve_relation_fact(&need)
            } else {
                materializer.resolve_relation_fact_async(&need).await
            };
            assert_eq!(actual.unwrap_err(), expected);
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(bindings.captured_bindings().is_empty());
    }

    #[tokio::test]
    async fn a_missing_relation_reanswers_each_exact_need_without_publishing_a_scan_token() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        for sql in [
            "SELECT x FROM ice.db.orders",
            "SELECT b.x FROM ice.db.another a JOIN ice.db.orders b ON 1=1",
        ] {
            let materializer = CatalogServiceMaterializer::new(
                Some("ice"),
                &service,
                bindings.clone(),
                Box::new(RefusingLoader {
                    calls: calls.clone(),
                    error: CatalogResolutionError::missing("absent original relation"),
                }),
            );
            let need = orders_need(sql);
            let fact = materializer
                .resolve_relation_fact_async(&need)
                .await
                .unwrap();
            assert_eq!(fact.id(), need.id());
            assert!(matches!(
                fact.outcome(),
                novarocks_sql::compiler::CatalogRelationOutcome::Missing { .. }
            ));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(bindings.captured_bindings().is_empty());
    }

    #[test]
    fn a_sealed_statement_cannot_observe_an_unknown_view_without_a_scan_token() {
        let service = crate::catalog_application::query_catalog::new_query_catalog_service();
        let bindings = Arc::new(QueryTableBindingStore::try_new().unwrap());
        bindings.seal_for_topology_replan();
        let calls = Arc::new(AtomicUsize::new(0));
        let materializer = CatalogServiceMaterializer::new(
            Some("ice"),
            &service,
            bindings,
            Box::new(RefusingLoader {
                calls: calls.clone(),
                error: CatalogResolutionError::missing("must not load"),
            }),
        );
        assert!(
            materializer
                .resolve_relation_fact(&orders_need("SELECT x FROM ice.db.orders"))
                .is_err()
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
}
