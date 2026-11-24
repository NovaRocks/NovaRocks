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

use super::construction::RowStage;

#[path = "metadata_provider.rs"]
mod metadata_provider;
use super::source::{StatementSystemTableSource, SystemValueRef};
use async_trait::async_trait;
use novarocks_mv_application::freshness::mv_publication_freshness;
use novarocks_query_application::system_tables::{
    SystemTableError, SystemTableProvider, SystemTableRegistry, SystemTableRequest,
    SystemTableSnapshot,
};
use novarocks_spi::connector::{ConnectorErrorKind, ConnectorOperationControl};
use novarocks_sql::system_table::SystemTableBinding;
use std::sync::Arc;

const RETIRED_MV_REASON: &str = "Retired MV persistence format supports DROP only";

fn refresh_policy(
    policy: novarocks_mv_application::persistence::codec::RefreshPolicy,
) -> &'static str {
    use novarocks_mv_application::persistence::codec::RefreshPolicy;
    match policy {
        RefreshPolicy::Manual => "MANUAL",
        RefreshPolicy::AsyncOnChange => "ON_CHANGE",
        RefreshPolicy::AsyncInterval => "INTERVAL",
    }
}

pub(crate) fn materialized_view_states_registration()
-> novarocks_query_application::system_tables::SystemTableRegistration {
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use novarocks_query_application::system_tables::{SystemTableRegistration, SystemTableScope};
    use novarocks_sql::system_table::SystemTableColumn;
    use novarocks_type_contract::FunctionValueType;
    let text_list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
    let columns = [
        ("catalog_name", DataType::Utf8, false),
        ("schema_name", DataType::Utf8, false),
        ("name", DataType::Utf8, false),
        ("refresh_policy", DataType::Utf8, false),
        ("refresh_interval_ms", DataType::Int64, true),
        ("refresh_paused", DataType::Boolean, false),
        (
            "last_refresh_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        ("last_refresh_rows", DataType::Int64, true),
        ("manageability", DataType::Utf8, false),
        ("manageability_reason", DataType::Utf8, true),
        ("base_tables", text_list.clone(), false),
        ("upstream_materialized_views", text_list, true),
    ]
    .into_iter()
    .map(|(name, ty, nullable)| SystemTableColumn {
        name: name.into(),
        hidden: false,
        value_type: FunctionValueType::new(ty, nullable),
    })
    .collect::<Vec<_>>();
    SystemTableRegistration {
        scope: SystemTableScope::System,
        schema: "runtime".into(),
        table: "materialized_view_states".into(),
        columns: columns.into(),
        key_columns: Arc::from([0, 1, 2]),
    }
}
fn manageability_parts(
    manageability: &novarocks_mv_application::readiness::MvListedManageability,
    phase: novarocks_mv_application::management::MvManagementPhase,
) -> (&'static str, Option<&str>) {
    use novarocks_mv_application::{
        management::MvManagementPhase as P, readiness::MvListedManageability as R,
    };
    match manageability {
        R::ReadOnly(reason) => ("READ_ONLY", Some(reason)),
        R::Unavailable(reason) => ("UNAVAILABLE", Some(reason)),
        R::Manageable => match phase {
            P::Manageable | P::NotObserved => ("MANAGEABLE", None),
            P::Managing => ("MANAGING", None),
            other => ("READ_ONLY", Some(other.as_str())),
        },
    }
}
fn signed_fact(value: u64, fact: &str) -> Result<i64, SystemTableError> {
    i64::try_from(value).map_err(|_| {
        novarocks_spi::connector::ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("materialized-view {fact} exceeds signed SQL width"),
        )
        .into()
    })
}
pub(crate) struct FrontendSystemTableProvider {
    registry: Arc<SystemTableRegistry>,
    source: Arc<dyn StatementSystemTableSource>,
}
impl FrontendSystemTableProvider {
    pub(crate) fn new(
        registry: Arc<SystemTableRegistry>,
        source: Arc<dyn StatementSystemTableSource>,
    ) -> Self {
        Self { registry, source }
    }
}
pub(super) struct Batches<'a> {
    provider: &'a FrontendSystemTableProvider,
    request: &'a SystemTableRequest<'a>,
    rows: usize,
}
impl<'a> Batches<'a> {
    fn new(provider: &'a FrontendSystemTableProvider, request: &'a SystemTableRequest<'a>) -> Self {
        Self {
            provider,
            request,
            rows: 0,
        }
    }
    async fn push<const N: usize>(
        &mut self,
        values: &[Option<&str>; N],
    ) -> Result<(), SystemTableError> {
        self.push_values(&values.map(SystemValueRef::from)).await
    }
    pub(super) async fn push_values(
        &mut self,
        values: &[SystemValueRef<'_>],
    ) -> Result<(), SystemTableError> {
        self.request
            .context
            .original_operation_control()
            .check_active()?;
        if self.provider.source.stage_row(values)? == RowStage::FlushRequired {
            self.flush().await?;
            if self.provider.source.stage_row(values)? != RowStage::Staged {
                return Err(SystemTableError::Internal(
                    "empty system-table batch cannot accept one row".into(),
                ));
            }
        }
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| SystemTableError::Internal("system-table row count overflow".into()))?;
        if self.rows == self.request.batch_target_rows {
            self.flush().await?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<(), SystemTableError> {
        if self.rows == 0 {
            return Ok(());
        }
        self.provider.source.materialize_batch().await?;
        self.rows = 0;
        Ok(())
    }
    async fn finish(mut self) -> Result<SystemTableSnapshot, SystemTableError> {
        self.flush().await?;
        self.provider.source.finish_snapshot().await
    }
}
#[async_trait]
impl SystemTableProvider for FrontendSystemTableProvider {
    async fn snapshot(
        &self,
        request: &SystemTableRequest<'_>,
    ) -> Result<SystemTableSnapshot, SystemTableError> {
        request
            .context
            .original_operation_control()
            .check_active()?;
        self.source
            .begin_snapshot(Arc::clone(&request.declaration), &request.required_columns)?;
        let mut batches = Batches::new(self, request);
        if request.key_constraint.is_none() {
            return batches.finish().await;
        }
        match (
            &request.declaration.binding,
            request.declaration.schema.as_str(),
            request.declaration.table.as_str(),
        ) {
            (SystemTableBinding::System, "metadata", "materialized_views") => {
                let mut inventory = self
                    .source
                    .materialized_views(request.requires(6), request.requires(9), request.context)
                    .await?;
                while let Some(facts) = inventory.next().await? {
                    if !request.key_constraint.allows(0, &facts.catalog)
                        || !request.key_constraint.allows(1, &facts.schema)
                        || !request.key_constraint.allows(2, &facts.name)
                    {
                        continue;
                    }
                    let freshness = if request.requires(6) {
                        if facts.unavailable {
                            Some("UNKNOWN")
                        } else {
                            let observed = self
                                .source
                                .current_sources(&facts.occurrences, request.context)
                                .await?;
                            Some(
                                mv_publication_freshness(
                                    facts.publication.as_deref(),
                                    &facts.occurrences,
                                    &observed,
                                )
                                .map_err(|error| SystemTableError::Internal(error.to_string()))?
                                .as_str(),
                            )
                        }
                    } else {
                        None
                    };
                    batches
                        .push(&[
                            Some(&facts.catalog),
                            Some(&facts.schema),
                            Some(&facts.name),
                            Some(&facts.catalog),
                            Some(&facts.schema),
                            Some(&facts.name),
                            freshness,
                            None,
                            Some(if facts.retired.is_some() {
                                RETIRED_MV_REASON
                            } else {
                                ""
                            }),
                            facts.retired.is_none().then_some(facts.definition.as_str()),
                        ])
                        .await?;
                }
            }
            (SystemTableBinding::System, "runtime", "materialized_view_states") => {
                let mut inventory = self
                    .source
                    .materialized_view_states(request.context)
                    .await?;
                while let Some(facts) = inventory.next().await? {
                    if !request.key_constraint.allows(0, &facts.catalog)
                        || !request.key_constraint.allows(1, &facts.schema)
                        || !request.key_constraint.allows(2, &facts.name)
                    {
                        continue;
                    }
                    if let Some(configuration) = &facts.retired {
                        let interval = configuration
                            .refresh_interval_ms
                            .filter(|_| request.requires(4))
                            .map(|value| signed_fact(value, "refresh interval"))
                            .transpose()?;
                        batches
                            .push_values(&[
                                SystemValueRef::Utf8(&facts.catalog),
                                SystemValueRef::Utf8(&facts.schema),
                                SystemValueRef::Utf8(&facts.name),
                                SystemValueRef::Utf8(refresh_policy(configuration.refresh_policy)),
                                interval.map_or(SystemValueRef::Null, SystemValueRef::Int64),
                                SystemValueRef::Boolean(configuration.paused),
                                SystemValueRef::Null,
                                SystemValueRef::Null,
                                SystemValueRef::Utf8("RETIRED"),
                                SystemValueRef::Utf8(RETIRED_MV_REASON),
                                SystemValueRef::Utf8List(&[]),
                                SystemValueRef::Null,
                            ])
                            .await?;
                        continue;
                    }
                    let state = facts.state.as_ref().ok_or_else(|| {
                        SystemTableError::Internal(
                            "materialized-view inventory omitted requested state facts".into(),
                        )
                    })?;
                    let projection = &state.loaded.projection.facts;
                    let configuration = projection.configuration();
                    use novarocks_mv_application::persistence::codec::RefreshPolicy;
                    let policy = refresh_policy(configuration.refresh_policy);
                    let interval = if request.requires(4)
                        && configuration.refresh_policy == RefreshPolicy::AsyncInterval
                    {
                        configuration
                            .refresh_interval_ms
                            .map(|v| signed_fact(v, "refresh interval"))
                            .transpose()?
                    } else {
                        None
                    };
                    let published = projection.publication();
                    let last_time = published
                        .filter(|_| request.requires(6))
                        .map(|p| {
                            signed_fact(p.publication_prepared_at_ms, "publication prepared time")
                        })
                        .transpose()?;
                    let last_rows = published
                        .filter(|_| request.requires(7))
                        .and_then(|p| p.logical_result_rows)
                        .map(|v| signed_fact(v, "publication row count"))
                        .transpose()?;
                    let (manageability, reason) =
                        manageability_parts(&state.manageability, state.phase);
                    let mut bases = Vec::new();
                    if request.requires(10) {
                        let mut seen = std::collections::BTreeSet::new();
                        for occurrence in &projection.binding().occurrences {
                            let name = format!(
                                "{}.{}.{}",
                                occurrence.catalog_at_binding,
                                occurrence.namespace_at_binding,
                                occurrence.relation_at_binding
                            );
                            if seen.insert((
                                &occurrence.catalog_at_binding,
                                &occurrence.namespace_at_binding,
                                &occurrence.relation_at_binding,
                            )) {
                                bases.push(name);
                            }
                        }
                    }
                    let upstream = if request.requires(11) && state.manageability == novarocks_mv_application::readiness::MvListedManageability::Manageable {
                        Some(self.source.upstream_materialized_views(&state.loaded, request.context).await?)
                    } else { None };
                    batches
                        .push_values(&[
                            SystemValueRef::Utf8(&facts.catalog),
                            SystemValueRef::Utf8(&facts.schema),
                            SystemValueRef::Utf8(&facts.name),
                            SystemValueRef::Utf8(policy),
                            interval.map_or(SystemValueRef::Null, SystemValueRef::Int64),
                            SystemValueRef::Boolean(configuration.paused),
                            last_time.map_or(SystemValueRef::Null, SystemValueRef::TimestampMillis),
                            last_rows.map_or(SystemValueRef::Null, SystemValueRef::Int64),
                            SystemValueRef::Utf8(manageability),
                            reason.into(),
                            SystemValueRef::Utf8List(&bases),
                            upstream
                                .as_deref()
                                .map_or(SystemValueRef::Null, SystemValueRef::Utf8List),
                        ])
                        .await?;
                }
            }
            (SystemTableBinding::System, "runtime", "nodes" | "queries") => {
                let observation = self.source.runtime_metadata().ok_or_else(|| {
                    SystemTableError::Internal("runtime observation owner is not installed".into())
                })?;
                if request.declaration.table == "nodes" {
                    observation
                        .metadata
                        .nodes(&mut batches, request, observation.capacity)
                        .await?;
                } else {
                    let principal = observation.principal.ok_or_else(|| {
                        SystemTableError::Internal(
                            "runtime queries require an admitted observer".into(),
                        )
                    })?;
                    observation
                        .metadata
                        .queries(&mut batches, request, principal)
                        .await?;
                }
            }
            (_, "information_schema", _) | (SystemTableBinding::System, "jdbc", _) => {
                self.discovery_snapshot(&mut batches, request).await?;
            }
            _ => {
                return Err(SystemTableError::InvalidRequest(
                    "no frontend provider for declared system table".into(),
                ));
            }
        }
        batches.finish().await
    }
}
