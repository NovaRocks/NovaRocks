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
use super::source::StatementSystemTableSource;
use async_trait::async_trait;
use novarocks_mv_application::freshness::mv_publication_freshness;
use novarocks_query_application::system_tables::{
    SystemTableError, SystemTableProvider, SystemTableRegistry, SystemTableRequest,
    SystemTableSnapshot,
};
use novarocks_spi::connector::{ConnectorErrorKind, ConnectorOperationControl};
use novarocks_sql::system_table::{SystemTableBinding, SystemTableKeyValues};
use novarocks_types::naming::DEFAULT_CATALOG;
use std::sync::Arc;

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
struct Batches<'a> {
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
    async fn push(&mut self, values: &[Option<&str>]) -> Result<(), SystemTableError> {
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
                            Some(""),
                            Some(&facts.definition),
                        ])
                        .await?;
                }
            }
            (binding, "information_schema", "schemata" | "tables") => {
                let catalog = match binding {
                    SystemTableBinding::System => "system",
                    SystemTableBinding::Catalog(name) => name.as_str(),
                };
                let schemata = request.declaration.table == "schemata";
                if !schemata
                    && !request.key_constraint.allows(3, "BASE TABLE")
                    && !request.key_constraint.allows(3, "VIEW")
                {
                    return batches.finish().await;
                }
                let schema_ordinal = 1;
                let mut schemas = if let Some(SystemTableKeyValues::Values(values)) =
                    request.key_constraint.values_for(schema_ordinal)
                {
                    let mut selected = Vec::new();
                    for schema in values {
                        let exists = if matches!(binding, SystemTableBinding::System) {
                            self.registry.schemas(binding).contains(schema)
                        } else {
                            self.source
                                .schema_exists(catalog, schema, request.context)
                                .await?
                        };
                        if exists {
                            selected.push(schema.clone());
                        }
                    }
                    selected
                } else {
                    self.schema_names(binding, catalog, request).await?
                };
                schemas.sort();
                schemas.dedup();
                for schema in schemas {
                    if !request.key_constraint.allows(1, &schema) {
                        continue;
                    }
                    if schemata && schema == "information_schema" {
                        continue;
                    }
                    if schemata {
                        batches
                            .push(&[
                                Some(catalog),
                                Some(&schema),
                                Some("utf8"),
                                Some("utf8_general_ci"),
                                None,
                            ])
                            .await?;
                        continue;
                    }
                    if matches!(binding, SystemTableBinding::System) {
                        for declaration in self.registry.declarations(binding) {
                            if declaration.schema == schema
                                && request.key_constraint.allows(2, &declaration.table)
                                && request.key_constraint.allows(3, "BASE TABLE")
                            {
                                batches
                                    .push(&[
                                        Some(catalog),
                                        Some(&schema),
                                        Some(&declaration.table),
                                        Some("BASE TABLE"),
                                    ])
                                    .await?;
                            }
                        }
                        continue;
                    }
                    if catalog != DEFAULT_CATALOG && request.key_constraint.allows(3, "BASE TABLE")
                    {
                        let mut names = self
                            .source
                            .table_names(catalog, &schema, request.context)
                            .await?;
                        names.sort();
                        names.dedup();
                        for name in names {
                            if request.key_constraint.allows(2, &name) {
                                batches
                                    .push(&[
                                        Some(catalog),
                                        Some(&schema),
                                        Some(&name),
                                        Some("BASE TABLE"),
                                    ])
                                    .await?;
                            }
                        }
                    }
                    if request.key_constraint.allows(3, "VIEW") {
                        let mut names = if catalog == DEFAULT_CATALOG {
                            self.source
                                .local_view_snapshot()
                                .names_in_namespace(catalog, &schema)
                                .map(str::to_owned)
                                .collect()
                        } else {
                            match self
                                .source
                                .view_names(catalog, &schema, request.context)
                                .await
                            {
                                Ok(names) => names,
                                Err(SystemTableError::Provider(error))
                                    if error.kind() == ConnectorErrorKind::Unsupported =>
                                {
                                    vec![]
                                }
                                Err(error) => return Err(error),
                            }
                        };
                        names.sort();
                        names.dedup();
                        for name in names {
                            if request.key_constraint.allows(2, &name) {
                                batches
                                    .push(&[
                                        Some(catalog),
                                        Some(&schema),
                                        Some(&name),
                                        Some("VIEW"),
                                    ])
                                    .await?;
                            }
                        }
                    }
                }
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
impl FrontendSystemTableProvider {
    async fn schema_names(
        &self,
        binding: &SystemTableBinding,
        catalog: &str,
        request: &SystemTableRequest<'_>,
    ) -> Result<Vec<String>, SystemTableError> {
        if matches!(binding, SystemTableBinding::System) {
            Ok(self.registry.schemas(binding))
        } else {
            self.source.schema_names(catalog, request.context).await
        }
    }
}
