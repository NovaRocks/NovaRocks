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

//! Statement-owned capabilities; the registry contains none of these ports.
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use novarocks_mv_application::freshness::MvSourceObservation;
use novarocks_mv_application::persistence::codec::{PublicationInput, RelationOccurrence};
use novarocks_mv_application::readiness::{MvKeyedProjectionInventory, MvListedManageability};
use novarocks_query_application::system_tables::{SystemTableError, SystemTableSnapshot};
use novarocks_query_application::view::LocalViewSnapshot;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorRequestContext};
use novarocks_sql::system_table::SystemTableDeclaration;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::sync::Arc;

/// A single batch of requested scalar facts, transferred to the original CPU
/// creator. It is never a complete, separately materialized system-table copy.
pub(crate) type SystemTableRow = Vec<Option<String>>;
pub(crate) struct MaterializedSystemTableBatch {
    pub batch: RecordBatch,
    pub storage: ValueTypeStorageLifetime,
}

#[derive(Clone)]
pub(crate) struct MaterializedViewFacts {
    pub catalog: String,
    pub schema: String,
    pub name: String,
    pub definition: String,
    pub unavailable: bool,
    pub occurrences: Vec<RelationOccurrence>,
    pub publication: Option<Vec<PublicationInput>>,
}
#[async_trait]
pub(crate) trait MaterializedViewInventory: Send {
    async fn next(&mut self) -> Result<Option<MaterializedViewFacts>, SystemTableError>;
}

/// Thin adapter over the original listable inventory. Only one loaded
/// projection is transformed at a time; the original inventory owns locators.
pub(crate) struct ListableMaterializedViews {
    inventory: MvKeyedProjectionInventory,
    include_freshness: bool,
    include_definition: bool,
}
impl ListableMaterializedViews {
    pub(crate) fn new(
        inventory: MvKeyedProjectionInventory,
        include_freshness: bool,
        include_definition: bool,
    ) -> Self {
        Self {
            inventory,
            include_freshness,
            include_definition,
        }
    }
}
#[async_trait]
impl MaterializedViewInventory for ListableMaterializedViews {
    async fn next(&mut self) -> Result<Option<MaterializedViewFacts>, SystemTableError> {
        let listed = self
            .inventory
            .next_listable()
            .await
            .map_err(repository_error)?;
        let Some(listed) = listed else {
            return Ok(None);
        };
        let facts = &listed.loaded.projection.facts;
        let target = &facts.source_revision().target;
        let unavailable = matches!(listed.manageability, MvListedManageability::Unavailable(_));
        Ok(Some(MaterializedViewFacts {
            catalog: target.instance_id.as_str().to_owned(),
            schema: target.namespace.to_string(),
            name: target.table.to_string(),
            definition: if self.include_definition {
                facts.definition().query.effective_sql.clone()
            } else {
                String::new()
            },
            unavailable,
            occurrences: if self.include_freshness && !unavailable {
                facts.definition().relation_occurrences.clone()
            } else {
                vec![]
            },
            publication: if self.include_freshness && !unavailable {
                facts.publication().map(|p| p.inputs.clone())
            } else {
                None
            },
        }))
    }
}
pub(crate) fn repository_error(
    error: novarocks_mv_application::repository::MvRepositoryError,
) -> SystemTableError {
    use novarocks_mv_application::repository::MvRepositoryErrorKind as K;
    let kind = match error.kind() {
        K::InvalidRequest => ConnectorErrorKind::InvalidRequest,
        K::NotFound => ConnectorErrorKind::NotFound,
        K::Conflict => ConnectorErrorKind::Unavailable,
        K::Corruption => ConnectorErrorKind::CorruptData,
        K::Unavailable => ConnectorErrorKind::Unavailable,
        K::CommitUnknown => ConnectorErrorKind::Unavailable,
    };
    ConnectorError::from_domain(kind, error).into()
}

/// Implementations retain the original statement pins, I/O owner, CPU sources
/// and StateStore requests in their stable Source until actual close. Borrowing
/// or cancelling a provider future does not retire those original resources.
#[async_trait]
pub(crate) trait StatementSystemTableSource: Send + Sync {
    async fn schema_names(
        &self,
        catalog: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError>;
    async fn schema_exists(
        &self,
        catalog: &str,
        schema: &str,
        context: &ConnectorRequestContext,
    ) -> Result<bool, SystemTableError>;
    async fn table_names(
        &self,
        catalog: &str,
        schema: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError>;
    async fn view_names(
        &self,
        catalog: &str,
        schema: &str,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError>;
    fn local_view_snapshot(&self) -> &LocalViewSnapshot;
    async fn materialized_views(
        &self,
        include_freshness: bool,
        include_definition: bool,
        context: &ConnectorRequestContext,
    ) -> Result<Box<dyn MaterializedViewInventory>, SystemTableError>;
    async fn current_sources(
        &self,
        occurrences: &[RelationOccurrence],
        context: &ConnectorRequestContext,
    ) -> Result<Vec<MvSourceObservation>, SystemTableError>;
    /// Begin one snapshot in the original stable CPU body; no payload is copied here.
    fn begin_snapshot(
        &self,
        declaration: Arc<SystemTableDeclaration>,
        required_columns: &[usize],
    ) -> Result<(), SystemTableError>;
    /// Reserve the original window before copying borrowed row facts into that body.
    fn stage_row(
        &self,
        values: &[Option<&str>],
    ) -> Result<super::construction::RowStage, SystemTableError>;
    /// Construct and retain a batch in the original CPU body after actual turn exit.
    async fn materialize_batch(&self) -> Result<(), SystemTableError>;
    /// Move the sequence only after its original creator has funded final storage.
    async fn finish_snapshot(&self) -> Result<SystemTableSnapshot, SystemTableError>;
}
