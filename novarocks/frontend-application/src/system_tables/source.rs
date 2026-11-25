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
use novarocks_mv_application::persistence::codec::{BindingOccurrence, PublicationInput};
use novarocks_mv_application::readiness::{MvKeyedProjectionInventory, MvListedManageability};
use novarocks_query_application::system_tables::{SystemTableError, SystemTableSnapshot};
use novarocks_query_application::view::LocalViewSnapshot;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
    DiscoveredRelation, DiscoveryProjection, DiscoveryRelationKind,
};
use novarocks_sql::system_table::SystemTableDeclaration;
use novarocks_type_contract::{CompileControlError, ValueTypeStorageLifetime};
use novarocks_workload_control::{ResultPayloadBytes, ResultWindowAlias, WorkError};
use std::sync::Arc;

use super::construction::{array_layout, work_error};

/// A single batch of requested scalar facts, transferred to the original CPU
/// creator. It is never a complete, separately materialized system-table copy.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SystemValue {
    Null,
    Utf8(String),
    Int64(i64),
    Boolean(bool),
    TimestampMillis(i64),
    ZonedTimestamp([u8; 14]),
    Utf8List(Vec<String>),
}
#[derive(Clone, Copy)]
pub(crate) enum SystemValueRef<'a> {
    Null,
    Utf8(&'a str),
    FormattedText(&'a (dyn std::fmt::Display + Sync)),
    Int64(i64),
    Boolean(bool),
    TimestampMillis(i64),
    ZonedTimestamp([u8; 14]),
    Utf8List(&'a [String]),
}
impl<'a> From<Option<&'a str>> for SystemValueRef<'a> {
    fn from(value: Option<&'a str>) -> Self {
        value.map_or(Self::Null, Self::Utf8)
    }
}
impl SystemValue {
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
    pub(crate) fn as_ref(&self) -> SystemValueRef<'_> {
        match self {
            Self::Null => SystemValueRef::Null,
            Self::Utf8(value) => SystemValueRef::Utf8(value),
            Self::Int64(value) => SystemValueRef::Int64(*value),
            Self::Boolean(value) => SystemValueRef::Boolean(*value),
            Self::TimestampMillis(value) => SystemValueRef::TimestampMillis(*value),
            Self::ZonedTimestamp(value) => SystemValueRef::ZonedTimestamp(*value),
            Self::Utf8List(value) => SystemValueRef::Utf8List(value),
        }
    }
}
pub(crate) type SystemTableRow = Vec<SystemValue>;
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
    pub occurrences: Vec<BindingOccurrence>,
    pub publication: Option<Vec<PublicationInput>>,
    pub state: Option<MaterializedViewStateFacts>,
    pub retired: Option<novarocks_mv_application::persistence::codec::ConfigurationDocument>,
}
#[derive(Clone)]
pub(crate) struct MaterializedViewStateFacts {
    pub loaded: novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection,
    pub manageability: MvListedManageability,
    pub phase: novarocks_mv_application::management::MvManagementPhase,
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
    management: Option<Arc<novarocks_mv_application::management::ManagementEntrance>>,
    retired:
        std::vec::IntoIter<novarocks_mv_application::persistence::keyed_documents::RetiredMvStatus>,
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
            management: None,
            retired: Vec::new().into_iter(),
        }
    }
    pub(crate) fn with_retired(
        mut self,
        retired: Vec<novarocks_mv_application::persistence::keyed_documents::RetiredMvStatus>,
    ) -> Self {
        self.retired = retired.into_iter();
        self
    }
    pub(crate) fn with_state_facts(
        mut self,
        management: Arc<novarocks_mv_application::management::ManagementEntrance>,
    ) -> Self {
        self.management = Some(management);
        self
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
            return Ok(self.retired.next().map(|retired| {
                let table = retired.target.table();
                MaterializedViewFacts {
                    catalog: table.instance_id.as_str().to_owned(),
                    schema: table.namespace.to_string(),
                    name: table.table.to_string(),
                    definition: String::new(),
                    unavailable: true,
                    occurrences: Vec::new(),
                    publication: None,
                    state: None,
                    retired: Some(retired.configuration),
                }
            }));
        };
        let facts = &listed.loaded.projection.facts;
        let target = &facts.source_revision().target;
        let unavailable = matches!(listed.manageability, MvListedManageability::Unavailable(_));
        let phase = self
            .management
            .as_ref()
            .map(|entrance| entrance.management_phase(target));
        let mut output = MaterializedViewFacts {
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
                facts.binding().occurrences.clone()
            } else {
                vec![]
            },
            state: None,
            retired: None,
            publication: if self.include_freshness && !unavailable {
                facts.publication().map(|p| p.inputs.clone())
            } else {
                None
            },
        };
        if let Some(phase) = phase {
            output.state = Some(MaterializedViewStateFacts {
                loaded: listed.loaded,
                manageability: listed.manageability,
                phase,
            });
        }
        Ok(Some(output))
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

#[derive(Debug)]
pub(crate) struct MetadataNames {
    names: Vec<String>,
    _retained: Option<ResultPayloadBytes>,
}

impl MetadataNames {
    /// Keep the FE copies charged while any caller borrows the listing.
    /// The provider still owns the original listing, pagination, and I/O.
    pub(crate) fn copy<'a, I: Iterator<Item = &'a str>>(
        window: &ResultWindowAlias,
        context: &ConnectorRequestContext,
        candidates: impl Fn() -> I,
    ) -> Result<Self, SystemTableError> {
        let control = context.original_operation_control();
        control.check_active()?;
        let overflow = || work_error(WorkError::ArithmeticOverflow);
        let (count, text_bytes) =
            candidates().try_fold((0usize, 0usize), |(count, size), name| {
                control.check_active()?;
                Ok::<_, SystemTableError>((
                    count.checked_add(1).ok_or_else(overflow)?,
                    size.checked_add(name.len()).ok_or_else(overflow)?,
                ))
            })?;
        let extent = array_layout::<String>(count)?
            .size()
            .checked_add(text_bytes)
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(overflow)?;
        let mut retained = window.reserve_payload_bytes(extent).map_err(work_error)?;
        let exhausted = || super::control_error(CompileControlError::ResourceExhausted);
        let mut names = Vec::new();
        names.try_reserve_exact(count).map_err(|_| exhausted())?;
        for name in candidates() {
            control.check_active()?;
            let mut copy = String::new();
            copy.try_reserve_exact(name.len())
                .map_err(|_| exhausted())?;
            copy.push_str(name);
            names.push(copy);
        }
        names.sort_unstable();
        let mut removed_text = 0usize;
        names.dedup_by(|later, earlier| {
            if later == earlier {
                removed_text += later.len();
                true
            } else {
                false
            }
        });
        retained
            .release_destroyed(u64::try_from(removed_text).map_err(|_| overflow())?)
            .map_err(work_error)?;
        Ok(Self {
            names,
            _retained: Some(retained),
        })
    }

    #[cfg(test)]
    pub(super) fn fixture(mut names: Vec<String>) -> Self {
        names.sort_unstable();
        names.dedup();
        Self {
            names,
            _retained: None,
        }
    }
}

impl std::ops::Deref for MetadataNames {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.names
    }
}

impl<'a> IntoIterator for &'a MetadataNames {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.names.iter()
    }
}

pub(crate) struct RuntimeMetadataObservation<'a> {
    pub(crate) metadata: &'a super::runtime::FrontendRuntimeMetadata,
    pub(crate) capacity:
        &'a novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    pub(crate) principal: Option<&'a str>,
}

/// Implementations retain the original statement pins, I/O owner, CPU sources
/// and StateStore requests in their stable Source until actual close. Borrowing
/// or cancelling a provider future does not retire those original resources.
#[async_trait]
pub(crate) trait StatementSystemTableSource: Send + Sync {
    fn runtime_metadata(&self) -> Option<RuntimeMetadataObservation<'_>> {
        None
    }
    fn catalog_names(&self) -> Result<MetadataNames, SystemTableError>;
    fn supports_discovery(&self, catalog: &str) -> Result<bool, SystemTableError>;
    fn can_probe_discovery_name(&self, catalog: &str, name: &str)
    -> Result<bool, SystemTableError>;
    async fn discover_relation(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        kind: Option<DiscoveryRelationKind>,
        projection: DiscoveryProjection,
    ) -> Result<Option<DiscoveredRelation>, SystemTableError>;
    async fn schema_names(
        &self,
        catalog: &str,
        context: &ConnectorRequestContext,
    ) -> Result<MetadataNames, SystemTableError>;
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
    ) -> Result<MetadataNames, SystemTableError>;
    async fn view_names(
        &self,
        catalog: &str,
        schema: &str,
        context: &ConnectorRequestContext,
    ) -> Result<MetadataNames, SystemTableError>;
    fn local_view_snapshot(&self) -> &LocalViewSnapshot;
    async fn materialized_views(
        &self,
        include_freshness: bool,
        include_definition: bool,
        context: &ConnectorRequestContext,
    ) -> Result<Box<dyn MaterializedViewInventory>, SystemTableError>;
    async fn materialized_view_states(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<Box<dyn MaterializedViewInventory>, SystemTableError>;
    async fn upstream_materialized_views(
        &self,
        projection: &novarocks_mv_application::state_store_repository::keyed::LoadedKeyedMvProjection,
        context: &ConnectorRequestContext,
    ) -> Result<Vec<String>, SystemTableError>;
    async fn current_sources(
        &self,
        occurrences: &[BindingOccurrence],
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
        values: &[SystemValueRef<'_>],
    ) -> Result<super::construction::RowStage, SystemTableError>;
    /// Construct and retain a batch in the original CPU body after actual turn exit.
    async fn materialize_batch(&self) -> Result<(), SystemTableError>;
    /// Move the sequence only after its original creator has funded final storage.
    async fn finish_snapshot(&self) -> Result<SystemTableSnapshot, SystemTableError>;
}
