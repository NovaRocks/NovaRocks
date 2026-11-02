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

// Typed fixture provider minting the original rewrite-read receipt. No stock
// Iceberg reads or external I/O are claimed by this component fixture.
use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadColumnBinding, ProviderReadFilterResult, ProviderReadLimitApplication,
    ProviderReadMetadata, ProviderReadRuntime, ProviderReadSystemTablePlan,
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CowReadColumn;
impl spi::read_stack::ColumnHandle for CowReadColumn {}
#[derive(Debug)]
pub struct CowReadSplit;
impl spi::read_stack::ConnectorSplit for CowReadSplit {
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

#[derive(Clone, Debug)]
pub struct CowProviderFrozenSource {
    owner: spi::ConnectorProviderBindingKey,
    source: ws::ConnectorWriteRewriteSource,
}

impl ProviderReadRuntime for FakeRowMutationProvider {
    type Table = CowProviderFrozenSource;
    type Column = CowReadColumn;
    type Transaction = ();
    type Split = CowReadSplit;
    fn descriptor(&self) -> &spi::ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &spi::CatalogHandle {
        &self.catalog_handle
    }
    fn transaction(&self) -> Self::Transaction {}
}
impl ProviderReadMetadata for FakeRowMutationProvider {
    type PreparedSourceRead =
        novarocks_spi::connector::read_stack::adapter::UnsupportedPreparedSourceRead;
    type PreparedRelation = (
        Self::Table,
        Option<novarocks_spi::connector::read_stack::SystemTableUnitHint>,
    );
    fn prepare_relation<'a>(
        &'a self,
        session: &'a novarocks_spi::connector::read_stack::ConnectorSession,
        request: &'a novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationRequest,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<Self::PreparedRelation>, novarocks_spi::connector::ConnectorError>,
    > {
        Box::pin(async move {
            use novarocks_spi::connector::read_stack::ConnectorReadRelationPreparationKind as K;
            let name = request.name();
            match request.kind() {
                K::Table { version, reference } => self
                    .get_table_handle(session, name, *version, reference.as_deref())
                    .map(|v| v.map(|v| (v, None))),
                K::Metadata { request } => match request {
                    Some(request) => self.get_system_table_plan_for_request(session, name, request),
                    None => self.get_system_table_plan(session, name),
                }
                .map(|v| {
                    v.map(|v| {
                        let hint = v.unit_hint();
                        (v.into_handle(), Some(hint))
                    })
                }),
                K::Delta { window } => self
                    .get_change_window_plan(session, name, *window)
                    .map(|v| v.map(|v| (v, None))),
                K::MvTarget {
                    object_id,
                    snapshot_id,
                } => self
                    .get_mv_target_handle(session, name, object_id, *snapshot_id)
                    .map(|v| v.map(|v| (v, None))),
                K::MvTargetPartition { selection } => self
                    .get_mv_target_partition_handle(session, name, selection)
                    .map(|v| v.map(|v| (v, None))),
                K::PinnedFileSet { pinned } => self
                    .get_pinned_file_set_handle(session, name, pinned)
                    .map(|v| v.map(|v| (v, None))),
                K::TableExecute { procedure } => self
                    .get_table_execute_plan(session, name, procedure.clone())
                    .map(|v| v.map(|v| (v, None))),
            }
        })
    }
    fn create_prepared_relation(
        &self,
        prepared: Self::PreparedRelation,
    ) -> Result<
        Option<
            novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation<Self::Table>,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(Some(
            novarocks_spi::connector::read_stack::adapter::ProviderReadOpenedRelation::new(
                prepared.0, prepared.1,
            ),
        ))
    }

    type FrozenSource = CowProviderFrozenSource;
    fn adopt_frozen_source(
        &self,
        _: &spi::read_stack::ConnectorSession,
        owner: &spi::ConnectorProviderBindingKey,
        source: &Self::FrozenSource,
    ) -> Result<Self::Table, spi::ConnectorError> {
        if owner != &source.owner
            || &owner.instance_id != source.source.source().owner()
            || owner.instance_id != self.descriptor.instance_id
        {
            return Err(spi::ConnectorError::new(
                spi::ConnectorErrorKind::InvalidRequest,
                "COW fixture frozen source owner differs from its provider",
            ));
        }
        Ok(source.clone())
    }
    fn get_table_handle(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &spi::read_stack::SchemaTableName,
        _: spi::read_stack::ConnectorReadRelationVersion,
        _: Option<&str>,
    ) -> Result<Option<Self::Table>, spi::ConnectorError> {
        unreachable!("COW component uses its original frozen provider source")
    }
    fn get_pinned_file_set_handle(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &spi::read_stack::SchemaTableName,
        _: &spi::ConnectorPinnedFileSet,
    ) -> Result<Option<Self::Table>, spi::ConnectorError> {
        unreachable!("COW component uses its original frozen provider source")
    }
    fn get_column_bindings(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &Self::Table,
    ) -> Result<Vec<ProviderReadColumnBinding<Self::Column>>, spi::ConnectorError> {
        unreachable!("COW component does not plan a provider read")
    }
    fn apply_filter(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &Self::Table,
        _: &spi::read_stack::Constraint<Self::Column>,
    ) -> ProviderReadFilterResult<Self::Table, Self::Column> {
        unreachable!("COW component does not plan a provider read")
    }
    fn apply_projection(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &Self::Table,
        _: &[spi::read_stack::Assignment<Self::Column>],
    ) -> Result<Option<Self::Table>, spi::ConnectorError> {
        unreachable!("COW component does not plan a provider read")
    }
    fn apply_limit(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &Self::Table,
        _: u64,
    ) -> Result<Option<ProviderReadLimitApplication<Self::Table>>, spi::ConnectorError> {
        unreachable!("COW component does not plan a provider read")
    }
    fn get_system_table_plan(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &spi::read_stack::SchemaTableName,
    ) -> Result<Option<ProviderReadSystemTablePlan<Self::Table>>, spi::ConnectorError> {
        unreachable!("COW component does not plan a provider read")
    }
    fn get_change_window_plan(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &spi::read_stack::SchemaTableName,
        _: spi::read_stack::ConnectorReadChangeWindow,
    ) -> Result<Option<Self::Table>, spi::ConnectorError> {
        unreachable!("COW component does not plan a provider read")
    }
    fn get_table_execute_plan(
        &self,
        _: &spi::read_stack::ConnectorSession,
        _: &spi::read_stack::SchemaTableName,
        _: spi::read_stack::ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<Self::Table>, spi::ConnectorError> {
        unreachable!("COW component does not plan a provider read")
    }
}
