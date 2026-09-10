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

//! Connector-owned read values behind the internal SPI handles.
//!
//! These enums deliberately carry the concrete Iceberg values, never their
//! protobuf representation.  The codec is the only place that converts them
//! to or from the central closed wire family.

use novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime;
use novarocks_spi::connector::read_stack::{
    ConnectorMergeTableHandle as _, ConnectorReadRelationKind, ConnectorSplit,
    ConnectorTableExecuteHandle as _, ConnectorTableHandle as _, HostAddress, SchemaTableName,
    SplitWeight,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorInstanceDescriptor};

use super::{
    FilesTableSplit, IcebergChangeSplit, IcebergChangeWindowHandle, IcebergMergeTableHandle,
    IcebergRewritePositionDeleteFilesSplit, IcebergSystemTableReference, IcebergTableExecuteHandle,
    IcebergTableHandle, TableChangesFunctionHandle, TableChangesSplit,
};

#[derive(Clone, Debug)]
pub enum IcebergRuntimeRelation {
    Table(IcebergTableHandle),
    TableFunction(TableChangesFunctionHandle),
    ChangeWindow(IcebergChangeWindowHandle),
    SystemTable(IcebergSystemTableReference),
    TableExecute(IcebergTableExecuteHandle),
    MergeTable(IcebergMergeTableHandle),
}

impl IcebergRuntimeRelation {
    pub const fn kind(&self) -> ConnectorReadRelationKind {
        match self {
            Self::Table(_) => ConnectorReadRelationKind::Table,
            Self::TableFunction(_) => ConnectorReadRelationKind::TableFunction,
            Self::ChangeWindow(_) => ConnectorReadRelationKind::ChangeWindow,
            Self::SystemTable(_) => ConnectorReadRelationKind::SystemTable,
            Self::TableExecute(_) => ConnectorReadRelationKind::TableExecute,
            Self::MergeTable(_) => ConnectorReadRelationKind::MergeTable,
        }
    }

    pub(crate) fn schema_table_name(&self) -> &SchemaTableName {
        match self {
            Self::Table(handle) => handle.schema_table_name(),
            Self::TableFunction(handle) => handle.schema_table_name(),
            Self::ChangeWindow(handle) => handle.schema_table_name(),
            Self::SystemTable(handle) => handle.schema_table_name(),
            Self::TableExecute(handle) => handle.schema_table_name(),
            Self::MergeTable(handle) => handle.schema_table_name(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum IcebergReadSplit {
    Data(IcebergTableHandleSplit),
    TableChanges(TableChangesSplit),
    ChangeWindow(IcebergChangeSplit),
    SystemFiles(FilesTableSplit),
    RewritePositionDeleteFiles(IcebergRewritePositionDeleteFilesSplit),
}

/// The backend-only half of Iceberg's read type family.
///
/// It carries only the exact immutable catalog identity and the frozen
/// transaction marker required to make opaque SPI handles coherent with the
/// selected codec. It deliberately does not hold an `IcebergMetadataContext`,
/// catalog client, table cache, or any other frontend planning authority.
#[derive(Clone, Debug)]
pub struct IcebergExecutionReadRuntime {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
    transaction: super::HiveTransactionHandle,
}

impl IcebergExecutionReadRuntime {
    pub const fn new(
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
        transaction: super::HiveTransactionHandle,
    ) -> Self {
        Self {
            descriptor,
            catalog_handle,
            transaction,
        }
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

impl ProviderReadRuntime for IcebergExecutionReadRuntime {
    type Table = IcebergRuntimeRelation;
    type Column = super::IcebergColumnHandle;
    type Transaction = super::HiveTransactionHandle;
    type Split = IcebergReadSplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    fn transaction(&self) -> Self::Transaction {
        self.transaction.clone()
    }
}

/// A local alias keeps the enum's public spelling focused on the runtime
/// family while retaining the established concrete split type.
pub type IcebergTableHandleSplit = super::IcebergSplit;

impl ConnectorSplit for IcebergReadSplit {
    fn is_remotely_accessible(&self) -> bool {
        match self {
            Self::Data(split) => split.is_remotely_accessible(),
            Self::TableChanges(split) => split.is_remotely_accessible(),
            Self::ChangeWindow(split) => split.is_remotely_accessible(),
            Self::SystemFiles(split) => split.is_remotely_accessible(),
            Self::RewritePositionDeleteFiles(split) => split.is_remotely_accessible(),
        }
    }

    fn addresses(&self) -> &[HostAddress] {
        match self {
            Self::Data(split) => split.addresses(),
            Self::TableChanges(split) => split.addresses(),
            Self::ChangeWindow(split) => split.addresses(),
            Self::SystemFiles(split) => split.addresses(),
            Self::RewritePositionDeleteFiles(split) => split.addresses(),
        }
    }

    fn affinity_key(&self) -> Option<&str> {
        match self {
            Self::Data(split) => split.affinity_key(),
            Self::TableChanges(split) => split.affinity_key(),
            Self::ChangeWindow(split) => split.affinity_key(),
            Self::SystemFiles(split) => split.affinity_key(),
            Self::RewritePositionDeleteFiles(split) => split.affinity_key(),
        }
    }

    fn split_weight(&self) -> SplitWeight {
        match self {
            Self::Data(split) => split.split_weight(),
            Self::TableChanges(split) => split.split_weight(),
            Self::ChangeWindow(split) => split.split_weight(),
            Self::SystemFiles(split) => split.split_weight(),
            Self::RewritePositionDeleteFiles(split) => split.split_weight(),
        }
    }

    fn retained_size_in_bytes(&self) -> u64 {
        match self {
            Self::Data(split) => split.retained_size_in_bytes(),
            Self::TableChanges(split) => split.retained_size_in_bytes(),
            Self::ChangeWindow(split) => split.retained_size_in_bytes(),
            Self::SystemFiles(split) => split.retained_size_in_bytes(),
            Self::RewritePositionDeleteFiles(split) => split.retained_size_in_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId, ConnectorProviderId};

    #[test]
    fn execution_runtime_retains_the_full_catalog_version() {
        let catalog_handle = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake.analytics")
                .expect("canonical catalog name"),
            CatalogVersion::from_bytes(std::array::from_fn(|index| index as u8)),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("static provider"),
            instance_id: catalog_handle.catalog_name().clone(),
        };
        let runtime = IcebergExecutionReadRuntime::new(
            descriptor,
            catalog_handle.clone(),
            super::super::HiveTransactionHandle::new(true, [0; 16]),
        );

        assert_eq!(runtime.catalog_handle(), &catalog_handle);
        assert_eq!(
            runtime.catalog_handle().version().as_bytes(),
            &std::array::from_fn(|index| index as u8),
        );
    }
}
