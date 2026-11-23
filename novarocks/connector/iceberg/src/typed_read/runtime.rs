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
    ConnectorMergeTableHandle as _, ConnectorReadRelationKind, ConnectorSession, ConnectorSplit,
    ConnectorTableHandle as _, HostAddress, SchemaTableName, SessionPropertyValue, SplitWeight,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
};

use crate::catalog_config::{
    IcebergCatalogReadConfig, PARQUET_READ_BLOCK_ROW_COUNT_SESSION_PROPERTY,
    PARQUET_READ_BLOCK_SIZE_SESSION_PROPERTY, parse_parquet_read_block_size,
    validate_parquet_read_block_rows,
};

use super::{
    IcebergChangeSplit, IcebergChangeWindowHandle, IcebergMergeTableHandle,
    IcebergSystemTableReference, IcebergTableHandle, SystemManifestSplit, SystemRelationSplit,
    TableChangesFunctionHandle, TableChangesSplit,
};

#[cfg_attr(novarocks_dev_controls, derive(serde::Deserialize))]
#[cfg_attr(novarocks_dev_controls, serde(rename_all = "kebab-case"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum IcebergReadLayout {
    #[default]
    Offsets,
    View,
}

impl IcebergReadLayout {
    pub(crate) fn byte_carrier(
        self,
        data_type: &arrow::datatypes::DataType,
        domain: novarocks_type_contract::ValueLogicalType,
    ) -> arrow::datatypes::DataType {
        use arrow::datatypes::DataType;
        use novarocks_type_contract::ValueLogicalType;
        match (self, domain, data_type) {
            (Self::View, ValueLogicalType::Physical, DataType::Utf8) => DataType::Utf8View,
            (Self::View, ValueLogicalType::Physical, DataType::Binary) => DataType::BinaryView,
            _ => data_type.clone(),
        }
    }

    pub(crate) fn applies_to(relation: &IcebergRuntimeRelation) -> bool {
        match relation {
            IcebergRuntimeRelation::Table(table) => !table.is_data_rewrite_read(),
            IcebergRuntimeRelation::ChangeWindow(_) => true,
            _ => false,
        }
    }
}

/// A data rewrite reads storage values for its exact writer binding. Ordinary
/// reads publish SQL values and may choose a view layout for variable bytes.
#[derive(Clone, Copy, Debug)]
pub(crate) enum IcebergReadProjection {
    Sql(IcebergReadLayout),
    DataRewrite,
}

impl IcebergReadProjection {
    pub(crate) fn for_table(table: &IcebergTableHandle, layout: IcebergReadLayout) -> Self {
        if table.is_data_rewrite_read() {
            Self::DataRewrite
        } else {
            Self::Sql(layout)
        }
    }

    pub(crate) const fn projects_fixed_as_binary(self) -> bool {
        matches!(self, Self::Sql(_))
    }

    pub(crate) fn byte_carrier(
        self,
        data_type: &arrow::datatypes::DataType,
        domain: novarocks_type_contract::ValueLogicalType,
    ) -> arrow::datatypes::DataType {
        match self {
            Self::Sql(layout) => layout.byte_carrier(data_type, domain),
            Self::DataRewrite => data_type.clone(),
        }
    }
}

/// Execution settings frozen independently of the exact selected table.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IcebergReadExecution {
    pub(crate) sizing: IcebergCatalogReadConfig,
    pub(crate) layout: IcebergReadLayout,
    pub(crate) preserve_top_level_dictionary: bool,
}

impl IcebergReadExecution {
    pub(crate) fn from_catalog_session(
        defaults: IcebergCatalogReadConfig,
        session: &ConnectorSession,
    ) -> Result<Self, ConnectorError> {
        fn invalid(message: impl Into<String>) -> ConnectorError {
            ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
        }
        let max_read_rows = match session.property(PARQUET_READ_BLOCK_ROW_COUNT_SESSION_PROPERTY) {
            Some(SessionPropertyValue::Integer(rows)) => {
                validate_parquet_read_block_rows(*rows).map_err(invalid)?
            }
            Some(_) => return Err(invalid("Parquet read block row count must be INTEGER")),
            None => defaults.max_read_rows,
        };
        let max_read_bytes = match session.property(PARQUET_READ_BLOCK_SIZE_SESSION_PROPERTY) {
            Some(SessionPropertyValue::Varchar(size)) => {
                parse_parquet_read_block_size(size).map_err(invalid)?
            }
            Some(_) => return Err(invalid("Parquet read block size must be VARCHAR")),
            None => defaults.max_read_bytes,
        };
        Ok(Self {
            sizing: IcebergCatalogReadConfig {
                max_read_rows,
                max_read_bytes,
            },
            layout: IcebergReadLayout::Offsets,
            preserve_top_level_dictionary: false,
        })
    }
}

#[derive(Clone, Debug)]
pub enum IcebergRuntimeRelation {
    Table(IcebergTableHandle),
    TableFunction(TableChangesFunctionHandle),
    ChangeWindow(IcebergChangeWindowHandle),
    SystemTable(IcebergSystemTableReference),
    MergeTable(IcebergMergeTableHandle),
}

impl IcebergRuntimeRelation {
    pub const fn kind(&self) -> ConnectorReadRelationKind {
        match self {
            Self::Table(_) => ConnectorReadRelationKind::Table,
            Self::TableFunction(_) => ConnectorReadRelationKind::TableFunction,
            Self::ChangeWindow(_) => ConnectorReadRelationKind::ChangeWindow,
            Self::SystemTable(_) => ConnectorReadRelationKind::SystemTable,
            Self::MergeTable(_) => ConnectorReadRelationKind::MergeTable,
        }
    }

    pub(crate) fn schema_table_name(&self) -> &SchemaTableName {
        match self {
            Self::Table(handle) => handle.schema_table_name(),
            Self::TableFunction(handle) => handle.schema_table_name(),
            Self::ChangeWindow(handle) => handle.schema_table_name(),
            Self::SystemTable(handle) => handle.schema_table_name(),
            Self::MergeTable(handle) => handle.schema_table_name(),
        }
    }

    pub(crate) fn read_projection(&self, layout: IcebergReadLayout) -> IcebergReadProjection {
        match self {
            Self::Table(table) => IcebergReadProjection::for_table(table, layout),
            _ => IcebergReadProjection::Sql(layout),
        }
    }
}

#[derive(Clone, Debug)]
pub enum IcebergReadSplit {
    Data(IcebergTableHandleSplit),
    TableChanges(TableChangesSplit),
    ChangeWindow(IcebergChangeSplit),
    SystemManifest(SystemManifestSplit),
    SystemRelation(SystemRelationSplit),
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
    type Execution = IcebergReadExecution;
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
        self.transaction
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
            Self::SystemManifest(split) => split.is_remotely_accessible(),
            Self::SystemRelation(split) => split.is_remotely_accessible(),
        }
    }

    fn addresses(&self) -> &[HostAddress] {
        match self {
            Self::Data(split) => split.addresses(),
            Self::TableChanges(split) => split.addresses(),
            Self::ChangeWindow(split) => split.addresses(),
            Self::SystemManifest(split) => split.addresses(),
            Self::SystemRelation(split) => split.addresses(),
        }
    }

    fn affinity_key(&self) -> Option<&str> {
        match self {
            Self::Data(split) => split.affinity_key(),
            Self::TableChanges(split) => split.affinity_key(),
            Self::ChangeWindow(split) => split.affinity_key(),
            Self::SystemManifest(split) => split.affinity_key(),
            Self::SystemRelation(split) => split.affinity_key(),
        }
    }

    fn split_weight(&self) -> SplitWeight {
        match self {
            Self::Data(split) => split.split_weight(),
            Self::TableChanges(split) => split.split_weight(),
            Self::ChangeWindow(split) => split.split_weight(),
            Self::SystemManifest(split) => split.split_weight(),
            Self::SystemRelation(split) => split.split_weight(),
        }
    }

    fn retained_size_in_bytes(&self) -> u64 {
        match self {
            Self::Data(split) => split.retained_size_in_bytes(),
            Self::TableChanges(split) => split.retained_size_in_bytes(),
            Self::ChangeWindow(split) => split.retained_size_in_bytes(),
            Self::SystemManifest(split) => split.retained_size_in_bytes(),
            Self::SystemRelation(split) => split.retained_size_in_bytes(),
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

#[cfg(test)]
mod read_settings_tests {
    use std::sync::Arc;

    use super::*;

    fn session(
        properties: impl IntoIterator<Item = (&'static str, SessionPropertyValue)>,
    ) -> ConnectorSession {
        ConnectorSession::try_new(
            "reader",
            "fixture",
            "UTC",
            "en",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .unwrap()
        .try_with_properties(
            properties
                .into_iter()
                .map(|(k, v)| (Arc::from(k), v))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn exact_catalog_defaults_and_overrides_are_independent() {
        let defaults = IcebergCatalogReadConfig {
            max_read_rows: 512,
            max_read_bytes: 1024,
        };
        let ordinary = IcebergReadExecution::from_catalog_session(defaults, &session([])).unwrap();
        assert_eq!(ordinary.sizing, defaults);
        assert_eq!(ordinary.layout, IcebergReadLayout::Offsets);
        assert!(!ordinary.preserve_top_level_dictionary);

        let rows = IcebergReadExecution::from_catalog_session(
            defaults,
            &session([(
                PARQUET_READ_BLOCK_ROW_COUNT_SESSION_PROPERTY,
                SessionPropertyValue::Integer(128),
            )]),
        )
        .unwrap();
        assert_eq!(
            rows.sizing,
            IcebergCatalogReadConfig {
                max_read_rows: 128,
                ..defaults
            }
        );
        let bytes = IcebergReadExecution::from_catalog_session(
            defaults,
            &session([(
                PARQUET_READ_BLOCK_SIZE_SESSION_PROPERTY,
                SessionPropertyValue::Varchar("0B".into()),
            )]),
        )
        .unwrap();
        assert_eq!(
            bytes.sizing,
            IcebergCatalogReadConfig {
                max_read_bytes: 0,
                ..defaults
            }
        );
        let reset = IcebergReadExecution::from_catalog_session(defaults, &session([])).unwrap();
        assert_eq!(reset, ordinary);
    }

    #[test]
    fn session_execution_refuses_values_of_the_wrong_type_or_catalog_range() {
        for (key, value) in [
            (
                PARQUET_READ_BLOCK_ROW_COUNT_SESSION_PROPERTY,
                SessionPropertyValue::Integer(127),
            ),
            (
                PARQUET_READ_BLOCK_ROW_COUNT_SESSION_PROPERTY,
                SessionPropertyValue::Varchar("8192".into()),
            ),
            (
                PARQUET_READ_BLOCK_SIZE_SESSION_PROPERTY,
                SessionPropertyValue::Integer(16),
            ),
            (
                PARQUET_READ_BLOCK_SIZE_SESSION_PROPERTY,
                SessionPropertyValue::Varchar("9223372036854775808B".into()),
            ),
        ] {
            let error = IcebergReadExecution::from_catalog_session(
                IcebergCatalogReadConfig::default(),
                &session([(key, value)]),
            )
            .unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        }
    }
}
