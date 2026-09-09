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

//! Validated public carriers for connector read views and relations.
//!
//! These wrappers validate only the common encoded envelope. Provider table,
//! transaction, and relation semantics remain inside the selected provider's
//! private codec.

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::{CatalogHandle, ConnectorCodecCategory, ConnectorEncodedPayload};

use crate::catalog::decode_catalog_handle;
use crate::{FieldPath, ProtocolError};

use super::{MAX_NAME_BYTES, MAX_SPLIT_ENCODED_BYTES, bounded_text, missing};

fn decode_payload(
    raw: Option<&novarocks_proto_models::connector_common::ConnectorEncodedPayload>,
    category: ConnectorCodecCategory,
    path: FieldPath,
) -> Result<ConnectorEncodedPayload, ProtocolError> {
    crate::connector_common::decode_embedded_connector_payload(
        raw,
        category,
        MAX_SPLIT_ENCODED_BYTES,
        path,
    )
}

macro_rules! provider_payload_wrapper {
    ($name:ident, $raw:ty, $category:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            provider_payload: ConnectorEncodedPayload,
        }

        impl $name {
            pub fn parse(raw: $raw, path: FieldPath) -> Result<Self, ProtocolError> {
                let provider_payload = decode_payload(
                    raw.provider_payload.as_ref(),
                    $category,
                    path.field("provider_payload"),
                )?;
                Ok(Self { provider_payload })
            }

            pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
                &self.provider_payload
            }

            pub fn into_proto(self) -> $raw {
                let mut raw: $raw = Default::default();
                raw.provider_payload =
                    Some(crate::connector_common::encode_connector_payload_message(
                        &self.provider_payload,
                    ));
                raw
            }
        }
    };
}

provider_payload_wrapper!(
    ValidatedTransactionHandle,
    dto::ConnectorTransactionHandle,
    ConnectorCodecCategory::ReadView
);
provider_payload_wrapper!(
    ValidatedConnectorTableHandle,
    dto::ConnectorTableHandle,
    ConnectorCodecCategory::ReadTable
);
provider_payload_wrapper!(
    ValidatedConnectorTableFunctionHandle,
    dto::ConnectorTableFunctionHandle,
    ConnectorCodecCategory::ReadTable
);
provider_payload_wrapper!(
    ValidatedConnectorChangeWindowHandle,
    dto::ConnectorChangeWindowHandle,
    ConnectorCodecCategory::ReadTable
);
provider_payload_wrapper!(
    ValidatedConnectorSystemTableReference,
    dto::ConnectorSystemTableReference,
    ConnectorCodecCategory::ReadTable
);
provider_payload_wrapper!(
    ValidatedConnectorTableExecuteHandle,
    dto::ConnectorTableExecuteHandle,
    ConnectorCodecCategory::ReadTable
);
provider_payload_wrapper!(
    ValidatedConnectorMergeTableHandle,
    dto::ConnectorMergeTableHandle,
    ConnectorCodecCategory::ReadTable
);

/// Compatibility export while procedure details move behind ReadTable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableExecuteProcedure {}

/// Public routing category. Relation contents are provider-private.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorRelationKind {
    Table,
    TableFunction,
    ChangeWindow,
    SystemTable,
    TableExecute,
    MergeTable,
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectorRelation<'a> {
    Table(&'a ValidatedConnectorTableHandle),
    TableFunction(&'a ValidatedConnectorTableFunctionHandle),
    ChangeWindow(&'a ValidatedConnectorChangeWindowHandle),
    SystemTable(&'a ValidatedConnectorSystemTableReference),
    TableExecute(&'a ValidatedConnectorTableExecuteHandle),
    MergeTable(&'a ValidatedConnectorMergeTableHandle),
}

impl ConnectorRelation<'_> {
    pub const fn kind(self) -> ConnectorRelationKind {
        match self {
            Self::Table(_) => ConnectorRelationKind::Table,
            Self::TableFunction(_) => ConnectorRelationKind::TableFunction,
            Self::ChangeWindow(_) => ConnectorRelationKind::ChangeWindow,
            Self::SystemTable(_) => ConnectorRelationKind::SystemTable,
            Self::TableExecute(_) => ConnectorRelationKind::TableExecute,
            Self::MergeTable(_) => ConnectorRelationKind::MergeTable,
        }
    }
}

impl<'a> ConnectorRelation<'a> {
    pub const fn provider_payload(self) -> &'a ConnectorEncodedPayload {
        match self {
            Self::Table(value) => value.provider_payload(),
            Self::TableFunction(value) => value.provider_payload(),
            Self::ChangeWindow(value) => value.provider_payload(),
            Self::SystemTable(value) => value.provider_payload(),
            Self::TableExecute(value) => value.provider_payload(),
            Self::MergeTable(value) => value.provider_payload(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OwnedConnectorRelation {
    Table(ValidatedConnectorTableHandle),
    TableFunction(ValidatedConnectorTableFunctionHandle),
    ChangeWindow(ValidatedConnectorChangeWindowHandle),
    SystemTable(ValidatedConnectorSystemTableReference),
    TableExecute(ValidatedConnectorTableExecuteHandle),
    MergeTable(ValidatedConnectorMergeTableHandle),
}

impl OwnedConnectorRelation {
    const fn borrowed(&self) -> ConnectorRelation<'_> {
        match self {
            Self::Table(value) => ConnectorRelation::Table(value),
            Self::TableFunction(value) => ConnectorRelation::TableFunction(value),
            Self::ChangeWindow(value) => ConnectorRelation::ChangeWindow(value),
            Self::SystemTable(value) => ConnectorRelation::SystemTable(value),
            Self::TableExecute(value) => ConnectorRelation::TableExecute(value),
            Self::MergeTable(value) => ConnectorRelation::MergeTable(value),
        }
    }
}

/// Catalog identity and public relation category surrounding independent
/// provider-private ReadView and ReadTable payloads.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTableHandle {
    raw: dto::CatalogTableHandle,
    catalog_handle: CatalogHandle,
    transaction: ValidatedTransactionHandle,
    relation: OwnedConnectorRelation,
}

impl CatalogTableHandle {
    pub fn parse(raw: dto::CatalogTableHandle, path: FieldPath) -> Result<Self, ProtocolError> {
        let raw_catalog_handle = raw.catalog_handle.clone().ok_or_else(|| {
            missing(
                path.clone().field("catalog_handle"),
                "catalog table handle requires a catalog handle",
            )
        })?;
        bounded_text(
            &raw_catalog_handle.catalog_name,
            MAX_NAME_BYTES,
            path.clone().field("catalog_handle").field("catalog_name"),
            false,
        )?;
        let catalog_handle =
            decode_catalog_handle(raw_catalog_handle, path.clone().field("catalog_handle"))?;
        let transaction = ValidatedTransactionHandle::parse(
            raw.transaction.clone().ok_or_else(|| {
                missing(
                    path.clone().field("transaction"),
                    "catalog table handle requires a transaction view",
                )
            })?,
            path.clone().field("transaction"),
        )?;
        let relation = match raw.relation.clone().ok_or_else(|| {
            missing(
                path.clone().field("relation"),
                "catalog table handle relation must be present",
            )
        })? {
            dto::catalog_table_handle::Relation::Table(value) => OwnedConnectorRelation::Table(
                ValidatedConnectorTableHandle::parse(value, path.clone().field("table"))?,
            ),
            dto::catalog_table_handle::Relation::TableFunction(value) => {
                OwnedConnectorRelation::TableFunction(ValidatedConnectorTableFunctionHandle::parse(
                    value,
                    path.clone().field("table_function"),
                )?)
            }
            dto::catalog_table_handle::Relation::ChangeWindow(value) => {
                OwnedConnectorRelation::ChangeWindow(ValidatedConnectorChangeWindowHandle::parse(
                    value,
                    path.clone().field("change_window"),
                )?)
            }
            dto::catalog_table_handle::Relation::SystemTable(value) => {
                OwnedConnectorRelation::SystemTable(ValidatedConnectorSystemTableReference::parse(
                    value,
                    path.clone().field("system_table"),
                )?)
            }
            dto::catalog_table_handle::Relation::TableExecute(value) => {
                OwnedConnectorRelation::TableExecute(ValidatedConnectorTableExecuteHandle::parse(
                    value,
                    path.clone().field("table_execute"),
                )?)
            }
            dto::catalog_table_handle::Relation::MergeTable(value) => {
                OwnedConnectorRelation::MergeTable(ValidatedConnectorMergeTableHandle::parse(
                    value,
                    path.clone().field("merge_table"),
                )?)
            }
        };
        Ok(Self {
            raw,
            catalog_handle,
            transaction,
            relation,
        })
    }

    pub const fn as_proto(&self) -> &dto::CatalogTableHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::CatalogTableHandle {
        self.raw
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    pub fn catalog_name(&self) -> &str {
        self.catalog_handle.catalog_name().as_str()
    }

    pub const fn transaction(&self) -> &ValidatedTransactionHandle {
        &self.transaction
    }

    pub const fn relation(&self) -> ConnectorRelation<'_> {
        self.relation.borrowed()
    }

    pub const fn relation_kind(&self) -> ConnectorRelationKind {
        self.relation().kind()
    }
}
