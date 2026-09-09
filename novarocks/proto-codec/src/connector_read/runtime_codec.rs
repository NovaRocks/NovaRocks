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

//! The wire codec boundary for transport-neutral connector reads.
//!
//! A codec is selected as part of an exact installed connector binding.  It
//! is the only boundary that may turn a validated closed carrier into an SPI
//! runtime handle or turn such a handle back into a central-IDL message.
//! Metadata, split enumeration, reader creation, registries, and lifecycle
//! state deliberately do not live here.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{
    Assignment, ConnectorReadColumnHandle, ConnectorReadRelation, ConnectorReadSplit,
    ConnectorReadTransactionHandle, ConnectorReadWorkSource, TupleDomain,
};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorReadRelationPayload,
    ConnectorReadSplitCategory, ConnectorReadSplitPayload, ConnectorReadWireDecoder,
    ConnectorReadWireEncoder,
};

use super::{
    CatalogTableHandle, ConnectorTableScanSource, ScheduledSplit, ValidatedColumnHandle,
    ValidatedConnectorSplit, ValidatedTransactionHandle, decode_tuple_domain, encode_tuple_domain,
    encode_value_type,
};

/// A codec error keeps the original wire field path and adds the binding owner
/// that selected this codec.  It never stores arbitrary provider payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadCodecError {
    owner: Arc<str>,
    protocol: ProtocolError,
}

impl ConnectorReadCodecError {
    pub fn new(owner: impl AsRef<str>, protocol: ProtocolError) -> Self {
        Self {
            owner: Arc::from(owner.as_ref()),
            protocol,
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub const fn protocol(&self) -> &ProtocolError {
        &self.protocol
    }

    pub fn invalid(owner: impl AsRef<str>, path: FieldPath, detail: impl Into<String>) -> Self {
        Self::new(
            owner,
            ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail),
        )
    }
}

impl fmt::Display for ConnectorReadCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "connector read codec '{}' rejected: {}",
            self.owner, self.protocol
        )
    }
}

impl std::error::Error for ConnectorReadCodecError {}

/// FE-to-wire half of the connector read codec contract.
///
/// An encoder can create only carrier values.  It has no methods that turn
/// untrusted carrier data into provider-owned runtime handles.
pub trait ConnectorReadEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_relation(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError>;

    fn encode_column(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError>;

    fn encode_transaction(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError>;

    fn encode_split(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError>;

    fn encode_tuple_domain(
        &self,
        domain: &TupleDomain<ConnectorReadColumnHandle>,
        path: FieldPath,
    ) -> Result<dto::TupleDomain, ConnectorReadCodecError> {
        let Some(domains) = domain.domains() else {
            return Ok(encode_tuple_domain(&TupleDomain::none()));
        };
        let mut validated = BTreeMap::new();
        for (column, value) in domains {
            let raw = self.encode_column(column)?;
            let raw = ValidatedColumnHandle::parse(raw, path.clone().field("column"))
                .map_err(|error| ConnectorReadCodecError::new(self.owner(), error))?;
            validated.insert(raw, value.clone());
        }
        Ok(encode_tuple_domain(
            &TupleDomain::with_column_domains(validated).map_err(|error| {
                ConnectorReadCodecError::invalid(self.owner(), path, error.to_string())
            })?,
        ))
    }

    fn encode_assignment(
        &self,
        assignment: &Assignment<ConnectorReadColumnHandle>,
        _path: FieldPath,
    ) -> Result<dto::ScanAssignment, ConnectorReadCodecError> {
        Ok(dto::ScanAssignment {
            variable: assignment.variable().to_owned(),
            column: Some(self.encode_column(assignment.column())?),
            value_type: Some(encode_value_type(assignment.value_type())),
        })
    }

    fn encode_scheduled_split(
        &self,
        sequence_id: u64,
        plan_node_id: i32,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ScheduledSplit, ConnectorReadCodecError> {
        Ok(dto::ScheduledSplit {
            sequence_id,
            plan_node_id,
            split: Some(self.encode_split(split)?),
        })
    }
}

impl<T> ConnectorReadEncoder for T
where
    T: ConnectorReadWireEncoder + ?Sized,
{
    fn owner(&self) -> &str {
        ConnectorReadWireEncoder::owner(self)
    }

    fn encode_relation(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<dto::CatalogTableHandle, ConnectorReadCodecError> {
        let payload = self
            .encode_relation_payload(relation)
            .map_err(|error| wire_error(self.owner(), error))?;
        validate_encoded_relation(self.owner(), &payload)?;
        let provider_payload = Some(crate::connector_common::encode_connector_payload_message(
            payload.table(),
        ));
        let relation = match payload.kind() {
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table => {
                dto::catalog_table_handle::Relation::Table(dto::ConnectorTableHandle {
                    provider_payload,
                })
            }
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::TableFunction => {
                dto::catalog_table_handle::Relation::TableFunction(
                    dto::ConnectorTableFunctionHandle { provider_payload },
                )
            }
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::ChangeWindow => {
                dto::catalog_table_handle::Relation::ChangeWindow(
                    dto::ConnectorChangeWindowHandle { provider_payload },
                )
            }
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::SystemTable => {
                dto::catalog_table_handle::Relation::SystemTable(
                    dto::ConnectorSystemTableReference { provider_payload },
                )
            }
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::TableExecute => {
                dto::catalog_table_handle::Relation::TableExecute(
                    dto::ConnectorTableExecuteHandle { provider_payload },
                )
            }
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::MergeTable => {
                dto::catalog_table_handle::Relation::MergeTable(dto::ConnectorMergeTableHandle {
                    provider_payload,
                })
            }
        };
        Ok(dto::CatalogTableHandle {
            catalog_handle: Some(crate::catalog::encode_catalog_handle(
                payload.table().header().catalog(),
            )),
            transaction: Some(dto::ConnectorTransactionHandle {
                provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                    payload.view(),
                )),
            }),
            relation: Some(relation),
        })
    }

    fn encode_column(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<dto::ColumnHandle, ConnectorReadCodecError> {
        let payload = self
            .encode_column_payload(column)
            .map_err(|error| wire_error(self.owner(), error))?;
        Ok(dto::ColumnHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        })
    }

    fn encode_transaction(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<dto::ConnectorTransactionHandle, ConnectorReadCodecError> {
        let payload = self
            .encode_transaction_payload(transaction)
            .map_err(|error| wire_error(self.owner(), error))?;
        Ok(dto::ConnectorTransactionHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        })
    }

    fn encode_split(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<dto::ConnectorSplit, ConnectorReadCodecError> {
        let payload = self
            .encode_split_payload(split)
            .map_err(|error| wire_error(self.owner(), error))?;
        let provider_payload = Some(crate::connector_common::encode_connector_payload_message(
            payload.provider_payload(),
        ));
        let category = match payload.category() {
            ConnectorReadSplitCategory::Data => {
                dto::connector_split::Category::Data(dto::DataSplit { provider_payload })
            }
            ConnectorReadSplitCategory::TableChanges => {
                dto::connector_split::Category::TableChanges(dto::TableChangesSplitCategory {
                    provider_payload,
                })
            }
            ConnectorReadSplitCategory::ChangeWindow => {
                dto::connector_split::Category::ChangeWindow(dto::ChangeWindowSplitCategory {
                    provider_payload,
                })
            }
            ConnectorReadSplitCategory::SystemFiles => {
                dto::connector_split::Category::SystemFiles(dto::SystemFilesSplitCategory {
                    provider_payload,
                })
            }
            ConnectorReadSplitCategory::RewritePositionDeleteFiles => {
                dto::connector_split::Category::RewritePositionDeleteFiles(
                    dto::RewritePositionDeleteFilesSplitCategory { provider_payload },
                )
            }
        };
        let facts = split.facts();
        Ok(dto::ConnectorSplit {
            split_weight_raw: facts.split_weight().raw_value(),
            remotely_accessible: facts.remotely_accessible(),
            addresses: facts
                .addresses()
                .iter()
                .map(|address| dto::HostAddress {
                    host: address.host().to_owned(),
                    port: u32::from(address.port()),
                })
                .collect(),
            affinity_key: facts.affinity_key().map(ToOwned::to_owned),
            retained_size_in_bytes: facts.retained_size_in_bytes(),
            category: Some(category),
        })
    }
}

/// Wire-to-BE half of the connector read codec contract.
///
/// A decoder is the only directional interface allowed to construct opaque
/// provider handles from a validated carrier.
pub trait ConnectorReadDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError>;

    fn decode_column(
        &self,
        column: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError>;

    fn decode_transaction(
        &self,
        transaction: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError>;

    fn decode_split(
        &self,
        split: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError>;

    fn decode_tuple_domain(
        &self,
        domain: &dto::TupleDomain,
        path: FieldPath,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        let validated = decode_tuple_domain(domain, path)
            .map_err(|error| ConnectorReadCodecError::new(self.owner(), error))?;
        let Some(domains) = validated.domains() else {
            return Ok(TupleDomain::none());
        };
        let mut decoded = BTreeMap::new();
        for (column, value) in domains {
            decoded.insert(self.decode_column(column)?, value.clone());
        }
        TupleDomain::with_column_domains(decoded).map_err(|error| {
            ConnectorReadCodecError::invalid(
                self.owner(),
                FieldPath::root("tuple_domain"),
                error.to_string(),
            )
        })
    }

    fn decode_validated_tuple_domain(
        &self,
        domain: &TupleDomain<ValidatedColumnHandle>,
    ) -> Result<TupleDomain<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        let Some(domains) = domain.domains() else {
            return Ok(TupleDomain::none());
        };
        let mut decoded = BTreeMap::new();
        for (column, value) in domains {
            decoded.insert(self.decode_column(column)?, value.clone());
        }
        TupleDomain::with_column_domains(decoded).map_err(|error| {
            ConnectorReadCodecError::invalid(
                self.owner(),
                FieldPath::root("tuple_domain"),
                error.to_string(),
            )
        })
    }

    fn decode_assignment(
        &self,
        assignment: &dto::ScanAssignment,
        path: FieldPath,
    ) -> Result<Assignment<ConnectorReadColumnHandle>, ConnectorReadCodecError> {
        let validated = super::ScanAssignment::parse(assignment.clone(), path.clone())
            .map_err(|error| ConnectorReadCodecError::new(self.owner(), error))?;
        Assignment::try_new(
            validated.variable(),
            self.decode_column(validated.column())?,
            validated.value_type(),
        )
        .map_err(|error| ConnectorReadCodecError::invalid(self.owner(), path, error.to_string()))
    }

    fn decode_scheduled_split(
        &self,
        scheduled: &ScheduledSplit,
    ) -> Result<DecodedScheduledReadSplit, ConnectorReadCodecError> {
        Ok(DecodedScheduledReadSplit::new(
            ReceivedScheduledSplit::from_scheduled(scheduled),
            self.decode_split(scheduled.split())?,
        ))
    }
}

impl<T> ConnectorReadDecoder for T
where
    T: ConnectorReadWireDecoder + ?Sized,
{
    fn owner(&self) -> &str {
        ConnectorReadWireDecoder::owner(self)
    }

    fn decode_relation(
        &self,
        relation: &CatalogTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorReadCodecError> {
        let kind = match relation.relation_kind() {
            super::ConnectorRelationKind::Table => {
                novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table
            }
            super::ConnectorRelationKind::TableFunction => {
                novarocks_spi::connector::read_stack::ConnectorReadRelationKind::TableFunction
            }
            super::ConnectorRelationKind::ChangeWindow => {
                novarocks_spi::connector::read_stack::ConnectorReadRelationKind::ChangeWindow
            }
            super::ConnectorRelationKind::SystemTable => {
                novarocks_spi::connector::read_stack::ConnectorReadRelationKind::SystemTable
            }
            super::ConnectorRelationKind::TableExecute => {
                novarocks_spi::connector::read_stack::ConnectorReadRelationKind::TableExecute
            }
            super::ConnectorRelationKind::MergeTable => {
                novarocks_spi::connector::read_stack::ConnectorReadRelationKind::MergeTable
            }
        };
        let payload = ConnectorReadRelationPayload::new(
            kind,
            relation.relation().provider_payload().clone(),
            relation.transaction().provider_payload().clone(),
        );
        validate_decoded_relation(self.owner(), relation, &payload)?;
        self.decode_relation_payload(&payload)
            .map_err(|error| wire_error(self.owner(), error))
    }

    fn decode_column(
        &self,
        column: &ValidatedColumnHandle,
    ) -> Result<ConnectorReadColumnHandle, ConnectorReadCodecError> {
        self.decode_column_payload(column.provider_payload())
            .map_err(|error| wire_error(self.owner(), error))
    }

    fn decode_transaction(
        &self,
        transaction: &ValidatedTransactionHandle,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorReadCodecError> {
        self.decode_transaction_payload(transaction.provider_payload())
            .map_err(|error| wire_error(self.owner(), error))
    }

    fn decode_split(
        &self,
        split: &ValidatedConnectorSplit,
    ) -> Result<ConnectorReadSplit, ConnectorReadCodecError> {
        let category = match split.category() {
            super::SplitCategory::Data => ConnectorReadSplitCategory::Data,
            super::SplitCategory::TableChanges => ConnectorReadSplitCategory::TableChanges,
            super::SplitCategory::ChangeWindow => ConnectorReadSplitCategory::ChangeWindow,
            super::SplitCategory::SystemFiles => ConnectorReadSplitCategory::SystemFiles,
            super::SplitCategory::RewritePositionDeleteFiles => {
                ConnectorReadSplitCategory::RewritePositionDeleteFiles
            }
        };
        self.decode_split_payload(
            &ConnectorReadSplitPayload::new(category, split.provider_payload().clone()),
            split.facts(),
        )
        .map_err(|error| wire_error(self.owner(), error))
    }
}

fn validate_encoded_relation(
    owner: &str,
    payload: &ConnectorReadRelationPayload,
) -> Result<(), ConnectorReadCodecError> {
    let table = payload.table().header();
    let view = payload.view().header();
    if table.category() != novarocks_spi::connector::ConnectorCodecCategory::ReadTable
        || view.category() != novarocks_spi::connector::ConnectorCodecCategory::ReadView
        || table.provider_id() != view.provider_id()
        || table.catalog() != view.catalog()
    {
        return Err(ConnectorReadCodecError::invalid(
            owner,
            FieldPath::root("catalog_table_handle"),
            "provider relation payloads disagree on category or binding",
        ));
    }
    Ok(())
}

fn validate_decoded_relation(
    owner: &str,
    relation: &CatalogTableHandle,
    payload: &ConnectorReadRelationPayload,
) -> Result<(), ConnectorReadCodecError> {
    validate_encoded_relation(owner, payload)?;
    if relation.catalog_handle() != payload.table().header().catalog() {
        return Err(ConnectorReadCodecError::invalid(
            owner,
            FieldPath::root("catalog_table_handle").field("catalog_handle"),
            "public catalog handle does not match provider relation payloads",
        ));
    }
    Ok(())
}

fn wire_error(owner: &str, error: ConnectorCodecError) -> ConnectorReadCodecError {
    let kind = match error.kind() {
        ConnectorCodecErrorKind::MissingField => ProtocolErrorKind::MissingField,
        ConnectorCodecErrorKind::InvalidEnum => ProtocolErrorKind::InvalidEnum,
        ConnectorCodecErrorKind::InvalidValue | ConnectorCodecErrorKind::UnknownField => {
            ProtocolErrorKind::InvalidValue
        }
        ConnectorCodecErrorKind::DuplicateField => ProtocolErrorKind::DuplicateField,
        ConnectorCodecErrorKind::InconsistentFields => ProtocolErrorKind::InconsistentFields,
        ConnectorCodecErrorKind::Unsupported => ProtocolErrorKind::Unsupported,
        ConnectorCodecErrorKind::Capacity => ProtocolErrorKind::Capacity,
        ConnectorCodecErrorKind::VersionMismatch => ProtocolErrorKind::VersionMismatch,
    };
    ConnectorReadCodecError::new(
        owner,
        ProtocolError::new(
            FieldPath::root("provider_payload"),
            kind,
            format!("{}: {}", error.path(), error.detail()),
        ),
    )
}

/// Sequence facts carried beside a provider-private split after decoding.
///
/// They are scheduling metadata only. The receiver retains no payload evidence
/// for retransmission: duplicate classification is solely the queue watermark.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedScheduledSplit {
    sequence_id: u64,
    plan_node_id: i32,
}

impl ReceivedScheduledSplit {
    pub fn from_scheduled(split: &ScheduledSplit) -> Self {
        Self {
            sequence_id: split.sequence_id(),
            plan_node_id: split.plan_node_id(),
        }
    }

    pub const fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }
}

#[derive(Clone, Debug)]
pub struct DecodedScheduledReadSplit {
    received: ReceivedScheduledSplit,
    split: ConnectorReadSplit,
}

impl DecodedScheduledReadSplit {
    pub const fn new(received: ReceivedScheduledSplit, split: ConnectorReadSplit) -> Self {
        Self { received, split }
    }

    pub const fn received(&self) -> &ReceivedScheduledSplit {
        &self.received
    }

    pub const fn split(&self) -> &ConnectorReadSplit {
        &self.split
    }

    pub fn into_parts(self) -> (ReceivedScheduledSplit, ConnectorReadSplit) {
        (self.received, self.split)
    }
}

/// Codec-only form of a frozen scan. Roles use this only at a protocol edge;
/// after decoding they retain the SPI values rather than DTO-backed handles.
#[derive(Clone, Debug)]
pub struct DecodedConnectorReadScan {
    relation: ConnectorReadRelation,
    assignments: Vec<Assignment<ConnectorReadColumnHandle>>,
    enforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    unenforced_predicate: TupleDomain<ConnectorReadColumnHandle>,
    remaining_expression: Option<novarocks_spi::connector::read_stack::ConnectorExpression>,
    work_source: ConnectorReadWorkSource,
}

impl DecodedConnectorReadScan {
    pub fn decode(
        codec: &dyn ConnectorReadWireDecoder,
        source: &ConnectorTableScanSource,
    ) -> Result<Self, ConnectorReadCodecError> {
        let relation = codec.decode_relation(source.table())?;
        let assignments = source
            .assignments()
            .iter()
            .enumerate()
            .map(|(index, assignment)| {
                codec.decode_assignment(
                    assignment.as_proto(),
                    FieldPath::root("connector_table_scan_source")
                        .field("assignments")
                        .index(index),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            relation,
            assignments,
            enforced_predicate: codec.decode_validated_tuple_domain(source.enforced_predicate())?,
            unenforced_predicate: codec
                .decode_validated_tuple_domain(source.unenforced_predicate())?,
            remaining_expression: source.remaining_expression().cloned(),
            work_source: match source.work_source() {
                super::ScanWorkSource::RuntimeSplits => ConnectorReadWorkSource::RuntimeSplits,
                super::ScanWorkSource::WholeRelation => ConnectorReadWorkSource::WholeRelation,
            },
        })
    }

    pub const fn relation(&self) -> &ConnectorReadRelation {
        &self.relation
    }

    pub fn assignments(&self) -> &[Assignment<ConnectorReadColumnHandle>] {
        &self.assignments
    }

    pub const fn enforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        &self.enforced_predicate
    }

    pub const fn unenforced_predicate(&self) -> &TupleDomain<ConnectorReadColumnHandle> {
        &self.unenforced_predicate
    }

    pub const fn remaining_expression(
        &self,
    ) -> Option<&novarocks_spi::connector::read_stack::ConnectorExpression> {
        self.remaining_expression.as_ref()
    }

    pub const fn work_source(&self) -> ConnectorReadWorkSource {
        self.work_source
    }
}
