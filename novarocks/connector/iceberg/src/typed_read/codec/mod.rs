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

//! Iceberg's adapter between public read carriers and its private wire codec.

use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::read_stack::adapter::{ProviderReadRuntime, ReadRuntimeAdapter};
use novarocks_spi::connector::read_stack::{
    ConnectorReadColumnHandle, ConnectorReadRelation, ConnectorReadSplit, ConnectorReadSplitFacts,
    ConnectorReadTransactionHandle,
};
use novarocks_spi::connector::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeContext, ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload,
    ConnectorEnvelopeHeader, ConnectorFieldPath, ConnectorReadRelationPayload,
    ConnectorReadSplitCategory, ConnectorReadSplitPayload, ConnectorReadWireDecoder,
    ConnectorReadWireEncoder,
};

use crate::provider_types::{IcebergReadTypes, IcebergReadView};

use super::{HiveTransactionHandle, IcebergColumnHandle, IcebergReadSplit, IcebergRuntimeRelation};

pub(crate) const ICEBERG_READ_CODEC_REVISION: u32 = 1;
const MAX_PRIVATE_READ_BYTES: usize = 16 * 1024 * 1024;
const MAX_PRIVATE_RETAINED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct IcebergConnectorReadWireAdapter<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    adapter: ReadRuntimeAdapter<P>,
    owner: Arc<str>,
}

impl<P> IcebergConnectorReadWireAdapter<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    pub fn new(adapter: ReadRuntimeAdapter<P>) -> Self {
        Self {
            owner: Arc::from(adapter.binding().descriptor().instance_id.as_str()),
            adapter,
        }
    }

    fn invalid(&self, path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
        ConnectorCodecError::new(path, ConnectorCodecErrorKind::InvalidValue, detail)
    }

    fn inconsistent(
        &self,
        path: ConnectorFieldPath,
        detail: impl AsRef<str>,
    ) -> ConnectorCodecError {
        ConnectorCodecError::new(path, ConnectorCodecErrorKind::InconsistentFields, detail)
    }

    fn private_rejection(&self, error: ConnectorCodecError) -> ConnectorCodecError {
        ConnectorCodecError::new(
            ConnectorFieldPath::root("provider_payload"),
            error.kind(),
            format!("{}: {}", error.path(), error.detail()),
        )
    }

    fn revision() -> ConnectorCodecRevision {
        ConnectorCodecRevision::try_new(ICEBERG_READ_CODEC_REVISION)
            .expect("Iceberg read codec revision is non-zero")
    }

    fn header(&self, category: ConnectorCodecCategory) -> ConnectorEnvelopeHeader {
        let binding = self.adapter.binding();
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            category,
            Self::revision(),
        )
    }

    fn envelope(
        &self,
        category: ConnectorCodecCategory,
        payload: Bytes,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(self.header(category), payload)
    }

    fn decode_limits() -> ConnectorDecodeLimits {
        ConnectorDecodeLimits::try_new(
            MAX_PRIVATE_READ_BYTES,
            MAX_PRIVATE_RETAINED_BYTES,
            MAX_PRIVATE_READ_BYTES,
            1_000_000,
            64,
        )
        .expect("Iceberg read decode limits are finite and non-zero")
    }

    fn decode_private<T>(
        &self,
        payload: &ConnectorEncodedPayload,
        category: ConnectorCodecCategory,
        decode: impl FnOnce(&[u8], &mut ConnectorDecodeContext<'_>) -> Result<T, ConnectorCodecError>,
    ) -> Result<T, ConnectorCodecError> {
        let header = self.header(category);
        payload
            .header()
            .validate_expected(
                header.provider_id(),
                header.catalog(),
                header.category(),
                header.codec_revision(),
            )
            .map_err(|error| self.private_rejection(error))?;
        let mut ledger = ConnectorDecodeLedger::new(Self::decode_limits());
        let mut context = ConnectorDecodeContext::new(&header, &mut ledger);
        decode(payload.payload().as_ref(), &mut context)
            .map_err(|error| self.private_rejection(error))
    }
}

impl<P> ConnectorReadWireDecoder for IcebergConnectorReadWireAdapter<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn owner(&self) -> &str {
        &self.owner
    }

    fn decode_relation_payload(
        &self,
        relation: &ConnectorReadRelationPayload,
    ) -> Result<ConnectorReadRelation, ConnectorCodecError> {
        let view = self.decode_private(
            relation.view(),
            ConnectorCodecCategory::ReadView,
            |payload, context| IcebergReadTypes::wire_codecs().decode_read_view(payload, context),
        )?;
        let table = self.decode_private(
            relation.table(),
            ConnectorCodecCategory::ReadTable,
            |payload, context| IcebergReadTypes::wire_codecs().decode_table(payload, context),
        )?;
        let public_kind = relation.kind();
        if table.kind() != public_kind {
            return Err(self.inconsistent(
                ConnectorFieldPath::root("catalog_table_handle").field("relation"),
                "public relation category does not match the Iceberg private table payload",
            ));
        }
        Ok(ConnectorReadRelation::new(
            public_kind,
            self.adapter.wrap_table(table),
            self.adapter.wrap_transaction(view.transaction().clone()),
        ))
    }

    fn decode_column_payload(
        &self,
        column: &ConnectorEncodedPayload,
    ) -> Result<ConnectorReadColumnHandle, ConnectorCodecError> {
        let column = self.decode_private(
            column,
            ConnectorCodecCategory::ReadColumn,
            |payload, context| IcebergReadTypes::wire_codecs().decode_column(payload, context),
        )?;
        Ok(self.adapter.wrap_column(column))
    }

    fn decode_transaction_payload(
        &self,
        transaction: &ConnectorEncodedPayload,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorCodecError> {
        let view = self.decode_private(
            transaction,
            ConnectorCodecCategory::ReadView,
            |payload, context| IcebergReadTypes::wire_codecs().decode_read_view(payload, context),
        )?;
        Ok(self.adapter.wrap_transaction(view.transaction().clone()))
    }

    fn decode_split_payload(
        &self,
        split: &ConnectorReadSplitPayload,
        facts: &ConnectorReadSplitFacts,
    ) -> Result<ConnectorReadSplit, ConnectorCodecError> {
        let decoded = self.decode_private(
            split.provider_payload(),
            ConnectorCodecCategory::ReadSplit,
            |payload, context| {
                IcebergReadTypes::wire_codecs().decode_split(payload, facts, context)
            },
        )?;
        if split_category(&decoded) != split.category() {
            return Err(self.inconsistent(
                ConnectorFieldPath::root("connector_split").field("category"),
                "public split category does not match the Iceberg private split payload",
            ));
        }
        Ok(self.adapter.wrap_split(decoded))
    }
}

impl<P> ConnectorReadWireEncoder for IcebergConnectorReadWireAdapter<P>
where
    P: ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn owner(&self) -> &str {
        &self.owner
    }

    fn encode_relation_payload(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<ConnectorReadRelationPayload, ConnectorCodecError> {
        let table = self.adapter.table(relation.table()).map_err(|error| {
            self.invalid(
                ConnectorFieldPath::root("relation").field("table"),
                error.to_string(),
            )
        })?;
        let transaction = self
            .adapter
            .transaction(relation.transaction())
            .map_err(|error| {
                self.invalid(
                    ConnectorFieldPath::root("relation").field("transaction"),
                    error.to_string(),
                )
            })?;
        if table.kind() != relation.kind() {
            return Err(self.inconsistent(
                ConnectorFieldPath::root("relation").field("kind"),
                "relation category does not match the Iceberg table handle",
            ));
        }

        let table_bytes = IcebergReadTypes::wire_codecs()
            .encode_table(table)
            .map_err(|error| self.private_rejection(error))?;
        let table_payload = self.envelope(ConnectorCodecCategory::ReadTable, table_bytes);
        let view_bytes = IcebergReadTypes::wire_codecs()
            .encode_read_view(&IcebergReadView::new(transaction.clone()))
            .map_err(|error| self.private_rejection(error))?;
        Ok(ConnectorReadRelationPayload::new(
            relation.kind(),
            table_payload,
            self.envelope(ConnectorCodecCategory::ReadView, view_bytes),
        ))
    }

    fn encode_column_payload(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        let column = self.adapter.column(column).map_err(|error| {
            self.invalid(ConnectorFieldPath::root("column_handle"), error.to_string())
        })?;
        let bytes = IcebergReadTypes::wire_codecs()
            .encode_column(column)
            .map_err(|error| self.private_rejection(error))?;
        Ok(self.envelope(ConnectorCodecCategory::ReadColumn, bytes))
    }

    fn encode_transaction_payload(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
        let transaction = self.adapter.transaction(transaction).map_err(|error| {
            self.invalid(
                ConnectorFieldPath::root("transaction_handle"),
                error.to_string(),
            )
        })?;
        let bytes = IcebergReadTypes::wire_codecs()
            .encode_read_view(&IcebergReadView::new(transaction.clone()))
            .map_err(|error| self.private_rejection(error))?;
        Ok(self.envelope(ConnectorCodecCategory::ReadView, bytes))
    }

    fn encode_split_payload(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<ConnectorReadSplitPayload, ConnectorCodecError> {
        let value = self.adapter.split(split).map_err(|error| {
            self.invalid(
                ConnectorFieldPath::root("connector_split"),
                error.to_string(),
            )
        })?;
        let bytes = IcebergReadTypes::wire_codecs()
            .encode_split(value)
            .map_err(|error| self.private_rejection(error))?;
        Ok(ConnectorReadSplitPayload::new(
            split_category(value),
            self.envelope(ConnectorCodecCategory::ReadSplit, bytes),
        ))
    }
}

const fn split_category(split: &IcebergReadSplit) -> ConnectorReadSplitCategory {
    match split {
        IcebergReadSplit::Data(_) => ConnectorReadSplitCategory::Data,
        IcebergReadSplit::TableChanges(_) => ConnectorReadSplitCategory::TableChanges,
        IcebergReadSplit::ChangeWindow(_) => ConnectorReadSplitCategory::ChangeWindow,
        IcebergReadSplit::SystemFiles(_) => ConnectorReadSplitCategory::SystemFiles,
        IcebergReadSplit::RewritePositionDeleteFiles(_) => {
            ConnectorReadSplitCategory::RewritePositionDeleteFiles
        }
    }
}
