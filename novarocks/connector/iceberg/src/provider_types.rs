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

use bytes::Bytes;
use novarocks_spi::connector::provider::ProviderReadTypes;
use novarocks_spi::connector::read_stack::ConnectorReadSplitFacts;
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorDecodeContext, ConnectorPrivateDecoder, ConnectorPrivateEncoder,
};

use crate::typed_read::{
    HiveTransactionHandle, IcebergColumnHandle, IcebergReadSplit, IcebergRuntimeRelation,
};
use crate::wire::read::IcebergReadWireCodec;

/// Frozen provider view carried separately from the table and split.
///
/// Iceberg's current transaction marker is retained as one view fact while
/// the provider-private wire migration moves the remaining snapshot facts out
/// of the public DTO. Paimon supplies a different concrete view through the
/// same family contract.
#[derive(Clone, Debug)]
pub struct IcebergReadView {
    transaction: HiveTransactionHandle,
}

impl IcebergReadView {
    pub const fn new(transaction: HiveTransactionHandle) -> Self {
        Self { transaction }
    }

    pub const fn transaction(&self) -> &HiveTransactionHandle {
        &self.transaction
    }
}

/// The single Iceberg read family shared by control and execution adapters.
pub struct IcebergReadTypes;

impl IcebergReadTypes {
    /// The one provider-owned codec shared by both role adapters.
    pub const fn wire_codecs() -> IcebergReadCodecs {
        IcebergReadCodecs
    }
}

/// Public Iceberg read-family facade over the provider-private protobuf.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergReadCodecs;

impl IcebergReadCodecs {
    pub fn encode_table(
        &self,
        value: &IcebergRuntimeRelation,
    ) -> Result<Bytes, ConnectorCodecError> {
        IcebergReadWireCodec.encode_private(value)
    }

    pub fn decode_table(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergRuntimeRelation, ConnectorCodecError> {
        IcebergReadWireCodec.decode_private(payload, context)
    }

    pub fn encode_column(&self, value: &IcebergColumnHandle) -> Result<Bytes, ConnectorCodecError> {
        IcebergReadWireCodec.encode_private(value)
    }

    pub fn decode_column(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergColumnHandle, ConnectorCodecError> {
        IcebergReadWireCodec.decode_private(payload, context)
    }

    pub fn encode_read_view(&self, value: &IcebergReadView) -> Result<Bytes, ConnectorCodecError> {
        IcebergReadWireCodec.encode_private(value)
    }

    pub fn decode_read_view(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergReadView, ConnectorCodecError> {
        IcebergReadWireCodec.decode_private(payload, context)
    }

    pub fn encode_split(&self, value: &IcebergReadSplit) -> Result<Bytes, ConnectorCodecError> {
        IcebergReadWireCodec.encode_private(value)
    }

    pub fn decode_split(
        &self,
        payload: &[u8],
        facts: &ConnectorReadSplitFacts,
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<IcebergReadSplit, ConnectorCodecError> {
        IcebergReadWireCodec.decode_split_private(payload, facts, context)
    }
}

impl ProviderReadTypes for IcebergReadTypes {
    type Table = IcebergRuntimeRelation;
    type Column = IcebergColumnHandle;
    type ReadView = IcebergReadView;
    type Split = IcebergReadSplit;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iceberg_declares_one_read_family_with_an_independent_view() {
        fn assert_family<F>()
        where
            F: ProviderReadTypes<
                    Table = IcebergRuntimeRelation,
                    Column = IcebergColumnHandle,
                    ReadView = IcebergReadView,
                    Split = IcebergReadSplit,
                >,
        {
        }
        assert_family::<IcebergReadTypes>();
        let _codec = IcebergReadTypes::wire_codecs();
    }
}
