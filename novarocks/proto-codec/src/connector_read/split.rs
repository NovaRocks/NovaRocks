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

//! Validation for the provider-neutral split carrier.
//!
//! The common codec owns only scheduling facts and the public split category.
//! The category message contains one opaque `ReadSplit` envelope; its concrete
//! meaning is validated by the selected provider codec.

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{ConnectorReadSplitFacts, HostAddress, SplitWeight};
use novarocks_spi::connector::{ConnectorCodecCategory, ConnectorEncodedPayload};
use prost::Message;

use crate::{FieldPath, ProtocolError};

use super::{
    MAX_AFFINITY_KEY_BYTES, MAX_SPLIT_ADDRESSES, MAX_SPLIT_ENCODED_BYTES,
    MAX_SPLIT_SCALAR_TOTAL_BYTES, bounded_text, missing, out_of_range,
};

const MAX_HOST_BYTES: usize = 255;

/// Public scheduling category. Provider split contents remain opaque.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitCategory {
    Data,
    TableChanges,
    ChangeWindow,
    SystemFiles,
    RewritePositionDeleteFiles,
}

/// A validated public split carrier.
///
/// The raw DTO is retained for lossless forwarding, while the independently
/// validated neutral scheduling facts and opaque provider payload are exposed
/// to their respective owners.
#[derive(Clone, Debug)]
pub struct ValidatedConnectorSplit {
    raw: dto::ConnectorSplit,
    facts: ConnectorReadSplitFacts,
    category: SplitCategory,
    provider_payload: ConnectorEncodedPayload,
}

impl ValidatedConnectorSplit {
    pub fn parse(raw: dto::ConnectorSplit, path: FieldPath) -> Result<Self, ProtocolError> {
        if raw.encoded_len() > MAX_SPLIT_ENCODED_BYTES {
            return Err(out_of_range(
                path,
                format!("split exceeds {MAX_SPLIT_ENCODED_BYTES} encoded bytes"),
            ));
        }

        let weight = SplitWeight::try_from_raw(raw.split_weight_raw).map_err(|error| {
            out_of_range(path.field("split_weight_raw"), error.message().to_owned())
        })?;

        if raw.addresses.len() > MAX_SPLIT_ADDRESSES {
            return Err(out_of_range(
                path.field("addresses"),
                "split address count exceeds the hard limit",
            ));
        }
        let mut scalar_bytes = 0usize;
        let mut addresses = Vec::with_capacity(raw.addresses.len());
        for (index, address) in raw.addresses.iter().enumerate() {
            let address_path = path.field("addresses").index(index);
            bounded_text(
                &address.host,
                MAX_HOST_BYTES,
                address_path.field("host"),
                false,
            )?;
            scalar_bytes = scalar_bytes.saturating_add(address.host.len());
            let port = u16::try_from(address.port).map_err(|_| {
                out_of_range(
                    address_path.field("port"),
                    "host address port must be within 1..=65535",
                )
            })?;
            if port == 0 {
                return Err(out_of_range(
                    address_path.field("port"),
                    "host address port must be within 1..=65535",
                ));
            }
            addresses.push(
                HostAddress::try_new(&address.host, port)
                    .map_err(|error| out_of_range(address_path, error.message().to_owned()))?,
            );
        }

        if let Some(affinity_key) = raw.affinity_key.as_deref() {
            bounded_text(
                affinity_key,
                MAX_AFFINITY_KEY_BYTES,
                path.field("affinity_key"),
                false,
            )?;
            scalar_bytes = scalar_bytes.saturating_add(affinity_key.len());
        }
        if scalar_bytes > MAX_SPLIT_SCALAR_TOTAL_BYTES {
            return Err(out_of_range(
                path.clone(),
                format!("split scalar fields exceed {MAX_SPLIT_SCALAR_TOTAL_BYTES} bytes in total"),
            ));
        }

        let (category, provider_payload) = decode_category(
            raw.category.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("category"),
                    "split category must be present",
                )
            })?,
            &path,
        )?;
        let facts = ConnectorReadSplitFacts::new(
            raw.remotely_accessible,
            addresses,
            raw.affinity_key.as_deref(),
            weight,
            raw.retained_size_in_bytes,
        );

        Ok(Self {
            raw,
            facts,
            category,
            provider_payload,
        })
    }

    pub const fn facts(&self) -> &ConnectorReadSplitFacts {
        &self.facts
    }

    pub const fn split_weight(&self) -> SplitWeight {
        self.facts.split_weight()
    }

    pub const fn is_remotely_accessible(&self) -> bool {
        self.facts.remotely_accessible()
    }

    pub fn addresses(&self) -> &[dto::HostAddress] {
        &self.raw.addresses
    }

    pub fn affinity_key(&self) -> Option<&str> {
        self.facts.affinity_key()
    }

    pub const fn retained_size_in_bytes(&self) -> u64 {
        self.facts.retained_size_in_bytes()
    }

    pub const fn category(&self) -> SplitCategory {
        self.category
    }

    pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
        &self.provider_payload
    }

    pub const fn as_proto(&self) -> &dto::ConnectorSplit {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorSplit {
        self.raw
    }
}

fn decode_category(
    raw: &dto::connector_split::Category,
    path: &FieldPath,
) -> Result<(SplitCategory, ConnectorEncodedPayload), ProtocolError> {
    let (category, payload, payload_path) = match raw {
        dto::connector_split::Category::Data(value) => (
            SplitCategory::Data,
            value.provider_payload.as_ref(),
            path.field("data").field("provider_payload"),
        ),
        dto::connector_split::Category::TableChanges(value) => (
            SplitCategory::TableChanges,
            value.provider_payload.as_ref(),
            path.field("table_changes").field("provider_payload"),
        ),
        dto::connector_split::Category::ChangeWindow(value) => (
            SplitCategory::ChangeWindow,
            value.provider_payload.as_ref(),
            path.field("change_window").field("provider_payload"),
        ),
        dto::connector_split::Category::SystemFiles(value) => (
            SplitCategory::SystemFiles,
            value.provider_payload.as_ref(),
            path.field("system_files").field("provider_payload"),
        ),
        dto::connector_split::Category::RewritePositionDeleteFiles(value) => (
            SplitCategory::RewritePositionDeleteFiles,
            value.provider_payload.as_ref(),
            path.field("rewrite_position_delete_files")
                .field("provider_payload"),
        ),
    };
    let payload = crate::connector_common::decode_embedded_connector_payload(
        payload,
        ConnectorCodecCategory::ReadSplit,
        MAX_SPLIT_ENCODED_BYTES,
        payload_path,
    )?;
    Ok((category, payload))
}
