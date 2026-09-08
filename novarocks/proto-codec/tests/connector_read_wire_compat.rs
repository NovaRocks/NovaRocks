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

//! Compatibility facts that must survive an internal connector-read migration.

use std::collections::BTreeMap;

use novarocks_proto_codec::connector_read::{
    MAX_AFFINITY_KEY_BYTES, MAX_SPLIT_ADDRESSES, ValidatedColumnHandle, ValidatedConnectorSplit,
    decode_tuple_domain, encode_tuple_domain,
};
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::{
    catalog as catalog_dto, connector_common as common_dto, connector_read as dto,
};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Domain, Range, SplitWeight, TupleDomain, ValueSet,
};

fn provider_payload(
    category: common_dto::ConnectorPayloadCategory,
    payload: Vec<u8>,
) -> common_dto::ConnectorEncodedPayload {
    common_dto::ConnectorEncodedPayload {
        header: Some(common_dto::ConnectorEnvelopeHeader {
            provider_id: "fixture".to_owned(),
            catalog: Some(catalog_dto::CatalogHandle {
                catalog_name: "lake".to_owned(),
                version: vec![7; 32],
            }),
            category: category as i32,
            codec_revision: 1,
        }),
        payload,
    }
}

fn column(private_key: Vec<u8>) -> dto::ColumnHandle {
    dto::ColumnHandle {
        provider_payload: Some(provider_payload(
            common_dto::ConnectorPayloadCategory::ReadColumn,
            private_key,
        )),
    }
}

fn bigint_domain(low: i64) -> Domain {
    let range = Range::try_new(
        ConnectorValueType::BigInt,
        Bound::Inclusive(ConnectorValue::BigInt(low)),
        Bound::Unbounded,
    )
    .expect("valid range");
    Domain::new(
        ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range]).expect("valid set"),
        false,
    )
}

fn private_column_key(column: &dto::ColumnHandle) -> &[u8] {
    &column
        .provider_payload
        .as_ref()
        .expect("provider payload")
        .payload
}

#[test]
fn outer_tuple_domain_keeps_the_existing_canonical_column_byte_order() {
    let c255 = ValidatedColumnHandle::parse(column(vec![0xff, 0x01]), FieldPath::root("column"))
        .expect("valid private key 255");
    let c256 = ValidatedColumnHandle::parse(column(vec![0x80, 0x02]), FieldPath::root("column"))
        .expect("valid private key 256");

    // The public tuple domain orders complete carrier encodings. These equal
    // length opaque keys intentionally have the opposite lexical order from
    // the provider's hypothetical numeric interpretation.
    assert!(c255.canonical_bytes() > c256.canonical_bytes());

    let tuple = TupleDomain::with_column_domains(BTreeMap::from([
        (c255, bigint_domain(255)),
        (c256, bigint_domain(256)),
    ]))
    .expect("bounded tuple domain");
    let encoded = encode_tuple_domain(&tuple);
    let order = encoded
        .column_domains
        .iter()
        .map(|domain| private_column_key(domain.column.as_ref().expect("column")))
        .collect::<Vec<_>>();
    assert_eq!(order, vec![&[0x80, 0x02][..], &[0xff, 0x01][..]]);
    assert_eq!(
        encode_tuple_domain(
            &decode_tuple_domain(&encoded, FieldPath::root("tuple")).expect("round trip")
        ),
        encoded
    );
}

#[test]
fn neutral_split_scheduling_facts_survive_the_wire_migration_baseline() {
    let raw = dto::ConnectorSplit {
        split_weight_raw: 37,
        remotely_accessible: false,
        addresses: vec![dto::HostAddress {
            host: "be-2.novarocks.internal".to_string(),
            port: 9060,
        }],
        affinity_key: Some("cohort/table/partition=7".to_string()),
        retained_size_in_bytes: 8192,
        category: Some(dto::connector_split::Category::Data(dto::DataSplit {
            provider_payload: Some(provider_payload(
                common_dto::ConnectorPayloadCategory::ReadSplit,
                b"provider-private-split".to_vec(),
            )),
        })),
    };

    let split = ValidatedConnectorSplit::parse(raw.clone(), FieldPath::root("connector_split"))
        .expect("valid split");
    assert_eq!(split.split_weight(), SplitWeight::try_from_raw(37).unwrap());
    assert!(!split.is_remotely_accessible());
    assert_eq!(split.addresses()[0].host, "be-2.novarocks.internal");
    assert_eq!(split.affinity_key(), Some("cohort/table/partition=7"));
    assert_eq!(split.retained_size_in_bytes(), 8192);
    assert_eq!(split.into_proto(), raw);
}

#[test]
fn public_carriers_enforce_category_and_scheduling_budgets_without_provider_semantics() {
    let wrong_column = dto::ColumnHandle {
        provider_payload: Some(provider_payload(
            common_dto::ConnectorPayloadCategory::ReadSplit,
            b"opaque".to_vec(),
        )),
    };
    assert_eq!(
        ValidatedColumnHandle::parse(wrong_column, FieldPath::root("column"))
            .expect_err("column carrier must require ReadColumn")
            .kind(),
        ProtocolErrorKind::InconsistentFields
    );

    let split =
        |addresses: Vec<dto::HostAddress>, affinity_key: Option<String>| dto::ConnectorSplit {
            split_weight_raw: 100,
            remotely_accessible: true,
            addresses,
            affinity_key,
            retained_size_in_bytes: 1,
            category: Some(dto::connector_split::Category::Data(dto::DataSplit {
                provider_payload: Some(provider_payload(
                    common_dto::ConnectorPayloadCategory::ReadSplit,
                    b"opaque".to_vec(),
                )),
            })),
        };
    let too_many_addresses = (0..=MAX_SPLIT_ADDRESSES)
        .map(|_| dto::HostAddress {
            host: "be.local".to_owned(),
            port: 9060,
        })
        .collect();
    assert_eq!(
        ValidatedConnectorSplit::parse(
            split(too_many_addresses, None),
            FieldPath::root("connector_split"),
        )
        .expect_err("address count must be bounded")
        .kind(),
        ProtocolErrorKind::OutOfRange
    );
    assert_eq!(
        ValidatedConnectorSplit::parse(
            split(Vec::new(), Some("a".repeat(MAX_AFFINITY_KEY_BYTES + 1))),
            FieldPath::root("connector_split"),
        )
        .expect_err("affinity key must be bounded")
        .kind(),
        ProtocolErrorKind::OutOfRange
    );
}
