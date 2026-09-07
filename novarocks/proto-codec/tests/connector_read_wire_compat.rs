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

use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    ValidatedColumnHandle, decode_tuple_domain, encode_tuple_domain,
};
use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Domain, Range, TupleDomain, ValueSet,
};

fn column(field_id: i32) -> dto::ColumnHandle {
    dto::ColumnHandle {
        handle: Some(dto::column_handle::Handle::Iceberg(
            dto::IcebergColumnHandle {
                base_column_identity: Some(dto::ColumnIdentity {
                    field_id,
                    // Keep the names equal so this fixture isolates protobuf's
                    // varint ordering for the two field IDs.
                    name: "x".to_owned(),
                    category: dto::ColumnIdentityCategory::Primitive as i32,
                    children: Vec::new(),
                }),
                base_type_json: "\"long\"".to_owned(),
                field_id_path: Vec::new(),
                type_json: "\"long\"".to_owned(),
                nullable: true,
                comment: None,
            },
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

fn field_id(column: &dto::ColumnHandle) -> i32 {
    match column.handle.as_ref().expect("closed column variant") {
        dto::column_handle::Handle::Iceberg(iceberg) => {
            iceberg
                .base_column_identity
                .as_ref()
                .expect("column identity")
                .field_id
        }
    }
}

#[test]
fn outer_tuple_domain_keeps_the_existing_canonical_column_byte_order() {
    let c255 = ValidatedColumnHandle::parse(column(255), FieldPath::root("column"))
        .expect("valid field 255");
    let c256 = ValidatedColumnHandle::parse(column(256), FieldPath::root("column"))
        .expect("valid field 256");

    // 255 encodes its first field-id varint as ff 01 and 256 as 80 02.
    // Therefore the outer wire order is 256 then 255 even though their
    // provider-internal numeric order would be 255 then 256.
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
        .map(|domain| field_id(domain.column.as_ref().expect("column")))
        .collect::<Vec<_>>();
    assert_eq!(order, vec![256, 255]);
    assert_eq!(
        encode_tuple_domain(
            &decode_tuple_domain(&encoded, FieldPath::root("tuple")).expect("round trip")
        ),
        encoded
    );
}
