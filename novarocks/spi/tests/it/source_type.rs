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

use novarocks_spi::connector::{
    IcebergSourcePrimitive as Primitive, IcebergSourceType as Iceberg,
    IcebergSourceTypeNode as Node, ProviderSourceType,
};

fn source(nodes: Vec<Node>) -> ProviderSourceType {
    ProviderSourceType::Iceberg(Iceberg::try_new(nodes).unwrap())
}

#[test]
fn source_identity_retains_ids_requiredness_and_nested_types() {
    let nested = |id, required, element| {
        source(vec![
            Node::Struct { field_count: 1 },
            Node::Field {
                id: 1,
                name: "items".into(),
                required: true,
            },
            Node::Map {
                key_id: 2,
                value_id: 3,
                value_required: true,
            },
            Node::Primitive(Primitive::String),
            Node::List {
                element_id: id,
                element_required: required,
            },
            Node::Primitive(element),
        ])
    };
    let original = nested(4, false, Primitive::Int);
    let new_id = nested(5, false, Primitive::Int);
    let required_list = nested(4, true, Primitive::Int);
    let new_element = nested(4, false, Primitive::Long);
    assert_eq!(original.to_string(), "struct<map>");
    assert_eq!(new_element.to_string(), original.to_string());
    assert_ne!(original.sub_digest(), new_id.sub_digest());
    assert_eq!(original.shape(), new_id.shape());
    assert_ne!(original.shape(), required_list.shape());
    assert_ne!(original.shape(), new_element.shape());
}

#[test]
fn source_identity_distinguishes_timestamp_precision_zone_and_uuid_width() {
    let identity = |value| source(vec![Node::Primitive(value)]).sub_digest();
    assert_ne!(
        identity(Primitive::Timestamp),
        identity(Primitive::Timestamptz)
    );
    assert_ne!(
        identity(Primitive::Timestamp),
        identity(Primitive::TimestampNs)
    );
    assert_ne!(
        identity(Primitive::Timestamptz),
        identity(Primitive::TimestamptzNs)
    );
    assert_ne!(identity(Primitive::Uuid), identity(Primitive::Fixed(16)));
    assert_ne!(
        identity(Primitive::Fixed(16)),
        identity(Primitive::Fixed(32))
    );
}

#[test]
fn incomplete_preorder_type_is_rejected() {
    assert!(
        Iceberg::try_new(vec![Node::List {
            element_id: 1,
            element_required: true,
        }])
        .is_err()
    );
    assert!(
        Iceberg::try_new(vec![
            Node::Primitive(Primitive::Int),
            Node::Primitive(Primitive::Long),
        ])
        .is_err()
    );
}
