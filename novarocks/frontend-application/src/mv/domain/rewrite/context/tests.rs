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

use super::*;
use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;

fn projection() -> StoredKeyedMvProjection {
    let object = ConnectorTableObjectId::try_new(Bytes::from_static(&[11])).unwrap();
    let revision = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
        novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
        &object,
        Some(12),
    )
    .unwrap();
    let (persisted_object, _) = persist_exact_connector_revision(&revision).unwrap();
    let mut fixture = ProjectionFixture::new(
        novarocks_mv_application::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
        None,
    );
    for occurrence in &mut fixture.binding.occurrences {
        occurrence.object_id = persisted_object.clone();
    }
    StoredKeyedMvProjection {
        mv_id: 17,
        facts: fixture.build().unwrap(),
    }
}

fn pin(id: u32) -> MvRewriteSourceSnapshot {
    let object = ConnectorTableObjectId::try_new(Bytes::from_static(&[11])).unwrap();
    MvRewriteSourceSnapshot {
        occurrence_id: SqlMvRelationOccurrenceId::new(id),
        snapshot_id: 12,
        table_object_id: object.clone(),
        semantic_revision: ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            novarocks_spi::connector::ConnectorProviderId::parse("iceberg").unwrap(),
            &object,
            Some(12),
        )
        .unwrap(),
    }
}

#[test]
fn sparse_repeated_relation_occurrences_remain_distinct() {
    let projection = projection();
    let pins = validate_source_pins(projection.facts.binding(), vec![pin(8), pin(7)]).unwrap();
    assert_eq!(pins.len(), 2);
    assert!(pins.contains_key(&SqlMvRelationOccurrenceId::new(7)));
    assert!(pins.contains_key(&SqlMvRelationOccurrenceId::new(8)));
}

#[test]
fn duplicate_and_dense_renumbered_pins_are_rejected() {
    let projection = projection();
    assert!(validate_source_pins(projection.facts.binding(), vec![pin(7), pin(7)]).is_err());
    assert!(validate_source_pins(projection.facts.binding(), vec![pin(0), pin(1)]).is_err());
}

#[test]
fn source_object_replacement_is_not_hidden_by_equal_snapshot() {
    let projection = projection();
    let mut replacement = pin(8);
    replacement.table_object_id =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"replacement")).unwrap();
    assert!(validate_source_pins(projection.facts.binding(), vec![pin(7), replacement]).is_err());
}

#[test]
fn never_published_does_not_accept_invented_history() {
    let projection = projection();
    let binding = projection.facts.binding();
    assert!(validate_history(binding, None, vec![pin(7), pin(8)]).is_err());
    assert!(
        validate_history(binding, None, Vec::new())
            .unwrap()
            .0
            .is_empty()
    );
}

#[test]
fn zoned_timestamp_uses_the_provider_timestamp_carrier() {
    use novarocks_spi::connector::{
        IcebergSourcePrimitive, IcebergSourceType, IcebergSourceTypeNode, ProviderSourceType,
    };
    let source = ProviderSourceType::Iceberg(
        IcebergSourceType::try_new(vec![IcebergSourceTypeNode::Primitive(
            IcebergSourcePrimitive::Timestamptz,
        )])
        .unwrap(),
    );
    assert_eq!(
        arrow_type_from_provider_type(&source).unwrap(),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    );
}

#[test]
fn recursive_provider_type_matches_the_sql_field_layout() {
    use arrow::datatypes::Field;
    use novarocks_spi::connector::{
        IcebergSourcePrimitive as Primitive, IcebergSourceType, IcebergSourceTypeNode as Node,
        ProviderSourceType,
    };

    for element_required in [false, true] {
        let source = ProviderSourceType::Iceberg(
            IcebergSourceType::try_new(vec![
                Node::Struct { field_count: 2 },
                Node::Field {
                    id: 6,
                    name: "items".into(),
                    required: true,
                },
                Node::List {
                    element_id: 9,
                    element_required,
                },
                Node::Primitive(Primitive::Long),
                Node::Field {
                    id: 7,
                    name: "attrs".into(),
                    required: false,
                },
                Node::Map {
                    key_id: 10,
                    value_id: 11,
                    value_required: true,
                },
                Node::Primitive(Primitive::String),
                Node::Primitive(Primitive::Long),
            ])
            .unwrap(),
        );
        let items = Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int64,
                !element_required,
            ))),
            false,
        );
        let entries = Field::new_struct(
            "entries",
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ],
            false,
        );
        let attrs = Field::new("attrs", DataType::Map(Arc::new(entries), false), true);
        assert_eq!(
            arrow_type_from_provider_type(&source).unwrap(),
            DataType::Struct(vec![items, attrs].into()),
        );
    }
}
