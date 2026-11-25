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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::iceberg::spec::{
    FormatVersion, ListType, PartitionSpec, PrimitiveType, SortOrder, TableMetadataBuilder,
};
use crate::schema_mapping::nominal_declaration::PROPERTY;
use novarocks_spi::connector::{ConnectorStopOwner, MvSourceFieldCoordinate};

use super::*;

fn metadata(fields: Vec<Arc<NestedField>>, properties: HashMap<String, String>) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(fields)
        .build()
        .expect("schema");
    TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec().into_unbound(),
        SortOrder::unsorted_order(),
        "file:///mv-source-binding".into(),
        FormatVersion::V3,
        properties,
    )
    .expect("metadata")
    .assign_uuid(
        "00000000-0000-0000-0000-000000000001"
            .parse()
            .expect("UUID"),
    )
    .build()
    .expect("metadata")
    .metadata
}

fn path(parts: &[(u32, &str)], whole_subtree: bool) -> MvSourceFieldPath {
    MvSourceFieldPath {
        fields: parts
            .iter()
            .map(|(ordinal, name)| MvSourceFieldCoordinate {
                ordinal: *ordinal,
                name: (*name).to_owned(),
            })
            .collect(),
        whole_subtree,
    }
}

fn observe(
    metadata: &TableMetadata,
    paths: &[MvSourceFieldPath],
) -> Result<MvSourceBindingObservation, ConnectorError> {
    let stop = ConnectorStopOwner::new();
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        stop.view(),
        4096,
        65_536,
    )
    .expect("context");
    project_source_binding(
        ConnectorInstanceId::parse("catalog").expect("instance"),
        metadata,
        metadata.current_schema(),
        paths,
        &context,
    )
}

fn nested_fields(required: bool, extra_field: bool) -> Vec<Arc<NestedField>> {
    let mut attrs = vec![
        Arc::new(NestedField::new(
            2,
            "x",
            Type::Primitive(PrimitiveType::Long),
            required,
        )),
        Arc::new(NestedField::optional(
            3,
            "tags",
            Type::List(ListType::new(Arc::new(NestedField::list_element(
                4,
                Type::Primitive(PrimitiveType::String),
                false,
            )))),
        )),
    ];
    if extra_field {
        attrs.push(Arc::new(NestedField::optional(
            5,
            "extra",
            Type::Primitive(PrimitiveType::Int),
        )));
    }
    vec![Arc::new(NestedField::optional(
        1,
        "attrs",
        Type::Struct(StructType::new(attrs)),
    ))]
}

#[test]
fn empty_bindings_keep_object_identity_without_io() {
    let source = metadata(nested_fields(true, false), HashMap::new());
    let original = observe(&source, &[]).expect("empty access set");
    let replacement = TableMetadataBuilder::new_from_metadata(source, None)
        .assign_uuid(
            "00000000-0000-0000-0000-000000000002"
                .parse()
                .expect("UUID"),
        )
        .build()
        .expect("replacement")
        .metadata;
    let replacement = observe(&replacement, &[]).expect("replacement access set");

    assert_eq!(original.catalog_instance.as_str(), "catalog");
    assert_eq!(
        original.object_id.as_bytes().as_ref(),
        b"00000000-0000-0000-0000-000000000001"
    );
    assert_eq!(original.object_kind, MvSourceObjectKind::Table);
    assert!(original.paths.is_empty());
    assert_ne!(original.object_fact, replacement.object_fact);
    assert_eq!(original.path_fact, replacement.path_fact);
    assert_eq!(original.type_fact, replacement.type_fact);
}

#[test]
fn nested_selection_preserves_required_and_ignores_unselected_siblings() {
    let source = metadata(nested_fields(true, false), HashMap::new());
    let unrelated = metadata(nested_fields(true, true), HashMap::new());
    let nullable = metadata(nested_fields(false, false), HashMap::new());
    let select_x = path(&[(0, "attrs"), (0, "x")], false);
    let x = observe(&source, std::slice::from_ref(&select_x)).expect("select x");
    let fields = &x.paths[0].fields;
    assert_eq!(fields[0].provider_field_id.as_ref(), &1_i32.to_be_bytes());
    assert_eq!(fields[1].provider_field_id.as_ref(), &2_i32.to_be_bytes());
    assert_eq!(fields[0].kind, MvSourceFieldKind::Struct);
    assert!(!fields[0].required);
    assert_eq!(fields[1].kind, MvSourceFieldKind::Scalar);
    assert!(fields[1].required);

    let same = observe(&unrelated, std::slice::from_ref(&select_x)).expect("unrelated sibling");
    assert_eq!(x, same);
    let changed = observe(&nullable, &[select_x]).expect("new required bit");
    assert_eq!(x.path_fact, changed.path_fact);
    assert_ne!(x.type_fact, changed.type_fact);

    let whole = path(&[(0, "attrs")], true);
    assert_ne!(
        observe(&source, std::slice::from_ref(&whole))
            .expect("whole attrs")
            .type_fact,
        observe(&unrelated, &[whole])
            .expect("larger attrs")
            .type_fact,
    );
    let tags = observe(&source, &[path(&[(0, "attrs"), (1, "tags")], true)]).expect("whole list");
    let novarocks_spi::connector::ProviderSourceType::Iceberg(leaf) = &tags.paths[0].leaf_type;
    assert!(matches!(
        leaf.nodes(),
        [
            novarocks_spi::connector::IcebergSourceTypeNode::List {
                element_required: false,
                ..
            },
            novarocks_spi::connector::IcebergSourceTypeNode::Primitive(
                novarocks_spi::connector::IcebergSourcePrimitive::String
            ),
        ],
    ));
}

#[test]
fn path_set_canonicalization_keeps_request_response_coordinates() {
    let source = metadata(nested_fields(true, false), HashMap::new());
    let x = path(&[(0, "attrs"), (0, "x")], false);
    let tags = path(&[(0, "attrs"), (1, "tags")], true);
    let forward = observe(&source, &[x.clone(), tags.clone()]).expect("forward paths");
    let reversed = observe(&source, &[tags, x.clone(), x]).expect("reordered duplicate path");
    assert_eq!(forward.path_fact, reversed.path_fact);
    assert_eq!(forward.type_fact, reversed.type_fact);
    assert_eq!(
        forward.read_declarations_fact,
        reversed.read_declarations_fact
    );
    assert_eq!(forward.paths[0], reversed.paths[1]);
    assert_eq!(forward.paths[1], reversed.paths[0]);
}

#[test]
fn read_declarations_detect_only_observed_read_changes() {
    let build = |initial, write, integer, nominal, mapping: &str| {
        let fields = vec![
            Arc::new(
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int))
                    .with_initial_default(Literal::int(initial))
                    .with_write_default(Literal::int(write)),
            ),
            Arc::new(NestedField::optional(
                2,
                "payload",
                Type::Primitive(PrimitiveType::Binary),
            )),
            Arc::new(NestedField::optional(
                3,
                "retired",
                Type::Primitive(PrimitiveType::Fixed(16)),
            )),
        ];
        let mut domains = vec![
            serde_json::json!({"id":1, "domain":integer}),
            serde_json::json!({"id":2, "domain":nominal}),
        ];
        if nominal == "hll" {
            domains.push(serde_json::json!({"id":3, "domain":"largeint"}));
        }
        metadata(
            fields,
            HashMap::from([
                (
                    PROPERTY.into(),
                    serde_json::json!({"version":1, "fields":domains}).to_string(),
                ),
                (
                    TableProperties::PROPERTY_DEFAULT_NAME_MAPPING.into(),
                    mapping.into(),
                ),
            ]),
        )
    };
    let map = r#"[{"field-id":1,"names":["id"]}]"#;
    let paths = [
        path(&[(0, "id")], false),
        path(&[(1, "payload")], false),
        path(&[(2, "retired")], false),
    ];
    let original = observe(&build(4, 5, "tinyint", "hll", map), &paths).expect("declarations");
    let write_only = observe(
        &build(
            4,
            7,
            "tinyint",
            "hll",
            r#"[ { "names": ["id"], "field-id": 1 } ]"#,
        ),
        &paths,
    )
    .expect("write-only and name-mapping spelling");
    assert_eq!(original, write_only);

    for modified in [
        build(6, 5, "tinyint", "hll", map),
        build(4, 5, "smallint", "hll", map),
        build(4, 5, "tinyint", "bitmap", map),
        build(
            4,
            5,
            "tinyint",
            "hll",
            r#"[{"field-id":1,"names":["old_id"]}]"#,
        ),
    ] {
        let modified = observe(&modified, &paths).expect("changed declaration");
        assert_eq!(original.object_fact, modified.object_fact);
        assert_eq!(original.path_fact, modified.path_fact);
        assert_eq!(original.type_fact, modified.type_fact);
        assert_ne!(
            original.read_declarations_fact,
            modified.read_declarations_fact
        );
    }

    let only_id = &paths[..1];
    let nominal_elsewhere = build(4, 5, "tinyint", "bitmap", map);
    assert_eq!(
        observe(&build(4, 5, "tinyint", "hll", map), only_id).expect("id only"),
        observe(&nominal_elsewhere, only_id).expect("unconsumed domains"),
    );
}

#[test]
fn bad_coordinates_and_container_descents_are_typed() {
    let source = metadata(nested_fields(true, false), HashMap::new());
    assert_eq!(
        observe(&source, &[path(&[(0, "attrs"), (0, "tags")], false)])
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::NotFound,
    );
    assert_eq!(
        observe(
            &source,
            &[path(&[(0, "attrs"), (1, "tags"), (0, "element")], false)]
        )
        .unwrap_err()
        .kind(),
        ConnectorErrorKind::Unsupported,
    );
    assert_eq!(
        observe(&source, &[path(&[(0, "attrs")], false)])
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest,
    );
}

#[test]
fn ancestor_default_projects_only_the_consumed_leaf() {
    let build = |selected, sibling| {
        let fields = vec![
            Arc::new(NestedField::optional(
                2,
                "x",
                Type::Primitive(PrimitiveType::Int),
            )),
            Arc::new(NestedField::optional(
                3,
                "sibling",
                Type::Primitive(PrimitiveType::Int),
            )),
        ];
        let default = Literal::Struct(
            [Some(Literal::int(selected)), Some(Literal::int(sibling))]
                .into_iter()
                .collect(),
        );
        let attrs = NestedField::optional(1, "attrs", Type::Struct(StructType::new(fields)))
            .with_initial_default(default);
        metadata(vec![Arc::new(attrs)], HashMap::new())
    };

    let selected = path(&[(0, "attrs"), (0, "x")], false);
    let a = observe(&build(1, 2), std::slice::from_ref(&selected)).expect("old default");
    let b = observe(&build(1, 3), std::slice::from_ref(&selected)).expect("new sibling default");
    let c = observe(&build(4, 2), &[selected]).expect("new selected default");
    assert_eq!(a, b);
    assert_eq!(a.type_fact, c.type_fact);
    assert_ne!(a.read_declarations_fact, c.read_declarations_fact);
}
