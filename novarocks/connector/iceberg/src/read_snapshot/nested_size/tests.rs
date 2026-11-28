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

use super::{manifest_nested_size, nested_average_size_fields};
use crate::iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type,
};

/// Inspects the selected schema leaf IDs and width evidence from one file.
#[test]
fn manifest_nested_size_uses_every_leaf_of_the_selected_snapshot_schema() {
    let leaf = |id, name| {
        Arc::new(NestedField::optional(
            id,
            name,
            Type::Primitive(PrimitiveType::Long),
        ))
    };
    let map = Type::Map(MapType::new(
        Arc::new(NestedField::map_key_element(
            4,
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::map_value_element(
            5,
            Type::Primitive(PrimitiveType::Long),
            false,
        )),
    ));
    let schema = Schema::builder()
        .with_fields(vec![
            leaf(1, "scalar"),
            Arc::new(NestedField::optional(
                2,
                "payload",
                Type::Struct(StructType::new(vec![
                    Arc::new(NestedField::optional(3, "attrs", map)),
                    Arc::new(NestedField::optional(
                        6,
                        "items",
                        Type::List(ListType::new(Arc::new(NestedField::list_element(
                            7,
                            Type::Primitive(PrimitiveType::Long),
                            false,
                        )))),
                    )),
                ])),
            )),
        ])
        .build()
        .unwrap();
    let selected = nested_average_size_fields(&schema, &["scalar", "PAYLOAD"], None).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].id, 2);
    assert_eq!(selected[0].leaves, [4, 5, 7]);

    let sizes = HashMap::from([(2, 99), (4, 1), (5, 2), (7, 3), (8, 999)]);
    let leaves = &selected[0].leaves;
    assert_eq!(manifest_nested_size(&sizes, leaves, None).unwrap(), Some(6));
    let missing = HashMap::from([(2, 99), (4, 1), (5, 2), (8, 999)]);
    assert_eq!(manifest_nested_size(&missing, leaves, None).unwrap(), None);
    let overflow = HashMap::from([(4, i64::MAX as u64), (5, 2), (7, 3)]);
    assert_eq!(manifest_nested_size(&overflow, leaves, None).unwrap(), None);
}
