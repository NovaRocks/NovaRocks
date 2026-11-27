// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{ValueTypeError, ValueTypeVisit};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

#[test]
fn wide_node_visits_all_siblings_in_declaration_order() {
    let fields: Vec<_> = (0..crate::MAX_VALUE_TYPE_NODES + 1)
        .map(|index| Arc::new(Field::new(index.to_string(), DataType::Int32, true)))
        .collect();
    let ty = DataType::Struct(fields.clone().into());
    let mut next = 0;
    let mut nodes = 0;
    crate::validate_value_type_structure_observed::<ValueTypeError>(&ty, |visit| {
        match visit {
            ValueTypeVisit::Field(field) => {
                assert!(std::ptr::eq(field, fields[next].as_ref()));
                next += 1;
            }
            ValueTypeVisit::TypeNode(_) => {
                assert_eq!(next, nodes);
                nodes += 1;
            }
            ValueTypeVisit::ChildEdge(_) => {}
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(next, fields.len());
    assert_eq!(nodes, fields.len() + 1);
}
