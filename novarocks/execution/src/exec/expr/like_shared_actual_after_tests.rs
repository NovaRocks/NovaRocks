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

//! Exact ordered LIKE definitions and recipes from the original SQL source.

use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source;
use super::sql_test_source::sql_source_with_single_field_and_core_residuals;
use arrow::datatypes::{DataType, Field};
use novarocks_expression::StaticExprKind;
use novarocks_expression_contract::RootOrdinal;
use novarocks_local_program::{LocalProgram, ProgramRootRef, ProgramUnitId};
use novarocks_type_contract::ControlShape;
use std::sync::Arc;

fn compiled(sql: &str, dtype: DataType) -> (Arc<LocalProgram>, ProgramRootRef) {
    let source =
        sql_source_with_single_field_and_core_residuals(sql, Field::new("name", dtype, true));
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        for (unit_ordinal, unit) in program.units().iter().enumerate() {
            for (root_ordinal, root) in unit.roots().iter().enumerate() {
                let definition = unit.definitions().node(root.definition).unwrap();
                if matches!(definition.kind(), StaticExprKind::PreparedLike { .. }) {
                    assert!(found.is_none(), "one actual LIKE root");
                    found = Some((
                        program.clone(),
                        ProgramRootRef {
                            unit: ProgramUnitId::new(unit_ordinal),
                            root: RootOrdinal::new(u32::try_from(root_ordinal).unwrap()),
                        },
                    ));
                }
            }
        }
    }
    found.expect("actual SQL LIKE root survived original lowering")
}
#[test]
fn like_actual_source_original_ordered_definitions_exact_recipe_and_negation() {
    for (sql, negated) in [
        ("SELECT name LIKE name FROM t0", false),
        ("SELECT name NOT LIKE 'a%' FROM t0", true),
    ] {
        let (program, site) = compiled(sql, DataType::Utf8);
        let unit = program.unit(site.unit).unwrap();
        let flow = unit.control_flow();
        let occurrence = &flow.uses()[&unit.root(site.root).unwrap().use_id];
        assert_eq!(occurrence.control, ControlShape::Eager);
        assert_eq!(occurrence.arguments.len(), 2);
        assert_ne!(occurrence.arguments[0], occurrence.arguments[1]);
        let definition = unit.definitions().node(occurrence.definition).unwrap();
        let StaticExprKind::PreparedLike {
            text,
            pattern,
            negated: actual,
        } = definition.kind()
        else {
            panic!("actual original LIKE")
        };
        assert_eq!(*actual, negated);
        assert_eq!(flow.uses()[&occurrence.arguments[0]].definition, *text);
        assert_eq!(flow.uses()[&occurrence.arguments[1]].definition, *pattern);
        let recipe = unit
            .recipes()
            .native_like(unit.root(site.root).unwrap().use_id)
            .unwrap();
        assert_eq!(recipe.text_type().data_type().clone(), DataType::Utf8);
        assert_eq!(recipe.pattern_type().data_type().clone(), DataType::Utf8);
        assert!(recipe.result_type().nullable());
        assert_eq!(recipe.negated(), negated);
    }
}
