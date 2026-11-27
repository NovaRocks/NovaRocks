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

fn like_use(
    unit: &novarocks_expression::CompiledExprSet,
    root: novarocks_type_contract::ExpressionUseId,
) -> Option<novarocks_type_contract::ExpressionUseId> {
    use novarocks_type_contract::OperatorFunction;
    let identity = |use_id| {
        OperatorFunction::from_function_id(unit.calls().get(&use_id)?.call_contract().function_id())
    };
    match identity(root)? {
        OperatorFunction::Like => Some(root),
        OperatorFunction::BooleanNot => {
            let [operand] = unit.control_flow().uses()[&root].arguments.as_ref() else {
                return None;
            };
            (identity(*operand) == Some(OperatorFunction::Like)).then_some(*operand)
        }
        _ => None,
    }
}

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
                if like_use(unit, root.use_id).is_some() {
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
        let root = unit.root(site.root).unwrap().use_id;
        let use_id = like_use(unit, root).unwrap();
        assert_eq!(root != use_id, negated);
        let occurrence = &flow.uses()[&use_id];
        assert_eq!(occurrence.control, ControlShape::OrderedNull);
        assert_eq!(occurrence.arguments.len(), 2);
        assert_ne!(occurrence.arguments[0], occurrence.arguments[1]);
        let definition = unit.definitions().node(occurrence.definition).unwrap();
        let StaticExprKind::BoundCall { args, .. } = definition.kind() else {
            panic!("selected LIKE call")
        };
        assert_eq!(flow.uses()[&occurrence.arguments[0]].definition, args[0]);
        assert_eq!(flow.uses()[&occurrence.arguments[1]].definition, args[1]);
        let contract = unit.calls()[&use_id].call_contract();
        for argument in contract.selected().argument_types.iter() {
            let novarocks_type_contract::FunctionArgumentType::Value(value) = argument else {
                panic!("LIKE has value arguments")
            };
            assert_eq!(value.data_type(), &DataType::Utf8);
        }
        assert!(definition.value_type().unwrap().nullable());
    }
}
