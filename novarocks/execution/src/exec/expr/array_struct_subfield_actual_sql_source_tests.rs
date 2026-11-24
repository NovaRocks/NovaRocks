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
//! SQL binding and complete local compilation for array struct field access.
use arrow::datatypes::{DataType, Field};
use novarocks_expression_contract::ExprKind;
use novarocks_physical_plan::FunctionArgumentType;
use novarocks_physical_plan::StaticFunctionArgument;

fn sources() -> [novarocks_sql::compiler::SqlAuthoredPhysicalPlan; 2] {
    use std::sync::Arc;

    let item = DataType::Struct(
        vec![
            Field::new("item", DataType::Int32, true),
            Field::new("count", DataType::Int64, false),
        ]
        .into(),
    );
    let fields = [
        Field::new("grp", DataType::Int32, true),
        Field::new(
            "entries",
            DataType::List(Arc::new(Field::new("item", item, true))),
            true,
        ),
    ];
    [
        "SELECT __array_struct_subfield(entries, 'item') FROM fixture.source",
        "SELECT grp, __array_struct_subfield(entries, 'item') FROM fixture.source ORDER BY grp",
    ]
    .map(|sql| super::sql_test_source::sql_source_with_declared_fields(sql, &fields))
}
#[test]
fn array_struct_subfield_actual_required_sql_full_binding_source() {
    let catalog = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let definition = catalog
        .definition(
            "__array_struct_subfield",
            novarocks_functions::FunctionKind::Scalar,
        )
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    assert_eq!(declaration.overloads().len(), 1);
    for source in sources() {
        let mut seen = 0;
        for fragment in source.plan().fragments().values() {
            for (_source_node, unit, id, node) in super::sql_test_source::unit_definitions(fragment)
            {
                let ExprKind::FunctionCall { function, args } = &node.kind else {
                    continue;
                };
                if function.function_id.as_str() != "builtin.scalar/__array_struct_subfield/v1" {
                    continue;
                }
                assert_eq!(
                    function.overload.as_str(),
                    "builtin.scalar/__array_struct_subfield/dynamic-v1"
                );
                let request = unit.requests().get(id).unwrap();
                assert_eq!(args.len(), 2);
                assert_eq!(request.logical_argument_count, 2);
                assert_eq!(request.arguments.len(), 2);
                assert_eq!(function.argument_types.len(), 2);
                for (actual, selected) in
                    request.arguments.iter().zip(function.argument_types.iter())
                {
                    let (
                        StaticFunctionArgument::Value {
                            value_type: actual, ..
                        },
                        FunctionArgumentType::Value(selected),
                    ) = (actual, selected)
                    else {
                        panic!("actual Value source");
                    };
                    assert_eq!(actual, selected);
                }
                let FunctionArgumentType::Value(input) = &function.argument_types[0] else {
                    panic!("actual input");
                };
                let DataType::List(item) = &input.data_type() else {
                    panic!("actual List");
                };
                let DataType::Struct(fields) = item.data_type() else {
                    panic!("actual Struct child");
                };
                let original = fields.iter().find(|field| field.name() == "item").unwrap();
                let DataType::List(output) = &node.ty.data_type() else {
                    panic!("actual projected List");
                };
                assert_eq!(output.data_type(), original.data_type());
                assert_eq!(
                    unit.definition(args[1]).unwrap().ty.data_type().clone(),
                    DataType::Utf8
                );
                eprintln!(
                    "ARRAY_STRUCT_SUBFIELD actual definition={id:?} binding={function:?} request={request:?} fieldNameSource={:?} result={:?}",
                    unit.definition(args[1]),
                    node.ty
                );
                seen += 1;
            }
        }
        assert!(
            seen > 0,
            "SQL field projection must publish its array struct subfield call"
        );
    }
}
#[test]
fn array_struct_subfield_actual_required_sql_compiler_closure() {
    for source in sources() {
        let results = super::filter_conjunction_actual_sql_compiler_tests::compiler_results(
            &source,
            &super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue(),
        );
        assert!(!results.is_empty());
        for (fragment, program) in results {
            program.unwrap_or_else(|error| {
                panic!("array struct field projection fragment {fragment:?}: {error:?}")
            });
        }
    }
}
