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
//! Actual original corpus call subtree with original no-op fold author retained in both modes.
//! The corpus outer Json -> Struct conversion already fails in Original and is a separate gate.
use arrow::datatypes::DataType;
use novarocks_physical_plan::{ExprKind, PhysicalCallDefinition, StaticFunctionArgument};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_type_contract::{FunctionArgumentType, FunctionValueType, ValueLogicalType};
const SQL: &str = "SELECT parse_json('[1,2,3]') AS parsed";
fn check(mode: SqlPhysicalEmissionMode) {
    let source =
        super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields(
            SQL,
            &[],
            mode,
        );
    let mut count = 0;
    for fragment in source.plan().fragments().values() {
        for (id, node) in fragment.expressions().iter() {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                continue;
            };
            if function.function_id.as_str() != "builtin.scalar/parse_json/v1" {
                continue;
            }
            let request = fragment
                .call_requests()
                .get(PhysicalCallDefinition::Expression(*id))
                .unwrap();
            assert_eq!(request.logical_argument_count, 1);
            assert_eq!(request.arguments.len(), 1);
            assert_eq!(args.len(), 1);
            let StaticFunctionArgument::Value { value_type, .. } = &request.arguments[0] else {
                panic!("original Value channel")
            };
            assert_eq!(value_type, &FunctionValueType::new(DataType::Utf8, false));
            assert_eq!(
                function.argument_types.as_ref(),
                [FunctionArgumentType::Value(value_type.clone())]
            );
            assert_eq!(&fragment.expressions().get(args[0]).unwrap().ty, value_type);
            let result = FunctionValueType::try_with_logical_type(
                DataType::Utf8,
                true,
                ValueLogicalType::Json,
            )
            .unwrap();
            assert_eq!(function.result_type, result);
            assert_eq!(node.ty, result);
            eprintln!(
                "PARSE_JSON actual kept call mode={mode:?} fragment={:?} definition={id:?} selected={function:?} request={request:?} children={args:?}",
                fragment.id()
            );
            count += 1;
        }
    }
    assert_eq!(
        count, 1,
        "same original corpus call subtree survives no-op fold"
    );
}
#[test]
fn parse_json_actual_original_sql_kept_call_source() {
    check(SqlPhysicalEmissionMode::OriginalNativeV1)
}
#[test]
fn parse_json_actual_candidate_sql_kept_call_source() {
    check(SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration)
}
