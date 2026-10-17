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
//! Real SQL fact/resume sources and complete BY window compilation oracles.
use super::filter_conjunction_actual_sql_compiler_tests::compiler_results_with_declared_read_fields;
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_declared_fields;
use arrow::datatypes::{DataType, Field};
use novarocks_physical_plan::ExprKind;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode};
fn fields(nullable: bool) -> Vec<Field> {
    vec![
        Field::new("c0", DataType::Int32, nullable),
        Field::new("c1", DataType::Int32, nullable),
        Field::new("c2", DataType::Decimal128(7, 2), nullable),
        Field::new("c3", DataType::Utf8, nullable),
    ]
}
fn running_fields() -> Vec<Field> {
    vec![
        Field::new("seq", DataType::Int32, true),
        Field::new("v", DataType::Int32, true),
        Field::new("k", DataType::Decimal128(18, 9), true),
        Field::new("neg_k", DataType::Decimal128(18, 9), true),
        Field::new("d", DataType::Decimal128(18, 9), true),
    ]
}
fn inspect(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut n = 0;
    for (fragment_id, fragment) in source.plan().fragments() {
        for (definition, node) in fragment.expressions().iter() {
            if let ExprKind::WindowCall {
                function,
                aggregate_binding: Some(binding),
                function_order_by,
                frame,
                args,
                ..
            } = &node.kind
            {
                if !matches!(
                    function.function_id.as_str(),
                    "builtin.aggregate/max_by/v1" | "builtin.aggregate/min_by/v1"
                ) {
                    continue;
                }
                n += 1;
                println!(
                    "actual BY window fragment={fragment_id:?} definition={definition:?} function={function:?} binding={binding:?} args={args:?} frame={frame:?} function_order={function_order_by:?} result={:?}",
                    node.ty
                );
                assert_eq!(args.len(), 2);
                assert_eq!(function.kind, novarocks_functions::FunctionKind::Aggregate);
                assert_eq!(
                    binding.phase,
                    novarocks_physical_plan::AggregatePhase::Single
                );
                assert!(function_order_by.is_empty());
                assert!(node.ty.nullable());
            }
        }
    }
    n
}
fn compile(sql: &str, fields: &[Field]) {
    let source = sql_source_with_declared_fields(
        sql,
        fields,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    assert_eq!(inspect(&source), 2);
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    let programs = compiler_results_with_declared_read_fields(&source, fields, &functions)
        .into_iter()
        .map(|(id, result)| (id, result.unwrap()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert!(!programs.is_empty());
    assert!(programs.values().any(
        |program| program.graph().nodes().iter().any(|node| matches!(
            node.kind(),
            novarocks_local_program::ProgramNodeKind::Analytic { .. }
        ))
    ));
}

const RUNNING: &str = "SELECT seq,\n       max_by(v,k) OVER (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v,\n       min_by(v,k) OVER (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v\nFROM fixture.max_min_by_null_value ORDER BY seq;";
const TEXT_KEY: &str = "select /*+ SET_VAR(new_planner_agg_stage='1') */\n  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint\nfrom (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;";
const NONNULL_INT: &str = "select /*+ SET_VAR(new_planner_agg_stage='1') */\n  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint\nfrom (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;";

#[test]
fn by_window_sql_source_required_three_case_full_binding_receipts() {
    for (sql, fields) in [
        (RUNNING, running_fields()),
        (TEXT_KEY, fields(true)),
        (NONNULL_INT, fields(false)),
    ] {
        let source = sql_source_with_declared_fields(
            sql,
            &fields,
            SqlPhysicalEmissionMode::OriginalNativeV1,
        );
        assert_eq!(inspect(&source), 2);
    }
}
#[test]
fn by_window_actual_compiler_required_running_int32_decimal_key() {
    compile(RUNNING, &running_fields());
}
#[test]
fn by_window_actual_compiler_required_nullable_decimal_value_utf8_key() {
    compile(TEXT_KEY, &fields(true));
}
#[test]
fn by_window_actual_compiler_required_nonnull_int_expression_key() {
    compile(NONNULL_INT, &fields(false));
}
