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

//! Public SQL completion must use the map's complete declared key type.

use super::*;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_physical_plan::{
    MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
};
use novarocks_type_contract::ValueLogicalType;

#[test]
fn public_sql_map_subscript_preserves_largeint_key_identity() {
    for (sql, carrier, domain) in [
        (
            "SELECT map_agg(CAST('11111' AS LARGEINT), CAST('2024-09-01 18:00:00' AS DATETIME))[11111]",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            ValueLogicalType::Physical,
        ),
        (
            "SELECT map(CAST('11111' AS LARGEINT), CAST('9223372036854775808' AS LARGEINT))[11111]",
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
        ),
        (
            "SELECT map(CAST('11111' AS LARGEINT), CAST('9223372036854775808' AS LARGEINT))[NULL]",
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
        ),
        (
            "SELECT m[k] FROM (SELECT map(CAST('11111' AS LARGEINT), 'selected') AS m, 11111 AS k) q",
            DataType::Utf8,
            ValueLogicalType::Physical,
        ),
    ] {
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([29; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "db".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            crate::constant::test_constant_policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        );
        let SqlCompileProgress::Complete(completed) = SqlCompiler::start(
            request.try_into_completion().unwrap(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap_or_else(|error| panic!("public SQL completion failed for {sql}: {error:?}")) else {
            panic!("source-free map lookup unexpectedly needs observations: {sql}");
        };
        let result = completed.plan().result_port().unwrap();
        assert_eq!(result.fields.len(), 1, "{sql}");
        assert_eq!(result.fields[0].ty.data_type(), &carrier, "{sql}");
        assert_eq!(result.fields[0].ty.logical_type(), domain, "{sql}");
        assert!(result.fields[0].ty.nullable(), "missing lookup key: {sql}");
        crate::compiler::root_render_type::client_render_schema(completed.plan(), 0)
            .unwrap_or_else(|error| panic!("client render declaration for {sql}: {error}"));
        novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
    }
}

#[test]
fn public_sql_empty_map_lookup_keeps_uninhabited_key_and_actual_probe() {
    for (ordinal, sql) in [
        "SELECT map{}",
        "SELECT map{}['a']",
        "SELECT element_at(map{}, 'a')",
        "SELECT m[p] FROM (SELECT map{} AS m, 'a' AS p) q",
    ]
    .into_iter()
    .enumerate()
    {
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([31; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "db".into(),
                optimizer_settings: SessionOptimizerSettings {
                    disabled_rules: vec!["FoldConstant".into()],
                    ..Default::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            crate::constant::test_constant_policy(),
            SqlPhysicalEmissionMode::OriginalNativeV1,
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        );
        let SqlCompileProgress::Complete(completed) = SqlCompiler::start(
            request.try_into_completion().unwrap_or_else(|error| {
                panic!("actual public completion failed for {sql}: {error:?}")
            }),
            &SqlCompileControl::unbounded(),
        )
        .unwrap_or_else(|error| panic!("actual public compiler failed for {sql}: {error:?}")) else {
            panic!("source-free query unexpectedly needs observations: {sql}");
        };
        let result = completed.plan().result_port().unwrap();
        assert_eq!(result.fields.len(), 1, "{sql}");
        let ty = &result.fields[0].ty;
        if ordinal == 0 {
            let DataType::Map(entries, _) = ty.data_type() else {
                panic!("empty Map: {ty:?}");
            };
            let DataType::Struct(fields) = entries.data_type() else {
                panic!("Map entries");
            };
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].data_type(), &DataType::Null);
            assert!(
                !fields[0].is_nullable(),
                "Map key contract remains non-null"
            );
            assert_eq!(fields[1].data_type(), &DataType::Null);
            assert!(fields[1].is_nullable());
        } else {
            assert_eq!(ty.data_type(), &DataType::Null, "{sql}");
            assert_eq!(ty.logical_type(), ValueLogicalType::Physical, "{sql}");
            assert!(ty.nullable(), "empty lookup result: {sql}");
        }
        novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
    }
}
