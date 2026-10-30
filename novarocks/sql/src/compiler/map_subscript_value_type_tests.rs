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
    // The lookup must retain the complete LargeInt key/value declarations.
    // Installed map_agg supplies those same declarations without depending on
    // the unavailable scalar map constructor; keep all lookup/nullability oracles.
    for (sql, carrier, domain) in [
        (
            "SELECT map_agg(CAST('11111' AS LARGEINT), CAST('2024-09-01 18:00:00' AS DATETIME))[11111]",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            ValueLogicalType::Physical,
        ),
        (
            "SELECT map_agg(CAST('11111' AS LARGEINT), CAST('9223372036854775808' AS LARGEINT))[11111]",
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
        ),
        (
            "SELECT map_agg(CAST('11111' AS LARGEINT), CAST('9223372036854775808' AS LARGEINT))[NULL]",
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
        ),
        (
            "SELECT m[k] FROM (SELECT map_agg(CAST('11111' AS LARGEINT), 'selected') AS m, 11111 AS k) q",
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
        crate::compiler::root_render_type::client_render_schema(
            completed.plan(),
            crate::compiler::RenderFamilyRequest::MysqlText {
                timezone_offset_seconds: 0,
            },
        )
        .unwrap_or_else(|error| panic!("client render declaration for {sql}: {error}"));
        novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
    }
}

#[test]
fn public_sql_empty_map_lookup_keeps_uninhabited_key_and_actual_probe() {
    // The installed aggregate emits an empty map for NULL keys while retaining
    // the uninhabited key type. The scalar map constructor is declaration-only.
    for (ordinal, sql) in [
        "SELECT map_agg(NULL,NULL)",
        "SELECT map_agg(NULL,NULL)['a']",
        "SELECT element_at(map_agg(NULL,NULL), 'a')",
        "SELECT m[p] FROM (SELECT map_agg(NULL,NULL) AS m, 'a' AS p) q",
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

#[test]
fn public_sql_map_float_probe_keeps_nullable_cast_before_optional_folding() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Probe;
    static PROBE: Probe = Probe;
    static OBSERVED: AtomicUsize = AtomicUsize::new(0);
    impl SqlConstantEvaluator for Probe {
        fn eval_scalar(
            &self,
            request: &FoldRequest,
            _: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<Option<novarocks_functions::ConstantValue>, SqlConstantEvaluationError>
        {
            if matches!(
                &request.kind,
                FrozenFoldNode::Intrinsic(FrozenFoldIntrinsic::Cast { .. })
            ) && request.result_type.data_type() == &DataType::Int32
                && request.args.len() == 1
                && request.args[0].value_type.data_type() == &DataType::Float64
            {
                assert_eq!(
                    request.result_type.logical_type(),
                    ValueLogicalType::Physical
                );
                assert!(
                    request.result_type.nullable(),
                    "float-to-int can return successful NULL before late lowering repairs"
                );
                assert!(!request.args[0].value_type.nullable());
                assert_eq!(
                    request.args[0].value.value_type(),
                    &request.args[0].value_type
                );
                OBSERVED.fetch_add(1, Ordering::SeqCst);
            }
            // Observe the actual frozen source only; do not supply another
            // evaluator or convert a computed value into a replacement literal.
            Ok(None)
        }
    }
    for sql in [
        "SELECT map_agg(CAST(1 AS INT),'one')[1e0]",
        "SELECT map_agg(CAST(1 AS INT),'one')[1e100]",
    ] {
        let before = OBSERVED.load(Ordering::SeqCst);
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([37; 16]).unwrap(),
            SqlStatementInput::sql(sql),
            SqlCompileIntent::Query,
            SqlSessionContext {
                sql_semantics: Default::default(),
                current_catalog: None,
                current_database: "db".into(),
                optimizer_settings: Default::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            &PROBE,
            crate::constant::test_constant_policy(),
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
        .unwrap_or_else(|error| panic!("actual map_agg/probe source {sql}: {error:?}")) else {
            panic!("source-free map_agg must not need table metadata")
        };
        assert!(
            OBSERVED.load(Ordering::SeqCst) > before,
            "the original float CAST must reach optional folding: {sql}"
        );
        let owner = completed.into_plan();
        let result = owner.plan().result_port().unwrap();
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].ty.data_type(), &DataType::Utf8);
        assert_eq!(
            result.fields[0].ty.logical_type(),
            ValueLogicalType::Physical
        );
        assert!(result.fields[0].ty.nullable());
        author_fragment_package_semantics(
            &owner,
            crate::constant::test_constant_policy(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap_or_else(|error| panic!("real package source for {sql}: {error:#?}"));
        novarocks_physical_plan::validate_plan(owner.plan()).unwrap();
    }
}
