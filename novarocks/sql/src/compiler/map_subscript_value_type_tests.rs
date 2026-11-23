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
use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId};
use novarocks_result_contract::{
    CharacterKind, CharacterType, NativeRenderType, RenderTemporalType,
};
use novarocks_type_contract::ValueLogicalType;

#[test]
fn public_sql_map_subscript_widens_integer_probe_and_preserves_value_declaration() {
    // The input map already has the common BIGINT key type. Its value
    // declaration must survive INTEGER probes, NULL probes and aliases.
    let decimal = NativeRenderType::Decimal {
        bits: 128,
        precision: 19,
        scale: 0,
    };
    for (sql, carrier, native_type) in [
        (
            "SELECT map_agg(BIGINT '11111', CAST('2024-09-01 18:00:00' AS TIMESTAMP(6)))[11111]",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            NativeRenderType::Temporal(RenderTemporalType::Timestamp(6)),
        ),
        (
            "SELECT map_agg(BIGINT '11111', DECIMAL '9223372036854775808')[11111]",
            DataType::Decimal128(19, 0),
            decimal.clone(),
        ),
        (
            "SELECT map_agg(BIGINT '11111', DECIMAL '9223372036854775808')[NULL]",
            DataType::Decimal128(19, 0),
            decimal,
        ),
        (
            "SELECT m[k] FROM (SELECT map_agg(BIGINT '11111', 'selected') AS m, 11111 AS k) q",
            DataType::Utf8,
            NativeRenderType::BoundedCharacter(
                CharacterType::new(CharacterKind::Varchar, 8).unwrap(),
            ),
        ),
    ] {
        let request = SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([29; 16]).unwrap(),
            SqlStatementInput::parsed_query(crate::compiler::test_query(sql)),
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
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            crate::constant::test_constant_policy(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
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
        assert_eq!(
            result.fields[0].ty.logical_type(),
            ValueLogicalType::Physical,
            "{sql}"
        );
        assert!(result.fields[0].ty.nullable(), "missing lookup key: {sql}");
        let render = crate::compiler::root_render_type::client_render_schema(
            completed.plan(),
            crate::compiler::RenderFamilyRequest::TrinoJson(
                &novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true,
                },
            ),
        )
        .unwrap_or_else(|error| panic!("client render declaration for {sql}: {error}"));
        assert_eq!(render.columns()[0].field.native_type, native_type, "{sql}");
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
            SqlStatementInput::parsed_query(crate::compiler::test_query(sql)),
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
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            crate::constant::test_constant_policy(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
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
