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
use super::extract_checked_packages;
use crate::query_execution::tests::{
    complete_dependency_fixture, dependency_fixture_constant_policy,
};
use novarocks_expression_contract::ExprKind;
use novarocks_sql::compiler::{
    SessionOptimizerSettings, SqlAuthoredPhysicalPlan, SqlCompileControl,
};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

#[test]
fn native_decimal_array_and_temporal_literal_compile_sources_remain_valid() {
    use arrow::datatypes::{DataType, Field};
    let decimal_items = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Decimal128(3, 2),
        true,
    )));
    let double_items = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
    for (sql, expected) in [
        // One integer digit and two fractional digits require DECIMAL(3,2).
        ("SELECT [0.0,0.25,0.5,1.0]", decimal_items),
        (
            "SELECT percentile_approx_weighted(2.0,1,[0.0,0.25,0.5,1.0],2048)",
            double_items.clone(),
        ),
        (
            "SELECT d = '2020-01-01' FROM (SELECT CAST('2020-01-01' AS DATE) AS d) t",
            DataType::Boolean,
        ),
        (
            "SELECT ABS(CAST(99999999.9949 AS DECIMAL(13,4))) > 99999999.9999",
            DataType::Boolean,
        ),
        // An explicit DOUBLE item declaration remains distinct from inference.
        (
            "SELECT CAST([0.0,0.25,0.5,1.0] AS ARRAY<DOUBLE>)",
            double_items,
        ),
    ] {
        let owner = complete_dependency_fixture(
            sql,
            SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..Default::default()
            },
        );
        let result = owner.plan().result_port().expect("public query result");
        assert_eq!(result.fields.len(), 1, "{sql}");
        assert_eq!(result.fields[0].ty.data_type(), &expected, "{sql}");
        assert_eq!(
            result.fields[0].ty.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical,
            "{sql}",
        );
        assert_checked_packages_for_source(sql, owner);
    }
}

fn assert_checked_packages_for_source(sql: &str, owner: SqlAuthoredPhysicalPlan) {
    let control = SqlCompileControl::unbounded();
    let fragment_count = owner.plan().fragments().len();
    let candidate =
        novarocks_query_application::preparation::CompletedPhysicalPlanCandidate::for_sql_program(
            owner, &control,
        )
        .unwrap_or_else(|error| panic!("real SQL plan candidate for {sql}: {error:#?}"));
    let packages = extract_checked_packages(
        &candidate,
        &std::collections::BTreeMap::new(),
        &std::collections::BTreeMap::new(),
        dependency_fixture_constant_policy(),
        &novarocks_plan_codec::physical_package_v2::PackageDecodeLimits::FROZEN.admission,
        &control,
    )
    .unwrap_or_else(|error| panic!("real literal package extraction for {sql}: {error:#?}"));
    assert_eq!(packages.len(), fragment_count, "{sql}");
    assert!(
        packages
            .values()
            .all(|package| package.statement_constant_policy()
                == dependency_fixture_constant_policy()),
        "{sql}"
    );
}

fn assert_real_null_list_cast(
    sql: &str,
    columns: &[(&str, novarocks_type_contract::ValueLogicalType, usize)],
) {
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let expected = columns
        .iter()
        .map(|(name, domain, len)| {
            let item =
                FunctionValueType::try_with_logical_type(DataType::Utf8, true, *domain).unwrap();
            let value_type = FunctionValueType::try_from_value(
                ValueTypeRef::try_new(
                    ValueKind::List(item.value_type().clone()),
                    true,
                    ValueTypeStorageLifetime::pure(),
                )
                .unwrap(),
                ValueLayout::try_from_normalized_arrow(
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    ValueTypeStorageLifetime::pure(),
                )
                .unwrap(),
            )
            .unwrap();
            (*name, *len, value_type)
        })
        .collect::<Vec<_>>();
    // Keep the declared runtime conversion and its independent ARRAY source.
    let owner = complete_dependency_fixture(
        sql,
        SessionOptimizerSettings {
            disabled_rules: vec!["FoldConstant".into()],
            enable_materialized_view_rewrite: Some(false),
            enable_common_subexpr_reuse: Some(false),
            ..Default::default()
        },
    );
    let computed = owner.plan().result_port().expect("computed query result");
    let result = owner
        .checked_original_result_declaration_observed(&SqlCompileControl::unbounded())
        .expect("same-owner declaration association")
        .expect("original public declaration");
    assert_eq!(computed.fields.len(), expected.len(), "{sql}");
    assert_eq!(result.fields().len(), expected.len(), "{sql}");
    for (computed, published) in computed.fields.iter().zip(result.fields()) {
        assert_eq!(
            computed.ty, published.ty,
            "computed and published full type"
        );
    }
    let source_type = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
        false,
    );
    for (field, (name, expected_items, expected_type)) in result.fields().iter().zip(&expected) {
        // This compiler fixture exposes the original expression declaration.
        // Final client aliases are checked by the native SQL result oracle.
        assert_eq!(&field.ty, expected_type, "{sql}: {name}");
        assert_eq!(
            field.ty.try_to_field(*name).unwrap(),
            expected_type.try_to_field(*name).unwrap(),
        );
        let mut actual_casts = 0;
        let units = owner
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| fragment.nodes().values())
            .filter_map(|node| node.expressions.as_ref());
        for (unit, expression) in units.flat_map(|unit| {
            unit.definitions()
                .iter()
                .map(move |expression| (unit, expression))
        }) {
            let ExprKind::Cast { expr, target, .. } = &expression.kind else {
                continue;
            };
            if expression.ty != *expected_type {
                continue;
            }
            actual_casts += 1;
            assert_eq!(target, expected_type.data_type());
            let input = unit.definition(*expr).expect("actual CAST child");
            assert_eq!(
                input.ty, source_type,
                "independent common NULL element type"
            );
            let ExprKind::FunctionCall { function, args } = &input.kind else {
                panic!("the actual unfurled ARRAY source must remain: {input:#?}");
            };
            assert_eq!(function.result_type, source_type);
            assert_eq!(args.len(), *expected_items);
            for argument in args.iter() {
                assert_eq!(
                    unit.definition(*argument).unwrap().ty,
                    FunctionValueType::new(DataType::Null, true),
                    "original NULL element declaration",
                );
            }
        }
        assert_eq!(
            actual_casts, 1,
            "one exact runtime conversion for {name}: {sql}"
        );
    }
    assert_checked_packages_for_source(sql, owner);
}

#[test]
fn real_fe_empty_array_cast_preserves_complete_json_list_output() {
    assert_real_null_list_cast(
        "SELECT CAST([] AS ARRAY<JSON>) AS json_values",
        &[(
            "json_values",
            novarocks_type_contract::ValueLogicalType::Json,
            0,
        )],
    );
}

#[test]
fn real_fe_null_child_array_cast_preserves_complete_json_list_output() {
    assert_real_null_list_cast(
        "SELECT CAST([NULL,NULL] AS ARRAY<JSON>) AS json_values",
        &[(
            "json_values",
            novarocks_type_contract::ValueLogicalType::Json,
            2,
        )],
    );
}

#[test]
fn real_fe_empty_array_cast_preserves_complete_varchar_list_output() {
    assert_real_null_list_cast(
        "SELECT CAST([] AS ARRAY<VARCHAR>) AS string_values",
        &[(
            "string_values",
            novarocks_type_contract::ValueLogicalType::Physical,
            0,
        )],
    );
}

#[test]
fn real_fe_null_child_array_cast_preserves_complete_varchar_list_output() {
    assert_real_null_list_cast(
        "SELECT CAST([NULL,NULL] AS ARRAY<VARCHAR>) AS string_values",
        &[(
            "string_values",
            novarocks_type_contract::ValueLogicalType::Physical,
            2,
        )],
    );
}

#[test]
fn real_fe_joint_empty_json_and_varchar_casts_keep_distinct_complete_outputs() {
    assert_real_null_list_cast(
        "SELECT CAST([] AS ARRAY<JSON>) AS json_values, CAST([] AS ARRAY<VARCHAR>) AS string_values",
        &[
            (
                "json_values",
                novarocks_type_contract::ValueLogicalType::Json,
                0,
            ),
            (
                "string_values",
                novarocks_type_contract::ValueLogicalType::Physical,
                0,
            ),
        ],
    );
}
