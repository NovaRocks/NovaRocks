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
use novarocks_sql::compiler::{SessionOptimizerSettings, SqlCompileControl};
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
        ("SELECT ARRAY<DOUBLE>[0.0,0.25,0.5,1.0]", double_items),
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
}
