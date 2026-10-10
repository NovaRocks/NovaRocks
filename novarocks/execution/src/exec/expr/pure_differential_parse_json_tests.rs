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
//! Permanent success expectations over the full selected Utf8 -> nullable Json declaration.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{ArrayRef, StringArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType, ValueLogicalType};
use std::sync::Arc;
fn strings(v: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn result() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json).unwrap()
}
#[test]
fn parse_json_full_selected_profile_nullability_values_slices_hidden_null_and_selection() {
    let long = format!(
        "{{\"z\":\"{}\",\"a\":[true,null,1,2.5]}}",
        "é中\\n".repeat(260)
    );
    let a = strings(vec![
        Some("guard"),
        Some(&long),
        Some("[]"),
        Some("{\"b\":1,\"a\":2}"),
        Some("null"),
        Some(""),
        Some("["),
        None,
        Some("1e10000"),
        Some(r#""\ud800""#),
        Some("guard"),
    ]);
    for input in [
        a.clone(),
        a.slice(1, 9),
        strings(vec![None; 3]),
        strings(vec![]),
    ] {
        for allow in [false, true] {
            for policy in [
                DecimalOverflowPolicy::ReportError,
                DecimalOverflowPolicy::OutputNull,
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("parse_json")
                        .column(input.clone())
                        .expect_result_type(result())
                        .allow_throw_exception(allow)
                        .decimal_overflow(policy)
                        .sparse_selections(5, 811),
                );
            }
        }
    }
    for nullable in [false, true] {
        assert_scalar_matches_v1(
            ScalarDiffSpec::new("parse_json")
                .typed_column(
                    FunctionValueType::new(DataType::Utf8, nullable),
                    strings(vec![
                        Some("[1,2]"),
                        Some("invalid"),
                        Some("true"),
                        Some("null"),
                    ]),
                )
                .expect_result_type(result())
                .sparse_selections(5, 823),
        );
    }
    let prefix = "{hidden invalid payload}";
    let raw = format!("{prefix}[1,2]");
    let hidden: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(
            vec![
                0,
                i32::try_from(prefix.len()).unwrap(),
                i32::try_from(raw.len()).unwrap(),
            ]
            .into(),
        ),
        Buffer::from(raw.as_bytes()),
        Some(NullBuffer::from(vec![false, true])),
    ));
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("parse_json")
            .column(hidden)
            .expect_result_type(result())
            .sparse_selections(5, 829),
    );
}
#[test]
fn parse_json_full_selected_profile_scalar_and_pool_constant_values() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for value in [
            Some("[1,2]"),
            Some("{\"b\":1,\"a\":2}"),
            Some("invalid"),
            Some("null"),
            Some(""),
            None,
        ] {
            for rows in [0, 1, 7] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("parse_json")
                        .constant_array(strings(vec![value]))
                        .legacy_constants(form)
                        .constant_rows(rows)
                        .expect_result_type(result())
                        .sparse_selections(5, 839),
                );
            }
        }
    }
}
