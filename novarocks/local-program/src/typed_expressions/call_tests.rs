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

//! Real sealed-catalogue tokens and actual local occurrence forests. These
//! tests verify correspondence, not complete expression/capture/runtime proof.

use super::*;
use crate::resolved_calls::tests::{TypedCallFixture, typed_call_fixture};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{EvaluationDemand, ValueLogicalType};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn value(ty: FunctionValueType) -> FunctionArgumentType {
    FunctionArgumentType::Value(ty)
}
fn integer() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn boolean() -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, true)
}
fn ordinary_types() -> Vec<FunctionArgumentType> {
    vec![value(integer()), value(integer()), value(boolean())]
}
fn table(
    entries: Vec<FunctionArgumentType>,
) -> BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>> {
    BTreeMap::from([(ProgramExpressionArena::Main, entries)])
}
fn assert_mismatch(calls: ProgramResolvedCalls, entries: Vec<FunctionArgumentType>) {
    assert_eq!(
        ProgramTypedExpressions::try_new(calls, table(entries), &Control).unwrap_err(),
        ProgramExpressionTypeError::TypeMismatch
    );
}

#[test]
fn frozen_scalar_signature_cannot_weaken_result_nullability_or_admit_nullable_arguments() {
    let calls = typed_call_fixture(TypedCallFixture::Scalar);
    let typed =
        ProgramTypedExpressions::try_new(calls.clone(), table(ordinary_types()), &Control).unwrap();
    assert_eq!(typed.resolved_calls().calls().len(), 1);
    let mut wrong_result = ordinary_types();
    wrong_result[2] = value(FunctionValueType::new(DataType::Boolean, false));
    assert_mismatch(calls.clone(), wrong_result);
    let mut wrong_argument = ordinary_types();
    wrong_argument[0] = value(FunctionValueType::new(DataType::Int64, true));
    assert_mismatch(calls, wrong_argument);
}

#[test]
fn type_only_dead_channels_still_require_exact_owner_static_types() {
    let calls = typed_call_fixture(TypedCallFixture::TypeOnly);
    assert_eq!(
        calls.snapshot().flows()[&ProgramExpressionArena::Main]
            .uses()
            .len(),
        1
    );
    ProgramTypedExpressions::try_new(calls.clone(), table(ordinary_types()), &Control).unwrap();
    let mut wrong = ordinary_types();
    wrong[1] = value(FunctionValueType::new(DataType::Int64, true));
    assert_mismatch(calls, wrong);
    // Admission can widen only top-level NULL presence in a value argument.
    let admitted = FunctionValueType::new(DataType::Int64, true);
    let calls = typed_call_fixture(TypedCallFixture::TypedTypeOnlyWithSources {
        selected: [admitted.clone(), admitted],
        sources: [integer(), integer()],
    });
    ProgramTypedExpressions::try_new(calls, table(ordinary_types()), &Control).unwrap();
}

#[test]
fn unchanged_nullable_boolean_type_is_preserved_for_truth_only_use() {
    let calls = typed_call_fixture(TypedCallFixture::TruthOnly);
    assert_eq!(
        calls
            .calls()
            .values()
            .next()
            .unwrap()
            .call_contract()
            .context()
            .demand,
        EvaluationDemand::TruthOnly
    );
    let typed =
        ProgramTypedExpressions::try_new(calls.clone(), table(ordinary_types()), &Control).unwrap();
    let Some(FunctionArgumentType::Value(actual)) =
        typed.definition_type(ProgramExpressionArena::Main, ProgramExprId::new(2))
    else {
        unreachable!()
    };
    assert!(actual.nullable());
    let FunctionResultType::Scalar(selected_result) = &typed
        .resolved_calls()
        .calls()
        .values()
        .next()
        .unwrap()
        .call_contract()
        .selected()
        .result_type
    else {
        unreachable!()
    };
    assert!(selected_result.nullable());
    let mut wrong = ordinary_types();
    wrong[2] = value(FunctionValueType::new(DataType::Boolean, false));
    assert_mismatch(calls, wrong);
}

#[test]
fn same_arrow_carrier_does_not_erase_frozen_root_logical_identity() {
    for (carrier, logical) in [
        (DataType::Utf8, ValueLogicalType::Json),
        (DataType::LargeBinary, ValueLogicalType::Variant),
        (DataType::Binary, ValueLogicalType::Hll),
        (DataType::LargeBinary, ValueLogicalType::Bitmap),
        (DataType::LargeBinary, ValueLogicalType::Object),
        (DataType::Binary, ValueLogicalType::Percentile),
    ] {
        let exact =
            FunctionValueType::try_with_logical_type(carrier.clone(), true, logical).unwrap();
        let calls = typed_call_fixture(TypedCallFixture::TypedTypeOnly([
            exact.clone(),
            exact.clone(),
        ]));
        let entries = vec![value(exact.clone()), value(exact), value(boolean())];
        ProgramTypedExpressions::try_new(calls.clone(), table(entries.clone()), &Control).unwrap();
        let mut wrong = entries;
        wrong[0] = value(FunctionValueType::new(carrier, true));
        assert_mismatch(calls, wrong);
    }
    let hll =
        FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Hll)
            .unwrap();
    let bitmap =
        FunctionValueType::try_with_logical_type(DataType::Binary, true, ValueLogicalType::Bitmap)
            .unwrap();
    let calls = typed_call_fixture(TypedCallFixture::TypedTypeOnly([hll.clone(), hll.clone()]));
    assert_mismatch(calls, vec![value(bitmap), value(hll), value(boolean())]);
}

#[allow(deprecated)]
fn nested(dictionary_id: i64, json: bool, nullable: bool) -> FunctionValueType {
    let source = DataType::Struct(
        vec![
            Field::new_dict(
                "encoded",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                nullable,
                dictionary_id,
                false,
            ),
            Field::new("json", DataType::Utf8, true),
        ]
        .into(),
    );
    crate::test_value_types::string_structure_from_source(
        &[("encoded", nullable, false), ("json", true, json)],
        true,
        &source,
    )
}
#[test]
fn nested_dictionary_identity_logical_metadata_and_null_fields_remain_exact() {
    let exact = nested(7, true, true);
    let calls = typed_call_fixture(TypedCallFixture::TypedTypeOnly([
        exact.clone(),
        exact.clone(),
    ]));
    ProgramTypedExpressions::try_new(
        calls.clone(),
        table(vec![
            value(exact.clone()),
            value(exact.clone()),
            value(boolean()),
        ]),
        &Control,
    )
    .unwrap();
    for wrong in [nested(7, false, true), nested(7, true, false)] {
        assert_mismatch(
            calls.clone(),
            vec![value(wrong), value(exact.clone()), value(boolean())],
        );
    }
    ProgramTypedExpressions::try_new(
        calls,
        table(vec![
            value(nested(8, true, true)),
            value(exact),
            value(boolean()),
        ]),
        &Control,
    )
    .unwrap();
}
