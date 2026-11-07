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

//! Public prepared Cast -> native typed-zero/Sub -> Cast stimulus, with
//! independent narrow-boundary values and required overflow attribution.
use arrow::array::{Array, ArrayRef, Int8Array, Int64Array};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, PreparedNativeNegateRecipe, Selection, native_negate_computed_result_type,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("native NEGATE fixture never waits")
    }
}
#[test]
fn numeric_unary_narrow_source_raw_cast_negate_required_error_and_recipe_metadata() {
    // These are explicit fixture declarations. They are never reconstructed
    // from the Arrow arrays' carrier types or their observed NULL counts.
    let source_type = FunctionValueType::new(DataType::Int64, false);
    let narrow_type = FunctionValueType::new(DataType::Int8, false);
    let computed = native_negate_computed_result_type(&narrow_type, &Control).unwrap();
    assert_eq!(computed.data_type().clone(), DataType::Int8);
    assert!(!computed.nullable());
    let computed_wide = native_negate_computed_result_type(&source_type, &Control).unwrap();
    assert!(
        !computed_wide.nullable(),
        "Int64 has row overflow, not successful own NULL"
    );

    // -128 is representable in INT8. Its additive inverse +128 is not, so
    // the public NEGATE owner reports required NumericOutOfRange before any
    // narrowing could turn that boundary into a successful NULL.
    let input: ArrayRef = Arc::new(Int8Array::from(vec![i8::MIN]));
    assert_eq!(input.data_type(), &DataType::Int8);
    assert!(!input.is_null(0), "the explicit narrow input is valid -128");
    let recipe = PreparedNativeNegateRecipe::try_new(&narrow_type, &computed, &Control).unwrap();
    assert_eq!(recipe.source_type(), &narrow_type);
    assert_eq!(recipe.result_type(), &computed);
    let actual = recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&input),
            Selection::all(1),
            &Control,
        )
        .unwrap();
    assert_eq!(actual.errors().len(), 1);
    assert_eq!(actual.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        actual.errors()[0].reason(),
        novarocks_functions::DataFailureReason::NumericOutOfRange
    );
    assert_eq!(actual.values().data_type(), &DataType::Int8);
    assert_eq!(
        actual.values().to_data(),
        Int8Array::from(vec![None::<i8>]).to_data()
    );

    // Preserve the genuine three-operation stimulus separately. INT64->INT8
    // admits the full INT64 source domain, hence its declaration permits own
    // successful NULLs even though this exact -128 input narrows successfully.
    let cast_narrow_type = FunctionValueType::new(DataType::Int8, true);
    let narrow_cast = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &source_type,
        &cast_narrow_type,
        DecimalOverflowPolicy::OutputNull,
        false,
        &Control,
    )
    .unwrap();
    assert_eq!(narrow_cast.source_type(), &source_type);
    assert_eq!(narrow_cast.result_type(), &cast_narrow_type);
    let wide_input: ArrayRef = Arc::new(Int64Array::from(vec![-128]));
    let narrowed = narrow_cast
        .evaluate_row(EvaluatedArgument::Column(&wide_input), 0, 0, &Control)
        .unwrap();
    let CastRowResult::Signed(value) = narrowed else {
        panic!("the real INT64 -128 to INT8 cast must succeed with a signed value");
    };
    assert_eq!(value, -128);
    // Package the actual public scalar result for the next prepared owner;
    // this checked representation conversion performs no expected calculation.
    let narrowed: ArrayRef = Arc::new(Int8Array::from(vec![i8::try_from(value).unwrap()]));
    let chain_result =
        native_negate_computed_result_type(narrow_cast.result_type(), &Control).unwrap();
    let chain_negate =
        PreparedNativeNegateRecipe::try_new(narrow_cast.result_type(), &chain_result, &Control)
            .unwrap();
    let negated = chain_negate
        .evaluate_selected(
            EvaluatedArgument::Column(&narrowed),
            Selection::all(1),
            &Control,
        )
        .unwrap();
    assert_eq!(negated.errors().len(), 1);
    assert_eq!(negated.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        negated.errors()[0].reason(),
        novarocks_functions::DataFailureReason::NumericOutOfRange
    );
    assert_eq!(
        negated.values().to_data(),
        Int8Array::from(vec![None::<i8>]).to_data()
    );
    let widened_type = FunctionValueType::new(DataType::Int64, true);
    let widen_cast = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        chain_negate.result_type(),
        &widened_type,
        DecimalOverflowPolicy::OutputNull,
        false,
        &Control,
    )
    .unwrap();
    assert_eq!(widen_cast.source_type(), chain_negate.result_type());
    assert_eq!(widen_cast.result_type(), &widened_type);
    // A scalar caller cannot discard the required child journal and feed its
    // placeholder as a successful NULL into the next conversion.
    assert!(matches!(
        widen_cast.evaluate_row(EvaluatedArgument::SelectedColumn(&negated), 0, 0, &Control),
        Err(KernelFailure::InvalidProgram(_))
    ));

    // The separate CAST contract still has successful NULL: narrowing the
    // actual INT64 value 128 fails under OutputNull, then widening preserves it.
    let outside_narrow: ArrayRef = Arc::new(Int64Array::from(vec![128]));
    assert_eq!(
        narrow_cast
            .evaluate_row(EvaluatedArgument::Column(&outside_narrow), 0, 0, &Control)
            .unwrap(),
        CastRowResult::Null,
    );
    let cast_null: ArrayRef = Arc::new(Int8Array::from(vec![None::<i8>]));
    assert_eq!(
        widen_cast
            .evaluate_row(EvaluatedArgument::Column(&cast_null), 0, 0, &Control)
            .unwrap(),
        CastRowResult::Null,
    );

    // INT64 MIN follows the same checked rule: its inverse +2^63 is outside
    // INT64 and retains its original required row attribution.
    let wide_recipe =
        PreparedNativeNegateRecipe::try_new(&source_type, &computed_wide, &Control).unwrap();
    let minimum: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
    let overflow = wide_recipe
        .evaluate_selected(
            EvaluatedArgument::Column(&minimum),
            Selection::all(1),
            &Control,
        )
        .unwrap();
    assert_eq!(overflow.errors().len(), 1);
    assert_eq!(overflow.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        overflow.errors()[0].message(),
        "Arithmetic overflow: Overflow happened on: 0 - -9223372036854775808"
    );
    assert!(overflow.values().is_null(0));
}
