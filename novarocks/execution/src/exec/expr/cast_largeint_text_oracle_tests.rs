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
//! Exact Physical LARGEINT text recipe with fixed signed decimal literals.
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::time::Duration;
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
        panic!("LARGEINT text never waits")
    }
}
#[test]
fn largeint_text_original_v1_all_policy_nullability_min_max_slice_and_empty() {
    for nullable in [false, true] {
        let mut values = vec![
            Some(i128::MIN),
            Some(i128::MAX),
            Some(-1),
            Some(0),
            Some(42),
        ];
        if nullable {
            values.push(None);
        }
        let input = novarocks_types::largeint::array_from_i128(&values).unwrap();
        let fixed = [
            Some("-170141183460469231731687303715884105728"),
            Some("170141183460469231731687303715884105727"),
            Some("-1"),
            Some("0"),
            Some("42"),
            None,
        ];
        for (source, expected_rows) in [
            (input.clone(), &fixed[..input.len()]),
            (input.slice(1, 3), &fixed[1..4]),
            (input.slice(0, 0), &[][..]),
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &FunctionValueType::new(DataType::FixedSizeBinary(16), nullable),
                        &FunctionValueType::new(DataType::Utf8, nullable),
                        policy,
                        allow,
                        None,
                        &Control,
                    )
                    .unwrap();
                    for (row, expected) in expected_rows.iter().enumerate() {
                        let expected = expected
                            .map_or(CastRowResult::Null, |text| CastRowResult::Text(text.into()));
                        assert_eq!(
                            r.evaluate_row(EvaluatedArgument::Column(&source), row, row, &Control)
                                .unwrap(),
                            expected
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn largeint_text_shared_raw_adapter_retains_original_full_errors() {
    let wrong: ArrayRef = std::sync::Arc::new(arrow::array::Int64Array::from(vec![1]));
    let refusal = novarocks_functions::largeint_text::cast_array(&wrong).unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert!(matches!(
        refusal,
        novarocks_functions::largeint::LargeIntCarrierFailure::ExpectedArray(_)
    ));
    assert_eq!(refusal.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(refusal.failure_fact().reason().code(), 292);
    let arr = novarocks_types::largeint::array_from_i128(&[Some(-1)]).unwrap();
    assert_eq!(
        novarocks_functions::largeint_text::cast_array(&arr)
            .unwrap()
            .to_data(),
        StringArray::from(vec!["-1"]).to_data()
    );
}
