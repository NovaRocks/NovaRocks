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

//! Exact Binary CAST type, address and first-failure observation contracts.
use super::super::*;
use crate::{ConstantPolicy, ConstantPool, KernelDiagnostic, SelectedValues, Selection};
use arrow_array::{ArrayRef, BinaryArray};
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refuse: Option<(usize, KernelFailure)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refuse {
            assert!(at <= *stop, "callback after refusal");
        }
        t.push(n);
        if let Some((stop, cause)) = &self.refuse
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("binary CAST never waits")
    }
}
fn recipe(nullable: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::Binary, nullable),
        &FunctionValueType::new(DataType::Utf8, true),
        DecimalOverflowPolicy::ReportError,
        true,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn binary_text_recipe_exact_type_root_null_obligation_and_refuses_other_routes() {
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let r = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &FunctionValueType::new(DataType::Binary, nullable),
                    &FunctionValueType::new(DataType::Utf8, true),
                    policy,
                    allow,
                    &Control::default(),
                )
                .unwrap();
                let context = ExpressionEffectContext {
                    use_id: ExpressionUseId::new(1),
                    domain: EvaluationDomainId::new(2),
                    demand: EvaluationDemand::Value,
                };
                assert!(
                    !r.own_effects(context)
                        .for_use(context)
                        .unwrap()
                        .may_raise_row_error
                );
                assert!(carrier_cast_can_produce_null(
                    &DataType::Binary,
                    &DataType::Utf8,
                    allow
                ));
                assert_eq!(
                    PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &FunctionValueType::new(DataType::Binary, nullable),
                        &FunctionValueType::new(DataType::Utf8, false),
                        policy,
                        allow,
                        &Control::default()
                    ),
                    Err(CastPrepareError::TypeMismatch)
                );
            }
        }
    }
    // LargeBinary now uses the admitted Arrow byte-width route; Fixed->UTF8
    // remains outside Arrow's supported direct cast pairs.
    super::super::tests::assert_f3_selected_literal_case(
        Arc::new(arrow_array::LargeBinaryArray::from(vec![
            Some(b"valid\0text".as_slice()),
            None,
        ])),
        Arc::new(arrow_array::StringArray::from(vec![
            Some("valid\0text"),
            None,
        ])),
    );
    for ty in [
        DataType::FixedSizeBinary(1),
        DataType::FixedSizeBinary(15),
        DataType::FixedSizeBinary(17),
    ] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(ty, true),
                &FunctionValueType::new(DataType::Utf8, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}
#[test]
fn binary_text_recipe_original_invalid_null_scalar_compact_slice_and_nonzero_pool_addresses() {
    let input: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(b"pad".as_slice()),
        Some(b"valid\0text".as_slice()),
        Some(b"\xff".as_slice()),
        None,
    ]));
    let r = recipe(true);
    let rows = [1, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Binary,
        input.slice(1, 3),
        Box::default(),
    )
    .unwrap();
    for (ordinal, row) in selection.iter().enumerate() {
        let expected = if row == 1 {
            CastRowResult::Text("valid\0text".into())
        } else {
            CastRowResult::Null
        };
        for arg in [
            EvaluatedArgument::Column(&input),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            assert_eq!(
                r.evaluate_row(arg, ordinal, row, &Control::default())
                    .unwrap(),
                expected
            );
        }
    }
    let scalar = input.slice(1, 1);
    assert_eq!(
        r.evaluate_row(
            EvaluatedArgument::Scalar(&scalar),
            77,
            999,
            &Control::default()
        )
        .unwrap(),
        CastRowResult::Text("valid\0text".into())
    );
    let ty = FunctionValueType::new(DataType::Binary, true);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("actual-binary").unwrap()),
        ty,
        input.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 8,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 64,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        },
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    for (ordinal, expected) in [
        (1, CastRowResult::Text("valid\0text".into())),
        (2, CastRowResult::Null),
        (3, CastRowResult::Null),
    ] {
        let value = pool.value(ordinal).unwrap();
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Constant(&value),
                7,
                999,
                &Control::default()
            )
            .unwrap(),
            expected
        );
    }
    assert!(matches!(
        recipe(false).evaluate_row(EvaluatedArgument::Column(&input), 0, 3, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
#[test]
fn binary_text_recipe_actual_byte_quanta_all_seven_primary_causes_and_fail_stop() {
    let long = vec![b'a'; 777];
    let input: ArrayRef = Arc::new(BinaryArray::from(vec![Some(long.as_slice())]));
    let r = recipe(false);
    let good = Control::default();
    assert_eq!(
        r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &good)
            .unwrap(),
        CastRowResult::Text("a".repeat(777))
    );
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for stop in 0..trace.len() {
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("injected invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("injected internal")),
            KernelFailure::Operational(KernelDiagnostic::new("injected operation")),
            KernelFailure::InstanceFailed,
        ] {
            let c = Control {
                refuse: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &c),
                Err(cause)
            );
            assert_eq!(c.trace.lock().unwrap().len(), stop + 1);
        }
    }
}

#[test]
fn binary_text_recipe_hidden_invalid_payload_under_selected_null_remains_null() {
    let input: ArrayRef = Arc::new(BinaryArray::new(
        arrow_buffer::OffsetBuffer::new(vec![0, 1].into()),
        arrow_buffer::Buffer::from(vec![0xff]),
        Some(arrow_buffer::NullBuffer::from(vec![false])),
    ));
    assert_eq!(
        recipe(true)
            .evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &Control::default())
            .unwrap(),
        CastRowResult::Null
    );
}
