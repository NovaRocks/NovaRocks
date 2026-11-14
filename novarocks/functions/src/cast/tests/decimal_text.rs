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

//! Exact decimal text recipe admission and selected address/control contracts.
use super::super::*;
use crate::{ConstantPolicy, ConstantPool, SelectedValues, Selection};
use arrow_array::ArrayRef;
use arrow_buffer::{NullBuffer, i256};
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
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refuse {
            assert!(at <= *stop, "callback after refusal");
        }
        trace.push(n);
        if let Some((stop, cause)) = &self.refuse
            && *stop == at
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("decimal text never waits")
    }
}
fn recipe(ty: DataType, nullable: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(ty, nullable),
        &FunctionValueType::new(DataType::Utf8, nullable),
        DecimalOverflowPolicy::ReportError,
        true,
        &Control::default(),
    )
    .unwrap()
}
fn raw(wide: bool, values: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                values
                    .into_iter()
                    .map(|v| v.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(76, 2)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 2)
                .unwrap(),
        )
    }
}
#[test]
fn decimal_text_exact_p_s_null_promise_and_other_cast_domains_stay_separate() {
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        for p in [1, max] {
            for s in [i8::MIN, -3, 0, 1, p as i8] {
                let ty = if wide {
                    DataType::Decimal256(p, s)
                } else {
                    DataType::Decimal128(p, s)
                };
                for nullable in [false, true] {
                    let r = recipe(ty.clone(), nullable);
                    assert_eq!(
                        r.source_type(),
                        &FunctionValueType::from_legacy_declared_projection(ty.clone(), nullable)
                    );
                    assert_eq!(
                        r.result_type(),
                        &FunctionValueType::new(DataType::Utf8, nullable)
                    );
                    let c = ExpressionEffectContext {
                        use_id: ExpressionUseId::new(1),
                        domain: EvaluationDomainId::new(2),
                        demand: EvaluationDemand::Value,
                    };
                    assert_eq!(
                        r.own_effects(c).for_use(c).unwrap().may_raise_row_error,
                        !wide && s > 0
                    );
                }
                assert_eq!(
                    PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &FunctionValueType::from_legacy_declared_projection(ty.clone(), true),
                        &FunctionValueType::new(DataType::Utf8, false),
                        DecimalOverflowPolicy::OutputNull,
                        false,
                        &Control::default()
                    ),
                    Err(CastPrepareError::TypeMismatch)
                );
                for target in [
                    DataType::Int64,
                    DataType::Date32,
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ] {
                    if !wide || target == DataType::Int64 {
                        // Approved Decimal128->integer/calendar and Decimal256->integer
                        // routes retain these exact p/s declarations, including -128.
                        super::super::tests::assert_f3_admitted_preparation(ty.clone(), target);
                    } else {
                        assert_eq!(
                            PreparedCastRecipe::try_new(
                                CastOperation::Carrier,
                                &FunctionValueType::from_legacy_declared_projection(
                                    ty.clone(),
                                    true
                                ),
                                &FunctionValueType::from_legacy_declared_projection(target, true),
                                DecimalOverflowPolicy::ReportError,
                                true,
                                &Control::default()
                            ),
                            Err(CastPrepareError::Unsupported)
                        );
                    }
                }
            }
        }
        for (p, s) in [(0, 0), (max + 1, 0), (1, 2)] {
            let data_type = if wide {
                DataType::Decimal256(p, s)
            } else {
                DataType::Decimal128(p, s)
            };
            assert!(
                FunctionValueType::try_with_logical_type(
                    data_type,
                    true,
                    novarocks_type_contract::ValueLogicalType::Physical
                )
                .is_err()
            );
        }
    }
}
#[test]
fn decimal_text_selected_compact_scalar_slice_and_nonzero_constant_keep_actual_addresses() {
    let rows = [1, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    for wide in [false, true] {
        let input = raw(wide, vec![Some(i128::MIN), Some(5), None]);
        let ty =
            FunctionValueType::from_legacy_declared_projection(input.data_type().clone(), true);
        let r = recipe(input.data_type().clone(), true);
        let compact = SelectedValues::try_new(
            selection,
            ty.data_type(),
            raw(wide, vec![Some(5), None]),
            Box::default(),
        )
        .unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            let expected = if ordinal == 0 {
                CastRowResult::Text("0.05".into())
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
        let sliced = input.slice(1, 1);
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Scalar(&sliced),
                77,
                999,
                &Control::default()
            )
            .unwrap(),
            CastRowResult::Text("0.05".into())
        );
        let pool_input = raw(wide, vec![Some(0), Some(5), None]);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("decimal-constant").unwrap()),
            ty,
            pool_input.to_data(),
            ConstantPolicy {
                max_rows: 8,
                max_array_nodes: 8,
                max_logical_elements: 32,
                max_retained_buffer_bytes: 4096,
                max_type_depth: 8,
                max_type_nodes: 64,
                max_dictionary_depth: 4,
                max_metadata_bytes: 1024,
                max_library_validation_work: 4096,
                max_library_validation_bytes: 8192,
            },
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        let value = pool.value(1).unwrap();
        let null = pool.value(2).unwrap();
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Constant(&value),
                33,
                999,
                &Control::default()
            )
            .unwrap(),
            CastRowResult::Text("0.05".into())
        );
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Constant(&null),
                33,
                999,
                &Control::default()
            )
            .unwrap(),
            CastRowResult::Null
        );
    }
}
#[test]
fn decimal_text_null_mask_foreign_carrier_p_s_and_inherited_error_refuse_before_values() {
    let hidden: ArrayRef = Arc::new(
        Decimal128Array::new(vec![i128::MIN].into(), Some(NullBuffer::new_null(1)))
            .with_precision_and_scale(38, 2)
            .unwrap(),
    );
    let r = recipe(hidden.data_type().clone(), true);
    assert_eq!(
        r.evaluate_row(
            EvaluatedArgument::Column(&hidden),
            0,
            0,
            &Control::default()
        )
        .unwrap(),
        CastRowResult::Null
    );
    assert!(matches!(
        recipe(hidden.data_type().clone(), false).evaluate_row(
            EvaluatedArgument::Column(&hidden),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let wrong: ArrayRef = Arc::new(
        Decimal128Array::from(vec![None])
            .with_precision_and_scale(37, 2)
            .unwrap(),
    );
    assert!(matches!(
        r.evaluate_row(EvaluatedArgument::Column(&wrong), 0, 0, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let wide = raw(true, vec![None]);
    assert!(matches!(
        r.evaluate_row(EvaluatedArgument::Column(&wide), 0, 0, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let unresolved = SelectedValues::try_new(
        selection,
        r.source_type().data_type(),
        hidden.clone(),
        vec![RowDataError::new(0, "original child")].into_boxed_slice(),
    )
    .unwrap();
    assert!(matches!(
        r.evaluate_row(
            EvaluatedArgument::SelectedColumn(&unresolved),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
#[test]
fn decimal_text_every_real_callback_preserves_three_original_typed_causes() {
    for wide in [false, true] {
        let input = raw(wide, vec![Some(5)]);
        let r = recipe(input.data_type().clone(), true);
        let good = Control::default();
        r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &good)
            .unwrap();
        let count = good.trace.lock().unwrap().len();
        assert!(count > 0);
        for stop in 0..count {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
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
}
