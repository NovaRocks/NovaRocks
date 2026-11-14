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

//! Exact observed List cast profiles and actual selected addressing/refusal.
use super::super::*;
use crate::{ConstantPolicy, ConstantPool, KernelDiagnostic, SelectedValues, Selection};
use arrow_array::{ArrayRef, ListArray, NullArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
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
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first refusal");
        }
        trace.push(n);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("List CAST never waits")
    }
}
fn input(null: bool, rows: usize) -> ArrayRef {
    let values: ArrayRef = if null {
        Arc::new(NullArray::new(rows * 2))
    } else {
        Arc::new(StringArray::from(
            (0..rows * 2)
                .map(|i| if i % 5 == 0 { None } else { Some("字\0x") })
                .collect::<Vec<_>>(),
        ))
    };
    let raw: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("", values.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(
            (0..=rows)
                .map(|i| i32::try_from(i * 2).unwrap())
                .collect::<Vec<_>>(),
        )),
        values,
        Some(NullBuffer::from(
            (0..rows).map(|i| i % 3 != 1).collect::<Vec<_>>(),
        )),
    ));
    crate::test_value_ingress::array_for_binding(
        &raw,
        &FunctionValueType::new(raw.data_type().clone(), true),
    )
    .unwrap()
}
fn target(null: bool, id: &str) -> DataType {
    let raw = DataType::List(Arc::new(
        Field::new(
            "element",
            if null {
                DataType::Int32
            } else {
                DataType::Utf8
            },
            true,
        )
        .with_metadata(HashMap::from([
            ("PARQUET:field_id".to_string(), id.to_string()),
            ("custom".to_string(), "preserve exactly".to_string()),
        ])),
    ));
    FunctionValueType::new(raw, true).data_type().clone()
}
fn recipe(
    a: &ArrayRef,
    target: &DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true),
        &FunctionValueType::from_legacy_declared_projection(target.clone(), true),
        policy,
        allow,
        &Control::default(),
    )
    .unwrap()
}
impl PreparedCastRecipe {
    /// Identical normalized bindings use the runtime identity edge. These
    /// cases exercise the independent selected-copy primitive directly.
    fn evaluate_list_primitive_for_test<'a>(
        &self,
        argument: EvaluatedArgument<'_>,
        selection: Selection<'a>,
        inherited: &[crate::RowDataError],
        control: &dyn KernelEvaluationControl,
    ) -> Result<crate::SelectedValues<'a>, KernelFailure> {
        if self.is_identity() {
            crate::list_cast_selected::evaluate(
                self.source_type(),
                self.result_type(),
                argument,
                selection,
                inherited,
                control,
            )
        } else {
            self.evaluate_collection(argument, selection, inherited, control)
        }
    }
}
fn legacy(a: &ArrayRef, ty: &DataType) -> ArrayRef {
    let DataType::List(field) = ty else {
        unreachable!()
    };
    crate::list_cast_core::cast::<KernelFailure>(a, field, &mut |child, target| {
        Ok(crate::list_cast_core::null_source(child.len(), target))
    })
    .unwrap()
}
fn same(a: &ArrayRef, b: &ArrayRef) {
    assert_eq!(a.data_type(), b.data_type());
    assert_eq!(a.len(), b.len());
    let (a, b) = (
        a.as_any().downcast_ref::<ListArray>().unwrap(),
        b.as_any().downcast_ref::<ListArray>().unwrap(),
    );
    for row in 0..a.len() {
        assert_eq!(a.is_null(row), b.is_null(row));
        if !a.is_null(row) {
            assert_eq!(a.value(row).to_data(), b.value(row).to_data());
        }
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("actual invalid cause"),
        internal("actual internal cause"),
        KernelFailure::Operational(KernelDiagnostic::new("actual operational cause")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn list_cast_recipe_all_observed_profiles_policies_sparse_empty_and_slices() {
    for (null, id) in [(true, ""), (false, "6"), (false, "7")] {
        let full = input(null, 7);
        let target = target(null, id);
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let r = recipe(&full, &target, policy, allow);
                assert!(r.is_collection() || r.is_identity());
                assert_eq!(r.is_identity(), !null);
                assert_eq!(r.policy(), policy);
                assert_eq!(r.allow_throw_exception(), allow);
                for a in [full.clone(), full.slice(1, 5), full.slice(3, 0)] {
                    for rows in [
                        (0..a.len()).collect::<Vec<_>>(),
                        (0..a.len()).filter(|r| r % 2 == 0).collect(),
                        Vec::new(),
                    ] {
                        let selection = Selection::try_sparse(a.len(), &rows).unwrap();
                        let result = r
                            .evaluate_list_primitive_for_test(
                                EvaluatedArgument::Column(&a),
                                selection,
                                &[],
                                &Control::default(),
                            )
                            .unwrap();
                        let indices = UInt64Array::from(
                            rows.iter()
                                .map(|r| u64::try_from(*r).unwrap())
                                .collect::<Vec<_>>(),
                        );
                        let expected =
                            arrow_select::take::take(legacy(&a, &target).as_ref(), &indices, None)
                                .unwrap();
                        same(result.values(), &expected);
                        assert!(result.errors().is_empty());
                    }
                }
            }
        }
    }
}
#[test]
fn list_cast_recipe_pool_ordinal_scalar_and_selected_compact_address_are_distinct() {
    for null in [false, true] {
        let full = input(null, 7);
        let target = target(null, "7");
        let r = recipe(&full, &target, DecimalOverflowPolicy::OutputNull, false);
        let value_type =
            FunctionValueType::from_legacy_declared_projection(full.data_type().clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(value_type.try_to_field("pool").unwrap()),
            value_type,
            full.to_data(),
            ConstantPolicy {
                max_rows: 16,
                max_array_nodes: 64,
                max_logical_elements: 4096,
                max_retained_buffer_bytes: 65536,
                max_type_depth: 8,
                max_type_nodes: 64,
                max_dictionary_depth: 4,
                max_metadata_bytes: 4096,
                max_library_validation_work: 65536,
                max_library_validation_bytes: 65536,
            },
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        let constant = pool.value(5).unwrap();
        let rows = [1, 8, 17];
        let selection = Selection::try_sparse(18, &rows).unwrap();
        let out = r
            .evaluate_list_primitive_for_test(
                EvaluatedArgument::Constant(&constant),
                selection,
                &[],
                &Control::default(),
            )
            .unwrap();
        let expanded = arrow_select::take::take(
            legacy(&full, &target).as_ref(),
            &UInt64Array::from(vec![5u64; 3]),
            None,
        )
        .unwrap();
        same(out.values(), &expanded);
        let scalar = full.slice(5, 1);
        let out = r
            .evaluate_list_primitive_for_test(
                EvaluatedArgument::Scalar(&scalar),
                selection,
                &[],
                &Control::default(),
            )
            .unwrap();
        same(out.values(), &expanded);
        let dense =
            arrow_select::take::take(full.as_ref(), &UInt64Array::from(vec![0u64, 3, 5]), None)
                .unwrap();
        let selected =
            SelectedValues::try_new(selection, dense.data_type(), dense.clone(), Box::new([]))
                .unwrap();
        let out = r
            .evaluate_list_primitive_for_test(
                EvaluatedArgument::SelectedColumn(&selected),
                selection,
                selected.errors(),
                &Control::default(),
            )
            .unwrap();
        same(out.values(), &legacy(&dense, &target));
        let wrong_rows = [0, 8, 17];
        let wrong = Selection::try_sparse(18, &wrong_rows).unwrap();
        assert!(matches!(
            r.evaluate_list_primitive_for_test(
                EvaluatedArgument::SelectedColumn(&selected),
                wrong,
                selected.errors(),
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}
#[test]
fn list_cast_recipe_inherited_errors_do_not_demand_child_rows_or_become_new_data_errors() {
    let full = input(true, 7);
    let target = target(true, "7");
    let r = recipe(&full, &target, DecimalOverflowPolicy::ReportError, true);
    let rows = [0, 3, 5];
    let selection = Selection::try_sparse(7, &rows).unwrap();
    let dense = arrow_select::take::take(
        full.as_ref(),
        &UInt64Array::from(vec![Some(0u64), None, Some(5)]),
        None,
    )
    .unwrap();
    let inherited = Box::new([RowDataError::new(1, "original child error")]);
    let selected =
        SelectedValues::try_new(selection, dense.data_type(), dense.clone(), inherited).unwrap();
    let result = r
        .evaluate_list_primitive_for_test(
            EvaluatedArgument::SelectedColumn(&selected),
            selection,
            selected.errors(),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(result.errors(), selected.errors());
    assert!(result.values().is_null(1));
    assert_eq!(result.values().data_type(), &target);
}
#[test]
fn list_cast_recipe_selected_children_do_not_retain_inactive_payload() {
    for null in [true, false] {
        let a = input(null, 7);
        let target = target(null, "6");
        let r = recipe(&a, &target, DecimalOverflowPolicy::OutputNull, false);
        let rows = [5];
        let selected = Selection::try_sparse(7, &rows).unwrap();
        let out = r
            .evaluate_list_primitive_for_test(
                EvaluatedArgument::Column(&a),
                selected,
                &[],
                &Control::default(),
            )
            .unwrap();
        let list = out.values().as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.values().len(), 2);
        assert_eq!(list.value_offsets(), &[0, 2]);
    }
}
#[test]
fn list_cast_recipe_no_new_row_error_and_explicit_unsupported_child_forms() {
    let a = input(true, 3);
    let ty = target(true, "6");
    let r = recipe(&a, &ty, DecimalOverflowPolicy::OutputNull, false);
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
    for target in [
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
    ] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true),
                &FunctionValueType::from_legacy_declared_projection(target, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    let physical = FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true);
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &physical,
            &FunctionValueType::from_legacy_declared_projection(ty, false),
            DecimalOverflowPolicy::OutputNull,
            false,
            &Control::default()
        ),
        Err(CastPrepareError::TypeMismatch)
    );
}
#[test]
fn list_cast_recipe_every_callback_preserves_seven_causes_and_no_footer() {
    for null in [true, false] {
        let a = input(null, 1025);
        let ty = target(null, "7");
        let r = recipe(&a, &ty, DecimalOverflowPolicy::ReportError, true);
        let selection = Selection::all(a.len());
        let ok = Control::default();
        r.evaluate_list_primitive_for_test(EvaluatedArgument::Column(&a), selection, &[], &ok)
            .unwrap();
        let callbacks = ok.trace.lock().unwrap().len();
        assert!(callbacks > 8);
        for stop in 0..callbacks {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    r.evaluate_list_primitive_for_test(
                        EvaluatedArgument::Column(&a),
                        selection,
                        &[],
                        &control
                    )
                    .unwrap_err(),
                    cause
                );
                assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            }
        }
    }
}

#[test]
fn list_cast_recipe_constant_null_root_broadcast_and_nested_logical_identity_guard() {
    let a = input(false, 7);
    let target = target(false, "6");
    let r = recipe(&a, &target, DecimalOverflowPolicy::OutputNull, false);
    let scalar = a.slice(1, 1);
    assert!(scalar.is_null(0));
    let rows = [0, 3, 17];
    let selection = Selection::try_sparse(18, &rows).unwrap();
    let result = r
        .evaluate_list_primitive_for_test(
            EvaluatedArgument::Scalar(&scalar),
            selection,
            &[],
            &Control::default(),
        )
        .unwrap();
    assert_eq!(result.values().null_count(), 3);
    assert_eq!(result.values().data_type(), &target);
    let DataType::List(field) = a.data_type() else {
        unreachable!()
    };
    let mut meta = field.metadata().clone();
    meta.insert(
        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_string(),
        "json".to_string(),
    );
    let json = DataType::List(Arc::new(field.as_ref().clone().with_metadata(meta)));
    // A nominal child may not silently become Physical just because Utf8 is shared.
    let source = FunctionValueType::from_legacy_declared_projection(json, true);
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &FunctionValueType::from_legacy_declared_projection(target, true),
            DecimalOverflowPolicy::OutputNull,
            false,
            &Control::default()
        ),
        Err(CastPrepareError::Unsupported)
    );
}

#[test]
fn list_cast_core_keeps_the_exact_child_failure_and_has_no_observation_footer() {
    #[derive(Debug)]
    enum CallerFailure {
        Original(Arc<String>),
        Domain,
    }
    impl From<crate::list_cast_core::ListCastFailure> for CallerFailure {
        fn from(_: crate::list_cast_core::ListCastFailure) -> Self {
            Self::Domain
        }
    }
    let a = input(true, 3);
    let DataType::List(field) = target(true, "7") else {
        unreachable!()
    };
    let message = Arc::new(format!("original child error {}", "x".repeat(2048)));
    let cause = crate::list_cast_core::cast(&a, &field, &mut |_, _| {
        Err(CallerFailure::Original(Arc::clone(&message)))
    })
    .unwrap_err();
    let CallerFailure::Original(actual) = cause else {
        panic!("child source was replaced");
    };
    assert!(Arc::ptr_eq(&actual, &message));
    let mut calls = 0;
    let result = crate::list_cast_core::cast_observed(
        &a,
        &field,
        &mut |_, _| {
            Err(crate::list_cast_core::ListCastError::Host(
                CallerFailure::Original(Arc::clone(&message)),
            ))
        },
        &mut |_| {
            calls += 1;
            Ok::<_, CallerFailure>(())
        },
    );
    let Err(crate::list_cast_core::ListCastError::Host(CallerFailure::Original(actual))) = result
    else {
        panic!("child source was replaced");
    };
    assert!(Arc::ptr_eq(&actual, &message));
    assert_eq!(calls, 1);
}
