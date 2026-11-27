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

use super::*;
use crate::{
    analysis::{SortItem, TypedExpr},
    compiler::SqlFunctionCatalog,
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::ConstantPool;
use novarocks_functions::{ConstantValidation, FunctionArgument, FunctionBindingError};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantValidation {
    novarocks_constant_contract::ConstantValidation::bounded(
        novarocks_constant_contract::ConstantPolicy {
            max_rows: 4,
            max_array_nodes: 16,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 8,
            max_type_nodes: 32,
            max_dictionary_depth: 4,
            max_metadata_bytes: 1 << 16,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    )
}
struct Fixture {
    source: AggregateArgumentSource<TypedExpr, SortItem>,
    pool: ConstantPool,
    field: Arc<Field>,
}
fn expression(pool: &ConstantPool, ordinal: u32) -> TypedExpr {
    TypedExpr {
        kind: crate::analysis::ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: pool.value_type().clone(),
    }
}
fn fixture() -> Fixture {
    let full = FunctionValueType::new(DataType::Int64, true);
    let field = Arc::new(
        Field::new("original aggregate constant", DataType::Int64, true).with_metadata(
            HashMap::from([
                ("source.field-id".to_owned(), "73".to_owned()),
                ("source.name".to_owned(), "原始".to_owned()),
            ]),
        ),
    );
    let array = Int64Array::from(vec![Some(101), Some(202), Some(303)]);
    let pool = ConstantPool::try_new(
        field.clone(),
        full,
        array.to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let argument = expression(&pool, 1);
    let order = SortItem {
        expr: expression(&pool, 2),
        asc: false,
        nulls_first: true,
    };
    // Select the actual installed ARRAY_AGG resolver with its one logical input
    // followed by the actual function ORDER channel; no synthetic declaration.
    let request = [
        crate::analysis::function_argument(&argument, policy(), &Control::default()).unwrap(),
        crate::analysis::function_argument(&order.expr, policy(), &Control::default()).unwrap(),
    ];
    let resolved = crate::functions::builtin_engine_function_catalog()
        .resolve_aggregate_binding("array_agg", 1, &request, &Control::default())
        .unwrap();
    assert_eq!(resolved.logical_argument_count, 1);
    assert_eq!(resolved.selected.argument_types.len(), 2);
    assert!(resolved.selected.aggregate.is_some());
    let binding = SqlFunctionBinding::from_test(resolved, DecimalOverflowPolicy::ReportError);
    Fixture {
        source: AggregateArgumentSource::logical_update(vec![argument], vec![order], binding),
        pool,
        field,
    }
}
fn captured(
    source: &AggregateArgumentSource<TypedExpr, SortItem>,
) -> CapturedAggregateLogicalRequest {
    capture_aggregate_logical_request(source, policy(), &Control::default()).unwrap()
}
fn constant(argument: &FunctionArgument) -> &novarocks_functions::ConstantValue {
    let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = argument
    else {
        panic!("actual admitted constant")
    };
    value
}
fn assert_constant(argument: &FunctionArgument, fixture: &Fixture, ordinal: u32) {
    let FunctionArgument::Value {
        value_type,
        constant: Some(value),
    } = argument
    else {
        panic!("actual constant channel")
    };
    assert_eq!(value_type, fixture.pool.value_type());
    assert_eq!(value.ordinal(), ordinal);
    assert!(Arc::ptr_eq(value.pool().field_ref(), &fixture.field));
    assert!(Arc::ptr_eq(value.pool().array(), fixture.pool.array()));
    assert_eq!(
        value.pool().backing_identity(),
        fixture.pool.backing_identity()
    );
    let array = value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        array.value(ordinal as usize),
        [101, 202, 303][ordinal as usize]
    );
}
fn project(
    source: &AggregateArgumentSource<TypedExpr, SortItem>,
) -> AggregateArgumentSource<FunctionArgument, (FunctionArgument, bool, bool)> {
    // A real typed-to-binding argument projection delegates to the sole
    // analysis author and preserves ORDER flags. It is not phase admission.
    source
        .try_map_parts(
            |arguments| {
                arguments
                    .iter()
                    .map(|argument| {
                        crate::analysis::function_argument(argument, policy(), &Control::default())
                    })
                    .collect::<Result<Vec<_>, FunctionBindingError>>()
            },
            |order| {
                order
                    .iter()
                    .map(|key| {
                        Ok((
                            crate::analysis::function_argument(
                                &key.expr,
                                policy(),
                                &Control::default(),
                            )?,
                            key.asc,
                            key.nulls_first,
                        ))
                    })
                    .collect::<Result<Vec<_>, FunctionBindingError>>()
            },
        )
        .unwrap()
}

#[test]
fn aggregate_independent_equal_installed_signatures_mint_distinct_source_identities() {
    let fixture = fixture();
    let original = &fixture.source;
    let independent = AggregateArgumentSource::logical_update(
        original.arguments().to_vec(),
        original.order_by().to_vec(),
        original.binding().clone(),
    );
    assert!(std::ptr::eq(
        original.binding().resolved(),
        independent.binding().resolved()
    ));
    assert_eq!(
        original.arguments()[0].value_type,
        independent.arguments()[0].value_type
    );
    let a = original.logical_identity().unwrap();
    let b = independent.logical_identity().unwrap();
    assert!(!a.same_lineage(b));
    assert!(!a.same_revision(b));
    let original_capture = captured(original);
    let independent_capture = captured(&independent);
    assert!(a.same_revision(original_capture.logical_identity()));
    assert!(b.same_revision(independent_capture.logical_identity()));
    assert!(
        !original_capture
            .logical_identity()
            .same_lineage(independent_capture.logical_identity())
    );
    assert_constant(&original_capture.request().arguments[0], &fixture, 1);
    assert_constant(&independent_capture.request().arguments[0], &fixture, 1);
}

#[test]
fn aggregate_clone_and_real_argument_projection_keep_revision_binding_and_selected_cv() {
    let fixture = fixture();
    let original = &fixture.source;
    let cloned = original.clone();
    let projected = project(original);
    let identity = original.logical_identity().unwrap();
    for other in [
        cloned.logical_identity().unwrap(),
        projected.logical_identity().unwrap(),
    ] {
        assert!(identity.same_lineage(other));
        assert!(identity.same_revision(other));
    }
    assert!(std::ptr::eq(
        original.binding().resolved(),
        cloned.binding().resolved()
    ));
    assert!(std::ptr::eq(
        original.binding().resolved(),
        projected.binding().resolved()
    ));
    assert_constant(&projected.arguments()[0], &fixture, 1);
    assert_constant(&projected.order_by()[0].0, &fixture, 2);
    assert_eq!(
        (projected.order_by()[0].1, projected.order_by()[0].2),
        (false, true)
    );
    let captured = captured(&cloned);
    assert!(identity.same_revision(captured.logical_identity()));
    assert!(std::ptr::eq(
        original.binding().resolved(),
        captured.binding().resolved()
    ));
    assert!(std::ptr::eq(
        &original.binding().resolved().selected,
        &captured.binding().resolved().selected
    ));
    assert_eq!(
        captured.binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
    assert_eq!(captured.constant_policy(), policy());
    assert_eq!(captured.request().logical_argument_count, 1);
    assert_eq!(captured.request().arguments.len(), 2);
    assert_constant(&captured.request().arguments[0], &fixture, 1);
    assert_constant(&captured.request().arguments[1], &fixture, 2);
}

#[test]
fn aggregate_successful_rewrite_preserves_lineage_but_captured_request_keeps_old_revision() {
    let mut fixture = fixture();
    let before = fixture.source.clone();
    let old_capture = captured(&fixture.source);
    let next_argument = expression(&fixture.pool, 2);
    let next_order = expression(&fixture.pool, 1);
    fixture.source.rewrite_channels(|arguments, order| {
        arguments[0] = next_argument;
        order[0].expr = next_order;
        order[0].asc = true;
        order[0].nulls_first = false;
    });
    let new = fixture.source.logical_identity().unwrap();
    let old = before.logical_identity().unwrap();
    assert!(old.same_lineage(new));
    assert!(!old.same_revision(new));
    assert!(old.same_revision(old_capture.logical_identity()));
    let new_capture = captured(&fixture.source);
    assert!(new.same_revision(new_capture.logical_identity()));
    assert!(
        !old_capture
            .logical_identity()
            .same_revision(new_capture.logical_identity())
    );
    assert!(std::ptr::eq(
        old_capture.binding().resolved(),
        new_capture.binding().resolved()
    ));
    assert_constant(&old_capture.request().arguments[0], &fixture, 1);
    assert_constant(&old_capture.request().arguments[1], &fixture, 2);
    assert_constant(&new_capture.request().arguments[0], &fixture, 2);
    assert_constant(&new_capture.request().arguments[1], &fixture, 1);
    assert_eq!(
        (
            fixture.source.order_by()[0].asc,
            fixture.source.order_by()[0].nulls_first
        ),
        (true, false)
    );
}

#[test]
fn aggregate_failed_partial_rewrite_changes_revision_and_uncertified_sources_never_upgrade() {
    let mut fixture = fixture();
    let before = fixture.source.clone();
    let old_capture = captured(&fixture.source);
    let updated = expression(&fixture.pool, 2);
    let result: Result<(), &'static str> = fixture.source.rewrite_channels(|arguments, _| {
        arguments[0] = updated;
        Err("rewrite failed after a real channel mutation")
    });
    assert_eq!(result, Err("rewrite failed after a real channel mutation"));
    let old = before.logical_identity().unwrap();
    let new = fixture.source.logical_identity().unwrap();
    assert!(old.same_lineage(new));
    assert!(!old.same_revision(new));
    assert!(old.same_revision(old_capture.logical_identity()));
    let new_capture = captured(&fixture.source);
    assert!(new.same_revision(new_capture.logical_identity()));
    assert_constant(&old_capture.request().arguments[0], &fixture, 1);
    assert_constant(&new_capture.request().arguments[0], &fixture, 2);

    let mut uncertified = AggregateArgumentSource::uncertified(
        before.arguments().to_vec(),
        before.order_by().to_vec(),
        before.binding().clone(),
    );
    assert!(uncertified.logical_identity().is_none());
    assert!(project(&uncertified).logical_identity().is_none());
    uncertified.rewrite_channels(|arguments, order| {
        arguments[0] = expression(&fixture.pool, 2);
        order[0].asc = true;
    });
    assert!(uncertified.logical_identity().is_none());
    assert!(uncertified.logical_parts().is_none());
    assert!(project(&uncertified).logical_identity().is_none());
    assert!(matches!(
        capture_aggregate_logical_request(&uncertified, policy(), &Control::default()),
        Err(AggregateRequestCaptureError::MissingLogicalSource)
    ));
}

#[test]
fn aggregate_capture_every_original_control_prefix_preserves_same_source_revision_and_binding() {
    let fixture = fixture();
    let mut malformed = fixture.source.clone();
    malformed.rewrite_channels(|arguments, _| arguments.push(arguments[0].clone()));
    let uncertified = AggregateArgumentSource::uncertified(
        fixture.source.arguments().to_vec(),
        fixture.source.order_by().to_vec(),
        fixture.source.binding().clone(),
    );
    for (source, expected) in [(&fixture.source, 0), (&malformed, 1), (&uncertified, 2)] {
        let retained = source.clone();
        let control = Control::default();
        let outcome = capture_aggregate_logical_request(source, policy(), &control);
        match expected {
            0 => {
                let capture = outcome.unwrap();
                assert!(
                    source
                        .logical_identity()
                        .unwrap()
                        .same_revision(capture.logical_identity())
                );
                assert_constant(&capture.request().arguments[0], &fixture, 1);
            }
            1 => assert!(matches!(
                outcome,
                Err(AggregateRequestCaptureError::InvalidSource(_))
            )),
            2 => assert!(matches!(
                outcome,
                Err(AggregateRequestCaptureError::MissingLogicalSource)
            )),
            _ => unreachable!(),
        }
        let baseline = control.trace.into_inner().unwrap();
        assert_eq!(baseline[0], 0);
        if expected != 0 {
            assert_eq!(
                baseline,
                vec![0, u32::from(expected == 1)],
                "ordinary refusal flushes only the source check actually performed"
            );
        }
        for stop in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(capture_aggregate_logical_request(source,policy(),&control),
                Err(AggregateRequestCaptureError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace.into_inner().unwrap(), baseline[..=stop]);
                assert!(std::ptr::eq(
                    source.binding().resolved(),
                    retained.binding().resolved()
                ));
                match (source.logical_identity(), retained.logical_identity()) {
                    (Some(actual), Some(original)) => {
                        assert!(actual.same_lineage(original));
                        assert!(actual.same_revision(original));
                    }
                    (None, None) => {}
                    _ => panic!("capture must not mutate source identity"),
                }
                // No retry may substitute a constant from another source.
                let crate::analysis::ExprKind::Constant(original) = &source.arguments()[0].kind
                else {
                    unreachable!()
                };
                assert_eq!(original.ordinal(), 1);
                assert!(Arc::ptr_eq(original.pool().array(), fixture.pool.array()));
            }
        }
    }
    assert_eq!(
        constant(&captured(&fixture.source).request().arguments[1]).ordinal(),
        2
    );
}

#[test]
fn aggregate_immutable_channel_substitution_renews_only_returned_revision_and_keeps_cv_loans() {
    let fixture = fixture();
    let original = &fixture.source;
    let old_capture = captured(original);
    let events = std::cell::RefCell::new(Vec::new());
    let rewritten = original
        .try_rewrite_parts(
            |arguments| {
                events.borrow_mut().push("arguments");
                let crate::analysis::ExprKind::Constant(value) = &arguments[0].kind else {
                    panic!("original admitted argument");
                };
                assert_eq!(value.ordinal(), 1);
                Ok::<_, &'static str>(vec![expression(&fixture.pool, 2)])
            },
            |order| {
                events.borrow_mut().push("order");
                let crate::analysis::ExprKind::Constant(value) = &order[0].expr.kind else {
                    panic!("original admitted ORDER channel");
                };
                assert_eq!(value.ordinal(), 2);
                Ok(vec![SortItem {
                    expr: expression(&fixture.pool, 1),
                    asc: true,
                    nulls_first: false,
                }])
            },
        )
        .unwrap();
    assert_eq!(*events.borrow(), vec!["arguments", "order"]);
    let old = original.logical_identity().unwrap();
    let new = rewritten.logical_identity().unwrap();
    assert!(old.same_lineage(new));
    assert!(!old.same_revision(new));
    assert!(old.same_revision(old_capture.logical_identity()));
    assert!(std::ptr::eq(
        original.binding().resolved(),
        rewritten.binding().resolved()
    ));
    assert!(std::ptr::eq(
        &original.binding().resolved().selected,
        &rewritten.binding().resolved().selected
    ));
    let new_capture = captured(&rewritten);
    assert!(new.same_revision(new_capture.logical_identity()));
    assert!(old.same_revision(captured(original).logical_identity()));
    assert_constant(&old_capture.request().arguments[0], &fixture, 1);
    assert_constant(&old_capture.request().arguments[1], &fixture, 2);
    assert_constant(&new_capture.request().arguments[0], &fixture, 2);
    assert_constant(&new_capture.request().arguments[1], &fixture, 1);
    assert_eq!(
        (
            original.order_by()[0].asc,
            original.order_by()[0].nulls_first
        ),
        (false, true)
    );
    assert_eq!(
        (
            rewritten.order_by()[0].asc,
            rewritten.order_by()[0].nulls_first
        ),
        (true, false)
    );
    assert_eq!(new_capture.constant_policy(), policy());
    assert_eq!(
        new_capture.binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SubstitutionFailure {
    Ordinary(&'static str),
    Control(CompileControlError),
}

#[test]
fn aggregate_immutable_substitution_failures_short_circuit_without_changing_source_revision() {
    let fixture = fixture();
    let uncertified = AggregateArgumentSource::uncertified(
        fixture.source.arguments().to_vec(),
        fixture.source.order_by().to_vec(),
        fixture.source.binding().clone(),
    );
    for source in [&fixture.source, &uncertified] {
        let before = source.clone();
        for fail_order in [false, true] {
            for cause in [
                None,
                Some(CompileControlError::Cancelled),
                Some(CompileControlError::DeadlineExceeded),
                Some(CompileControlError::ResourceExhausted),
            ] {
                let control = Control {
                    refusal: cause.clone().map(|cause| (usize::from(fail_order), cause)),
                    ..Control::default()
                };
                let events = std::cell::RefCell::new(Vec::new());
                let result = source.try_rewrite_parts(
                    |arguments| {
                        events.borrow_mut().push("arguments");
                        // This callback's original observation is zero work;
                        // it does not invent CPU credit for the generic port.
                        control
                            .checkpoint(CompilePhase::FunctionSpecialization, 0)
                            .map_err(SubstitutionFailure::Control)?;
                        if !fail_order {
                            return Err(SubstitutionFailure::Ordinary(
                                "argument substitution failed",
                            ));
                        }
                        let mut projected = arguments.to_vec();
                        projected[0] = expression(&fixture.pool, 2);
                        Ok(projected)
                    },
                    |_order| {
                        events.borrow_mut().push("order");
                        control
                            .checkpoint(CompilePhase::FunctionSpecialization, 0)
                            .map_err(SubstitutionFailure::Control)?;
                        Err::<Vec<SortItem>, _>(SubstitutionFailure::Ordinary(
                            "ORDER substitution failed",
                        ))
                    },
                );
                let expected = cause.map_or_else(
                    || {
                        SubstitutionFailure::Ordinary(if fail_order {
                            "ORDER substitution failed"
                        } else {
                            "argument substitution failed"
                        })
                    },
                    SubstitutionFailure::Control,
                );
                assert!(matches!(result, Err(actual) if actual == expected));
                assert_eq!(
                    *events.borrow(),
                    if fail_order {
                        vec!["arguments", "order"]
                    } else {
                        vec!["arguments"]
                    }
                );
                assert_eq!(
                    control.trace.into_inner().unwrap(),
                    if fail_order { vec![0, 0] } else { vec![0] }
                );
                assert!(std::ptr::eq(
                    source.binding().resolved(),
                    before.binding().resolved()
                ));
                match (source.logical_identity(), before.logical_identity()) {
                    (Some(actual), Some(old)) => {
                        assert!(actual.same_lineage(old));
                        assert!(actual.same_revision(old));
                        let unchanged = captured(source);
                        assert!(actual.same_revision(unchanged.logical_identity()));
                        assert_constant(&unchanged.request().arguments[0], &fixture, 1);
                        assert_constant(&unchanged.request().arguments[1], &fixture, 2);
                    }
                    (None, None) => assert!(matches!(
                        capture_aggregate_logical_request(source, policy(), &Control::default()),
                        Err(AggregateRequestCaptureError::MissingLogicalSource)
                    )),
                    _ => panic!("failed immutable substitution changed source origin"),
                }
                let crate::analysis::ExprKind::Constant(value) = &source.arguments()[0].kind else {
                    unreachable!()
                };
                assert_eq!(value.ordinal(), 1);
                assert!(Arc::ptr_eq(value.pool().array(), fixture.pool.array()));
            }
        }
    }
    let rewritten = uncertified
        .try_rewrite_parts(
            |_| Ok::<_, &'static str>(vec![expression(&fixture.pool, 2)]),
            |_| {
                Ok(vec![SortItem {
                    expr: expression(&fixture.pool, 1),
                    asc: true,
                    nulls_first: false,
                }])
            },
        )
        .unwrap();
    assert!(rewritten.logical_identity().is_none());
    assert!(rewritten.logical_parts().is_none());
    assert!(std::ptr::eq(
        uncertified.binding().resolved(),
        rewritten.binding().resolved()
    ));
    assert!(matches!(
        capture_aggregate_logical_request(&rewritten, policy(), &Control::default()),
        Err(AggregateRequestCaptureError::MissingLogicalSource)
    ));
}
