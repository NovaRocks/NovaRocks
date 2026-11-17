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
use crate::{Fragment, PlanLimits, UnpivotConstant, ValueId};
use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct BorrowedControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl BorrowedControl {
    fn new(refusal: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for BorrowedControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        // A hidden private Validate entry is an actual test failure.
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        if let Some((refusal, cause)) = self.refusal.clone()
            && at == refusal
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
fn borrowed<T>(
    control: &BorrowedControl,
    action: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, ConstantReferenceError>,
) -> Result<T, ConstantReferenceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = action(&mut work);
    if matches!(&result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn every_prefix(
    action: impl Fn(&mut CompileCheckpoints<'_>) -> Result<(), ConstantReferenceError>,
    expected: Result<(), ConstantReferenceError>,
) -> Vec<(CompilePhase, u32)> {
    let baseline = BorrowedControl::new(None);
    assert_eq!(borrowed(&baseline, &action), expected);
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
    for cause in CAUSES {
        for at in 0..trace.len() {
            let control = BorrowedControl::new(Some((at, cause.clone())));
            assert_eq!(
                borrowed(&control, &action),
                Err(ConstantReferenceError::Control(cause.clone()))
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    trace
}
fn lists(width: usize) -> ConstantPool {
    let mut builder = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    for row in [
        vec![9999],
        (0..width).map(|i| i32::try_from(i).unwrap() - 2).collect(),
        vec![11],
    ] {
        for value in row {
            builder.values().append_value(value);
        }
        builder.append(true);
    }
    plain(Arc::new(builder.finish()), false)
}
fn maps() -> ConstantPool {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
        .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    // The unused row is legal generic Map data, but violates Unpivot ordering.
    for row in [
        vec![("z", "unused"), ("a", "unused")],
        vec![("a", "λ\0"), ("z", "tail")],
        vec![("b", "v")],
    ] {
        for (key, value) in row {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
    }
    plain(Arc::new(builder.finish()), false)
}
fn table(backing: &ConstantPool, ids: &[u32]) -> ConstantPools {
    let mut pools = ConstantPools::empty();
    for id in ids {
        pools
            .insert(ConstantPoolId::new(*id), backing.clone())
            .unwrap();
    }
    pools
}
// A genuine checked construction fragment. The tests below invoke the actual
// mandatory constant-source gate, not a fabricated complete package.
fn collection_fragment(
    constants: &[UnpivotConstant],
    ty: &FunctionValueType,
) -> (Fragment, ValueId) {
    collection_fragment_with_scalar(constants, ty, None)
}
fn collection_fragment_with_scalar(
    constants: &[UnpivotConstant],
    ty: &FunctionValueType,
    scalar: Option<ConstantReference>,
) -> (Fragment, ValueId) {
    let mut builder = crate::FragmentBuilder::new(crate::FragmentId::new(91));
    let empty = crate::NodeId::new(u32::MAX);
    let project = crate::NodeId::new(0);
    let unpivot = crate::NodeId::new(901);
    let integer = FunctionValueType::new(DataType::Int64, false);
    let source_value = builder
        .add_value(
            integer.clone(),
            crate::ValueOrigin::NodeOutput {
                node: empty,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(empty, Box::default(), Box::from([source_value]))
        .unwrap();
    let expression = builder
        .add_expression(
            project,
            integer.clone(),
            crate::ExprKind::Value(source_value),
        )
        .unwrap();
    let input = builder
        .add_value(
            integer.clone(),
            crate::ValueOrigin::Expr {
                node: project,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            empty,
            Box::from([(expression, input)]),
            Box::from([input]),
        )
        .unwrap();
    let scalar = scalar.map(|reference| {
        let expression = builder
            .add_expression(unpivot, ty.clone(), crate::ExprKind::Constant(reference))
            .unwrap();
        UnpivotConstant::Scalar(expression)
    });
    let constants = scalar.as_ref().map_or(constants, std::slice::from_ref);
    let value = builder
        .add_value(
            integer,
            crate::ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let literal = builder
        .add_value(
            ty.clone(),
            crate::ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .add_row_rewriting(
            unpivot,
            project,
            Some(&BTreeMap::new()),
            Box::from([value, literal]),
            crate::NodeKind::Unpivot {
                spec: crate::UnpivotSpec {
                    passthrough: Box::default(),
                    value_output: value,
                    literal_outputs: Box::from([literal]),
                    mappings: constants
                        .iter()
                        .cloned()
                        .map(|constant| crate::UnpivotValueMapping {
                            input,
                            constants: Box::from([constant]),
                        })
                        .collect(),
                    max_output_rows: 1024,
                    max_output_bytes: 1 << 20,
                },
            },
        )
        .unwrap();
    let fragment = builder
        .finish_structure(
            unpivot,
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control::good(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    (fragment, literal)
}

#[test]
fn borrowed_fragment_scalar_sparse_source_and_closed_ordinary_errors_keep_every_prefix() {
    let backing = plain(Arc::new(Int64Array::from(vec![99, 7])), false);
    let pools = table(&backing, &[0, u32::MAX]);
    let fragment = constant_fragment(
        201,
        &[
            (reference(0, 1), backing.value_type().clone()),
            (reference(u32::MAX, 1), backing.value_type().clone()),
        ],
    );
    every_prefix(
        |w| validate_fragment_constants_in(&fragment, &pools, true, PlanLimits::FROZEN, w),
        Ok(()),
    );
    let c = BorrowedControl::new(None);
    let selected = borrowed(&c, |w| {
        pools.resolve_source_observed(reference(u32::MAX, 1), w)
    })
    .unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert_eq!(selected.try_i64().unwrap(), Some(7));
    assert!(Arc::ptr_eq(selected.pool().array(), backing.array()));
    assert!(Arc::ptr_eq(
        selected.pool().field_ref(),
        backing.field_ref()
    ));

    for (address, ty, expected) in [
        (
            reference(4, 1),
            backing.value_type().clone(),
            ConstantReferenceError::MissingPool(ConstantPoolId::new(4)),
        ),
        (
            reference(0, 2),
            backing.value_type().clone(),
            ConstantReferenceError::Constant(ConstantError::Invalid(
                "constant ordinal is outside its pool",
            )),
        ),
        (
            reference(0, 1),
            FunctionValueType::new(DataType::Int64, true),
            ConstantReferenceError::SourceTypeMismatch(reference(0, 1)),
        ),
    ] {
        let invalid = constant_fragment(202, &[(address, ty)]);
        every_prefix(
            |w| validate_fragment_constants_in(&invalid, &pools, true, PlanLimits::FROZEN, w),
            Err(expected),
        );
    }
    let one = constant_fragment(203, &[(reference(0, 1), backing.value_type().clone())]);
    every_prefix(
        |w| validate_fragment_constants_in(&one, &pools, true, PlanLimits::FROZEN, w),
        Err(ConstantReferenceError::UnusedPools),
    );
    every_prefix(
        |w| validate_fragment_constants_in(&one, &pools, false, PlanLimits::FROZEN, w),
        Ok(()),
    );
}

#[test]
fn borrowed_plan_closes_peer_pools_without_private_fragment_scopes() {
    let backing = plain(Arc::new(Int64Array::from(vec![31, 41])), false);
    let pools = table(&backing, &[0, u32::MAX]);
    let a = constant_fragment(211, &[(reference(0, 1), backing.value_type().clone())]);
    let b = constant_fragment(
        212,
        &[(reference(u32::MAX, 1), backing.value_type().clone())],
    );
    let plan = plan_builder(&[a.clone(), b], pools.clone())
        .finish_observed(&Control::good())
        .unwrap();
    every_prefix(|w| validate_plan_constants_in(&plan, w), Ok(()));
    // This preserves an original mutation/input boundary, not a new checked Plan.
    let parts = plan_builder(&[a], pools).finish_observed(&Control::good());
    assert_eq!(
        parts.unwrap_err(),
        crate::PlanConstructionError::Constants(ConstantReferenceError::UnusedPools)
    );
    let baseline = Control::good();
    validate_plan_constants_observed(&plan, &baseline).unwrap();
    assert!(
        baseline
            .trace()
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Validate)
    );
}

#[test]
fn borrowed_int32_list_preserves_selected_row_limits_and_original_unpivot_law() {
    let backing = lists(3);
    let pools = table(&backing, &[0]);
    let constant = UnpivotConstant::Int32List(reference(0, 1));
    let (fragment, _) = collection_fragment(std::slice::from_ref(&constant), backing.value_type());
    every_prefix(
        |w| validate_fragment_constants_in(&fragment, &pools, true, PlanLimits::FROZEN, w),
        Ok(()),
    );
    every_prefix(
        |w| {
            let usage = unpivot_collection_usage_in(
                &pools,
                &constant,
                Some(backing.value_type()),
                3,
                12,
                w,
            )?;
            assert_eq!((usage.items, usage.payload_bytes), (3, 12));
            Ok(())
        },
        Ok(()),
    );
    every_prefix(
        |w| unpivot_collection_usage_in(&pools, &constant, None, 3, 12, w).map(|_| ()),
        Err(ConstantReferenceError::InvalidConsumer(
            "Unpivot literal output is missing",
        )),
    );
    for (items, bytes) in [(2, 12), (3, 11)] {
        let c = BorrowedControl::new(None);
        assert_eq!(
            borrowed(&c, |w| unpivot_collection_usage_in(
                &pools,
                &constant,
                Some(backing.value_type()),
                items,
                bytes,
                w
            ))
            .unwrap_err(),
            ConstantReferenceError::Control(CompileControlError::ResourceExhausted)
        );
    }
}

#[test]
fn borrowed_utf8_map_keeps_actual_nonzero_row_and_rejects_unsorted_source_with_caller_tail() {
    let backing = maps();
    let pools = table(&backing, &[u32::MAX]);
    let constant = UnpivotConstant::Utf8Map(reference(u32::MAX, 1));
    let (fragment, _) = collection_fragment(std::slice::from_ref(&constant), backing.value_type());
    every_prefix(
        |w| validate_fragment_constants_in(&fragment, &pools, true, PlanLimits::FROZEN, w),
        Ok(()),
    );
    every_prefix(
        |w| {
            let usage = unpivot_collection_usage_in(
                &pools,
                &constant,
                Some(backing.value_type()),
                2,
                9,
                w,
            )?;
            assert_eq!((usage.items, usage.payload_bytes), (2, 9));
            let selected = pools.resolve_source_observed(reference(u32::MAX, 1), w)?;
            let view = selected.utf8_map_in(w)?.unwrap();
            assert_eq!(view.item_observed(0, w)?, (Some("a"), Some("λ\0")));
            assert_eq!(view.item_observed(1, w)?, (Some("z"), Some("tail")));
            Ok(())
        },
        Ok(()),
    );
    let invalid = UnpivotConstant::Utf8Map(reference(u32::MAX, 0));
    every_prefix(
        |w| {
            unpivot_collection_usage_in(&pools, &invalid, Some(backing.value_type()), 2, 100, w)
                .map(|_| ())
        },
        Err(ConstantReferenceError::InvalidConsumer(
            "unpivot map keys must be non-empty and strictly increasing",
        )),
    );
}

#[test]
fn borrowed_window_frame_owner_uses_exact_checked_offsets_and_original_current_row_rule() {
    let backing = plain(Arc::new(Int64Array::from(vec![99, 7, 3, 0])), false);
    let pools = table(&backing, &[u32::MAX]);
    let fragment = constant_fragment(
        220,
        &[
            (reference(u32::MAX, 1), backing.value_type().clone()),
            (reference(u32::MAX, 2), backing.value_type().clone()),
            (reference(u32::MAX, 3), backing.value_type().clone()),
        ],
    );
    let ids = fragment
        .expressions()
        .iter()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    let frame = crate::WindowFrame {
        units: crate::WindowFrameUnits::Rows,
        start: crate::WindowBound::Preceding(ids[0]),
        end: crate::WindowBound::Preceding(ids[1]),
        exclusion: crate::WindowFrameExclusion::NoOthers,
    };
    // Actual frame/constant owner component; this does not certify a Window kernel.
    every_prefix(
        |w| validate_window_constants(&fragment, &pools, &frame, w),
        Ok(()),
    );
    let reversed = crate::WindowFrame {
        start: frame.end,
        end: frame.start,
        ..frame
    };
    every_prefix(
        |w| validate_window_constants(&fragment, &pools, &reversed, w),
        Err(ConstantReferenceError::InvalidConsumer(
            "window frame start follows its end after comparing exact constant offsets",
        )),
    );
    every_prefix(
        |w| window_offset_observed(&fragment, &pools, ids[2], w).map(|_| ()),
        Err(ConstantReferenceError::InvalidConsumer(
            "window constant offset must be a non-negative non-null exact I64/U64; zero requires CURRENT ROW",
        )),
    );
}

#[test]
fn borrowed_unpivot_scalar_payload_reads_only_actual_selected_constant_on_caller_scope() {
    let unused = "ignored source bytes".repeat(512);
    let backing = plain(
        Arc::new(StringArray::from(vec![unused.as_str(), "λ\0"])),
        false,
    );
    let pools = table(&backing, &[u32::MAX]);
    let (fragment, _) =
        collection_fragment_with_scalar(&[], backing.value_type(), Some(reference(u32::MAX, 1)));
    every_prefix(
        |w| validate_fragment_constants_in(&fragment, &pools, true, PlanLimits::FROZEN, w),
        Ok(()),
    );
    every_prefix(
        |w| {
            let selected = pools.resolve_source_observed(reference(u32::MAX, 1), w)?;
            assert_eq!(selected.selected_payload_bytes_in(w)?, 3);
            assert_eq!(selected.ordinal(), 1);
            assert!(Arc::ptr_eq(selected.pool().array(), backing.array()));
            Ok(())
        },
        Ok(()),
    );
}
