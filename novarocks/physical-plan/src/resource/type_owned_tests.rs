// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueLogicalType};
use std::{
    alloc::Layout,
    collections::HashMap,
    sync::{Arc, Mutex},
};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after type refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Clone, Copy)]
struct Caps {
    requests: usize,
    bytes: usize,
    work: usize,
}
const OPEN: Caps = Caps {
    requests: usize::MAX,
    bytes: usize::MAX,
    work: usize::MAX,
};
fn gate(facts: &ControlOwnedResourceFacts, caps: Caps) -> Result<(), CompileControlError> {
    if facts.allocation_requests_upper_bound > caps.requests
        || facts.allocation_request_bytes_upper_bound > caps.bytes
        || facts.cumulative_work_upper_bound > caps.work
    {
        Err(CompileControlError::ResourceExhausted)
    } else {
        Ok(())
    }
}
#[derive(Debug)]
struct Outcome {
    items: usize,
    bytes: usize,
    errors: Vec<ValidationError>,
    facts: ControlOwnedResourceFacts,
}
fn run(
    ty: &ValueType,
    source: usize,
    caps: Caps,
    control: &Control,
) -> Result<Outcome, ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let mut resources = ControlResourceCounter::default();
    let mut cut = CutResourcePreflight::new();
    let mut errors = ValidationContext::new();
    cut.add_value_type_in(
        ty,
        "types.actual",
        &mut errors,
        source,
        &mut resources,
        &mut |facts| gate(facts, caps),
        &mut work,
    )?;
    work.finish()?;
    Ok(Outcome {
        items: cut.usage.items,
        bytes: cut.usage.bytes,
        errors: errors.into_vec(),
        facts: resources.facts(),
    })
}
fn run_raw(
    root: &DataType,
    source: usize,
    caps: Caps,
    control: &Control,
) -> Result<Outcome, ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let mut resources = ControlResourceCounter::default();
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    validate_data_type_core(
        root,
        "types.actual",
        &mut usage,
        &mut errors,
        &mut CallerTypeValidation {
            source,
            resources: &mut resources,
            admit: &mut |facts| gate(facts, caps),
            work: &mut work,
        },
    )?;
    work.finish()?;
    Ok(Outcome {
        items: usage.items,
        bytes: usage.bytes,
        errors: errors.into_vec(),
        facts: resources.facts(),
    })
}
fn raw(root: &DataType, source: usize) -> Outcome {
    run_raw(root, source, OPEN, &Control::default()).unwrap()
}
fn plain(ty: &ValueType) -> (usize, usize, Vec<ValidationError>) {
    let mut cut = CutResourcePreflight::new();
    let mut errors = ValidationContext::new();
    cut.add_value_type(ty, "types.actual", &mut errors);
    (cut.usage.items, cut.usage.bytes, errors.into_vec())
}
fn assert_plain(ty: &ValueType, actual: &Outcome) {
    let (items, bytes, errors) = plain(ty);
    assert_eq!(actual.items, items);
    assert_eq!(actual.bytes, bytes);
    assert_eq!(actual.errors, errors);
}
fn wide_raw(count: usize) -> (DataType, usize) {
    // These fresh source Fields have empty String/HashMap backings. Their
    // actual Arc<Field> and Arc<[FieldRef]> layouts give a truthful union
    // source invoice without guessing existing HashMap deleted capacity.
    let fields: Vec<_> = (0..count)
        .map(|_| Arc::new(Field::new("", DataType::Int64, false)))
        .collect();
    let field_bytes =
        novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Field>())
            .unwrap()
            .size();
    let slice = novarocks_type_contract::owned_resources::layout::arc_layout(
        Layout::array::<Arc<Field>>(count).unwrap(),
    )
    .unwrap()
    .size();
    (
        DataType::Struct(fields.into()),
        std::mem::size_of::<ValueType>() + slice + count * field_bytes,
    )
}
fn wide(count: usize) -> (ValueType, usize) {
    let (data_type, source) = wide_raw(count);
    let ty = ValueType::new(data_type, false);
    // The checked author retains independent semantic holders alongside the
    // normalized Arrow tree. Query their actual private layouts, not mirrors.
    let mut source = source
        + novarocks_type_contract::ValueLayout::storage_allocation_layout()
            .unwrap()
            .size();
    novarocks_type_contract::visit_value_type_observed(ty.value_type(), |visit| {
        match visit {
            novarocks_type_contract::ValueSemanticVisit::Node(value) => {
                source += novarocks_type_contract::ValueTypeRef::storage_allocation_layout()
                    .unwrap()
                    .size();
                if let novarocks_type_contract::ValueKind::Struct(fields) = value.kind() {
                    source += Layout::array::<novarocks_type_contract::ValueField>(fields.len())
                        .unwrap()
                        .size();
                }
            }
            novarocks_type_contract::ValueSemanticVisit::NamedField(_) => {
                source += novarocks_type_contract::ValueField::storage_allocation_layout()
                    .unwrap()
                    .size();
            }
        }
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    (ty, source)
}
fn metadata_field(reserved: usize, retained: usize) -> (DataType, usize) {
    let mut metadata = HashMap::new();
    metadata.try_reserve(reserved).unwrap();
    for i in 0..reserved {
        metadata.insert(format!("k{i}"), "v".to_owned());
    }
    for i in retained..reserved {
        metadata.remove(&format!("k{i}"));
    }
    let string_backings: usize = metadata
        .iter()
        .map(|(k, v)| k.capacity() + v.capacity())
        .sum();
    // This is the original one-reserve fresh-table owner, retained after
    // deletions. The public len/capacity is deliberately not the bucket model.
    let table = hashmap::fresh_table_layout::<String, String>(reserved).unwrap();
    let field = Arc::new(Field::new("", DataType::Int64, false).with_metadata(metadata));
    let source = std::mem::size_of::<ValueType>()
        + novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Field>())
            .unwrap()
            .size()
        + table.request_bytes_upper_bound
        + string_backings;
    (DataType::List(field), source)
}

#[test]
fn real_pending_vec_initial_and_branch_exact_layouts_have_independent_goldens() {
    let primitive = raw(&DataType::Int64, std::mem::size_of::<ValueType>());
    assert_eq!(primitive.facts.allocation_requests_upper_bound, 1);
    assert_eq!(
        primitive.facts.allocation_request_bytes_upper_bound,
        Layout::array::<(&DataType, usize)>(1).unwrap().size()
    );
    let (ty, source) = wide(MAX_DATA_TYPE_NODES - 1);
    let out = raw(ty.data_type(), source);
    assert!(out.errors.is_empty());
    assert_eq!(out.items, 2 * MAX_DATA_TYPE_NODES - 1);
    // The caller policy performs the actual branch reserve_exact(4095),
    // after the original quota accepts and before any Field observer.
    assert_eq!(out.facts.allocation_requests_upper_bound, 2);
    assert_eq!(
        out.facts.allocation_request_bytes_upper_bound,
        Layout::array::<(&DataType, usize)>(1).unwrap().size()
            + Layout::array::<(&DataType, usize)>(MAX_DATA_TYPE_NODES - 1)
                .unwrap()
                .size()
    );
    let (over, source) = wide_raw(MAX_DATA_TYPE_NODES);
    assert!(
        ValueType::try_with_logical_type(over.clone(), false, ValueLogicalType::Physical).is_err()
    );
    let over = raw(&over, source);
    assert_eq!(over.items, 1);
    assert_eq!(over.errors.len(), 1);
    assert_eq!(
        over.errors[0].category(),
        ValidationErrorCategory::ResourceLimit
    );
    assert_eq!(
        over.errors[0].message(),
        format!("Arrow data type contains more than {MAX_DATA_TYPE_NODES} nodes")
    );
    // Initial tuple plus the report's String/path/message/collector requests,
    // and no rejected child backing request.
    assert_eq!(over.facts.allocation_requests_upper_bound, 5);
}

#[test]
fn original_lifo_decimal_dictionary_and_negative_fixed_size_diagnostics_stay_exact() {
    let ty = DataType::Struct(
        vec![
            Arc::new(Field::new("", DataType::Decimal128(0, 0), false)),
            Arc::new(Field::new("", DataType::FixedSizeBinary(-1), false)),
        ]
        .into(),
    );
    assert!(
        ValueType::try_with_logical_type(ty.clone(), false, ValueLogicalType::Physical).is_err()
    );
    let out = raw(&ty, 4096);
    assert_eq!(
        out.errors.iter().map(|e| e.message()).collect::<Vec<_>>(),
        [
            "Arrow fixed-size length -1 is negative",
            "Arrow decimal precision/scale (0, 0) is outside 1..=38 and -38..=38",
        ]
    );
    assert_eq!(
        out.errors[0].category(),
        ValidationErrorCategory::StructuralInvariant
    );
    let dictionary = DataType::Dictionary(
        Box::new(DataType::Utf8),
        Box::new(DataType::FixedSizeBinary(-1)),
    );
    assert!(
        ValueType::try_with_logical_type(dictionary.clone(), false, ValueLogicalType::Physical)
            .is_err()
    );
    let out = raw(&dictionary, 4096);
    assert_eq!(
        out.errors[0].message(),
        "Arrow dictionary key must be an integer type"
    );
    assert_eq!(
        out.errors[1].message(),
        "Arrow fixed-size length -1 is negative"
    );
}

#[test]
fn real_deleted_metadata_backing_is_charged_without_len_or_capacity_inference() {
    let (fresh, fresh_source) = metadata_field(1, 1);
    let (deleted, deleted_source) = metadata_field(2048, 1);
    assert!(deleted_source > fresh_source);
    let original_metadata = match &deleted {
        DataType::List(field) => field.metadata(),
        _ => unreachable!(),
    };
    assert_eq!(original_metadata.len(), 1);
    let fresh = raw(&fresh, fresh_source);
    let out = raw(&deleted, deleted_source);
    assert!(out.errors.is_empty());
    assert_eq!(out.items, fresh.items);
    assert_eq!(out.bytes, fresh.bytes);
    assert_eq!(
        out.facts.allocation_requests_upper_bound,
        fresh.facts.allocation_requests_upper_bound
    );
    assert!(out.facts.cumulative_work_upper_bound > fresh.facts.cumulative_work_upper_bound);
    assert_eq!(out.facts.allocation_requests_upper_bound, 1);
}

#[test]
fn typed_semantic_author_refuses_invalid_domains_and_does_not_read_metadata() {
    assert_eq!(
        ValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Variant)
            .unwrap_err(),
        novarocks_type_contract::ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Variant)
    );
    let field = Arc::new(
        Field::new("ignored-role", DataType::Int64, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
                "unknown".to_owned(),
            )]),
        ),
    );
    let nested = ValueType::new(DataType::List(field), false);
    let out = run(&nested, 4096, OPEN, &Control::default()).unwrap();
    assert_plain(&nested, &out);
    assert!(out.errors.is_empty());
    let DataType::List(field) = nested.data_type() else {
        unreachable!()
    };
    assert!(field.metadata().is_empty());
    let primitive = ValueType::new(DataType::Int64, false);
    let out = run(
        &primitive,
        std::mem::size_of::<ValueType>(),
        OPEN,
        &Control::default(),
    )
    .unwrap();
    let carrier = raw(primitive.data_type(), std::mem::size_of::<ValueType>());
    assert_eq!(
        out.facts.allocation_requests_upper_bound,
        carrier.facts.allocation_requests_upper_bound
    );
    assert_eq!(
        out.facts.cumulative_work_upper_bound - carrier.facts.cumulative_work_upper_bound,
        1
    );
}

#[test]
fn every_actual_small_success_and_ordinary_type_callback_preserves_three_causes() {
    for ty in [
        ValueType::new(DataType::Int64, false),
        ValueType::new(DataType::Decimal32(9, 0), false),
    ] {
        let good = Control::default();
        let result = run(&ty, 4096, OPEN, &good).unwrap();
        assert_plain(&ty, &result);
        let trace = good.trace();
        assert!(trace.len() > 1 && trace.iter().any(|u| *u > 0));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(run(&ty,4096,OPEN,&control), Err(ControlResourceError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn known_initial_and_branch_requests_refuse_before_real_late_controls() {
    use novarocks_type_contract::owned_resources::copy::copy_string;
    let text = "x".repeat(255);
    for cause in CAUSES {
        let control = Control {
            refusal: Some((3, cause)),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let copied = copy_string::<ControlResourceError>(&text, &mut work).unwrap();
        assert_eq!(copied, text);
        assert_eq!(control.trace(), [0, 0, 1]);
        let mut resources = ControlResourceCounter::default();
        let mut errors = ValidationContext::new();
        let mut cut = CutResourcePreflight::new();
        let result = cut.add_value_type_in(
            &ValueType::new(DataType::Int64, false),
            "types.actual",
            &mut errors,
            4096,
            &mut resources,
            &mut |f| {
                gate(
                    f,
                    Caps {
                        requests: 0,
                        ..OPEN
                    },
                )
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(ControlResourceError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [0, 0, 1]);
    }
    let (ty, source) = wide(3);
    // Capture the actual branch admission position from the real same core;
    // no meter/model is preloaded and no future callback is invented.
    let good = Control::default();
    let mut work = CompileCheckpoints::try_new(&good, CompilePhase::Validate).unwrap();
    let mut resources = ControlResourceCounter::default();
    let mut cut = CutResourcePreflight::new();
    let mut errors = ValidationContext::new();
    let mut branch_at = None;
    cut.add_value_type_in(
        &ty,
        "types.actual",
        &mut errors,
        source,
        &mut resources,
        &mut |f| {
            if f.allocation_requests_upper_bound == 2 && branch_at.is_none() {
                branch_at = Some(good.trace().len());
            }
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let at = branch_at.unwrap();
    let trace = good.trace();
    for cause in CAUSES {
        let control = Control {
            refusal: Some((at, cause)),
            ..Control::default()
        };
        assert!(matches!(
            run(
                &ty,
                source,
                Caps {
                    requests: 1,
                    ..OPEN
                },
                &control
            ),
            Err(ControlResourceError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), trace[..at]);
    }
}

#[test]
fn actual_metadata_take_257_has_real_quantum_and_keeps_original_overlimit_law() {
    let (ty, source) = metadata_field(320, 320);
    let good = Control::default();
    let out = run_raw(&ty, source, OPEN, &good).unwrap();
    assert_eq!(out.errors.len(), 1);
    assert_eq!(
        out.errors[0].message(),
        "Arrow field metadata contains more than 256 entries"
    );
    let trace = good.trace();
    let quantum = trace.iter().position(|u| *u == 256).unwrap();
    for at in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(run_raw(&ty,source,OPEN,&control),
            Err(ControlResourceError::Control(actual)) if actual==cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn all_owned_axes_exact_replay_and_saturated_original_usage_remain_distinct() {
    let ty = ValueType::new(DataType::Int64, false);
    let source = std::mem::size_of::<ValueType>();
    let measured = run(&ty, source, OPEN, &Control::default()).unwrap();
    let f = measured.facts;
    let exact = Caps {
        requests: f.allocation_requests_upper_bound,
        bytes: f.allocation_request_bytes_upper_bound,
        work: f.cumulative_work_upper_bound,
    };
    let replay = run(&ty, source, exact, &Control::default()).unwrap();
    assert_eq!(replay.facts, f);
    for cap in [
        Caps {
            requests: exact.requests - 1,
            ..exact
        },
        Caps {
            bytes: exact.bytes - 1,
            ..exact
        },
        Caps {
            work: exact.work - 1,
            ..exact
        },
    ] {
        assert!(matches!(
            run(&ty, source, cap, &Control::default()),
            Err(ControlResourceError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut resources = ControlResourceCounter::default();
    let mut errors = ValidationContext::new();
    let mut cut = CutResourcePreflight::new();
    cut.add_items(MAX_FRAGMENT_DYNAMIC_ITEMS + 1);
    cut.add_value_type_in(
        &ty,
        "types.actual",
        &mut errors,
        source,
        &mut resources,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(resources.facts(), ControlOwnedResourceFacts::default());
    assert!(errors.is_empty());
    assert_eq!(control.trace(), [0, 0]);
}

/// Actual semantic visits no longer probe Arrow metadata. The independent
/// retained Arrow layout's iterator source bound remains affine, so a large
/// original source invoice cannot make semantic name work quadratic.
#[test]
#[cfg(target_pointer_width = "64")]
fn semantic_walk_is_source_independent_and_layout_header_work_remains_linear() {
    let ty = ValueType::new(
        DataType::Struct(
            vec![
                Arc::new(Field::new("a", DataType::Int64, false)),
                Arc::new(Field::new("b", DataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    );
    let work = |source| {
        let out = run(&ty, source, OPEN, &Control::default()).unwrap();
        assert_plain(&ty, &out);
        assert!(out.errors.is_empty());
        out.facts.cumulative_work_upper_bound
    };
    let key = novarocks_type_contract::NR_LOGICAL_TYPE_KEY.len();
    let header = |source| hashmap::source_iterator_work_upper_bound(source, 0).unwrap();
    let step = 1usize << 30;
    let (one, two, three) = (work(step), work(2 * step), work(3 * step));
    // Only the physical field header's original opaque iterator source bound
    // depends on source B; semantic name bytes are the actual retained names.
    let per_step = 2 * (header(2 * step) - header(step));
    assert_eq!(two - one, per_step);
    assert_eq!(three - two, per_step);
    // A source-sized key length charged about 2 * source^2 per field: two
    // nested fields under a 2 GiB invoice overflowed the meter.
    let receiver = 2 * step;
    assert!(
        hashmap::string_operations_work_upper_bound(receiver, 1, key, receiver)
            .unwrap()
            .checked_mul(2)
            .is_none()
    );
    assert_eq!(two, work(receiver));
}
