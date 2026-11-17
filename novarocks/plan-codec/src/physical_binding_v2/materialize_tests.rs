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
use crate::physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
use std::{
    alloc::Layout,
    collections::HashMap,
    mem::size_of_val,
    sync::{Arc, Mutex},
};

const PRIOR: usize = 128 * 1024;
const SOURCE: usize = 512 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    active: bool,
    stop: Option<(usize, CompileControlError)>,
    trace: Vec<u32>,
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        *self.0.lock().unwrap() = State {
            active: true,
            stop,
            trace: Vec::new(),
        };
    }
    fn trace(&self) -> Vec<u32> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut s = self.0.lock().unwrap();
        if !s.active {
            return Ok(());
        }
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let at = s.trace.len();
        if let Some((stop, _)) = s.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        s.trace.push(units);
        match s.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 1024,
        max_type_references: 4096,
        max_request_bytes: 4 * 1024 * 1024,
        max_allocation_requests: 8192,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 512 * 1024 * 1024,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 65536,
    }
}
fn types(control: &Control) -> DecodedTypeTable {
    let child = Arc::new(
        Field::new("named", DataType::Int64, true)
            .with_metadata(HashMap::from([("custom".into(), "retained".into())])),
    );
    let source = [
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            u32::MAX,
            FunctionValueType::new(DataType::Struct(vec![child].into()), true),
        ),
        (
            8,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
        (
            9,
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ),
    ];
    let raw = encode_type_table(&source, type_limits(), control).unwrap();
    decode_type_table(&raw, type_limits(), control).unwrap()
}
fn value(id: u32) -> wire::FunctionArgumentType {
    wire::FunctionArgumentType {
        kind: Some(wire::function_argument_type::Kind::ValueTypeId(id)),
    }
}
fn raw(
    id: u32,
    kind: wire::FunctionKind,
    args: Vec<wire::FunctionArgumentType>,
    result: wire::function_binding_definition::Result,
) -> wire::FunctionBindingDefinition {
    wire::FunctionBindingDefinition {
        id,
        function_id: "test/f".into(),
        overload_id: "test/o".into(),
        kind: kind as i32,
        arguments: args,
        result: Some(result),
    }
}
fn definitions() -> Vec<wire::FunctionBindingDefinition> {
    vec![
        raw(
            u32::MAX,
            wire::FunctionKind::Scalar,
            vec![value(8)],
            wire::function_binding_definition::Result::ScalarValueTypeId(u32::MAX),
        ),
        raw(
            0,
            wire::FunctionKind::Aggregate,
            vec![wire::FunctionArgumentType {
                kind: Some(wire::function_argument_type::Kind::Lambda(
                    wire::LambdaArgumentType {
                        parameter_value_type_ids: vec![0, u32::MAX],
                        result_value_type_id: Some(0),
                    },
                )),
            }],
            wire::function_binding_definition::Result::ScalarValueTypeId(0),
        ),
        raw(
            7,
            wire::FunctionKind::Window,
            vec![],
            wire::function_binding_definition::Result::ScalarValueTypeId(0),
        ),
        raw(
            3,
            wire::FunctionKind::Table,
            vec![value(0)],
            wire::function_binding_definition::Result::Relation(wire::RelationResultTypes {
                value_type_ids: vec![u32::MAX, 9, u32::MAX],
            }),
        ),
    ]
}
fn with_headers<T>(
    control: &Control,
    raw: &[wire::FunctionBindingDefinition],
    call: impl FnOnce(&PreparedFunctionBindingHeaders<'_>) -> T,
) -> T {
    let types = types(control);
    let headers = prepare_function_binding_headers(raw, &types, PRIOR, limits(), control).unwrap();
    call(&headers)
}
fn prefix(
    call: impl Fn(&Control, Option<(usize, CompileControlError)>) -> Result<(), Error>,
    success: bool,
) {
    let control = Control::default();
    assert_eq!(call(&control, None).is_ok(), success);
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control::default();
            assert!(
                matches!(call(&c,Some((at,cause.clone()))),Err(Error::Control(actual))if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
#[test]
fn materialized_all_kinds_preserve_complete_signature_order_and_source_fields() {
    let control = Control::default();
    let raw = definitions();
    with_headers(&control, &raw, |headers| {
        control.arm(None);
        let prepared =
            prepare_function_bindings_materialization(headers, SOURCE, limits()).unwrap();
        let output = materialize_function_bindings(prepared).unwrap();
        assert_eq!(
            output
                .definitions()
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>(),
            [u32::MAX, 0, 7, 3]
        );
        let MaterializedFunctionBinding::Scalar(a) = &output.definitions()[0].1 else {
            panic!("scalar");
        };
        assert_eq!(a.kind, FunctionKind::Scalar);
        assert!(a.legacy_metadata.is_none());
        assert_eq!(a.function_id.as_str(), "test/f");
        assert_eq!(a.overload.as_str(), "test/o");
        assert_eq!(
            a.argument_types[0],
            FunctionArgumentType::Value(headers.type_table().value_type(8).unwrap().clone())
        );
        let DataType::Struct(fields) = &a.result_type.data_type() else {
            panic!("struct");
        };
        let DataType::Struct(original) = &headers
            .type_table()
            .value_type(u32::MAX)
            .unwrap()
            .data_type()
        else {
            panic!("struct");
        };
        assert!(Arc::ptr_eq(&fields[0], &original[0]));
        assert!(fields[0].metadata().is_empty());
        let MaterializedFunctionBinding::Scalar(b) = &output.definitions()[1].1 else {
            panic!("aggregate");
        };
        assert_eq!(b.kind, FunctionKind::Aggregate);
        let FunctionArgumentType::Lambda {
            parameter_types,
            result_type,
        } = &b.argument_types[0]
        else {
            panic!("lambda");
        };
        assert_eq!(parameter_types.len(), 2);
        assert_eq!(parameter_types[1], a.result_type);
        assert_eq!(result_type.data_type(), &DataType::Int64);
        let MaterializedFunctionBinding::Scalar(c) = &output.definitions()[2].1 else {
            panic!("window");
        };
        assert_eq!(c.kind, FunctionKind::Window);
        assert!(c.argument_types.is_empty());
        let MaterializedFunctionBinding::Table(d) = &output.definitions()[3].1 else {
            panic!("table");
        };
        assert!(d.legacy_metadata.is_none());
        assert_eq!(d.result_types.len(), 3);
        assert_eq!(d.result_types[0], d.result_types[2]);
        assert_eq!(
            d.result_types[1].data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(d.result_types[1].logical_type(), ValueLogicalType::LargeInt);
        assert!(
            output.retained_invoice_floor().unwrap()
                < SOURCE + output.facts().request_bytes_upper_bound + size_of_val(&output)
        );
        assert_eq!(output.into_definitions().len(), 4);
        for fault in 0..3 {
            let mut bad = raw.clone();
            match fault {
                0 => bad[0].kind = 0,
                1 => bad[0].result = None,
                _ => bad[0].arguments[0] = value(99),
            }
            assert!(
                prepare_function_binding_headers(
                    &bad,
                    headers.type_table(),
                    PRIOR,
                    limits(),
                    &control
                )
                .is_err()
            );
        }
    });
}
#[test]
fn materialization_requests_have_independent_layout_oracle() {
    let control = Control::default();
    let raw = definitions();
    with_headers(&control, &raw, |headers| {
        control.arm(None);
        let prepared =
            prepare_function_bindings_materialization(headers, SOURCE, limits()).unwrap();
        let f = prepared.facts();
        let pair = Layout::array::<(u32, MaterializedFunctionBinding)>(4)
            .unwrap()
            .size();
        let args = Layout::array::<FunctionArgumentType>(3).unwrap().size();
        let params = Layout::array::<FunctionValueType>(2).unwrap().size();
        let results = Layout::array::<FunctionValueType>(3).unwrap().size();
        // Vec-to-Box backing plus one actual private holder for each nonempty
        // collection; both identities have a Box and an Arc per definition.
        let arguments = novarocks_type_contract::FunctionArgumentTypes::storage_allocation_layout()
            .unwrap()
            .size();
        let parameters =
            novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout()
                .unwrap()
                .size();
        let identities = FunctionId::storage_allocation_layout().unwrap().size()
            + FunctionOverloadId::storage_allocation_layout()
                .unwrap()
                .size();
        assert_eq!(f.allocation_requests_upper_bound, 2 + 9 + 3 + 3 + 16);
        let golden = 2 * (pair + args + params + results)
            + 8 * 6
            + 3 * arguments
            + 2 * parameters
            + 4 * identities;
        assert_eq!(f.request_bytes_upper_bound, golden);
        assert_eq!(f.type_reference_count, 11);
        assert_eq!(
            f.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + golden
        );
    });
}
#[test]
fn materialization_each_six_axis_exact_and_one_under_is_admitted_before_outputs() {
    let control = Control::default();
    let raw = definitions();
    with_headers(&control, &raw, |headers| {
        let f = *prepare_function_bindings_materialization(headers, SOURCE, limits())
            .unwrap()
            .facts();
        let exact = BindingProjectionLimits {
            max_definitions: f.definition_count,
            max_type_references: f.type_reference_count,
            max_request_bytes: f.request_bytes_upper_bound,
            max_allocation_requests: f.allocation_requests_upper_bound,
            max_coexisting_source_and_request_bytes: f
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: f.cumulative_work_upper_bound,
        };
        control.arm(None);
        materialize_function_bindings(
            prepare_function_bindings_materialization(headers, SOURCE, exact).unwrap(),
        )
        .unwrap();
        for axis in 0..6 {
            let mut small = exact;
            match axis {
                0 => small.max_definitions -= 1,
                1 => small.max_type_references -= 1,
                2 => small.max_request_bytes -= 1,
                3 => small.max_allocation_requests -= 1,
                4 => small.max_coexisting_source_and_request_bytes -= 1,
                _ => small.max_work -= 1,
            };
            control.arm(None);
            assert!(matches!(
                prepare_function_bindings_materialization(headers, SOURCE, small),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
        }
    });
}
#[test]
fn materialization_original_control_all_success_and_ordinary_prefixes() {
    let raw = vec![raw(
        0,
        wire::FunctionKind::Scalar,
        vec![value(0)],
        wire::function_binding_definition::Result::ScalarValueTypeId(0),
    )];
    prefix(
        |c, stop| {
            with_headers(c, &raw, |h| {
                c.arm(stop);
                prepare_function_bindings_materialization(h, SOURCE, limits()).map(|_| ())
            })
        },
        true,
    );
    prefix(
        |c, stop| {
            with_headers(c, &raw, |h| {
                let p = prepare_function_bindings_materialization(h, SOURCE, limits()).unwrap();
                c.arm(stop);
                materialize_function_bindings(p).map(|_| ())
            })
        },
        true,
    );
    prefix(
        |c, stop| {
            with_headers(c, &raw, |h| {
                c.arm(stop);
                prepare_function_bindings_materialization(h, 0, limits()).map(|_| ())
            })
        },
        false,
    );
}
#[test]
fn materialized_lookup_sparse_ids_missing_and_foreign_control() {
    let c = Control::default();
    let raw = definitions();
    with_headers(&c, &raw, |h| {
        let output = materialize_function_bindings(
            prepare_function_bindings_materialization(h, SOURCE, limits()).unwrap(),
        )
        .unwrap();
        c.arm(None);
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        assert!(output.definition_observed(0, &mut w).unwrap().is_some());
        assert!(
            output
                .definition_observed(u32::MAX, &mut w)
                .unwrap()
                .is_some()
        );
        assert!(output.definition_observed(1, &mut w).unwrap().is_none());
        w.finish().unwrap();
        let other = Control::default();
        other.arm(None);
        let mut w = CompileCheckpoints::try_new(&other, CompilePhase::Decode).unwrap();
        assert!(matches!(
            output.definition_observed(0, &mut w),
            Err(Error::InvalidShape(_))
        ));
        w.finish().unwrap();
    });
}
#[test]
fn materialization_wide_actual_argument_loop_has_256_unit_sample() {
    let raw = vec![raw(
        u32::MAX,
        wire::FunctionKind::Scalar,
        (0..320).map(|_| value(0)).collect(),
        wire::function_binding_definition::Result::ScalarValueTypeId(0),
    )];
    let c = Control::default();
    let trace = with_headers(&c, &raw, |h| {
        c.arm(None);
        let p = prepare_function_bindings_materialization(h, SOURCE, limits()).unwrap();
        assert_eq!(p.facts().type_reference_count, 321);
        c.trace()
    });
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("real own argument loop");
    for at in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let c = Control::default();
            with_headers(&c, &raw, |h| {
                c.arm(Some((at, cause.clone())));
                assert!(
                    matches!(prepare_function_bindings_materialization(h,SOURCE,limits()),Err(Error::Control(actual))if actual==cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            });
        }
    }
    let c = Control::default();
    let emit_trace = with_headers(&c, &raw, |h| {
        let prepared = prepare_function_bindings_materialization(h, SOURCE, limits()).unwrap();
        c.arm(None);
        let output = materialize_function_bindings(prepared).unwrap();
        let MaterializedFunctionBinding::Scalar(binding) = &output.definitions()[0].1 else {
            panic!("scalar");
        };
        assert_eq!(binding.argument_types.len(), 320);
        assert!(binding.legacy_metadata.is_none());
        for argument in &binding.argument_types {
            assert_eq!(
                argument,
                &FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, false))
            );
        }
        c.trace()
    });
    // Emission delegates bounded type copies; its flushes need not contain 256.
    for at in [0, emit_trace.len() / 2, emit_trace.len() - 1] {
        for cause in CAUSES {
            let c = Control::default();
            with_headers(&c, &raw, |h| {
                let prepared =
                    prepare_function_bindings_materialization(h, SOURCE, limits()).unwrap();
                c.arm(Some((at, cause.clone())));
                assert!(
                    matches!(materialize_function_bindings(prepared),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(c.trace(), emit_trace[..=at]);
            });
        }
    }
    assert_eq!(trace[1], 256);
    for cause in CAUSES {
        let c = Control::default();
        with_headers(&c, &raw, |h| {
            let mut small = limits();
            small.max_type_references = 255;
            c.arm(Some((1, cause)));
            assert!(matches!(
                prepare_function_bindings_materialization(h, SOURCE, small),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(c.trace(), [0]);
        });
    }
}

#[test]
fn composition_requests_admit_temporary_layout_work_without_inventing_retained_backing() {
    let mut model = Model::for_composition(2, 0, 1000, 1000);
    model.request::<u32>(4, 2).unwrap();
    model
        .request_layouts(Layout::array::<u64>(3).unwrap(), 1)
        .unwrap();
    model
        .request_layouts(Layout::from_size_align(64, 8).unwrap(), 3)
        .unwrap();
    model.add_work(17).unwrap();
    model.check(limits()).unwrap();
    // Independent request invoice: two 16-byte buffers, a temporary 24-byte
    // buffer and three actual 64-byte layouts. Only one final 16-byte buffer
    // is a necessary retained floor; private tree maxima are not measurements.
    assert_eq!(model.facts.allocation_requests_upper_bound, 6);
    assert_eq!(model.facts.request_bytes_upper_bound, 248);
    assert_eq!(
        model.facts.coexisting_source_and_request_bytes_upper_bound,
        1248
    );
    assert_eq!(model.retained, 16);
    assert_eq!(model.facts.cumulative_work_upper_bound, 1207);
    let exact = BindingProjectionLimits {
        max_definitions: 2,
        max_type_references: 0,
        max_request_bytes: 248,
        max_allocation_requests: 6,
        max_coexisting_source_and_request_bytes: 1248,
        max_work: 1207,
    };
    model.check(exact).unwrap();
    for cause in CAUSES {
        let c = Control::default();
        c.arm(Some((1, cause)));
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let small = BindingProjectionLimits {
            max_work: 1206,
            ..exact
        };
        assert!(matches!(
            finish::<()>(work, model.check(small)),
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(c.trace(), [0]);
    }
    // Delegated work remains cumulative across later checks, rather than
    // being overwritten by the original model's recomputed own-work invoice.
    model.add_work(1).unwrap();
    assert!(matches!(
        model.check(exact),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    assert_eq!(model.retained, 16);
}

#[test]
fn composition_empty_and_overflow_requests_preserve_typed_refusal() {
    let mut model = Model::for_composition(0, 0, 1000, 1000);
    model
        .request_layouts(Layout::array::<u8>(0).unwrap(), 2)
        .unwrap();
    model
        .request_layouts(Layout::array::<()>(usize::MAX).unwrap(), 2)
        .unwrap();
    model
        .request_layouts(Layout::new::<()>(), usize::MAX)
        .unwrap();
    model.check(limits()).unwrap();
    assert_eq!(model.facts.allocation_requests_upper_bound, 0);
    assert_eq!(model.facts.request_bytes_upper_bound, 0);
    assert_eq!(model.retained, 0);
    assert!(matches!(
        model.request_layouts(Layout::new::<u64>(), usize::MAX),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
    model.add_work(usize::MAX).unwrap();
    assert!(matches!(
        model.add_work(1),
        Err(Error::Control(CompileControlError::ResourceExhausted))
    ));
}

fn node_limits() -> resources::NodeProjectionLimits {
    resources::NodeProjectionLimits {
        max_input_nodes: usize::MAX,
        max_value_references: usize::MAX,
        max_list_items: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
        properties: crate::physical_properties_v2::PhysicalPropertyProjectionLimits {
            max_value_references: usize::MAX,
            max_allocation_requests: usize::MAX,
            max_allocation_request_bytes: usize::MAX,
            max_coexisting_source_and_request_bytes: usize::MAX,
            max_work: usize::MAX,
        },
    }
}
fn composed_model() -> Model<'static> {
    let mut model = Model::for_composition(1, 0, SOURCE, 0);
    model.items = 3;
    model.request::<u64>(2, 2).unwrap();
    model
}
#[test]
fn containing_node_all_known_axes_are_checked_without_late_observation() {
    let base = resources::Model {
        inputs: 1,
        refs: 1,
        items: 2,
        requests: 1,
        requested: 8,
        delegated_work: 7,
    };
    let mut model = composed_model();
    model
        .compose_in_node(base, 1, node_limits(), limits())
        .unwrap();
    let control = Control::default();
    control.arm(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let pure = model
        .node_numerical_facts(model.facts.cumulative_work_upper_bound)
        .unwrap();
    assert_eq!(control.trace(), [0]);
    let facts = model
        .node_facts(model.facts.cumulative_work_upper_bound, &mut work)
        .unwrap();
    assert_eq!(pure, facts);
    assert_eq!(facts.input_node_count, 1);
    assert_eq!(facts.value_reference_count, 1);
    assert_eq!(facts.list_item_count, 5);
    assert_eq!(facts.allocation_requests_upper_bound, 3);
    assert_eq!(facts.allocation_request_bytes_upper_bound, 40);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + 40
    );
    // Independent hand sum: original Node own work + original child coarse work.
    assert_eq!(
        facts.cumulative_work_upper_bound,
        256 + 32 * 6 + 34 + 4 * 40 + 7 + 128 + 32 * 4 + 4 * 32 + 2
    );
    let mut exact = node_limits();
    exact.max_input_nodes = facts.input_node_count;
    exact.max_value_references = facts.value_reference_count;
    exact.max_list_items = facts.list_item_count;
    exact.max_allocation_requests = facts.allocation_requests_upper_bound;
    exact.max_allocation_request_bytes = facts.allocation_request_bytes_upper_bound;
    exact.max_coexisting_source_and_request_bytes =
        facts.coexisting_source_and_request_bytes_upper_bound;
    exact.max_work = facts.cumulative_work_upper_bound;
    composed_model()
        .compose_in_node(base, 1, exact, limits())
        .unwrap();
    for axis in 0..7 {
        let mut l = exact;
        let maximum = match axis {
            0 => &mut l.max_input_nodes,
            1 => &mut l.max_value_references,
            2 => &mut l.max_list_items,
            3 => &mut l.max_allocation_requests,
            4 => &mut l.max_allocation_request_bytes,
            5 => &mut l.max_coexisting_source_and_request_bytes,
            6 => &mut l.max_work,
            _ => unreachable!(),
        };
        *maximum -= 1;
        for cause in CAUSES {
            control.arm(Some((1, cause)));
            let mut pending = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                pending.step().unwrap();
            }
            assert!(matches!(
                composed_model().compose_in_node(base, 1, l, limits()),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [0]);
        }
    }
    let peak = model.node_work_peak();
    let mut updated = base;
    updated.delegated_work += 17;
    model
        .compose_in_node(updated, 1, node_limits(), limits())
        .unwrap();
    assert!(model.node_work_peak() >= peak);
    control.arm(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert_eq!(
        model
            .node_facts(peak, &mut work)
            .unwrap()
            .cumulative_work_upper_bound,
        facts.cumulative_work_upper_bound + 17
    );
}
#[test]
fn real_signature_collections_cannot_bypass_tighter_containing_node_items() {
    let ty = FunctionValueType::new(DataType::Int64, false);
    for lambda in [false, true] {
        let args = if lambda {
            vec![FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    vec![ty.clone(); 320].into_boxed_slice(),
                ),
                result_type: ty.clone(),
            }]
        } else {
            vec![FunctionArgumentType::Value(ty.clone()); 320]
        };
        let signature = BoundFunction::from_exact_signature(
            FunctionId::try_new("test/f").unwrap(),
            FunctionOverloadId::try_new("test/o").unwrap(),
            FunctionKind::Scalar,
            novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                args.into_boxed_slice(),
            ),
            ty.clone(),
        );
        let mut l = node_limits();
        l.max_list_items = if lambda { 320 } else { 319 };
        let mut child_limits = limits();
        child_limits.max_work = usize::MAX;
        for cause in CAUSES {
            let control = Control::default();
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut model = Model::for_composition(1, 0, SOURCE, 0);
            model
                .compose_in_node(resources::Model::default(), 0, l, child_limits)
                .unwrap();
            assert!(matches!(
                crate::physical_binding_v2::preflight_scalar_signature_copy_counts(
                    &signature,
                    &mut model,
                    child_limits,
                    &mut work
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [0]);
        }
    }
}
#[test]
fn known_clone_work_limit_precedes_post_delegate_control() {
    let source = FunctionValueType::new(DataType::Utf8, true);
    for parent in [false, true] {
        for cause in CAUSES {
            let control = Control::default();
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut model = Model::for_composition(1, 0, SOURCE, 0);
            model.facts.type_reference_count = 1;
            let mut child = limits();
            let mut node = node_limits();
            // The shared clone needs no allocation, but its two handle work
            // units remain in the original child's and containing node's caps.
            if parent {
                node.max_work = 0;
            } else {
                child.max_work = 0;
            }
            let result = model
                .compose_in_node(resources::Model::default(), 0, node, child)
                .and_then(|_| model.count_owned_type_clone(&source, child, &mut work));
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [0]);
        }
    }
}

#[test]
fn containing_node_updates_keep_original_envelope_and_monotone_base() {
    let base = resources::Model {
        inputs: 1,
        refs: 1,
        items: 2,
        requests: 1,
        requested: 8,
        delegated_work: 7,
    };
    let mut original_limits = node_limits();
    original_limits.max_input_nodes = 16;
    let mut model = composed_model();
    model
        .compose_in_node(base, 1, original_limits, limits())
        .unwrap();
    let peak = model.node_work_peak();
    for axis in 0..6 {
        let mut smaller = base;
        *match axis {
            0 => &mut smaller.inputs,
            1 => &mut smaller.refs,
            2 => &mut smaller.items,
            3 => &mut smaller.requests,
            4 => &mut smaller.requested,
            5 => &mut smaller.delegated_work,
            _ => unreachable!(),
        } -= 1;
        assert!(matches!(
            model.compose_in_node(smaller, 1, original_limits, limits()),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(model.node_work_peak(), peak);
    }
    assert!(matches!(
        model.compose_in_node(base, 2, original_limits, limits()),
        Err(Error::InvalidShape(_))
    ));
    let mut changed = original_limits;
    changed.max_input_nodes = 15;
    assert!(matches!(
        model.compose_in_node(base, 1, changed, limits()),
        Err(Error::InvalidShape(_))
    ));
    changed = original_limits;
    changed.properties.max_work -= 1;
    assert!(matches!(
        model.compose_in_node(base, 1, changed, limits()),
        Err(Error::InvalidShape(_))
    ));
    let mut grown = base;
    grown.delegated_work += 1;
    model
        .compose_in_node(grown, 1, original_limits, limits())
        .unwrap();
    assert_eq!(model.node_work_peak(), peak);
}

#[test]
fn admitted_clone_uses_original_requests_and_callbacks_without_double_charge() {
    let source = FunctionValueType::new(
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Dictionary(
                Box::new(DataType::Int16),
                Box::new(DataType::Utf8),
            )),
        ),
        true,
    );
    let control = Control::default();
    let run = |admitted, control: &Control| -> Result<BindingProjectionFacts, Error> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let mut model = Model::for_composition(1, 0, SOURCE, 0);
        model.facts.type_reference_count = 1;
        model.compose_in_node(resources::Model::default(), 0, node_limits(), limits())?;
        if admitted {
            let mut last = 0;
            model.count_owned_type_clone_admitted(
                &source,
                limits(),
                &mut |facts| {
                    assert!(facts.allocation_requests_upper_bound >= last);
                    last = facts.allocation_requests_upper_bound;
                    Ok(())
                },
                &mut work,
            )?;
        } else {
            model.count_owned_type_clone(&source, limits(), &mut work)?;
        }
        work.finish()?;
        Ok(model.facts)
    };
    control.arm(None);
    let original = run(false, &control).unwrap();
    let trace = control.trace();
    assert_eq!(original.allocation_requests_upper_bound, 0);
    assert_eq!(original.request_bytes_upper_bound, 0);
    control.arm(None);
    let admitted = run(true, &control).unwrap();
    assert_eq!(admitted.definition_count, original.definition_count);
    assert_eq!(admitted.type_reference_count, original.type_reference_count);
    assert_eq!(
        admitted.allocation_requests_upper_bound,
        original.allocation_requests_upper_bound
    );
    assert_eq!(
        admitted.request_bytes_upper_bound,
        original.request_bytes_upper_bound
    );
    assert_eq!(
        admitted.coexisting_source_and_request_bytes_upper_bound,
        original.coexisting_source_and_request_bytes_upper_bound
    );
    assert_eq!(
        admitted.cumulative_work_upper_bound,
        original.cumulative_work_upper_bound
    );
    assert_eq!(control.trace(), trace);
    for at in 0..trace.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(run(true, &control), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn admitted_shared_clone_preserves_numeric_and_parent_first_refusal() {
    let source = FunctionValueType::new(DataType::Utf8, true);
    for numeric in [false, true] {
        for cause in CAUSES {
            let control = Control::default();
            control.arm(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut model = Model::for_composition(1, 0, SOURCE, 0);
            model.facts.type_reference_count = 1;
            model
                .compose_in_node(resources::Model::default(), 0, node_limits(), limits())
                .unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut child = limits();
            if numeric {
                child.max_work = 0;
            }
            let result = model.count_owned_type_clone_admitted(
                &source,
                child,
                &mut |_| Err(CompileControlError::ResourceExhausted),
                &mut work,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [0]);
            assert_eq!(model.facts.allocation_requests_upper_bound, 0);
            assert_eq!(model.facts.request_bytes_upper_bound, 0);
        }
    }
}
