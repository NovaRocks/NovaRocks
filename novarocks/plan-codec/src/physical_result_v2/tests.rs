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
use crate::{
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
    },
    physical_properties_v2::PhysicalPropertyProjectionLimits,
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::ValueLogicalType;
use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};
const PRIOR: usize = 32 << 10;
const VALUE_SOURCE: usize = 128 << 10;
const SOURCE: usize = 256 << 10;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    state: Mutex<(bool, Option<(usize, CompileControlError)>)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let (active, stop) = self.state.lock().unwrap().clone();
        if !active {
            return Ok(());
        }
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after original refusal");
        }
        events.push((phase, units));
        match stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.events.lock().unwrap().clear();
        *self.state.lock().unwrap() = (true, stop);
    }
    fn disarm(&self) {
        *self.state.lock().unwrap() = (false, None);
        self.events.lock().unwrap().clear();
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
fn limits() -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: 0,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 << 20,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_work: usize::MAX / 2,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 1024,
        },
    }
}
fn types_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 256,
        max_string_bytes: 64 << 10,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 0,
        max_payload_bytes: 0,
        max_allocation_requests: 0,
        max_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: VALUE_SOURCE,
        max_work: 64 << 20,
    }
}
fn value_limits() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 8,
        max_origin_references: 32,
        max_allocation_requests: 128,
        max_allocation_request_bytes: 128 << 10,
        max_coexisting_source_and_request_bytes: 4 << 20,
        max_work: 256 << 20,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 32,
            max_allocation_request_bytes: 64 << 10,
            max_coexisting_source_and_request_bytes: 4 << 20,
            max_work: 64 << 20,
        },
    }
}
struct Fixture {
    roots: Vec<(u32, FunctionValueType)>,
    definitions: Vec<p::ValueDef>,
}
impl Fixture {
    fn new() -> Self {
        #[allow(deprecated)]
        let field = Arc::new(
            Field::new_dict(
                "nested",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                41,
                true,
            )
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "source.tag".into(),
                "exact".into(),
            )])),
        );
        let roots = vec![
            (0, FunctionValueType::new(DataType::Utf8, true)),
            (
                7,
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    true,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ),
            (
                u32::MAX,
                FunctionValueType::new(
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ),
            (
                9,
                FunctionValueType::new(DataType::Struct(vec![field].into()), true),
            ),
        ];
        let definitions = roots
            .iter()
            .enumerate()
            .map(|(ordinal, (id, ty))| p::ValueDef {
                id: p::ValueId::new(*id),
                ty: ty.clone(),
                origin: p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(u32::MAX),
                    output_ordinal: u32::try_from(ordinal).unwrap(),
                },
            })
            .collect();
        Self { roots, definitions }
    }
    fn result(&self) -> p::ResultPort {
        p::ResultPort {
            scalar_schema: None,
            fragment: p::FragmentId::new(0),
            output: p::OutputPort {
                node: p::NodeId::new(u32::MAX),
                columns: Box::from([p::ValueId::new(0), p::ValueId::new(7), p::ValueId::new(0)]),
            },
            fields: Box::from([
                p::ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Plain,
                    name: "中\0".into(),
                    alias: None,
                    value: p::ValueId::new(0),
                    ty: self.roots[0].1.clone(),
                },
                p::ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Json,
                    name: "b".into(),
                    alias: Some("".into()),
                    value: p::ValueId::new(7),
                    ty: self.roots[1].1.clone(),
                },
                p::ResultField {
                    domain: novarocks_physical_plan::ResultValueDomain::Plain,
                    name: "c".into(),
                    alias: Some("别\0".into()),
                    value: p::ValueId::new(0),
                    ty: self.roots[0].1.clone(),
                },
            ]),
        }
    }
    fn single(&self, id: u32) -> p::ResultPort {
        let ty = self
            .roots
            .iter()
            .find(|(key, _)| *key == id)
            .unwrap()
            .1
            .clone();
        p::ResultPort {
            scalar_schema: None,
            fragment: p::FragmentId::new(u32::MAX),
            output: p::OutputPort {
                node: p::NodeId::new(0),
                columns: Box::from([p::ValueId::new(id)]),
            },
            fields: Box::from([p::ResultField {
                domain: if id == 7 {
                    p::ResultValueDomain::Json
                } else {
                    p::ResultValueDomain::Plain
                },
                name: "value".into(),
                alias: None,
                value: p::ValueId::new(id),
                ty,
            }]),
        }
    }
    fn with_tokens<R>(
        &self,
        c: &Control,
        run: impl FnOnce(&EncodedValues<'_, '_, '_>, &DecodedValues<'_, '_, '_>) -> R,
    ) -> R {
        c.disarm();
        let types = encode_type_table_sources(&self.roots, &[], types_limits(), c).unwrap();
        let read_types = decode_type_table(types.as_wire(), types_limits(), c).unwrap();
        let payloads = encode_connector_payloads(&[], PRIOR, payload_limits(), c).unwrap();
        let read_payloads =
            decode_connector_payloads(payloads.as_wire(), PRIOR, payload_limits(), c).unwrap();
        let inputs = self
            .definitions
            .iter()
            .map(|source| ValueSource {
                source,
                value_type_id: source.id.get(),
            })
            .collect::<Vec<_>>();
        let values =
            encode_values(&inputs, &payloads, &types, VALUE_SOURCE, value_limits()).unwrap();
        let read_values = decode_values(
            values.as_wire(),
            &read_payloads,
            &read_types,
            VALUE_SOURCE,
            value_limits(),
        )
        .unwrap();
        run(&values, &read_values)
    }
}
fn encode(
    input: Option<&p::ResultPort>,
    ids: &[u32],
    values: &EncodedValues<'_, '_, '_>,
    source: usize,
    l: NodeProjectionLimits,
    _c: &Control,
) -> Result<(Option<wire::ResultPort>, NodeProjectionFacts), Error> {
    prepare_result_encode(input, ids, values, source, l, values.original_control())?.emit()
}
fn decode(
    input: Option<&wire::ResultPort>,
    values: &DecodedValues<'_, '_, '_>,
    source: usize,
    l: NodeProjectionLimits,
    _c: &Control,
) -> Result<(Option<p::ResultPort>, NodeProjectionFacts), Error> {
    prepare_result_decode(input, values, source, l, values.original_control())?.emit()
}
fn expected() -> wire::ResultPort {
    wire::ResultPort {
        scalar_schema: None,
        fragment_id: Some(0),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![0, 7, 0],
        }),
        fields: vec![
            wire::ResultField {
                domain: Some(wire::ResultValueDomain::Plain as i32),
                name: "中\0".into(),
                alias: None,
                value_id: Some(0),
                value_type_id: Some(0),
            },
            wire::ResultField {
                domain: Some(wire::ResultValueDomain::Json as i32),
                name: "b".into(),
                alias: Some("".into()),
                value_id: Some(7),
                value_type_id: Some(7),
            },
            wire::ResultField {
                domain: Some(wire::ResultValueDomain::Plain as i32),
                name: "c".into(),
                alias: Some("别\0".into()),
                value_id: Some(0),
                value_type_id: Some(0),
            },
        ],
    }
}
fn control_error<T>(result: Result<T, Error>, cause: CompileControlError) {
    assert!(matches!(result,Err(Error::Control(actual)) if actual==cause));
}
fn replay(c: &Control, invoke: impl Fn() -> Result<(), Error>) {
    c.arm(None);
    invoke().unwrap();
    let trace = c.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            c.arm(Some((at, cause.clone())));
            control_error(invoke(), cause);
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
#[test]
fn result_none_and_present_empty_preserve_distinct_options() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        assert!(
            encode(None, &[], values, SOURCE, limits(), &c)
                .unwrap()
                .0
                .is_none()
        );
        assert!(
            decode(None, read, SOURCE, limits(), &c)
                .unwrap()
                .0
                .is_none()
        );
        assert!(matches!(
            prepare_result_encode(
                None,
                &[0],
                values,
                SOURCE,
                limits(),
                values.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
        let empty = p::ResultPort {
            scalar_schema: None,
            fragment: p::FragmentId::new(0),
            output: p::OutputPort {
                node: p::NodeId::new(0),
                columns: Box::default(),
            },
            fields: Box::default(),
        };
        let raw = encode(Some(&empty), &[], values, SOURCE, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(
            raw,
            wire::ResultPort {
                scalar_schema: None,
                fragment_id: Some(0),
                output: Some(wire::OutputPort {
                    node_id: Some(0),
                    value_ids: vec![]
                }),
                fields: vec![]
            }
        );
        assert_eq!(
            decode(Some(&raw), read, SOURCE, limits(), &c).unwrap().0,
            Some(empty)
        );
    });
}
#[test]
fn ordered_repeated_sparse_results_match_independent_wire_and_typed_values() {
    let fixture = Fixture::new();
    let input = fixture.result();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        let raw = encode(Some(&input), &[0, 7, 0], values, SOURCE, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(raw, expected());
        let result = decode(Some(&expected()), read, SOURCE, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(result.fragment.get(), 0);
        assert_eq!(result.output.node.get(), u32::MAX);
        assert_eq!(
            result
                .output
                .columns
                .iter()
                .map(|v| v.get())
                .collect::<Vec<_>>(),
            [0, 7, 0]
        );
        assert_eq!(
            result
                .fields
                .iter()
                .map(|f| (&*f.name, f.alias.as_deref()))
                .collect::<Vec<_>>(),
            [("中\0", None), ("b", Some("")), ("c", Some("别\0"))]
        );
        assert_eq!(
            result.fields[0].ty.logical_type(),
            ValueLogicalType::Physical
        );
        assert_eq!(result.fields[1].ty.logical_type(), ValueLogicalType::Json);
        assert!(result.fields.iter().all(|f| f.ty.nullable()));
        let sparse = fixture.single(u32::MAX);
        let raw = encode(Some(&sparse), &[u32::MAX], values, SOURCE, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(raw.fragment_id, Some(u32::MAX));
        assert_eq!(raw.fields[0].value_type_id, Some(u32::MAX));
    });
}
#[test]
fn full_nominal_nested_and_dictionary_fidelity_uses_original_type_owners() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        for id in [9, u32::MAX] {
            let input = fixture.single(id);
            let raw = encode(Some(&input), &[id], values, SOURCE, limits(), &c)
                .unwrap()
                .0
                .unwrap();
            let output = decode(Some(&raw), read, SOURCE, limits(), &c)
                .unwrap()
                .0
                .unwrap();
            match &output.fields[0].ty.data_type() {
                DataType::Utf8 => {
                    assert_eq!(
                        output.fields[0].ty,
                        read.types().value_type(id).unwrap().clone()
                    );
                }
                DataType::Struct(fields) => {
                    let DataType::Struct(original) =
                        &read.types().value_type(id).unwrap().data_type()
                    else {
                        panic!("original Struct expected")
                    };
                    assert!(Arc::ptr_eq(&fields[0], &original[0]));
                    assert_eq!(fields[0].name(), "nested");
                    assert!(!fields[0].is_nullable());
                    assert_eq!(
                        fields[0].metadata().get("source.tag").map(String::as_str),
                        None
                    );
                    #[allow(deprecated)]
                    {
                        assert_eq!(fields[0].dict_id(), None);
                    }
                    assert_eq!(fields[0].dict_is_ordered(), None);
                }
                _ => panic!("exact nested carrier expected"),
            }
        }
        let mut bad = fixture.single(0);
        bad.fields[0].ty = fixture.roots[1].1.clone();
        assert!(matches!(
            prepare_result_encode(
                Some(&bad),
                &[7],
                values,
                SOURCE,
                limits(),
                values.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
        let mut bad = fixture.single(9);
        // The checked source declares a required Struct child. Dictionary ID
        // differences cannot change value identity; child NULL drift can.
        let field = Field::new("nested", DataType::Utf8, true);
        bad.fields[0].ty =
            FunctionValueType::new(DataType::Struct(vec![Arc::new(field)].into()), true);
        assert!(matches!(
            prepare_result_encode(
                Some(&bad),
                &[9],
                values,
                SOURCE,
                limits(),
                values.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
        let mut bad = fixture.single(0);
        bad.fields[0].ty = bad.fields[0].ty.with_nullable(false).unwrap();
        assert!(matches!(
            prepare_result_encode(
                Some(&bad),
                &[0],
                values,
                SOURCE,
                limits(),
                values.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
    });
}
#[test]
fn result_presence_unknown_references_and_foreign_control_never_default() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        let input = fixture.result();
        assert!(matches!(
            prepare_result_encode(
                Some(&input),
                &[0],
                values,
                SOURCE,
                limits(),
                values.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
        assert!(matches!(
            prepare_result_encode(
                Some(&input),
                &[8, 7, 0],
                values,
                SOURCE,
                limits(),
                values.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
        let foreign = Control::default();
        assert!(matches!(
            prepare_result_encode(Some(&input), &[0, 7, 0], values, SOURCE, limits(), &foreign),
            Err(Error::InvalidShape(_))
        ));
        assert!(matches!(
            prepare_result_decode(Some(&expected()), read, SOURCE, limits(), &foreign),
            Err(Error::InvalidShape(_))
        ));
        for which in 0..7 {
            let mut bad = expected();
            match which {
                0 => bad.fragment_id = None,
                1 => bad.output = None,
                2 => bad.output.as_mut().unwrap().node_id = None,
                3 => bad.fields[0].value_id = None,
                4 => bad.fields[0].value_type_id = None,
                5 => bad.fields[0].value_id = Some(8),
                _ => bad.fields[0].value_type_id = Some(8),
            };
            assert!(matches!(
                prepare_result_decode(Some(&bad), read, SOURCE, limits(), read.original_control()),
                Err(Error::InvalidShape(_))
            ));
        }
        let mut bad = expected();
        bad.fields[0].value_type_id = Some(7);
        assert!(matches!(
            prepare_result_decode(Some(&bad), read, SOURCE, limits(), read.original_control()),
            Err(Error::InvalidShape(_))
        ));
        // Empty names are preserved vocabulary. The original Package result
        // validator, not this codec, owns the nonempty result-name condition.
        let mut empty = expected();
        empty.fields[0].name.clear();
        assert_eq!(
            &*decode(Some(&empty), read, SOURCE, limits(), &c)
                .unwrap()
                .0
                .unwrap()
                .fields[0]
                .name,
            ""
        );
    });
}
#[test]
fn result_request_layouts_have_independent_string_vec_box_and_dictionary_goldens() {
    let fixture = Fixture::new();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        let input = fixture.result();
        let send = prepare_result_encode(
            Some(&input),
            &[0, 7, 0],
            values,
            SOURCE,
            limits(),
            values.original_control(),
        )
        .unwrap();
        let text_bytes = 4 + 1 + 1 + 4;
        assert_eq!(send.facts().allocation_requests_upper_bound, 6);
        assert_eq!(
            send.facts().allocation_request_bytes_upper_bound,
            Layout::array::<u32>(3).unwrap().size()
                + Layout::array::<wire::ResultField>(3).unwrap().size()
                + text_bytes
        );
        let raw = expected();
        let receive =
            prepare_result_decode(Some(&raw), read, SOURCE, limits(), read.original_control())
                .unwrap();
        assert_eq!(receive.facts().allocation_requests_upper_bound, 12);
        assert_eq!(
            receive.facts().allocation_request_bytes_upper_bound,
            2 * Layout::array::<p::ValueId>(3).unwrap().size()
                + 2 * Layout::array::<p::ResultField>(3).unwrap().size()
                + 2 * text_bytes
        );
        let dictionary = fixture.single(u32::MAX);
        let raw = encode(Some(&dictionary), &[u32::MAX], values, SOURCE, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        let facts =
            *prepare_result_decode(Some(&raw), read, SOURCE, limits(), read.original_control())
                .unwrap()
                .facts();
        assert_eq!(facts.allocation_requests_upper_bound, 6);
        assert_eq!(
            facts.allocation_request_bytes_upper_bound,
            2 * Layout::new::<p::ValueId>().size()
                + 2 * Layout::new::<p::ResultField>().size()
                + 10
        );
    });
}
fn exact_limits(f: NodeProjectionFacts) -> NodeProjectionLimits {
    NodeProjectionLimits {
        max_input_nodes: f.input_node_count,
        max_value_references: f.value_reference_count,
        max_list_items: f.list_item_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
        ..limits()
    }
}
fn under(mut l: NodeProjectionLimits, axis: usize) -> NodeProjectionLimits {
    match axis {
        0 => l.max_value_references -= 1,
        1 => l.max_list_items -= 1,
        2 => l.max_allocation_requests -= 1,
        3 => l.max_allocation_request_bytes -= 1,
        4 => l.max_coexisting_source_and_request_bytes -= 1,
        _ => l.max_work -= 1,
    }
    l
}
#[test]
fn result_all_six_nonzero_axes_and_source_capacity_have_exact_boundaries() {
    let fixture = Fixture::new();
    let input = fixture.result();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        let send = *prepare_result_encode(
            Some(&input),
            &[0, 7, 0],
            values,
            SOURCE,
            limits(),
            values.original_control(),
        )
        .unwrap()
        .facts();
        let raw = expected();
        let receive =
            *prepare_result_decode(Some(&raw), read, SOURCE, limits(), read.original_control())
                .unwrap()
                .facts();
        encode(
            Some(&input),
            &[0, 7, 0],
            values,
            SOURCE,
            exact_limits(send),
            &c,
        )
        .unwrap();
        decode(Some(&raw), read, SOURCE, exact_limits(receive), &c).unwrap();
        for axis in 0..6 {
            control_error(
                prepare_result_encode(
                    Some(&input),
                    &[0, 7, 0],
                    values,
                    SOURCE,
                    under(exact_limits(send), axis),
                    values.original_control(),
                ),
                CompileControlError::ResourceExhausted,
            );
            control_error(
                prepare_result_decode(
                    Some(&raw),
                    read,
                    SOURCE,
                    under(exact_limits(receive), axis),
                    read.original_control(),
                ),
                CompileControlError::ResourceExhausted,
            );
        }
        // A genuine Dictionary result exhausts the admitted clone-walk
        // ceiling. The next original callback must never replace this known
        // numerical refusal, in either direction.
        let dictionary = fixture.single(u32::MAX);
        let dictionary_raw = encode(Some(&dictionary), &[u32::MAX], values, SOURCE, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        for received in [false, true] {
            let invoke = |l: NodeProjectionLimits| -> Result<NodeProjectionFacts, Error> {
                if received {
                    decode(Some(&dictionary_raw), read, SOURCE, l, &c).map(|(_, f)| f)
                } else {
                    encode(Some(&dictionary), &[u32::MAX], values, SOURCE, l, &c).map(|(_, f)| f)
                }
            };
            c.arm(None);
            let f = invoke(limits()).unwrap();
            let successful = c.trace();
            let ceiling = physical_type_v2::value_type_clone_preflight_work_upper_bound();
            let mut l = exact_limits(f);
            l.max_work -= if received { 2 * ceiling } else { ceiling };
            c.arm(None);
            control_error(invoke(l), CompileControlError::ResourceExhausted);
            let refused = c.trace();
            assert!(refused.len() < successful.len());
            assert_eq!(refused, successful[..refused.len()]);
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
            ] {
                c.arm(Some((refused.len(), cause)));
                control_error(invoke(l), CompileControlError::ResourceExhausted);
                assert_eq!(c.trace(), refused);
            }
        }
        c.arm(None);
        let mut raw = expected();
        raw.fields.reserve_exact(256);
        raw.output.as_mut().unwrap().value_ids.reserve_exact(256);
        raw.fields[0].name.reserve_exact(4096);
        // The original wire ResultPort is inside the namespace's whole-source
        // invoice. Keep its independently measured backing floor without
        // adding the same original source a second time.
        let result_floor = std::mem::size_of::<wire::ResultPort>()
            + Layout::array::<u32>(raw.output.as_ref().unwrap().value_ids.capacity())
                .unwrap()
                .size()
            + Layout::array::<wire::ResultField>(raw.fields.capacity())
                .unwrap()
                .size()
            + raw
                .fields
                .iter()
                .map(|f| f.name.capacity() + f.alias.as_ref().map_or(0, String::capacity))
                .sum::<usize>();
        let known = read.retained_invoice_floor().unwrap().max(result_floor);
        decode(Some(&raw), read, known, limits(), &c).unwrap();
        assert!(matches!(
            prepare_result_decode(
                Some(&raw),
                read,
                known - 1,
                limits(),
                read.original_control()
            ),
            Err(Error::InvalidShape(_))
        ));
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
        ] {
            c.arm(Some((1, cause)));
            let mut l = limits();
            l.max_value_references = 0;
            control_error(
                prepare_result_encode(
                    Some(&input),
                    &[0, 7, 0],
                    values,
                    SOURCE,
                    l,
                    values.original_control(),
                ),
                CompileControlError::ResourceExhausted,
            );
            assert_eq!(c.trace(), [(CompilePhase::Encode, 0)]);
        }
    });
}
#[test]
fn every_small_result_callback_preserves_three_primary_causes_and_ordinary_tails() {
    let fixture = Fixture::new();
    let input = fixture.result();
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        replay(&c, || {
            encode(Some(&input), &[0, 7, 0], values, SOURCE, limits(), &c).map(|_| ())
        });
        let raw = expected();
        replay(&c, || {
            decode(Some(&raw), read, SOURCE, limits(), &c).map(|_| ())
        });
        for decode_direction in [false, true] {
            let ordinary = || -> Result<(), Error> {
                if decode_direction {
                    let mut bad = expected();
                    bad.fields[2].value_type_id = Some(8);
                    prepare_result_decode(
                        Some(&bad),
                        read,
                        SOURCE,
                        limits(),
                        read.original_control(),
                    )
                    .map(|_| ())
                } else {
                    prepare_result_encode(
                        Some(&input),
                        &[0, 7, 8],
                        values,
                        SOURCE,
                        limits(),
                        values.original_control(),
                    )
                    .map(|_| ())
                }
            };
            c.arm(None);
            assert!(matches!(ordinary(), Err(Error::InvalidShape(_))));
            let trace = c.trace();
            assert!(trace.iter().any(|(_, units)| *units > 0));
            for at in 0..trace.len() {
                for cause in CAUSES {
                    c.arm(Some((at, cause.clone())));
                    control_error(ordinary(), cause);
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
        }
    });
}
#[test]
fn wide_result_actual_byte_copy_and_occurrences_expose_bounded_quantum() {
    let fixture = Fixture::new();
    let mut input = fixture.single(0);
    input.output.columns = vec![p::ValueId::new(0); 320].into_boxed_slice();
    input.fields = (0..320)
        .map(|_| p::ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "中".repeat(110).into_boxed_str(),
            alias: None,
            value: p::ValueId::new(0),
            ty: fixture.roots[0].1.clone(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let ids = vec![0; 320];
    let c = Control::default();
    fixture.with_tokens(&c, |values, read| {
        c.arm(None);
        let raw = encode(Some(&input), &ids, values, SOURCE * 2, limits(), &c)
            .unwrap()
            .0
            .unwrap();
        assert_eq!(raw.fields.len(), 320);
        assert!(raw.fields.iter().all(|f| f.name == "中".repeat(110)));
        assert_eq!(raw.output.as_ref().unwrap().value_ids, vec![0; 320]);
        for direction in [false, true] {
            let invoke = || -> Result<(), Error> {
                if direction {
                    decode(Some(&raw), read, SOURCE * 2, limits(), &c).map(|_| ())
                } else {
                    encode(Some(&input), &ids, values, SOURCE * 2, limits(), &c).map(|_| ())
                }
            };
            c.arm(None);
            invoke().unwrap();
            let trace = c.trace();
            let quantum = trace
                .iter()
                .position(|(_, u)| *u == 256)
                .expect("actual 256-unit callback");
            let mut samples = vec![0, quantum, trace.len() - 1];
            if quantum > 0 {
                samples.push(quantum - 1);
            }
            if quantum + 1 < trace.len() {
                samples.push(quantum + 1);
            }
            samples.sort_unstable();
            samples.dedup();
            for at in samples {
                for cause in CAUSES {
                    c.arm(Some((at, cause.clone())));
                    control_error(invoke(), cause);
                    assert_eq!(c.trace(), trace[..=at]);
                }
            }
        }
    });
}

#[path = "owner_tests.rs"]
mod owner_tests;
