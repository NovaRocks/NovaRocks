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
use crate::physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{
    FunctionArgumentType, FunctionId, FunctionKind, FunctionOverloadId, FunctionValueType,
};
use std::{
    alloc::Layout,
    collections::HashMap,
    sync::{Arc, Mutex},
};

const B: usize = 512 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    armed: bool,
    stop: Option<(usize, CompileControlError)>,
    trace: Vec<(CompilePhase, u32)>,
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        *self.0.lock().unwrap() = State {
            armed: true,
            stop,
            trace: Vec::new(),
        };
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, p: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        let mut s = self.0.lock().unwrap();
        if !s.armed {
            return Ok(());
        }
        assert_eq!(p, CompilePhase::Validate);
        assert!(n <= 256);
        let at = s.trace.len();
        if let Some((i, _)) = s.stop {
            assert!(at <= i, "callback after originating refusal");
        }
        s.trace.push((p, n));
        match s.stop.clone() {
            Some((i, c)) if i == at => Err(c),
            _ => Ok(()),
        }
    }
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 4096,
        max_type_references: 8192,
        max_request_bytes: 32 * 1024 * 1024,
        max_allocation_requests: 16384,
        max_coexisting_source_and_request_bytes: 64 * 1024 * 1024,
        max_work: 4_000_000_000,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 65536,
    }
}
fn values() -> [(u32, FunctionValueType); 3] {
    [
        (0, FunctionValueType::new(DataType::Int64, false)),
        (
            7,
            FunctionValueType::new(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("child", DataType::Int32, true)
                            .with_metadata(HashMap::from([("original".into(), "field".into())])),
                    )]
                    .into(),
                ),
                true,
            ),
        ),
        (
            u32::MAX,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
    ]
}
fn scalar(
    kind: FunctionKind,
    arguments: Vec<FunctionArgumentType>,
    result: FunctionValueType,
) -> BoundFunction {
    BoundFunction::from_exact_signature(
        FunctionId::try_new("owner/exact").unwrap(),
        FunctionOverloadId::try_new("owner/selected").unwrap(),
        kind,
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            arguments.into_boxed_slice(),
        ),
        result,
    )
}
fn finish<T>(
    out: Result<T, BindingCodecError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, BindingCodecError> {
    if matches!(&out, Err(BindingCodecError::Control(_))) {
        return out;
    }
    work.finish()?;
    out
}
fn workflow(
    c: &Control,
    stop: Option<(usize, CompileControlError)>,
    bad: bool,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    workflow_with_limits(c, stop, bad, Some(limits()))
}

fn workflow_with_limits(
    c: &Control,
    stop: Option<(usize, CompileControlError)>,
    bad: bool,
    limits: Option<BindingProjectionLimits>,
) -> Result<BindingProjectionFacts, BindingCodecError> {
    let v = values();
    let type_sources = encode_type_table_sources(&v, &[], type_limits(), c)?;
    let types = decode_type_table(type_sources.as_wire(), type_limits(), c)?;
    let scalar_binding = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(v[2].1.clone())],
        v[1].1.clone(),
    );
    let aggregate = scalar(
        FunctionKind::Aggregate,
        vec![FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                Box::from([v[0].1.clone(), v[2].1.clone()]),
            ),
            result_type: v[1].1.clone(),
        }],
        v[0].1.clone(),
    );
    let window = scalar(FunctionKind::Window, Vec::new(), v[0].1.clone());
    let table = BoundTableFunction::from_exact_signature(
        FunctionId::try_new("owner/table").unwrap(),
        FunctionOverloadId::try_new("owner/table-selected").unwrap(),
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(v[0].1.clone()),
        ])),
        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::from([
            v[0].1.clone(),
            v[1].1.clone(),
        ])),
    );
    let scalar_args = [ArgumentTypeIds::Value(u32::MAX)];
    let lambda_params = [0, u32::MAX];
    let lambda_args = [ArgumentTypeIds::Lambda {
        parameters: &lambda_params,
        result: 7,
    }];
    let table_args = [ArgumentTypeIds::Value(0)];
    let relation = [0, 7];
    let inputs = [
        FunctionBindingInput {
            id: 0,
            source: BindingSource::Scalar(&scalar_binding),
            arguments: &scalar_args,
            result: ResultTypeIds::Scalar(7),
        },
        FunctionBindingInput {
            id: 9,
            source: BindingSource::Scalar(&aggregate),
            arguments: &lambda_args,
            result: ResultTypeIds::Scalar(0),
        },
        FunctionBindingInput {
            id: 27,
            source: BindingSource::Scalar(&window),
            arguments: &[],
            result: ResultTypeIds::Scalar(0),
        },
        FunctionBindingInput {
            id: u32::MAX,
            source: BindingSource::Table(&table),
            arguments: &table_args,
            result: ResultTypeIds::Relation(&relation),
        },
    ];
    c.arm(stop);
    let original: &dyn PureCompileControl = c;
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate)?;
    let out = (|| {
        let encoded = encode_function_bindings_in(
            &type_sources,
            &inputs,
            B,
            limits,
            &mut |_| Ok(()),
            &mut work,
        )?;
        assert!(std::ptr::eq(
            encoded
                .scalar_binding_in(0, &mut |_| Ok(()), &mut work)?
                .unwrap(),
            &scalar_binding
        ));
        assert_eq!(
            encoded.table_source_id_in(&table, &mut |_| Ok(()), &mut work)?,
            u32::MAX
        );
        let mut raw = encoded.into_wire();
        if bad {
            raw[1].result = None;
        }
        let headers = prepare_function_binding_headers_in(
            &raw,
            &types,
            B,
            limits,
            &mut |_| Ok(()),
            &mut work,
        )?;
        assert_eq!(
            headers
                .definition_in(u32::MAX, &mut |_| Ok(()), &mut work)?
                .unwrap()
                .id,
            u32::MAX
        );
        let token = prepare_function_bindings_materialization_in(
            &headers,
            B * 2,
            limits,
            &mut |_| Ok(()),
            &mut work,
        )?;
        let bound = materialize_function_bindings_in(token, &mut |_| Ok(()), &mut work)?;
        let MaterializedFunctionBinding::Scalar(actual) =
            bound.definition_in(9, &mut |_| Ok(()), &mut work)?.unwrap()
        else {
            panic!("original aggregate carrier")
        };
        assert_eq!(actual, &aggregate);
        assert!(actual.legacy_metadata.is_none());
        let MaterializedFunctionBinding::Table(actual) = bound
            .definition_in(u32::MAX, &mut |_| Ok(()), &mut work)?
            .unwrap()
        else {
            panic!("original table carrier")
        };
        assert_eq!(actual, &table);
        Ok(*bound.facts())
    })();
    finish(out, work)
}
#[test]
fn same_caller_chain_preserves_all_kinds_sparse_ids_lambda_and_complete_fields() {
    let c = Control::default();
    let facts = workflow(&c, None, false).unwrap();
    assert_eq!(facts.definition_count, 4);
    assert_eq!(facts.type_reference_count, 10);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        B * 2 + facts.request_bytes_upper_bound
    );
    assert!(c.trace().iter().all(|(p, _)| *p == CompilePhase::Validate));
}
#[test]
fn input_derived_binding_projection_preserves_chain_geometry_and_source_checks() {
    let explicit = Control::default();
    let input_derived = Control::default();
    assert_eq!(
        workflow_with_limits(&input_derived, None, false, None).unwrap(),
        workflow(&explicit, None, false).unwrap(),
    );
    assert_eq!(input_derived.trace(), explicit.trace());
    assert!(workflow_with_limits(&Control::default(), None, true, None).is_err());
}

#[test]
fn every_actual_chain_callback_preserves_success_ordinary_tail_and_three_primary_causes() {
    for bad in [false, true] {
        let c = Control::default();
        assert_eq!(workflow(&c, None, bad).is_ok(), !bad);
        let trace = c.trace();
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let c = Control::default();
                assert!(
                    matches!(workflow(&c, Some((at, cause.clone())), bad), Err(BindingCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(c.trace(), trace[..=at]);
            }
        }
    }
}
#[test]
fn known_header_index_and_clone_requests_win_before_pending_control() {
    let c = Control::default();
    let v = values();
    let sources = encode_type_table_sources(&v, &[], type_limits(), &c).unwrap();
    let types = decode_type_table(sources.as_wire(), type_limits(), &c).unwrap();
    let f = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(v[2].1.clone())],
        v[0].1.clone(),
    );
    let ids = [ArgumentTypeIds::Value(u32::MAX)];
    let inputs = [FunctionBindingInput {
        id: u32::MAX,
        source: BindingSource::Scalar(&f),
        arguments: &ids,
        result: ResultTypeIds::Scalar(0),
    }];
    let raw = encode_function_bindings(&sources, &inputs, B, limits(), &c)
        .unwrap()
        .into_wire();
    let original: &dyn PureCompileControl = &c;
    let headers = prepare_function_binding_headers(&raw, &types, B, limits(), original).unwrap();
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            for stage in 0..3 {
                c.arm(Some((1, cause.clone())));
                let mut work =
                    CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let mut l = limits();
                l.max_allocation_requests = 0;
                let out = match stage {
                    0 => encode_function_bindings_in(
                        &sources,
                        &inputs,
                        B,
                        l,
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .map(|_| ()),
                    1 => prepare_function_binding_headers_in(
                        &raw,
                        &types,
                        B,
                        l,
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .map(|_| ()),
                    _ => prepare_function_bindings_materialization_in(
                        &headers,
                        B * 2,
                        l,
                        &mut |_| Ok(()),
                        &mut work,
                    )
                    .map(|_| ()),
                };
                assert!(matches!(
                    out,
                    Err(BindingCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(c.trace(), [(CompilePhase::Validate, 0)]);
            }
        }
    }
    // Actual decoded namespace lookup captures the Dictionary loan before its
    // completed callback. Pending work is a private caller-meter seam, not a
    // claim about arbitrary std BTree lookup internal cooperation.
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            c.arm(Some((2, cause)));
            let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
            let mut model = MaterializationModel::for_composition(1, 4, B * 2, 0);
            model.facts.type_reference_count = 1;
            let mut observed_parent = false;
            let mut parent = |facts: &BindingProjectionFacts| {
                if facts.type_reference_count == 1 {
                    observed_parent = true;
                    assert_eq!(facts.allocation_requests_upper_bound, 0);
                    assert_eq!(facts.request_bytes_upper_bound, 0);
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            };
            let result = materialize::value_captured(
                &types,
                u32::MAX,
                &mut |source, work| {
                    assert!(std::ptr::eq(source, types.value_type(u32::MAX).unwrap()));
                    assert_eq!(
                        c.trace(),
                        [(CompilePhase::Validate, 0), (CompilePhase::Validate, 0)]
                    );
                    for _ in 0..pending {
                        work.step()?;
                    }
                    model.count_owned_type_clone_in(source, limits(), &mut parent, work)
                },
                &mut work,
            );
            assert!(matches!(
                result,
                Err(BindingCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert!(observed_parent);
            assert_eq!(
                c.trace(),
                [(CompilePhase::Validate, 0), (CompilePhase::Validate, 0)]
            );
        }
    }
    c.arm(None);
    // Root Vec/Box, argument Vec/Box/holder, and two identity Box/Arc
    // pairs are known before lookup. The normalized type clone allocates zero.
    let base_bytes = 2 * Layout::array::<(u32, MaterializedFunctionBinding)>(1)
        .unwrap()
        .size()
        + 2 * Layout::array::<FunctionArgumentType>(1).unwrap().size()
        + f.function_id.as_str().len()
        + f.overload.as_str().len();
    let expected_bytes = base_bytes
        + novarocks_type_contract::FunctionArgumentTypes::storage_allocation_layout()
            .unwrap()
            .size()
        + FunctionId::storage_allocation_layout().unwrap().size()
        + FunctionOverloadId::storage_allocation_layout()
            .unwrap()
            .size();
    let mut capture_at = None;
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
    let mut observe = |facts: &BindingProjectionFacts| {
        if facts.allocation_requests_upper_bound == 9 {
            assert_eq!(facts.request_bytes_upper_bound, expected_bytes);
            capture_at.get_or_insert(c.trace().len());
        }
        Ok(())
    };
    prepare_function_bindings_materialization_in(
        &headers,
        B * 2,
        limits(),
        &mut observe,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let baseline = c.trace();
    let capture_at = capture_at.expect("actual guarded backing request capture");
    assert!(capture_at < baseline.len());
    for stop in
        std::iter::once(None).chain(CAUSES.into_iter().map(|cause| Some((capture_at, cause))))
    {
        c.arm(stop);
        let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
        let mut rejected = None;
        let mut parent = |facts: &BindingProjectionFacts| {
            // Refuse the first actual backing/holder prefix, before further
            // work or an ordinary footer can publish a later cause.
            if facts.allocation_requests_upper_bound > 8 {
                assert_eq!(facts.allocation_requests_upper_bound, 9);
                assert_eq!(facts.request_bytes_upper_bound, expected_bytes);
                rejected = Some(c.trace().len());
                Err(CompileControlError::ResourceExhausted)
            } else {
                Ok(())
            }
        };
        let result = prepare_function_bindings_materialization_in(
            &headers,
            B * 2,
            limits(),
            &mut parent,
            &mut work,
        );
        assert!(matches!(
            result,
            Err(BindingCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(rejected, Some(capture_at));
        assert_eq!(c.trace(), baseline[..capture_at]);
    }
    c.arm(None);
    // Independent index request: one usize per actual definition, not MAX ID.
    let c = Control::default();
    let original: &dyn PureCompileControl = &c;
    c.arm(None);
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
    let headers =
        prepare_function_binding_headers_in(&raw, &types, B, limits(), &mut |_| Ok(()), &mut work)
            .unwrap();
    assert_eq!(headers.facts().allocation_requests_upper_bound, 1);
    assert_eq!(
        headers.facts().request_bytes_upper_bound,
        Layout::array::<usize>(1).unwrap().size()
    );
    let token = prepare_function_bindings_materialization_in(
        &headers,
        B * 2,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    // Output Vec+Box (2), argument Vec+Box+holder (3), and two ID
    // Box+Arc pairs (4). The normalized FVT clone has no new allocation.
    assert_eq!(token.facts().allocation_requests_upper_bound, 2 + 3 + 4);
    assert_eq!(token.facts().request_bytes_upper_bound, expected_bytes);
}
#[test]
fn same_resource_author_exact_replay_underbounds_and_foreign_control_are_explicit() {
    let c = Control::default();
    let v = values();
    let sources = encode_type_table_sources(&v, &[], type_limits(), &c).unwrap();
    let types = decode_type_table(sources.as_wire(), type_limits(), &c).unwrap();
    let f = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(v[0].1.clone())],
        v[2].1.clone(),
    );
    let ids = [ArgumentTypeIds::Value(0)];
    let inputs = [FunctionBindingInput {
        id: 0,
        source: BindingSource::Scalar(&f),
        arguments: &ids,
        result: ResultTypeIds::Scalar(u32::MAX),
    }];
    let raw = encode_function_bindings(&sources, &inputs, B, limits(), &c)
        .unwrap()
        .into_wire();
    let original: &dyn PureCompileControl = &c;
    let headers = prepare_function_binding_headers(&raw, &types, B, limits(), original).unwrap();
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
    let golden = *prepare_function_bindings_materialization_in(
        &headers,
        B * 2,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap()
    .facts();
    let exact = BindingProjectionLimits {
        max_definitions: golden.definition_count,
        max_type_references: golden.type_reference_count,
        max_request_bytes: golden.request_bytes_upper_bound,
        max_allocation_requests: golden.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: golden
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: golden.cumulative_work_upper_bound,
    };
    prepare_function_bindings_materialization_in(
        &headers,
        B * 2,
        exact,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    for axis in 0..6 {
        let mut l = exact;
        match axis {
            0 => l.max_definitions -= 1,
            1 => l.max_type_references -= 1,
            2 => l.max_request_bytes -= 1,
            3 => l.max_allocation_requests -= 1,
            4 => l.max_coexisting_source_and_request_bytes -= 1,
            _ => l.max_work -= 1,
        }
        assert!(matches!(
            prepare_function_bindings_materialization_in(
                &headers,
                B * 2,
                l,
                &mut |_| Ok(()),
                &mut work
            ),
            Err(BindingCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
    }
    let other = Control::default();
    let mut foreign = CompileCheckpoints::try_new(&other, CompilePhase::Validate).unwrap();
    assert!(matches!(
        prepare_function_bindings_materialization_in(
            &headers,
            B * 2,
            limits(),
            &mut |_| Ok(()),
            &mut foreign
        ),
        Err(BindingCodecError::InvalidShape(
            "binding materialization has a different original control"
        ))
    ));
    let mut work = CompileCheckpoints::try_new(original, CompilePhase::Validate).unwrap();
    assert!(matches!(
        prepare_function_bindings_materialization_in(
            &headers,
            0,
            limits(),
            &mut |_| Ok(()),
            &mut work
        ),
        Err(BindingCodecError::InvalidShape(_))
    ));
}

#[test]
fn wide_actual_sender_walk_retains_real_quantum_and_original_control_samples() {
    let c = Control::default();
    let v = [(0, FunctionValueType::new(DataType::Int64, false))];
    let types = encode_type_table_sources(&v, &[], type_limits(), &c).unwrap();
    let source = scalar(
        FunctionKind::Scalar,
        vec![FunctionArgumentType::Value(v[0].1.clone())],
        v[0].1.clone(),
    );
    let args = [ArgumentTypeIds::Value(0)];
    let inputs: Vec<_> = (0..320)
        .map(|id| FunctionBindingInput {
            id,
            source: BindingSource::Scalar(&source),
            arguments: &args,
            result: ResultTypeIds::Scalar(0),
        })
        .collect();
    let run = |stop| {
        c.arm(stop);
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate)?;
        let out =
            encode_function_bindings_in(&types, &inputs, B, limits(), &mut |_| Ok(()), &mut work)
                .map(|encoded| {
                    assert_eq!(encoded.as_wire().len(), 320);
                    assert_eq!(encoded.as_wire()[319].id, 319);
                    *encoded.facts()
                });
        finish(out, work)
    };
    assert_eq!(run(None).unwrap().type_reference_count, 640);
    let trace = c.trace();
    let quantum = trace
        .iter()
        .position(|(_, n)| *n == 256)
        .expect("actual count loop quantum");
    for at in [0, quantum, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            assert!(
                matches!(run(Some((at, cause.clone()))), Err(BindingCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
