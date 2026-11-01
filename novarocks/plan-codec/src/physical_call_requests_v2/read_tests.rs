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
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_physical_plan::ConstantPool;
use novarocks_type_contract::ValueLogicalType;
use std::sync::{Arc, Mutex};

const SOURCE: usize = 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((stop, _)) = self.stop {
            assert!(trace.len() <= stop, "callback after refusal");
        }
        let at = trace.len();
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> Limits {
    Limits {
        max_definitions: 1024,
        max_type_references: 4096,
        max_request_bytes: 1024 * 1024,
        max_allocation_requests: 4096,
        max_coexisting_source_and_request_bytes: 2 * 1024 * 1024,
        max_work: 512 * 1024 * 1024,
    }
}
fn pool_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    }
}
fn types() -> DecodedTypeTable {
    #[allow(deprecated)]
    let field = Field::new_dict(
        "original_child\0字",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
        i64::MIN,
        true,
    )
    .with_metadata(std::collections::HashMap::<String, String>::from([(
        "author".into(),
        "retained\0元".into(),
    )]));
    let roots = [
        (0, FunctionValueType::new(DataType::Int64, true)),
        (7, FunctionValueType::new(DataType::Int64, false)),
        (
            42,
            FunctionValueType::new(
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                true,
            ),
        ),
        (
            8,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
        (
            u32::MAX,
            FunctionValueType::new(DataType::Struct(vec![Arc::new(field)].into()), true),
        ),
    ];
    let l = TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 1024,
        max_string_bytes: 65536,
    };
    let wire = encode_type_table(&roots, l, &Setup).unwrap();
    decode_type_table(&wire, l, &Setup).unwrap()
}
fn pools() -> ConstantPools {
    let ty = FunctionValueType::new(DataType::Int64, true);
    let field = Arc::new(ty.try_to_field("original_admitted\0field").unwrap());
    let pool = ConstantPool::try_new(
        field,
        ty,
        Int64Array::from(vec![Some(71), None]).to_data(),
        pool_policy(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools.insert(ConstantPoolId::new(7), pool.clone()).unwrap();
    pools.insert(ConstantPoolId::new(u32::MAX), pool).unwrap();
    pools
}
fn zero_policy() -> wire::SourceConstantPolicy {
    wire::SourceConstantPolicy {
        max_rows: Some(0),
        max_array_nodes: Some(0),
        max_logical_elements: Some(0),
        max_retained_buffer_bytes: Some(0),
        max_type_depth: Some(0),
        max_type_nodes: Some(0),
        max_dictionary_depth: Some(0),
        max_metadata_bytes: Some(0),
        max_library_validation_work: Some(0),
        max_library_validation_bytes: Some(0),
    }
}
fn expression(id: u32) -> wire::CallRequestDefinition {
    wire::CallRequestDefinition {
        kind: Some(wire::call_request_definition::Kind::ExpressionDefinitionId(
            id,
        )),
    }
}
fn value_argument(
    id: u32,
    constant: Option<wire::ConstantReference>,
) -> wire::OriginalFunctionArgument {
    wire::OriginalFunctionArgument {
        kind: Some(wire::original_function_argument::Kind::Value(
            wire::OriginalValueArgument {
                value_type_id: Some(id),
                constant,
            },
        )),
    }
}
fn request(id: u32) -> wire::OriginalCallRequest {
    wire::OriginalCallRequest {
        definition: Some(expression(id)),
        arguments: vec![],
        logical_argument_count: Some(0),
        expected_result_value_type_id: None,
        constant_policy: Some(zero_policy()),
    }
}
fn source() -> wire::FragmentCallRequests {
    let mut entry = request(u32::MAX);
    entry.arguments = vec![
        value_argument(0, None),
        value_argument(
            0,
            Some(wire::ConstantReference {
                pool_id: Some(7),
                row_ordinal: 1,
            }),
        ),
        wire::OriginalFunctionArgument {
            kind: Some(wire::original_function_argument::Kind::Lambda(
                wire::LambdaArgumentType {
                    parameter_value_type_ids: vec![u32::MAX, 42, 8, 0],
                    result_value_type_id: Some(7),
                },
            )),
        },
    ];
    entry.logical_argument_count = Some(2);
    entry.expected_result_value_type_id = Some(u32::MAX);
    wire::FragmentCallRequests {
        entries: vec![entry],
    }
}
fn execute(
    source: Option<&wire::FragmentCallRequests>,
    types: &DecodedTypeTable,
    pools: &ConstantPools,
    invoice: usize,
    l: Limits,
    c: &dyn PureCompileControl,
) -> Result<Vec<(PhysicalCallDefinition, PhysicalCallRequest)>, E> {
    decode_call_requests(prepare_call_requests_decode(
        source, types, pools, invoice, l, c,
    )?)
}
fn assert_causes(run: impl Fn(&Control) -> Result<(), E>) {
    let c = Control::default();
    let baseline = run(&c);
    assert!(
        !matches!(baseline, Err(E::Control(_))),
        "baseline unexpectedly resource/control refused"
    );
    let trace = c.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(run(&c),Err(E::Control(actual)) if actual==cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn original_request_decode_requires_present_table_count_and_all_ten_policy_fields_including_zero() {
    let t = types();
    let p = ConstantPools::empty();
    assert!(matches!(
        execute(None, &t, &p, SOURCE, limits(), &Control::default()),
        Err(E::InvalidShape(_))
    ));
    let empty = wire::FragmentCallRequests { entries: vec![] };
    assert!(
        execute(Some(&empty), &t, &p, SOURCE, limits(), &Control::default())
            .unwrap()
            .is_empty()
    );
    let valid = wire::FragmentCallRequests {
        entries: vec![request(0)],
    };
    let decoded = execute(Some(&valid), &t, &p, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(
        decoded[0].0,
        PhysicalCallDefinition::Expression(ExprId::new(0))
    );
    assert_eq!(decoded[0].1.logical_argument_count, 0);
    assert!(decoded[0].1.expected_result_type.is_none());
    assert_eq!(decoded[0].1.constant_policy.max_rows, 0);
    for missing in 0..12 {
        let mut bad = valid.clone();
        let e = &mut bad.entries[0];
        match missing {
            0 => e.logical_argument_count = None,
            1 => e.constant_policy = None,
            n => {
                let p = e.constant_policy.as_mut().unwrap();
                match n {
                    2 => p.max_rows = None,
                    3 => p.max_array_nodes = None,
                    4 => p.max_logical_elements = None,
                    5 => p.max_retained_buffer_bytes = None,
                    6 => p.max_type_depth = None,
                    7 => p.max_type_nodes = None,
                    8 => p.max_dictionary_depth = None,
                    9 => p.max_metadata_bytes = None,
                    10 => p.max_library_validation_work = None,
                    11 => p.max_library_validation_bytes = None,
                    _ => unreachable!(),
                }
            }
        }
        assert!(
            matches!(
                execute(Some(&bad), &t, &p, SOURCE, limits(), &Control::default()),
                Err(E::InvalidShape(_))
            ),
            "missing {missing}"
        );
    }
    let mut maximum = valid.clone();
    let p = maximum.entries[0].constant_policy.as_mut().unwrap();
    p.max_rows = Some(u64::MAX);
    p.max_type_depth = Some(u32::MAX);
    let p = execute(
        Some(&maximum),
        &t,
        &ConstantPools::empty(),
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap()[0]
        .1
        .constant_policy;
    assert_eq!(p.max_rows, u64::MAX);
    assert_eq!(p.max_type_depth, u32::MAX);
}
#[test]
fn original_request_decode_keeps_sparse_definition_namespace_relational_order_and_rejects_duplicates()
 {
    use novarocks_proto_models::physical_semantics_v2 as sw;
    let t = types();
    let p = ConstantPools::empty();
    let sites = [
        PhysicalCallSite::Aggregate {
            node: novarocks_physical_plan::NodeId::new(u32::MAX),
            call: 0,
        },
        PhysicalCallSite::TopNState {
            node: novarocks_physical_plan::NodeId::new(0),
            call: u32::MAX,
        },
        PhysicalCallSite::WriterPartial {
            node: novarocks_physical_plan::NodeId::new(7),
            call: 7,
        },
        PhysicalCallSite::WriterFinal {
            node: novarocks_physical_plan::NodeId::new(7),
            call: 7,
        },
        PhysicalCallSite::Table {
            node: novarocks_physical_plan::NodeId::new(u32::MAX),
        },
    ];
    let mut source = wire::FragmentCallRequests {
        entries: vec![request(u32::MAX), request(0)],
    };
    for site in sites {
        let mut e = request(0);
        e.definition = Some(wire::CallRequestDefinition {
            kind: Some(wire::call_request_definition::Kind::Relational(
                crate::physical_semantics_v2::encode_call_site(site),
            )),
        });
        source.entries.push(e);
    }
    let output = execute(Some(&source), &t, &p, SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(
        output[0].0,
        PhysicalCallDefinition::Expression(ExprId::new(u32::MAX))
    );
    assert_eq!(
        output[1].0,
        PhysicalCallDefinition::Expression(ExprId::new(0))
    );
    for (entry, site) in output[2..].iter().zip(sites) {
        assert_eq!(entry.0, PhysicalCallDefinition::Relational(site));
    }
    let mut duplicate = source.clone();
    duplicate.entries.push(source.entries[0].clone());
    assert!(matches!(
        execute(
            Some(&duplicate),
            &t,
            &p,
            SOURCE,
            limits(),
            &Control::default()
        ),
        Err(E::InvalidShape("call request definition is duplicated"))
    ));
    let mut bad = source.clone();
    bad.entries[0].definition = Some(wire::CallRequestDefinition {
        kind: Some(wire::call_request_definition::Kind::Relational(
            sw::CallSite {
                kind: Some(sw::call_site::Kind::ExpressionUseId(u32::MAX)),
            },
        )),
    });
    assert!(matches!(
        execute(Some(&bad), &t, &p, SOURCE, limits(), &Control::default()),
        Err(E::InvalidShape(_))
    ));
    bad.entries[0].definition.as_mut().unwrap().kind = None;
    assert!(matches!(
        execute(Some(&bad), &t, &p, SOURCE, limits(), &Control::default()),
        Err(E::InvalidShape(_))
    ));
}
#[test]
fn original_request_decode_preserves_none_typed_null_cv_ordinal_full_lambda_and_field_arc() {
    let t = types();
    let pools = pools();
    let raw = source();
    let c = Control::default();
    let token = prepare_call_requests_decode(Some(&raw), &t, &pools, SOURCE, limits(), &c).unwrap();
    assert!(std::ptr::eq(token.source, &raw));
    assert!(matches!(token.types,CallRequestTypeOwner::Package(source) if std::ptr::eq(source,&t)));
    assert!(std::ptr::eq(token._pools, &pools));
    let output = decode_call_requests(token).unwrap();
    let request = &output[0].1;
    assert_eq!(request.arguments.len(), 3);
    assert_eq!(request.logical_argument_count, 2);
    assert_eq!(
        request.expected_result_type.as_ref().unwrap(),
        t.value_type(u32::MAX).unwrap()
    );
    assert!(matches!(
        &request.arguments[0],
        StaticFunctionArgument::Value { constant: None, .. }
    ));
    let StaticFunctionArgument::Value {
        value_type,
        constant: Some(address),
    } = &request.arguments[1]
    else {
        panic!("Some NULL lost")
    };
    assert_eq!(address.pool, ConstantPoolId::new(7));
    assert_eq!(address.ordinal, 1);
    let mut w = CompileCheckpoints::try_new(&Setup, CompilePhase::Validate).unwrap();
    let original = pools.resolve_source_observed(*address, &mut w).unwrap();
    w.finish().unwrap();
    assert_eq!(original.value_type(), value_type);
    assert_eq!(original.ordinal(), 1);
    assert_eq!(
        original.pool().backing_identity(),
        pools.entries()[&address.pool].backing_identity()
    );
    assert!(Arc::ptr_eq(
        original.pool().field_ref(),
        pools.entries()[&address.pool].field_ref()
    ));
    assert!(original.pool().array().is_null(1));
    let StaticFunctionArgument::Lambda {
        parameter_types,
        result_type,
    } = &request.arguments[2]
    else {
        panic!("Lambda lost")
    };
    assert_eq!(parameter_types.len(), 4);
    assert_eq!(result_type, t.value_type(7).unwrap());
    assert_eq!(parameter_types[2].logical_type(), ValueLogicalType::Json);
    let (DataType::Struct(a), DataType::Struct(b)) = (
        &parameter_types[0].data_type(),
        &t.value_type(u32::MAX).unwrap().data_type(),
    ) else {
        panic!("Struct lost")
    };
    assert!(Arc::ptr_eq(&a[0], &b[0]));
    assert!(a[0].metadata().as_arc().is_none());
    assert_eq!(parameter_types[1].data_type(), &DataType::Utf8);
    assert!(std::ptr::eq(
        parameter_types[1].data_type(),
        t.value_type(42).unwrap().data_type()
    ));
    assert!(std::ptr::eq(
        parameter_types[0].data_type(),
        t.value_type(u32::MAX).unwrap().data_type()
    ));
    assert_eq!(
        request.constant_policy.max_rows, 0,
        "source policy was incorrectly used to re-admit the pool"
    );
}
#[test]
fn original_request_decode_rejects_missing_oob_and_full_type_drift_without_new_cv_admission() {
    let t = types();
    let p = pools();
    let valid = source();
    for case in 0..8 {
        let mut bad = valid.clone();
        match case {
            0 => bad.entries[0].arguments[0].kind = None,
            1 => {
                let wire::original_function_argument::Kind::Value(v) =
                    bad.entries[0].arguments[0].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.value_type_id = None;
            }
            2 => {
                let wire::original_function_argument::Kind::Value(v) =
                    bad.entries[0].arguments[0].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.value_type_id = Some(999);
            }
            3 => {
                let wire::original_function_argument::Kind::Value(v) =
                    bad.entries[0].arguments[1].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.constant.as_mut().unwrap().pool_id = None;
            }
            4 => {
                let wire::original_function_argument::Kind::Value(v) =
                    bad.entries[0].arguments[1].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.constant.as_mut().unwrap().pool_id = Some(999);
            }
            5 => {
                let wire::original_function_argument::Kind::Value(v) =
                    bad.entries[0].arguments[1].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.constant.as_mut().unwrap().row_ordinal = 2;
            }
            6 => {
                let wire::original_function_argument::Kind::Value(v) =
                    bad.entries[0].arguments[1].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.value_type_id = Some(7);
            }
            7 => {
                let wire::original_function_argument::Kind::Lambda(v) =
                    bad.entries[0].arguments[2].kind.as_mut().unwrap()
                else {
                    unreachable!()
                };
                v.result_value_type_id = None;
            }
            _ => unreachable!(),
        }
        assert!(
            execute(Some(&bad), &t, &p, SOURCE, limits(), &Control::default()).is_err(),
            "case {case}"
        );
    }
    let mut pool_zero = source();
    let wire::original_function_argument::Kind::Value(v) =
        pool_zero.entries[0].arguments[1].kind.as_mut().unwrap()
    else {
        unreachable!()
    };
    v.constant.as_mut().unwrap().pool_id = Some(0);
    v.constant.as_mut().unwrap().row_ordinal = 0;
    let mut zero_pools = ConstantPools::empty();
    zero_pools
        .insert(
            ConstantPoolId::new(0),
            p.entries()[&ConstantPoolId::new(7)].clone(),
        )
        .unwrap();
    let output = execute(
        Some(&pool_zero),
        &t,
        &zero_pools,
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    assert!(
        matches!(&output[0].1.arguments[1],StaticFunctionArgument::Value {constant:Some(ConstantReference {pool,ordinal:0}),..} if pool.get()==0)
    );
}
#[test]
fn original_request_decode_independent_layout_and_all_six_exact_under_limits() {
    let t = types();
    let p = pools();
    let raw = source();
    let c = Control::default();
    let token = prepare_call_requests_decode(Some(&raw), &t, &p, SOURCE, limits(), &c).unwrap();
    let facts = *token.facts();
    // One sparse key Vec and outer output Vec; arguments Vec+Box, four Lambda
    // parameters Vec+Box and their actual guarded collection Arc; FVT clones allocate nothing.
    let golden = Layout::array::<PhysicalCallDefinition>(1).unwrap().size()
        + Layout::array::<(PhysicalCallDefinition, PhysicalCallRequest)>(1)
            .unwrap()
            .size()
        + 2 * Layout::array::<StaticFunctionArgument>(3).unwrap().size()
        + 2 * Layout::array::<FunctionValueType>(4).unwrap().size()
        + novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout()
            .unwrap()
            .size();
    assert_eq!(facts.request_bytes_upper_bound, golden);
    assert_eq!(facts.allocation_requests_upper_bound, 7);
    assert_eq!(facts.definition_count, 1);
    assert_eq!(facts.type_reference_count, 8);
    let exact = Limits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_request_bytes: facts.request_bytes_upper_bound,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    };
    assert!(execute(Some(&raw), &t, &p, SOURCE, exact, &Control::default()).is_ok());
    for axis in 0..6 {
        let mut under = exact;
        match axis {
            0 => under.max_definitions -= 1,
            1 => under.max_type_references -= 1,
            2 => under.max_request_bytes -= 1,
            3 => under.max_allocation_requests -= 1,
            4 => under.max_coexisting_source_and_request_bytes -= 1,
            5 => under.max_work -= 1,
            _ => unreachable!(),
        }
        assert!(
            matches!(
                execute(Some(&raw), &t, &p, SOURCE, under, &Control::default()),
                Err(E::Control(CompileControlError::ResourceExhausted))
            ),
            "axis {axis}"
        );
    }
    assert!(matches!(
        execute(
            Some(&raw),
            &t,
            &p,
            usize::MAX,
            limits(),
            &Control::default()
        ),
        Err(E::Control(CompileControlError::ResourceExhausted))
    ));
}
#[test]
fn original_request_decode_source_capacity_floor_aliases_and_every_small_control_prefix() {
    let t = types();
    let p = pools();
    let raw = source();
    assert!(matches!(
        execute(Some(&raw), &t, &p, 0, limits(), &Control::default()),
        Err(E::InvalidShape(_))
    ));
    // Inflate a real retained source Vec capacity without adding definitions.
    let mut capacity = wire::FragmentCallRequests {
        entries: Vec::with_capacity(512),
    };
    capacity.entries.push(request(0));
    let occupied = size_of::<wire::FragmentCallRequests>()
        + Layout::array::<wire::OriginalCallRequest>(1)
            .unwrap()
            .size()
        + size_of::<DecodedTypeTable>()
        + t.value_types().len() * (size_of::<u32>() + size_of::<FunctionValueType>())
        + size_of::<ConstantPools>();
    assert!(matches!(
        execute(
            Some(&capacity),
            &t,
            &ConstantPools::empty(),
            occupied,
            limits(),
            &Control::default()
        ),
        Err(E::InvalidShape(_))
    ));
    assert!(
        execute(
            Some(&capacity),
            &t,
            &ConstantPools::empty(),
            SOURCE,
            limits(),
            &Control::default()
        )
        .is_ok()
    );
    assert_causes(|c| execute(Some(&raw), &t, &p, SOURCE, limits(), c).map(|_| ()));
    let mut ordinary = raw.clone();
    ordinary.entries[0].logical_argument_count = None;
    assert_causes(|c| execute(Some(&ordinary), &t, &p, SOURCE, limits(), c).map(|_| ()));
    assert_causes(|c| execute(None, &t, &p, SOURCE, limits(), c).map(|_| ()));
}
#[test]
fn original_request_decode_wide_real_argument_quantum_and_budget_prefixes() {
    let t = types();
    let p = ConstantPools::empty();
    let mut entry = request(u32::MAX);
    entry.arguments = (0..320).map(|_| value_argument(7, None)).collect();
    entry.logical_argument_count = Some(320);
    let raw = wire::FragmentCallRequests {
        entries: vec![entry],
    };
    let c = Control::default();
    let out = execute(Some(&raw), &t, &p, SOURCE, limits(), &c).unwrap();
    assert_eq!(out[0].1.arguments.len(), 320);
    let trace = c.trace.lock().unwrap().clone();
    // Opaque type lookups flush often; actual callbacks still cover all 320
    // inputs. This does not fabricate a 256 library-internal work claim.
    assert!(trace.iter().any(|(_, n)| *n > 0));
    assert!(trace.iter().all(|(_, n)| *n <= 256));
    for at in [0, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            let c = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(execute(Some(&raw),&t,&p,SOURCE,limits(),&c),Err(E::Control(actual)) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
    // An admitted 320-definition key pass has a real cooperative heap-sort
    // quantum independent of the per-type opaque lookup exits above.
    let keys = wire::FragmentCallRequests {
        entries: (0..320).rev().map(request).collect(),
    };
    let c = Control::default();
    execute(Some(&keys), &t, &p, SOURCE, limits(), &c).unwrap();
    let trace = c.trace.lock().unwrap().clone();
    let quantum = trace
        .iter()
        .position(|(_, n)| *n == 256)
        .expect("actual key loop quantum");
    for cause in CAUSES {
        let c = Control {
            stop: Some((quantum, cause.clone())),
            ..Control::default()
        };
        assert!(
            matches!(execute(Some(&keys),&t,&p,SOURCE,limits(),&c),Err(E::Control(actual)) if actual==cause)
        );
        assert_eq!(*c.trace.lock().unwrap(), trace[..=quantum]);
    }
}

#[test]
fn original_request_decode_new_argument_resource_at_real_pending_256_never_observes_later_control()
{
    let t = types();
    let original = pools();
    let mut p = ConstantPools::empty();
    for id in 0..4 {
        p.insert(
            ConstantPoolId::new(id),
            original.entries()[&ConstantPoolId::new(7)].clone(),
        )
        .unwrap();
    }
    let mut raw = wire::FragmentCallRequests {
        entries: (0..42).map(request).collect(),
    };
    raw.entries[41].arguments.push(value_argument(7, None));
    raw.entries[41].logical_argument_count = Some(1);
    // This authentic source reaches the first real 256-unit callback precisely
    // when admission of the last entry's new argument succeeds.
    let c = Control::default();
    execute(Some(&raw), &t, &p, SOURCE, limits(), &c).unwrap();
    let trace = c.trace.lock().unwrap().clone();
    assert_eq!(trace.iter().position(|(_, units)| *units == 256), Some(1));
    // Admit both original outer arrays exactly, but no new argument Vec/Box.
    // Repeating the same backing under four keys is legal and counted once as
    // a backing floor, while each occupied key remains actual source storage.
    let mut l = limits();
    l.max_request_bytes = Layout::array::<PhysicalCallDefinition>(42).unwrap().size()
        + Layout::array::<(PhysicalCallDefinition, PhysicalCallRequest)>(42)
            .unwrap()
            .size();
    for cause in CAUSES {
        let c = Control {
            stop: Some((1, cause)),
            ..Control::default()
        };
        assert!(matches!(
            execute(Some(&raw), &t, &p, SOURCE, l, &c),
            Err(E::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(
            *c.trace.lock().unwrap(),
            [(CompilePhase::Decode, 0)],
            "known numeric resource refusal must precede a later callback"
        );
    }
}

mod receiver_owned_tests {
    include!("receiver_owned_tests.rs");
}
