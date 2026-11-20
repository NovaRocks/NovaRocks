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

//! Real FE workload admission on the original SQL request projection.
use crate::query_execution::tests::complete_dependency_fixture;
use arrow::array::StringArray;
use novarocks_physical_plan::{
    ConstantPools, FragmentCallRequests, NodeKind, PhysicalCallSite, StaticFunctionArgument,
};
use novarocks_plan_codec::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    physical_binding_v2::ArgumentTypeIds,
    physical_call_requests_v2::{
        CallRequestCodecError, CallRequestProjectionFacts, CallRequestProjectionLimits,
        CallRequestTypeIds, prepare_call_requests_encode_in,
        prepare_call_requests_encode_with_host_in,
    },
    physical_type_v2::{EncodedTypeTable, TypeProjectionLimits, encode_type_table_sources},
};
use novarocks_sql::compiler::SessionOptimizerSettings;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_workload_control::{
    Reservation, ResourceClass, ResourceConfig, WorkClass, WorkError, WorkRequest, WorkloadConfig,
    WorkloadControl,
};
use std::{convert::Infallible, sync::Mutex};
#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        self.calls.lock().unwrap().push(n);
        Ok(())
    }
}
fn limits() -> CallRequestProjectionLimits {
    CallRequestProjectionLimits {
        max_definitions: 64,
        max_type_references: 128,
        max_request_bytes: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    }
}
fn owner() -> WorkloadControl {
    let owner = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 64 * 1024 * 1024,
            control_bytes: 1024,
            per_scope_bytes: 64 * 1024 * 1024 - 1024,
        },
    )
    .unwrap();
    owner.mark_ready().unwrap();
    owner
}
fn stock(
    held: &mut Option<Reservation>,
    authority: &novarocks_workload_control::LocalResourceAuthority,
    scope: &novarocks_workload_control::WorkScope,
    bytes: usize,
) -> Result<(), WorkError> {
    let bytes = u64::try_from(bytes).unwrap();
    match held {
        Some(current) if bytes > current.remaining_bytes() => {
            current.grow(bytes - current.remaining_bytes())
        }
        Some(_) => Ok(()),
        None if bytes > 0 => {
            *held = Some(authority.reserve(scope, bytes, ResourceClass::Data)?);
            Ok(())
        }
        None => Ok(()),
    }
}
// This helper borrows the one real SQL source. IDs are only namespace projection;
// they do not resolve names, recreate bindings or author constant provenance.
fn source<T>(
    f: impl FnOnce(
        &FragmentCallRequests,
        &EncodedTypeTable<'_>,
        &[CallRequestTypeIds<'_>],
        &ConstantPools,
    ) -> T,
) -> T {
    let owner = complete_dependency_fixture(
        "SELECT max('kept') AS source_call FROM (VALUES (1), (2)) AS t(n)",
        SessionOptimizerSettings {
            disabled_rules: vec!["FoldConstant".into()],
            enable_materialized_view_rewrite: Some(false),
            enable_common_subexpr_reuse: Some(false),
            ..Default::default()
        },
    );
    let fragment = owner
        .plan()
        .fragments()
        .values()
        .find(|fragment| {
            fragment
                .call_requests()
                .entries()
                .iter()
                .any(|(site, request)| {
                    let PhysicalCallSite::Aggregate { node, call } = site else {
                        return false;
                    };
                    let NodeKind::Aggregate { calls, .. } = &fragment.nodes()[node].kind else {
                        return false;
                    };
                    calls[*call as usize]
                        .binding
                        .phase
                        .consumes_logical_arguments()
                        && request.arguments.iter().any(|argument| {
                            matches!(
                                argument,
                                StaticFunctionArgument::Value {
                                    constant: Some(_),
                                    ..
                                }
                            )
                        })
                })
        })
        .expect("actual SQL must publish an aggregate request with its literal");
    let requests = fragment.call_requests();
    let pools = owner.plan().constants();
    let mut values = Vec::new();
    let mut arguments = Vec::new();
    let mut expected = Vec::new();
    let mut constants = 0;
    let source_control = Control::default();
    let mut source_work =
        CompileCheckpoints::try_new(&source_control, CompilePhase::Validate).unwrap();
    for request in requests.entries().values() {
        let mut ids = Vec::new();
        for argument in &request.arguments {
            let StaticFunctionArgument::Value {
                value_type,
                constant,
            } = argument
            else {
                panic!("real MAX fixture has value channels");
            };
            let id = u32::try_from(values.len() + 100).unwrap();
            values.push((id, value_type.clone()));
            ids.push(ArgumentTypeIds::Value(id));
            if let Some(reference) = constant {
                let selected = pools
                    .resolve_source_observed(*reference, &mut source_work)
                    .unwrap();
                assert_eq!(selected.value_type(), value_type);
                assert_eq!(selected.ordinal(), reference.ordinal);
                let backing = selected.pool().array();
                let array = backing.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(array.value(selected.ordinal() as usize), "kept");
                constants += 1;
            }
        }
        arguments.push(ids);
        expected.push(request.expected_result_type.as_ref().map(|v| {
            let id = u32::try_from(values.len() + 100).unwrap();
            values.push((id, v.clone()));
            id
        }));
    }
    source_work.finish().unwrap();
    assert!(
        constants > 0,
        "actual original source retains pool reference and ordinal"
    );
    let type_control = Control::default();
    let types = encode_type_table_sources(
        &values,
        &[],
        TypeProjectionLimits {
            max_definitions: 128,
            max_expanded_nodes: 256,
            max_string_bytes: 65536,
        },
        &type_control,
    )
    .unwrap();
    let ids = requests
        .entries()
        .keys()
        .enumerate()
        .map(|(i, definition)| CallRequestTypeIds {
            definition: *definition,
            arguments: &arguments[i],
            expected_result_type: expected[i],
        })
        .collect::<Vec<_>>();
    f(requests, &types, &ids, pools)
}
const SOURCE_ENVELOPE: usize = 1024 * 1024; // Finite test invoice, never actual total or a grant.
fn run<H>(
    control: &Control,
    admit: &mut dyn FnMut(&CallRequestProjectionFacts) -> Result<(), AdmissionRefusal<H>>,
) -> Result<String, ProjectionFailure<CallRequestCodecError, H>> {
    source(|requests, types, ids, pools| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let token = prepare_call_requests_encode_with_host_in(
            requests,
            types,
            ids,
            pools,
            SOURCE_ENVELOPE,
            limits(),
            admit,
            &mut work,
        )?;
        let wire = token.emit_with_host_in(admit, &mut work)?;
        work.finish()?;
        Ok(format!("{wire:?}"))
    })
}
#[test]
fn sql_dependency_request_host_abort_actual_sql_requests_match_original_full_wire() {
    source(|requests, types, ids, pools| {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let token = prepare_call_requests_encode_in(
            requests,
            types,
            ids,
            pools,
            SOURCE_ENVELOPE,
            limits(),
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let original = token.emit_in(&mut |_| Ok(()), &mut work).unwrap();
        work.finish().unwrap();
        let actual = owner();
        let resources = actual
            .resources()
            .expect("test workload resource authority");
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = task.owner.scope();
        let (same, callbacks, held) = {
            let mut held = None;
            let mut callbacks = 0;
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let mut admit =
                |facts: &CallRequestProjectionFacts| -> Result<(), AdmissionRefusal<WorkError>> {
                    callbacks += 1;
                    stock(
                        &mut held,
                        &resources,
                        &scope,
                        facts.request_bytes_upper_bound,
                    )
                    .map_err(AdmissionRefusal::Host)
                };
            let token = prepare_call_requests_encode_with_host_in(
                requests,
                types,
                ids,
                pools,
                SOURCE_ENVELOPE,
                limits(),
                &mut admit,
                &mut work,
            )
            .unwrap();
            let same = token.emit_with_host_in(&mut admit, &mut work).unwrap();
            work.finish().unwrap();
            (same, callbacks, held)
        };
        assert_eq!(same, original);
        assert!(callbacks > 1);
        assert!(resources.snapshot().data_reserved_bytes > 0);
        assert_eq!(resources.snapshot().data_used_bytes, 0);
        drop(same);
        drop(original);
        drop(held);
        assert_eq!(resources.snapshot().held_bytes(), 0);
    })
}
#[test]
fn sql_dependency_request_host_abort_every_admission_preserves_borrowed_cause_and_rolls_back() {
    let mut count = 0;
    run(&Control::default(), &mut |_| -> Result<
        (),
        AdmissionRefusal<Infallible>,
    > {
        count += 1;
        Ok(())
    })
    .unwrap();
    assert!(count > 1);
    for at in 1..=count {
        let actual = owner();
        let resources = actual
            .resources()
            .expect("test workload resource authority");
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = task.owner.scope();
        let foreign = owner();
        let cause = foreign
            .resources()
            .expect("test workload resource authority")
            .reserve(&scope, 1, ResourceClass::Data)
            .err()
            .unwrap();
        assert_eq!(cause, WorkError::ForeignAuthority);
        let mut calls = 0;
        let mut held = None;
        let control = Control::default();
        let mut refusal_checkpoint = None;
        let error = run(&control, &mut |facts| {
            calls += 1;
            if calls == at {
                refusal_checkpoint = Some(control.calls.lock().unwrap().len());
                return Err(AdmissionRefusal::Host(&cause));
            }
            stock(
                &mut held,
                &resources,
                &scope,
                facts.request_bytes_upper_bound,
            )
            .unwrap();
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(error,ProjectionFailure::Host(found) if std::ptr::eq(found,&cause)));
        assert_eq!(calls, at);
        assert_eq!(
            control.calls.lock().unwrap().len(),
            refusal_checkpoint.unwrap()
        );
        drop(held);
        assert_eq!(resources.snapshot().held_bytes(), 0);
        assert_eq!(
            foreign
                .resources()
                .expect("test workload resource authority")
                .snapshot()
                .held_bytes(),
            0
        );
    }
}
#[test]
fn sql_dependency_request_host_abort_emission_refuses_before_original_dto_work() {
    source(|requests, types, ids, pools| {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let token = prepare_call_requests_encode_with_host_in(
            requests,
            types,
            ids,
            pools,
            SOURCE_ENVELOPE,
            limits(),
            &mut |_| -> Result<(), AdmissionRefusal<Infallible>> { Ok(()) },
            &mut work,
        )
        .unwrap();
        let actual = owner();
        let foreign = owner();
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let cause = foreign
            .resources()
            .expect("test workload resource authority")
            .reserve(&task.owner.scope(), 1, ResourceClass::Data)
            .err()
            .unwrap();
        let before = control.calls.lock().unwrap().len();
        let mut called = 0;
        let error = token
            .emit_with_host_in(
                &mut |_| {
                    called += 1;
                    Err(AdmissionRefusal::Host(&cause))
                },
                &mut work,
            )
            .unwrap_err();
        assert!(matches!(error,ProjectionFailure::Host(found) if std::ptr::eq(found,&cause)));
        assert_eq!(called, 1);
        assert_eq!(control.calls.lock().unwrap().len(), before);
    })
}
#[test]
fn sql_dependency_request_host_abort_actual_capacity_projection_keeps_original_cause() {
    let actual = owner();
    let resources = actual
        .resources()
        .expect("test workload resource authority");
    let task = actual
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let cause = resources
        .reserve(&task.owner.scope(), u64::MAX, ResourceClass::Data)
        .err()
        .unwrap();
    assert_eq!(cause, WorkError::Capacity("scope allocation bytes"));
    let mut original = None;
    let error = run(&Control::default(), &mut |_| -> Result<
        (),
        AdmissionRefusal<Infallible>,
    > {
        original = Some(&cause);
        Err(AdmissionRefusal::Control(
            CompileControlError::ResourceExhausted,
        ))
    })
    .unwrap_err();
    assert!(matches!(
        error,
        ProjectionFailure::Codec(CallRequestCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert!(std::ptr::eq(original.unwrap(), &cause));
    assert_eq!(resources.snapshot().held_bytes(), 0);
}
