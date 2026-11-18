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

//! The actual prefold SQL binding enters the one sender without a Physical wrapper.
use super::sql_fold_dependency_observation_tests::{REAL, compile_with_observer};
use novarocks_functions::{FunctionArgumentType, FunctionResultType};
use novarocks_plan_codec::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    physical_binding_v2::{
        ArgumentTypeIds, BindingCodecError, BindingProjectionFacts, BindingProjectionLimits,
        BindingSource, FunctionBindingInput, ResultTypeIds, encode_function_bindings_with_host_in,
    },
    physical_type_v2::{TypeProjectionLimits, encode_type_table_sources},
};
use novarocks_sql::compiler::{
    SqlFoldDependencyInput, SqlFoldDependencyObserver, SqlFoldDependencySource,
    SqlFoldEvaluationOutcome,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_workload_control::{
    Reservation, ResourceClass, ResourceConfig, WorkClass, WorkError, WorkRequest, WorkloadConfig,
    WorkloadControl,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
struct Probe {
    actual: WorkloadControl,
    task: novarocks_workload_control::WorkOwner,
    foreign: WorkloadControl,
    raw: WorkError,
    mode: u8,
    seen: AtomicUsize,
    produced: AtomicBool,
    kept: Mutex<Option<String>>,
    cause_identity: AtomicBool,
}
impl Probe {
    fn new(mode: u8) -> Arc<Self> {
        fn owner() -> WorkloadControl {
            let v = WorkloadControl::try_new(
                WorkloadConfig::default(),
                ResourceConfig {
                    total_bytes: 64 * 1024 * 1024,
                    control_bytes: 1024,
                    per_scope_bytes: 64 * 1024 * 1024 - 1024,
                },
            )
            .unwrap();
            v.mark_ready().unwrap();
            v
        }
        let actual = owner();
        let task = actual
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap()
            .owner;
        let foreign = owner();
        let raw = if mode == 2 {
            actual
                .resources()
                .expect("test workload resource authority")
                .reserve(&task.scope(), u64::MAX, ResourceClass::Data)
                .err()
                .unwrap()
        } else {
            foreign
                .resources()
                .expect("test workload resource authority")
                .reserve(&task.scope(), 1, ResourceClass::Data)
                .err()
                .unwrap()
        };
        Arc::new(Self {
            actual,
            task,
            foreign,
            raw,
            mode,
            seen: AtomicUsize::new(0),
            produced: AtomicBool::new(false),
            kept: Mutex::new(None),
            cause_identity: AtomicBool::new(false),
        })
    }
}
impl SqlFoldDependencyObserver for Probe {
    fn before_fold_dependency_observed(
        &self,
        input: SqlFoldDependencyInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        let SqlFoldDependencySource::Function(original) = input.source else {
            return Ok(());
        };
        self.seen.fetch_add(1, Ordering::Relaxed);
        // This is the real original declaration constraint, not Some(selected result).
        assert!(original.result_constraint().is_none());
        let binding = original.resolved();
        let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
            panic!("actual UPPER scalar source");
        };
        assert_eq!(result, &input.request.result_type);
        let mut roots = Vec::new();
        let mut ids = Vec::new();
        for arg in &binding.selected.argument_types {
            let FunctionArgumentType::Value(v) = arg else {
                panic!("actual UPPER value source");
            };
            let id = u32::try_from(roots.len() + 100).unwrap();
            roots.push((id, v.clone()));
            ids.push(ArgumentTypeIds::Value(id));
        }
        let result_id = u32::try_from(roots.len() + 100).unwrap();
        roots.push((result_id, result.clone()));
        let types = encode_type_table_sources(
            &roots,
            &[],
            TypeProjectionLimits {
                max_definitions: 64,
                max_expanded_nodes: 256,
                max_string_bytes: 65536,
            },
            control,
        )
        .map_err(|error| match error {
            novarocks_plan_codec::physical_type_v2::TypeCodecError::Control(cause) => cause,
            other => panic!("actual admitted SQL type sender refused: {other}"),
        })?;
        let inputs = [FunctionBindingInput {
            id: 17,
            source: BindingSource::ResolvedScalar(binding),
            arguments: &ids,
            result: ResultTypeIds::Scalar(result_id),
        }];
        let mut held: Option<Reservation> = None;
        let resources = self
            .actual
            .resources()
            .expect("test workload resource authority");
        let scope = self.task.scope();
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let sent = encode_function_bindings_with_host_in(
            &types,
            &inputs,
            64 * 1024,
            BindingProjectionLimits {
                max_definitions: 64,
                max_type_references: 128,
                max_request_bytes: usize::MAX,
                max_allocation_requests: usize::MAX,
                max_coexisting_source_and_request_bytes: usize::MAX,
                max_work: usize::MAX,
            },
            &mut |facts: &BindingProjectionFacts| -> Result<(), AdmissionRefusal<&WorkError>> {
                if self.mode == 1 {
                    return Err(AdmissionRefusal::Host(&self.raw));
                }
                if self.mode == 2 {
                    assert_eq!(self.raw, WorkError::Capacity("scope allocation bytes"));
                    self.cause_identity.store(true, Ordering::Relaxed);
                    return Err(AdmissionRefusal::Control(
                        CompileControlError::ResourceExhausted,
                    ));
                }
                let bytes = u64::try_from(facts.request_bytes_upper_bound).unwrap();
                match held.as_mut() {
                    Some(v) if bytes > v.remaining_bytes() => {
                        v.grow(bytes - v.remaining_bytes()).unwrap()
                    }
                    Some(_) => {}
                    None if bytes > 0 => {
                        held = Some(
                            resources
                                .reserve(&scope, bytes, ResourceClass::Data)
                                .unwrap(),
                        )
                    }
                    None => {}
                };
                Ok(())
            },
            &mut work,
        );
        let outcome = match sent {
            Ok(encoded) => {
                let wire = encoded.as_wire();
                assert_eq!(wire.len(), 1);
                let row = &wire[0];
                assert_eq!(row.function_id, binding.function_id.as_str());
                assert_eq!(row.overload_id, binding.selected.overload.as_str());
                assert_eq!(row.arguments.len(), binding.selected.argument_types.len());
                assert!(resources.snapshot().data_reserved_bytes > 0);
                assert_eq!(resources.snapshot().data_used_bytes, 0);
                *self.kept.lock().unwrap() = Some(format!("{row:?}"));
                drop(encoded);
                work.finish()?;
                Ok(())
            }
            Err(ProjectionFailure::Host(cause)) => {
                assert!(std::ptr::eq(cause, &self.raw));
                self.cause_identity.store(true, Ordering::Relaxed);
                // Capture-only rejection does not become an SQL or capability error.
                Ok(())
            }
            Err(ProjectionFailure::Codec(BindingCodecError::Control(cause))) => Err(cause),
            Err(ProjectionFailure::Codec(error)) => {
                panic!("actual original scalar signature refused: {error}")
            }
        };
        drop(held);
        assert_eq!(resources.snapshot().held_bytes(), 0);
        assert_eq!(
            self.foreign
                .resources()
                .expect("test workload resource authority")
                .snapshot()
                .held_bytes(),
            0
        );
        outcome
    }
    fn after_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        outcome: SqlFoldEvaluationOutcome<'_>,
    ) {
        if matches!(outcome, SqlFoldEvaluationOutcome::ProducedConstant(_)) {
            self.produced.store(true, Ordering::Relaxed);
        }
    }
}
#[test]
fn sql_dependency_resolved_binding_real_fold_keeps_original_selection_without_physical_clone() {
    let probe = Probe::new(0);
    let plan = compile_with_observer("SELECT upper('kept')", &REAL, Some(probe.clone())).unwrap();
    assert!(probe.seen.load(Ordering::Relaxed) > 0);
    assert!(probe.produced.load(Ordering::Relaxed));
    assert!(probe.kept.lock().unwrap().is_some());
    assert!(
        super::sql_fold_dependency_observation_tests::expression_sources(&plan).all(
            |(_, _, source)| !matches!(
                source.kind,
                novarocks_expression_contract::ExprKind::FunctionCall { .. }
            )
        )
    );
}
#[test]
fn sql_dependency_resolved_binding_capture_host_refusal_keeps_original_fold_and_typed_cause() {
    let probe = Probe::new(1);
    assert_eq!(probe.raw, WorkError::ForeignAuthority);
    let plan = compile_with_observer("SELECT upper('kept')", &REAL, Some(probe.clone())).unwrap();
    assert!(probe.cause_identity.load(Ordering::Relaxed));
    assert!(probe.produced.load(Ordering::Relaxed));
    assert!(probe.kept.lock().unwrap().is_none());
    assert!(plan.plan().result_port().is_some());
}
#[test]
fn sql_dependency_resolved_binding_actual_capacity_is_original_control_before_calculation() {
    let probe = Probe::new(2);
    let error = compile_with_observer("SELECT upper('kept')", &REAL, Some(probe.clone()))
        .err()
        .unwrap();
    assert!(matches!(
        error,
        novarocks_sql::compiler::SqlCompileProgressError::Compile(
            novarocks_sql::compiler::SqlCompileError::ResourceExhausted
        )
    ));
    assert!(probe.cause_identity.load(Ordering::Relaxed));
    assert!(!probe.produced.load(Ordering::Relaxed));
    assert!(probe.kept.lock().unwrap().is_none());
}
