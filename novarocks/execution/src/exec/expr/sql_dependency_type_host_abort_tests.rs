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

//! Actual original type sender with real host admission and nominal capture-only refusal.
use arrow::datatypes::{DataType, Field};
use novarocks_plan_codec::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    physical_type_v2::{
        PackageTypeProjectionLimits, TypeCodecError, encode_borrowed_type_table_writer_sources_in,
        encode_borrowed_type_table_writer_sources_with_host_in,
    },
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueLogicalType,
};
use novarocks_workload_control::{
    Reservation, ResourceClass, ResourceConfig, WorkClass, WorkError, WorkRequest, WorkloadConfig,
    WorkloadControl,
};
use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
struct Control {
    calls: Mutex<Vec<u32>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(refuse: Option<(usize, CompileControlError)>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refuse,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push(units);
        if let Some((at, cause)) = self.refuse.clone()
            && calls.len() == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn limits() -> PackageTypeProjectionLimits {
    PackageTypeProjectionLimits {
        max_definitions: 64,
        max_expanded_nodes: 64,
        max_string_bytes: 4096,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
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
fn value() -> FunctionValueType {
    let field = Arc::new(Field::new("nested", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "actual_key".to_string(),
            "original_value".to_string(),
        )]),
    ));
    FunctionValueType::try_with_logical_type(
        DataType::Struct(vec![field].into()),
        true,
        ValueLogicalType::Physical,
    )
    .unwrap()
}
fn stock(
    reservation: &mut Option<Reservation>,
    authority: &novarocks_workload_control::LocalResourceAuthority,
    scope: &novarocks_workload_control::WorkScope,
    bytes: usize,
) -> Result<(), WorkError> {
    let bytes = u64::try_from(bytes).unwrap();
    match reservation {
        Some(current) if bytes > current.remaining_bytes() => {
            current.grow(bytes - current.remaining_bytes())
        }
        Some(_) => Ok(()),
        None if bytes > 0 => {
            *reservation = Some(authority.reserve(scope, bytes, ResourceClass::Data)?);
            Ok(())
        }
        None => Ok(()),
    }
}
#[test]
fn sql_dependency_type_host_abort_control_adapter_matches_full_original_emission() {
    let v = value();
    let values = [(u32::MAX, &v)];
    let fields = [];
    // Explicit finite fixture source envelope, not the actual retained total.
    // It includes Arrow Field/metadata backing omitted by the old inline-only invoice.
    let source = 64 * 1024;
    let control = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let original = encode_borrowed_type_table_writer_sources_in(
        &values,
        &fields,
        &[],
        source,
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let owner = owner();
    let task = owner
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let resources = owner.resources().expect("test workload resource authority");
    let scope = task.owner.scope();
    let mut reservation = None;
    let mut callbacks = 0;
    let same = encode_borrowed_type_table_writer_sources_with_host_in(
        &values,
        &fields,
        &[],
        source,
        limits(),
        &mut |facts| -> Result<(), AdmissionRefusal<WorkError>> {
            callbacks += 1;
            stock(
                &mut reservation,
                &resources,
                &scope,
                facts.allocation_request_bytes_upper_bound,
            )
            .map_err(AdmissionRefusal::Host)
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(same.as_wire(), original.as_wire());
    assert!(callbacks > 1);
    assert!(resources.snapshot().data_reserved_bytes > 0);
    assert_eq!(resources.snapshot().data_used_bytes, 0);
    drop(same);
    drop(reservation);
    assert_eq!(resources.snapshot().held_bytes(), 0);
}
#[test]
fn sql_dependency_type_host_abort_borrows_the_actual_foreign_authority_cause_without_control() {
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
    assert_eq!(cause, WorkError::ForeignAuthority);
    let v = FunctionValueType::try_with_logical_type(
        DataType::Int64,
        false,
        ValueLogicalType::Physical,
    )
    .unwrap();
    let values = [(0, &v)];
    let source = std::mem::size_of_val(&values) + std::mem::size_of_val(&v);
    let control = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let calls = AtomicUsize::new(0);
    let error = encode_borrowed_type_table_writer_sources_with_host_in(
        &values,
        &[],
        &[],
        source,
        limits(),
        &mut |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            Err(AdmissionRefusal::Host(&cause))
        },
        &mut work,
    )
    .err()
    .unwrap();
    assert!(matches!(error,ProjectionFailure::Host(error) if std::ptr::eq(error,&cause)));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(control.calls.lock().unwrap().len(), 1);
    assert_eq!(
        actual
            .resources()
            .expect("test workload resource authority")
            .snapshot()
            .held_bytes(),
        0
    );
    assert_eq!(
        foreign
            .resources()
            .expect("test workload resource authority")
            .snapshot()
            .held_bytes(),
        0
    );
}
#[test]
fn sql_dependency_type_host_abort_every_actual_admission_rolls_back_and_has_no_tail() {
    let v = value();
    let values = [(0, &v)];
    // Explicit finite fixture source envelope, not the actual retained total.
    // It includes Arrow Field/metadata backing omitted by the old inline-only invoice.
    let source = 64 * 1024;
    let actual = owner();
    let resources = actual
        .resources()
        .expect("test workload resource authority");
    let task = actual
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let scope = task.owner.scope();
    let foreign = owner();
    let count = AtomicUsize::new(0);
    let mut held = None;
    let control = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let success = encode_borrowed_type_table_writer_sources_with_host_in(
        &values,
        &[],
        &[],
        source,
        limits(),
        &mut |facts| -> Result<(), AdmissionRefusal<WorkError>> {
            count.fetch_add(1, Ordering::Relaxed);
            stock(
                &mut held,
                &resources,
                &scope,
                facts.allocation_request_bytes_upper_bound,
            )
            .map_err(AdmissionRefusal::Host)
        },
        &mut work,
    )
    .unwrap();
    drop(success);
    drop(held);
    for at in 1..=count.load(Ordering::Relaxed) {
        let mut seen = 0;
        let mut held = None;
        let control = Control::new(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let error = encode_borrowed_type_table_writer_sources_with_host_in(
            &values,
            &[],
            &[],
            source,
            limits(),
            &mut |facts| -> Result<(), AdmissionRefusal<WorkError>> {
                seen += 1;
                if seen == at {
                    let cause = foreign
                        .resources()
                        .expect("test workload resource authority")
                        .reserve(
                            &scope,
                            facts.allocation_request_bytes_upper_bound.max(1) as u64,
                            ResourceClass::Data,
                        )
                        .err()
                        .unwrap();
                    return Err(AdmissionRefusal::Host(cause));
                }
                stock(
                    &mut held,
                    &resources,
                    &scope,
                    facts.allocation_request_bytes_upper_bound,
                )
                .map_err(AdmissionRefusal::Host)
            },
            &mut work,
        )
        .err()
        .unwrap();
        assert!(matches!(
            error,
            ProjectionFailure::Host(WorkError::ForeignAuthority)
        ));
        assert_eq!(seen, at);
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
fn sql_dependency_type_host_abort_all_original_control_checkpoints_preserve_first_cause() {
    let v = value();
    let values = [(0, &v)];
    // Explicit finite fixture source envelope, not the actual retained total.
    // It includes Arrow Field/metadata backing omitted by the old inline-only invoice.
    let source = 64 * 1024;
    let success = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&success, CompilePhase::Encode).unwrap();
    let _ = encode_borrowed_type_table_writer_sources_with_host_in(
        &values,
        &[],
        &[],
        source,
        limits(),
        &mut |_| -> Result<(), AdmissionRefusal<Infallible>> { Ok(()) },
        &mut work,
    )
    .unwrap();
    let checkpoints = success.calls.lock().unwrap().len();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        // The parent's original entry is checked before the encoder can borrow work.
        for at in 2..=checkpoints {
            let control = Control::new(Some((at, cause.clone())));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let error = encode_borrowed_type_table_writer_sources_with_host_in(
                &values,
                &[],
                &[],
                source,
                limits(),
                &mut |_| -> Result<(), AdmissionRefusal<Infallible>> { Ok(()) },
                &mut work,
            )
            .err()
            .unwrap();
            assert!(
                matches!(error,ProjectionFailure::Codec(TypeCodecError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.calls.lock().unwrap().len(), at);
        }
    }
}

#[test]
fn sql_dependency_type_host_abort_only_real_capacity_uses_control_projection_and_keeps_work_cause()
{
    let actual = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 256,
            control_bytes: 64,
            per_scope_bytes: 192,
        },
    )
    .unwrap();
    actual.mark_ready().unwrap();
    let task = actual
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let v = FunctionValueType::try_with_logical_type(
        DataType::Int64,
        false,
        ValueLogicalType::Physical,
    )
    .unwrap();
    let values = [(0, &v)];
    let source = std::mem::size_of_val(&values) + std::mem::size_of_val(&v);
    let control = Control::new(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let mut provenance = None;
    let error = encode_borrowed_type_table_writer_sources_with_host_in(
        &values,
        &[],
        &[],
        source,
        limits(),
        &mut |facts| -> Result<(), AdmissionRefusal<WorkError>> {
            let cause = actual
                .resources()
                .expect("test workload resource authority")
                .reserve(
                    &task.owner.scope(),
                    facts.allocation_request_bytes_upper_bound as u64,
                    ResourceClass::Data,
                )
                .err()
                .unwrap();
            match &cause {
                WorkError::Capacity(_) => {
                    provenance = Some(cause);
                    Err(AdmissionRefusal::Control(
                        CompileControlError::ResourceExhausted,
                    ))
                }
                _ => Err(AdmissionRefusal::Host(cause)),
            }
        },
        &mut work,
    )
    .err()
    .unwrap();
    assert!(matches!(
        error,
        ProjectionFailure::Codec(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert!(matches!(provenance, Some(WorkError::Capacity(_))));
    assert_eq!(
        actual
            .resources()
            .expect("test workload resource authority")
            .snapshot()
            .held_bytes(),
        0
    );
}
