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

//! Runs the actual private FE record writer with actual SQL-owned Physical binding.
use crate::query_execution::dependency_artifact_records as records;
use crate::query_execution::dependency_artifact_storage;
use dependency_artifact_storage::{CaptureOutputDescriptor, SqlDependencyArtifactFactory};
use novarocks_plan_codec::{
    host_projection_v2::AdmissionRefusal,
    physical_binding_v2::{
        ArgumentTypeIds, BindingProjectionLimits, BindingSource, FunctionBindingInput,
        ResultTypeIds, encode_function_bindings_with_host_in,
    },
    physical_type_v2::{EncodedTypeTable, TypeProjectionLimits, encode_type_table_sources},
};
use novarocks_query_application::session_control::{SessionToken, StatementToken};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_workload_control::{
    LocalResourceAuthority, ResourceConfig, WorkClass, WorkError, WorkRequest, WorkloadConfig,
    WorkloadControl,
};
use prost::Message;
use records::{BindingRecordError, OriginalBindingRecordWriter};
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt;
use std::{
    convert::Infallible,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let p =
            std::env::temp_dir().join(format!("novarocks-binding-record-{}", uuid::Uuid::new_v4()));
        let mut b = std::fs::DirBuilder::new();
        #[cfg(unix)]
        b.mode(0o700);
        b.create(&p).unwrap();
        Self(p)
    }
    fn descriptor(&self, max: u64) -> CaptureOutputDescriptor {
        CaptureOutputDescriptor {
            version: 1,
            run_namespace: "actual".into(),
            output_root: self.0.clone(),
            max_statement_bytes: max,
        }
    }
    fn file(&self) -> std::path::PathBuf {
        self.0.join(format!("actual-{}-13.cap", (7u64 << 32) | 11))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
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
struct Control {
    resources: LocalResourceAuthority,
    used: AtomicU64,
    calls: Mutex<usize>,
    refuse: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(
        resources: LocalResourceAuthority,
        refuse: Option<(usize, CompileControlError)>,
    ) -> Self {
        Self {
            resources,
            used: AtomicU64::new(0),
            calls: Mutex::new(0),
            refuse,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        self.used
            .fetch_max(self.resources.snapshot().data_used_bytes, Ordering::Relaxed);
        let mut c = self.calls.lock().unwrap();
        *c += 1;
        if let Some((at, cause)) = self.refuse.clone()
            && *c == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn source<T>(f: impl FnOnce(&EncodedTypeTable<'_>, &[FunctionBindingInput<'_>]) -> T) -> T {
    let original = super::exact("SELECT upper('kept') AS original_call");
    let binding = original
        .plan()
        .fragments()
        .values()
        .find_map(|v| {
            v.expressions().iter().find_map(|(_, e)| match &e.kind {
                novarocks_physical_plan::ExprKind::FunctionCall { function, .. } => Some(function),
                _ => None,
            })
        })
        .unwrap();
    let mut values = Vec::new();
    let mut ids = Vec::new();
    for v in &binding.argument_types {
        let novarocks_type_contract::FunctionArgumentType::Value(v) = v else {
            panic!("actual UPPER value source");
        };
        let id = u32::try_from(values.len() + 100).unwrap();
        values.push((id, v.clone()));
        ids.push(ArgumentTypeIds::Value(id));
    }
    let result = u32::try_from(values.len() + 100).unwrap();
    values.push((result, binding.result_type.clone()));
    let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
    let types = encode_type_table_sources(
        &values,
        &[],
        TypeProjectionLimits {
            max_definitions: 64,
            max_expanded_nodes: 128,
            max_string_bytes: 65536,
        },
        &control,
    )
    .unwrap();
    let inputs = [FunctionBindingInput {
        id: 17,
        source: BindingSource::Scalar(binding),
        arguments: &ids,
        result: ResultTypeIds::Scalar(result),
    }];
    f(&types, &inputs)
}
fn limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 64,
        max_type_references: 128,
        max_request_bytes: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    }
}
#[test]
fn sql_dependency_original_binding_record_real_grant_charge_prost_and_drop() {
    source(|types, inputs| {
        let dir = Directory::new();
        let owner = owner();
        let resources = owner.resources().expect("test workload resource authority");
        let task = owner
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let factory =
            SqlDependencyArtifactFactory::try_new(dir.descriptor(1 << 20), resources.clone())
                .unwrap();
        let artifact = factory
            .begin(
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (7_u64 << 32) | 11_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    13,
                ),
                &[42; 32],
                &task.owner.scope(),
            )
            .unwrap();
        let mut writer = OriginalBindingRecordWriter::new(artifact);
        let mut stock = writer.projection_stock();
        let control = Control::new(resources.clone(), None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let encoded = encode_function_bindings_with_host_in(
            types,
            inputs,
            64 * 1024,
            limits(),
            &mut |facts| stock.admit(facts).map_err(AdmissionRefusal::Host),
            &mut work,
        )
        .unwrap();
        let dto = encoded.owned_backing_bytes_observed(&mut work).unwrap();
        let expected = encoded.as_wire()[0].encode_to_vec();
        let grant = resources.snapshot().data_reserved_bytes;
        assert!(grant >= dto as u64);
        assert_eq!(resources.snapshot().data_used_bytes, 0);
        writer
            .write_bindings(encoded, &mut stock, 2, 91, &mut work)
            .unwrap();
        assert!(control.used.load(Ordering::Relaxed) >= dto as u64);
        assert_eq!(resources.snapshot().data_used_bytes, 0);
        assert!(resources.snapshot().peak_held_bytes >= grant + expected.len() as u64 + 17);
        drop(stock);
        assert_eq!(resources.snapshot().held_bytes(), 0);
        writer.finish_storage().unwrap();
        let raw = std::fs::read(dir.file()).unwrap();
        assert_eq!(&raw[..8], b"NRDEPS02");
        assert_eq!(raw[64], 2);
        assert_eq!(u64::from_le_bytes(raw[65..73].try_into().unwrap()), 91);
        assert_eq!(
            u64::from_le_bytes(raw[73..81].try_into().unwrap()),
            expected.len() as u64
        );
        assert_eq!(&raw[81..81 + expected.len()], expected);
        assert_eq!(&raw[81 + expected.len()..], b"NRFLUSH1");
        assert_eq!(writer.bytes_written(), raw.len() as u64);
    })
}
#[test]
fn sql_dependency_original_binding_record_missing_grant_is_not_late_permission_and_latches() {
    source(|types, inputs| {
        let dir = Directory::new();
        let owner = owner();
        let resources = owner.resources().expect("test workload resource authority");
        let task = owner
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let factory =
            SqlDependencyArtifactFactory::try_new(dir.descriptor(1 << 20), resources.clone())
                .unwrap();
        let artifact = factory
            .begin(
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (7_u64 << 32) | 11_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    13,
                ),
                &[42; 32],
                &task.owner.scope(),
            )
            .unwrap();
        let mut writer = OriginalBindingRecordWriter::new(artifact);
        let mut stock = writer.projection_stock();
        let control = Control::new(resources.clone(), None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let encoded = encode_function_bindings_with_host_in(
            types,
            inputs,
            64 * 1024,
            limits(),
            &mut |_| -> Result<(), AdmissionRefusal<Infallible>> { Ok(()) },
            &mut work,
        )
        .unwrap();
        let result = writer.write_bindings(encoded, &mut stock, 2, 91, &mut work);
        assert!(
            matches!(result,Err(BindingRecordError::BackingExceedsGrant{actual,granted:0}) if actual>0)
        );
        assert!(writer.finish_storage().is_err());
        assert_eq!(std::fs::read(dir.file()).unwrap().len(), 64);
        assert_eq!(resources.snapshot().held_bytes(), 0);
        let encoded = encode_function_bindings_with_host_in(
            types,
            inputs,
            64 * 1024,
            limits(),
            &mut |_| -> Result<(), AdmissionRefusal<Infallible>> { Ok(()) },
            &mut work,
        )
        .unwrap();
        let before = *control.calls.lock().unwrap();
        assert!(matches!(
            writer.write_bindings(encoded, &mut stock, 2, 91, &mut work),
            Err(BindingRecordError::Closed)
        ));
        assert_eq!(*control.calls.lock().unwrap(), before);
    })
}
#[test]
fn sql_dependency_original_binding_record_output_failure_releases_dto_before_stock_and_no_ack() {
    source(|types, inputs| {
        let dir = Directory::new();
        let owner = owner();
        let resources = owner.resources().expect("test workload resource authority");
        let task = owner
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let factory =
            SqlDependencyArtifactFactory::try_new(dir.descriptor(64), resources.clone()).unwrap();
        let artifact = factory
            .begin(
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (7_u64 << 32) | 11_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    13,
                ),
                &[42; 32],
                &task.owner.scope(),
            )
            .unwrap();
        let mut writer = OriginalBindingRecordWriter::new(artifact);
        let mut stock = writer.projection_stock();
        let control = Control::new(resources.clone(), None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let encoded = encode_function_bindings_with_host_in(
            types,
            inputs,
            64 * 1024,
            limits(),
            &mut |facts| stock.admit(facts).map_err(AdmissionRefusal::Host),
            &mut work,
        )
        .unwrap();
        let error = writer
            .write_bindings(encoded, &mut stock, 2, 91, &mut work)
            .unwrap_err();
        assert!(matches!(
            error,
            BindingRecordError::Storage(dependency_artifact_storage::CaptureRecordError::Storage(
                dependency_artifact_storage::CaptureStorageError::StatementLimit { .. }
            ))
        ));
        assert_eq!(resources.snapshot().data_used_bytes, 0);
        drop(stock);
        assert_eq!(resources.snapshot().held_bytes(), 0);
        assert!(writer.finish_storage().is_err());
        assert_eq!(std::fs::read(dir.file()).unwrap().len(), 64);
    })
}
#[test]
fn sql_dependency_original_binding_record_foreign_stock_keeps_actual_authority_cause() {
    source(|types, inputs| {
        let dir = Directory::new();
        let a = owner();
        let b = owner();
        let ar = a.resources().expect("test workload resource authority");
        let br = b.resources().expect("test workload resource authority");
        let ta = a
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let tb = b
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let fa =
            SqlDependencyArtifactFactory::try_new(dir.descriptor(1 << 20), ar.clone()).unwrap();
        let mut other_descriptor = dir.descriptor(1 << 20);
        other_descriptor.run_namespace = "other".into();
        let fb = SqlDependencyArtifactFactory::try_new(other_descriptor, br.clone()).unwrap();
        let mut writer = OriginalBindingRecordWriter::new(
            fa.begin(
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (7_u64 << 32) | 11_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    13,
                ),
                &[42; 32],
                &ta.owner.scope(),
            )
            .unwrap(),
        );
        let other = OriginalBindingRecordWriter::new(
            fb.begin(
                StatementToken::new(
                    SessionToken::new(
                        novarocks_query_application::session_control::ClientSessionKey::new(
                            (7_u64 << 32) | 11_u64,
                        )
                        .expect("nonzero fixture session"),
                    ),
                    13,
                ),
                &[42; 32],
                &tb.owner.scope(),
            )
            .unwrap(),
        );
        let mut stock = other.projection_stock();
        let control = Control::new(br.clone(), None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let encoded = encode_function_bindings_with_host_in(
            types,
            inputs,
            64 * 1024,
            limits(),
            &mut |facts| stock.admit(facts).map_err(AdmissionRefusal::Host),
            &mut work,
        )
        .unwrap();
        let error = writer
            .write_bindings(encoded, &mut stock, 2, 91, &mut work)
            .unwrap_err();
        // The original authority validates its scope pointer before any DTO charge.
        assert!(matches!(
            error,
            BindingRecordError::Work(WorkError::ForeignAuthority)
        ));
        assert_eq!(ar.snapshot().data_used_bytes, 0);
        assert_eq!(br.snapshot().data_used_bytes, 0);
        drop(stock);
        assert_eq!(ar.snapshot().held_bytes(), 0);
        assert_eq!(br.snapshot().held_bytes(), 0);
    })
}

#[test]
fn sql_dependency_original_binding_record_every_control_refusal_has_no_footer_or_storage_ack() {
    source(|types, inputs| {
        let mut count = 0;
        for cause in [
            None,
            Some(CompileControlError::Cancelled),
            Some(CompileControlError::DeadlineExceeded),
            Some(CompileControlError::ResourceExhausted),
        ] {
            let range = if cause.is_none() { 1..=1 } else { 1..=count };
            for at in range {
                let dir = Directory::new();
                let owner = owner();
                let resources = owner.resources().expect("test workload resource authority");
                let task = owner
                    .try_begin_root(WorkRequest::new(WorkClass::Query))
                    .unwrap();
                let factory = SqlDependencyArtifactFactory::try_new(
                    dir.descriptor(1 << 20),
                    resources.clone(),
                )
                .unwrap();
                let artifact = factory
                    .begin(
                        StatementToken::new(SessionToken::new(novarocks_query_application::session_control::ClientSessionKey::new((7_u64 << 32) | 11_u64).expect("nonzero fixture session")), 13),
                        &[42; 32],
                        &task.owner.scope(),
                    )
                    .unwrap();
                let mut writer = OriginalBindingRecordWriter::new(artifact);
                let mut stock = writer.projection_stock();
                let source_control = novarocks_sql::compiler::SqlCompileControl::unbounded();
                let mut source_work =
                    CompileCheckpoints::try_new(&source_control, CompilePhase::Encode).unwrap();
                let encoded = encode_function_bindings_with_host_in(
                    types,
                    inputs,
                    64 * 1024,
                    limits(),
                    &mut |facts| stock.admit(facts).map_err(AdmissionRefusal::Host),
                    &mut source_work,
                )
                .unwrap();
                let control = Control::new(
                    resources.clone(),
                    cause.clone().map(|cause| (at + 1, cause)),
                );
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
                let result = writer.write_bindings(encoded, &mut stock, 2, 91, &mut work);
                match cause.clone() {
                    None => {
                        result.unwrap();
                        count = *control.calls.lock().unwrap() - 1;
                        assert!(count > 0);
                    }
                    Some(expected) => {
                        assert!(
                            matches!(result,Err(BindingRecordError::Codec(novarocks_plan_codec::physical_binding_v2::BindingCodecError::Control(found))) if found==expected)
                        );
                        assert_eq!(*control.calls.lock().unwrap(), at + 1);
                        assert!(writer.finish_storage().is_err());
                        assert!(!std::fs::read(dir.file()).unwrap().ends_with(b"NRFLUSH1"));
                    }
                }
                assert_eq!(resources.snapshot().data_used_bytes, 0);
                drop(stock);
                assert_eq!(resources.snapshot().held_bytes(), 0);
            }
        }
    })
}
