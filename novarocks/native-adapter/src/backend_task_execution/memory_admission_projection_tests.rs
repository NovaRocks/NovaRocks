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

//! Exact real admission refusal projected at the existing task-resource boundary.
use super::memory_admission_failure_to_host;
use crate::native_query_context::{NativeQueryContextRuntime, NativeTaskAdmissionError};
use novarocks_execution::runtime::query_context::QueryMemoryAccountError;
use novarocks_execution_contract::{TaskFailureCategory, TaskIdentity};
use novarocks_memory::{AuthorityConfig, CapacityError, MemoryAuthority};
use novarocks_types::identity::{
    AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
};
use std::sync::Arc;

#[test]
fn mem_a1_v2_actual_none_metadata_refusal_uses_existing_task_resource_category() {
    let mut config = AuthorityConfig::new(1 << 30, 1 << 29, 1 << 29);
    config.max_accounts = 1;
    let authority = Arc::new(MemoryAuthority::new(config).unwrap());
    let runtime = NativeQueryContextRuntime::new(authority);
    let execution =
        QueryExecutionId::new(QueryId::new(97401, 97402), AttemptId::new(1).unwrap()).unwrap();
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    let error = runtime
        .establish_query_resources(execution, None)
        .err()
        .unwrap();
    assert!(matches!(
        &error,
        NativeTaskAdmissionError::MemoryAccount(QueryMemoryAccountError::Capacity(
            CapacityError::MetadataExhausted { .. }
        ))
    ));

    let expected = novarocks_execution_contract::failure::FailureProjection::failure_fact(&error);
    let projected = memory_admission_failure_to_host(identity, error);
    assert_eq!(
        novarocks_execution_contract::failure::FailureProjection::failure_fact(&projected),
        expected
    );
    assert_eq!(projected.category(), TaskFailureCategory::ResourceExhausted);
    assert!(projected.detail().as_str().len() <= 512);
    assert!(std::error::Error::source(&projected).is_some());
}

#[test]
fn read_schema_construction_failure_keeps_resource_and_local_model_categories() {
    use super::{CompiledPackageError, compiled_package_rejection};
    use novarocks_plan_codec::physical_package_v2::PackageDecodeError;
    use novarocks_plan_codec::physical_public_source_schema_v2::PublicSourceSchemaCodecError;
    use novarocks_plan_codec::physical_read_scan_v2::ReadScanCodecError;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    let execution =
        QueryExecutionId::new(QueryId::new(97411, 97412), AttemptId::new(1).unwrap()).unwrap();
    let identity = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    // The real checked Layout author refuses this array before allocation.
    let arithmetic = std::alloc::Layout::array::<u64>(usize::MAX)
        .map_err(|_| LayoutResourceError::Arithmetic)
        .unwrap_err();
    for (scan, category) in [
        (
            ReadScanCodecError::ConstructionResource(arithmetic),
            TaskFailureCategory::ResourceExhausted,
        ),
        (
            ReadScanCodecError::InvalidSourceSchema(PublicSourceSchemaCodecError::Layout(
                arithmetic,
            )),
            TaskFailureCategory::ResourceExhausted,
        ),
        (
            ReadScanCodecError::ConstructionResource(LayoutResourceError::SourceModel),
            TaskFailureCategory::Internal,
        ),
        (
            ReadScanCodecError::InvalidSourceSchema(PublicSourceSchemaCodecError::SourceModel(
                "locked source model is unavailable",
            )),
            TaskFailureCategory::Internal,
        ),
    ] {
        let cause = CompiledPackageError::Decode(PackageDecodeError::Scan(scan));
        use novarocks_execution_contract::failure::FailureProjection;
        let expected = cause.failure_fact();
        let projected = compiled_package_rejection(identity, cause);
        assert_eq!(projected.category(), category);
        assert_eq!(projected.failure_fact(), expected);
        let original = crate::backend_test_support::find_source::<ReadScanCodecError>(&projected)
            .expect("actual scan codec source, not a reconstructed summary");
        assert_eq!(original.failure_fact(), expected);
        assert!(projected.detail().as_str().len() <= 512);
    }
}

#[test]
fn actual_schema_tracker_refusal_is_projected_as_resource_without_text_inference() {
    use novarocks_execution::exec::pipeline::operator::input_construction::{
        ConstructionFailure, InputConstructionRequestHost, runtime::TrackedInputConstructionHost,
    };
    use novarocks_execution::runtime::{mem_tracker::MemTracker, task::ExecutionFailure};
    let tracker = MemTracker::new_root("typed schema limit");
    tracker.install_limit_once(1).unwrap();
    let host = TrackedInputConstructionHost::new(tracker.clone());
    let cause = host
        .request(std::alloc::Layout::array::<u64>(2).unwrap())
        .unwrap_err();
    assert!(matches!(&cause, ConstructionFailure::Memory(_)));
    assert_eq!(tracker.current(), 0, "unconstructed request is balanced");
    let original = cause.to_string();
    let failure = ExecutionFailure::from(cause);
    assert_eq!(failure.to_string(), original);
    assert_eq!(
        super::failure_category(failure.cause()),
        TaskFailureCategory::ResourceExhausted
    );
}

#[test]
fn compiled_capability_refusal_does_not_reclassify_content_or_control_failures() {
    use super::{CompiledPackageError, compiled_package_rejection};
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection, domain};
    use novarocks_local_compiler::FragmentCompileError;
    use novarocks_physical_plan::NodeId;
    use novarocks_plan_codec::physical_package_v2::PackageDecodeError;
    use novarocks_type_contract::CompileControlError;
    let identity = TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(97421, 97422), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    );
    for (error, expected) in [
        (
            CompiledPackageError::Compile(FragmentCompileError::Unsupported {
                node: Some(NodeId::new(7)),
                feature: "FULL OUTER hash join",
            }),
            TaskFailureCategory::Unsupported,
        ),
        (
            CompiledPackageError::Decode(PackageDecodeError::Invalid(
                "unsupported diagnostic from malformed package",
            )),
            TaskFailureCategory::Execution,
        ),
        (
            CompiledPackageError::Control(CompileControlError::ResourceExhausted),
            TaskFailureCategory::ResourceExhausted,
        ),
        (
            CompiledPackageError::Control(CompileControlError::Cancelled),
            TaskFailureCategory::Execution,
        ),
        (
            CompiledPackageError::Control(CompileControlError::DeadlineExceeded),
            TaskFailureCategory::Execution,
        ),
    ] {
        let expected_fact = error.failure_fact();
        let projected = compiled_package_rejection(identity, error);
        assert_eq!(projected.category(), expected);
        assert_eq!(projected.failure_fact(), expected_fact);
        assert!(projected.detail().as_str().len() <= 512);
        let original = crate::backend_test_support::find_source::<CompiledPackageError>(&projected)
            .expect("the whole original compiled package cause remains available");
        assert_eq!(original.failure_fact(), expected_fact);
        match original {
            CompiledPackageError::Compile(_) => {
                assert_eq!(expected_fact.class(), FailureClass::Unsupported);
                assert_eq!(
                    expected_fact.reason().domain(),
                    domain::LOCAL_COMPILER.get()
                );
                assert_eq!(expected_fact.reason().code(), 1);
                assert!(matches!(
                    crate::backend_test_support::find_source::<FragmentCompileError>(&projected),
                    Some(FragmentCompileError::Unsupported {
                        node: Some(node),
                        feature: "FULL OUTER hash join",
                    }) if *node == NodeId::new(7)
                ));
            }
            CompiledPackageError::Decode(_) => {
                assert_eq!(expected_fact.class(), FailureClass::Data);
                assert_eq!(expected_fact.reason().domain(), domain::PLAN_CODEC.get());
                assert_eq!(expected_fact.reason().code(), 38);
                assert!(matches!(
                    crate::backend_test_support::find_source::<PackageDecodeError>(&projected),
                    Some(PackageDecodeError::Invalid(
                        "unsupported diagnostic from malformed package",
                    ))
                ));
            }
            CompiledPackageError::Control(cause) => {
                assert_eq!(expected_fact, cause.failure_fact());
                assert_eq!(
                    crate::backend_test_support::find_source::<CompileControlError>(&projected),
                    Some(cause),
                );
            }
            _ => panic!("the test's exact original author must not be replaced"),
        }
    }
}
