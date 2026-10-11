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

//! Focused contracts using the real native interpreter and original account.
use super::super::execution_host::tests::compiled_package_host::{
    interpreter, select_one_producer,
};
use super::super::project_materialization::{
    CompiledProjectMetadataHost, ProjectMetadataHost, ProjectMetadataJournal, ProjectMetadataPhase,
};
use super::*;
use novarocks_execution::{
    exec::chunk::ChunkSchema,
    runtime::{
        fragment::ExecutionFailureCause,
        preparation_memory::{PreparationMemoryRefusal, PreparationMemoryStop},
        preparation_metadata::{
            CompiledSchemaMetadataScope, PreparationMetadataFailure, ProjectSchemaSite,
        },
        query_memory::QueryMemoryBinding,
    },
};
use novarocks_local_compiler::{ProjectMetadataScope, ProjectOutputRequestFacts};
use novarocks_local_program::{ProgramNodeId, ProgramNodeKind, StaticLayout};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_type_contract::{CompleteMetadataRequestFacts, MetadataRequestError};
use novarocks_types::{
    QueryId, SlotId,
    identity::{AttemptId, QueryExecutionId},
};
use novarocks_worker::{PreparationStop, TestPreparationControl};
use std::{cell::Cell, sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn binding(limit: u64) -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(128 << 20, 64 << 20, 64 << 20);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    account.install_policy(limit, novarocks_memory::LimitDimension::Work);
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 3), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}
fn compile(
    binding: &QueryMemoryBinding,
    preparation: &novarocks_worker::PreparationControlLoan<'_>,
    journal: &mut ProjectMetadataJournal,
) -> CompiledTaskProgram {
    let bytes = select_one_producer().0;
    let mut type_journal = super::super::TypeMaterializationJournal::default();
    let mut type_host = TypeMaterializationHost::new(binding, preparation, &mut type_journal);
    let mut project_host = ProjectMetadataHost::new(binding, preparation, journal);
    interpreter()
        .compile(
            &bytes,
            CompiledTaskOptions {
                pipeline_dop: NonZeroUsize::MIN,
                exchange_wait: Duration::from_millis(50),
            },
            &Control,
            &mut type_host,
            &mut project_host,
        )
        .unwrap()
}
fn project(program: &LocalProgram) -> ProgramNodeId {
    program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .find(|(_, node)| matches!(node.kind(), ProgramNodeKind::Project { .. }))
        .map(|(index, _)| ProgramNodeId::new(index))
        .expect("the real SELECT producer has a Project")
}

#[test]
fn project_metadata_real_native_compile_and_schema_share_one_binding() {
    let binding = binding(64 << 20);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let output = compile(&binding, &loan, &mut journal);
    assert!(matches!(
        journal.phase,
        Some(ProjectMetadataPhase::Compile(_))
    ));
    let facts = journal.facts.unwrap();
    assert_eq!(
        journal.workset_bytes,
        Some(
            facts.allocation_request_bytes_upper_bound
                + novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES
                    * facts.allocation_requests_upper_bound
        )
    );
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    let output = output.into_preparation();
    let program = output.program();
    let node = project(program);
    let layout = program.graph().nodes()[node.index()].output_layout();
    let expected = ChunkSchema::from_compiled_layout(layout).unwrap();
    let calls = Cell::new(0);
    let mut host = CompiledProjectMetadataHost::new(
        output.native_metadata_loan().unwrap(),
        &binding,
        &loan,
        &Control,
        &mut journal,
    );
    let actual = host
        .materialize(program, ProjectSchemaSite::Project(node), layout, || {
            calls.set(calls.get() + 1);
            ChunkSchema::from_compiled_layout(layout)
        })
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(actual.slot_ids(), expected.slot_ids());
    assert_eq!(actual.arrow_schema_ref(), expected.arrow_schema_ref());
    drop(host);
    assert_eq!(journal.phase, Some(ProjectMetadataPhase::Prepare(node)));
    assert!(journal.facts.is_some());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
}

#[test]
fn project_metadata_private_native_loan_rejects_wrong_program_and_layout_before_body() {
    let binding = binding(64 << 20);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let output = compile(&binding, &loan, &mut journal).into_preparation();
    let other = compile(&binding, &loan, &mut journal).into_preparation();
    let program = output.program();
    let node = project(program);
    let layout = program.graph().nodes()[node.index()].output_layout();
    let copied = layout.clone();
    let calls = Cell::new(0);
    let mut host = CompiledProjectMetadataHost::new(
        output.native_metadata_loan().unwrap(),
        &binding,
        &loan,
        &Control,
        &mut journal,
    );
    for (program, site, layout) in [
        (other.program(), ProjectSchemaSite::Project(node), layout),
        (program, ProjectSchemaSite::Project(node), &copied),
        (
            program,
            ProjectSchemaSite::Project(ProgramNodeId::new(usize::MAX)),
            layout,
        ),
    ] {
        let failure = host
            .materialize(program, site, layout, || {
                calls.set(calls.get() + 1);
                ChunkSchema::from_compiled_layout(layout)
            })
            .unwrap_err();
        assert!(matches!(
            failure.cause(),
            ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Request(
                MetadataRequestError::SourceModel(_)
            ))
        ));
    }
    assert_eq!(calls.get(), 0);
}

#[test]
fn project_metadata_explicit_direct_product_cannot_lend_native_provenance() {
    let binding = binding(64 << 20);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let output = compile(&binding, &loan, &mut journal);
    let native = match output.output {
        CompiledProgramOutput::Native(native) => native,
        CompiledProgramOutput::Direct(_) => {
            panic!("only the real interpreter issues this native output")
        }
    };
    let direct = CompiledTaskProgram::direct(native.program, output.runtime_filters);
    assert!(matches!(
        direct.program().graph().nodes()[project(direct.program()).index()].kind(),
        ProgramNodeKind::Project { .. }
    ));
    assert!(direct.into_preparation().native_metadata_loan().is_none());
}

#[test]
fn project_metadata_original_body_error_keeps_owned_bytes_through_the_real_host() {
    let binding = binding(64 << 20);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let output = compile(&binding, &loan, &mut journal).into_preparation();
    let program = output.program();
    let node = project(program);
    let layout = program.graph().nodes()[node.index()].output_layout();
    // Obtain the actual old diagnostic outside this transport-only test body.
    // This is not proof of an invalid Map passing native FE validation or of
    // that different Map constructor's resource envelope.
    let malformed = StaticLayout::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new(
                "map",
                arrow::datatypes::DataType::Map(
                    Arc::new(arrow::datatypes::Field::new(
                        "entries",
                        arrow::datatypes::DataType::Int64,
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ])),
        Arc::from([SlotId::new(1)]),
    )
    .unwrap();
    let original = ChunkSchema::from_compiled_layout(&malformed).unwrap_err();
    let pointer = original.as_ptr();
    let mut host = CompiledProjectMetadataHost::new(
        output.native_metadata_loan().unwrap(),
        &binding,
        &loan,
        &Control,
        &mut journal,
    );
    let failure = host
        .materialize(program, ProjectSchemaSite::Project(node), layout, || {
            Err(original)
        })
        .unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Pipeline(_)
    ));
    assert_eq!(failure.detail(), "map entries is not struct: Int64");
    assert_eq!(failure.detail().as_ptr(), pointer);
    drop(host);
    assert!(journal.body.settlement.is_some());
}

#[test]
fn project_metadata_original_worker_stop_refuses_compile_body_with_exact_cause() {
    use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
    for stop in [
        PreparationStop::Cancel(CancelReason::UpstreamNoLongerNeeded),
        PreparationStop::Abort(AbortCause::QueryFailed),
        PreparationStop::Abort(AbortCause::LeaseExpired),
        PreparationStop::Abort(AbortCause::PeerTaskFailed),
    ] {
        let binding = binding(64 << 20);
        let owner = TestPreparationControl::new(Duration::from_millis(1));
        assert!(owner.stop(stop));
        let loan = owner.loan();
        let mut journal = ProjectMetadataJournal::default();
        let mut host = ProjectMetadataHost::new(&binding, &loan, &mut journal);
        let calls = Cell::new(0);
        let facts = ProjectOutputRequestFacts {
            node: novarocks_physical_plan::NodeId::new(7),
            requests: CompleteMetadataRequestFacts {
                allocation_requests_upper_bound: 1,
                allocation_request_bytes_upper_bound: 1024,
            },
        };
        let failure = host
            .materialize(&facts, || {
                calls.set(calls.get() + 1);
                panic!("stopped metadata must not execute")
            })
            .err()
            .unwrap();
        let expected = match stop {
            PreparationStop::Cancel(cause) => PreparationMemoryStop::Cancel(cause),
            PreparationStop::Abort(cause) => PreparationMemoryStop::Abort(cause),
        };
        assert!(
            matches!(failure, novarocks_local_compiler::ProjectMetadataFailure::Host(PreparationMemoryRefusal::Stopped(cause)) if cause == expected)
        );
        assert_eq!(calls.get(), 0);
        drop(host);
        assert!(journal.body.settlement.is_none());
    }
}

#[test]
fn project_metadata_real_capacity_and_arithmetic_refuse_before_compile_body() {
    let binding = binding(1024);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    for requests in [
        CompleteMetadataRequestFacts {
            allocation_requests_upper_bound: 1,
            allocation_request_bytes_upper_bound: 1 << 20,
        },
        CompleteMetadataRequestFacts {
            allocation_requests_upper_bound: usize::MAX,
            allocation_request_bytes_upper_bound: 1,
        },
    ] {
        let mut journal = ProjectMetadataJournal::default();
        let mut host = ProjectMetadataHost::new(&binding, &loan, &mut journal);
        let calls = Cell::new(0);
        let error = host
            .materialize(
                &ProjectOutputRequestFacts {
                    node: novarocks_physical_plan::NodeId::new(7),
                    requests,
                },
                || {
                    calls.set(calls.get() + 1);
                    panic!("refused metadata must not execute")
                },
            )
            .err()
            .unwrap();
        assert!(matches!(
            error,
            novarocks_local_compiler::ProjectMetadataFailure::Host(
                PreparationMemoryRefusal::Capacity(_)
            )
        ));
        assert_eq!(calls.get(), 0);
        drop(host);
        assert!(journal.capacity.is_some());
        assert!(journal.body.settlement.is_none());
    }
}

struct BoundedFirstProjectQualification<'a> {
    host: ProjectMetadataHost<'a>,
    occupied: Option<novarocks_memory::FundingDomain>,
    admissions: usize,
    calls: &'a Cell<usize>,
}
impl ProjectMetadataScope for BoundedFirstProjectQualification<'_> {
    type HostError = PreparationMemoryRefusal;
    fn materialize<B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<
        novarocks_local_compiler::ProjectMetadataOutput,
        novarocks_local_compiler::ProjectMetadataFailure<Self::HostError>,
    >
    where
        B: FnOnce() -> Result<
            novarocks_local_compiler::ProjectMetadataOutput,
            novarocks_local_compiler::FragmentCompileError,
        >,
    {
        use novarocks_execution::runtime::kernel_memory::{
            KernelMemoryAdmission, KernelMemoryRequest, request, request_complete_operation,
        };
        let occupied = &mut self.occupied;
        let admissions = &mut self.admissions;
        let calls = self.calls;
        self.host.materialize_with_request(facts, || { calls.set(calls.get() + 1); body() }, |binding, peak| {
            *admissions += 1;
            if let Some(domain) = occupied.take() {
                let outcome = request(Some(binding), KernelMemoryRequest { workset_bytes: peak as u64, stock_bytes: peak as u64, threshold_bytes: 0, maintenance_budget: 0 });
                assert!(matches!(&outcome, KernelMemoryAdmission::SettlementPending(receipt) if !receipt.complete));
                assert_eq!(calls.get(), 0);
                // This real domain has no active writer during the original wait.
                domain.stop_producing().unwrap();
                outcome
            } else { request_complete_operation(Some(binding), peak) }
        })
    }
}
#[test]
fn project_metadata_real_pending_requalifies_one_original_compiler_body() {
    const TARGET: u64 = 2 << 20;
    let mut cfg = AuthorityConfig::new(TARGET * 2, TARGET, TARGET);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let other = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let binding = QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 4), AttemptId::new(1).unwrap()).unwrap(),
        Arc::clone(&authority),
        account,
    )
    .unwrap();
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut type_journal = super::super::TypeMaterializationJournal::default();
    let mut type_host = TypeMaterializationHost::new(&binding, &loan, &mut type_journal);
    let interpreter = interpreter();
    let package = decode_fragment_package_with_type_host(
        &select_one_producer().0,
        &interpreter.model,
        &interpreter.decode_limits,
        &Control,
        &mut type_host,
    )
    .unwrap();
    let validated =
        validate_fragment_providers(Arc::new(package), &interpreter.providers, &Control).unwrap();
    let occupied = other
        .create_domain(TARGET - authority.root().committed_bytes() - 1024)
        .unwrap();
    let calls = Cell::new(0);
    let mut journal = ProjectMetadataJournal::default();
    let mut host = BoundedFirstProjectQualification {
        host: ProjectMetadataHost::new(&binding, &loan, &mut journal),
        occupied: Some(occupied),
        admissions: 0,
        calls: &calls,
    };
    let program = compile_fragment_with_project_metadata_host(
        validated,
        &interpreter.functions,
        LocalCompileOptions {
            pipeline_dop: NonZeroUsize::MIN,
            root_sink_dop: Some(NonZeroUsize::MIN),
            kernel_abi: KernelAbiVersion::CURRENT,
            constants: interpreter.constants,
            exchange_wait: Duration::from_millis(50),
        },
        &Control,
        &mut host,
    )
    .unwrap();
    let projects = program
        .graph()
        .nodes()
        .iter()
        .filter(|node| matches!(node.kind(), ProgramNodeKind::Project { .. }))
        .count();
    assert!(projects > 0);
    assert_eq!(calls.get(), projects);
    assert_eq!(host.admissions, projects + 1);
    drop(host);
    assert!(!journal.pending.unwrap().complete);
    assert!(journal.capacity.is_none() && journal.shortage.is_none());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
}
