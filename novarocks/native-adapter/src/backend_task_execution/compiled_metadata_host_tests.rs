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
use crate::backend_task_execution::tests::node_fixture::{NativeNodePreparation, PreparationStop};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::{
    exec::chunk::ChunkSchema,
    runtime::{
        fragment::ExecutionFailureCause,
        preparation_memory::PreparationMemoryRefusal,
        preparation_metadata::{
            CompiledSchemaMetadataScope, CompiledSchemaSite, PreparationMetadataFailure,
        },
        query_memory::QueryMemoryBinding,
    },
};
use novarocks_local_compiler::{
    FragmentCompileError, ProjectMetadataScope, ProjectOutputRequestFacts,
};
use novarocks_local_program::{ProgramNodeId, ProgramNodeKind};
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_plan_codec::physical_package_v2::decode_fragment_package_with_type_host;
use novarocks_type_contract::{CompleteMetadataRequestFacts, MetadataRequestError};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{cell::Cell, sync::Arc, time::Duration};

fn preparation_identity(
    binding: &QueryMemoryBinding,
) -> novarocks_execution_contract::TaskIdentity {
    use novarocks_types::identity::{BackendProcessId, StageId, TaskId};
    novarocks_execution_contract::TaskIdentity::new(
        binding.execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

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
    compile_native_package_with_funding_for_test(
        &interpreter(),
        &bytes,
        CompiledTaskOptions {
            pipeline_dop: NonZeroUsize::MIN,
        },
        MemTracker::new_root("native funded compile fixture"),
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
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
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
    let node = project(output.program());
    let prepared = super::super::prepare_project_metadata_for_test(
        output,
        CompiledSchemaSite::Project(node),
        &binding,
        MemTracker::new_root("actual prepared Project fixture"),
        &loan,
        &Control,
        &mut journal,
    )
    .unwrap();
    let layout = prepared.program.graph().nodes()[node.index()].output_layout();
    assert_eq!(prepared.schema.slot_ids(), layout.slots());
    assert_eq!(
        prepared.schema.arrow_schema_ref().as_ref(),
        layout.schema().as_ref()
    );
    assert_eq!(journal.phase, Some(ProjectMetadataPhase::Prepare(node)));
    assert!(journal.facts.is_some());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
}

#[test]
fn project_metadata_private_native_loan_rejects_wrong_program_and_layout_before_body() {
    let binding = binding(64 << 20);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
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
        (other.program(), CompiledSchemaSite::Project(node), layout),
        (program, CompiledSchemaSite::Project(node), &copied),
        (
            program,
            CompiledSchemaSite::Project(ProgramNodeId::new(usize::MAX)),
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
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
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

/// Exercise a real prepared operator's creator refusal inside the funded body.
/// A malformed Map is rejected by main's declaration validator before this
/// boundary, so it cannot serve as the body-error transport oracle anymore.
fn assert_actual_schema_body_error_keeps_owned_bytes(
    compiled: CompiledTaskProgram,
    site: CompiledSchemaSite,
    binding: &QueryMemoryBinding,
    loan: &novarocks_worker::PreparationControlLoan<'_>,
    journal: &mut ProjectMetadataJournal,
) {
    use novarocks_execution::{
        exec::pipeline::{operator::ConstructionFailure, operator_factory::OperatorFactory},
        runtime::{
            preparation_metadata::CompiledSchemaPreparationScope, runtime_state::RuntimeErrorState,
        },
    };
    let prepared = compiled.into_preparation();
    let program = prepared.program();
    let node = match site {
        CompiledSchemaSite::Project(node) | CompiledSchemaSite::AnalyticOutput(node) => node,
        CompiledSchemaSite::FinalResult => {
            panic!("this fixture exercises a real Project or Analytic")
        }
    };
    let layout = program.graph().nodes()[node.index()].output_layout();
    let error_state = Arc::new(RuntimeErrorState::default());
    let mut operator = match site {
        CompiledSchemaSite::Project(node) => {
            novarocks_execution::exec::operators::prepare_project_factory_for_test(
                Arc::clone(program),
                Some(node),
                Arc::clone(&error_state),
            )
            .unwrap()
            .create(1, 0)
        }
        CompiledSchemaSite::AnalyticOutput(node) => {
            novarocks_execution::exec::operators::prepare_window_factory_for_test(
                Arc::clone(program),
                node,
                Arc::clone(&error_state),
            )
            .unwrap()
            .create(1, 0)
        }
        CompiledSchemaSite::FinalResult => unreachable!(),
    };
    let tracker = MemTracker::new_root("funded schema body creator refusal");
    let construction = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        Arc::clone(&tracker), error_state,
    ).unwrap();
    let existing = tracker.current();
    assert!(
        existing > 0,
        "the actual constructor adapters were created successfully"
    );
    // Freeze the real positive limit after adapter creation. The next actual
    // schema-scope request must refuse inside Operator.prepare, not in setup.
    tracker.install_limit_once(existing).unwrap();
    operator.set_construction_factory(construction);
    let calls = Cell::new(0);
    let allocated_before = tracker.allocated();
    let original_pointer = Cell::new(std::ptr::null::<u8>());
    let mut host = CompiledProjectMetadataHost::new(
        prepared.native_metadata_loan().unwrap(),
        binding,
        loan,
        &Control,
        journal,
    );
    let failure = host
        .prepare(program, site, layout, &mut || {
            calls.set(calls.get() + 1);
            let original = operator
                .prepare()
                .expect_err("the actual schema creator must cross its installed limit");
            assert!(matches!(
                original.cause(),
                ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
            ));
            original_pointer.set(original.detail().as_ptr());
            Err(original)
        })
        .unwrap_err();
    assert_eq!(calls.get(), 1);
    let ExecutionFailureCause::Construction(ConstructionFailure::Memory(original)) =
        failure.cause()
    else {
        panic!("the original typed creator cause was replaced: {failure:?}");
    };
    assert_eq!(failure.detail().as_ptr(), original_pointer.get());
    assert_eq!(original.detail().as_ptr(), original_pointer.get());
    assert_eq!(
        failure.detail(),
        format!(
            "ResourceExhausted: memory limit exceeded for tracker {}: current {} bytes, limit {} bytes",
            tracker.label(),
            existing + (tracker.allocated() - allocated_before),
            existing,
        )
    );
    assert!(tracker.peak() > existing);
    assert_eq!(
        tracker.current(),
        existing,
        "the unconstructed request was rolled back"
    );
    assert!(
        operator.prepared_schema_for_test().is_none(),
        "failed preparation published no schema"
    );
    {
        let _host = host;
    }
    assert_eq!(journal.phase, Some(ProjectMetadataPhase::Prepare(node)));
    assert!(journal.facts.is_some());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    drop(operator);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn project_metadata_original_body_error_keeps_owned_bytes_through_the_real_host() {
    let binding = binding(64 << 20);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let compiled = compile(&binding, &loan, &mut journal);
    let node = project(compiled.program());
    assert_actual_schema_body_error_keeps_owned_bytes(
        compiled,
        CompiledSchemaSite::Project(node),
        &binding,
        &loan,
        &mut journal,
    );
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
        let owner = NativeNodePreparation::new(preparation_identity(&binding));
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
            .materialize::<FragmentCompileError, _>(&facts, || {
                calls.set(calls.get() + 1);
                panic!("stopped metadata must not execute")
            })
            .err()
            .unwrap();
        let expected = loan.checkpoint().unwrap_err();
        assert!(
            matches!(failure, novarocks_local_compiler::ProjectMetadataFailure::Host(PreparationMemoryRefusal::Interrupted(cause)) if std::ptr::eq(cause.cause(), expected.cause()))
        );
        assert_eq!(calls.get(), 0);
        {
            let _host = host;
        }
        assert!(journal.body.settlement.is_none());
    }
}

#[test]
fn project_metadata_real_capacity_and_arithmetic_refuse_before_compile_body() {
    let binding = binding(1024);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
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
            .materialize::<FragmentCompileError, _>(
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
        {
            let _host = host;
        }
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
    fn materialize<C, B>(
        &mut self,
        facts: &ProjectOutputRequestFacts,
        body: B,
    ) -> Result<
        novarocks_local_compiler::ProjectMetadataOutput,
        novarocks_local_compiler::ProjectMetadataFailure<Self::HostError, C>,
    >
    where
        C: From<FragmentCompileError>,
        B: FnOnce() -> Result<novarocks_local_compiler::ProjectMetadataOutput, C>,
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
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
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
    let program = with_native_type_construction_for_test(
        MemTracker::new_root("native Pending Project fixture"),
        &Control,
        |creation, scopes| {
            use novarocks_type_contract::ValueSchemaCreationFactory;
            let mut temporary = scopes.begin_scope().unwrap();
            novarocks_local_compiler::compile_fragment_with_creation_and_project_metadata_host(
                validated,
                &interpreter.functions,
                LocalCompileOptions {
                    pipeline_dop: NonZeroUsize::MIN,
                    root_sink_dop: Some(NonZeroUsize::MIN),
                    kernel_abi: KernelAbiVersion::CURRENT,
                    constants: interpreter.constants,
                },
                creation,
                &mut temporary,
                scopes,
                &Control,
                &mut host,
            )
            .unwrap()
        },
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
    {
        let _host = host;
    }
    assert!(!journal.pending.unwrap().complete);
    assert!(journal.capacity.is_none() && journal.shortage.is_none());
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
}

fn compile_window(
    binding: &QueryMemoryBinding,
    preparation: &novarocks_worker::PreparationControlLoan<'_>,
    journal: &mut ProjectMetadataJournal,
) -> CompiledTaskProgram {
    use super::super::execution_host::tests::compiled_package_host::{
        window_interpreter, window_producer,
    };
    let mut type_journal = super::super::TypeMaterializationJournal::default();
    let mut type_host = TypeMaterializationHost::new(binding, preparation, &mut type_journal);
    let mut project_host = ProjectMetadataHost::new(binding, preparation, journal);
    compile_native_package_with_funding_for_test(
        &window_interpreter(),
        &window_producer(),
        CompiledTaskOptions {
            pipeline_dop: NonZeroUsize::MIN,
        },
        MemTracker::new_root("native funded compile fixture"),
        &Control,
        &mut type_host,
        &mut project_host,
    )
    .unwrap()
}
fn analytic(program: &LocalProgram) -> ProgramNodeId {
    ProgramNodeId::new(
        program
            .graph()
            .nodes()
            .iter()
            .position(|node| matches!(node.kind(), ProgramNodeKind::Analytic { .. }))
            .expect("actual SQL has an Analytic"),
    )
}
#[test]
fn analytic_schema_real_native_compile_and_original_schema_share_one_binding() {
    let binding = binding(64 << 20);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let compiled = compile_window(&binding, &loan, &mut journal);
    let node = analytic(compiled.program());
    let prepared = super::super::prepare_window_metadata_for_test(
        compiled,
        node,
        &binding,
        MemTracker::new_root("actual prepared Analytic fixture"),
        &loan,
        &Control,
    )
    .unwrap();
    let layout = prepared.program.graph().nodes()[node.index()].output_layout();
    assert_eq!(prepared.occurrence.schema.slot_ids(), layout.slots());
    assert_eq!(
        prepared.occurrence.schema.arrow_schema_ref().as_ref(),
        layout.schema().as_ref()
    );
    let journal = prepared.occurrence.journal;
    assert_eq!(journal.phase, Some(ProjectMetadataPhase::Prepare(node)));
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
    assert_eq!(journal.body.stopped, Some(Ok(())));
}
#[test]
fn analytic_schema_private_native_loan_rejects_wrong_program_kind_site_and_layout_before_body() {
    let binding = binding(64 << 20);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let prepared = compile_window(&binding, &loan, &mut journal).into_preparation();
    let other = compile_window(&binding, &loan, &mut journal).into_preparation();
    let program = prepared.program();
    let node = analytic(program);
    let layout = program.graph().nodes()[node.index()].output_layout();
    let copied = layout.clone();
    let wrong_kind = ProgramNodeId::new(
        program
            .graph()
            .nodes()
            .iter()
            .position(|node| !matches!(node.kind(), ProgramNodeKind::Analytic { .. }))
            .unwrap(),
    );
    let prior_phase = journal.phase;
    let prior_workset = journal.workset_bytes;
    let prior_settlement = journal.body.settlement.is_some();
    let calls = Cell::new(0);
    let mut host = CompiledProjectMetadataHost::new(
        prepared.native_metadata_loan().unwrap(),
        &binding,
        &loan,
        &Control,
        &mut journal,
    );
    for (source, site, layout, message) in [
        (
            other.program(),
            CompiledSchemaSite::AnalyticOutput(node),
            layout,
            "Analytic schema program is not its native owner",
        ),
        (
            program,
            CompiledSchemaSite::AnalyticOutput(ProgramNodeId::new(usize::MAX)),
            layout,
            "Analytic schema site has no native node",
        ),
        (
            program,
            CompiledSchemaSite::AnalyticOutput(wrong_kind),
            program.graph().nodes()[wrong_kind.index()].output_layout(),
            "Analytic schema site is not an Analytic",
        ),
        (
            program,
            CompiledSchemaSite::AnalyticOutput(node),
            &copied,
            "Analytic schema layout is not its native output",
        ),
    ] {
        let failure = host
            .materialize(source, site, layout, || {
                calls.set(calls.get() + 1);
                panic!("a rejected source must not construct its schema")
            })
            .unwrap_err();
        assert_eq!(
            failure.cause(),
            &ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Request(
                MetadataRequestError::SourceModel(message)
            ))
        );
    }
    assert_eq!(calls.get(), 0);
    {
        let _host = host;
    }
    // Source refusal precedes journal.start, so the last compiler receipt
    // remains intact and must not be mistaken for this declined occurrence.
    assert_eq!(journal.phase, prior_phase);
    assert_eq!(journal.workset_bytes, prior_workset);
    assert_eq!(journal.body.settlement.is_some(), prior_settlement);
}
#[test]
fn analytic_schema_actual_worker_stop_keeps_nominal_cause_and_runs_no_original_body() {
    use novarocks_execution_contract::task_execution::status::{AbortCause, CancelReason};
    for stop in [
        PreparationStop::Cancel(CancelReason::UpstreamNoLongerNeeded),
        PreparationStop::Abort(AbortCause::QueryFailed),
        PreparationStop::Abort(AbortCause::LeaseExpired),
        PreparationStop::Abort(AbortCause::PeerTaskFailed),
    ] {
        let binding = binding(64 << 20);
        let owner = NativeNodePreparation::new(preparation_identity(&binding));
        let loan = owner.loan();
        let mut journal = ProjectMetadataJournal::default();
        let prepared = compile_window(&binding, &loan, &mut journal).into_preparation();
        let program = prepared.program();
        let node = analytic(program);
        let layout = program.graph().nodes()[node.index()].output_layout();
        assert!(owner.stop(stop));
        let calls = Cell::new(0);
        let mut host = CompiledProjectMetadataHost::new(
            prepared.native_metadata_loan().unwrap(),
            &binding,
            &loan,
            &Control,
            &mut journal,
        );
        let failure = host
            .materialize(
                program,
                CompiledSchemaSite::AnalyticOutput(node),
                layout,
                || {
                    calls.set(calls.get() + 1);
                    panic!("stopped preparation must not construct its schema")
                },
            )
            .unwrap_err();
        let expected = loan.checkpoint().unwrap_err();
        let ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::Interrupted(actual),
        )) = failure.cause()
        else {
            panic!("metadata retains the original Task observation in its declared Host channel");
        };
        assert!(std::ptr::eq(actual.cause(), expected.cause()));
        assert_eq!(calls.get(), 0);
        {
            let _host = host;
        }
        assert!(journal.body.settlement.is_none());
        assert!(journal.body.stopped.is_none());
    }
}

#[test]
fn analytic_schema_original_body_error_keeps_owned_bytes_through_the_real_host() {
    let binding = binding(64 << 20);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let compiled = compile_window(&binding, &loan, &mut journal);
    let node = analytic(compiled.program());
    assert_actual_schema_body_error_keeps_owned_bytes(
        compiled,
        CompiledSchemaSite::AnalyticOutput(node),
        &binding,
        &loan,
        &mut journal,
    );
}

#[test]
fn analytic_schema_actual_policy_refuses_before_original_body() {
    let binding = binding(64 << 20);
    let owner = NativeNodePreparation::new(preparation_identity(&binding));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let output = compile_window(&binding, &loan, &mut journal).into_preparation();
    let program = output.program();
    let node = analytic(program);
    let layout = program.graph().nodes()[node.index()].output_layout();
    binding
        .account()
        .install_policy(0, novarocks_memory::LimitDimension::Work);
    let calls = Cell::new(0);
    let mut host = CompiledProjectMetadataHost::new(
        output.native_metadata_loan().unwrap(),
        &binding,
        &loan,
        &Control,
        &mut journal,
    );
    let error = host
        .materialize(
            program,
            CompiledSchemaSite::AnalyticOutput(node),
            layout,
            || {
                calls.set(calls.get() + 1);
                ChunkSchema::from_compiled_layout(layout)
            },
        )
        .unwrap_err();
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::Capacity(_)
        ))
    ));
    assert_eq!(calls.get(), 0);
    {
        let _host = host;
    }
    assert!(journal.capacity.is_some());
    assert!(journal.body.settlement.is_none());
}

/// Numeric diagnostics use the actual Native creator, never a test grant. The
/// recorder checks the already frozen invoice against actual request callbacks;
/// GLOBAL capacity/origin qualification remains in the Server targets.
#[test]
fn type_tail_frozen_invoice_covers_actual_native_factory_callbacks() {
    use novarocks_plan_codec::{
        host_projection_v2::ProjectionFailure,
        physical_type_v2::{
            DecodedTypeTable, PackageTypeFundingScope, PackageTypeProjectionFacts, TypeCodecError,
            TypeMaterializationNamespace,
        },
    };
    use novarocks_type_contract::{
        ValueSchemaCreationFactory, ValueSchemaCreationScope, ValueSchemaScopeBeginFailure,
    };
    use std::alloc::Layout;
    type Counts = Cell<(usize, usize)>;
    fn record(counts: &Counts, bytes: usize) {
        if bytes != 0 {
            let (requests, previous) = counts.get();
            counts.set((
                requests.checked_add(1).unwrap(),
                previous.checked_add(bytes).unwrap(),
            ));
        }
    }
    struct Scope<'a> {
        inner: super::CompiledTypeScope<'a>,
        counts: &'a Counts,
    }
    impl ValueSchemaCreationScope<NativeTypeConstructionError> for Scope<'_> {
        fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
            self.inner.storage_lifetime()
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
            self.inner.request_layout(layout)?;
            record(self.counts, layout.size());
            Ok(())
        }
        fn observe_opaque_upper_bound(
            &mut self,
            bytes: usize,
        ) -> Result<(), NativeTypeConstructionError> {
            self.inner.observe_opaque_upper_bound(bytes)?;
            record(self.counts, bytes);
            Ok(())
        }
    }
    struct Factory<'a, 'control> {
        inner: &'a CompiledTypeScopes<'a, 'control>,
        counts: &'a Counts,
    }
    impl ValueSchemaCreationFactory<NativeTypeConstructionError> for Factory<'_, '_> {
        type Scope<'a>
            = Scope<'a>
        where
            Self: 'a;
        fn visit_scope_allocation_layouts<E>(
            &self,
            visit: &mut dyn FnMut(Layout) -> Result<(), E>,
        ) -> Result<(), E>
        where
            E: From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>,
        {
            self.inner.visit_scope_allocation_layouts(visit)
        }
        fn begin_scope(&self) -> Result<Self::Scope<'_>, NativeTypeConstructionError> {
            self.begin_scope_observed(&mut |_| Ok::<_, NativeTypeConstructionError>(()))
                .map_err(|error| match error {
                    ValueSchemaScopeBeginFailure::Observer(error)
                    | ValueSchemaScopeBeginFailure::Creator(error) => error,
                })
        }
        fn begin_scope_observed<O>(
            &self,
            observe: &mut dyn FnMut(Layout) -> Result<(), O>,
        ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>>
        {
            let inner = self.inner.begin_scope_observed(&mut |layout| {
                observe(layout)?;
                record(self.counts, layout.size());
                Ok(())
            })?;
            Ok(Scope {
                inner,
                counts: self.counts,
            })
        }
    }
    struct Invoices<'a> {
        counts: &'a Counts,
        calls: usize,
        scopes: usize,
    }
    impl PackageTypeFundingScope<NativeTypeConstructionError> for Invoices<'_> {
        fn materialize(
            &mut self,
            _: TypeMaterializationNamespace,
            facts: &PackageTypeProjectionFacts,
            body: &mut dyn FnMut() -> Result<
                DecodedTypeTable,
                ProjectionFailure<TypeCodecError, NativeTypeConstructionError>,
            >,
        ) -> Result<DecodedTypeTable, ProjectionFailure<TypeCodecError, NativeTypeConstructionError>>
        {
            let frozen = *facts;
            let before = self.counts.get();
            self.calls += 1;
            let result = body();
            let after = self.counts.get();
            let requests = after.0.checked_sub(before.0).unwrap();
            let bytes = after.1.checked_sub(before.1).unwrap();
            self.scopes += requests;
            assert!(
                requests <= frozen.allocation_requests_upper_bound,
                "actual request callbacks exceed the frozen tail invoice"
            );
            assert!(
                bytes <= frozen.allocation_request_bytes_upper_bound,
                "actual request bytes exceed the frozen tail invoice"
            );
            result
        }
    }
    let tracker = MemTracker::new_root("actual Native tail invoice diagnostic");
    let output = with_native_type_construction_for_test(Arc::clone(&tracker), &Control, |creation, scopes| {
        let counts = Counts::new((0, 0));
        let factory = Factory { inner: scopes, counts: &counts };
        let mut invoices = Invoices { counts: &counts, calls: 0, scopes: 0 };
        let interpreter = interpreter();
        let storage = creation.storage_lifetime();
        let output = creation.with_request_ports(|request, opaque| {
            let mut request = |layout: Layout| {
                request(layout)?;
                record(&counts, layout.size());
                Ok(())
            };
            let mut opaque = |bytes| {
                opaque(bytes)?;
                record(&counts, bytes);
                Ok(())
            };
            let mut observed = novarocks_type_contract::ValueSchemaCreation::new(storage, &mut request, &mut opaque);
            novarocks_plan_codec::physical_package_v2::decode_fragment_package_with_factory_and_funding(
                &select_one_producer().0, &interpreter.model, &interpreter.decode_limits,
                &mut observed, &factory, &Control, &mut invoices,
            )
        }).unwrap();
        assert_eq!(invoices.calls, 2, "each original namespace body executes once");
        assert!(invoices.scopes > 0, "the actual tail must open scopes and issue real requests");
        output
    }).unwrap();
    assert!(tracker.current() > 0);
    drop(output);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn accepted_original_task_source_crosses_both_compile_views_and_metadata_before_body() {
    use novarocks_execution::runtime::fragment::ExecutionFailure;
    use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
    for raw in [
        ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted),
        ExecutionFailure::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        ),
    ] {
        let binding = binding(64 << 20);
        let expected = raw.failure_fact();
        let owner = NativeNodePreparation::new(preparation_identity(&binding));
        owner.report_failure(raw.clone());
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let control = TaskPreparationControl::new(stop.view(), owner.stop_view());
        let interruption = control
            .checkpoint(CompilePhase::LowerProgram, 0)
            .unwrap_err();
        assert_eq!(interruption.failure_fact(), expected);
        let source =
            crate::backend_test_support::find_source::<ExecutionFailure>(&interruption).unwrap();
        assert!(std::ptr::eq(source.cause(), raw.cause()));
        assert_eq!(expected.evidence(), FailureEvidence::Originating);
        let tracker = MemTracker::new_root("original Task source before native type reservation");
        let refusal = match crate::native_type_construction::NativeTypeConstruction::try_new(
            tracker.clone(),
            stop.view(),
            owner.stop_view(),
        ) {
            Ok(_) => panic!("an accepted failure cannot authorize a type reservation"),
            Err(error) => error,
        };
        let source =
            crate::backend_test_support::find_source::<ExecutionFailure>(&refusal).unwrap();
        assert!(std::ptr::eq(source.cause(), raw.cause()));
        assert_eq!(tracker.current(), 0);
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
            .materialize::<FragmentCompileError, _>(&facts, || {
                calls.set(calls.get() + 1);
                panic!("an accepted original failure must precede capacity and body");
            })
            .err()
            .unwrap();
        let novarocks_local_compiler::ProjectMetadataFailure::Host(
            PreparationMemoryRefusal::Interrupted(actual),
        ) = failure
        else {
            panic!("retain the original control channel");
        };
        assert!(std::ptr::eq(actual.cause(), raw.cause()));
        assert_eq!(actual.failure_fact(), expected);
        assert_eq!(calls.get(), 0);
        {
            let _host = host;
        }
        assert!(journal.body.settlement.is_none());
        assert_eq!(binding.account().snapshot().granted_bytes, 0);
        drop(raw);
    }
}
