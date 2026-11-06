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

//! Task preparation for one compiled LocalProgram (local-compiler output).
//!
//! Task preparation owns its handle, resources, result session, runtime state
//! and failure points. Nothing here reads a legacy fragment program,
//! thaws an expression arena or takes expression semantics from the query
//! options: those come from the compiled program alone.

use crate::runtime::task::io::ResultPresentation;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind, StaticSinkProgram};

use super::*;
use crate::exec::fragment::error::{
    FragmentBindingError, FragmentBindingErrorKind, FragmentBindingTarget,
};
use crate::exec::fragment::program::FragmentSinkKind;
use crate::exec::node::table_finish::TableFinishRuntimeBinding;
use crate::exec::node::table_writer::TableWriterRuntimeBinding;
use crate::exec::operators::ResultBufferSinkFactory;
use crate::exec::pipeline::executor::prepare_compiled_program_with_sink_materializer;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::task::exchange::materialize_compiled_exchange_receivers;
use crate::runtime::task::instance::TaskInstanceSpec;
use crate::runtime::task::scan::{
    CompiledScanSources, materialize_compiled_scan_bindings, validate_compiled_scan_sources,
};
use crate::runtime::task::sink::materialize_compiled_sink_for_pipeline;

/// One Task's write capabilities for its compiled program: one bound write
/// capability per compiled TableWriter and one validation authority per
/// compiled TableFinish, each keyed by its local node.
#[derive(Default)]
pub struct CompiledWriterBindings {
    writers: BTreeMap<ProgramNodeId, TableWriterRuntimeBinding>,
    finishers: BTreeMap<ProgramNodeId, TableFinishRuntimeBinding>,
}

impl std::fmt::Debug for CompiledWriterBindings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledWriterBindings")
            .field("writers", &self.writers.keys().collect::<Vec<_>>())
            .field("finishers", &self.finishers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CompiledWriterBindings {
    /// Bind the write capability of the TableWriter at `node`; a node is
    /// bound once.
    pub fn bind_writer(
        &mut self,
        node: ProgramNodeId,
        binding: TableWriterRuntimeBinding,
    ) -> crate::runtime::task::ExecutionResult<()> {
        if self.writers.insert(node, binding).is_some() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled table writer at local node {} is bound twice",
                    node.index()
                ),
            ));
        }
        Ok(())
    }

    /// Bind the validation authority of the TableFinish at `node`; a node is
    /// bound once.
    pub fn bind_finish(
        &mut self,
        node: ProgramNodeId,
        binding: TableFinishRuntimeBinding,
    ) -> crate::runtime::task::ExecutionResult<()> {
        if self.finishers.insert(node, binding).is_some() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled table finish at local node {} is bound twice",
                    node.index()
                ),
            ));
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.writers.is_empty() && self.finishers.is_empty()
    }

    pub(crate) fn writer(&self, node: ProgramNodeId) -> Option<&TableWriterRuntimeBinding> {
        self.writers.get(&node)
    }

    pub(crate) fn finisher(&self, node: ProgramNodeId) -> Option<&TableFinishRuntimeBinding> {
        self.finishers.get(&node)
    }

    /// The bindings cover exactly the program's writer family: one writer
    /// capability per TableWriter, which is exactly a node with a provider
    /// recipe, bound for the recipe's own write binding; one validation
    /// authority per TableFinish; nothing else.
    pub fn validate(&self, program: &LocalProgram) -> crate::runtime::task::ExecutionResult<()> {
        let mut writers = BTreeSet::new();
        let mut finishers = BTreeSet::new();
        for (index, node) in program.graph().nodes().iter().enumerate() {
            match node.kind() {
                ProgramNodeKind::TableWriter { .. } => {
                    writers.insert(ProgramNodeId::new(index));
                }
                ProgramNodeKind::TableFinish { .. } => {
                    finishers.insert(ProgramNodeId::new(index));
                }
                _ => {}
            }
        }
        if writers != program.write_recipes().keys().copied().collect() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                "compiled write recipes do not address exactly the program's table writers",
            ));
        }
        if writers != self.writers.keys().copied().collect() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                "Task write bindings do not cover exactly the program's table writers",
            ));
        }
        if finishers != self.finishers.keys().copied().collect() {
            return Err(crate::exec::fragment::error::program_binding_failure(
                FragmentBindingErrorKind::InvalidAssignment,
                "Task finish bindings do not cover exactly the program's table finishes",
            ));
        }
        for (id, binding) in &self.finishers {
            let ProgramNodeKind::TableFinish {
                inputs,
                input_roles,
                count_mode,
                ..
            } = program.graph().nodes()[id.index()].kind()
            else {
                unreachable!("finish binding set was checked above");
            };
            let receiver = input_roles
                .summaries
                .write_counts
                .and_then(|ordinal| inputs.get(ordinal as usize))
                .and_then(|input| program.exchange_inputs().get(input));
            let valid =
                match (count_mode, receiver, &binding.logical_summary) {
                    (
                        novarocks_local_program::FinishCountMode::LogicalChanges,
                        Some(receiver),
                        Some(bound),
                    ) => !bound.sources.is_empty()
                        && receiver.role
                            == novarocks_local_program::ProgramOutputRole::LogicalSummary
                        && receiver.delivery
                            == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                        && i32::try_from(receiver.receiver_node).ok() == Some(bound.receiver_node),
                    (novarocks_local_program::FinishCountMode::LogicalChanges, _, _) => false,
                    (_, None, None) => true,
                    _ => false,
                };
            if !valid {
                return Err(crate::exec::fragment::error::program_binding_failure(
                    FragmentBindingErrorKind::InvalidAssignment,
                    "Task finish summary binding differs from its exact input role",
                ));
            }
        }
        for (node, binding) in &self.writers {
            let recipe = &program.write_recipes()[node];
            if binding.handle().binding() != recipe.draft().binding() {
                return Err(crate::exec::fragment::error::program_binding_failure(
                    FragmentBindingErrorKind::InvalidAssignment,
                    format!(
                        "Task write binding of local node {} is not its recipe's write binding",
                        node.index()
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// One compiled fragment instance: a LocalProgram compiled for this Task, the
/// Task-owned source of each of its scans, the Task's write capabilities, and
/// the instance facts it runs with.
pub struct CompiledTaskSubmission {
    program: Arc<LocalProgram>,
    scans: CompiledScanSources,
    writers: CompiledWriterBindings,
    instance: TaskInstanceSpec,
    sink_kind: FragmentSinkKind,
}

impl std::fmt::Debug for CompiledTaskSubmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledTaskSubmission")
            .field("nodes", &self.program.graph().nodes().len())
            .field("scans", &self.scans.len())
            .field("writers", &self.writers)
            .field("sink_kind", &self.sink_kind)
            .field("instance", &self.instance)
            .finish()
    }
}

impl CompiledTaskSubmission {
    /// `scans` holds exactly one Task source per compiled Scan, and the
    /// instance assigns exactly the physical scan nodes those Scans address.
    pub fn try_new(
        program: Arc<LocalProgram>,
        scans: CompiledScanSources,
        instance: TaskInstanceSpec,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        Self::try_new_with_writers(program, scans, CompiledWriterBindings::default(), instance)
    }

    /// As [`Self::try_new`], with the Task's write capabilities: exactly one
    /// per compiled writer and finish, each refused before anything is
    /// registered when it does not fit the program.
    pub fn try_new_with_writers(
        program: Arc<LocalProgram>,
        scans: CompiledScanSources,
        writers: CompiledWriterBindings,
        instance: TaskInstanceSpec,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        let expected_dop = program.graph().profile().pipeline_dop();
        if expected_dop != instance.pipeline_dop() {
            return Err(FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "compiled program compiled for pipeline DOP {}, got {}",
                    expected_dop.get(),
                    instance.pipeline_dop().get()
                ),
            )
            .into());
        }
        // A scan without its Task source, or a source or assignment without
        // its scan, is refused before anything is registered.
        validate_compiled_scan_sources(&program, &scans, &instance)?;
        writers.validate(&program)?;
        let sink_kind = compiled_sink_kind(program.graph().sink())?;
        Ok(Self {
            program,
            scans,
            writers,
            instance,
            sink_kind,
        })
    }

    pub fn program(&self) -> &Arc<LocalProgram> {
        &self.program
    }

    pub const fn instance(&self) -> &TaskInstanceSpec {
        &self.instance
    }

    pub const fn sink_kind(&self) -> FragmentSinkKind {
        self.sink_kind
    }
}

/// The Task-visible sink kind of a compiled program's static sink.
pub fn compiled_sink_kind(
    sink: Option<&StaticSinkProgram>,
) -> Result<FragmentSinkKind, FragmentBindingError> {
    match sink {
        Some(StaticSinkProgram::Result | StaticSinkProgram::RootResult(_)) => {
            Ok(FragmentSinkKind::Result)
        }
        Some(StaticSinkProgram::Noop) => Ok(FragmentSinkKind::Noop),
        Some(StaticSinkProgram::DataStream { .. }) => Ok(FragmentSinkKind::DataStream),
        Some(StaticSinkProgram::MultiCastDataStream { .. }) => {
            Ok(FragmentSinkKind::MultiCastDataStream)
        }
        Some(StaticSinkProgram::SplitDataStream { .. }) => Ok(FragmentSinkKind::SplitDataStream),
        Some(StaticSinkProgram::ChangeStreamRouter { .. }) => Ok(FragmentSinkKind::SplitDataStream),
        None => Err(FragmentBindingError::new(
            FragmentBindingTarget::Sink,
            FragmentBindingErrorKind::MissingAssignment,
            "compiled program has no static sink",
        )),
    }
}

/// Prepare one compiled fragment instance into the same dormant handle a
/// legacy submission produces, so the Task host starts, observes and cleans
/// it up unchanged.
pub fn prepare_compiled_task(
    submission: CompiledTaskSubmission,
    context: LocalTaskPrepareContext,
) -> Result<PreparedTaskExecution, LocalTaskLaunchError> {
    prepare_compiled_task_in(submission, context, None)
}

pub fn prepare_compiled_task_with_metadata_host(
    submission: CompiledTaskSubmission,
    context: LocalTaskPrepareContext,
    host: &mut dyn crate::runtime::preparation_metadata::CompiledSchemaPreparationScope,
) -> Result<PreparedTaskExecution, LocalTaskLaunchError> {
    prepare_compiled_task_in(submission, context, Some(host))
}

fn prepare_compiled_task_in(
    mut submission: CompiledTaskSubmission,
    mut context: LocalTaskPrepareContext,
    schema_preparation: Option<
        &mut dyn crate::runtime::preparation_metadata::CompiledSchemaPreparationScope,
    >,
) -> Result<PreparedTaskExecution, LocalTaskLaunchError> {
    // Validate the complete Task capability binding before acquiring a sink,
    // opening a result session or allowing any execution capture to attach.
    let control_error = match (context.result_identity, context.task_control.as_ref()) {
        (Some(identity), Some(control)) if identity == control.identity() => None,
        (None, None) => None,
        (Some(_), None) => Some(crate::runtime::task::ExecutionFailure::from_domain(
            crate::runtime::task_control::MissingTaskExecutionControl,
        )),
        _ => Some(crate::runtime::task::ExecutionFailure::from_domain(
            crate::runtime::task_control::TaskControlIdentityMismatch,
        )),
    };
    if let Some(error) = control_error {
        return Err(LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::ValidateSubmission,
            LocalTaskLaunchErrorKind::Binding,
            error,
        ));
    }
    if context.result_identity.is_some() && context.completion_work.is_none() {
        return Err(LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::ValidateSubmission,
            LocalTaskLaunchErrorKind::Binding,
            FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                "Task preparation requires its pre-admitted original completion roles",
            )
            .into(),
        ));
    }
    if context
        .completion_work
        .as_ref()
        .is_some_and(|roles| roles.identity() != context.result_identity)
    {
        return Err(LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::ValidateSubmission,
            LocalTaskLaunchErrorKind::Binding,
            FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                "admitted completion roles do not match the fragment's exact Task identity",
            )
            .into(),
        ));
    }
    // The write capabilities move into the one pipeline graph that owns them.
    let writers = std::mem::take(&mut submission.writers);
    let program = submission.program();
    let instance = submission.instance();
    let query_id = instance.query_id();
    let instance_task_key = instance.task_key().get();
    let pipeline_dop = i32::try_from(instance.pipeline_dop().get()).map_err(|_| {
        LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::BuildPipelines,
            LocalTaskLaunchErrorKind::PipelineBuild,
            FragmentBindingError::new(
                FragmentBindingTarget::Instance,
                FragmentBindingErrorKind::InvalidAssignment,
                format!(
                    "pipeline DOP {} exceeds runtime representation",
                    instance.pipeline_dop()
                ),
            )
            .into(),
        )
    })?;
    // The root sink width is a compiled profile fact; a host override that
    // disagrees with it is refused, never applied.
    let frozen_root_sink_dop = program
        .graph()
        .profile()
        .root_sink_dop()
        .and_then(|dop| i32::try_from(dop.get()).ok());
    if context.root_sink_dop.is_some() && context.root_sink_dop != frozen_root_sink_dop {
        return Err(LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::ValidateSubmission,
            LocalTaskLaunchErrorKind::Binding,
            FragmentBindingError::new(
                FragmentBindingTarget::Sink,
                FragmentBindingErrorKind::InvalidAssignment,
                format!("host root sink width {:?} differs from compiled width {frozen_root_sink_dop:?}", context.root_sink_dop),
            ).into(),
        ));
    }
    let mut resources = LocalTaskResources::new(
        Arc::clone(&context.commit_port),
        Arc::clone(&context.exchange_receiver_port),
        context.cleanup_faults(),
    );
    let prepare_result = (|| {
        resources.acquire_sink_commit(instance_task_key)?;
        context.fail_if_injected(PrepareFailurePoint::AfterSinkCommit)?;
        let mut result_spec = context.result_spec.clone().unwrap_or_else(|| {
            ResultWriteSpec::new(
                instance_task_key,
                ResultPresentation::ArrowIpc,
                None,
                instance.runtime_options().typed_result_sink(),
            )
        });
        if let Some(identity) = context.result_identity {
            result_spec = result_spec.with_task_identity(identity);
        }
        resources.acquire_root_result_for_static(
            program.graph().sink(),
            context.root_result_session.clone(),
            context.result_identity,
        )?;
        resources.acquire_result_for_static(
            program.graph().sink(),
            &context.result_writer,
            result_spec,
        )?;
        context.fail_if_injected(PrepareFailurePoint::AfterResult)?;
        let runtime_state = build_runtime_state(RuntimeStateInputs {
            query_options: apply_query_option_overrides(
                Some(instance.runtime_options().query_options().clone()),
                context.execution_runtime.as_deref(),
            ),
            query_id: Some(query_id),
            task_key: Some(instance_task_key),
            backend_num: Some(instance.backend_num().get()),
            mem_tracker: context.mem_tracker.clone(),
            runtime_filter_session: context.runtime_filter.clone(),
            execution_runtime: context.execution_runtime.clone(),
            query_memory: context.query_memory.clone(),
            task_identity: context.result_identity,
            task_control: context.task_control.clone(),
            #[cfg(novarocks_dev_controls)]
            exchange_test_observer: context.exchange_test_observer.clone(),
        })
        .map_err(|error| {
            LocalTaskLaunchError::from_failure(
                LocalTaskLaunchStage::BuildRuntimeState,
                LocalTaskLaunchErrorKind::ResourceUnavailable,
                error,
            )
        })?;
        #[cfg(novarocks_dev_controls)]
        let runtime_state = {
            let mut state = runtime_state;
            Arc::get_mut(&mut state)
                .expect("newly constructed runtime state has no aliases")
                .install_join_build_control(context.join_build_control.clone());
            Arc::get_mut(&mut state)
                .expect("newly constructed runtime state has no aliases")
                .install_query_tracker_witness(context.query_tracker_witness.clone());
            state
        };
        // Both receiver lifetimes use the same tracker, model and creation
        // author. Native explicitly retains its stop owner through retirement.
        let (receiver_creation, receiver_construction_stop) =
            if program.exchange_inputs().is_empty() {
                (None, None)
            } else {
                let (factory, stop) =
                    context
                        .receiver_construction(&runtime_state)
                        .map_err(|cause| {
                            LocalTaskLaunchError::from_failure(
                                LocalTaskLaunchStage::Register,
                                LocalTaskLaunchErrorKind::ResourceUnavailable,
                                cause.into(),
                            )
                        })?;
                (Some(factory), stop)
            };
        let receivers = materialize_compiled_exchange_receivers(
            program,
            instance_task_key,
            instance.exchange_inputs(),
            Arc::clone(&context.exchange_receiver_port),
            receiver_creation.as_ref(),
        )
        .map_err(|cause| {
            let kind = match &cause {
                crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(_) => {
                    LocalTaskLaunchErrorKind::Binding
                }
                _ => LocalTaskLaunchErrorKind::ResourceUnavailable,
            };
            LocalTaskLaunchError::from_failure(LocalTaskLaunchStage::Register, kind, cause.into())
        })?;
        resources.acquire_compiled_exchange(receivers.registrations, receiver_construction_stop)?;
        context.fail_if_injected(PrepareFailurePoint::AfterExchange)?;

        let result_sink = match program.graph().sink() {
            Some(StaticSinkProgram::RootResult(_)) => {
                let session = resources.root_result_session().ok_or_else(|| {
                    LocalTaskLaunchError::from_failure(
                        LocalTaskLaunchStage::Materialize,
                        LocalTaskLaunchErrorKind::Materialization,
                        FragmentBindingError::new(
                            FragmentBindingTarget::Sink,
                            FragmentBindingErrorKind::MissingAssignment,
                            "compiled bounded root requires an opened host-owned RootResult session",
                        ).into(),
                    )
                })?;
                let dop = frozen_root_sink_dop.ok_or_else(|| {
                    LocalTaskLaunchError::from_failure(
                        LocalTaskLaunchStage::Materialize,
                        LocalTaskLaunchErrorKind::Materialization,
                        FragmentBindingError::new(
                            FragmentBindingTarget::Sink,
                            FragmentBindingErrorKind::MissingAssignment,
                            "compiled bounded root requires its frozen sink DOP",
                        )
                        .into(),
                    )
                })?;
                Some(Box::new(
                    crate::exec::operators::RootResultSinkFactory::try_new(session, dop).map_err(
                        |error| {
                            LocalTaskLaunchError::from_failure(
                                LocalTaskLaunchStage::Materialize,
                                LocalTaskLaunchErrorKind::Materialization,
                                error.into(),
                            )
                        },
                    )?,
                ) as Box<dyn OperatorFactory>)
            }
            Some(StaticSinkProgram::Result) => {
                let session = resources.result_session().ok_or_else(|| {
                    LocalTaskLaunchError::from_failure(
                        LocalTaskLaunchStage::Materialize,
                        LocalTaskLaunchErrorKind::Materialization,
                        FragmentBindingError::new(
                            FragmentBindingTarget::Sink,
                            FragmentBindingErrorKind::MissingAssignment,
                            "compiled RESULT_SINK requires an opened Fragment result session",
                        )
                        .into(),
                    )
                })?;
                Some(Box::new(ResultBufferSinkFactory::new(session, None))
                    as Box<dyn OperatorFactory>)
            }
            _ => None,
        };
        let scan_bindings =
            materialize_compiled_scan_bindings(program, &submission.scans, instance)?;
        prepare_compiled_program_with_sink_materializer(
            Arc::clone(program),
            Duration::from_millis(50),
            |actual_root_dop| {
                materialize_compiled_sink_for_pipeline(
                    program,
                    instance.sink_assignment(),
                    instance_task_key,
                    Arc::clone(&context.exchange_transmitter),
                    result_sink,
                    context.edge_gates.clone(),
                    actual_root_dop,
                    runtime_state.mem_tracker(),
                )
            },
            receivers.bindings,
            scan_bindings,
            writers,
            Some((instance_task_key.high(), instance_task_key.low())),
            context.profiler.clone(),
            pipeline_dop,
            Arc::clone(&runtime_state),
            Arc::clone(&context.event_sink),
            schema_preparation,
        )
    })();
    match prepare_result {
        Ok(prepared) => {
            if context.completion_work.is_none() && context.result_identity.is_none() {
                let (roles, poller) =
                    crate::runtime::local_completion::LocalCompletionPoller::admit(
                        prepared.failure_control().exit_health_reporter(),
                    );
                context.completion_work = Some(roles);
                prepared.admit_local_completion_poller(poller);
            }
            let roles = context
                .completion_work
                .take()
                .expect("validated Task roles or the original taskless admission");
            let (finisher, wall_time, publisher) = roles.into_parts();
            let (permit, work) = finisher.into_parts();
            prepared.admit_completion_work(work, wall_time);
            let (settlement, publisher_work) = (permit, publisher);
            Ok(PreparedTaskExecution {
                prepared,
                resources,
                query_id,
                task_key: instance_task_key,
                profiler: context.profiler.clone(),
                mem_tracker: context.mem_tracker.clone(),
                start_failure: context.start_failure(),
                settlement,
                publisher_work,
                success_validator: None,
            })
        }
        Err(error) => Err(error.with_cleanup_diagnostics(resources.rollback())),
    }
}

#[cfg(test)]
#[path = "compiled_prepare_tests.rs"]
pub(in crate::runtime::task) mod tests;
