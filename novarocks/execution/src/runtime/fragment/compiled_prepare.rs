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
//! The same handle, resources, result session, runtime state and failure
//! points as [`prepare_fragment`](super::prepare_fragment) are reused; only
//! the program source differs. Nothing here reads a legacy fragment program,
//! thaws an expression arena or takes expression semantics from the query
//! options: those come from the compiled program alone.

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
use crate::exec::pipeline::executor::{
    prepare_compiled_program_pipeline_execution_with_profiler,
    prepare_compiled_program_pipeline_execution_with_profiler_and_metadata_host,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::exchange::materialize_compiled_exchange_receivers;
use crate::runtime::fragment::instance::FragmentInstanceSpec;
use crate::runtime::fragment::scan::{
    CompiledScanSources, materialize_compiled_scan_bindings, validate_compiled_scan_sources,
};
use crate::runtime::fragment::sink::materialize_compiled_sink;
use crate::runtime::preparation_metadata::{
    CompiledMetadataMode, CompiledSchemaMetadataScope, DirectCompiledSchemaMetadataScope,
};

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
    ) -> Result<(), String> {
        if self.writers.insert(node, binding).is_some() {
            return Err(format!(
                "compiled table writer at local node {} is bound twice",
                node.index()
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
    ) -> Result<(), String> {
        if self.finishers.insert(node, binding).is_some() {
            return Err(format!(
                "compiled table finish at local node {} is bound twice",
                node.index()
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
    pub fn validate(&self, program: &LocalProgram) -> Result<(), String> {
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
            return Err(
                "compiled write recipes do not address exactly the program's table writers"
                    .to_string(),
            );
        }
        if writers != self.writers.keys().copied().collect() {
            return Err(
                "Task write bindings do not cover exactly the program's table writers".to_string(),
            );
        }
        if finishers != self.finishers.keys().copied().collect() {
            return Err(
                "Task finish bindings do not cover exactly the program's table finishes"
                    .to_string(),
            );
        }
        for (node, binding) in &self.writers {
            let recipe = &program.write_recipes()[node];
            if binding.handle().binding() != recipe.draft().binding() {
                return Err(format!(
                    "Task write binding of local node {} is not its recipe's write binding",
                    node.index()
                ));
            }
        }
        Ok(())
    }
}

/// One compiled fragment instance: a LocalProgram compiled for this Task, the
/// Task-owned source of each of its scans, the Task's write capabilities, and
/// the instance facts it runs with.
pub struct CompiledFragmentSubmission {
    program: Arc<LocalProgram>,
    scans: CompiledScanSources,
    writers: CompiledWriterBindings,
    instance: FragmentInstanceSpec,
    sink_kind: FragmentSinkKind,
}

impl std::fmt::Debug for CompiledFragmentSubmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledFragmentSubmission")
            .field("nodes", &self.program.graph().nodes().len())
            .field("scans", &self.scans.len())
            .field("writers", &self.writers)
            .field("sink_kind", &self.sink_kind)
            .field("instance", &self.instance)
            .finish()
    }
}

impl CompiledFragmentSubmission {
    /// `scans` holds exactly one Task source per compiled Scan, and the
    /// instance assigns exactly the physical scan nodes those Scans address.
    pub fn try_new(
        program: Arc<LocalProgram>,
        scans: CompiledScanSources,
        instance: FragmentInstanceSpec,
    ) -> Result<Self, FragmentBindingError> {
        Self::try_new_with_writers(program, scans, CompiledWriterBindings::default(), instance)
    }

    /// As [`Self::try_new`], with the Task's write capabilities: exactly one
    /// per compiled writer and finish, each refused before anything is
    /// registered when it does not fit the program.
    pub fn try_new_with_writers(
        program: Arc<LocalProgram>,
        scans: CompiledScanSources,
        writers: CompiledWriterBindings,
        instance: FragmentInstanceSpec,
    ) -> Result<Self, FragmentBindingError> {
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
            ));
        }
        // A scan without its Task source, or a source or assignment without
        // its scan, is refused before anything is registered.
        validate_compiled_scan_sources(&program, &scans, &instance)?;
        writers.validate(&program).map_err(|detail| {
            FragmentBindingError::new(
                FragmentBindingTarget::Program,
                FragmentBindingErrorKind::InvalidAssignment,
                detail,
            )
        })?;
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

    pub const fn instance(&self) -> &FragmentInstanceSpec {
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
pub fn prepare_compiled_fragment(
    submission: CompiledFragmentSubmission,
    context: FragmentPrepareContext,
) -> Result<DormantFragmentHandle, FragmentLaunchError> {
    prepare_compiled_fragment_in(
        submission,
        context,
        &mut CompiledMetadataMode::<DirectCompiledSchemaMetadataScope>::Direct,
    )
}

pub fn prepare_compiled_fragment_with_metadata_host<H: CompiledSchemaMetadataScope>(
    submission: CompiledFragmentSubmission,
    context: FragmentPrepareContext,
    host: &mut H,
) -> Result<DormantFragmentHandle, FragmentLaunchError> {
    prepare_compiled_fragment_in(submission, context, &mut CompiledMetadataMode::Hosted(host))
}

fn prepare_compiled_fragment_in<H: CompiledSchemaMetadataScope>(
    mut submission: CompiledFragmentSubmission,
    context: FragmentPrepareContext,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> Result<DormantFragmentHandle, FragmentLaunchError> {
    // The write capabilities move into the one pipeline graph that owns them.
    let writers = std::mem::take(&mut submission.writers);
    let program = submission.program();
    let instance = submission.instance();
    let query_id = instance.query_id();
    let finst_id = instance.fragment_instance_id().get();
    let pipeline_dop = i32::try_from(instance.pipeline_dop().get()).map_err(|_| {
        FragmentLaunchError::new(
            FragmentLaunchStage::BuildPipelines,
            FragmentLaunchErrorKind::PipelineBuild,
            format!(
                "pipeline DOP {} exceeds runtime representation",
                instance.pipeline_dop()
            ),
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
        return Err(FragmentLaunchError::new(
            FragmentLaunchStage::ValidateSubmission,
            FragmentLaunchErrorKind::Binding,
            format!(
                "host root sink width {:?} differs from compiled width {frozen_root_sink_dop:?}",
                context.root_sink_dop
            ),
        ));
    }
    let mut resources = FragmentResources::new(
        Arc::clone(&context.commit_port),
        Arc::clone(&context.exchange_receiver_port),
        context.cleanup_faults(),
    );
    let prepare_result = (|| {
        resources.acquire_sink_commit(finst_id)?;
        context.fail_if_injected(PrepareFailurePoint::AfterSinkCommit)?;
        let mut result_spec = context.result_spec.clone().unwrap_or_else(|| {
            ResultWriteSpec::new(
                finst_id,
                ResultPresentation::MysqlText,
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
        let receivers = materialize_compiled_exchange_receivers(
            program,
            finst_id,
            instance.exchange_inputs(),
            Arc::clone(&context.exchange_receiver_port),
        )
        .map_err(|detail| {
            FragmentLaunchError::new(
                FragmentLaunchStage::Register,
                FragmentLaunchErrorKind::Binding,
                detail,
            )
        })?;
        resources.acquire_compiled_exchange(receivers.registrations)?;
        context.fail_if_injected(PrepareFailurePoint::AfterExchange)?;

        let runtime_state = build_runtime_state(RuntimeStateInputs {
            query_options: apply_query_option_overrides(
                Some(instance.runtime_options().query_options().clone()),
                context.execution_runtime.as_deref(),
            ),
            query_id: Some(query_id),
            fragment_instance_id: Some(finst_id),
            backend_num: Some(instance.backend_num().get()),
            mem_tracker: context.mem_tracker.clone(),
            runtime_filter_session: context.runtime_filter.clone(),
            execution_runtime: context.execution_runtime.clone(),
            query_memory: context.query_memory.clone(),
            task_identity: context.result_identity,
        })
        .map_err(|error| {
            FragmentLaunchError::new(
                FragmentLaunchStage::BuildRuntimeState,
                FragmentLaunchErrorKind::ResourceUnavailable,
                error,
            )
        })?;
        let result_sink = match program.graph().sink() {
            Some(StaticSinkProgram::RootResult(_)) => {
                let session = resources.root_result_session().ok_or_else(|| {
                    FragmentLaunchError::new(
                        FragmentLaunchStage::Materialize,
                        FragmentLaunchErrorKind::Materialization,
                        "compiled bounded root requires an opened host-owned RootResult session",
                    )
                })?;
                let dop = frozen_root_sink_dop.ok_or_else(|| {
                    FragmentLaunchError::new(
                        FragmentLaunchStage::Materialize,
                        FragmentLaunchErrorKind::Materialization,
                        "compiled bounded root requires its frozen sink DOP",
                    )
                })?;
                Some(Box::new(
                    crate::exec::operators::RootResultSinkFactory::try_new(session, dop).map_err(
                        |error| {
                            FragmentLaunchError::new(
                                FragmentLaunchStage::Materialize,
                                FragmentLaunchErrorKind::Materialization,
                                error,
                            )
                        },
                    )?,
                ) as Box<dyn OperatorFactory>)
            }
            Some(StaticSinkProgram::Result) => {
                let session = resources.result_session().ok_or_else(|| {
                    FragmentLaunchError::new(
                        FragmentLaunchStage::Materialize,
                        FragmentLaunchErrorKind::Materialization,
                        "compiled RESULT_SINK requires an opened Fragment result session",
                    )
                })?;
                Some(Box::new(ResultBufferSinkFactory::new(session, None))
                    as Box<dyn OperatorFactory>)
            }
            _ => None,
        };
        let sink = materialize_compiled_sink(
            program,
            instance.sink_assignment(),
            finst_id,
            Arc::clone(&context.exchange_transmitter),
            result_sink,
            context.edge_gates.clone(),
        )?;
        let scan_bindings =
            materialize_compiled_scan_bindings(program, &submission.scans, instance)?;
        match metadata {
            CompiledMetadataMode::Direct => {
                prepare_compiled_program_pipeline_execution_with_profiler(
                    Arc::clone(program),
                    Duration::from_millis(50),
                    sink,
                    receivers.bindings,
                    scan_bindings,
                    writers,
                    Some((finst_id.high(), finst_id.low())),
                    context.profiler.clone(),
                    pipeline_dop,
                    runtime_state,
                    Arc::clone(&context.event_sink),
                )
            }
            CompiledMetadataMode::Hosted(host) => {
                prepare_compiled_program_pipeline_execution_with_profiler_and_metadata_host(
                    Arc::clone(program),
                    Duration::from_millis(50),
                    sink,
                    receivers.bindings,
                    scan_bindings,
                    writers,
                    Some((finst_id.high(), finst_id.low())),
                    context.profiler.clone(),
                    pipeline_dop,
                    runtime_state,
                    Arc::clone(&context.event_sink),
                    *host,
                )
            }
        }
        .map_err(|error| {
            FragmentLaunchError::from_failure(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                error,
            )
        })
    })();
    match prepare_result {
        Ok(prepared) => Ok(DormantFragmentHandle {
            prepared,
            resources,
            query_id,
            fragment_instance_id: finst_id,
            profiler: context.profiler.clone(),
            mem_tracker: context.mem_tracker.clone(),
            start_failure: context.start_failure(),
        }),
        Err(error) => Err(error.with_cleanup_diagnostics(resources.rollback())),
    }
}

#[cfg(test)]
#[path = "compiled_prepare_tests.rs"]
mod tests;
