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
//! Pipeline structure metadata.
//!
//! Responsibilities:
//! - Represents one pipeline with ordered operator factories and dependency edges.
//! - Carries execution properties used by scheduler and driver construction.
//!
//! Key exported interfaces:
//! - Types: `Pipeline`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::runtime::task::ExecutionResult;

use std::sync::Arc;

use super::driver::{PipelineDriver, PipelineDriverBindings};
use super::instance::OperatorInstance;
use super::operator::input_construction::{
    InputConstructionProfile, try_runtime_input_construction_factory,
};
use super::operator_factory::OperatorFactory;
use super::task_context::TaskContext;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use tracing::debug;

/// Bounds for construction observations; actual installed tracker limits still
/// decide each request. This is no separate query or batch capacity grant.
pub fn representation_construction_profile()
-> Result<InputConstructionProfile, novarocks_type_contract::CompileControlError> {
    let max_bytes = usize::try_from(i64::MAX)
        .map_err(|_| novarocks_type_contract::CompileControlError::ResourceExhausted)?;
    let max_requests = novarocks_type_contract::MAX_VALUE_TYPE_NODES
        .checked_mul(novarocks_type_contract::MAX_VALUE_TYPE_NODES)
        .and_then(|nodes| nodes.checked_mul(64))
        .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
    let opaque_per_node = novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
        .checked_add(2)
        .and_then(|depth| depth.checked_mul(12))
        .and_then(|requests| requests.checked_add(32))
        .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
    let max_opaque_requests = novarocks_type_contract::MAX_VALUE_TYPE_NODES
        .checked_mul(novarocks_type_contract::MAX_VALUE_TYPE_NODES)
        .and_then(|nodes| nodes.checked_mul(opaque_per_node))
        .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
    Ok(InputConstructionProfile {
        max_requests,
        max_request_bytes: max_bytes,
        max_opaque_requests,
        max_opaque_bytes: max_bytes,
        max_work: usize::MAX,
        max_depth: novarocks_type_contract::MAX_VALUE_TYPE_DEPTH,
    })
}

/// One pipeline definition containing ordered operator factories and dependency links.
pub struct Pipeline {
    id: i32,
    op_factories: Vec<Box<dyn OperatorFactory>>,
    dop: i32,
}

impl Pipeline {
    pub fn new(id: i32, op_factories: Vec<Box<dyn OperatorFactory>>, dop: i32) -> Self {
        Self {
            id,
            op_factories,
            dop: dop.max(1),
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn dop(&self) -> i32 {
        self.dop
    }

    pub(crate) fn instantiate_drivers(
        &self,
        ctx: &Arc<TaskContext>,
    ) -> ExecutionResult<Vec<PipelineDriver>> {
        self.instantiate_drivers_with_schema_preparation(ctx, None)
    }

    pub(crate) fn instantiate_drivers_with_schema_preparation(
        &self,
        ctx: &Arc<TaskContext>,
        mut schema_preparation: Option<
            &mut dyn crate::runtime::preparation_metadata::CompiledSchemaPreparationScope,
        >,
    ) -> ExecutionResult<Vec<PipelineDriver>> {
        for factory in &self.op_factories {
            factory.bind_pipeline_dop(self.dop)?;
        }
        let mut drivers = Vec::new();
        let pipeline_profiler = ctx
            .profiler()
            .map(|p| p.child(format!("Pipeline (id={})", self.id)));
        for i in 0..self.dop {
            let mut operators = Vec::with_capacity(self.op_factories.len());
            let mut operator_profiles = Vec::new();
            let mem_root = ctx.runtime_state().mem_tracker();
            let mut source_idx = None;
            let mut sink_idx = None;
            let driver_id = ctx.next_driver_id();
            if self
                .op_factories
                .iter()
                .any(|factory| factory.name().contains("LOCAL_EXCHANGE_SINK"))
            {
                let op_names = self
                    .op_factories
                    .iter()
                    .map(|factory| factory.name())
                    .collect::<Vec<_>>()
                    .join(" -> ");
                debug!(
                    "Pipeline driver created: pipeline_id={} driver_id={} local_index={} dop={} operators={}",
                    self.id, driver_id, i, self.dop, op_names
                );
            }
            let driver_profiler = pipeline_profiler
                .as_ref()
                .map(|p| p.child(format!("PipelineDriver (id={})", driver_id)));
            for (idx, factory) in self.op_factories.iter().enumerate() {
                if factory.is_source() {
                    if source_idx.is_some() {
                        return Err(PipelineShapeFailure::MultipleSource.into());
                    }
                    source_idx = Some(idx);
                }
                if factory.is_sink() {
                    if sink_idx.is_some() {
                        return Err(PipelineShapeFailure::MultipleSink.into());
                    }
                    sink_idx = Some(idx);
                }
                let mut op = factory.create(self.dop, i);
                let operator_mem_tracker = mem_root.as_ref().map(|root| {
                    MemTracker::new_child(format!("operator {}: {}", idx, op.name()), root)
                });
                if let Some(tracker) = operator_mem_tracker.as_ref() {
                    op.set_mem_tracker(Arc::clone(tracker));
                }
                // Keep the original-input root edge outside ordinary input
                // construction. Its permit and actual-exit owner stay intact.
                let construction_factory = if op.creates_typed_storage() {
                    operator_mem_tracker
                        .as_ref()
                        .map(|tracker| {
                            try_runtime_input_construction_factory(
                                representation_construction_profile()?,
                                Arc::clone(tracker),
                                ctx.runtime_state().error_state(),
                            )
                        })
                        .transpose()?
                } else {
                    None
                };
                if let Some(factory) = construction_factory.as_ref() {
                    op.set_construction_factory(Arc::clone(factory));
                }
                if let Some(driver_profiler) = driver_profiler.as_ref() {
                    let op_profile = driver_profiler.child(op.name().to_string());
                    if let Some(identity) = factory.compiled_operator_profile() {
                        op_profile.set_compiled_operator(Arc::clone(identity));
                    }
                    let profiles = OperatorProfiles::new(op_profile);
                    op.set_profiles(profiles.clone());
                    operator_profiles.push(profiles);
                }
                // Activation may create asynchronous workers which retain the
                // event sink. Install the fragment-owned sink while preparing
                // so they cannot capture the default no-op recorder later.
                op.set_task_event_sink(ctx.event_sink());
                let site = op
                    .compiled_schema_preparation_site()
                    .map(|(program, site)| (Arc::clone(program), site));
                match (site, schema_preparation.as_mut()) {
                    (Some((program, site)), Some(observer)) => {
                        use crate::runtime::preparation_metadata::CompiledSchemaSite;
                        let node = match site {
                            CompiledSchemaSite::Project(node)
                            | CompiledSchemaSite::AnalyticOutput(node) => node,
                            CompiledSchemaSite::FinalResult => program.graph().root(),
                        };
                        let layout = program.graph().nodes()[node.index()].output_layout();
                        observer.prepare(&program, site, layout, &mut || op.prepare())?;
                    }
                    _ => op.prepare()?,
                }
                op.bind_runtime_state(ctx.runtime_state())?;
                operators.push(OperatorInstance::new(
                    idx,
                    op,
                    operator_mem_tracker,
                    ctx.runtime_state().error_state(),
                ));
            }
            let source_idx = source_idx.ok_or(PipelineShapeFailure::MissingSource)?;
            let sink_idx = sink_idx.ok_or(PipelineShapeFailure::MissingSink)?;
            if source_idx != 0 {
                return Err(PipelineShapeFailure::SourcePosition.into());
            }
            if sink_idx + 1 != operators.len() {
                return Err(PipelineShapeFailure::SinkPosition.into());
            }
            drivers.push(PipelineDriver::from_prepared(
                driver_id,
                operators,
                driver_profiler,
                operator_profiles,
                std::sync::Arc::clone(ctx.runtime_state()),
                ctx.task_key(),
                PipelineDriverBindings::new(ctx.event_sink())
                    .with_query_memory(ctx.runtime_state().query_memory().cloned()),
            ));
        }
        Ok(drivers)
    }
}

/// The pipeline author validates its own endpoint topology before activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipelineShapeFailure {
    MultipleSource,
    MultipleSink,
    MissingSource,
    MissingSink,
    SourcePosition,
    SinkPosition,
}
impl std::fmt::Display for PipelineShapeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MultipleSource => "pipeline has multiple source operators",
            Self::MultipleSink => "pipeline has multiple sink operators",
            Self::MissingSource => "pipeline missing source operator",
            Self::MissingSink => "pipeline missing sink operator",
            Self::SourcePosition => "pipeline source must be the first operator",
            Self::SinkPosition => "pipeline sink must be the last operator",
        })
    }
}
impl std::error::Error for PipelineShapeFailure {}
impl novarocks_failure_contract::FailureProjection for PipelineShapeFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::MultipleSource | Self::MultipleSink => ExecutionReason::PipelineMultipleEndpoint,
            Self::MissingSource | Self::MissingSink => ExecutionReason::PipelineMissingEndpoint,
            Self::SourcePosition | Self::SinkPosition => ExecutionReason::PipelineEndpointPosition,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<PipelineShapeFailure> for crate::runtime::task::ExecutionFailure {
    fn from(cause: PipelineShapeFailure) -> Self {
        Self::from_domain(cause)
    }
}
