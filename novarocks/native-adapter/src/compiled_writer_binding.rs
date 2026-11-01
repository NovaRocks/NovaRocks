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

//! Task binding of a compiled program's provider writers and finishes.
//!
//! A compiled TableWriter carries its writer as one provider-validated
//! recipe: the exact write binding, the provider's handle payload and the
//! input shape the provider signed. This binder turns each recipe into the
//! Task's write capability. It reads no generated DTO: the handle is decoded
//! from the recipe's own payload by the installed write execution of exactly
//! the recipe's binding, and the decoded handle must name that same binding.
//! Each finish gets the Task's canonical carrier validator. Commit authority
//! never reaches a backend: it stays with the frontend's write session.
//!
//! The writer ordinal is pinned to 0: the compiler admits exactly one writer
//! per fragment, so no consumer can distinguish another ordinal. Operator
//! diagnostics and failpoints are keyed by the compiled display identity, the
//! writer's local node index.

use novarocks_execution::runtime::fragment::ExecutionFailure;

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use novarocks_connector_contract::ConnectorWriteRecipe;
use novarocks_execution::exec::node::table_finish::TableFinishRuntimeBinding;
use novarocks_execution::exec::node::table_writer::{
    TableWriterPhysicalContextTemplate, TableWriterRuntimeBinding,
};
use novarocks_execution::runtime::fragment::CompiledWriterBindings;
use novarocks_execution::runtime::query_options::{QueryOptions, query_expire_durations};
use novarocks_local_program::{LocalProgram, ProgramNodeId, ProgramNodeKind};
use novarocks_spi::connector::write_stack::validate_writer_handle_bytes;
use novarocks_spi::connector::{
    ConnectorRequestContext, ConnectorStopView, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use novarocks_types::UniqueId;
use novarocks_worker::TypedScanRuntime;
use novarocks_worker::connector_write_runtime::ObservedConnectorWriteExecution;

#[cfg(novarocks_dev_controls)]
use crate::connector_write_data_plane::QueryScopedTableWriteAggregateGuard;
use crate::connector_write_data_plane::{
    NativeConnectorWriteObservationPort, RoleBoundCommitFragmentEncoder,
    RootCommitFragmentCarrierValidator,
};

/// The Task facts every compiled writer and finish of one task binds with.
pub(crate) struct CompiledWriteTask<'a> {
    #[cfg(novarocks_dev_controls)]
    pub(crate) test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation_controls:
        Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    pub(crate) backend_process_id: novarocks_types::BackendProcessId,
    pub(crate) runtime: &'a TypedScanRuntime,
    pub(crate) fragment_instance_id: UniqueId,
    pub(crate) query_options: &'a QueryOptions,
    pub(crate) stop: ConnectorStopView,
}

/// Why a compiled writer or finish could not be bound to its Task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledWriterBindingError {
    node: Option<usize>,
    source: ExecutionFailure,
}
impl CompiledWriterBindingError {
    fn at_failure(node: ProgramNodeId, source: ExecutionFailure) -> Self {
        Self {
            node: Some(node.index()),
            source,
        }
    }
    fn at_domain<E>(node: ProgramNodeId, source: E) -> Self
    where
        E: std::error::Error
            + novarocks_execution_contract::failure::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        Self::at_failure(node, ExecutionFailure::from_domain(source))
    }
    fn program_failure(source: ExecutionFailure) -> Self {
        Self { node: None, source }
    }
    pub const fn node(&self) -> Option<usize> {
        self.node
    }
    pub fn detail(&self) -> &str {
        self.source.detail()
    }
}
impl fmt::Display for CompiledWriterBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.node {
            Some(node) => write!(f, "compiled writer at local node {node}: {}", self.source),
            None => self.source.fmt(f),
        }
    }
}
impl std::error::Error for CompiledWriterBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for CompiledWriterBindingError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        self.source.failure_fact()
    }
}

#[derive(Debug)]
enum WriterBindingContractFailure {
    MissingRecipe,
    SummaryBinding(&'static str),
    DisplayRange {
        index: usize,
        source: std::num::TryFromIntError,
    },
    WrongBinding,
    Observation(novarocks_execution_contract::failure::BoundedDetail),
}
impl fmt::Display for WriterBindingContractFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingRecipe => f.write_str("the writer carries no provider write recipe"),
            Self::SummaryBinding(detail) => write!(f, "compiled summary binding refused: {detail}"),
            Self::DisplayRange { index, .. } => {
                write!(f, "local node index {index} exceeds the display identity")
            }
            Self::WrongBinding => {
                f.write_str("the decoded writer handle does not name the recipe's write binding")
            }
            Self::Observation(detail) => write!(f, "compiled writer observation refused: {detail}"),
        }
    }
}
impl std::error::Error for WriterBindingContractFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DisplayRange { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for WriterBindingContractFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::MissingRecipe => (FailureClass::Invariant, 9720),
            Self::SummaryBinding(_) => (FailureClass::Invariant, 9724),
            Self::DisplayRange { .. } => (FailureClass::Invariant, 9721),
            Self::WrongBinding => (FailureClass::Invariant, 9722),
            Self::Observation(_) => (FailureClass::Unexpected, 9723),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// Bind every compiled TableWriter and TableFinish of `program` to the Task.
/// Every refusal precedes registration; nothing is opened here.
pub(crate) fn bind_compiled_writers(
    program: &LocalProgram,
    task: &CompiledWriteTask<'_>,
    topology: &novarocks_execution_contract::descriptor::ExchangeTopology,
) -> Result<CompiledWriterBindings, CompiledWriterBindingError> {
    let mut bindings = CompiledWriterBindings::default();
    for (index, node) in program.graph().nodes().iter().enumerate() {
        let id = ProgramNodeId::new(index);
        match node.kind() {
            ProgramNodeKind::TableWriter { .. } => {
                let recipe = program.write_recipes().get(&id).ok_or_else(|| {
                    CompiledWriterBindingError::at_domain(
                        id,
                        WriterBindingContractFailure::MissingRecipe,
                    )
                })?;
                let writer = bind_compiled_writer(id, recipe, task)?;
                bindings
                    .bind_writer(id, writer)
                    .map_err(|source| CompiledWriterBindingError::at_failure(id, source))?;
            }
            ProgramNodeKind::TableFinish {
                inputs,
                input_roles,
                count_mode,
                ..
            } => {
                let mut finish = bind_compiled_finish(id, task)?;
                if *count_mode == novarocks_local_program::FinishCountMode::LogicalChanges {
                    let invalid = |detail| {
                        CompiledWriterBindingError::at_domain(
                            id,
                            WriterBindingContractFailure::SummaryBinding(detail),
                        )
                    };
                    let receiver = input_roles
                        .logical_summary
                        .and_then(|ordinal| inputs.get(ordinal as usize))
                        .and_then(|input| program.exchange_inputs().get(input))
                        .ok_or_else(|| invalid("the logical role has no compiled receiver"))?;
                    if receiver.role != novarocks_local_program::ProgramOutputRole::LogicalSummary
                        || receiver.delivery
                            != novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                    {
                        return Err(invalid(
                            "the compiled input is not a strict logical summary",
                        ));
                    }
                    let receiver_node = i32::try_from(receiver.receiver_node).map_err(|_| {
                        invalid("summary receiver node exceeds the native identity")
                    })?;
                    let inbound = topology
                        .inbound_node(
                            novarocks_execution_contract::descriptor::FragmentNodeId::new(
                                receiver_node,
                            ),
                        )
                        .ok_or_else(|| {
                            invalid("the summary receiver has no frozen inbound capability")
                        })?;
                    if inbound.delivery() != receiver.delivery
                        || inbound.sources().iter().any(|source| {
                            source.task().query_execution_id() != task.runtime.execution_id()
                        })
                    {
                        return Err(invalid(
                            "the summary source set has another attempt or delivery law",
                        ));
                    }
                    finish = finish
                        .with_logical_summary(receiver_node, Arc::from(inbound.sources()))
                        .map_err(|source| CompiledWriterBindingError::at_failure(id, source))?;
                }
                bindings
                    .bind_finish(id, finish)
                    .map_err(|source| CompiledWriterBindingError::at_failure(id, source))?;
            }
            _ => {}
        }
    }
    // The program's own law, checked again before the Task registers
    // anything: exactly one capability per writer-family node.
    bindings
        .validate(program)
        .map_err(CompiledWriterBindingError::program_failure)?;
    Ok(bindings)
}

fn display_id(id: ProgramNodeId) -> Result<i32, CompiledWriterBindingError> {
    i32::try_from(id.index()).map_err(|source| {
        CompiledWriterBindingError::at_domain(
            id,
            WriterBindingContractFailure::DisplayRange {
                index: id.index(),
                source,
            },
        )
    })
}

/// Bind one compiled writer's recipe as the Task's write capability.
pub(crate) fn bind_compiled_writer(
    id: ProgramNodeId,
    recipe: &ConnectorWriteRecipe,
    task: &CompiledWriteTask<'_>,
) -> Result<TableWriterRuntimeBinding, CompiledWriterBindingError> {
    let draft = recipe.draft();
    let node_id = display_id(id)?;

    // Only the single-handle cap: the query-wide unique-handle budget is the
    // frontend's, the only owner that sees the whole unique set.
    validate_writer_handle_bytes(draft.payload().payload().len())
        .map_err(|source| CompiledWriterBindingError::at_domain(id, source))?;
    let binding = task
        .runtime
        .catalog_write_execution(draft.binding().catalog_handle())
        .map_err(|source| CompiledWriterBindingError::at_domain(id, source))?;
    let handle = binding
        .handle_decoder()
        .decode_writer_handle_payload(draft.payload())
        .map_err(|source| CompiledWriterBindingError::at_domain(id, source))?;
    if handle.binding() != draft.binding() {
        return Err(CompiledWriterBindingError::at_domain(
            id,
            WriterBindingContractFailure::WrongBinding,
        ));
    }

    // The query and attempt are the identity this Task was admitted under,
    // never a plan fact: a replacement attempt never inherits a predecessor's
    // writer context.
    let execution_id = task.runtime.execution_id();
    let physical_template = TableWriterPhysicalContextTemplate::new(
        uuid_bytes(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        ),
        execution_id.attempt_id().get(),
        uuid_bytes(
            task.fragment_instance_id.high(),
            task.fragment_instance_id.low(),
        ),
        0,
    );
    let (_, query_expire) = query_expire_durations(Some(task.query_options));
    let request_context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        task.stop.clone(),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map(|context| context.with_storage_resolver(task.runtime.storage_resolver()))
    .map_err(|source| CompiledWriterBindingError::at_domain(id, source))?;
    let execution = Arc::new(ObservedConnectorWriteExecution::new(
        binding.execution(),
        execution_id,
        node_id,
        Arc::new({
            let observer = NativeConnectorWriteObservationPort::new(task.backend_process_id);
            #[cfg(novarocks_dev_controls)]
            let observer = if let Some(controls) = task.test_controls.as_ref() {
                observer.with_test_controls(Arc::clone(controls))
            } else {
                observer
            };
            observer
        }),
        #[cfg(novarocks_dev_controls)]
        task.observation_controls
            .as_ref()
            .map(|controls| controls.observation_for("writer-marker", execution_id))
            .transpose()
            .map_err(|detail| {
                CompiledWriterBindingError::at_domain(
                    id,
                    WriterBindingContractFailure::Observation(
                        novarocks_execution_contract::failure::BoundedDetail::truncating(&detail),
                    ),
                )
            })?
            .flatten(),
    ));
    let fragment_encoder =
        RoleBoundCommitFragmentEncoder::new(binding.fragment_encoder(), execution_id, node_id);
    #[cfg(novarocks_dev_controls)]
    let fragment_encoder = if let Some(controls) = task.test_controls.as_ref() {
        fragment_encoder.with_test_controls(Arc::clone(controls))
    } else {
        fragment_encoder
    };
    let fragment_encoder = Arc::new(fragment_encoder);
    let writer = TableWriterRuntimeBinding::try_new(
        handle,
        execution,
        physical_template,
        request_context,
        fragment_encoder,
    )
    .map_err(|source| CompiledWriterBindingError::at_domain(id, source))?;
    #[cfg(novarocks_dev_controls)]
    let writer = if let Some(controls) = task.test_controls.as_ref() {
        writer.with_aggregate_guard(Arc::new(QueryScopedTableWriteAggregateGuard::new(
            execution_id,
            node_id,
            Arc::clone(controls),
        )))
    } else {
        writer
    };
    Ok(writer)
}

/// Bind one compiled finish to the Task's canonical carrier validator.
pub(crate) fn bind_compiled_finish(
    id: ProgramNodeId,
    task: &CompiledWriteTask<'_>,
) -> Result<TableFinishRuntimeBinding, CompiledWriterBindingError> {
    let execution_id = task.runtime.execution_id();
    let node_id = display_id(id)?;
    let validator = RootCommitFragmentCarrierValidator::new(execution_id, node_id);
    #[cfg(novarocks_dev_controls)]
    let validator = if let Some(controls) = task.test_controls.as_ref() {
        validator.with_test_controls(Arc::clone(controls))
    } else {
        validator
    };
    let finish = TableFinishRuntimeBinding::new(Arc::new(validator));
    #[cfg(novarocks_dev_controls)]
    let finish = if let Some(controls) = task.test_controls.as_ref() {
        finish.with_aggregate_guard(Arc::new(QueryScopedTableWriteAggregateGuard::new(
            execution_id,
            node_id,
            Arc::clone(controls),
        )))
    } else {
        finish
    };
    Ok(finish)
}

/// The 16-byte form of a native `(high, low)` identity, in the same big-endian
/// halves its UUID rendering uses.
const fn uuid_bytes(high: i64, low: i64) -> [u8; 16] {
    let high = high.to_be_bytes();
    let low = low.to_be_bytes();
    [
        high[0], high[1], high[2], high[3], high[4], high[5], high[6], high[7], low[0], low[1],
        low[2], low[3], low[4], low[5], low[6], low[7],
    ]
}

#[cfg(test)]
#[path = "compiled_writer_binding_tests.rs"]
pub(crate) mod tests;
