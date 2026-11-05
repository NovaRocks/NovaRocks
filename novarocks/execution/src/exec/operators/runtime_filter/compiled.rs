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

//! Runtime-filter consumers of a compiled LocalProgram node.
//!
//! The shared consumer state owns bindings, subscriptions, the gate and row
//! masks independently of compiled key evaluation.
//! Each binding's key is the node's `RuntimeFilter { binding }` root, and every
//! driver evaluates it through its own `CompiledExpressionInstance`. The root
//! reads exactly its frozen input port, so a chunk is filtered as it arrives:
//! nothing is hydrated or retyped before a key is evaluated.

use std::sync::Arc;

use arrow::array::ArrayRef;
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
};

use super::{RuntimeFilterApplication, RuntimeFilterConsumerState, RuntimeFilterKeyProvider};
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::CompiledExpressionInstance;
use crate::exec::operators::compiled_expression::{
    RuntimeKernelControl, evaluate_all, evaluate_chunk_all, instances, root_value_type,
};
use crate::runtime::runtime_state::RuntimeErrorState;
use crate::runtime::task::{ExecutionFailure, ExecutionResult};
use crate::runtime_filter as execution;
use crate::runtime_filter::RuntimeFilterExecutionContract;

/// The runtime-filter consumers of one compiled node, shared by its drivers:
/// the consumer state and the key root of every binding, in binding order.
pub(crate) struct CompiledRuntimeFilterConsumers {
    state: RuntimeFilterConsumerState,
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    error: Arc<RuntimeErrorState>,
}

impl CompiledRuntimeFilterConsumers {
    /// `contracts` are the runtime contracts of `node`'s runtime-filter
    /// bindings in their frozen order; binding `i` is keyed by the node's
    /// `RuntimeFilter { binding: i }` root, whose static type must be exactly
    /// the binding's membership key type.
    #[cfg(test)]
    #[cfg(test)]
    pub(crate) fn try_new(
        owner: &'static str,
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        contracts: Vec<execution::RuntimeFilterConsumerContract>,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let contracts = contracts
            .into_iter()
            .enumerate()
            .map(|(index, contract)| {
                let ordinal = u32::try_from(index).map_err(|_| {
                    super::contract_failure("compiled runtime-filter binding count exceeds u32")
                })?;
                Ok((ordinal, contract))
            })
            .collect::<ExecutionResult<Vec<_>>>()?;
        Self::try_new_at_ordinals(owner, program, node, contracts, error)
    }

    /// A Scan splits ordered and membership consumers without changing any
    /// root's ordinal in the original complete binding list.
    pub(crate) fn try_new_at_ordinals(
        owner: &'static str,
        program: Arc<LocalProgram>,
        node: ProgramNodeId,
        contracts: Vec<(u32, execution::RuntimeFilterConsumerContract)>,
        error: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<Self> {
        let sites = contracts
            .iter()
            .map(|(binding, _)| ProgramExpressionRootSite::Node {
                node,
                role: ProgramNodeExpressionRole::RuntimeFilter { binding: *binding },
            })
            .collect::<Arc<[_]>>();
        let contracts = contracts
            .into_iter()
            .map(|(_, contract)| contract)
            .collect();
        let state = RuntimeFilterConsumerState::from_contracts(
            owner,
            contracts,
            |index, contract| {
                let site = sites[index];
                let key = root_value_type(&program, site)?;
                let RuntimeFilterExecutionContract::Membership(schema) = contract.contract() else {
                    return Err(super::contract_failure(format!(
                        "native {owner} runtime-filter binding_id={} requires a membership SetUnion contract",
                        contract.binding_id().get()
                    )));
                };
                if key.data_type() != schema.data_type() {
                    return Err(super::contract_failure(format!(
                        "compiled {owner} runtime-filter binding_id={} key root at local node {} has type {:?}, not its membership type {:?}",
                        contract.binding_id().get(),
                        node.index(),
                        key.data_type(),
                        schema.data_type()
                    )));
                }
                Ok(())
            },
        )?;
        Ok(Self {
            state,
            program,
            sites,
            error,
        })
    }

    /// The original membership gate/state is retained; only this direct
    /// ordered-scan route uses controlled row matching and source custody.
    pub(crate) fn driver_keys_controlled(
        &self,
        host: Arc<dyn novarocks_functions::AggregateStateAllocator>,
    ) -> CompiledRuntimeFilterKeys {
        let mut keys = self.driver_keys();
        keys.controlled_host = Some(host);
        keys
    }

    /// The consumer state every driver of the node shares.
    pub(crate) fn state(&self) -> &RuntimeFilterConsumerState {
        &self.state
    }

    /// The key roots one driver evaluates: its own instances, created on its
    /// first filtered chunk.
    pub(crate) fn driver_keys(&self) -> CompiledRuntimeFilterKeys {
        CompiledRuntimeFilterKeys {
            program: Arc::clone(&self.program),
            sites: Arc::clone(&self.sites),
            control: RuntimeKernelControl::new(Arc::clone(&self.error)),
            instances: None,
            controlled_host: None,
        }
    }
}

/// One driver's instances of a compiled consumer's key roots.
pub(crate) struct CompiledRuntimeFilterKeys {
    program: Arc<LocalProgram>,
    sites: Arc<[ProgramExpressionRootSite]>,
    control: RuntimeKernelControl,
    instances: Option<Vec<CompiledExpressionInstance>>,
    controlled_host: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
}

impl CompiledRuntimeFilterKeys {
    pub(crate) fn bind_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.control.bind_construction_factory(factory);
    }

    pub(crate) fn bind_runtime_state(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) {
        self.control.bind_runtime_memory(state);
    }
    pub(crate) fn bind_mem_tracker(
        &mut self,
        tracker: Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
        self.control.bind_mem_tracker(tracker);
    }
}
impl RuntimeFilterKeyProvider for CompiledRuntimeFilterKeys {
    fn apply_snapshot(
        &mut self,
        input: Chunk,
        array: &ArrayRef,
        snapshot: &execution::RuntimeFilterSnapshot,
    ) -> ExecutionResult<RuntimeFilterApplication> {
        let Some(host) = &self.controlled_host else {
            return super::apply_legacy_runtime_filter_snapshot(input, array, snapshot);
        };
        let outcome = execution::evaluator::evaluate_rows_controlled(
            snapshot.binding_id(),
            snapshot.logical_version(),
            snapshot.artifact_query().as_ref(),
            array,
            Arc::clone(host),
            &self.control,
        )?;
        let execution::RuntimeFilterRowEvaluation::Evaluated { mask, .. } =
            outcome.outcome().evaluation()
        else {
            return Ok(RuntimeFilterApplication::NotEvaluated(input));
        };
        let effect = outcome.effect_controlled(&self.control)?.ok_or_else(|| {
            ExecutionFailure::from(novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new(
                    "evaluated runtime-filter row outcome has no effect",
                ),
            ))
        })?;
        let owner = self.control.construction_owner(input.chunk_schema())?;
        let output = input.filter_ordered_scan_controlled(
            mask,
            Arc::clone(host),
            &self.control,
            owner.as_ref(),
        )?;
        // The borrowed mask and its original controlled stock stay together
        // through the synchronous creator; only the complete output escapes.
        Ok(RuntimeFilterApplication::Evaluated { output, effect })
    }

    /// The frozen root port is exact: the chunk is evaluated as delivered.
    fn prepare_input(&mut self, chunk: Chunk) -> ExecutionResult<Chunk> {
        Ok(chunk)
    }

    fn evaluate_key(&mut self, binding: usize, input: &Chunk) -> ExecutionResult<ArrayRef> {
        instances(
            &mut self.instances,
            &self.program,
            &self.sites,
            &self.control,
        )?;
        let site = *self.sites.get(binding).ok_or_else(|| {
            super::contract_failure("compiled runtime-filter binding index drifted")
        })?;
        let instance = self
            .instances
            .as_mut()
            .and_then(|instances| instances.get_mut(binding))
            .ok_or_else(|| {
                super::contract_failure("compiled runtime-filter key instance is missing")
            })?;
        if self.controlled_host.is_some() {
            evaluate_chunk_all(instance, site, input, &self.control)
        } else {
            evaluate_all(instance, site, input.record_batch(), &self.control)
        }
    }
}
