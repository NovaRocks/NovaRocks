// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original root read bindings, typed failures and producer-exit cuts.

use crate::api::{BoundedRootReadPort, NativeAttemptTopologyRequirement, QueryExecutionError};
use crate::execution_error::ExecutionFailurePresentation;
use novarocks_coordination_contract::AttemptFailureClass;
use novarocks_execution_contract::TaskIdentity;
use novarocks_workload_control::WorkError;
use std::sync::Arc;

/// Typed adapter failure and its attempt-level recovery class.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootResultFetchFailure {
    pub(in crate::coordination) error: QueryExecutionError,
    pre_ready_replacement: Option<crate::api::PreReadyProcessReplacementWitness>,
}
impl RootResultFetchFailure {
    pub fn from_input(input: crate::execution_error::ExecutionFailureInput) -> Self {
        Self {
            error: QueryExecutionError::from_execution(input),
            pre_ready_replacement: None,
        }
    }
    pub fn input(&self) -> &crate::execution_error::ExecutionFailureInput {
        self.error
            .execution_failure()
            .expect("typed constructor retains its original failure input")
    }
    pub const fn pre_ready_replacement(
        &self,
    ) -> Option<&crate::api::PreReadyProcessReplacementWitness> {
        self.pre_ready_replacement.as_ref()
    }
    pub fn topology_requirement(&self) -> NativeAttemptTopologyRequirement {
        self.pre_ready_replacement.as_ref().map_or_else(
            || self.input().topology_requirement(),
            |witness| {
                NativeAttemptTopologyRequirement::ExcludeProcess(
                    witness.participation().backend_process_id(),
                )
            },
        )
    }
    pub fn class(&self) -> AttemptFailureClass {
        self.input().attempt_class()
    }
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

/// One root stream relayed to its consumer without decode: the read port,
/// the frontier that decides each read, and the window alias every reply
/// retains until its delivery exits.
pub struct RootRelayBinding {
    pub port: Arc<dyn BoundedRootReadPort>,
    pub frontier: super::RootRelayFrontier,
    pub window: novarocks_workload_control::ResultWindowAlias,
    pub max_wait: std::time::Duration,
}

/// Minted only after this original pump consumed Root End, drained its
/// real delivery/read slots and obtained its accepted completion or exact Internal Ready. Moving this
/// token out is not P retirement: the original exit owner consumes it only
/// after the relay work and its captures have actually returned.
pub struct StreamedRowsComputedCut {
    source: crate::admitted_query_context::StreamedComputationCutHandle,
    _activation: novarocks_coordination_contract::AttemptActivationIdentity,
    _root: TaskIdentity,
    sealing: Result<(), WorkError>,
}
impl StreamedRowsComputedCut {
    pub(crate) fn new(
        source: crate::admitted_query_context::StreamedComputationCutHandle,
        activation: novarocks_coordination_contract::AttemptActivationIdentity,
        root: TaskIdentity,
    ) -> Self {
        let sealing = source.seal_producer();
        Self {
            source,
            _activation: activation,
            _root: root,
            sealing,
        }
    }

    pub(crate) fn is_for(
        &self,
        source: &crate::admitted_query_context::StreamedComputationCutHandle,
    ) -> bool {
        self.source.same_source(source)
    }
    pub async fn retire_after_pump_exit(self) -> Result<(), WorkError> {
        self.sealing.clone()?;
        let source = self.source.clone();
        source.retire_after_pump_exit(self).await
    }
}
