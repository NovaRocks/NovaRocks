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

//! Private mechanical dispatch to the SAME installed application originals.
//! Implementations grant no work, identity, settlement or local-exit authority.
use novarocks_coordination_contract::{
    AttemptActivationIdentity, MonotonicInstant, QueryExecutionError,
};
use novarocks_logical_execution_protocol::{effect::NativeEffect, input::NativeFact};
use novarocks_query_application::{
    api::logical_runtime::LogicalFuture,
    coordination::executor::{GovernanceExecutor, LogicalExecutors, NativeCleanupInventory},
};
use novarocks_types::QueryExecutionId;
use std::{sync::Arc, time::Instant};

pub(in crate::control_runtime::logical) trait LogicalFailureOriginals:
    Send + Sync
{
    fn governance(&self) -> Arc<GovernanceExecutor>;
    fn begin_failure(&self, error: QueryExecutionError);
    fn fail_one_pending_request(&self) -> bool;
    fn cleanup_inventory(&self) -> Result<NativeCleanupInventory, QueryExecutionError>;
    fn original_cleanup_deadline(&self, deadline: MonotonicInstant) -> Instant;
    fn execute_native(&self, effect: NativeEffect) -> LogicalFuture<'_, NativeFact>;
    fn close_registry_ingress_after_owner_failure(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
    fn adopt_pending_activation_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<Option<AttemptActivationIdentity>, QueryExecutionError>>;
    fn converge_dormant_for_owner_failure(
        &self,
        execution: QueryExecutionId,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
    fn retire_adopted_dormant_for_cleanup(
        &self,
        activation: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
    fn close_original_body_for_cleanup(&self)
    -> LogicalFuture<'_, Result<(), QueryExecutionError>>;
}
impl LogicalFailureOriginals for LogicalExecutors {
    fn governance(&self) -> Arc<GovernanceExecutor> {
        self.governance.clone()
    }
    fn begin_failure(&self, error: QueryExecutionError) {
        self.native.record_owner_failure(error.clone());
        self.consumer.begin_supervised_failure(error.clone());
        self.relay.begin_supervised_failure(error);
        self.cancel.stop();
    }
    fn fail_one_pending_request(&self) -> bool {
        self.consumer.fail_one_supervised_request() || self.relay.fail_one_supervised_request()
    }
    fn cleanup_inventory(&self) -> Result<NativeCleanupInventory, QueryExecutionError> {
        self.native.cleanup_inventory()
    }
    fn original_cleanup_deadline(&self, deadline: MonotonicInstant) -> Instant {
        self.native.original_cleanup_deadline(deadline)
    }
    fn execute_native(&self, effect: NativeEffect) -> LogicalFuture<'_, NativeFact> {
        self.native.execute(effect)
    }
    fn close_registry_ingress_after_owner_failure(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        self.native.close_registry_ingress_after_owner_failure()
    }
    fn adopt_pending_activation_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<Option<AttemptActivationIdentity>, QueryExecutionError>> {
        self.native.adopt_pending_activation_for_cleanup()
    }
    fn converge_dormant_for_owner_failure(
        &self,
        execution: QueryExecutionId,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        self.native.converge_dormant_for_owner_failure(execution)
    }
    fn retire_adopted_dormant_for_cleanup(
        &self,
        activation: AttemptActivationIdentity,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        self.native.retire_adopted_dormant_for_cleanup(activation)
    }
    fn close_original_body_for_cleanup(
        &self,
    ) -> LogicalFuture<'_, Result<(), QueryExecutionError>> {
        self.native.close_original_body_for_cleanup()
    }
}
