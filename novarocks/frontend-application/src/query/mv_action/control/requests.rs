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

//! The same DROP guard keeps this request choice; no physical owner is replaced.
use novarocks_state_store_api::{
    StateStoreExecutionContext, StateStoreExecutionFailure, StateStoreExecutionRequests,
    StateStoreReadExecution,
};
use std::sync::{Arc, OnceLock};

pub(super) struct DropProjectionRequests {
    business: Arc<dyn StateStoreExecutionRequests>,
    obligation: OnceLock<Arc<dyn StateStoreExecutionRequests>>,
}
impl DropProjectionRequests {
    pub(super) fn new(business: Arc<dyn StateStoreExecutionRequests>) -> Self {
        Self {
            business,
            obligation: OnceLock::new(),
        }
    }
    pub(super) fn selected(&self) -> bool {
        self.obligation.get().is_some()
    }
    pub(super) fn select_applied(
        &self,
        receiving: &novarocks_spi::connector::ConnectorCatalogMutationReceiving,
        io: &Arc<crate::query_execution::maintenance::io_admission::MetadataIoAdmission>,
        root: &novarocks_workload_control::WorkScope,
        owner: &crate::state_store::execution::StateStoreOriginalOwner,
        duration: std::time::Duration,
    ) -> Result<
        novarocks_spi::connector::ConnectorRequestContext,
        novarocks_query_application::session_error::QueryServiceError,
    > {
        use novarocks_query_application::session_error::{
            QueryServiceError, QueryServiceErrorKind,
        };
        let invalid = |message| QueryServiceError::new(QueryServiceErrorKind::Internal, message);
        let context = {
            let observation = receiving.observation();
            if observation.applied_receipt().is_none() {
                return Err(invalid(
                    "MV continuation requires its original Applied receipt",
                ));
            }
            observation.finalization_context().cloned()
        }
        .ok_or_else(|| invalid("Applied MV mutation has no original finite finalization context"))?
        .map_err(|error| {
            QueryServiceError::from_failure_cause(
                novarocks_failure_contract::SharedFailureCause::new(error),
            )
        })?;
        if self.obligation.get().is_some() {
            return Err(invalid("MV continuation was already selected"));
        }
        let token = io
            .existing_metadata_obligation(root, &context)
            .map_err(QueryServiceError::from_failure_cause)?;
        let requests = owner
            .existing_obligation_requests(duration, token)
            .map_err(QueryServiceError::from_failure_cause)?;
        // No borrowed transaction is changed. Each subsequent independent
        // transaction still derives its own deadline under this fixed cap.
        assert!(
            self.obligation.set(requests).is_ok(),
            "one original DROP continuation selection"
        );
        Ok(context)
    }
    fn current(&self) -> &dyn StateStoreExecutionRequests {
        self.obligation.get().unwrap_or(&self.business).as_ref()
    }
}
impl StateStoreReadExecution for DropProjectionRequests {
    fn read_transaction_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        self.current().read_transaction_context()
    }
}
impl StateStoreExecutionRequests for DropProjectionRequests {
    fn write_operation_context(
        &self,
    ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
        self.current().write_operation_context()
    }
}
