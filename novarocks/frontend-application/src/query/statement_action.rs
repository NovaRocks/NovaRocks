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

//! Stable result receiving and UI observation of the original statement source.

use super::*;
use novarocks_query_application::session_control::{
    GovernedQueryStatementOwner, GovernedStatementAction, StatementActionReady,
    StatementActionReceipt, StatementActionWaitError,
};

/// Exactly one receiving field in the existing session. The tail cannot erase
/// the installed owner/result if its callback panics.
#[derive(Default)]
pub(super) struct StatementReadyReceiver {
    ready: Mutex<Option<StatementActionReady>>,
}
impl StatementReadyReceiver {
    fn install(&self, receipt: StatementActionReceipt) -> Result<(), QueryServiceError> {
        let installed = {
            let mut ready = self.ready.lock().unwrap_or_else(|p| p.into_inner());
            receipt.take_ready_in(&mut ready)
        };
        let tail =
            installed.map_err(|_receipt| internal_error("statement receiving slot is occupied"))?;
        tail.finish()
            .map_err(|e| internal_error(format!("statement progress notification refused: {e:?}")))
    }
    fn take_protocol(
        &self,
        cancellation: &QueryCancellationView,
    ) -> Result<StatementResult, QueryServiceError> {
        let original_error = {
            let ready = self.ready.lock().unwrap_or_else(|p| p.into_inner());
            ready
                .as_ref()
                .and_then(|ready| ready.result.as_ref().err())
                .cloned()
        };
        // An eligible observation may accept an expired Root deadline and
        // invoke its callbacks. The original Err and owner stay in this stable
        // receiver while reconciliation runs outside its mutex. Success never
        // observes cancellation here.
        let reconciled = original_error
            .as_ref()
            .map(|error| error.clone().reconcile_local_stop_observation(cancellation));
        let mut output = None;
        {
            let mut ready = self.ready.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(original) = original_error.as_ref() {
                let current = ready.as_mut().and_then(|ready| ready.result.as_mut().err());
                let Some(current) = current.filter(|current| *current == original) else {
                    return Err(internal_error(
                        "statement receiving error changed during reconciliation",
                    ));
                };
                *current = reconciled.expect("same observed business error");
            }
            StatementActionReady::transfer_protocol_in(&mut ready, &mut output)
                .map_err(internal_error)?;
        }
        Ok(output.expect("silent transfer installed original output"))
    }
}

impl FrontendQuerySession {
    pub(super) async fn execute_observed_statement_action(
        &self,
        statement: GovernedQueryStatementOwner,
        action: Box<dyn GovernedStatementAction>,
    ) -> Result<StatementResult, QueryServiceError> {
        let cancellation = QueryCancellationView::governed(
            statement.cancellation().clone(),
            statement.timeout_ms(),
        );
        let mut owner = Some(statement);
        let mut action = Some(action);
        let mut waiter = None;
        let tail = match self.service.query_control.publish_statement_action_in(
            &mut owner,
            &mut action,
            &mut waiter,
        ) {
            Ok(tail) => tail,
            Err(error) => {
                return Ok(self.governed_typed_error(
                    internal_error(format!("statement source publication refused: {error:?}")),
                    owner.take().expect("refusal preserves original owner"),
                ));
            }
        };
        if let Err(error) = tail.finish() {
            return Ok(self.governed_typed_error(
                internal_error(format!("statement source publication refused: {error:?}")),
                owner.take().expect("refusal preserves original owner"),
            ));
        }
        let receipt = match waiter.as_mut().expect("published original observer").await {
            Ok(receipt) => receipt,
            Err(StatementActionWaitError::Stopped { external_effect }) => {
                let error = match cancellation.reason() {
                    Some(reason) => cancellation_error(reason),
                    None => internal_error("statement stopped before delivery"),
                };
                return Err(match external_effect {
                    Some(disposition) => error.attach_external_effect_disposition(disposition),
                    None => error,
                });
            }
            Err(StatementActionWaitError::Failed(failure)) => {
                let error = internal_error(
                    "statement action failed; original cause retained by supervisor",
                );
                return Err(match failure.external_effect_disposition() {
                    Some(disposition) => error.attach_external_effect_disposition(disposition),
                    None => error,
                });
            }
        };
        self.statement_ready.install(receipt)?;
        self.statement_ready.take_protocol(&cancellation)
    }
}
