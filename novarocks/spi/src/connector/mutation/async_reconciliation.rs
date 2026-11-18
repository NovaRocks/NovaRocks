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

//! Asynchronous reconciliation retains the actual original mutation identity.

use super::*;

impl ConnectorCatalogMutationSession {
    /// Observe the original operation once without executing it again.
    ///
    /// A fresh receiving belongs to this reconciliation call. The caller keeps
    /// both original calls and their independent raw outcomes until actual exit.
    /// This method supplies no replay policy and never reacquires a generation.
    pub async fn reconcile_async_in(
        &self,
        original: &ConnectorCatalogMutationRequest,
        request: ConnectorCatalogMutationReconcileRequest,
        receiving: &ConnectorCatalogMutationReceiving,
    ) -> Result<(), ConnectorError> {
        receiving.install_request(self, original)?;
        let admission = self
            .validate_request(original)
            .and_then(|()| self.validate_evidence(&request.evidence))
            .and_then(|()| {
                if request.evidence.operation_id() != original.operation_id
                    || request.evidence.operation_kind() != original.operation.kind()
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "reconciliation evidence differs from the original mutation identity",
                    ));
                }
                Ok(())
            });
        if let Err(error) = admission {
            receiving.reject_request(error.clone());
            return Err(error);
        }
        receiving.enter_factory();
        let future = self.mutation.reconcile_async(request, receiving);
        receiving.leave_pending();
        let mut child = CatalogMutationChild {
            future: Some(future),
            receiving,
        };
        let outcome = std::future::poll_fn(|cx| child.poll(cx)).await;
        receiving.publish_raw(outcome);
        receiving.validate_raw(self);
        drop(child);
        receiving.completed_result()
    }
}
