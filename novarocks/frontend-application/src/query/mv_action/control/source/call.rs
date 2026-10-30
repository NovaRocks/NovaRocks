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

//! The one catalog Call retains its inputs, receiving and child after business Ready.
use super::*;
type MutationFuture = Pin<Box<dyn Future<Output = Result<(), ConnectorError>> + Send>>;
pub(super) struct PublicationInput {
    lease: ConnectorCatalogMutationLease,
    pub(super) operation: ConnectorCatalogMutationOperation,
    operation_id: ConnectorMutationOperationId,
    context: ConnectorRequestContext,
}
#[derive(Default)]
pub(super) struct MutationCall {
    future: Option<MutationFuture>,
    pub(super) result: Option<Result<(), ConnectorError>>,
    pub(super) busy: bool,
    pub(super) exited: bool,
}
pub(in crate::query::mv_action::control) struct CatalogCall {
    pub(super) input: OnceLock<Arc<PublicationInput>>,
    pub(in crate::query::mv_action::control) receiving: Arc<ConnectorCatalogMutationReceiving>,
    pub(super) state: Mutex<MutationCall>,
}
struct Loan<'a> {
    source: &'a CatalogCall,
    future: Option<MutationFuture>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let mut state = self.source.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                state.future.is_none(),
                "same original catalog Future receiver"
            );
            state.future = Some(future);
        }
    }
}
impl CatalogCall {
    pub(super) fn new() -> Self {
        Self {
            input: OnceLock::new(),
            receiving: Arc::new(ConnectorCatalogMutationReceiving::new()),
            state: Mutex::new(MutationCall::default()),
        }
    }
    pub(super) fn started(&self) -> bool {
        self.input.get().is_some()
    }
    pub(super) fn start(
        &self,
        lease: ConnectorCatalogMutationLease,
        operation_id: ConnectorMutationOperationId,
        operation: ConnectorCatalogMutationOperation,
        context: ConnectorRequestContext,
    ) {
        let original = Arc::new(PublicationInput {
            lease,
            operation_id,
            operation,
            context,
        });
        assert!(
            self.input.set(original.clone()).is_ok(),
            "one catalog original request"
        );
        let receiving = self.receiving.clone();
        let future = Box::pin(async move {
            crate::connector::mutation::dispatch_catalog_mutation_once_with_lease_async_in(
                &original.lease,
                original.operation_id,
                original.operation.clone(),
                original.context.clone(),
                &receiving,
            )
            .await
        });
        self.state.lock().unwrap_or_else(|p| p.into_inner()).future = Some(future);
    }
    pub(in crate::query::mv_action::control) fn poll_progress(&self, cx: &mut Context<'_>) {
        let future = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.busy || state.exited {
                return;
            }
            let Some(future) = state.future.take() else {
                return;
            };
            state.busy = true;
            future
        };
        let mut loan = Loan {
            source: self,
            future: Some(future),
        };
        match loan.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                state.future = loan.future.take();
                state.busy = false;
            }
            Poll::Ready(raw) => {
                self.state.lock().unwrap_or_else(|p| p.into_inner()).result = Some(raw);
                // SPI already retained raw and validation before child Drop;
                // this outer result also precedes its original Future Drop.
                drop(loan.future.take());
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                state.exited = true;
                state.busy = false;
            }
        }
    }
}
#[cfg(test)]
#[path = "call_tests.rs"]
pub(in crate::query::mv_action::control) mod tests;
