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

//! Stable receiving for one original metadata capture Future and CPU Source.

use novarocks_query_application::api::StatementNativeHealthFailure;
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorError, ConnectorErrorKind, ConnectorProcessLocalPayload,
    ConnectorTableObjectBinding, ConnectorTableObjectCaptureRequest, ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
};

type Raw = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>;
type CaptureFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
struct Inputs {
    planning_pin: ConnectorControlPlanningPin,
    port: Arc<dyn ProviderMetadataCpuPort>,
    request: ConnectorTableObjectCaptureRequest,
}
#[derive(Default)]
struct Call {
    active: bool,
    inputs: Option<Arc<Inputs>>,
    future: Option<CaptureFuture>,
    owner: Option<ConnectorProcessLocalPayload>,
    raw: Option<Raw>,
    in_foreign: bool,
    exited: bool,
    owners_retired: bool,
}
pub(crate) struct MetadataCapture {
    call: Mutex<Call>,
    health: StatementNativeHealthFailure,
    occupied_message: &'static str,
    cpu_exit_message: &'static str,
}
// The original F owns this temporary loan, and is itself retained by the
// receiving header. Weak receiver edges prevent a header/F custody cycle.
struct OwnerLoan {
    receiver: Weak<MetadataCapture>,
    owner: Option<ConnectorProcessLocalPayload>,
}
impl Drop for OwnerLoan {
    fn drop(&mut self) {
        let Some(receiver) = self.receiver.upgrade() else {
            // The receiving header itself is being destroyed. Its scheduled
            // job still has the original QA retirement owner; this Drop emits
            // no completion/exit fact and must not panic during another unwind.
            return;
        };
        let mut call = receiver.call.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            call.owner.is_none(),
            "one original metadata Source receiver"
        );
        call.owner = self.owner.take();
    }
}
struct FutureLoan<'a> {
    receiver: &'a MetadataCapture,
    future: Option<CaptureFuture>,
}
impl Drop for FutureLoan<'_> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let mut call = self.receiver.call.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                call.future.is_none(),
                "same original capture Future receiver"
            );
            call.future = Some(future);
        }
    }
}
impl MetadataCapture {
    pub(crate) fn new(
        health: StatementNativeHealthFailure,
        occupied_message: &'static str,
        cpu_exit_message: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            call: Mutex::new(Call::default()),
            health,
            occupied_message,
            cpu_exit_message,
        })
    }
    fn install(
        self: &Arc<Self>,
        lease: &ConnectorControlPlanningPin,
        port: Arc<dyn ProviderMetadataCpuPort>,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<(), ConnectorError> {
        let inputs = Arc::new(Inputs {
            planning_pin: lease.clone(),
            port,
            request,
        });
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        if call.active || call.in_foreign {
            drop(call);
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                self.occupied_message,
            ));
        }
        // No factory/foreign poll occurs while this mutex is held.
        let receiver = Arc::downgrade(self);
        let original_inputs = Arc::clone(&inputs);
        let future = Box::pin(async move {
            let mut owner = OwnerLoan {
                receiver: receiver.clone(),
                owner: None,
            };
            let mut original = original_inputs
                .planning_pin
                .binding()
                .metadata()
                .capture_table_object_with_snapshot_cpu_in(
                    original_inputs.request.clone(),
                    &original_inputs.port,
                    &mut owner.owner,
                );
            std::future::poll_fn(|cx| match original.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(raw) => {
                    let receiver = receiver.upgrade().expect("same original Current receiving");
                    let mut call = receiver.call.lock().unwrap_or_else(|p| p.into_inner());
                    assert!(call.raw.is_none(), "one original capture result");
                    call.raw = Some(raw);
                    Poll::Ready(())
                }
            })
            .await;
            // Actual raw result precedes both Provider F and Source return.
            drop(original);
            drop(owner);
        });
        *call = Call {
            active: true,
            inputs: Some(inputs),
            future: Some(future),
            ..Call::default()
        };
        Ok(())
    }
    pub(crate) fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        let future = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.in_foreign {
                return Poll::Pending;
            }
            if !call.active || call.exited {
                return Poll::Ready(());
            }
            let future = call.future.take().expect("same original Current capture F");
            call.in_foreign = true;
            future
        };
        let mut loan = FutureLoan {
            receiver: self,
            future: Some(future),
        };
        match loan.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                call.future = loan.future.take();
                call.in_foreign = false;
                Poll::Pending
            }
            Poll::Ready(()) => {
                // raw is already stable. Panic in F Drop retains that raw and
                // sticky unproven exit; the same poisoned F is not repolled.
                drop(loan.future.take());
                let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                call.exited = true;
                call.in_foreign = false;
                Poll::Ready(())
            }
        }
    }
    pub(crate) fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        std::task::ready!(self.poll_progress(cx));
        let retired = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.in_foreign {
                return Poll::Pending;
            }
            if !call.active || call.owners_retired {
                return Poll::Ready(());
            }
            assert!(
                call.exited,
                "actual capture F exit before Source retirement"
            );
            call.in_foreign = true;
            (call.owner.take(), call.inputs.take())
        };
        // All unique SDK/port/lease destructors remain outside the mutex. The
        // actual raw result is still retained if any one of them panics.
        drop(retired);
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        call.owners_retired = true;
        call.in_foreign = false;
        Poll::Ready(())
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        let (unproven, port) = {
            let call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            (
                call.in_foreign,
                call.inputs.as_ref().map(|i| Arc::clone(&i.port)),
            )
        };
        if unproven {
            return Some(self.health.clone());
        }
        port.and_then(|port| port.local_failure()).map(|cause| {
            StatementNativeHealthFailure::actual_exit_unproven(self.cpu_exit_message, cause)
        })
    }
    pub(crate) async fn capture_with_port(
        self: &Arc<Self>,
        lease: &ConnectorControlPlanningPin,
        port: Arc<dyn ProviderMetadataCpuPort>,
        request: ConnectorTableObjectCaptureRequest,
    ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
        self.install(lease, port, request)?;
        std::future::poll_fn(|cx| self.poll_closed(cx)).await;
        let raw = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            assert!(
                call.exited && call.owners_retired && !call.in_foreign,
                "only actual capture exit hands off metadata"
            );
            let raw = call.raw.take().expect("original capture result");
            call.active = false;
            raw
        };
        raw.map(|(binding, _snapshot)| binding)
    }
}
