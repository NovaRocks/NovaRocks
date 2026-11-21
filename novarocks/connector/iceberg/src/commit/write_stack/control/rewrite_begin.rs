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

//! Closed Rewrite admission continuation in the original WriteControl.
use super::*;
use novarocks_spi::connector::{
    ConnectorOriginalResultScope, ConnectorRewriteWriteBeginSource, ProviderMetadataCpuPort,
    RewriteCpuInput, RewriteCpuPort, RewritePlanCpuSource,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

type OriginalFuture =
    Pin<Box<dyn Future<Output = Result<ConnectorWriteSessionPlan, ConnectorError>> + Send>>;
struct ObserveRequest {
    namespace: String,
    table: String,
    context: ConnectorRequestContext,
}

enum IoState {
    Idle,
    Observe(Box<ObserveRequest>),
    Observed(Result<crate::loaded_table::IcebergPhysicalTable, String>),
}
struct IoBody {
    state: IoState,
}
pub(super) struct RewriteBeginIo {
    control: Arc<IcebergWriteSessionControl>,
    capture: ConnectorOriginalResultScope,
    body: Mutex<Option<IoBody>>,
}
struct IoLoan<'a> {
    original: &'a RewriteBeginIo,
    body: Option<IoBody>,
}
impl<'a> IoLoan<'a> {
    fn take(original: &'a RewriteBeginIo) -> Self {
        let body = original
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        assert!(body.is_some(), "one original WriteBegin I/O loan");
        Self { original, body }
    }
}
impl Drop for IoLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self.original.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same WriteBegin I/O slot");
        *slot = self.body.take();
    }
}
impl RewriteBeginIo {
    pub(super) fn formation_scope(
        &self,
    ) -> Result<super::rewrite_scope::RewriteBeginScope<'_>, ConnectorError> {
        super::rewrite_scope::RewriteBeginScope::new(&self.capture)
            .map_err(crate::distributed_rewrite::async_plan::formation_failure)
    }

    pub(super) async fn observe(
        &self,
        namespace: &str,
        table: &str,
        context: &ConnectorRequestContext,
    ) -> Result<crate::loaded_table::IcebergPhysicalTable, String> {
        std::future::poll_fn(|_| {
            let mut loan = IoLoan::take(self);
            let body = loan.body.as_mut().unwrap();
            match &body.state {
                IoState::Idle => {
                    body.state = IoState::Observe(Box::new(ObserveRequest {
                        namespace: namespace.to_owned(),
                        table: table.to_owned(),
                        context: context.clone(),
                    }));
                    Poll::Pending
                }
                IoState::Observe(_) => Poll::Pending,
                IoState::Observed(_) => {
                    let IoState::Observed(value) =
                        std::mem::replace(&mut body.state, IoState::Idle)
                    else {
                        unreachable!()
                    };
                    Poll::Ready(value)
                }
                _ => panic!("WriteBegin observation crossed its original continuation"),
            }
        })
        .await
    }
    async fn fulfill(&self) -> Result<(), ConnectorError> {
        let mut loan = IoLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        match &mut body.state {
            IoState::Observe(request) => {
                let ObserveRequest {
                    namespace,
                    table,
                    context,
                } = request.as_mut();
                let value = self
                    .control
                    .runtime
                    .observe_table_classified_for_request_with_snapshots_async(
                        namespace,
                        table,
                        context,
                        crate::catalog::SnapshotLoading::All,
                    )
                    .await
                    .map_err(|(_, message)| message);
                body.state = IoState::Observed(value);
            }
            _ => return Err(internal("WriteBegin pure continuation has no I/O request")),
        }
        Ok(())
    }
}
struct BeginBody {
    future: Option<OriginalFuture>,
    result: Option<Result<ConnectorWriteSessionPlan, ConnectorError>>,
    started: bool,
    actual_joined: bool,
}
pub(super) struct RewriteBeginOriginal {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    io: Arc<RewriteBeginIo>,
    body: Mutex<Option<BeginBody>>,
}
struct BodyLoan<'a> {
    original: &'a RewriteBeginOriginal,
    body: Option<BeginBody>,
}
impl<'a> BodyLoan<'a> {
    fn take(original: &'a RewriteBeginOriginal) -> Self {
        let body = original
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        assert!(body.is_some(), "one original WriteBegin CPU loan");
        Self { original, body }
    }
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self.original.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same WriteBegin CPU slot");
        *slot = self.body.take();
    }
}
impl RewritePlanCpuSource for RewriteBeginOriginal {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    fn run_plan(&self) {
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        assert!(
            !body.started && body.result.is_none(),
            "one admitted WriteBegin CPU turn"
        );
        body.started = true;
        let mut cx = Context::from_waker(Waker::noop());
        if let Poll::Ready(result) = body
            .future
            .as_mut()
            .expect("same original begin F")
            .as_mut()
            .poll(&mut cx)
        {
            body.result = Some(result);
            // Receive first. If original Drop panics, the restoration loan
            // retains result and the finite CPU source reports unproven exit.
            drop(body.future.take());
        }
    }
}
struct RewriteBeginSource {
    original: Arc<RewriteBeginOriginal>,
}
impl RewriteBeginSource {
    async fn prepare_original(&self, cpu: &dyn RewriteCpuPort) -> Result<(), ConnectorError> {
        loop {
            let mut input = Some(RewriteCpuInput(self.original.clone()));
            cpu.submit_in(&mut input)?;
            std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
            {
                let mut loan = BodyLoan::take(&self.original);
                let body = loan.body.as_mut().unwrap();
                if body.result.is_some() {
                    body.actual_joined = true;
                    return Ok(());
                }
                // Only the same successful actual CPU/forwarder join enables
                // another turn of this exact original continuation.
                body.started = false;
            }
            self.original.io.fulfill().await?;
        }
    }
    async fn close_original(&self, cpu: &dyn RewriteCpuPort) -> Result<(), ConnectorError> {
        std::future::poll_fn(|cx| cpu.poll_closed(cx)).await?;
        let mut loan = BodyLoan::take(&self.original);
        let body = loan.body.as_mut().unwrap();
        // A pending pure reader has no external mutation. Its only physical
        // I/O is in the retained parent driver, which must exit before close.
        drop(body.future.take());
        Ok(())
    }
}
impl ConnectorRewriteWriteBeginSource for RewriteBeginSource {
    fn prepare<'a>(
        &'a self,
        cpu: &'a dyn RewriteCpuPort,
        _metadata_cpu: &'a Arc<dyn ProviderMetadataCpuPort>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConnectorError>> + Send + 'a>> {
        Box::pin(self.prepare_original(cpu))
    }
    fn take_plan_in(
        &self,
        target: &mut Option<Result<ConnectorWriteSessionPlan, ConnectorError>>,
    ) -> Result<(), ConnectorError> {
        if target.is_some() {
            return Err(invalid("WriteBegin plan receiver is occupied"));
        }
        let mut loan = BodyLoan::take(&self.original);
        let body = loan.body.as_mut().unwrap();
        if !body.actual_joined || body.future.is_some() || body.result.is_none() {
            return Err(invalid("WriteBegin original result or exit is not ready"));
        }
        *target = body.result.take();
        Ok(())
    }
    fn close<'a>(
        &'a self,
        cpu: &'a dyn RewriteCpuPort,
    ) -> Pin<Box<dyn Future<Output = Result<(), ConnectorError>> + Send + 'a>> {
        Box::pin(self.close_original(cpu))
    }
}
async fn original_begin(
    control: Arc<IcebergWriteSessionControl>,
    request: ConnectorWriteBeginRequest,
    io: Arc<RewriteBeginIo>,
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    control
        .begin_write_original_with_rewrite_io(request, Some(&io))
        .await
}
pub(super) fn prepare_in(
    control: Arc<IcebergWriteSessionControl>,
    request: &mut Option<ConnectorWriteBeginRequest>,
    capture: ConnectorOriginalResultScope,
    receiving: &mut Option<Arc<dyn ConnectorRewriteWriteBeginSource>>,
) -> Result<(), ConnectorError> {
    if receiving.is_some() {
        return Err(invalid("Rewrite begin source receiver is occupied"));
    }
    let value = request
        .as_ref()
        .ok_or_else(|| invalid("Rewrite begin request is absent"))?;
    if !matches!(
        value.flavor,
        ConnectorWriteSessionFlavor::DistributedRewrite { .. }
    ) {
        return Err(invalid(
            "Rewrite begin source requires its exact distributed rewrite flavor",
        ));
    }
    validate_context(&value.context)?;
    if capture.original_deadline() != value.context.deadline()
        || value
            .context
            .io_admission()
            .and_then(std::sync::Weak::upgrade)
            .is_none()
    {
        return Err(invalid(
            "Rewrite begin requires its exact original deadline and live MIO port",
        ));
    }
    capture
        .check_active()
        .map_err(|e| crate::distributed_rewrite::async_plan::formation_failure(e.into()))?;
    let io = Arc::new(RewriteBeginIo {
        control: control.clone(),
        capture,
        body: Mutex::new(Some(IoBody {
            state: IoState::Idle,
        })),
    });
    let original = Arc::new(RewriteBeginOriginal {
        descriptor: control.descriptor.clone(),
        incarnation: control.key.incarnation,
        io: io.clone(),
        body: Mutex::new(Some(BeginBody {
            future: Some(Box::pin(original_begin(
                control,
                request.take().expect("preflighted original request"),
                io,
            ))),
            result: None,
            started: false,
            actual_joined: false,
        })),
    });
    *receiving = Some(Arc::new(RewriteBeginSource { original }));
    Ok(())
}
