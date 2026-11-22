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
struct BeginBody {
    future: Option<OriginalFuture>,
    result: Option<Result<ConnectorWriteSessionPlan, ConnectorError>>,
    started: bool,
    actual_joined: bool,
}
pub(super) struct RewriteBeginOriginal {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    _capture: ConnectorOriginalResultScope,
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
        let result = match body
            .future
            .as_mut()
            .expect("same original begin F")
            .as_mut()
            .poll(&mut cx)
        {
            Poll::Ready(result) => result,
            Poll::Pending => Err(internal("Frozen Rewrite begin suspended its pure CPU work")),
        };
        body.result = Some(result);
        // Retain the result in the original slot before its future exits.
        drop(body.future.take());
    }
}
struct RewriteBeginSource {
    original: Arc<RewriteBeginOriginal>,
}
impl RewriteBeginSource {
    async fn prepare_original(&self, cpu: &dyn RewriteCpuPort) -> Result<(), ConnectorError> {
        let mut input = Some(RewriteCpuInput(self.original.clone()));
        cpu.submit_in(&mut input)?;
        std::future::poll_fn(|cx| cpu.poll_finished(cx)).await?;
        let mut loan = BodyLoan::take(&self.original);
        let body = loan.body.as_mut().unwrap();
        if body.result.is_none() {
            return Err(internal("Frozen Rewrite begin has no CPU result"));
        }
        body.actual_joined = true;
        Ok(())
    }
    async fn close_original(&self, cpu: &dyn RewriteCpuPort) -> Result<(), ConnectorError> {
        std::future::poll_fn(|cx| cpu.poll_closed(cx)).await?;
        let mut loan = BodyLoan::take(&self.original);
        let body = loan.body.as_mut().unwrap();
        // Retire an unstarted work item after the original CPU port closes.
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
    capture: ConnectorOriginalResultScope,
) -> Result<ConnectorWriteSessionPlan, ConnectorError> {
    control
        .begin_write_original_with_rewrite_scope(request, Some(&capture))
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
    let original = Arc::new(RewriteBeginOriginal {
        descriptor: control.descriptor.clone(),
        incarnation: control.key.incarnation,
        _capture: capture.clone(),
        body: Mutex::new(Some(BeginBody {
            future: Some(Box::pin(original_begin(
                control,
                request.take().expect("preflighted original request"),
                capture,
            ))),
            result: None,
            started: false,
            actual_joined: false,
        })),
    });
    *receiving = Some(Arc::new(RewriteBeginSource { original }));
    Ok(())
}
