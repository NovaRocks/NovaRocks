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

//! Single governed cleanup source with separate output and actual exit.

use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use novarocks_spi::connector::{
    ConnectorCleanupAsyncExecution, ConnectorCleanupAsyncOutput as Output,
    ConnectorCleanupAsyncSource, ConnectorCleanupAsyncStart as Start, ConnectorMetadata,
    LakePublicationDisposition, ProviderMetadataCpuPort,
};

use super::*;
use crate::catalog::CatalogTableName;
use crate::catalog::admission::{CatalogAdmissionRequest, CatalogOperation, connector_unsupported};

type StepF = Pin<Box<dyn Future<Output = Result<Output, ConnectorError>> + Send>>;

struct Body {
    future: Option<StepF>,
    result: Option<Result<Output, ConnectorError>>,
    unproven: bool,
}
struct Original {
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    body: Mutex<Option<Body>>,
}
struct Loan<'a> {
    source: &'a Original,
    body: Option<Body>,
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut row = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(row.is_none(), "same original cleanup F");
        *row = self.body.take();
    }
}
impl Original {
    fn loan(&self) -> Result<Loan<'_>, ConnectorError> {
        let body = self
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .ok_or_else(|| internal("cleanup original is already borrowed"))?;
        Ok(Loan {
            source: self,
            body: Some(body),
        })
    }
}
fn poll_step(body: &mut Body, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
    if body.unproven {
        return Poll::Ready(Err(internal("cleanup original Future exit is unproven")));
    }
    if body.result.is_some() {
        return Poll::Ready(Ok(()));
    }
    let Some(future) = body.future.as_mut() else {
        return Poll::Ready(Err(invalid("cleanup source output was already consumed")));
    };
    body.unproven = true;
    let result = match future.as_mut().poll(cx) {
        Poll::Pending => {
            body.unproven = false;
            return Poll::Pending;
        }
        Poll::Ready(result) => result,
    };
    // Original output/effects are stable BEFORE potentially unwinding F Drop.
    body.result = Some(result);
    drop(body.future.take());
    body.unproven = false;
    Poll::Ready(Ok(()))
}
impl ConnectorCleanupAsyncSource for Original {
    fn poll_output(&self, cx: &mut Context<'_>) -> Poll<Result<Output, ConnectorError>> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        std::task::ready!(poll_step(body, cx))?;
        Poll::Ready(body.result.take().unwrap())
    }

    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        if body.unproven {
            return Poll::Ready(Err(internal("cleanup original Future exit is unproven")));
        }
        if body.future.is_some() {
            std::task::ready!(poll_step(body, cx))?;
        }
        self.cpu.poll_closed(cx)
    }

    fn publication_fact(&self) -> (Option<LakePublicationDisposition>, bool) {
        (None, false)
    }
}

pub(super) fn install_in(
    adapter: &IcebergCleanupMaintenanceAdapter,
    start: &mut Option<Start>,
    metadata: Arc<dyn ConnectorMetadata>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    target: &mut Option<ConnectorCleanupAsyncExecution>,
) -> Result<(), ConnectorError> {
    let input = start
        .as_ref()
        .ok_or_else(|| invalid("cleanup start is absent"))?;
    if target.is_some()
        || input.table.instance_id != adapter.key.instance_id
        || metadata.instance_id() != &adapter.key.instance_id
    {
        return Err(invalid(
            "cleanup start does not match exact original generation",
        ));
    }
    if input
        .context
        .io_admission()
        .and_then(std::sync::Weak::upgrade)
        .is_none()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cleanup requires its original MetadataIoAdmission",
        ));
    }
    if input.original.original_deadline() != input.context.deadline() {
        return Err(invalid("cleanup changed its original absolute deadline"));
    }
    let (metadata_key, metadata_type) =
        metadata.provider_metadata_cpu_author().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "cleanup metadata has no original CPU author",
            )
        })?;
    if metadata_key != adapter.key || metadata_type != adapter.metadata_cpu_source_type_id()? {
        return Err(invalid(
            "cleanup metadata CPU author belongs to another exact generation",
        ));
    }
    input.original.check_active().map_err(scope_error)?;
    let input = start.take().unwrap();
    let adapter = adapter.clone();
    let future = Box::pin(async move {
        input.original.check_active().map_err(scope_error)?;
        adapter
            .runtime
            .novarocks_catalog()
            .admit(&CatalogAdmissionRequest::new(
                CatalogOperation::RemoveOrphanFiles,
                CatalogTableName::new(input.table.namespace.as_ref(), input.table.table.as_ref()),
                input.context.initiation(),
            ))
            .map_err(connector_unsupported)?;
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "scanned orphan cleanup is unsupported",
        ))
    });
    *target = Some(ConnectorCleanupAsyncExecution::from_original(Arc::new(
        Original {
            cpu,
            body: Mutex::new(Some(Body {
                future: Some(future),
                result: None,
                unproven: false,
            })),
        },
    )));
    Ok(())
}

fn scope_error(error: novarocks_spi::connector::OriginalResultCheckError) -> ConnectorError {
    ConnectorError::compile_interruption(novarocks_type_contract::CompileControlError::from_domain(
        error,
    ))
}
