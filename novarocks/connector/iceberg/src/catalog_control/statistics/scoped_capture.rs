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

//! Stable custody for one original metadata observation and binding CPU phase.
use super::IcebergMetadataCpuOriginal;
use crate::metadata::{IcebergMetadata, IcebergPreparedTableFacts};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMetadata, ConnectorTableObjectBinding,
    ConnectorTableObjectCaptureRequest, ConnectorTablePreparation, ConnectorTableRequest,
    ProviderMetadataCpuInput, ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
};

type Loader = Pin<Box<dyn Future<Output = ()> + Send>>;
type CaptureResult = Result<(ConnectorTableObjectBinding, Option<i64>), ConnectorError>;
struct Body {
    loader: Option<Loader>,
    loader_in_foreign: bool,
    loader_exited: bool,
    input: Option<ProviderMetadataCpuInput>,
    submit_in_foreign: bool,
    finished: Option<Result<(), ConnectorError>>,
    closed: Option<Result<(), ConnectorError>>,
}
/// The actual caller's opaque receiver holds this header before first poll.
/// Its Source fields survive a dropped borrowed waiter and an unwinding poll.
/// Progress still belongs to the caller's SAME retained original Future.
pub(crate) struct MetadataCaptureOriginal {
    provider: IcebergMetadata,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    binding_source: OnceLock<Arc<IcebergMetadataCpuOriginal>>,
    loaded: Mutex<Option<Result<ConnectorTablePreparation, ConnectorError>>>,
    body: Mutex<Option<Body>>,
}
struct Loan<'a> {
    source: &'a MetadataCaptureOriginal,
    body: Option<Body>,
}
impl<'a> Loan<'a> {
    fn take(source: &'a MetadataCaptureOriginal) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original metadata capture loan");
        Self { source, body }
    }
    fn body(&mut self) -> &mut Body {
        self.body.as_mut().expect("same original capture body")
    }
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut target = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(target.is_none(), "same original metadata capture receiver");
        *target = self.body.take();
    }
}
impl MetadataCaptureOriginal {
    pub(crate) fn new(
        provider: IcebergMetadata,
        request: ConnectorTableObjectCaptureRequest,
        cpu: Arc<dyn ProviderMetadataCpuPort>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|receiver: &std::sync::Weak<Self>| {
            let receiver = receiver.clone();
            let loader_provider = provider.clone();
            // One concrete Provider Future. Its output enters the stable
            // receiver INSIDE the normal Ready poll, before that F is dropped.
            // The weak edge only finds that receiver; it grants no authority
            // and cannot keep a Source alive after its original owners exit.
            let loader = Box::pin(async move {
                let mut original = loader_provider.prepare_table(ConnectorTableRequest {
                    table: request.table,
                    resolution: request.resolution,
                    context: request.context,
                });
                std::future::poll_fn(|cx| match original.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        let source = receiver
                            .upgrade()
                            .expect("original capture receiver remains owned");
                        let mut target = source.loaded.lock().unwrap_or_else(|p| p.into_inner());
                        assert!(target.is_none(), "one original preparation result");
                        *target = Some(result);
                        Poll::Ready(())
                    }
                })
                .await;
                // A destructor panic leaves loaded output in the original
                // receiver, while poll_loader keeps exit explicitly unproven.
                drop(original);
            });
            Self {
                provider,
                cpu,
                binding_source: OnceLock::new(),
                loaded: Mutex::new(None),
                body: Mutex::new(Some(Body {
                    loader: Some(loader),
                    loader_in_foreign: false,
                    loader_exited: false,
                    input: None,
                    submit_in_foreign: false,
                    finished: None,
                    closed: None,
                })),
            }
        })
    }

    fn poll_loader(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut loan = Loan::take(self);
        let body = loan.body();
        if body.loader_exited {
            return Poll::Ready(());
        }
        // A panic leaves the same poisoned F and all original data in Source.
        assert!(
            !body.loader_in_foreign,
            "unproven metadata loader cannot be repolled"
        );
        body.loader_in_foreign = true;
        let result = body
            .loader
            .as_mut()
            .expect("original loader")
            .as_mut()
            .poll(cx);
        match result {
            Poll::Pending => {
                body.loader_in_foreign = false;
                Poll::Pending
            }
            Poll::Ready(()) => {
                // The Provider result and its normal F Drop were observed by
                // the stable loader before this wrapper itself can exit.
                let loader = body.loader.take();
                drop(loader);
                body.loader_exited = true;
                body.loader_in_foreign = false;
                Poll::Ready(())
            }
        }
    }
    fn prepare_input(&self) -> Result<(), ConnectorError> {
        let mut loan = Loan::take(self);
        let body = loan.body();
        assert!(body.loader_exited, "capture requires actual loader exit");
        let preparation = self
            .loaded
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("one original table preparation")?;
        let facts = preparation
            .into_facts::<IcebergPreparedTableFacts>(&self.provider.descriptor().instance_id)?;
        // SAME closed metadata-binding Source and pure algorithm, not a new
        // CPU input author. Its generation is checked by that author and port.
        let source =
            IcebergMetadataCpuOriginal::metadata_binding(self.provider.clone(), facts, None);
        assert!(
            self.binding_source.set(Arc::clone(&source)).is_ok(),
            "one original binding phase"
        );
        body.input = Some(ProviderMetadataCpuInput(source));
        Ok(())
    }
    fn submit(&self) -> Result<(), ConnectorError> {
        let mut loan = Loan::take(self);
        let body = loan.body();
        assert!(
            !body.submit_in_foreign,
            "unproven metadata submit cannot be repeated"
        );
        body.submit_in_foreign = true;
        let result = self.cpu.submit_metadata_in(&mut body.input);
        body.submit_in_foreign = false;
        // A normal refusal preserves the same input. The binding Source also
        // remains in its original header if a foreign submit consumed then panicked.
        result
    }
    pub(crate) async fn drive(&self) -> CaptureResult {
        std::future::poll_fn(|cx| self.poll_loader(cx)).await;
        self.prepare_input()?;
        self.submit()?;
        let finished = std::future::poll_fn(|cx| self.cpu.poll_finished(cx)).await;
        Loan::take(self).body().finished = Some(finished);
        // No business handoff before the SAME original CPU/footer really closes.
        let closed = std::future::poll_fn(|cx| self.cpu.poll_closed(cx)).await;
        Loan::take(self).body().closed = Some(closed);
        {
            let mut loan = Loan::take(self);
            loan.body()
                .finished
                .as_ref()
                .expect("original CPU result")
                .clone()?;
            loan.body()
                .closed
                .as_ref()
                .expect("original CPU exit")
                .clone()?;
        }
        self.binding_source
            .get()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "metadata capture has no original binding Source",
                )
            })?
            .take_binding_with_snapshot_result()
    }
}
