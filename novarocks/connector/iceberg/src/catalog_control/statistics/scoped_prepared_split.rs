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

//! The original FE source-open frame for one prepared read projection.
//! Fresh metadata proves visibility; complete discovery still reads the frozen
//! Table and original domains. CPU workers only borrow this same closed Source.
// Design: ADR-0207 (docs/adr/ADR-0207-single-prepared-change-window-and-frozen-source-endpoints.md)

use super::*;
use crate::delete_semantics::ReadDomain;
use crate::loaded_table::IcebergPhysicalTable;
use crate::typed_boundary::{IcebergTypedBoundary, PreparedSourceAccess};
use crate::typed_read::IcebergPlannedDataFile;
use crate::typed_read::prepared_source_proof::{
    validate_prepared_endpoint, validate_referenced_specs, validate_window_lineage,
};
use novarocks_spi::connector::read_stack::ConnectorReadSourceProjection;
use novarocks_spi::connector::read_stack::adapter::{
    ProviderReadRuntime, ProviderReadSplitOpenRequest, ProviderReadSplitSource,
};
use novarocks_spi::connector::{
    ConnectorOriginalResultScope, ConnectorPayloadRetentionGuard, ConnectorRequestContext,
    ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

type SplitSource = Box<dyn ProviderReadSplitSource<IcebergTypedBoundary>>;
type FreshLoader =
    Pin<Box<dyn Future<Output = Result<IcebergPhysicalTable, ConnectorError>> + Send>>;

/// Raw receipt precedes the original foreign F's Drop. A failed poll or Drop
/// leaves the same input/result and the unproven marker in this stable field.
struct FreshObservation {
    loader: Option<FreshLoader>,
    result: Option<Result<IcebergPhysicalTable, ConnectorError>>,
    foreign: bool,
    exited: bool,
}
impl FreshObservation {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        if self.foreign {
            return Poll::Ready(Err(invalid(
                "original fresh metadata poll or Drop is unproven",
            )));
        }
        if self.exited {
            return Poll::Ready(Ok(()));
        }
        self.foreign = true;
        match self
            .loader
            .as_mut()
            .expect("original fresh metadata F")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => {
                self.foreign = false;
                Poll::Pending
            }
            Poll::Ready(raw) => {
                self.result = Some(raw);
                drop(self.loader.take());
                self.exited = true;
                self.foreign = false;
                Poll::Ready(Ok(()))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Proof(usize),
    ReferencedSpecs(usize),
    Finish,
}

/// One endpoint's completed original pure projection. The From stage must
/// finish every hard data check before any To endpoint proof can begin.
pub(crate) struct PreparedSplitEndpoint {
    pub(crate) files: Vec<IcebergPlannedDataFile>,
    pub(crate) retention: ConnectorPayloadRetentionGuard,
}

pub(crate) struct Work {
    boundary: Arc<IcebergTypedBoundary>,
    request: Option<ProviderReadSplitOpenRequest<IcebergTypedBoundary>>,
    receipt: PreparedSourceAccess,
    context: ConnectorRequestContext,
    original: ConnectorOriginalResultScope,
    frozen: IcebergPhysicalTable,
    domains: [Option<Arc<ReadDomain>>; 2],
    fresh: FreshObservation,
    phase: Phase,
    snapshot: Option<Arc<IcebergMetadataCpuOriginal>>,
    endpoints: [Option<super::scoped_snapshot::ScopedReadSnapshot>; 2],
    planned: [Option<PreparedSplitEndpoint>; 2],
    result: Option<Result<SplitSource, ConnectorError>>,
}
impl Work {
    fn run_phase(&mut self) -> Result<(), ConnectorError> {
        self.original.check_active().map_err(|error| {
            super::scoped_read::checked_read(
                crate::read_snapshot::cow_capture::ReadFailure::Original(error.into()),
            )
        })?;
        self.context.check_active()?;
        let observed = self
            .fresh
            .result
            .as_ref()
            .ok_or_else(|| invalid("fresh metadata has not returned"))?
            .as_ref()
            .map_err(Clone::clone)?;
        match self.phase {
            Phase::Proof(index) => {
                let domain = self.domains[index]
                    .as_ref()
                    .ok_or_else(|| invalid("prepared endpoint domain is absent"))?;
                // The original helper checks control/UUID before this endpoint
                // and its To-effective interpretation. It never checks Current.
                validate_prepared_endpoint(
                    self.receipt.source.access.metadata(),
                    observed.table.metadata(),
                    domain,
                    &self.context,
                )?;
                if index == 1
                    && let Some(from) = &self.domains[0]
                {
                    validate_window_lineage(
                        observed.table.metadata(),
                        from.endpoint().snapshot_id(),
                        domain.endpoint().snapshot_id(),
                        &self.context,
                    )?;
                }
            }
            Phase::ReferencedSpecs(index) => {
                let endpoint = self.endpoints[index]
                    .as_ref()
                    .ok_or_else(|| invalid("original prepared discovery has not completed"))?;
                let inventory = endpoint.referenced_specs.as_ref().ok_or_else(|| {
                    invalid("prepared discovery returned no original spec inventory")
                })?;
                validate_referenced_specs(
                    self.receipt.source.access.metadata(),
                    observed.table.metadata(),
                    self.domains[index].as_ref().unwrap(),
                    inventory.ids(),
                    &self.context,
                )?;
                // Finish the SAME endpoint's original manifest facts/data-file
                // projection now. Its Data failure must precede To proof, not
                // be deferred to final two-endpoint split assembly.
                let snapshot = self.endpoints[index]
                    .take()
                    .ok_or_else(|| invalid("original prepared endpoint was already consumed"))?;
                self.planned[index] =
                    Some(self.boundary.plan_prepared_source_endpoint(
                        self.request.as_ref().ok_or_else(|| {
                            invalid("prepared split inputs were already consumed")
                        })?,
                        self.domains[index].as_ref().unwrap(),
                        snapshot,
                    )?);
            }
            Phase::Finish => {
                let request = self
                    .request
                    .take()
                    .ok_or_else(|| invalid("prepared split inputs were already consumed"))?;
                let selected = self.planned[1]
                    .take()
                    .ok_or_else(|| invalid("selected prepared discovery is absent"))?;
                // The pure mapper receives complete observations, including
                // their original guards; it transfers custody to the actual
                // split source. No old get_splits or domain mint is invoked.
                let result = self.boundary.finish_prepared_source_splits(
                    request,
                    self.planned[0].take(),
                    selected,
                );
                self.result = Some(result);
            }
        }
        Ok(())
    }
    pub(super) fn run(&mut self) {
        if let Err(error) = self.run_phase() {
            self.result = Some(Err(error));
        }
    }
}

/// Pure original-owner checks shared by real discovery and a proved zero-Work
/// opening. This packet carries the existing Root; it grants no new work.
pub(crate) fn validate_prepared_split_request(
    boundary: &IcebergTypedBoundary,
    request: &ProviderReadSplitOpenRequest<IcebergTypedBoundary>,
    receipt: &PreparedSourceAccess,
) -> Result<ConnectorRequestContext, ConnectorError> {
    if receipt.source.owner.instance_id != boundary.descriptor().instance_id
        || receipt.source.owner.incarnation != boundary.incarnation()
        || &receipt.source.catalog != boundary.catalog_handle()
    {
        return Err(invalid(
            "prepared split source has a foreign exact read binding",
        ));
    }
    // Check the mandatory packet before moving any original input. The
    // caller already cloned it for drive/close; the Work stores its same
    // original scope, without a Source -> port -> job -> Source cycle.
    let preparation = request
        .preparation
        .as_ref()
        .ok_or_else(|| invalid("prepared split source requires its original preparation packet"))?;
    let context = boundary.prepared_source_request_context()?;
    context.check_active()?;
    preparation.original().check_active().map_err(|error| {
        super::scoped_read::checked_read(crate::read_snapshot::cow_capture::ReadFailure::Original(
            error.into(),
        ))
    })?;
    Ok(context)
}

impl IcebergMetadataCpuOriginal {
    /// Construct only. The original caller installs this Source before polling
    /// the returned driver and strongly retains the exact CPU packet to close.
    pub(crate) fn prepared_split_source(
        boundary: Arc<IcebergTypedBoundary>,
        mut request: ProviderReadSplitOpenRequest<IcebergTypedBoundary>,
        receipt: PreparedSourceAccess,
    ) -> Result<Arc<Self>, ConnectorError> {
        let context = validate_prepared_split_request(&boundary, &request, &receipt)?;
        let domains = match receipt.projection {
            ConnectorReadSourceProjection::To => [
                None,
                Some(Arc::clone(
                    receipt
                        .source
                        .relations
                        .to
                        .read_domain()
                        .ok_or_else(|| invalid("prepared To relation has no domain"))?,
                )),
            ],
            ConnectorReadSourceProjection::From => [
                None,
                Some(Arc::clone(
                    receipt
                        .source
                        .relations
                        .from
                        .as_ref()
                        .ok_or_else(|| invalid("prepared decision has no From relation"))?
                        .read_domain()
                        .ok_or_else(|| invalid("prepared From relation has no domain"))?,
                )),
            ],
            ConnectorReadSourceProjection::Delta => {
                let delta = receipt
                    .source
                    .relations
                    .delta
                    .as_ref()
                    .ok_or_else(|| invalid("prepared decision has no Delta relation"))?;
                [
                    Some(Arc::clone(delta.from_read_domain())),
                    Some(Arc::clone(delta.to_read_domain())),
                ]
            }
        };
        let name = receipt.source.request.name();
        // Lookup only: this is the access phase's original request-bound frozen
        // FileIO frame, not a catalog or Current metadata observation.
        let frozen = boundary.pinned_table_for_attempt(name)?;
        let runtime = boundary.prepared_source_runtime();
        let namespace = name.schema_name().to_string();
        let table = name.table_name().to_string();
        let actual = context.clone();
        let fresh = FreshObservation {
            loader: Some(Box::pin(async move {
                runtime
                    .observe_table_original_for_request_with_snapshots_async(
                        &namespace,
                        &table,
                        &actual,
                        crate::catalog::SnapshotLoading::All,
                    )
                    .await
            })),
            result: None,
            foreign: false,
            exited: false,
        };
        let preparation = request
            .preparation
            .take()
            .expect("checked original preparation");
        Ok(Self::new(
            boundary.descriptor().clone(),
            boundary.incarnation(),
            CpuBody::PreparedSplit(Box::new(Work {
                boundary,
                request: Some(request),
                receipt,
                context,
                original: preparation.original().clone(),
                frozen,
                domains,
                fresh,
                phase: Phase::Proof(0),
                snapshot: None,
                endpoints: [None, None],
                planned: [None, None],
                result: None,
            })),
        ))
    }

    fn selected_prepared_split_error(&self) -> Option<ConnectorError> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let original = state.as_ref()?;
        let CpuBody::PreparedSplit(body) = &original.body else {
            return None;
        };
        body.result.as_ref()?.as_ref().err().cloned()
    }
    fn poll_prepared_split_observation(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ConnectorError>> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::PreparedSplit(body) = &mut loan.body.as_mut().unwrap().body else {
            return Poll::Ready(Err(invalid("foreign prepared split source")));
        };
        body.fresh.poll(cx)
    }
    fn prepare_split_phase(&self, phase: Phase) -> Result<(), ConnectorError> {
        let mut loan = BodyLoan::take(self);
        let original = loan.body.as_mut().unwrap();
        let CpuBody::PreparedSplit(body) = &mut original.body else {
            return Err(invalid("foreign prepared split source"));
        };
        body.phase = phase;
        original.started = false;
        Ok(())
    }
    async fn run_prepared_split_phase(
        self: &Arc<Self>,
        cpu: &Arc<dyn ProviderMetadataCpuPort>,
        phase: Phase,
    ) -> Result<(), ConnectorError> {
        self.prepare_split_phase(phase)?;
        let completed = super::run_provider_metadata(cpu, self).await;
        if let Some(original) = self.selected_prepared_split_error() {
            return Err(original);
        }
        completed
    }

    pub(crate) async fn drive_prepared_split(
        self: &Arc<Self>,
        cpu: &Arc<dyn ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        std::future::poll_fn(|cx| self.poll_prepared_split_observation(cx)).await?;
        {
            let mut loan = BodyLoan::take(self);
            let CpuBody::PreparedSplit(body) = &mut loan.body.as_mut().unwrap().body else {
                return Err(invalid("foreign prepared split source"));
            };
            if let Some(Err(error)) = &body.fresh.result {
                // The real request Err precedes every later CPU/control check.
                body.result = Some(Err(error.clone()));
                return Ok(());
            }
        }
        for index in 0..2 {
            let present = {
                let loan = BodyLoan::take(self);
                let CpuBody::PreparedSplit(body) = &loan.body.as_ref().unwrap().body else {
                    return Err(invalid("foreign prepared split source"));
                };
                body.domains[index].is_some()
            };
            if !present {
                continue;
            }
            if let Err(error) = self
                .run_prepared_split_phase(cpu, Phase::Proof(index))
                .await
            {
                if self.selected_prepared_split_error().is_some() {
                    return Ok(());
                }
                return Err(error);
            }
            let (source, snapshot_id) = {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSplit(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared split source"));
                };
                let domain = Arc::clone(body.domains[index].as_ref().unwrap());
                let id = domain.endpoint().snapshot_id();
                let source = Self::read_snapshot_source_for_generation(
                    body.boundary.descriptor().clone(),
                    body.boundary.incarnation(),
                    body.frozen.table.clone(),
                    id,
                    body.context.clone(),
                    body.original.clone(),
                    super::scoped_snapshot::SnapshotReadKind::PreparedSource { domain },
                );
                body.snapshot = Some(Arc::clone(&source));
                (source, id)
            };
            let completed =
                super::scoped_snapshot::drive_change_window_snapshot(&source, cpu, snapshot_id)
                    .await;
            if let Some(error) = source.selected_read_snapshot_error() {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSplit(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared split source"));
                };
                body.result = Some(Err(error));
                return Ok(());
            }
            completed?;
            {
                let mut loan = BodyLoan::take(self);
                let CpuBody::PreparedSplit(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign prepared split source"));
                };
                let mut received = None;
                source.take_read_snapshot_in(&mut received)?;
                match received.ok_or_else(|| {
                    invalid("prepared endpoint did not return its original observation")
                })? {
                    Ok(endpoint) => body.endpoints[index] = Some(endpoint),
                    Err(error) => {
                        body.result = Some(Err(error));
                        return Ok(());
                    }
                }
                drop(body.snapshot.take());
            }
            drop(source);
            if let Err(error) = self
                .run_prepared_split_phase(cpu, Phase::ReferencedSpecs(index))
                .await
            {
                if self.selected_prepared_split_error().is_some() {
                    return Ok(());
                }
                return Err(error);
            }
        }
        if let Err(error) = self.run_prepared_split_phase(cpu, Phase::Finish).await {
            if self.selected_prepared_split_error().is_some() {
                return Ok(());
            }
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn take_prepared_split(&self) -> Result<SplitSource, ConnectorError> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::PreparedSplit(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign prepared split source"));
        };
        body.result
            .take()
            .ok_or_else(|| invalid("prepared split source is not ready"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct OriginalRaw {
        raw: Option<ConnectorError>,
        dropped: Arc<AtomicBool>,
        panic_on_drop: bool,
    }
    impl Future for OriginalRaw {
        type Output = Result<IcebergPhysicalTable, ConnectorError>;
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(self.raw.take().expect("one original raw result")))
        }
    }
    impl Drop for OriginalRaw {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "original raw F Drop panic");
        }
    }
    #[test]
    fn fresh_raw_error_is_received_before_normal_or_panicking_foreign_drop() {
        for panic_on_drop in [false, true] {
            let original = ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "original fresh metadata failure",
            );
            let dropped = Arc::new(AtomicBool::new(false));
            let mut observation = FreshObservation {
                loader: Some(Box::pin(OriginalRaw {
                    raw: Some(original.clone()),
                    dropped: dropped.clone(),
                    panic_on_drop,
                })),
                result: None,
                foreign: false,
                exited: false,
            };
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observation.poll(&mut cx)
            }));
            assert_eq!(
                observation.result.as_ref().unwrap().as_ref().err().unwrap(),
                &original
            );
            assert!(dropped.load(Ordering::SeqCst));
            if panic_on_drop {
                assert!(polled.is_err());
                assert!(observation.foreign);
                assert!(!observation.exited);
                assert!(matches!(observation.poll(&mut cx), Poll::Ready(Err(_))));
                assert_eq!(
                    observation.result.as_ref().unwrap().as_ref().err().unwrap(),
                    &original
                );
            } else {
                assert!(matches!(polled.unwrap(), Poll::Ready(Ok(()))));
                assert!(observation.exited);
                assert!(!observation.foreign);
            }
        }
    }
    struct HeldRaw {
        released: Arc<AtomicBool>,
        original: OriginalRaw,
    }
    impl Future for HeldRaw {
        type Output = Result<IcebergPhysicalTable, ConnectorError>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.released.load(Ordering::SeqCst) {
                Pin::new(&mut self.original).poll(cx)
            } else {
                Poll::Pending
            }
        }
    }
    #[test]
    fn dropping_borrowed_fresh_waiter_keeps_the_original_foreign_f_and_raw_receipt() {
        let original = ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "held original fresh failure",
        );
        let dropped = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        let mut observation = FreshObservation {
            loader: Some(Box::pin(HeldRaw {
                released: released.clone(),
                original: OriginalRaw {
                    raw: Some(original.clone()),
                    dropped: dropped.clone(),
                    panic_on_drop: false,
                },
            })),
            result: None,
            foreign: false,
            exited: false,
        };
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let mut borrowed = Box::pin(std::future::poll_fn(|cx| observation.poll(cx)));
        assert!(borrowed.as_mut().poll(&mut cx).is_pending());
        drop(borrowed);
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(observation.loader.is_some());
        assert!(observation.result.is_none());
        assert!(!observation.exited);
        released.store(true, Ordering::SeqCst);
        assert!(matches!(observation.poll(&mut cx), Poll::Ready(Ok(()))));
        assert_eq!(
            observation.result.as_ref().unwrap().as_ref().err().unwrap(),
            &original
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert!(observation.exited);
    }
}
