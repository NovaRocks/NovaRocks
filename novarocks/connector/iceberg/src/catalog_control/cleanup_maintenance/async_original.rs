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

//! Original cleanup F/journal custody; borrowed waits never own these fields.
use super::*;
use crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal;
use crate::commit::{
    attempt::observation::{CatalogPublicationFailure, JournalEntryRef, PublicationJournal},
    model::FrozenRequest,
};
use novarocks_spi::connector::{
    ConnectorCleanupAsyncExecution, ConnectorCleanupAsyncOutput as Output,
    ConnectorCleanupAsyncSource, ConnectorCleanupAsyncStart as Start,
    ConnectorCleanupAsyncStep as Step, ConnectorMetadata, ConnectorOriginalResultScope,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    ExternalMutationOutcome, LakePublicationDisposition, ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

type Physical = crate::iceberg::table::Table;
type Journal = PublicationJournal<ReceiptRecord>;
type StepF = Pin<Box<dyn Future<Output = Result<Output, ConnectorError>> + Send>>;
#[derive(Clone, Copy, Default)]
struct Facts {
    applied: bool,
    disposition: Option<LakePublicationDisposition>,
}
impl Facts {
    fn observe(&mut self, disposition: LakePublicationDisposition, applied: bool) {
        self.applied |= applied;
        self.disposition = Some(match (self.disposition, disposition) {
            (Some(LakePublicationDisposition::CommitUnknown), _)
            | (_, LakePublicationDisposition::CommitUnknown) => {
                LakePublicationDisposition::CommitUnknown
            }
            (Some(LakePublicationDisposition::KnownCommitted), _)
            | (_, LakePublicationDisposition::KnownCommitted) => {
                LakePublicationDisposition::KnownCommitted
            }
            _ => LakePublicationDisposition::KnownUncommitted,
        });
    }
}
#[derive(Clone)]
enum OriginalFailure {
    Invocation(ConnectorError),
    Publication(CatalogPublicationFailure),
}
impl OriginalFailure {
    fn connector_error(&self) -> ConnectorError {
        match self {
            Self::Invocation(failure) => failure.clone(),
            Self::Publication(failure) => failure.connector_error(),
        }
    }
}
struct Shared {
    adapter: IcebergCleanupMaintenanceAdapter,
    metadata: Arc<dyn ConnectorMetadata>,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    original: ConnectorOriginalResultScope,
    plan_stamp: Mutex<Option<([u8; 32], [u8; 32])>>,
    executed: Mutex<std::collections::BTreeSet<u32>>,
    cpu_original: Mutex<Option<Arc<IcebergMetadataCpuOriginal>>>,
    // The delegated metadata capture also borrows this SAME serial CPU port.
    // A failed call is not an actual exit receipt for that original input.
    metadata_call_unproven: std::sync::atomic::AtomicBool,
    journals: Mutex<Vec<Journal>>,
    first_failure: Mutex<Option<OriginalFailure>>,
    receipt_cpu_failure: Mutex<Option<ConnectorError>>,
    facts: Mutex<Facts>,
}
impl Shared {
    fn retain_original_failure(&self, failure: OriginalFailure) {
        let mut first = self.first_failure.lock().unwrap_or_else(|p| p.into_inner());
        if first.is_none() {
            *first = Some(failure);
        }
    }
    fn original_failure(&self) -> Option<ConnectorError> {
        let failure = self
            .first_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        // The original Connector/domain projection runs after the slot lock.
        failure.as_ref().map(OriginalFailure::connector_error)
    }
    fn receipt_cpu_refused(&self, refusal: ConnectorError) -> ConnectorError {
        {
            let mut slot = self
                .receipt_cpu_failure
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if slot.is_none() {
                *slot = Some(refusal.clone());
            }
        }
        // A later ordinary receipt refusal does not replace an already
        // observed publication/required-finalize originating failure. The
        // actual CPU input and raw health remain with their existing owners.
        self.original_failure().unwrap_or(refusal)
    }
    fn publication_fact(&self) -> (Option<LakePublicationDisposition>, bool) {
        // Same lock order as retirement: a reader cannot miss a journal between
        // its removal and the cumulative known fact being installed.
        let journals = self.journals.lock().unwrap_or_else(|p| p.into_inner());
        let mut facts = *self.facts.lock().unwrap_or_else(|p| p.into_inner());
        for journal in journals.iter() {
            let (disposition, applied) = journal
                .catalog_dispatch_fact()
                .unwrap_or((LakePublicationDisposition::CommitUnknown, false));
            facts.observe(disposition, applied);
        }
        (facts.disposition, facts.applied)
    }
    /// Called only after the original publication F returned (including its
    /// necessary finalization), or after that step and its CPU actually exited.
    /// Remaining Unknown journals stay original responsibilities. Known suffixes
    /// are enough: an earlier unknown still prevents source retirement.
    fn retire_returned_publications(&self) {
        loop {
            let retired = {
                let mut journals = self.journals.lock().unwrap_or_else(|p| p.into_inner());
                let Some((disposition, applied)) =
                    journals.last().and_then(Journal::catalog_dispatch_fact)
                else {
                    return;
                };
                if disposition == LakePublicationDisposition::CommitUnknown {
                    return;
                }
                if let Some(failure) = journals.last().unwrap().catalog_publication_failure() {
                    self.retain_original_failure(OriginalFailure::Publication(failure));
                }
                self.facts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .observe(disposition, applied);
                journals.pop()
            };
            // Drop the original journal alias only outside the receiving lock.
            drop(retired);
        }
    }
}
struct Body {
    future: Option<StepF>,
    result: Option<Result<Output, ConnectorError>>,
    closed: bool,
    unproven: bool,
}
struct Original {
    shared: Arc<Shared>,
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
        return Poll::Ready(Err(invalid("cleanup Source has no original step")));
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
    fn install_step_in(&self, step: &mut Option<Step>) -> Result<(), ConnectorError> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        if body.closed
            || body.unproven
            || body.future.is_some()
            || body.result.is_some()
            || step.is_none()
        {
            return Err(invalid("cleanup step receiving is not empty"));
        }
        let plan = match step.as_ref().unwrap() {
            Step::Prepare(x) => &x.plan,
            Step::Execute(x) => &x.plan,
            Step::Page(x) => &x.plan,
            Step::Finalize(x) => &x.plan,
        };
        if self
            .shared
            .plan_stamp
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            != Some(&(plan.plan_digest(), plan.manifest_digest()))
        {
            return Err(invalid(
                "cleanup step differs from its original frozen plan",
            ));
        }
        if matches!(step.as_ref(), Some(Step::Execute(_)))
            && !self
                .shared
                .journals
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty()
        {
            return Err(invalid("cleanup retains an unresolved original journal"));
        }
        if let Step::Execute(request) = step.as_ref().unwrap()
            && !self
                .shared
                .executed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(request.prepared.batch_ordinal())
        {
            return Err(invalid(
                "cleanup prepared batch was already dispatched in this original source",
            ));
        }
        let step = step.take().unwrap();
        body.future = Some(Box::pin(run_step(self.shared.clone(), step)));
        Ok(())
    }
    fn poll_output(&self, cx: &mut Context<'_>) -> Poll<Result<Output, ConnectorError>> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        std::task::ready!(poll_step(body, cx))?;
        Poll::Ready(body.result.take().unwrap())
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        body.closed = true;
        if body.unproven {
            return Poll::Ready(Err(internal("cleanup original Future exit is unproven")));
        }
        if body.future.is_some() {
            std::task::ready!(poll_step(body, cx))?;
        }
        // A normally completed turn already observed its original worker and
        // forwarder exit. Do not close the shared serial port: a later selected
        // plan uses that SAME port. Only a still-owned failed turn needs exit.
        if self
            .shared
            .cpu_original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
            || self
                .shared
                .metadata_call_unproven
                .load(std::sync::atomic::Ordering::Acquire)
        {
            std::task::ready!(self.shared.cpu.poll_closed(cx))?;
            self.shared
                .metadata_call_unproven
                .store(false, std::sync::atomic::Ordering::Release);
            let retired = {
                self.shared
                    .cpu_original
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
            };
            drop(retired);
        }
        self.shared.retire_returned_publications();
        if !self
            .shared
            .journals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
        {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
    fn publication_fact(&self) -> (Option<LakePublicationDisposition>, bool) {
        self.shared.publication_fact()
    }
    fn original_failure(&self) -> Option<(ConnectorError, bool)> {
        let failure = self
            .shared
            .first_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()?;
        let required = matches!(
            &failure,
            OriginalFailure::Publication(CatalogPublicationFailure::RequiredFinalization(_))
        );
        Some((failure.connector_error(), required))
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
    let shared = Arc::new(Shared {
        adapter: adapter.clone(),
        metadata,
        cpu,
        original: input.original.clone(),
        plan_stamp: Mutex::new(None),
        executed: Mutex::new(std::collections::BTreeSet::new()),
        cpu_original: Mutex::new(None),
        metadata_call_unproven: std::sync::atomic::AtomicBool::new(false),
        journals: Mutex::new(Vec::new()),
        first_failure: Mutex::new(None),
        receipt_cpu_failure: Mutex::new(None),
        facts: Mutex::new(Facts::default()),
    });
    let input = start.take().unwrap();
    let future = Box::pin(plan_start(shared.clone(), input));
    *target = Some(ConnectorCleanupAsyncExecution::from_original(Arc::new(
        Original {
            shared,
            body: Mutex::new(Some(Body {
                future: Some(future),
                result: None,
                closed: false,
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
async fn run_cpu(shared: &Arc<Shared>, input: CpuInput) -> Result<CpuOutput, ConnectorError> {
    let source = IcebergMetadataCpuOriginal::cleanup_source(
        shared.adapter.descriptor.clone(),
        shared.adapter.key.incarnation,
        CpuWork {
            input,
            result: None,
        },
    );
    {
        let mut slot = shared
            .cpu_original
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(internal("cleanup CPU original is occupied"));
        }
        *slot = Some(source.clone());
    }
    crate::catalog_control::statistics::scoped_cpu::run_provider_metadata(&shared.cpu, &source)
        .await?;
    let result = source.take_cleanup_output();
    let retired = {
        shared
            .cpu_original
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    };
    drop(retired);
    result
}
async fn plan_start(shared: Arc<Shared>, start: Start) -> Result<Output, ConnectorError> {
    match run_cpu(
        &shared,
        CpuInput::Admission {
            adapter: shared.adapter.clone(),
            table: start.table.clone(),
            context: start.context.clone(),
        },
    )
    .await?
    {
        CpuOutput::Admitted => {}
        _ => return Err(internal("cleanup policy admission output differs")),
    }

    shared
        .metadata_call_unproven
        .store(true, std::sync::atomic::Ordering::Release);
    let (binding, _) = shared
        .metadata
        .capture_table_object_with_snapshot_cpu(
            ConnectorTableObjectCaptureRequest {
                table: start.table,
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: start.context.clone(),
            },
            shared.cpu.clone(),
        )
        .await?;
    shared
        .metadata_call_unproven
        .store(false, std::sync::atomic::Ordering::Release);
    let CpuOutput::Request(request, target) = run_cpu(
        &shared,
        CpuInput::Start {
            adapter: shared.adapter.clone(),
            table: binding.metadata.table,
            operation: start.operation_id,
            older: start.older_than_ms,
            selection: start.selection,
            context: start.context,
        },
    )
    .await?
    else {
        return Err(internal("cleanup initial request output differs"));
    };
    // Reuse the exact request-local table installed by the preceding binding
    // author. Per-step incarnation validation below retains its original reload.
    let physical = shared
        .adapter
        .runtime
        .load_table_classified_for_request_async(&target.namespace, &target.table, &request.context)
        .await
        .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
    let output = run_cpu(
        &shared,
        CpuInput::Plan {
            adapter: shared.adapter.clone(),
            request,
            target,
            table: physical.table,
            original: shared.original.clone(),
        },
    )
    .await?;
    match output {
        CpuOutput::Public(Output::Plan(plan)) => {
            *shared.plan_stamp.lock().unwrap_or_else(|p| p.into_inner()) =
                Some((plan.plan_digest(), plan.manifest_digest()));
            Ok(Output::Plan(plan))
        }
        _ => Err(internal("cleanup plan output differs")),
    }
}
async fn manifest(
    shared: &Arc<Shared>,
    plan: &ConnectorCleanupPlan,
    context: &ConnectorRequestContext,
) -> Result<(PlanPayload, Arc<[ManifestRecord]>), ConnectorError> {
    let CpuOutput::Payload(payload) = run_cpu(
        shared,
        CpuInput::Payload {
            adapter: shared.adapter.clone(),
            plan: plan.clone(),
        },
    )
    .await?
    else {
        return Err(internal("cleanup payload output differs"));
    };
    shared
        .adapter
        .runtime
        .control_state()
        .invalidate_table(&payload.namespace, &payload.table);
    let physical = shared
        .adapter
        .runtime
        .observe_table_classified_for_request_with_snapshots_async(
            &payload.namespace,
            &payload.table,
            context,
            crate::catalog::SnapshotLoading::All,
        )
        .await
        .map_err(|(kind, message)| ConnectorError::new(kind, message))?;
    let output = run_cpu(
        shared,
        CpuInput::Manifest {
            key: shared.adapter.key.clone(),
            plan: plan.clone(),
            payload: payload.clone(),
            table: physical.table,
        },
    )
    .await?;
    let CpuOutput::Records(records) = output else {
        return Err(internal("cleanup manifest output differs"));
    };
    Ok((payload, records))
}
async fn run_step(shared: Arc<Shared>, step: Step) -> Result<Output, ConnectorError> {
    match step {
        Step::Prepare(request) => {
            validate_context(&request.context)?;
            shared.adapter.ensure_owner(request.plan.owner())?;
            let (_, records) = manifest(&shared, &request.plan, &request.context).await?;
            let output = run_cpu(
                &shared,
                CpuInput::Prepare {
                    key: shared.adapter.key.clone(),
                    request,
                    records,
                },
            )
            .await?;
            match output {
                CpuOutput::Public(value) => Ok(value),
                _ => Err(internal("cleanup prepare output differs")),
            }
        }
        Step::Page(request) => {
            validate_context(&request.context)?;
            shared.adapter.ensure_owner(request.plan.owner())?;
            let (payload, records) = manifest(&shared, &request.plan, &request.context).await?;
            let output = run_cpu(
                &shared,
                CpuInput::Page {
                    key: shared.adapter.key.clone(),
                    request,
                    records,
                    uuid: table_uuid_from_payload(&payload)?,
                },
            )
            .await?;
            match output {
                CpuOutput::Public(value) => Ok(value),
                _ => Err(internal("cleanup page output differs")),
            }
        }
        Step::Finalize(request) => {
            // Closing projects the SAME completed operation. It starts no I/O
            // or CPU and must not replace returned journal facts with a late
            // foreground stop. install_step_in already checked its plan stamp;
            // the execution wrapper also verifies the exact owner/operation.
            shared.adapter.ensure_owner(request.plan.owner())?;
            Ok(Output::Finalized)
        }
        Step::Execute(request) => execute_batch(&shared, request).await,
    }
}
async fn execute_batch(
    shared: &Arc<Shared>,
    request: ConnectorCleanupExecuteRequest,
) -> Result<Output, ConnectorError> {
    validate_context(&request.context)?;
    shared.adapter.ensure_owner(request.plan.owner())?;
    let (payload, records) = manifest(shared, &request.plan, &request.context).await?;
    let output = run_cpu(
        shared,
        CpuInput::Batch {
            adapter: shared.adapter.clone(),
            request: request.clone(),
            records,
        },
    )
    .await?;
    let CpuOutput::Records(batch) = output else {
        return Err(internal("cleanup frozen batch output differs"));
    };
    // All object records are refused before stat/list/delete. This never gates
    // the separate exact Catalog ref or operation-owned journal cleanup.
    if batch
        .iter()
        .any(|r| matches!(&r.candidate, ManifestCandidate::Object { .. }))
    {
        iceberg_scan_gc_safety().require_scanned_object_gc()?;
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "cleanup has no scanned-object author",
        ));
    }
    shared.original.check_active().map_err(scope_error)?;
    let mut outcomes = Vec::with_capacity(batch.len());
    {
        let mut journals = shared.journals.lock().unwrap_or_else(|p| p.into_inner());
        journals.reserve(batch.len());
    }
    for record in batch.iter() {
        let (coordination, physical) = shared
            .adapter
            .runtime
            .coordinate_table_for_request_async(
                &payload.namespace,
                &payload.table,
                &request.context,
            )
            .await?;
        shared.original.check_active().map_err(scope_error)?;
        let output = run_cpu(
            shared,
            CpuInput::Freeze {
                adapter: shared.adapter.clone(),
                operation: request.plan.operation_id(),
                payload: payload.clone(),
                record: record.clone(),
                context: request.context.clone(),
                table: physical.table,
            },
        )
        .await?;
        let CpuOutput::Frozen(prepared) = output else {
            return Err(internal("cleanup exact freeze output differs"));
        };
        let Some((frozen, journal)) = prepared else {
            outcomes.push(receipt(
                record.ordinal,
                ObjectOutcome::Failed,
                Some("owned ref provenance changed before exact retirement".into()),
            )?);
            drop(coordination);
            continue;
        };
        // Original journal installed before crossing the one dispatch barrier.
        shared
            .journals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(journal.clone());
        let catalog = shared.adapter.runtime.novarocks_catalog().clone();
        let policy = catalog.evidence_policy();
        let reply = journal
            .publish_once(
                request.context.clone(),
                policy,
                move |descriptor, generation, original| async move {
                    catalog
                        .prepare_frozen_publication(
                            frozen,
                            CatalogOperation::DropBranch,
                            None,
                            descriptor,
                            generation,
                            original,
                        )
                        .await
                },
            )
            .await;
        let result = receive_publication(shared, journal, reply, record.ordinal).await?;
        outcomes.push(result);
        drop(coordination);
    }
    let output = run_cpu(
        shared,
        CpuInput::Receipt {
            descriptor: shared.adapter.descriptor.clone(),
            key: shared.adapter.key.clone(),
            request,
            outcomes,
        },
    )
    .await?;
    match output {
        CpuOutput::Public(value) => Ok(value),
        _ => Err(internal("cleanup receipt output differs")),
    }
}

async fn receive_publication(
    shared: &Arc<Shared>,
    journal: Journal,
    reply: crate::iceberg::Result<JournalEntryRef>,
    ordinal: u32,
) -> Result<ReceiptRecord, ConnectorError> {
    // Dispatch and necessary finalization have actually returned. Publish the
    // original neutral fact BEFORE receipt formation can refuse or unwind.
    // The stable step/CPU input still holds its original journal until exit.
    if let Some(failure) = journal.catalog_publication_failure() {
        shared.retain_original_failure(OriginalFailure::Publication(failure));
    }
    shared.retire_returned_publications();
    let reply = match reply {
        Ok(entry) => Ok(entry),
        Err(error) => {
            let error =
                crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure(
                    error,
                );
            shared.retain_original_failure(OriginalFailure::Invocation(error.clone()));
            Err(error)
        }
    };
    match run_cpu(
        shared,
        CpuInput::Outcome {
            journal,
            reply,
            ordinal,
        },
    )
    .await
    {
        Ok(CpuOutput::Outcome(record)) => Ok(record),
        Ok(_) => Err(internal("cleanup publication output differs")),
        Err(refusal) => Err(shared.receipt_cpu_refused(refusal)),
    }
}

pub(crate) struct CpuWork {
    input: CpuInput,
    result: Option<Result<CpuOutput, ConnectorError>>,
}
enum CpuInput {
    Admission {
        adapter: IcebergCleanupMaintenanceAdapter,
        table: novarocks_spi::connector::ConnectorTableIdentity,
        context: ConnectorRequestContext,
    },

    Start {
        adapter: IcebergCleanupMaintenanceAdapter,
        table: novarocks_spi::connector::ConnectorTableHandle,
        operation: novarocks_spi::connector::ConnectorCleanupOperationId,
        older: i64,
        selection: Option<ConnectorCleanupOwnedRefSelection>,
        context: ConnectorRequestContext,
    },
    Payload {
        adapter: IcebergCleanupMaintenanceAdapter,
        plan: ConnectorCleanupPlan,
    },
    Plan {
        adapter: IcebergCleanupMaintenanceAdapter,
        request: Box<ConnectorCleanupPlanningRequest>,
        target: IcebergTablePayload,
        table: Physical,
        original: ConnectorOriginalResultScope,
    },
    Manifest {
        key: ConnectorProviderBindingKey,
        plan: ConnectorCleanupPlan,
        payload: PlanPayload,
        table: Physical,
    },
    Prepare {
        key: ConnectorProviderBindingKey,
        request: ConnectorCleanupPrepareRequest,
        records: Arc<[ManifestRecord]>,
    },
    Page {
        key: ConnectorProviderBindingKey,
        request: ConnectorCleanupCandidatePageRequest,
        records: Arc<[ManifestRecord]>,
        uuid: uuid::Uuid,
    },
    Batch {
        adapter: IcebergCleanupMaintenanceAdapter,
        request: ConnectorCleanupExecuteRequest,
        records: Arc<[ManifestRecord]>,
    },
    Freeze {
        adapter: IcebergCleanupMaintenanceAdapter,
        operation: novarocks_spi::connector::ConnectorCleanupOperationId,
        payload: PlanPayload,
        record: ManifestRecord,
        context: ConnectorRequestContext,
        table: Physical,
    },
    Outcome {
        journal: Journal,
        reply: Result<JournalEntryRef, ConnectorError>,
        ordinal: u32,
    },
    Receipt {
        descriptor: ConnectorInstanceDescriptor,
        key: ConnectorProviderBindingKey,
        request: ConnectorCleanupExecuteRequest,
        outcomes: Vec<ReceiptRecord>,
    },
}
pub(in crate::catalog_control) enum CpuOutput {
    Admitted,
    Request(Box<ConnectorCleanupPlanningRequest>, IcebergTablePayload),
    Payload(PlanPayload),
    Public(Output),
    Records(Arc<[ManifestRecord]>),
    Frozen(Option<(FrozenRequest, Journal)>),
    Outcome(ReceiptRecord),
}
impl CpuWork {
    pub(crate) fn run(&mut self) {
        assert!(self.result.is_none(), "one actual cleanup CPU phase");
        self.result = Some(self.run_once());
    }
    pub(in crate::catalog_control) fn take_output(&mut self) -> Result<CpuOutput, ConnectorError> {
        self.result
            .take()
            .ok_or_else(|| internal("cleanup CPU result is absent"))?
    }
    fn run_once(&self) -> Result<CpuOutput, ConnectorError> {
        match &self.input {
            CpuInput::Admission {
                adapter,
                table,
                context,
            } => {
                validate_context(context)?;
                adapter
                    .runtime
                    .novarocks_catalog()
                    .admit(&CatalogAdmissionRequest::new(
                        CatalogOperation::RemoveOrphanFiles,
                        CatalogTableName::new(table.namespace.to_string(), table.table.to_string()),
                        context.initiation(),
                    ))
                    .map_err(connector_unsupported)?;
                Ok(CpuOutput::Admitted)
            }

            CpuInput::Start {
                adapter,
                table,
                operation,
                older,
                selection,
                context,
            } => {
                let operation_value=novarocks_spi::connector::ConnectorCleanupOperation::remove_unreferenced_objects(table.clone(),*older)?;
                let request = match selection {
                    Some(selection) => {
                        ConnectorCleanupPlanningRequest::try_new_selected_owned_refs(
                            *operation,
                            adapter.key.clone(),
                            operation_value,
                            selection.clone(),
                            context.clone(),
                        )?
                    }
                    None => ConnectorCleanupPlanningRequest::try_new(
                        *operation,
                        adapter.key.clone(),
                        operation_value,
                        context.clone(),
                    )?,
                };
                let target = serde_json::from_slice(request.operation().table().payload())
                    .map_err(|_| invalid("invalid cleanup physical table payload"))?;
                Ok(CpuOutput::Request(Box::new(request), target))
            }
            CpuInput::Payload { adapter, plan } => {
                plan.validate()?;
                adapter.plan_payload(plan).map(CpuOutput::Payload)
            }
            CpuInput::Plan {
                adapter,
                request,
                target,
                table,
                original,
            } => {
                original.check_active().map_err(scope_error)?;
                adapter
                    .runtime
                    .novarocks_catalog()
                    .admit(&CatalogAdmissionRequest::new(
                        CatalogOperation::RemoveOrphanFiles,
                        CatalogTableName::new(target.namespace.clone(), target.table.clone()),
                        request.context.initiation(),
                    ))
                    .map_err(connector_unsupported)?;
                plan_loaded(adapter, request, target.clone(), table.clone())
                    .map(|p| CpuOutput::Public(Output::Plan(p)))
            }
            CpuInput::Manifest {
                key,
                plan,
                payload,
                table,
            } => manifest_loaded(key, plan, payload, table).map(CpuOutput::Records),
            CpuInput::Prepare {
                key,
                request,
                records,
            } => {
                let request = ConnectorCleanupPrepareRequest::try_new(
                    request.plan.clone(),
                    request.batch_ordinal,
                    request.context.clone(),
                )?;
                prepare_from_records(key, &request, records)
                    .map(|p| CpuOutput::Public(Output::Prepared(p)))
            }
            CpuInput::Page {
                key,
                request,
                records,
                uuid,
            } => {
                let request = ConnectorCleanupCandidatePageRequest::try_new(
                    request.plan.clone(),
                    request.offset,
                    request.limit,
                    request.context.clone(),
                )?;
                page_from_records(key, &request, records, *uuid)
                    .map(|p| CpuOutput::Public(Output::Page(p)))
            }
            CpuInput::Batch {
                adapter,
                request,
                records,
            } => {
                let request = ConnectorCleanupExecuteRequest::try_new(
                    request.plan.clone(),
                    request.prepared.clone(),
                    request.context.clone(),
                )?;
                let evidence: PreparedPayload = decode_canonical(
                    request.prepared.evidence_payload(),
                    "cleanup prepared evidence",
                )?;
                if evidence.version != ARTIFACT_VERSION
                    || evidence.batch_ordinal != request.prepared.batch_ordinal()
                    || evidence.record_count == 0
                    || evidence.record_count as usize > MAX_BATCH_OBJECTS
                    || evidence.batch_digest_hex != hex_encode(request.prepared.batch_digest())
                    || request.prepared.batch_ordinal() >= request.plan.summary().batch_count()
                {
                    return Err(corrupt("Iceberg cleanup prepared evidence is invalid"));
                }
                adapter.ensure_owner(request.plan.owner())?;
                let start = evidence.first_ordinal as usize;
                let end = start
                    .checked_add(evidence.record_count as usize)
                    .ok_or_else(|| corrupt("cleanup batch range overflow"))?;
                let batch = records
                    .get(start..end)
                    .ok_or_else(|| corrupt("cleanup batch exceeds frozen manifest"))?;
                if batch_digest(batch) != request.prepared.batch_digest() {
                    return Err(corrupt("cleanup batch digest differs"));
                }
                Ok(CpuOutput::Records(Arc::from(batch)))
            }
            CpuInput::Freeze {
                adapter,
                operation,
                payload,
                record,
                context,
                table,
            } => {
                let ManifestCandidate::OwnedRef {
                    name,
                    head_snapshot_id,
                    provenance_version,
                    provenance_digest_hex,
                    created_at_ms,
                } = &record.candidate
                else {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "cleanup exact ref Source cannot execute an object record",
                    ));
                };
                let Some(frozen) = freeze_owned_ref_on_table(
                    &adapter.key,
                    *operation,
                    payload,
                    record.ordinal,
                    name,
                    *head_snapshot_id,
                    *provenance_version,
                    provenance_digest_hex.as_deref(),
                    *created_at_ms,
                    context,
                    table,
                )?
                else {
                    return Ok(CpuOutput::Frozen(None));
                };
                let journal = ref_journal(
                    &adapter.runtime,
                    &adapter.key,
                    *operation,
                    payload,
                    record.ordinal,
                    name,
                    *head_snapshot_id,
                    &frozen,
                )?;
                Ok(CpuOutput::Frozen(Some((frozen, journal))))
            }
            CpuInput::Outcome {
                journal,
                reply,
                ordinal,
            } => {
                let outcome = match reply {
                    Ok(entry) => journal.catalog_entry_outcome(*entry),
                    Err(_) => journal
                        .catalog_bridge_failure("Owned ref publication invocation failed".into()),
                };
                match outcome {
                    ExternalMutationOutcome::KnownCommitted {
                        receipt: actual,
                        finalization,
                        ..
                    } => {
                        let record = match finalization {
                            novarocks_spi::connector::ExternalMutationFinalization::Complete => {
                                actual
                            }
                            novarocks_spi::connector::ExternalMutationFinalization::Failed(
                                failure,
                            ) => {
                                receipt(*ordinal, ObjectOutcome::Failed, Some(failure.to_string()))?
                            }
                        };
                        Ok(CpuOutput::Outcome(record))
                    }
                    ExternalMutationOutcome::KnownUncommitted { failure, .. } => {
                        Ok(CpuOutput::Outcome(receipt(
                            *ordinal,
                            ObjectOutcome::Failed,
                            Some(failure.to_string()),
                        )?))
                    }
                    ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                        Ok(CpuOutput::Outcome(receipt(
                            *ordinal,
                            ObjectOutcome::Unknown,
                            Some(failure.to_string()),
                        )?))
                    }
                }
            }
            CpuInput::Receipt {
                descriptor,
                key,
                request,
                outcomes,
            } => receipt_value(
                descriptor,
                key,
                &request.plan,
                &request.prepared,
                domain_digest(RECEIPT_DOMAIN, &canonical(outcomes)?),
                outcomes,
            )
            .map(|r| CpuOutput::Public(Output::Receipt(r))),
        }
    }
}
#[cfg(test)]
#[path = "async_original/tests.rs"]
mod tests;
