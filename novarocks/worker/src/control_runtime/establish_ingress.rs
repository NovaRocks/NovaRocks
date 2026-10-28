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

//! An original RPC lends its Step authority to one host getter. The raw
//! fields remain with a separately paid Cleanup child until real disposal or
//! an explicit, funded handoff. No opaque source enters the protocol core.

use super::actors::WorkerActor;
use super::{control_request::ControlRequestNotice, preparation::QueuedPreparation};
use novarocks_control_runtime::driver::ActorRuntime;
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_role::{ExitAuthority, ExitRole, ExitRoleError, ExitRolePhase},
    lease::{CapacityBundle, WorkLease},
    owned_return::OwnedReturnSender,
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::identity::ActorId;
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, ScopeId, WorkId},
    scope::WorkKind,
};
use novarocks_execution_contract::{
    AdmissionTicketId, CodecOwnedContent, CredentialUpdate, EstablishQueryContext,
    EstablishQueryContextParts, LeaseValidFor, QueryContextRef, TaskOperationId,
};
use novarocks_worker_protocol::context::ContextEstablishment;
use novarocks_worker_protocol::{control::EstablishSemanticFacts, creation::PayloadId};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

fn source_consumer() -> ConsumerId {
    ConsumerId::new(1).expect("original source consumer")
}
type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct EstablishProjection {
    pub operation: TaskOperationId,
    pub context: QueryContextRef,
    pub ticket: AdmissionTicketId,
    pub payload: PayloadId,
    pub semantics: EstablishSemanticFacts,
}

struct Sources {
    operation: TaskOperationId,
    context: QueryContextRef,
    ticket: AdmissionTicketId,
    payload: PayloadId,
    lease: LeaseValidFor,
    // A panic in any one destructor must not unwind through the other three.
    catalog: ManuallyDrop<Arc<dyn CodecOwnedContent>>,
    filter: ManuallyDrop<Arc<dyn CodecOwnedContent>>,
    options: ManuallyDrop<Arc<dyn CodecOwnedContent>>,
    credential: ManuallyDrop<CredentialUpdate>,
}
impl Sources {
    fn new(request: EstablishQueryContext, payload: PayloadId) -> Self {
        let EstablishQueryContextParts {
            envelope,
            context,
            admission_ticket_id,
            catalog_binding,
            initial_runtime_filter,
            query_options,
            initial_credential,
            initial_lease_valid_for,
        } = request.into_owned_parts();
        Self {
            operation: envelope.operation_id(),
            context,
            ticket: admission_ticket_id,
            payload,
            lease: initial_lease_valid_for,
            catalog: ManuallyDrop::new(catalog_binding),
            filter: ManuallyDrop::new(initial_runtime_filter),
            options: ManuallyDrop::new(query_options),
            credential: ManuallyDrop::new(initial_credential),
        }
    }
    fn project(&self) -> EstablishProjection {
        EstablishProjection {
            operation: self.operation,
            context: self.context,
            ticket: self.ticket,
            payload: self.payload,
            semantics: EstablishSemanticFacts {
                catalog: self.catalog.fingerprint(),
                shared_filter: self.filter.fingerprint(),
                query_options: self.options.fingerprint(),
                credential_lease: self.credential.lease_id(),
                credential_epoch: self.credential.epoch(),
                initial_lease_valid_for: self.lease,
            },
        }
    }
    fn shared_facts(&self) -> crate::SharedFactsRequest<'_> {
        crate::SharedFactsRequest::new(
            self.context,
            &self.catalog,
            &self.filter,
            &self.options,
            &self.credential,
        )
    }
    fn dispose(self) -> [Option<PanicPayload>; 4] {
        // Extract all fields before entering any destructor. Each remaining
        // field stays inert while another boundary unwinds.
        let Self {
            catalog,
            filter,
            options,
            credential,
            ..
        } = self;
        [
            catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(catalog))))
                .err()
                .map(ManuallyDrop::new),
            catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(filter))))
                .err()
                .map(ManuallyDrop::new),
            catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(options))))
                .err()
                .map(ManuallyDrop::new),
            catch_unwind(AssertUnwindSafe(|| {
                drop(ManuallyDrop::into_inner(credential))
            }))
            .err()
            .map(ManuallyDrop::new),
        ]
    }
}

struct SourceState {
    sources: Option<Sources>,
    authority: Option<ExitAuthority>,
    // The loan is exclusive; a source cannot be disposed during a getter.
    getter_finished: bool,
    materialization_issued: bool,
    disposal_issued: bool,
    panic: [Option<PanicPayload>; 5],
    consumer_retired: bool,
    scope_removed: bool,
}
struct Custody {
    scope: ScopeHandle,
    work: WorkId,
    ledger: RuntimeLedger,
    parent: ScopeId,
    role: ExitRole,
    report: ReportSlot,
    state: Mutex<SourceState>,
    notice: ControlRequestNotice,
    retained: Mutex<Option<Arc<Self>>>,
}
#[derive(Clone)]
#[must_use = "The original raw source child remains until real retirement"]
pub(crate) struct EstablishIngressOwner(Arc<Custody>);

#[must_use = "Retain every actually created partial child before capture"]
pub(crate) struct EstablishIngressPartial {
    pub scope: ScopeHandle,
    pub work: WorkLease,
    pub capacity: CapacityBundle,
}
pub(crate) struct EstablishIngressBootstrapRefusal {
    pub error: ExitRoleError,
    pub partial: Option<EstablishIngressPartial>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum EstablishIngressOutcome {
    Projected(EstablishProjection),
    StartRefused(ExitRoleError),
    CancelledBeforeStart,
    GetterPanicked,
}
#[must_use = "Restore the same original RPC authority before submitting its input"]
pub(crate) struct EstablishIngressReturned {
    pub authority: Option<ExitAuthority>,
    pub outcome: EstablishIngressOutcome,
}
#[must_use = "A queue refusal returns this same original authority and sender"]
pub(crate) struct EstablishIngressJob {
    owner: EstablishIngressOwner,
    context: QueryContextRef,
    authority: Option<ExitAuthority>,
    sender: Option<OwnedReturnSender<EstablishIngressReturned>>,
}
#[must_use = "Run raw field disposal only on the actual cleanup host"]
pub(crate) struct EstablishDisposalJob {
    owner: EstablishIngressOwner,
    context: QueryContextRef,
    sources: Option<Sources>,
    active: bool,
}

/// One exclusive borrow of the original RPC fields by its elected M call.
/// The original paid source child continues to own every raw capture. Moving
/// the loan neither supplies an M Work/report nor publishes materialization.
/// The admitted host job must keep this loan through its complete invocation.
#[must_use = "Return the original source loan after the actual host call"]
pub(crate) struct EstablishMaterializationLoan {
    owner: EstablishIngressOwner,
    sources: Option<Sources>,
}
/// Selection authority from the actual original Root, kept separate from the
/// raw borrow. A same-shaped request from another runtime cannot enter a
/// Context's independently admitted host roles.
pub(crate) struct ValidatedMaterializationSource {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorId,
    generation: Generation,
    establishment: ContextEstablishment,
    request: novarocks_worker_protocol::creation::RequestContext,
    loan: EstablishMaterializationLoan,
}
impl ValidatedMaterializationSource {
    pub(super) fn new(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorId,
        generation: Generation,
        establishment: ContextEstablishment,
        request: novarocks_worker_protocol::creation::RequestContext,
        loan: EstablishMaterializationLoan,
    ) -> Self {
        Self {
            runtime,
            root,
            generation,
            establishment,
            request,
            loan,
        }
    }
    pub(crate) fn shared_facts(&self) -> crate::SharedFactsRequest<'_> {
        self.loan.shared_facts()
    }
    pub(super) fn request(&self) -> novarocks_worker_protocol::creation::RequestContext {
        self.request
    }
    pub(super) fn matches(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        root: ActorId,
        context: QueryContextRef,
        generation: Generation,
        establishment: ContextEstablishment,
    ) -> bool {
        self.runtime.same_runtime(runtime)
            && self.root == root
            && self.generation == generation
            && self.establishment == establishment
            && self.shared_facts().context() == context
    }
}
impl EstablishMaterializationLoan {
    pub(crate) fn shared_facts(&self) -> crate::SharedFactsRequest<'_> {
        self.sources
            .as_ref()
            .expect("original materialization source loan")
            .shared_facts()
    }
}
impl Drop for EstablishMaterializationLoan {
    fn drop(&mut self) {
        // Only metadata moves occur under this lock. Sources uses inert field
        // wrappers, and the original self-retained child owns their explicit
        // host disposal. This also returns the borrow on a caught host panic;
        // it does not assert that the host job or its report has exited.
        let sources = self.sources.take().expect("one source loan return");
        let mut state = self.owner.0.state.lock().expect("source loan return");
        if state.sources.is_some() || state.disposal_issued {
            std::process::abort();
        }
        state.sources = Some(sources);
        drop(state);
        self.owner.0.notice.notify();
    }
}

impl EstablishIngressOwner {
    pub(super) fn phase(&self) -> ExitRolePhase {
        self.0.role.phase()
    }
    /// No request has been captured yet. Parent-link/root/report headers are
    /// drawn only from the original RPC's prepaid, fixed child inventory.
    pub(super) fn bootstrap(
        authority: &mut ExitAuthority,
        generation: Generation,
        ledger: &RuntimeLedger,
        parent: ScopeId,
        notice: ControlRequestNotice,
    ) -> Result<Self, EstablishIngressBootstrapRefusal> {
        let (scope, guard) = authority
            .open_child_scope_with_root(generation, 1, 1, WorkKind::Cleanup)
            .map_err(|error| EstablishIngressBootstrapRefusal {
                error,
                partial: None,
            })?;
        let (work, capacity) = match guard.bind(WorkKind::Cleanup) {
            Ok(parts) => parts,
            Err(_) => std::process::abort(),
        };
        let report = match ReportSlot::reserve_next(
            &scope,
            scope.gates().expect("original child gates").generation,
            work.id(),
            1,
            [(source_consumer(), ConsumerKind::Observer)],
        ) {
            Ok(report) => report,
            Err(error) => {
                return Err(EstablishIngressBootstrapRefusal {
                    error: ExitRoleError::Runtime(error),
                    partial: Some(EstablishIngressPartial {
                        scope,
                        work,
                        capacity,
                    }),
                });
            }
        };
        let work_id = work.id();
        let reported = match ReportedWork::try_new(work, capacity, report.clone()) {
            Ok(reported) => reported,
            Err(refusal) => {
                std::mem::forget(refusal);
                std::process::abort();
            }
        };
        let (role, authority) = ExitRole::from_admitted(reported);
        let shared = Arc::new(Custody {
            scope,
            work: work_id,
            ledger: ledger.clone(),
            parent,
            role,
            report,
            state: Mutex::new(SourceState {
                sources: None,
                authority: Some(authority),
                getter_finished: false,
                materialization_issued: false,
                disposal_issued: false,
                panic: [None, None, None, None, None],
                consumer_retired: false,
                scope_removed: false,
            }),
            notice,
            retained: Mutex::new(None),
        });
        *shared.retained.lock().expect("original ingress retention") = Some(shared.clone());
        Ok(Self(shared))
    }
    pub(super) fn capture(
        &self,
        request: EstablishQueryContext,
        payload: PayloadId,
        authority: ExitAuthority,
        sender: OwnedReturnSender<EstablishIngressReturned>,
    ) -> EstablishIngressJob {
        // Splitting the owned transport fields invokes no getter or Drop.
        let sources = Sources::new(request, payload);
        let context = sources.context;
        let mut state = self.0.state.lock().expect("ingress source metadata");
        if state.sources.is_some() || state.getter_finished || state.disposal_issued {
            std::mem::forget(authority);
            std::mem::forget(sender);
            std::process::abort();
        }
        state.sources = Some(sources);
        drop(state);
        EstablishIngressJob {
            owner: self.clone(),
            context,
            authority: Some(authority),
            sender: Some(sender),
        }
    }
    pub(super) fn take_disposal(&self) -> Option<EstablishDisposalJob> {
        let mut state = self.0.state.lock().expect("ingress source metadata");
        if !state.getter_finished || state.disposal_issued {
            return None;
        }
        let sources = state.sources.take()?;
        let context = sources.context;
        state.disposal_issued = true;
        Some(EstablishDisposalJob {
            owner: self.clone(),
            context,
            sources: Some(sources),
            active: true,
        })
    }
    /// Called only after the original R validates its committed Materialize
    /// ticket. A replay, an unfinished getter and a second M cannot borrow it.
    pub(super) fn take_materialization(&self) -> Option<EstablishMaterializationLoan> {
        let mut state = self.0.state.lock().expect("materialization source loan");
        if !state.getter_finished || state.disposal_issued || state.materialization_issued {
            return None;
        }
        let sources = state.sources.take()?;
        state.materialization_issued = true;
        Some(EstablishMaterializationLoan {
            owner: self.clone(),
            sources: Some(sources),
        })
    }
    pub(super) fn try_close(&self) -> Result<bool, ExitRoleError> {
        let mut state = self.0.state.lock().expect("ingress source metadata");
        if state.scope_removed {
            return Ok(true);
        }
        if !matches!(
            self.0.role.phase(),
            ExitRolePhase::Exited | ExitRolePhase::Retired
        ) {
            return Ok(false);
        }
        if !state.consumer_retired {
            self.0.report.consume(source_consumer())?;
            state.consumer_retired = true;
        }
        if self.0.role.phase() != ExitRolePhase::Retired && !self.0.role.try_retire()? {
            return Ok(false);
        }
        self.0.scope.close_external()?;
        self.0.ledger.reap_scope_ready(self.0.scope.id(), 1)?;
        self.0.ledger.remove_scope(self.0.scope.id())?;
        self.0.ledger.reap_scope_ready(self.0.parent, 1)?;
        state.scope_removed = true;
        drop(state);
        drop(
            self.0
                .retained
                .lock()
                .expect("original ingress retention")
                .take(),
        );
        Ok(true)
    }
}
impl EstablishIngressPartial {
    pub(super) fn close(mut self, ledger: &RuntimeLedger, parent: ScopeId) -> Result<(), Self> {
        // This partial was refused before any raw field or report was captured.
        let result = (|| {
            self.work.body_exited()?;
            self.work.captures_exited()?;
            self.scope.close_external()?;
            ledger.reap_scope_ready(self.scope.id(), 1)?;
            self.capacity.release(&self.capacity.resources()?)?;
            ledger.remove_scope(self.scope.id())?;
            ledger.reap_scope_ready(parent, 1)?;
            Ok::<(), RuntimeError>(())
        })();
        if result.is_err() { Err(self) } else { Ok(()) }
    }
}
impl QueuedPreparation for EstablishIngressJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        (self.owner.0.scope.id(), self.owner.0.work)
    }
    fn fairness_group(&self, _scope: ScopeId) -> super::preparation::HostFairnessGroup {
        super::preparation::HostFairnessGroup::Context(self.context)
    }
}
impl QueuedPreparation for EstablishDisposalJob {
    fn queue_key(&self) -> (ScopeId, WorkId) {
        (self.owner.0.scope.id(), self.owner.0.work)
    }
    fn fairness_group(&self, _scope: ScopeId) -> super::preparation::HostFairnessGroup {
        super::preparation::HostFairnessGroup::Context(self.context)
    }
}
impl EstablishIngressJob {
    pub(super) fn belongs_to_owner(&self, original: &EstablishIngressOwner) -> bool {
        Arc::ptr_eq(&self.owner.0, &original.0)
    }
    pub(super) fn belongs_to(&self, ready: &super::control_request::ControlRequestReady) -> bool {
        self.owner.0.notice.belongs_to(ready)
    }
    pub(crate) fn cancel_before_start(mut self) {
        self.owner
            .0
            .state
            .lock()
            .expect("ingress source metadata")
            .getter_finished = true;
        let returned = EstablishIngressReturned {
            authority: self.authority.take(),
            outcome: EstablishIngressOutcome::CancelledBeforeStart,
        };
        if let Err(refusal) = self
            .sender
            .take()
            .expect("original RPC return slot")
            .publish(returned)
        {
            std::mem::forget(refusal);
            std::process::abort();
        }
        self.owner.0.notice.notify();
    }
    pub(crate) fn run(mut self) {
        let mut authority = self.authority.take().expect("original RPC authority");
        let outcome = match authority.start() {
            Err(error) => EstablishIngressOutcome::StartRefused(error),
            Ok(()) => {
                let sources = self
                    .owner
                    .0
                    .state
                    .lock()
                    .expect("ingress source loan")
                    .sources
                    .take()
                    .expect("one original ingress source loan");
                let result = catch_unwind(AssertUnwindSafe(|| sources.project()));
                let mut state = self.owner.0.state.lock().expect("ingress source loan");
                state.sources = Some(sources);
                match result {
                    Ok(projection) => EstablishIngressOutcome::Projected(projection),
                    Err(payload) => {
                        state.panic[0] = Some(ManuallyDrop::new(payload));
                        EstablishIngressOutcome::GetterPanicked
                    }
                }
            }
        };
        self.owner
            .0
            .state
            .lock()
            .expect("ingress source metadata")
            .getter_finished = true;
        // The borrowed invocation has returned through this supervision
        // boundary, including on unwind. Its complete Sources loan was restored
        // above. Preserve the same authority until the actual RPC, raw fields
        // and retained panic payload all exit; failure is not unknown exit.
        let returned = EstablishIngressReturned {
            authority: Some(authority),
            outcome,
        };
        if let Err(refusal) = self
            .sender
            .take()
            .expect("original RPC return slot")
            .publish(returned)
        {
            std::mem::forget(refusal);
            std::process::abort();
        }
        self.owner.0.notice.notify();
    }
}
impl Drop for EstablishIngressJob {
    fn drop(&mut self) {
        if self.sender.is_some() || self.authority.is_some() {
            // Loss is an original Unknown, never unused source exit.
            drop(self.authority.take());
            drop(self.sender.take());
            self.owner.0.notice.notify();
        }
    }
}
impl EstablishDisposalJob {
    pub(super) fn belongs_to_owner(&self, original: &EstablishIngressOwner) -> bool {
        Arc::ptr_eq(&self.owner.0, &original.0)
    }
    pub(super) fn belongs_to(&self, ready: &super::control_request::ControlRequestReady) -> bool {
        self.owner.0.notice.belongs_to(ready)
    }
    pub(crate) fn run(mut self) -> bool {
        let mut authority = self
            .owner
            .0
            .state
            .lock()
            .expect("source cleanup authority")
            .authority
            .take()
            .expect("one original source cleanup authority");
        if authority.start().is_err() {
            drop(authority);
            self.owner
                .0
                .state
                .lock()
                .expect("source cleanup metadata")
                .sources = self.sources.take();
            self.active = false;
            self.owner.0.notice.notify();
            return false;
        }
        let getter_panic = self
            .owner
            .0
            .state
            .lock()
            .expect("source panic custody")
            .panic[0]
            .take();
        let panic_drop = getter_panic.and_then(|payload| {
            catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(payload)))).err()
        });
        let panics = self.sources.take().expect("one raw disposal").dispose();
        let ok = panic_drop.is_none() && panics.iter().all(Option::is_none);
        if ok {
            authority
                .exited()
                .expect("true independent raw field exits");
        } else {
            drop(authority);
            let mut state = self.owner.0.state.lock().expect("source panic custody");
            state.panic[0] = panic_drop.map(ManuallyDrop::new);
            for (index, payload) in panics.into_iter().enumerate() {
                state.panic[index + 1] = payload;
            }
        }
        self.active = false;
        self.owner.0.notice.notify();
        ok
    }
}
impl Drop for EstablishDisposalJob {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.owner.0.state.lock().expect("source disposal metadata");
            state.sources = self.sources.take();
            drop(state.authority.take());
            drop(state);
            self.owner.0.notice.notify();
        }
    }
}
