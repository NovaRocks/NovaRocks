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

//! Original LP publication and recipient-local delivery are separate facts.
use super::root_control::{
    RootControlCommand, RootControlKind, RootControlRef, RootControlRequests, RootMaterialization,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        logical::adapter::LogicalHostEffect,
    },
    task_execution::host::observations::ObservationError,
};
use novarocks_attempt_protocol::{input::AttemptInput, port_projection::project_logical_message};
use novarocks_control_runtime::{
    actor_turn::{
        ActorFailureProof, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        StandingInputObligation,
    },
    coalescing_input::{CoalescedReceipt, CoalescingInput, Merge},
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitObligation},
    header_readiness::{HeaderReadyError, HeaderReadyRegistration},
    obligation::WorkRetirementObserver,
    publication::PublishedSnapshot,
    reported_work::{ReportedWork, ReportedWorkBindingError},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    completion::ConsumerKind,
    identity::{ConsumerId, InputId},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptComputationCut, AttemptConvergenceMode, AttemptRequestToken,
    MonotonicInstant,
    logical_port::{LogicalToAttempt, LogicalToAttemptBody},
};
use std::{
    alloc::Layout,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Debug)]
pub(crate) enum PortControlError {
    PublicationWake {
        revision: u64,
        error: ActorTurnError,
    },
    ForeignPair,
    ForeignActivation,
    ForeignRoot,
    WrongMessage,
    Closed,
    IdentityExhausted,
    ConflictingRootToken,
    ConflictingChangedToken,
    ConflictingCut,
    Unmaterialized,
    UnsettledRoot,
    Runtime(ActorTurnError),
    Storage(ReportedWorkBindingError),
    Custody(ExitCustodyError),
    Ledger(RuntimeError),
    Header(HeaderReadyError),
    Observation(ObservationError),
}
impl From<ActorTurnError> for PortControlError {
    fn from(v: ActorTurnError) -> Self {
        Self::Runtime(v)
    }
}
impl From<ExitCustodyError> for PortControlError {
    fn from(v: ExitCustodyError) -> Self {
        Self::Custody(v)
    }
}
impl From<RuntimeError> for PortControlError {
    fn from(v: RuntimeError) -> Self {
        Self::Ledger(v)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ConvergenceCommand {
    pub(crate) mode: AttemptConvergenceMode,
    pub(crate) remote_deadline: MonotonicInstant,
    pub(crate) computation_ready: Option<AttemptComputationCut>,
    pub(crate) retain_diagnostics: bool,
}
impl ConvergenceCommand {
    fn merge(&mut self, new: Self) -> Result<(), PortControlError> {
        if self.computation_ready != new.computation_ready {
            return Err(PortControlError::ConflictingCut);
        }
        if new.mode == AttemptConvergenceMode::Cleanup {
            self.mode = AttemptConvergenceMode::Cleanup;
        }
        self.remote_deadline = self.remote_deadline.min(new.remote_deadline);
        self.retain_diagnostics |= new.retain_diagnostics;
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct Versioned<T> {
    revision: u64,
    value: T,
}
#[derive(Clone, Copy)]
struct Packet {
    activation: AttemptActivationIdentity,
    revision: u64,
    convergence: Option<Versioned<ConvergenceCommand>>,
    changed: Option<Versioned<AttemptRequestToken>>,
    seal: Option<Versioned<RootControlCommand>>,
    terminal: Option<Versioned<RootControlCommand>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Convergence(ConvergenceCommand),
    Changed(AttemptRequestToken),
    Root(RootControlCommand),
}
fn parse(
    original: &LogicalToAttempt,
    root: novarocks_execution_contract::TaskIdentity,
) -> Result<Command, PortControlError> {
    Ok(match &original.body {
        LogicalToAttemptBody::BeginConvergence {
            mode,
            remote_deadline,
            computation_ready,
            retain_diagnostics,
        } => Command::Convergence(ConvergenceCommand {
            mode: *mode,
            remote_deadline: *remote_deadline,
            computation_ready: *computation_ready,
            retain_diagnostics: *retain_diagnostics,
        }),
        LogicalToAttemptBody::PreReadyReplacementChanged { token } => Command::Changed(*token),
        LogicalToAttemptBody::SealRequest { token } => Command::Root(RootControlCommand {
            token: *token,
            kind: RootControlKind::Seal,
            root,
        }),
        LogicalToAttemptBody::TerminalControlDemand {
            token,
            root: actual,
        } => {
            if *actual != root {
                return Err(PortControlError::ForeignRoot);
            }
            Command::Root(RootControlCommand {
                token: *token,
                kind: RootControlKind::Terminal,
                root,
            })
        }
        _ => return Err(PortControlError::WrongMessage),
    })
}
fn ticket_message(
    ticket: &EffectTicket<FrontendEffect>,
) -> Result<&LogicalToAttempt, PortControlError> {
    let FrontendEffect::Logical(envelope) = &ticket.effect else {
        return Err(PortControlError::WrongMessage);
    };
    let LogicalHostEffect::Protocol(effect) = &envelope.body else {
        return Err(PortControlError::WrongMessage);
    };
    let novarocks_logical_execution_protocol::effect::LogicalEffectBody::AttemptPort(original) =
        &effect.body
    else {
        return Err(PortControlError::WrongMessage);
    };
    Ok(original)
}

pub(crate) struct ControlPositions {
    pub(crate) stop: StandingInputObligation<FrontendActor>,
    pub(crate) convergence: StandingInputObligation<FrontendActor>,
    pub(crate) changed: StandingInputObligation<FrontendActor>,
}
pub(crate) struct ControlBinding {
    pub(crate) lp: ActorRegistration,
    pub(crate) ap: ActorRegistration,
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) positions: ControlPositions,
    pub(crate) storage: ReportedWork,
    pub(crate) observer: ConsumerId,
    pub(crate) roots: Arc<RootControlRequests>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerFailedStopReceipt {
    Submitted(InputId),
    AlreadyInFlight(InputId),
}
struct Publication {
    cell: Option<PublishedSnapshot<Packet>>,
    closed: bool,
    owner_failed_stop: Option<OwnerFailedStopReceipt>,
}
struct Shared {
    lp: ActorRegistration,
    ap: ActorRegistration,
    activation: AttemptActivationIdentity,
    publication: Mutex<Publication>,
    stop: Mutex<Option<StandingInputObligation<FrontendActor>>>,
    roots: Arc<RootControlRequests>,
    changed_retired: AtomicU64,
    retained: Mutex<Option<Arc<Shared>>>,
}
/// Weak publisher projection. It neither owns a second mailbox nor prolongs
/// the actual reader/storage Work. Losing the reader preserves UNKNOWN custody.
#[derive(Clone)]
pub(crate) struct SourcePublishedSlot(Weak<Shared>);
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CommittedControlReceipt {
    pub(crate) revision: u64,
}

/// Refusals preserve the exact paid storage and every original position.
#[must_use]
pub(crate) struct ControlBindingRefusal {
    pub(crate) error: PortControlError,
    pub(crate) original: Box<ControlBindingOriginal>,
}
pub(crate) enum ControlBindingOriginal {
    Unbound {
        storage: Box<ReportedWork>,
        positions: Box<ControlPositions>,
    },
    Bound {
        reader: Box<ControlReaderObligation>,
    },
}
impl std::fmt::Debug for ControlBindingRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlBindingRefusal")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}
impl SourcePublishedSlot {
    pub(crate) fn matches_pair(
        &self,
        lp: &ActorRegistration,
        ap: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.0.upgrade().is_some_and(|shared| {
            shared.lp.same_registration(lp)
                && shared.ap.same_registration(ap)
                && shared.activation == activation
        })
    }
    /// The original LP storage Work must already include its actual report and
    /// capture. All destination roles belong to the same actual AP incarnation.
    pub(crate) fn bind(
        runtime: &ActorRuntime<FrontendActor>,
        binding: ControlBinding,
    ) -> Result<(Self, ControlReaderObligation), ControlBindingRefusal> {
        let ControlBinding {
            lp,
            ap,
            activation,
            positions,
            mut storage,
            observer,
            roots,
        } = binding;
        let validation = (|| {
            runtime.validate_registration(&lp)?;
            runtime.validate_registration(&ap)?;
            if lp.same_registration(&ap)
                || !lp.matches_scope(storage.work().scope())
                || !roots.binding().registration().same_registration(&ap)
                || roots.binding().activation() != activation
                || [&positions.stop, &positions.convergence, &positions.changed]
                    .into_iter()
                    .any(|p| !p.matches_registration(&ap))
            {
                return Err(PortControlError::ForeignPair);
            }
            let profile = storage
                .bound_storage_profile()
                .map_err(PortControlError::Storage)?;
            if storage.work().kind()? != novarocks_control_runtime_core::scope::WorkKind::Settlement
                || profile.captures == 0
                || !matches!(
                    storage.report().consumer_kind(observer),
                    Ok(ConsumerKind::Observer)
                )
            {
                return Err(PortControlError::ForeignPair);
            }
            Ok(())
        })();
        if let Err(error) = validation {
            return Err(ControlBindingRefusal {
                error,
                original: Box::new(ControlBindingOriginal::Unbound {
                    storage: Box::new(storage),
                    positions: Box::new(positions),
                }),
            });
        }
        let readiness = match storage.header_readiness_registration() {
            Ok(value) => value,
            Err(error) => {
                return Err(ControlBindingRefusal {
                    error: PortControlError::Ledger(error),
                    original: Box::new(ControlBindingOriginal::Unbound {
                        storage: Box::new(storage),
                        positions: Box::new(positions),
                    }),
                });
            }
        };
        let scope = storage.work().scope().clone();
        let header = storage.work().retirement_observer();
        let report = storage.report().clone();
        let (custody, exit) = ExitCustody::from_admitted(storage);
        let packet = Packet {
            activation,
            revision: 0,
            convergence: None,
            changed: None,
            seal: None,
            terminal: None,
        };
        let shared = Arc::new(Shared {
            lp: lp.clone(),
            ap,
            activation,
            publication: Mutex::new(Publication {
                cell: Some(PublishedSnapshot::new_for_actor(&lp, Arc::new(packet))),
                closed: false,
                owner_failed_stop: None,
            }),
            stop: Mutex::new(Some(positions.stop)),
            roots,
            changed_retired: AtomicU64::new(0),
            retained: Mutex::new(None),
        });
        *shared
            .retained
            .lock()
            .expect("original control reader retention") = Some(shared.clone());
        let mut reader = ControlReaderObligation {
            shared: shared.clone(),
            convergence: Some(CoalescingInput::new(
                positions.convergence,
                convergence_input,
            )),
            changed: Some(CoalescingInput::new(positions.changed, changed_input)),
            convergence_seen: 0,
            changed_seen: 0,
            seal_seen: 0,
            terminal_seen: 0,
            changed_receipt: None,
            cursor: 0,
            remaining: 5,
            rescan_requested: false,
            observed_revision: 0,
            indexed: [None; 3],
            custody,
            exit: Some(exit),
            report,
            observer,
            scope,
            header,
            readiness,
            reader_exited: false,
            report_consumed: false,
            retired: false,
        };
        if reader.custody.phase()
            == novarocks_control_runtime::exit_custody::ExitCustodyPhase::Admitted
            && let Err(error) = reader.exit.as_mut().expect("original reader exit").start()
        {
            return Err(ControlBindingRefusal {
                error: PortControlError::Custody(error),
                original: Box::new(ControlBindingOriginal::Bound {
                    reader: Box::new(reader),
                }),
            });
        }
        Ok((Self(Arc::downgrade(&shared)), reader))
    }
    /// Actual fixed metadata payloads. The payer also quotes Arc/control-block
    /// overhead using its existing layout author, never by copying this Work.
    pub(crate) fn backing_payload_layouts() -> [Layout; 7] {
        [
            Layout::new::<Shared>(),
            Layout::new::<Mutex<Arc<Packet>>>(),
            Layout::new::<Packet>(),
            Layout::new::<Packet>(),
            Layout::new::<ControlReaderObligation>(),
            CoalescingInput::<FrontendActor, ConvergenceNotice>::backing_payload_layout(),
            CoalescingInput::<FrontendActor, ChangedNotice>::backing_payload_layout(),
        ]
    }
    /// The SAME failed LP session may publish one original Stop. A prior
    /// in-flight Stop retains its identity and reason; no Scope is stopped here.
    /// Retain the actual handoff receipt even after AP input retirement.
    pub(crate) fn publish_owner_failed_stop(
        &self,
        session: &mut novarocks_control_runtime::publication::FailurePublicationSession<
            FrontendActor,
        >,
        failure: &novarocks_coordination_contract::ExecutionFailure,
    ) -> Result<OwnerFailedStopReceipt, PortControlError> {
        let shared = self.0.upgrade().ok_or(PortControlError::Closed)?;
        if !session.registration().same_registration(&shared.lp) {
            return Err(PortControlError::ForeignPair);
        }
        let mut publication = shared
            .publication
            .lock()
            .expect("original control publication");
        if let Some(receipt) = publication.owner_failed_stop {
            return Ok(receipt);
        }
        if publication.closed {
            return Err(PortControlError::Closed);
        }
        let stop = shared.stop.lock().expect("original Stop role");
        let target = stop.as_ref().ok_or(PortControlError::Closed)?;
        let receipt = if let Some(input) = target.in_flight()? {
            OwnerFailedStopReceipt::AlreadyInFlight(input)
        } else {
            let message = LogicalToAttempt {
                activation: shared.activation,
                body: LogicalToAttemptBody::Stop {
                    reason:
                        novarocks_coordination_contract::AttemptStopReason::LogicalExecutionFailed,
                    failure: failure.clone(),
                },
            };
            // Only carrier/runtime metadata runs inside this call. A kernel
            // invariant panic is process-fatal, not an application panic that
            // could lose an indeterminate Stop publication and be retried.
            let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.fill_standing(
                    target,
                    None,
                    FrontendInput::Attempt(Box::new(AttemptInput::Port(project_logical_message(
                        &message,
                    )))),
                )
            }));
            let input = match delivered {
                Ok(Ok(input)) => input,
                Ok(Err((error, _))) => return Err(PortControlError::Runtime(error)),
                Err(original) => {
                    std::mem::forget(original);
                    std::process::abort();
                }
            };
            OwnerFailedStopReceipt::Submitted(input)
        };
        publication.owner_failed_stop = Some(receipt);
        Ok(receipt)
    }
    pub(crate) fn deliver_stop(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: LogicalToAttempt,
    ) -> Result<(), PortControlError> {
        let shared = self.0.upgrade().ok_or(PortControlError::Closed)?;
        let source = runtime.registration_from_effect(ticket)?;
        if !source.same_registration(&shared.lp) || original.activation != shared.activation {
            return Err(PortControlError::ForeignPair);
        }
        let actual = ticket_message(ticket)?;
        if actual.activation != original.activation {
            return Err(PortControlError::ForeignActivation);
        }
        match (&actual.body, &original.body) {
            (
                LogicalToAttemptBody::Stop {
                    reason: a,
                    failure: b,
                },
                LogicalToAttemptBody::Stop {
                    reason: c,
                    failure: d,
                },
            ) if a == c && b == d => {}
            _ => return Err(PortControlError::WrongMessage),
        }
        let publication = shared
            .publication
            .lock()
            .expect("original control publication");
        if publication.closed {
            return Err(PortControlError::Closed);
        }
        let stop = shared.stop.lock().expect("original Stop role");
        let target = stop.as_ref().ok_or(PortControlError::Closed)?;
        runtime
            .fill_from_effect(
                ticket,
                target,
                FrontendInput::Attempt(Box::new(AttemptInput::Port(project_logical_message(
                    &original,
                )))),
            )
            .map_err(|(error, _)| PortControlError::Runtime(error))?;
        Ok(())
    }
    /// This is the cross-owner handoff, not AP input consumption. Main must
    /// already have taken the original step guard before this Published phase.
    pub(crate) fn commit(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        original: &LogicalToAttempt,
    ) -> Result<CommittedControlReceipt, PortControlError> {
        let shared = self.0.upgrade().ok_or(PortControlError::Closed)?;
        let source = runtime.registration_from_effect(ticket)?;
        if !source.same_registration(&shared.lp) || original.activation != shared.activation {
            return Err(PortControlError::ForeignPair);
        }
        let command = parse(original, shared.roots.binding().root())?;
        let actual = ticket_message(ticket)?;
        if actual.activation != original.activation
            || parse(actual, shared.roots.binding().root())? != command
        {
            return Err(PortControlError::WrongMessage);
        }
        let receipt = {
            let publication = shared
                .publication
                .lock()
                .expect("original control publication");
            if publication.closed {
                return Err(PortControlError::Closed);
            }
            let cell = publication.cell.as_ref().ok_or(PortControlError::Closed)?;
            let mut next = *cell.read();
            let revision = next
                .revision
                .checked_add(1)
                .ok_or(PortControlError::IdentityExhausted)?;
            match command {
                Command::Convergence(mut value) => {
                    if value
                        .computation_ready
                        .is_some_and(|cut| cut.activation != shared.activation)
                    {
                        return Err(PortControlError::ForeignActivation);
                    }
                    if let Some(prior) = next.convergence {
                        let mut merged = prior.value;
                        merged.merge(value)?;
                        value = merged;
                    }
                    next.convergence = Some(Versioned { revision, value });
                }
                Command::Changed(value) => {
                    if next.changed.is_some_and(|prior| {
                        prior.value != value
                            && shared.changed_retired.load(Ordering::Acquire) < prior.revision
                    }) {
                        return Err(PortControlError::ConflictingChangedToken);
                    }
                    next.changed = Some(Versioned { revision, value });
                }
                Command::Root(value) => {
                    if [next.seal, next.terminal]
                        .into_iter()
                        .flatten()
                        .any(|prior| {
                            prior.value.token == value.token && prior.value.kind != value.kind
                        })
                    {
                        return Err(PortControlError::ConflictingRootToken);
                    }
                    let row = match value.kind {
                        RootControlKind::Seal => &mut next.seal,
                        RootControlKind::Terminal => &mut next.terminal,
                    };
                    if row.is_some_and(|prior| prior.value != value) {
                        return Err(PortControlError::ConflictingRootToken);
                    }
                    *row = Some(Versioned { revision, value });
                }
            }
            next.revision = revision;
            // Packet contains only Copy passive facts. Its replaced/rejected
            // Arc has no foreign destructor; no callback runs under this gate.
            let old = runtime
                .commit_snapshot(ticket, cell, Arc::new(next))
                .map_err(|(error, _)| PortControlError::Runtime(error))?;
            drop(old);
            CommittedControlReceipt { revision }
        };
        runtime.notify_host_actor(&shared.ap).map_err(|error| {
            PortControlError::PublicationWake {
                revision: receipt.revision,
                error,
            }
        })?;
        Ok(receipt)
    }
    /// Admission closure is serialized with a live commit, never inferred from
    /// cancellation or an observation. It does not retire the reader or Work.
    pub(crate) fn close_publication(&self) {
        if let Some(shared) = self.0.upgrade() {
            shared
                .publication
                .lock()
                .expect("original control publication")
                .closed = true;
        }
    }
}

#[derive(Clone, Copy)]
struct ConvergenceNotice {
    activation: AttemptActivationIdentity,
    command: ConvergenceCommand,
}
impl Merge for ConvergenceNotice {
    fn merge(&mut self, new: Self) {
        assert_eq!(self.activation, new.activation);
        self.command
            .merge(new.command)
            .expect("LP-committed cut remains exact");
    }
    fn unit() -> Self {
        panic!("control notifications are never Waker-authored")
    }
}
fn convergence_input(n: &ConvergenceNotice) -> FrontendInput {
    FrontendInput::Attempt(Box::new(AttemptInput::Port(project_logical_message(
        &LogicalToAttempt {
            activation: n.activation,
            body: LogicalToAttemptBody::BeginConvergence {
                mode: n.command.mode,
                remote_deadline: n.command.remote_deadline,
                computation_ready: n.command.computation_ready,
                retain_diagnostics: n.command.retain_diagnostics,
            },
        },
    ))))
}
#[derive(Clone, Copy)]
struct ChangedNotice {
    activation: AttemptActivationIdentity,
    token: AttemptRequestToken,
}
impl Merge for ChangedNotice {
    fn merge(&mut self, new: Self) {
        assert_eq!(self.activation, new.activation);
        assert_eq!(self.token, new.token);
    }
    fn unit() -> Self {
        panic!("permission hints are never Waker-authored")
    }
}
fn changed_input(n: &ChangedNotice) -> FrontendInput {
    FrontendInput::Attempt(Box::new(AttemptInput::Port(project_logical_message(
        &LogicalToAttempt {
            activation: n.activation,
            body: LogicalToAttemptBody::PreReadyReplacementChanged { token: n.token },
        },
    ))))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ControlProgress {
    pub(crate) units: usize,
    pub(crate) progressed: bool,
    pub(crate) more: bool,
}
/// The sole actual reader and its original paid storage/report/exit authority.
/// Drop supplies no producer exit and retains the original unknown custody.
#[must_use = "Close the actual reader and its original positions before retiring its storage Work"]
pub(crate) struct ControlReaderObligation {
    shared: Arc<Shared>,
    convergence: Option<CoalescingInput<FrontendActor, ConvergenceNotice>>,
    changed: Option<CoalescingInput<FrontendActor, ChangedNotice>>,
    convergence_seen: u64,
    changed_seen: u64,
    seal_seen: u64,
    terminal_seen: u64,
    changed_receipt: Option<(u64, CoalescedReceipt<FrontendActor, ChangedNotice>)>,
    cursor: usize,
    remaining: usize,
    rescan_requested: bool,
    observed_revision: u64,
    indexed: [Option<InputId>; 3],
    custody: ExitCustody,
    exit: Option<ExitObligation>,
    report: ReportSlot,
    observer: ConsumerId,
    scope: ScopeHandle,
    header: WorkRetirementObserver,
    readiness: HeaderReadyRegistration,
    reader_exited: bool,
    report_consumed: bool,
    retired: bool,
}
impl ControlReaderObligation {
    /// Called for real Source/input/header progress, not after a completed
    /// sibling scan. This keeps composite host families from restarting each other.
    pub(crate) fn begin_pass(&mut self) {
        if self.reader_exited {
            return;
        }
        if self.remaining == 0 {
            self.remaining = 5;
        } else {
            self.rescan_requested = true;
        }
    }
    pub(crate) fn index_committed(&mut self, inputs: &[InputId]) -> Result<(), PortControlError> {
        for (row, current) in [
            self.convergence
                .as_ref()
                .map(|p| p.in_flight())
                .transpose()?
                .flatten(),
            self.changed
                .as_ref()
                .map(|p| p.in_flight())
                .transpose()?
                .flatten(),
            self.shared
                .stop
                .lock()
                .expect("original Stop role")
                .as_ref()
                .map(|p| p.in_flight())
                .transpose()?
                .flatten(),
        ]
        .into_iter()
        .enumerate()
        {
            if let Some(input) = current
                && inputs.contains(&input)
            {
                if self.indexed[row].is_some_and(|prior| prior != input) {
                    return Err(PortControlError::ForeignPair);
                }
                self.indexed[row] = Some(input);
            }
        }
        if !inputs.is_empty() {
            self.begin_pass();
        }
        Ok(())
    }
    pub(crate) fn progress_ordered(
        &mut self,
        budget: usize,
    ) -> Result<ControlProgress, PortControlError> {
        let roots = self.shared.roots.clone();
        let progress = self.progress(budget, |reference| roots.enqueue_original(reference))?;
        if let Some(original) = roots.take_writer_error() {
            return Err(PortControlError::Observation(original));
        }
        Ok(progress)
    }
    pub(crate) fn progress(
        &mut self,
        budget: usize,
        mut enqueue_root: impl FnMut(RootControlRef) -> Result<(), ObservationError>,
    ) -> Result<ControlProgress, PortControlError> {
        if self.reader_exited {
            return Ok(ControlProgress {
                units: 0,
                progressed: false,
                more: false,
            });
        }
        // Copy fixed facts and release ALL snapshot/publication locks before
        // recipient kernel calls. The LP writer takes ledger -> snapshot.
        let packet = {
            let publication = self
                .shared
                .publication
                .lock()
                .expect("original control publication");
            *publication
                .cell
                .as_ref()
                .ok_or(PortControlError::Closed)?
                .read()
        };
        if packet.activation != self.shared.activation {
            return Err(PortControlError::ForeignActivation);
        }
        if packet.revision > self.observed_revision {
            self.observed_revision = packet.revision;
            if self.remaining == 0 {
                self.remaining = 5;
            } else if self.remaining < 5 {
                self.rescan_requested = true;
            }
        }
        let units = budget.min(self.remaining);
        let mut progressed = false;
        for _ in 0..units {
            let row = self.cursor;
            self.cursor = (self.cursor + 1) % 5;
            self.remaining -= 1;
            match row {
                0 => {
                    let input = self
                        .convergence
                        .as_ref()
                        .expect("original convergence role");
                    if let Some(indexed) = self.indexed[0] {
                        let retired = if input.in_flight()?.is_none() {
                            input.after_retire()?;
                            true
                        } else {
                            input.retire_indexed_after_exit(indexed)?
                        };
                        if retired {
                            self.indexed[0] = None;
                            progressed = true;
                        }
                    }
                    if let Some(v) = packet.convergence
                        && v.revision > self.convergence_seen
                    {
                        input.notify(ConvergenceNotice {
                            activation: packet.activation,
                            command: v.value,
                        })?;
                        self.convergence_seen = v.revision;
                        progressed = true;
                    }
                }
                1 => {
                    let input = self.changed.as_ref().expect("original permission role");
                    if let Some(indexed) = self.indexed[1] {
                        let retired = if input.in_flight()?.is_none() {
                            input.after_retire()?;
                            true
                        } else {
                            input.retire_indexed_after_exit(indexed)?
                        };
                        if retired {
                            self.indexed[1] = None;
                            progressed = true;
                        }
                    }
                    if self
                        .changed_receipt
                        .as_ref()
                        .is_some_and(|(_, receipt)| receipt.retired())
                    {
                        let (version, _) = self
                            .changed_receipt
                            .take()
                            .expect("original changed receipt");
                        self.shared
                            .changed_retired
                            .fetch_max(version, Ordering::Release);
                    }
                    if let Some(v) = packet.changed
                        && v.revision > self.changed_seen
                    {
                        let receipt = input.notify_tracked(ChangedNotice {
                            activation: packet.activation,
                            token: v.value,
                        })?;
                        if let Some((version, prior)) = &mut self.changed_receipt {
                            *version = v.revision;
                            prior.retain_later(receipt);
                        } else {
                            self.changed_receipt = Some((v.revision, receipt));
                        }
                        self.changed_seen = v.revision;
                        progressed = true;
                    }
                }
                // Terminal priority is recipient-local; both original commands
                // remain independently correlated in the same ordered writer.
                2 => {
                    if let Some(v) = packet.terminal
                        && v.revision > self.terminal_seen
                    {
                        match self.shared.roots.materialize(v.value, &mut enqueue_root)? {
                            RootMaterialization::Queued | RootMaterialization::AlreadyQueued => {
                                self.terminal_seen = v.revision;
                                progressed = true;
                            }
                            RootMaterialization::Blocked => {}
                        }
                    }
                }
                3 => {
                    if let Some(v) = packet.seal
                        && v.revision > self.seal_seen
                    {
                        match self.shared.roots.materialize(v.value, &mut enqueue_root)? {
                            RootMaterialization::Queued | RootMaterialization::AlreadyQueued => {
                                self.seal_seen = v.revision;
                                progressed = true;
                            }
                            RootMaterialization::Blocked => {}
                        }
                    }
                }
                _ => {
                    if let Some(stop) = self
                        .shared
                        .stop
                        .lock()
                        .expect("original Stop role")
                        .as_ref()
                    {
                        // An empty role is idle, not a new progress notification.
                        if self.indexed[2].is_some() && stop.retire_ready_after_exit()? {
                            self.indexed[2] = None;
                            progressed = true;
                        }
                    }
                }
            }
        }
        if self.remaining == 0 && std::mem::take(&mut self.rescan_requested) {
            self.remaining = 5;
        }
        Ok(ControlProgress {
            units,
            progressed,
            more: self.remaining != 0,
        })
    }
    /// Only the actual AP Source calls this after its producer/captures end.
    /// LP failure, remote closure, or a packet read cannot substitute for it.
    pub(crate) fn close_after_exit(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> Result<bool, PortControlError> {
        if self.retired {
            return Ok(true);
        }
        if !self.reader_exited {
            let packet = {
                let mut publication = self
                    .shared
                    .publication
                    .lock()
                    .expect("original control publication");
                publication.closed = true;
                *publication
                    .cell
                    .as_ref()
                    .ok_or(PortControlError::Closed)?
                    .read()
            };
            if packet
                .convergence
                .is_some_and(|v| v.revision > self.convergence_seen)
                || packet
                    .changed
                    .is_some_and(|v| v.revision > self.changed_seen)
                || packet.seal.is_some_and(|v| v.revision > self.seal_seen)
                || packet
                    .terminal
                    .is_some_and(|v| v.revision > self.terminal_seen)
            {
                return Ok(false);
            }
            self.shared.roots.close_after_exit()?;
            if !self
                .convergence
                .as_ref()
                .expect("original convergence role")
                .retire_ready_after_exit()?
                && self.convergence.as_ref().unwrap().in_flight()?.is_some()
            {
                return Ok(false);
            }
            if !self
                .changed
                .as_ref()
                .expect("original permission role")
                .retire_ready_after_exit()?
                && self.changed.as_ref().unwrap().in_flight()?.is_some()
            {
                return Ok(false);
            }
            if self.convergence.as_ref().unwrap().in_flight()?.is_some()
                || self.changed.as_ref().unwrap().in_flight()?.is_some()
            {
                return Ok(false);
            }
            {
                let mut stop = self.shared.stop.lock().expect("original Stop role");
                if !stop
                    .as_ref()
                    .expect("original Stop role")
                    .retire_ready_after_exit()?
                {
                    return Ok(false);
                }
                stop.as_mut().unwrap().close_after_exit()?;
                stop.take();
            }
            self.convergence.as_ref().unwrap().close_after_exit()?;
            self.changed.as_ref().unwrap().close_after_exit()?;
            self.convergence = None;
            self.changed = None;
            self.changed_receipt = None;
            self.shared
                .publication
                .lock()
                .expect("original control publication")
                .cell = None;
            self.exit
                .take()
                .expect("same original reader exit authority")
                .exited()?;
            self.reader_exited = true;
        }
        self.finish_retirement(runtime)
    }
    fn finish_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
    ) -> Result<bool, PortControlError> {
        if !self.report_consumed {
            self.report.consume(self.observer)?;
            self.report_consumed = true;
        }
        if !self.custody.try_retire()? || !self.scope.reap_observed_header(&self.header)? {
            self.readiness
                .arm(runtime, &self.shared.ap, &self.header)
                .map_err(PortControlError::Header)?;
            return Ok(false);
        }
        self.readiness.cancel();
        self.retired = true;
        self.shared
            .retained
            .lock()
            .expect("original reader retention")
            .take();
        Ok(true)
    }
    /// Failure closes only this exact AP reader. No LP failure proof can
    /// abandon a still-healthy recipient's already-published commands.
    pub(crate) fn close_after_failure(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        proof: &ActorFailureProof,
    ) -> Result<bool, PortControlError> {
        if !proof.registration().same_registration(&self.shared.ap) {
            return Err(PortControlError::ForeignPair);
        }
        if self.reader_exited {
            return self.finish_retirement(runtime);
        }
        self.shared
            .publication
            .lock()
            .expect("original control publication")
            .closed = true;
        self.shared.roots.close_after_failure();
        // Each role has only the passive input constructed in this module.
        let convergence = self.convergence.as_ref().unwrap();
        convergence.abandon_pending_after_failure();
        if let Some(payload) = convergence.take_failed_role_payload()? {
            payload.dispose()?;
        }
        convergence.retire_failed_after_exit()?;
        let changed = self.changed.as_ref().unwrap();
        changed.abandon_pending_after_failure();
        if let Some(payload) = changed.take_failed_role_payload()? {
            payload.dispose()?;
        }
        changed.retire_failed_after_exit()?;
        {
            let stop = self.shared.stop.lock().expect("original Stop role");
            if let Some(payload) = stop.as_ref().unwrap().take_failed_role_payload()? {
                payload.dispose()?;
            }
            if !stop.as_ref().unwrap().retire_ready_after_exit()? {
                return Ok(false);
            }
        }
        // Published pending commands are abandoned only under the original AP
        // failure fence; local roles still prove their own actual retirement.
        self.convergence_seen = u64::MAX;
        self.changed_seen = u64::MAX;
        self.seal_seen = u64::MAX;
        self.terminal_seen = u64::MAX;
        self.close_after_exit(runtime)
    }
}

#[cfg(test)]
pub(crate) mod tests;
