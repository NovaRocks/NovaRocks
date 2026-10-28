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

//! Actual-exit custody for one original prepaid role. The original owner keeps
//! its captures elsewhere and supplies positive proof only after their real
//! disposal. This type executes no host callback or application policy.

use crate::{
    completion::ReportSlot,
    lease::{CapacityBundle, WorkLease},
    reported_work::ReportedWork,
    sync::{Arc, Mutex},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{identity::Generation, scope::WorkKind};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitRolePhase {
    Admitted,
    Started,
    Exited,
    Unresolved,
    Retired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExitRoleError {
    InvalidPhase(ExitRolePhase),
    Runtime(RuntimeError),
}
impl From<RuntimeError> for ExitRoleError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl std::fmt::Display for ExitRoleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original role custody failure: {self:?}")
    }
}
impl std::error::Error for ExitRoleError {}

struct State {
    phase: ExitRolePhase,
    work: Option<WorkLease>,
    capacity: Option<CapacityBundle>,
    body_exited: bool,
    captures_exited: bool,
}
struct Custody {
    state: Mutex<State>,
    report: ReportSlot,
    // Unknown exit remains owned even if every external handle disappears.
    // Only successful full retirement breaks this original custody cycle.
    retained: Mutex<Option<Arc<Self>>>,
}
/// Clones address one actual role and never issue more exit authority.
#[derive(Clone)]
#[must_use = "An original role retains responsibility until actual retirement"]
pub struct ExitRole(Arc<Custody>);
impl std::fmt::Debug for ExitRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExitRole")
            .field("phase", &self.phase())
            .finish()
    }
}
/// The original holder's unique authority. Neither dropping it nor stopping
/// its scope proves that a body or its captures have exited.
#[must_use = "Dropping authority records unresolved exit and keeps all custody"]
pub struct ExitAuthority {
    role: ExitRole,
    active: bool,
}
impl std::fmt::Debug for ExitAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExitAuthority")
            .field("active", &self.active)
            .finish()
    }
}
impl ExitRole {
    /// Takes the carrier's original WorkLease, capacity and reserved report.
    /// Its binding was validated before custody or unique authority exists.
    /// Old admitted roles remain valid after stop/failure; identity is not a
    /// current gate.
    /// This constructor allocates no credit or replacement responsibility.
    pub fn from_admitted(reported: ReportedWork) -> (Self, ExitAuthority) {
        let (work, capacity, report) = reported.into_parts();
        let custody = Arc::new(Custody {
            state: Mutex::new(State {
                phase: ExitRolePhase::Admitted,
                work: Some(work),
                capacity: Some(capacity),
                body_exited: false,
                captures_exited: false,
            }),
            report,
            retained: Mutex::new(None),
        });
        *custody
            .retained
            .lock()
            .expect("original role retention lock") = Some(custody.clone());
        let role = Self(custody);
        let authority = ExitAuthority {
            role: role.clone(),
            active: true,
        };
        (role, authority)
    }
    pub fn phase(&self) -> ExitRolePhase {
        self.0
            .state
            .lock()
            .expect("original role custody lock")
            .phase
    }
    pub fn body_exited(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original role custody lock")
            .body_exited
    }
    pub fn captures_exited(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("original role custody lock")
            .captures_exited
    }
    /// Physical readiness, independently of report storage retirement. A host
    /// uses this to publish the exit fact whose real consumption subsequently
    /// allows `try_retire`; waiting for that consumption here would deadlock.
    pub fn actual_exit_ready(&self) -> Result<bool, ExitRoleError> {
        let state = self.0.state.lock().expect("original role custody lock");
        if state.phase == ExitRolePhase::Retired {
            return Ok(true);
        }
        if state.phase != ExitRolePhase::Exited {
            return Ok(false);
        }
        Ok(state
            .work
            .as_ref()
            .expect("original role work")
            .actual_exit_ready()?)
    }
    /// Actual body/capture exit is necessary but not sufficient: children and
    /// every independent report consumer must also retire before capacity can
    /// be reused. No consumer is acknowledged implicitly by this role.
    pub fn try_retire(&self) -> Result<bool, ExitRoleError> {
        let mut state = self.0.state.lock().expect("original role custody lock");
        if state.phase != ExitRolePhase::Exited {
            return Ok(false);
        }
        if !state
            .work
            .as_ref()
            .expect("original role work")
            .actual_exit_ready()?
        {
            return Ok(false);
        }
        if !self.0.report.try_retire()? {
            return Ok(false);
        }
        let capacity = state.capacity.as_mut().expect("original role capacity");
        let release = capacity
            .resources()
            .and_then(|resources| capacity.release(&resources));
        if let Err(error) = release {
            state.phase = ExitRolePhase::Unresolved;
            state
                .work
                .as_ref()
                .expect("original role work")
                .mark_unresolved();
            return Err(error.into());
        }
        let work = state.work.take();
        let capacity = state.capacity.take();
        state.phase = ExitRolePhase::Retired;
        drop(state);
        drop(work);
        drop(capacity);
        let retained = self
            .0
            .retained
            .lock()
            .expect("original role retention lock")
            .take();
        drop(retained);
        Ok(true)
    }
}
impl ExitAuthority {
    /// Derives an actual child scope from this original active holder. The
    /// caller already pays its finite parent-link/root storage; this performs
    /// no new capacity reservation beyond the existing zero-resource bootstrap.
    /// The original parent Work/capacity/report never leave their custody.
    ///
    /// This is general derivation authority, not a one-child factory: an
    /// application with one paid child position must enforce its own issuance.
    /// Cloned observation handles do not gain this authority:
    /// ```compile_fail
    /// use novarocks_control_runtime::exit_role::ExitRole;
    /// use novarocks_control_runtime_core::{identity::Generation, scope::WorkKind};
    /// fn observer(role: ExitRole, generation: Generation) {
    ///     role.open_child_scope_with_root(generation, 1, 0, WorkKind::Business);
    /// }
    /// ```
    /// Positive exit consumes the unique authority before further derivation:
    /// ```compile_fail
    /// use novarocks_control_runtime::exit_role::ExitAuthority;
    /// use novarocks_control_runtime_core::{identity::Generation, scope::WorkKind};
    /// fn after_exit(mut authority: ExitAuthority, generation: Generation) {
    ///     authority.exited().unwrap();
    ///     authority.open_child_scope_with_root(generation, 1, 0, WorkKind::Business);
    /// }
    /// ```
    pub fn open_child_scope_with_root(
        &mut self,
        generation: Generation,
        max_work: usize,
        max_slots: usize,
        kind: WorkKind,
    ) -> Result<(ScopeHandle, crate::lease::StepGuard), ExitRoleError> {
        let state = self
            .role
            .0
            .state
            .lock()
            .expect("original role custody lock");
        if !self.active
            || !matches!(
                state.phase,
                ExitRolePhase::Admitted | ExitRolePhase::Started
            )
        {
            return Err(ExitRoleError::InvalidPhase(state.phase));
        }
        let work = state.work.as_ref().expect("active original role work");
        // Same state -> ledger lock order as start/exited/retirement. The
        // ledger transaction admits and publishes its real parent link/root
        // atomically; no callback or application policy executes here.
        work.scope()
            .runtime
            .open_child_scope_with_root(work, generation, max_work, max_slots, kind)
            .map_err(Into::into)
    }

    /// Starts only the original admitted work. A refused start changes no exit
    /// evidence, so the holder can still positively exit a never-run role.
    pub fn start(&mut self) -> Result<(), ExitRoleError> {
        self.start_guarded(None)
    }
    /// Checks the delayed call's original generation and starts its Work in
    /// the same ledger transaction as scope failure and stop. Refusal leaves
    /// this authority admitted, with no positive body or capture exit.
    pub fn start_in_generation(&mut self, generation: Generation) -> Result<(), ExitRoleError> {
        self.start_guarded(Some(generation))
    }
    fn start_guarded(&mut self, generation: Option<Generation>) -> Result<(), ExitRoleError> {
        let mut state = self
            .role
            .0
            .state
            .lock()
            .expect("original role custody lock");
        if state.phase != ExitRolePhase::Admitted {
            return Err(ExitRoleError::InvalidPhase(state.phase));
        }
        let work = state.work.as_mut().expect("original role work");
        match generation {
            Some(generation) => work.start_in_generation(generation)?,
            None => work.start()?,
        }
        state.phase = ExitRolePhase::Started;
        Ok(())
    }
    /// The original never-started holder and all of its captures have actually
    /// exited without publishing a report. Revoke that reserved report rather
    /// than inventing consumption by any of its declared consumers. Children
    /// and the common reaper still retain their independent retirement duties.
    pub fn revoke_unused_and_exit(self) -> Result<(), ExitRoleError> {
        self.exit_without_publication(false)
    }
    /// The original holder's body and captures actually exited without
    /// publishing its plain report, even if its body had already started.
    /// Withdraw that unpublished report without acknowledging any consumer.
    /// An owned return payload and actual children keep their independent
    /// retirement gates; taking a return alone is never proof of this exit.
    pub fn withdraw_unpublished_and_exit(self) -> Result<(), ExitRoleError> {
        self.exit_without_publication(true)
    }
    fn exit_without_publication(mut self, allow_started: bool) -> Result<(), ExitRoleError> {
        let mut state = self
            .role
            .0
            .state
            .lock()
            .expect("original role custody lock");
        if !self.active
            || !(state.phase == ExitRolePhase::Admitted
                || allow_started && state.phase == ExitRolePhase::Started)
        {
            return Err(ExitRoleError::InvalidPhase(state.phase));
        }
        let observation = (|| -> Result<(), RuntimeError> {
            // Revoke first: a filled report must refuse before publishing any
            // new body/capture observation. Partial progress is never undone.
            self.role.0.report.revoke_unused()?;
            state
                .work
                .as_mut()
                .expect("original role work")
                .body_exited()?;
            state.body_exited = true;
            state
                .work
                .as_mut()
                .expect("original role work")
                .captures_exited()?;
            state.captures_exited = true;
            self.role.0.report.body_exited()?;
            self.role.0.report.captures_exited()?;
            Ok(())
        })();
        self.active = false;
        state.phase = if observation.is_ok() {
            ExitRolePhase::Exited
        } else {
            ExitRolePhase::Unresolved
        };
        if observation.is_err() {
            state
                .work
                .as_ref()
                .expect("original role work")
                .mark_unresolved();
        }
        observation.map_err(Into::into)
    }
    /// Positive proof from the original holder after its actual call and all
    /// captures have exited, or after a never-started unused holder truly exits.
    /// Scope failure neither supplies nor prevents this physical observation.
    pub fn exited(mut self) -> Result<(), ExitRoleError> {
        let mut state = self
            .role
            .0
            .state
            .lock()
            .expect("original role custody lock");
        if !matches!(
            state.phase,
            ExitRolePhase::Admitted | ExitRolePhase::Started
        ) {
            return Err(ExitRoleError::InvalidPhase(state.phase));
        }
        let observation = (|| -> Result<(), RuntimeError> {
            state
                .work
                .as_mut()
                .expect("original role work")
                .body_exited()?;
            state.body_exited = true;
            state
                .work
                .as_mut()
                .expect("original role work")
                .captures_exited_with_report(&self.role.0.report)?;
            state.captures_exited = true;
            self.role.0.report.body_exited()?;
            self.role.0.report.captures_exited()?;
            Ok(())
        })();
        self.active = false;
        state.phase = if observation.is_ok() {
            ExitRolePhase::Exited
        } else {
            ExitRolePhase::Unresolved
        };
        if observation.is_err() {
            state
                .work
                .as_ref()
                .expect("original role work")
                .mark_unresolved();
        }
        observation.map_err(Into::into)
    }
}
impl Drop for ExitAuthority {
    fn drop(&mut self) {
        if self.active {
            let mut state = self
                .role
                .0
                .state
                .lock()
                .expect("original role custody lock");
            if matches!(
                state.phase,
                ExitRolePhase::Admitted | ExitRolePhase::Started | ExitRolePhase::Unresolved
            ) {
                state
                    .work
                    .as_ref()
                    .expect("original role work")
                    .mark_unresolved();
                state.phase = ExitRolePhase::Unresolved;
            }
        }
    }
}
