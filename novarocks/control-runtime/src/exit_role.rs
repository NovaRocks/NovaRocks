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
    sync::{Arc, Mutex},
    transaction::RuntimeError,
};

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
    /// Takes the original matching WorkLease, capacity and reserved exit
    /// report. The admission owner supplies all three from the same role;
    /// this constructor allocates no credit and signs no new responsibility.
    pub fn from_admitted(
        work: WorkLease,
        capacity: CapacityBundle,
        report: ReportSlot,
    ) -> (Self, ExitAuthority) {
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
    /// Starts only the original admitted work. A refused start changes no exit
    /// evidence, so the holder can still positively exit a never-run role.
    pub fn start(&mut self) -> Result<(), ExitRoleError> {
        let mut state = self
            .role
            .0
            .state
            .lock()
            .expect("original role custody lock");
        if state.phase != ExitRolePhase::Admitted {
            return Err(ExitRoleError::InvalidPhase(state.phase));
        }
        state.work.as_mut().expect("original role work").start()?;
        state.phase = ExitRolePhase::Started;
        Ok(())
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
