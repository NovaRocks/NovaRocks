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

//! One owned payload in an original paid report. The pure slot is the only
//! retirement authority; this cell holds opaque data without interpreting it.

use crate::{
    completion::ReportSlot,
    sync::{Arc, Mutex},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    completion::{CompletionError, ReturnState},
    history::HistoryFact,
    scope::{ScopeError, WorkPhase},
};
use std::mem::ManuallyDrop;

struct Cell<T: Send + 'static> {
    payload: Option<ManuallyDrop<T>>,
    retained: Option<Arc<Shared<T>>>,
}
struct Shared<T: Send + 'static> {
    report: ReportSlot,
    cell: Mutex<Cell<T>>,
}
/// Unique producer of one original report payload. Losing it is not unused
/// revocation. No method executes a payload destructor or host callback.
#[must_use = "Publish once or explicitly revoke this original unused return"]
pub struct OwnedReturnSender<T: Send + 'static> {
    shared: Arc<Shared<T>>,
    active: bool,
}
/// Unique reader. Taking transfers the same value to its supervised caller;
/// it does not prove producer exit, consumer acknowledgement or value Drop.
#[must_use = "The original reader must take the payload or retain unknown custody"]
pub struct OwnedReturnReader<T: Send + 'static> {
    shared: Arc<Shared<T>>,
    terminal: Option<ReturnState>,
}
/// Ordinary refusal preserves both the original value and producer authority.
/// Their eventual disposal belongs to the caller, after this call unlocked.
#[must_use = "The refused original payload and sender still belong to their caller"]
pub struct OwnedReturnPublishRefusal<T: Send + 'static> {
    pub error: RuntimeError,
    pub value: T,
    pub sender: OwnedReturnSender<T>,
}
#[must_use = "The refused original sender remains owned"]
pub struct OwnedReturnRevocationRefusal<T: Send + 'static> {
    pub error: RuntimeError,
    pub sender: OwnedReturnSender<T>,
}
impl<T: Send + 'static> std::fmt::Debug for OwnedReturnSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedReturnSender")
            .field("slot", &self.shared.report.id())
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}
impl<T: Send + 'static> std::fmt::Debug for OwnedReturnReader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedReturnReader")
            .field("slot", &self.shared.report.id())
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}
impl<T: Send + 'static> std::fmt::Debug for OwnedReturnPublishRefusal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedReturnPublishRefusal")
            .field("error", &self.error)
            .field("sender", &self.sender)
            .finish_non_exhaustive()
    }
}
impl<T: Send + 'static> std::fmt::Debug for OwnedReturnRevocationRefusal<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedReturnRevocationRefusal")
            .field("error", &self.error)
            .field("sender", &self.sender)
            .finish()
    }
}
pub(crate) fn claim<T: Send + 'static>(
    report: ReportSlot,
) -> Result<(OwnedReturnSender<T>, OwnedReturnReader<T>), RuntimeError> {
    let runtime = report.runtime.clone();
    let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
    let slot = state
        .slots
        .get(&report.id)
        .ok_or(RuntimeError::UnknownSlot)?;
    let owner = state
        .scopes
        .get(&slot.scope())
        .ok_or(RuntimeError::UnknownScope)?;
    let producer = owner.work(slot.producer()).ok_or(ScopeError::UnknownWork)?;
    if producer.unbound_guard
        || producer.phase != WorkPhase::Queued
        || producer.body_exited
        || producer.captures_exited
    {
        return Err(ScopeError::InvalidTransition.into());
    }
    state
        .slots
        .get_mut(&report.id)
        .expect("original report")
        .claim_return()?;
    let shared = Arc::new(Shared {
        report,
        cell: Mutex::new(Cell {
            payload: None,
            retained: None,
        }),
    });
    // Only this original finite slot can claim the cell. All shared access
    // uses Ledger -> cell, including the initialization before publication.
    shared.cell.lock().expect("owned return cell").retained = Some(shared.clone());
    runtime.shared.record(HistoryFact::ReturnClaimed {
        slot: shared.report.id,
    });
    drop(state);
    runtime.shared.changed.notify();
    Ok((
        OwnedReturnSender {
            shared: shared.clone(),
            active: true,
        },
        OwnedReturnReader {
            shared,
            terminal: None,
        },
    ))
}
impl<T: Send + 'static> Shared<T> {
    fn lose(&self) {
        let runtime = &self.report.runtime;
        let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
        let cell = self.cell.lock().expect("owned return cell");
        let changed = if let Some(slot) = state.slots.get_mut(&self.report.id) {
            slot.lose_return()
        } else {
            // An unused revocation may already have retired its report before
            // the remaining reader drops. The original self pin was removed
            // only by a successful pure Taken/RevokedUnused transition.
            assert!(
                cell.retained.is_none(),
                "an unfinished return keeps its report paid"
            );
            false
        };
        if changed {
            runtime.shared.record(HistoryFact::ReturnUnknown {
                slot: self.report.id,
            });
        }
        drop(cell);
        drop(state);
        if changed {
            runtime.shared.changed.notify();
        }
        // Unknown retains the exact payload and self pin. No arbitrary Drop
        // or inferred producer/consumer exit is allowed on this path.
    }
    fn state(&self) -> Result<ReturnState, RuntimeError> {
        self.report.owned_return_state()
    }
}
impl<T: Send + 'static> OwnedReturnSender<T> {
    pub fn report_slot(&self) -> &ReportSlot {
        &self.shared.report
    }
    pub fn publish(mut self, value: T) -> Result<(), OwnedReturnPublishRefusal<T>> {
        let shared = self.shared.clone();
        let runtime = &shared.report.runtime;
        let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
        let checked = state
            .slots
            .get_mut(&shared.report.id)
            .ok_or(RuntimeError::UnknownSlot)
            .and_then(|slot| slot.fill_return().map_err(Into::into));
        if let Err(error) = checked {
            drop(state);
            return Err(OwnedReturnPublishRefusal {
                error,
                value,
                sender: self,
            });
        }
        let mut cell = shared.cell.lock().expect("owned return cell");
        assert!(cell.payload.is_none(), "one original payload fill");
        cell.payload = Some(ManuallyDrop::new(value));
        self.active = false;
        runtime.shared.record(HistoryFact::ReturnFilled {
            slot: shared.report.id,
        });
        drop(cell);
        drop(state);
        runtime.shared.changed.notify();
        Ok(())
    }
    /// Withdraw only the never-published return. The caller must separately
    /// observe actual role/capture exit and retire all report consumers.
    pub fn revoke_unused(mut self) -> Result<(), OwnedReturnRevocationRefusal<T>> {
        let shared = self.shared.clone();
        let runtime = &shared.report.runtime;
        let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
        let checked = state
            .slots
            .get_mut(&shared.report.id)
            .ok_or(RuntimeError::UnknownSlot)
            .and_then(|slot| slot.revoke_return_unused().map_err(Into::into));
        if let Err(error) = checked {
            drop(state);
            return Err(OwnedReturnRevocationRefusal {
                error,
                sender: self,
            });
        }
        let mut cell = shared.cell.lock().expect("owned return cell");
        assert!(cell.payload.is_none(), "unused return never held a value");
        let retained = cell.retained.take();
        self.active = false;
        runtime.shared.record(HistoryFact::ReturnRevokedUnused {
            slot: shared.report.id,
        });
        drop(cell);
        drop(state);
        runtime.shared.changed.notify();
        drop(retained);
        Ok(())
    }
}
impl<T: Send + 'static> Drop for OwnedReturnSender<T> {
    fn drop(&mut self) {
        if self.active {
            self.shared.lose();
        }
    }
}
impl<T: Send + 'static> OwnedReturnReader<T> {
    pub fn report_slot(&self) -> &ReportSlot {
        &self.shared.report
    }
    pub fn state(&self) -> Result<ReturnState, RuntimeError> {
        if let Some(terminal) = self.terminal {
            return Ok(terminal);
        }
        self.shared.state()
    }
    /// Sample RuntimeLedger::change_version before this predicate, then wait
    /// with that version only if still pending. Wakes never prove payload exit.
    pub fn try_take(&mut self) -> Result<Option<T>, RuntimeError> {
        if self.terminal.is_some() {
            return Ok(None);
        }
        let shared = self.shared.clone();
        let runtime = &shared.report.runtime;
        let mut state = runtime.shared.state.lock().expect("runtime ledger lock");
        let slot = state
            .slots
            .get_mut(&shared.report.id)
            .ok_or(RuntimeError::UnknownSlot)?;
        match slot.return_state() {
            ReturnState::Reserved => return Ok(None),
            ReturnState::RevokedUnused => {
                self.terminal = Some(ReturnState::RevokedUnused);
                return Ok(None);
            }
            ReturnState::Filled => (),
            other => return Err(CompletionError::InvalidReturnState(other).into()),
        }
        let mut cell = shared.cell.lock().expect("owned return cell");
        slot.take_return()?;
        let value = cell.payload.take().expect("the original filled payload");
        let retained = cell.retained.take();
        self.terminal = Some(ReturnState::Taken);
        runtime.shared.record(HistoryFact::ReturnTaken {
            slot: shared.report.id,
        });
        drop(cell);
        drop(state);
        runtime.shared.changed.notify();
        drop(retained);
        Ok(Some(ManuallyDrop::into_inner(value)))
    }
}
impl<T: Send + 'static> Drop for OwnedReturnReader<T> {
    fn drop(&mut self) {
        if self.terminal.is_none() {
            self.shared.lose();
        }
    }
}
