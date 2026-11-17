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

//! Exclusive custody of the same prepaid logical input inventory.
//!
//! This bank grants neither Work nor activation authority. The Source makes a
//! short bounded loan; an already-authorized AP registration awaits this same
//! inventory and validates its original activation link at its own endpoint.
use super::inputs::LogicalInputs;
use novarocks_query_application::api::LogicalSourceClosed;
use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
};
use tokio::sync::Notify;

struct State {
    original: Option<LogicalInputs>,
    host_waiting: bool,
    async_closed: bool,
}

pub(crate) struct LogicalInputBank {
    state: Mutex<State>,
    available: Notify,
    host: Waker,
    admission_closed: Arc<AtomicBool>,
}
impl LogicalInputBank {
    pub(crate) fn new(original: LogicalInputs, host: Waker) -> Arc<Self> {
        let admission_closed = original.admission_gate();
        Arc::new(Self {
            state: Mutex::new(State {
                original: Some(original),
                host_waiting: false,
                async_closed: false,
            }),
            available: Notify::new(),
            host,
            admission_closed,
        })
    }
    /// Close the SAME weak-sender admission gate even while the original
    /// inventory is loaned out. This projection retires no Work or position,
    /// and an already-issued AP may still borrow until actual retirement.
    pub(super) fn close_admission_gate(&self) {
        self.admission_closed.store(true, Ordering::Release);
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// The Source never waits inside a host turn. A Busy result requests one
    /// host wake when the SAME inventory actually returns, not a retry poll.
    /// After retirement this loan remains available for pure inspection of the
    /// original empty inventory; AP callers use `loan` and receive Closed.
    pub(crate) fn try_loan(self: &Arc<Self>) -> Option<LogicalInputsLoan> {
        let mut state = self.lock();
        match state.original.take() {
            Some(original) => Some(LogicalInputsLoan {
                bank: Arc::clone(self),
                original: Some(original),
            }),
            None => {
                state.host_waiting = true;
                None
            }
        }
    }

    /// Borrow only for the caller's already-funded AP binding. Notify is
    /// enabled BEFORE checking the original, so a return cannot be lost in
    /// between the Busy check and sleeping. Every Busy recheck follows an
    /// actual availability/terminal-gate notification; there is no Busy spin,
    /// replacement inventory, queued fact or newly admitted Work here.
    pub(crate) async fn loan(self: &Arc<Self>) -> Result<LogicalInputsLoan, LogicalSourceClosed> {
        loop {
            let returned = self.available.notified();
            tokio::pin!(returned);
            returned.as_mut().enable();
            {
                let mut state = self.lock();
                if state.async_closed {
                    return Err(LogicalSourceClosed);
                }
                if let Some(original) = state.original.take() {
                    return Ok(LogicalInputsLoan {
                        bank: Arc::clone(self),
                        original: Some(original),
                    });
                }
            }
            returned.await;
        }
    }
}

/// Checked by Drop on normal return and unwinding. No method can extract,
/// clone or replace the original inventory. Kernel calls through Deref run
/// after the bank mutex has been released.
#[must_use = "Return the same original logical input inventory loan"]
pub(crate) struct LogicalInputsLoan {
    bank: Arc<LogicalInputBank>,
    original: Option<LogicalInputs>,
}
impl LogicalInputsLoan {
    /// A wake of the same original LP host, without any additional author or
    /// input position. The exclusive inventory remains this same loan.
    pub(crate) fn host_waker(&self) -> Waker {
        self.bank.host.clone()
    }
    /// Called by the Source only after all actual jobs and the AP's original
    /// input rows have closed. External close is NOT this gate: an issued
    /// activation may still complete binding under its original AP author.
    /// The bank verifies the same inventory's retirement, never reconstructs
    /// an author from a number, and does not decide a business identity.
    pub(crate) fn mark_retired(&mut self) {
        assert!(
            self.deref().is_retired(),
            "original logical input authors must actually retire before closing AP loans"
        );
        let changed = {
            let mut state = self.bank.lock();
            !std::mem::replace(&mut state.async_closed, true)
        };
        if changed {
            // Terminal availability changed. Existing AP waiters finish Closed
            // instead of sleeping for an inventory they may no longer borrow.
            self.bank.available.notify_waiters();
        }
    }
}
impl Deref for LogicalInputsLoan {
    type Target = LogicalInputs;
    fn deref(&self) -> &Self::Target {
        self.original
            .as_ref()
            .expect("same original logical inputs loan")
    }
}
impl DerefMut for LogicalInputsLoan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.original
            .as_mut()
            .expect("same original mutable logical inputs loan")
    }
}
impl Drop for LogicalInputsLoan {
    fn drop(&mut self) {
        let wake_host = {
            let mut state = self.bank.lock();
            assert!(
                state.original.is_none(),
                "one original logical input inventory loan"
            );
            state.original = Some(
                self.original
                    .take()
                    .expect("original logical inputs return once"),
            );
            std::mem::take(&mut state.host_waiting)
        };
        // Only a genuine None -> Some return (or mark_retired above) notifies
        // AP waiters. No foreign callback or kernel operation runs under the
        // bank mutex, and the original is back BEFORE a waker may unwind.
        wake_returned(|| {
            self.bank.available.notify_waiters();
            if wake_host {
                self.bank.host.wake_by_ref();
            }
        });
    }
}
fn wake_returned(wake: impl FnOnce()) {
    if std::thread::panicking() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(wake));
    } else {
        wake();
    }
}

#[cfg(test)]
mod tests;
