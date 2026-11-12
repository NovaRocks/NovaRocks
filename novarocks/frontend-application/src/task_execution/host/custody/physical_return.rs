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

//! Final Source consumption of an actual Native return packet. The same
//! queue and encoding partitions finish before any WC input leaves its queue.

use super::TransportPhysicalRetirement;
use crate::native::apply_send_owner::{SharedApplyFunding, SharedPhysicalReturn};
use novarocks_control_runtime::{obligation::ReservationObligation, transaction::RuntimeError};
use novarocks_workload_control::{WorkError, joint_admission::NativeQueuedInputGrant};
use std::mem::ManuallyDrop;

#[cfg(test)]
std::thread_local! {
    static INPUT_EXIT_OBSERVER: std::cell::RefCell<Option<Box<dyn FnMut()>>> = const {
        std::cell::RefCell::new(None)
    };
}

/// Observes the exact successful queue exit during the fixture's synchronous Host turn.
#[cfg(test)]
pub(crate) fn with_input_exit_observer_for_test<T>(
    observer: impl FnMut() + 'static,
    run: impl FnOnce() -> T,
) -> T {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            INPUT_EXIT_OBSERVER.with_borrow_mut(|row| drop(row.take()));
        }
    }
    INPUT_EXIT_OBSERVER.with_borrow_mut(|row| assert!(row.replace(Box::new(observer)).is_none()));
    let _clear = Clear;
    run()
}

#[derive(Debug)]
#[allow(
    dead_code,
    reason = "The retirement failure retains the typed cause while the Source owns the original resources."
)]
pub(crate) enum PhysicalReturnError {
    Financial(RuntimeError),
    Queue(WorkError),
}
impl From<RuntimeError> for PhysicalReturnError {
    fn from(value: RuntimeError) -> Self {
        Self::Financial(value)
    }
}
enum Funding {
    Encoded {
        queue: Option<ReservationObligation>,
        encoding: Option<ReservationObligation>,
    },
    UnusedQueue(Option<ReservationObligation>),
}
impl Funding {
    fn finish(&mut self) -> Result<(), RuntimeError> {
        fn finish_leg(leg: &mut Option<ReservationObligation>) -> Result<(), RuntimeError> {
            if let Some(original) = leg.as_mut() {
                let remaining = original.resources()?;
                original.release_quota_with_keyed(&remaining.quota, remaining.keyed.as_ref())?;
                original.finish()?;
                drop(leg.take());
            }
            Ok(())
        }
        match self {
            Self::Encoded { queue, encoding } => {
                finish_leg(encoding)?;
                finish_leg(queue)
            }
            Self::UnusedQueue(queue) => finish_leg(queue),
        }
    }
}
/// Checked by Native's actual cleanup completion and original financial
/// objects. Abandonment retains the same originals as UNKNOWN; it neither
/// refunds capacity nor exits any still-queued WC grant.
#[must_use = "finish the original financial legs before the original WC inputs"]
pub(crate) struct PhysicalInputRetirementObligation {
    funding: Option<Funding>,
    inputs: ManuallyDrop<Vec<NativeQueuedInputGrant>>,
    physical_retirement: Option<TransportPhysicalRetirement>,
}
impl PhysicalInputRetirementObligation {
    /// Only Native's private actual-disposal path can create this packet.
    /// Moving its existing Vec and original partitions allocates no new record.
    pub(crate) fn new(original: SharedPhysicalReturn) -> Self {
        let (funding, inputs, physical_retirement) = original.into_original();
        let funding = match funding {
            SharedApplyFunding::Encoded(original) => Funding::Encoded {
                queue: Some(original.queue),
                encoding: Some(original.encoding),
            },
            SharedApplyFunding::UnusedQueue(queue) => Funding::UnusedQueue(Some(queue)),
        };
        Self {
            funding: Some(funding),
            inputs: ManuallyDrop::new(inputs),
            physical_retirement: Some(physical_retirement),
        }
    }
    pub(super) fn unused_queue(
        queue: ReservationObligation,
        inputs: Vec<NativeQueuedInputGrant>,
        physical_retirement: TransportPhysicalRetirement,
    ) -> Self {
        Self {
            funding: Some(Funding::UnusedQueue(Some(queue))),
            inputs: ManuallyDrop::new(inputs),
            physical_retirement: Some(physical_retirement),
        }
    }
    /// One bounded Source continuation: at most two actual financial finishes
    /// and one input queue exit. The caller charges this work before entry.
    /// Source must first observe its same encoding Work's actual body/capture
    /// exit and keep that custody independently. This consumes the returned
    /// financial partitions; it cannot manufacture the child's actual exit.
    /// Errors keep all unconsumed originals in this same obligation.
    pub(crate) fn finish_one(&mut self) -> Result<bool, PhysicalReturnError> {
        if let Some(funding) = self.funding.as_mut() {
            funding.finish()?;
            drop(self.funding.take());
        }
        let notification = if let Some(input) = self.inputs.pop() {
            match input.queue_exited_in() {
                Ok(notification) => {
                    #[cfg(test)]
                    INPUT_EXIT_OBSERVER.with_borrow_mut(|row| {
                        if let Some(observer) = row {
                            observer();
                        }
                    });
                    drop(input);
                    Some(notification)
                }
                Err(error) => {
                    // pop left an existing position: restoring the same input
                    // needs no allocation and cannot dispose its unpaid queue.
                    self.inputs.push(input);
                    return Err(PhysicalReturnError::Queue(error));
                }
            }
        } else {
            None
        };
        let done = self.inputs.is_empty();
        if done && let Some(retirement) = self.physical_retirement.take() {
            retirement.finish();
        }
        if let Some(notification) = notification {
            notification.notify();
        }
        Ok(done)
    }
}
impl Drop for PhysicalInputRetirementObligation {
    fn drop(&mut self) {
        if let Some(funding) = self.funding.take() {
            // No financial partition is implicitly released on owner loss.
            std::mem::forget(funding);
        }
        if self.inputs.is_empty() {
            let original = std::mem::replace(&mut self.inputs, ManuallyDrop::new(Vec::new()));
            drop(ManuallyDrop::into_inner(original));
        }
    }
}
