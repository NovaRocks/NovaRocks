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

//! Original admission-row custody for lock-free Waker callbacks.

use super::*;

// Fields drop in this order, including unwind: actual old/new handles first,
// then the original row's registration pin. No guard is stored in State.
pub(super) struct Registration<'a> {
    old: Option<Waker>,
    new: Option<Waker>,
    _pin: RegistrationPin<'a>,
}
struct RegistrationPin<'a> {
    inner: &'a Inner,
    id: u64,
    reference_exit: bool,
}
impl Registration<'_> {
    pub(super) fn new<'a>(inner: &'a Inner, id: u64, waker: &Waker) -> Registration<'a> {
        {
            let mut state = inner.state.lock().unwrap();
            let request = state.requests.get_mut(&id).unwrap();
            assert!(!request.retired && !request.registration_in_flight);
            request.registration_in_flight = true;
        }
        let mut registration = Registration {
            old: None,
            new: None,
            _pin: RegistrationPin {
                inner,
                id,
                reference_exit: false,
            },
        };
        // RawWaker::clone is an external vtable call too.
        registration.new = Some(waker.clone());
        registration._pin.reference_exit = true;
        registration
    }

    pub(super) fn install(&mut self, request: &mut PendingAdmission) {
        let new = self.new.as_ref().unwrap();
        if request.waker.as_ref().is_none_or(|old| !old.will_wake(new)) {
            self.old = request.waker.replace(self.new.take().unwrap());
            self._pin.reference_exit = self.old.is_some();
        }
    }
}
impl Drop for RegistrationPin<'_> {
    fn drop(&mut self) {
        let removed = {
            let mut state = self.inner.state.lock().unwrap();
            state
                .requests
                .get_mut(&self.id)
                .unwrap()
                .registration_in_flight = false;
            let removed = finish_retired(&mut state, self.id);
            if removed {
                state.advance_progress_revision();
            }
            removed
        };
        if self.reference_exit {
            self.inner.notify_reclaim_progress();
        } else if removed {
            self.inner.changed.notify_waiters();
        }
    }
}

struct Delivery<'a> {
    waker: Option<Waker>,
    wake: bool,
    _pin: DeliveryPin<'a>,
}
struct DeliveryPin<'a> {
    inner: &'a Inner,
    id: u64,
}
impl Delivery<'_> {
    fn run(mut self) {
        let waker = self.waker.take().unwrap();
        if self.wake {
            waker.wake();
        } else {
            drop(waker);
        }
    }
}
impl Drop for DeliveryPin<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.requests.get_mut(&self.id).unwrap().delivery_in_flight = false;
            let removed = finish_retired(&mut state, self.id);
            if removed {
                state.advance_progress_revision();
            }
        }
        // The consumed Waker's actual ownership has exited before this pin,
        // including a callback unwind. Other pending rows remain observable.
        self.inner.notify_reclaim_progress();
    }
}

pub(crate) fn has_pending_wakes(state: &State) -> bool {
    state.requests.values().any(|request| {
        !request.delivery_in_flight
            && request.waker.is_some()
            && (request.retired || !matches!(request.state, AdmissionState::Waiting))
    })
}

pub(crate) fn drain_wakes(inner: &Inner) {
    let frontier = {
        let state = inner.state.lock().unwrap();
        state.requests.last_key_value().map(|(&id, _)| id)
    };
    let Some(frontier) = frontier else {
        return;
    };
    let mut after = 0;
    loop {
        let delivery = {
            let mut state = inner.state.lock().unwrap();
            let next = state
                .requests
                .range_mut((Excluded(after), std::ops::Bound::Included(frontier)))
                .find(|(_, request)| {
                    !request.delivery_in_flight
                        && request.waker.is_some()
                        && (request.retired || !matches!(request.state, AdmissionState::Waiting))
                });
            let Some((&id, request)) = next else {
                break;
            };
            after = id;
            request.delivery_in_flight = true;
            Delivery {
                waker: request.waker.take(),
                wake: !request.retired,
                _pin: DeliveryPin { inner, id },
            }
        };
        // Independent updates may help other rows. A reentrant callback cannot
        // take this row again; its physical tail still consumes the original W.
        delivery.run();
    }
}
