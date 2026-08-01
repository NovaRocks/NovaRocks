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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_types::UniqueId;

pub(super) trait FragmentControlHandle: Send + Sync + 'static {
    fn cancel(&self, reason: &str);
}

enum FragmentControlState {
    Reserved { pending_cancel: Option<String> },
    Running(Arc<dyn FragmentControlHandle>),
}

struct FragmentControlEntry {
    generation: u64,
    state: FragmentControlState,
}

#[derive(Default)]
struct FragmentControlRegistryState {
    next_generation: u64,
    entries: HashMap<UniqueId, FragmentControlEntry>,
}

#[derive(Default)]
pub(crate) struct FragmentControlRegistry {
    state: Mutex<FragmentControlRegistryState>,
}

impl FragmentControlRegistry {
    pub(crate) fn publish_resource_snapshot(&self) {
        let (reserved, running) = {
            let state = self.state.lock().expect("fragment control registry lock");
            (
                state
                    .entries
                    .values()
                    .filter(|entry| matches!(entry.state, FragmentControlState::Reserved { .. }))
                    .count(),
                state
                    .entries
                    .values()
                    .filter(|entry| matches!(entry.state, FragmentControlState::Running(_)))
                    .count(),
            )
        };
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_reserved",
            reserved,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_running",
            running,
        );
    }

    pub(super) fn reserve(
        self: &Arc<Self>,
        fragment_instance_id: UniqueId,
    ) -> Result<FragmentControlReservation, String> {
        let mut state = self.state.lock().expect("fragment control registry lock");
        if state.entries.contains_key(&fragment_instance_id) {
            return Err(format!(
                "native fragment control route already registered for {fragment_instance_id}"
            ));
        }
        state.next_generation = state.next_generation.wrapping_add(1).max(1);
        let generation = state.next_generation;
        state.entries.insert(
            fragment_instance_id,
            FragmentControlEntry {
                generation,
                state: FragmentControlState::Reserved {
                    pending_cancel: None,
                },
            },
        );
        let snapshot = (
            state
                .entries
                .values()
                .filter(|entry| matches!(entry.state, FragmentControlState::Reserved { .. }))
                .count(),
            state
                .entries
                .values()
                .filter(|entry| matches!(entry.state, FragmentControlState::Running(_)))
                .count(),
        );
        drop(state);
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_reserved",
            snapshot.0,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_running",
            snapshot.1,
        );
        Ok(FragmentControlReservation {
            registry: Arc::clone(self),
            fragment_instance_id,
            generation,
            published: false,
        })
    }

    pub(super) fn cancel(&self, fragment_instance_id: UniqueId, reason: &str) {
        let running = {
            let mut state = self.state.lock().expect("fragment control registry lock");
            let Some(entry) = state.entries.get_mut(&fragment_instance_id) else {
                return;
            };
            match &mut entry.state {
                FragmentControlState::Reserved { pending_cancel } => {
                    if pending_cancel.is_none() {
                        *pending_cancel = Some(reason.to_string());
                    }
                    None
                }
                FragmentControlState::Running(handle) => Some(Arc::clone(handle)),
            }
        };
        if let Some(handle) = running {
            handle.cancel(reason);
        }
    }

    pub(crate) fn cancel_many(&self, fragment_instance_ids: &[UniqueId], reason: &str) {
        for fragment_instance_id in fragment_instance_ids {
            self.cancel(*fragment_instance_id, reason);
        }
    }

    fn publish(
        &self,
        fragment_instance_id: UniqueId,
        generation: u64,
        handle: Arc<dyn FragmentControlHandle>,
    ) -> Option<String> {
        let mut state = self.state.lock().expect("fragment control registry lock");
        let entry = state
            .entries
            .get_mut(&fragment_instance_id)
            .expect("reserved fragment control route");
        assert_eq!(
            entry.generation, generation,
            "fragment control reservation generation changed before publish"
        );
        let pending_cancel = match &mut entry.state {
            FragmentControlState::Reserved { pending_cancel } => pending_cancel.take(),
            FragmentControlState::Running(_) => {
                panic!("fragment control reservation published more than once")
            }
        };
        entry.state = FragmentControlState::Running(handle);
        let snapshot = (
            state
                .entries
                .values()
                .filter(|entry| matches!(entry.state, FragmentControlState::Reserved { .. }))
                .count(),
            state
                .entries
                .values()
                .filter(|entry| matches!(entry.state, FragmentControlState::Running(_)))
                .count(),
        );
        drop(state);
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_reserved",
            snapshot.0,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_running",
            snapshot.1,
        );
        pending_cancel
    }

    fn remove_if_generation(&self, fragment_instance_id: UniqueId, generation: u64) {
        let mut state = self.state.lock().expect("fragment control registry lock");
        if state
            .entries
            .get(&fragment_instance_id)
            .is_some_and(|entry| entry.generation == generation)
        {
            state.entries.remove(&fragment_instance_id);
        }
        let snapshot = (
            state
                .entries
                .values()
                .filter(|entry| matches!(entry.state, FragmentControlState::Reserved { .. }))
                .count(),
            state
                .entries
                .values()
                .filter(|entry| matches!(entry.state, FragmentControlState::Running(_)))
                .count(),
        );
        drop(state);
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_reserved",
            snapshot.0,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "fragment_controls_running",
            snapshot.1,
        );
    }
}

pub(super) struct FragmentControlReservation {
    registry: Arc<FragmentControlRegistry>,
    fragment_instance_id: UniqueId,
    generation: u64,
    published: bool,
}

impl fmt::Debug for FragmentControlReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FragmentControlReservation")
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("generation", &self.generation)
            .field("published", &self.published)
            .finish()
    }
}

impl FragmentControlReservation {
    pub(super) fn publish(
        mut self,
        handle: Arc<dyn FragmentControlHandle>,
    ) -> FragmentControlToken {
        let pending_cancel = self.registry.publish(
            self.fragment_instance_id,
            self.generation,
            Arc::clone(&handle),
        );
        self.published = true;
        if let Some(reason) = pending_cancel {
            handle.cancel(&reason);
        }
        FragmentControlToken {
            registry: Arc::clone(&self.registry),
            fragment_instance_id: self.fragment_instance_id,
            generation: self.generation,
            active: true,
        }
    }
}

impl Drop for FragmentControlReservation {
    fn drop(&mut self) {
        if !self.published {
            self.registry
                .remove_if_generation(self.fragment_instance_id, self.generation);
        }
    }
}

pub(super) struct FragmentControlToken {
    registry: Arc<FragmentControlRegistry>,
    fragment_instance_id: UniqueId,
    generation: u64,
    active: bool,
}

impl FragmentControlToken {
    pub(super) fn complete(mut self) {
        self.registry
            .remove_if_generation(self.fragment_instance_id, self.generation);
        self.active = false;
    }
}

impl Drop for FragmentControlToken {
    fn drop(&mut self) {
        if self.active {
            self.registry
                .remove_if_generation(self.fragment_instance_id, self.generation);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use novarocks_types::UniqueId;

    use super::{FragmentControlHandle, FragmentControlRegistry};

    #[derive(Default)]
    struct RecordingHandle {
        reasons: Mutex<Vec<String>>,
    }

    impl FragmentControlHandle for RecordingHandle {
        fn cancel(&self, reason: &str) {
            self.reasons
                .lock()
                .expect("recording handle reasons")
                .push(reason.to_string());
        }
    }

    #[test]
    fn cancel_before_running_publish_reaches_the_handle() {
        let registry = Arc::new(FragmentControlRegistry::default());
        let finst_id = UniqueId::new(41, 42);
        let reservation = registry
            .reserve(finst_id)
            .expect("first reservation succeeds");

        registry.cancel(finst_id, "cancel during start");
        let handle = Arc::new(RecordingHandle::default());
        let token = reservation.publish(handle.clone());

        assert_eq!(
            *handle.reasons.lock().expect("recording handle reasons"),
            vec!["cancel during start"]
        );
        token.complete();
    }

    #[test]
    fn duplicate_registration_fails_until_the_original_reservation_rolls_back() {
        let registry = Arc::new(FragmentControlRegistry::default());
        let finst_id = UniqueId::new(51, 52);
        let reservation = registry
            .reserve(finst_id)
            .expect("first reservation succeeds");

        let error = registry
            .reserve(finst_id)
            .expect_err("duplicate reservation must fail");
        assert!(error.contains("already registered"), "{error}");

        drop(reservation);
        assert!(
            registry.reserve(finst_id).is_ok(),
            "dropping an unpublished reservation must release the route"
        );
    }
}
