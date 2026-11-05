// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding
// copyright ownership. The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this
// file except in compliance with the License. You may obtain a copy at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Fixed authenticated incoming connection positions in the original StockCore.
//! A signed process and manifest lane identify a key; source ports, subjects,
//! request bodies and topology lookups are not identity authorities. Unsealed
//! connections remain under the separate original physical/acquisition gates.

use std::io;
use std::sync::{Mutex, MutexGuard};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_native_trust::NativeProcessIdentity;
use novarocks_proto_codec::native_rpc::{
    FrontendNativeLane, NATIVE_METHODS, NativeDirection, NativeEndpointDomain, NativeTrafficClass,
};

const GEOMETRY: NativeResultSupportGeometry = NativeResultSupportGeometry::V1;
const BACKENDS: usize = GEOMETRY.transport_maximum_live_backends as usize;
const FRONTENDS: usize = GEOMETRY.transport_authenticated_live_frontends_per_backend as usize;
const ROWS: usize = FRONTENDS
    * (GEOMETRY.transport_connections_per_frontend_backend_result
        + GEOMETRY.transport_connections_per_frontend_backend_submission
        + GEOMETRY.transport_connections_per_frontend_backend_observation
        + GEOMETRY.transport_connections_per_frontend_backend_lifecycle_control
        + 4 * GEOMETRY.transport_closing_positions_per_lane) as usize
    + BACKENDS
        * (GEOMETRY.transport_exchange_connections_per_peer
            + GEOMETRY.transport_exchange_closing_positions_per_peer
            + GEOMETRY.transport_runtime_filter_connections_per_peer
            + GEOMETRY.transport_runtime_filter_closing_positions_per_peer
            + 1
            + GEOMETRY.transport_closing_positions_per_lane) as usize;

/// A validated signed peer and one incoming manifest lane, shared by all of
/// that lane's methods. The endpoint domain is part of exact identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeIncomingKey {
    peer: NativeProcessIdentity,
    endpoint: NativeEndpointDomain,
    traffic: NativeTrafficClass,
}

impl NativeIncomingKey {
    pub(crate) fn new(
        peer: NativeProcessIdentity,
        endpoint: NativeEndpointDomain,
        traffic: NativeTrafficClass,
    ) -> io::Result<Self> {
        let allowed = traffic != NativeTrafficClass::Retired
            && NATIVE_METHODS.iter().any(|contract| {
                contract.endpoint == endpoint
                    && contract.traffic == traffic
                    && matches!(
                        (peer, contract.direction),
                        (
                            NativeProcessIdentity::Frontend(_),
                            NativeDirection::FrontendToBackend
                        ) | (
                            NativeProcessIdentity::Backend(_),
                            NativeDirection::BackendToBackend
                        ) | (
                            NativeProcessIdentity::Backend(_),
                            NativeDirection::BackendToFrontend
                        )
                    )
            });
        if !allowed {
            return Err(invalid());
        }
        Ok(Self {
            peer,
            endpoint,
            traffic,
        })
    }

    pub(crate) const fn peer(self) -> NativeProcessIdentity {
        self.peer
    }
    #[cfg(test)]
    pub(crate) const fn endpoint(self) -> NativeEndpointDomain {
        self.endpoint
    }
    pub(crate) const fn traffic(self) -> NativeTrafficClass {
        self.traffic
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Vacant,
    Live,
    Closing,
    RetiringLive,
}

#[derive(Clone, Copy)]
struct Row {
    key: Option<NativeIncomingKey>,
    generation: u64,
    phase: Phase,
}

impl Row {
    const VACANT: Self = Self {
        key: None,
        generation: 0,
        phase: Phase::Vacant,
    };
}
struct State {
    rows: [Row; ROWS],
}

/// Tokens refer to their exact issuing StockCore, retained by the caller.
/// They neither own a position nor prove that its physical aliases have exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeIncomingKeySlot {
    index: usize,
    generation: u64,
}

pub(crate) struct NativeIncomingKeyCapacity {
    state: Mutex<State>,
}

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}
fn full() -> io::Error {
    io::ErrorKind::WouldBlock.into()
}

fn limits(key: NativeIncomingKey) -> (usize, usize) {
    let (live, closing) = match key.traffic() {
        NativeTrafficClass::Frontend(lane) => (
            match lane {
                FrontendNativeLane::ResultData => {
                    GEOMETRY.transport_connections_per_frontend_backend_result
                }
                FrontendNativeLane::Submission => {
                    GEOMETRY.transport_connections_per_frontend_backend_submission
                }
                FrontendNativeLane::Observation => {
                    GEOMETRY.transport_connections_per_frontend_backend_observation
                }
                FrontendNativeLane::LifecycleControl => {
                    GEOMETRY.transport_connections_per_frontend_backend_lifecycle_control
                }
            },
            GEOMETRY.transport_closing_positions_per_lane,
        ),
        NativeTrafficClass::Exchange => (
            GEOMETRY.transport_exchange_connections_per_peer,
            GEOMETRY.transport_exchange_closing_positions_per_peer,
        ),
        NativeTrafficClass::RuntimeFilter => (
            GEOMETRY.transport_runtime_filter_connections_per_peer,
            GEOMETRY.transport_runtime_filter_closing_positions_per_peer,
        ),
        NativeTrafficClass::Membership => (1, GEOMETRY.transport_closing_positions_per_lane),
        NativeTrafficClass::Retired => {
            unreachable!("validated incoming keys exclude retired traffic")
        }
    };
    (live as usize, closing as usize)
}

impl State {
    fn row(&self, token: NativeIncomingKeySlot) -> io::Result<Row> {
        self.rows
            .get(token.index)
            .copied()
            .filter(|row| row.phase != Phase::Vacant && row.generation == token.generation)
            .ok_or_else(invalid)
    }
    fn count(&self, key: NativeIncomingKey, phases: &[Phase]) -> usize {
        self.rows
            .iter()
            .filter(|row| row.key == Some(key) && phases.contains(&row.phase))
            .count()
    }
    fn admits_peer(&self, key: NativeIncomingKey) -> bool {
        if self
            .rows
            .iter()
            .any(|row| row.key.is_some_and(|stored| stored.peer == key.peer))
        {
            return true;
        }
        let is_frontend = |peer| matches!(peer, NativeProcessIdentity::Frontend(_));
        let mut count = 0;
        for (index, row) in self.rows.iter().enumerate() {
            let Some(stored) = row.key else {
                continue;
            };
            if is_frontend(stored.peer) != is_frontend(key.peer()) {
                continue;
            }
            if !self.rows[..index]
                .iter()
                .any(|previous| previous.key.is_some_and(|other| other.peer == stored.peer))
            {
                count += 1;
            }
        }
        count
            < if is_frontend(key.peer()) {
                FRONTENDS
            } else {
                BACKENDS
            }
    }
}

impl NativeIncomingKeyCapacity {
    /// One fixed row table per role process.
    pub(crate) fn new() -> io::Result<Self> {
        let result = Self {
            state: Mutex::new(State {
                rows: [Row::VACANT; ROWS],
            }),
        };
        Ok(result)
    }
    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| io::ErrorKind::InvalidData.into())
    }
    /// Seal one authenticated physical connection as Live. No Connecting row
    /// is created here, and Closing/Retiring rows still retain peer identity.
    pub(crate) fn claim(&self, key: NativeIncomingKey) -> io::Result<NativeIncomingKeySlot> {
        let mut state = self.lock()?;
        if !state.admits_peer(key)
            || state.count(key, &[Phase::Live, Phase::RetiringLive]) >= limits(key).0
        {
            return Err(full());
        }
        let index = state
            .rows
            .iter()
            .position(|row| row.phase == Phase::Vacant && row.generation != u64::MAX)
            .ok_or_else(full)?;
        let generation = state.rows[index]
            .generation
            .checked_add(1)
            .ok_or_else(full)?;
        state.rows[index] = Row {
            key: Some(key),
            generation,
            phase: Phase::Live,
        };
        Ok(NativeIncomingKeySlot { index, generation })
    }
    /// Closing may free a live charge, but never the physical position. If its
    /// single Closing row is occupied, keep the retiring live charge instead.
    pub(crate) fn retire(&self, token: NativeIncomingKeySlot) -> io::Result<()> {
        let mut state = self.lock()?;
        let row = state.row(token)?;
        match row.phase {
            Phase::Closing | Phase::RetiringLive => return Ok(()),
            Phase::Live => {}
            Phase::Vacant => return Err(invalid()),
        }
        let key = row.key.ok_or_else(invalid)?;
        state.rows[token.index].phase = if state.count(key, &[Phase::Closing]) < limits(key).1 {
            Phase::Closing
        } else {
            Phase::RetiringLive
        };
        Ok(())
    }
    /// Call only after the exact physical generation's last original alias
    /// exits. A stale or already-exited token cannot alter a replacement row.
    pub(crate) fn exit(&self, token: NativeIncomingKeySlot) -> io::Result<()> {
        let mut state = self.lock()?;
        state.row(token)?;
        state.rows[token.index].key = None;
        state.rows[token.index].phase = Phase::Vacant;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use FrontendNativeLane::{LifecycleControl, Observation, ResultData, Submission};
    use NativeEndpointDomain::{BackendControl, BackendData, FrontendMembership};
    use NativeTrafficClass::{Exchange, Frontend, Membership, Retired, RuntimeFilter};
    use novarocks_types::{BackendProcessId, FrontendProcessId};

    fn peer(frontend: bool, suffix: u8) -> NativeProcessIdentity {
        let mut bytes = [0; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[15] = suffix;
        if frontend {
            NativeProcessIdentity::Frontend(FrontendProcessId::try_from_bytes(bytes).unwrap())
        } else {
            NativeProcessIdentity::Backend(BackendProcessId::try_from_bytes(bytes).unwrap())
        }
    }
    fn key(frontend: bool, suffix: u8, traffic: NativeTrafficClass) -> NativeIncomingKey {
        let endpoint = match traffic {
            Frontend(LifecycleControl) => BackendControl,
            Membership => FrontendMembership,
            _ => BackendData,
        };
        NativeIncomingKey::new(peer(frontend, suffix), endpoint, traffic).unwrap()
    }
    fn blocked<T: std::fmt::Debug>(result: io::Result<T>) {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn finite_embedded_layout_and_all_exact_live_quotas() {
        assert_eq!(ROWS, 254);
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        for (traffic, expected) in [
            (Frontend(ResultData), 4),
            (Frontend(Submission), 2),
            (Frontend(Observation), 4),
            (Frontend(LifecycleControl), 1),
        ] {
            let exact = key(true, 1, traffic);
            assert_eq!(limits(exact), (expected, 1));
            for _ in 0..expected {
                capacity.claim(exact).unwrap();
            }
            blocked(capacity.claim(exact));
        }
        for (traffic, expected) in [(Exchange, 2), (RuntimeFilter, 1), (Membership, 1)] {
            let exact = key(false, 1, traffic);
            assert_eq!(limits(exact), (expected, 1));
            for _ in 0..expected {
                capacity.claim(exact).unwrap();
            }
            blocked(capacity.claim(exact));
        }
    }

    #[test]
    fn role_and_endpoint_follow_the_actual_manifest() {
        for contract in NATIVE_METHODS {
            let frontend = contract.direction == NativeDirection::FrontendToBackend;
            let result =
                NativeIncomingKey::new(peer(frontend, 1), contract.endpoint, contract.traffic);
            assert_eq!(result.is_ok(), contract.traffic != Retired);
            assert_eq!(
                NativeIncomingKey::new(peer(!frontend, 1), contract.endpoint, contract.traffic)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        for (front, endpoint, traffic) in [
            (true, BackendData, Frontend(LifecycleControl)),
            (true, BackendControl, Frontend(ResultData)),
            (false, BackendControl, Exchange),
            (false, BackendData, Membership),
            (true, FrontendMembership, Membership),
        ] {
            assert_eq!(
                NativeIncomingKey::new(peer(front, 1), endpoint, traffic)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let exact = key(true, 1, Frontend(Observation));
        assert_eq!(exact.peer(), peer(true, 1));
        assert_eq!(exact.endpoint(), BackendData);
        assert_eq!(exact.traffic(), Frontend(Observation));
        assert_ne!(exact, key(true, 2, Frontend(Observation)));
        assert_ne!(exact, key(true, 1, Frontend(ResultData)));
        assert_ne!(peer(true, 1), peer(false, 1));
    }

    #[test]
    fn closing_alias_and_full_closing_never_fake_physical_exit() {
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        let exact = key(false, 1, RuntimeFilter);
        let closing = capacity.claim(exact).unwrap();
        capacity.retire(closing).unwrap();
        let retiring = capacity.claim(exact).unwrap();
        capacity.retire(retiring).unwrap();
        capacity.retire(retiring).unwrap();
        assert_eq!(
            capacity.lock().unwrap().rows[retiring.index].phase,
            Phase::RetiringLive
        );
        blocked(capacity.claim(exact));
        capacity.exit(closing).unwrap();
        blocked(capacity.claim(exact));
        capacity.exit(retiring).unwrap();
        capacity.claim(exact).unwrap();
    }

    #[test]
    fn identity_limits_aggregate_all_lanes_and_include_closing() {
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        let first = capacity.claim(key(true, 1, Frontend(ResultData))).unwrap();
        capacity
            .claim(key(true, 2, Frontend(LifecycleControl)))
            .unwrap();
        capacity.claim(key(true, 1, Frontend(Observation))).unwrap();
        capacity.retire(first).unwrap();
        blocked(capacity.claim(key(true, 3, Frontend(Submission))));
        capacity.exit(first).unwrap();
        blocked(capacity.claim(key(true, 3, Frontend(Submission))));
        let backends: [_; BACKENDS] = std::array::from_fn(|index| {
            capacity
                .claim(key(
                    false,
                    (index + 1) as u8,
                    if index % 3 == 0 {
                        Exchange
                    } else if index % 3 == 1 {
                        RuntimeFilter
                    } else {
                        Membership
                    },
                ))
                .unwrap()
        });
        capacity.claim(key(false, 1, Membership)).unwrap();
        capacity.retire(backends[1]).unwrap();
        blocked(capacity.claim(key(false, 33, Exchange)));
        capacity.exit(backends[1]).unwrap();
        capacity.claim(key(false, 33, Membership)).unwrap();
    }

    #[test]
    fn closing_alone_retains_the_peer_until_actual_exit() {
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        let first = capacity
            .claim(key(true, 1, Frontend(LifecycleControl)))
            .unwrap();
        capacity
            .claim(key(true, 2, Frontend(LifecycleControl)))
            .unwrap();
        capacity.retire(first).unwrap();
        blocked(capacity.claim(key(true, 3, Frontend(LifecycleControl))));
        capacity.exit(first).unwrap();
        capacity
            .claim(key(true, 3, Frontend(LifecycleControl)))
            .unwrap();
    }

    #[test]
    fn every_fixed_row_can_be_resident_without_growth() {
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        let fill = |exact| {
            let closing = capacity.claim(exact).unwrap();
            capacity.retire(closing).unwrap();
            for _ in 0..limits(exact).0 {
                capacity.claim(exact).unwrap();
            }
        };
        for p in 1..=FRONTENDS as u8 {
            for lane in [ResultData, Submission, Observation, LifecycleControl] {
                fill(key(true, p, Frontend(lane)));
            }
        }
        for p in 1..=BACKENDS as u8 {
            for traffic in [Exchange, RuntimeFilter, Membership] {
                fill(key(false, p, traffic));
            }
        }
        assert_eq!(
            capacity
                .lock()
                .unwrap()
                .rows
                .iter()
                .filter(|row| row.phase != Phase::Vacant)
                .count(),
            ROWS
        );
        blocked(capacity.claim(key(false, 1, Exchange)));
    }

    #[test]
    fn stale_generation_cannot_retire_or_exit_a_replacement() {
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        let old = capacity.claim(key(false, 1, Exchange)).unwrap();
        capacity.exit(old).unwrap();
        let new = capacity.claim(key(false, 2, Exchange)).unwrap();
        assert_eq!(old.index, new.index);
        assert!(new.generation > old.generation);
        for result in [capacity.retire(old), capacity.exit(old)] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        }
        assert_eq!(
            capacity.lock().unwrap().rows[new.index].key,
            Some(key(false, 2, Exchange))
        );
        let forged = NativeIncomingKeySlot {
            index: usize::MAX,
            generation: new.generation,
        };
        assert_eq!(
            capacity.exit(forged).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn generation_exhaustion_never_wraps_or_reuses_an_old_token() {
        let capacity = NativeIncomingKeyCapacity::new().unwrap();
        for row in &mut capacity.lock().unwrap().rows {
            row.generation = u64::MAX;
        }
        blocked(capacity.claim(key(false, 1, Exchange)));
        capacity.lock().unwrap().rows[7].generation = u64::MAX - 1;
        let last = capacity.claim(key(false, 1, Exchange)).unwrap();
        assert_eq!(last.index, 7);
        assert_eq!(last.generation, u64::MAX);
        capacity.exit(last).unwrap();
        blocked(capacity.claim(key(false, 1, Exchange)));
    }
}
