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

//! Fixed physical connection positions, embedded in their original StockCore.
//! BE lane quotas aggregate exact process and method across endpoint changes.
//! Every row still retains its complete endpoint identity. No Channel, callback,
//! factory alias or independently allocated registry owner is stored here.

use std::io;
use std::sync::{Mutex, MutexGuard};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;

use crate::native_channel_identity::InlineNativeChannelIdentity;

const GEOMETRY: NativeResultSupportGeometry = NativeResultSupportGeometry::V1;
const BACKENDS: usize = GEOMETRY.transport_maximum_live_backends as usize;
const FRONTENDS: usize = GEOMETRY.transport_authenticated_live_frontends_per_backend as usize;
const ROWS: usize = BACKENDS
    * (GEOMETRY.transport_exchange_connections_per_peer
        + GEOMETRY.transport_exchange_connecting_positions_per_peer
        + GEOMETRY.transport_exchange_closing_positions_per_peer
        + GEOMETRY.transport_runtime_filter_connections_per_peer
        + GEOMETRY.transport_runtime_filter_connecting_positions_per_peer
        + GEOMETRY.transport_runtime_filter_closing_positions_per_peer) as usize
    + FRONTENDS
        * (1 + GEOMETRY.transport_connecting_positions_per_lane
            + GEOMETRY.transport_closing_positions_per_lane) as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Vacant,
    Connecting,
    Live,
    Closing,
    RetiringConnecting,
    RetiringLive,
}

#[derive(Clone, Copy)]
struct Row {
    key: Option<InlineNativeChannelIdentity>,
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

/// An exact reservation in its issuing StockCore's embedded kernel.
/// The caller must retain that exact StockCore; tokens do not own allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NativeConnectionKeySlot {
    index: usize,
    generation: u64,
}

pub(crate) struct NativeConnectionKeyCapacity {
    state: Mutex<State>,
}

#[derive(Clone, Copy)]
struct Limits {
    live: usize,
    connecting: usize,
    closing: usize,
}

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}
fn full() -> io::Error {
    io::ErrorKind::WouldBlock.into()
}

fn limits(key: InlineNativeChannelIdentity) -> io::Result<Limits> {
    match (key.peer(), key.method()) {
        (Some(_), NativeRpcMethod::DeliverExchangeUnit) => Ok(Limits {
            live: GEOMETRY.transport_exchange_connections_per_peer as usize,
            connecting: GEOMETRY.transport_exchange_connecting_positions_per_peer as usize,
            closing: GEOMETRY.transport_exchange_closing_positions_per_peer as usize,
        }),
        (Some(_), NativeRpcMethod::TransmitRuntimeFilterEnvelope) => Ok(Limits {
            live: GEOMETRY.transport_runtime_filter_connections_per_peer as usize,
            connecting: GEOMETRY.transport_runtime_filter_connecting_positions_per_peer as usize,
            closing: GEOMETRY.transport_runtime_filter_closing_positions_per_peer as usize,
        }),
        (None, NativeRpcMethod::AnnounceBackend) => Ok(Limits {
            live: 1,
            connecting: GEOMETRY.transport_connecting_positions_per_lane as usize,
            closing: GEOMETRY.transport_closing_positions_per_lane as usize,
        }),
        _ => Err(invalid()),
    }
}

fn same_lane(first: InlineNativeChannelIdentity, second: InlineNativeChannelIdentity) -> bool {
    match (first.peer(), second.peer()) {
        (Some(first_peer), Some(second_peer)) => {
            first_peer == second_peer && first.method() == second.method()
        }
        (None, None) => first == second,
        _ => false,
    }
}

impl State {
    fn row(&self, token: NativeConnectionKeySlot) -> io::Result<Row> {
        self.rows
            .get(token.index)
            .copied()
            .filter(|row| row.phase != Phase::Vacant && row.generation == token.generation)
            .ok_or_else(invalid)
    }

    fn lane_count(&self, key: InlineNativeChannelIdentity, phases: &[Phase]) -> usize {
        self.rows
            .iter()
            .filter(|row| {
                phases.contains(&row.phase) && row.key.is_some_and(|stored| same_lane(stored, key))
            })
            .count()
    }

    fn admits_peer(&self, key: InlineNativeChannelIdentity) -> bool {
        let same_peer = |stored: InlineNativeChannelIdentity| match (stored.peer(), key.peer()) {
            (Some(first), Some(second)) => first == second,
            (None, None) => stored == key,
            _ => false,
        };
        if self.rows.iter().any(|row| row.key.is_some_and(same_peer)) {
            return true;
        }
        let mut count = 0;
        for (index, row) in self.rows.iter().enumerate() {
            let Some(stored) = row.key else {
                continue;
            };
            if stored.peer().is_some() != key.peer().is_some() {
                continue;
            }
            let previously_seen = self.rows[..index].iter().any(|previous| {
                previous
                    .key
                    .is_some_and(|other| match (other.peer(), stored.peer()) {
                        (Some(first), Some(second)) => first == second,
                        (None, None) => other == stored,
                        _ => false,
                    })
            });
            if !previously_seen {
                count += 1;
            }
        }
        count
            < if key.peer().is_some() {
                BACKENDS
            } else {
                FRONTENDS
            }
    }
}

impl NativeConnectionKeyCapacity {
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

    /// Claim a physical connecting position before creating or polling IO.
    /// All active and retiring rows count toward the distinct peer limit.
    pub(crate) fn claim(
        &self,
        key: InlineNativeChannelIdentity,
    ) -> io::Result<NativeConnectionKeySlot> {
        let limits = limits(key)?;
        let mut state = self.lock()?;
        if !state.admits_peer(key)
            || state.lane_count(key, &[Phase::Connecting, Phase::RetiringConnecting])
                >= limits.connecting
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
            phase: Phase::Connecting,
        };
        Ok(NativeConnectionKeySlot { index, generation })
    }

    /// Publish Live only when its lane has room. Retirement cannot be undone.
    pub(crate) fn install(&self, token: NativeConnectionKeySlot) -> io::Result<()> {
        let mut state = self.lock()?;
        let row = state.row(token)?;
        if row.phase == Phase::Live {
            return Ok(());
        }
        if row.phase != Phase::Connecting {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        let key = row.key.ok_or_else(invalid)?;
        if state.lane_count(key, &[Phase::Live, Phase::RetiringLive]) >= limits(key)?.live {
            return Err(full());
        }
        state.rows[token.index].phase = Phase::Live;
        Ok(())
    }

    /// Move to Closing only when there is a closing position. Otherwise retain
    /// the original live/connecting charge until actual exit, without waiting.
    /// Repeated retirement is idempotent for the same still-active token.
    pub(crate) fn retire(&self, token: NativeConnectionKeySlot) -> io::Result<()> {
        let mut state = self.lock()?;
        let row = state.row(token)?;
        let fallback = match row.phase {
            Phase::Connecting => Phase::RetiringConnecting,
            Phase::Live => Phase::RetiringLive,
            Phase::Closing | Phase::RetiringConnecting | Phase::RetiringLive => return Ok(()),
            Phase::Vacant => return Err(invalid()),
        };
        let key = row.key.ok_or_else(invalid)?;
        state.rows[token.index].phase =
            if state.lane_count(key, &[Phase::Closing]) < limits(key)?.closing {
                Phase::Closing
            } else {
                fallback
            };
        Ok(())
    }

    /// Clear the physical position only after its last original owner exits.
    /// Old or already-exited tokens cannot mutate a replacement generation.
    pub(crate) fn exit(&self, token: NativeConnectionKeySlot) -> io::Result<()> {
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
    use novarocks_types::{
        BackendProcessId, CanonicalDnsName, NativeEndpoint, NativeReferenceHost,
    };

    fn key(peer: Option<u8>, method: NativeRpcMethod, port: u16) -> InlineNativeChannelIdentity {
        let peer = peer.map(|suffix| {
            let mut bytes = [0; 16];
            bytes[6] = 0x70;
            bytes[8] = 0x80;
            bytes[15] = suffix;
            BackendProcessId::try_from_bytes(bytes).unwrap()
        });
        InlineNativeChannelIdentity::from_parts(
            peer,
            &NativeEndpoint::from_host_port("127.0.0.1", port).unwrap(),
            method,
        )
        .unwrap()
    }

    fn exchange(peer: u8, port: u16) -> InlineNativeChannelIdentity {
        key(Some(peer), NativeRpcMethod::DeliverExchangeUnit, port)
    }
    fn filter(peer: u8, port: u16) -> InlineNativeChannelIdentity {
        key(
            Some(peer),
            NativeRpcMethod::TransmitRuntimeFilterEnvelope,
            port,
        )
    }
    fn membership(port: u16) -> InlineNativeChannelIdentity {
        key(None, NativeRpcMethod::AnnounceBackend, port)
    }
    fn blocked<T: std::fmt::Debug>(result: io::Result<T>) {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn frozen_rows_and_embedded_layout_are_finite() {
        assert_eq!(ROWS, 230);
    }

    #[test]
    fn endpoint_changes_share_the_exact_process_lane_quota() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        let first = capacity.claim(exchange(1, 9000)).unwrap();
        blocked(capacity.claim(exchange(1, 9001)));
        capacity.install(first).unwrap();
        let second = capacity.claim(exchange(1, 9001)).unwrap();
        capacity.install(second).unwrap();
        let third = capacity.claim(exchange(1, 9002)).unwrap();
        blocked(capacity.install(third));
        assert_eq!(
            capacity.lock().unwrap().rows[second.index].key,
            Some(exchange(1, 9001))
        );
        let other_lane = capacity.claim(filter(1, 9002)).unwrap();
        capacity.install(other_lane).unwrap();
        let extra_filter = capacity.claim(filter(1, 9003)).unwrap();
        blocked(capacity.install(extra_filter));
    }

    #[test]
    fn full_closing_keeps_retiring_live_and_connecting_charges() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        let key = exchange(1, 9000);
        let closing = capacity.claim(key).unwrap();
        capacity.install(closing).unwrap();
        capacity.retire(closing).unwrap();
        let retiring_live = capacity.claim(key).unwrap();
        capacity.install(retiring_live).unwrap();
        let live = capacity.claim(key).unwrap();
        capacity.install(live).unwrap();
        capacity.retire(retiring_live).unwrap();
        capacity.retire(retiring_live).unwrap();
        let retiring_connecting = capacity.claim(key).unwrap();
        blocked(capacity.install(retiring_connecting));
        capacity.retire(retiring_connecting).unwrap();
        capacity.retire(retiring_connecting).unwrap();
        blocked(capacity.claim(key));
        assert_eq!(
            capacity.install(retiring_live).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            capacity.install(retiring_connecting).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        capacity.exit(closing).unwrap();
        blocked(capacity.claim(key));
        capacity.exit(retiring_live).unwrap();
        blocked(capacity.claim(key));
        capacity.exit(retiring_connecting).unwrap();
        let replacement = capacity.claim(key).unwrap();
        capacity.install(replacement).unwrap();
        capacity.exit(live).unwrap();
        capacity.exit(replacement).unwrap();
    }

    #[test]
    fn exchange_and_filter_share_the_thirty_two_process_limit_through_closing() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        let tokens: [_; BACKENDS] = std::array::from_fn(|index| {
            let key = if index % 2 == 0 {
                exchange((index + 1) as u8, 9000)
            } else {
                filter((index + 1) as u8, 9000)
            };
            let token = capacity.claim(key).unwrap();
            capacity.install(token).unwrap();
            token
        });
        blocked(capacity.claim(exchange(33, 9000)));
        let shared = capacity.claim(filter(1, 9000)).unwrap();
        capacity.install(shared).unwrap();
        capacity.retire(tokens[1]).unwrap();
        blocked(capacity.claim(exchange(33, 9000)));
        capacity.exit(tokens[1]).unwrap();
        capacity.claim(exchange(33, 9000)).unwrap();
    }

    #[test]
    fn two_membership_endpoints_include_reference_host_kind() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        let ip = membership(9000);
        let dns = InlineNativeChannelIdentity::from_parts(
            None,
            &NativeEndpoint::new(
                NativeReferenceHost::Dns(CanonicalDnsName::parse("127.0.0.1").unwrap()),
                9000,
            )
            .unwrap(),
            NativeRpcMethod::AnnounceBackend,
        )
        .unwrap();
        assert_ne!(ip, dns);
        let first = capacity.claim(ip).unwrap();
        capacity.install(first).unwrap();
        let second = capacity.claim(dns).unwrap();
        capacity.install(second).unwrap();
        blocked(capacity.claim(membership(9001)));
        let connecting = capacity.claim(ip).unwrap();
        blocked(capacity.install(connecting));
        capacity.retire(first).unwrap();
        capacity.install(connecting).unwrap();
        blocked(capacity.claim(membership(9001)));
        capacity.exit(first).unwrap();
        blocked(capacity.claim(membership(9001)));
        capacity.exit(connecting).unwrap();
        capacity.claim(membership(9001)).unwrap();
    }

    #[test]
    fn all_two_hundred_thirty_physical_rows_fit_without_a_spare_position() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        let fill_lane = |key| {
            let closing = capacity.claim(key).unwrap();
            capacity.install(closing).unwrap();
            capacity.retire(closing).unwrap();
            for _ in 0..limits(key).unwrap().live {
                let live = capacity.claim(key).unwrap();
                capacity.install(live).unwrap();
            }
            capacity.claim(key).unwrap()
        };
        for peer in 1..=BACKENDS as u8 {
            fill_lane(exchange(peer, 9000));
            fill_lane(filter(peer, 9000));
        }
        fill_lane(membership(9000));
        let last = fill_lane(membership(9001));
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
        blocked(capacity.claim(exchange(1, 9001)));
        blocked(capacity.claim(exchange(33, 9000)));
        blocked(capacity.claim(membership(9002)));
        capacity.exit(last).unwrap();
        let replacement = capacity.claim(membership(9001)).unwrap();
        assert_eq!(replacement.index, last.index);
        assert!(replacement.generation > last.generation);
    }

    #[test]
    fn stale_token_cannot_modify_the_replacement_or_restore_retirement() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        let old = capacity.claim(exchange(1, 9000)).unwrap();
        capacity.exit(old).unwrap();
        let new = capacity.claim(exchange(2, 9000)).unwrap();
        assert_eq!(old.index, new.index);
        assert!(new.generation > old.generation);
        for result in [
            capacity.install(old),
            capacity.retire(old),
            capacity.exit(old),
        ] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        }
        capacity.install(new).unwrap();
        capacity.retire(new).unwrap();
        capacity.retire(new).unwrap();
        assert_eq!(
            capacity.install(new).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            capacity.lock().unwrap().rows[new.index].key,
            Some(exchange(2, 9000))
        );
        let invalid = NativeConnectionKeySlot {
            index: usize::MAX,
            generation: new.generation,
        };
        for result in [
            capacity.install(invalid),
            capacity.retire(invalid),
            capacity.exit(invalid),
        ] {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn exhausted_row_generation_never_wraps_and_full_rows_refuse() {
        let capacity = NativeConnectionKeyCapacity::new().unwrap();
        {
            let mut state = capacity.lock().unwrap();
            for row in &mut state.rows {
                row.generation = u64::MAX;
            }
        }
        blocked(capacity.claim(exchange(1, 9000)));
        {
            capacity.lock().unwrap().rows[7].generation = u64::MAX - 1;
        }
        let last = capacity.claim(exchange(1, 9000)).unwrap();
        assert_eq!(last.index, 7);
        assert_eq!(last.generation, u64::MAX);
        capacity.exit(last).unwrap();
        blocked(capacity.claim(exchange(1, 9000)));
    }
}
