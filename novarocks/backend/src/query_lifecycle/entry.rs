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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use novarocks::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode};
use novarocks_protocol::lifecycle::{
    FragmentLiveObservation, FragmentTerminalSnapshot, ParticipantManifest,
    ParticipantManifestDigest, ParticipantTerminalOutcome, QueryControlEvent, QueryInitOutcome,
    QueryTerminalSnapshot, QueryTerminationReason, StageDigest,
};
use novarocks_types::UniqueId;
use prost::Message;

use super::stage::StartGate;
use crate::runtime_filter::participant::RuntimeFilterParticipant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecyclePhase {
    Initializing,
    Initialized,
    ControlAttached,
    Staging,
    Staged,
    Running,
    TerminalRetained,
    Terminating,
    Tombstone,
}

pub(crate) struct QueryLifecycleEntry {
    pub(crate) digest: ParticipantManifestDigest,
    pub(crate) manifest: ParticipantManifest,
    /// Backend-local execution routing IDs.  The manifest remains the
    /// canonical Protocol value; this converts generated IDs once for the
    /// BE runtime maps that key on `novarocks_types::UniqueId`.
    pub(crate) expected_fragment_instance_ids: Vec<UniqueId>,
    pub(crate) state: Mutex<QueryLifecycleEntryState>,
    pub(crate) init_completed: Condvar,
    pub(crate) stage_completed: Condvar,
    /// Wakes the deferred fallback as soon as TerminalAck releases the
    /// immutable record, so an acknowledged snapshot does not retain a
    /// sleeping fallback worker for the full ACK timeout.
    pub(crate) terminal_delivery_completed: Condvar,
}

/// Backend-owned retention bookkeeping for an already sealed Protocol
/// terminal snapshot.  It deliberately adds no lifecycle value model: the
/// snapshot itself is the validated generated Protocol carrier.
#[derive(Clone, Debug)]
pub(crate) struct ImmutableQueryTerminalRecord {
    snapshot: QueryTerminalSnapshot,
    encoded_len: usize,
}

impl ImmutableQueryTerminalRecord {
    pub(crate) fn new(
        snapshot: QueryTerminalSnapshot,
        max_encoded_bytes: usize,
    ) -> Result<Self, QueryLifecycleError> {
        let encoded_len = snapshot.as_proto().encoded_len();
        if encoded_len > max_encoded_bytes {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "query terminal snapshot exceeds retained-record byte limit",
            ));
        }
        Ok(Self {
            snapshot,
            encoded_len,
        })
    }

    pub(crate) const fn snapshot(&self) -> &QueryTerminalSnapshot {
        &self.snapshot
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        self.encoded_len
    }
}

pub(crate) struct QueryLifecycleEntryState {
    pub(crate) phase: QueryLifecyclePhase,
    pub(crate) init_outcome: Option<QueryInitOutcome>,
    pub(crate) termination_reason: Option<QueryTerminationReason>,
    /// Published with a successful Init and owned by this full execution
    /// attempt. No process-global context owns a second Service.
    pub(crate) runtime_filter: Option<Arc<RuntimeFilterParticipant>>,
    /// True after a non-empty RF install succeeds. It distinguishes a real
    /// RF-less participant from an illegal cleanup-before-capture state.
    pub(crate) runtime_filter_installed: bool,
    pub(crate) runtime_filter_close_in_flight: bool,
    pub(crate) ever_initialized: bool,
    pub(crate) terminated_at: Option<Instant>,
    pub(crate) in_flight_fragments: BTreeSet<UniqueId>,
    pub(crate) accepted_fragments: BTreeSet<UniqueId>,
    /// Immutable fragment terminal facts are admitted exactly once per staged
    /// instance.  QLC-4 uses this set to publish LocalDrained before Finalize;
    /// it is intentionally separate from routing removal.
    pub(crate) completed_fragments: BTreeSet<UniqueId>,
    pub(crate) local_drained_emitted: bool,
    /// A running attempt that has observed a failed/cancelled fragment keeps
    /// its entry until terminal facts drain or the bounded drain deadline
    /// synthesizes explicit IncompleteDrain facts.
    pub(crate) failure_drain_scheduled: bool,
    pub(crate) terminal_facts: BTreeMap<UniqueId, FragmentTerminalSnapshot>,
    /// First-wins latch covering capture, canonical encoding, and retained
    /// record installation. Expensive capture never runs under this lock.
    pub(crate) terminal_freeze_in_flight: bool,
    pub(crate) terminal_record: Option<ImmutableQueryTerminalRecord>,
    /// The immutable delivery carrier is always retained, including when P1
    /// snapshot formation failed and only a negative attestation is available.
    pub(crate) terminal_outcome: Option<ParticipantTerminalOutcome>,
    pub(crate) pre_start_deadline: Option<Instant>,
    pub(crate) last_heartbeat: Option<Instant>,
    pub(crate) frontend_owner_epoch: Option<u64>,
    pub(crate) events: Option<tokio::sync::mpsc::Sender<QueryControlEvent>>,
    /// Latest-only telemetry slot, intentionally independent from correctness
    /// delivery permits. `watch` replaces an unread observation without making
    /// a fragment producer wait for the control-stream consumer.
    pub(crate) observations: Option<tokio::sync::watch::Sender<Option<FragmentLiveObservation>>>,
    pub(crate) observation_sequences: BTreeMap<UniqueId, u64>,
    /// LocalDrained is a correctness barrier, not best-effort telemetry. Keep
    /// one queue slot reserved so heartbeat ACK backpressure cannot drop it.
    pub(crate) local_drained_event_permit:
        Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
    /// TerminalSnapshot must remain deliverable even when the normal event
    /// budget is saturated. Unary fallback is recovery, not a substitute for
    /// a reliable attached control stream.
    pub(crate) terminal_snapshot_event_permit:
        Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
    pub(crate) terminal_event_permit: Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
    /// The opaque QLC-3 batch identity.  The state-only slice owns no fragment
    /// workspace yet, but it still has to make Stage and Start idempotent.
    pub(crate) stage_digest: Option<StageDigest>,
    pub(crate) start_gate: Option<Arc<StartGate>>,
    /// Backend-global accounting retained for a successfully staged bundle.
    /// It is released exactly when Start or terminal cleanup wins.
    pub(crate) stage_resources: Option<super::registry::StageResourceReservation>,
}

impl QueryLifecycleEntry {
    pub(crate) fn initializing(
        manifest: ParticipantManifest,
        digest: ParticipantManifestDigest,
    ) -> Self {
        let expected_fragment_instance_ids = manifest
            .expected_fragment_instance_ids()
            .into_iter()
            .map(|value| UniqueId::new(value.hi, value.lo))
            .collect();
        Self {
            digest,
            manifest,
            expected_fragment_instance_ids,
            state: Mutex::new(QueryLifecycleEntryState {
                phase: QueryLifecyclePhase::Initializing,
                init_outcome: None,
                termination_reason: None,
                runtime_filter: None,
                runtime_filter_installed: false,
                runtime_filter_close_in_flight: false,
                ever_initialized: false,
                terminated_at: None,
                in_flight_fragments: BTreeSet::new(),
                accepted_fragments: BTreeSet::new(),
                completed_fragments: BTreeSet::new(),
                local_drained_emitted: false,
                failure_drain_scheduled: false,
                terminal_facts: BTreeMap::new(),
                terminal_freeze_in_flight: false,
                terminal_record: None,
                terminal_outcome: None,
                pre_start_deadline: None,
                last_heartbeat: None,
                frontend_owner_epoch: None,
                events: None,
                observations: None,
                observation_sequences: BTreeMap::new(),
                local_drained_event_permit: None,
                terminal_snapshot_event_permit: None,
                terminal_event_permit: None,
                stage_digest: None,
                start_gate: None,
                stage_resources: None,
            }),
            init_completed: Condvar::new(),
            stage_completed: Condvar::new(),
            terminal_delivery_completed: Condvar::new(),
        }
    }
}
