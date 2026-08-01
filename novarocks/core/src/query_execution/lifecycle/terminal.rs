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

//! Immutable, process-neutral terminal facts for one lifecycle participant.
//!
//! The lifecycle registry owns retention and delivery.  This module only owns
//! value semantics: canonical ordering, validation and the V1 digest.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};

use crate::common::types::UniqueId;
use crate::proto::novarocks;
use crate::runtime::fragment::fact::{FragmentOutcome, FragmentTerminalFact};
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::sink_commit::SinkCommitReportSnapshot;

use super::{
    ParticipantBackendIdentity, ParticipantManifestDigest, QueryExecutionId, QueryLifecycleError,
};

pub const QUERY_TERMINAL_SNAPSHOT_VERSION_V1: u32 = 1;
const QUERY_TERMINAL_SNAPSHOT_V1_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.terminal-snapshot.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryTerminalSnapshotDigest([u8; 32]);

impl QueryTerminalSnapshotDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, QueryLifecycleError> {
        let bytes = bytes.try_into().map_err(|_| {
            QueryLifecycleError::invalid_manifest("query terminal snapshot digest must be 32 bytes")
        })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentTerminalOutcome {
    Succeeded,
    Failed { code: String, detail: String },
    Cancelled { detail: String },
    IncompleteDrain { detail: String },
}

impl FragmentTerminalOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FragmentTerminalSnapshot {
    fragment_instance_id: UniqueId,
    backend_num: i32,
    outcome: FragmentTerminalOutcome,
    sink: SinkCommitReportSnapshot,
    profile: Option<RuntimeProfileTree>,
    statistics_payload: Vec<u8>,
}

impl FragmentTerminalSnapshot {
    pub fn new(
        fragment_instance_id: UniqueId,
        backend_num: i32,
        outcome: FragmentTerminalOutcome,
        sink: SinkCommitReportSnapshot,
        profile: Option<RuntimeProfileTree>,
    ) -> Result<Self, QueryLifecycleError> {
        if fragment_instance_id.high() == 0 && fragment_instance_id.low() == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal fragment instance id must be nonzero",
            ));
        }
        if backend_num < 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal fragment backend number must be nonnegative",
            ));
        }
        Ok(Self {
            fragment_instance_id,
            backend_num,
            outcome,
            sink,
            profile,
            statistics_payload: Vec::new(),
        })
    }

    pub fn from_fact(
        fact: FragmentTerminalFact,
        backend_num: i32,
        sink: SinkCommitReportSnapshot,
    ) -> Result<Self, QueryLifecycleError> {
        let outcome = match fact.outcome() {
            FragmentOutcome::Succeeded => FragmentTerminalOutcome::Succeeded,
            FragmentOutcome::Failed(error) => FragmentTerminalOutcome::Failed {
                code: "FRAGMENT_EXECUTION_FAILED".to_string(),
                detail: error.to_string(),
            },
            FragmentOutcome::Cancelled { reason } => FragmentTerminalOutcome::Cancelled {
                detail: reason.detail().to_string(),
            },
        };
        Self::new(
            fact.fragment_instance_id(),
            backend_num,
            outcome,
            sink,
            fact.profile().cloned(),
        )
        .and_then(|snapshot| snapshot.with_statistics_payload(fact.statistics_payload().to_vec()))
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn backend_num(&self) -> i32 {
        self.backend_num
    }

    pub const fn outcome(&self) -> &FragmentTerminalOutcome {
        &self.outcome
    }

    pub const fn sink(&self) -> &SinkCommitReportSnapshot {
        &self.sink
    }

    pub const fn profile(&self) -> Option<&RuntimeProfileTree> {
        self.profile.as_ref()
    }

    pub fn with_statistics_payload(
        mut self,
        statistics_payload: Vec<u8>,
    ) -> Result<Self, QueryLifecycleError> {
        if statistics_payload.len()
            > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
        {
            return Err(QueryLifecycleError::invalid_manifest(
                "terminal fragment statistics payload exceeds the connector statistics limit",
            ));
        }
        self.statistics_payload = statistics_payload;
        Ok(self)
    }

    pub fn statistics_payload(&self) -> &[u8] {
        &self.statistics_payload
    }
}

/// Reserved typed carrier.  V1 deliberately has no query-scoped profile
/// contribution; RFD-8A adds a concrete versioned value rather than opaque data.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryTerminalProfileContributionV1;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalSnapshot {
    version: u32,
    execution_id: QueryExecutionId,
    backend: ParticipantBackendIdentity,
    init_digest: ParticipantManifestDigest,
    fragments: Vec<FragmentTerminalSnapshot>,
    profile_contribution: QueryTerminalProfileContributionV1,
    digest: QueryTerminalSnapshotDigest,
}

impl QueryTerminalSnapshot {
    pub fn new(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        init_digest: ParticipantManifestDigest,
        mut fragments: Vec<FragmentTerminalSnapshot>,
    ) -> Result<Self, QueryLifecycleError> {
        fragments.sort_by_key(|fact| fact.fragment_instance_id());
        let mut ids = BTreeSet::new();
        for fragment in &fragments {
            if !ids.insert(fragment.fragment_instance_id()) {
                return Err(QueryLifecycleError::invalid_manifest(
                    "query terminal snapshot contains duplicate fragment facts",
                ));
            }
        }
        let mut snapshot = Self {
            version: QUERY_TERMINAL_SNAPSHOT_VERSION_V1,
            execution_id,
            backend,
            init_digest,
            fragments,
            profile_contribution: QueryTerminalProfileContributionV1,
            digest: QueryTerminalSnapshotDigest::new([0; 32]),
        };
        snapshot.digest = snapshot.compute_digest();
        Ok(snapshot)
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub fn fragments(&self) -> &[FragmentTerminalSnapshot] {
        &self.fragments
    }

    pub const fn digest(&self) -> QueryTerminalSnapshotDigest {
        self.digest
    }

    pub fn is_success(&self) -> bool {
        self.fragments
            .iter()
            .all(|fragment| fragment.outcome.is_success())
    }

    pub fn validate(&self) -> Result<(), QueryLifecycleError> {
        if self.version != QUERY_TERMINAL_SNAPSHOT_VERSION_V1 {
            return Err(QueryLifecycleError::invalid_manifest(
                "unsupported query terminal snapshot version",
            ));
        }
        if self.compute_digest() != self.digest {
            return Err(QueryLifecycleError::new(
                super::QueryLifecycleErrorCode::Conflict,
                "query terminal snapshot digest does not match canonical content",
            ));
        }
        Ok(())
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        put_u32(&mut bytes, self.version);
        put_i64(&mut bytes, self.execution_id.query_id().high());
        put_i64(&mut bytes, self.execution_id.query_id().low());
        put_u64(&mut bytes, self.execution_id.attempt_id().get());
        put_u64(&mut bytes, self.backend.backend_id());
        put_string(&mut bytes, self.backend.endpoint().host());
        put_u16(&mut bytes, self.backend.endpoint().port());
        put_u64(&mut bytes, self.backend.start_epoch());
        put_bytes(&mut bytes, self.init_digest.as_bytes());
        put_u64(&mut bytes, self.fragments.len() as u64);
        for fragment in &self.fragments {
            put_i64(&mut bytes, fragment.fragment_instance_id.high());
            put_i64(&mut bytes, fragment.fragment_instance_id.low());
            put_i32(&mut bytes, fragment.backend_num);
            match &fragment.outcome {
                FragmentTerminalOutcome::Succeeded => put_u8(&mut bytes, 1),
                FragmentTerminalOutcome::Failed { code, detail } => {
                    put_u8(&mut bytes, 2);
                    put_string(&mut bytes, code);
                    put_string(&mut bytes, detail);
                }
                FragmentTerminalOutcome::Cancelled { detail } => {
                    put_u8(&mut bytes, 3);
                    put_string(&mut bytes, detail);
                }
                FragmentTerminalOutcome::IncompleteDrain { detail } => {
                    put_u8(&mut bytes, 4);
                    put_string(&mut bytes, detail);
                }
            }
            put_sink(&mut bytes, &fragment.sink);
            match &fragment.profile {
                Some(profile) => {
                    put_u8(&mut bytes, 1);
                    put_profile(&mut bytes, profile);
                }
                None => put_u8(&mut bytes, 0),
            }
            put_bytes(&mut bytes, &fragment.statistics_payload);
        }
        // V1's query-scoped contribution is explicitly empty and versioned.
        put_u8(&mut bytes, 0);
        bytes
    }

    fn compute_digest(&self) -> QueryTerminalSnapshotDigest {
        let mut hasher = Sha256::new();
        hasher.update(QUERY_TERMINAL_SNAPSHOT_V1_DOMAIN);
        hasher.update(self.canonical_bytes());
        QueryTerminalSnapshotDigest::new(hasher.finalize().into())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ImmutableQueryTerminalRecord {
    snapshot: QueryTerminalSnapshot,
    encoded: Vec<u8>,
}

impl ImmutableQueryTerminalRecord {
    pub fn new(
        snapshot: QueryTerminalSnapshot,
        max_encoded_bytes: usize,
    ) -> Result<Self, QueryLifecycleError> {
        snapshot.validate()?;
        let encoded = snapshot.canonical_bytes();
        if encoded.len() > max_encoded_bytes {
            return Err(QueryLifecycleError::new(
                super::QueryLifecycleErrorCode::Capacity,
                "query terminal snapshot exceeds configured encoded-byte limit",
            ));
        }
        Ok(Self { snapshot, encoded })
    }

    pub const fn snapshot(&self) -> &QueryTerminalSnapshot {
        &self.snapshot
    }

    pub fn encoded(&self) -> &[u8] {
        &self.encoded
    }

    pub fn encoded_len(&self) -> usize {
        self.encoded.len()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalSet {
    snapshots: Vec<QueryTerminalSnapshot>,
}

impl QueryTerminalSet {
    pub fn new(mut snapshots: Vec<QueryTerminalSnapshot>) -> Result<Self, QueryLifecycleError> {
        snapshots.sort_by_key(|snapshot| {
            (
                snapshot.execution_id(),
                snapshot.backend().backend_id(),
                snapshot.backend().start_epoch(),
            )
        });
        let mut identities = BTreeSet::new();
        for snapshot in &snapshots {
            snapshot.validate()?;
            let identity = (
                snapshot.execution_id(),
                snapshot.backend().backend_id(),
                snapshot.backend().start_epoch(),
            );
            if !identities.insert(identity) {
                return Err(QueryLifecycleError::new(
                    super::QueryLifecycleErrorCode::Conflict,
                    "query terminal set contains duplicate participant identity",
                ));
            }
        }
        Ok(Self { snapshots })
    }

    pub fn snapshots(&self) -> &[QueryTerminalSnapshot] {
        &self.snapshots
    }

    pub fn fragments(&self) -> impl Iterator<Item = &FragmentTerminalSnapshot> {
        self.snapshots
            .iter()
            .flat_map(QueryTerminalSnapshot::fragments)
    }

    pub fn is_success(&self) -> bool {
        self.snapshots.iter().all(QueryTerminalSnapshot::is_success)
    }
}

fn put_sink(bytes: &mut Vec<u8>, sink: &SinkCommitReportSnapshot) {
    let mut connector = sink
        .connector_staged_report_frames
        .iter()
        .map(crate::query_execution::write::encode_connector_staged_report_frame)
        .map(|frame| canonical_connector_staged_report_frame(&frame))
        .collect::<Vec<_>>();
    connector.sort();
    put_u64(bytes, connector.len() as u64);
    for fact in connector {
        put_bytes(bytes, &fact);
    }
    let mut committed = sink
        .tablet_commit_infos
        .iter()
        .map(|fact| (fact.tablet_id, fact.backend_id))
        .collect::<Vec<_>>();
    committed.sort_unstable();
    put_u64(bytes, committed.len() as u64);
    for (tablet, backend) in committed {
        put_i64(bytes, tablet);
        put_i64(bytes, backend);
    }
    let mut failed = sink
        .tablet_fail_infos
        .iter()
        .map(|fact| (fact.tablet_id, fact.backend_id))
        .collect::<Vec<_>>();
    failed.sort_unstable();
    put_u64(bytes, failed.len() as u64);
    for (tablet, backend) in failed {
        put_i64(bytes, tablet);
        put_i64(bytes, backend);
    }
    put_i64(bytes, sink.load_stats.loaded_rows);
    put_i64(bytes, sink.load_stats.loaded_bytes);
    put_i64(bytes, sink.load_stats.filtered_rows);
}

fn canonical_connector_staged_report_frame(
    frame: &novarocks::ConnectorStagedReportFrame,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_u32(&mut bytes, frame.contract_version);
    match &frame.writer {
        Some(writer) => {
            put_u8(&mut bytes, 1);
            put_bytes(&mut bytes, &writer.operation_id);
            put_bytes(&mut bytes, &writer.cohort_id);
            put_bytes(&mut bytes, &writer.execution_query_id);
            put_u64(&mut bytes, writer.execution_attempt_id);
            match &writer.fragment_instance_id {
                Some(id) => {
                    put_u8(&mut bytes, 1);
                    put_i64(&mut bytes, id.hi);
                    put_i64(&mut bytes, id.lo);
                }
                None => put_u8(&mut bytes, 0),
            }
            put_i32(&mut bytes, writer.fragment_id);
            put_i32(&mut bytes, writer.backend_num);
            put_u32(&mut bytes, writer.sink_ordinal);
            put_string(&mut bytes, &writer.connector_instance_id);
            put_bytes(&mut bytes, &writer.connector_incarnation);
        }
        None => put_u8(&mut bytes, 0),
    }
    put_u32(&mut bytes, frame.terminal_state);
    put_u64(&mut bytes, frame.input_rows);
    put_u64(&mut bytes, frame.staged_bytes);
    put_u64(&mut bytes, frame.artifact_count);
    put_u32(&mut bytes, frame.part_index);
    put_u32(&mut bytes, frame.part_count);
    put_u64(&mut bytes, frame.logical_payload_len);
    put_bytes(&mut bytes, &frame.logical_payload_sha256);
    put_bytes(&mut bytes, &frame.frame_payload);
    put_bytes(&mut bytes, &frame.frame_payload_sha256);
    bytes
}

/// Profile wire messages contain map fields, so their ordinary prost encoding
/// is not a deterministic digest input. Encode the typed tree directly:
/// repeated counters and children keep semantic order while BTreeMap-backed
/// info strings use key order.
fn put_profile(bytes: &mut Vec<u8>, profile: &RuntimeProfileTree) {
    put_profile_node(bytes, &profile.root);
}

fn put_profile_node(bytes: &mut Vec<u8>, node: &crate::runtime::profile::ProfileNode) {
    put_string(bytes, &node.name);
    put_i32(bytes, node.node_id);
    put_u64(bytes, node.counters.len() as u64);
    for counter in &node.counters {
        put_string(bytes, &counter.name);
        put_string(bytes, &counter.parent_name);
        put_i32(bytes, counter.unit.to_proto() as i32);
        put_i64(bytes, counter.value);
        put_optional_i64(bytes, counter.min_value);
        put_optional_i64(bytes, counter.max_value);
    }
    put_u64(bytes, node.info_strings.len() as u64);
    for (key, value) in &node.info_strings {
        put_string(bytes, key);
        put_string(bytes, value);
    }
    put_u64(bytes, node.children.len() as u64);
    for child in &node.children {
        put_profile_node(bytes, child);
    }
}

fn put_optional_i64(bytes: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            put_u8(bytes, 1);
            put_i64(bytes, value);
        }
        None => put_u8(bytes, 0),
    }
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}
fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_i32(bytes: &mut Vec<u8>, value: i32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}
fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}
fn put_string(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::{AttemptId, QueryControlEndpoint};
    use crate::runtime::profile::{
        CounterStrategy, ProfileCounter, ProfileNode, ProfileUnit, RuntimeProfileTree,
    };

    fn snapshot(fragment_ids: &[i64]) -> QueryTerminalSnapshot {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let facts = fragment_ids
            .iter()
            .map(|low| {
                FragmentTerminalSnapshot::new(
                    UniqueId::new(0, *low),
                    0,
                    FragmentTerminalOutcome::Succeeded,
                    SinkCommitReportSnapshot::default(),
                    None,
                )
                .unwrap()
            })
            .collect();
        QueryTerminalSnapshot::new(
            execution,
            backend,
            ParticipantManifestDigest::new([7; 32]),
            facts,
        )
        .unwrap()
    }

    #[test]
    fn terminal_snapshot_digest_is_order_independent_for_fragment_facts() {
        let first = snapshot(&[2, 1]);
        let second = snapshot(&[1, 2]);
        assert_eq!(first.digest(), second.digest());
        assert_eq!(first.fragments()[0].fragment_instance_id().low(), 1);
    }

    #[test]
    fn terminal_snapshot_rejects_duplicate_fragments() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let fact = FragmentTerminalSnapshot::new(
            UniqueId::new(0, 1),
            0,
            FragmentTerminalOutcome::Succeeded,
            SinkCommitReportSnapshot::default(),
            None,
        )
        .unwrap();
        assert!(
            QueryTerminalSnapshot::new(
                execution,
                backend,
                ParticipantManifestDigest::new([7; 32]),
                vec![fact.clone(), fact]
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_record_enforces_encoded_limit() {
        let snapshot = snapshot(&[1]);
        assert!(ImmutableQueryTerminalRecord::new(snapshot, 1).is_err());
    }

    #[test]
    fn query_lifecycle_terminal_snapshot_profile_digest_is_canonical() {
        let execution =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
        let backend = ParticipantBackendIdentity::new(
            1,
            QueryControlEndpoint::new("127.0.0.1", 9030).unwrap(),
            1,
        )
        .unwrap();
        let profile = RuntimeProfileTree {
            root: ProfileNode {
                name: "fragment".to_string(),
                node_id: 7,
                counters: vec![ProfileCounter {
                    name: "Rows".to_string(),
                    parent_name: String::new(),
                    unit: ProfileUnit::Unit,
                    strategy: CounterStrategy::new(
                        crate::runtime::profile::CounterAggregateType::Sum,
                    ),
                    value: 11,
                    min_value: Some(3),
                    max_value: Some(8),
                }],
                info_strings: [
                    ("alpha".to_string(), "first".to_string()),
                    ("omega".to_string(), "last".to_string()),
                ]
                .into_iter()
                .collect(),
                children: vec![ProfileNode {
                    name: "child".to_string(),
                    node_id: 8,
                    counters: Vec::new(),
                    info_strings: Default::default(),
                    children: Vec::new(),
                }],
            },
        };
        let fact = FragmentTerminalSnapshot::new(
            UniqueId::new(0, 1),
            0,
            FragmentTerminalOutcome::Succeeded,
            SinkCommitReportSnapshot::default(),
            Some(profile),
        )
        .unwrap();
        let snapshot = QueryTerminalSnapshot::new(
            execution,
            backend,
            ParticipantManifestDigest::new([7; 32]),
            vec![fact],
        )
        .unwrap();

        assert_eq!(
            snapshot.digest().as_bytes(),
            &[
                65, 88, 68, 23, 208, 128, 182, 219, 96, 206, 131, 141, 227, 131, 105, 7, 103, 11,
                157, 196, 130, 194, 85, 51, 110, 231, 175, 136, 116, 234, 47, 242,
            ]
        );
        snapshot.validate().expect("profile digest stays canonical");
        let wire = super::super::encode_query_terminal_snapshot(&snapshot);
        let decoded =
            super::super::decode_query_terminal_snapshot(&wire).expect("profile wire round trip");
        assert_eq!(decoded.digest(), snapshot.digest());
        assert_eq!(decoded, snapshot);
    }
}
