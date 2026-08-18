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

//! Frontend lifecycle completion aggregation.
//!
//! Native lifecycle wire values remain in `lifecycle::terminal`. This module
//! owns the coordinator-only aggregate reconstructed after protocol ingress
//! validates every participant terminal snapshot.

use std::collections::BTreeSet;

use novarocks_protocol::lifecycle::QueryTerminalSnapshot as ProtocolQueryTerminalSnapshot;

use crate::query_lifecycle::terminal::{
    FragmentTerminalOutcome, FragmentTerminalSnapshot, QueryTerminalSnapshot,
    decode_fragment_terminal_profile_telemetry,
    decode_query_terminal_profile_contribution_telemetry,
};
use crate::query_lifecycle::{
    ParticipantBackendIdentity, ParticipantManifestDigest, QueryExecutionId, QueryLifecycleError,
    QueryLifecycleErrorCode,
};

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
                    QueryLifecycleErrorCode::Conflict,
                    "query terminal set contains duplicate participant identity",
                ));
            }
        }
        Ok(Self { snapshots })
    }

    /// Rebuilds the coordinator-only lease aggregate after Protocol validates
    /// its canonical snapshot. No RPC boundary carries this projection.
    pub fn from_protocol_snapshots(
        snapshots: Vec<ProtocolQueryTerminalSnapshot>,
    ) -> Result<Self, QueryLifecycleError> {
        snapshots
            .into_iter()
            .map(|snapshot| decode_protocol_terminal_snapshot_projection(snapshot.as_proto()))
            .collect::<Result<Vec<_>, _>>()
            .and_then(Self::new)
    }

    pub fn snapshots(&self) -> &[QueryTerminalSnapshot] {
        &self.snapshots
    }

    pub fn fragments(
        &self,
    ) -> impl Iterator<Item = &crate::query_lifecycle::FragmentTerminalSnapshot> {
        self.snapshots
            .iter()
            .flat_map(QueryTerminalSnapshot::fragments)
    }

    pub fn is_success(&self) -> bool {
        self.snapshots.iter().all(QueryTerminalSnapshot::is_success)
    }
}

/// Rebuilds the coordinator-only lease aggregate after Protocol validates its
/// terminal snapshot. No RPC boundary carries this projection.
fn decode_protocol_terminal_snapshot_projection(
    value: &novarocks_protocol::novarocks::QueryTerminalSnapshot,
) -> Result<QueryTerminalSnapshot, QueryLifecycleError> {
    use crate::runtime::sink_commit::{
        SinkCommitReportSnapshot, SinkLoadStats, TabletCommitInfo, TabletFailInfo,
    };

    let fragments = value
        .fragments
        .iter()
        .map(|fragment| {
            let id = fragment.fragment_instance_id.as_ref().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("terminal fragment instance id is required")
            })?;
            let outcome = match fragment.outcome {
                1 => FragmentTerminalOutcome::Succeeded,
                2 if !fragment.error_code.trim().is_empty() => FragmentTerminalOutcome::Failed {
                    code: fragment.error_code.clone(),
                    detail: fragment.error_detail.clone(),
                    detail_truncated: fragment.error_detail_truncated,
                },
                3 => FragmentTerminalOutcome::Cancelled {
                    detail: fragment.error_detail.clone(),
                    detail_truncated: fragment.error_detail_truncated,
                },
                4 => FragmentTerminalOutcome::IncompleteDrain {
                    detail: fragment.error_detail.clone(),
                    detail_truncated: fragment.error_detail_truncated,
                },
                _ => {
                    return Err(QueryLifecycleError::invalid_manifest(
                        "invalid terminal fragment outcome",
                    ));
                }
            };
            let stats = fragment.load_stats.as_ref().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("terminal fragment load stats are required")
            })?;
            let sink = SinkCommitReportSnapshot {
                connector_staged_report_frames: fragment
                    .connector_staged_report_frames
                    .iter()
                    .map(crate::query_execution::write::decode_connector_staged_report_frame)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| QueryLifecycleError::invalid_manifest(error.message()))?,
                tablet_commit_infos: fragment
                    .tablet_commit_infos
                    .iter()
                    .map(|value| TabletCommitInfo {
                        tablet_id: value.tablet_id,
                        backend_id: value.backend_id,
                    })
                    .collect(),
                tablet_fail_infos: fragment
                    .tablet_fail_infos
                    .iter()
                    .map(|value| TabletFailInfo {
                        tablet_id: value.tablet_id,
                        backend_id: value.backend_id,
                    })
                    .collect(),
                load_stats: SinkLoadStats {
                    loaded_rows: stats.loaded_rows,
                    loaded_bytes: stats.loaded_bytes,
                    filtered_rows: stats.filtered_rows,
                },
            };
            let profile = decode_fragment_terminal_profile_telemetry(
                fragment.profile.as_ref().ok_or_else(|| {
                    QueryLifecycleError::invalid_manifest(
                        "terminal fragment profile telemetry is required",
                    )
                })?,
            )?;
            FragmentTerminalSnapshot::new_with_profile_telemetry(
                novarocks_types::UniqueId::new(id.hi, id.lo),
                fragment.backend_num,
                outcome,
                sink,
                profile,
            )
            .and_then(|snapshot| {
                snapshot.with_statistics_payload(fragment.statistics_payload.clone())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let profile_contribution = decode_query_terminal_profile_contribution_telemetry(
        value.profile_contribution.as_ref().ok_or_else(|| {
            QueryLifecycleError::invalid_manifest(
                "query terminal profile contribution telemetry is required",
            )
        })?,
    )?;
    QueryTerminalSnapshot::new_with_profile_telemetry(
        value
            .execution_id
            .as_ref()
            .ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("terminal execution id is required")
            })
            .and_then(|raw| {
                QueryExecutionId::try_from_proto(raw).map_err(QueryLifecycleError::from)
            })?,
        value
            .backend
            .clone()
            .ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("terminal backend identity is required")
            })
            .and_then(|raw| {
                ParticipantBackendIdentity::parse(raw).map_err(QueryLifecycleError::from)
            })?,
        ParticipantManifestDigest::try_from_slice(&value.init_digest)
            .map_err(QueryLifecycleError::from)?,
        fragments,
        profile_contribution,
    )
}
