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

//! Native commit-fragment wire authority for connector writes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_execution::exec::node::table_write_relation::{
    ConnectorCommitFragmentCarrierValidator, ConnectorCommitFragmentEncoder,
};
#[cfg(debug_assertions)]
use novarocks_execution::exec::node::table_write_relation::{
    TableWriteAggregateBoundary, TableWriteAggregateGuard,
};
use novarocks_proto_codec::connector_write::{
    ConnectorWriteFragmentEncoder, ValidatedCommitFragment,
};
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::connector_write as dto;
use novarocks_spi::connector::ConnectorWriteFragmentWireEncoder;
use novarocks_spi::connector::write_stack::{ConnectorCommitFragment, WriteTargetOrdinal};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_types::QueryExecutionId;
use prost::Message;

const WRITE_EVENT_TARGET: &str = "novarocks::connector_write";
pub struct RoleBoundCommitFragmentEncoder {
    encoder: Arc<dyn ConnectorWriteFragmentWireEncoder>,
    execution_id: QueryExecutionId,
    node_id: i32,
}

impl RoleBoundCommitFragmentEncoder {
    pub fn new(
        encoder: Arc<dyn ConnectorWriteFragmentWireEncoder>,
        execution_id: QueryExecutionId,
        node_id: i32,
    ) -> Self {
        Self {
            encoder,
            execution_id,
            node_id,
        }
    }
}

impl ConnectorCommitFragmentEncoder for RoleBoundCommitFragmentEncoder {
    fn encode(
        &self,
        target: WriteTargetOrdinal,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorError> {
        writer_failpoint(self.execution_id, self.node_id, target)?;
        self.encoder
            .canonical_commit_fragment_bytes(fragment)
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!(
                        "encode connector commit fragment for write target {}: {error}",
                        target.get()
                    ),
                )
            })
    }
}

/// Canonical commit-fragment ingress for the single root aggregation.
///
/// It proves a carrier is a canonical, in-bounds commit fragment of a provider
/// the closed carrier vocabulary names, and stops there. It never produces a
/// provider value, and it holds no provider binding that could produce one.
// Design: ADR-0133 (docs/adr/ADR-0133-dataflow-connector-writer-and-frontend-commit.md)
pub struct RootCommitFragmentCarrierValidator {
    execution_id: QueryExecutionId,
    node_id: i32,
    /// The prepared set this root has accepted so far. It only grows within one
    /// attempt, so the running total is also the peak.
    accepted_bytes: AtomicU64,
    accepted_entries: AtomicU64,
}

impl RootCommitFragmentCarrierValidator {
    pub const fn new(execution_id: QueryExecutionId, node_id: i32) -> Self {
        Self {
            execution_id,
            node_id,
            accepted_bytes: AtomicU64::new(0),
            accepted_entries: AtomicU64::new(0),
        }
    }

    pub fn accepted_counts(&self) -> (u64, u64) {
        (
            self.accepted_bytes.load(Ordering::Relaxed),
            self.accepted_entries.load(Ordering::Relaxed),
        )
    }
}

impl ConnectorCommitFragmentCarrierValidator for RootCommitFragmentCarrierValidator {
    fn validate(&self, target: WriteTargetOrdinal, encoded: &[u8]) -> Result<(), ConnectorError> {
        root_failpoint(self.execution_id, self.node_id, target)?;
        let raw = dto::ConnectorCommitFragment::decode(encoded).map_err(|error| {
            carrier_error(
                target,
                format!("carrier is not a connector commit fragment message: {error}"),
            )
        })?;
        let validated = ValidatedCommitFragment::parse(raw, FieldPath::root("commit_fragment"))
            .map_err(|error| {
                // A fragment that is merely too large is not corrupt, and the
                // difference is what an operator acts on: one says the write
                // outgrew a frozen budget, the other says something rewrote the
                // bytes in flight. The size gate runs inside `parse`, before the
                // ledger would have charged it, so the kind has to be recovered
                // here or the budget's documented failure never appears.
                if error.kind() == ProtocolErrorKind::OutOfRange {
                    return ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        format!(
                            "connector commit fragment carrier for write target {}: {error}",
                            target.get()
                        ),
                    );
                }
                carrier_error(target, format!("carrier failed validation: {error}"))
            })?;
        // Canonicality is a byte-exact property, not a length coincidence: the
        // producing backend encodes with the same canonical encoder, so a
        // carrier that re-encodes to different bytes was rewritten in flight.
        if validated.encoded_len() != encoded.len()
            || validated.as_proto().encode_to_vec() != encoded
        {
            return Err(carrier_error(
                target,
                "carrier is not the canonical encoding of the fragment it decodes to".to_string(),
            ));
        }
        let bytes = self
            .accepted_bytes
            .fetch_add(encoded.len() as u64, Ordering::Relaxed)
            .saturating_add(encoded.len() as u64);
        let entries = self
            .accepted_entries
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        crate::backend_metrics::publish_connector_write_root_prepared_set_peak(bytes, entries);
        tracing::debug!(
            target: WRITE_EVENT_TARGET,
            role = "be",
            event = "connector_write_root_fragment_accepted",
            query_id = %self.execution_id.query_id(),
            attempt_id = self.execution_id.attempt_id().get(),
            node_id = self.node_id,
            write_target_ordinal = target.get(),
            peak_set_bytes = bytes,
            peak_set_entries = entries,
            "root aggregation accepted a commit fragment carrier"
        );
        Ok(())
    }
}

/// Query-scoped rejection guard installed on both halves of the composite
/// write aggregate. The backend owns binding a runner token to an exact native
/// attempt; Execution sees only a typed boundary and can neither inspect the
/// token protocol nor fabricate replacement aggregate data.
#[cfg(debug_assertions)]
pub struct QueryScopedTableWriteAggregateGuard {
    execution_id: QueryExecutionId,
    node_id: i32,
}

#[cfg(debug_assertions)]
impl QueryScopedTableWriteAggregateGuard {
    pub const fn new(execution_id: QueryExecutionId, node_id: i32) -> Self {
        Self {
            execution_id,
            node_id,
        }
    }
}

#[cfg(debug_assertions)]
impl TableWriteAggregateGuard for QueryScopedTableWriteAggregateGuard {
    fn check(&self, boundary: TableWriteAggregateBoundary) -> Result<(), ConnectorError> {
        let kind = match boundary {
            TableWriteAggregateBoundary::PartialUpdate => {
                novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWritePartialUpdateFailure
            }
            TableWriteAggregateBoundary::PartialFinalize => {
                novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWritePartialFinalizeFailure
            }
            TableWriteAggregateBoundary::FinalMerge => {
                novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteFinalMergeFailure
            }
            TableWriteAggregateBoundary::FinalFinalize => {
                novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteFinalFinalizeFailure
            }
        };
        claim_write_fault(self.execution_id, kind).map_or(Ok(()), |token| {
            Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!(
                    "injected connector write {} on node_id={} (token={token})",
                    kind.file_stem(),
                    self.node_id
                ),
            ))
        })
    }
}

fn carrier_error(target: WriteTargetOrdinal, detail: String) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        format!(
            "connector commit fragment carrier for write target {}: {detail}",
            target.get()
        ),
    )
}
fn writer_failpoint(
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
) -> Result<(), ConnectorError> {
    claim_write_fault(
        execution_id,
        novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteWriterFailure,
    )
    .map_or(Ok(()), |token| {
        Err(ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!(
                "injected connector write writer failure on node_id={node_id} write target {} (token={token})",
                target.get()
            ),
        ))
    })
}

#[cfg(not(debug_assertions))]
fn writer_failpoint(
    _execution_id: QueryExecutionId,
    _node_id: i32,
    _target: WriteTargetOrdinal,
) -> Result<(), ConnectorError> {
    Ok(())
}

/// Test-only root-aggregation fault. Like the writer fault it only rejects.
#[cfg(debug_assertions)]
fn root_failpoint(
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
) -> Result<(), ConnectorError> {
    claim_write_fault(
        execution_id,
        novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootFailure,
    )
    .map_or(Ok(()), |token| {
        Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!(
                "injected connector write root failure on node_id={node_id} write target {} (token={token})",
                target.get()
            ),
        ))
    })
}

#[cfg(not(debug_assertions))]
fn root_failpoint(
    _execution_id: QueryExecutionId,
    _node_id: i32,
    _target: WriteTargetOrdinal,
) -> Result<(), ConnectorError> {
    Ok(())
}

/// Claim one armed fault for this exact attempt through the single existing
/// query-lifecycle fault channel.
///
/// The channel has two halves and only the backend half lives here: a harness
/// arms `be-<i>.<stem>.arm`, the frontend's scheduler binds it into a
/// `be-<i>.<stem>.trigger` carrying this attempt's execution id, and the claim
/// below consumes it. A kind the frontend's bind list does not name therefore
/// stays inert rather than firing on the wrong attempt, which is the correct
/// failure mode for a fault that is not fully wired.
#[cfg(debug_assertions)]
pub fn claim_write_fault(
    execution_id: QueryExecutionId,
    kind: novarocks_failpoint::QueryLifecycleFaultKind,
) -> Option<String> {
    let root = novarocks_failpoint::configured_root()?;
    match novarocks_failpoint::claim_matching_receiver_agnostic_fault(&root, kind, execution_id) {
        Ok(Some(scope)) => Some(scope.token),
        Ok(None) | Err(_) => None,
    }
}
