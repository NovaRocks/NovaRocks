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
use novarocks_worker::connector_write_runtime::{
    ConnectorWriteObservation, ConnectorWriteObservationPort, ConnectorWriterAbortOutcome,
    ConnectorWriterObservation,
};
use prost::Message;

const WRITE_EVENT_TARGET: &str = "novarocks::connector_write";

/// Native rendering of Worker-observed connector writer lifecycle facts.
///
/// The Worker decides when it can open, append to, finish, or abort a writer.
/// This adapter turns those settled facts into role-local metrics and logs, and
/// may consume the existing debug hold fault after an append is already live.
/// It cannot open a writer or change its result.
#[derive(Debug, Default)]
pub struct NativeConnectorWriteObservationPort;

impl ConnectorWriteObservationPort for NativeConnectorWriteObservationPort {
    fn observe(&self, observation: ConnectorWriteObservation) {
        match observation {
            ConnectorWriteObservation::WriterOpened { writer, catalog } => {
                crate::backend_metrics::record_connector_write_writer_open("opened");
                tracing::info!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_open",
                    query_id = %writer.execution_id().query_id(),
                    attempt_id = writer.execution_id().attempt_id().get(),
                    node_id = writer.node_id(),
                    catalog = %catalog.catalog_name().as_str(),
                    write_target_ordinal = writer.target().get(),
                    writer_ordinal = writer.physical().writer_ordinal(),
                    driver_id = writer.physical().driver_id(),
                    "opened a driver-local connector writer"
                );
            }
            ConnectorWriteObservation::WriterOpenFailed {
                writer,
                catalog,
                reason,
            } => {
                crate::backend_metrics::record_connector_write_writer_open("failed");
                tracing::warn!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_open_failed",
                    query_id = %writer.execution_id().query_id(),
                    attempt_id = writer.execution_id().attempt_id().get(),
                    node_id = writer.node_id(),
                    catalog = %catalog.catalog_name().as_str(),
                    write_target_ordinal = writer.target().get(),
                    writer_ordinal = writer.physical().writer_ordinal(),
                    driver_id = writer.physical().driver_id(),
                    reason = %reason,
                    "failed to open a driver-local connector writer"
                );
            }
            ConnectorWriteObservation::WriterFinished {
                writer,
                rows,
                commit_fragments,
            } => {
                crate::backend_metrics::record_connector_write_writer_finished(
                    rows,
                    commit_fragments,
                );
                tracing::info!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_finished",
                    query_id = %writer.execution_id().query_id(),
                    attempt_id = writer.execution_id().attempt_id().get(),
                    node_id = writer.node_id(),
                    write_target_ordinal = writer.target().get(),
                    writer_ordinal = writer.physical().writer_ordinal(),
                    driver_id = writer.physical().driver_id(),
                    rows,
                    commit_fragments,
                    "finished a driver-local connector writer"
                );
            }
            ConnectorWriteObservation::WriterFinishFailed {
                writer,
                rows,
                reason,
            } => {
                tracing::warn!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_failed",
                    query_id = %writer.execution_id().query_id(),
                    attempt_id = writer.execution_id().attempt_id().get(),
                    node_id = writer.node_id(),
                    write_target_ordinal = writer.target().get(),
                    writer_ordinal = writer.physical().writer_ordinal(),
                    driver_id = writer.physical().driver_id(),
                    rows,
                    reason = %reason,
                    "a driver-local connector writer failed to finish"
                );
            }
            ConnectorWriteObservation::WriterAborted {
                writer,
                rows,
                outcome,
            } => {
                let outcome = match outcome {
                    ConnectorWriterAbortOutcome::Succeeded => "succeeded",
                    ConnectorWriterAbortOutcome::Failed => "failed",
                };
                crate::backend_metrics::record_connector_write_writer_abort(outcome);
                tracing::info!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_aborted",
                    query_id = %writer.execution_id().query_id(),
                    attempt_id = writer.execution_id().attempt_id().get(),
                    node_id = writer.node_id(),
                    write_target_ordinal = writer.target().get(),
                    writer_ordinal = writer.physical().writer_ordinal(),
                    driver_id = writer.physical().driver_id(),
                    rows,
                    outcome,
                    "completed a driver-local connector writer abort"
                );
            }
        }
    }

    fn hold_append(&self, writer: ConnectorWriterObservation) -> bool {
        #[cfg(debug_assertions)]
        {
            let Some(token) = claim_write_fault(
                writer.execution_id(),
                novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteAppendHold,
            ) else {
                return false;
            };
            crate::backend_metrics::record_connector_write_debug_fault("append_hold");
            tracing::info!(
                target: WRITE_EVENT_TARGET,
                role = "be",
                event = "connector_write_append_hold",
                query_id = %writer.execution_id().query_id(),
                attempt_id = writer.execution_id().attempt_id().get(),
                node_id = writer.node_id(),
                write_target_ordinal = writer.target().get(),
                token,
                "holding a driver-local writer append until query cancellation"
            );
            true
        }
        #[cfg(not(debug_assertions))]
        {
            let _ = writer;
            false
        }
    }
}
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
#[cfg(debug_assertions)]
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

#[cfg(test)]
mod tests {
    use novarocks_execution::exec::node::table_write_relation::{
        ConnectorCommitFragmentCarrierValidator, ConnectorCommitFragmentEncoder,
    };
    use novarocks_proto_models::connector_write as dto;
    use novarocks_spi::connector::ConnectorWriteFragmentWireEncoder;
    use novarocks_spi::connector::write_stack::{
        MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, ProviderWriteRuntime, WriteRuntimeAdapter,
    };
    use novarocks_spi::connector::{
        CatalogVersion, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId,
    };
    use novarocks_types::{AttemptId, QueryId};
    use prost::Message;

    use super::*;

    fn catalog_handle() -> novarocks_spi::connector::CatalogHandle {
        novarocks_spi::connector::CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("write_test").expect("canonical instance id"),
            CatalogVersion::from_bytes([5; 32]),
        )
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(31, 41), AttemptId::new(2).expect("attempt"))
            .expect("execution id")
    }

    #[derive(Debug)]
    struct StubWriteRuntime {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
    }

    impl StubWriteRuntime {
        fn new() -> Self {
            let catalog_handle = catalog_handle();
            Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
                    instance_id: catalog_handle.catalog_name().clone(),
                },
                catalog_handle,
            }
        }
    }

    impl ProviderWriteRuntime for StubWriteRuntime {
        type CommitHandle = ();
        type WriterHandle = String;
        type CommitFragment = String;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
            &self.catalog_handle
        }
    }

    fn adapter() -> WriteRuntimeAdapter<StubWriteRuntime> {
        WriteRuntimeAdapter::new(Arc::new(StubWriteRuntime::new()))
    }

    struct StubFragmentEncoder {
        adapter: WriteRuntimeAdapter<StubWriteRuntime>,
    }

    impl ConnectorWriteFragmentWireEncoder for StubFragmentEncoder {
        fn owner(&self) -> &str {
            "write_test"
        }

        fn encode_commit_fragment_payload(
            &self,
            fragment: &ConnectorCommitFragment,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            let value = self
                .adapter
                .commit_fragment(fragment)
                .cloned()
                .map_err(|error| {
                    novarocks_spi::connector::ConnectorCodecError::new(
                        novarocks_spi::connector::ConnectorFieldPath::root("commit_fragment"),
                        novarocks_spi::connector::ConnectorCodecErrorKind::InvalidValue,
                        error.to_string(),
                    )
                })?;
            Ok(novarocks_spi::connector::ConnectorEncodedPayload::new(
                novarocks_spi::connector::ConnectorEnvelopeHeader::new(
                    ConnectorProviderId::parse("iceberg").expect("provider id"),
                    catalog_handle(),
                    novarocks_spi::connector::ConnectorCodecCategory::CommitFragment,
                    novarocks_spi::connector::ConnectorCodecRevision::try_new(1)
                        .expect("codec revision"),
                ),
                bytes::Bytes::from(value),
            ))
        }
    }

    fn target(value: u32) -> WriteTargetOrdinal {
        WriteTargetOrdinal::try_new(value).expect("bounded ordinal")
    }

    fn data_file_fragment(path: &str) -> dto::ConnectorCommitFragment {
        use novarocks_proto_codec::connector_common::encode_connector_payload_message;

        let payload = novarocks_spi::connector::ConnectorEncodedPayload::new(
            novarocks_spi::connector::ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("iceberg").expect("provider id"),
                catalog_handle(),
                novarocks_spi::connector::ConnectorCodecCategory::CommitFragment,
                novarocks_spi::connector::ConnectorCodecRevision::try_new(1)
                    .expect("codec revision"),
            ),
            bytes::Bytes::copy_from_slice(path.as_bytes()),
        );
        dto::ConnectorCommitFragment {
            provider_payload: Some(encode_connector_payload_message(&payload)),
        }
    }

    #[test]
    fn the_encoder_produces_the_canonical_carrier_its_validator_accepts() {
        let encoder = RoleBoundCommitFragmentEncoder::new(
            Arc::new(StubFragmentEncoder { adapter: adapter() }),
            execution_id(),
            11,
        );
        let fragment = adapter().wrap_commit_fragment("s3://b/t/a.parquet".to_owned());
        let bytes = encoder
            .encode(target(0), &fragment)
            .expect("encode commit fragment");

        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        validator
            .validate(target(0), &bytes)
            .expect("the canonical carrier is accepted");
    }

    #[test]
    fn the_validator_rejects_a_non_canonical_carrier() {
        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        let canonical = data_file_fragment("s3://b/t/a.parquet").encode_to_vec();
        // Appending an unknown field decodes to the same message but is not
        // the exact canonical encoding the producer was required to send.
        let mut rewritten = canonical;
        rewritten.extend_from_slice(&[0xf8, 0x7f, 0x00]);
        let error = validator
            .validate(target(0), &rewritten)
            .expect_err("a rewritten carrier is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(
            error.to_string().contains("canonical"),
            "unexpected rejection: {error}"
        );
    }

    #[test]
    fn the_validator_reports_a_carrier_over_the_frozen_fragment_budget_as_exhausted() {
        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        let oversized = data_file_fragment(&"s".repeat(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1))
            .encode_to_vec();
        let error = validator
            .validate(target(0), &oversized)
            .expect_err("an oversized carrier is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn the_validator_still_reports_a_malformed_carrier_as_corrupt() {
        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        let error = validator
            .validate(target(0), b"not a protobuf message at all")
            .expect_err("a malformed carrier is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn the_validator_reports_the_running_prepared_set_as_its_peak() {
        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        let first = data_file_fragment("s3://b/t/a.parquet").encode_to_vec();
        let second = data_file_fragment("s3://b/t/b.parquet").encode_to_vec();
        validator.validate(target(0), &first).expect("first");
        validator.validate(target(0), &second).expect("second");
        assert_eq!(
            validator.accepted_counts(),
            ((first.len() + second.len()) as u64, 2)
        );
    }
}
