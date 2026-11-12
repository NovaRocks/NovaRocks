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
#[cfg(novarocks_dev_controls)]
use novarocks_execution::exec::node::table_write_relation::{
    TableWriteAggregateBoundary, TableWriteAggregateFaultHook,
};
use novarocks_proto_codec::connector_write::{
    ConnectorWriteFragmentEncoder, ValidatedCommitFragment,
};
use novarocks_proto_codec::{FieldPath, ProtocolErrorKind};
use novarocks_proto_models::connector_write as dto;
use novarocks_spi::connector::ConnectorWriteFragmentWireEncoder;
use novarocks_spi::connector::write_stack::{ConnectorCommitFragment, WriteTargetOrdinal};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_types::{BackendProcessId, QueryExecutionId};
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
#[derive(Debug)]
pub struct NativeConnectorWriteObservationPort {
    #[cfg(novarocks_dev_controls)]
    process_id: BackendProcessId,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
}

impl NativeConnectorWriteObservationPort {
    pub fn new(process_id: BackendProcessId) -> Self {
        #[cfg(not(novarocks_dev_controls))]
        let _ = process_id;
        Self {
            #[cfg(novarocks_dev_controls)]
            process_id,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        self.test_controls = Some(controls);
        self
    }
    #[cfg(novarocks_dev_controls)]
    fn claim_append_hold(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<Option<crate::test_controls::NativeFaultClaim>, ConnectorError> {
        let Some(controls) = self.test_controls.as_ref() else {
            return Ok(None);
        };
        controls
            .claim(
                novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteAppendHold,
                execution_id,
                self.process_id,
                false,
            )
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))
    }
}

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

    #[cfg(novarocks_dev_controls)]
    fn hold_append(
        &self,
        writer: ConnectorWriterObservation,
    ) -> Result<
        Option<novarocks_worker::connector_write_runtime::ConnectorAppendHold>,
        ConnectorError,
    > {
        let Some(scope) = self.claim_append_hold(writer.execution_id())? else {
            return Ok(None);
        };
        scope
            .record_hit()
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
        crate::backend_metrics::record_connector_write_debug_fault("append_hold");
        tracing::info!(target:WRITE_EVENT_TARGET,role="be",event="connector_write_append_hold",query_id=%writer.execution_id().query_id(),attempt_id=writer.execution_id().attempt_id().get(),node_id=writer.node_id(),write_target_ordinal=writer.target().get(),token=%scope.token,backend_process_id=%self.process_id,"holding a driver-local writer append until selected action end, query control or role session cancellation");
        Ok(Some(Box::pin(async move {
            scope
                .wait_for_action_end()
                .await
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))
        })))
    }
}
pub struct RoleBoundCommitFragmentEncoder {
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
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
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            encoder,
            execution_id,
            node_id,
        }
    }
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        self.test_controls = Some(controls);
        self
    }
}

impl ConnectorCommitFragmentEncoder for RoleBoundCommitFragmentEncoder {
    fn encode(
        &self,
        target: WriteTargetOrdinal,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorError> {
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.test_controls.as_ref() {
            writer_failpoint(controls, self.execution_id, self.node_id, target)?;
        }
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
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
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
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            execution_id,
            node_id,
            accepted_bytes: AtomicU64::new(0),
            accepted_entries: AtomicU64::new(0),
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub fn with_test_controls(
        mut self,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        self.test_controls = Some(controls);
        self
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
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.test_controls.as_ref() {
            root_failpoint(controls, self.execution_id, self.node_id, target)?;
        }
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
                if error.kind() == &ProtocolErrorKind::OutOfRange {
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
#[cfg(novarocks_dev_controls)]
pub struct QueryScopedTableWriteAggregateFaultHook {
    controls: Arc<crate::test_controls::NativeTestControls>,
    execution_id: QueryExecutionId,
    node_id: i32,
}

#[cfg(novarocks_dev_controls)]
impl QueryScopedTableWriteAggregateFaultHook {
    pub fn new(
        execution_id: QueryExecutionId,
        node_id: i32,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        Self {
            execution_id,
            node_id,
            controls,
        }
    }
}

#[cfg(novarocks_dev_controls)]
impl TableWriteAggregateFaultHook for QueryScopedTableWriteAggregateFaultHook {
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
        claim_write_fault(&self.controls, self.execution_id, kind)?.map_or(Ok(()), |token| {
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
#[cfg(novarocks_dev_controls)]
fn writer_failpoint(
    controls: &crate::test_controls::NativeTestControls,
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
) -> Result<(), ConnectorError> {
    claim_write_fault(
        controls,
        execution_id,
        novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteWriterFailure,
    )?
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

/// Test-only root-aggregation fault. Like the writer fault it only rejects.
#[cfg(novarocks_dev_controls)]
fn root_failpoint(
    controls: &crate::test_controls::NativeTestControls,
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
) -> Result<(), ConnectorError> {
    claim_write_fault(
        controls,
        execution_id,
        novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteRootFailure,
    )?
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

/// Claim one armed fault for this exact attempt through the single existing
/// query-lifecycle fault channel.
///
/// The channel has two halves and only the backend half lives here: a harness
/// arms `be-<i>.<stem>.arm`, the frontend's scheduler binds it into a
/// `be-<i>.<stem>.trigger` carrying this attempt's execution id, and the claim
/// below consumes it. A kind the frontend's bind list does not name therefore
/// stays inert rather than firing on the wrong attempt, which is the correct
/// failure mode for a fault that is not fully wired.
#[cfg(novarocks_dev_controls)]
pub fn claim_write_fault(
    controls: &crate::test_controls::NativeTestControls,
    execution_id: QueryExecutionId,
    kind: novarocks_failpoint::QueryLifecycleFaultKind,
) -> Result<Option<String>, ConnectorError> {
    let Some(scope) = controls
        .claim_receiver(kind, execution_id)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?
    else {
        return Ok(None);
    };
    scope
        .record_hit()
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))?;
    Ok(Some(scope.token.clone()))
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

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn append_holds_belong_to_the_exact_admitted_backend_process_and_attempt() {
        let fixture = crate::test_controls::tests::controlled("connector-write-append-hold");
        let kind = novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteAppendHold;
        novarocks_failpoint::target_binding::StatementExecutionBinding::new(
            "native-tests".into(),
            fixture.token.clone(),
            0,
            fixture.process,
            fixture.execution,
        )
        .unwrap()
        .write_private(&crate::test_controls::fault_binding_path(
            fixture.directory.path(),
            kind,
            0,
            &fixture.token,
        ))
        .unwrap();
        let replaced = NativeConnectorWriteObservationPort::new(BackendProcessId::new_v7())
            .with_test_controls(Arc::clone(&fixture.controls));
        assert!(
            replaced
                .claim_append_hold(fixture.execution)
                .unwrap()
                .is_none()
        );
        let observer = NativeConnectorWriteObservationPort::new(fixture.process)
            .with_test_controls(Arc::clone(&fixture.controls));
        let next = QueryExecutionId::new(fixture.execution.query_id(), AttemptId::new(2).unwrap())
            .unwrap();
        assert!(observer.claim_append_hold(next).unwrap().is_none());
        let scope = observer
            .claim_append_hold(fixture.execution)
            .unwrap()
            .unwrap();
        assert_eq!(scope.process_id, fixture.process);
        assert_eq!(scope.token, fixture.token);
        assert!(
            observer
                .claim_append_hold(fixture.execution)
                .unwrap()
                .is_none()
        );
    }

    #[cfg(novarocks_dev_controls)]
    mod append_hold_exit_tests {
        use super::*;
        use arrow::{
            array::{Array, Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        };
        use novarocks_spi::connector::write_stack::{
            ConnectorBatchWriter, ConnectorOpenWriterRequest, ConnectorWriteExecution,
            ConnectorWriterPhysicalContext,
        };
        use novarocks_spi::connector::{
            CatalogHandle, ConnectorRequestContext, ConnectorStopOwner,
        };
        use novarocks_worker::connector_write_runtime::ObservedConnectorWriteExecution;
        use std::{
            future::Future,
            sync::atomic::{AtomicBool, AtomicUsize},
            task::Poll,
            time::{Duration, Instant},
        };

        #[derive(Default)]
        struct Calls {
            appended: AtomicUsize,
            aborted: AtomicUsize,
            dropped: AtomicBool,
        }
        struct InnerExecution(Arc<Calls>, CatalogHandle);
        struct InnerWriter(Arc<Calls>);
        impl Drop for InnerWriter {
            fn drop(&mut self) {
                self.0.dropped.store(true, Ordering::Release);
            }
        }
        #[async_trait::async_trait]
        impl ConnectorBatchWriter for InnerWriter {
            async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(values.value(0), 7);
                assert!(values.is_null(1));
                self.0.appended.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                panic!("the held-append cleanup never finishes a writer")
            }
            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.0.aborted.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for InnerExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.1
            }
            async fn open_writer(
                &self,
                _: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(InnerWriter(self.0.clone())))
            }
        }
        #[derive(Clone, Copy)]
        enum Release {
            TaskStop,
            SessionEnd,
            SelectedEnd,
            SelectedEndAndTaskStop,
            ForeignSelectedEnd,
            OriginalDeadline,
        }

        async fn real_hold_case(release: Release) {
            let kind = novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteAppendHold;
            let fixture = crate::test_controls::tests::controlled_with_follow_up(
                kind.as_str(),
                matches!(release, Release::SelectedEnd),
            );
            let binding = crate::test_controls::tests::bind_fixture(&fixture, kind);
            let observer = Arc::new(
                NativeConnectorWriteObservationPort::new(fixture.process)
                    .with_test_controls(fixture.controls.clone()),
            );
            let calls = Arc::new(Calls::default());
            let execution = ObservedConnectorWriteExecution::new(
                Arc::new(InnerExecution(calls.clone(), catalog_handle())),
                fixture.execution,
                7,
                observer,
                None,
            );
            let stop = ConnectorStopOwner::new();
            let deadline = Instant::now()
                + match release {
                    Release::OriginalDeadline => Duration::from_millis(100),
                    _ => Duration::from_secs(60),
                };
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
            let mut writer = execution
                .open_writer(ConnectorOpenWriterRequest {
                    resources: None,
                    handle: adapter().wrap_writer_handle("held".to_owned()),
                    target: WriteTargetOrdinal::try_new(0).unwrap(),
                    expected_schema: schema.clone(),
                    physical: ConnectorWriterPhysicalContext::new([1; 16], 2, [3; 16], 4, 5),
                    context: ConnectorRequestContext::try_new(
                        deadline,
                        stop.view(),
                        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                    )
                    .unwrap(),
                })
                .await
                .unwrap();
            let array = Arc::new(Int64Array::from(vec![Some(7), None]));
            let original_array = Arc::downgrade(&array);
            let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
            let mut append = Box::pin(writer.append(batch));
            // Poll the actual Native hold through the real Worker wrapper.
            if !matches!(release, Release::OriginalDeadline) {
                let pending =
                    std::future::poll_fn(|cx| Poll::Ready(append.as_mut().poll(cx).is_pending()))
                        .await;
                assert!(pending);
            }
            assert!(original_array.upgrade().is_some());
            assert_eq!(calls.appended.load(Ordering::Acquire), 0);
            assert_eq!(calls.aborted.load(Ordering::Acquire), 0);
            assert!(!calls.dropped.load(Ordering::Acquire));
            match release {
                Release::TaskStop => stop.request_stop(),
                Release::SessionEnd => fixture.role.close(),
                Release::SelectedEnd => {
                    crate::test_controls::tests::end_fixture(&fixture, kind, &binding);
                    assert!(fixture.role.scope().is_active());
                }
                Release::SelectedEndAndTaskStop => {
                    crate::test_controls::tests::end_fixture(&fixture, kind, &binding);
                    stop.request_stop();
                }
                Release::ForeignSelectedEnd => {
                    // No valid retirement accompanies this foreign End: its
                    // mismatched execution must fail, never release the hold.
                    let foreign =
                        novarocks_failpoint::target_binding::StatementExecutionBinding::new(
                            binding.target_session_id.clone(),
                            fixture.token.clone(),
                            0,
                            fixture.process,
                            QueryExecutionId::new(
                                novarocks_types::QueryId::new(71, 74),
                                novarocks_types::AttemptId::new(1).unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap();
                    crate::test_controls::tests::end_fixture(&fixture, kind, &foreign);
                }
                Release::OriginalDeadline => {}
            }
            let outcome = tokio::time::timeout(Duration::from_secs(5), append.as_mut())
                .await
                .unwrap();
            drop(append);
            assert!(
                original_array.upgrade().is_none(),
                "the original held batch actually retires"
            );
            match release {
                Release::TaskStop | Release::SelectedEndAndTaskStop => {
                    assert_eq!(outcome.unwrap_err().kind(), ConnectorErrorKind::Cancelled);
                    assert!(fixture.role.scope().is_active());
                    assert_eq!(calls.appended.load(Ordering::Acquire), 0);
                }
                Release::SessionEnd => {
                    outcome.unwrap();
                    assert!(!stop.view().is_stopped());
                    assert_eq!(calls.appended.load(Ordering::Acquire), 1);
                }
                Release::SelectedEnd => {
                    outcome.unwrap();
                    assert!(!stop.view().is_stopped());
                    assert!(fixture.role.scope().is_active());
                    assert_eq!(calls.appended.load(Ordering::Acquire), 1);
                    // The same role still owns its predeclared later statement
                    // and observations; do not end the role to release DOP 1.
                    let next_execution = QueryExecutionId::new(
                        novarocks_types::QueryId::new(71, 74),
                        novarocks_types::AttemptId::new(1).unwrap(),
                    )
                    .unwrap();
                    let next_token = format!("{}-next", fixture.token);
                    crate::test_controls::tests::bind_statement(
                        &fixture,
                        kind,
                        next_execution,
                        &next_token,
                    );
                    let next = fixture
                        .controls
                        .claim(kind, next_execution, fixture.process, false)
                        .unwrap()
                        .unwrap();
                    assert_eq!(next.token, next_token);
                    assert!(!next.action_ended().unwrap());
                    next.record_hit().unwrap();
                    let observations = fixture.role.scope().observations();
                    assert_eq!(observations.iter().filter(|e| e.event == "hit").count(), 2);
                    assert!(
                        observations
                            .iter()
                            .any(|e| { e.grant_id == "second-action" && e.event == "hit" })
                    );
                }
                Release::ForeignSelectedEnd => {
                    assert_eq!(outcome.unwrap_err().kind(), ConnectorErrorKind::Internal);
                    assert!(!stop.view().is_stopped());
                    assert!(fixture.role.scope().is_active());
                    assert_eq!(calls.appended.load(Ordering::Acquire), 0);
                }
                Release::OriginalDeadline => {
                    assert_eq!(
                        outcome.unwrap_err().kind(),
                        ConnectorErrorKind::DeadlineExceeded
                    );
                    assert!(!stop.view().is_stopped());
                    assert!(fixture.role.scope().is_active());
                    assert_eq!(calls.appended.load(Ordering::Acquire), 0);
                }
            }
            assert_eq!(
                calls.aborted.load(Ordering::Acquire),
                0,
                "the gate itself has no abort authority"
            );
            writer.abort().await.unwrap();
            assert_eq!(calls.aborted.load(Ordering::Acquire), 1);
            assert!(!calls.dropped.load(Ordering::Acquire));
            drop(writer);
            assert!(calls.dropped.load(Ordering::Acquire));
        }

        #[tokio::test]
        async fn held_append_retires_on_original_task_stop_while_the_role_permission_is_active() {
            real_hold_case(Release::TaskStop).await;
        }
        #[tokio::test]
        async fn role_session_end_only_releases_the_gate_without_stopping_the_original_operation() {
            real_hold_case(Release::SessionEnd).await;
        }
        #[tokio::test]
        async fn selected_action_end_releases_original_append_and_preserves_the_next_statement() {
            real_hold_case(Release::SelectedEnd).await;
        }
        #[tokio::test]
        async fn selected_action_end_cannot_override_the_original_task_stop() {
            real_hold_case(Release::SelectedEndAndTaskStop).await;
        }
        #[tokio::test]
        async fn foreign_selected_action_end_fails_without_appending_or_closing_the_role() {
            real_hold_case(Release::ForeignSelectedEnd).await;
        }
        #[tokio::test]
        async fn held_append_observes_the_original_operation_deadline_without_a_new_stop_owner() {
            real_hold_case(Release::OriginalDeadline).await;
        }
    }

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
        type CommitSession = ();
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
