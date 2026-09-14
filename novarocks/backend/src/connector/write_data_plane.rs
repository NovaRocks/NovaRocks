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

//! The backend half of the connector write data plane.
//!
//! `novarocks-execution` cannot reach the generated wire crates, so it moves
//! commit fragments as opaque bytes and asks its owner to translate them. This
//! module is that owner. It installs, per decoded plan node, the two ports the
//! execution write nodes declare:
//!
//! | port | direction | backed by |
//! |---|---|---|
//! | [`ConnectorCommitFragmentEncoder`] | writer -> carrier bytes | the role binding's `fragment_encoder()` |
//! | [`ConnectorCommitFragmentCarrierValidator`] | carrier bytes -> accepted | `ValidatedCommitFragment::parse` |
//!
//! The asymmetry is deliberate. Encoding needs the exact provider generation
//! that produced the artifact, so it goes through the query-leased role
//! binding. Validation must *not* need one: the root aggregation counts and
//! bounds fragments from every writer, and turning one back into a provider
//! value is the frontend control binding's job. The validator therefore holds
//! no decoder at all — it structurally cannot interpret what it admits.
//!
//! This module also owns the write data plane's observation. A writer open,
//! the rows one driver accepted, and the fragments it produced are all facts
//! only the backend sees, and the decorator here is the single place they are
//! counted and logged.

use arrow::array::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ConnectorWriterPhysicalContext, WriteTargetOrdinal,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorError};
use novarocks_types::QueryExecutionId;
use std::sync::Arc;

/// Stable log target for every write data-plane event emitted here.
const WRITE_EVENT_TARGET: &str = "novarocks::connector_write";

#[cfg(debug_assertions)]
use novarocks_native_adapter::connector_write_data_plane::claim_write_fault;
/// Canonical commit-fragment egress, bound to one exact query-leased provider
/// generation.
///
/// It can only turn a fragment the local writer just produced into carrier
/// bytes. It holds no decoder, so it cannot read a carrier that came from
/// anywhere else.
pub(crate) use novarocks_native_adapter::connector_write_data_plane::{
    QueryScopedTableWriteAggregateGuard, RoleBoundCommitFragmentEncoder,
    RootCommitFragmentCarrierValidator,
};

/// Acceptance evidence that a driver-local connector writer really opened on
/// this backend for this exact write target.
///
/// A result-only assertion cannot show it: a write whose rows never reached a
/// backend, and one that opened no writer because its branch was never sealed,
/// both leave the same committed table behind. The structured
/// `connector_write_writer_open` event above says the same thing to an
/// operator; this says it on a stream a cross-process test can read.
fn emit_writer_marker(marker: &str, plan_node_id: i32, target: WriteTargetOrdinal) {
    if !novarocks_native_adapter::debug_environment::debug_emit_connector_writer_marker() {
        return;
    }
    println!(
        "{marker} plan_node={plan_node_id} write_target={}",
        target.get()
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// The write execution decorator that owns write data-plane observation.
///
/// It adds no authority: every call is forwarded to the query-leased binding's
/// own execution, which can open writers and nothing else.
pub(crate) struct ObservedConnectorWriteExecution {
    inner: Arc<dyn ConnectorWriteExecution>,
    execution_id: QueryExecutionId,
    node_id: i32,
}

impl ObservedConnectorWriteExecution {
    pub(crate) const fn new(
        inner: Arc<dyn ConnectorWriteExecution>,
        execution_id: QueryExecutionId,
        node_id: i32,
    ) -> Self {
        Self {
            inner,
            execution_id,
            node_id,
        }
    }
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for ObservedConnectorWriteExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        self.inner.catalog_handle()
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        let target = request.target;
        let physical = request.physical;
        let catalog_name = self
            .inner
            .catalog_handle()
            .catalog_name()
            .as_str()
            .to_string();
        match self.inner.open_writer(request).await {
            Ok(writer) => {
                novarocks_native_adapter::backend_metrics::record_connector_write_writer_open(
                    "opened",
                );
                tracing::info!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_open",
                    query_id = %self.execution_id.query_id(),
                    attempt_id = self.execution_id.attempt_id().get(),
                    node_id = self.node_id,
                    catalog = %catalog_name,
                    write_target_ordinal = target.get(),
                    writer_ordinal = physical.writer_ordinal(),
                    driver_id = physical.driver_id(),
                    "opened a driver-local connector writer"
                );
                emit_writer_marker("NOVAROCKS_CONNECTOR_WRITER_OPENED", self.node_id, target);
                Ok(Box::new(ObservedConnectorBatchWriter {
                    inner: writer,
                    execution_id: self.execution_id,
                    node_id: self.node_id,
                    target,
                    physical,
                    rows: 0,
                }))
            }
            Err(error) => {
                novarocks_native_adapter::backend_metrics::record_connector_write_writer_open(
                    "failed",
                );
                tracing::warn!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_open_failed",
                    query_id = %self.execution_id.query_id(),
                    attempt_id = self.execution_id.attempt_id().get(),
                    node_id = self.node_id,
                    catalog = %catalog_name,
                    write_target_ordinal = target.get(),
                    writer_ordinal = physical.writer_ordinal(),
                    driver_id = physical.driver_id(),
                    reason = %error,
                    "failed to open a driver-local connector writer"
                );
                Err(error)
            }
        }
    }
}

/// One driver's writer, wrapped so its rows and fragments are counted exactly
/// once, where they happen.
struct ObservedConnectorBatchWriter {
    inner: Box<dyn ConnectorBatchWriter>,
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
    physical: ConnectorWriterPhysicalContext,
    rows: u64,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for ObservedConnectorBatchWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        writer_append_holdpoint(self.execution_id, self.node_id, self.target).await;
        let rows = batch.num_rows() as u64;
        self.inner.append(batch).await?;
        self.rows = self.rows.saturating_add(rows);
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        match self.inner.finish().await {
            Ok(fragments) => {
                let produced = fragments.len() as u64;
                novarocks_native_adapter::backend_metrics::record_connector_write_writer_finished(
                    self.rows, produced,
                );
                tracing::info!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_finished",
                    query_id = %self.execution_id.query_id(),
                    attempt_id = self.execution_id.attempt_id().get(),
                    node_id = self.node_id,
                    write_target_ordinal = self.target.get(),
                    writer_ordinal = self.physical.writer_ordinal(),
                    driver_id = self.physical.driver_id(),
                    rows = self.rows,
                    commit_fragments = produced,
                    "finished a driver-local connector writer"
                );
                Ok(fragments)
            }
            Err(error) => {
                tracing::warn!(
                    target: WRITE_EVENT_TARGET,
                    role = "be",
                    event = "connector_write_writer_failed",
                    query_id = %self.execution_id.query_id(),
                    attempt_id = self.execution_id.attempt_id().get(),
                    node_id = self.node_id,
                    write_target_ordinal = self.target.get(),
                    writer_ordinal = self.physical.writer_ordinal(),
                    driver_id = self.physical.driver_id(),
                    rows = self.rows,
                    reason = %error,
                    "a driver-local connector writer failed to finish"
                );
                Err(error)
            }
        }
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        let result = self.inner.abort().await;
        let outcome = if result.is_ok() {
            "succeeded"
        } else {
            "failed"
        };
        novarocks_native_adapter::backend_metrics::record_connector_write_writer_abort(outcome);
        tracing::info!(
            target: WRITE_EVENT_TARGET,
            role = "be",
            event = "connector_write_writer_aborted",
            query_id = %self.execution_id.query_id(),
            attempt_id = self.execution_id.attempt_id().get(),
            node_id = self.node_id,
            write_target_ordinal = self.target.get(),
            writer_ordinal = self.physical.writer_ordinal(),
            driver_id = self.physical.driver_id(),
            rows = self.rows,
            outcome,
            "completed a driver-local connector writer abort"
        );
        result
    }
}

/// Hold one exact attempt inside a live writer append until cancellation drops
/// the append future. The query-lifecycle fault token is consumed only after
/// the writer exists and receives input, so its metric is proof that the
/// deadline did not race ahead of writer execution.
#[cfg(debug_assertions)]
async fn writer_append_holdpoint(
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
) {
    let Some(token) = claim_write_fault(
        execution_id,
        novarocks_failpoint::QueryLifecycleFaultKind::ConnectorWriteAppendHold,
    ) else {
        return;
    };
    novarocks_native_adapter::backend_metrics::record_connector_write_debug_fault("append_hold");
    tracing::info!(
        target: WRITE_EVENT_TARGET,
        role = "be",
        event = "connector_write_append_hold",
        query_id = %execution_id.query_id(),
        attempt_id = execution_id.attempt_id().get(),
        node_id,
        write_target_ordinal = target.get(),
        token,
        "holding a driver-local writer append until query cancellation"
    );
    std::future::pending::<()>().await;
}

#[cfg(not(debug_assertions))]
async fn writer_append_holdpoint(
    _execution_id: QueryExecutionId,
    _node_id: i32,
    _target: WriteTargetOrdinal,
) {
}

/// Test-only writer fault, claimed once per armed trigger for this exact
/// attempt. It can only fail an in-flight writer; there is no branch here that
/// substitutes a value, so it can never become a production fallback.
#[cfg(debug_assertions)]
#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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

    fn catalog_handle() -> CatalogHandle {
        CatalogHandle::new(
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
        catalog_handle: CatalogHandle,
    }

    impl StubWriteRuntime {
        fn new() -> Self {
            let handle = catalog_handle();
            Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
                    instance_id: handle.catalog_name().clone(),
                },
                catalog_handle: handle,
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

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    fn adapter() -> WriteRuntimeAdapter<StubWriteRuntime> {
        WriteRuntimeAdapter::new(Arc::new(StubWriteRuntime::new()))
    }

    /// A fragment encoder that simply hands back the carrier the provider
    /// value already is. It stands in for a real provider codec.
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

    fn stub_fragment_encoder() -> Arc<dyn ConnectorWriteFragmentWireEncoder> {
        Arc::new(StubFragmentEncoder { adapter: adapter() })
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

    fn target(value: u32) -> WriteTargetOrdinal {
        WriteTargetOrdinal::try_new(value).expect("bounded ordinal")
    }

    #[test]
    fn the_encoder_produces_the_canonical_carrier_its_validator_accepts() {
        let encoder =
            RoleBoundCommitFragmentEncoder::new(stub_fragment_encoder(), execution_id(), 11);
        let fragment = adapter().wrap_commit_fragment("s3://b/t/a.parquet".to_owned());
        let bytes = encoder
            .encode(target(0), &fragment)
            .expect("encode commit fragment");
        assert_eq!(
            bytes,
            data_file_fragment("s3://b/t/a.parquet").encode_to_vec()
        );

        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        validator
            .validate(target(0), &bytes)
            .expect("the canonical carrier is accepted");
    }

    #[test]
    fn the_validator_rejects_a_non_canonical_carrier() {
        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        let canonical = data_file_fragment("s3://b/t/a.parquet").encode_to_vec();
        // Appending a zero-length, unknown-field tag decodes to the same
        // message but is not the canonical encoding of it.
        let mut rewritten = canonical.clone();
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
    fn the_validator_rejects_a_carrier_without_a_provider_variant() {
        let validator = RootCommitFragmentCarrierValidator::new(execution_id(), 12);
        let empty = dto::ConnectorCommitFragment {
            provider_payload: None,
        }
        .encode_to_vec();
        let error = validator
            .validate(target(0), &empty)
            .expect_err("a variantless carrier is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    /// An oversized carrier exhausts a frozen budget; it is not corrupt data.
    ///
    /// The distinction is what an operator acts on: one says the write outgrew
    /// a budget, the other says something rewrote the bytes in flight. The size
    /// gate lives inside the carrier parse, which runs before the ledger would
    /// have charged it, so this is the only place the budget's documented
    /// failure can surface.
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

    /// A carrier that is within the budget but structurally wrong stays
    /// CorruptData, so the two failures cannot collapse into one.
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

    struct CountingWriteExecution {
        catalog_handle: CatalogHandle,
        opened: Arc<Mutex<Vec<(u32, u32)>>>,
    }

    struct CountingWriter {
        rows: Arc<Mutex<u64>>,
    }

    #[async_trait::async_trait]
    impl ConnectorBatchWriter for CountingWriter {
        async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
            *self.rows.lock().expect("rows") += batch.num_rows() as u64;
            Ok(())
        }

        async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
            Ok(vec![
                adapter().wrap_commit_fragment("s3://b/t/a.parquet".to_owned()),
            ])
        }

        async fn abort(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ConnectorWriteExecution for CountingWriteExecution {
        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }

        async fn open_writer(
            &self,
            request: ConnectorOpenWriterRequest,
        ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
            self.opened.lock().expect("opened").push((
                request.physical.driver_id(),
                request.physical.writer_ordinal(),
            ));
            Ok(Box::new(CountingWriter {
                rows: Arc::new(Mutex::new(0)),
            }))
        }
    }

    #[tokio::test]
    async fn the_observed_execution_forwards_its_binding_and_counts_each_open() {
        let opened = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(CountingWriteExecution {
            catalog_handle: catalog_handle(),
            opened: Arc::clone(&opened),
        });
        let observed = ObservedConnectorWriteExecution::new(inner, execution_id(), 7);
        assert_eq!(observed.catalog_handle(), &catalog_handle());
        for driver_id in 0..3 {
            let mut writer = observed
                .open_writer(ConnectorOpenWriterRequest {
                    handle: adapter().wrap_writer_handle("recipe".to_string()),
                    target: target(0),
                    expected_schema: Arc::new(arrow::datatypes::Schema::empty()),
                    physical: ConnectorWriterPhysicalContext::new(
                        [1; 16], 2, [3; 16], driver_id, 0,
                    ),
                    context: request_context(),
                })
                .await
                .expect("open writer");
            assert_eq!(writer.finish().await.expect("finish").len(), 1);
        }
        assert_eq!(
            *opened.lock().expect("opened"),
            vec![(0, 0), (1, 0), (2, 0)],
            "each driver opens its own writer with its own driver id"
        );
    }

    fn request_context() -> novarocks_spi::connector::ConnectorRequestContext {
        struct NeverCancelled;
        impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
            fn is_cancelled(&self) -> bool {
                false
            }
        }
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            Arc::new(NeverCancelled),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context")
    }
}
