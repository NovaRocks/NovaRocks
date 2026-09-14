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

//! Worker-owned connector writer lifecycle and its role-local observation port.
//!
//! A Worker opens and drives the writer that one of its drivers owns.  Native
//! metrics, logging, and debug fault selection remain role-local effects, so
//! this module emits already-observed lifecycle facts through a one-way port
//! instead of depending on a Native adapter.  The port cannot open a writer,
//! produce a fragment, or alter a writer result.  Its only control result is a
//! debug-only request to hold an already-live append; the Worker retains the
//! pending future and cancellation ownership.

use std::sync::Arc;

use arrow::array::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ConnectorWriterPhysicalContext, WriteTargetOrdinal,
};
use novarocks_spi::connector::{CatalogHandle, ConnectorError};
use novarocks_types::QueryExecutionId;

/// Immutable identity of one driver-local connector writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorWriterObservation {
    execution_id: QueryExecutionId,
    node_id: i32,
    target: WriteTargetOrdinal,
    physical: ConnectorWriterPhysicalContext,
}

impl ConnectorWriterObservation {
    pub const fn new(
        execution_id: QueryExecutionId,
        node_id: i32,
        target: WriteTargetOrdinal,
        physical: ConnectorWriterPhysicalContext,
    ) -> Self {
        Self {
            execution_id,
            node_id,
            target,
            physical,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn node_id(&self) -> i32 {
        self.node_id
    }

    pub const fn target(&self) -> WriteTargetOrdinal {
        self.target
    }

    pub const fn physical(&self) -> ConnectorWriterPhysicalContext {
        self.physical
    }
}

/// A Worker-owned writer lifecycle fact for a role-local observer to render.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteObservation {
    WriterOpened {
        writer: ConnectorWriterObservation,
        catalog: CatalogHandle,
    },
    WriterOpenFailed {
        writer: ConnectorWriterObservation,
        catalog: CatalogHandle,
        reason: String,
    },
    WriterFinished {
        writer: ConnectorWriterObservation,
        rows: u64,
        commit_fragments: u64,
    },
    WriterFinishFailed {
        writer: ConnectorWriterObservation,
        rows: u64,
        reason: String,
    },
    WriterAborted {
        writer: ConnectorWriterObservation,
        rows: u64,
        outcome: ConnectorWriterAbortOutcome,
    },
}

/// The settled outcome of a best-effort local writer abort.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorWriterAbortOutcome {
    Succeeded,
    Failed,
}

/// Renders Worker-observed connector writer facts without gaining write
/// authority.
pub trait ConnectorWriteObservationPort: Send + Sync {
    fn observe(&self, observation: ConnectorWriteObservation);

    /// Returns whether this exact live append should wait for cancellation.
    ///
    /// This is only a debug fault hook.  Returning `true` does not cancel,
    /// retry, or replace the append; the Worker owns the resulting pending
    /// future and the query lifecycle drops it on cancellation.
    fn hold_append(&self, writer: ConnectorWriterObservation) -> bool;
}

/// The Worker-owned decorator around one query-leased write capability.
///
/// It forwards all provider authority unchanged.  The decorator observes facts
/// only after each corresponding provider call settles, so an observer cannot
/// manufacture a writer, a row count, or a commit fragment.
pub struct ObservedConnectorWriteExecution {
    inner: Arc<dyn ConnectorWriteExecution>,
    execution_id: QueryExecutionId,
    node_id: i32,
    observation: Arc<dyn ConnectorWriteObservationPort>,
    emit_writer_markers: bool,
}

impl ObservedConnectorWriteExecution {
    pub fn new(
        inner: Arc<dyn ConnectorWriteExecution>,
        execution_id: QueryExecutionId,
        node_id: i32,
        observation: Arc<dyn ConnectorWriteObservationPort>,
        emit_writer_markers: bool,
    ) -> Self {
        Self {
            inner,
            execution_id,
            node_id,
            observation,
            emit_writer_markers,
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
        let writer = ConnectorWriterObservation::new(
            self.execution_id,
            self.node_id,
            request.target,
            request.physical,
        );
        let catalog = self.inner.catalog_handle().clone();
        match self.inner.open_writer(request).await {
            Ok(inner) => {
                self.observation
                    .observe(ConnectorWriteObservation::WriterOpened { writer, catalog });
                emit_writer_marker(
                    self.emit_writer_markers,
                    "NOVAROCKS_CONNECTOR_WRITER_OPENED",
                    writer.node_id(),
                    writer.target(),
                );
                Ok(Box::new(ObservedConnectorBatchWriter {
                    inner,
                    writer,
                    observation: Arc::clone(&self.observation),
                    rows: 0,
                }))
            }
            Err(error) => {
                self.observation
                    .observe(ConnectorWriteObservation::WriterOpenFailed {
                        writer,
                        catalog,
                        reason: error.to_string(),
                    });
                Err(error)
            }
        }
    }
}

/// One driver's writer, wrapped so its rows and lifecycle facts remain local
/// to the Worker that actually drove it.
struct ObservedConnectorBatchWriter {
    inner: Box<dyn ConnectorBatchWriter>,
    writer: ConnectorWriterObservation,
    observation: Arc<dyn ConnectorWriteObservationPort>,
    rows: u64,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for ObservedConnectorBatchWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        if self.observation.hold_append(self.writer) {
            std::future::pending::<()>().await;
        }
        let rows = batch.num_rows() as u64;
        self.inner.append(batch).await?;
        self.rows = self.rows.saturating_add(rows);
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        match self.inner.finish().await {
            Ok(fragments) => {
                self.observation
                    .observe(ConnectorWriteObservation::WriterFinished {
                        writer: self.writer,
                        rows: self.rows,
                        commit_fragments: fragments.len() as u64,
                    });
                Ok(fragments)
            }
            Err(error) => {
                self.observation
                    .observe(ConnectorWriteObservation::WriterFinishFailed {
                        writer: self.writer,
                        rows: self.rows,
                        reason: error.to_string(),
                    });
                Err(error)
            }
        }
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        let result = self.inner.abort().await;
        self.observation
            .observe(ConnectorWriteObservation::WriterAborted {
                writer: self.writer,
                rows: self.rows,
                outcome: if result.is_ok() {
                    ConnectorWriterAbortOutcome::Succeeded
                } else {
                    ConnectorWriterAbortOutcome::Failed
                },
            });
        result
    }
}

/// Emit one connector-writer evidence marker behind the immutable role input.
///
/// The marker is emitted by the Worker because it observes the writer opening;
/// the Native adapter reads the process-owned debug switch before composing the
/// Worker runtime.
fn emit_writer_marker(enabled: bool, marker: &str, plan_node_id: i32, target: WriteTargetOrdinal) {
    if !enabled {
        return;
    }
    println!(
        "{marker} plan_node={plan_node_id} write_target={}",
        target.get()
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::write_stack::{
        ProviderWriteRuntime, WriteRuntimeAdapter, WriteTargetOrdinal,
    };
    use novarocks_spi::connector::{
        CatalogVersion, ConnectorCancellation, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId, ConnectorRequestContext,
    };
    use novarocks_types::{AttemptId, QueryId};

    use super::*;

    fn catalog_handle() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("write_test").expect("canonical instance id"),
            CatalogVersion::from_bytes([5; 32]),
        )
    }

    #[derive(Debug)]
    struct StubWriteRuntime {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
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
        type WriterHandle = ();
        type CommitFragment = ();

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

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(31, 41), AttemptId::new(2).expect("attempt"))
            .expect("execution id")
    }

    fn target() -> WriteTargetOrdinal {
        WriteTargetOrdinal::try_new(0).expect("bounded ordinal")
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context")
    }

    struct CountingWriteExecution {
        catalog_handle: CatalogHandle,
        opened: Arc<Mutex<Vec<(u32, u32)>>>,
    }

    struct CountingWriter;

    #[async_trait::async_trait]
    impl ConnectorBatchWriter for CountingWriter {
        async fn append(&mut self, _: RecordBatch) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
            Ok(Vec::new())
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
            Ok(Box::new(CountingWriter))
        }
    }

    #[derive(Default)]
    struct RecordingObservation(Mutex<Vec<ConnectorWriteObservation>>);

    impl ConnectorWriteObservationPort for RecordingObservation {
        fn observe(&self, observation: ConnectorWriteObservation) {
            self.0.lock().expect("observations").push(observation);
        }

        fn hold_append(&self, _: ConnectorWriterObservation) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn the_worker_writer_runtime_observes_the_settled_open_and_finish_facts() {
        let opened = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(CountingWriteExecution {
            catalog_handle: catalog_handle(),
            opened: Arc::clone(&opened),
        });
        let observation = Arc::new(RecordingObservation::default());
        let observed = ObservedConnectorWriteExecution::new(
            inner,
            execution_id(),
            7,
            Arc::clone(&observation) as Arc<dyn ConnectorWriteObservationPort>,
            false,
        );
        let physical = ConnectorWriterPhysicalContext::new([1; 16], 2, [3; 16], 4, 5);
        let mut writer = observed
            .open_writer(ConnectorOpenWriterRequest {
                handle: adapter().wrap_writer_handle(()),
                target: target(),
                expected_schema: Arc::new(arrow::datatypes::Schema::empty()),
                physical,
                context: request_context(),
            })
            .await
            .expect("open writer");
        writer.finish().await.expect("finish writer");

        assert_eq!(*opened.lock().expect("opened"), vec![(4, 5)]);
        assert_eq!(
            *observation.0.lock().expect("observations"),
            vec![
                ConnectorWriteObservation::WriterOpened {
                    writer: ConnectorWriterObservation::new(execution_id(), 7, target(), physical),
                    catalog: catalog_handle(),
                },
                ConnectorWriteObservation::WriterFinished {
                    writer: ConnectorWriterObservation::new(execution_id(), 7, target(), physical),
                    rows: 0,
                    commit_fragments: 0,
                },
            ]
        );
    }
}
