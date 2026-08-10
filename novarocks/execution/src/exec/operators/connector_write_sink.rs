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

//! Provider-neutral Arrow batch staging sink.
//!
//! A factory owns one logical SPI writer for a fragment-instance/sink ordinal.
//! Pipeline drivers serialize append calls through that writer and only the
//! final driver calls `finish`; therefore exactly one opaque staged report is
//! published to the fragment-owned collector.  This module deliberately has
//! no catalog, commit, or Iceberg dependencies.

use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{ConnectorBatchWriter, ConnectorOpenWriterRequest};

use crate::exec::chunk::Chunk;
use crate::exec::fragment::sink::{ConnectorWriteInputProjection, ConnectorWriteSinkProgram};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::connector_write_report::ConnectorStagedReportCollector;
use crate::runtime::runtime_state::RuntimeState;

pub struct ConnectorWriteSinkFactory {
    name: String,
    shared: Arc<Mutex<ConnectorWriterLifecycle>>,
    input_ordinals: Option<Vec<usize>>,
    input_projection: Option<ConnectorWriteInputProjection>,
    report_collector: ConnectorStagedReportCollector,
}

struct ConnectorWriterLifecycle {
    writer: Option<Box<dyn ConnectorBatchWriter>>,
    driver_count: usize,
    finished_drivers: usize,
    terminal: bool,
}

impl ConnectorWriteSinkFactory {
    pub fn try_new(program: &ConnectorWriteSinkProgram) -> Result<Self, String> {
        let request = program.request().clone();
        let writer = open_writer(program, request)?;
        Ok(Self {
            name: program.name().to_string(),
            shared: Arc::new(Mutex::new(ConnectorWriterLifecycle {
                writer: Some(writer),
                driver_count: 0,
                finished_drivers: 0,
                terminal: false,
            })),
            input_ordinals: program.input_ordinals().map(ToOwned::to_owned),
            input_projection: program.input_projection(),
            report_collector: program.report_collector(),
        })
    }
}

fn open_writer(
    program: &ConnectorWriteSinkProgram,
    request: ConnectorOpenWriterRequest,
) -> Result<Box<dyn ConnectorBatchWriter>, String> {
    let execution = program.binding().write().ok_or_else(|| {
        "resolved connector execution binding has no write capability during sink materialization"
            .to_string()
    })?;
    let writer = execution
        .open_writer(request)
        .map_err(|error| format!("open connector batch writer: {error}"))?;
    eprintln!("NOVAROCKS_CONNECTOR_WRITER_OPENED");
    Ok(writer)
}

impl OperatorFactory for ConnectorWriteSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        let mut lifecycle = self.shared.lock().expect("connector writer lifecycle lock");
        lifecycle.driver_count = usize::try_from(dop.max(1)).unwrap_or(1);
        drop(lifecycle);
        Box::new(ConnectorWriteSinkOperator {
            name: self.name.clone(),
            shared: Arc::clone(&self.shared),
            input_ordinals: self.input_ordinals.clone(),
            input_projection: self.input_projection.clone(),
            report_collector: self.report_collector.clone(),
            finishing: false,
            runtime_bound: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct ConnectorWriteSinkOperator {
    name: String,
    shared: Arc<Mutex<ConnectorWriterLifecycle>>,
    input_ordinals: Option<Vec<usize>>,
    input_projection: Option<ConnectorWriteInputProjection>,
    report_collector: ConnectorStagedReportCollector,
    finishing: bool,
    runtime_bound: bool,
}

impl Operator for ConnectorWriteSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.runtime_bound {
            return Ok(());
        }
        let collector = state.connector_staged_report_collector().ok_or_else(|| {
            "connector writer sink requires a fragment-owned staged-report collector".to_string()
        })?;
        if !collector.same_instance(&self.report_collector) {
            return Err(
                "connector writer sink collector does not match fragment runtime ownership"
                    .to_string(),
            );
        }
        self.runtime_bound = true;
        Ok(())
    }

    fn cancel(&mut self) {
        let _ = abort_writer(&self.shared);
        self.finishing = true;
    }

    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        self.finishing
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for ConnectorWriteSinkOperator {
    fn need_input(&self) -> bool {
        !self.finishing
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        let batch = match &self.input_projection {
            Some(projection) => projection.project(&chunk)?,
            None => project_batch(chunk.batch, self.input_ordinals.as_deref())?,
        };
        let mut lifecycle = self
            .shared
            .lock()
            .map_err(|error| format!("lock connector writer lifecycle: {error}"))?;
        if lifecycle.terminal {
            return Err("connector writer received a batch after terminal transition".to_string());
        }
        lifecycle
            .writer
            .as_mut()
            .ok_or_else(|| "connector writer is unavailable".to_string())?
            .append(batch)
            .map_err(|error| format!("append connector batch: {error}"))
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finishing {
            return Ok(());
        }
        let report = {
            let mut lifecycle = self
                .shared
                .lock()
                .map_err(|error| format!("lock connector writer lifecycle: {error}"))?;
            if lifecycle.terminal {
                self.finishing = true;
                return Ok(());
            }
            lifecycle.finished_drivers = lifecycle.finished_drivers.saturating_add(1);
            if lifecycle.finished_drivers < lifecycle.driver_count {
                self.finishing = true;
                return Ok(());
            }
            lifecycle.terminal = true;
            lifecycle
                .writer
                .as_mut()
                .ok_or_else(|| "connector writer is unavailable during finish".to_string())?
                .finish()
                .map_err(|error| format!("finish connector batch writer: {error}"))?
        };
        if let Err(error) = self.report_collector.record(report) {
            let _ = abort_writer(&self.shared);
            return Err(error);
        }
        self.finishing = true;
        Ok(())
    }
}

fn abort_writer(shared: &Arc<Mutex<ConnectorWriterLifecycle>>) -> Result<(), String> {
    let mut lifecycle = shared
        .lock()
        .map_err(|error| format!("lock connector writer lifecycle: {error}"))?;
    if lifecycle.terminal {
        return Ok(());
    }
    lifecycle.terminal = true;
    if let Some(writer) = lifecycle.writer.as_mut() {
        writer
            .abort()
            .map_err(|error| format!("abort connector batch writer: {error}"))?;
    }
    Ok(())
}

fn project_batch(batch: RecordBatch, ordinals: Option<&[usize]>) -> Result<RecordBatch, String> {
    let Some(ordinals) = ordinals else {
        return Ok(batch);
    };
    let fields = ordinals
        .iter()
        .map(|ordinal| {
            batch
                .schema()
                .fields()
                .get(*ordinal)
                .cloned()
                .ok_or_else(|| {
                    format!("connector writer input ordinal {ordinal} is outside batch schema")
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let columns = ordinals
        .iter()
        .map(|ordinal| {
            batch.columns().get(*ordinal).cloned().ok_or_else(|| {
                format!("connector writer input ordinal {ordinal} is outside batch columns")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
        .map_err(|error| format!("project connector writer input batch: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorError, ConnectorExecutionBinding,
        ConnectorExecutionBindingKey, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorProviderId, ConnectorRequestContext, ConnectorStagedReport,
        ConnectorStagedReportSummary, ConnectorWriteExecution, ConnectorWriteExecutionId,
        ConnectorWriteOperationId, ConnectorWriterHandle, ConnectorWriterIdentity,
        ConnectorWriterTerminalState,
    };

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use novarocks_types::SlotId;

    #[derive(Default)]
    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Default)]
    struct WriterStats {
        appended_rows: AtomicUsize,
        finished: AtomicUsize,
        aborted: AtomicUsize,
    }

    struct TestWriteExecution {
        key: ConnectorExecutionBindingKey,
        stats: Arc<WriterStats>,
    }

    impl ConnectorWriteExecution for TestWriteExecution {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn open_writer(
            &self,
            request: ConnectorOpenWriterRequest,
        ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
            Ok(Box::new(TestBatchWriter {
                writer: request.handle.writer().clone(),
                stats: Arc::clone(&self.stats),
            }))
        }
    }

    struct TestBatchWriter {
        writer: ConnectorWriterIdentity,
        stats: Arc<WriterStats>,
    }

    impl ConnectorBatchWriter for TestBatchWriter {
        fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
            self.stats
                .appended_rows
                .fetch_add(batch.num_rows(), Ordering::Relaxed);
            Ok(())
        }

        fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError> {
            self.stats.finished.fetch_add(1, Ordering::Relaxed);
            ConnectorStagedReport::try_new(
                self.writer.clone(),
                1,
                ConnectorWriterTerminalState::Staged,
                ConnectorStagedReportSummary {
                    input_rows: self.stats.appended_rows.load(Ordering::Relaxed) as u64,
                    staged_bytes: 0,
                    artifact_count: 1,
                },
                Bytes::from_static(b"test-staged-report"),
            )
        }

        fn abort(&mut self) -> Result<(), ConnectorError> {
            self.stats.aborted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn test_program(stats: Arc<WriterStats>) -> ConnectorWriteSinkProgram {
        let key = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("test.connector").expect("instance"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let operation_id = ConnectorWriteOperationId::from_bytes([1; 16]);
        let writer = ConnectorWriterIdentity::new(
            operation_id,
            novarocks_spi::connector::ConnectorWriteCohortId::primary(operation_id),
            ConnectorWriteExecutionId::new([2; 16], 3),
            [4; 16],
            5,
            6,
            0,
            key.clone(),
        );
        let handle =
            ConnectorWriterHandle::try_new(key.clone(), writer, 1, Bytes::new()).expect("handle");
        let binding = Arc::new(
            ConnectorExecutionBinding::try_new_capabilities(
                ConnectorProviderId::parse("test").expect("provider"),
                key,
                None,
                Some(Arc::new(TestWriteExecution {
                    key: handle.owner().clone(),
                    stats,
                })),
            )
            .expect("binding"),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("context");
        ConnectorWriteSinkProgram::try_new(
            binding,
            ConnectorOpenWriterRequest {
                handle,
                expected_schema: schema,
                context,
            },
            1,
            None,
        )
        .expect("program")
    }

    fn int_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .expect("batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn drivers_share_one_writer_and_publish_one_report() {
        let stats = Arc::new(WriterStats::default());
        let factory =
            ConnectorWriteSinkFactory::try_new(&test_program(Arc::clone(&stats))).expect("factory");
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        let state = RuntimeState::default();
        first
            .as_processor_mut()
            .expect("first processor")
            .push_chunk(&state, int_chunk(vec![1, 2]))
            .expect("first append");
        second
            .as_processor_mut()
            .expect("second processor")
            .push_chunk(&state, int_chunk(vec![3]))
            .expect("second append");
        first
            .as_processor_mut()
            .expect("first processor")
            .set_finishing(&state)
            .expect("first finish");
        assert_eq!(stats.finished.load(Ordering::Relaxed), 0);
        second
            .as_processor_mut()
            .expect("second processor")
            .set_finishing(&state)
            .expect("second finish");
        assert_eq!(stats.appended_rows.load(Ordering::Relaxed), 3);
        assert_eq!(stats.finished.load(Ordering::Relaxed), 1);
        assert_eq!(factory.report_collector.take().len(), 1);
    }

    #[test]
    fn cancellation_aborts_shared_writer_once() {
        let stats = Arc::new(WriterStats::default());
        let factory =
            ConnectorWriteSinkFactory::try_new(&test_program(Arc::clone(&stats))).expect("factory");
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        first.cancel();
        second.cancel();
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 1);
    }
}
