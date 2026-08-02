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

//! Iceberg's BE-only DATA writer adapter for the connector write SPI.
//!
//! The adapter deliberately accepts only a secret-free opaque handle plus
//! local startup-bound storage configuration.  It stages data files and emits
//! a canonical provider report; it has neither a catalog client nor a commit
//! capability.  Other Iceberg writer modes remain fail-closed until their
//! equivalence adapters are implemented.

use std::collections::BTreeMap;
use std::time::Instant;

use arrow::array::{Array, Int64Array, StringArray, UInt32Array};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::{
    ConnectorBatchWriter, ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorOpenWriterRequest, ConnectorStagedReport, ConnectorStagedReportSummary,
    ConnectorWriteExecution, ConnectorWriterTerminalState,
};

use super::commit::{
    DeletionVector, write_equality_delete_file, write_single_deletion_vector_puffin,
};
use super::data_writer::{
    StagedDataFile, StagedWriteContext, StagedWriteOptions, cleanup_staged_files,
    staged_data_file_to_writer_report, write_record_batches,
};
use super::delete_file::IcebergFileContent;
use super::position_delete_descriptor::canonical_output_schema;
use super::provider::IcebergReadBinding;
use super::report::{
    IcebergPartitionReport, IcebergWriterReport, partition_path_from_struct,
    writer_report_from_unpartitioned_equality_delete_file,
};
use super::sink::{build_staged_file_io, unique_file_path, write_parquet_file};
use super::sink_plan::IcebergSinkObjectStoreConfig;
use super::write_contract::{
    IcebergPositionDeleteHandle, IcebergPositionDeletePartition, IcebergPositionDeleteStagedFile,
    IcebergWriteHandleMode, data_sink_plan_from_handle_payload,
    equality_delete_handle_from_payload, position_delete_handle_from_payload,
    staged_report_from_iceberg_reports, staged_report_from_position_delete_files,
    staged_report_from_unpartitioned_equality_delete_reports, write_handle_mode,
};
use crate::runtime::global_async_runtime::data_block_on;

/// BE execution capability rooted only in process-startup storage bindings.
#[derive(Clone)]
pub(crate) struct IcebergDataWriteExecution {
    key: ConnectorExecutionBindingKey,
    binding: IcebergReadBinding,
}

impl IcebergDataWriteExecution {
    pub(crate) fn new(key: ConnectorExecutionBindingKey, binding: IcebergReadBinding) -> Self {
        Self { key, binding }
    }
}

impl ConnectorWriteExecution for IcebergDataWriteExecution {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        if request.context.cancellation().is_cancelled() {
            return Err(error(
                ConnectorErrorKind::Cancelled,
                "connector writer open was cancelled",
            ));
        }
        if Instant::now() >= request.context.deadline() {
            return Err(error(
                ConnectorErrorKind::DeadlineExceeded,
                "connector writer open deadline elapsed",
            ));
        }
        if request.handle.owner() != &self.key || request.handle.writer().binding_key() != &self.key
        {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg connector writer handle does not belong to this exact BE binding",
            ));
        }
        match write_handle_mode(request.handle.payload())
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?
        {
            IcebergWriteHandleMode::Data => self.open_data_writer(request),
            IcebergWriteHandleMode::EqualityDeletes => self.open_equality_delete_writer(request),
            IcebergWriteHandleMode::PositionDeletes => self.open_position_delete_writer(request),
            IcebergWriteHandleMode::DeletionVectors => self.open_deletion_vector_writer(request),
            unsupported => Err(error(
                ConnectorErrorKind::Unsupported,
                format!(
                    "Iceberg connector writer mode {unsupported:?} has no BE execution adapter"
                ),
            )),
        }
    }
}

impl IcebergDataWriteExecution {
    fn open_data_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        let mut plan = data_sink_plan_from_handle_payload(
            request.handle.payload(),
            request.expected_schema.clone(),
            None,
        )
        .map_err(|message| error(ConnectorErrorKind::Unsupported, message))?;
        plan.object_store_s3 = self
            .binding
            .write_object_store_config(&plan.data_location)
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let report_file_format = plan.report_file_format.clone();
        let context = plan
            .build_staged_write_context()
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        Ok(Box::new(IcebergDataBatchWriter {
            writer: request.handle.writer().clone(),
            context,
            report_file_format,
            row_lineage_data: plan.row_lineage_data,
            request,
            reports: Vec::new(),
            staged_paths: Vec::new(),
            summary: ConnectorStagedReportSummary::default(),
            terminal: false,
        }))
    }

    fn open_equality_delete_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        let (data_location, partition_spec_id, columns) = equality_delete_handle_from_payload(
            request.handle.payload(),
            request.expected_schema.clone(),
        )
        .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let object_store_s3 = self
            .binding
            .write_object_store_config(&data_location)
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let file_io = build_staged_file_io(&data_location, object_store_s3.as_ref())
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        Ok(Box::new(IcebergEqualityDeleteBatchWriter {
            writer: request.handle.writer().clone(),
            file_io,
            staging_dir: data_location.trim_end_matches('/').to_string(),
            partition_spec_id,
            columns,
            request,
            reports: Vec::new(),
            staged_paths: Vec::new(),
            summary: ConnectorStagedReportSummary::default(),
            terminal: false,
        }))
    }

    fn open_position_delete_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        let handle =
            position_delete_handle_from_payload(request.handle.payload(), &request.expected_schema)
                .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let object_store_s3 = self
            .binding
            .write_object_store_config(&handle.data_location)
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let file_io = build_staged_file_io(&handle.data_location, object_store_s3.as_ref())
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        Ok(Box::new(IcebergPositionDeleteBatchWriter {
            writer: request.handle.writer().clone(),
            file_io,
            object_store_s3,
            handle,
            request,
            files: Vec::new(),
            staged_paths: Vec::new(),
            summary: ConnectorStagedReportSummary::default(),
            next_file_sequence: 0,
            terminal: false,
        }))
    }

    fn open_deletion_vector_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        let handle =
            position_delete_handle_from_payload(request.handle.payload(), &request.expected_schema)
                .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        if handle.mode != IcebergWriteHandleMode::DeletionVectors {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "deletion-vector adapter received a non-DV handle",
            ));
        }
        let object_store_s3 = self
            .binding
            .write_object_store_config(&handle.data_location)
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let file_io = build_staged_file_io(&handle.data_location, object_store_s3.as_ref())
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        Ok(Box::new(IcebergDeletionVectorBatchWriter {
            writer: request.handle.writer().clone(),
            file_io,
            handle,
            request,
            pending: BTreeMap::new(),
            files: Vec::new(),
            staged_paths: Vec::new(),
            summary: ConnectorStagedReportSummary::default(),
            next_file_sequence: 0,
            terminal: false,
        }))
    }
}

struct IcebergDataBatchWriter {
    writer: novarocks_spi::connector::ConnectorWriterIdentity,
    context: StagedWriteContext,
    report_file_format: String,
    row_lineage_data: bool,
    request: ConnectorOpenWriterRequest,
    reports: Vec<IcebergWriterReport>,
    staged_paths: Vec<String>,
    summary: ConnectorStagedReportSummary,
    terminal: bool,
}

impl IcebergDataBatchWriter {
    fn ensure_open(&self) -> Result<(), ConnectorError> {
        if self.terminal {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg connector batch writer is already terminal",
            ));
        }
        if self.request.context.cancellation().is_cancelled() {
            return Err(error(
                ConnectorErrorKind::Cancelled,
                "Iceberg connector writer was cancelled",
            ));
        }
        if Instant::now() >= self.request.context.deadline() {
            return Err(error(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg connector writer deadline elapsed",
            ));
        }
        Ok(())
    }

    fn record_staged_files(
        &mut self,
        staged_files: Vec<StagedDataFile>,
        first_row_id: Option<i64>,
    ) -> Result<(), ConnectorError> {
        let mut first_row_id_cursor = first_row_id;
        for staged_file in staged_files {
            self.staged_paths
                .push(staged_file.data_file.file_path().to_string());
            let partition_spec = staged_file
                .metadata
                .partition_spec_by_id(staged_file.partition_spec_id)
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "staged Iceberg file references an unknown partition spec",
                    )
                })?;
            let (partition_path, null_fingerprint) =
                partition_path_from_struct(staged_file.data_file.partition(), &partition_spec)
                    .map_err(|message| error(ConnectorErrorKind::CorruptData, message))?;
            let (mut report, _) = staged_data_file_to_writer_report(
                &staged_file,
                IcebergPartitionReport {
                    partition_path,
                    null_fingerprint,
                    partition_spec_id: staged_file.partition_spec_id,
                    partition_values: staged_file.data_file.partition().clone(),
                },
                self.report_file_format.clone(),
                IcebergFileContent::Data,
            )
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            if let Some(cursor) = first_row_id_cursor.as_mut() {
                report.file.first_row_id = Some(*cursor);
                *cursor = cursor
                    .checked_add(report.file.record_count)
                    .ok_or_else(|| {
                        error(
                            ConnectorErrorKind::CorruptData,
                            "Iceberg row-lineage writer first_row_id overflow",
                        )
                    })?;
            }
            self.summary.artifact_count = self.summary.artifact_count.saturating_add(1);
            self.summary.staged_bytes = self
                .summary
                .staged_bytes
                .saturating_add(report.file.file_size_in_bytes.max(0) as u64);
            self.reports.push(report);
        }
        Ok(())
    }

    fn append_row_lineage_batch(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        let row_id_idx = batch
            .schema()
            .index_of(crate::exec::row_position::ICEBERG_ROW_ID_COL)
            .map_err(|_| {
                error(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg row-lineage writer input is missing _row_id",
                )
            })?;
        let row_ids = batch
            .column(row_id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg row-lineage writer _row_id column must be Int64",
                )
            })?;
        if row_ids.null_count() > 0 {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg row-lineage writer rejects NULL _row_id",
            ));
        }
        let mut indices = (0..batch.num_rows())
            .map(|index| u32::try_from(index).expect("record batch index fits u32"))
            .collect::<Vec<_>>();
        indices.sort_by_key(|index| row_ids.value(*index as usize));
        let mut start = 0usize;
        while start < indices.len() {
            let first = row_ids.value(indices[start] as usize);
            if first < 0 {
                return Err(error(
                    ConnectorErrorKind::InvalidRequest,
                    format!("Iceberg row-lineage writer _row_id must be non-negative: {first}"),
                ));
            }
            let mut end = start + 1;
            let mut previous = first;
            while end < indices.len() {
                let next = row_ids.value(indices[end] as usize);
                if next < 0 {
                    return Err(error(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg row-lineage writer _row_id must be non-negative: {next}"),
                    ));
                }
                if next == previous {
                    return Err(error(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg row-lineage writer encountered duplicate _row_id {next}"),
                    ));
                }
                if next != previous + 1 {
                    break;
                }
                previous = next;
                end += 1;
            }
            let run_indices = UInt32Array::from(indices[start..end].to_vec());
            let run =
                arrow::compute::take_record_batch(&batch, &run_indices).map_err(|arrow_error| {
                    error(ConnectorErrorKind::Internal, arrow_error.to_string())
                })?;
            let staged = data_block_on(write_record_batches(
                &self.context,
                [run],
                &StagedWriteOptions::default(),
            ))
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            self.record_staged_files(staged, Some(first))?;
            start = end;
        }
        Ok(())
    }
}

impl ConnectorBatchWriter for IcebergDataBatchWriter {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let input_rows = batch.num_rows() as u64;
        if self.row_lineage_data {
            self.append_row_lineage_batch(batch)?;
        } else {
            let staged = data_block_on(write_record_batches(
                &self.context,
                [batch],
                &StagedWriteOptions::default(),
            ))
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            self.record_staged_files(staged, None)?;
        }
        self.summary.input_rows = self.summary.input_rows.saturating_add(input_rows);
        Ok(())
    }

    fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError> {
        self.ensure_open()?;
        self.terminal = true;
        staged_report_from_iceberg_reports(
            self.writer.clone(),
            ConnectorWriterTerminalState::Staged,
            self.summary.clone(),
            &self.reports,
            self.context.metadata(),
        )
        .map_err(|message| error(ConnectorErrorKind::Internal, message))
    }

    fn abort(&mut self) -> Result<(), ConnectorError> {
        if !self.staged_paths.is_empty() {
            data_block_on(cleanup_staged_files(&self.context, &self.staged_paths))
                .map_err(|message| error(ConnectorErrorKind::Internal, message))?
                .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            self.staged_paths.clear();
        }
        self.terminal = true;
        Ok(())
    }

    fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary.clone()
    }
}

struct IcebergEqualityDeleteBatchWriter {
    writer: novarocks_spi::connector::ConnectorWriterIdentity,
    file_io: iceberg::io::FileIO,
    staging_dir: String,
    partition_spec_id: i32,
    columns: Vec<super::commit::EqualityDeleteColumn>,
    request: ConnectorOpenWriterRequest,
    reports: Vec<IcebergWriterReport>,
    staged_paths: Vec<String>,
    summary: ConnectorStagedReportSummary,
    terminal: bool,
}

struct IcebergPositionDeleteStagedFileOwned {
    path: String,
    record_count: i64,
    file_size_in_bytes: i64,
    split_offsets: Option<Vec<i64>>,
    column_stats: Option<super::report::IcebergColumnStats>,
    referenced_data_file: String,
    partition: IcebergPositionDeletePartition,
    format: String,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    cardinality: Option<i64>,
}

struct IcebergPositionDeleteBatchWriter {
    writer: novarocks_spi::connector::ConnectorWriterIdentity,
    file_io: iceberg::io::FileIO,
    object_store_s3: Option<IcebergSinkObjectStoreConfig>,
    handle: IcebergPositionDeleteHandle,
    request: ConnectorOpenWriterRequest,
    files: Vec<IcebergPositionDeleteStagedFileOwned>,
    staged_paths: Vec<String>,
    summary: ConnectorStagedReportSummary,
    next_file_sequence: u64,
    terminal: bool,
}

impl IcebergPositionDeleteBatchWriter {
    fn ensure_open(&self) -> Result<(), ConnectorError> {
        if self.terminal {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg position-delete batch writer is already terminal",
            ));
        }
        if self.request.context.cancellation().is_cancelled() {
            return Err(error(
                ConnectorErrorKind::Cancelled,
                "Iceberg position-delete writer was cancelled",
            ));
        }
        if Instant::now() >= self.request.context.deadline() {
            return Err(error(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg position-delete writer deadline elapsed",
            ));
        }
        Ok(())
    }

    fn next_path(&mut self, partition: &IcebergPositionDeletePartition) -> String {
        let base = self.handle.data_location.trim_end_matches('/');
        let fragment = uuid::Uuid::from_bytes(self.writer.fragment_instance_id());
        let name = format!(
            "delete-{}-{}-sink{}-{}.parquet",
            self.writer.operation_id(),
            fragment,
            self.writer.sink_ordinal(),
            self.next_file_sequence,
        );
        self.next_file_sequence = self.next_file_sequence.saturating_add(1);
        let partition = partition.partition_path.trim_matches('/');
        if partition.is_empty() {
            format!("{base}/{name}")
        } else {
            format!("{base}/{partition}/{name}")
        }
    }

    fn cleanup_staged_paths(&mut self) -> Result<(), ConnectorError> {
        if self.staged_paths.is_empty() {
            return Ok(());
        }
        let paths = self.staged_paths.clone();
        data_block_on(async {
            for path in &paths {
                self.file_io.delete(path).await.map_err(|error| {
                    format!("cleanup staged position-delete file {path} failed: {error}")
                })?;
            }
            Ok::<(), String>(())
        })
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        self.staged_paths.clear();
        Ok(())
    }
}

impl ConnectorBatchWriter for IcebergPositionDeleteBatchWriter {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let file_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg position-delete _file column must be Utf8",
                )
            })?;
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg position-delete _pos column must be Int64",
                )
            })?;
        if file_paths.null_count() != 0 || positions.null_count() != 0 {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg position-delete rows must not contain NULL _file or _pos",
            ));
        }
        let mut groups = BTreeMap::<String, Vec<u32>>::new();
        for row in 0..batch.num_rows() {
            let file = file_paths.value(row);
            if !self.handle.partitions.contains_key(file) {
                return Err(error(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg position-delete handle has no frozen partition for data file `{file}`"
                    ),
                ));
            }
            groups.entry(file.to_string()).or_default().push(row as u32);
        }
        let input_rows = batch.num_rows() as u64;
        for (referenced_data_file, mut indices) in groups {
            indices.sort_by_key(|row| positions.value(*row as usize));
            let part_batch = arrow::compute::take_record_batch(&batch, &UInt32Array::from(indices))
                .map_err(|arrow_error| {
                    error(
                        ConnectorErrorKind::Internal,
                        format!("take Iceberg position-delete batch failed: {arrow_error}"),
                    )
                })?;
            let delete_batch = position_delete_storage_batch(&part_batch)?;
            let partition = self
                .handle
                .partitions
                .get(&referenced_data_file)
                .cloned()
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::InvalidRequest,
                        "Iceberg position-delete partition disappeared from frozen handle",
                    )
                })?;
            let path = self.next_path(&partition);
            let written = write_parquet_file(
                &path,
                self.object_store_s3.as_ref(),
                delete_batch.schema(),
                &delete_batch,
                self.handle.compression,
            )
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            let record_count = i64::try_from(delete_batch.num_rows()).map_err(|_| {
                error(
                    ConnectorErrorKind::Internal,
                    "Iceberg position-delete record count overflows i64",
                )
            })?;
            let file_size_in_bytes = i64::try_from(written.file_size).map_err(|_| {
                error(
                    ConnectorErrorKind::Internal,
                    "Iceberg position-delete file size overflows i64",
                )
            })?;
            let reported_reference = unique_file_path(&delete_batch)
                .map_err(|message| error(ConnectorErrorKind::CorruptData, message))?
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "partitioned position-delete file must reference one data file",
                    )
                })?;
            if reported_reference != referenced_data_file {
                return Err(error(
                    ConnectorErrorKind::CorruptData,
                    "position-delete batch grouping changed referenced data file",
                ));
            }
            self.staged_paths.push(path.clone());
            self.summary.artifact_count = self.summary.artifact_count.saturating_add(1);
            self.summary.staged_bytes = self.summary.staged_bytes.saturating_add(written.file_size);
            self.files.push(IcebergPositionDeleteStagedFileOwned {
                path,
                record_count,
                file_size_in_bytes,
                split_offsets: written.split_offsets,
                column_stats: written.column_stats,
                referenced_data_file,
                partition,
                format: self.handle.report_file_format.clone(),
                content_offset: None,
                content_size_in_bytes: None,
                cardinality: None,
            });
        }
        self.summary.input_rows = self.summary.input_rows.saturating_add(input_rows);
        Ok(())
    }

    fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError> {
        self.ensure_open()?;
        self.terminal = true;
        let files = self
            .files
            .iter()
            .map(|file| IcebergPositionDeleteStagedFile {
                path: &file.path,
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                split_offsets: file.split_offsets.clone(),
                column_stats: file.column_stats.clone(),
                referenced_data_file: file.referenced_data_file.clone(),
                partition: &file.partition,
                format: &file.format,
                content_offset: file.content_offset,
                content_size_in_bytes: file.content_size_in_bytes,
                cardinality: file.cardinality,
            })
            .collect::<Vec<_>>();
        staged_report_from_position_delete_files(
            self.writer.clone(),
            ConnectorWriterTerminalState::Staged,
            self.summary.clone(),
            &files,
        )
        .map_err(|message| error(ConnectorErrorKind::Internal, message))
    }

    fn abort(&mut self) -> Result<(), ConnectorError> {
        self.cleanup_staged_paths()?;
        self.terminal = true;
        Ok(())
    }

    fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary.clone()
    }
}

/// BE-only deletion-vector writer.  It combines incoming positions with the
/// canonical old vector frozen by FE planning, then stages one replacement
/// Puffin file per referenced data file.  No catalog, manifest, or prior
/// delete file is opened on the BE.
struct IcebergDeletionVectorBatchWriter {
    writer: novarocks_spi::connector::ConnectorWriterIdentity,
    file_io: iceberg::io::FileIO,
    handle: IcebergPositionDeleteHandle,
    request: ConnectorOpenWriterRequest,
    pending: BTreeMap<String, DeletionVector>,
    files: Vec<IcebergPositionDeleteStagedFileOwned>,
    staged_paths: Vec<String>,
    summary: ConnectorStagedReportSummary,
    next_file_sequence: u64,
    terminal: bool,
}

impl IcebergDeletionVectorBatchWriter {
    fn ensure_open(&self) -> Result<(), ConnectorError> {
        if self.terminal {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg deletion-vector batch writer is already terminal",
            ));
        }
        if self.request.context.cancellation().is_cancelled() {
            return Err(error(
                ConnectorErrorKind::Cancelled,
                "Iceberg deletion-vector writer was cancelled",
            ));
        }
        if Instant::now() >= self.request.context.deadline() {
            return Err(error(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg deletion-vector writer deadline elapsed",
            ));
        }
        Ok(())
    }

    fn next_path(&mut self, partition: &IcebergPositionDeletePartition) -> String {
        let base = self.handle.data_location.trim_end_matches('/');
        let fragment = uuid::Uuid::from_bytes(self.writer.fragment_instance_id());
        let name = format!(
            "dv-{}-{}-sink{}-{}.puffin",
            self.writer.operation_id(),
            fragment,
            self.writer.sink_ordinal(),
            self.next_file_sequence,
        );
        self.next_file_sequence = self.next_file_sequence.saturating_add(1);
        let partition = partition.partition_path.trim_matches('/');
        if partition.is_empty() {
            format!("{base}/{name}")
        } else {
            format!("{base}/{partition}/{name}")
        }
    }

    fn cleanup_staged_paths(&mut self) -> Result<(), ConnectorError> {
        let paths = self.staged_paths.clone();
        data_block_on(async {
            for path in &paths {
                self.file_io.delete(path).await.map_err(|error| {
                    format!("cleanup staged deletion-vector file {path} failed: {error}")
                })?;
            }
            Ok::<(), String>(())
        })
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        self.staged_paths.clear();
        Ok(())
    }
}

impl ConnectorBatchWriter for IcebergDeletionVectorBatchWriter {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let file_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg deletion-vector _file column must be Utf8",
                )
            })?;
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg deletion-vector _pos column must be Int64",
                )
            })?;
        if file_paths.null_count() != 0 || positions.null_count() != 0 {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg deletion-vector rows must not contain NULL _file or _pos",
            ));
        }
        for row in 0..batch.num_rows() {
            let referenced = file_paths.value(row);
            if !self.handle.partitions.contains_key(referenced) {
                return Err(error(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg deletion-vector handle has no frozen partition for data file `{referenced}`"
                    ),
                ));
            }
            let position = u64::try_from(positions.value(row)).map_err(|_| {
                error(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg deletion-vector position must be non-negative",
                )
            })?;
            self.pending
                .entry(referenced.to_string())
                .or_default()
                .insert(position)
                .map_err(|vector_error| {
                    error(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg deletion-vector position is invalid: {vector_error}"),
                    )
                })?;
        }
        self.summary.input_rows = self
            .summary
            .input_rows
            .saturating_add(batch.num_rows() as u64);
        Ok(())
    }

    fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError> {
        self.ensure_open()?;
        self.terminal = true;
        let pending = std::mem::take(&mut self.pending);
        for (referenced_data_file, mut vector) in pending {
            let partition = self
                .handle
                .partitions
                .get(&referenced_data_file)
                .cloned()
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "frozen deletion-vector partition disappeared",
                    )
                })?;
            if let Some(payload) = &partition.existing_deletion_vector_payload {
                let existing =
                    DeletionVector::from_iceberg_payload(payload).map_err(|vector_error| {
                        error(
                            ConnectorErrorKind::CorruptData,
                            format!("decode frozen deletion vector failed: {vector_error}"),
                        )
                    })?;
                vector.merge(&existing);
            }
            let path = self.next_path(&partition);
            let written = data_block_on(write_single_deletion_vector_puffin(
                &self.file_io,
                &path,
                &referenced_data_file,
                &vector,
            ))
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?
            .map_err(|write_error| {
                error(
                    ConnectorErrorKind::Internal,
                    format!("stage Iceberg deletion vector failed: {write_error}"),
                )
            })?;
            let record_count = i64::try_from(written.cardinality).map_err(|_| {
                error(
                    ConnectorErrorKind::Internal,
                    "Iceberg deletion-vector cardinality overflows i64",
                )
            })?;
            let file_size_in_bytes = i64::try_from(written.file_size_in_bytes).map_err(|_| {
                error(
                    ConnectorErrorKind::Internal,
                    "Iceberg deletion-vector file size overflows i64",
                )
            })?;
            self.summary.artifact_count = self.summary.artifact_count.saturating_add(1);
            self.summary.staged_bytes = self
                .summary
                .staged_bytes
                .saturating_add(written.file_size_in_bytes);
            self.staged_paths.push(written.path.clone());
            self.files.push(IcebergPositionDeleteStagedFileOwned {
                path: written.path,
                record_count,
                file_size_in_bytes,
                split_offsets: None,
                column_stats: None,
                referenced_data_file: written.referenced_data_file,
                partition,
                format: "puffin".to_string(),
                content_offset: Some(written.content_offset),
                content_size_in_bytes: Some(written.content_size_in_bytes),
                cardinality: Some(record_count),
            });
        }
        let files = self
            .files
            .iter()
            .map(|file| IcebergPositionDeleteStagedFile {
                path: &file.path,
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                split_offsets: file.split_offsets.clone(),
                column_stats: file.column_stats.clone(),
                referenced_data_file: file.referenced_data_file.clone(),
                partition: &file.partition,
                format: &file.format,
                content_offset: file.content_offset,
                content_size_in_bytes: file.content_size_in_bytes,
                cardinality: file.cardinality,
            })
            .collect::<Vec<_>>();
        staged_report_from_position_delete_files(
            self.writer.clone(),
            ConnectorWriterTerminalState::Staged,
            self.summary.clone(),
            &files,
        )
        .map_err(|message| error(ConnectorErrorKind::Internal, message))
    }

    fn abort(&mut self) -> Result<(), ConnectorError> {
        self.cleanup_staged_paths()?;
        self.terminal = true;
        Ok(())
    }
    fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary.clone()
    }
}

impl IcebergEqualityDeleteBatchWriter {
    fn ensure_open(&self) -> Result<(), ConnectorError> {
        if self.terminal {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg equality-delete batch writer is already terminal",
            ));
        }
        if self.request.context.cancellation().is_cancelled() {
            return Err(error(
                ConnectorErrorKind::Cancelled,
                "Iceberg equality-delete writer was cancelled",
            ));
        }
        if Instant::now() >= self.request.context.deadline() {
            return Err(error(
                ConnectorErrorKind::DeadlineExceeded,
                "Iceberg equality-delete writer deadline elapsed",
            ));
        }
        Ok(())
    }

    fn cleanup_staged_paths(&mut self) -> Result<(), ConnectorError> {
        if self.staged_paths.is_empty() {
            return Ok(());
        }
        let paths = self.staged_paths.clone();
        data_block_on(async {
            for path in &paths {
                self.file_io.delete(path).await.map_err(|error| {
                    format!("cleanup staged equality-delete file {path} failed: {error}")
                })?;
            }
            Ok::<(), String>(())
        })
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        self.staged_paths.clear();
        Ok(())
    }
}

impl ConnectorBatchWriter for IcebergEqualityDeleteBatchWriter {
    fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.ensure_open()?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let input_rows = batch.num_rows() as u64;
        let written = data_block_on(write_equality_delete_file(
            &self.file_io,
            &self.staging_dir,
            self.partition_spec_id,
            self.columns.clone(),
            batch,
        ))
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        if let Some(written) = written {
            let report = writer_report_from_unpartitioned_equality_delete_file(&written)
                .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            self.staged_paths.push(written.path);
            self.summary.artifact_count = self.summary.artifact_count.saturating_add(1);
            self.summary.staged_bytes = self
                .summary
                .staged_bytes
                .saturating_add(report.file.file_size_in_bytes.max(0) as u64);
            self.reports.push(report);
        }
        self.summary.input_rows = self.summary.input_rows.saturating_add(input_rows);
        Ok(())
    }

    fn finish(&mut self) -> Result<ConnectorStagedReport, ConnectorError> {
        self.ensure_open()?;
        self.terminal = true;
        staged_report_from_unpartitioned_equality_delete_reports(
            self.writer.clone(),
            ConnectorWriterTerminalState::Staged,
            self.summary.clone(),
            &self.reports,
        )
        .map_err(|message| error(ConnectorErrorKind::Internal, message))
    }

    fn abort(&mut self) -> Result<(), ConnectorError> {
        self.cleanup_staged_paths()?;
        self.terminal = true;
        Ok(())
    }

    fn summary(&self) -> ConnectorStagedReportSummary {
        self.summary.clone()
    }
}

/// The execution plan uses `_file`/`_pos` as internal row-identity columns,
/// while Iceberg position-delete Parquet files have the standardized
/// `file_path`/`pos` schema.  Keep that translation at the writer boundary so
/// an internal construction detail never becomes persisted table data.
fn position_delete_storage_batch(batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    let input_schema = batch.schema();
    if input_schema.fields().len() != 2 {
        return Err(error(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg position-delete storage batch requires exactly two columns",
        ));
    }
    let schema = canonical_output_schema();
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(|arrow_error| {
        error(
            ConnectorErrorKind::Internal,
            format!("build Iceberg position-delete storage batch failed: {arrow_error}"),
        )
    })
}

fn error(kind: ConnectorErrorKind, message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(kind, message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::position_delete_storage_batch;
    use crate::connector::iceberg::position_delete_descriptor::{
        ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
    };

    #[test]
    fn position_delete_storage_batch_uses_iceberg_column_names() {
        let input = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["s3://warehouse/t/data.parquet"])),
                Arc::new(Int64Array::from(vec![4])),
            ],
        )
        .expect("internal position-delete batch");

        let stored = position_delete_storage_batch(&input).expect("storage batch");

        assert_eq!(stored.schema().field(0).name(), "file_path");
        assert_eq!(stored.schema().field(1).name(), "pos");
        assert_eq!(
            stored
                .schema()
                .field(0)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID.to_string())
        );
        assert_eq!(
            stored
                .schema()
                .field(1)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY),
            Some(&ICEBERG_POSITION_DELETE_POS_FIELD_ID.to_string())
        );
        assert_eq!(stored.column(0).as_ref(), input.column(0).as_ref());
        assert_eq!(stored.column(1).as_ref(), input.column(1).as_ref());
    }
}
