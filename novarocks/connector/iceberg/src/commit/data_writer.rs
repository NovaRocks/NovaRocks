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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[path = "data_writer/staged_paths.rs"]
mod staged_paths;
pub(crate) use staged_paths::StagedPathJournal;

use super::write_adaptation::adapt_batch;
use crate as novarocks_connector_iceberg;
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use novarocks_connector_iceberg::iceberg::arrow::{
    RecordBatchPartitionSplitter, schema_to_arrow_schema,
};
use novarocks_connector_iceberg::iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, NestedField, PartitionSpecRef, PrimitiveType,
    SchemaRef, TableMetadata, Type,
};
use novarocks_connector_iceberg::iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use novarocks_connector_iceberg::iceberg::writer::file_writer::ParquetWriterBuilder;
use novarocks_connector_iceberg::iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator,
};
use novarocks_connector_iceberg::iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use novarocks_connector_iceberg::iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use novarocks_connector_iceberg::row_lineage_synth::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use parquet::file::properties::WriterProperties;

use novarocks_connector_iceberg::commit::report as iceberg_report;
use novarocks_connector_iceberg::delete_file::IcebergFileContent;

type IcebergDataFileWriterBuilder = DataFileWriterBuilder<
    ParquetWriterBuilder,
    JournaledLocationGenerator,
    DefaultFileNameGenerator,
>;

/// Wrap the original SDK's resolved path, including its real partition/object
/// placement policy. No copied path template or second allocation identity.
#[derive(Clone, Debug)]
struct JournaledLocationGenerator {
    original: DefaultLocationGenerator,
    journal: Option<StagedPathJournal>,
}
impl LocationGenerator for JournaledLocationGenerator {
    fn generate_location(
        &self,
        partition: Option<&crate::iceberg::spec::PartitionKey>,
        name: &str,
    ) -> String {
        let path = self.original.generate_location(partition, name);
        if let Some(journal) = &self.journal {
            journal.record(&path);
        }
        path
    }
}

pub(crate) const PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY: &str = "write.parquet.row-group-size-bytes";
const MAX_WRITE_BATCH_ROWS: usize = 1024;
const ROW_GROUP_WRITE_BATCH_DIVISOR: usize = 64;

fn row_group_write_batch_rows(max_row_group_bytes: usize) -> usize {
    (max_row_group_bytes / ROW_GROUP_WRITE_BATCH_DIVISOR).clamp(1, MAX_WRITE_BATCH_ROWS)
}

pub(crate) fn parquet_row_group_size_bytes(
    table_properties: &HashMap<String, String>,
) -> Result<Option<usize>, String> {
    table_properties
        .get(PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY)
        .map(|value| {
            let max_row_group_bytes = value.parse::<usize>().map_err(|error| {
            format!(
                "Iceberg table property {PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY} must be a positive byte count: {error}"
            )
        })?;
            if max_row_group_bytes == 0 {
                return Err(format!(
                    "Iceberg table property {PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY} must be a positive byte count"
                ));
            }
            Ok(max_row_group_bytes)
        })
        .transpose()
}

fn parquet_writer_properties(
    table_properties: &HashMap<String, String>,
) -> Result<WriterProperties, String> {
    let mut properties = WriterProperties::builder();
    if let Some(max_row_group_bytes) = parquet_row_group_size_bytes(table_properties)? {
        // Parquet only evaluates the byte threshold between write batches.
        // Keep an explicitly small Iceberg row-group limit meaningful instead
        // of allowing the default 1024-row batch to overshoot it wholesale.
        properties = properties
            .set_max_row_group_bytes(Some(max_row_group_bytes))
            .set_write_batch_size(row_group_write_batch_rows(max_row_group_bytes));
    }
    Ok(properties.build())
}

pub struct StagedDataFile {
    pub data_file: DataFile,
    pub metadata: Arc<TableMetadata>,
    pub partition_spec_id: i32,
}

pub struct StagedDataFileWriter {
    ctx: StagedWriteContext,
    buffered: Vec<RecordBatch>,
}

impl StagedDataFileWriter {
    pub fn new(ctx: StagedWriteContext) -> Self {
        Self {
            ctx,
            buffered: Vec::new(),
        }
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        if batch.num_rows() > 0 {
            self.buffered.push(batch);
        }
        Ok(())
    }

    pub async fn finish(self) -> Result<Vec<StagedDataFile>, String> {
        if self.buffered.is_empty() {
            return Ok(Vec::new());
        }
        write_record_batches(&self.ctx, self.buffered).await
    }
}

pub struct StagedWriteContext {
    metadata: Arc<novarocks_connector_iceberg::iceberg::spec::TableMetadata>,
    file_io: novarocks_connector_iceberg::iceberg::io::FileIO,
    writer_schema: SchemaRef,
    annotated_schema: arrow::datatypes::SchemaRef,
    /// Target table partition spec id reported in sink commit metadata. The
    /// staged writer may use synthetic metadata for schema/partition binding,
    /// but the coordinator must commit files under the real target spec id.
    partition_spec_id: i32,
}

impl StagedWriteContext {
    pub fn from_table(
        table: &novarocks_connector_iceberg::iceberg::table::Table,
    ) -> Result<Self, String> {
        let writer_schema = table.metadata().current_schema().clone();
        let annotated_schema = Arc::new(
            schema_to_arrow_schema(&writer_schema)
                .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
        );
        Self::from_table_with_schema(table, writer_schema, annotated_schema)
    }

    fn from_table_with_schema(
        table: &novarocks_connector_iceberg::iceberg::table::Table,
        writer_schema: SchemaRef,
        annotated_schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, String> {
        Self::from_parts(
            table.metadata().clone(),
            table.file_io().clone(),
            writer_schema,
            annotated_schema,
        )
    }

    pub fn from_parts(
        metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata,
        file_io: novarocks_connector_iceberg::iceberg::io::FileIO,
        writer_schema: SchemaRef,
        annotated_schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, String> {
        let partition_spec_id = metadata.default_partition_spec_id();
        Self::from_parts_with_partition_spec_id(
            metadata,
            file_io,
            writer_schema,
            annotated_schema,
            partition_spec_id,
        )
    }

    pub fn from_parts_with_partition_spec_id(
        metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata,
        file_io: novarocks_connector_iceberg::iceberg::io::FileIO,
        writer_schema: SchemaRef,
        annotated_schema: arrow::datatypes::SchemaRef,
        partition_spec_id: i32,
    ) -> Result<Self, String> {
        let metadata = Arc::new(metadata);
        Ok(Self {
            metadata,
            file_io,
            writer_schema,
            annotated_schema,
            partition_spec_id,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.writer_schema
    }

    pub fn partition_spec(&self) -> &PartitionSpecRef {
        self.metadata.default_partition_spec()
    }

    pub fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub fn metadata(&self) -> &TableMetadata {
        self.metadata.as_ref()
    }

    pub fn file_io(&self) -> &novarocks_connector_iceberg::iceberg::io::FileIO {
        &self.file_io
    }

    fn data_file_writer_builder(
        &self,
        journal: Option<&StagedPathJournal>,
    ) -> Result<IcebergDataFileWriterBuilder, String> {
        let location_generator = DefaultLocationGenerator::new(self.metadata.as_ref())
            .map_err(|e| format!("build iceberg location generator failed: {e}"))?;
        let location_generator = JournaledLocationGenerator {
            original: location_generator,
            journal: journal.cloned(),
        };
        let file_name_generator = DefaultFileNameGenerator::new(
            "novarocks".to_string(),
            Some(unique_file_suffix()),
            DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            parquet_writer_properties(self.metadata.properties())?,
            self.writer_schema.clone(),
        );
        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            self.file_io.clone(),
            location_generator,
            file_name_generator,
        );
        Ok(DataFileWriterBuilder::new(rolling_builder))
    }
}

#[derive(Clone, Debug)]
pub struct RowLineageColumns {
    pub row_ids: arrow::array::Int64Array,
    pub last_updated_sequence_numbers: arrow::array::Int64Array,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RowLineageWriteBatch {
    pub user_batch: arrow::record_batch::RecordBatch,
    pub lineage: RowLineageColumns,
}

/// Streaming-shape facade over the staged data-file writer for the IVM-A1 MV
/// merge sink. It preserves the legacy `DataFile` surface while delegating
/// write semantics to the shared staged writer kernel.
pub struct IcebergStreamingDataFileWriter {
    writer: StagedDataFileWriter,
}

impl IcebergStreamingDataFileWriter {
    pub fn new(table: novarocks_connector_iceberg::iceberg::table::Table) -> Result<Self, String> {
        let ctx = StagedWriteContext::from_table(&table)?;
        let writer = StagedDataFileWriter::new(ctx);
        Ok(Self { writer })
    }

    pub async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        self.writer.write_batch(batch).await
    }

    pub async fn finish(self) -> Result<Vec<DataFile>, String> {
        let staged = self.writer.finish().await?;
        Ok(staged.into_iter().map(to_iceberg_data_file).collect())
    }
}

pub async fn write_record_batches_as_data_files(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<DataFile>, String> {
    let ctx = StagedWriteContext::from_table(table)?;
    let staged = write_record_batches(&ctx, batches).await?;
    Ok(staged.into_iter().map(to_iceberg_data_file).collect())
}

pub async fn write_record_batches(
    ctx: &StagedWriteContext,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<StagedDataFile>, String> {
    write_record_batches_with_journal(ctx, batches, None).await
}

/// Actual BE writer path: its stable original inventory precedes every SDK
/// output, and outlives errors, cancellation and fallible post-write metadata.
pub(crate) async fn write_record_batches_journaled(
    ctx: &StagedWriteContext,
    batches: impl IntoIterator<Item = RecordBatch>,
    journal: &StagedPathJournal,
) -> Result<Vec<StagedDataFile>, String> {
    write_record_batches_with_journal(ctx, batches, Some(journal)).await
}
async fn write_record_batches_with_journal(
    ctx: &StagedWriteContext,
    batches: impl IntoIterator<Item = RecordBatch>,
    journal: Option<&StagedPathJournal>,
) -> Result<Vec<StagedDataFile>, String> {
    let data_file_builder = ctx.data_file_writer_builder(journal)?;
    // Parquet checks its row-group byte threshold between RecordBatch writes.
    // The SQL sink can hand us a much larger batch, so preserve the configured
    // limit's usefulness by making the same bounded write granularity explicit.
    let write_batch_rows = parquet_row_group_size_bytes(ctx.metadata.properties())?
        .map(row_group_write_batch_rows)
        .unwrap_or(usize::MAX);

    if ctx.partition_spec().fields().is_empty() {
        let mut writer = data_file_builder
            .build(None)
            .await
            .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let annotated = adapt_batch(&batch, &ctx.annotated_schema)?;
            for offset in
                (0..annotated.num_rows()).step_by(write_batch_rows.min(annotated.num_rows()))
            {
                let length = (annotated.num_rows() - offset).min(write_batch_rows);
                writer
                    .write(annotated.slice(offset, length))
                    .await
                    .map_err(|e| format!("iceberg data file write failed: {e}"))?;
            }
        }
        let data_files = writer
            .close()
            .await
            .map_err(|e| format!("iceberg data file writer close failed: {e}"))?;
        return data_files
            .into_iter()
            .map(|data_file| {
                Ok(StagedDataFile {
                    data_file: retag_data_file_partition_spec_id(
                        data_file,
                        ctx.partition_spec_id(),
                    )?,
                    metadata: Arc::clone(&ctx.metadata),
                    partition_spec_id: ctx.partition_spec_id(),
                })
            })
            .collect();
    }

    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        ctx.schema().clone(),
        ctx.partition_spec().clone(),
    )
    .map_err(|e| format!("build iceberg partition splitter failed: {e}"))?;
    let mut staged_files = Vec::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let annotated = adapt_batch(&batch, &ctx.annotated_schema)?;
        let partitioned = splitter
            .split(&annotated)
            .map_err(|e| format!("split iceberg batch by partition spec failed: {e}"))?;
        for (partition_key, partition_batch) in partitioned {
            let mut writer = data_file_builder
                .build(Some(partition_key))
                .await
                .map_err(|e| format!("build iceberg partitioned data file writer failed: {e}"))?;
            for offset in (0..partition_batch.num_rows())
                .step_by(write_batch_rows.min(partition_batch.num_rows()))
            {
                let length = (partition_batch.num_rows() - offset).min(write_batch_rows);
                writer
                    .write(partition_batch.slice(offset, length))
                    .await
                    .map_err(|e| format!("iceberg partitioned data file write failed: {e}"))?;
            }
            let data_files = writer
                .close()
                .await
                .map_err(|e| format!("iceberg partitioned data file writer close failed: {e}"))?;
            for data_file in data_files {
                staged_files.push(StagedDataFile {
                    data_file: retag_data_file_partition_spec_id(
                        data_file,
                        ctx.partition_spec_id(),
                    )?,
                    metadata: Arc::clone(&ctx.metadata),
                    partition_spec_id: ctx.partition_spec_id(),
                });
            }
        }
    }
    Ok(staged_files)
}

pub async fn cleanup_staged_files(
    ctx: &StagedWriteContext,
    paths: &[String],
) -> Result<(), String> {
    for path in paths {
        ctx.file_io()
            .delete(path)
            .await
            .map_err(|e| format!("cleanup staged file {path} failed: {e}"))?;
    }
    Ok(())
}

pub fn to_iceberg_data_file(staged: StagedDataFile) -> DataFile {
    staged.data_file
}

pub fn staged_data_file_to_writer_report(
    staged: &StagedDataFile,
    partition: iceberg_report::IcebergPartitionReport,
    format: String,
    content: IcebergFileContent,
) -> Result<iceberg_report::IcebergWriterReport, String> {
    let df = &staged.data_file;
    let report = iceberg_report::IcebergWriterReport {
        file: iceberg_report::IcebergWrittenFileReport {
            path: df.file_path().to_string(),
            format,
            content,
            record_count: u64_to_i64(df.record_count(), "record_count")?,
            file_size_in_bytes: u64_to_i64(df.file_size_in_bytes(), "file_size_in_bytes")?,
            partition,
            split_offsets: df.split_offsets().map(|offsets| offsets.to_vec()),
            column_stats: iceberg_data_file_to_report_column_stats(df)?,
            referenced_data_file: df.referenced_data_file(),
            first_row_id: df.first_row_id(),
            equality_ids: df.equality_ids(),
            key_metadata: df.key_metadata().map(|k| k.to_vec()),
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        },
        is_overwrite: None,
        is_rewrite: None,
    };
    Ok(report)
}

fn iceberg_data_file_to_report_column_stats(
    df: &DataFile,
) -> Result<Option<iceberg_report::IcebergColumnStats>, String> {
    let column_sizes = u64_stats_to_i64(df.column_sizes(), "column_sizes")?;
    let value_counts = u64_stats_to_i64(df.value_counts(), "value_counts")?;
    let null_value_counts = u64_stats_to_i64(df.null_value_counts(), "null_value_counts")?;
    let nan_value_counts = u64_stats_to_i64(df.nan_value_counts(), "nan_value_counts")?;
    let lower_bounds = datum_bounds_to_bytes(df.lower_bounds(), "lower_bounds")?;
    let upper_bounds = datum_bounds_to_bytes(df.upper_bounds(), "upper_bounds")?;

    if column_sizes.is_empty()
        && value_counts.is_empty()
        && null_value_counts.is_empty()
        && nan_value_counts.is_empty()
        && lower_bounds.is_empty()
        && upper_bounds.is_empty()
    {
        return Ok(None);
    }

    Ok(Some(iceberg_report::IcebergColumnStats {
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts,
        lower_bounds,
        upper_bounds,
    }))
}

fn u64_to_i64(value: u64, field: &str) -> Result<i64, String> {
    i64::try_from(value)
        .map_err(|_| format!("iceberg data file {field} value {value} exceeds i64::MAX"))
}

fn u64_stats_to_i64(stats: &HashMap<i32, u64>, field: &str) -> Result<BTreeMap<i32, i64>, String> {
    stats
        .iter()
        .map(|(field_id, value)| {
            u64_to_i64(*value, &format!("{field}[{field_id}]")).map(|value| (*field_id, value))
        })
        .collect()
}

fn datum_bounds_to_bytes(
    bounds: &HashMap<i32, novarocks_connector_iceberg::iceberg::spec::Datum>,
    field: &str,
) -> Result<BTreeMap<i32, Vec<u8>>, String> {
    bounds
        .iter()
        .map(|(field_id, datum)| {
            datum
                .to_bytes()
                .map(|bytes| (*field_id, bytes.to_vec()))
                .map_err(|e| {
                    format!("convert iceberg datum bound {field}[{field_id}] to bytes failed: {e}")
                })
        })
        .collect()
}

async fn write_record_batches_as_data_files_with_schema(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
    writer_schema: SchemaRef,
    annotated_schema: arrow::datatypes::SchemaRef,
) -> Result<Vec<DataFile>, String> {
    let ctx = StagedWriteContext::from_table_with_schema(table, writer_schema, annotated_schema)?;
    let staged = write_record_batches(&ctx, batches).await?;
    Ok(staged.into_iter().map(to_iceberg_data_file).collect())
}

fn retag_data_file_partition_spec_id(
    data_file: DataFile,
    partition_spec_id: i32,
) -> Result<DataFile, String> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(data_file.content_type())
        .file_path(data_file.file_path().to_string())
        .file_format(data_file.file_format())
        .partition(data_file.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(data_file.record_count())
        .file_size_in_bytes(data_file.file_size_in_bytes());

    if !data_file.column_sizes().is_empty() {
        builder.column_sizes(data_file.column_sizes().clone());
    }
    if !data_file.value_counts().is_empty() {
        builder.value_counts(data_file.value_counts().clone());
    }
    if !data_file.null_value_counts().is_empty() {
        builder.null_value_counts(data_file.null_value_counts().clone());
    }
    if !data_file.nan_value_counts().is_empty() {
        builder.nan_value_counts(data_file.nan_value_counts().clone());
    }
    if !data_file.lower_bounds().is_empty() {
        builder.lower_bounds(data_file.lower_bounds().clone());
    }
    if !data_file.upper_bounds().is_empty() {
        builder.upper_bounds(data_file.upper_bounds().clone());
    }
    if let Some(key_metadata) = data_file.key_metadata() {
        builder.key_metadata(Some(key_metadata.to_vec()));
    }
    if let Some(split_offsets) = data_file.split_offsets() {
        builder.split_offsets(Some(split_offsets.to_vec()));
    }
    if let Some(equality_ids) = data_file.equality_ids() {
        builder.equality_ids(Some(equality_ids));
    }
    if let Some(sort_order_id) = data_file.sort_order_id() {
        builder.sort_order_id(sort_order_id);
    }
    builder
        .first_row_id(data_file.first_row_id())
        .referenced_data_file(data_file.referenced_data_file())
        .content_offset(data_file.content_offset())
        .content_size_in_bytes(data_file.content_size_in_bytes());

    builder.build().map_err(|e| {
        format!("failed to retag iceberg data file with partition spec id {partition_spec_id}: {e}")
    })
}

#[allow(dead_code)]
pub async fn write_row_lineage_batches_as_data_files(
    table: &novarocks_connector_iceberg::iceberg::table::Table,
    batches: &[RowLineageWriteBatch],
) -> Result<Vec<novarocks_connector_iceberg::iceberg::spec::DataFile>, String> {
    let writer_schema = build_row_lineage_writer_schema(table.metadata().current_schema())?;
    let annotated_schema = Arc::new(
        schema_to_arrow_schema(&writer_schema)
            .map_err(|e| format!("convert row-lineage iceberg schema to arrow failed: {e}"))?,
    );
    let runs = split_row_lineage_batches_into_manifest_runs(batches)?;
    let mut data_files = Vec::new();
    for run in runs {
        let enriched = append_row_lineage_columns(&run.user_batch, run.lineage)?;
        data_files.extend(
            write_record_batches_as_data_files_with_schema(
                table,
                [enriched],
                Arc::clone(&writer_schema),
                Arc::clone(&annotated_schema),
            )
            .await?,
        );
    }
    Ok(data_files)
}

fn split_row_lineage_batches_into_manifest_runs(
    batches: &[RowLineageWriteBatch],
) -> Result<Vec<RowLineageWriteBatch>, String> {
    let mut out = Vec::new();
    for batch in batches {
        let rows = batch.user_batch.num_rows();
        if rows == 0 {
            continue;
        }
        if batch.lineage.row_ids.len() != rows
            || batch.lineage.last_updated_sequence_numbers.len() != rows
        {
            return Err(format!(
                "row-lineage writer batch length mismatch: rows={}, row_ids={}, last_updated={}",
                rows,
                batch.lineage.row_ids.len(),
                batch.lineage.last_updated_sequence_numbers.len()
            ));
        }

        let mut start = 0;
        let mut prev_row_id = row_lineage_row_id_at(&batch.lineage.row_ids, 0)?;
        let mut prev_seq = row_lineage_sequence_at(&batch.lineage.last_updated_sequence_numbers, 0);
        for row in 1..rows {
            let row_id = row_lineage_row_id_at(&batch.lineage.row_ids, row)?;
            let seq = row_lineage_sequence_at(&batch.lineage.last_updated_sequence_numbers, row);
            if row_id != prev_row_id + 1 || seq != prev_seq {
                push_row_lineage_manifest_run(
                    &mut out,
                    slice_row_lineage_write_batch(batch, start, row - start),
                )?;
                start = row;
            }
            prev_row_id = row_id;
            prev_seq = seq;
        }
        push_row_lineage_manifest_run(
            &mut out,
            slice_row_lineage_write_batch(batch, start, rows - start),
        )?;
    }
    Ok(out)
}

fn push_row_lineage_manifest_run(
    out: &mut Vec<RowLineageWriteBatch>,
    run: RowLineageWriteBatch,
) -> Result<(), String> {
    let Some(last) = out.last_mut() else {
        out.push(run);
        return Ok(());
    };
    if !can_merge_row_lineage_manifest_runs(last, &run)? {
        out.push(run);
        return Ok(());
    }

    *last = merge_row_lineage_manifest_runs(last, &run)?;
    Ok(())
}

fn can_merge_row_lineage_manifest_runs(
    left: &RowLineageWriteBatch,
    right: &RowLineageWriteBatch,
) -> Result<bool, String> {
    let left_rows = left.user_batch.num_rows();
    let right_rows = right.user_batch.num_rows();
    if left_rows == 0 || right_rows == 0 {
        return Ok(left_rows == 0 || right_rows == 0);
    }
    if left.user_batch.schema().as_ref() != right.user_batch.schema().as_ref() {
        return Ok(false);
    }

    let left_last_row_id = row_lineage_row_id_at(&left.lineage.row_ids, left_rows - 1)?;
    let right_first_row_id = row_lineage_row_id_at(&right.lineage.row_ids, 0)?;
    let contiguous = left_last_row_id
        .checked_add(1)
        .is_some_and(|expected| expected == right_first_row_id);
    if !contiguous {
        return Ok(false);
    }

    let left_seq =
        row_lineage_sequence_at(&left.lineage.last_updated_sequence_numbers, left_rows - 1);
    let right_seq = row_lineage_sequence_at(&right.lineage.last_updated_sequence_numbers, 0);
    Ok(left_seq == right_seq)
}

fn merge_row_lineage_manifest_runs(
    left: &RowLineageWriteBatch,
    right: &RowLineageWriteBatch,
) -> Result<RowLineageWriteBatch, String> {
    use arrow::array::{Array, Int64Array};
    use arrow::compute::{concat, concat_batches};

    if left.user_batch.num_rows() == 0 {
        return Ok(right.clone());
    }
    if right.user_batch.num_rows() == 0 {
        return Ok(left.clone());
    }

    let schema = left.user_batch.schema();
    let user_batch = concat_batches(&schema, [&left.user_batch, &right.user_batch])
        .map_err(|e| format!("merge row-lineage user batches failed: {e}"))?;
    let row_ids = concat(&[
        &left.lineage.row_ids as &dyn Array,
        &right.lineage.row_ids as &dyn Array,
    ])
    .map_err(|e| format!("merge row-lineage row-id columns failed: {e}"))?
    .as_any()
    .downcast_ref::<Int64Array>()
    .ok_or_else(|| "merged row-lineage row-id column must be Int64".to_string())?
    .clone();
    let last_updated_sequence_numbers = concat(&[
        &left.lineage.last_updated_sequence_numbers as &dyn Array,
        &right.lineage.last_updated_sequence_numbers as &dyn Array,
    ])
    .map_err(|e| format!("merge row-lineage sequence columns failed: {e}"))?
    .as_any()
    .downcast_ref::<Int64Array>()
    .ok_or_else(|| "merged row-lineage sequence column must be Int64".to_string())?
    .clone();

    Ok(RowLineageWriteBatch {
        user_batch,
        lineage: RowLineageColumns {
            row_ids,
            last_updated_sequence_numbers,
        },
    })
}

fn row_lineage_row_id_at(row_ids: &arrow::array::Int64Array, row: usize) -> Result<i64, String> {
    if row_ids.is_null(row) {
        return Err(format!(
            "row-lineage {ICEBERG_ROW_ID_COL} column contains null at row {row}"
        ));
    }
    let row_id = row_ids.value(row);
    if row_id < 0 {
        return Err(format!(
            "row-lineage {ICEBERG_ROW_ID_COL} column must be non-negative: row={row}, value={row_id}"
        ));
    }
    Ok(row_id)
}

fn row_lineage_sequence_at(seqs: &arrow::array::Int64Array, row: usize) -> Option<i64> {
    if seqs.is_null(row) {
        None
    } else {
        Some(seqs.value(row))
    }
}

fn slice_row_lineage_write_batch(
    batch: &RowLineageWriteBatch,
    offset: usize,
    len: usize,
) -> RowLineageWriteBatch {
    RowLineageWriteBatch {
        user_batch: batch.user_batch.slice(offset, len),
        lineage: RowLineageColumns {
            row_ids: slice_i64_array(&batch.lineage.row_ids, offset, len),
            last_updated_sequence_numbers: slice_i64_array(
                &batch.lineage.last_updated_sequence_numbers,
                offset,
                len,
            ),
        },
    }
}

fn slice_i64_array(
    array: &arrow::array::Int64Array,
    offset: usize,
    len: usize,
) -> arrow::array::Int64Array {
    arrow::array::Int64Array::from(
        (offset..offset + len)
            .map(|row| {
                if array.is_null(row) {
                    None
                } else {
                    Some(array.value(row))
                }
            })
            .collect::<Vec<_>>(),
    )
}

fn build_row_lineage_writer_schema(current_schema: &SchemaRef) -> Result<SchemaRef, String> {
    Ok(Arc::new(
        current_schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(vec![
                NestedField::required(
                    ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                    ICEBERG_ROW_ID_COL,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
                NestedField::optional(
                    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                    ICEBERG_LAST_UPDATED_SEQ_COL,
                    Type::Primitive(PrimitiveType::Long),
                )
                .into(),
            ])
            .build()
            .map_err(|e| format!("build row-lineage iceberg schema failed: {e}"))?,
    ))
}

pub fn append_row_lineage_columns(
    batch: &arrow::record_batch::RecordBatch,
    lineage: RowLineageColumns,
) -> Result<arrow::record_batch::RecordBatch, String> {
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use std::collections::HashMap;
    use std::sync::Arc;

    if batch.num_rows() != lineage.row_ids.len()
        || batch.num_rows() != lineage.last_updated_sequence_numbers.len()
    {
        return Err(format!(
            "row-lineage column length mismatch: rows={}, row_ids={}, last_updated={}",
            batch.num_rows(),
            lineage.row_ids.len(),
            lineage.last_updated_sequence_numbers.len()
        ));
    }
    for row in 0..lineage.row_ids.len() {
        if lineage.row_ids.is_null(row) {
            return Err(format!(
                "row-lineage {ICEBERG_ROW_ID_COL} column contains null at row {row}"
            ));
        }
        let row_id = lineage.row_ids.value(row);
        if row_id < 0 {
            return Err(format!(
                "row-lineage {ICEBERG_ROW_ID_COL} column must be non-negative: row={row}, value={row_id}"
            ));
        }
    }

    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(
        Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
        )])),
    ));
    fields.push(Arc::new(
        Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true).with_metadata(
            HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER.to_string(),
            )]),
        ),
    ));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(lineage.row_ids) as ArrayRef);
    columns.push(Arc::new(lineage.last_updated_sequence_numbers) as ArrayRef);
    arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("build row-lineage record batch failed: {e}"))
}

fn unique_file_suffix() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use novarocks_connector_iceberg::iceberg::spec::{DataContentType, NestedField, Struct};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

    fn local_test_binding() -> novarocks_connector_iceberg::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        novarocks_connector_iceberg::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    #[test]
    fn parquet_writer_properties_honor_iceberg_row_group_byte_limit() {
        let properties = parquet_writer_properties(&HashMap::from([(
            PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_string(),
            "1024".to_string(),
        )]))
        .expect("valid row-group byte limit");

        assert_eq!(properties.max_row_group_bytes(), Some(1024));
        assert_eq!(properties.write_batch_size(), 16);
        assert_eq!(row_group_write_batch_rows(1024), 16);
    }

    #[test]
    fn parquet_writer_properties_reject_zero_or_malformed_row_group_limit() {
        for value in ["0", "not-a-number"] {
            let error = parquet_writer_properties(&HashMap::from([(
                PARQUET_ROW_GROUP_SIZE_BYTES_PROPERTY.to_string(),
                value.to_string(),
            )]))
            .expect_err("invalid row-group byte limit must fail");
            assert!(error.contains("positive byte count"));
        }
    }

    #[test]
    fn retag_unpartitioned_data_file_with_current_default_spec_id() {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path("file:///tmp/data.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(128);
        let data_file = builder.build().unwrap();

        let data_file = retag_data_file_partition_spec_id(data_file, 7).unwrap();

        assert!(
            format!("{data_file:?}").contains("partition_spec_id: 7"),
            "retagged data file should carry the evolved default partition spec id"
        );
    }

    #[tokio::test]
    async fn staged_write_context_from_table_exposes_schema_and_spec() {
        let table = build_unpartitioned_test_table("ctx_schema").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx from table");
        assert_eq!(
            ctx.schema().as_struct().fields().len(),
            table.metadata().current_schema().as_struct().fields().len(),
            "context schema must match table current schema"
        );
        assert_eq!(
            ctx.partition_spec_id(),
            table.metadata().default_partition_spec_id()
        );
        assert_eq!(
            ctx.partition_spec().fields().len(),
            table.metadata().default_partition_spec().fields().len(),
            "context partition spec must match table default partition spec"
        );
        let _ = ctx.file_io();
    }

    #[tokio::test]
    async fn staged_write_context_from_parts_uses_supplied_metadata_file_io_and_data_location() {
        let table = build_local_fs_test_table("ctx_from_parts", true).await;
        let writer_schema = table.metadata().current_schema().clone();
        let data_location = format!(
            "{}/custom-data",
            table.metadata().location().trim_end_matches('/')
        );
        let metadata = novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::new(
            writer_schema.as_ref().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
            novarocks_connector_iceberg::iceberg::spec::SortOrder::unsorted_order(),
            table.metadata().location().to_string(),
            novarocks_connector_iceberg::iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::from([(
                "write.data.path".to_string(),
                data_location.clone(),
            )]),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let annotated_schema = Arc::new(
            schema_to_arrow_schema(&writer_schema).expect("convert schema to arrow schema"),
        );

        let ctx = StagedWriteContext::from_parts(
            metadata,
            table.file_io().clone(),
            writer_schema,
            annotated_schema,
        )
        .expect("context from parts");

        assert_eq!(
            ctx.partition_spec_id(),
            table.metadata().default_partition_spec_id()
        );
        assert_eq!(
            ctx.partition_spec().fields().len(),
            table.metadata().default_partition_spec().fields().len()
        );

        let staged = write_record_batches(&ctx, vec![test_batch(&[4, 4])])
            .await
            .expect("write through context from parts");
        assert_eq!(staged.len(), 1);
        let path = staged[0].data_file.file_path().to_string();
        assert!(
            path.starts_with(&format!("{data_location}/id=4/")),
            "staged writer should honor supplied write.data.path, got {path}"
        );
        assert!(ctx.file_io().exists(&path).await.expect("exists"));
    }

    #[tokio::test]
    async fn write_record_batches_unpartitioned_produces_one_file() {
        let table = build_unpartitioned_test_table("kernel_unpart").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2]), test_batch(&[3])])
            .await
            .expect("write");
        assert_eq!(staged.len(), 1, "one file for unpartitioned batches");
        assert_eq!(staged[0].data_file.record_count(), 3);
        assert!(staged[0].data_file.file_size_in_bytes() > 0);
        let path = staged[0].data_file.file_path().to_string();
        assert!(
            ctx.file_io().exists(&path).await.expect("exists"),
            "staged file must exist"
        );
    }

    #[tokio::test]
    async fn cleanup_staged_files_removes_written_files() {
        let table = build_unpartitioned_test_table("kernel_cleanup").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 3])])
            .await
            .expect("write");
        let path = staged[0].data_file.file_path().to_string();
        assert!(ctx.file_io().exists(&path).await.expect("exists before"));
        cleanup_staged_files(&ctx, std::slice::from_ref(&path))
            .await
            .expect("cleanup");
        assert!(
            !ctx.file_io().exists(&path).await.expect("exists after"),
            "file removed"
        );
    }

    #[tokio::test]
    async fn to_iceberg_data_file_is_identity() {
        let table = build_unpartitioned_test_table("kernel_id").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let mut staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 3])])
            .await
            .expect("write");
        let one = staged.remove(0);
        let path = one.data_file.file_path().to_string();
        let count = one.data_file.record_count();
        let df = to_iceberg_data_file(one);
        assert_eq!(df.file_path(), path);
        assert_eq!(df.record_count(), count);
    }

    #[tokio::test]
    async fn staged_data_file_to_writer_report_maps_fields() {
        let table = build_unpartitioned_test_table("kernel_commit").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 2, 3])])
            .await
            .expect("write");
        let s = &staged[0];
        let expected_path = s.data_file.file_path().to_string();
        let expected_count =
            u64_to_i64(s.data_file.record_count(), "record_count").expect("record count");
        let expected_size =
            u64_to_i64(s.data_file.file_size_in_bytes(), "file_size_in_bytes").expect("file size");

        let report = staged_data_file_to_writer_report(
            s,
            iceberg_report::IcebergPartitionReport {
                partition_path: String::new(),
                null_fingerprint: String::new(),
                partition_spec_id: ctx.partition_spec_id(),
                partition_values: s.data_file.partition().clone(),
            },
            "parquet".to_string(),
            IcebergFileContent::Data,
        )
        .expect("writer report");

        assert_eq!(report.file.path, expected_path);
        assert_eq!(report.file.format, "parquet");
        assert_eq!(report.file.record_count, expected_count);
        assert_eq!(report.file.file_size_in_bytes, expected_size);
        assert_eq!(
            report.file.partition.partition_spec_id,
            ctx.partition_spec_id()
        );
        assert_eq!(report.file.content, IcebergFileContent::Data);
        let column_stats = report.file.column_stats.as_ref().expect("column stats");
        assert_eq!(
            column_stats.column_sizes,
            u64_stats_to_i64(s.data_file.column_sizes(), "column_sizes").expect("column sizes")
        );
        assert_eq!(
            column_stats.value_counts,
            u64_stats_to_i64(s.data_file.value_counts(), "value_counts").expect("value counts")
        );
        assert_eq!(
            column_stats.null_value_counts,
            u64_stats_to_i64(s.data_file.null_value_counts(), "null_value_counts")
                .expect("null value counts")
        );
        assert_eq!(
            column_stats.nan_value_counts,
            u64_stats_to_i64(s.data_file.nan_value_counts(), "nan_value_counts")
                .expect("nan value counts")
        );
        assert_eq!(
            column_stats.lower_bounds,
            datum_bounds_to_bytes(s.data_file.lower_bounds(), "lower_bounds")
                .expect("lower bounds")
        );
        assert_eq!(
            column_stats.upper_bounds,
            datum_bounds_to_bytes(s.data_file.upper_bounds(), "upper_bounds")
                .expect("upper bounds")
        );
    }

    #[test]
    fn iceberg_data_file_to_report_column_stats_preserves_nan_value_counts() {
        let mut builder = DataFileBuilder::default();
        builder
            .content(DataContentType::Data)
            .file_path("file:///tmp/nan.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(128)
            .nan_value_counts(HashMap::from([(1, 1)]));
        let df = builder.build().expect("data file");

        let stats = iceberg_data_file_to_report_column_stats(&df)
            .expect("report column stats")
            .expect("nan-only stats should be preserved");

        assert_eq!(stats.nan_value_counts.get(&1), Some(&1));
        assert!(stats.column_sizes.is_empty());
        assert!(stats.value_counts.is_empty());
        assert!(stats.null_value_counts.is_empty());
        assert!(stats.lower_bounds.is_empty());
        assert!(stats.upper_bounds.is_empty());
    }

    #[tokio::test]
    async fn streaming_writer_matches_batch_form() {
        let table = build_unpartitioned_test_table("kernel_stream").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let mut w = StagedDataFileWriter::new(ctx);
        w.write_batch(test_batch(&[1, 2])).await.expect("b1");
        w.write_batch(test_batch(&[3])).await.expect("b2");
        let staged = w.finish().await.expect("finish");
        let total: u64 = staged.iter().map(|s| s.data_file.record_count()).sum();
        assert_eq!(total, 3);
    }

    #[tokio::test]
    async fn legacy_streaming_writer_matches_table_batch_api() {
        let table = build_unpartitioned_test_table("legacy_stream").await;
        let batch_files = write_record_batches_as_data_files(
            &table,
            vec![test_batch(&[1, 2]), test_batch(&[]), test_batch(&[3])],
        )
        .await
        .expect("batch write");

        let mut writer =
            IcebergStreamingDataFileWriter::new(table.table.clone()).expect("legacy writer");
        writer
            .write_record_batch(test_batch(&[1, 2]))
            .await
            .expect("stream first batch");
        writer
            .write_record_batch(test_batch(&[]))
            .await
            .expect("stream empty batch");
        writer
            .write_record_batch(test_batch(&[3]))
            .await
            .expect("stream second batch");
        let stream_files = writer.finish().await.expect("stream finish");

        assert_eq!(stream_files.len(), batch_files.len());
        assert_eq!(
            stream_files
                .iter()
                .map(|data_file| data_file.record_count())
                .sum::<u64>(),
            batch_files
                .iter()
                .map(|data_file| data_file.record_count())
                .sum::<u64>()
        );
        assert!(
            stream_files
                .iter()
                .all(|data_file| data_file.file_size_in_bytes() > 0)
        );
    }

    #[tokio::test]
    async fn row_lineage_writer_uses_extended_schema_and_retains_default_spec_id() {
        use arrow::array::Int64Array;

        let table = build_local_fs_test_table_with_spec_id("row_lineage_part_spec", true, 7).await;
        let data_files = write_row_lineage_batches_as_data_files(
            &table,
            &[RowLineageWriteBatch {
                user_batch: test_batch(&[0, 0, 1]),
                lineage: RowLineageColumns {
                    row_ids: Int64Array::from(vec![10, 11, 12]),
                    last_updated_sequence_numbers: Int64Array::from(vec![
                        Some(3),
                        Some(3),
                        Some(3),
                    ]),
                },
            }],
        )
        .await
        .expect("row-lineage write");

        assert!(
            !data_files.is_empty(),
            "row-lineage write must produce at least one data file"
        );
        assert_eq!(
            data_files
                .iter()
                .map(|data_file| data_file.record_count())
                .sum::<u64>(),
            3
        );
        for data_file in data_files {
            assert!(
                format!("{data_file:?}").contains("partition_spec_id: 7"),
                "row-lineage files must retain the evolved default partition spec id"
            );
        }
    }

    #[tokio::test]
    async fn row_lineage_writer_merges_contiguous_runs_across_batches() {
        use arrow::array::Int64Array;

        let table = build_unpartitioned_test_table("row_lineage_cross_batch_merge").await;
        let data_files = write_row_lineage_batches_as_data_files(
            &table,
            &[
                RowLineageWriteBatch {
                    user_batch: test_batch(&[1, 2]),
                    lineage: RowLineageColumns {
                        row_ids: Int64Array::from(vec![10, 11]),
                        last_updated_sequence_numbers: Int64Array::from(vec![Some(3), Some(3)]),
                    },
                },
                RowLineageWriteBatch {
                    user_batch: test_batch(&[3]),
                    lineage: RowLineageColumns {
                        row_ids: Int64Array::from(vec![12]),
                        last_updated_sequence_numbers: Int64Array::from(vec![Some(3)]),
                    },
                },
            ],
        )
        .await
        .expect("row-lineage write");

        assert_eq!(
            data_files.len(),
            1,
            "contiguous rows with the same last-updated sequence should compact into one file"
        );
        assert_eq!(data_files[0].record_count(), 3);
    }

    #[tokio::test]
    async fn write_record_batches_partitioned_produces_file_per_partition() {
        let table = build_local_fs_test_table("kernel_part", true).await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let batch = test_batch(&[0, 0, 1, 1]);
        let staged = write_record_batches(&ctx, vec![batch])
            .await
            .expect("write");
        assert_eq!(staged.len(), 2, "one file per distinct partition value");
        let mut record_counts = staged
            .iter()
            .map(|s| s.data_file.record_count())
            .collect::<Vec<_>>();
        record_counts.sort_unstable();
        assert_eq!(record_counts, vec![2, 2]);
        for staged_file in &staged {
            let path = staged_file.data_file.file_path().to_string();
            assert!(
                ctx.file_io().exists(&path).await.expect("exists"),
                "staged partition file must exist"
            );
        }
    }

    pub(super) struct LocalFsTestTable {
        table: novarocks_connector_iceberg::iceberg::table::Table,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for LocalFsTestTable {
        type Target = novarocks_connector_iceberg::iceberg::table::Table;

        fn deref(&self) -> &Self::Target {
            &self.table
        }
    }

    async fn build_unpartitioned_test_table(name: &str) -> LocalFsTestTable {
        build_local_fs_test_table(name, false).await
    }

    pub(super) async fn build_local_fs_test_table(
        name: &str,
        partitioned: bool,
    ) -> LocalFsTestTable {
        build_local_fs_test_table_with_spec_id(name, partitioned, 0).await
    }

    async fn build_local_fs_test_table_with_spec_id(
        name: &str,
        partitioned: bool,
        partition_spec_id: i32,
    ) -> LocalFsTestTable {
        let safe_name: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let dir = tempfile::Builder::new()
            .prefix(&format!("novarocks-iceberg-data-writer-{safe_name}-"))
            .tempdir()
            .expect("create table dir");
        let location = format!("file://{}", dir.path().display());
        let file_io_location = location.clone();

        let schema = Arc::new(
            novarocks_connector_iceberg::iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata_builder = if partitioned && partition_spec_id > 0 {
            let mut builder =
                novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::new(
                    schema.as_ref().clone(),
                    novarocks_connector_iceberg::iceberg::spec::PartitionSpec::unpartition_spec(),
                    novarocks_connector_iceberg::iceberg::spec::SortOrder::unsorted_order(),
                    location,
                    novarocks_connector_iceberg::iceberg::spec::FormatVersion::V2,
                    std::collections::HashMap::new(),
                )
                .expect("builder");
            for idx in 1..partition_spec_id {
                let transform = if idx <= 3 {
                    novarocks_connector_iceberg::iceberg::spec::Transform::Bucket((idx + 1) as u32)
                } else {
                    novarocks_connector_iceberg::iceberg::spec::Transform::Truncate(
                        (idx - 2) as u32,
                    )
                };
                let spec =
                    novarocks_connector_iceberg::iceberg::spec::UnboundPartitionSpec::builder()
                        .add_partition_field(1, format!("id_evolved_{idx}"), transform)
                        .expect("add evolved partition field")
                        .build();
                builder = builder
                    .add_default_partition_spec(spec)
                    .expect("add evolved partition spec");
            }
            let identity_spec =
                novarocks_connector_iceberg::iceberg::spec::UnboundPartitionSpec::builder()
                    .add_partition_field(
                        1,
                        "id",
                        novarocks_connector_iceberg::iceberg::spec::Transform::Identity,
                    )
                    .expect("add identity partition field")
                    .build();
            builder
                .add_default_partition_spec(identity_spec)
                .expect("add identity partition spec")
        } else {
            let partition_spec = if partitioned {
                novarocks_connector_iceberg::iceberg::spec::PartitionSpec::builder(schema.clone())
                    .with_spec_id(partition_spec_id)
                    .add_partition_field(
                        "id",
                        "id",
                        novarocks_connector_iceberg::iceberg::spec::Transform::Identity,
                    )
                    .expect("add partition field")
                    .build()
                    .expect("partition spec")
            } else {
                novarocks_connector_iceberg::iceberg::spec::PartitionSpec::unpartition_spec()
            };
            novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::new(
                schema.as_ref().clone(),
                partition_spec,
                novarocks_connector_iceberg::iceberg::spec::SortOrder::unsorted_order(),
                location,
                novarocks_connector_iceberg::iceberg::spec::FormatVersion::V2,
                std::collections::HashMap::new(),
            )
            .expect("builder")
        };
        let metadata = metadata_builder.build().expect("metadata").metadata;

        let table = novarocks_connector_iceberg::iceberg::table::Table::builder()
            .identifier(
                novarocks_connector_iceberg::iceberg::TableIdent::from_strs(["db", name])
                    .expect("table ident"),
            )
            .file_io(
                novarocks_connector_iceberg::fs_io::build_file_io_for_location(
                    &file_io_location,
                    local_test_binding(),
                ),
            )
            .metadata(metadata)
            .runtime(crate::iceberg::Runtime::try_current().expect("test runtime"))
            .build()
            .expect("table");

        LocalFsTestTable { table, _dir: dir }
    }

    pub(super) fn test_batch(ids: &[i32]) -> arrow::record_batch::RecordBatch {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::collections::HashMap;

        let field = Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )]));
        let schema = Arc::new(Schema::new(vec![field]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(ids.to_vec()))],
        )
        .expect("test batch")
    }

    fn arrow_field_with_iceberg_id(
        name: &str,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
        field_id: i32,
    ) -> arrow::datatypes::Field {
        arrow::datatypes::Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    #[test]
    fn append_row_lineage_columns_sets_reserved_field_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let out = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10, 11]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect("append");
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.schema().field(1).name(), "_row_id");
        assert!(!out.schema().field(1).is_nullable());
        assert_eq!(
            out.schema()
                .field(1)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2147483540")
        );
        assert_eq!(
            out.schema().field(2).name(),
            "_last_updated_sequence_number"
        );
        assert!(out.schema().field(2).is_nullable());
        assert_eq!(
            out.schema()
                .field(2)
                .metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2147483539")
        );
    }

    #[test]
    fn append_row_lineage_columns_rejects_length_mismatch() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let err = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect_err("length mismatch");

        assert_eq!(
            err,
            "row-lineage column length mismatch: rows=2, row_ids=1, last_updated=2"
        );
    }

    #[test]
    fn append_row_lineage_columns_rejects_null_row_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let err = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![Some(10), None]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect_err("null row id");

        assert_eq!(err, "row-lineage _row_id column contains null at row 1");
    }

    #[test]
    fn append_row_lineage_columns_rejects_negative_row_ids() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let err = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10, -1]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect_err("negative row id");

        assert_eq!(
            err,
            "row-lineage _row_id column must be non-negative: row=1, value=-1"
        );
    }

    #[test]
    fn enriched_row_lineage_columns_reannotate_with_extended_schema() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use novarocks_connector_iceberg::iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = Arc::new(
            novarocks_connector_iceberg::iceberg::spec::Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    NestedField::optional(1, "v", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
        )
        .expect("batch");
        let enriched = append_row_lineage_columns(
            &batch,
            RowLineageColumns {
                row_ids: Int64Array::from(vec![10, 11]),
                last_updated_sequence_numbers: Int64Array::from(vec![None, Some(3)]),
            },
        )
        .expect("append");
        let extended_schema = build_row_lineage_writer_schema(&iceberg_schema).expect("schema");
        let annotated_schema = Arc::new(schema_to_arrow_schema(&extended_schema).expect("arrow"));
        let annotated = adapt_batch(&enriched, &annotated_schema).expect("annotate");

        assert_eq!(annotated.num_columns(), 3);
        assert_eq!(annotated.schema().field(1).name(), "_row_id");
        assert_eq!(
            annotated.schema().field(2).name(),
            "_last_updated_sequence_number"
        );
    }

    #[test]
    fn adaptation_rejects_missing_projected_columns() {
        use arrow::array::{Int32Array, Int64Array};
        use arrow::datatypes::{DataType, Schema};
        use std::sync::Arc;

        let source_schema = Arc::new(Schema::new(vec![
            arrow_field_with_iceberg_id("id", DataType::Int32, false, 1),
            arrow_field_with_iceberg_id("amount", DataType::Int64, true, 2),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![4])),
                Arc::new(Int64Array::from(vec![400_i64])),
            ],
        )
        .expect("source batch");
        let annotated_schema = Arc::new(Schema::new(vec![
            arrow_field_with_iceberg_id("id", DataType::Int32, false, 1),
            arrow_field_with_iceberg_id("amount", DataType::Int64, true, 2),
            arrow_field_with_iceberg_id("category", DataType::Utf8, true, 3),
        ]));

        let error = adapt_batch(&source, &annotated_schema).expect_err("missing projected column");
        assert!(error.contains("column count mismatch"), "{error}");
    }

    #[test]
    fn adaptation_rejects_unprojected_dropped_columns() {
        use arrow::array::{Int32Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Schema};
        use std::sync::Arc;

        let source_schema = Arc::new(Schema::new(vec![
            arrow_field_with_iceberg_id("id", DataType::Int32, false, 1),
            arrow_field_with_iceberg_id("region", DataType::Utf8, true, 2),
            arrow_field_with_iceberg_id("amount", DataType::Int64, true, 3),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![4])),
                Arc::new(StringArray::from(vec!["US"])),
                Arc::new(Int64Array::from(vec![400_i64])),
            ],
        )
        .expect("source batch");
        let annotated_schema = Arc::new(Schema::new(vec![
            arrow_field_with_iceberg_id("amount", DataType::Int64, true, 3),
            arrow_field_with_iceberg_id("id", DataType::Int32, false, 1),
        ]));

        let error =
            adapt_batch(&source, &annotated_schema).expect_err("unprojected dropped column");
        assert!(error.contains("column count mismatch"), "{error}");
    }

    #[test]
    fn annotate_batch_keeps_count_mismatch_error_without_iceberg_identity() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![4]))],
        )
        .expect("source batch");
        let annotated_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int64, true),
        ]));

        let err = adapt_batch(&source, &annotated_schema).expect_err("count mismatch");

        assert_eq!(
            err,
            "Iceberg write adaptation column count mismatch: input=1 target=2"
        );
    }

    #[test]
    fn annotate_batch_reannotates_map_column_with_field_ids() {
        use arrow::array::{ArrayRef, Int64Array, MapArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Fields, Schema};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::collections::HashMap;
        use std::sync::Arc;

        // Build a runtime MapArray with NO PARQUET:field_id metadata on its
        // inner key / value fields, mirroring execution-produced map arrays.
        let runtime_key_field = Arc::new(Field::new("key", DataType::Int64, false));
        let runtime_value_field = Arc::new(Field::new("value", DataType::Int64, true));
        let runtime_entries_struct_fields: Fields =
            vec![runtime_key_field.clone(), runtime_value_field.clone()].into();
        let runtime_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(runtime_entries_struct_fields.clone()),
            false,
        ));
        let keys = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
        let values = Arc::new(Int64Array::from(vec![Some(10_i64), Some(20), None])) as ArrayRef;
        let runtime_entries = StructArray::new(
            runtime_entries_struct_fields.clone(),
            vec![keys, values],
            None,
        );
        let offsets = OffsetBuffer::new(vec![0_i32, 2, 3].into());
        let runtime_map = MapArray::try_new(
            runtime_entries_field.clone(),
            offsets,
            runtime_entries,
            None,
            false,
        )
        .expect("runtime map");
        let runtime_schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(runtime_entries_field.clone(), false),
            false,
        )]));
        let runtime_batch =
            RecordBatch::try_new(runtime_schema, vec![Arc::new(runtime_map)]).expect("batch");

        // Build the annotated target schema where the inner key / value
        // Fields carry PARQUET:field_id metadata. This mirrors what
        // schema_to_arrow_schema produces for an Iceberg Map column.
        let id_meta = |id: &str| -> HashMap<String, String> {
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())])
        };
        let target_key_field =
            Arc::new(Field::new("key", DataType::Int64, false).with_metadata(id_meta("9")));
        let target_value_field =
            Arc::new(Field::new("value", DataType::Int64, true).with_metadata(id_meta("10")));
        let target_entries_struct_fields: Fields =
            vec![target_key_field, target_value_field].into();
        let target_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(target_entries_struct_fields.clone()),
            false,
        ));
        let target_map_field = Arc::new(
            Field::new(
                "m",
                DataType::Map(target_entries_field.clone(), false),
                false,
            )
            .with_metadata(id_meta("5")),
        );
        let annotated_schema = Arc::new(Schema::new(vec![target_map_field]));

        // The trivial path (RecordBatch::try_new with no rebuild) should fail
        // because the inner Struct fields differ in metadata. Our new
        // annotate_batch must succeed by deep-rebuilding the column.
        let trivial = RecordBatch::try_new(
            Arc::clone(&annotated_schema),
            runtime_batch.columns().to_vec(),
        );
        assert!(
            trivial.is_err(),
            "sanity check: strict RecordBatch::try_new should reject the runtime Map column"
        );

        let annotated =
            adapt_batch(&runtime_batch, &annotated_schema).expect("annotate map column");
        assert_eq!(annotated.num_columns(), 1);
        let out_map = annotated
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("MapArray");
        assert_eq!(out_map.len(), 2);
        // The output's data_type must exactly equal the target's; otherwise
        // try_new would not have returned Ok.
        assert_eq!(
            annotated.schema().field(0).data_type(),
            &DataType::Map(target_entries_field, false)
        );
        // Buffers preserved.
        let out_keys = out_map
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array keys");
        assert_eq!(out_keys.values(), &[1, 2, 3]);
        let out_values = out_map
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array values");
        assert_eq!(out_values.value(0), 10);
        assert_eq!(out_values.value(1), 20);
        assert!(out_values.is_null(2));
    }

    #[tokio::test]
    async fn sdk_writer_preserves_values_and_writes_target_timezone_and_nested_ids() {
        use crate::iceberg::spec::{
            FormatVersion, ListType, MapType, PartitionSpec, SortOrder, TableMetadataBuilder,
        };
        use arrow::array::{
            ArrayRef, BinaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
            TimestampMicrosecondArray,
        };
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::basic::LogicalType;

        let dir = tempfile::tempdir().unwrap();
        let location = format!("file://{}", dir.path().display());
        let schema = crate::iceberg::spec::Schema::builder()
            .with_schema_id(17)
            .with_fields(vec![
                NestedField::optional(1, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
                NestedField::optional(
                    2,
                    "l",
                    Type::List(ListType::new(
                        NestedField::optional(3, "element", Type::Primitive(PrimitiveType::Binary))
                            .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    4,
                    "m",
                    Type::Map(MapType::new(
                        NestedField::required(5, "key", Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::optional(6, "value", Type::Primitive(PrimitiveType::Int))
                            .into(),
                    )),
                )
                .into(),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            location.clone(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let table = crate::iceberg::table::Table::builder()
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "sdk_layout"]).unwrap())
            .file_io(crate::fs_io::build_file_io_for_location(
                &location,
                local_test_binding(),
            ))
            .metadata(metadata)
            .runtime(crate::iceberg::Runtime::try_current().unwrap())
            .build()
            .unwrap();
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Binary, true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(BinaryArray::from(vec![Some(&[0xff, 0][..]), None])),
            None,
        ));
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![Some(1), None])),
            ],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            entries,
            None,
            false,
        ));
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(-1234567), None]));
        let arrays = vec![ts, list, map];
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(
                arrays
                    .iter()
                    .zip(["ts", "l", "m"])
                    .map(|(array, name)| Field::new(name, array.data_type().clone(), true))
                    .collect::<Vec<_>>(),
            )),
            arrays,
        )
        .unwrap();
        let files = write_record_batches_as_data_files(&table, vec![batch])
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        let path = files[0]
            .file_path()
            .strip_prefix("file://")
            .unwrap_or(files[0].file_path());
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
        let columns = reader.parquet_schema().columns();
        assert!(matches!(
            columns[0].logical_type_ref(),
            Some(LogicalType::Timestamp(value)) if value.is_adjusted_to_u_t_c
        ));
        let paths = columns
            .iter()
            .map(|column| column.path().string())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "ts",
                "l.list.element",
                "m.key_value.key",
                "m.key_value.value"
            ]
        );
        // TableMetadataBuilder assigns fresh IDs: all top-level fields precede children.
        assert_eq!(
            columns
                .iter()
                .map(|column| column.self_type().get_basic_info().id())
                .collect::<Vec<_>>(),
            vec![1, 4, 5, 6]
        );
        let batches = reader
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let output = &batches[0];
        let ts = output
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), -1234567);
        assert!(ts.is_null(1));
        let list = output
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<arrow::array::LargeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), &[0xff, 0]);
        assert!(values.is_null(1));
        let map = output
            .column(2)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(
            map.keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
        assert!(map.values().is_null(1));
    }
}

#[cfg(test)]
#[path = "data_writer/journal_tests.rs"]
mod journal_tests;
