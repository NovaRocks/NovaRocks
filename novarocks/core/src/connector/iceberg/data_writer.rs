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

use crate::exec::row_position::{
    ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID, ICEBERG_ROW_ID_COL,
};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{
    DataFile, DataFileBuilder, DataFileFormat, NestedField, PartitionSpecRef, PrimitiveType,
    SchemaRef, TableMetadata, Type,
};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use parquet::file::properties::WriterProperties;

use super::delete_file::IcebergFileContent;
use super::theta_sketch::ThetaSketchHandle;
use super::variant_write::{
    VariantShreddingConfig, apply_variant_shredding_to_arrow_schema,
    parse_variant_shredding_properties, transform_variant_columns_for_write, variant_field_indices,
};

type IcebergDataFileWriterBuilder =
    DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StagedContent {
    Data,
    PositionDeletes,
}

#[derive(Clone, Debug)]
pub(crate) struct StagedWriteOptions {
    pub collect_theta_sketches: bool,
    pub content: StagedContent,
}

impl Default for StagedWriteOptions {
    fn default() -> Self {
        Self {
            collect_theta_sketches: false,
            content: StagedContent::Data,
        }
    }
}

pub(crate) struct StagedDataFile {
    pub data_file: DataFile,
    pub metadata: Arc<TableMetadata>,
    pub partition_spec_id: i32,
    pub theta_sketches: Option<HashMap<i32, ThetaSketchHandle>>,
}

pub(crate) struct StagedDataFileWriter {
    ctx: StagedWriteContext,
    opts: StagedWriteOptions,
    buffered: Vec<RecordBatch>,
}

impl StagedDataFileWriter {
    pub(crate) fn new(ctx: StagedWriteContext, opts: StagedWriteOptions) -> Result<Self, String> {
        ensure_data_file_staged_content(opts.content)?;
        Ok(Self {
            ctx,
            opts,
            buffered: Vec::new(),
        })
    }

    pub(crate) async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        if batch.num_rows() > 0 {
            self.buffered.push(batch);
        }
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<Vec<StagedDataFile>, String> {
        if self.buffered.is_empty() {
            return Ok(Vec::new());
        }
        write_record_batches(&self.ctx, self.buffered, &self.opts).await
    }
}

pub(crate) struct StagedWriteContext {
    metadata: Arc<iceberg::spec::TableMetadata>,
    file_io: iceberg::io::FileIO,
    writer_schema: SchemaRef,
    annotated_schema: arrow::datatypes::SchemaRef,
    variant_input_schema: arrow::datatypes::SchemaRef,
    variant_shredding: VariantShreddingConfig,
    /// Target table partition spec id reported in sink commit metadata. The
    /// staged writer may use synthetic metadata for schema/partition binding,
    /// but the coordinator must commit files under the real target spec id.
    partition_spec_id: i32,
}

impl StagedWriteContext {
    pub(crate) fn from_table(table: &iceberg::table::Table) -> Result<Self, String> {
        let writer_schema = table.metadata().current_schema().clone();
        let annotated_schema = Arc::new(
            schema_to_arrow_schema(&writer_schema)
                .map_err(|e| format!("convert iceberg schema to arrow failed: {e}"))?,
        );
        Self::from_table_with_schema(table, writer_schema, annotated_schema)
    }

    fn from_table_with_schema(
        table: &iceberg::table::Table,
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

    pub(crate) fn from_parts(
        metadata: iceberg::spec::TableMetadata,
        file_io: iceberg::io::FileIO,
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

    pub(crate) fn from_parts_with_partition_spec_id(
        metadata: iceberg::spec::TableMetadata,
        file_io: iceberg::io::FileIO,
        writer_schema: SchemaRef,
        annotated_schema: arrow::datatypes::SchemaRef,
        partition_spec_id: i32,
    ) -> Result<Self, String> {
        let metadata = Arc::new(metadata);
        let variant_shredding =
            parse_variant_shredding_properties(metadata.properties(), &writer_schema)?;
        let writer_arrow_schema =
            apply_variant_shredding_to_arrow_schema(&annotated_schema, &variant_shredding)?;
        Ok(Self {
            metadata,
            file_io,
            writer_schema,
            annotated_schema: writer_arrow_schema,
            variant_input_schema: annotated_schema,
            variant_shredding,
            partition_spec_id,
        })
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.writer_schema
    }

    pub(crate) fn partition_spec(&self) -> &PartitionSpecRef {
        self.metadata.default_partition_spec()
    }

    pub(crate) fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }

    pub(crate) fn file_io(&self) -> &iceberg::io::FileIO {
        &self.file_io
    }

    pub(crate) fn data_file_writer_builder(&self) -> Result<IcebergDataFileWriterBuilder, String> {
        let location_generator = DefaultLocationGenerator::new(self.metadata.as_ref().clone())
            .map_err(|e| format!("build iceberg location generator failed: {e}"))?;
        let file_name_generator = DefaultFileNameGenerator::new(
            "novarocks".to_string(),
            Some(unique_file_suffix()),
            DataFileFormat::Parquet,
        );
        let parquet_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), self.writer_schema.clone())
                .with_arrow_schema_override(Arc::clone(&self.annotated_schema));
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
pub(crate) struct RowLineageColumns {
    pub row_ids: arrow::array::Int64Array,
    pub last_updated_sequence_numbers: arrow::array::Int64Array,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct RowLineageWriteBatch {
    pub user_batch: arrow::record_batch::RecordBatch,
    pub lineage: RowLineageColumns,
}

/// Streaming-shape facade over the staged data-file writer for the IVM-A1 MV
/// merge sink. It preserves the legacy `DataFile` surface while delegating
/// write semantics to the shared staged writer kernel.
pub(crate) struct IcebergStreamingDataFileWriter {
    writer: StagedDataFileWriter,
}

impl IcebergStreamingDataFileWriter {
    pub(crate) fn new(table: iceberg::table::Table) -> Result<Self, String> {
        let ctx = StagedWriteContext::from_table(&table)?;
        let writer = StagedDataFileWriter::new(ctx, StagedWriteOptions::default())?;
        Ok(Self { writer })
    }

    pub(crate) async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        self.writer.write_batch(batch).await
    }

    pub(crate) async fn finish(self) -> Result<Vec<DataFile>, String> {
        let staged = self.writer.finish().await?;
        Ok(staged.into_iter().map(to_iceberg_data_file).collect())
    }
}

pub(crate) async fn write_record_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<DataFile>, String> {
    let ctx = StagedWriteContext::from_table(table)?;
    let staged = write_record_batches(&ctx, batches, &StagedWriteOptions::default()).await?;
    Ok(staged.into_iter().map(to_iceberg_data_file).collect())
}

pub(crate) async fn write_record_batches(
    ctx: &StagedWriteContext,
    batches: impl IntoIterator<Item = RecordBatch>,
    opts: &StagedWriteOptions,
) -> Result<Vec<StagedDataFile>, String> {
    ensure_data_file_staged_content(opts.content)?;
    let data_file_builder = ctx.data_file_writer_builder()?;
    let variant_indices = variant_field_indices(ctx.schema());

    if ctx.partition_spec().fields().is_empty() {
        let mut writer = data_file_builder
            .build(None)
            .await
            .map_err(|e| format!("build iceberg data file writer failed: {e}"))?;
        let mut batch_sketches = Vec::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let staged = if variant_indices.is_empty() {
                batch
            } else {
                transform_variant_columns_for_write(
                    &batch,
                    &ctx.variant_input_schema,
                    &variant_indices,
                    &ctx.variant_shredding,
                )?
            };
            let annotated = annotate_batch(&staged, &ctx.annotated_schema)?;
            if let Some(sketches) = maybe_collect_sketches(opts, &annotated)? {
                batch_sketches.push(sketches);
            }
            writer
                .write(annotated)
                .await
                .map_err(|e| format!("iceberg data file write failed: {e}"))?;
        }
        let data_files = writer
            .close()
            .await
            .map_err(|e| format!("iceberg data file writer close failed: {e}"))?;
        let combined_sketches = if batch_sketches.is_empty() {
            None
        } else {
            Some(merge_theta_sketches(batch_sketches))
        };
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
                    theta_sketches: combined_sketches.as_ref().map(clone_theta_sketches),
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
        let staged = if variant_indices.is_empty() {
            batch
        } else {
            transform_variant_columns_for_write(
                &batch,
                &ctx.variant_input_schema,
                &variant_indices,
                &ctx.variant_shredding,
            )?
        };
        let annotated = annotate_batch(&staged, &ctx.annotated_schema)?;
        let partitioned = splitter
            .split(&annotated)
            .map_err(|e| format!("split iceberg batch by partition spec failed: {e}"))?;
        for (partition_key, partition_batch) in partitioned {
            let theta_sketches = maybe_collect_sketches(opts, &partition_batch)?;
            let mut writer = data_file_builder
                .build(Some(partition_key))
                .await
                .map_err(|e| format!("build iceberg partitioned data file writer failed: {e}"))?;
            writer
                .write(partition_batch)
                .await
                .map_err(|e| format!("iceberg partitioned data file write failed: {e}"))?;
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
                    theta_sketches: theta_sketches.as_ref().map(clone_theta_sketches),
                });
            }
        }
    }
    Ok(staged_files)
}

fn ensure_data_file_staged_content(content: StagedContent) -> Result<(), String> {
    match content {
        StagedContent::Data => Ok(()),
        unsupported => Err(format!(
            "unsupported staged content {unsupported:?} for staged data-file writer kernel; this writer only produces DATA files"
        )),
    }
}

pub(crate) async fn cleanup_staged_files(
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

pub(crate) fn to_iceberg_data_file(staged: StagedDataFile) -> DataFile {
    staged.data_file
}

pub(crate) fn staged_data_file_to_writer_report(
    staged: &StagedDataFile,
    partition: crate::connector::iceberg::report::IcebergPartitionReport,
    format: String,
    content: IcebergFileContent,
) -> Result<
    (
        crate::connector::iceberg::report::IcebergWriterReport,
        Option<super::stats_assembler::FileSketchSet>,
    ),
    String,
> {
    let df = &staged.data_file;
    let report = crate::connector::iceberg::report::IcebergWriterReport {
        file: crate::connector::iceberg::report::IcebergWrittenFileReport {
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
    let sketch_set =
        staged
            .theta_sketches
            .as_ref()
            .map(|sketches| super::stats_assembler::FileSketchSet {
                file_path: df.file_path().to_string(),
                sketches: clone_theta_sketches(sketches),
            });
    Ok((report, sketch_set))
}

fn iceberg_data_file_to_report_column_stats(
    df: &DataFile,
) -> Result<Option<crate::connector::iceberg::report::IcebergColumnStats>, String> {
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

    Ok(Some(
        crate::connector::iceberg::report::IcebergColumnStats {
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts,
            lower_bounds,
            upper_bounds,
        },
    ))
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
    bounds: &HashMap<i32, iceberg::spec::Datum>,
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

fn maybe_collect_sketches(
    opts: &StagedWriteOptions,
    batch: &RecordBatch,
) -> Result<Option<HashMap<i32, ThetaSketchHandle>>, String> {
    if !opts.collect_theta_sketches {
        return Ok(None);
    }
    Ok(super::sink::compute_theta_sketches_for_batch(batch))
}

fn merge_theta_sketches(
    sketches: Vec<HashMap<i32, ThetaSketchHandle>>,
) -> HashMap<i32, ThetaSketchHandle> {
    let mut by_field = HashMap::<i32, Vec<ThetaSketchHandle>>::new();
    for batch_sketches in sketches {
        for (field_id, sketch) in batch_sketches {
            by_field.entry(field_id).or_default().push(sketch);
        }
    }
    by_field
        .into_iter()
        .map(|(field_id, field_sketches)| {
            let refs = field_sketches.iter().collect::<Vec<_>>();
            (field_id, ThetaSketchHandle::union(&refs))
        })
        .collect()
}

fn clone_theta_sketches(
    sketches: &HashMap<i32, ThetaSketchHandle>,
) -> HashMap<i32, ThetaSketchHandle> {
    sketches
        .iter()
        .map(|(field_id, sketch)| (*field_id, ThetaSketchHandle::union(&[sketch])))
        .collect()
}

async fn write_record_batches_as_data_files_with_schema(
    table: &iceberg::table::Table,
    batches: impl IntoIterator<Item = RecordBatch>,
    writer_schema: SchemaRef,
    annotated_schema: arrow::datatypes::SchemaRef,
) -> Result<Vec<DataFile>, String> {
    let ctx = StagedWriteContext::from_table_with_schema(table, writer_schema, annotated_schema)?;
    let staged = write_record_batches(&ctx, batches, &StagedWriteOptions::default()).await?;
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
pub(crate) async fn write_row_lineage_batches_as_data_files(
    table: &iceberg::table::Table,
    batches: &[RowLineageWriteBatch],
) -> Result<Vec<iceberg::spec::DataFile>, String> {
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

pub(crate) fn append_row_lineage_columns(
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

fn annotate_batch(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, String> {
    use arrow::array::ArrayRef;

    if batch_has_iceberg_field_ids(batch)
        || batch_has_reordered_target_names(batch, annotated_schema)
    {
        return annotate_batch_by_identity(batch, annotated_schema);
    }

    if batch.num_columns() != annotated_schema.fields().len() {
        return Err(format!(
            "annotate_batch column count mismatch: batch={} schema={}",
            batch.num_columns(),
            annotated_schema.fields().len()
        ));
    }
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (idx, (col, target_field)) in batch
        .columns()
        .iter()
        .zip(annotated_schema.fields().iter())
        .enumerate()
    {
        let new_col = reannotate_array(col, target_field.data_type())
            .map_err(|e| format!("annotate_batch column {idx} ({}): {e}", target_field.name()))?;
        new_columns.push(new_col);
    }
    RecordBatch::try_new(Arc::clone(annotated_schema), new_columns)
        .map_err(|e| format!("re-annotate batch with iceberg field ids failed: {e}"))
}

fn batch_has_iceberg_field_ids(batch: &RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .any(|field| iceberg_field_id(field).is_some())
}

fn iceberg_field_id(field: &arrow::datatypes::Field) -> Option<&str> {
    field
        .metadata()
        .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
        .map(String::as_str)
}

fn batch_has_reordered_target_names(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
) -> bool {
    if batch.num_columns() != annotated_schema.fields().len() {
        return false;
    }

    let batch_fields = batch.schema();
    let batch_fields = batch_fields.fields();
    let target_fields = annotated_schema.fields();
    let names_match_positionally = batch_fields
        .iter()
        .zip(target_fields.iter())
        .all(|(source, target)| source.name().eq_ignore_ascii_case(target.name()));
    if names_match_positionally {
        return false;
    }

    let mut source_names = batch_fields
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut target_names = target_fields
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect::<Vec<_>>();
    source_names.sort();
    target_names.sort();
    source_names == target_names
}

fn annotate_batch_by_identity(
    batch: &RecordBatch,
    annotated_schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, String> {
    use arrow::array::ArrayRef;

    let source_schema = batch.schema();
    let mut source_by_field_id: HashMap<String, usize> = HashMap::new();
    let mut source_by_name: HashMap<String, usize> = HashMap::new();
    for (idx, field) in source_schema.fields().iter().enumerate() {
        if let Some(field_id) = iceberg_field_id(field) {
            if source_by_field_id
                .insert(field_id.to_string(), idx)
                .is_some()
            {
                return Err(format!(
                    "annotate_batch duplicate iceberg field id {field_id} in source batch"
                ));
            }
        }
        let name_key = field.name().to_ascii_lowercase();
        if source_by_name.insert(name_key.clone(), idx).is_some() {
            return Err(format!(
                "annotate_batch duplicate source column name `{}`",
                field.name()
            ));
        }
    }

    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(annotated_schema.fields().len());
    for (target_idx, target_field) in annotated_schema.fields().iter().enumerate() {
        let source_idx = iceberg_field_id(target_field)
            .and_then(|field_id| source_by_field_id.get(field_id).copied())
            .or_else(|| {
                source_by_name
                    .get(&target_field.name().to_ascii_lowercase())
                    .copied()
            });

        let new_col = if let Some(source_idx) = source_idx {
            let col = batch.column(source_idx);
            reannotate_array(col, target_field.data_type()).map_err(|e| {
                format!(
                    "annotate_batch column {target_idx} ({}): {e}",
                    target_field.name()
                )
            })?
        } else {
            if !target_field.is_nullable() {
                return Err(format!(
                    "annotate_batch missing required target column `{}`",
                    target_field.name()
                ));
            }
            arrow::array::new_null_array(target_field.data_type(), batch.num_rows())
        };
        new_columns.push(new_col);
    }

    RecordBatch::try_new(Arc::clone(annotated_schema), new_columns)
        .map_err(|e| format!("re-annotate batch with iceberg field ids failed: {e}"))
}

/// Whether `dtype` is a nested / composite Arrow type whose inner `Field`
/// definitions are part of the `DataType` itself (so structural identity is
/// load-bearing). `reannotate_array` rebuilds the supported nested types
/// (Struct / List / Map) through dedicated arms and must NEVER hand a nested
/// type to the general scalar cast: a nested-vs-scalar pair (e.g. List -> Int)
/// or an unsupported nested-vs-nested pair (e.g. List -> Struct) must fail
/// fast via the catch-all. This guard gates the general scalar coercion arm so
/// only genuine scalar <-> scalar pairs are delegated to the engine cast.
fn is_nested_dtype(dtype: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(
        dtype,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

/// Rebuild `array` so its `data_type()` matches `target_dtype` exactly,
/// preserving the underlying data buffers.
///
/// Arrow's `RecordBatch::try_new` performs a strict recursive `PartialEq`
/// on data types. For scalar / primitive columns the comparison succeeds
/// because the field-level metadata (e.g. `PARQUET:field_id`) lives on the
/// `Schema`'s top-level `Field`, not inside the `DataType`. But for
/// `DataType::Map(entries_field, _)`, `DataType::List(child_field)`, and
/// `DataType::Struct(fields)`, the inner `Field` definitions are part of
/// the `DataType` itself, so any difference in their metadata or
/// nullability fails the comparison.
///
/// The Iceberg sink produces an annotated schema whose inner fields carry
/// `PARQUET:field_id`; the runtime chunk does not. This helper deep-rebuilds
/// the array so its inner field layout matches the target, while keeping the
/// original buffers.
///
/// Beyond field-id reannotation, this is also the single place that reconciles
/// an INSERT's source Arrow type to the sink column's target type, matching the
/// native Iceberg write path. The arms are tried in order:
///   1. exact type equality -> passthrough;
///   2. supported nested rebuilds (Map / Struct / List -> same kind), recursing
///      into children;
///   3. Arrow `Null` source (bare NULL literal insert) -> all-null target array;
///   4. general scalar <-> scalar coercion via `cast_with_special_rules`,
///      GUARDED so neither side is a nested/composite type;
///   5. catch-all fail-fast `Err` for everything else (structural mismatches
///      such as List -> Int, scalar -> nested, or unsupported nested pairs),
///      per CLAUDE.md rule #2.
fn reannotate_array(
    array: &arrow::array::ArrayRef,
    target_dtype: &arrow::datatypes::DataType,
) -> Result<arrow::array::ArrayRef, String> {
    use arrow::array::{ArrayRef, ListArray, MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::DataType;

    if array.data_type() == target_dtype {
        return Ok(array.clone());
    }

    match (array.data_type(), target_dtype) {
        (DataType::Map(_, _), DataType::Map(target_entries_field, target_sorted)) => {
            let map = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                "reannotate_array: Map data_type but array is not MapArray".to_string()
            })?;
            let target_entries_struct_fields = match target_entries_field.data_type() {
                DataType::Struct(fields) => fields,
                other => {
                    return Err(format!(
                        "reannotate_array: target Map entries must be Struct, got {other:?}"
                    ));
                }
            };
            if target_entries_struct_fields.len() != 2 {
                return Err(format!(
                    "reannotate_array: target Map entries Struct must have 2 fields, got {}",
                    target_entries_struct_fields.len()
                ));
            }
            let target_key_field = &target_entries_struct_fields[0];
            let target_value_field = &target_entries_struct_fields[1];

            let keys_in: ArrayRef = Arc::new(map.keys().clone());
            let values_in: ArrayRef = Arc::new(map.values().clone());
            let new_keys = reannotate_array(&keys_in, target_key_field.data_type())?;
            let new_values = reannotate_array(&values_in, target_value_field.data_type())?;

            // The Iceberg MAP key field is `required` per the Iceberg spec, so
            // its Arrow representation is a non-nullable Struct field. A NULL map
            // key is not representable in an Iceberg table. Building a
            // StructArray with unmasked nulls on a non-nullable field panics
            // inside Arrow's `StructArray::new`; fail fast with a clear error on
            // user input instead (CLAUDE.md rule #2).
            if !target_key_field.is_nullable() && new_keys.null_count() > 0 {
                return Err(format!(
                    "Iceberg MAP keys must be non-null (column key field `{}`); cannot insert a NULL map key",
                    target_key_field.name()
                ));
            }
            let new_entries = StructArray::try_new(
                target_entries_struct_fields.clone(),
                vec![new_keys, new_values],
                map.entries().nulls().cloned(),
            )
            .map_err(|e| {
                format!("reannotate_array: rebuild Map entries StructArray failed: {e}")
            })?;
            let new_map = MapArray::try_new(
                target_entries_field.clone(),
                OffsetBuffer::new(map.value_offsets().to_vec().into()),
                new_entries,
                map.nulls().cloned(),
                *target_sorted,
            )
            .map_err(|e| format!("reannotate_array: rebuild MapArray failed: {e}"))?;
            Ok(Arc::new(new_map) as ArrayRef)
        }
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    "reannotate_array: Struct data_type but array is not StructArray".to_string()
                })?;
            if struct_arr.num_columns() != target_fields.len() {
                return Err(format!(
                    "reannotate_array: Struct child count mismatch: array={} target={}",
                    struct_arr.num_columns(),
                    target_fields.len()
                ));
            }
            let mut new_children: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());
            for (i, target_child_field) in target_fields.iter().enumerate() {
                let child = struct_arr.column(i).clone();
                let new_child = reannotate_array(&child, target_child_field.data_type())?;
                new_children.push(new_child);
            }
            let new_struct = StructArray::try_new(
                target_fields.clone(),
                new_children,
                struct_arr.nulls().cloned(),
            )
            .map_err(|e| format!("reannotate_array: rebuild StructArray failed: {e}"))?;
            Ok(Arc::new(new_struct) as ArrayRef)
        }
        (DataType::List(_), DataType::List(target_child_field)) => {
            let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                "reannotate_array: List data_type but array is not ListArray".to_string()
            })?;
            let values_in: ArrayRef = list.values().clone();
            let new_values = reannotate_array(&values_in, target_child_field.data_type())?;
            let new_list = ListArray::try_new(
                target_child_field.clone(),
                OffsetBuffer::new(list.value_offsets().to_vec().into()),
                new_values,
                list.nulls().cloned(),
            )
            .map_err(|e| format!("reannotate_array: rebuild ListArray failed: {e}"))?;
            Ok(Arc::new(new_list) as ArrayRef)
        }
        (a, b) if a == b => Ok(array.clone()),
        // Arrow `Null` source -> any target type. A `Null` array is produced
        // when an INSERT supplies a bare NULL literal (no type information):
        // every row is NULL. Build an all-null array of the target type
        // directly rather than routing through a cast kernel; this is always
        // valid for a nullable insert and avoids the kernel-specific holes
        // around `Null -> <T>`. Length is preserved.
        (DataType::Null, _) => Ok(arrow::array::new_null_array(target_dtype, array.len())),
        // General scalar <-> scalar coercion. INSERT-SELECT may feed a source
        // scalar column into a different-typed scalar sink column, e.g.
        //   * numeric <-> numeric (integer narrowing, integer -> Decimal128,
        //     Decimal128 -> Decimal128 narrowing with half-up rounding,
        //     integer -> float widening, float narrowing/float -> integer);
        //   * scalar -> STRING (numeric/boolean/temporal -> Utf8);
        //   * STRING -> scalar and temporal <-> string, etc.
        // The native Iceberg write path accepts these coercions. We delegate to
        // the same relaxed cast the
        // engine uses for `CAST(... AS <type>)`, which applies safe=true
        // semantics (out-of-range values become NULL, matching the DECIMAL
        // overflow convention) and identical textual formatting for ->STRING.
        //
        // This arm is GUARDED to scalar pairs only: if either side is a nested
        // / composite type (`is_nested_dtype`), we fall through to the
        // fail-fast catch-all below. Supported nested rebuilds (Struct/List/Map
        // -> same kind) are handled by the dedicated arms ABOVE; a nested ->
        // scalar pair (e.g. List -> Int), a scalar -> nested pair, or an
        // unsupported nested -> nested pair MUST NOT reach the cast and instead
        // errors out, preserving CLAUDE.md rule #2 (fail fast on structural
        // mismatches).
        (a, b) if !is_nested_dtype(a) && !is_nested_dtype(b) => {
            crate::exec::expr::cast_with_special_rules(array, target_dtype).map_err(|e| {
                format!(
                    "reannotate_array: coerce scalar {:?} to {:?} failed: {e}",
                    array.data_type(),
                    target_dtype
                )
            })
        }
        (a, b) => Err(format!(
            "reannotate_array: incompatible data types: array={a:?}, target={b:?}"
        )),
    }
}

fn unique_file_suffix() -> String {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut bytes = [0_u8; 16];
    rng.fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataContentType, NestedField, Struct};

    use super::*;

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
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            writer_schema.as_ref().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
            iceberg::spec::SortOrder::unsorted_order(),
            table.metadata().location().to_string(),
            iceberg::spec::FormatVersion::V2,
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

        let staged = write_record_batches(
            &ctx,
            vec![test_batch(&[4, 4])],
            &StagedWriteOptions::default(),
        )
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
    async fn write_record_batches_unpartitioned_produces_one_file_with_stats() {
        let table = build_unpartitioned_test_table("kernel_unpart").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let staged = write_record_batches(
            &ctx,
            vec![test_batch(&[1, 2]), test_batch(&[3])],
            &StagedWriteOptions::default(),
        )
        .await
        .expect("write");
        assert_eq!(staged.len(), 1, "one file for unpartitioned batches");
        assert_eq!(staged[0].data_file.record_count(), 3);
        assert!(staged[0].data_file.file_size_in_bytes() > 0);
        assert!(
            staged[0].theta_sketches.is_none(),
            "sketches off by default"
        );
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
        let staged = write_record_batches(
            &ctx,
            vec![test_batch(&[1, 2, 3])],
            &StagedWriteOptions::default(),
        )
        .await
        .expect("write");
        let path = staged[0].data_file.file_path().to_string();
        assert!(ctx.file_io().exists(&path).await.expect("exists before"));
        cleanup_staged_files(&ctx, &[path.clone()])
            .await
            .expect("cleanup");
        assert!(
            !ctx.file_io().exists(&path).await.expect("exists after"),
            "file removed"
        );
    }

    #[tokio::test]
    async fn theta_sketches_collected_only_when_requested() {
        let table = build_unpartitioned_test_table("kernel_sketch").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let opts = StagedWriteOptions {
            collect_theta_sketches: true,
            content: StagedContent::Data,
        };
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 2, 3])], &opts)
            .await
            .expect("write");
        assert_eq!(staged.len(), 1);
        let sketches = staged[0].theta_sketches.as_ref().expect("sketches present");
        assert!(
            sketches.contains_key(&1),
            "theta sketch for field id 1 (id column)"
        );

        let staged_off = write_record_batches(
            &ctx,
            vec![test_batch(&[1, 2])],
            &StagedWriteOptions::default(),
        )
        .await
        .expect("write off");
        assert!(staged_off[0].theta_sketches.is_none());
    }

    #[tokio::test]
    async fn staged_data_file_writer_rejects_position_delete_content() {
        let table = build_unpartitioned_test_table("kernel_position_delete_content").await;
        let opts = StagedWriteOptions {
            collect_theta_sketches: false,
            content: StagedContent::PositionDeletes,
        };

        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let write_err = match write_record_batches(&ctx, vec![test_batch(&[1])], &opts).await {
            Ok(_) => panic!("position delete content should be rejected by batch writer"),
            Err(err) => err,
        };
        assert!(
            write_err.contains("PositionDeletes"),
            "error should mention unsupported content, got: {write_err}"
        );

        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let new_err = match StagedDataFileWriter::new(ctx, opts) {
            Ok(_) => panic!("position delete content should be rejected by staged writer"),
            Err(err) => err,
        };
        assert!(
            new_err.contains("PositionDeletes"),
            "error should mention unsupported content, got: {new_err}"
        );
    }

    #[tokio::test]
    async fn to_iceberg_data_file_is_identity() {
        let table = build_unpartitioned_test_table("kernel_id").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let mut staged = write_record_batches(
            &ctx,
            vec![test_batch(&[1, 2, 3])],
            &StagedWriteOptions::default(),
        )
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
    async fn staged_data_file_to_writer_report_maps_fields_and_sketches() {
        let table = build_unpartitioned_test_table("kernel_commit").await;
        let ctx = StagedWriteContext::from_table(&table).expect("ctx");
        let opts = StagedWriteOptions {
            collect_theta_sketches: true,
            content: StagedContent::Data,
        };
        let staged = write_record_batches(&ctx, vec![test_batch(&[1, 2, 2, 3])], &opts)
            .await
            .expect("write");
        let s = &staged[0];
        let expected_path = s.data_file.file_path().to_string();
        let expected_count =
            u64_to_i64(s.data_file.record_count(), "record_count").expect("record count");
        let expected_size =
            u64_to_i64(s.data_file.file_size_in_bytes(), "file_size_in_bytes").expect("file size");

        let (report, sketch_set) = staged_data_file_to_writer_report(
            s,
            crate::connector::iceberg::report::IcebergPartitionReport {
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

        let sketch_set = sketch_set.expect("sketch set");
        assert_eq!(sketch_set.file_path, report.file.path);
        assert!(sketch_set.sketches.contains_key(&1));
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
        let mut w = StagedDataFileWriter::new(ctx, StagedWriteOptions::default()).expect("new");
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
        let staged = write_record_batches(&ctx, vec![batch], &StagedWriteOptions::default())
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
            assert!(
                staged_file.theta_sketches.is_none(),
                "sketches off by default"
            );
            let path = staged_file.data_file.file_path().to_string();
            assert!(
                ctx.file_io().exists(&path).await.expect("exists"),
                "staged partition file must exist"
            );
        }
    }

    struct LocalFsTestTable {
        table: iceberg::table::Table,
        _dir: tempfile::TempDir,
    }

    impl std::ops::Deref for LocalFsTestTable {
        type Target = iceberg::table::Table;

        fn deref(&self) -> &Self::Target {
            &self.table
        }
    }

    async fn build_unpartitioned_test_table(name: &str) -> LocalFsTestTable {
        build_local_fs_test_table(name, false).await
    }

    async fn build_local_fs_test_table(name: &str, partitioned: bool) -> LocalFsTestTable {
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
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata_builder = if partitioned && partition_spec_id > 0 {
            let mut builder = iceberg::spec::TableMetadataBuilder::new(
                schema.as_ref().clone(),
                iceberg::spec::PartitionSpec::unpartition_spec(),
                iceberg::spec::SortOrder::unsorted_order(),
                location,
                iceberg::spec::FormatVersion::V2,
                std::collections::HashMap::new(),
            )
            .expect("builder");
            for idx in 1..partition_spec_id {
                let transform = if idx <= 3 {
                    iceberg::spec::Transform::Bucket((idx + 1) as u32)
                } else {
                    iceberg::spec::Transform::Truncate((idx - 2) as u32)
                };
                let spec = iceberg::spec::UnboundPartitionSpec::builder()
                    .add_partition_field(1, format!("id_evolved_{idx}"), transform)
                    .expect("add evolved partition field")
                    .build();
                builder = builder
                    .add_default_partition_spec(spec)
                    .expect("add evolved partition spec");
            }
            let identity_spec = iceberg::spec::UnboundPartitionSpec::builder()
                .add_partition_field(1, "id", iceberg::spec::Transform::Identity)
                .expect("add identity partition field")
                .build();
            builder
                .add_default_partition_spec(identity_spec)
                .expect("add identity partition spec")
        } else {
            let partition_spec = if partitioned {
                iceberg::spec::PartitionSpec::builder(schema.clone())
                    .with_spec_id(partition_spec_id)
                    .add_partition_field("id", "id", iceberg::spec::Transform::Identity)
                    .expect("add partition field")
                    .build()
                    .expect("partition spec")
            } else {
                iceberg::spec::PartitionSpec::unpartition_spec()
            };
            iceberg::spec::TableMetadataBuilder::new(
                schema.as_ref().clone(),
                partition_spec,
                iceberg::spec::SortOrder::unsorted_order(),
                location,
                iceberg::spec::FormatVersion::V2,
                std::collections::HashMap::new(),
            )
            .expect("builder")
        };
        let metadata = metadata_builder.build().expect("metadata").metadata;

        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", name]).expect("table ident"))
            .file_io(
                crate::connector::iceberg::fs_io::build_file_io_for_location(
                    &file_io_location,
                    None,
                ),
            )
            .metadata(metadata)
            .build()
            .expect("table");

        LocalFsTestTable { table, _dir: dir }
    }

    fn test_batch(ids: &[i32]) -> arrow::record_batch::RecordBatch {
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
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use std::sync::Arc;

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
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
        let annotated = annotate_batch(&enriched, &annotated_schema).expect("annotate");

        assert_eq!(annotated.num_columns(), 3);
        assert_eq!(annotated.schema().field(1).name(), "_row_id");
        assert_eq!(
            annotated.schema().field(2).name(),
            "_last_updated_sequence_number"
        );
    }

    #[test]
    fn annotate_batch_aligns_by_iceberg_field_id_and_fills_nullable_added_columns() {
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

        let annotated = annotate_batch(&source, &annotated_schema).expect("annotate");

        assert_eq!(annotated.num_columns(), 3);
        assert_eq!(annotated.schema().field(2).name(), "category");
        assert_eq!(annotated.column(2).data_type(), &DataType::Utf8);
        assert!(annotated.column(2).is_null(0));
    }

    #[test]
    fn annotate_batch_aligns_by_iceberg_field_id_and_ignores_dropped_columns() {
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

        let annotated = annotate_batch(&source, &annotated_schema).expect("annotate");

        assert_eq!(annotated.num_columns(), 2);
        let amount = annotated
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount");
        let id = annotated
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id");
        assert_eq!(amount.value(0), 400);
        assert_eq!(id.value(0), 4);
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

        let err = annotate_batch(&source, &annotated_schema).expect_err("count mismatch");

        assert_eq!(
            err,
            "annotate_batch column count mismatch: batch=1 schema=2"
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
            annotate_batch(&runtime_batch, &annotated_schema).expect("annotate map column");
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
    async fn write_variant_column_round_trips_through_local_parquet() {
        use arrow::array::{Int32Array, LargeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;
        use std::sync::Arc;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let location = format!("file://{}", dir.path().display());

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            location.clone(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(crate::connector::iceberg::fs_io::build_file_io_for_location(&location, None))
            .metadata(metadata)
            .build()
            .expect("table");

        // Build a 1-row record batch where `v` holds a serialized variant
        // (short string "hello").
        let payload = {
            let metadata = vec![0x01u8, 0x00, 0x00];
            let mut value = Vec::new();
            let s = b"hello";
            value.push(((s.len() as u8) << 2) | 0b01);
            value.extend_from_slice(s);
            let total = (metadata.len() + value.len()) as u32;
            let mut out = Vec::new();
            out.extend_from_slice(&total.to_le_bytes());
            out.extend_from_slice(&metadata);
            out.extend_from_slice(&value);
            out
        };
        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([payload.as_slice()])),
            ],
        )
        .expect("batch");

        let data_files = write_record_batches_as_data_files(&table, vec![batch])
            .await
            .expect("write");
        assert_eq!(data_files.len(), 1);
        let path = data_files[0].file_path().to_string();
        let on_disk = path.strip_prefix("file://").unwrap_or(&path);

        // Re-open the parquet file with the standard parquet-rs reader and
        // assert the physical layout matches the spec.
        let f = File::open(on_disk).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).expect("builder");
        let parquet_schema = builder.parquet_schema();
        let v_node = parquet_schema
            .columns()
            .iter()
            .find(|c| c.path().string().starts_with("v"))
            .expect("v column");
        assert!(
            v_node.path().string() == "v.metadata" || v_node.path().string() == "v.value",
            "expected leaf path under v.*; got {}",
            v_node.path().string()
        );
        // Look at the parent group's logical type via the parquet schema descr.
        let root = builder.parquet_schema().root_schema();
        let v_field = root
            .get_fields()
            .iter()
            .find(|f| f.name() == "v")
            .expect("v");
        assert!(
            format!("{:?}", v_field.get_basic_info().logical_type_ref())
                .to_lowercase()
                .contains("variant"),
            "v parent group must carry LogicalType::Variant; got {:?}",
            v_field.get_basic_info().logical_type_ref()
        );
    }

    #[tokio::test]
    async fn write_variant_column_with_shredding_property_outputs_typed_value() {
        use arrow::array::{ArrayRef, BinaryArray, BinaryViewArray, Int32Array, LargeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::variant::json_to_variant;
        use std::collections::HashMap;
        use std::fs::File;
        use std::sync::Arc;
        use tempfile::tempdir;

        fn binary_value(array: &ArrayRef, row: usize) -> Vec<u8> {
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                return arr.value(row).to_vec();
            }
            if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                return arr.value(row).to_vec();
            }
            if let Some(arr) = array.as_any().downcast_ref::<BinaryViewArray>() {
                return arr.value(row).to_vec();
            }
            panic!("unexpected binary array type: {:?}", array.data_type());
        }

        fn engine_variant_payload_from_json(json: &str) -> Vec<u8> {
            let json_array: ArrayRef = Arc::new(arrow::array::StringArray::from(vec![json]));
            let variant = json_to_variant(&json_array).expect("json_to_variant");
            let inner = variant.into_inner();
            let metadata = binary_value(inner.column_by_name("metadata").unwrap(), 0);
            let value = binary_value(inner.column_by_name("value").unwrap(), 0);
            let total = (metadata.len() + value.len()) as u32;
            let mut out = Vec::with_capacity(4 + metadata.len() + value.len());
            out.extend_from_slice(&total.to_le_bytes());
            out.extend_from_slice(&metadata);
            out.extend_from_slice(&value);
            out
        }

        let dir = tempdir().expect("tempdir");
        let location = format!("file://{}", dir.path().display());

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            location.clone(),
            iceberg::spec::FormatVersion::V3,
            HashMap::from([(
                "write.parquet.variant-shredding.v".to_string(),
                "a bigint".to_string(),
            )]),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t_shred"]).unwrap())
            .file_io(crate::connector::iceberg::fs_io::build_file_io_for_location(&location, None))
            .metadata(metadata)
            .build()
            .expect("table");

        let payload = engine_variant_payload_from_json(r#"{"a": 42, "b": "x"}"#);
        let input_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([payload.as_slice()])),
            ],
        )
        .expect("batch");

        let data_files = write_record_batches_as_data_files(&table, vec![batch])
            .await
            .expect("write");
        assert_eq!(data_files.len(), 1);
        let path = data_files[0].file_path().to_string();
        let on_disk = path.strip_prefix("file://").unwrap_or(&path);

        let f = File::open(on_disk).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).expect("builder");
        let paths = builder
            .parquet_schema()
            .columns()
            .iter()
            .map(|c| c.path().string())
            .collect::<Vec<_>>();
        assert!(
            paths.iter().any(|p| p == "v.typed_value.a.typed_value"),
            "expected shredded typed_value leaf, got paths: {paths:?}"
        );
    }

    /// OQ-3.1: a NovaRocks-written Iceberg data file must carry per-column
    /// min/max bounds end-to-end through the standalone commit round-trip
    /// (`DataFile` → `WrittenFile` → committed `DataFile`), so range-predicate
    /// selectivity reflects the real value range instead of the 0.5 fallback.
    #[tokio::test]
    async fn standalone_commit_round_trip_preserves_column_bounds() {
        use arrow::array::Int32Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::spec::{Datum, NestedField, PrimitiveType, Type};
        use std::sync::Arc;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let location = format!("file://{}", dir.path().display());

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "k1", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            location.clone(),
            iceberg::spec::FormatVersion::V2,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(crate::connector::iceberg::fs_io::build_file_io_for_location(&location, None))
            .metadata(metadata)
            .build()
            .expect("table");

        let input_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "k1",
            DataType::Int32,
            false,
        )]));
        let values: Vec<i32> = (1..=1000).collect();
        let batch = RecordBatch::try_new(input_schema, vec![Arc::new(Int32Array::from(values))])
            .expect("batch");

        let data_files = write_record_batches_as_data_files(&table, vec![batch])
            .await
            .expect("write");
        assert_eq!(data_files.len(), 1);
        let df = &data_files[0];
        // The iceberg-rust ParquetWriter populates bounds from the parquet footer.
        assert_eq!(df.lower_bounds().get(&1), Some(&Datum::int(1)));
        assert_eq!(df.upper_bounds().get(&1), Some(&Datum::int(1000)));

        // Round-trip through the standalone commit path and assert the committed
        // DataFile still carries the bounds (the OQ-3.1 fix).
        let wf = crate::engine::iceberg_writer::data_file_to_written_file(df, 0).expect("wf");
        assert_eq!(wf.lower_bounds.get(&1), Some(&Datum::int(1)));
        assert_eq!(wf.upper_bounds.get(&1), Some(&Datum::int(1000)));

        let collector = crate::connector::iceberg::commit::IcebergCommitCollector::new(
            crate::connector::iceberg::commit::CommitOpKind::FastAppend,
            iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
            None,
            0,
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().clone(),
            "file:///tmp/staging".to_string(),
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(table.metadata().clone());
        let committed =
            crate::connector::iceberg::commit::written_file_to_iceberg_data_file(&wf, &collector)
                .expect("committed");
        assert_eq!(committed.lower_bounds().get(&1), Some(&Datum::int(1)));
        assert_eq!(committed.upper_bounds().get(&1), Some(&Datum::int(1000)));
    }

    /// reannotate_array must narrow Decimal128(src_p, src_s) → Decimal128(tgt_p, tgt_s)
    /// with half-up rounding rather than returning an error.
    #[test]
    fn reannotate_decimal128_narrows_scale_with_rounding() {
        use arrow::array::{ArrayRef, Decimal128Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        // Source: DECIMAL(13, 4) — values as returned by CAST(x AS DECIMAL(13,4)).
        let src = Arc::new(
            Decimal128Array::from(vec![
                Some(12344_i128),  // 1.2344 -> rounds DOWN to 1.23
                Some(12356_i128),  // 1.2356 -> rounds UP to 1.24
                Some(-23444_i128), // -2.3444 -> rounds DOWN (toward 0) to -2.34
                Some(-23456_i128), // -2.3456 -> rounds away from 0 to -2.35
                None,              // NULL preserves NULL
            ])
            .with_precision_and_scale(13, 4)
            .expect("src decimal"),
        ) as ArrayRef;

        let target_dtype = DataType::Decimal128(10, 2);
        let result = reannotate_array(&src, &target_dtype).expect("reannotate must succeed");

        let out = result
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");

        assert_eq!(out.precision(), 10);
        assert_eq!(out.scale(), 2);
        assert_eq!(out.len(), 5);

        // 1.2344 -> 1.23
        assert_eq!(out.value(0), 123_i128);
        // 1.2356 -> 1.24
        assert_eq!(out.value(1), 124_i128);
        // -2.3444 -> -2.34
        assert_eq!(out.value(2), -234_i128);
        // -2.3456 -> -2.35
        assert_eq!(out.value(3), -235_i128);
        // NULL
        assert!(out.is_null(4));
    }

    /// reannotate_array: same Decimal128 precision and scale is a no-op (fast path).
    #[test]
    fn reannotate_decimal128_same_type_is_passthrough() {
        use arrow::array::{ArrayRef, Decimal128Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src = Arc::new(
            Decimal128Array::from(vec![Some(100_i128), None])
                .with_precision_and_scale(10, 2)
                .expect("src"),
        ) as ArrayRef;

        let target_dtype = DataType::Decimal128(10, 2);
        let result = reannotate_array(&src, &target_dtype).expect("same-type must succeed");
        // The early equality check returns the original Arc.
        assert!(Arc::ptr_eq(&src, &result));
    }

    /// reannotate_array must narrow Int64 → Int32 losslessly when every value fits.
    #[test]
    fn reannotate_int64_narrows_to_int32_lossless() {
        use arrow::array::{ArrayRef, Int32Array, Int64Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1_i64),
            Some(20_i64),
            Some(99999_i64),
            None,
        ]));

        let result =
            reannotate_array(&src, &DataType::Int32).expect("lossless narrow must succeed");
        let out = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");

        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), 1_i32);
        assert_eq!(out.value(1), 20_i32);
        assert_eq!(out.value(2), 99999_i32);
        assert!(out.is_null(3));
    }

    /// reannotate_array must produce NULL (not error) when Int64 → Int32 overflows.
    #[test]
    fn reannotate_int64_to_int32_overflow_returns_null() {
        use arrow::array::{ArrayRef, Int32Array, Int64Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        // i32::MAX is 2_147_483_647; values beyond that should become NULL.
        let src: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(42_i64),
            Some(i64::from(i32::MAX) + 1), // overflows
            Some(-1_i64),
            None,
        ]));

        let result =
            reannotate_array(&src, &DataType::Int32).expect("overflow arm must not return Err");
        let out = result
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");

        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), 42_i32);
        assert!(out.is_null(1), "overflowing value must become NULL");
        assert_eq!(out.value(2), -1_i32);
        assert!(out.is_null(3));
    }

    /// reannotate_array must narrow Int32 → Int16 losslessly.
    #[test]
    fn reannotate_int32_narrows_to_int16() {
        use arrow::array::{ArrayRef, Int16Array, Int32Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(100_i32),
            Some(-200_i32),
            None,
            Some(i32::from(i16::MAX) + 1), // overflows i16
        ]));

        let result =
            reannotate_array(&src, &DataType::Int16).expect("Int32->Int16 must not return Err");
        let out = result
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("Int16Array");

        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), 100_i16);
        assert_eq!(out.value(1), -200_i16);
        assert!(out.is_null(2));
        assert!(out.is_null(3), "overflow must become NULL");
    }

    /// reannotate_array: same Int32 type is a passthrough (regression guard).
    #[test]
    fn reannotate_int32_same_type_is_passthrough() {
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int32Array::from(vec![Some(7_i32), None]));
        let result = reannotate_array(&src, &DataType::Int32).expect("same Int32 must succeed");
        assert!(Arc::ptr_eq(&src, &result));
    }

    /// reannotate_array must implicitly coerce an integer column to Utf8 when the
    /// iceberg sink column is STRING/VARCHAR, matching the native write path.
    #[test]
    fn reannotate_int64_coerces_to_utf8() {
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1_i64),
            Some(2_i64),
            Some(3_i64),
            None,
        ]));

        let result = reannotate_array(&src, &DataType::Utf8).expect("Int64->Utf8 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");

        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), "1");
        assert_eq!(out.value(1), "2");
        assert_eq!(out.value(2), "3");
        assert!(out.is_null(3));
    }

    /// reannotate_array must coerce a DECIMAL column to Utf8 using the same
    /// formatting the engine uses for CAST(... AS STRING) (value 150, scale 2 -> "1.50").
    #[test]
    fn reannotate_decimal128_coerces_to_utf8() {
        use arrow::array::{ArrayRef, Decimal128Array, StringArray};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(150_i128), Some(-25_i128), None])
                .with_precision_and_scale(10, 2)
                .expect("src decimal"),
        );

        let result =
            reannotate_array(&src, &DataType::Utf8).expect("Decimal128->Utf8 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");

        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), "1.50");
        assert_eq!(out.value(1), "-0.25");
        assert!(out.is_null(2));
    }

    /// reannotate_array must coerce a BOOLEAN column to Utf8 ("true"/"false").
    #[test]
    fn reannotate_boolean_coerces_to_utf8() {
        use arrow::array::{ArrayRef, BooleanArray, StringArray};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));

        let result = reannotate_array(&src, &DataType::Utf8).expect("Boolean->Utf8 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");

        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), "true");
        assert_eq!(out.value(1), "false");
        assert!(out.is_null(2));
    }

    /// reannotate_array must implicitly coerce an integer column to Decimal128
    /// when the iceberg sink column is DECIMAL, matching the native write path
    /// (e.g. INSERT of a BIGINT expression into a DECIMAL(18,0) column).
    #[test]
    fn reannotate_int64_coerces_to_decimal128_scale0() {
        use arrow::array::{ArrayRef, Decimal128Array, Int64Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1_i64),
            Some(2_i64),
            Some(3_i64),
            None,
        ]));

        let target_dtype = DataType::Decimal128(18, 0);
        let result = reannotate_array(&src, &target_dtype).expect("Int64->Decimal128 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");

        assert_eq!(out.precision(), 18);
        assert_eq!(out.scale(), 0);
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), 1_i128);
        assert_eq!(out.value(1), 2_i128);
        assert_eq!(out.value(2), 3_i128);
        assert!(out.is_null(3));
    }

    /// reannotate_array must scale integer values when coercing to a DECIMAL with
    /// non-zero scale (1 -> 1.00 under DECIMAL(10,2), i.e. raw value 100).
    #[test]
    fn reannotate_int64_coerces_to_decimal128_scaled() {
        use arrow::array::{ArrayRef, Decimal128Array, Int64Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), Some(25_i64), None]));

        let target_dtype = DataType::Decimal128(10, 2);
        let result =
            reannotate_array(&src, &target_dtype).expect("Int64->Decimal128 scaled must succeed");
        let out = result
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");

        assert_eq!(out.precision(), 10);
        assert_eq!(out.scale(), 2);
        assert_eq!(out.len(), 3);
        // 1 -> 1.00 (raw 100), 25 -> 25.00 (raw 2500).
        assert_eq!(out.value(0), 100_i128);
        assert_eq!(out.value(1), 2500_i128);
        assert!(out.is_null(2));
    }

    /// reannotate_array must implicitly widen an integer column to Float64 when
    /// the iceberg sink column is DOUBLE.
    #[test]
    fn reannotate_int32_coerces_to_float64() {
        use arrow::array::{ArrayRef, Float64Array, Int32Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1_i32),
            Some(-2_i32),
            Some(300_i32),
            None,
        ]));

        let result =
            reannotate_array(&src, &DataType::Float64).expect("Int32->Float64 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");

        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), 1.0_f64);
        assert_eq!(out.value(1), -2.0_f64);
        assert_eq!(out.value(2), 300.0_f64);
        assert!(out.is_null(3));
    }

    /// reannotate_array must narrow Float64 -> Float32 (clean numeric->numeric).
    #[test]
    fn reannotate_float64_narrows_to_float32() {
        use arrow::array::{ArrayRef, Float32Array, Float64Array};
        use arrow::datatypes::DataType;
        use std::sync::Arc;

        let src: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.5_f64),
            Some(-2.25_f64),
            None,
        ]));

        let result =
            reannotate_array(&src, &DataType::Float32).expect("Float64->Float32 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array");

        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), 1.5_f32);
        assert_eq!(out.value(1), -2.25_f32);
        assert!(out.is_null(2));
    }

    /// reannotate_array must keep failing fast for genuinely incompatible pairs
    /// (e.g. Struct -> Int32). The fix only adds the `-> Utf8` coercion arm; it
    /// must not broaden the catch-all into arbitrary scalar/composite casts.
    #[test]
    fn reannotate_struct_to_int32_still_errors() {
        use arrow::array::{ArrayRef, Int64Array, StructArray};
        use arrow::datatypes::{DataType, Field, Fields};
        use std::sync::Arc;

        let child = Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef;
        let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int64, true))].into();
        let src: ArrayRef = Arc::new(StructArray::new(fields, vec![child], None));

        let err = reannotate_array(&src, &DataType::Int32)
            .expect_err("Struct -> Int32 must remain a fail-fast error");
        assert!(
            err.contains("incompatible data types"),
            "expected the fail-fast catch-all error, got: {err}"
        );
    }

    /// reannotate_array must coerce an Arrow `Null` source array (all nulls,
    /// e.g. an INSERT of a bare NULL literal) into an all-null array of the
    /// sink column's BOOLEAN type. The native write path accepts NULL inserts
    /// into any nullable column, so the iceberg path must too.
    #[test]
    fn reannotate_null_array_coerces_to_boolean() {
        use arrow::array::{ArrayRef, BooleanArray};
        use arrow::datatypes::DataType;

        let src: ArrayRef = arrow::array::new_null_array(&DataType::Null, 3);
        let result =
            reannotate_array(&src, &DataType::Boolean).expect("Null->Boolean must succeed");
        let out = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");

        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 3);
    }

    /// reannotate_array must coerce an Arrow `Null` source array into an
    /// all-null Int64 array.
    #[test]
    fn reannotate_null_array_coerces_to_int64() {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::DataType;

        let src: ArrayRef = arrow::array::new_null_array(&DataType::Null, 3);
        let result = reannotate_array(&src, &DataType::Int64).expect("Null->Int64 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");

        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 3);
    }

    /// reannotate_array must coerce an Arrow `Null` source array into an
    /// all-null Utf8 array.
    #[test]
    fn reannotate_null_array_coerces_to_utf8() {
        use arrow::array::{ArrayRef, StringArray};
        use arrow::datatypes::DataType;

        let src: ArrayRef = arrow::array::new_null_array(&DataType::Null, 3);
        let result = reannotate_array(&src, &DataType::Utf8).expect("Null->Utf8 must succeed");
        let out = result
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");

        assert_eq!(out.len(), 3);
        assert_eq!(out.null_count(), 3);
    }

    /// reannotate_array must keep failing fast when a NESTED source (List) is
    /// fed into a scalar sink column (Int32). Generalizing scalar->scalar
    /// coercion must NOT open a path that casts nested arrays to scalars; such
    /// structural mismatches must still hit the fail-fast catch-all.
    #[test]
    fn reannotate_list_to_int32_still_errors() {
        use arrow::array::{ArrayRef, Int32Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field};
        use std::sync::Arc;

        // List<Int32> with 2 lists: [1,2] and [3].
        let values = Arc::new(Int32Array::from(vec![1_i32, 2, 3])) as ArrayRef;
        let child_field = Arc::new(Field::new("item", DataType::Int32, true));
        let offsets = OffsetBuffer::new(vec![0_i32, 2, 3].into());
        let list = ListArray::try_new(child_field, offsets, values, None).expect("list");
        let src: ArrayRef = Arc::new(list);

        let err = reannotate_array(&src, &DataType::Int32)
            .expect_err("List<Int32> -> Int32 must remain a fail-fast error");
        assert!(
            err.contains("incompatible data types"),
            "expected the fail-fast catch-all error, got: {err}"
        );
    }

    /// reannotate_array must return a clean Err (not panic) when a runtime Map
    /// column carrying a NULL key is reannotated to the Iceberg target Map type
    /// whose key field is non-nullable (the spec marks map keys `required`).
    /// Without the guard, Arrow's `StructArray::new` panics with
    /// "Found unmasked nulls for non-nullable StructArray field \"key\"".
    #[test]
    fn reannotate_map_with_null_key_to_required_key_target_errors() {
        use arrow::array::{ArrayRef, Int32Array, MapArray, StringArray, StructArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::{DataType, Field, Fields};
        use std::sync::Arc;

        // Runtime source MapArray: key field nullable (as execution produces),
        // with a NULL key in the single entry [{null: "x"}].
        let src_key_field = Arc::new(Field::new("key", DataType::Int32, true));
        let src_value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let src_entries_fields: Fields =
            vec![src_key_field.clone(), src_value_field.clone()].into();
        let src_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(src_entries_fields.clone()),
            false,
        ));
        let keys = Arc::new(Int32Array::from(vec![None])) as ArrayRef;
        let values = Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef;
        let src_entries = StructArray::new(src_entries_fields, vec![keys, values], None);
        let src_map = MapArray::try_new(
            src_entries_field,
            OffsetBuffer::new(vec![0_i32, 1].into()),
            src_entries,
            None,
            false,
        )
        .expect("src map");
        let src: ArrayRef = Arc::new(src_map);

        // Target Iceberg Map type: key field NON-nullable (required).
        let target_key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let target_value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let target_entries_field = Arc::new(Field::new(
            "key_value",
            DataType::Struct(vec![target_key_field, target_value_field].into()),
            false,
        ));
        let target_dtype = DataType::Map(target_entries_field, false);

        let err = reannotate_array(&src, &target_dtype)
            .expect_err("null map key into a required key field must be a clean Err, not a panic");
        assert!(
            err.contains("Iceberg MAP keys must be non-null"),
            "expected the null-map-key fail-fast error, got: {err}"
        );
    }
}
