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
//! Streaming source operator for the `IcebergDeltaScan` ExecNode.
//!
//! Per-driver pull-driven scanner: at each `pull_chunk` call, advances
//! through `change_files`, opening a per-role scanner on demand and emitting
//! one chunk at a time. The `__change_op` column is injected as a constant
//! per-file value (`+1` for `DataFile`, `-1` for the three delete roles).
//!
//! Phase 1 (Task 4) lays only the factory + operator skeleton with all role
//! scanners stubbed out. Tasks 5-8 fill in the per-role scanner bodies.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int8Array};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;

use crate::exec::change_op::{CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::exec::node::iceberg_delta_scan::{
    DeltaSourceFile, DeltaSourceRole, IcebergDeltaScanNode,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Number of trailing virtual columns the scan-tuple advertises after the
/// data columns, in fixed order: `_file`, `_pos`, `_row_id`,
/// `_last_updated_sequence_number`, `__change_op`. The first four are
/// synthesized per-batch by the role scanners (`DataFileScanner` appends
/// them directly; the three delete-side scanners receive them prepended by
/// the underlying `scan_*` helpers in `connector::iceberg`). `__change_op`
/// is appended at the operator level by `inject_change_op_column`. Together
/// they define the trailing layout of the operator's chunk contract; the
/// projection logic below depends on this constant to split each scanner's
/// returned batch into "data columns" (which need iceberg-schema-evolution
/// projection) and "lineage columns" (which are already aligned).
///
/// The constant is shared with `build_iceberg_table_def_for_delta_scan`
/// (`src/connector/iceberg/catalog/backend.rs`) — keep in lockstep.
const ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT: usize = 5;
const ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT: usize = 4;

pub struct IcebergDeltaScanFactory {
    name: String,
    node: Arc<IcebergDeltaScanNode>,
}

impl IcebergDeltaScanFactory {
    pub fn new(node: IcebergDeltaScanNode) -> Self {
        let name = format!("IcebergDeltaScan (id={})", node.node_id);
        Self {
            name,
            node: Arc::new(node),
        }
    }
}

impl OperatorFactory for IcebergDeltaScanFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // Phase 1: single-driver only. driver_id != 0 returns an empty stream.
        let pending: VecDeque<DeltaSourceFile> = if driver_id == 0 {
            self.node.change_files.iter().cloned().collect()
        } else {
            VecDeque::new()
        };
        Box::new(IcebergDeltaScanOperator {
            name: self.name.clone(),
            node: Arc::clone(&self.node),
            pending,
            current_scanner: None,
            finished: false,
            data_projection: None,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct IcebergDeltaScanOperator {
    name: String,
    node: Arc<IcebergDeltaScanNode>,
    pending: VecDeque<DeltaSourceFile>,
    current_scanner: Option<Box<dyn DeltaFileScanner>>,
    finished: bool,
    /// Cached projection plan that maps each scanner's raw "data columns"
    /// (the parquet file's snapshot-time layout) onto the current iceberg
    /// schema's data columns as advertised by codegen (in `output_chunk_schema`).
    /// Built lazily on the first batch because constructing it requires
    /// reading the base table's current schema and converting it to arrow.
    data_projection: Option<Arc<IcebergDataColumnProjection>>,
}

pub(crate) trait DeltaFileScanner: Send {
    /// Pull the next batch from the underlying scan. Returns `None` when the
    /// scanner has been fully drained.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String>;
    /// Constant `__change_op` value to inject on every batch this scanner
    /// produces.
    fn change_op_value(&self) -> i8;
}

impl Operator for IcebergDeltaScanOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for IcebergDeltaScanOperator {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        !self.finished
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("IcebergDeltaScan does not accept input".to_string())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        loop {
            if self.current_scanner.is_none() {
                let Some(next) = self.pending.pop_front() else {
                    self.finished = true;
                    return Ok(None);
                };
                self.current_scanner = Some(open_scanner_for_role(&self.node, next)?);
            }
            match self.current_scanner.as_mut().unwrap().next_batch()? {
                Some(batch) => {
                    let op = self.current_scanner.as_ref().unwrap().change_op_value();
                    let projection = self.ensure_data_projection()?;
                    let realigned = project_scanner_batch_to_contract(&batch, &projection)?;
                    let tagged = inject_change_op_column(realigned, op)?;
                    let chunk = Chunk::try_new_with_chunk_schema(
                        tagged,
                        self.node.output_chunk_schema.clone(),
                    )?;
                    return Ok(Some(chunk));
                }
                None => {
                    self.current_scanner = None;
                }
            }
        }
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
}

impl IcebergDeltaScanOperator {
    /// Lazily build (and cache) the per-operator projection plan that maps
    /// each scanner's raw data columns to the current iceberg schema columns
    /// as advertised by codegen. The plan is invariant for the lifetime of
    /// this operator (a single delta-scan node only ever references one base
    /// table at one current schema), so we compute it once on the first
    /// batch.
    fn ensure_data_projection(&mut self) -> Result<Arc<IcebergDataColumnProjection>, String> {
        if let Some(existing) = self.data_projection.as_ref() {
            return Ok(Arc::clone(existing));
        }
        let plan = build_data_column_projection_plan(&self.node)?;
        let plan = Arc::new(plan);
        self.data_projection = Some(Arc::clone(&plan));
        Ok(plan)
    }
}

/// Per-output-column projection target derived from the codegen scan-tuple.
///
/// `name` is the iceberg-current-schema column name (== codegen slot name).
/// `field_id` is the iceberg field-id, used to find the matching column in
/// the parquet file by `parquet.field.id` metadata. `expected_data_type`
/// is the data type the current iceberg schema declares for this column
/// (which equals the codegen slot's arrow data type for IVM-A1; type
/// evolution is not yet supported and we surface a clear error if mismatched).
struct IcebergDataColumnTarget {
    name: String,
    field_id: i32,
    expected_data_type: arrow::datatypes::DataType,
    nullable: bool,
}

/// Plan for projecting each scanner's raw "data columns" prefix (everything
/// before the four virtual lineage columns) onto the codegen-advertised
/// current-iceberg-schema column list.
pub(crate) struct IcebergDataColumnProjection {
    targets: Vec<IcebergDataColumnTarget>,
}

fn build_data_column_projection_plan(
    node: &IcebergDeltaScanNode,
) -> Result<IcebergDataColumnProjection, String> {
    // The chunk-schema is laid out as:
    //   [data_columns ... (N) ][_file, _pos, _row_id, _last_updated_sequence_number, __change_op]
    // The trailing virtual-column count is fixed (see
    // `ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT` and
    // `build_iceberg_table_def_for_delta_scan`).
    let slots = node.output_chunk_schema.slots();
    if slots.len() < ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT {
        return Err(format!(
            "iceberg delta-scan chunk schema is too short to contain the {} \
             trailing virtual columns: {} slots",
            ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT,
            slots.len(),
        ));
    }
    let data_slot_count = slots.len() - ICEBERG_DELTA_TRAILING_VIRTUAL_COLUMN_COUNT;
    let current_schema = node.iceberg_runtime.base_table.metadata().current_schema();
    let mut targets = Vec::with_capacity(data_slot_count);
    for slot in &slots[..data_slot_count] {
        let name = slot.name().to_string();
        let nested = current_schema
            .field_by_name(slot.name())
            .or_else(|| current_schema.field_by_name_case_insensitive(slot.name()))
            .ok_or_else(|| {
                format!(
                    "iceberg delta-scan codegen tuple references column `{}` that is not in \
                     the current iceberg schema (schema_id={})",
                    slot.name(),
                    current_schema.schema_id(),
                )
            })?;
        targets.push(IcebergDataColumnTarget {
            name,
            field_id: nested.id,
            expected_data_type: slot.data_type().clone(),
            nullable: slot.nullable(),
        });
    }
    Ok(IcebergDataColumnProjection { targets })
}

/// Realign a scanner's returned batch so its data-column prefix matches the
/// codegen scan-tuple's current-iceberg-schema view.
///
/// Each scanner returns a batch with the layout:
///   `[<raw parquet data columns>, _file, _pos, _row_id, _last_updated_sequence_number]`
/// The data-column prefix preserves the parquet file's **snapshot-time**
/// schema (Iceberg metadata-only DDL — DROP / RENAME / REORDER — leaves on-disk
/// files untouched). The lineage suffix is operator-synthesized and already
/// aligned. This function rebuilds the data-column prefix in
/// `projection.targets` order by matching parquet columns to iceberg
/// field-ids (via `parquet.field.id` metadata), then concatenates the
/// untouched lineage columns. `__change_op` is appended downstream by
/// `inject_change_op_column`.
fn project_scanner_batch_to_contract(
    batch: &RecordBatch,
    projection: &IcebergDataColumnProjection,
) -> Result<RecordBatch, String> {
    let batch_schema = batch.schema();
    let batch_cols = batch.num_columns();
    if batch_cols < ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT {
        return Err(format!(
            "iceberg delta-scan: scanner returned batch with {} columns; expected at least {} \
             trailing lineage columns",
            batch_cols, ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT,
        ));
    }
    let lineage_start = batch_cols - ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT;
    let data_fields = &batch_schema.fields()[..lineage_start];
    let parquet_has_any_field_id = data_fields.iter().any(|f| {
        f.metadata()
            .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
            .is_some()
    });

    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(
        projection.targets.len() + ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT,
    );
    let mut new_fields: Vec<Field> = Vec::with_capacity(
        projection.targets.len() + ICEBERG_DELTA_PRE_CHANGE_OP_LINEAGE_COLUMN_COUNT,
    );

    for target in &projection.targets {
        // Prefer parquet field-id match for evolution-correct projection.
        let by_field_id = data_fields.iter().position(|f| {
            f.metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
                .and_then(|raw| raw.parse::<i32>().ok())
                == Some(target.field_id)
        });
        let source_idx = if let Some(idx) = by_field_id {
            idx
        } else if parquet_has_any_field_id {
            return Err(format!(
                "iceberg delta-scan: parquet data file does not contain field_id={} for \
                 column `{}` (the column may have been dropped from the file at write time, \
                 or the writer did not stamp field-ids on this column)",
                target.field_id, target.name,
            ));
        } else {
            // Tolerate parquet writers that did not stamp field-ids by
            // falling back to a case-insensitive name match. v3 row-lineage
            // tables are expected to have field-ids; this branch is purely
            // defensive.
            data_fields
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(&target.name))
                .ok_or_else(|| {
                    format!(
                        "iceberg delta-scan: parquet data file (no field-ids) has no column \
                         named `{}` (case-insensitive)",
                        target.name,
                    )
                })?
        };
        let source_field = batch_schema.field(source_idx);
        if source_field.data_type() != &target.expected_data_type {
            return Err(format!(
                "iceberg delta-scan: column `{}` (field_id={}) has parquet type {:?} but \
                 codegen tuple expects {:?}; type evolution is not yet supported in IVM-A1",
                target.name,
                target.field_id,
                source_field.data_type(),
                target.expected_data_type,
            ));
        }
        new_columns.push(batch.column(source_idx).clone());
        new_fields.push(Field::new(
            target.name.clone(),
            target.expected_data_type.clone(),
            target.nullable,
        ));
    }

    // Lineage tail: pass through untouched. We re-emit the field metadata
    // exactly as the scanner wrote it.
    for idx in lineage_start..batch_cols {
        new_columns.push(batch.column(idx).clone());
        new_fields.push(batch_schema.field(idx).clone());
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    RecordBatch::try_new_with_options(
        new_schema,
        new_columns,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| format!("project iceberg delta-scan batch to codegen contract: {e}"))
}

fn inject_change_op_column(batch: RecordBatch, value: i8) -> Result<RecordBatch, String> {
    let rows = batch.num_rows();
    let arr: ArrayRef = Arc::new(Int8Array::from(vec![value; rows]));
    let mut fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(crate::exec::change_op::change_op_field());
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    let mut columns = batch.columns().to_vec();
    columns.push(arr);
    RecordBatch::try_new(new_schema, columns).map_err(|e| format!("inject __change_op column: {e}"))
}

fn open_scanner_for_role(
    node: &IcebergDeltaScanNode,
    file: DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    match &file.role {
        DeltaSourceRole::DataFile => open_data_file_scanner(node, &file),
        DeltaSourceRole::PositionDelete {
            targets,
            file_format,
            content_offset,
            content_size_in_bytes,
        } => open_position_delete_scanner(
            node,
            &file,
            targets,
            *file_format,
            *content_offset,
            *content_size_in_bytes,
        ),
        DeltaSourceRole::EqualityDelete {
            equality_field_ids,
            targets: _,
        } => open_equality_delete_scanner(node, &file, equality_field_ids),
        DeltaSourceRole::DeletedDataFile {
            previous_data_file_visibility,
        } => open_deleted_data_file_scanner(node, &file, previous_data_file_visibility),
    }
}

struct DataFileScanner {
    batches: std::vec::IntoIter<RecordBatch>,
    /// File path used to populate the `_file` lineage column on every row.
    file_path: String,
    /// Base row id stamped on the manifest entry; the per-row `_row_id` for
    /// row `r` within this scanner is `first_row_id + (rows_emitted_so_far + r)`.
    first_row_id: i64,
    /// Manifest entry's `data_sequence_number`; replicated across all rows
    /// as `_last_updated_sequence_number`.
    data_sequence_number: i64,
    /// Rows emitted so far across all batches in this scanner. Used to make
    /// `_pos` and `_row_id` strictly monotonic over the file.
    rows_emitted: i64,
    /// V3 CoW-aware filter (Bug 3): when `Some`, only emit rows whose
    /// synthesized `_row_id` is in this set. `None` = emit all rows.
    row_id_allow_list: Option<std::collections::BTreeSet<i64>>,
}

impl DeltaFileScanner for DataFileScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        let Some(batch) = self.batches.next() else {
            return Ok(None);
        };
        let row_count = batch.num_rows();
        let pos_start = self.rows_emitted;
        let enriched = append_data_file_lineage_columns(
            &batch,
            &self.file_path,
            pos_start,
            self.first_row_id,
            self.data_sequence_number,
        )?;
        self.rows_emitted = self
            .rows_emitted
            .checked_add(row_count as i64)
            .ok_or_else(|| {
                format!(
                    "ivm-a1 data-file scanner row count overflow: file={} rows_so_far={} batch_rows={}",
                    self.file_path, self.rows_emitted, row_count
                )
            })?;

        // V3 CoW-unchanged-row filter: drop rows whose `_row_id` is not in
        // the allow list. `None` (no filter) is the common case.
        if let Some(allow_list) = self.row_id_allow_list.as_ref() {
            use arrow::array::{Array, BooleanArray, Int64Array};
            use arrow::compute::filter_record_batch;
            let row_ids_array = enriched
                .column_by_name("_row_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    format!(
                        "ivm-a1 data-file scanner: enriched batch has no _row_id column for {}",
                        self.file_path
                    )
                })?;
            let keep: Vec<bool> = (0..row_ids_array.len())
                .map(|i| !row_ids_array.is_null(i) && allow_list.contains(&row_ids_array.value(i)))
                .collect();
            // Skip filtering work when nothing to drop.
            if !keep.iter().all(|&k| k) {
                let mask = BooleanArray::from(keep);
                let filtered = filter_record_batch(&enriched, &mask).map_err(|e| {
                    format!(
                        "ivm-a1 data-file scanner: filter by allow_list failed for {}: {e}",
                        self.file_path
                    )
                })?;
                return Ok(Some(filtered));
            }
        }
        Ok(Some(enriched))
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_INSERT
    }
}

fn open_data_file_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let batches = crate::connector::iceberg::changes::scan_one_added_data_file(
        &file.path,
        file.size,
        &node.iceberg_runtime.base_table,
        node.object_store_config.as_ref(),
    )?;
    // Iceberg v3 row-lineage data files always carry `first_row_id` on the
    // manifest entry; the IVM-A1 contract is to fail loudly if either lineage
    // input is missing rather than silently fall back to defaults.
    let first_row_id = file.first_row_id.ok_or_else(|| {
        format!(
            "ivm-a1 data-file scanner: manifest entry for {} is missing first_row_id (Iceberg v3 \
             row-lineage required)",
            file.path
        )
    })?;
    let data_sequence_number = file.data_sequence_number.ok_or_else(|| {
        format!(
            "ivm-a1 data-file scanner: manifest entry for {} is missing data_sequence_number \
             (Iceberg v3 row-lineage required)",
            file.path
        )
    })?;
    Ok(Box::new(DataFileScanner {
        batches: batches.into_iter(),
        file_path: file.path.clone(),
        first_row_id,
        data_sequence_number,
        rows_emitted: 0,
        row_id_allow_list: file.row_id_allow_list.clone(),
    }))
}

/// Append the four Iceberg v3 row-lineage virtual columns to a raw data-file
/// batch (`_file`, `_pos`, `_row_id`, `_last_updated_sequence_number`).
/// Mirrors the order codegen registers in the scan-tuple descriptor through
/// `build_iceberg_table_def_for_delta_scan::iceberg_row_lineage_metadata_columns`,
/// so the chunk schema contract length matches.
///
/// V3 row-lineage rule: if the data file carries stored `_row_id` /
/// `_last_updated_sequence_number` columns, the stored non-NULL values take
/// precedence over the file-level `first_row_id + position` fallback. This
/// helper drops the stored lineage columns from the output schema and emits
/// only the synthesized virtual columns at the end of the batch — matching
/// the scan-tuple descriptor's column count and ordering.
fn append_data_file_lineage_columns(
    batch: &RecordBatch,
    file_path: &str,
    pos_start: i64,
    first_row_id: i64,
    data_sequence_number: i64,
) -> Result<RecordBatch, String> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let row_count = batch.num_rows();

    // Compute absolute positions for this batch within its source data file.
    let pos_values: Vec<i64> = (0..row_count as i64).map(|i| pos_start + i).collect();

    // V3-compliant synthesis: stored column wins, fall back to first_row_id + pos.
    let stored_idx = crate::connector::iceberg::row_lineage_synth::stored_row_lineage_indices(
        batch.schema().as_ref(),
    );
    let row_id_values = crate::connector::iceberg::row_lineage_synth::synthesize_row_id(
        batch.schema().as_ref(),
        batch.columns(),
        row_count,
        first_row_id,
        Some(&pos_values),
    )?;
    let last_updated_seq_values =
        crate::connector::iceberg::row_lineage_synth::synthesize_last_updated_sequence_number(
            batch.schema().as_ref(),
            batch.columns(),
            row_count,
            data_sequence_number,
        )?;

    let file_col: ArrayRef = Arc::new(StringArray::from(vec![file_path.to_string(); row_count]));
    let pos_col: ArrayRef = Arc::new(Int64Array::from(pos_values));
    let row_id_col: ArrayRef = Arc::new(Int64Array::from(row_id_values));
    let seq_col: ArrayRef = Arc::new(Int64Array::from(last_updated_seq_values));

    // Strip stored row-lineage columns from the output: we are emitting them as
    // virtual columns at the end of the batch, and the codegen scan-tuple
    // descriptor does not include the stored columns.
    let drop_indices: std::collections::HashSet<usize> = stored_idx
        .row_id
        .into_iter()
        .chain(stored_idx.last_updated_seq.into_iter())
        .collect();

    let mut fields: Vec<arrow::datatypes::Field> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(idx, _)| !drop_indices.contains(idx))
        .map(|(_, f)| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| !drop_indices.contains(idx))
        .map(|(_, c)| c.clone())
        .collect();

    fields.push(Field::new("_file", DataType::Utf8, false));
    fields.push(Field::new("_pos", DataType::Int64, false));
    fields.push(Field::new("_row_id", DataType::Int64, false));
    fields.push(Field::new(
        "_last_updated_sequence_number",
        DataType::Int64,
        false,
    ));
    columns.push(file_col);
    columns.push(pos_col);
    columns.push(row_id_col);
    columns.push(seq_col);

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("ivm-a1 data-file scanner: rebuild lineage batch failed: {e}"))
}

struct PositionDeleteScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for PositionDeleteScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_position_delete_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    targets: &[crate::exec::node::iceberg_delta_scan::PositionDeleteTargetData],
    file_format: crate::exec::node::iceberg_delta_scan::PositionDeleteFileFormat,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let referenced = if targets.len() == 1 {
        Some(targets[0].data_file_path.clone())
    } else {
        None
    };
    let iceberg_format = match file_format {
        crate::exec::node::iceberg_delta_scan::PositionDeleteFileFormat::Parquet => {
            iceberg::spec::DataFileFormat::Parquet
        }
        crate::exec::node::iceberg_delta_scan::PositionDeleteFileFormat::Puffin => {
            iceberg::spec::DataFileFormat::Puffin
        }
    };
    let delete = crate::connector::iceberg::changes::PositionDeleteRef {
        delete_file_path: file.path.clone(),
        delete_file_size: file.size,
        record_count: None,
        referenced_data_file: referenced,
        file_format: iceberg_format,
        content_offset,
        content_size_in_bytes,
        partition_values: Vec::new(),
    };
    let delete_side = node.iceberg_runtime.delete_side.as_ref().ok_or_else(|| {
        format!(
            "ivm-a1 position-delete scanner: runtime.delete_side missing for {} (lower_plan \
             should have preloaded it when the change batch has DELETE-side roles)",
            file.path
        )
    })?;
    let lineage = position_delete_lineage_lookup(delete_side);
    let rows = crate::connector::iceberg::changes::scan_position_delete_rows_for_targets(
        &node.iceberg_runtime.base_table,
        &delete,
        &lineage,
        &delete_side.deleted_data_file_paths,
        node.iceberg_runtime.object_store_factory.as_ref(),
        node.object_store_config.as_ref(),
    )?;
    Ok(Box::new(PositionDeleteScanner {
        batches: rows.into_iter(),
    }))
}

fn position_delete_lineage_lookup(
    delete_side: &crate::exec::node::iceberg_delta_scan::DeltaScanDeleteSide,
) -> std::collections::HashMap<String, crate::exec::node::iceberg_delta_scan::BaseDataFileLineage> {
    let mut lineage = delete_side.base_data_file_lineage.clone();
    for (path, previous) in &delete_side.previous_data_file_lineage {
        lineage.entry(path.clone()).or_insert(*previous);
    }
    lineage
}

struct EqualityDeleteScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for EqualityDeleteScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_equality_delete_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    equality_field_ids: &[i32],
) -> Result<Box<dyn DeltaFileScanner>, String> {
    let delete = crate::connector::iceberg::changes::EqualityDeleteRef {
        delete_file_path: file.path.clone(),
        delete_file_size: file.size,
        record_count: None,
        equality_ids: equality_field_ids.to_vec(),
        sequence_number: file.data_sequence_number,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        partition_values: Vec::new(),
    };
    let rows =
        crate::connector::iceberg::changes::scan_equality_delete_rows_for_one_with_v3_lineage_at(
            &node.iceberg_runtime.base_table,
            &delete,
            node.to_snapshot_id,
            node.iceberg_runtime.object_store_factory.as_ref(),
            node.object_store_config.as_ref(),
        )?;
    Ok(Box::new(EqualityDeleteScanner {
        batches: rows.into_iter(),
    }))
}

struct DeletedDataFileScanner {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl DeltaFileScanner for DeletedDataFileScanner {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, String> {
        Ok(self.batches.next())
    }

    fn change_op_value(&self) -> i8 {
        CHANGE_OP_DELETE
    }
}

fn open_deleted_data_file_scanner(
    node: &IcebergDeltaScanNode,
    file: &DeltaSourceFile,
    _visibility: &Option<crate::exec::node::iceberg_delta_scan::DeletedFileVisibility>,
) -> Result<Box<dyn DeltaFileScanner>, String> {
    // Use the preloaded full-table visibility from runtime.delete_side; the
    // operator no longer rebuilds per-file visibility from
    // `DeletedFileVisibility::already_deleted_positions`.
    let delete_side = node.iceberg_runtime.delete_side.as_ref().ok_or_else(|| {
        format!(
            "ivm-a1 deleted-data-file scanner: runtime.delete_side missing for {} (lower_plan \
             should have preloaded it when the change batch has DELETE-side roles)",
            file.path
        )
    })?;
    // Resolve the file's `first_row_id` / `data_sequence_number`. The
    // OVERWRITE manifest may only carry the per-DataFile fields when the
    // original APPEND explicitly stamped them. iceberg-rust's writer often
    // sets only the manifest-level `first_row_id`, so the per-DataFile
    // fields can be `None` here. In that case, fall back to the
    // previous-snapshot data-file lineage index (where the file is still
    // alive and its row-lineage attributes are unambiguous).
    let resolved_first_row_id = match file.first_row_id {
        Some(v) => v,
        None => delete_side
            .previous_data_file_lineage
            .get(&file.path)
            .map(|lineage| lineage.first_row_id)
            .ok_or_else(|| {
                format!(
                    "iceberg MV deleted-data-file reverse projection requires first_row_id for {} \
                     and the previous-snapshot data-file lineage index does not contain this file; \
                     this typically means the file was added and overwrite-deleted within the same \
                     delta range, which IVM-A1 does not support",
                    file.path,
                )
            })?,
    };
    let resolved_data_sequence_number = match file.data_sequence_number {
        Some(v) => v,
        None => delete_side
            .previous_data_file_lineage
            .get(&file.path)
            .map(|lineage| lineage.data_sequence_number)
            .ok_or_else(|| {
                format!(
                    "iceberg MV deleted-data-file reverse projection requires data_sequence_number \
                     for {} and the previous-snapshot data-file lineage index does not contain \
                     this file; rebuild the MV after enabling Iceberg v3 row-lineage metadata",
                    file.path,
                )
            })?,
    };
    let deleted_file = crate::connector::iceberg::changes::DeletedDataFileRef {
        path: file.path.clone(),
        size: file.size,
        record_count: None,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key.clone(),
        partition_values: Vec::new(),
        first_row_id: Some(resolved_first_row_id),
        data_sequence_number: Some(resolved_data_sequence_number),
    };
    let rows = crate::connector::iceberg::changes::scan_one_deleted_data_file(
        &node.iceberg_runtime.base_table,
        &deleted_file,
        node.object_store_config.as_ref(),
        &delete_side.previous_delete_visibility,
    )?;
    Ok(Box::new(DeletedDataFileScanner {
        batches: rows.into_iter(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_delete_lineage_lookup_falls_back_to_previous_snapshot_files() {
        let mut current = std::collections::HashMap::new();
        current.insert(
            "current.parquet".to_string(),
            crate::exec::node::iceberg_delta_scan::BaseDataFileLineage {
                first_row_id: 100,
                data_sequence_number: 7,
            },
        );
        current.insert(
            "rewritten.parquet".to_string(),
            crate::exec::node::iceberg_delta_scan::BaseDataFileLineage {
                first_row_id: 200,
                data_sequence_number: 8,
            },
        );
        let mut previous = std::collections::HashMap::new();
        previous.insert(
            "deleted-later.parquet".to_string(),
            crate::exec::node::iceberg_delta_scan::BaseDataFileLineage {
                first_row_id: 300,
                data_sequence_number: 9,
            },
        );
        previous.insert(
            "rewritten.parquet".to_string(),
            crate::exec::node::iceberg_delta_scan::BaseDataFileLineage {
                first_row_id: 400,
                data_sequence_number: 10,
            },
        );
        let delete_side = crate::exec::node::iceberg_delta_scan::DeltaScanDeleteSide {
            base_data_file_lineage: current,
            previous_delete_visibility: Default::default(),
            previous_data_file_lineage: previous,
            deleted_data_file_paths: Default::default(),
        };

        let lineage = position_delete_lineage_lookup(&delete_side);
        assert_eq!(lineage["current.parquet"].first_row_id, 100);
        assert_eq!(lineage["deleted-later.parquet"].first_row_id, 300);
        assert_eq!(lineage["rewritten.parquet"].first_row_id, 200);
    }

    /// Compile-only smoke test: the factory type is wired up and its trait
    /// implementations resolve. Semantic verification of empty-stream
    /// behavior happens in the SQL suite (Phase 7), where `iceberg::Table`
    /// fixtures are available.
    #[test]
    fn iceberg_delta_scan_factory_compiles_as_operator_factory() {
        fn assert_is_factory<T: OperatorFactory + ?Sized>() {}
        assert_is_factory::<IcebergDeltaScanFactory>();
    }

    #[test]
    fn append_data_file_lineage_columns_uses_stored_row_id() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field};
        use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
        use std::collections::HashMap;

        let mut meta_row_id = HashMap::new();
        meta_row_id.insert(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            crate::exec::row_position::ICEBERG_RESERVED_FIELD_ID_ROW_ID.to_string(),
        );
        let user_field = Field::new("id", DataType::Int64, false);
        let stored_row_id_field =
            Field::new("_row_id", DataType::Int64, true).with_metadata(meta_row_id);
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            user_field,
            stored_row_id_field,
        ]));
        let id_col: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![100i64, 200]));
        let stored_col: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![Some(42i64), None]));
        let batch = RecordBatch::try_new(schema, vec![id_col, stored_col]).expect("batch");

        let out =
            append_data_file_lineage_columns(&batch, "f.parquet", 7, 1000, 99).expect("append ok");

        let out_schema = out.schema();
        let names: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "id",
                "_file",
                "_pos",
                "_row_id",
                "_last_updated_sequence_number"
            ]
        );

        let row_ids = out
            .column_by_name("_row_id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("row id array");
        assert_eq!(row_ids.value(0), 42, "stored row id wins on row 0");
        assert_eq!(
            row_ids.value(1),
            1008,
            "fallback first_row_id + position on row 1"
        );
    }

    #[test]
    fn append_data_file_lineage_columns_falls_back_without_stored_column() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field};

        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let id_col: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![100i64, 200, 300]));
        let batch = RecordBatch::try_new(schema, vec![id_col]).expect("batch");

        let out =
            append_data_file_lineage_columns(&batch, "g.parquet", 0, 500, 12).expect("append ok");

        let row_ids = out
            .column_by_name("_row_id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("row id array");
        assert_eq!(row_ids.value(0), 500);
        assert_eq!(row_ids.value(1), 501);
        assert_eq!(row_ids.value(2), 502);

        let seqs = out
            .column_by_name("_last_updated_sequence_number")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("seq array");
        assert_eq!(seqs.value(0), 12);
        assert_eq!(seqs.value(1), 12);
        assert_eq!(seqs.value(2), 12);
    }
}
