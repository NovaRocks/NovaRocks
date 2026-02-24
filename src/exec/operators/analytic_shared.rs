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
//! Shared analytic-window state for sink/source split execution.
//!
//! Responsibilities:
//! - Stores partition-sorted chunks and window frame metadata produced by analytic sink operators.
//! - Coordinates readiness, errors, and output visibility for analytic source operators.
//!
//! Key exported interfaces:
//! - Types: `AnalyticSharedState`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Decimal256Array, Float64Builder, Int64Builder, ListArray,
    RecordBatch, StructArray, UInt32Builder, new_null_array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, concat, concat_batches, take};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_buffer::OffsetBuffer;
use arrow_buffer::i256;

use crate::common::config::operator_buffer_chunks;
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
use crate::exec::expr::agg::{
    AggScalarValue, AggStateArena, agg_scalar_from_array, build_agg_scalar_array, build_kernel_set,
    compare_agg_scalar_values,
};
use crate::exec::expr::decimal::{div_round_i128, div_round_i256, pow10_i128, pow10_i256};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
use crate::exec::node::analytic::{
    AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind, WindowFunctionSpec,
    WindowType,
};
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;

struct AnalyticState {
    input: Vec<Chunk>,
    output: VecDeque<Chunk>,
    pending: VecDeque<Chunk>,
    sink_finishing: bool,
    sink_complete: bool,
    computed: bool,
}

#[derive(Clone)]
/// Shared analytic window state containing buffered partitions, progress markers, and sink/source coordination flags.
pub(crate) struct AnalyticSharedState {
    inner: Arc<Mutex<AnalyticState>>,
    arena: Arc<ExprArena>,
    partition_exprs: Vec<ExprId>,
    order_by_exprs: Vec<ExprId>,
    functions: Vec<WindowFunctionSpec>,
    window: Option<WindowFrame>,
    output_columns: Vec<AnalyticOutputColumn>,
    output_slots: Vec<SlotId>,
    buffer_limit: usize,
    observable: Arc<Observable>,
    label: String,
    queue_tracker: Arc<OnceLock<Arc<MemTracker>>>,
}

impl AnalyticSharedState {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        partition_exprs: Vec<ExprId>,
        order_by_exprs: Vec<ExprId>,
        functions: Vec<WindowFunctionSpec>,
        window: Option<WindowFrame>,
        output_columns: Vec<AnalyticOutputColumn>,
        output_slots: Vec<SlotId>,
        node_id: i32,
    ) -> Self {
        let buffer_limit = operator_buffer_chunks().max(1);
        let label = if node_id >= 0 {
            format!("analytic_queue_{node_id}")
        } else {
            "analytic_queue".to_string()
        };
        Self {
            inner: Arc::new(Mutex::new(AnalyticState {
                input: Vec::new(),
                output: VecDeque::new(),
                pending: VecDeque::new(),
                sink_finishing: false,
                sink_complete: false,
                computed: false,
            })),
            arena,
            partition_exprs,
            order_by_exprs,
            functions,
            window,
            output_columns,
            output_slots,
            buffer_limit,
            observable: Arc::new(Observable::new()),
            label,
            queue_tracker: Arc::new(OnceLock::new()),
        }
    }

    pub(crate) fn push_input(&self, state: &RuntimeState, mut chunk: Chunk) {
        if let Some(tracker) = self.queue_mem_tracker(state).as_ref() {
            chunk.transfer_to(tracker);
        }
        let mut guard = self.inner.lock().expect("analytic state lock");
        if !chunk.is_empty() {
            guard.input.push(chunk);
        }
    }

    pub(crate) fn finish(&self, state: &RuntimeState) -> Result<(), String> {
        let notify = self.observable.defer_notify();
        let mut guard = self.inner.lock().expect("analytic state lock");
        guard.sink_finishing = true;
        if !guard.computed {
            let outputs = self
                .compute_outputs(&guard.input)
                .map_err(|e| e.to_string())?;
            let mut outputs = outputs;
            if let Some(tracker) = self.queue_mem_tracker(state).as_ref() {
                for chunk in outputs.iter_mut() {
                    chunk.transfer_to(tracker);
                }
            }
            guard.pending = outputs.into();
            guard.computed = true;
        }

        // Drain pending into output buffer up to the configured limit.
        while guard.output.len() < self.buffer_limit {
            let Some(chunk) = guard.pending.pop_front() else {
                break;
            };
            guard.output.push_back(chunk);
        }

        guard.sink_complete = true;
        drop(guard);
        notify.arm();
        Ok(())
    }

    pub(crate) fn has_output(&self) -> bool {
        let guard = self.inner.lock().expect("analytic state lock");
        !guard.output.is_empty() || !guard.pending.is_empty()
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    pub(crate) fn pop_output(&self) -> Option<Chunk> {
        let mut guard = self.inner.lock().expect("analytic state lock");
        if guard.output.is_empty() && !guard.pending.is_empty() {
            while guard.output.len() < self.buffer_limit {
                let Some(chunk) = guard.pending.pop_front() else {
                    break;
                };
                guard.output.push_back(chunk);
            }
        }
        let mut chunk = guard.output.pop_front()?;
        if let Some(tracker) = self.queue_tracker.get() {
            chunk.transfer_to(tracker);
        }
        Some(chunk)
    }

    pub(crate) fn is_done(&self) -> bool {
        let guard = self.inner.lock().expect("analytic state lock");
        guard.sink_complete && guard.output.is_empty() && guard.pending.is_empty()
    }

    fn compute_outputs(&self, input: &[Chunk]) -> Result<VecDeque<Chunk>, String> {
        if input.is_empty() {
            return Ok(VecDeque::new());
        }

        let input_schema = input[0].schema();
        let batches: Vec<RecordBatch> = input.iter().map(|c| c.batch.clone()).collect();
        let batch = concat_batches(&input_schema, &batches)
            .map_err(|e| format!("concat_batches: {}", e))?;
        let total_rows = batch.num_rows();
        if total_rows == 0 {
            return Ok(VecDeque::new());
        }

        let concat_chunk = Chunk::new(batch.clone());
        let mut ordered_chunk = concat_chunk;
        let mut partition_keys = self.eval_exprs(&ordered_chunk, &self.partition_exprs)?;
        let mut order_keys = self.eval_exprs(&ordered_chunk, &self.order_by_exprs)?;

        // Hash-based analytic plans may emit the same partition key in non-contiguous blocks.
        // For partition-only full-frame window aggregates, regroup rows by partition key first.
        if should_reorder_window_input(&self.functions, &order_keys, self.window.as_ref())?
            && !self.partition_exprs.is_empty()
        {
            ordered_chunk = reorder_chunk_by_partition_keys(&ordered_chunk, &partition_keys)?;
            partition_keys = self.eval_exprs(&ordered_chunk, &self.partition_exprs)?;
            order_keys = self.eval_exprs(&ordered_chunk, &self.order_by_exprs)?;
        }

        let partitions = compute_partitions(&partition_keys, total_rows)?;

        let mut func_outputs: Vec<ArrayRef> = Vec::with_capacity(self.functions.len());
        for (func_idx, func) in self.functions.iter().enumerate() {
            let arrays = self.eval_exprs(&ordered_chunk, &func.args)?;
            let out = compute_window_function(
                &self.arena,
                func,
                &arrays,
                &partitions,
                &order_keys,
                self.window.as_ref(),
                total_rows,
            )
            .map_err(|e| format!("window function #{}: {}", func_idx, e))?;
            func_outputs.push(out);
        }

        let output_schema = build_output_schema(
            Arc::clone(&input_schema),
            &self.output_columns,
            &self.functions,
            &self.output_slots,
        )?;

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.output_columns.len());
        for col in &self.output_columns {
            match *col {
                AnalyticOutputColumn::InputSlotId(slot_id) => {
                    columns.push(ordered_chunk.column_by_slot_id(slot_id)?);
                }
                AnalyticOutputColumn::Window(window_idx) => {
                    let arr = func_outputs.get(window_idx).ok_or_else(|| {
                        format!("window output index out of range: {}", window_idx)
                    })?;
                    columns.push(Arc::clone(arr));
                }
            }
        }
        let out_batch = RecordBatch::try_new(Arc::clone(&output_schema), columns)
            .map_err(|e| format!("build analytic output batch: {}", e))?;

        Ok(VecDeque::from(vec![Chunk::new(out_batch)]))
    }

    fn eval_exprs(&self, chunk: &Chunk, exprs: &[ExprId]) -> Result<Vec<ArrayRef>, String> {
        let mut out = Vec::with_capacity(exprs.len());
        for id in exprs {
            out.push(self.arena.eval(*id, chunk)?);
        }
        Ok(out)
    }

    fn queue_mem_tracker(&self, state: &RuntimeState) -> Option<Arc<MemTracker>> {
        let root = state.mem_tracker()?;
        let label = self.label.clone();
        let tracker = self
            .queue_tracker
            .get_or_init(|| MemTracker::new_child(label, &root));
        Some(Arc::clone(tracker))
    }
}

fn build_output_schema(
    input_schema: SchemaRef,
    output_columns: &[AnalyticOutputColumn],
    functions: &[WindowFunctionSpec],
    output_slots: &[SlotId],
) -> Result<SchemaRef, String> {
    if !output_slots.is_empty() && output_slots.len() != output_columns.len() {
        return Err(format!(
            "analytic output slot count mismatch: slots={} columns={}",
            output_slots.len(),
            output_columns.len()
        ));
    }
    let in_fields = input_schema.fields();
    let mut slot_to_input_idx = std::collections::HashMap::<SlotId, usize>::new();
    for (idx, f) in in_fields.iter().enumerate() {
        let Some(slot_id) = field_slot_id(f.as_ref())? else {
            continue;
        };
        if slot_to_input_idx.insert(slot_id, idx).is_some() {
            return Err(format!(
                "analytic input schema has duplicate slot_id={}, cannot build a stable mapping",
                slot_id
            ));
        }
    }
    let mut fields: Vec<Field> = Vec::with_capacity(output_columns.len());
    for (idx, col) in output_columns.iter().enumerate() {
        match *col {
            AnalyticOutputColumn::InputSlotId(input_slot_id) => {
                let input_idx =
                    slot_to_input_idx
                        .get(&input_slot_id)
                        .copied()
                        .ok_or_else(|| {
                            format!(
                                "analytic output refers to missing input slot_id={} in schema",
                                input_slot_id
                            )
                        })?;
                let f = in_fields.get(input_idx).ok_or_else(|| {
                    format!("input schema field index out of range: {}", input_idx)
                })?;
                let mut field = (**f).clone();
                if let Some(slot_id) = output_slots.get(idx) {
                    field = field_with_slot_id(field, *slot_id);
                }
                fields.push(field);
            }
            AnalyticOutputColumn::Window(i) => {
                let func = functions
                    .get(i)
                    .ok_or_else(|| format!("window function index out of range: {}", i))?;
                let mut field =
                    Field::new(format!("analytic_{}", i), func.return_type.clone(), true);
                if let Some(slot_id) = output_slots.get(idx) {
                    field = field_with_slot_id(field, *slot_id);
                }
                fields.push(field);
            }
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn compute_partitions(keys: &[ArrayRef], rows: usize) -> Result<Vec<(usize, usize)>, String> {
    if rows == 0 {
        return Ok(Vec::new());
    }
    if keys.is_empty() {
        return Ok(vec![(0, rows)]);
    }
    let mut parts = Vec::new();
    let mut start = 0usize;
    for i in 1..rows {
        if !row_equal_on_keys(keys, i - 1, i)? {
            parts.push((start, i));
            start = i;
        }
    }
    parts.push((start, rows));
    Ok(parts)
}

fn should_reorder_window_input(
    functions: &[WindowFunctionSpec],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
) -> Result<bool, String> {
    if !order_keys.is_empty() || window.is_some() {
        return Ok(false);
    }
    Ok(functions.iter().all(|func| {
        matches!(
            func.kind,
            WindowFunctionKind::Count
                | WindowFunctionKind::Sum
                | WindowFunctionKind::Avg
                | WindowFunctionKind::Min
                | WindowFunctionKind::Max
                | WindowFunctionKind::BitmapUnion
                | WindowFunctionKind::MinBy
                | WindowFunctionKind::MinByV2
                | WindowFunctionKind::VarianceSamp
                | WindowFunctionKind::StddevSamp
                | WindowFunctionKind::BoolOr
                | WindowFunctionKind::CovarPop
                | WindowFunctionKind::CovarSamp
                | WindowFunctionKind::Corr
        )
    }))
}

fn reorder_chunk_by_partition_keys(chunk: &Chunk, keys: &[ArrayRef]) -> Result<Chunk, String> {
    let rows = chunk.len();
    if rows <= 1 || keys.is_empty() {
        return Ok(chunk.clone());
    }

    let mut perm: Vec<usize> = (0..rows).collect();
    let mut sort_err: Option<String> = None;
    perm.sort_by(|left, right| {
        if sort_err.is_some() {
            return Ordering::Equal;
        }
        match compare_rows_on_partition_keys(keys, *left, *right) {
            Ok(ord) => {
                if ord.is_eq() {
                    left.cmp(right)
                } else {
                    ord
                }
            }
            Err(e) => {
                sort_err = Some(e);
                Ordering::Equal
            }
        }
    });
    if let Some(err) = sort_err {
        return Err(err);
    }
    if perm.iter().enumerate().all(|(idx, row)| idx == *row) {
        return Ok(chunk.clone());
    }

    let mut idx_builder = UInt32Builder::with_capacity(rows);
    for row in perm {
        idx_builder.append_value(row as u32);
    }
    let idx_arr = Arc::new(idx_builder.finish()) as ArrayRef;

    let mut reordered_columns: Vec<ArrayRef> = Vec::with_capacity(chunk.batch.num_columns());
    for column in chunk.batch.columns() {
        let reordered = take(column.as_ref(), idx_arr.as_ref(), None).map_err(|e| e.to_string())?;
        reordered_columns.push(reordered);
    }
    let out_batch = RecordBatch::try_new(chunk.batch.schema(), reordered_columns)
        .map_err(|e| format!("build reordered analytic batch: {}", e))?;
    Ok(Chunk::new(out_batch))
}

fn compare_rows_on_partition_keys(
    keys: &[ArrayRef],
    left: usize,
    right: usize,
) -> Result<Ordering, String> {
    for array in keys {
        match (array.is_null(left), array.is_null(right)) {
            (true, true) => continue,
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {
                let ord = compare_at(array.as_ref(), left, right)?;
                if !ord.is_eq() {
                    return Ok(ord);
                }
            }
        }
    }
    Ok(Ordering::Equal)
}

fn compute_peer_groups(
    order_keys: &[ArrayRef],
    part_start: usize,
    part_end: usize,
) -> Result<Vec<(usize, usize)>, String> {
    if part_start >= part_end {
        return Ok(Vec::new());
    }
    if order_keys.is_empty() {
        return Ok(vec![(part_start, part_end)]);
    }
    let mut groups = Vec::new();
    let mut start = part_start;
    for i in (part_start + 1)..part_end {
        if !row_equal_on_keys(order_keys, i - 1, i)? {
            groups.push((start, i));
            start = i;
        }
    }
    groups.push((start, part_end));
    Ok(groups)
}

fn row_equal_on_keys(keys: &[ArrayRef], left: usize, right: usize) -> Result<bool, String> {
    for a in keys {
        if !value_equal_or_both_null(a.as_ref(), left, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn value_equal_or_both_null(array: &dyn Array, left: usize, right: usize) -> Result<bool, String> {
    if array.is_null(left) && array.is_null(right) {
        return Ok(true);
    }
    if array.is_null(left) || array.is_null(right) {
        return Ok(false);
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "failed to downcast BooleanArray".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "failed to downcast Int8Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "failed to downcast Int16Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast Int32Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "failed to downcast Int64Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::UInt32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .ok_or_else(|| "failed to downcast UInt32Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "failed to downcast Float32Array".to_string())?;
            Ok(a.value(left).to_bits() == a.value(right).to_bits())
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast Float64Array".to_string())?;
            Ok(a.value(left).to_bits() == a.value(right).to_bits())
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "failed to downcast StringArray".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "failed to downcast Date32Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampSecondArray".to_string())?;
                Ok(a.value(left) == a.value(right))
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampMillisecondArray".to_string())?;
                Ok(a.value(left) == a.value(right))
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampMicrosecondArray".to_string())?;
                Ok(a.value(left) == a.value(right))
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampNanosecondArray".to_string())?;
                Ok(a.value(left) == a.value(right))
            }
        },
        DataType::Decimal128(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "failed to downcast Decimal128Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        DataType::Decimal256(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .ok_or_else(|| "failed to downcast Decimal256Array".to_string())?;
            Ok(a.value(left) == a.value(right))
        }
        other => Err(format!("unsupported key type for equality: {:?}", other)),
    }
}

fn compute_window_function(
    arena: &ExprArena,
    func: &WindowFunctionSpec,
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    match &func.kind {
        WindowFunctionKind::RowNumber => compute_row_number(partitions, total_rows),
        WindowFunctionKind::Rank => compute_rank(partitions, order_keys, total_rows),
        WindowFunctionKind::DenseRank => compute_dense_rank(partitions, order_keys, total_rows),
        WindowFunctionKind::CumeDist => compute_cume_dist(partitions, order_keys, total_rows),
        WindowFunctionKind::PercentRank => compute_percent_rank(partitions, order_keys, total_rows),
        WindowFunctionKind::Ntile => compute_ntile(arena, func, args, partitions, total_rows),
        WindowFunctionKind::FirstValue { ignore_nulls } => compute_first_last_value(
            args,
            partitions,
            order_keys,
            window,
            *ignore_nulls,
            true,
            total_rows,
        ),
        WindowFunctionKind::FirstValueRewrite { ignore_nulls } => {
            let pad_rows = if func.args.len() > 1 {
                parse_int64_literal(arena, func.args[1])?
            } else {
                -1
            };
            compute_first_value_rewrite(
                args,
                partitions,
                order_keys,
                window,
                *ignore_nulls,
                pad_rows,
                total_rows,
            )
        }
        WindowFunctionKind::LastValue { ignore_nulls } => compute_first_last_value(
            args,
            partitions,
            order_keys,
            window,
            *ignore_nulls,
            false,
            total_rows,
        ),
        WindowFunctionKind::Lead { ignore_nulls } => compute_lead_lag(
            arena,
            func,
            args,
            partitions,
            *ignore_nulls,
            false,
            total_rows,
        ),
        WindowFunctionKind::Lag { ignore_nulls } => compute_lead_lag(
            arena,
            func,
            args,
            partitions,
            *ignore_nulls,
            true,
            total_rows,
        ),
        WindowFunctionKind::SessionNumber => {
            compute_session_number(arena, func, args, partitions, total_rows)
        }
        WindowFunctionKind::Count => {
            compute_count(args, partitions, order_keys, window, total_rows)
        }
        WindowFunctionKind::Sum => compute_sum(
            args,
            partitions,
            order_keys,
            window,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::Avg => compute_avg(
            args,
            partitions,
            order_keys,
            window,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::Min => compute_min_max(
            args,
            partitions,
            order_keys,
            window,
            true,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::Max => compute_min_max(
            args,
            partitions,
            order_keys,
            window,
            false,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::BitmapUnion => compute_window_custom_aggregate(
            "bitmap_union",
            args,
            partitions,
            order_keys,
            window,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::MinBy => compute_window_custom_aggregate(
            "min_by",
            args,
            partitions,
            order_keys,
            window,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::MinByV2 => compute_window_custom_aggregate(
            "min_by_v2",
            args,
            partitions,
            order_keys,
            window,
            &func.return_type,
            total_rows,
        ),
        WindowFunctionKind::VarianceSamp => {
            compute_variance_samp(args, partitions, order_keys, window, total_rows)
        }
        WindowFunctionKind::StddevSamp => {
            compute_stddev_samp(args, partitions, order_keys, window, total_rows)
        }
        WindowFunctionKind::BoolOr => {
            compute_bool_or(args, partitions, order_keys, window, total_rows)
        }
        WindowFunctionKind::CovarPop => compute_covar_corr(
            args,
            partitions,
            order_keys,
            window,
            total_rows,
            "covar_pop",
        ),
        WindowFunctionKind::CovarSamp => compute_covar_corr(
            args,
            partitions,
            order_keys,
            window,
            total_rows,
            "covar_samp",
        ),
        WindowFunctionKind::Corr => {
            compute_covar_corr(args, partitions, order_keys, window, total_rows, "corr")
        }
        WindowFunctionKind::ArrayAgg {
            is_distinct,
            is_asc_order,
            nulls_first,
        } => compute_window_array_agg(
            args,
            partitions,
            order_keys,
            window,
            total_rows,
            &func.return_type,
            *is_distinct,
            is_asc_order.as_slice(),
            nulls_first.as_slice(),
        ),
        WindowFunctionKind::ApproxTopK => compute_window_approx_top_k(
            arena, func, args, partitions, order_keys, window, total_rows,
        ),
    }
}

fn compute_row_number(
    partitions: &[(usize, usize)],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let mut b = Int64Builder::with_capacity(total_rows);
    for (start, end) in partitions {
        for i in *start..*end {
            b.append_value((i - start + 1) as i64);
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_rank(
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let mut b = Int64Builder::with_capacity(total_rows);
    for (p_start, p_end) in partitions {
        let peer_groups = compute_peer_groups(order_keys, *p_start, *p_end)?;
        let mut rank = 1i64;
        for (g_start, g_end) in peer_groups {
            let size = (g_end - g_start) as i64;
            for _ in g_start..g_end {
                b.append_value(rank);
            }
            rank += size;
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_dense_rank(
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let mut b = Int64Builder::with_capacity(total_rows);
    for (p_start, p_end) in partitions {
        let peer_groups = compute_peer_groups(order_keys, *p_start, *p_end)?;
        let mut rank = 1i64;
        for (g_start, g_end) in peer_groups {
            for _ in g_start..g_end {
                b.append_value(rank);
            }
            rank += 1;
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_cume_dist(
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let mut b = Float64Builder::with_capacity(total_rows);
    for (p_start, p_end) in partitions {
        let peer_groups = compute_peer_groups(order_keys, *p_start, *p_end)?;
        let partition_size = (*p_end - *p_start) as f64;
        for (_g_start, g_end) in peer_groups {
            let rank = (g_end - *p_start) as f64;
            let v = rank / partition_size;
            let group_size = g_end - _g_start;
            for _ in 0..group_size {
                b.append_value(v);
            }
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_percent_rank(
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let mut b = Float64Builder::with_capacity(total_rows);
    for (p_start, p_end) in partitions {
        let peer_groups = compute_peer_groups(order_keys, *p_start, *p_end)?;
        let partition_size = (*p_end - *p_start) as f64;
        let denom = if partition_size > 1.0 {
            partition_size - 1.0
        } else {
            1.0
        };
        let mut rank = 1f64;
        for (g_start, g_end) in peer_groups {
            let v = if partition_size > 1.0 {
                (rank - 1.0) / denom
            } else {
                0.0
            };
            let group_size = (g_end - g_start) as f64;
            for _ in g_start..g_end {
                b.append_value(v);
            }
            rank += group_size;
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_ntile(
    arena: &ExprArena,
    func: &WindowFunctionSpec,
    _args: &[ArrayRef],
    partitions: &[(usize, usize)],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let buckets = if func.args.is_empty() {
        return Err("ntile missing buckets argument".to_string());
    } else {
        parse_int64_literal(arena, func.args[0])?
    };
    if buckets <= 0 {
        return Err("ntile buckets must be positive".to_string());
    }

    let mut b = Int64Builder::with_capacity(total_rows);
    for (p_start, p_end) in partitions {
        let n = (*p_end - *p_start) as i64;
        let num_buckets = buckets;
        let small_bucket_size = n / num_buckets;
        let large_bucket_size = small_bucket_size + 1;
        let num_large_buckets = n % num_buckets;
        let num_large_bucket_rows = num_large_buckets * large_bucket_size;
        for (pos, _row) in (*p_start..*p_end).enumerate() {
            let pos = pos as i64;
            let id = if pos < num_large_bucket_rows {
                pos / large_bucket_size + 1
            } else {
                (pos - num_large_bucket_rows) / small_bucket_size + num_large_buckets + 1
            };
            b.append_value(id);
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_window_custom_aggregate(
    func_name: &str,
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    return_type: &DataType,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let input_array = match args {
        [] => None,
        [single] => Some(Arc::clone(single)),
        many => {
            let mut fields = Vec::with_capacity(many.len());
            for (idx, arr) in many.iter().enumerate() {
                fields.push(Field::new(format!("f{idx}"), arr.data_type().clone(), true));
            }
            let packed = StructArray::new(Fields::from(fields), many.to_vec(), None);
            Some(Arc::new(packed) as ArrayRef)
        }
    };

    let kernels = build_kernel_set(
        &[AggFunction {
            name: func_name.to_string(),
            inputs: Vec::new(),
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: None,
                output_type: Some(return_type.clone()),
                input_arg_type: input_array.as_ref().map(|a| a.data_type().clone()),
            }),
        }],
        &[input_array.as_ref().map(|a| a.data_type().clone())],
    )?;
    let kernel = kernels
        .entries
        .first()
        .ok_or_else(|| format!("missing aggregate kernel for {}", func_name))?;

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut out_arrays: Vec<ArrayRef> = Vec::with_capacity(total_rows);
    let mut state_ptrs = Vec::new();
    let mut state_arena = AggStateArena::new(8 * 1024);

    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        for (frame_start, frame_end) in frames.into_iter() {
            let state_ptr = state_arena.alloc(kernels.layout.total_size, kernel.state_align());
            kernel.init_state(state_ptr);

            if frame_start < frame_end {
                let frame_len = frame_end - frame_start;
                let frame_input = input_array
                    .as_ref()
                    .map(|array| array.slice(frame_start, frame_len));
                let view = kernel.build_input_view(&frame_input)?;
                state_ptrs.clear();
                state_ptrs.resize(frame_len, state_ptr);
                kernel.update_batch(&state_ptrs, &view)?;
            }

            let mut out = kernel.build_array(&[state_ptr], false)?;
            if out.data_type() != return_type {
                let from_type = out.data_type().clone();
                out = cast(out.as_ref(), return_type).map_err(|e| {
                    format!(
                        "cast {} window aggregate output from {:?} to {:?}: {}",
                        func_name, from_type, return_type, e
                    )
                })?;
            }
            out_arrays.push(out);
            kernel.drop_state(state_ptr);
        }
    }

    if out_arrays.is_empty() {
        return Ok(new_null_array(return_type, 0));
    }
    let out_refs: Vec<&dyn Array> = out_arrays.iter().map(|array| array.as_ref()).collect();
    concat(out_refs.as_slice())
        .map_err(|e| format!("concat {} window aggregate outputs: {}", func_name, e))
}

fn compute_first_last_value(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    ignore_nulls: bool,
    is_first: bool,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "first_value/last_value missing value argument".to_string())?;

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut indices = UInt32Builder::with_capacity(total_rows);

    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        let part_len = *p_end - *p_start;
        let (next_non_null, prev_non_null) = if ignore_nulls {
            let mut next = vec![None; part_len + 1];
            let mut prev = vec![None; part_len];

            let mut next_idx = None;
            for off in (0..part_len).rev() {
                let row = *p_start + off;
                if !value.is_null(row) {
                    next_idx = Some(row);
                }
                next[off] = next_idx;
            }

            let mut prev_idx = None;
            for off in 0..part_len {
                let row = *p_start + off;
                if !value.is_null(row) {
                    prev_idx = Some(row);
                }
                prev[off] = prev_idx;
            }
            (next, prev)
        } else {
            (Vec::new(), Vec::new())
        };

        for (frame_start, frame_end) in frames.into_iter() {
            let idx = if frame_start >= frame_end {
                None
            } else if is_first {
                if !ignore_nulls {
                    Some(frame_start)
                } else {
                    next_non_null[frame_start - *p_start].and_then(|v| (v < frame_end).then_some(v))
                }
            } else {
                if !ignore_nulls {
                    Some(frame_end - 1)
                } else {
                    prev_non_null[frame_end - 1 - *p_start]
                        .and_then(|v| (v >= frame_start).then_some(v))
                }
            };
            if let Some(i) = idx {
                indices.append_value(i as u32);
            } else {
                indices.append_null();
            }
        }
    }

    let idx_arr = Arc::new(indices.finish()) as ArrayRef;
    let out = take(value.as_ref(), idx_arr.as_ref(), None).map_err(|e| e.to_string())?;
    Ok(out)
}

fn compute_first_value_rewrite(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    ignore_nulls: bool,
    pad_rows: i64,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "first_value_rewrite missing value argument".to_string())?;

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let pad_rows = if pad_rows < 0 { 0 } else { pad_rows as usize };

    let mut indices = UInt32Builder::with_capacity(total_rows);
    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        let part_len = *p_end - *p_start;
        let (next_non_null, prev_non_null) = if ignore_nulls {
            let mut next = vec![None; part_len + 1];
            let mut prev = vec![None; part_len];

            let mut next_idx = None;
            for off in (0..part_len).rev() {
                let row = *p_start + off;
                if !value.is_null(row) {
                    next_idx = Some(row);
                }
                next[off] = next_idx;
            }

            let mut prev_idx = None;
            for off in 0..part_len {
                let row = *p_start + off;
                if !value.is_null(row) {
                    prev_idx = Some(row);
                }
                prev[off] = prev_idx;
            }
            (next, prev)
        } else {
            (Vec::new(), Vec::new())
        };
        let first_non_null_in_partition = if ignore_nulls { next_non_null[0] } else { None };

        for (row_idx, (frame_start, frame_end)) in frames.into_iter().enumerate() {
            if row_idx < pad_rows {
                indices.append_null();
                continue;
            }

            let idx = if frame_start >= frame_end {
                // For empty frames, first_value_rewrite should fall back to the partition start.
                if ignore_nulls {
                    first_non_null_in_partition
                } else {
                    Some(*p_start)
                }
            } else {
                if !ignore_nulls {
                    Some(frame_end - 1)
                } else {
                    prev_non_null[frame_end - 1 - *p_start]
                        .and_then(|v| (v >= frame_start).then_some(v))
                }
            };
            if let Some(i) = idx {
                indices.append_value(i as u32);
            } else {
                indices.append_null();
            }
        }
    }

    let idx_arr = Arc::new(indices.finish()) as ArrayRef;
    let out = take(value.as_ref(), idx_arr.as_ref(), None).map_err(|e| e.to_string())?;
    Ok(out)
}

fn compute_lead_lag(
    arena: &ExprArena,
    func: &WindowFunctionSpec,
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    ignore_nulls: bool,
    is_lag: bool,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "lead/lag missing value argument".to_string())?;

    let offset = if func.args.len() >= 2 {
        parse_int64_literal(arena, func.args[1])?
    } else {
        1
    };
    if offset < 0 {
        return Err("lead/lag offset must be non-negative".to_string());
    }
    let offset = offset as i64;

    let default_array = if func.args.len() >= 3 {
        // Evaluated on the concatenated chunk by the caller; args includes it.
        args.get(2).cloned()
    } else {
        None
    };
    let default_array = if let Some(def) = default_array {
        if def.data_type() == value.data_type() {
            Some(def)
        } else if matches!(def.data_type(), DataType::Null) {
            Some(new_null_array(value.data_type(), def.len()))
        } else {
            let from_type = def.data_type().clone();
            let to_type = value.data_type().clone();
            Some(cast(def.as_ref(), value.data_type()).map_err(|e| {
                format!(
                    "cast lead/lag default value from {:?} to {:?}: {}",
                    from_type, to_type, e
                )
            })?)
        }
    } else {
        None
    };

    let mut indices = UInt32Builder::with_capacity(total_rows);
    let mut use_default = BooleanBuilder::with_capacity(total_rows);

    for (p_start, p_end) in partitions {
        let part_len = *p_end - *p_start;
        let mut non_null_positions: Vec<usize> = Vec::new();
        let mut prefix_non_null: Vec<usize> = Vec::new();
        if ignore_nulls {
            prefix_non_null = vec![0; part_len + 1];
            for (off, row) in (*p_start..*p_end).enumerate() {
                let mut cnt = prefix_non_null[off];
                if !value.is_null(row) {
                    non_null_positions.push(row);
                    cnt += 1;
                }
                prefix_non_null[off + 1] = cnt;
            }
        }

        let offset_usize = usize::try_from(offset).ok();
        for row in *p_start..*p_end {
            let target = if is_lag {
                (row as i64) - offset
            } else {
                (row as i64) + offset
            };

            let mut found: Option<usize> = None;
            if ignore_nulls {
                if offset == 0 {
                    found = Some(row);
                } else {
                    if let Some(off) = offset_usize {
                        if is_lag {
                            let count_before = prefix_non_null[row - *p_start];
                            if count_before >= off {
                                found = non_null_positions.get(count_before - off).copied();
                            }
                        } else {
                            let start_rank = prefix_non_null[row - *p_start + 1];
                            if let Some(target_rank) = start_rank.checked_add(off - 1) {
                                found = non_null_positions.get(target_rank).copied();
                            }
                        }
                    }
                }
            } else if target >= *p_start as i64 && target < *p_end as i64 {
                found = Some(target as usize);
            }

            if let Some(i) = found {
                indices.append_value(i as u32);
                use_default.append_value(false);
            } else if default_array.is_some() {
                indices.append_null();
                use_default.append_value(true);
            } else {
                indices.append_null();
                use_default.append_value(false);
            }
        }
    }

    let idx_arr = Arc::new(indices.finish()) as ArrayRef;
    let taken = take(value.as_ref(), idx_arr.as_ref(), None).map_err(|e| e.to_string())?;

    if let Some(def) = default_array.as_ref() {
        let mask = use_default.finish();
        let out = zip(&mask, def, &taken).map_err(|e| e.to_string())?;
        return Ok(out);
    }

    Ok(taken)
}

fn compute_session_number(
    arena: &ExprArena,
    func: &WindowFunctionSpec,
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    total_rows: usize,
) -> Result<ArrayRef, String> {
    if func.args.len() != 2 {
        return Err("session_number expects 2 arguments".to_string());
    }
    let delta_node = arena
        .node(func.args[1])
        .ok_or_else(|| "invalid ExprId".to_string())?;
    let delta = match delta_node {
        ExprNode::Literal(LiteralValue::Null) => {
            return Ok(new_null_array(&DataType::Int64, total_rows));
        }
        _ => parse_int64_literal(arena, func.args[1])? as i64,
    };

    let value = args
        .get(0)
        .ok_or_else(|| "session_number missing value argument".to_string())?;

    let mut b = Int64Builder::with_capacity(total_rows);

    for (p_start, p_end) in partitions {
        let mut session_id = 1i64;
        let mut has_last = false;
        let mut last_value: i64 = 0;
        for row in *p_start..*p_end {
            if value.is_null(row) {
                b.append_null();
                continue;
            }
            let cur = scalar_i64(value.as_ref(), row)?;
            if has_last && (cur - last_value) > delta {
                session_id += 1;
            }
            has_last = true;
            last_value = cur;
            b.append_value(session_id);
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_count(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args.get(0).cloned();

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut b = Int64Builder::with_capacity(total_rows);

    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        let mut prefix_non_null: Vec<i64> = Vec::new();
        if let Some(v) = value.as_ref() {
            prefix_non_null = vec![0; (*p_end - *p_start) + 1];
            for (i, row) in (*p_start..*p_end).enumerate() {
                prefix_non_null[i + 1] = prefix_non_null[i] + if v.is_null(row) { 0 } else { 1 };
            }
        }
        for (row, (frame_start, frame_end)) in (*p_start..*p_end).zip(frames.into_iter()) {
            let cnt = if value.is_none() {
                (frame_end as i64) - (frame_start as i64)
            } else {
                let s = frame_start - *p_start;
                let e = frame_end - *p_start;
                prefix_non_null[e] - prefix_non_null[s]
            };
            let _ = row;
            b.append_value(cnt);
        }
    }

    Ok(Arc::new(b.finish()))
}

fn compute_sum(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    return_type: &DataType,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "sum missing value argument".to_string())?;
    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    match return_type {
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![0i128; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                for (i, row) in (*p_start..*p_end).enumerate() {
                    prefix_sum[i + 1] = prefix_sum[i]
                        + if value.is_null(row) {
                            0
                        } else {
                            scalar_i128(value.as_ref(), row)?
                        };
                    prefix_cnt[i + 1] = prefix_cnt[i] + if value.is_null(row) { 0 } else { 1 };
                }
                for (row, (frame_start, frame_end)) in (*p_start..*p_end).zip(frames.into_iter()) {
                    let s = frame_start - *p_start;
                    let e = frame_end - *p_start;
                    let cnt = prefix_cnt[e] - prefix_cnt[s];
                    if cnt == 0 {
                        b.append_null();
                    } else {
                        let sum = prefix_sum[e] - prefix_sum[s];
                        let sum_i64: i64 =
                            sum.try_into().map_err(|_| "sum overflow".to_string())?;
                        b.append_value(sum_i64);
                    }
                    let _ = row;
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![0f64; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                for (i, row) in (*p_start..*p_end).enumerate() {
                    prefix_sum[i + 1] = prefix_sum[i]
                        + if value.is_null(row) {
                            0.0
                        } else {
                            scalar_f64(value.as_ref(), row)?
                        };
                    prefix_cnt[i + 1] = prefix_cnt[i] + if value.is_null(row) { 0 } else { 1 };
                }
                for (row, (frame_start, frame_end)) in (*p_start..*p_end).zip(frames.into_iter()) {
                    let s = frame_start - *p_start;
                    let e = frame_end - *p_start;
                    let cnt = prefix_cnt[e] - prefix_cnt[s];
                    if cnt == 0 {
                        b.append_null();
                    } else {
                        let sum = prefix_sum[e] - prefix_sum[s];
                        b.append_value(sum);
                    }
                    let _ = row;
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Decimal128(out_precision, out_scale) => {
            let input_scale = decimal_input_scale(value.data_type())?;
            let scale_diff = (*out_scale as i32) - (input_scale as i32);
            let factor = if scale_diff == 0 {
                None
            } else {
                Some(pow10_i128(scale_diff.unsigned_abs() as usize)?)
            };

            let mut out = Vec::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![0i128; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                for (i, row) in (*p_start..*p_end).enumerate() {
                    prefix_sum[i + 1] = prefix_sum[i]
                        + if value.is_null(row) {
                            0
                        } else {
                            scalar_i128(value.as_ref(), row)?
                        };
                    prefix_cnt[i + 1] = prefix_cnt[i] + if value.is_null(row) { 0 } else { 1 };
                }
                for (frame_start, frame_end) in frames.into_iter() {
                    let s = frame_start - *p_start;
                    let e = frame_end - *p_start;
                    let cnt = prefix_cnt[e] - prefix_cnt[s];
                    if cnt == 0 {
                        out.push(None);
                        continue;
                    }
                    let mut sum = prefix_sum[e] - prefix_sum[s];
                    if let Some(factor) = factor {
                        if scale_diff > 0 {
                            sum = sum
                                .checked_mul(factor)
                                .ok_or_else(|| "decimal overflow".to_string())?;
                        } else {
                            sum /= factor;
                        }
                    }
                    out.push(Some(sum));
                }
            }

            let array = arrow::array::Decimal128Array::from(out)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(out_precision, out_scale) => {
            let input_scale = decimal_input_scale(value.data_type())?;
            let scale_diff = (*out_scale as i32) - (input_scale as i32);
            let factor = if scale_diff == 0 {
                None
            } else {
                Some(pow10_i256(scale_diff.unsigned_abs() as usize)?)
            };

            let mut out = Vec::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![i256::ZERO; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                let mut prefix_valid = true;
                for (i, row) in (*p_start..*p_end).enumerate() {
                    if value.is_null(row) {
                        prefix_sum[i + 1] = prefix_sum[i];
                        prefix_cnt[i + 1] = prefix_cnt[i];
                        continue;
                    }
                    let add = scalar_i256(value.as_ref(), row)?;
                    let Some(next) = prefix_sum[i].checked_add(add) else {
                        prefix_valid = false;
                        break;
                    };
                    prefix_sum[i + 1] = next;
                    prefix_cnt[i + 1] = prefix_cnt[i] + 1;
                }

                if prefix_valid {
                    for (frame_start, frame_end) in frames.iter().copied() {
                        let s = frame_start - *p_start;
                        let e = frame_end - *p_start;
                        let cnt = prefix_cnt[e] - prefix_cnt[s];
                        if cnt == 0 {
                            out.push(None);
                            continue;
                        }

                        let Some(mut sum) = prefix_sum[e].checked_sub(prefix_sum[s]) else {
                            out.push(None);
                            continue;
                        };

                        if let Some(factor) = factor {
                            if scale_diff > 0 {
                                let Some(scaled) = sum.checked_mul(factor) else {
                                    out.push(None);
                                    continue;
                                };
                                sum = scaled;
                            } else {
                                let Some(scaled) = sum.checked_div(factor) else {
                                    out.push(None);
                                    continue;
                                };
                                sum = scaled;
                            }
                        }

                        out.push(Some(sum));
                    }
                    continue;
                }

                // Fallback to per-frame accumulation when prefix accumulation overflows.
                for (frame_start, frame_end) in frames.into_iter() {
                    let mut cnt = 0i64;
                    let mut sum = i256::ZERO;
                    let mut overflowed = false;
                    for row in frame_start..frame_end {
                        if value.is_null(row) {
                            continue;
                        }
                        cnt += 1;
                        let add = scalar_i256(value.as_ref(), row)?;
                        let Some(next) = sum.checked_add(add) else {
                            overflowed = true;
                            break;
                        };
                        sum = next;
                    }
                    if cnt == 0 || overflowed {
                        out.push(None);
                        continue;
                    }
                    if let Some(factor) = factor {
                        if scale_diff > 0 {
                            let Some(scaled) = sum.checked_mul(factor) else {
                                out.push(None);
                                continue;
                            };
                            sum = scaled;
                        } else {
                            let Some(scaled) = sum.checked_div(factor) else {
                                out.push(None);
                                continue;
                            };
                            sum = scaled;
                        }
                    }
                    out.push(Some(sum));
                }
            }

            let array = Decimal256Array::from(out)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        other => Err(format!("unsupported sum return type: {:?}", other)),
    }
}

fn compute_avg(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    return_type: &DataType,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "avg missing value argument".to_string())?;
    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    match return_type {
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![0f64; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                for (i, row) in (*p_start..*p_end).enumerate() {
                    prefix_sum[i + 1] = prefix_sum[i]
                        + if value.is_null(row) {
                            0.0
                        } else {
                            scalar_f64(value.as_ref(), row)?
                        };
                    prefix_cnt[i + 1] = prefix_cnt[i] + if value.is_null(row) { 0 } else { 1 };
                }
                for (frame_start, frame_end) in frames.into_iter() {
                    let s = frame_start - *p_start;
                    let e = frame_end - *p_start;
                    let cnt = prefix_cnt[e] - prefix_cnt[s];
                    if cnt == 0 {
                        b.append_null();
                    } else {
                        let sum = prefix_sum[e] - prefix_sum[s];
                        b.append_value(sum / (cnt as f64));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Decimal128(out_precision, out_scale) => {
            let input_scale = decimal_input_scale(value.data_type())?;
            let scale_diff = (*out_scale as i32) - (input_scale as i32);
            let factor = if scale_diff == 0 {
                None
            } else {
                Some(pow10_i128(scale_diff.unsigned_abs() as usize)?)
            };

            let mut out = Vec::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![0i128; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                for (i, row) in (*p_start..*p_end).enumerate() {
                    prefix_sum[i + 1] = prefix_sum[i]
                        + if value.is_null(row) {
                            0
                        } else {
                            scalar_i128(value.as_ref(), row)?
                        };
                    prefix_cnt[i + 1] = prefix_cnt[i] + if value.is_null(row) { 0 } else { 1 };
                }
                for (frame_start, frame_end) in frames.into_iter() {
                    let s = frame_start - *p_start;
                    let e = frame_end - *p_start;
                    let cnt = prefix_cnt[e] - prefix_cnt[s];
                    if cnt == 0 {
                        out.push(None);
                        continue;
                    }
                    let mut sum = prefix_sum[e] - prefix_sum[s];
                    if let Some(factor) = factor {
                        if scale_diff > 0 {
                            sum = sum
                                .checked_mul(factor)
                                .ok_or_else(|| "decimal overflow".to_string())?;
                        } else {
                            sum /= factor;
                        }
                    }
                    let avg = div_round_i128(sum, cnt as i128);
                    out.push(Some(avg));
                }
            }

            let array = arrow::array::Decimal128Array::from(out)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(out_precision, out_scale) => {
            let input_scale = decimal_input_scale(value.data_type())?;
            let scale_diff = (*out_scale as i32) - (input_scale as i32);
            let factor = if scale_diff == 0 {
                None
            } else {
                Some(pow10_i256(scale_diff.unsigned_abs() as usize)?)
            };

            let mut out = Vec::with_capacity(total_rows);
            for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
                let peer_groups = &peer_groups_by_partition[part_idx];
                let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
                let mut prefix_sum = vec![i256::ZERO; (*p_end - *p_start) + 1];
                let mut prefix_cnt = vec![0i64; (*p_end - *p_start) + 1];
                let mut prefix_valid = true;
                for (i, row) in (*p_start..*p_end).enumerate() {
                    if value.is_null(row) {
                        prefix_sum[i + 1] = prefix_sum[i];
                        prefix_cnt[i + 1] = prefix_cnt[i];
                        continue;
                    }
                    let add = scalar_i256(value.as_ref(), row)?;
                    let Some(next) = prefix_sum[i].checked_add(add) else {
                        prefix_valid = false;
                        break;
                    };
                    prefix_sum[i + 1] = next;
                    prefix_cnt[i + 1] = prefix_cnt[i] + 1;
                }

                if prefix_valid {
                    for (frame_start, frame_end) in frames.iter().copied() {
                        let s = frame_start - *p_start;
                        let e = frame_end - *p_start;
                        let cnt = prefix_cnt[e] - prefix_cnt[s];
                        if cnt == 0 {
                            out.push(None);
                            continue;
                        }
                        let Some(mut sum) = prefix_sum[e].checked_sub(prefix_sum[s]) else {
                            out.push(None);
                            continue;
                        };
                        if let Some(factor) = factor {
                            if scale_diff > 0 {
                                let Some(scaled) = sum.checked_mul(factor) else {
                                    out.push(None);
                                    continue;
                                };
                                sum = scaled;
                            } else {
                                let Some(scaled) = sum.checked_div(factor) else {
                                    out.push(None);
                                    continue;
                                };
                                sum = scaled;
                            }
                        }
                        match div_round_i256(sum, i256::from_i128(cnt as i128)) {
                            Ok(avg) => out.push(Some(avg)),
                            Err(_) => out.push(None),
                        }
                    }
                    continue;
                }

                // Fallback to per-frame accumulation when prefix accumulation overflows.
                for (frame_start, frame_end) in frames.into_iter() {
                    let mut cnt = 0i64;
                    let mut sum = i256::ZERO;
                    let mut overflowed = false;
                    for row in frame_start..frame_end {
                        if value.is_null(row) {
                            continue;
                        }
                        cnt += 1;
                        let add = scalar_i256(value.as_ref(), row)?;
                        let Some(next) = sum.checked_add(add) else {
                            overflowed = true;
                            break;
                        };
                        sum = next;
                    }
                    if cnt == 0 || overflowed {
                        out.push(None);
                        continue;
                    }
                    if let Some(factor) = factor {
                        if scale_diff > 0 {
                            let Some(scaled) = sum.checked_mul(factor) else {
                                out.push(None);
                                continue;
                            };
                            sum = scaled;
                        } else {
                            let Some(scaled) = sum.checked_div(factor) else {
                                out.push(None);
                                continue;
                            };
                            sum = scaled;
                        }
                    }
                    match div_round_i256(sum, i256::from_i128(cnt as i128)) {
                        Ok(avg) => out.push(Some(avg)),
                        Err(_) => out.push(None),
                    }
                }
            }

            let array = Decimal256Array::from(out)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        other => Err(format!("unsupported avg return type: {:?}", other)),
    }
}

fn compute_variance_samp(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "variance_samp missing value argument".to_string())?;
    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut b = Float64Builder::with_capacity(total_rows);
    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        for (frame_start, frame_end) in frames {
            let mut n = 0f64;
            let mut mean = 0f64;
            let mut m2 = 0f64;
            for row in frame_start..frame_end {
                if value.is_null(row) {
                    continue;
                }
                let x = scalar_f64(value.as_ref(), row)?;
                n += 1.0;
                let delta = x - mean;
                mean += delta / n;
                m2 += delta * (x - mean);
            }
            if n <= 1.0 {
                b.append_null();
            } else {
                b.append_value(m2 / (n - 1.0));
            }
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_stddev_samp(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let variance = compute_variance_samp(args, partitions, order_keys, window, total_rows)?;
    let variance = variance
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| "stddev_samp internal type mismatch".to_string())?;
    let mut out = Float64Builder::with_capacity(total_rows);
    for row in 0..total_rows {
        if variance.is_null(row) {
            out.append_null();
            continue;
        }
        out.append_value(variance.value(row).sqrt());
    }
    Ok(Arc::new(out.finish()))
}

fn compute_bool_or(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .ok_or_else(|| "bool_or missing value argument".to_string())?;
    let value = value
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| "bool_or expects boolean input".to_string())?;

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut b = BooleanBuilder::with_capacity(total_rows);
    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        for (frame_start, frame_end) in frames {
            let mut out = false;
            for row in frame_start..frame_end {
                if !value.is_null(row) && value.value(row) {
                    out = true;
                    break;
                }
            }
            b.append_value(out);
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_covar_corr(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
    kind: &str,
) -> Result<ArrayRef, String> {
    let x = args
        .get(0)
        .ok_or_else(|| format!("{} missing first argument", kind))?;
    let y = args
        .get(1)
        .ok_or_else(|| format!("{} missing second argument", kind))?;

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut b = Float64Builder::with_capacity(total_rows);
    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        for (frame_start, frame_end) in frames {
            let mut n = 0f64;
            let mut mean_x = 0f64;
            let mut mean_y = 0f64;
            let mut c2 = 0f64;
            let mut m2x = 0f64;
            let mut m2y = 0f64;
            for row in frame_start..frame_end {
                if x.is_null(row) || y.is_null(row) {
                    continue;
                }
                let vx = scalar_f64(x.as_ref(), row)?;
                let vy = scalar_f64(y.as_ref(), row)?;
                n += 1.0;
                let dx = vx - mean_x;
                let dy = vy - mean_y;
                mean_x += dx / n;
                mean_y += dy / n;
                c2 += dx * (vy - mean_y);
                m2x += dx * (vx - mean_x);
                m2y += dy * (vy - mean_y);
            }

            let out = match kind {
                "covar_pop" => {
                    if n == 0.0 {
                        None
                    } else {
                        Some(c2 / n)
                    }
                }
                "covar_samp" => {
                    if n <= 1.0 {
                        None
                    } else {
                        Some(c2 / (n - 1.0))
                    }
                }
                "corr" => {
                    if n <= 1.0 {
                        None
                    } else {
                        let denom = (m2x * m2y).sqrt();
                        if denom == 0.0 { None } else { Some(c2 / denom) }
                    }
                }
                other => {
                    return Err(format!(
                        "unsupported covariance/correlation kind: {}",
                        other
                    ));
                }
            };
            if let Some(v) = out {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
    }
    Ok(Arc::new(b.finish()))
}

fn compute_window_array_agg(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
    return_type: &DataType,
    is_distinct: bool,
    is_asc_order: &[bool],
    nulls_first: &[bool],
) -> Result<ArrayRef, String> {
    if is_asc_order.len() != nulls_first.len() {
        return Err(format!(
            "array_agg window metadata length mismatch: is_asc_order={} nulls_first={}",
            is_asc_order.len(),
            nulls_first.len()
        ));
    }
    let input = args
        .first()
        .ok_or_else(|| "array_agg window function missing input argument".to_string())?;
    let DataType::List(item_field) = return_type else {
        return Err(format!(
            "array_agg window return type must be LIST, got {:?}",
            return_type
        ));
    };

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut offsets: Vec<i32> = Vec::with_capacity(total_rows + 1);
    offsets.push(0);
    let mut current_total: i64 = 0;
    let mut flat_values: Vec<Option<AggScalarValue>> = Vec::new();

    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        for (frame_start, frame_end) in frames {
            let mut rows: Vec<(Option<AggScalarValue>, Vec<Option<AggScalarValue>>)> =
                Vec::with_capacity(frame_end.saturating_sub(frame_start));
            for row_idx in frame_start..frame_end {
                let scalar = agg_scalar_from_array(input, row_idx)?;
                rows.push(split_window_array_agg_row(scalar, is_asc_order.len())?);
            }

            sort_window_array_agg_rows(&mut rows, is_asc_order, nulls_first)?;

            let mut seen = if is_distinct {
                Some(HashSet::<Vec<u8>>::new())
            } else {
                None
            };
            for (value, _order_values) in rows {
                if let Some(seen) = seen.as_mut() {
                    let encoded = encode_window_approx_top_k_optional_scalar(&value);
                    if !seen.insert(encoded) {
                        continue;
                    }
                }
                flat_values.push(value);
                current_total += 1;
                if current_total > i32::MAX as i64 {
                    return Err("array_agg window output offset overflow".to_string());
                }
            }
            offsets.push(current_total as i32);
        }
    }

    if offsets.len() != total_rows + 1 {
        return Err(format!(
            "array_agg window output size mismatch: offsets={} rows={}",
            offsets.len(),
            total_rows
        ));
    }

    let value_array = build_agg_scalar_array(item_field.data_type(), flat_values)?;
    let list_array = ListArray::new(
        item_field.clone(),
        OffsetBuffer::new(offsets.into()),
        value_array,
        None,
    );
    Ok(Arc::new(list_array))
}

fn split_window_array_agg_row(
    row_value: Option<AggScalarValue>,
    order_key_count: usize,
) -> Result<(Option<AggScalarValue>, Vec<Option<AggScalarValue>>), String> {
    match row_value {
        None => Ok((None, vec![None; order_key_count])),
        Some(AggScalarValue::Struct(fields)) => {
            if fields.is_empty() {
                return Err(
                    "array_agg packed input struct must contain at least one field".to_string(),
                );
            }
            if fields.len() < (1 + order_key_count) {
                return Err(format!(
                    "array_agg packed input field count mismatch: fields={} order_keys={}",
                    fields.len(),
                    order_key_count
                ));
            }
            let value = fields.first().cloned().unwrap_or(None);
            let order_values = fields
                .into_iter()
                .skip(1)
                .take(order_key_count)
                .collect::<Vec<_>>();
            Ok((value, order_values))
        }
        Some(value) => {
            if order_key_count > 0 {
                return Err(
                    "array_agg ORDER BY requires packed struct input with order keys".to_string(),
                );
            }
            Ok((Some(value), Vec::new()))
        }
    }
}

fn sort_window_array_agg_rows(
    rows: &mut [(Option<AggScalarValue>, Vec<Option<AggScalarValue>>)],
    is_asc_order: &[bool],
    nulls_first: &[bool],
) -> Result<(), String> {
    if is_asc_order.is_empty() {
        return Ok(());
    }
    let mut error: Option<String> = None;
    rows.sort_by(|(_left_value, left_keys), (_right_value, right_keys)| {
        if error.is_some() {
            return Ordering::Equal;
        }
        for key_idx in 0..is_asc_order.len() {
            let left = left_keys.get(key_idx).cloned().unwrap_or(None);
            let right = right_keys.get(key_idx).cloned().unwrap_or(None);
            match compare_window_array_agg_optional_values(
                &left,
                &right,
                is_asc_order[key_idx],
                nulls_first[key_idx],
            ) {
                Ok(Ordering::Equal) => continue,
                Ok(ord) => return ord,
                Err(err) => {
                    error = Some(err);
                    return Ordering::Equal;
                }
            }
        }
        Ordering::Equal
    });
    if let Some(err) = error {
        return Err(err);
    }
    Ok(())
}

fn compare_window_array_agg_optional_values(
    left: &Option<AggScalarValue>,
    right: &Option<AggScalarValue>,
    asc: bool,
    nulls_first: bool,
) -> Result<Ordering, String> {
    let ord = match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ord = compare_agg_scalar_values(left, right)?;
            if asc { ord } else { ord.reverse() }
        }
    };
    Ok(ord)
}

const WINDOW_APPROX_TOP_K_DEFAULT_K: usize = 5;
const WINDOW_APPROX_TOP_K_MAX_COUNTER_NUM: usize = 100_000;

fn compute_window_approx_top_k(
    _arena: &ExprArena,
    func: &WindowFunctionSpec,
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .first()
        .ok_or_else(|| "approx_top_k window function missing value argument".to_string())?;
    let k_array = args.get(1);
    let counter_num_array = args.get(2);

    let DataType::List(list_field) = &func.return_type else {
        return Err(format!(
            "approx_top_k window return type must be LIST<STRUCT>, got {:?}",
            func.return_type
        ));
    };
    let DataType::Struct(struct_fields) = list_field.data_type() else {
        return Err(format!(
            "approx_top_k window list element type must be STRUCT, got {:?}",
            list_field.data_type()
        ));
    };
    if struct_fields.len() < 2 {
        return Err("approx_top_k window output struct must have at least 2 fields".to_string());
    }

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut offsets: Vec<i32> = Vec::with_capacity(total_rows + 1);
    offsets.push(0);
    let mut current_total: i64 = 0;
    let mut items: Vec<Option<AggScalarValue>> = Vec::new();
    let mut counts: Vec<Option<AggScalarValue>> = Vec::new();
    let mut extras: Vec<Vec<Option<AggScalarValue>>> =
        (2..struct_fields.len()).map(|_| Vec::new()).collect();

    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        for (row, (frame_start, frame_end)) in (*p_start..*p_end).zip(frames.into_iter()) {
            let mut k = WINDOW_APPROX_TOP_K_DEFAULT_K;
            if let Some(k_arr) = k_array {
                let parsed = window_approx_top_k_scalar_to_i64(&agg_scalar_from_array(k_arr, row)?)
                    .and_then(window_approx_top_k_clamp_k);
                if let Some(v) = parsed {
                    k = v;
                }
            }

            let mut counter_num = window_approx_top_k_default_counter_num(k);
            if let Some(counter_arr) = counter_num_array {
                let parsed =
                    window_approx_top_k_scalar_to_i64(&agg_scalar_from_array(counter_arr, row)?)
                        .and_then(|v| window_approx_top_k_clamp_counter_num(v, k));
                if let Some(v) = parsed {
                    counter_num = v;
                }
            }
            let _ = counter_num;

            let mut freqs: HashMap<Vec<u8>, (Option<AggScalarValue>, i64)> = HashMap::new();
            for idx in frame_start..frame_end {
                let scalar = agg_scalar_from_array(value, idx)?;
                let key = encode_window_approx_top_k_optional_scalar(&scalar);
                let entry = freqs.entry(key).or_insert((scalar, 0));
                entry.1 += 1;
            }

            let mut top_entries: Vec<(Vec<u8>, (Option<AggScalarValue>, i64))> =
                freqs.into_iter().collect();
            top_entries.sort_by(
                |(left_key, (_, left_count)), (right_key, (_, right_count))| match right_count
                    .cmp(left_count)
                {
                    Ordering::Equal => left_key.cmp(right_key),
                    other => other,
                },
            );
            if top_entries.len() > k {
                top_entries.truncate(k);
            }

            current_total += top_entries.len() as i64;
            if current_total > i32::MAX as i64 {
                return Err("approx_top_k window output offset overflow".to_string());
            }
            offsets.push(current_total as i32);

            for (_key, (item, count)) in top_entries {
                items.push(item);
                counts.push(Some(AggScalarValue::Int64(count)));
                for extra in &mut extras {
                    extra.push(None);
                }
            }
        }
    }

    if offsets.len() != total_rows + 1 {
        return Err(format!(
            "approx_top_k window output size mismatch: offsets={} rows={}",
            offsets.len(),
            total_rows
        ));
    }

    let item_array = build_agg_scalar_array(struct_fields[0].data_type(), items)?;
    let count_array = build_agg_scalar_array(struct_fields[1].data_type(), counts)?;
    let mut struct_columns = vec![item_array, count_array];
    for (idx, field) in struct_fields.iter().enumerate().skip(2) {
        let values = std::mem::take(&mut extras[idx - 2]);
        if values.is_empty() {
            struct_columns.push(new_null_array(field.data_type(), 0));
        } else {
            struct_columns.push(build_agg_scalar_array(field.data_type(), values)?);
        }
    }
    let struct_array = StructArray::new(struct_fields.clone(), struct_columns, None);
    let list_array = ListArray::new(
        list_field.clone(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        None,
    );
    Ok(Arc::new(list_array))
}

fn window_approx_top_k_default_counter_num(k: usize) -> usize {
    (2 * k).max(100).min(WINDOW_APPROX_TOP_K_MAX_COUNTER_NUM)
}

fn window_approx_top_k_clamp_k(v: i64) -> Option<usize> {
    if v <= 0 {
        return None;
    }
    let v = usize::try_from(v).ok()?;
    if v == 0 || v > WINDOW_APPROX_TOP_K_MAX_COUNTER_NUM {
        return None;
    }
    Some(v)
}

fn window_approx_top_k_clamp_counter_num(v: i64, k: usize) -> Option<usize> {
    if v <= 0 {
        return None;
    }
    let v = usize::try_from(v).ok()?;
    if v == 0 || v > WINDOW_APPROX_TOP_K_MAX_COUNTER_NUM {
        return None;
    }
    Some(v.max(k))
}

fn window_approx_top_k_scalar_to_i64(value: &Option<AggScalarValue>) -> Option<i64> {
    match value {
        Some(AggScalarValue::Int64(v)) => Some(*v),
        Some(AggScalarValue::Float64(v)) => Some(*v as i64),
        Some(AggScalarValue::Decimal128(v)) => i64::try_from(*v).ok(),
        Some(AggScalarValue::Decimal256(v)) => v.to_string().parse::<i64>().ok(),
        _ => None,
    }
}

fn encode_window_approx_top_k_optional_scalar(value: &Option<AggScalarValue>) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        Some(v) => {
            out.push(1);
            encode_window_approx_top_k_scalar(&mut out, v);
        }
        None => out.push(0),
    }
    out
}

fn encode_window_approx_top_k_scalar(buf: &mut Vec<u8>, value: &AggScalarValue) {
    match value {
        AggScalarValue::Bool(v) => {
            buf.push(1);
            buf.push(*v as u8);
        }
        AggScalarValue::Int64(v) => {
            buf.push(2);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Float64(v) => {
            buf.push(3);
            buf.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        AggScalarValue::Utf8(v) => {
            buf.push(4);
            let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        AggScalarValue::Date32(v) => {
            buf.push(5);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Timestamp(v) => {
            buf.push(6);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal128(v) => {
            buf.push(7);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        AggScalarValue::Decimal256(v) => {
            buf.push(11);
            let text = v.to_string();
            let len = u32::try_from(text.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(text.as_bytes());
        }
        AggScalarValue::Struct(items) => {
            buf.push(8);
            let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&len.to_le_bytes());
            for item in items {
                match item {
                    Some(v) => {
                        buf.push(1);
                        encode_window_approx_top_k_scalar(buf, v);
                    }
                    None => buf.push(0),
                }
            }
        }
        AggScalarValue::Map(items) => {
            buf.push(9);
            let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&len.to_le_bytes());
            for (k, v) in items {
                match k {
                    Some(v) => {
                        buf.push(1);
                        encode_window_approx_top_k_scalar(buf, v);
                    }
                    None => buf.push(0),
                }
                match v {
                    Some(v) => {
                        buf.push(1);
                        encode_window_approx_top_k_scalar(buf, v);
                    }
                    None => buf.push(0),
                }
            }
        }
        AggScalarValue::List(items) => {
            buf.push(10);
            let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
            buf.extend_from_slice(&len.to_le_bytes());
            for item in items {
                match item {
                    Some(v) => {
                        buf.push(1);
                        encode_window_approx_top_k_scalar(buf, v);
                    }
                    None => buf.push(0),
                }
            }
        }
    }
}

fn decimal_input_scale(input_type: &DataType) -> Result<i8, String> {
    match input_type {
        DataType::Decimal128(_, scale) => Ok(*scale),
        DataType::Decimal256(_, scale) => Ok(*scale),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(0),
        other => Err(format!("unsupported decimal input type: {:?}", other)),
    }
}

fn compute_min_max(
    args: &[ArrayRef],
    partitions: &[(usize, usize)],
    order_keys: &[ArrayRef],
    window: Option<&WindowFrame>,
    is_min: bool,
    return_type: &DataType,
    total_rows: usize,
) -> Result<ArrayRef, String> {
    let value = args
        .get(0)
        .cloned()
        .ok_or_else(|| "min/max missing value argument".to_string())?;
    let value = if value.data_type() == return_type {
        value
    } else {
        let from_type = value.data_type().clone();
        cast(value.as_ref(), return_type).map_err(|e| {
            format!(
                "cast min/max argument from {:?} to {:?}: {}",
                from_type, return_type, e
            )
        })?
    };

    let peer_groups_by_partition: Vec<Vec<(usize, usize)>> = partitions
        .iter()
        .map(|(s, e)| compute_peer_groups(order_keys, *s, *e))
        .collect::<Result<_, _>>()?;

    let mut indices = UInt32Builder::with_capacity(total_rows);

    for (part_idx, (p_start, p_end)) in partitions.iter().enumerate() {
        let peer_groups = &peer_groups_by_partition[part_idx];
        let frames = compute_frames_for_partition(*p_start, *p_end, peer_groups, window)?;
        let idxs = compute_min_max_indices(value.as_ref(), *p_start, *p_end, &frames, is_min)?;
        for i in idxs {
            if let Some(v) = i {
                indices.append_value(v as u32);
            } else {
                indices.append_null();
            }
        }
    }

    let idx_arr = Arc::new(indices.finish()) as ArrayRef;
    let out = take(value.as_ref(), idx_arr.as_ref(), None).map_err(|e| e.to_string())?;
    Ok(out)
}

fn compute_min_max_indices(
    array: &dyn Array,
    part_start: usize,
    part_end: usize,
    frames: &[(usize, usize)],
    is_min: bool,
) -> Result<Vec<Option<usize>>, String> {
    let n = part_end - part_start;
    if frames.len() != n {
        return Err("frame size mismatch".to_string());
    }
    let mut out = Vec::with_capacity(n);

    // Fast path: if frame end is monotonic, use a deque-based sliding algorithm.
    let mut last_end = 0usize;
    for (_, end) in frames {
        if *end < last_end {
            last_end = usize::MAX;
            break;
        }
        last_end = *end;
    }
    if last_end == usize::MAX {
        for (start, end) in frames {
            out.push(scan_min_max(array, *start, *end, is_min)?);
        }
        return Ok(out);
    }

    let mut deque: VecDeque<usize> = VecDeque::new();
    let mut j = part_start;
    for (start, end) in frames {
        while j < *end {
            if !array.is_null(j) {
                while let Some(&back) = deque.back() {
                    let ord = compare_at(array, back, j)?;
                    let pop = if is_min { ord.is_gt() } else { ord.is_lt() };
                    if pop {
                        deque.pop_back();
                    } else {
                        break;
                    }
                }
                deque.push_back(j);
            }
            j += 1;
        }
        while let Some(&front) = deque.front() {
            if front < *start {
                deque.pop_front();
            } else {
                break;
            }
        }
        out.push(deque.front().copied());
    }
    Ok(out)
}

fn scan_min_max(
    array: &dyn Array,
    start: usize,
    end: usize,
    is_min: bool,
) -> Result<Option<usize>, String> {
    if start >= end {
        return Ok(None);
    }
    let mut best: Option<usize> = None;
    for i in start..end {
        if array.is_null(i) {
            continue;
        }
        match best {
            None => best = Some(i),
            Some(b) => {
                let ord = compare_at(array, b, i)?;
                if (is_min && ord.is_gt()) || (!is_min && ord.is_lt()) {
                    best = Some(i);
                }
            }
        }
    }
    Ok(best)
}

fn compare_at(array: &dyn Array, left: usize, right: usize) -> Result<std::cmp::Ordering, String> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "failed to downcast BooleanArray".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "failed to downcast Int8Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "failed to downcast Int16Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast Int32Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "failed to downcast Int64Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "failed to downcast Float32Array".to_string())?;
            Ok(cmp_f64(a.value(left) as f64, a.value(right) as f64))
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast Float64Array".to_string())?;
            Ok(cmp_f64(a.value(left), a.value(right)))
        }
        DataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampSecondArray".to_string())?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampMillisecondArray".to_string())?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampMicrosecondArray".to_string())?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                let a = array
                    .as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampNanosecondArray".to_string())?;
                Ok(a.value(left).cmp(&a.value(right)))
            }
        },
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| "failed to downcast StringArray".to_string())?;
            Ok(a.value(left).cmp(a.value(right)))
        }
        DataType::Date32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "failed to downcast Date32Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Decimal128(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "failed to downcast Decimal128Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        DataType::Decimal256(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .ok_or_else(|| "failed to downcast Decimal256Array".to_string())?;
            Ok(a.value(left).cmp(&a.value(right)))
        }
        other => Err(format!("unsupported type for min/max: {:?}", other)),
    }
}

fn cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
    }
}

fn compute_frames_for_partition(
    part_start: usize,
    part_end: usize,
    peer_groups: &[(usize, usize)],
    window: Option<&WindowFrame>,
) -> Result<Vec<(usize, usize)>, String> {
    let n = part_end - part_start;
    if n == 0 {
        return Ok(Vec::new());
    }
    let Some(w) = window else {
        return Ok(vec![(part_start, part_end); n]);
    };

    match w.window_type {
        WindowType::Range => {
            let end_is_current = matches!(w.end, Some(WindowBoundary::CurrentRow));
            if !end_is_current {
                return Ok(vec![(part_start, part_end); n]);
            }
            let mut out = Vec::with_capacity(n);
            for (g_start, g_end) in peer_groups {
                for _ in *g_start..*g_end {
                    out.push((part_start, *g_end));
                }
            }
            Ok(out)
        }
        WindowType::Rows => {
            let mut out = Vec::with_capacity(n);
            for row in part_start..part_end {
                let (start, end) = rows_frame_bounds(row, part_start, part_end, w)?;
                out.push((start, end));
            }
            Ok(out)
        }
    }
}

fn rows_frame_bounds(
    row: usize,
    part_start: usize,
    part_end: usize,
    w: &WindowFrame,
) -> Result<(usize, usize), String> {
    let start = match w.start.as_ref() {
        None => part_start,
        Some(WindowBoundary::CurrentRow) => row,
        Some(WindowBoundary::Preceding(n)) => {
            let n = *n;
            if n < 0 {
                return Err("window start PRECEDING offset must be non-negative".to_string());
            }
            let n = n as usize;
            row.saturating_sub(n).max(part_start)
        }
        Some(WindowBoundary::Following(n)) => {
            let n = *n;
            if n < 0 {
                return Err("window start FOLLOWING offset must be non-negative".to_string());
            }
            let n = n as usize;
            (row + n).min(part_end)
        }
    };

    let end_exclusive = match w.end.as_ref() {
        None => part_end,
        Some(WindowBoundary::CurrentRow) => (row + 1).min(part_end),
        Some(WindowBoundary::Preceding(n)) => {
            let n = *n;
            if n < 0 {
                return Err("window end PRECEDING offset must be non-negative".to_string());
            }
            let n = n as i64;
            let v = (row as i64) - n + 1;
            if v <= part_start as i64 {
                part_start
            } else {
                (v as usize).min(part_end)
            }
        }
        Some(WindowBoundary::Following(n)) => {
            let n = *n;
            if n < 0 {
                return Err("window end FOLLOWING offset must be non-negative".to_string());
            }
            let n = n as usize;
            (row + n + 1).min(part_end)
        }
    };

    if start >= end_exclusive {
        return Ok((end_exclusive, end_exclusive));
    }
    Ok((start, end_exclusive))
}

fn parse_int64_literal(arena: &ExprArena, id: ExprId) -> Result<i64, String> {
    let node = arena.node(id).ok_or_else(|| "invalid ExprId".to_string())?;
    match node {
        ExprNode::Literal(LiteralValue::Int64(v)) => Ok(*v),
        ExprNode::Literal(LiteralValue::Int32(v)) => Ok(*v as i64),
        other => Err(format!("expected integer literal, got {:?}", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;

    #[test]
    fn first_value_rewrite_fills_initial_rows() {
        let values = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;
        let order = Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef;

        let partitions = vec![(0usize, 4usize)];
        let window = WindowFrame {
            window_type: WindowType::Rows,
            start: None,
            end: Some(WindowBoundary::Preceding(2)),
        };

        let out = compute_first_value_rewrite(
            &[values.clone()],
            &partitions,
            &[order],
            Some(&window),
            false,
            -1,
            4,
        )
        .unwrap();
        let out_arr = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let actual: Vec<i32> = (0..out_arr.len()).map(|i| out_arr.value(i)).collect();
        assert_eq!(actual, vec![10, 10, 10, 20]);
    }
}

fn scalar_i64(array: &dyn Array, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "failed to downcast Int8Array".to_string())?;
            Ok(a.value(row) as i64)
        }
        DataType::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "failed to downcast Int16Array".to_string())?;
            Ok(a.value(row) as i64)
        }
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| "failed to downcast Int64Array".to_string())?;
            Ok(a.value(row))
        }
        DataType::Int32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .ok_or_else(|| "failed to downcast Int32Array".to_string())?;
            Ok(a.value(row) as i64)
        }
        other => Err(format!("unsupported i64 scalar type: {:?}", other)),
    }
}

fn scalar_i128(array: &dyn Array, row: usize) -> Result<i128, String> {
    match array.data_type() {
        DataType::Int8 => scalar_i64(array, row).map(|v| v as i128),
        DataType::Int16 => scalar_i64(array, row).map(|v| v as i128),
        DataType::Int64 => scalar_i64(array, row).map(|v| v as i128),
        DataType::Int32 => scalar_i64(array, row).map(|v| v as i128),
        DataType::Decimal128(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| "failed to downcast Decimal128Array".to_string())?;
            Ok(a.value(row))
        }
        other => Err(format!("unsupported i128 scalar type: {:?}", other)),
    }
}

fn scalar_i256(array: &dyn Array, row: usize) -> Result<i256, String> {
    match array.data_type() {
        DataType::Int8 => scalar_i64(array, row).map(|v| i256::from_i128(v as i128)),
        DataType::Int16 => scalar_i64(array, row).map(|v| i256::from_i128(v as i128)),
        DataType::Int64 => scalar_i64(array, row).map(|v| i256::from_i128(v as i128)),
        DataType::Int32 => scalar_i64(array, row).map(|v| i256::from_i128(v as i128)),
        DataType::Decimal128(_, _) => scalar_i128(array, row).map(i256::from_i128),
        DataType::Decimal256(_, _) => {
            let a = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "failed to downcast Decimal256Array".to_string())?;
            Ok(a.value(row))
        }
        other => Err(format!("unsupported i256 scalar type: {:?}", other)),
    }
}

fn scalar_f64(array: &dyn Array, row: usize) -> Result<f64, String> {
    match array.data_type() {
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .ok_or_else(|| "failed to downcast Float64Array".to_string())?;
            Ok(a.value(row))
        }
        DataType::Float32 => {
            let a = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "failed to downcast Float32Array".to_string())?;
            Ok(a.value(row) as f64)
        }
        DataType::Int8 => scalar_i64(array, row).map(|v| v as f64),
        DataType::Int16 => scalar_i64(array, row).map(|v| v as f64),
        DataType::Int64 => scalar_i64(array, row).map(|v| v as f64),
        DataType::Int32 => scalar_i64(array, row).map(|v| v as f64),
        other => Err(format!("unsupported f64 scalar type: {:?}", other)),
    }
}
