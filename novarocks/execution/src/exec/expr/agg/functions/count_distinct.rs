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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    Decimal256Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    Int64Builder, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::{MemTracker, process_mem_tracker};

use super::super::*;
use super::AggregateFunction;
use super::common::{AggScalarValue, scalar_from_array as scalar_from_any_array};

struct DistinctSet {
    allocator: AggregateAllocator,
    values: AggregateHashSet<AggregateVec<u8>>,
}

impl DistinctSet {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            values: aggregate_hash_set(allocator.clone()),
            allocator,
        }
    }

    fn insert(&mut self, value: Vec<u8>) -> Result<(), String> {
        if self
            .values
            .iter()
            .any(|existing| existing.as_slice() == value.as_slice())
        {
            return Ok(());
        }
        self.values
            .try_reserve(1)
            .map_err(|_| self.allocator.allocation_error("reserve distinct hash set"))?;
        let value = aggregate_bytes(self.allocator.clone(), &value)?;
        self.values.insert(value);
        Ok(())
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn iter(&self) -> impl Iterator<Item = &AggregateVec<u8>> {
        self.values.iter()
    }

    fn retained_bytes(&self) -> usize {
        0
    }
}

pub(super) struct CountDistinctAgg;

unsafe fn get_or_init_set<'a>(ptr: *mut u8) -> &'a mut DistinctSet {
    unsafe { &mut *ptr.cast::<DistinctSet>() }
}

fn encode_u8(v: u8) -> Vec<u8> {
    vec![v]
}

fn encode_le<T: Copy>(v: T) -> Vec<u8> {
    // Safety: We only use this for plain-old-data numeric scalars.
    unsafe {
        std::slice::from_raw_parts((&v as *const T) as *const u8, std::mem::size_of::<T>()).to_vec()
    }
}

fn encode_scalar_value(value: &Option<AggScalarValue>) -> Vec<u8> {
    fn encode_into(buf: &mut Vec<u8>, value: &AggScalarValue) {
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
            AggScalarValue::Binary(v) => {
                buf.push(12);
                let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(v);
            }
            AggScalarValue::Struct(items) => {
                buf.push(8);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v);
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
                            encode_into(buf, v);
                        }
                        None => buf.push(0),
                    }
                    match v {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v);
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
                            encode_into(buf, v);
                        }
                        None => buf.push(0),
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    match value {
        Some(v) => {
            out.push(1);
            encode_into(&mut out, v);
        }
        None => out.push(0),
    }
    out
}

fn struct_contains_null_field(value: &AggScalarValue) -> bool {
    match value {
        AggScalarValue::Struct(items) => items.iter().any(|item| item.is_none()),
        _ => false,
    }
}

fn serialize_set(set: &DistinctSet) -> Vec<u8> {
    let mut out = Vec::new();
    let count = set.len() as u32;
    out.extend_from_slice(&count.to_le_bytes());
    for v in set.iter() {
        let len = v.len() as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(v);
    }
    out
}

fn deserialize_set(bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if bytes.len() < 4 {
        return Err("invalid distinct set encoding".to_string());
    }
    let mut pos = 0usize;
    let count = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    let mut vals = Vec::with_capacity(count);
    for _ in 0..count {
        if pos + 4 > bytes.len() {
            return Err("invalid distinct set encoding".to_string());
        }
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            return Err("invalid distinct set encoding".to_string());
        }
        vals.push(bytes[pos..pos + len].to_vec());
        pos += len;
    }
    Ok(vals)
}

impl AggregateFunction for CountDistinctAgg {
    fn build_spec_from_type(
        &self,
        _func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        if input_type.is_none() {
            return Err("count_distinct requires 1 argument".to_string());
        }
        // multi_distinct_count is the FE-internal name for all COUNT(DISTINCT) operations.
        // Count all non-null distinct values regardless of sign.
        let kind = AggKind::CountDistinct;
        Ok(AggSpec {
            kind,
            output_type: DataType::Int64,
            intermediate_type: DataType::Binary,
            input_arg_type: None,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::CountDistinct => (
                std::mem::size_of::<DistinctSet>(),
                std::mem::align_of::<DistinctSet>(),
            ),
            other => unreachable!("unexpected kind for count_distinct: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_distinct input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "count_distinct intermediate input missing".to_string())?;
        let binary = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
        Ok(AggInputView::Binary(binary))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            ptr.cast::<DistinctSet>()
                .write(DistinctSet::new(process_mem_tracker()))
        };
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked count_distinct state requires a memory tracker".to_string()
        })?;
        unsafe { ptr.cast::<DistinctSet>().write(DistinctSet::new(tracker)) };
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe { ptr.cast::<DistinctSet>().drop_in_place() };
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        unsafe { (&*ptr.cast::<DistinctSet>()).retained_bytes() }
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("count_distinct batch input type mismatch".to_string());
        };
        match array.data_type() {
            DataType::Null => Ok(()),
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "failed to downcast to Int64Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row)))?;
                }
                Ok(())
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "failed to downcast to Int32Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row)))?;
                }
                Ok(())
            }
            DataType::Int16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .ok_or_else(|| "failed to downcast to Int16Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row)))?;
                }
                Ok(())
            }
            DataType::Int8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| "failed to downcast to Int8Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row)))?;
                }
                Ok(())
            }
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "failed to downcast to Float64Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row).to_bits()))?;
                }
                Ok(())
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| "failed to downcast to Float32Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row).to_bits()))?;
                }
                Ok(())
            }
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| "failed to downcast to BooleanArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_u8(arr.value(row) as u8))?;
                }
                Ok(())
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(arr.value(row).as_bytes().to_vec())?;
                }
                Ok(())
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| "failed to downcast to BinaryArray".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(arr.value(row).to_vec())?;
                }
                Ok(())
            }
            DataType::Date32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| "failed to downcast to Date32Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_le(arr.value(row)))?;
                }
                Ok(())
            }
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| "failed to downcast to TimestampSecondArray".to_string())?;
                    for (row, &base) in state_ptrs.iter().enumerate() {
                        if arr.is_null(row) {
                            continue;
                        }
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let set = unsafe { get_or_init_set(ptr) };
                        set.insert(encode_le(arr.value(row)))?;
                    }
                    Ok(())
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMillisecondArray".to_string()
                        })?;
                    for (row, &base) in state_ptrs.iter().enumerate() {
                        if arr.is_null(row) {
                            continue;
                        }
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let set = unsafe { get_or_init_set(ptr) };
                        set.insert(encode_le(arr.value(row)))?;
                    }
                    Ok(())
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampMicrosecondArray".to_string()
                        })?;
                    for (row, &base) in state_ptrs.iter().enumerate() {
                        if arr.is_null(row) {
                            continue;
                        }
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let set = unsafe { get_or_init_set(ptr) };
                        set.insert(encode_le(arr.value(row)))?;
                    }
                    Ok(())
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            "failed to downcast to TimestampNanosecondArray".to_string()
                        })?;
                    for (row, &base) in state_ptrs.iter().enumerate() {
                        if arr.is_null(row) {
                            continue;
                        }
                        let ptr = unsafe { (base as *mut u8).add(offset) };
                        let set = unsafe { get_or_init_set(ptr) };
                        set.insert(encode_le(arr.value(row)))?;
                    }
                    Ok(())
                }
            },
            DataType::Decimal128(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(arr.value(row).to_le_bytes().to_vec())?;
                }
                Ok(())
            }
            DataType::Decimal256(_, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .ok_or_else(|| "failed to downcast to Decimal256Array".to_string())?;
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(arr.value(row).to_le_bytes().to_vec())?;
                }
                Ok(())
            }
            DataType::List(_) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if array.is_null(row) {
                        continue;
                    }
                    let value = scalar_from_any_array(array, row)?;
                    let Some(value) = value else {
                        continue;
                    };

                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_scalar_value(&Some(value)))?;
                }
                Ok(())
            }
            DataType::Struct(_) => {
                for (row, &base) in state_ptrs.iter().enumerate() {
                    if array.is_null(row) {
                        continue;
                    }
                    let value = scalar_from_any_array(array, row)?;
                    let Some(value) = value else {
                        continue;
                    };
                    if struct_contains_null_field(&value) {
                        continue;
                    }

                    let ptr = unsafe { (base as *mut u8).add(offset) };
                    let set = unsafe { get_or_init_set(ptr) };
                    set.insert(encode_scalar_value(&Some(value)))?;
                }
                Ok(())
            }
            other => Err(format!(
                "unsupported count_distinct input type: {:?}",
                other
            )),
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(arr) = input else {
            return Err("count_distinct merge input type mismatch".to_string());
        };

        for (row, &base) in state_ptrs.iter().enumerate() {
            if arr.is_null(row) {
                continue;
            }
            let bytes = arr.value(row);
            let vals = deserialize_set(bytes)?;
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let set = unsafe { get_or_init_set(ptr) };
            for v in vals {
                set.insert(v)?;
            }
        }
        Ok(())
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let set = unsafe { &*ptr.cast::<DistinctSet>() };
                let bytes = serialize_set(set);
                builder.append_value(bytes);
            }
            return Ok(std::sync::Arc::new(builder.finish()));
        }

        let mut builder = Int64Builder::new();
        for &base in group_states {
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let count = unsafe { (&*ptr.cast::<DistinctSet>()).len() };
            builder.append_value(count as i64);
        }
        Ok(std::sync::Arc::new(builder.finish()))
    }
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;
    use std::sync::Arc;

    #[cfg(feature = "core-pipeline-integration")]
    use std::collections::HashMap;
    #[cfg(feature = "core-pipeline-integration")]
    use std::time::Duration;

    #[cfg(feature = "core-pipeline-integration")]
    use arrow::array::Int32Array;
    use arrow::array::{ArrayRef, Int64Array, ListArray, NullArray};
    use arrow::datatypes::{DataType, Int32Type};
    #[cfg(feature = "core-pipeline-integration")]
    use arrow::datatypes::{Field, Schema};
    #[cfg(feature = "core-pipeline-integration")]
    use arrow::record_batch::RecordBatch;

    use super::{AggregateFunction, CountDistinctAgg, DistinctSet, get_or_init_set};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
    use crate::exec::expr::agg::{AggInputView, AggStatePtr};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::expr::{ExprArena, ExprNode};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::node::aggregate::AggregateNode;
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::node::values::ValuesNode;
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::pipeline::binding::{ExchangeBindings, ScanBindings};
    #[cfg(feature = "core-pipeline-integration")]
    use crate::exec::pipeline::executor::execute_native_plan_with_pipeline;
    use crate::runtime::mem_tracker::MemTracker;
    #[cfg(feature = "core-pipeline-integration")]
    use crate::runtime::runtime_state::RuntimeState;
    #[cfg(feature = "core-pipeline-integration")]
    use novarocks_types::SlotId;

    #[cfg(feature = "core-pipeline-integration")]
    fn chunk_schema_of(schema: &Arc<Schema>, slot_ids: &[SlotId]) -> ChunkSchemaRef {
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), slot_ids)
            .expect("chunk schema")
    }

    #[test]
    fn count_distinct_supports_int_list_values() {
        let values = vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(2), Some(1)]),
            None,
        ];
        let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(values)) as ArrayRef;
        let func = AggFunction {
            name: "multi_distinct_count".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = CountDistinctAgg
            .build_spec_from_type(&func, Some(array.data_type()), false)
            .expect("count distinct spec");

        let mut state = MaybeUninit::<DistinctSet>::uninit();
        CountDistinctAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; array.len()];
        CountDistinctAgg
            .update_batch(&spec, 0, &state_ptrs, &AggInputView::Any(&array))
            .expect("update list values");
        let out = CountDistinctAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .expect("output");
        CountDistinctAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 2);
    }

    #[test]
    fn count_distinct_ignores_null_typed_input() {
        let array = Arc::new(NullArray::new(3)) as ArrayRef;
        let func = AggFunction {
            name: "multi_distinct_count".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(DataType::Binary),
                output_type: Some(DataType::Int64),
                input_arg_type: None,
            }),
            ..Default::default()
        };
        let spec = CountDistinctAgg
            .build_spec_from_type(&func, Some(array.data_type()), false)
            .expect("count distinct spec");

        let mut state = MaybeUninit::<DistinctSet>::uninit();
        CountDistinctAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; array.len()];
        CountDistinctAgg
            .update_batch(&spec, 0, &state_ptrs, &AggInputView::Any(&array))
            .expect("update null values");
        let out = CountDistinctAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .expect("output");
        CountDistinctAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 0);
    }

    #[cfg(feature = "core-pipeline-integration")]
    #[test]
    fn group_by_multi_distinct_count_is_correct_with_dop_2() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3, 3])) as arrow::array::ArrayRef;
        let vals =
            Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8, 9, 9])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };
        let output_chunk_schema = chunk_schema_of(
            &Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int32, false),
                Field::new("cnt", DataType::Int64, true),
            ])),
            &[SlotId::new(1), SlotId::new(2)],
        );

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![k],
                    functions: vec![AggFunction {
                        name: "multi_distinct_count".to_string(),
                        inputs: vec![v],
                        input_is_intermediate: false,
                        types: Some(AggTypeSignature {
                            intermediate_type: Some(DataType::Binary),
                            output_type: Some(DataType::Int64),
                            input_arg_type: None,
                        }),
                        ..Default::default()
                    }],
                    resolved_aggregates: vec![
                        crate::exec::expr::agg::test_builtin_execution_function_set()
                            .catalog()
                            .resolve_aggregate_trusted("multi_distinct_count", &[DataType::Int32])
                            .expect("resolved builtin aggregate"),
                    ],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_chunk_schema,
                    runtime_filter_spec: crate::exec::node::aggregate::AggregateRuntimeFilterSpec {
                        topn_producers: Vec::new(),
                    },
                    streaming_preaggregation_mode: None,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_native_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            ExchangeBindings::default(),
            ScanBindings::default(),
            None,
            None,
            2,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut out: HashMap<i32, i64> = HashMap::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            assert_eq!(chunk.columns().len(), 2);
            let k_arr = chunk
                .columns()
                .first()
                .expect("k column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("k Int32");
            let v_arr = chunk
                .columns()
                .get(1)
                .expect("distinct count column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("distinct count Int64");
            for i in 0..chunk.len() {
                out.insert(k_arr.value(i), v_arr.value(i));
            }
        }

        assert_eq!(out.get(&1).copied(), Some(2));
        assert_eq!(out.get(&2).copied(), Some(1));
        assert_eq!(out.get(&3).copied(), Some(3));
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn retained_bytes_track_unique_key_capacity_and_drop_clears_slot() {
        let mut slot = MaybeUninit::<DistinctSet>::uninit();
        let ptr = slot.as_mut_ptr().cast::<u8>();
        let tracker = MemTracker::new_root("count-distinct-test");
        unsafe {
            ptr.cast::<DistinctSet>()
                .write(DistinctSet::new(Arc::clone(&tracker)))
        };
        let state = unsafe { get_or_init_set(ptr) };
        state.insert(Vec::with_capacity(32)).unwrap();
        let first = tracker.current();
        assert!(first > 0);

        state.insert(Vec::with_capacity(32)).unwrap();
        assert_eq!(state.values.len(), 1);
        assert_eq!(tracker.current(), first);

        let func = CountDistinctAgg;
        let spec = func
            .build_spec_from_type(&AggFunction::default(), Some(&DataType::Int64), false)
            .expect("build spec");
        assert_eq!(func.retained_bytes(&spec, ptr), 0);
        func.drop_state(&spec, ptr);
        assert_eq!(tracker.current(), 0);
    }
}
