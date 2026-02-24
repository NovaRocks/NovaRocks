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
    ArrayRef, BooleanArray, Int32Array, Int32Builder, Int64Array, Int64Builder, ListArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::exec::node::aggregate::AggFunction;

use super::super::*;
use super::AggregateFunction;
use chrono::{Datelike, NaiveDate};
use std::sync::Arc;

const UNIX_EPOCH_DAY_OFFSET: i32 = 719163;

fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
}

fn julian_from_date(date: NaiveDate) -> i32 {
    let y = date.year();
    let m = date.month() as i32;
    let d = date.day() as i32;
    let a = (14 - m) / 12;
    let y = y + 4800 - a;
    let m = m + 12 * a - 3;
    d + ((153 * m + 2) / 5) + 365 * y + y / 4 - y / 100 + y / 400 - 32045
}

pub(super) struct WindowFunnelAgg;

const MODE_DEDUPLICATION: i32 = 1;
const MODE_FIXED: i32 = 2;
const MODE_DEDUPLICATION_FIXED: i32 = 3;
const MODE_INCREASE: i32 = 4;

#[derive(Clone, Debug)]
struct TimestampPair {
    start_timestamp: i64,
    last_timestamp: i64,
}

#[derive(Clone, Debug)]
struct WindowFunnelState {
    window_size: Option<i64>,
    mode: Option<i32>,
    events_size: Option<usize>,
    sorted: bool,
    events_list: Vec<(i64, u8)>,
}

impl Default for WindowFunnelState {
    fn default() -> Self {
        Self {
            window_size: None,
            mode: None,
            events_size: None,
            sorted: true,
            events_list: Vec::new(),
        }
    }
}

impl WindowFunnelState {
    fn ensure_params(&mut self, window_size: i64, mode: i32) -> Result<(), String> {
        if let Some(prev) = self.window_size {
            if prev != window_size {
                return Err("window_funnel window_size must be constant".to_string());
            }
        } else {
            self.window_size = Some(window_size);
        }

        if let Some(prev) = self.mode {
            if prev != mode {
                return Err("window_funnel mode must be constant".to_string());
            }
        } else {
            self.mode = Some(mode);
        }
        Ok(())
    }

    fn ensure_events_size(&mut self, size: usize) -> Result<(), String> {
        if let Some(prev) = self.events_size {
            if prev != size {
                return Err("window_funnel event size must be constant".to_string());
            }
        } else {
            self.events_size = Some(size);
        }
        Ok(())
    }

    fn update_event(&mut self, timestamp: i64, event_level: u8) {
        if !self.events_list.is_empty() && event_level == 0 {
            return;
        }
        if self.sorted && !self.events_list.is_empty() {
            let (last_ts, last_level) = *self.events_list.last().unwrap();
            if last_ts == timestamp {
                self.sorted = last_level <= event_level;
            } else {
                self.sorted = last_ts <= timestamp;
            }
        }
        self.events_list.push((timestamp, event_level));
    }

    fn sort_events(&mut self) {
        self.events_list.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
        self.sorted = true;
    }

    fn deserialize_and_merge(&mut self, data: &[i64]) -> Result<(), String> {
        if data.is_empty() {
            return Ok(());
        }
        if data.len() < 2 {
            return Err("window_funnel merge input is malformed".to_string());
        }
        let events_size = data[0] as usize;
        let other_sorted = data[1] != 0;
        self.ensure_events_size(events_size)?;

        let mut other_list = Vec::new();
        let mut idx = 2;
        while idx + 1 < data.len() {
            let ts = data[idx];
            let level = data[idx + 1] as u8;
            other_list.push((ts, level));
            idx += 2;
        }

        self.events_list.extend(other_list);
        if !self.sorted || !other_sorted {
            self.sort_events();
        } else {
            self.sorted = true;
        }
        Ok(())
    }

    fn serialize_to_vec(&self) -> Vec<i64> {
        if self.events_list.is_empty() {
            return Vec::new();
        }
        let events_size = self.events_size.unwrap_or(0) as i64;
        let mut out = Vec::with_capacity(self.events_list.len() * 2 + 2);
        out.push(events_size);
        out.push(if self.sorted { 1 } else { 0 });
        for (ts, level) in &self.events_list {
            out.push(*ts);
            out.push(*level as i64);
        }
        out
    }

    fn get_event_level(&mut self) -> i32 {
        let Some(events_size) = self.events_size else {
            return 0;
        };
        if events_size == 0 {
            return 0;
        }
        if !self.sorted {
            self.sort_events();
        }
        let window_size = self.window_size.unwrap_or(0);
        let mut mode = self.mode.unwrap_or(0);
        let increase = (mode & MODE_INCREASE) != 0;
        mode &= MODE_DEDUPLICATION_FIXED;

        let mut events_timestamp = vec![
            TimestampPair {
                start_timestamp: -1,
                last_timestamp: -1,
            };
            events_size
        ];

        if mode == 0 {
            for (timestamp, level) in &self.events_list {
                if *level == 0 {
                    continue;
                }
                let event_idx = (*level as i32) - 1;
                if event_idx == 0 {
                    events_timestamp[0].start_timestamp = *timestamp;
                    events_timestamp[0].last_timestamp = *timestamp;
                } else if events_timestamp[(event_idx - 1) as usize].start_timestamp >= 0 {
                    let mut matched = *timestamp
                        <= events_timestamp[(event_idx - 1) as usize].start_timestamp + window_size;
                    if increase {
                        matched &=
                            events_timestamp[(event_idx - 1) as usize].last_timestamp < *timestamp;
                    }
                    if matched {
                        events_timestamp[event_idx as usize].start_timestamp =
                            events_timestamp[(event_idx - 1) as usize].start_timestamp;
                        events_timestamp[event_idx as usize].last_timestamp = *timestamp;
                        if (event_idx + 1) as usize == events_size {
                            return events_size as i32;
                        }
                    }
                }
            }

            for event in (1..=events_size).rev() {
                if events_timestamp[event - 1].start_timestamp >= 0 {
                    return event as i32;
                }
            }
            return 0;
        }

        let mut max_level: i32 = -1;
        let mut curr_level: i32 = -1;

        match mode {
            MODE_DEDUPLICATION => {
                for (timestamp, level) in &self.events_list {
                    if *level == 0 {
                        continue;
                    }
                    let event_idx = (*level as i32) - 1;
                    if event_idx == 0 {
                        events_timestamp[0].start_timestamp = *timestamp;
                        if event_idx > curr_level {
                            curr_level = event_idx;
                        }
                    } else if events_timestamp[event_idx as usize].start_timestamp >= 0 {
                        if curr_level > max_level {
                            max_level = curr_level;
                        }
                        eliminate_last_event_chains(&mut curr_level, &mut events_timestamp);
                    } else if events_timestamp[(event_idx - 1) as usize].start_timestamp >= 0 {
                        if promote_to_next_level(
                            &mut events_timestamp,
                            *timestamp,
                            event_idx,
                            &mut curr_level,
                            window_size,
                            increase,
                            events_size,
                        ) {
                            return events_size as i32;
                        }
                    }
                }
            }
            MODE_FIXED => {
                let mut first_event = false;
                for (timestamp, level) in &self.events_list {
                    if *level == 0 {
                        continue;
                    }
                    let event_idx = (*level as i32) - 1;
                    if event_idx == 0 {
                        events_timestamp[0].start_timestamp = *timestamp;
                        if event_idx > curr_level {
                            curr_level = event_idx;
                        }
                        first_event = true;
                    } else if first_event
                        && events_timestamp[(event_idx - 1) as usize].start_timestamp < 0
                    {
                        if curr_level >= 0 {
                            if curr_level > max_level {
                                max_level = curr_level;
                            }
                            eliminate_last_event_chains(&mut curr_level, &mut events_timestamp);
                        }
                    } else if events_timestamp[(event_idx - 1) as usize].start_timestamp >= 0 {
                        if promote_to_next_level(
                            &mut events_timestamp,
                            *timestamp,
                            event_idx,
                            &mut curr_level,
                            window_size,
                            increase,
                            events_size,
                        ) {
                            return events_size as i32;
                        }
                    }
                }
            }
            MODE_DEDUPLICATION_FIXED => {
                let mut first_event = false;
                for (timestamp, level) in &self.events_list {
                    if *level == 0 {
                        continue;
                    }
                    let event_idx = (*level as i32) - 1;
                    if event_idx == 0 {
                        events_timestamp[0].start_timestamp = *timestamp;
                        if event_idx > curr_level {
                            curr_level = event_idx;
                        }
                        first_event = true;
                    } else if events_timestamp[event_idx as usize].start_timestamp >= 0 {
                        if curr_level > max_level {
                            max_level = curr_level;
                        }
                        eliminate_last_event_chains(&mut curr_level, &mut events_timestamp);
                    } else if first_event
                        && events_timestamp[(event_idx - 1) as usize].start_timestamp < 0
                    {
                        if curr_level >= 0 {
                            if curr_level > max_level {
                                max_level = curr_level;
                            }
                            eliminate_last_event_chains(&mut curr_level, &mut events_timestamp);
                        }
                    } else if events_timestamp[(event_idx - 1) as usize].start_timestamp >= 0 {
                        if promote_to_next_level(
                            &mut events_timestamp,
                            *timestamp,
                            event_idx,
                            &mut curr_level,
                            window_size,
                            increase,
                            events_size,
                        ) {
                            return events_size as i32;
                        }
                    }
                }
            }
            _ => {}
        }

        if curr_level > max_level {
            curr_level + 1
        } else {
            max_level + 1
        }
    }
}

fn eliminate_last_event_chains(curr_level: &mut i32, events_timestamp: &mut [TimestampPair]) {
    while *curr_level >= 0 {
        events_timestamp[*curr_level as usize].start_timestamp = -1;
        *curr_level -= 1;
    }
}

fn promote_to_next_level(
    events_timestamp: &mut [TimestampPair],
    timestamp: i64,
    event_idx: i32,
    curr_level: &mut i32,
    window_size: i64,
    increase: bool,
    events_size: usize,
) -> bool {
    let first_timestamp = events_timestamp[(event_idx - 1) as usize].start_timestamp;
    let mut matched = timestamp <= first_timestamp + window_size;
    if increase {
        matched &= events_timestamp[(event_idx - 1) as usize].last_timestamp < timestamp;
    }

    if matched {
        events_timestamp[event_idx as usize].start_timestamp = first_timestamp;
        events_timestamp[event_idx as usize].last_timestamp = timestamp;
        if event_idx > *curr_level {
            *curr_level = event_idx;
        }
        if (event_idx + 1) as usize == events_size {
            return true;
        }
    }
    false
}

impl AggregateFunction for WindowFunnelAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let sig = super::super::agg_type_signature(func)
            .ok_or_else(|| "window_funnel type signature missing".to_string())?;
        let output_type = sig
            .output_type
            .as_ref()
            .ok_or_else(|| "window_funnel output type missing".to_string())?;
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .ok_or_else(|| "window_funnel intermediate type missing".to_string())?;

        if input_is_intermediate {
            return Ok(AggSpec {
                kind: AggKind::WindowFunnel,
                output_type: output_type.clone(),
                intermediate_type,
                input_arg_type: sig.input_arg_type.clone(),
                count_all: false,
            });
        }

        let input_type =
            input_type.ok_or_else(|| "window_funnel input type missing".to_string())?;
        match input_type {
            DataType::Struct(fields) => {
                if fields.len() != 4 {
                    return Err("window_funnel expects 4 arguments".to_string());
                }
                let events_type = fields[3].data_type();
                if !matches!(events_type, DataType::List(_)) {
                    return Err("window_funnel expects array<bool> events".to_string());
                }
            }
            other => {
                return Err(format!(
                    "window_funnel expects struct input, got {:?}",
                    other
                ));
            }
        }

        Ok(AggSpec {
            kind: AggKind::WindowFunnel,
            output_type: output_type.clone(),
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, _kind: &AggKind) -> (usize, usize) {
        (
            std::mem::size_of::<WindowFunnelState>(),
            std::mem::align_of::<WindowFunnelState>(),
        )
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "window_funnel input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "window_funnel merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::write(ptr as *mut WindowFunnelState, WindowFunnelState::default());
        }
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut WindowFunnelState);
        }
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("window_funnel input type mismatch".to_string());
        };
        let struct_arr = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| "window_funnel expects struct input".to_string())?;
        if struct_arr.num_columns() != 4 {
            return Err("window_funnel expects 4 arguments".to_string());
        }

        let window_arr = struct_arr.column(0);
        let ts_arr = struct_arr.column(1);
        let mode_arr = struct_arr.column(2);
        let events_arr = struct_arr.column(3);

        let window_view = IntArrayView::new(window_arr)?;
        let mode_view = IntArrayView::new(mode_arr)?;
        let list_arr = events_arr
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "window_funnel expects list events".to_string())?;

        let values = list_arr.values();
        let bool_arr = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "window_funnel events must be boolean".to_string())?;

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut WindowFunnelState) };

            let window_size = window_view
                .value_at(row)
                .ok_or_else(|| "window_funnel window_size is null".to_string())?;
            let mode = mode_view
                .value_at(row)
                .ok_or_else(|| "window_funnel mode is null".to_string())?;
            state.ensure_params(window_size, mode as i32)?;

            let timestamp = match timestamp_value(ts_arr, row)? {
                Some(v) => v,
                None => continue,
            };

            if list_arr.is_null(row) {
                continue;
            }

            let offsets = list_arr.value_offsets();
            let offset = offsets[row] as usize;
            let length = (offsets[row + 1] - offsets[row]) as usize;
            state.ensure_events_size(length)?;

            for i in 0..length {
                let idx = offset + i;
                if bool_arr.is_null(idx) {
                    continue;
                }
                if bool_arr.value(idx) {
                    state.update_event(timestamp, (i + 1) as u8);
                }
            }
        }

        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("window_funnel merge input type mismatch".to_string());
        };

        if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
            if struct_arr.num_columns() != 3 {
                return Err("window_funnel merge expects 3 arguments".to_string());
            }
            let list_arr = struct_arr
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "window_funnel merge expects list input".to_string())?;
            let window_view = IntArrayView::new(struct_arr.column(1))?;
            let mode_view = IntArrayView::new(struct_arr.column(2))?;
            merge_from_list(
                list_arr,
                Some(&window_view),
                Some(&mode_view),
                offset,
                state_ptrs,
            )?;
            return Ok(());
        }

        let list_arr = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "window_funnel merge expects list input".to_string())?;
        merge_from_list(list_arr, None, None, offset, state_ptrs)
    }

    fn build_array(
        &self,
        _spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = arrow::array::ListBuilder::new(Int64Builder::new());
            for &base in group_states {
                let state =
                    unsafe { &*((base as *mut u8).add(offset) as *const WindowFunnelState) };
                let serialized = state.serialize_to_vec();
                for v in serialized {
                    builder.values().append_value(v);
                }
                builder.append(true);
            }
            return Ok(Arc::new(builder.finish()));
        }

        let mut builder = Int32Builder::new();
        for &base in group_states {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut WindowFunnelState) };
            builder.append_value(state.get_event_level());
        }
        Ok(Arc::new(builder.finish()))
    }
}

fn merge_from_list(
    list_arr: &ListArray,
    window_view: Option<&IntArrayView<'_>>,
    mode_view: Option<&IntArrayView<'_>>,
    offset: usize,
    state_ptrs: &[AggStatePtr],
) -> Result<(), String> {
    let values = list_arr.values();
    let int_values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "window_funnel merge expects int64 list".to_string())?;

    for (row, &base) in state_ptrs.iter().enumerate() {
        let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut WindowFunnelState) };

        if list_arr.is_null(row) {
            continue;
        }

        if let (Some(window_view), Some(mode_view)) = (window_view, mode_view) {
            if let Some(window_size) = window_view.value_at(row) {
                if let Some(mode) = mode_view.value_at(row) {
                    state.ensure_params(window_size, mode as i32)?;
                }
            }
        } else if state.window_size.is_none() || state.mode.is_none() {
            return Err("window_funnel merge missing window_size/mode".to_string());
        }

        let offsets = list_arr.value_offsets();
        let offset = offsets[row] as usize;
        let length = (offsets[row + 1] - offsets[row]) as usize;
        if length == 0 {
            continue;
        }
        let mut data = Vec::with_capacity(length);
        for i in 0..length {
            let idx = offset + i;
            if int_values.is_null(idx) {
                continue;
            }
            data.push(int_values.value(idx));
        }
        state.deserialize_and_merge(&data)?;
    }
    Ok(())
}

fn timestamp_value(array: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast Int64Array".to_string())?;
            Ok((!arr.is_null(row)).then(|| arr.value(row)))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast Int32Array".to_string())?;
            Ok((!arr.is_null(row)).then(|| arr.value(row) as i64))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "failed to downcast Date32Array".to_string())?;
            if arr.is_null(row) {
                return Ok(None);
            }
            let date = date32_to_naive(arr.value(row))
                .ok_or_else(|| "invalid date32 value".to_string())?;
            Ok(Some(julian_from_date(date) as i64))
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampSecondArray".to_string())?;
                Ok((!arr.is_null(row)).then(|| arr.value(row)))
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampMillisecondArray".to_string())?;
                Ok((!arr.is_null(row)).then(|| arr.value(row) / 1_000))
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampMicrosecondArray".to_string())?;
                Ok((!arr.is_null(row)).then(|| arr.value(row) / 1_000_000))
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| "failed to downcast TimestampNanosecondArray".to_string())?;
                Ok((!arr.is_null(row)).then(|| arr.value(row) / 1_000_000_000))
            }
        },
        other => Err(format!(
            "window_funnel unsupported timestamp type: {:?}",
            other
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanBuilder, ListBuilder, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use std::mem::MaybeUninit;

    #[test]
    fn test_window_funnel_simple() {
        let window_size = Arc::new(Int64Array::from(vec![1800])) as ArrayRef;
        let ts = chrono::NaiveDate::from_ymd_opt(2022, 6, 10)
            .unwrap()
            .and_hms_opt(12, 30, 30)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        let ts_arr = Arc::new(TimestampMicrosecondArray::from(vec![Some(ts)])) as ArrayRef;
        let mode_arr = Arc::new(Int32Array::from(vec![2])) as ArrayRef;

        let mut list_builder = ListBuilder::new(BooleanBuilder::new());
        list_builder.values().append_value(true);
        list_builder.values().append_value(true);
        list_builder.append(true);
        let events_arr = Arc::new(list_builder.finish()) as ArrayRef;

        let fields = vec![
            Field::new("f0", DataType::Int64, true),
            Field::new("f1", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("f2", DataType::Int32, true),
            Field::new(
                "f3",
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                true,
            ),
        ];
        let struct_type = DataType::Struct(Fields::from(fields.clone()));
        let struct_arr = StructArray::new(
            fields.into(),
            vec![window_size, ts_arr, mode_arr, events_arr],
            None,
        );
        let array_ref = Arc::new(struct_arr) as ArrayRef;

        let func = AggFunction {
            name: "window_funnel".to_string(),
            inputs: vec![],
            input_is_intermediate: false,
            types: Some(crate::exec::node::aggregate::AggTypeSignature {
                intermediate_type: Some(DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Int64,
                    true,
                )))),
                output_type: Some(DataType::Int32),
                input_arg_type: Some(DataType::Int64),
            }),
        };

        let spec = WindowFunnelAgg
            .build_spec_from_type(&func, Some(&struct_type), false)
            .unwrap();

        let mut state = MaybeUninit::<WindowFunnelState>::uninit();
        WindowFunnelAgg.init_state(&spec, state.as_mut_ptr() as *mut u8);
        let state_ptr = state.as_mut_ptr() as AggStatePtr;
        let state_ptrs = vec![state_ptr; 1];
        let input = AggInputView::Any(&array_ref);

        WindowFunnelAgg
            .update_batch(&spec, 0, &state_ptrs, &input)
            .unwrap();
        let out = WindowFunnelAgg
            .build_array(&spec, 0, &[state_ptr], false)
            .unwrap();
        WindowFunnelAgg.drop_state(&spec, state.as_mut_ptr() as *mut u8);

        let out_arr = out.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(out_arr.value(0), 2);
    }
}
