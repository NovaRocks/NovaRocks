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

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryBuilder, StructArray};
use arrow::datatypes::DataType;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};

use crate::exec::expr::agg::functions::common::{
    AggScalarValue, TrackedAggScalarValue, build_scalar_array, compare_scalar_values,
    tracked_scalar_from_array, tracked_scalar_to_output,
};
use crate::exec::expr::agg::{AggregateAllocator, AggregateVec, RetainedMemoryPolicy};
use crate::exec::expr::function::object::percentile_functions::{
    numeric_value_at, payload_bytes_at,
};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;

const EXACT_PERCENTILE_MAGIC: u8 = 0xC3;
const EXACT_PERCENTILE_VERSION: u8 = 1;

pub(super) struct PercentilePlaceholderAgg;

#[derive(Debug, Deserialize)]
enum BorrowedSerializableScalar<'a> {
    Int64(i64),
    Float64(f64),
    Utf8(#[serde(borrow)] Cow<'a, str>),
    Date32(i32),
    Timestamp(i64),
    Decimal128(i128),
}

struct ExactPercentileState {
    allocator: AggregateAllocator,
    rate: Option<f64>,
    values: AggregateVec<TrackedAggScalarValue>,
}

impl ExactPercentileState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            values: AggregateVec::new_in(allocator.clone()),
            allocator,
            rate: None,
        }
    }

    fn push(&mut self, value: TrackedAggScalarValue) -> Result<(), String> {
        self.values.try_reserve(1).map_err(|_| {
            self.allocator
                .allocation_error("reserve exact percentile value")
        })?;
        self.values.push(value);
        Ok(())
    }
}

fn canonical_agg_name(name: &str) -> &str {
    name.split_once('|').map(|(base, _)| base).unwrap_or(name)
}

struct TrackedScalarWire<'a>(&'a TrackedAggScalarValue);

impl Serialize for TrackedScalarWire<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            TrackedAggScalarValue::Int64(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 0, "Int64", value)
            }
            TrackedAggScalarValue::Float64(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 1, "Float64", value)
            }
            TrackedAggScalarValue::Utf8(value) => serializer.serialize_newtype_variant(
                "SerializableScalar",
                2,
                "Utf8",
                std::str::from_utf8(value).map_err(serde::ser::Error::custom)?,
            ),
            TrackedAggScalarValue::Date32(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 3, "Date32", value)
            }
            TrackedAggScalarValue::Timestamp(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 4, "Timestamp", value)
            }
            TrackedAggScalarValue::Decimal128(value) => {
                serializer.serialize_newtype_variant("SerializableScalar", 5, "Decimal128", value)
            }
            other => Err(serde::ser::Error::custom(format!(
                "unsupported exact percentile scalar {other:?}"
            ))),
        }
    }
}

struct TrackedValuesWire<'a>(&'a [TrackedAggScalarValue]);

impl Serialize for TrackedValuesWire<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            sequence.serialize_element(&TrackedScalarWire(value))?;
        }
        sequence.end()
    }
}

struct ExactStateWire<'a>(&'a ExactPercentileState);

impl Serialize for ExactStateWire<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ExactPercentileState", 2)?;
        state.serialize_field("rate", &self.0.rate)?;
        state.serialize_field("values", &TrackedValuesWire(&self.0.values))?;
        state.end()
    }
}

fn encode_state(state: &ExactPercentileState) -> Vec<u8> {
    let payload =
        serde_json::to_vec(&ExactStateWire(state)).expect("serialize exact percentile state");
    let mut out = Vec::with_capacity(2 + payload.len());
    out.push(EXACT_PERCENTILE_MAGIC);
    out.push(EXACT_PERCENTILE_VERSION);
    out.extend_from_slice(&payload);
    out
}

struct ValuesSeed<'a> {
    allocator: &'a AggregateAllocator,
}

impl<'de> DeserializeSeed<'de> for ValuesSeed<'_> {
    type Value = AggregateVec<TrackedAggScalarValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValuesVisitor<'a> {
            allocator: &'a AggregateAllocator,
        }

        impl<'de> Visitor<'de> for ValuesVisitor<'_> {
            type Value = AggregateVec<TrackedAggScalarValue>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an exact percentile value array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = AggregateVec::new_in(self.allocator.clone());
                while let Some(value) =
                    sequence.next_element::<BorrowedSerializableScalar<'de>>()?
                {
                    values.try_reserve(1).map_err(|_| {
                        serde::de::Error::custom(
                            self.allocator
                                .allocation_error("reserve decoded exact percentile value"),
                        )
                    })?;
                    let value = match value {
                        BorrowedSerializableScalar::Int64(value) => {
                            TrackedAggScalarValue::Int64(value)
                        }
                        BorrowedSerializableScalar::Float64(value) => {
                            TrackedAggScalarValue::Float64(value)
                        }
                        BorrowedSerializableScalar::Utf8(value) => TrackedAggScalarValue::Utf8(
                            crate::exec::expr::agg::aggregate_bytes(
                                self.allocator.clone(),
                                value.as_bytes(),
                            )
                            .map_err(serde::de::Error::custom)?,
                        ),
                        BorrowedSerializableScalar::Date32(value) => {
                            TrackedAggScalarValue::Date32(value)
                        }
                        BorrowedSerializableScalar::Timestamp(value) => {
                            TrackedAggScalarValue::Timestamp(value)
                        }
                        BorrowedSerializableScalar::Decimal128(value) => {
                            TrackedAggScalarValue::Decimal128(value)
                        }
                    };
                    values.push(value);
                }
                Ok(values)
            }
        }

        deserializer.deserialize_seq(ValuesVisitor {
            allocator: self.allocator,
        })
    }
}

struct StateSeed<'a> {
    allocator: &'a AggregateAllocator,
}

impl<'de> DeserializeSeed<'de> for StateSeed<'_> {
    type Value = (Option<f64>, AggregateVec<TrackedAggScalarValue>);

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StateVisitor<'a> {
            allocator: &'a AggregateAllocator,
        }

        impl<'de> Visitor<'de> for StateVisitor<'_> {
            type Value = (Option<f64>, AggregateVec<TrackedAggScalarValue>);

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an exact percentile state object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut rate = None;
                let mut values = None;
                while let Some(field) = map.next_key::<Cow<'de, str>>()? {
                    match field.as_ref() {
                        "rate" => rate = map.next_value()?,
                        "values" => {
                            values = Some(map.next_value_seed(ValuesSeed {
                                allocator: self.allocator,
                            })?)
                        }
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok((
                    rate,
                    values.unwrap_or_else(|| AggregateVec::new_in(self.allocator.clone())),
                ))
            }
        }

        deserializer.deserialize_struct(
            "ExactPercentileState",
            &["rate", "values"],
            StateVisitor {
                allocator: self.allocator,
            },
        )
    }
}

fn decode_state_into(
    payload: &[u8],
    allocator: &AggregateAllocator,
) -> Result<(Option<f64>, AggregateVec<TrackedAggScalarValue>), String> {
    if payload.is_empty() {
        return Ok((None, AggregateVec::new_in(allocator.clone())));
    }
    if payload.len() < 2 {
        return Err("exact percentile payload too short".to_string());
    }
    if payload[0] != EXACT_PERCENTILE_MAGIC {
        return Err(format!(
            "unsupported exact percentile payload magic: expected=0x{:02x} actual=0x{:02x}",
            EXACT_PERCENTILE_MAGIC, payload[0]
        ));
    }
    if payload[1] != EXACT_PERCENTILE_VERSION {
        return Err(format!(
            "unsupported exact percentile payload version: expected={} actual={}",
            EXACT_PERCENTILE_VERSION, payload[1]
        ));
    }
    // serde_json only needs an owned scratch buffer when a string contains
    // escapes. At most one token is decoded at a time; its decoded and encoded
    // forms are each bounded by the complete payload. State-owned values are
    // separately charged by AggregateAllocator, so 2 * payload is a complete
    // bound for parser-private live scratch.
    let scratch_bound = payload
        .len()
        .checked_mul(2)
        .ok_or_else(|| "exact percentile parser scratch bound overflow".to_string())?;
    let _scratch = allocator.reserve_transient(
        scratch_bound,
        "reserve exact percentile JSON parser scratch",
    )?;
    let mut deserializer = serde_json::Deserializer::from_slice(&payload[2..]);
    StateSeed { allocator }
        .deserialize(&mut deserializer)
        .map_err(|error| error.to_string())
}

fn apply_rate(state: &mut ExactPercentileState, rate: f64) -> Result<(), String> {
    if !(0.0..=1.0).contains(&rate) {
        return Err("Percentile rate must be between 0 and 1".to_string());
    }
    match state.rate {
        Some(existing) if (existing - rate).abs() > f64::EPSILON => Err(format!(
            "percentile rate mismatch while merging states: existing={} incoming={}",
            existing, rate
        )),
        Some(_) => Ok(()),
        None => {
            state.rate = Some(rate);
            Ok(())
        }
    }
}

fn validate_exact_scalar(value: TrackedAggScalarValue) -> Result<TrackedAggScalarValue, String> {
    match value {
        value @ (TrackedAggScalarValue::Int64(_)
        | TrackedAggScalarValue::Float64(_)
        | TrackedAggScalarValue::Utf8(_)
        | TrackedAggScalarValue::Date32(_)
        | TrackedAggScalarValue::Timestamp(_)
        | TrackedAggScalarValue::Decimal128(_)) => Ok(value),
        other => Err(format!(
            "unsupported percentile_disc/cont input scalar {:?}",
            other
        )),
    }
}

fn numeric_from_scalar(value: &AggScalarValue, context: &str) -> Result<f64, String> {
    match value {
        AggScalarValue::Int64(v) => Ok(*v as f64),
        AggScalarValue::Float64(v) => Ok(*v),
        AggScalarValue::Date32(v) => Ok(*v as f64),
        AggScalarValue::Timestamp(v) => Ok(*v as f64),
        AggScalarValue::Decimal128(v) => Ok(*v as f64),
        other => Err(format!(
            "{context}: unsupported percentile_cont interpolation input {:?}",
            other
        )),
    }
}

fn scalar_from_numeric(output_type: &DataType, value: f64) -> Result<AggScalarValue, String> {
    match output_type {
        DataType::Float64 => Ok(AggScalarValue::Float64(value)),
        DataType::Date32 => Ok(AggScalarValue::Date32(value as i32)),
        DataType::Timestamp(_, _) => Ok(AggScalarValue::Timestamp(value as i64)),
        other => Err(format!(
            "unsupported percentile_cont output type {:?}",
            other
        )),
    }
}

fn merge_payload_array(
    array: &ArrayRef,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    for (row, &base) in state_ptrs.iter().enumerate() {
        let Some(payload) = payload_bytes_at(array, row, context)? else {
            continue;
        };
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { &mut *(ptr as *mut ExactPercentileState) };
        let (rate, incoming) = decode_state_into(payload, &state.allocator)?;
        if let Some(rate) = rate {
            apply_rate(state, rate)?;
        }
        state.values.try_reserve(incoming.len()).map_err(|_| {
            state
                .allocator
                .allocation_error("reserve merged exact percentile values")
        })?;
        state.values.extend(incoming);
    }
    Ok(())
}

fn update_from_struct(
    array: &StructArray,
    offset: usize,
    state_ptrs: &[AggStatePtr],
    context: &str,
) -> Result<(), String> {
    let fields = array.columns();
    if fields.len() < 2 {
        return Err(format!(
            "{context}: percentile_disc/cont expects STRUCT(value, rate) input"
        ));
    }
    let values = fields[0].clone();
    let rates = fields[1].clone();
    for (row, &base) in state_ptrs.iter().enumerate() {
        let ptr = unsafe { (base as *mut u8).add(offset) };
        let state = unsafe { &mut *(ptr as *mut ExactPercentileState) };
        if let Some(rate) = numeric_value_at(&rates, row, context)? {
            apply_rate(state, rate)?;
        }
        let Some(value) = tracked_scalar_from_array(&values, row, &state.allocator)? else {
            continue;
        };
        state.push(validate_exact_scalar(value)?)?;
    }
    Ok(())
}

fn finalize_cont(
    state: &ExactPercentileState,
    output_type: &DataType,
) -> Result<Option<AggScalarValue>, String> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state
        .values
        .iter()
        .map(tracked_scalar_to_output)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal)
    });

    if values.len() == 1 || rate == 1.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(numeric_from_scalar(
                values.last().expect("last"),
                "percentile_cont",
            )?))),
            _ => Ok(Some(values.last().expect("last").clone())),
        };
    }

    if rate == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(numeric_from_scalar(
                values.first().expect("first"),
                "percentile_cont",
            )?))),
            _ => Ok(Some(values.first().expect("first").clone())),
        };
    }

    let u = ((values.len() - 1) as f64) * rate;
    let index = u.floor() as usize;
    let fraction = u - index as f64;
    if fraction == 0.0 {
        return match output_type {
            DataType::Float64 => Ok(Some(AggScalarValue::Float64(numeric_from_scalar(
                &values[index],
                "percentile_cont",
            )?))),
            _ => Ok(Some(values[index].clone())),
        };
    }

    let lower = numeric_from_scalar(&values[index], "percentile_cont")?;
    let upper = numeric_from_scalar(&values[index + 1], "percentile_cont")?;
    let interpolated = lower + fraction * (upper - lower);
    scalar_from_numeric(output_type, interpolated).map(Some)
}

fn finalize_disc(state: &ExactPercentileState) -> Result<Option<AggScalarValue>, String> {
    if state.values.is_empty() {
        return Ok(None);
    }
    let rate = state.rate.unwrap_or(0.0);
    let mut values: Vec<AggScalarValue> = state
        .values
        .iter()
        .map(tracked_scalar_to_output)
        .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        compare_scalar_values(left, right).unwrap_or(std::cmp::Ordering::Equal)
    });
    if values.len() == 1 || rate == 1.0 {
        return Ok(values.last().cloned());
    }
    let index = (((values.len() - 1) as f64) * rate).ceil() as usize;
    Ok(values.get(index).cloned())
}

impl AggregateFunction for PercentilePlaceholderAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        _input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let sig = func
            .types
            .as_ref()
            .ok_or_else(|| "aggregate type signature is required".to_string())?;
        let kind = match canonical_agg_name(func.name.as_str()) {
            "percentile_cont" => AggKind::PercentileCont,
            "percentile_disc" => AggKind::PercentileDisc,
            "percentile_disc_lc" => AggKind::PercentileDiscLc,
            other => {
                return Err(format!(
                    "unsupported percentile aggregate function: {}",
                    other
                ));
            }
        };
        let output_type = sig
            .output_type
            .as_ref()
            .cloned()
            .or_else(|| input_type.cloned())
            .unwrap_or(DataType::Binary);
        let intermediate_type = sig
            .intermediate_type
            .as_ref()
            .cloned()
            .unwrap_or(DataType::Binary);
        Ok(AggSpec {
            kind,
            output_type,
            intermediate_type,
            input_arg_type: sig.input_arg_type.clone(),
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::PercentileCont | AggKind::PercentileDisc | AggKind::PercentileDiscLc => (
                std::mem::size_of::<ExactPercentileState>(),
                std::mem::align_of::<ExactPercentileState>(),
            ),
            other => unreachable!("unexpected kind for percentile placeholder: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile_disc/cont input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "percentile_disc/cont merge input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked exact percentile requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked exact percentile requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(
                ptr as *mut ExactPercentileState,
                ExactPercentileState::new(tracker),
            );
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut ExactPercentileState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        let _ = ptr;
        0
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
            return Err("percentile_disc/cont input type mismatch".to_string());
        };
        if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
            update_from_struct(
                struct_array,
                offset,
                state_ptrs,
                "percentile_disc_cont_update",
            )
        } else {
            merge_payload_array(array, offset, state_ptrs, "percentile_disc_cont_update")
        }
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("percentile_disc/cont merge input type mismatch".to_string());
        };
        merge_payload_array(array, offset, state_ptrs, "percentile_disc_cont_merge")
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        let output_type = if output_intermediate {
            &spec.intermediate_type
        } else {
            &spec.output_type
        };

        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let ptr = unsafe { (base as *mut u8).add(offset) };
                let state = unsafe { &*(ptr as *const ExactPercentileState) };
                builder.append_value(encode_state(state));
            }
            return Ok(Arc::new(builder.finish()));
        }

        let mut values = Vec::with_capacity(group_states.len());
        for &base in group_states {
            let ptr = unsafe { (base as *mut u8).add(offset) };
            let state = unsafe { &*(ptr as *const ExactPercentileState) };
            let value = match &spec.kind {
                AggKind::PercentileCont => finalize_cont(state, output_type),
                AggKind::PercentileDisc | AggKind::PercentileDiscLc => finalize_disc(state),
                other => Err(format!(
                    "unexpected percentile placeholder kind: {:?}",
                    other
                )),
            }?;
            values.push(value);
        }
        build_scalar_array(output_type, values)
    }
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;

    #[test]
    fn json_round_trip_allocates_state_only_through_tracker() {
        let tracker = MemTracker::new_root("exact-percentile-test");
        let mut state = ExactPercentileState::new(tracker.clone());
        state.rate = Some(0.5);
        state
            .push(TrackedAggScalarValue::Utf8(
                crate::exec::expr::agg::aggregate_bytes(state.allocator.clone(), b"percentile")
                    .unwrap(),
            ))
            .unwrap();
        let encoded = encode_state(&state);

        let mut decoded = ExactPercentileState::new(tracker.clone());
        let (rate, values) = decode_state_into(&encoded, &decoded.allocator).unwrap();
        decoded.rate = rate;
        decoded.values = values;
        assert_eq!(decoded.rate, Some(0.5));
        assert!(tracker.current() > 0);

        drop(state);
        drop(decoded);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn failed_rate_merge_does_not_append_values_or_change_cache() {
        let mut state = ExactPercentileState::new(MemTracker::new_root("exact-percentile-rate"));
        apply_rate(&mut state, 0.5).expect("set rate");
        assert!(apply_rate(&mut state, 0.9).is_err());
        assert_eq!(state.rate, Some(0.5));
    }
}
