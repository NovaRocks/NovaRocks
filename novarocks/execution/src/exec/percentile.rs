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

use allocator_api2::alloc::{Allocator, Global};
use allocator_api2::vec::Vec as AllocVec;
use serde::{Deserialize, Serialize};

const PERCENTILE_STATE_MAGIC: u8 = 0xA2;
const PERCENTILE_STATE_VERSION: u8 = 4;
const QUANTILE_KIND_NONE: u8 = 0;
const QUANTILE_KIND_SCALAR: u8 = 1;
const QUANTILE_KIND_ARRAY: u8 = 2;
const HEADER_LEN: usize = 11;
const QUANTILE_TOLERANCE: f64 = 1e-12;

pub const MIN_COMPRESSION: f64 = 2048.0;
pub const MAX_COMPRESSION: f64 = 10000.0;
pub const DEFAULT_COMPRESSION_FACTOR: usize = 10000;
pub const MAX_QUANTILE_COUNT: usize = 4096;
const MAX_DECODED_PROCESSED: usize = 40_000;
const MAX_DECODED_UNPROCESSED: usize = 160_000;
const MAX_DECODED_CUMULATIVE: usize = MAX_DECODED_PROCESSED + 1;

#[derive(Clone, Debug, PartialEq)]
pub enum QuantileSpec<A: Allocator + Clone = Global> {
    Scalar(f64),
    Array(AllocVec<f64, A>),
}

#[derive(Clone, Debug)]
pub struct PercentileState<A: Allocator + Clone = Global> {
    allocator: A,
    pub digest: TDigest<A>,
    pub quantiles: Option<QuantileSpec<A>>,
    pub compression: usize,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Centroid {
    mean: f32,
    weight: f32,
}

impl Centroid {
    fn new(mean: f32, weight: f32) -> Self {
        Self { mean, weight }
    }

    fn add(&mut self, other: &Centroid) {
        if self.weight != 0.0 {
            self.weight += other.weight;
            self.mean += other.weight * (other.mean - self.mean) / self.weight;
        } else {
            self.weight = other.weight;
            self.mean = other.mean;
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SerializableTDigest {
    compression: f32,
    min: f32,
    max: f32,
    max_processed: usize,
    max_unprocessed: usize,
    processed_weight: f32,
    unprocessed_weight: f32,
    processed: Vec<Centroid>,
    unprocessed: Vec<Centroid>,
    cumulative: Vec<f32>,
}

#[derive(Clone, Debug)]
pub struct TDigest<A: Allocator + Clone = Global> {
    allocator: A,
    compression: f32,
    min: f32,
    max: f32,
    max_processed: usize,
    max_unprocessed: usize,
    processed_weight: f32,
    unprocessed_weight: f32,
    processed: AllocVec<Centroid, A>,
    unprocessed: AllocVec<Centroid, A>,
    cumulative: AllocVec<f32, A>,
}

impl<A: Allocator + Clone> TDigest<A> {
    fn new_in(compression: f32, allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            compression,
            min: f32::MAX,
            max: f32::MIN,
            max_processed: (2.0 * compression.ceil()) as usize,
            max_unprocessed: (8.0 * compression.ceil()) as usize,
            processed_weight: 0.0,
            unprocessed_weight: 0.0,
            processed: AllocVec::new_in(allocator.clone()),
            unprocessed: AllocVec::new_in(allocator.clone()),
            cumulative: AllocVec::new_in(allocator),
        }
    }

    fn is_empty(&self) -> bool {
        self.processed.is_empty() && self.unprocessed.is_empty()
    }

    fn total_weight(&self) -> f32 {
        self.processed_weight + self.unprocessed_weight
    }

    fn try_clone(&self) -> Result<Self, String> {
        Ok(Self {
            allocator: self.allocator.clone(),
            compression: self.compression,
            min: self.min,
            max: self.max,
            max_processed: self.max_processed,
            max_unprocessed: self.max_unprocessed,
            processed_weight: self.processed_weight,
            unprocessed_weight: self.unprocessed_weight,
            processed: try_copy_slice_in(
                &self.processed,
                self.allocator.clone(),
                "TDigest processed centroids",
            )?,
            unprocessed: try_copy_slice_in(
                &self.unprocessed,
                self.allocator.clone(),
                "TDigest unprocessed centroids",
            )?,
            cumulative: try_copy_slice_in(
                &self.cumulative,
                self.allocator.clone(),
                "TDigest cumulative weights",
            )?,
        })
    }

    pub fn count(&self) -> f32 {
        self.total_weight()
    }

    fn add(&mut self, value: f32, weight: f32) -> Result<(), String> {
        if value.is_nan() || weight <= 0.0 {
            return Ok(());
        }
        self.unprocessed
            .try_reserve(1)
            .map_err(|_| "ResourceExhausted: reserve TDigest centroid".to_string())?;
        self.unprocessed.push(Centroid::new(value, weight));
        self.unprocessed_weight += weight;
        self.process_if_necessary()
    }

    fn merge(&mut self, other: &TDigest<A>) -> Result<(), String> {
        if other.is_empty() {
            return Ok(());
        }
        if !other.processed.is_empty() {
            self.processed_weight += other.processed_weight;
            self.processed =
                merge_sorted_centroids(&self.processed, &other.processed, self.allocator.clone())?;
            if let Some(first) = self.processed.first() {
                self.min = self.min.min(first.mean);
            }
            if let Some(last) = self.processed.last() {
                self.max = self.max.max(last.mean);
            }
        }
        if !other.unprocessed.is_empty() {
            self.unprocessed
                .try_reserve(other.unprocessed.len())
                .map_err(|_| "ResourceExhausted: reserve merged TDigest centroids".to_string())?;
            self.unprocessed.extend_from_slice(&other.unprocessed);
            self.unprocessed_weight += other.unprocessed_weight;
        }
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.process_if_necessary()?;
        self.update_cumulative()
    }

    fn quantile(&mut self, q: f32) -> Result<Option<f32>, String> {
        if !(0.0..=1.0).contains(&q) {
            return Ok(None);
        }
        if self.have_unprocessed() || self.is_dirty() {
            self.process()?;
        }
        Ok(self.quantile_processed(q))
    }

    fn serialize_binary(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            4 * std::mem::size_of::<f32>()
                + 2 * std::mem::size_of::<u64>()
                + 3 * std::mem::size_of::<u32>()
                + self.processed.len() * 2 * std::mem::size_of::<f32>()
                + self.unprocessed.len() * 2 * std::mem::size_of::<f32>()
                + self.cumulative.len() * std::mem::size_of::<f32>(),
        );
        out.extend_from_slice(&self.compression.to_le_bytes());
        out.extend_from_slice(&self.min.to_le_bytes());
        out.extend_from_slice(&self.max.to_le_bytes());
        out.extend_from_slice(&(self.max_processed as u64).to_le_bytes());
        out.extend_from_slice(&(self.max_unprocessed as u64).to_le_bytes());
        out.extend_from_slice(&self.processed_weight.to_le_bytes());
        out.extend_from_slice(&self.unprocessed_weight.to_le_bytes());
        out.extend_from_slice(&(self.processed.len() as u32).to_le_bytes());
        for centroid in &self.processed {
            out.extend_from_slice(&centroid.mean.to_le_bytes());
            out.extend_from_slice(&centroid.weight.to_le_bytes());
        }
        out.extend_from_slice(&(self.unprocessed.len() as u32).to_le_bytes());
        for centroid in &self.unprocessed {
            out.extend_from_slice(&centroid.mean.to_le_bytes());
            out.extend_from_slice(&centroid.weight.to_le_bytes());
        }
        out.extend_from_slice(&(self.cumulative.len() as u32).to_le_bytes());
        for value in &self.cumulative {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn deserialize_binary_in(payload: &[u8], allocator: A) -> Result<Self, String> {
        let mut offset = 0usize;
        let compression = read_f32(payload, &mut offset, "tdigest compression")?;
        let min = read_f32(payload, &mut offset, "tdigest min")?;
        let max = read_f32(payload, &mut offset, "tdigest max")?;
        let max_processed = read_u64(payload, &mut offset, "tdigest max_processed")? as usize;
        let max_unprocessed = read_u64(payload, &mut offset, "tdigest max_unprocessed")? as usize;
        let processed_weight = read_f32(payload, &mut offset, "tdigest processed_weight")?;
        let unprocessed_weight = read_f32(payload, &mut offset, "tdigest unprocessed_weight")?;

        if !compression.is_finite() || compression <= 0.0 || compression > MAX_COMPRESSION as f32 {
            return Err(format!("tdigest compression out of bounds: {compression}"));
        }
        if max_processed > MAX_DECODED_PROCESSED {
            return Err(format!(
                "tdigest max_processed {max_processed} exceeds {MAX_DECODED_PROCESSED}"
            ));
        }
        if max_unprocessed > MAX_DECODED_UNPROCESSED {
            return Err(format!(
                "tdigest max_unprocessed {max_unprocessed} exceeds {MAX_DECODED_UNPROCESSED}"
            ));
        }

        let processed_len = read_u32(payload, &mut offset, "tdigest processed len")? as usize;
        if processed_len > MAX_DECODED_PROCESSED {
            return Err(format!(
                "tdigest processed length {processed_len} exceeds {MAX_DECODED_PROCESSED}"
            ));
        }
        let mut processed = AllocVec::new_in(allocator.clone());
        processed.try_reserve_exact(processed_len).map_err(|_| {
            "ResourceExhausted: reserve decoded TDigest processed centroids".to_string()
        })?;
        for _ in 0..processed_len {
            processed.push(Centroid::new(
                read_f32(payload, &mut offset, "tdigest processed mean")?,
                read_f32(payload, &mut offset, "tdigest processed weight")?,
            ));
        }

        let unprocessed_len = read_u32(payload, &mut offset, "tdigest unprocessed len")? as usize;
        if unprocessed_len > MAX_DECODED_UNPROCESSED {
            return Err(format!(
                "tdigest unprocessed length {unprocessed_len} exceeds {MAX_DECODED_UNPROCESSED}"
            ));
        }
        let mut unprocessed = AllocVec::new_in(allocator.clone());
        unprocessed
            .try_reserve_exact(unprocessed_len)
            .map_err(|_| {
                "ResourceExhausted: reserve decoded TDigest unprocessed centroids".to_string()
            })?;
        for _ in 0..unprocessed_len {
            unprocessed.push(Centroid::new(
                read_f32(payload, &mut offset, "tdigest unprocessed mean")?,
                read_f32(payload, &mut offset, "tdigest unprocessed weight")?,
            ));
        }

        let cumulative_len = read_u32(payload, &mut offset, "tdigest cumulative len")? as usize;
        if cumulative_len > MAX_DECODED_CUMULATIVE {
            return Err(format!(
                "tdigest cumulative length {cumulative_len} exceeds {MAX_DECODED_CUMULATIVE}"
            ));
        }
        let mut cumulative = AllocVec::new_in(allocator.clone());
        cumulative.try_reserve_exact(cumulative_len).map_err(|_| {
            "ResourceExhausted: reserve decoded TDigest cumulative weights".to_string()
        })?;
        for _ in 0..cumulative_len {
            cumulative.push(read_f32(payload, &mut offset, "tdigest cumulative value")?);
        }
        if offset != payload.len() {
            return Err(format!(
                "tdigest payload has trailing bytes: consumed={} total={}",
                offset,
                payload.len()
            ));
        }
        Ok(Self {
            allocator,
            compression,
            min,
            max,
            max_processed,
            max_unprocessed,
            processed_weight,
            unprocessed_weight,
            processed,
            unprocessed,
            cumulative,
        })
    }

    fn have_unprocessed(&self) -> bool {
        !self.unprocessed.is_empty()
    }

    fn is_dirty(&self) -> bool {
        self.processed.len() > self.max_processed || self.unprocessed.len() > self.max_unprocessed
    }

    fn process_if_necessary(&mut self) -> Result<(), String> {
        if self.is_dirty() {
            self.process()?;
        }
        Ok(())
    }

    fn process(&mut self) -> Result<(), String> {
        if self.unprocessed.is_empty() && self.processed.is_empty() {
            return Ok(());
        }

        self.unprocessed.sort_by(|left, right| {
            left.mean
                .partial_cmp(&right.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let processed = std::mem::replace(
            &mut self.processed,
            AllocVec::new_in(self.allocator.clone()),
        );
        if !processed.is_empty() {
            let merged_capacity = self
                .unprocessed
                .len()
                .checked_add(processed.len())
                .ok_or_else(|| "TDigest merged centroid length overflow".to_string())?;
            let mut merged = AllocVec::new_in(self.allocator.clone());
            merged
                .try_reserve_exact(merged_capacity)
                .map_err(|_| "ResourceExhausted: reserve sorted TDigest merge".to_string())?;
            let mut left_idx = 0usize;
            let mut right_idx = 0usize;
            while left_idx < self.unprocessed.len() && right_idx < processed.len() {
                if self.unprocessed[left_idx].mean <= processed[right_idx].mean {
                    merged.push(self.unprocessed[left_idx]);
                    left_idx += 1;
                } else {
                    merged.push(processed[right_idx]);
                    right_idx += 1;
                }
            }
            merged.extend_from_slice(&self.unprocessed[left_idx..]);
            merged.extend_from_slice(&processed[right_idx..]);
            self.unprocessed = merged;
        }

        self.processed_weight += self.unprocessed_weight;
        self.unprocessed_weight = 0.0;

        let Some(first) = self.unprocessed.first().copied() else {
            return Ok(());
        };
        let mut processed = AllocVec::new_in(self.allocator.clone());
        processed
            .try_reserve_exact(self.max_processed.max(1))
            .map_err(|_| "ResourceExhausted: reserve processed TDigest centroids".to_string())?;
        self.processed = processed;
        self.processed.push(first);
        let mut w_so_far = first.weight;
        let mut w_limit = self.processed_weight * self.integrated_q(1.0);

        for centroid in self.unprocessed.iter().skip(1).copied() {
            let projected = w_so_far + centroid.weight;
            if projected <= w_limit {
                w_so_far = projected;
                self.processed
                    .last_mut()
                    .expect("processed has first centroid")
                    .add(&centroid);
            } else {
                let k1 = self.integrated_location(w_so_far / self.processed_weight);
                w_limit = self.processed_weight * self.integrated_q(k1 + 1.0);
                w_so_far += centroid.weight;
                self.processed.try_reserve(1).map_err(|_| {
                    "ResourceExhausted: grow processed TDigest centroids".to_string()
                })?;
                self.processed.push(centroid);
            }
        }

        self.unprocessed.clear();
        self.min = self
            .min
            .min(self.processed.first().map(|c| c.mean).unwrap_or(self.min));
        self.max = self
            .max
            .max(self.processed.last().map(|c| c.mean).unwrap_or(self.max));
        self.update_cumulative()
    }

    fn quantile_processed(&self, q: f32) -> Option<f32> {
        if self.processed.is_empty() {
            return None;
        }
        if self.processed.len() == 1 {
            return Some(self.processed[0].mean);
        }

        let n = self.processed.len();
        let index = q * self.processed_weight;

        if index <= self.weight(0) / 2.0 {
            return Some(self.min + 2.0 * index / self.weight(0) * (self.mean(0) - self.min));
        }

        if let Some(i) = self.cumulative.iter().position(|value| *value >= index)
            && i > 0
            && i < self.cumulative.len() - 1
        {
            let z1 = index - self.cumulative[i - 1];
            let z2 = self.cumulative[i] - index;
            return Some(Self::weighted_average(
                self.mean(i - 1),
                z2,
                self.mean(i),
                z1,
            ));
        }

        let z1 = index - self.processed_weight - self.weight(n - 1) / 2.0;
        let z2 = self.weight(n - 1) / 2.0 - z1;
        Some(Self::weighted_average(self.mean(n - 1), z1, self.max, z2))
    }

    fn update_cumulative(&mut self) -> Result<(), String> {
        self.cumulative.clear();
        self.cumulative
            .try_reserve(self.processed.len() + 1)
            .map_err(|_| "ResourceExhausted: reserve TDigest cumulative weights".to_string())?;
        let mut previous = 0.0;
        for centroid in &self.processed {
            let half_current = centroid.weight / 2.0;
            self.cumulative.push(previous + half_current);
            previous += centroid.weight;
        }
        self.cumulative.push(previous);
        Ok(())
    }

    fn mean(&self, idx: usize) -> f32 {
        self.processed[idx].mean
    }

    fn weight(&self, idx: usize) -> f32 {
        self.processed[idx].weight
    }

    fn integrated_location(&self, q: f32) -> f32 {
        self.compression
            * (((2.0 * q - 1.0).asin() + std::f32::consts::FRAC_PI_2) / std::f32::consts::PI)
    }

    fn integrated_q(&self, k: f32) -> f32 {
        (((k.min(self.compression) * std::f32::consts::PI / self.compression)
            - std::f32::consts::FRAC_PI_2)
            .sin()
            + 1.0)
            / 2.0
    }

    fn weighted_average(x1: f32, w1: f32, x2: f32, w2: f32) -> f32 {
        if x1 <= x2 {
            Self::weighted_average_sorted(x1, w1, x2, w2)
        } else {
            Self::weighted_average_sorted(x2, w2, x1, w1)
        }
    }

    fn weighted_average_sorted(x1: f32, w1: f32, x2: f32, w2: f32) -> f32 {
        let x = (x1 * w1 + x2 * w2) / (w1 + w2);
        x.max(x1).min(x2)
    }

    #[cfg(test)]
    fn retained_bytes(&self) -> usize {
        self.processed
            .capacity()
            .saturating_mul(std::mem::size_of::<Centroid>())
            .saturating_add(
                self.unprocessed
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Centroid>()),
            )
            .saturating_add(
                self.cumulative
                    .capacity()
                    .saturating_mul(std::mem::size_of::<f32>()),
            )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PercentileStateMeta {
    quantiles: Option<SerializableQuantileSpec>,
    compression: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum SerializableQuantileSpec {
    Scalar(f64),
    Array(Vec<f64>),
}

impl Default for PercentileState<Global> {
    fn default() -> Self {
        Self::new_in(DEFAULT_COMPRESSION_FACTOR, Global)
    }
}

impl<A: Allocator + Clone> PercentileState<A> {
    pub fn new_in(compression: usize, allocator: A) -> Self {
        Self {
            allocator: allocator.clone(),
            digest: TDigest::new_in(compression as f32, allocator),
            quantiles: None,
            compression,
        }
    }

    pub fn allocator(&self) -> A {
        self.allocator.clone()
    }

    /// Heap bytes owned by containers in this state. The state body itself is
    /// charged by its aggregate arena (or by its ordinary owner on scalar paths).
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        let quantile_bytes = match &self.quantiles {
            Some(QuantileSpec::Array(values)) => {
                values.capacity().saturating_mul(std::mem::size_of::<f64>())
            }
            _ => 0,
        };
        std::mem::size_of::<Self>()
            .saturating_add(self.digest.retained_bytes())
            .saturating_add(quantile_bytes)
    }
}

pub fn normalize_compression(compression: Option<f64>) -> Result<usize, String> {
    let Some(value) = compression else {
        return Ok(DEFAULT_COMPRESSION_FACTOR);
    };
    if !value.is_finite() {
        return Err("percentile compression must be finite".to_string());
    }
    if value <= 0.0 {
        return Err(format!(
            "compression parameter must be positive in percentile_approx_weighted, but got: {}",
            value
        ));
    }
    if !(MIN_COMPRESSION..=MAX_COMPRESSION).contains(&value) {
        return Ok(DEFAULT_COMPRESSION_FACTOR);
    }
    Ok(value.round() as usize)
}

pub fn add_value<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    value: f64,
) -> Result<(), String> {
    state.digest.add(value as f32, 1.0)?;
    validate_state(state)
}

pub fn add_weighted_value<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    value: f64,
    weight: i64,
) -> Result<(), String> {
    if weight < 0 {
        return Err(format!(
            "percentile weight must be non-negative, got {}",
            weight
        ));
    }
    if weight == 0 {
        return Ok(());
    }
    state.digest.add(value as f32, weight as f32)?;
    validate_state(state)
}

pub fn set_quantile<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    quantile: f64,
) -> Result<(), String> {
    validate_quantile(quantile)?;
    match &state.quantiles {
        Some(QuantileSpec::Scalar(existing)) => {
            if (existing - quantile).abs() > QUANTILE_TOLERANCE {
                return Err(format!(
                    "percentile quantile mismatch while merging states: existing={} incoming={}",
                    existing, quantile
                ));
            }
        }
        Some(QuantileSpec::Array(existing)) => {
            if existing.len() != 1 || (existing[0] - quantile).abs() > QUANTILE_TOLERANCE {
                return Err(
                    "percentile quantile mismatch while merging states: scalar/array mismatch"
                        .to_string(),
                );
            }
        }
        None => state.quantiles = Some(QuantileSpec::Scalar(quantile)),
    }
    Ok(())
}

pub fn set_quantiles<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    quantiles: &[f64],
) -> Result<(), String> {
    if quantiles.is_empty() {
        return Err("percentile array cannot be empty".to_string());
    }
    if quantiles.len() > MAX_QUANTILE_COUNT {
        return Err(format!(
            "percentile quantile count {} exceeds {}",
            quantiles.len(),
            MAX_QUANTILE_COUNT
        ));
    }
    for &quantile in quantiles {
        validate_quantile(quantile)?;
    }
    match &state.quantiles {
        Some(QuantileSpec::Scalar(existing)) => {
            if quantiles.len() != 1 || (existing - quantiles[0]).abs() > QUANTILE_TOLERANCE {
                return Err(
                    "percentile quantile mismatch while merging states: scalar/array mismatch"
                        .to_string(),
                );
            }
        }
        Some(QuantileSpec::Array(existing)) => {
            if !same_quantile_vec(existing, quantiles) {
                return Err("percentile quantile array mismatch while merging states".to_string());
            }
        }
        None => {
            state.quantiles = Some(QuantileSpec::Array(try_copy_slice_in(
                quantiles,
                state.allocator.clone(),
                "percentile quantile array",
            )?))
        }
    }
    Ok(())
}

pub fn set_compression<A: Allocator + Clone>(
    state: &mut PercentileState<A>,
    compression: f64,
) -> Result<(), String> {
    let normalized = normalize_compression(Some(compression))?;
    state.compression = normalized;
    if state.digest.is_empty() {
        state.digest = TDigest::new_in(normalized as f32, state.allocator.clone());
    }
    Ok(())
}

pub fn merge_state<A: Allocator + Clone>(
    target: &mut PercentileState<A>,
    incoming: &PercentileState<A>,
) -> Result<(), String> {
    if let Some(quantiles) = &incoming.quantiles {
        match quantiles {
            QuantileSpec::Scalar(q) => set_quantile(target, *q)?,
            QuantileSpec::Array(qs) => set_quantiles(target, qs)?,
        }
    }
    if target.digest.is_empty() {
        target.compression = incoming.compression;
        target.digest = incoming.digest.try_clone()?;
        return validate_state(target);
    }
    target.compression = target.compression.max(incoming.compression);
    target.digest.merge(&incoming.digest)?;
    validate_state(target)
}

pub fn merge_serialized_state_into(
    target: &mut PercentileState,
    payload: &[u8],
) -> Result<(), String> {
    let decoded = decode_state(payload)?;
    merge_state(target, &decoded)
}

/// Aggregate execution accepts only the current bounded binary format. Legacy
/// JSON v3 decoding remains available to scalar compatibility readers, but it
/// has no allocation bound and therefore cannot enter a hard-limited aggregate
/// state.
pub fn merge_bounded_serialized_state_into(
    target: &mut PercentileState<impl Allocator + Clone>,
    payload: &[u8],
) -> Result<(), String> {
    if payload.get(1).copied() != Some(PERCENTILE_STATE_VERSION) {
        return Err(format!(
            "bounded percentile aggregate requires state version {PERCENTILE_STATE_VERSION}"
        ));
    }
    let decoded = decode_state_v4_in(payload, target.allocator())?;
    merge_state(target, &decoded)?;
    validate_state(target)
}

pub fn encode_empty_state() -> Vec<u8> {
    encode_state(&PercentileState::default())
}

pub fn encode_single_value(value: f64) -> Vec<u8> {
    let mut state = PercentileState::default();
    add_value(&mut state, value).expect("single percentile value must fit bounded TDigest");
    encode_state(&state)
}

pub fn encode_state<A: Allocator + Clone>(state: &PercentileState<A>) -> Vec<u8> {
    let (quantile_kind, quantiles) = match &state.quantiles {
        Some(QuantileSpec::Scalar(q)) => (QUANTILE_KIND_SCALAR, std::slice::from_ref(q)),
        Some(QuantileSpec::Array(values)) => (QUANTILE_KIND_ARRAY, values.as_slice()),
        None => (QUANTILE_KIND_NONE, &[][..]),
    };
    let digest_payload = if state.digest.is_empty() {
        Vec::new()
    } else {
        state.digest.serialize_binary()
    };
    let mut out =
        Vec::with_capacity(HEADER_LEN + std::mem::size_of_val(quantiles) + digest_payload.len());
    out.push(PERCENTILE_STATE_MAGIC);
    out.push(PERCENTILE_STATE_VERSION);
    out.push(quantile_kind);
    out.extend_from_slice(&(state.compression as u32).to_le_bytes());
    out.extend_from_slice(&(quantiles.len() as u32).to_le_bytes());
    for quantile in quantiles {
        out.extend_from_slice(&quantile.to_le_bytes());
    }
    out.extend_from_slice(&digest_payload);
    out
}

pub fn decode_state(payload: &[u8]) -> Result<PercentileState, String> {
    if payload.is_empty() {
        return Ok(PercentileState::default());
    }
    if payload.len() < HEADER_LEN {
        return Err(format!(
            "percentile state payload too short: expected>={} actual={}",
            HEADER_LEN,
            payload.len()
        ));
    }
    if payload[0] != PERCENTILE_STATE_MAGIC {
        return Err(format!(
            "unsupported percentile state payload magic: expected=0x{:02x} actual=0x{:02x}",
            PERCENTILE_STATE_MAGIC, payload[0]
        ));
    }
    match payload[1] {
        PERCENTILE_STATE_VERSION => decode_state_v4(payload),
        3 => decode_state_v3(payload),
        other => Err(format!(
            "unsupported percentile state payload version: expected={} actual={}",
            PERCENTILE_STATE_VERSION, other
        )),
    }
}

pub fn quantile_value<A: Allocator + Clone>(
    state: &PercentileState<A>,
    quantile: f64,
) -> Result<Option<f64>, String> {
    let mut digest = state.digest.clone();
    Ok(digest.quantile(quantile as f32)?.map(|value| value as f64))
}

pub fn quantile_from_state<A: Allocator + Clone>(
    state: &PercentileState<A>,
    quantile: Option<f64>,
) -> Result<Option<f64>, String> {
    let q = match quantile {
        Some(q) => q,
        None => match &state.quantiles {
            Some(QuantileSpec::Scalar(q)) => *q,
            Some(QuantileSpec::Array(values)) if values.len() == 1 => values[0],
            _ => return Ok(None),
        },
    };
    quantile_value(state, q)
}

pub fn quantiles_from_state<A: Allocator + Clone>(
    state: &PercentileState<A>,
) -> Result<Option<Vec<f64>>, String> {
    let quantiles = match &state.quantiles {
        Some(QuantileSpec::Scalar(q)) => vec![*q],
        Some(QuantileSpec::Array(values)) => values.iter().copied().collect(),
        None => return Ok(None),
    };
    if state.digest.is_empty() {
        return Ok(Some(vec![f64::NAN; quantiles.len()]));
    }
    let mut digest = state.digest.clone();
    Ok(Some(
        quantiles
            .into_iter()
            .map(|q| Ok(digest.quantile(q as f32)?.unwrap_or(f32::NAN) as f64))
            .collect::<Result<Vec<_>, String>>()?,
    ))
}

pub fn validate_state<A: Allocator + Clone>(state: &PercentileState<A>) -> Result<(), String> {
    let quantile_count = match &state.quantiles {
        Some(QuantileSpec::Array(values)) => values.len(),
        Some(QuantileSpec::Scalar(_)) => 1,
        None => 0,
    };
    if quantile_count > MAX_QUANTILE_COUNT {
        return Err(format!(
            "percentile quantile count {quantile_count} exceeds {MAX_QUANTILE_COUNT}"
        ));
    }
    Ok(())
}

fn validate_quantile(quantile: f64) -> Result<(), String> {
    if !quantile.is_finite() {
        return Err("percentile quantile must be finite".to_string());
    }
    if !(0.0..=1.0).contains(&quantile) {
        return Err(format!(
            "percentile quantile must be between 0 and 1, got {}",
            quantile
        ));
    }
    Ok(())
}

fn same_quantile_vec(left: &[f64], right: &[f64]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(l, r)| (*l - *r).abs() <= QUANTILE_TOLERANCE)
}

fn merge_sorted_centroids<A: Allocator + Clone>(
    left: &[Centroid],
    right: &[Centroid],
    allocator: A,
) -> Result<AllocVec<Centroid, A>, String> {
    if left.is_empty() {
        return try_copy_slice_in(right, allocator, "TDigest right centroids");
    }
    if right.is_empty() {
        return try_copy_slice_in(left, allocator, "TDigest left centroids");
    }
    let capacity = left
        .len()
        .checked_add(right.len())
        .ok_or_else(|| "TDigest merge length overflow".to_string())?;
    let mut merged = AllocVec::new_in(allocator);
    merged
        .try_reserve_exact(capacity)
        .map_err(|_| "ResourceExhausted: reserve TDigest merge".to_string())?;
    let mut left_idx = 0usize;
    let mut right_idx = 0usize;
    while left_idx < left.len() && right_idx < right.len() {
        if left[left_idx].mean <= right[right_idx].mean {
            merged.push(left[left_idx]);
            left_idx += 1;
        } else {
            merged.push(right[right_idx]);
            right_idx += 1;
        }
    }
    merged.extend_from_slice(&left[left_idx..]);
    merged.extend_from_slice(&right[right_idx..]);
    Ok(merged)
}

fn decode_state_v4(payload: &[u8]) -> Result<PercentileState, String> {
    decode_state_v4_in(payload, Global)
}

fn decode_state_v4_in<A: Allocator + Clone>(
    payload: &[u8],
    allocator: A,
) -> Result<PercentileState<A>, String> {
    if payload.len() < HEADER_LEN {
        return Err("percentile state payload too short".to_string());
    }
    let quantile_kind = payload[2];
    let compression = u32::from_le_bytes(
        payload[3..7]
            .try_into()
            .map_err(|_| "percentile state compression decode failed".to_string())?,
    ) as usize;
    let quantile_count = u32::from_le_bytes(
        payload[7..11]
            .try_into()
            .map_err(|_| "percentile state quantile count decode failed".to_string())?,
    ) as usize;
    if quantile_count > MAX_QUANTILE_COUNT {
        return Err(format!(
            "percentile state quantile count {quantile_count} exceeds {MAX_QUANTILE_COUNT}"
        ));
    }
    if compression == 0 || compression > MAX_COMPRESSION as usize {
        return Err(format!(
            "percentile state compression {compression} exceeds bounded range"
        ));
    }
    let quantile_bytes = quantile_count
        .checked_mul(std::mem::size_of::<f64>())
        .ok_or_else(|| "percentile state quantile bytes overflow".to_string())?;
    let quantile_end = HEADER_LEN
        .checked_add(quantile_bytes)
        .ok_or_else(|| "percentile state quantile end overflow".to_string())?;
    if payload.len() < quantile_end {
        return Err("percentile state quantile payload truncated".to_string());
    }

    let mut quantiles = AllocVec::new_in(allocator.clone());
    quantiles
        .try_reserve_exact(quantile_count)
        .map_err(|_| "ResourceExhausted: reserve decoded percentile quantiles".to_string())?;
    let mut offset = HEADER_LEN;
    for _ in 0..quantile_count {
        quantiles.push(read_f64(payload, &mut offset, "percentile state quantile")?);
    }

    let quantiles = match (quantile_kind, quantiles.len()) {
        (QUANTILE_KIND_NONE, 0) => None,
        (QUANTILE_KIND_SCALAR, 1) => Some(QuantileSpec::Scalar(quantiles[0])),
        (QUANTILE_KIND_ARRAY, _) => Some(QuantileSpec::Array(quantiles)),
        _ => {
            return Err(format!(
                "invalid percentile state quantile metadata: kind={} count={}",
                quantile_kind, quantile_count
            ));
        }
    };

    let digest = if payload.len() == quantile_end {
        TDigest::new_in(compression as f32, allocator.clone())
    } else {
        TDigest::deserialize_binary_in(&payload[quantile_end..], allocator.clone())?
    };
    let state = PercentileState {
        allocator,
        digest,
        quantiles,
        compression,
    };
    validate_state(&state)?;
    Ok(state)
}

fn try_copy_slice_in<T: Copy, A: Allocator + Clone>(
    values: &[T],
    allocator: A,
    label: &str,
) -> Result<AllocVec<T, A>, String> {
    let mut copied = AllocVec::new_in(allocator);
    copied
        .try_reserve_exact(values.len())
        .map_err(|_| format!("ResourceExhausted: reserve {label}"))?;
    copied.extend_from_slice(values);
    Ok(copied)
}

fn decode_state_v3(payload: &[u8]) -> Result<PercentileState, String> {
    let meta_len = u32::from_le_bytes(
        payload[2..6]
            .try_into()
            .map_err(|_| "percentile state meta length decode failed".to_string())?,
    ) as usize;
    if payload.len() < 6 + meta_len {
        return Err("percentile state meta payload truncated".to_string());
    }
    let meta: PercentileStateMeta =
        serde_json::from_slice(&payload[6..6 + meta_len]).map_err(|e| e.to_string())?;
    let digest = if payload.len() == 6 + meta_len {
        TDigest::new_in(meta.compression as f32, Global)
    } else {
        let decoded: SerializableTDigest =
            serde_json::from_slice(&payload[6 + meta_len..]).map_err(|e| e.to_string())?;
        TDigest {
            allocator: Global,
            compression: decoded.compression,
            min: decoded.min,
            max: decoded.max,
            max_processed: decoded.max_processed,
            max_unprocessed: decoded.max_unprocessed,
            processed_weight: decoded.processed_weight,
            unprocessed_weight: decoded.unprocessed_weight,
            processed: decoded.processed.into_iter().collect(),
            unprocessed: decoded.unprocessed.into_iter().collect(),
            cumulative: decoded.cumulative.into_iter().collect(),
        }
    };
    Ok(PercentileState {
        allocator: Global,
        digest,
        quantiles: meta.quantiles.map(|quantiles| match quantiles {
            SerializableQuantileSpec::Scalar(value) => QuantileSpec::Scalar(value),
            SerializableQuantileSpec::Array(values) => {
                QuantileSpec::Array(values.into_iter().collect())
            }
        }),
        compression: meta.compression,
    })
}

fn read_u32(payload: &[u8], offset: &mut usize, label: &str) -> Result<u32, String> {
    let end = offset
        .checked_add(std::mem::size_of::<u32>())
        .ok_or_else(|| format!("{label} offset overflow"))?;
    let bytes: [u8; 4] = payload
        .get(*offset..end)
        .ok_or_else(|| format!("{label} truncated"))?
        .try_into()
        .map_err(|_| format!("{label} decode failed"))?;
    *offset = end;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(payload: &[u8], offset: &mut usize, label: &str) -> Result<u64, String> {
    let end = offset
        .checked_add(std::mem::size_of::<u64>())
        .ok_or_else(|| format!("{label} offset overflow"))?;
    let bytes: [u8; 8] = payload
        .get(*offset..end)
        .ok_or_else(|| format!("{label} truncated"))?
        .try_into()
        .map_err(|_| format!("{label} decode failed"))?;
    *offset = end;
    Ok(u64::from_le_bytes(bytes))
}

fn read_f32(payload: &[u8], offset: &mut usize, label: &str) -> Result<f32, String> {
    let bits = read_u32(payload, offset, label)?;
    Ok(f32::from_le_bytes(bits.to_le_bytes()))
}

fn read_f64(payload: &[u8], offset: &mut usize, label: &str) -> Result<f64, String> {
    let bits = read_u64(payload, offset, label)?;
    Ok(f64::from_le_bytes(bits.to_le_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tdigest_round_trip_preserves_weight() {
        let mut state = PercentileState::default();
        add_value(&mut state, 1.0).unwrap();
        add_value(&mut state, 2.0).unwrap();
        add_value(&mut state, 3.0).unwrap();
        let encoded = encode_state(&state);
        let decoded = decode_state(&encoded).expect("decode");
        assert_eq!(decoded.digest.total_weight() as i64, 3);
    }

    #[test]
    fn merge_serialized_state_into_empty_target_preserves_compression() {
        let mut source = PercentileState::default();
        set_quantiles(&mut source, &[0.5, 0.9]).expect("set quantiles");
        set_compression(&mut source, 5000.0).expect("set compression");
        for value in 1..=50_000 {
            add_value(&mut source, value as f64).unwrap();
        }

        let payload = encode_state(&source);
        let expected = quantiles_from_state(&decode_state(&payload).expect("decode source"))
            .expect("source quantiles");

        let mut merged = PercentileState::default();
        merge_serialized_state_into(&mut merged, &payload).expect("merge payload");
        let actual = quantiles_from_state(&merged).expect("merged quantiles");

        assert_eq!(merged.compression, 5000);
        assert_eq!(actual, expected);
    }

    #[test]
    fn retained_bytes_tracks_quantiles_digest_growth_and_merge() {
        let mut source = PercentileState::default();
        let empty = source.retained_bytes();
        set_quantiles(&mut source, &Vec::with_capacity(8)).expect_err("empty quantiles");
        assert_eq!(source.retained_bytes(), empty);

        let mut quantiles = Vec::with_capacity(8);
        quantiles.extend([0.1, 0.5, 0.9]);
        set_quantiles(&mut source, &quantiles).expect("set quantiles");
        let with_quantiles = source.retained_bytes();
        let retained_quantile_bytes = match &source.quantiles {
            Some(QuantileSpec::Array(values)) => values.capacity() * std::mem::size_of::<f64>(),
            _ => panic!("array quantiles must remain an array"),
        };
        assert_eq!(with_quantiles, empty + retained_quantile_bytes);

        for value in 0..1024 {
            add_value(&mut source, value as f64).unwrap();
        }
        let with_digest = source.retained_bytes();
        assert!(with_digest > with_quantiles);

        let mut merged = PercentileState::default();
        merge_state(&mut merged, &source).expect("merge state");
        let expected_quantiles = match &merged.quantiles {
            Some(QuantileSpec::Array(values)) => values.capacity() * std::mem::size_of::<f64>(),
            _ => 0,
        };
        assert_eq!(
            merged.retained_bytes(),
            std::mem::size_of::<PercentileState>()
                + merged.digest.retained_bytes()
                + expected_quantiles
        );
        assert!(merged.retained_bytes() > empty);
    }

    #[test]
    fn failed_weighted_update_does_not_change_retained_bytes() {
        let mut state = PercentileState::default();
        let before = state.retained_bytes();
        assert!(add_weighted_value(&mut state, 1.0, -1).is_err());
        assert_eq!(state.retained_bytes(), before);
    }
}
