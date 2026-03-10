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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum QuantileSpec {
    Scalar(f64),
    Array(Vec<f64>),
}

#[derive(Clone, Debug)]
pub struct PercentileState {
    pub digest: TDigest,
    pub quantiles: Option<QuantileSpec>,
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
pub struct TDigest {
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

impl TDigest {
    fn new(compression: f32) -> Self {
        Self {
            compression,
            min: f32::MAX,
            max: f32::MIN,
            max_processed: (2.0 * compression.ceil()) as usize,
            max_unprocessed: (8.0 * compression.ceil()) as usize,
            processed_weight: 0.0,
            unprocessed_weight: 0.0,
            processed: Vec::new(),
            unprocessed: Vec::new(),
            cumulative: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.processed.is_empty() && self.unprocessed.is_empty()
    }

    fn total_weight(&self) -> f32 {
        self.processed_weight + self.unprocessed_weight
    }

    pub fn count(&self) -> f32 {
        self.total_weight()
    }

    fn add(&mut self, value: f32, weight: f32) {
        if value.is_nan() || weight <= 0.0 {
            return;
        }
        self.unprocessed.push(Centroid::new(value, weight));
        self.unprocessed_weight += weight;
        self.process_if_necessary();
    }

    fn merge(&mut self, other: &TDigest) {
        if other.is_empty() {
            return;
        }
        if !other.processed.is_empty() {
            self.processed_weight += other.processed_weight;
            self.processed = merge_sorted_centroids(&self.processed, &other.processed);
            if let Some(first) = self.processed.first() {
                self.min = self.min.min(first.mean);
            }
            if let Some(last) = self.processed.last() {
                self.max = self.max.max(last.mean);
            }
        }
        if !other.unprocessed.is_empty() {
            self.unprocessed.reserve(other.unprocessed.len());
            self.unprocessed.extend_from_slice(&other.unprocessed);
            self.unprocessed_weight += other.unprocessed_weight;
        }
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.process_if_necessary();
        self.update_cumulative();
    }

    fn quantile(&mut self, q: f32) -> Option<f32> {
        if !(0.0..=1.0).contains(&q) {
            return None;
        }
        if self.have_unprocessed() || self.is_dirty() {
            self.process();
        }
        self.quantile_processed(q)
    }

    fn serialize_binary(&self) -> Vec<u8> {
        let payload = SerializableTDigest {
            compression: self.compression,
            min: self.min,
            max: self.max,
            max_processed: self.max_processed,
            max_unprocessed: self.max_unprocessed,
            processed_weight: self.processed_weight,
            unprocessed_weight: self.unprocessed_weight,
            processed: self.processed.clone(),
            unprocessed: self.unprocessed.clone(),
            cumulative: self.cumulative.clone(),
        };
        let mut out = Vec::with_capacity(
            4 * std::mem::size_of::<f32>()
                + 2 * std::mem::size_of::<u64>()
                + 3 * std::mem::size_of::<u32>()
                + payload.processed.len() * 2 * std::mem::size_of::<f32>()
                + payload.unprocessed.len() * 2 * std::mem::size_of::<f32>()
                + payload.cumulative.len() * std::mem::size_of::<f32>(),
        );
        out.extend_from_slice(&payload.compression.to_le_bytes());
        out.extend_from_slice(&payload.min.to_le_bytes());
        out.extend_from_slice(&payload.max.to_le_bytes());
        out.extend_from_slice(&(payload.max_processed as u64).to_le_bytes());
        out.extend_from_slice(&(payload.max_unprocessed as u64).to_le_bytes());
        out.extend_from_slice(&payload.processed_weight.to_le_bytes());
        out.extend_from_slice(&payload.unprocessed_weight.to_le_bytes());
        out.extend_from_slice(&(payload.processed.len() as u32).to_le_bytes());
        for centroid in &payload.processed {
            out.extend_from_slice(&centroid.mean.to_le_bytes());
            out.extend_from_slice(&centroid.weight.to_le_bytes());
        }
        out.extend_from_slice(&(payload.unprocessed.len() as u32).to_le_bytes());
        for centroid in &payload.unprocessed {
            out.extend_from_slice(&centroid.mean.to_le_bytes());
            out.extend_from_slice(&centroid.weight.to_le_bytes());
        }
        out.extend_from_slice(&(payload.cumulative.len() as u32).to_le_bytes());
        for value in &payload.cumulative {
            out.extend_from_slice(&value.to_le_bytes());
        }
        out
    }

    fn deserialize_binary(payload: &[u8]) -> Result<Self, String> {
        let mut offset = 0usize;
        let compression = read_f32(payload, &mut offset, "tdigest compression")?;
        let min = read_f32(payload, &mut offset, "tdigest min")?;
        let max = read_f32(payload, &mut offset, "tdigest max")?;
        let max_processed = read_u64(payload, &mut offset, "tdigest max_processed")? as usize;
        let max_unprocessed = read_u64(payload, &mut offset, "tdigest max_unprocessed")? as usize;
        let processed_weight = read_f32(payload, &mut offset, "tdigest processed_weight")?;
        let unprocessed_weight = read_f32(payload, &mut offset, "tdigest unprocessed_weight")?;

        let processed_len = read_u32(payload, &mut offset, "tdigest processed len")? as usize;
        let mut processed = Vec::with_capacity(processed_len);
        for _ in 0..processed_len {
            processed.push(Centroid::new(
                read_f32(payload, &mut offset, "tdigest processed mean")?,
                read_f32(payload, &mut offset, "tdigest processed weight")?,
            ));
        }

        let unprocessed_len = read_u32(payload, &mut offset, "tdigest unprocessed len")? as usize;
        let mut unprocessed = Vec::with_capacity(unprocessed_len);
        for _ in 0..unprocessed_len {
            unprocessed.push(Centroid::new(
                read_f32(payload, &mut offset, "tdigest unprocessed mean")?,
                read_f32(payload, &mut offset, "tdigest unprocessed weight")?,
            ));
        }

        let cumulative_len = read_u32(payload, &mut offset, "tdigest cumulative len")? as usize;
        let mut cumulative = Vec::with_capacity(cumulative_len);
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

    fn process_if_necessary(&mut self) {
        if self.is_dirty() {
            self.process();
        }
    }

    fn process(&mut self) {
        if self.unprocessed.is_empty() && self.processed.is_empty() {
            return;
        }

        self.unprocessed.sort_by(|left, right| {
            left.mean
                .partial_cmp(&right.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let processed = std::mem::take(&mut self.processed);
        if !processed.is_empty() {
            let mut merged = Vec::with_capacity(self.unprocessed.len() + processed.len());
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
            return;
        };
        self.processed = Vec::with_capacity(self.max_processed.max(1));
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
        self.update_cumulative();
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

        if let Some(i) = self.cumulative.iter().position(|value| *value >= index) {
            if i > 0 && i < self.cumulative.len() - 1 {
                let z1 = index - self.cumulative[i - 1];
                let z2 = self.cumulative[i] - index;
                return Some(Self::weighted_average(
                    self.mean(i - 1),
                    z2,
                    self.mean(i),
                    z1,
                ));
            }
        }

        let z1 = index - self.processed_weight - self.weight(n - 1) / 2.0;
        let z2 = self.weight(n - 1) / 2.0 - z1;
        Some(Self::weighted_average(self.mean(n - 1), z1, self.max, z2))
    }

    fn update_cumulative(&mut self) {
        self.cumulative.clear();
        self.cumulative.reserve(self.processed.len() + 1);
        let mut previous = 0.0;
        for centroid in &self.processed {
            let half_current = centroid.weight / 2.0;
            self.cumulative.push(previous + half_current);
            previous += centroid.weight;
        }
        self.cumulative.push(previous);
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PercentileStateMeta {
    quantiles: Option<QuantileSpec>,
    compression: usize,
}

impl Default for PercentileState {
    fn default() -> Self {
        Self {
            digest: TDigest::new(DEFAULT_COMPRESSION_FACTOR as f32),
            quantiles: None,
            compression: DEFAULT_COMPRESSION_FACTOR,
        }
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

pub fn add_value(state: &mut PercentileState, value: f64) {
    state.digest.add(value as f32, 1.0);
}

pub fn add_weighted_value(
    state: &mut PercentileState,
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
    state.digest.add(value as f32, weight as f32);
    Ok(())
}

pub fn set_quantile(state: &mut PercentileState, quantile: f64) -> Result<(), String> {
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

pub fn set_quantiles(state: &mut PercentileState, quantiles: Vec<f64>) -> Result<(), String> {
    if quantiles.is_empty() {
        return Err("percentile array cannot be empty".to_string());
    }
    for &quantile in &quantiles {
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
            if !same_quantile_vec(existing, &quantiles) {
                return Err("percentile quantile array mismatch while merging states".to_string());
            }
        }
        None => state.quantiles = Some(QuantileSpec::Array(quantiles)),
    }
    Ok(())
}

pub fn set_compression(state: &mut PercentileState, compression: f64) -> Result<(), String> {
    let normalized = normalize_compression(Some(compression))?;
    state.compression = normalized;
    if state.digest.is_empty() {
        state.digest = TDigest::new(normalized as f32);
    }
    Ok(())
}

pub fn merge_state(target: &mut PercentileState, incoming: &PercentileState) -> Result<(), String> {
    if let Some(quantiles) = &incoming.quantiles {
        match quantiles {
            QuantileSpec::Scalar(q) => set_quantile(target, *q)?,
            QuantileSpec::Array(qs) => set_quantiles(target, qs.clone())?,
        }
    }
    target.compression = target.compression.max(incoming.compression);
    target.digest.merge(&incoming.digest);
    Ok(())
}

pub fn merge_serialized_state_into(
    target: &mut PercentileState,
    payload: &[u8],
) -> Result<(), String> {
    let decoded = decode_state(payload)?;
    merge_state(target, &decoded)
}

pub fn encode_empty_state() -> Vec<u8> {
    encode_state(&PercentileState::default())
}

pub fn encode_single_value(value: f64) -> Vec<u8> {
    let mut state = PercentileState::default();
    add_value(&mut state, value);
    encode_state(&state)
}

pub fn encode_state(state: &PercentileState) -> Vec<u8> {
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
    let mut out = Vec::with_capacity(
        HEADER_LEN + quantiles.len() * std::mem::size_of::<f64>() + digest_payload.len(),
    );
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

pub fn quantile_value(state: &PercentileState, quantile: f64) -> Option<f64> {
    let mut digest = state.digest.clone();
    digest.quantile(quantile as f32).map(|value| value as f64)
}

pub fn quantile_from_state(state: &PercentileState, quantile: Option<f64>) -> Option<f64> {
    let q = match quantile {
        Some(q) => q,
        None => match &state.quantiles {
            Some(QuantileSpec::Scalar(q)) => *q,
            Some(QuantileSpec::Array(values)) if values.len() == 1 => values[0],
            _ => return None,
        },
    };
    quantile_value(state, q)
}

pub fn quantiles_from_state(state: &PercentileState) -> Option<Vec<f64>> {
    let quantiles = match &state.quantiles {
        Some(QuantileSpec::Scalar(q)) => vec![*q],
        Some(QuantileSpec::Array(values)) => values.clone(),
        None => return None,
    };
    if state.digest.is_empty() {
        return Some(vec![f64::NAN; quantiles.len()]);
    }
    let mut digest = state.digest.clone();
    Some(
        quantiles
            .into_iter()
            .map(|q| digest.quantile(q as f32).unwrap_or(f32::NAN) as f64)
            .collect(),
    )
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

fn merge_sorted_centroids(left: &[Centroid], right: &[Centroid]) -> Vec<Centroid> {
    if left.is_empty() {
        return right.to_vec();
    }
    if right.is_empty() {
        return left.to_vec();
    }
    let mut merged = Vec::with_capacity(left.len() + right.len());
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
    merged
}

fn decode_state_v4(payload: &[u8]) -> Result<PercentileState, String> {
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
    let quantile_bytes = quantile_count
        .checked_mul(std::mem::size_of::<f64>())
        .ok_or_else(|| "percentile state quantile bytes overflow".to_string())?;
    let quantile_end = HEADER_LEN
        .checked_add(quantile_bytes)
        .ok_or_else(|| "percentile state quantile end overflow".to_string())?;
    if payload.len() < quantile_end {
        return Err("percentile state quantile payload truncated".to_string());
    }

    let mut quantiles = Vec::with_capacity(quantile_count);
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
        TDigest::new(compression as f32)
    } else {
        TDigest::deserialize_binary(&payload[quantile_end..])?
    };
    Ok(PercentileState {
        digest,
        quantiles,
        compression,
    })
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
        TDigest::new(meta.compression as f32)
    } else {
        let decoded: SerializableTDigest =
            serde_json::from_slice(&payload[6 + meta_len..]).map_err(|e| e.to_string())?;
        TDigest {
            compression: decoded.compression,
            min: decoded.min,
            max: decoded.max,
            max_processed: decoded.max_processed,
            max_unprocessed: decoded.max_unprocessed,
            processed_weight: decoded.processed_weight,
            unprocessed_weight: decoded.unprocessed_weight,
            processed: decoded.processed,
            unprocessed: decoded.unprocessed,
            cumulative: decoded.cumulative,
        }
    };
    Ok(PercentileState {
        digest,
        quantiles: meta.quantiles,
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
        add_value(&mut state, 1.0);
        add_value(&mut state, 2.0);
        add_value(&mut state, 3.0);
        let encoded = encode_state(&state);
        let decoded = decode_state(&encoded).expect("decode");
        assert_eq!(decoded.digest.total_weight() as i64, 3);
    }
}
