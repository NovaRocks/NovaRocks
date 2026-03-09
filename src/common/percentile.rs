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

const PERCENTILE_STATE_MAGIC: u8 = 0xA1;
const FLAG_HAS_QUANTILE: u8 = 0x01;
const HEADER_LEN: usize = 1 + 1 + 4;
const QUANTILE_TOLERANCE: f64 = 1e-12;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PercentileState {
    pub values: Vec<f64>,
    pub quantile: Option<f64>,
}

pub fn add_value(state: &mut PercentileState, value: f64) {
    state.values.push(value);
}

pub fn set_quantile(state: &mut PercentileState, quantile: f64) -> Result<(), String> {
    if !quantile.is_finite() {
        return Err("percentile quantile must be finite".to_string());
    }
    if let Some(existing) = state.quantile {
        if (existing - quantile).abs() > QUANTILE_TOLERANCE {
            return Err(format!(
                "percentile quantile mismatch while merging states: existing={} incoming={}",
                existing, quantile
            ));
        }
    } else {
        state.quantile = Some(quantile);
    }
    Ok(())
}

pub fn merge_state(target: &mut PercentileState, incoming: &PercentileState) -> Result<(), String> {
    if let Some(quantile) = incoming.quantile {
        set_quantile(target, quantile)?;
    }
    target.values.extend_from_slice(&incoming.values);
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
    let mut flags = 0u8;
    let mut capacity = HEADER_LEN + state.values.len() * std::mem::size_of::<f64>();
    if state.quantile.is_some() {
        flags |= FLAG_HAS_QUANTILE;
        capacity += std::mem::size_of::<f64>();
    }

    let mut out = Vec::with_capacity(capacity);
    out.push(PERCENTILE_STATE_MAGIC);
    out.push(flags);
    out.extend_from_slice(&(state.values.len() as u32).to_le_bytes());
    if let Some(quantile) = state.quantile {
        out.extend_from_slice(&quantile.to_le_bytes());
    }
    for value in &state.values {
        out.extend_from_slice(&value.to_le_bytes());
    }
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

    let flags = payload[1];
    let count = u32::from_le_bytes(
        payload[2..6]
            .try_into()
            .map_err(|_| "percentile state decode count failed".to_string())?,
    ) as usize;

    let has_quantile = (flags & FLAG_HAS_QUANTILE) != 0;
    let expected = HEADER_LEN
        + usize::from(has_quantile) * std::mem::size_of::<f64>()
        + count * std::mem::size_of::<f64>();
    if payload.len() != expected {
        return Err(format!(
            "percentile state payload length mismatch: expected={} actual={} count={} flags={}",
            expected,
            payload.len(),
            count,
            flags
        ));
    }

    let mut offset = HEADER_LEN;
    let quantile = if has_quantile {
        let value = f64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .map_err(|_| "percentile state decode quantile failed".to_string())?,
        );
        offset += 8;
        Some(value)
    } else {
        None
    };

    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let value = f64::from_le_bytes(
            payload[offset..offset + 8]
                .try_into()
                .map_err(|_| "percentile state decode value failed".to_string())?,
        );
        offset += 8;
        values.push(value);
    }

    Ok(PercentileState { values, quantile })
}

pub fn quantile_value(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() || !quantile.is_finite() {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);

    if sorted.len() == 1 {
        return Some(sorted[0]);
    }

    let q = quantile.clamp(0.0, 1.0);
    let position = q * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        return Some(sorted[lower]);
    }
    let fraction = position - lower as f64;
    Some(sorted[lower] + (sorted[upper] - sorted[lower]) * fraction)
}

pub fn quantile_from_state(state: &PercentileState, quantile: Option<f64>) -> Option<f64> {
    quantile_value(&state.values, quantile.or(state.quantile)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_state_round_trip_preserves_values_and_quantile() {
        let state = PercentileState {
            values: vec![3.0, 1.0, 2.0],
            quantile: Some(0.75),
        };
        let encoded = encode_state(&state);
        let decoded = decode_state(&encoded).expect("decode");
        assert_eq!(decoded, state);
    }

    #[test]
    fn quantile_value_returns_extrema() {
        let values = vec![5.0, 1.0, 3.0];
        assert_eq!(quantile_value(&values, 0.0), Some(1.0));
        assert_eq!(quantile_value(&values, 1.0), Some(5.0));
    }

    #[test]
    fn merge_state_rejects_mismatched_quantiles() {
        let mut left = PercentileState {
            values: vec![1.0],
            quantile: Some(0.0),
        };
        let right = PercentileState {
            values: vec![2.0],
            quantile: Some(1.0),
        };
        let err = merge_state(&mut left, &right).expect_err("must fail");
        assert!(err.contains("quantile mismatch"));
    }
}
