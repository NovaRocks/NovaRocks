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

use datasketches::hll::{HllSketch, HllType, HllUnion};

// Keep the legacy module name to minimize call-site churn while removing the C++ dependency.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HllTargetType {
    Hll4,
    Hll6,
    Hll8,
}

impl HllTargetType {
    fn ensure_supported(self, context: &str) -> Result<(), String> {
        match self {
            Self::Hll4 => Err(format!(
                "{context}: HLL_4 is unsupported without the removed Apache DataSketches C++ backend; use HLL_6 or HLL_8"
            )),
            Self::Hll6 | Self::Hll8 => Ok(()),
        }
    }

    fn into_native(self) -> HllType {
        match self {
            Self::Hll4 => HllType::Hll4,
            Self::Hll6 => HllType::Hll6,
            Self::Hll8 => HllType::Hll8,
        }
    }

    fn from_native(value: HllType) -> Self {
        match value {
            HllType::Hll4 => Self::Hll4,
            HllType::Hll6 => Self::Hll6,
            HllType::Hll8 => Self::Hll8,
        }
    }
}

fn deserialize_hll(payload: &[u8], context: &str) -> Result<HllSketch, String> {
    HllSketch::deserialize(payload)
        .map_err(|err| format!("{context}: failed to deserialize HLL payload: {err}"))
}

pub fn hll_estimate(payload: &[u8]) -> Result<i64, String> {
    Ok(deserialize_hll(payload, "ds_hll")?.estimate().round() as i64)
}

pub struct HllHandle {
    target_type: HllTargetType,
    sketch_union: HllUnion,
}

impl HllHandle {
    pub fn new(log_k: u8, target_type: HllTargetType) -> Result<Self, String> {
        if !(4..=21).contains(&log_k) {
            return Err(format!("ds_hll log_k must be in [4, 21], got {log_k}"));
        }
        target_type.ensure_supported("ds_hll")?;
        Ok(Self {
            target_type,
            sketch_union: HllUnion::new(log_k),
        })
    }

    pub fn from_payload(payload: &[u8]) -> Result<Self, String> {
        let sketch = deserialize_hll(payload, "ds_hll")?;
        let target_type = HllTargetType::from_native(sketch.target_type());
        target_type.ensure_supported("ds_hll")?;
        let mut sketch_union = HllUnion::new(sketch.lg_config_k());
        sketch_union.update(&sketch);
        Ok(Self {
            target_type,
            sketch_union,
        })
    }

    pub fn update_hash(&mut self, hash: u64) -> Result<(), String> {
        self.sketch_union.update_value(hash);
        Ok(())
    }

    pub fn merge_payload(&mut self, payload: &[u8]) -> Result<(), String> {
        let sketch = deserialize_hll(payload, "ds_hll")?;
        self.sketch_union.update(&sketch);
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        Ok(self
            .sketch_union
            .get_result(self.target_type.into_native())
            .serialize())
    }

    pub fn estimate(&self) -> Result<i64, String> {
        Ok(self.sketch_union.estimate().round() as i64)
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    use super::{HllHandle, HllTargetType, hll_estimate};

    #[test]
    fn known_compact_payload_is_still_readable() {
        let payload = STANDARD
            .decode("AgEHEQMIAQQ9nPUc")
            .expect("decode base64 payload");

        assert_eq!(hll_estimate(&payload).expect("estimate"), 1);
        assert_eq!(
            HllHandle::from_payload(&payload)
                .expect("handle from payload")
                .estimate()
                .expect("estimate from handle"),
            1
        );
    }

    #[test]
    fn native_hll_roundtrip_merges_without_cpp() {
        let mut left = HllHandle::new(10, HllTargetType::Hll6).expect("left handle");
        for value in 0_u64..64 {
            left.update_hash(value).expect("update left");
        }
        let left_payload = left.serialize().expect("serialize left");

        let mut right = HllHandle::new(10, HllTargetType::Hll6).expect("right handle");
        for value in 64_u64..128 {
            right.update_hash(value).expect("update right");
        }
        let right_payload = right.serialize().expect("serialize right");

        let mut merged = HllHandle::from_payload(&left_payload).expect("merged handle");
        merged.merge_payload(&right_payload).expect("merge right");

        let estimate = merged.estimate().expect("estimate merged");
        assert!(
            (110..=150).contains(&estimate),
            "merged estimate out of expected range: {estimate}"
        );
    }

    #[test]
    fn hll4_is_rejected_without_cpp_backend() {
        match HllHandle::new(10, HllTargetType::Hll4) {
            Ok(_) => panic!("hll4 should be rejected"),
            Err(err) => assert!(
                err.contains("HLL_4 is unsupported"),
                "unexpected error: {err}"
            ),
        }
    }
}
