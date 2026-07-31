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

//! Theta sketch wrapper for Iceberg Puffin NDV statistics.
//!
//! Wraps `datasketches::theta::ThetaSketch` and adds:
//! - Serialization to Apache DataSketches compact binary format
//! - Deserialization from compact binary format
//! - Set union across multiple sketches
//!
//! The compact binary format is compatible with Java/Spark/Trino DataSketches
//! implementations, enabling full interoperability via the standard
//! `apache-datasketches-theta-v1` Puffin blob type.

// NOTE: Most of the public surface is only consumed by callers that will be
// wired in by follow-up agents (StatsAssembler and StatsLoader). Suppress
// dead-code warnings until those land.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::hash::Hash;

use datasketches::theta::ThetaSketch;

// -- Compact binary format constants ------------------------------------------

/// Serial version for the compact sketch format.
const SERIAL_VERSION: u8 = 3;
/// Family ID for CompactSketch.
const FAMILY_COMPACT: u8 = 3;
/// Flag bit: sketch is empty.
const FLAG_EMPTY: u8 = 1 << 2;
/// Flag bit: compact format.
const FLAG_COMPACT: u8 = 1 << 3;
/// Flag bit: hash values are ordered (sorted ascending).
const FLAG_ORDERED: u8 = 1 << 4;
/// Default seed hash used by the Java DataSketches library (MurmurHash3-based
/// seed hash of DEFAULT_UPDATE_SEED = 9001).
const DEFAULT_SEED_HASH: u16 = 0x93CC;
/// Maximum theta value used by the Apache DataSketches library. Hashes are
/// 64-bit but theta is normalized over `i64::MAX` so that the Java/Spark
/// representation (signed `long`) aligns 1:1 with this Rust port. The
/// underlying `datasketches::theta` crate uses the same `MAX_THETA =
/// i64::MAX as u64` convention internally.
const MAX_THETA: u64 = i64::MAX as u64;

/// Theta sketch handle wrapping `datasketches::theta::ThetaSketch` with
/// serialize/deserialize/union support.
///
/// Follows the same ergonomic pattern as `HllHandle` in
/// `src/common/datasketches.rs`.
pub struct ThetaSketchHandle {
    /// Live mutable sketch used for building from input data.
    /// `None` when the handle was created via `deserialize` from a compact
    /// representation that cannot be replayed into a mutable sketch without
    /// losing accuracy (theta may have been reduced).
    inner: Option<ThetaSketch>,
    /// Compact representation: sorted retained hash values.
    /// Populated lazily on first serialize or immediately on deserialize.
    compact_hashes: Option<Vec<u64>>,
    /// Compact theta value. `MAX_THETA` (i64::MAX) means theta has never
    /// been reduced. The DataSketches convention treats `theta == MAX_THETA`
    /// as "exact" / "no rejection".
    compact_theta: u64,
    /// lg_k used to construct the sketch.
    lg_k: u8,
}

impl ThetaSketchHandle {
    /// Create a new empty sketch with the given `lg_k` (log2 of nominal size k).
    pub fn new(lg_k: u8) -> Self {
        Self {
            inner: Some(ThetaSketch::builder().lg_k(lg_k).build()),
            compact_hashes: None,
            compact_theta: MAX_THETA,
            lg_k,
        }
    }

    /// Update the sketch with a hashable value.
    pub fn update<T: Hash>(&mut self, value: T) {
        if let Some(ref mut sketch) = self.inner {
            sketch.update(value);
            // Invalidate cached compact form.
            self.compact_hashes = None;
        }
    }

    /// Update the sketch with an f64 value (canonical double handling).
    pub fn update_f64(&mut self, value: f64) {
        if let Some(ref mut sketch) = self.inner {
            sketch.update_f64(value);
            self.compact_hashes = None;
        }
    }

    /// Return the cardinality estimate.
    pub fn estimate(&self) -> f64 {
        if let Some(ref sketch) = self.inner {
            return sketch.estimate();
        }
        // Deserialized compact form: compute estimate from retained hashes and theta.
        let hashes = self.compact_hashes.as_deref().unwrap_or(&[]);
        if hashes.is_empty() {
            return 0.0;
        }
        // theta is normalized over MAX_THETA = i64::MAX, matching the
        // DataSketches Java/Rust convention. Using u64::MAX here would
        // halve theta_fraction and double the estimate.
        let theta_fraction = self.compact_theta as f64 / MAX_THETA as f64;
        hashes.len() as f64 / theta_fraction
    }

    /// Serialize the sketch to Apache DataSketches compact binary format.
    ///
    /// The output is compatible with the Java DataSketches library and can be
    /// embedded directly as an `apache-datasketches-theta-v1` Puffin blob.
    pub fn serialize(&self) -> Vec<u8> {
        let (hashes, theta) = self.extract_compact();
        Self::write_compact(&hashes, theta, self.lg_k)
    }

    /// Deserialize a sketch from Apache DataSketches compact binary format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err(format!(
                "theta sketch compact bytes too short: {} < 8",
                bytes.len()
            ));
        }

        let preamble_longs = bytes[0];
        let serial_version = bytes[1];
        let family = bytes[2];
        let lg_nom_size = bytes[3];
        // bytes[4] is lg_arr_size, unused for compact
        let flags = bytes[5];
        let seed_hash = u16::from_le_bytes([bytes[6], bytes[7]]);

        if serial_version != SERIAL_VERSION {
            return Err(format!(
                "unsupported theta sketch serial version: {serial_version} (expected {SERIAL_VERSION})"
            ));
        }
        if family != FAMILY_COMPACT {
            return Err(format!(
                "unsupported theta sketch family: {family} (expected {FAMILY_COMPACT})"
            ));
        }
        if seed_hash != DEFAULT_SEED_HASH {
            return Err(format!(
                "unsupported theta sketch seed hash: 0x{seed_hash:04X} (expected 0x{DEFAULT_SEED_HASH:04X})"
            ));
        }

        let is_empty = flags & FLAG_EMPTY != 0;

        if is_empty || preamble_longs == 1 {
            // Empty sketch.
            let lg_k = if lg_nom_size == 0 { 12 } else { lg_nom_size };
            return Ok(Self {
                inner: None,
                compact_hashes: Some(Vec::new()),
                compact_theta: MAX_THETA,
                lg_k,
            });
        }

        // Non-empty: preamble_longs >= 2, bytes 8..15 hold retained_count + padding.
        if bytes.len() < 16 {
            return Err(format!(
                "theta sketch compact bytes too short for non-empty header: {} < 16",
                bytes.len()
            ));
        }
        let retained_count =
            u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        // bytes[12..16] is padding

        let theta = if preamble_longs >= 3 {
            if bytes.len() < 24 {
                return Err(format!(
                    "theta sketch compact bytes too short for theta field: {} < 24",
                    bytes.len()
                ));
            }
            u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22],
                bytes[23],
            ])
        } else {
            MAX_THETA
        };

        let hash_offset = (preamble_longs as usize) * 8;
        let expected_len = hash_offset + retained_count * 8;
        if bytes.len() < expected_len {
            return Err(format!(
                "theta sketch compact bytes too short for hash values: {} < {expected_len}",
                bytes.len()
            ));
        }

        let mut hashes = Vec::with_capacity(retained_count);
        for i in 0..retained_count {
            let offset = hash_offset + i * 8;
            let hash = u64::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            hashes.push(hash);
        }
        // Ensure sorted ascending (compact ordered format).
        hashes.sort_unstable();

        let lg_k = if lg_nom_size == 0 { 12 } else { lg_nom_size };

        Ok(Self {
            inner: None,
            compact_hashes: Some(hashes),
            compact_theta: theta,
            lg_k,
        })
    }

    /// Rebuild a compact sketch from Core's bounded, canonical collection
    /// state.  This is intentionally crate-visible: it lets the native
    /// Iceberg statistics provider emit standard Puffin blobs without making
    /// an Iceberg representation part of the provider-neutral SPI.
    pub(crate) fn from_compact_parts(
        lg_k: u8,
        theta: u64,
        hashes: Vec<u64>,
    ) -> Result<Self, String> {
        if !(5..=16).contains(&lg_k) {
            return Err("Theta compact lg_k must be between 5 and 16".to_string());
        }
        if hashes.windows(2).any(|pair| pair[0] >= pair[1])
            || hashes.iter().any(|hash| *hash >= theta)
        {
            return Err("Theta compact hashes are not canonical".to_string());
        }
        Ok(Self {
            inner: None,
            compact_hashes: Some(hashes),
            compact_theta: theta,
            lg_k,
        })
    }

    /// Union multiple sketches into a single result sketch.
    ///
    /// Algorithm: collect all retained hashes from all sketches, take the
    /// minimum theta, keep only hashes strictly below min_theta, deduplicate,
    /// and sort.
    pub fn union(sketches: &[&Self]) -> Self {
        if sketches.is_empty() {
            return Self::new(12);
        }

        let lg_k = sketches.iter().map(|s| s.lg_k).max().unwrap_or(12);
        let mut min_theta = MAX_THETA;
        let mut all_hashes = BTreeSet::new();

        for sketch in sketches {
            let (hashes, theta) = sketch.extract_compact();
            min_theta = min_theta.min(theta);
            for &h in &hashes {
                all_hashes.insert(h);
            }
        }

        // Keep only hashes strictly below min_theta.
        let merged: Vec<u64> = all_hashes.into_iter().filter(|&h| h < min_theta).collect();

        Self {
            inner: None,
            compact_hashes: Some(merged),
            compact_theta: min_theta,
            lg_k,
        }
    }

    /// Deserialize multiple compact binary blobs and union them.
    pub fn union_bytes(serialized: &[&[u8]]) -> Result<Self, String> {
        let deserialized: Vec<Self> = serialized
            .iter()
            .map(|b| Self::deserialize(b))
            .collect::<Result<Vec<_>, _>>()?;
        let refs: Vec<&Self> = deserialized.iter().collect();
        Ok(Self::union(&refs))
    }

    // -- Internal helpers -----------------------------------------------------

    /// Extract the compact representation (sorted hashes + theta) from
    /// whichever internal state is available.
    fn extract_compact(&self) -> (Vec<u64>, u64) {
        if let Some(ref hashes) = self.compact_hashes
            && self.inner.is_none()
        {
            return (hashes.clone(), self.compact_theta);
        }
        if let Some(ref sketch) = self.inner {
            let theta = sketch.theta64();
            let mut hashes: Vec<u64> = sketch.iter().collect();
            hashes.sort_unstable();
            (hashes, theta)
        } else {
            (Vec::new(), self.compact_theta)
        }
    }

    /// Encode the compact binary representation.
    fn write_compact(hashes: &[u64], theta: u64, lg_k: u8) -> Vec<u8> {
        let is_empty = hashes.is_empty() && theta == MAX_THETA;
        let theta_is_max = theta == MAX_THETA;

        let preamble_longs: u8 = if is_empty {
            1
        } else if theta_is_max {
            2
        } else {
            3
        };

        let flags = if is_empty {
            FLAG_EMPTY | FLAG_COMPACT | FLAG_ORDERED
        } else {
            FLAG_COMPACT | FLAG_ORDERED
        };

        let header_size = (preamble_longs as usize) * 8;
        let total_size = header_size + hashes.len() * 8;
        let mut buf = Vec::with_capacity(total_size);

        // Header (first 8 bytes, always present)
        buf.push(preamble_longs);
        buf.push(SERIAL_VERSION);
        buf.push(FAMILY_COMPACT);
        buf.push(lg_k);
        buf.push(0); // lg_arr_size = 0 for compact
        buf.push(flags);
        buf.extend_from_slice(&DEFAULT_SEED_HASH.to_le_bytes());

        if !is_empty {
            // Retained count + padding (bytes 8..15)
            buf.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes()); // padding
        }

        if !theta_is_max && !is_empty {
            // Theta (bytes 16..23)
            buf.extend_from_slice(&theta.to_le_bytes());
        }

        // Hash values (sorted ascending, little-endian)
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sketch_estimate_is_zero() {
        let sketch = ThetaSketchHandle::new(12);
        assert!((sketch.estimate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn basic_update_and_estimate() {
        let mut sketch = ThetaSketchHandle::new(12);
        for i in 0..10_000 {
            sketch.update(i);
        }
        let est = sketch.estimate();
        // With lg_k=12 (k=4096), error should be within ~3% for 10k distinct values.
        assert!(
            (9_500.0..10_500.0).contains(&est),
            "estimate {est} out of expected range for 10k distinct values"
        );
    }

    #[test]
    fn serialize_empty_roundtrip() {
        let sketch = ThetaSketchHandle::new(12);
        let bytes = sketch.serialize();
        // Empty sketch: preamble_longs=1 → 8 bytes total
        assert_eq!(bytes.len(), 8);
        assert_eq!(bytes[0], 1); // preamble_longs
        assert_eq!(bytes[1], SERIAL_VERSION);
        assert_eq!(bytes[2], FAMILY_COMPACT);
        assert_eq!(bytes[5] & FLAG_EMPTY, FLAG_EMPTY);

        let restored = ThetaSketchHandle::deserialize(&bytes).expect("deserialize empty");
        assert!((restored.estimate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn serialize_deserialize_roundtrip_preserves_estimate() {
        let mut sketch = ThetaSketchHandle::new(12);
        for i in 0..10_000 {
            sketch.update(i);
        }
        let original_estimate = sketch.estimate();

        let bytes = sketch.serialize();
        let restored = ThetaSketchHandle::deserialize(&bytes).expect("deserialize");
        let restored_estimate = restored.estimate();

        let diff = (original_estimate - restored_estimate).abs();
        assert!(
            diff < 1.0,
            "roundtrip estimate drift too large: original={original_estimate}, restored={restored_estimate}"
        );
    }

    #[test]
    fn serialize_deserialize_small_sketch_exact_mode() {
        // Fewer items than k → theta stays at MAX, exact mode.
        let mut sketch = ThetaSketchHandle::new(12);
        for i in 0..100 {
            sketch.update(i);
        }
        let bytes = sketch.serialize();
        // preamble_longs should be 2 (non-empty, theta=MAX)
        assert_eq!(bytes[0], 2);

        let restored = ThetaSketchHandle::deserialize(&bytes).expect("deserialize");
        assert!(
            (restored.estimate() - 100.0).abs() < 1.0,
            "exact-mode roundtrip should give ~100, got {}",
            restored.estimate()
        );
    }

    #[test]
    fn serialize_deserialize_estimation_mode() {
        // More items than k → theta < MAX, estimation mode.
        let mut sketch = ThetaSketchHandle::new(10); // k=1024
        for i in 0..50_000 {
            sketch.update(i);
        }
        let bytes = sketch.serialize();
        // preamble_longs should be 3 (non-empty, theta < MAX)
        assert_eq!(bytes[0], 3);

        let restored = ThetaSketchHandle::deserialize(&bytes).expect("deserialize");
        let diff = (sketch.estimate() - restored.estimate()).abs();
        assert!(
            diff < 1.0,
            "estimation-mode roundtrip drift: original={}, restored={}",
            sketch.estimate(),
            restored.estimate()
        );
    }

    #[test]
    fn union_disjoint_sketches() {
        let mut a = ThetaSketchHandle::new(12);
        for i in 0..5_000 {
            a.update(i);
        }
        let mut b = ThetaSketchHandle::new(12);
        for i in 5_000..10_000 {
            b.update(i);
        }
        let combined = ThetaSketchHandle::union(&[&a, &b]);
        let est = combined.estimate();
        // Should be approximately 10k.
        assert!(
            (9_000.0..11_000.0).contains(&est),
            "disjoint union estimate {est} out of expected range"
        );
    }

    #[test]
    fn union_overlapping_sketches() {
        let mut a = ThetaSketchHandle::new(12);
        for i in 0..8_000 {
            a.update(i);
        }
        let mut b = ThetaSketchHandle::new(12);
        for i in 4_000..12_000 {
            b.update(i);
        }
        let combined = ThetaSketchHandle::union(&[&a, &b]);
        let est = combined.estimate();
        // True distinct count is 12000.
        assert!(
            (10_500.0..13_500.0).contains(&est),
            "overlapping union estimate {est} out of expected range for 12k distinct"
        );
    }

    #[test]
    fn union_empty_sketches() {
        let a = ThetaSketchHandle::new(12);
        let b = ThetaSketchHandle::new(12);
        let combined = ThetaSketchHandle::union(&[&a, &b]);
        assert!((combined.estimate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn union_one_empty_one_non_empty() {
        let empty = ThetaSketchHandle::new(12);
        let mut nonempty = ThetaSketchHandle::new(12);
        for i in 0..1_000 {
            nonempty.update(i);
        }
        let combined = ThetaSketchHandle::union(&[&empty, &nonempty]);
        let est = combined.estimate();
        assert!(
            (900.0..1_100.0).contains(&est),
            "union with empty sketch estimate {est} should be ~1000"
        );
    }

    #[test]
    fn union_bytes_roundtrip() {
        let mut a = ThetaSketchHandle::new(12);
        for i in 0..5_000 {
            a.update(i);
        }
        let mut b = ThetaSketchHandle::new(12);
        for i in 5_000..10_000 {
            b.update(i);
        }
        let a_bytes = a.serialize();
        let b_bytes = b.serialize();
        let combined = ThetaSketchHandle::union_bytes(&[&a_bytes, &b_bytes]).expect("union_bytes");
        let est = combined.estimate();
        assert!(
            (9_000.0..11_000.0).contains(&est),
            "union_bytes estimate {est} out of expected range"
        );
    }

    #[test]
    fn deserialize_rejects_bad_serial_version() {
        let mut bytes = ThetaSketchHandle::new(12).serialize();
        bytes[1] = 99; // bad serial version
        assert!(ThetaSketchHandle::deserialize(&bytes).is_err());
    }

    #[test]
    fn deserialize_rejects_bad_family() {
        let mut bytes = ThetaSketchHandle::new(12).serialize();
        bytes[2] = 99; // bad family
        assert!(ThetaSketchHandle::deserialize(&bytes).is_err());
    }

    #[test]
    fn deserialize_rejects_truncated_bytes() {
        assert!(ThetaSketchHandle::deserialize(&[0; 4]).is_err());
    }

    #[test]
    fn compact_format_header_fields() {
        let mut sketch = ThetaSketchHandle::new(12);
        for i in 0..100 {
            sketch.update(i);
        }
        let bytes = sketch.serialize();

        // Verify header fields.
        assert_eq!(bytes[1], SERIAL_VERSION);
        assert_eq!(bytes[2], FAMILY_COMPACT);
        assert_eq!(bytes[3], 12); // lg_k
        assert_eq!(bytes[4], 0); // lg_arr_size
        assert!(bytes[5] & FLAG_COMPACT != 0);
        assert!(bytes[5] & FLAG_ORDERED != 0);
        let seed_hash = u16::from_le_bytes([bytes[6], bytes[7]]);
        assert_eq!(seed_hash, DEFAULT_SEED_HASH);
    }

    #[test]
    fn update_f64_works() {
        let mut sketch = ThetaSketchHandle::new(12);
        for i in 0..1_000 {
            sketch.update_f64(i as f64);
        }
        let est = sketch.estimate();
        assert!(
            (900.0..1_100.0).contains(&est),
            "f64 update estimate {est} should be ~1000"
        );
    }

    #[test]
    fn union_serialized_then_re_serialize() {
        // Verify that a union result can be serialized and deserialized again.
        let mut a = ThetaSketchHandle::new(12);
        for i in 0..3_000 {
            a.update(i);
        }
        let mut b = ThetaSketchHandle::new(12);
        for i in 3_000..6_000 {
            b.update(i);
        }
        let combined = ThetaSketchHandle::union(&[&a, &b]);
        let bytes = combined.serialize();
        let restored = ThetaSketchHandle::deserialize(&bytes).expect("deserialize union result");
        let diff = (combined.estimate() - restored.estimate()).abs();
        assert!(
            diff < 1.0,
            "union result serialize/deserialize drift: {} vs {}",
            combined.estimate(),
            restored.estimate()
        );
    }
}
