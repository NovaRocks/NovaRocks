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

//! Per-refresh MV provenance record, carried in an Iceberg snapshot's
//! `summary.additional_properties`.
//!
//! [`MvProvenanceV1`] is a superset of the narrow
//! [`crate::commit::mv_refresh_ref::MvRefreshSnapshotMarker`]:
//! it carries the same `refresh_id` / `mv_id` / `token` identity fields, plus
//! the refresh technique, the per-base watermark (`from_snapshot` /
//! `to_snapshot`), a definition fingerprint, and the resulting row count.
//! [`MvProvenanceV1::to_summary_properties`] always emits the three narrow
//! marker keys alongside the full record so that
//! [`crate::commit::mv_refresh_ref::snapshot_matches_refresh_marker`]
//! and W3b crash recovery keep working unchanged against a provenance-carrying
//! snapshot.
//!
//! The record has no physical fields (no timestamps, no paths, no local
//! snapshot ids): the `from`/`to` snapshot ids in each [`ProvenanceBase`] are
//! semantic watermarks against the shared base lake, so they are identical
//! across a byte-for-byte rebuild from the same base history. That means the
//! full canonical-JSON hash ([`MvProvenanceV1::content_hash`]) already is the
//! "normalized" hash — there is no separate physical/logical split like some
//! descriptor types need.
//!
//! [`MvProvenanceV1::waterline_hash`] hashes only the watermark projection
//! (`table_fqn` + `uuid` + `to_snapshot` per base) and is what the W0 harness
//! exposes as `WaterlineHash`: it changes exactly when the *consumed* base
//! state changes, independent of `rows`, `technique`, or refresh identity.
//!
//! This module also hosts the W3a spike tests (`spike_tests`) proving
//! iceberg-rust 0.9.0 can commit a *data-free* snapshot (needed for the
//! "result unchanged but watermark advances" metadata-only refresh case);
//! see their doc comments for the two candidate approaches.

use std::collections::BTreeMap;

use crate::iceberg::spec::Snapshot;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::commit::{MV_ID_PROP, MV_REFRESH_ID_PROP, MV_REFRESH_TOKEN_PROP};

pub const MV_PROVENANCE_V1_PROP: &str = "novarocks.mv.provenance.v1";
pub const MV_PROVENANCE_VERSION: u16 = 1;
pub const MV_REFRESH_ROW_COUNT_PROP: &str = "novarocks.mv.refresh.row_count";

/// How this refresh derived the MV's new state from its bases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

/// Per-base watermark consumed by a single refresh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceBase {
    pub table_fqn: String,
    pub uuid: String,
    /// Inclusive watermark lower bound consumed by the previous refresh;
    /// `None` on the first refresh.
    #[serde(default)]
    pub from_snapshot: Option<i64>,
    /// Base snapshot consumed by THIS refresh (the new watermark).
    pub to_snapshot: i64,
}

/// Per-refresh provenance record, superset of `MvRefreshSnapshotMarker`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvProvenanceV1 {
    pub provenance_version: u16,
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
    pub technique: RefreshTechnique,
    pub bases: Vec<ProvenanceBase>,
    pub definition_fingerprint: String,
    pub rows: i64,
}

/// Watermark-only projection of a [`ProvenanceBase`], used by
/// [`MvProvenanceV1::waterline_hash`]. Deliberately excludes `from_snapshot`
/// (a historical detail, not part of the current watermark) and every
/// non-base field on the record (`technique`, `rows`, ids).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WaterlineBase {
    table_fqn: String,
    uuid: String,
    to_snapshot: i64,
}

impl MvProvenanceV1 {
    /// Render the full record as canonical (key-sorted) JSON, plus the three
    /// narrow `MvRefreshSnapshotMarker` keys, so a provenance-carrying
    /// snapshot still satisfies `snapshot_matches_refresh_marker` and W3b
    /// crash recovery unchanged.
    pub fn to_summary_properties(&self) -> Result<BTreeMap<String, String>, String> {
        let mut props = BTreeMap::new();
        props.insert(MV_REFRESH_ID_PROP.to_string(), self.refresh_id.to_string());
        props.insert(MV_ID_PROP.to_string(), self.mv_id.to_string());
        props.insert(MV_REFRESH_TOKEN_PROP.to_string(), self.token.clone());
        props.insert(MV_PROVENANCE_V1_PROP.to_string(), self.to_canonical_json()?);
        Ok(props)
    }

    /// Read and parse the provenance record from a snapshot's summary, if
    /// present. Returns `Ok(None)` when the property key is absent, `Err` on
    /// malformed JSON or a version mismatch.
    pub fn from_snapshot_summary(snapshot: &Snapshot) -> Result<Option<Self>, String> {
        let Some(raw) = snapshot
            .summary()
            .additional_properties
            .get(MV_PROVENANCE_V1_PROP)
        else {
            return Ok(None);
        };
        Self::from_json(raw).map(Some)
    }

    /// Sha256 hex over the canonical JSON of the whole record. The record has
    /// no physical fields, so this full hash already serves as the
    /// "normalized" content hash.
    pub fn content_hash(&self) -> Result<String, String> {
        let canonical_json = self.to_canonical_json()?;
        Ok(hex_encode(&Sha256::digest(canonical_json.as_bytes())))
    }

    /// Return the same immutable provenance identity with the row-count fact
    /// observed from the committed Iceberg snapshot summary.
    pub fn with_rows(&self, rows: i64) -> Result<Self, String> {
        if rows < 0 {
            return Err("MV provenance row count cannot be negative".to_string());
        }
        let mut updated = self.clone();
        updated.rows = rows;
        Ok(updated)
    }

    /// Sha256 hex over a canonical JSON of just the watermark projection: a
    /// sorted list of `{table_fqn, uuid, to_snapshot}` from `bases`. This is
    /// the hash the W0 harness exposes as `WaterlineHash` — it changes
    /// exactly when the consumed base state changes.
    pub fn waterline_hash(&self) -> Result<String, String> {
        let mut waterline_bases: Vec<WaterlineBase> = self
            .bases
            .iter()
            .map(|base| WaterlineBase {
                table_fqn: base.table_fqn.clone(),
                uuid: base.uuid.clone(),
                to_snapshot: base.to_snapshot,
            })
            .collect();
        waterline_bases.sort_by(|left, right| {
            (left.table_fqn.as_str(), left.uuid.as_str())
                .cmp(&(right.table_fqn.as_str(), right.uuid.as_str()))
        });

        let value = serde_json::to_value(&waterline_bases)
            .map_err(|err| format!("failed to serialize MV provenance waterline: {err}"))?;
        let canonical_json = serde_json::to_string(&sort_json_value(value)).map_err(|err| {
            format!("failed to render canonical MV provenance waterline JSON: {err}")
        })?;
        Ok(hex_encode(&Sha256::digest(canonical_json.as_bytes())))
    }

    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV provenance: {err}"))?;
        serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV provenance JSON: {err}"))
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        let record: Self = serde_json::from_str(s)
            .map_err(|err| format!("failed to parse MV provenance JSON: {err}"))?;
        if record.provenance_version != MV_PROVENANCE_VERSION {
            return Err(format!(
                "unsupported MV provenance version: expected {}, got {}",
                MV_PROVENANCE_VERSION, record.provenance_version
            ));
        }
        Ok(record)
    }
}

fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_value).collect()),
        Value::Object(object) => {
            let mut entries = object
                .into_iter()
                .map(|(key, value)| (key, sort_json_value(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));

            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{Operation, Summary};

    fn sample() -> MvProvenanceV1 {
        MvProvenanceV1 {
            provenance_version: MV_PROVENANCE_VERSION,
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
            technique: RefreshTechnique::Incremental,
            bases: vec![
                ProvenanceBase {
                    table_fqn: "ice.sales.orders".to_string(),
                    uuid: "uuid-orders".to_string(),
                    from_snapshot: Some(100),
                    to_snapshot: 200,
                },
                ProvenanceBase {
                    table_fqn: "ice.sales.customers".to_string(),
                    uuid: "uuid-customers".to_string(),
                    from_snapshot: None,
                    to_snapshot: 50,
                },
            ],
            definition_fingerprint: "fp-abc123".to_string(),
            rows: 4242,
        }
    }

    fn snapshot_with_properties(properties: BTreeMap<String, String>) -> Snapshot {
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: properties.into_iter().collect(),
        };
        Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build()
    }

    #[test]
    fn provenance_summary_props_are_superset_of_marker_and_round_trip() {
        let record = sample();
        let props = record.to_summary_properties().unwrap();

        assert_eq!(
            props.get(MV_REFRESH_ID_PROP).map(String::as_str),
            Some("77")
        );
        assert_eq!(props.get(MV_ID_PROP).map(String::as_str), Some("12"));
        assert_eq!(
            props.get(MV_REFRESH_TOKEN_PROP).map(String::as_str),
            Some("token-77")
        );
        assert!(props.contains_key(MV_PROVENANCE_V1_PROP));

        let snapshot = snapshot_with_properties(props);
        let parsed = MvProvenanceV1::from_snapshot_summary(&snapshot).unwrap();

        assert_eq!(parsed, Some(record));
    }

    #[test]
    fn from_snapshot_summary_none_when_absent() {
        let snapshot = snapshot_with_properties(BTreeMap::new());

        let parsed = MvProvenanceV1::from_snapshot_summary(&snapshot).unwrap();

        assert_eq!(parsed, None);
    }

    #[test]
    fn from_snapshot_summary_rejects_version_mismatch() {
        let mut props = BTreeMap::new();
        props.insert(
            MV_PROVENANCE_V1_PROP.to_string(),
            r#"{"provenance_version":2,"refresh_id":1,"mv_id":1,"token":"t","technique":"FULL","bases":[],"definition_fingerprint":"fp","rows":0}"#.to_string(),
        );
        let snapshot = snapshot_with_properties(props);

        let err = MvProvenanceV1::from_snapshot_summary(&snapshot).unwrap_err();

        assert!(
            err.contains("unsupported MV provenance version"),
            "got: {err}"
        );
        assert!(err.contains('2'), "got: {err}");
    }

    #[test]
    fn content_hash_stable_and_waterline_detects_watermark_change() {
        let record = sample();
        let clone = record.clone();

        assert_eq!(
            record.content_hash().unwrap(),
            clone.content_hash().unwrap()
        );
        assert_eq!(
            record.waterline_hash().unwrap(),
            clone.waterline_hash().unwrap()
        );

        // Mutating the watermark changes BOTH hashes.
        let mut watermark_changed = record.clone();
        watermark_changed.bases[0].to_snapshot += 1;
        assert_ne!(
            record.content_hash().unwrap(),
            watermark_changed.content_hash().unwrap()
        );
        assert_ne!(
            record.waterline_hash().unwrap(),
            watermark_changed.waterline_hash().unwrap()
        );

        // Mutating `rows` changes content_hash but NOT waterline_hash: the
        // waterline is watermark-only.
        let mut rows_changed = record.clone();
        rows_changed.rows += 1;
        assert_ne!(
            record.content_hash().unwrap(),
            rows_changed.content_hash().unwrap()
        );
        assert_eq!(
            record.waterline_hash().unwrap(),
            rows_changed.waterline_hash().unwrap()
        );
    }
}
