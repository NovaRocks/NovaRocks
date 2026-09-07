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

//! Iceberg Puffin statistics loader.
//!
//! Given a table's [`TableMetadata`] and a snapshot id, read the registered
//! Puffin `apache-datasketches-theta-v1` blobs and return per-column NDV
//! estimates keyed by Iceberg field id.
//!
//! All errors are logged and downgraded to an empty map; missing or corrupt
//! statistics never block query planning — the optimizer simply falls back
//! to its manifest-derived heuristics. This matches the spec section 9
//! "Error handling and graceful degradation".

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::iceberg::io::FileIO;
use crate::iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, PuffinReader};
use crate::iceberg::spec::{StatisticsFile, TableMetadata};
use novarocks_connector_iceberg_functions::validate_compact_theta;

/// Standard Puffin property carrying the estimate represented by a Theta body.
pub const NDV_PROPERTY: &str = "ndv";

static THETA_BODY_READS: AtomicU64 = AtomicU64::new(0);

/// Loader for Iceberg Puffin statistics. Produces a `field_id → NDV` map.
pub struct StatsLoader;

impl StatsLoader {
    /// Read NDV estimates for the given snapshot from the table's registered
    /// Puffin statistics file.
    ///
    /// Returns an empty map when:
    /// - No `StatisticsFile` is registered for `snapshot_id`.
    /// - The Puffin file fails to open or parse.
    /// - The file contains no `apache-datasketches-theta-v1` blobs.
    ///
    /// Failures during blob deserialization log a warning but do not abort
    /// the load; surviving columns are returned and missing ones are silently
    /// dropped from the map.
    pub async fn load_ndv(
        table_metadata: &TableMetadata,
        snapshot_id: i64,
        file_io: &FileIO,
    ) -> HashMap<i32, f64> {
        let Some(stats_file) = table_metadata.statistics_for_snapshot(snapshot_id) else {
            return HashMap::new();
        };
        let _ = file_io;
        match Self::load_ndv_from_metadata(stats_file) {
            Ok(map) => map,
            Err(error) => {
                tracing::warn!(snapshot_id, error = %error, "iceberg statistics metadata is unusable");
                HashMap::new()
            }
        }
    }

    /// Read ordinary optimizer NDV without opening the Puffin object. Iceberg
    /// copies blob properties into `StatisticsFile.blob_metadata`, so this is
    /// both the cheapest path and the authoritative metadata contract.
    pub fn load_ndv_from_metadata(
        statistics: &StatisticsFile,
    ) -> Result<HashMap<i32, f64>, String> {
        let mut ndv = HashMap::new();
        for blob in &statistics.blob_metadata {
            if blob.r#type != APACHE_DATASKETCHES_THETA_V1 {
                continue;
            }
            let [field_id] = blob.fields.as_slice() else {
                return Err("Theta blob metadata must name exactly one field".to_string());
            };
            let rendered = blob.properties.get(NDV_PROPERTY).ok_or_else(|| {
                format!("Theta blob metadata for field {field_id} is missing ndv")
            })?;
            let value = rendered.parse::<f64>().map_err(|error| {
                format!("Theta ndv for field {field_id} is not numeric: {error}")
            })?;
            if !value.is_finite() || value < 0.0 {
                return Err(format!(
                    "Theta ndv for field {field_id} must be finite and non-negative"
                ));
            }
            if ndv.insert(*field_id, value).is_some() {
                return Err(format!(
                    "statistics metadata contains duplicate Theta blobs for field {field_id}"
                ));
            }
        }
        Ok(ndv)
    }

    pub fn theta_body_reads() -> u64 {
        THETA_BODY_READS.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn reset_theta_body_reads_for_test() {
        THETA_BODY_READS.store(0, Ordering::Relaxed);
    }

    /// Load compact bodies only for parent union/rewrite. Ordinary optimizer
    /// reads must use `load_ndv_from_metadata` and never call this method.
    pub(crate) async fn load_theta_bodies_from_file(
        puffin_path: &str,
        file_io: &FileIO,
    ) -> Result<HashMap<i32, Vec<u8>>, String> {
        let input_file = file_io
            .new_input(puffin_path)
            .map_err(|error| format!("open Puffin {puffin_path}: {error}"))?;
        let reader = PuffinReader::new(input_file);
        let metadata = reader
            .file_metadata()
            .await
            .map_err(|error| format!("read Puffin metadata: {error}"))?;
        let mut bodies = HashMap::new();
        for blob_metadata in metadata.blobs() {
            if blob_metadata.blob_type() != APACHE_DATASKETCHES_THETA_V1 {
                continue;
            }
            let [field_id] = blob_metadata.fields() else {
                return Err("Theta blob metadata must name exactly one field".to_string());
            };
            THETA_BODY_READS.fetch_add(1, Ordering::Relaxed);
            let blob = reader
                .blob(blob_metadata)
                .await
                .map_err(|error| format!("read Theta blob for field {field_id}: {error}"))?;
            validate_compact_theta(blob.data())
                .map_err(|error| format!("validate Theta blob for field {field_id}: {error}"))?;
            if bodies.insert(*field_id, blob.data().to_vec()).is_some() {
                return Err(format!(
                    "Puffin contains duplicate Theta blobs for field {field_id}"
                ));
            }
        }
        Ok(bodies)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    fn metadata_with_ndv(field_id: i32, ndv: &str) -> StatisticsFile {
        StatisticsFile {
            snapshot_id: 7,
            statistics_path: "file:///must-not-be-opened.puffin".to_string(),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: vec![crate::iceberg::spec::BlobMetadata {
                r#type: APACHE_DATASKETCHES_THETA_V1.to_string(),
                snapshot_id: 7,
                sequence_number: 1,
                fields: vec![field_id],
                properties: Map::from([(NDV_PROPERTY.to_string(), ndv.to_string())]),
            }],
        }
    }

    #[test]
    fn optimizer_ndv_reads_only_registered_blob_metadata() {
        StatsLoader::reset_theta_body_reads_for_test();
        let ndv = StatsLoader::load_ndv_from_metadata(&metadata_with_ndv(11, "42.5"))
            .expect("metadata NDV");
        assert_eq!(ndv, Map::from([(11, 42.5)]));
        assert_eq!(StatsLoader::theta_body_reads(), 0);
    }

    #[test]
    fn metadata_ndv_rejects_missing_invalid_and_duplicate_values() {
        let mut missing = metadata_with_ndv(11, "42");
        missing.blob_metadata[0].properties.clear();
        assert!(StatsLoader::load_ndv_from_metadata(&missing).is_err());
        assert!(StatsLoader::load_ndv_from_metadata(&metadata_with_ndv(11, "NaN")).is_err());

        let mut duplicate = metadata_with_ndv(11, "42");
        duplicate
            .blob_metadata
            .push(duplicate.blob_metadata[0].clone());
        assert!(StatsLoader::load_ndv_from_metadata(&duplicate).is_err());
    }
}
