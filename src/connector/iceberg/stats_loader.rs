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

use iceberg::io::FileIO;
use iceberg::puffin::{APACHE_DATASKETCHES_THETA_V1, PuffinReader};
use iceberg::spec::TableMetadata;

use super::theta_sketch::ThetaSketchHandle;

/// Loader for Iceberg Puffin statistics. Produces a `field_id → NDV` map.
pub(crate) struct StatsLoader;

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
        match Self::load_ndv_inner(stats_file.statistics_path.as_str(), file_io).await {
            Ok(map) => map,
            Err(err) => {
                tracing::warn!(
                    snapshot_id,
                    puffin_path = %stats_file.statistics_path,
                    error = %err,
                    "iceberg puffin stats load failed; falling back to manifest heuristics",
                );
                HashMap::new()
            }
        }
    }

    async fn load_ndv_inner(
        puffin_path: &str,
        file_io: &FileIO,
    ) -> Result<HashMap<i32, f64>, String> {
        let input_file = file_io
            .new_input(puffin_path)
            .map_err(|e| format!("open puffin {puffin_path}: {e}"))?;
        let reader = PuffinReader::new(input_file);
        let file_metadata = reader
            .file_metadata()
            .await
            .map_err(|e| format!("read puffin metadata: {e}"))?;

        let mut ndv_map: HashMap<i32, f64> = HashMap::new();
        for blob_metadata in file_metadata.blobs() {
            if blob_metadata.blob_type() != APACHE_DATASKETCHES_THETA_V1 {
                continue;
            }
            let Some(&field_id) = blob_metadata.fields().first() else {
                // Theta blob without a field id has no consumer in the
                // optimizer — skip it rather than producing a phantom entry.
                continue;
            };
            let blob = match reader.blob(blob_metadata).await {
                Ok(b) => b,
                Err(err) => {
                    tracing::warn!(
                        field_id,
                        error = %err,
                        "iceberg puffin theta blob read failed; skipping field",
                    );
                    continue;
                }
            };
            match ThetaSketchHandle::deserialize(blob.data()) {
                Ok(sketch) => {
                    ndv_map.insert(field_id, sketch.estimate().max(0.0));
                }
                Err(err) => {
                    tracing::warn!(
                        field_id,
                        error = %err,
                        "iceberg puffin theta blob deserialize failed; skipping field",
                    );
                }
            }
        }
        Ok(ndv_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    use iceberg::puffin::{Blob, CompressionCodec, PuffinWriter};
    use tempfile::tempdir;

    /// Build a tiny Puffin file via `PuffinWriter` for round-trip tests.
    /// The file lives under a temp directory on the local filesystem so it
    /// can be re-opened via `FileIO`.
    async fn write_puffin_file(path: &str, sketches: &[(i32, &ThetaSketchHandle, i64)]) -> FileIO {
        let file_io = FileIO::new_with_fs();
        let output = file_io.new_output(path).expect("new output");
        let mut writer = PuffinWriter::new(&output, Map::new(), false)
            .await
            .expect("puffin writer");
        for (field_id, sketch, snapshot_id) in sketches {
            let blob = Blob::builder()
                .r#type(APACHE_DATASKETCHES_THETA_V1.to_string())
                .fields(vec![*field_id])
                .snapshot_id(*snapshot_id)
                .sequence_number(1)
                .data(sketch.serialize())
                .properties(Map::new())
                .build();
            writer
                .add(blob, CompressionCodec::None)
                .await
                .expect("write blob");
        }
        writer.close().await.expect("close puffin writer");
        file_io
    }

    fn build_sketch(values: i64) -> ThetaSketchHandle {
        let mut s = ThetaSketchHandle::new(12);
        for i in 0..values {
            s.update(i);
        }
        s
    }

    #[tokio::test]
    async fn loads_ndv_from_local_puffin() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("stats.puffin");
        let path_str = format!("file://{}", path.display());

        let sketch_a = build_sketch(1_000);
        let sketch_b = build_sketch(500);
        let file_io =
            write_puffin_file(&path_str, &[(1, &sketch_a, 100), (2, &sketch_b, 100)]).await;

        let map = StatsLoader::load_ndv_inner(&path_str, &file_io)
            .await
            .expect("load_ndv_inner");
        assert_eq!(map.len(), 2);
        let ndv1 = map.get(&1).copied().unwrap_or(0.0);
        let ndv2 = map.get(&2).copied().unwrap_or(0.0);
        assert!(
            (900.0..1100.0).contains(&ndv1),
            "field 1 NDV {ndv1} should be ~1000"
        );
        assert!(
            (450.0..550.0).contains(&ndv2),
            "field 2 NDV {ndv2} should be ~500"
        );
    }

    #[tokio::test]
    async fn returns_empty_on_missing_puffin_file() {
        let file_io = FileIO::new_with_fs();
        let map = StatsLoader::load_ndv_inner("file:///definitely/missing.puffin", &file_io).await;
        assert!(map.is_err());
    }

    #[tokio::test]
    async fn skips_non_theta_blobs() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("mixed.puffin");
        let path_str = format!("file://{}", path.display());
        let file_io = FileIO::new_with_fs();

        let output = file_io.new_output(&path_str).expect("new output");
        let mut writer = PuffinWriter::new(&output, Map::new(), false)
            .await
            .expect("puffin writer");
        // A non-theta blob — should be ignored by the loader.
        let other_blob = Blob::builder()
            .r#type("something-else".to_string())
            .fields(vec![10])
            .snapshot_id(7)
            .sequence_number(1)
            .data(vec![1, 2, 3, 4])
            .properties(Map::new())
            .build();
        writer
            .add(other_blob, CompressionCodec::None)
            .await
            .expect("write");
        // A theta blob — should be picked up.
        let sketch = build_sketch(200);
        let theta_blob = Blob::builder()
            .r#type(APACHE_DATASKETCHES_THETA_V1.to_string())
            .fields(vec![3])
            .snapshot_id(7)
            .sequence_number(1)
            .data(sketch.serialize())
            .properties(Map::new())
            .build();
        writer
            .add(theta_blob, CompressionCodec::None)
            .await
            .expect("write");
        writer.close().await.expect("close");

        let map = StatsLoader::load_ndv_inner(&path_str, &file_io)
            .await
            .expect("load ndv");
        assert_eq!(map.len(), 1, "only theta blob should be loaded: {map:?}");
        assert!(map.contains_key(&3));
    }
}
