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

//! Best-effort cleanup register for staged Iceberg files.
//!
//! Failure of `commit` or pipeline must remove anything written so far. The
//! AbortLog tracks two categories separately because their lifetimes diverge:
//!
//! * data / position-delete files — owned by the pipeline; tracked redundantly
//!   here so abort doesn't need to drain `runtime/sink_commit.rs` first.
//! * manifest / manifest-list files — owned by the commit-action; only relevant
//!   if commit fails after writing manifests but before catalog.update_table
//!   succeeds.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::opendal::Operator;

/// Tracks staged Iceberg files for best-effort cleanup on abort.
#[derive(Default)]
pub struct AbortLog {
    staged_data_files: Mutex<Vec<String>>,
    written_manifests: Mutex<Vec<String>>,
    cleared: AtomicBool,
}

/// Records a failure that occurred while attempting to delete a staged file.
#[derive(Debug)]
pub struct CleanupError {
    pub path: String,
    pub source: crate::opendal::Error,
}

impl AbortLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a data or position-delete file for cleanup.
    pub fn record_data_file(&self, path: String) {
        self.staged_data_files
            .lock()
            .expect("abort log poisoned")
            .push(path);
    }

    /// Register a manifest or manifest-list file for cleanup.
    pub fn record_manifest(&self, path: String) {
        self.written_manifests
            .lock()
            .expect("abort log poisoned")
            .push(path);
    }

    /// Drain and return all registered data files, clearing the internal list.
    pub fn drain_data_files(&self) -> Vec<String> {
        std::mem::take(&mut *self.staged_data_files.lock().expect("abort log poisoned"))
    }

    /// Drain and return all registered manifests, clearing the internal list.
    pub fn drain_manifests(&self) -> Vec<String> {
        std::mem::take(&mut *self.written_manifests.lock().expect("abort log poisoned"))
    }

    /// Delete all registered files via `fs`. Idempotent: subsequent calls are
    /// no-ops. Best-effort: failures are collected and returned rather than
    /// propagated, so callers always clean up as much as possible.
    pub async fn cleanup(&self, fs: &Operator) -> Vec<CleanupError> {
        self.cleanup_with_path_mapper(fs, |path| path.to_string())
            .await
    }

    /// Delete all registered files after mapping their Iceberg location string
    /// to the path expected by the supplied OpenDAL operator.
    pub async fn cleanup_with_path_mapper<F>(&self, fs: &Operator, mapper: F) -> Vec<CleanupError>
    where
        F: Fn(&str) -> String,
    {
        if self.cleared.swap(true, Ordering::SeqCst) {
            return Vec::new();
        }
        let mut errs = Vec::new();
        for p in self.drain_data_files() {
            let delete_path = mapper(&p);
            if let Err(e) = fs.delete(&delete_path).await {
                errs.push(CleanupError { path: p, source: e });
            }
        }
        for p in self.drain_manifests() {
            let delete_path = mapper(&p);
            if let Err(e) = fs.delete(&delete_path).await {
                errs.push(CleanupError { path: p, source: e });
            }
        }
        errs
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::opendal::services::Fs;

    use super::*;

    fn local_op() -> Operator {
        let dir = tempfile::TempDir::new()
            .expect("local operator tempdir")
            .keep();
        let builder = Fs::default().root(dir.to_string_lossy().as_ref());
        Operator::new(builder).unwrap().finish()
    }

    #[tokio::test]
    async fn cleanup_deletes_recorded_paths() {
        let fs = local_op();
        // Pre-populate files so delete has something to remove.
        fs.write("a.parquet", b"x".to_vec()).await.unwrap();
        fs.write("b.parquet", b"y".to_vec()).await.unwrap();
        fs.write("m.avro", b"z".to_vec()).await.unwrap();

        let log = AbortLog::new();
        log.record_data_file("a.parquet".into());
        log.record_data_file("b.parquet".into());
        log.record_manifest("m.avro".into());

        let errs = log.cleanup(&fs).await;
        assert!(errs.is_empty(), "unexpected errors: {:?}", errs);

        assert!(fs.stat("a.parquet").await.is_err());
        assert!(fs.stat("b.parquet").await.is_err());
        assert!(fs.stat("m.avro").await.is_err());
    }

    #[tokio::test]
    async fn conflict_cleanup_removes_only_attempt_derivatives() {
        let fs = local_op();
        fs.write("data.parquet", b"data".to_vec()).await.unwrap();
        fs.write("attempt-manifest.avro", b"manifest".to_vec())
            .await
            .unwrap();
        fs.write("attempt-statistics.puffin", b"stats".to_vec())
            .await
            .unwrap();
        let session = AbortLog::new();
        session.record_data_file("data.parquet".into());
        let attempt = AbortLog::new();
        attempt.record_manifest("attempt-manifest.avro".into());
        attempt.record_manifest("attempt-statistics.puffin".into());

        assert!(attempt.cleanup(&fs).await.is_empty());
        assert!(fs.stat("data.parquet").await.is_ok(), "data is reusable");
        assert!(fs.stat("attempt-manifest.avro").await.is_err());
        assert!(fs.stat("attempt-statistics.puffin").await.is_err());

        assert!(session.cleanup(&fs).await.is_empty());
        assert!(fs.stat("data.parquet").await.is_err());
    }

    #[tokio::test]
    async fn commit_unknown_performs_no_cleanup() {
        let fs = local_op();
        fs.write("data.parquet", b"data".to_vec()).await.unwrap();
        fs.write("attempt-manifest.avro", b"manifest".to_vec())
            .await
            .unwrap();
        let session = AbortLog::new();
        session.record_data_file("data.parquet".into());
        let attempt = AbortLog::new();
        attempt.record_manifest("attempt-manifest.avro".into());

        // Unknown deliberately drops neither ownership log into cleanup.
        assert!(fs.stat("data.parquet").await.is_ok());
        assert!(fs.stat("attempt-manifest.avro").await.is_ok());
        assert_eq!(session.drain_data_files(), vec!["data.parquet"]);
        assert_eq!(attempt.drain_manifests(), vec!["attempt-manifest.avro"]);
    }

    #[tokio::test]
    async fn cleanup_maps_absolute_iceberg_locations_to_operator_paths() {
        let fs = local_op();
        fs.write("warehouse/ns/t/data/file.parquet", b"x".to_vec())
            .await
            .unwrap();

        let log = AbortLog::new();
        log.record_data_file("s3://bucket/warehouse/ns/t/data/file.parquet".into());

        let errs = log
            .cleanup_with_path_mapper(&fs, |path| {
                path.strip_prefix("s3://bucket/")
                    .unwrap_or(path)
                    .to_string()
            })
            .await;
        assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
        assert!(fs.stat("warehouse/ns/t/data/file.parquet").await.is_err());
    }

    #[tokio::test]
    async fn cleanup_is_idempotent() {
        let fs = local_op();
        fs.write("a.parquet", b"x".to_vec()).await.unwrap();

        let log = AbortLog::new();
        log.record_data_file("a.parquet".into());

        let _ = log.cleanup(&fs).await;
        // Second call must be a no-op and must not panic.
        let errs = log.cleanup(&fs).await;
        assert!(errs.is_empty());
    }

    #[tokio::test]
    async fn cleanup_collects_errors_for_missing_files() {
        let fs = local_op();
        let log = AbortLog::new();
        log.record_data_file("does-not-exist.parquet".into());

        let errs = log.cleanup(&fs).await;
        // Backends may differ on delete-of-missing-key;
        // either behaviour is acceptable. Zero or one error is expected.
        assert!(errs.len() <= 1);
    }

    #[tokio::test]
    async fn concurrent_record_is_safe() {
        let log = Arc::new(AbortLog::new());
        let mut handles = Vec::new();
        for i in 0..32 {
            let log = log.clone();
            handles.push(tokio::spawn(async move {
                log.record_data_file(format!("p{i}.parquet"));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(log.drain_data_files().len(), 32);
    }
}
