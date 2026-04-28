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

//! `Transaction::fast_append` wrapper for INSERT INTO. Simplest of the
//! three commit-action implementations — iceberg-rust handles all manifest
//! authoring including v2/v3 format selection.

use async_trait::async_trait;
use iceberg::spec::DataContentType;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use super::action::{CommitCtx, IcebergCommitAction};
use super::data_file::written_file_to_iceberg_data_file;
use super::types::CommitOutcome;

pub struct FastAppendCommit;

#[async_trait]
impl IcebergCommitAction for FastAppendCommit {
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        let written = ctx.collector.take_written_files()?;

        // Spec §4.1: empty input is a no-op — return the existing snapshot id
        // (or 0 for an empty table) and skip the catalog round-trip.
        if written.is_empty() {
            let id = ctx
                .table
                .metadata()
                .current_snapshot()
                .map(|s| s.snapshot_id())
                .unwrap_or(0);
            return Ok(CommitOutcome {
                new_snapshot_id: id,
                written_manifest_paths: vec![],
            });
        }

        // FastAppendAction::validate_added_data_files rejects any non-Data
        // content — catch the misuse here with a clearer error.
        for f in &written {
            if f.content != DataContentType::Data {
                return Err(format!(
                    "FastAppendCommit received {:?} content; expected Data only",
                    f.content
                ));
            }
        }

        let data_files: Vec<iceberg::spec::DataFile> = written
            .iter()
            .map(|f| written_file_to_iceberg_data_file(f, ctx.collector))
            .collect::<Result<Vec<_>, _>>()?;

        let tx = Transaction::new(ctx.table);
        let action = tx
            .fast_append()
            .add_data_files(data_files)
            .set_commit_uuid(ctx.commit_uuid);
        let tx = action
            .apply(tx)
            .map_err(|e| format!("fast_append apply failed: {e}"))?;
        let table_after = tx
            .commit(ctx.catalog)
            .await
            .map_err(|e| format!("fast_append commit failed: {e}"))?;
        let new_snapshot_id = table_after
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
            .ok_or_else(|| "fast_append committed but new snapshot not visible".to_string())?;
        Ok(CommitOutcome {
            new_snapshot_id,
            // FastAppendAction owns its manifest lifecycle; nothing for us
            // to clean up on later abort.
            written_manifest_paths: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_compiles() {
        let _ = FastAppendCommit;
    }
}
