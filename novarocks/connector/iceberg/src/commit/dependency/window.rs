// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Design: ADR-0244 (docs/adr/ADR-0244-iceberg-row-dml-conflict-validation.md)

//! Shared manifest traversal and entry facts for commit validation windows.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fmt;

use super::ValidationInputs;
use super::history::HistoryWindow;
use crate::commit::conflict::CandidateFile;
use crate::commit::manifest_sources::SnapshotSources;
use crate::commit::model::{ArtifactWriter, CommitOperation, IsolationLevel};
use crate::delete_semantics::POSITION_FILE_PATH_FIELD_ID;
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ManifestContentType, Operation, PrimitiveLiteral,
    TableMetadata,
};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::manifest::binding::BoundEntry;

pub(super) struct WindowTrace {
    attempt: crate::commit::model::AttemptToken,
    stage: Option<usize>,
    start: Option<i64>,
    parent: Option<i64>,
    operation: CommitOperation,
    isolation: Option<IsolationLevel>,
    pub(super) window_len: Option<usize>,
    pub(super) checks: Vec<&'static str>,
    failed_check: Option<&'static str>,
    object: Option<String>,
    pub(super) complete: bool,
}

impl WindowTrace {
    pub(super) fn new(
        operation: CommitOperation,
        isolation: Option<IsolationLevel>,
        inputs: &ValidationInputs<'_>,
        start: Option<i64>,
        io: &dyn ArtifactWriter,
        checks: Vec<&'static str>,
    ) -> Self {
        Self {
            attempt: io.attempt_token(),
            stage: inputs.stage,
            start,
            parent: inputs.parent,
            operation,
            isolation,
            window_len: None,
            checks,
            failed_check: None,
            object: None,
            complete: false,
        }
    }

    pub(super) fn reject(&mut self, check: &'static str, object: Option<&str>) {
        self.failed_check = Some(check);
        self.object = object.map(ToOwned::to_owned);
        self.complete = true;
    }
}

impl Drop for WindowTrace {
    fn drop(&mut self) {
        tracing::debug!(
            operation = ?self.attempt.operation(),
            operation_label = ?self.operation,
            commit_style = ?self.operation.style(),
            attempt = self.attempt.ordinal(),
            stage = ?self.stage,
            source_snapshot = ?self.start,
            parent_snapshot = ?self.parent,
            isolation = ?self.isolation,
            window_len = ?self.window_len,
            checks = ?self.checks,
            failed_check = self.failed_check,
            object = self.object.as_deref(),
            complete = self.complete,
            "Iceberg commit window validation",
        );
    }
}

pub(super) struct WindowFiles<'a> {
    pub(super) metadata: &'a TableMetadata,
    pub(super) io: &'a dyn ArtifactWriter,
    pub(super) window: &'a HistoryWindow,
}

impl WindowFiles<'_> {
    pub(super) async fn find(
        &self,
        operations: &[Operation],
        content: ManifestContentType,
        must_have_additions: bool,
        mut find: impl FnMut(&BoundEntry<'_>) -> Result<Option<String>>,
    ) -> Result<Option<String>> {
        for snapshot in self.window.filtered(operations) {
            let sources = SnapshotSources::load(self.metadata, snapshot, self.io.file_io(), || {
                self.io.check_active()
            })
            .await?;
            let result = sources
                .fold(
                    self.io.file_io(),
                    || self.io.check_active(),
                    |descriptor| {
                        descriptor.content == content
                            && descriptor.added_snapshot_id == snapshot.snapshot_id()
                            && (!must_have_additions || descriptor.has_added_files())
                    },
                    |entry| match find(&entry)? {
                        Some(detail) => Err(Error::new(
                            ErrorKind::Unexpected,
                            "Commit window entry matched",
                        )
                        .with_source(MatchedEntry(detail))),
                        None => Ok(()),
                    },
                )
                .await;
            if let Err(error) = result {
                use std::error::Error as _;
                if let Some(matched) = error
                    .source()
                    .and_then(|cause| cause.downcast_ref::<MatchedEntry>())
                {
                    return Ok(Some(matched.0.clone()));
                }
                return Err(error);
            }
        }
        Ok(None)
    }
}

// SnapshotSources::fold stops at the first match without reading another
// manifest. Original reader, predicate and control errors travel unchanged.
#[derive(Debug)]
struct MatchedEntry(String);

impl fmt::Display for MatchedEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MatchedEntry {}

pub(super) fn snapshot_ids(window: &HistoryWindow, operations: &[Operation]) -> BTreeSet<i64> {
    window
        .filtered(operations)
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

pub(super) fn has_owner(entry: &BoundEntry<'_>, owners: &BTreeSet<i64>) -> bool {
    entry
        .sequences
        .snapshot_id
        .is_some_and(|id| owners.contains(&id))
}

pub(super) fn delete_sequence(entry: &BoundEntry<'_>) -> Result<i64> {
    entry
        .sequences
        .data_sequence
        .filter(|sequence| *sequence >= 0)
        .ok_or_else(|| {
            invalid(format!(
                "Delete file {} has no resolved data sequence",
                entry.raw.data_file().file_path()
            ))
        })
}

pub(super) fn data_candidate<'a>(
    metadata: &'a TableMetadata,
    entry: &BoundEntry<'a>,
) -> Result<CandidateFile<'a>> {
    let file = entry.raw.data_file();
    if file.content_type() != DataContentType::Data {
        return Err(invalid("A data manifest contains a delete entry"));
    }
    Ok(CandidateFile {
        metadata,
        spec_id: entry.source.descriptor.partition_spec_id,
        file,
    })
}

pub(super) fn delete_candidate<'a>(
    metadata: &'a TableMetadata,
    entry: &BoundEntry<'a>,
) -> Result<CandidateFile<'a>> {
    let file = entry.raw.data_file();
    if !matches!(
        file.content_type(),
        DataContentType::EqualityDeletes | DataContentType::PositionDeletes
    ) {
        return Err(invalid("A delete manifest contains a data entry"));
    }
    Ok(CandidateFile {
        metadata,
        spec_id: entry.source.descriptor.partition_spec_id,
        file,
    })
}

pub(super) fn position_target(file: &DataFile) -> Result<Option<Cow<'_, str>>> {
    if file.content_type() == DataContentType::EqualityDeletes {
        return Ok(None);
    }
    if let Some(path) = file.referenced_data_file() {
        return Ok(Some(Cow::Owned(path)));
    }
    if file.file_format() == DataFileFormat::Puffin {
        return Err(invalid(
            "Concurrent deletion vector is missing its referenced data file",
        ));
    }
    let lower = file.lower_bounds().get(&POSITION_FILE_PATH_FIELD_ID);
    let upper = file.upper_bounds().get(&POSITION_FILE_PATH_FIELD_ID);
    match (
        lower.map(|bound| bound.literal()),
        upper.map(|bound| bound.literal()),
    ) {
        (Some(PrimitiveLiteral::String(lower)), Some(PrimitiveLiteral::String(upper)))
            if lower == upper =>
        {
            Ok(Some(Cow::Borrowed(lower.as_str())))
        }
        _ => Ok(None),
    }
}

// The delete index checks equality field ranges after partition and sequence.
pub(super) fn equality_can_match(
    delete: &DataFile,
    data: &DataFile,
    metadata: &TableMetadata,
) -> bool {
    let Some(ids) = delete.equality_ids() else {
        return true;
    };
    ids.iter().all(|id| {
        let field = metadata.current_schema().field_by_id(*id).or_else(|| {
            // A delete can still carry metrics for a field that was dropped.
            metadata
                .schemas_iter()
                .filter_map(|schema| Some((schema.schema_id(), schema.field_by_id(*id)?)))
                .max_by_key(|(schema_id, _)| *schema_id)
                .map(|(_, field)| field)
        });
        let Some(field) = field else {
            return true;
        };
        if !field.field_type.is_primitive() {
            return true;
        }
        let data_nulls = data.null_value_counts().get(id).copied();
        let delete_nulls = delete.null_value_counts().get(id).copied();
        let data_values = data.value_counts().get(id).copied();
        let delete_values = delete.value_counts().get(id).copied();

        if !field.required {
            // Two nullable files might have a matching null, even when their
            // ordinary values' bounds are disjoint.
            if data_nulls != Some(0) && delete_nulls != Some(0) {
                return true;
            }
            if (data_nulls.is_some() && data_nulls == data_values && delete_nulls == Some(0))
                || (delete_nulls.is_some()
                    && delete_nulls == delete_values
                    && data_nulls == Some(0))
            {
                return false;
            }
        }
        let (Some(data_lower), Some(data_upper), Some(delete_lower), Some(delete_upper)) = (
            data.lower_bounds().get(id),
            data.upper_bounds().get(id),
            delete.lower_bounds().get(id),
            delete.upper_bounds().get(id),
        ) else {
            return true;
        };
        !matches!(
            data_lower.partial_cmp(delete_upper),
            Some(std::cmp::Ordering::Greater)
        ) && !matches!(
            delete_lower.partial_cmp(data_upper),
            Some(std::cmp::Ordering::Greater)
        )
    })
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
