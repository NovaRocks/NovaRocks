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

//! Logical cardinality from the exact artifacts a managed publication prepared.
//! Physical snapshot totals and writer input counts cannot establish this fact.

use super::*;
use novarocks_spi::connector::write_stack::ConnectorWriteRowChange;

pub(super) fn project(
    handle: &IcebergWriteCommitSession,
    validated: &[ValidatedFragment<'_>],
) -> Result<Option<ConnectorWriteRowChange>, ConnectorError> {
    if handle.document_publication().is_none() {
        return Ok(None);
    }
    let op = handle.commit_op_kind();
    if !matches!(
        op,
        CommitOpKind::FastAppend | CommitOpKind::Overwrite | CommitOpKind::RowDeltaDvFromFiles
    ) {
        return Ok(None);
    }
    let mut inserted = 0_u64;
    let mut deleted = 0_u64;
    let mut exact = true;
    for entry in validated {
        match entry.fragment.artifact() {
            IcebergCommitArtifact::DataFile(file) => {
                inserted = inserted
                    .checked_add(file.metrics().record_count())
                    .ok_or_else(|| {
                        corrupt("Iceberg prepared logical insertion count overflowed")
                    })?;
            }
            IcebergCommitArtifact::DeletionVector(vector)
                if op == CommitOpKind::RowDeltaDvFromFiles =>
            {
                let frozen = handle
                    .frozen_delete_merge_targets()
                    .and_then(|targets| targets.get(&entry.ordinal))
                    .and_then(|files| files.get(vector.referenced_data_file()));
                let Some(frozen) = frozen else {
                    exact = false;
                    continue;
                };
                if vector.cardinality() > frozen.data_file_record_count() {
                    return Err(corrupt(
                        "Iceberg prepared deletion vector exceeds its frozen data-file row count",
                    ));
                }
                let references = frozen.references();
                // A v3 data file can have one live DV. Different blobs in a
                // shared Puffin object still have independent entry identities.
                let mut old_vector = None;
                for reference in references.iter().filter(|reference| {
                    reference.file_format() == crate::delete_file::IcebergFileFormat::Puffin
                }) {
                    if old_vector.replace(reference).is_some() {
                        return Err(corrupt(
                            "Iceberg frozen data file has more than one deletion vector",
                        ));
                    }
                    if let Some(count) = reference.record_count() {
                        if count > frozen.data_file_record_count() {
                            return Err(corrupt(
                                "Iceberg frozen deletion vector exceeds its data-file row count",
                            ));
                        }
                        if count > vector.cardinality() {
                            return Err(corrupt(
                                "Iceberg prepared deletion vector lost frozen deleted rows",
                            ));
                        }
                    }
                }
                let previous = match references {
                    [] => Some(0),
                    [reference]
                        if reference.file_format()
                            == crate::delete_file::IcebergFileFormat::Puffin
                            && reference.referenced_data_file()
                                == Some(vector.referenced_data_file()) =>
                    {
                        reference.record_count()
                    }
                    // Parquet delete rows may repeat positions or be shared
                    // across data files; their sum is not unique cardinality.
                    _ => None,
                };
                let Some(previous) = previous else {
                    exact = false;
                    continue;
                };
                let newly_deleted =
                    vector.cardinality().checked_sub(previous).ok_or_else(|| {
                        corrupt("Iceberg prepared deletion vector lost frozen deleted rows")
                    })?;
                deleted = deleted
                    .checked_add(newly_deleted)
                    .ok_or_else(|| corrupt("Iceberg prepared logical deletion count overflowed"))?;
            }
            _ => {
                return Err(invalid(
                    "Iceberg prepared artifact contradicts its logical publication operation",
                ));
            }
        }
    }
    if !exact {
        return Ok(None);
    }
    Ok(Some(if op == CommitOpKind::Overwrite {
        ConnectorWriteRowChange::Replace(inserted)
    } else {
        ConnectorWriteRowChange::Delta { inserted, deleted }
    }))
}
