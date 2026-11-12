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
        CommitOpKind::FastAppend | CommitOpKind::Overwrite | CommitOpKind::MaterializedRowDelta
    ) {
        return Ok(None);
    }
    let mut inserted = 0_u64;
    let mut deleted = 0_u64;
    for entry in validated {
        match entry.fragment.artifact() {
            IcebergCommitArtifact::DataFile(file) => {
                inserted = inserted
                    .checked_add(file.metrics().record_count())
                    .ok_or_else(|| {
                        corrupt("Iceberg prepared logical insertion count overflowed")
                    })?;
            }
            IcebergCommitArtifact::FinalDelete(effect)
                if op == CommitOpKind::MaterializedRowDelta =>
            {
                deleted = deleted
                    .checked_add(effect.newly_deleted_rows())
                    .ok_or_else(|| corrupt("Iceberg prepared logical deletion count overflowed"))?;
            }
            _ => {
                return Err(invalid(
                    "Iceberg prepared artifact contradicts its logical publication operation",
                ));
            }
        }
    }
    Ok(Some(if op == CommitOpKind::Overwrite {
        ConnectorWriteRowChange::Replace(inserted)
    } else {
        ConnectorWriteRowChange::Delta { inserted, deleted }
    }))
}
