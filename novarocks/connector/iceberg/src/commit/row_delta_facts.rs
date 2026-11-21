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

use std::alloc::Layout;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use super::conflict::RowDeltaValidationFacts;
use super::model::EntryIdentity;
use super::write_stack::receiving::ReceivedCompactFiles;
use super::write_stack::receiving::compact::CompactDeleteKind;

pub(crate) struct RetainedRowDeltaFacts {
    pub(crate) facts: Arc<RowDeltaValidationFacts>,
    // The receipt outlives the copied paths and identities.
    _receipt: Option<ConnectorPayloadRetentionGuard>,
}

pub(crate) fn extract(
    files: &ReceivedCompactFiles,
) -> Result<RetainedRowDeltaFacts, ConnectorError> {
    let (bytes, references) = retained_bytes(files)?;
    let receipt = if bytes == 0 {
        None
    } else {
        Some(
            files
                .author
                .as_ref()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "Iceberg validation facts have no original byte author",
                    )
                })?
                .reserve_owned_bytes(bytes)?,
        )
    };
    let mut facts = RowDeltaValidationFacts {
        replaced: Vec::with_capacity(references),
        ..RowDeltaValidationFacts::default()
    };
    facts
        .referenced
        .extend(files.v2_targets.keys().map(|path| Arc::from(path.as_str())));
    for entry in files.entries() {
        let Some(effect) = &entry.delete_effect else {
            continue;
        };
        let path: Arc<str> = Arc::from(effect.target.data_file_path());
        match &effect.kind {
            CompactDeleteKind::RemoveDataFile {
                old_exclusive_deletes,
                ..
            } => {
                facts.removed.insert(path);
                facts.replaced.extend(old_exclusive_deletes.iter().cloned());
            }
            CompactDeleteKind::ReplacePositions {
                merged_old_references,
                ..
            } => {
                facts.referenced.insert(Arc::clone(&path));
                facts.dv_targets.insert(path);
                facts.replaced.extend(merged_old_references.iter().cloned());
            }
        }
    }
    facts.replaced.sort_unstable();
    facts.replaced.dedup();
    Ok(RetainedRowDeltaFacts {
        facts: Arc::new(facts),
        _receipt: receipt,
    })
}

fn retained_bytes(files: &ReceivedCompactFiles) -> Result<(usize, usize), ConnectorError> {
    let mut path_bytes = 0usize;
    let mut referenced = files.v2_targets.len();
    let mut removed = 0usize;
    let mut dv_targets = 0usize;
    let mut reference_bytes = 0usize;
    let mut references = 0usize;
    for path in files.v2_targets.keys() {
        add(&mut path_bytes, path_backing(path)?)?;
    }
    for entry in files.entries() {
        let Some(effect) = &entry.delete_effect else {
            continue;
        };
        add(
            &mut path_bytes,
            path_backing(effect.target.data_file_path())?,
        )?;
        let old = match &effect.kind {
            CompactDeleteKind::RemoveDataFile {
                old_exclusive_deletes,
                ..
            } => {
                add(&mut removed, 1)?;
                old_exclusive_deletes.as_ref()
            }
            CompactDeleteKind::ReplacePositions {
                merged_old_references,
                ..
            } => {
                add(&mut referenced, 1)?;
                add(&mut dv_targets, 1)?;
                merged_old_references.as_slice()
            }
        };
        add(&mut references, old.len())?;
        for identity in old {
            add(&mut reference_bytes, identity.path().len())?;
            if let EntryIdentity::DeletionVector {
                referenced_data_file,
                ..
            } = identity
            {
                add(&mut reference_bytes, referenced_data_file.len())?;
            }
        }
    }
    // Allow the worst-case unfilled root and spare slots/links in each
    // nonempty index. Both sets for a new DV share its actual string.
    let mut bytes = path_bytes;
    for slots in [referenced, removed, dv_targets] {
        if slots != 0 {
            let storage = slots
                .checked_add(11)
                .and_then(|slots| slots.checked_mul(3 * size_of::<(Arc<str>, usize)>()))
                .ok_or_else(overflow)?;
            add(&mut bytes, storage)?;
        }
    }
    add(&mut bytes, reference_bytes)?;
    add(
        &mut bytes,
        references
            .checked_mul(size_of::<EntryIdentity>())
            .ok_or_else(overflow)?,
    )?;
    if bytes != 0 {
        add(
            &mut bytes,
            size_of::<(RowDeltaValidationFacts, [AtomicUsize; 2])>(),
        )?;
    }
    Ok((bytes, references))
}

fn path_backing(path: &str) -> Result<usize, ConnectorError> {
    Layout::array::<u8>(path.len())
        .and_then(|bytes| Layout::new::<[AtomicUsize; 2]>().extend(bytes))
        .map(|(layout, _)| layout.pad_to_align().size())
        .map_err(|_| overflow())
}

fn add(total: &mut usize, bytes: usize) -> Result<(), ConnectorError> {
    *total = total.checked_add(bytes).ok_or_else(overflow)?;
    Ok(())
}

fn overflow() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg row-delta validation backing size overflow",
    )
}

#[cfg(test)]
mod tests;
