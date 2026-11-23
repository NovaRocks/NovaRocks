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

//! Frozen removals from the same paid Rewrite manifest observation.

use std::alloc::Layout;
use std::mem::size_of;
use std::num::NonZeroU64;
use std::sync::Arc;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::SnapshotIoGate;
use crate::commit::model::{EntryIdentity, FrozenEntry};
use crate::delete_semantics::{DeleteKind, RawDeleteEntry};
use crate::iceberg::spec::{DataContentType, Literal, Manifest, PrimitiveLiteral};
use crate::manifest::source::ManifestSource;

use super::scoped_capture::ReadFailure;

#[derive(Default)]
pub(crate) struct RewriteEntries {
    entries: Vec<FrozenEntry>,
}

impl RewriteEntries {
    /// Freeze a manifest while the already-observed Avro remains in custody.
    /// The extra pass is over that decoded object and issues no object read.
    pub(super) async fn capture_manifest(
        &mut self,
        source: ManifestSource<'_>,
        manifest: &Manifest,
        deletes: &[(Option<usize>, RawDeleteEntry)],
        gate: Option<&SnapshotIoGate>,
        active: impl Fn() -> Result<(), ReadFailure>,
    ) -> Result<(), ReadFailure> {
        let mut live = 0usize;
        let mut variable = 0usize;
        let mut deletes = deletes.iter();
        for (ordinal, entry) in manifest.entries().iter().enumerate() {
            active()?;
            if !entry.is_alive() {
                continue;
            }
            let file = entry.data_file();
            live = add(live, 1).map_err(scoped_provider)?;
            variable = add(variable, file.file_path().len()).map_err(scoped_provider)?;
            let fields = file.partition().fields();
            variable = add(
                variable,
                array_bytes::<Option<Literal>>(fields.len()).map_err(scoped_provider)?,
            )
            .map_err(scoped_provider)?;
            for literal in fields.iter().flatten() {
                active()?;
                let bytes = match literal {
                    Literal::Primitive(PrimitiveLiteral::String(text)) => text.len(),
                    Literal::Primitive(PrimitiveLiteral::Binary(bytes)) => bytes.len(),
                    Literal::Primitive(_) => 0,
                    _ => {
                        return Err(scoped_provider(corrupt(
                            "Iceberg rewrite partition field is not primitive",
                        )));
                    }
                };
                variable = add(variable, bytes).map_err(scoped_provider)?;
            }
            if file.content_type() != DataContentType::Data {
                let (delete_ordinal, delete) = deletes.next().ok_or_else(|| {
                    scoped_provider(corrupt(
                        "Iceberg rewrite delete is missing from its original manifest observation",
                    ))
                })?;
                if *delete_ordinal != Some(ordinal) {
                    return Err(scoped_provider(corrupt(
                        "Iceberg rewrite delete ordinal differs from its observation",
                    )));
                }
                if let DeleteKind::DeletionVector { exact_target } = &delete.file.kind {
                    variable = add(variable, exact_target.len()).map_err(scoped_provider)?;
                }
            }
        }
        if deletes.next().is_some() {
            return Err(scoped_provider(corrupt(
                "Iceberg rewrite manifest observation has an unexpected delete entry",
            )));
        }
        if live == 0 {
            return Ok(());
        }

        let present = self.entries.len();
        let required = add(present, live).map_err(scoped_provider)?;
        let capacity = self.entries.capacity();
        let new_capacity = if required > capacity {
            required.max(
                capacity
                    .checked_mul(2)
                    .ok_or_else(|| scoped_provider(capacity_failure()))?,
            )
        } else {
            0
        };
        // The selection borrows one identity per chosen entry and moves the
        // chosen values into an Arc slice. Prepay the final spine as well as
        // the inventory's old/new coexistence during geometric growth.
        let selection = array_bytes::<FrozenEntry>(live).map_err(scoped_provider)?;
        let sort_keys = array_bytes::<&str>(live).map_err(scoped_provider)?;
        let bytes = add(
            add(variable, selection).map_err(scoped_provider)?,
            sort_keys,
        )
        .map_err(scoped_provider)?;
        let bytes = add(
            bytes,
            array_bytes::<FrozenEntry>(new_capacity).map_err(scoped_provider)?,
        )
        .map_err(scoped_provider)?;
        let bytes = if present == 0 {
            add(
                bytes,
                size_of::<[usize; 4]>()
                    + size_of::<novarocks_spi::connector::ConnectorPayloadRetentionGuard>(),
            )
            .map_err(scoped_provider)?
        } else {
            bytes
        };
        if let Some(gate) = gate {
            let bytes = u64::try_from(bytes)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or_else(|| scoped_provider(capacity_failure()))?;
            gate.reserve_owned_backing(bytes).await?;
        }
        active()?;
        if new_capacity != 0 {
            self.entries
                .try_reserve_exact(new_capacity - present)
                .map_err(|_| scoped_provider(capacity_failure()))?;
        }
        for entry in source.bind(manifest).map_err(raw_failure)? {
            active()?;
            let entry = entry.map_err(raw_failure)?;
            if entry.raw.is_alive() {
                self.entries
                    .push(FrozenEntry::from_bound_entry(&entry).map_err(raw_failure)?);
            }
        }
        if self.entries.len() != required {
            return Err(scoped_provider(corrupt(
                "Iceberg rewrite manifest changed within its original observation",
            )));
        }
        Ok(())
    }

    /// Retain only the exact entries selected by the rewrite groups.
    pub(crate) fn select(
        mut self,
        mut data_paths: Vec<&str>,
        mut delete_identities: Vec<&EntryIdentity>,
        active: impl Fn() -> Result<(), ConnectorError>,
    ) -> Result<Arc<[FrozenEntry]>, ConnectorError> {
        active()?;
        data_paths.sort_unstable();
        delete_identities.sort_unstable();
        if data_paths.windows(2).any(|pair| pair[0] == pair[1])
            || delete_identities.windows(2).any(|pair| pair[0] == pair[1])
            || delete_identities
                .iter()
                .any(|identity| matches!(identity, EntryIdentity::DataFile { .. }))
        {
            return Err(corrupt(
                "Iceberg rewrite groups repeat or misclassify a removal identity",
            ));
        }
        let mut stop = Ok(());
        self.entries.retain(|entry| {
            if stop.is_ok() {
                stop = active();
            }
            stop.is_ok()
                && match entry.identity() {
                    EntryIdentity::DataFile { path } => {
                        data_paths.binary_search(&path.as_str()).is_ok()
                    }
                    identity => delete_identities.binary_search(&identity).is_ok(),
                }
        });
        stop?;
        self.entries
            .sort_unstable_by(|left, right| left.identity().cmp(right.identity()));
        for pair in self.entries.windows(2) {
            active()?;
            if pair[0].identity() == pair[1].identity() {
                return Err(corrupt(format!(
                    "Iceberg rewrite removal has multiple source entries: {}",
                    pair[0].identity().path()
                )));
            }
        }
        if self.entries.len() != add(data_paths.len(), delete_identities.len())? {
            return Err(corrupt(
                "Iceberg rewrite selection is missing an original manifest entry",
            ));
        }
        active()?;
        Ok(self.entries.into())
    }
}

fn add(left: usize, right: usize) -> Result<usize, ConnectorError> {
    left.checked_add(right).ok_or_else(capacity_failure)
}

fn array_bytes<T>(count: usize) -> Result<usize, ConnectorError> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| capacity_failure())
}

fn capacity_failure() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Iceberg rewrite entry backing is not representable",
    )
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn raw_failure(error: crate::iceberg::Error) -> ReadFailure {
    ReadFailure::Original(
        crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure(error)
            .into(),
    )
}

fn scoped_provider(error: ConnectorError) -> ReadFailure {
    ReadFailure::Original(error.into())
}

#[cfg(test)]
mod tests;
