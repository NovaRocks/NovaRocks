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

//! Read applicable old positions from the fixed mutation endpoint.

use std::collections::BTreeSet;

use novarocks_fs::FileCancellation;
use novarocks_spi::connector::{
    ConnectorError, ConnectorOperationControl, ConnectorRequestContext,
};
use roaring::RoaringTreemap;

use crate::access_binding::IcebergReadBinding;
use crate::commit::model::EntryIdentity;
use crate::commit::write_stack::domain::corrupt;
use crate::delete_file::IcebergDeleteFileSpec;

#[derive(Clone, Debug, Default)]
pub(crate) struct ScopedOldPositionsOutcome {
    positions: RoaringTreemap,
    merged_references: Vec<EntryIdentity>,
}

impl ScopedOldPositionsOutcome {
    pub(crate) fn into_positions(self) -> RoaringTreemap {
        self.positions
    }

    /// Exclusive applicable entries replaced by the staged effect.
    pub(crate) fn merged_references(&self) -> &[EntryIdentity] {
        &self.merged_references
    }
}

/// Complete shared-Parquet observations live in this writer's fixed request.
pub(crate) struct ScopedOldPositions {
    resources: novarocks_spi::connector::ConnectorExecutionResources,
    puffin_lengths: std::collections::BTreeMap<
        (String, u64),
        novarocks_spi::connector::ConnectorResourceReservation,
    >,
    parquet: std::collections::BTreeMap<
        (String, u64),
        (
            crate::position_delete::PositionDeleteIndex,
            novarocks_spi::connector::ConnectorResourceReservation,
        ),
    >,
}

impl ScopedOldPositions {
    pub(crate) fn new(resources: novarocks_spi::connector::ConnectorExecutionResources) -> Self {
        Self {
            resources,
            puffin_lengths: std::collections::BTreeMap::new(),
            parquet: std::collections::BTreeMap::new(),
        }
    }

    pub(crate) fn evict(&mut self) {
        self.puffin_lengths.clear();
        self.parquet.clear();
    }

    pub(crate) async fn read(
        &mut self,
        facts: &crate::manifest::target_query::TargetFacts,
        binding: &IcebergReadBinding,
        request: &ConnectorRequestContext,
    ) -> Result<ScopedOldPositionsOutcome, ConnectorError> {
        use crate::delete_semantics::{DeleteContentAddress, DeleteFormat, DeleteKind};
        use novarocks_spi::connector::{ConnectorErrorKind, ConnectorResourceClass};
        let target = facts.data.frozen.identity().path();
        let row_count = facts.data.frozen.facts().record_count;
        let context = binding.file_read_context(
            FileCancellation::from_connector_request(request),
            request.deadline(),
        )?;
        let mut positions = RoaringTreemap::new();
        let mut exclusive = BTreeSet::new();
        for selected in &facts.positions {
            request.check_active()?;
            let fact = selected.delete();
            let path = fact.address().path();
            let read = fact.read();
            let access = binding
                .resolve_access_for_locations_async([path], &context)
                .await?;
            if !read.key_metadata.is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "encrypted old position-delete object",
                ));
            }
            let is_exclusive = match fact.kind() {
                DeleteKind::Position { exact_target } => exact_target.is_some(),
                DeleteKind::DeletionVector { .. } => true,
                DeleteKind::Equality(_) => {
                    return Err(corrupt("equality delete entered position materialization"));
                }
            };
            let length = read.file_size;
            let stat = || async {
                let file = access
                    .bind_location(path, novarocks_fs::FileIdentity::new(path, 0, None))
                    .map_err(crate::file_reader::map_file_error)?;
                let actual = file
                    .stat(&context.bounded_cancellation())
                    .await
                    .map_err(crate::file_reader::map_file_error)?;
                if actual != length {
                    return Err(corrupt(format!(
                        "Iceberg old delete artifact {path} has size {actual}, expected {length}"
                    )));
                }
                Ok(())
            };
            let decoded = match (fact.address(), read.format) {
                (
                    DeleteContentAddress::PuffinBlob {
                        offset,
                        length: blob_length,
                        ..
                    },
                    DeleteFormat::Puffin,
                ) => {
                    let key = (path.to_owned(), length);
                    if !self.puffin_lengths.contains_key(&key) {
                        stat().await?;
                        let bytes = path.len() as u64
                            + std::mem::size_of::<(
                                String,
                                u64,
                                novarocks_spi::connector::ConnectorResourceReservation,
                            )>() as u64;
                        match self
                            .resources
                            .try_reserve(ConnectorResourceClass::WriterState, bytes)
                        {
                            Ok(receipt) => {
                                self.puffin_lengths.insert(key, receipt);
                            }
                            Err(cause) if cause.kind() == ConnectorErrorKind::ResourceExhausted => {
                            }
                            Err(cause) => return Err(cause),
                        }
                    }
                    let spec = IcebergDeleteFileSpec::puffin_position_delete(
                        path.to_owned(),
                        Some(length),
                        i64::try_from(*offset)
                            .map_err(|_| corrupt("Puffin blob offset exceeds i64"))?,
                        i64::try_from(*blob_length)
                            .map_err(|_| corrupt("Puffin blob length exceeds i64"))?,
                    );
                    let (decoded, _) =
                        crate::position_delete::load_position_deletes_async_with_metrics(
                            &[spec],
                            target,
                            &access,
                            &context,
                            |_| {},
                        )
                        .await?;
                    if decoded.len() != read.record_count {
                        return Err(corrupt(format!(
                            "Iceberg old DV {path} declares {} positions but decoded {}",
                            read.record_count,
                            decoded.len(),
                        )));
                    }
                    decoded
                }
                (DeleteContentAddress::File(_), DeleteFormat::Parquet) => {
                    let key = (path.to_owned(), length);
                    let checked_positions =
                        |index: &crate::position_delete::PositionDeleteIndex| {
                            if index.rows != read.record_count
                                || (is_exclusive && index.by_path.keys().any(|path| path != target))
                            {
                                return Err(corrupt(format!(
                                    "Iceberg old position-delete {path} differs from its declared count or scope"
                                )));
                            }
                            Ok(index.by_path.get(target).cloned().unwrap_or_default())
                        };
                    if let Some((index, _)) = self.parquet.get(&key) {
                        checked_positions(index)?
                    } else {
                        stat().await?;
                        let spec = IcebergDeleteFileSpec::parquet_position_delete(
                            path.to_owned(),
                            Some(length),
                        );
                        let index = crate::position_delete::load_position_delete_index(
                            &spec, &access, &context,
                        )
                        .await?;
                        let decoded = checked_positions(&index)?;
                        let bytes = index.retained_bytes().saturating_add(path.len() as u64);
                        let receipt = match self
                            .resources
                            .try_reserve(ConnectorResourceClass::WriterState, bytes)
                        {
                            Ok(receipt) => Some(receipt),
                            Err(error) if error.kind() == ConnectorErrorKind::ResourceExhausted => {
                                self.evict();
                                match self
                                    .resources
                                    .try_reserve(ConnectorResourceClass::WriterState, bytes)
                                {
                                    Ok(receipt) => Some(receipt),
                                    Err(error)
                                        if error.kind()
                                            == ConnectorErrorKind::ResourceExhausted =>
                                    {
                                        None
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            Err(error) => return Err(error),
                        };
                        if let Some(receipt) = receipt {
                            self.parquet.insert(key, (index, receipt));
                        }
                        decoded
                    }
                }
                _ => return Err(corrupt("old position-delete address and format disagree")),
            };
            if decoded.max().is_some_and(|position| position >= row_count) {
                return Err(corrupt(format!(
                    "Iceberg old position-delete {path} references a position outside {target}"
                )));
            }
            positions |= decoded;
            if is_exclusive && !exclusive.insert(selected.entry.frozen.identity().clone()) {
                return Err(corrupt(format!(
                    "Duplicate exclusive Iceberg old position-delete identity at {path}"
                )));
            }
        }
        request.check_active()?;
        Ok(ScopedOldPositionsOutcome {
            positions,
            merged_references: exclusive.into_iter().collect(),
        })
    }
}
