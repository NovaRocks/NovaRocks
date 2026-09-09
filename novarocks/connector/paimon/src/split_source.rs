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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

use novarocks_spi::connector::read_stack::{
    ConnectorSplit, ConnectorSplitBatch, ConnectorSplitSource, DynamicFilterSnapshot,
    SplitSourceProfile, SplitWeight,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorResourceReservation};
use paimon::DataSplit;
use paimon::spec::TableSchema;

use crate::catalog::map_sdk_error;
use crate::domain::{
    PaimonBinaryTableStats, PaimonColumn, PaimonDataFile, PaimonDataFileFacts, PaimonMergeEngine,
    PaimonSplit,
};
use crate::metadata::{PaimonFrozenRead, validate_schema_evolution};
use crate::resources::PaimonRequestResources;

pub const DEFAULT_PAIMON_TARGET_SPLIT_BYTES: u64 = 128 * 1024 * 1024;
pub const MAX_PAIMON_PLANNED_SPLITS: usize = 1_000_000;
pub const MAX_PAIMON_PLANNED_FILES: usize = 4_000_000;
pub const MAX_PAIMON_SPLIT_METADATA_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct PaimonSplitPlanningLimits {
    pub target_split_bytes: u64,
    pub max_splits: usize,
    pub max_files: usize,
    pub max_retained_metadata_bytes: u64,
}

impl Default for PaimonSplitPlanningLimits {
    fn default() -> Self {
        Self {
            target_split_bytes: DEFAULT_PAIMON_TARGET_SPLIT_BYTES,
            max_splits: MAX_PAIMON_PLANNED_SPLITS,
            max_files: MAX_PAIMON_PLANNED_FILES,
            max_retained_metadata_bytes: MAX_PAIMON_SPLIT_METADATA_BYTES,
        }
    }
}

impl PaimonSplitPlanningLimits {
    pub fn validate(self) -> Result<Self, ConnectorError> {
        if self.target_split_bytes == 0
            || self.max_splits == 0
            || self.max_splits > MAX_PAIMON_PLANNED_SPLITS
            || self.max_files == 0
            || self.max_files > MAX_PAIMON_PLANNED_FILES
            || self.max_retained_metadata_bytes == 0
            || self.max_retained_metadata_bytes > MAX_PAIMON_SPLIT_METADATA_BYTES
        {
            return Err(invalid("Paimon split planning limits are invalid"));
        }
        Ok(self)
    }
}

struct SplitPlanLease(Mutex<ConnectorResourceReservation>);

impl std::fmt::Debug for SplitPlanLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SplitPlanLease")
            .field(
                &self
                    .0
                    .lock()
                    .expect("Paimon split plan lease mutex poisoned")
                    .bytes(),
            )
            .finish()
    }
}

/// One indivisible SDK merge group and its transport domain projection.
/// `sdk_split` is retained one-to-one so FE-local readers and tests never
/// reconstruct or re-partition the merge tree.
#[derive(Clone)]
pub struct PaimonPlannedSplit {
    split: PaimonSplit,
    sdk_split: DataSplit,
    historical_schemas: Arc<BTreeMap<i64, Arc<TableSchema>>>,
    _lease: Arc<SplitPlanLease>,
}

impl PaimonPlannedSplit {
    pub fn split(&self) -> &PaimonSplit {
        &self.split
    }

    pub fn sdk_split(&self) -> &DataSplit {
        &self.sdk_split
    }

    pub fn historical_schema(&self, schema_id: i64) -> Option<&Arc<TableSchema>> {
        self.historical_schemas.get(&schema_id)
    }

    pub fn historical_schemas(&self) -> &BTreeMap<i64, Arc<TableSchema>> {
        &self.historical_schemas
    }
}

impl std::fmt::Debug for PaimonPlannedSplit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonPlannedSplit")
            .field("split", &self.split)
            .field("sdk_split", &self.sdk_split)
            .field("historical_schema_ids", &self.historical_schemas.keys())
            .finish_non_exhaustive()
    }
}

/// Run SDK discovery once against the exact table in `frozen`. The SDK's
/// eager plan is bounded before it becomes a backpressured split source.
pub async fn plan_splits(
    frozen: &PaimonFrozenRead,
    resources: PaimonRequestResources,
    limits: PaimonSplitPlanningLimits,
) -> Result<Vec<PaimonPlannedSplit>, ConnectorError> {
    let limits = limits.validate()?;
    resources.checkpoint()?;
    let Some(snapshot_id) = frozen.view().snapshot_id() else {
        return Ok(Vec::new());
    };
    let plan = frozen
        .sdk_table()
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .map_err(map_sdk_error)?;
    resources.checkpoint()?;
    if plan.splits().len() > limits.max_splits {
        return Err(exhausted("Paimon plan exceeds the split count limit"));
    }

    let mut file_count = 0_usize;
    let mut schema_ids = BTreeSet::new();
    for sdk_split in plan.splits() {
        if sdk_split.snapshot_id() != snapshot_id {
            return Err(corrupt("Paimon SDK planned a split from another snapshot"));
        }
        file_count = file_count
            .checked_add(sdk_split.data_files().len())
            .ok_or_else(|| exhausted("Paimon planned file count overflow"))?;
        if file_count > limits.max_files {
            return Err(exhausted("Paimon plan exceeds the file count limit"));
        }
        for file in sdk_split.data_files() {
            schema_ids.insert(file.schema_id);
        }
    }

    let mut retained = (plan.splits().len() * size_of::<PaimonPlannedSplit>()) as u64;
    for sdk_split in plan.splits() {
        retained = retained
            .checked_add(estimate_sdk_split_bytes(sdk_split))
            .ok_or_else(|| exhausted("Paimon SDK split metadata size overflow"))?;
    }
    if retained > limits.max_retained_metadata_bytes {
        return Err(exhausted("Paimon SDK plan exceeds the metadata byte limit"));
    }
    let mut reservation = resources.reserve_split_planning(retained.max(1))?;

    let mut historical_schemas = BTreeMap::new();
    for schema_id in schema_ids {
        resources.checkpoint()?;
        let schema = frozen
            .sdk_table()
            .schema_manager()
            .schema(schema_id)
            .await
            .map_err(map_sdk_error)?;
        validate_schema_evolution(&schema, frozen.output_schema())?;
        let schema_bytes = estimate_schema_bytes(&schema);
        retained = retained
            .checked_add(schema_bytes)
            .ok_or_else(|| exhausted("Paimon historical schema size overflow"))?;
        if retained > limits.max_retained_metadata_bytes {
            return Err(exhausted(
                "Paimon historical schemas exceed the metadata byte limit",
            ));
        }
        reservation.try_grow(estimate_historical_schema_entry_bytes())?;
        historical_schemas.insert(schema_id, schema);
    }
    let historical_schemas = Arc::new(historical_schemas);

    let mut converted = Vec::with_capacity(plan.splits().len());
    for sdk_split in plan.splits() {
        resources.checkpoint()?;
        let split = convert_split(frozen, sdk_split, limits.target_split_bytes)?;
        retained = retained
            .checked_add(split.retained_size_in_bytes())
            .ok_or_else(|| exhausted("Paimon split metadata size overflow"))?;
        if retained > limits.max_retained_metadata_bytes {
            return Err(exhausted("Paimon split metadata exceeds the byte limit"));
        }
        reservation.try_grow(split.retained_size_in_bytes())?;
        converted.push((split, sdk_split.clone()));
    }
    let lease = Arc::new(SplitPlanLease(Mutex::new(reservation)));
    resources.checkpoint()?;
    Ok(converted
        .into_iter()
        .map(|(split, sdk_split)| PaimonPlannedSplit {
            split,
            sdk_split,
            historical_schemas: Arc::clone(&historical_schemas),
            _lease: Arc::clone(&lease),
        })
        .collect())
}

fn convert_split(
    frozen: &PaimonFrozenRead,
    sdk_split: &DataSplit,
    target_split_bytes: u64,
) -> Result<PaimonSplit, ConnectorError> {
    let files = sdk_split
        .data_files()
        .iter()
        .map(|file| {
            PaimonDataFile::try_new(PaimonDataFileFacts {
                file_name: file.file_name.clone(),
                file_size: u64::try_from(file.file_size)
                    .map_err(|_| corrupt("Paimon file size is negative"))?,
                row_count: u64::try_from(file.row_count)
                    .map_err(|_| corrupt("Paimon file row count is negative"))?,
                min_key: file.min_key.clone(),
                max_key: file.max_key.clone(),
                key_stats: PaimonBinaryTableStats::try_new(
                    file.key_stats.min_values().to_vec(),
                    file.key_stats.max_values().to_vec(),
                    file.key_stats.null_counts().to_vec(),
                )?,
                value_stats: PaimonBinaryTableStats::try_new(
                    file.value_stats.min_values().to_vec(),
                    file.value_stats.max_values().to_vec(),
                    file.value_stats.null_counts().to_vec(),
                )?,
                min_sequence_number: file.min_sequence_number,
                max_sequence_number: file.max_sequence_number,
                schema_id: file.schema_id,
                level: file.level,
                extra_files: file.extra_files.clone(),
                creation_time_millis: file.creation_time.map(|value| value.timestamp_millis()),
                delete_row_count: file
                    .delete_row_count
                    .map(u64::try_from)
                    .transpose()
                    .map_err(|_| corrupt("Paimon delete row count is negative"))?,
                embedded_index: file.embedded_index.clone(),
                file_source: file.file_source,
                value_stats_cols: file.value_stats_cols.clone(),
                external_path: file.external_path.clone(),
                first_row_id: file.first_row_id,
                write_cols: file.write_cols.clone(),
                compression: frozen.options().data_compression,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let total_bytes = files.iter().try_fold(0_u64, |total, file| {
        total
            .checked_add(file.file_size())
            .ok_or_else(|| exhausted("Paimon split byte size overflow"))
    })?;
    if files.is_empty() || total_bytes == 0 {
        return Err(corrupt("Paimon SDK emitted an empty data split"));
    }
    if sdk_split.data_deletion_files().is_some() {
        return Err(unsupported(
            "Paimon deletion-vector reads are unsupported in PAI-1",
        ));
    }
    if sdk_split.row_ranges().is_some() {
        return Err(unsupported(
            "Paimon row-range reads are unsupported in PAI-1",
        ));
    }
    let contains_delete_rows = sdk_split.data_files().iter().any(|file| {
        file.delete_row_count.is_some_and(|count| count > 0)
            || (frozen.table().merge_engine() == PaimonMergeEngine::Deduplicate
                && file.delete_row_count.is_none())
    });
    PaimonSplit::try_new(
        sdk_split.snapshot_id(),
        frozen.view().schema_id(),
        sdk_split.partition().arity(),
        sdk_split.partition().data().to_vec(),
        sdk_split.bucket(),
        sdk_split.bucket_path(),
        sdk_split.total_buckets(),
        files,
        None,
        None,
        sdk_split.raw_convertible(),
        contains_delete_rows,
        SplitWeight::from_proportion(total_bytes as f64 / target_split_bytes as f64)?,
    )
}

/// Synchronous, bounded delivery over an eagerly discovered SDK plan.
pub struct PaimonSplitSource {
    pending: VecDeque<PaimonPlannedSplit>,
    resources: PaimonRequestResources,
    profile: SplitSourceProfile,
    closed: bool,
}

impl PaimonSplitSource {
    pub fn new(
        planned: Vec<PaimonPlannedSplit>,
        resources: PaimonRequestResources,
    ) -> Result<Self, ConnectorError> {
        resources.checkpoint()?;
        let files_considered = planned.iter().try_fold(0_u64, |count, split| {
            count
                .checked_add(split.sdk_split().data_files().len() as u64)
                .ok_or_else(|| exhausted("Paimon split profile file count overflow"))
        })?;
        Ok(Self {
            pending: planned.into(),
            resources,
            profile: SplitSourceProfile {
                files_considered,
                files_expanded: files_considered,
                ..SplitSourceProfile::default()
            },
            closed: false,
        })
    }

    /// Provider-local form used before transport projection. Each returned
    /// item still carries the exact SDK DataSplit and its metadata lease.
    pub fn next_planned_batch(
        &mut self,
        max_size: usize,
    ) -> Result<ConnectorSplitBatch<PaimonPlannedSplit>, ConnectorError> {
        if max_size == 0 {
            return Err(invalid("Paimon split batch size must be positive"));
        }
        self.resources.checkpoint()?;
        if self.closed {
            return Ok(ConnectorSplitBatch::finished());
        }
        let count = max_size.min(self.pending.len());
        let splits = self.pending.drain(..count).collect::<Vec<_>>();
        self.profile.splits_emitted = self
            .profile
            .splits_emitted
            .checked_add(splits.len() as u64)
            .ok_or_else(|| exhausted("Paimon emitted split count overflow"))?;
        Ok(ConnectorSplitBatch::new(splits, self.pending.is_empty()))
    }
}

impl ConnectorSplitSource for PaimonSplitSource {
    type Split = PaimonSplit;
    type Column = PaimonColumn;

    fn profile_snapshot(&self) -> SplitSourceProfile {
        self.profile
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        _dynamic_filter: &DynamicFilterSnapshot<Self::Column>,
    ) -> Result<ConnectorSplitBatch<Self::Split>, ConnectorError> {
        let batch = self.next_planned_batch(max_size)?;
        let no_more = batch.no_more_splits();
        Ok(ConnectorSplitBatch::new(
            batch
                .into_splits()
                .into_iter()
                .map(|planned| planned.split)
                .collect(),
            no_more,
        ))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.pending.is_empty()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.pending.clear();
        self.closed = true;
        Ok(())
    }
}

fn estimate_sdk_split_bytes(split: &DataSplit) -> u64 {
    split.data_files().iter().fold(
        (size_of::<DataSplit>() + split.partition().to_serialized_bytes().len()) as u64,
        |bytes, file| {
            bytes
                .saturating_add(size_of_val(file) as u64)
                .saturating_add(file.file_name.len() as u64)
                .saturating_add(file.external_path.as_ref().map_or(0, String::len) as u64)
                .saturating_add(file.extra_files.iter().map(String::len).sum::<usize>() as u64)
        },
    )
}

fn estimate_schema_bytes(schema: &TableSchema) -> u64 {
    schema
        .fields()
        .iter()
        .fold(size_of_val(schema) as u64, |bytes, field| {
            bytes
                .saturating_add(size_of_val(field) as u64)
                .saturating_add(field.name().len() as u64)
                .saturating_add(field.description().map_or(0, str::len) as u64)
        })
}

fn estimate_historical_schema_entry_bytes() -> u64 {
    // The schema heap is already charged by SchemaManager and its reservation
    // follows every TableSchema clone. This lease owns only the map entry and
    // one additional Arc; two tuple widths conservatively cover BTree links
    // and node metadata amortized across entries.
    u64::try_from(size_of::<(i64, Arc<TableSchema>)>().saturating_mul(2)).unwrap_or(u64::MAX)
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn exhausted(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

fn unsupported(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}
