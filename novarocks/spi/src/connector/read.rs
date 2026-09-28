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

use std::cmp::Ordering;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorPredicateDisposition, ConnectorProviderBindingKey,
    ConnectorReadSessionLease, ConnectorRequestContext, ConnectorScanHandle, ConnectorSplit,
    ConnectorStaticPredicate,
};

const MAX_CONNECTOR_CHANGE_PARTITIONS: usize = 16_384;
const MAX_CONNECTOR_CHANGE_PARTITION_FIELDS: usize = 256;
const MAX_CONNECTOR_CHANGE_PARTITION_TOTAL_FIELDS: usize = 65_536;
const MAX_CONNECTOR_CHANGE_ID_BYTES: usize = 1024;
const CHANGE_ADMISSION_BYTES: usize = 16;
const CHANGE_PARTITION_IMPACT_BYTES: usize = 16;
const CHANGE_PARTITION_BYTES: usize = 16;
const CHANGE_PARTITION_FIELD_BYTES: usize = 24;

#[derive(Clone, Debug)]
pub struct ConnectorScan {
    owner: ConnectorProviderBindingKey,
    selection: ConnectorScanSelection,
    admission: ConnectorScanAdmission,
    handle: ConnectorScanHandle,
    output_schema: SchemaRef,
    predicate_dispositions: Vec<ConnectorPredicateDisposition>,
    selection_digest: [u8; 32],
    handle_digest: [u8; 32],
    admission_digest: [u8; 32],
    seal_digest: [u8; 32],
}

impl ConnectorScan {
    pub fn try_new_snapshot(
        owner: ConnectorProviderBindingKey,
        selector: ConnectorReadSelector,
        handle: ConnectorScanHandle,
        output_schema: SchemaRef,
        predicate_dispositions: Vec<ConnectorPredicateDisposition>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            owner,
            ConnectorScanSelection::Snapshot(selector),
            ConnectorScanAdmission::Snapshot,
            handle,
            output_schema,
            predicate_dispositions,
        )
    }

    pub fn try_new_change_window(
        owner: ConnectorProviderBindingKey,
        window: ConnectorChangeWindow,
        admission: ConnectorChangeWindowAdmission,
        handle: ConnectorScanHandle,
        output_schema: SchemaRef,
        predicate_dispositions: Vec<ConnectorPredicateDisposition>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_change_window_admission(&admission, context)?;
        Self::try_new(
            owner,
            ConnectorScanSelection::ChangeWindow(window),
            ConnectorScanAdmission::ChangeWindow(admission),
            handle,
            output_schema,
            predicate_dispositions,
        )
    }

    fn try_new(
        owner: ConnectorProviderBindingKey,
        selection: ConnectorScanSelection,
        admission: ConnectorScanAdmission,
        handle: ConnectorScanHandle,
        output_schema: SchemaRef,
        predicate_dispositions: Vec<ConnectorPredicateDisposition>,
    ) -> Result<Self, ConnectorError> {
        if handle.owner() != &owner.instance_id {
            return Err(invalid(
                "connector scan handle owner does not match the exact control generation",
            ));
        }
        if !matches!(
            (&selection, &admission),
            (
                ConnectorScanSelection::Snapshot(_),
                ConnectorScanAdmission::Snapshot
            ) | (
                ConnectorScanSelection::ChangeWindow(_),
                ConnectorScanAdmission::ChangeWindow(_)
            )
        ) {
            return Err(invalid(
                "connector scan selection and admission tags do not match",
            ));
        }
        let selection_digest = connector_scan_selection_digest(selection);
        let handle_digest = Sha256::digest(handle.payload()).into();
        let admission_digest = connector_scan_admission_digest(&admission);
        let seal_digest =
            connector_scan_seal_digest(&owner, selection_digest, handle_digest, admission_digest);
        Ok(Self {
            owner,
            selection,
            admission,
            handle,
            output_schema,
            predicate_dispositions,
            selection_digest,
            handle_digest,
            admission_digest,
            seal_digest,
        })
    }

    pub fn validate(
        &self,
        expected_owner: &ConnectorProviderBindingKey,
        expected_selection: ConnectorScanSelection,
    ) -> Result<(), ConnectorError> {
        if &self.owner != expected_owner {
            return Err(invalid(
                "connector scan does not belong to the expected exact generation",
            ));
        }
        if self.selection != expected_selection {
            return Err(invalid(
                "connector scan selection does not match the expected read",
            ));
        }
        if self.handle.owner() != &self.owner.instance_id {
            return Err(corrupt("connector scan handle owner changed after sealing"));
        }
        let selection_digest = connector_scan_selection_digest(self.selection);
        let handle_digest: [u8; 32] = Sha256::digest(self.handle.payload()).into();
        let admission_digest = connector_scan_admission_digest(&self.admission);
        let seal_digest = connector_scan_seal_digest(
            &self.owner,
            selection_digest,
            handle_digest,
            admission_digest,
        );
        if selection_digest != self.selection_digest
            || handle_digest != self.handle_digest
            || admission_digest != self.admission_digest
            || seal_digest != self.seal_digest
        {
            return Err(corrupt("connector scan seal does not match its contents"));
        }
        Ok(())
    }

    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }

    pub const fn selection(&self) -> ConnectorScanSelection {
        self.selection
    }

    pub fn admission(&self) -> &ConnectorScanAdmission {
        &self.admission
    }

    pub fn handle(&self) -> &ConnectorScanHandle {
        &self.handle
    }

    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    pub fn predicate_dispositions(&self) -> &[ConnectorPredicateDisposition] {
        &self.predicate_dispositions
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSelector {
    Current,
    SnapshotId(i64),
    TimestampMicros(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorScanSelection {
    Snapshot(ConnectorReadSelector),
    ChangeWindow(ConnectorChangeWindow),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorChangeWindow {
    from_exclusive: i64,
    to_inclusive: i64,
}

impl ConnectorChangeWindow {
    pub const fn new(from_exclusive: i64, to_inclusive: i64) -> Self {
        Self {
            from_exclusive,
            to_inclusive,
        }
    }

    pub const fn from_exclusive(self) -> i64 {
        self.from_exclusive
    }

    pub const fn to_inclusive(self) -> i64 {
        self.to_inclusive
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorScanAdmission {
    Snapshot,
    ChangeWindow(ConnectorChangeWindowAdmission),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowAdmission {
    MetadataOnly,
    Incremental {
        has_inserts: bool,
        has_deletes: bool,
        partition_impact: ConnectorChangeWindowPartitionImpact,
    },
    FullRebuild(ConnectorChangeWindowFullRebuildReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowFullRebuildReason {
    LineageBroken {
        from_snapshot_id: i64,
    },
    UnprovenReplace {
        snapshot_id: i64,
        failure: ConnectorChangeWindowReplaceFailure,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowReplaceFailure {
    MissingParent,
    RecordCountChanged,
    MissingOrInvalidSummary,
    InvalidDataFileCounts,
    SchemaChanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowPartitionImpact {
    Unavailable,
    Unpartitioned,
    Exact {
        has_row_deletes: bool,
        added: Vec<ConnectorChangePartition>,
        removed: Vec<ConnectorChangePartition>,
    },
}

impl ConnectorChangeWindowPartitionImpact {
    pub fn try_exact(
        has_row_deletes: bool,
        mut added: Vec<ConnectorChangePartition>,
        mut removed: Vec<ConnectorChangePartition>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        canonicalize_partitions(&mut added);
        canonicalize_partitions(&mut removed);
        let impact = Self::Exact {
            has_row_deletes,
            added,
            removed,
        };
        validate_partition_impact(&impact, context)?;
        Ok(impact)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorChangePartition {
    partition_spec_identity: Bytes,
    fields: Vec<ConnectorChangePartitionField>,
}

impl ConnectorChangePartition {
    pub fn try_new(
        partition_spec_identity: Bytes,
        mut fields: Vec<ConnectorChangePartitionField>,
    ) -> Result<Self, ConnectorError> {
        if partition_spec_identity.is_empty()
            || partition_spec_identity.len() > MAX_CONNECTOR_CHANGE_ID_BYTES
        {
            return Err(invalid(
                "connector change partition has an invalid partition spec identity",
            ));
        }
        if fields.is_empty() || fields.len() > MAX_CONNECTOR_CHANGE_PARTITION_FIELDS {
            return Err(invalid(
                "connector change partition has an invalid field count",
            ));
        }
        fields.sort_by(canonical_field_key_cmp);
        for pair in fields.windows(2) {
            if canonical_field_key_cmp(&pair[0], &pair[1]) == Ordering::Equal {
                return Err(invalid(
                    "connector change partition contains a duplicate source identity and transform",
                ));
            }
        }
        Ok(Self {
            partition_spec_identity,
            fields,
        })
    }

    pub fn partition_spec_identity(&self) -> &Bytes {
        &self.partition_spec_identity
    }

    pub fn fields(&self) -> &[ConnectorChangePartitionField] {
        &self.fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorChangePartitionField {
    source_field_identity: Bytes,
    source_column: Arc<str>,
    transform: ConnectorChangePartitionTransform,
    value: ConnectorChangePartitionValue,
}

impl ConnectorChangePartitionField {
    pub fn try_new(
        source_field_identity: Bytes,
        source_column: impl Into<Arc<str>>,
        transform: ConnectorChangePartitionTransform,
        value: ConnectorChangePartitionValue,
    ) -> Result<Self, ConnectorError> {
        let source_column = source_column.into();
        if source_field_identity.is_empty()
            || source_field_identity.len() > MAX_CONNECTOR_CHANGE_ID_BYTES
        {
            return Err(invalid(
                "connector change partition source field identity is invalid",
            ));
        }
        if source_column.trim().is_empty() {
            return Err(invalid(
                "connector change partition source column must not be empty",
            ));
        }
        Ok(Self {
            source_field_identity,
            source_column,
            transform,
            value,
        })
    }

    pub fn source_field_identity(&self) -> &Bytes {
        &self.source_field_identity
    }

    pub fn source_column(&self) -> &str {
        &self.source_column
    }

    pub const fn transform(&self) -> ConnectorChangePartitionTransform {
        self.transform
    }

    pub fn value(&self) -> &ConnectorChangePartitionValue {
        &self.value
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ConnectorChangePartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { buckets: NonZeroU32 },
    Truncate { width: NonZeroU32 },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ConnectorChangePartitionValue {
    Null,
    String(Arc<str>),
}

fn canonicalize_partitions(partitions: &mut Vec<ConnectorChangePartition>) {
    partitions.sort_by(canonical_partition_cmp);
    partitions.dedup_by(|right, left| semantic_partition_cmp(left, right) == Ordering::Equal);
}

fn validate_change_window_admission(
    admission: &ConnectorChangeWindowAdmission,
    context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    let bytes = match admission {
        ConnectorChangeWindowAdmission::MetadataOnly => CHANGE_ADMISSION_BYTES,
        ConnectorChangeWindowAdmission::Incremental {
            has_inserts,
            has_deletes,
            partition_impact,
        } => {
            if !has_inserts && !has_deletes {
                return Err(invalid(
                    "connector incremental change-window admission has no row changes",
                ));
            }
            CHANGE_ADMISSION_BYTES
                .saturating_add(validate_partition_impact(partition_impact, context)?)
        }
        ConnectorChangeWindowAdmission::FullRebuild(_) => {
            CHANGE_ADMISSION_BYTES.saturating_add(2 * std::mem::size_of::<i64>())
        }
    };
    validate_admission_bytes(bytes, context)
}

fn validate_partition_impact(
    impact: &ConnectorChangeWindowPartitionImpact,
    context: &ConnectorRequestContext,
) -> Result<usize, ConnectorError> {
    let ConnectorChangeWindowPartitionImpact::Exact { added, removed, .. } = impact else {
        validate_admission_bytes(CHANGE_PARTITION_IMPACT_BYTES, context)?;
        return Ok(CHANGE_PARTITION_IMPACT_BYTES);
    };

    for partition in added.iter().chain(removed) {
        validate_partition(partition)?;
    }
    validate_canonical_partitions(added, "added")?;
    validate_canonical_partitions(removed, "removed")?;

    if unique_partition_count(added, removed) > MAX_CONNECTOR_CHANGE_PARTITIONS {
        return Err(resource_exhausted(
            "connector change-window partition impact exceeds the unique partition limit",
        ));
    }
    let total_fields = added
        .iter()
        .chain(removed)
        .fold(0_usize, |total, partition| {
            total.saturating_add(partition.fields.len())
        });
    if total_fields > MAX_CONNECTOR_CHANGE_PARTITION_TOTAL_FIELDS {
        return Err(resource_exhausted(
            "connector change-window partition impact exceeds the total field limit",
        ));
    }

    let bytes =
        added
            .iter()
            .chain(removed)
            .fold(CHANGE_PARTITION_IMPACT_BYTES, |total, partition| {
                partition.fields.iter().fold(
                    total
                        .saturating_add(CHANGE_PARTITION_BYTES)
                        .saturating_add(partition.partition_spec_identity.len()),
                    |total, field| {
                        total
                            .saturating_add(CHANGE_PARTITION_FIELD_BYTES)
                            .saturating_add(field.source_field_identity.len())
                            .saturating_add(field.source_column.len())
                            .saturating_add(match &field.value {
                                ConnectorChangePartitionValue::Null => 0,
                                ConnectorChangePartitionValue::String(value) => value.len(),
                            })
                    },
                )
            });
    validate_admission_bytes(bytes, context)?;
    Ok(bytes)
}

fn validate_partition(partition: &ConnectorChangePartition) -> Result<(), ConnectorError> {
    if partition.partition_spec_identity.is_empty()
        || partition.partition_spec_identity.len() > MAX_CONNECTOR_CHANGE_ID_BYTES
    {
        return Err(corrupt(
            "connector change-window partition has an invalid partition spec identity",
        ));
    }
    if partition.fields.is_empty() || partition.fields.len() > MAX_CONNECTOR_CHANGE_PARTITION_FIELDS
    {
        return Err(corrupt(
            "connector change-window partition has an invalid field count",
        ));
    }
    if partition.fields.iter().any(|field| {
        field.source_field_identity.is_empty()
            || field.source_field_identity.len() > MAX_CONNECTOR_CHANGE_ID_BYTES
            || field.source_column.trim().is_empty()
    }) {
        return Err(corrupt(
            "connector change-window partition has an invalid source field",
        ));
    }
    if partition
        .fields
        .windows(2)
        .any(|pair| canonical_field_key_cmp(&pair[0], &pair[1]) != Ordering::Less)
    {
        return Err(corrupt(
            "connector change-window partition fields are not canonical",
        ));
    }
    Ok(())
}

fn validate_canonical_partitions(
    partitions: &[ConnectorChangePartition],
    side: &str,
) -> Result<(), ConnectorError> {
    if partitions
        .windows(2)
        .any(|pair| semantic_partition_cmp(&pair[0], &pair[1]) != Ordering::Less)
    {
        return Err(corrupt(format!(
            "connector change-window {side} partitions are not canonical"
        )));
    }
    Ok(())
}

fn validate_admission_bytes(
    bytes: usize,
    context: &ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if bytes > context.max_total_payload_bytes() {
        return Err(resource_exhausted(
            "connector change-window admission exceeds the request total payload budget",
        ));
    }
    Ok(())
}

fn unique_partition_count(
    added: &[ConnectorChangePartition],
    removed: &[ConnectorChangePartition],
) -> usize {
    let (mut added_index, mut removed_index, mut count) = (0_usize, 0_usize, 0_usize);
    while added_index < added.len() && removed_index < removed.len() {
        count = count.saturating_add(1);
        match semantic_partition_cmp(&added[added_index], &removed[removed_index]) {
            Ordering::Less => added_index += 1,
            Ordering::Greater => removed_index += 1,
            Ordering::Equal => {
                added_index += 1;
                removed_index += 1;
            }
        }
    }
    count
        .saturating_add(added.len().saturating_sub(added_index))
        .saturating_add(removed.len().saturating_sub(removed_index))
}

fn canonical_field_key_cmp(
    left: &ConnectorChangePartitionField,
    right: &ConnectorChangePartitionField,
) -> Ordering {
    left.source_field_identity
        .cmp(&right.source_field_identity)
        .then_with(|| left.transform.cmp(&right.transform))
}

fn canonical_partition_cmp(
    left: &ConnectorChangePartition,
    right: &ConnectorChangePartition,
) -> Ordering {
    semantic_partition_cmp(left, right).then_with(|| {
        left.fields
            .iter()
            .map(|field| field.source_column.as_ref())
            .cmp(
                right
                    .fields
                    .iter()
                    .map(|field| field.source_column.as_ref()),
            )
    })
}

fn semantic_partition_cmp(
    left: &ConnectorChangePartition,
    right: &ConnectorChangePartition,
) -> Ordering {
    let ordering = left
        .partition_spec_identity
        .cmp(&right.partition_spec_identity);
    if ordering != Ordering::Equal {
        return ordering;
    }
    for (left, right) in left.fields.iter().zip(&right.fields) {
        let ordering = left
            .source_field_identity
            .cmp(&right.source_field_identity)
            .then_with(|| left.transform.cmp(&right.transform))
            .then_with(|| left.value.cmp(&right.value));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.fields.len().cmp(&right.fields.len())
}

fn connector_scan_selection_digest(selection: ConnectorScanSelection) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.connector.scan.selection.v1\0");
    match selection {
        ConnectorScanSelection::Snapshot(selector) => {
            digest.update([0]);
            match selector {
                ConnectorReadSelector::Current => digest.update([0]),
                ConnectorReadSelector::SnapshotId(snapshot_id) => {
                    digest.update([1]);
                    digest.update(snapshot_id.to_le_bytes());
                }
                ConnectorReadSelector::TimestampMicros(timestamp) => {
                    digest.update([2]);
                    digest.update(timestamp.to_le_bytes());
                }
            }
        }
        ConnectorScanSelection::ChangeWindow(window) => {
            digest.update([1]);
            digest.update(window.from_exclusive.to_le_bytes());
            digest.update(window.to_inclusive.to_le_bytes());
        }
    }
    digest.finalize().into()
}

fn connector_scan_admission_digest(admission: &ConnectorScanAdmission) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.connector.scan.admission.v1\0");
    match admission {
        ConnectorScanAdmission::Snapshot => digest.update([0]),
        ConnectorScanAdmission::ChangeWindow(admission) => {
            digest.update([1]);
            hash_change_window_admission(&mut digest, admission);
        }
    }
    digest.finalize().into()
}

fn hash_change_window_admission(digest: &mut Sha256, admission: &ConnectorChangeWindowAdmission) {
    match admission {
        ConnectorChangeWindowAdmission::MetadataOnly => digest.update([0]),
        ConnectorChangeWindowAdmission::Incremental {
            has_inserts,
            has_deletes,
            partition_impact,
        } => {
            digest.update([1, u8::from(*has_inserts), u8::from(*has_deletes)]);
            hash_partition_impact(digest, partition_impact);
        }
        ConnectorChangeWindowAdmission::FullRebuild(reason) => {
            digest.update([2]);
            match reason {
                ConnectorChangeWindowFullRebuildReason::LineageBroken { from_snapshot_id } => {
                    digest.update([0]);
                    digest.update(from_snapshot_id.to_le_bytes());
                }
                ConnectorChangeWindowFullRebuildReason::UnprovenReplace {
                    snapshot_id,
                    failure,
                } => {
                    digest.update([1]);
                    digest.update(snapshot_id.to_le_bytes());
                    digest.update([match failure {
                        ConnectorChangeWindowReplaceFailure::MissingParent => 0,
                        ConnectorChangeWindowReplaceFailure::RecordCountChanged => 1,
                        ConnectorChangeWindowReplaceFailure::MissingOrInvalidSummary => 2,
                        ConnectorChangeWindowReplaceFailure::InvalidDataFileCounts => 3,
                        ConnectorChangeWindowReplaceFailure::SchemaChanged => 4,
                    }]);
                }
            }
        }
    }
}

fn hash_partition_impact(digest: &mut Sha256, impact: &ConnectorChangeWindowPartitionImpact) {
    match impact {
        ConnectorChangeWindowPartitionImpact::Unavailable => digest.update([0]),
        ConnectorChangeWindowPartitionImpact::Unpartitioned => digest.update([1]),
        ConnectorChangeWindowPartitionImpact::Exact {
            has_row_deletes,
            added,
            removed,
        } => {
            digest.update([2, u8::from(*has_row_deletes)]);
            hash_partitions(digest, added);
            hash_partitions(digest, removed);
        }
    }
}

fn hash_partitions(digest: &mut Sha256, partitions: &[ConnectorChangePartition]) {
    digest.update((partitions.len() as u64).to_le_bytes());
    for partition in partitions {
        hash_bytes(digest, &partition.partition_spec_identity);
        digest.update((partition.fields.len() as u64).to_le_bytes());
        for field in &partition.fields {
            hash_bytes(digest, &field.source_field_identity);
            hash_bytes(digest, field.source_column.as_bytes());
            hash_partition_transform(digest, field.transform);
            match &field.value {
                ConnectorChangePartitionValue::Null => digest.update([0]),
                ConnectorChangePartitionValue::String(value) => {
                    digest.update([1]);
                    hash_bytes(digest, value.as_bytes());
                }
            }
        }
    }
}

fn hash_partition_transform(digest: &mut Sha256, transform: ConnectorChangePartitionTransform) {
    match transform {
        ConnectorChangePartitionTransform::Identity => digest.update([0]),
        ConnectorChangePartitionTransform::Year => digest.update([1]),
        ConnectorChangePartitionTransform::Month => digest.update([2]),
        ConnectorChangePartitionTransform::Day => digest.update([3]),
        ConnectorChangePartitionTransform::Hour => digest.update([4]),
        ConnectorChangePartitionTransform::Bucket { buckets } => {
            digest.update([5]);
            digest.update(buckets.get().to_le_bytes());
        }
        ConnectorChangePartitionTransform::Truncate { width } => {
            digest.update([6]);
            digest.update(width.get().to_le_bytes());
        }
    }
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_le_bytes());
    digest.update(bytes);
}

fn connector_scan_seal_digest(
    owner: &ConnectorProviderBindingKey,
    selection_digest: [u8; 32],
    handle_digest: [u8; 32],
    admission_digest: [u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.connector.scan.seal.v1\0");
    hash_bytes(&mut digest, owner.instance_id.as_str().as_bytes());
    digest.update(owner.incarnation.to_bytes());
    digest.update(selection_digest);
    digest.update(handle_digest);
    digest.update(admission_digest);
    digest.finalize().into()
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn resource_exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

/// SQL-owned intent for a provider-neutral read. Providers may use this to
/// reject read handles that are valid for ordinary scans but unsafe for a
/// specialized consumer, without exposing provider payloads to Core.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConnectorReadPurpose {
    #[default]
    Query,
    MvTargetState,
    MvTargetLocator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorBatchBudget {
    pub max_rows: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
}

#[derive(Clone)]
pub struct ConnectorBeginScanRequest {
    pub projection: Vec<usize>,
    pub static_predicates: Vec<ConnectorStaticPredicate>,
    pub selection: ConnectorScanSelection,
    pub purpose: ConnectorReadPurpose,
    pub limit: Option<u64>,
    pub batch: ConnectorBatchBudget,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorSplitPlanningRequest {
    pub target_parallelism: NonZeroUsize,
    pub max_split_bytes: Option<NonZeroU64>,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorSplitPlanningMetrics {
    pub candidate_units_considered: u64,
    pub candidate_units_pruned: u64,
    pub composite_splits_planned: u64,
    pub scan_units_planned: u64,
}

#[derive(Clone, Debug)]
pub struct ConnectorSplitPlanningResult {
    pub splits: Vec<ConnectorSplit>,
    pub metrics: ConnectorSplitPlanningMetrics,
    /// FE-local prepared remote session. It never enters any execution carrier.
    pub session: Option<ConnectorReadSessionLease>,
}

impl ConnectorSplitPlanningResult {
    pub fn try_new(
        splits: Vec<ConnectorSplit>,
        metrics: ConnectorSplitPlanningMetrics,
    ) -> Result<Self, ConnectorError> {
        if metrics.candidate_units_pruned > metrics.candidate_units_considered {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector split planning metrics report more pruned units than considered units",
            ));
        }
        Ok(Self {
            splits,
            metrics,
            session: None,
        })
    }

    pub fn try_new_with_session(
        splits: Vec<ConnectorSplit>,
        metrics: ConnectorSplitPlanningMetrics,
        session: ConnectorReadSessionLease,
    ) -> Result<Self, ConnectorError> {
        let mut result = Self::try_new(splits, metrics)?;
        result.session = Some(session);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;

    use super::*;
    use crate::connector::{ConnectorInstanceId, ProviderBindingEpoch};

    fn context(max_handle_bytes: usize, max_total_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            crate::connector::ConnectorStopOwner::new().view(),
            max_handle_bytes,
            max_total_bytes,
        )
        .expect("valid connector request context")
    }

    fn owner() -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg.test").expect("instance ID"),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        }
    }

    fn handle(owner: &ConnectorProviderBindingKey, payload: &'static [u8]) -> ConnectorScanHandle {
        ConnectorScanHandle::try_new(owner.instance_id.clone(), Bytes::from_static(payload))
            .expect("scan handle")
    }

    fn field(
        source: &str,
        transform: ConnectorChangePartitionTransform,
        value: &str,
    ) -> ConnectorChangePartitionField {
        ConnectorChangePartitionField::try_new(
            Bytes::from(source.to_ascii_lowercase()),
            source,
            transform,
            ConnectorChangePartitionValue::String(Arc::from(value)),
        )
        .expect("partition field")
    }

    fn partition(source: &str, value: &str) -> ConnectorChangePartition {
        ConnectorChangePartition::try_new(
            Bytes::from_static(b"spec-1"),
            vec![field(
                source,
                ConnectorChangePartitionTransform::Identity,
                value,
            )],
        )
        .expect("partition")
    }

    #[test]
    fn sealed_snapshot_scan_rejects_selection_and_handle_tampering() {
        let owner = owner();
        let mut scan = ConnectorScan::try_new_snapshot(
            owner.clone(),
            ConnectorReadSelector::Current,
            handle(&owner, b"scan-v1"),
            Arc::new(Schema::empty()),
            Vec::new(),
        )
        .expect("sealed snapshot scan");

        scan.validate(
            &owner,
            ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
        )
        .expect("matching scan seal");
        assert_eq!(
            scan.validate(
                &owner,
                ConnectorScanSelection::Snapshot(ConnectorReadSelector::SnapshotId(7)),
            )
            .expect_err("selection replay must fail")
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        scan.handle = handle(&owner, b"scan-v2");
        assert_eq!(
            scan.validate(
                &owner,
                ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
            )
            .expect_err("handle tampering must fail")
            .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn scan_rejects_mismatched_selection_and_admission_tags() {
        let owner = owner();
        let error = ConnectorScan::try_new(
            owner.clone(),
            ConnectorScanSelection::Snapshot(ConnectorReadSelector::Current),
            ConnectorScanAdmission::ChangeWindow(ConnectorChangeWindowAdmission::MetadataOnly),
            handle(&owner, b"scan-v1"),
            Arc::new(Schema::empty()),
            Vec::new(),
        )
        .expect_err("selection and admission tags must match");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn sealed_change_window_rejects_admission_tampering() {
        let owner = owner();
        let request_context = context(1024, 4096);
        let window = ConnectorChangeWindow::new(9, 9);
        let mut scan = ConnectorScan::try_new_change_window(
            owner.clone(),
            window,
            ConnectorChangeWindowAdmission::MetadataOnly,
            handle(&owner, b"delta-v1"),
            Arc::new(Schema::empty()),
            Vec::new(),
            &request_context,
        )
        .expect("same-endpoint metadata-only admission is valid");

        scan.validate(&owner, ConnectorScanSelection::ChangeWindow(window))
            .expect("matching metadata-only admission");
        scan.admission =
            ConnectorScanAdmission::ChangeWindow(ConnectorChangeWindowAdmission::FullRebuild(
                ConnectorChangeWindowFullRebuildReason::LineageBroken {
                    from_snapshot_id: 9,
                },
            ));
        assert_eq!(
            scan.validate(&owner, ConnectorScanSelection::ChangeWindow(window))
                .expect_err("admission tampering must fail")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn exact_partition_impact_is_canonical_and_sealed() {
        let owner = owner();
        let request_context = context(1024, 16 * 1024);
        let partition_a = ConnectorChangePartition::try_new(
            Bytes::from_static(b"spec-1"),
            vec![
                field("Zed", ConnectorChangePartitionTransform::Year, "2026"),
                field("account", ConnectorChangePartitionTransform::Identity, "7"),
            ],
        )
        .expect("canonical fields");
        assert_eq!(partition_a.fields()[0].source_column(), "account");
        let partition_b = partition("account", "8");
        let impact = ConnectorChangeWindowPartitionImpact::try_exact(
            true,
            vec![partition_b.clone(), partition_a.clone(), partition_b],
            vec![partition_a],
            &request_context,
        )
        .expect("canonical exact impact");
        let admission = ConnectorChangeWindowAdmission::Incremental {
            has_inserts: true,
            has_deletes: true,
            partition_impact: impact,
        };
        let window = ConnectorChangeWindow::new(11, 19);
        let scan = ConnectorScan::try_new_change_window(
            owner.clone(),
            window,
            admission,
            handle(&owner, b"delta-v1"),
            Arc::new(Schema::empty()),
            Vec::new(),
            &request_context,
        )
        .expect("sealed change-window scan");

        scan.validate(&owner, ConnectorScanSelection::ChangeWindow(window))
            .expect("matching change-window seal");
        let ConnectorScanAdmission::ChangeWindow(ConnectorChangeWindowAdmission::Incremental {
            partition_impact: ConnectorChangeWindowPartitionImpact::Exact { added, removed, .. },
            ..
        }) = scan.admission()
        else {
            panic!("expected exact change-window admission")
        };
        assert_eq!(added.len(), 2);
        assert_eq!(removed.len(), 1);
    }

    #[test]
    fn exact_partition_impact_keeps_distinct_provider_identities() {
        let make_partition = |spec: &'static [u8], source: &'static [u8]| {
            ConnectorChangePartition::try_new(
                Bytes::from_static(spec),
                vec![
                    ConnectorChangePartitionField::try_new(
                        Bytes::from_static(source),
                        "region",
                        ConnectorChangePartitionTransform::Identity,
                        ConnectorChangePartitionValue::String(Arc::from("us")),
                    )
                    .expect("field"),
                ],
            )
            .expect("partition")
        };
        let impact = ConnectorChangeWindowPartitionImpact::try_exact(
            false,
            vec![
                make_partition(b"spec-1", b"field-1"),
                make_partition(b"spec-2", b"field-1"),
                make_partition(b"spec-1", b"field-2"),
            ],
            Vec::new(),
            &context(1024, 4096),
        )
        .expect("distinct exact partitions");
        let ConnectorChangeWindowPartitionImpact::Exact { added, .. } = impact else {
            panic!("expected exact partition impact")
        };
        assert_eq!(added.len(), 3);
    }

    #[test]
    fn incremental_without_row_changes_is_rejected() {
        let owner = owner();
        let error = ConnectorScan::try_new_change_window(
            owner.clone(),
            ConnectorChangeWindow::new(5, 5),
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts: false,
                has_deletes: false,
                partition_impact: ConnectorChangeWindowPartitionImpact::Unpartitioned,
            },
            handle(&owner, b"delta-v1"),
            Arc::new(Schema::empty()),
            Vec::new(),
            &context(1024, 4096),
        )
        .expect_err("empty incremental admission must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn duplicate_partition_source_identity_and_transform_is_rejected() {
        let error = ConnectorChangePartition::try_new(
            Bytes::from_static(b"spec-1"),
            vec![
                ConnectorChangePartitionField::try_new(
                    Bytes::from_static(b"field-1"),
                    "OldName",
                    ConnectorChangePartitionTransform::Identity,
                    ConnectorChangePartitionValue::String(Arc::from("7")),
                )
                .expect("first field"),
                ConnectorChangePartitionField::try_new(
                    Bytes::from_static(b"field-1"),
                    "NewName",
                    ConnectorChangePartitionTransform::Identity,
                    ConnectorChangePartitionValue::String(Arc::from("8")),
                )
                .expect("second field"),
            ],
        )
        .expect_err("duplicate provider source identity must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn noncanonical_direct_partition_impact_is_rejected() {
        let owner = owner();
        let later = partition("account", "9");
        let earlier = partition("account", "1");
        let error = ConnectorScan::try_new_change_window(
            owner.clone(),
            ConnectorChangeWindow::new(1, 2),
            ConnectorChangeWindowAdmission::Incremental {
                has_inserts: true,
                has_deletes: false,
                partition_impact: ConnectorChangeWindowPartitionImpact::Exact {
                    has_row_deletes: false,
                    added: vec![later, earlier],
                    removed: Vec::new(),
                },
            },
            handle(&owner, b"delta-v1"),
            Arc::new(Schema::empty()),
            Vec::new(),
            &context(1024, 4096),
        )
        .expect_err("noncanonical direct exact impact must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn partition_impact_is_charged_to_request_payload_budget() {
        let source = "a".repeat(128);
        let oversized = ConnectorChangeWindowPartitionImpact::try_exact(
            false,
            vec![partition(&source, "value")],
            Vec::new(),
            &context(32, 64),
        )
        .expect_err("partition facts over the request budget must fail");
        assert_eq!(oversized.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn partition_impact_enforces_unique_partition_limit() {
        let added = (0..=MAX_CONNECTOR_CHANGE_PARTITIONS)
            .map(|value| partition("account", &value.to_string()))
            .collect();
        let error = ConnectorChangeWindowPartitionImpact::try_exact(
            false,
            added,
            Vec::new(),
            &context(
                crate::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                crate::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            ),
        )
        .expect_err("unique partition bound must be enforced");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }
}
