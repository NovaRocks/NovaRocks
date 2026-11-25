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
use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::read_stack::ConnectorReadSourceProjection;
use super::{
    ConnectorError, ConnectorErrorKind, ConnectorPredicateDisposition, ConnectorProviderBindingKey,
    ConnectorRequestContext, ConnectorScanHandle, ConnectorSplit, ConnectorStaticPredicate,
    SharedConnectorReadSession,
};

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorScanAdmission {
    Snapshot,
}

/// Provider facts about one exact source window, frozen before projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowFacts {
    NotIncremental(ConnectorChangeWindowNotIncrementalReason),
    NetZero(ConnectorChangeWindowNetZeroBasis),
    Difference {
        negative_weight: ConnectorChangeWindowNegativeWeight,
        partition_impact: ConnectorChangeWindowPartitionImpact,
        volume: ConnectorChangeWindowVolume,
    },
}

impl ConnectorChangeWindowFacts {
    pub const fn permits(&self, projection: ConnectorReadSourceProjection) -> bool {
        match projection {
            ConnectorReadSourceProjection::To => true,
            ConnectorReadSourceProjection::From => matches!(self, Self::Difference { .. }),
            ConnectorReadSourceProjection::Delta => {
                matches!(self, Self::NetZero(_) | Self::Difference { .. })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowNotIncrementalReason {
    SnapshotAncestryBroken { from_snapshot_id: i64 },
    SchemaChanged { snapshot_id: i64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowNetZeroBasis {
    SameSnapshot,
    ReplaceOnly,
    EqualEndpoints,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowNegativeWeight {
    Absent,
    Possible,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorChangeWindowVolume {
    /// Upper bound under the format contract, before positive and negative events cancel.
    pub source_event_rows_upper_bound: u128,
    /// Physical size of the source data files selected by the difference, once per variant.
    pub source_data_file_bytes: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorChangeWindowPartitionImpact {
    Unavailable,
    Unpartitioned,
    Exact(Arc<ConnectorChangeWindowExactPartitionImpact>),
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConnectorChangeWindowExactPartitionImpact {
    has_row_deletes: bool,
    added: Vec<ConnectorChangePartition>,
    removed: Vec<ConnectorChangePartition>,
}

impl ConnectorChangeWindowExactPartitionImpact {
    pub const fn has_row_deletes(&self) -> bool {
        self.has_row_deletes
    }

    pub fn added(&self) -> &[ConnectorChangePartition] {
        &self.added
    }

    pub fn removed(&self) -> &[ConnectorChangePartition] {
        &self.removed
    }
}

/// Retains only the canonical entries that fit the request's optional impact budget.
pub struct ConnectorChangeWindowPartitionImpactBuilder {
    has_row_deletes: bool,
    max_bytes: usize,
    partitions: Option<RetainedChangePartitions>,
}

struct RetainedChangePartitions {
    bytes: usize,
    added: BTreeSet<SemanticChangePartition>,
    removed: BTreeSet<SemanticChangePartition>,
}

impl ConnectorChangeWindowPartitionImpactBuilder {
    pub fn new(has_row_deletes: bool, context: &ConnectorRequestContext) -> Self {
        let max_bytes = context.max_total_payload_bytes();
        Self {
            has_row_deletes,
            max_bytes,
            partitions: (CHANGE_PARTITION_IMPACT_BYTES <= max_bytes).then(|| {
                RetainedChangePartitions {
                    bytes: CHANGE_PARTITION_IMPACT_BYTES,
                    added: BTreeSet::new(),
                    removed: BTreeSet::new(),
                }
            }),
        }
    }

    pub const fn is_available(&self) -> bool {
        self.partitions.is_some()
    }

    pub fn mark_row_deletes(&mut self) {
        self.has_row_deletes = true;
    }

    pub fn add(&mut self, partition: ConnectorChangePartition) {
        self.retain(partition, |partitions| &mut partitions.added);
    }

    pub fn remove(&mut self, partition: ConnectorChangePartition) {
        self.retain(partition, |partitions| &mut partitions.removed);
    }

    fn retain(
        &mut self,
        partition: ConnectorChangePartition,
        side: impl FnOnce(&mut RetainedChangePartitions) -> &mut BTreeSet<SemanticChangePartition>,
    ) {
        let Some(partitions) = &mut self.partitions else {
            return;
        };
        let candidate = SemanticChangePartition(partition);
        let bytes = partitions.bytes;
        let side = side(partitions);
        let previous_bytes = if let Some(existing) = side.get(&candidate) {
            if canonical_partition_cmp(&candidate.0, &existing.0) != Ordering::Less {
                return;
            }
            existing
                .0
                .payload_bytes()
                .expect("retained partition was charged")
        } else {
            0
        };
        let new_bytes = candidate
            .0
            .payload_bytes()
            .and_then(|candidate_bytes| (bytes - previous_bytes).checked_add(candidate_bytes));
        if let Some(new_bytes) = new_bytes.filter(|bytes| *bytes <= self.max_bytes) {
            side.replace(candidate);
            partitions.bytes = new_bytes;
        } else {
            self.partitions = None;
        }
    }

    pub fn finish(self) -> ConnectorChangeWindowPartitionImpact {
        let Some(partitions) = self.partitions else {
            return ConnectorChangeWindowPartitionImpact::Unavailable;
        };
        ConnectorChangeWindowPartitionImpact::Exact(Arc::new(
            ConnectorChangeWindowExactPartitionImpact {
                has_row_deletes: self.has_row_deletes,
                added: partitions.added.into_iter().map(|entry| entry.0).collect(),
                removed: partitions
                    .removed
                    .into_iter()
                    .map(|entry| entry.0)
                    .collect(),
            },
        ))
    }
}

struct SemanticChangePartition(ConnectorChangePartition);

impl PartialEq for SemanticChangePartition {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SemanticChangePartition {}

impl PartialOrd for SemanticChangePartition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SemanticChangePartition {
    fn cmp(&self, other: &Self) -> Ordering {
        semantic_partition_cmp(&self.0, &other.0)
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
        if partition_spec_identity.is_empty() {
            return Err(invalid(
                "connector change partition has an invalid partition spec identity",
            ));
        }
        if fields.is_empty() {
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
        if source_field_identity.is_empty() {
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

impl ConnectorChangePartition {
    fn payload_bytes(&self) -> Option<usize> {
        self.fields.iter().try_fold(
            CHANGE_PARTITION_BYTES.checked_add(self.partition_spec_identity.len())?,
            |bytes, field| {
                let value_bytes = match &field.value {
                    ConnectorChangePartitionValue::Null => 0,
                    ConnectorChangePartitionValue::String(value) => value.len(),
                };
                bytes
                    .checked_add(CHANGE_PARTITION_FIELD_BYTES)?
                    .checked_add(field.source_field_identity.len())?
                    .checked_add(field.source_column.len())?
                    .checked_add(value_bytes)
            },
        )
    }
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
    }
    digest.finalize().into()
}

fn connector_scan_admission_digest(admission: &ConnectorScanAdmission) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"novarocks.connector.scan.admission.v1\0");
    match admission {
        ConnectorScanAdmission::Snapshot => digest.update([0]),
    }
    digest.finalize().into()
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
    pub session: Option<SharedConnectorReadSession>,
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
        session: SharedConnectorReadSession,
    ) -> Result<Self, ConnectorError> {
        let mut result = Self::try_new(splits, metrics)?;
        result.session = Some(session);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use bytes::Bytes;

    use super::*;
    use crate::connector::{ConnectorInstanceId, ProviderBindingEpoch};

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
}
