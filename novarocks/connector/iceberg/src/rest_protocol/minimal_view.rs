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

//! Minimal views: the few fields attribution and acquisition need, decoded
//! by the streaming scanner without building upstream metadata objects.
//!
//! Five views exist:
//! - table response (commit, create table, stage):
//!   [`TableResponseSink`] -> [`TableResponseView`];
//! - create namespace: [`NamespaceSink`] -> [`NamespaceView`];
//! - view mutation: [`ViewMutationSink`] -> [`ViewMutationView`];
//! - credentials endpoint: [`CredentialsSink`] -> [`CredentialsView`];
//! - backend acquisition (load table):
//!   [`BackendAcquisitionSink`] -> [`BackendAcquisitionView`].
//!
//! Every retained value has an explicit length limit and every retained
//! collection an explicit entry limit, so a view's size does not depend on the
//! response size. A field the view uses that occurs twice is a
//! `DuplicateField` error rather than "first wins" or "last wins".
//!
//! Views hold response values, which may be sensitive (credential material in
//! particular), so their `Debug` output reports presence and counts only.

use std::collections::BTreeMap;
use std::fmt;

use super::json::{Key, MAX_KEY_LEN, Scalar, ScanError, ValueKind, ViewSink};

/// Limit of a `metadata-location` value.
pub(crate) const METADATA_LOCATION_LIMIT: usize = 2 * 1024;
/// Limit of a table or view UUID text.
pub(crate) const UUID_TEXT_LIMIT: usize = 64;
/// Limit of the text of an integer such as a snapshot id.
pub(crate) const INTEGER_TEXT_LIMIT: usize = 32;
/// Limit of one namespace level.
pub(crate) const NAMESPACE_LEVEL_LIMIT: usize = 1024;
/// Most namespace levels retained.
pub(crate) const MAX_NAMESPACE_LEVELS: usize = 64;
/// Most storage credentials retained.
pub(crate) const MAX_STORAGE_CREDENTIALS: usize = 64;
/// Most config entries retained per storage credential.
pub(crate) const MAX_CREDENTIAL_CONFIG_ENTRIES: usize = 64;
/// Limit of a storage credential prefix.
pub(crate) const CREDENTIAL_PREFIX_LIMIT: usize = 2 * 1024;
/// Limit of a storage credential config value.
pub(crate) const CREDENTIAL_VALUE_LIMIT: usize = 16 * 1024;

fn first(seen: &mut bool, field: &'static str) -> Result<(), ScanError> {
    if std::mem::replace(seen, true) {
        Err(ScanError::DuplicateField(field))
    } else {
        Ok(())
    }
}

fn expect_kind(
    kind: ValueKind,
    allowed: &[ValueKind],
    field: &'static str,
) -> Result<(), ScanError> {
    if allowed.contains(&kind) {
        Ok(())
    } else {
        Err(ScanError::UnexpectedType(field))
    }
}

fn retained_integer(value: Scalar<'_>, field: &'static str) -> Result<Option<i64>, ScanError> {
    match value {
        Scalar::Number(number) => number
            .as_i64()
            .map(Some)
            .ok_or(ScanError::InvalidValue(field)),
        Scalar::Overlong(_) => Err(ScanError::ValueTooLong(field)),
        _ => Ok(None),
    }
}

fn retained_string(value: Scalar<'_>, field: &'static str) -> Result<Option<String>, ScanError> {
    match value {
        Scalar::Str(text) => Ok(Some(text.to_owned())),
        Scalar::Overlong(_) => Err(ScanError::ValueTooLong(field)),
        _ => Ok(None),
    }
}

/// A `metadata-location` member as it appeared.
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum LocationField {
    Absent,
    Null,
    Text(String),
}

impl LocationField {
    /// A non-empty location string.
    pub(crate) fn non_empty(&self) -> Option<&str> {
        match self {
            Self::Text(text) if !text.is_empty() => Some(text),
            _ => None,
        }
    }

    /// Absent, null or empty: the shape a stage response must have.
    pub(crate) fn is_unset(&self) -> bool {
        self.non_empty().is_none()
    }
}

impl fmt::Debug for LocationField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => f.write_str("Absent"),
            Self::Null => f.write_str("Null"),
            Self::Text(text) if text.is_empty() => f.write_str("Empty"),
            Self::Text(_) => f.write_str("Present"),
        }
    }
}

// ---------------------------------------------------------------------------
// Table response
// ---------------------------------------------------------------------------

/// Why a [`TableViewRequest`] cannot be scanned for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ViewRequestError {
    RefNameTooLong,
    PropertyKeyTooLong,
    DuplicatePropertyKey,
}

impl fmt::Display for ViewRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RefNameTooLong => "target ref name exceeds the response key limit",
            Self::PropertyKeyTooLong => "expected property key exceeds the response key limit",
            Self::DuplicatePropertyKey => "expected property key is named twice",
        })
    }
}

impl std::error::Error for ViewRequestError {}

/// What a table-response scan retains besides location and UUID.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TableViewRequest {
    snapshot: Option<i64>,
    target_ref: Option<String>,
    property_keys: Vec<String>,
    property_value_lengths: Vec<usize>,
    removed_snapshots: Vec<i64>,
    retention_refs: Vec<String>,
}

impl TableViewRequest {
    /// `snapshot`: the snapshot whose presence in `metadata.snapshots` is
    /// recorded. `target_ref`: the ref whose `snapshot-id` is recorded for
    /// diagnosis. `properties`: NovaRocks-named `metadata.properties` keys and
    /// the UTF8 byte lengths of their expected values.
    pub(crate) fn new(
        snapshot: Option<i64>,
        target_ref: Option<String>,
        properties: Vec<(String, usize)>,
    ) -> Result<Self, ViewRequestError> {
        if target_ref
            .as_ref()
            .is_some_and(|name| name.len() > MAX_KEY_LEN)
        {
            return Err(ViewRequestError::RefNameTooLong);
        }
        for (index, (key, _)) in properties.iter().enumerate() {
            if key.len() > MAX_KEY_LEN {
                return Err(ViewRequestError::PropertyKeyTooLong);
            }
            if properties[..index]
                .iter()
                .any(|(previous, _)| previous == key)
            {
                return Err(ViewRequestError::DuplicatePropertyKey);
            }
        }
        let (property_keys, property_value_lengths) = properties.into_iter().unzip();
        Ok(Self {
            snapshot,
            target_ref,
            property_keys,
            property_value_lengths,
            removed_snapshots: Vec::new(),
            retention_refs: Vec::new(),
        })
    }

    pub(crate) fn with_retention(
        mut self,
        removed_snapshots: Vec<i64>,
        refs: Vec<String>,
    ) -> Result<Self, ViewRequestError> {
        if refs.iter().any(|name| name.len() > MAX_KEY_LEN) {
            return Err(ViewRequestError::RefNameTooLong);
        }
        self.removed_snapshots = removed_snapshots;
        self.retention_refs = refs;
        Ok(self)
    }
    pub(crate) fn removed_snapshots(&self) -> &[i64] {
        &self.removed_snapshots
    }
    pub(crate) fn retention_refs(&self) -> &[String] {
        &self.retention_refs
    }

    pub(crate) fn snapshot(&self) -> Option<i64> {
        self.snapshot
    }

    pub(crate) fn target_ref(&self) -> Option<&str> {
        self.target_ref.as_deref()
    }

    pub(crate) fn property_keys(&self) -> &[String] {
        &self.property_keys
    }
}

/// Whether the requested snapshot appeared in `metadata.snapshots`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotCheck {
    pub(crate) snapshot_id: i64,
    pub(crate) present: bool,
}

/// The requested ref's `snapshot-id`, for diagnosis only.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct RefCheck {
    pub(crate) name: String,
    /// `None` when the ref or its `snapshot-id` is absent.
    pub(crate) snapshot_id: Option<i64>,
}

impl fmt::Debug for RefCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefCheck")
            .field("name", &self.name)
            .field("snapshot_present", &self.snapshot_id.is_some())
            .finish()
    }
}

/// A requested property as it appeared.
#[derive(Clone, Eq, PartialEq)]
pub(crate) enum PropertyValue {
    Absent,
    Text(String),
    /// Longer than the request's expected property value.
    Overlong,
}

impl fmt::Debug for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Absent => "Absent",
            Self::Text(_) => "Present",
            Self::Overlong => "Overlong",
        })
    }
}

/// Minimal view of a `LoadTableResult` / `CommitTableResponse`.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct TableResponseView {
    pub(crate) metadata_location: LocationField,
    pub(crate) metadata_predecessor: crate::catalog::transaction::MetadataPredecessorObservation,
    pub(crate) table_uuid: Option<String>,
    /// `Some` exactly when the request named a snapshot.
    pub(crate) snapshot: Option<SnapshotCheck>,
    /// `Some` exactly when the request named a target ref.
    pub(crate) target_ref: Option<RefCheck>,
    /// One entry per requested key, in request order.
    pub(crate) properties: Vec<(String, PropertyValue)>,
    pub(crate) retention_snapshots: Vec<SnapshotCheck>,
    pub(crate) retention_refs: Vec<(String, bool, Option<i64>)>,
}

impl fmt::Debug for TableResponseView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableResponseView")
            .field("metadata_location", &self.metadata_location)
            .field("table_uuid_present", &self.table_uuid.is_some())
            .field("snapshot", &self.snapshot)
            .field("target_ref", &self.target_ref)
            .field("properties", &self.properties)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableNode {
    Root,
    MetadataLocation,
    Metadata,
    TableUuid,
    Snapshots,
    Snapshot,
    SnapshotId,
    Refs,
    TargetRef,
    TargetRefSnapshotId,
    Properties,
    Property(usize),
    RetentionRef(usize),
    RetentionRefSnapshotId(usize),
    MetadataLog,
    MetadataLogEntry,
    MetadataLogFile,
    MetadataLogTimestamp,
}

/// Sink for the table-response view.
pub(crate) struct TableResponseSink {
    request: TableViewRequest,
    seen_location: bool,
    seen_metadata: bool,
    seen_uuid: bool,
    seen_snapshots: bool,
    seen_refs: bool,
    seen_properties: bool,
    seen_target_ref: bool,
    seen_ref_snapshot_id: bool,
    seen_element_snapshot_id: bool,
    seen_property: Vec<bool>,
    metadata_location: LocationField,
    table_uuid: Option<String>,
    snapshot_present: bool,
    retention_snapshots_present: Vec<bool>,
    retention_refs: Vec<(bool, Option<i64>)>,
    seen_retention_ref_id: Vec<bool>,
    ref_snapshot_id: Option<i64>,
    properties: Vec<PropertyValue>,
    seen_metadata_log: bool,
    metadata_log_invalid: bool,
    log_entry_valid: bool,
    seen_log_file: bool,
    seen_log_timestamp: bool,
    log_file: Option<String>,
    log_timestamp: Option<i64>,
    metadata_predecessor: crate::catalog::transaction::MetadataPredecessorObservation,
}

impl TableResponseSink {
    pub(crate) fn new(request: TableViewRequest) -> Self {
        let keys = request.property_keys.len();
        let removed = request.removed_snapshots.len();
        let refs = request.retention_refs.len();
        Self {
            request,
            seen_location: false,
            seen_metadata: false,
            seen_uuid: false,
            seen_snapshots: false,
            seen_refs: false,
            seen_properties: false,
            seen_target_ref: false,
            seen_ref_snapshot_id: false,
            seen_element_snapshot_id: false,
            seen_property: vec![false; keys],
            metadata_location: LocationField::Absent,
            table_uuid: None,
            snapshot_present: false,
            retention_snapshots_present: vec![false; removed],
            retention_refs: vec![(false, None); refs],
            seen_retention_ref_id: vec![false; refs],
            ref_snapshot_id: None,
            properties: vec![PropertyValue::Absent; keys],
            seen_metadata_log: false,
            metadata_log_invalid: false,
            log_entry_valid: false,
            seen_log_file: false,
            seen_log_timestamp: false,
            log_file: None,
            log_timestamp: None,
            metadata_predecessor: Default::default(),
        }
    }
}

impl ViewSink for TableResponseSink {
    type Node = TableNode;
    type View = TableResponseView;

    fn retention_cap(&self) -> usize {
        self.request
            .property_value_lengths
            .iter()
            .map(|length| length.saturating_add(1))
            .max()
            .unwrap_or(0)
            .max(METADATA_LOCATION_LIMIT)
    }

    fn root(&self) -> TableNode {
        TableNode::Root
    }

    fn open(&mut self, node: TableNode, kind: ValueKind) -> Result<usize, ScanError> {
        use ValueKind::*;
        match node {
            TableNode::Root => expect_kind(kind, &[Object], "response")?,
            TableNode::MetadataLocation => {
                expect_kind(kind, &[String, Null], "metadata-location")?;
                return Ok(METADATA_LOCATION_LIMIT);
            }
            TableNode::Metadata => expect_kind(kind, &[Object], "metadata")?,
            TableNode::TableUuid => {
                expect_kind(kind, &[String], "metadata.table-uuid")?;
                return Ok(UUID_TEXT_LIMIT);
            }
            TableNode::Snapshots => expect_kind(kind, &[Array, Null], "metadata.snapshots")?,
            TableNode::Snapshot => {
                expect_kind(kind, &[Object], "metadata.snapshots[]")?;
                self.seen_element_snapshot_id = false;
            }
            TableNode::SnapshotId => {
                expect_kind(kind, &[Number], "metadata.snapshots[].snapshot-id")?;
                return Ok(INTEGER_TEXT_LIMIT);
            }
            TableNode::Refs => expect_kind(kind, &[Object, Null], "metadata.refs")?,
            TableNode::TargetRef | TableNode::RetentionRef(_) => {
                expect_kind(kind, &[Object], "metadata.refs.<target>")?
            }
            TableNode::TargetRefSnapshotId | TableNode::RetentionRefSnapshotId(_) => {
                expect_kind(kind, &[Number], "metadata.refs.<target>.snapshot-id")?;
                return Ok(INTEGER_TEXT_LIMIT);
            }
            TableNode::MetadataLog => {
                if kind != Array {
                    self.metadata_log_invalid = true;
                }
            }
            TableNode::MetadataLogEntry => {
                self.log_entry_valid = kind == Object;
                if !self.log_entry_valid {
                    self.metadata_log_invalid = true;
                }
                self.log_file = None;
                self.log_timestamp = None;
                self.seen_log_file = false;
                self.seen_log_timestamp = false;
            }
            TableNode::MetadataLogFile => {
                if kind == String {
                    return Ok(METADATA_LOCATION_LIMIT);
                }
                self.log_entry_valid = false;
            }
            TableNode::MetadataLogTimestamp => {
                if kind == Number {
                    return Ok(INTEGER_TEXT_LIMIT);
                }
                self.log_entry_valid = false;
            }
            TableNode::Properties => expect_kind(kind, &[Object, Null], "metadata.properties")?,
            TableNode::Property(index) => {
                expect_kind(kind, &[String], "metadata.properties")?;
                return Ok(self.request.property_value_lengths[index].saturating_add(1));
            }
        }
        Ok(0)
    }

    fn member(&mut self, object: TableNode, key: Key<'_>) -> Result<Option<TableNode>, ScanError> {
        let Key::Text(key) = key else {
            // Every key this view names fits MAX_KEY_LEN (checked by
            // TableViewRequest::new), so an overlong key is never one of them.
            return Ok(None);
        };
        Ok(match (object, key) {
            (TableNode::Root, "metadata-location") => {
                first(&mut self.seen_location, "metadata-location")?;
                Some(TableNode::MetadataLocation)
            }
            (TableNode::Root, "metadata") => {
                first(&mut self.seen_metadata, "metadata")?;
                Some(TableNode::Metadata)
            }
            (TableNode::Metadata, "table-uuid") => {
                first(&mut self.seen_uuid, "metadata.table-uuid")?;
                Some(TableNode::TableUuid)
            }
            (TableNode::Metadata, "metadata-log") => {
                if self.seen_metadata_log {
                    self.metadata_log_invalid = true;
                }
                self.seen_metadata_log = true;
                Some(TableNode::MetadataLog)
            }
            (TableNode::MetadataLogEntry, "metadata-file") => {
                if self.seen_log_file {
                    self.log_entry_valid = false;
                }
                self.seen_log_file = true;
                Some(TableNode::MetadataLogFile)
            }
            (TableNode::MetadataLogEntry, "timestamp-ms") => {
                if self.seen_log_timestamp {
                    self.log_entry_valid = false;
                }
                self.seen_log_timestamp = true;
                Some(TableNode::MetadataLogTimestamp)
            }
            (TableNode::Metadata, "snapshots")
                if self.request.snapshot.is_some()
                    || !self.request.removed_snapshots.is_empty() =>
            {
                first(&mut self.seen_snapshots, "metadata.snapshots")?;
                Some(TableNode::Snapshots)
            }
            (TableNode::Metadata, "refs")
                if self.request.target_ref.is_some() || !self.request.retention_refs.is_empty() =>
            {
                first(&mut self.seen_refs, "metadata.refs")?;
                Some(TableNode::Refs)
            }
            (TableNode::Metadata, "properties") if !self.request.property_keys.is_empty() => {
                first(&mut self.seen_properties, "metadata.properties")?;
                Some(TableNode::Properties)
            }
            (TableNode::Snapshot, "snapshot-id") => {
                first(
                    &mut self.seen_element_snapshot_id,
                    "metadata.snapshots[].snapshot-id",
                )?;
                Some(TableNode::SnapshotId)
            }
            (TableNode::Refs, name) if self.request.target_ref.as_deref() == Some(name) => {
                first(&mut self.seen_target_ref, "metadata.refs.<target>")?;
                Some(TableNode::TargetRef)
            }
            (TableNode::TargetRef, "snapshot-id") => {
                first(
                    &mut self.seen_ref_snapshot_id,
                    "metadata.refs.<target>.snapshot-id",
                )?;
                Some(TableNode::TargetRefSnapshotId)
            }
            (TableNode::Refs, name) => {
                if let Some(index) = self
                    .request
                    .retention_refs
                    .iter()
                    .position(|key| key == name)
                {
                    first(
                        &mut self.retention_refs[index].0,
                        "metadata.refs.<retention>",
                    )?;
                    Some(TableNode::RetentionRef(index))
                } else {
                    None
                }
            }
            (TableNode::RetentionRef(index), "snapshot-id") => {
                first(
                    &mut self.seen_retention_ref_id[index],
                    "metadata.refs.<retention>.snapshot-id",
                )?;
                Some(TableNode::RetentionRefSnapshotId(index))
            }
            (TableNode::Properties, name) => {
                match self
                    .request
                    .property_keys
                    .iter()
                    .position(|key| key == name)
                {
                    Some(index) => {
                        first(&mut self.seen_property[index], "metadata.properties")?;
                        Some(TableNode::Property(index))
                    }
                    None => None,
                }
            }
            _ => None,
        })
    }

    fn element(&mut self, array: TableNode, _index: u64) -> Result<Option<TableNode>, ScanError> {
        Ok(match array {
            TableNode::Snapshots => Some(TableNode::Snapshot),
            TableNode::MetadataLog => Some(TableNode::MetadataLogEntry),
            _ => None,
        })
    }

    fn scalar(&mut self, node: TableNode, value: Scalar<'_>) -> Result<(), ScanError> {
        match node {
            TableNode::MetadataLocation => {
                self.metadata_location = match retained_string(value, "metadata-location")? {
                    Some(text) => LocationField::Text(text),
                    None => LocationField::Null,
                };
            }
            TableNode::TableUuid => {
                self.table_uuid = retained_string(value, "metadata.table-uuid")?;
            }
            TableNode::MetadataLogFile => match value {
                Scalar::Str(text) if !text.is_empty() => self.log_file = Some(text.to_owned()),
                _ => self.log_entry_valid = false,
            },
            TableNode::MetadataLogTimestamp => {
                self.log_timestamp =
                    retained_integer(value, "metadata.metadata-log[].timestamp-ms")
                        .ok()
                        .flatten();
                if self.log_timestamp.is_none() {
                    self.log_entry_valid = false;
                }
            }
            TableNode::SnapshotId => {
                let id = retained_integer(value, "metadata.snapshots[].snapshot-id")?;
                if id.is_some() && id == self.request.snapshot {
                    self.snapshot_present = true;
                }
                if let Some(id) = id
                    && let Some(index) = self
                        .request
                        .removed_snapshots
                        .iter()
                        .position(|candidate| *candidate == id)
                {
                    self.retention_snapshots_present[index] = true;
                }
            }
            TableNode::TargetRefSnapshotId => {
                self.ref_snapshot_id =
                    retained_integer(value, "metadata.refs.<target>.snapshot-id")?;
            }
            TableNode::RetentionRefSnapshotId(index) => {
                self.retention_refs[index].1 =
                    retained_integer(value, "metadata.refs.<retention>.snapshot-id")?;
            }
            TableNode::Property(index) => {
                self.properties[index] = match value {
                    Scalar::Str(text)
                        if text.len() <= self.request.property_value_lengths[index] =>
                    {
                        PropertyValue::Text(text.to_owned())
                    }
                    Scalar::Str(_) => PropertyValue::Overlong,
                    Scalar::Overlong(_) => PropertyValue::Overlong,
                    _ => PropertyValue::Absent,
                };
            }
            // Containers reported as null carry nothing.
            _ => {}
        }
        Ok(())
    }

    fn close(&mut self, node: TableNode) -> Result<(), ScanError> {
        if node == TableNode::Snapshot
            && !self.request.removed_snapshots.is_empty()
            && !self.seen_element_snapshot_id
        {
            return Err(ScanError::MissingField("metadata.snapshots[].snapshot-id"));
        }
        if node == TableNode::MetadataLogEntry {
            use crate::catalog::transaction::MetadataPredecessorObservation;
            self.metadata_predecessor = match (
                self.log_entry_valid,
                self.log_file.take(),
                self.log_timestamp.take(),
            ) {
                (true, Some(location), Some(timestamp_ms)) => {
                    MetadataPredecessorObservation::Candidate {
                        location,
                        timestamp_ms,
                    }
                }
                _ => MetadataPredecessorObservation::Unusable,
            };
        }
        Ok(())
    }

    fn finish(self) -> Result<TableResponseView, ScanError> {
        let snapshot = self.request.snapshot.map(|snapshot_id| SnapshotCheck {
            snapshot_id,
            present: self.snapshot_present,
        });
        let target_ref = self.request.target_ref.map(|name| RefCheck {
            name,
            snapshot_id: self.ref_snapshot_id,
        });
        let properties = self
            .request
            .property_keys
            .into_iter()
            .zip(self.properties)
            .collect();
        let retention_snapshots = self
            .request
            .removed_snapshots
            .into_iter()
            .zip(self.retention_snapshots_present)
            .map(|(snapshot_id, present)| SnapshotCheck {
                snapshot_id,
                present,
            })
            .collect();
        let retention_refs = self
            .request
            .retention_refs
            .into_iter()
            .zip(self.retention_refs)
            .map(|(name, (present, snapshot))| (name, present, snapshot))
            .collect();
        Ok(TableResponseView {
            metadata_location: self.metadata_location,
            metadata_predecessor: if self.metadata_log_invalid {
                crate::catalog::transaction::MetadataPredecessorObservation::Unusable
            } else {
                self.metadata_predecessor
            },
            table_uuid: self.table_uuid,
            retention_snapshots,
            retention_refs,
            snapshot,
            target_ref,
            properties,
        })
    }
}

// ---------------------------------------------------------------------------
// Create namespace
// ---------------------------------------------------------------------------

/// Minimal view of a `CreateNamespaceResponse`.
#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct NamespaceView {
    pub(crate) namespace: Option<Vec<String>>,
}

impl fmt::Debug for NamespaceView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceView")
            .field("levels", &self.namespace.as_ref().map(Vec::len))
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NamespaceNode {
    Root,
    Namespace,
    Level,
}

/// Sink for the namespace view.
#[derive(Default)]
pub(crate) struct NamespaceSink {
    seen_namespace: bool,
    namespace: Option<Vec<String>>,
}

impl NamespaceSink {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl ViewSink for NamespaceSink {
    type Node = NamespaceNode;
    type View = NamespaceView;

    fn retention_cap(&self) -> usize {
        NAMESPACE_LEVEL_LIMIT
    }

    fn root(&self) -> NamespaceNode {
        NamespaceNode::Root
    }

    fn open(&mut self, node: NamespaceNode, kind: ValueKind) -> Result<usize, ScanError> {
        match node {
            NamespaceNode::Root => expect_kind(kind, &[ValueKind::Object], "response")?,
            NamespaceNode::Namespace => {
                expect_kind(kind, &[ValueKind::Array], "namespace")?;
                self.namespace = Some(Vec::new());
            }
            NamespaceNode::Level => {
                expect_kind(kind, &[ValueKind::String], "namespace[]")?;
                return Ok(NAMESPACE_LEVEL_LIMIT);
            }
        }
        Ok(0)
    }

    fn member(
        &mut self,
        object: NamespaceNode,
        key: Key<'_>,
    ) -> Result<Option<NamespaceNode>, ScanError> {
        match (object, key) {
            (NamespaceNode::Root, Key::Text("namespace")) => {
                first(&mut self.seen_namespace, "namespace")?;
                Ok(Some(NamespaceNode::Namespace))
            }
            _ => Ok(None),
        }
    }

    fn element(
        &mut self,
        array: NamespaceNode,
        index: u64,
    ) -> Result<Option<NamespaceNode>, ScanError> {
        match array {
            NamespaceNode::Namespace if index >= MAX_NAMESPACE_LEVELS as u64 => {
                Err(ScanError::TooManyEntries("namespace"))
            }
            NamespaceNode::Namespace => Ok(Some(NamespaceNode::Level)),
            _ => Ok(None),
        }
    }

    fn scalar(&mut self, node: NamespaceNode, value: Scalar<'_>) -> Result<(), ScanError> {
        if node == NamespaceNode::Level
            && let Some(level) = retained_string(value, "namespace[]")?
            && let Some(namespace) = self.namespace.as_mut()
        {
            namespace.push(level);
        }
        Ok(())
    }

    fn close(&mut self, _node: NamespaceNode) -> Result<(), ScanError> {
        Ok(())
    }

    fn finish(self) -> Result<NamespaceView, ScanError> {
        Ok(NamespaceView {
            namespace: self.namespace,
        })
    }
}

// ---------------------------------------------------------------------------
// View mutation
// ---------------------------------------------------------------------------

/// Minimal view of a `LoadViewResult`.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ViewMutationView {
    pub(crate) metadata_location: LocationField,
    pub(crate) view_uuid: Option<String>,
}

impl fmt::Debug for ViewMutationView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewMutationView")
            .field("metadata_location", &self.metadata_location)
            .field("view_uuid_present", &self.view_uuid.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ViewMutationNode {
    Root,
    MetadataLocation,
    Metadata,
    ViewUuid,
}

/// Sink for the view-mutation view.
pub(crate) struct ViewMutationSink {
    seen_location: bool,
    seen_metadata: bool,
    seen_uuid: bool,
    metadata_location: LocationField,
    view_uuid: Option<String>,
}

impl ViewMutationSink {
    pub(crate) fn new() -> Self {
        Self {
            seen_location: false,
            seen_metadata: false,
            seen_uuid: false,
            metadata_location: LocationField::Absent,
            view_uuid: None,
        }
    }
}

impl ViewSink for ViewMutationSink {
    type Node = ViewMutationNode;
    type View = ViewMutationView;

    fn retention_cap(&self) -> usize {
        METADATA_LOCATION_LIMIT
    }

    fn root(&self) -> ViewMutationNode {
        ViewMutationNode::Root
    }

    fn open(&mut self, node: ViewMutationNode, kind: ValueKind) -> Result<usize, ScanError> {
        use ValueKind::*;
        match node {
            ViewMutationNode::Root => expect_kind(kind, &[Object], "response")?,
            ViewMutationNode::MetadataLocation => {
                expect_kind(kind, &[String, Null], "metadata-location")?;
                return Ok(METADATA_LOCATION_LIMIT);
            }
            ViewMutationNode::Metadata => expect_kind(kind, &[Object], "metadata")?,
            ViewMutationNode::ViewUuid => {
                expect_kind(kind, &[String], "metadata.view-uuid")?;
                return Ok(UUID_TEXT_LIMIT);
            }
        }
        Ok(0)
    }

    fn member(
        &mut self,
        object: ViewMutationNode,
        key: Key<'_>,
    ) -> Result<Option<ViewMutationNode>, ScanError> {
        Ok(match (object, key) {
            (ViewMutationNode::Root, Key::Text("metadata-location")) => {
                first(&mut self.seen_location, "metadata-location")?;
                Some(ViewMutationNode::MetadataLocation)
            }
            (ViewMutationNode::Root, Key::Text("metadata")) => {
                first(&mut self.seen_metadata, "metadata")?;
                Some(ViewMutationNode::Metadata)
            }
            (ViewMutationNode::Metadata, Key::Text("view-uuid")) => {
                first(&mut self.seen_uuid, "metadata.view-uuid")?;
                Some(ViewMutationNode::ViewUuid)
            }
            _ => None,
        })
    }

    fn element(
        &mut self,
        _array: ViewMutationNode,
        _index: u64,
    ) -> Result<Option<ViewMutationNode>, ScanError> {
        Ok(None)
    }

    fn scalar(&mut self, node: ViewMutationNode, value: Scalar<'_>) -> Result<(), ScanError> {
        match node {
            ViewMutationNode::MetadataLocation => {
                self.metadata_location = match retained_string(value, "metadata-location")? {
                    Some(text) => LocationField::Text(text),
                    None => LocationField::Null,
                };
            }
            ViewMutationNode::ViewUuid => {
                self.view_uuid = retained_string(value, "metadata.view-uuid")?;
            }
            _ => {}
        }
        Ok(())
    }

    fn close(&mut self, _node: ViewMutationNode) -> Result<(), ScanError> {
        Ok(())
    }

    fn finish(self) -> Result<ViewMutationView, ScanError> {
        Ok(ViewMutationView {
            metadata_location: self.metadata_location,
            view_uuid: self.view_uuid,
        })
    }
}

// ---------------------------------------------------------------------------
// Storage credentials
// ---------------------------------------------------------------------------

/// One `{prefix, config}` storage credential.
///
/// Its config values are secret material: there is no `Clone`, and `Debug`
/// prints config key names only. The only way to the values is
/// [`into_parts`](Self::into_parts), which consumes it.
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct RawStorageCredential {
    prefix: String,
    config: BTreeMap<String, String>,
}

impl RawStorageCredential {
    /// Adopt one credential from a fully decoded response document.
    pub(crate) fn from_decoded(prefix: String, config: BTreeMap<String, String>) -> Self {
        Self { prefix, config }
    }

    pub(crate) fn config_len(&self) -> usize {
        self.config.len()
    }

    /// Hand the raw values to the one adapter that translates them into the
    /// closed, redacted credential seed.
    pub(crate) fn into_parts(self) -> (String, BTreeMap<String, String>) {
        (self.prefix, self.config)
    }
}

impl fmt::Debug for RawStorageCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawStorageCredential")
            .field("config_keys", &self.config.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// The `storage-credentials` member as it appeared.
///
/// `Absent` covers both a missing member and `null`, which the upstream wire
/// type (`Option<Vec<_>>`) also treats alike. `Present` may be empty: the
/// translation into the credential seed refuses both shapes today, and keeping
/// them apart preserves that distinction for diagnosis.
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) enum StorageCredentials {
    Absent,
    Present(Vec<RawStorageCredential>),
}

impl StorageCredentials {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Absent => 0,
            Self::Present(credentials) => credentials.len(),
        }
    }
}

impl fmt::Debug for StorageCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => f.write_str("Absent"),
            Self::Present(credentials) => f.debug_list().entries(credentials).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialNode {
    List,
    Credential,
    Prefix,
    Config,
    ConfigValue,
}

#[derive(Default)]
struct PartialCredential {
    seen_prefix: bool,
    seen_config: bool,
    prefix: Option<String>,
    config: Option<BTreeMap<String, String>>,
}

/// Shared collector for `storage-credentials`, used by both credential views.
#[derive(Default)]
struct CredentialCollector {
    present: bool,
    credentials: Vec<RawStorageCredential>,
    current: Option<PartialCredential>,
    pending_key: Option<String>,
}

impl CredentialCollector {
    fn open(&mut self, node: CredentialNode, kind: ValueKind) -> Result<usize, ScanError> {
        use ValueKind::*;
        match node {
            CredentialNode::List => {
                expect_kind(kind, &[Array, Null], "storage-credentials")?;
                self.present = kind == Array;
            }
            CredentialNode::Credential => {
                expect_kind(kind, &[Object], "storage-credentials[]")?;
                self.current = Some(PartialCredential::default());
            }
            CredentialNode::Prefix => {
                expect_kind(kind, &[String], "storage-credentials[].prefix")?;
                return Ok(CREDENTIAL_PREFIX_LIMIT);
            }
            CredentialNode::Config => {
                expect_kind(kind, &[Object], "storage-credentials[].config")?;
                if let Some(current) = self.current.as_mut() {
                    current.config = Some(BTreeMap::new());
                }
            }
            CredentialNode::ConfigValue => {
                expect_kind(kind, &[String], "storage-credentials[].config")?;
                return Ok(CREDENTIAL_VALUE_LIMIT);
            }
        }
        Ok(0)
    }

    fn member(
        &mut self,
        object: CredentialNode,
        key: Key<'_>,
    ) -> Result<Option<CredentialNode>, ScanError> {
        let Some(current) = self.current.as_mut() else {
            return Ok(None);
        };
        match (object, key) {
            (CredentialNode::Credential, Key::Text("prefix")) => {
                first(&mut current.seen_prefix, "storage-credentials[].prefix")?;
                Ok(Some(CredentialNode::Prefix))
            }
            (CredentialNode::Credential, Key::Text("config")) => {
                first(&mut current.seen_config, "storage-credentials[].config")?;
                Ok(Some(CredentialNode::Config))
            }
            (CredentialNode::Config, Key::Overlong) => {
                Err(ScanError::ValueTooLong("storage-credentials[].config key"))
            }
            (CredentialNode::Config, Key::Text(name)) => {
                let config = current.config.get_or_insert_with(BTreeMap::new);
                if config.contains_key(name) {
                    return Err(ScanError::DuplicateField("storage-credentials[].config"));
                }
                if config.len() >= MAX_CREDENTIAL_CONFIG_ENTRIES {
                    return Err(ScanError::TooManyEntries("storage-credentials[].config"));
                }
                self.pending_key = Some(name.to_owned());
                Ok(Some(CredentialNode::ConfigValue))
            }
            _ => Ok(None),
        }
    }

    fn element(
        &mut self,
        array: CredentialNode,
        index: u64,
    ) -> Result<Option<CredentialNode>, ScanError> {
        match array {
            CredentialNode::List if index >= MAX_STORAGE_CREDENTIALS as u64 => {
                Err(ScanError::TooManyEntries("storage-credentials"))
            }
            CredentialNode::List => Ok(Some(CredentialNode::Credential)),
            _ => Ok(None),
        }
    }

    fn scalar(&mut self, node: CredentialNode, value: Scalar<'_>) -> Result<(), ScanError> {
        match node {
            CredentialNode::Prefix => {
                let prefix = retained_string(value, "storage-credentials[].prefix")?;
                if let Some(current) = self.current.as_mut() {
                    current.prefix = prefix;
                }
            }
            CredentialNode::ConfigValue => {
                let value = retained_string(value, "storage-credentials[].config")?;
                if let (Some(key), Some(value), Some(config)) = (
                    self.pending_key.take(),
                    value,
                    self.current
                        .as_mut()
                        .and_then(|current| current.config.as_mut()),
                ) {
                    config.insert(key, value);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn close(&mut self, node: CredentialNode) -> Result<(), ScanError> {
        if node != CredentialNode::Credential {
            return Ok(());
        }
        let current = self.current.take().unwrap_or_default();
        let prefix = current
            .prefix
            .ok_or(ScanError::MissingField("storage-credentials[].prefix"))?;
        let config = current
            .config
            .ok_or(ScanError::MissingField("storage-credentials[].config"))?;
        self.credentials
            .push(RawStorageCredential { prefix, config });
        Ok(())
    }

    fn finish(self) -> StorageCredentials {
        if self.present {
            StorageCredentials::Present(self.credentials)
        } else {
            StorageCredentials::Absent
        }
    }
}

/// Minimal view of a `LoadCredentialsResponse`.
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct CredentialsView {
    credentials: StorageCredentials,
}

impl CredentialsView {
    pub(crate) fn credentials(&self) -> &StorageCredentials {
        &self.credentials
    }

    pub(crate) fn into_credentials(self) -> StorageCredentials {
        self.credentials
    }
}

impl fmt::Debug for CredentialsView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialsView")
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialsNode {
    Root,
    Credentials(CredentialNode),
}

/// Sink for the credentials-endpoint view.
#[derive(Default)]
pub(crate) struct CredentialsSink {
    seen_credentials: bool,
    collector: CredentialCollector,
}

impl CredentialsSink {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl ViewSink for CredentialsSink {
    type Node = CredentialsNode;
    type View = CredentialsView;

    fn retention_cap(&self) -> usize {
        CREDENTIAL_VALUE_LIMIT.max(CREDENTIAL_PREFIX_LIMIT)
    }

    fn root(&self) -> CredentialsNode {
        CredentialsNode::Root
    }

    fn open(&mut self, node: CredentialsNode, kind: ValueKind) -> Result<usize, ScanError> {
        match node {
            CredentialsNode::Root => {
                expect_kind(kind, &[ValueKind::Object], "response")?;
                Ok(0)
            }
            CredentialsNode::Credentials(node) => self.collector.open(node, kind),
        }
    }

    fn member(
        &mut self,
        object: CredentialsNode,
        key: Key<'_>,
    ) -> Result<Option<CredentialsNode>, ScanError> {
        match (object, key) {
            (CredentialsNode::Root, Key::Text("storage-credentials")) => {
                first(&mut self.seen_credentials, "storage-credentials")?;
                Ok(Some(CredentialsNode::Credentials(CredentialNode::List)))
            }
            (CredentialsNode::Root, _) => Ok(None),
            (CredentialsNode::Credentials(node), key) => Ok(self
                .collector
                .member(node, key)?
                .map(CredentialsNode::Credentials)),
        }
    }

    fn element(
        &mut self,
        array: CredentialsNode,
        index: u64,
    ) -> Result<Option<CredentialsNode>, ScanError> {
        match array {
            CredentialsNode::Root => Ok(None),
            CredentialsNode::Credentials(node) => Ok(self
                .collector
                .element(node, index)?
                .map(CredentialsNode::Credentials)),
        }
    }

    fn scalar(&mut self, node: CredentialsNode, value: Scalar<'_>) -> Result<(), ScanError> {
        match node {
            CredentialsNode::Root => Ok(()),
            CredentialsNode::Credentials(node) => self.collector.scalar(node, value),
        }
    }

    fn close(&mut self, node: CredentialsNode) -> Result<(), ScanError> {
        match node {
            CredentialsNode::Root => Ok(()),
            CredentialsNode::Credentials(node) => self.collector.close(node),
        }
    }

    fn finish(self) -> Result<CredentialsView, ScanError> {
        Ok(CredentialsView {
            credentials: self.collector.finish(),
        })
    }
}

// ---------------------------------------------------------------------------
// Backend acquisition (load table)
// ---------------------------------------------------------------------------

/// Minimal view of the backend's load-table acquisition: the table UUID it
/// must match and the storage credentials it installs.
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct BackendAcquisitionView {
    table_uuid: Option<String>,
    credentials: StorageCredentials,
}

impl BackendAcquisitionView {
    pub(crate) fn table_uuid(&self) -> Option<&str> {
        self.table_uuid.as_deref()
    }

    pub(crate) fn credentials(&self) -> &StorageCredentials {
        &self.credentials
    }

    pub(crate) fn into_parts(self) -> (Option<String>, StorageCredentials) {
        (self.table_uuid, self.credentials)
    }
}

impl fmt::Debug for BackendAcquisitionView {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackendAcquisitionView")
            .field("table_uuid_present", &self.table_uuid.is_some())
            .field("credentials", &self.credentials)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BackendAcquisitionNode {
    Root,
    Metadata,
    TableUuid,
    Credentials(CredentialNode),
}

/// Sink for the backend-acquisition view.
#[derive(Default)]
pub(crate) struct BackendAcquisitionSink {
    seen_metadata: bool,
    seen_uuid: bool,
    seen_credentials: bool,
    table_uuid: Option<String>,
    collector: CredentialCollector,
}

impl BackendAcquisitionSink {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl ViewSink for BackendAcquisitionSink {
    type Node = BackendAcquisitionNode;
    type View = BackendAcquisitionView;

    fn retention_cap(&self) -> usize {
        CREDENTIAL_VALUE_LIMIT.max(CREDENTIAL_PREFIX_LIMIT)
    }

    fn root(&self) -> BackendAcquisitionNode {
        BackendAcquisitionNode::Root
    }

    fn open(&mut self, node: BackendAcquisitionNode, kind: ValueKind) -> Result<usize, ScanError> {
        match node {
            BackendAcquisitionNode::Root => {
                expect_kind(kind, &[ValueKind::Object], "response")?;
                Ok(0)
            }
            BackendAcquisitionNode::Metadata => {
                expect_kind(kind, &[ValueKind::Object], "metadata")?;
                Ok(0)
            }
            BackendAcquisitionNode::TableUuid => {
                expect_kind(kind, &[ValueKind::String], "metadata.table-uuid")?;
                Ok(UUID_TEXT_LIMIT)
            }
            BackendAcquisitionNode::Credentials(node) => self.collector.open(node, kind),
        }
    }

    fn member(
        &mut self,
        object: BackendAcquisitionNode,
        key: Key<'_>,
    ) -> Result<Option<BackendAcquisitionNode>, ScanError> {
        match (object, key) {
            (BackendAcquisitionNode::Root, Key::Text("metadata")) => {
                first(&mut self.seen_metadata, "metadata")?;
                Ok(Some(BackendAcquisitionNode::Metadata))
            }
            (BackendAcquisitionNode::Root, Key::Text("storage-credentials")) => {
                first(&mut self.seen_credentials, "storage-credentials")?;
                Ok(Some(BackendAcquisitionNode::Credentials(
                    CredentialNode::List,
                )))
            }
            (BackendAcquisitionNode::Metadata, Key::Text("table-uuid")) => {
                first(&mut self.seen_uuid, "metadata.table-uuid")?;
                Ok(Some(BackendAcquisitionNode::TableUuid))
            }
            (BackendAcquisitionNode::Credentials(node), key) => Ok(self
                .collector
                .member(node, key)?
                .map(BackendAcquisitionNode::Credentials)),
            _ => Ok(None),
        }
    }

    fn element(
        &mut self,
        array: BackendAcquisitionNode,
        index: u64,
    ) -> Result<Option<BackendAcquisitionNode>, ScanError> {
        match array {
            BackendAcquisitionNode::Credentials(node) => Ok(self
                .collector
                .element(node, index)?
                .map(BackendAcquisitionNode::Credentials)),
            _ => Ok(None),
        }
    }

    fn scalar(&mut self, node: BackendAcquisitionNode, value: Scalar<'_>) -> Result<(), ScanError> {
        match node {
            BackendAcquisitionNode::TableUuid => {
                self.table_uuid = retained_string(value, "metadata.table-uuid")?;
                Ok(())
            }
            BackendAcquisitionNode::Credentials(node) => self.collector.scalar(node, value),
            _ => Ok(()),
        }
    }

    fn close(&mut self, node: BackendAcquisitionNode) -> Result<(), ScanError> {
        match node {
            BackendAcquisitionNode::Credentials(node) => self.collector.close(node),
            _ => Ok(()),
        }
    }

    fn finish(self) -> Result<BackendAcquisitionView, ScanError> {
        Ok(BackendAcquisitionView {
            table_uuid: self.table_uuid,
            credentials: self.collector.finish(),
        })
    }
}

// ---------------------------------------------------------------------------
// Mutation views
// ---------------------------------------------------------------------------

/// The decoded minimal view of a mutation's success response, by shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MutationView {
    Table(TableResponseView),
    Namespace(NamespaceView),
    View(ViewMutationView),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rest_protocol::json::{JsonScanner, scan_complete};

    const UUID: &str = "9c12d441-03fe-4693-9a96-a0705ddf69c1";

    fn table_request() -> TableViewRequest {
        TableViewRequest::new(
            Some(42),
            Some("main".to_owned()),
            vec![
                ("novarocks.ctas.operation-id".to_owned(), "op-7".len()),
                ("absent".to_owned(), 0),
            ],
        )
        .unwrap()
    }

    fn commit_response() -> String {
        format!(
            r#"{{"metadata-location":"s3://b/t/metadata/00002.json",
               "metadata":{{"format-version":2,"table-uuid":"{UUID}",
                 "location":"s3://b/t","schemas":[{{"type":"struct","fields":[]}}],
                 "snapshots":[{{"snapshot-id":41,"summary":{{"operation":"append"}}}},
                              {{"snapshot-id":42,"parent-snapshot-id":41}}],
                 "refs":{{"dev":{{"snapshot-id":41,"type":"branch"}},
                          "main":{{"snapshot-id":42,"type":"branch"}}}},
                 "properties":{{"novarocks.ctas.operation-id":"op-7","other":"x"}}}},
               "config":{{"k":"v"}}}}"#
        )
    }

    fn scan_table(document: &str) -> Result<TableResponseView, ScanError> {
        scan_complete(TableResponseSink::new(table_request()), document.as_bytes())
    }

    /// Every two-way split agrees with the single feed.
    fn assert_split_invariant<S: ViewSink>(make: impl Fn() -> S, document: &[u8])
    where
        S::View: PartialEq + fmt::Debug,
    {
        let whole = scan_complete(make(), document);
        for split in 0..=document.len() {
            let mut scanner = JsonScanner::new(make());
            let result = scanner
                .feed(&document[..split])
                .and_then(|()| scanner.feed(&document[split..]));
            let result = result.and_then(|()| scanner.finish());
            assert_eq!(result, whole, "split at {split}");
        }
    }

    #[test]
    fn table_view_keeps_only_requested_fields() {
        let view = scan_table(&commit_response()).unwrap();
        assert_eq!(
            view.metadata_location,
            LocationField::Text("s3://b/t/metadata/00002.json".to_owned())
        );
        assert_eq!(view.table_uuid.as_deref(), Some(UUID));
        assert_eq!(
            view.snapshot,
            Some(SnapshotCheck {
                snapshot_id: 42,
                present: true
            })
        );
        assert_eq!(
            view.target_ref,
            Some(RefCheck {
                name: "main".to_owned(),
                snapshot_id: Some(42)
            })
        );
        assert_eq!(
            view.properties,
            vec![
                (
                    "novarocks.ctas.operation-id".to_owned(),
                    PropertyValue::Text("op-7".to_owned())
                ),
                ("absent".to_owned(), PropertyValue::Absent),
            ]
        );
        assert_split_invariant(
            || TableResponseSink::new(table_request()),
            commit_response().as_bytes(),
        );
    }

    #[test]
    fn table_view_without_requests_skips_snapshots_refs_and_properties() {
        // Duplicates of fields the view does not use are not errors.
        let document = format!(
            r#"{{"metadata-location":"m","metadata":{{"table-uuid":"{UUID}",
               "snapshots":[],"snapshots":"skipped","refs":1,"refs":2,"properties":[]}}}}"#
        );
        let view = scan_complete(
            TableResponseSink::new(TableViewRequest::default()),
            document.as_bytes(),
        )
        .unwrap();
        assert_eq!(view.snapshot, None);
        assert_eq!(view.target_ref, None);
        assert!(view.properties.is_empty());
    }

    #[test]
    fn table_view_reports_absent_snapshot_and_moved_ref() {
        let document = format!(
            r#"{{"metadata-location":"m","metadata":{{"table-uuid":"{UUID}",
               "snapshots":[{{"snapshot-id":7}}],"refs":{{"main":{{"snapshot-id":43}}}}}}}}"#
        );
        let view = scan_table(&document).unwrap();
        assert_eq!(
            view.snapshot,
            Some(SnapshotCheck {
                snapshot_id: 42,
                present: false
            })
        );
        assert_eq!(view.target_ref.unwrap().snapshot_id, Some(43));
        let empty = scan_table(&format!(
            r#"{{"metadata":{{"table-uuid":"{UUID}","snapshots":null,"refs":null}}}}"#
        ))
        .unwrap();
        assert_eq!(empty.metadata_location, LocationField::Absent);
        assert!(!empty.snapshot.unwrap().present);
        assert_eq!(empty.target_ref.unwrap().snapshot_id, None);
    }

    #[test]
    fn stage_location_shapes_are_distinguished() {
        let scan = |location: &str| {
            let document = format!(r#"{{{location}"metadata":{{"table-uuid":"{UUID}"}}}}"#);
            scan_complete(
                TableResponseSink::new(TableViewRequest::default()),
                document.as_bytes(),
            )
            .unwrap()
            .metadata_location
        };
        assert_eq!(scan(""), LocationField::Absent);
        assert_eq!(scan(r#""metadata-location":null,"#), LocationField::Null);
        assert_eq!(
            scan(r#""metadata-location":"","#),
            LocationField::Text(String::new())
        );
        assert!(scan("").is_unset() && scan(r#""metadata-location":"","#).is_unset());
        assert!(!scan(r#""metadata-location":"m","#).is_unset());
    }

    #[test]
    fn duplicate_used_fields_are_refused() {
        let cases = [
            (
                r#"{"metadata-location":"a","metadata-location":"a","metadata":{}}"#.to_owned(),
                "metadata-location",
            ),
            (r#"{"metadata":{},"metadata":{}}"#.to_owned(), "metadata"),
            (
                format!(r#"{{"metadata":{{"table-uuid":"{UUID}","table-uuid":"{UUID}"}}}}"#),
                "metadata.table-uuid",
            ),
            (
                r#"{"metadata":{"snapshots":[],"snapshots":[]}}"#.to_owned(),
                "metadata.snapshots",
            ),
            (
                r#"{"metadata":{"snapshots":[{"snapshot-id":1,"snapshot-id":2}]}}"#.to_owned(),
                "metadata.snapshots[].snapshot-id",
            ),
            (
                r#"{"metadata":{"refs":{},"refs":{}}}"#.to_owned(),
                "metadata.refs",
            ),
            (
                r#"{"metadata":{"refs":{"main":{},"main":{}}}}"#.to_owned(),
                "metadata.refs.<target>",
            ),
            (
                r#"{"metadata":{"refs":{"main":{"snapshot-id":1,"snapshot-id":1}}}}"#.to_owned(),
                "metadata.refs.<target>.snapshot-id",
            ),
            (
                r#"{"metadata":{"properties":{},"properties":{}}}"#.to_owned(),
                "metadata.properties",
            ),
            (
                r#"{"metadata":{"properties":{"absent":"a","absent":"a"}}}"#.to_owned(),
                "metadata.properties",
            ),
        ];
        for (document, field) in cases {
            assert_eq!(
                scan_table(&document),
                Err(ScanError::DuplicateField(field)),
                "{document}"
            );
        }
        // Repeated snapshot elements are not duplicate fields.
        let document =
            r#"{"metadata":{"snapshots":[{"snapshot-id":1},{"snapshot-id":1}]}}"#.to_owned();
        assert!(scan_table(&document).is_ok());
        // An escaped spelling is the same key.
        let document = r#"{"metadata":{},"met\u0061data":{}}"#;
        assert_eq!(
            scan_table(document),
            Err(ScanError::DuplicateField("metadata"))
        );
    }

    #[test]
    fn wrong_types_and_values_are_refused() {
        let cases = [
            ("[]".to_owned(), ScanError::UnexpectedType("response")),
            (
                r#"{"metadata":"x"}"#.to_owned(),
                ScanError::UnexpectedType("metadata"),
            ),
            (
                r#"{"metadata-location":7}"#.to_owned(),
                ScanError::UnexpectedType("metadata-location"),
            ),
            (
                r#"{"metadata":{"table-uuid":null}}"#.to_owned(),
                ScanError::UnexpectedType("metadata.table-uuid"),
            ),
            (
                r#"{"metadata":{"snapshots":[{"snapshot-id":"42"}]}}"#.to_owned(),
                ScanError::UnexpectedType("metadata.snapshots[].snapshot-id"),
            ),
            (
                r#"{"metadata":{"snapshots":[{"snapshot-id":42.0}]}}"#.to_owned(),
                ScanError::InvalidValue("metadata.snapshots[].snapshot-id"),
            ),
            (
                r#"{"metadata":{"snapshots":[7]}}"#.to_owned(),
                ScanError::UnexpectedType("metadata.snapshots[]"),
            ),
            (
                r#"{"metadata":{"properties":{"absent":1}}}"#.to_owned(),
                ScanError::UnexpectedType("metadata.properties"),
            ),
            (
                format!(
                    r#"{{"metadata-location":"{}"}}"#,
                    "m".repeat(METADATA_LOCATION_LIMIT + 1)
                ),
                ScanError::ValueTooLong("metadata-location"),
            ),
            (
                format!(
                    r#"{{"metadata":{{"table-uuid":"{}"}}}}"#,
                    "u".repeat(UUID_TEXT_LIMIT + 1)
                ),
                ScanError::ValueTooLong("metadata.table-uuid"),
            ),
        ];
        for (document, error) in cases {
            assert_eq!(scan_table(&document), Err(error), "{document}");
        }
        let long = r#"{"metadata":{"properties":{"absent":"p"}}}"#;
        assert_eq!(
            scan_table(long).unwrap().properties[1].1,
            PropertyValue::Overlong
        );
    }

    #[test]
    fn view_requests_keep_key_and_ref_validation_without_property_quotas() {
        assert_eq!(
            TableViewRequest::new(None, Some("r".repeat(MAX_KEY_LEN + 1)), Vec::new()),
            Err(ViewRequestError::RefNameTooLong)
        );
        assert_eq!(
            TableViewRequest::new(None, None, vec![("k".repeat(MAX_KEY_LEN + 1), 0)]),
            Err(ViewRequestError::PropertyKeyTooLong)
        );
        assert_eq!(
            TableViewRequest::new(None, None, vec![("a".to_owned(), 0), ("a".to_owned(), 0)]),
            Err(ViewRequestError::DuplicatePropertyKey)
        );
        // More than the former 32-key quota; only requested keys are retained.
        let properties = (0..40)
            .map(|index| (format!("key-{index}"), "v".to_owned()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let request = TableViewRequest::new(
            None,
            None,
            properties
                .iter()
                .map(|(key, value)| (key.clone(), value.len()))
                .collect(),
        )
        .unwrap();
        let document = serde_json::json!({"metadata": {"properties": properties}}).to_string();
        let view = scan_complete(TableResponseSink::new(request), document.as_bytes()).unwrap();
        assert_eq!(view.properties.len(), 40);
        assert!(
            view.properties
                .iter()
                .all(|(_, value)| value == &PropertyValue::Text("v".into()))
        );

        for expected in [String::new(), "原文".repeat(32 * 1024)] {
            // The UTF8 input crosses the former 64KiB quota. Empty expectations
            // also distinguish a fully retained one-byte mismatch from equality.
            let request =
                TableViewRequest::new(None, None, vec![("payload".into(), expected.len())])
                    .unwrap();
            assert_eq!(
                TableResponseSink::new(request.clone()).retention_cap(),
                METADATA_LOCATION_LIMIT.max(expected.len() + 1)
            );
            for extra in [0, 1, 10_000] {
                let value = format!("{expected}{}", "x".repeat(extra));
                let document =
                    serde_json::json!({"metadata": {"properties": {"payload": value}}}).to_string();
                let mut scanner = JsonScanner::new(TableResponseSink::new(request.clone()));
                for chunk in document.as_bytes().chunks(997) {
                    scanner.feed(chunk).unwrap();
                }
                let view = scanner.finish().unwrap();
                let observed = &view.properties[0].1;
                if extra == 0 {
                    assert_eq!(observed, &PropertyValue::Text(expected.clone()));
                } else {
                    assert_eq!(observed, &PropertyValue::Overlong);
                }
            }
        }
    }

    /// Writes a commit response with `count` snapshots chunk by chunk, never
    /// holding more than one chunk, and returns the view and the scanner's
    /// retention at the end.
    fn scan_generated_commit(count: u64) -> (TableResponseView, usize) {
        const CHUNK: usize = 64 * 1024;
        let expected = 42;
        let mut scanner = JsonScanner::new(TableResponseSink::new(table_request()));
        let fixed = scanner.retained_bytes();
        let mut buffer = String::with_capacity(CHUNK + 1024);
        let flush = |buffer: &mut String, scanner: &mut JsonScanner<TableResponseSink>, force| {
            if force || buffer.len() >= CHUNK {
                scanner.feed(buffer.as_bytes()).unwrap();
                buffer.clear();
                assert_eq!(scanner.retained_bytes(), fixed);
            }
        };
        buffer.push_str(&format!(
            r#"{{"metadata-location":"s3://b/t/metadata/00002.json","metadata":{{"table-uuid":"{UUID}","snapshots":["#
        ));
        for index in 0..count {
            let id = 1_000 + index as i64;
            if index > 0 {
                buffer.push(',');
            }
            // The expected snapshot sits in the middle of the list.
            let id = if index == count / 2 { expected } else { id };
            buffer.push_str(&format!(
                r#"{{"snapshot-id":{id},"parent-snapshot-id":{},"timestamp-ms":1700000000000,"manifest-list":"s3://b/t/metadata/snap-{id}.avro","summary":{{"operation":"append","added-files":"1"}},"schema-id":0}}"#,
                id - 1
            ));
            flush(&mut buffer, &mut scanner, false);
        }
        buffer.push_str(
            r#"],"refs":{"main":{"snapshot-id":42,"type":"branch"}},"properties":{"novarocks.ctas.operation-id":"op-7"}}}"#,
        );
        flush(&mut buffer, &mut scanner, true);
        let retained = scanner.retained_bytes();
        (scanner.finish().unwrap(), retained)
    }

    #[test]
    fn snapshot_count_does_not_change_view_or_retention() {
        let (small, small_retained) = scan_generated_commit(1_000);
        let (large, large_retained) = scan_generated_commit(1_000_000);
        assert_eq!(small, large);
        assert_eq!(small_retained, large_retained);
        assert!(small.snapshot.unwrap().present);
        // Scratch is derived from this request, plus the fixed frame stack.
        assert!(
            small_retained
                <= TableResponseSink::new(table_request()).retention_cap()
                    + crate::rest_protocol::json::MAX_DEPTH * 64
        );
    }

    #[test]
    fn namespace_view() {
        let view = scan_complete(
            NamespaceSink::new(),
            br#"{"namespace":["a","b\u0063"],"properties":{"x":"y"}}"#,
        )
        .unwrap();
        assert_eq!(view.namespace, Some(vec!["a".to_owned(), "bc".to_owned()]));
        assert_eq!(
            scan_complete(NamespaceSink::new(), br#"{"properties":{}}"#).unwrap(),
            NamespaceView { namespace: None }
        );
        assert_eq!(
            scan_complete(NamespaceSink::new(), br#"{"namespace":[],"namespace":[]}"#),
            Err(ScanError::DuplicateField("namespace"))
        );
        assert_eq!(
            scan_complete(NamespaceSink::new(), br#"{"namespace":"a"}"#),
            Err(ScanError::UnexpectedType("namespace"))
        );
        assert_eq!(
            scan_complete(NamespaceSink::new(), br#"{"namespace":[1]}"#),
            Err(ScanError::UnexpectedType("namespace[]"))
        );
        let many = format!(
            r#"{{"namespace":[{}]}}"#,
            vec![r#""l""#; MAX_NAMESPACE_LEVELS + 1].join(",")
        );
        assert_eq!(
            scan_complete(NamespaceSink::new(), many.as_bytes()),
            Err(ScanError::TooManyEntries("namespace"))
        );
        assert_split_invariant(NamespaceSink::new, br#"{"namespace":["a","b"]}"#);
    }

    #[test]
    fn view_mutation_view() {
        let document = format!(
            r#"{{"metadata-location":"s3://v/m.json","metadata":{{"view-uuid":"{UUID}","versions":[]}},"config":{{}}}}"#
        );
        let view = scan_complete(ViewMutationSink::new(), document.as_bytes()).unwrap();
        assert_eq!(
            view.metadata_location,
            LocationField::Text("s3://v/m.json".to_owned())
        );
        assert_eq!(view.view_uuid.as_deref(), Some(UUID));
        assert_eq!(
            scan_complete(
                ViewMutationSink::new(),
                br#"{"metadata":{"view-uuid":"a","view-uuid":"a"}}"#
            ),
            Err(ScanError::DuplicateField("metadata.view-uuid"))
        );
        assert_split_invariant(ViewMutationSink::new, document.as_bytes());
    }

    const SECRET: &str = "SECRET-VALUE-do-not-print";

    fn credentials_document() -> String {
        format!(
            r#"{{"storage-credentials":[
                {{"prefix":"s3://b/t","config":{{"s3.access-key-id":"AKIA","s3.secret-access-key":"{SECRET}"}}}},
                {{"config":{{}},"prefix":"s3://b/u"}}]}}"#
        )
    }

    #[test]
    fn credentials_view_distinguishes_absent_and_empty() {
        let view =
            scan_complete(CredentialsSink::new(), credentials_document().as_bytes()).unwrap();
        let StorageCredentials::Present(credentials) = view.into_credentials() else {
            panic!("credentials must be present");
        };
        let parts: Vec<_> = credentials
            .into_iter()
            .map(RawStorageCredential::into_parts)
            .collect();
        assert_eq!(parts[0].0, "s3://b/t");
        assert_eq!(
            parts[0].1.get("s3.secret-access-key").map(String::as_str),
            Some(SECRET)
        );
        assert_eq!(parts[1], ("s3://b/u".to_owned(), BTreeMap::new()));

        for absent in [&br#"{}"#[..], br#"{"storage-credentials":null}"#] {
            let view = scan_complete(CredentialsSink::new(), absent).unwrap();
            assert_eq!(view.credentials(), &StorageCredentials::Absent);
        }
        let empty =
            scan_complete(CredentialsSink::new(), br#"{"storage-credentials":[]}"#).unwrap();
        assert_eq!(
            empty.credentials(),
            &StorageCredentials::Present(Vec::new())
        );
        assert_split_invariant(CredentialsSink::new, credentials_document().as_bytes());
    }

    #[test]
    fn credentials_are_validated_and_bounded() {
        let scan = |document: String| scan_complete(CredentialsSink::new(), document.as_bytes());
        let cases = [
            (
                r#"{"storage-credentials":[{"config":{}}]}"#.to_owned(),
                ScanError::MissingField("storage-credentials[].prefix"),
            ),
            (
                r#"{"storage-credentials":[{"prefix":"p"}]}"#.to_owned(),
                ScanError::MissingField("storage-credentials[].config"),
            ),
            (
                r#"{"storage-credentials":[{"prefix":"p","prefix":"p","config":{}}]}"#.to_owned(),
                ScanError::DuplicateField("storage-credentials[].prefix"),
            ),
            (
                r#"{"storage-credentials":[{"prefix":"p","config":{"k":"a","k":"b"}}]}"#.to_owned(),
                ScanError::DuplicateField("storage-credentials[].config"),
            ),
            (
                r#"{"storage-credentials":[{"prefix":"p","config":{"k":1}}]}"#.to_owned(),
                ScanError::UnexpectedType("storage-credentials[].config"),
            ),
            (
                r#"{"storage-credentials":{}}"#.to_owned(),
                ScanError::UnexpectedType("storage-credentials"),
            ),
            (
                r#"{"storage-credentials":[],"storage-credentials":[]}"#.to_owned(),
                ScanError::DuplicateField("storage-credentials"),
            ),
            (
                format!(
                    r#"{{"storage-credentials":[{{"prefix":"p","config":{{"k":"{}"}}}}]}}"#,
                    "v".repeat(CREDENTIAL_VALUE_LIMIT + 1)
                ),
                ScanError::ValueTooLong("storage-credentials[].config"),
            ),
            (
                format!(
                    r#"{{"storage-credentials":[{{"prefix":"p","config":{{"{}":"v"}}}}]}}"#,
                    "k".repeat(MAX_KEY_LEN + 1)
                ),
                ScanError::ValueTooLong("storage-credentials[].config key"),
            ),
            (
                format!(
                    r#"{{"storage-credentials":[{}]}}"#,
                    vec![r#"{"prefix":"p","config":{}}"#; MAX_STORAGE_CREDENTIALS + 1].join(",")
                ),
                ScanError::TooManyEntries("storage-credentials"),
            ),
            (
                format!(
                    r#"{{"storage-credentials":[{{"prefix":"p","config":{{{}}}}}]}}"#,
                    (0..=MAX_CREDENTIAL_CONFIG_ENTRIES)
                        .map(|i| format!(r#""k{i}":"v""#))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                ScanError::TooManyEntries("storage-credentials[].config"),
            ),
        ];
        for (document, error) in cases {
            assert_eq!(scan(document.clone()).map(|_| ()), Err(error), "{document}");
        }
    }

    #[test]
    fn credential_debug_prints_key_names_only() {
        let view =
            scan_complete(CredentialsSink::new(), credentials_document().as_bytes()).unwrap();
        let debug = format!("{view:?}");
        assert!(debug.contains("s3.secret-access-key"));
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains("AKIA"));
        assert!(!debug.contains("s3://b/t"));

        let document = format!(
            r#"{{"metadata":{{"table-uuid":"{UUID}"}},{}"#,
            &credentials_document()[1..]
        );
        let view = scan_complete(BackendAcquisitionSink::new(), document.as_bytes()).unwrap();
        let debug = format!("{view:?}");
        assert!(!debug.contains(SECRET) && !debug.contains(UUID));
        assert_eq!(view.table_uuid(), Some(UUID));
        assert_eq!(view.credentials().len(), 2);
    }

    #[test]
    fn backend_acquisition_view() {
        let document = format!(
            r#"{{"metadata-location":"m","metadata":{{"table-uuid":"{UUID}","snapshots":[]}},"config":{{}},
               "storage-credentials":[{{"prefix":"s3://b","config":{{"k":"v"}}}}]}}"#
        );
        let view = scan_complete(BackendAcquisitionSink::new(), document.as_bytes()).unwrap();
        let (uuid, credentials) = view.into_parts();
        assert_eq!(uuid.as_deref(), Some(UUID));
        assert_eq!(credentials.len(), 1);
        let absent = scan_complete(
            BackendAcquisitionSink::new(),
            format!(r#"{{"metadata":{{"table-uuid":"{UUID}"}}}}"#).as_bytes(),
        )
        .unwrap();
        assert_eq!(absent.credentials(), &StorageCredentials::Absent);
        assert_eq!(
            scan_complete(
                BackendAcquisitionSink::new(),
                br#"{"metadata":{"table-uuid":"a","table-uuid":"b"}}"#
            ),
            Err(ScanError::DuplicateField("metadata.table-uuid"))
        );
        assert_split_invariant(BackendAcquisitionSink::new, document.as_bytes());
    }
}

#[cfg(test)]
mod metadata_log_tests {
    use super::*;
    use crate::catalog::transaction::MetadataPredecessorObservation;
    use crate::rest_protocol::evidence::{
        AttributionExpectation, BodyRead, DispatchStage, RestEvidence, classify,
    };
    use crate::rest_protocol::json::scan_complete;

    fn view(metadata_log: Option<serde_json::Value>) -> TableResponseView {
        let mut response = serde_json::json!({"metadata-location":"s3://warehouse/metadata/response.json", "metadata":{"table-uuid":uuid::Uuid::nil()}});
        if let Some(log) = metadata_log {
            response["metadata"]["metadata-log"] = log;
        }
        scan_complete(
            TableResponseSink::new(TableViewRequest::default()),
            &serde_json::to_vec(&response).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn response_log_retains_only_its_last_predecessor_as_a_non_authoritative_clue() {
        let view = view(Some(serde_json::json!([
            {"timestamp-ms":1,"metadata-file":"s3://warehouse/metadata/planning-base.json"},
            {"timestamp-ms":2,"metadata-file":"s3://warehouse/metadata/actual-predecessor.json"}
        ])));
        assert_eq!(
            view.metadata_predecessor,
            MetadataPredecessorObservation::Candidate {
                location: "s3://warehouse/metadata/actual-predecessor.json".into(),
                timestamp_ms: 2,
            }
        );
        let expect = AttributionExpectation::table_commit(uuid::Uuid::nil(), None).unwrap();
        let evidence = classify(
            &DispatchStage::Status {
                code: 200,
                header_names: vec![],
            },
            BodyRead::Complete(MutationView::Table(view)),
            &expect,
        );
        let RestEvidence::Applied { receipt } = evidence else {
            panic!("valid table success must remain Applied")
        };
        assert!(matches!(
            receipt.metadata_predecessor,
            MetadataPredecessorObservation::Candidate {
                timestamp_ms: 2,
                ..
            }
        ));
        assert!(!format!("{:?}", receipt.metadata_predecessor).contains("actual-predecessor"));
    }
    #[test]
    fn missing_or_unusable_diagnostics_cannot_downgrade_decisive_table_evidence() {
        assert_eq!(
            view(None).metadata_predecessor,
            MetadataPredecessorObservation::NotReported
        );
        for log in [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!([{"metadata-file":"valid", "timestamp-ms":"wrong-type"}]),
            serde_json::json!([{"metadata-file":"x".repeat(METADATA_LOCATION_LIMIT+1), "timestamp-ms":1}]),
        ] {
            let view = view(Some(log));
            assert_eq!(
                view.metadata_predecessor,
                MetadataPredecessorObservation::Unusable
            );
            let expect = AttributionExpectation::table_commit(uuid::Uuid::nil(), None).unwrap();
            assert!(matches!(
                classify(
                    &DispatchStage::Status {
                        code: 200,
                        header_names: vec![]
                    },
                    BodyRead::Complete(MutationView::Table(view)),
                    &expect
                ),
                RestEvidence::Applied { .. }
            ));
        }
    }
}
