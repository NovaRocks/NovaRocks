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

//! Inline document vocabulary. Applications own payloads; providers own storage.

use super::document::invalid;
use super::{ConnectorDocumentFormat, ConnectorManagedObjectMarker};
use crate::connector::{
    ConnectorCommittedVersion, ConnectorError, ConnectorProviderBindingKey, ConnectorTableIdentity,
    ConnectorTableObjectId,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const KEYED_DOCUMENT_VERSION_BYTES: usize = 64 * 1024;
pub const KEYED_DOCUMENT_PRIVATE_BYTES: usize = 256 * 1024;
pub const KEYED_DOCUMENT_RECORD_BYTES: usize = 4 * 1024;

pub fn validate_document_token(value: &str) -> Result<(), ConnectorError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit()
        || !bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(b))
    {
        return Err(invalid(format!("invalid document token: {value:?}")));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DocumentIdentity([u8; 32]);
impl DocumentIdentity {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
    pub fn parse_hex(value: &str) -> Result<Self, ConnectorError> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid(
                "document identity must be 64 lowercase hexadecimal digits",
            ));
        }
        let mut bytes = [0; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[2 * i..2 * i + 2], 16)
                .map_err(|_| invalid("invalid document identity"))?;
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorDocumentVersion {
    owner: String,
    name: String,
    format: ConnectorDocumentFormat,
    references: BTreeMap<String, DocumentIdentity>,
    content: Bytes,
}
impl ConnectorDocumentVersion {
    pub fn try_new(
        owner: impl Into<String>,
        name: impl Into<String>,
        format: ConnectorDocumentFormat,
        references: Vec<(String, DocumentIdentity)>,
        content: Bytes,
    ) -> Result<Self, ConnectorError> {
        let owner = owner.into();
        let name = name.into();
        validate_document_token(&owner)?;
        validate_document_token(&name)?;
        validate_format(&format)?;
        let mut sorted = BTreeMap::new();
        for (relationship, target) in references {
            validate_document_token(&relationship)?;
            if sorted.insert(relationship, target).is_some() {
                return Err(invalid("duplicate document relationship"));
            }
        }
        Ok(Self {
            owner,
            name,
            format,
            references: sorted,
            content,
        })
    }
    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn format(&self) -> &ConnectorDocumentFormat {
        &self.format
    }
    pub fn references(&self) -> &BTreeMap<String, DocumentIdentity> {
        &self.references
    }
    pub fn content(&self) -> &Bytes {
        &self.content
    }
    pub fn identity(&self) -> DocumentIdentity {
        let mut hash = Sha256::new();
        hash.update(b"novarocks-document-identity/v2\0");
        hash_value_header(&mut hash, &self.owner, &self.name, &self.format);
        hash.update((self.references.len() as u32).to_be_bytes());
        for (relationship, target) in &self.references {
            hash_lp32(&mut hash, relationship.as_bytes());
            hash.update(target.to_bytes());
        }
        hash_content(&mut hash, &self.content);
        DocumentIdentity(hash.finalize().into())
    }
}

fn validate_format(format: &ConnectorDocumentFormat) -> Result<(), ConnectorError> {
    validate_document_token(format.owner())?;
    validate_document_token(format.name())
}
fn hash_lp32(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u32).to_be_bytes());
    hash.update(bytes);
}
fn hash_value_header(hash: &mut Sha256, owner: &str, name: &str, format: &ConnectorDocumentFormat) {
    hash_lp32(hash, owner.as_bytes());
    hash_lp32(hash, name.as_bytes());
    hash_lp32(hash, format.owner().as_bytes());
    hash_lp32(hash, format.name().as_bytes());
    hash.update(format.version().to_be_bytes());
}
fn hash_content(hash: &mut Sha256, content: &[u8]) {
    hash.update((content.len() as u64).to_be_bytes());
    hash.update(content);
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorSlotName {
    pub owner: String,
    pub name: String,
}
impl ConnectorSlotName {
    pub fn try_new(
        owner: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, ConnectorError> {
        let value = Self {
            owner: owner.into(),
            name: name.into(),
        };
        value.validate()?;
        Ok(value)
    }
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_document_token(&self.owner)?;
        validate_document_token(&self.name)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorSlotValue {
    pub format: ConnectorDocumentFormat,
    pub content: Bytes,
}
impl ConnectorSlotValue {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_format(&self.format)
    }
    pub fn digest(&self, name: &ConnectorSlotName, writer: Option<i64>) -> DocumentIdentity {
        let mut hash = Sha256::new();
        hash.update(b"novarocks-document-slot/v2\0");
        hash_value_header(&mut hash, &name.owner, &name.name, &self.format);
        hash.update([u8::from(writer.is_some())]);
        if let Some(writer) = writer {
            hash.update(writer.to_be_bytes());
        }
        hash_content(&mut hash, &self.content);
        DocumentIdentity(hash.finalize().into())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorObservedSlot {
    pub value: ConnectorSlotValue,
    pub writer: Option<i64>,
}
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorMarkerKey {
    pub owner: String,
    pub name: String,
    pub binding: String,
}
impl ConnectorMarkerKey {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_document_token(&self.owner)?;
        validate_document_token(&self.name)?;
        validate_document_token(&self.binding)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMarkerValue {
    pub format: ConnectorDocumentFormat,
    pub content: Bytes,
}
impl ConnectorMarkerValue {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_format(&self.format)
    }
    pub fn digest(&self, key: &ConnectorMarkerKey) -> DocumentIdentity {
        let mut hash = Sha256::new();
        hash.update(b"novarocks-document-marker/v2\0");
        hash_value_header(&mut hash, &key.owner, &key.name, &self.format);
        hash_lp32(&mut hash, key.binding.as_bytes());
        hash_content(&mut hash, &self.content);
        DocumentIdentity(hash.finalize().into())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorSnapshotRecord {
    pub owner: String,
    pub name: String,
    pub binding: Option<DocumentIdentity>,
    pub payload: Bytes,
}
impl ConnectorSnapshotRecord {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_document_token(&self.owner)?;
        validate_document_token(&self.name)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyedCreate {
    pub versions: Vec<ConnectorDocumentVersion>,
    pub slots: BTreeMap<ConnectorSlotName, ConnectorSlotValue>,
    pub roots: BTreeMap<String, DocumentIdentity>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyedMarkerWrite {
    pub markers: BTreeMap<ConnectorMarkerKey, ConnectorMarkerValue>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyedPublication {
    pub records: Vec<ConnectorSnapshotRecord>,
    pub versions: Vec<ConnectorDocumentVersion>,
    pub roots: BTreeMap<String, DocumentIdentity>,
    pub markers_set: BTreeMap<ConnectorMarkerKey, ConnectorMarkerValue>,
    pub markers_remove: BTreeSet<ConnectorMarkerKey>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct KeyedSlotChange {
    pub slots: BTreeMap<ConnectorSlotName, ConnectorSlotValue>,
    pub markers_remove: BTreeSet<ConnectorMarkerKey>,
}

fn validate_slots(
    slots: &BTreeMap<ConnectorSlotName, ConnectorSlotValue>,
) -> Result<(), ConnectorError> {
    for (name, value) in slots {
        name.validate()?;
        value.validate()?;
    }
    Ok(())
}
fn validate_markers(
    markers: &BTreeMap<ConnectorMarkerKey, ConnectorMarkerValue>,
) -> Result<(), ConnectorError> {
    for (key, value) in markers {
        key.validate()?;
        value.validate()?;
    }
    Ok(())
}
fn validate_versions(versions: &[ConnectorDocumentVersion]) -> Result<(), ConnectorError> {
    let mut seen = BTreeSet::new();
    for version in versions {
        if !seen.insert(version.identity()) {
            return Err(invalid("duplicate document version"));
        }
    }
    Ok(())
}
impl KeyedCreate {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_versions(&self.versions)?;
        validate_slots(&self.slots)?;
        for owner in self.roots.keys() {
            validate_document_token(owner)?;
        }
        Ok(())
    }
}
impl KeyedMarkerWrite {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.markers.is_empty() {
            return Err(invalid("marker write must contain a marker"));
        }
        validate_markers(&self.markers)
    }
}
impl KeyedSlotChange {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        if self.slots.is_empty() {
            return Err(invalid("slot change must contain a slot"));
        }
        validate_slots(&self.slots)?;
        for marker in &self.markers_remove {
            marker.validate()?;
        }
        Ok(())
    }
}
impl KeyedPublication {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        validate_versions(&self.versions)?;
        validate_markers(&self.markers_set)?;
        let mut names = BTreeSet::new();
        for record in &self.records {
            record.validate()?;
            if !names.insert((&record.owner, &record.name)) {
                return Err(invalid("duplicate snapshot record"));
            }
        }
        for owner in self.roots.keys() {
            validate_document_token(owner)?;
        }
        for key in &self.markers_remove {
            key.validate()?;
            if self.markers_set.contains_key(key) {
                return Err(invalid(
                    "marker cannot be set and removed in one publication",
                ));
            }
        }
        Ok(())
    }
}

/// Provider-owned representation of one full load. The payload must retain the
/// exact loaded metadata; expiry must not reload or substitute a newer base.
#[derive(Clone, Eq, PartialEq)]
pub struct FullObservationToken {
    owner: ConnectorProviderBindingKey,
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    payload: Bytes,
    sealed: bool,
}
impl std::fmt::Debug for FullObservationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullObservationToken")
            .field("table", &self.table)
            .field("metadata_version", &self.metadata_version)
            .finish_non_exhaustive()
    }
}
impl FullObservationToken {
    pub fn try_new(
        owner: ConnectorProviderBindingKey,
        table: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        metadata_version: ConnectorCommittedVersion,
        payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        super::observation::validate_table(&table)?;
        metadata_version.validate()?;
        if table.instance_id != owner.instance_id || payload.is_empty() {
            return Err(invalid(
                "full document observation owner or payload is invalid",
            ));
        }
        Ok(Self {
            owner,
            table,
            object_id,
            metadata_version,
            payload,
            sealed: false,
        })
    }
    pub(crate) fn seal(&mut self) {
        self.sealed = true;
    }
    pub fn validate_sealed(&self) -> Result<(), ConnectorError> {
        if !self.sealed {
            return Err(invalid(
                "full observation was not returned by its retained document lease",
            ));
        }
        Ok(())
    }
    pub fn owner(&self) -> &ConnectorProviderBindingKey {
        &self.owner
    }
    pub fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }
    pub fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }
    pub fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedExpiry {
    pub observation: FullObservationToken,
    pub remove_snapshots: BTreeSet<i64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotFileTotals {
    pub data_files: Option<u64>,
    pub delete_files: Option<u64>,
}
impl SnapshotFileTotals {
    pub fn is_empty(self) -> bool {
        self.data_files == Some(0) && self.delete_files == Some(0)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedDocumentRoot {
    pub identity: DocumentIdentity,
    pub dangling: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedDocumentView {
    pub owner: ConnectorProviderBindingKey,
    pub table: ConnectorTableIdentity,
    pub object_id: ConnectorTableObjectId,
    pub metadata_version: ConnectorCommittedVersion,
    pub current_snapshot: Option<i64>,
    pub current_snapshot_file_totals: SnapshotFileTotals,
    pub slots: BTreeMap<ConnectorSlotName, ConnectorObservedSlot>,
    pub markers: BTreeMap<ConnectorMarkerKey, ConnectorMarkerValue>,
    pub versions: BTreeMap<DocumentIdentity, ConnectorDocumentVersion>,
    pub roots: BTreeMap<String, KeyedDocumentRoot>,
    pub current_records: Vec<ConnectorSnapshotRecord>,
    pub management: Option<ConnectorManagedObjectMarker>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedSnapshotObservation {
    pub snapshot_id: i64,
    pub parent: Option<i64>,
    pub records: Vec<ConnectorSnapshotRecord>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullKeyedDocumentView {
    pub current: KeyedDocumentView,
    pub snapshots: Vec<KeyedSnapshotObservation>,
    pub refs: BTreeMap<String, i64>,
    pub diagnostics: Vec<String>,
    pub token: FullObservationToken,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnknownCommitIdentity {
    SlotWriter {
        slot: ConnectorSlotName,
    },
    RecordBytes {
        owner: String,
        name: String,
        bytes: Bytes,
    },
    None,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnknownCommitEvidence {
    pub asserted_parent: Option<i64>,
    pub preselected_snapshot: i64,
    pub identity: UnknownCommitIdentity,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitProof {
    Main,
    SlotWriter,
    Snapshot,
    ParentPointer,
    CarriedRecord,
    NegativeProof,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitConclusion {
    Committed(CommitProof),
    NotCommitted(CommitProof),
    InvalidatedUndetermined,
    StillUnknown,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyedDocumentLimits {
    pub version_value_bytes: usize,
    pub private_property_bytes: usize,
    pub record_bytes: usize,
}
impl Default for KeyedDocumentLimits {
    fn default() -> Self {
        Self {
            version_value_bytes: KEYED_DOCUMENT_VERSION_BYTES,
            private_property_bytes: KEYED_DOCUMENT_PRIVATE_BYTES,
            record_bytes: KEYED_DOCUMENT_RECORD_BYTES,
        }
    }
}

/// The exact admission remains attached to every catalog mutation.
#[expect(
    clippy::large_enum_variant,
    reason = "The public keyed-mutation union carries exact owned intents by value; boxing a payload would change the shared SPI contract"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyedMutation {
    Markers(KeyedMarkerWrite),
    SlotChange(KeyedSlotChange),
    Expiry(KeyedExpiry),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedMutationIntent {
    pub admission: super::ConnectorDocumentManagementAdmission,
    pub expected_metadata: ConnectorCommittedVersion,
    pub expected_snapshot: Option<i64>,
    pub mutation: KeyedMutation,
}
impl KeyedMutationIntent {
    pub fn validate_for(
        &self,
        owner: &ConnectorProviderBindingKey,
        operation_id: crate::connector::ConnectorMutationOperationId,
    ) -> Result<(), ConnectorError> {
        self.admission.validate()?;
        self.expected_metadata.validate()?;
        if self.admission.owner() != owner
            || self.admission.operation_id() != operation_id
            || self.admission.operation()
                != super::ConnectorDocumentManagementOperation::SingleTargetUpdate
        {
            return Err(invalid(
                "keyed mutation does not match its exact management admission",
            ));
        }
        match &self.mutation {
            KeyedMutation::Markers(write) => write.validate(),
            KeyedMutation::SlotChange(change) => change.validate(),
            KeyedMutation::Expiry(expiry) => {
                expiry.observation.validate_sealed()?;
                if expiry.observation.owner() != owner
                    || expiry.observation.table() != self.admission.target()
                    || Some(expiry.observation.object_id()) != self.admission.expected_object_id()
                    || expiry.observation.metadata_version() != &self.expected_metadata
                {
                    return Err(invalid("expiry does not match its full observation"));
                }
                Ok(())
            }
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedCreateIntent {
    pub target: crate::connector::ConnectorPreparedCreateDocumentTarget,
    pub admission: super::ConnectorDocumentManagementAdmission,
    pub documents: KeyedCreate,
}
impl KeyedCreateIntent {
    pub fn validate_for_handle(
        &self,
        handle: &crate::connector::ConnectorStagedTableHandle,
    ) -> Result<(), ConnectorError> {
        self.admission.validate()?;
        self.documents.validate()?;
        if handle.document_target() != Some(&self.target)
            || self.admission.operation() != super::ConnectorDocumentManagementOperation::Create
            || self.admission.target() != self.target.target()
            || self.admission.owner() != self.target.owner()
        {
            return Err(invalid(
                "keyed create does not match its exact staged target",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyedPublicationIntent {
    pub declaration: super::ConnectorDocumentPublicationDeclaration,
    pub documents: KeyedPublication,
}
impl KeyedPublicationIntent {
    pub fn validate_for(
        &self,
        declaration: &super::ConnectorDocumentPublicationDeclaration,
    ) -> Result<(), ConnectorError> {
        self.documents.validate()?;
        if &self.declaration != declaration {
            return Err(invalid(
                "keyed publication does not match its write declaration",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct KeyedDocumentObservationRequest {
    pub owner: ConnectorProviderBindingKey,
    pub catalog_handle: crate::connector::CatalogHandle,
    pub target: ConnectorTableIdentity,
    pub object_id: ConnectorTableObjectId,
    pub context: crate::connector::ConnectorRequestContext,
}
impl KeyedDocumentObservationRequest {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        super::observation::validate_table(&self.target)?;
        if self.owner.instance_id != self.target.instance_id
            || self.catalog_handle.catalog_name() != &self.owner.instance_id
        {
            return Err(invalid(
                "keyed observation does not match its catalog owner",
            ));
        }
        Ok(())
    }
}

/// A legacy table is distinguishable from damaged data and may still be dropped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyedDocumentReadError {
    LegacyFormat { key: String },
    Invalid(ConnectorError),
}
impl From<ConnectorError> for KeyedDocumentReadError {
    fn from(error: ConnectorError) -> Self {
        Self::Invalid(error)
    }
}
impl std::fmt::Display for KeyedDocumentReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LegacyFormat { key } => {
                write!(f, "LegacyFormat: {key}; only DROP TABLE is supported")
            }
            Self::Invalid(error) => write!(f, "{error}"),
        }
    }
}
impl std::error::Error for KeyedDocumentReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::LegacyFormat { .. } => None,
        }
    }
}

/// The keyed-document codec authors this format rejection. It is not a
/// compiled-plan capability or a synthetic Connector operation failure.
pub const LEGACY_KEYED_DOCUMENT_FORMAT: std::num::NonZeroU16 =
    std::num::NonZeroU16::new(916).unwrap();

impl novarocks_failure_contract::FailureProjection for KeyedDocumentReadError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        match self {
            Self::Invalid(error) => {
                novarocks_failure_contract::FailureProjection::failure_fact(error)
            }
            Self::LegacyFormat { .. } => FailureFact::new(
                FailureClass::Unsupported,
                FailureReason::new(domain::METADATA, LEGACY_KEYED_DOCUMENT_FORMAT),
            ),
        }
    }
}

impl KeyedDocumentView {
    pub fn validate_for(
        &self,
        request: &KeyedDocumentObservationRequest,
    ) -> Result<(), ConnectorError> {
        self.metadata_version.validate()?;
        if self.owner != request.owner
            || self.table != request.target
            || self.object_id != request.object_id
        {
            return Err(invalid(
                "keyed observation returned a different table or control generation",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn version(refs: Vec<(String, DocumentIdentity)>) -> ConnectorDocumentVersion {
        ConnectorDocumentVersion::try_new(
            "novarocks.mv",
            "definition",
            ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 1).unwrap(),
            refs,
            Bytes::from_static(b"abc"),
        )
        .unwrap()
    }
    #[test]
    fn identity_matches_independent_length_prefixed_golden() {
        assert_eq!(
            version(vec![]).identity().to_hex(),
            "b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e"
        );
        assert_eq!(
            DocumentIdentity::parse_hex(
                "b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e"
            )
            .unwrap(),
            version(vec![]).identity()
        );
    }
    #[test]
    fn identity_covers_format_references_names_and_content() {
        let base = version(vec![]);
        let reference = version(vec![(
            "definition".into(),
            DocumentIdentity::from_bytes([1; 32]),
        )]);
        assert_ne!(base.identity(), reference.identity());
        let make = |owner, name, format, content| {
            ConnectorDocumentVersion::try_new(owner, name, format, vec![], content)
                .unwrap()
                .identity()
        };
        assert_ne!(
            base.identity(),
            make(
                "another",
                "definition",
                base.format().clone(),
                base.content().clone()
            )
        );
        assert_ne!(
            base.identity(),
            make(
                "novarocks.mv",
                "another",
                base.format().clone(),
                base.content().clone()
            )
        );
        assert_ne!(
            base.identity(),
            make(
                "novarocks.mv",
                "definition",
                ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 2).unwrap(),
                base.content().clone()
            )
        );
        assert_ne!(
            base.identity(),
            make(
                "novarocks.mv",
                "definition",
                base.format().clone(),
                Bytes::from_static(b"abcd")
            )
        );
    }
    #[test]
    fn references_are_sorted_and_relationships_are_unique() {
        let a = ("a".into(), DocumentIdentity::from_bytes([1; 32]));
        let b = ("b".into(), DocumentIdentity::from_bytes([2; 32]));
        assert_eq!(
            version(vec![a.clone(), b.clone()]),
            version(vec![b, a.clone()])
        );
        assert!(
            ConnectorDocumentVersion::try_new(
                "novarocks.mv",
                "definition",
                version(vec![]).format().clone(),
                vec![a.clone(), a],
                Bytes::new()
            )
            .is_err()
        );
    }
    #[test]
    fn key_tokens_follow_the_frozen_grammar() {
        for valid in ["a", "0", "novarocks.mv", "a_b-c.d"] {
            validate_document_token(valid).unwrap();
        }
        for invalid in ["", "A", "a~b", "a/b", "a b", "-a", "é"] {
            assert!(validate_document_token(invalid).is_err());
        }
        assert!(validate_document_token(&"a".repeat(65)).is_err());
        assert!(DocumentIdentity::parse_hex(&"A".repeat(64)).is_err());
    }
    #[test]
    fn slot_writer_and_marker_binding_are_integrity_facts() {
        let value = ConnectorSlotValue {
            format: version(vec![]).format().clone(),
            content: Bytes::from_static(b"value"),
        };
        let name = ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap();
        assert_ne!(value.digest(&name, None), value.digest(&name, Some(1)));
        assert_ne!(value.digest(&name, Some(1)), value.digest(&name, Some(2)));
        let marker = ConnectorMarkerValue {
            format: value.format.clone(),
            content: value.content.clone(),
        };
        let key = ConnectorMarkerKey {
            owner: name.owner,
            name: name.name,
            binding: "first".into(),
        };
        let mut other = key.clone();
        other.binding = "second".into();
        assert_ne!(marker.digest(&key), marker.digest(&other));
    }
    #[test]
    fn slot_change_accepts_same_value_and_has_no_caller_writer() {
        let mut change = KeyedSlotChange::default();
        assert!(change.validate().is_err());
        change.slots.insert(
            ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap(),
            ConnectorSlotValue {
                format: version(vec![]).format().clone(),
                content: Bytes::from_static(b"same"),
            },
        );
        change.validate().unwrap();
        change.clone().validate().unwrap();
        assert!(KeyedMarkerWrite::default().validate().is_err());
    }
    #[test]
    fn empty_snapshot_totals_must_be_known() {
        assert!(!SnapshotFileTotals::default().is_empty());
        assert!(
            !SnapshotFileTotals {
                data_files: Some(0),
                delete_files: None
            }
            .is_empty()
        );
        assert!(
            SnapshotFileTotals {
                data_files: Some(0),
                delete_files: Some(0)
            }
            .is_empty()
        );
    }

    #[test]
    fn invalid_keyed_read_delegates_the_actual_connector_fact() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        let connector = ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::Cancelled,
        );
        let original = std::error::Error::source(&connector)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>())
            .unwrap()
            .clone();
        let expected = connector.failure_fact();
        let keyed = KeyedDocumentReadError::Invalid(connector);
        assert_eq!(keyed.failure_fact(), expected);
        let actual = std::error::Error::source(&keyed)
            .unwrap()
            .downcast_ref::<ConnectorError>()
            .unwrap();
        assert_eq!(
            std::error::Error::source(actual)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
            Some(&original),
        );
        assert_eq!(actual.failure_fact(), original.failure_fact());
    }

    #[test]
    fn legacy_keyed_format_authors_its_own_unsupported_metadata_fact() {
        use novarocks_failure_contract::{
            FailureClass, FailureProjection, SharedFailureCause, domain,
        };
        let legacy = KeyedDocumentReadError::LegacyFormat {
            key: "original-legacy-key".into(),
        };
        let fact = legacy.failure_fact();
        assert_eq!(fact.class(), FailureClass::Unsupported);
        assert_eq!(fact.reason().domain(), domain::METADATA.get());
        assert_eq!(fact.reason().code(), LEGACY_KEYED_DOCUMENT_FORMAT.get());
        let cause = SharedFailureCause::new(legacy.clone());
        assert_eq!(cause.failure_fact(), fact);
        assert_eq!(
            cause.downcast_ref::<KeyedDocumentReadError>(),
            Some(&legacy)
        );
        assert_eq!(cause.clone(), cause);
        assert!(cause.detail().contains("original-legacy-key"));
        assert!(cause.downcast_ref::<ConnectorError>().is_none());
    }
}
