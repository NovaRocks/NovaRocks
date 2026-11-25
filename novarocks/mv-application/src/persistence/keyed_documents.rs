// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! MV-owned keyed document mapping. Legacy documents remain only for the
//! staged FE migration; this path never converts a keyed record into legacy P.

use super::{
    binding_envelope::inspect_binding, codec::keyed::*, codec::*, documents::MvDocumentError,
    identity::DocumentRevision,
};
use crate::management::{DeploymentOwner, ManagedMvTarget, ProcessIncarnation};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::BTreeMap;

pub const MV_DOCUMENT_OWNER: &str = "novarocks.mv";
pub const CONFIGURATION_SLOT_OWNER: &str = MV_DOCUMENT_OWNER;
pub const MANAGEMENT_SLOT_OWNER: &str = "novarocks.storage";
pub const MANAGEMENT_SLOT_NAME: &str = "management";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvKeyedVersions {
    pub definition: ConnectorDocumentVersion,
    pub interpretation: ConnectorDocumentVersion,
    pub mapping: ConnectorDocumentVersion,
    pub binding: ConnectorDocumentVersion,
    pub binding_document: BindingDocument,
    pub mapping_document: StorageMappingDocument,
}
impl MvKeyedVersions {
    pub fn definition_revision(&self) -> DocumentRevision {
        revision(self.definition.identity())
    }
    pub fn interpretation_revision(&self) -> DocumentRevision {
        revision(self.interpretation.identity())
    }
    pub fn versions(&self) -> Vec<ConnectorDocumentVersion> {
        vec![
            self.definition.clone(),
            self.interpretation.clone(),
            self.mapping.clone(),
            self.binding.clone(),
        ]
    }
}

fn contract<T>(message: impl Into<String>) -> Result<T, MvDocumentError> {
    Err(MvDocumentError::Contract(message.into()))
}
fn format(name: &str, version: u32) -> Result<ConnectorDocumentFormat, MvDocumentError> {
    Ok(ConnectorDocumentFormat::try_new(
        MV_DOCUMENT_OWNER,
        name,
        version,
    )?)
}
fn revision(id: DocumentIdentity) -> DocumentRevision {
    DocumentRevision::try_from_bytes(&id.to_bytes())
        .expect("SPI and MV identities are both 32 bytes")
}
fn version(
    name: &str,
    format_version: u32,
    refs: Vec<(String, DocumentIdentity)>,
    content: Vec<u8>,
) -> Result<ConnectorDocumentVersion, MvDocumentError> {
    let value = ConnectorDocumentVersion::try_new(
        MV_DOCUMENT_OWNER,
        name,
        format(name, format_version)?,
        refs,
        Bytes::from(content),
    )?;
    Ok(value)
}

/// Build D with its complete MV-owned semantic identity.
pub(crate) fn definition_version(
    definition: &DefinitionDocument,
) -> Result<ConnectorDocumentVersion, MvDocumentError> {
    version(
        "definition",
        DEFINITION_FORMAT_VERSION,
        vec![],
        encode_definition(definition)
            .map_err(|e| MvDocumentError::Contract(format!("document version definition: {e}")))?
            .into_bytes(),
    )
}

/// The same complete D identity consumed by CREATE and keyed L references.
pub(crate) fn definition_revision(
    definition: &DefinitionDocument,
) -> Result<DocumentRevision, MvDocumentError> {
    Ok(revision(definition_version(definition)?.identity()))
}

pub fn build_keyed_versions(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    binding: BindingDocument,
    mapping: StorageMappingDocument,
) -> Result<MvKeyedVersions, MvDocumentError> {
    let d = definition_version(definition)?;
    let mut interpretation = interpretation.clone();
    interpretation.definition_revision = revision(d.identity());
    super::validation::validate_binding_interpretation(
        &binding,
        interpretation.definition_revision,
        &interpretation,
        &mapping,
    )?;
    let l = version(
        "interpretation",
        KEYED_INTERPRETATION_FORMAT_VERSION,
        vec![("definition".into(), d.identity())],
        encode_keyed_interpretation(&interpretation)?.into_bytes(),
    )?;
    let m = version(
        "mapping",
        STORAGE_MAPPING_FORMAT_VERSION,
        vec![],
        encode_storage_mapping(&mapping)?.into_bytes(),
    )?;
    let b = version(
        "binding",
        BINDING_FORMAT_VERSION,
        vec![
            ("definition".into(), d.identity()),
            ("interpretation".into(), l.identity()),
            ("mapping".into(), m.identity()),
        ],
        encode_binding(&binding)?.into_bytes(),
    )?;
    Ok(MvKeyedVersions {
        definition: d,
        interpretation: l,
        mapping: m,
        binding: b,
        binding_document: binding,
        mapping_document: mapping,
    })
}

pub fn keyed_create_document_set(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    configuration: &ConfigurationDocument,
    binding: BindingDocument,
    mapping: StorageMappingDocument,
    target: &ConnectorPreparedCreateDocumentTarget,
    owner: &DeploymentOwner,
    incarnation: &ProcessIncarnation,
) -> Result<KeyedCreate, MvDocumentError> {
    super::documents::validate_create_target(interpretation, target)?;
    let versions = build_keyed_versions(definition, interpretation, binding, mapping)?;
    Ok(KeyedCreate {
        versions: versions.versions(),
        slots: BTreeMap::from([
            (
                configuration_slot_name()?,
                configuration_slot(configuration)?,
            ),
            (
                management_slot_name()?,
                management_slot(owner, incarnation)?,
            ),
        ]),
        roots: BTreeMap::from([(MV_DOCUMENT_OWNER.into(), versions.binding.identity())]),
    })
}
pub fn keyed_publication_document_set(
    versions: &MvKeyedVersions,
    publication: &KeyedPublicationDocument,
) -> Result<KeyedPublication, MvDocumentError> {
    if publication.definition_revision != versions.definition_revision()
        || publication.interpretation_revision != versions.interpretation_revision()
    {
        return contract("publication revisions do not bind the exact D/L identities");
    }
    let payload = encode_publication_payload(publication, &versions.binding_document)?;
    let record = ConnectorSnapshotRecord {
        owner: MV_DOCUMENT_OWNER.into(),
        name: "publication".into(),
        binding: Some(versions.binding.identity()),
        payload: Bytes::from(payload),
    };
    Ok(KeyedPublication {
        records: vec![record],
        versions: versions.versions(),
        roots: BTreeMap::from([(MV_DOCUMENT_OWNER.into(), versions.binding.identity())]),
        ..Default::default()
    })
}
pub fn keyed_repartition_document_set(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    binding: BindingDocument,
    mapping: StorageMappingDocument,
    publication: &KeyedPublicationDocument,
) -> Result<KeyedPublication, MvDocumentError> {
    let versions = build_keyed_versions(definition, interpretation, binding, mapping)?;
    keyed_publication_document_set(&versions, publication)
}
pub fn configuration_slot_name() -> Result<ConnectorSlotName, MvDocumentError> {
    Ok(ConnectorSlotName::try_new(
        MV_DOCUMENT_OWNER,
        "configuration",
    )?)
}
pub fn management_slot_name() -> Result<ConnectorSlotName, MvDocumentError> {
    Ok(ConnectorSlotName::try_new(
        MANAGEMENT_SLOT_OWNER,
        MANAGEMENT_SLOT_NAME,
    )?)
}
pub fn configuration_slot(
    value: &ConfigurationDocument,
) -> Result<ConnectorSlotValue, MvDocumentError> {
    Ok(ConnectorSlotValue {
        format: format("configuration", 1)?,
        content: Bytes::from(encode_configuration(value)?.into_bytes()),
    })
}
pub fn management_slot(
    owner: &DeploymentOwner,
    incarnation: &ProcessIncarnation,
) -> Result<ConnectorSlotValue, MvDocumentError> {
    let mut content = Vec::new();
    for value in ["materialized-view", owner.as_str(), incarnation.as_str()] {
        content.extend_from_slice(&(value.len() as u32).to_be_bytes());
        content.extend_from_slice(value.as_bytes());
    }
    Ok(ConnectorSlotValue {
        format: format("management", 1)?,
        content: Bytes::from(content),
    })
}
pub fn keyed_configuration_slot_change(
    configuration: &ConfigurationDocument,
) -> Result<KeyedSlotChange, MvDocumentError> {
    Ok(KeyedSlotChange {
        slots: BTreeMap::from([(
            configuration_slot_name()?,
            configuration_slot(configuration)?,
        )]),
        ..Default::default()
    })
}
/// Same-value handovers remain real commits; only the provider fills writer.
pub fn keyed_management_slot_change(
    owner: &DeploymentOwner,
    incarnation: &ProcessIncarnation,
) -> Result<KeyedSlotChange, MvDocumentError> {
    Ok(KeyedSlotChange {
        slots: BTreeMap::from([(
            management_slot_name()?,
            management_slot(owner, incarnation)?,
        )]),
        ..Default::default()
    })
}

pub fn decode_management_slot(
    value: &ConnectorSlotValue,
) -> Result<(DeploymentOwner, ProcessIncarnation), MvDocumentError> {
    if value.format != format("management", 1)? {
        return contract("unsupported MV management slot format");
    }
    let mut rest = value.content.as_ref();
    let mut fields = Vec::new();
    for _ in 0..3 {
        if rest.len() < 4 {
            return contract("truncated management slot");
        }
        let len = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
        rest = &rest[4..];
        if len > rest.len() {
            return contract("truncated management slot identity");
        }
        let text = std::str::from_utf8(&rest[..len])
            .map_err(|_| MvDocumentError::Contract("management identity is not UTF-8".into()))?;
        fields.push(text);
        rest = &rest[len..];
    }
    if !rest.is_empty() || fields[0] != "materialized-view" {
        return contract("invalid MV management slot kind or trailing bytes");
    }
    Ok((
        DeploymentOwner::parse(fields[1]).map_err(|e| MvDocumentError::Contract(e.to_string()))?,
        ProcessIncarnation::parse(fields[2])
            .map_err(|e| MvDocumentError::Contract(e.to_string()))?,
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvKeyedObservedCurrentDocuments {
    pub management_target: ManagedMvTarget,
    pub deployment_owner: DeploymentOwner,
    pub process_incarnation: ProcessIncarnation,
    pub view: KeyedDocumentView,
    pub definition: DefinitionDocument,
    pub interpretation: InterpretationDocument,
    pub versions: MvKeyedVersions,
    pub configuration: ConfigurationDocument,
    pub configuration_revision: DocumentRevision,
    pub publication: Option<KeyedPublicationDocument>,
    pub publication_revision: Option<DocumentRevision>,
    /// The snapshot carrying the current record, including maintenance/slot copies.
    pub publication_snapshot: Option<i64>,
}
impl MvKeyedObservedCurrentDocuments {
    pub fn decoded_markers(&self) -> Result<Vec<(ConnectorMarkerKey, Marker)>, MvDocumentError> {
        self.view
            .markers
            .iter()
            .filter(|(key, _)| key.owner == MV_DOCUMENT_OWNER)
            .map(|(key, value)| Ok((key.clone(), Marker::decode(key, value)?)))
            .collect()
    }

    pub fn root_binding_revision(&self) -> DocumentRevision {
        revision(self.versions.binding.identity())
    }

    pub fn is_never_published(&self) -> bool {
        never_published(&self.view)
    }

    pub fn publication_evidence<'a>(
        &'a self,
        root: &'a DocumentRevision,
    ) -> super::codec::PublicationEvidence<'a> {
        use super::codec::PublicationEvidence;
        match self.publication.as_ref() {
            Some(publication) => PublicationEvidence::Current(&publication.publication_id),
            None if self.is_never_published() && *root == self.root_binding_revision() => {
                PublicationEvidence::Unpublished(root)
            }
            None => PublicationEvidence::Unknown,
        }
    }
    pub fn management_dependencies(
        &self,
        control: ConnectorControlRuntimeId,
    ) -> crate::management::ManagementDependencySet {
        crate::management::ManagementDependencySet::new(
            *self.versions.definition_revision().as_bytes(),
            *self.versions.interpretation_revision().as_bytes(),
            self.publication_revision.map(|r| *r.as_bytes()),
            control,
        )
    }
}
pub fn never_published(view: &KeyedDocumentView) -> bool {
    view.before_first_data_publication
        && !view
            .current_records
            .iter()
            .any(|r| r.owner == MV_DOCUMENT_OWNER)
        && (view.current_snapshot.is_none() || view.current_snapshot_file_totals.is_empty())
}
fn decode_keyed_binding_documents(
    view: &KeyedDocumentView,
    binding_id: DocumentIdentity,
    configuration_content: &[u8],
) -> Result<(DefinitionDocument, InterpretationDocument, MvKeyedVersions), MvDocumentError> {
    let envelope = inspect_binding(view, binding_id)?;
    envelope.require_current()?;
    let m = envelope.mapping.ok_or_else(|| {
        MvDocumentError::Contract("current binding lacks its storage mapping".into())
    })?;
    let d = envelope.definition;
    let l = envelope.interpretation;
    let b = envelope.binding;
    preflight_keyed_document_set(
        d.content(),
        l.content(),
        b.content(),
        m.content(),
        configuration_content,
    )?;
    let definition = decode_definition(d.content())?;
    let interpretation = decode_keyed_interpretation(l.content())?;
    let binding = decode_binding(b.content())?;
    let mapping = decode_storage_mapping(m.content())?;
    super::validation::validate_binding_interpretation(
        &binding,
        revision(d.identity()),
        &interpretation,
        &mapping,
    )?;
    if interpretation.target.object_id.as_bytes() != view.object_id.as_bytes().as_ref() {
        return contract("interpretation binds a different target object");
    }
    let versions = MvKeyedVersions {
        definition: d.clone(),
        interpretation: l.clone(),
        mapping: m.clone(),
        binding: b.clone(),
        binding_document: binding,
        mapping_document: mapping,
    };
    Ok((definition, interpretation, versions))
}

/// Exact root facts used solely for deletion of an already observed target.
/// The body of a retired binding is deliberately never decoded by this path.
pub(crate) struct MvKeyedDropDocuments {
    pub target: ManagedMvTarget,
    pub owner: DeploymentOwner,
    pub incarnation: ProcessIncarnation,
    pub root_binding: DocumentIdentity,
    pub definition_revision: DocumentRevision,
    pub interpretation_revision: DocumentRevision,
    pub configuration_revision: DocumentRevision,
    pub(crate) retired: bool,
    pub(crate) management_slot_digest: DocumentIdentity,
    pub(crate) configuration_slot_digest: DocumentIdentity,
    pub(crate) configuration: ConfigurationDocument,
    pub(crate) metadata_version: ConnectorCommittedVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredMvStatus {
    pub target: ManagedMvTarget,
    pub configuration: ConfigurationDocument,
}

pub fn project_retired_mv_status(
    view: &KeyedDocumentView,
    catalog: CatalogHandle,
) -> Result<Option<RetiredMvStatus>, MvDocumentError> {
    let documents = decode_keyed_drop_documents(view, catalog)?;
    Ok(documents.retired.then_some(RetiredMvStatus {
        target: documents.target,
        configuration: documents.configuration,
    }))
}

pub(crate) fn decode_keyed_drop_documents(
    view: &KeyedDocumentView,
    catalog: CatalogHandle,
) -> Result<MvKeyedDropDocuments, MvDocumentError> {
    if view.metadata_version.snapshot_id() != view.current_snapshot
        || catalog.catalog_name() != &view.table.instance_id
        || view.owner.instance_id != view.table.instance_id
    {
        return contract("MV DROP keyed catalog or current metadata scope mismatch");
    }
    let target = ManagedMvTarget::try_new(catalog, view.table.clone(), view.object_id.clone())
        .map_err(|error| MvDocumentError::Contract(error.to_string()))?;
    let management_name = management_slot_name()?;
    let management = view
        .slots
        .get(&management_name)
        .ok_or_else(|| MvDocumentError::Contract("missing MV management slot".into()))?;
    let (owner, incarnation) = decode_management_slot(&management.value)?;
    let configuration_name = configuration_slot_name()?;
    let configuration = view
        .slots
        .get(&configuration_name)
        .ok_or_else(|| MvDocumentError::Contract("missing MV configuration slot".into()))?;
    if configuration.value.format != format("configuration", 1)? {
        return contract("unsupported MV configuration slot format");
    }
    let configuration_document = decode_configuration(&configuration.value.content)?;
    let root = view
        .roots
        .get(MV_DOCUMENT_OWNER)
        .filter(|root| !root.dangling)
        .ok_or_else(|| MvDocumentError::Contract("MV binding root is absent or dangling".into()))?;
    let envelope = inspect_binding(view, root.identity)?;
    Ok(MvKeyedDropDocuments {
        target,
        owner,
        incarnation,
        root_binding: envelope.binding.identity(),
        definition_revision: revision(envelope.definition.identity()),
        interpretation_revision: revision(envelope.interpretation.identity()),
        configuration_revision: DocumentRevision::from_canonical_bytes(
            &configuration.value.content,
        ),
        retired: envelope.generation == super::binding_envelope::BindingGeneration::Retired,
        management_slot_digest: management.value.digest(&management_name, management.writer),
        configuration_slot_digest: configuration
            .value
            .digest(&configuration_name, configuration.writer),
        configuration: configuration_document,
        metadata_version: view.metadata_version.clone(),
    })
}

/// Ordinary direct reads only need the known format gate over the original
/// Current envelopes. They do not decode the definition or publication data.
pub fn require_readable_keyed_format(view: &KeyedDocumentView) -> Result<(), MvDocumentError> {
    let mut publications = view
        .current_records
        .iter()
        .filter(|record| record.owner == MV_DOCUMENT_OWNER && record.name == "publication");
    let binding = match publications.next() {
        Some(record) => {
            if publications.next().is_some() {
                return contract("duplicate MV publication record");
            }
            record.binding
        }
        None => view
            .roots
            .get(MV_DOCUMENT_OWNER)
            .filter(|root| !root.dangling)
            .map(|root| root.identity),
    }
    .ok_or_else(|| MvDocumentError::Contract("MV has no exact Current binding envelope".into()))?;
    inspect_binding(view, binding)?.require_current()
}

pub fn decode_current_keyed_documents(
    view: KeyedDocumentView,
    catalog: CatalogHandle,
) -> Result<MvKeyedObservedCurrentDocuments, MvDocumentError> {
    if view.metadata_version.snapshot_id() != view.current_snapshot {
        return contract("MV keyed metadata version and current snapshot disagree");
    }
    if catalog.catalog_name() != &view.table.instance_id
        || view.owner.instance_id != view.table.instance_id
    {
        return contract("keyed view catalog owner mismatch");
    }
    let slot = view
        .slots
        .get(&management_slot_name()?)
        .ok_or_else(|| MvDocumentError::Contract("missing MV management slot".into()))?;
    let (owner, incarnation) = decode_management_slot(&slot.value)?;
    let management_target =
        ManagedMvTarget::try_new(catalog, view.table.clone(), view.object_id.clone())
            .map_err(|e| MvDocumentError::Contract(e.to_string()))?;
    let c = view
        .slots
        .get(&configuration_slot_name()?)
        .ok_or_else(|| MvDocumentError::Contract("missing MV configuration slot".into()))?;
    if c.value.format != format("configuration", 1)? {
        return contract("unsupported MV configuration slot format");
    }
    let configuration = decode_configuration(&c.value.content)?;
    let configuration_revision = DocumentRevision::from_canonical_bytes(&c.value.content);
    let records = view
        .current_records
        .iter()
        .filter(|r| r.owner == MV_DOCUMENT_OWNER && r.name == "publication")
        .collect::<Vec<_>>();
    if records.len() > 1 {
        return contract("duplicate MV publication record");
    }
    let record = records.first().copied();
    if record.is_none() && !never_published(&view) {
        return contract(
            "current snapshot contains data but no MV publication record (external modification)",
        );
    }
    let binding_id = match record {
        Some(r) => r.binding.ok_or_else(|| {
            MvDocumentError::Contract("publication has no binding identity".into())
        })?,
        None => view
            .roots
            .get(MV_DOCUMENT_OWNER)
            .filter(|r| !r.dangling)
            .map(|r| r.identity)
            .ok_or_else(|| {
                MvDocumentError::Contract("MV binding root is absent or dangling".into())
            })?,
    };
    let (definition, interpretation, versions) =
        decode_keyed_binding_documents(&view, binding_id, &c.value.content)?;
    let publication = record
        .map(|r| {
            decode_publication_payload(
                &r.payload,
                versions.definition_revision(),
                versions.interpretation_revision(),
                &versions.binding_document,
            )
        })
        .transpose()?;
    let publication_revision =
        record.map(|r| DocumentRevision::from_canonical_bytes(&snapshot_record_frame(r)));
    let publication_snapshot = record
        .map(|_| {
            view.current_snapshot.ok_or_else(|| {
                MvDocumentError::Contract("publication record has no carrier snapshot".into())
            })
        })
        .transpose()?;
    Ok(MvKeyedObservedCurrentDocuments {
        management_target,
        deployment_owner: owner,
        process_incarnation: incarnation,
        view,
        definition,
        interpretation,
        versions,
        configuration,
        configuration_revision,
        publication,
        publication_revision,
        publication_snapshot,
    })
}
pub fn observe_current_keyed_documents(
    lease: &ConnectorDocumentStorage,
    request: KeyedDocumentObservationRequest,
) -> Result<MvKeyedObservedCurrentDocuments, MvDocumentError> {
    let catalog = request.catalog_handle.clone();
    let view = lease
        .observe_keyed_documents(request)
        .map_err(MvDocumentError::KeyedStorage)?;
    decode_current_keyed_documents(view, catalog)
}

pub fn snapshot_record_frame(record: &ConnectorSnapshotRecord) -> Vec<u8> {
    format!(
        "2.{}.{}",
        record
            .binding
            .map(|id| id.to_hex())
            .unwrap_or_else(|| "-".into()),
        base64url(&record.payload)
    )
    .into_bytes()
}
fn base64url(bytes: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((bytes.len() * 4).div_ceil(3));
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(CHARS[((n >> 18) & 63) as usize] as char);
        out.push(CHARS[((n >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(CHARS[((n >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(CHARS[(n & 63) as usize] as char);
        }
    }
    out
}

/// Immutable Accelerator facts for the keyed representation. The legacy P
/// model remains separately callable during migration and is never synthesized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvKeyedDocumentProjection {
    pub(crate) source: super::definition::MvAcceleratorSourceRevision,
    pub(crate) metadata_version: ConnectorCommittedVersion,
    pub(crate) definition: DefinitionDocument,
    pub(crate) interpretation: InterpretationDocument,
    pub(crate) configuration: ConfigurationDocument,
    pub(crate) versions: MvKeyedVersions,
    pub(crate) publication: Option<KeyedPublicationDocument>,
    pub(crate) file_totals: SnapshotFileTotals,
}
impl MvKeyedDocumentProjection {
    pub fn try_from_current(observed: MvKeyedObservedCurrentDocuments) -> Result<Self, String> {
        use super::definition::{
            MvAcceleratorCommittedVersionRevision, MvAcceleratorSourceRevision,
        };
        let output = observed.publication_snapshot.map(|_| {
            MvAcceleratorCommittedVersionRevision::from_committed(&observed.view.metadata_version)
        });
        Ok(Self {
            source: MvAcceleratorSourceRevision {
                target: observed.view.table,
                target_object_id: observed.view.object_id,
                metadata_version: MvAcceleratorCommittedVersionRevision::from_committed(
                    &observed.view.metadata_version,
                ),
                definition_revision: observed.versions.definition_revision(),
                interpretation_revision: observed.versions.interpretation_revision(),
                publication_revision: observed.publication_revision,
                publication_output_version: output,
                configuration_revision: observed.configuration_revision,
                deployment_owner: observed.deployment_owner,
                process_incarnation: observed.process_incarnation,
            },
            metadata_version: observed.view.metadata_version,
            definition: observed.definition,
            interpretation: observed.interpretation,
            configuration: observed.configuration,
            versions: observed.versions,
            publication: observed.publication,
            file_totals: observed.view.current_snapshot_file_totals,
        })
    }
    pub fn source_revision(&self) -> &super::definition::MvAcceleratorSourceRevision {
        &self.source
    }
    pub fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub fn definition(&self) -> &DefinitionDocument {
        &self.definition
    }
    pub fn binding(&self) -> &BindingDocument {
        &self.versions.binding_document
    }
    pub fn mapping(&self) -> &StorageMappingDocument {
        &self.versions.mapping_document
    }
    pub fn interpretation(&self) -> &InterpretationDocument {
        &self.interpretation
    }
    pub fn configuration(&self) -> &ConfigurationDocument {
        &self.configuration
    }
    pub fn publication(&self) -> Option<&KeyedPublicationDocument> {
        self.publication.as_ref()
    }
    pub fn publication_snapshot(&self) -> Option<i64> {
        self.publication
            .as_ref()
            .and(self.metadata_version.snapshot_id())
    }
    pub fn versions(&self) -> &MvKeyedVersions {
        &self.versions
    }
    pub fn target(&self) -> crate::product::MvTarget {
        crate::product::MvTarget::from_parts(
            Some(self.source.target.instance_id.as_str()),
            &self.source.target.namespace,
            &self.source.target.table,
        )
    }
    pub fn management_dependencies(
        &self,
        control: ConnectorControlRuntimeId,
    ) -> crate::management::ManagementDependencySet {
        self.source.management_dependencies(control)
    }
    pub fn never_published(&self) -> bool {
        self.metadata_version.snapshot_id().is_none()
            || (self.publication.is_none() && self.file_totals.is_empty())
    }
    /// A refresh rechecks the never-published witness at the current snapshot;
    /// an intervening empty slot-change snapshot is allowed.
    pub fn validate_refresh_fence(
        &self,
        current: &KeyedDocumentView,
    ) -> Result<(), MvDocumentError> {
        if current.table != self.source.target || current.object_id != self.source.target_object_id
        {
            return contract("MV refresh target was replaced");
        }
        if self.never_published() {
            if !never_published(current) {
                return contract("MV first-refresh witness no longer holds");
            }
        } else if current.current_snapshot != self.publication_snapshot() {
            return contract("MV current publication carrier changed before refresh");
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredKeyedMvProjection {
    pub mv_id: i64,
    pub facts: MvKeyedDocumentProjection,
}

pub fn observe_current_keyed_documents_with_management(
    lease: &ConnectorDocumentStorage,
    request: KeyedDocumentObservationRequest,
    entrance: &crate::management::ManagementEntrance,
) -> Result<
    (
        MvKeyedObservedCurrentDocuments,
        crate::management::SlotChangeConvergence,
    ),
    MvDocumentError,
> {
    let catalog = request.catalog_handle.clone();
    let (view, convergence) = observe_current_keyed_view_with_management(lease, request, entrance)?;
    let observed = decode_current_keyed_documents(view, catalog)?;
    Ok((observed, convergence))
}

pub async fn observe_current_keyed_documents_with_management_async(
    lease: &ConnectorDocumentStorage,
    request: KeyedDocumentObservationRequest,
    entrance: &crate::management::ManagementEntrance,
) -> Result<
    (
        MvKeyedObservedCurrentDocuments,
        crate::management::SlotChangeConvergence,
    ),
    MvDocumentError,
> {
    let catalog = request.catalog_handle.clone();
    let (view, convergence) =
        observe_current_keyed_view_with_management_async(lease, request, entrance).await?;
    let observed = decode_current_keyed_documents(view, catalog)?;
    Ok((observed, convergence))
}

pub(crate) fn observe_current_keyed_view_with_management(
    lease: &ConnectorDocumentStorage,
    request: KeyedDocumentObservationRequest,
    entrance: &crate::management::ManagementEntrance,
) -> Result<(KeyedDocumentView, crate::management::SlotChangeConvergence), MvDocumentError> {
    let catalog = request.catalog_handle.clone();
    let mut view = lease
        .observe_keyed_documents(request.clone())
        .map_err(MvDocumentError::KeyedStorage)?;
    // Revocation grants no authority. A generic reader may still return valid
    // foreign-owned facts for a read-only projection; corrupt P remains an error.
    entrance.revoke_foreign_current_management(&view, &catalog)?;
    let convergence = entrance.resolve_pending_slot_changes(lease, request.clone())?;
    if convergence.reinstall_current_projection {
        view = lease
            .observe_keyed_documents(request)
            .map_err(MvDocumentError::KeyedStorage)?;
        entrance.revoke_foreign_current_management(&view, &catalog)?;
    }
    Ok((view, convergence))
}

pub(crate) async fn observe_current_keyed_view_with_management_async(
    lease: &ConnectorDocumentStorage,
    request: KeyedDocumentObservationRequest,
    entrance: &crate::management::ManagementEntrance,
) -> Result<(KeyedDocumentView, crate::management::SlotChangeConvergence), MvDocumentError> {
    let catalog = request.catalog_handle.clone();
    let mut view = lease
        .observe_keyed_documents_async(request.clone())
        .await
        .map_err(MvDocumentError::KeyedStorage)?;
    // Revocation grants no authority. A generic reader may still return valid
    // foreign-owned facts for a read-only projection; corrupt P remains an error.
    entrance.revoke_foreign_current_management(&view, &catalog)?;
    let convergence = entrance
        .resolve_pending_slot_changes_async(lease, request.clone())
        .await?;
    if convergence.reinstall_current_projection {
        view = lease
            .observe_keyed_documents_async(request)
            .await
            .map_err(MvDocumentError::KeyedStorage)?;
        entrance.revoke_foreign_current_management(&view, &catalog)?;
    }
    Ok((view, convergence))
}

#[cfg(any(test, novarocks_dev_controls))]
pub fn keyed_view_fixture(
    snapshot: Option<i64>,
    published: bool,
) -> (CatalogHandle, KeyedDocumentView) {
    let f = crate::persistence::test_support::MvDocumentFixture::new();
    let mut inputs = Vec::new();
    for occurrence in &f.binding.occurrences {
        let raw = super::exact_revision::restore_persisted_object(&occurrence.object_id).unwrap();
        let source = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &raw,
            Some(17),
        )
        .unwrap();
        let (_, native_data_version) =
            super::exact_revision::persist_exact_connector_revision(&source).unwrap();
        inputs.push(PublicationInput {
            relation_occurrence_id: occurrence.occurrence_id,
            object_id: occurrence.object_id.clone(),
            native_data_version,
        });
    }
    let versions =
        build_keyed_versions(&f.definition, &f.interpretation, f.binding, f.mapping).unwrap();
    let publication = KeyedPublicationDocument {
        publication_prepared_at_ms: 42,
        publication_id: super::identity::PublicationIdentity::try_new(vec![0x11; 16]).unwrap(),
        definition_revision: versions.definition_revision(),
        interpretation_revision: versions.interpretation_revision(),
        inputs,
        logical_result_rows: Some(1),
    };
    let records = if published {
        keyed_publication_document_set(&versions, &publication)
            .unwrap()
            .records
    } else {
        vec![]
    };
    let table = ConnectorTableIdentity {
        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
        namespace: "sales".into(),
        table: "mv".into(),
    };
    let catalog = CatalogHandle::new(
        table.instance_id.clone(),
        CatalogVersion::from_bytes([1; 32]),
    );
    let owner = DeploymentOwner::parse("deployment").unwrap();
    let incarnation = ProcessIncarnation::parse("process").unwrap();
    let slots = BTreeMap::from([
        (
            configuration_slot_name().unwrap(),
            ConnectorObservedSlot {
                value: configuration_slot(&f.configuration).unwrap(),
                writer: snapshot,
            },
        ),
        (
            management_slot_name().unwrap(),
            ConnectorObservedSlot {
                value: management_slot(&owner, &incarnation).unwrap(),
                writer: snapshot,
            },
        ),
    ]);
    let view = KeyedDocumentView {
        owner: ConnectorProviderBindingKey {
            instance_id: table.instance_id.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([3; 16]),
        },
        table,
        object_id: f.object_id,
        metadata_version: ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"metadata"),
            snapshot,
        )
        .unwrap(),
        current_snapshot: snapshot,
        before_first_data_publication: !published,
        current_snapshot_file_totals: SnapshotFileTotals {
            data_files: Some(u64::from(published)),
            delete_files: Some(0),
        },
        slots,
        markers: BTreeMap::new(),
        versions: versions
            .versions()
            .into_iter()
            .map(|v| (v.identity(), v))
            .collect(),
        roots: BTreeMap::from([(
            MV_DOCUMENT_OWNER.into(),
            KeyedDocumentRoot {
                identity: versions.binding.identity(),
                dangling: false,
            },
        )]),
        current_records: records,
        management: None,
    };
    (catalog, view)
}

#[cfg(test)]
pub(crate) mod tests {
    pub(crate) use super::keyed_view_fixture as fixture;
    use super::*;
    #[test]
    fn no_publication_requires_absent_snapshot_or_known_empty_file_totals() {
        let (catalog, mut view) = fixture(Some(17), false);
        assert!(never_published(&view));
        assert!(
            decode_current_keyed_documents(view.clone(), catalog.clone())
                .unwrap()
                .is_never_published()
        );
        view.current_snapshot_file_totals.data_files = None;
        assert!(!never_published(&view));
        assert!(decode_current_keyed_documents(view.clone(), catalog.clone()).is_err());
        view.current_snapshot_file_totals.data_files = Some(1);
        assert!(decode_current_keyed_documents(view, catalog).is_err());
    }

    #[test]
    fn an_empty_external_snapshot_does_not_prove_first_publication() {
        let (catalog, mut view) = fixture(Some(17), false);
        view.before_first_data_publication = false;
        assert!(view.current_snapshot_file_totals.is_empty());
        assert!(!never_published(&view));
        assert!(decode_current_keyed_documents(view, catalog).is_err());
    }
    #[test]
    fn copied_record_projects_its_current_carrier_and_c_revision_ignores_writer() {
        let (catalog, view) = fixture(Some(17), true);
        let first = decode_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
        let mut later = view;
        later.current_snapshot = Some(18);
        later.metadata_version =
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata-later"), Some(18))
                .unwrap();
        later
            .slots
            .get_mut(&configuration_slot_name().unwrap())
            .unwrap()
            .writer = Some(18);
        later.roots.get_mut(MV_DOCUMENT_OWNER).unwrap().dangling = true;
        let second = decode_current_keyed_documents(later, catalog).unwrap();
        assert_eq!(first.configuration_revision, second.configuration_revision);
        assert_eq!(first.publication_revision, second.publication_revision);
        assert_eq!(second.publication_snapshot, Some(18));
        assert_eq!(first.publication, second.publication);
    }
    #[test]
    fn same_value_management_change_is_not_elided_and_slot_is_strict() {
        let owner = DeploymentOwner::parse("deployment").unwrap();
        let inc = ProcessIncarnation::parse("process").unwrap();
        let change = keyed_management_slot_change(&owner, &inc).unwrap();
        assert_eq!(change.slots.len(), 1);
        let value = change.slots.values().next().unwrap();
        assert_eq!(decode_management_slot(value).unwrap(), (owner, inc));
        let mut trailing = value.clone();
        trailing.content = Bytes::from([value.content.to_vec(), vec![0]].concat());
        assert!(decode_management_slot(&trailing).is_err());
        let mut wrong = value.clone();
        wrong.format = ConnectorDocumentFormat::try_new("another", "management", 1).unwrap();
        assert!(decode_management_slot(&wrong).is_err());
    }
    #[test]
    fn p_frame_matches_the_shared_independent_golden() {
        let r = ConnectorSnapshotRecord {
            owner: MV_DOCUMENT_OWNER.into(),
            name: "publication".into(),
            binding: Some(
                DocumentIdentity::parse_hex(
                    "b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e",
                )
                .unwrap(),
            ),
            payload: Bytes::from_static(b"abc"),
        };
        assert_eq!(
            snapshot_record_frame(&r),
            b"2.b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e.YWJj"
        );
    }
    #[test]
    fn p04_union_documents_cross_legacy_sizes_and_keep_exact_current_facts() {
        use crate::persistence::test_support::{
            observed_current_keyed, union_all_keyed_projection_fixture,
        };
        // Component proof of the native case's shape; the native runner still
        // owns CREATE/publication/refresh/restart validation.
        let fixture = union_all_keyed_projection_fixture(
            crate::product::MvTarget::from_parts(Some("ice"), "sales", "capacity_mv"),
            Some(17),
            370,
            10,
            16_384,
        );
        let expected_definition = fixture.definition.clone();
        let expected_inputs = fixture.publication.as_ref().unwrap().inputs.clone();
        let facts = fixture.build().unwrap();
        let d = facts.definition();
        let b = facts.binding();
        let items = b.occurrences.len()
            + b.outputs.len()
            + b.occurrences.iter().map(|r| r.paths.len()).sum::<usize>()
            + b.outputs
                .iter()
                .map(|o| o.source_fields.len())
                .sum::<usize>();
        assert_eq!(b.occurrences.len(), 370);
        assert_eq!(b.outputs.len(), 10);
        assert_eq!(items, 4_450);
        assert_eq!(d, &expected_definition);
        assert!(facts.versions().definition.content().len() > 65_536);
        assert_eq!(facts.interpretation().branches.len(), 370);
        let publication = facts.publication().unwrap();
        assert_eq!(publication.inputs, expected_inputs);
        assert_eq!(publication.logical_result_rows, Some(740));
        let payload = encode_publication_payload(publication, b).unwrap();
        assert!(payload.len() > 4_096);
        assert_eq!(payload.len(), 4_106);
        assert_eq!(
            decode_publication_payload(
                &payload,
                facts.versions().definition_revision(),
                facts.versions().interpretation_revision(),
                &facts.versions().binding_document,
            )
            .unwrap(),
            *publication
        );
        let set = keyed_publication_document_set(facts.versions(), publication).unwrap();
        assert_eq!(set.records.len(), 1);
        assert!(snapshot_record_frame(&set.records[0]).len() > 4_096);
        let catalog = CatalogHandle::new(
            facts.source_revision().target.instance_id.clone(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let restored =
            MvKeyedDocumentProjection::try_from_current(observed_current_keyed(&facts, catalog))
                .unwrap();
        assert_eq!(restored, facts);
        assert_eq!(
            restored.definition().query.effective_sql.as_bytes(),
            expected_definition.query.effective_sql.as_bytes(),
        );
        for (before, after) in facts
            .versions()
            .versions()
            .iter()
            .zip(restored.versions().versions())
        {
            assert_eq!(before.identity(), after.identity());
            assert_eq!(before.content(), after.content());
        }
        let projection = StoredKeyedMvProjection {
            mv_id: 7,
            facts: facts.clone(),
        };
        let encoded_projection = crate::state_store_repository::codec::encode_keyed_projection(
            uuid::Uuid::from_u128(7),
            &projection,
        )
        .unwrap();
        assert!(encoded_projection.as_bytes().len() > 65_536);
        eprintln!(
            "P04 component documents: occurrences={}, outputs={}, items={}, sql_bytes={}, D_bytes={}, L_bytes={}, B_bytes={}, P_bytes={}, record_frame_bytes={}, projection_bytes={}",
            b.occurrences.len(),
            b.outputs.len(),
            items,
            d.query.effective_sql.len(),
            facts.versions().definition.content().len(),
            facts.versions().interpretation.content().len(),
            facts.versions().binding.content().len(),
            payload.len(),
            snapshot_record_frame(&set.records[0]).len(),
            encoded_projection.as_bytes().len(),
        );
    }

    #[test]
    fn retired_root_grants_exact_drop_dependencies_without_decoding_retired_payloads() {
        let (catalog, mut view) = fixture(None, false);
        let definition = version("definition", 1, vec![], vec![8, 1, 0xff]).unwrap();
        let interpretation = version(
            "interpretation",
            2,
            vec![("definition".into(), definition.identity())],
            vec![8, 2, 0xff],
        )
        .unwrap();
        let binding = version(
            "binding",
            1,
            vec![
                ("definition".into(), definition.identity()),
                ("interpretation".into(), interpretation.identity()),
            ],
            vec![8, 1, 0xff],
        )
        .unwrap();
        view.roots.get_mut(MV_DOCUMENT_OWNER).unwrap().identity = binding.identity();
        view.versions = [definition.clone(), interpretation.clone(), binding.clone()]
            .into_iter()
            .map(|document| (document.identity(), document))
            .collect();
        assert!(matches!(
            decode_current_keyed_documents(view.clone(), catalog.clone()),
            Err(MvDocumentError::KeyedStorage(
                KeyedDocumentReadError::LegacyFormat { .. }
            ))
        ));
        let drop = decode_keyed_drop_documents(&view, catalog.clone()).unwrap();
        assert!(drop.retired);
        assert!(
            project_retired_mv_status(&view, catalog.clone())
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            require_readable_keyed_format(&view),
            Err(MvDocumentError::KeyedStorage(
                KeyedDocumentReadError::LegacyFormat { .. }
            ))
        ));
        assert_eq!(drop.root_binding, binding.identity());
        assert_eq!(drop.definition_revision, revision(definition.identity()));
        assert_eq!(
            drop.interpretation_revision,
            revision(interpretation.identity())
        );
        view.versions.remove(&interpretation.identity());
        assert!(decode_keyed_drop_documents(&view, catalog).is_err());
    }

    #[test]
    fn semantic_definition_identity_covers_the_document_header() {
        let (_, view) = fixture(None, false);
        let b = &view.versions[&view.roots[MV_DOCUMENT_OWNER].identity];
        let id = b.references()["definition"];
        let d = &view.versions[&id];
        assert_ne!(
            id.to_bytes().as_slice(),
            DocumentRevision::from_canonical_bytes(d.content()).as_bytes()
        );
        let bytes =
            decode_keyed_interpretation(view.versions[&b.references()["interpretation"]].content())
                .unwrap();
        assert_eq!(bytes.definition_revision, revision(id));
    }
}
