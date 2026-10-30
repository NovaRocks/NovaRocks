// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! MV-owned keyed document mapping. Legacy documents remain only for the
//! staged FE migration; this path never converts a keyed record into legacy P.

use super::{
    codec::keyed::*, codec::*, documents::MvDocumentError, identity::DocumentRevision,
    validation::PersistenceDecodeBudget,
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
    pub binding: ConnectorDocumentVersion,
    pub binding_document: BindingDocument,
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
    // Compute the exact canonical outer JSON value size before provider dispatch.
    let refs = value
        .references()
        .iter()
        .map(|(rel, id)| serde_json::json!({"rel":rel,"id":id.to_hex()}))
        .collect::<Vec<_>>();
    let envelope = serde_json::json!({"v":2,"owner":value.owner(),"name":value.name(),"format":{"owner":value.format().owner(),"name":value.format().name(),"version":value.format().version()},"refs":refs,"content":base64url(value.content()),"id":value.identity().to_hex()});
    let size = serde_json::to_vec(&envelope)
        .map_err(|e| MvDocumentError::Contract(e.to_string()))?
        .len();
    if size > KEYED_DOCUMENT_VERSION_BYTES {
        return contract(format!(
            "document version {name} uses {size} bytes, exceeding limit {KEYED_DOCUMENT_VERSION_BYTES}"
        ));
    }
    Ok(value)
}

/// Build D with its complete MV-owned semantic identity and envelope limit.
pub(crate) fn definition_version(
    definition: &DefinitionDocument,
) -> Result<ConnectorDocumentVersion, MvDocumentError> {
    version(
        "definition",
        1,
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
) -> Result<MvKeyedVersions, MvDocumentError> {
    if binding.data_versions.len() != definition.relation_occurrences.len() {
        return contract("binding occurrence count differs from definition");
    }
    for (occurrence, descriptor) in definition
        .relation_occurrences
        .iter()
        .zip(&binding.data_versions)
    {
        let object = super::exact_revision::decode_fact(occurrence.object_id.as_bytes())?;
        if object.provider != descriptor.provider {
            return contract(
                "binding data-version provider differs from definition object provider",
            );
        }
    }
    let d = definition_version(definition)?;
    let mut interpretation = interpretation.clone();
    interpretation.definition_revision = revision(d.identity());
    super::validation::validate_definition_interpretation(
        definition,
        revision(d.identity()),
        &interpretation,
    )?;
    let l = version(
        "interpretation",
        2,
        vec![("definition".into(), d.identity())],
        encode_keyed_interpretation(&interpretation)
            .map_err(|e| {
                MvDocumentError::Contract(format!("document version interpretation: {e}"))
            })?
            .into_bytes(),
    )?;
    let b = version(
        "binding",
        1,
        vec![
            ("definition".into(), d.identity()),
            ("interpretation".into(), l.identity()),
        ],
        encode_binding(&binding)
            .map_err(|e| MvDocumentError::Contract(format!("document version binding: {e}")))?
            .into_bytes(),
    )?;
    let max_payload = maximum_publication_payload_bytes(&binding)?;
    let max_record = "novarocks.doc.v2.record.novarocks.mv~publication".len()
        + 2
        + 64
        + 1
        + (max_payload * 4).div_ceil(3);
    if max_record > KEYED_DOCUMENT_RECORD_BYTES {
        return contract(format!(
            "snapshot record publication maximum uses {max_record} bytes, exceeding limit {KEYED_DOCUMENT_RECORD_BYTES}"
        ));
    }
    Ok(MvKeyedVersions {
        definition: d,
        interpretation: l,
        binding: b,
        binding_document: binding,
    })
}

pub fn keyed_create_document_set(
    definition: &DefinitionDocument,
    interpretation: &InterpretationDocument,
    configuration: &ConfigurationDocument,
    binding: BindingDocument,
    target: &ConnectorPreparedCreateDocumentTarget,
    owner: &DeploymentOwner,
    incarnation: &ProcessIncarnation,
) -> Result<KeyedCreate, MvDocumentError> {
    super::documents::validate_create_target(interpretation, target)?;
    let versions = build_keyed_versions(definition, interpretation, binding)?;
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
    definition: &DefinitionDocument,
    publication: &KeyedPublicationDocument,
) -> Result<KeyedPublication, MvDocumentError> {
    if publication.definition_revision != versions.definition_revision()
        || publication.interpretation_revision != versions.interpretation_revision()
    {
        return contract("publication revisions do not bind the exact D/L identities");
    }
    let payload = encode_publication_payload(publication, definition, &versions.binding_document)?;
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
    publication: &KeyedPublicationDocument,
) -> Result<KeyedPublication, MvDocumentError> {
    let versions = build_keyed_versions(definition, interpretation, binding)?;
    keyed_publication_document_set(&versions, definition, publication)
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
fn configuration_slot(
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
    pub fn is_never_published(&self) -> bool {
        never_published(&self.view)
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
    view.current_snapshot.is_none()
        || (!view
            .current_records
            .iter()
            .any(|r| r.owner == MV_DOCUMENT_OWNER && r.name == "publication")
            && view.current_snapshot_file_totals.is_empty())
}
fn exact_version<'a>(
    view: &'a KeyedDocumentView,
    id: DocumentIdentity,
    name: &str,
    version: u32,
) -> Result<&'a ConnectorDocumentVersion, MvDocumentError> {
    let value = view
        .versions
        .get(&id)
        .ok_or_else(|| MvDocumentError::Contract(format!("missing MV document version {name}")))?;
    if value.identity() != id
        || value.owner() != MV_DOCUMENT_OWNER
        || value.name() != name
        || value.format() != &format(name, version)?
    {
        return contract(format!("invalid MV document version {name}"));
    }
    Ok(value)
}
fn decode_keyed_binding_documents(
    view: &KeyedDocumentView,
    binding_id: DocumentIdentity,
    configuration_content: &[u8],
    publication_payload: Option<&[u8]>,
    budget: PersistenceDecodeBudget,
) -> Result<(DefinitionDocument, InterpretationDocument, MvKeyedVersions), MvDocumentError> {
    let b = exact_version(view, binding_id, "binding", 1)?;
    let d_id = *b
        .references()
        .get("definition")
        .ok_or_else(|| MvDocumentError::Contract("binding lacks definition reference".into()))?;
    let l_id = *b.references().get("interpretation").ok_or_else(|| {
        MvDocumentError::Contract("binding lacks interpretation reference".into())
    })?;
    if b.references().len() != 2 {
        return contract("binding has unexpected references");
    }
    let d = exact_version(view, d_id, "definition", 1)?;
    let l = exact_version(view, l_id, "interpretation", 2)?;
    if !d.references().is_empty()
        || l.references() != &BTreeMap::from([("definition".into(), d_id)])
    {
        return contract("invalid D/L document reference closure");
    }
    preflight_keyed_document_set(
        d.content(),
        l.content(),
        b.content(),
        configuration_content,
        publication_payload,
        budget,
    )?;
    let definition = decode_definition(d.content(), budget)?;
    let interpretation = decode_keyed_interpretation(l.content(), budget)?;
    let binding = decode_binding(b.content(), budget)?;
    super::validation::validate_definition_interpretation(
        &definition,
        revision(d_id),
        &interpretation,
    )?;
    if interpretation.target.object_id.as_bytes() != view.object_id.as_bytes().as_ref() {
        return contract("interpretation binds a different target object");
    }
    if binding.data_versions.len() != definition.relation_occurrences.len() {
        return contract("binding occurrence count differs from definition");
    }
    let versions = MvKeyedVersions {
        definition: d.clone(),
        interpretation: l.clone(),
        binding: b.clone(),
        binding_document: binding,
    };
    Ok((definition, interpretation, versions))
}

/// Strict root facts used solely for deletion of an already admitted target.
/// This is neither a current data projection nor a readmission capability.
pub(crate) struct MvKeyedDropDocuments {
    pub target: ManagedMvTarget,
    pub owner: DeploymentOwner,
    pub incarnation: ProcessIncarnation,
    pub versions: MvKeyedVersions,
    pub configuration_revision: DocumentRevision,
}

pub(crate) fn decode_keyed_drop_documents(
    view: &KeyedDocumentView,
    catalog: CatalogHandle,
    budget: PersistenceDecodeBudget,
) -> Result<MvKeyedDropDocuments, MvDocumentError> {
    if view.metadata_version.snapshot_id() != view.current_snapshot
        || catalog.catalog_name() != &view.table.instance_id
        || view.owner.instance_id != view.table.instance_id
    {
        return contract("MV DROP keyed catalog or current metadata scope mismatch");
    }
    let target = ManagedMvTarget::try_new(catalog, view.table.clone(), view.object_id.clone())
        .map_err(|error| MvDocumentError::Contract(error.to_string()))?;
    let management = view
        .slots
        .get(&management_slot_name()?)
        .ok_or_else(|| MvDocumentError::Contract("missing MV management slot".into()))?;
    let (owner, incarnation) = decode_management_slot(&management.value)?;
    let configuration = view
        .slots
        .get(&configuration_slot_name()?)
        .ok_or_else(|| MvDocumentError::Contract("missing MV configuration slot".into()))?;
    if configuration.value.format != format("configuration", 1)? {
        return contract("unsupported MV configuration slot format");
    }
    decode_configuration(&configuration.value.content, budget)?;
    let root = view
        .roots
        .get(MV_DOCUMENT_OWNER)
        .filter(|root| !root.dangling)
        .ok_or_else(|| MvDocumentError::Contract("MV binding root is absent or dangling".into()))?;
    let (_, _, versions) = decode_keyed_binding_documents(
        view,
        root.identity,
        &configuration.value.content,
        None,
        budget,
    )?;
    Ok(MvKeyedDropDocuments {
        target,
        owner,
        incarnation,
        versions,
        configuration_revision: DocumentRevision::from_canonical_bytes(
            &configuration.value.content,
        ),
    })
}

pub fn decode_current_keyed_documents(
    view: KeyedDocumentView,
    catalog: CatalogHandle,
    budget: PersistenceDecodeBudget,
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
    let configuration = decode_configuration(&c.value.content, budget)?;
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
    if let Some(record) = record {
        let bytes = "novarocks.doc.v2.record.novarocks.mv~publication".len()
            + 2
            + 64
            + 1
            + (record.payload.len() * 4).div_ceil(3);
        if bytes > KEYED_DOCUMENT_RECORD_BYTES {
            return contract(format!(
                "snapshot record publication uses {bytes} bytes, exceeding limit {KEYED_DOCUMENT_RECORD_BYTES}"
            ));
        }
    }
    let (definition, interpretation, versions) = decode_keyed_binding_documents(
        &view,
        binding_id,
        &c.value.content,
        record.map(|r| r.payload.as_ref()),
        budget,
    )?;
    let publication = record
        .map(|r| {
            decode_publication_payload(
                &r.payload,
                &definition,
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
    lease: &ConnectorDocumentStorageLease,
    request: KeyedDocumentObservationRequest,
    budget: PersistenceDecodeBudget,
) -> Result<MvKeyedObservedCurrentDocuments, MvDocumentError> {
    let catalog = request.catalog_handle.clone();
    let view = lease
        .observe_keyed_documents(request)
        .map_err(MvDocumentError::KeyedStorage)?;
    decode_current_keyed_documents(view, catalog, budget)
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
    lease: &ConnectorDocumentStorageLease,
    request: KeyedDocumentObservationRequest,
    budget: PersistenceDecodeBudget,
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
    let observed = decode_current_keyed_documents(view, catalog, budget)?;
    Ok((observed, convergence))
}

pub async fn observe_current_keyed_documents_with_management_async(
    lease: &ConnectorDocumentStorageLease,
    request: KeyedDocumentObservationRequest,
    budget: PersistenceDecodeBudget,
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
    let observed = decode_current_keyed_documents(view, catalog, budget)?;
    Ok((observed, convergence))
}

pub(crate) fn observe_current_keyed_view_with_management(
    lease: &ConnectorDocumentStorageLease,
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
    lease: &ConnectorDocumentStorageLease,
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
    let mut formats = Vec::new();
    for occurrence in &f.definition.relation_occurrences {
        let raw = super::exact_revision::restore_persisted_object(&occurrence.object_id).unwrap();
        let source = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &raw,
            Some(17),
        )
        .unwrap();
        formats.push(DataVersionFormat::from_issued_fact(source.data_version()).unwrap());
        let (_, native_data_version) =
            super::exact_revision::persist_exact_connector_revision(&source).unwrap();
        inputs.push(PublicationInput {
            relation_occurrence_id: occurrence.occurrence_id,
            object_id: occurrence.object_id.clone(),
            native_data_version,
        });
    }
    let versions = build_keyed_versions(
        &f.definition,
        &f.interpretation,
        BindingDocument {
            data_versions: formats,
        },
    )
    .unwrap();
    let publication = KeyedPublicationDocument {
        publication_prepared_at_ms: 42,
        publication_id: super::identity::PublicationIdentity::try_new(vec![0x11; 16]).unwrap(),
        definition_revision: versions.definition_revision(),
        interpretation_revision: versions.interpretation_revision(),
        inputs,
        logical_result_rows: Some(1),
    };
    let records = if published {
        keyed_publication_document_set(&versions, &f.definition, &publication)
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
            decode_current_keyed_documents(
                view.clone(),
                catalog.clone(),
                PersistenceDecodeBudget::default()
            )
            .unwrap()
            .is_never_published()
        );
        view.current_snapshot_file_totals.data_files = None;
        assert!(!never_published(&view));
        assert!(
            decode_current_keyed_documents(
                view.clone(),
                catalog.clone(),
                PersistenceDecodeBudget::default()
            )
            .is_err()
        );
        view.current_snapshot_file_totals.data_files = Some(1);
        assert!(
            decode_current_keyed_documents(view, catalog, PersistenceDecodeBudget::default())
                .is_err()
        );
    }
    #[test]
    fn copied_record_projects_its_current_carrier_and_c_revision_ignores_writer() {
        let (catalog, view) = fixture(Some(17), true);
        let first = decode_current_keyed_documents(
            view.clone(),
            catalog.clone(),
            PersistenceDecodeBudget::default(),
        )
        .unwrap();
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
        let second =
            decode_current_keyed_documents(later, catalog, PersistenceDecodeBudget::default())
                .unwrap();
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
    fn semantic_definition_identity_covers_the_document_header() {
        let (_, view) = fixture(None, false);
        let b = &view.versions[&view.roots[MV_DOCUMENT_OWNER].identity];
        let id = b.references()["definition"];
        let d = &view.versions[&id];
        assert_ne!(
            id.to_bytes().as_slice(),
            DocumentRevision::from_canonical_bytes(d.content()).as_bytes()
        );
        let bytes = decode_keyed_interpretation(
            view.versions[&b.references()["interpretation"]].content(),
            PersistenceDecodeBudget::default(),
        )
        .unwrap();
        assert_eq!(bytes.definition_revision, revision(id));
    }
}
