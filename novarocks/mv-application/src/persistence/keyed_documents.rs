// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! MV-owned keyed document mapping. Legacy documents remain only for the
//! staged FE migration; this path never converts a keyed record into legacy P.

use super::{
    binding_envelope::inspect_binding,
    codec::keyed::*,
    codec::*,
    documents::MvDocumentError,
    identity::{DocumentRevision, PublicationIdentity},
    resolution::{
        ApplicablePublication, current_publication_records, resolve_applicable_publication,
    },
};
use crate::management::{DeploymentOwner, ManagedMvTarget, ProcessIncarnation};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::{BTreeMap, BTreeSet};

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

pub fn keyed_recovery_publication_document_set(
    versions: &MvKeyedVersions,
    publication: &KeyedPublicationDocument,
    recovery: &RecoveryRecord,
) -> Result<KeyedPublication, MvDocumentError> {
    validate_recovery_publication(recovery, publication)?;
    let mut keyed = keyed_publication_document_set(versions, publication)?;
    keyed.records.push(recovery.encode()?);
    Ok(keyed)
}

fn validate_recovery_publication(
    recovery: &RecoveryRecord,
    publication: &KeyedPublicationDocument,
) -> Result<(), MvDocumentError> {
    if publication.publication_id.as_bytes() != recovery.operation.to_bytes()
        || recovery.recovered_from.iter().any(|source| {
            let prior = match source {
                RecoverySource::Invalid { publication }
                | RecoverySource::Pending { publication, .. } => publication,
            };
            prior == &publication.publication_id
        })
    {
        return contract("MV recovery record does not bind the new publication");
    }
    Ok(())
}

fn decode_recovery_publication(
    record: Option<&ConnectorSnapshotRecord>,
    publication: Option<&KeyedPublicationDocument>,
) -> Result<Option<RecoveryRecord>, MvDocumentError> {
    let Some(record) = record else {
        return Ok(None);
    };
    let Some(publication) = publication else {
        return contract("MV recovery record must accompany its publication");
    };
    let recovery = RecoveryRecord::decode(record)?;
    validate_recovery_publication(&recovery, publication)?;
    Ok(Some(recovery))
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
    pub recovery: Option<RecoveryRecord>,
    pub publication_revision: Option<DocumentRevision>,
    /// The snapshot carrying the current record, including maintenance/slot copies.
    pub publication_snapshot: Option<i64>,
    applicable_publication: ApplicablePublication,
    markers: Vec<(ConnectorMarkerKey, Marker)>,
}
impl MvKeyedObservedCurrentDocuments {
    pub fn decoded_markers(&self) -> Result<Vec<(ConnectorMarkerKey, Marker)>, MvDocumentError> {
        decode_current_mv_markers(&self.view)
    }

    pub fn root_binding_revision(&self) -> DocumentRevision {
        revision(self.versions.binding.identity())
    }

    pub fn is_never_published(&self) -> bool {
        matches!(
            self.applicable_publication,
            ApplicablePublication::NeverPublished(_)
        )
    }

    pub fn applicable_publication(&self) -> &ApplicablePublication {
        &self.applicable_publication
    }

    pub fn markers(&self) -> &[(ConnectorMarkerKey, Marker)] {
        &self.markers
    }

    pub fn eligibility(&self) -> MarkerEligibility<'_> {
        marker_eligibility(self.applicable_publication.evidence(), self.markers())
    }

    pub fn servable_publication(&self) -> Option<&KeyedPublicationDocument> {
        if self.eligibility().rewrite_serving {
            self.publication.as_ref()
        } else {
            None
        }
    }

    pub fn publication_evidence<'a>(
        &'a self,
        root: &'a DocumentRevision,
    ) -> PublicationEvidence<'a> {
        match self.applicable_publication.evidence() {
            PublicationEvidence::Unpublished(binding) if binding != root => {
                PublicationEvidence::Unknown
            }
            evidence => evidence,
        }
    }

    fn accelerator_source_revision(&self) -> super::definition::MvAcceleratorSourceRevision {
        use super::definition::{
            MvAcceleratorCommittedVersionRevision, MvAcceleratorSourceRevision,
        };
        let metadata_version =
            MvAcceleratorCommittedVersionRevision::from_committed(&self.view.metadata_version);
        MvAcceleratorSourceRevision {
            target: self.view.table.clone(),
            target_object_id: self.view.object_id.clone(),
            metadata_version: metadata_version.clone(),
            definition_revision: self.versions.definition_revision(),
            interpretation_revision: self.versions.interpretation_revision(),
            publication_revision: self.publication_revision,
            publication_output_version: self.publication_snapshot.map(|_| metadata_version),
            configuration_revision: self.configuration_revision,
            deployment_owner: self.deployment_owner.clone(),
            process_incarnation: self.process_incarnation.clone(),
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
/// Form the marker mutation for a proved refusal of this exact original
/// incremental publication. The caller retains the original write outcome
/// and uses the existing scoped marker channel. Ancestor observations never
/// produce a known-uncommitted write completion.
pub fn pending_release_for_publication(
    current: &KeyedDocumentView,
    intent: &crate::publication::MvRefreshPublicationIntent,
    previous: &crate::persistence::identity::PublicationIdentity,
    original: &LakePublicationTerminal,
) -> Result<Option<novarocks_spi::connector::KeyedMarkerWrite>, MvDocumentError> {
    pending_release_under_refusal(current, intent, previous, original, None)
}

/// Recovery has independently proved that the original Pending baseline is
/// equivalent to current main. Only a different main can fence the old write.
pub fn pending_release_after_recovery_refusal(
    current: &KeyedDocumentView,
    intent: &crate::publication::MvRefreshPublicationIntent,
    previous: &crate::persistence::identity::PublicationIdentity,
    original: &LakePublicationTerminal,
    pending: &crate::persistence::codec::PendingEvidence,
) -> Result<Option<novarocks_spi::connector::KeyedMarkerWrite>, MvDocumentError> {
    pending_release_under_refusal(current, intent, previous, original, Some(pending))
}

fn pending_release_under_refusal(
    current: &KeyedDocumentView,
    intent: &crate::publication::MvRefreshPublicationIntent,
    previous: &crate::persistence::identity::PublicationIdentity,
    original: &LakePublicationTerminal,
    recovering: Option<&crate::persistence::codec::PendingEvidence>,
) -> Result<Option<novarocks_spi::connector::KeyedMarkerWrite>, MvDocumentError> {
    use crate::publication::MvRefreshPublicationTechnique;
    if original.disposition() != LakePublicationDisposition::KnownUncommitted
        || original.header().family() != LakePublicationFamily::MaterializedViewRefresh
        || original.header().publication_id() != intent.publication_id()
        || original.target().catalog() != current.table.instance_id.as_str()
        || original.target().namespace() != current.table.namespace.as_ref()
        || original.target().table() != Some(current.table.table.as_ref())
        || !matches!(
            intent.technique(),
            MvRefreshPublicationTechnique::Incremental
                | MvRefreshPublicationTechnique::MetadataOnly
        )
        || current.object_id != *intent.target_object_id()
        || &current.table != intent.admission().target()
        || current.owner != *intent.admission().owner()
    {
        return Ok(None);
    }
    let Some(base) = intent.expected_target_snapshot_id() else {
        return Ok(None);
    };
    let Some(storage_uuid) = std::str::from_utf8(current.object_id.as_bytes())
        .ok()
        .and_then(|name| uuid::Uuid::parse_str(name).ok())
    else {
        return Ok(None);
    };
    if recovering.is_some_and(|pending| {
        pending.baseline_snapshot == base || current.current_snapshot != Some(base)
    }) {
        return Ok(None);
    }
    let attempt = recovering.map_or_else(
        || intent.publication_id().to_bytes(),
        |pending| pending.attempt,
    );
    let mut exact = false;
    for (key, value) in current
        .markers
        .iter()
        .filter(|(key, _)| key.owner == MV_DOCUMENT_OWNER && key.name == "pending")
    {
        let Marker::Pending {
            publication,
            evidence,
        } = Marker::decode(key, value)?
        else {
            unreachable!("the marker codec validates its key and body");
        };
        if publication == *previous
            && evidence.attempt == attempt
            && evidence.storage_uuid == *storage_uuid.as_bytes()
            && recovering.map_or(evidence.baseline_snapshot == base, |pending| {
                pending == &evidence
            })
        {
            exact = true;
            break;
        }
    }
    if !exact {
        return Ok(None);
    }
    let (key, value) = Marker::PendingReleased {
        publication: previous.clone(),
        attempt,
    }
    .encode()?;
    if current.markers.get(&key) == Some(&value) {
        return Ok(None);
    }
    Ok(Some(novarocks_spi::connector::KeyedMarkerWrite {
        markers: BTreeMap::from([(key, value)]),
    }))
}

pub fn released_pending_marker_keys(
    current: &MvKeyedObservedCurrentDocuments,
) -> Result<BTreeSet<ConnectorMarkerKey>, MvDocumentError> {
    let decoded = current.decoded_markers()?;
    let releases: BTreeMap<_, _> = decoded
        .iter()
        .filter_map(|(key, marker)| match marker {
            Marker::PendingReleased {
                publication,
                attempt,
            } => Some(((publication, *attempt), key)),
            _ => None,
        })
        .collect();
    let mut remove = BTreeSet::new();
    for (key, marker) in &decoded {
        if let Marker::Pending {
            publication,
            evidence,
        } = marker
            && let Some(release) = releases.get(&(publication, evidence.attempt))
        {
            remove.insert(key.clone());
            remove.insert((*release).clone());
        }
    }
    Ok(remove)
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
    let records = current_publication_records(view.current_snapshot, &view.current_records)?;
    let binding = match records.publication {
        Some(record) => record.binding,
        None => view
            .roots
            .get(MV_DOCUMENT_OWNER)
            .filter(|root| !root.dangling)
            .map(|root| root.identity),
    }
    .ok_or_else(|| MvDocumentError::Contract("MV has no exact Current binding envelope".into()))?;
    inspect_binding(view, binding)?.require_current()
}

/// Transitional entry point for consumers which still require a publication
/// or first-refresh witness. Use the layered observation for new callers.
pub fn decode_current_keyed_documents(
    view: KeyedDocumentView,
    catalog: CatalogHandle,
) -> Result<MvKeyedObservedCurrentDocuments, MvDocumentError> {
    let observed = resolve_current_keyed_documents(view, catalog)?;
    if matches!(
        observed.applicable_publication(),
        ApplicablePublication::RetiredPublication(_) | ApplicablePublication::Unattributed
    ) {
        return contract(
            "current snapshot contains data but no MV publication record (external modification)",
        );
    }
    Ok(observed)
}

/// Resolve one historical publication from its explicit snapshot pointer.
pub struct MvHistoricalPublication {
    pub definition: DefinitionDocument,
    pub interpretation: InterpretationDocument,
    pub versions: MvKeyedVersions,
    pub publication: KeyedPublicationDocument,
    pub record: novarocks_spi::connector::ConnectorSnapshotRecord,
    snapshot_id: i64,
    current_table: ConnectorTableIdentity,
    current_object: ConnectorTableObjectId,
    current_version: ConnectorCommittedVersion,
}

/// A historical P authorized as a refresh input by one exact Current and its
/// original Pending. This value cannot stand in for a Current projection.
#[derive(Clone, Debug)]
pub struct MvStrictRecoveryPublication {
    current_source: super::definition::MvAcceleratorSourceRevision,
    current_version: ConnectorCommittedVersion,
    current_binding: DocumentIdentity,
    current_mapping: DocumentIdentity,
    current_applicability: ApplicablePublication,
    baseline_publication: KeyedPublicationDocument,
}

impl MvStrictRecoveryPublication {
    pub fn baseline_publication(&self) -> &KeyedPublicationDocument {
        &self.baseline_publication
    }

    pub fn validate_current_projection(
        &self,
        current: &MvKeyedDocumentProjection,
    ) -> Result<(), MvDocumentError> {
        let applicable = match &self.current_applicability {
            ApplicablePublication::Bound(id) => current
                .publication()
                .is_some_and(|publication| &publication.publication_id == id),
            ApplicablePublication::Unattributed => current.publication().is_none(),
            _ => false,
        };
        if self.current_source != current.source
            || self.current_version != current.metadata_version
            || self.current_binding != current.versions.binding.identity()
            || self.current_mapping != current.versions.mapping.identity()
            || current.data_eligibility != Some(MvCurrentDataEligibility::StrictRecovery)
            || !applicable
        {
            return contract("MV strict recovery projection differs from its original Current");
        }
        Ok(())
    }
}

impl MvHistoricalPublication {
    /// Freeze only the document portion of replay. The caller also proves
    /// original source windows and target data equivalence before execution.
    pub fn for_strict_recovery(
        &self,
        current: &MvKeyedObservedCurrentDocuments,
        publication: &PublicationIdentity,
        evidence: &PendingEvidence,
    ) -> Result<MvStrictRecoveryPublication, MvDocumentError> {
        let eligibility = current.eligibility();
        let mut pending = eligibility.active.iter().filter_map(|marker| match marker {
            Marker::Pending {
                publication,
                evidence,
            } => Some((publication, evidence)),
            _ => None,
        });
        let applicable = match current.applicable_publication() {
            ApplicablePublication::Bound(id) => id == publication,
            ApplicablePublication::Unattributed => true,
            _ => false,
        };
        if !eligibility.strict_recovery_required
            || !applicable
            || pending.next() != Some((publication, evidence))
            || pending.next().is_some()
            || self.current_table != current.view.table
            || self.current_object != current.view.object_id
            || self.current_version != current.view.metadata_version
            || self.snapshot_id != evidence.baseline_snapshot
            || self.publication.publication_id != *publication
            || self.versions != current.versions
            || revision(self.versions.mapping.identity()) != evidence.mapping_revision
            || self.interpretation.is_aggregate()
            || current.interpretation.is_aggregate()
        {
            return contract("MV strict recovery has a foreign Current, baseline or Pending");
        }
        Ok(MvStrictRecoveryPublication {
            current_source: current.accelerator_source_revision(),
            current_version: current.view.metadata_version.clone(),
            current_binding: current.versions.binding.identity(),
            current_mapping: current.versions.mapping.identity(),
            current_applicability: current.applicable_publication().clone(),
            baseline_publication: self.publication.clone(),
        })
    }
}

pub fn resolve_historical_keyed_publication(
    full: &novarocks_spi::connector::FullKeyedDocumentView,
    snapshot: i64,
) -> Result<MvHistoricalPublication, MvDocumentError> {
    let mut carriers = full
        .snapshots
        .iter()
        .filter(|entry| entry.snapshot_id == snapshot);
    let carrier = carriers.next().ok_or_else(|| {
        MvDocumentError::Contract("Pending baseline snapshot is no longer retained".into())
    })?;
    if carriers.next().is_some() {
        return contract("Pending baseline snapshot has duplicate observation entries");
    }
    let records = current_publication_records(Some(snapshot), &carrier.records)?;
    let original = records.publication.ok_or_else(|| {
        MvDocumentError::Contract("Pending baseline has no original MV publication record".into())
    })?;
    let binding = original.binding.ok_or_else(|| {
        MvDocumentError::Contract("Pending baseline publication has no binding identity".into())
    })?;
    let configuration = full
        .current
        .slots
        .get(&configuration_slot_name()?)
        .ok_or_else(|| {
            MvDocumentError::Contract("Pending target has no configuration slot".into())
        })?;
    let (definition, interpretation, versions) =
        decode_keyed_binding_documents(&full.current, binding, &configuration.value.content)?;
    let publication = decode_publication_payload(
        &original.payload,
        versions.definition_revision(),
        versions.interpretation_revision(),
        &versions.binding_document,
    )?;
    decode_recovery_publication(records.recovery, Some(&publication))?;
    Ok(MvHistoricalPublication {
        definition,
        interpretation,
        versions,
        publication,
        record: original.clone(),
        snapshot_id: snapshot,
        current_table: full.current.table.clone(),
        current_object: full.current.object_id.clone(),
        current_version: full.current.metadata_version.clone(),
    })
}

/// Decodes exact management documents from the provider's storage observation.
/// Whether the data publication can serve a rewrite is decided independently.
// Design: ADR-0278 (docs/adr/ADR-0278-mv-maintains-visible-tuples-and-replays-verification.md)
pub fn resolve_current_keyed_documents(
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
    let records = current_publication_records(view.current_snapshot, &view.current_records)?;
    let record = records.publication;
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
    let recovery = decode_recovery_publication(records.recovery, publication.as_ref())?;
    let publication_revision =
        record.map(|r| DocumentRevision::from_canonical_bytes(&snapshot_record_frame(r)));
    let publication_snapshot = record.and(view.current_snapshot);
    let markers = decode_current_mv_markers(&view)?;
    let applicable_publication = resolve_applicable_publication(
        &view,
        publication.as_ref(),
        records.retirement,
        revision(versions.binding.identity()),
        &markers,
    )?;
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
        recovery,
        publication_revision,
        publication_snapshot,
        applicable_publication,
        markers,
    })
}
fn decode_current_mv_markers(
    view: &KeyedDocumentView,
) -> Result<Vec<(ConnectorMarkerKey, Marker)>, MvDocumentError> {
    view.markers
        .iter()
        .filter(|(key, _)| key.owner == MV_DOCUMENT_OWNER)
        .map(|(key, value)| Ok((key.clone(), Marker::decode(key, value)?)))
        .collect()
}

pub fn observe_current_keyed_documents(
    lease: &ConnectorDocumentStorage,
    request: KeyedDocumentObservationRequest,
) -> Result<MvKeyedObservedCurrentDocuments, MvDocumentError> {
    let catalog = request.catalog_handle.clone();
    let view = lease
        .observe_keyed_documents(request)
        .map_err(MvDocumentError::KeyedStorage)?;
    resolve_current_keyed_documents(view, catalog)
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

/// Last exact Current data eligibility that the Accelerator observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvCurrentDataEligibility {
    Servable,
    NeverPublished,
    StrictRecovery,
    ExplicitRebuild,
    Unattributed,
    RewriteBlocked,
}

impl MvCurrentDataEligibility {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Servable => "SERVABLE",
            Self::NeverPublished => "NEVER_PUBLISHED",
            Self::StrictRecovery => "STRICT_RECOVERY_REQUIRED",
            Self::ExplicitRebuild => "EXPLICIT_REBUILD_REQUIRED",
            Self::Unattributed => "UNATTRIBUTED",
            Self::RewriteBlocked => "REWRITE_BLOCKED",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        [
            Self::Servable,
            Self::NeverPublished,
            Self::StrictRecovery,
            Self::ExplicitRebuild,
            Self::Unattributed,
            Self::RewriteBlocked,
        ]
        .into_iter()
        .find(|state| state.as_str() == value)
        .ok_or_else(|| format!("unknown cached MV Current data eligibility {value}"))
    }
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
    /// None means a local projection written before this provider witness was retained.
    pub(crate) never_published: Option<bool>,
    pub(crate) data_eligibility: Option<MvCurrentDataEligibility>,
}
impl MvKeyedDocumentProjection {
    pub fn try_from_current(observed: MvKeyedObservedCurrentDocuments) -> Result<Self, String> {
        let source = observed.accelerator_source_revision();
        let never_published = observed.is_never_published();
        let eligibility = observed.eligibility();
        let data_eligibility = if eligibility.explicit_rebuild_required {
            MvCurrentDataEligibility::ExplicitRebuild
        } else if eligibility.strict_recovery_required {
            MvCurrentDataEligibility::StrictRecovery
        } else if eligibility.rewrite_serving {
            MvCurrentDataEligibility::Servable
        } else if never_published && !eligibility.automatic_reuse_blocked {
            MvCurrentDataEligibility::NeverPublished
        } else if matches!(
            observed.applicable_publication(),
            ApplicablePublication::Unattributed
        ) {
            MvCurrentDataEligibility::Unattributed
        } else {
            MvCurrentDataEligibility::RewriteBlocked
        };
        Ok(Self {
            source,
            metadata_version: observed.view.metadata_version,
            definition: observed.definition,
            interpretation: observed.interpretation,
            configuration: observed.configuration,
            versions: observed.versions,
            publication: observed.publication,
            file_totals: observed.view.current_snapshot_file_totals,
            never_published: Some(never_published),
            data_eligibility: Some(data_eligibility),
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
    pub const fn data_eligibility(&self) -> Option<MvCurrentDataEligibility> {
        self.data_eligibility
    }

    pub fn never_published(&self) -> bool {
        self.never_published == Some(true)
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
        match self.never_published {
            Some(true) if !never_published(current) => {
                contract("MV first-refresh witness no longer holds")
            }
            Some(false) if current.current_snapshot != self.metadata_version.snapshot_id() => {
                contract("MV current carrier changed before refresh")
            }
            None if current.metadata_version != self.metadata_version => {
                contract("MV cached current carrier requires a fresh observation")
            }
            _ => Ok(()),
        }
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
    let observed = resolve_current_keyed_documents(view, catalog)?;
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
    let observed = resolve_current_keyed_documents(view, catalog)?;
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
    keyed_view_from_fixture(
        crate::persistence::test_support::MvDocumentFixture::new(),
        snapshot,
        published,
    )
}

#[cfg(any(test, novarocks_dev_controls))]
fn keyed_view_from_fixture(
    f: crate::persistence::test_support::MvDocumentFixture,
    snapshot: Option<i64>,
    published: bool,
) -> (CatalogHandle, KeyedDocumentView) {
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
    fn pending_release_requires_the_original_refusal_and_exact_attempt_and_scope() {
        use crate::persistence::identity::{
            NativeDataVersion, ObjectIdentity, PublicationIdentity,
        };
        use crate::publication::{MvRefreshPublicationIntent, MvRefreshPublicationTechnique};

        let uuid = uuid::Uuid::now_v7();
        let object_bytes = uuid.to_string().into_bytes();
        let mut documents = crate::persistence::test_support::MvDocumentFixture::new();
        documents.interpretation.target.object_id =
            ObjectIdentity::try_new(object_bytes.clone()).unwrap();
        documents.object_id = ConnectorTableObjectId::try_new(Bytes::from(object_bytes)).unwrap();
        let (catalog, mut view) = keyed_view_from_fixture(documents, Some(17), true);
        let previous = decode_current_keyed_documents(view.clone(), catalog.clone())
            .unwrap()
            .publication
            .unwrap()
            .publication_id;
        let attempt = LakePublicationId::new_v7();
        let admission = crate::test_admission::publication_admission(
            view.table.instance_id.as_str(),
            &view.table.namespace,
            &view.table.table,
            attempt,
            &view.object_id,
        );
        view.owner = admission.owner().clone();
        let intent = MvRefreshPublicationIntent::try_new(
            attempt,
            view.object_id.clone(),
            Some(17),
            admission,
            MvRefreshPublicationTechnique::Incremental,
            vec![
                crate::publication::MvRefreshPublicationBase::try_new(
                    0,
                    "ice.sales.base".into(),
                    view.object_id.clone(),
                    Some(3),
                    4,
                )
                .unwrap(),
            ],
            "definition".into(),
            view.table.instance_id.as_str().into(),
            view.table.namespace.to_string(),
            view.table.table.to_string(),
        )
        .unwrap();
        let pending = |publication: PublicationIdentity, attempt: [u8; 16]| {
            let rev = DocumentRevision::from_canonical_bytes(b"scope");
            Marker::Pending {
                publication,
                evidence: PendingEvidence {
                    attempt,
                    refresh_identity: uuid::Uuid::now_v7(),
                    source_windows: vec![SourceVersionWindow {
                        occurrence: 0,
                        from: NativeDataVersion::try_new(vec![1]).unwrap(),
                        to: NativeDataVersion::try_new(vec![2]).unwrap(),
                    }],
                    storage_uuid: *uuid.as_bytes(),
                    baseline_snapshot: 17,
                    mapping_revision: rev,
                    verification_requirement: PendingVerificationRequirement::content_locate(
                        10, 20,
                    )
                    .unwrap(),
                },
            }
            .encode()
            .unwrap()
        };
        let (orig_key, orig_value) = pending(previous.clone(), attempt.to_bytes());
        let (other_key, other_value) = pending(previous.clone(), [3; 16]);
        view.markers.extend([
            (orig_key.clone(), orig_value),
            (other_key.clone(), other_value),
        ]);
        let terminal = |id, disposition| {
            LakePublicationTerminal::new(
                LakePublicationMarkerHeader::new(
                    id,
                    LakePublicationFamily::MaterializedViewRefresh,
                ),
                LakePublicationTarget::try_new(
                    view.table.instance_id.as_str().into(),
                    view.table.namespace.to_string(),
                    Some(view.table.table.to_string()),
                    None,
                )
                .unwrap(),
                disposition,
                LakePublicationNextAction::InspectPublishedState,
                None,
            )
        };
        assert!(
            pending_release_for_publication(
                &view,
                &intent,
                &previous,
                &terminal(attempt, LakePublicationDisposition::CommitUnknown),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            pending_release_for_publication(
                &view,
                &intent,
                &previous,
                &terminal(
                    LakePublicationId::new_v7(),
                    LakePublicationDisposition::KnownUncommitted
                ),
            )
            .unwrap()
            .is_none()
        );
        let refused = terminal(attempt, LakePublicationDisposition::KnownUncommitted);
        let release = pending_release_for_publication(&view, &intent, &previous, &refused)
            .unwrap()
            .expect("only the definitively refused original attempt releases");
        let release_key = release.markers.keys().next().unwrap().clone();
        view.markers.extend(release.markers);
        assert!(
            pending_release_for_publication(&view, &intent, &previous, &refused)
                .unwrap()
                .is_none()
        );
        let current = decode_current_keyed_documents(view, catalog).unwrap();
        assert_eq!(
            released_pending_marker_keys(&current).unwrap(),
            BTreeSet::from([orig_key, release_key]),
        );
        assert!(current.view.markers.contains_key(&other_key));
    }

    #[test]
    fn strict_recovery_uses_exact_historical_publication_for_unattributed_current() {
        use crate::persistence::exact_revision::{
            persist_exact_connector_revision, restore_persisted_object,
        };
        use crate::persistence::test_support::{
            observed_current_keyed, union_all_keyed_projection_fixture,
        };

        let storage_uuid = uuid::Uuid::now_v7();
        let mut fixture = union_all_keyed_projection_fixture(
            crate::product::MvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(17),
            2,
            2,
            0,
        );
        fixture.object_id =
            ConnectorTableObjectId::try_new(Bytes::from(storage_uuid.to_string())).unwrap();
        fixture.interpretation.apply_key = None;
        fixture.interpretation.branches.clear();
        fixture.interpretation.target.fields.retain(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::Output(_)
            )
        });
        let published = fixture.build().unwrap();
        let publication = published.publication().unwrap();
        let catalog = CatalogHandle::new(
            published.source_revision().target.instance_id.clone(),
            CatalogVersion::from_bytes([1; 32]),
        );
        let mut current = observed_current_keyed(&published, catalog.clone()).view;
        let records = std::mem::take(&mut current.current_records);
        current.current_snapshot = Some(18);
        current.metadata_version =
            ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata-18"), Some(18))
                .unwrap();
        let evidence = PendingEvidence {
            attempt: LakePublicationId::new_v7().to_bytes(),
            refresh_identity: uuid::Uuid::now_v7(),
            source_windows: publication
                .inputs
                .iter()
                .map(|input| {
                    let object = restore_persisted_object(&input.object_id).unwrap();
                    let to = ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
                        ConnectorProviderId::parse("iceberg").unwrap(),
                        &object,
                        Some(19),
                    )
                    .unwrap();
                    SourceVersionWindow {
                        occurrence: input.relation_occurrence_id,
                        from: input.native_data_version.clone(),
                        to: persist_exact_connector_revision(&to).unwrap().1,
                    }
                })
                .collect(),
            storage_uuid: *storage_uuid.as_bytes(),
            baseline_snapshot: 17,
            mapping_revision: revision(published.versions().mapping.identity()),
            verification_requirement: PendingVerificationRequirement::content_locate(10, 20)
                .unwrap(),
        };
        current.markers.extend([Marker::Pending {
            publication: publication.publication_id.clone(),
            evidence: evidence.clone(),
        }
        .encode()
        .unwrap()]);
        let observed = resolve_current_keyed_documents(current.clone(), catalog.clone()).unwrap();
        assert_eq!(
            observed.applicable_publication(),
            &ApplicablePublication::Unattributed
        );
        let full = FullKeyedDocumentView {
            token: FullObservationToken::try_new(
                current.owner.clone(),
                current.table.clone(),
                current.object_id.clone(),
                current.metadata_version.clone(),
                Bytes::from_static(b"full-observation"),
            )
            .unwrap(),
            snapshots: [16, 17, 18]
                .into_iter()
                .map(|snapshot_id| KeyedSnapshotObservation {
                    snapshot_id,
                    parent: Some(snapshot_id - 1),
                    records: if snapshot_id == 18 {
                        Vec::new()
                    } else {
                        records.clone()
                    },
                    slot_storage_effect_digest: None,
                    full_refresh_stage_witness: None,
                    managed_repartition_effect: None,
                })
                .collect(),
            refs: BTreeMap::from([("main".into(), 18)]),
            diagnostics: Vec::new(),
            current,
        };
        let baseline = resolve_historical_keyed_publication(&full, 17).unwrap();
        let recovery = baseline
            .for_strict_recovery(&observed, &publication.publication_id, &evidence)
            .unwrap();
        assert_eq!(recovery.baseline_publication(), publication);
        let projection = MvKeyedDocumentProjection::try_from_current(observed.clone()).unwrap();
        assert!(projection.publication().is_none());
        assert_eq!(
            projection.data_eligibility(),
            Some(MvCurrentDataEligibility::StrictRecovery)
        );
        recovery.validate_current_projection(&projection).unwrap();
        assert!(
            resolve_historical_keyed_publication(&full, 16)
                .unwrap()
                .for_strict_recovery(&observed, &publication.publication_id, &evidence)
                .is_err()
        );

        let mut foreign = full.current;
        foreign.metadata_version = ConnectorCommittedVersion::try_new(
            Bytes::from_static(b"foreign-metadata-18"),
            Some(18),
        )
        .unwrap();
        let foreign = resolve_current_keyed_documents(foreign, catalog).unwrap();
        assert!(
            baseline
                .for_strict_recovery(&foreign, &publication.publication_id, &evidence)
                .is_err()
        );
        assert!(
            recovery
                .validate_current_projection(
                    &MvKeyedDocumentProjection::try_from_current(foreign).unwrap()
                )
                .is_err()
        );
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
