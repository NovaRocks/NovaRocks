// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Canonical MV markers, scoped to one publication or an unpublished binding.

use super::{PersistenceCodecError, encode_message, proto, required, wire};
use crate::persistence::identity::{DocumentRevision, NativeDataVersion, PublicationIdentity};
use crate::persistence::keyed_documents::MV_DOCUMENT_OWNER;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorDocumentFormat, ConnectorMarkerKey, ConnectorMarkerValue, DocumentIdentity,
};
use novarocks_type_contract::CanonicalFactWriter;
use prost::Message;
use std::collections::BTreeSet;
use uuid::{Uuid, Variant, Version};

use proto::marker_document::{Body, Scope};

const VERSION: u32 = 1;

/// Frozen admission and evidence requirements for the original ContentLocate window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PendingVerificationRequirement {
    estimate_bytes: u64,
    budget: u64,
}

impl PendingVerificationRequirement {
    const ENCODED_LEN: usize = 20;
    // Version 1, ContentLocate, WriteCounts + DeleteVerification, target coverage.
    const HEADER: [u8; 4] = [1, 1, 0b11, 1];

    pub fn content_locate(estimate_bytes: u64, budget: u64) -> Result<Self, PersistenceCodecError> {
        if budget == 0 || estimate_bytes > budget {
            return Err(invalid("invalid MV pending verification budget"));
        }
        Ok(Self {
            estimate_bytes,
            budget,
        })
    }

    pub const fn estimate_bytes(&self) -> u64 {
        self.estimate_bytes
    }

    pub const fn budget(&self) -> u64 {
        self.budget
    }

    fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut encoded = [0; Self::ENCODED_LEN];
        encoded[..4].copy_from_slice(&Self::HEADER);
        encoded[4..12].copy_from_slice(&self.estimate_bytes.to_be_bytes());
        encoded[12..].copy_from_slice(&self.budget.to_be_bytes());
        encoded
    }

    fn decode(bytes: &[u8]) -> Result<Self, PersistenceCodecError> {
        if bytes.len() != Self::ENCODED_LEN || bytes[..4] != Self::HEADER {
            return Err(invalid("invalid MV pending verification requirement"));
        }
        let estimate_bytes = u64::from_be_bytes(bytes[4..12].try_into().expect("fixed length"));
        let budget = u64::from_be_bytes(bytes[12..].try_into().expect("fixed length"));
        Self::content_locate(estimate_bytes, budget)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarkerScope {
    Publication(PublicationIdentity),
    UnpublishedBinding(DocumentRevision),
}

pub use proto::{BindingChangeKind, InvalidDefinitionKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingChange {
    pub kind: BindingChangeKind,
    pub observed_revision: DocumentRevision,
    pub current_revision: DocumentRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceVersionWindow {
    pub occurrence: u32,
    pub from: NativeDataVersion,
    pub to: NativeDataVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingEvidence {
    pub attempt: [u8; 16],
    pub refresh_identity: Uuid,
    pub source_windows: Vec<SourceVersionWindow>,
    pub storage_uuid: [u8; 16],
    pub baseline_snapshot: i64,
    pub mapping_revision: DocumentRevision,
    pub verification_requirement: PendingVerificationRequirement,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Marker {
    Pending {
        publication: PublicationIdentity,
        evidence: PendingEvidence,
    },
    PendingReleased {
        publication: PublicationIdentity,
        attempt: [u8; 16],
    },
    NeedsRebuild {
        scope: MarkerScope,
        changes: Vec<BindingChange>,
    },
    InvalidDefinition {
        scope: MarkerScope,
        kind: InvalidDefinitionKind,
        diagnostic: String,
    },
    Invalid {
        publication: PublicationIdentity,
        retirement_record_revision: DocumentRevision,
    },
}

fn invalid(message: impl Into<String>) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(message.into())
}

impl Marker {
    pub fn scope(&self) -> MarkerScope {
        match self {
            Self::Pending { publication, .. }
            | Self::PendingReleased { publication, .. }
            | Self::Invalid { publication, .. } => MarkerScope::Publication(publication.clone()),
            Self::NeedsRebuild { scope, .. } | Self::InvalidDefinition { scope, .. } => {
                scope.clone()
            }
        }
    }

    pub fn encode(
        &self,
    ) -> Result<(ConnectorMarkerKey, ConnectorMarkerValue), PersistenceCodecError> {
        let (name, body, attempt) = match self {
            Self::Pending { evidence, .. } => {
                if evidence.refresh_identity.get_version() != Some(Version::SortRand)
                    || evidence.refresh_identity.get_variant() != Variant::RFC4122
                    || evidence.source_windows.is_empty()
                    || !evidence
                        .source_windows
                        .windows(2)
                        .all(|pair| pair[0].occurrence < pair[1].occurrence)
                {
                    return Err(invalid("invalid MV pending evidence"));
                }
                (
                    "pending",
                    Body::Pending(proto::PendingMarker {
                        attempt: Some(evidence.attempt.to_vec()),
                        refresh_identity: Some(evidence.refresh_identity.as_bytes().to_vec()),
                        source_windows: evidence
                            .source_windows
                            .iter()
                            .map(|window| proto::SourceVersionWindow {
                                occurrence: Some(window.occurrence),
                                from_data_version: Some(window.from.as_bytes().to_vec()),
                                to_data_version: Some(window.to.as_bytes().to_vec()),
                            })
                            .collect(),
                        storage_object_id: Some(evidence.storage_uuid.to_vec()),
                        baseline_snapshot: Some(evidence.baseline_snapshot),
                        mapping_revision: Some(evidence.mapping_revision.as_bytes().to_vec()),
                        verification_requirement: Some(
                            evidence.verification_requirement.encode().to_vec(),
                        ),
                    }),
                    Some(evidence.attempt),
                )
            }
            Self::PendingReleased { attempt, .. } => (
                "pending-released",
                Body::PendingReleased(proto::PendingReleasedMarker {
                    attempt: Some(attempt.to_vec()),
                }),
                Some(*attempt),
            ),
            Self::NeedsRebuild { changes, .. } => {
                if changes.is_empty()
                    || changes
                        .iter()
                        .any(|change| change.kind == BindingChangeKind::Unspecified)
                    || !changes
                        .windows(2)
                        .all(|pair| (pair[0].kind as i32) < (pair[1].kind as i32))
                {
                    return Err(invalid("invalid or unordered MV rebuild changes"));
                }
                (
                    "needs-rebuild",
                    Body::NeedsRebuild(proto::NeedsRebuildMarker {
                        changes: changes
                            .iter()
                            .map(|change| proto::BindingChange {
                                kind: Some(change.kind as i32),
                                observed_revision: Some(
                                    change.observed_revision.as_bytes().to_vec(),
                                ),
                                current_revision: Some(change.current_revision.as_bytes().to_vec()),
                            })
                            .collect(),
                    }),
                    None,
                )
            }
            Self::InvalidDefinition {
                kind, diagnostic, ..
            } => {
                if *kind == InvalidDefinitionKind::Unspecified || diagnostic.trim().is_empty() {
                    return Err(invalid("invalid MV definition diagnostic"));
                }
                (
                    "invalid-definition",
                    Body::InvalidDefinition(proto::InvalidDefinitionMarker {
                        kind: Some(*kind as i32),
                        diagnostic: Some(diagnostic.clone()),
                    }),
                    None,
                )
            }
            Self::Invalid {
                retirement_record_revision,
                ..
            } => (
                "invalid",
                Body::Invalid(proto::InvalidMarker {
                    retirement_record_revision: Some(
                        retirement_record_revision.as_bytes().to_vec(),
                    ),
                }),
                None,
            ),
        };
        let mut binding = CanonicalFactWriter::new("novarocks.mv.marker-binding", VERSION);
        let scope = match self.scope() {
            MarkerScope::Publication(id) if id.as_bytes().len() == 16 => {
                binding.u32(1);
                binding.bytes(id.as_bytes());
                Scope::Publication(id.as_bytes().to_vec())
            }
            MarkerScope::Publication(_) => {
                return Err(invalid("MV marker publication identity must have 16 bytes"));
            }
            MarkerScope::UnpublishedBinding(revision) => {
                binding.u32(2);
                binding.bytes(revision.as_bytes());
                Scope::UnpublishedBinding(revision.as_bytes().to_vec())
            }
        };
        if let Some(attempt) = attempt {
            binding.bytes(&attempt);
        }
        let key = ConnectorMarkerKey {
            owner: MV_DOCUMENT_OWNER.into(),
            name: name.into(),
            binding: DocumentIdentity::from_bytes(
                *DocumentRevision::from_canonical_bytes(&binding.finish()).as_bytes(),
            )
            .to_hex(),
        };
        key.validate().map_err(|error| invalid(error.to_string()))?;
        let format = ConnectorDocumentFormat::try_new(MV_DOCUMENT_OWNER, name, VERSION)
            .map_err(|error| invalid(error.to_string()))?;
        let content = encode_message(proto::MarkerDocument {
            format_version: Some(VERSION),
            scope: Some(scope),
            body: Some(body),
        })?;
        Ok((
            key,
            ConnectorMarkerValue {
                format,
                content: Bytes::from(content.into_bytes()),
            },
        ))
    }

    pub fn decode(
        key: &ConnectorMarkerKey,
        value: &ConnectorMarkerValue,
    ) -> Result<Self, PersistenceCodecError> {
        wire::preflight(&value.content, wire::Schema::MarkerDocument)?;
        let document = proto::MarkerDocument::decode(value.content.as_ref())
            .map_err(PersistenceCodecError::ProtobufDecode)?;
        let version = required(document.format_version, "marker.format_version")?;
        if version != VERSION {
            return Err(PersistenceCodecError::UnknownFormatVersion {
                document: "marker",
                version,
            });
        }
        let scope = match required(document.scope, "marker.scope")? {
            Scope::Publication(id) => MarkerScope::Publication(PublicationIdentity::try_new(id)?),
            Scope::UnpublishedBinding(id) => {
                MarkerScope::UnpublishedBinding(DocumentRevision::try_from_bytes(&id)?)
            }
        };
        let publication = || match &scope {
            MarkerScope::Publication(id) => Ok(id.clone()),
            MarkerScope::UnpublishedBinding(_) => {
                Err(invalid("MV marker requires a publication scope"))
            }
        };
        let marker = match required(document.body, "marker.body")? {
            Body::Pending(item) => Self::Pending {
                publication: publication()?,
                evidence: PendingEvidence {
                    attempt: exact_16(item.attempt, "marker.pending.attempt")?,
                    refresh_identity: Uuid::from_bytes(exact_16(
                        item.refresh_identity,
                        "marker.pending.refresh_identity",
                    )?),
                    source_windows: item
                        .source_windows
                        .into_iter()
                        .map(|window| {
                            Ok(SourceVersionWindow {
                                occurrence: required(
                                    window.occurrence,
                                    "marker.pending.window.occurrence",
                                )?,
                                from: NativeDataVersion::try_new(required(
                                    window.from_data_version,
                                    "marker.pending.window.from",
                                )?)?,
                                to: NativeDataVersion::try_new(required(
                                    window.to_data_version,
                                    "marker.pending.window.to",
                                )?)?,
                            })
                        })
                        .collect::<Result<_, PersistenceCodecError>>()?,
                    storage_uuid: exact_16(item.storage_object_id, "marker.pending.storage_uuid")?,
                    baseline_snapshot: required(
                        item.baseline_snapshot,
                        "marker.pending.baseline_snapshot",
                    )?,
                    mapping_revision: revision(
                        item.mapping_revision,
                        "marker.pending.mapping_revision",
                    )?,
                    verification_requirement: PendingVerificationRequirement::decode(&required(
                        item.verification_requirement,
                        "marker.pending.verification_requirement",
                    )?)?,
                },
            },
            Body::PendingReleased(item) => Self::PendingReleased {
                publication: publication()?,
                attempt: exact_16(item.attempt, "marker.released.attempt")?,
            },
            Body::NeedsRebuild(item) => Self::NeedsRebuild {
                scope,
                changes: item
                    .changes
                    .into_iter()
                    .map(|change| {
                        let field = "marker.change.kind";
                        let raw = required(change.kind, field)?;
                        Ok(BindingChange {
                            kind: BindingChangeKind::try_from(raw).map_err(|_| {
                                PersistenceCodecError::UnknownEnum { field, value: raw }
                            })?,
                            observed_revision: revision(
                                change.observed_revision,
                                "marker.change.observed_revision",
                            )?,
                            current_revision: revision(
                                change.current_revision,
                                "marker.change.current_revision",
                            )?,
                        })
                    })
                    .collect::<Result<_, PersistenceCodecError>>()?,
            },
            Body::InvalidDefinition(item) => {
                let field = "marker.invalid_definition.kind";
                let raw = required(item.kind, field)?;
                Self::InvalidDefinition {
                    scope,
                    kind: InvalidDefinitionKind::try_from(raw)
                        .map_err(|_| PersistenceCodecError::UnknownEnum { field, value: raw })?,
                    diagnostic: required(item.diagnostic, "marker.invalid_definition.diagnostic")?,
                }
            }
            Body::Invalid(item) => Self::Invalid {
                publication: publication()?,
                retirement_record_revision: revision(
                    item.retirement_record_revision,
                    "marker.invalid.retirement_record_revision",
                )?,
            },
        };
        let (encoded_key, encoded_value) = marker.encode()?;
        if encoded_key != *key || encoded_value != *value {
            return Err(invalid(
                "MV marker key, value, and canonical scope disagree",
            ));
        }
        Ok(marker)
    }
}

fn exact_16(
    value: Option<Vec<u8>>,
    field: &'static str,
) -> Result<[u8; 16], PersistenceCodecError> {
    required(value, field)?
        .try_into()
        .map_err(|_| invalid(format!("{field} must have 16 bytes")))
}

fn revision(
    value: Option<Vec<u8>>,
    field: &'static str,
) -> Result<DocumentRevision, PersistenceCodecError> {
    Ok(DocumentRevision::try_from_bytes(&required(value, field)?)?)
}

/// Eligibility derived from the original fenced observation.
#[derive(Clone, Copy)]
pub enum PublicationEvidence<'a> {
    Current(&'a PublicationIdentity),
    Unpublished(&'a DocumentRevision),
    Retired(&'a PublicationIdentity),
    Unattributed,
    Unknown,
}

pub struct MarkerEligibility<'a> {
    pub active: Vec<&'a Marker>,
    pub inactive: BTreeSet<ConnectorMarkerKey>,
    pub automatic_reuse_blocked: bool,
    pub automatic_refresh: bool,
    pub rewrite_serving: bool,
    pub strict_recovery_required: bool,
    pub explicit_rebuild_required: bool,
}

pub fn marker_eligibility<'a>(
    evidence: PublicationEvidence<'_>,
    markers: &'a [(ConnectorMarkerKey, Marker)],
) -> MarkerEligibility<'a> {
    let mut result = MarkerEligibility {
        active: Vec::new(),
        inactive: BTreeSet::new(),
        automatic_reuse_blocked: matches!(
            evidence,
            PublicationEvidence::Retired(_)
                | PublicationEvidence::Unattributed
                | PublicationEvidence::Unknown
        ),
        automatic_refresh: false,
        rewrite_serving: false,
        strict_recovery_required: false,
        explicit_rebuild_required: matches!(evidence, PublicationEvidence::Retired(_)),
    };
    if matches!(evidence, PublicationEvidence::Unknown) {
        return result;
    }
    let releases = markers
        .iter()
        .filter_map(|(_, marker)| match marker {
            Marker::PendingReleased {
                publication,
                attempt,
            } => Some((publication, *attempt)),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    for (key, marker) in markers {
        let in_scope = match (&evidence, marker.scope()) {
            (
                PublicationEvidence::Current(expected) | PublicationEvidence::Retired(expected),
                MarkerScope::Publication(actual),
            ) => **expected == actual,
            (
                PublicationEvidence::Unpublished(expected),
                MarkerScope::UnpublishedBinding(actual),
            ) => **expected == actual,
            (PublicationEvidence::Unattributed, _) => {
                matches!(marker, Marker::Invalid { .. } | Marker::Pending { .. })
            }
            _ => false,
        };
        let relevant = match marker {
            Marker::Pending {
                publication,
                evidence,
            } => !releases.contains(&(publication, evidence.attempt)),
            Marker::PendingReleased { .. } => false,
            Marker::NeedsRebuild { .. } | Marker::InvalidDefinition { .. } => {
                !matches!(evidence, PublicationEvidence::Retired(_))
            }
            Marker::Invalid { .. } => true,
        };
        if in_scope && relevant {
            result.automatic_reuse_blocked = true;
            result.active.push(marker);
        } else if !matches!(evidence, PublicationEvidence::Unattributed) {
            // An unowned snapshot proves no scope obsolete.
            result.inactive.insert(key.clone());
        }
    }
    result.explicit_rebuild_required |= result
        .active
        .iter()
        .any(|m| matches!(m, Marker::Invalid { .. }));
    result.strict_recovery_required = !result.explicit_rebuild_required
        && result
            .active
            .iter()
            .any(|m| matches!(m, Marker::Pending { .. }));
    result.automatic_refresh = !result.explicit_rebuild_required;
    result.rewrite_serving =
        matches!(evidence, PublicationEvidence::Current(_)) && !result.automatic_reuse_blocked;
    result
}

#[cfg(test)]
mod tests;
