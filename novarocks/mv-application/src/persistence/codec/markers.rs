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

use proto::marker_document::{Body, Scope};

const VERSION: u32 = 1;

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
    pub refresh_identity: Bytes,
    pub source_windows: Vec<SourceVersionWindow>,
    pub storage_uuid: [u8; 16],
    pub baseline_snapshot: i64,
    pub mapping_revision: DocumentRevision,
    pub verification_requirement: Bytes,
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
                if evidence.refresh_identity.is_empty()
                    || evidence.verification_requirement.is_empty()
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
                        refresh_identity: Some(evidence.refresh_identity.to_vec()),
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
                        verification_requirement: Some(evidence.verification_requirement.to_vec()),
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
                    refresh_identity: required(
                        item.refresh_identity,
                        "marker.pending.refresh_identity",
                    )?
                    .into(),
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
                    verification_requirement: required(
                        item.verification_requirement,
                        "marker.pending.verification_requirement",
                    )?
                    .into(),
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
    Unknown,
}

pub struct MarkerEligibility<'a> {
    pub active: Vec<&'a Marker>,
    pub inactive: BTreeSet<ConnectorMarkerKey>,
    pub automatic_reuse_blocked: bool,
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
            PublicationEvidence::Retired(_) | PublicationEvidence::Unknown
        ),
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
        } else {
            result.inactive.insert(key.clone());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publication(identity: u8) -> PublicationIdentity {
        PublicationIdentity::try_new(vec![identity; 16]).unwrap()
    }

    fn pending(publication: PublicationIdentity, attempt: u8) -> Marker {
        Marker::Pending {
            publication,
            evidence: PendingEvidence {
                attempt: [attempt; 16],
                refresh_identity: Bytes::from_static(b"refresh"),
                source_windows: vec![SourceVersionWindow {
                    occurrence: 0,
                    from: NativeDataVersion::try_new(vec![1]).unwrap(),
                    to: NativeDataVersion::try_new(vec![2]).unwrap(),
                }],
                storage_uuid: [5; 16],
                baseline_snapshot: 6,
                mapping_revision: DocumentRevision::from_canonical_bytes(b"mapping"),
                verification_requirement: Bytes::from_static(b"verification"),
            },
        }
    }

    fn observed(markers: impl IntoIterator<Item = Marker>) -> Vec<(ConnectorMarkerKey, Marker)> {
        markers
            .into_iter()
            .map(|marker| (marker.encode().unwrap().0, marker))
            .collect()
    }

    #[test]
    fn all_marker_kinds_roundtrip_and_check_scope_against_the_key() {
        let id = publication(1);
        let revision = DocumentRevision::from_canonical_bytes(b"binding");
        let markers = [
            pending(id.clone(), 1),
            Marker::PendingReleased {
                publication: id.clone(),
                attempt: [1; 16],
            },
            Marker::NeedsRebuild {
                scope: MarkerScope::Publication(id.clone()),
                changes: vec![BindingChange {
                    kind: BindingChangeKind::Resolution,
                    observed_revision: revision,
                    current_revision: DocumentRevision::from_canonical_bytes(b"replacement"),
                }],
            },
            Marker::InvalidDefinition {
                scope: MarkerScope::UnpublishedBinding(revision),
                kind: InvalidDefinitionKind::Resolution,
                diagnostic: "Missing source field".into(),
            },
            Marker::Invalid {
                publication: id,
                retirement_record_revision: DocumentRevision::from_canonical_bytes(b"retirement"),
            },
        ];
        for marker in markers {
            let (key, value) = marker.encode().unwrap();
            let decoded = Marker::decode(&key, &value).unwrap();
            assert_eq!(decoded, marker);
            assert_eq!(decoded.encode().unwrap(), (key.clone(), value.clone()));
            let mut wrong_scope = key;
            wrong_scope.binding = "another-binding".into();
            assert!(Marker::decode(&wrong_scope, &value).is_err());
        }
    }

    #[test]
    fn release_matches_only_the_original_publication_and_attempt() {
        let id = publication(1);
        let revision = DocumentRevision::from_canonical_bytes(b"unpublished");
        let markers = observed([
            pending(id.clone(), 1),
            pending(id.clone(), 2),
            Marker::PendingReleased {
                publication: id.clone(),
                attempt: [1; 16],
            },
            Marker::PendingReleased {
                publication: publication(9),
                attempt: [2; 16],
            },
            Marker::InvalidDefinition {
                scope: MarkerScope::UnpublishedBinding(revision),
                kind: InvalidDefinitionKind::Resolution,
                diagnostic: "Missing source field".into(),
            },
        ]);
        let current = marker_eligibility(PublicationEvidence::Current(&id), &markers);
        assert_eq!(current.active, vec![&markers[1].1]);
        assert!(current.inactive.contains(&markers[0].0));
        assert!(current.automatic_reuse_blocked);
        let retired = marker_eligibility(PublicationEvidence::Retired(&id), &markers);
        assert_eq!(retired.active, current.active);
        let unpublished = marker_eligibility(PublicationEvidence::Unpublished(&revision), &markers);
        assert_eq!(unpublished.active, vec![&markers[4].1]);
        let unknown = marker_eligibility(PublicationEvidence::Unknown, &markers);
        assert!(unknown.automatic_reuse_blocked);
        assert!(unknown.inactive.is_empty());
    }
}
