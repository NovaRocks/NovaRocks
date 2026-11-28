// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Refresh classification over exact observed binding and layout evidence.

use super::codec::{
    BindingChange, BindingChangeKind, BindingDigests, InvalidDefinitionKind, Marker, MarkerScope,
    PublicationEvidence, marker_eligibility,
};
use super::identity::DocumentRevision;
use super::keyed_documents::MvKeyedVersions;
use novarocks_spi::connector::ConnectorMarkerKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RebuildEvidence {
    pub binding: BindingDigests,
    pub interpretation: DocumentRevision,
    pub mapping: DocumentRevision,
}

impl From<&MvKeyedVersions> for RebuildEvidence {
    fn from(versions: &MvKeyedVersions) -> Self {
        Self {
            binding: versions.binding_document.digests(),
            interpretation: versions.interpretation_revision(),
            mapping: DocumentRevision::try_from_bytes(&versions.mapping.identity().to_bytes())
                .expect("connector document identities have 32 bytes"),
        }
    }
}

pub enum DefinitionAnalysisFailure<E> {
    Deterministic(E),
    Transient(E),
}

pub enum ReplacementEvidence<E> {
    Complete(RebuildEvidence),
    ChangedOutput(BindingDigests),
    IncompatibleStorage {
        binding: BindingDigests,
        target_revision: DocumentRevision,
        cause: E,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RefreshBindingClassification<E> {
    Unchanged,
    NeedsRebuild(Marker),
    NeedsRebuildWithoutMarker,
    InvalidDefinition { marker: Marker, cause: Option<E> },
    IncompatibleStorage { marker: Marker, cause: E },
    Blocked,
}

/// Transient errors preserve their original cause and never produce a marker.
pub fn classify_refresh_binding<E: std::fmt::Display>(
    observed: RebuildEvidence,
    replacement: Result<RebuildEvidence, DefinitionAnalysisFailure<E>>,
    publication: PublicationEvidence<'_>,
    markers: &[(ConnectorMarkerKey, Marker)],
) -> Result<RefreshBindingClassification<E>, E> {
    classify_refresh_observation(
        observed,
        replacement.map(ReplacementEvidence::Complete),
        publication,
        markers,
    )
}

pub fn classify_refresh_observation<E: std::fmt::Display>(
    observed: RebuildEvidence,
    replacement: Result<ReplacementEvidence<E>, DefinitionAnalysisFailure<E>>,
    publication: PublicationEvidence<'_>,
    markers: &[(ConnectorMarkerKey, Marker)],
) -> Result<RefreshBindingClassification<E>, E> {
    if let Err(DefinitionAnalysisFailure::Transient(cause)) = replacement {
        return Err(cause);
    }
    let scope = match publication {
        PublicationEvidence::Current(id) => Some(MarkerScope::Publication(id.clone())),
        PublicationEvidence::Unpublished(revision) => {
            Some(MarkerScope::UnpublishedBinding(*revision))
        }
        PublicationEvidence::Unattributed => None,
        PublicationEvidence::Retired(_) | PublicationEvidence::Unknown => {
            return Ok(RefreshBindingClassification::Blocked);
        }
    };
    let active = marker_eligibility(publication, markers).active;
    if active
        .iter()
        .any(|marker| matches!(marker, Marker::Pending { .. } | Marker::Invalid { .. }))
    {
        return Ok(RefreshBindingClassification::Blocked);
    }
    let replacement = match replacement {
        Ok(value) => value,
        Err(DefinitionAnalysisFailure::Deterministic(cause)) => {
            let Some(scope) = scope else {
                return Err(cause);
            };
            let marker = Marker::InvalidDefinition {
                scope,
                kind: InvalidDefinitionKind::Resolution,
                diagnostic: cause.to_string(),
            };
            return Ok(RefreshBindingClassification::InvalidDefinition {
                marker,
                cause: Some(cause),
            });
        }
        Err(DefinitionAnalysisFailure::Transient(cause)) => return Err(cause),
    };
    let binding = match &replacement {
        ReplacementEvidence::Complete(value) => value.binding,
        ReplacementEvidence::ChangedOutput(value)
        | ReplacementEvidence::IncompatibleStorage { binding: value, .. } => *value,
    };
    if observed.binding.outputs != binding.outputs {
        let Some(scope) = scope else {
            return Ok(RefreshBindingClassification::Blocked);
        };
        let marker = Marker::InvalidDefinition {
            scope,
            kind: InvalidDefinitionKind::OutputDeclaration,
            diagnostic: format!(
                "The analyzed output declaration changed from {} to {}",
                hex::encode(observed.binding.outputs.as_bytes()),
                hex::encode(binding.outputs.as_bytes()),
            ),
        };
        return Ok(RefreshBindingClassification::InvalidDefinition {
            marker,
            cause: None,
        });
    }
    let replacement = match replacement {
        ReplacementEvidence::Complete(value) => value,
        ReplacementEvidence::ChangedOutput(_) => return Ok(RefreshBindingClassification::Blocked),
        ReplacementEvidence::IncompatibleStorage {
            target_revision,
            cause,
            ..
        } => {
            let Some(scope) = scope else {
                return Err(cause);
            };
            let marker = Marker::NeedsRebuild {
                scope,
                changes: vec![BindingChange {
                    kind: BindingChangeKind::StorageMapping,
                    observed_revision: observed.mapping,
                    current_revision: target_revision,
                }],
            };
            return Ok(RefreshBindingClassification::IncompatibleStorage { marker, cause });
        }
    };
    let mut changes = [
        (
            BindingChangeKind::Resolution,
            observed.binding.resolution,
            replacement.binding.resolution,
        ),
        (
            BindingChangeKind::Dependency,
            observed.binding.dependencies,
            replacement.binding.dependencies,
        ),
        (
            BindingChangeKind::Interpretation,
            observed.interpretation,
            replacement.interpretation,
        ),
        (
            BindingChangeKind::StorageMapping,
            observed.mapping,
            replacement.mapping,
        ),
    ]
    .into_iter()
    .filter_map(|(kind, observed_revision, current_revision)| {
        (observed_revision != current_revision).then_some(BindingChange {
            kind,
            observed_revision,
            current_revision,
        })
    })
    .collect::<Vec<_>>();
    if active.iter().any(|marker| {
        matches!(
            marker,
            Marker::NeedsRebuild { .. } | Marker::InvalidDefinition { .. }
        )
    }) {
        changes.push(BindingChange {
            kind: BindingChangeKind::RecoveredDefinition,
            observed_revision: observed.binding.resolution,
            current_revision: replacement.binding.resolution,
        });
    }
    if changes.is_empty() {
        Ok(RefreshBindingClassification::Unchanged)
    } else if let Some(scope) = scope {
        Ok(RefreshBindingClassification::NeedsRebuild(
            Marker::NeedsRebuild { scope, changes },
        ))
    } else {
        Ok(RefreshBindingClassification::NeedsRebuildWithoutMarker)
    }
}

#[cfg(test)]
mod tests;
