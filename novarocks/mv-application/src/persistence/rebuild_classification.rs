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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RefreshBindingClassification<E> {
    Unchanged,
    NeedsRebuild(Marker),
    InvalidDefinition { marker: Marker, cause: Option<E> },
    Blocked,
}

/// Transient errors preserve their original cause and never produce a marker.
pub fn classify_refresh_binding<E: std::fmt::Display>(
    observed: RebuildEvidence,
    replacement: Result<RebuildEvidence, DefinitionAnalysisFailure<E>>,
    publication: PublicationEvidence<'_>,
    markers: &[(ConnectorMarkerKey, Marker)],
) -> Result<RefreshBindingClassification<E>, E> {
    if let Err(DefinitionAnalysisFailure::Transient(cause)) = replacement {
        return Err(cause);
    }
    let scope = match publication {
        PublicationEvidence::Current(id) => MarkerScope::Publication(id.clone()),
        PublicationEvidence::Unpublished(revision) => MarkerScope::UnpublishedBinding(*revision),
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
    if observed.binding.outputs != replacement.binding.outputs {
        let marker = Marker::InvalidDefinition {
            scope,
            kind: InvalidDefinitionKind::OutputDeclaration,
            diagnostic: format!(
                "The analyzed output declaration changed from {} to {}",
                hex::encode(observed.binding.outputs.as_bytes()),
                hex::encode(replacement.binding.outputs.as_bytes()),
            ),
        };
        return Ok(RefreshBindingClassification::InvalidDefinition {
            marker,
            cause: None,
        });
    }
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
    } else {
        Ok(RefreshBindingClassification::NeedsRebuild(
            Marker::NeedsRebuild { scope, changes },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::identity::PublicationIdentity;

    fn facts(byte: u8) -> DocumentRevision {
        DocumentRevision::try_from_bytes(&[byte; 32]).unwrap()
    }

    fn evidence() -> RebuildEvidence {
        RebuildEvidence {
            binding: BindingDigests {
                resolution: facts(1),
                dependencies: facts(2),
                outputs: facts(3),
            },
            interpretation: facts(4),
            mapping: facts(5),
        }
    }

    #[test]
    fn classification_prioritizes_output_changes_and_preserves_transient_causes() {
        let stored = evidence();
        let mut current = stored;
        current.binding.resolution = facts(6);
        current.binding.outputs = facts(7);
        let unpublished = facts(8);
        assert!(matches!(
            classify_refresh_binding::<String>(stored, Ok(current), PublicationEvidence::Unpublished(&unpublished), &[]).unwrap(),
            RefreshBindingClassification::InvalidDefinition {
                marker: Marker::InvalidDefinition { scope: MarkerScope::UnpublishedBinding(id), kind: InvalidDefinitionKind::OutputDeclaration, .. },
                cause: None,
            } if id == unpublished
        ));
        assert_eq!(
            classify_refresh_binding(
                stored,
                Err(DefinitionAnalysisFailure::Transient("cancelled")),
                PublicationEvidence::Unpublished(&unpublished),
                &[]
            ),
            Err("cancelled")
        );
    }

    #[test]
    fn changed_dependency_and_aggregate_layout_require_rebuild() {
        let stored = evidence();
        let mut current = stored;
        current.binding.dependencies = facts(6);
        current.interpretation = facts(7);
        let publication = PublicationIdentity::try_new(vec![9; 16]).unwrap();
        assert!(matches!(
            classify_refresh_binding::<String>(stored, Ok(current), PublicationEvidence::Current(&publication), &[]).unwrap(),
            RefreshBindingClassification::NeedsRebuild(Marker::NeedsRebuild { scope: MarkerScope::Publication(id), changes })
                if id == publication && changes.iter().map(|change| change.kind).collect::<Vec<_>>()
                    == [BindingChangeKind::Dependency, BindingChangeKind::Interpretation]
        ));
    }

    #[test]
    fn recovery_at_identical_facts_requires_an_active_marker_in_that_generation() {
        let stored = evidence();
        let original = facts(7);
        let other = facts(8);
        let marker = Marker::InvalidDefinition {
            scope: MarkerScope::UnpublishedBinding(original),
            kind: InvalidDefinitionKind::Resolution,
            diagnostic: "Missing source".into(),
        };
        let markers = [(marker.encode().unwrap().0, marker)];
        assert!(matches!(
            classify_refresh_binding::<String>(stored, Ok(stored), PublicationEvidence::Unpublished(&original), &markers).unwrap(),
            RefreshBindingClassification::NeedsRebuild(Marker::NeedsRebuild { changes, .. })
                if changes[0].kind == BindingChangeKind::RecoveredDefinition
        ));
        assert_eq!(
            classify_refresh_binding::<String>(
                stored,
                Ok(stored),
                PublicationEvidence::Unpublished(&other),
                &markers
            )
            .unwrap(),
            RefreshBindingClassification::Unchanged
        );
    }
}
