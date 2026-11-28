// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

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
fn storage_refusal_keeps_the_exact_scope_and_original_cause() {
    let original = evidence();
    let publication = PublicationIdentity::try_new(vec![2; 16]).unwrap();
    let incompatible = |binding| ReplacementEvidence::IncompatibleStorage {
        binding,
        target_revision: facts(9),
        cause: "original physical mapping refusal",
    };
    assert!(matches!(
        classify_refresh_observation(
            original,
            Ok(incompatible(original.binding)),
            PublicationEvidence::Current(&publication),
            &[],
        ).unwrap(),
        RefreshBindingClassification::IncompatibleStorage {
            marker: Marker::NeedsRebuild { scope: MarkerScope::Publication(id), changes },
            cause: "original physical mapping refusal",
        } if id == publication && changes[0].current_revision == facts(9)
    ));
    let mut changed_output = original.binding;
    changed_output.outputs = facts(8);
    assert!(matches!(
        classify_refresh_observation(
            original,
            Ok(incompatible(changed_output)),
            PublicationEvidence::Current(&publication),
            &[],
        )
        .unwrap(),
        RefreshBindingClassification::InvalidDefinition { .. }
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

#[test]
fn unattributed_current_accepts_compatible_binding_without_inventing_a_marker_scope() {
    let stored = evidence();
    let mut rebound = stored;
    rebound.binding.dependencies = facts(6);
    let mut changed_output = stored;
    changed_output.binding.outputs = facts(7);
    for (observed, classification) in [
        (stored, RefreshBindingClassification::Unchanged),
        (
            rebound,
            RefreshBindingClassification::NeedsRebuildWithoutMarker,
        ),
        (changed_output, RefreshBindingClassification::Blocked),
    ] {
        assert_eq!(
            classify_refresh_binding::<String>(
                stored,
                Ok(observed),
                PublicationEvidence::Unattributed,
                &[],
            )
            .unwrap(),
            classification,
        );
    }
}
