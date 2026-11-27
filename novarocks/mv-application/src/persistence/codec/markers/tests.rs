// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use super::*;

fn publication(identity: u8) -> PublicationIdentity {
    PublicationIdentity::try_new(vec![identity; 16]).unwrap()
}

fn pending(publication: PublicationIdentity, attempt: u8) -> Marker {
    Marker::Pending {
        publication,
        evidence: PendingEvidence {
            attempt: [attempt; 16],
            refresh_identity: Uuid::now_v7(),
            source_windows: vec![SourceVersionWindow {
                occurrence: 0,
                from: NativeDataVersion::try_new(vec![1]).unwrap(),
                to: NativeDataVersion::try_new(vec![2]).unwrap(),
            }],
            storage_uuid: [5; 16],
            baseline_snapshot: 6,
            mapping_revision: DocumentRevision::from_canonical_bytes(b"mapping"),
            verification_requirement: PendingVerificationRequirement::content_locate(10, 20)
                .unwrap(),
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

#[test]
fn pending_requirement_round_trips_its_exact_verified_budget() {
    let verification = PendingVerificationRequirement::content_locate(31, 59).unwrap();
    let bytes = verification.encode();
    assert_eq!(&bytes[..4], &[1, 1, 0b11, 1]);
    let decoded = PendingVerificationRequirement::decode(&bytes).unwrap();
    assert_eq!(decoded.estimate_bytes(), 31);
    assert_eq!(decoded.budget(), 59);
    assert!(PendingVerificationRequirement::decode(&bytes[..19]).is_err());
    let mut wrong_version = bytes;
    wrong_version[0] = 2;
    assert!(PendingVerificationRequirement::decode(&wrong_version).is_err());
    assert!(PendingVerificationRequirement::content_locate(60, 59).is_err());
}

#[test]
fn pending_requires_the_original_logical_refresh_uuid_v7() {
    let marker = pending(publication(1), 2);
    let Marker::Pending {
        publication,
        mut evidence,
    } = marker
    else {
        unreachable!();
    };
    evidence.refresh_identity = Uuid::nil();
    assert!(
        Marker::Pending {
            publication,
            evidence
        }
        .encode()
        .is_err()
    );
}
