// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use super::*;
use crate::persistence::codec::{
    PendingEvidence, PendingVerificationRequirement, SourceVersionWindow,
};
use crate::persistence::identity::NativeDataVersion;
use crate::persistence::keyed_documents::{
    keyed_view_fixture, require_readable_keyed_format, resolve_current_keyed_documents,
};
use novarocks_spi::connector::{ConnectorMutationOperationId, SnapshotFileTotals};
use uuid::Uuid;

fn add_marker(view: &mut KeyedDocumentView, marker: Marker) -> ConnectorMarkerKey {
    let (key, value) = marker.encode().unwrap();
    view.markers.insert(key.clone(), value);
    key
}

fn pending(publication: PublicationIdentity, attempt: u8) -> Marker {
    Marker::Pending {
        publication,
        evidence: PendingEvidence {
            attempt: [attempt; 16],
            refresh_identity: Uuid::now_v7(),
            source_windows: vec![SourceVersionWindow {
                occurrence: 5,
                from: NativeDataVersion::try_new(vec![1]).unwrap(),
                to: NativeDataVersion::try_new(vec![2]).unwrap(),
            }],
            storage_uuid: [4; 16],
            baseline_snapshot: 17,
            mapping_revision: DocumentRevision::from_canonical_bytes(b"mapping"),
            verification_requirement: PendingVerificationRequirement::content_locate(30, 40)
                .unwrap(),
        },
    }
}

#[test]
fn bound_publication_uses_its_own_markers() {
    let (catalog, mut view) = keyed_view_fixture(Some(17), true);
    let original = resolve_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
    let publication = original
        .publication
        .as_ref()
        .unwrap()
        .publication_id
        .clone();
    let stale = add_marker(
        &mut view,
        pending(PublicationIdentity::try_new(vec![8; 16]).unwrap(), 2),
    );
    let observed = resolve_current_keyed_documents(view, catalog).unwrap();

    assert_eq!(
        observed.applicable_publication(),
        &ApplicablePublication::Bound(publication)
    );
    assert_eq!(
        observed.servable_publication(),
        original.publication.as_ref()
    );
    assert!(observed.eligibility().inactive.contains(&stale));
    assert!(!observed.eligibility().strict_recovery_required);
}

#[test]
fn retirement_is_manageable_with_the_exact_invalid_record_and_blocks_serving() {
    let (catalog, mut view) = keyed_view_fixture(Some(17), true);
    let original = resolve_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
    let retirement = RetirementRecord {
        retired_publication: original.publication.unwrap().publication_id,
        operation: ConnectorMutationOperationId::from_bytes([8; 16]),
        requested: 4,
        matched: 3,
        insufficient_keys: 1,
        content_fingerprint: Some([7; 16]),
    };
    view.current_records = vec![retirement.encode().unwrap()];
    view.current_snapshot_file_totals = SnapshotFileTotals {
        data_files: Some(0),
        delete_files: Some(0),
    };

    // Direct reading still checks the exact binding envelope.
    require_readable_keyed_format(&view).unwrap();
    assert!(resolve_current_keyed_documents(view.clone(), catalog.clone()).is_err());
    add_marker(
        &mut view,
        Marker::Invalid {
            publication: retirement.retired_publication.clone(),
            retirement_record_revision: retirement.revision().unwrap(),
        },
    );
    let observed = resolve_current_keyed_documents(view, catalog).unwrap();
    assert_eq!(
        observed.applicable_publication(),
        &ApplicablePublication::RetiredPublication(retirement)
    );
    assert_eq!(observed.definition, original.definition);
    assert_eq!(observed.configuration, original.configuration);
    assert!(!observed.is_never_published());
    assert!(observed.servable_publication().is_none());
    assert!(observed.eligibility().explicit_rebuild_required);
    assert!(!observed.eligibility().automatic_refresh);
}

#[test]
fn unattributed_snapshot_never_serves_and_invalid_precedes_strict_recovery() {
    let (catalog, mut view) = keyed_view_fixture(Some(17), false);
    view.before_first_data_publication = false;
    let observed = resolve_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
    assert_eq!(
        observed.applicable_publication(),
        &ApplicablePublication::Unattributed
    );
    assert!(observed.servable_publication().is_none());
    assert!(observed.eligibility().automatic_refresh);
    assert!(!observed.eligibility().strict_recovery_required);
    let p = PublicationIdentity::try_new(vec![8; 16]).unwrap();
    let pending_key = add_marker(&mut view, pending(p.clone(), 2));
    let observed = resolve_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
    assert!(observed.eligibility().strict_recovery_required);
    assert!(observed.eligibility().automatic_refresh);

    add_marker(
        &mut view,
        Marker::PendingReleased {
            publication: p.clone(),
            attempt: [2; 16],
        },
    );
    let selected = FrozenMvMarkerKeys::capture(&view);
    assert!(selected.keys().contains(&pending_key));
    let observed = resolve_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
    assert!(!observed.eligibility().strict_recovery_required);
    assert!(selected.matches(&view));

    add_marker(&mut view, pending(p.clone(), 1));
    assert!(!selected.matches(&view));
    add_marker(
        &mut view,
        Marker::Invalid {
            publication: p,
            retirement_record_revision: DocumentRevision::from_canonical_bytes(b"prior retirement"),
        },
    );
    let observed = resolve_current_keyed_documents(view, catalog).unwrap();
    assert!(observed.eligibility().explicit_rebuild_required);
    assert!(!observed.eligibility().strict_recovery_required);
    assert!(!observed.eligibility().automatic_refresh);
    assert!(observed.servable_publication().is_none());
}

#[test]
fn only_authoritative_initial_history_can_prove_never_published() {
    for snapshot in [None, Some(17)] {
        let (catalog, mut view) = keyed_view_fixture(snapshot, false);
        let observed = resolve_current_keyed_documents(view.clone(), catalog.clone()).unwrap();
        assert_eq!(
            observed.applicable_publication(),
            &ApplicablePublication::NeverPublished(observed.root_binding_revision()),
        );
        assert!(observed.servable_publication().is_none());
        if snapshot.is_some() {
            for files in [None, Some(1)] {
                view.current_snapshot_file_totals.data_files = files;
                assert_eq!(
                    resolve_current_keyed_documents(view.clone(), catalog.clone())
                        .unwrap()
                        .applicable_publication(),
                    &ApplicablePublication::Unattributed,
                );
            }
        }
        view.current_snapshot_file_totals.data_files = Some(0);
        view.before_first_data_publication = false;
        assert_eq!(
            resolve_current_keyed_documents(view, catalog)
                .unwrap()
                .applicable_publication(),
            &ApplicablePublication::Unattributed,
        );
    }
}
