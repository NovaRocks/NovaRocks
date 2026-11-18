// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Observes the exact owned-artifact ledger and frozen coverage rules.

use super::*;
use novarocks_spi::connector::ConnectorWriteOperationId;

fn object(path: &str) -> ObjectIdentity {
    ObjectIdentity::new(path).unwrap()
}
fn ledger() -> (ArtifactLedger, AttemptToken) {
    let token = OperationToken::from_write(ConnectorWriteOperationId::from_bytes([1; 16]));
    (ArtifactLedger::new(token), AttemptToken::new(token, 0))
}
fn produced(
    ledger: &mut ArtifactLedger,
    attempt: AttemptToken,
    path: &str,
    kind: ArtifactKind,
) -> ObjectIdentity {
    let object = object(path);
    ledger
        .register(ArtifactRecord::produced(object.clone(), attempt, kind))
        .unwrap();
    ledger.confirm_written_for_test(&object).unwrap();
    object
}
#[test]
fn received_objects_cannot_be_written_and_paths_are_never_registered_twice() {
    let (mut ledger, attempt) = ledger();
    let data = object("data.parquet");
    ledger
        .register(ArtifactRecord::received(
            data.clone(),
            ReceivedPublication::ViaManifest,
        ))
        .unwrap();
    assert!(ledger.mark_writing(&data).is_err());
    let manifest = produced(
        &mut ledger,
        attempt,
        "manifest.avro",
        ArtifactKind::Manifest,
    );
    assert!(ledger.mark_writing(&manifest).is_err());
    ledger.forget_removed(&manifest);
    assert!(
        ledger
            .register(ArtifactRecord::produced(
                manifest,
                attempt,
                ArtifactKind::Manifest
            ))
            .is_err()
    );
    assert!(ledger.mark_writing(&object("external.parquet")).is_err());
}
#[test]
fn retirement_closes_admission_but_preserves_actual_writer_completion() {
    let (mut ledger, attempt) = ledger();
    let manifest = object("manifest.avro");
    ledger
        .register(ArtifactRecord::produced(
            manifest.clone(),
            attempt,
            ArtifactKind::Manifest,
        ))
        .unwrap();
    ledger.mark_writing(&manifest).unwrap();
    ledger.retire(&manifest).unwrap();
    assert!(ledger.check_writable(&manifest).is_err());
    ledger.mark_written(&manifest).unwrap();
    assert!(ledger.record_manifest(&manifest, &[], &[], &[]).is_err());
}
#[test]
fn frozen_references_include_all_snapshots_and_received_objects() {
    let (mut ledger, attempt) = ledger();
    let data = object("data.parquet");
    let provenance = object("provenance.json");
    ledger
        .register(ArtifactRecord::received(
            data.clone(),
            ReceivedPublication::ViaManifest,
        ))
        .unwrap();
    ledger
        .register(ArtifactRecord::received(
            provenance.clone(),
            ReceivedPublication::WithRequest,
        ))
        .unwrap();
    let manifest = produced(&mut ledger, attempt, "data.avro", ArtifactKind::Manifest);
    ledger
        .record_manifest(&manifest, &["data.parquet".into()], &[], &[])
        .unwrap();
    let first = produced(
        &mut ledger,
        attempt,
        "first.avro",
        ArtifactKind::ManifestList,
    );
    ledger
        .record_manifest_list(&first, 1, &["data.avro".into(), "external.avro".into()])
        .unwrap();
    let last = produced(
        &mut ledger,
        attempt,
        "last.avro",
        ArtifactKind::ManifestList,
    );
    ledger.record_manifest_list(&last, 1, &[]).unwrap();
    let orphan = produced(&mut ledger, attempt, "orphan.avro", ArtifactKind::Manifest);
    let facts = ledger
        .snapshot_for_request(attempt, &[first.clone(), last.clone()], &[])
        .unwrap();
    let expected: BTreeSet<_> = [data, provenance, manifest, first, last]
        .into_iter()
        .collect();
    assert_eq!(
        facts.references().iter().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    let remainder = ledger.settle_published(facts.references().iter().collect());
    assert_eq!(remainder.len(), 1);
    assert_eq!(remainder[0].object, orphan);
    assert!(remainder[0].retired);
    assert_eq!(ledger.selected(CleanupScope::EntireOperation), remainder);
}
#[test]
fn uncovered_received_object_refuses_publication_and_replacements_cannot_coexist() {
    let (mut ledger, attempt) = ledger();
    let data = object("uncovered.parquet");
    ledger
        .register(ArtifactRecord::received(
            data,
            ReceivedPublication::ViaManifest,
        ))
        .unwrap();
    assert!(ledger.snapshot_for_request(attempt, &[], &[]).is_err());
    let manifest = produced(
        &mut ledger,
        attempt,
        "replacement.avro",
        ArtifactKind::Manifest,
    );
    ledger
        .record_manifest(&manifest, &[], &[], &["source.avro".into()])
        .unwrap();
    let list = produced(
        &mut ledger,
        attempt,
        "list.avro",
        ArtifactKind::ManifestList,
    );
    assert!(
        ledger
            .record_manifest_list(&list, 1, &["replacement.avro".into(), "source.avro".into()])
            .is_err()
    );
}

fn descriptor(path: &str, snapshot: i64) -> ManifestFile {
    ManifestFile {
        manifest_path: path.into(),
        manifest_length: 123,
        partition_spec_id: 0,
        content: crate::iceberg::spec::ManifestContentType::Data,
        sequence_number: -1,
        min_sequence_number: -1,
        added_snapshot_id: snapshot,
        added_files_count: None,
        existing_files_count: None,
        deleted_files_count: None,
        added_rows_count: None,
        existing_rows_count: None,
        deleted_rows_count: None,
        partitions: None,
        key_metadata: None,
        first_row_id: None,
    }
}

#[test]
fn compact_coverage_counts_entries_even_when_their_physical_file_is_shared() {
    let (mut ledger, attempt) = ledger();
    ledger
        .register(ArtifactRecord::received(
            object("shared.puffin"),
            ReceivedPublication::ViaManifest,
        ))
        .unwrap();
    let names = ["first.avro", "second.avro"];
    let manifests = names.map(|name| produced(&mut ledger, attempt, name, ArtifactKind::Manifest));
    let descriptors = names.map(|name| descriptor(name, 7));
    ledger
        .record_manifest(&manifests[0], &["shared.puffin".into()], &[], &[])
        .unwrap();
    ledger
        .record_manifest(&manifests[1], &["foreign.puffin".into()], &[], &[])
        .unwrap();
    assert!(ledger.seal_compact_stage(7, &descriptors, 2).is_err());
    ledger
        .record_manifest(&manifests[1], &["shared.puffin".into()], &[], &[])
        .unwrap();
    assert!(ledger.seal_compact_stage(7, &descriptors, 3).is_err());
    ledger.seal_compact_stage(7, &descriptors, 2).unwrap();

    let merge = produced(&mut ledger, attempt, "merged.avro", ArtifactKind::Manifest);
    let replacements = names.map(str::to_owned);
    ledger
        .record_manifest(&merge, &["shared.puffin".into()], &[], &replacements)
        .unwrap();
    let list = produced(
        &mut ledger,
        attempt,
        "list.avro",
        ArtifactKind::ManifestList,
    );
    ledger
        .record_manifest_list(&list, 7, &["merged.avro".into()])
        .unwrap();
    assert!(
        ledger
            .snapshot_for_request(attempt, std::slice::from_ref(&list), &[])
            .is_err()
    );
    ledger
        .record_manifest(
            &merge,
            &["shared.puffin".into(), "shared.puffin".into()],
            &[],
            &replacements,
        )
        .unwrap();
    let request = ledger
        .snapshot_for_request(attempt, std::slice::from_ref(&list), &[])
        .unwrap();
    assert!(request.references().contains(&object("shared.puffin")));
    assert!(request.references().contains(&merge));
    assert!(!request.references().contains(&manifests[0]));
}
