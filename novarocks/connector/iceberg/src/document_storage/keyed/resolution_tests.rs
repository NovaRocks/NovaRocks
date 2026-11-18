// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{Resolution, resolve};
use crate::document_storage::keyed::{
    calculation_test_support::{Snapshot, full, record},
    envelope::encode_slot,
    keys::slot_key,
    record::encode_record,
};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::HashMap;

fn evidence(parent: Option<i64>) -> UnknownCommitEvidence {
    UnknownCommitEvidence {
        asserted_parent: parent,
        preselected_snapshot: 20,
        identity: UnknownCommitIdentity::None,
    }
}
fn conclusion(result: Resolution) -> CommitConclusion {
    let Resolution::Conclusion(conclusion) = result else {
        panic!("full observation must settle classification");
    };
    conclusion
}
#[test]
fn refs_positive_main_does_not_need_full_or_lose_earlier_proof() {
    let selected = full(
        Some(20),
        vec![Snapshot::new(10, None), Snapshot::new(20, Some(10))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&selected.current, None, &evidence(Some(10))).unwrap()),
        CommitConclusion::Committed(CommitProof::Main)
    );
    let later = full(
        Some(40),
        vec![Snapshot::new(40, Some(30))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&selected.current, Some(&later), &evidence(Some(10))).unwrap()),
        CommitConclusion::Committed(CommitProof::Main)
    );
}
#[test]
fn refs_with_asserted_parent_is_still_unknown_and_changed_main_needs_full() {
    let parent = full(
        Some(10),
        vec![Snapshot::new(10, None)],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&parent.current, None, &evidence(Some(10))).unwrap()),
        CommitConclusion::StillUnknown
    );
    let advanced = full(
        Some(30),
        vec![Snapshot::new(30, Some(10))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        resolve(&advanced.current, None, &evidence(Some(10))).unwrap(),
        Resolution::NeedFull
    );
    let empty = full(None, vec![], HashMap::new(), vec![]);
    assert_eq!(
        conclusion(resolve(&empty.current, None, &evidence(None)).unwrap()),
        CommitConclusion::StillUnknown
    );
}
#[test]
fn visible_selected_snapshot_survives_later_main() {
    let view = full(
        Some(40),
        vec![
            Snapshot::new(10, None),
            Snapshot::new(20, Some(10)),
            Snapshot::new(30, Some(20)),
            Snapshot::new(40, Some(30)),
        ],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &evidence(Some(10))).unwrap()),
        CommitConclusion::Committed(CommitProof::Snapshot)
    );
}
#[test]
fn expired_selected_snapshot_can_be_proved_by_parent_pointer() {
    let view = full(
        Some(30),
        vec![Snapshot::new(30, Some(20))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &evidence(Some(10))).unwrap()),
        CommitConclusion::Committed(CommitProof::ParentPointer)
    );
}
#[test]
fn carried_record_is_exact_bytes_owner_and_name() {
    let publication = record(b"publication-20", None);
    let view = full(
        Some(40),
        vec![Snapshot::new(40, Some(30)).record(publication.clone())],
        HashMap::new(),
        vec![],
    );
    let evidence = UnknownCommitEvidence {
        identity: UnknownCommitIdentity::RecordBytes {
            owner: publication.owner.clone(),
            name: publication.name.clone(),
            bytes: Bytes::from(encode_record(&publication).unwrap()),
        },
        ..evidence(Some(10))
    };
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &evidence).unwrap()),
        CommitConclusion::Committed(CommitProof::CarriedRecord)
    );
    let mut other = evidence.clone();
    let UnknownCommitIdentity::RecordBytes { bytes, .. } = &mut other.identity else {
        unreachable!()
    };
    *bytes = Bytes::from_static(b"2.-.b3RoZXI");
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &other).unwrap()),
        CommitConclusion::InvalidatedUndetermined
    );
}
#[test]
fn selected_visible_record_must_match_frozen_bytes() {
    let publication = record(b"publication-20", None);
    let view = full(
        Some(40),
        vec![
            Snapshot::new(20, Some(10)).record(publication.clone()),
            Snapshot::new(40, Some(30)),
        ],
        HashMap::new(),
        vec![],
    );
    let mut evidence = evidence(Some(10));
    evidence.identity = UnknownCommitIdentity::RecordBytes {
        owner: publication.owner,
        name: publication.name,
        bytes: Bytes::from_static(b"different"),
    };
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &evidence).unwrap()),
        CommitConclusion::InvalidatedUndetermined
    );
}
#[test]
fn slot_writer_proves_effect_after_selected_snapshot_expires() {
    let slot = ConnectorSlotName::try_new("novarocks.test", "configuration").unwrap();
    let value = ConnectorSlotValue {
        format: ConnectorDocumentFormat::try_new("novarocks.test", "configuration", 1).unwrap(),
        content: Bytes::from_static(b"paused"),
    };
    let view = full(
        Some(40),
        vec![Snapshot::new(40, Some(30))],
        HashMap::from([(
            slot_key(&slot),
            encode_slot(&slot, &value, Some(20)).unwrap(),
        )]),
        vec![],
    );
    let evidence = UnknownCommitEvidence {
        identity: UnknownCommitIdentity::SlotWriter { slot },
        ..evidence(Some(10))
    };
    assert_eq!(
        conclusion(resolve(&view.current, None, &evidence).unwrap()),
        CommitConclusion::Committed(CommitProof::SlotWriter)
    );
}
#[test]
fn complete_alternative_ancestry_proves_not_committed() {
    let view = full(
        Some(40),
        vec![
            Snapshot::new(10, None),
            Snapshot::new(30, Some(10)),
            Snapshot::new(40, Some(30)),
        ],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &evidence(Some(10))).unwrap()),
        CommitConclusion::NotCommitted(CommitProof::NegativeProof)
    );
}
#[test]
fn initially_empty_parent_requires_chain_to_first_snapshot() {
    let view = full(
        Some(40),
        vec![Snapshot::new(30, None), Snapshot::new(40, Some(30))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&view.current, Some(&view), &evidence(None)).unwrap()),
        CommitConclusion::NotCommitted(CommitProof::NegativeProof)
    );
    let broken = full(
        Some(40),
        vec![Snapshot::new(40, Some(30))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&broken.current, Some(&broken), &evidence(None)).unwrap()),
        CommitConclusion::InvalidatedUndetermined
    );
}
#[test]
fn missing_parent_or_broken_main_chain_is_not_negative_proof() {
    for snapshots in [
        vec![Snapshot::new(40, Some(30))],
        vec![Snapshot::new(10, None), Snapshot::new(40, Some(30))],
    ] {
        let view = full(Some(40), snapshots, HashMap::new(), vec![]);
        assert_eq!(
            conclusion(resolve(&view.current, Some(&view), &evidence(Some(10))).unwrap()),
            CommitConclusion::InvalidatedUndetermined
        );
    }
}
#[test]
fn full_may_still_show_asserted_parent_or_offer_new_positive_main() {
    let refs = full(
        Some(30),
        vec![Snapshot::new(30, None)],
        HashMap::new(),
        vec![],
    );
    let parent = full(
        Some(10),
        vec![Snapshot::new(10, None)],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&refs.current, Some(&parent), &evidence(Some(10))).unwrap()),
        CommitConclusion::StillUnknown
    );
    let selected = full(
        Some(20),
        vec![Snapshot::new(20, Some(10))],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&refs.current, Some(&selected), &evidence(Some(10))).unwrap()),
        CommitConclusion::Committed(CommitProof::Main)
    );
}
#[test]
fn mismatched_scope_rejects_cross_table_evidence() {
    let view = full(
        Some(30),
        vec![Snapshot::new(30, None)],
        HashMap::new(),
        vec![],
    );
    let mut other = view.clone();
    other.current.object_id =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"other-table")).unwrap();
    assert_eq!(
        resolve(&view.current, Some(&other), &evidence(Some(10)))
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
}

#[test]
fn combined_slot_recovery_requires_the_selected_immutable_property_effect() {
    let old = Some(10);
    let effect_digest = [7; 32];
    let evidence = UnknownCommitEvidence {
        identity: UnknownCommitIdentity::SlotStorageEffect {
            effect_digest,
            request_fingerprint: [11; 32],
        },
        ..evidence(old)
    };
    let mut selected = full(
        Some(20),
        vec![Snapshot::new(10, None), Snapshot::new(20, old)],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        resolve(&selected.current, None, &evidence).unwrap(),
        Resolution::NeedFull
    );
    assert_eq!(
        conclusion(resolve(&selected.current, Some(&selected), &evidence).unwrap()),
        CommitConclusion::StillUnknown,
    );
    let target = selected
        .snapshots
        .iter_mut()
        .find(|snapshot| snapshot.snapshot_id == 20)
        .unwrap();
    target.slot_storage_effect_digest = Some([8; 32]);
    assert_eq!(
        conclusion(resolve(&selected.current, Some(&selected), &evidence).unwrap()),
        CommitConclusion::StillUnknown,
    );

    let mut advanced = full(
        Some(40),
        vec![
            Snapshot::new(10, None),
            Snapshot::new(20, old),
            Snapshot::new(40, Some(20)),
        ],
        HashMap::new(),
        vec![],
    );
    assert_eq!(
        conclusion(resolve(&advanced.current, Some(&advanced), &evidence).unwrap()),
        CommitConclusion::StillUnknown,
        "a parent pointer proves snapshot identity alone",
    );
    let target = advanced
        .snapshots
        .iter_mut()
        .find(|snapshot| snapshot.snapshot_id == 20)
        .unwrap();
    target.slot_storage_effect_digest = Some(effect_digest);
    assert_eq!(
        conclusion(resolve(&advanced.current, Some(&advanced), &evidence).unwrap()),
        CommitConclusion::Committed(CommitProof::Snapshot),
    );
}
