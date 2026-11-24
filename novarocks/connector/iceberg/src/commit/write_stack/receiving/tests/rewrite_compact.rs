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

use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use super::*;
use crate::commit::model::{EntryIdentity, SeqField};
use crate::commit::report::IcebergColumnStats;
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergCommitFragment, IcebergDataFileArtifact,
    IcebergFrozenRewriteBranchInput, IcebergSessionFacts, IcebergWriteSessionId,
    IcebergWriteTableFacts,
};
use crate::commit::write_stack::test_support::{paid_fragments_counted, sample_partition};
use crate::delete_file::IcebergFileFormat;
use crate::row_lineage_synth::{
    ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER as SEQUENCE,
    ICEBERG_RESERVED_FIELD_ID_ROW_ID as ROW_ID,
};
use novarocks_spi::connector::ConnectorWriteAdmissionPurpose;

fn rewrite_session(version: u8, groups: u32) -> IcebergWriteCommitSession {
    let table = IcebergWriteTableFacts::try_new(
        "6f7c3a0e-0000-4000-8000-000000000001".into(),
        "db".into(),
        "t".into(),
        "s3://bucket/table".into(),
        "s3://bucket/table/data".into(),
        "main".into(),
        Some(931),
        if version == 1 { 0 } else { 27 },
        1,
        0,
        version,
    )
    .unwrap();
    let mut facts =
        IcebergSessionFacts::ordinary(ConnectorWriteAdmissionPurpose::OrdinaryDml, None);
    facts.rewrite_inputs = (0..groups)
        .map(|group| {
            IcebergFrozenRewriteBranchInput::try_new(
                BTreeSet::from([EntryIdentity::DataFile {
                    path: format!("s3://bucket/table/data/source-{group}.parquet"),
                }]),
                BTreeSet::new(),
            )
            .unwrap()
        })
        .collect();
    let targets = (0..groups)
        .map(|group| {
            IcebergSealedWriteTarget::new(
                WriteTargetOrdinal::try_new(group).unwrap(),
                IcebergWriteBranch::Data,
            )
        })
        .collect();
    IcebergWriteCommitSession::try_new_sealed(
        IcebergWriteSessionId::new(),
        table,
        IcebergWriteFlavor::DistributedRewrite,
        facts,
        targets,
    )
    .unwrap()
}

fn lineage() -> IcebergColumnStats {
    IcebergColumnStats {
        null_value_counts: BTreeMap::from([(ROW_ID, 0), (SEQUENCE, 0)]),
        lower_bounds: BTreeMap::from([
            (ROW_ID, 104_i64.to_le_bytes().to_vec()),
            (SEQUENCE, 3_i64.to_le_bytes().to_vec()),
        ]),
        upper_bounds: BTreeMap::from([
            (ROW_ID, 117_i64.to_le_bytes().to_vec()),
            (SEQUENCE, 24_i64.to_le_bytes().to_vec()),
        ]),
        ..IcebergColumnStats::default()
    }
}

fn output(
    name: &str,
    stats: Option<IcebergColumnStats>,
    first: Option<i64>,
) -> IcebergCommitFragment {
    IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            format!("s3://bucket/table/data/{name}.parquet"),
            IcebergFileFormat::Parquet,
            sample_partition(),
            IcebergArtifactMetrics::try_new(2, 1024, vec![], stats).unwrap(),
            first,
        )
        .unwrap(),
    )
}

fn start(
    state: &mut IcebergReceiving,
    handle: &IcebergWriteCommitSession,
    target: WriteTargetOrdinal,
) -> ConnectorWriteQueryIdentity {
    let query = write_query_identity();
    state
        .begin_query(query, &[target], &mut None, handle.targets(), None)
        .unwrap();
    query
}

#[test]
fn compact_rewrite_freezes_data_age_and_lineage_on_the_original_paid_arrival() {
    // Stored lineage can be absent or partly null. It never supplies a file base.
    for (version, seq, stats) in [
        (1, SeqField::Inherit, None),
        (2, SeqField::Explicit(27), None),
        (3, SeqField::Explicit(27), None),
        (
            3,
            SeqField::Explicit(27),
            Some(IcebergColumnStats {
                null_value_counts: BTreeMap::from([(ROW_ID, 1), (SEQUENCE, 1)]),
                ..lineage()
            }),
        ),
    ] {
        let adapter = adapter("optimize-receiving", 1);
        let handle = rewrite_session(version, 1);
        let target = handle.targets()[0].ordinal();
        let (originals, author, live) = paid_fragments_counted(
            &adapter,
            vec![adapter.wrap_commit_fragment(output("rewrite", stats, None))],
        );
        let original_bytes = live.load(Ordering::SeqCst);
        handle
            .with_active_receiving(|state| {
                let query = start(state, &handle, target);
                state.arrive_original(
                    &handle,
                    &adapter,
                    query,
                    originals
                        .into_iter()
                        .map(|fragment| (target, fragment))
                        .collect(),
                    &author,
                )?;
                state.confirm(query)?;
                let compact = state.compact_files();
                assert_eq!(compact.counts.data.rows, 2);
                let mut entries = compact.entries();
                let entry = entries.next().expect("rewrite has one paid addition");
                assert!(entries.next().is_none());
                let file = entry.addition.as_ref().unwrap();
                assert_eq!(entry.receipt.original_bytes, original_bytes);
                assert_eq!(file.data_sequence, seq);
                assert!(live.load(Ordering::SeqCst) >= original_bytes);
                Ok(())
            })
            .unwrap();
        drop(handle.begin_commit().unwrap());
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}

#[test]
fn invalid_stored_lineage_refuses_the_entire_root_record_and_returns_its_receipts() {
    let adapter = adapter("optimize-receiving", 1);
    let handle = rewrite_session(3, 1);
    let target = handle.targets()[0].ordinal();
    handle
        .with_active_receiving(|state| {
            let query = start(state, &handle, target);
            for (bad, supplied_marker, detail) in [
                (
                    {
                        let mut stats = lineage();
                        stats
                            .lower_bounds
                            .insert(ROW_ID, (-1_i64).to_le_bytes().to_vec());
                        stats
                    },
                    None,
                    "stored lineage lower bound is negative",
                ),
                (
                    {
                        let mut stats = lineage();
                        stats
                            .lower_bounds
                            .insert(SEQUENCE, (-1_i64).to_le_bytes().to_vec());
                        stats
                    },
                    None,
                    "stored lineage lower bound is negative",
                ),
                (
                    {
                        let mut stats = lineage();
                        stats.lower_bounds.insert(SEQUENCE, vec![0; 7]);
                        stats
                    },
                    None,
                    "stored lineage lower bound is not an Iceberg Long",
                ),
                (
                    lineage(),
                    Some(104),
                    "must not supply a first-row-id marker",
                ),
            ] {
                let (originals, author, live) = paid_fragments_counted(
                    &adapter,
                    vec![
                        adapter.wrap_commit_fragment(output("valid", Some(lineage()), None)),
                        adapter.wrap_commit_fragment(output("bad", Some(bad), supplied_marker)),
                    ],
                );
                let error = state
                    .arrive_original(
                        &handle,
                        &adapter,
                        query,
                        originals
                            .into_iter()
                            .map(|fragment| (target, fragment))
                            .collect(),
                        &author,
                    )
                    .unwrap_err();
                assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
                assert!(error.message().contains(detail), "{error}");
                assert_eq!(live.load(Ordering::SeqCst), 0);
                let compact = state.compact_files();
                assert!(compact.is_empty());
                assert_eq!(compact.counts.data.rows, 0);
            }

            // The previously refused records installed no duplicate keys.
            let (originals, author, _) = paid_fragments_counted(
                &adapter,
                vec![adapter.wrap_commit_fragment(output("valid", Some(lineage()), None))],
            );
            state.arrive_original(
                &handle,
                &adapter,
                query,
                originals
                    .into_iter()
                    .map(|fragment| (target, fragment))
                    .collect(),
                &author,
            )?;
            state.confirm(query)
        })
        .unwrap();
    assert!(handle.begin_commit().is_ok());
}

#[test]
fn finish_requires_each_exact_group_to_confirm_even_when_its_output_is_empty() {
    let handle = rewrite_session(2, 2);
    let zero = handle.targets()[0].ordinal();
    let one = handle.targets()[1].ordinal();
    let adapter = adapter("optimize-receiving", 1);

    handle
        .with_active_receiving(|state| {
            let error = state
                .begin_query(
                    write_query_identity(),
                    &[zero, one],
                    &mut None,
                    handle.targets(),
                    None,
                )
                .unwrap_err();
            assert!(error.message().contains("exactly one frozen group"));
            let query = start(state, &handle, one);
            let (originals, author, live) = paid_fragments_counted(
                &adapter,
                vec![adapter.wrap_commit_fragment(output("foreign-target", None, None))],
            );
            let error = state
                .arrive_original(
                    &handle,
                    &adapter,
                    query,
                    originals
                        .into_iter()
                        .map(|fragment| (zero, fragment))
                        .collect(),
                    &author,
                )
                .unwrap_err();
            assert!(error.message().contains("outside the sealed set"));
            assert_eq!(live.load(Ordering::SeqCst), 0);
            state.confirm(query)?;
            // Abort can still prove actual completion of the original, partial
            // production; publication needs affirmative proof of every group.
            state.require_complete()
        })
        .unwrap();
    assert!(
        handle
            .begin_commit()
            .err()
            .unwrap()
            .message()
            .contains("does not cover")
    );

    handle
        .with_active_receiving(|state| {
            let query = start(state, &handle, zero);
            state.confirm(query)
        })
        .unwrap();
    assert!(handle.begin_commit().is_ok());

    let duplicate = rewrite_session(2, 2);
    duplicate
        .with_active_receiving(|state| {
            for _ in 0..2 {
                let query = start(state, &duplicate, zero);
                state.confirm(query)?;
            }
            Ok(())
        })
        .unwrap();
    assert!(
        duplicate
            .begin_commit()
            .err()
            .unwrap()
            .message()
            .contains("repeats")
    );
}
