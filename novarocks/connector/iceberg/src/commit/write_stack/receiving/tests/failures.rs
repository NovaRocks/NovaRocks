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

//! Observes the original prepared-write receipt, exact receiving state and local inventory on refusal.

use super::*;
use crate::commit::write_stack::codec::{IcebergWriteFragmentDecoder, IcebergWriteFragmentEncoder};
use crate::commit::write_stack::domain::IcebergWriteSessionId;
use crate::commit::write_stack::planning::{IcebergWriteSessionPlanInput, plan_write_session};
use crate::commit::write_stack::test_support::{
    data_branch_plan, paid_fragments_counted, table_facts,
};
use novarocks_spi::connector::write_stack::ConnectorWriteAbortProduction;
use novarocks_spi::connector::{
    ConnectorPayloadByteBudget, ConnectorWriteAbortOutcome, ConnectorWriteAdmissionPurpose,
    ConnectorWriteFragmentWireDecoder, ConnectorWriteFragmentWireEncoder,
    ExternalMutationFinalization,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct RefusingWindow {
    used: Arc<AtomicUsize>,
    limit: AtomicUsize,
}

impl ConnectorPayloadByteBudget for RefusingWindow {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        struct Receipt(Arc<AtomicUsize>, usize);
        impl Drop for Receipt {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, Ordering::SeqCst);
            }
        }

        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes)
                    .filter(|total| *total <= self.limit.load(Ordering::SeqCst))
            })
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "original result byte budget is full",
                )
            })?;
        Ok(ConnectorPayloadRetentionGuard::new(Receipt(
            Arc::clone(&self.used),
            bytes,
        )))
    }
}

fn append_session() -> IcebergWriteCommitSession {
    plan_write_session(
        IcebergWriteSessionId::new(),
        IcebergWriteSessionPlanInput {
            flavor: IcebergWriteFlavor::Append,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            table: table_facts(),
            base_version_digest: None,
            staged_metadata: None,
            data: data_branch_plan(),
            deletes: Vec::new(),
        },
    )
    .unwrap()
    .0
}

#[test]
fn a_duplicate_record_leaves_first_receipt_and_no_publication() {
    let lake = FrozenRewriteLake::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("accepted.parquet");
    std::fs::write(&path, b"original accepted object").unwrap();
    let handle = append_session();
    let target = handle.expected_targets()[0];
    let commit = lake.adapter.wrap_commit_session(handle);
    let query = write_query_identity();
    lake.control
        .begin_write_query(&commit, &query, &[target], None)
        .unwrap();
    let wrap = || {
        lake.adapter
            .wrap_commit_fragment(data_fragment(&format!("file://{}", path.display())))
    };
    let (mut originals, author, live) = paid_fragments_counted(&lake.adapter, vec![wrap()]);
    lake.control
        .arrive_write_fragments(
            &commit,
            &query,
            vec![(target, originals.pop().unwrap())],
            &author,
        )
        .unwrap();
    assert!(live.load(Ordering::SeqCst) > 0);
    let (mut duplicates, duplicate_author, duplicate_live) =
        paid_fragments_counted(&lake.adapter, vec![wrap()]);
    let error = lake
        .control
        .arrive_write_fragments(
            &commit,
            &query,
            vec![(target, duplicates.pop().unwrap())],
            &duplicate_author,
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert_eq!(duplicate_live.load(Ordering::SeqCst), 0);
    let handle = lake.adapter.commit_session(&commit).unwrap();
    handle
        .with_active_receiving(|state| {
            assert!(state.require_complete().is_err());
            assert_eq!(state.compact_files().unwrap().entries().count(), 1);
            Ok(())
        })
        .unwrap();
    assert!(handle.retained_publication().unwrap().is_none());
    let outcome = lake
        .control
        .abort_write(ConnectorWriteSessionAbortRequest {
            commit: &commit,
            context: request_context(),
            production: ConnectorWriteAbortProduction::Unproven,
        })
        .unwrap();
    assert!(
        matches!(outcome, ConnectorWriteAbortOutcome::KnownUncommitted { cleanup } if !matches!(cleanup, ExternalMutationFinalization::Complete))
    );
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(path.is_file());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn compact_top_up_refusal_releases_only_the_unaccepted_record() {
    let lake = FrozenRewriteLake::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unaccepted.parquet");
    std::fs::write(&path, b"original unaccepted object").unwrap();
    let handle = append_session();
    let target = handle.expected_targets()[0];
    let commit = lake.adapter.wrap_commit_session(handle);
    let query = write_query_identity();
    lake.control
        .begin_write_query(&commit, &query, &[target], None)
        .unwrap();

    let budget = Arc::new(RefusingWindow::default());
    budget.limit.store(usize::MAX, Ordering::SeqCst);
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(Arc::clone(&budget));
    let encoded = IcebergWriteFragmentEncoder::new(lake.adapter.clone())
        .encode_commit_fragment_payload(
            &lake
                .adapter
                .wrap_commit_fragment(data_fragment(&format!("file://{}", path.display()))),
        )
        .unwrap();
    let fragment = IcebergWriteFragmentDecoder::new(lake.adapter.clone())
        .decode_commit_fragment_payload(&encoded, &author)
        .unwrap();
    budget
        .limit
        .store(budget.used.load(Ordering::SeqCst), Ordering::SeqCst);
    let refusal = lake
        .control
        .arrive_write_fragments(&commit, &query, vec![(target, fragment)], &author)
        .unwrap_err();
    assert_eq!(refusal.kind(), ConnectorErrorKind::ResourceExhausted);
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
    let handle = lake.adapter.commit_session(&commit).unwrap();
    handle
        .with_active_receiving(|state| {
            assert!(state.require_complete().is_err());
            assert!(state.is_empty());
            Ok(())
        })
        .unwrap();
    assert!(handle.retained_publication().unwrap().is_none());
    let outcome = lake
        .control
        .abort_write(ConnectorWriteSessionAbortRequest {
            commit: &commit,
            context: request_context(),
            production: ConnectorWriteAbortProduction::Unproven,
        })
        .unwrap();
    assert!(
        matches!(outcome, ConnectorWriteAbortOutcome::KnownUncommitted { cleanup } if !matches!(cleanup, ExternalMutationFinalization::Complete))
    );
    assert!(path.is_file());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
