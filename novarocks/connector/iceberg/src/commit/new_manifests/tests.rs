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

//! Exercises the one-stage manifest writer and observes actual receipt releases.

mod delete_encoding;
mod format_guard;

mod lower_bound;
pub(in crate::commit) mod registered;
mod rewrite_equality;
mod rolling;

use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_spi::connector::{
    ConnectorPayloadByteBudget, ConnectorWriteFragmentWireDecoder,
    ConnectorWriteFragmentWireEncoder,
};

use super::*;
use crate::commit::model::ObjectIdentity;
use crate::commit::overwrite::preparer_tests::Fixture;
use crate::commit::write_stack::codec::{IcebergWriteFragmentDecoder, IcebergWriteFragmentEncoder};
use crate::commit::write_stack::domain::IcebergWriteTableFacts;
use crate::commit::write_stack::tests::{adapter, data_fragment};

#[derive(Default)]
struct Window(Arc<AtomicUsize>);

struct Receipt(Arc<AtomicUsize>, usize);

impl Drop for Receipt {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::SeqCst);
    }
}

impl ConnectorPayloadByteBudget for Window {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> std::result::Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        self.0.fetch_add(bytes, Ordering::SeqCst);
        Ok(ConnectorPayloadRetentionGuard::new(Receipt(
            self.0.clone(),
            bytes,
        )))
    }
}

pub(in crate::commit) fn install_appends(
    fixture: &Fixture,
    metadata: &TableMetadata,
    count: usize,
) -> Arc<AtomicUsize> {
    install_appends_with_adoption(fixture, metadata, count, |_| true)
}

pub(in crate::commit) fn install_appends_with_adoption(
    fixture: &Fixture,
    metadata: &TableMetadata,
    count: usize,
    adopt: impl Fn(usize) -> bool,
) -> Arc<AtomicUsize> {
    install_compact_files(fixture, metadata, count, None, adopt)
}

pub(in crate::commit) fn install_rewrite(fixture: &Fixture, metadata: &TableMetadata) {
    let snapshot = metadata.current_snapshot().unwrap();
    let facts = IcebergWriteTableFacts::try_new(
        metadata.uuid().to_string(),
        "db".into(),
        "t".into(),
        metadata.location().to_owned(),
        format!("{}/data", metadata.location()),
        "main".into(),
        Some(snapshot.snapshot_id()),
        snapshot.sequence_number(),
        metadata.current_schema_id(),
        metadata.default_partition_spec_id(),
        metadata.format_version() as u8,
    )
    .unwrap();
    install_compact_files(fixture, metadata, 1, Some(&facts), |_| true);
}

fn install_compact_files(
    fixture: &Fixture,
    metadata: &TableMetadata,
    count: usize,
    rewrite: Option<&IcebergWriteTableFacts>,
    adopt: impl Fn(usize) -> bool,
) -> Arc<AtomicUsize> {
    let window = Arc::new(Window::default());
    let live = window.0.clone();
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(window);
    let adapter = adapter("compact-manifests", 1);
    let encoder = IcebergWriteFragmentEncoder::new(adapter.clone());
    let decoder = IcebergWriteFragmentDecoder::new(adapter.clone());
    let mut received = ReceivedCompactFiles::default();
    for ordinal in 0..count {
        let path = format!(
            "file://{}/data/compact-{ordinal:04}.parquet",
            fixture.directory.path().display(),
        );
        let fragment = adapter.wrap_commit_fragment(data_fragment(&path));
        let encoded = encoder.encode_commit_fragment_payload(&fragment).unwrap();
        let mut original = Some(
            decoder
                .decode_commit_fragment_payload(&encoded, &author)
                .unwrap(),
        );
        let mut pending =
            CompactEntry::prepare(&adapter, original.as_ref().unwrap(), 0, &author, rewrite)
                .unwrap();
        let entry = pending.finish(&adapter, &mut original).unwrap();
        if adopt(ordinal) {
            fixture
                .operation
                .adopt_session_data(
                    ObjectIdentity::from_shared_path(
                        entry.addition.as_ref().unwrap().object_path.clone(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        received.counts = received.counts.checked_add(entry.counts).unwrap();
        received.push(entry);
    }
    received.author = Some(author);
    fixture
        .operation
        .install_stage_new_files(
            0,
            PendingNewFiles {
                received,
                source: Arc::new(metadata.clone()),
                codec: CompactFragmentCodec::for_adapter(&adapter),
            },
        )
        .unwrap();
    live
}
