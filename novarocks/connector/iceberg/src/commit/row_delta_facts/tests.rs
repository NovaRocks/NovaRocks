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

use std::sync::atomic::Ordering;

use super::*;
use crate::commit::write_stack::domain::{
    IcebergCommitFragment, IcebergCreationReceipt, IcebergCreationScope,
    IcebergDeleteTargetLocator, IcebergFinalDeleteEffect, IcebergFinalDeleteKind,
};
use crate::commit::write_stack::receiving::compact::CompactEntry;
use crate::commit::write_stack::test_support::{paid_fragments_counted, sample_partition};
use crate::commit::write_stack::tests::{data_fragment, dv_session, fixed_dv_fragment};

#[test]
fn keeps_new_delete_references_separate_and_accounts_for_owned_facts() {
    let v2 = "s3://warehouse/table/v2-target.parquet";
    let dv = "s3://warehouse/table/dv-target.parquet";
    let removed = "s3://warehouse/table/removed.parquet";
    let old = EntryIdentity::DeletionVector {
        path: "s3://warehouse/table/old.puffin".into(),
        offset: 4,
        length: 2,
        referenced_data_file: dv.into(),
    };
    let other_old = EntryIdentity::DeleteFile {
        path: "s3://warehouse/table/old.parquet".into(),
    };
    let removal = IcebergFinalDeleteEffect::try_new(
        IcebergDeleteTargetLocator::try_new(
            "s3://warehouse/table/list.avro".into(),
            "s3://warehouse/table/data.avro".into(),
            0,
            5,
            1,
            removed.into(),
            3,
        )
        .unwrap(),
        3,
        IcebergFinalDeleteKind::RemoveDataFile {
            partition: sample_partition(),
            old_exclusive_deletes: Box::new([other_old.clone()]),
        },
    )
    .unwrap();
    let (handle, adapter) = dv_session();
    let position = handle
        .targets()
        .iter()
        .find(|target| target.branch().writes_deletes())
        .unwrap();
    let creation = IcebergCreationScope::try_new(handle.session_id(), [2; 16], [3; 32]).unwrap();
    let writer = novarocks_spi::connector::write_stack::ConnectorWriterPhysicalContext::new(
        [1; 16],
        1,
        [4; 16],
        0,
        position.ordinal().get(),
    );
    let replacement = fixed_dv_fragment("s3://warehouse/table/new.puffin", dv, vec![old.clone()]);
    let identity = creation
        .identity(
            writer,
            replacement.new_object_path().unwrap(),
            crate::delete_file::IcebergFileFormat::Puffin,
        )
        .unwrap();
    let receipt =
        IcebergCreationReceipt::try_new(writer, [7; 16], identity, 1024, [6; 32]).unwrap();
    let replacement = replacement.try_with_creation(receipt).unwrap();
    let (fragments, author, live) = paid_fragments_counted(
        &adapter,
        [
            data_fragment("s3://warehouse/table/new.parquet"),
            replacement,
            IcebergCommitFragment::final_delete(removal),
        ]
        .into_iter()
        .map(|fragment| adapter.wrap_commit_fragment(fragment))
        .collect(),
    );
    let mut files = ReceivedCompactFiles::default();
    files.v2_targets.insert(v2.into(), sample_partition());
    for fragment in fragments {
        files.push(CompactEntry::from_arrival(&adapter, &mut Some(fragment), 0, &author).unwrap());
    }
    files.author = Some(author);
    let already_paid = live.load(Ordering::Relaxed);
    let (extra, _) = retained_bytes(&files).unwrap();
    let retained = extract(&files).unwrap();
    assert!(extra > 0);
    assert_eq!(live.load(Ordering::Relaxed), already_paid + extra);
    assert_eq!(
        retained.facts.referenced,
        [Arc::from(v2), Arc::from(dv)].into()
    );
    assert_eq!(retained.facts.removed, [Arc::from(removed)].into());
    assert_eq!(retained.facts.dv_targets, [Arc::from(dv)].into());
    assert_eq!(retained.facts.replaced, [other_old, old]);
    drop(files);
    assert_eq!(live.load(Ordering::Relaxed), extra);
    drop(retained);
    assert_eq!(live.load(Ordering::Relaxed), 0);
}
