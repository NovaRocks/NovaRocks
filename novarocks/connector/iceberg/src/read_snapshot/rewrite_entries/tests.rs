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

use super::*;
use crate::commit::model::EntryFacts;
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    DataFileBuilder, DataFileFormat, ManifestWriterBuilder, NestedField, PartitionSpec,
    PrimitiveType, Schema, Struct, Type,
};

#[tokio::test]
async fn selected_facts_survive_loss_of_the_observed_manifest() {
    let io = FileIO::new_with_memory();
    let path = "memory:///e-data.avro";
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
    );
    let mut writer = ManifestWriterBuilder::new(
        io.new_output(path).unwrap(),
        Some(31),
        schema,
        PartitionSpec::unpartition_spec(),
    )
    .build_v3_data();
    for data_path in ["memory:///chosen.parquet", "memory:///other.parquet"] {
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(data_path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .record_count(5)
            .file_size_in_bytes(100)
            .partition(Struct::empty())
            .build()
            .unwrap();
        writer.add_file(file, 3).unwrap();
    }
    let mut descriptor = writer.write_manifest_file().await.unwrap();
    descriptor.sequence_number = 7;
    descriptor.first_row_id = Some(40);
    let source = ManifestSource::committed(41, "memory:///e-list.avro", &descriptor);
    let loaded = source.load(&io).await.unwrap();
    let mut inventory = RewriteEntries::default();
    inventory
        .capture_manifest(source, &loaded, &[], None, || Ok(()))
        .await
        .unwrap();
    drop(loaded);
    io.delete(path).await.unwrap();

    let selected = inventory
        .select(vec!["memory:///chosen.parquet"], Vec::new(), || Ok(()))
        .unwrap();
    assert_eq!(selected.len(), 1);
    let facts = selected[0].facts();
    assert_eq!(facts.added_snapshot_id, Some(31));
    assert_eq!(facts.data_sequence, Some(3));
    assert_eq!(facts.file_sequence, Some(7));
    assert_eq!(facts.first_row_id, Some(40));
    assert_eq!(facts.record_count, 5);
}

fn frozen(identity: EntryIdentity, sequence: i64) -> FrozenEntry {
    FrozenEntry::new(
        identity,
        EntryFacts {
            added_snapshot_id: Some(31),
            data_sequence: Some(sequence),
            file_sequence: Some(7),
            partition_spec_id: 0,
            partition: Struct::empty(),
            record_count: 1,
            first_row_id: None,
        },
    )
    .unwrap()
}

#[test]
fn selects_dv_by_blob_identity_and_only_rejects_selected_delete_ambiguity() {
    let deletion = EntryIdentity::DeleteFile {
        path: "file:///eq.parquet".into(),
    };
    let dv = |offset, target: &str| EntryIdentity::DeletionVector {
        path: "file:///vectors.puffin".into(),
        offset,
        length: 8,
        referenced_data_file: target.into(),
    };
    let selected_dv = dv(12, "file:///b.parquet");
    let observed = RewriteEntries {
        entries: vec![
            frozen(dv(0, "file:///a.parquet"), 3),
            frozen(selected_dv.clone(), 3),
            frozen(deletion.clone(), 3),
            frozen(deletion.clone(), 4),
        ],
    };
    let same_observation = RewriteEntries {
        entries: observed.entries.clone(),
    };

    let selected = observed
        .select(Vec::new(), vec![&selected_dv], || Ok(()))
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].identity(), &selected_dv);

    let error = same_observation
        .select(Vec::new(), vec![&deletion], || Ok(()))
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    assert!(error.message().contains("multiple source entries"));
}
