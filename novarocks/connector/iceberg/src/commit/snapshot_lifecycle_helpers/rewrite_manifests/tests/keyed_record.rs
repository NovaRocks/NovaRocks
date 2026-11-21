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
#[test]
fn rewrite_summary_carries_exact_record_bytes_and_refuses_malformed_current_record() {
    let record = novarocks_spi::connector::ConnectorSnapshotRecord {
        owner: "example.application".into(),
        name: "state".into(),
        binding: None,
        payload: bytes::Bytes::from_static(b"opaque-state"),
    };
    let key = crate::document_storage::keyed::keys::record_key(&record.owner, &record.name);
    let value = crate::document_storage::keyed::record::encode_record(&record).unwrap();
    let mut previous = Summary {
        operation: Operation::Append,
        additional_properties: HashMap::from([(key.clone(), value.clone())]),
    };
    let rewritten = finalize_snapshot_summary(HashMap::new(), Some(&previous)).unwrap();
    assert_eq!(rewritten.get(&key), Some(&value));
    previous
        .additional_properties
        .insert(key, "malformed".into());
    assert!(finalize_snapshot_summary(HashMap::new(), Some(&previous),).is_err());
}

struct RewriteArtifacts {
    io: FileIO,
    token: crate::commit::model::OperationToken,
    attempt: Mutex<crate::commit::model::AttemptToken>,
    ledger: Mutex<crate::commit::model::ArtifactLedger>,
    stage_ids: Mutex<BTreeMap<usize, i64>>,
    reuse: Mutex<crate::commit::model::ManifestReuseIndex>,
}
impl RewriteArtifacts {
    fn new() -> Self {
        let token = crate::commit::model::OperationToken::from_write(
            novarocks_spi::connector::ConnectorWriteOperationId::from_bytes([41; 16]),
        );
        Self {
            io: FileIO::new_with_memory(),
            token,
            attempt: Mutex::new(crate::commit::model::AttemptToken::new(token, 0)),
            ledger: Mutex::new(crate::commit::model::ArtifactLedger::new(token)),
            stage_ids: Mutex::new(BTreeMap::new()),
            reuse: Mutex::new(crate::commit::model::ManifestReuseIndex::default()),
        }
    }
}
impl crate::commit::model::ArtifactWriter for RewriteArtifacts {
    fn rewrite_reuse_backing_bytes(&self, stage: usize) -> crate::iceberg::Result<u64> {
        self.reuse.lock().unwrap().rewrite_backing_bytes(stage)
    }
    fn find_reusable_rewrite(
        &self,
        stage: usize,
        encoding: &[u8],
        latest: &[ManifestFile],
    ) -> crate::iceberg::Result<Option<ManifestRewriteReuse>> {
        self.reuse.lock().unwrap().find_rewrite(
            &self.ledger.lock().unwrap(),
            stage,
            encoding,
            latest,
        )
    }
    fn remember_reusable_rewrite(
        &self,
        stage: usize,
        result: ManifestRewriteReuse,
    ) -> crate::iceberg::Result<()> {
        self.reuse
            .lock()
            .unwrap()
            .remember_rewrite(&self.ledger.lock().unwrap(), stage, result)
    }
    fn file_io(&self) -> &FileIO {
        &self.io
    }
    fn stage_snapshot_id(
        &self,
        stage: usize,
        metadata: &crate::iceberg::spec::TableMetadata,
    ) -> crate::iceberg::Result<i64> {
        let mut ids = self.stage_ids.lock().unwrap();
        Ok(*ids
            .entry(stage)
            .or_insert_with(|| crate::commit::staging::new_snapshot_id(metadata)))
    }
    fn allocate(
        &self,
        kind: crate::commit::model::ArtifactKind,
    ) -> crate::iceberg::Result<crate::commit::model::ObjectIdentity> {
        use crate::commit::model::*;
        let mut ledger = self.ledger.lock().unwrap();
        let object = ObjectIdentity::new(format!(
            "memory://rewrite/staged-{}.{}",
            ledger.records().count(),
            kind.extension()
        ))?;
        ledger.register(ArtifactRecord::produced(
            object.clone(),
            self.attempt_token(),
            kind,
        ))?;
        Ok(object)
    }
    fn attempt_token(&self) -> crate::commit::model::AttemptToken {
        *self.attempt.lock().unwrap()
    }
    fn check_active(&self) -> crate::iceberg::Result<()> {
        Ok(())
    }
    fn snapshot_artifacts(
        &self,
        lists: &[crate::commit::model::ObjectIdentity],
        statistics: &[crate::commit::model::ObjectIdentity],
    ) -> crate::iceberg::Result<crate::commit::model::AttemptArtifacts> {
        self.ledger
            .lock()
            .unwrap()
            .snapshot_for_request(self.attempt_token(), lists, statistics)
    }

    fn record_manifest(
        &self,
        object: &crate::commit::model::ObjectIdentity,
        added_paths: &[String],
        existing_paths: &[String],
        replaces: &[String],
    ) -> crate::iceberg::Result<()> {
        let mut ledger = self.ledger.lock().unwrap();
        // This callback follows the successful FileIO write in the fixture.
        let written = ledger.records().any(|record| {
            record.object == *object
                && record.write_state == crate::commit::model::ArtifactWriteState::Written
        });
        if !written {
            ledger.confirm_written_for_test(object)?;
        }
        ledger.record_manifest(object, added_paths, existing_paths, replaces)
    }

    fn record_manifest_list(
        &self,
        object: &crate::commit::model::ObjectIdentity,
        snapshot_id: i64,
        paths: &[String],
    ) -> crate::iceberg::Result<()> {
        let mut ledger = self.ledger.lock().unwrap();
        // This callback follows the successful FileIO write in the fixture.
        let written = ledger.records().any(|record| {
            record.object == *object
                && record.write_state == crate::commit::model::ArtifactWriteState::Written
        });
        if !written {
            ledger.confirm_written_for_test(object)?;
        }
        ledger.record_manifest_list(object, snapshot_id, paths)
    }
}

#[tokio::test]
async fn staged_rewrite_carries_record_closure_with_base_cas_and_reuses_operation_manifests() {
    use crate::commit::model::*;
    use crate::commit::staging::{StagingBase, StagingEngine};
    use crate::document_storage::keyed::{envelope, keys, record};
    use crate::iceberg::spec::*;
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorDocumentFormat, ConnectorDocumentVersion, ConnectorSnapshotRecord,
        DocumentIdentity,
    };

    let artifacts = RewriteArtifacts::new();
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let version = |name: &str, refs, payload: &'static [u8]| {
        ConnectorDocumentVersion::try_new(
            "novarocks.mv",
            name,
            ConnectorDocumentFormat::try_new("novarocks.mv", name, 1).unwrap(),
            refs,
            Bytes::from_static(payload),
        )
        .unwrap()
    };
    let d = version("definition", vec![], b"definition");
    let l = version(
        "interpretation",
        vec![("definition".into(), d.identity())],
        b"layout",
    );
    let b = version(
        "binding",
        vec![
            ("definition".into(), d.identity()),
            ("interpretation".into(), l.identity()),
        ],
        b"binding",
    );
    let mut expected_closure = [&d, &l, &b]
        .into_iter()
        .map(|version| {
            (
                keys::version_key(version.identity()),
                envelope::encode_version(version).unwrap(),
            )
        })
        .collect::<HashMap<_, _>>();
    let root = keys::root_key("novarocks.mv");
    let mut setup = expected_closure.clone();
    setup.insert(root.clone(), DocumentIdentity::from_bytes([9; 32]).to_hex());
    expected_closure.insert(root.clone(), b.identity().to_hex());
    let expected_records = [
        ConnectorSnapshotRecord {
            owner: "novarocks.mv".into(),
            name: "publication".into(),
            binding: Some(b.identity()),
            payload: Bytes::from_static(b"publication"),
        },
        ConnectorSnapshotRecord {
            owner: "novarocks.test".into(),
            name: "annotation".into(),
            binding: None,
            payload: Bytes::from_static(b"annotation"),
        },
    ]
    .into_iter()
    .map(|entry| {
        (
            keys::record_key(&entry.owner, &entry.name),
            record::encode_record(&entry).unwrap(),
        )
    })
    .collect::<HashMap<_, _>>();
    let base = TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://rewrite/table".into(),
        FormatVersion::V2,
        setup,
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let target = TableTarget {
        ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
        uuid: Some(base.uuid()),
    };
    let seed_intent = OperationIntent::new(OperationIntentParts {
        target: target.clone(),
        target_ref: "main".into(),
        start: None,
        changes: FileChanges::default(),
        dependencies: vec![Dependency::NoRead],
        protocol: crate::commit::model::CommitProtocol::Legacy,
        shape: RequestShape::SnapshotProducing,
        summary: BTreeMap::new(),
        token: artifacts.token,
    })
    .unwrap();
    let mut sources = Vec::new();
    // Real Avro files feed the production preparer; this is staging evidence,
    // not a catalog dispatch or native-runtime publication claim.
    for index in 0..2 {
        let mut writer = ManifestWriterBuilder::new(
            artifacts
                .io
                .new_output(format!("memory://rewrite/source-{index}.avro"))
                .unwrap(),
            Some(77),
            base.current_schema().clone(),
            base.default_partition_spec().as_ref().clone(),
        )
        .build_v2_data();
        writer
            .add_existing_file(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!("memory://rewrite/data-{index}.parquet"))
                    .file_format(DataFileFormat::Parquet)
                    .partition(Struct::empty())
                    .partition_spec_id(0)
                    .record_count(1)
                    .file_size_in_bytes(10)
                    .build()
                    .unwrap(),
                77,
                1,
                Some(1),
            )
            .unwrap();
        sources.push(writer.write_manifest_file().await.unwrap());
    }
    let existing = |metadata| StagingBase::Existing {
        metadata,
        metadata_location: "memory://rewrite/table/metadata/base.metadata.json".into(),
    };
    let mut seed = StagingEngine::begin(existing(base), &seed_intent, &artifacts).unwrap();
    let mut seed_summary = expected_records.clone();
    seed_summary.insert("added-data-files".into(), "2".into());
    seed_summary.insert("added-records".into(), "2".into());
    let change = crate::commit::overwrite::prepare_snapshot_change(
        &seed.view(),
        &seed_intent,
        77,
        Operation::Append,
        sources,
        seed_summary,
        false,
    )
    .await
    .unwrap();
    seed.stage_change(change).unwrap();
    let parent = seed.metadata().clone();
    let intent = OperationIntent::new(OperationIntentParts {
        target,
        target_ref: "main".into(),
        start: Some(StartSnapshot {
            snapshot_id: 77,
            sequence_number: 1,
        }),
        changes: FileChanges::default(),
        dependencies: vec![Dependency::RefUnchanged],
        protocol: crate::commit::model::CommitProtocol::Legacy,
        shape: RequestShape::SnapshotProducing,
        summary: BTreeMap::new(),
        token: artifacts.token,
    })
    .unwrap();
    let preparer = RewriteManifestsPreparer::new();
    let mut paths = Vec::new();
    let mut lists = Vec::new();
    for attempt in 1..=2 {
        *artifacts.attempt.lock().unwrap() = AttemptToken::new(artifacts.token, attempt);
        let mut engine =
            StagingEngine::begin(existing(parent.clone()), &intent, &artifacts).unwrap();
        engine.stage(&preparer).await.unwrap();
        let after = engine.metadata().clone();
        let snapshot = after.current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Replace);
        for (key, value) in &expected_records {
            assert_eq!(
                snapshot.summary().additional_properties.get(key),
                Some(value)
            );
        }
        assert_eq!(after.properties().get(&root), Some(&b.identity().to_hex()));
        let list = crate::manifest::load_manifest_list(snapshot, &artifacts.io, &after)
            .await
            .unwrap();
        assert_eq!(list.entries().len(), 1);
        paths.push(list.entries()[0].manifest_path.clone());
        lists.push(snapshot.manifest_list().to_string());
        let merged = ObjectIdentity::new(paths.last().unwrap().clone()).unwrap();
        let request = engine.freeze().unwrap();
        let expected = std::collections::BTreeSet::from([
            merged,
            ObjectIdentity::new(snapshot.manifest_list().to_owned()).unwrap(),
        ]);
        assert_eq!(
            request
                .artifacts()
                .references()
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            expected
        );
        assert!(
            request
                .requirements()
                .contains(&TableRequirement::UuidMatch {
                    uuid: parent.uuid()
                })
        );
        let cas = TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".into(),
            snapshot_id: Some(77),
        };
        assert_eq!(request.requirements().iter().filter(|requirement|
            matches!(requirement, TableRequirement::RefSnapshotIdMatch { r#ref, .. } if r#ref == "main")
        ).collect::<Vec<_>>(), vec![&cas]);
        assert!(cas.check(Some(&parent)).is_ok());
        assert!(cas.check(Some(&after)).is_err());
        assert!(request.updates().iter().any(|update| matches!(update,
            TableUpdate::SetProperties { updates } if updates == &expected_closure)));
    }
    assert_eq!(
        paths[0], paths[1],
        "The operation reuses its merged manifest"
    );
    assert_ne!(lists[0], lists[1], "Each attempt owns a new manifest-list");
    let ledger = artifacts.ledger.lock().unwrap();
    assert_eq!(
        ledger
            .records()
            .filter(|entry| entry.object.path() == paths[0])
            .count(),
        1
    );
}
