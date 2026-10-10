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

//! Whole-table overwrite preparation and shared snapshot assembly.
//!
//! Live data and delete entries retain their original identity and source facts
//! when removed. The current attempt owns new manifests and the exact list's
//! actual row-ID allocation; frozen data files remain operation-owned inputs.

use std::collections::{BTreeMap, HashMap};

use crate::iceberg::spec::{
    DataContentType, FormatVersion, ManifestContentType, ManifestFile, Operation, Snapshot,
    SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::{TableRequirement, TableUpdate};
use async_trait::async_trait;

use super::action::merge_snapshot_summary_properties;
use super::helpers::{finalize_snapshot_summary, now_ms};

/// Prepare a whole-table overwrite against the current target-ref state.
/// The intent owns added files; this attempt owns only new metadata artifacts.
pub(crate) struct OverwritePreparer;

#[async_trait]
impl super::staging::Preparer for OverwritePreparer {
    async fn prepare(
        &self,
        view: &super::staging::StagedView<'_>,
        intent: &super::model::OperationIntent,
    ) -> crate::iceberg::Result<super::staging::PreparedChange> {
        validate_added_data(intent)?;
        let parent = view
            .metadata()
            .snapshot_for_ref(intent.target_ref())
            .map(|s| s.snapshot_id());
        let mut inputs =
            super::dependency::ValidationInputs::new(view.metadata(), parent, view.artifacts());
        let removed: Vec<_> = inputs.live_set().await?.values().cloned().collect();
        if removed.is_empty() && intent.changes().added.is_empty() && intent.summary().is_empty() {
            return Ok(super::staging::PreparedChange::default());
        }
        let snapshot_id = super::staging::new_snapshot_id(view.metadata());
        let mut manifests = write_live_entry_groups(
            view,
            snapshot_id,
            removed.iter().cloned().map(|e| (e, true)).collect(),
        )
        .await?;
        manifests.extend(write_added_intent_data(view, intent, snapshot_id).await?);
        let operation = match (intent.changes().added.is_empty(), removed.is_empty()) {
            (false, true) => Operation::Append,
            (true, false) => Operation::Delete,
            _ => Operation::Overwrite,
        };
        let mut summary = snapshot_file_summary(&intent.changes().added, &removed)?;
        // Every old entry is removed, so the new row total is fully known even
        // when the parent's total includes already-deleted rows or is absent.
        summary.insert("total-records".into(), summary["added-records"].clone());
        prepare_snapshot_change(
            view,
            intent,
            snapshot_id,
            operation,
            manifests,
            summary,
            false,
        )
        .await
    }
}

pub(crate) fn validate_added_data(
    intent: &super::model::OperationIntent,
) -> crate::iceberg::Result<()> {
    for added in &intent.changes().added {
        if added.file().content_type() != DataContentType::Data {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "Data-only preparer received a delete entry",
            ));
        }
        if added.data_sequence() != super::model::SeqField::Inherit {
            return Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                "New logical data must inherit its publication sequence",
            ));
        }
    }
    Ok(())
}

/// Added files retain their explicitly frozen spec, never inferred from a tuple.
pub(crate) async fn write_added_intent_data(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    snapshot_id: i64,
) -> crate::iceberg::Result<Vec<ManifestFile>> {
    if intent.changes().added.is_empty() {
        return Ok(Vec::new());
    }
    let metadata = view.metadata();
    let mut groups: BTreeMap<i32, Vec<_>> = BTreeMap::new();
    for added in &intent.changes().added {
        groups
            .entry(added.partition_spec_id())
            .or_default()
            .push(added.clone());
    }
    let mut manifests = Vec::new();
    for (spec_id, files) in groups {
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                format!("Added data references missing partition spec {spec_id}"),
            )
        })?;
        let partition_type = spec.partition_type(metadata.current_schema().as_ref())?;
        for added in &files {
            if added.file().partition().fields().len() != partition_type.fields().len() {
                return Err(crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Added data partition tuple does not match its frozen spec",
                ));
            }
            for (value, field) in added.file().partition().iter().zip(partition_type.fields()) {
                if let Some(value) = value {
                    value.clone().try_into_json(field.field_type.as_ref())?;
                }
            }
        }
        manifests.push(
            super::staging::write_manifest(
                view.artifacts(),
                metadata.format_version(),
                snapshot_id,
                metadata.current_schema().clone(),
                spec.as_ref().clone(),
                ManifestContentType::Data,
                files
                    .into_iter()
                    .map(super::staging::ManifestEntryWrite::Added),
            )
            .await?,
        );
    }
    Ok(manifests)
}

/// Carry true entry facts under their original partition spec. Assigned and
/// unassigned data groups stay separate so historic first assignment cannot
/// consume a second range for entries whose row IDs already exist.
pub(crate) async fn write_live_entry_groups(
    view: &super::staging::StagedView<'_>,
    snapshot_id: i64,
    entries: Vec<(super::dependency::LiveEntry, bool)>,
) -> crate::iceberg::Result<Vec<ManifestFile>> {
    let mut groups: BTreeMap<(i32, bool, bool, bool), Vec<super::dependency::LiveEntry>> =
        BTreeMap::new();
    for (entry, deleted) in entries {
        let is_data = entry.file.content_type() == DataContentType::Data;
        let assigned = !deleted && is_data && entry.frozen.facts().first_row_id.is_some();
        groups
            .entry((
                entry.frozen.facts().partition_spec_id,
                is_data,
                deleted,
                assigned,
            ))
            .or_default()
            .push(entry);
    }
    let mut manifests = Vec::new();
    let metadata = view.metadata();
    for ((spec_id, is_data, deleted, assigned), entries) in groups {
        let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
            crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::DataInvalid,
                format!("Live entry references missing partition spec {spec_id}"),
            )
        })?;
        let first = if assigned {
            entries
                .iter()
                .filter_map(|e| e.frozen.facts().first_row_id)
                .min()
                .map(u64::try_from)
                .transpose()
                .map_err(|_| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Assigned row ID is negative",
                    )
                })?
        } else {
            None
        };
        let mut manifest = super::staging::write_manifest(
            view.artifacts(),
            metadata.format_version(),
            snapshot_id,
            metadata.current_schema().clone(),
            spec.as_ref().clone(),
            if is_data {
                ManifestContentType::Data
            } else {
                ManifestContentType::Deletes
            },
            entries.into_iter().map(|entry| {
                if deleted {
                    super::staging::ManifestEntryWrite::Deleted {
                        file: entry.file,
                        frozen: entry.frozen,
                    }
                } else {
                    super::staging::ManifestEntryWrite::Existing {
                        file: entry.file,
                        frozen: entry.frozen,
                    }
                }
            }),
        )
        .await?;
        if metadata.format_version() == FormatVersion::V3 && assigned {
            manifest.first_row_id = first;
        }
        manifests.push(manifest);
    }
    Ok(manifests)
}

/// Shared snapshot assembly uses the actual successful manifest-list allocation.
pub(crate) async fn prepare_snapshot_change(
    view: &super::staging::StagedView<'_>,
    intent: &super::model::OperationIntent,
    snapshot_id: i64,
    operation: Operation,
    manifests: Vec<ManifestFile>,
    properties: HashMap<String, String>,
    truncate_full_table: bool,
) -> crate::iceberg::Result<super::staging::PreparedChange> {
    let metadata = view.metadata();
    let parent = metadata
        .snapshot_for_ref(intent.target_ref())
        .map(|s| s.snapshot_id());
    let parent_summary = parent
        .and_then(|id| metadata.snapshot_by_id(id))
        .map(|s| s.summary());
    // A row-level preparer can calculate visible rows from its exact delete
    // applicability facts. File counters alone cannot reconstruct that value.
    let exact_records = properties.get("total-records").cloned();
    let mut finalized = finalize_snapshot_summary(properties, parent_summary, truncate_full_table);
    if let Some(total) = exact_records {
        finalized.insert("total-records".into(), total);
    }
    let properties = merge_snapshot_summary_properties(
        finalized,
        intent.summary(),
        metadata.uuid(),
        snapshot_id,
    )
    .map_err(to_iceberg_unexpected)?;
    let list = super::staging::write_manifest_list(
        view.artifacts(),
        metadata,
        snapshot_id,
        parent,
        manifests,
    )
    .await?;
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(metadata.next_sequence_number())
        .with_timestamp_ms(now_ms().max(metadata.last_updated_ms()))
        .with_manifest_list(list.object.path().to_string())
        .with_schema_id(metadata.current_schema_id())
        .with_summary(Summary {
            operation,
            additional_properties: properties,
        });
    let snapshot = match list.row_range {
        Some((first, count)) => snapshot.with_row_range(first, count).build(),
        None => snapshot.build(),
    };
    let retention = metadata
        .refs()
        .get(intent.target_ref())
        .map(|r| r.retention.clone())
        .unwrap_or(SnapshotRetention::Branch {
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        });
    Ok(super::staging::PreparedChange {
        updates: vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: intent.target_ref().to_string(),
                reference: SnapshotReference {
                    snapshot_id,
                    retention,
                },
            },
        ],
        requirements: vec![
            TableRequirement::CurrentSchemaIdMatch {
                current_schema_id: metadata.current_schema_id(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: intent.target_ref().to_string(),
                snapshot_id: parent,
            },
        ],
    })
}

/// Snapshot sizes count logical delete blobs rather than shared Puffin containers.
pub(crate) fn set_visible_rows_after_removal(
    view: &super::staging::StagedView<'_>,
    removed: &[super::dependency::LiveEntry],
    summary: &mut HashMap<String, String>,
) -> crate::iceberg::Result<()> {
    let Some(total) = view
        .metadata()
        .snapshot_for_ref(view.target_ref())
        .and_then(|s| s.summary().additional_properties.get("total-records"))
    else {
        return Ok(());
    };
    let invalid =
        |message| crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message);
    let parent: u64 = total
        .parse()
        .map_err(|_| invalid("Parent total-records is invalid"))?;
    let data_paths: std::collections::BTreeSet<_> = removed
        .iter()
        .filter_map(|entry| match entry.frozen.identity() {
            super::model::EntryIdentity::DataFile { path } => Some(path),
            _ => None,
        })
        .collect();
    let deleted_vectors = removed.iter().filter(|entry| matches!(entry.frozen.identity(),
        super::model::EntryIdentity::DeletionVector { referenced_data_file, .. } if data_paths.contains(referenced_data_file)
    )).try_fold(0u64, |sum, entry| sum.checked_add(entry.file.record_count())
        .ok_or_else(|| invalid("Removed deletion-vector cardinality overflow")))?;
    let removed_data: u64 = summary["deleted-records"]
        .parse()
        .expect("checked summary count");
    let added: u64 = summary["added-records"]
        .parse()
        .expect("checked summary count");
    let removed_live = removed_data
        .checked_sub(deleted_vectors)
        .ok_or_else(|| invalid("Removed deletion-vector cardinality exceeds removed data rows"))?;
    let total = parent
        .checked_sub(removed_live)
        .and_then(|n| n.checked_add(added))
        .ok_or_else(|| invalid("Visible row total overflow or underflow"))?;
    summary.insert("total-records".into(), total.to_string());
    Ok(())
}

/// Snapshot sizes count logical delete blobs rather than shared Puffin containers.
pub(crate) fn snapshot_file_summary(
    added: &[super::model::AddedContent],
    removed: &[super::dependency::LiveEntry],
) -> crate::iceberg::Result<HashMap<String, String>> {
    let mut counters = BTreeMap::<&str, u64>::new();
    for (file, is_added) in added
        .iter()
        .map(|a| (a.file(), true))
        .chain(removed.iter().map(|e| (&e.file, false)))
    {
        let size = if file.content_type() == DataContentType::PositionDeletes
            && file.file_format() == crate::iceberg::spec::DataFileFormat::Puffin
        {
            file.content_size_in_bytes()
                .ok_or_else(|| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Deletion vector has no blob size",
                    )
                })?
                .try_into()
                .map_err(|_| {
                    crate::iceberg::Error::new(
                        crate::iceberg::ErrorKind::DataInvalid,
                        "Deletion vector blob size is negative",
                    )
                })?
        } else {
            file.file_size_in_bytes()
        };
        let mut bump = |key, value| -> crate::iceberg::Result<()> {
            let total = counters.entry(key).or_default();
            *total = total.checked_add(value).ok_or_else(|| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Snapshot summary counter overflow",
                )
            })?;
            Ok(())
        };
        bump(
            if is_added {
                "added-files-size"
            } else {
                "removed-files-size"
            },
            size,
        )?;
        match file.content_type() {
            DataContentType::Data => {
                bump(
                    if is_added {
                        "added-data-files"
                    } else {
                        "deleted-data-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-records"
                    } else {
                        "deleted-records"
                    },
                    file.record_count(),
                )?;
            }
            DataContentType::PositionDeletes => {
                bump(
                    if is_added {
                        "added-delete-files"
                    } else {
                        "removed-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-position-delete-files"
                    } else {
                        "removed-position-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-position-deletes"
                    } else {
                        "removed-position-deletes"
                    },
                    file.record_count(),
                )?;
            }
            DataContentType::EqualityDeletes => {
                bump(
                    if is_added {
                        "added-delete-files"
                    } else {
                        "removed-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-equality-delete-files"
                    } else {
                        "removed-equality-delete-files"
                    },
                    1,
                )?;
                bump(
                    if is_added {
                        "added-equality-deletes"
                    } else {
                        "removed-equality-deletes"
                    },
                    file.record_count(),
                )?;
            }
        }
    }
    for key in [
        "added-data-files",
        "added-records",
        "added-files-size",
        "added-delete-files",
        "deleted-data-files",
        "deleted-records",
        "removed-files-size",
        "removed-delete-files",
        "added-position-delete-files",
        "removed-position-delete-files",
        "added-position-deletes",
        "removed-position-deletes",
        "added-equality-delete-files",
        "removed-equality-delete-files",
        "added-equality-deletes",
        "removed-equality-deletes",
    ] {
        counters.entry(key).or_default();
    }
    Ok(counters
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect())
}

fn to_iceberg_unexpected(s: String) -> crate::iceberg::Error {
    crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, s)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::commit::attempt::{self, OwnerPublisher, RetryPolicy, TransitionalPublisher};
    use crate::commit::model::{
        AddedContent, Dependency, FileChanges, IcebergCleanupReport, IsolationLevel,
        OperationIntent, OperationIntentParts, OperationToken, PublicationOutcome, RequestShape,
        StartSnapshot, TableTarget,
    };
    use crate::commit::operation::{IcebergCommitOperation, OperationLimits};
    use crate::commit::staging::PreparedChange;
    use crate::iceberg::spec::{
        DataFile, FormatVersion, NestedField, PrimitiveType, Schema, Struct, Type as IcebergType,
    };
    use crate::iceberg::table::Table;
    use crate::iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorStopOwner, ConnectorWriteOperationId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
    };

    fn local_test_binding() -> crate::access_binding::IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

    struct LocalTableFixture {
        catalog: Arc<dyn Catalog>,
        owner: Arc<dyn crate::catalog::NovaRocksCatalog>,
        binding: crate::access_binding::IcebergReadBinding,
        table_ident: TableIdent,
        _warehouse: tempfile::TempDir,
    }

    async fn empty_local_table(format_version: FormatVersion) -> LocalTableFixture {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let binding = local_test_binding();
        let concrete = Arc::new(
            crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                crate::fs_io::build_file_io_for_location(&warehouse_uri, binding.clone()),
                warehouse_uri,
                binding.clone(),
            ),
        );
        let catalog: Arc<dyn Catalog> = concrete.clone();
        let owner =
            crate::catalog::factory::NovaRocksCatalogFactory::adopt_recording_hadoop_for_test(
                concrete,
                catalog.clone(),
            );
        let namespace = NamespaceIdent::new("db".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                IcebergType::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("build schema");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(format_version)
                    .build(),
            )
            .await
            .expect("create table");
        LocalTableFixture {
            catalog,
            owner,
            binding,
            table_ident: TableIdent::new(namespace, "t".to_string()),
            _warehouse: warehouse,
        }
    }

    async fn publish_append(
        fixture: &LocalTableFixture,
        table: &Table,
        target_ref: &str,
        added: Vec<DataFile>,
        summary: BTreeMap<String, String>,
    ) -> i64 {
        let stop = ConnectorStopOwner::new();
        let token = OperationToken::from_write(ConnectorWriteOperationId::from_bytes(
            *uuid::Uuid::now_v7().as_bytes(),
        ));
        let operation = IcebergCommitOperation::new(
            token,
            table.metadata().location(),
            fixture.binding.clone(),
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(60),
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap(),
            crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current()),
            OperationLimits::default(),
        )
        .unwrap();
        let intent = OperationIntent::new(OperationIntentParts {
            target: TableTarget {
                ident: fixture.table_ident.clone(),
                uuid: Some(table.metadata().uuid()),
            },
            target_ref: target_ref.to_string(),
            start: table
                .metadata()
                .snapshot_for_ref(target_ref)
                .map(|snapshot| StartSnapshot {
                    snapshot_id: snapshot.snapshot_id(),
                    sequence_number: snapshot.sequence_number(),
                }),
            changes: FileChanges {
                added: added
                    .into_iter()
                    .map(|file| {
                        AddedContent::new_logical_data(
                            file,
                            table.metadata().default_partition_spec_id(),
                        )
                        .unwrap()
                    })
                    .collect(),
                removed: Vec::new(),
            },
            dependencies: vec![Dependency::NoReadDependency],
            isolation: IsolationLevel::Snapshot,
            shape: RequestShape::SnapshotProducing,
            summary,
            token,
        })
        .unwrap();
        let publisher = OwnerPublisher {
            operation: token,
            marker: None,
            target: TransitionalPublisher {
                catalog: fixture.owner.clone(),
                ident: fixture.table_ident.clone(),
                target_ref: target_ref.to_string(),
                evidence: crate::catalog::error::CatalogCommitEvidence::for_target("db.t")
                    .with_target_uuid(table.metadata().uuid().to_string()),
                recovery_preflight: Arc::new(|request, operation| {
                    let facts = crate::commit::recovery::FrozenPublicationFacts::from_request(
                        request, operation,
                    )?;
                    facts.validate(operation.token())?;
                    let encoded = serde_json::to_vec(&facts).map_err(|error| {
                        crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::Unexpected,
                            "Encode test publication recovery failed",
                        )
                        .with_source(error)
                    })?;
                    if encoded.len() > MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES {
                        return Err(crate::iceberg::Error::new(
                            crate::iceberg::ErrorKind::DataInvalid,
                            "Test publication recovery exceeds evidence capacity",
                        ));
                    }
                    Ok(())
                }),
            },
        };
        let report = attempt::run(
            &operation,
            &intent,
            &PreparedChange::default(),
            &[&super::super::fast_append::FastAppendPreparer],
            &publisher,
            RetryPolicy::from_properties(table.metadata().properties()).unwrap(),
        )
        .await;
        assert!(
            matches!(report.cleanup, IcebergCleanupReport::Complete { .. }),
            "{:?}",
            report.cleanup
        );
        let PublicationOutcome::Committed(proof) = report.publication else {
            panic!(
                "Canonical append must be known committed: {:?}",
                report.publication
            );
        };
        proof.snapshot_id.expect("append snapshot proof")
    }

    async fn append_synthetic_data(
        fixture: &LocalTableFixture,
        table: &Table,
        target_ref: &str,
        path: String,
    ) -> i64 {
        let file = super::preparer_tests::data(&path, 1, Struct::empty());
        publish_append(fixture, table, target_ref, vec![file], BTreeMap::new()).await
    }

    fn pending_document_properties() -> (BTreeMap<String, String>, Vec<u8>) {
        let content = b"publication".to_vec();
        let manifest =
            crate::document_storage::envelope::IcebergDocumentManifestV1 {
                version: crate::document_storage::envelope::DOCUMENT_MANIFEST_VERSION,
                documents: vec![crate::document_storage::envelope::IcebergDocumentEnvelopeV1 {
                version: crate::document_storage::envelope::DOCUMENT_ENVELOPE_VERSION,
                owner: "novarocks.mv".to_string(),
                name: "publication".to_string(),
                format_owner: "novarocks.mv".to_string(),
                format_name: "publication".to_string(),
                format_version: 1,
                revision: novarocks_spi::connector::ConnectorDocumentRevision::for_content(
                    &content,
                )
                .to_bytes(),
                encoded_len: content.len() as u64,
                references: Vec::new(),
                attachment:
                    crate::document_storage::envelope::IcebergDocumentAttachmentV1::CommitOutput,
                carrier: crate::document_storage::envelope::IcebergDocumentCarrierV1::Available {
                    content,
                },
            }],
            };
        let unresolved = crate::document_storage::codec::encode_document_manifest(&manifest)
            .expect("encode prepared publication")
            .to_vec();
        let properties = BTreeMap::from([(
            crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY.to_string(),
            String::from_utf8(unresolved.clone()).expect("manifest is utf8"),
        )]);
        (properties, unresolved)
    }

    async fn live_data_paths(table: &Table, target_ref: &str) -> BTreeSet<String> {
        let Some(snapshot) = table.metadata().snapshot_for_ref(target_ref) else {
            return BTreeSet::new();
        };
        let list = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        let mut paths = BTreeSet::new();
        for descriptor in list.entries() {
            if descriptor.content != ManifestContentType::Data {
                continue;
            }
            let manifest = descriptor.load_manifest(table.file_io()).await.unwrap();
            paths.extend(
                manifest
                    .entries()
                    .iter()
                    .filter(|entry| entry.is_alive())
                    .filter(|entry| entry.data_file().content_type() == DataContentType::Data)
                    .map(|entry| entry.data_file().file_path().to_string()),
            );
        }
        paths
    }

    #[tokio::test]
    async fn metadata_only_document_publication_creates_one_exact_snapshot() {
        let fixture = empty_local_table(FormatVersion::V2).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let before_count = table.metadata().snapshots().len();
        let (snapshot_properties, unresolved) = pending_document_properties();
        let snapshot_id =
            publish_append(&fixture, &table, "main", Vec::new(), snapshot_properties).await;
        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        assert_eq!(reloaded.metadata().current_snapshot_id(), Some(snapshot_id));
        assert_eq!(reloaded.metadata().snapshots().len(), before_count + 1);
        crate::document_storage::publication::validate_expected_manifest(
            reloaded.metadata(),
            snapshot_id,
            &unresolved,
        )
        .expect("validate exact committed output attachment");
        let current = reloaded.metadata().current_snapshot().unwrap();
        assert_eq!(
            current.summary().additional_properties["added-data-files"],
            "0"
        );
        assert_eq!(
            current.summary().additional_properties["added-records"],
            "0"
        );
        assert!(!current.summary().additional_properties.contains_key(
            crate::document_storage::publication::PENDING_DOCUMENT_MANIFEST_PROPERTY,
        ));
    }

    #[tokio::test]
    async fn v06_metadata_only_document_publication_preserves_populated_live_data_files() {
        let fixture = empty_local_table(FormatVersion::V2).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let existing_path = format!("{}/data/existing.parquet", table.metadata().location());
        append_synthetic_data(&fixture, &table, "main", existing_path.clone()).await;
        let populated = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let base_snapshot_id = populated.metadata().current_snapshot_id().unwrap();
        let before_paths = live_data_paths(&populated, "main").await;
        assert_eq!(before_paths, BTreeSet::from([existing_path]));
        let (snapshot_properties, unresolved) = pending_document_properties();
        let snapshot_id = publish_append(
            &fixture,
            &populated,
            "main",
            Vec::new(),
            snapshot_properties,
        )
        .await;
        let published = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        assert_ne!(snapshot_id, base_snapshot_id);
        assert_eq!(
            published.metadata().current_snapshot_id(),
            Some(snapshot_id)
        );
        assert_eq!(live_data_paths(&published, "main").await, before_paths);
        let snapshot = published.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.parent_snapshot_id(), Some(base_snapshot_id));
        assert_eq!(
            snapshot.summary().additional_properties["added-records"],
            "0"
        );
        assert_eq!(
            snapshot.summary().additional_properties["added-data-files"],
            "0"
        );
        crate::document_storage::publication::validate_expected_manifest(
            published.metadata(),
            snapshot_id,
            &unresolved,
        )
        .expect("validate documents on the advanced snapshot");
    }

    #[tokio::test]
    async fn v06_ordinary_non_main_branch_append_remains_supported() {
        let fixture = empty_local_table(FormatVersion::V3).await;
        let table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let first_path = format!("{}/data/main.parquet", table.metadata().location());
        let seed = append_synthetic_data(&fixture, &table, "main", first_path.clone()).await;
        let seeded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let plan = super::super::ref_action::RefActionPlan {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            action: super::super::ref_action::RefAction::CreateBranch {
                name: "dev".to_string(),
                snapshot_id: seed,
                replace: false,
                if_not_exists: false,
                expected_table_uuid: Some(seeded.metadata().uuid()),
            },
        };
        super::super::ref_action::execute_ref_action(fixture.catalog.as_ref(), &seeded, &plan)
            .await
            .expect("create dev branch");
        let branched = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        let main_before = branched.metadata().current_snapshot_id();
        let dev_before = branched.metadata().refs()["dev"].snapshot_id;
        let branch_path = format!("{}/data/dev.parquet", branched.metadata().location());
        let snapshot_id =
            append_synthetic_data(&fixture, &branched, "dev", branch_path.clone()).await;
        let reloaded = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .unwrap();
        assert_eq!(reloaded.metadata().current_snapshot_id(), main_before);
        assert_ne!(snapshot_id, dev_before);
        assert_eq!(reloaded.metadata().refs()["dev"].snapshot_id, snapshot_id);
        assert_eq!(
            reloaded
                .metadata()
                .snapshot_for_ref("dev")
                .unwrap()
                .parent_snapshot_id(),
            Some(dev_before)
        );
        assert_eq!(
            live_data_paths(&reloaded, "main").await,
            BTreeSet::from([first_path.clone()])
        );
        assert_eq!(
            live_data_paths(&reloaded, "dev").await,
            BTreeSet::from([first_path, branch_path])
        );
    }
}

#[cfg(test)]
pub(crate) mod preparer_tests {
    use super::*;
    use crate::commit::model::{
        AddedContent, Dependency, FileChanges, FrozenRequest, IsolationLevel, OperationIntent,
        OperationIntentParts, OperationToken, RequestShape, TableTarget,
    };
    use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
    use crate::commit::staging::{PreparedChange, Preparer, StagingBase, StagingEngine};
    use crate::iceberg::spec::{
        DataFile, DataFileBuilder, DataFileFormat, Manifest, ManifestStatus, NestedField,
        PartitionSpec, PrimitiveType, Schema, SortOrder, Struct, TableMetadata,
        TableMetadataBuilder, Type,
    };
    use crate::iceberg::{NamespaceIdent, TableIdent};
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorStopOwner, ConnectorWriteOperationId,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    pub(crate) struct Fixture {
        pub directory: tempfile::TempDir,
        pub operation: IcebergCommitOperation,
        _stop: ConnectorStopOwner,
    }
    impl Fixture {
        pub(crate) fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let location = format!("file://{}", directory.path().display());
            let runtime = tokio::runtime::Handle::current();
            let binding = crate::access_binding::IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime.clone())),
            );
            let stop = ConnectorStopOwner::new();
            let operation = IcebergCommitOperation::new(
                OperationToken::from_write(ConnectorWriteOperationId::from_bytes([7; 16])),
                location,
                binding,
                ConnectorRequestContext::try_new(
                    Instant::now() + Duration::from_secs(60),
                    stop.view(),
                    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
                )
                .unwrap(),
                crate::resources::IcebergCatalogRuntime::new(runtime),
                OperationLimits::default(),
            )
            .unwrap();
            Self {
                directory,
                operation,
                _stop: stop,
            }
        }
        pub(crate) fn cancel(&self) {
            self._stop.request_stop();
        }

        pub(crate) fn metadata(&self, version: FormatVersion) -> TableMetadata {
            let schema = Schema::builder()
                .with_fields(vec![Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                ))])
                .build()
                .unwrap();
            TableMetadataBuilder::new(
                schema,
                PartitionSpec::unpartition_spec(),
                SortOrder::unsorted_order(),
                format!("file://{}", self.directory.path().display()),
                version,
                HashMap::new(),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata
        }
        pub(crate) fn intent(
            &self,
            metadata: &TableMetadata,
            target_ref: &str,
            added: Vec<AddedContent>,
        ) -> OperationIntent {
            OperationIntent::new(OperationIntentParts {
                target: TableTarget {
                    ident: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
                    uuid: Some(metadata.uuid()),
                },
                target_ref: target_ref.into(),
                start: metadata.snapshot_for_ref(target_ref).map(|s| {
                    crate::commit::model::StartSnapshot {
                        snapshot_id: s.snapshot_id(),
                        sequence_number: s.sequence_number(),
                    }
                }),
                changes: FileChanges {
                    added,
                    removed: Vec::new(),
                },
                dependencies: vec![Dependency::NoReadDependency],
                isolation: IsolationLevel::Snapshot,
                shape: RequestShape::SnapshotProducing,
                summary: BTreeMap::new(),
                token: self.operation.token(),
            })
            .unwrap()
        }
        pub(crate) async fn stage(
            &self,
            metadata: TableMetadata,
            intent: &OperationIntent,
            preparer: &dyn Preparer,
        ) -> (TableMetadata, FrozenRequest) {
            let attempt = self.operation.begin_attempt().unwrap();
            let mut engine = StagingEngine::begin(
                StagingBase::Existing {
                    metadata,
                    metadata_location: format!(
                        "file://{}/base.metadata.json",
                        self.directory.path().display()
                    ),
                },
                intent,
                &attempt,
            )
            .unwrap();
            engine.stage(preparer).await.unwrap();
            let after = engine.metadata().clone();
            (after, engine.freeze(&[]).unwrap())
        }
    }
    pub(crate) fn data(path: &str, count: u64, partition: Struct) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.into())
            .file_format(DataFileFormat::Parquet)
            .record_count(count)
            .file_size_in_bytes(count * 10)
            .partition(partition)
            .build()
            .unwrap()
    }
    fn dv(path: &str, data: &str, offset: i64, length: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.into())
            .file_format(DataFileFormat::Puffin)
            .record_count(1)
            .file_size_in_bytes(1000)
            .partition(Struct::empty())
            .referenced_data_file(Some(data.into()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(length))
            .build()
            .unwrap()
    }
    pub(crate) async fn raw_entries(
        metadata: &TableMetadata,
        target_ref: &str,
        attempt: &IcebergCommitAttempt,
    ) -> Vec<crate::iceberg::spec::ManifestEntry> {
        use crate::commit::model::ArtifactWriter;
        let snapshot = metadata.snapshot_for_ref(target_ref).unwrap();
        let list = snapshot
            .load_manifest_list(attempt.file_io(), metadata)
            .await
            .unwrap();
        let mut entries = Vec::new();
        for manifest in list.entries() {
            let bytes = attempt
                .file_io()
                .new_input(&manifest.manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            entries.extend(
                Manifest::parse_avro(&bytes)
                    .unwrap()
                    .entries()
                    .iter()
                    .map(|e| e.as_ref().clone()),
            );
        }
        entries
    }
    struct Seed(Vec<AddedContent>);
    #[async_trait]
    impl Preparer for Seed {
        async fn prepare(
            &self,
            view: &super::super::staging::StagedView<'_>,
            intent: &OperationIntent,
        ) -> crate::iceberg::Result<PreparedChange> {
            let id = super::super::staging::new_snapshot_id(view.metadata());
            let mut manifests = Vec::new();
            for content in [ManifestContentType::Data, ManifestContentType::Deletes] {
                let entries: Vec<_> = self
                    .0
                    .iter()
                    .filter(|a| {
                        (a.file().content_type() == DataContentType::Data)
                            == (content == ManifestContentType::Data)
                    })
                    .cloned()
                    .map(super::super::staging::ManifestEntryWrite::Added)
                    .collect();
                if entries.is_empty() {
                    continue;
                }
                manifests.push(
                    super::super::staging::write_manifest(
                        view.artifacts(),
                        view.metadata().format_version(),
                        id,
                        view.metadata().current_schema().clone(),
                        view.metadata().default_partition_spec().as_ref().clone(),
                        content,
                        entries,
                    )
                    .await?,
                );
            }
            prepare_snapshot_change(
                view,
                intent,
                id,
                Operation::Append,
                manifests,
                snapshot_file_summary(&self.0, &[])?,
                false,
            )
            .await
        }
    }

    #[tokio::test]
    async fn append_preparer_inherits_sequences_and_assigns_historical_v2_rows() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/old.parquet", 3, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let base = base
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/new.parquet", 2, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (after, request) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let snapshot = after.current_snapshot().unwrap();
        assert_eq!(snapshot.row_range(), Some((0, 5)));
        assert_eq!(after.next_row_id(), 5);
        assert_eq!(snapshot.summary().operation, Operation::Append);
        let attempt = fixture.operation.begin_attempt().unwrap();
        let entries = raw_entries(&after, "main", &attempt).await;
        let new = entries
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/new.parquet")
            .unwrap();
        assert_eq!(new.sequence_number, None);
        assert_eq!(new.file_sequence_number, None);
        assert_eq!(
            request
                .requirements()
                .iter()
                .filter(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn overwrite_preparer_deletes_every_logical_delete_blob_and_uses_actual_sizes() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let seed = vec![
            AddedContent::new_logical_data(data("s3://b/a.parquet", 3, Struct::empty()), 0)
                .unwrap(),
            AddedContent::new_logical_data(data("s3://b/b.parquet", 2, Struct::empty()), 0)
                .unwrap(),
            AddedContent::new_logical_data(dv("s3://b/shared.puffin", "s3://b/a.parquet", 4, 7), 0)
                .unwrap(),
            AddedContent::new_logical_data(
                dv("s3://b/shared.puffin", "s3://b/b.parquet", 11, 11),
                0,
            )
            .unwrap(),
        ];
        let intent = fixture.intent(&base, "main", Vec::new());
        let (base, _) = fixture.stage(base, &intent, &Seed(seed)).await;
        let start = base.current_snapshot_id().unwrap();
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/new.parquet", 2, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (after, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        let snapshot = after.current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(snapshot.row_range(), Some((5, 2)));
        assert_eq!(
            snapshot.summary().additional_properties["removed-files-size"],
            "68"
        );
        assert_eq!(
            snapshot.summary().additional_properties["removed-delete-files"],
            "2"
        );
        let entries =
            raw_entries(&after, "main", &fixture.operation.begin_attempt().unwrap()).await;
        let deleted: Vec<_> = entries
            .iter()
            .filter(|e| e.status == ManifestStatus::Deleted)
            .collect();
        assert_eq!(deleted.len(), 4);
        assert!(
            deleted
                .iter()
                .all(|e| e.snapshot_id == Some(snapshot.snapshot_id())
                    && e.sequence_number == Some(1)
                    && e.file_sequence_number == Some(1))
        );
        let data = deleted
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/a.parquet")
            .unwrap();
        assert_eq!(data.data_file().first_row_id(), Some(0));
        assert_ne!(start, snapshot.snapshot_id());
    }

    #[tokio::test]
    async fn overwrite_labels_distinguish_append_and_delete() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/a.parquet", 3, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        assert_eq!(
            base.current_snapshot().unwrap().summary().operation,
            Operation::Append
        );
        let intent = fixture.intent(&base, "main", Vec::new());
        let (after, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        assert_eq!(
            after.current_snapshot().unwrap().summary().operation,
            Operation::Delete
        );
        assert_eq!(after.current_snapshot().unwrap().row_range(), Some((3, 0)));
    }

    #[tokio::test]
    async fn truncate_preparer_reads_the_target_ref_and_preserves_main() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/a.parquet", 3, Struct::empty()), 0)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let branch_id = base.current_snapshot_id().unwrap();
        let base = base
            .into_builder(None)
            .set_ref(
                "audit",
                SnapshotReference::new(
                    branch_id,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/main-only.parquet", 5, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let main = base.current_snapshot_id();
        let intent = fixture.intent(&base, "audit", Vec::new());
        let (after, request) = fixture
            .stage(base, &intent, &super::super::truncate::TruncatePreparer)
            .await;
        assert_eq!(after.current_snapshot_id(), main);
        let branch = after.snapshot_for_ref("audit").unwrap();
        assert_eq!(branch.parent_snapshot_id(), Some(branch_id));
        assert_eq!(
            branch.summary().additional_properties["deleted-records"],
            "3"
        );
        assert_eq!(branch.summary().additional_properties["total-records"], "0");
        assert_eq!(branch.row_range(), Some((8, 0)));
        assert!(
            request
                .requirements()
                .contains(&TableRequirement::RefSnapshotIdMatch {
                    r#ref: "audit".into(),
                    snapshot_id: Some(branch_id)
                })
        );
        let entries =
            raw_entries(&after, "audit", &fixture.operation.begin_attempt().unwrap()).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].status, ManifestStatus::Deleted);
        assert_eq!(entries[0].data_file().file_path(), "s3://b/a.parquet");
    }

    #[tokio::test]
    async fn dynamic_overwrite_assigns_only_unassigned_survivors_and_new_rows() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let spec = crate::iceberg::spec::UnboundPartitionSpecBuilder::new()
            .add_partition_field(1, "id", crate::iceberg::spec::Transform::Identity)
            .unwrap()
            .build();
        let base = base
            .into_builder(None)
            .add_default_partition_spec(spec)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let spec_id = base.default_partition_spec_id();
        let partition = |id| {
            [Some(crate::iceberg::spec::Literal::long(id))]
                .into_iter()
                .collect::<Struct>()
        };
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/p1.parquet", 3, partition(1)), spec_id)
                    .unwrap(),
                AddedContent::new_logical_data(data("s3://b/p2.parquet", 5, partition(2)), spec_id)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let base = base
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/new-p1.parquet", 2, partition(1)),
                    spec_id,
                )
                .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::overwrite_partitions::OverwritePartitionsPreparer,
            )
            .await;
        assert_eq!(
            base.current_snapshot().unwrap().summary().operation,
            Operation::Overwrite
        );
        assert_eq!(base.current_snapshot().unwrap().row_range(), Some((0, 7)));
        assert_eq!(base.next_row_id(), 7);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/newer-p1.parquet", 1, partition(1)),
                    spec_id,
                )
                .unwrap(),
            ],
        );
        let (after, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::overwrite_partitions::OverwritePartitionsPreparer,
            )
            .await;
        assert_eq!(after.current_snapshot().unwrap().row_range(), Some((7, 1)));
        let entries =
            raw_entries(&after, "main", &fixture.operation.begin_attempt().unwrap()).await;
        let survivor = entries
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/p2.parquet")
            .unwrap();
        assert_eq!(survivor.status, ManifestStatus::Existing);
        assert_eq!(survivor.data_file().first_row_id(), Some(0));
        assert_eq!(survivor.sequence_number, Some(1));
        assert_eq!(survivor.file_sequence_number, Some(1));
    }
    #[tokio::test]
    async fn dynamic_overwrite_removes_referenced_dv_even_with_a_different_partition() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let spec = crate::iceberg::spec::UnboundPartitionSpecBuilder::new()
            .add_partition_field(1, "id", crate::iceberg::spec::Transform::Identity)
            .unwrap()
            .build();
        let base = base
            .into_builder(None)
            .add_default_partition_spec(spec)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let spec_id = base.default_partition_spec_id();
        let partition = |id| {
            [Some(crate::iceberg::spec::Literal::long(id))]
                .into_iter()
                .collect::<Struct>()
        };
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/p1.parquet", 3, partition(1)), spec_id)
                    .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let vector = crate::iceberg::spec::DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("s3://b/shared.puffin".into())
            .file_format(crate::iceberg::spec::DataFileFormat::Puffin)
            .partition(partition(2))
            .partition_spec_id(spec_id)
            .record_count(1)
            .file_size_in_bytes(1000)
            .content_offset(Some(4))
            .content_size_in_bytes(Some(11))
            .referenced_data_file(Some("s3://b/p1.parquet".into()))
            .build()
            .unwrap();
        let vector_id = super::super::model::EntryIdentity::try_from(&vector).unwrap();
        let intent = fixture.intent(
            &base,
            "main",
            vec![AddedContent::new_logical_data(vector, spec_id).unwrap()],
        );
        let (base, _) = fixture
            .stage(base, &intent, &super::super::row_delta::RowDeltaPreparer)
            .await;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/new-p1.parquet", 2, partition(1)),
                    spec_id,
                )
                .unwrap(),
            ],
        );
        let (after, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::overwrite_partitions::OverwritePartitionsPreparer,
            )
            .await;
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut inputs = super::super::dependency::ValidationInputs::new(
            &after,
            after.current_snapshot_id(),
            &attempt,
        );
        let live = inputs.live_set().await.unwrap();
        assert!(!live.contains_key(&vector_id));
        assert!(
            !live.contains_key(&super::super::model::EntryIdentity::DataFile {
                path: "s3://b/p1.parquet".into()
            })
        );
        assert_eq!(
            after
                .current_snapshot()
                .unwrap()
                .summary()
                .additional_properties["removed-delete-files"],
            "1"
        );
    }

    #[tokio::test]
    async fn overwrite_preparer_can_replace_historical_unassigned_data() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/historic.parquet", 3, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (base, _) = fixture
            .stage(
                base,
                &intent,
                &super::super::fast_append::FastAppendPreparer,
            )
            .await;
        let base = base
            .into_builder(None)
            .upgrade_format_version(FormatVersion::V3)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(
                    data("s3://b/replacement.parquet", 2, Struct::empty()),
                    0,
                )
                .unwrap(),
            ],
        );
        let (after, _) = fixture.stage(base, &intent, &OverwritePreparer).await;
        assert_eq!(after.current_snapshot().unwrap().row_range(), Some((0, 2)));
        assert_eq!(after.next_row_id(), 2);
        let entries =
            raw_entries(&after, "main", &fixture.operation.begin_attempt().unwrap()).await;
        let old = entries
            .iter()
            .find(|e| e.data_file().file_path() == "s3://b/historic.parquet")
            .unwrap();
        assert_eq!(old.status, ManifestStatus::Deleted);
        assert_eq!(old.data_file().first_row_id(), None);
        assert_eq!(old.sequence_number, Some(1));
        assert_eq!(old.file_sequence_number, Some(1));
    }

    #[tokio::test]
    async fn append_preparer_refuses_partition_values_that_disagree_with_frozen_spec() {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V3);
        let tuple = [Some(crate::iceberg::spec::Literal::long(1))]
            .into_iter()
            .collect();
        let intent = fixture.intent(
            &base,
            "main",
            vec![
                AddedContent::new_logical_data(data("s3://b/wrong-partition.parquet", 1, tuple), 0)
                    .unwrap(),
            ],
        );
        let attempt = fixture.operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Existing {
                metadata: base,
                metadata_location: "s3://b/base.metadata.json".into(),
            },
            &intent,
            &attempt,
        )
        .unwrap();
        let error = engine
            .stage(&super::super::fast_append::FastAppendPreparer)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), crate::iceberg::ErrorKind::DataInvalid);
        assert!(engine.updates().is_empty());
    }
}
