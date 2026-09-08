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

//! Snapshot resolution for time travel, mirroring Java `TimeTravelUtil`.

use crate::spec::{CoreOptions, Snapshot, TimeTravelSelector};
use crate::table::SnapshotManager;
use crate::table::TagManager;
use crate::Error;
use std::collections::HashMap;

/// Resolve the snapshot selected by the time-travel options, if any.
///
/// Returns `Ok(None)` when no time-travel selector is configured. Returns an
/// error for invalid or conflicting selectors, and when the selector does not
/// match any snapshot — callers that need Java `tryTravelToSnapshot`'s silent
/// fallback (keep the current schema on failure) handle the `Err` themselves.
pub(crate) async fn travel_to_snapshot(
    snapshot_manager: &SnapshotManager,
    tag_manager: &TagManager,
    options: &HashMap<String, String>,
) -> crate::Result<Option<Snapshot>> {
    let core_options = CoreOptions::new(options);

    match core_options.try_time_travel_selector()? {
        Some(TimeTravelSelector::TimestampMillis(ts)) => {
            match snapshot_manager.earlier_or_equal_time_millis(ts).await? {
                Some(s) => Ok(Some(s)),
                None => Err(Error::DataInvalid {
                    message: format!("No snapshot found with timestamp <= {ts}"),
                    source: None,
                }),
            }
        }
        Some(TimeTravelSelector::Version {
            value: v,
            option_name,
        }) => {
            // `scan.version` is ambiguous by design: tag first, then snapshot id.
            if tag_manager.tag_exists(v).await? {
                resolve_tag(tag_manager, v).await.map(Some)
            } else if let Ok(id) = v.parse::<i64>() {
                snapshot_manager.get_snapshot(id).await.map(Some)
            } else {
                Err(Error::DataInvalid {
                    message: format!("{option_name} '{v}' is not a valid tag name or snapshot id."),
                    source: None,
                })
            }
        }
        Some(TimeTravelSelector::SnapshotId {
            value: v,
            option_name,
        }) => {
            // An explicit snapshot id: parse strictly, never resolve a tag.
            match v.parse::<i64>() {
                Ok(id) => snapshot_manager.get_snapshot(id).await.map(Some),
                Err(_) => Err(Error::DataInvalid {
                    message: format!("{option_name} '{v}' is not a valid snapshot id."),
                    source: None,
                }),
            }
        }
        Some(TimeTravelSelector::TagName {
            value: v,
            option_name: _,
        }) => {
            // An explicit tag name: resolve strictly by tag, never as a snapshot id.
            if tag_manager.tag_exists(v).await? {
                resolve_tag(tag_manager, v).await.map(Some)
            } else {
                Err(Error::DataInvalid {
                    message: format!("Tag '{v}' doesn't exist."),
                    source: None,
                })
            }
        }
        None => Ok(None),
    }
}

/// Fetch a tag known to exist, mapping an unexpectedly-missing tag to an error.
async fn resolve_tag(tag_manager: &TagManager, name: &str) -> crate::Result<Snapshot> {
    match tag_manager.get(name).await? {
        Some(s) => Ok(s),
        None => Err(Error::DataInvalid {
            message: format!("Tag '{name}' doesn't exist."),
            source: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{DataType, IntType, Schema, TableSchema};
    use crate::table::{SnapshotManager, Table, TableCommit, TableWrite, TagManager};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn schema_v0() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn schema_v1() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("age", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(1, &schema)
    }

    fn make_table(file_io: &FileIO, table_path: &str, schema: TableSchema) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "evolved"),
            table_path.to_string(),
            schema,
            None,
        )
    }

    async fn write_schema_file(file_io: &FileIO, table_path: &str, schema: &TableSchema) {
        file_io
            .mkdirs(&format!("{table_path}/schema/"))
            .await
            .unwrap();
        let path = format!("{table_path}/schema/schema-{}", schema.id());
        let content = serde_json::to_string(schema).unwrap();
        file_io
            .new_output(&path)
            .unwrap()
            .write(content.into())
            .await
            .unwrap();
    }

    fn batch_v0(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    fn batch_v1(ids: Vec<i32>, values: Vec<i32>, ages: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
            ArrowField::new("age", ArrowDataType::Int32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
                Arc::new(Int32Array::from(ages)),
            ],
        )
        .unwrap()
    }

    async fn write_and_commit(table: &Table, batch: &RecordBatch) {
        let mut write = TableWrite::new(table, "test-user".to_string()).unwrap();
        write.write_arrow_batch(batch).await.unwrap();
        let messages = write.prepare_commit().await.unwrap();
        let commit = TableCommit::new(table.clone(), "test-user".to_string());
        commit.commit(messages).await.unwrap();
    }

    /// Table with two schema versions and one snapshot per version:
    /// snapshot 1 (schema 0: id, value) and snapshot 2 (schema 1: + age).
    async fn setup_evolved_table() -> (FileIO, String) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/evolved_table";
        for dir in ["snapshot", "manifest"] {
            file_io
                .mkdirs(&format!("{table_path}/{dir}/"))
                .await
                .unwrap();
        }
        write_schema_file(&file_io, table_path, &schema_v0()).await;
        let table_v0 = make_table(&file_io, table_path, schema_v0());
        write_and_commit(&table_v0, &batch_v0(vec![1, 2, 3], vec![10, 20, 30])).await;

        write_schema_file(&file_io, table_path, &schema_v1()).await;
        let table_v1 = make_table(&file_io, table_path, schema_v1());
        write_and_commit(&table_v1, &batch_v1(vec![4, 5], vec![40, 50], vec![14, 15])).await;

        (file_io, table_path.to_string())
    }

    fn latest_table(file_io: &FileIO, table_path: &str) -> Table {
        make_table(file_io, table_path, schema_v1())
    }

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_switches_to_snapshot_schema() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();

        assert_eq!(traveled.schema().id(), 0);
        let names: Vec<&str> = traveled
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect();
        assert_eq!(names, vec!["id", "value"]);
        assert!(traveled.is_time_traveled());
        // Options stay the merged ones, not the historical schema's options.
        assert_eq!(
            traveled.schema().options().get("scan.version"),
            Some(&"1".to_string())
        );
        // The resolved snapshot is cached for scans, and invalidated when the
        // selector changes.
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(1));
        let recopied = traveled.copy_with_options(options(&[("scan.version", "2")]));
        assert!(recopied.travel_snapshot().is_none());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_same_schema_still_rejects_write() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        // Snapshot 2 carries the current schema, so the schema is not
        // switched — but the copy still reads a pinned snapshot, so writing
        // through it is rejected like any other time-travelled copy.
        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "2")]))
            .await
            .unwrap();

        assert_eq!(traveled.schema().id(), 1);
        assert!(!traveled.is_time_traveled());
        assert!(traveled.new_write_builder().new_write().is_err());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_without_selector_is_noop() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let copied = table.copy_with_time_travel(HashMap::new()).await.unwrap();

        assert_eq!(copied.schema(), table.schema());
        assert!(!copied.is_time_traveled());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_invalid_selector_falls_back_silently() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        // Not a tag and not a snapshot id: kept latest, error deferred to scan.
        let copied = table
            .copy_with_time_travel(options(&[("scan.version", "no-such-version")]))
            .await
            .unwrap();
        assert_eq!(copied.schema().id(), 1);
        assert!(!copied.is_time_traveled());

        // Conflicting selectors behave the same.
        let copied = table
            .copy_with_time_travel(options(&[
                ("scan.version", "1"),
                ("scan.timestamp-millis", "123"),
            ]))
            .await
            .unwrap();
        assert_eq!(copied.schema().id(), 1);
        assert!(!copied.is_time_traveled());
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_by_timestamp_and_tag() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.clone());
        let snapshot1 = snapshot_manager.get_snapshot(1).await.unwrap();

        let traveled = table
            .copy_with_time_travel(options(&[(
                "scan.timestamp-millis",
                &snapshot1.time_millis().to_string(),
            )]))
            .await
            .unwrap();
        assert_eq!(traveled.schema().id(), 0);

        let tag_manager = TagManager::new(file_io.clone(), table_path.clone());
        tag_manager.create("v1-tag", &snapshot1).await.unwrap();
        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "v1-tag")]))
            .await
            .unwrap();
        assert_eq!(traveled.schema().id(), 0);
        assert!(traveled.is_time_traveled());
    }

    #[tokio::test]
    async fn test_time_traveled_table_rejects_write() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();

        let err = match traveled.new_write_builder().new_write() {
            Err(e) => e,
            Ok(_) => panic!("expected write rejection on time-travelled table"),
        };
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("time-travel option")),
            "expected write rejection on time-travelled table, got {err:?}"
        );
        // The latest table is unaffected.
        assert!(table.new_write_builder().new_write().is_ok());
    }

    #[tokio::test]
    async fn test_changing_selector_after_travel_fails_scan() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();

        // Merging unrelated options keeps the resolved snapshot/schema pair.
        let recopied = traveled.copy_with_options(options(&[("k", "v")]));
        assert_eq!(recopied.travel_snapshot().map(|s| s.id()), Some(1));

        // Changing the selector without re-resolving leaves a historical
        // schema with no matching snapshot; scanning such a copy must fail
        // instead of evolving another snapshot's files to the stale schema.
        let stale = traveled.copy_with_options(options(&[("scan.version", "2")]));
        assert!(stale.travel_snapshot().is_none());
        let err = stale
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .expect_err("scan after selector change must fail");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("copy_with_time_travel")),
            "expected stale time-travel state error, got {err:?}"
        );

        // Re-resolving through copy_with_time_travel is the supported path.
        let retraveled = traveled
            .copy_with_time_travel(options(&[("scan.version", "2")]))
            .await
            .unwrap();
        assert_eq!(retraveled.schema().id(), 1);
        assert_eq!(retraveled.travel_snapshot().map(|s| s.id()), Some(2));
    }

    #[tokio::test]
    async fn test_invalid_snapshot_id_error_names_original_option() {
        let (file_io, table_path) = setup_evolved_table().await;
        let opts = options(&[("scan.snapshot-id", "abc")]);
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        let err = super::travel_to_snapshot(&sm, &tm, &opts)
            .await
            .expect_err("non-numeric snapshot-id must fail");
        match err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains("scan.snapshot-id"), "got: {message}");
                assert!(!message.contains("scan.version"), "got: {message}");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_time_travel_read_uses_snapshot_schema() {
        use futures::TryStreamExt;

        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();
        let builder = traveled.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let names: Vec<String> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        assert_eq!(names, vec!["id", "value"]);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);

        // Latest table sees both snapshots' data and the evolved schema.
        let builder = table.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches[0].schema().fields().len(), 3);
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5);
    }

    #[tokio::test]
    async fn test_copy_with_time_travel_rejects_unsupported_scan_option() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);
        let err = table
            .copy_with_time_travel(options(&[("scan.watermark", "5")]))
            .await
            .expect_err("unsupported scan option must fail");
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("scan.watermark"))
        );
    }

    #[tokio::test]
    async fn test_has_resolved_travel_snapshot_reflects_resolution() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let resolved = table
            .copy_with_time_travel(options(&[("scan.snapshot-id", "1")]))
            .await
            .unwrap();
        assert!(resolved.has_resolved_travel_snapshot());

        // Nonexistent snapshot id: core silently falls back, helper reports false.
        let unresolved = table
            .copy_with_time_travel(options(&[("scan.snapshot-id", "999")]))
            .await
            .unwrap();
        assert!(!unresolved.has_resolved_travel_snapshot());

        // No selector at all: false.
        let none = table.copy_with_time_travel(HashMap::new()).await.unwrap();
        assert!(!none.has_resolved_travel_snapshot());
    }

    #[tokio::test]
    async fn test_changing_snapshot_id_or_tag_selector_after_travel_invalidates_cache() {
        let (file_io, table_path) = setup_evolved_table().await;
        let table = latest_table(&file_io, &table_path);

        let traveled = table
            .copy_with_time_travel(options(&[("scan.version", "1")]))
            .await
            .unwrap();
        assert_eq!(traveled.travel_snapshot().map(|s| s.id()), Some(1));

        // scan.snapshot-id and scan.tag-name are first-class time-travel
        // selectors, so re-selecting through either on an already-travelled
        // copy must invalidate the cached snapshot — otherwise the stale
        // snapshot would be reused and the new selector silently ignored.
        for selector in ["scan.snapshot-id", "scan.tag-name"] {
            let stale = traveled.copy_with_options(options(&[(selector, "2")]));
            assert!(
                stale.travel_snapshot().is_none(),
                "{selector} must invalidate the cached travel snapshot"
            );
        }
    }

    #[tokio::test]
    async fn test_snapshot_id_selector_rejects_tag_name() {
        let (file_io, table_path) = setup_evolved_table().await;
        // A tag named "v1-tag" exists, but it is not a valid snapshot id.
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let snapshot1 = sm.get_snapshot(1).await.unwrap();
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        tm.create("v1-tag", &snapshot1).await.unwrap();

        // scan.snapshot-id must only accept a numeric snapshot id; it must not
        // fall back to resolving a tag of the same name.
        let err = super::travel_to_snapshot(&sm, &tm, &options(&[("scan.snapshot-id", "v1-tag")]))
            .await
            .expect_err("scan.snapshot-id must not resolve a tag name");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("scan.snapshot-id")),
            "expected snapshot-id parse error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_tag_name_selector_rejects_snapshot_id() {
        let (file_io, table_path) = setup_evolved_table().await;
        // No tag named "1" exists, but snapshot 1 does.
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let tm = TagManager::new(file_io.clone(), table_path.clone());
        let err = super::travel_to_snapshot(&sm, &tm, &options(&[("scan.tag-name", "1")]))
            .await
            .expect_err("scan.tag-name must not resolve a snapshot id");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("Tag '1'")),
            "expected missing-tag error, got {err:?}"
        );
    }
}
