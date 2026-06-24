# Iceberg MV Repartition Non-PF Shapes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 扩展 `ALTER MATERIALIZED VIEW ... REPARTITION`，让安全的 Iceberg aggregate、join 和 multi-base MV 走 full rebuild repartition，并保留明确的 unsupported shape 错误。

**Architecture:** `repartition_iceberg_mv` 继续是唯一 DDL 入口，仍使用 refresh lock、staging branch、operation lifecycle、partition contract finalize 和 recovery 机制。实现把 shape capability 判定、snapshot pin、rebuild payload 生成、overwrite publish/finalize 分层：projection/filter 复用现有路径，aggregate 使用现有 aggregate first-refresh state chunk 生成逻辑，join projection/filter 使用现有 join full-refresh rewrite/coalescer 生成 apply key 后以 overwrite 提交。operation 记录通过已有 `iceberg.operation.commit_request` 保存 typed repartition intent JSON，不引入新的 Avro schema version。

**Tech Stack:** Rust, Iceberg Rust catalog/commit APIs, NovaRocks standalone SQL analyzer/executor, SQLite metadata repository, existing `sql-tests` runner.

---

## Scope Check

这份 spec 只覆盖 Iceberg-backed MV 的 `ALTER MATERIALIZED VIEW ... REPARTITION`。不改普通 refresh 的语义，不实现 Iceberg multi-spec live-data 读写兼容，不引入并发 refresh/repartition 共享执行。

可以在同一个 PR 内完成，因为所有变更共享同一个 DDL entrypoint、operation lifecycle 和 SQL suite。不要拆到 parser、optimizer 或 connector 独立 PR；这些模块只做必要调用点适配。

## File Structure

- Modify: `src/engine/mv/iceberg_refresh.rs`
  - 新增 `RepartitionSupport`、support validator、typed intent struct、rebuild payload enum。
  - 重构 `repartition_iceberg_mv`，移除 single-base projection/filter-only guard。
  - 把 full rebuild 分成 payload 生成和 overwrite publish/finalize。
  - 增加 aggregate、join、UNION projection/filter full rebuild builders。
  - 增加 recovery finalize 对 repartition partition contract 的重放。
  - 增加 Rust 单测。
- Modify: `src/meta/repository/iceberg_operation.rs`
  - 增加 `record_commit_request` 方法，用已有 `commit_request: Option<String>` 持久化 repartition intent JSON。
- Modify: `tests/meta_repository.rs`
  - 增加 repository 层 commit request 写入和 Avro round-trip 断言。
- Modify: `sql-tests/iceberg-ivm/sql/iceberg_ivm_projection_repartition.sql`
  - 把 aggregate rejection 改成成功 case。
  - 增加 join projection/filter repartition 成功 case。
  - 增加 unsupported shape 的具体错误 case。
- Modify: `sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result`
  - 更新 aggregate 和 join repartition 查询结果。

## Task 1: Repartition Support Classifier

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:2599`

- [ ] **Step 1: Add failing support-classifier tests**

Add these tests inside the existing `#[cfg(test)] mod tests` in `src/engine/mv/iceberg_refresh.rs`.

```rust
#[test]
fn repartition_support_accepts_projection_filter_and_aggregate() {
    let projection = RefreshCapabilities {
        snapshot_policy: BaseSnapshotPolicy::SingleBase,
        has_agg_state: false,
        identity: RefreshIdentity::BaseRowId,
        apply_key_column: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: ApplyKeyValueType::Int64,
        partition_pruning: PartitionPruningPolicy::BestEffort,
    };
    assert_eq!(
        validate_repartition_support(&projection).expect("projection/filter support"),
        RepartitionSupport::ProjectionFilterSingleBase
    );

    let aggregate = RefreshCapabilities {
        snapshot_policy: BaseSnapshotPolicy::SingleBase,
        has_agg_state: true,
        identity: RefreshIdentity::GroupRowId,
        apply_key_column: ICEBERG_MV_GROUP_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: ApplyKeyValueType::Utf8,
        partition_pruning: PartitionPruningPolicy::BestEffort,
    };
    assert_eq!(
        validate_repartition_support(&aggregate).expect("aggregate support"),
        RepartitionSupport::AggregateSingleBase
    );
}

#[test]
fn repartition_support_accepts_join_and_multi_base_shapes() {
    let join = RefreshCapabilities {
        snapshot_policy: BaseSnapshotPolicy::JoinPairPartialInitialSkip,
        has_agg_state: false,
        identity: RefreshIdentity::JoinRowKey,
        apply_key_column: ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: ApplyKeyValueType::Utf8,
        partition_pruning: PartitionPruningPolicy::BestEffort,
    };
    assert_eq!(
        validate_repartition_support(&join).expect("join support"),
        RepartitionSupport::JoinProjectionFilter
    );

    let union_projection = RefreshCapabilities {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        has_agg_state: false,
        identity: RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::BaseRowId)),
        apply_key_column: ICEBERG_MV_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: ApplyKeyValueType::BranchInt64,
        partition_pruning: PartitionPruningPolicy::BestEffort,
    };
    assert_eq!(
        validate_repartition_support(&union_projection).expect("union projection support"),
        RepartitionSupport::UnionProjectionFilter
    );
}

#[test]
fn repartition_support_rejects_specific_unsupported_shape() {
    let invalid = RefreshCapabilities {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        has_agg_state: false,
        identity: RefreshIdentity::JoinRowKey,
        apply_key_column: ICEBERG_MV_JOIN_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: ApplyKeyValueType::Utf8,
        partition_pruning: PartitionPruningPolicy::BestEffort,
    };

    let err = validate_repartition_support(&invalid).expect_err("shape must be rejected");
    assert!(err.contains("UnsupportedRepartitionShape"));
    assert!(err.contains("JoinRowKey"));
    assert!(err.contains("AllBasesRequired"));
    assert!(err.contains("aggregate_state=false"));

    let branch_union_aggregate = RefreshCapabilities {
        snapshot_policy: BaseSnapshotPolicy::AllBasesRequired,
        has_agg_state: true,
        identity: RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId)),
        apply_key_column: ICEBERG_MV_GROUP_APPLY_KEY_COLUMN.to_string(),
        apply_key_value_type: ApplyKeyValueType::BranchUtf8,
        partition_pruning: PartitionPruningPolicy::BestEffort,
    };
    let err = validate_repartition_support(&branch_union_aggregate)
        .expect_err("branch UNION ALL aggregate repartition is unsupported");
    assert!(err.contains("UnsupportedRepartitionShape"));
    assert!(err.contains("BranchScoped"));
    assert!(err.contains("aggregate_state=true"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test --lib repartition_support_ -- --nocapture
```

Expected: FAIL with unresolved `RepartitionSupport` and `validate_repartition_support`.

- [ ] **Step 3: Add support enum and validator**

Insert this block near `repartition_iceberg_mv` in `src/engine/mv/iceberg_refresh.rs`, before the function.

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
enum RepartitionSupport {
    ProjectionFilterSingleBase,
    AggregateSingleBase,
    JoinProjectionFilter,
    JoinAggregate,
    FanInAggregate,
    UnionProjectionFilter,
}

impl RepartitionSupport {
    fn label(&self) -> &'static str {
        match self {
            Self::ProjectionFilterSingleBase => "projection/filter single-base",
            Self::AggregateSingleBase => "aggregate single-base",
            Self::JoinProjectionFilter => "join projection/filter",
            Self::JoinAggregate => "join aggregate",
            Self::FanInAggregate => "fan-in aggregate",
            Self::UnionProjectionFilter => "UNION ALL projection/filter",
        }
    }
}

fn validate_repartition_support(
    caps: &RefreshCapabilities,
) -> Result<RepartitionSupport, String> {
    match (&caps.snapshot_policy, caps.has_agg_state, &caps.identity) {
        (BaseSnapshotPolicy::SingleBase, false, RefreshIdentity::BaseRowId) => {
            Ok(RepartitionSupport::ProjectionFilterSingleBase)
        }
        (BaseSnapshotPolicy::SingleBase, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionSupport::AggregateSingleBase)
        }
        (BaseSnapshotPolicy::JoinPairPartialInitialSkip, false, RefreshIdentity::JoinRowKey) => {
            Ok(RepartitionSupport::JoinProjectionFilter)
        }
        (BaseSnapshotPolicy::JoinPairPartialInitialSkip, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionSupport::JoinAggregate)
        }
        (BaseSnapshotPolicy::AllBasesRequired, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionSupport::FanInAggregate)
        }
        (
            BaseSnapshotPolicy::AllBasesRequired,
            false,
            RefreshIdentity::BranchScoped(inner),
        ) if matches!(inner.as_ref(), RefreshIdentity::BaseRowId) => {
            Ok(RepartitionSupport::UnionProjectionFilter)
        }
        _ => Err(format!(
            "UnsupportedRepartitionShape: ALTER MATERIALIZED VIEW ... REPARTITION does not support identity={:?}, snapshot_policy={:?}, aggregate_state={}; supported shapes are projection/filter single-base, aggregate single-base, join projection/filter, join aggregate, fan-in aggregate, and UNION ALL projection/filter",
            caps.identity, caps.snapshot_policy, caps.has_agg_state
        )),
    }
}
```

- [ ] **Step 4: Replace the old one-shot guard**

In `repartition_iceberg_mv`, replace the current guard:

```rust
if caps.has_agg_state
    || caps.snapshot_policy != BaseSnapshotPolicy::SingleBase
    || caps.identity != RefreshIdentity::BaseRowId
{
    return Err(format!(
        "ALTER MATERIALIZED VIEW ... REPARTITION currently supports single-base projection/filter Iceberg MVs only; {}.{}.{} has identity={:?}, snapshot_policy={:?}, aggregate_state={}",
        target.catalog,
        target.namespace,
        target.table,
        caps.identity,
        caps.snapshot_policy,
        caps.has_agg_state
    ));
}
```

with:

```rust
let support = validate_repartition_support(&caps).map_err(|err| {
    format!(
        "{err}; target={}.{}.{}",
        target.catalog, target.namespace, target.table
    )
})?;
```

- [ ] **Step 5: Run support tests**

Run:

```bash
cargo test --lib repartition_support_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor: classify iceberg mv repartition support"
```

## Task 2: Multi-Base Pin and Rebuild Payload Refactor

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:2448`
- Modify: `src/engine/mv/iceberg_refresh.rs:7947`

- [ ] **Step 1: Add failing multi-base pin rewrite test**

Add this test next to `rewrite_full_refresh_select_with_pin_injects_version_as_of`.

```rust
#[test]
fn rewrite_full_refresh_select_with_pin_injects_all_base_versions() {
    let pin = crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin::from_entries_for_tests(
        &[
            ("ice.db.left_orders", 101, "left-uuid"),
            ("ice.db.right_orders", 202, "right-uuid"),
        ],
    );

    let rewritten = rewrite_full_refresh_select_with_pin_for_scope(
        "SELECT l.id, r.name FROM ice.db.left_orders l JOIN ice.db.right_orders r ON l.rid = r.id",
        &pin,
        Some("ice"),
        "db",
    )
    .expect("rewrite select with two pinned bases");

    assert!(rewritten.contains("VERSION AS OF 101"), "{rewritten}");
    assert!(rewritten.contains("VERSION AS OF 202"), "{rewritten}");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test --lib rewrite_full_refresh_select_with_pin_injects_all_base_versions -- --nocapture
```

Expected: FAIL with unresolved `rewrite_full_refresh_select_with_pin_for_scope`.

- [ ] **Step 3: Add scope-based rewrite helper**

Replace the current `rewrite_full_refresh_select_with_pin` function with this pair. The wrapper preserves existing single-base call sites.

```rust
fn rewrite_full_refresh_select_with_pin_for_scope(
    select_sql: &str,
    pin: &crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<String, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
        .map_err(|e| format!("iceberg MV full refresh pin SELECT normalize error: {e}"))?;
    let mut stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|e| format!("iceberg MV full refresh pin SELECT parse error: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = &mut stmt else {
        return Err("iceberg MV full refresh pin SELECT expects a SELECT query".to_string());
    };
    crate::connector::starrocks::table::refresh_pin::inject_pin_as_for_version_as_of(
        query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    Ok(stmt.to_string())
}

fn rewrite_full_refresh_select_with_pin(
    select_sql: &str,
    pin: &crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin,
    base_ref: &IcebergTableRef,
) -> Result<String, String> {
    rewrite_full_refresh_select_with_pin_for_scope(
        select_sql,
        pin,
        Some(&base_ref.catalog),
        &base_ref.namespace,
    )
}
```

- [ ] **Step 4: Add rebuild payload structs**

Add this block before `rebuild_iceberg_mv`.

```rust
struct RepartitionRebuildPayload {
    chunks: Vec<crate::exec::chunk::Chunk>,
    base_snapshots: BTreeMap<String, i64>,
    base_table_uuids: BTreeMap<String, String>,
}

impl RepartitionRebuildPayload {
    fn from_chunks(
        chunks: Vec<crate::exec::chunk::Chunk>,
        pin: &crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin,
    ) -> Self {
        Self {
            chunks,
            base_snapshots: pin.to_snapshot_map(),
            base_table_uuids: pin.to_table_uuid_map(),
        }
    }
}
```

If `BTreeMap` is not in scope in that region, add this import near the top of `src/engine/mv/iceberg_refresh.rs`:

```rust
use std::collections::{BTreeMap, HashSet};
```

- [ ] **Step 5: Extract common overwrite commit helper**

Add this helper above `rebuild_iceberg_mv`, using the body of the existing `rebuild_iceberg_mv` after chunk production. Keep the existing staging branch and publish/finalize order.

```rust
#[allow(clippy::too_many_arguments)]
fn commit_repartition_rebuild_payload(
    state: &Arc<StandaloneState>,
    target: &IcebergMvTarget,
    target_entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    iceberg_catalog: &Arc<dyn iceberg::Catalog>,
    expected_main_snapshot_id: Option<i64>,
    staging_branch: &str,
    refresh_id: i64,
    mv_definition: &StoredMvDefinition,
    payload: RepartitionRebuildPayload,
    partition_contract: &crate::meta::repository::mv_contract::MvPartitionContract,
) -> Result<StatementResult, String> {
    let total_rows: i64 = payload
        .chunks
        .iter()
        .map(|chunk| chunk.batch.num_rows() as i64)
        .sum();
    let ident = iceberg_mv_table_ident(target)?;
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let new_snapshot_id = match data_block_on(async {
        let data_files = if payload.chunks.iter().all(|chunk| chunk.batch.num_rows() == 0) {
            Vec::new()
        } else {
            write_chunks_as_iceberg_data_files(&target_table, &payload.chunks).await?
        };
        commit_overwrite_iceberg_mv_with_ref(
            &target_table,
            iceberg_catalog,
            target_entry,
            &ident,
            data_files,
            staging_branch,
            marker,
        )
        .await
    }) {
        Ok(Ok(snapshot_id)) => snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        total_rows,
        payload.base_table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
    finalize_iceberg_mv_refresh_with_partition_contract(
        state,
        refresh_id,
        total_rows,
        payload.base_snapshots,
        payload.base_table_uuids,
        published_snapshot_id,
        partition_contract,
        IcebergMvPartitionStateFinalize::Clear,
    )?;
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 6: Change `rebuild_iceberg_mv` to call the helper**

Inside `rebuild_iceberg_mv`, keep the visible SELECT execution, then replace the existing commit/publish/finalize body with:

```rust
let payload = RepartitionRebuildPayload {
    chunks,
    base_snapshots: base_snapshot_id
        .map(|snapshot_id| single_snapshot_map(base_ref, snapshot_id))
        .unwrap_or_default(),
    base_table_uuids: single_table_uuid_map(base_ref, current_table_uuid),
};
let partition_contract = partition_contract.ok_or_else(|| {
    "iceberg MV repartition rebuild requires a partition contract".to_string()
})?;
commit_repartition_rebuild_payload(
    state,
    target,
    target_entry,
    iceberg_catalog,
    expected_main_snapshot_id,
    staging_branch,
    refresh_id,
    mv_definition,
    payload,
    partition_contract,
)
```

- [ ] **Step 7: Run projection/filter regression unit**

Run:

```bash
cargo test --lib alter_iceberg_mv_repartition_overwrites_data_and_updates_contract -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Run new pin rewrite test**

Run:

```bash
cargo test --lib rewrite_full_refresh_select_with_pin_injects_all_base_versions -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "refactor: reuse pinned rebuild payload for mv repartition"
```

## Task 3: Persist Typed Repartition Operation Intent

**Files:**
- Modify: `src/meta/repository/iceberg_operation.rs:285`
- Modify: `tests/meta_repository.rs:379`
- Modify: `src/engine/mv/iceberg_refresh.rs:5935`

- [ ] **Step 1: Add failing repository test for commit request updates**

Add this test in `tests/meta_repository.rs` near the existing Iceberg operation tests.

```rust
#[test]
fn iceberg_operation_repository_records_commit_request()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    let repository = IcebergOperationRepository::default();
    let operation_id = {
        let mut write = provider.begin_write("create iceberg operation")?;
        let stored = repository.create_operation(
            write.as_mut(),
            CreateIcebergOperationRequest {
                operation_kind: IcebergOperationKind::Maintenance,
                operation_subkind: Some("MV_REPARTITION".to_string()),
                target: IcebergOperationTarget {
                    catalog: "ice".to_string(),
                    namespace: "analytics".to_string(),
                    table: "mv_sales".to_string(),
                    ref_name: Some("__nova_mv_repartition_test".to_string()),
                },
                attempt_id: "mv-repartition-test".to_string(),
                base_snapshot_id: Some(10),
                base_snapshot_map: BTreeMap::from([("ice.sales.orders".to_string(), 11)]),
                staged_artifacts: vec!["branch:__nova_mv_repartition_test".to_string()],
                created_at_ms: 1000,
            },
        )?;
        write.commit()?;
        stored.operation_id
    };

    {
        let mut write = provider.begin_write("record commit request")?;
        repository.record_commit_request(
            write.as_mut(),
            operation_id,
            "{\"kind\":\"MV_REPARTITION\",\"new_target_spec_id\":7}".to_string(),
            1200,
        )?;
        write.commit()?;
    }

    let read = provider.begin_read()?;
    let loaded = repository
        .load_operation(read.as_ref(), operation_id)?
        .expect("operation");
    assert_eq!(
        loaded.commit_request.as_deref(),
        Some("{\"kind\":\"MV_REPARTITION\",\"new_target_spec_id\":7}")
    );
    assert_eq!(loaded.updated_at_ms, 1200);
    assert_eq!(loaded.state, IcebergOperationState::Preparing);
    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test --test meta_repository iceberg_operation_repository_records_commit_request -- --nocapture
```

Expected: FAIL with missing `record_commit_request`.

- [ ] **Step 3: Implement repository method**

Add this method inside `impl IcebergOperationRepository` in `src/meta/repository/iceberg_operation.rs`.

```rust
pub fn record_commit_request(
    &self,
    txn: &mut dyn MetaWriteTxn,
    operation_id: i64,
    commit_request: String,
    now_ms: i64,
) -> RepositoryResult<()> {
    let mut versioned = load_versioned_operation(txn, operation_id)?.ok_or_else(|| {
        RepositoryError::not_found(format!("iceberg operation {operation_id} not found"))
    })?;
    if let Some(existing) = &versioned.value.commit_request {
        if existing != &commit_request {
            return Err(RepositoryError::conflict(format!(
                "conflicting Iceberg operation commit request for operation {operation_id}"
            )));
        }
        return Ok(());
    }
    versioned.value.commit_request = Some(commit_request);
    versioned.value.updated_at_ms = now_ms;
    put_operation(
        txn,
        &versioned.value,
        ExpectedRevision::Exact(versioned.record_revision),
    )
}
```

- [ ] **Step 4: Add typed repartition intent**

Add this struct block in `src/engine/mv/iceberg_refresh.rs` near `begin_staged_iceberg_mv_repartition_intent`.

```rust
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct MvRepartitionOperationIntent {
    kind: String,
    old_partition_contract: Option<crate::meta::repository::mv_contract::MvPartitionContract>,
    new_partition_contract: crate::meta::repository::mv_contract::MvPartitionContract,
    base_snapshot_map: BTreeMap<String, i64>,
    staging_branch: String,
    target_pre_repartition_snapshot_id: Option<i64>,
    target_new_default_spec_id: i32,
}

impl MvRepartitionOperationIntent {
    fn new(
        old_partition_contract: Option<crate::meta::repository::mv_contract::MvPartitionContract>,
        new_partition_contract: crate::meta::repository::mv_contract::MvPartitionContract,
        base_snapshot_map: BTreeMap<String, i64>,
        staging_branch: String,
        target_pre_repartition_snapshot_id: Option<i64>,
        target_new_default_spec_id: i32,
    ) -> Self {
        Self {
            kind: "MV_REPARTITION".to_string(),
            old_partition_contract,
            new_partition_contract,
            base_snapshot_map,
            staging_branch,
            target_pre_repartition_snapshot_id,
            target_new_default_spec_id,
        }
    }
}
```

- [ ] **Step 5: Add operation id loader and intent recorder**

Add this helper near `load_iceberg_mv_refresh_operation_id`.

```rust
fn record_iceberg_mv_repartition_operation_intent(
    state: &Arc<StandaloneState>,
    refresh_id: i64,
    intent: &MvRepartitionOperationIntent,
) -> Result<(), String> {
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "metadata provider required for iceberg mv repartition".to_string())?;
    let mut txn = provider
        .begin_write("record iceberg materialized view repartition operation intent")
        .map_err(|e| format!("open iceberg mv repartition intent transaction failed: {e}"))?;
    let Some(operation_id) = load_iceberg_mv_refresh_operation_id(state, txn.as_ref(), refresh_id)?
    else {
        return Err(format!(
            "mv repartition refresh {refresh_id} has no iceberg operation id"
        ));
    };
    let encoded = serde_json::to_string(intent)
        .map_err(|e| format!("encode iceberg mv repartition operation intent failed: {e}"))?;
    state
        .iceberg_operation_repo
        .record_commit_request(txn.as_mut(), operation_id, encoded, now_ms())
        .map_err(|e| format!("record iceberg mv repartition operation intent failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit iceberg mv repartition operation intent failed: {e}"))?;
    Ok(())
}
```

- [ ] **Step 6: Record intent after new partition contract is known**

In `repartition_iceberg_mv`, after `new_partition_contract` is built and before building the full refresh payload, add:

```rust
record_iceberg_mv_repartition_operation_intent(
    state,
    refresh_id,
    &MvRepartitionOperationIntent::new(
        previous_partition_contract.cloned(),
        new_partition_contract.clone(),
        snapshots.clone(),
        staging_branch.clone(),
        expected_main_snapshot_id,
        new_default_spec_id,
    ),
)?;
```

If the function returns an error after the default spec has already changed, route that error through `abort_and_restore_iceberg_mv_repartition_default_spec` using the same `new_default_spec_id` and `old_default_spec_id`.

- [ ] **Step 7: Assert repartition operation contains intent**

Extend `alter_iceberg_mv_repartition_overwrites_data_and_updates_contract` after it loads `operation`:

```rust
let intent_json = operation
    .commit_request
    .as_deref()
    .expect("repartition operation intent");
let intent: MvRepartitionOperationIntent =
    serde_json::from_str(intent_json).expect("decode repartition intent");
assert_eq!(intent.kind, "MV_REPARTITION");
assert_eq!(intent.base_snapshot_map, operation.base_snapshot_map);
assert_eq!(intent.staging_branch, operation.target.ref_name.clone().expect("ref"));
assert_eq!(
    intent.new_partition_contract.target_spec_id,
    mv.partition_spec.as_ref().expect("partition contract").target_spec_id
);
```

- [ ] **Step 8: Run tests**

Run:

```bash
cargo test --test meta_repository iceberg_operation_repository_records_commit_request -- --nocapture
cargo test --lib alter_iceberg_mv_repartition_overwrites_data_and_updates_contract -- --nocapture
```

Expected: both PASS.

- [ ] **Step 9: Commit**

```bash
git add src/meta/repository/iceberg_operation.rs tests/meta_repository.rs src/engine/mv/iceberg_refresh.rs
git commit -m "feat: persist iceberg mv repartition operation intent"
```

## Task 4: Aggregate Repartition Full Rebuild

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:2599`
- Modify: `sql-tests/iceberg-ivm/sql/iceberg_ivm_projection_repartition.sql:111`
- Modify: `sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result`

- [ ] **Step 1: Add failing Rust e2e test for aggregate repartition**

Add this test near `alter_iceberg_mv_repartition_overwrites_data_and_updates_contract`.

```rust
#[test]
fn alter_iceberg_aggregate_mv_repartition_rebuilds_state_and_contract() {
    let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
    create_aggregate_fact_table(&env.state, "ice", "sales", "orders");
    insert_into_aggregate_fact_table(
        &env.state,
        "ice",
        "sales",
        "orders",
        &[(1, "east", 10), (2, "west", 20), (3, "east", 30)],
    );
    let stmt = parse_create_mv(
        "CREATE MATERIALIZED VIEW mv_sales_agg \
         PARTITION BY region \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES ('storage_engine'='iceberg') AS \
         SELECT region, COUNT(*) AS c, SUM(amount) AS s \
         FROM ice.sales.orders GROUP BY region",
    );
    create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
        .expect("create aggregate mv");
    refresh_iceberg_mv(
        &env.state,
        Some("ice"),
        &env.current_db,
        &parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_sales_agg"),
    )
    .expect("first aggregate refresh");
    assert_aggregate_region_rows(
        &env.state,
        "ice",
        &env.current_db,
        "mv_sales_agg",
        &[("east", 2, 40), ("west", 1, 20)],
    );

    let alter =
        parse_alter_mv("ALTER MATERIALIZED VIEW mv_sales_agg REPARTITION BY (truncate(region, 2))");
    repartition_iceberg_mv(&env.state, Some("ice"), &env.current_db, &alter)
        .expect("aggregate repartition");
    assert_aggregate_region_rows(
        &env.state,
        "ice",
        &env.current_db,
        "mv_sales_agg",
        &[("east", 2, 40), ("west", 1, 20)],
    );

    let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_sales_agg")
        .expect("mv definition");
    assert_eq!(
        mv.partition_spec
            .as_ref()
            .expect("partition contract")
            .fields[0]
            .partition_field_name,
        "region_truncate_2"
    );
    let refresh = load_all_mv_refreshes(&env.state)
        .pop()
        .expect("repartition refresh");
    let operation = load_test_operation_for_refresh(&env.state, refresh.refresh_id);
    assert_eq!(operation.operation_subkind.as_deref(), Some("MV_REPARTITION"));
    assert_eq!(
        operation.state,
        crate::meta::repository::iceberg_operation::IcebergOperationState::Finalized
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test --lib alter_iceberg_aggregate_mv_repartition_rebuilds_state_and_contract -- --nocapture
```

Expected: FAIL before implementation. The failure should be from the repartition support/build path, not from MV creation.

- [ ] **Step 3: Add schema-contract validation for all repartition bases**

Add this helper near `ensure_schema_contract_compatible_for_refresh`.

```rust
fn validate_repartition_schema_contract(
    state: &Arc<StandaloneState>,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    base_refs: &[IcebergTableRef],
    target_table: &iceberg::table::Table,
) -> Result<(), String> {
    if schema_contract.join.is_some() {
        if base_refs.len() != 2 {
            return Err(format!(
                "iceberg MV repartition join contract requires exactly two bases, got {}",
                base_refs.len()
            ));
        }
        let left_loaded = load_current_iceberg_base_table(state, &base_refs[0])?;
        let right_loaded = load_current_iceberg_base_table(state, &base_refs[1])?;
        let join_bases = [
            (&base_refs[0], &left_loaded.table),
            (&base_refs[1], &right_loaded.table),
        ];
        let _ = validate_join_schema_contract(schema_contract, &join_bases, target_table)?;
        return Ok(());
    }
    if !schema_contract.bases.is_empty() {
        for base_ref in base_refs {
            let loaded = load_current_iceberg_base_table(state, base_ref)?;
            if schema_contract.aggregate.is_some() {
                validate_aggregate_schema_contract_for_base(
                    schema_contract,
                    base_ref,
                    &loaded.table,
                    target_table,
                )?;
            }
        }
        return Ok(());
    }
    let [base_ref] = base_refs else {
        return Err(format!(
            "iceberg MV repartition single-base contract requires exactly one base, got {}",
            base_refs.len()
        ));
    };
    let loaded = load_current_iceberg_base_table(state, base_ref)?;
    ensure_schema_contract_compatible_for_refresh(schema_contract, &loaded.table, target_table)
}
```

- [ ] **Step 4: Add aggregate rebuild payload builder**

Add this helper near `prepare_aggregate_first_refresh_chunks_for_select_sql`.

```rust
fn build_aggregate_repartition_payload(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    select_sql: &str,
    pin: &crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin,
) -> Result<RepartitionRebuildPayload, String> {
    let query = parse_mv_select_query(select_sql)?;
    let calls =
        crate::connector::starrocks::table::aggregate_sql_calls::extract_aggregate_sql_calls(
            &query,
        )?;
    let chunks = prepare_aggregate_first_refresh_chunks_for_select_sql(
        state,
        current_catalog,
        current_database,
        select_sql,
        &calls,
        pin,
    )?;
    Ok(RepartitionRebuildPayload::from_chunks(chunks, pin))
}
```

- [ ] **Step 5: Use aggregate payload in `repartition_iceberg_mv`**

In `repartition_iceberg_mv`, replace the single-base destructuring and validation:

```rust
let [base_ref] = base_refs.as_slice() else {
    return Err(format!(
        "ALTER MATERIALIZED VIEW ... REPARTITION currently supports exactly one base table, got {}",
        base_refs.len()
    ));
};
let loaded_base = load_current_iceberg_base_table(state, base_ref)?;
ensure_schema_contract_compatible_for_refresh(
    schema_contract,
    &loaded_base.table,
    &target_loaded.table,
)?;
```

with:

```rust
validate_repartition_schema_contract(state, schema_contract, &base_refs, &target_loaded.table)?;
```

Then capture the pin over all bases and validate UUIDs:

```rust
let pin = crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin::capture(
    state, &base_refs,
)?;
validate_refresh_pin_table_uuids(&mv_definition, &pin, &base_refs)?;
let snapshots = pin.to_snapshot_map();
```

Replace the final `rebuild_iceberg_mv(...)` call with:

```rust
let payload = match support {
    RepartitionSupport::ProjectionFilterSingleBase => {
        let [base_ref] = base_refs.as_slice() else {
            return Err(format!(
                "projection/filter single-base repartition requires exactly one base, got {}",
                base_refs.len()
            ));
        };
        let pinned_full_select_sql = rewrite_full_refresh_select_with_pin(
            &mv_definition.select_sql,
            &pin,
            base_ref,
        )?;
        let physical_sql = iceberg_mv_physical_select_sql(&pinned_full_select_sql)?;
        let chunks = run_mv_full_select_chunks(state, current_database, &physical_sql)?;
        RepartitionRebuildPayload::from_chunks(chunks, &pin)
    }
    RepartitionSupport::AggregateSingleBase
    | RepartitionSupport::JoinAggregate
    | RepartitionSupport::FanInAggregate => build_aggregate_repartition_payload(
        state,
        current_catalog,
        current_database,
        &mv_definition.select_sql,
        &pin,
    )?,
    RepartitionSupport::JoinProjectionFilter
    | RepartitionSupport::UnionProjectionFilter => {
        return Err(format!(
            "UnsupportedRepartitionShape: {} repartition payload builder is not enabled in this task",
            support.label()
        ));
    }
};

commit_repartition_rebuild_payload(
    state,
    &target,
    &target_entry,
    &iceberg_catalog,
    expected_main_snapshot_id,
    &staging_branch,
    refresh_id,
    &mv_definition,
    payload,
    &new_partition_contract,
)
```

- [ ] **Step 6: Route post-spec errors through restore**

Wrap the payload build and commit block in a `match`. On `Err(err)`, call:

```rust
return Err(abort_and_restore_iceberg_mv_repartition_default_spec(
    state,
    refresh_id,
    &target_entry,
    &target,
    new_default_spec_id,
    old_default_spec_id,
    err,
));
```

This preserves the old partition spec if aggregate payload generation fails after `replace_default_partition_spec`.

- [ ] **Step 7: Run aggregate Rust test**

Run:

```bash
cargo test --lib alter_iceberg_aggregate_mv_repartition_rebuilds_state_and_contract -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: support aggregate iceberg mv repartition"
```

## Task 5: Join and Multi-Base Repartition Builders

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:9109`
- Modify: `src/engine/mv/iceberg_refresh.rs:2599`

- [ ] **Step 1: Add failing join repartition Rust test**

Add this test near the aggregate repartition test.

```rust
fn join_repartition_rows(
    state: &Arc<StandaloneState>,
    current_catalog: &str,
    current_database: &str,
    mv_name: &str,
) -> Vec<(i64, String, i64)> {
    let sql = format!("SELECT id, region, amount FROM {mv_name} ORDER BY id");
    let session = crate::engine::StandaloneSession {
        inner: Arc::clone(state),
    };
    let result = match session
        .execute_in_context(&sql, Some(current_catalog), current_database, None)
        .expect("query join repartition mv")
    {
        StatementResult::Query(result) => result,
        StatementResult::Ok => panic!("expected query result for {sql}"),
    };
    let mut rows = Vec::new();
    for chunk in &result.chunks {
        let id = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let region = chunk
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("region column");
        let amount = chunk
            .batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("amount column");
        for row in 0..chunk.batch.num_rows() {
            rows.push((id.value(row), region.value(row).to_string(), amount.value(row)));
        }
    }
    rows
}

#[test]
fn alter_iceberg_join_mv_repartition_rebuilds_with_join_apply_key() {
    let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
    execute_iceberg_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "CREATE DATABASE IF NOT EXISTS ice.sales",
    );
    execute_iceberg_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "CREATE TABLE ice.sales.orders (id BIGINT NOT NULL, customer_id BIGINT, amount BIGINT) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    );
    execute_iceberg_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "CREATE TABLE ice.sales.customers (customer_id BIGINT NOT NULL, region STRING) \
         TBLPROPERTIES (\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
    );
    execute_iceberg_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "INSERT INTO ice.sales.orders VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300)",
    );
    execute_iceberg_sql(
        &env.state,
        Some("ice"),
        &env.current_db,
        "INSERT INTO ice.sales.customers VALUES (10, 'east'), (20, 'west')",
    );
    let stmt = parse_create_mv(
        "CREATE MATERIALIZED VIEW mv_join_orders \
         PARTITION BY region \
         DISTRIBUTED BY HASH(region) BUCKETS 1 \
         PROPERTIES ('storage_engine'='iceberg') AS \
         SELECT o.id, c.region, o.amount \
         FROM ice.sales.orders o JOIN ice.sales.customers c ON o.customer_id = c.customer_id",
    );
    create_iceberg_mv(&env.state, Some("ice"), &env.current_db, &stmt)
        .expect("create join mv");
    refresh_iceberg_mv(
        &env.state,
        Some("ice"),
        &env.current_db,
        &parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_join_orders"),
    )
    .expect("first join refresh");

    let alter =
        parse_alter_mv("ALTER MATERIALIZED VIEW mv_join_orders REPARTITION BY (truncate(region, 2))");
    repartition_iceberg_mv(&env.state, Some("ice"), &env.current_db, &alter)
        .expect("join repartition");

    assert_eq!(
        join_repartition_rows(&env.state, "ice", &env.current_db, "mv_join_orders"),
        vec![
            (1, "east".to_string(), 100),
            (2, "west".to_string(), 200),
            (3, "east".to_string(), 300),
        ]
    );
    let mv = find_iceberg_mv_definition(&env.state, "ice", "analytics", "mv_join_orders")
        .expect("mv definition");
    assert_eq!(
        mv.partition_spec
            .as_ref()
            .expect("partition contract")
            .fields[0]
            .partition_field_name,
        "region_truncate_2"
    );
    let entry = {
        let catalogs = env.state.iceberg_catalogs.read().expect("iceberg catalogs");
        catalogs.get("ice").expect("catalog")
    };
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, "analytics", "mv_join_orders")
            .expect("load join target");
    assert!(
        loaded
            .table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .any(|field| field.name == ICEBERG_MV_JOIN_APPLY_KEY_COLUMN),
        "join repartition target must retain the hidden join apply key column"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test --lib alter_iceberg_join_mv_repartition_rebuilds_with_join_apply_key -- --nocapture
```

Expected: FAIL with `UnsupportedRepartitionShape: join projection/filter repartition payload builder is not enabled in this task`.

- [ ] **Step 3: Add join overwrite builder**

Add this helper near `first_refresh_iceberg_join_mv`. It mirrors the existing first-refresh join logic but uses `CommitOpKind::Overwrite` and returns the common repartition payload metadata through finalize.

```rust
#[allow(clippy::too_many_arguments)]
fn commit_join_repartition_overwrite(
    state: &Arc<StandaloneState>,
    ctx: &IcebergMvRefreshContext,
    staging_branch: &str,
    refresh_id: i64,
    aliases: &crate::connector::starrocks::table::aggregate_sql_calls::JoinAliases,
    left_ref: &IcebergTableRef,
    right_ref: &IcebergTableRef,
    partition_contract: &crate::meta::repository::mv_contract::MvPartitionContract,
) -> Result<StatementResult, String> {
    let target = &ctx.rewrite.target;
    let target_entry = &*ctx.target_entry;
    let iceberg_catalog = &ctx.iceberg_catalog;
    let expected_main_snapshot_id = ctx.rewrite.target_snapshot_id;
    let current_database = ctx.rewrite.current_database.as_str();
    let mv_definition = &*ctx.rewrite.mv_definition;
    let pin = &*ctx.rewrite.pin;
    if let Err(err) = ensure_iceberg_mv_staging_branch(
        iceberg_catalog,
        target,
        staging_branch,
        expected_main_snapshot_id,
    ) {
        abort_iceberg_mv_refresh(state, refresh_id)?;
        return Err(err);
    }
    let target_table = match reload_iceberg_mv_target_table(target_entry, target) {
        Ok(table) => table,
        Err(err) => {
            return Err(handle_iceberg_mv_definite_pre_publish_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let left_snapshot = pin
        .get(left_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", left_ref.fqn()))?;
    let right_snapshot = pin
        .get(right_ref)
        .ok_or_else(|| format!("missing refresh pin for {}", right_ref.fqn()))?;
    let mut query = parse_mv_select_query(&mv_definition.select_sql)?;
    rewrite_join_full_refresh_query(
        &mut query,
        left_ref,
        left_snapshot,
        right_ref,
        right_snapshot,
        &aliases.left_alias,
        &aliases.right_alias,
    )?;
    let branch_catalog =
        build_join_snapshot_catalog(state, &[(left_ref, left_snapshot), (right_ref, right_snapshot)])?;
    let coalescer = crate::engine::mv::iceberg_join_coalesce::JoinDeltaCoalescer::new(
        pin.uuid(left_ref)
            .ok_or_else(|| format!("missing uuid for {}", left_ref.fqn()))?
            .to_string(),
        pin.uuid(right_ref)
            .ok_or_else(|| format!("missing uuid for {}", right_ref.fqn()))?
            .to_string(),
        1_000_000,
    );
    let sink = crate::engine::mv::iceberg_join_coalesce::IcebergJoinCoalesceSinkFactory::new(
        Arc::clone(&coalescer),
    );
    {
        let connectors_snapshot = state
            .connectors
            .read()
            .expect("standalone connector registry read lock")
            .clone();
        let catalogs_guard = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        crate::engine::execute_query_with_options(
            &query,
            &branch_catalog,
            &connectors_snapshot,
            current_database,
            state.exchange_port,
            None,
            Some(Box::new(sink)),
            Some(&*catalogs_guard),
            None,
        )
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            )
        })?;
    }
    let marker = load_iceberg_mv_refresh_marker(state, refresh_id, mv_definition.mv_id)?
        .to_summary_properties();
    let ident = iceberg_mv_table_ident(target)?;
    let collector =
        new_iceberg_mv_commit_collector(&target_table, &ident, staging_branch, CommitOpKind::Overwrite);
    let flush_outcome = coalescer
        .flush_to_iceberg_commit_collector(&target_table, Arc::clone(&collector), None)
        .map_err(|err| {
            handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            )
        })?;
    let new_snapshot_id = match data_block_on(commit_iceberg_mv_with_populated_collector(
        &target_table,
        iceberg_catalog,
        target_entry,
        &ident,
        Arc::clone(&collector),
        staging_branch,
        marker,
    )) {
        Ok(Ok(outcome)) => outcome.new_snapshot_id,
        Ok(Err(err)) | Err(err) => {
            return Err(handle_iceberg_mv_commit_error(
                state,
                target,
                target_entry,
                staging_branch,
                refresh_id,
                err,
            ));
        }
    };
    let snapshots = pin.to_snapshot_map();
    let table_uuids = pin.to_table_uuid_map();
    record_iceberg_mv_staging_commit(
        state,
        refresh_id,
        new_snapshot_id,
        flush_outcome.added_rows,
        table_uuids.clone(),
    )?;
    let published_snapshot_id = publish_iceberg_mv_refresh(
        state,
        target,
        target_entry,
        staging_branch,
        expected_main_snapshot_id,
        new_snapshot_id,
        refresh_id,
        mv_definition.mv_id,
    )?;
    record_iceberg_mv_publish_commit(state, refresh_id, published_snapshot_id)?;
    drop_iceberg_mv_staging_branch(state, target, target_entry, staging_branch)?;
    finalize_iceberg_mv_refresh_with_partition_contract(
        state,
        refresh_id,
        flush_outcome.added_rows,
        snapshots,
        table_uuids,
        published_snapshot_id,
        partition_contract,
        IcebergMvPartitionStateFinalize::Clear,
    )?;
    Ok(StatementResult::Ok)
}
```

- [ ] **Step 4: Add UNION projection/filter payload builder**

Add this helper near `build_aggregate_repartition_payload`.

```rust
fn build_union_projection_repartition_payload(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    mv_definition: &StoredMvDefinition,
    schema_contract: &crate::meta::repository::mv_contract::MvSchemaContract,
    pin: &crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin,
) -> Result<RepartitionRebuildPayload, String> {
    let branch_count = schema_contract
        .branch
        .as_ref()
        .map(|branch| branch.branch_count as usize)
        .unwrap_or_else(|| {
            parse_mv_select_query(&mv_definition.select_sql)
                .map(|query| union_branch_count(&query) as usize)
                .unwrap_or(0)
        });
    if branch_count == 0 {
        return Err("UNION ALL projection/filter repartition requires at least one branch".to_string());
    }
    let rewritten = rewrite_union_projection_full_refresh_select_with_pin(
        &mv_definition.select_sql,
        pin,
        branch_count,
        current_catalog,
        current_database,
    )?;
    let chunks = run_mv_full_select_chunks(state, current_database, &rewritten)?;
    Ok(RepartitionRebuildPayload::from_chunks(chunks, pin))
}
```

- [ ] **Step 5: Route all supported variants**

In the `match support` block from Task 4, replace the unsupported arm with:

```rust
    RepartitionSupport::UnionProjectionFilter => build_union_projection_repartition_payload(
        state,
        current_catalog,
        current_database,
        &mv_definition,
        schema_contract,
        &pin,
    )?,
    RepartitionSupport::JoinProjectionFilter => {
        let join_aliases =
            crate::connector::starrocks::table::aggregate_sql_calls::extract_join_aliases(
                &canonical_select_query,
            )?;
        validate_join_aliases_base_refs(&join_aliases, &base_refs)?;
        let (left_ref, right_ref) =
            join_base_refs_for_aliases(&join_aliases, &base_refs)?;
        let ctx = {
            let iceberg_catalog_guard = state
                .iceberg_catalogs
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            IcebergMvRefreshContext::new_with_pruning_limits(
                target.clone(),
                mv_definition.mv_id,
                current_catalog,
                current_database,
                Arc::new(mv_definition.clone()),
                Arc::new(canonical_select_query.clone()),
                Arc::from(base_refs.clone()),
                Arc::new(pin.clone()),
                &iceberg_catalog_guard,
                Arc::new(target_entry.clone()),
                iceberg_catalog.clone(),
                target_loaded.table.clone(),
                state.mv_refresh_pruning_limits,
            )?
        };
        return commit_join_repartition_overwrite(
            state,
            &ctx,
            &staging_branch,
            refresh_id,
            &join_aliases,
            left_ref,
            right_ref,
            &new_partition_contract,
        );
    }
```

Keep the common `commit_repartition_rebuild_payload(...)` after the match for non-join payloads.

- [ ] **Step 6: Run join test**

Run:

```bash
cargo test --lib alter_iceberg_join_mv_repartition_rebuilds_with_join_apply_key -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Run existing join refresh tests**

Run:

```bash
cargo test --lib iceberg_join -- --nocapture
```

Expected: PASS. If the filter matches zero tests, run:

```bash
cargo test --lib join_mv -- --nocapture
```

Expected: PASS or a non-zero set of existing join MV tests passing.

- [ ] **Step 8: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "feat: support join and multi-base iceberg mv repartition"
```

## Task 6: Repartition Recovery Finalize Replay

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:6273`
- Modify: `src/engine/mv/iceberg_refresh.rs:6703`

- [ ] **Step 1: Add failing recovery test for finalize replay**

Add this test near the existing recovery tests.

```rust
#[test]
fn recover_repartition_finalize_failure_replays_partition_contract() {
    let env = open_test_state_with_hadoop_iceberg_catalog("ice", "analytics");
    create_base_table_with_rows(&env.state, "ice", "sales", "orders", &[(1, "east")]);
    create_mv_only(&env.state, Some("ice"), &env.current_db, "mv_orders");
    refresh_iceberg_mv(
        &env.state,
        Some("ice"),
        &env.current_db,
        &parse_refresh_mv("REFRESH MATERIALIZED VIEW mv_orders"),
    )
    .expect("first refresh");

    let alter = parse_alter_mv("ALTER MATERIALIZED VIEW mv_orders REPARTITION BY (truncate(name, 2))");
    repartition_iceberg_mv(&env.state, Some("ice"), &env.current_db, &alter)
        .expect("repartition");

    let refresh = load_all_mv_refreshes(&env.state)
        .pop()
        .expect("repartition refresh");
    let operation = load_test_operation_for_refresh(&env.state, refresh.refresh_id);
    record_iceberg_mv_operation_finalize_failure(
        &env.state,
        operation.operation_id,
        "synthetic finalize retry".to_string(),
    )
    .expect("mark finalize failed");

    recover_iceberg_mv_refreshes(&env.state).expect("recover repartition");

    let provider = env.state.metadata_provider.as_ref().expect("provider");
    let read = provider.begin_read().expect("read");
    let mv = env
        .state
        .mv_repo
        .find_by_target(read.as_ref(), "ice", "analytics", "mv_orders")
        .expect("find mv")
        .expect("mv definition");
    assert!(mv.partition_spec.is_some());
    assert_eq!(
        load_test_operation_for_refresh(&env.state, refresh.refresh_id).state,
        crate::meta::repository::iceberg_operation::IcebergOperationState::Finalized
    );
}
```

- [ ] **Step 2: Run test to verify it fails or exposes no-op recovery**

Run:

```bash
cargo test --lib recover_repartition_finalize_failure_replays_partition_contract -- --nocapture
```

Expected: FAIL if recovery does not replay partition contract from intent, or PASS if the previous tasks already finalized before failure injection. If it passes because the operation state remains `FinalizeFailedKnownCommitted`, continue with Step 3.

- [ ] **Step 3: Add intent decoder**

Add this helper near `record_iceberg_mv_repartition_operation_intent`.

```rust
fn decode_mv_repartition_operation_intent(
    operation: &crate::meta::repository::iceberg_operation::StoredIcebergOperation,
) -> Result<Option<MvRepartitionOperationIntent>, String> {
    if operation.operation_subkind.as_deref() != Some("MV_REPARTITION") {
        return Ok(None);
    }
    let Some(commit_request) = operation.commit_request.as_deref() else {
        return Err(format!(
            "iceberg MV repartition operation {} is missing repartition intent",
            operation.operation_id
        ));
    };
    serde_json::from_str(commit_request)
        .map(Some)
        .map_err(|e| format!("decode iceberg MV repartition operation intent failed: {e}"))
}
```

- [ ] **Step 4: Replay partition contract during recovered finalize**

Modify `finalize_recovered_iceberg_mv_refresh`. Load the operation, decode the intent, and call the partition-contract finalize path when the operation is `MV_REPARTITION`.

```rust
fn finalize_recovered_iceberg_mv_refresh(
    state: &Arc<StandaloneState>,
    refresh: &StoredMvRefresh,
) -> Result<(), String> {
    let target_snapshot_id = recovered_published_snapshot_id(refresh)
        .or(refresh.staging_snapshot_id)
        .ok_or_else(|| {
            format!(
                "mv refresh {} missing recovered target snapshot",
                refresh.refresh_id
            )
        })?;
    let rows = refresh.rows.ok_or_else(|| {
        format!(
            "mv refresh {} missing recovered row count",
            refresh.refresh_id
        )
    })?;
    let repartition_intent = if let Some(operation_id) = refresh.operation_id {
        let provider = state
            .metadata_provider
            .as_ref()
            .ok_or_else(|| "metadata provider required for iceberg mv recovery".to_string())?;
        let read = provider
            .begin_read()
            .map_err(|e| format!("open iceberg mv recovery operation read failed: {e}"))?;
        let operation = state
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .map_err(|e| format!("load iceberg mv recovery operation failed: {e}"))?
            .ok_or_else(|| format!("iceberg operation {operation_id} not found"))?;
        decode_mv_repartition_operation_intent(&operation)?
    } else {
        None
    };
    if let Some(intent) = repartition_intent {
        return finalize_iceberg_mv_refresh_with_partition_contract(
            state,
            refresh.refresh_id,
            rows,
            refresh.target_snapshots.clone(),
            refresh.base_table_uuids.clone(),
            target_snapshot_id,
            &intent.new_partition_contract,
            IcebergMvPartitionStateFinalize::Clear,
        );
    }
    finalize_iceberg_mv_refresh(
        state,
        refresh.refresh_id,
        rows,
        refresh.target_snapshots.clone(),
        refresh.base_table_uuids.clone(),
        target_snapshot_id,
    )
}
```

- [ ] **Step 5: Ensure pre-commit recovery restores old default spec**

Add this branch in `reconcile_iceberg_mv_refresh` for `MV_REPARTITION` `IntentCreated` and `StagingCommitted` abort cases after the code determines the commit did not publish. Decode the old contract from intent and restore `old_partition_contract.target_spec_id` when the table default spec still equals `target_new_default_spec_id`.

```rust
fn restore_repartition_default_spec_from_intent(
    target: &IcebergMvTarget,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    table: &iceberg::table::Table,
    intent: &MvRepartitionOperationIntent,
) -> Result<(), String> {
    let Some(old_contract) = intent.old_partition_contract.as_ref() else {
        return Ok(());
    };
    let current_default = table.metadata().default_partition_spec_id();
    if current_default != intent.target_new_default_spec_id {
        return Ok(());
    }
    crate::connector::iceberg::catalog::registry::set_default_partition_spec_id(
        entry,
        &target.namespace,
        &target.table,
        intent.target_new_default_spec_id,
        old_contract.target_spec_id,
    )?;
    Ok(())
}
```

Call it only when recovery is aborting a repartition before publish; do not call it after main has advanced to the staging snapshot.

- [ ] **Step 6: Run recovery tests**

Run:

```bash
cargo test --lib recover_repartition_finalize_failure_replays_partition_contract -- --nocapture
cargo test --lib recover_staging_committed_refresh_ -- --nocapture
cargo test --lib recover_publish_committed_refresh_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/engine/mv/iceberg_refresh.rs
git commit -m "fix: replay iceberg mv repartition recovery metadata"
```

## Task 7: SQL Golden Coverage

**Files:**
- Modify: `sql-tests/iceberg-ivm/sql/iceberg_ivm_projection_repartition.sql`
- Modify: `sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result`

- [ ] **Step 1: Update SQL case comments and aggregate section**

In `sql-tests/iceberg-ivm/sql/iceberg_ivm_projection_repartition.sql`, replace the opening comment block with:

```sql
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,projection_filter,aggregate,join,partitioned,repartition
-- Test Point: Iceberg-backed MV repartition supports projection/filter,
-- aggregate, and join full rebuild shapes while preserving post-repartition refresh.
-- Method: Repartition a projection/filter MV, an aggregate MV, and a join MV
-- from one target partition spec to another, then verify visible results match
-- a full recompute. Keep one unsupported shape assertion with a concrete
-- UnsupportedRepartitionShape error.
-- Scope: Iceberg target MV, repartition, operation lifecycle, full rebuild,
-- post-repartition incremental refresh for projection/filter.
```

Replace the current aggregate rejection section:

```sql
-- query 12
-- @expect_error=currently supports single-base projection/filter Iceberg MVs only
ALTER MATERIALIZED VIEW agg_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));
```

with:

```sql
-- query 12
-- @skip_result_check=true
ALTER MATERIALIZED VIEW agg_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 13
SELECT region, c, s
FROM agg_repart_mv_${uuid0}
ORDER BY region;

-- query 14
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region
ORDER BY region;
```

- [ ] **Step 2: Add join repartition SQL case**

Append before cleanup:

```sql
-- query 15
-- @skip_result_check=true
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.customers (
  customer_id BIGINT NOT NULL,
  region STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.customers VALUES
  (10, 'east'),
  (20, 'west'),
  (30, 'north');
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders (
  id BIGINT NOT NULL,
  customer_id BIGINT,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders VALUES
  (101, 10, 100),
  (102, 20, 200),
  (103, 10, 300);
CREATE MATERIALIZED VIEW join_repart_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT o.id, c.region, o.amount
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders o
JOIN ice_ivm_repart_${uuid0}.ns_${uuid0}.customers c
  ON o.customer_id = c.customer_id;
REFRESH MATERIALIZED VIEW join_repart_mv_${uuid0};
ALTER MATERIALIZED VIEW join_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));

-- query 16
SELECT id, region, amount
FROM join_repart_mv_${uuid0}
ORDER BY id;

-- query 17
SELECT o.id, c.region, o.amount
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders o
JOIN ice_ivm_repart_${uuid0}.ns_${uuid0}.customers c
  ON o.customer_id = c.customer_id
ORDER BY o.id;
```

- [ ] **Step 3: Add explicit unsupported shape assertion**

Append before cleanup:

```sql
-- query 18
-- @expect_error=UnsupportedRepartitionShape
CREATE TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT,
  category STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra VALUES
  (201, 'east', 11, 'books'),
  (202, 'west', 22, 'games');
CREATE MATERIALIZED VIEW unsupported_repart_mv_${uuid0}
PARTITION BY region
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders
GROUP BY region
UNION ALL
SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra
GROUP BY region;
REFRESH MATERIALIZED VIEW unsupported_repart_mv_${uuid0};
ALTER MATERIALIZED VIEW unsupported_repart_mv_${uuid0} REPARTITION BY (truncate(region, 2));
```

- [ ] **Step 4: Update cleanup**

Replace the cleanup query with:

```sql
-- query 19
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS unsupported_repart_mv_${uuid0};
DROP MATERIALIZED VIEW join_repart_mv_${uuid0};
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.join_orders;
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.customers;
DROP MATERIALIZED VIEW agg_repart_mv_${uuid0};
DROP MATERIALIZED VIEW pf_repart_mv_${uuid0};
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders_extra;
DROP TABLE ice_ivm_repart_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_repart_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_repart_${uuid0};
```

- [ ] **Step 5: Run SQL test in verify mode to see expected golden diff**

Start the shared Iceberg fixture and standalone server from this worktree:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
NO_PROXY=127.0.0.1,localhost cargo run --profile dev-opt -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

In a second shell:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --profile dev-opt --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_projection_repartition \
  --mode verify
```

Expected: FAIL only because `sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result` is stale.

- [ ] **Step 6: Record golden**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --profile dev-opt --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_projection_repartition \
  --mode record \
  --record-from target \
  --update-expected
```

Expected: PASS and `sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result` updated.

- [ ] **Step 7: Verify recorded golden**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --profile dev-opt --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_projection_repartition \
  --mode verify
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add sql-tests/iceberg-ivm/sql/iceberg_ivm_projection_repartition.sql sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result
git commit -m "test: cover iceberg mv repartition non-pf shapes"
```

## Task 8: Final Validation

**Files:**
- No source edits.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: exits 0.

- [ ] **Step 2: Compile library**

Run:

```bash
cargo check --profile dev-opt --lib
```

Expected: exits 0.

- [ ] **Step 3: Run targeted Rust tests**

Run:

```bash
cargo test --lib repartition_support_ -- --nocapture
cargo test --lib rewrite_full_refresh_select_with_pin_injects_all_base_versions -- --nocapture
cargo test --lib alter_iceberg_mv_repartition_overwrites_data_and_updates_contract -- --nocapture
cargo test --lib alter_iceberg_aggregate_mv_repartition_rebuilds_state_and_contract -- --nocapture
cargo test --lib alter_iceberg_join_mv_repartition_rebuilds_with_join_apply_key -- --nocapture
cargo test --lib recover_repartition_finalize_failure_replays_partition_contract -- --nocapture
cargo test --test meta_repository iceberg_operation_repository_records_commit_request -- --nocapture
```

Expected: every command exits 0.

- [ ] **Step 4: Run SQL repartition case**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --profile dev-opt --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_ivm_projection_repartition \
  --mode verify
```

Expected: PASS.

- [ ] **Step 5: Run broader Iceberg IVM smoke subset**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --profile dev-opt --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm \
  --only iceberg_backed_mv_projection_filter,iceberg_ivm_aggregate_target,iceberg_ivm_join_two_base_delta,iceberg_ivm_union_projection_filter \
  --mode verify
```

Expected: PASS.

- [ ] **Step 6: Review final diff**

Run:

```bash
git diff --stat
git diff -- src/engine/mv/iceberg_refresh.rs src/meta/repository/iceberg_operation.rs tests/meta_repository.rs sql-tests/iceberg-ivm/sql/iceberg_ivm_projection_repartition.sql sql-tests/iceberg-ivm/result/iceberg_ivm_projection_repartition.result
```

Expected: diff contains only repartition support, operation intent persistence, recovery finalize replay, and SQL golden updates.

- [ ] **Step 7: Commit validation cleanups if formatting changed files**

If `cargo fmt` changed files after the previous commits, run:

```bash
git add src/engine/mv/iceberg_refresh.rs src/meta/repository/iceberg_operation.rs tests/meta_repository.rs
git commit -m "style: format iceberg mv repartition changes"
```

Expected: commit succeeds only when there are formatting changes.

## Self-Review

- Spec coverage: aggregate repartition is covered by Task 4; join and multi-base builders are covered by Task 5; single active spec and contract update are covered by Task 2 and Task 6; operation lifecycle metadata is covered by Task 3 and Task 6; SQL golden update is covered by Task 7.
- Placeholder scan: this plan contains no open-ended placeholder markers, no unscoped "add tests" instruction, and no step that asks the engineer to invent behavior without code or commands.
- Type consistency: `RepartitionSupport`, `MvRepartitionOperationIntent`, `RepartitionRebuildPayload`, and `record_commit_request` are defined before later tasks reference them. `commit_repartition_rebuild_payload` is the shared non-join commit path; `commit_join_repartition_overwrite` is the join-specific overwrite path.
