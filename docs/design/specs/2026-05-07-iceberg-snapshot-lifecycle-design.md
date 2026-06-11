# Iceberg Snapshot 生命周期治理设计

- **日期**：2026-05-07
- **范围**：Iceberg 完成度清单 §8.3 全部 3 项剩余语法
- **关联清单**：[[NovaRocks Iceberg v3 完成度清单]] §8.3 / §11
- **关联 spec**：[[2026-05-06-iceberg-v3-write-path-completion-design]]（同步执行 + commit-action 模式来源）/ [[2026-05-06-iceberg-schema-evolution-phase2-design]]（`commit_with_retry` 来源）
- **状态**：待实现

---

## 0. 背景与目标

### 0.1 当前状态

NovaRocks 写路径补齐 PR-1/2/3 已落地（TRUNCATE / OVERWRITE PARTITIONS / CTAS）。Iceberg
完成度清单 §8.3 Snapshot 生命周期治理仍是空白：

- snapshot 历史无限增长，metadata.json 越积越大
- 失败写入留下的孤儿 data file 没有回收路径（CTAS spec 已显式留 TODO）
- manifest 长期不合并，plan 阶段越来越慢

业界标杆（Spark / Trino / Flink）都用 `EXPIRE SNAPSHOTS` / `REMOVE ORPHAN FILES`
/ `REWRITE MANIFESTS` 三件套。本 spec 给 NovaRocks 补齐对应能力。

### 0.2 目标

- 实现 3 条 SQL 语法，与 StarRocks 方言 `ALTER TABLE x OPTIMIZE` 一致
- 同步执行模型，复用写路径补齐的 commit-action / `commit_with_retry` 基础设施
- v2 + v3 表统一支持
- 所有 3 条命令一个 PR 落地（用户决策：参见 §11）

### 0.3 Non-goals

- 多步骤事务（一次 transaction 跨多个 maintenance op）
- 异步执行 / job queue（与 OPTIMIZE 不同款）
- per-ref retention 属性读取（`branch.<n>.min-snapshots-to-keep` /
  `branch.<n>.max-snapshot-age-ms` / `branch.<n>.max-ref-age-ms`）
- 自动调度（cron / auto-maintenance）
- ORPHAN 并发扫描 / 并发删除
- REWRITE MANIFESTS 按目标大小（如 8MB）分组
- REWRITE MANIFESTS 在 branch 上工作
- EXPIRE / ORPHAN 在 branch 上工作
- REWRITE POSITION DELETES（v2→DV 升级路径，清单 §11 单独条目）

---

## 1. SQL 语法

```sql
ALTER TABLE <tbl> EXPIRE SNAPSHOTS [OLDER THAN '<ts>'] [RETAIN LAST <n>];
ALTER TABLE <tbl> REMOVE ORPHAN FILES OLDER THAN '<ts>';
ALTER TABLE <tbl> REWRITE MANIFESTS;
```

`<ts>` 接受：

- RFC 3339（`'2026-04-01T00:00:00Z'`）
- `YYYY-MM-DD HH:MM:SS`（`'2026-04-01 00:00:00'`）
- epoch-ms 整数（`1743465600000`）

复用 `src/sql/analyzer/iceberg_ref.rs` 的 timestamp 解析器（time-travel
SELECT 已落地用同款解析）。

### 1.1 Parse-time reject 矩阵

| 命令 | 必须给 | 不接受 |
|---|---|---|
| EXPIRE SNAPSHOTS | 至少 OLDER THAN 或 RETAIN LAST 之一 | 两者全无 / `t.branch_<x>` 后缀 / `RETAIN LAST 0` |
| REMOVE ORPHAN FILES | OLDER THAN 强制 | 不写 OLDER THAN / `t.branch_<x>` 后缀 |
| REWRITE MANIFESTS | —— | `t.branch_<x>` 后缀 |

### 1.2 Engine-level reject

- 非 iceberg backend → `"<COMMAND> only supports iceberg backends, got <name>"`
- 表不存在 → 复用 `resolve_existing_table_target` 错误

---

## 2. 架构与代码布局

### 2.1 分层

| 层 | 文件 | 责任 |
|---|---|---|
| **Parser** | `src/engine/statement.rs` | `looks_like_alter_table_*` 探测 + `parse_alter_table_*_sql` 分析 + 3 个新 stmt struct |
| **Engine entry** | `src/engine/iceberg_expire_snapshots.rs` | `execute_iceberg_expire_snapshots(state, target, stmt)` |
| | `src/engine/iceberg_remove_orphan_files.rs` | `execute_iceberg_remove_orphan_files(state, target, stmt)` |
| | `src/engine/iceberg_rewrite_manifests.rs` | `execute_iceberg_rewrite_manifests(state, target)` |
| **Engine dispatch** | `src/engine/mod.rs::execute_in_context` | 在 `looks_like_alter_table_optimize` 旁加 3 个分支 |
| **Commit-action** | `src/connector/iceberg/commit/expire_snapshots.rs` | `ExpireSnapshotsCommit` 实现 `CommitAction` |
| | `src/connector/iceberg/commit/remove_orphan_files.rs` | `RemoveOrphanFilesAction`（不走 `CommitAction` trait，纯物理删除）|
| | `src/connector/iceberg/commit/rewrite_manifests.rs` | `RewriteManifestsCommit` 实现 `CommitAction` |
| **CommitOpKind 扩展** | `src/connector/iceberg/commit/types.rs` | 新增 `CommitOpKind::ExpireSnapshots` / `RewriteManifests` |

### 2.2 核心架构原则

- **同步执行**：复用 `iceberg_writer.rs` / `iceberg_truncate.rs` 的 catalog handle / `block_on_iceberg` 模式
- **OCC + retry**：复用 schema-evolution PR-2 的 `commit_with_retry`（3 次指数退避 10/100/500ms），仅 EXPIRE / REWRITE 用（ORPHAN 无 commit）
- **物理删除 best-effort**：删除文件失败时记 `log::warn!` 不回滚（与 abort log 哲学一致）
- **不做 reader-coordination**：phase 1 不防御 "EXPIRE 进行中有人正在读老 snapshot"；spec §10 R1 列为已知限制

### 2.3 共享工具函数

新增模块 `src/connector/iceberg/commit/snapshot_lifecycle_helpers.rs`，三条命令共用：

- `compute_live_snapshot_set(metadata) -> HashSet<i64>` —— 走所有 ref 的 ancestor chain
- `enumerate_files_for_snapshots(catalog, metadata, snapshot_ids) -> Result<FileSet>` —— 给定 snapshot 集合，枚举其引用的所有 (data, delete, manifest, manifest-list) 文件路径
- `puffin_half_reference_protection(candidates: &mut FileSet, all_manifest_entries: &[ManifestEntry], live_data_files: &FileSet)` —— 文件级 puffin 保护

---

## 3. EXPIRE SNAPSHOTS 算法

### 3.1 输入

```rust
pub(crate) struct AlterTableExpireSnapshotsStmt {
    pub table: ObjectName,
    pub older_than_ms: Option<i64>,
    pub retain_last: Option<u32>,
}
```

至少给一个；二者可同时给。

### 3.2 算法

**Step 1：枚举 live snapshot 集合**

```text
live_set = ∅
snapshot_by_id = { s.snapshot_id → s for s in metadata.snapshots() }
for ref in metadata.refs():        # 所有 branch + tag
    sid = Some(ref.snapshot_id)
    while sid is Some:
        live_set.insert(sid.unwrap())
        sid = snapshot_by_id[sid.unwrap()].parent_snapshot_id
```

`live_set` = 任何 ref 祖先链覆盖的 snapshot id。

**Step 2：枚举 candidate-to-expire**

```text
candidates = { s ∈ snapshots() | s.snapshot_id ∉ live_set }
```

**Step 3：应用 OLDER THAN**

```text
if older_than_ms.is_some():
    candidates.retain(|s| s.timestamp_ms < older_than_ms)
```

**Step 4：应用 RETAIN LAST N（仅保护 main ancestor chain）**

```text
if retain_last.is_some():
    main_chain = ancestors(metadata.current_snapshot_id)
                  sorted by timestamp_ms descending
    keep_set = main_chain[..min(N, main_chain.len())]
    candidates -= keep_set
```

**Step 5：candidates 为空 → 早返回**

不写 metadata.json，不删文件，返回 `Ok`。

**Step 6：枚举要删除的文件**

```text
files_to_check = ∅
for s in candidates:
    files_to_check.insert(manifest_list_path(s))
    for m in manifests_of(s):
        files_to_check.insert(m.path)
        for entry in m.entries():
            files_to_check.insert(entry.data_file.file_path)

# 保护：所有"不被 expire 的 snapshot"引用的文件，含 live_set
# 以及 RETAIN LAST 保住的 main chain 老 snapshot
# 以及虽 non-live 但因 OLDER THAN 阈值未到而留在 metadata 中的 snapshot
protected_snapshots = metadata.snapshots() - candidates
live_files = enumerate_files_for_snapshots(protected_snapshots)
files_to_delete = files_to_check - live_files
```

**Step 7：Puffin 半引用保护**

```text
for puffin_path in files_to_delete.iter().filter(is_puffin):
    if any blob in puffin_path references a data file ∈ live_files:
        files_to_delete.remove(puffin_path)
```

实现：构建 `puffin_path → set<referenced_data_file>` 索引，从 manifest entry
的 `(file_path, referenced_data_file)` 二元组聚合。

**Step 8：构造新 metadata + commit**

新 metadata.json：

- `snapshots`：移除 `candidates`
- `snapshot_log`：同步移除（保持 chronological 不变量）
- `current_snapshot_id`、`refs`、`last_sequence_number`：完全不变

`TableUpdate`：`RemoveSnapshots { snapshot_ids: candidates.collect() }`
+ 必要时手动调整 `snapshot_log`（iceberg-rust 0.9 的 `RemoveSnapshots`
是否自动同步 snapshot_log，实施时 spike；若不自动则补 `SetSnapshotRefs` /
`SetCurrentSchema` 都不需要，只需要 RemoveSnapshots + 手动 snapshot_log 修剪
通过 `MetadataPatch` 风格走）。

走 `commit_with_retry`（OCC：`AssertCurrentSchemaIdMatch` +
`AssertRefSnapshotIdMatch`）。冲突重试时重 enumerate live_set / candidates。

**Step 9：物理删除（best-effort）**

新 metadata.json commit 成功后，对 `files_to_delete` 逐个调对象存储 delete API：

- 失败：`log::warn!` 记 path + error，不回滚
- 串行（phase 1，简化错误处理）

### 3.3 边界

| 场景 | 行为 |
|---|---|
| 表无 snapshot | candidates 空，早返回 Ok |
| OLDER THAN 时间在未来 | 接受（candidates 自然空） |
| RETAIN LAST = 0 | parse-time reject |
| RETAIN LAST > 总 snapshot 数 | 接受（保护所有，candidates 空） |
| OLDER THAN + RETAIN LAST 全无 | parse-time reject |
| 表只有 1 个 snapshot | main ref 保护，noop |

### 3.4 不变量（实施时 assert）

1. commit 后所有 ref `snapshot_id` ∈ `new metadata.snapshots`
2. commit 后 `current_snapshot_id` 仍存在
3. commit 后 `last_sequence_number` 不倒退

---

## 4. REMOVE ORPHAN FILES 算法

### 4.1 输入

```rust
pub(crate) struct AlterTableRemoveOrphanFilesStmt {
    pub table: ObjectName,
    pub older_than_ms: i64,  // mandatory
}
```

### 4.2 算法

**Step 1：枚举 live file set（保护所有 metadata 中的 snapshot，不只 live_set）**

```text
live_files = ∅
live_files.insert(<location>/metadata/<current>.metadata.json)
for log_entry in metadata.metadata_log():
    live_files.insert(log_entry.metadata_file)

for s in metadata.snapshots():       # 注意：所有 snapshot，不只 reachable
    live_files.insert(manifest_list_path(s))
    for m in manifests_of(s):
        live_files.insert(m.path)
        for entry in m.entries():
            live_files.insert(entry.data_file.file_path)
```

**关键语义**：ORPHAN 必须保护 metadata.json 中**所有 snapshot**（含 non-live）。
理由：metadata.json 里登记的 snapshot 都还在，对应文件就还有引用。EXPIRE 才是
把 snapshot 从 metadata 摘掉的命令；如果用户没 EXPIRE 就跑 ORPHAN，孤儿不
会包括 "non-live snapshot 的文件"。

**Step 2：扫描 warehouse 物理路径**

```text
scan_paths = [
    "<location>/data/",       # 含 _staging 子目录
    "<location>/metadata/",
]
all_files = recursively list all files under scan_paths
```

走 `src/fs/` 的 `ObjectStore` / opendal `Operator`（按 catalog file IO 选）。
`mtime`（`last_modified`）从 list API 直接拿。

**保护：scan_path 必须落在表 location 下**

```text
canonical(scan_path).starts_with(canonical(table.location))
```

防御 location 被构造逃出表目录。

**Step 3：算 candidate**

```text
candidates = ∅
for f in all_files:
    if f.path ∈ live_files: continue
    if f.last_modified_ms >= older_than_ms: continue
    candidates.insert(f.path)
```

**Step 4：Puffin 半引用保护**（同 §3.2 Step 7，保守起见仍加）

**Step 5：物理删除**

逐个 delete，失败记 warning 不回滚，**不写新 metadata.json**。

### 4.3 边界

| 场景 | 行为 |
|---|---|
| 不写 OLDER THAN | parse-time reject |
| OLDER THAN 在未来 | 接受 |
| 表无 snapshot（CREATE TABLE 后未写入）| 接受；只保 metadata.json + metadata-log |
| 扫描发现 0 个文件 | 接受，noop |
| `data/_staging/` 下文件 | 按 OLDER THAN 阈值统一处理 |

### 4.4 性能

- 大表 100k+ 文件场景：单线程扫，按 list API 分页（每页 1000）
- 内存预估：100k path × 200B ≈ 20MB，可承受
- 并发扫 / 并发删留作后续优化

### 4.5 不变量

1. 当前 metadata.json 不能被删（`live_files` 必须包含）
2. metadata-log 引用的所有历史 metadata.json 不能被删

---

## 5. REWRITE MANIFESTS 算法

### 5.1 输入

```rust
pub(crate) struct AlterTableRewriteManifestsStmt {
    pub table: ObjectName,
}
```

### 5.2 算法

**Step 1：加载 + 早返回**

```text
table = load_table()
metadata = table.metadata()
current = metadata.current_snapshot()
if current is None:        return Ok       # 空表
manifest_list = read(current.manifest_list_path)
if manifest_list.entries().count() <= 1: return Ok   # 单 manifest
```

**Step 2：按 (partition_spec_id, content) 分组**

```text
groups: Map<(spec_id: i32, content: ManifestContentType), Vec<ManifestFile>> = ∅
for m in manifest_list.entries():
    key = (m.partition_spec_id, m.content)    # content ∈ {Data, Deletes}
    groups[key].push(m)
```

**Step 3：每组合并**

```text
new_manifests: Vec<ManifestFile> = []
for (key, group) in groups:
    if group.len() == 1:
        new_manifests.push(group[0])         # 单 manifest 组：原样保留
        continue

    merged_entries = []
    for m in group:
        for entry in read_manifest(m).entries():
            if entry.status == DELETED: continue   # spec：丢弃 DELETED
            merged_entries.push(entry.with_status(EXISTING))

    new_manifest = write_manifest(
        path = "<location>/metadata/<uuid>-m0.avro",
        partition_spec_id = key.0,
        content = key.1,
        entries = merged_entries,
        snapshot_id = new_snapshot_id,       # 在 Step 5 分配
    )
    new_manifests.push(new_manifest)
```

**v3 row-lineage 字段保留要求**（必须逐字段 round-trip）：

- `data_file.first_row_id`
- `entry.snapshot_id` / `sequence_number` / `file_sequence_number`（保留原值，不重写）
- `data_file.row_range` / `null_value_counts` / `lower_bounds` / `upper_bounds` / `key_metadata`
- delete file 的 `referenced_data_file` / `content_offset` / `content_size_in_bytes`

→ 优先复用 iceberg-rust 0.9 的 `ManifestEntry` 解码再编码，**不手工拆字段**。
若 0.9 的 `ManifestEntry` round-trip 缺字段（实施时 spike 验证），fallback
到 NovaRocks 自有 manifest writer（`src/connector/iceberg/commit/` 已有）。

**Step 4：早返回 noop**

```text
if 每组都只有 1 个 manifest（new_manifests 与原始完全一致）:
    return Ok       # 不 commit 新 snapshot
```

**Step 5：写新 manifest list + commit replace snapshot**

```text
new_snapshot_id = generate_snapshot_id()
manifest_list_path = "<location>/metadata/snap-<id>-1-<uuid>.avro"
# 关键：snapshot-level sequence_number 必须严格递增（iceberg-rs catalog 不变量
# table_metadata_builder.rs:358：snapshot.sequence_number > last_sequence_number）。
# 每个 manifest entry 的 data_sequence_number / file_sequence_number 保留不变（合并不动 data）。
new_seq = metadata.last_sequence_number + 1
write_manifest_list(manifest_list_path, new_manifests, sequence_number = new_seq)

new_snapshot = Snapshot {
    snapshot_id: new_snapshot_id,
    parent_snapshot_id: Some(current.snapshot_id),
    sequence_number: new_seq,
    timestamp_ms: now_ms(),
    manifest_list: manifest_list_path,
    summary: {
        "operation": "replace",
        "replaced-manifests-count": <count>,
        "added-manifests-count": <count>,
    },
    schema_id: current.schema_id,
}

# TableUpdate 序列：AddSnapshot + SetSnapshotRef("main", new_snapshot_id)
# OCC 由 AssertRefSnapshotIdMatch("main", current.snapshot_id) 保护
commit_with_retry([
    TableUpdate::AddSnapshot { snapshot: new_snapshot },
    TableUpdate::SetSnapshotRef { ref_name: "main", reference: SnapshotReference { snapshot_id: new_snapshot_id, ... } },
])
```

`operation=replace` 是 spec 允许的合法 op；下游读路径已支持（compact 也用过）。
**注意 sequence_number 语义**：snapshot 自身的 `sequence_number` 必须 +1（catalog 不变量），
但每个 manifest entry 的 `data_sequence_number` / `file_sequence_number` 保留入参原值（reflect
the original commit that introduced each file）。"replace doesn't introduce new data" 指的是
后者，不是前者。

**Step 6：物理删除老 manifest（best-effort）**

被合并的老 manifest 文件可以删（manifest list 已不再引用）。

**注意**：老 manifest list **不能删**（历史 snapshot 还在 metadata 里、还引用它）。

### 5.3 边界

| 场景 | 行为 |
|---|---|
| 空表（无 snapshot）| noop |
| 单 manifest 表 | noop |
| 所有组都是 1 个 manifest | noop（不写新 snapshot） |
| partition evolution 多 spec_id | 按 spec_id 分组，每组独立 |
| 同时含 data + delete manifest | 按 content 分组 |
| manifest entry 有 DELETED status | 合并时丢弃 |

### 5.4 不变量

1. 合并组内所有 entry 的 `data_file.first_row_id` round-trip 全等
2. commit 后 manifest list 总 record_count 与 commit 前一致（DELETED 丢弃后，ADDED+EXISTING 总和守恒）
3. commit 后 `last_sequence_number` 严格 +1（catalog 不变量）；但每个 manifest entry 的 `data_sequence_number` / `file_sequence_number` 保留 commit 前原值
4. commit 后 main ref 指向 new_snapshot_id，parent 链可回溯到 commit 前 current

---

## 6. 错误分类与处理

| 类型 | 处理 |
|---|---|
| Parse-time（语法错 / branch suffix / EXPIRE 无子句 / ORPHAN 无 OLDER THAN） | 返回 `Err(String)`，不进 engine 层 |
| 表不存在 / 非 iceberg backend | engine 层早返回错误，与 OPTIMIZE / TRUNCATE 一致 |
| Catalog commit conflict（EXPIRE / REWRITE） | `commit_with_retry` 3 次指数退避，超限报错 |
| 物理删除失败（EXPIRE / ORPHAN / REWRITE） | `log::warn!` 记 path + error，不回滚 commit |
| Object store list 失败（ORPHAN 扫描）| 整命令失败（无 commit 副作用，安全 retry） |
| Manifest 解码失败（REWRITE） | 整命令失败（不写新 metadata.json） |

---

## 7. 测试计划

### 7.1 SQL 套件（end-to-end record + verify）

**`iceberg_v3_expire_snapshots.sql`** （约 12 case）

- happy path：5 次 INSERT 后 OLDER THAN 阈值删 3 个 → 验证 metadata.json snapshot 数 + data file 物理已删
- RETAIN LAST 5 → 验证保留最新 5 个 main chain
- OLDER THAN + RETAIN LAST 同时给 → 验证交集语义
- branch 保护：建 branch B 指向老 snapshot → EXPIRE OLDER THAN 不能删 B 的 ancestor
- tag 保护：同上 with tag
- 无 ref 的 dangling snapshot（branch 删除后）→ 可以被 EXPIRE
- 表无子句 → reject
- branch suffix → reject
- v2 表（含 position-delete）→ position-delete file 同步删
- v3 表（含 DV puffin）→ puffin 文件按半引用规则
- DV puffin 半引用：1 puffin 含 2 blob、1 个关联 live data → puffin 保留
- RETAIN LAST = 0 → reject

**`iceberg_v3_remove_orphan_files.sql`** （约 10 case）

- happy path：手动放孤儿文件到 `data/` → ORPHAN OLDER THAN '1970-01-01' 删除
- 不写 OLDER THAN → reject
- 保护当前 metadata.json + 历史 metadata-log
- 保护 staging 目录"未到阈值"文件
- 删除 staging 目录"过阈值"文件
- v2 / v3 表分别覆盖
- branch suffix → reject
- 大量文件（200+）→ 性能 smoke 不超时
- 表无 snapshot（CREATE TABLE 后）→ 接受
- 与 EXPIRE 联动：EXPIRE 后跑 ORPHAN 回收孤儿

**`iceberg_v3_rewrite_manifests.sql`** （约 10 case）

- happy path：5 次 INSERT 后 5 manifest → REWRITE 后 1 manifest，data 全保
- 单 manifest 表 → noop
- 空表 → noop
- partition evolution 多 spec_id → 按 spec 分组（每组 1 manifest）
- v3 row-lineage：REWRITE 前后 `_row_id` 全等（select 验证）
- v2 含 position-delete manifest → data + delete manifest 各自合并
- v3 含 DV manifest → DV 引用保留
- branch suffix → reject
- REWRITE 后 EXPIRE 仍正常工作
- REWRITE 后老 manifest 物理删除验证

### 7.2 单测

| 模块 | 数 | 覆盖 |
|---|---|---|
| `commit/expire_snapshots.rs::tests` | ~12 | reachability、puffin 半引用、RETAIN LAST 取最新 N、snapshot_log 同步裁剪、OCC retry、empty candidates 早返回 |
| `commit/remove_orphan_files.rs::tests` | ~10 | live_files 构造、扫描越界拒绝、OLDER THAN 过滤、puffin 半引用、metadata-log 保护 |
| `commit/rewrite_manifests.rs::tests` | ~10 | (spec_id, content) 分组、单 manifest noop、DELETED 丢弃、row-lineage 字段 round-trip、record_count 守恒、空表 noop |
| `engine/iceberg_*.rs::tests` 各 | ~3 | parse-time reject 矩阵 + engine-level reject |

合计：**~45 unit + 32 SQL case**。

### 7.3 必跑 regression

- 现有 `iceberg_*` 套件全套
- 现有 `iceberg_time_travel_select.sql`（confirm EXPIRE 不破坏 time travel 边界）
- 现有 `iceberg_branch_tag_ddl.sql` / `iceberg_branch_write.sql`（confirm ref 保护）

不跑 SSB / TPC-H / TPC-DS。

---

## 8. 关键不变量汇总

实施时 assert：

1. **EXPIRE**：commit 后所有 ref `snapshot_id` ∈ `new metadata.snapshots`
2. **EXPIRE**：commit 后 `current_snapshot_id` 仍存在
3. **EXPIRE**：commit 后 `last_sequence_number` 不倒退
4. **REWRITE**：commit 后 manifest list 总 record_count 与 commit 前一致
5. **REWRITE**：合并组内所有 entry 的 `data_file.first_row_id` round-trip 全等
6. **ORPHAN**：当前 metadata.json 不能被删
7. **ORPHAN**：metadata-log 引用的所有历史 metadata.json 不能被删
8. **REWRITE**：commit 后 `last_sequence_number` 严格 +1（catalog 不变量），但每个 manifest entry 的 `data_sequence_number` / `file_sequence_number` 保留原值

---

## 9. 与现有代码的复用边界

| 现有模块 | 复用点 |
|---|---|
| `iceberg_writer.rs::build_abort_cleanup_for_catalog_entry` | EXPIRE / REWRITE 失败时清理新写出但未 commit 的 manifest / metadata.json |
| `iceberg_truncate.rs` 结构 | engine 入口的 catalog handle / `block_on_iceberg` / `target_string` 模板 |
| `commit/helpers.rs::generate_snapshot_id / now_ms / metadata_dir / write_manifest_list` | REWRITE / EXPIRE 直接复用 |
| schema-evolution PR-2 `commit_with_retry` | EXPIRE / REWRITE 的 OCC 重试入口 |
| `sql/analyzer/iceberg_ref.rs` timestamp 解析 | EXPIRE / ORPHAN OLDER THAN 字面量解析 |
| `connector/iceberg/scan_deletes.rs` / `read.rs` 的 manifest entry 解析 | REWRITE 若 iceberg-rust 0.9 round-trip 不行时 fallback |
| `fs/object_store.rs` / opendal Operator | ORPHAN 扫 warehouse、EXPIRE/ORPHAN 物理删除 |

---

## 10. 风险

| # | 风险 | 概率 | 缓解 |
|---|---|---|---|
| R1 | EXPIRE 进行中有人在读老 snapshot，文件被删导致 reader 崩 | 中 | Phase 1 不做 reader-coordination；spec §0.3 列为已知限制；OLDER THAN 提供时间窗保护；建议低流量窗口跑 |
| R2 | ORPHAN 误删 in-flight 写入文件 | 低 | 强制 OLDER THAN（建议 ≥ 3 天）；扫描路径必须落在表 location 下 |
| R3 | REWRITE MANIFESTS 丢字段（v3 row-lineage / DV 引用）| 中 | 单测覆盖 round-trip；优先复用 iceberg-rust 0.9 `ManifestEntry`，不行用自有 writer；SQL 套件 select 比对 `_row_id` |
| R4 | Puffin 半引用判断错（删了还在用的 puffin）| 中 | EXPIRE / ORPHAN 都按文件级保守保留 |
| R5 | `commit_with_retry` 在 EXPIRE / REWRITE 路径上无 QueryContext，cancellation 不生效 | 低 | 与 schema-evolution PR-2 同款 limitation，留 `TODO(cancellation)` |
| R6 | 大表 ORPHAN 内存爆（100k+ 文件路径全装内存）| 低 | Phase 1 接受；spec §0.3 列；超大表用户改用外部脚本 |
| R7 | iceberg-rust 0.9 的 `ManifestEntry` 没有 `referenced_data_file` getter | 低 | 实施时如发现，用 NovaRocks 已有 manifest 解析路径（`scan_deletes.rs` / `read.rs`）|

---

## 11. 决策记录

| 决策点 | 选择 | 理由 |
|---|---|---|
| Scope 拆分 | 一个 spec + 一个大 PR（用户 §0.2 决策） | 三条命令共享 helper / 测试基础设施，单 PR review 量可控 |
| 执行模型 | 全同步 | 与写路径补齐 PR-1/2/3 一致；async 框架成本不值得 |
| EXPIRE 不写子句 | 报错 | 强制用户明确意图，避免误操作 |
| EXPIRE branch retention | Phase 1 不读 per-ref retention 属性 | 实现量可控；保守版只保护 ref 当前 snapshot；spec §0.3 列 |
| ORPHAN OLDER THAN | 强制 | 防御 in-flight 写入误删 |
| ORPHAN staging 处理 | 一并扫，按 OLDER THAN 阈值过滤 | 让 CTAS 失败的孤儿有兜底回收路径 |
| REWRITE 合并粒度 | 按 (spec_id, content) 分组，每组全合并到一个 manifest | 实现量比"按 8MB 分组"小；比"全表合一个"安全（partition evolution 表会炸） |
| Branch suffix 在三条命令 | 全 reject | 表级 maintenance 不应受 branch 概念污染 |
| Format version | v2 + v3 都支持 | v2 表也需要治理；puffin 半引用按文件级保守保留 |

---

## 12. 实施 PR 计划

按落地顺序（共 1 个 PR，但内部分 commit）：

1. **Helpers + types**（`snapshot_lifecycle_helpers.rs`、`CommitOpKind` 扩展、stmt struct）
2. **REWRITE MANIFESTS**（commit-action + engine entry + parser + SQL 套件 + 单测） —— 最简单，先打通骨架
3. **EXPIRE SNAPSHOTS**（commit-action + engine entry + parser + SQL 套件 + 单测）
4. **REMOVE ORPHAN FILES**（action + engine entry + parser + SQL 套件 + 单测）
5. **文档同步**（`docs/guides/iceberg-v3/maintenance.md` + `support-matrix.md` + 完成度清单 §8.3 / §11 / §20 勾选）

---

## 13. 文档同步清单（PR 落地时一并更新）

- `docs/guides/iceberg-v3/maintenance.md` —— 三条命令从 ❌ 改 ✅，补行为 + 示例 + 限制
- `docs/guides/iceberg-v3/reference/support-matrix.md` —— 同步勾选
- `docs/guides/iceberg-v3/overview.md` —— "需要 EXPIRE / ORPHAN / REWRITE MANIFESTS" 段落改写
- `NovaRocks Iceberg v3 完成度清单.md`（Obsidian） —— §8.3 三项 + §20 三套件 + 变更记录
