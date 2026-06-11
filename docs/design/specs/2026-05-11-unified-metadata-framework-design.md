# NovaRocks 统一 Metadata Framework 设计

日期：2026-05-11

## 背景

`META-1` 到 `META-3` 已经在当前分支提供了 provider 事务语义、record/revision 模型和 SQLite provider shell。`META-4` 已在其他会话实现，本设计不重新覆盖该部分。

当前剩余目标是从 `META-5` 到 `META-12` 完成统一 metadata framework：

- 收敛 ID allocation、revision 和 CAS 原语。
- 把 managed-lake、MV、Iceberg catalog registration、job 等领域 metadata 从 SQLite 具体 API 中拆出。
- 在统一 provider transaction 上建立 managed-lake publish txn 与 MV refresh transaction/finalize 语义。
- 支持 provider bootstrap，并把 Iceberg MV orchestration 从 managed-lake 模块中移出。

该系统仍处于开发阶段，不需要兼容已部署的旧 metadata DB。设计优先级是语义清晰、模块边界干净、测试可证明，而不是 legacy schema 升级。

## 目标

本 PR 以一个统一落地为目标，但内部按依赖分层推进：

```text
META-5 -> META-8 -> META-10 -> META-6 -> META-7 -> META-9 -> META-11 -> META-12
```

最终结构应满足：

```text
engine / connector flows
  -> domain repositories
       -> MetaReadTxn / MetaWriteTxn
            -> MetaStoreProvider
                 -> SQLite provider
```

`MetaStoreProvider` 只提供事务、record、revision、ID allocation 和 provider capability。Repository 负责 domain metadata 的 typed API、key/payload 编码、状态机校验和领域错误映射。

## 非目标

- 不保留旧 `SqliteMetadataStore` domain 表作为新的语义来源。
- 不设计旧 DB 到新 provider records 的兼容迁移。
- 不实现 remote metadata provider。
- 不把 provider 层设计成 MV、managed-lake 或 Iceberg catalog 的业务模型。
- 不让 engine / connector flow 直接拼 record key、解析 payload 或操作 provider-private schema。

## Repository 抽象

Repository 是某个 metadata 领域模型的持久化边界和事务内操作集合。它不是数据库连接，也不是 provider 的替代品。

职责划分：

- Provider 负责底层原语：snapshot read、atomic write、record revision、ID allocation、commit outcome。
- Repository 负责领域语义：对象模型、状态机、lookup record、payload schema、CAS 策略和领域错误。
- Flow 层只调用 typed repository API，不关心 record key、payload encoding 或 SQLite schema。

多个 repository 可以共享同一个 `MetaWriteTxn`，从而实现跨领域原子更新。例如一次 MV refresh finalize 可以在同一个 metadata commit 内更新 MV refresh state、managed-lake visible version 和 job state。

## 模块布局

新增或整理后的中性模块建议如下：

```text
src/meta/
  error.rs
  id.rs
  keys.rs
  mod.rs
  payload.rs
  provider.rs
  record.rs
  repository/
    mod.rs
    iceberg_catalog.rs
    job.rs
    managed_lake.rs
    managed_txn.rs
    mv.rs
  sqlite/
    mod.rs
    schema.rs
    txn.rs
```

`connector/starrocks/managed` 只保留 managed-lake 存储和执行相关代码。Metadata 读写应通过 `src/meta/repository`，而不是继续扩展 `SqliteMetadataStore`。

## Provider 原语收敛

`META-5` 的重点是让业务 ID 和 revision 原语从业务 SQL update 中移出：

- 使用稳定的 `IdScope` 常量分配 ID，例如 `managed.db`、`managed.table`、`managed.partition`、`managed.index`、`managed.tablet`、`managed.txn`、`mv.id`、`job.erase`、`job.iceberg_optimize`、`refresh.id`。
- `visible_version`、`commit_version` 等业务版本由 repository 在同一个 `MetaWriteTxn` 中读取当前对象、校验状态并写回。
- 状态机记录使用 `ExpectedRevision::Exact` 或等价 CAS helper，避免 last-writer-wins 覆盖状态。
- Provider record revision 只用于同一个 key 的 CAS，不暴露为业务 version。

`src/meta` 可增加 key builder、payload serde helper 和 repository error adapter，但这些 helper 不应了解具体 domain 业务。

## Domain Record Model

统一 provider record 是新的唯一语义源。建议 namespace 与主 key 结构如下。

### Managed-Lake

```text
managed/database/{db_id}
managed/database-name/{normalized_db_name}
managed/table/{table_id}
managed/table-name/{db_id}/{normalized_table_name}
managed/schema/{schema_id}
managed/table-current-schema/{table_id}
managed/partition/{partition_id}
managed/table-partition/{table_id}/{normalized_partition_name}
managed/index/{index_id}
managed/tablet/{tablet_id}
managed/partition-tablets/{partition_id}/{bucket_seq}
```

对象 payload 保存 typed domain struct。Name lookup record 只保存目标 ID 和必要的 normalized identity。

### Managed-Lake Transaction

```text
managed.txn/{txn_id}
managed.txn/by-table/{table_id}/{txn_id}
managed.txn/by-partition/{partition_id}/{txn_id}
```

主记录保存 `txn_id`、`table_id`、`partition_id`、`base_version`、`commit_version`、`state`、`retry_at_ms`、`updated_at_ms`。

### Materialized View

```text
mv/by-id/{mv_id}
mv/by-target/{catalog}/{namespace}/{table}
mv/dependency/{mv_id}/{catalog}/{namespace}/{table}
mv/refresh/{refresh_id}
mv/refresh/by-mv/{mv_id}/{refresh_id}
```

MV definition 保存 SQL、refresh mode、base dependencies、primary key columns、storage engine、target identity、last refresh snapshot map、target snapshot id 和 refresh summary。

Target lookup record 避免通过 scan 反推 `catalog.namespace.table -> mv_id`。

### Iceberg Catalog Registration

```text
iceberg.catalog/catalog/{catalog}
iceberg.catalog/namespace/{catalog}/{namespace}
iceberg.catalog/table/{catalog}/{namespace}/{table}
```

Catalog payload 保存 properties；namespace/table registration payload 保存 canonical identity 和创建时间等最小元信息。

### Job

```text
job/erase/{job_id}
job/iceberg-optimize/{job_id}
job/refresh/{job_id}
```

Job repository 负责 claim、finish、fail、retry 状态机。Job ID 使用 provider ID scope，不再通过 `MAX(id) + 1` 分配。

## Managed-Lake Repository

`ManagedLakeMetaRepository` 负责 database/table/schema/partition/index/tablet 的 typed CRUD 和 snapshot reconstruction。

关键规则：

- 创建 database/table 时，ID allocation、主记录写入和 name lookup 写入必须在同一个 `MetaWriteTxn` 内完成。
- 删除 table/partition 时，repository 写入 retired/dropping 状态和 erase job record，避免 flow 层同时操作多个 metadata 表。
- snapshot reconstruction 从 provider records scan 得到，与物理 provider schema 解耦。
- `visible_version` 和 `next_version` 只通过 repository 方法推进。

`ManagedLakeTxnRepository` 负责 publish txn：

```text
Prepared -> Written -> Visible
         -> Aborted
```

规则：

- `prepare` 读取 partition 当前 visible version，生成 `commit_version = visible_version + 1`，并写入 prepared txn。
- `mark_written` 必须 CAS 从 `Prepared` 到 `Written`。
- `mark_visible` 必须 CAS txn state，并在同一个 metadata txn 中推进 partition visible version。
- `abort` 只能推进 txn state，不改变 partition visible version。

## MV Repository

`MvMetaRepository` 负责 MV definition、dependency、target lookup 和 refresh metadata。

关键 API 包括：

- `create_mv_definition`
- `find_by_target`
- `drop_by_target`
- `begin_refresh_intent`
- `record_external_commit_outcome`
- `finalize_refresh`
- `abort_refresh`

MV repository 必须维护 target lookup 与 dependency record 的一致性。Catalog/namespace/table 删除时，Iceberg catalog repository 可以在同一个 transaction 中调用 MV repository 清理相关关系。

## Refresh Transaction Framework

`META-9` 在 MV repository 上建立 refresh transaction intent/finalize 语义：

```text
IntentCreated -> ExternalCommitted -> Finalized
              -> AbortRequested -> Aborted
              -> CommitUnknown
```

Refresh intent payload 至少包含：

- `refresh_id`
- `mv_id`
- target table identity
- target snapshot before refresh
- base table snapshot map
- base table UUID map
- expected MV record revision
- external commit identifier 或 commit outcome
- finalization state

Finalize 必须幂等。外部 Iceberg commit 成功但 metadata finalize 崩溃时，恢复流程应读取 intent，确认 external outcome，然后只补 metadata finalize。不能因为 metadata commit 失败就盲目重复写 target table。

## Iceberg MV Orchestration Relocation

`META-12` 在 repository 和 refresh transaction 可用后执行。

当前 Iceberg MV refresh orchestration 不应继续放在 `connector/starrocks/managed`。新位置应是中性模块，例如：

```text
src/engine/mv/iceberg_refresh.rs
```

或等价的中性路径。该模块只依赖：

- `MvMetaRepository`
- `IcebergCatalogMetaRepository`
- provider transaction
- Iceberg connector commit API
- 必要的 managed-lake adapter trait

managed-lake 模块只保留 managed-lake storage/write/path 相关能力。Iceberg MV orchestration 通过 trait 或 typed adapter 调用这些能力，而不是反向寄居在 managed 模块中。

## Bootstrap

`META-11` 引入 provider 配置和启动初始化：

```toml
[metadata]
provider = "sqlite"
path = "meta/catalog.db"
```

Standalone 启动流程：

```text
config
  -> create MetaStoreProvider
  -> initialize provider generic schema
  -> construct repositories
  -> build StandaloneState
  -> engine / connector flows use repositories
```

因为不需要兼容旧部署，`[standalone_server].metadata_db_path` 可以被迁移为 `[metadata].path` 的新配置入口，并清理调用链中对旧字段的强依赖。

## Error Handling

错误分三层：

- Provider 层返回 `MetaErrorKind`，表达 conflict、not found、commit unknown、provider corruption、invalid request 等底层事实。
- Repository 层映射为领域错误，例如 MV not found、refresh conflict、managed txn conflict、catalog registration conflict。
- Engine/server 层返回明确的人类可读错误，不暴露 SQLite 细节或 provider-private record key。

状态机冲突默认不重试覆盖。上层如果要重试，必须重新读取 repository state 后重新构造请求。

## Testing Strategy

测试按层组织：

- Provider conformance tests：atomic commit、snapshot read、ID allocation、CAS、delete、scan。
- Repository unit tests：每个 repository 用 SQLite provider 的临时 DB 验证 typed record、lookup、一致性和状态机。
- Flow tests：managed DDL/txn、MV refresh、Iceberg catalog registration 改到 repository 后保留现有 Rust 覆盖。
- SQL tests：最终跑一组 targeted SQL case，覆盖 DDL、refresh、catalog/table drop 与 lookup 清理。

需要 Iceberg REST 时，使用当前 worktree 的生成环境：

```bash
source docker/iceberg-rest/runtime/current/env.sh
```

不猜测 NovaRocks server port，不使用固定 `9030`。

## Completion Criteria

完成后应满足：

- 业务路径不再直接依赖 `SqliteMetadataStore` 的 domain SQL API。
- 新 metadata 语义通过 repository records 表达。
- ID allocation 统一走 provider transaction。
- managed-lake txn 和 MV refresh finalize 有明确状态机和 CAS 保护。
- Iceberg catalog registration 与 MV relationship 清理通过 repository 完成。
- Standalone bootstrap 从统一 metadata provider 构造 repository。
- Iceberg MV orchestration 已移出 managed-lake 模块。
- Focused Rust tests、provider/repository tests 和必要 SQL tests 通过。
