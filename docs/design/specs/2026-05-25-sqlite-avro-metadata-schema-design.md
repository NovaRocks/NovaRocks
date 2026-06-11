# SQLite Avro Metadata Schema 演进设计

日期：2026-05-25

## 背景

当前 SQLite metadata provider 已经收敛到 `src/meta` 的统一 provider 和 repository
结构。SQLite 物理层通过 `meta_records` 存储 `namespace + key -> payload`，业务
对象由 `src/meta/repository/*` 负责 typed API、key 构造和 payload 编解码。

当前 payload 仍是 JSON + 手写 `payload_schema_version` 检查。这个模型有两个问题：

- 如果采用启动时自动 `vN -> vN+1` rewrite，升级后会破坏回滚能力。高版本二进制一旦
  改写 metadata，低版本二进制可能无法再读取或写回。
- 如果每次字段变化都维护手写 migration step，长期会积累大量兼容逻辑，且很难删除。

本设计放弃“历史 JSON metadata 自动兼容”的目标，改为建立面向未来的 schema
evolution 合同：SQLite 只作为稳定 KV 外壳，业务 payload 使用 Avro，并由静态 schema
catalog 和 compatibility gate 管理升级与回滚边界。

## 目标

- 让业务 metadata schema 演进由 Avro writer schema / reader schema resolution
  承担，而不是由 repository 手写 `vN -> vN+1` 转换链承担。
- 默认要求普通 schema 变更满足 `FULL_TRANSITIVE`，使升级和回滚都可被测试证明。
- 保持 SQLite provider 的物理 schema 稳定，避免把业务字段演进变成 SQLite
  `ALTER TABLE` 链条。
- 对不兼容 schema 变化建立显式机制：新 subject 或 metadata epoch，而不是隐式迁移。
- 对已有非空 JSON metadata 库 fail fast，不做 JSON -> Avro 兼容迁移。

## 非目标

- 不兼容历史 JSON metadata DB。
- 不引入外部 Schema Registry 服务。
- 不把 schema 文本写入 SQLite 作为主要 registry。
- 不为普通字段演进写长期保留的手工 migration 链。
- 不允许 repository 绕过 schema catalog 直接用 serde struct 读写 provider payload。

## 总体架构

```text
engine / connector flows
  -> domain repositories
       -> Avro payload codec + static schema catalog
            -> MetaReadTxn / MetaWriteTxn
                 -> SQLite provider KV storage
```

三层职责如下：

- SQLite provider 负责事务、revision、ID allocation、record 持久化和 provider
  corruption 检查。
- Repository 负责领域 API、key 结构、状态机和 domain struct。
- Avro codec + schema catalog 负责 payload 编码、writer schema 查找、reader schema
  resolution、fingerprint 校验和 compatibility 测试。

SQLite 的 `kind` 字段即 Avro subject，例如 `mv.definition`、`managed.table`、
`job.erase`。同一个 subject 下的 schema 文件是 immutable 的版本历史。

## SQLite 物理模型

`meta_records` 继续保持 KV 语义：

```text
namespace TEXT NOT NULL
key TEXT NOT NULL
kind TEXT NOT NULL
revision INTEGER NOT NULL
payload_encoding TEXT NOT NULL
payload_schema_id INTEGER NOT NULL
payload_schema_fingerprint TEXT NOT NULL
payload BLOB NOT NULL
created_at_ms INTEGER NOT NULL
updated_at_ms INTEGER NOT NULL
PRIMARY KEY(namespace, key)
```

`payload_encoding` 新版本只写 `avro`。`payload_schema_id` 表示 writer schema id，
`payload_schema_fingerprint` 是 canonical schema fingerprint，用于防止 schema id 和
实际 schema 不一致。

当前的 `payload_schema_version` 不再表达 serde struct version。实现时应改名为
`payload_schema_id`，避免继续诱导手写版本迁移模型。

SQLite schema 后续只允许少量 provider-level additive 变化，例如索引、辅助统计表或
provider capability 元数据。业务字段不展开成 SQLite column。

`meta_provider_schema` 只保存 provider-level marker，不作为业务 schema registry：

```text
store_format = avro
metadata_epoch = 1
```

`store_format` 用于区分新的 Avro metadata store 和旧 JSON store。`metadata_epoch` 是不兼容
断点标记，不随普通 Avro schema 变更递增。空 DB 初始化这些 marker；非空但缺 marker 的 DB
拒绝启动。

## Static Schema Catalog

schema catalog 随代码提交，建议布局：

```text
src/meta/avro/schemas/
  mv.definition/0001.avsc
  mv.definition/0002.avsc
  managed.table/0001.avsc
  managed.partition/0001.avsc
  job.erase/0001.avsc
```

每个 `.avsc` 文件必须是 immutable 的。已合入的 schema 不能修改；新需求只能新增下一版。

catalog 在编译期或测试期加载并验证：

- subject + schema id 唯一。
- fingerprint 与 schema canonical form 匹配。
- latest schema 可作为当前 reader schema。
- 每个 subject 的历史 schema 满足 compatibility policy。

SQLite record 只保存 schema id/fingerprint，不保存 schema 文本。这样 standalone 不需要外部
registry 服务，且 schema 变更通过代码 review 和 CI 管理。

## Compatibility Policy

默认策略是 `FULL_TRANSITIVE`：

- latest reader schema 必须能读取该 subject 所有历史 writer schema 写出的 payload。
- 所有历史 reader schema 必须能读取 latest writer schema 写出的 payload。
- 检查范围是所有历史版本，不只相邻版本。

普通兼容变更包括：

- 新增带默认值的 optional 字段。
- 删除本来就 optional/default 的字段。
- 使用 Avro alias 做字段重命名，并通过 compatibility test。
- 仅在 Avro 规则允许且业务语义安全时做类型提升。

禁止作为普通兼容变更：

- 新增无默认值的关键字段。
- 删除旧版本必须依赖的字段。
- 复用字段名但改变业务语义。
- 增加旧 reader 无法安全处理的新 enum symbol。

`FULL_TRANSITIVE` 是必要条件，不是唯一条件。旧二进制回滚后可能读取新 payload，再按旧
schema 写回同一 record；这种写回会丢弃旧 schema 不认识的新字段。因此新增字段还必须满足
“被旧 writer 丢弃仍不破坏语义”。如果做不到，该变更必须新建 subject 或 bump metadata
epoch。

## Incompatible Changes

不兼容变更不能通过不断扩展 migration 链解决。可选路径只有两类：

- 新建 subject，例如从 `mv.definition` 切到更具体的新 record kind。
- bump metadata epoch，并在启动时明确拒绝低 epoch 二进制打开。

metadata epoch 是显式断点：进入新 epoch 后不承诺直接回滚，只能通过快照、导出恢复或专门
downgrade 工具处理。普通 schema 演进不得偷偷提升 epoch。

## Read Flow

读取 record 时：

1. Provider 返回 `kind`、schema id、fingerprint 和 Avro payload bytes。
2. Catalog 根据 `kind + schema_id` 查找 writer schema。
3. 校验 stored fingerprint 与 catalog fingerprint 一致。
4. 用 writer schema + latest reader schema 执行 Avro resolution。
5. 解码成 repository domain struct。

如果 schema id 未知、fingerprint 不匹配、encoding 不是 `avro`、resolution 失败或
payload 解码失败，返回明确错误并阻止 standalone state 恢复。

## Write Flow

写入 record 时：

1. Repository 根据 record kind 查找 latest writer schema。
2. Domain struct 按 latest schema 编码为 Avro binary。
3. `MetaWriteTxn::put` 写入 payload、schema id 和 fingerprint。
4. 继续使用现有 `ExpectedRevision` / revision token 做并发控制。

Repository 不应把 Avro resolution 当作业务默认值来源。业务上必须显式校验的字段仍然由
repository 或 flow 层校验。

## Startup Behavior

启动时采用 fail-fast 策略：

- 空 SQLite DB 初始化为 Avro metadata store。
- 非空 DB 缺少 `store_format = avro` marker，拒绝启动。
- 非空 DB 中存在 `payload_encoding != avro` 的 record，拒绝启动。
- 非空 DB 中存在缺失 schema id/fingerprint 的 record，拒绝启动。
- DB 的 `metadata_epoch` 高于当前代码支持范围，拒绝启动。
- schema id 未知、fingerprint 不匹配或 Avro 解码失败，拒绝启动。
- 不执行 JSON -> Avro 自动迁移。

这符合当前项目“不为历史用户写兼容迁移”的原则，同时为未来演进建立可测试合同。

## Testing

测试分四类：

1. Schema catalog 测试：扫描所有 `.avsc`，校验 subject/id/fingerprint 唯一、schema
   可解析、默认值合法。
2. Compatibility 测试：对每个 subject 执行 `FULL_TRANSITIVE` 检查，覆盖 latest 读所有
   historical writer，以及 historical reader 读 latest writer。
3. Repository round-trip 测试：核心 repository 使用 Avro 写入和读取 domain struct，覆盖
   MV definition、managed lake、job、txn 等 subject。
4. Startup rejection 测试：构造 JSON record、缺 schema id/fingerprint、未知 schema id、
   fingerprint mismatch 和坏 Avro payload，确认 fail fast。

预期验证命令：

```bash
cargo fmt -- --check
cargo check --all-targets
cargo test --test meta_sqlite_provider
cargo test --test meta_repository
```

如果实现触及 managed-lake 或 MV restore 行为，再追加对应 targeted Rust tests 和必要的
SQL suite。

## References

- [Avro specification](https://avro.apache.org/docs/1.12.0/specification/):
  writer schema / reader schema resolution.
- [Confluent Schema Registry schema evolution](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html):
  `BACKWARD`、`FORWARD`、`FULL` 和 transitive compatibility 的工程语义。
