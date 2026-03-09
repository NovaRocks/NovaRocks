---
name: novarocks-debug-tools
description: 在 NovaRocks 仓库中使用 `tools/src/bin` 下的 probe/inspect 工具排查 StarRocks lake metadata、bundle metadata、segment footer、txn log 和 parquet fallback 问题。适用于 schema change、mixed rowset schema、写入发布异常、bundle 元数据核对，以及用户提到 `bundle_meta_probe`、`starrocks_inspect`、`txn_log_probe`、`oss_parquet_probe`、`tools/bin` 或“tools 里的 probe 工具”时。
---

# NovaRocks Debug Tools

这个 skill 用来统一指导 `tools/src/bin/` 下几类调试工具的使用。

注意：

- 仓库里没有现成的 `tools/bin/` 目录，源码在 `tools/src/bin/`，编译产物默认在 `tools/target/debug/`。
- 本次我实际用到的是 `bundle_meta_probe`。其余几个工具属于同一类诊断工具，一并纳入这个 skill。

## 1. 先选工具

- `bundle_meta_probe`
  - 看 bundle metadata、`tablet_to_schema`、tablet rowset 列表、nested schema、历史 schema 形状。
  - 适合排查：schema change 后 old/new rowset 混读、bundle metadata 是否带对 schema、tablet 元数据是否符合预期。
- `starrocks_inspect`
  - 看 tablet snapshot、segment footer、列 unique id、并尝试构造 native read plan。
  - 适合排查：footer 结构、列 unique id、segment 偏移、read plan 构造失败。
  - 更适合本地 tablet root。
- `txn_log_probe`
  - 看一个 txn log 里的 `op_write` 摘要、rowset、del files、schema key。
  - 适合排查：写入发布、partial update、delete file、txn log 路径是否正确。
- `oss_parquet_probe`
  - 直接扫 OSS/S3 上的 parquet 文件，打印 parquet schema 和 Arrow schema。
  - 适合排查：parquet fallback、Arrow metadata、schema 识别差异。

## 2. 当前状态

当前仓库下，工具状态分两类：

- 已验证可直接编译运行
  - `bundle_meta_probe`
  - `txn_log_probe`
- 当前源码与主仓接口有漂移，使用前要先同步
  - `starrocks_inspect`
    - `tools/src/bin/starrocks_inspect.rs`
    - 典型漂移：`load_tablet_snapshot` 现在要求 `Option<&ObjectStoreProfile>`，`build_native_read_plan` 现在有第 4 个参数，文件里还有旧的 `starust::...` 路径。
  - `oss_parquet_probe`
    - `tools/src/bin/oss_parquet_probe.rs`
    - 典型漂移：旧的 `oss_config()` 访问路径已经不存在，需要改成显式构造 `ObjectStoreConfig` 或显式传 OSS 参数。

如果用户只是要快速排障，不要先修所有工具。优先选已经可用的工具；只有当问题明确落在 footer/read-plan/parquet fallback 时，再补修对应工具。

## 3. 编译方式

在仓库根目录执行：

```bash
cargo build --manifest-path tools/Cargo.toml --bin bundle_meta_probe
cargo build --manifest-path tools/Cargo.toml --bin txn_log_probe
```

编译后可直接运行：

```bash
tools/target/debug/bundle_meta_probe --help
tools/target/debug/txn_log_probe --help
```

如果只是一次性运行，也可以：

```bash
cargo run --manifest-path tools/Cargo.toml --bin bundle_meta_probe -- --help
cargo run --manifest-path tools/Cargo.toml --bin txn_log_probe -- --help
```

## 4. 推荐工作流

遇到 lake/schema-change 类问题时，默认按下面顺序：

1. 先用 `bundle_meta_probe` 看 bundle 里 tablet、rowset、schema 映射是不是对的。
2. 如果怀疑读路径或 footer unique id，再看 `starrocks_inspect`。
3. 如果是写入/发布链路问题，再看 `txn_log_probe`。
4. 如果怀疑写入退化到了 parquet，或 Arrow 解释 parquet schema 有差异，再看 `oss_parquet_probe`。

## 5. `bundle_meta_probe`

源码：`tools/src/bin/bundle_meta_probe.rs`

用途：

- 看 bundle 文件里有哪些 tablet
- 看 `tablet_to_schema`
- 看 rowset 列表、segment 名、`del_files`
- 看 root schema 和 nested children

支持：

- 本地路径
- `s3://` / `oss://` 路径

常用参数：

- `--root <path>`
- `--version <v>`
- `--tablet-id <id>`，可重复
- `--fs-option <k=v>`，可重复

对象存储常用 `fs-option`：

- `fs.s3a.endpoint`
- `fs.s3a.access.key`
- `fs.s3a.secret.key`
- `fs.s3a.endpoint.region`
- `fs.s3a.path.style.access`

本地示例：

```bash
tools/target/debug/bundle_meta_probe \
  --root /tmp/tablet_root \
  --version 2 \
  --tablet-id 62988
```

S3/OSS 示例：

```bash
tools/target/debug/bundle_meta_probe \
  --root s3://bucket/root/db62983/62985/62987 \
  --version 2 \
  --tablet-id 62988 \
  --fs-option fs.s3a.endpoint=127.0.0.1:9000 \
  --fs-option fs.s3a.access.key=xxx \
  --fs-option fs.s3a.secret.key=yyy \
  --fs-option fs.s3a.path.style.access=true
```

读输出时重点看：

- `bundle tablet_ids=[...]`
- `tablet_to_schema tablet_id=... schema_id=...`
- `rowset id=... segments=... version=...`
- `schema id=... root_columns=...`
- nested `col name=... unique_id=... type=... children=...`

适合回答的问题：

- 一个 tablet 的 old rowset / new rowset 是否挂到了不同 schema id
- bundle 里有没有历史 schema
- nested struct/array 字段名和 child 数量是否符合预期

## 6. `txn_log_probe`

源码：`tools/src/bin/txn_log_probe.rs`

用途：

- 读取一个原始 txn log protobuf 文件
- 输出 `op_write`、rowset、`dels`、`del_files`

限制：

- 期待输入的是“解包后的原始 `.log` payload”
- 不是 MinIO `xl.meta` 包裹层

常用参数：

- `--root <path>`
- `--tablet-id <id>`
- `--txn-id <id>`
- `--load-id-hi <id>`
- `--load-id-lo <id>`

普通 txn log：

```bash
tools/target/debug/txn_log_probe \
  --root /tmp/tablet_root \
  --tablet-id 10035 \
  --txn-id 20001
```

带 load id 的 txn log：

```bash
tools/target/debug/txn_log_probe \
  --root /tmp/tablet_root \
  --tablet-id 10035 \
  --txn-id 20001 \
  --load-id-hi 123 \
  --load-id-lo 456
```

读输出时重点看：

- `schema_key`
- `dels`
- `rowset segments=...`
- `rowset.segments=[...]`
- `rowset.del_files=[...]`

适合回答的问题：

- 这次 publish 到底写了哪些 segment
- 是否生成了 del file
- `op_write` 有没有落 rowset
- load-id 路径是不是拼对了

## 7. `starrocks_inspect`

源码：`tools/src/bin/starrocks_inspect.rs`

定位：

- 这是“snapshot + footer + read plan”的综合检查工具。
- 如果它已经同步到当前接口，适合做 bundle/segment/footer/read-plan 一把梭检查。

当前状态：

- 当前源码未同步到主仓接口，直接 `cargo build --bin starrocks_inspect` 会失败。
- 若要修，优先改这几个点：
  - 把旧的 `starust::...` 路径改成 `novarocks::...`
  - 同步 `load_tablet_snapshot(..., object_store_profile)`
  - 同步 `build_native_read_plan(..., source_tablet_schema)`

理想用法（修完后）：

```bash
cargo run --manifest-path tools/Cargo.toml --bin starrocks_inspect -- \
  --tablet-id 10035 \
  --version 4 \
  --root /tmp/starrocks_tablet \
  --columns c1,c2,c3
```

适合回答的问题：

- snapshot 里有哪些 segment
- footer 中每列的 `unique_id/logical_type/encoding`
- 某几个输出列能不能成功构造 read plan

## 8. `oss_parquet_probe`

源码：`tools/src/bin/oss_parquet_probe.rs`

定位：

- 这是 parquet fallback 诊断工具。
- 用来比较 parquet 文件元数据、Arrow schema、跳过 Arrow metadata 后的 schema。

当前状态：

- 当前源码依赖的旧 `oss_config()` 接口已经不存在，直接编译会失败。
- 若要修，建议改成“命令行显式传对象存储参数，然后构造 `ObjectStoreConfig`”。

理想用法（修完后）：

```bash
cargo run --manifest-path tools/Cargo.toml --bin oss_parquet_probe -- \
  --prefix db1/table1 \
  --max-files 3
```

适合回答的问题：

- parquet 文件自身 schema 是什么
- Arrow 读取时有没有因为 metadata 导致 schema 变化
- parquet fallback 文件是否可被当前 reader 正确识别

## 9. 使用原则

- 如果问题是 schema change / mixed rowset schema，先上 `bundle_meta_probe`。
- 如果问题是写路径或发布结果，先上 `txn_log_probe`。
- 不要一开始就修 `starrocks_inspect` 和 `oss_parquet_probe`，除非问题已经明确落在那两类场景。
- 如果用户只给了一个 S3 tablet root，又要看 schema 形状，优先 `bundle_meta_probe`，不要先写临时脚本。
