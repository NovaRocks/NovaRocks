---
id: ADR-0118
title: "Iceberg catalog semantics live behind one provider-private catalog owner with operation-shaped admission"
domain: [provider-spi, crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0171
    scope: "Internal reuse of vendored transaction/action preparation; catalog owner and late TableCommit carrier remain effective"
    link: "ADR-0171-commit-operation-model.md"
date: 2026-08-26
provenance:
  - "discussion: 2026-08-26 Trino 风格 Iceberg Catalog 与操作型准入"
  - "PR: <backfill after merge>"
code-anchors:
  - "novarocks/connector/iceberg/src/catalog/mod.rs (NovaRocksCatalog)"
  - "novarocks/connector/iceberg/src/catalog/factory.rs (NovaRocksCatalogFactory)"
  - "novarocks/connector/iceberg/src/catalog/transaction.rs (Transaction)"
---

## 问题

「这个 Iceberg Catalog 支持这次操作吗」由谁回答、在哪一刻回答？是由一张脱离请求的能力表回答，
还是由具体 Catalog 实现针对这一次具体 request 回答？

## 背景与执行事实

Iceberg provider 此前没有等价于 Trino `TrinoCatalog` 的语义边界。一个 control generation 同时持有
generic `Catalog`、optional REST concrete client 与 optional Hadoop concrete client
（`IcebergCatalogClient`），运行时容器再保存同一组 bag 并向所有 operation family 暴露
`catalog()`/`rest_catalog()`/`hadoop_catalog()`。由此「支持」这件事同时有四个互不协调的来源：

- factory 按 `rest_catalog().is_some()` 决定是否安装 staged-create 与 unanchored cleanup slot，于是
  **slot presence 被当成 Catalog 能力事实**；
- catalog mutation owner 内部显式检查 `IcebergCatalogKind::Hadoop` 并旁路 generic path；
- DML publication 路径经 `uses_remote_catalog()` 做第二条 kind 分支（`ensure_hadoop_registration`
  每次 Hadoop 写入都 best-effort `create_namespace` 后 `register_table`，其中 namespace 错误被静默吞掉）；
- 其余情况一路跑到具体 helper 才报错。

更关键的是，**支持与否本来就不是一个脱离 request 的布尔量**：同一个 Hadoop Catalog 能原子创建空表
（ADR-0077 的 storage 条件创建 v1 metadata 线性化点），却不能满足标准 REST staged CTAS；同一个 REST
Catalog 能创建表，却在缺少可枚举 warehouse staging root 时不能安全完成 CTAS。`supports_create = true`
或 `supports_stage_create = false` 都无法表达真实操作。

同期还有两个具体的诚实性缺口：

- 非 REST Catalog 的 `view_exists` 返回 `false`、`list_views` 返回空集合，把「答不上来」伪装成
  「authoritative 没有」；
- vendored `iceberg::Transaction::commit` 只返回普通 `Result<Table>`，并按 retryable error 自动重试。
  它无法表达请求是否可能已经发出，因此不能直接作为 ADR-0110（lake publication crash-only contract） 所要求的 publication frontier。

ADR-0110 已把 lake publication 冻结为 `KnownUncommitted` / `CommitUnknown` / `KnownCommitted` 三态、
target exact OCC 与年龄窗 GC，并要求「不能证明所需路径的 provider 在零副作用点返回 typed `Unsupported`」。
ADR-0110 当时把不同 Catalog 的落地工作概括为「探测并缓存 capability matrix」——本 ADR 取代这句措辞，
但不改变它的三态与 GC 裁决。ADR-0094 已在真实 owner 收敛后删除空的 `novarocks-catalog` crate。

## 考虑过的选项

**A. 探测并缓存 capability matrix。** 启动时对 Catalog 做能力探测，把结果缓存成布尔表，调用方查表。
优势是形态简单、调用方改动小。代价是它从根上答错了问题：探测发生在没有 request 的时刻，而支持与否依赖
request 本身（目标是否已存在、有没有 explicit warehouse、intent 是空表还是 CTAS）。缓存还会漂移——
服务端配置变化后表仍然为真。这条路会把一个「按请求裁决」的问题永久固化成「按进程裁决」。

**B. 保留 kind 分支，只做命名整理。** 只把 `IcebergControlProvider` 等改名成 Trino 风格，不动 dispatch。
成本最低。但四个能力事实来源一个都不会消失，失败时机漂移（先跑 source / 建 staging / dispatch writer，
再在 helper 里发现不支持）依然违反 ADR-0110 的零副作用 fail-fast 顺序。这是把问题改了个名字。

**C. 直接把 vendored `iceberg::Catalog` 和 `iceberg::Transaction` 暴露给各 operation family。** 代码最少。
但 vendored commit 的自动重试会在 dispatch certainty 未知时重发同一请求，直接违反 ADR-0110 中
「一旦请求可能已发出，内核不得 retry」这一条。要修就得改 vendor 的重试策略——那等于用伪造的强结果
掩盖观察能力的缺失。否决。

**D. 把新接口提升为跨 crate 的通用 catalog domain（复活 `novarocks-catalog`）。** 表面上更「通用」。
但当前只有 Iceberg 一个 provider 需要这个形状：StarRocks 是只读外部 Connector，不拥有 catalog mutation 或
publication frontier。ADR-0094 删除空 catalog crate 的理由在此仍然成立——没有第二个 owner 时，抽出去的
crate 是一个没有 domain 的 facade，只会把 Iceberg 私有语义伪装成平台契约。否决。

**E（选中）. provider-private 宽接口 + operation-shaped admission + 统一 Transaction。** 对照 Trino
`TrinoCatalog`/`IcebergMetadata` 的分层与 `newTransaction` / `newCreateTableTransaction` /
`newCreateOrReplaceTableTransaction` 三构造方法的命名，但**不复制**其较弱的失败语义：Trino 的 generic
exception、backend-specific stage-create fallback 与未闭合的 rollback cleanup 都不满足 ADR-0110 的强三态。

## 裁决

- 在 `novarocks-connector-iceberg` 内建立 provider-private 的宽接口 `NovaRocksCatalog`，作为 Iceberg
  metadata/catalog 语义的**唯一** concrete dispatch boundary。它不进入 SPI、Core、Frontend 或 native wire。
- `NovaRocksCatalogFactory` 为一个 exact Connector control generation 创建且只创建一个
  `NovaRocksRestCatalog`、`NovaRocksHiveCatalog` 或 `NovaRocksHadoopCatalog`。`IcebergCatalogKind`
  只作为 validated configuration 到 concrete implementation 的 factory 输入；**factory 之后不得再出现
  concrete kind 分支**，包括 `uses_remote_catalog()` 这类间接形式。
- **Operation admission 由「能否为这次具体 request 构造 transaction 或执行一个 direct method」承担**，
  取代独立、可漂移的 boolean capability authority。create request 必须显式携带 operation intent，至少区分
  empty table 与 CTAS：Hadoop 接受 empty-create 而对 CTAS 在副作用前 `Unsupported`；REST 只有同时能证明
  标准 stage-create、absent-target commit 与 exact unanchored staging root 时才接受 CTAS。
- 允许一个**与构造器同源**的准入询问：`admit_create(intent)`。它不是能力表——它回答的就是
  `new_create_table_transaction` 会做的那个裁决，只是让必须在构建表定义之前拒绝的调用方能够问到
  （CTAS 必须在 source 执行前被拦下，先把定义建好等于为一个注定失败的请求白做工）。构造器自身调用它，
  所以两个答案不可能漂移。
- 采用 `new_transaction` / `new_create_table_transaction` / `new_create_or_replace_table_transaction`
  三个构造方法，统一返回一个 provider-private `Transaction`。不公开 `TableTransaction` /
  `CreateTableTransaction` 等平行类型；内部可用 private state 表达 existing-table、empty-create、
  REST staged-create 与 create-or-replace，但这些 variant 不得泄漏给 `IcebergMetadata`。
- provider-private `Transaction` 可以内部复用 vendored transaction/action/table commit，但**不得把裸
  `iceberg::Transaction::commit` 作为 publication boundary 暴露**。commit 必须是单次 dispatch 并返回
  ADR-0110 三态；`abort()` 只在 publication dispatch 前允许；`CommitUnknown` 之后 mutation authority
  永久关闭，`reconcile()` 只做只读 exact-positive 裁定。
- `Unsupported` 只能是**已知零副作用**的 admission 结果。允许 concrete Catalog 方法自己报告
  `Unsupported`；禁止的是先产生 external side effect 再发现不支持。constructor 可以做只读 discovery，
  但一旦 mutation request、staged create 或 writer effect 可能发生，后续结果只能是三态，不能降回
  `Unsupported`。
- **Catalog visibility 与 FileSystem/GC authority 分离**：`NovaRocksCatalog` 拥有 namespace/table/view/ref
  的 catalog 可见性、metadata condition 与 exact outcome；`IcebergFileSystemFactory` 拥有授权对象访问、
  exact object identity 与删除原语；`IcebergMetadata` 是两者的组合 owner，但**二者不构成原子事务**。
  `DROP TABLE ... PURGE` 因此固定为：先冻结 exact object identity → Catalog drop 拿三态 →
  只有 `KnownCommitted` 才把旧 object/ref 交给 ADR-0110 的年龄窗 cleanup → `CommitUnknown` 禁止任何
  文件删除 → cleanup 失败只是 finalization/GC 状态，不把已知 committed 回写成 uncommitted。
- **文件系统 Catalog 的边界细节**：对 Hadoop 这类 filesystem catalog，「catalog 条目」本身就是存储对象——
  表是否存在通过 `version-hint.text`、其次通过规范的 `v1.metadata.json` 解析。因此删除这两个指针**是**
  catalog 操作，与 ADR-0077「写 `v1.metadata.json` 使表存在」对称；数据文件与被取代的历史 metadata 才是
  对象，走年龄窗 handoff。顺序固定为先删 hint 再删 `v1.metadata.json`：两步之间失败时表仍可经 v1 解析、
  hint 下次读取自我修复，即「drop 未提交」，重试安全；移除 `v1.metadata.json` 是提交点。
- 读取结果保留 typed `NotFound` / `Unsupported` / `Unavailable` 的区别。**不能回答 view 枚举的
  concrete Catalog 必须返回 `Unsupported`，不得返回 `false` 或空集合伪装事实**；调用方不得把该结果
  在上层翻译回「无 view」。

## 接受的妥协（诚实记录）

- **接口显著变宽，弱 Catalog 要显式写大量 `Unsupported` 方法。** 这是换取单一语义 owner 与 Trino
  可比性的直接成本。我们接受它，因为替代方案（选项 A/B）省下的正是「谁拥有这个事实」这条边界本身。
- **包装 transaction 是纯增量代码。** 它不带来任何新功能，唯一产出是 dispatch certainty 的可观察性。
  如果 vendored iceberg 将来提供能区分「未发出 / 可能已发出 / 确认提交」的 commit API，这层包装的大部分
  就是冗余的。现在写它，是因为 ADR-0110 的三态没有它就无法在 Iceberg 侧成立。
- **view 枚举的诚实化是一次真实的用户可见回归，不是纯粹的正确性收紧。** Hadoop/Hive 上目标 namespace
  存在时的 `DROP DATABASE ... FORCE` 与 view 列举，从静默成功变成报 `Unsupported`。此前那个空集合是
  **有意**加的，注释写明就是为了不让 DROP DATABASE FORCE 被一个用户没问过的 catalog kind 变成硬错误。
  我们推翻它，代价是已知打到 `tests/sql/correctness/complex-type` 的 13 个用例（它们在默认
  `iceberg_catalog_type = "hadoop"` 上以 FORCE 收尾），这些用例的 teardown 随本次改动一并改写。
- **我们有意不区分「结构上存不了 view」与「答不上来」。** Hadoop catalog 确实无法承载 view，因此
  「零个 view」在那里其实是真事实，返回 authoritative empty 在技术上说得通。我们仍然选择一律
  `Unsupported`，因为一旦允许 provider 声明「我确定没有」，就等于重新引入了一个脱离 request 的能力断言——
  而那正是本 ADR 要消除的东西。这是为了边界纯度而接受更差的即时人机工程。
- **一次性命名 hard cut 的 diff 很大。** 不保留 alias / 双 factory / feature flag 会显著增大单个 PR。
  选择这样做不是因为大 PR 更好，而是因为长期双命名的认知税和后续对照 Trino 的迁移税更贵。
- **Catalog 与 FileSystem 不是原子事务，purge 与 GC 可能延迟或泄漏。** 面对 unknown 选择不删是
  ADR-0110 已接受的安全取舍，本 ADR 只是把它落到 owner 边界上，没有改善它。
- **本裁决收紧了 Hadoop DROP TABLE 的既有行为，代价是它不再立即回收空间。** 此前 Hadoop catalog 的
  `drop_table` 会对整个表目录做递归前缀删除。那个行为同时错了四处：忽略 caller 的 data disposition
  （要求保留数据的 drop 照样销毁数据）、不给并发读者任何窗口（刚解析完该表的读者在扫描中途丢文件）、
  按词法前缀而非表的精确对象身份匹配、并且吞掉自身失败（部分删除仍报成功）。改为只删 catalog 指针后，
  空间回收要等到年龄窗到期的清理 pass，磁盘占用因此会在一段时间内高于从前。这是用回收延迟换正确性，
  我们认为值得；但它确实是可观测的行为退化，不应被描述为纯粹的改进。

## 实现期修订（合入前）

本 ADR 在实现期收到四处修订，均在合入前完成，因此直接写入正文而非另立 ADR：

1. **文件系统 Catalog 的边界细节**（见「裁决」）：删除 Hadoop 的递归前缀删除后表重建失败，暴露出
   filesystem catalog 的 catalog 条目本身就是存储对象。这不是立场变化，是把裁决说准。
2. **view 一律 `Unsupported` 的代价被低估了**：实施后发现 NovaRocks 当时**没有任何枚举表的手段**
   （解析器无 `SHOW TABLES`，`information_schema` 只有 `schemata`），因此 `DROP DATABASE ... FORCE`
   是删非空库的唯一途径——「改用显式删表」对不预先知道表名的人不可达，SQL 测试 runner 的通用用例隔离
   机制就是活证人。裁决保持不变（仍然一律 `Unsupported`），但同时补上 `information_schema.tables`
   把能力洞堵上。这条正是下面「何时重新评估」第六项在实现期就触发的结果。
3. **publication receipt 的形状不是设计问题**：实现期一度把「`Transaction::commit` 只返回一个
   `CommitProof`，而各 operation family 需要形状不同的 receipt」当成阻塞裁决的设计问题。它不是。
   `CommitProof` 采用**扁平的可选字段**（`table_uuid` / `metadata_location` / `metadata_digest` /
   `snapshot_id`），receipt 的形状由 proof 实际携带了什么决定，而不是由「这是哪种 catalog」决定——
   后者恰好是本 ADR 要消除的那个提问方式。带 metadata identity 的 proof 得到可按 metadata 身份对账的
   receipt，不带的仍然报告 create、只是无法这样对账。`AdmissionFacts` 对准入所观察到的事实做同样处理，
   使调用方能在 dispatch 之前冻结 publication evidence。CREATE TABLE 因此对**所有** catalog 都走
   create-table transaction，工厂之外再无 `IcebergCatalogKind` 分派。
4. **「唯一语义 owner」在读路径上一度是空话**：owner trait 建好后，生产的 catalog 读取仍然经
   `vendored_client()` 直接调用通用 client，owner 自己的 `load_table` / `list_tables` /
   `table_exists` / `namespace_exists` / `list_views` / `load_view` 以及两个 namespace mutation
   全都没有生产调用者。这个事实**被模块级 `#![allow(dead_code)]` 掩盖了**——去掉它才暴露出来。已全部
   改经 owner；读路径的包装代码反而更短（owner 已经做了各调用点各自重复的排序、去重与 bookkeeping
   namespace 过滤）。教训写在这里：owner 的边界不是由 trait 是否存在证明的，而是由「绕过它的路径是否
   为零」证明的，而 blanket dead-code 允许会让这个证据消失。

   同期删除的两条「第二路径」都已经真的分叉了：`NovaRocksCatalogFactory::build` 与 `adopt` 各自做一遍
   kind 分派，且 `build` 包的是与 `adopt` 不同的 Hive client，还会**再建一个 client**——正是 `adopt`
   的注释所说、本分支已经修过一次的 two-clients-one-lake 故障；`create_table` 与
   `execute_create_table` 并存，导致 `CREATE TABLE IF NOT EXISTS` 的 no-op 语义在统一到 transaction
   时被静默丢掉（由套件用例抓到）。**没有调用者的并行路径不是「预留」，它是会分叉的负债。**

## 何时重新评估

- 出现第二个真正需要同样 owner 形状的 provider（拥有自己的 catalog mutation 与 publication frontier）——
  那时才重新考虑把 `NovaRocksCatalog` 提升为跨 crate 契约，并重新审视 ADR-0094 的结论；
- vendored iceberg（或其替代）提供能观察 dispatch certainty 的 commit API——那时统一 `Transaction`
  的包装层可以大幅收窄；
- Hive Metastore 或 Hadoop catalog 上游获得标准 staged create / view 支持——那时对应的 `Unsupported`
  面收缩，`NovaRocksHiveCatalog` / `NovaRocksHadoopCatalog` 的契约需要重写；
- `Unsupported` 的覆盖面大到用户无法预期哪些语句可用——那时需要的是**只读的**诊断能力展示（明确不参与
  admission），而不是把布尔能力表请回来；
- unanchored staging 或 drop-purge 的泄漏率超出运维可接受范围——那时重新评估的是 GC 触发方式，
  而不是放宽「unknown 时不删」这条；
- 若实践证明「一律 `Unsupported`」造成的运维摩擦大于它买到的边界纯度（例如 DROP DATABASE FORCE 的失败
  被反复绕过），则重新评估是否引入 authoritative-empty 与 unanswerable 的类型区分。
