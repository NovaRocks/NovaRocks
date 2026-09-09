---
id: ADR-0138
title: "Vendor Paimon 0.3.0 only for bounded authorized read hooks"
domain: [provider-spi, crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-09-08
provenance:
  - "discussion: 2026-09-08 Paimon read-only SDK integration decision"
  - "PR: pending — backfill the number once the Paimon connector merges"
code-anchors:
  - "Cargo.toml ([patch.crates-io] paimon)"
  - "vendor/paimon-0.3.0/PATCH.md (NovaRocks read-host patch)"
  - "novarocks/connector/paimon/src/role_binding.rs (PaimonRoleFileIoFactory)"
---

## 问题

NovaRocks 首期读取 Paimon 时，为什么维护 `paimon` 0.3.0 的可追溯 vendored patch，而不是直接依赖 crates.io 包、让 SDK 自己创建存储客户端，或在 SDK 外包一层资源检查？

## 背景与执行事实

首期产品边界是 Filesystem Catalog、共享 S3/MinIO、追加表与 `deduplicate` 主键表的当前快照读取。NovaRocks 的 Server 拥有 role-local secret、authorized object-store access、request deadline、cancellation 与 fragment/query/process memory ledger；provider 和 SDK 不得从环境变量、catalog secret 或默认 storage feature 构造第二条 I/O 路径。

冻结 snapshot 只是读取一致性事实，不是外部保留权。只读 host patch 不写 tag、consumer state 或 retention lease；Paimon 外部 GC/expiration 必须保证 snapshot、schema、manifest 与数据文件至少存活到最长 NovaRocks 查询结束。若外部系统提前清理必要对象，reader 必须保留对象缺失错误并终止，不能切换到 latest、返回部分关系或把缺失解释为空表。

crates.io 的 `paimon` 0.3.0 能读取目标文件格式，但其公开 API 缺少 NovaRocks 需要的四类接缝：注入 storage-client-neutral 的只读 FileIO；在 metadata、Parquet 和 PK merge 内部执行 request cancellation/checkpoint；对 retained bytes 使用随 owner 生命周期移动的 reservation；在 merge 前严格验证历史物理 schema 与 KV system columns。其 `BinaryTableStats` 也未公开，provider 无法从私有 wire 无损重建完整 `DataFileMeta`。

只在 SDK 调用前后检查请求无法约束调用内部的 listing collection、whole/range bytes、Avro 解压、Parquet decode、同 key history、cursor batch 和输出构造。只包装 SDK 返回的 Arrow batch则会漏计还未 yield 的内部状态，并在长时间无输出的 delete/merge 路径中无法及时取消。让 SDK 通过自己的 OpenDAL storage feature 访问 S3，又会绕开 NovaRocks 的 access domain、credential generation 和审计边界。

当前副本精确来自 crates.io `paimon` 0.3.0，crate archive SHA-256 为 `525d11131f96bd6fe858c39b1a662179fee701e49fee92dad273445c56e936dc`，对应上游 git revision `7b54d44c487590f4d84952533f110ba16e9346a4`。根 manifest 仍声明精确 `=0.3.0`、关闭 default features，并以 `[patch.crates-io]` 指向该副本。

## 考虑过的选项

**A. 直接使用 crates.io 0.3.0，不改 SDK。** 供应链最简单，也不承担私有 patch，但无法把授权 I/O、内部 retained memory 和 merge 循环取消完整交给 NovaRocks host。SDK 外层无法观察所有真实 owner，因此否决。

**B. 启用 SDK 的 S3/OpenDAL storage feature。** 改动最少，SDK 可以自行读取 warehouse，但 credential、endpoint、retry 和 client 生命周期会出现第二个 owner，并绕过 `FsAccessHandle` 的 scope 与 request cancellation。NovaRocks FS 与该 SDK 还解析到不同 OpenDAL 版本，直接交换 Operator 也会把实现类型泄漏过边界，因此否决。

**C. 只在 provider 外围增加 semaphore、timeout 和 batch memory sampling。** 可以限制并发并统计已输出 batch，却覆盖不了 metadata collection、压缩解码、same-key history 和无输出 merge；超限时资源已经分配。这个方案会把不完整的观测冒充 admission，因此否决。

**D. 等待上游发布全部接缝。** 长期维护成本最低，但当前没有包含所需 API 与严格校验的已发布版本，会使第二个真实 provider 的实现无限期依赖外部时间表，因此本期不选。

**E（选中）. 精确 vendor 0.3.0，并维护最小只读 host-control patch。** 增加 `ReadOnlyFileIO`、`ReadControl`、reservation/checkpoint、严格物理输入验证和必要只读 visibility；生产关闭 SDK storage features，所有读 I/O 都经 NovaRocks 授权 handle。

## 裁决

根 workspace 精确声明 `paimon = "=0.3.0"` 且 `default-features = false`，并仅通过 `[patch.crates-io] paimon = { path = "vendor/paimon-0.3.0" }` 使用记录了 archive checksum 与上游 revision 的副本。`PATCH.md` 必须说明每个修改接缝、来源身份和退出条件；不得顺便升级 SDK、重写合法 winner 算法或启用生产 storage backend。

Patch 增加 storage-neutral `ReadOnlyFileIO` 和 `ReadControl`。Paimon provider 从 Server 的 exact role-local credential binding 建立 warehouse-bounded `FsAccessHandle`，再注入 SDK；list/read/range 以及所有 SDK mutation entry 在 host boundary 明确区分，写操作在调用外部存储前拒绝。SDK 内部的 metadata、Avro、Parquet、cursor、same-key history、row materialization 和 yielded batch reservation 随真实 owner 转移，并在成功、错误、取消与 Drop 路径释放。

合法历史 schema 中不存在的 nullable field 可以按 Paimon 规则补 NULL；历史 schema 声明存在却在物理 batch 缺失或类型不符的 field 必须报损坏。主键读取的 `_SEQUENCE_NUMBER` 与 `_VALUE_KIND` 必须具有准确非空类型和值域，并在 merge 前验证。`BinaryTableStats` 只提升必要 visibility，让 provider 私有 codec 无损重建 SDK `DataFileMeta`；它不成为 SPI 或公共 Native 类型。

NovaRocks 只承诺该补丁覆盖首期只读路径。外部 DDL、DML、maintenance、statistics publication、SDK cache/prefetch 和其它 merge engine 不因 vendoring 获得支持；对应 capability 保持缺席或明确 `Unsupported`。

部署文档必须把外部 retention 前提作为 Paimon 可用性条件，并说明 NovaRocks 没有写侧保留协议。任何需要在外部 GC 并发下保证长查询存活的方案，都必须先为 Paimon 定义可验证的 retention/lease owner，不能通过 reader fallback 放宽冻结快照语义。

## 接受的妥协（诚实记录）

**NovaRocks 暂时承担一个 SDK 私有维护分支。** 安全修复、上游同步、Rust/Arrow 版本升级和冲突处理都需要同时审查原包与本地 patch。vendoring 是为取得当前不可外包的内部控制点而接受的成本，不表示私有副本优于 registry 依赖。

**仓库与构建输入变大。** 完整 crate 源码进入仓库，source review、license 检查和 diff 噪声高于普通 crates.io 依赖。精确 checksum、上游 revision、最小 patch 说明和依赖图守卫用于让成本可见，不能消除它。

**Patch 与 0.3.0 内部结构耦合。** retained owner 或 merge pipeline 的上游重构可能使补丁难以移植；编译通过不足以证明资源语义仍正确，每次同步都必须重新运行低预算、取消、损坏输入和真实跨引擎 fixture。

**首期支持面保持较窄。** 为了能完整审计读取 correctness，只支持 Parquet、明确 codec/type/merge-engine 集合和静态 S3 credential。更宽的上游能力会被拒绝，这会使部分合法 Paimon 表暂时不可读。

## 何时重新评估

- 上游发布版本提供等价的 authorized read-only FileIO 注入、request checkpoint、retained reservation 和严格物理输入验证，并能通过 NovaRocks 的资源、取消、损坏输入与跨引擎用例时，删除 `[patch.crates-io]` 和 vendor 副本；
- 上游愿意接收这些通用接缝时，优先将 additive API 回馈并跟踪发布版，避免本地 patch 长期分叉；
- `paimon` 出现安全公告、许可证变化、Rust/Arrow/Parquet 不兼容或维护停滞时，立即重新评估版本或替代 SDK，而不是因已 vendor 而冻结；
- 产品要支持写入、其它 catalog、其它 merge engine、cache/prefetch 或 vended credential 时，重新审查完整 effect、恢复、资源和权限边界；不得把当前只读 patch 直接视为授权；
- 可复现证据表明 patch 的维护或构建成本超过直接实现最小标准读取器，并且替代实现能证明同等格式兼容与跨引擎 correctness 时，再比较替代 SDK；不能只按代码量决定。
