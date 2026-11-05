---
id: ADR-0216
title: "The read-only HMS entry uses the upstream catalog crate and accepts read fallbacks"
domain: [crate-boundary, provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0174
    scope: "HMS H3 and H4 retention rows and their deletion conditions: both patches exit with the vendored HMS package, and the HMS consumer accepts read fallbacks instead of delivering an equivalent seam"
    link: "ADR-0174-vendored-iceberg-baseline-and-patch-policy.md"
  - id: ADR-0215
    scope: "HMS as a retained vendored package with registered divergences: HMS uses the unmodified registry release, and core is the only retained Iceberg vendor package"
    link: "ADR-0215-released-iceberg-package-provenance.md"
date: 2026-11-05
provenance:
  - "discussion: 2026-11-04 accepted the unmodified upstream HMS catalog crate with missing-table, metadata-read and listing fallbacks"
  - "reference: Trino source 45f9d30281d77ad0eb9196f0d27ee28b68039297 (nearest tag 483), TrinoHiveCatalog.listTables and IcebergMetadata.getRelationTypes"
code-anchors:
  - "Cargo.toml ([patch.crates-io])"
  - "novarocks/connector/iceberg/src/catalog/hive.rs (NovaRocksHiveCatalog::load_table_for_read)"
---

## 问题

HMS 只读兼容入口为何直接使用未修改的上游 catalog crate，上游缺少的读取能力（缺表类型、只列 Iceberg 表、按请求读取 metadata）由哪些可见回退代替？

## 背景与执行事实

Hive Metastore（HMS）是永久只读兼容入口（[ADR-0169](ADR-0169-read-only-hms-and-single-writer-admission.md)）：按名称读取外部引擎写入的 Iceberg 表、查询元数据表、做 branch/tag/snapshot 时间旅行；所有 mutation 在第一次外部副作用之前拒绝。Connector 的 `NovaRocksHiveCatalog` 包装上游 `HmsCatalog`，分工如下：

| Owner | 负责 | 不负责 |
|---|---|---|
| 上游 `HmsCatalog`（crates.io `iceberg-catalog-hms`） | Thrift 调用；用构造 catalog 时建立的 FileIO 读取 metadata；构造 `Table`；列出名称 | 缺表的类型化区分、条目类型、按请求读取 |
| Connector `NovaRocksHiveCatalog` | 写入拒绝（`admit_operation`）；请求上下文检查；每次 Thrift 调用前对所选 Metastore 服务做依赖准入（`admit_selected_io`）；错误映射（`map_read_error`）；列表上界与观测 | 补回上游缺少的能力 |
| FE 名称解析（`connector_table_resolution_error`） | 把 `NotFound` 呈现为 `unknown table`，其余错误按 provider 错误呈现 | 识别 HMS 的缺表 |
| `information_schema.tables`（`build_tables_batch`） | 把表枚举的每个条目标为 `BASE TABLE`；视图只经视图接口报告，而 HMS 的 `list_views` 回答不支持 | 区分 HMS 中的视图与非 Iceberg 表 |

上游 0.11.0 对三项能力的实际行为：

| 能力 | 上游行为 | 能否只用公开接口补回 |
|---|---|---|
| 缺表的类型化错误 | `load_table` 经 `from_thrift_exception` 把 Metastore 声明的全部异常（含 `NoSuchObjectException`）转为 `Unexpected`，正文为 `Operation failed for hitting thrift error`，异常放在 source 中；`table_exists` 把 `NoSuchObjectException` 映射为 `false` | 能：加载失败后再调用 `table_exists`（选项三） |
| 只列 Iceberg 表 | `list_tables` 调用一次 `get_all_tables`，返回库中全部名称；Thrift 客户端是私有字段，外部取不到条目类型 | 不能 |
| 按请求读取 metadata | `load_table` 用 catalog 自己的 FileIO 读 metadata，返回的 `Table` 也携带它；没有按调用传入 FileIO 的接口 | 不能 |

本裁决之前，vendored HMS 包靠三处本地分歧提供这些能力：H3 把加载时的 `NoSuchObjectException` 报为 `TableNotFound`；H4 按每批至多 100 个名字取回实体，只保留 `table_type` 为 `iceberg` 的条目；公开方法 `load_table_with_file_io` 用调用方的 FileIO 读 metadata。前两项登记在 [ADR-0174](ADR-0174-vendored-iceberg-baseline-and-patch-policy.md) 与包的来源清单中；第三项随请求级依赖事实（统一分布式意图执行 owner，PR #1266，[ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)）加入，没有登记用途与退出条件。三项都没有上游来源。

两种 FileIO 的区别决定了 metadata 读取的回退：

| FileIO | 构造 | 读取时 | 使用者 |
|---|---|---|---|
| 请求级 | `fs_io::build_file_io_for_location` 按本次请求的 `IcebergReadBinding` 构造 | 检查请求仍然有效、取存储准入、计入请求的依赖事实 | REST 与 Hadoop 的读路径 |
| catalog 级 | `catalog_runtime::build_hms_catalog` 构造 generation 时按 catalog binding 注入 | 没有请求上下文：存储准入直接放行、不检查请求取消、不计入依赖事实 | HMS 的全部 FE 侧 metadata 读取 |

`load_table_for_read` 的名字与参数容易误导：它接收请求的 `IcebergReadBinding`，但只用它检查请求上下文、在 Thrift 准入期间调用上游 `load_table`，不用它构造 FileIO。上游返回的 `Table` 按请求物化时原样返回，所以 FE 侧后续的 manifest list 与 manifest 读取（含元数据表与时间旅行规划）也走 catalog 级 FileIO。读取本身正常完成、结果正确；带请求上下文加载的表仍不进入 generation 缓存。BE 读数据文件使用执行侧 binding，不受影响。

错误分类沿已有映射传递：`Unexpected` 经 `map_read_error` 成为 `ConnectorErrorKind::Unavailable`，它在 SPI 中是可重试的瞬时类别（catalog materialization 把它归为 `Transient`）；没有 `metadata_location` 的条目（Hive 表、视图）由上游报 `DataInvalid`（`No 'metadata_location' set on table`），映射为 `CorruptData`。列表经 `ConnectorListingBound` 收集，条目数或名称字节超出上界时整次失败，不截断。

对照 Trino：其 Iceberg 连接器在 HMS 上同样列出库中全部名称（`TrinoHiveCatalog.listTables`），并从 Metastore 取得每个条目的关系类型（`IcebergMetadata.getRelationTypes`）。NovaRocks 经上游公开接口只拿到名称，所以名称与 Trino 一致，类型不一致。

## 考虑过的选项

**选项一：保留 vendored HMS 及其三处分歧。成本否决。** 三项能力不变。代价是每次协调重设基线都要重放补丁；未登记的方法要补登记用途、owner 与退出条件；path 包得不到 registry 的安全公告覆盖。HMS 只是只读兼容入口，这份持续维护成本高于三项能力的价值。否决依据是维护成本，不是行为更差。

**选项二：NovaRocks 自写 HMS 只读客户端。成本否决。** 自己发 Thrift 请求，可以保留全部三项能力，并能按条目类型准确区分 Hive 表与视图。代价是自有 Thrift 协议代码与 HMS 版本兼容都由 NovaRocks 维护，例如 Hive 4 的取表请求 `get_table_req`：上游 main 已在 apache/iceberg-rust #3187 中改用它，0.11.0 尚未包含，自有客户端须自行跟进。对只读兼容入口，这份复杂度不值得。

**选项三：加载失败后调用上游 `table_exists`，把缺表改判为 `NotFound`。成本否决。** 只用公开接口、约十行代码就能恢复 `unknown table`。代价是失败路径上多一次 Metastore 调用，并在 Connector 中为上游缺口维护补偿逻辑；收益只在错误呈现。接受缺表报为 `Unavailable`。

**选项四：拒绝 HMS 列表。设计否决。** 不再输出类型不准确的条目，但用户无法在 NovaRocks 中发现表名。名称发现是只读兼容入口读取外部表的基本能力，Trino 在同一 Metastore 上也列出全部名称；除非产品取消 HMS 的名称发现，否则不采用。

**选项五：直接依赖上游原包，接受三项回退。采用。** 无补丁、无自有协议代码，HMS 进入 registry 的公告与许可检查，Connector 的代码面最小；代价见「接受的妥协」。

## 裁决

开发规则：

1. **上游原包规则。** HMS 依赖 crates.io 的 `iceberg-catalog-hms`（0.11.0，checksum `b377f757ed01d7e08bd4dfc0dfdfecaf00d50576b8942c205a6565ac1b2df172`）。根 `[patch.crates-io]` 不含 HMS 条目，仓库不保留 HMS vendor 目录。registry 包依赖的 `iceberg` 仍经根 patch 解析到同一 vendored core，lock 中只有一个 iceberg 版本；ADR-0174 的同源家族规则不变。
2. **只用公开接口、不补偿规则。** Connector 的 HMS 路径只调用上游公开接口，不在 Connector 中重建已放弃的能力：加载失败后不调用 `table_exists` 改判缺表，不按名称或类型过滤列表，不为 HMS 构造请求级 FileIO。恢复任何一项能力，须按「何时重新评估」另立 ADR，不以补丁或补偿代码顺带恢复。
3. **准入归 Connector 规则。** 每个 HMS 读入口先检查请求上下文，再对所选 Metastore 服务做 Thrift 依赖准入，在准入期间调用上游；写入按 ADR-0169 在副作用之前拒绝。上游客户端不感知请求，Thrift 准入不能当作 metadata 读取已计入请求的证明。
4. **回退即当前行为规则。** 以下三项写入用户指南：
   - 缺表：上游 `Unexpected` 映射为 `Unavailable`，SQL 报 provider 错误，不报 `unknown table`。
   - metadata 读取：FE 侧读取 HMS 表的 metadata JSON、manifest list 与 manifest（含元数据表与时间旅行规划）使用 catalog 级 FileIO，不计入请求的存储准入、取消检查与依赖事实；Thrift 调用仍逐请求准入，BE 读数据文件不变。
   - 列表：返回 Metastore 库中的全部名称，与 Trino 列出的名称一致；`information_schema.tables` 把每个条目标为 `BASE TABLE`；列表上界按全部名称计数；非 Iceberg 条目在访问时以 `No 'metadata_location' set on table` 失败，归为 `CorruptData`。

   它们是接受的退化，不是产品承诺：不为它们新增断言或 fixture；列表断言只描述「列出库中全部名称」，不对条目类型作正确性声明。

调试规则：

5. **缺表先看 source。** HMS 读取报 `Unavailable`、正文为 `Operation failed for hitting thrift error` 时，先看错误来源：含 `NoSuchObjectException` 就是缺表，不是 Metastore 故障。
6. **`metadata_location` 错误表示非 Iceberg 条目。** `No 'metadata_location' set on table` 说明该条目是 Hive 表或视图，不是 Iceberg metadata 损坏。
7. **列表上界按全部名称计。** HMS 列表超出上界时，按库中全部名称（含 Hive 表与视图）判断，不按 Iceberg 表数判断。
8. **取消等在途读取。** HMS 查询取消后，已开始的 metadata 读取会继续到正常结束；这不是取消失效。

本 ADR 只部分替代两份旧 ADR：ADR-0174 中 HMS H3、H4 的保留行及其删除条件——这两项不是由消费者交付等价接缝后删除，而是由 HMS 消费者接受回退、随 vendor 目录退出；ADR-0215 中 HMS 作为保留分歧的 vendored 包这一部分。ADR-0174 的同源家族与 core 补丁的消费者退出责任，ADR-0215 的发布字节来源、源码与验证分离规则继续有效。

## 接受的妥协（诚实记录）

- 取舍的真实依据是维护成本：只读兼容入口不值得长期维护补丁或自有协议代码。回退后的行为并不比原来更好。
- 缺表不再是 SQL 名称解析错误。按错误类别重试或告警的调用方会把它当作瞬时故障，监控也可能把缺表计为服务不可用。
- HMS 的 metadata 读取不受请求约束：取消要等在途读取结束；workload 准入少记一项存储依赖；本裁决不另设补偿机制。HMS 的请求级 metadata 记账此前只由未登记的分歧提供，退出后没有替代。
- `information_schema.tables` 把 Hive 视图、Iceberg 视图与 Hive 表都标为 `BASE TABLE`；`SHOW TABLES` 会列出无法按 Iceberg 读取的名称。Hive 表很多的库可能触发列表上界而整次报错，此前只计 Iceberg 表。
- 非 Iceberg 条目的访问错误仍归为数据损坏，类别有误导性；本裁决不改变它。
- 回退行为没有专门测试；HMS 的端到端覆盖仍是 `iceberg-hms` 套件中的读取、元数据表、时间旅行与写入拒绝。
- 上游 0.11.0 用 `get_table` 取表；Hive 4 Metastore 的兼容不在本裁决内，随下一次协调重设基线进入。

## 何时重新评估

- **上游提供类型化缺表映射或按调用传入 FileIO 的接口**：随正常基线采用，恢复 `NotFound` 或请求级 metadata 读取，并更新用户指南；不需要补丁。
- **产品需要只列 Iceberg 表或准确的条目类型**：先推动上游暴露条目类型；上游不可行时重新讨论选项二。本条不把重新 vendor 作为恢复路径；再引入补丁须另立 ADR，说明上游与选项二为何都不可行。
- **HMS 列表上界在真实部署中成为障碍**：待上游或选项二能提供条目类型后过滤列表，或由列表上界的 owner 按其规则重新评估上界；不以截断代替报错。
- **缺表被当作服务不可用，在真实部署中造成重试或告警问题**：重新评估选项三，或推动上游的类型化映射。
- **HMS metadata 读取不受请求取消与准入约束，在真实部署中造成可观察的取消延迟或资源超额**：推动上游提供按调用传入 FileIO 的接口，并重新比较选项二。
- **下一次协调重设基线带来上游 Hive 4 兼容**：核对 HMS 读取与三项回退在新基线上是否仍成立；选项二的成本随之重新比较，因为自有客户端须自行跟进 Hive 版本兼容。
- **产品改变 HMS 的只读定位**：按 ADR-0169 另立 ADR，本条随之重审。
