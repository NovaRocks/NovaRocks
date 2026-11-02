---
id: ADR-0170
title: "Third-party crates are bounded through public configuration, not forked for resource accounting"
domain: [crate-boundary, memory-governance]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0204
    scope: "Patch allowlist: remove paimon while its provider is suspended; no accounting forks and registry identity rules remain active"
    link: "ADR-0204-upstream-iceberg-backports-and-provider-suspension.md"
  - id: ADR-0174
    scope: "Patch allowlist: add iceberg-property-macro; current Iceberg baseline and vendor code anchors"
date: 2026-10-06
provenance:
  - "discussion: 2026-10-06 rejection of a vendored Tokio/Hyper/H2/Tonic/Tower/HTTP/Bytes/Arrow stack for bounded result delivery"
  - "discussion: 2026-10-08 accepted MEM-1-M07 revision 7, trusted catalog SDK listing growth with three narrow protocol changes (D15/D16)"
  - "discussion: 2026-10-10 accepted MEM-1-M07 revision 8, retain existing Iceberg serialization without adding APIs (D17)"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1173 (first implementation merged; follow-up verification remains in progress)"
code-anchors:
  - "Cargo.toml ([patch.crates-io])"
  - "novarocks/native-adapter/src/native_transport_admission.rs (NativeTransportAdmission)"
  - "novarocks/worker/src/guarded_bytes.rs (bytes_with_exit_guard)"
  - "novarocks/execution/src/exec/chunk/root_array_storage.rs (ARROW_BUFFER_OWNER_METADATA_BOUND)"
  - "novarocks/connector/iceberg/src/catalog/listing_admission.rs (ListingAdmission)"
  - "novarocks/fs/src/list_body_limit.rs (ListBodyLimitFetch)"
  - "novarocks/connector/iceberg/src/metadata.rs (frozen_copy_on_write_source_payload)"
  - "vendor/iceberg-0.11.0/src/spec/table_metadata.rs (TableMetadata::serialize)"
  - "vendor/iceberg-catalog-rest-0.11.0/PATCH.md (single-page REST protocol extension and upstream exit)"
---

## 问题

需要对第三方库（Tokio、Hyper、H2、Tonic、Tower、HTTP、Bytes、Arrow 等）内部的内存或任务做上界控制时，为什么不 vendor/fork 它们来取得内部钩子，而是只用公开配置、库外准入和测量？

## 背景与执行事实

NovaRocks 的内存治理分两类对象，保证程度不同：

| 对象 | 例子 | 保证方式 | 退出口径 |
|---|---|---|---|
| NovaRocks 自有对象 | 结果窗口段、行游标、collector、交给传输层的 payload `Bytes`、自建队列 | 事前精确授权 | 最后一个 NovaRocks owner 被 Drop（payload 用上游 `Bytes::from_owner` 观察第三方持有的最后 alias） |
| 第三方内部对象 | HPACK 表、帧缓冲、Hyper/Tonic 任务、Tokio socket 注册与 TaskCell、Tower Buffer 内部、错误 Box | 公开配置限定数量和单项尺寸；NovaRocks 在库外持有计数门；字节为结构上界，用 jemalloc 测量验证 | 公开 API 可观察的事件：IO wrapper 被 Drop、JoinHandle 返回、response body EOF/RST/Drop |
| 受信 catalog SDK 列表内部 | REST/HMS 响应体、反序列化对象及 SDK 跨页累积 | 调用准入、绝对期限、协议分页与观测；没有配置推导的单响应字节上界（D15） | SDK future 返回或取消后 Drop；位置在该 future 退出后归还 |
| Iceberg 序列化内部临时副本 | COW metadata/schema Serialize 及其内部子序列化创建的临时图 | 原 Internal 调用并发与 scope/绝对期限、实际退出责任和测量；没有构造前堆字节硬界（D17） | 实际同步调用返回/unwind，阻塞 owner 实际退出；timeout 不代替退出 |
| 第三方内部分配的记账 | 上一行对象的实际字节 | BE 侧由归属 allocator 在执行作用域下分配时归属（见 memory-governance 领域）；FE 侧只有结构上界与测量 | 归属 allocator 的真实 free |

`[patch.crates-io]` 对整个 workspace 全局生效：一旦 patch Tokio，锁文件中所有依赖 Tokio 的包（数十个，包括 AWS SDK、OpenDAL）都跑在私有副本上。path 依赖没有 registry 身份，cargo-deny 对这些包的已知安全公告不再可见，`deny.toml` 中对应的 ignore 条目也会因“未使用”而必须删除，于是公告从治理视野中消失，而不是被修复。

截至本 ADR，仓库只允许以下 vendor patch，各自有 `vendor/*/PATCH.md` 记录接缝与退出条件：`iceberg`、`iceberg-catalog-rest`、`iceberg-catalog-hms`、`paimon`（ADR-0138）、`opensrv-mysql`。它们 patch 的是领域协议/SDK 行为，而不是为了在库内部计量分配。

正例锚点：`NativeTransportAdmission` 用 Tokio 公开的 `Semaphore` 在库外持有每类连接的物理位置与握手位置，位置随连接 IO wrapper 一起退出；`bytes_with_exit_guard` 只用上游 `Bytes::from_owner` 让授权在最后一个 alias 消失时归还；`ARROW_BUFFER_OWNER_METADATA_BOUND` 用计数 allocator 测试钉住上游 Arrow 私有 owner 记录的尺寸，而不是给 Arrow 加 getter。

## 考虑过的选项

**A（选中）只对自有对象精确授权；第三方内部用公开配置、库外准入和测量约束。** 增长控制只依赖公开 API，升级第三方库只需重跑测量门与复核配置语义。代价是第三方内部字节只有结构上界加测量，不能逐字节事前授权。

**B 为取得内部钩子 vendor/fork 第三方库。** 可以对库内每次分配事前授权并证明物理释放。**设计否决**：fork 通过全局 `[patch]` 影响全部依赖它的包；隐藏安全公告；每次升级都要移植与重新审计补丁；对基础网络/运行时栈这类钩子几乎不可能被上游接受，私有分支会无限期存在。曾经的实践中，一个结果投递任务为此在 7 个网络/运行时 crate 与 3 个 Arrow crate 上累积了约 1.5 万行补丁，并引出更多仍无法闭合的内部对象（每个 clone 的 readiness future、错误 Box、DNS 解析退出），说明这条路线没有终点。

**C 自研替代传输/格式实现以拥有全部内部内存。** 例如为结果数据面写私有 TCP 帧协议。**成本否决**：要自行承担流控、TLS、认证、背压与诊断，并偏离“FE/BE 之间用 native gRPC”的边界；控制面与运行时仍在第三方库上，问题只是转移。

同类 Rust 引擎（Databend、RisingWave）的依赖清单中没有 fork Tokio/H2/Hyper/Tonic；这只说明 fork 网络栈不是常规做法，不说明它们对传输内存给出了任何特定上界。

## 裁决

**依赖规则：**
1. **No accounting forks**：不得为取得资源计量、授权或退出观测钩子而 vendor/fork 第三方 crate。
2. **Patch allowlist**：`[patch.crates-io]` 只包含本 ADR 列出的条目；新增条目必须先经设计讨论，并以新 ADR（或 supersede 本 ADR）记录接缝、理由与退出条件，同时提供 `PATCH.md`。
3. **Registry identity**：被 patch 以外的依赖保持 registry 原版本与 checksum；升级第三方库是独立、显式的依赖变更，不借重构顺带发生。
4. **Visible advisories**：已知公告的 ignore 条目必须对应真实的 registry 包；不得用 path 依赖让公告“消失”。

**设计规则：**
5. **Own what you fund**：只有 NovaRocks 自己创建并持有的对象才做事前精确授权；其退出以最后一个 NovaRocks owner 被 Drop 为准。
6. **Bound by public knobs**：第三方内部的数量与单项尺寸用其公开配置限定；公开配置不够时，在库外加 NovaRocks 持有的计数门（信号量、单飞、连接 IO wrapper），位置持到公开退出事件。
7. **Measure the rest**：第三方内部字节以“数量上限 × 配置单项上限 + 测得的每对象固定开销”作为结构上界，用 jemalloc 测量门验证线性与回落；测量失败先补库外准入或修正配置，不以修改第三方库过门。
8. **Pin private layouts by test**：确需引用第三方私有类型尺寸时，用常量加计数 allocator 测试钉住，测试在升级导致尺寸变化时失败。

**外部 SDK 列表的窄例外（2026-10-08，D15/D16）：**

远端 catalog 决定响应大小。规则 6、7 不为 Iceberg REST/HMS、Paimon 与 OpenDAL 的 SDK 列表内部缓冲声称配置字节上界，也不把进程高水位拟合成列表响应的固定尺寸。管理员配置的受信端点是此前提；NovaRocks 自有名字、页/token 记录与结果副本仍在增长前检查，越界拒绝整批，不截断。单表加载、config 与 OAuth 同样可能整体读入，本例外不构成对恶意 catalog 的 FE 安全保证。

- Iceberg REST/HMS 列表计数位置属于准确 catalog generation；等待与已进入 SDK 调用均受原绝对 deadline/stop 控制。超时或取消是结束请求，位置必须等 SDK future 返回或 Drop 后才归还。`ListingAdmission` 当前冻结为每 generation 八个位置。
- 已允许的 vendored REST crate 只增加 `list_tables_page`、`list_namespaces_page`、`list_views_page` 协议接口；NovaRocks 持有分页循环及页数、名字字节、token 长度/重复检查。支持分页的服务端按请求 pageSize 返回；忽略分页的服务端仍可能整体返回。补丁不增加 SDK 分配计量或退出钩子，不新增 patch crate；上游提供等价单页/流式接口后删除补丁。
- OpenDAL 只经现有公开 `HttpClientLayer`/`HttpFetch` 对 `Operation::List` 包装响应体。实际读取超过冻结 16 MiB 时返回不可重试错误，非列表 I/O 保留自己的合同。`raw` 接缝不承诺升级稳定，升级必须重跑钉住行为测试。有限响应体不等于 XML/SDK 内部全部分配的逐字节授权。
- HMS 保留 `get_all_*`，加调用准入与绝对期限；使用服务端支持的既有 framed 选项，不 fork pilota 或生成代码作计量。framed 消息限制不能防止解码器在检查实际剩余数据之前按声明长度/元素数预分配。

仓库钉住的 pilota 0.11.10 `thrift/binary.rs::read_string` 先按线上长度 `vec![0; len]`；hive_metastore 0.2.0 的 GetAllTables 生成解码先按声明元素数 `Vec::with_capacity`。误配传输、错端口或流错位也可能触发大分配；该缺口按已接受设计交上游独立修复，M07 不扩大 vendor 范围。

CL 记录条目、名字字节、页数、实际进程身份及调用期间 jemalloc 采样高水位，不进入 `E_FE_result` 结构上界证明。整个 information_schema SQL 还包含自有 AST/规划/wire 工作，lake discovery 还包含单表加载与 request metadata cache；这些整段高水位不能直接归因为 SDK 列表缓冲。HTTP fixture 的 handler 结束也不能代替 SDK future 或后台 job 的退出。受控协议检查不替代真实 REST/HMS/Paimon 跨 provider 验收；测试入口与产物保存边界见 [有界结果交付测试资产](../testing/mem-1-m07/README.md)。

**Iceberg 现有序列化的窄边界（2026-10-10，D17）：**

COW 冻结旧文件读取句柄需要原 snapshot metadata。现有 `TableMetadata::Serialize` 在库内执行 `self.clone().try_into()`，公开 API 不提供私有 metadata 图/Map 容量的准确上界。用户明确选择保留现有序列化、不新增 API；不为计量给 vendor 增加 getter/钩子，也不维护第二套完整 v1/v2/v3 codec。

规则 6、7 对这一项不声称构造前堆字节硬界。调用只能在原 Internal 工作位置内串行，复用原 scope 与原绝对期限；计数和写入阶段也不增加并发位置。调用前、阶段之间与发布前检查取消/期限。同步 Serialize 不可被 deadline 强行抢占；已进入的调用及原阻塞 owner 未实际返回前仍持有责任，晚到输出不得发布。

输出 JSON/String/Bytes、Nova 显式 clone、schema/default 投影、解析 scratch、group/recipe、route 与 compiler 仍是 Nova 自有对象，须增长前检查完整共存上界并持原 guard 到最后 holder 退出。D17 不允许将私有未知容量计为零、不将 wire 长度当 heap 上界、不增加窗口、不因无法证明库私有布局拒绝正常支持的类型/default 或 COW 输入。

P00b/P09 单列格式版本、metadata/schema/default/snapshot 规模、serialized 字节、原调用 owner/并发/期限/实际退出及 jemalloc 高水位；整体 COW 峰值不能直接归因于库内临时副本，测量不进入已证明 `E_FE_result`。测量出现无法解释增长或退出后不回落时重审；升级 Iceberg 重跑正常 frozen-read/default/格式语义和测量。此边界不泛化为所有 SDK 调用或所有显式克隆的豁免，D15 列表规则继续独立适用。

## 接受的妥协（诚实记录）

**第三方内部字节没有事前逐字节授权。** 单帧解码临时缓冲、任务结构体、Waker、错误对象等只由数量与配置间接约束，并通过测量验证；测量门本身依赖代表性负载，不是形式证明。

**公开退出事件可能早于内部全部 free。** 例如连接 IO wrapper 已 Drop 时 H2 仍在析构 stream 表；差额是有限固定开销，由测量覆盖、由归属 allocator 记账，而不是由位置持有到物理 dealloc。

**一些上游语义观察不到。** 例如 H2 SETTINGS 交换何时完成、GOAWAY 关闭阶段；对应的控制点要改用可观察的事件（首个认证请求头、IO Drop），语义比钩子版本更粗。

**常量钉住私有布局会随升级失效。** 这是有意的：失效以测试失败的形式出现，提醒复核，而不是静默漂移。

## 何时重新评估

- 某个第三方库在公开 API 中提供了所需的计量或退出钩子：改用公开接口，并删除对应的库外近似。
- 测量门持续出现无法用库外准入或配置解释的非线性增长或不回落：先回到设计讨论，评估更换实现（选项 C）或升级版本，而不是 fork。
- 某个第三方依赖停止维护、出现无法规避的安全问题或许可证变化：评估替换依赖；vendor 只作为有退出条件的过渡，并按规则 2 单独立 ADR。
- 产品需要对 FE 进程建立单一内存账本：重新审视 FE 侧第三方内部内存只做结构上界与测量的范围。
- 非管理员/RBAC/多租户允许任意 catalog 端点：重审全部 catalog I/O 边界，不能只界住列表。
- 上游提供等价分页/流式列表或响应体上限：复核并改用上游，删除对应协议补丁。
- 测量确认列表峰值成为 FE 内存主要来源或存在无法解释的增长：回到设计讨论；先区分自有副本、单表加载/缓存和 SDK 列表缓冲，不用整体 SQL 峰值冒充单一来源。
