---
id: ADR-0134
title: "Use the exact registry DataSketches prerelease as the probabilistic substrate"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-09-03
provenance:
  - "discussion: 2026-09-03 registry DataSketches substrate decision"
  - "PR: pending — backfill the number once the registry cutover merges"
code-anchors:
  - "novarocks/connector/iceberg-functions/src/theta.rs (IcebergThetaAggregateFamily)"
  - "novarocks/execution/src/exec/hll.rs (HllHandle allocation admission)"
---

## 问题

NovaRocks 应如何依赖尚处于预发布阶段的 Rust DataSketches 实现，才能同时保证构建可复现、概率结构只有一个格式与集合运算 owner、Java/C++ 互操作可验证，并且不维护私有 fork？

## 背景与执行事实

HLL 和 Theta sketch 不是普通的可替换容器。它们的 seed、hash 输入域、状态迁移、压缩布局、序列化版本和集合运算共同决定估值与 wire 兼容性。只复用上游算法却自行解释或重写二进制格式，会形成两个正确性 owner：上游拥有 sketch 状态，而 NovaRocks 另行拥有 codec 或 union 语义；两者升级时可以各自通过单元测试，却在跨语言数据上静默分叉。

Rust `datasketches` 的目标 API 目前以预发布版本发布。预发布依赖若只写宽松 semver 范围，或者从可移动的 Git 分支构建，同一天的源码也可能解析到不同实现。Cargo 编译器能强制 crate 依赖边界，却不能仅凭源代码 import 证明最终图中只有一个版本、来源是 crates.io、且下载内容对应预期 checksum。因此此处需要检查 Cargo metadata 与 lockfile 所表达的实际依赖事实；这不是扫描目录、文件数量或源码 token 的 source-shape guard。

`ThetaSketchHandle` 是 Iceberg provider 将 Arrow 值输入、标准 compact body、set union 与 Puffin 统计连接起来的窄适配边界。NovaRocks 的私有统计载体只拥有 admission：总长度上界、载体版本、`lg_k`、有序性与 retained-entry 上界。其形状固定为 `V2 | lg_k | opaque ordered v3 compact body`；opaque body 的格式、反序列化和集合运算仍由上游标准实现唯一拥有。

## 考虑过的选项

**A. crates.io 0.2 加 NovaRocks 手写 codec 与集合运算。** 版本稳定、已有调用面改动较小，但 0.2 缺失所需标准能力时只能由项目补齐二进制解释与 set operation。这样会永久形成第二格式 owner，也让 Java/C++ 兼容依赖本地实现细节，因此否决。

**B. vendored 源码或跟随上游 Git main。** 可以立即取得未发布修复，也能冻结一份提交；代价是 vendored 副本成为事实上的私有维护分支，而 Git main 是可移动来源。二者都绕开 crates.io 包内容与 checksum 的统一供应链身份，并把后续同步责任留给 NovaRocks，因此否决。

**C. 抽取、改名或用内部 facade 包装所需上游实现。** facade 可以缩小调用面，但若其复制算法、codec 或 set operation，只是把私有 fork 藏进内部名字；若它只转发 API，则不能解决来源与图唯一性，反而增加一个无语义 owner。因此不以 extract/rename/facade 作为依赖策略。

**D. 同时保留 0.2 与 0.5 预发布版本。** 渐进迁移的局部风险较低，但同一进程内存在两套 hash、格式和集合语义，会迫使调用方决定何时转换并长期承担双栈测试。概率结构不能通过普通类型适配证明等价，因此否决。

**E. 经 Java FFI 使用成熟实现。** 可以直接复用 Java 生态的格式实现，但会把 JVM 生命周期、跨语言内存、错误映射和调用开销带进 Rust 执行热路径；NovaRocks 已有可用的 Rust 标准实现，这个复杂度没有对应收益，因此否决。

**F. 建立 NovaRocks Alpha 私有 fork。** 它能让项目自行稳定 API、快速合并修复，却同时取得发布、漏洞响应、格式兼容和长期 rebase 的全部责任。除非上游路线无法满足真实产品需求，否则这与“不维护第二实现”的目标相反，因此否决。

**G（选中）. 精确锁定 crates.io 的 `datasketches = "=0.5.0-rc.1"`。** 每个 consumer 只启用自身需要的最小 feature，由上游标准 API 拥有 codec、状态和集合运算；NovaRocks 只保留有界 carrier/admission、跨语言 TCK 与依赖图守卫。

## 裁决

所有产品 consumer 精确依赖 crates.io 的 `datasketches = "=0.5.0-rc.1"`，不得使用宽松版本、Git、path、vendor、patch 或私有 fork。该包的预期 registry checksum 是 `407f3fe0c32e6547cb8637b11a8a765ff027afa31e5f6f732b23f8d74672087b`；Cargo.lock 与 registry source 是解析后依赖身份的权威，CI 通过 Cargo metadata 与 lockfile 的关联检查确保全仓只有这一版本和这一来源。每个 consumer 仅启用实际需要的 `hll`、`theta` 等最小 feature，不用全功能默认集掩盖所有权。

HLL/Theta 的构造、更新、compact 序列化、反序列化、estimate 和 union 必须调用 0.5 标准 API。Java、C++ 与 Rust 生成的固定向量组成跨语言 TCK，验证标准 body 的读取、拒绝条件和 set operation；本地 round-trip 不能替代这组证据。

格式所有权明确分层：上游 DataSketches 是 sketch body、状态机、codec 与集合运算的唯一 owner；NovaRocks 只拥有产品载体和 admission。Theta partial carrier 固定为 `V2 | lg_k | opaque ordered v3 compact body`，先执行总长度与头部检查，再由上游标准 decoder 解释 opaque body，最后验证 ordered 与 retained-entry 上界。NovaRocks 不解析 body 字段、不复制上游 codec，也不以兼容 shim 接受第二种权威表示。

HLL 内存 admission 与该精确版本的分配形状一起冻结，但不取得 body 语义所有权。preflight 只解析 RC1 固定头部中影响分配的 mode、target、`lg_k`、`lg_arr`、coupon/aux count，结合当前 union 的有效 `lg_k`、mode、capacity 与 generation，给出 current、绝对 peak 和 additional headroom；标准 decoder 仍负责 estimator 与 body 的语义验证。当前 union 在 `lg_k=5` 时 LIST 与 HLL8 dense 都占 32 bytes，preflight 必须显式保留该歧义并对 lower-`lg_k` dense merge 取两条分配路径的较大值，不能以 estimator 猜 mode；LIST/SET 的 coupon 上界只可使用 RC1 `Container::estimate = max(exact_len, interpolation)` 的硬下界性质。调用方先按 additional headroom 取得 reservation guard，操作在任何 DataSketches 分配或修改前重验 token、当前状态和 payload 头，返回精确新 current 后由调用方在 guard 仍存活时完成永久 charge 对账。库内不得自行取得或提前释放 reservation，也不得用全局 `lg_k=21` 最大值替代实时配置与状态推导。

永久守卫检查实际 Cargo 图与 lockfile，而不是源码形状：动态发现 workspace，运行 locked Cargo metadata，关联精确版本、crates.io source 与 checksum，并拒绝旧版本、双版本、Git/path/vendor/patch 和 checksum 漂移。上游发布包、跨语言 TCK、consumer 测试和图守卫四者共同构成升级门；任何一项缺失都不能宣称兼容迁移完成。

## 接受的妥协（诚实记录）

**接受预发布 API 风险。** `0.5.0-rc.1` 可能在下一 RC 或稳定版改变 API、feature 或格式细节。精确锁定让当前构建可复现，却不会降低未来迁移成本；项目必须主动完成一次受证据约束的升级，而不能依赖 semver 自动漂移。

**接受 registry 与缓存可用性成为构建条件。** 不保存 vendored 副本意味着首次构建需要从 registry 获得精确包，离线环境必须预热 Cargo cache。这个供应可用性风险是真实的；选择它是为了避免 NovaRocks 同时成为分发者和补丁维护者，而不是因为 registry 永不故障。

**接受更重的升级门。** 每次版本变化都要重新跑跨语言 TCK、图守卫、consumer 验证和结构性 benchmark，并检查 checksum。它比普通库升级更昂贵，但概率结构的错误常表现为估值偏差或历史数据不兼容，无法靠编译成功发现。

**最小 feature 是逐 consumer 的显式维护负担。** 新调用能力不会自动可用，manifest 必须随真实需求调整。接受这点是为了让依赖面和审计事实可见，而不是依赖一个方便但不可审计的全功能集合。

## 何时重新评估

- crates.io 发布稳定 `0.5`，或新的 RC 能在保持标准 body 兼容的前提下通过现有跨语言 TCK、图守卫、consumer 测试与结构性 benchmark；
- 上游出现由官方维护的 Rust Alpha 路线，且 NovaRocks 有当前实现无法满足的真实产品需求；届时比较 upstream 贡献、官方 Alpha 与独立实现，而不是默认建立私有 fork；
- 当前版本出现 CVE、供应链撤包、长期无人维护或无法获得必要安全修复；
- 相同 workload、参数与构建 profile 下出现可复现的结构性 benchmark 退化，而非单次时延噪声；
- DataSketches 引入新的格式 major version、seed/hash domain 变化或集合语义变化，使现有 Java/C++/Rust 向量不再代表目标互操作契约。
- DataSketches 改变 HLL list/set/dense、HLL4 aux map、decode 或 union transition 的分配形状；升级前必须重审 allocation header parser、状态推导和全局 allocator 峰值测试。
