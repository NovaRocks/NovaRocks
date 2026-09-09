---
id: ADR-0140
title: "StateStore contracts get their own crate, and test mechanics another"
domain: [crate-boundary, provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-09-09
provenance:
  - "discussion: 2026-09-09 StateStore contract package independence and test-mechanics isolation"
  - "PR: StateStore contract and testkit crate split #1031"
code-anchors:
  - "novarocks/state-store/api/src/lib.rs (StateStore, StateStoreProviderFactory)"
  - "novarocks/state-store/testkit/src/lib.rs (conformance, testing)"
  - "novarocks/spi/src/lib.rs (connector-only SPI surface)"
  - "tools/ci/check-spi-dependency-boundary.py (SPI dependency boundary check)"
---

## 问题

Connector 与 StateStore 这两类可替换 provider，是否必须共用同一个物理编译包？共享的 fake 与行为 suite 应该挂在契约包的
feature 上，还是自成一个包？

## 背景与执行事实

**一、`novarocks-spi` 同时是两个领域的契约包。** `novarocks/spi/src/lib.rs` 迁移前公开 `connector` 与 `state_store`
两个顶层模块。`novarocks/spi/Cargo.toml` 中 `arrow = { version = "58.2.0" }` 是**非可选 normal 依赖**——它是 Connector
读路径的列存运行时词汇，与 StateStore 无关。

**二、StateStore 领域完全不碰 Arrow。** 迁移前 `novarocks/spi/src/state_store/**` 里没有任何一处引用 arrow。该领域
实际需要的第三方依赖只有 `async-trait`、`bytes`、`sha2`、`uuid` 四个。

**三、于是每个 StateStore provider 都背着一条无语义的依赖边。** `novarocks-state-store-sqlite`、`-mysql`、
`-foundationdb` 都把 `novarocks-spi` 列为 normal dependency，因此三者都存在
`provider → novarocks-spi → arrow` 的 normal 依赖路径。这条边不表达任何 StateStore 事实，却出现在依赖图、编译计划和
安全审计的视野里。（其中只有 SQLite 是 production provider，见 ADR-0122。）

**四、测试机制原本是契约包的一个 feature。** 共享 conformance suite 与 in-memory fake 挂在
`state-store-conformance = ["dep:tokio"]` 上；`novarocks/frontend`、`novarocks/state-store/mysql`、
`novarocks/state-store/foundationdb` 三处以 dev-dependency 打开它取用。

**五、Cargo 的 feature 是 union 语义，不是隔离机制。** 同一次 resolve 中只要有任一 crate 打开
`state-store-conformance`，该次解析出的 `novarocks-spi` 就带上它，fake 与 suite 对所有依赖者一并可见。resolver v2
只在 dev-dependency 未被构建时才不做统一；一旦构建 test target（`cargo test --workspace`、
`cargo check --all-targets`），能挡住 fake 进入生产闭包的就只剩「没有人在 normal dependency 上打开它」这条**约定**，
而不是 Cargo 的强制。

**六、ADR-0006 自己写下了这条触发条件。** 其「何时重新评估」第二条：「某 SPI provider 模块需要与其他模块明显不同且
互斥的基础依赖，导致统一 crate 的 dependency ceiling 无法维持时」。arrow 正是这个互斥基础依赖，条件已经成立。

**七、既有 guard 的形态。** `tools/ci/check-spi-dependency-boundary.py` 以 `cargo metadata` 断言 SPI 的 normal
依赖集合与 conformance feature 形状；它是按包名硬编码依赖清单的检查，不是通用的方向性约束。

## 考虑过的选项

**选项一：维持统一 SPI，把 arrow 变成 optional，由 `connector` feature 打开。**
manifest 改动最小。但 feature 仍是 union：任何同时选中 Connector consumer 的构建都会重新打开 arrow，于是「StateStore
provider 的依赖图是否干净」不再是它自己 manifest 上可读的性质，而是要靠整个 workspace 的 feature 解析结果才能证明。
而且这只处理 arrow 一个符号——下一个 Connector-only 的基础依赖出现时，同样的辩论要重来一遍。

**选项二：StateStore 契约独立成 crate，但 fake 与 suite 继续挂在它的 feature 上。**
依赖隔离拿到了，测试机制的问题原样保留：feature 仍是 union，仍然靠「没人打开」的约定，只是把这条约定搬进了一个更小的
包。收益一半，风险照旧。

**选项三：契约独立成 crate，测试机制再独立成一个只被 dev-dependency 引用的 crate。**（采纳）
两条边都由 Cargo 强制：api 的 normal 依赖里没有列存运行时，testkit 只以 dev-dependency 出现，因此 production 依赖
闭包在结构上到不了 fake。代价是仓库多两个 crate、fake 演进跨包。

**选项四：保留旧 SPI 的 `state_store` 入口做 re-export / facade。**
迁移量最小，但 facade 不拥有类型，仍携带 transitive arrow，且长期并存两条 canonical path——依赖方向根本没变。
ADR-0006 的选项四已经否决过同一形态，这里不重开。

## 裁决

**一、`novarocks-state-store-api` 是 StateStore 领域公共类型的唯一 owner。** 它拥有 transaction、range、version、
commit outcome、error、limits、metrics 与 provider factory/instance/lifecycle 的完整词汇；normal 依赖只有
`async-trait`、`bytes`、`sha2`、`uuid`；不依赖列存运行时、Connector 契约或任何应用。

**二、旧入口整体删除。** `novarocks-spi` 只剩 `connector` 一个模块。不保留 re-export、facade 或第二条 canonical
path；调用方一次性改为 `novarocks_state_store_api::*`。

**三、`novarocks-state-store-testkit` 拥有测试机制。** in-memory reference store 与共享行为 suite 归它，它依赖 api；
**api 不依赖 testkit**（api 自己的契约测试用 target-local stub，不用 fake）。provider 与应用只能以 dev-dependency
使用 testkit。

**四、「测试专用」用 crate 边界表达，不用 feature。** 理由是机制性的：dev-dependency 是 Cargo 真正能把一个包挡在
production 依赖闭包之外的边，feature 不是。这与 ADR-0058 的推论同源——一条约束只能靠约定或扫描表达时，正确反应是把
边界物理化成 crate；也与 ADR-0069 的分工一致——中立的测试机械能力归 `novarocks-test-support`，而领域 conformance
跟随它的领域 owner，这里就是 StateStore 自己的 testkit。

**五、ADR-0006 的其余原则继续有效，本篇只替换它的一条前提。** 仍然有效的是：SPI 是完整 provider contract 而非
trait-only crate；host 拥有 provider 选择、装配与生命周期，consumer 不依赖 provider 身份；同仓按 workspace 原子演进、
不留 bridge / 双写 / 兼容层；禁止 `Services`、`get<T>()`、`dyn Any` 分派、万能 `SpiContext`、production global install
与 service locator。本篇替换的**只有**「Connector 与 StateStore 两个 provider 类别共用同一个物理 SPI package」这一条
前提。

因此 **ADR-0006 不标 `superseded`**：本目录 README 的 supersede 是整篇语义（`status: superseded` 与
`superseded-by` 互为充要条件，且旧条目移入领域「历史」小节），而 ADR-0006 的准入模型并未被推翻。frontmatter 里没有
表达「部分替代」的字段，本篇不发明一个；这条关系由本节与 README 索引行承载。

**六、本次是纯物理迁移。** 公共类型、语义、配置、持久格式与测试行为都不变。契约收缩——例如删除跨重启历史协议、
attempt 状态机——是后续的独立裁决，不在本篇范围，也不得引用本篇作为已完成的证据。

## 接受的妥协（诚实记录）

**只买到依赖隔离，没有买到任何新能力。** 代价是仓库多两个 crate，以及一次全仓导入路径改动（三个 provider crate、
frontend、server 的 `novarocks_spi::state_store::*` 全部改名）。选它的真实理由是那条 `provider → spi → arrow` 的边会
持续误导依赖图读者、并把列存编译成本压进与列存无关的 provider——**不是**因为「crate 越多越解耦」。ADR-0006 的选项三
（机械地把每种能力提成 crate）仍然被否决；本篇只针对 ADR-0006 自己写下的那条已经成立的触发条件动手。

**testkit 独立后，fake 的演进要跨 crate 协调。** 同包 feature 下改 trait 与改 fake 是一次编辑；现在是两个 package
的两次编辑，且三个 provider 的 dev-dependency 要一起过一遍。接受这份繁琐，因为换来的是 Cargo 能强制的隔离，而不是靠
review 记得「别在 normal dependency 上打开那个 feature」。

**本篇不解决 StateStore 公共契约本身过宽。** 契约里仍有为跨重启恢复设计、但当前没有生产消费者的部分（跨重启历史
协议、attempt 状态机一类）。把它们搬进新 crate 只是换了位置，没有收缩一个字节的 surface。要不要删、删到哪，需要另一篇
ADR。本篇不得被引用为「StateStore 契约已经收敛」。

**guard 守的是依赖方向与能力边界，不是精确依赖清单。** CI 的依赖边界检查能挡住 api 重新长出列存、Connector 或应用
依赖，也能挡住 testkit 变成 normal dependency；它挡不住有人往 api 里加一个中立但无用的第三方依赖。那一层仍然只有
review。

**真实节省的生产编译面小于「三个 provider」给人的印象。** 只有 SQLite 是 production provider（ADR-0122），MySQL 与
FoundationDB 是实验 leaf crate。拆分让三个 crate 的依赖图都变干净，但其中两个本来就不进生产闭包。

## 何时重新评估

1. **出现第二个需要同一 StateStore 契约的独立应用宿主时。** 今天只有 frontend/server 一条组合路径，api 的 surface
   是否真中立没有第二个证人；届时应重新判断它是按单宿主需要裁剪的，还是确实领域完整。
2. **StateStore 需要在仓库外发布或承诺 ABI 时。** 那要求 api 有自己的 semver、兼容区间与 loader 隔离决策，触发的是
   ADR-0006 的第一条重评条件，范围大于本篇。
3. **testkit 的跨 crate 协调成本被实测为主要瓶颈时**（例如契约演进被 fake 的滞后持续阻塞）：重新比较「同包 test-only
   模块 + 更强 guard」的方案。但不得退回 feature 隔离，除非 Cargo 提供了能把 feature 挡在 production 闭包之外的机制。
4. **`novarocks-spi` 里出现第二个与 Connector 基础依赖互斥的 provider 类别时。** 按本篇同一判据物理拆分，不再重开
   「统一包还是独立包」的讨论。
5. **契约收缩裁决落地后。** 回来核对本篇「纯物理迁移、语义不变」的声明是否仍准确描述当时的 api。
