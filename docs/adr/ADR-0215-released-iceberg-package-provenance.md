---
id: ADR-0215
title: "Released Iceberg packages define provenance and unmodified crates use the registry"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0216
    scope: "HMS as a retained vendored package with registered divergences: HMS uses the unmodified registry release, and core is the only retained Iceberg vendor package"
    link: "ADR-0216-hms-uses-upstream-catalog-with-read-fallbacks.md"
partially-supersedes:
  - id: ADR-0174
    scope: "Official release provenance and generation method; property macro retention/allowlist; four-path-package advisory compromise"
    link: "ADR-0174-vendored-iceberg-baseline-and-patch-policy.md"
date: 2026-11-04
provenance:
  - "discussion: 2026-11-04 accepted coordinated official Iceberg release package provenance and removal of unmodified vendor copies"
code-anchors:
  - "Cargo.toml ([patch.crates-io])"
  - "vendor/iceberg-0.11.0/PATCH.md"
---

## 问题

上游已正式发布时，Iceberg 家族应以什么制品确定来源，哪些包还应保留 vendor，独立检查怎样避免改写来源记录？

## 背景与执行事实

同源家族由 core、HMS、REST 与 property macro 组成。core/HMS 仍有已登记分歧；macro 无源码分歧，REST SDK 仅作测试对照。Cargo 的根 patch 传递到 registry REST 的 core 依赖，因此混合 registry/path 来源仍可共同消费一个 core。

| 载体 | 拥有的事实 |
|---|---|
| registry `.crate` 与 index checksum | 对外可复核的发布字节身份 |
| 每包 `PATCH.md` | 正式 tag/commit、checksum、内容核验与分歧退出责任 |
| 包级发布 lock | 发布包附带的原解析记录，不决定应用生产图 |
| 独立检查副本的验证 lock | 翻译 manifest 后实际用于独立检查的解析结果 |
| 根 `Cargo.lock` | 唯一应用 workspace 的生产解析结果 |

正式 0.11.0 基线为 `20eb0feddac8f40bb9e9108a0744b7f356d69480`。core 的 Arrow 60 移植仍受 ADR-0204 约束；不能因正式发布存在就删除尚无等价替代的分歧。

## 考虑过的选项

- **从正式 tag 本地重新打包：设计否决。** 会制造与公开 registry 制品并存的第二字节身份，不同 Cargo 打包版本也可能不同。
- **所有包继续 vendor：设计否决。** 没有分歧的副本增加维护面并失去 registry 公告覆盖，不能以当前目录存在作为保留理由。
- **所有包立即用 registry：当前能力条件下否决。** 会丢失仍有消费者的协议分歧与同版本 Arrow 接口；待正式上游满足等价行为后重新评估。
- **有分歧者以发布包为底，无分歧者直接 registry：采用。** 保留共同基线和退出责任，同时缩小本地维护面。

## 裁决

1. **Published bytes first。** 正式发布家族以 registry 原包/checksum 为来源，不以本地重打包代替。核对 VCS 声明和实际内容；声明本身不构成来源证明。
2. **Only divergence remains vendored。** 无本地分歧的包直接依赖 registry；保留的 core/HMS 使用同一正式发布基线，原样重放已登记补丁/移植，不扩授权。
3. **Separate source and validation。** 分别解释源码、规范化元数据与打包生成物。原包、原样树和 vendor 发布 lock 不被检查工具改写；独立检查使用候选源码副本，其验证 lock 不回灌 vendor 或根 lock。
4. **Consumer-owned exit remains。** ADR-0174 的同源家族、补丁删除责任及 ADR-0204 的移植退出条件继续生效。新规则仅替代正式发布身份、macro 保留项及四 path 包的妥协。

## 接受的妥协（诚实记录）

core/HMS 仍为 path 包，来源清单不能补成 registry 安全公告覆盖。正式包仍可能需要依赖翻译；发布 lock 与验证 lock 不一致是需要解释的记录，不能称作原样解析重现。独立开发依赖不可解析时保留真实缺口，应用主图通过不替代它。

## 何时重新评估

上游正式版本包含 Arrow 60 和等价协议能力时，维护者协调重设整个家族基线并按消费者覆盖删除分歧。上游移植改写、关闭或需要广泛改造时，按 ADR-0204 回到裁决；包被 yank 或来源内容无法解释时停止切换。没有正式 registry 发布时，采用的临时来源身份须独立说明，不冒充正式发布物。
