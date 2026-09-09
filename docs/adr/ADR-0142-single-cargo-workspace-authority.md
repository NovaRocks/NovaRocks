---
id: ADR-0142
title: "All NovaRocks packages share one Cargo workspace and lock authority"
domain: [crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-09-09
provenance:
  - "discussion: 2026-09-09 Cargo dependency governance"
  - "PR: pending — backfill the number once the workspace convergence merges"
code-anchors:
  - "Cargo.toml ([workspace])"
---

## 问题

NovaRocks 同仓维护的产品、测试与工具 package 应各自拥有 Cargo workspace、resolver 和 lockfile，还是由根 workspace
统一拥有解析与供应链政策？

## 背景与执行事实

仓库曾有一个包含 32 个 package 的根 workspace，以及 SQL runner、通用 tools、connector binding benchmark、
DataSketches benchmark 和 error-manifest 五个独立 workspace。SQL runner 通过 path dependency 重新编译多个产品 crate，
因此独立 lock 并不只是隔离一个外部客户端，而是允许同一份 NovaRocks 源码在另一张依赖图中选择不同的传递版本。

五个独立 package 都与根仓库原子演进、使用同一 Rust toolchain，也没有独立发布或独立 MSRV 契约。SQL runner 的
`dev-opt` profile 与根定义相同，tools 的 Iceberg/Paimon patches 也只是复制根 policy。只读合并探针证明 37 个自有
package 可以在根 resolver 2 和一份 lock 下联合解析与检查；合并会把工具专属依赖加入根 lock，也可能在兼容 semver
范围内重选少量传递版本，这些是单一解析 authority 的可见结果，而不是产品协议变化。

ADR-0071 对 cluster harness 的唯一 owner、SQL runner 只作 frontend adapter 等裁决继续有效；其中“SQL runner 保持独立
workspace/profile”的历史妥协由本 ADR 替换，因为统一后的根 profile 已能表达相同行为，而双 lock 会削弱依赖治理。

## 考虑过的选项

1. **保留六个 workspace 与 lock。** 每个工具可以独立解析和调整 profile，局部构建较轻；但产品源码会被多张图重新解析，
   duplicate、source、advisory 与 license policy 必须重复维护，普通 PR 也难证明所有图都受同一门禁。
2. **只统一 manifest 声明，保留独立 lock。** Workspace inheritance 可以减少文本重复，却不能统一实际解析结果；同一 direct
   requirement 仍可能在不同 lock 中选择不同传递版本，因此只解决书写形式，不解决 authority。
3. **所有 NovaRocks 自有 package 进入根 workspace，共享 resolver 和 committed lock。** Root 唯一拥有共同解析与 policy，
   member 保留自己的依赖 kind、target、optional 与 features；代价是根 lock 和 `--workspace` gate 变大。（采纳）
4. **额外引入 workspace-hack/Hakari。** 它可能稳定 feature closure 和编译缓存，但当前没有同机同缓存数据证明 feature
   抖动是瓶颈；在统一 authority 之前引入会混淆 topology 与性能优化，因此暂不采用。

## 裁决

所有随 NovaRocks 仓库原子演进的自有 Rust package 都是根 workspace member，共享根 `Cargo.lock` 和显式
`resolver = "2"`。只有具有独立上游所有权的 vendored package 保留自己的 workspace/lock。

根 manifest 唯一拥有 profiles、registry patches、共享 package metadata、共享 lint baseline，以及 direct dependency 的
共同 version/source/default-feature policy。Member manifest 继续拥有是否消费、normal/dev/build/target kind、`optional` 和
consumer-specific features。`cargo run --manifest-path ...` 等入口可以保留，但必须向上解析到当前 checkout 的根 workspace
与 lock，不得恢复局部解析 authority。

依赖声明统一不等于 resolved graph 已受治理。PR gate 必须在 committed lock 上运行 Cargo metadata、全 workspace targets
和成熟的 resolved-graph policy 工具，检查 duplicate、source、advisory 与 license；NovaRocks 自制 guard 只保护产品特有
owner/closure contract，不复制 Cargo 或图工具的通用谓词。

## 接受的妥协（诚实记录）

- 根 lock 会包含只供工具和测试使用的 packages，lock diff 和首次解析比只构建 server 更大；接受该成本以换取唯一解析事实。
- 显式 `--workspace --all-targets` 会编译更多 package，CI 冷缓存时间可能增加；默认 `cargo build` 仍可通过
  `default-members` 聚焦 server，但治理 gate 不允许借此隐藏成员。
- 五个 edition 2024 独立 workspace 从 Cargo 默认 resolver 3 收敛到根 resolver 2。这样先把 topology 与 resolver 升级
  分开归因；它不表示 resolver 2 长期优于 resolver 3。
- 共享 policy 会让真正的 member-specific 例外更显眼，也需要逐项说明。接受显式例外维护，不用全局 allow 或复制声明来
  换取表面整齐。

## 何时重新评估

- 项目明确并验证 MSRV，且 resolver 3 的 MSRV-aware resolution 能产出可解释的 graph diff 时，单独评估根 resolver 升级。
- 某个工具取得独立发布、独立版本生命周期或与产品不兼容的 toolchain/MSRV 契约时，重新判断它是否仍属于同一原子仓库。
- 同机同缓存测量证明统一 feature closure 造成可复现的显著重编译成本时，再评估 workspace-hack/Hakari；不能只凭 package
  数量引入。
- 某 package 需要与根 profile 无法共同表达的 target/profile 行为时，先调整 package/target 边界；只有证明确需独立解析
  authority 后才恢复独立 workspace，不能以目录便利为理由。
