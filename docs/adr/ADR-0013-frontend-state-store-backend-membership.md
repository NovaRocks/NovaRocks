---
id: ADR-0013
title: "Frontend StateStore owns durable backend membership"
domain: [cluster-membership]
status: active
supersedes: []
superseded-by: null
date: 2026-07-29
provenance:
  - "discussion: 2026-07-29 frontend backend membership authority"
code-anchors:
  - "novarocks/frontend/src/topology.rs (ClusterBackendService)"
  - "novarocks/frontend/src/topology.rs (ClusterBackendRepository)"
  - "novarocks/frontend/src/application.rs (FrontendApplicationHost)"
---

## 问题

分布式 FE 如何在进程重启后恢复 backend membership，同时不把 durable membership、heartbeat observation 和 core execution 混为多个 owner？

## 背景与执行事实

`BackendTopologyPort` 是 core 消费的中立 topology 与 backend-management 边界；frontend 已拥有 coordinator、query event、heartbeat 和 application lifecycle。backend 的期望成员状态需要跨 FE 重启保存，而 heartbeat、generation、live 状态和 scheduled fragment 数是运行期事实，不能作为 durable catalog 的替代。

StateStore 已提供 provider-neutral 的 value、transaction、compare-and-set、operation id 与 commit resolution contract。将 backend membership 继续留在 core metadata repository 或 test-only global registry，会形成双写、运行中安装 persistence 或隐式 fallback，无法给 SQL management 和 restart recovery 定义唯一 authority。

## 考虑过的选项

1. 继续在 core metadata repository 持久化、frontend controller 仅同步：短期改动较小，但 durable truth 与 runtime owner 分离，且需要 bridge、global install 或双写来保持一致。
2. 以 heartbeat/live registry 作为恢复来源：运行期信息更新快，但它不能表达 decommission intent，重启后也不能证明动态 ADD/DROP 的最终状态。
3. frontend 以 StateStore 单 aggregate 持久化 desired membership，`ClusterBackendService` 从同一 authority 恢复 runtime topology：durable 和 runtime 的边界明确，但 FE 必须显式配置 StateStore，且 mutation/commit resolution 需要严格串行化。

## 裁决

选择选项 3。`ClusterBackendService` 是 backend membership 的唯一 concrete owner；它在构造时固定为 durable repository 或 transient mode，不支持运行中安装、替换 persistence，core 只保留 `BackendTopologyPort`。

role=fe 必须打开 StateStore。repository 使用一个版本化 aggregate 保存稳定 backend id、canonical endpoint、desired `Active|Decommissioning` state、sequence 和 operation identity；configured backends 只是按配置顺序补充缺失 endpoint 的 additive seeds，绝不删除 durable-only backend、复用 id 或重新激活 decommissioning entry。SQL ADD/DROP 的 durable mutation 先提交并完成 commit resolution，随后才把 authoritative catalog 应用到 runtime topology、revision 和 query events。

heartbeat success、lost/restart、generation、live state 与 fragment activity 只更新 runtime facts，绝不回写 StateStore。non-force DROP 先 durable mark decommissioning 并停止新调度，等待 query drain 后再删除 durable entry；force DROP 立即删除并发送 query unavailable event。commit 成功而 runtime 无法应用时，service fail closed，不反向篡改 durable authority。

role=all-in-one 继续使用 transient membership，不持久化 loopback backend；role=be 不创建 frontend membership owner、StateStore consumer、QueryService 或 MySQL listener。

## 接受的妥协（诚实记录）

单 FE writer 是当前的明确边界。SQLite 因此只能用于一个 active FE；多 FE failover、lease、fencing 和 change stream 不在本裁决内。StateStore 暂时不可用时，已恢复 backend 的 heartbeat observation 可以继续，但 ADD/DROP 必须失败，不能退回内存或 legacy metadata。

单 aggregate 对 backend 数量受 StateStore value limit 约束；在真实规模证明需要分片前，不提前引入 per-backend 多键、dual read/write 或 compatibility facade。每次 mutation 也会增加 durable transaction 与 authoritative read 的开销，以换取 restart 和 `CommitUnknown` 下可验证的唯一结果。

## 何时重新评估

- 部署需要多个可同时写 membership 的 FE，且已有可验证的 lease、fencing、takeover 和 provider failure 模型时；
- backend membership aggregate 接近 provider value limit，且有真实规模数据证明需要版本化分片时；
- `BackendTopologyPort` 需要跨进程暴露 durability 或 lifecycle details，而不再只是 core consumer port 时；
- StateStore contract 无法提供 membership 所需的 commit resolution、CAS 或 availability guarantees，必须重新选择唯一 durable authority 时。
