---
id: ADR-0172
title: "MySQL query deadlines and cumulative active delivery work budgets have separate owners"
domain: [distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0213]
partially-superseded-scope:
  ADR-0213: "MySQL writer cumulative delivery budget, receipts, Closing and startup projection are retired; original query deadlines, typed causes, success visibility and actual resource-exit rules remain active."
date: 2026-10-13
provenance:
  - "discussion: 2026-10-13 MySQL producer waiting, cumulative active delivery work and bounded Closing"
code-anchors:
  - "novarocks/mysql-adapter/src/active_delivery_budget.rs (ActiveDeliveryWorkBudget, ActivePhase::drop)"
  - "novarocks/mysql-adapter/src/relay_result_writer.rs (write_relay_result_one, close_relay)"
  - "novarocks/mysql-adapter/src/local_result_writer.rs (write_local_result_one, finish_closing)"
  - "novarocks/mysql-adapter/src/terminal.rs (write_success)"
  - "novarocks/mysql-adapter/src/startup_profile.rs (MysqlStartupParameters)"
---

## 问题

MySQL 响应如何分别约束查询计算、等待结果、客户端接收和取消后的尾部交付，同时防止分段写出无限续期？

## 背景与执行事实

一个 statement 有两个不同的推进者。Query Application 在 admission 时固定查询期限和取消身份，执行器负责产出结果；MySQL Adapter 消费结果，并通过协议 writer 交付 metadata、row 和 terminal。等待下一段结果不表示正在向客户端写出。将两者放在同一个响应时钟下，会让协议层提前裁决查询生命周期。

| 时间责任 | owner 与执行边界 | 有效约束 |
|---|---|---|
| 查询期限与执行失败 | Query Application 的 cancellation/failure view | 从原 admission 延续；Adapter 观察原类型与原因，不创建新查询期限 |
| producer 等待 | `StreamingStatementResult::next_delivery` | 观察原查询取消和执行失败；不持有活跃写出 phase |
| 活跃交付工作 | 响应私有 `ActiveDeliveryWorkBudget` | 一份累计 30 秒额度，覆盖有限预检、编码、streaming lease、metadata、Data、terminal 与 flush |
| Local 结果工作 | `write_local_result_one` | 已封闭本地图的有限 render/counting turn 与写出共用同一额度，不伪称 producer 等待 |
| completion-only 成功 | `write_success` | 普通受治理 terminal 成功同样由活跃交付工作额度约束 |
| Closing | 原 writer receipt、已驻留合法尾部及 Closing owner | 独立绝对 5 秒期限；取消不会恢复普通响应额度或延长期限 |

`ActiveDeliveryWorkBudget::run` 首次 poll 时取得剩余额度的 phase；`run_sync` 对有限同步工作计费。`ActivePhase::drop` 在正常返回、错误、超时或外层取消丢弃 future 时扣除实际 elapsed。零额度拒绝进一步工作；每个 phase 结束都检查期限，即使内部同步 poll 已经 ready 也不能据此续期。有限同步工作不能被时钟抢占，因此阶段内调度、framing 与复制都计入交付工作，而不是只统计 socket 阻塞。

Relay 与 Local 成功仍先取得原 success visibility seal，再实际完成 terminal/flush，最后发布 complete。seal 固定成功可见性，不保证物理 flush 已成功。取消的 borrowed write future 先真实 drop，再按原 receipt 决定合法 Closing；无合法驻留尾部时断连，Closing 不重新 fetch/render producer。

## 考虑过的选项

**metadata 起点或第一行起点的绝对响应期限。设计否决。** 时钟简单，但后续 producer 空隙同样扣费，协议层会以写出超时推翻未到期的查询期限。更换起点不能解决 owner 混合。

**每帧、每次 write 或每次进展重置期限。设计否决。** 可容忍短写，但慢速持续进展可以无限延长一个响应的资源持有，无法兑现响应整体有界的交付工作。

**仅在 `poll_write` / `poll_write_vectored` / `poll_flush` 层精确计费。成本否决。** 可以更细分 socket 行为，但需要扩大 owned writer 改造及取消交接面；当前没有必须消费这类精确归因的 owner，因此不承担这项改造，亦不把有限 phase 计时声称为该精度。

**查询期限、累计活跃交付工作和独立 Closing 分责。裁决。** 沿现有 owner 区分 producer 空隙与有限交付 phase，累计额度防止跨帧续期，原 receipt 和实际退出继续决定资源交接。

## 裁决

1. **原查询期限规则。** producer 等待只观察原查询取消与执行失败；不得创建 adapter 执行超时或修改用户查询期限。Query cancellation 的原类型、优先级与原因保持。
2. **单响应累计规则。** relay、Local 与 completion-only 的普通成功响应使用一份累计 30 秒活跃交付工作额度。metadata、Data、terminal、flush 及有限本地编码共债；分帧、producer 空隙和少量进展不得恢复额度。
3. **退出计费规则。** 正常完成、失败、timeout 和 future drop 都扣除已进行的 phase 时间。额度为零即拒绝新 phase，不能因 future 立即 ready 放行。
4. **Closing 独立规则。** 取消后只使用原 writer receipt 允许的合法驻留尾部及绝对 5 秒 Closing 期限；重复取消不重置。不得为写错误额外等待 producer，也不得重建 row。
5. **终态与实际完成规则。** success seal 后仍以剩余额度约束物理 terminal/flush；实际 finish 成功才发布 complete。写出超时沿 I/O/断连 settlement，不能伪装为 producer 查询超时或替代实际资源释放。
6. **语义命名规则。** startup projection 使用 `active_delivery_work_budget`，不保留容易被理解为整条响应绝对期限的别名；既有 auth/command 绝对期限另归其 owner。

## 接受的妥协（诚实记录）

30 秒是累计有限交付工作的上限，不是 CPU 时间或内核 socket blocked 时间。phase 中的调度等待、编码、复制也占额度；机器繁忙可能使正常读取客户端耗尽额度。选择该边界是为了沿已有 writer 生命周期安全落地，而非证明它比底层精确计费更准确。

有限同步 render/preflight 只能在返回时检查耗时，不能承诺抢占一个不返回的同步 kernel。正常结果等待可以长于 30 秒，仍受原 query deadline 控制。成功 seal 后物理交付可能失败，已决定的成功可见性与连接是否完成交付必须保持区别。

本裁决不新增配置、产品钩子、协议字段，也不扩展 Closing 资源容量或全部 command 路径；不能从期限分责推导所有查询正确性或任何性能结论。

## 何时重新评估

- 若产品必须区分客户端背压与 adapter 调度/编码时间，并有实际消费者依赖该精度，重新评估底层 writer 的精确计费及 owned future/receipt 交接成本。
- 若有限同步工作不再有可审查的轮次上界，重新设计可取消的工作边界；不能只提高累计额度掩盖不可抢占工作。
- 若新增 MySQL binary delivery、command 响应或新 streaming carrier，必须显式决定其 owner 与 phase，不能套用按帧重置时钟。
- 若改变 success visibility、驻留尾部或 Closing 模型，联合复评查询取消、物理完成与原 receipt 的交接证明；不能仅因超时频率改变 30 秒/5 秒责任边界。
