# ADR-0038：Frontend 为何拥有 MV background worker lifecycle 与 per-target activity gate

- Status: accepted
- Date: 2026-08-03
- Decision Makers: NovaRocks maintainers
- Tags: frontend, materialized-view, lifecycle, table-maintenance
- Related: ADR-0009, ADR-0036, ADR-0037
- Provenance: discussion: 2026-08-03 frontend MV background lifecycle

## 背景与问题

异步物化视图除了用户显式 refresh 外，还需要按周期、按基表变更触发 refresh，并在 refresh 后执行过期、rewrite 和 optimize 等自动维护。历史实现把这类 worker 放在 Core，导致 Core 同时保存调度状态、启动 handle、错误重试解释和 statement dispatch；这使 durable MV attempt、query orchestration、backend topology 与 Frontend 已拥有的表维护 lifecycle 出现双 owner。

同一 canonical MV target 的手动 refresh、调度 refresh 和自动维护还可能并发运行。仅对其中一个入口限流会让 refresh receipt、metadata commit 或 durable optimize job 的完成顺序失去确定性；仅在提交时加锁也无法覆盖整个可恢复的维护 lifecycle。

## 决策

Frontend 拥有两个 sibling background worker：refresh scheduler 与 automatic maintenance coordinator，并拥有其 admission、bounded concurrency、取消、join、refresh-completed event 和关闭顺序。Core 只在 restore、recovery、provider binding 与表维护恢复完成后，向 Frontend 绑定 consumer-owned、provider-neutral 的 background engine port；该 port 只返回已准备的 refresh 步骤、snapshot vector、maintenance facts、live execution context 和 typed error，不能启动 worker 或保留 worker handle。

Frontend 以 canonical MV target 建立共享 FIFO activity gate。任意 refresh 或 automatic maintenance attempt 在完整的 reserve/prepare/execute/finalize 或 durable job 生命周期外层取得一个 ticket；等待 ticket 不占 worker permit，worker 停止时拒绝新 ticket 并以 `ServerShutdown` 取消自己拥有的 active attempt。空闲 gate entry 必须回收。

`ASYNC ON CHANGE` 只比较 provider 当前 snapshot vector 和 MV durable `last_refresh_snapshots`。scheduler 把 typed result 投影为明确 disposition；不从错误字符串或隐藏前缀推断 retry。只有既有 refresh finalize 能推进 durable watermark。

automatic expire、rewrite 和 optimize 必须进入既有 durable metadata-maintenance、distributed-rewrite、optimize-job lifecycle。一次 policy evaluation 及其全部 action 对同一 target 持有 maintenance permit 和 activity gate；不同 target 才可并行。停止时先关闭 MV admission、取消并 join 两个 MV worker，再停止表维护和释放 query/topology/StateStore。

## 后果

正面后果是 durable MV lifecycle、query cancellation、topology capture 和 StateStore 的释放顺序均由 Frontend 统一裁决；Core 保持 provider-neutral connector and execution kernel。per-target gate 使跨入口的完成顺序可解释，typed disposition 使 permanent、transient、recovery、target-gone 与 shutdown 不会被误写成普通 retry。

代价是 Frontend 需要维护 worker runtime、FIFO gate 与可测试的时钟/执行器，并通过窄 port 接收 Core 的 provider facts。automatic action 必须复用 durable lifecycle，因此不能以方便为由回退到 generic direct action。该决定不引入 durable schema、multi-FE lease/fencing、provider DTO 或 all-in-one 分支；这些仍是单独决策。

## 考虑过的替代方案

1. 保留 Core coordinator，只把结果 callback 给 Frontend。拒绝：worker admission、handle 和错误语义仍会成为第二个 owner。
2. 为每种入口单独 mutex 或只在提交时限流。拒绝：无法序列化完整 attempt/job，且无法保证 FIFO、取消与空 entry 回收。
3. 对 `ON CHANGE` 保存内存 observation watermark。拒绝：重启丢失基线，且首次 refresh 和恢复窗口语义错误。
4. 让 automatic action 调用 generic engine direct action。拒绝：绕过 durable intent/job、known/unknown reconciliation 和 restart recovery。

## 实施与验证

实现以 Frontend 的 `FrontendMvService`、application host 和 table-maintenance service 为 lifecycle owner；Core 的 MV background adapter 只实现 typed port。核心锚点：`novarocks/frontend/src/mv/service.rs` 的 refresh lifecycle、`novarocks/frontend/src/application.rs` 的 host shutdown、`novarocks/frontend/src/table_maintenance/mod.rs` 的 durable maintenance route，以及 `novarocks/core/src/mv/` 的 provider-neutral background port。

验证包括 deterministic FIFO/concurrency/cancellation tests、durable watermark/disposition/recovery tests、automatic expire/rewrite/optimize known/unknown tests，以及独立进程 1FE+3BE startup, catch-up, bounds, shutdown and restart matrix。source audit 必须确认 Core production 不再启动 MV workers、保留 observation watermark 或解析 scheduler error text。
