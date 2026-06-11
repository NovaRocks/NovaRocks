# IV3-11 · Iceberg MV 自动 Maintenance 调度器(Compaction policy / scheduler + retention)设计

- 日期:2026-06-10
- 来源:`NovaRocks TODO/IV3-11-compaction-scheduler-retention.md`
- 状态:设计已评审,待实现

## 1. 背景与问题

maintenance 的「动作」已全部落地且为手动触发:

- `run_expire_snapshots`(`src/connector/iceberg/commit/expire_snapshots.rs`)
- `run_rewrite_manifests`、`run_remove_orphan_files`、`run_rewrite_position_delete_files`(DV compaction,独立动作)
- OPTIMIZE(rewrite data files):已有异步执行半边 —— `ALTER TABLE ... OPTIMIZE` 在 SQLite 建 job,`spawn_optimize_worker`(`src/connector/iceberg/compact.rs`)后台 500ms 轮询领取执行

缺的是「什么时候自动做」:没有策略引擎和调度器;`history.expire.*` 表属性可设置、可持久化,但全代码库无消费方。

## 2. 调研结论(摘要)

### 业界产品对标

- **启用模型由表的所有权决定**:平台拥有表(Snowflake managed / S3 Tables / Databricks)→ 默认开(opt-out);只挂外部 catalog(Glue / Dremio / Lakekeeper)→ opt-in + 层级继承。表级逃生门是普遍标配。
- **触发信号收敛为两类**:compaction 用文件统计阈值(「小于 target 75% 算小文件」是事实标准,Glue 触发条件为文件数 >100 且 <75% target);retention 用时间周期。演进方向是 commit 事件驱动替代 cron 轮询(Lakekeeper/Tabular)。
- **默认值高度一致**(同源于 Iceberg 社区属性默认):target file size 512MB、snapshot 最大年龄 5 天、min-snapshots-to-keep 1。
- **安全护栏共识**:只动自己拥有的表;配置冲突宁可拒绝(S3 Tables 见到用户 branch/tag 直接 FAIL);连续失败 N 次熔断(Glue 4 次、Hive 2 次);自动必须配可审计。

### StarRocks 参考(`~/project/starrocks` lake compaction)

三层架构(CompactionMgr 状态持有 / CompactionScheduler 守护线程 / 可插拔 Selector+Sorter)、score 阈值触发(`lake_compaction_score_selector_min_score`)、`runningCompactions` 映射防重入、失败用时间延迟而非重试队列、环形历史队列可观测。已确认 StarRocks 对外部 Iceberg 表无任何自动 maintenance,可照抄的是调度骨架而非 Iceberg 策略层。

## 3. 范围决策

NovaRocks 可见两类 Iceberg 表,所有权不同,启用模型不同:

| 表类型 | 所有权 | 启用模型 |
|---|---|---|
| NovaRocks MV 存储表 | NovaRocks 是唯一 committer | **默认开**(managed 语义) |
| 外部 base 表(Spark 等产生) | 外部系统 | 表属性 opt-in,默认不碰 |

**v1 只做 MV 表(默认开)**。理由:候选集封闭(`mv_repo` 枚举,零发现成本);commit 事件免费(refresh 即 commit);痛点最大(refresh 持续产生 snapshot 与小文件);无越权风险。外部表 opt-in 留二期,届时只需给 Coordinator 新增一个事件源(snapshot-watch 轮询)与候选发现机制,策略引擎与执行通道全复用。

**v1 自动动作集** = EXPIRE SNAPSHOTS + OPTIMIZE + DV compaction。REWRITE MANIFESTS、REMOVE ORPHAN FILES 不进自动集(orphan 删除风险最高,业界均单独管理并设时间安全垫)。

### 非目标

- 不改各 maintenance commit-action 本身(继承自 IV3-11)
- 不做跨表全局资源调度(单表策略;全局仅 `max_concurrent` 并发上限)
- 不做外部 base 表纳管(二期)
- 不做系统表观测面(`SHOW MAINTENANCE` 等留后续;v1 为结构化日志)
- 不做阈值类参数的表级覆盖(v1 仅全局配置)

## 4. 架构(方案 C:Coordinator + 事件 + 兜底 tick)

```text
mv_flow::refresh_mv 成功提交 ──RefreshCompleted(mv_id)──┐
                                                        v
                                    MaintenanceCoordinator(独立线程)
                                    mpsc::recv_timeout(兜底 tick,默认 600s)
                                                        │
                                          读 mv_repo 取候选 + 防重入过滤
                                          (仅 Iceberg-backed MV)
                                                        │
                                          读表 metadata(snapshot summary)
                                                        v
                                    PolicyEngine(纯函数,无 IO)
                                    TableStats × Policy -> Vec<MaintenanceAction>
                                                        │
                    ┌───────────────────────────────────┼──────────────────────┐
                    v                                   v                      v
            EXPIRE SNAPSHOTS                    DV compaction              OPTIMIZE
        coordinator 直接 await               coordinator 直接 await      复用现有 SQLite job 队列
        run_expire_snapshots             run_rewrite_position_delete    (iceberg-optimize-worker)
```

候选方案对比:A(refresh 内联钩子)停刷表 retention 有尾巴、重操作阻塞 refresh、二期无法复用;B(纯周期 tick)滞后一个 tick 且每 tick 拉全量 metadata。C 以一个 mpsc channel 的成本获得 commit 驱动的即时性,兜底 tick 覆盖停刷表,与业界演进方向一致。

### 组件

1. **`MaintenanceCoordinator`**(新模块 `src/engine/mv_maintenance/`):独立 OS 线程,骨架照 `RefreshCoordinator`(`src/engine/mv_scheduler.rs`):mpsc stop channel、Handle `Drop` 时发停止信号并 join、`running_table_ids` 防重入、`max_concurrent` 默认 1。事件信号 v1 为无载荷的 `Wake`(refresh 成功后投递,突发事件合并)与 `Stop`;收到 Wake 或兜底 tick 超时后做一轮全量评估,逐表靠 snapshot 短路保持廉价 —— 与「携带 mv_id 的定向评估」语义等价,实现更简单。
2. **`MaintenancePolicy`**:策略合成 —— 全局默认(`[standalone_server]` 配置)+ 表属性覆盖(`history.expire.*`、`novarocks.maintenance.enabled`),产出解析好的 policy 结构。这是 `history.expire.*` 在代码库中的第一个消费者。
3. **`PolicyEngine`**:纯函数,输入 snapshot summary 统计 + 该表上次 maintenance 内存状态,输出动作列表。零 IO,可直接单测。
4. **执行通道**:EXPIRE 与 DV compaction 由 Coordinator 用现有 block-on 模式直接 await `run_*` 函数;OPTIMIZE 走现有 job 队列入口(同 `create_legacy_optimize_job` 路径),只提交不等待。
5. **事件接线**:`StandaloneState` 增加 `Option<mpsc::Sender<MaintenanceEvent>>`,server 启动 Coordinator 后注入;`mv_flow::refresh_mv` 成功路径末尾 `try_send`,发送失败静默忽略。投递点在 `refresh_mv` 公共路径而非 RefreshCoordinator,使**手动 REFRESH 与调度刷新行为一致**。测试/未启用时为 `None`,零开销。

### 不改动的部分

`RefreshCoordinator` 零改动;各 `run_*` commit-action 零改动;`mv_repo` schema 不加字段(maintenance 状态全内存,重启后首轮 tick 重新评估,动作幂等)。

## 5. 策略引擎

### 信号

全部来自当前 snapshot 的 summary(只读表 metadata JSON,零 manifest walk):`total-data-files`、`total-files-size`、`total-delete-files`;加 Coordinator 内存的「上次评估 snapshot id / 上次动作时间 / 连续失败计数」。「当前 snapshot id 与上次评估相同」时跳过 OPTIMIZE / DV compaction 评估(Dremio 模式);EXPIRE 每轮基于已加载的 metadata 纯计算评估(零额外 IO),仅当存在可过期 snapshot 时才执行动作——这保证停止刷新的表残留的 retention 尾巴仍会被兜底 tick 清掉。

### 触发规则与默认值

| 动作 | 触发条件 | 默认阈值 |
|---|---|---|
| EXPIRE SNAPSHOTS | 存在超龄 snapshot 且 snapshot 数 > min-snapshots-to-keep | `history.expire.max-snapshot-age-ms` 缺省 5 天;`history.expire.min-snapshots-to-keep` 缺省 1(Iceberg 社区默认) |
| OPTIMIZE | `total-data-files >= min_data_files` 且 平均文件大小 < `write.target-file-size-bytes` 的 75% | min_data_files 默认 100;target 缺省 512MB;75% 固定不暴露 |
| DV compaction | `total-delete-files >= min_delete_files` | 默认 10 |

平均文件大小 = `total-files-size / total-data-files`。EXPIRE 参数映射:`older_than_ms = now - max-snapshot-age-ms`,`retain_last = min-snapshots-to-keep`。

### 修正规则

1. **优先级抑制**:同轮 OPTIMIZE 触发则跳过 DV compaction(全表重写吸收 delete files)。EXPIRE 独立评估,不受抑制。
2. **下游增量链安全下界**:EXPIRE 前从 `mv_repo` 查所有以该表为 base 的增量 MV 的已消费 snapshot,`older_than_ms` 收紧为 `min(now - max_age, 最老未消费 snapshot 的 timestamp)`;下游消费状态未知时跳过 EXPIRE 并记日志。不改 `run_expire_snapshots`,只收紧参数。
3. **ref 守卫**:表存在非 main 的 branch/tag 时跳过自动 EXPIRE(记日志)。配置语义冲突宁可拒绝(S3 Tables 教训)。
4. **写放大冷却**:OPTIMIZE / DV compaction 每表冷却期默认 1 小时(当前 OPTIMIZE 仅支持全表重写,触发必须保守);EXPIRE 无冷却(snapshot 未变短路已天然限频)。no-op 结果(如纯 equality-delete 表的 DV compaction)不计失败但照常进入冷却。

### 执行顺序

每表一轮内:EXPIRE(轻)→ DV compaction → OPTIMIZE(提交 job),先轻后重。

## 6. 配置面

`[standalone_server]`(沿用 `mv_refresh_scheduler_*` 命名惯例,`src/common/app_config.rs`):

```toml
iceberg_maintenance_enabled = true             # 总开关;v1 决策:MV 表默认开
iceberg_maintenance_tick_interval_ms = 600000  # 兜底 tick,10 分钟
iceberg_maintenance_max_concurrent = 1
iceberg_maintenance_compaction_min_data_files = 100
iceberg_maintenance_dv_min_delete_files = 10
iceberg_maintenance_action_cooldown_ms = 3600000
iceberg_maintenance_max_consecutive_failures = 4   # 熔断阈值,对齐 Glue
```

表级覆盖收窄到两类:标准 Iceberg 属性 `history.expire.*`(覆盖 retention 窗口)+ 逃生门 `novarocks.maintenance.enabled = false`(单表关闭)。实现时需在 `src/connector/iceberg/catalog/schema_update.rs` 的 reserved-property 检查中将 `novarocks.maintenance.enabled` 加入白名单(当前 `novarocks.*` namespace 整体被拒绝)。

## 7. 执行、并发与失败处理

- **防重入两道闸**:Coordinator 内存 `running_table_ids`,处理中的表新事件直接丢弃(不积压;语义是幂等的「检查当前状态」,由下次事件或 tick 补);OPTIMIZE 提交前查 job 表,已有 pending/running job 则跳过。
- **失败退避与熔断**:失败后该 `(table, action)` 指数退避(60s 起 ×2,封顶 30 分钟);连续失败 4 次熔断该表该动作,`warn` 日志,仅停自动调度,手动命令不受影响;重启重置。失败分类与退避实现参考 `mv_scheduler` 现有代码。
- **OCC 冲突**:与 refresh 并发提交由 `run_*` 内部 `commit_with_retry` 处理;重试耗尽按普通可重试失败计数。
- **状态持久化**:无。失败计数、冷却、熔断全内存;动作幂等,重启后重新评估得出一致决策。

## 8. 可观测性(v1)

- 触发动作:`info` 日志,带表名、动作、触发原因具体数值(如 `data_files=152 avg_file_size=3.2MB target=512MB`)、结果(过期 snapshot 数 / job id)。
- 跳过决策:`debug` 日志,带原因(snapshot 未变 / 冷却 / ref 守卫 / 下游下界 / 熔断)。
- OPTIMIZE 结果复用 job 队列已有 outcome 记录(SQLite 可查)。
- 对应 IV3-11 验收标准「自动 maintenance 有明确的触发日志,可关闭」。

## 9. 边界情况

1. **replace snapshot 对下游增量 MV 必须是 no-op —— 实现的前置验证项**。代码调研确认该能力已存在:`src/connector/iceberg/changes.rs` 的 `classify_snapshot` 对 `Operation::Replace` 做验证后静默吸收(`validate_replace_snapshot` 校验 total-records 不变、added/deleted 文件数符合 compaction 形态、schema 未变),验证失败则增量刷新显式报错。实现计划第一步用端到端测试确认该链路对 OPTIMIZE 与 DV compaction 的产物成立;若测试失败则先修这个阻塞依赖。同文件的 `LineageBroken` 错误路径(previous snapshot 被 expire 后增量断链)印证了下游安全下界的必要性。
2. **MV 在 maintenance 中被 DROP**:动作失败记一次;候选来自 `mv_repo`,已删表自然消失,不会重试堆积。事件中 `mv_id` 加载失败静默跳过。
3. **非 Iceberg 存储的 MV**(managed-lake 后端):候选过滤阶段排除。
4. **纯 equality-delete 表**:`total-delete-files` 不区分 delete 类型,DV compaction 会 no-op;不计失败、照常冷却,防空转。
5. **自动与手动 maintenance 并发**:OPTIMIZE 有 job 表互斥;EXPIRE / DV compaction 靠 Iceberg OCC,后提交方重试或退避,无需额外协调。

## 10. 测试策略

1. **PolicyEngine 纯函数单测(主力)**:表驱动覆盖阈值边界、OPTIMIZE 抑制 DV、下游安全下界收紧、ref 守卫、冷却、熔断、snapshot 未变短路。
2. **Coordinator 生命周期测试**:启动/关闭/disabled、事件投递与运行中丢弃、job 表防重入;写法照 `mv_scheduler` 现有测试。
3. **端到端集成测试**(Rust 级,hadoop 本地 catalog 环境,直接调用 Coordinator 的评估入口注入 `now_ms`,完全确定性、无线程/计时依赖;场景与原 sql-tests 方案一致,放弃计时敏感的 sql-tests 形态):
   - ① 反复 refresh 积累小文件后自动 OPTIMIZE,文件数收敛(对应验收标准 1)
   - ② 配 `history.expire.*` 的 MV 自动过期旧 snapshot(对应验收标准 2)
   - ③ 关键回归:base MV + 下游增量 MV,自动 maintenance 开启后下游增量刷新结果正确、未触发全量回退
   - ④ `novarocks.maintenance.enabled=false` 逃生门生效(对应验收标准 3 的「可关闭」)

## 11. 二期展望(非本 spec 范围,仅记接口预留)

外部 base 表 opt-in:新增 `SnapshotWatch` 事件源(轮询外部表 current snapshot,复用 MV scheduler 的 snapshot-watch 模式)+ 候选发现机制(显式登记或周期枚举,届时另行设计);策略引擎、执行通道、防重入、熔断全部复用。
