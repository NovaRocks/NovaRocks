---
id: ADR-0231
title: "Maintenance and statistics belong to their original statement, with typed state surfaces"
domain: [table-maintenance, provider-spi, sql-language, frontend-mv]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0169
    scope: "StatementJob/JobAttempt admission and catalog-dependent Detached/AwaitTerminal completion, job-based completion and cancellation in decisions 4-6; owner admission, HMS read-only and publication separation remain"
  - id: ADR-0111
    scope: "Permission to retain maintenance/statistics job queues, workers, histories and observer-only cancellation in decisions 1 and 3; crash-only publication, activity gates and GC Accelerator remain"
  - id: ADR-0226
    scope: "Decision 11 retention of SHOW MATERIALIZED VIEWS/BACKENDS/TABLE STATS and maintenance/statistics job SHOW surfaces; typed system relations and MV management procedures remain"
date: 2026-11-13
provenance:
  - "discussion: 2026-11-11 accepted statement-owned maintenance and statistics, Trino statistics and typed MV state migration"
code-anchors:
  - "novarocks/frontend-application/src/query_execution/maintenance/rewrite_job/direct.rs (DirectRewriteAction, prepare_optimize)"
  - "novarocks/frontend-application/src/statistics/attempt_executor/statement.rs (StatementStatisticsAction, StatisticsStatementExecutor)"
  - "novarocks/frontend-application/src/statistics/show_stats.rs (render_show_stats)"
  - "novarocks/frontend-application/src/system_tables/provider.rs (FrontendSystemTableProvider)"
  - "novarocks/frontend-application/src/metrics/management.rs (frontend_backends)"
---

## 问题

维护与统计是否需要独立于提交语句的进程内作业系统；语句完成、取消以及产品状态应由谁拥有和对外表达？

## 背景与执行事实

Root、查询许可、冻结会话设置、Provider Source 和实际退出属于执行责任；Catalog snapshot、统计文件与 MV 文档属于外部业务事实。后台 job 的接受或终态既不是原语句完成，也不是湖上发布证明。按 catalog 区分 Detached 与 AwaitTerminal 会使同一条 SQL 在不同 catalog 上具有不同生命周期。

| 入口 | 原作者与结果 | 生命周期 |
| --- | --- | --- |
| `ALTER TABLE t OPTIMIZE` | `DirectRewriteAction` 复用已有数据重写作者，返回三行指标 | 同一语句、表级 Optimize activity、原 Native/Provider/CPU Source |
| `ANALYZE TABLE t` | `StatementStatisticsAction` 捕获、聚合并发布；一列 `rows bigint`，值为 NULL | 同一 Internal Root、查询许可、冻结会话设置与首次完整 binding |
| `SHOW STATS FOR t` | Iceberg 提供同一元数据观察下的统计事实，FE 归一化并呈现七列 | 原语句只读 Source；不创建采集或发布工作 |
| `system.runtime.materialized_view_states` | MV 产品配置、发布记录、manageability 与依赖事实 | 复用原系统表快照、结果预算与实际退出 |
| `GET /v1/frontend/backends` | 原 topology owner 的类型化行 | FE management HTTP 只读投影 |

计算就绪、必要业务就绪、publication outcome 与原资源实际退出是独立事实。客户端仍须请求 Trino 终页确认，结果数据不能代替退出证明。GC first-observation 仅证明准确外部元组第一次被观察的时间，仍是可重建 Accelerator，不能成为执行或恢复 authority。

## 考虑过的选项

1. **保留进程内 job、串行 worker 和 SHOW 轮询。设计否决。** 可沿用原队列与历史，但产生第二个 Root、许可移交及取消 owner；用户需要两套完成协议，同一 FE 的无关表还被串行排队。普通维护与统计已有可复用的 governed Action，没有独立作业产品合同需要保留。
2. **只让单写者 catalog 等待，其余 catalog 分离执行。设计否决。** 可减少行为变化，却让 catalog 决定 SQL 生命周期，并保留 StatementJob/JobAttempt 与两套完成政策。catalog 应回答本次操作能否执行，不应替 SQL 定义作业产品。
3. **同一语句执行，复用原 Source 与原领域事实。采纳。** 取消、发布和资源退出保留原 owner；状态查询分别使用标准统计、系统关系与管理 API。代价是维护和统计成为长语句，客户端必须持续消费或明确取消。
4. **提供外部持久作业平台。待评估。** 它可能满足跨重启调度、进度和只读历史需求，但需要单独的身份、权限与生命周期合同，且不得反向授予旧未知提交的恢复权。本裁决没有实现该产品。

## 裁决

1. **原语句所有权规则。** OPTIMIZE 与 ANALYZE 从准入到实际退出使用原语句 Root、查询许可与冻结会话设置；不建立第二 job、队列、worker、历史或许可移交。普通 SQL 的 Connector 发起方式为 Statement；不得为了复用旧实现伪造 JobAttempt。不同表可并行，OPTIMIZE 保留原同表 activity；ANALYZE 不新增同表互斥，提交冲突由原 Provider 判断。
2. **完整捕获规则。** ANALYZE 首次捕获的表句柄、统计版本与 schema 原样交给规划、采集和发布，不按名称二次解析。OPTIMIZE 的物理对象身份贯穿规划、cohort 与提交；替换以 typed TARGET_REPLACED 失败，规划前发现替换时不派发工作。所有 mutation 仍须在首次副作用前由唯一 catalog owner 准入；HMS 永久只读，Hadoop 单写者前提不变。
3. **原退出规则。** 取消首先阻止未派发工作，再由原 owner 取消、关闭并观察 Native、Provider、CPU、存储与结果责任的真实退出；取消借用 future 不销毁 Source。OPTIMIZE activity 在真实 close 与 quiescence 后释放。错误、raw health、取消意图与退出分别保留，不用终态、超时或停止等待替代实际退出。
4. **原发布规则。** 必要业务成功与 KnownCommitted、KnownUncommitted、CommitUnknown 继续由原 Provider 事实决定。未知提交不自动重试、回滚或跨重启恢复。空表 ANALYZE 在 Provider 给出空 requirements 和 NoOp publication 时成功完成，不生成 Puffin、不制造 snapshot；逻辑 NoOp 不是一次真实发布。collect-on-write 保留原数据流与发布作者。
5. **选择与指标规则。** OPTIMIZE 使用默认 100MB 阈值：无删除且大于阈值的文件跳过；当前 spec 的单个无删除候选分区跳过；分区有多个候选或删除时重写该分区候选；存在旧 spec 候选则重写全部候选。只移除选中数据及其挂靠删除，保留未选条目，summary 按父快照累计。零组返回三项 0 且不提交；非零结果取实际 receipt，未知 added count 保持 NULL。固定三行依次为 rewritten_data_files_count、removed_delete_files_count、added_data_files_count，updateType 为 ALTER TABLE EXECUTE，无 updateCount。RefUnchanged 的现有冲突判定保留。扫描保留分布式输入，重写 writer 使用 Gather 与单 driver；原 Iceberg 分区 writer 跨输入 batch 保持至 finish，沿现有 Parquet 滚动与 journal 清理，不把批次边界误当文件边界，也不缓存整份输入。
6. **统计呈现规则。** SHOW STATS FOR 只支持有真实统计来源的 Iceberg 基表；查询、view、MV、系统表与其他 Connector 明确 NOT_SUPPORTED，不存在的表为 TABLE_NOT_FOUND。七列按 Trino 顺序保留 column_name、data_size、distinct_values_count、nulls_fraction、row_count、low_value、high_value；每个可见列一行，末尾汇总行只填 row_count。可选 manifest 指标缺失是合法输入；按原读取事实与 Trino 归一化处理，未知不猜值。Puffin ndv 为十进制非负整数属性，sketch 本体不变。
7. **状态分工规则。** MV 状态表与标准 MV 清单取相同行集合；按显式 catalog/schema/name 键剪枝，不隐式绑定当前会话。manageability 与 reason 分列；last_refresh_time 是 publication_prepared_at_ms，不伪称提交完成时间。只在投影需要且原 MvListedManageability 为 Manageable 时读依赖；ReadOnly/Unavailable 时 upstream_materialized_views 为 NULL。ManagementEntrance 的 phase 只影响展示状态，不反向改变原读取资格。BE 状态只从 management API 读取。旧 SHOW MATERIALIZED VIEWS、SHOW BACKENDS、SHOW TABLE STATS、SHOW ANALYZE JOBS 与 SHOW ALTER TABLE OPTIMIZE 退出语句集合，不保留兼容解析或服务；MV 管理过程保留。
8. **真实容量规则。** 系统表与重写产物沿原结果预算、背压、分页、yield 与取消治理。100MB 是文件选择语义；不以本地载体方便为由恢复无格式或测量依据的 4096 项、4MiB 等总量门槛。共享分页和 canonical 校验仍保留，读取规模与完整快照责任不能因去掉硬上限而隐去。

## 接受的妥协（诚实记录）

- 客户端需要等待长语句并持续取页；取消可能仍需等待真实外部退出。删除 job 历史意味着不再提供进程内进度与重启后的轮询方式。
- OPTIMIZE 的当前提交仍用 RefUnchanged，不能像 Trino 的 RewriteFiles 校验那样容许规划后的并发追加；选择规则对齐不表示提交内核完全相同。旧单文件场景现在可能合理地成为零重写。
- SHOW STATS FOR 暂不为任意查询或无真实来源的对象提供优化器估计。状态表也不猜下次刷新、调度器错误或未知发布时间；这些需要原领域新增事实。
- 去掉任意载体总量门槛不代表无限资源。整份系统快照和合法重写产物继续承担实际内存与结果责任，资源不足由原预算作者 typed 拒绝。
- 本 ADR 记录已接受并落地的所有权合同；组件、Native、SQL/System 与最终全工作区验证分别取证，不能由文档或局部测试推导最终准入。

## 何时重新评估

- 用户需要跨重启调度、进度或历史时，评估选项四的独立外部产品；只读历史不得成为旧 CommitUnknown 的重试、接管或恢复权威。
- 提交内核提供具名的选中集合校验与并发追加证明后，再评估放宽 OPTIMIZE 的 RefUnchanged，不在 FE 旁路提交验证。
- 新增真实统计来源或调度器持久事实时，由各原 owner 扩展 SHOW STATS FOR 支持范围或 MV 状态列，不以默认值补齐。
- 实测显示完整快照或合法重写产物超出既有预算能力时，评估原 owner 的流式边界和容量布局；不得以无依据固定总量替代测量。
