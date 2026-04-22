# Standalone Managed Lake Lifecycle Design

## 摘要

这份设计定义 standalone managed lake table 的第一阶段生命周期语义，目标是让 `DROP TABLE` 和 `TRUNCATE TABLE` 的逻辑与实现方向都对齐 StarRocks lake table：

- `DROP TABLE` 先让表从 active catalog 消失，再异步清理对象存储
- `TRUNCATE TABLE` 不做原地清空，而是走 partition replacement
- 物理删除不放在前台 DDL 请求里做，而是交给可重试的后台 erase worker
- 重启恢复以 SQLite 控制面为准，确保中间态可收敛

本设计建立在已有的 standalone managed lake metadata 方案之上，不考虑旧 local parquet managed table 的兼容性。

## 目标

- 为 standalone managed lake table 增加与 StarRocks 一致的生命周期主路径
- 让 `DROP TABLE` 在用户语义上立即生效，但不要求同步硬删对象存储
- 让 `TRUNCATE TABLE` 通过切换新 partition 达到“立刻变空”的效果
- 让对象存储清理具备异步、可重试、可恢复的执行语义
- 复用现有 managed snapshot、tablet bootstrap、txn recovery 基础

## 非目标

- 第一阶段不实现 `RECOVER TABLE`
- 第一阶段不实现 `DROP TABLE FORCE`
- 第一阶段不提供用户可见 recycle bin
- 第一阶段不实现复杂 GC/vacuum 策略
- 第一阶段不引入多分区用户语义；仍然只支持每表一个 active partition

## 背景与现状

当前 standalone managed lake 已经支持：

- `CREATE TABLE`
- `INSERT`
- `SELECT`
- 启动恢复和 `WRITTEN -> VISIBLE` reconcile

但生命周期仍然存在明显缺口：

- `DROP TABLE` 对 managed table 直接报不支持
- `TRUNCATE TABLE` 对 managed table 直接报不支持
- 当前状态模型只有 `Creating / Active / Failed`
- 当前路径模型以 `db_<id>/table_<id>` 为 shared root，不适合做 partition replacement 后的定向清理

这些限制会让 share-data 主路径虽然能跑通，但无法形成完整的表生命周期闭环。

## 设计原则

### 与 StarRocks 对齐的主语义

- `DROP TABLE` 的前台语义是 metadata removal，不是同步硬删对象存储
- `TRUNCATE TABLE` 的前台语义是 replace partition，不是 wipe existing tablet files
- 物理删除是后台异步动作，失败后应可重试

### 控制面优先保证“用户可见面单调收敛”

- 查询只看 active metadata
- 中间态对象不应重新暴露给用户
- 对象存储清理可以慢，但控制面可见性不能回摆

### 不追求跨 SQLite 与对象存储的强原子提交

第一阶段继续采用“可恢复状态机”而不是分布式两阶段提交：

- SQLite 负责控制面可见性与任务编排
- 对象存储负责 tablet metadata、rowset 和版本文件
- 启动恢复负责把中间态收敛到最终状态

## 数据模型变更

### 状态扩展

`ManagedTableState` 新增：

- `Dropping`

`ManagedPartitionState` 新增：

- `Retired`

`ManagedIndexState` 新增：

- `Retired`

状态语义：

- `Creating`
  - 控制面已分配对象，但对象存储初始化或切换尚未完成
- `Active`
  - 用户可见且参与查询、写入
- `Dropping`
  - 表已从 active 视图移除，等待后台 erase
- `Retired`
  - 旧 partition/index 已退出 active 视图，等待后台 erase
- `Failed`
  - 建表或 staging 失败，需要恢复逻辑清理或标记不可用

### 新增 `erase_jobs`

新增持久化表 `erase_jobs`，用于表达后台物理清理任务。

建议字段：

- `job_id`
- `job_kind`
  - `DROP_TABLE`
  - `DROP_PARTITION`
- `table_id`
- `partition_id`
  - `DROP_TABLE` 时可为空
- `root_path`
- `state`
  - `PENDING`
  - `RUNNING`
  - `FAILED`
  - `FINISHED`
- `retry_at_ms`
- `updated_at_ms`
- `last_error`

第一阶段不必把 retired metadata 立即编码进 `erase_jobs` 的 payload 后就删除原始 metadata。更稳妥的做法是保留 retired rows，等 erase 成功后再 purge。

### 路径模型调整

当前 managed lake root 以 table 为单位组织，不利于 `TRUNCATE TABLE` 的 partition replacement。

第一阶段建议把 shared root 从：

- `.../db_<db_id>/table_<table_id>`

调整为：

- `.../db_<db_id>/table_<table_id>/partition_<partition_id>`

这样可以保证：

- 同一个 active partition 内所有 tablet 仍然共享一个 root
- truncate 后新旧 partition 拥有独立 root
- `DROP_PARTITION` erase job 可以定向删除旧 partition 的对象存储内容

## Active Catalog 规则

内存中的 `ManagedLakeCatalog` 和逻辑 catalog 只暴露：

- `ManagedTableState::Active` 的表
- `ManagedPartitionState::Active` 的 partition
- `ManagedIndexState::Active` 的 index

以下对象绝不重新暴露给查询：

- `Dropping`
- `Retired`
- `Creating`
- `Failed`

这保证了 drop/truncate/recovery 期间用户可见面始终单调收敛。

## `DROP TABLE` 语义

### 用户可见行为

- `DROP TABLE` 成功返回后，表立刻从 catalog 消失
- 后续 `SELECT` / `INSERT` / `DESCRIBE` 应返回 unknown table
- 对象存储删除不阻塞前台请求

### 控制面事务

`DROP TABLE` 在一个 SQLite 事务中完成以下动作：

1. 校验该表不存在未终态事务
2. 将表状态改为 `Dropping`
3. 将该表下 active partition 改为 `Retired`
4. 将该表下 active index 改为 `Retired`
5. 插入一条 `DROP_TABLE` 类型的 `erase_job`

事务提交后：

- rebuilt catalog 不再注册这张表
- managed runtime 不再暴露这张表

### 物理删除

物理删除由后台 erase worker 处理：

- 删除该表对应的对象存储 root
- 成功后 purge retired metadata
- 失败后记录错误并回退重试

第一阶段不要求实现用户可见 recycle bin，但内部行为与 StarRocks 的“先摘 metadata，再异步删物理数据”保持一致。

## `TRUNCATE TABLE` 语义

### 用户可见行为

- `TRUNCATE TABLE` 成功返回后，表定义保留
- 查询立即看到空表
- 旧数据不会阻塞前台 DDL，但会在后台清理

### 为什么不用原地清空

原地清空现有 tablet 会带来几个问题：

- 需要直接破坏当前 active partition 的数据面状态
- SQLite 与对象存储之间的 crash window 更难恢复
- 与 StarRocks lake table 的 replace partition 语义不一致

因此第一阶段明确采用 partition replacement。

### 两阶段流程

`TRUNCATE TABLE` 分三步完成。

#### 1. Stage New Partition

先在一个 SQLite 事务中分配新的：

- `partition_id`
- `index_id`
- `tablet_id`

并插入：

- `Creating` 状态的新 partition
- `Creating` 状态的新 index
- 新 tablets rows

此时旧 active partition 仍保持可见。

#### 2. Bootstrap New Partition

事务提交后，调用现有 tablet bootstrap 路径为新 partition 创建空 tablets：

- 使用新的 `partition_<partition_id>` root
- 复用现有 `create_lake_tablet_from_req`
- 验证 tablet runtime 与初始 snapshot 一致

#### 3. Activate Replacement

在第二个 SQLite 事务中：

- 将新 partition/index 从 `Creating` 切为 `Active`
- 将旧 active partition/index 切为 `Retired`
- 插入一条 `DROP_PARTITION` 类型的 `erase_job`

事务提交后：

- 查询只看到新 partition
- 表立刻表现为空表
- 旧 partition 留给后台 erase

## Erase Worker

### 职责

新增一个最小后台模块，例如 `src/standalone/lake_erase.rs`，负责：

1. 拉取待执行的 `erase_jobs`
2. 删除对象存储对应 root
3. 更新 job 状态
4. 在删除成功后 purge retired metadata

### 执行模型

第一阶段不需要复杂调度框架。

推荐最小模型：

- `StandaloneNovaRocks::open()` 启动一个后台线程
- 周期性扫描 `PENDING/FAILED` 且到达重试时间的 job
- 每次只处理少量 job，避免长时间占用 metadata store

### 失败处理

删除失败时：

- `erase_job.state = FAILED`
- 写入 `last_error`
- 设置新的 `retry_at_ms`

启动恢复后，worker 应继续重试这些失败任务。

## 恢复与收敛

### 启动恢复原则

恢复时先按 SQLite 重建 active catalog，再处理中间态和后台任务：

- active metadata 先恢复查询可见面
- 中间态对象不暴露给查询
- 后续通过 reconcile 和 erase worker 完成收敛

### `DROP TABLE` 恢复

如果进程死在 `DROP TABLE` 提交之后：

- 表仍保持不可见
- 启动后重新扫描 `DROP_TABLE` erase job
- 后台继续物理清理

### `TRUNCATE TABLE` 恢复

如果进程死在 `Stage New Partition` 之后但 bootstrap 未完成：

- 会留下 `Creating` 状态的新 partition/index/tablets
- 恢复逻辑应检查对应 partition root 是否完整
- 若不完整，清理 staging root 和 staging metadata

如果进程死在 bootstrap 完成后但激活事务之前：

- 旧 partition 仍是 active
- 新 partition 仍是 `Creating`
- 恢复逻辑可以基于对象存储完整性决定重试激活或回滚 staging metadata

如果进程死在激活事务之后：

- 新 partition 已是 active
- 旧 partition 已是 retired
- 启动后只需恢复 `DROP_PARTITION` erase job 的继续执行

## 与现有模块的对接

### `src/standalone/store.rs`

需要承担：

- 新状态枚举的持久化
- `erase_jobs` schema 与 CRUD
- `DROP/TRUNCATE` 所需的 staged metadata 事务接口

### `src/standalone/lake_ddl.rs`

需要新增：

- managed `DROP TABLE`
- managed `TRUNCATE TABLE`
- truncate 的 staging/bootstrap/activate 三段控制流程

### `src/standalone/lake_recovery.rs`

需要新增：

- `Creating` partition 的启动期 reconcile
- `Dropping/Retired` 对象的不暴露规则
- erase job 的恢复调度入口

### `src/standalone/lake_txn.rs`

继续保持“只向 active partition 写入”的规则，不需要感知 retired partition。

### `src/standalone/engine.rs`

需要把 managed table 的 `DROP` / `TRUNCATE` 从“不支持”改为委派到 managed lifecycle 路径。

## 测试策略

第一阶段至少覆盖以下用例。

### `DROP TABLE`

- drop 后表立刻不可见
- 重启后表仍不可见
- erase job 成功后 retired metadata 被清理
- erase job 失败后可重试

### `TRUNCATE TABLE`

- truncate 后表 schema 保持不变
- truncate 后立刻查询为空
- truncate 前写入的数据在旧 partition root 中，直到 erase 完成才被删除
- 重启后 active partition 仍指向新 partition

### 崩溃恢复

针对以下窗口做故障注入：

- truncate stage 完成后、bootstrap 前
- bootstrap 完成后、activate 前
- drop metadata 事务提交后、erase worker 执行前

目标是验证重启后系统能收敛到唯一稳定状态。

## 分阶段边界

第一阶段完成标准：

- managed `DROP TABLE` 已可用
- managed `TRUNCATE TABLE` 已可用
- 后台 erase worker 已可运行并在重启后继续任务
- active catalog 不会重新暴露 retired/dropping objects

后续阶段再考虑：

- `RECOVER TABLE`
- `DROP TABLE FORCE`
- 用户可见 recycle bin
- 更复杂的 GC、vacuum、保留窗口和统计
