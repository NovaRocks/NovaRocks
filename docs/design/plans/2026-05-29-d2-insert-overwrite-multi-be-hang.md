# D2 遗留问题：INSERT OVERWRITE 在 1FE+N≥2 BE 下挂起

- 状态：未修复（根因已钉死、修复方案已设计）
- 发现时间：2026-05-29（D2 多 BE 验收期）
- 影响范围：**写路径**（iceberg INSERT OVERWRITE）。D2 分布式**读**核心（SELECT/JOIN/聚合/HASH-shuffle/format-v3 读/分区读/INSERT INTO append）在 1FE+2BE 下已实测正确，**不受影响**。
- 关联分支：`claude/d2-multi-be-execution`

## 一、现象

`INSERT OVERWRITE <iceberg 表>` 在 `role=fe` 且 `[cluster].backends.len() >= 2` 时**挂起**（客户端 query_timeout 超时）；同一语句在 `backends.len() == 1` 时正常。读查询、`INSERT INTO`（append）在 N≥2 下均正常。

- runner（稳定复现）：`iceberg-rest --only iceberg_rest_insert_select --cluster-mode cross-process --cluster-size 2` → 超时失败；`--cluster-size 1` → 通过。
- 单条语句在隔离环境下 **flaky**（时好时坏）；完整 `insert_select` 12 条语句序列（单连接）在 2BE 下**稳定复现**。

## 二、复现步骤（手动最小复现）

1. 起 iceberg REST + MinIO fixture：`docker/iceberg-rest/up.sh`，`source docker/iceberg-rest/runtime/current/env.sh`。
2. 用同一个 base 配置（`$NOVAROCKS_STANDALONE_CONFIG`）生成 3 份配置：2 个 `role=be`（各自独立 http/starlet 端口），1 个 `role=fe`（`backends=[be0_starlet, be1_starlet]`、独立 mysql_port）。各进程 stdout 重定向到文件。
3. 起 be0、be1（等 `NOVAROCKS_READY role=be`），再起 fe（等 `NOVAROCKS_READY mysql_port=`）。
4. 连 fe 的 mysql 端口，在**同一连接**上按顺序执行（关键是序列 + 分区表 + OVERWRITE）：
   ```sql
   CREATE EXTERNAL CATALOG ice PROPERTIES('type'='iceberg','iceberg.catalog.type'='rest','uri'=..., 'warehouse'=..., 'aws.s3.*'=...);
   CREATE DATABASE ice.io_db;
   CREATE TABLE ice.io_db.t (id BIGINT, region STRING, amount DOUBLE) PARTITION BY (region);
   INSERT INTO ice.io_db.t VALUES (1,'us',10.5),(2,'us',20.0),(3,'eu',30.25);
   SELECT COUNT(*) FROM ice.io_db.t;                              -- 通过
   INSERT INTO ice.io_db.t SELECT id+100,region,amount*2 FROM ice.io_db.t WHERE id<=2;  -- 通过(RETURNING-OK)
   INSERT OVERWRITE ice.io_db.t VALUES (999,'ap',0.0),(998,'ap',1.0);                    -- 写提交成功(RETURNING-OK)
   SELECT COUNT(*) FROM ice.io_db.t;                              -- 此后某次 iceberg 读挂起 → 客户端超时
   ```
5. 现象：服务端日志显示 iceberg 写入+提交都成功（打到 `RETURNING-OK`），但客户端始终收不到响应而超时。

> 注：standalone-server 此前**完全没有日志输出**（tracing subscriber 从未初始化），已在本分支顺手修复（commit `6d5a5611`），上述诊断依赖该修复。

## 三、根因（已钉死）

挂起是**共享 `data_runtime`（固定 8 worker 的 tokio 运行时）上的 `block_on` / `block_in_place` 调度饥饿竞态**，不是 iceberg / scheduler / exchange 的逻辑 bug。

诊断证据（给 `src/runtime/global_async_runtime.rs::data_block_on` 加 ENTER/EXIT 配对插桩后跑稳定复现）：
- 恰好 **1 次** `data_block_on` "进入了但从未退出"：`path=A:block_in_place`、`in_runtime=true`、`thread=tokio-runtime-worker`。即某一次 `block_in_place(|| data_runtime.block_on(future))` 的 future 永远没被推进完成。
- 全部 ~300 次 `data_block_on` **都来自 `tokio-runtime-worker`，没有一次来自 `novarocks-data-runtime`（data 运行时自己的 worker）** → 排除"桥从自己的运行时里被递归调用"那种同运行时嵌套死锁。
- 前 290+ 次同样的调用都成功；只有在累积负载（OVERWRITE 密集序列 + 2BE）下偶发卡住 → **竞态指纹**（逻辑 bug 会稳定错；只有调度竞态才随负载时灵时不灵）。
- 挂起瞬间：8 个 `novarocks-data-runtime` + 8 个 `tokio-runtime-worker` 线程全部 parked；约 17 个并发 `block_on` 上下文。
- iceberg 写入/提交本身成功（`execute_iceberg_insert_or_overwrite` 打到 `RETURNING-OK`）；挂的是其后某次 iceberg 读的 `data_block_on`。

结构性病根：standalone FE 把**所有**阻塞 iceberg/对象存储/REST I/O 漏斗进**同一个固定 worker 数的 `data_runtime`**，并用 `block_on`+`block_in_place` 从 server-worker 线程驱动；`block_on` 会**占住** reactor 的 worker。并发的 `block_on` 持有者一多，运行时就无法把某一个 future 推进完成 → 永久 park。

相关代码：
- `src/runtime/global_async_runtime.rs:70` `data_block_on`（`Handle::try_current().is_ok()` → Path A `block_in_place`）
- `src/connector/iceberg/catalog/registry.rs:1568` `block_on_iceberg` = `data_block_on`
- `src/engine/iceberg_writer.rs:53` `execute_iceberg_insert_or_overwrite`（多次 `block_on_iceberg`：load_table / write / commit）
- `src/server/mod.rs:998` 语句经 `task::spawn_blocking(session.execute_in_context)` 执行

## 四、修复思路

参考 StarRocks 的原则（见 `be/src/runtime/exec_env.h` 的多专用线程池、`be/src/exec/workgroup/scan_executor.h` 的 ScanExecutor、`be/src/exec/pipeline/pipeline_driver.h` 的 `PENDING_FINISH` 协作式让出、`be/src/runtime/exec_env.cpp:803` 的 `lake_metadata_fetch_thread_pool`）：

> **执行/reactor 的 worker 线程永远不被阻塞 I/O 占用；阻塞 I/O 永远在"为阻塞而生"的专用池里做（且按用途分池）。**

三个修复方向（由轻到重）：

- **方向 A（近期推荐）**：把"阻塞等待"放回 tokio 的阻塞池，别占 reactor worker。用 `spawn` + channel 取代 `block_on`：future 用 `data_runtime.spawn` 交给运行时**公平调度**（成为普通任务，不占死 worker），同步侧在 `spawn_blocking` 阻塞线程上只等一个 channel（`oneshot::blocking_recv`），**去掉 `block_in_place`**。配套：(1) 加守卫 `debug_assert!` 桥不可从 data-runtime worker 调用；(2) 收敛成唯一的 `run_blocking_async()`，禁止别处裸用 `block_on`/`block_in_place`/`data_block_on`。改动集中在 `data_block_on` 一处，从根上消除这一类饥饿。
- **方向 B（中期加固）**：按用途给"data 类工作"分池（如把 FE 侧 iceberg 元数据/提交桥接独立出专用阻塞池，类比 StarRocks `lake_metadata_fetch_thread_pool`），与 scan-range/exchange I/O 分开，互不饿死。
- **方向 C（终局架构）**：执行路径协作式让出，执行线程永不阻塞 I/O（照搬 StarRocks pipeline 的 `PENDING_FINISH` + `ScanExecutor`）。大重构，作为路线图方向。

验收门槛（修复后必过）：
1. 稳定复现转绿：`iceberg-rest --only iceberg_rest_insert_select --cluster-mode cross-process --cluster-size 2`。
2. 零回归：all-in-one `cargo test --test cluster_mvp`(D1 6/6)、standalone 单测、all-in-one iceberg DML 抽查、iceberg-rest 整套不回退。
3. 因 `data_block_on` 是全局共享原语，跑一遍读类性能抽查，确认无明显回退。
