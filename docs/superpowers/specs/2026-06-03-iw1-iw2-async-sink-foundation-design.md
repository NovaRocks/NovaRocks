# IW-1 + IW-2 设计：执行资源边界 + 异步 Sink 算子契约

- 日期：2026-06-03
- 分支：`claude/iw1-iw2-async-sink-foundation`（基于 origin/main `fe8d7f4c`）
- 关联 Roadmap：`Iceberg Distributed Write Pipeline`（IW-1、IW-2）
- 关联遗留问题：D2 `INSERT OVERWRITE` 多 BE 调度饥饿 hang（`docs/superpowers/plans/2026-05-29-d2-insert-overwrite-multi-be-hang.md`）

> 语言策略：本设计文档用中文；代码标识符 / 类型名 / 日志 / 错误信息用英文。

---

## 1. 背景与目标

D2 遗留问题暴露的架构事实：共享的 `data_runtime`（multi-thread tokio runtime）既承担 connector / object-store / REST I/O，又被同步语句通过 `data_block_on`（`block_in_place` + `block_on`）驱动，在多 BE 写路径下形成调度饥饿，导致 `INSERT OVERWRITE` 偶发挂起。

长期解法（Roadmap `Iceberg Distributed Write Pipeline`）是把写路径改造成 StarRocks 式的 pipeline-native 异步写：driver 线程只推进 pipeline 状态，真正的阻塞 I/O 进入专用执行服务；sink 从"push 时同步做完"改成"push enqueue、finish 异步 drain、driver 用 `PendingFinish` 等待"。

本设计覆盖该长期解法的**两块地基**：

- **IW-1 执行资源边界与专用服务**：建立明确的执行服务分类（driver / scan I/O / sink I/O / metadata I/O / commit），让后续 sink / coordinator 不再直接拿全局 `data_runtime`。
- **IW-2 异步 sink 算子契约**：给 sink 一个"push 非阻塞、finish 异步、driver 协作式让出"的 pipeline contract，并提供可复用的通用包装算子。

本任务**不**实现真实 Iceberg writer（IW-3），**不**切换 INSERT 语义（IW-7/IW-8），**不**修 D2 hang 本身（在 IW-8 cutover 时随真实路径转绿）。

### 关键现状结论（已核对当前代码）

IW-2 所需的协作式调度地基**已经存在**，比 IW-2 文档假设的更完整：

- `DriverState`（`src/exec/pipeline/driver.rs:64`）已含全部 7 态，包括 `PendingFinish`（在 finishing 算子仍 pending 时设置，`:694`）。
- 阻塞 driver 是**事件驱动、非忙轮询**：parked 在 event scheduler 的 `blocked` map，由 `Observable` 回调重新入队（`src/exec/pipeline/schedule/observer.rs` 的 `do_update → scheduler.enqueue`）。`need_input()==false` → `BlockedReason::OutputFull` → park → 经 `sink_observable` 唤醒。
- 已有独立的 `src/exec/pipeline/blocked_driver_poller.rs`，对 `PendingFinish` driver 每 10ms `check_is_ready()` 并重新入队。
- `RuntimeErrorState::set_error()`（`src/runtime/runtime_state.rs:58`，首写者胜）是后台 → query 的错误通道。
- 现有 `IcebergTableSinkOperator`（`src/connector/iceberg/sink.rs:228`）在 `push_chunk` 内**同步**写 parquet，`set_finishing` 只置 flag —— 正是 IW-2 要替换的反面教材。
- `ProcessorOperator`（`src/exec/pipeline/operator.rs:100`）已提供 `need_input / has_output / push_chunk / pull_chunk / set_finishing / sink_observable / source_observable / precondition_dependency`；`Operator` 提供 `prepare / bind_runtime_state / close / cancel / is_finished / pending_finish`。

**结论**：IW-2 不依赖 E 系列（Pipeline Executor Concurrency）执行器重构——它需要的 driver 状态机、observable 唤醒、pending-finish poller 都已就位。IW-2 是给 sink 一个**使用**这些机制的契约；IW-1 是给后台 I/O 一个**不是共享 data_runtime** 的池子去跑。

---

## 2. 范围 / 非目标

### 2.1 范围

**IW-1**
- 新增进程级 `ExecutionServices` 单例（风格对齐现有 `data_runtime()` / `global_driver_executor()`），暴露 `execution_services()` 访问器。
- 统一句柄类型 `IoExecutor`，5 类服务共用；本支只把 `sink_io` 做成真正独立的专用 tokio runtime，`metadata_io / commit / scan_io` 先别名到 `data_runtime` 的 `Handle`（同类型，后续 IW-3/5/7 切真时调用方零改动）。
- per-service metrics（`IoExecutorMetrics`）：queue length / running / completed / wait time / errors。
- 接线入口：`RuntimeState` 暴露 `sink_io_executor()`（背后即全局单例），`query_context` / `StandaloneState` 同样暴露访问器。
- 新增配置段 `[runtime.execution_services]`，默认值不改变 all-in-one 功能行为。

**IW-2**
- 新增极小后端接口 `AsyncSinkBackend`（具体 sink 只实现 `write_chunk` / `finish`）。
- 新增通用包装算子 `AsyncSinkOperator<B>`，本身是 `ProcessorOperator`，承载全部 pipeline 契约（有界队列、后台 drain 任务、backpressure、async finish、错误传播）。**driver / pipeline builder 零改动。**
- 交付 `TestAsyncSink`（测试后端）+ 测试矩阵，证明契约成立。

### 2.2 非目标

- 不实现真实 Parquet / Iceberg writer（IW-3）。
- 不切换 INSERT / INSERT OVERWRITE 语义（IW-7 / IW-8）。
- 不重构现有 `data_block_on` 调用；现有所有 `block_on_iceberg` 路径与现有同步 sink **原样不动**。
- 不引入动态资源组调度。
- 不修复 D2 hang 本身（在 IW-8 cutover 随真实路径转绿；如需提前缓解，另起独立小 PR 做 interim 方向 A，本支不含）。
- 不做 E 系列执行器（MLFQ / 无锁队列）改造。

---

## 3. IW-1 架构：执行资源边界（`ExecutionServices`）

### 3.1 形态

进程级 `OnceLock` 单例，新增 `execution_services()` 访问器。它**不持有** driver_executor 的所有权，只引用现有 `global_driver_executor()`，避免改动调度器。

```text
execution_services()  (进程级 OnceLock 单例)
├── driver_executor   → 引用现有 global_driver_executor()              [不改动]
├── sink_io   : IoExecutor → 专用 tokio Runtime "novarocks-sink-io"    [本支真实独立]
├── metadata_io : IoExecutor → 别名 data_runtime().handle()            [本支别名]
├── commit      : IoExecutor → 别名 data_runtime().handle()            [本支别名]
└── scan_io     : IoExecutor → 别名 data_runtime().handle()            [本支别名]
```

### 3.2 `IoExecutor` 统一句柄

5 类共用一个类型，差别只在背后绑定的 runtime：

```rust
pub enum Spawner {
    Owned(Arc<tokio::runtime::Runtime>),   // sink_io：自带 runtime
    Borrowed(tokio::runtime::Handle),      // 其余：借 data_runtime 的 Handle
}

pub struct IoExecutor {
    kind: ExecutorKind,                    // Driver/ScanIo/SinkIo/MetadataIo/Commit（用于 metrics 标签/日志）
    spawner: Spawner,
    metrics: Arc<IoExecutorMetrics>,
}

impl IoExecutor {
    /// 统一提交入口；内部完成排队/起跑/完成/错误/等待时间记账。
    pub fn spawn<F>(&self, fut: F) -> tokio::task::JoinHandle<F::Output>
    where F: Future + Send + 'static, F::Output: Send + 'static;

    pub fn metrics(&self) -> IoExecutorMetricsSnapshot;
}
```

后续把某个服务"切真" = 把它的 `spawner` 从 `Borrowed(data_runtime)` 换成 `Owned(新 runtime)`，**调用方零改动**。

### 3.3 `sink_io` 专用 runtime

- `tokio::runtime::Builder::new_multi_thread()`，`enable_all()`。
- worker 数走配置 `sink_io_worker_threads`（默认 `min(4, available_parallelism)`）。
- `max_blocking_threads = sink_io_max_blocking_threads`（默认 16）。
- 线程名 `novarocks-sink-io`，栈 16 MiB（与 data_runtime 的 `WORKER_STACK_SIZE_BYTES` 对齐）。

### 3.4 per-service metrics

在 `IoExecutor::spawn` 的包装里记账，**不依赖 tokio 内部**：

```rust
pub struct IoExecutorMetrics {
    submitted: AtomicU64,        // 入队
    started: AtomicU64,          // 起跑
    completed: AtomicU64,        // 完成（含失败）
    errors: AtomicU64,           // 失败计数
    wait_time_ns_total: AtomicU64, // (start_ts - enqueue_ts) 累加
}
// 派生：queue_len = submitted - started; running = started - completed
```

`spawn` 内部：提交时 `submitted += 1` 并记 enqueue 时间戳；future 起跑时 `started += 1`、累加 wait_time；future 结束时 `completed += 1`（错误则 `errors += 1`）。`snapshot()` 供日志 / 后续 IW-10 观测接入。

> 规划阶段确认：若仓库已有统一 metrics 注册中心（profile/registry），把 `IoExecutorMetrics` 接进去；否则先以原子计数 + `snapshot()` 暴露，IW-10 再统一。

### 3.5 配置

```toml
[runtime.execution_services]
sink_io_worker_threads = 4          # 默认 min(4, cores)；仅多几条空闲线程，无功能影响
sink_io_max_blocking_threads = 16
# metadata_io / commit / scan_io 本支别名 data_runtime，暂不开 size 配置（注释标注 future）
```

默认值仅多出 `sink_io` 的几条（多为空闲）线程，**不改变 all-in-one 的功能行为**。

### 3.6 接线入口

- 算子拿 `sink_io` 用已存在的注入点 `Operator::bind_runtime_state(&mut self, state: &RuntimeState)`：`RuntimeState` 暴露 `sink_io_executor() -> IoExecutor`（背后即全局单例），`AsyncSinkOperator` 在 `bind_runtime_state` 时抓到 `sink_io` 句柄与错误通道。
- `query_context` / `StandaloneState` 同样暴露访问器，满足 IW-1"挂到明确入口"。

### 3.7 代码入口

`src/runtime/global_async_runtime.rs`、`src/runtime/runtime_state.rs`、`src/runtime/query_context.rs`、`src/common/app_config.rs`、`src/engine/mod.rs`（StandaloneState 访问器）。
参考 StarRocks：`be/src/runtime/exec_env.*`、`be/src/runtime/env/global_thread_pools.*`。

---

## 4. IW-2 异步 Sink 契约

### 4.1 两层划分

具体 sink 只实现极小后端接口；通用包装算子承载全部 pipeline 契约。

```rust
// 具体 sink 实现这个（IW-3 的 iceberg writer 之后实现；IW-2 只实现 TestAsyncSink）
pub trait AsyncSinkBackend: Send + 'static {
    type Output: Send;   // 写出结果（staged files / stats），喂给后续 IW-4 commit
    fn write_chunk(&mut self, chunk: Chunk) -> BoxFuture<'_, Result<(), String>>;
    fn finish(&mut self) -> BoxFuture<'_, Result<Self::Output, String>>;
}
```

> 备选：若项目 Rust 版本支持 RPITIT（return-position `impl Trait` in trait），可用 `impl Future` 替代 `BoxFuture` 省一次装箱；规划阶段按 MSRV 定。这里以 `BoxFuture` 为保守默认。

### 4.2 `AsyncSinkOperator<B>` 内部结构

```text
sender : Option<mpsc::Sender<Chunk>>        // 有界队列（容量=high_watermark，配置，默认 8）
shared : Arc<SinkShared> {
    observable : Arc<Observable>            // = sink_observable()，唤醒被 OutputFull park 的 driver
    queued     : AtomicUsize               // need_input 水位 + metrics
    finished   : AtomicBool                // 后台 drain + finish 全部完成
    errored    : AtomicBool
    result     : Mutex<Option<B::Output>>  // finish() 输出，供 take_output()
}
error_state : Arc<RuntimeErrorState>        // 后台失败写这里
sink_io     : IoExecutor                    // 来自 RuntimeState（IW-1）
join        : Option<JoinHandle<()>>        // 后台任务句柄，用于 cancel 时 abort
```

队列用 `tokio::sync::mpsc`：生产者 = driver 线程（同步可调 `try_send`），消费者 = 后台 async 任务（`recv().await`）。

### 4.3 契约方法映射（全部落在现有 `ProcessorOperator` 语义上）

| 方法 | 行为 |
|---|---|
| `bind_runtime_state(state)` | 抓 `state.sink_io_executor()` 与错误通道；在 `sink_io` 上 spawn 后台 drain 任务；建好 `sender` |
| `need_input()` | `!finishing && !errored && queued < cap` —— 队列满即 `false` → driver 进 `Blocked(OutputFull)` |
| `push_chunk(state, chunk)` | 仅 `queued += 1` + `sender.try_send(chunk)`（need_input 已 gate，单生产者不会 Full；防御性地把意外 Full 当 error）；**driver 线程不做 I/O** |
| `has_output()` | `false`（sink 不产出） |
| `set_finishing(state)` | `finishing = true`；`sender.take()` 丢弃 → 关闭 channel，通知后台"输入结束、开始 close" |
| `pending_finish()` | `finishing && !finished` —— 后台还在 drain / finish 时为 `true` → driver 进 `PendingFinish`，不占 worker |
| `is_finished()` | `finished` |
| `sink_observable()` | `Some(shared.observable.clone())` —— 队列腾空 / finish 完成时唤醒 driver |
| `cancel()` | 置 cancel；`sender.take()`；`join.abort()`。非阻塞 |
| `close()` | 到此后台已 `finished`，`take_output()` 取出 `B::Output`。非阻塞 |

### 4.4 后台 drain 任务（跑在 `sink_io` 专用 runtime）

```text
loop {
  match rx.recv().await {
    Some(chunk) => match backend.write_chunk(chunk).await {
                     Ok(())  => { queued -= 1; observable.notify(); }   // 腾空 → 唤醒 backpressure 中的 driver
                     Err(e)  => { fail(e); return; }
                   }
    None        => break;                                              // sender 被 set_finishing/cancel 丢弃
  }
}
match backend.finish().await {
  Ok(out) => { *result.lock() = Some(out); }
  Err(e)  => { error_state.set_error(e); errored = true; }
}
finished = true; observable.notify();

fn fail(e): error_state.set_error(e); errored = true; finished = true; observable.notify();
```

### 4.5 完整状态机 / 数据流

```text
driver 线程 (global_driver_executor)            sink_io 后台任务
  │ need_input()? ──true──> push_chunk ──try_send──▶ [有界队列] ──recv──▶ write_chunk().await
  │ need_input()? ──false(满)──> Blocked(OutputFull)                         │ 写完 queued--
  │        (park, 不占 worker) ◀───── observable.notify() ◀──────────────────┘ (腾空唤醒)
  │ 重新入队 → 继续 push …
  │ 上游 EOS → set_finishing() ─drop sender─▶ recv()=None
  │ pending_finish()==true → PendingFinish (park, 不占 worker)               backend.finish().await
  │        ◀─ blocked_driver_poller 每 10ms check_is_ready() ─┐             finished=true
  │ finished==true → DriverState::Finished                    └─(也可被 notify 提前唤醒)
  │ close() → take_output()
```

---

## 5. 错误处理与取消

**核心不变量**：driver 线程、`close()`、`Drop` **永远不阻塞等后台 I/O**——否则就把刚移走的阻塞又搬回来了。所有"等待"只通过 `PendingFinish`(park) + poller / observable 表达。

### 5.1 错误传播（保证"不丢错 + 不挂起"）

后台任一步失败 → `error_state.set_error(e)`（首写者胜）+ `errored = true` + `finished = true` + `observable.notify()`。三种 driver 姿态都能被叫醒并收敛到 `Failed`：

- park 在 `Blocked(OutputFull)` → `notify()` 重新入队 → 下一轮 `process()` 查 `runtime_state.error()` → `Failed`。
- park 在 `PendingFinish` → `finished = true` 使 `pending_finish()` 翻 `false` → poller 10ms 内 re-queue → `Failed`。
- 正在 `Running` → 本轮结束查 error → `Failed`。

`errored` 同时让 `need_input()` 返回 `false`，driver 不再 push，无死等。

### 5.2 取消语义

- `cancel()`：置 cancel 标志、`sender.take()` 丢弃、`join.abort()` 终止后台任务（in-flight 写被 drop）。**非阻塞返回**。
- 残留 staged 文件的清理是 **IW-6** 的范围，不在本任务；`TestAsyncSink` 取消后只需干净停住。
- `close()`：到此后台已 `finished`，`take_output()` 取出 `B::Output`（IW-2 测试读它，IW-4 coordinator 以后读它）。非阻塞。

### 5.3 边界情况

- 空输入（无 chunk 直接 finishing）：`recv()` 立即返回 `None` → `finish()` → `finished = true`，正常收敛。
- backpressure 正确性：`need_input` gating 保证 `try_send` 不会 Full（单生产者）；意外 Full 作为内部错误处理。

---

## 6. 配置汇总

```toml
[runtime.execution_services]
sink_io_worker_threads = 4
sink_io_max_blocking_threads = 16

[runtime.async_sink]
queue_capacity = 8          # AsyncSinkOperator 有界队列容量（high_watermark）
```

所有新增项均有保守默认值，不改变 all-in-one 功能行为。

---

## 7. 测试与验收

### 7.1 `TestAsyncSink` 后端（可配）

- 每 chunk 延迟（模拟慢 I/O）。
- 写入 gate / barrier（模拟 backpressure，可由测试控制何时放行）。
- 第 N chunk 失败（模拟后台错误）。
- 记录已写 chunk（断言顺序与完整性）。
- `Output` = 简单结构（如已写行数 / chunk 数）。

### 7.2 测试矩阵（IW-2）

1. **backpressure**：小队列 + gate 住后端 → 断言 driver 进 `Blocked(OutputFull)` 且不空转；放开 gate → drain、driver 恢复、chunk 顺序完整。
2. **async finish / pending_finish**：`finish()` 挂一会 → 断言 driver 进 `PendingFinish` 且不占 worker；finish 完成后到 `Finished`，`take_output()` 拿到结果。
3. **后台失败**：第 N chunk 失败 → `runtime_state.error()` 被置、driver → `Failed`、有界时间内无挂起（不丢错）。
4. **取消**：写到一半 `cancel()` → 后台被 abort、无挂起、driver → `Canceled`。
5. **不破坏同步 sink**：现有 pipeline / driver 测试与一小撮 sql 套件子集保持绿（新路径 opt-in，仅实现 `AsyncSinkBackend` 的算子才走包装）。

### 7.3 测试矩阵（IW-1）

- `execution_services()` 单例性（多线程取到同一实例）。
- 在 `sink_io` 上 spawn 的任务里断言 `std::thread::current().name()` 含 `novarocks-sink-io`（证明真离开了 data_runtime）。
- `IoExecutorMetrics` 计数随提交 / 起跑 / 完成推进、`wait_time_ns_total` 累加、错误计数。
- 别名服务（metadata_io / commit / scan_io）确与 data_runtime 同 `Handle`。

### 7.4 驱动测试的方式

优先复用现有 pipeline / driver 测试脚手架，构造 "test source → `AsyncSinkOperator<TestAsyncSink>`" 单 driver，断言 `DriverState` 迁移；若无现成脚手架则补一个最小的（规划阶段确认）。

### 7.5 验收对齐

**IW-1**（逐条覆盖）：代码可辨 5 类服务（`ExecutionServices`）/ 新增配置默认不改 all-in-one（小 sink_io 池 + 其余别名）/ metrics 可见队列长度与耗时（`IoExecutorMetrics`）/ 后续 sink / coordinator 只依赖服务句柄（`IoExecutor` via `RuntimeState`）。

**IW-2**（逐条覆盖）：测试 sink 模拟 async backpressure + async finish（测试 1、2）/ driver 在 pending finish 进 `PendingFinish` 且后台完成后重新调度 close（测试 2）/ 后台失败让 query 失败且不丢错误（测试 3）/ 不破坏现有同步 sink（测试 5）。

---

## 8. 与其他 Roadmap 的关系

- **E 系列（Pipeline Executor Concurrency）**：IW-2 不依赖它。E 系列改的是同一执行器的吞吐（MLFQ / 无锁队列），与 IW-2 的正确性正交；本设计沿用现有 `global_driver_executor` + `blocked_driver_poller` + `Observable`。
- **IW-3（Iceberg Async File Writer Sink）**：直接复用 `AsyncSinkBackend` + `AsyncSinkOperator<B>`，把 `TestAsyncSink` 换成真实 iceberg writer 后端，其 `finish()` 产出 staged files / stats 作为 `Output`。
- **IW-4（Distributed Write Coordinator）**：消费 `AsyncSinkOperator` 的 `Output`（writer result），做 BE → FE 结果上报与单点 commit。
- **IW-7 / IW-8（INSERT / INSERT OVERWRITE cutover）**：把 FE 侧同步 `iceberg_writer.rs` 路径改道到上述异步 sink；D2 hang 在 IW-8 转绿。

---

## 9. 风险与决策记录

- **决策：IW-1 走"最小真实切片"**（仅 `sink_io` 切真，其余别名）。理由：YAGNI——其余池子本支无消费者；同类型句柄保证后续切真零改动；最小化对 all-in-one 的线程影响。
- **决策：sink 契约用"Backend + 通用 wrapper"（方案 1）**，不新增 `AsyncSinkOperator` trait、不改 driver。理由：现有 `ProcessorOperator + DriverState + Observable` 已覆盖语义；wrapper 把易错契约收敛到一处；满足 IW-2"契约服务所有未来 async sink"。
- **风险：`BoxFuture` vs RPITIT** 取决于 MSRV，规划阶段定（不影响契约语义）。
- **风险：metrics 接入点**——若已有统一 registry 则接入，否则先原子计数，IW-10 收口。
- **风险：D2 hang 在本支不修**——若需提前缓解，另起独立小 PR 做 interim 方向 A（`data_block_on` 改 spawn + channel、去 `block_in_place`）。本支保持聚焦。

---

## 10. 预计文件改动清单

**IW-1**
- 新增 `src/runtime/execution_services.rs`（`ExecutionServices` / `IoExecutor` / `Spawner` / `IoExecutorMetrics` / `execution_services()`）。
- `src/runtime/global_async_runtime.rs`：复用 `data_runtime()` 句柄供别名服务。
- `src/runtime/runtime_state.rs`：暴露 `sink_io_executor()` 等访问器 + 错误通道访问。
- `src/runtime/query_context.rs`、`src/engine/mod.rs`（StandaloneState）：访问器。
- `src/common/app_config.rs`：`[runtime.execution_services]` + `[runtime.async_sink]` 配置项与默认值。

**IW-2**
- 新增 `src/exec/pipeline/async_sink.rs`（`AsyncSinkBackend` / `AsyncSinkOperator<B>` / `SinkShared`）。
- `src/exec/pipeline/operator.rs`：如需，补文档化契约说明（不改方法签名）。
- 测试：`TestAsyncSink` + 测试矩阵（位置随现有 pipeline/driver 测试脚手架）。

> 具体文件落点与拆分以 writing-plans 阶段的实现计划为准。
