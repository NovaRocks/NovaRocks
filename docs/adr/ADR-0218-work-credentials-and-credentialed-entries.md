---
id: ADR-0218
title: "Work credentials stay with join owners and execution starts through credentialed entries"
domain: [pipeline-execution, distributed-query-lifecycle, crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-11-06
provenance:
  - "discussion: 2026-11-06 accepted work credentials, credentialed execution entries and actual-exit boundaries"
related:
  - "ADR-0211-deterministic-control-runtime-and-task-ownership.md"
  - "ADR-0216-execution-vocabulary-participation-task-and-handles.md"
  - "ADR-0194-strict-clippy-ci-baseline.md"
code-anchors:
  - "novarocks/control-runtime/src/exit_work.rs (run_future, run_blocking, PanicSink)"
  - "novarocks/control-runtime/src/exit_custody.rs (ExitCustody, ExitObligation)"
  - "novarocks/worker/src/control_runtime/participation_host/work_issuer.rs (ParticipationWorkRegistry, ParticipationInstanceGrant, InstanceWorkIssuer)"
  - "novarocks/worker/src/host.rs (TaskPreparationContext, TaskScanSourceIssuer, PreparationControlLoan)"
  - "novarocks/execution/src/runtime/query_context.rs (QueryContext, QueryTaskGuard)"
  - "novarocks/native-adapter/src/backend_task_execution/participation_host.rs (NativeParticipationInstance)"
  - "novarocks/native-adapter/src/lib.rs (BackendDataRuntime::spawn_with_obligation)"
  - "novarocks/native-adapter/src/exchange_transmitter.rs (spawn_exchange_call)"
  - "clippy.toml (disallowed-methods, disallowed-types)"
---

## 问题

查询工作怎样在现有执行器上启动和派生，才能让负责等待退出的对象始终保管原义务，并使取消、终态、捕获析构与资源退休各有准确证明？

## 背景与执行事实

Task 终态说明协议结论，不能说明 driver、IO actor、发送活动或资源别名已经退出。反过来，一个 future 完成也不能说明它的报告已经被每个消费者消费、工作 header 已退休或 Scope 已移除。查询工作同时使用 driver 池、IO runtime、阻塞池和协议 actor；它们是载体，不因能够 spawn 就获得查询工作权限。

沿用 [ADR-0211](ADR-0211-deterministic-control-runtime-and-task-ownership.md) 的原工作账本与 [ADR-0216](ADR-0216-execution-vocabulary-participation-task-and-handles.md) 的词汇，工作凭证是已准入的 `WorkObligation`。跨执行边界移动同一原件时，`ExitObligation` 随工作持有，`ExitCustody` 留在负责 join、消费报告与退休的 owner；不再增加一套凭证树或收敛计数。

| 对象或入口 | 提供的事实与责任 | 不提供的保证 |
|---|---|---|
| `run_future` / `run_blocking` | 保管原 `ExitObligation`；开始前经过原启动门，实际销毁本工作及 captures 后报告退出；原 panic 留在 `PanicSink` | 不替第三方内部独立作业或移交出去的返回值证明退出 |
| `ParticipationWorkRegistry` | 从原 LIFETIME 为实例派生工作；看管原报告、panic、header 与父子退休关系 | 不向 Native 暴露通用派生能力或任意身份入口 |
| `ParticipationInstanceGrant` / `InstanceWorkIssuer` | 原 Participation 的实例义务及窄 RF sender 签发入口；sender 子工作由原实例派生 | 不从一个 cloneable host/Task 句柄得到无限工作权限 |
| `TaskPreparationContext` / `TaskScanSourceIssuer` | Worker 依据原 Task 记录签发控制令牌、scan operations 根与范围令牌 | 不给执行宿主任意自填 query、attempt、Task 身份的生产构造路径 |
| `AdmissionProof` | `WorkChildAuthority::verify_live` 对原已绑定父项的一次存活核对 | 不持账本、不延长工作、不授予容量/派生权，不证明未来存活或应用身份 |
| `QueryContext` / `QueryTaskGuard` | 原参与实例建立查询资源；Task 持原上下文的真实引用，guard 随 Task 资源退出 | active_tasks 只是实际 guard 数，不是协议寿命、查找路由或独立回收条件 |
| `ExchangeCallActivity` / sink IO live | 原 join owner 对自己确实等待的发送或 actor 活动计数 | 不另给每个 driver/actor 派生全局工作凭证 |

`NativeParticipationInstance` 保管安装的查询资源和原实例义务。正常 Release 关闭本实例资源，最后一个受看管实例引用实际析构后才报告实例退出；RF sender 的工作独立等待其 future/captures 退出与原报告退休。Task 的 `QueryTaskGuard` 从准备上下文移入 `LocalTaskResources` / `RuntimeTaskResources`，后者先确认其它资源 cell 退出，再真实销毁 guard。Task 的 `CompletionExited` 与参与的 `ExecutionCustodyRetired` 保持原作者：前者不能先于真实本地资源退出，后者以原 Scope 实际移除为前提。

准备期的 native 子对象、回调和已接收结果仍由原 preparation 工作看管。body 结束不等于已登记子对象退出；它们的 `poll_exit`、实际 Arc/capture 析构与 report/header 退休分别取证。正常拒绝、未启动撤回与取消不会凭空制造 Unknown。

## 考虑过的选项

**给每个 driver、IO actor、range 子请求都新增全局工作凭证：成本否决。** 它提供更细的统一诊断，但 owner 已有准确 join 时，会引入重复账本转换、锁访问与生命周期接线。当前只给没有其它 owner 会 join 的分离工作派生子义务；原 owner 的 join 仍是下层退出权威。若之后确需逐单元诊断，应先证明收益超过接线和热路径成本，不能把两个计数都当退休权威。

**只靠 Task 身份、取消令牌、Drop 或协议终态推断全部工作退出：设计否决。** 身份不签发容量，取消是停止请求，终态也可能先于异步工作退出。析构没有覆盖到的阻塞作业和捕获仍可能持有资源；因此不能据此清账、移除 Scope 或退款。

**执行宿主自行建立账本、填写身份并构造控制/scan 令牌：设计否决。** 它把身份权威移出 Worker，使看似同号的 Task 与原准入失去关系。采用 Worker 的原 Task 记录与已核对的派生能力签发窄令牌，执行宿主只消费。

**新建通用 spawn 包装平台或依赖库 fork 来捕获所有内部任务：待评估。** 这能扩大监督范围，但跨第三方任务语义、取消和内部资源转移的成本尚未定。当前保持既有载体与上游注入点，不以包装 NovaRocks future 冒充第三方全部实际退出。

**义务留在 join owner，窄入口带凭证执行，根 lint 阻止误用：采纳。** 保留已有 driver、actor、range、IO 的真实 join；实例与 RF sender 等无其它 join 的工作移动原 `ExitObligation`。生产入口要求 Worker 签发令牌或原义务，底层 carrier 的裸调用只有逐处带理由的例外。

## 裁决

1. **原件与看管规则。** 凭证落在负责 join 的 owner，沿用 `ExitCustody` / `ExitObligation`，不克隆必须消费的义务。工作开始、body/captures 退出、报告消费、header 退休与回收分别推进；看管方负责完成原退休和 reap。由 owner 确实 join 的下层单元不再新增一套全局权威。

2. **签发与窄入口规则。** Worker 按原 Participation/Task 记录签发实例 grant、RF sender 子义务、`TaskExecutionControl`、scan operations 根与范围令牌；构造阶段核对原工作仍可用。`FileRangeScope` 只从 Connector 范围令牌得到，发送活动只由原 sender 创建。执行侧不拿通用子工作 authority、自填身份或 raw runtime/submit 接口。该保证防架构误用，不是对故意自建账本的安全隔离；一次 `AdmissionProof` 不代替每次原 Scope 启动门、停止检查或预算准入。

3. **真实析构规则。** 包装器先销毁本工作及 captures，再报告退出；未启动就撤回 unused 并退出，已启动则经原义务报告。取消/abort 只有在载体真正析构 future 时才成为退出，不能提前扣 live。阻塞闭包一旦运行，必须等返回或 unwind；停止等待不等于停止工作。显式 capture 析构 panic 无法确认退出，保留未解决与原 panic，不能在剩余字段自动 Drop 时误报成功。

4. **边界移交规则。** “析构即退出”只覆盖被包装的 NovaRocks 工作项及其 captures。返回值交给接收者后，其寿命属于接收者；缓存异步填充等明确服务移交的 `Bytes` 继续归服务，查询退出不说明其内存已释放。`tokio::fs`、opendal 文件后端等依赖内部阻塞池副作业可能在外层 future 析构后继续完成，不在此证明内。第三方连接池任务属于连接池服务，不能按查询 future 的退出直接归零。

5. **实例与资源顺序规则。** Native 不能将实例 Arc 克隆到不受看管的持有者。M/R/advance 等调用保管自己的实际 clone，发布完成不代替 clone 析构。运行中的 Task guard 与 QueryContext 引用必须在 `CompletionExited` 前实际放下，不能依赖 terminal callback 或等待最终实例析构。参与仅在子工作/报告/header 真实退休并移除执行 Scope 后交 `ExecutionCustodyRetired`；active_tasks、超时与健康标志都不能替代这两个事实。

6. **关闭与未解决规则。** RF 本地分发读栅栏覆盖当前 callback；close 取写栅栏等待当前分发退出，随后拒绝新分发并请求 sender 停止，abort 只兜底。sink IO 关闭先封新准入再等真实 live actor，原期限只界定等待。正常取消、未执行即销毁和已知业务拒绝不得制造未解决；真实失联、capture 析构异常等不能证明退出的缺陷保留责任、阻止 Scope 剥离并进入 `ExecutionHealth` 的进程故障路径，不新增重试次数或容忍阈值。

7. **默认禁止与例外规则。** 根 `clippy.toml` 是禁止启动 API 清单的唯一出处；不在 ADR 复制易漂移的 API 列表。受覆盖 crate 的新增裸启动由严格 Clippy 拒绝。服务 carrier 或已带凭证的私有提交点使用最小位置、带 `reason` 的 `#[expect]`，遵循 [ADR-0194](ADR-0194-strict-clippy-ci-baseline.md)，不加共享 crate 全局 allow。Clippy 只读最近配置、不合并；现有 FE 专用与测试基础设施以自身配置明确豁免，任何 BE production 消费、职责变化都要求重新核对，不能把豁免迁移到新共享 crate。FE 工作接入共同监督时逐 crate 收回豁免。

8. **第三方覆盖诚实规则。** trait executor 是否被当前 Clippy 路径准确命中，必须以真实 probe 判定；`allow-invalid = true` 仅容许 feature-gated/不存在路径，不证明拦截。`hyper::rt::Executor::execute` 与 `opendal::Execute::execute` 已在第一方生产编译 probe 中准确触发禁令。Native 向 Hyper HTTP/2 builder 注入 `TokioExecutor` 时，H2 task 的启动发生在依赖内部，仍是没有逐查询凭证监督的已知边界，第一方显式调用禁令不递归检查它。未来调整 H2 executor、启用 opendal 并发或 iceberg-rust 分离 spawn 进入 BE 读取时，先重新审计原 join 边界并使用上游 executor 注入点；opendal 有 `Executor::with`，iceberg-rust Runtime 当前只接 handle，不能把 handle 注入称为逐工作监督。

## 接受的妥协（诚实记录）

每个 join owner 负责下层退出，诊断不会默认细到每个 driver/actor，监督正确性依赖 owner 的真实 join 和移交顺序。换取的是不重复维护收敛树，也不在每个热单元起止时访问全局账本；若 owner join 本身有缺陷，必须修复真实边界，不能用多一层计数掩盖。

令牌签发、子工作报告与显式销毁增加了接口参数、原件移动和测试 owner。测试须真实消费 grant、等待工作退出、退休/reap，不用丢弃/forget 或将 unresolved 视作正常结束来简化 fixture。

第三方内部阻塞副作业仍处在明确边界之外；连接池等服务可比查询工作活得更久。缓存每次填充的 OS 线程也仍是服务工作，`Bytes` 明确移交后不跟查询义务退休。统一缓存载体与扩大第三方监督尚未交付；不能据本规则声称查询退出时进程物理内存归零。

FE 与测试基础设施的近邻配置保留裸启动能力，属于当前监督覆盖面的限制。根禁令不是第三方依赖审计，也不是所有 release 构建自动执行的安全门；必须运行计划要求的严格 Clippy。源码接线、局部测试与 ADR 均不证明完整测试族、真实部署或最终准入通过。

## 何时重新评估

- 真实诊断需求证明 owner 粒度不足，或测得账本/唤醒成本需要改变时，重新权衡逐单元监督、局部聚合和执行策略；保持单一真实 join 权威，不加没有资源依据的容量门。
- opendal 启用并发、iceberg-rust 分离任务进入 BE 读取、tokio/第三方执行器 API 或取消行为升级时，重新核对注入能力、实际 captures/副作业退出与 lint probe；需要新平台或上游适配时另行裁决，不靠 fork/裸 handle 悄悄扩覆盖声明。
- 缓存填充需要统一队列、生命周期或资源治理时，评估缓存自有 carrier，继续保管原 `Bytes` 的服务寿命与实际退出。
- FE 工作接入共同控制运行时后，按实际生产依赖逐 crate 撤除近邻配置豁免；测试基础设施新增生产消费者、FE 专用 crate 成为共享依赖时，立即重新审查豁免与凭证入口。
- 若要改变 Task/Participation 身份来源、启动门、必须消费义务或 `CompletionExited` / `ExecutionCustodyRetired` 的含义，写新 ADR 并同步原协议与消费者，不原地改写本条或历史 ADR。
