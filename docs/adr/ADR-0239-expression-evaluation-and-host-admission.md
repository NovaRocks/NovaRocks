---
id: ADR-0239
title: "Expression evaluation owns the scalar call protocol"
domain: [sql-compiler, crate-boundary, pipeline-execution]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0238]
date: 2026-11-18
implementation-status: implemented
provenance:
  - "discussion: 2026-11-18 accepted compiled expression evaluation and host admission boundary"
code-anchors:
  - "novarocks/expression/src/evaluator.rs (CompiledExpressionInstance::evaluate)"
  - "novarocks/expression/src/evaluator/host.rs (ScalarCallHost)"
  - "novarocks/expression/src/evaluator/input.rs (EvaluationInput, InputSourceStorage)"
  - "novarocks/expression/src/evaluator/scalar_invocation.rs (call_scalar, call_invocation, call_scoped_invocation)"
  - "novarocks/expression/src/evaluator/failure.rs (ScalarCallFailure, ExprEvaluationFailure)"
  - "novarocks/execution/src/exec/operators/compiled_expression.rs (RuntimeKernelControl, evaluate_selected)"
  - "novarocks/execution/src/exec/chunk/chunk_impl.rs (InputSourceStorage for Chunk)"
  - "novarocks/execution/src/runtime/scalar_memory.rs (RuntimeScalarMemoryScope, RuntimeScalarMemoryRefusal)"
  - "novarocks/execution/src/runtime/task/execution_failure.rs (ExecutionFailure)"
  - "novarocks/frontend-application/src/query_execution/required_constant_eval.rs (RequiredConstantInterpreter)"
  - "novarocks/frontend-application/src/query_execution/required_constant_eval/resources.rs (RootResources)"
  - "novarocks/failure-contract/src/shared_cause.rs (SharedFailureCause)"
---

## 问题

表达式求值怎样在多个运行时复用同一份编译单元和调用顺序，同时让每个宿主继续拥有自己的资源准入、输入存储与失败保管？

## 背景与执行事实

编译单元以 `Arc<CompiledExprSet>` 共享，根实例按根序号创建，保存该根的可变 kernel 实例与失败状态。求值有明确的实际行域、需求行选择及激活方式。守护帧、布尔区域、Filter 合取和单节点常量入口消费同一表达式层的编译与求值事实。Functions 拥有具体 kernel 和资源 profile。宿主把控制、分配、调用准入和输入存储借给求值器。

| 运行时接口 | 定义方 | 生产绑定与职责 |
|---|---|---|
| `KernelEvaluationControl` | Functions | Execution 用 `RuntimeKernelControl`，FE 必需常量用 `CompileKernelControl`，为同步工作提供检查点与可中断等待。 |
| `AggregateStateAllocator` | Functions | Execution 的跟踪分配器与 FE 的语句资源为状态、有限存储及结果保管提供实际分配能力。 |
| `ScalarCallHost` | Expression | Execution 的 `RuntimeScalarMemoryScope` 与 FE 的 `RootResources` 接过阶段和需求事实，按各自账本准入，在准入范围内运行求值器提交的调用体。 |
| `InputSourceStorage` | Expression | Execution 的 `Chunk` 在结果需保留输入来源时提供真实 `ValueTypeStorageLifetime`，由原分配宿主支撑新增持有者。 |

`EvaluationInput` 借入完整 Arrow 批次，或借入带完整源声明与物理列序号映射的投影，两种输入都可附来源存储。求值器按编译环境的冻结布局核对字段与类型。Execution 的输入适配核对原 `SlotId` 和类型绑定，再构造中立投影。

## 考虑过的选项

- **阶段性成本折衷：求值器留在 Execution。** 当节点还在逐个迁移为自包含单元时，这个边界把现有运行路径保留在原地。独立编译的单元可以按其有序端口和根求值后，继续沿用该边界会让复用者引入关系执行依赖，也会迫使表达式层公开求值器用的内部结构。
- **设计否决：每个宿主执行一份完整叶子调用协议。** 迁移代码较少，但每个宿主都要自行判定实例的失败状态、profile、准备前缀和回调需求相加顺序。新增调用路径或变更 profile 时会再次复制影响运行行为的决定。资源政策的差异只要求宿主各自处理准入和结算。
- **设计否决：将宿主拒绝一律转成不透明的共享原因。** 可复用共享保管节点，但 Execution 的内存拒绝有名义类型和后续投影。统一擦除类型会让原拥有者只能通过下行转换取回这个区别。

## 裁决

1. **单元编译与求值同层。** `novarocks-expression` 拥有从冻结环境编译单元、创建根实例及执行求值的生产入口。依赖止于 Functions、表达式契约、中立契约与 Arrow。Execution 在自身算子与运行时接线中适配输入、控制、分配和结果，FE 的必需常量在原语句工作中借用同一根求值入口。关系计划与包的原编译组合保持各自所有权。
2. **求值器唯一撰写调用协议。** 每次叶子调用由 `begin_call` 开始。普通调用有资源 profile 时先由求值器取得需求，再请求调用体准入。整次调用使用 profile 时，先请求准备前缀准入，在其中计算需求，必要时请求原宿主的回调需求并受检相加，随后请求调用体准入。已失败实例直接进入原实例的失败路径，无 profile 的调用走原 kernel 路径。显式实际域的整次调用若实例活跃且需要 profile，统一返回 `KernelFailure::InvalidProgram`，因为这个调用形状没有已安装的度量来源传输。
3. **宿主拥有准入政策和真实回执。** `run_admitted` 接收 `ScalarCallPhase`、`ScalarCallRequestFacts` 与同步调用体。宿主负责需求换算、准入、结算和原资源观察。准入成功时调用体执行一次，准入拒绝时执行零次。调用体的失败保留原首因。`callback_request_facts` 只提供原回调的需求事实。来源保管由实际输入缓冲与原分配能力决定，持有来源存储期不会产生新的容量。
4. **类型化理由留在求值值中，发生身份留在宿主。** `ScalarCallFailure<R>` 区分 `Kernel`、`Data`、`Source` 与 `Host(R)`。`ExprEvaluationFailure<R>` 在同一组原因上增加 `RequiredRow`。求值器传播首因，普通根行错误随选中结果返回，调用者按必需行语义处理。Execution 用 `RuntimeScalarMemoryRefusal` 保留具体内存拒绝并把失败转换成一次 `ExecutionFailure` 发生。FE 的拒绝类型是 `Infallible`，它在必需常量的原错误端口使用 `SharedFailureCause` 保管同一次发生及来源。控制失败、行数据失败、需求失败和宿主拒绝维持各自的失败事实与显示。
5. **部分替代范围。** 本裁决完成 ADR-0238 里求值器暂留 Execution 的阶段性折衷。其自包含单元、显式环境、关系调用所有权和 wire 规则继续适用。

排查时从发生的边界回到其作者。检查取消与等待进入哪一个 `KernelEvaluationControl`。内存拒绝核对 Execution 的准备前缀、调用体及结算回执，FE 预留失败核对同一语句资源与共享原因。结果别名跨批保留时沿 `InputSourceStorage` 检查原批次的存储期和分配能力。此次边界迁移沿用既有容量与配置默认值，计划契约修订号、目录和 wire 版本继续由原契约拥有。

## 接受的妥协（诚实记录）

- 表达式层的依赖方会一并编译求值代码。以一个 crate 内的定义、编译与求值换取单元内部结构的封装，并为未来的折叠复用提供同一生产入口。
- 求值入口带有宿主拒绝类型参数，两个宿主分别完成转换。保留名义拒绝与原发生保管的收益来自这一明确的类型与职责边界。
- 可选常量折叠暂时继续走独立单节点入口，复用表达式层的守护帧和 Functions。统一折叠输入表示需要独立迁移原调用声明及转换的作者。
- 带 profile 的显式实际域整次调用在 FE 采用统一的 `InvalidProgram` 原因。当前安装的函数不进入这个条件。若安装这样的函数，须先解决该形状真实度量来源的传输。

## 何时重新评估

- 当折叠的调用及转换事实能够由完整中立单元表达，并保留原常量、错误和可选策略时，评估删除过渡单节点入口。
- 当需要给显式实际域的整次调用安装带 profile 的 kernel 时，先核实可传输的实际域与原度量来源、准入顺序、失败保真及结算。新绑定应继续消费同一调用协议。
- 当出现无法用同步调用体表达的 kernel，或编译资源测量表明共同 crate 构成实际可观察瓶颈时，重新衡量接口或 crate 的边界。评估仍以单一协议作者和宿主资源归属为约束。
