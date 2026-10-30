---
id: ADR-0191
title: "Frozen fold facts and shared compiled evaluation"
domain: [sql-compiler, pipeline-execution, frontend-dml, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0100
    scope: "Frozen binding-owned requests and shared compiled single-node evaluation replace SQL/execution literal-node mapping and the retained evaluator implementation; the compiler port, stateless lifetime and optional-fold policy remain active"
    link: "ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md"
date: 2026-10-21
provenance:
  - "discussion: 2026-10-20 frozen fold facts, shared compiled evaluation, selection-owned COW conversion and exact compatibility material"
  - "discussion: 2026-10-21 ordinary CAST semantics for nonfinite text in selection-owned COW assignment"
code-anchors:
  - "novarocks/sql/src/compiler/fold_node.rs (FrozenFoldNode, FrozenFoldCall, ConversionPlan)"
  - "novarocks/sql/src/compiler/selected_scalar_call.rs (freeze_fold_call, freeze_conversion)"
  - "novarocks/sql/src/compiler/completion_driver.rs (FinalPlanCommon, analyze_with_catalog)"
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant.rs (try_fold_node_with_services)"
  - "novarocks/frontend-application/src/query_execution/constant_eval.rs (ExecutionConstantEvaluator::eval_scalar)"
  - "novarocks/execution/src/exec/expr/compiled_program/constant_node.rs (evaluate_constant_node)"
  - "novarocks/execution/src/exec/expr/compiled_program/constant_driver.rs (evaluate_inner)"
  - "novarocks/execution/src/exec/expr/compiled_program/guarded.rs (Frame)"
  - "novarocks/execution/src/exec/expr/compiled_program.rs (CompiledExpressionInstance::prepare)"
  - "novarocks/functions/src/control_values.rs (supports_indexed_result, assemble_values)"
  - "novarocks/functions/src/selected_copy.rs (fixed_interleave_extent, preflight_guarded_interleave)"
  - "novarocks/functions/src/selected_copy/guarded_nested.rs (supports_guarded_result, guarded_result_extent)"
  - "novarocks/functions/src/evaluation.rs (CanonicalConstantView, EvaluatedArgument)"
  - "novarocks/functions/src/invocation_scope.rs (ScalarInvocationSource::validate)"
  - "novarocks/functions/src/array_backing_geometry.rs (source_metadata_graph, source_request_facts)"
  - "novarocks/functions/src/arrow_result_custody.rs (wrap_data, wrap_original_group)"
  - "novarocks/functions/src/cast/f3_scalar.rs (Body::TextFloat)"
  - "novarocks/execution/src/exec/expr/compiled_program/constant_projection.rs (project)"
  - "novarocks/execution/src/exec/expr/compiled_program/input_source.rs (retain_result_type_with_creator)"
  - "novarocks/types/src/column_value_type.rs (ColumnDef::declared_value_type_observed)"
  - "novarocks/frontend-application/src/query_execution/dml/cow_materialized_values.rs (pool)"
  - "novarocks/frontend-application/src/query_execution/dml/cow_closed_route.rs (build_cow_append_query_core, build_cow_rewrite_query_core)"
  - "novarocks/functions/src/native_inlist.rs (PreparedNativeInListRecipe, InRows)"
  - "novarocks/execution/src/exec/expr/compiled_program/utf8_inlist.rs (utf8_equality_observed)"
  - "novarocks/sql/src/analyzer/root_output_declaration.rs (apply_root_output_declaration, root_output_type_matches)"
  - "novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs (execute_exact_cow_match_query)"
  - "novarocks/frontend-application/src/query_execution/row_mutation/signed_layout.rs (normalize)"
  - "novarocks/frontend-application/src/coordinator/internal_result.rs (InternalDomainFailure::execution_input)"
  - "novarocks/query-application/src/execution_error.rs (ExecutionFailureInput)"
  - "novarocks/execution/src/runtime/function_set.rs (ExecutionFunctionSetBuilder, SealedExecutionFunctionSet)"
  - "novarocks/version/src/lib.rs (NATIVE_COMPAT_EPOCH, derive_native_compatibility_material)"
---


## 问题

如何让优化期常量折叠与运行期表达式执行共用同一个计算作者，同时把 SQL 绑定、COW 赋值转换、签名布局和 Native 兼容材料放在各自的真实 owner？

## 背景与执行事实

SQL 的分析与优化决定一个表达式的完整领域、精确 binding、参数来源与可折叠性；运行期 compiled evaluator 决定同一准备结果怎样计算、选择分支并携带行错误。把 SQL 语法重新解释成另一棵执行表达式树，会使相同的绑定、NULL、转换和错误规则分别演进。共享纯 kernel 后，应让可复用的计算入口承接 SQL 已经冻结的事实。

| Owner | 实际输入与职责 | 不可替代的事实 |
|---|---|---|
| SQL `FrozenFoldCall` / `ConversionPlan` | 由与 package lowering 共用的 `selected_scalar_call` 作者冻结一次调用或 intrinsic | 原 binding、selected preparation、源语义、左右转换、decimal/throw policy 与完整结果类型 |
| SQL `FoldConstant` | 递归遍历、先折子节点、执行原有可折形状与环境策略 | 原 admitted CV 留在 `FoldRequest.args`；canonical runtime 参数留在 `FrozenFoldCall.arguments()` |
| FE `ExecutionConstantEvaluator` | 检查完整 CV 类型并投影到单节点端口，再按原输出约束接收结果 | 不重绑函数、不按名字分派、不计算值，不建临时 plan 或 dummy input |
| Execution `evaluate_constant_node` / `guarded::Frame` | 在真实的一行零列端口上建立 `ActualInvocationDomain::root`，复用 Frame 的 required-child、选择和行错误规则 | 空端口只提供常量调用的行域；节点、binding 和转换仍来自 SQL 冻结事实，不制造 LocalProgram |
| SQL COW 根输出声明 | `DmlInternalRead` 用完整签名类型声明选择查询根，调用原 value-conversion 作者 | 赋值 cast 是查询的一部分；不从类型文本或 Arrow carrier 猜逻辑领域 |
| FE signed-layout normalizer | 在真实签名上检查并规范化 metadata/布局，准确展开允许的 dictionary | 不执行值转换，不借布局重标放宽 signed NULL/domain/width 要求 |
| metadata function set / Version | 封存函数目录；派生 exact Native compatibility material | 不再用旧 evaluator 的执行 map 或实现清单冒充 compiled 安装事实 |

checked constant 进入 SQL continuation 后仍由原 source 拥有其 pool/ordinal/type 事实。`FinalPlanCommon` 保留 Arc-backed source，分析每一份实际 query 时重新解析结构 site 并在原请求 control 下建立 loan；不把前一份 AST 的地址映射跨克隆复用，也不以展示 SQL 重造值。

`CanonicalConstantView` 借用原 checked CV 与共享 representation author 产生的同形完整池 array，保留原 pool/ordinal、完整 logical/nullability 和 source 事实；广播仍读取原 ordinal，不把它重标为 `Scalar`，也不借 metadata 规范化执行值转换或 encoded decode。`from_authored_projection` 是要求实际 producer 正确配对的 facts loan，本身不证明 payload equality 或 encoding capability；`ScalarInvocationSource::validate` 仍检查完整语义与实际 carrier。Execution 的 `constant_projection::project` 拒绝 encoded 访问，并在真实 hosted 路径保留原 source 与新 wrapper construction stock；即使原 CV 为 pure，新 wrapper 的实际 creator 也必须经 continuation 穿过 root gather 与最后 alias。来源持有、creator 生存期和容量授权是不同事实。实际 Arrow 根节点没有 Buffer，不代表其后代也没有可承接的 backing；source custody 按同一棵实际树的 geometry 与 validity 事实预检和续接。只有整个子树都没有可承接 Buffer 时，来源才由根持有；不能凭类型名、root-only 标志或别名身份跳过后代，也不能据此推导容量授权。

类型来源也不能按 FE/BE 名字推断。普通语法和 catalog 列由其声明作者构造 pure 类型；COW 的 `cow_materialized_values` 则确实创建 held pool，保留原签名值类型。当前 COW 查询经 `cow_closed_route` 把这些 CV 放在 VALUES，外层使用 ColumnRef；`FoldConstant` 不提交裸 Constant 或 CASE 自身，并要求可折调用的全部 child 已是常量。这个实际路由边界不等于所有外部可提交的 `FoldRequest` 都有 pure 结果，也不允许清除 finite storage 来适配端口。

CASE、IF 等控制结果的 carrier 准入、组装和布局预检由 compiled preparation 与 Functions `control_values` / `selected_copy` 共用契约。准入面必须与实际 Arrow 作者支持的载体一致，并递归核对 List/Struct 子域；FixedSizeBinary 保留原精确宽度、父子 NULL mask 与字节，不按整数或变长 Binary 重解释。MutableBuffer 的容量取整与实际平台对齐属于表示作者的布局要求，不是值转换或容量授权。支持一个叶载体不自动准入其它容器或编码协议。

签名非空检查产生的 Connector Data fact 经 COW 私有 Internal domain 与 CPU 结果保留到 coordinator 的既有 typed latch，再由中立 `ExecutionFailureInput` 的 fact、固定安全 detail 与 site 投影公开错误。这里保留的是领域事实；公开载体不携带原 Error 对象，不承诺富 source 链端到端存在，也不根据 signed diagnostic 文本重建类别。

`native_inlist.rs` 拥有保留的有符号 IN membership/equality helper 与 prepared recipe；compiled `utf8_inlist.rs` 拥有 UTF8 借用比较 helper，`guarded` 使用它们与同一个 Frame。目录是否叫 `expr` 或 `node` 不决定退休范围，共享值类型、typed failure 与真实 construction helper 继续由其 owner 持有。

ADR-0100 的 compiler port、进程生命周期无状态单例、折叠阶段和既有拒折策略继续有效。本条部分替代其请求词汇与实现映射的妥协，不顺带扩大优化器覆盖。ADR-0185 的退役条件是承接实际生产调用方与独立正确性保护后再删旧；这些条件由调用方迁移、独立保护与实际构建、运行验证分别确认，不能从静态删除推断。

## 考虑过的选项

1. **保留旧 evaluator 作为常量折叠服务。设计否决。** 复用原外壳可缩小初始改动，却长期留下名字分派、类型/字面量映射与第二个计算作者；不满足共用 compiled 语义和退休旧实现的约束。
2. **在 FE 重写可折函数或控制表达式计算。设计否决。** 局部直接计算看似简单，但 NULL、分支选择、decimal policy 和错误来源仍要与运行期分别维护。相同输入出现两个计算作者的问题没有消失。
3. **在 FE 虚构 LocalProgram 或猜测布局与 binding 来装配每次折叠。设计否决。** 它可复用公开执行入口，却制造原请求未拥有的节点、布局和 binding，不能证明被求值的是 SQL 已冻结的那个单节点。此否决不包括 Execution 内部真实的一行零列常量端口；该端口明确建立原调用的行域，不补造表达式或 binding。
4. **SQL 冻结原事实，FE 经窄端口调用共享 compiled 单节点核心。选择。** SQL 与 Execution 保持 crate 边界，原来的 Frame/recipe 作者负责计算；代价是需要显式定义单节点端口和投影，并保留优化器自身的覆盖策略。
5. **FE 收到 COW 结果后再次 cast；继续携带旧实现摘要。设计否决。** 前者把赋值语义移出已冻结的选择查询，后者把不再执行的实现列表当作兼容事实；二者都让表示或身份取代实际 owner。

## 裁决

1. **冻结作者规则。** 只有 SQL 的实际 binding/lowering 作者能产生 FrozenFoldCall 与转换步骤。求值端口消费完整类型与准备事实，不能从函数名、carrier 或诊断文本反推语义。请求原 CV 与 canonical 参数分开保留，typed NULL 规范化不能抹掉观测来源。
2. **唯一计算规则。** FE 只做契约检查与无计算投影；调用、比较、转换和控制节点都复用实际 compiled 核心与 Functions owner。短路、strict NULL、所需子节点和成功 NULL/行错误的区分交给共享 Frame；不新增 FE 算法或第二控制解释器。
3. **失败保真规则。** 能力缺失可以拒折，原表达式仍由运行时拥有；仅实际可选求值的行数据错误可以 fail-open。非法冻结输入与 outer failure 始终 fatal，控制、容量、类型与源错误保留原 typed fact/首因，不因为显示相似就重分类。成功 NULL 与拒折不同，不能把错误换成 NULL。
4. **覆盖独立规则。** 共享核心支持更多 carrier 不自动扩大 FoldConstant 覆盖；原结果类型门、decimal 精度、byte-carrying/环境敏感名单及既定特殊形状拒折继续存在。扩覆盖需要单独接受的语义和独立预期。
5. **赋值属于查询规则。** COW 的完整根输出声明由选择查询分析接受，赋值转换只由普通 value-conversion author 与 compiled execution 完成。实际冻结的 SQL 根输出须保留原作者签署的逻辑领域、类型参数、宽度和 nullable 事实；Provider 的业务目标约束与 SQL 生产者的输出承诺是两层契约，不能互相替代。执行前核对真实根类型；FE 先按实际冻结的 SQL 输出承诺验证载体和值，再检查原 Provider/领域签署的目标约束，仅做布局/metadata 规范化和允许的准确 dictionary 展开，不以字段字符串或 Arrow 表示猜测类型、重建 cast 或伪造更宽的声明。若真实 SQL 输出承诺 nonnullable 而实际含 NULL，属于 Invariant，After 角色也不能将其改成 Data；合法 nullable SQL 值不满足 After 业务 required 时，则由原 Provider/领域约束在写入前以 Connector Data fact 拒绝，写入不能完成。Identity、Before 的原有目标不变量仍须分别验证；Source 或其它 Invariant 不因角色或目标 required 被重分类。STRING 解析为 FLOAT/DOUBLE 时，NaN、±Inf 与过大指数文本采用普通 CAST 的成功 NULL 结果：可空业务目标允许写入 NULL，必填业务目标沿上述业务约束拒绝，并经安全错误投影保留原 Data fact。此处明确放弃旧 COW 独立 helper 保留这些解析结果的行为，不新增 COW 专用 cast mode、wire 词汇或 FE 转换作者；输入本来就是浮点的非有限值不属于文本解析规则，其同型与既有浮点转换语义不变。
6. **兼容材料真实性规则。** metadata-only function set 不保留 legacy aggregate implementation map/manifest。Native compatibility domain 与生产 epoch 为 7，移除该旧实现摘要；函数目录、准确 pure 目录摘要、descriptor、plan contract 与 Provider/carrier 事实继续参与 exact identity，static-plan interpreter component 的既有 domain/tag/bytes 不变。不同 epoch 仍在不同岛，不能混部、协商或 fallback。独立执行侧安装清单尚未交付，当前目录一致性不冒充该证明。
7. **按消费者退休规则。** 删除旧实现前先承接真实 FE/compiled 调用、共享 helper 与独立保护。旧私有 oracle 的动态结果不能继续决定新 expected；共享 pure 数据与真实 SQL 作者不按旧模块名一起删除。静态删除、局部检查、完整验证和 Native 分布式证据分别记录。

## 接受的妥协（诚实记录）

保留注入端口和很薄的类型/节点契约投影，没有让 SQL 直接依赖 Execution。这保住既有 crate 边界，也使所有权能审阅；投影本身仍需完整类型、来源与错误链测试，不能因为没有计算代码就假定天然正确。

共享单节点入口当前没有 allocation host 参数。其 `finish_root_value` 对 finite RESULT layout 仍要求真实 host；直接公用 API 的任意合法 finite-result 请求不能据此宣称已完整支持。当前普通 parsed/catalog FE 路由的类型作者与 COW 的实际 AST 形状没有构成该请求的生产触发证据；这不证明公用端口的完整域，也不授权新增拒折或重标 pure。扩展到真实 finite-result 消费者时，必须显式传递其构造责任与容量 owner，再验证实际结果 continuation。

赋值语义统一会改变旧 COW 独立 helper 的部分结果，非有限文本就是明确接受的差异。选择一个可审阅的普通 CAST 作者，优先于保持两套转换长期分歧；这个取舍不允许把普通成功 NULL 解释为求值失败，也不允许在 FE 静默保留非有限值。

优化器覆盖仍比共享核心窄。保留已有拒折名单和特殊边界，是为了控制计划与成功覆盖变化，不是因为 compiled 核心不能表示更多值；这些策略仍需单独维护，不能声称已经由类型系统全部推导。

移除旧实现摘要后，兼容性依赖真实目录及 frozen contract 事实；执行侧独立安装清单仍是待交付能力。epoch 变化带来保守的同岛部署边界，即使部分 SQL 可能互通也不协商。值计算的共用不证明当前所有能力已恢复，更不替代最终全量/跨进程验证。

## 何时重新评估

- 新的优化器覆盖、timestamp/复合类型或 source-sensitive 函数需要折叠时，重新评估原拒折策略，并用独立预期与相同 compiled 路径验证；不能把共享核心的支持域直接当作优化策略。
- 出现新的 planning 求值消费者或实际 finite-result 来源时，评估单节点端口能否表达其实际来源、控制、构造责任与真实 host；若需要图或 Task 能力，另裁决其 owner，不向此端口加入虚构上下文，也不把持有 source 当作容量授权。
- 执行侧独立安装清单成熟时，将其真实摘要作为新的兼容事实纳入，并重新审阅 metadata 自带 owner 的一致性证明；不恢复旧名字执行 map。
- 产品需要跨版本混部或独立插件时，按 ADR-0124 重新裁决兼容 authority 与矩阵；不能删除 pure 摘要或利用本入口添加隐式 fallback。
- 签名布局、dictionary 表示或赋值转换领域演进时，在 SQL conversion 与 FE layout 各自 owner 重新验证边界、预检和实际构造，不把更宽的 cast 许可塞回 FE。
