---
id: ADR-0222
title: "Session property declarations and required constant evaluation"
domain: [sql-language, sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-07
symptoms:
  - "同一属性从 SQL 和请求头设置时表现不同"
  - "INVALID_SESSION_PROPERTY / X-Trino-Session / X-Trino-Set-Session"
provenance:
  - "discussion: 2026-11-07 typed session property ownership and required constant evaluation"
code-anchors:
  - "novarocks/query-application/src/sql/session/registry.rs (SessionPropertyRegistry)"
  - "novarocks/query-application/src/sql/session/registry/inputs.rs (SessionPropertyInputs::insert)"
  - "novarocks/query-application/src/sql/session/registry/catalog.rs (SessionPropertyRegistry::bind_catalog)"
  - "novarocks/catalog-application/src/service.rs (CatalogApplicationService::admit_catalog_configuration)"
  - "novarocks/sql/src/compiler/required_constant.rs (prepare_required_constant)"
  - "novarocks/frontend-application/src/query_execution/required_constant_eval.rs (RequiredConstantInterpreter::evaluate)"
  - "novarocks/frontend-application/src/query/session_property_computation.rs (SessionPropertyCpuInput::run)"
  - "novarocks/frontend-application/src/query/set_action.rs (SetStatementAction::poll_set)"
---

## 问题

SQL 常量表达式与 HTTP 文本如何共用一份会话属性声明，同时保留输入、默认值、有效设置、catalog 实例身份及求值的真实生命周期？

## 背景与执行事实

会话属性跨越四个作者。Query Application 定义属性名称、类型、领域校验与设置投影；SQL 定义表达式、隐式转换和 bound call；Frontend 持有会话与受监督计算；Trino Adapter 只处理 HTTP framing 和应用已确定的效果。协议文本不是求值器，优化器也不是会话存储。

| 实体 | 作者与用途 | 不能替代的事实 |
|---|---|---|
| `PropertyDeclaration` | Query Application 中唯一声明名称、五种 SQL 类型、说明、hidden、可选默认值及校验/投影 | 不按协议再列一份同名表，不用消费者 profile 反造默认 |
| `SessionPropertyInputs` | 同一会话 owner 保存原 header 文本或 SQL 渲染文本，以及已解码的显式 typed override | 不把有效执行设置反向编码为用户输入 |
| 声明默认值 | 原消费作者的固定常量或可空默认；用于声明/元数据 | 不是一次显式 SET，也不保证每项都有值 |
| 有效设置 | 从显式覆盖与原消费回退派生；DOP 可 Auto、资源值可 None、拓扑取真实冻结快照 | 不是第二份可独立修改的输入真相 |
| `PreparedRequiredConstant` / `ConstantValue` | 原 analyzer 与转换作者准备，由原 compiled 内核计算；CV 保完整类型、Field、pool/ordinal 和 creator/source | 不是 QA 专属值，也不是重新解析的显示文本 |
| `ValidatedSessionProperty` / `SessionEffects` | 声明校验后携带 typed 值、响应文本与必要的原 CV 来源，交原效果消费者接收 | 准备完成不等于会话已提交，也不等于原 CPU 已退出 |
| `CatalogPropertyMetadata` | provider 声明工厂消费已准入实例的真实配置，产生该实例/代次的不可变声明 | 同 provider 名或同 catalog 名不证明同 attachment、instance、generation |

`SessionPropertyInputs::insert` 只保长期需要的 raw/decoded 信息；效果中保管的原 CV 到接收边界为止。它不能让长期会话继续钉住前一条语句的 result window。执行设置是同一会话输入的派生投影，不新建持久状态或全局缓存。

普通 `SET [SESSION] name = expression` 是必求值请求：调用方必须得到值或准确失败。ADR-0100、ADR-0191 的 optional FoldConstant 则可在不能安全折叠时保留原表达式。两者复用 analyzer、转换与 compiled 计算能力，但失败合同不同，关闭优化规则不能使合法 SET 失效。

HTTP 输入先由协议作者完成 URL/header framing 与重复名称处理，注册表再解码值。`SessionPropertyRegistry::prepare_header_inputs` 先遍历系统属性，再遍历 catalog 属性，分别保留原阶段内顺序。`CatalogApplicationPort::admit_catalog_configuration` 在原本机投影中取得 runtime 和真实已验证配置，并核对原 binding/incarnation；结果是只读事实，不授予未来代次访问权，也不触发 Provider I/O 或物化。

业界对照采用固定 [Trino SessionPropertyEvaluator](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-main/src/main/java/io/trino/execution/SessionPropertyEvaluator.java)、[SessionPropertyManager](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-main/src/main/java/io/trino/metadata/SessionPropertyManager.java) 与 [Session](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-main/src/main/java/io/trino/Session.java) 的源码：SQL 先求值与序列化，再走声明 decoder；原请求文本、typed 值与 Connector 实例声明分开，系统属性验证先于 catalog。这里的依据是这些固定版本源码，不宣称不同版本全部源码一致或本地已经运行全部协议输入。

## 考虑过的选项

1. **按协议维护属性表，直接修改执行设置。设计否决。** 分支短、初次接线容易，但名称、值域与默认会在 SQL、header、消费者之间分裂；只保存最终 settings 还会丢失 `TRUE` 等合法原文与“未设置”的含义。声明必须集中，协议只做输入/输出投影。
2. **把 SET 当文本赋值或依赖 optional FoldConstant。设计否决。** 字符串重解析无法继承完整类型与 short-circuit；optional miss 不能履行必求值合同。另写一个 SET evaluator 或恢复旧执行器同样会制造第二套转换和错误语义。
3. **在 session Mutex 或 async 驱动中完成求值，逐项写 live state。设计否决。** 这会把实际 CPU 工作放错 owner，并让后项失败留下前项效果。分析、转换、kernel、渲染和 staged projection 必须留在原监督计算中，锁只用于快照与一次提交。
4. **provider 工厂按已准入 catalog 实例与配置产生声明。采用。** 保留同 provider 多 catalog 的默认差异、drop/recreate 与代次边界；代价是必须显式提供真实配置快照与完整身份，不能拿空配置或 provider-wide 缓存补齐。
5. **完全复制 Java 的宽松 header decoder。设计否决。** 可减少客户端迁移，但会保留非 true 布尔静默变 false、数值解析失败成为内部错误等行为，违反这里明确采用的严格值准入和普通输入错误合同。当前选择完整且严格的 decoder，并承担协议兼容成本；不声称这些宽松行为在业界不存在。
6. **允许 volatile/stateful/effectful 调用作为 SET 值。成本否决。** 可接近允许 `rand()` 求值的行为，但当前必求值服务复用已封存的纯函数/生命周期及环境合同，不另建有状态解释器。缺环境的 environment-dependent 调用也不能假装纯常量。将来是否扩展，须由实际函数和执行 owner 共同裁决。

## 裁决

1. **单声明规则。** 系统属性只在 Query Application 注册一次；catalog 属性由 provider 工厂声明。声明组合时拒绝重复名。SQL 名称按原 SQL 规则查找，header 名称不额外转小写；协议不再维护同名枚举或默认表。
2. **四层输入规则。** raw 输入、explicit typed override、optional default、effective settings 分别表达，受同一会话 owner 管理。无覆盖保留 None/Auto；拓扑回退仍取真实冻结拓扑，不能为构造默认元数据固定成某个 profile 的节点数。header `TRUE` 的 raw 保留，执行只消费 typed true。
3. **必求值规则。** SQL 在原空关系/常量准入边界分析，按属性目标类型只允许既有隐式转换；原 bound metadata 决定 lifecycle、稳定性、状态、副作用和 environment 准入。使用原 compiled kernels，保 CASE/IF/AND/OR/COALESCE 选支及未选分支不求值；实际选中错误不可当作 optional miss。optional FoldConstant 的开关、拒折与 fail-open 合同保持不变。
4. **中立值与交接规则。** 跨 SQL/FE/QA 交中立 CV，验证实际选中 ordinal、完整逻辑/物理类型、NULL、来源与 creator。转换必须运行原转换作者，不重贴 metadata。原 CV 跟随 prepared effects 到原消费者接收；长期 SessionPropertyInputs 保 decoded/raw，不能持有旧 statement CV 来延长结果资源寿命。
5. **原 CPU、一次提交规则。** SET 冻结初始会话和函数快照，全部 RHS 及 staged projection 在同 Root 的原受监督 Local CPU 工作中完成。业务结果、取消、raw health 和真实退出分别保留；必须先观察原 CPU 退出，再在原成功可见性/取消/代次门内短锁交换完整 staged state。任何一项失败均无部分 live 覆盖或成功 effects，旧 state 析构不在该锁内执行。
6. **实例声明规则。** catalog 名经原 admission 得到 Absent/Unavailable/Ready。Ready 必须带原 attachment、instance、provider、generation 与同代已验证配置；工厂产物不可变。Unavailable 不是 Absent；同名重建不能复用旧 typed override。raw catalog 输入按 catalog 分开存放，但原身份仍决定覆盖是否适用。当前生产 provider 声明为空，不提前交付真实 catalog 属性或 BE ConnectorSession 传递。
7. **协议顺序与失败规则。** framing 错误仍走原 HTTP 400；格式合法后系统属性先于 catalog，缺 catalog 在前也不抢过未知系统属性。普通未知名、类型/NULL/值域拒绝为 14，真实缺 catalog 为 44；资源、取消、期限、内部不变量与实际 Source 保原 typed 类别/cause，不统一改成 14。失败不得输出 session effects。
8. **响应与上下文规则。** SQL effect 从已求值目标值渲染：boolean、十进制 integer/bigint、原 varchar 及 Java 形式 double（如 `1.0`）；不是把 SQL RHS 原文回送。header 保原文用于元数据，同时执行 typed 值。`USE` 只表达 catalog/schema，不用 `Clear-Session: catalog` 伪装撤销属性；catalog 上下文不是可登记的伪系统属性。
9. **原消费优先级规则。** `query_mem_limit` 与递归深度均为 statement hint > explicit session > 原消费默认/无覆盖。递归继续原有限截深机制；这项属性接线不引入另一套递归求值算法。

## 接受的妥协（诚实记录）

- **更严格的 header 输入。** boolean 只接受完整的 true/false（大小写不敏感），数字必须完整解析且可表示，随后仍受该属性自身领域校验；不会因统一 decoder 擅自扩大 finite-only 拒绝。这会拒绝部分宽松 Java 客户端过去能发送的输入，维护者必须把它当可见差异。
- **纯常量能力边界。** volatile、有状态、有副作用及无实际环境事实的调用不能在此入口执行。确定性并不单独构成准入资格；完整 bound call 与现有 compiled 能力仍须成立。准确能力拒绝、未运行输入与不支持的 catalog 属性分别保留，不能借此声称全部 Trino 会话语义已交付。
- **声明接口先于 catalog 功能。** 当前工厂/实例配置/代次链可表达准确声明，但生产 catalog 属性为空，BE 传递尚未交付。未来不能把这条空列表或本地元数据方法当成真实属性已生效的证据。
- **两种失败合同共用计算内核。** required evaluation 必须暴露真实失败，optional folding 仍可跳过优化；维护成本是分别保护两种合同，不能为减少接口把所有失败混成 miss 或 14。
- **运行资格独立。** 声明、源码接线和局部 FE/Server 编译检查不证明 unit、协议场景或完整原生验收已通过。历史上游观察的原件不可用时只能保留历史指针，不能用源码推导冒充重采真值。

## 何时重新评估

- 需要接入真实 catalog 属性或 BE ConnectorSession 时，先定义 provider 实际消费、冻结传递与同代覆盖生命周期；已有 instance 工厂不能授权跨代重用或隐藏 provider I/O。
- 只有后续兼容策略明确改变严格输入及普通值错误合同，才重新裁决宽松 boolean/数字解码；必须先说明哪些输入成为合法值、为什么允许静默解释，不能直接把无效值恢复为成功默认。
- 当原执行 owner 能监督实际有状态/volatile/environment-dependent 调用，且其取消、副作用、资源和真实退出合同明确时，重新评估纯常量边界；不得仅放开函数名或稳定性标记。
- 增加类型、RESET/SHOW、statement-local 覆盖或其它语言入口时，沿同声明与原会话 owner 演进输入层；先明确 raw/default/override 的含义与代次，避免另一份可变 settings 或注册表。
- 共享 CPU 或结果窗口的长期保管方式变化时，重新核查“效果接收前保原 CV、长期会话不钉旧 CV”及先真实退出再提交的边界；超时、终态或返回 Ready 仍不能替代原退出。
