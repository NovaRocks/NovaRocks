---
id: ADR-0079
title: "IDL is the canonical FE/BE query lifecycle contract"
domain: [distributed-query-lifecycle, crate-boundary]
status: active
supersedes: []
superseded-by: null
date: 2026-08-16
provenance:
  - "discussion: 2026-08-15 native query lifecycle canonical contract and Protocol ownership"
code-anchors:
  - "idl/novarocks/service.proto (FE/BE lifecycle messages)"
  - "novarocks/proto/src/lib.rs (schema artifact exports)"
  - "novarocks/proto/src/lifecycle/mod.rs (lifecycle contract module)"
---

## 问题

FE 与 BE 共享的 query lifecycle 事实应以哪一种形式作为唯一规范，以及 `novarocks-proto` 应拥有到什么边界，才能避免 Rust value 与 protobuf message 并行演进而削弱跨进程围栏？

## 背景与执行事实

Native query lifecycle 的 Init、Stage、Start、Abort、control stream 与 terminal delivery 都跨越独立的 FE/BE 进程和故障域。它们已经由 repository-level IDL 生成 protobuf message；同时，部分 lifecycle 事实又有手写 Rust value、逐字段 encode/decode 与手写 digest 投影。于是同一事实有两处定义，正确性取决于人工维持双向映射。

这种并行表示的风险不是编译器能捕获的。proto3 message 增加字段时，生成 DTO 会更新；手写 digest 投影、decode、跨字段检查却可能遗漏该字段。尤其当 digest 被用作重复投递一致性与 participant manifest 围栏时，遗漏会使封存内容、wire 内容和 hash 覆盖面不再一致。

`novarocks-proto` 原先只导出 generated DTO、descriptor set 与 schema-ledger metadata。它可以在不依赖 Core、Frontend、Backend、Execution、SPI、SQL、Server 或 provider runtime 的条件下，拥有纯 schema、descriptor-driven canonicalization/digest、以及只执行 value/wire 校验的 validated value。FE 与 BE 各自的 registry、timer、transport、liveness 和状态机仍是角色本地实现，受 ADR-0007 的进程分离约束。

仓库当前没有独立 schema baseline、mixed-version negotiation 或 rolling-upgrade digest 兼容承诺。因此同一部署中 FE 与 BE 可在一次原子发布中共同切换 digest 算法；这不构成对不同版本进程互通的承诺。

## 考虑过的选项

1. **保留手写 Rust lifecycle model，并继续维护双向 codec 与 digest 投影。** 角色代码可继续使用熟悉的类型，但每次 schema 演进都要求人工同步多处字段，遗漏只能在运行时或生产围栏异常后发现。
2. **把现有手写 model 原样搬进 Protocol。** crate 图会更集中，却仍保留「Rust model 与 proto message」两份规范；重复和人工双射被制度化，而非消除。
3. **只暴露裸 generated message，让每个调用方自行检查。** 表示只有一份，但 required message presence、未知 enum 值、集合唯一性、规模上界、跨字段约束与 digest conflict 会分散到 FE/BE 调用点，最终再次形成多份语义。
4. **以 IDL/proto 为唯一规范形式，并由 Protocol 提供 validated value 与纯校验。** generated message 仍是唯一存储字段；wrapper 的私有内部仅持有对应 message 一个字段，构造函数在边界完成校验，descriptor 驱动 canonicalization/digest。（采纳）

## 裁决

IDL/proto message 是所有 FE↔BE 中立 query lifecycle 事实的唯一规范形式。canonical bytes、digest 输入与字段演进均以 descriptor 所描述的 schema 为准；不再把手写 Rust value 当作可与 proto 并列的第二份 contract。

`novarocks-proto` 扩展为三层稳定 surface：source IDL、generated DTO 与 descriptor set 组成 schema artifact；descriptor-driven canonical projection 与 digest 是可复用的纯工具；validated newtype 和纯校验规则封装 schema 无法表达的 value 不变量。每个 validated newtype 的字段私有且只持有一个 generated message；它不缓存 digest、runtime handle、registry reference、callback 或可变能力。解码后是校验并包装，编码是取出同一 message，而不是 Rust/proto 的双向语义转换。

Protocol 校验 required message presence、已知 enum 值、非零/上界、集合唯一性、跨字段一致性、digest 与内容一致性以及重复请求的 typed conflict。它不判断 ControlReady 是否收齐、participant 当前相位、timeout 是否触发、registry 是否接纳消息、SQL terminal verdict 或 retry/recovery 策略；这些依旧是各角色状态机的责任。

manifest 与 stage digest 都在 generated message 上经 descriptor 规范化后计算。manifest digest 的数值因此有意改变一次：验收是同一 schema 下 FE/BE 同源计算、round-trip 一致和重新固化 known-digest fixture，而不是与旧手写投影保持字节相等。query options 中需要区分「未设置」与「设置为零」的字段必须使用 proto3 `optional` 表达 presence；wire 不能表达的区分不得在 Rust 层伪造。

## 接受的妥协（诚实记录）

- Protocol 不再是纯 codegen crate：它将包含手写 validated wrapper、校验和 digest 代码，审阅与维护成本随之集中。选择这一点是为了消灭并行规范和人工双射，不是因为把更多 Rust 放进 Protocol 天然更简洁。
- 新增 proto 字段会自动进入 descriptor-driven digest 覆盖面，却**不会**自动产生业务校验规则。每次 schema 演进仍须人工判断是否需要 presence、上界、唯一性或跨字段校验，并补 negative test；否则 schema 是完整的，value validity 仍可能不足。
- 一次性改变 manifest digest 值会使旧 fixture 和旧进程不能与新实现混用。接受该迁移成本，是因为旧值来自有损/手写投影，维持它会继续把未来字段排除在围栏外；本决策没有声称 rolling upgrade 安全。
- validated wrapper 只保存 message，不能缓存导出的执行期结构或预计算派生值，可能增加反复读取/转换成本。选择它是为防止 wrapper 再次演化成平行 model；若性能需要专用运行期表示，应由实际执行 owner 单向派生，而不回流为 wire authority。

## 何时重新评估

1. 若产品需要 mixed-version FE/BE、滚动升级或独立 schema baseline，必须先定义版本协商、digest versioning 和跨版本兼容窗口；不能假设本决策的一次原子切换仍然成立。
2. 若某个 lifecycle 规则需要 live registry、timer、transport 或 provider state 才能判断，说明它不是 Protocol 校验；应留在相应角色 owner，而不是为通过校验把 runtime capability 引入 Protocol。
3. 若 descriptor-driven digest 的 CPU、内存或 deterministic ordering 成为可测瓶颈，应以固定 schema fixture 和跨进程一致性证明替代实现；不得恢复字段手抄投影作为未经证明的快路径。
4. 若 validated value 的单-message 形状无法承载新的跨角色事实，应先判断 IDL 是否缺少字段或 owner 是否错误；只有明确裁决新的持久/运行期 contract 后才能放宽该形状，不能默默添加缓存或第二份 Rust model。
