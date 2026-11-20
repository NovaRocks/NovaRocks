---
id: ADR-0244
title: "Operators are identified catalogue functions"
domain: [sql-compiler, pipeline-execution]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0242
    scope: "Numeric negation joins ordinary immutable call folding; the same production ownership, admission and failure rules remain"
    link: "ADR-0242-constant-folding-compiles-expression-units.md"
date: 2026-11-20
provenance:
  - "discussion: 2026-11-20 accepted operator identity, migration and folding contracts"
code-anchors:
  - "novarocks/type-contract/src/operator_function.rs (OperatorFunction)"
  - "novarocks/functions/src/operator_functions.rs (installed operator catalogue validation)"
  - "novarocks/sql/src/common/negation.rs (typed and optimizer operand views)"
---

## 问题

表达式层怎样为运算符提供稳定身份、普通函数的求值路径和精确的优化器识别，同时保留用户所写的运算符形式？

## 背景与执行事实

| 作者 | 事实 |
|---|---|
| 类型契约 | `OperatorFunction` 将每个已迁入运算符映射到唯一 `FunctionId` |
| Functions | 一个隐藏目录函数拥有签名、重载效应与 kernel，生产封存核对每个成员及其全部重载 |
| SQL | 分析通过可信身份调用，类型化视图按身份借用实参，EXPLAIN 依据运算符结合和词法边界呈现，MV 身份使用实际 `FunctionId` |
| 编解码与运行时 | 调用随普通单元契约传递完整绑定、请求和控制形状，表达式求值器通过同一个准备后调用执行 |

取负的有符号整数和 LARGEINT 重载发布 `MayRaise`，浮点和 DECIMAL 重载发布 `NoRowError`。Strict 调用沿实际输入决定可空性。浮点符号由 IEEE 取反决定，DECIMAL 保留原载体、精度和刻度。有符号极值通过原选择序号返回行错误。

## 考虑过的选项

1. **每类运算符继续使用专用执行节点。设计否决。** 容易直接识别语法，却要重复维护名称绑定、物理请求、求值、效应和编解码，每次新增能力都扩展第二条路径。
2. **按目录函数名字字符串识别。设计否决。** 实现简单，别名或诊断改名会改变优化器识别结果。稳定的目录身份可以同时支持可靠识别和独立的用户呈现。
3. **隐藏目录函数和封闭运算符枚举。采用。** 参考 Trino 的运算符身份与类型化视图，复用普通调用并保留精确语法语义。比较迁移保留原操作方向和写法，避免把表示标准化与关系语义绑定。
4. **所有旧运算符在同一提交中改写。成本否决。** 算术、比较、转换及控制结构有独立的失败和需求规则，统一切换难以逐一承接已有行为证据。

## 裁决

1. **可信运算符入口。** 在共享定型的节点规则按枚举绑定，用户显式调用隐藏名称沿目录拒绝。运算符用完整类型选择重载，每个重载发布真实效应及可空性。
2. **按身份消费。** 优化器和渲染器先核对身份，再由类型化视图读取参数。渲染依据实际子式优先级、连续负号和数字面量边界。MV 以同一身份发布。
3. **迁移单元。** 逐构造迁入，契约、编解码、编译与求值走普通调用。先承接该构造已有的值、NULL 与失败性质，再删除旧节点、recipe 和专属内部测试。wire 删除保留编号并推进计划契约修订。新增能力或改变遗留构造的语义时，由该改动迁入构造。局部缺陷修复可沿原所有者执行。
4. **构造保持控制边界。** 算术、比较、NOT、LIKE、IN 和 BETWEEN 逐项迁移，CAST 和 CASE 保留各自根据输入定型与条件求值的职责。BETWEEN 引入单次绑定的 `Let` 以保证非平凡操作数只求值一次。
5. **常量折叠。** 取负成为普通不可变调用后按 ADR-0242 的相同单元、输入类型、完整效应和结果准入参与折叠。选中行报数据错误时保留原式。能折成字面量的谓词可获得原有的连接器下推；可能报错的重载继续约束依赖 can-fail 的改写。

## 接受的妥协（诚实记录）

未迁入的运算符仍需维护共享契约和 recipe，直到最后一个消费者退出。遗留面清单记录 owner 与迁移触发，构造迁移各自承接验证。运算符目录身份随计划兼容岛部署，含取负的旧 MV 文档需要按同构建版本切换重新建立。

## 何时重新评估

新增或扩展遗留构造语义时重估其在普通目录调用、控制结构或转换节点中的归属。迁到最后一个消费者时删除仍在用的共享旧设施。发现调用控制不足以表达单次求值与行错误时，先冻结所需控制契约，再迁入该构造。
