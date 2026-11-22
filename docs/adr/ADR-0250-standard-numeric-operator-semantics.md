---
id: ADR-0250
title: "Standard numeric operator semantics and join key matching"
domain: [sql-compiler, join-execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-21
provenance:
  - "discussion: 2026-11-21 TRC-6a Trino 484 numeric operators and legacy wide-number handoff"
code-anchors:
  - "novarocks/expression/src/typing.rs (type_node, finish_call)"
  - "novarocks/functions/src/builtin/binary_operator_owner.rs (BinaryOperatorOwner)"
  - "novarocks/functions/src/arithmetic/trino_decimal.rs (result_type)"
  - "novarocks/sql/src/optimizer/cascades_rules/implement.rs (extract_eq_conditions)"
  - "novarocks/execution/src/exec/hash_table/key_index/local_hash.rs (selected key participation)"
---

## 问题

标准 SQL 数值运算需要在同一条语义链上确定结果类型、失败、比较和连接匹配。规划期折叠、普通行求值和哈希连接如何消费这一选择？

## 背景与执行事实

Trino 484 的算术重载决定有符号整数共同宽度、DECIMAL 刻度与舍入，以及 REAL 和 DOUBLE 的实际计算宽度。一个超过 DECIMAL 到 REAL 舍入中点的输入可能在先转为 DOUBLE 时丢掉决定方向的位。比较中的普通浮点等值不匹配 NaN，空值安全比较则匹配两个 NaN。两种等值都匹配正零与负零。

旧标准乘法的 `decimal_overflow_to_double` 在会话级更改乘积的类型。现存 LARGEINT、宽 DECIMAL 和 CAST 有独立的类型及溢出承诺，子查询成员关系还有独立的 NULL 范围和匹配需求。

## 考虑过的选项

1. **继续在遗留 Binary 中扩展类型、比较和错误。否决。** 两套装配必须重复冻结同一个重载，规划和运行容易分别修正同一语义。
2. **先按载体分类，再由执行器选择普通或空值安全等值。否决。** 原宽数比较和新谓词都可以得到 DOUBLE 载体。两个表达式的运行关系仍取决于原定型。
3. **以已选函数冻结数值语义，再证成关系快捷路径。采用。** SQL 保管原形状和求值范围。共享表达式定型对转换后的实参重验原重载。Functions 提供值与效果。经证明的连接键保留确切匹配关系。

## 裁决

1. 标准 Add、Subtract、Multiply、Divide、Modulo、六种普通比较及两种 DISTINCT 谓词通过隐藏目录身份发布。REAL 和 DOUBLE 保持各自宽度，标准 DECIMAL 隐式转换直接向所选 IEEE 宽度舍入。标准 DECIMAL 的结果声明与错误来自原重载。纯折叠保留错误节点，让执行端在实际需用的行上报告。
2. 普通浮点等值连接只在实参和剩余谓词均有提前匹配证明时抽键。计划冻结准确的键关系。BE 在 presence 查找和重复行组展开之前拒绝 IEEE 键中的 NaN。原行仍可参加外连接补空。网络分区继续以正负零的统一散列保证可能命中的键同处。简单 CASE 在隐式转换前核对宽数来源，冻结所选等值模式。求值器继续只计算一次主表达式，对尚未命中的行按顺序求值标签。
3. 成员判断继续保留原值比较、相关资格及 NULL 探针。普通 SELECT 中的新浮点谓词遵循标准运算符。排序、分组、成员查找及结果身份各自消费原键契约。
4. 移除旧标准乘法属性和专用语义参数，保留 wire 编号及名称。计划修订为 34，生产 Native epoch 为 17。存量宽数和内部消费者继续使用 Binary 对应的既有 recipe。后续删除必须有消费者退出证据。

## 接受的妥协（诚实记录）

不能证明提前匹配安全时，原布尔谓词继续消费本来的求值范围。可用计划空间取决于原连接的运行能力。外部 Provider 的剪枝保持保守，执行仍按实际定义核对谓词。

## 何时重新评估

6b 收回 LARGEINT 和外部宽 DECIMAL 的 SQL 准入时，逐项验明仍有生产消费者的运算、物化和 wire。改变成员关系、CASE 比较、排序或分组时，独立核对各自的 NaN、正负零、NULL 和控制关系。

[ADR-0244](ADR-0244-operators-are-identified-catalogue-functions.md) · [ADR-0245](ADR-0245-scalar-typing-finalizes-the-original-overload.md)
