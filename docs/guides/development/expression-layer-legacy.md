# 表达式层遗留面

新增能力或改变遗留构造的语义时，承担改动的 PR 将该构造迁入共享定型与普通调用，承接原性质并更新本清单。局部缺陷仍由对应作者修复。最后一个消费者迁移后清理共享旧设施。迁移步骤见 [ADR-0244](../../adr/ADR-0244-operators-are-identified-catalogue-functions.md)，语义转换和物理签名分工见 [ADR-0245](../../adr/ADR-0245-scalar-typing-finalizes-the-original-overload.md)。

## 构造、转换与关系调用

| 遗留面 | 当前入口与实际计算 | 迁移方 |
| --- | --- | --- |
| 二元算术 | SQL `analyzer/resolve_expr.rs::analyze_binary_op`，`type-contract/src/arithmetic.rs` 推导，`functions/src/arithmetic/` 的已选 recipe 计算 | TRC-6a |
| 比较与 IS [NOT] DISTINCT FROM | SQL `analyze_binary_op`、`analyze_predicated`，类型契约的 `comparison_coercion.rs`，Functions 的 `comparison/` | TRC-6a。保留比较原方向与用户呈现 |
| 一元正号 | SQL `signed_unary_plus_identity` 只接纳有符号物理整数，已接纳时直接保留原表达式。Trino 484 的数值正号范围更广 | TRC-6a |
| NOT、LIKE、IN、BETWEEN | SQL `analyze_expr_impl` 与 `analyze_predicated`，表达式层的 `evaluator/guarded.rs`、`evaluator/utf8_inlist.rs` 与既有 Functions recipe | 相应语义改动迁入。BETWEEN 的迁移引入单次求值绑定 |
| IS [NOT] TRUE/FALSE | SQL `analyze_predicated` 仍产出 `IsTruthValue`，契约、编解码和求值都保管该构造 | UEA-5E B8c 退役原产出后清理 |
| CAST 和 TRY_CAST | SQL `analyze_expr_impl`、`analyzer/value_conversion.rs`、表达式 cast 节点与 `functions/src/cast.rs`。标量 TRY_CAST 的 `safe` 仍用遗留规则 | TRC-6b |
| 简单 CASE | SQL `analyze_case` 的比较公共类型，`contract_lowering/scalar_emission.rs` 的旧分支转换，表达式层 CASE 控制域 | 随比较迁移 |
| 时间值、精度与运算 | SQL `resolve_expr.rs` 中的`date_trunc` 与间隔专用定型，Functions 对应时间/日期 owner | TRC-6c 先统一时间类型与运算，按名特设随函数迁移 |
| 聚合、窗口与 MV 状态 | SQL `analyzer/functions.rs` 与 `resolve_expr.rs` 的特设参数、类型和校验，`planning/mv.rs` 另有窗口判断名单。关系调用沿原函数 owner 的 ABI | TRC-6d 收敛重复定型和窗口名单，保留独立关系调用生命周期 |

路径以 `novarocks/` 为根。SQL 符号以 `novarocks/sql/src/` 为根。

## 按名特设和高阶函数

[共享迁移判定](../../../novarocks/expression/src/typing/route.rs) 列出具名函数和可达别名。迁移整个函数时同步 SQL 与文本入口的归属。

- **结构与语法特设，由 TRC-6d 迁移。** `resolve_expr.rs` 保有 `strpos`、`normalize`、`element_at` 及其数组和 MAP 内部名称、`typeof`、`array_length`、`cardinality`、`array_sortby`、`concat`、`array_generate`、`array_flatten`、`if`、`variant_get` 及别名、`named_struct`、`array_concat`，以及 FIELD 的参数规范化和公开名称 `negative` 的原拒绝。
- **字符串预转换，由 TRC-6d 迁移。** SQL 的 `apply_implicit_string_function_casts` 保有拼接、TRIM、长度与大小写、正则、替换、拆分、子串等具名规则，实际实现由 Functions 对应 owner 执行。
- **按名校验，由 TRC-6d 迁移。** `analyzer/functions.rs::validate_scalar_function_call_impl` 保有 MAP 键、数组约束、固定参数个数及 MV 状态辅助函数规则。
- **高阶函数与 lambda。** `resolve_expr.rs` 的按名改写、SQL lambda scope、Functions 的动态重载与现有 ABI 共同承接数组及 MAP 的高阶调用。UEA-5E B7 或 TRC-6d 的首个新增高阶能力 PR 冻结两段式 lambda 接口。
- **ROW 安装缺口。** Parser 映射 `ROW`，元数据声明 `builtin.scalar/row/dynamic-v1`。原生封存仍缺准确实现，UEA-5E B7 承接。

## 规划期和降级期

| 遗留面 | 原代码位置与后续收敛 |
| --- | --- |
| 规划与改写决定 | `optimizer/scalar/` 的重新绑定、`analyzer/subquery_rewrite.rs`、`analyzer/resolve_from.rs` 的 USING、IMV 改写与谓词下推重建 CAST。相应改动迁移本构造，并保留实际列与关系语义 |
| 降级重算 | `contract_lowering/scalar_nullability.rs`、`compiler/selected_scalar_call.rs` 的转换选择和 `canonical_null_target` 承接旧构造。各构造迁移后移除其语义重算，真实物理 NULL 传播继续由原单元事务维护 |
| 共享旧结构 | Functions recipe、契约中的旧运算符枚举以及 `DecimalOverflowToDouble` 和 `AllowThrowException`。最后一个构造的迁移负责前两项，语义开关由 TRC 收敛 |

## 已知差异与缺陷

- 二元运算在用户可见 EXPLAIN 的通用表现可能丢失括号。相应构造迁移时收敛，取负的专用渲染已接入按身份的路径。
- LARGEINT 减法的回绕位于 `functions/src/arithmetic/largeint.rs`，由 TRC-6a 承接。
- SQL 的 `analyzer/helpers.rs` 与 `analyzer/resolve_from.rs` 仍有直接从 AST 常量取负的路径，最小 i64 的受检语义需由原消费方修复。
- NovaRocks 在分析期对裸 `-NULL` 返回 `TYPE_MISMATCH`。Trino 484 返回 `AMBIGUOUS_FUNCTION_CALL`，两侧都要求给出明确的数值类型。错误映射差异由 TRC 核对。
