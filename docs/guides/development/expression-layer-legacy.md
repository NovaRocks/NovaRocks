# 表达式层遗留面

新增能力或改变遗留构造的语义时，承担改动的 PR 将该构造迁入共享定型与普通调用，承接原性质并更新本清单。局部缺陷仍由对应作者修复。最后一个消费者迁移后清理共享旧设施。迁移步骤见 [ADR-0244](../../adr/ADR-0244-operators-are-identified-catalogue-functions.md)，语义转换和物理签名分工见 [ADR-0245](../../adr/ADR-0245-scalar-typing-finalizes-the-original-overload.md)。

## 构造、转换与关系调用

TRC-6a 的标准算术和比较经共享定型选择隐藏重载，EXPLAIN 按身份渲染原运算符。标准一元正号保持已定型的有符号整数、REAL 或 DOUBLE 实参。常量折叠和执行共用已选调用，实际数据错误沿原需用位置报告。 [ADR-0250](../../adr/ADR-0250-standard-numeric-operator-semantics.md)

| 遗留面 | 当前入口与实际计算 | 迁移方 |
| --- | --- | --- |
| 宽数二元算术 | `resolve_expr.rs::analyze_binary_op` 根据原完整声明把 LARGEINT 和已准入宽数送入遗留分支。LARGEINT 与 DECIMAL 加减可能保留 Decimal256 结果。走 `ExprKind::Binary`、wire 字段 9 和原宽数 recipe。混合 DECIMAL 沿原 `DecimalOverflowPolicy`，LARGEINT 沿原签名。由 `decimal_largeint_add_sub` 和对应 Rust/Codec 用例保护 | TRC-6b 收回宽数准入并核对转换和其他消费者后清理 |
| MV 内部撤回计数 | `planner/imv_rewrite/aggregate_rewrite.rs` 的 `RetractionCount` 对已有状态加本批增量。CASE 先处理旧行缺席，已有状态走原 Binary 加法、`AllowThrowException` 参数和原算术 recipe | MV 状态合并定型迁移时核对存量状态和溢出契约 |
| 宽数及集合比较 | `resolve_expr.rs::analyze_comparison_predicate` 按原递归类型选择路径。保留的宽数即使收敛成 DOUBLE，仍沿原 Binary 比较。有符号整数叶 ARRAY 的普通等值走共享比较，缺安装的复合类型报告准确拒绝。JOIN USING 的已准入宽数跟随原比较 | TRC-6b 核对宽数退出。B7e 沿普通等值作者扩展集合范围 |
| 字符串或布尔与数值的显式比较 | `resolve_expr.rs::analyze_comparison_predicate` 识别原混合输入，用 `comparison_common_value_type` 冻结原数值转换及 Binary 比较。比如 `VARCHAR '10' > 5` 保留原数值结果。JOIN USING 和简单 CASE 仍使用各自的原公共类型作者 | TRC-6b 收敛混合域准入和比较转换 |
| 子查询成员比较 | `subquery_rewrite.rs` 保管原 `IN`、`NOT IN` 的值相等、相关资格和 NULL。已准入的不相关复合 IN 保留实际右关系，由普通等值和嵌套循环标记连接归约。标量仍有 Binary 与冻结键消费者。由 `join_non_finite_float_key_null_semantics`、`join_null_aware_anti` 和复合标记连接用例保护 | B7e 扩展普通比较能力，标量存量路径按原成员语义演进 |
| NOT、LIKE、IN、BETWEEN | SQL `analyze_expr_impl` 与 `analyze_predicated`，表达式层的 `evaluator/guarded.rs`、`evaluator/utf8_inlist.rs` 与既有 Functions recipe | 相应语义改动迁入。BETWEEN 的迁移引入单次求值绑定 |
| IS [NOT] TRUE/FALSE | SQL `analyze_predicated` 以已绑定 Identical 及原 NOT 表达 TRUE/FALSE 测试，UNKNOWN 走原 IsNull。契约、编解码和求值仍保留 `IsTruthValue` 的存量入口 | UEA-5E B8c 退役原产出后清理 |
| 内部值转换 | SQL `analyzer/value_conversion.rs` 和 Functions 的存量值域转换仍有内部调用。公开 CAST、TRY_CAST 和 TRY 的语境及错误合同见 [ADR-0252](../../adr/ADR-0252-trino-casts-and-row-error-scopes.md) | 按原消费者收口 |
| 简单 CASE | SQL `analyze_case` 保留当前共同类型。在宽数转换前选择匹配方式，表达式单元冻结该选择。CASE 仍只求值一次主表达式并按剩余行求值标签。宽数保留原比较 | TRC-6b 涉及宽数准入时复验 |
| 时间值、精度与运算 | SQL `resolve_expr.rs` 中的`date_trunc` 与间隔专用定型，Functions 对应时间/日期 owner | TRC-6c 先统一时间类型与运算，按名特设随函数迁移 |
| 聚合、窗口与 MV 状态 | SQL `analyzer/functions.rs` 与 `resolve_expr.rs` 的特设参数、类型和校验，`planning/mv.rs` 另有窗口判断名单。关系调用沿原函数 owner 的 ABI | TRC-6d 收敛重复定型和窗口名单，保留独立关系调用生命周期 |

路径以 `novarocks/` 为根。SQL 符号以 `novarocks/sql/src/` 为根。

## 按名特设和高阶函数

[共享迁移判定](../../../novarocks/expression/src/typing/route.rs) 列出具名函数和可达别名。迁移整个函数时同步 SQL 与文本入口的归属。

- **结构与语法特设，由 TRC-6d 迁移。** `resolve_expr.rs` 保有 `strpos`、`normalize`、`variant_get` 及别名，以及 FIELD 的参数规范化和公开名称 `negative` 的原拒绝。集合调用的已迁入重载通过目录定型，`TABLE(sequence(...))` 沿独立关系入口准备必需常量。 两参 IF、NULLIF 行需求及 TYPEOF 原实参求值已迁入共享定型与执行。 [ADR-0255](../../adr/ADR-0255-collection-calls-and-relational-sequence.md) · [ADR-0263](../../adr/ADR-0263-trino-scalar-values-and-publication-eligibility.md)
- **字符串预转换，由 TRC-6d 迁移。** SQL 的 `apply_implicit_string_function_casts` 保有拼接、TRIM、长度与大小写、正则、替换、拆分、子串等具名规则，实际实现由 Functions 对应 owner 执行。
- **按名校验，由 TRC-6d 迁移。** `analyzer/functions.rs::validate_scalar_function_call_impl` 仍承担部分数组约束、固定参数个数及 MV 状态辅助函数规则。MAP 构造的键校验由已安装的原 kernel 承担。
- **高阶函数与 lambda。** 公开数组和 MAP 的高阶重载按目录声明，缺少可执行 ABI 时报告具名能力拒绝。UEA-5E B7c 负责两段式定型和执行。
- **ROW 安装缺口。** `ROW(...)` 沿 Parser 语法入口定型，内部 IVM 使用明确的构造身份。元数据声明 `builtin.scalar/row/dynamic-v1`，原生缺项由 UEA-5E B7a 承接。

## 规划期和降级期

| 遗留面 | 原代码位置与后续收敛 |
| --- | --- |
| 规划与改写决定 | `optimizer/scalar/` 的重新绑定、`analyzer/subquery_rewrite.rs`、`analyzer/resolve_from.rs` 的 USING、IMV 改写与谓词下推重建 CAST。相应改动迁移本构造，并保留实际列与关系语义 |
| 降级重算 | `contract_lowering/scalar_nullability.rs`、`compiler/selected_scalar_call.rs` 的转换选择和 `canonical_null_target` 承接旧构造。各构造迁移后移除其语义重算，真实物理 NULL 传播继续由原单元事务维护 |
| 共享旧结构 | Binary、IN、BETWEEN 和各自的原 Functions recipe 仍有生产消费者。CAST 及需此设置的存量算术继续签发 `AllowThrowException`，混合宽数和 CAST 保留原 `DecimalOverflowPolicy`。 `decimal_overflow_to_double` 随 TRC-6a 退役 |

## 已知差异与缺陷

- LARGEINT 减法沿 `functions/src/arithmetic/largeint.rs` 保留现有行为，随 TRC-6b 的准入和遗留覆盖审查。
- SQL 的 `analyzer/helpers.rs` 与 `analyzer/resolve_from.rs` 仍有直接从 AST 常量取负的路径，最小 i64 的受检语义需由原消费方修复。
