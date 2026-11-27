# 表达式层遗留面

M06a-14 收回原清单中的表达式迁移项。运算符经共享定型选定目录重载，冻结包以原实参、效果与使用位置供 FE 常量求值和 BE 执行消费。词法绑定与首错合同见 [ADR-0279](../../adr/ADR-0279-argument-demand-preserves-sql-errors.md)，选型与转换分工见 [ADR-0244](../../adr/ADR-0244-operators-are-identified-catalogue-functions.md) 和 [ADR-0245](../../adr/ADR-0245-scalar-typing-finalizes-the-original-overload.md)。

| 构造 | 现行入口与规则 |
| --- | --- |
| 标准及内部宽数算术 | SQL 和 Expression 共享选择调用。Functions 已选重载保管标准 Trino 数值域及仍可达的内部宽数。MV 撤回计数先经 CASE 判定旧行，再调用原整数状态加法。 |
| 比较与关系键 | 普通比较、混合显式转换、CASE、JOIN USING、子查询和 IMV 从各自原输入定型并保留原选定签名。普通浮点使用 IEEE 谓词等值。复合值和 NULL-safe 等值由各自已选内核执行。 |
| NOT、LIKE、IN、BETWEEN 和真值测试 | SQL 隐藏身份区分普通比较、真值与不定值。IN 冻结有序候选组。BETWEEN 对非平凡探测引入一次求值的 Let，布尔需求沿实际使用位置传播。 |
| 转换和 typed NULL | CAST、TRY_CAST、隐式转换与写入赋值保留原转换语境。共享定型保管 typed NULL，降级在实际端口复核选定结果。见 [ADR-0252](../../adr/ADR-0252-trino-casts-and-row-error-scopes.md)。 |
| 公开标量与专用语法 | 普通公开调用、字符转换和参数校验由已安装作者定稿。解析器保留 ROW、日期时间与其它专用语法的原投影。时间调用保留已冻结的时区、精度和语句环境。见 [ADR-0256](../../adr/ADR-0256-temporal-calls-freeze-overloads-and-selected-row-effects.md)。 |
| 聚合、窗口、表函数与 MV 状态 | SQL 读取原目录及稳定重载。实参转换、生命周期与准备仍沿实际关系调用 ABI。MV 状态算法保留自己的输入和产物责任。 |
| 高阶函数与 lambda | 已装重载走共享两段定型。原 HigherOrderV1 负责父行的下一体请求和受检结果。none_match、reduce、zip_with、map_zip_with 保留具名能力拒绝。 |
| AST 整数常量 | 窗口界和内部快照范围共享解析后受检求值。两个入口保留各自原位置与错误投影。公开 VERSION AS OF 沿原字面量合同。 |

Unary、Binary、InList、Between、Like 和 IsTruthValue 已退出冻结表达式词汇。原 wire 标签永久保留。老版本的值需求、行错误与 MV 刷新影响见 [ADR-0279 的版本影响](../../adr/ADR-0279-argument-demand-preserves-sql-errors.md#版本影响)。
