---
id: ADR-0266
title: "Array scalar overloads and host admission"
domain: [sql-compiler, sql-language]
status: active
supersedes: []
superseded-by: null
date: 2026-11-24
provenance:
  - "UEA-5E B7b spec v3 and plan v2"
  - "Trino 484 e2d50bb733e193607c44be84ad80ce2430ba1d01"
code-anchors:
  - "novarocks/functions/src/builtin/catalogue.rs (overload selection)"
  - "novarocks/value-arrays/src/hash_eq/key.rs (PreparedKeyOperations)"
  - "novarocks/functions/src/selected_copy/list_output.rs (list output preflight)"
  - "novarocks/functions/src/builtin/array_join_owner.rs (element CAST)"
  - "novarocks/functions/src/builtin/array_generation_owner.rs (repeat and scalar sequence)"
---

# ADR-0266 数组标量重载与宿主准入

## 问题

安装 Trino 484 的普通数组重载时，如何统一 FE 折叠和 BE 批执行的值行为、错误与资源准入？

## 裁决

1. SQL 和 Expression 冻结准确重载、完整类型、转换与实参求值顺序。目录按准确重载发布安装事实。自然排序、比较器排序、标量序列和表序列分别保留自己的实现合同。
2. `contains`、`array_position`、`array_remove`、`contains_sequence` 和 `arrays_overlap` 使用 B7a 的普通等值，按各函数的访问和空值规则归约结果。`array_distinct` 与 `array_intersect` 使用同一规范身份和相容哈希，输出最先出现的代表。交集沿较长输入的顺序，等长用左侧。自然排序和极值使用共享顺序契约，`zip` 按位置形成匿名 ROW。
3. 根据实际选行预检可达输入、List 几何和所需输出。共享构造入口使用原宿主准入与收据。临时索引、展开结果和实际复制所需的缓冲按原资源模型收费。共享子缓冲的最后实际别名退役时结算。List 偏移溢出沿原 Representation 失败。标量 `sequence` 保留 10,000 项上限，`repeat` 保留 100,000 次上限，计数不依赖载体。
4. `array_join` 按准确元素类型使用 VARCHAR 转换的原作者。行的数据失败按原 CAST 类别传播，`TRY` 按原行错误作用域捕获。未安装的准确转换按绑定期错误处理。控制、容量及表示失败继续沿首因报告。

## 固定参照的分歧

| Trino 484 行为 | NovaRocks 行为 |
| --- | --- |
| `repeat` 核对 4 MiB Block 逻辑大小。 | 结果按宿主的实际申请与 List 表示能力准入。 |
| 通用 `array_distinct` 与 `array_intersect` 核对 4 MiB BlockBuilder 增量。 | 索引和输出经同一次宿主准入，满足预算与表示时可超过该字节数。 |
| `array_join` 将元素数据错误包装成 `GENERIC_INTERNAL_ERROR`。 | 传播原 CAST 类别，例如 `INVALID_CAST_ARGUMENT`，并遵循现行 `TRY`。 |
| `slice` 的终点加法可能回绕。 | 有效起点后的请求按实际剩余长度截断。 |
| 高精度年月 `sequence` 以 epoch 微秒判定方向。 | 使用完整时间戳精度判定方向。 |

拒绝模拟 Trino 的 JVM Block 字节口径。这会把 Java 特化、RLE 和嵌套布局变成 SQL 准入条件，并与实际内存预算同时生效。拒绝复刻错误重分类和两处边界缺陷。计数限制的判断只取决于实参，按已批准的集合契约保留。

## 兼容与诊断

本次安装进入封存函数目录，由原 FE/BE 兼容岛校验目录及 pure 摘要。诊断先核对 `--build-info` 与准确重载，再从 Trino 错误名和内部原因区分参数、转换、资源及表示失败。需要验证向量域时，通过同一实际列对照常量输入，检查值、错误和结果声明。

以 `slice(ARRAY[1,2,3], 2, 9223372036854775807)` 验证截断，以 `TRY(array_join(ARRAY[JSON '[1]'], ','))` 验证 CAST 的行错误范围，以同一 epoch 微秒内不同皮秒的两个时间戳验证年月步长方向。宽数组诊断从真实列生成数据并返回基数，以隔离调用预算和客户端输出预算。扩充元素域时，复查完整类型上的关系、公共转换及真实输出费用。

## 相关合同

- [ADR-0239](ADR-0239-expression-evaluation-and-host-admission.md) 的表达式求值与原宿主准入。
- [ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md) 的 CAST 与 TRY。
- [ADR-0255](ADR-0255-collection-calls-and-relational-sequence.md) 的集合目录与独立表序列。
- [ADR-0265](ADR-0265-row-declarations-and-canonical-map-keys.md) 的 ROW 声明与规范键。
