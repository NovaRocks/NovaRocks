---
id: ADR-0254
title: "Exact temporal values and frozen statement time"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-23
provenance:
  - "discussion: 2026-11-21 Trino 484 temporal types and statement time semantics"
code-anchors:
  - "novarocks/type-contract/src/temporal_type.rs (TemporalType, TemporalPrecision)"
  - "novarocks/query-application/src/sql/session.rs (statement_time)"
  - "novarocks/functions/src/temporal_zone_rules.rs (TemporalZoneSource, temporal_zone_rule_material)"
  - "novarocks/connector/iceberg/src/temporal.rs (declaration, from_storage, to_storage)"
  - "novarocks/trino-adapter/src/request.rs (PARAMETRIC_DATETIME, PARAMETRIC_INTERVAL)"
---

## 问题

怎样让时间类型、精度与时区语义贯穿 SQL、分布式执行和客户端，同时让重试与常量折叠对当前时间和夏令时边界作出一致判断？

## 背景与执行事实

- Trino 484 的 `TIME` 和 `TIMESTAMP` 支持 0 到 12 位小数。省略声明精度时取 3，字面量的尾零也参与精度推断。带时区值有逐值身份，等价比较按时间域归一化。
- Trino 484 的时间运算分别使用 JDK 和 Joda 规则。历史偏移、夏令时缺口与重叠会暴露规则和操作之间的差异。
- Iceberg 的存储类型有独立精度。既有 NovaRocks 视图中的裸 `TIMESTAMP` 还承载过微秒精度语义，重新分析同一文本可能改变已有视图的声明和结果。

## 考虑过的选项

1. **复用单个 Arrow 时间戳及列级时区。否决。** 纳秒载体丢失更细小数，列级元数据无法在同列保留逐值时区，统一标量整数还会缩小可表示日期范围。
2. **各执行进程读取系统时间与本地 tzdata。否决。** 折叠、不同 BE 与重试会观察不同输入，单一规则库也会改变 Trino 484 的既有边界行为。
3. **冻结语句环境，保留精确值域。采用。** 类型与值契约贯穿编译和执行，在连接器存储与客户端呈现处各自应用原边界规则。

## 裁决

1. **类型声明与实际值精度共同冻结。** 无时区且不超过微秒的时间值可用紧凑载体，高精度值以整数粗粒度部分和余数组合。带时区时间与时间戳携带逐值时区。带时区时间按由语句日期求得的偏移计算 UTC 日内比较键，带时区时间戳按瞬时点比较。等价、哈希和排序共享归一化规则，返回代表值时保留该值的时区身份。
2. **原语句拥有当前时间。** Query Application 在准入时捕获一次时间点，并依次从 SQL 会话属性、Trino 请求时区和 FE 默认值选取有效时区。表达式、常量折叠、BE 和重试消费同一冻结环境。Functions 按操作选择随版本封存的 JDK 或 Joda 规则，规则材料参与 Native 兼容性摘要。
3. **Iceberg 拥有存储语义。** 写入精度取自目标存储声明，按 Iceberg 版本应用微秒或纳秒目标。读取已持久化的带时区时间戳时使用 UTC。谓词裁剪另行做保守投影，嵌套键在舍入后须核对唯一性再写入。已有视图通过显式精度迁移保留微秒语义。MV 在发布前拒绝依赖语句时间或会话时区的定义。
4. **Trino Adapter 拥有客户端呈现。** `PARAMETRIC_DATETIME` 和 `PARAMETRIC_INTERVAL` 决定协议呈现，内部声明、连接器存储和 Native 载体保持准确的类型事实。

## 接受的妥协（诚实记录）

- 升级规则基线时须一并维护 JDK 与 Joda 的离线规则材料、边界对照和 Native 兼容性。
- Iceberg 的时间以微秒存储，时间戳依格式版本使用微秒或纳秒。SQL 层的皮秒精度、带时区时间和 interval 具备独立语义，持久化能力受 Iceberg 目标类型约束。
- 已有视图需要精度迁移才能保留旧语义。依赖语句环境的 MV 定义在具备可重放语义前保持显式拒绝。

## 何时重新评估

升级 Trino 语义基线或规则版本、Iceberg 增加原生时态存储能力，或 MV 具备可重放的语句环境时，重新核对标准值、历史偏移、夏令时边界、已持久化定义和 FE 与 BE 的原生兼容性。
