---
id: ADR-0256
title: "Temporal calls freeze overloads and selected-row effects"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-23
provenance:
  - "discussion: 2026-11-23, exact Trino temporal scalar contracts and persisted-definition cutover"
code-anchors:
  - "novarocks/functions/src/builtin/date_shift_owner.rs (DateShiftResolver)"
  - "novarocks/functions/src/builtin/temporal_context.rs (TemporalContextResolver)"
  - "novarocks/sql/src/optimizer/rewrite/rules/fold_constant/admission.rs (admit_result)"
---

## 问题

时态调用怎样在 SQL 定型、常量折叠、分布式执行和已发布数据之间保住同一套输入、结果和错误语义？

## 背景与执行事实

[ADR-0254](ADR-0254-exact-temporal-values-and-frozen-statement-time.md) 规定值时区、精度和语句环境的冻结边界。[ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md) 规定行错误及 `TRY` 的捕获范围。时态调用的重载在这两个边界上分别携带完整类型、精度、环境依赖和选中行值错误。使用同一函数名的不同重载，例如单参和显式时区的 `from_unixtime`，可携带不同环境依赖。

| 所有者 | 契约 |
|---|---|
| Parser 和表达式定型 | `EXTRACT` 尊重字段和 interval 限定符。普通调用依已公开的重载绑定。 |
| Functions | 历法、时区和模板规则按确切重载执行。调用读取原选中行与已冻结参数，DATE 产出须先核对有符号 32 位日序。 |
| FoldConstant | 编译原调用并核对封存事实。行失败和可选结果的封存限额保留原表达式。 |
| Catalog 和 MV | 定义、基表和实际发布各有身份。定义可解析仍须确认现存发布和后续维护所用语义。 |

## 考虑过的选项

- 继续按名字统一禁折，并按宿主环境解释日期。设计否决。显式时区与会话时区重载的输入不同，FE、BE 和重试必须消费可验证的同一输入。
- 同时支持可配置语言和再解释旧 MV 发布。待评估。两者分别需要冻结会话语言和保真核对已物化值的机制。
- 采用精确重载和选行错误，沿现有管理权完成涉及语义变化的 MV 切换。接受。调用和发布各保留自己的输入与验证边界。

## 裁决

1. **在原重载上定稿。** 历法源类型、日内精度、值时区及必要的隐式转换由同一调用保存。真实缺失实现的已声明重载返回能力错误，安装清单只包含实际内核。
2. **按实际调用选行求值。** 严格 NULL 和控制流领先于值校验。DATE 结果超界与未知 epoch 区名保留独立原因。行数据失败继续由原 `TRY` 捕获表裁定。
3. **由调用效应决定折叠。** FE 保留原调用、转换和语句环境。可选折叠的结果超出常量封存范围时保留原调用，必需常量与实际资源、控制失败沿原契约返回。
4. **格式和解析共享固定语言。** 英文月名、星期及半日词汇固定为 `en-US`。精确模板和通用字段模板都消费原文，结果沿准确时间戳或字符串类型输出。
5. **发布切换要求验值。** 定义或函数语义变化时，沿实际管理权取得经核对的新发布，再开放直读、改写与维护。旧语义回退用经验证的旧发布或旧版 FULL 重建。见[切换规程](../guides/iceberg-v3/materialized-views.md)。

## 接受的妥协（诚实记录）

DATE 中间值在每个时态调用返回时就要符合 NovaRocks Date32 载体。Trino 能继续消费某些超出 Date32 的中间日序，因此嵌套调用在该边界有明确差异。本期同时固定英文格式词汇，并保留已声明的 interval 字段待实现状态，避免将会话语言或通用 MV 效应扩进这次切换。

## 何时重新评估

- 有实际的会话级语言需求时，先定义语言参数在准入、Native 执行、折叠与持久化定义之间的冻结合同。
- 改变 DATE 载体或升级 Trino 语义基线时，重新核对临界中间值、DST、模板字段和错误分类。
- MV 具备可重放的语句环境或可证明的跨版本发布语义时，重新评估旧发布保真核对与切换中可免 FULL 的条件。
