---
id: ADR-0270
title: "Native JSON values and Binary membership keys"
domain: [sql-language, functions, physical-plan, arrow-batch, root-result, iceberg]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0220
    scope: "Items 2 and 3 on JSON representation and 483 equality, and the JSON normalization and depth details in item 7."
    link: "ADR-0220-canonical-json-membership-and-utc-variant-timestamps.md"
date: 2026-11-25
provenance:
  - "7B3F F6 spec v3 and plan v4, approved vault snapshot 54716657ad5cd68ace75df62d4894975b4b5c66a"
  - "Trino 484 source e2d50bb733e193607c44be84ad80ce2430ba1d01 and fixed 484 terminal-page oracle"
code-anchors:
  - "novarocks/functions/src/json_value.rs"
  - "novarocks/plan-codec/src/physical_package_v2/decode.rs (validate_json_constants_in)"
  - "novarocks/execution/src/runtime/exchange/online_unit.rs (decode)"
  - "novarocks/functions/src/json_canonical.rs (normalize_json_key)"
  - "novarocks/functions/src/native_inlist.rs (JsonProbeKeys, JsonConstantKeyCache)"
  - "novarocks/result-render/src/trino_json.rs"
  - "novarocks/connector/iceberg/src/commit/validation.rs (deny_column_definitions)"
---

## 问题

SQL JSON 怎样在 FE 定型、BE 求值、Exchange 与结果页之间保留 Trino 484 的原值，并继续支持已有 IN 位置的等值与三值语义？

## 背景与执行事实

JSON 对象保留成员原顺序和重复次数。数值、带时区时间和日期同时有用户可见的呈现与可比较的值。
同一时间点的两个带时区时间戳可以渲染出不同的区域标识。两个数值相等的十进制也可以保留不同的标度。
SQL NULL 和 JSON null 各自有原值。在 Trino HTTP 的 JSON 列中，JSON null 呈现为字符串 `"null"`。

## 考虑过的选项

- UTF-8 文本外加类型旁路会让值、精度和时区身份在常量、递归 Arrow 容器、选行和 Exchange 中各自迁移。单一版本化值承载这些事实。
- 使用通用对象 map 会合并重复键及原顺序。采用有序成员和受检目录，对象比较再独立归约为含重复次数的成员多重集。
- 每对值做文本比较会重复扫描，且无法支持原哈希 mark 的键。已有 mark 用隐藏的普通 Binary 投影消费原比较和哈希。

## 裁决

1. **唯一计算载体。** 完整声明的 `ValueLogicalType::Json` 使用 `LargeBinary` v1。根标记后正好一个带类型 item。SQL NULL 由 Arrow validity 表示。
   顺序容器按原序保存成员，从 8 个成员起选择可随机访问的最小宽度目录。索引对象按原 UTF-8 键及 ordinal 排序目录，
   查找重复键时按原 ordinal 返回。长度和数目用最短 ULEB128，精确数字的有符号系数用最短小端二补码。
   已冻结常量保留可见类型、浮点位、标度、时区和成员顺序作为原值事实。
2. **按边界校验。** Package 解码在常量恢复之后、绑定请求之前完整验证每个可达 JSON 值，跳过父 NULL 和无关载体。
   同兼容岛的 Exchange 使用精确 Arrow 布局及可达根范围。InternalScalar 保留版本化 JSON bytes，
   记录作者检查帧与类型，Native 标量作者校验 JSON 根范围。访问器每次解读实际触及的项和范围。
   计划修订 45、结果 descriptor 标签和密封函数目录共同区分本次原生切换，继续使用执行 epoch 18。
3. **共用扫描与呈现。** JSON 字面量、`json_parse`、构造和选中行转换共用值合同。文本经相同扫描作者测量并直接写最终区域。
   容器支持 1,024 层，独立于 SQL 类型深度。结果页、`json_format` 与 `JSON_SERIALIZE` 共用文档写出，
   公共 JSON 列仍输出 Trino 类型签名和字符串值。到 VARCHAR 的 CAST 按标量转换合同求值。TRY 只接收原允许的行错误。
4. **Binary 等值。** 有限数字按精确系数与 scale 归约，DOUBLE 用 Java 最短十进制，REAL 先精确拓宽。
   同值整数、小数和浮点共享等值键，有符号零归为一个键。NaN、正无穷和负无穷各有键。
   时态按原域、准确时间坐标与时区归约。
   数组保序，对象排序完整成员配对并保留次数。列表 probe 按实际需求在调用帧归约一次，直接常量由原表达式实例惰性持有键。
   动态候选仍按原位置求值。mark 的放置、SQL NULL 三值和原 Hash LeftMark 沿 [ADR-0220](ADR-0220-canonical-json-membership-and-utc-variant-timestamps.md)。
5. **外部目标准入。** Iceberg Provider 使用同一递归定义遍历拒绝实际 JSON 目标，包括集合叶、新 CTAS 和 MV。
   已有目标和缺失路径先由原分支裁决。实际 MV 存储映射也拒绝 JSON 到 STRING。需持久化文本时，
   使用普通 VARCHAR 声明和显式序列化。
6. **资源和退出。** 扫描、构造、规范键及写出按实际字节和几何向已有作者申请空间，扩容期间保留旧与新存储的责任。
   构造、排序和遍历复用工作检查点，归还随最后实际别名。计划、数据与客户结果继续消费原退出和终页合同。

## 接受的妥协

重复键对象的规范键需要按实际配对排序。字节原值保留用于展示、常量身份和后续路径访问。已有 mark 放置仍可使用完整广播。

## 何时重新评估

扩大 JSON 关系位置、开放 SQL/JSON 路径、引入 Variant 转换或新增可写外部存储类型时，重新验证各自的签名、行域、错误及归属。
从新外部信任边界接收 native JSON 前，确定一次性完整验证和资源 owner。
