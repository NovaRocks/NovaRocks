---
id: ADR-0220
title: "Canonical JSON membership and UTC Variant timestamp rendering"
domain: [sql-compiler, join-execution, sql-language]
status: active
supersedes: []
superseded-by: null
date: 2026-11-06
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1283"
  - "discussion: 2026-11-03 Trino 483 JSON equality and deterministic Variant rendering"
code-anchors:
  - "novarocks/functions/src/json_canonical.rs (normalize_json_key)"
  - "novarocks/sql/src/analyzer/resolve_expr.rs (check_json_comparison_domains)"
  - "novarocks/sql/src/analyzer/subquery_rewrite.rs (JSON mark admission and original key bindings)"
  - "novarocks/sql/src/planner/logical/build/subquery.rs (LeftMark planning)"
  - "novarocks/sql/src/optimizer/derive/hash_join.rs (hash_join_only_broadcast)"
  - "novarocks/local-compiler/src/join.rs (LeftMark lowering)"
  - "novarocks/execution/src/exec/operators/hashjoin/compiled_hash_join/phased_probe.rs (ProbeMatch, advance_probe)"
  - "novarocks/execution/src/exec/expr/compiled_program/utf8_inlist.rs (utf8_equality_observed)"
  - "novarocks/types/src/value/variant.rs (format_variant_timestamp)"
---

## 问题

JSON 值的 IN/NOT IN 如何在分布式执行中保留逻辑类型、相等与三值语义，同时让 Variant 时间戳的文本表示不受会话、主机或执行时刻影响？

## 背景与执行事实

JSON、VARCHAR 和 Variant 可以共享部分物理载体，但它们不是同一个逻辑域。JSON 对象的键顺序、重复键、数字 scale 与 Unicode 表示影响其比较政策；载体相同不授予隐式跨域比较。规范化比较键与原始存储文本也有不同用途，不能用其中一个重写另一个。

| 组件 | 唯一责任 |
|---|---|
| SQL analyzer | 证明两侧为 JSON、冻结准入位置与原始函数绑定，拒绝跨域与尚未支持的形态 |
| Functions `normalize_json_key` | 从合法 JSON 产生统一比较键；保留原内存与取消作者 |
| SQL planner/optimizer | 将 probe/RHS 各自物化并投射规范键，选择原 LeftMark 与合法分布 |
| 原 Execution hash join | 每个 probe occurrence 输出一个 nullable Boolean，消费完整 build 的相等、NULL 与非空事实 |
| Types Variant renderer | 纯值转换，统一微秒/纳秒与有无时区的格式；不读取环境时间或时区 |

共享 build、数据发布、实际 E/F/Q/T 与真实退出继续由 ADR-0212 的原 join 框架拥有。这里补充其 mark 输出用途，不另建 live RHS 注册表、执行器或完成协议。

## 考虑过的选项

1. **按 Arrow Utf8 一律作 JSON 比较：设计否决。** 它会把普通 VARCHAR 的字节语义改变成 JSON 相等，并把 Variant 的序列化文本误认成 JSON 逻辑域。比较必须消费 analyzer 冻结的逻辑身份。
2. **每对 JSON 用通用 JSON 值对象比较：设计否决。** 通用数字等值可能擦除 scale，Rust Unicode-scalar 字符串也不能完整表达 Java UTF-16 的孤立代理与键排序。重复解析还不能提供可复用的哈希相等类。采用一个显式、可哈希的规范键作者。
3. **专用 membership 执行节点或逐对 nested loop：设计否决。** 独立节点会重复 build、三值、分布和退出机制；逐对比较无需建键，却与已选择的哈希语义和复用方向不符。原 LeftMark 消费规范字节键，以现有 KeyIndex 与 null-aware facts 实现归约。
4. **按规范键直接哈希分区：待评估。** 它能减少每个目的 Task 的完整 RHS 驻留，但每个分区必须知道 RHS 是否为空、是否含 SQL NULL。当前没有复制 NULL 与任意行的交换合同，普通 shuffle 不足以保留三值语义。
5. **Variant 渲染使用会话或主机时区：设计否决。** 同一值的文本会随节点、会话或查询时刻变化，并引入时区快照及过期政策。时间戳值的规范渲染采用 UTC，不让进程环境成为第二来源。

## 裁决

1. **逻辑域规则。** JSON membership 两侧必须由原分析证据证明为 JSON。JSON 与 VARIANT、普通 STRING 或数值的比较在分析期拒绝；SQL NULL 仍保留原三值规则。普通 VARCHAR IN 按字节相等，执行侧不以文本内容猜域，也不保留 serialized Variant 的 UTF8 比较回退。
2. **单一规范键规则。** Functions 唯一拥有 JSON 规范化：对象键按 UTF-16 code unit 排序、重复键取最后一个、数组保持顺序、字符串按解码内容比较且接受孤立代理、忽略非语义空白。没有小数点或指数的整数精确保留；十进制与指数按保留 scale 的 Java BigDecimal.toString 表示。因此 `1.0 != 1.00`、`1e2 != 100.0`、`100e0 = 100`、`-0.0 = 0.0`。JSON 文本 `null` 是一个值，不是 SQL NULL。
3. **原值与比较分开规则。** 规范键在 probe 所属行域与 RHS producer 各投射一次，之后按键字节哈希相等；它不改写原始落湖值或显示文本。IN-list 消费同一规范键作者，未来 JSON 输出规范化应复用该作者，而不是维护另一套近似相等规则。
4. **原 mark join 规则。** 已准入的值形态使用 Hash LeftMark：左侧为 probe，右侧为 build；保留所有左侧原列、行数与重复次数，增加一个 nullable Boolean mark。NOT IN 只在归约后的 mark 上否定一次。空 build 也输出每个 probe，不能套用“空输出”的快路。
5. **完整三值规则。** RHS 空时 IN 为 FALSE，即使 probe 为 SQL NULL；只要有相等键则 TRUE；否则 probe 为 SQL NULL或 RHS 含 SQL NULL 时为 UNKNOWN；其余为 FALSE。不能预先过滤 build 来改变 NULL 或非空事实。必要的规范化错误在投射处产生，不因匹配结果提前跳过。
6. **准入与分布规则。** 只开放非相关、单列、标量 JSON operands 的 SELECT 值形态及 WHERE/HAVING 中 OR 的操作数形态；AggregateInput、JOIN ON、相关、多列、顶层筛行 IN 以及仅被 NOT/CASE 包含的 WHERE/HAVING 形态仍明确拒绝。RHS 原有 DISTINCT、过滤或排序不因准入而剥除。分布只能是 Singleton，或 probe SingleCopy 加完整广播 build；不以普通 hash shuffle 假装已有 null-aware 全局事实。
7. **原资源与退出规则。** 每个目的 Task 使用原共享 build、原 KeyIndex 与原实际 driver；本地 DOP 共用一个 build。原输入封存、唯一数据发布、背压、取消及实际退出沿 ADR-0212；规范化临时分配、键和 mark 输出沿真实受检 owner，容量失败保持类型化失败。不新增 JSON 大小、深度或键数配额，不自动换算法、spill 或跨 Task registry。
8. **Variant 时间戳规则。** Variant 转 JSON/文本中的带时区时间戳统一按 UTC 渲染，微秒为 `YYYY-MM-DD HH:MM:SS.ffffff UTC`，纳秒保留九位小数；无时区时间戳保留同样数位但没有时区后缀。以欧几里得除法拆解 epoch，负值也保持正确日期与小数。FE/BE 复用相同纯函数，不读取会话时区、主机 Local 或当前时间。超出可表示日期范围时类型化拒绝，不回退 epoch。

## 接受的妥协（诚实记录）

- JSON 相等不是抽象“数值相等”：scale 与指数的规范表示属于所选 Trino 483 政策；Java UTF-16 排序和孤立代理需要独立规范化逻辑，不能直接交给 Rust String 的排序。
- 广播 build 让每个目的 Task 驻留完整规范键集合。没有 null-aware 分区交换前，优先保持准确结果；不足沿原预算拒绝，不借一个词汇项宣称分区能力。
- 已开放的 JSON IN 位置有限，规范键可用不表示一般 JSON join/group/distinct 或所有布尔位置已经开放；这些能力须分别证明原行域、NULL 与分布合同。
- 规范化增加投射和中间键成本，但避免逐 pair 重复解析，并让 IN-list 与子查询使用相同相等政策。有效性、功能结果与全量验收仍分别表达。
- Variant 的 UTC 裁决只覆盖此处的时间戳渲染。binary、浮点、decimal 等其余原语，以及其他 SQL 时间函数的会话时区政策，不由本条声称已全部对齐。

## 何时重新评估

- 提供“向所有分区复制 NULL 与任意一行”的冻结交换合同后，重新评估 LeftMark 与 null-aware anti 的按键分区，同时验证空 RHS、NULL、重复 probe、取消和实际退出。
- 扩展顶层筛行 IN、相关、多列或一般 JSON join/group/distinct 时，先在 SQL 原作者上定义其行域、等价类、NULL 与放置合同，再消费同一规范键；不静默放宽现有准入。
- 产品兼容目标或 Trino 的 JSON 规范化发生变化时，按原规范键与已有独立 oracle 重评；不得让不同消费者各选一套数字或 Unicode 规则。
- 统一 JSON 输出/显示或其余 Variant 原语时，明确哪些是比较键、哪些是用户文本以及精度合同，避免重写存储身份或恢复 Local 时区来源。
