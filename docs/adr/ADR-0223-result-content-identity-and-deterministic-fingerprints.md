---
id: ADR-0223
title: "Result content identity is explicit and fingerprints remain evidence"
domain: [join-execution, pipeline-execution, frontend-mv]
status: active
supersedes: []
superseded-by: null
date: 2026-11-07
related:
  - "ADR-0205-shared-local-key-index-and-consumer-row-topology.md"
  - "ADR-0195-public-value-access-and-construction.md"
  - "ADR-0222-typed-task-summaries-and-per-kind-completion.md"
provenance:
  - "discussion: 2026-11-07 explicit result-content identity within admitted capabilities, first-admitted representatives and deterministic evidence"
code-anchors:
  - "novarocks/execution/src/exec/hash_table/key_index/prepared.rs (KeyEquality, KeySpec::with_equalities, prepare_projection, PreparedKeyBatch::encode_component_row)"
  - "novarocks/execution/src/exec/hash_table/hash.rs (content_f32_bits, content_f64_bits)"
  - "novarocks/execution/src/exec/hash_table/key_builder.rs (append_content_nullable_tracked_observed, ComparingKeyByteSink)"
  - "novarocks/execution/src/exec/hash_table/key_index/pairs.rs (compare_prepared_pairs, compare_fixed_pairs)"
  - "novarocks/execution/src/exec/hash_table/key_index/local_hash.rs (fixed_hashes, complex_hashes)"
  - "novarocks/execution/src/exec/hash_table/key_index/canonical_store.rs (OwnedCanonicalColumn)"
  - "novarocks/execution/src/exec/hash_table/key_index/content_fingerprint.rs (PreparedKeyBatch::content_fingerprint)"
  - "novarocks/execution/src/exec/hash_table/key_index/content_tests.rs (content identity, representative and fixed-vector tests)"
---

## 问题

同一个本地键索引怎样同时表达 SQL 分组与结果内容身份，并让准确比较、代表导出和跨 Task 核验证据保持一致？

## 背景与执行事实

SQL 分组相等回答能否共用一个组；内容身份回答一个已准入的结果值属于哪个内容类。两者须显式选择。[ADR-0205](ADR-0205-shared-local-key-index-and-consumer-row-topology.md) 的唯一实例内索引、消费者行拓扑与分布式边界继续有效；本条补充它的逐列相等政策，不改变已有消费者的 SQL 分组行为。

| 作者或政策 | 拥有的事实 | 边界 |
|---|---|---|
| `KeySpec` / `KeyEquality` | 随完整列类型冻结 `SqlGrouping(NullPolicy)` 或 `ResultContent` | 不授予 SQL、函数、Connector 或增量维护资格 |
| `SqlGrouping` | 原 SQL 分组身份；浮点合并 `±0` 和所有 NaN，根 NULL 按显式政策处理 | 原路由 hash 与既有调用保持原合同 |
| `ResultContent` | 浮点区分 `±0`，所有符号与 payload 的 NaN 为一类；根 NULL 相等 | 当前 NovaRocks 准入支持面内的显式内容身份，不是任意 SQL 观察上下文的等价证明 |
| canonical walker / key store | 用统一值作者产生身份字节，另保留索引实际首次接纳的代表 | 原始代表的 NaN 位不参与相等判定；不承诺全局源序 |
| `content_fingerprint` | 对完整内容元组计算固定种子的 XXH3-128 证据 | 不替代准确比较、来源覆盖或业务裁决 |

内容类还保留字符串及 JSON 的原文差异、LIST 项顺序、STRUCT 字段顺序和原 MAP 存储顺序；NULL 父值下的子值和 NULL 槽残留字节不参与身份。同一逻辑域的合法字典、slice 和 byte 布局按实际逻辑值准备，不以物理字典码或 offset 为身份。现有准入支持根 byte dictionary，以及经原 Hydrate 的嵌套 byte dictionary；数字根 Dictionary 不因此获得准入。原 complex canonical codec 的类型能力也不因新增顶层 fixed 指纹 helper 扩大。

**外部对照有适用范围。** Trino 484 的固定来源是 `e2d50bb733e193607c44be84ad80ce2430ba1d01`。其 [REAL IEEE 函数](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-main/src/main/java/io/trino/operator/scalar/VarbinaryFunctions.java#L252)直接转换 native 整数位与字节，能保留 NaN payload；`to_ieee754_64` 才显式调用 `doubleToLongBits`。[RealType](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-spi/src/main/java/io/trino/spi/type/RealType.java#L84) 的 `writeLong` 保留原 int 位，`writeFloat` 才规范化 NaN，不能据后者声称所有 REAL 写入都规范化。REAL 和 DOUBLE 的 compare 都先对数值 `left == right` 返回零，再调用 Java compare，因此 `±0` 在该比较中同序；见 [REAL compare](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-spi/src/main/java/io/trino/spi/type/RealType.java#L223) 与 [DOUBLE compare](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-spi/src/main/java/io/trino/spi/type/DoubleType.java#L266)。这些实现不能推出本政策与 Trino 的通用可见等价或比较全序相同。

Rust 的 [NaN 位模式规定](https://github.com/rust-lang/rust/blob/1.98.1/library/core/src/primitive_docs.rs)区分算术与按位操作：算术产生的 NaN 符号及 payload 可以不确定；`to_bits` / `from_bits` 等按位操作保留相应位。这不意味着已经产生的值在普通复制或位转换时会任意变化，也不能用算术规则抹去代表保留义务。[`f64::NAN` 的定义](https://github.com/rust-lang/rust/blob/1.98.1/library/core/src/num/f64.rs)只承诺 quiet NaN，符号和 payload 不是固定编码依据；因此内容编码使用显式位字面量。

## 考虑过的选项

1. **所有用途复用 SQL 分组身份。设计否决。** 可以少一种政策，但会合并内容合同要求保留的 `-0.0` 与 `+0.0`。业务调用者再补比较器会形成第二个身份作者，破坏 hash、比较与存储的一致性。
2. **把所有浮点原位差异都作为内容身份。当前不选；扩展观察能力时待评估。** 这能区分 NaN 符号与 payload，但与当前明确选择的 NaN 一类合同不同；算术重算也不保证产生相同 payload。若准入原位观察能力，必须重新判断需要的身份，而不是依赖现政策继续适用。
3. **逐列冻结显式内容政策，准确索引与诊断指纹分工。选定。** 相等、hash 和编码由同一列合同选择，代表保存与身份规范化分开；保留当前内容类，也使未来扩展必须显式重审支持面。
4. **用指纹直接判相等或删除核验是否成功。设计否决。** 有限位 hash 存在碰撞；相同摘要不能证明内容键或来源集合相同。业务资格和完整性仍由原消费者拥有。

## 裁决

1. **政策随列冻结。** `KeySpec::with_equalities` 与完整类型共同冻结政策；`ResultContent` 隐含根 NULL 相等，不可再组合 `Unmatchable`。固定 helper 或复杂 walker 的 const 参数在列级选定，逐行热循环不重新匹配政策。旧 `KeySpec::new` 保持 SQL 分组政策。
2. **身份与代表分开。** 内容身份对 f64/f32 的 NaN 分别使用 `0x7ff8000000000000` / `0x7fc00000`，其它位不变。输入比较、本地 hash、嵌套 walker 和指纹用相同内容位。固定存储保留原值；canonical 存储保留代表原位，比较 sink 对双方浮点叶位规范化，不能因原位存储绕过身份政策。
3. **首次接纳有准确范围。** 类代表是该索引实际首次接纳的输入值；已有类不因后续相等输入更换代表。compact 与 typed export 保留其原位。bulk partial 可以按就绪顺序合并，因此不承诺全局 capture、page、task 或 SQL 源序中的首个 NaN。
4. **载体归一化归原作者。** byte 使用原 DecodedBytes，复杂值使用原 Hydrate/projection 和准确 row mapping；identity、representative 与 fingerprint 复用一个 walker。物理 byte marker 的指纹归一化不改变原存储 decoder 或分布式路由格式，不扩大支持类型或猜测逻辑域。
5. **确定指纹只作证据。** `PreparedKeyBatch::content_fingerprint` 对完整内容元组按顺序编码，使用固定 seed `0x4e52434f4e54454e` 的 XXH3-128，NaN 已规范化，合法载体不影响编码。同一冻结列合同与 exact material island 内可比较证据；不承诺跨任意版本的持久 hash 协议，也不承诺跨类型无碰撞。准确相等始终由 `KeyIndex` 比较决定；实例随机 local hash 不能充作该证据。
6. **适用性由准入拥有。** 当前内容政策本身不证明某条查询可安全增量维护。增量内容额度与删除核验消费者必须按其实际表达式、函数、类型与可观察能力完成资格判断，保留准确输入耗尽、来源覆盖和失败语义；`KeySpec` 或指纹不能代替该判断。[ADR-0222](ADR-0222-typed-task-summaries-and-per-kind-completion.md) 的 summary 只传递核验事实，不决定业务结果。

## 接受的妥协（诚实记录）

- 当前身份主动合并 NaN 的符号与 payload，同时保留一个实际代表的原位。它没有承诺任意重算产生相同 NaN 位，也不能覆盖能观察这些位的所有未来函数；准入 owner 必须守住这一边界。
- `±0` 保持不同内容类。若计算路径产生不同符号的零，精确内容匹配可能不足；消费者须沿自己的明确失败或退役规则处理，不能通过折叠零符号制造匹配成功。
- 并行构建可能选中不同 NaN 原位代表；合同保证该次实际选中值的保留，不付出全局排序与替换账本的成本来提供未声明的源序保证。
- 固定指纹提供有界证据，碰撞可能降低诊断区分度；不提供键列表，也不承担正确性判据。固定向量与局部测试只验证相应编码和索引路径，不代替原生分布式或消费者资格验证。

## 何时重新评估

- 准备新增 `to_ieee754_32/64`、raw IEEE cast，或任何可观察 NaN 符号、payload 的函数、协议或存储能力时，先重裁内容身份与准入；此时重新评估原位身份选项，不能仅在函数处强制规范化来维持旧结论。
- 新的比较、聚合、重写或 Connector 能力改变增量路径可观察的结果差异或重算行为时，由资格 owner 重新证明适用范围；不能把 `KeyEquality::ResultContent` 的存在当作通过证明。
- 需要全局源序代表、跨实例 ID，或跨版本持久指纹协议时，单独裁决顺序、身份与编码演进；不从当前私有代表和 exact-island 证据推导这些能力。
- 扩大 Dictionary 准入、复杂 codec 类型域或 byte 载体后，重新核对逻辑域、隐藏 NULL、row mapping 与指纹不变性，再扩展支持声明。
