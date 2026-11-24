---
id: ADR-0265
title: "ROW declarations and canonical MAP keys"
domain: [sql-language, sql-compiler, map-semantics]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - "ADR-0183: MAP key relation and validation now use the complete canonical key domain"
date: 2026-11-24
provenance:
  - "discussion: 2026-11-24 ROW declarations, MAP identity, and ordered NULL argument demand"
code-anchors:
  - "novarocks/functions/src/builtin/row_owner.rs (ROW result binding)"
  - "novarocks/value-arrays/src/hash_eq/key.rs (PreparedKeyOperations)"
  - "novarocks/execution/src/exec/hash_table/key_builder/map_identity.rs (MAP canonical encoding)"
  - "novarocks/type-contract/src/partition.rs (PartitionHashAlgorithm::NativeExchangeV2)"
  - "novarocks/expression/src/evaluator/guarded/ordered_null.rs (ordered argument evaluation)"
---


# ADR-0265 ROW 静态声明与 MAP 规范键

## 问题

ROW 的字段名称如何从定型保留到客户端和持久化？同一个 MAP 如何沿查找、聚合、分组和分区维持相容的键关系，并保留原值和错误顺序？

## 背景与执行事实

| 边界 | 原作者与交接 |
| --- | --- |
| ROW | SQL 和 Expression 完成字段定型。Functions 的构造重载接收完整类型约束。Trino HTTP 渲染原声明，Iceberg 对持久 schema 单独准入。 |
| 键关系 | Type Contract 定义完整类型上的关系。Value Arrays 观察受检的 Arrow 行。聚合按同一关系读取已保管的值。 |
| 执行与分区 | K 用规范键字节做分组和集合判断。Exchange 使用冻结的哈希算法。每个组的输出沿首个实际代表读取原条目位置。 |
| 实参需求 | 冻结重载选择实参控制，Expression 在选中行上推进需求。到达叶内核时，调用具有已定型的非空实参。 |

## 考虑过的选项

- **按原 MAP 条目顺序编码身份，设计否决。** 原位置适用于呈现。以它作关系判等会让相同键值集合因条目位置落入不同分组。
- **在总身份中用普通等值寻址内层键，设计否决。** 普通等值可能返回 FALSE 或 UNKNOWN，无法给含 NaN 或递归 NULL 的合法 MAP 保证总等价与哈希相容。
- **每组交换律哈希并在实际碰撞时比较 MAP，成本否决。** 这可回避每次哈希前的排序。K 的字节索引和 Exchange 目前共用规范编码，采用独立比较器需要替换键存储与碰撞路径。

## 裁决

1. ROW 构造按原字段顺序评价值。字段名及匿名性经完整结果类型约束进入一个 ROW 构造重载。重新定型保留原名字和字段位置，并以实际子值更新类型和空性。字段访问由 SQL 冻结唯一 ordinal，BE 只消费已检查的 ordinal。Trino HTTP 的匿名字段没有显示名，签名保留位置。Iceberg 创建持久化 schema 时拒绝匿名或重复的 ROW 字段。
2. Type Contract 冻结完整类型的键关系，Value Arrays 在实际选行域执行。普通等值分别保留 TRUE、FALSE、UNKNOWN 和实际内层键寻址不确定。MAP 查找优先取确定命中，随后检查实际比较的不确定状态，最后按缺键处理。值确定性只作省下扫描的证明。普通 MAP 等值优先取确定不等。外层归约完成后再投影键不确定的 NOT_SUPPORTED。
3. MAP 的规范 IDENTICAL 递归比较键和值。所有 NaN 共享规范浮点身份，正负零共享规范浮点身份。构造、转换判重、MAP_AGG 和已获准的执行键消费这一关系。持久结果内容沿其原合同保留来源与位置。
4. K 为获准的 MAP 分组、关系去重和集合运算编码可达的完整键。MAP 的规范键字节决定条目排列，值字节跟随对应键。对有效且键唯一的 MAP，相同的规范字节能够还原同一组规范键值配对。真实输出仍读取原代表。含 MAP 的相应网络分区及本地 driver 分流冻结相容合同，FE/BE 用显式新算法身份共同切换。旧算法及平坦 writer、bucket、RF 合同继续各自解释其版本化材料。
5. 普通复合谓词沿已安装的残余谓词、nested-loop 或关系 LeftMark。哈希连接提键和 NAA 的原准入继续保留。复合哈希提键须证明实际谓词关系、空性、错误效应和同位条件。
6. MAP 转换沿原通用配方，按键、值、目标键判重的顺序完成每一有效行。TRY_CAST 将该行自身可捕获的转换错误投影成整行 NULL。有序 NULL 控制保留逐行实际的原调用需求。已选择该控制的精确重载从左到右求值，成功 NULL 结束该行，先发生的错误按原通道传播。余下非 NULL 参数交给原叶内核。正则的模式编译时机按其原合同。

## 接受的妥协

规范 MAP 编码需要条目键字节与排序空间，大 MAP 的 CPU 成本随条目规模上升。分区 V2 复用同一编码，原生混合版本由现有兼容岛原则拒绝。复合连接提键继续依赖相应谓词关系与准入合同。

## 何时重新评估

宽 MAP 的真实查询出现显著排序或临时空间开销时，用组规模、键字节量和碰撞分布评估交换律哈希及受检比较路径。采用新路径时需要重证分区相容与原代表保管。

## 兼容与诊断

新 Native 包合同封存准确的 ROW 声明、分区算法和有序控制，由原 exact-island 准入核对。检查工作计划的实际分区身份及 Task 绑定时，可核对 Type Contract 的分区枚举、LocalCompiler 的分流合同和 Server 的 build-info。旧 V1 的 hash 向量继续按旧身份复验。

针对查找，使用包含 NULL 但实际配对确定不等的输入诊断缺键，使用实际不确定配对并且无命中的输入诊断 NOT_SUPPORTED。对 MAP_AGG、GROUP BY、SELECT DISTINCT 和比较集合运算，使用反序条目但相同键值的实际列输入核对键域。关注多 driver 的聚合分流和跨 BE 的真实分区证据。Protocol 的原条目代表经原始 HTTP `typeSignature` 和值页核对。

Trino 484 以类型的 IDENTICAL 和 hash 分组。它的 MAP IDENTICAL 通过普通等值寻址内层键，导致含 NaN 键的 MAP 可以失去自反性。NovaRocks 采用递归总等价，并记录探测布局、客户端条目位置及宽 ROW 确定性的固定版本差异。 [Trino 分组](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-main/src/main/java/io/trino/operator/FlatHashStrategyCompiler.java#L148-L156) · [Trino MAP 关系](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-spi/src/main/java/io/trino/spi/type/MapType.java#L229-L250)

同签名结果语义变化写入版本说明。运维按说明全量重算受影响的 MV。实际输出声明和聚合状态格式各由原绑定及状态合同处理。B7a 保留存量结果语义标记的原值，由对应持久化迁移退出。

## 相关合同

- [ADR-0255](ADR-0255-collection-calls-and-relational-sequence.md) 的集合调用行为和普通等值归约。
- [ADR-0183](ADR-0183-map-keys-are-non-null-and-unique.md) 的 MAP 构造键约束。
- [ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md) 的 CAST 语境与行错误。
- [ADR-0257](ADR-0257-ordered-regexp-evaluation-and-resource-ownership.md) 的正则专用有序编译。
- [ADR-0223](ADR-0223-result-content-identity-and-deterministic-fingerprints.md) 的独立结果内容身份。
