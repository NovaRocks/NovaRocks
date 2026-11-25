---
id: ADR-0263
title: "Trino scalar values and MV publication eligibility"
domain: [sql-compiler, map-semantics]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0267
    scope: "MV publication eligibility and the definition format transition described under version evidence."
    link: "ADR-0267-mv-publishes-bound-source-and-storage-facts.md"
date: 2026-11-24
provenance:
  - "TRC-6d3 approved Obsidian snapshot 27ecea4c80eebe79fb4f4a5fe1355c934b7d3269"
code-anchors:
  - "novarocks/functions/src/persistent_scalar.rs"
  - "novarocks/sql/src/planning/mv_persistence.rs"
  - "novarocks/mv-application/src/persistence/codec/model.rs"
---

## 定型与求值

Functions 为公开标量函数签发实际重载、实参域和行为。比较、转换与原行错误保留冻结的实参类型。NULLIF 只对左值成功且非空的行进入比较区域。比较使用 Trino 公共类型和已安装等值作者。TRUE 输出左类型的 NULL，FALSE 与 UNKNOWN 保留求值一次得到的原左值。TYPEOF 从实参的完整声明生成类型名，同时消费原实参求值。标量极值复用方向性比较，并保留首个并列值。

UUID 字面量和 uuid() 生成名义 UUID。固定 16 字节沿原结果和交换契约传输，UUID 文本呈现与显式转换共用规范格式化。转换接受准确的类型及字节宽度。查询定义中的 CURRENT_USER、CURRENT_CATALOG、CURRENT_SCHEMA 读取语句准入冻结的调用方事实。普通视图按存档上下文查找表。MV 定义准入对展开后的会话依赖给出拒绝。

## 发布资格

MV 定义在原 D 保留展开后实际选中的受影响标量函数身份、重载身份和语义代次。有记录时这些字段进入原 computation identity。旧格式文档保留原计算身份，并能以空记录集合解码。验证用的是实际 P 引用的 D/L。增量前驱和查询改写分别取得同一已发布定义的当前绑定，再与原标量记录比较，同时沿既有边界校验聚合语义。

旧发布缺少当前需要的标量记录时，改写跳过该候选，刷新报告受影响函数需要重建。创建新的定义并完成实际发布后，新记录随 D/L/P 生效。原数据和发布状态继续通过原管理、可见性与验值入口解释。可根据诊断记录中的函数重载、发布标识及定义指纹定位实际需要重建的对象。

## 版本依据

物理包 revision 42 冻结 NULLIF 的控制及相应等值身份。Native UUID 呈现由结果 descriptor 声明。Descriptor、计划修订及已封存的函数目录共同构成 Native 兼容岛。生产运行 epoch 为 18。MV 定义持久格式 1 增加可缺省的标量语义记录，空集合维持原版本的编码和身份。旧解码器会拒绝含新记录的定义。回退时，受影响对象需要恢复到旧端可读取且已验值的发布，或由旧端按原规则重建。

相关合同见 [ADR-0252](ADR-0252-trino-casts-and-row-error-scopes.md)、[ADR-0258](ADR-0258-prepared-sql-request-snapshots-and-semantic-authors.md) 和 [ADR-0259](ADR-0259-trino-aggregate-state-and-published-semantics.md)。
