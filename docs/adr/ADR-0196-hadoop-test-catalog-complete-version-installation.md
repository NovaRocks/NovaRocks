---
id: ADR-0196
title: "Hadoop test catalogs use complete non-replacing version installation and read-only discovery"
domain: [provider-spi, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0227
    scope: "Ruling 3 fixed 65,536-entry inventory limit; complete discovery, hint, installation and lifecycle rules remain active"
    link: "ADR-0227-metadata-listing-and-task-transport-use-real-capacity-boundaries.md"
partially-supersedes:
  - id: ADR-0077
    scope: "Complete local installation, physical version discovery and CREATE inventory prerequisite; conditional creation and exact positive attribution remain active"
    link: "ADR-0077-hadoop-catalog-create-uses-storage-conditional-publication.md"
  - id: ADR-0118
    scope: "Hadoop hint/v1 existence and canonical-v1 DROP commit point; provider-private ownership and operation admission remain active"
    link: "ADR-0118-iceberg-provider-private-catalog-owner.md"
  - id: ADR-0179
    scope: "Retained Hadoop update metadata-plus-hint acknowledgement and numbering protocol; frozen requests, journal and phase grants remain active"
    link: "ADR-0179-iceberg-publication-lifecycle-and-evidence.md"
date: 2026-10-23
provenance:
  - "discussion: 2026-10-23 approved Hadoop test catalog complete-installation and discovery design"
code-anchors:
  - "novarocks/fs/src/access.rs (FsAccessHandle::create_if_absent)"
  - "novarocks/connector/iceberg/src/hadoop_catalog/versions.rs (inventory, discover_current_version)"
  - "novarocks/connector/iceberg/src/hadoop_catalog/publication.rs (FrozenInstallation, HadoopHintFinalization)"
  - "novarocks/connector/iceberg/src/hadoop_catalog/drop_publication.rs (HadoopPreparedDrop)"
  - "novarocks/connector/iceberg/src/catalog_metadata_root.rs (is_catalog_metadata_root)"
---

## 问题

没有独立 catalog 服务的 Hadoop 测试仓库，如何在单写者功能边界内发现当前表、完整发布 metadata，并诚实区分确定冲突、已生效与结果未知？

## 背景与执行事实

| 实体 | 唯一职责 |
|---|---|
| Hadoop provider 私有 `versions` | 规范版本名、完整直属清单、只读发现和固定表根校验 |
| `FsAccessHandle::create_if_absent` | 在原授权路径安装完整字节，且不替换已经存在的目标 |
| `FrozenInstallation` | CREATE/UPDATE 共用安装与准确读回判定 |
| publication journal | 沿 ADR-0179 独占结论、派发屏障与阶段期限 |
| hint finalization | Applied 之后用独立期限更新发现提示 |
| 普通对象回收 | 保留既有对象、年龄及 generation 校验，另排除 catalog 发现根 |

表根由 warehouse 和 TableIdent 决定；metadata 固定在根内。规范版本为 `vN.metadata.json`、`vN.gz.metadata.json`、`vN.metadata.json.gz` 的直属普通文件，N 为无前导零的 1..=2147483647。目录、临时名和其他 basename 不构成版本。hint 是加速提示，不是表的存在性或第二个提交点。

## 考虑过的选项

- **仅相信 hint 或 canonical v1：设计否决。** 提示写入失败会遮蔽已经安装的版本，删掉 v1 也不能消除其余版本；不能用提示或 JSON 可读性替代物理版本清单。
- **普通覆盖写或逐步写目标：设计否决。** 它们分别允许复用已占用版本槽和暴露半个 metadata，不能建立完整安装事实。
- **每次读取都完整 LIST：成本否决。** 能得到完整清单，但常见读取无需枚举历史；有效 hint 前探即可。无 hint 时仍需完整清单。
- **fsync 链、持久 tombstone、多写者协调与 Unknown 后恢复写入：本裁决不采用。** 它们服务更强的持久恢复与生产并发需求，超出测试用途；不能把当前条件安装描述为已经提供这些保证。

## 裁决

1. **完整安装规则。** CREATE 冻结 v1，UPDATE 从已验证的准确基线 V 冻结 V+1、UUID、完整字节及摘要，派发时重核基线。两者调用同一安装入口。本地在同目录写全并关闭唯一临时文件，再 hard-link 到目标；对象存储沿原 operator 和 RetryLayer 做整块条件写。禁止覆盖已占用目标，不删除已安装目标来回滚。
2. **条件证据规则。** 原安装成功，或准确目标读回的 UUID 和摘要均匹配本次请求，才是 Applied。AlreadyExists 加完整他方 metadata：CREATE strict 为 AlreadyExists、IF NOT EXISTS 为 NoOp，UPDATE 为 Conflict；缺失、无效内容、读失败或证据期限耗尽为 Unknown。其他模糊错误只有准确正面证明可升级。保留原安装原因，读回失败另作阶段诊断；错误类别本身不决定提交结果。
3. **只读发现规则。** 有效 hint 从所指版本逐号前探；缺失或非法 hint 才完整列举取最大版本。有效 hint 指向缺失文件而清单非空时报告损坏；同号多种文件报损坏。读取和 Observe 不修复 hint。CREATE/DROP 用完整清单，列表可在找到一个物理版本后结束。完整枚举在过滤前累计每个真实直属条目，最多 65,536 个，不用部分结果回答不存在或选择最大值。
4. **建删表规则。** 空清单才允许 CREATE；先探测残留 hint，再完整复核清单，确有残留时才在安装前删除，空目录不派发多余 DELETE。DROP 冻结并复核完整版本集合，先删 hint，再按版本递增删除，最大版本组的最后一个锚点最后删除。最后删除确认后 Applied；其结果模糊时仅原派发 owner 可用期限内的完整空清单证明 Applied。中途失败说明低版本可能已删除；后续 Observe 见空不能升级 Unknown。损坏 hint/JSON 不阻止显式 DROP。PURGE 仍要求原 Applied 加准确 capture，不产生新的删除许可。
5. **阶段与退出规则。** 原准备控制约束新工作，派发/读回用 evidence grant，hint 用独立 finalization grant。本地同步安装与 unlink 留在保有至 join 的 catalog bridge；已开始的系统调用等到实际返回，期限只约束下一步。成功 ACK 不被随后取消遮蔽。固定根拒绝其他 CREATE location、SetLocation、write.metadata.path；HDFS 继续在配置阶段拒绝。
6. **发现根保护规则。** 规范化路径的直接父目录为 `metadata` 且 basename 是规范版本或 hint 时，普通对象回收拒绝删除。候选发现和最终删除都检查，已冻结候选也不能绕过；同形普通路径宁可多保留。该规则不替代 owner/generation、表 UUID、对象身份、年龄或 Puffin 校验。显式 Hadoop DROP 是版本链删除入口。

## 接受的妥协（诚实记录）

Hadoop 仅承担测试用途的单写者功能，不提供生产并发、跨引擎 CAS、fsync/断电持久性或安全恢复。Unknown 后继续写入，可能与迟到 PUT/DELETE 交错，出现 DROP 后重现或影响同名重建；条件槽在安装期间未被删除重用，是 AlreadyExists 加完整他方内容可判确定冲突的前提。

有效 hint 的逐号前探依赖正常版本链；历史损坏明确报错，不静默猜测。目录条目上限包含无关文件，超界拒绝，不自动清历史。本地 resolver 构建 operator 时可能创建缺失目录，当前不承诺目录树零变化。同步本地系统调用可能晚于 deadline 返回，以真实退出换取诚实结案。路径保护会保留同形普通对象，但不永久保护版本引用的全部旧数据。

## 何时重新评估

需要生产多写者、HDFS、断电恢复、Unknown 后可靠继续或跨进程接管时，重新设计 catalog authority；不能在当前适配器上补私有 marker 宣称获得标准并发语义。常见目录规模使完整 LIST 成为实际成本时，评估同等级正确性的索引。底层条件写能力或 Java Hadoop layout 变化时，重查完整安装与发现前提。未来 PURGE 消费端必须自行定义执行时安全，不把当前 capture 当作永久删除许可。
