---
id: ADR-0164
title: "Iceberg delete membership is frozen in FE closures and executed by task-local unions"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
date: 2026-09-29
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1127"
  - "discussion: 2026-09-29 Iceberg delete closure authority, equality sharing and endpoint visibility"
code-anchors:
  - "novarocks/connector/iceberg/src/read_snapshot.rs (build_read_snapshot_in_domain)"
  - "novarocks/connector/iceberg/src/delete_semantics/candidate_index.rs (DeleteCandidateIndex)"
  - "novarocks/connector/iceberg/src/delete_semantics/set_view.rs (DeleteSet, LoadView)"
  - "novarocks/connector/iceberg/src/typed_read/delete_manager.rs (DeleteManager, EqualityUnion)"
  - "novarocks/connector/iceberg/src/typed_read/change_window_page_source.rs (ChangeWindowRead)"
---

## 问题

Iceberg 的删除成员、内容地址、共享执行索引和双快照可见性应如何分配权威与生命周期，才能同时保持正确性、可扩展性和可验证的异步退出？

## 背景与执行事实

Provider 拥有表格式事实；FE 的 provider planning 从冻结 metadata 观察 manifest，BE 的 provider reader 执行已选择的成员。Native carrier 与通用 execution 不解释删除适用规则。ADR-0015 的读取正确性归属、ADR-0137 的私有 wire、ADR-0159 的 driver poll 合同仍成立。

| 实体 | 身份与职责 |
|---|---|
| `ReadDomain` | 一次 read observation 的准确 endpoint、schema、spec 与分区存储类型；关系端独立冻结期望域，不能从收到的 split 自造期望值 |
| `DeleteFact` | 内容地址与应用解释分开；Puffin 地址含 path、offset、length，应用解释保留 sequence、scope、count、字段 ID/类型 |
| `DeleteCandidateIndex` | FE 按 global/typed partition、路径、字段组和序号召回；检查 DV 唯一性及替代关系 |
| `DeleteSet` / `LoadView` | 逻辑集合由共享冻结桶及序号后缀表示；统计只影响加载视图，不改变逻辑删除身份 |
| `DeleteManager` | 一个 task/scan 的解码、应用完成状态与 equality union owner；不在 task 或 BE 间共享 |
| `EqualityUnion` | 准确 domain × scope × 字段/类型组内的 `key → maxSeq`；不同 endpoint 不混用 |

Equality 用 `deleteSeq > dataSeq`，position/DV 用 `>=`。适用 DV 替代旧 position 来源；准确目标但序号过旧的 DV 显式失败。路径相同不代表 blob 或应用身份相同。历史 partition 存储类型独立于查询输出 schema，不能用 current schema 猜测已删除字段的存储解释。

同桶的不同文件主要使用不同序号后缀。逐后缀重建聚合会将 U 个后缀与 M 个成员的成本相乘。共享 union 可以累加其他 split 的合法成员，但当前 split 必须等自身所需成员完整并入。统计排除能够保持这一结论的前提是统计正确且 provider 解码/比较正确；未知或不足的统计不能证明排除。

## 考虑过的选项

1. **BE 再选择成员或逐行解释 metadata（设计否决）。** 局部看似灵活，却制造第二成员权威，破坏冻结快照和 FE/BE 配套验证。BE 验证结构、域与合法解释，执行行级 sequence 判定，不重新读取 metadata 规划。
2. **每个精确成员子集建立独立聚合（成本否决）。** 隔离直观，但流式 upsert 的大量后缀重复构建同一 key 集合；不采用它作为普通 equality 执行结构。未来若支持不满足共享证明的语义，必须重新评估。
3. **完整共享桶与只增 union（采用）。** FE 以后缀视图复用事实，BE 在同域、同 scope、同类型组内维护 maxSeq，独立记录每个应用的完成事实。
4. **跨 task/process cache（待评估）。** 能减少 global 删除重复加载，却要求明确付费、租约、取消、eviction 和真实物理退出责任。本记录不把 provider map 提升为进程全局 cache。
5. **计划层分区化 anti-join 或 spill（待评估）。** 能处理超过内存预算的大集合；完整 scope、字段 ID/类型和序号事实必须保留。广播不是默认的大集合路线。
6. **position 全内容按路径建共享索引（待评估）。** 缺统计布局可能受益，但增加累计驻留。当前下推准确 `file_path` 与必要列，交由 FS 物理裁剪。

## 裁决

采用 FE 冻结闭包、BE 执行 task-local union，并固化以下规则：

1. **单一成员权威规则。** 所有 snapshot/change-window 消费者使用同一规范观察和候选索引；删除种类、scope、DV 替代和内容地址只由 provider 决定。异常输入默认跟随固定 Iceberg Java 实测；若损害合规表正确性才裁定例外。
2. **完整身份规则。** 域、typed scope、字段 ID/类型及完整 blob 地址不可用 path-only key 替代。应用 identity 与兼容物理解码 identity 分开，解码复用不等于应用语义相同。
3. **逻辑与加载分离规则。** 统计正确是裁剪的信任前提，不为错误 writer 统计承诺确定结果；合法截断、null/NaN、类型演进和 provider 解码仍须验证。统计变化不能伪造 change event 或删除撤回。
4. **后缀共享规则。** 无统计路径的 FE split 持有桶/后缀引用，不为每个 D 复制成员数组。迭代、统计例外和序列化展开成本另外计量。
5. **完成屏障规则。** 完整物理解码校验成功后才并入 union；原子 max 更新与应用 Ready 幂等。page stream 等待返回 Pending 并由事件唤醒，不同步占住 driver。取消一个等待者不取消仍有 demand 的共享加载；最后 demand 退出沿现有 FS owner 到物理 settlement。
6. **完整端点规则。** From/To 分别拥有完整可见性与 union；delete-side 是 lower-visible 减 upper-visible，保留行重数。只有逻辑集合及解释完全一致才能跳过差分扫描。真实行重新出现或未支持的解释明确失败，不输出局部事件。
7. **诚实成本规则。** 物理 position I/O 包括 footer、投影列块、range 合并放大与缺统计退化。union key 容量、Ready artifact cache、bitmap heap、暂存和进程 RSS 分开记录；owner 释放与 RSS 下降不等价。
8. **配套兼容规则。** 同一 Iceberg provider 的 read/write declarations 与 recipes 共用一个私有 revision；进入 NativeCompatibilityId 并在 placement/ingress 隔离不同岛，旧私有载荷另行严格拒绝。

## 接受的妥协（诚实记录）

FE 当前仍 eager 观察快照，双端点 metadata 同时物化；私有 wire 仍每 split 内联展开成员与域。后缀共享解决 FE 列表重复，但不证明 wire、BE 描述或首 split 常驻有界。懒发现和任务级字典需要独立生命周期设计。

每 task/scan manager 保留自己的 union/global 桶，K 个 manager 会重复 K 份工作集。From/To union 独立，工作集相近时近似翻倍。成功解码的每 artifact key vector/position bitmap 保留到 manager 最后 owner 释放；不同 artifact 的相同 key 仍可能各占一份 cache，即使 union cardinality 不增长。高压缩 DV 也可显著扩大 bitmap heap。当前不提供全局内存上限、spill 或 eviction；这些必须由资源 owner 治理，而非用物理文件大小或合作 yield chunk 冒充总内存预算。

逐文件统计裁剪降低无用 I/O，却依赖可信统计。错误统计可使共享 union 结果随其他合法加载变化；没有为这类输入额外容错，因为全引擎文件裁剪已依赖同一前提。provider 自身解码错误仍是必须修复的正确性问题。

## 何时重新评估

- live placement 的多个 manager 重复 global 工作集成为主要成本，或 Ready decode/双端点驻留造成容量压力：由 MEM/Worker 裁决增长收费、lease、eviction 与共享归属。
- 删除集合超过可授予内存，或 key 宽度/字段组数量显著增加：评估分区化 anti-join 与 spill，保留完整 domain/scope/sequence 语义。
- 缺统计或宽 row group 的多目标 position 负载长期重复读大量相同内容：比较共享路径索引的 I/O 收益和实际 heap 成本。
- 首 split 延迟或内联 wire/域描述放大主导：以拉取式发现及 task-level 桶表/后缀引用收敛，不退回每精确后缀聚合。
- 引入不满足同桶共享证明的新删除语义或更强不可信统计保证：重评估成员隔离与精确子集策略，不能复用现有 union 的证明。
- 支持新的 schema 单位转换、数据重出现或 change policy：先裁定完整端点行语义和类型绑定，再扩执行能力。
