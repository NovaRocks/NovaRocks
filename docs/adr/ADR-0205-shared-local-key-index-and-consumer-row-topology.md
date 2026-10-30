---
id: ADR-0205
title: "Instance-local key indexing is shared while row topology stays consumer-owned"
domain: [join-execution, pipeline-execution]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0004]
date: 2026-10-29
provenance:
  - "discussion: 2026-10-26 instance-local typed key equality, bulk construction and consumer ownership"
code-anchors:
  - "novarocks/execution/src/exec/hash_table/key_index/mod.rs (instance-local key index)"
  - "novarocks/execution/src/exec/hash_table/key_index/bulk_tasks.rs (BulkTaskGraph)"
  - "novarocks/execution/src/exec/operators/hashjoin/join_hash_table.rs (JoinHashTable)"
---

## 问题

join、GROUP BY、集合和唯一性检查都需要判定键相等，为什么共享一个实例内索引，而各自保留行拓扑、业务状态和完成协议？

## 背景与执行事实

键相等类与相等类上的业务关系不同。GROUP BY 为一个相等类保留一份聚合状态；join 可能为同一个类保留多条物理 build 行；只检查存在性的 semi/anti 不需要这些行。让共同索引承担某一消费者的完整形状，会把不需要的枚举、状态或行映射带给其他消费者；分别维护索引，又会重复字典、NULL、碰撞、所有权和扩容规则。

| 作者 | 拥有的事实 | 不授予的能力 |
|---|---|---|
| `KeySpec` / `KeyInput` | 完整 `FunctionValueType`、逐列 NULL 政策、准确输入 occurrence 与来源 | 从 Arrow carrier 猜逻辑类型、SQL 或 wire 准入 |
| `KeyIndex` / `IndexView` | 实例内键相等类、碰撞验证、稠密 `KeyId`、存在性与 typed 读取/导出 | join 输出行、聚合状态、集合业务标记、分布式身份 |
| `CaptureBuilder` / `CompleteInput` | 同一冻结输入域的原页、cached hashes、完整覆盖与统计 | EOS、driver 完成、跨实例合并或重新 hash 授权 |
| `BulkTaskGraph` | 可领取工作、准确一次完成、阶段屏障与只读 Ready | 线程池、driver observable、Task 生命周期或资源预算 |
| join 的 `JoinHashTable` | 相等类到真实物理 build 行的 CSR，供原 selection/gather 使用 | 第二个 key store、hash/equality 或整数直址实现 |
| aggregate / setop / assert owners | 原 group state、markers、applicability、Data 错误与诊断 | 通过公共索引改变业务顺序或编译准入 |

公共值准备、比较和构造继续遵循 ADR-0195。内部 byte view、nominal logical binding 和对外 signed layout 分开保留；复制前沿实际调用 owner 申请容量，来源存活不能代替资金授权，最后真实 alias 退出才结束对应责任。

## 考虑过的选项

1. **每个消费者自建索引，仅共享抽取/hash。设计否决。** 各自的行关系确实不同，但键相等、字典逻辑值和碰撞验证并不因此不同。让消费者重写这些算法与唯一值作者冲突，也使 direct/hash、资源错误和 typed 输出容易分叉。
2. **以一个完整聚合表同时承载 join 和集合。设计否决。** 聚合状态、join 多行关系和存在性有不同的业务信息需求；把其中一种表当作共同接口，会迫使其他用途保留或解释无用状态。共享层因此只拥有键相等类和按用途选择的输出。
3. **共同实例内索引，加消费者自己的行/状态关系。选定。** 普通 hash、direct integer 和 presence-only 共享同一键合同；消费者显式处理其物理行、业务状态与生命周期。代价是 join 仍需建立独立 CSR，不能把相等类数量误作物理行数。

## 裁决

1. **单一键作者。** 所有已迁移消费者使用 `KeyIndex` 或只读 `IndexView`。完整类型和两种 NULL 政策随实例冻结；hash 相同仅产生候选，准确比较才决定相等。索引 ID 是实例内部坐标，不序列化、不跨实例比较，不充当分布式 partition hash。
2. **用途决定保留的信息。** `KeyIds` 提供类 ID、代表和原 occurrence assignment；`PresenceOnly` 只提供准确存在性，不生成供消费者枚举的公共 ID/行映射。join 的重复、NULL 物理行及 match 标记归原 join owner，不能因键去重而丢行。aggregate state、集合 markers、唯一性检查的 applicability 和错误分类也保持原 owner。
3. **统计不决定结果。** 完整输入的 min/max、NULL 与 NDV 为策略/容量提供依据；HLL 只是 hint，低估或缺失不丢键、不改变 ID 或相等结果。direct 的 checked span 与实际用途成本比较属于私有优化选择；跨度不合适选择 hash，真实资源/控制失败保留原 cause，不以 fallback 掩盖。没有独立的行数、键数或碰撞次数配额；实际表示边界与原资源管理分别负责其拒绝。
4. **bulk 工作与完成协议分开。** capture 对原输入准备并保存 hashes；图按原页/range 领取工作，不先整表 flatten。route 的 partial 一就绪即可 merge，活跃 frontier 按 caller 的 W×L workspace 保留，不能等待所有 route 完成才释放。所有任务准确完成及阶段屏障关闭后才能发布 Ready；失去任务、失败或取消不能发布成功。caller 拥有调度、等待、唤醒、EOS 和原 driver 完成事实。
5. **ID 规则具名。** 增量插入保持既有实例 ID 稳定；bulk direct 按值槽序产生稠密 ID，NULL 类单列，hash bulk 的 ID 不承诺与增量或不同分区策略相同。调用者只能依赖同一 Ready 内的准确 assignment 与读接口，不能把 ID 当结果排序。
6. **表示与分布边界保留。** 普通 byte 走公共批值算法，复杂值保留原 canonical 作者；nominal logical binding 不伪签为私有 view carrier。typed export 使用调用者准确 signed layout 和原构造端口。`key_builder` 的 canonical framing、生产 FNV/CRC 及 NativeExchangeV1 向量独立保留，实例内 XXH3 policy 不改变它们。RF 继续使用其原 canonical/digest 与值复制作者。
7. **实际资源与失败保留。** 索引、代表页、assignment、CSR、返回映射及旧/新 buffer 并存沿真实 creator/control 接线。只读失败不改写索引成功状态；变更失败按原作者锁存。scope、ID 或估计容量不创造预算，退款按最后实际持有者退出；不另造队列、资源限额或无来源的精确字节账。

本条部分替代 ADR-0004 的“仅共享抽取/hash、不共享索引”裁决及旧索引 method 的实现前提；保留其 join 专有行拓扑、membership 不枚举、selection/gather 与语义正确性原则。现有未支持 SQL 形态仍准确拒绝；索引迁移、源码结构和局部验证不能证明后继 join 能力、全部原生验证或正式准入完成。

## 接受的妥协（诚实记录）

- join 在共同类索引之外保留 CSR 与物理 build 数据，承担相应 O(B+D) 关系存储；这是保存真实重复行的业务成本，不能用 D 代替 B 逃避。
- direct 与 hash 的内部 ID 排列可以不同，消费者必须通过 assignment 和 typed 读接口关联状态；不提供跨策略稳定 ID。
- bulk 保留输入页和局部统计/任务目录，并要求 caller 正确推进可领取工作；组件不代替 driver 的完成协议，也不提供跨进程索引服务。
- complex canonical 和 NativeExchangeV1 保持原表示合同，实例内索引不会自动扩展它们的类型能力；性能结论也不能由结构测试或普通 elapsed 输出推得。

## 何时重新评估

- 出现新的消费者，证明其“相等”不能由完整类型加当前逐列 NULL 政策表达时，先裁决业务相等语义，不能在调用处拼第二个比较器。
- 真实负载测量表明代表页、CSR 或 direct/hash 成本模型成为主要成本时，在同一所有权与准确结果合同内评估新布局；不因此发明任意合法输入上限。
- 若需要跨实例/跨进程 ID、spill、持久化或新的异步构建协议，应单独裁决身份、载体、资金和完成所有权，不能把当前私有坐标或纯工作图直接升级为协议。
