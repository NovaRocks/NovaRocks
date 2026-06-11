# IVM-A9 · Iceberg target 行身份与增量 apply 协议

## 状态

- 优先级：⭐⭐⭐
- 当前范围：单 base table、projection/filter MV、base table 必须是 Iceberg v3 且启用 row-lineage
- 非目标范围：join、aggregate、多 base table、非 row-lineage Iceberg、显式 MV `PRIMARY KEY` 语义
- 依赖：IVM-A4 change-op exec plan、IVM-A8 Iceberg MV restore/registration、IVM-A10 Iceberg target commit framework
- 关联：IVM-A11 MV schema field-id contract

## 问题

当前 Iceberg-backed MV 的 incremental refresh 只处理 append-only delta。`plan_changes`
发现 position delete、equality delete 或 deleted data files 时，会回退到
`rebuild_iceberg_mv`。要支持真正的 IVM，必须让 target 能根据 base change 精确
定位并 apply 到已有 MV target 行。

证据：

- `src/engine/mv/iceberg_refresh.rs:132-135`：Iceberg MV target 已创建为
  `format-version=3` 且 `write.row-lineage=true`。
- `src/engine/mv/iceberg_refresh.rs:1668-1775`：`incremental_refresh_iceberg_mv`
  当前遇到 delete-bearing change batch 仍然 fallback rebuild。
- `src/connector/iceberg/commit/row_delta_dv.rs:18-43`：v3 row-lineage DELETE 的
  Puffin DV commit action 已存在。
- `src/connector/iceberg/data_writer.rs:255-309`：写数据文件时已经能写入 Iceberg
  reserved `_row_id` / `_last_updated_sequence_number` row-lineage columns。

## 当前阶段约束

1. base table 必须满足：
   - Iceberg format-version = 3；
   - `write.row-lineage=true`；
   - 能在 SELECT / delta scan 中读取 `_row_id`。
2. MV 定义必须满足：
   - exactly one base table；
   - projection/filter shape；
   - 输出行与 base row 是 row-preserving 关系；
   - 不支持 join、aggregate、union、distinct、window、non-row-preserving operator。
3. 如果任一条件不满足，CREATE 或 REFRESH 必须 fail fast，不 fallback full refresh 来伪装 incremental。

## 行身份模型

当前阶段采用最小且正确的模型：

```text
TargetApplyKey = base._row_id
```

这里的 `base._row_id` 是 base Iceberg v3 row-lineage id，用来定位这个 base row
在 MV target 中对应的投影行。它应写入 MV target 的隐藏 apply-key column，不暴露给
用户查询结果。

几个边界要明确：

- `base table uuid` 是一致性 guard，不是 `TargetApplyKey` 的组成部分。
  如果 refresh 时发现 base table uuid 与 MV 上次记录的不一致，说明依赖表已经换掉，
  应直接 fail fast，要求用户显式 rebuild 或重新绑定 MV；自动 incremental refresh
  不能自行 fallback。
- `relation_instance_id` 当前阶段没有意义。单 base projection/filter 不存在同一
  base row 在不同输入角色中产生多条语义不同 target row 的问题。
- target Iceberg 表自己的 reserved `_row_id` 是 target 物理行身份，用于 Iceberg
  row-level DELETE / Puffin DV。它不等于 base `_row_id`。
- 因此需要区分两个身份：
  - hidden apply key：base `_row_id`，用于从 base change 找 target 行；
  - target row-lineage：target Iceberg writer 分配的 `_row_id`，用于写 DV / row-level mutation。

## Target Schema

Iceberg MV target 需要新增一个内部隐藏列，用来存储 base `_row_id`：

```text
__nova_base_row_id BIGINT NOT NULL HIDDEN
```

约束：

- 用户 `SELECT * FROM mv` 和 MySQL result metadata 不显示该列。
- 该列不是 Iceberg reserved `_row_id`。它是普通 Iceberg data column，只是 NovaRocks
  catalog / SQL 层隐藏。
- field id 必须稳定，并纳入 IVM-A11 的 schema
  contract，避免 schema evolution 后 apply key 丢失或错绑。
- target table 仍必须保持 Iceberg v3 row-lineage，以便删除 target 物理行时写合法
  Puffin DV。

## Refresh Guard

每次 incremental refresh 前都要校验 MV 记录的 base invariants：

- base table FQN 与当前解析结果一致；
- base table uuid 与上次 refresh 记录一致；
- base table 当前 metadata 仍是 format-version=3；
- `write.row-lineage=true`；
- MV target table 仍是 format-version=3 且 row-lineage enabled；
- MV target schema 仍包含隐藏 `__nova_base_row_id`，类型和 field id 与 MV metadata 一致。

这些 guard 失败时直接报错。尤其是 base table uuid 变化不能参与 hash/encode 生成新的
target id；那会让旧 MV 行残留，破坏 correctness。显式 full rebuild 可以作为恢复操作，
但不能作为 incremental refresh 的自动降级路径。

## Apply 协议

delta 输入需要携带：

```text
__change_op
base._row_id
visible projection columns
```

apply 规则：

- `+` 行：
  - 计算/携带 `__nova_base_row_id = base._row_id`；
  - 追加到 target；
  - target writer 正常分配 target reserved `_row_id`。
- `-` 行：
  - 用 `__nova_base_row_id = base._row_id` 在 target 当前快照中查找对应 target physical row；
  - 将 target physical row 转换为 `(referenced_data_file, position)`；
  - 走 v3 row-lineage `RowDeltaDvCommit` 写 Puffin DV。
- update：
  - 如果 base update 在 change stream 中表现为 `- old` + `+ new`，则先按旧
    `base._row_id` 删除旧 target row，再插入新 projection row。
  - 对 row-lineage 语义，update 后是否沿用同一个 base `_row_id` 取决于 Iceberg
    change planner 的输出；A9 不自行猜测。

## 组件边界

### 1. MV Definition / Schema Contract

负责在 CREATE MV 时固定协议，不参与 refresh 执行。

需要记录：

```text
base table fqn
base table uuid
base snapshot id
target table fqn
target schema apply-key field id
apply key column name = __nova_base_row_id
apply key source = base._row_id
```

### 2. Refresh Guard

负责在每次 refresh 开始前校验不变量并 fail fast。它只判断 incremental refresh
是否安全，不做修复、不 fallback、不 rebuild。

### 3. Delta Source

负责从 base table change 中产出 logical change rows：

```text
__change_op
base._row_id
visible projection input columns
```

Delta Source 不关心 target table 的文件、position、DV。

### 4. Projection Evaluator

负责把 base delta row 变成 MV target row：

```text
__change_op
__nova_base_row_id = base._row_id
visible MV columns
```

filter 语义必须基于 change row 本身判断：

- `+` row 不满足 filter：不写 target；
- `-` row 是否删除 MV target row：基于 old row 是否满足 filter；
- update 表现为 `- old` + `+ new` 时，自然覆盖进入 filter、离开 filter、值变化三类情况。

### 5. Target Row Locator

负责把 logical delete key 转成 Iceberg physical delete target。

输入：

```text
__nova_base_row_id values from - rows
```

输出：

```text
Vec<PositionDeleteGroup> // referenced_data_file + positions
```

它通过扫描 target 当前快照，查找 `__nova_base_row_id in (...)` 的 live rows，并返回
这些行所在 data file 和 row position。它不写 commit，只做定位。

正确性要求：

- 每个 delete key 必须最多匹配一条 live target row；
- 匹配 0 行说明 target 和 base change stream 不一致，必须报错；
- 匹配多行说明 target 已有重复 apply 结果，必须报错。

### 6. Iceberg Apply Sink

负责把 `+` 和 `-` 应用到 target。

- `+` rows：写 data files，保留 `__nova_base_row_id`，target writer 分配 target
  reserved `_row_id`。
- `-` rows：调用 Target Row Locator，再用 `RowDeltaDvCommit` 写 Puffin DV。
- mixed update：同一 refresh publish 语义里 delete old target rows + append new target rows。

Apply Sink 不重新解释 MV SQL；它只消费已经投影好的 change rows。

### 7. Commit / Metadata Finalize

负责把 target commit 和 MV repo metadata 推进到同一个 refresh 结果。

需要记录：

```text
new base snapshot id
new target snapshot id
base table uuid
refresh marker / staging branch
```

如果 commit 成功但 metadata finalize 失败，沿用 IVM-A10
的恢复 / marker 机制处理，A9 不重新设计事务恢复框架。

## Error Handling

A9 的原则是：只要无法证明 incremental apply 正确，就 fail fast，不自动 rebuild。

必须报错的情况：

1. **base 不满足 row-lineage contract**
   - base 不是 Iceberg v3；
   - `write.row-lineage` 不是 `true`；
   - 无法读取 `_row_id`。
2. **base table identity 变化**
   - 当前 base table uuid 与 MV metadata 记录不一致；
   - 不把 uuid 编进 key，也不 fallback。
3. **target apply-key schema 不匹配**
   - 缺少 `__nova_base_row_id`；
   - 类型不是 BIGINT；
   - field id 与 metadata 记录不一致。
4. **delete key 找不到 target row**
   - `-` change 携带的 `base._row_id` 在 target 当前快照匹配不到 live row；
   - 这不是 noop，说明 lineage、target snapshot 或前序 apply 已不一致。
5. **delete key 匹配多条 target row**
   - 同一个 `__nova_base_row_id` 匹配到多条 live target row；
   - 继续写 DV 可能删除过多行，必须停止。
6. **locator 无法正确解释 target 当前 delete state**
   - 如果 target 当前快照存在 equality deletes 或 legacy v2 position deletes，而 locator
     无法在 live-row 视图下正确排除它们，应提示先 compact。
7. **mixed update 不能原子 publish**
   - delete 和 append 必须在同一个 refresh publish 语义下提交；
   - 不能 delete 成功、append 失败后 finalize base snapshot。

建议错误信息保持 action-oriented，例如：

```text
iceberg MV incremental refresh requires base table to be Iceberg v3 with write.row-lineage=true
iceberg MV base table identity changed; incremental refresh is unsafe, rebuild or recreate the MV
iceberg MV target row not found for base row id ...
```

## Noop / Empty Delta

这些情况可以成功但不写新 target snapshot：

- `plan_changes` 发现 from == to；
- change batch 只有 metadata-only change，没有 inserts/deletes；
- delta 有 `+` 行但全部被 filter 掉，且没有 `-` 行；
- delete key set 为空。

但只要存在 `-` row，就必须走 locator 校验，不能因为最终 visible output 为空就跳过。

## 与 StarRocks 参考实现的关系

StarRocks 的 IVM 抽象也区分两种 row-id 来源：

- `AUTO_INCREMENT`：append-only source 下由 storage 分配 row id；
- `QUERY_COMPUTED`：query 自己计算 hidden `__ROW_ID__`，aggregate 当前用
  `encode(group_by_keys)`。

但 StarRocks 当前并没有完整实现 “multi-base row ids -> join target id”。它的 join
delta rule 处理的是 join 增量公式，不负责为 retractable join 结果合成稳定 target
row id。因此 NovaRocks A9 不应照搬 StarRocks 当前 append-only join 路径；应只借鉴
“target apply key 是 query-computed hidden key” 这个架构方向。

## 长期扩展方向

A9 当前阶段只实现：

```text
projection/filter: TargetApplyKey = base._row_id
```

未来扩展时再引入更通用的 key 生成器：

```text
aggregate: TargetApplyKey = encode(group_by_key_values)
join:      TargetApplyKey = encode(per-input source identities in plan roles)
```

这里的 `relation_instance_id` / input role 只属于未来 self-join、多 base join 场景。
它不应污染当前单 base projection/filter 的协议。

## 验收

### Unit Tests

- validate base contract：v2 / v3 without row-lineage / v3 with row-lineage。
- validate target schema contract：缺列、错类型、错 field id、正确 schema。
- projection evaluator：`+`、`-`、`- old + new`、filter enter/leave。
- target locator：
  - key -> one row 成功；
  - key -> zero row 报错；
  - key -> duplicate rows 报错；
  - existing delete files 不支持时 fail fast。

### SQL / Integration Tests

- 创建 Iceberg v3 row-lineage base，创建 projection MV。
- base insert 后 refresh：target 新增行，隐藏列不出现在 `SELECT *`。
- base delete 后 refresh：target 行消失，target snapshot 产生 Puffin DV。
- base update 后 refresh：旧 projection row 被删，新 projection row 插入。
- filter 场景：
  - old row 满足 filter，delete 后 MV 删除；
  - update 从满足变成不满足，MV 删除；
  - update 从不满足变成满足，MV 插入；
  - update 一直满足，MV 替换。
- base table uuid 变化后 refresh 报错。
- target hidden apply-key schema 被破坏后 refresh 报错。

### Compatibility / Regression

- `SELECT * FROM mv` 不显示 `__nova_base_row_id`。
- 默认不允许显式查询 `__nova_base_row_id`，除非未来引入 debug/session flag。
- append-only incremental refresh 继续走 fast path，但写入 target 时也填充
  `__nova_base_row_id`，为后续 delete 做准备。
- full rebuild 路径也必须填充 `__nova_base_row_id`，否则 rebuild 后下一次 delete
  refresh 找不到 target row。
- delete-bearing delta 不再统一 fallback rebuild，而是通过 hidden apply key 找到 target
  physical row 并写合法 Puffin DV。

## 不建议的绕过

- 不要把 base table uuid 编进当前阶段的 target apply key。uuid 变化是 guard failure，
  不是一个新的 key namespace。
- 不要依赖 MV DDL 的显式 `PRIMARY KEY` 作为当前 A9 的必要条件；当前阶段的行身份来自
  Iceberg v3 base `_row_id`。
- 不要把 target reserved `_row_id` 与 base `_row_id` 混为一谈。前者是 target 物理行
  identity，后者是 apply key。
- 不要把所有 delete-bearing delta fallback full refresh 当作长期方案；这会让 IVM 的
  correctness 和性能目标失效。
- 不要把 hidden identity columns 当普通用户列暴露。target apply 需要它们，但 SQL
  schema 不应被污染。
