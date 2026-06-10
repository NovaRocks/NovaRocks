# IV3-6 · Variant 能力补全 — 设计文档

- 日期:2026-06-10
- 状态:已评审(分节确认),待实现计划
- Roadmap 条目:IV3-6-variant-capability-completion(⭐⭐,阶段 2)
- 范围决策:完整 IV3-6(查询侧优先),shredded 读+写都做,SQL 表面为 `variant_get` 函数,shredding 配置用显式表属性

## 1. 背景与现状

variant 当前状态:内部表示为 `LargeBinary`(序列化 `[size:u32 LE | metadata | value]`,`src/exec/variant.rs`);parquet 物理存储为 `Struct{metadata: Binary, value: Binary}`(vendored iceberg PATCH 6 映射);仅 INSERT 可写,行级变更 / 分区排序键 / 谓词下推全部缺失。

调研确认的关键事实(影响设计走向):

1. **standalone codegen 不直接产出 ExecPlan**:发射 Thrift 计划(`TPlan` + `TDescriptorTable`)后复用 FE-compat 共享 lowering(`src/lower/**`)。合成槽位需要 Thrift 载体,且必须对 FE-compat 模式零影响。
2. **Iceberg 扫描当前整体关闭 parquet 内部 min/max 裁剪**(`src/formats/parquet/mod.rs:370-373`,schema evolution 下按列名匹配不安全)。variant 裁剪必须新做 field-id 寻址的并行通道。
3. **shredded 读今天是数据损坏而非"不支持"**:非 iceberg 路径 `convert_variant_columns` 对 shredded 行静默置 null(`mod.rs:2060-2064`);iceberg 路径因 `is_variant_struct_data_type` 要求恰好 2 子列(`mod.rs:1934`)落入通用 cast 而报模糊错误。**OPTIMIZE/compaction 今天没有 variant 守卫**,compaction 一张被 Spark shred 过的表会静默丢数据。
4. **fixture 的 Spark 3.5.5 没有 VariantType**(jar 实证,0 个 variant 类);但 Iceberg 1.11 jar 含完整 shredding 机器,官方表属性 `write.parquet.shred-variants` 已存在。
5. **CoW UPDATE 有被守卫掩盖的必炸缝**:`build_cow_rewrite_batches` 混拼 iceberg-rust `ArrowReader` 的 Struct 编码与引擎内 `LargeBinary` 编码(`src/engine/mutation_flow.rs:1119-1124`)。
6. 行级 conjunct 在 scan 算子内永远全量重评估(`runner.rs:562-583`)——裁剪只是优化,正确性由重评估兜底。
7. 上游 `parquet-variant-compute` 58.2 提供 `shred_variant` / `unshred_variant` / `variant_get`(shredding-aware)/ `VariantArray` 内核,NovaRocks 当前零使用。

### Spike 实证(/tmp/variant-spike,parquet 58.2 + variant_experimental)

1. **写**:`ArrowWriter` 接受 3 子列 shredded variant struct(metadata/value/typed_value),根 group 正确发射 `LogicalType::Variant`,arrow 字段上的 `PARQUET:field_id` 落进 parquet schema。
2. **统计**:`v.typed_value.a.typed_value` 叶子有完整 min/max/null_count 列块统计 → row-group 裁剪物理可行。
3. **读**:`skip_arrow_metadata(true)`(NovaRocks reader 既有配置)推断出完整 shredded 结构,自动附 `arrow.parquet.variant` extension 标记 → per-file shredding 检测无需 vendor patch。
4. **语义一致性**:kernel 对同一数据 shredded / unshredded 两种输入提取结果逐行相等(含 missing 字段、类型不匹配 safe-cast→NULL)。
5. 新发现:`shred_variant` 产出 `BinaryView` 列,与 PATCH 6 schema 的 `Binary` 不同——写侧 schema 必须从实际数组类型派生。

## 2. 目标与非目标

**目标**
- variant 路径表达式查询优化:`variant_get`/`try_variant_get` 函数 + 优化器合成槽位重写 + per-file 自适应 shredded 取数 + row-group/page 级裁剪。
- shredded 读(Spark/iceberg-java 写的文件正确读取)与 shredded 写(显式表属性)。
- 行级变更解禁:DELETE / UPDATE / MERGE / INSERT OVERWRITE / ADD EQUALITY DELETE 对含 variant 列的表可用。
- 分区/排序语义澄清:variant 自身保持 spec 拒绝(提前到 DDL 期),variant 表用其他列分区/排序端到端可用。

**非目标**
- variant 作为分区 transform source / 排序键(Iceberg V3 spec 禁止,保持拒绝)。
- file 级(manifest)variant 路径裁剪:spec 对 variant 无 bounds,field-id→name 映射只到顶层列,物理不可能。明确放弃。
- 完整 variant SQL 函数生态(按业务另立子任务)。
- 旧 `get_variant_*` 家族的下推(语义宽松,见决策 C)。
- Spark 4.x fixture 升级与真跨引擎用例 → 子任务 IV3-6.1。
- per-operator runtime stats 观测面(roadmap 既有 follow-up)。

## 3. 查询侧架构

### 3.1 数据流

```text
SQL:  SELECT variant_get(v,'$.a','bigint') FROM t WHERE variant_get(v,'$.a','bigint') > 5

[Analyzer]   variant_get/try_variant_get 注册;第 3 参字面量驱动返回类型推断
     |       (resolve_expr 后置 pass,named_struct 先例)
[Optimizer]  新重写规则 VariantPathPushdown(SET disable_optimizer_rules 可关)
     |         - 匹配 FunctionCall{variant_get, [ColumnRef(变体列,直接来自 iceberg scan),
     |           路径字符串字面量, 类型字符串字面量]}
     |         - ColumnRefFactory 铸造新 ColumnId,在 ScanNode 追加合成输出列
     |         - ScanNode 新增 variant_columns: Vec<ScanVariantColumn{
     |             source_column, canonical_path, requested_type, strict, synthetic_name}>
     |         - 表达式替换为 ColumnRef 后,谓词成为普通 SlotRef 比较,
     |           经现有 PushDownPredicateScan 自然下推
     |         - 规则阶段位置:predicate pushdown 之后、TagRequiredColumns 之前
[Codegen]    visit_scan 为合成列分配 slot(命名空间 __nr_var_<n>);
     |       Thrift 载体 = THdfsScanNode 新增 optional 字段(决策 A)
[Lowering]   lower_hdfs_scan_node 把合成槽位分流出 data_columns,
     |       建 VariantPathSpec(仿 IcebergVirtualSpec);源变体列不在输出时走
     |       hidden slot(next_hidden_slot_id 先例)
[Reader]     open_next_reader 检测文件实际 schema(field-id 找 variant 根 +
             extension 标记 + typed_value 子树):
             - 路径+类型与 shredded 布局精确匹配 → kernel 零拷贝取数
               + row-group/page 裁剪(PR-5)
             - 否则 → 读 Struct{metadata,value},kernel 重建求值(无裁剪,语义相同)
```

### 3.2 关键决策

**决策 A:Thrift 载体用 `THdfsScanNode` 新 optional 字段,不用槽位名编码。**
路径含任意字符(`$.a["x.y"]`),名字编码需转义且长度无界;optional 尾部字段是仓库既有先例(`extended_columns`)。FE 永不发送该字段 → FE-compat 零行为变化。合成槽位名 `__nr_var_<n>` 仅作显示;载荷(源列 field-id、规范化路径、目标类型、严格性)走结构化字段。

**决策 B:槽位求值统一走 `parquet-variant-compute::variant_get` kernel,shredded/非 shredded 分支同一 kernel。**
语义一致性之根:同一查询里 A 文件 shredded、B 文件不是,两边行必须等值(spike 已实证)。`variant_get`/`try_variant_get` 表达式层实现(非下推场景)同样 kernel-backed(LargeBinary 按偏移重切回 metadata/value 零拷贝),保证规则开/关结果一致。

**决策 C:v1 重写规则只对 `variant_get`/`try_variant_get` 生效。**
旧 `get_variant_*` 家族有宽松强转(double 截断为 int 等),与 kernel 严格语义不同,下推会改结果。旧家族行为保持不变;文档引导用 `variant_get` 获得优化。

**决策 D:裁剪只做 row-group 级(footer 统计)+ page 级(沿用 `enable_parquet_reader_page_index` 既有开关);file 级明确不做。**
新裁剪通道按 field-id 找 variant 根、按路径找 typed_value 叶子寻址,绕开按列名的旧机制,与 iceberg 的"裁剪整体禁用"互不干扰、互不解锁。

**决策 E:可下推/可 shred-取数类型对 = 精确匹配白名单。**
v1:`boolean↔Boolean`、`bigint↔Int64`、`double↔Float64`、`string↔Utf8`、`date↔Date32`。统计比较在 shredded 物理类型上做。datetime 因时区语义分歧(内部 local-offset vs kernel UTC)v1 排除在 shred-取数外;含数组下标的路径(kernel `NotYetImplemented`)只走重建分支。两者均不报错、只是不加速。

**决策 F:正确性保底沿用现有契约**——裁剪只是优化,scan 算子对全部 conjunct 重评估;合成槽位的值本身由决策 B/E 保证精确。

### 3.3 各层触点(实现入口)

| 层 | 触点 |
|---|---|
| Analyzer | `src/sql/analyzer/functions.rs`(返回类型表)、`resolve_expr.rs`(字面量后置 pass)、`src/sql/functions/registry.rs` |
| Optimizer | 新规则于 `src/sql/optimizer/rewrite/rules/`;`registry.rs` 注册 stage;`plan.rs` ScanNode 加 `variant_columns`;memo 两拷贝点 `convert.rs` / `cascades_rules/implement.rs` 必须穿透 |
| Codegen | `fragment_builder.rs::visit_scan` 槽位分配(row-lineage 伪列先例);`expr_compiler.rs` 返回类型表;合成名不得进 `hive_column_names` |
| Lowering | `lower/node/hdfs_scan.rs` 槽位分类分流 + `min_max_conjuncts` 中合成槽位谓词转为 variant 裁剪谓词;`lower/expr/function_call.rs` 允许表 |
| Reader | `formats/parquet/mod.rs::open_next_reader` per-file 检测与取数;`row_group_selector.rs`/`page_selection.rs` variant 通道 |
| Exec | `exec/expr/function/variant/` 加 kernel-backed `variant_get`;`dispatch.rs` 双表同步 |

### 3.4 已知约束(显式声明)

- **RF 兼容(高危)**:`runtime_filters_to_min_max_predicates`(`parquet/mod.rs:127-180`)的"槽位数==列数"位置不变式会被合成槽位破坏并静默关闭全 scan 的 RF 裁剪。必须重建 slot→物理列映射;落在合成槽位上的 RF v1 降级为算子内过滤;普通列 RF 裁剪不受影响(专项回归)。
- **位置对齐**:需要行位置(`_pos`/`_row_id`/有 delete files)的文件保持裁剪关闭(沿用现有 min_max 清空规则,`hdfs_scan.rs:1073-1082`)。
- **EXPLAIN**:Verbose+ 在 SCAN 下新增 `variant columns: __nr_var_1 := variant_get(v, '$.a', 'bigint')` 行;扩展隐藏槽位名回溯(dict 先例,`explain.rs:867-885`)。
- **CBO**:合成列无统计,选择率走默认启发式;`variant_columns` 防丢字段(`already_pushed` 教训)。
- **路径规范化**:规则需解析路径为段并定义规范等价(同一路径两种写法共享一个槽位)。
- 第二 reader 站点 `runtime/lookup.rs:314` v1 不接合成槽位(lookup 路径不触发规则)。

## 4. `variant_get` / `try_variant_get` 函数语义

- 签名:`variant_get(v, path[, type])`;2 参返回 variant(LargeBinary),3 参返回指定类型。`type` 必须是字符串字面量(否则 analyzer 报错);v1 类型集:`boolean/int/bigint/float/double/string/date/datetime`,经 `parse_modifier_to_sql_type` 映射到引擎 SQL 类型(`int`→Int32、`bigint`→Int64,与 Spark `variant_get` 的类型字面量语义对齐)。其中 datetime 与 int/float 仅函数语义;shred-取数零拷贝白名单见决策 E(`int`↔Int32 精确匹配虽安全,v1 保守不入白名单,实现期可零成本放开)。
- 语义(Spark 对齐):missing path → NULL;variant null → NULL;cast 失败 → `variant_get` 报错(`CastOptions.safe=false`)、`try_variant_get` → NULL(`safe=true`)。
- 实现:kernel-backed;常量路径快速通道(现有家族逐行重解析路径的已知开销不复制)。
- 注册必须三表同步:`analyzer/functions.rs`、`codegen/expr_compiler.rs`(对未知名报错)、`lower/expr/function_call.rs` 允许表;exec 侧 `VARIANT_FUNCTIONS` + `VARIANT_METADATA` 双表同步。
- 裁剪与严格模式的可观察宽松:被裁剪页中本会 cast 报错的行不会报错(Spark 同样行为),文档明记。

## 5. Shredded 写入

- **属性键**:`write.parquet.variant-shredding.<列名>`,值 `'a bigint, b.c string'`(路径+类型列表)。`write.*` 命名空间是硬约束(ALTER denylist 拒 `novarocks.*` 放行 `write.*`)。与 iceberg-java 官方 `write.parquet.shred-variants`(布尔+推断)不冲突不混用,NovaRocks 不读该键。
- **类型集**与决策 E 白名单一致;值解析复用 `parse_sql_type_string` + 括号感知逗号切分。
- **校验两处**:DDL 期(CREATE/ALTER 命中前缀即解析校验)+ 写入打开期(`StagedWriteContext::from_table`)。
- **机制**(收口于 `write_record_batches` 唯一 choke point,INSERT/CoW/MoR/MERGE-insert/compaction/MV refresh 自动继承):
  1. 现有 `transform_variant_columns_for_write` 产出 `Struct{metadata,value}`;
  2. 属性命中列接 `VariantArray::try_new` → `shred_variant(as_type)` → `into_inner()`;
  3. 写侧 arrow schema 从 shredded 数组实际类型派生(BinaryView!),回贴 extension + field-id 元数据。
- **vendor PATCH 8**:vendored `ParquetWriter` 文件 schema 从 iceberg schema 派生(2 子列),不接受 3 子列 batch(`parquet_writer.rs:492`)——补"允许显式 arrow schema 覆盖"。
- **统计安全(已验证)**:两套 stats 收集器均按 field-id 跳过无 id 的 typed_value 叶子,manifest 写入无完整性假设;写侧无 variant manifest 统计(裁剪只靠读侧 parquet 统计,与决策 D 一致)。
- **行为语义**:未开属性的表重写 shredded 输入时输出非 shredded(de-shred,文档明示);开属性的表 compaction/CoW 输出保持 shredded。

## 6. 行级变更解禁

**守卫移除**(`ensure_no_variant_columns_for_row_level_mutation` 全部 6 个调用点):`delete_flow.rs:140`、`mutation_flow.rs:94`、`mutation_flow.rs:1467`、`equality_delete_flow.rs:82`、`iceberg_writer.rs:108`、`iceberg_writer.rs:118`。

| 路径 | 数据搬运 | 解禁所需 |
|---|---|---|
| DELETE(PD/DV) | 只写位置删除,零 variant 字节 | 守卫移除 + 匹配扫描修剪 |
| MoR UPDATE | DV + 新行(标准扫描来源 LargeBinary) | 守卫移除即可 |
| CoW UPDATE | 全文件重写,混拼两种编码 | 同步修 seam |
| MERGE | 三个独立子事务复用上述路径 | 随上述 |
| OVERWRITE | 已验证 INSERT 路径 + commit 删旧 | 守卫移除即可 |
| 等值删除 | 只写 key 列 | 守卫移除 + key 禁 variant |

**必须同步修的两点**:
1. **CoW seam**:`user_batch_from_scan_batch`(`mutation_flow.rs:1138-1159`)把 iceberg-rust reader 的 Struct 输出(含 shredded,经 kernel 重建)collapse 成 LargeBinary 再与 `new_rows` 拼接。
2. **DELETE 匹配扫描修剪**:`scan_for_position_deletes_at`(`delete_flow.rs:951-954`)从选全列改为 `_file`/`_pos` + WHERE 引用列(WHERE 本就不允许直接比较 variant,修剪后 DELETE 不触碰 variant 数据)。

**保留并补强的次级守卫**:
- variant 作分区源/排序键保持拒绝;`ensure_iceberg_write_supported` 从 UPDATE/MERGE/DELETE 入口也可达(今天只有 INSERT/OVERWRITE 调用)。
- 等值删除 key 含 variant:显式拒绝 arm + 清晰错误(今天靠 `primitive_to_arrow_type` catch-all 兜底)。
- DELETE/UPDATE WHERE 直接比较 variant 列:保持 fail-fast(`variant_get` 谓词是受支持形式)。
- MERGE INSERT 保留列拒绝(`_row_id` 等):不变。

**顺序依赖(硬)**:shredded 读正确性(PR-1)必须先于守卫移除落地,否则 CoW/compaction 读 Spark-shredded 文件即静默丢数据(compaction 今天已无守卫,PR-1 同时除掉既有隐患)。

**待 spike 风险**:iceberg-rust `ArrowReader`(CoW/DELETE 使用)对 variant 列读取行为未实证(PATCH 6 只改 schema 映射);不行则把 CoW 重读切到 NovaRocks reader 或加 collapse shim。

## 7. 分区/排序

- variant 自身作分区源/排序键:**保持 spec 拒绝**(Iceberg V3 variant 不可比较)。roadmap 一句话中的"variant 列参与分区/排序"按非目标节口径执行。
- **补 DDL 期守卫**:今天 CREATE/ALTER 不拒绝、第一次写入才报错——错误提前到 DDL。
- variant 表用其他列分区/排序:链路已通(分区写分支同样过 transform kernel),解禁后为 mutation 路径补端到端用例。

## 8. 错误处理 / fail-fast 边界

| 场景 | 行为 |
|---|---|
| reader 遇 shredded 文件(PR-1 落地前的过渡提交) | 静默置 null 换成显式错误 `shredded variant read not supported yet`;完整支持落地后替换 |
| `variant_get` cast 失败 | 报错(Spark 对齐);`try_variant_get` → NULL |
| 类型字符串非法 / 第 3 参非字面量 | analyzer 期 fail fast |
| 路径非字面量 | 规则不触发(函数逐行求值,无下推),不报错 |
| shredding 属性值非法 | DDL + 写入打开双重校验,报错点名属性键 |
| RF 静默失效 | 重建映射 + 专项回归;debug 日志记录 RF 降级事件 |

## 9. 测试策略

**单元/差分**:kernel vs 现有 `variant_query` 逐行差分(语义一致性持续防线);shred/unshred round-trip;shredded 物理类型 min/max 比较器;RF 映射重建。

**sql-tests**:
- `iceberg-dml`:`variant_unsupported.sql` 负例翻正(与无 variant 对照表语义一致);shredding 属性写入用例;断言一律走 `variant_typeof`/`variant_get`(runner 文本协议不能裸 SELECT variant,既有约束)。
- `optimizer`:`VariantPathPushdown` EXPLAIN VERBOSE 全文 golden(`variant columns:` 行)+ `@explain_contains` 内联断言 + 规则开/关结果一致对照。
- 裁剪有效性:reader 级 pruned-row-group 计数(debug 日志)+ `row_group_selector` variant 通道单测;不发明新观测面。

**跨引擎**:fixture Spark 3.5.5 无 VariantType(jar 实证)。
- v1 主力:arrow-rs 生成 canned shredded parquet(spike 同款代码)+ `ADD FILES` 挂表 → 覆盖 shredded 读/裁剪/mutation 读端。
- v1 自验:NovaRocks 写 shredded → 读 round-trip;写侧 footer 断言(LogicalType::Variant + spec 布局)。
- **IV3-6.1 子任务**(不阻塞):Spark 4.x fixture 升级 + 真跨引擎用例(13 个既有 compatibility golden 的 Spark 4 行为差异是独立风险面)。

## 10. 交付切分(PR 序列)

```text
PR-1 shredded 读正确性(地基):typed_value 检测 + kernel 重建 → LargeBinary,
     覆盖 iceberg align 与 convert_variant_columns 两条路径;canned 文件测试
  ├─→ PR-3 行级变更解禁:守卫移除 + CoW seam + DELETE 扫描修剪
  │        + 次级守卫补强 + DDL 期分区/排序守卫 + 负例翻正
  └─→ PR-6 shredded 写:属性解析 + shred 接入 + vendor PATCH 8 + round-trip

PR-2 variant_get/try_variant_get(kernel-backed,三表同步注册)
  └─→ PR-4 VariantPathPushdown:thrift 载体 + lowering spec
           + 合成槽位 per-file 自适应取数(零拷贝,不带裁剪)+ EXPLAIN + plan goldens
        └─→ PR-5 裁剪通道:field-id 寻址 row-group/page 裁剪 + RF 映射重建 + 位置对齐 gate

IV3-6.1(并行子任务):Spark 4 fixture 升级 + 真跨引擎用例
```

PR-1/PR-2 无依赖可并行;PR-4 不带裁剪有独立价值(shredded 零拷贝取数省去全列重建+逐行解码);裁剪单拆因牵动 RF/位置对齐两个高危面。

## 11. 风险与未决

1. iceberg-rust `ArrowReader` variant 读取行为未实证(影响 PR-3 的 CoW/DELETE)——PR-3 内先 spike。
2. `parquet-variant-compute` 是 experimental feature,版本演进 API 可能漂移——以 58.2.0 行为为锚加测试钉住(尤其 `try_perfect_shredding` null-union 与 `canonicalize_shredded_types`)。
3. timestamp 时区语义分歧(内部 local-offset vs kernel UTC)——v1 datetime 不参与 shred-取数,后续统一时再放开。
4. `parse_json` 注册返回类型为 Utf8 的既有疑点——`variant_get(parse_json(s), ...)` 组合的类型检查在 PR-2 中核实并处理(函数接受 Utf8 入参的 JSON-string 分支为兜底)。
5. NovaRocks 写出的 shredded 文件被 Spark/iceberg-java 读回的互操作性,受 fixture 限制 v1 无法真验——footer 断言 + IV3-6.1 补真实读回。
6. 嵌套结构内的 variant(struct/list 内)既有缺口(`variant_field_indices` 只看顶层),shredding 继承该限制,不在本任务修。

## 12. 验收标准(对照 roadmap)

- 含 variant 列的表正确执行 DELETE / UPDATE / MERGE / OVERWRITE,结果与不含 variant 的对照表语义一致。✅ PR-3
- variant 路径谓词能下推并裁剪(row-group/page 级,shredded 文件)。✅ PR-4/5
- 跨引擎读一致:v1 以 canned 文件 + NovaRocks round-trip 覆盖;真 Spark 跨引擎 → IV3-6.1。
- `variant_get`/`try_variant_get` 与 Spark 语义对齐。✅ PR-2
