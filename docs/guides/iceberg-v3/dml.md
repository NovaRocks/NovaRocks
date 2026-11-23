<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DML（INSERT / DELETE / UPDATE / MERGE / CTAS / TRUNCATE / ADD FILES）

> NovaRocks 支持 INSERT、DELETE、UPDATE、MERGE、TRUNCATE 与 ADD FILES。CTAS 使用标准 REST staged create。Catalog 写入准入和 v3 row-lineage 要求见各节。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| INSERT INTO（VALUES / SELECT） | ✅ | 零行结果跳过提交 |
| DELETE FROM（V2 position-delete + V3 DV 双路径） | ✅ | |
| UPDATE（MOR + UPDATE FROM source） | ✅ | PR #76 |
| MERGE INTO（matched UPDATE / matched DELETE / not matched INSERT） | ✅ | PR #78 |
| OPTIMIZE TABLE（whole-table 重写） | ✅ | 见 [maintenance](maintenance.md) |
| CTAS（`CREATE TABLE AS SELECT`） | ✅ | 标准 REST staged create，格式版本 1–3，缺省 2 |
| TRUNCATE TABLE | ✅ | 清空目标 ref，保留 schema 和历史 |
| ADD FILES（登记已有 Parquet） | ✅ | 按 catalog 启用，规划目标为 v1/v2 非分区表，见下文 |
| CDC sink（Flink-style 持续写入） | ❌ | |

---

## 并发提交政策

源输出计算完成后，每次 attempt 仍需验证操作依赖。

| 操作 | 提交时验证 |
|---|---|
| 普通 INSERT（VALUES / SELECT） | 核对同一目标表及写出所用的默认分区规范，可在新 head 上重新准备 |
| ADD FILES | 候选路径未在准确 `main` 快照存活。快照变化时复核候选，依赖名称映射时同时保护整版 metadata |
| DELETE / UPDATE / MERGE | 按隔离级别验证准入观察与连续快照窗口 |
| OPTIMIZE、主分支 TRUNCATE | 按各操作协议在本轮基准核对，见 [lake publication](lake-publication.md#冲突与重新准备) |
| MV 全量刷新、重分区及其它绑定基线的文档提交 | 目标 ref 与输出基线相同 |
| ANALYZE | 测得统计的快照在准备时存在；发布时存在性不受原子条件保护 |

要求目标 ref 不变的语句遇到 head 变化时以确定未提交的冲突结束。仅当 Catalog 已确定拒绝且该操作
的依赖仍成立时，才允许重新准备。重试表属性、清理结果与 Unknown 核对见 [lake publication](lake-publication.md)。

## 登记已有 Parquet 文件

在目标 Iceberg catalog 的 properties 中设置 `"iceberg.add-files-procedure.enabled" = "true"`。缺省关闭。目标在规划时须为 v1/v2，执行时须用非分区 spec，操作追加到 `main`。HMS 保持只读，MV 存储表保留原守卫。目录或单文件都可作为 `location`，`format` 须显式给出 `PARQUET`。

```sql
ALTER TABLE lake.sales.orders EXECUTE add_files(
    location => 's3://landing/orders/2026-11-20',
    format => 'PARQUET',
    recursive_directory => 'false'
);
```

| `recursive_directory` | 目录行为 |
| --- | --- |
| `'true'` | 登记可见的子目录文件 |
| `'false'` | 跳过子目录，只登记顶层文件 |
| `'fail'`（缺省） | 遇到可见子目录文件即失败 |

目录中的隐藏条目及目录标记跳过，末尾斜杠不改变这个规则。候选按路径处理，读取时检查实际 Parquet footer。对象存储前缀完全不存在时返回 `NOT_FOUND`。存在的目录或前缀若只有 `_SUCCESS` 等跳过项，仍提交一次空 append。成功返回 `metric_name varchar` 和 `metric_value bigint`，一行 `('added_data_files', N)`。Trino update type 为 `ALTER TABLE EXECUTE`。

标准 REST 登记有嵌入式 field ID 的文件。无 ID 文件需要能比较整版 metadata 并进行条件安装的 Hadoop catalog。已有 `schema.name-mapping.default` 时沿用，缺省时随 append 安装经核对的映射。已有映射的 REST 表可正常读取无 ID 文件，登记新无 ID 文件仍返回 `NOT_SUPPORTED`。

登记先共享实际读取绑定，能正常忽略未映射的额外列。文件的原始字段只有部分 ID、目标绑定使用当前未支持的载体（如 INT96），都会拒绝。必填列有 NULL 或无法从 footer 确认其 NULL 个数为零时返回 `CONSTRAINT_VIOLATION`。重复的存活路径返回 `ICEBERG_COMMIT_ERROR`。成功后可从 `"orders$files"` 检查登记路径、记录数和列指标，并从 `"orders$snapshots"` 核对追加快照。

准备期间保持输入路径和字节稳定。登记失败时，只清理操作生成且获得删除权的元数据。登记后，文件按表的快照生命周期管理，源在表目录之外也可能在最后一份引用快照过期后被清理。源在表目录内时，应与能扫描同一前缀的外部孤儿文件清理错开。遇到结果未知，按[发布核对流程](lake-publication.md#不确定提交)只读核对原快照标记。结论仍未知时保留对象并禁止自动重试。设计与并发边界见 [ADR-0246](../../adr/ADR-0246-iceberg-add-files-uses-read-binding-and-atomic-registration.md).

## ✅ INSERT INTO

```sql
-- VALUES
INSERT INTO orders VALUES (1, 1001, 19.90, '2026-05-01');

-- SELECT
INSERT INTO orders SELECT id, user_id, amount * 1.1, ts FROM orders_staging;

-- 显式列表（配合 default value 自动 fill）
INSERT INTO orders (id, user_id, amount) VALUES (2, 1002, 30.00);
```

输出为零行时跳过提交。v3 的新逻辑行在提交时分配 `first_row_id`，snapshot 的 row range 取实际 manifest-list 分配量，见 [row-lineage](row-lineage.md)。

## 迁移旧覆盖写

全表清空使用 `TRUNCATE TABLE`，再按需执行 `INSERT INTO`。按条件替换可用
`DELETE ... WHERE` 加 `INSERT INTO`，或将适用的匹配更新、删除和新增表达为 `MERGE`。
由两条语句完成的替换会进行两次独立提交。

## ✅ DELETE FROM

```sql
DELETE FROM orders WHERE amount < 10.0;
```

V2 / V3 双路径：

- V2 表：写 position-delete 文件
- V3 表：本次与旧删除位置的并集部分覆盖数据文件时写 Puffin DV；完整覆盖时逻辑移除数据文件及它独占的旧位置删除项，不新增 DV。两种效果均**不分配新 `_row_id`**

入口：`novarocks/frontend-application/src/query_execution/dml/delete/`。

## ✅ UPDATE 与关联更新（MOR）

```sql
-- 简单 UPDATE
UPDATE orders SET amount = amount * 1.1 WHERE user_id = 1001;

-- 关联其他表更新，按 id 匹配
MERGE INTO orders t
USING (SELECT id, new_amount FROM repricing_staging) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.new_amount;
```

NovaRocks 的 UPDATE 与所有 MERGE 形状只走 MOR。有删除效果时按上述逐文件规则物化：部分覆盖写 DV，完整覆盖移除数据文件及独占旧位置删除项；更新分支另写 after-image。完整覆盖移除属于同一 MOR 路径，不是 COW 整文件重写。
`novarocks.update.mode`、`write.update.mode` 及其他 `write.*.mode` 不选择执行算法；已有值、
`copy-on-write` 或非法值均不能改变原 v3、类型与 ref 准入。

MOR 更新行的 `_last_updated_sequence_number` 写 NULL，读时继承实际提交的 data sequence；
其他 branch 先提交也不会使它落到预测的旧版本。入口：`novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs`。

### 赋值类型与必填约束

赋值继续使用普通 SQL 转换，在完整来源查询之后得到真实值 V，保留来源 NULL 与原字节。
Provider 独立签发规范输入 W 和存储目标 T；定长 data 字段可使用明确许可的 Binary 输入，
W 仍保留 T 的各层必填约束。只有实际选中的 MERGE 分支或 writer assignment 才执行写入投影。

可达嵌套必填 NULL 在 Provider I/O 前以 Data 错误拒绝；父 NULL 或偏移未引用的子值不误拒绝。
定长 `fixed(N)` 存储按可达非 NULL 原字节长度检查，长度不符以 Data 拒绝，不截断或填充有效值。
普通 Binary→Fixed CAST 的坏宽度 NULL 结果不能替代这个检查，普通嵌套 CAST 能力也不扩大。
STRING 转 FLOAT/DOUBLE 的非有限文本仍按普通 CAST 变成 NULL；可空目标接受，必填目标拒绝。
UUID、Variant 等原有 Provider 不可写类型限制保留。上述为写入合同，不宣称所有来源转换或最终
端到端测试已通过；完整来源/目标接线与原生用例的最终验证须分别取证。

## ✅ MERGE INTO

```sql
MERGE INTO orders t
USING (SELECT id, user_id, amount, ts FROM orders_changes) s
   ON t.id = s.id
 WHEN MATCHED AND s.amount IS NULL THEN DELETE
 WHEN MATCHED THEN UPDATE SET amount = s.amount, ts = s.ts
 WHEN NOT MATCHED THEN INSERT (id, user_id, amount, ts) VALUES (s.id, s.user_id, s.amount, s.ts);
```

实现概要：

- 保留原 admitted AST；匹配读取使用 Provider 签发的同一 target ref/base。
- matched UPDATE、matched DELETE 和 not matched INSERT 形成原逻辑 effect，由同一 MOR 写入会话路由。
- after-image、删除目标与统计事实保留 Provider 的 signed ordinals；逻辑计数仍由原 Root 完成事实提供。

要求基表 v3；`write.row-lineage` 不参与准入。

UPDATE、UPDATE FROM 与 MERGE 统一在 MOR 路径按目标行物理位置与命中子句判重，未命中子句的匹配行也参与；重复返回 Trino 119，UPDATE FROM 保留原 UPDATE 语句身份。

### ❌ MERGE INTO 写指定 branch

phase 1 仅覆盖 INSERT / UPDATE / DELETE 写指定 branch，MERGE INTO 暂未支持。

## ✅ CTAS（CREATE TABLE AS SELECT）

```sql
CREATE TABLE t_new AS SELECT * FROM t_old;
```

CTAS 要求支持标准 staged create 的 REST Catalog。Hadoop 和 HMS 会在产生副作用前拒绝 CTAS。

REST CTAS 与普通 `CREATE TABLE` 共用位置规则。显式 `location` 优先。其次使用 namespace location
下的 `<表名>-<UUID>`。namespace 没有 location 时由 Catalog 分配。NovaRocks 选择的位置在写入或
注册前检查是否已有文件。非空位置返回 `ICEBERG_FILESYSTEM_ERROR`。

格式版本缺省 2，可通过 `format_version` 选择 1、2 或 3。显式 `write.row-lineage` 作为普通表属性保留，
v3 的 lineage 能力由格式版本决定。

初始化、追加及启用后的写时统计随同一个 assert-create 请求发布一次。零行结果发布一个空 append
快照并跳过写时统计。`IF NOT EXISTS` 在分析阶段发现表已存在时返回 0 行。分析后被并发建表抢先时，
返回 `TABLE_ALREADY_EXISTS`。确定拒绝时清理本语句拥有的对象，结果未知时按
[lake publication](lake-publication.md#不确定提交) 核对。

## ✅ TRUNCATE TABLE

发布目标 ref 的清空 snapshot，将所有 data / delete / DV 逻辑条目标为 DELETED，保留 schema 和历史
snapshot。它不启动 BE 数据 writer，也不立即删除历史仍引用的物理文件。

```sql
TRUNCATE TABLE orders;
```

支持 v2 与 v3；`TRUNCATE ... PARTITION (...)` 和 `TRUNCATE ... WHERE ...` 在解析阶段拒绝。

## ❌ CDC sink

Spec：Flink-style 持续从 source 流（Kafka / 上游 CDC）写入 Iceberg，按 sequence number 切 snapshot。

**TODO**：未实现。如果你需要 CDC，目前只能让 Flink / Spark 直接写 Iceberg 表，再让 NovaRocks 读。
