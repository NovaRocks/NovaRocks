# 追加套件迁移 Iceberg v3 结果（complex-type / decimal / function / iceberg-mv-scheduler）

承接 `2026-05-31-stable-sql-suites-iceberg-v3-migration-design.md`，用同样方式迁移并测试
这 4 个套件。这 4 个套件原本都在每日稳定集的"排除"列表里（非每日验证）。

## 结果总览

| 套件 | 结果 | 说明 |
|---|---|---|
| **iceberg-mv-scheduler** | ✅ 4/4 | 本就是 Iceberg v3；只需用**启用 MV 调度器的 server 配置**（`$NOVAROCKS_STANDALONE_SCHEDULER_CONFIG`，含 `mv_refresh_scheduler_enabled=true`）运行。无代码改动。manual_policy 首跑偶发时序抖动，复跑稳定。 |
| **complex-type** | ✅ iceberg 31/33 + native 3/3 | **拆分**：33 个可迁用例→`complex-type`（Iceberg v3）；3 个含 iceberg 禁止的 **null map 键** 的用例（complex_binary_comparison / complex_group_by / complex_in_predicate，null 处理是显式目标）→新建 `complex-type-native`（原生）。 |
| **function** | ✅ iceberg 26/27 + native 8/8 | **拆分**：存储无关用例→`function`（Iceberg v3）；8 个 **BITMAP/HLL 聚合表存储** 用例（BITMAP 列本身是被测对象，无法靠删行修复）→新建 `function-native`（原生）。 |
| **decimal** | ⛔ 不迁移（保持现状） | 10/13 文件测 **DECIMAL256**（precision 39-76）。NovaRocks standalone 的 Arrow 执行层是 Decimal128（上限 38），**原生也全失败**（`build DECIMAL array failed: precision 40 > max 38`）——与存储无关的预存不支持。让其通过需在引擎实现 DECIMAL256（巨大工程，另立）。 |

## 关键约束：runner 无逐用例 catalog 覆盖

`tests/sql-test-runner/src/parser.rs` 明确 `@catalog` 仅限 suite init.sql（套件级唯一）。
因此**一个套件内无法混合 iceberg 与原生存储**。对"多数可迁、少数 iceberg 不兼容"的套件
（complex-type、function），唯一保留全部覆盖又让多数走 iceberg 的办法是**拆成两个套件**
（`<suite>` iceberg + `<suite>-native` 原生），新原生套件无 init.sql。

## 引擎修复 #6：binary 函数接受 LargeBinary（两层）

iceberg 二进制（VARBINARY）列被 Parquet 读路径物化为 Arrow **LargeBinary**（字符串=Utf8）。
`from_binary`/`to_binary`/`sha2`/`aes_*`/`xx_hash3_128` 的类型守卫只认 `Utf8|Binary`，故对
iceberg 二进制列报 `arg0 must be VARCHAR or VARBINARY`。修复需**两层**：
- 降级层（`src/lower/expr/function_call.rs`）：守卫加 `LargeBinary|LargeUtf8`（commit `b0fc8b25`）。
- 执行层（`src/exec/expr/function/encryption/common.rs` 的 `to_owned_bytes_array`、
  `src/exec/expr/function/bit/xx_hash3_128.rs` 的 `to_bytes_array`）：报错前用 arrow cast 把
  `LargeBinary→Binary`、`LargeUtf8→Utf8` 归一化（commit `aaade167`）。
修复 `function_binary_functions`；嵌套/不兼容类型仍 fail-fast。

## 预存失败（与本次无关，存储无关，已记录不追）

- complex-type：`complex_test_array`（`ARRAY` 不支持 `>` 比较操作符）、`complex_test_array_sortby`
  （120s 查询超时，debug 慢）——native 上也失败。
- function：`function_time_slice`（`time_slice` 第二参须为常量 interval）——native 上也失败。
- decimal：整套（DECIMAL256 不支持，见上）。
- `cargo test --lib`：5 个预存失败（`mv_shape`×2、`pipeline::builder`×3，源自 main 的 45f6e676）。

## 测试数据调整（按 join 先例，已授权）

- complex-type 的 3 个 null-map-键用例：移到 `complex-type-native`（保留原生 DDL + 原生 golden）。
- `function_conditional`：含 iceberg 无效的 null-map-键条目（`map{1:null,null:null}`），按 join 先例
  仅删 null-键条目（保留非空），golden 未变。

## 已知非阻塞遗留

- 迁移辅助脚本 `tools/dev/migrate_suite_iceberg_v3.py` 的 `create\s+table` 正则会被**含
  "create table ("的注释**误触发（本次手修了 2 个受影响文件：function_binary_type、function_regex；
  verify 门兜底）。未来复用前宜加"跳过注释行"。
- runner 把 `USE ${case_db};\n<SELECT>` 当多语句步，会继承 `@skip_result_check` 而不做结果比对
  （预存 runner 行为，非本次引入），导致此类步的 golden 实际不参与比对——覆盖弱于表面。
