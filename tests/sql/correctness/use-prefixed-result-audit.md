# USE 前缀结果比较审计

状态：runner 修复及四个 suite 复验已完成；仍有失败，不能宣称 SQL 全部通过。已确认的引擎问题与待判定差异均保留原期望。

## 修复与验证方法

基线为 `94304f0154b6af25cc7d0378557ae2ab3ccd15ff`。runner 复用执行层的 SQL
分句器，按最后一条实际语句判断是否隐式跳过比较。`USE db; SELECT ...` 必须
比较，`USE db; SET ...` 仍隐式跳过。显式 `@skip_result_check=true` 的行为不变。

runner 的 243 个 Rust 单元测试通过，包括 USE + SELECT、USE + SET、纯 SELECT、
注释或字符串中的分号，以及无效 SQL 的保守分类。

SQL 验证使用本 worktree 的生成配置，每次由 harness 启动新 1FE+3BE 集群；
启动日志确认四个进程的构建身份均为上述 SHA。harness 分配并记录实际端口。

```bash
source docker/iceberg-rest/runtime/current/env.sh
NOVAROCKS_BIN="$PWD/target/dev-opt/novarocks" \
NO_PROXY=127.0.0.1,localhost \
target/debug/novarocks-sql-test \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite aggregate --mode verify \
  --cluster-mode cross-process --cluster-size 3 --query-timeout 180 -j 1 \
  --write-actual-dir /tmp/use-prefix-actual
```

每个 case 的正式 verify 在第一个失败后跳过后续普通步骤。因此还需要将失败
case 的诊断输出收集到临时目录，逐条检查后续差异。临时输出不能整体覆盖仓库
期望；只有下表中有独立依据的记录才能更新。

## 已有独立依据的旧期望

| Case | Query | 依据与修正 |
|---|---|---|
| `agg_test_agg_with_limit` | 39/40/42/43/44 | q34 插入 1..9000 的余数数据，q35 插入 9000 条 NULL 测量值，q37 复制到 t5。对 c0 分组，每组 sum(c3)=9000；对 c1/c2 分组，每个非 NULL 余数出现 1000 次，sum=余数×1000。NULL 组的 max/sum 仍为 NULL。q39 的实际四组 9000 与独立枚举计算一致。旧约 100000 的记录来自更大 fixture。 |
| `agg_test_approx_top_k_with_null` | 8/9/11/12/14/15/18/19 | q3 从 1000 行开始，q4 复制四次后是 16000 行，而非 128000。0/1 各 8000；每个 c1 组有 1600。q10 增加 1000 个 NULL（每组 100），q13 再增加 16000 个 NULL，所以后续 NULL 总数 17000、每组 1700。t2 的 NULL 总数为 16000。q8 实际 8000/8000 与计算一致。 |
| `agg_test_avg_over_flow` | 5 | 两组数据完全相同；avg(v2)=32500000，avg(v3)=4.5。使用 Decimal 独立计算 `32500000 - 1.86659630566164 * (4.5 - 3.062175673706)` 得到 `32499997.31616242434918316908083784`，与实际输出一致。旧记录截断到小数点后 18 位。 |
| `agg_test_bitmap_agg` | 28 | `bitmap_union_int.rs` 明确规定没有有效输入时输出 NULL，并有 empty/all-null 单元测试；非 NULL 的空 bitmap 与 NULL 不同。fixture 的 c1=4/6 为 NULL 输入，c1=5 的负值被 bitmap 输入转换过滤。旧空字符串记录应为 NULL。 |
| `agg_test_bitmap_agg` | 30/48 | 同一空输入契约用于 bitmap_union_count(to_bitmap(...)) 和 bitmap_agg；无有效 bitmap 的组输出 NULL。 |
| `agg_test_bitmap_agg` | 31/32 | c1=5 的 c3=-11、c4=-111 均为非 NULL，各有一个不同值；普通 COUNT DISTINCT 不应套用 bitmap 的负数过滤规则，旧 0 改为 1。 |
| `agg_test_meta_scan_agg` | 22/23/25/26/28/29 | q21 仅插入 (1),(2),(3)，q24/q27 只重命名列；metadata 与普通扫描的 COUNT 均为 3。补齐旧文件缺失的 section。 |
| `complex_test_array_remove` / `complex_test_array_top_n` | 22/23/24 / 8 | fixture 的对应数组列声明 DECIMAL(10,2)，旧记录错误使用 9 位 scale。用 Python Decimal 检查每个元素数值、顺序、NULL 和 id 均完全相等后改为 2 位输出；Decimal128 精度损失的 q31/32/33 保留原期望。 |
| `function_typeof` | 25/28/31 | Iceberg `catalog_control/type_mapping.rs:75-102` 明确把 tinyint/smallint 存为 int、json 存为 string、bitmap/hll 存为 binary，并递归映射复合类型。旧期望是原始 StarRocks 存储类型；当前 fixture 创建普通 Iceberg 表，typeof 应反映读取到的类型。 |
| `agg_test_string_agg` | 26 | 原 SQL 没有 ORDER BY，要求 Tom:90,Tom:80 的固定拼接次序是无效 oracle。保留这两个值，添加聚合内 ORDER BY score DESC 和 ordered_scores 别名；新集群完整 case 已通过。 |
| 多个 case | 见逐项清单 | 无显式 alias 的列名格式/大小写差异由 `sql/src/analyzer/helpers.rs:208` 的 expr_display_name 生成；数据行相同。只替换 header，保留全部原有数据行，未放宽比较规则。 |


## 已更新 query 清单

共修正 183 个结果 section。列名修正不覆盖数据行。

| Suite / Case | Query | 分类 |
|---|---|---|
| `aggregate/agg_test_agg_with_limit` | 39, 40, 42, 43, 44 | 旧期望：fixture/既有契约（上表说明） |
| `aggregate/agg_test_approx_top_k` | 181, 182, 183 | 旧期望：列名格式（数据行保留） |
| `aggregate/agg_test_approx_top_k_with_null` | 8, 9, 11, 12, 14, 15, 18, 19 | 旧期望：fixture/既有契约（上表说明） |
| `aggregate/agg_test_avg_over_flow` | 5 | 旧期望：fixture/既有契约（上表说明） |
| `aggregate/agg_test_bitmap_agg` | 28, 30, 31, 32, 48 | 旧期望：fixture/既有契约（上表说明） |
| `aggregate/agg_test_distinct_agg` | 50 | 旧期望：列名格式（数据行保留） |
| `aggregate/agg_test_meta_scan_agg` | 22, 23, 25, 26, 28, 29 | 旧期望：fixture/既有契约（上表说明） |
| `aggregate/agg_test_percentile_cont` | 31, 32, 34, 35 | 旧期望：列名格式（数据行保留） |
| `aggregate/agg_test_string_agg` | 26 | 旧期望：补齐 SQL 的确定性排序并固定列名，数据行保留 |
| `complex-type/complex_test_array` | 57, 60, 61 | 旧期望：列名格式（数据行保留） |
| `complex-type/complex_test_array_contains` | 14, 15, 16, 17, 18, 19, 29, 30, 60, 63, 64, 90 | 旧期望：列名格式（数据行保留） |
| `complex-type/complex_test_array_map_2` | 10, 40 | 旧期望：列名格式（数据行保留） |
| `complex-type/complex_test_array_remove` | 22, 23, 24 | 旧期望：fixture/既有契约（上表说明） |
| `complex-type/complex_test_array_sort_lambda` | 12, 39, 43, 45 | 旧期望：列名格式（数据行保留） |
| `complex-type/complex_test_array_sortby` | 34, 36, 39, 40 | 旧期望：列名格式（数据行保留） |
| `complex-type/complex_test_array_top_n` | 8 | 旧期望：fixture/既有契约（上表说明） |
| `complex-type/complex_test_arrays_zip` | 30 | 旧期望：列名格式（数据行保留） |
| `complex-type/complex_test_cast_array` | 5, 6 | 旧期望：列名格式（数据行保留） |
| `function/function_cast_string_to_datetime` | 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68 | 旧期望：列名格式（数据行保留） |
| `function/function_conditional` | 10, 11 | 旧期望：列名格式（数据行保留） |
| `function/function_encode_row_id` | 2, 3, 4, 5, 6, 7, 10, 31, 32, 33, 34, 35, 36, 37, 38 | 旧期望：列名格式（数据行保留） |
| `function/function_field` | 5 | 旧期望：列名格式（数据行保留） |
| `function/function_hash` | 3, 4 | 旧期望：列名格式（数据行保留） |
| `function/function_time_slice` | 1, 2, 3, 4, 5, 6, 7, 8, 22, 23, 24, 25, 26, 38, 39, 40, 41, 42, 43, 44, 45, 59, 60, 61, 62, 63 | 旧期望：列名格式（数据行保留） |
| `function/function_typeof` | 25, 28, 31 | 旧期望：fixture/既有契约（上表说明） |

## 未重录的差异清单

每个差异 query 都归入下表；“待判定”表示当前证据不足以把旧记录或实现判错，原期望保留。指纹差异尤其不能当作新 oracle。

| Suite / Case | Query | 分类 | 证据 / 后续核验 |
|---|---|---|---|
| `aggregate/agg_test_array_agg` | 38, 281 | 引擎结果错误 | q38 ROLLUP 总计 COUNT(DISTINCT id)=0，但 fixture 存在非 NULL id；q281 另有数组指纹差异。 |
| `aggregate/agg_test_count_distinct` | 21, 26, 29 | 引擎结果错误 | q21 AVG DISTINCT 返回普通 AVG 的 1.6，应为 (1+2+3)/3=2；q26/29 也不同。 |
| `aggregate/agg_test_distinct_agg` | 38, 39, 40, 41, 56, 60, 65, 71, 75, 80, 86, 90, 95, 101, 105, 110, 111, 117, 121, 125, 131, 135, 139, 145, 149, 153, 159, 163, 167, 168, 174, 178, 182, 188, 192, 196, 202, 206, 210, 216, 220, 224, 225, 238 | 待判定，保留期望 | 分组/排序/NULL 或复合指纹的行值不同；尚未建立独立逐行 oracle。 |
| `aggregate/agg_test_ds_hll` | 22 | 待判定，保留期望 | 预录 coupon 与原生 sketch 合并后的准确性布尔断言变成 0；需要独立 sketch 兼容性核验。 |
| `aggregate/agg_test_group_concat` | 255, 257, 259, 261, 263, 265, 267, 274, 276, 279, 281 | 引擎结果错误 | SET group_concat_max_len=4 后仍输出 TomEnglish,TomMath；截断未生效。其他分组/排序结果同时保留。 |
| `aggregate/agg_test_grouping_set` | 5, 9 | 引擎结果错误 | 用 q3 的九个 tuple 在 Python 中独立分别按 (v1,v2) 和 (v3) 分组，忽略 NULL 计算 SUM/MIN；q5 的 13 行 multiset 与旧期望完全一致，与实际不一致。q9 的差异也保留。 |
| `aggregate/agg_test_hll_sketch_count` | 5 | 待判定，保留期望 | lgK=4 的估计未满足 90..110；精度断言可能过紧，不能把断言改成 0。 |
| `aggregate/agg_test_max_min_by_not_filter_nulls_with_nulls` | 2, 3, 4, 6, 8, 9, 10, 11, 12, 13, 14, 16, 18, 19, 20, 21, 22, 23, 24, 26, 28, 29, 30, 31, 32, 33, 34, 36, 38, 39, 40, 41, 42, 43, 44, 46, 48, 49, 50, 51, 52, 53, 54, 56, 58, 59, 60, 61, 62, 63, 64, 66, 68, 69, 70, 71 | 待判定，保留期望 | 分组/排序/NULL 或复合指纹的行值不同；尚未建立独立逐行 oracle。 |
| `aggregate/agg_test_ndv_with_varbinary_type` | 9 | 待判定，保留期望 | 精确基数 100，旧近似估计 101，当前 100；须确认算法/误差契约，未重录。 |
| `aggregate/agg_test_percentile_approx_weighted` | 5, 59, 102 | 待判定，保留期望 | 完整复验中 q5 也出现 35355/35356；诊断中 q59/q102 有差异；需要核验权重/近似插值与整型转换。 |
| `aggregate/compressed_key` | 7, 11, 20, 21, 22, 27, 28, 29, 30, 34, 35, 36, 37, 38, 39, 40, 41, 45, 46, 47, 48, 129, 141, 156 | 待判定，保留期望 | 分组/排序/NULL 或复合指纹的行值不同；尚未建立独立逐行 oracle。 |
| `complex-type/complex_test_array` | 18, 54, 55, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81 | 待判定，保留期望 | 分组/排序/NULL 或复合指纹的行值不同；尚未建立独立逐行 oracle。 |
| `complex-type/complex_test_array_intersect` | 15, 18 | 引擎结果错误 | Decimal128(38,10) 精确匹配得到空交集。 |
| `complex-type/complex_test_array_map_2` | 43 | 引擎结果错误 | q43 期望嵌套数组 [[str],[str]]，实际为平面字符串数组 [str[],str[]]。 |
| `complex-type/complex_test_array_min_max` | 31, 32, 37, 38, 76, 77, 82, 83 | 引擎结果错误 | Decimal128(38,10) 值 1234567890.1234567890 变成 1234567890.1234567000。 |
| `complex-type/complex_test_array_remove` | 31, 32, 33 | 引擎结果错误 | Decimal128 精度丢失，并使应该移除的值留在数组中；仅 scale=2 的无损格式差异已修正。 |
| `complex-type/complex_test_array_sortby` | 15, 16, 21, 22, 27, 28, 68, 78 | 待判定，保留期望 | 分组/排序/NULL 或复合指纹的行值不同；尚未建立独立逐行 oracle。 |
| `complex-type/complex_test_array_sum_avg` | 4, 5 | 引擎结果错误 | Decimal128 大数求和/平均精度丢失；不能按实际值重录。 |
| `function/function_conditional` | 8, 9, 12, 13, 15, 16, 17, 20, 24 | 待判定，保留期望 | NULL map key/value、复合值差异；需核验构造器与条件表达式的 NULL 契约。 |
| `function/function_encode_row_id` | 8, 9, 11, 12, 13, 20, 21, 28, 29 | 待判定，保留期望 | SHA256 指纹及 sort-key 二进制不同；需要独立编码契约核验。 |
| `function/function_field` | 6, 8, 24 | 待判定，保留期望 | FIELD 的数值/类型转换结果不同，不能按新值直接重录。 |
| `function/function_time_slice` | 9, 10, 11, 12, 13, 14, 15, 16, 27, 28, 29, 30, 31, 46, 47, 48, 49, 50, 51, 52, 53, 64, 65, 66, 67, 68, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98 | 待判定，保留期望 | 年份下界/时间边界结果不同，例如 0001-01-01 变成 NULL；需核验边界语义。 |
| `analytic/analytic_test_array_agg_over_window` | 19, 23, 27, 33, 34, 52, 65, 78 | 待判定，保留期望 | 分组/排序/NULL 或复合指纹的行值不同；尚未建立独立逐行 oracle。 |
| `aggregate/agg_test_ds_hll_staging` | 21 | 待判定，保留期望 | 复验出现 grouped sketch 估计值差异（例如旧 12966、实际 12241）；首轮通过，需核验近似误差与合并次序，不能反复按当前采样值重录。 |
| `complex-type/complex_test_array_contains` | 122, 124, 167, 169 | 引擎结果错误 | Decimal128(38,10) 精确值在 fixture 中存在，但 contains 查询返回空集、position 返回 0；与其他 Decimal128 精度丢失表现一致。未定位到具体丢失精度的阶段。 |
| `complex-type/complex_test_array_contains` | 333 | 待判定，保留期望 | [NULL] 与 NULL needle 的 contains/position 输出 0/0，旧 1/1；需要核验 NULL-element 相等契约。 |
| `complex-type/complex_test_arrays_overlap` | 14, 29 | 待判定，保留期望 | 字符串/浮点数组两个参数顺序都出现 1/0 差异，需核验跨类型相等语义。 |

## 诊断覆盖与执行错误

正式 verify 在一个 case 的首个失败后跳过剩余普通步骤。额外 record 仅写入
`/tmp/use-prefix-record/<suite>` 或 `/tmp/use-prefix-record-last/<suite>`，用于看到失败后的其他差异；从未整文件覆盖仓库期望。
诊断清单比较 header 和行 multiset，仅用于分流，正式 runner 仍按原来的顺序/epsilon 规则比较。

array_contains 的第三次串行 record 完整通过，11 个新增 header 差异的数据行都相同，已仅修正 header；后续 verify 实际比较到 q122（实际 0 行、期望 2 行）。arrays_overlap 的第三次 record 也完整通过，完整诊断仅见 q14/q29 两个行值差异。

两个复杂类型诊断 case 在新集群、串行复试时仍中断：
`complex_test_array_contains` step 9、`complex_test_arrays_overlap` step 15 返回
`Query execution was interrupted because the server is shutting down`。首次诊断分别在
step 323/31 中断。前两次未得到完整 record 文件；第三次串行运行完成了两个 case 的诊断。此中断错误根因尚未确定。

aggregate 的 approx_top_k、function 的 encode_row_id、analytic 的 window_function_streaming
在串行诊断复试中全部执行完成；前两者补齐了诊断差异清单，后者没有结果差异。

首次 debug aggregate 的 S3 PermissionDenied 和 frozen endpoint snapshot 错误未在
完整 dev-opt 复跑中重现；记录为首次运行异常，未改动期望来掩盖它们。

complex_binary_comparison q105 在最终完整 verify 返回 MySQL result delivery cancelled: ServerShutdown；首轮通过，保留期望，作为执行异常报告，根因待查。

## 验证回执

runner library 单元测试 **243/243 PASS**；`cargo fmt -p novarocks-sql-test-runner --check` 和 `git diff --check` 通过。

首轮完整 verify 的 case 回执为 aggregate 69/15、complex-type 24/13、function 28/7、analytic 34/2（PASS/FAIL）。

修正后的 aggregate 重复整套验证在完成 73/84 case 后，因持续 case 间长停顿及 FE 高 CPU 被主动结束；完整日志和两秒线程采样保留。剩余 11 case 加已改动的 string_agg 用新集群单独复验，下面 aggregate 的数字是每个 case 最新回执的合并，不能当作一次连续整套运行的 footer。其他三套均为完成整套 verify 的 footer；complex-type 的 array_contains 在随后修正 header 后又定向复验，仍失败，失败点已从 q15 移至 q122，计数不变。

每次调用均启动新 1FE+3BE，使用生成配置；进程构建身份均为基线 SHA，runner 则包含本次工作区修复。未执行全仓 CI。以上为发布前验证回执；发布信息以关联 PR 为准。

| Suite | PASS | FAIL | 总 case |
|---|---:|---:|---:|
| aggregate | 72 | 12 | 84 |
| complex-type | 27 | 10 | 37 |
| function | 31 | 4 | 35 |
| analytic | 35 | 1 | 36 |

最终首个失败点如下；对未重录差异的分类及依据见上表，执行中断另列。

| Suite / Case | Query | 最终失败类型 |
|---|---|---|
| `aggregate/agg_test_array_agg` | 38 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_count_distinct` | 21 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_distinct_agg` | 38 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_ds_hll` | 22 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_ds_hll_staging` | 21 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_group_concat` | 255 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_grouping_set` | 5 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_hll_sketch_count` | 5 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_max_min_by_not_filter_nulls_with_nulls` | 2 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_ndv_with_varbinary_type` | 9 | 行值差异（分类见上表，保留期望） |
| `aggregate/agg_test_percentile_approx_weighted` | 5 | 行值差异（分类见上表，保留期望） |
| `aggregate/compressed_key` | 7 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_binary_comparison` | 105 | 执行错误（原期望不变，根因待查） |
| `complex-type/complex_test_array` | 18 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_test_array_contains` | 122 | 行值差异：精确 Decimal128 匹配失败，保留期望 |
| `complex-type/complex_test_array_intersect` | 15 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_test_array_map_2` | 43 | 列名及行值均不同（完整诊断已收集，原数据行保留；分类见上表） |
| `complex-type/complex_test_array_min_max` | 31 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_test_array_remove` | 31 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_test_array_sum_avg` | 4 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_test_arrays_overlap` | 14 | 行值差异（分类见上表，保留期望） |
| `complex-type/complex_test_array_sortby` | 15 | 行值差异（分类见上表，保留期望） |
| `function/function_conditional` | 8 | 行值差异（分类见上表，保留期望） |
| `function/function_encode_row_id` | 8 | 列名及行值均不同（完整诊断已收集，原数据行保留；分类见上表） |
| `function/function_field` | 6 | 列名及行值均不同（完整诊断已收集，原数据行保留；分类见上表） |
| `function/function_time_slice` | 9 | 列名及行值均不同（完整诊断已收集，原数据行保留；分类见上表） |
| `analytic/analytic_test_array_agg_over_window` | 19 | 行值差异（分类见上表，保留期望） |

本地证据位于 `logs/sql-use-prefix-audit/`：`receipt.json`、四个 `*-final-verify.log`、初轮 `*-verify.log`、诊断 `*-record*.log` 和逐 query 差异 `*-diffs.json`，以及 contains/overlap 的第三次完整诊断和 contains 的最新定向 verify 日志。这些文件被 gitignore 排除；原生 failure artifacts 仍保留在 `logs/sql-test-failures/`，完整路径记录在相应日志中。fixture 数学修正和每个保留差异的 query 清单已记录于本文，便于后续独立处理引擎问题。
