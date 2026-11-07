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

# SQL correctness entrypoint

原生 runner 仅发现 `tests/sql/correctness/`。当前阶段只维护功能与数据正确性，SQL benchmark binary、专用 workload、比较/采样协议及隐藏参数已退出；普通 elapsed 输出是运行说明。资产准入、失败保留和批准例外见 [测试治理](../../docs/guides/development/test-governance.md)，suite 覆盖见 [correctness map](correctness/README.md)。

## 原生运行与输入

生产验收使用 cross-process 1FE+3BE。runner 通过唯一显式 `NOVAROCKS_BIN` 指定主 Server，原生 owner 为实际选中有限动作建立准确授权并拥有 spawn、READY、执行和实际 shutdown；`all-in-one` 只作 smoke。标准 dev 的能力由实际 cfg 编入，能力不授予控制权。

先用规范 [dev-test.sh](../../tools/ci/dev-test.sh) 同次准备 workspace tests/bins，真实 Cargo JSON 绑定路径、自报和 hash。CLI 保留原 `tests/sql/runner/Cargo.toml` 与包身份，库位于其子目录；裸 `cargo run --manifest-path tests/sql/runner/Cargo.toml -- ...` 仍明确运行 SQL CLI。日常/CI 使用已准备程序，退出次级构建。完整合同见 [构建选择](../../docs/guides/development/test-governance.md#构建选择与控制授权)。

非隔离套件先绑定验证过 BOM 的 fixture，再解析一次 immutable publication；不猜固定端点和凭据：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
tools/ci/dev-test.sh prepare logs/sql-attempt/artifacts.json
source tools/ci/lib/build_capabilities.sh
export NOVA_CI_ARTIFACT_RECEIPT="$PWD/logs/sql-attempt/artifacts.json"
export NOVAROCKS_BIN="$(ci_bound_binary novarocks)"
sql_runner="$(ci_bound_binary novarocks-sql-test)"
"$sql_runner" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite filter --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1
```

共享fixture由机器owner显式start/upgrade，普通up只附着ready同版本机器。SQL在首次scratch或私有fixture副作用前准入 `--fixture-budget-secs`（默认3600），继承原run ID与完整期限，工作期只能收窄，原完整期限内预留清理；超上限BLOCKED，准入后到期FAIL。namespace使用完整run身份与有限ordinal，保留原CREATE/golden；不写namespace location，JDBC默认表/view、rename、显式位置和CTAS统一验证属于固定三范围。独立原生运行确认角色及producer退出后按整运行catalog→IAM→文件顺序清理；嵌套/外部server保留原owner责任与deferred事实。嵌套shell/retry/role restart不续期；只读READY继续原warehouse与check-only。见 [ADR-0191](../../docs/adr/ADR-0191-machine-shared-fixture-and-bounded-scratch.md)。

依赖 HMS fixture 的功能测试只维护在 `iceberg-hms` suite，除非明确要求，否则不运行。列举准入资源上界由默认 connector 组件测试保护。

HMS 的 `iceberg-hms` suite 以封闭的 `hms-read-only-compatibility-v1` case 输入合同声明准备工作。runner 先做纯定义读取和 case 选择，由同一运行的 owner 在最终 SQL 绑定前准备真实表并冻结首个 append snapshot ID；只读发现入口不启动 producer，也不补造 snapshot ID。准备沿原 run 身份、namespace、storage 和截止时间执行，cloneable 的 SQL case 不携带清理权限。

固定 recipe 的 q1 核验已准备的输入，q11 以 metadata 查询和独立行断言核验冻结 ID，q12 保留 numeric `VERSION AS OF` 和原三行期望。完整 59 步、38 个拒绝断言和 11 段旧期望继续受原比较规则约束。准备 receipt、绑定后每步实际 SQL 的 hash 以及输入 hash 保存在 `tests/sql/.runtime/hms-evidence/<namespace>/`。

该 owner 在公共收尾中保管失败、取消和未使用的准备结果；清理首次派发前持久登记，同一次失败或 Unknown 不自动重派。远端 Spark settlement、原 host 进程组及 Server 的真实退出分别取证，未知状态保留原 intent，截止时间不续期。具体 fixture 操作见 [HMS fixture](fixtures/iceberg-hms/README.md)。

`--list-suites` 列出所有注册套件，包括 explicit-only；`--list-declarations` 输出 schema 1 的 suite impact、环境输入、reuse 初始状态/范围、重项依据与 case 清单，不启动集群。`--suite all` 按 suite manifest 排除 explicit-only 和 DISABLED，构成常规 SQL 集合的原生来源。`--list-extensions` 不启动集群。默认配置不提供完整 fixture 端点，不能用旧环境补造缺项。隔离 REST 套件由 runner 启停私有 fixture，准备和测试分离，见 [fixture 操作](../../docker/iceberg-rest/README.md)。

能力需求来自 merge/过滤后真正选中的有限步骤、已有指令与原生 capability tag；普通 SQL 仍使用同一主产物，内部断言不能因缺测试产物而跳过。feature 编入、profile、suite 名或旧环境不授予控制，裸测试 CALL 也没有自动权限。`test-capability:mv-document-observation` 只声明非破坏观察，不授予 wipe；未知 `test-capability:` 值明确拒绝。`mv_storage_contract_shared_document_revisions.sql` query 5/6 已就近声明此 tag，并保留原六个普通标签；两步只请求非破坏观察，SQL/expected/oracle 不变。G2 的完整 storage-contract 十个 case 已通过，验证范围与未运行项见 [G2 验证记录](../../docs/guides/development/tst-g2-validation-status.md)。

每次 FE/BE launch/restart 在 spawn 前读真实产物自报，保存原双流、实际退出和角色映射；会话绑定实际 Native process 与准确 statement/动作目标。自报的 profile 声明、binary cfg、package 局部事实及 null/unknown 分列，明确不作发布认证。缺授权、错误产物、原期限到期和实际资源/进程清理失败都保留原始失败，不能用未命中注入的正常结果代替通过。

LNP-3A/LNP-3D 与 `mv-storage-contract` 保留原 SQL、`.result` 和独立文档图，旧 CALL/directive 禁止扩展。当前 whole-family wipe 必须是显式 FE launch/family 授权、一次认领、隔离串行且 MV owner 停止 admission/实际 join 后台；普通裸 CALL 不授予擦除权。唯一后续迁移责任见 [correctness map](correctness/README.md#mv-受控过渡与后续迁移)，不在本次新增恢复或擦除 API。

## Trino 错误断言

```sql
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE TABLE ${case_db}.missing_name AS SELECT CAST(1 AS integer);
```

两项必填，去掉参数周围空白后，分别精确比较本次实际 Trino 错误的 `errorName` 与 `errorType`。每步只能有一条可执行 SQL；注释和引号内分号沿原词法 splitter 处理，负例 SQL 不在 runner 内做语义解析。空步骤、多语句、shell 和同一步其他报错指令均拒绝。该断言不进行外层重试或 transient commit 重试；成功、HTTP／transport／本地控制失败、额外错误和任一字段不符都失败，消息文本不能替代结构化错误。

verify、record 与 conversion 共用同一判定和 `expected_error` oracle。执行调用返回本次所有 attempt／statement 的实际提交、完成、typed result 与失败；后续查询不改写前次事实。普通多语句 conversion 仍保留成功 suffix 与全部失败，基础设施失败停止普通后继；多个错误不能被首个匹配错误变成通过。解析与 HTTP 边界回归属于原 runner 常规 Rust 测试，复用其本地独占 peer 与退出 owner，不需额外 fixture。

## golden 格式

golden 保存按 Trino 列类型解码后的规范值，不是 CLI 显示文本。格式与来源裁决见 [ADR-0210](../../docs/adr/ADR-0210-sql-golden-is-canonical-rendering-of-typed-trino-results.md)，实现由 runner 的 [golden 模块](runner/library/src/golden/mod.rs) 统一拥有。

### section 与块

每个参与结果比较的 step 都写 `-- query N`，步骤号递增且不重复，单结果文件也不能省略。section 之间恰好一个空行；没有返回列的 step 可使用空 section；有列但零行的结果仍写类型行与列名。同一步的每条返回行语句各写一个类型块，不能把后一个表头拼成数据。以下示例以 `\t` 表示真实制表符：

```text
-- query 1
-- types\tbigint\tvarchar\tmap(integer, varchar)
id\tnote\tattrs
1\tNULL\t{"1":"x","2":"y"}
2\t"NULL"\t{}

-- query 3
-- shell
0
""
done

-- query 4
```

类型块以 `-- types<TAB>完整类型…` 开始，后接名称行和数据行。类型、名称与每行数据的列数相同，至少一列。类型保留 decimal 精度/标度、时间精度和 row 字段声明，如 `decimal(10, 2)`、`timestamp(6)`、`row("select" bigint)`。名称使用字符串规则。shell 块保存退出码和逐行 stdout，空输出行写 `""`；退出码与输出仍逐行严格比较。

第 0 列以 `-- ` 开头的行是元数据，只接受 `query`、`types`、`shell`、`legacy`；数据字符串以 `--` 开头时必须加引号。新增块或来源头属于显式格式扩展。

### 单元格与规范校验

| 类型或值 | 规范文本 |
|---|---|
| SQL NULL | `NULL`；字符串 `NULL` 写 `"NULL"`，空串写 `""` |
| boolean、整数 | `true/false`、十进制整数 |
| real、double | JDK 19+ `Float.toString/Double.toString` 规则，例如 `2.0`、`1.0E20`、`4.9E-324`、`-0.0`；非有限值写 `NaN/Infinity/-Infinity` |
| varchar、char、decimal、date、time、timestamp、json | 原字符串；按下面条件裸写或加 JSON 引号，时间精度保留 wire 原文 |
| varbinary | base64 文本，再使用同一字符串规则 |
| array、map、row | 紧凑 JSON；row 按位置写数组；map 键按 wire 文本转成字符串，并按键文本 UTF-8 字节序排序 |
| variant | 内联 JSON 原文，保留成员顺序；json 内容也不重排 |
| unknown | 只接受 `NULL` |

字符串仅在非空、不等于 `NULL`、不以 `"` 或 `--` 开头、不含 C0/DEL/反斜杠且没有尾部空白时裸写。前导空白可以裸写，因此 EXPLAIN 缩进保留。其余写 JSON 字符串：使用 `\"`、`\\`、`\b\f\n\r\t`，其余 C0 与 DEL 写大写十六进制 `\u00XX`；非 ASCII 和 `/` 原样保留。嵌套字符串总有引号，嵌套 SQL NULL 为 `null`，非有限浮点为 JSON 字符串。map 排序只是文件表示，不放宽比较。

runner 先展开 fixture/catalog 占位符，再按类型解析并重新渲染，要求逐字相同；额外空白、非规范转义、未排序 map、错误宽度和未知类型会明确失败。手工编辑后先运行所属 suite，不能靠 record 覆盖不一致。文本断言使用解码后的字符串内容，其余单元格使用规范文本；多个块按语句顺序连接。类型与名称严格相等，结果行继续沿用 `@order_sensitive`、无序默认和 `@float_epsilon` 的既有口径。

### 语料级会话声明

[corpus.toml](correctness/corpus.toml) 是语料输入，内容为：

```toml
[session]
time_zone = "Asia/Shanghai"
```

声明缺失或有未知键即启动失败。runner 每次请求发送该时区，客户端能力固定为 `PARAMETRIC_DATETIME,VARIANT`。每个用例持有独立 catalog/schema/属性，逐页累积会话变化，成功结束才应用；每步覆盖 `query_timeout`。FE 重启后保留会话事实并重建 HTTP 连接池。固定请求时区不宣称已经交付新的 SQL 时区语义。

[suite manifest](correctness/optimizer/suite.toml) 还可声明可选的初始会话输入，例如 optimizer 的 `[session] pipeline_dop = 5`。该正整数只影响声明它的 suite；缺省保留原默认，未知键和 0 明确拒绝。声明随原生 suite 输入及 hash 保留。Trino 将它写入每个用例的初始客户端属性，通过请求头发送，不插入隐藏的 SET；普通 SQL SET 可覆盖，重建 HTTP 连接池保留已经生效的值。历史 MySQL 基线适配曾读取同一版本化声明，当前 runner 仅使用 Trino。CLI 外加 `--target-session-sql` 仍属于 DIAGNOSTIC，不能代替这项语料输入。

### 旧格式块与转换

迁移暂时允许 `-- legacy` 块，其名称和数据仍是原 TSV 转义，内容不加类型、不猜测类型：

```text
-- query 6
-- legacy
old_value
3
```

旧块按迁移证据分为 L 与 U，均保留原 TSV 原文，不增加新的 golden 块种类：

- **L 无法确认。** 仍无法确认类型化结果的本语句有准确处置覆盖，或属于停用 suite；不能借整个 case 的已知失败覆盖其他语句。
- **TRC-2 D28精确迁移处置。** 仅封存71段自己的SQL拒绝按本轮L保原文，不新增运行时quarantine或case通过；五段尚无本步结果的旧答案单列本次待确认例外，不冒充R1/R2/L/U。仅spine q13另按真实列类型保原4行16格转写；与D26十二段一样保留错误查询结果/R_old拒绝和原FAIL。精确24项原SQL基线失败仅本轮按旧/新逐项不回退验收，rawFAIL/官方NEW及7项独立负例仍保留。新失败、步骤/错误边界变化、缺记录或清理未知不享例外。所有保留旧块仍由G10c及原owner清零，普通Verify成功到达Legacy仍失败。
- **U 基线不可达旧期望（TRC-2 D20）。** 仅限D20原945段及D27明确追加的210段封存候选，共1155段；候选不是准入。P00 本步 required 结果检查确未执行，且更早 primary 有当前活动的准确 unit/code 处置；当前真实普通 Verify 必须仍在该块前被同一更早已登记 primary 阻断，官方 matcher 实际匹配当前活动的准确 unit/code。记录 primary 序号及 SQL 来源，前序独立 oracle 无新增实际 FAIL，source/SQL/原期望/P10 旧 loader map、fixture/会话/预算的 hash 链及原 owner、cleanup、end 闭合均须逐段核实；授权 D12 输入变化另保准确 delta，不跨旧输入补证。三段 P00 已 PASS、11 段无 P00、一段无准确处置排除；`agg_test_groupby_array` q7 的首错 q3→q5 差异在解释清楚前拒绝。

U 只在迁移账中标 `NOT_EXECUTED`，保留 authoring 的完整实际结果及 `R_old` FAIL，不授本语句 PASS、R1、R2 或 L，也不改变 case PASS/FAIL/KNOWN。不能用 authoring 的更早 code 代替普通 Verify；前序独立 FAIL、Infra、owner 缺口或 primary/处置/输入前提变化使 U 不成立。未执行不是通过，不批准删除或重录。

正常 verify 到达旧块：语句失败沿现有错误及处置处理；语句成功则报“旧格式期望待转换”，包括原 U 候选，不能当作通过。U 不增加运行时 skip、fallback 或比较模式。旧块、旧渲染 `R_old` 和旧块 reader 最迟由 G10c（全语料参照换源与旧期望清零）删除；不可移植残余也必须由 owner 复核为带类型 snapshot 或随处置删除，U 不延长寿命或改变 owner 责任。

转换模式用实际类型化结果经过 `R_old`，只有在原排序/容差政策下等于旧期望时才生成类型块，保留旧行序和占位符。新增类型与 NULL/字符串消歧信息属于目标端 `snapshot`，不是独立 oracle。转换可越过同一用例中的语句失败继续取类型；harness、集群和传输失败仍停止该用例。普通 record 与受旧期望约束的转换是不同操作；不得直接重录来处理 mismatch。

下列命令使用前文已绑定的 `$sql_runner`、`NOVAROCKS_BIN` 和 fixture 配置，分别执行受旧期望约束的转换、普通 verify 和只读旧块清单。转换报告不等于普通 verify 通过；仍有旧块或其他失败时，保留原始结果并逐条核对，不能以转换覆盖判定。

```bash
# Export baseline_variant_offset_seconds from the recorded baseline UTC offset.
"$sql_runner" --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite join \
  --mode record --convert-legacy \
  --legacy-variant-offset-seconds "$baseline_variant_offset_seconds" \
  --cluster-mode cross-process --cluster-size 3 -j 1

"$sql_runner" --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite join \
  --mode verify --cluster-mode cross-process --cluster-size 3 -j 1

"$sql_runner" --suite all --list-legacy-goldens
```

`--legacy-variant-offset-seconds <i32>` 在转换时必填。值来自基线主机的实际记录，不能从语料请求时区推导。基线、转换与判定核对在同一台具备完整 fixture、READY 数据集和足够空间的 macOS 或 Linux 验收主机上执行；保留各次源码、程序自报、fixture、实际主机和原始结果。偏移必须取自该主机的实际基线记录，不能用当前默认偏移补造基线事实。列出命令每行输出一个旧块 JSON，包含 suite、case、step 和块序号；停用 suite 的残余同样要纳入最终清单。每块的等价核对、处置覆盖、协议边界改写和新旧判定都保存在本次独占尝试中，原始失败与后续复跑分别保留。

## 开发选择与逐 case 证据

开发阶段用 `tools/ci/dev-verify.sh plan|run|resume|compose` 消费原生声明和已准备产物，见 [层规则与入口](../../docs/guides/development/test-governance.md#三层验证与证据组合)。本地完整入口的 SQL 集合来自原生默认发现，已知失败和不稳定在 [dispositions.json](../../tools/ci/baselines/dispositions.json) 中显式处置。`explicit_only` 表示重项；它与暂停、不稳定及已知失败是不同状态。相关重项仍是作者义务。正式准入与合入后全量由 G17 运行，本地结果不能自行成为正式来源。

加 `--results-file /absolute/local/new-attempt/sql.results.jsonl` 写出独占 schema 1 JSONL。开始前写 run 和全部 selection（含加载失败项），逐个 case 终结后立即追加 case，suite 写 init/cleanup/输入确认，最终 end 写 Server/fixture 收束与完成事实。原 SQL golden、断言、原始日志和退出码保持权威；case 记录包含声明与实际执行的 oracle、断言策略、有效 verify 开关、输入摘要、失败特征与快照引用。缺 end、坏记录、缺 case 终态和未验证不能补成通过。

case的`diagnostics`独立保存publication故障中原查询的错误、部分执行和保留状态，不进入`oracle.required/executed`。保留诊断不授予正确性断言或完整`R_old`输入资格；真实保留失败仍使原case失败，不能靠诊断字段伪造PASS。

共享生命周期默认以整个 suite 为复用范围，只有原生 owner 声明 case 范围及 `isolated/reset/immutable` 初始状态后开发工具才可拆 case；当前 corpus 仍以 suite 为范围。直接 `--only` 可用于定位，它不能自动得到可复用的独立范围。`--no-verify`、record/update、自定义 epsilon、关闭 oracle 或改变断言策略的结果保留为诊断，不满足默认验证义务。结束确认输入改变、旧失败、新失败、取消和清理违约分别保留；一次定向通过不能覆盖旧反例。

## 受管进程与原始双流

cluster-harness 继续拥有 FE/BE 的应用生命周期、原生 1FE+NBE 拓扑、readiness、准确授权与 restart；共享 test-support 的独立 monitor 拥有具体 OS Child、进程组信号与实际 wait。runner、IDE 或外层测试异常退出后，monitor 仍履行失联回收；失联和清理错误继续保留，不因最终未发现残留进程变成通过。普通成功退出、原始断言和日志检查完成后才释放诊断，正式材料仍由外层正式入口保留。

角色的输出定位文件指向 run-owned `stdout.log`、`stderr.log` 和监护记录，不能把 locator 当 combined log。runner/harness 通过原始 source handle 读取，失败材料关联真实位置；实时完整 UTF-8 行诊断上限为 1 MiB，超限明确失败并保留原始字节，不截断完整 SQL golden 输出。snapshot 拒绝替换/截短的日志文件。完整 capture 消费原期限，timeout/复制失败后的原始双流保留在 `diagnostic_root`。

受原查询期限约束的 shell 走 `ManagedCommand`/capture；共享运行中未单独指定query期限的shell仍受原fixture预算；没有fixture预算的私有路径由原owner运行。Docker/fixture owner、stdin 协议 helper、listing observer 和专用 fencing helper 也不被这份共享接线自动接管。嵌在 nextest 中首次启动 runner 前，外层测试必须先 `ensure_run_retention(deadline)` 并保留继承上下文；直接运行 SQL CLI 无需伪造 nextest 身份。普通保留默认最近三次已收束调用；活动和未完成材料不淘汰，正式材料不受该窗口影响。

容器运行状态根及正式证据根需 bind-mount 支持的本地文件系统；overlay 不作锁语义 fallback。详细 API、保留/释放责任、macOS 排查与 native Linux x86_64 尚未补齐的验证见 [test-support README](../test-support/README.md)。规范 Rust 的 nextest 与正式原始结果接口见 [CI README](../../tools/ci/README.md)；这些机械验证不替代本 suite 的真实 fixture 和 native 1FE+3BE 验收。

## SSB/TPC 数据正确性

`ssb`、`tpc-h`、`tpc-ds` 与其他 suite 共用正常 case discovery、suite init/cleanup、结果比对和 server cleanup；分别消费 scale `1`、`1`、`1GB` 的共享 READY 数据。仅以完整 suite manifest 的 `prepared_dataset` 绑定输入，不读取旧测量 scenario/window 定义。

输入 owner 先显式准备。下例变量需要替换成仓库外独占尝试目录和真实 suite/scale，不在 verify 的错误恢复路径自动供给：

```bash
python3 tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py \
  --suite "$suite" --scale "$scale" --shared-root "$NOVA_ENV_SHARED_BENCHMARK_ROOT" > "$attempt/resolved-dataset.json"
tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh \
  --suite "$suite" --scale "$scale" --resolved-dataset "$attempt/resolved-dataset.json" --ensure
```

同次 publication、真实 Spark image/BOM、schema与对象布局由既有输入 owner 核对。消费者只运行 `--check`，核对 READY 的 dataset key、manifest URI、publication identity/ETag及实际 warehouse；缺失、失效或不匹配即非零退出，不启动集群或重写数据。新对象存储需重新显式供给，共享 READY 不属于 worktree purge。

```bash
"$sql_runner" \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite ssb,tpc-h,tpc-ds --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 --cluster-launch-profile performance
```

`performance` 是 clean harness 启动配置，支持 release binary、拒绝 fault 注入；本命令不测吞吐或延迟分位数。三套高成本 suite 是 explicit-only，不扩默认 CI。runner/输入绑定变更必跑三套；领域 SQL 变更按相关性选择；维护者每周手工完整轮次，自动调度尚未交付。邻近说明见 [SSB](correctness/ssb/README.md)、[TPC-H](correctness/tpc-h/README.md)、[TPC-DS](correctness/tpc-ds/README.md)。

## 失败、材料与退出

语句失败与 mismatch 非零退出，原始 FAIL 保留。未传 `--write-actual-dir` 也由失败材料根保存 case/expected/actual/diff；默认根为 `logs/sql-test-failures`，可用 `--failure-artifact-dir` 或现有环境变量选择真实独占材料位置。FE/BE 日志、suite hook错误、primary error和cleanup error分别记录，cleanup失败不能被结果 PASS 覆盖。每次重跑换新材料根，不自动复跑或删除旧失败。

suite init失败仍执行所属cleanup一次；case失败不跳过正常cleanup；runner拥有实际cluster shutdown和残留进程核对。执行成功、SQL ERR、取消、owner排空、进程退出是不同事实。定向通过不覆盖全量 raw FAIL，批准的本次例外与运行结果分别表达。
