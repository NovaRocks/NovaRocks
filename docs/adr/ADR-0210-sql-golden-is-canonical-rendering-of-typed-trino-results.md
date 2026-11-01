---
id: ADR-0210
title: "SQL goldens are canonical renderings of typed Trino results"
domain: [sql-test-oracles]
status: active
supersedes: []
superseded-by: null
date: 2026-10-29
implementation-status: implemented
provenance:
  - "discussion: 2026-10-29 typed SQL expectations, old-oracle-preserving migration and explicit client sessions"
  - "reference: ADR-0203 frozen Trino 483 client presentation"
code-anchors:
  - "tests/trino-client/src/value.rs (TypeSignature, TrinoValue, StatementResult)"
  - "tests/trino-client/src/decode.rs (decode_value, decode_map_key)"
  - "tests/trino-client/src/session.rs (ClientSession, SessionEffects)"
  - "tests/sql/runner/library/src/suite_manifest.rs (SuiteSessionDefaults)"
  - "tests/sql/runner/library/src/session.rs (TrinoCaseSession)"
  - "tests/sql/runner/library/src/golden/grammar.rs (GoldenFile, Section, Block)"
  - "tests/sql/runner/library/src/golden/cell.rs (render_cell, parse_cell)"
  - "tests/sql/runner/library/src/golden/trino_type.rs (parse_type)"
  - "tests/sql/runner/library/src/golden/legacy.rs (old_render_cell, old_equivalent)"
---

## 问题

SQL 正确性期望怎样同时保留精确类型、无歧义的值和可追溯的来源，并在切换客户端协议时证明旧期望没有被实际结果悄悄替换？

## 背景与执行事实

SQL 文本协议的显示字符串不等于 SQL 值。SQL NULL 与字符串 `NULL`、空串与空行、整数与布尔值、带精度时间与省略小数的时间，都可能在无类型 TSV 中合并成同一种写法。客户端显示版式还会随协议和客户端版本改变。

[ADR-0203](ADR-0203-client-result-presentation-and-trino-paged-delivery.md) 已冻结 Trino 483 的列类型与 JSON 呈现。测试侧分开承担以下责任：

| owner | 输入和职责 | 不授予的结论 |
|---|---|---|
| 共享 Trino 测试客户端 | 从列签名和原始 JSON 单元格解码值，逐页累积会话变化，取得结束页 | 不判断 golden 正确性，不重试，不按客户端显示文本恢复值 |
| `TypeSignature`、`TrinoValue` | 整数、浮点、字符串、二进制、数组、按位置的 row、保留 wire 顺序的 map 和原文 Variant | map 的 wire 顺序不是 SQL 相等语义；类型信息本身不证明来源独立 |
| golden 模块 | 按类型渲染、反向解析并检查规范形式，保存 section 和块 | 规范化不改变查询排序或浮点容差政策 |
| 旧渲染 `R_old` | 将类型化实际结果写成原 MySQL 编码器及 TSV 的旧文本，按原比较政策核对 | 旧期望渲染等价不证明新增类型和消歧信息来自独立 oracle |
| runner | 拥有客户端会话、比较政策、步骤断言和迁移处置 | 客户端连接池不是会话事实或预期来源 |

数字从 JSON 原文直接交给目标宽度的解析器；map 对象通过 visitor 按文档顺序收集。普通 `serde_json::Value` 会改变对象顺序，其默认浮点解析也不能作为正确舍入的保证，因此不承载类型化结果。golden 的浮点期望以 JDK 19+ 生成的样本表独立核对，不能由被测渲染器生成。

本条记录已接受的设计。源码模块和局部检查不代表全语料转换、macOS 或 Linux 原生 1FE+3BE 判定核对或最终全量验证已经完成。

Trino 呈现延续原结果的资源 owner。按 [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)，EXPLAIN ANALYZE 的 Internal 结果保留同 Root 的数据预算与 alias；metadata、page、重放和在途 body 沿该原件保管。adapter 通过原 alias 的 `payload_capacity_bytes()` 读取已配置容量，继续核对 source、metadata 和 page 的完整重叠，不能要求 Internal 结果取得另一份 Window position，也不能从身份或缺省值重建容量。

## 考虑过的选项

| 方案 | 吸引力 | 裁决 |
|---|---|---|
| 保留 StarRocks 风格的无类型 TSV 与嵌套私有文本 | 旧文件改动少 | **设计否决。** SQL NULL、字符串和行边界有歧义，无法提供按类型可逆的持久期望 |
| 保存 Trino CLI 显示文本 | 容易阅读和获取 | **设计否决。** 嵌套字符串的显示没有充分引号，且版式依赖 CLI 版本 |
| 每列只记录单字母或不含参数的基本类型 | 文件更短 | **设计否决。** 丢失 decimal 精度、标度、时间精度及 row 字段声明 |
| 经过 `serde_json::Value`，或为它统一启用排序、浮点特性 | 使用通用 JSON API | **设计否决。** 会改变 wire 顺序或浮点值；为测试改变工作区 JSON 特性还会影响产品构建 |
| 切换协议后直接从目标端重录全部 golden | 实现简单 | **设计否决。** 会把不一致结果写成正确答案，原本失败的语句也无法证明旧期望 |
| 保留旧的逐步参照与 diff 模式，只替换传输 | 复用既有选项与执行分支 | **设计否决。** 整脚本重放包含目标专有准备语句，不能提供独立命名空间、步骤分类和两次重放一致的参照合同 |
| 类型化值、规范渲染及以旧期望为准的迁移 | 类型可逆、显示稳定且能核对来源变化 | **采用。** 接受独立客户端解码、渲染器和临时旧渲染的维护责任 |

## 裁决

1. **原文解码规则。** 共享测试客户端保持零产品依赖。每个 JSON 单元格先以 `RawValue` 保存，再按列签名解码；浮点以 `str::parse::<f32/f64>()` 正确舍入，map 保存原条目顺序，row 保存位置，Variant 保存内联 JSON 原文。禁止借用产品编码器作为 golden 的独立 oracle。
2. **类型化文件规则。** 文件总是以 `-- query N` 标识递增、不重复的步骤；每个 section 包含零个或多个块。类型块为 `-- types<TAB>类型…`、名称行和数据行，类型完整保存列声明，至少一列，名称和各行宽度与类型数相等。多条返回行的语句各写一个块。shell 块为 `-- shell`、退出码和逐行输出。迁移块为 `-- legacy` 加旧 TSV 原文。没有返回行的步骤保留空 section。第 0 列的 `-- ` 元数据仅接受 `query/types/shell/legacy`；新增关键字属于格式修订。
3. **标量规范规则。** SQL NULL 写 `NULL`；布尔写 `true/false`，整数写十进制。浮点按 JDK 19+ 的 `Float.toString/Double.toString`，保留 `-0.0`，非有限值为 `NaN/Infinity/-Infinity`。字符串值类型保留原文，二进制使用 base64。字符串只有在非空、不等于 `NULL`、不以双引号或 `--` 开头、不含 C0/DEL/反斜杠且无尾部空白时裸写，否则写 JSON 字符串。名称与 shell 输出同样适用。转义使用 `\"`、`\\`、`\b\f\n\r\t`；其余 C0 和 DEL 使用大写十六进制 `\u00XX`，非 ASCII 与 `/` 原样写出。
4. **嵌套规范规则。** array、map、row 写紧凑 JSON；字符串始终有引号，SQL NULL 为 `null`，非有限浮点为 JSON 字符串。row 按字段位置写数组；map 键以原类型的 wire `toString` 文本写字符串，条目按键文本的 UTF-8 字节序排序。json 文本与 Variant 成员顺序不重排。map 排序只规定文件表示，不改变结果比较政策。
5. **可逆读取规则。** 占位符先由 runner 展开，再按类型解析单元格并重新渲染；结果必须逐字等于原文件，否则明确拒绝。字面量 `NULL` 永远是 SQL NULL；字符串 `NULL` 与空串必须加引号。类型解析与单元格解析由 golden 模块统一拥有，后续比较器及参照录制复用这一个作者。
6. **旧期望优先规则。** 协议迁移只有在类型化实际结果经过 `R_old` 后，按原顺序与容差政策等于旧期望时，才采用实际结果生成类型块。无序结果沿旧期望的行序匹配并保留占位符。新增的类型及原歧义的消解均标为 `snapshot` 来源；不能冒充独立答案。原有 mismatch 处置覆盖的转写单独复核旧值和新摘要，协议边界改写分别保存等值证据。
7. **临时旧块规则。** 为确认类型，转换可越过同一用例内的语句失败继续执行；集群、harness、传输等基础设施失败仍停止该用例。L 类仍要求本语句无法确认且有准确处置覆盖，或 suite 停用。D20 另允许逐段证明的 U 类保留基线不可达旧期望：P00 本步 required 结果检查未执行且更早 primary 有当前活动的准确 unit/code 处置，当前真实普通 Verify 仍在该块前由同一更早已登记 primary 阻断，官方 matcher 实际匹配当前活动的同 unit/code，primary 序号及 SQL 来源明确，前序独立 oracle 无新增实际 FAIL，输入/hash 链和原 owner、cleanup、end 全部完整。首轮只限封存的945待证段，D27仅追加封存210段、合计1155，追加时初始准入0；三段 P00 已 PASS、11 段无 P00、一段无准确处置排除，`agg_test_groupby_array` q7 首错 q3→q5 在解释清楚前拒绝。保存原 SQL/期望/P10 旧 loader map 与当前 fixture、会话、预算来源；后续授权输入变更必须绑定准确 delta。U 只在迁移账标 `NOT_EXECUTED`，保留 Legacy 与 authoring `R_old` FAIL，不授本语句 PASS/R1/R2/L，不改变 case 判定，也不新增 golden 语法、运行时 skip/fallback 或比较模式。verify 到达旧块而语句成功时仍明确判“旧格式期望待转换”，U 候选亦同。旧块、`R_old` 与旧块读取代码最迟由 G10c（全语料参照换源与旧期望清零）删除：可移植语句换成参照两次重放的一致答案，不可移植残余由 owner 复核为类型化 `snapshot` 或随处置删除；U 不批准删除、重录或延期。列出旧块的命令公开剩余清单。
8. **客户端会话规则。** 语料声明时区，suite 可声明版本化的正整数初始 `pipeline_dop`；缺省保留原默认，未知键和 0 明确拒绝。Trino 将初始属性放入客户端会话并通过请求头发送，不合成 SET；普通 SQL SET 可覆盖，重建 HTTP 池保留已经生效的值。旧 MySQL 基线适配与 Trino runner 读取同一声明，CLI 外加会话 SQL 不因此获得默认验证资格。runner 每用例持有独立 catalog/schema、属性及时区；会话变化从每一页响应累积，只在语句成功后应用，未实现的会话变化头明确报错。每步覆盖 `query_timeout`，每次请求发送语料声明时区，初始声明为 `Asia/Shanghai`；能力为 `PARAMETRIC_DATETIME,VARIANT`。最后一个数据页后继续拉取至结束页。FE 重启后重建 HTTP 连接池，保留客户端会话事实；传输失败不自动重试，取消通过当前 `nextUri` 的 `DELETE`。
9. **判定与来源分离规则。** 类型和列名严格相等；结果行继续使用原有无序默认、顺序声明和浮点容差。shell 的退出码与输出逐行相等。文本断言使用名称与数据的内容形式，多个块按语句顺序连接。类型行和规范行参与新的结果摘要；独立参照及新的比较政策必须另有自己的来源、重放和验收合同，不能从文件规范化推得。

### TRC-2 D28的准确迁移例外

D28仅对封存71个本步SQL拒绝旧答案授本轮迁移L处置，五个无本步结果旧答案另列待确认例外，不能称为R1/R2/L/U或步骤PASS；spine q13只依实际类型保原4行16格补类型，错误实际结果、R_old拒绝和原FAIL保持。精确24项旧SQL基线失败仅本轮按不回退验收，原始FAIL、官方NEW与独立负例均保留，不添加运行时quarantine。新失败/错误步骤或边界变化、缺记录、清理未知不享例外。全部旧块仍沿原G10c寿命和成功到达时失败的合同；这不是可泛化的宽松比较规则。

publication故障的原查询诊断由case独立`diagnostics`保管，不能混入已执行正确性oracle；部分执行不冒充完整R_old输入，诊断保留失败仍按原行为失败。

## 接受的妥协（诚实记录）

完整类型、嵌套 JSON 与必要引号增加文件长度，row 字段名只出现在类型行。代价换来 NULL、空串、字符串、类型参数和行边界的可逆表示；保留无歧义的前导空白则让 EXPLAIN 缩进仍可读。

规范文本不统一 json 语义，不放宽浮点比较，也不自动判断查询是否有稳定排序。这些是独立的比较政策，暂时沿用旧口径，避免协议迁移同时改变正确性判据。

旧期望本来没有类型，也可能把 NULL 和字符串合并。渲染等价只能证明没有改变原文本判据；补上的类型和消歧仍依赖目标端观察，必须承认 `snapshot` 来源。失败语句暂留旧块，使迁移不删除独立预期，同时增加明确有终点的双表示负担。转换越过语句失败取得的后续结果只用于等价核对，不能用来证明正常 verify 已通过。 D20 的 U 保留 P00 与当前普通 Verify 均未到达的旧期望，承认尚未验证，而不把 authoring 的完整结果或 `R_old` 拒绝改称成功。原945及D27精确追加210、共1155候选在各自批准时初始准入均为0；每段真实收据另行核实，未执行不是 PASS，旧块与原 owner 的 G10c 清零责任不变。

共享客户端增加独立的类型解码与会话实现，未实现的 session header 会让语句失败；这是防止未来参照会话悄悄偏离的代价。显式时区只固定请求事实，其 SQL 时间语义仍由产品时间语义 owner 定义。

## 何时重新评估

- Trino 呈现词汇增加类型、能力或时间精度时，同时修订客户端解码、golden 类型解析、规范文本与独立样本，不通过默认字符串回退扩展支持。
- 需要事务、预编译、path、role 或授权用户变化时，在共享客户端逐项实现其会话语义，再解除相应响应头的明确拒绝。
- 引入来源头、受影响行数块、错误块、类型化浮点比较或无序规范行序时，以格式与比较政策的独立裁决扩展；不能静默赋予旧文件新来源。
- 全语料参照换源与旧期望清零阶段到达时，按 owner 逐块清零并删除 `R_old` 和旧块 reader，不能因为残余旧块仍能读取而续期。
- U 候选若被普通 Verify 到达、primary/活动处置/输入前提改变，或出现新的前序独立 FAIL、Infra、原 owner 闭合缺口，立即按原失败及待转换规则处理，不能借旧 U 收据续用。扩展候选集合或改变 U 条件须另作明确设计裁决；不能用 authoring、其他用例的处置或 missing oracle 补造通过。
- 迁移交叉核对发现旧渲染与原 MySQL 编码器存在系统性分歧时，先修复核对作者；需要改变独立 oracle 的实现路线时，重新评估保留证据及依赖边界，不把不一致降为已接受的旧块。
