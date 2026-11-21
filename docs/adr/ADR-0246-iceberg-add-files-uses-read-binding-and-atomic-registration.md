---
id: ADR-0246
title: "Iceberg ADD FILES uses read binding and atomic registration"
domain: [provider-spi, frontend-dml]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0024
    scope: "ADD FILES source manifest, field identity, fixed limits and re-planning per attempt"
    link: "ADR-0024-connector-data-mutation-contract.md"
  - id: ADR-0110
    scope: "Refreshing the base and repeating ADD FILES duplicate validation on every attempt"
    link: "ADR-0110-lake-publication-crash-only-contract.md"
  - id: ADR-0171
    scope: "Section 1 ADD FILES candidate dependency and its validation at the exact main reference"
    link: "ADR-0171-commit-operation-model.md"
  - id: ADR-0239
    scope: "Rule 5 for coverage of registered files that have no receiving creation receipt"
    link: "ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md"
date: 2026-11-20
implementation_status: implemented
verification_status: scoped-tracked-separately
provenance:
  - "decision: RWM-17c spec revision 3 and implementation plan revision 2 approved on 2026-11-20"
  - "decision: INT96 footer counters follow Trino 484 where applicable, while read admission stays unchanged"
  - "reference: Trino 484 e2d50bb733e193607c44be84ad80ce2430ba1d01"
  - "reference: Apache Iceberg Java 1.12.0 212aa894e919ff9fdf89160da4aadcbe6858ac01"
code-anchors:
  - "novarocks/connector/iceberg/src/catalog_config.rs (parse_add_files_enabled)"
  - "novarocks/connector/iceberg/src/catalog_control/data_mutation.rs (validate_registration_target)"
  - "novarocks/connector/iceberg/src/catalog_control/add_files.rs (RegistrationPreparation)"
  - "novarocks/connector/iceberg/src/typed_read/schema_binding/registration.rs (preview_registration)"
  - "novarocks/connector/iceberg/src/file_reader/field_identity.rs (resolve_parquet_fields)"
  - "novarocks/connector/iceberg/src/parquet_metrics.rs (analyze_footer_metrics)"
  - "novarocks/connector/iceberg/src/catalog_control/data_mutation/publication/registration_checks.rs (RegistrationChecks)"
  - "novarocks/connector/iceberg/src/hadoop_catalog/publication.rs (native conditional installation)"
  - "novarocks/connector/iceberg/src/commit/new_manifests/registered.rs (RegisteredNewFiles)"
  - "novarocks/connector/iceberg/src/typed_read/split_source.rs (data_file_affinity_key)"
  - "novarocks/frontend-application/src/dml/add_files.rs (registration_byte_author)"
---

## 问题

`ALTER TABLE … EXECUTE add_files` 怎样把已有 Parquet 数据登记为普通 Iceberg append，同时保证文件可读、统计指向正确字段，并让同路径登记及名称映射变更在 Catalog 并发下得到确定结论？

## 背景与考虑过的选项

ADD FILES 由 FE 原语句执行。外部文件已经存在，BE 及写会话未创建它，因此登记没有文件生产回执或源文件删除权。读写侧的物理行身份使用文件路径与行位置。同一存活集合里重复登记路径会破坏这个身份。

- **规划冻结完整清单，每轮再列举。** 这个方案需要跨规划与执行保管全部文件事实，还把目录变化与 Catalog 冲突混在一起。目录的实际内容可由执行一次冻结，提交重试只重新核对目标的新事实。
- **沿用目录、footer 与文件数的独立固定额度。** 这些额度与语句已保留的登记条目及实际分配峰值无关。存量扫描载体和 FS inspection 已各有自己的边界。
- **沿用查询外的全局登记锁，或先发布名称映射再追加。** 进程内锁覆盖不了其他 Catalog 客户端。拆成两个请求会产生只设置映射的可见效果，也会在两者间留下映射被修改的窗口。
- **按 Trino 原样登记后由读取报错。** 旧文件可能可以提交却无法查询，甚至因字段重排读出错误值。共享读取绑定可以在写出 manifest 之前复用实际支持的类型与字段身份。

## 裁决

### SQL 准入与执行输入

`iceberg.add-files-procedure.enabled` 是 catalog 的字符串布尔属性，默认 `false`。Hive Metastore catalog 保持只读，MV 存储表保留原准入守卫。规划检查开关、必填 `location` 和 `format`、目标 v1/v2 及 Parquet 能力，保留目标 UUID 与已解析的参数。`ORC`、`AVRO` 和规划时已是 v3 的目标返回 `NOT_SUPPORTED`。执行加载同 UUID 的表后检查当前 spec 为非分区，提交 `main`。格式版本能力属于规划判定，期间兼容升级沿普通 append 的发布规则处理。

执行先探测目录，再尝试精确的单文件。目录中的候选按 URI 字典序处理。相对路径内任一层以 `.` 或 `_` 开头的条目及目录标记跳过。`recursive_directory` 取 `TRUE`、`FALSE` 或默认 `FAIL`，分别登记、跳过或因可见子目录失败。单文件直接登记，文件名后缀不参与筛选。对象存储前缀没有任何对象时返回 `NOT_FOUND`。已有目录只包含被跳过的条目时，提交一次空 append 快照。

`location` 的末尾斜杠在目录处理中归一化，顶层 `_SUCCESS` 等隐藏条目仍会跳过。Trino 484 在该形态可能把同一条目当数据文件尝试读取。NovaRocks 接受合法路径中的 `@`，凭据类 URI 成分在派发前拒绝。登记源可以在表目录或 warehouse 内部。成功结果含 `metric_name varchar`、`metric_value bigint`，数据行是 `('added_data_files', N)`，Trino update type 为 `ALTER TABLE EXECUTE`。

### 共享读取与统计

普通、历史和变更扫描与登记预演沿同一 Parquet 解释政策，使用原 footer 与生效名称映射。完整的嵌入式 ID 按 ID 匹配，struct 子字段按目标的字段 ID 对齐。原文件部分具备 ID 时拒绝。无 ID 文件使用现有 `schema.name-mapping.default`，缺省时从当前 schema 生成。未匹配的额外列被忽略，空别名与无 ID 映射项有其 Iceberg 语义。列表及映射组件按逻辑 `element`、`key`、`value` 解析，旧版 Parquet 的物理元素名不改变该逻辑身份。

嵌套重建保留父层有效位。缺失字段按初始缺省、可选 NULL 或必填拒绝处理。登记还逐文件核对顶层必填列的 NULL 计数，缺失或非零均返回 `CONSTRAINT_VIOLATION`。类型不兼容返回 `TYPE_MISMATCH`，绑定的读取载体尚无支持时返回 `NOT_SUPPORTED`。其中包括目标列绑定到 INT96，不能凭无界值通过读取准入。

footer 统计使用与预演相同的叶子身份和生效映射。通常依 Iceberg Java 1.12.0 的缺省 `truncate(16)` 规则，重复容器内的叶子只记录列大小，缺失的 NULL 计数保持未知。可能包含数据的行组没有可信界值时省略整叶界值。非法 UTF-8、无法证明的全 NaN 行组及旧式二进制极值不会产生不安全界值。INT96 指标依 Trino 484 的实测裁定。有 ID 的叶子保留大小，非重复叶子保留值计数，所有行组都提供 NULL 计数时才记录总数，始终省略上下界。这适用于可登记文件中未绑定到目标的 INT96 叶子。

### 登记依赖与发布

执行取得的第一个准确表版本记为 E，发布每轮重新加载的准确基准记为 B。E 上核对本批候选的唯一性及它们在 `main` 的存活情况。每轮核对 B 的表 UUID 与该批文件使用的 partition spec。`main` 仍指向最近已核对的相同快照时，沿用候选未存活的证据。指向不同快照时，在 B 的存活数据 manifest 重新匹配本批候选，同一个 B 的 ref 条件保护最终追加。路径命中即失败。多个存活条目引用同一候选也足以否定登记资格，行级精确身份查询仍按其契约将这类重复视为损坏。

并发登记同一路径时，有效 Catalog 条件保证只有首个成功请求按其基准推进 `main`。另一个请求收到确定冲突后刷新，若路径仍存活则报 `ICEBERG_COMMIT_ERROR`。不同路径的普通追加可以重试并复用 operation 已写的新增 manifest。只对确定竞争使用 `commit.retry.*`，重试用尽报 `TRANSACTION_CONFLICT`。

无 ID 文件的读值和统计均依赖实际使用的映射。登记保留每条逻辑路径与其解析结果，包括未匹配的结果。在 B 上允许其他作者添加与这些结果等价的字段或别名，保留其新属性。已用绑定变化、原有映射被移除，或未匹配路径在 B 获得新的解释时，确定拒绝。E 与 B 都没有映射时，从 B 的 schema 生成，核对同样的使用事实，再把映射和 append 放进一个请求。有 ID 的文件和空批次无需写映射。

依赖名称映射时，即使 `main` 未变也要比较整版 metadata。只有支持准确版本比较与条件安装的 Hadoop catalog 路径准入。标准 REST 可登记有 ID 的文件，已有映射也不能让无 ID 文件通过其登记准入。REST 恢复这类登记须提供同一请求内可验证的 metadata 基准条件与原子安装，覆盖并发改写映射而 `main` 未变的场景。已有名称映射的正常读路径依 Iceberg 规则继续可用。

### 条目保管、容量与清理

所有候选完成校验后移交同一个 `IcebergCommitOperation`。登记来源持有受检的 `RegisteredFile` 序列。新增 manifest stage 按估算的编码大小顺序消费条目，实际 descriptor 校准后续滚动。首次写出核对每条输入恰好消费一次及 ADDED 计数，发布轮再核对新增 manifest 被直接保留或由合并产物覆盖。接收写入自己的创建回执、对象覆盖及删除权继续按 [Iceberg ADR-0239](ADR-0239-iceberg-compact-receiving-and-rolling-manifests.md) 执行。

FE 从原结果窗口派生登记字节能力。候选、路径 backing、映射使用事实、已登记条目和 Provider 控制的临时副本在分配前取得同一预算的回执。构造 `DataFile` 时，SDK builder 与建成值并存的已知峰值也需准入。回执随实际 backing 和最后一个共享别名退出，manifest 滚动估算只控制布局。额度不足时及时返回类型化资源失败，避免语句等待自己释放数据。FS inspection 沿既有 footer、物理列、行组和统计值限制。底层 FS/SDK 内部短期缓冲由各自的 I/O 准入和边界管理。

路径及待写映射在登记前复用扫描作者的线路核对。扫描载体继续发送完整数据文件 URI，单路径当前上限为 16 KiB。普通与变更扫描都用完整 URI 的 SHA-256 生成短亲和键，放置键不参与文件身份判断。ADD FILES 自设的文件数、路径和 footer 固定额度已删除。

确定未提交时，operation 只清理自己生成且可删除的元数据。未知结果保留可能被引用的产物，并在同一语句用原 snapshot marker 作一次核查。能证实成功时返回成功，仍未知则报告 `ICEBERG_COMMIT_ERROR` 和不可自动重试的结论。登记期间的源文件删除权不随这些状态变化。

## 调用方约束与排查

- 发布前保持所提供文件的位置、字节和可访问性稳定。名称映射安装后，后续更改仍须保持 Iceberg 读约定，不可用属性覆盖去重新解释已有数据。
- 成功后，登记文件进入表的数据生命周期。文件即使位于表目录之外，在最后一份引用快照过期后也可能按表维护规则删除。登记操作的失败清理只能处理它生成的元数据。
- 表目录内的尚未登记文件可能已满足扫描式 `remove_orphan_files` 的年龄条件。应协调同一前缀的外部或未来获准的孤儿文件清理，或者在表目录之外准备输入。年龄门槛无法保护等待第一次发布的文件。
- 开关、表版本、递归、列身份与 Catalog 准入可按[用户指南](../guides/iceberg-v3/dml.md#登记已有-parquet-文件)逐项核对。成功后用 `$snapshots` 与 `$files` 查看真实快照、路径与指标。未知结果按[发布核对流程](../guides/iceberg-v3/lake-publication.md#不确定提交)读取证据。

## 何时重新评估

标准 REST 提供可核验的整版条件安装时，重新评估无 ID 登记。共享 reader 支持 INT96 或其它当前拒绝的物理载体时，独立验证物理解码、范围、字段绑定和实际读回，再更新登记准入。若导入规模经常耗尽原语句预算，按实际保留布局和对象存取证据评估溢写与原子可见性，保持 operation 的源文件与自产对象责任明确。
