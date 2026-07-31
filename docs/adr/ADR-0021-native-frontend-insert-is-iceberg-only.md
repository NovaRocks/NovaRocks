---
id: ADR-0021
title: "Native frontend INSERT is Iceberg-only"
domain: [frontend-dml]
status: active
supersedes: [ADR-0019]
superseded-by: null
date: 2026-07-31
provenance:
  - "PR: compat wire visibility boundary and standalone StarRocks table subsystem removal #780"
  - "PR: frontend INSERT application ownership #792"
  - "discussion: 2026-07-31 post-cutover capability audit"
code-anchors:
  - "novarocks/core/src/engine/insert_engine.rs (InsertEngine)"
  - "novarocks/frontend/src/dml/insert/mod.rs (DmlService::try_execute_insert)"
---

## 问题

standalone StarRocks 表子系统已被删除后，native frontend 的 INSERT application flow 应继续宣称支持哪些持久化
target，compat StarRocks INSERT 又应如何与 native capability 隔离？

## 背景与执行事实

PR #780 删除了 core 中 feature-only 的 standalone StarRocks table catalog、DDL、DML 与 transaction/publish
subsystem。保留下来的 `StarRocksTableSinkProgram` / `OlapTableSinkFactory` 是 BE 执行内核：sink program 由 compat
Thrift decoder 根据外部 StarRocks FE plan 构造。native plan protocol 没有 StarRocks data sink，native frontend
也不拥有 index、partition、tablet location、load id 或 transaction 等写计划必需的元数据。

native SQL parser 的 `CreateTableKind` 只有 Iceberg；`backend_resolver` 明确拒绝把 `default_catalog` 当作用户持久表
catalog，并把用户表操作解析到外部 Iceberg catalog。所谓 Local target 没有 production table owner 或真实 sink。
PR #792 一度保留的 Local/StarRocks `InsertTargetBackend`、append port 与 fake-engine 测试因此不是可用 capability：
Local 分支会查找从未注册的 `local` sink，StarRocks 分支则通过未声明的 core feature 隐藏了对已删除 primitive 的
引用。

compat 模式没有 native standalone MySQL 入口。客户端连接外部 StarRocks FE；FE 生成完整 StarRocks sink plan，compat
adapter 解码后交给 BE 执行内核。这条链路不经过 frontend `DmlService`，也不需要 native frontend 伪造同名能力。

## 考虑过的选项

**继续宣称 all-target，并恢复旧 standalone StarRocks table subsystem。** 能让 PR #792 的旧 capability matrix 表面
成立，但会撤销 PR #780 的所有权裁决，把已删除的 catalog/DDL/txn/publish 状态重新塞回 core，并形成第二条与 compat
FE plan 并行的 StarRocks write control plane，因此拒绝。

**在 Local/StarRocks 分支返回 unsupported，保留 enum、append port 与 fake 测试。** 改动小，但公共 contract 继续
表达不存在的产品能力，后续维护者仍会把死路由误当成待接线实现；无消费者的 legacy `TableSink` registry 也会继续
制造错误扩展点，因此拒绝。

**native frontend INSERT 收敛为 Iceberg-only。** frontend 继续拥有 statement conversion、Iceberg shaping、durable
operation lifecycle、statistics sequencing 与唯一 production route；core typed port 只暴露 Iceberg target resolution、
prepare/write/commit/finalize truth。compat StarRocks INSERT 保持 FE plan 到 BE sink 的独立协议链。

## 裁决

native standalone frontend 的持久化 INSERT capability 只支持 Iceberg。`DmlService` 对识别出的 INSERT 统一走 Iceberg
application flow；core `InsertEngine` 不再暴露 Local/StarRocks backend enum、row/batch append、独立 INSERT SELECT
materialization 或 pipeline capability 字段。target resolution 通过现有 Iceberg catalog resolver；`default_catalog` 和
缺失当前 external catalog 按既有错误契约 fail fast。

删除仅服务失效 non-Iceberg route 的 legacy `TableSink` registry 与 Iceberg adapter。production Iceberg distributed
write 继续使用 `IcebergTableSinkFactory`；compat StarRocks write 继续使用由外部 StarRocks FE plan 构造的
`StarRocksTableSinkProgram`，不受本裁决影响。

同时删除旧 standalone StarRocks INSERT 专用的 unpartitioned routing helper，以及可达但已无 table owner 的
`ALTER TABLE ADD PARTITION` standalone StarRocks handler。保留 StarRocks sink routing 时，只允许由完整 external FE
plan 或未来正式 external connector write contract 提供 partition、tablet 与 transaction truth。

删除无 production owner、仅靠测试自我维持的 standalone StarRocks table/txn metadata repository、Avro schemas、ID
scopes 与 `[standalone_server.object_store]`/`warehouse_uri` 配置。对象存储启动凭据统一属于
`[connector.object_store]`；Iceberg warehouse 属于 external catalog properties，不再存在 native managed-lake warehouse
配置面。

Iceberg INSERT 的 StateStore operation journal、immutable admitted execution context、fail-before-side-effect、
append-empty/overwrite-empty、branch ref 与 commit recovery 语义保持不变。未来若要增加 native StarRocks 或其他持久表
INSERT，StarRocks 必须被建模为 external connector，与其他外部数据源一样通过 catalog/provider 与版本化 native plan
contract 接入；它不能成为 native 内部 StarRocks 表，也不能恢复旧 core catalog/tablet/txn subsystem。其他新持久化
provider 同样必须先建立明确的 catalog、write-plan、transaction authority 与真实分布式验收，不能先加空
sink/feature 占位。

保留的 StarRocks scan/codec/tablet execution 代码只表达 compat BE execution 或 future external connector 可复用的
execution primitives，不构成 native table DDL、durable metadata 或 INSERT capability。代码与文档不得再用
`default_catalog` 内表语义解释这些 primitives。

## 接受的妥协（诚实记录）

这项裁决缩小了 PR #792 曾宣称的 capability matrix，也删除了一部分通用 batch-shaping 代码。选择收窄不是因为
Iceberg-only 在抽象上更通用，而是因为它诚实匹配当前产品拥有的唯一 native persistent-table control plane；保留无
owner、无协议、无真实 sink 的抽象会把未实现能力伪装成架构承诺，长期成本更高。

compat 与 native 的 INSERT 入口因此不共享 application service：前者服从外部 StarRocks FE plan，后者由 NovaRocks
frontend 拥有 Iceberg SQL application flow。两条链共享 BE execution primitives，但不共享不存在的 control-plane
metadata，这是进程和产品边界带来的有意隔离。

## 何时重新评估

- native frontend 增加新的 external connector write contract，并能由对应 connector 权威提供写入所需元数据时；
- native plan protocol 增加正式的 external StarRocks 或其他 connector sink contract，且 FE/BE conformance 与 1FE+3BE
  transaction lifecycle 已有独立设计和验收时；
- 产品重新引入 managed internal tables，并明确其 durable metadata、transaction/publish 与 recovery owner 时；
- Iceberg 不再是 native 唯一持久表 catalog，新增 external provider 形成真实、稳定的多实现 INSERT contract 时。
