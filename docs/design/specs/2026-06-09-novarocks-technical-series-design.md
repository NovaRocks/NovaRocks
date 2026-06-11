# NovaRocks 技术分析系列 · 设计文档

> 状态：已确认篇目与结构，待落成每篇写作计划。
> 日期：2026-06-09

## 1. 目标与定位

- **主线**：系统设计剖析（剖析 NovaRocks 本身的架构与关键子系统的设计与取舍），而非引擎教学，也非 AI 协作方法论复盘。
- **目标读者**：懂行的数据库/系统方向工程师。默认读者熟悉 SQL、列式执行、MPP 的基本概念，不需要从零铺垫。
- **语言 / 平台**：中文 · 国内技术社区（公众号 / 知乎 / 掘金）。语气可略口语化、重叙事；单篇控制在适合完读的长度。
- **形态**：双层结构 —— 1 篇旗舰总览 + 子系统深度剖析，共 7 篇。
- **成功标准**：
  1. 每篇能独立成立、独立转发；
  2. 整体拼出一张"NovaRocks 系统设计地图"；
  3. 技术准确、代码片段来自真实源码；
  4. 诚实对待项目的实验性定位，不夸大、不假装成熟。

## 2. 系列结构

- **双层结构**：第 0 篇是旗舰总览（给地图和入口），第 1-6 篇是子系统深度剖析。
- **共 7 篇**，可顺序发布、随时叫停。
- **编排主线**：顺着"一条查询的一生 + 系统分层"走，前几篇打通共享执行内核，再揭示 standalone 这个"另一个前门"，然后接入数据湖，IMV 压轴，正确性工程收尾。
- **统一立意**：每篇都不停在"是什么"，而是讲**为什么这么设计、放弃了什么、对照 StarRocks / 其他引擎有何异同**。

## 3. 篇目（共 7 篇）

### 第 0 篇 · 总览 + 从计划到可执行计划
- **抓手**：FE 发来一棵 thrift 计划树，NovaRocks 怎么把它变成能跑的东西？借这个问题铺开全局。
- **覆盖**：
  - 全局地图：NovaRocks 是什么、为什么用 Rust 重写 StarRocks BE、双模式（FE 兼容 / standalone）如何共享同一执行内核。
  - 分离原则："C++ shim 只做 brpc 协议网关 / 执行语义全在 Rust"。
  - fail-fast 哲学。
  - Plan Lowering：`src/lower/**`，`TPlanNode`/`TExpr` → `ExecPlan`/`ExprArena`、layout/slot 推断、type lowering。
- **关键代码**：`src/main.rs`、`src/service/internal_service.rs`、`src/service/engine_ffi.rs`、`src/shim/compat.h`、`src/lower/{fragment,node,expr,layout,type_lowering}.rs`。
- **看点 / 取舍**：FE 契约边界为什么坚持 fail-fast 而非"尽力而为"。FE 契约与 StarRocks 兼容**点到为止**，作背景不作主角。
- **衔接**：结尾用"一棵 thrift 计划树变成 ExecPlan"自然过渡到执行引擎。

### 第 1 篇 · 执行引擎（列式 + 算子 + pipeline + exchange）
- **抓手**：ExecPlan 拿到手，数据是怎么一批批流过算子、并行跑起来、跨节点搬运的？
- **覆盖**（一层讲透执行，全景高度）：
  - 列式核心：Arrow `RecordBatch` 如何包成 `Chunk`（slot↔列映射、内存计量）、算子工厂、`ExprArena` 表达式图。
  - Pipeline 调度：从 `ExecPlan` 建图、driver 执行、dependency 管理、全局调度、可观测事件。
  - Exchange 与运行时：gRPC exchange、sender 背压、`ExchangeScanOp` 阻塞到 EOS、result buffer、取消传播、runtime filter 传输。
- **关键代码**：`src/exec/chunk/`、`src/exec/node/`、`src/exec/operators/`、`src/exec/expr/`、`src/exec/pipeline/**`、`src/runtime/exchange*`、`src/service/grpc_*`、`src/runtime/result_buffer.rs`。
- **看点 / 取舍**：为什么 Arrow-first；StarRocks 的 pipeline 模型在 Rust 里怎么落地；分布式执行里最容易出 bug 的协同（背压 / EOS / 取消）怎么收敛。
- **风险标注**：本篇合并了三块，3000-4500 字只能走全景高度（架构图为主、代码点到为止）。若写时发现 pipeline 或 exchange 值得展开，可临时拆"加餐篇"。

### 第 2 篇 · Standalone SQL 栈与优化器
- **抓手**：脱离 StarRocks FE，NovaRocks 自己怎么把一句 SQL 变成能跑的计划？
- **覆盖**：parser / analyzer / optimizer / codegen 全链路；通用逻辑重写框架（见 `docs/design/2026-05-25-general-logical-rewrite-framework.md`）；CBO 统计 / NDV；aggregate pushdown、runtime filter 规划、column pruning；MySQL 协议兼容。
- **关键代码**：`src/sql/parser/**`、`src/sql/analyzer/**`、`src/sql/optimizer/**`、`src/sql/codegen/**`、`src/engine/mod.rs`、`src/server/mod.rs`、`src/sql/explain.rs`。
- **看点 / 取舍**：同一执行内核的"另一个前门"；优化器规则如何可观测、可 bisect（`disable_optimizer_rules`）。

### 第 3 篇 · Iceberg 集成与 Format v3
- **抓手**：一个用 Rust 写的引擎，怎么把 Iceberg 啃到 format v3，还能和 Spark 互通？
- **覆盖**：catalog 三态（memory / hadoop / rest）；读写路径；v3 新特性（deletion vectors / row lineage / variant / 纳秒时间戳）；与 Spark 跨引擎兼容怎么验证（docker iceberg-rest + MinIO + Spark）。
- **关键代码**：`src/connector/iceberg/**`、`src/engine/iceberg_writer.rs`、`docs/guides/iceberg-v3/**`、`docker/iceberg-rest/`。
- **看点 / 取舍**：开源引擎里少见的 v3 完整度；跨引擎兼容的工程代价。

### 第 4 篇 · Managed-Lake 元数据与事务
- **抓手**：没有 FE、没有外部 metastore，standalone 模式下"自管的湖"怎么保证一致性？
- **覆盖**：SQLite 元数据 + 对象存储、DDL/DML、事务生命周期、erase worker、MV 管理。
- **关键代码**：`src/connector/starrocks/managed/**`、`src/engine/statement.rs`。
- **看点 / 取舍**：存算分离在 standalone 模式下的落地；元数据事务与对象存储最终一致性的协调。

### 第 5 篇 · 压轴｜增量物化视图 IMV
- **抓手**：物化视图最难的不是建，是"只刷新变化的那部分"。NovaRocks 怎么做增量？
- **覆盖**：从 shape-dispatch 重构到 capability property（property framework）；branch-union refresh；delta derivation；aggregate-over-UNION-ALL 增量刷新。
- **关键代码**：`src/engine/mv_flow.rs`、IMV 相关 refresh 路径、`docs/design/plans/` 中的 IMV/IVM 系列计划文档。
- **看点 / 取舍**：系列的"代表作"；增量正确性的难点与边界。

### 第 6 篇 · 收尾｜正确性工程：SQL 回归作为 ground truth
- **抓手**：3.5 个月、近 55 万行、大量 AI 协作，为什么没失控？
- **覆盖**：`sql-tests` 统一 runner、record/verify/diff 闭环、`docker/iceberg-rest` 跨引擎 fixture、把"正确性"当成工程闭环。轻量呼应"AI 高速协作为什么没失控"，但不喧宾夺主。
- **关键代码**：`tests/sql-test-runner/`、`sql-tests/`、`docker/iceberg-rest/`。
- **看点 / 取舍**：正确性如何成为高速迭代的护栏。

## 4. 每篇统一写作模板

1. **钩子**：用一个具体问题或场景切入，不从定义开始。
2. **定位**：回扣总览图，给最小必要的 StarRocks 背景。
3. **设计与实现**（主体）：1 张架构图 + 关键数据结构/接口 + 选择性真实代码片段 + 走一条主路径。
4. **取舍与对照**（差异化核心）：为什么这么设计、放弃了什么、对照 StarRocks / 其他引擎的异同、踩过的坑。
5. **小结 + 下一篇钩子**。

## 5. 准确性与取材约定（质量命门）

- 每篇**动笔前实际读对应源码**，代码片段必须来自真实文件并标注路径 —— 不靠记忆、不靠 `AGENTS.md`/`CLAUDE.md` 转述。
- **不夸大**：诚实对待"实验性 / AI 辅助 / 未生产验证"的定位，总览交代一次即可，后续不反复强调，也不假装是成熟系统。
- 只讲**已验证存在**的算法/数据结构，不臆造内部细节；架构图与代码保持一致。
- 中文行文，但保留代码标识符原文（`Chunk` / `ExecPlan` / `ExprArena` / `ExchangeKey` …），符合仓库语言策略。
- 引用**性能数字必须有来源**（benchmark / 实测），否则只做定性描述。

## 6. 存放与产出

- 本设计文档：`docs/design/specs/2026-06-09-novarocks-technical-series-design.md`
- 文章成稿：`docs/articles/zh/NN-<slug>.md`（`NN` 为 00-06）
- 配图：先用 mermaid 或文字描述占位，发布前再出正式图。

## 7. 推进方式

- **一次一篇**：动笔前读源码 → 写草稿 → 自查（准确性约定）→ 用户过稿 → 定稿。
- 第 0 篇先行；后续按顺序，可随时叫停或插入"加餐篇"。
