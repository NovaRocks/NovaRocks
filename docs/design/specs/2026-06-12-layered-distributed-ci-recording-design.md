# Layered Distributed CI Recording 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

PR #295 后的 CI 状态有一个管理问题：旧 run 的失败列表、targeted rerun 的通过情况、
未重跑的 suite、已修复但未在 full CI 验证的 case 混在一起。随着 1FE+3BE 的问题被拆成
schema、aggregate state、remote result、Iceberg descriptor、optimizer boundary 等多条线，
CI tooling 也需要记录“已修复、仍失败、未重跑、stale failure”。

已有 `tools/ci/local-full-ci.sh` 和 `logs/ci-full/<timestamp>/summary.md` 是基础。
本 spec 定义下一层 structured recording。

## 2. 目标

- 每次 CI run 记录 cluster mode、cluster size、profile、commit、config。
- suite/case failure 自动分类。
- targeted rerun 能标记旧 failure 是否 stale。
- 支持 smoke、targeted regression、full matrix 三层入口。
- summary 同时适合人读和机器解析。

## 3. 非目标

- 不接 GitHub Actions。
- 不实现自动修复。
- 不发送通知或写 Obsidian。
- 不把 SQL runner 改成全新框架。

## 4. Run metadata

每次运行生成：

```text
logs/ci-full/YYYYmmdd-HHMMSS/
  summary.md
  run.json
  failures.jsonl
  reruns.jsonl
  env.log
  sql/
```

`run.json`：

```json
{
  "commit": "...",
  "branch": "...",
  "cluster_mode": "cross-process",
  "cluster_size": 3,
  "profile": "dev-opt",
  "started_at": "...",
  "novarocks_config": "...",
  "sql_test_config": "..."
}
```

`failures.jsonl` 每行一条 failure：

```json
{
  "suite": "aggregate",
  "case": "agg_test_count_distinct",
  "query": 25,
  "category": "AggregateStateSchemaMismatch",
  "message": "scalar output type mismatch for Decimal128",
  "log": "sql/aggregate.log",
  "status": "open"
}
```

## 5. Failure 分类

初始分类：

- `SchemaMismatch`
- `AggregateStateSchemaMismatch`
- `RemoteResultCoercion`
- `ResultMismatch`
- `Timeout`
- `IcebergWriteDescriptor`
- `IcebergCommitLifecycle`
- `OptimizerBoundaryProperty`
- `RuntimeFilter`
- `HarnessFailure`
- `Unknown`

分类规则先用错误文本和 suite/case hint 实现，不依赖 ML 或外部服务。

## 6. 三层入口

### Smoke

快速验证当前改动是否破坏分布式基础：

```bash
tools/ci/local-full-ci.sh --cluster-mode cross-process --cluster-size 3 --tier smoke
```

覆盖：server readiness、simple exchange、simple aggregate、simple Iceberg REST insert/read。

### Targeted regression

按问题类别跑目标 case：

```bash
tools/ci/local-full-ci.sh --tier targeted --category AggregateStateSchemaMismatch
tools/ci/local-full-ci.sh --tier targeted --suite aggregate --only agg_test_count_distinct
```

### Full matrix

完整本机 full CI：

```bash
tools/ci/local-full-ci.sh --cluster-mode cross-process --cluster-size 3 --tier full
```

## 7. Stale failure 处理

提供 rerun manifest：

```text
logs/ci-full/<old-run>/failures.jsonl
  -> tools/ci/rerun-failures.sh --from <old-run> --category SchemaMismatch
```

rerun 结果写入新 run 的 `reruns.jsonl`：

```json
{
  "old_run": "20260611-160353",
  "suite": "aggregate",
  "case": "agg_test_count_distinct",
  "old_status": "failed",
  "new_status": "passed",
  "classification": "stale_fixed"
}
```

summary 中分开显示：

- still failing
- stale fixed
- not rerun
- newly failing

## 8. 工具边界

`local-full-ci.sh` 保持主入口。新增 helper：

```text
tools/ci/lib/classify_failure.sh
tools/ci/lib/jsonl.sh
tools/ci/rerun-failures.sh
tools/ci/targeted-suites/
  aggregate-state.txt
  remote-result.txt
  iceberg-write-descriptor.txt
  optimizer-boundary.txt
```

SQL runner 本身只需要输出可定位 suite/case/query 的日志；分类由 CI layer 完成。

## 9. 验证

- Shell unit-style tests 使用 fixture logs 验证分类规则。
- 本机 smoke run 生成 `run.json`、`failures.jsonl`、`summary.md`。
- rerun fixture 能正确标记 stale fixed / still failing / not rerun。
- summary 不把 skipped/not-rerun 误报为 passed。

## 10. 成功标准

- full CI 后能一眼区分旧失败、新失败、已修复、未重跑。
- 失败能按 schema/aggregate/remote result/Iceberg/optimizer 等方向分组。
- 后续每个长期 spec 的 implementation PR 都能声明对应 targeted category 并复用同一入口。
