---
id: ADR-0202
title: "Benchmark READY is a completion marker without conditional replacement"
domain: [test-fixtures]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0173]
date: 2026-10-28
provenance:
  - "discussion: 2026-10-28 remove benchmark conditional publication and in-place rebuild"
code-anchors:
  - "tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh (main, publish_ready, check_manifest_objects)"
  - "tests/sql/fixtures/benchmarks/benchmark_fixture_publication.sh (fixture_publication_put)"
---

## 问题

标准测试数据如何保留完整性与显式供给边界，而不要求对象存储提供条件覆盖或维护并发重建协议？

## 背景与执行事实

数据生成使用每次独立的 staging 根。READY 记录准确输入契约、实际 producer、warehouse 和 manifest，消费者沿该记录验证所需对象。完成标记可以防止把部分数据当作可复用输入；它不必同时充当多写者之间的 compare-and-swap 原语。

本条仅替代 ADR-0173 中显式 `--rebuild`、条件 READY 发布及由其隐含的并发胜者保证。其显式供给、单快照消费、准确身份、只读检查、失败与共享数据保留规则继续有效。

## 考虑过的选项

**维护条件覆盖与原地重建。** 需要底层的 compare-and-swap，并为不同对象存储引入对应协议。其并发重建能力不属于当前手动数据准备的需求，删除该能力。

**增加版本选择、发布日志或协调服务。** 可以恢复并发发布语义，但会引入新的数据选择和恢复责任。当前不提供这些能力。

**完整数据之后普通发布 READY。裁决。** 沿用准确输入和独立 staging；完成全部数据与 manifest 核验后以普通 PUT 写入完成标记。消费继续只读检查记录及其准确对象。

## 裁决

1. 显式准备只提供 `--ensure`：已有有效 READY 时复用，不存在时生成；存在但无效时明确失败，不自动修复。
2. 删除原地 `--rebuild` 与条件发布实现；不以其他参数、锁、版本管理或发布日志恢复同一功能。
3. 每次 producer 使用独立 staging 根，发布前核对 manifest、输入契约、producer 与所要求的 metadata/statistics 对象；实际生成和上传失败不得发布成功标记。
4. READY 使用普通对象 PUT。ETag 只记录读取到的对象身份，不授予覆盖条件、持有权或并发胜者语义。
5. 消费者只执行 `--check`，沿同一次实际读取的 READY 校验准确数据；不恢复运行实例、不启动 producer、不切换到其他目录掩盖失败。
6. 普通准备与消费不删除旧 staging、已发布数据或兄弟 dataset。取消仍由原 owner 停止实际 writer，退出后不得继续发布。

## 接受的妥协（诚实记录）

多个手动 producer 可以同时为同一输入生成完整数据，后完成的普通 PUT 可以更新 READY；系统不承诺唯一胜者或不会重复生成。每次候选须完整，已返回的消费绑定继续指向其原 warehouse，不跟随标记热切换。

无效 READY 需要维护者在准确作用域内另行处置；没有一键覆盖修复入口。本裁决不提供云端数据回收策略，也不证明任何对象存储的实际接线已经可用。

## 何时重新评估

- 明确需要多机器协调的数据发布时，重新裁决其 owner、外部原子操作和未知结果恢复，不从普通 PUT 推导并发保证。
- 需要修复损坏的已发布输入时，另行确定准确数据身份和维护操作，不赋予普通消费者隐式重建或删除权。
