---
id: ADR-0173
title: "Prepared dataset consumers validate READY without supplying or rebinding runtimes"
domain: [test-fixtures]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0202
    scope: "Remove conditional READY publication and in-place rebuild; check-only consumption, exact input validation and shared-data retention remain effective"
    link: "ADR-0202-benchmark-ready-after-complete-data.md"
partially-supersedes: [ADR-0165]
date: 2026-10-13
provenance:
  - "discussion: 2026-10-13 explicit dataset supply and check-only READY consumption"
code-anchors:
  - "tests/sql/runner/src/dataset_fixture.rs (bind_prepared_dataset, validate_checked_result)"
  - "tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh (main, ensure_docker_services, check_readiness, check_manifest_objects)"
  - "tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py"
---

## 问题

测试如何消费一个已有的准确数据输入，而不把运行测试变成供给数据、恢复服务或重新验证当前生产者的动作？

## 背景与执行事实

输入供给、运行实例和数据消费分别有 owner。ADR-0155 固定输入 provision/BOM；运行层现由ADR-0191固定机器激活与原子worktree publication；ADR-0165保留历史版本化模型。标准数据还有独立 READY 发布，指向准确 dataset key、manifest、warehouse 与 producer fingerprint。worktree runtime publication 选择连接事实，dataset READY publication 选择要读取的数据事实，两者不是同一个身份。

| 入口 | 输入与核验 | 允许的副作用 |
|---|---|---|
| 显式 `--ensure` / `--rebuild` | runtime bind 后使用同一不可变 publication；核对实际 Spark container image ID 与 BOM/resolver 的 producer 投影 | 由输入 owner 生产、复用或显式重建数据，再条件发布 READY |
| SQL consumer `bind_prepared_dataset` | resolver 的准确 suite/scale/contract、已选 fixture publication、typed check result | 只调用 `--check`，不暴露供给模式、不自动重试为供给 |
| `--check` | 解析 caller 的 publication，拒绝 unbound；核对 READY、contract、producer fingerprint、manifest 及所要求的 metadata/statistics 对象存在 | 只读对象事实，不 bind/up/recover runtime，不启动 Spark，不创建 FE/MySQL/catalog 状态 |
| `ReadyDatasetBinding` | `dataset_key`、`exact_warehouse`、`manifest_uri`、READY URI、publication identity/ETag | suite 只使用校验绑定的输入，成功须 `ReadyValid`、`reused=true`、`built=false` |

producer fingerprint 证明数据发布时选定的供给身份与当前 fixture contract 相符；check-only 不声称重新验真当前正在运行的 Spark。保存的 runtime ready 只表示记录状态，不能证明现在服务健康。READY 缺失、unbound、typed result 不合法或身份不匹配都是消费前置失败，不能通过生产新数据来悄悄修复。

本条仅部分替代 ADR-0165 裁决第 9 条对已有 READY 消费者的适用范围：actual Spark/BOM producer 核验仍完整适用于显式供给；实例所有权、原子 publication、清理与数据引用规则继续有效。ADR-0165 的历史正文保留，双向 metadata 表达这项范围变化。

## 考虑过的选项

**所有测试先 bind/up 并核实当前 Spark，再检查或生产数据。设计否决为消费者默认行为。** 操作入口统一，但测试读取既有数据会获得运行实例恢复与供给权，依赖当前 Spark 就绪才能消费旧数据，破坏输入供给与测试消费的边界。显式供给继续使用此流程。

**READY 缺失时自动 ensure，身份失效时自动 rebuild。设计否决。** 容易使用，却会改变本次测试输入与失败材料，并使测试成功无法证明使用了调用方给定的发布身份。

**只检查 READY 存在或使用固定 warehouse。设计否决。** 文件名存在不能证明 suite/scale/contract、producer、manifest 与实际对象一致；固定位置不能表达版本化输入身份。

**准确 binding 加独立 check-only 消费。裁决。** 用同一解析快照传递端点，用 READY identity/ETag 与 typed result 固定数据事实，消费者仅校验，缺失交给显式输入 owner 处理。

## 裁决

1. **供给显式规则。** 只有 ensure/rebuild 可以 bind runtime、启动实际 producer 或写入数据；必须继续校验真实 Spark image 与 BOM 的投影。测试消费不隐式拥有这些能力。
2. **单快照消费规则。** 调用方一次解析 fixture publication，consumer 与子进程使用其中的准确路径和端点。不得在 check 期间重新绑定 current、恢复服务或猜固定端点。
3. **准确输入规则。** 成功绑定必须与请求 suite/scale/fixture contract 的 dataset key、READY URI、manifest、warehouse、publication identity/ETag 一致，且 typed result 明确 `ReadyValid`、`reused=true`、`built=false`。
4. **前置失败规则。** unbound、READY 缺失、身份不匹配、对象校验失败或未知 typed result 非零退出；不改写 golden，不自动 supply/rebuild，不把新数据的成功算作原输入成功。
5. **证明分层规则。** producer fingerprint 是已发布数据的供给身份，runtime ready 是保存状态；两者都不能冒充当前 Spark/服务健康证明。消费对象的实际读取失败仍保留原失败事实。
6. **共享数据保留规则。** worktree 私有 purge 不删除共享标准正确性 READY 数据；数据清理仍遵守准确引用与显式 owner 操作，不扩大普通 consumer 的权力。

## 接受的妥协（诚实记录）

首次或新对象存储上的消费者不能自助修复缺失输入，使用者必须先执行显式供给并校验其结果。历史脚本、配置键仍保留 benchmark 命名，这是输入供给的现存入口，不赋予正确性 runner 测量职责。

check 验证发布契约和要求的对象事实，不重新运行 producer，也不对所有数据字节给出端到端内容证明；查询结果仍由各 suite 的独立 expected 校验。只读 binding 没有在线租约，校验后发生外部删除或服务中断仍可能使实际读取失败。选择显式失败与保留身份，而不是在读取期间透明切换输入。

## 何时重新评估

- 若必须在校验与查询之间保护数据免遭删除，评估由数据 owner 签发的在线租约和真实退出协调；不得靠自动 rebuild 模拟持有权。
- 若供给发布能提供完整不可变内容证明，评估是否扩大 check 的对象/字节验证范围，并明确验证成本与查询 expected 的责任区别。
- 若消费者需要跨发布热切换，重新裁决 query-local 输入快照、版本身份及失败恢复；不能让 current 指针变化改变已开始的测试输入。
- 若移除或重命名历史供给入口，在准确身份、producer 核验、条件 READY 发布和显式消费边界保持的前提下机械迁移调用方。
