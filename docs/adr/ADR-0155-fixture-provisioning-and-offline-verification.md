---
id: ADR-0155
title: "Fixture creation uses recommended versions without automatic environment admission"
domain: [test-fixtures]
status: active
supersedes: [ADR-0141]
superseded-by: null
date: 2026-09-20
updated: 2026-11-03
provenance:
  - "discussion: 2026-09-20 fixture image and Maven input availability in local CI"
  - "accepted design and approved plan: TST-G18a revision 1, 2026-11-03"
code-anchors:
  - "docker/fixture-inputs/versions.json"
  - "docker/fixture-inputs/provision.py (InputBuilder, prepare_inputs)"
  - "docker/iceberg-rest/machine_runtime.py (ensure_created, operation_snapshot, check_environment)"
  - "docker/iceberg-rest/runtime_entry.py"
  - "tools/ci/local-full-ci.sh (prepare_runtime)"
---

## 问题

如何让开发者直接使用已有 fixture，并在确实缺失时有统一的推荐版本和构建配方，而不让环境验证、升级和恢复成为每次测试的前置协议？

## 替代关系

2026-11-03 的 TST-G18a 裁决替代本 ADR 原先的“digest 源锁、不可变 BOM/READY、离线 verify 后才消费”规则。旧记录只说明历史实现，不再构成当前开发、测试或普通 CI 的准入条件。ADR-0191 的单机器 owner、原运行预算和清理所有权继续有效。

## 裁决

1. `docker/fixture-inputs/versions.json` 是推荐软件版本、平台变体、下载输入和构建 recipe 的唯一声明来源。镜像使用可读版本 tag；不锁定环境镜像 digest。构建所需 artifact 的长度、校验和与 Gradle 依赖校验继续维护，确保取得的构建输入完整。
2. 已有 fixture 直接使用保存的配置。普通入口不自动检查环境 digest、BOM、版本、健康或能力，也不把推荐版本、当前 recipe 或当前 checkout 与运行中环境比较后决定能否运行。
3. 只有确实不存在的 fixture 或实际需要而缺失的输入，才由同一个输入 builder 按推荐版本创建。已有目标镜像直接复用，不递归检查其构建依赖。Docker inspect 的“不存在”与权限、daemon、网络等错误严格区分；后者保留实际失败。
4. 已有但损坏、维护中、部分创建或状态不明的环境报错，交操作员维护。普通入口不自动修复、重建、升级、切换来源或创建第二套共享栈。修改推荐版本不会改写已有机器、数据卷或 publication。
5. `fixture-runtime.sh check` 是显式手动诊断：报告推荐版本、已有实际版本事实、保存的机器目标、健康观察和错误；未知实际版本保留未知，不从 tag 或推荐值推造。它不修复环境，也不授予测试运行资格。普通 CI 不自动调用；固定全量 CI 机器以后可显式调用并保存结果。
6. 操作仍绑定原 daemon、generation、容器、volume、网络、publication、run scope 与完整期限。持锁排序、未知 mutation、IAM 撤销、catalog 屏障和真实 writer 退出属于实际操作安全与清理责任，不能删除，也不能扩张为全机环境扫描。
7. 数据集 READY、内容 oracle、producer 来源和原 publication 绑定仍由原数据集 owner 维护。它们证明具体数据或本次操作目标，不是当前推荐环境一致性的证明。已有 benchmark 消费者继续 check-only，不因本裁决重写 warehouse 或重建数据。
8. 创建或实际消费失败保留原命令、退出、阶段与清理结果。普通 CI 不根据历史 fixture exit 75 将实际准备失败改成环境前检 BLOCKED；原预算在零副作用点的拒绝等具名语义仍按其 owner 分类。

## 输入创建与维护

显式预构建可以调用 `docker/fixture-inputs/provision.sh --consumer <name>`；普通创建入口复用同一个 builder，不存在第二套供给协议。平台只选择已声明 recipe，缺变体明确报错，不自动仿真或降级。OSS、MinIO、HMS 与暂停的 Paimon 工具分别选择实际所需闭包。

已有 artifact 校验失败保留原件并报错，不能覆盖它来掩盖损坏。缺失下载在独占临时位置完成校验后原子移交。显式传输映射只改变下载位置，不改变 artifact 内容约束，失败不自动回退。

维护者需要切换已有机器时显式执行 `fixture-runtime.sh upgrade`。仍须遵守 ADR-0191 的原维护意图、旧写者退出、凭证和 catalog 屏障；新推荐值本身不授权升级。

## 接受的代价与重新评估

已有环境可能与推荐版本不同，实际执行也可能暴露环境问题；这是直接消费而不建立自动准入协议的明确选择。agent 无法继续时保存实际失败，交人工升级，不自动寻找兼容版本。

缺失输入的创建可能联网和构建，失败不伪装成产品测试成功。空白机器首次构建、特定平台可用性和实际互操作仍须各有真实证据；mock、推荐清单和手动诊断均不能代替这些事实。

若未来固定全量 CI 需要集中环境报告，调用同一个显式诊断入口；是否因此引入新的准入或升级政策须另行讨论，不能暗中恢复日常 gate。
