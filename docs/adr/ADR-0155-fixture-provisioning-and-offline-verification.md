---
id: ADR-0155
title: "Fixture inputs are provisioned as a verified local BOM before offline test execution"
domain: [test-fixtures]
status: active
supersedes: [ADR-0141]
superseded-by: null
date: 2026-09-20
provenance:
  - "discussion: 2026-09-20 fixture image and Maven input availability in local CI"
code-anchors:
  - "docker/fixture-inputs/lock.json"
  - "docker/fixture-inputs/provision.py"
  - "docker/fixture-inputs/verify.py"
  - "docker/paimon-read/fixture.py (load_writer_bom)"
  - "docker/iceberg-rest/up.sh (fixture input verification)"
  - "tools/ci/local-full-ci.sh (prepare_runtime)"
---

## 问题

测试 fixture 如何既锁定镜像和 Maven 产物身份，又让验证阶段完全不依赖网络或临时 Docker 构建？

## 背景与执行事实

fixture 的输入不止 Compose 服务镜像。Paimon writer 需要特定平台的 Spark base 和两个 JAR；Iceberg
Spark 需要 base、REST/MinIO 镜像与四个 JAR。镜像 tag 不是身份，Dockerfile 在运行期下载 JAR 或构建
derived image 也会把网络和缓存状态混入测试结果。

`docker/fixture-inputs/lock.json` 是唯一输入锁：每个外部镜像记录 manifest digest 与 platform，每个
artifact 记录 URL、字节数和 SHA-1，derived image 记录 Dockerfile、输入 artifact 和定义文件。成功
provision 在本机 store 发布一个不可变 generation，并以 `bom.json` + `READY` 作为可消费证据；BOM
只记录 digest、平台、别名、hash、路径和生成时间，不能包含 secret。

## 考虑过的选项

**A. 各 fixture 在缺失时自行 pull、下载或 build。** 使用方便，但来源和时点随机器网络状态变化，且网络
故障会伪装成测试失败。这是设计否决。

**B. 只保持本机镜像预检。** 能避免 Compose 临时 pull，却不能约束 Maven JAR、derived image 与其
Dockerfile 定义；输入身份仍分散。这是设计否决。

**C. 以一个锁定的 provision/BOM 边界供给全部输入，验证者只消费。** provision 可联网获取并逐项
验证，verify 和消费者只做本机 inspect/hash/label 校验。这是裁决。

## 裁决

1. **供给规则**：仅 `docker/fixture-inputs/provision.sh` 可获取外部 fixture 输入并构建 derived image；
   它必须按 lock 验证后才原子发布 READY BOM。
2. **消费规则**：`verify.sh`、Paimon writer、Iceberg REST 环境和 local full CI 不得 pull、下载或 build；
   必须先验证 BOM，缺失或不一致时 fail fast。
3. **身份规则**：消费者只接受 BOM 中锁定的本机别名和 derived-image definition label；不能以 tag、
   镜像站名称或当前 Docker cache 代替 manifest/platform/hash 身份。
4. **平台与覆盖规则**：schema 2 源锁按准确平台声明变体，共享 artifacts；统一解析作者冻结平台绑定 lock_id，consumer 只选择依赖闭包，不改变身份。实际运行核对本机与 daemon 原生平台；缺变体在副作用前拒绝。部分 provision 不回退同锁/平台已验证覆盖，补齐时重新核验并合并有效回执和 artifact。旧 schema 1 BOM 明确重新供给，不猜测升级、不删除旧数据。
5. **数据集身份规则**：运行供给证明与数据集语义 key 分开；producer 仅消费对应平台的 recipe/基础镜像/artifact 语义投影，传输来源或别名不构成内容身份。producer 源码摘要仍由原数据集 owner 维护。
6. **CI 分类规则**：fixture 前置条件缺失是 `BLOCKED`，不是 Rust 或 SQL `VERIFY FAILED`；CI 不得为
   解除 BLOCKED 自行 provision。

## 接受的妥协（诚实记录）

首次 provision 需要网络、磁盘和 Docker build 权限，并且针对不同平台需要分别 provision。这是为了把
不稳定性隔离到显式供给阶段而承担的成本，不是测试运行期的能力。BOM 位于本机 cache，不是跨主机可移植
artifact；若需要共享，后续应发布经过同一 lock 校验的受管缓存，而不是放宽 verify。

## 何时重新评估

- CI runner 能可靠预置并证明所有锁定输入时，可把 provision 移入 runner image 制作流程，但 verify/BOM
  边界仍保留。
- 需要支持新的 CPU 平台时，为该平台新增锁定输入和独立 provision 证据，不能以不同 manifest 替代。
- 引入受管 artifact 镜像或缓存时，应让 provision 从该受管来源获取，同时保留 digest/hash 校验与原子发布。
