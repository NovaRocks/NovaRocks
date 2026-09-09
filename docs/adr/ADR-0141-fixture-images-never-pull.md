---
id: ADR-0141
title: "Test fixtures resolve pinned images from the local store and never pull"
domain: [test-fixtures]
status: active
supersedes: []
superseded-by: null
date: 2026-09-09
provenance:
  - "discussion: 2026-09-09 BuildKit resolves a digest-pinned FROM against the registry even for a local image"
  - "PR: pending — backfill the number once the never-pull fixture convergence merges"
code-anchors:
  - "docker/paimon-read/fixture.py (resolve_local_base_image)"
  - "docker/paimon-read/Dockerfile (SPARK_BASE)"
  - "docker/iceberg-rest/up.sh (require_local_image)"
  - "docker/iceberg-rest/compose.yml (pull_policy)"
  - "tests/datasketches-tck/interop/trino/verify_rest_catalog.sh (resolve_local_trino_image)"
---

## 问题

测试 fixture 的镜像身份该由谁裁定——registry，还是本机镜像库加一次显式预检？

## 背景与执行事实

BuildKit 对 digest 形式的 `FROM repo@sha256:…` **一律先向 registry 解析元数据**，即使该 manifest 已完整存在于本机、`RepoDigests` 逐字匹配、平台也一致。本机实测（Docker 29.4.2、BuildKit v0.29.0、containerd image store、docker driver）：

- `FROM --platform=linux/amd64 apache/spark@sha256:b2da01c5…` 停在 `[internal] load metadata`，向 `registry-1.docker.io` 发 manifest HEAD，120 秒后 `DeadlineExceeded` 构建失败；而 `docker image inspect apache/spark@sha256:b2da01c5…` 在同一台机器上立即返回，平台为 `linux/amd64`。
- 同一个 image ID 改成 **tag** 引用后 `load metadata` 为 `DONE 0.0s`，零 registry 流量；**带 `--platform` 也一样**。去掉 `--platform` 不改变 digest 引用的失败结论——触发条件是 digest 引用本身，与平台标记无关。
- 同一 digest 换成本机独有的仓库名（`novarocks/spark-base@sha256:b2da01c5…`）会在**耗尽同样的 120 秒 deadline 之后**回落到本机并成功。本地回落确实存在，但它只是远端解析失败后的兜底；对 `docker.io/apache/spark` 这种能正常取到 token 的公开仓库，超时是致命错误而非回落。

工具面的能力是不对称的：`docker run` 与 `docker compose up|run` 有 `--pull never`，compose 服务还能声明 `pull_policy: never`；**`docker build` 没有等价开关**。buildx 只把 `--pull` 映射为 resolve mode `pull`，未暴露 `prefer-local`。

镜像的**名字不是身份**。经镜像站拉取记录的是 `<mirror>/<repo>@<digest>`，不是上游 `<repo>@<digest>`，而 `docker tag` 不能生成 digest 引用，因此无法把前者改写成后者。同一个 manifest 在不同机器上完全可能只以镜像站的名字存在。

身份证据的形态也取决于 image store：containerd store 把 **manifest digest** 报成 image id；graphdriver store 报 **config digest**，并把 manifest digest 放在 `RepoDigests` 里。

这两点叠加会产生同一类故障的两个实例：`docker/paimon-read` 的 digest 钉死 `FROM`（构建必然往返 registry），以及 Trino REST interop 校验只认 `trinodb/trino@<digest>` 这一个名字（本机有该 manifest 却被判定缺失）。此外 MySQL fixture 原本在本地未命中时 `docker compose pull mysql` 兜底，使镜像来源随机器而异。

## 考虑过的选项

**A. 保留 digest 形式的 `FROM`，靠可达的 registry mirror 兜住。** 机制：把仓库路径换成镜像站。优势：改动最小，digest 校验仍由 BuildKit resolver 执行。代价：每次构建——包括完全命中缓存的构建——仍是一次 registry 往返；本机已有该 manifest 也用不上；断网机器无法构建。本质上只是把不可达的 registry 换成另一个 registry，并未消除「测试运行期依赖 registry」这个属性，因此否决。

**B. 让 BuildKit 本地优先。** 机制：`image-resolve-mode=prefer-local`。该 frontend 选项存在于 BuildKit，但 buildx 与 `docker build` 均未暴露，`docker build` 也没有 `--pull never`。当前不可行，故否决。

**C. 缺失即自行拉取（原 MySQL fixture 的形态）。** 优势：新机器一条命令跑通。代价：测试运行的镜像来源不确定——同一次运行在一台机器上用刚拉的镜像、在另一台用久存缓存；并把网络故障伪装成测试失败。与「fixture 是可复现基线」的前提冲突，否决。

**D. 本机镜像库是唯一来源，digest 由显式预检裁定。** 机制：compose 服务声明 `pull_policy: never`；`docker build` 之前预检 base 镜像必须在本机；digest 钉死的 base 按 **digest** 在本机解析（候选包含裸 digest，因而不认名字），校验 manifest 身份与平台后打一个内嵌 digest 的本地别名 tag 交给 BuildKit；`docker run` / `compose run` 加 `--pull never`；缺失是错误，并给出一次性导入命令。对照 Bazel / Nix 一类「外部依赖先声明、再进入构建」的做法：**获取与使用分离**，provisioning 是独立步骤。

## 裁决

选 D。本机镜像库是 fixture 镜像的唯一来源；钉住的 digest 是身份，仓库名只是本机细节；缺失是错误而不是下载。获取镜像是与测试运行分离的显式步骤（CI 中体现为一个独立的 import 步骤，镜像名从 compose 文件读出以防漂移）。

## 接受的妥协（诚实记录）

1. **digest 不可变性的执行者从 BuildKit resolver 变成我们自己的预检。** 这不是因为我们的校验更强，而**只是因为 BuildKit 不提供「本地优先且仍校验 digest」的模式**。预检同时接受两种身份证据（image id 等于 manifest digest，或 `RepoDigests` 含该 digest）以覆盖 containerd 与 graphdriver 两种 image store；若这两个字段在某个 Docker 版本上语义变化，校验会**静默变弱**。这是真实风险，目前靠 fixture 单测钉住「缺失即报错」「平台或 manifest 不符即报错」来兜，没有更强的保障。

2. **`FROM` 从 digest 引用降级为 tag 引用。** Dockerfile 单独看不再自证 base 身份，必须连同 `versions.env` 的钉死 digest 与预检一起读。补偿仅是：别名 tag 内嵌 digest 前缀，且单测断言 `FROM` 行不含 `@sha256:`、`SPARK_BASE` 默认值等于预检生成的别名。

3. **新机器多一步人工导入。** 这是有意选择而非疏漏；代价由 provisioning 承担，换来测试运行的确定性。

4. **`PAIMON_SPARK_IMAGE_REPOSITORY` 语义改变**：原为「从这个镜像站拉」，现为「它在本机叫这个名字」。由于候选里有裸 digest，该变量在 containerd store 上通常已不必要；保留它是为 graphdriver store 与「显式钉死单一引用」的场合。

5. **Maven jar 仍在构建期从网络获取**（Paimon 的两个 jar、Hive 的 hadoop-aws）。本 ADR 只裁决**镜像**，未把 jar 一并本地预置——那需要一个受管的产物缓存，成本超出本次范围。因此「测试运行不触网」目前只对镜像成立，对构建期产物不成立。

## 何时重新评估

- **BuildKit / buildx 暴露 `image-resolve-mode=prefer-local`，或 `docker build` 获得 `--pull never`**：届时应把 `FROM` 改回 digest 引用，让 resolver 重新成为不可变性的执行者，并删除别名 tag 机制与妥协 1、2。
- **Docker 全面切到 containerd image store 且 `.Id` / `RepoDigests` 语义稳定**：预检的双分支可收敛为一条，妥协 1 的静默变弱风险随之下降。
- **出现受管的构建产物缓存（镜像与 jar 同源）**：妥协 5 可一并消除，「测试运行不获取外部产物」才真正成立。
- **CI 迁到预置镜像的 runner**：显式 import 步骤可以去掉。
