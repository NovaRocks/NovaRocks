<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 测试准入、尝试材料与证据边界

开发规则由 [workflow-contract](../../../.agents/skills/workbench/skills/workbench/references/workflow-contract.md) §8.5–§8.8 定义；本指南落实 NovaRocks 的位置、操作责任和现有报告解释，不建立第二套运行协议或结果 schema。新资产及实质扩展从规则合入起消费合同；存量按领域迁移，文件存在或一次通过不表示已经合规。当前阶段仅维护功能和数据正确性；SQL 原生入口仅发现 correctness suite，旧常设测量载体已退出。

## 三层验证与证据组合

| 层 | 必需集合与来源 | 结论边界 |
|---|---|---|
| 开发阶段 | 影响图选中的原生单元及必需模式；本轮执行或合格的本地/正式同输入来源 | 改动相关部分已验证；其余常规项明确延后给准入，重项履行作者义务或写明理由 |
| PR 准入 | 合入队列的合并组提交上，常规集合中已经启用的单元及必需模式；当前正式尝试，暂不跨运行复用 | 仅对启用范围下结论；未纳入、无纳入计划和列举缺口逐项保留；不读影响图 |
| 合入后全量 | 全部未暂停单元及全部必需模式，仅认本轮执行 | 登记新失败、仍失败、最近一次通过但未关闭、已关闭与未运行；不阻断合入 |

G15 交付集合派生、证据组合和续跑规则；PR 准入入口提供正式上下文、启用范围与合入队列检查。合入后全量、跨运行共享证据及登记展示尚未交付。`local-full-ci.sh` 是本地完整入口，`dev-verify.sh compose --layer admission` 仅作本地准入演练；运行 wrapper、干净 commit、文件名或 `.formal` 进程登记上下文均不能自行将材料升级为正式来源。它们也不证明实际合入的提交已验证。

组合记录把五件事分开：来源是本轮还是历史；原始结果是通过、失败、未知、取消还是未运行；记录/单元/共享清理/整个尝试分别完成到哪一级；输入与验证义务是否适用；本层如何处置。`complete=true` 不等于通过，整个尝试未完成也不必抹掉其中已收束的独立范围。旧材料与所有反例保留，组合只写派生记录。

复用要求声明完整的输入和初始状态合同，开始/结束确认输入稳定，实际执行本单元的默认断言、正确模式与独立 oracle，且同一复用范围的生命周期与必要清理完成。源码 SHA、成功退出、suite 名或一个 hash 不能代替这些事实。SQL 当前默认复用范围是整个 suite；缺少 `isolated/reset/immutable` 初始状态声明时不能沿用历史结果，也不能因只改一个文件就在开发入口拆成 `--only`。同身份一败一过是“不一致”，不能写成已修复。取消后有合格完整执行可补覆盖，但旧取消及未确认清理义务仍保留。

层级与排除状态相互独立。常规项进入准入，重项在全量中运行；重的依据必须是额外程序、fixture、大数据集、nightly 工具链等准入环境外前置，或实测超出准入预算。暂停项由原生 DISABLED/paused 声明排除所有层，保留原因与 owner；不稳定项由处置记录退出准入判定，全量仍运行；已知失败只处置已诊断的具体失败，不删除执行义务。

[dispositions.json](../../../tools/ci/baselines/dispositions.json) 是处置来源。`known_failures` 仅接受 default SQL case，必须有 reason、owner、expires；特征为准确错误码，或语句序号加 actual 摘要的结果 mismatch。Rust 断言、超时、suite 初始化/加载失败、取消、未知、未运行、未执行验证和清理违约不可作为已知失败豁免。`quarantine` 必须有 reason、owner、exit_condition、review_by；过期失效。通过后提示陈旧处置，不自动删除记录或关闭问题。

全量失败只按具体身份、输入、失败特征与来源登记。一次通过更新“最近一次通过但未关闭”；关闭还需关联修复与符合要求的复验。G15 的登记派生接口不创建共享问题库，也不自动关闭或修复任何问题。

## PR 准入链路

[Admission 工作流](../../../.github/workflows/admission.yml) 在 `merge_group` 上运行名为 `admission` 的检查；PR 上该 job 跳过，独立的 `pr-validation` 在普通 `pull_request` 事件运行完整 cargo-only 链，默认检出 PR 的测试合并 SHA。它与正式 job 通过 YAML anchors 共用工具准备、环境、执行步骤、失败摘要与原始材料上传；PR 材料仍归为 `diagnostic`。`precheck` 继续提供独立快速反馈。PR 的两种检查各有按 PR 编号隔离的并发组，新提交取消各自的过时运行，不取消合并组或 main 手动运行。

`pr-validation` 在合入按钮上方的 Checks 区保留真实的排队、运行、成功、失败或取消状态，不使用 `continue-on-error` 或成功包装。它不加入 required checks，因此其失败本身不阻塞合入；是否要求检查通过由仓库设置决定。为提前获得完整反馈，接受每次 PR 更新额外运行完整 cargo-only 链的成本，见 [ADR-0188](../../adr/ADR-0188-optional-pr-validation-and-formal-admission.md)。手动运行用于基线与诊断；工作流文件存在不代表规则集已经启用，仓库设置与真实队列验收须另外完成。

```bash
tools/ci/admission.sh --output /absolute/new/admission-attempt
```

输出目录必须不存在。入口运行一次 `local-full-ci.sh --cargo-only`，保留其真实退出，再以组合的 `layer_ok` 决定退出：0 为启用范围通过，1 为未通过，2 为入口错误。构建/原生列举失败仍保存 `entry.json`、失败的 `composition.json`、`summary.md` 与已有原始材料；本次未能列举的种类是阻断缺口，空列举不能成为通过。

正式来源必须同时属于准确仓库的 `.github/workflows/admission.yml`、GitHub 托管 runner，以及 `merge_group` 或 main 的手动事件。合并组 head/base 来自原始事件，head 与 GitHub SHA 和干净源码开始/结束记录一致；保存 workflow ref/SHA、run/attempt 和 runner 事实。其它 GitHub 上下文为 `diagnostic`，本地为 `local-rehearsal`，两者都不授予正式来源。下载材料或伪造环境变量不具备 GitHub 检查的权威。

[admission-scope.json](../../../tools/ci/baselines/admission-scope.json) 是分步启用声明。第一批只含 cargo-only 阶段、全部常规 Rust test 与 workspace doctest；SQL/System 暂未纳入。具名阶段显式列名，只有 Rust 种类允许 `all`；名称用本次原生列举校验。未纳入的常规范围、缺少纳入计划和绑定构造的列举缺口都在组合与摘要中逐项可见；任何已观察清理违约仍阻断。新增成员须先在准入环境的 main 通过或取得有效处置，再通过 PR 纳入；全部常规单元纳入后可删除该文件，恢复完整常规集合规则。

准入/全量的 Rust 聚合阶段同时记录原生退出与成员推导结论。准入隔离可以排除成员断言，但批处理必须完整收束、清理通过，prepare/error-manifest/final-prepare 成功，无环境错误或取消；nextest 100 是普通测试失败。现有 Cargo doctest 非零退出无法区分断言与环境错误，不能据此推导聚合通过。全量不应用准入隔离；开发层的聚合延后规则保持不变。批处理索引中的 prepare、error_manifest、final_prepare 分别提供联合准备、错误清单新鲜度和 build-info 阶段观测，不改写原生索引。

每次正式运行只消费当前尝试。GitHub artifact 包含入口输出和原生运行目录（包括隐藏诊断文件），名称带 run/attempt，配置保留 90 天；这一有限平台保留期不构成持久证据服务。维护者须在到期前为未解决失败和正式引用安排可恢复交接。复跑另建尝试，不能覆盖原失败。只有一位维护者时不要求自审，准入逻辑修改由入队者审阅 diff；只读令牌、无 secret 和每次全新的托管 runner 是执行边界。

上线顺序是本地验证、经授权发布/合入、main 正式基线、经授权启用规则集、真实队列成功/失败/修复再入队验收。本地可发布检查点不等于门禁交付；未执行的正式基线、设置和队列验收继续记为 NOT_RUN。长期规则见 [ADR-0189](../../adr/ADR-0189-merge-queue-admission-and-current-attempt-evidence.md)。

## 影响声明、发现与开发入口

Rust 从 Cargo 归属和原生 nextest `rdeps()` 选择，不另造依赖图；运行输入与环境声明在 package metadata，ignored 项区分内部 helper 与需要作者履行的 heavy 测试。SQL 的 `--list-declarations` 是 suite impact、环境输入、reuse 与 explicit-only 的单一来源，[suite map](../../../tests/sql/correctness/README.md) 由其生成。System 同名原生列举给出 default/explicit/compatibility/paused、impact、环境输入、初始状态及重项依据。具名静态检查与 Loom/Miri 保留 owner 的独立 stage 和模式。

影响图只用于开发阶段。未覆盖路径产生可见缺口：可界定 owner 时扩大到该领域及其重项，无法界定时广覆盖；共享类型、wire、持久格式和跨模块公共合同先扩大到已知受影响领域。修补映射，不能长期依赖扩大。重项由作者合入前本地运行或手动派发到 G16，结果或跳过理由写入 PR 验证记录；准入不检查这份作者记录。明确获批的 plan 必跑项仍必须执行。

```bash
# These commands prepare the canonical graph unless --prepared names validated inputs.
tools/ci/dev-verify.sh plan --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh run --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh resume --base origin/main --root /absolute/local/task-evidence
tools/ci/dev-verify.sh compose --base origin/main --root /absolute/local/task-evidence
```

根保存多个独占 attempt 和 composition；`resume` 重新读取当前声明与输入，重跑不满足或仍有清理义务的范围，不修改旧尝试。开发层保留逐项 Rust 测试与 workspace doctest 义务，原生工作区聚合阶段明确延后到 admission，准入/全量仍要求该阶段；聚合原始失败与未关闭清理事实继续可见。仅当整个待运行目标集合只剩 doctest 时，允许一次正常 native batch 附带重跑一个同身份、已满足的常规 nextest 单元；执行前记录具名例外、重复范围、精确筛选和输出引用，缺少合格单元则保留未运行。重复运行的新失败和旧清理未知照常阻断，不循环重试。首次先读 `plan` 的理由与缺口，使用真实 fixture publication/数据集前置。原生发现/声明校验、材料路径、只观察的 `confirm-cleanup` 和已知入口限制见 [CI README](../../../tools/ci/README.md#development-selection-and-evidence-composition)。基础 Rust 图、原生结果和进程回收的边界见下文[构建选择与控制授权](#构建选择与控制授权)。

新增不随进程退出消失的外部资源 owner，必须在首个副作用前登记，并在所有 shutdown/失败路径写出释放或清理失败；失去 owner、坏记录与未收束保留未完成。登记不是删除权限，复跑不能代替确认旧资源退出。具体 API、现有 binding-only 场景的 G5 缺口见 [System README](../../../tests/system-test-runner/README.md#声明与收束材料)。

## 准入与领域迁移

新增测试、workload、采样/比较定义、SQL 指令或架构检查前，先确认现有覆盖为何不足、产品性质、资产性质和最低充分层级、独立预期，以及输入、隔离、真实退出、维护 owner、成本、消费链路和频率。答案靠近原生定义；简单测试可由函数、断言和模块说明承载，无需逐例表单。一次性探索只有明确长期消费者后才按同样条件晋升源码资产。

SQL 文件保留必要的语义输入和断言，多步生命周期编排归 system owner；未知、非法或未实现指令不得静默成为无断言通过。现有指令拒绝行为由 TST-G13 迁移。依赖方向由 crate 结构与 review 维持，不新增依赖边界检查、源码形状守卫或产品前提检查。API 级约束使用所属 crate 的 Clippy `disallowed-*`，供应链使用 cargo-deny。产品前提优先由行为测试保护；没有行为测试时，在设计中写明它的定位与接受的代价。长期裁决见 [ADR-0190](../../adr/ADR-0190-dependency-direction-by-structure-and-review.md)。

产品修复与治理场景可以成对迁移，但先接受唯一领域 spec；产品 plan 与治理 plan 引用同一设计，划分切片并反链完成情况。没有领域 spec 时先细化并接受，不在两个 plan 各造一套测试设计。M04b 成对迁移先接受 G3 spec，NIG-1 成对迁移先接受 G12 spec；这不要求先完成整个领域。消费未交付接口时记录具体实现硬前置，原有 MEM/M07 等产品验收继续生效。

### 逻辑执行的实际资源行为

[ADR-0198](../../adr/ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)
的容量与退出判据沿原资源 owner 表达：真实 queue Full 可取消、同输入只处理一次，出队
归还 queue 位置与工作/结果析构实际退出分别取证；超时不能退款仍活工作。Unknown 保留原
journal、未决对象与数据，不要求已退出动作继续占运行位置。实际 buffer 的 old+new 授权、
receipt 交接/合并和最后 alias 退出须对应真实 backing；same Root 自持不足明确拒绝，不能
等待自己。Client 使用原窗口，Internal 计算位置与保留字节分别退出，不用通用峰值 floor
或 worker 最低数代替实际资源前提。

公平性场景须明确资源可用、依赖能够返回；所有共享 worker 永久阻塞时如实记录不可用。
保留既有单消息、完整结果与准确 BE 的 W≤P 约束，不再把类别/依赖域矩阵或递归对象图
invoice 当作产品 oracle。本节不新增测量、RSS 上界、准入阶段、全量门槛或静态 token
检查。组件测试与源码候选不证明原生调用面或最终全工作区验证已完成。

## 获批判据的 Git 快照

原生执行 owner 负责 workload、动作、合法输入、采样有效性、正确性断言、资源保护停止条件与实际退出；分析 owner 负责方法适用性、比较、效应量和不确定度。runner 不读取任务验收门限，不以历史报告判断当前运行资格。一次性实验的方法与输入引用已有不可变版本；本阶段不维护常设基准或持续测量协议。任务特有对照、门限和适用范围写入获批 plan，在候选确认性测量前固定。需要探索的先声明探索及后续判据确认，不能测后挑门限。

NovaRocks 的工作文档根按 [AGENTS §12](../../../AGENTS.md#12-project-development-workflow-and-knowledge-base) 解析；vault Git 仓库可能位于该项目文档根的上级。操作时先确认真实仓库根和仓库相对路径，不把机器路径固化为项目接口。

1. 用户明确批准准确磁盘版本后，将该 plan 标为 approved，核对正文及判据引用与批准内容一致。仅选该 plan，以及直接承载判据、需要固定的已接受设计/方法文件；源码协议可引用已有 commit。背景链接不递归快照。
2. 读取 vault 状态和索引，保留无关笔记及无关暂存内容。限定路径提交，不要求整库 clean，不做全库 add、reset、stash 或自动 push。若选定文件原来包含其他人的暂存修改，先区分该文件的授权内容，不能顺手覆盖。已有仓库可使用下面的路径限定操作；新文件仅对选定路径 add。
3. 提交成功后查看完整 commit 的文件清单和每个选定文件，确认文本符合批准版本，并核对无关工作树/索引与之前一致。提交失败或内容不符时，不能开展声称遵循获批判据的确认性测量；探索仍可如实记录。
4. 后续运行记录/报告引用 vault 仓库身份、完整 commit 和仓库相对路径。不要把 commit 回填到该快照自身制造自引用；后续进度可写工作文档，但旧判据引用保持不可变。实质修订重新批准并形成新 commit。

以下变量由实际仓库与选定文件填入，示意命令不代替上述内容核对：

```bash
git -C "$vault_root" status --short
git -C "$vault_root" diff --cached
git -C "$vault_root" commit --only -m "Record approved test criteria" -- "$plan_path" "$criteria_path"
# Record the full result in subsequent execution materials.
git -C "$vault_root" rev-parse HEAD
git -C "$vault_root" show --format=fuller --stat "$approval_commit"
git -C "$vault_root" show "$approval_commit:$plan_path"
git -C "$vault_root" diff --cached
```

选定文件集合可以只含 plan；不要为了示例中的第二个变量新增文件。被引用判据必须能从该快照或明确的不可变外部版本恢复，不能指向工作树“最新版”。本地 commit 仅固定批准文本，既不是 runner 准入锁也不是共享交付；正式证据 owner 还需保证相关 Git 对象和引用文件可恢复。

## 每次尝试的位置与独占

一次性实验默认放在源码版本库外：

```text
${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/<topic>/<attempt-id>/
```

先展开为实际绝对路径；topic 可以是产品问题或任务号。attempt-id 使用 UTC 时间加随机后缀，子目录以独占 mkdir 创建，已存在即失败，不复用或清空旧目录。下面是一次性的创建示例，不是要入库的目录分配器：

```bash
python3 - <<'PY_CREATE'
from datetime import datetime, timezone
from pathlib import Path
import os
import secrets

state = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
parent = (state / "novarocks/experiments" / "your-topic").expanduser().resolve()
parent.mkdir(parents=True, exist_ok=True)
attempt = parent / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4))
attempt.mkdir(exist_ok=False)
print(attempt)
PY_CREATE
```

在材料中记录实际路径，将临时脚本、输入/环境说明、实际命令与输出一起保存。可以明确选择其它实验根并承担同样保留责任；这不是新增环境变量、CLI 或目录管理机制。不默认写 `/tmp`，不在 `tests/benchmarks/<任务>` 或 `docs/testing/<任务>` 新建永久验收机器。

现有 runner 的原生 artifact-root、checkout 内 ignored logs/reports 仍允许。可通过新根满足独占的使用原生参数，如 system 的 `--artifact-root`；不能独占的明确记录存量缺口并交对应迁移。system `run_one` 对固定场景子目录用 `create_dir_all`，没有原子拒绝已存在目录的保证。调用者选新根是目前操作责任，本文不声称 runner 已强制执行。关联所有输出的真实位置，不提交运行产物进源码 Git。

## 保留、共享交接与清理

材料默认不自动过期，未解决失败继续保留。只有任务结论已审阅、正式引用和重分析所需脚本/输入说明已共享交接且核实可恢复后，才按记录的处置清理本地副本。未被正式引用的纯探索可在明确结束并记录取舍后清理。正式材料由存储 owner 记录访问、保留依据与处置；不能用删除 worktree 或定时清理代替。

本地路径只称本地证据。正式共享引用需有真实共享访问、保留 owner 以及所引用 Git 对象/输入/脚本的恢复条件；尚无共享交接时如实声明。本指南不自动上传、创建云服务或删除材料，凭据继续按既有规则脱敏。历史失败可显式用于分析与定位，不能成为新采集的隐藏输入；声明为长期 fixture 时按 fixture 自身合同维护。周期调度和共享证据存储尚未交付；TST-G7a/G7b 已取消，没有新的持续测量/共享历史服务承诺。干净工作树或本地 commit 不替代这些证据。

## 代表性原生报告映射

下表是当前写出端的维护说明，不是全仓资产索引或另一份运行权威。字段可表达某个事实与某次运行已写出该事实必须分开；以下仅核验源码，没有生成通过样例或运行集群/性能实验。正式报告优先引用实际原生输出，补充判据版本、缺失事实、材料关联和结论边界。

| 写出端/材料 | 当前能表达的事实 | 尚不能由它单独证明、后续责任 |
|---|---|---|
| system [scenario.rs `write_evidence`](../../../tests/system-test-runner/src/scenario.rs) → 场景根 `scenario-evidence.json` | `scenario`、`command`、开始/结束时间、`source_revision/source_dirty/source_tree_sha256`、runner identity/路径、Cargo.lock hash、平台、`actions/phase_observations`、runtime/binary/config 路径及 config hash、`cluster_size`、`launch_profile`、进程 launch identities、有效配置及 hash、`outcome/exit_code`、失败 diagnostics；当前 schema 5 | `launch_profile` 是 harness 的 `fault-scenario/performance` 配置，不是 Cargo profile。binary 路径与 runner identity 不证明各 FE/BE 实际能力；实际 role-build-information 单独保留。字段不保证每个阶段均被观察或此次已运行，各领域仍须提供性质/退出观察。 |
| system [runner.rs `run_one`](../../../tests/system-test-runner/src/runner.rs) → stdout/stderr、场景 evidence 与 runtime/log 根 | 选择由 registry/`--only` 决定；失败保留材料、动作、诊断和 rerun 命令。正常路径先检查 cluster shutdown 与 fixture teardown，追加 cleanup action，再写 Passed；失败路径先写 Failed，随后处理 cleanup 错误 | evidence 的 `exit_code` 按 outcome 合成 0/1，不是每个子进程退出码。schema 5 的 Failed 可能先于 cleanup 写出，不能单独证明收束；另读 schema 1 `scenario-teardown.json` 的 fixture/cluster 终态与资源登记。缺收束记录保持未知；部分 cluster 启动由对应 monitor 事实补足。打印的旧 rerun 根不能直接复用，复跑换新根。 |
| CI [logging.sh `ci_render_summary`](../../../tools/ci/lib/logging.sh) → `CI_SUMMARY` | 外层传入的 Status、开始/结束/耗时、Repo/Branch/Commit/Run dir、runtime/SQL config 与 MySQL port；stage、system scenario、SQL suite/case rows 的原生 status/耗时/log/artifact；可选 blocked、SQL failure classification、failure tail | summary 是外层记录的汇总，未写入的行不能补为通过；Commit 不等于二进制来源或能力，config 路径不证明有效配置。状态不代替实际进程退出/清理日志、不可变判据引用或共享保留。G2 补构建事实；实际 CI/领域 owner 与 G6b 保留运行边界；共享历史服务尚未交付。 |

Native build identity 的来源在薄 CLI build.rs 冻结并注入 Version，不是能力或发布认证。实际产物的 `--build-info` 由 [Server build_information](../../../novarocks-server/library/src/build_information.rs) 与两 runner 各自写出；owner 能力来自实际 crate 的编译事实，binary 的 `actual_cfg` 与 build-script 的 package 局部事实分列。未声明 profile、不可观测的最终 LTO/codegen/dependency overrides/额外 flags 保留 null/unknown；不能从源码 HEAD、target 路径、文件名、identity 或 runner profile 补造。`release_certification` 明确为 `not-certified-by-self-report`，字段存在不证明 G2 全部验收通过。

## 构建选择与控制授权

日常开发只有一个 `dev` 主测试图。规范入口是 [dev-test.sh](../../../tools/ci/dev-test.sh)：`prepare <receipt>` 同次 `cargo build --workspace --tests --bins --profile dev --locked --target-dir "$PWD/target"`，从开始包含 dev-dependencies 和真实程序；`test` 使用原生 nextest 逐测试进程执行，`list` 保存/显示原生发现，`doc` 单独运行 Cargo doctest。选择用 `-E 'package(...) & test(...)'` 等原生 filter，不把运行选择翻译成 Cargo 选包或另一份 feature 图。clippy 的 `--all-targets` 是额外静态检查范围，不扩大行为测试到暂停项、bench 或 example。窄 `cargo test -p <owner>` 适合内循环，选包会改变依赖统一和必要链接，甚至暴露窄图缺少的 feature 前置；原始失败保留，不能冒称完整规范图。

标准 `dev` 的 workspace 与非 workspace 普通目标均使用 `opt-level=1`，保留 debug assertions、增量编译、256 codegen units 和关闭 LTO；第一方使用 line tables，非 workspace 依赖不生成 debug symbols。host build-script/proc-macro 按 Cargo profile override 优先级处理：`package."*"` 覆盖非 workspace 成员及其 host 单元，workspace build script 仍遵循默认 build override；实际值以 Cargo 产物记录为准。该集合不能等同语义上的全部第三方。这个唯一默认组合由 G2a 的有界应用成本观察选择；成本观察不替代 native 正确性验收，原失败和未完成项单独保留。

主缓存属于当前 worktree 的 `target/`。实际 `debug_assertions` 由共享 build helper 投影为 `novarocks_dev_controls`：标准 dev 编入控制能力，标准 release 排除；不再通过镜像 feature 或声明 profile 改变能力。`NOVAROCKS_BUILD_PROFILE` 不参与构建来源或能力判断。Server CLI 保留 `novarocks-server` 包名，SQL CLI 保留 `tests/sql/runner/Cargo.toml` 和 `novarocks-sql-test` 身份；库分别为 `novarocks-server-application` 与 `novarocks-sql-test-library`。System CLI 包是 `novarocks-system-test-cli`，库保留 `novarocks-system-test-runner`。Git 来源仅薄 CLI 冻结，库不监听来源。

规范 helper 从 Cargo JSON 的真实 bin artifact 绑定 Server、SQL/System runner 和 error-manifest 的可执行路径与 SHA-256。最后一次相关 Cargo 后重新采集，并在消费前检查内容；缺路径、缺 required bin 或内容变化失败，不能回退旧程序。SQL 列举、执行和 error-manifest 检查直接使用已准备程序，不另起 `cargo run`。自报保存 stdout/stderr/实际退出；capabilities 据实际编译事实保留，`declared_profile=null` 与其它 unknown 不补造。自报不认证依赖最终 profile 或发布资格。

普通 SQL 和有有限控制需求的 SQL/System 使用同一个显式主 Server。SQL 只接受 `NOVAROCKS_BIN`，System 使用 `--binary`。原生 owner 按本次实际选中动作冻结角色会话、statement、目标和授权，启动后控制仍需准确匹配；编入能力、debug/profile、`fault-scenario` 名称、旧环境或裸测试 CALL 均不授予动作权。非破坏观察也不授予 wipe。开发 cfg 使通用控制接缝可见，不会自动启用 HMS/MySQL exact/closing/root 专项；这些模块仍需显式选择各自的 `mem-1-m07-*` feature，运行时还需准确授权及对应 fixture/binding。外部 fixture、私有历史矩阵和 required-but-not-run 仍独立报告，主图单测通过不代替完整原输入。

### 原生 Rust 执行、进程回收与原始结果

```bash
tools/ci/dev-test.sh list -E 'package(novarocks-memory)'
tools/ci/dev-test.sh test -E 'package(novarocks-memory) & test(=attribution_reconcile)'
tools/ci/dev-test.sh test -E 'rdeps(novarocks-test-support)' --test-threads 2
tools/ci/dev-test.sh doc
# Formal verification includes its existing governance gates.
tools/ci/local-full-ci.sh --cargo-only
```

nextest 日常使用原生最低版本要求 **0.9.148**，兼容新版不因自定义精确版本判断被拒绝；正式工具准备固定 0.9.148。缺工具、无效配置和必要选择为空明确失败，不静默回退旧串行入口。原生配置默认 4 slots、无重试、非 fail-fast、泄漏检测失败；Server smoke/cluster 以及进程机械测试使用各自的原生资源组，不能从 slot 推断整个进程树、内存预算或清理完成。watchdog 防止挂死，不续产品或 helper 的原始 deadline。

日常入口先准备原生 Cargo/nextest metadata，最后联合构建并核对实际 executable/hash，再前台 exec nextest，保留原生 TTY、输入和作业控制。正式批处理使用同一准备/绑定和运行选择，独占保存 nextest/JUnit、Cargo/doctest、源码/产物/配置事实及监护记录；普通断言失败后仍执行独立 doctest，取消、环境或清理失败则保留未运行原因。相关 Cargo 结束后再次联合准备并刷新程序绑定，后续 SQL/System 消费最后 receipt。原始结果和阶段完整性分别表达；消费者通过 [CI 执行材料接口](../../../tools/ci/README.md) 的 `execution-index.json` schema 1 读取，不解析内部人类日志或改写原生报告。

共享 `ManagedCommand` 从构造时就通过独立 monitor 启动 payload；monitor 持真实 Child、准确进程组和实际 wait，控制方失联后继续清理。`ManagedProcess` 的输出定位文件指向 run-owned 原始双流，不能再当 combined log 读取。完整行诊断的 1 MiB 界限只约束 `live_log_line`，原始输出、readiness 与完整 golden capture 保留自身合同。普通调用成功检查退出和日志断言后可显式释放；source clone 继续 pin 材料。生命周期完成不能证明测试断言已通过。

普通 nextest 首次受管操作才懒建原生 run ID 对应的材料租约，纯测试与 list 不建 root/guard。嵌套 runner 的外层测试先 `ensure_run_retention(deadline)`，随后保留原生身份及本地状态上下文；直接 Cargo/IDE/SQL/System 没有 nextest 身份时按调用进程归属。普通路径由 Rust 辅助程序维护，不引入 Python 依赖；这不等于准备阶段也无需 Python。材料默认保留最近 3 次已收束调用，默认窗口可通过正数 `NOVAROCKS_TEST_RUN_RETENTION_COUNT` 调整；活动 pin、未知结束、缺 Final 和未完成清理不被窗口淘汰，数量窗口不是磁盘硬上限。同次调用不能提前淘汰早期失败。

正式材料不受普通 release/GC 影响，继续遵守上文正式保留/交接规则。运行锁及状态目录只支持经实际 FD 检查的本地 APFS/ext/XFS/Btrfs/tmpfs；容器显式 bind-mount 支持的本地状态目录，overlay/网络/未知文件系统不降级。完整范围、嵌套示例、平台验证缺口和 macOS 按需排查入口见 [test-support README](../../../tests/test-support/README.md)。当前 native Linux x86_64 仍缺本轮生命周期/FD及真实消费者证据，文档和 macOS 定向通过不能替代该平台验收。

Hadoop local fencing 保留普通原生测试，内部 child helper 为 ignored 且只能由父测试 `--ignored --exact` 启动；缺输入失败。MinIO、MySQL、FoundationDB 和 Java interop 继续各自专用入口及真实前置，Paimon 暂停边界保留。Loom/Miri 保留专用工具链与 Tree Borrows 等条件。nextest 全量不自动扩展这些矩阵，也不接管 Docker、外部实例或所有 raw spawn 的生命周期。

### 兼容专项的选择

`local-full-ci.sh` 只运行原生 `--list-default` 集合，排除所有 `native-compatibility/*` 与其它显式专项；默认结果仅覆盖功能集合，不声称所有注册场景通过，也不准备兼容变体。`tools/ci/native-compatibility-ci.sh` 是独立入口：不传 selector 运行完整兼容集合，重复 `--only <exact>` 运行精确子集；未知、跨套件或空 selector 在 fixture/变体准备前失败。专项只准备和执行自己的程序/场景，不顺带运行 Rust/SQL/普通 System 全量。[手动 workflow](../../../.github/workflows/native-compatibility.yml) 调用同一入口。

身份兼容材料或比较、角色身份传播与准入/切换，或专项的构建、绑定、选择、授权接线发生实现/合同变更时，维护者按影响面选择兼容场景并记录理由；用户或 reviewer 显式要求时也运行。普通业务改动、普通提交仅更新 Git SHA/诊断 identity、来源刷新或重新编译不自动触发。场景性质、完整生命周期程序与 fixture 需求由 [System README](../../../tests/system-test-runner/README.md#兼容专项与程序需求) 说明，shell 只消费原生需求并集。原生场景声明和统一影响选择承接这些触发，开发阶段选中的重场景保留作者义务；G17 在容量和前置就绪后安排全量。独立兼容入口仍不自带 schedule 或 diff 选择，也不要求每个 PR 运行专项。

Compatible 使用主 workspace/tests/bins/dev 图，仅改变薄 Server CLI 的诊断 identity；OtherIsland 默认仅准备 Server binary 和必要 compatibility feature，P05 的原始观察决定是否调整选包，不能凭路径推断成本。两个变体串行复用主 target，各从 Cargo artifact 独立复制到 `target/variants/{compatible,other-island}/novarocks`，只保留最终程序与绑定材料；不用硬链接/软链接、不克隆 Cargo cache。准备后恢复主输入并刷新主图，重新绑定主程序、自报和 hash 后才启动消费者。原始准备失败优先保留，恢复失败另列，不覆盖原退出。

### IDE 与缓存维护

IDE 默认 Cargo/libtest 单例按钮仍可局部调试，并获得库级 monitor 回收；它不自动提供 nextest 逐测试隔离、规范主图或正式汇总。不要把旧的 Cargo `test.overrideCommand` 参数模板机械转给 nextest。需要规范 workspace 选择时，建立调用绝对 worktree 路径的外部 task，例如 VS Code `tasks.json`；将示例路径替换成实际 worktree：

```json
{
  "version": "2.0.0",
  "tasks": [{
    "label": "NovaRocks native exact test",
    "type": "process",
    "command": "/absolute/path/to/NovaRocks/tools/ci/dev-test.sh",
    "args": ["test", "-E", "package(novarocks-memory) & test(=attribution_reconcile)"],
    "options": {"cwd": "/absolute/path/to/NovaRocks"},
    "problemMatcher": []
  }]
}
```

原生 `test(=...)` 精确选择及 `package()/binary()/rdeps()` 按 nextest 语义组合。内部 ignored helper 不作为 IDE 全量或 `--run-ignored all` 的普通目标；公开 ignored fixture 测试只有在其真实前置满足后才精确运行。保留 `rust-analyzer.cargo.targetDir=true` 的私有检查缓存设置，不覆盖用户其它编辑器配置。

`cargo.targetDir=true` 为 flycheck/build-script/proc-macro 使用 worktree target 下私有目录，不能让 overrideCommand 强制主 target。若覆盖 check 命令，必须先从编辑器有效命令确认实际目录，并通过 `tools/ci/dev-test.sh check --target-dir <actual-IDE-dir>` 显式透传。规范测试 runner 仍用主 target；IDE check 是独立编译单元，有额外磁盘和重编成本。不要覆盖用户其它编辑器设置。新 worktree 冷缓存和底层库改动的必要重链接是合法成本；历史 18s/32s 不是现机收益，最终 opt 选择只引用实际尝试材料。

主 target 仍用于增量开发则保留。停止编辑器和构建、核对 owner/无消费者及材料交接后，用 `cargo clean --target-dir <exact-worktree-target>` 清理主缓存；仅回收 IDE 时给准确私有目录。兼容程序用准确 `target/variants/compatible` / `other-island` 目录回收，不把它们当 Cargo cache。finish/archive 承担临时变体处置和失败材料保留责任，不删除其它 worktree、共享 fixture 或未知缓存；未解决失败继续保留，历史报告不回写成新 PASS。

当前 MV 测试保持受控过渡，LNP-3A/LNP-3D、storage-contract 和 system 现有 SQL/expected/独立 oracle 不重写，旧 CALL/directive 禁止扩展。whole-family wipe 绑定本次 FE launch 与 MV Accelerator family，一次认领、隔离串行，先由原 MV owner 停止 admission 并实际 join 后台；不能把传入 MV 名当逐 MV 权限，也不能删 whole StateStore 代替。

| 唯一后续产品 owner | 成对迁移的现有性质与载体 |
|---|---|
| 7B3 | LNP-3A、LNP-3D 对应 case、`mv/lake-publication-restart-rebuild`：非聚合冷恢复、配置/paused、known-committed、Unknown/迟到提交与管理接续 |
| 7B4 | spine SUM/COUNT、T/S/P 聚合状态、聚合冷恢复及后续刷新；保留缓存 restart 不冒充缺失缓存恢复 |
| 7B5 | V/S 身份、view 发现与 CREATE/DROP、fixture/REST 路由、实际 materialized S 读取与 SQL fallback；适配前序已迁移场景 |
| 7B11 | SQL/system 独立 document graph、shared-document-revisions、文档格式；替代保护通过后退出只服务旧图形的 package/provenance 消费者 |

每个 owner 按自身 accepted spec/approved plan 取得替代保护后逐项退役，最后消费者退出再删共享载体。G2 不新增启动期缓存缺失、管理收束或通用擦除 API，不代表上述产品任务已完成。

## 当前功能资产、成本与频率

| 性质 / owner | 入口与成本 | 相关改动与频率 |
|---|---|---|
| result support、FS corpus、MV owner | 对应 Rust crate；本地、无集群 | 模块/helper/输入改动必跑，沿已有 Rust CI |
| 慢端、并发结果与原查询期限 | system 精确 selector；1FE+3BE、显式 | writer/buffer/ACK/取消/helper 改动必跑；领域维护者每周手工轮次 |
| range/delete/planning/DOP | system 显式功能组；真实 fixture 和 1FE+3BE，成本较高 | provider/cache/delete/range/admission/fixture 改动必跑；每周相关领域轮次，缺输入如实记录 |
| MV/OPTIMIZE | SQL correctness 的具体 case | publication/maintenance 改动必跑，沿既有 suite |
| SSB/TPC-H/TPC-DS | 原生 SQL 三套 `explicit_only`；高成本 | runner/输入绑定改动必跑三套；领域 SQL 按相关性选择，每周手工完整数据集轮次 |

这些周期是手工维护责任，G17 的正式全量调度尚未交付；没有真实运行就不记录周期通过。触发由原生 impact 声明维护，本表说明成本与责任，不是第二份选择清单。完整发现与默认执行分别由 `--list/--list-default`（system）和 `--list-suites`/suite manifest（SQL）定义；机器选择均消费 `--list-declarations`。`performance` launch profile 只选择干净的原生 harness 配置，支持 release binary 并拒绝 fault 注入，不是性能实验。相关指南见 [system](../../../tests/system-test-runner/README.md) 与 [SQL](../../../tests/sql/README.md)。

SSB/TPC 输入 owner 显式供给 READY，消费者仅 check，不在失败后自动 ensure。SQL 正常成功、语句错误、结果 mismatch 和清理失败分别保留原生返回值、case/expected/actual/diff、FE/BE 日志和实际退出；耗时只作普通运行说明，不转成测量 CSV 或比较门。FE preparation 运行级路由和计时钩子退出，独立鉴权的 statement observation 保留功能消费者；这不表示 G2 发布能力门控完成。

## 失败与结论边界

原始失败、复跑结果、归因和已批准处置分别记录。单跑通过只证明该次未复现；时间常量加并发负载也不能单独排除产品竞争。调查应保留实际负载/进程观察、失败阶段和相同条件下的比较证据；未知原因就保留未知，不以测试名或一次成功归因噪声。全量原始失败不能被定向通过覆盖，历史已批准失败处置也不能改写本次原始结果。

缺终态保持未完成；清理失败、必跑覆盖未执行、另行豁免及构建事实缺失均保留原状并缩小结论。采集成功只证明取得数据，没有适用比较方法时明确未评估。all-in-one smoke、定向功能检查、本地机制运行与真实 1FE+3BE/Linux 性能验收分别表达，不用全局 PASS 掩盖它们的边界。

G7c 本次批准的剩余 SQL 与单测失败例外只影响本次退役推进条件：保留各次 raw FAIL、具体失败集合、来源和批准处置，不改 golden、断言或原始退出码，不称全套 SQL/UT 已通过。该例外不豁免新的语句观测、生命周期、输入校验与清理性质，也不构成后续任务的全局 baseline 豁免。最终 P09/workspace 结论由真实独立结果填写。
