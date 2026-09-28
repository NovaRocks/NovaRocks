---
name: workbench
description: "Route engineering work through explicit stages, and maintain a durable engineering knowledge base. Development half: explain concepts read-only, discuss the problem, write an accepted spec, persist and approve an implementation plan, execute under a persistent goal, verify, then publish and archive only when separately authorized. Knowledge half: capture reproducible customer scenarios, first-hand debugging cases, and architecture decision records; look them up by symptom when a problem recurs. Use when the user asks to understand, start, continue, or drive a development task, when the correct stage is unclear, or when engineering knowledge should be recorded or retrieved."
---

# Workbench

把工程工作路由到正确的阶段或知识操作，并守住阶段门。不要在这个 skill 中重新实现各阶段的具体方法。

## 两个半区

同一个 `DOC_ROOT` 下并列两个半区，性质不同，不要混用：

| 半区 | 目录 | 性质 | 契约 |
|---|---|---|---|
| 开发 | `DOC_ROOT/workflow/` | **要做的事**：有生命周期，开 PR 后归档 | `references/workflow-contract.md` |
| 知识 | `DOC_ROOT/ops/` | **已知的事**：无生命周期，只增补与修正 | `references/ops-contract.md` |

知识条目可以反链到它对应的 spec / PR（包括已归档的）；反之 spec 不因引用知识条目而改变其生命周期。

## 加载契约与解析文档根

在任何动作前：

1. 完整读取本次路由方向对应的 contract；它是该半区的唯一流程源。
   开发阶段读 `references/workflow-contract.md`；知识操作读 `references/ops-contract.md`。
   两个半区都涉及时，两份都读。
2. 读取仓库根目录 `AGENTS.md`。
3. 按 contract 的优先级解析 `DOC_ROOT`；memory 没有可用候选时，使用仓库根目录下的 `docs/workbench/`。
   `DOC_ROOT` 是**项目文档根**，其下并列 `workflow/` 与 `ops/`，不要把 `DOC_ROOT` 解析到其中任一子目录。
4. 读取 `DOC_ROOT` 及目标子目录中任何更具体的 `AGENTS.md`。
5. 检查当前代码、活跃文档和归档文档；不要仅凭对话记忆判断当前阶段。

不要把解析出的机器相关路径写回 skill。Memory 只负责定位；bundled contract、适用的 `AGENTS.md` 和当前代码
共同定义实时规则。

## 路由

| 当前状态或用户意图 | 调用 |
|---|---|
| 主要目标是从基础理解技术概念、内部机制、代码 / 架构 / plan 变化或数据流 | `$dev-workflow-explain-technical-concept` |
| 问题、边界或方案尚未讨论清楚 | `$dev-workflow-discuss-design` |
| 设计已明确接受，但尚无 spec | `$dev-workflow-write-spec` |
| 多子任务 arc：要写或修订 umbrella 的整体路线（“umbrella plan”） | `$dev-workflow-write-spec`（路线写在 umbrella 内，不另建 plan） |
| Umbrella 已接受，推进下一个子任务 | spec 仍是骨架：`$dev-workflow-write-spec` 细化；已细化：`$dev-workflow-plan` |
| 已有 accepted spec，但尚无 approved plan | `$dev-workflow-plan` |
| 已有 accepted spec + approved plan，用户要求实现 | `$dev-workflow-execute` |
| 实现已验证，用户明确要求提交、开 PR 或归档 | `$dev-workflow-finish` |
| 刚查清一个问题 / 复现了一个场景 / 裁决了一个设计，值得沉淀 | `$ops-capture` |
| 遇到症状，需要知道我们是否处理过同类问题 | `$ops-lookup` |

如果用户显式指定某个阶段，直接进入该阶段；不要强迫完整重走前序流程，但必须检查该阶段所需输入是否存在。
“完成了吗”“当前状态如何”等状态询问属于只读状态报告，不进入 finish，也不构成 commit、push、PR 或归档授权。
技术讲解是可以从任意阶段进入的只读旁路，不改变 spec、plan、goal 或发布状态；讲解完成后返回原阶段。
知识操作同样是可以从任意阶段进入的旁路：`$ops-lookup` 只读；`$ops-capture` 只写 `DOC_ROOT/ops/`
（以及项目自维护的 ADR 目录），二者都不改变当前开发阶段的状态。

## 状态机与阶段门

```text
discussion --设计被明确接受--> spec --spec 已确认--> plan drafting + persistence
persisted plan --plan 被明确批准--> goal execution
goal execution --验收证据充分--> verified
verified --用户另行明确授权发布--> PR + archive
```

多子任务 arc 只有一份 arc 级文档：umbrella 同时写设计与整体路线（主稿），用户接受后，子任务逐个走上面的状态机；
接受 umbrella 不授权执行任何子任务。

只设置两个常规人工门：

1. **设计接受门**：用户明确接受问题定义、目标、非目标和关键设计决策。
2. **计划批准门**：用户明确批准已经落盘的实现计划。

进入执行后，不因普通实现困难、测试失败、耗时超预期或局部代码选择而停下。仅在 `$dev-workflow-execute`
定义的重大决策点暂停。

知识半区没有人工门：捕获与检索都不改变代码或发布状态。但 `$ops-capture` 受诚实性规则约束
（只记录已验证事实，未验证内容必须标注），详见 `references/ops-contract.md`。

## 连续执行

当用户明确要求端到端完成：

1. 在每个阶段完成后自动进入下一阶段。
2. plan 阶段直接在当前可编辑模式中把计划写入 `DOC_ROOT/workflow/plans/`；不要要求用户切换 Codex Plan mode，
   也不要只在对话中保留计划。
3. 计划先以 `status: draft` 落盘并在原文档中迭代；用户明确批准该版本后改为 `status: approved`，再开始实现。
4. 将计划组织为依赖清晰、尽可能可并行调度的 task graph，以便 execute 阶段使用 sub-agent。
5. 实现开始时创建 goal；在 goal 的终态条件真正满足前持续工作。
6. 允许在任务本地开发分支创建检查点 commit；execute 阶段始终禁止 push 和 PR。
7. 只有用户另行明确授权发布后，才进入 finish 阶段执行 push / PR。

## Sub-agent 使用

全流程均可在确有必要时使用 sub-agent，例如并行代码调研、独立证据核验、互不重叠的实现切片或高风险改动审查。

- 主 agent 保持阶段所有权，亲自读取适用的 skill、项目契约和最终证据。
- 只委派边界清晰、可独立完成的子任务；不要为形式而拆分。
- 合并 sub-agent 结论前对照当前代码核实。
- sub-agent 不得绕过设计接受门、计划批准门、Goal 终态或发布权限。

## 范围纪律

- 技术讲解保持只读，先交付用户请求的解释；不得把理解请求当成 spec、plan 或实现授权。
- 讨论阶段保持只读。
- spec 阶段只写设计文档，不写 plan 或代码。
- plan 阶段只规划，不实现。
- execute 阶段实现与验证，可创建本地检查点 commit，但明确禁止 push 和 PR。
- finish 阶段只在用户明确授权后 push、开 PR 和归档。
- 设计变更回退到讨论阶段；不要在实现中静默修改 accepted spec。
- `$ops-lookup` 只读，不写任何文件；未命中时如实声明，不得把「疑似」表述为「命中」。
- `$ops-capture` 只写知识条目，不改代码、不改 spec / plan；知识条目不写进 `workflow/`，
  spec / plan 也不写进 `ops/`。
