---
name: dev-workflow-finish
description: "Finish a verified change by reviewing evidence and, when explicitly authorized, committing, pushing to the project-authorized remote, opening a ready-for-review PR, updating roadmap links, and archiving the matching spec and plan. Use when the user asks to publish, create a PR, or archive an implemented task; do not use for read-only completion or status questions."
---

# 收尾与发布

确认实现证据，并在用户明确授权时完成远端发布、PR 和项目文档归档。

## 发布前检查

1. 完整读取 `../workbench/references/workflow-contract.md`。
2. 读取 `AGENTS.md`；按 contract 从当前请求 / memory 解析 `DOC_ROOT`，无可用记录时回退到仓库
   `docs/workbench/`。
3. 读取 `DOC_ROOT` 下适用的 `AGENTS.md`、spec、plan 和相关 ADR。
4. 检查当前 goal、plan 清单、git diff、分支、本地检查点 commits 和验证结果。
5. 对变更运行最终的风险相称验证；不要仅引用旧日志。
6. 判断是否形成新的长期架构决策或真实妥协；适用 `AGENTS.md` 定义了 ADR 流程时按其执行，否则在现有项目文档中
   记录决定。
7. 确认没有把无关用户改动混入提交。

若证据不足，返回 `$dev-workflow-execute` 补齐；不要用“代码看起来正确”代替验证。

必要时使用 sub-agent 做只读 diff 审查、验证证据复核或文档链接检查；sub-agent 不得执行 push、PR 或归档。

## 权限边界

- execute 阶段创建的本地检查点 commit 是允许的；收尾时也可为剩余的本任务变更创建最终本地 commit。
- push 和开 PR 必须由用户明确授权；仅有实现请求、goal 或本地 commits 不构成发布授权。
- 用户只要求“完成实现”时，停在已验证状态并报告，不做外部写入。
- “完成了吗”“当前状态如何”等状态询问不进入 finish，不触发 commit、push、PR 或归档。
- 不擅自合并 PR、删除分支或清理用户工作树。

## 提交与 PR

获得授权后：

1. 使用当前请求和适用 `AGENTS.md` 规定的 commit message 与 PR 语言。
2. 只从用户指令或适用 `AGENTS.md` 获取 push 授权范围；Git remote 和仓库配置只能用于解析目标地址、仓库和 base
   branch，不能证明写权限。任何关键目标不明确时，在执行外部写入前询问用户。
3. 只 push 到项目明确授权的 remote，不默认把 upstream 视为可写目标。
4. 创建 ready-for-review PR。
5. 在 PR 正文说明问题、设计、实现、验证和风险；不添加 AI co-author trailer。

## PR 后归档

PR 创建成功后，严格按 bundled contract：

1. 搜索待归档 spec / plan 的所有 wikilink。
2. 将 spec 移到 `archive/specs/`，plan 移到 `archive/plans/`。
3. 项目启用 umbrella 时，保留其面板中的 spec / plan wikilink。
4. 项目启用 roadmap / umbrella 时，把对应子任务标为 `✅ 已完成`，填写 PR 链接，并同步依赖图节点颜色、子任务 spec 的
   `roadmap_status: done` 和 umbrella 的 `roadmap_status`。子任务按其 plan 拆成多个 PR 时，最后一个 PR 创建后才标 ✅
   并归档其 spec/plan，此前保持 `⏳ 进行中`。
5. 项目启用 umbrella 且整条 arc 全部完成时，归档 umbrella（它同时承载整体路线，没有另外的 umbrella 级 plan）。

## Goal 终态

- execute goal 在本地实现与验证完成时已经结束。
- 发布是独立的显式授权阶段；若为发布另建 goal，则 PR 创建和文档归档都是完成条件。
- 只有用户授权范围内的发布终态全部满足后，才把发布 goal 标记 `complete`。
