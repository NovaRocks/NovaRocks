---
name: dev-workflow-discuss-design
description: "Investigate and discuss a development problem until its current behavior, scope, constraints, alternatives, and major design decisions are clear. Use before writing a spec for a feature, architecture change, roadmap item, or refactor; also use when implementation reveals that an accepted design is invalid or materially incomplete."
---

# 设计讨论

以代码证据为基础把问题讨论清楚。此阶段只读，不写 spec、plan 或代码。

## 准备

1. 完整读取 `../workbench/references/workflow-contract.md`。
2. 读取 `AGENTS.md`；按 contract 从当前请求 / memory 解析 `DOC_ROOT`，无可用记录时回退到仓库
   `docs/workbench/`。
3. 读取 `DOC_ROOT` 下适用的 `AGENTS.md`，并搜索相关设计、计划、归档文档和仓库 ADR。
4. 检查当前分支与代码；对可能漂移的结论重新核实。

必要时使用 sub-agent 并行调查互相独立的代码路径、历史实现或外部对照；主 agent 必须复核证据并主持最终设计裁决。

## 建立问题模型

按以下顺序推进：

1. 用一句话定义用户实际要解决的问题。
2. 描述当前行为，并给出 `file:line` 代码证据。
3. 区分：
   - **事实**：当前代码或可复现实验已经证明；
   - **怀疑**：仍需证据；
   - **提案**：尚未接受的设计选择。
4. 明确目标、非目标、约束和验收结果。
5. 找出必须由用户裁决的重大决策。
6. 给出 2–3 个真正不同的方案、取舍和推荐理由；不存在有意义的替代方案时，不凑数。取舍按 contract 第 2.2 节：
   长期路线优先，先对照业界做法，现有类型与 ADR 不构成保留理由，新增硬上限写明依据。

按 contract 第 2.1 节明确正常输入前提与失败边界。合法但罕见的输入和运行期资源/I/O 失败需要处理；违反契约的
bug、错误数据或人为畸形格式直接拒绝、报错，实际 bug 修复根因，不为它们发明特殊兼容或恢复架构。

## 对话方式

- 使用当前请求和适用 `AGENTS.md` 规定的沟通语言。
- 每次只提出一个会改变设计方向的关键问题。
- 先给已知证据和为什么需要这个决定，再问问题。
- 用户已明确给出的决定不重复询问。
- 对局部、可逆、不会改变外部契约的工程选择作出合理判断，不上升为用户决策。

## 设计接受门

只有同时满足以下条件，才把讨论标为完成：

- 问题定义、目标和非目标明确；
- 当前行为与缺口有代码证据；
- 外部契约、所有权边界、失败语义和适用的生产拓扑语义已经说明；
- 主要替代方案的取舍已讨论；
- 没有未决的重大决策；
- 用户明确表示接受当前设计或要求据此写 spec。

结束时输出一份紧凑的“已接受设计摘要”，包含：

- 问题；
- 目标 / 非目标；
- 已接受决策；
- 验收边界；
- 关键代码证据；
- 仍属实现细节、可留给 plan 的事项。

若实现阶段发现 spec 不成立，保留证据并回到本阶段；不要在代码里悄悄改变设计。
