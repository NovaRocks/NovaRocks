---
name: ops-capture
description: "Record durable engineering knowledge into the project knowledge base: a reproducible customer scenario (scale, topology, environment parameters, how to reproduce it scaled-down), a first-hand debugging case (symptom, investigation path, root cause with file:line, fix, recurrence check), or an architecture decision record with its rejected alternatives and accepted compromises. Use when the user asks to write something down, save a lesson, pin a scenario, or record why a decision was made — and proactively offer it right after finishing a root-cause investigation, a reproduction run, or a design ruling, because knowledge is captured only on request and is otherwise lost."
---

# 沉淀工程知识

把一次调查、一个现场形态或一个设计裁决写成可检索的长期条目。不要在这个 skill 中重述契约规则。

## 加载契约

行动前：

1. 完整读取 `../workbench/references/ops-contract.md`；它是知识半区的唯一流程源。
2. 读取仓库根目录 `AGENTS.md`。
3. 按 contract §1.1 解析 `DOC_ROOT` → `OPS_ROOT`；写 ADR 时另按 §1.2 探测 `ADR_HOME`，
   命中项目内目录时**先读该目录的 README 并以它为准**。
4. 按 contract §8 检索已有条目：同一问题的新发现补进既有条目，不另开一条。

## 流程

1. **判定类型**（contract §3.2）。判别标准是「将来会怎么用它」：
   要拿来复现 → 场景；要拿来匹配症状 → 案例；要拿来防止重提已否决方案 → ADR。
   一次调查同时产生多类时分别立条目并 `related` 互链，不要塞进一条。类型不明显时问用户。
   **打算写 ADR 的，先过 contract §7.1 的反事实检验**：假设引发这次调查的故障从未发生，
   这条 ADR 还值得存在吗？不值得就是 CS，不是 ADR。
2. **抽取字段**。只从**本会话已验证的事实**中抽取。缺的字段留空，**绝不编造**；
   整条未经验证时标 `status: unverified` 并在正文开头说明哪部分未验证。
3. **校验 `symptoms`**（contract §4）：必须同时含**用户口吻**与**机器特征**各至少一条。
   只有内部术语的条目将来检索不到——缺哪类就补哪类，补不出来就问用户现场是怎么描述的。
4. **分配 ID**（contract §3.3），按对应模板写卡：
   `assets/scenario-template.md` / `assets/case-template.md` / `assets/adr-template.md`。
5. **写正文**。标注「最有价值」的那几节不得敷衍——它们是这条条目存在的理由：
   - 场景：**复现方案**（缩比策略及其理由）
   - 案例：**定位路径**（怎么查出来的，含被证伪的假设）与**复发检测**（30 秒内怎么确认）
   - ADR：**接受的妥协**与**何时重新评估**；另外「背景」要写成**模型说明书**而非调查记录，
     「选项」要针对整类问题并标明否决性质（设计 / 成本 / 待评估），
     「裁决」要产出具名可复用的规则（contract §7.1–7.3）
6. **同步索引**：`OPS_ROOT/INDEX.md` 追加一行；新增组件标签补进词表。
   仓库内 ADR 改为按其 README 的领域分组加索引行。
7. **执行完成检查**（contract §10；ADR 另加 §7.6 self-check），报告落盘路径。

## 何时主动提议

知识只在被要求时才会沉淀，否则就丢了。以下时刻**主动提议**沉淀（提议即可，不要擅自写）：

- 刚完成一次根因定位，尤其是走过弯路、排除过错误假设的；
- 刚跑通一次复现，尤其是找到了可用的缩比手法；
- 刚裁决一个设计问题，尤其是否决了看起来合理的备选方案；
- 刚查清一个「默认值咬人」的配置项；
- 用户说「原来如此」「这个坑」「难怪」这类表示刚建立新认知的话。

已有条目覆盖同一问题时，提议**补录**而不是新建。

## 边界

- 只写知识条目。不改代码、不改 spec / plan、不改 `DOC_ROOT/workflow/` 下任何文件。
- 不做检索判定——那是 `$ops-lookup` 的职责。
- ADR 落项目内目录时，该目录 README 的规则优先于 contract；两者冲突时以 README 为准并说明。
- 用户明确拒绝沉淀时不要反复提议。
