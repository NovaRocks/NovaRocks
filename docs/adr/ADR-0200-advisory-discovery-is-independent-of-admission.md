---
id: ADR-0200
title: "Advisory discovery is independent of admission"
domain: [crate-boundary, test-capabilities]
status: active
supersedes: []
superseded-by: null
date: 2026-10-28
provenance:
  - "discussion: 2026-10-28, accepted separation of advisory discovery from admission"
related: [ADR-0188, ADR-0189, ADR-0190, ADR-0170]
code-anchors:
  - "tools/ci/local-full-ci.sh (ci_list_stages, run_cargo_gates, ci_execute_selected_stage)"
  - "tools/ci/dependency-advisories.py (native advisory command and summary)"
  - ".github/workflows/dependency-advisories.yml (dependency-advisories)"
  - "deny.toml (advisories)"
---

## 问题

依赖安全公告使用仓库外、随时间变化的输入时，如何持续提供真实发现，同时使合入准入不受公告更新或获取故障影响？

## 背景与执行事实

Cargo 的依赖要求、锁定图与供应链政策属于仓库输入；RustSec 公告库与 registry index 的 yanked 状态随时间变化。即使源码和锁文件相同，公告检查也可能得出不同结果。cargo-deny 的 `check advisories` 同时消费这些外部事实和 `deny.toml` 的精确例外；配置中的 `unused-ignored-advisory = "deny"` 使失效例外可见。

| 原生 owner / 实体 | 输入与责任 |
|---|---|
| `Cargo dependency policy` | `run_cargo_gates` 中的 `bans licenses sources`，保留准入和 fail-fast 行为 |
| `Cargo dependency advisories` | `ci_list_stages` 声明为 `full`，由 `ci_execute_selected_stage` 派发同一原生命令 |
| `dependency-advisories.py` | 执行 cargo-deny、保留真实双流和退出，分类摘要并记录所观测的公告库 commit |
| `dependency-advisories` workflow | PR 可见检查与人工发现入口，权限只读，无自动 issue、无定时任务 |
| `dev-verify` / 全量层 | 依原生 domain 选择或覆盖公告阶段，不建立另一套安全选择规则 |
| 维护者与依赖 owner | 审阅发现，选择兼容升级或有明确退出条件的精确例外 |

检查的非阻断性由 GitHub required-check 配置决定，不由工具退出码或摘要决定。ADR-0188 的真实 PR 状态规则、ADR-0189 的正式来源与上线顺序、ADR-0190 的通用供应链职责以及 ADR-0170 的 registry 公告可见性继续有效。工作流代码存在不证明远程触发或设置已经验收。

[cargo-deny-action 的固定版本推荐管线](https://github.com/EmbarkStudios/cargo-deny-action/blob/9c67a826e39827395bfa619cd85cb2aaeb43cfaa/README.md) 将 advisories 与 bans/licenses/sources 分开，以免新公告突然阻断 CI。本设计采用职责拆分，并按 ADR-0188 保留失败检查的真实状态。

## 考虑过的选项

1. **最新公告库硬阻断准入。设计否决。** 可以立即拦截已知风险，但相同仓库输入会因外部更新改变准入结果，获取故障也会阻断无关改动，与准入不消费动态公告事实的约束冲突。
2. **固定公告库版本，另行 bump。成本否决。** 可以同时获得固定输入和公告阻断，但 cargo-deny 0.20.2 没有固定公告库 revision 的原生接口，需要自建获取、版本校验、陈旧判断与更新责任。当前接受非阻断发现，避免维护第二套数据库生命周期；上游原生能力成熟时重评。
3. **只阻断依赖差异新引入的公告。设计否决当前的在线准入形态。** 仍在准入读取最新外部库，不能消除相同输入结论变化；cargo-deny 也没有原生差异模式。若同时改为固定数据库输入，则属于另一个待评估的方案，而非已被证明不可行。
4. **公告检查独立于准入，保留 PR 与人工入口。采用。** 准入只运行稳定政策项，发现仍取得最新公告并显式失败；代价是安全发现交由评审和维护者处理。
5. **用 `continue-on-error` 保证非阻断。设计否决。** 它把原生失败包装为成功。非阻断由不列入必需检查表达，失败必须保留。
6. **定时扫描并自动开 issue。待评估。** 能减少人工触发遗漏，但需要成熟的 CI 调度、处置责任和额外写权限；当前只交付只读入口。

## 裁决

1. **稳定政策准入规则。** 准入与 PR 完整验证共用的 `--cargo-only` 链只运行 `bans licenses sources`；该阶段继续 fail-fast，不从公告库可达性或公告结果推导成功与失败。
2. **独立发现规则。** 公告检查是 `full` 层原生阶段，PR 检查和人工入口只调用同一阶段。本地与开发影响选择消费同一声明；全量运行公告阶段。该检查不得列入必需检查或合并组准入。
3. **原始结果规则。** 保留 cargo-deny 双流和真实退出码；摘要区分通过、公告发现和输入错误，单列可移除例外。输入错误与公告同时出现时继续展示输入诊断，不因摘要分类改变退出码。
4. **输入追溯规则。** 摘要记录实际公告库 commit、提交时间和获取时间；读取失败或目录不唯一时保留 unknown，不猜测。此记录只用于追溯，不作为版本固定、判据或复用资格的替代证明。
5. **最小权限规则。** 发现 workflow 只使用 `contents: read`，不自动开 issue，不设定时器；摘要和原始材料即使失败也尝试发布。远程触发和必需检查设置按独立上线步骤核对。
6. **传递 unsound 可见规则。** `unsound = "all"` 保留。第三方代码可能在任意依赖深度进入同一进程，是否可达应按公告触发条件逐条分析，不能按依赖深度排除。公告发现不阻断准入后，扩大报告范围的代价属于发现处置，不能靠缩窄范围隐藏问题。`unmaintained = "workspace"` 不随本裁决改变。
7. **例外单一来源规则。** 两个阶段读取同一 `deny.toml`；不复制例外、不过滤原生发现、不用 path 替换隐藏 registry 公告。依赖处置仍由对应 owner 负责。

## 接受的妥协（诚实记录）

引入有漏洞依赖的 PR 可以通过准入并被合入；非必需 PR 检查即使失败，也只提供评审信号。这个窗口是明确接受的风险，不能称为安全门禁已覆盖。人工入口不是持续监测，没有人触发和处置就可能延迟发现；PR 上的检查也不能保证有人响应。

在线公告检查本身仍可能因网络、registry index 或公告库故障失败。摘要只区分已观察事实，不修复输入，也不保证数据库绝对新鲜。stage 身份不含在线库，开发层同 root 的复用可能沿用较早观测，必须保留其时间与数据库 commit；要发现最新事实时应建立新尝试。全量层仍如实保留发现失败。

固定数据库的替代方案可以提供更强的输入稳定性与阻断能力；当前未选是因为工具与生命周期维护成本，并非认为安全性更差。读取本机数据库的 commit 只覆盖 cargo-deny 推导目录可唯一确认的情况，无法确认时接受追溯事实缺失。

## 何时重新评估

- CI 环境与处置责任成熟到可承担定时任务时，评估调度和通知；自动 issue 单独明确写权限及去重、关闭责任。
- cargo-deny 提供原生 revision 固定、数据库新鲜度或差异检查能力时，重评固定数据库与 bump 成本，以及固定输入上的差异阻断方案。
- 有漏洞依赖经 PR 合入并造成实际风险，或非必需检查长期无人响应时，重评非阻断发现的可接受性与维护者响应方式。
- 上游改变公告、unused ignore 或 yanked 诊断结构时，复核分类与原始结果保留；工具升级时同步两个 workflow 的固定版本。
- 产品运行方式或依赖可达性分析能力变化时，重评 `unsound = "all"` 的范围；不能仅因报告增加而缩小范围。
