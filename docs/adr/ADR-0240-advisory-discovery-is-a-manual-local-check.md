---
id: ADR-0240
title: "Advisory discovery is a manual local check"
domain: [crate-boundary, test-capabilities]
status: active
supersedes: [ADR-0200]
superseded-by: null
date: 2026-11-18
provenance:
  - "discussion: 2026-11-17, advisory checks leave PR, admission, and verification"
  - "PR: #1315 showed a non-required latest-advisory check failing on an unrelated optimizer change"
related: [ADR-0200, ADR-0188, ADR-0189, ADR-0190]
code-anchors:
  - "tools/ci/dependency-advisories.py (local advisory command and summary)"
  - "deny.toml (advisories)"
  - "tools/ci/local-full-ci.sh (run_cargo_gates)"
---

## 问题

公告检查读取随时间变化的输入（RustSec advisory-db 的 HEAD，以及本机 Cargo index 的 yanked 缓存）时，如何保留发现和修复，同时不让这些输入改变 PR、准入、dev-verify 或全量验证的结果？

## 背景与执行事实

仓库内的依赖要求、`Cargo.lock` 和供应链政策是稳定输入。RustSec 公告库的 HEAD 与 registry index 的 yanked 状态随时间变化：相同的源码和锁文件仍可能得到不同的公告结论。本裁决替换 ADR-0200。准入继续只消费稳定政策；公告发现不再进入任何验证层。

| 实体 | 输入与责任 |
|---|---|
| `Cargo dependency policy` / `run_cargo_gates` | 运行 `cargo deny --locked check bans licenses sources`。不读取公告库 |
| `deny.toml` `[advisories]` | 例外的唯一来源，包括 `unsound = "all"` 与 `unused-ignored-advisory = "deny"`。`yanked = "deny"` 与 `unmaintained = "workspace"` 仍以该文件为准 |
| `python3 tools/ci/dependency-advisories.py` | 维护者在本地运行的唯一发现命令 |
| 自动入口 | 没有 `dependency-advisories` workflow，没有 `workflow_dispatch`，没有定时任务，也没有名为 `Cargo dependency advisories` 的原生阶段 |

本地命令按这个顺序工作：

1. 用 `cargo deny --version` 对照 `.github/workflows/admission.yml` 中唯一的 `NOVA_CI_CARGO_DENY_VERSION`。这是该版本的唯一声明。
2. 用 `cargo update --dry-run` 刷新 index，使 yanked 状态对齐 registry 此刻的事实。
3. 运行 `cargo deny --format json --locked check advisories`。

命令不修改 `Cargo.lock` 或 `deny.toml`。它记录这两份文件是否保持未改，也不在它们发生变化时把它们恢复回去。版本与声明不一致、声明缺失或重复、刷新失败，都是命令输入错误（退出码 2），此时不运行检查。检查实际运行之后，进程退出码就是 cargo-deny 的退出码。非空输出目录同样是输入错误（退出码 2），已有结果不会被覆盖。

cargo-deny 0.20.2 报告 vulnerability、unsound、unmaintained、notice、yanked 和 unused ignore。cargo-audit 不报告 unused ignore，因此发现工具保持 cargo-deny。ADR-0188 的真实 PR 状态、ADR-0189 的正式准入来源、ADR-0190 的通用供应链职责继续约束它们自己的检查；那些检查不再选择或覆盖公告。

## 考虑过的选项

1. **用最新公告库阻断准入。设计否决。** 相同的仓库输入会随上游发布公告而改变准入结果，获取失败也会阻断无关工作。这与准入不消费随时间变化的公告事实冲突。

2. **非必需 PR 检查，另加人工入口（ADR-0200）。成本否决。** 新公告发布之后，每个 PR 都会失败，而 PR 作者不是处置 owner。Databend 移除了这类扫描。RisingWave 改成每日 cron，由 cron 开 issue。DataFusion 只在 Cargo 文件变更和 merge_group 上运行 cargo-audit，且该检查不是必需检查。三者都放弃或降级了「最新库阻断，或每个 PR 都扫一次」的做法。

3. **把公告库固定一个周期，把该基线放回准入，并由机器人每周开修复 PR。成本否决。** 这能稳定 PR 结果，但需要自建获取与固定路径、带写权限的 workflow，以及比上述三个项目更多的机械。维护者拒绝了该方案。

4. **只检查改动了依赖的 PR（DataFusion 的触发范围）。成本否决。** 无关 PR 可以保持绿色。一次依赖 PR 仍会撞上与本次改动无关的新公告，安全处置继续混在普通开发里。

5. **定时扫描并开 issue（RisingWave）。待评估。** 发现不依赖有人记得去跑。它需要 `issues:write`；在那个项目里，未关闭的重复 issue 会堆积。

6. **维护者在本地检查，再用普通修复 PR 处置。采用。** 动态输入只进入一次由人发起的命令。PR、准入、dev-verify 和全量验证都不再读取公告库或 yanked 缓存。

## 裁决

1. **无自动发现规则。** 公告检查不是 PR 检查，不是准入阶段，不是 dev-verify 的选择项，也不是全量层阶段。没有 workflow、`workflow_dispatch` 或定时任务去运行它。

2. **单一本地命令规则。** 维护者运行 `python3 tools/ci/dependency-advisories.py`。cargo-deny 版本必须与准入声明 `NOVA_CI_CARGO_DENY_VERSION` 一致。检查之前先刷新 index。保留 cargo-deny 的原始双流和原生退出码。摘要分开记录 pass、findings 和 input error，并单列可移除例外（`advisory-not-detected`、`yanked-not-detected`）。追溯按字段独立记录：公告库 URL、完整 commit SHA、提交时间、获取时间、index 刷新、工具版本（声明值与实际值）、开始时间。某个字段读不到时，只有该字段记为 unknown。退出码不按摘要标签改写；findings 与 input error 同时出现时，输入错误仍然可见。

3. **普通修复 PR 规则。** 每条 finding 带直接入边的 crate 名和修复分类。`compatible`：dry-run 候选满足 patched 或 unaffected，或者一个 yanked crate 有兼容候选。`major-or-exception`：没有更新的兼容版本，或者候选仍然受影响。`unknown`：版本范围或公告库读不到。兼容候选用 `cargo update -p <crate>` 应用，然后重新运行命令确认；dry-run 候选可以和 `cargo update -p` 的结果不同。需要大版本或例外时，在 `deny.toml` 写一条精确例外，写明入边、owner 和退出路径。命令列为可移除的例外予以删除。PR 描述附上前后摘要。准入仍然不检查公告。

4. **传递 unsound 可见规则。** `unsound = "all"` 保留。它只影响本地报告，不阻断准入。不得为了缩短报告而收窄它。第三方代码可以在任意依赖深度进入同一进程；是否可达按每条公告的触发条件单独判断。

5. **例外单一来源规则。** 例外只写在 `deny.toml`。不另存一份例外清单，不用 path 替换把 registry 公告藏起来。

## 接受的妥协（诚实记录）

没有任何机制自动发现公告。一个 PR 可以引入有漏洞的依赖并合入；发现要等到维护者下一次手动运行。发现频率完全由维护者的节奏决定。

全量验证层不再覆盖公告。

yanked 结论取决于该次运行的 index 刷新。刷新失败时，命令拒绝检查，不依据陈旧缓存给出结论。网络或 index 故障会使这一次发现无法完成。

摘要中的兼容版本是 `cargo update --dry-run` 的候选。维护者应用该候选并重新运行命令之后，它才算被确认。

## 何时重新评估

- 维护者多于一人、发生真实安全事件，或需要定时发现时，评估 RisingWave 式定时任务，包括写权限和去重。处置不再只落在 PR 作者或单一维护者身上时，同时重评已成本否决的非必需 PR 检查（选项 2）。
- GitHub security alerts 这类被动渠道变得可信时，重评是否由它承担发现，而不再只靠维护者记得去跑。
- cargo-deny 提供原生的数据库 revision 固定时，重评把固定基线放回准入的成本（选项 3）。只有当一次依赖变更可以对照固定基线、不再与检查窗口内新发布的无关公告碰撞时，才重评「只检查依赖变更的 PR」（选项 4）。unused ignore 出现在另一个值得换用的工具上时，重评继续使用 cargo-deny 的理由。
- 产品可达性分析发生变化时，重评 `unsound = "all"`。不得只因为报告变长就重评。

用最新公告库阻断准入（选项 1）是设计否决。只有「准入不消费随时间变化的外部输入」这一约束本身被取消时，才重新打开它。
