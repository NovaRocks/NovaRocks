---
id: ADR-0199
title: "Readonly observations and mutation settlement have separate obligations"
domain: [test-fixtures]
status: active
supersedes: []
superseded-by: null
date: 2026-10-28
provenance:
  - "mechanism: explicit owner disposition of failed readonly control observations"
code-anchors:
  - "docker/iceberg-rest/machine_runtime.py (MachineDocker.readonly_command, failed_readonly_disposition)"
  - "docker/iceberg-rest/machine_runtime.py (MachineOwner.resolve_readonly_observation)"
---

## 问题

共享 fixture 的只读观察失败，应如何影响机器准入，同时保留命令退出与变更结算的真实责任？

## 背景与执行事实

ADR-0191 规定机器维护、命令监督、真实 writer 退出和准确 scratch 清理。控制 journal 同时记录观察命令和变更命令：未取得观察结果，不等于 daemon 上存在待结算的变更；观察者的物理退出也不能从命令类别推定。

`MachineDocker.command` 与恢复入口共用 `readonly_command` 的封闭 Docker 只读词汇。`unsettled_operations` 拥有机器变更屏障；`failed_observation_status` 分别展示原失败、原退出字段及处置。运行清理仍由原 context、期限、writer 退出和 catalog 屏障决定。

## 考虑过的选项

- **所有未结算观察都阻止机器准入：设计否决。** 记录简单，但把缺失观察结果误作未知 mutation，使无变更义务的观察者决定无关机器工作的可用性。
- **将只读失败改成成功或已退出：设计否决。** 命令类别没有提供结果或物理退出证明，也不能支持 writer 收尾或文件删除。
- **按具体异常或某个查询参数增加例外：设计否决。** 异常名字不决定 daemon 是否发生变更，独立例外会偏离实际派发作者。
- **原 owner 显式处置只读观察的变更义务：采用。** 共用命令分类并保留原始事实，机器准入与物理退出各守自己的证据。

## 裁决

1. **命令分类只有一个作者。** 派发、期限处理和恢复共用封闭的只读命令分类；未知命令、exec 和变更命令不能从 `mutating=false` 或文本中含有 `inspect` 获得只读资格。
2. **处置只属于准确原件。** 显式入口在原维护锁及同一绝对操作期限内核对 live daemon、机器身份、私有 journal、原请求与材料身份，只追加所选 journal 的无 mutation 义务处置。重复有效处置不重写原件；材料变化使旧处置失效。
3. **结果、退出与变更义务分开。** 原 phase、失败及退出字段保留，缺失退出证明保持 Unknown。有效处置仅解除该只读观察对机器变更准入的阻挡；status 同时呈现原失败与处置。
4. **恢复不扩大维护权。** 准确入口不遍历其他操作，不启动、升级、清扫或删除资源。未知 mutation 继续阻止准入；处置不能证明 CLI 或远程 writer 退出、容器消失或 scratch 可删。

## 接受的妥协（诚实记录）

原只读命令可能仍缺少成功结果或物理退出证明，处置后也不把相关运行改为通过。维护者须明确选择准确 journal；这保留操作成本，以避免恢复流程静默改变原始事实。恢复不会修复观察命令本身的失败原因。

## 何时重新评估

新增控制命令时重新确认其实际 Docker 语义并扩展共同分类；不能仅因恢复需要将它声明为只读。若运行必须等待所有观察进程退出，另行落实原进程 owner 的真实退出协议；该需求不能通过变更准入处置模拟。若要自动处置观察失败，须单独明确触发权与诊断责任，不从本条的显式入口推导自动恢复或资源清理权限。
