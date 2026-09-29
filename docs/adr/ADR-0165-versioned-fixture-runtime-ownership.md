---
id: ADR-0165
title: "Versioned fixture runtimes have one owner and one published worktree binding"
domain: [test-fixtures]
status: active
supersedes: []
superseded-by: null
date: 2026-09-29
provenance:
  - "discussion: 2026-09-29 versioned fixture runtime ownership and atomic publication"
code-anchors:
  - "docker/iceberg-rest/fixture_runtime.py (RuntimeOwner.bind, publish, unbind, delete_catalog, manage)"
  - "docker/iceberg-rest/runtime_entry.py (render_entry, worktree_main)"
  - "docker/iceberg-rest/templates/object-store.yml"
  - "docker/iceberg-rest/templates/catalog.yml"
  - "tests/cluster-harness/src/isolated_iceberg_rest.rs"
  - "tools/ci/local-full-ci.sh (load_fixture_publication, prepare_runtime)"
---

## 问题

多个工作区如何共享可恢复的测试服务、让输入版本并存，同时准确决定谁可以改变绑定、清理数据和删除资源？

## 背景与执行事实

输入供给与运行实例是两个责任。ADR-0155 的 provision/BOM 负责镜像与 artifact 输入；runtime owner 消费验证后的本机事实，负责资源身份、端口预留、恢复、发布和显式删除。一次普通附着不能以可变镜像 tag 或当前 checkout 的 Compose 内容覆盖已经运行的版本。

| 实体 | 身份与权威 | 责任 |
|---|---|---|
| owner locator | 规范化控制目录 + 本机 Docker daemon ID | 查找记录及隔离资源命名空间；不以 daemon 全局项目名认领资源 |
| 对象存储实例 | 协议、真实 MinIO/mc image ID、模板、凭证/桶配置的 key | 独立项目、卷、端口和共享网络连接 |
| catalog 实例 | 协议、真实 REST/Spark/mc image ID、模板、配置、对象存储 ID 的 key | 持久 catalog 数据库、项目、实例标签、服务端 warehouse |
| worktree 入口 | 规范化 entry 路径及固定 `.owner.lock` | 唯一当前绑定及所有历史数据位置 |
| publication | 完整文件集合 + `published` 原子指针 | 同时提交绑定、owner locator、配置、记录投影与 producer receipt |
| `data_locations` | owner locator + 对象存储 ID + 私有前缀 | 保存解除绑定后仍需清理的数据引用 |
| 删除操作 | `deleting + deletion_id` | 准确恢复同次删除，不等同于 runtime key 或 incarnation |

`RuntimeOwner.new_definition` 使用全 key 校验短 ID 冲突；资源标签与实例镜像标签包含 owner 命名空间。`materialize` 只有缺失记录才分配端口；starting、停止后恢复及容器重建沿用预留。catalog 的数据库使用持久卷，服务端 warehouse 与 worktree 客户端 warehouse 分开。MinIO 恢复重新接入所有记录中的 catalog 网络并保留其 aliases。

`RuntimeOwner.publish` 先写入并落盘完整不可变目录，最后替换并落盘 `published`。current 定位固定入口，入口中的各文件链接指向 published；不存在另一份可先于文件提交的绑定权威。`runtime_entry.render_entry` 只生成文本，共享发布权在 owner；SQLite 等运行数据保留在 publication 外的稳定目录。消费者解析一次 publication，env、runner 配置、Spark defaults 和子进程端点都来自这次快照。

`prepare_entry` 不调用 Docker：已有 binding 按保存的 locator 查询记录，两个保存记录 ready 时恢复配置，否则发布 unbound。ready 只说明记录状态，不能证明服务健康。unbound 的 env 可以读取，但没有可被误用的占位端点或镜像。

## 考虑过的选项

**固定共享项目，普通 up 按当前 Compose 重建。设计否决。** 易操作，但任一 checkout 可以重写别人使用的服务；输入变化、数据保留、端口和恢复身份无法独立表达，版本也不能并存。

**所有 worktree 独占完整服务。设计否决为默认共享模型。** 隔离直观但丢失相同输入复用和对象存储共享能力；它保留为需要 catalog namespace 隔离的测试 profile，其生命周期由独立 harness 负责。

**分别更新 binding 索引与运行配置。设计否决。** 读者会看到已更新绑定与旧文件、或新文件与旧索引，进程中断使恢复判断失去唯一依据。binding 和配置必须有同一个提交点。

**解绑立即释放数据引用，或 force 递归删除所有依赖。设计否决。** 数据仍在对象存储时释放引用会允许误删；递归删除会侵入其它 owner 的 HMS/消费者资源。force 只明确允许的中断和目标条件解绑，不代表依赖消失。

**运行实例 owner、不可变 publication 和显式引用。裁决。** 按内容键复用资源，把绑定与文件一并发布，保留未清理的数据位置，删除逐阶段核实准确资源。

**精确供给快照、统一 hook 输入供给、在线消费者租约与自动 GC。待评估。** 这些机制有独立边界，当前运行 owner 不把它们隐藏成兼容路径或自动动作。

## 裁决

1. **唯一实例所有权规则。** 从保存的 owner locator 与 runtime 记录操作资源；本机 daemon 必须吻合。不同控制目录是不同命名空间。未解绑的入口拒绝切换 owner，旧 REST/Hive 不自动接管。
2. **锁序与恢复规则。** 固定 worktree 锁 → 对象存储 → catalog → ports；健康对象存储路径使用共享锁，让不同 catalog 创建/附着并发。维修需释放共享锁、取独占锁并重新核实，不能原地升级锁或沿用过期判断。外部命令有界，超时/取消释放锁并保留恢复记录。
3. **唯一发布规则。** 所有共享入口发布和旧输出清理由 owner 在固定 W 锁内执行。指针提交前旧 publication 完整可读，提交后新 publication 完整可读；shell 不在返回后补写入口。坏指针或索引 fail closed，不能当作未绑定或无引用。
4. **消费者快照规则。** 启动时只解析一次 publication，使用其中的不可变 env/config 路径。五项物理端点向配置别名与子进程环境统一投影；SQL/engine 故障代理是显式 transport overlay，provider 子进程仍使用物理 fixture publication。`NOVA_ENV_RUNTIME_DIR` 只认稳定运行数据；不能构造它下面的 env.sh。隔离 harness 不写共享 current，publication hook 在创建前选定 profile。
5. **数据引用规则。** unbind 发布 unbound 并保留全部历史 `data_locations`；purge 全部历史位置成功才清空引用。失败保留原 publication 和所有引用以供幂等重试。私有清理不删除标准 benchmark READY 数据。
6. **显式生命周期规则。** catalog 有绑定时普通 stop/delete 拒绝。force-delete 逐个固定入口复核，只有仍指向目标 owner/catalog 才解绑，保留数据引用。对象存储有 catalog 或数据引用时默认 stop 和任何 delete 都拒绝；force-stop 可以中断使用者，但保留记录、卷和端口。
7. **删除操作身份规则。** 首先保存 deleting 与 deletion_id，跨阶段重新核实同一操作身份；停止、服务端前缀清理、MinIO 断网、项目/卷/网络删除、实例标签移除全部核验后原子退休记录目录。旧删除者不能处理同 key 重建出的新资源。
8. **外部连接规则。** HMS 等 endpoint 未退出时，普通/force catalog delete 均在破坏操作前拒绝。HMS owner 撤销自身准确连接与项目，REST owner 不停止别人的容器；最终还要确认网络确已删除。
9. **实际生产者规则。** benchmark 先 bind，再读取同一 publication；实际 Spark 容器 image ID 和 BOM producer 必须匹配 resolver 的投影才检查/生产 READY。dry-run 不启动 Docker，不宣称实际 producer 已验证。新对象存储需要重新构建标准数据。
10. **分类规则。** BOM 供给前置条件失败（75）是 BLOCKED；端口、身份和生命周期等 owner 错误是准备阶段 VERIFY FAILED。CI 记录两个 runtime ID、publication 和实际端点，失败不进入 Cargo gates。

## 接受的妥协（诚实记录）

保存记录 ready 不等于当前健康，offline prepare 只是配置恢复；运行者必须正常 up 才得到健康核验和恢复。显式 unbind/force-stop/force-delete 可以使已经运行的消费者失效；这里没有在线租约、退出协调或透明切换。

旧 publication 与历史数据位置会占磁盘，正常 bind 不做后台 GC。私有 purge 要遍历历史 owner/对象存储位置，部分失败必须保留引用；因此回收需要明确操作，不能以删除目录替代资源清理。首次新对象存储的 benchmark 构建有实际时间/存储成本。

输入 BOM 的读取快照与 derived-image 可变 alias 仍存在供给竞态；私有 store 并不隔离 daemon 全局 alias。实例独立标签/真实 image ID 和 actual producer 核验能拒绝不一致，不能被描述为消除了供给竞态。publication-hook 镜像仍由隔离测试创建前从本机基镜像构建，没有宣称它已纳入普通供给 BOM。

旧 REST/Hive 保留，需要用户确认旧消费者退出及数据保存后另行退役。runtime owner 不自动迁移旧数据或按项目名前缀清理。

## 何时重新评估

- 供给机制能发布一个完整、不可变且精确引用的镜像/artifact 快照时，替换 BOM 读取与可变 alias 的来源，并将 publication hook 输入纳入统一供给；仍保留 actual producer 核验。
- 需要透明切换在线消费者或自动回收时，单独设计可验证的租约、退出协调与 GC 权威；不能放宽引用/外部连接检查来模拟这些能力。
- 新平台不能保证要求的锁、文件落盘与原子 pointer 替换，或 Docker daemon 不支持本机网络连接/aliases 时，重新裁决存储和网络边界，不引入固定端点 fallback。
- 需要退出旧项目或迁移其数据时，明确授权、资源清单与迁移验收单独成事；不扩大普通 up/down 的权力。
