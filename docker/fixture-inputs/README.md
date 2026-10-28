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

# Fixture 输入供给与平台

`lock.json` 是唯一源锁。schema 2 为镜像和派生 recipe 声明平台变体，公共 artifacts 共享；解析、闭包选择和平台规范化由 `fixture_inputs.py` 统一拥有。`linux/amd64` 和 `linux/arm64` 的 Iceberg 输入准确区分；Paimon writer 仍仅支持 amd64。

```bash
# Explicit online acquisition and build; select only the required consumer.
docker/fixture-inputs/provision.sh --consumer iceberg-rest --platform linux/amd64
# Local observation only: no pull, network download or build.
docker/fixture-inputs/verify.sh --consumer iceberg-rest --platform linux/amd64
```

省略 platform 时读取本机原生事实；实际供给/消费还核对 Docker daemon 与目标一致。纯本地 context/producer 投影可以显式选择目标；缺失变体明确拒绝，不仿真、不静默缩小 all。arm64 的 all 包含不支持的 Paimon 时应拒绝；显式选 iceberg-rest。使用 `--store` 选择账户的 store，消费者必须消费同一个位置。

平台参与有效 lock_id，consumer 只决定闭包，不改变 lock_id。同 lock/platform 的重复供给验证实际输入，已满足时不重发 BOM；补充闭包时重新核验保留回执，将有效 artifact 移入新 generation，原子发布合并结果。不得以部分供给回退已有有效覆盖。

BOM schema 2 记录 platform、锁身份、镜像/派生定义和 artifact 证明。schema 1 BOM 不猜测升级；旧输入、旧材料和数据卷保留，显式重新 provision，再按 [机器维护入口](../iceberg-rest/README.md) start/upgrade 和 worktree attach。新 BOM 不可直接从另一机器复制，实际 image ID 和 daemon 仍需本机证明。

传输 repository 可用 `--image-source logical-name=transport-repository` 配置。来源不是内容身份：digest、平台、artifact 长度/hash 不变。数据集 producer 的语义 key 使用对应平台的实际 recipe/基础镜像/artifact 子投影；无关镜像别名或传输变化不应改变 key，producer 源码变化仍按原规则产生新 key。

镜像或平台变更由 fixture 维护者更新唯一源锁，保留上游 index、平台 manifest 和 config 的原始证据，再执行 provision 和实际消费者验证。仅修改 JSON 不能证明平台运行可用。单测入口：`bash docker/fixture-inputs/tests/run.sh`；machine、Paimon 和 benchmark reader 回归由各自原测试入口维护。
