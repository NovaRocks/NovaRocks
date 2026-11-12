---
id: ADR-0229
title: "Rust tests are placed by assertion owner and never widen the production dependency graph"
domain: [crate-boundary, test-capabilities]
status: active
supersedes: []
superseded-by: null
date: 2026-11-12
provenance:
  - "discussion: 2026-11-12 Rust test organization methodology: placement, dependency direction, fixtures and growth"
code-anchors:
  - "docs/guides/development/rust-test-organization.md (operating rules and placement procedure)"
  - "novarocks/execution/src/exec/chunk/mod.rs (single cfg(test) tests entry in a production module)"
  - "novarocks/execution/src/exec/chunk/tests.rs (white-box test file outside the production file)"
  - "novarocks/state-store/testkit/Cargo.toml (domain testkit crate shape)"
  - "tests/test-support/Cargo.toml (mechanical support leaf)"
  - "tools/ci/verification/select.py (Rust selection by reverse dependencies)"
  - ".config/nextest.toml (binary-scoped test groups)"
---

## 问题

一个 Rust 测试应当放在哪个 crate 的哪个位置，它可以依赖谁，它的输入由谁提供，以及测试随功能增长时怎样累加？

## 背景与执行事实

操作规则在 `docs/guides/development/rust-test-organization.md`。本条记录这些规则背后的模型和裁决。

**Cargo 给出的四种位置，各有不同的可见性与链接代价。**

| 位置 | 能看到什么 | 链接单元 | nextest binary ID |
|---|---|---|---|
| 生产文件内的 `mod tests { ... }` | 私有项 | 与 lib 单元测试同一个 | `<package>` |
| `src/` 内的独立测试文件（`foo/tests.rs`） | 私有项 | 与 lib 单元测试同一个 | `<package>` |
| `<crate>/tests/<name>.rs` 或 `tests/<name>/main.rs` | 只有公共 API | 每个顶层条目一个 | `<package>::<name>` |
| 另一个 workspace 成员 | 只有公共 API | 该成员自己的 | 该成员的 ID |

前两种的区别不在可见性，而在构建。非测试构建不读取独立的测试文件，修改它不会使这个 crate 的库及其下游失效；
修改内联测试会。

**依赖按 crate 声明，后果也按 crate 计算。** 一个 crate 的 `[dev-dependencies]` 对它的全部测试生效。
三处机制都以 crate 为单位：

| 机制 | 以什么为单位 | 一条越出生产闭包的 dev 依赖带来什么 |
|---|---|---|
| 开发期的 Rust 测试选择（`select.py` 用 nextest 的 `rdeps()`） | 包图上的反向依赖，不区分生产依赖与 dev 依赖 | 对方一改，本 crate 及其全部下游的测试被选中 |
| 测试二进制的链接 | 整个 crate 的测试 target | 测试二进制链接对方的整个栈 |
| 失败归因 | 测试所在的 crate | 报告指向本 crate，要修的可能是对方 |

**同一次构建产生测试和真实二进制。** 规范准备把 workspace 的测试与真实程序放在一次 Cargo 调用里构建。
Cargo 在一次构建内统一 feature，所以任何成员为测试开启的 feature 都会被编进被测的真实二进制。
ADR-0140 已据此否决用 feature 隔离测试机制。

**testkit 回指自身会得到两份类型。** 设 `U-testkit` 依赖 U。U 的 lib 单元测试若使用 `U-testkit`，
测试二进制里有两份 U，同名类型互不相同。U 的集成测试链接 U 的 lib，与 testkit 用的是同一份，不受影响。

**这类问题为什么会反复出现。** 在记录本条时的代码中：

| 观察 | 数值 |
|---|---|
| 名称以 `_test(s).rs` 结尾的第一方文件 | 966 个，约 5,963 个测试、515,665 行，平均每个测试约 86 行 |
| 数据驱动或快照类依赖 | 无 |
| dev 依赖越出自身生产依赖闭包的 workspace 成员 | 70 个中的 16 个 |
| Execution 中引用 SQL 编译器的测试文件 | 69 个，其中 8 个一路走到求值 |
| 改 SQL 时被选中的包 | 只算生产依赖 12 个，算上 dev 依赖 16 个，多出的都在 BE 一侧 |
| 集成测试 target | 172 个，其中两个 crate 各有 30 个以上 |

文件名里常见工作项与阶段，说明测试是按当时的任务产出的：每项工作新开一个文件、自带一套构造代码。
没有“加进已有主题”的约定时，位置规则只能决定这些文件落在哪个目录，不能阻止它们继续出现。

**与既有裁决的关系。** ADR-0056 规定 Provider 断言归 owner；本条把“断言归 owner”推广到全部 Rust 测试。
ADR-0069 规定机械测试资源有一个专用的依赖叶子，并否决万能 testkit；本条沿用。
ADR-0140 规定契约与测试机制拆成独立 crate，并否决 feature 隔离；本条把它作为领域 testkit 的形态。
ADR-0190 规定依赖方向由 crate 结构与 review 维持；本条的生效方式遵循它。四者继续有效。

**正例。** `novarocks/execution/src/exec/chunk/mod.rs` 只声明一个测试入口，测试在同目录的 `tests.rs`。
`novarocks/state-store/testkit` 是领域 testkit 的现有形态。

## 考虑过的选项

**全部测试写成集成测试，lib 不编测试。** 每个 crate 只有一个测试二进制，库不必为测试再编一遍。
但私有状态机、局部算法和内部错误传播只能用白盒测试表达，搬出去就要放宽生产可见性或丢掉覆盖。
*设计否决*：与“不为测试扩大产品公共 API”冲突。

**每个 crate 无条件只有一个集成 target。** 链接单元最少。但全局 allocator、自定义 harness、
`required-features`、Loom、Miri 和按 binary 选择的资源组都是 target 级属性，混编会改变被测的语义。
*设计否决*：这些属性由 Cargo 与 nextest 按 target 赋予，无法在一个 target 内共存。

**用 feature 门控暴露测试支持。** 测试与生产共用一个 crate，不必另建 testkit。
*设计否决*：同一次构建会把 test feature 编进被测的真实二进制，测到的不再是产品。

**用无条件的 `pub` 模块暴露测试支持。** 没有 feature 统一的问题，lib 单元测试也能用。
*设计否决*：测试支持成为产品公共 API 的一部分并进入生产构建。

**组合测试留在下层库，经上层的 testkit 取得真实输入。** 不需要新的 package，测试紧挨下层代码。
*设计否决*：越出生产闭包的 dev 依赖仍然存在，选择放大、链接耦合与归因模糊都按 crate 计算，不因中间多一层 testkit 而消失。

**组合测试放进一个生产上同时依赖两侧的应用 crate。** 不需要新的 package，也没有越界的依赖。
*设计否决*：断言保护的是穿过接缝的结果，不是那个应用 crate 的职责；失败会指向一个不拥有该行为的 owner。

**一个依赖所有层的通用测试 crate。** 任何跨层测试都有去处。
*设计否决*：它把所有领域的依赖汇到一处，任何改动都会选中它；ADR-0069 已否决同类结构。

**用 lint 或 CI 检查测试文件的名称与位置。** 规则有工具强制。
*设计否决*：这是源码形状检查，ADR-0190 已确定依赖方向与结构由 crate 边界和 review 维持。

**把存量的内联测试全部迁到独立文件，使白盒形态统一。** 构建行为更一致，规则更简单。
*成本否决*：内联测试分布在上千个文件里，其中多数很小，迁移它们的评审代价高于收益。

**引入数据驱动或快照类依赖，或提供不启动集群的进程内 SQL 语料入口，作为统一的测试形态。**
*待评估*：本条只规定写法约定，没有裁决是否引入这类工具。

## 裁决

Rust 测试按“断言归谁”放置，测试依赖不比生产依赖宽，输入有唯一作者，测试按行为轴累加。固化为以下规则。

**放置规则**

1. **断言归 owner。** 测试归“这条断言失败时要修的那个 crate”。文件所在目录和用到的类型都不决定归属。
   检查交接的断言归消费方；写在一起的独立断言按 owner 拆开。
2. **白盒在独立测试文件。** 需要私有项的测试留在 `src/`，默认放在独立文件，生产模块只留一个测试入口。
   内联只用于不需要任何 helper 的少量断言。
3. **公开契约进 `tests/it`。** 只用公共 API 的测试集中在一个集成 target 里按领域分子模块。
   需要本 crate 领域 testkit 的测试必须在这里。不为了换位置而放宽可见性。
4. **独立 target 要有 target 级理由。** 理由只有：自定义 harness、全局 allocator、`required-features`、
   Loom、Miri、按 binary 选择的资源组。理由写在文件顶部。

**依赖规则**

5. **测试依赖不越界。** 一个 crate 的测试只依赖它的生产依赖闭包、闭包内 crate 的领域 testkit 和机械支持。
6. **无生产边的组合进接缝。** 组合的两侧之间有生产依赖时，组合测试归下游。没有时，它进入接缝测试 package：
   只含测试、按一条产品接缝命名、依赖两侧、不被任何成员依赖。接缝测试 package 的数量由产品接缝决定。

**输入规则**

7. **fixture 沿阶梯归属。** 就地、测试树、同 crate、领域 testkit、机械支持，取到够用的一级为止。
   第二个 crate 实际消费时才建 testkit。不复制 helper，不用 feature、`pub` 或跨 package 的 `#[path]` 共享。
8. **testkit 只构造输入。** 它不实现被测的消费规则，不给出由被测代码算出的预期。

**增长规则**

9. **测试按行为轴累加。** 测试文件以被测对象的行为轴命名，不含工作项、阶段或编号。新用例先加进既有主题。
   每棵测试树有一个 check 式入口，用例体只写数据与期望。预期独立于被测代码。

**生效方式**

10. **由结构与评审生效。** `tests/it` 的黑盒性质由编译器保证，testkit 与接缝由 crate 边界承载，
    新测试的位置在 plan 中写明并接受评审。不增加检查文件名、位置或数量的 lint 与 CI。

## 接受的妥协（诚实记录）

- **增长规则没有工具强制。** 规则 9 只靠 plan 中的测试落点和评审维持。评审松懈时，按任务命名的测试文件会重新出现。
  接受它是因为可行的强制手段都是源码形状检查。
- **两种布局长期并存。** 存量不随本条立即迁移，相邻文件在一段时间内不是范例。新来的人和 agent 必须读指南，
  不能照抄邻居。
- **下层测试的输入可能与真实产物分叉。** 下层改用闭包内的手工输入后，可能在上层编译器不会产出的形状上通过。
  接缝测试保留由真实产物驱动的用例来覆盖这一点，但它的数量远少于下层测试。
- **接缝测试离被测代码更远，并多一个链接单元。** 每条接缝新增一个 workspace 成员。接受它是为了换掉一条
  对整个 crate 生效的越界依赖。
- **一部分测试不能写成单元测试。** 需要本 crate testkit 的测试只能是集成测试，即使写成单元测试更顺手。
- **内联测试的不统一被保留。** 不迁移存量内联测试是出于迁移代价，不是因为内联更好。
- **本条不固定依赖试点的细节。** 具体的接缝测试 package、testkit 接口和迁移核对方法不在本条内，
  它们确定后若构成新的长期裁决，另立一条。

## 何时重新评估

- Cargo 提供稳定的、按 package 隔离的 feature 解析，或规范准备不再把测试与真实二进制放在同一次构建里。
  这时 feature 门控的否决理由消失，重新比较它与独立 testkit crate。
- 开发期的测试选择不再沿 dev 依赖扩散。规则 5 的选择放大理由减弱，链接与归因两条理由仍在，需要重新权衡规则 6 的代价。
- 接缝测试 package 的数量明显超过产品接缝的数量，或某个 package 开始接收不穿过它那条接缝的断言。
  说明它在退化成通用测试 crate。
- 某条已定规则在真实代码上不可执行，例如某一类下层语义确实无法用闭包内的输入表达。
- 修改内联测试导致的重编成为可观察到的负担。这是“全部迁到独立文件”一项成本否决的触发条件。
- 某个领域的测试中，构造代码持续远多于数据与期望，check 式入口不足以收敛。
  这是数据驱动、快照依赖或进程内 SQL 语料入口一项待评估的触发条件。
