---
id: ADR-0229
title: "Rust tests are placed by assertion owner, prefer the public boundary, and widen dependencies only to declared input-contract producers"
domain: [crate-boundary, test-capabilities]
status: active
supersedes: []
superseded-by: null
date: 2026-11-12
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1304"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1307"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1308"
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/1313"
  - "discussion: 2026-11-12 Rust test organization methodology: placement, dependency direction, fixtures and growth"
  - "discussion: 2026-11-17 finalization after the cross-owner, white-box tree and integration target pilots"
code-anchors:
  - "docs/guides/development/rust-test-organization.md (operating rules and placement procedure)"
  - "novarocks/execution/src/exec/chunk/mod.rs (single cfg(test) tests entry in a production module)"
  - "novarocks/execution/src/exec/chunk/tests.rs (white-box test file outside the production file)"
  - "novarocks/execution/Cargo.toml (input-contract producer dev-dependencies of Execution)"
  - "novarocks/plan-codec/src/physical_package_v2.rs (development-cfg gated testkit module shared with the owner's white-box tests)"
  - "novarocks/sql/testkit/Cargo.toml (domain testkit crate for SQL compile inputs)"
  - "novarocks/state-store/testkit/Cargo.toml (domain testkit crate shape)"
  - "tests/compile-to-execute/Cargo.toml (compile-to-execute seam test package)"
  - "novarocks/control-runtime/tests/it/main.rs (one public-contract integration target with its mode gate)"
  - "novarocks/control-runtime/tests/loom/main.rs (one Loom target per crate, model families as modules)"
  - "tests/test-support/Cargo.toml (mechanical support leaf)"
  - "tools/ci/verification/select.py (Rust selection by reverse dependencies)"
  - ".config/nextest.toml (binary-scoped test groups)"
---

## 问题

一个 Rust 测试应当放在哪个 crate 的哪个位置，它可以依赖谁，它的输入由谁提供，测试随功能增长时怎样累加，
以及早于这些规则的存量测试怎样收敛？

## 背景与执行事实

操作规则在 `docs/guides/development/rust-test-organization.md`。本条记录这些规则背后的模型和裁决。
本条随试行规则在 PR #1304 合入，经三个试点（PR #1307 跨 owner 归位与接缝、PR #1308 白盒测试树、PR #1313 集成目标）后原地定稿。

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

| 机制 | 以什么为单位 | 一条越出允许范围的 dev 依赖带来什么 |
|---|---|---|
| 开发期的 Rust 测试选择（`select.py` 用 nextest 的 `rdeps()`） | 包图上的反向依赖，不区分生产依赖与 dev 依赖 | 对方一改，本 crate 及其全部下游的测试被选中 |
| 测试二进制的链接 | 整个 crate 的测试 target | 测试二进制链接对方的整个栈 |
| 失败归因 | 测试所在的 crate | 报告指向本 crate，要修的可能是对方 |

**同一次构建产生测试和真实二进制。** 规范准备把 workspace 的测试与真实程序放在一次 Cargo 调用里构建。
Cargo 在一次构建内统一 feature，所以任何成员为测试开启的 feature 都会被编进被测的真实二进制。
ADR-0140 已据此否决用 feature 隔离测试机制。开发 cfg `novarocks_dev_controls` 不同：它由实际的 debug assertions 决定，
不是 feature，没有统一的问题，标准 release 也不包含它（ADR-0181）。仓库里这类门控的 `pub` 有一百多处，
分属运行期控制接缝、需要私有项的测试入口和纯输入构造三种。

**testkit 回指自身会得到两份类型。** 设 `U-testkit` 依赖 U。U 的 lib 单元测试若使用 `U-testkit`，
测试二进制里有两份 U，同名类型互不相同。U 的集成测试链接 U 的 lib，与 testkit 用的是同一份，不受影响。

**有些输入只能由另一个 crate 产生。** Execution 的输入契约 `LocalProgram` 定义在它的生产闭包内，但没有测试从零构造它；
至少 72 个测试文件经物理计划片段与局部编译器取得输入，其中很多是必须留在 `src/` 的白盒测试。
物理计划与局部编译器都不在 Execution 的生产闭包内；生产上只有 Native Adapter 同时依赖局部编译器与 Execution，
并把前者的输出交给后者。

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

**试点的结果。** 跨 owner 的试点建立了 SQL 编译输入的 testkit crate（`novarocks-sql-testkit`）、“编译到执行”接缝测试 package
与 SQL、Query Application 的 `tests/it`；接缝测试需要 Execution 私有项的一例，改用公开的分配器入口后迁入。
白盒测试树的试点在 Functions 数值域按文件判定：363 个测试中 60 个整文件公开、进入 `tests/it`，其余留在白盒测试树，
其中约 69 个经静态分析可以公开，它们多数只是借用了私有的构造简写。集成目标的试点把 Control Runtime 的 41 个集成
target 合成 `it` 与 `loom` 两个，Loom 包装器改为按模型族检查，重新启动自身的测试改为由 `module_path!()` 推导子进程路径；
一次不作验收的构建观察显示，改一个测试文件的重编变慢、改库文件的重编变快、测试二进制大幅减少。

**与既有裁决的关系。** ADR-0056 规定 Provider 断言归 owner；本条把“断言归 owner”推广到全部 Rust 测试。
ADR-0069 规定机械测试资源有一个专用的依赖叶子，并否决万能 testkit；本条沿用。
ADR-0140 规定契约与测试机制拆成独立 crate，并否决 feature 隔离；本条把它作为领域 testkit 的形态。
ADR-0175 与 ADR-0181 管理运行期的控制与观察接缝及开发 cfg 的编入；本条的门控测试支持沿用开发 cfg，运行期接缝仍归它们。
ADR-0190 规定依赖方向由 crate 结构与 review 维持；本条的生效方式遵循它。这些裁决继续有效。

**正例。** `novarocks/execution/src/exec/chunk/mod.rs` 只声明一个测试入口，测试在同目录的 `tests.rs`。
`novarocks/state-store/testkit` 与 `novarocks/sql/testkit` 是领域 testkit 的现有形态；`novarocks/plan-codec/src/physical_package_v2.rs`
的 `test_support` 是门控 testkit 模块的现有形态；`tests/compile-to-execute` 是接缝测试 package 的现有形态。

## 考虑过的选项

**全部测试写成集成测试，lib 不编测试。** 每个 crate 只有一个测试二进制，库不必为测试再编一遍。
但私有状态机、局部算法和内部错误传播只能用白盒测试表达，搬出去就要放宽生产可见性或丢掉覆盖。
*设计否决*：与“不为测试扩大产品公共 API”冲突。

**每个 crate 无条件只有一个集成 target。** 链接单元最少。但全局 allocator、自定义 harness、
`required-features`、Loom、Miri 和按 binary 选择的资源组都是 target 级属性，混编会改变被测的语义。
*设计否决*：这些属性由 Cargo 与 nextest 按 target 赋予，无法在一个 target 内共存。理由相同的 target 仍合成一个。

**用 feature 门控暴露测试支持。** 测试与生产共用一个 crate，不必另建 testkit。
*设计否决*：同一次构建会把 test feature 编进被测的真实二进制，测到的不再是产品。

**用无条件的 `pub` 模块暴露测试支持。** 没有 feature 统一的问题，lib 单元测试也能用。
*设计否决*：测试支持成为产品公共 API 的一部分并进入生产构建。

**只用 testkit crate，禁止任何 `pub` 的测试支持。** 规则最少。
*设计否决*：其他 crate 的测试需要 owner 私有项时无法表达；owner 的白盒测试与其他 crate 共用的输入会有两个作者，
因为 owner 的白盒测试用不了依赖 owner 的 testkit crate。

**跨 crate 的测试支持统一用开发 cfg 门控的 `pub`。** 规则最简单，也与存量的多数写法一致。
*成本否决*：门控代码编在库的 dev 构建里，纯输入构造全部放进去，改一次 fixture 就要重编 owner 的全部下游；
这与白盒测试独立成文件、让测试改动不影响非测试构建的方向相反。

**依赖规则不变，为 Execution 另建闭包内的 `LocalProgram` 构造器。** Execution 的测试不再依赖局部编译器。
*设计否决*：等于在测试里重写局部编译器，构造器会与真实编译产物分叉，Execution 的测试测到的不再是生产输入。

**依赖规则不变，把经局部编译器取输入的测试搬到 Native Adapter 或接缝测试 package。** 依赖不越界。
*设计否决*：其中很多是白盒测试，需要 Execution 的非 `pub` 项，搬出去就要放宽可见性。

**组合测试留在下层库，经上层的 testkit 取得真实输入。** 不需要新的 package，测试紧挨下层代码。
*设计否决*：越出允许范围的 dev 依赖仍然存在，选择放大、链接耦合与归因模糊都按 crate 计算，不因中间多一层 testkit 而消失。

**组合测试放进一个生产上同时依赖两侧的应用 crate。** 不需要新的 package，也没有越界的依赖。
*设计否决*：断言保护的是穿过接缝的结果，不是那个应用 crate 的职责；失败会指向一个不拥有该行为的 owner。

**一个依赖所有层的通用测试 crate。** 任何跨层测试都有去处。
*设计否决*：它把所有领域的依赖汇到一处，任何改动都会选中它；ADR-0069 已否决同类结构。

**用 lint 或 CI 检查测试文件的名称、位置或模块登记。** 规则有工具强制。
*设计否决*：这是源码形状检查，ADR-0190 已确定依赖方向与结构由 crate 边界和 review 维持。

**成批迁移存量，按文件纯位置移动，或按测试函数改写。** 收敛有明确终点，前者的核对可以机械完成。
*成本否决*：迁移工作与真实改动脱钩；按文件移动还会把本可公开的测试留在白盒；运算符与 CAST 的测试随表达式层迁移时还要再改一遍。

**把存量的内联测试全部迁到独立文件，使白盒形态统一。** 构建行为更一致，规则更简单。
*成本否决*：内联测试分布在上千个文件里，其中多数很小，迁移它们的评审代价高于收益。

**引入数据驱动或快照类依赖，或提供不启动集群的进程内 SQL 语料入口，作为统一的测试形态。**
*待评估*：本条只规定写法约定，没有裁决是否引入这类工具。

## 裁决

Rust 测试按“断言归谁”放置，先找公开边界，测试依赖只向显式登记的输入契约生产者放宽，输入有唯一作者，
测试按行为轴累加，存量随改动迁移。固化为以下规则。

**放置规则**

1. **断言归 owner。** 测试归“断言失败时其承诺没有兑现的那个 crate”。文件位置、文件名、导入行和用到的类型都不决定归属。
   以测试函数为单位，整体归一个 owner；沿链调用多个 crate 的入口时归最下游的那个，中间检查随测试留下。
   同一行为在上游就能观察到时写在上游。
2. **白盒在独立测试文件。** 需要观察内部性质的测试留在 `src/`，默认放在独立文件，生产模块只留一个测试入口，并写明观察的内部性质。
   内联只用于不需要任何 helper 的少量断言。经 `#[path]` 从非标准位置加载的生产模块要接测试树时，先把生产文件移到标准位置。
3. **先找公开边界。** 断言与输入都能经 owner 公共 API 表达的，写成公开契约测试，集中在一个 `tests/it` 里按领域分子模块；
   只有观察内部性质的才写白盒。生产可见性按真实调用方决定，不为换位置放宽，也不为本 crate 的 `tests/it` 加薄入口。
   需要本 crate 领域 testkit 的测试必须在 `tests/it`。
4. **独立 target 要有 target 级理由。** 理由只有：自定义 harness、全局 allocator、`required-features`、
   Loom、Miri、按 binary 选择的资源组。理由写在 target 根的顶部。同一 crate 中理由相同的独立 target 只有一个，
   合并时原先按 target 提供的完整性保证要在新的粒度上保留；运行模式的门控写在 target 根上。
   同时需要私有项的测试留在 `src/` 内按该模式门控的模块，规则 2 优先。

**依赖规则**

5. **测试依赖只向输入契约生产者放宽。** 一个 crate 的测试在 workspace 成员中只依赖四类：它的生产依赖闭包、
   闭包内 crate 的领域 testkit、机械支持，以及指南“输入契约生产者”表中为它列出的 crate。第三方测试库不在此规则内。
   表中新增一行须同时满足：契约类型定义在 owner 的生产闭包内；它没有可行的闭包内构造方式；生产者的公开生产 API
   直接产出它，且生产上由同一角色的组合点把生产者的输出交给 owner；生产者自己的生产闭包不含另一角色一侧的 crate。
   新增一行是指南修订，改变这些条件要修订本条。
6. **无生产边的组合进接缝。** 组合的两侧之间有生产依赖时，组合测试归下游。没有时，先做替换检验：
   期望值相对 owner 自己的输入陈述的，上游只是输入的构造者，测试留在 owner 并改用允许范围内的输入；
   期望值相对上游原始输入陈述的，进入接缝测试 package。它只含测试、按一条产品接缝命名、依赖两侧、
   只用两侧的公共 API、不被任何成员依赖，数量由产品接缝决定。接缝测试需要一侧的私有项时，依次考虑：
   改用公共途径观察同一个量；把断言拆回那一侧，接缝里保留真实产物形状的断言；性质本身跨接缝时才加薄入口。

**输入规则**

7. **fixture 沿阶梯归属，测试支持按性质暴露。** 就地、测试树、同 crate、owner 内的门控模块、领域 testkit、机械支持，
   取到够用的一级为止。给 owner 之外的测试（包括 owner 自己的 `tests/it`）用的支持代码，依次判断：运行期接缝归 ADR-0175
   与 ADR-0181；必须触及私有项的，用 owner 内开发 cfg 门控的 `*_for_test` 薄入口，只转调；owner 的白盒测试也用的输入，
   无论是否需要私有项，用 owner 内开发 cfg 门控的 testkit 模块；其余用领域 testkit crate，它带入的依赖也要落在每个消费者的
   允许范围内。不复制 helper，不用 feature、无条件的 `pub` 或跨 package 的 `#[path]` 共享。
8. **testkit 只构造输入。** 它不实现被测的消费规则，不给出由被测代码算出的预期。

**增长规则**

9. **测试按行为轴累加。** 测试文件以被测对象的行为轴命名，不含工作项、阶段或编号。新用例先加进规范位置上的既有主题，
   不再加进旧布局的文件。每棵测试树有一个 check 式入口，用例体只写数据与期望。预期独立于被测代码。
10. **存量随改动迁移。** 不成批迁移存量。一项改动改变了存量测试保护的行为、让它所测的对象退役，或者用新测试替代了它，
    就在同一个改动里按测试函数迁移：能经公开入口表达的改写到公开入口，其余留在白盒并写明内部性质；输入、期望值及其来源
    与测试函数名保持，因本次行为改变而改动的期望值单独说明。被替代或已废弃的旧测试在同一个改动里删除，逐项记录它保护的
    每条性质由哪个测试接手，或随哪项退役不再成立。没被改到的存量保持现状。

**生效方式**

11. **由结构与评审生效。** `tests/it` 的黑盒性质由编译器保证，testkit 与接缝由 crate 边界承载；
    新测试的位置、被迁移与删除的存量测试及其性质去处在 plan 中写明并接受评审。不增加检查文件名、位置、数量或模块登记的
    lint 与 CI。

## 接受的妥协（诚实记录）

- **增长规则没有工具强制。** 规则 9 只靠 plan 中的测试落点和评审维持。评审松懈时，按任务命名的测试文件会重新出现。
  接受它是因为可行的强制手段都是源码形状检查。
- **存量没有收敛终点。** 规则 10 让迁移跟着改动走，长期不动的区域会一直保持旧布局，相邻文件在很长时间内不是范例。
  新来的人和 agent 必须读指南，不能照抄邻居。
- **改写的核对靠评审。** 改写到公开入口的测试不能只用旧新身份映射机械核对，评审要逐条审阅性质去处记录；
  记录不完整时，覆盖可能在不被察觉的情况下缩小。
- **输入契约生产者扩大了选择。** 改局部编译器或物理计划会选中 Execution 的全部测试。接受它是因为这些改动本来就影响 Execution 的输入。
- **门控测试支持编进 dev 构建。** 门控 testkit 模块与薄入口编在库的 dev 构建里，修改它们会让 owner 的下游在 dev 构建中重编。
  所以纯输入构造只在 owner 的白盒测试也要用时才放进门控模块。
- **过渡期的越界依赖。** 接缝测试 package 与 SQL 编译输入建立之前写成的 Execution 测试，经转调依赖 SQL 的 testkit crate 与
  接缝 package 的 lib；在最后的使用方迁走之前，“接缝 package 不被任何成员依赖”不成立。
- **下层测试的输入可能与真实产物分叉。** 下层改用允许范围内的手工输入后，可能在上层编译器不会产出的形状上通过。
  接缝测试保留由真实产物驱动的用例来覆盖这一点，但它的数量远少于下层测试。
- **接缝测试离被测代码更远，并多一个链接单元。** 每条接缝新增一个 workspace 成员。接受它是为了换掉一条
  对整个 crate 生效的越界依赖。
- **一部分测试不能写成单元测试。** 需要本 crate testkit 的测试只能是集成测试，即使写成单元测试更顺手。
- **单个 `it` 让改一个测试的重编变慢。** 合并集成 target 后，改一个测试文件要重编整个 `it`；只在实测成为负担，
  或一部分测试需要不同的 cfg、`required-features` 或运行模式时，才按领域拆分。
- **内联测试的不统一被保留。** 不迁移存量内联测试是出于迁移代价，不是因为内联更好。

## 何时重新评估

- Cargo 提供稳定的、按 package 隔离的 feature 解析，或规范准备不再把测试与真实二进制放在同一次构建里。
  这时 feature 门控的否决理由消失，重新比较它与独立 testkit crate。
- 开发期的测试选择不再沿 dev 依赖扩散。规则 5 的选择放大理由减弱，链接与归因两条理由仍在，需要重新权衡规则 6 的代价。
- 接缝测试 package 的数量明显超过产品接缝的数量，或某个 package 开始接收不穿过它那条接缝的断言。
  说明它在退化成通用测试 crate。
- 输入契约生产者表增长到多行，且各行的依据开始相互矛盾。说明准入条件不再可判定。
- 开发 cfg 的机制被 ADR-0181 的后继改变，或门控代码不再随库的 dev 构建使下游重编。这时重新比较门控 `pub` 与 testkit crate 的分工。
- 某个存量热点反复拖累开发。这是成批迁移一项成本否决的触发条件，届时为那个热点单独设计整理。
- 某条已定规则在真实代码上不可执行。上一次触发是 Execution 的输入只能由局部编译器产生，规则 5 因此增加了输入契约生产者一类。
- 修改内联测试导致的重编成为可观察到的负担。这是“全部迁到独立文件”一项成本否决的触发条件。
- 某个领域的测试中，构造代码持续远多于数据与期望，check 式入口不足以收敛。
  这是数据驱动、快照依赖或进程内 SQL 语料入口一项待评估的触发条件。
