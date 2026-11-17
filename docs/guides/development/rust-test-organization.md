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

# Rust 测试组织

本指南回答写一个 Rust 测试之前的四个问题：它放在哪个 crate 的哪个位置，它可以依赖谁，它的输入由谁提供，
以及它怎样加进已有的测试而不是另起一个文件。改动代码时碰到旧布局的测试，怎样把它迁到这套规则下，也写在这里。
理由、被否决的方案和重新评估的条件在
[ADR-0229](../../adr/ADR-0229-rust-test-placement-dependency-direction-and-fixture-ownership.md)，这里只写操作。

本指南是这些规则的唯一完整表述。`AGENTS.md` 和 [test-governance](test-governance.md) 里只有入口与摘要；
摘要与本指南不一致时以本指南为准。ADR-0229 记录裁决；本指南与它不一致时以 ADR 为准，并在同一个改动里修订本指南。

仓库里现有的测试布局大多早于本指南，相邻文件的写法不是范例。新测试按下面的判定程序放置；旧测试不成批迁移，
由改动到它的任务按[存量测试的迁移](#存量测试的迁移)处理。

## 适用范围

适用于第一方 workspace 成员的 Rust 单元测试、crate 内的集成测试、领域 testkit 和接缝测试 package。

不适用于 SQL、System、Spark 套件和 `tests/sql/correctness` 语料；它们的规则见 [test-governance](test-governance.md) 和
各自目录的 README。测试资产的准入、尝试材料与证据边界也由 test-governance 规定，本指南不重复。

## 速查

| 你要写的测试 | 放在哪里 |
|---|---|
| 断言与输入都能经 crate 的公共 API 表达（公开契约） | `<crate>/tests/it/` 下按行为轴命名的子模块 |
| 必须观察内部性质（私有状态、内部共享与释放、公开入口到达不了的内部错误路径），或输入只能经私有项构造（白盒） | 被测模块的 `tests.rs`，主题多时接 `tests/<subject>.rs`，并写明观察的内部性质 |
| 需要本 crate 的领域 testkit | 只能是 `<crate>/tests/it/` |
| 检查下游怎样消费上游的真实产物，且下游生产依赖上游 | 下游 crate |
| 穿过“编译到执行”接缝、按[选层](#选层)属于接缝那一行的性质，例如期望值相对 SQL 文本陈述，或要靠非法 package 触发 | 接缝测试 package `tests/compile-to-execute`；一条语句端到端的结果归 SQL 套件 |
| 从物理计划片段经局部编译器取输入，检查 Execution 的行为 | Execution，见[输入契约生产者](#输入契约生产者) |
| 需要 Loom、全局 allocator、自定义 harness 或 `required-features` | 该 crate 中同一理由的那一个独立 target，target 根的顶部写明理由；同时需要私有项的留在 `src/`，见[第 4 问](#第-4-问是否需要-target-级属性) |
| 需要 nextest 资源组、更长的超时或 Miri | 沿用第 3 问的位置，按测试名选择，不为此拆 target |
| 启动真实进程、走真实协议或多个角色协作 | 拥有该场景的 Server、cluster harness 或 System owner，沿现有入口 |
| 多个 crate 要用同一份领域输入 | 按[测试支持的暴露方式](#测试支持的暴露方式)选择门控模块或 testkit crate |
| 改动到一个旧布局的测试 | [存量测试的迁移](#存量测试的迁移) |

## 选层

先决定一个性质该由哪一层的测试验证，再决定 Rust 测试放在哪里。开发期的快速验证靠 owner crate 的 Rust 测试，
具体位置按下面的判定程序；SQL 级测试只有一份语料，由 SQL runner 在真实部署上运行，开发期按影响声明选择运行，
收尾验证时运行，目前还不在自动准入中。
不为验证 SQL 的计算语义在 Rust 测试里起服务，也不为“快速路径”另立一种用例形式；进程与协议级行为仍由拥有该场景的
owner 启动真实进程。

| 要验证的性质 | 去处 |
|---|---|
| 单个函数、转换或运算的值、类型与失败 | Functions 的 Rust 测试；Functions 的 testkit 建成后使用它 |
| 算子与 pipeline 的局部行为 | Execution 的 Rust 测试 |
| 只有在编译产物与求值器之间才看得见的性质；需要特殊输入形状、注入控制或非法 package 才能触发的行为 | 接缝测试 package `tests/compile-to-execute` |
| 一条语句端到端的结果，以及依赖拓扑的行为 | SQL 套件，在 1FE+3BE 上运行 |
| 启动真实进程、走真实协议的行为 | 原生多节点测试与 System 场景；协议核心的纯逻辑仍是 owner crate 的 Rust 测试 |

预期由测试手写，独立于被测代码，不引入 golden 录制。这个分工接受一个代价：跨层问题，例如分析期推导的类型与
运行期结果类型不一致，发现得较晚；接缝测试 package 覆盖其中最常见的一段。

## 放置判定程序

按顺序回答六个问题。前四问决定位置，后两问决定怎样落笔。`cargo check --tests` 通过只说明编译可见，
不能代替第 1、2 问。

### 第 1 问：断言保护谁的性质

原则是：断言失败时，哪个 crate 承诺的行为没有兑现，测试就归哪个 crate。原则与下面的做法指向不同 crate 时，以做法为准。

**怎样判定。** 以测试函数为单位，看它调用的入口；经测试 helper 间接调用的也算，要穿过 helper 看里面的生产调用。
入口指执行行为的生产 API，例如编译、冻结、求值；只读取产物的访问器不算入口。文件位置、文件名和导入行都不是依据。

- 只调用一个 crate 的入口：owner 是那个 crate。
- 沿一条链依次调用多个 crate 的入口：owner 是最下游的那个，即链条上最后被调用的入口所在的 crate。
  它能不能留在那里由第 2 问决定。
- 往返测试（例如先编码再解码），或拿另一个 crate 的结果作参照的测试：看被断言的值由哪个入口产出，owner 是那个入口所在的 crate。

一个测试函数整体归一个 owner，不按断言拆开。链条中间对上游产物的检查是前置条件，随测试留下。
唯一的例外在[接缝测试 package](#接缝测试-package) 的三步次序里。

写新测试时先选层（见[选层](#选层)），并且一个测试函数只证明一个结论。要保护的行为如果在上游 crate 自己的入口就能观察到，
就写在上游；只有当行为要到下游才显现时才写在下游。这就是 test-governance 所说的最低充分层级。

**得到什么。** 一个 owner crate。

**反例。** 测试文件在 Execution 的目录里，只调用了 SQL 编译器，断言的是 SQL 产出的计划有几个参数。
“文件在哪个目录”和“用了哪个 crate 的类型”都不决定 owner；它只调用并观察了 SQL 的入口，所以归 SQL。

### 第 2 问：需要的 crate 是否都在允许范围内

允许范围由四类 workspace 成员组成：

1. owner 的生产依赖闭包，即从 owner 出发、沿 `[dependencies]` 能到达的全部 workspace 成员；
2. 闭包内 crate 的领域 testkit crate；
3. 机械支持 `novarocks-test-support`；
4. [输入契约生产者](#输入契约生产者)表中为 owner 列出的 crate。

第三方测试库不在这条规则之内，按普通依赖评审。查看生产依赖闭包：

```bash
cargo tree -p <crate> -e normal --locked --prefix none | grep '^novarocks' | sort -u
```

**怎样判定。** 列出测试二进制实际编入的全部 workspace 成员，包括经 testkit 间接带入的，都在允许范围内就进入第 3 问。
有越出范围的 crate 时，做一次替换检验，看测试的期望值是相对什么陈述的：

- 期望值相对 owner 自己的输入陈述，例如“这个程序对这几列求值得到这些值”。越界的 crate 只是用来把输入构造出来，
  换一种构造方式期望值不变。测试留在 owner，输入改在允许范围内构造，进入第 3 问。
- 期望值相对上游的原始输入陈述，例如“这条 SQL 文本求值得到这些值”。换掉上游的真实产物，期望值就失去了依据。
  这个性质按[选层](#选层)属于接缝那一行时，它属于对应的[接缝测试 package](#接缝测试-package)，位置判定到此结束，
  第 5、6 问在那个 package 内照常适用；一条语句端到端的结果归 SQL 套件，单个函数的值归 Functions。

拿不准时，看结论是否依赖上游对原始输入的解释，例如类型推导、隐式转换或计划选择：依赖的，期望值就是相对上游原始输入陈述的。
接缝两侧之间如果有生产依赖，上游本来就在下游的闭包内，不会走到这一步。

替换检验指向留在 owner、而这份输入在允许范围内造不出来时，看 owner 真正消费的是什么，改用它的替身。例如 Worker 的
任务流测试用局部编译器编出 `LocalProgram` 来实现执行宿主，而 Worker 消费的是 `TaskExecutionHost`，不是 `LocalProgram`：
测试改用允许范围内构造的宿主，期望值不变。替身保不住要断言的性质时，这条规则在真实代码上不可执行：
被改到的存量测试暂时保留现有依赖并在迁移记录里写明，按 ADR-0229 的再评估条件提出，不另起新的越界依赖。

**得到什么。** 确认测试留在 owner，或者进入接缝测试 package。任何情况下都不为测试新增允许范围之外的 dev 依赖。

**反例。** 在 Execution 的测试里写“SQL 文本经编译后求值”。Execution 的生产闭包里没有 SQL，
SQL 也不在它的输入契约生产者表里。把 SQL 加进 Execution 的 `[dev-dependencies]` 能让它编译，但从此任何 SQL 改动
都会选中 Execution 的全部测试。原因见[测试依赖方向](#测试依赖方向)。

### 第 3 问：能否经公开边界表达

**怎样判定。** 先找公开边界：断言观察的量和测试需要的输入，能否只用 owner 的公共 API 表达。

- 能：公开契约测试，进 `<crate>/tests/it/`。这是新写测试的首选。
- 不能：白盒测试，留在 `src/`，形态见[白盒测试的形态](#白盒测试的形态)，并写明它观察的内部性质。内部性质指公开入口
  观察不到的量：私有状态、内部共享与释放、公开入口到达不了的内部错误路径。输入只能经私有构造得到、又没有公开的
  等价写法时，也属于这一类。
- 需要本 crate 的领域 testkit：必须进 `<crate>/tests/it/`，原因见 [testkit 的“两份类型”规则](#testkit-的两份类型规则)。

判定的单位是测试函数。看的是生产代码：被测对象、观察点，以及搭建观察环境时必须调用、但在 crate 外不可达的生产项。
测试自己的 helper 与 fixture 不算，它们随测试一起移动；crate 内只为测试存在的 `cfg(test)` 共享模块也不算。
一个测试只因借用私有的构造简写而用到私有项时，先改用公开的等价写法，例如直接用公开的错误类型构造注入的失败，
而不是调用 crate 私有的简写函数。

生产可见性按真实调用方决定。不为了让测试进 `tests/it/` 放宽可见性，也不在本 crate 的 `tests/it/` 里使用
`*_for_test` 薄入口；薄入口只给其他 crate 的测试用，见[测试支持的暴露方式](#测试支持的暴露方式)。

**得到什么。** `tests/it/` 中的位置，或 `src/` 内的测试树。

**反例。** 为了让测试进 `tests/it/` 把一个内部函数改成 `pub`，加一个 feature 把它暴露出来，或者给它包一个
`*_for_test` 入口。位置服从可见性，不是反过来：确实需要私有项就留在 `src/`。

### 第 4 问：是否需要 target 级属性

**怎样判定。** 下面的清单是封闭的。测试需要其中任何一项，才有理由成为独立的集成 target：

- 自定义 harness（`harness = false`）；
- 全局 allocator；
- `required-features`；
- Loom 这类改变整个构建的运行模式。`--cfg loom` 会改变生产代码本身，例如换成 Loom 的同步原语、去掉部分项，
  普通测试在这种构建里不成立，所以按模式分 target。

nextest 的资源组、超时和 Miri 不在清单内：它们都能按测试名选择，例如在 `.config/nextest.toml` 的 filter 里加
`test(...)`，或给 `cargo miri test` 传测试名过滤。需要它们的测试沿用第 3 问的位置，公开契约测试与白盒测试都一样。

**得到什么。** 需要其中一项、且第 3 问的答案是公开契约时，放进该 crate 中理由相同的那一个独立 target；
还没有就新建 `<crate>/tests/<name>/main.rs`，并在 target 根的顶部用一行英文注释写明是哪一项。不用 `tests/<name>.rs`
作为有子模块的 target 根：它的子模块解析到 `tests/` 顶层，写成 `tests/<sub>.rs` 时会被 Cargo 当成另外的 target。理由相同指同一项属性且取值相同，
例如同一个 allocator、同一组 `required-features` 或同一种运行模式；这样的独立 target 在一个 crate 中只有一个，各主题作为它的子模块。
不需要时沿用第 3 问的位置。集成 target 的组织细节见[公开契约测试与集成 target](#公开契约测试与集成-target)。

第 3 问优先。Loom 模型如果同时需要私有项，留在 `src/`，放进被测模块测试树里按模式门控的模块，例如在
`foo/tests.rs` 里声明 `#[cfg(loom)] mod loom;`，不为了成为独立 target 而放宽可见性。独立 target 的现有例子是
`novarocks/control-runtime/tests/loom/main.rs`（6 个模型族是它的子模块）；`src/` 内现有的
`novarocks/memory/src/lane_loom.rs` 等由 `lib.rs` 逐个声明的平铺文件属于旧布局，不作范例。

全局 allocator 作用于整个测试二进制，一个二进制只能有一个。白盒测试声明的 allocator 对这个 crate 的全部 lib 单元测试生效，
所以它必须转发给 `System`，只在当前测试经线程局部开关启用时计数或注入失败，其余测试不受影响。
`novarocks/functions/src/source_custody_resources_tests.rs` 与 `novarocks/proto-codec/src/scalar_result.rs` 的测试模块
是这种写法（它们的文件布局属于旧布局）。需要 allocator 的白盒测试复用 crate 里已有的那一个，没有时按这种写法在测试树里新建。
自定义 harness 只能作用于整个 target：需要它的性质改写成公开契约测试，进 harness 相同的独立 target。

**反例。** “这些测试会互相干扰全局状态，所以各自一个 binary。”仓库的规范执行器 nextest 每个测试一个进程，
同一个 binary 里的测试并不共享进程状态，这不构成理由。直接用 `cargo test` 时同一个 binary 的测试共享一个进程：
合并依赖进程级全局状态的测试之前，确认它们在 `cargo test` 下也成立，或者让它们自行隔离，例如重新启动自身。

### 第 5 问：是否已有承载它的主题

**怎样判定。** 在 owner crate 里找承载同一个行为轴的测试文件。行为轴是被测对象的一个行为维度，
例如空值语义、溢出、取消；可以把它理解为读者会对这个对象提的一个问题，例如“它怎样处理 NULL”。

- 有，而且它就在第 3、4 问得到的位置上（同在 `src/` 的测试树里，或同在 `tests/it/`，或同在理由相同的独立 target 里）：
  把用例加进那个文件。判断标准是文件里已有的用例是否在回答同一个问题，不看文件名。一个文件里的用例各自回答不同的问题时，
  它不是现成主题；以层次、契约或工作项命名的文件通常属于这种情况。
- 只在旧布局的文件里有，例如 `src/` 下平铺的 `foo_tests.rs`、生产 `mod.rs` 逐个声明的测试模块，或既没有 target 级理由、
  也不是按领域拆分出的公开契约 target 的顶层 `tests/<name>.rs`：新用例不加进旧文件，放到规范位置。可以在同一个改动里把那个旧文件整体挪到规范位置，
  按[移动或删除测试时的同步](#移动或删除测试时的同步)处理，但不强制。
- 没有：在第 3、4 问得到的规范位置新建文件，按行为轴命名。新建之前要能回答“为什么没有现成主题可容纳”。

**得到什么。** 一个具体的文件。写 plan 时把它记为测试落点，见 [plan 中的测试落点](#plan-中的测试落点)。

**反例。** 为一项工作新建 `foo_phase2_tests.rs`，里面的用例其实都是 `foo` 的空值语义。它们应当加进承载空值语义的文件。

### 第 6 问：输入从哪来

**怎样判定。** 沿 [fixture 归属阶梯](#fixture-归属阶梯)由近到远，取到够用的那一级为止。

**得到什么。** 输入的位置和作者。

**反例。** 另一个 crate 里有一个差不多的构造函数，于是复制一份过来，或者用 `#[path]` 把那个文件接进来。
同一份领域输入只有一个作者；它若放在不该依赖的位置，由需要它的任务把它移到事实 owner，原位置改为转调，
见[测试支持的暴露方式](#测试支持的暴露方式)末尾。

## 白盒测试的形态

白盒测试默认放在独立文件。生产文件里只留一行入口：

```text
foo/mod.rs                  # production code; declares `#[cfg(test)] mod tests;`
foo/tests.rs                # white-box assertions, or the root of the test tree
foo/tests/
    slicing.rs              # one behaviour axis per file
    nullability.rs
    fixtures.rs             # inputs and the check entry for this tree only
```

`foo.rs` 或 `foo/mod.rs` 里的 `#[cfg(test)] mod tests;` 都解析到 `foo/tests.rs`，它的子模块解析到 `foo/tests/`。
两种模块入口都可以：一个具名实现文件加一个测试模块时，沿用 `foo.rs` 与 `foo/tests.rs`；已有目录式的子系统沿用 `mod.rs`。
现有的例子是 `novarocks/value-arrays/src/storage/mod.rs` 与同目录的 `tests.rs`，以及
`novarocks/functions/src/arithmetic/decimal.rs` 与 `arithmetic/decimal/tests.rs`。

独立文件的理由是构建行为。非测试构建不读取 `tests.rs`，修改它不会让这个 crate 的库和所有下游失效；
修改写在生产文件里的内联测试则会。底层 crate 的下游很多，这个差别直接决定改一个测试要重编多少东西。

规则：

- 内联 `mod tests { ... }` 只用于不需要任何 helper 的少量断言。一旦需要构造函数、fixture 或第二个主题，移到独立文件。
  Functions 里有两组实例（路径相对 `novarocks/functions/src/`）：`builtin/value_conversion.rs` 与 `builtin/round_cast_text.rs`
  的内联块各有多个场景和顶层 helper，已移到各自的 `tests.rs`；`decimal_text.rs` 与 `date_float_cast.rs` 的内联块
  都只有一个短断言、没有 helper，保留内联。行数不是判断标准。向一个已经带 helper 的内联块加用例时，先把它整体移到独立文件。
- 测试主题的清单放在测试树里：测试树的根（`tests.rs`）声明它的子模块，不在生产 `mod.rs` 里逐个列出
  `#[cfg(test)] mod xxx_tests;`。已有的 `tests` 模块要承载多个主题时，它自己成为根，继续保留原有的断言与 helper；
  子文件用显式的 `super::super::` 访问被测模块，不经根的通配导入转取，以免根里的 helper 遮蔽被测模块的同名私有项。
  `novarocks/functions/src/cast/tests.rs` 与 `cast/tests/` 是这种写法。
- 不用 `#[path]` 把兄弟文件接成测试模块，不新建 `foo_tests.rs` 这样的平铺兄弟文件。生产模块经 `#[path]` 从非标准位置
  加载、又要接测试树时，先把生产文件移到标准位置（只改路径，内容不变），再用标准写法接入。`#[path]` 有真实理由时保留，
  例如按平台或 cfg 选择实现文件，或者这个文件被多个 package 经 `#[path]` 引入；这时测试入口写成
  `#[cfg(test)] #[path = "<实现文件名>/tests/mod.rs"] mod tests;`。经 `#[path]` 加载的文件按 `mod.rs` 的方式解析子模块：
  不写路径时 `mod tests;` 会解析到同目录的 `tests.rs`，几个实现文件会互相冲突；测试树的根本身也经 `#[path]` 加载，
  所以放在 `<实现文件名>/tests/mod.rs`，主题文件与它同在 `<实现文件名>/tests/` 下。
- 白盒测试写明它观察的内部性质：在测试函数或测试树根的文档注释里用一句英文说明，例如“two internal conversions
  share the target storage until the last reference is dropped”。
- 已有的合理内联测试不需要为了统一外观而迁移。

## 公开契约测试与集成 target

公开契约测试默认集中在一个集成 target 里，子模块按行为轴命名：

```text
<crate>/tests/it/main.rs    # mod scan; mod output_contract;
<crate>/tests/it/scan.rs
<crate>/tests/it/output_contract.rs
<crate>/tests/it/fixtures/mod.rs
```

Cargo 把 `tests/it/main.rs` 编成一个名为 `it` 的集成测试。每个集成 target 是一个独立的链接单元，
`tests/` 下每个顶层文件一个 binary 会把链接成本乘以文件数，所以不为每个主题建一个顶层文件。
现有的例子是 SQL、Query Application、Functions、Control Runtime 与 Value Arrays 的 `tests/it`。

**显式模块清单。** `tests/it/main.rs` 与测试树的根逐个声明子模块，Cargo 不会自动发现它们。新文件必须同时写 `mod` 行，
否则它不编译、不检查、也不运行，没有工具会报告。评审时核对新文件已经登记；移动测试时核对每个文件恰好被声明一次。

**按领域拆分。** 一个 binary 不是终点。只有在实测“改一个测试”的重编代价成为负担，或者其中一部分测试需要不同的
cfg、`required-features` 或运行模式时，才拆成少数几个按领域命名的 target；dev 依赖对一个包的所有 target 相同，不能作为拆分理由。
它们共用的输入放进 `fixtures` 模块或 testkit，而不是在各个 target 里各留一份。测试或文件的数量本身不是拆分的理由。
按领域拆分出的 target 是公开契约 target 的正式形态，不属于旧布局；拆分时在改动中附上实测数据。可参照的本地数据只有一组：Control Runtime
把 41 个集成 target 合成 `it` 与 `loom` 之后，改一个测试文件后的重编从约 0.23 秒变为约 0.60 秒，改一个库文件后的重编
从约 1.95 秒变为约 0.98 秒，测试二进制从 42 个、约 87 MB 变为 3 个、约 13 MB（同一台 Mac，中位数）。它只代表一个
依赖很浅的小 crate。

**合并有 target 级理由的 target。** 同一 crate 中理由相同的独立 target 合成一个，原文件成为子模块。合并不能降低原有的保证：
原先按 target 提供的完整性检查要在新的粒度上保留。例如 Loom 模型合进一个 target 后，
`tools/ci/control-runtime-model-checks.sh` 检查每个模型族至少有一个通过的模型，而不只是整个 target 有模型通过。

**crate 级属性。** 运行模式的门控写在 target 的根上，例如 `novarocks/control-runtime/tests/it/main.rs` 的
`#![cfg(not(loom))]` 与 `tests/loom/main.rs` 的 `#![cfg(loom)]`，新模块自动继承。原来写在各文件顶部的其他 crate 级属性
（例如 `#![expect(...)]`）留在原文件成为模块级属性，不提到根上，以免扩大作用范围。只能写在 crate 根上的属性
（例如 `#![recursion_limit]`）例外：它们提到根上，并在改动中说明作用范围因此扩大。

**重新启动自身的测试。** 有的测试会以子进程方式重新运行自己的测试二进制，只执行某个入口测试。子进程的测试路径
必须由 `module_path!()` 去掉 crate 段后拼出，不能写死；入口测试在开头输出一行标记，父测试先确认标记出现，再判断
子进程的结果。这样测试移动到别的模块后不会静默空跑。入口测试只在父测试为子进程设置的环境变量存在时才执行，
否则直接返回，作为普通测试运行时不做任何事；标记是同一文件里输出方与检查方共用的常量。生产代码输出、由测试消费的日志标记
仍按 AGENTS.md 在 `novarocks/failpoint/src/observation_marker.rs` 登记。`novarocks/control-runtime/tests/it/actor_failure_policy.rs`
是这种写法。

### testkit 的“两份类型”规则

设 U 是事实 owner，`U-testkit` 依赖 U。U 的 lib 单元测试如果使用 `U-testkit`，测试二进制里就有两份 U：
被测的那份，和 testkit 链接的那份。两份里的同名类型互不相同：U 的类型跨越 testkit 与被测代码的边界时编译失败；
只传 std 或 Arrow 类型时仍能编译，但两份各有自己的静态状态，测试观察到的可能是另一份。编译器并不总能拦住，评审要核对。

| 使用方 | 能否使用 `U-testkit` | 原因 |
|---|---|---|
| 下游 crate 的单元测试与集成测试 | 能 | 只链接一份 U |
| U 的 `tests/it` | 能 | 集成测试链接 U 的 lib，与 testkit 用的是同一份 |
| U 的 lib 单元测试 | 不能 | U 被编译两份 |

由此得到三条规则：

- 需要本 crate 领域 testkit 的测试必须是 `tests/it`。
- lib 单元测试不能用本 crate 的 testkit crate。一个单元测试如果需要只有本 crate testkit 才提供的领域输入、又不需要私有项，
  说明它断言的是公开契约，应当移到 `tests/it`；既需要私有项又需要这份输入时，把输入放进本 crate 的门控 testkit 模块。
  白盒测试经允许范围内其他 crate 的公共 API 取得完整输入是可以的，例如 Execution 的白盒测试经局部编译器取得 `LocalProgram`。
- 同一份输入既要给 U 的白盒测试用、又要给 U 之外的测试用时，不放 testkit crate，放 U 内的门控 testkit 模块，
  见[测试支持的暴露方式](#测试支持的暴露方式)。

这样同一份领域输入不会出现两个作者。

## 测试依赖方向

**规则。** 一个 crate 的测试，无论 lib 单元测试还是 `tests/it`，只能依赖第 2 问列出的四类 workspace 成员。
测试依赖图只在“输入契约生产者”这一类上比生产依赖图宽。

**为什么。** 依赖是按 crate 声明的，不是按测试声明的。只要测试二进制里有一个用例需要某个允许范围之外的 crate，
这条 dev 依赖就对整个 crate 成立，带来三个后果：

- 开发期的测试选择按反向依赖计算，不区分生产依赖和 dev 依赖。对方一改，这个 crate 的全部测试都被选中。
- 这个 crate 的测试二进制要链接对方的整个栈，对方一改就要重新编译和链接。
- 失败报告指向这个 crate，而要修的可能是对方。

**何时起作用。** 组合的两侧之间有生产依赖时，规则与“归下游 owner”结论相同。例如 Query Application 生产依赖 SQL，
“SQL 的产物被 Query Application 冻结”这类测试放在 Query Application，不产生越界的依赖。
两侧之间没有生产依赖、并且测试的期望值相对上游的原始输入陈述时（第 2 问的替换检验），
它不属于任何一侧，进入接缝测试 package。

### 输入契约生产者

有些 owner 的输入类型定义在自己的生产闭包内，却只能由另一个 crate 产生。对这样的 owner，允许它的测试依赖那个生产者。
适用范围由下表显式给出，不由写测试的人自行推断：

| owner | 输入契约 | 允许依赖的生产者 | 生产组合点 | 依据 |
|---|---|---|---|---|
| `novarocks-execution` | `LocalProgram`（`novarocks-local-program`） | `novarocks-local-compiler`，及其生产闭包内的 `novarocks-physical-plan` | BE 一侧 `novarocks-native-adapter`；FE 一侧 `novarocks-frontend-application` 的常量求值 | 没有测试从零构造 `LocalProgram`；至少 72 个测试文件经物理计划片段与局部编译器取得输入，其中很多是白盒测试 |

新增一行需要同时满足四个条件，并在改动中给出证据：

1. 契约类型定义在 owner 的生产闭包内；
2. 它没有可行的闭包内构造方式；
3. 生产者的公开生产 API 直接产出该类型，并且生产上由同一角色的组合点把生产者的输出交给 owner；
4. 生产者自己的生产闭包不含另一角色一侧的 crate。

新增一行是一次普通的指南修订；改变这四个条件要写新的 ADR，部分替代 ADR-0229。SQL、parser 与应用层 crate 不在表内，
以 SQL 文本为输入的测试仍按替换检验归 owner 或进接缝测试 package。Plan Codec 也不在表内：Execution 不消费 package。
检查 package 编解码本身、或要靠非法 package 触发的测试属于接缝测试；package 只是用来构造输入的，按替换检验去掉 package
往返，改为从物理计划片段经局部编译器取输入，留在 Execution。

这样做保留了规则要防的事：表里只有本来就影响 owner 输入的 crate，改局部编译器或物理计划时选中 Execution 的测试是合理的，
SQL、parser 与应用层 crate 的改动不会因为这张表而选中 Execution 的测试。这一点要等 Execution 的过渡期越界依赖迁走后
才完全成立，见[接缝测试 package](#接缝测试-package) 末尾。

**Execution 自己的输入。** Execution 的算子与求值器测试经物理计划片段与局部编译器取得 `LocalProgram`，例如
`novarocks/execution/src/exec/expr/f3_concat_trim_sql_boundary_tests.rs` 里的 `compiled_slot_program`。
这类测试的 owner 是 Execution，依赖合规，在 Execution 内按第 3 问放置，不进接缝测试 package。

### 接缝测试 package

它是只含测试的 workspace 成员，按一条真实的产品接缝命名，依赖接缝两侧和它们的 testkit，不被任何成员依赖
（过渡期的例外见本节末尾）。它不是通用的测试 crate：数量由产品接缝的数量决定，每个 package 只放穿过那条接缝、
并且按[选层](#选层)属于接缝的断言：期望值相对上游原始输入陈述，或者要靠特殊输入形状、注入控制或非法 package 才能触发。

接缝测试 package 的允许范围是接缝两侧、连接两侧所需的中间 crate，以及它们的 testkit 与机械支持。新建一个接缝测试 package
是新的长期结构决策，先在对应任务中明确设计，不在写测试时顺手新建。

当前唯一的接缝是“编译到执行”：SQL 文本经 SQL 编译、package 编码和局部编译之后，在 Execution 中求值，允许范围即 SQL、
Plan Codec、局部编译器、物理计划与 Execution 及其 testkit。
对应的 package 是 `tests/compile-to-execute`（`novarocks-test-compile-to-execute`）：lib 承接“SQL 计划到 `LocalProgram`”
这半条链路的 helper（`src/programs.rs`、`src/compiler_results.rs`），`tests/it` 承接测试。从物理计划片段经局部编译器
取输入的测试不穿过这条接缝，归 Execution。

**接缝测试需要一侧的私有项时**，按顺序判断：

1. 先改用公共途径观察同一个量，期望值不变；一侧的普通 `pub` API 或现有的开发 cfg 门控入口都算公共途径。
2. 不行就把这条断言拆回那一侧，作为那一侧的测试，输入改在那一侧的允许范围内构造；接缝 package 里保留或新增一条断言，
   说明真实编译产物具有那份输入所假设的形状。拆分要给出断言对应。
3. 只有性质本身跨接缝、无法拆开时，才在那一侧加开发 cfg 门控的 `*_for_test` 薄入口。

**过渡期的依赖。** 接缝 package 与 SQL 的编译输入建立之前，Execution 里已有不少测试依赖这些输入与链路 helper。
它们尚未迁走，Execution 因此暂时 dev 依赖 `novarocks-sql-testkit` 与接缝 package 的 lib，并在原位置保留转调，
例如 `novarocks/execution/src/exec/expr/sql_test_source.rs`。这还不是全部：Execution 的 dev 依赖里，除生产者表中的
`novarocks-local-compiler` 与 `novarocks-physical-plan` 外，其余 workspace 成员都越出允许范围，包括 `novarocks-sql`、
`novarocks-parser`、`novarocks-query-application`、`novarocks-plan-codec` 等。新测试不经由转调，也不使用这些依赖；
转调与依赖在最后的使用方随改动迁走时删除，已经没有使用方的依赖直接删除。它们迁走之前，改这些 crate 仍会选中
Execution 及其下游的测试。

## fixture 归属阶梯

由近到远，用到够用的那一级为止。

| 级 | 何时用 | 位置 | 约束 |
|---|---|---|---|
| 就地 | 只有一个测试用 | 测试函数内或同一文件内 | 无 |
| 测试树 | 同一棵测试树内共用 | 树内的 `fixtures` 模块 | 只服务这棵树 |
| 同 crate | 同一 crate 内多棵测试树共用 | 它们公共祖先下的 `cfg(test)` 模块 | 不进生产构建 |
| owner 内的门控模块 | 同一份输入既被 owner 的白盒测试用、又被 owner 之外的测试用；或 owner 之外的测试必须经私有项才能做到 | owner 内开发 cfg 门控的 `pub` testkit 模块，或 `*_for_test` 薄入口 | 只用 owner 的生产依赖；薄入口不写输入构造；生产代码不引用 |
| 领域 testkit crate | 第二个 crate 实际消费同一份领域输入，只用公共 API 且 owner 的白盒测试不用它；或需要 owner 生产闭包之外的依赖 | 事实 owner 持有的独立 crate | 只作为 dev 依赖；遵守“两份类型”规则 |
| 机械支持 | 只描述进程、端口、I/O 等机械资源 | `tests/test-support` | 不放领域输入 |

同名的 fixture 不一定该合并。先比较语义和输入责任：同一事实的重复构造收口到一处，语义不同的本地 stub 即使长得相似也各自保留。

testkit 构造领域输入，也可以按 ADR-0140 承载跨实现共用的行为 suite，例如 `novarocks/state-store/testkit` 里供各 provider
调用的一致性检查，其中的预期按契约手写。它不实现被测的消费规则，也不给出由被测代码算出来的预期。

“同 crate”一级只服务 `src/` 内的测试树；`tests/it/` 是另一个 crate，看不见 `cfg(test)` 模块，用自己的 `fixtures` 模块。
同一份 helper 既被留在 `src/` 的白盒测试使用、又被本 crate `tests/it/` 的测试使用时，放进本 crate 的门控 testkit 模块，不复制。
门控模块只能使用 owner 的生产依赖；这份 helper 需要生产闭包之外的依赖时，它构造的是别的 crate 的事实，放进那个事实 owner
持有、不依赖本 crate 的 testkit crate，见[测试支持的暴露方式](#测试支持的暴露方式)第 4 步。

### 测试支持的暴露方式

一份要给 owner 之外的测试使用的支持代码，按顺序判断它放在哪里。owner 自己的 `tests/it/` 也算 owner 之外，它是另一个 crate。

1. 它是运行期的控制或观察接缝吗，例如 `with_test_controls`、观察器、故障钩子？是，则不属于本指南，按
   [测试控制 ADR-0175](../../adr/ADR-0175-test-capabilities-and-finite-role-authorization.md) 与
   [ADR-0181](../../adr/ADR-0181-unified-development-capabilities-and-executable-provenance.md)。
2. 它必须触及 owner 的私有项吗？是，则在 owner 内放开发 cfg 门控、以 `_for_test` 结尾的 `pub` 函数，只转调私有项，
   不写输入构造逻辑。例如 `novarocks/execution/src/exec/operators/compiled_expression.rs` 的
   `prepare_project_factory_for_test`，供 Native Adapter 的测试使用。如果这份支持本身是一份被 owner 白盒测试共用的输入构造，
   直接按第 3 步放进门控 testkit 模块：门控模块在 owner 内，可以使用私有项。本 crate 的 `tests/it/` 不使用薄入口。
3. 同一份输入也被 owner 的 `src/` 白盒测试使用吗？是，则放在 owner 内开发 cfg 门控的 testkit 模块，保持单份。例如
   `novarocks/plan-codec/src/physical_package_v2.rs` 的 `test_support` 模块，被 Plan Codec 自己的白盒测试和其他 crate 的测试共用。
   门控模块只能使用 owner 的生产依赖；需要闭包之外的依赖时转到第 4 步。
4. 其余情况放 testkit crate；需要 owner 生产闭包之外的依赖时，也只能用 testkit crate。例如 `novarocks/sql/testkit`
   （`novarocks-sql-testkit`）只用 SQL 的公共 API 构造编译输入，SQL 自己的白盒测试不使用它。testkit crate 带入的依赖
   同样要落在每个消费者的允许范围内。要给某个 crate 自己的白盒测试用的 testkit crate 不能依赖那个 crate（见“两份类型”规则），
   它由所构造事实的 owner 持有。例如 Execution 白盒测试与 `tests/it` 共用、经局部编译器构造 `LocalProgram` 的 helper，
   放进 `novarocks-local-program` 的领域 testkit crate：Local Program 在 Execution 的生产闭包内，这个 testkit 带入的
   局部编译器与物理计划都在 Execution 的允许范围内，它也不依赖 Execution。目前还没有这个 crate，由第一个需要它的改动新建。

门控代码的写法与约束：

- owner 的白盒测试也要用时写 `#[cfg(any(test, novarocks_dev_controls))]`，只给其他 crate 用时写 `#[cfg(novarocks_dev_controls)]`。
  `novarocks_dev_controls` 由实际的 debug assertions 决定，不是 Cargo feature，标准 release 不包含它，机制见 ADR-0181。
- 这个 cfg 只在 `build.rs` 调用共享 build helper 的 crate 里存在：`build.rs` 经 `#[path]` 引入
  `build-support/development.rs` 并调用 `development::emit()`，写法见 `novarocks/execution/build.rs`。不是每个 crate 都有它；
  在没有的 crate 里放门控代码之前，先在同一个改动里加上这个 `build.rs`。否则 lib 单元测试会因 `unexpected_cfgs` 在
  Clippy 中失败，本 crate 的 `tests/it` 也看不到门控项。
- 门控代码编在库的非测试构建里，只能使用 owner 的生产依赖；修改它会让 owner 及其下游在 dev 构建中重编。
  所以既不被 owner 白盒测试使用、也不需要私有项的输入构造不放进门控模块。
- 生产代码不得引用它。按仓库约定，Rust 测试只在 dev 配置下构建运行，消费门控支持的测试随之只在 dev 下成立，不另加门控。
- 薄入口或转调失去最后一个使用方时，在同一个改动里删除。

始终不采用的做法：

- 复制 helper；
- 用跨 package 的 `#[path]` 共享测试源码或编入另一个 crate 的生产实现；
- 用生产 crate 的 feature 或无条件的 `pub` 暴露测试支持。

**输入放错位置时。** 新测试需要的输入只存在于它不该依赖的位置时，由这个任务把输入移到事实 owner，原位置改为转调，
现有使用方不必同时改动。事实 owner 是定义并解释这份输入所表达事实的 crate：SQL 编译产物属于 SQL，内置函数目录属于 Functions；
由几部分组成的输入按部分归属，由使用它的测试树或 testkit 组装。例如 `novarocks-sql-testkit` 里的内置函数目录 helper
只为一个控制替身依赖 SQL，它的事实 owner 是 Functions。

新建领域 testkit 时参照 `novarocks/state-store/testkit/Cargo.toml` 的形态：

```toml
[package]
name = "novarocks-<domain>-testkit"
version.workspace = true
edition.workspace = true
license.workspace = true

[lints]
workspace = true

[lib]
doctest = false

[package.metadata.novarocks.test]
runtime-inputs = []
env-inputs = []
```

把它加入根 `Cargo.toml` 的 `members` 与 `[workspace.dependencies]`，消费者只在 `[dev-dependencies]` 里引用。
testkit 是否只被当作 dev 依赖使用，由 manifest 评审核对；拆成独立 crate 本身不会阻止一条错误的普通依赖。

## 形态与增长规则

位置规则决定文件在哪，下面四条决定文件怎样长。它们约束新写的测试，以及随改动迁移之后的测试；
没被改到的存量测试不为此改写。

**按行为轴命名，长期存在。** 测试文件以被测对象的一个行为轴命名。文件名不含工作项、阶段、PR 或 ADR 编号，
也不含“之前”“之后”“基线”这类只在当时成立的词。

| | 文件名 |
|---|---|
| 反例 | `f3_concat_trim_sql_boundary_tests.rs`、`inlist_signed_actual_after_tests.rs` |
| 正例 | `tests/nullability.rs`、`tests/overflow.rs` |

**新工作向既有主题累加。** 一项工作需要新用例时，先加进承载该行为的既有文件。只有没有现成主题可容纳时才新建。

**每棵测试树一个 check 式入口。** 入口接收输入、返回或比较被观察的值，输入的构造细节放在 `fixtures` 里。
用例体只写这个用例的数据与期望；同一性质的多组数据用表驱动。下面是示意：

```rust
// fixtures.rs
/// Evaluates `function` over `columns` and returns the result column as strings.
pub(super) fn evaluate(function: &str, columns: &[&[Option<&str>]]) -> Vec<Option<String>> {
    // build the program once, here
}

// nullability.rs
#[test]
fn concat_is_strict_on_null() {
    for (a, b, expected) in [
        (Some("x"), Some("y"), Some("xy")),
        (Some("x"), None, None),
        (None, None, None),
    ] {
        assert_eq!(evaluate("concat", &[&[a], &[b]]), [expected.map(str::to_owned)]);
    }
}
```

这是写法约定，不是测试框架：不引入宏、DSL 或运行器。反例是每个测试函数各自重复一遍几十行的构造代码，
用例之间真正不同的只有两三个值。

**预期独立于被测代码。** 期望值来自规范、独立实现或手工推导，不由被测代码或与它共享实现的 helper 计算。

### plan 中的测试落点

为一项工作写 plan 时，每个新增测试的 task 写明它的测试落点：

- 扩展哪个既有主题文件；或者
- 新建哪个文件，以及为什么没有现成主题可容纳。

如果测试需要新的 dev 依赖，同时写明它属于允许范围的哪一类。

如果这项工作会改到存量测试，plan 同时列出：迁移哪些测试、迁到哪里；删除哪些测试，以及它们保护的每条性质由哪个测试接手，
或者随哪项退役不再成立，见[存量测试的迁移](#存量测试的迁移)。

评审 plan 的人按本指南的判定程序核对落点。这是规则生效的位置；仓库不用源码扫描检查文件名或位置。

## 命名与词汇

`test_support` 这个词根在仓库里曾经同时指两种相反的责任。本指南把它们分开：

| 责任 | 用词 | 位置 |
|---|---|---|
| 领域输入与本树的 check 入口 | fixtures | 测试树内，或 crate 内的 `cfg(test)` 模块 |
| 跨 crate 的领域输入，以及跨实现共用的行为 suite | testkit | `novarocks/<domain>/testkit`，或 owner 内开发 cfg 门控的 `testkit` 模块 |
| owner 之外的测试需要的私有入口 | `*_for_test` | owner 内开发 cfg 门控的 `pub` 函数 |
| 进程、端口、I/O 等机械资源 | support | `tests/test-support` |

`tests/` 目录下的文件按行为轴命名，不带 `_test` 或 `_tests` 后缀。这适用于白盒的 `foo/tests/` 和公开契约的 `tests/it/`。
有 target 级理由的独立 target 按运行模式或行为轴命名，例如 `loom`，它的子模块不再重复模式后缀。
命名规则约束文件与模块，不要求重命名测试函数。crate 内多棵测试树共用的 `cfg(test)` 模块叫 `fixtures`。
现有名为 `test_support` 的领域模块（包括门控的）在随改动迁移时改名。

## 存量测试的迁移

旧布局的测试不成批迁移。哪个开发任务改动到它，就由这个任务在同一个改动里把它迁到本指南的规则下；
没被改到的保持现状，包括仍在使用越界 dev 依赖的那些。专门的整理，例如为一个反复拖累开发的热点合并集成 target，
只在单独立项之后进行，并按本节同样的规则核对。

本指南 2026-11-17 定稿之前已获批的 plan 不追溯：其中写明的测试落点照常执行；plan 修订时，按本指南重新核对修订到的落点。

**何时迁。** 一个任务在下列情况下迁移相应的存量测试：

- 它要改变某个存量测试断言的行为，或这个测试保护的契约；
- 它让某个存量测试所测的类型、入口或路径退役；
- 它新写的测试替代了某个存量测试的保护。

只因生产代码改名、移动而做的导入与路径修正不算。

**怎样迁。** 对每个被改到的测试函数，重新走一遍判定程序：

1. 先按[选层](#选层)确认这个性质该在哪一层。与现在的层不同时，例如接缝 package 里只保护单个 owner 计算语义的值断言，
   改写到那一层的 owner；断言一条语句端到端结果的，交给 SQL 套件，并实际运行对应的 suite。SQL 套件目前不在自动准入中，
   记录里写明这条保护改由它承担。被替代的旧测试按下面删除旧测试的规则处理。
2. 第 1 问给出的 owner 可能与现在的位置不同，例如 Execution 目录里只断言 SQL 产物的测试，迁到真实 owner。
3. 第 3 问先找公开边界：能经公开入口表达的，改写到公开入口，进 owner 的 `tests/it/`；否则留在 owner 的测试树，
   并写明观察的内部性质。
4. 留在原层的测试，改写只动入口、输入与注入失败的构造方式、helper 的位置和导入；入口只能换成同一个 owner 的公开入口，
   不借改写改变 owner。改层的测试按第 1 步替代旧测试，不适用本步。
   输入值、期望值及其独立来源、测试函数名、`cfg` 与 `#[ignore]` 保持不变；因本次行为改变而改动的期望值单独标出，并说明依据。
   不借迁移合并测试或删减断言。
5. 迁移之后的测试按新测试对待：此后加入的用例遵循[形态与增长规则](#形态与增长规则)；迁移当次不为套用形态而合并或拆分测试函数。

**删除旧测试。** 被替代或已废弃的旧测试在同一个改动里删除。删除时逐项写明它保护的每条性质现在由哪个测试断言，
或者随哪项退役不再成立、为什么；写在 plan 的测试落点和 PR 说明里，评审按这份记录核对。随同一项退役而删除的测试
可以合并成一条记录，写明退役项与被删测试的范围。处置清单里有记录的测试
被删除或改名时，同步处理对应的处置，不能借删除或改名清掉历史失败。

**只挪位置。** 只移动、不改写时，例如第 5 问允许的顺手挪动，按纯位置迁移处理：输入、断言、测试函数名、`cfg`、
忽略状态与生命周期检查保持不变；按每种运行模式核对原生发现列表的旧新身份逐项对应；按下一节同步耦合点；
核对没有孤儿文件。只挪位置时可以把文件改成行为轴的名字，或按行为轴把一个文件里的测试函数分到几个文件，函数本身不变。

**与表达式层的关系。** 运算符与 CAST 迁入表达式层框架时，测试由执行迁移的任务写进新测试框架，
旧测试在同一个改动里删除，删除时同样写明性质去处。

**首次使用时验证的步骤。** 下面几项的规则已定，但具体步骤还没有在真实代码上走过。第一个用到它的任务按本指南执行并核对结果；
需要补充步骤时直接修订本指南：

- 迁移带自定义 harness、全局 allocator 或 `required-features` 的 target；
- 合并只为 Miri、资源组或超时而存在的 target，把 `.config/nextest.toml` 与 Miri 包装器里按 binary 的选择改写成按测试名选择；
- 处置清单的 Rust 单元键随测试身份改名；
- 接缝测试三步次序中的第 2、3 步；
- 新建门控 testkit 模块。

## 移动或删除测试时的同步

nextest 用 binary ID 标识测试：lib 单元测试是 `<package>`，集成 target 是 `<package>::<target>`；测试名带着模块路径。
一个测试从 `src/` 移到 `tests/it/`，binary ID 会从 `<package>` 变成 `<package>::it`；移到接缝测试 package 则变成
`novarocks-test-compile-to-execute::it`；换模块时测试名也随之改变。

移动、改名或删除测试时，在同一个改动里核对并同步：

- `.config/nextest.toml` 中按 `binary(...)` 选择的资源组与超时；
- `tools/ci/baselines/dispositions.json` 中以 `rust-test:` 开头的处置键；
- 按 target 名选择、检查或运行的 CI 包装器，例如 Loom 包装器的模型族清单；新建或合并按模式运行的 target 时，包装器要同步覆盖它；
- 重新启动自身的测试的子进程路径，它们应当已由 `module_path!()` 推导；还没有的，先在原位置单独加固并验证，再移动；
- `tests/it/main.rs` 与测试树根的模块清单，以及没有被声明的孤儿文件；
- 经 `include!` 或 `#[path]` 接入的测试文件。

新建测试通常不涉及前两处。新测试需要资源组或更长的超时时，在 `.config/nextest.toml` 里按测试名选择，不为此拆出 target。

## 走查用例

下面的用例演示判定程序。A～D、J～L、O、P 取自现有代码，其余是假设的新测试。

**A：SQL 编译 `SELECT concat(a,b,c) FROM t` 后检查计划的参数个数与类型顺序。**
现位于 `novarocks/execution/src/exec/expr/f3_concat_trim_sql_boundary_tests.rs`。第 1 问：它只调用 SQL 编译器这一个入口，
之后只是读取产物，owner 是 SQL。第 2 问：只需要 SQL 闭包内的 crate。第 3 问：断言与输入都能经 SQL 的公共 API 表达。
第 4 问：不需要。结果：SQL 的 `tests/it/`，输入取自 `novarocks-sql-testkit`。它目前是存量，由改动到它的任务迁走。

**B：搭一个只含 `concat` 的物理计划片段，经局部编译器得到 `LocalProgram`，检查求值器在稀疏选择下按编译出的槽位读写，
输出行与选择向量一一对应。**
与 A 在同一个文件里，经 `compiled_slot_program` 取得输入。选层：槽位与选择向量是 Execution 求值器的局部行为。
第 1 问：链条上最后被调用的是 Execution 的求值入口，owner 是 Execution。第 2 问：物理计划与局部编译器不在 Execution 的
生产闭包内，但在它的输入契约生产者表里，依赖合规。第 3 问：求值入口与观察的结果都是公共 API。结果：Execution 的
`tests/it/`，还没有就新建，不进接缝测试 package。如果只检查 `concat` 自己对 NULL 与空串的值，按选层归 Functions 的
Rust 测试，那里已有同样的用例。A 与 B 同在一个文件却归属不同，所以要按测试函数判定，不按文件判定。

**C：SQL 文本经 SQL 编译、package 编码、局部编译后求值，检查带符号转换的最终结果。**
第 1 问：沿链依次调用 SQL 编译、package 编码、局部编译和 Execution 求值，最下游是 Execution。第 2 问：SQL 与 Plan Codec
不在 Execution 的允许范围内；替换检验：期望值相对 SQL 文本陈述。结果：接缝测试 package，即现在的
`tests/compile-to-execute/tests/it/signed_cast_nullability.rs`。它原先经公开的 `try_new_with_allocator` 传入 Execution
私有的分配宿主，观察 `MemTracker` 的记账；迁入时按三步次序的第 1 步改为传入测试自己的计数分配器，观察的仍是同一个量，
即求值结束后没有未释放的分配。生产分配宿主的记账继续由 Execution 的白盒测试断言，例如
`novarocks/execution/src/exec/expr/compiled_program/` 下的 `scalar_invocation_frame_tests.rs` 与 `input_source_tests.rs`。

**D：Query Application 冻结 SQL 产出的结果声明，并拒绝与声明不符的输出。**
第 1 问：它先调用 SQL 编译，再调用 Query Application 的冻结入口，最下游是 Query Application；对 SQL 声明的检查是前置条件，
整个测试归 Query Application，不拆。第 2 问：Query Application 生产依赖 SQL，SQL 输入取自 `novarocks-sql-testkit`。
第 3 问：用的是 Query Application 的公共 API。结果：Query Application 的 `tests/it/`，即现在的
`novarocks/query-application/tests/it/frozen_output_contract.rs`。

**E：新增，验证某个哈希表的私有探测序列在冲突下的行为。**
第 1 问：owner 是哈希表所在的 crate。第 2 问：在闭包内。第 3 问：探测序列是内部状态，公开入口观察不到。
第 5 问：已有承载冲突行为的主题文件就加进去。结果：该模块测试树里承载冲突行为的文件，并写明观察的内部性质。

**F：新增，Parser 对一条新语法的公开解析结果。**
第 1 问：owner 是 Parser。第 3 问：只用公共 API。第 4 问：不需要。第 5 问：Parser 现有的顶层集成测试文件是旧布局，
新用例不加进去。结果：Parser 的 `tests/it/` 中按该语法族命名的子模块，还没有 `tests/it/` 就新建；
可以顺手把同语法族的旧文件挪进来，不强制。

**G：新增，某个并发结构的 Loom 模型检查。**
第 3 问：经公共 API 就能驱动这个结构。第 4 问：Loom 是 target 级模式。结果：该 crate 的 Loom target 中的一个子模块，
模式门控在 target 根上；还没有 Loom target 就新建，并写明理由。如果模型检查必须触及私有项，第 3 问优先，
它留在 `src/`，放在被测模块测试树里按 Loom 门控的模块。

**H：新增，两个 crate 的测试都要用同一份领域输入。**
第 6 问：到达阶梯上跨 crate 的一级，按[测试支持的暴露方式](#测试支持的暴露方式)判定：事实 owner 的白盒测试也用这份输入，
就放 owner 内的门控 testkit 模块；否则由事实 owner 新建领域 testkit crate。两个消费者各自依赖它，不复制。

**I：新增，Native Adapter 的测试要用 Execution 私有的 project 工厂准备逻辑。**
第 2 问：Native Adapter 生产依赖 Execution。测试支持的暴露方式第 2 步：在 Execution 内放开发 cfg 门控的薄入口，
只转调私有项。现有的例子是 `prepare_project_factory_for_test`。

**J：Plan Codec 的 package 测试输入，被 Plan Codec 自己的白盒测试和其他 crate 的测试共用。**
测试支持的暴露方式第 3 步：放在 Plan Codec 内的门控 testkit 模块，保持单份。现有的例子是
`physical_package_v2::test_support`；新建的同类模块叫 `testkit`。

**K：经 SQL 公共 API 构造的编译输入，供 SQL 与 Query Application 的 `tests/it` 及接缝测试使用；SQL 自己的白盒测试
走另一条私有流程，不使用它。**
测试支持的暴露方式第 3 步不满足，落到第 4 步：独立的 testkit crate，即 `novarocks-sql-testkit`。Execution 目前经过渡期的
转调也在使用它，见[接缝测试 package](#接缝测试-package) 末尾；新的 Execution 测试不这样做。

**L：某个 crate 有几十个没有 target 级理由的顶层集成 target，另有几个都是 Loom 模型的 target。**
第 4 问：普通 target 没有清单内的理由，合成一个 `it`，各文件成为子模块；Loom target 理由相同，合成一个 `loom`，
包装器按模型族检查。按[移动或删除测试时的同步](#移动或删除测试时的同步)核对身份与耦合点。现有的例子是 Control Runtime。

**M：一个测试以子进程方式重新运行自己的测试二进制，只执行一个入口测试。**
子进程的测试路径由 `module_path!()` 推导，入口测试输出标记，父测试先确认标记再判断结果。迁移中遇到这样的存量测试时，
先单独加固，再移动。

**N：一个生产模块经 `#[path]` 从非标准位置加载，现在要给它接测试树。**
先把生产文件移到标准位置（只改路径，内容不变），再用 `#[cfg(test)] mod tests;` 接入。`#[path]` 有真实理由时保留，
测试入口写成 `#[cfg(test)] #[path = "<实现文件名>/tests/mod.rs"] mod tests;`，见[白盒测试的形态](#白盒测试的形态)。

**O：修复有符号整数运算的控制错误传播缺陷时，改到了 `novarocks/functions/src/arithmetic/tests.rs` 里的相关测试。**
这些测试调用公开的 `PreparedArithmeticRecipe::evaluate_row`，留在白盒只因用了 crate 私有的简写 `invalid()`、`internal()`
构造注入的失败。按存量测试的迁移：新增的回归测试与被改到的测试都改用公开的失败类型构造输入，写进 Functions 的
`tests/it/`；旧测试在同一个改动里删除，并写明每条性质由哪个新测试断言。同一模块里没被改到的 `decimal/tests.rs` 与
`float/tests.rs` 保持原样。

**P：从物理计划片段经局部编译器取输入，检查 Execution 写入布局的路由。**
现位于 `tests/compile-to-execute/tests/it/write_layout.rs`。第 1 问：最下游是 Execution。第 2 问：输入来自 Execution 的
输入契约生产者，不从 SQL 文本出发，不穿过“编译到执行”接缝。结果：owner 是 Execution；它要用 Execution 的私有入口，
所以放在 Execution 的白盒测试树里，函数目录输入取自事实 owner Functions。它目前是存量，由改动到它的任务迁走。

**Q：新增一个测试，它的主题目前只在一个平铺的旧文件 `foo_tests.rs` 里有。**
第 5 问：旧布局的文件不再接收新用例。结果：按第 3 问放到规范位置，主题文件按行为轴新建；可以在同一个改动里
把 `foo_tests.rs` 整体挪进测试树，按同步一节核对身份，不强制。
