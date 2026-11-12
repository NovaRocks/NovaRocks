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
以及它怎样加进已有的测试而不是另起一个文件。理由、被否决的方案和重评条件在
[ADR-0229](../../adr/ADR-0229-rust-test-placement-dependency-direction-and-fixture-ownership.md)，这里只写操作。

本指南是这些规则的唯一完整表述。`AGENTS.md` 和 [test-governance](test-governance.md) 里只有入口与摘要；
摘要与本指南不一致时以本指南为准。

仓库里现有的测试布局大多早于本指南。相邻文件的写法不是范例，请按下面的判定程序放置新测试。
本指南处于试行期，哪些规则已定、哪些还在等待确认，见[试行期规则](#试行期规则)。

## 适用范围

适用于第一方 workspace 成员的 Rust 单元测试、crate 内的集成测试、领域 testkit 和接缝测试 package。

不适用于 SQL、System、Spark 套件和 `tests/sql/correctness` 语料；它们的规则见 [test-governance](test-governance.md) 和
各自目录的 README。测试资产的准入、尝试材料与证据边界也由 test-governance 规定，本指南不重复。

## 速查

| 你要写的测试 | 放在哪里 |
|---|---|
| 需要私有项或 `cfg(test)` 项（白盒） | 被测模块的 `tests.rs`，主题多时接 `tests/<subject>.rs` |
| 只用 crate 的公共 API（公开契约） | `<crate>/tests/it/` 下承载该领域的子模块 |
| 需要本 crate 的领域 testkit | 只能是 `<crate>/tests/it/` |
| 检查下游怎样消费上游的真实产物，且下游生产依赖上游 | 下游 crate 的 `tests/it/` |
| 从上游的原始输入出发、检查下游最终结果，而两侧之间没有生产依赖 | 对应的接缝测试 package |
| 需要 Loom、Miri、全局 allocator、自定义 harness、`required-features` 或按 binary 选择的资源组 | 独立的 `<crate>/tests/<name>.rs`，顶部写明理由；同时需要私有项的留在 `src/` 内按该模式门控的模块 |
| 启动真实进程、走真实协议或多个角色协作 | 拥有该场景的 Server、cluster harness 或 System owner，沿现有入口 |
| 多个 crate 要用同一份领域输入 | 事实 owner 的领域 testkit，见 [fixture 归属阶梯](#fixture-归属阶梯) |

## 放置判定程序

按顺序回答六个问题。前四问决定位置，后两问决定怎样落笔。`cargo check --tests` 通过只说明编译可见，
不能代替第 1、2 问。

### 第 1 问：断言保护谁的性质

原则是：断言失败时，哪个 crate 承诺的行为没有兑现，测试就归哪个 crate。

**怎样判定。** 以测试函数为单位，看它直接调用并观察的入口。

- 只调用一个 crate 的入口：owner 是那个 crate。
- 沿一条链依次调用多个 crate 的入口：owner 是最下游的那个，即测试最后调用并观察其结果的入口所在的 crate。
  链条中间对上游产物的检查是前置条件，随测试留下，不单独拆走。它能不能留在最下游的 crate 由第 2 问决定。
- 只有当一个测试函数里的几组断言之间没有前后依赖，删掉任何一组都不影响其余各组的结论时，才按各自的入口拆开。

写新测试时先选层。要保护的行为如果在上游 crate 自己的入口就能观察到，就写在上游；
只有当行为要到下游才显现时才写在下游。这就是 test-governance 所说的最低充分层级。

**得到什么。** 一个 owner crate。

**反例。** 测试文件在 Execution 的目录里，只调用了 SQL 编译器，断言的是 SQL 产出的计划有几个参数。
“文件在哪个目录”和“用了哪个 crate 的类型”都不决定 owner；它只调用并观察了 SQL 的入口，所以归 SQL。

### 第 2 问：需要的 crate 是否都在 owner 的生产依赖闭包内

生产依赖闭包是从 owner 出发、沿 `[dependencies]` 能到达的全部 workspace 成员。查看方法：

```bash
cargo tree -p <crate> -e normal --locked --prefix none | grep '^novarocks' | sort -u
```

**怎样判定。** 列出测试必须编入的 workspace 成员。允许的只有三类：闭包内的 crate、闭包内 crate 的领域 testkit、
机械支持（`novarocks-test-support`）。第三方库不在这条规则之内。都在范围内就进入第 3 问。

有越出范围的 crate 时，做一次替换检验。看测试的期望值是相对什么陈述的：

- 期望值相对 owner 自己的输入陈述，例如“这个程序对这几列求值得到这些值”。越界的 crate 只是用来把输入构造出来，
  换一种构造方式期望值不变。改用闭包内的输入，进入第 3 问。
- 期望值相对上游的原始输入陈述，例如“这条 SQL 文本求值得到这些值”。换掉上游的真实产物，期望值就失去了依据。
  它属于对应的接缝测试 package，位置判定到此结束，第 5、6 问在那个 package 内照常适用。

接缝两侧之间如果有生产依赖，上游本来就在下游的闭包内，不会走到这一步。

接缝测试 package 是独立的 crate，只能使用两侧的公共 API。组合测试里需要某一侧私有项的断言，
说的是那一侧自己的性质，把它拆出来，按第 1 问重新判定。

**得到什么。** 确认测试留在 owner，或者进入接缝测试 package。任何情况下都不为测试新增越出闭包的 dev 依赖。

**反例。** 在 Execution 的测试里写“SQL 文本经编译后求值”。Execution 是 BE 的执行引擎，生产闭包里没有 SQL。
把 SQL 加进 Execution 的 `[dev-dependencies]` 能让它编译，但从此任何 SQL 改动都会选中 Execution 的全部测试。
原因见[测试依赖方向与接缝测试 package](#测试依赖方向与接缝测试-package)。

### 第 3 问：是否需要私有项

**怎样判定。** 断言必须触及 owner crate 里的非 `pub` 项，或只在 `cfg(test)` 下存在的观察点，才能写出来吗。
这里看的是生产代码：被测对象、观察点，以及搭建观察环境时必须调用的非 `pub` 生产项，都算。
测试自己的 helper 与 fixture 不算，它们随测试一起移动。

- 需要：白盒测试，留在 `src/`，形态见[白盒测试的形态](#白盒测试的形态)。
- 不需要：公开契约测试，进 `<crate>/tests/it/`。
- 需要本 crate 的领域 testkit：必须进 `<crate>/tests/it/`，原因见[公开契约测试与集成 target](#公开契约测试与集成-target)。

**得到什么。** `src/` 内的测试树，或 `tests/it/`。

**反例。** 为了让测试进 `tests/it/` 把一个内部函数改成 `pub`，或者加一个 feature 把它暴露出来。
位置服从可见性，不是反过来。需要私有项就留在 `src/`。

### 第 4 问：是否需要 target 级属性

**怎样判定。** 下面的清单是封闭的。测试需要其中任何一项，才有理由成为独立的集成 target：

- 自定义 harness（`harness = false`）；
- 全局 allocator；
- `required-features`；
- Loom 或 Miri 这类按 target 运行的模式；
- `.config/nextest.toml` 中按 binary 选择的资源组或超时。

**得到什么。** 需要其中一项、且第 3 问的答案是“不需要私有项”时，建 `<crate>/tests/<name>.rs`，
并在文件顶部用一行英文注释写明是哪一项。不需要时沿用第 3 问的位置。

第 3 问优先。Loom、Miri 这类模式下的测试如果同时需要私有项，留在 `src/`，放进按该模式的 cfg 门控的独立模块，
不为了成为独立 target 而放宽可见性。两种放法的现有例子是 `novarocks/memory/src/lane_loom.rs`（在 `src/` 内）
和 `novarocks/control-runtime/tests/concurrency_loom.rs`（独立 target）。

**反例。** “这些测试会互相干扰全局状态，所以各自一个 binary。”nextest 每个测试一个进程，
同一个 binary 里的测试并不共享进程状态，这不构成理由。

### 第 5 问：是否已有承载它的主题

**怎样判定。** 在 owner crate 里找承载同一个行为轴的测试文件。行为轴是被测对象的一个行为维度，
例如空值语义、溢出、取消；可以把它理解为读者会对这个对象提的一个问题，例如“它怎样处理 NULL”。

- 有，而且它与第 3、4 问得到的位置同属一类（都在 `src/` 内，或都是集成测试，并且独立 target 的理由相同）：
  把用例加进那个文件。这包括旧布局里的文件，例如平铺的 `foo_tests.rs` 和顶层的 `tests/<name>.rs`。
  判断标准是文件里已有的用例是否在回答同一个问题，不是文件名；以层次、契约或工作项命名的宽泛文件不算现成主题。
- 没有：在第 3、4 问得到的规范位置新建文件，按行为轴命名。新建之前要能回答“为什么没有现成主题可容纳”。

**得到什么。** 一个具体的文件。写 plan 时把它记为测试落点，见 [plan 中的测试落点](#plan-中的测试落点)。

**反例。** 为一项工作新建 `foo_phase2_tests.rs`，里面的用例其实都是 `foo` 的空值语义。它们应当加进承载空值语义的文件。

### 第 6 问：输入从哪来

**怎样判定。** 沿 [fixture 归属阶梯](#fixture-归属阶梯)由近到远，取到够用的那一级为止。

**得到什么。** 输入的位置和作者。

**反例。** 另一个 crate 里有一个差不多的构造函数，于是复制一份过来，或者用 `#[path]` 把那个文件接进来。
同一份领域输入只有一个作者；第二个 crate 确实要用时，由事实 owner 提供 testkit。

## 白盒测试的形态

白盒测试默认放在独立文件。生产文件里只留一行入口：

```text
exec/chunk/mod.rs           # production code; declares `#[cfg(test)] mod tests;`
exec/chunk/tests.rs         # white-box assertions, or the entry of the test tree
exec/chunk/tests/
    slicing.rs              # one behaviour axis per file
    nullability.rs
    fixtures.rs             # inputs and the check entry for this tree only
```

`foo.rs` 或 `foo/mod.rs` 里的 `#[cfg(test)] mod tests;` 都解析到 `foo/tests.rs`，它的子模块解析到 `foo/tests/`。
现有的例子是 `novarocks/execution/src/exec/chunk/mod.rs` 与同目录的 `tests.rs`，
以及 `novarocks/execution/src/exec/expr/compiled_program/tests.rs`。

独立文件的理由是构建行为。非测试构建不读取 `tests.rs`，修改它不会让这个 crate 的库和所有下游失效；
修改写在生产文件里的内联测试则会。底层 crate 的下游很多，这个差别直接决定改一个测试要重编多少东西。

规则：

- 内联 `mod tests { ... }` 只用于不需要任何 helper 的少量断言。一旦需要构造函数、fixture 或第二个主题，移到独立文件。
- 测试主题的清单放在测试树里（`tests.rs` 声明它的子模块），不在生产 `mod.rs` 里逐个列出
  `#[cfg(test)] mod xxx_tests;`。
- 不用 `#[path]` 把兄弟文件接成测试模块，不新建 `foo_tests.rs` 这样的平铺兄弟文件。
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

一个 binary 不是终点。公开契约测试多到分开更合适时，可以拆成少数几个按领域命名的 target，
前提是它们共用的输入已经放进 `fixtures` 模块或 testkit，而不是在各个 target 里各留一份。

> **试行**：按领域拆分的具体做法，以及拆分后身份与处置的同步步骤，等待集成 target 的试点确认。

### testkit 的“两份类型”规则

设 U 是事实 owner，`U-testkit` 依赖 U。U 的 lib 单元测试如果使用 `U-testkit`，测试二进制里就有两份 U：
被测的那份，和 testkit 链接的那份。两份里的同名类型互不相同，编译会失败。

| 使用方 | 能否使用 `U-testkit` | 原因 |
|---|---|---|
| 下游 crate 的单元测试与集成测试 | 能 | 只链接一份 U |
| U 的 `tests/it` | 能 | 集成测试链接 U 的 lib，与 testkit 用的是同一份 |
| U 的 lib 单元测试 | 不能 | U 被编译两份 |

由此得到两条规则：

- 需要本 crate 领域 testkit 的测试必须是 `tests/it`。
- lib 单元测试只测单个模块的私有性质，输入在模块内构造。一个单元测试如果需要完整的领域输入，
  例如整条 SQL 编译，说明它断言的是公开契约，应当移到 `tests/it`。

这样同一份领域输入不会出现 testkit 和 crate 内两个作者。

## 测试依赖方向与接缝测试 package

**规则。** 一个 crate 的测试，无论 lib 单元测试还是 `tests/it`，只能依赖三类 workspace 成员：它自己生产依赖闭包内的 crate、
这些 crate 的领域 testkit、机械支持。测试依赖图不比生产依赖图宽。
规则约束的是 workspace 成员之间的依赖；第三方测试库（例如 `loom`）按普通的依赖评审。

**为什么。** 依赖是按 crate 声明的，不是按测试声明的。只要测试二进制里有一个用例需要某个闭包外的 crate，
这条 dev 依赖就对整个 crate 成立，带来三个后果：

- 开发期的测试选择按反向依赖计算，不区分生产依赖和 dev 依赖。闭包外的那个 crate 一改，这个 crate 的全部测试都被选中。
- 这个 crate 的测试二进制要链接闭包外的整个栈，对方一改就要重新编译和链接。
- 失败报告指向这个 crate，而要修的可能是对方。

**何时起作用。** 组合的两侧之间有生产依赖时，规则与“归下游 owner”结论相同。例如 Query Application 生产依赖 SQL，
“SQL 的产物被 Query Application 冻结”这类测试放在 Query Application，不产生越界的依赖。
两侧之间没有生产依赖时，组合测试不属于任何一侧，进入接缝测试 package。

**接缝测试 package。** 它是只含测试的 workspace 成员，按一条真实的产品接缝命名，依赖接缝两侧和它们的 testkit，
不被任何成员依赖。它不是通用的测试 crate：数量由产品接缝的数量决定，每个 package 只放穿过那条接缝的断言。
当前已识别的接缝是“编译到执行”：SQL 文本经 SQL 编译、package 编码和局部编译之后，在 Execution 中求值。

> **试行**：“编译到执行”接缝测试 package 尚未建立，它的名称与位置等待跨 owner 与接缝的试点确认。
> 建立之前怎样放置这类测试，见[试行期规则](#试行期规则)。

**下层自己的输入。** 组合测试迁走之后，下层 crate 自身的语义测试使用闭包内的输入。
Execution 的输入契约是 `LocalProgram`，它在 Execution 的闭包内。

> **试行**：Execution 现有的语义测试大多不直接构造 `LocalProgram`，而是先搭物理计划片段，再经局部编译器得到它；
> 物理计划与局部编译器都不在 Execution 的闭包内。这类测试归到哪里、是否由 `local-program` 提供简洁的构造入口，
> 等待跨 owner 与接缝的试点确认。在那之前见[试行期规则](#试行期规则)。

## fixture 归属阶梯

由近到远，用到够用的那一级为止。

| 级 | 何时用 | 位置 | 约束 |
|---|---|---|---|
| 就地 | 只有一个测试用 | 测试函数内或同一文件内 | 无 |
| 测试树 | 同一棵测试树内共用 | 树内的 `fixtures` 模块 | 只服务这棵树 |
| 同 crate | 同一 crate 内多棵测试树共用 | 它们公共祖先下的 `cfg(test)` 模块 | 不进生产构建 |
| 领域 testkit | 第二个 crate 实际消费同一份领域输入时才创建 | 事实 owner 持有的独立 crate | 只作为 dev 依赖；遵守“两份类型”规则 |
| 机械支持 | 只描述进程、端口、I/O 等机械资源 | `tests/test-support` | 不放领域输入 |

同名的 fixture 不一定该合并。先比较语义和输入责任：同一事实的重复构造收口到一处，语义不同的本地 stub 即使长得相似也各自保留。

testkit 只构造输入。它不实现被测的消费规则，也不给出由被测代码算出来的预期。

始终不采用的做法：

- 复制 helper；
- 用跨 package 的 `#[path]` 共享测试源码或编入另一个 crate 的生产实现；
- 用生产 crate 的 feature 或 `pub` 暴露仅供测试的模块。

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

位置规则决定文件在哪，下面四条决定文件怎样长。它们约束新写的测试；存量测试不要求为此改写。

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

如果测试需要新的 dev 依赖，同时写明它在 owner 的生产依赖闭包内，或者属于允许的 testkit 与机械支持。
评审 plan 的人按本指南的判定程序核对落点。这是规则生效的位置；仓库不用源码扫描检查文件名或位置。

## 命名与词汇

`test_support` 这个词根在仓库里曾经同时指两种相反的责任。本指南把它们分开：

| 责任 | 用词 | 位置 |
|---|---|---|
| 领域输入与本树的 check 入口 | fixtures | 测试树内，或 crate 内的 `cfg(test)` 模块 |
| 跨 crate 的领域输入 | testkit | `novarocks/<domain>/testkit` |
| 进程、端口、I/O 等机械资源 | support | `tests/test-support` |

`tests/` 目录下的文件按行为轴命名，不带 `_test` 或 `_tests` 后缀。这适用于白盒的 `foo/tests/` 和公开契约的 `tests/it/`。
有 target 级理由的独立 target 同样按行为轴命名，可以带表示运行模式的后缀，例如 `_loom`。
命名规则约束文件与模块，不要求重命名测试函数。

> **试行**：crate 内多棵测试树共用的 `cfg(test)` 模块的统一拼写，等待白盒测试树的试点确认。在那之前新建的这类模块使用 `fixtures`。

## 试行期规则

本指南从合入起生效，其中一部分规则还在等待试点确认。

### 哪些已定，哪些待确认

| 规则 | 状态 | 由谁确认 |
|---|---|---|
| 六问判定程序、白盒与公开契约的位置、独立 target 的理由清单 | 已定 | — |
| 测试依赖方向规则、接缝测试 package 的定义 | 已定 | — |
| fixture 归属阶梯、“两份类型”规则、不用 feature 与 `pub` 暴露测试支持 | 已定 | — |
| 形态与增长规则、命名与词汇的分工 | 已定 | — |
| “编译到执行”接缝测试 package 的名称与位置，SQL 领域 testkit 的名称、位置与接口 | 待确认，尚不存在 | 跨 owner 与接缝的试点 |
| 经局部编译器取得输入的下层测试的去向，`LocalProgram` 的简洁构造入口 | 待确认 | 跨 owner 与接缝的试点 |
| 既有平铺文件按行为轴归并的做法，crate 内共用输入模块的拼写 | 待确认 | 白盒测试树的试点 |
| 集成 target 按领域拆分的做法，身份与处置的同步步骤 | 待确认 | 集成 target 的试点 |
| 存量迁移的核对方法 | 不在本指南内 | 定稿时写入 |

### 对新测试的三条约束

1. 不新建平铺的 `foo_tests.rs` 兄弟文件，不在生产 `mod.rs` 里新增 `#[cfg(test)] mod xxx_tests;`。
   新的白盒测试文件进测试树。
2. 不新增“每个文件一个 binary”的集成 target。新的公开契约测试进 `tests/it/`，这个 crate 还没有就新建。
   第 4 问清单内的独立 target 不受此限。
3. 不新增越出生产依赖闭包的 dev 依赖。

### 不要求的事

- 向旧布局里的既有测试文件加用例是允许的，这本身就是累加。旧布局包括 `src/` 下平铺的 `foo_tests.rs` 和顶层的 `tests/<name>.rs`。
- 改到存量测试时不要求顺手迁移。存量由专门的迁移批次归位。
- 本指南合入之前已获批的 plan 不追溯，其中已写明的测试位置照原 plan 执行。

### 规范位置尚不存在时

有三种情形，判定程序给出的结果现在还落不了地：

- 位置是接缝测试 package，而它尚未建立；
- 测试需要的领域输入目前只存在于另一个 crate 的测试 helper 里，对应的领域 testkit 尚未建立。
  当前的例子是“声明字段后的真实 SQL 编译”，它的构造代码在 Execution 的测试目录里；
- 结果取决于上表中待确认的规则，例如经局部编译器取得输入的下层测试。

这三种情形的做法相同，并且优先于上面的三条约束：新用例暂时加进现有同类用例所在的文件，使用那里已有的输入构造。
“同类”指输入来源和断言对象都相同的用例。那个文件所在的 crate 已有的越界 dev 依赖可以继续使用，
但不因此新增越出闭包的 dev 依赖，也不把那里的 helper 复制到别处。
没有现成的同类文件时，在 plan 的测试落点里写明判定结果和暂时的位置。

不属于这三种情形的新测试直接放到判定结果的位置。例如只需要 owner 自己公共 API 就能构造输入的公开契约测试，
现在就进 `tests/it/`。

对应的试点合入后，本节会改为实际位置，之后的新用例直接按判定结果放置。

## 走查用例

下面八个用例演示判定程序。前四个取自现有代码，后四个是假设的新测试。

**A：SQL 编译 `SELECT concat(a,b,c) FROM t` 后检查计划的参数个数与类型顺序。**
现位于 `novarocks/execution/src/exec/expr/f3_concat_trim_sql_boundary_tests.rs`。
第 1 问：它只调用并观察 SQL 编译器，owner 是 SQL。第 2 问：只需要 SQL 闭包内的 crate。第 3 问：检查的是 SQL 编译的公开产物，不需要私有项。
第 4 问：不需要。结果：SQL 的 `tests/it/`。

**B：搭一个只含 `concat` 的物理计划片段，经局部编译器得到 `LocalProgram`，检查它对 NULL 与空串的求值。**
与 A 在同一个文件里。第 1 问：它最后调用并观察的是 Execution 的求值入口，owner 是 Execution。
第 2 问：物理计划与局部编译器不在 Execution 的闭包内。替换检验：期望值是相对“这个程序对这几列求值”陈述的，
换一种方式构造同一个程序期望值不变，所以它们只是输入的构造者。第 3 问：求值入口是公共 API，观察的也是公共结果。
结果：归 Execution。它最终是留在 Execution 并改用闭包内的输入，还是随输入的构造方式另行安置，属于待确认的规则；
在那之前，同类新用例按“规范位置尚不存在时”加进现有文件。
A 与 B 同在一个文件却归属不同，所以要按断言逐个判定，不按文件判定。

**C：SQL 文本经 SQL 编译、package 编码、局部编译后求值，检查最终结果。**
现位于 `novarocks/execution/src/exec/expr/signed_cast_nullability_sql_tests.rs`。
第 1 问：它沿链依次调用 SQL 编译、package 编码、局部编译和 Execution 求值，最下游是 Execution；
中间对计划和 package 的检查是前置条件，随测试留下。第 2 问：SQL 等上游不在 Execution 的闭包内。
替换检验：期望值是相对 SQL 文本陈述的。结果：“编译到执行”接缝测试 package。
其中必须调用 Execution 非 `pub` 生产项的断言（内部内存记账归零）在接缝 package 里写不出来，
拆给 Execution 的白盒测试。

**D：Query Application 冻结 SQL 产出的结果声明，并拒绝与声明不符的输出。**
现位于 `novarocks/execution/src/exec/expr/numeric_unary_public_transport_tests.rs`。
第 1 问：它先调用 SQL，再调用并观察 Query Application 的冻结入口，最下游是 Query Application；
对 SQL 声明的检查是前置条件，随测试留下。第 2 问：Query Application 生产依赖 SQL，在闭包内。
第 3 问：用的是 Query Application 的公共 API。结果：Query Application 的 `tests/it/`，SQL 的真实产物作输入。
它需要的 SQL 输入构造目前只在 Execution 的测试 helper 里，所以现在新增的同类用例按“规范位置尚不存在时”加进现有文件。

**E：新增，验证某个哈希表的私有探测序列在冲突下的行为。**
第 1 问：owner 是哈希表所在的 crate。第 2 问：在闭包内。第 3 问：需要私有项。第 5 问：已有承载冲突行为的主题文件就加进去。
结果：该模块的 `tests.rs` 或 `tests/` 下承载冲突行为的文件。

**F：新增，Parser 对一条新语法的公开解析结果。**
第 1 问：owner 是 Parser。第 3 问：只用公共 API。第 4 问：不需要。第 5 问：Parser 现有的顶层集成测试文件里
如果有承载这一语法族的，加进那个文件；没有时，结果是 Parser 的 `tests/it/` 中按该语法族命名的子模块，
还没有 `tests/it/` 就新建，不新增顶层的 `tests/<name>.rs`。

**G：新增，某个并发结构的 Loom 模型检查。**
第 3 问：经公共 API 就能驱动这个结构。第 4 问：Loom 是 target 级模式。结果：独立的集成 target，文件顶部写明理由。
如果模型检查必须触及私有项，第 3 问优先，它留在 `src/` 内按 Loom 门控的模块里。

**H：新增，两个 crate 的测试都要用同一份领域输入。**
第 6 问：已到达阶梯的 testkit 一级。结果：由拥有这份事实的 crate 新建领域 testkit，两个消费者各自 dev 依赖它，不复制。

## nextest 身份提示

nextest 用 binary ID 标识测试：lib 单元测试是 `<package>`，集成 target 是 `<package>::<target>`。
一个测试从 `src/` 移到 `tests/it/`，binary ID 会从 `<package>` 变成 `<package>::it`；移到接缝测试 package 则换成那个 package 的 ID。

下面两处按 binary 或测试身份匹配，测试换位置时在同一个改动里同步：

- `.config/nextest.toml` 中按 `binary(...)` 选择的资源组与超时；
- `tools/ci/baselines/dispositions.json` 中以 `rust-test:` 开头的单元键。

新建测试通常不涉及这两处。只有当新测试需要某个资源组时，才按第 4 问把它放进对应的独立 target。
