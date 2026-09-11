# Memory crate boundary

The memory domain is split across two crates. This page says which one a new
type or fact belongs in, which changes belong to neither, and what the CI guard
actually enforces.

## The two layers

| Crate | Package | Owns |
|---|---|---|
| `novarocks/memory` | `novarocks-memory` | The neutral memory core: capacity, reservation and ownership facts, accounting arithmetic, and the errors those facts produce. No carrier, no runtime. |
| `novarocks/memory-arrow` | `novarocks-memory-arrow` | The Arrow adapter: the single place a memory fact meets an Arrow buffer or allocator. |

The adapter depends on the core. Nothing depends on the adapter except the
crates that actually hold Arrow data.

## Where does my change go?

**A capacity or ownership fact** goes in the core. "How many bytes does this
own", "may this reservation grow", "who released this" — these are statements
about accounting, and the core can state them without knowing what the bytes
are. If a fact cannot be written down without naming a buffer type, it is not a
core fact.

**Anything that touches an Arrow buffer** goes in the adapter. Measuring a
`RecordBatch`, wiring an Arrow allocator, translating a buffer's lifetime into a
core reservation: all adapter work. The core stays ignorant of Arrow so that the
accounting model can be tested and reasoned about without a columnar runtime in
the graph.

**A policy or a queue belongs to neither crate.** Admission order, per-query
budgets, spill thresholds, what to do when a request cannot be satisfied — those
are decisions, not facts, and they belong to the arbitrator alongside the
workload governance owner. The memory crates answer "how much is held and by
whom"; they have no opinion about who should get the next allocation. Putting a
policy in the core is the most common way this boundary is broken, because a
policy looks like it needs the same numbers.

## Why the core has zero dependencies

`novarocks-memory` declares no dependencies at all: no Arrow, no SQL, no query
context, no scheduler, no async runtime. This is a contract, not an accident of
the current code.

The reason is that the core is the one place in the engine where accounting must
be true independently of who is asking. A core that could name a query context
would grow methods that only make sense during a query; a core that could name
an async runtime would grow reservation calls that only make sense inside a
task. Both would make the accounting model impossible to test in isolation, and
both are how a memory core usually turns into a scheduler.

Zero dependencies also makes the rule cheap to check and impossible to erode
gradually. There is no allow-list to argue about and no "just one small utility
crate" conversation: the answer is that the crate has none.

## What CI enforces

`tools/ci/check-memory-dependency-boundary.py` reads the resolved Cargo
dependency graph and checks that:

- `novarocks-memory`'s resolved normal dependency closure is empty, and its
  declared dependency table is empty too. The declared scan is separate on
  purpose: `cargo metadata` resolves default features, so an optional dependency
  behind a non-default feature never appears in the resolve graph;
- `novarocks-memory`'s dev dependency closure contains no `novarocks-*` package,
  no `arrow*` package, and no `tokio`. Today the core has no dev-dependencies;
  the rule guards the future, because a test that needs an Arrow buffer to say
  something about the core is a test that belongs to the adapter;
- no first-party (`novarocks-*`) package other than `novarocks-memory` appears
  in `novarocks-memory-arrow`'s normal closure;
- neither memory crate appears in `novarocks-state-store-api`'s normal closure.
  That contract is byte-oriented and its own guard forbids Arrow, which the
  adapter would drag in;
- no first-party package reaches both `novarocks-memory-arrow` and the retired
  `novarocks-connector-starrocks`, so the retired reference implementation
  cannot come back through a memory consumer.

`novarocks-memory-arrow` is treated as **optional**. The core landed before the
adapter, and when the adapter package is absent from `cargo metadata` its rules
are skipped and the guard still passes — the skip is printed rather than
implied. The same applies to the retired connector, which may simply be deleted
one day.

## What the guard does not enforce

It checks the **resolved dependency graph**, never source text. Per
[ADR-0058](../../adr/ADR-0058-crate-boundaries-enforce-isolation-not-source-shape-guards.md),
a guard protects a real dependency boundary; a scanner that describes the
current file layout goes stale silently when directories move, and then keeps
failing — or keeps passing — for reasons that have nothing to do with the
boundary. If a memory constraint can only be expressed by scanning source, the
correct response is to make it a crate boundary, not to write a scanner.

It does not freeze the adapter's third-party dependency list. The contract is
that the adapter's third-party side is the Arrow family plus whatever that
family pulls in, and that closure is not enumerable by hand — so a neutral
utility crate on the adapter is a review question, not a build failure. What is
enforced there is the adapter's *first-party* reach, which is the part that
decides whether the split is real.

It cannot tell you whether a type belongs in the memory domain at all. An
arbitration policy that only uses core arithmetic passes every dependency check.
That judgment stays with the reviewer, and the questions above are the ones to
ask.

## Running it locally

```bash
python3 tools/ci/check-memory-dependency-boundary.py --manifest-path Cargo.toml
tools/ci/tests/memory-dependency-boundary-test.sh
```

The first command is the gate. The second is its mutation test: it builds
throwaway fixture workspaces under a temporary directory and asserts the checker
rejects each violation and accepts a clean graph, including the graph where the
adapter package does not exist yet. Both run in
`tools/ci/local-full-ci.sh` as the `memory dependency boundary` and
`memory dependency boundary mutations` stages.
