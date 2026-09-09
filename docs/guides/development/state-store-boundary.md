# StateStore crate boundary

The StateStore provider domain is split across three kinds of crate. This page
says which one a new type, trait, or test helper belongs in, and what the CI
guard actually enforces.

## The three layers

| Crate | Package | Owns |
|---|---|---|
| `novarocks/state-store/api` | `novarocks-state-store-api` | The neutral storage contract: transactions, ranges and pagination tokens, versions, commit outcomes, typed errors, limits, and the provider descriptor/factory/instance/lifecycle contract. |
| `novarocks/state-store/testkit` | `novarocks-state-store-testkit` | Shared test mechanics: the in-memory reference store and the behaviour suite every provider is expected to satisfy. |
| `novarocks/state-store/{sqlite,mysql,foundationdb}` | one package each | Physical storage: connections, workers, private schema and encoding, driver errors, internal metrics. |

Consumers (the frontend application runtime, and the server as composition
root) depend on the API crate only. SQLite is the only production provider;
MySQL and FoundationDB are experimental leaf crates that exist to keep the
contract honest against more than one storage model.

## Where does my change go?

**A value type, trait, or validation rule that both a provider and a consumer
must agree on** goes in the API crate. It is a public contract: adding to it
obliges every provider.

**A helper only one provider needs** stays private to that provider, even when
a second provider would find it convenient. Two providers wanting the same
private helper is a signal to look at whether the contract is missing
something, not a reason to add a crate.

**A fake, a fixture, or a behaviour assertion shared across providers** goes in
the testkit. Never in the API crate, and never behind a feature flag on the API
crate.

**Anything about how the frontend uses storage** — retry policy, operation
budgets, record formats, key prefixes — is not part of this domain at all. It
belongs to the application that owns those records. The StateStore contract
knows about keys, values, and transactions; it has no opinion about what a
frontend state family means.

## Why testkit is a crate and not a feature

Cargo features are additive across a workspace: once any crate turns on a
feature of a shared dependency, every other crate resolving that dependency
gets it too. A `state-store-conformance` feature on the contract crate could
therefore pull an in-memory fake into a production dependency graph without
anyone writing a line of code to ask for it.

A separate crate consumed as a `dev-dependency` does not have that failure
mode: Cargo tracks dev edges separately and never propagates them into a
normal dependency closure. The boundary is enforced by the build graph rather
than by review discipline.

## What CI enforces

`tools/ci/check-state-store-dependency-boundary.py` reads the resolved
dependency graph and checks that:

- the API crate and every provider have a normal dependency closure free of
  columnar runtimes (Arrow, Parquet), the Connector contract, and any
  application or execution crate;
- the same capability check runs against the *declared* dependency table, not
  only the resolved graph. `cargo metadata` resolves default features, so an
  optional dependency behind a non-default feature is invisible to the resolve
  graph; declaring one is still a break;
- no `novarocks-*` crate other than the contract itself and a short neutral
  allow-list appears in a production closure, even when no named capability
  covers it;
- no package anywhere in the workspace reaches the testkit through a normal
  dependency edge;
- the API crate does not depend on the testkit in any dependency kind.

It reports normal, dev, and build closures separately, so a test-only
dependency on Tokio reads as what it is rather than as production coupling.

The guard deliberately checks **direction and capability**, not an exact list
of allowed dependency names. A frozen list breaks on every harmless addition
and teaches people to edit the guard instead of thinking about the boundary.
Adding a neutral utility crate to the API is a review question; adding Arrow to
it is a build failure.

## Attempts, and what an outcome is worth

A write is authorised by a `WriteAttempt` the open store instance issues. A
consumer cannot mint one, and an attempt from a previous instance is refused
rather than answered, so "ask the store about an id I made up" is no longer a
question the contract can be asked.

Three answers exist and they are not interchangeable:

- **Committed** and **NotCommitted** are terminal and never flip.
- **Unresolved** means nothing was proven. It is not a denial, and it grants no
  right to run the work again.

`NotCommitted` is a proof obligation. A provider may return it only once the
attempt can no longer commit, its worker or connection has finished, and the
evidence the decision rests on was readable at that moment. A failed read,
released evidence, or work still in flight all produce `Unresolved`. The
temptation to treat absent evidence as absence of a commit is the single most
dangerous shortcut available here, and it is the thing the shared suite checks
hardest.

## Who cleans up, and when

A provider releases the evidence for an outcome **it witnessed itself**,
immediately after publishing the terminal — publish first, release second, so a
later reader reads a recorded verdict rather than re-deriving one from evidence
on its way out. When an outcome is instead recovered through adjudication, the
supervisor publishes it and therefore releases the evidence.

That leaves one case with nobody in it: an attempt whose commit was dispatched
and whose handles were all dropped before anything settled. Those queue as
abandoned, keep their capacity slot, and are returned by a host driving
`drain_abandoned_attempts` — see `state_store::sweeper`. The supervisor spawns
nothing, so an unwired host means the mechanism never runs.

A provider that is not ready to release yet says so without it counting as a
fault, so an ordinary sweep on a busy instance is quiet.

## What the guard cannot tell you

It cannot tell you whether a type belongs in the contract at all. A contract
that grows types no consumer needs still passes every dependency check. That
judgment stays with the reviewer, and the questions above are the ones to ask.
