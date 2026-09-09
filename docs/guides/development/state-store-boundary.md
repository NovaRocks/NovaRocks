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

## What the guard cannot tell you

It cannot tell you whether a type belongs in the contract at all. A contract
that grows types no consumer needs still passes every dependency check. That
judgment stays with the reviewer, and the questions above are the ones to ask.
