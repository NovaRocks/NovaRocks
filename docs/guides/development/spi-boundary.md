# Connector SPI dependency boundary

`novarocks-spi` owns Connector contracts. Providers and application hosts consume
those contracts; the contracts must not acquire their runtime, storage, or
execution capabilities.

The finite neutral internal allow-list contains:

| Package | Shared responsibility |
|---|---|
| `novarocks-connector-contract` | Read/write vocabulary and pure provider preparation contracts. |
| `novarocks-secret` | Secret value handling without provider or application ownership. |
| `novarocks-type-contract` | Semantic types, deterministic type rules, source geometry and borrowed control. |
| `novarocks-result-contract` | Result vocabulary, pure record validation/encoding and caller-buffer operations. |

These are allowed roles, not a required dependency set. Arrow vocabulary is
allowed because Connector contracts are columnar. Neutral third-party utilities
are not frozen into an exact dependency array.

## What the guard checks

`tools/ci/check-spi-dependency-boundary.py` reads Cargo metadata and checks both:

- SPI's declared normal dependencies, including disabled optional dependencies;
- the resolved default-feature closure reached from SPI over normal edges.

Both reject async runtimes, StateStore contracts, provider implementations and
application/execution owners. Unreviewed `novarocks-*` packages are also refused,
including Memory. Admitting a neutral package does not exempt its resolved
normal dependencies from these rules. Dev/build edges are outside the normal
production closure; SPI's test-only Tokio dependency does not grant a production
runtime.

The guard does not inspect every transitive package's disabled optional
declarations or every non-default feature combination. It also cannot establish
that every API belongs in a neutral role: that remains a source review question.
Ordinary container construction, borrowed control and pure record codecs do not
grant scheduler, socket, application or Memory account ownership. A build-host
toolchain check is distinct from product runtime capability.

Run the checker and its path-only mutation tests with:

```bash
python3 tools/ci/check-spi-dependency-boundary.py --manifest-path Cargo.toml
bash tools/ci/tests/spi-dependency-boundary-test.sh
```

Dependency direction follows [ADR-0058](../../adr/ADR-0058-crate-boundaries-enforce-isolation-not-source-shape-guards.md).
The separate storage domain follows [ADR-0140](../../adr/ADR-0140-state-store-contract-and-testkit-crates.md)
and has its own [StateStore boundary](state-store-boundary.md).
