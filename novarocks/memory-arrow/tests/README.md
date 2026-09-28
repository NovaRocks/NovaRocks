# Governed retention checks

The adapter has no production address index. Tests under this directory use
`BackingCollector` on live Arrow payloads as an independent, quiescent-point
oracle; the census does not read candidate Entry identities.

| Contract | Test file |
|---|---|
| Standard provenance, allocation base, full capacity, grouped dictionary and IPC-style body sharing, nested/null/view traversal | `backing_identity.rs` |
| Grouped import, fork, derive, leaf lifetime and final settlement | `retained_lifecycle.rs` |
| Capacity refusal leaves sources intact; bare reimport is conservatively charged; unknown provenance and foreign authority fail | `retained_admission.rs` |
| Concurrent last references and unwind | `retained_concurrency.rs` |
| Independent unique-backing census, duplicate-import delta, long stream and low account-slot reuse | `retention_census.rs` |
| Explicit service borrowing, sponsor ownership, unsupported publication and Pin generation | `shared_handover.rs` |

Run local correctness with `cargo test -p novarocks-memory-arrow`. The
`retained_cost` benchmark is a separate Linux acceptance input; local checks
or macOS results do not satisfy its performance gates. The raw Arrow payload
accessor is a borrow for operators: cloning it without carrying the lineage
creates an unsupported bare alias, which the adapter cannot discover later.
