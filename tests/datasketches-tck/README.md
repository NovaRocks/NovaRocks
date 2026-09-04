# NovaRocks DataSketches interoperability TCK

This crate tests the wire and set-operation contract consumed by NovaRocks against checked-in
fixtures from Apache DataSketches Java 6.2.0, Apache DataSketches C++, and the exact Rust
`datasketches 0.5.0-rc.1` package. Normal `cargo test` is offline-capable after Cargo dependencies
have been fetched: it does not start Java or C++, run a generator, or access the network.

The Java generator also pins Apache Iceberg 1.10.0 and emits the primitive-to-Theta oracle consumed
by `novarocks-connector-iceberg-functions`.

## Daily test

```shell
cargo test -p novarocks-datasketches-tck --locked
```

The tests verify every fixture SHA-256 against `fixtures/manifest.tsv`, decode all recorded facts,
exercise mutable/compact Theta union, intersection and A-not-B combinations, test Java Alpha only
as an opaque compact input, exercise HLL4/HLL6/HLL8 union and downsampling, and reject the bounded
malformed corpus without panicking.

Trailing backing-buffer capacity and reserved flag bits are intentionally accepted. Java 6.2.0,
the pinned C++ producer, and Rust RC1 share this tolerant behavior; these bytes are not malformed
unless a defined field contradicts the payload.

## Manual fixture refresh

Never point a generator at `fixtures/` during routine validation. Generate into temporary
directories, compare bytes and decoded facts, and review every manifest change:

```shell
tests/datasketches-tck/generate/verify.sh

java_out=$(mktemp -d)
tests/datasketches-tck/generate/java/generate.sh "$java_out"

cpp_out=$(mktemp -d)
tests/datasketches-tck/generate/cpp/generate.sh "$cpp_out"

rust_out=$(mktemp -d)
cargo run -p novarocks-datasketches-tck --bin generate-rust-fixtures -- "$rust_out"

cargo run -p novarocks-datasketches-tck --bin fixture-facts -- \
  tests/datasketches-tck/fixtures > /tmp/datasketches-fixture-facts.tsv
```

The Java and C++ outputs are subsets of the checked-in family directories, so compare the selected
filenames listed by the generator rather than treating unrelated producer files as deletions.
`fixture-facts` is an audit helper, not an expected-output recorder: a refresh must explain changes
to producer bytes and decoded facts before `manifest.tsv` is edited.

See `fixtures/PROVENANCE.md` for exact source pins and digest evidence.
