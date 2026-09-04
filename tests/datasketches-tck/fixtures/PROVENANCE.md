# Fixture provenance

`manifest.tsv` is versioned independently from the sketch formats. Every row records the producer,
version, exact source commit, profile, seed, `lg_k`, deterministic input, serialization form,
payload SHA-256, and decoded observable facts. `hashes_sha256` is SHA-256 over sorted retained
Theta hashes encoded as little-endian `u64`; it keeps the full retained set auditable without a
second large text copy.

## Java fixtures

- Producer: `org.apache.datasketches:datasketches-java:6.2.0`
- Release commit: `9ca65f12b7bdde9b424f27be1d16f2f9dc365a7a`
- Jar SHA-256: `1b55103e1f7564150a0867eca4ce3bca13cd5935a32c199a5e738f8c5c24901a`
- Memory dependency: `datasketches-memory:3.0.2`, SHA-256
  `a3dbdec4de16bf2b0a4c9b1b253bd4064d587675fc76063f8972cdfa104c66cb`
- Iceberg value-conversion oracle: `org.apache.iceberg:iceberg-api:1.10.0`, jar SHA-256
  `627061d401dba9d1a8cada2da6394640c2e803102bb676fb34e5f65503cf2c51`
- Generators: `generate/java/src/GenerateFixtures.java` and
  `generate/java/src/GenerateIcebergThetaVectors.java`; `generate/java/pom.xml` pins the released
  artifacts and `generate/java/generate.sh` verifies all jar digests before execution.

The Java corpus deliberately labels QuickSelect and Alpha separately. Rust only checks that the
Alpha compact is readable and participates in standard set operations; no row claims Alpha
producer equivalence or provenance encoded in the wire image.

`theta/iceberg_java62_single_value_vectors.tsv` records Iceberg's canonical primitive bytes and
the resulting default-seed, lg_k=12, ordered QuickSelect compact image. It includes decimal,
floating-point payload, time, UTF-8, binary, fixed, and UUID cases used by the lightweight Iceberg
function bundle. The TSV is oracle evidence and is intentionally not a sketch-manifest row.

## C++ fixtures

- Producer source: `apache/datasketches-cpp@fe0261aa043c1d3af9a92a62fa286caabbf6fa84`
- Corpus source: `apache/datasketches-tck@c0a180708c6e6433e4cba7fba091713eb8af3eaa`
- TCK pin evidence: that revision's `config.toml` binds `snapshot.cpp` to the producer commit above.
- Generator/audit: `generate/cpp/generate.sh` fetches the exact TCK revision, verifies its C++ pin,
  and copies only the allowlisted snapshot names.

These are upstream TCK bytes, not NovaRocks or Rust self-round-trips.

## Rust fixtures

- Producer: crates.io `datasketches 0.5.0-rc.1`
- Published source metadata commit: `77f5652016b3859c23b60c5b8b9e94578ef484f0`
- Crate checksum: `407f3fe0c32e6547cb8637b11a8a765ff027afa31e5f6f732b23f8d74672087b`
- Generator: `src/bin/generate_rust_fixtures.rs`

Rust fixtures are explicitly labeled `rust`; they provide deterministic canonical byte checks and
are never represented as external Java or C++ evidence.
