<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# NovaRocks DataSketches substrate benchmark

This root-workspace benchmark package measures the exact registry dependency
`datasketches = "=0.5.0-rc.1"` with the `theta` and `hll` features. It does not
participate in production dependency composition. Its `--manifest-path` commands
resolve through the repository root lock and resolver.

The deterministic workload is defined in `workloads.toml`. The harness parses
that file outside measured closures, pre-constructs input vectors, and records
its SHA-256 digest in the size report. Data generation is therefore excluded
from update timings. Divan's `AllocProfiler` reports allocations for each timed
operation. The manifest fixes 100 samples and records zero seconds of separate
warmup; Divan's own calibration is not relabeled as a user-configured warmup.

## Run

```bash
cargo test --manifest-path tools/datasketches-bench/Cargo.toml --locked
cargo bench --locked \
  --manifest-path tools/datasketches-bench/Cargo.toml \
  --bench substrate
cargo run --release --locked \
  --manifest-path tools/datasketches-bench/Cargo.toml \
  --bin size-profile -- \
  --output logs/datasketches-bench/<timestamp>/size.json
```

Pass a Divan filter after `--` for a focused rerun, for example:

```bash
cargo bench --locked \
  --manifest-path tools/datasketches-bench/Cargo.toml \
  --bench substrate -- hll4_update
```

Run the size profile twice at the same revision and compare
`environment.workload_sha256` plus the ordered `(family, phase, target, lg_k,
input_count)` key set. Machine-specific reports belong under the ignored
`logs/datasketches-bench/` directory, not in source control.

## Measurement boundary

The timing matrix covers Theta unique `u64`/raw bytes, 90% duplicate updates,
exact-to-estimation transition, ordered/unordered compact, compressed and
uncompressed serialization/deserialization, flat/tree union at 0/50/95%
overlap, intersection, and A-not-B. HLL covers HLL4/HLL6/HLL8 update,
serialization/deserialization, sparse-to-dense transition, same/mixed target
union, and downsampling.

`size-profile` is a self-checking retained-memory probe. It compares the public
`estimated_size()` result with the inline object size plus the allocator's
currently live requested bytes while the object is retained. Deterministic
update-count phases cover HLL List, Set, Array4, Array6, Array8, an actually
reached HLL4 AuxMap, and HllUnion transitions. Theta points cover mutable resize,
nominal-k, estimation, compact, flat union, and every tree-union level.

Serialized length is recorded as a separate observation; it is never used to
calculate retained size. Likewise, `lg_k` selects and labels workloads but is
never used as a maximum-size substitute. Allocator requested bytes are more
precise than RSS for this contract, but do not include allocator metadata or
fragmentation. These are microbenchmarks and retained-memory evidence, not an
end-to-end query SLO.
