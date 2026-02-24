# Tests Overview

This directory contains:

- Rust tests (`tests/*.rs` and unit tests under `src/**`)
- Shared Rust test helpers (`tests/common/**`)
- Test data (`tests/data/**`)

## Directory Layout

```text
tests/
├── README.md
├── *.rs                     # Rust integration tests (unit-like / fast path)
└── common/                  # Shared Rust test helpers
```

## Quick Entry

- Rust tests: `cargo test`
- SQL tests guide: `sql-tests/README.md`

## About Rust Target Discovery

Cargo auto-discovers `tests/*.rs`.  
Data-dependent SSB checks are maintained as SQL+result cases under `sql-tests/ssb/sql/`.
