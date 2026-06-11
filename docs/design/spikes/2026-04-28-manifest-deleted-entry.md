# Spike — ManifestWriter status=DELETED entry path

**Date:** 2026-04-28
**Spec ref:** §7.1
**Outcome:** Path A succeeded — `ManifestWriter::add_delete_file` is public and works

## Summary

iceberg-rust 0.9 exposes a fully public method `ManifestWriter::add_delete_file` that
creates a `ManifestEntry { status: ManifestStatus::Deleted }` and writes it to an avro
manifest file. No fork or workaround is needed.

Key API facts confirmed:

| API | Visibility | Notes |
|-----|-----------|-------|
| `ManifestWriterBuilder::new(...)` | `pub` | builds the writer |
| `ManifestWriterBuilder::build_v2_data()` | `pub` | produces `ManifestWriter` |
| `ManifestWriter::add_delete_file(data_file, seq, file_seq)` | `pub` | **the production path** |
| `ManifestWriter::add_delete_entry(entry)` | `pub(crate)` | not accessible from NovaRocks |
| `ManifestWriter::add_entry(entry)` | `pub(crate)` | not accessible; also forces status=Added |
| `ManifestEntry` struct fields (`status`, `snapshot_id`, …) | `pub` | struct-init compiles |
| `DataFileBuilder::default().content(...).build()` | `pub` | standard construction pattern |

`add_entry_inner` (private) validates that `Deleted` entries must have both
`sequence_number` and `file_sequence_number` set (non-None) — callers must
supply the original base-snapshot values.

## Code path that works

```rust
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, ManifestWriterBuilder, Struct};

// 1. Build the DataFile from the base snapshot's entry.
let data_file = DataFileBuilder::default()
    .content(DataContentType::Data)
    .file_path(base_entry.data_file().file_path().to_string())
    .file_format(base_entry.data_file().file_format())
    .partition(base_entry.data_file().partition().clone())
    .record_count(base_entry.data_file().record_count())
    .file_size_in_bytes(base_entry.data_file().file_size_in_bytes())
    .partition_spec_id(base_entry.data_file().partition_spec_id())
    .build()?;

// 2. Write a DELETED manifest entry preserving the original sequence numbers.
writer.add_delete_file(
    data_file,
    base_entry.sequence_number().expect("base entry must have seq_num"),
    base_entry.data_file().first_row_id().map(|_| {
        // file_sequence_number from base snapshot, stored in the manifest entry
        base_entry.sequence_number().unwrap()
    }),
)?;
```

Simpler production form for OverwriteCommit (field-preserving):

```rust
/// Write a DELETED manifest entry for a base-snapshot data file.
///
/// `base_seq` and `base_file_seq` must be the original sequence numbers
/// from the base snapshot manifest, not UNASSIGNED_SEQUENCE_NUMBER.
pub(crate) fn write_deleted_entry(
    writer: &mut ManifestWriter,
    data_file: DataFile,
    base_seq: i64,
    base_file_seq: Option<i64>,
) -> iceberg::Result<()> {
    writer.add_delete_file(data_file, base_seq, base_file_seq)
}
```

## Implications for Task 10 (OverwriteCommit)

OverwriteCommit must iterate over all `ManifestEntry` records from the base snapshot
that are `ManifestStatus::Added` or `ManifestStatus::Existing` (i.e., alive), reconstruct
each as a `DataFile` (or clone directly from the entry), and pass them to
`writer.add_delete_file(data_file, seq_num, file_seq_num)`.

The recommended helper signature:

```rust
pub(crate) fn write_deleted_entry(
    writer: &mut ManifestWriter,
    data_file: DataFile,
    base_seq: i64,
    base_file_seq: Option<i64>,
) -> iceberg::Result<()>
```

This is thin wrapper around the public API — no forking required.

The new manifest produced by OverwriteCommit for the overwrite snapshot will
contain only DELETED entries (one per base-snapshot data file). The new data
files will go into a separate ADDED manifest, as per Iceberg spec §7.1.

## Tested

- `tests/scratch_manifest_deleted.rs` — both tests pass with `cargo test --test scratch_manifest_deleted`
- Tested with iceberg-rust 0.9.0, format version V2 data manifest
