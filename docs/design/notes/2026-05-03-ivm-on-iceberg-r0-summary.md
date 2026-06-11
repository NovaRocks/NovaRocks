# R0 - IVM on Iceberg Baseline Summary

**Branch:** `codex/iceberg-ivm-object-delete-projection`
**Baseline commit:** `82c80b8 Support Iceberg IVM delete projection on object stores`

## Supported

- Aggregate MV full refresh over managed-lake storage.
- Aggregate MV incremental refresh for Iceberg append snapshots.
- Aggregate MV DELETE retract for Iceberg v2 Parquet position deletes.
- Aggregate MV DELETE retract for Iceberg v3 Puffin deletion vectors.
- Aggregate MV DELETE retract for Iceberg equality deletes.
- Projection/filter MV DELETE apply for Iceberg position deletes, equality
  deletes, and v3 Puffin deletion vectors when the MV has PRIMARY KEY row
  identity.
- Projection/filter MV hidden primary-key projection for delete apply when
  PRIMARY KEY columns are not part of the user-visible SELECT output.
- Object-store delete reverse projection for S3/S3A-style paths through configured catalog credentials.
- Empty change stream metadata advance without writing data chunks.
- `_row_id` and `_last_updated_sequence_number` reads on Iceberg v3 row-lineage base tables.

## Explicitly Unsupported

- `INSERT OVERWRITE` incremental bridging.
- Projection/filter MV DELETE apply without PRIMARY KEY row identity.
- Schema evolution where the MV uses the changed column.
- Partition evolution policy.
- Concurrent refresh on the same MV.

## Verification

- `cargo test --lib`
- `cargo fmt --check`
- `git diff --check`
