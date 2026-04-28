// Spike test: verify iceberg-rust 0.9 public API for writing ManifestEntry with status=DELETED.
//
// Spec §7.1 — OverwriteCommit needs to mark base-snapshot data files as DELETED.
// This test confirms which public path works before Task 10 implementation.
//
// Findings:
//   Path A  — ManifestWriter::add_delete_file (public) — WORKS
//   Path A' — ManifestEntry struct-init + ManifestWriter::add_delete_entry (pub(crate)) — NOT PUBLIC
//
// Conclusion: use Path A (add_delete_file) in production OverwriteCommit code.

use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestEntry, ManifestStatus,
    ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema, Struct, Type,
};
use tempfile::TempDir;

/// Helper: build a minimal unpartitioned data file.
fn make_data_file(path: &str) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .record_count(100u64)
        .file_size_in_bytes(4096u64)
        .partition_spec_id(0i32)
        .build()
        .expect("DataFile build must succeed")
}

/// Helper: build a minimal schema with one Long column.
fn make_schema() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("Schema build must succeed"),
    )
}

/// Path A: ManifestWriter::add_delete_file is a public method.
///
/// This is the production path for OverwriteCommit: for each base-snapshot
/// data file, call add_delete_file(data_file, sequence_number, file_sequence_number).
///
/// Note: add_entry_inner enforces that Deleted/Existing entries must have both
/// sequence_number and file_sequence_number set (non-None), so we must supply them.
#[test]
fn path_a_add_delete_file_is_public() {
    let schema = make_schema();
    let partition_spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .build()
        .expect("PartitionSpec build must succeed");

    let tmp_dir = TempDir::new().expect("create tempdir");
    let manifest_path = tmp_dir.path().join("deleted_manifest.avro");
    let io = FileIO::new_with_fs();
    let output_file = io
        .new_output(manifest_path.to_str().unwrap())
        .expect("new_output must succeed");

    let mut writer =
        ManifestWriterBuilder::new(output_file, Some(42), None, schema, partition_spec)
            .build_v2_data();

    let data_file = make_data_file("s3://bucket/table/data/base-file-001.parquet");

    // Path A: add_delete_file(data_file, sequence_number, file_sequence_number)
    // sequence_number and file_sequence_number are the original values from the base snapshot.
    writer
        .add_delete_file(data_file, 1, Some(1))
        .expect("add_delete_file must succeed");

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let manifest_file = rt
        .block_on(writer.write_manifest_file())
        .expect("write_manifest_file must succeed");

    assert_eq!(manifest_file.deleted_files_count, Some(1));
    assert_eq!(manifest_file.deleted_rows_count, Some(100));
    assert_eq!(manifest_file.added_files_count, Some(0));

    println!("Path A OK: deleted_files={:?}", manifest_file.deleted_files_count);
    println!("  manifest written to: {}", manifest_file.manifest_path);
}

/// Path A': ManifestEntry struct-init with status=Deleted, then add_entry.
///
/// ManifestEntry fields are public in iceberg-rust 0.9, so direct struct-init
/// works. However, ManifestWriter::add_entry forces status to Added regardless
/// of the entry's original status — it is pub(crate) only, so we cannot call
/// add_delete_entry either. This path documents that struct-init of ManifestEntry
/// is possible, but the only public writer method that produces DELETED entries
/// is add_delete_file.
#[test]
fn path_a_prime_manifest_entry_struct_init_is_possible() {
    // ManifestEntry fields are pub — direct struct-init compiles.
    let data_file = make_data_file("s3://bucket/table/data/base-file-002.parquet");
    let entry = ManifestEntry {
        status: ManifestStatus::Deleted,
        snapshot_id: Some(1),
        sequence_number: Some(1),
        file_sequence_number: Some(1),
        data_file,
    };
    // Verify the entry has the expected status.
    assert_eq!(entry.status, ManifestStatus::Deleted);
    assert!(!entry.is_alive());

    // NOTE: we cannot pass this entry to a ManifestWriter publicly.
    // ManifestWriter::add_entry is pub(crate) and forces status=Added.
    // ManifestWriter::add_delete_entry is pub(crate) and marked #[allow(dead_code)].
    // The only public path is add_delete_file, confirmed in path_a_add_delete_file_is_public.
    println!("Path A': ManifestEntry struct-init works; add_delete_file is the public writer path");
}
