use std::process::Command;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use tempfile::NamedTempFile;

fn write_parquet_file() -> NamedTempFile {
    let file = NamedTempFile::new().expect("create temp file");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ],
    )
    .expect("build record batch");
    let writer_file = std::fs::File::create(file.path()).expect("open parquet output");
    let mut writer =
        ArrowWriter::try_new(writer_file, schema, None).expect("create parquet writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close parquet writer");
    file
}

#[test]
fn standalone_cli_select_all_from_registered_parquet_table() {
    let parquet = write_parquet_file();
    let output = Command::new(env!("CARGO_BIN_EXE_novarocks"))
        .arg("standalone")
        .arg("--table")
        .arg("tbl")
        .arg("--path")
        .arg(parquet.path())
        .arg("--sql")
        .arg("select * from tbl")
        .output()
        .expect("run standalone cli");

    assert!(
        output.status.success(),
        "standalone cli failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("| id | name |"), "stdout={stdout}");
    assert!(stdout.contains("| 1  | a    |"), "stdout={stdout}");
    assert!(stdout.contains("rows: 3"), "stdout={stdout}");
}

#[test]
fn standalone_cli_projects_and_filters_rows() {
    let parquet = write_parquet_file();
    let output = Command::new(env!("CARGO_BIN_EXE_novarocks"))
        .arg("standalone")
        .arg("--table")
        .arg("tbl")
        .arg("--path")
        .arg(parquet.path())
        .arg("--sql")
        .arg("select name from tbl where id = 2")
        .output()
        .expect("run standalone cli");

    assert!(
        output.status.success(),
        "standalone cli failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("| name |"), "stdout={stdout}");
    assert!(stdout.contains("| b    |"), "stdout={stdout}");
    assert!(stdout.contains("rows: 1"), "stdout={stdout}");
}
