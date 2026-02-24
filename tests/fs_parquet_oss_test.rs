// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//! Integration tests for parquet probing via the local filesystem operator.

use anyhow::{Context, Result};
use opendal::Operator;
use parquet::basic::Compression;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::fs::File;
use std::sync::Arc;

#[tokio::test]
async fn fs_operator_can_read_and_parse_parquet() -> Result<()> {
    let dir = tempfile::tempdir().context("tempdir")?;
    let path = dir.path().join("test.parquet");

    let schema = Arc::new(parse_message_type(
        r#"
            message schema {
              REQUIRED INT32 id;
              REQUIRED BINARY name (UTF8);
            }
        "#,
    )?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = File::create(&path).context("create parquet file")?;
    let mut writer =
        SerializedFileWriter::new(file, schema, props).context("create parquet writer")?;

    {
        let mut rg = writer.next_row_group().context("next row group")?;
        while let Some(mut col) = rg.next_column().context("next column")? {
            match col.untyped() {
                ColumnWriter::Int32ColumnWriter(w) => {
                    w.write_batch(&[1, 2, 3], None, None)
                        .context("write int32 batch")?;
                }
                ColumnWriter::ByteArrayColumnWriter(w) => {
                    let values = vec![
                        ByteArray::from("a"),
                        ByteArray::from("b"),
                        ByteArray::from("c"),
                    ];
                    w.write_batch(&values, None, None)
                        .context("write byte array batch")?;
                }
                _ => anyhow::bail!("unexpected column writer type"),
            }
            col.close().context("close column")?;
        }
        rg.close().context("close row group")?;
    }

    writer.close().context("close parquet writer")?;

    let fs = opendal::services::Fs::default().root(dir.path().to_string_lossy().as_ref());
    let op = Operator::new(fs)?.finish();

    let probe = novarocks::fs::opendal::probe_parquet(&op, "test.parquet").await?;
    assert_eq!(probe.num_rows, 3);
    assert_eq!(probe.num_row_groups, 1);
    Ok(())
}
