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

use super::*;
use novarocks_value_arrays::measure::{OutputColumn, OutputRows};

#[test]
fn all_output_columns_share_one_complete_prefix_and_a_wide_row_can_progress() {
    use novarocks_spi::connector::ENGINE_OUTPUT_POLICY;
    let runtime = tokio_runtime();
    let context = read_context(&runtime);
    let target = ENGINE_OUTPUT_POLICY.soft_target_new_column_bytes as usize;
    let wide = "x".repeat(target + 1);
    let text: ArrayRef = Arc::new(arrow::array::StringViewArray::from(vec![
        Some(wide.as_str()),
        None,
        Some("last"),
    ]));
    let path: ArrayRef = Arc::new(StringArray::from(vec!["partition/file.parquet"]));
    let sign: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![-1]));
    let columns = [
        OutputColumn::new(&text, &DataType::Utf8),
        OutputColumn::constant(&path, &DataType::Utf8),
        OutputColumn::constant(&sign, &DataType::Int32),
    ];
    let mut cursor = crate::typed_read::output::OutputCursor::new(
        &columns,
        OutputRows::Contiguous(0..3),
        &context,
    )
    .unwrap();
    let first = cursor.next_page(&context).unwrap().unwrap();
    assert_eq!(first.position_count(), 1);
    let (_, columns) = first.into_columns().unwrap();
    assert_eq!(
        columns[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len(),
        target + 1
    );
    assert_eq!(
        columns[1]
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "partition/file.parquet"
    );
    assert_eq!(
        columns[2]
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .value(0),
        -1
    );
    let second = cursor.next_page(&context).unwrap().unwrap();
    assert_eq!(second.position_count(), 2);
    let (_, columns) = second.into_columns().unwrap();
    let text = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
    assert!(text.is_null(0));
    assert_eq!(text.value(1), "last");
    assert!(cursor.next_page(&context).unwrap().is_none());
}
