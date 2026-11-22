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

#[test]
fn fixed_byte_projection_preserves_selected_bytes_and_nulls_after_binding() {
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use arrow::array::{BinaryArray, FixedSizeBinaryArray};

    let runtime = tokio_runtime();
    let context = read_context(&runtime);
    let frozen = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            7,
            "bytes",
            Type::Primitive(PrimitiveType::Fixed(4)),
        ))])
        .build()
        .unwrap();
    let bound = crate::typed_read::schema_binding::annotated_read_schema(&frozen).unwrap();
    let target = super::super::output_carrier(
        bound.field(0),
        super::super::IcebergReadLayout::Offsets,
        &Default::default(),
        &frozen,
    )
    .unwrap();
    assert_eq!(target, DataType::Binary);
    let fixed: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some([88, 88, 88, 88]),
                Some([0, 255, 128, 34]),
                None,
                Some([1, 2, 3, 4]),
            ]
            .into_iter(),
            4,
        )
        .unwrap()
        .slice(1, 3),
    );
    let columns = [OutputColumn::new(&fixed, &target).with_fixed_binary_projection()];
    let mut cursor = crate::typed_read::output::OutputCursor::new(
        &columns,
        OutputRows::SharedSelected(Arc::from([2, 1, 0])),
        &context,
    )
    .unwrap();
    let page = cursor.next_page(&context).unwrap().unwrap();
    let (_, columns) = page.into_columns().unwrap();
    let array = columns[0].as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(array.value(0), &[1, 2, 3, 4]);
    assert!(array.is_null(1));
    assert_eq!(array.value(2), &[0, 255, 128, 34]);
    assert!(cursor.next_page(&context).unwrap().is_none());
}
