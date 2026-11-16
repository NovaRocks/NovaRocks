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
use crate::iceberg::arrow::RecordBatchPartitionSplitter;
use crate::iceberg::spec::{NestedField, PrimitiveType, StructType, Type};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{Field, Int32Type};

struct Input {
    kind: PrimitiveType,
    array: ArrayRef,
    transforms: &'static [Transform],
}

fn input<A: arrow::array::Array + 'static>(
    kind: PrimitiveType,
    array: A,
    transforms: &'static [Transform],
) -> Input {
    Input {
        kind,
        array: Arc::new(array),
        transforms,
    }
}

#[test]
fn provider_partition_keys_match_the_sdk_data_splitter_across_supported_sources() {
    const IDENTITY: &[Transform] = &[Transform::Identity];
    const BUCKET: &[Transform] = &[Transform::Identity, Transform::Bucket(17)];
    const TRUNCATE: &[Transform] = &[
        Transform::Identity,
        Transform::Bucket(17),
        Transform::Truncate(10),
    ];
    const DATE: &[Transform] = &[
        Transform::Identity,
        Transform::Bucket(17),
        Transform::Year,
        Transform::Month,
        Transform::Day,
    ];
    const TIMESTAMP: &[Transform] = &[
        Transform::Identity,
        Transform::Bucket(17),
        Transform::Year,
        Transform::Month,
        Transform::Day,
        Transform::Hour,
    ];
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    const NANOS_PER_DAY: i64 = MICROS_PER_DAY * 1000;
    let uuid_a = [1_u8; 16];
    let uuid_b = [2_u8; 16];
    let inputs = vec![
        input(
            PrimitiveType::Boolean,
            BooleanArray::from(vec![None, Some(true), Some(true), Some(false)]),
            IDENTITY,
        ),
        input(
            PrimitiveType::Int,
            Int8Array::from(vec![None, Some(-7), Some(-7), Some(3)]),
            TRUNCATE,
        ),
        input(
            PrimitiveType::Int,
            Int16Array::from(vec![None, Some(-101), Some(-101), Some(214)]),
            TRUNCATE,
        ),
        input(
            PrimitiveType::Int,
            Int32Array::from(vec![None, Some(-901), Some(-901), Some(792)]),
            TRUNCATE,
        ),
        input(
            PrimitiveType::Long,
            Int64Array::from(vec![None, Some(-8), Some(-8), Some(12)]),
            TRUNCATE,
        ),
        input(
            PrimitiveType::Float,
            Float32Array::from(vec![None, Some(-0.0), Some(0.0), Some(f32::NAN)]),
            IDENTITY,
        ),
        input(
            PrimitiveType::Double,
            Float64Array::from(vec![None, Some(f64::NAN), Some(-f64::NAN), Some(2.0)]),
            IDENTITY,
        ),
        input(
            PrimitiveType::Decimal {
                precision: 18,
                scale: 2,
            },
            Decimal128Array::from(vec![None, Some(-101), Some(-101), Some(999)])
                .with_precision_and_scale(18, 2)
                .unwrap(),
            TRUNCATE,
        ),
        input(
            PrimitiveType::Date,
            Date32Array::from(vec![None, Some(-1), Some(-1), Some(732)]),
            DATE,
        ),
        input(
            PrimitiveType::Time,
            Time64MicrosecondArray::from(vec![None, Some(11), Some(11), Some(422)]),
            BUCKET,
        ),
        input(
            PrimitiveType::Timestamp,
            TimestampMicrosecondArray::from(vec![None, Some(-1), Some(-1), Some(MICROS_PER_DAY)]),
            TIMESTAMP,
        ),
        input(
            PrimitiveType::Timestamptz,
            TimestampMicrosecondArray::from(vec![None, Some(-1), Some(-1), Some(MICROS_PER_DAY)]),
            TIMESTAMP,
        ),
        input(
            PrimitiveType::TimestampNs,
            TimestampNanosecondArray::from(vec![None, Some(-1), Some(-1), Some(NANOS_PER_DAY)]),
            TIMESTAMP,
        ),
        input(
            PrimitiveType::TimestamptzNs,
            TimestampNanosecondArray::from(vec![None, Some(-1), Some(-1), Some(NANOS_PER_DAY)]),
            TIMESTAMP,
        ),
        input(
            PrimitiveType::String,
            StringArray::from(vec![None, Some("iceberg"), Some("iceberg"), Some("trino")]),
            TRUNCATE,
        ),
        input(
            PrimitiveType::String,
            [None, Some("iceberg"), Some("iceberg"), Some("trino")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
            TRUNCATE,
        ),
        input(
            PrimitiveType::Binary,
            BinaryArray::from(vec![
                None,
                Some(b"a".as_slice()),
                Some(b"a".as_slice()),
                Some(b"b".as_slice()),
            ]),
            BUCKET,
        ),
        input(
            PrimitiveType::Fixed(4),
            BinaryArray::from(vec![
                None,
                Some(b"long".as_slice()),
                Some(b"long".as_slice()),
                Some(b"dige".as_slice()),
            ]),
            BUCKET,
        ),
        input(
            PrimitiveType::Uuid,
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [
                    None,
                    Some(uuid_a.as_slice()),
                    Some(uuid_a.as_slice()),
                    Some(uuid_b.as_slice()),
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
            BUCKET,
        ),
    ];
    for sample in inputs {
        let schema = Arc::new(
            Schema::builder()
                .with_fields([Arc::new(NestedField::optional(
                    1,
                    "source",
                    Type::Primitive(sample.kind.clone()),
                ))])
                .build()
                .unwrap(),
        );
        let input_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "source",
            sample.array.data_type().clone(),
            true,
        )]));
        let input = RecordBatch::try_new(input_schema.clone(), vec![sample.array]).unwrap();
        let adapted =
            adapt_batch(&input, &Arc::new(schema_to_arrow_schema(&schema).unwrap())).unwrap();
        for transform in sample.transforms {
            // Include a void field in the tuple, alongside a real partition field.
            let spec = Arc::new(
                PartitionSpec::builder(schema.clone())
                    .add_partition_field("source", "key", *transform)
                    .unwrap()
                    .add_partition_field("source", "retired_key", Transform::Void)
                    .unwrap()
                    .build()
                    .unwrap(),
            );
            let evaluator =
                IcebergPartitionKeyEvaluator::new(&schema, &spec, &input_schema).unwrap();
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
                schema.clone(),
                spec.clone(),
            )
            .unwrap();
            let keys = evaluator.partition_keys(&input).unwrap_or_else(|error| {
                panic!("partition key for {:?} {transform}: {error}", sample.kind)
            });
            let partitions = (0..input.num_rows())
                .map(|row| {
                    let split = splitter.split(&adapted.slice(row, 1)).unwrap();
                    assert_eq!(split.len(), 1);
                    split[0].0.data().clone()
                })
                .collect::<Vec<_>>();
            for i in 0..input.num_rows() {
                for j in 0..input.num_rows() {
                    assert_eq!(
                        keys.value(i) == keys.value(j),
                        partitions[i] == partitions[j],
                        "{:?} {transform}, rows {i} and {j}",
                        sample.kind,
                    );
                }
            }
            for start in [0, 2] {
                let again = evaluator.partition_keys(&input.slice(start, 2)).unwrap();
                assert_eq!(again.values().as_ref(), &keys.values()[start..start + 2]);
            }
        }
    }
}

#[test]
fn void_of_a_nested_source_does_not_add_a_partition_distinction() {
    let schema = Arc::new(
        Schema::builder()
            .with_fields([
                Arc::new(NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "retired",
                    Type::Struct(StructType::new(vec![Arc::new(NestedField::required(
                        3,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    ))])),
                )),
            ])
            .build()
            .unwrap(),
    );
    let spec = Arc::new(
        PartitionSpec::builder(schema.clone())
            .add_partition_field("id", "partition_id", Transform::Identity)
            .unwrap()
            .add_partition_field("retired", "retired_key", Transform::Void)
            .unwrap()
            .build()
            .unwrap(),
    );
    let target = Arc::new(schema_to_arrow_schema(&schema).unwrap());
    let arrow::datatypes::DataType::Struct(fields) = target.field(1).data_type() else {
        panic!("nested retired field");
    };
    let values = arrow::array::StructArray::new(
        fields.clone(),
        vec![Arc::new(Int64Array::from(vec![7, 8, 9]))],
        None,
    );
    let batch = RecordBatch::try_new(
        target.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 1, 2])), Arc::new(values)],
    )
    .unwrap();
    let evaluator = IcebergPartitionKeyEvaluator::new(&schema, &spec, &target).unwrap();
    let splitter =
        RecordBatchPartitionSplitter::try_new_with_computed_values(schema, spec).unwrap();
    let keys = evaluator.partition_keys(&batch).unwrap();
    assert_eq!(keys.value(0), keys.value(1));
    assert_ne!(keys.value(0), keys.value(2));
    let split = splitter.split(&batch).unwrap();
    assert_eq!(split.len(), 2);
    assert!(split.iter().all(|(key, _)| key.data().is_null_at_index(1)));
}
