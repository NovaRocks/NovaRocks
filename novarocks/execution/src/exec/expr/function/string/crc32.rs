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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;

pub fn eval_crc32<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let input = arena.eval(args[0], chunk)?;
    Ok((novarocks_functions::builtin::crc32::evaluate_legacy(&input))?)
}

#[cfg(test)]
mod legacy_crc32_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{
        Array, BinaryArray, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    // Independent Python standard-library zlib.crc32 fixtures. Recording old
    // runtime carriers does not install additional pure function profiles.
    const TEXT: [&str; 6] = ["", "123456789", "hello", "你好", "\0", "a\0b"];
    const CRC: [i64; 6] = [
        0,
        3_421_780_262,
        907_060_870,
        1_352_841_281,
        3_523_407_757,
        367_556_721,
    ];

    fn evaluate(input: ArrayRef) -> Result<Vec<Option<i64>>, String> {
        let slot = SlotId::new(1);
        let dtype = input.data_type().clone();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("input", dtype.clone(), true)])),
            vec![input],
        )
        .unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot])
                .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(slot), dtype);
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::String("crc32"),
                args: vec![input],
            },
            DataType::Int64,
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(output.data_type(), &DataType::Int64);
        Ok(output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect())
    }

    #[test]
    fn frozen_utf8_ieee_oracle_keeps_nul_utf8_and_positive_unsigned_checksum() {
        let input: ArrayRef = Arc::new(StringArray::from(TEXT.to_vec()));
        assert_eq!(evaluate(input).unwrap(), CRC.map(Some));
        assert!(CRC[1] > i64::from(i32::MAX));
    }

    #[test]
    fn frozen_nullable_slice_keeps_selected_bytes_and_empty_distinct_from_null() {
        let backing: ArrayRef = Arc::new(StringArray::from(vec![
            Some("discard"),
            None,
            Some(""),
            Some("hello"),
            Some("discard"),
        ]));
        assert_eq!(
            evaluate(backing.slice(1, 3)).unwrap(),
            [None, Some(0), Some(907_060_870)]
        );
    }

    #[test]
    fn frozen_old_large_text_and_binary_carriers_keep_the_same_ieee_bytes() {
        let bytes: Vec<_> = TEXT.iter().map(|text| text.as_bytes()).collect();
        let inputs: [ArrayRef; 3] = [
            Arc::new(LargeStringArray::from(TEXT.to_vec())),
            Arc::new(BinaryArray::from(bytes.clone())),
            Arc::new(LargeBinaryArray::from(bytes)),
        ];
        for input in inputs {
            assert_eq!(evaluate(input).unwrap(), CRC.map(Some));
        }
    }

    #[test]
    fn frozen_long_text_has_independent_zlib_oracle_across_batch_partition() {
        let long = "x".repeat(10_000);
        let input: ArrayRef = Arc::new(StringArray::from(vec![long.as_str(); 3]));
        assert_eq!(evaluate(input.clone()).unwrap(), [Some(223_716_515); 3]);
        for row in 0..3 {
            assert_eq!(evaluate(input.slice(row, 1)).unwrap(), [Some(223_716_515)]);
        }
    }

    #[test]
    fn frozen_integer_carrier_is_explicitly_unsupported() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        assert!(
            evaluate(input)
                .unwrap_err()
                .contains("crc32 expects VARCHAR/BINARY input")
        );
    }
}
