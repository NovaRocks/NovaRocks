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
use super::LiteralValue;
use crate::common::largeint;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, NullArray,
    StringArray,
};
use std::sync::Arc;

pub fn eval(value: &LiteralValue, len: usize) -> Result<ArrayRef, String> {
    match value {
        LiteralValue::Null => Ok(Arc::new(NullArray::new(len))),
        LiteralValue::Int8(v) => {
            let arr = Int8Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Int16(v) => {
            let arr = Int16Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Int32(v) => {
            let arr = Int32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Bool(v) => {
            let arr = BooleanArray::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Int64(v) => {
            let arr = Int64Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::LargeInt(v) => {
            let values = vec![Some(*v); len];
            largeint::array_from_i128(&values)
        }
        LiteralValue::Float32(v) => {
            let arr = Float32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Float64(v) => {
            let arr = Float64Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Utf8(v) => {
            let arr = StringArray::from(vec![v.as_str(); len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Binary(v) => {
            let values = std::iter::repeat(v.as_slice())
                .take(len)
                .collect::<Vec<&[u8]>>();
            let arr = BinaryArray::from_vec(values);
            Ok(Arc::new(arr))
        }
        LiteralValue::Date32(v) => {
            let arr = Date32Array::from(vec![*v; len]);
            Ok(Arc::new(arr))
        }
        LiteralValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let arr = Decimal128Array::from(vec![*value; len])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(arr))
        }
        LiteralValue::Decimal256 {
            value,
            precision,
            scale,
        } => {
            let arr = Decimal256Array::from(vec![*value; len])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| e.to_string())?;
            Ok(Arc::new(arr))
        }
    }
}
