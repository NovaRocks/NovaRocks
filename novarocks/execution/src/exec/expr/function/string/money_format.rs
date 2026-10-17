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
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow::datatypes::DataType;
use novarocks_functions::builtin::string_extended::{StringOperation, evaluate_legacy};

pub fn eval_money_format<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let input = arena.eval(args[0], chunk)?;
    macro_rules! checked {
        ($ty:ty, $message:expr) => {
            input
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| $message.to_string())?
        };
    }
    match input.data_type() {
        DataType::Int8 => {
            checked!(Int8Array, "money_format expects int8");
        }
        DataType::Int16 => {
            checked!(Int16Array, "money_format expects int16");
        }
        DataType::Int32 => {
            checked!(Int32Array, "money_format expects int32");
        }
        DataType::Int64 => {
            checked!(Int64Array, "money_format expects int64");
        }
        DataType::Float32 => {
            checked!(Float32Array, "money_format expects float");
        }
        DataType::Float64 => {
            checked!(Float64Array, "money_format expects double");
        }
        DataType::Decimal128(..) => {
            checked!(Decimal128Array, "money_format expects decimal");
        }
        other => return Err((format!("money_format expects numeric, got {:?}", other)).into()),
    }
    let rows = input.len();
    Ok((evaluate_legacy(StringOperation::Money, &[input], rows))?)
}
