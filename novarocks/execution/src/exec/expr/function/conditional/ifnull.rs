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
use arrow::array::{Array, ArrayRef};
use arrow::compute::kernels::boolean::is_not_null;
use arrow::compute::kernels::cast;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;

// IFNULL function for Arrow arrays
pub fn eval_ifnull(
    arena: &ExprArena,
    expr1: ExprId,
    expr2: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array1 = arena.eval(expr1, chunk)?;
    let array2 = arena.eval(expr2, chunk)?;

    let mut target_type = array1.data_type().clone();
    if matches!(target_type, DataType::Null) && !matches!(array2.data_type(), DataType::Null) {
        target_type = array2.data_type().clone();
    }

    let array1 = if array1.data_type() != &target_type {
        cast(array1.as_ref(), &target_type).map_err(|e| e.to_string())?
    } else {
        array1
    };
    let array2 = if array2.data_type() != &target_type {
        cast(array2.as_ref(), &target_type).map_err(|e| e.to_string())?
    } else {
        array2
    };

    let mask = is_not_null(array1.as_ref()).map_err(|e| e.to_string())?;
    let result = zip(
        &mask,
        &array1.as_ref() as &dyn arrow::array::Datum,
        &array2.as_ref() as &dyn arrow::array::Datum,
    )
    .map_err(|e| e.to_string())?;
    Ok(result)
}
