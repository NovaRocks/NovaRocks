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
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};

pub(super) const OLAP_STRING_MAX_LENGTH: usize = 1_048_576;

pub(super) enum IntArgArray<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl IntArgArray<'_> {
    pub(super) fn len(&self) -> usize {
        match self {
            IntArgArray::Int32(arr) => arr.len(),
            IntArgArray::Int64(arr) => arr.len(),
        }
    }

    pub(super) fn is_null(&self, row: usize) -> bool {
        match self {
            IntArgArray::Int32(arr) => arr.is_null(row),
            IntArgArray::Int64(arr) => arr.is_null(row),
        }
    }

    pub(super) fn value(&self, row: usize) -> i64 {
        match self {
            IntArgArray::Int32(arr) => arr.value(row) as i64,
            IntArgArray::Int64(arr) => arr.value(row),
        }
    }
}

pub(super) fn downcast_int_arg_array<'a>(
    arr: &'a ArrayRef,
    func_name: &str,
) -> Result<IntArgArray<'a>, String> {
    if let Some(v) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(IntArgArray::Int32(v));
    }
    if let Some(v) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(IntArgArray::Int64(v));
    }
    Err(format!("{func_name} expects int"))
}
