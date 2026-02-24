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
use super::common::{datetime_from_local_now, naive_to_date32};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Date32Array};
use std::sync::Arc;

pub fn eval_current_date(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let date = datetime_from_local_now().date();
    let days = naive_to_date32(date);
    let values = vec![Some(days); chunk.len()];
    Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

pub fn eval_curdate(
    _arena: &ExprArena,
    _expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let date = datetime_from_local_now().date();
    let days = naive_to_date32(date);
    let values = vec![Some(days); chunk.len()];
    Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::date::test_utils::assert_date_function_logic;

    #[test]
    fn test_current_date_logic() {
        assert_date_function_logic("current_date");
    }

    #[test]
    fn test_curdate_logic() {
        assert_date_function_logic("curdate");
    }
}
