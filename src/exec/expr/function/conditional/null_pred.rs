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
use arrow::array::{ArrayRef, BooleanArray};
use std::sync::Arc;

// IS NULL predicate for Arrow arrays
pub fn eval_is_null(arena: &ExprArena, child: ExprId, chunk: &Chunk) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;

    // Check if array has null buffer
    if let Some(null_buffer) = array.nulls() {
        // Invert the null buffer: Arrow stores validity (1 = valid, 0 = null),
        // we want is_null (1 = null, 0 = not null)
        let null_count = null_buffer.null_count();
        let len = array.len();

        if null_count == 0 {
            // No nulls, return all false
            Ok(Arc::new(BooleanArray::from(vec![false; len])))
        } else if null_count == len {
            // All nulls, return all true
            Ok(Arc::new(BooleanArray::from(vec![true; len])))
        } else {
            // Mixed nulls and non-nulls
            let result: Vec<bool> = (0..len).map(|i| array.is_null(i)).collect();
            Ok(Arc::new(BooleanArray::from(result)))
        }
    } else {
        // No null buffer means no nulls, return all false
        Ok(Arc::new(BooleanArray::from(vec![false; array.len()])))
    }
}

// IS NOT NULL predicate for Arrow arrays
pub fn eval_is_not_null(
    arena: &ExprArena,
    child: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(child, chunk)?;

    // Check if array has null buffer
    if let Some(null_buffer) = array.nulls() {
        // Arrow stores validity (1 = valid, 0 = null),
        // we want is_not_null (1 = not null, 0 = null)
        let null_count = null_buffer.null_count();
        let len = array.len();

        if null_count == 0 {
            // No nulls, return all true
            Ok(Arc::new(BooleanArray::from(vec![true; len])))
        } else if null_count == len {
            // All nulls, return all false
            Ok(Arc::new(BooleanArray::from(vec![false; len])))
        } else {
            // Mixed nulls and non-nulls
            let result: Vec<bool> = (0..len).map(|i| array.is_valid(i)).collect();
            Ok(Arc::new(BooleanArray::from(result)))
        }
    } else {
        // No null buffer means no nulls, return all true
        Ok(Arc::new(BooleanArray::from(vec![true; array.len()])))
    }
}
