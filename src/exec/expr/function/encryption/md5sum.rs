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
use arrow::array::{ArrayRef, StringArray};
use md5::{Digest, Md5};
use std::sync::Arc;

pub fn eval_md5sum(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;

    let mut inputs = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        inputs.push(super::common::to_owned_bytes_array(
            arena.eval(*arg, chunk)?,
            "md5sum",
            idx,
        )?);
    }

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let mut hasher = Md5::new();
        for input in &inputs {
            if input.is_null(row) {
                continue;
            }
            hasher.update(input.bytes(row));
        }
        out.push(Some(hex::encode(hasher.finalize())));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
