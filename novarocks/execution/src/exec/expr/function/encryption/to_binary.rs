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
use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use novarocks_functions::builtin::string_extended::{StringOperation, evaluate_legacy};

pub fn eval_to_binary<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    if args.len() != 1 && args.len() != 2 {
        return Err(("to_binary expects 1 or 2 arguments".to_string()).into());
    }

    let input = arena.eval(args[0], chunk)?;
    input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "to_binary expects VARCHAR as first argument".to_string())?;
    let mut inputs = vec![input];
    if args.len() == 2 {
        let format = arena.eval(args[1], chunk)?;
        format
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "to_binary expects VARCHAR format argument".to_string())?;
        inputs.push(format);
    }
    let output = evaluate_legacy(StringOperation::ToBinary, &inputs, chunk.len())?;
    // The v1 shell's declared carrier controls its Latin1 projection.
    let binary = output
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("shared TO_BINARY output is Binary");
    let values = binary
        .iter()
        .map(|value| value.map(<[u8]>::to_vec))
        .collect();
    Ok((super::common::build_bytes_output_latin1(values, arena.data_type(expr)))?)
}
