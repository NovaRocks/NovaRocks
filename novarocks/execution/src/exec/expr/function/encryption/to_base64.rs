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
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{ArrayRef, StringArray};
use std::sync::Arc;

fn should_prefer_latin1_bytes<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    arg: ExprId,
) -> bool {
    let name = match arena.node(arg) {
        Some(ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            ..
        }) => Some(*name),
        _ => None,
    };
    novarocks_type_contract::ToBase64ByteSource::from_immediate_encryption_identity(name)
        .prefers_latin1()
}

pub fn eval_to_base64<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let _ = expr;
    let prefer_latin1 = should_prefer_latin1_bytes(arena, args[0]);
    let input = super::common::to_owned_bytes_array(arena.eval(args[0], chunk)?, "to_base64", 0)?;

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        out.push(novarocks_functions::builtin::to_base64_shared::encode_row(
            &input,
            row,
            if prefer_latin1 {
                novarocks_type_contract::ToBase64ByteSource::NativeV1EncryptionLatin1
            } else {
                novarocks_type_contract::ToBase64ByteSource::Ordinary
            },
        ));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
