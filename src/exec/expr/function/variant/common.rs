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
use arrow::array::{ArrayRef, LargeBinaryArray};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

pub(super) fn eval_variant_arg(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!(
            "{} expects 1 argument, got {}",
            fn_name,
            args.len()
        ));
    }
    arena.eval(args[0], chunk)
}

pub(super) fn downcast_variant_arg<'a>(
    variant_array: &'a ArrayRef,
    fn_name: &str,
) -> Result<&'a LargeBinaryArray, String> {
    variant_array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| format!("{} expects LargeBinary for variant argument", fn_name))
}

#[cfg(test)]
pub(crate) fn variant_primitive_serialized(type_id: u8, payload: &[u8]) -> Vec<u8> {
    let metadata = crate::exec::variant::VariantMetadata::empty();
    let mut value = vec![type_id << 2];
    value.extend_from_slice(payload);
    crate::exec::variant::VariantValue::create(metadata.raw(), &value)
        .unwrap()
        .serialize()
}
