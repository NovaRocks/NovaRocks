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

//! Fragment function call expression lowering.

use arrow::datatypes::DataType;

use super::{collection, lower_expr_list};
use novarocks_execution::exec::expr::function::{FunctionKind, function_metadata};
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::expr;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_function_call(
    expression: &expr::Expr,
    call: &expr::FunctionCall,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
    declared: Option<&novarocks_type_contract::FunctionValueType>,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    if call.distinct {
        return Err(super::NativeExpressionDecodeError::unsupported(
            path.clone().field("distinct"),
            format!(
                "DISTINCT scalar FunctionCall '{}' is unsupported",
                call.function_name
            ),
        ));
    }
    use crate::fragment_original_requests::scalar::NativeScalarActor;
    let actor = type_decoder.validate_default_scalar(expression, call, declared, path.clone())?;
    if matches!(actor, NativeScalarActor::ArrayLiteral) {
        return collection::lower_array_literal(
            call,
            path,
            arena,
            input_layout,
            data_type,
            type_decoder,
        );
    }
    if matches!(actor, NativeScalarActor::CarrierCast) {
        let [operand] = call.args.as_slice() else {
            return Err(super::NativeExpressionDecodeError::inconsistent(
                path.clone().field("args"),
                "validated scalar CAST requires its one original operand",
            ));
        };
        let child = super::decode_expr_at_with_types(
            operand,
            path.clone().field("args").index(0),
            arena,
            input_layout,
            type_decoder,
        )?;
        return Ok(arena.push_typed(
            ExprNode::Cast(
                child,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            data_type,
        ));
    }
    if call.function_name.eq_ignore_ascii_case("map") {
        return collection::lower_map_constructor(
            call,
            path,
            arena,
            input_layout,
            data_type,
            declared,
            type_decoder,
        );
    }
    let (kind, regexp_count_source) = match actor {
        NativeScalarActor::Registered(kind) => (kind, None),
        NativeScalarActor::RegexpCount(source) => {
            (FunctionKind::String("regexp_count"), Some(source))
        }
        _ => unreachable!("the original constructor actors were handled above"),
    };
    let args = lower_expr_list(
        &call.args,
        path.clone().field("args"),
        arena,
        input_layout,
        type_decoder,
    )?;
    validate_function_arity(&call.function_name, kind, args.len()).map_err(|error| {
        super::NativeExpressionDecodeError::invalid_value(path.clone().field("args"), error)
    })?;
    let id = arena.push_typed(ExprNode::FunctionCall { kind, args }, data_type);
    if let Some(source) = regexp_count_source {
        arena
            .set_regexp_count_pattern_source(id, source)
            .map_err(|error| {
                super::NativeExpressionDecodeError::invalid_value(path.clone(), error)
            })?;
    }
    Ok(id)
}

pub(super) fn validate_function_arity(
    name: &str,
    kind: FunctionKind,
    arg_count: usize,
) -> Result<(), String> {
    let metadata = function_metadata(kind);
    if arg_count < metadata.min_args || arg_count > metadata.max_args {
        return Err(format!(
            "function '{}' expects {} to {} arguments, got {}",
            name, metadata.min_args, metadata.max_args, arg_count
        ));
    }
    Ok(())
}
