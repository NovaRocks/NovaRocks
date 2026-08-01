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

//! Nested expression lowering.

use arrow::datatypes::DataType;

use super::{decode_expr_at, decode_expr_type_at};
use novarocks::exec::expr::{ExprArena, ExprId};
use novarocks::protocol::FieldPath;
use novarocks_protocol::expr;

use super::NativeExpressionInputLayout;

pub(crate) fn lower_nested(
    nested: &expr::NestedExpr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
    data_type: DataType,
) -> Result<ExprId, super::NativeExpressionDecodeError> {
    let inner = nested.inner.as_ref().ok_or_else(|| {
        super::NativeExpressionDecodeError::missing(
            path.clone().field("inner"),
            "native NestedExpr requires inner",
        )
    })?;
    let inner_type = decode_expr_type_at(inner, path.clone().field("inner"))?;
    if inner_type != data_type {
        return Err(super::NativeExpressionDecodeError::inconsistent(
            path.clone().field("inner").field("type"),
            format!("NestedExpr type {data_type:?} does not match inner type {inner_type:?}"),
        ));
    }
    decode_expr_at(inner, path.field("inner"), arena, input_layout)
}

pub(super) fn is_encoded_variant_payload_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::Null
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::{lower_err_with_slots, scalar_expr, string_lit};
    use arrow::datatypes::DataType;
    use novarocks_protocol::expr;

    #[test]
    fn nested_requires_outer_and_inner_type_match() {
        let nested = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Nested(Box::new(expr::NestedExpr {
                inner: Some(Box::new(string_lit("x"))),
            })),
        );

        let err = lower_err_with_slots(&nested, &[]);
        assert!(err.contains("NestedExpr type Int64 does not match inner type Utf8"));
    }
}
