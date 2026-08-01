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

//! Proto expression lowering.

use arrow::datatypes::DataType;

use self::error::NativeExpressionDecodeError;
use super::type_decode::{decode_field_type, decode_type};
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::ChunkFieldSchema;
use novarocks::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks::protocol::FieldPath;
use novarocks_protocol::expr;

mod binary;
mod case;
mod cast;
mod collection;
mod error;
mod function_call;
mod lambda;
mod literal;
mod min_max;
mod nested;
mod predicate;
mod unary;

/// Immutable input-slot layout used solely by Backend native expression
/// decoding. It carries no runtime or connector state.
#[derive(Clone, Debug, Default)]
pub(crate) struct NativeExpressionInputLayout {
    slots: Vec<SlotId>,
}

impl NativeExpressionInputLayout {
    pub(crate) fn from_slot_ids(slots: impl IntoIterator<Item = SlotId>) -> Self {
        let mut layout = Self::default();
        for slot in slots {
            if !layout.slots.contains(&slot) {
                layout.slots.push(slot);
            }
        }
        layout
    }

    pub(crate) fn resolve_column_id(
        &self,
        column_id: u32,
        path: FieldPath,
    ) -> Result<SlotId, novarocks::protocol::ProtocolError> {
        let slot = SlotId::new(column_id);
        if self.slots.contains(&slot) {
            Ok(slot)
        } else {
            Err(novarocks::protocol::ProtocolError::new(
                novarocks::protocol::ProtocolFamily::Native,
                path.field("column_id"),
                novarocks::protocol::ProtocolErrorKind::InvalidValue,
                format!("ColumnRef column_id={column_id} not found in input layout"),
            ))
        }
    }
}

pub(crate) use min_max::extract_min_max_predicates;

#[allow(dead_code)]
pub(crate) fn decode_expr(
    e: &expr::Expr,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<ExprId, NativeExpressionDecodeError> {
    decode_expr_at(e, FieldPath::root("expr"), arena, input_layout)
}

pub(crate) fn decode_expr_at(
    e: &expr::Expr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<ExprId, NativeExpressionDecodeError> {
    validate_proto_expr_shape_at(e, path.clone())?;
    let data_type = decode_expr_type_at(e, path.clone())?;
    let kind = e.kind.as_ref().expect("validated native expression kind");

    let id = match kind {
        expr::expr::Kind::ColumnRef(column) => {
            let slot_id = input_layout
                .resolve_column_id(column.column_id, path.clone().field("column_ref"))?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Literal(literal) => {
            let literal_path = path.clone().field("literal");
            let value = literal::lower_literal_at(literal, &data_type)
                .map_err(|error| error.into_protocol(literal_path))?;
            Ok(arena.push_typed(ExprNode::Literal(value), data_type))
        }
        expr::expr::Kind::BinaryOp(binary) => binary::lower_binary_op(
            binary,
            path.clone().field("binary_op"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::UnaryOp(unary) => unary::lower_unary_op(
            unary,
            path.clone().field("unary_op"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::FunctionCall(call) => function_call::lower_function_call(
            call,
            path.clone().field("function_call"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::AggregateCall(_) => Err(NativeExpressionDecodeError::unsupported(
            path.clone().field("aggregate_call"),
            "native scalar expression lowering does not lower AggregateCall",
        )),
        expr::expr::Kind::WindowCall(_) => Err(NativeExpressionDecodeError::unsupported(
            path.clone().field("window_call"),
            "native scalar expression lowering does not lower WindowCall",
        )),
        expr::expr::Kind::Cast(cast) => cast::lower_cast(
            cast,
            path.clone().field("cast"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::IsNull(is_null) => predicate::lower_is_null(
            is_null,
            path.clone().field("is_null"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::InList(in_list) => predicate::lower_in_list(
            in_list,
            path.clone().field("in_list"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::Between(between) => predicate::lower_between(
            between,
            path.clone().field("between"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::Like(like) => predicate::lower_like(
            like,
            path.clone().field("like"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::CaseExpr(case_expr) => case::lower_case(
            case_expr,
            path.clone().field("case_expr"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::IsTruth(is_truth) => predicate::lower_is_truth(
            is_truth,
            path.clone().field("is_truth"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::LambdaParamRef(param) => {
            let slot_id = SlotId::try_from(param.slot_id).map_err(|error| {
                NativeExpressionDecodeError::out_of_range(
                    path.clone().field("lambda_param_ref").field("slot_id"),
                    error,
                )
            })?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Lambda(lambda) => lambda::lower_lambda(
            lambda,
            path.clone().field("lambda"),
            arena,
            input_layout,
            data_type,
        ),
        expr::expr::Kind::Nested(nested) => nested::lower_nested(
            nested,
            path.clone().field("nested"),
            arena,
            input_layout,
            data_type,
        ),
    }?;
    set_proto_field_schema(e, arena, id);
    Ok(id)
}

fn decode_expr_type_at(
    e: &expr::Expr,
    path: FieldPath,
) -> Result<DataType, NativeExpressionDecodeError> {
    let desc = e.r#type.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("type"),
            "native Expr requires type",
        )
    })?;
    decode_type(desc)
        .map_err(|error| NativeExpressionDecodeError::invalid_value(path.field("type"), error))
}

pub(crate) fn validate_proto_expr_shape_at(
    e: &expr::Expr,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    let type_desc = e.r#type.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("type"),
            "native Expr requires type",
        )
    })?;
    decode_type(type_desc).map_err(|error| {
        NativeExpressionDecodeError::invalid_value(
            path.clone().field("type"),
            format!("native Expr type is invalid: {error}"),
        )
    })?;
    let kind = e.kind.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("kind"),
            "native Expr requires kind",
        )
    })?;

    let required_boxed = |owner_path: FieldPath,
                          child: &Option<Box<expr::Expr>>,
                          field: &'static str|
     -> Result<(), NativeExpressionDecodeError> {
        let child_path = owner_path.field(field);
        let child = child.as_deref().ok_or_else(|| {
            NativeExpressionDecodeError::missing(
                child_path.clone(),
                format!("native Expr requires {field}"),
            )
        })?;
        validate_proto_expr_shape_at(child, child_path)
    };
    let list = |owner_path: FieldPath,
                values: &[expr::Expr],
                field: &'static str|
     -> Result<(), NativeExpressionDecodeError> {
        for (index, value) in values.iter().enumerate() {
            validate_proto_expr_shape_at(value, owner_path.clone().field(field).index(index))?;
        }
        Ok(())
    };

    match kind {
        expr::expr::Kind::BinaryOp(binary) => {
            let path = path.clone().field("binary_op");
            let op = expr::BinaryOp::try_from(binary.op).map_err(|_| {
                NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    format!("unknown BinaryOp {}", binary.op),
                )
            })?;
            if op == expr::BinaryOp::Unspecified {
                return Err(NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    "BinaryOp.op is unspecified",
                ));
            }
            required_boxed(path.clone(), &binary.left, "left")?;
            required_boxed(path, &binary.right, "right")?;
        }
        expr::expr::Kind::UnaryOp(unary) => {
            let path = path.clone().field("unary_op");
            let op = expr::UnaryOp::try_from(unary.op).map_err(|_| {
                NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    format!("unknown UnaryOp {}", unary.op),
                )
            })?;
            if op == expr::UnaryOp::Unspecified {
                return Err(NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    "UnaryOp.op is unspecified",
                ));
            }
            required_boxed(path, &unary.operand, "operand")?;
        }
        expr::expr::Kind::FunctionCall(call) => {
            let path = path.clone().field("function_call");
            validate_function_name_at(&call.function_name, path.clone().field("function_name"))?;
            list(path, &call.args, "args")?;
        }
        expr::expr::Kind::AggregateCall(call) => {
            let path = path.clone().field("aggregate_call");
            validate_function_name_at(&call.function_name, path.clone().field("function_name"))?;
            list(path.clone(), &call.args, "args")?;
            validate_sort_items_at(&call.order_by, path.clone().field("order_by"))?;
        }
        expr::expr::Kind::WindowCall(call) => {
            let path = path.clone().field("window_call");
            validate_function_name_at(&call.function_name, path.clone().field("function_name"))?;
            list(path.clone(), &call.args, "args")?;
            list(path.clone(), &call.partition_by, "partition_by")?;
            validate_sort_items_at(&call.order_by, path.clone().field("order_by"))?;
            if let Some(frame) = &call.frame {
                validate_window_frame_at(frame, path.clone().field("frame"))?;
            }
        }
        expr::expr::Kind::Cast(cast) => {
            let path = path.clone().field("cast");
            required_boxed(path.clone(), &cast.operand, "operand")?;
            let target = cast.target.as_ref().ok_or_else(|| {
                NativeExpressionDecodeError::missing(
                    path.clone().field("target"),
                    "native Cast requires target",
                )
            })?;
            decode_type(target).map_err(|error| {
                NativeExpressionDecodeError::invalid_value(path.clone().field("target"), error)
            })?;
        }
        expr::expr::Kind::IsNull(is_null) => {
            required_boxed(path.clone().field("is_null"), &is_null.operand, "operand")?
        }
        expr::expr::Kind::InList(in_list) => {
            let path = path.clone().field("in_list");
            required_boxed(path.clone(), &in_list.operand, "operand")?;
            if in_list.list.is_empty() {
                return Err(NativeExpressionDecodeError::invalid_value(
                    path.clone().field("list"),
                    "InList.list is empty",
                ));
            }
            list(path, &in_list.list, "list")?;
        }
        expr::expr::Kind::Between(between) => {
            let path = path.clone().field("between");
            required_boxed(path.clone(), &between.operand, "operand")?;
            required_boxed(path.clone(), &between.low, "low")?;
            required_boxed(path, &between.high, "high")?;
        }
        expr::expr::Kind::Like(like) => {
            let path = path.clone().field("like");
            required_boxed(path.clone(), &like.operand, "operand")?;
            required_boxed(path, &like.pattern, "pattern")?;
        }
        expr::expr::Kind::CaseExpr(case_expr) => {
            let path = path.clone().field("case_expr");
            if let Some(operand) = &case_expr.operand {
                validate_proto_expr_shape_at(operand, path.clone().field("operand"))?;
            }
            if case_expr.when_then.is_empty() {
                return Err(NativeExpressionDecodeError::invalid_value(
                    path.clone().field("when_then"),
                    "CaseExpr.when_then is empty",
                ));
            }
            for (index, branch) in case_expr.when_then.iter().enumerate() {
                let branch_path = path.clone().field("when_then").index(index);
                let when = branch.when.as_ref().ok_or_else(|| {
                    NativeExpressionDecodeError::missing(
                        branch_path.clone().field("when"),
                        "native CaseExpr branch requires when",
                    )
                })?;
                validate_proto_expr_shape_at(when, branch_path.clone().field("when"))?;
                let then = branch.then.as_ref().ok_or_else(|| {
                    NativeExpressionDecodeError::missing(
                        branch_path.clone().field("then"),
                        "native CaseExpr branch requires then",
                    )
                })?;
                validate_proto_expr_shape_at(then, branch_path.field("then"))?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                validate_proto_expr_shape_at(else_expr, path.clone().field("else_expr"))?;
            }
        }
        expr::expr::Kind::IsTruth(is_truth) => {
            required_boxed(path.clone().field("is_truth"), &is_truth.operand, "operand")?
        }
        expr::expr::Kind::Lambda(lambda) => {
            let path = path.clone().field("lambda");
            if lambda.params.is_empty() {
                return Err(NativeExpressionDecodeError::invalid_value(
                    path.clone().field("params"),
                    "LambdaExpr.params is empty",
                ));
            }
            let mut slots = std::collections::BTreeSet::new();
            for (index, param) in lambda.params.iter().enumerate() {
                let param_path = path.clone().field("params").index(index);
                if param.slot_id <= 0 {
                    return Err(NativeExpressionDecodeError::out_of_range(
                        param_path.clone().field("slot_id"),
                        "Lambda parameter slot_id must be positive",
                    ));
                }
                if !slots.insert(param.slot_id) {
                    return Err(NativeExpressionDecodeError::inconsistent(
                        param_path.field("slot_id"),
                        format!("duplicate Lambda parameter slot_id={}", param.slot_id),
                    ));
                }
                let param_type = param.r#type.as_ref().ok_or_else(|| {
                    NativeExpressionDecodeError::missing(
                        param_path.clone().field("type"),
                        "native Lambda parameter requires type",
                    )
                })?;
                decode_type(param_type).map_err(|error| {
                    NativeExpressionDecodeError::invalid_value(param_path.field("type"), error)
                })?;
            }
            required_boxed(path, &lambda.body, "body")?;
        }
        expr::expr::Kind::Nested(nested) => {
            required_boxed(path.clone().field("nested"), &nested.inner, "inner")?
        }
        expr::expr::Kind::ColumnRef(column) => {
            let path = path.clone().field("column_ref");
            if column.column_id == 0 {
                return Err(NativeExpressionDecodeError::out_of_range(
                    path.clone().field("column_id"),
                    "ColumnRef.column_id must be positive",
                ));
            }
        }
        expr::expr::Kind::Literal(literal) => {
            let path = path.clone().field("literal");
            let value = literal.value.as_ref().ok_or_else(|| {
                NativeExpressionDecodeError::missing(
                    path.clone().field("value"),
                    "native LiteralExpr requires value",
                )
            })?;
            if value.value.is_none() {
                return Err(NativeExpressionDecodeError::missing(
                    path.clone().field("value").field("value"),
                    "native LiteralValue requires value",
                ));
            }
        }
        expr::expr::Kind::LambdaParamRef(param) => {
            let path = path.clone().field("lambda_param_ref");
            if param.slot_id <= 0 {
                return Err(NativeExpressionDecodeError::out_of_range(
                    path.clone().field("slot_id"),
                    "LambdaParamRef.slot_id must be positive",
                ));
            }
        }
    }
    Ok(())
}

fn validate_function_name_at(
    name: &str,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    if name.is_empty() {
        return Err(NativeExpressionDecodeError::invalid_value(
            path,
            "function_name is empty",
        ));
    }
    Ok(())
}

fn validate_sort_items_at(
    items: &[expr::SortItem],
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    for (index, item) in items.iter().enumerate() {
        let expression_path = path.clone().index(index).field("expr");
        let expression = item.expr.as_ref().ok_or_else(|| {
            NativeExpressionDecodeError::missing(
                expression_path.clone(),
                "native SortItem requires expr",
            )
        })?;
        validate_proto_expr_shape_at(expression, expression_path)?;
    }
    Ok(())
}

fn validate_window_frame_at(
    frame: &expr::WindowFrame,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    let frame_type = expr::WindowFrameType::try_from(frame.frame_type).map_err(|_| {
        NativeExpressionDecodeError::invalid_enum(
            path.clone().field("frame_type"),
            format!("unknown WindowFrameType {}", frame.frame_type),
        )
    })?;
    if frame_type == expr::WindowFrameType::Unspecified {
        return Err(NativeExpressionDecodeError::invalid_enum(
            path.clone().field("frame_type"),
            "WindowFrame.frame_type is unspecified",
        ));
    }
    let start = frame.start.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("start"),
            "native WindowFrame requires start",
        )
    })?;
    validate_window_bound_at(start, path.clone().field("start"))?;
    let end = frame.end.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("end"),
            "native WindowFrame requires end",
        )
    })?;
    validate_window_bound_at(end, path.field("end"))?;
    Ok(())
}

fn validate_window_bound_at(
    bound: &expr::WindowBound,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    match bound.bound.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("bound"),
            "native WindowBound requires bound",
        )
    })? {
        expr::window_bound::Bound::UnboundedPreceding(true)
        | expr::window_bound::Bound::CurrentRow(true)
        | expr::window_bound::Bound::UnboundedFollowing(true) => Ok(()),
        expr::window_bound::Bound::UnboundedPreceding(false)
        | expr::window_bound::Bound::CurrentRow(false)
        | expr::window_bound::Bound::UnboundedFollowing(false) => Err(
            NativeExpressionDecodeError::invalid_value(path, "WindowBound marker must be true"),
        ),
        expr::window_bound::Bound::Preceding(offset)
        | expr::window_bound::Bound::Following(offset)
            if offset >= &0 =>
        {
            Ok(())
        }
        expr::window_bound::Bound::Preceding(offset)
        | expr::window_bound::Bound::Following(offset) => {
            Err(NativeExpressionDecodeError::out_of_range(
                path,
                format!("WindowBound offset must be nonnegative, got {offset}"),
            ))
        }
    }
}

fn set_proto_field_schema(e: &expr::Expr, arena: &mut ExprArena, id: ExprId) {
    let Some(desc) = e.r#type.as_ref() else {
        return;
    };
    let Ok(field) = decode_field_type("_expr", e.nullable, desc) else {
        return;
    };
    if let Ok(field_schema) = ChunkFieldSchema::from_field(&field) {
        arena.set_field_schema(id, field_schema);
    }
}

fn lower_required_child(
    child: &Option<Box<expr::Expr>>,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<ExprId, NativeExpressionDecodeError> {
    let child = child.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(path.clone(), "native Expr child is required")
    })?;
    decode_expr_at(child, path, arena, input_layout)
}

fn lower_required_unboxed_child(
    child: &Option<expr::Expr>,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<ExprId, NativeExpressionDecodeError> {
    let child = child.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(path.clone(), "native Expr child is required")
    })?;
    decode_expr_at(child, path, arena, input_layout)
}

fn lower_expr_list(
    values: &[expr::Expr],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<Vec<ExprId>, NativeExpressionDecodeError> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| decode_expr_at(value, path.clone().index(index), arena, input_layout))
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    use super::super::type_decode::encode_type;
    use super::*;
    use novarocks::common::ids::SlotId;
    use novarocks::exec::chunk::Chunk;
    use novarocks::exec::expr::{ExprArena, ExprNode, LiteralValue, function::FunctionKind};
    use novarocks_protocol::{common, expr};
    use novarocks_types::logical::{LogicalType, field_with_logical_type};

    pub(crate) fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    pub(crate) fn scalar_expr(data_type: DataType, kind: expr::expr::Kind) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(kind),
        }
    }

    pub(crate) fn int_lit(value: i64) -> expr::Expr {
        scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            }),
        )
    }

    pub(crate) fn string_lit(value: &str) -> expr::Expr {
        scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            }),
        )
    }

    pub(crate) fn bool_lit(value: bool) -> expr::Expr {
        scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            }),
        )
    }

    pub(crate) fn null_lit(data_type: DataType) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            }),
        )
    }

    pub(crate) fn col(column_id: u32, data_type: DataType) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            }),
        )
    }

    pub(crate) fn map_string_json_type() -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(field_with_logical_type(
                        Field::new("value", DataType::Utf8, true),
                        LogicalType::Json,
                    )),
                ])),
                false,
            )),
            false,
        )
    }

    pub(crate) fn layout_for_slots(slots: &[u32]) -> NativeExpressionInputLayout {
        NativeExpressionInputLayout::from_slot_ids(slots.iter().copied().map(SlotId::new))
    }

    pub(crate) fn lower_with_slots(
        e: &expr::Expr,
        slots: &[u32],
    ) -> (ExprArena, novarocks::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        let id = decode_expr(e, &mut arena, &layout).expect("lower proto expr");
        (arena, id)
    }

    pub(crate) fn lower(e: &expr::Expr) -> (ExprArena, novarocks::exec::expr::ExprId) {
        lower_with_slots(e, &[1, 7, 42])
    }

    pub(crate) fn lower_err_with_slots(e: &expr::Expr, slots: &[u32]) -> String {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        decode_expr(e, &mut arena, &layout).unwrap_err().to_string()
    }

    pub(crate) fn make_i64_chunk(slot: SlotId, values: Vec<Option<i64>>) -> Chunk {
        let field = Field::new("c0", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let chunk_schema = novarocks::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[slot],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn lowers_column_ref_to_slot_id_with_decoded_type() {
        let (arena, id) = lower(&col(42, DataType::Int32));

        assert!(matches!(
            arena.node(id),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(42)
        ));
        assert_eq!(arena.data_type(id), Some(&DataType::Int32));
    }

    #[test]
    fn column_ref_missing_from_layout_fails() {
        let err = lower_err_with_slots(&col(42, DataType::Int32), &[7]);

        assert!(err.contains("ColumnRef column_id=42 not found in input layout"));
    }

    #[test]
    fn lowers_recursive_binary_cast_and_function_call() {
        let add = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(col(7, DataType::Int64))),
                right: Some(Box::new(int_lit(5))),
            })),
        );
        let cast = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(add)),
                target: Some(type_desc(&DataType::Utf8)),
            })),
        );
        let call = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "upper".to_string(),
                args: vec![cast],
                distinct: false,
            }),
        );

        let (arena, id) = lower(&call);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected function call");
        };
        assert_eq!(*kind, FunctionKind::Upper);
        assert_eq!(args.len(), 1);
        let Some(ExprNode::Cast(add_id)) = arena.node(args[0]) else {
            panic!("expected cast arg");
        };
        assert!(matches!(
            arena.node(*add_id),
            Some(ExprNode::Add(left, right))
                if matches!(arena.node(*left), Some(ExprNode::SlotId(SlotId(7))))
                    && matches!(arena.node(*right), Some(ExprNode::Literal(LiteralValue::Int64(5))))
        ));
    }

    #[test]
    fn lowers_case_in_like_and_nested() {
        let in_pred = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::InList(Box::new(expr::InListExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                list: vec![string_lit("a"), string_lit("b")],
                negated: false,
            })),
        );
        let like = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Like(Box::new(expr::LikeExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                pattern: Some(Box::new(string_lit("x%"))),
                negated: false,
            })),
        );
        let case_expr = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Nested(Box::new(expr::NestedExpr {
                inner: Some(Box::new(scalar_expr(
                    DataType::Utf8,
                    expr::expr::Kind::CaseExpr(Box::new(expr::CaseExpr {
                        operand: None,
                        when_then: vec![
                            expr::WhenThen {
                                when: Some(in_pred),
                                then: Some(string_lit("in")),
                            },
                            expr::WhenThen {
                                when: Some(like),
                                then: Some(string_lit("like")),
                            },
                        ],
                        else_expr: Some(Box::new(string_lit("miss"))),
                    })),
                ))),
            })),
        );

        let (arena, id) = lower(&case_expr);
        let Some(ExprNode::Case {
            has_case_expr,
            has_else_expr,
            children,
        }) = arena.node(id)
        else {
            panic!("expected CASE after nested lowering");
        };
        assert!(!has_case_expr);
        assert!(has_else_expr);
        assert_eq!(children.len(), 5);
        assert!(matches!(arena.node(children[0]), Some(ExprNode::In { .. })));
        assert!(matches!(
            arena.node(children[2]),
            Some(ExprNode::FunctionCall {
                kind: FunctionKind::Like,
                ..
            })
        ));
    }

    #[test]
    fn fails_fast_for_aggregate_and_window_calls() {
        let aggregate = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::AggregateCall(expr::AggregateCall {
                function_name: "count".to_string(),
                args: vec![int_lit(1)],
                distinct: false,
                order_by: vec![],
            }),
        );
        let window = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::WindowCall(expr::WindowCall {
                function_name: "rank".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
                ignore_nulls: false,
            }),
        );

        for (expr, needle) in [(aggregate, "AggregateCall"), (window, "WindowCall")] {
            let mut arena = ExprArena::default();
            let layout = layout_for_slots(&[]);
            let err = decode_expr(&expr, &mut arena, &layout).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }
}
