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

//! Projection between the decoder arena and the shared static expression ABI.

use std::sync::Arc;

use novarocks_local_program::{
    ImmutableExpressions, ProgramExprId, StaticExprKind, StaticExprNode, StaticExpressionError,
    StaticFieldSchema, StaticFunctionKind, StaticLiteral,
};

use super::function::FunctionKind;
use super::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::exec::chunk::ChunkFieldSchema;

impl ExprArena {
    /// Consume the decoder-owned arena once. Static nodes and dictionaries are
    /// frozen before any Task or driver state is created.
    pub fn into_immutable(self) -> Result<ImmutableExpressions, StaticExpressionError> {
        if self.runtime_error.is_some() {
            return Err(StaticExpressionError::RuntimeBoundArena);
        }
        let ExprArena {
            nodes,
            types,
            field_schemas,
            allow_throw_exception,
            query_global_dicts,
            session_time_zone,
            runtime_error: _,
        } = self;
        if nodes.len() != types.len() || nodes.len() != field_schemas.len() {
            return Err(StaticExpressionError::InvalidMetadataArity);
        }
        let mut frozen = Vec::with_capacity(nodes.len());
        for ((node, data_type), field_schema) in nodes.into_iter().zip(types).zip(field_schemas) {
            frozen.push(StaticExprNode::new(
                freeze_kind(node),
                data_type,
                field_schema.map(freeze_field_schema),
            ));
        }
        ImmutableExpressions::try_new(
            frozen,
            allow_throw_exception,
            query_global_dicts,
            session_time_zone.map(Arc::from),
        )
    }

    /// Build the existing expression kernel ABI for one LocalProgram instance.
    /// The source remains the only retained static expression graph; dictionary
    /// bytes stay behind shared Arcs.
    pub(crate) fn from_immutable(expressions: &ImmutableExpressions) -> Self {
        let mut arena = Self::default();
        arena.set_allow_throw_exception(expressions.allow_throw_exception());
        arena.set_query_global_dicts(expressions.query_global_dicts().clone());
        arena.set_session_time_zone(expressions.session_time_zone().map(str::to_owned));
        for node in expressions.nodes() {
            let id = arena.push_typed(thaw_kind(node.kind()), node.data_type().clone());
            if let Some(schema) = node.field_schema() {
                arena.set_field_schema(id, thaw_field_schema(schema));
            }
        }
        arena
    }
}

pub(crate) fn freeze_field_schema(schema: ChunkFieldSchema) -> StaticFieldSchema {
    StaticFieldSchema::new(
        schema.logical_type(),
        schema
            .children()
            .iter()
            .cloned()
            .map(freeze_field_schema)
            .collect(),
    )
}

pub(crate) fn thaw_field_schema(schema: &StaticFieldSchema) -> ChunkFieldSchema {
    ChunkFieldSchema::new(
        schema.logical_type(),
        schema.children().iter().map(thaw_field_schema).collect(),
    )
}

fn old_id(id: ProgramExprId) -> ExprId {
    ExprId(id.index())
}

fn thaw_kind(kind: &StaticExprKind) -> ExprNode {
    use StaticExprKind as Static;
    match kind {
        Static::Literal(value) => ExprNode::Literal(thaw_literal(value)),
        Static::SlotId(slot) => ExprNode::SlotId(*slot),
        Static::ArrayExpr { elements } => ExprNode::ArrayExpr {
            elements: elements.iter().copied().map(old_id).collect(),
        },
        Static::StructExpr { fields } => ExprNode::StructExpr {
            fields: fields.iter().copied().map(old_id).collect(),
        },
        Static::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
        } => ExprNode::LambdaFunction {
            body: old_id(*body),
            arg_slots: arg_slots.clone(),
            common_sub_exprs: common_sub_exprs
                .iter()
                .map(|(slot, expr)| (*slot, old_id(*expr)))
                .collect(),
            is_nondeterministic: *is_nondeterministic,
        },
        Static::DictDecode { child, dict } => ExprNode::DictDecode {
            child: old_id(*child),
            dict: Arc::clone(dict),
        },
        Static::Cast(child) => ExprNode::Cast(old_id(*child)),
        Static::CastTime(child) => ExprNode::CastTime(old_id(*child)),
        Static::CastTimeFromDatetime(child) => ExprNode::CastTimeFromDatetime(old_id(*child)),
        Static::Add(a, b) => ExprNode::Add(old_id(*a), old_id(*b)),
        Static::Sub(a, b) => ExprNode::Sub(old_id(*a), old_id(*b)),
        Static::Mul(a, b) => ExprNode::Mul(old_id(*a), old_id(*b)),
        Static::Div(a, b) => ExprNode::Div(old_id(*a), old_id(*b)),
        Static::Mod(a, b) => ExprNode::Mod(old_id(*a), old_id(*b)),
        Static::Eq(a, b) => ExprNode::Eq(old_id(*a), old_id(*b)),
        Static::EqForNull(a, b) => ExprNode::EqForNull(old_id(*a), old_id(*b)),
        Static::Ne(a, b) => ExprNode::Ne(old_id(*a), old_id(*b)),
        Static::Lt(a, b) => ExprNode::Lt(old_id(*a), old_id(*b)),
        Static::Le(a, b) => ExprNode::Le(old_id(*a), old_id(*b)),
        Static::Gt(a, b) => ExprNode::Gt(old_id(*a), old_id(*b)),
        Static::Ge(a, b) => ExprNode::Ge(old_id(*a), old_id(*b)),
        Static::And(a, b) => ExprNode::And(old_id(*a), old_id(*b)),
        Static::Or(a, b) => ExprNode::Or(old_id(*a), old_id(*b)),
        Static::Not(child) => ExprNode::Not(old_id(*child)),
        Static::IsNull(child) => ExprNode::IsNull(old_id(*child)),
        Static::IsNotNull(child) => ExprNode::IsNotNull(old_id(*child)),
        Static::In {
            child,
            values,
            is_not_in,
        } => ExprNode::In {
            child: old_id(*child),
            values: values.iter().copied().map(old_id).collect(),
            is_not_in: *is_not_in,
        },
        Static::Case {
            has_case_expr,
            has_else_expr,
            children,
        } => ExprNode::Case {
            has_case_expr: *has_case_expr,
            has_else_expr: *has_else_expr,
            children: children.iter().copied().map(old_id).collect(),
        },
        Static::FunctionCall { kind, args } => ExprNode::FunctionCall {
            kind: thaw_function(*kind),
            args: args.iter().copied().map(old_id).collect(),
        },
        Static::Clone(child) => ExprNode::Clone(old_id(*child)),
    }
}

fn thaw_literal(value: &StaticLiteral) -> LiteralValue {
    match value {
        StaticLiteral::Null => LiteralValue::Null,
        StaticLiteral::Int8(value) => LiteralValue::Int8(*value),
        StaticLiteral::Int16(value) => LiteralValue::Int16(*value),
        StaticLiteral::Int32(value) => LiteralValue::Int32(*value),
        StaticLiteral::Int64(value) => LiteralValue::Int64(*value),
        StaticLiteral::LargeInt(value) => LiteralValue::LargeInt(*value),
        StaticLiteral::Float32(value) => LiteralValue::Float32(*value),
        StaticLiteral::Float64(value) => LiteralValue::Float64(*value),
        StaticLiteral::Bool(value) => LiteralValue::Bool(*value),
        StaticLiteral::Utf8(value) => LiteralValue::Utf8(value.to_string()),
        StaticLiteral::Binary(value) => LiteralValue::Binary(value.to_vec()),
        StaticLiteral::Date32(value) => LiteralValue::Date32(*value),
        StaticLiteral::Decimal128 {
            value,
            precision,
            scale,
        } => LiteralValue::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        },
        StaticLiteral::Decimal256 {
            value,
            precision,
            scale,
        } => LiteralValue::Decimal256 {
            value: *value,
            precision: *precision,
            scale: *scale,
        },
    }
}

fn thaw_function(kind: StaticFunctionKind) -> FunctionKind {
    match kind {
        StaticFunctionKind::ArrayMap => FunctionKind::ArrayMap,
        StaticFunctionKind::Substring => FunctionKind::Substring,
        StaticFunctionKind::Like => FunctionKind::Like,
        StaticFunctionKind::Upper => FunctionKind::Upper,
        StaticFunctionKind::Split => FunctionKind::Split,
        StaticFunctionKind::Year => FunctionKind::Year,
        StaticFunctionKind::AssertTrue => FunctionKind::AssertTrue,
        StaticFunctionKind::If => FunctionKind::If,
        StaticFunctionKind::IfNull => FunctionKind::IfNull,
        StaticFunctionKind::Coalesce => FunctionKind::Coalesce,
        StaticFunctionKind::IsNull => FunctionKind::IsNull,
        StaticFunctionKind::IsNotNull => FunctionKind::IsNotNull,
        StaticFunctionKind::Abs => FunctionKind::Abs,
        StaticFunctionKind::Round => FunctionKind::Round,
        StaticFunctionKind::Date(name) => FunctionKind::Date(name),
        StaticFunctionKind::Array(name) => FunctionKind::Array(name),
        StaticFunctionKind::Map(name) => FunctionKind::Map(name),
        StaticFunctionKind::StructFn(name) => FunctionKind::StructFn(name),
        StaticFunctionKind::Math(name) => FunctionKind::Math(name),
        StaticFunctionKind::String(name) => FunctionKind::String(name),
        StaticFunctionKind::Bit(name) => FunctionKind::Bit(name),
        StaticFunctionKind::Matching(name) => FunctionKind::Matching(name),
        StaticFunctionKind::Encryption(name) => FunctionKind::Encryption(name),
        StaticFunctionKind::Variant(name) => FunctionKind::Variant(name),
        StaticFunctionKind::Object(name) => FunctionKind::Object(name),
        StaticFunctionKind::MvState(name) => FunctionKind::MvState(name),
        StaticFunctionKind::NullIf => FunctionKind::NullIf,
        StaticFunctionKind::IcebergTransformIdentity => FunctionKind::IcebergTransformIdentity,
        StaticFunctionKind::IcebergTransformVoid => FunctionKind::IcebergTransformVoid,
        StaticFunctionKind::IcebergTransformYear => FunctionKind::IcebergTransformYear,
        StaticFunctionKind::IcebergTransformMonth => FunctionKind::IcebergTransformMonth,
        StaticFunctionKind::IcebergTransformDay => FunctionKind::IcebergTransformDay,
        StaticFunctionKind::IcebergTransformHour => FunctionKind::IcebergTransformHour,
        StaticFunctionKind::IcebergTransformBucket => FunctionKind::IcebergTransformBucket,
        StaticFunctionKind::IcebergTransformTruncate => FunctionKind::IcebergTransformTruncate,
    }
}

fn id(id: ExprId) -> ProgramExprId {
    ProgramExprId::new(id.0)
}

fn freeze_kind(node: ExprNode) -> StaticExprKind {
    match node {
        ExprNode::Literal(value) => StaticExprKind::Literal(freeze_literal(value)),
        ExprNode::SlotId(slot) => StaticExprKind::SlotId(slot),
        ExprNode::ArrayExpr { elements } => StaticExprKind::ArrayExpr {
            elements: elements.into_iter().map(id).collect(),
        },
        ExprNode::StructExpr { fields } => StaticExprKind::StructExpr {
            fields: fields.into_iter().map(id).collect(),
        },
        ExprNode::LambdaFunction {
            body,
            arg_slots,
            common_sub_exprs,
            is_nondeterministic,
        } => StaticExprKind::LambdaFunction {
            body: id(body),
            arg_slots,
            common_sub_exprs: common_sub_exprs
                .into_iter()
                .map(|(slot, expr)| (slot, id(expr)))
                .collect(),
            is_nondeterministic,
        },
        ExprNode::DictDecode { child, dict } => StaticExprKind::DictDecode {
            child: id(child),
            dict,
        },
        ExprNode::Cast(child) => StaticExprKind::Cast(id(child)),
        ExprNode::CastTime(child) => StaticExprKind::CastTime(id(child)),
        ExprNode::CastTimeFromDatetime(child) => StaticExprKind::CastTimeFromDatetime(id(child)),
        ExprNode::Add(a, b) => StaticExprKind::Add(id(a), id(b)),
        ExprNode::Sub(a, b) => StaticExprKind::Sub(id(a), id(b)),
        ExprNode::Mul(a, b) => StaticExprKind::Mul(id(a), id(b)),
        ExprNode::Div(a, b) => StaticExprKind::Div(id(a), id(b)),
        ExprNode::Mod(a, b) => StaticExprKind::Mod(id(a), id(b)),
        ExprNode::Eq(a, b) => StaticExprKind::Eq(id(a), id(b)),
        ExprNode::EqForNull(a, b) => StaticExprKind::EqForNull(id(a), id(b)),
        ExprNode::Ne(a, b) => StaticExprKind::Ne(id(a), id(b)),
        ExprNode::Lt(a, b) => StaticExprKind::Lt(id(a), id(b)),
        ExprNode::Le(a, b) => StaticExprKind::Le(id(a), id(b)),
        ExprNode::Gt(a, b) => StaticExprKind::Gt(id(a), id(b)),
        ExprNode::Ge(a, b) => StaticExprKind::Ge(id(a), id(b)),
        ExprNode::And(a, b) => StaticExprKind::And(id(a), id(b)),
        ExprNode::Or(a, b) => StaticExprKind::Or(id(a), id(b)),
        ExprNode::Not(child) => StaticExprKind::Not(id(child)),
        ExprNode::IsNull(child) => StaticExprKind::IsNull(id(child)),
        ExprNode::IsNotNull(child) => StaticExprKind::IsNotNull(id(child)),
        ExprNode::In {
            child,
            values,
            is_not_in,
        } => StaticExprKind::In {
            child: id(child),
            values: values.into_iter().map(id).collect(),
            is_not_in,
        },
        ExprNode::Case {
            has_case_expr,
            has_else_expr,
            children,
        } => StaticExprKind::Case {
            has_case_expr,
            has_else_expr,
            children: children.into_iter().map(id).collect(),
        },
        ExprNode::FunctionCall { kind, args } => StaticExprKind::FunctionCall {
            kind: freeze_function(kind),
            args: args.into_iter().map(id).collect(),
        },
        ExprNode::Clone(child) => StaticExprKind::Clone(id(child)),
    }
}

fn freeze_literal(value: LiteralValue) -> StaticLiteral {
    match value {
        LiteralValue::Null => StaticLiteral::Null,
        LiteralValue::Int8(value) => StaticLiteral::Int8(value),
        LiteralValue::Int16(value) => StaticLiteral::Int16(value),
        LiteralValue::Int32(value) => StaticLiteral::Int32(value),
        LiteralValue::Int64(value) => StaticLiteral::Int64(value),
        LiteralValue::LargeInt(value) => StaticLiteral::LargeInt(value),
        LiteralValue::Float32(value) => StaticLiteral::Float32(value),
        LiteralValue::Float64(value) => StaticLiteral::Float64(value),
        LiteralValue::Bool(value) => StaticLiteral::Bool(value),
        LiteralValue::Utf8(value) => StaticLiteral::Utf8(Arc::from(value)),
        LiteralValue::Binary(value) => StaticLiteral::Binary(Arc::from(value)),
        LiteralValue::Date32(value) => StaticLiteral::Date32(value),
        LiteralValue::Decimal128 {
            value,
            precision,
            scale,
        } => StaticLiteral::Decimal128 {
            value,
            precision,
            scale,
        },
        LiteralValue::Decimal256 {
            value,
            precision,
            scale,
        } => StaticLiteral::Decimal256 {
            value,
            precision,
            scale,
        },
    }
}

fn freeze_function(kind: FunctionKind) -> StaticFunctionKind {
    match kind {
        FunctionKind::ArrayMap => StaticFunctionKind::ArrayMap,
        FunctionKind::Substring => StaticFunctionKind::Substring,
        FunctionKind::Like => StaticFunctionKind::Like,
        FunctionKind::Upper => StaticFunctionKind::Upper,
        FunctionKind::Split => StaticFunctionKind::Split,
        FunctionKind::Year => StaticFunctionKind::Year,
        FunctionKind::AssertTrue => StaticFunctionKind::AssertTrue,
        FunctionKind::If => StaticFunctionKind::If,
        FunctionKind::IfNull => StaticFunctionKind::IfNull,
        FunctionKind::Coalesce => StaticFunctionKind::Coalesce,
        FunctionKind::IsNull => StaticFunctionKind::IsNull,
        FunctionKind::IsNotNull => StaticFunctionKind::IsNotNull,
        FunctionKind::Abs => StaticFunctionKind::Abs,
        FunctionKind::Round => StaticFunctionKind::Round,
        FunctionKind::Date(name) => StaticFunctionKind::Date(name),
        FunctionKind::Array(name) => StaticFunctionKind::Array(name),
        FunctionKind::Map(name) => StaticFunctionKind::Map(name),
        FunctionKind::StructFn(name) => StaticFunctionKind::StructFn(name),
        FunctionKind::Math(name) => StaticFunctionKind::Math(name),
        FunctionKind::String(name) => StaticFunctionKind::String(name),
        FunctionKind::Bit(name) => StaticFunctionKind::Bit(name),
        FunctionKind::Matching(name) => StaticFunctionKind::Matching(name),
        FunctionKind::Encryption(name) => StaticFunctionKind::Encryption(name),
        FunctionKind::Variant(name) => StaticFunctionKind::Variant(name),
        FunctionKind::Object(name) => StaticFunctionKind::Object(name),
        FunctionKind::MvState(name) => StaticFunctionKind::MvState(name),
        FunctionKind::NullIf => StaticFunctionKind::NullIf,
        FunctionKind::IcebergTransformIdentity => StaticFunctionKind::IcebergTransformIdentity,
        FunctionKind::IcebergTransformVoid => StaticFunctionKind::IcebergTransformVoid,
        FunctionKind::IcebergTransformYear => StaticFunctionKind::IcebergTransformYear,
        FunctionKind::IcebergTransformMonth => StaticFunctionKind::IcebergTransformMonth,
        FunctionKind::IcebergTransformDay => StaticFunctionKind::IcebergTransformDay,
        FunctionKind::IcebergTransformHour => StaticFunctionKind::IcebergTransformHour,
        FunctionKind::IcebergTransformBucket => StaticFunctionKind::IcebergTransformBucket,
        FunctionKind::IcebergTransformTruncate => StaticFunctionKind::IcebergTransformTruncate,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;
    use novarocks_types::SlotId;

    use super::*;

    #[test]
    fn runtime_bound_arena_cannot_be_frozen_again() {
        let mut arena = ExprArena::default();
        arena.bind_runtime_error(Arc::new(
            crate::runtime::runtime_state::RuntimeErrorState::default(),
        ));
        assert!(matches!(
            arena.into_immutable(),
            Err(StaticExpressionError::RuntimeBoundArena)
        ));
    }

    #[test]
    fn freeze_preserves_options_and_shared_dictionary_values() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int32);
        arena.push_typed(
            ExprNode::DictDecode {
                child: source,
                dict: Arc::new(HashMap::from([(1, vec![b'x'])])),
            },
            DataType::Utf8,
        );
        arena.set_allow_throw_exception(true);
        arena.set_session_time_zone(Some("Asia/Shanghai".to_string()));
        arena.set_query_global_dicts(HashMap::from([(
            SlotId::new(7),
            Arc::new(HashMap::from([(1, vec![b'x'])])),
        )]));
        let static_expressions = arena.into_immutable().expect("frozen expressions");
        assert!(static_expressions.allow_throw_exception());
        assert_eq!(
            static_expressions.session_time_zone(),
            Some("Asia/Shanghai")
        );
        assert_eq!(
            static_expressions
                .query_global_dict(SlotId::new(7))
                .unwrap()[&1]
                .as_slice(),
            b"x"
        );
        assert!(matches!(
            static_expressions
                .node(ProgramExprId::new(1))
                .unwrap()
                .kind(),
            StaticExprKind::DictDecode { .. }
        ));
    }
}
