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

//! Immutable expression facts shared by local-program instances.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use novarocks_types::logical::LogicalType;

/// Matches the native-v1 expanded-expression budget. This bounds the arena
/// independently of the encoded descriptor's byte budget.
pub const MAX_STATIC_EXPRESSIONS: usize = 262_144;
pub const MAX_STATIC_EXPRESSION_DYNAMIC_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_STATIC_EXPRESSION_DEPTH: usize = 96;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramExprId(usize);

impl ProgramExprId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

/// A literal has no Chunk accounting or provider output lease.
#[derive(Clone, Debug)]
pub enum StaticLiteral {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Utf8(Arc<str>),
    Binary(Arc<[u8]>),
    Date32(i32),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        value: i256,
        precision: u8,
        scale: i8,
    },
}

impl StaticLiteral {
    fn dynamic_bytes(&self) -> usize {
        match self {
            Self::Utf8(value) => value.len(),
            Self::Binary(value) => value.len(),
            _ => 0,
        }
    }
}

/// The closed family matches the current Execution function dispatch. Names
/// inside a family are resolved by the kernel ABI before a program is run.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StaticFunctionKind {
    ArrayMap,
    Substring,
    Like,
    Upper,
    Split,
    Year,
    AssertTrue,
    If,
    IfNull,
    Coalesce,
    IsNull,
    IsNotNull,
    Abs,
    Round,
    Date(&'static str),
    Array(&'static str),
    Map(&'static str),
    StructFn(&'static str),
    Math(&'static str),
    String(&'static str),
    Bit(&'static str),
    Matching(&'static str),
    Encryption(&'static str),
    Variant(&'static str),
    Object(&'static str),
    MvState(&'static str),
    NullIf,
    IcebergTransformIdentity,
    IcebergTransformVoid,
    IcebergTransformYear,
    IcebergTransformMonth,
    IcebergTransformDay,
    IcebergTransformHour,
    IcebergTransformBucket,
    IcebergTransformTruncate,
}

#[derive(Clone, Debug)]
pub enum StaticExprKind {
    Literal(StaticLiteral),
    SlotId(SlotId),
    ArrayExpr {
        elements: Vec<ProgramExprId>,
    },
    StructExpr {
        fields: Vec<ProgramExprId>,
    },
    LambdaFunction {
        body: ProgramExprId,
        arg_slots: Vec<SlotId>,
        common_sub_exprs: Vec<(SlotId, ProgramExprId)>,
        is_nondeterministic: bool,
    },
    DictDecode {
        child: ProgramExprId,
        dict: Arc<HashMap<i32, Vec<u8>>>,
    },
    Cast(ProgramExprId, DecimalOverflowPolicy),
    CastTime(ProgramExprId, DecimalOverflowPolicy),
    CastTimeFromDatetime(ProgramExprId, DecimalOverflowPolicy),
    Add(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Sub(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Mul(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Div(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Mod(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Eq(ProgramExprId, ProgramExprId),
    EqForNull(ProgramExprId, ProgramExprId),
    Ne(ProgramExprId, ProgramExprId),
    Lt(ProgramExprId, ProgramExprId),
    Le(ProgramExprId, ProgramExprId),
    Gt(ProgramExprId, ProgramExprId),
    Ge(ProgramExprId, ProgramExprId),
    And(ProgramExprId, ProgramExprId),
    Or(ProgramExprId, ProgramExprId),
    Not(ProgramExprId),
    IsNull(ProgramExprId),
    IsNotNull(ProgramExprId),
    In {
        child: ProgramExprId,
        values: Vec<ProgramExprId>,
        is_not_in: bool,
    },
    Case {
        has_case_expr: bool,
        has_else_expr: bool,
        children: Vec<ProgramExprId>,
    },
    FunctionCall {
        kind: StaticFunctionKind,
        args: Vec<ProgramExprId>,
    },
    Clone(ProgramExprId),
}

impl StaticExprKind {
    pub fn decimal_overflow_policy(&self) -> Option<DecimalOverflowPolicy> {
        match self {
            Self::Cast(_, policy)
            | Self::CastTime(_, policy)
            | Self::CastTimeFromDatetime(_, policy)
            | Self::Add(_, _, policy)
            | Self::Sub(_, _, policy)
            | Self::Mul(_, _, policy)
            | Self::Div(_, _, policy)
            | Self::Mod(_, _, policy) => Some(*policy),
            _ => None,
        }
    }

    fn references(&self) -> Vec<ProgramExprId> {
        match self {
            Self::Literal(_) | Self::SlotId(_) => Vec::new(),
            Self::ArrayExpr { elements } | Self::StructExpr { fields: elements } => {
                elements.clone()
            }
            Self::LambdaFunction {
                body,
                common_sub_exprs,
                ..
            } => std::iter::once(*body)
                .chain(common_sub_exprs.iter().map(|(_, id)| *id))
                .collect(),
            Self::DictDecode { child, .. }
            | Self::Cast(child, _)
            | Self::CastTime(child, _)
            | Self::CastTimeFromDatetime(child, _)
            | Self::Not(child)
            | Self::IsNull(child)
            | Self::IsNotNull(child)
            | Self::Clone(child) => vec![*child],
            Self::Add(a, b, _)
            | Self::Sub(a, b, _)
            | Self::Mul(a, b, _)
            | Self::Div(a, b, _)
            | Self::Mod(a, b, _)
            | Self::Eq(a, b)
            | Self::EqForNull(a, b)
            | Self::Ne(a, b)
            | Self::Lt(a, b)
            | Self::Le(a, b)
            | Self::Gt(a, b)
            | Self::Ge(a, b)
            | Self::And(a, b)
            | Self::Or(a, b) => vec![*a, *b],
            Self::In { child, values, .. } => std::iter::once(*child)
                .chain(values.iter().copied())
                .collect(),
            Self::Case { children, .. } => children.clone(),
            Self::FunctionCall { args, .. } => args.clone(),
        }
    }
}

/// Pure nested field semantics formerly attached to a mutable Chunk schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticFieldSchema {
    logical_type: Option<LogicalType>,
    children: Arc<[StaticFieldSchema]>,
}

impl StaticFieldSchema {
    pub fn new(logical_type: Option<LogicalType>, children: Vec<Self>) -> Self {
        Self {
            logical_type,
            children: Arc::from(children),
        }
    }

    pub const fn logical_type(&self) -> Option<LogicalType> {
        self.logical_type
    }

    pub fn children(&self) -> &[Self] {
        &self.children
    }
}

#[derive(Clone, Debug)]
pub struct StaticExprNode {
    kind: StaticExprKind,
    data_type: DataType,
    field_schema: Option<StaticFieldSchema>,
}

impl StaticExprNode {
    pub fn new(
        kind: StaticExprKind,
        data_type: DataType,
        field_schema: Option<StaticFieldSchema>,
    ) -> Self {
        Self {
            kind,
            data_type,
            field_schema,
        }
    }

    pub const fn kind(&self) -> &StaticExprKind {
        &self.kind
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn field_schema(&self) -> Option<&StaticFieldSchema> {
        self.field_schema.as_ref()
    }
}

#[derive(Clone, Debug)]
pub struct ImmutableExpressions {
    nodes: Arc<[StaticExprNode]>,
    allow_throw_exception: bool,
    query_global_dicts: Arc<HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>>,
    session_time_zone: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticExpressionError {
    RuntimeBoundArena,
    TooManyNodes,
    InvalidReference,
    TooDeep,
    TooManyBytes,
    DuplicateLambdaSlot,
    InvalidMetadataArity,
    UnsupportedDecimalCastPolicy,
}

impl fmt::Display for StaticExpressionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static expression graph: {self:?}")
    }
}

impl std::error::Error for StaticExpressionError {}

impl ImmutableExpressions {
    pub fn try_new(
        nodes: Vec<StaticExprNode>,
        allow_throw_exception: bool,
        query_global_dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>,
        session_time_zone: Option<Arc<str>>,
    ) -> Result<Self, StaticExpressionError> {
        if nodes.len() > MAX_STATIC_EXPRESSIONS {
            return Err(StaticExpressionError::TooManyNodes);
        }
        let mut depths = Vec::with_capacity(nodes.len());
        let mut dynamic_bytes = session_time_zone.as_ref().map_or(0, |zone| zone.len());
        let mut charged_dicts = HashSet::new();
        for (index, node) in nodes.iter().enumerate() {
            let mut depth = 1_usize;
            for child in node.kind.references() {
                if child.index() >= index {
                    return Err(StaticExpressionError::InvalidReference);
                }
                depth = depth.max(depths[child.index()] + 1);
            }
            if let StaticExprKind::Cast(child, policy)
            | StaticExprKind::CastTime(child, policy)
            | StaticExprKind::CastTimeFromDatetime(child, policy) = &node.kind
                && !novarocks_type_contract::decimal_error_policy_cast_supported(
                    &nodes[child.index()].data_type,
                    &node.data_type,
                    *policy,
                )
            {
                return Err(StaticExpressionError::UnsupportedDecimalCastPolicy);
            }
            if depth > MAX_STATIC_EXPRESSION_DEPTH {
                return Err(StaticExpressionError::TooDeep);
            }
            depths.push(depth);
            if let StaticExprKind::Literal(literal) = &node.kind {
                dynamic_bytes = dynamic_bytes
                    .checked_add(literal.dynamic_bytes())
                    .ok_or(StaticExpressionError::TooManyBytes)?;
            }
            if let StaticExprKind::DictDecode { dict, .. } = &node.kind
                && charged_dicts.insert(Arc::as_ptr(dict))
            {
                for value in dict.values() {
                    dynamic_bytes = dynamic_bytes
                        .checked_add(value.len())
                        .ok_or(StaticExpressionError::TooManyBytes)?;
                }
            }
            if let StaticExprKind::LambdaFunction {
                arg_slots,
                common_sub_exprs,
                ..
            } = &node.kind
            {
                let mut slots = BTreeSet::new();
                if arg_slots
                    .iter()
                    .chain(common_sub_exprs.iter().map(|(slot, _)| slot))
                    .any(|slot| !slots.insert(*slot))
                {
                    return Err(StaticExpressionError::DuplicateLambdaSlot);
                }
            }
            if dynamic_bytes > MAX_STATIC_EXPRESSION_DYNAMIC_BYTES {
                return Err(StaticExpressionError::TooManyBytes);
            }
        }
        for dict in query_global_dicts.values() {
            if charged_dicts.insert(Arc::as_ptr(dict)) {
                for value in dict.values() {
                    dynamic_bytes = dynamic_bytes
                        .checked_add(value.len())
                        .ok_or(StaticExpressionError::TooManyBytes)?;
                    if dynamic_bytes > MAX_STATIC_EXPRESSION_DYNAMIC_BYTES {
                        return Err(StaticExpressionError::TooManyBytes);
                    }
                }
            }
        }
        Ok(Self {
            nodes: Arc::from(nodes),
            allow_throw_exception,
            query_global_dicts: Arc::new(query_global_dicts),
            session_time_zone,
        })
    }

    pub fn nodes(&self) -> &[StaticExprNode] {
        &self.nodes
    }

    pub fn node(&self, id: ProgramExprId) -> Option<&StaticExprNode> {
        self.nodes.get(id.index())
    }

    pub const fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }

    pub fn query_global_dict(&self, slot: SlotId) -> Option<&Arc<HashMap<i32, Vec<u8>>>> {
        self.query_global_dicts.get(&slot)
    }

    pub fn query_global_dicts(&self) -> &HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>> {
        &self.query_global_dicts
    }

    pub fn session_time_zone(&self) -> Option<&str> {
        self.session_time_zone.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_forward_references_and_cycles_before_freeze() {
        let nodes = vec![StaticExprNode::new(
            StaticExprKind::Not(ProgramExprId::new(0)),
            DataType::Boolean,
            None,
        )];
        assert!(matches!(
            ImmutableExpressions::try_new(nodes, false, HashMap::new(), None),
            Err(StaticExpressionError::InvalidReference)
        ));
    }

    #[test]
    fn shares_frozen_dict_backing_without_rebuilding() {
        let dict = Arc::new(HashMap::from([(1, vec![b'a'])]));
        let nodes = vec![
            StaticExprNode::new(
                StaticExprKind::SlotId(SlotId::new(1)),
                DataType::Int32,
                None,
            ),
            StaticExprNode::new(
                StaticExprKind::DictDecode {
                    child: ProgramExprId::new(0),
                    dict: Arc::clone(&dict),
                },
                DataType::Utf8,
                None,
            ),
        ];
        let expressions = ImmutableExpressions::try_new(nodes, false, HashMap::new(), None)
            .expect("valid expressions");
        let cloned = expressions.clone();
        assert!(Arc::ptr_eq(&expressions.nodes, &cloned.nodes));
        let StaticExprKind::DictDecode { dict: stored, .. } = cloned.nodes()[1].kind() else {
            panic!("expected dict decode")
        };
        assert!(Arc::ptr_eq(&dict, stored));
    }
    #[test]
    fn static_binding_rejects_nested_reporting_decimal_casts() {
        use arrow_schema::{Field, Fields};
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        fn containers(decimal: DataType) -> Vec<DataType> {
            let field = Arc::new(Field::new("element", decimal.clone(), true));
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", decimal.clone(), true),
                ])),
                false,
            );
            vec![
                DataType::List(field.clone()),
                DataType::LargeList(field.clone()),
                DataType::FixedSizeList(field, 2),
                DataType::Struct(Fields::from(vec![Field::new("value", decimal, true)])),
                DataType::Map(Arc::new(entries), false),
            ]
        }
        for (source, target) in containers(DataType::Decimal128(10, 2))
            .into_iter()
            .zip(containers(DataType::Decimal128(12, 3)))
        {
            for (result_type, policy, accepted) in [
                (target.clone(), Policy::ReportError, false),
                (target, Policy::OutputNull, true),
                (source.clone(), Policy::ReportError, true),
            ] {
                let result = ImmutableExpressions::try_new(
                    vec![
                        StaticExprNode::new(
                            StaticExprKind::SlotId(SlotId::new(1)),
                            source.clone(),
                            None,
                        ),
                        StaticExprNode::new(
                            StaticExprKind::Cast(ProgramExprId::new(0), policy),
                            result_type,
                            None,
                        ),
                    ],
                    false,
                    HashMap::new(),
                    None,
                );
                if accepted {
                    result.unwrap();
                } else {
                    assert!(matches!(
                        result,
                        Err(StaticExpressionError::UnsupportedDecimalCastPolicy)
                    ));
                }
            }
        }
    }
}
