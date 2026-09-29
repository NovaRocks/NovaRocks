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

//! Optimizer-native interned scalar expression IR (ScalarArena + ScalarId).
//!
//! Operators will reference scalar expressions by a Copy `ScalarId` handle
//! instead of owning analyzer `TypedExpr` by value, so cloning an operator /
//! memo expression is O(1). `intern` hash-conses: structurally-identical nodes
//! with identical type metadata share one id, giving id-equality == typed
//! structural-equality (the property the dedup sites and future CSE rely on).
//! M1 memo/physical operators store scalar handles; rewrite and codegen stages
//! still use the `TypedExpr` bridge during the migration.
#![allow(dead_code)] // wired into operators in M1.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use arrow::datatypes::DataType;

use crate::column_id::ColumnId;
use crate::common::{BinOp, LambdaParam, LiteralValue, UnOp, WindowFrame};
use crate::functions::FunctionVolatility;

/// `LiteralValue` is only `PartialEq` (it holds `Float(f64)` / `Decimal(String)`),
/// so it cannot be a `HashMap` key directly. This newtype provides `Eq`/`Hash`
/// by hashing/comparing floats via their bit pattern (NaN compares equal to NaN,
/// which is exactly what we want for structural dedup of identical literals).
#[derive(Clone, Debug)]
pub(crate) struct HashableLiteral(pub LiteralValue);

impl PartialEq for HashableLiteral {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (LiteralValue::Float(a), LiteralValue::Float(b)) => a.to_bits() == b.to_bits(),
            (a, b) => a == b,
        }
    }
}

impl Eq for HashableLiteral {}

impl Hash for HashableLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            LiteralValue::Null => {}
            LiteralValue::Bool(b) => b.hash(state),
            LiteralValue::Int(i) => i.hash(state),
            LiteralValue::LargeInt(i) => i.hash(state),
            LiteralValue::Float(f) => f.to_bits().hash(state),
            LiteralValue::Decimal(s) | LiteralValue::String(s) => s.hash(state),
            LiteralValue::Binary(b) => b.hash(state),
        }
    }
}

/// Copy handle into a `ScalarArena`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ScalarId(u32);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct SortKey {
    pub expr: ScalarId,
    pub asc: bool,
    pub nulls_first: bool,
    pub display: Option<ColumnDisplay>,
}

/// One scalar node. Children are referenced by `ScalarId` (never inlined), so a
/// node is cheap to hash/compare. Type metadata is part of `ScalarKey`; semantic
/// operation details that are not implied by children still belong in the node.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ScalarNode {
    ColumnRef(ColumnId),
    LambdaParamRef {
        name: String,
        slot_id: i32,
    },
    Literal(HashableLiteral),
    BinaryOp {
        op: BinOp,
        left: ScalarId,
        right: ScalarId,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    },
    UnaryOp {
        op: UnOp,
        child: ScalarId,
    },
    FunctionCall {
        name: String,
        args: Vec<ScalarId>,
        distinct: bool,
        binding: crate::binding::SqlFunctionBinding,
        volatility: FunctionVolatility,
    },
    LambdaFunction {
        params: Vec<LambdaParam>,
        body: ScalarId,
    },
    AggregateCall {
        name: String,
        args: Vec<ScalarId>,
        distinct: bool,
        order_by: Vec<SortKey>,
        resolved: crate::binding::SqlFunctionBinding,
    },
    Cast {
        child: ScalarId,
        target: DataType,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    },
    IsNull {
        child: ScalarId,
        negated: bool,
    },
    InList {
        child: ScalarId,
        list: Vec<ScalarId>,
        negated: bool,
    },
    Between {
        child: ScalarId,
        low: ScalarId,
        high: ScalarId,
        negated: bool,
    },
    Like {
        child: ScalarId,
        pattern: ScalarId,
        negated: bool,
    },
    Case {
        operand: Option<ScalarId>,
        when_then: Vec<(ScalarId, ScalarId)>,
        else_expr: Option<ScalarId>,
    },
    IsTruthValue {
        child: ScalarId,
        value: bool,
        negated: bool,
    },
    Nested(ScalarId),
    WindowCall {
        name: String,
        args: Vec<ScalarId>,
        distinct: bool,
        binding: crate::binding::SqlFunctionBinding,
        function_order_by: Vec<SortKey>,
        aggregate_binding: Option<crate::binding::SqlFunctionBinding>,
        partition_by: Vec<ScalarId>,
        order_by: Vec<SortKey>,
        window_frame: Option<WindowFrame>,
        ignore_nulls: bool,
    },
    Lambda {
        params: Vec<String>,
        body: ScalarId,
    },
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ScalarKey {
    node: ScalarNode,
    data_type: DataType,
    nullable: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColumnDisplay {
    pub qualifier: Option<String>,
    pub column: String,
}

impl ColumnDisplay {
    pub(crate) fn new(qualifier: Option<String>, column: String) -> Self {
        Self { qualifier, column }
    }

    fn is_fallback_for(&self, column_id: ColumnId) -> bool {
        self.qualifier.is_none() && self.column == format!("col{}", column_id.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ColumnDisplayPriority {
    Expression,
    ProjectOutput,
}

#[derive(Clone, Debug)]
struct StoredColumnDisplay {
    display: ColumnDisplay,
    priority: ColumnDisplayPriority,
}

/// Owns all scalar nodes for one optimize() call; interns (hash-conses) on push.
#[derive(Clone, Debug)]
pub(crate) struct ScalarArena {
    nodes: Vec<ScalarNode>,
    types: Vec<DataType>,
    nullable: Vec<bool>,
    /// Function semantics resolved at scalar interning time.  It is kept
    /// alongside the compact node representation so optimizer passes never
    /// have to carry their own name-based volatility policy.
    function_volatility: Vec<Option<FunctionVolatility>>,
    intern: HashMap<ScalarKey, ScalarId>,
    column_displays: HashMap<ColumnId, StoredColumnDisplay>,
}

impl ScalarArena {
    pub(crate) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            types: Vec::new(),
            nullable: Vec::new(),
            function_volatility: Vec::new(),
            intern: HashMap::new(),
            column_displays: HashMap::new(),
        }
    }

    /// Intern a typed node. Returns the existing id for a structurally-identical
    /// node with the same type and nullability metadata.
    pub(crate) fn intern(&mut self, node: ScalarNode, ty: DataType, nullable: bool) -> ScalarId {
        let node = Self::normalize(node);
        let key = ScalarKey {
            node: node.clone(),
            data_type: ty.clone(),
            nullable,
        };
        if let Some(&id) = self.intern.get(&key) {
            return id;
        }
        let id = ScalarId(self.nodes.len() as u32);
        let function_volatility = match &node {
            ScalarNode::FunctionCall { volatility, .. } => Some(*volatility),
            _ => None,
        };
        self.nodes.push(node);
        self.types.push(ty);
        self.nullable.push(nullable);
        self.function_volatility.push(function_volatility);
        self.intern.insert(key, id);
        id
    }

    /// Canonicalize commutative binary ops by ordering operands by ScalarId, so
    /// `a AND b` and `b AND a` intern to one id. Mirrors StarRocks
    /// normalizeChildrenGroup.
    fn normalize(node: ScalarNode) -> ScalarNode {
        if let ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy,
        } = node
        {
            let commutative = matches!(op, BinOp::And | BinOp::Or | BinOp::Eq);
            if commutative && left.0 > right.0 {
                return ScalarNode::BinaryOp {
                    op,
                    left: right,
                    right: left,
                    decimal_overflow_policy,
                };
            }
            return ScalarNode::BinaryOp {
                op,
                left,
                right,
                decimal_overflow_policy,
            };
        }
        node
    }

    pub(crate) fn node(&self, id: ScalarId) -> &ScalarNode {
        &self.nodes[id.0 as usize]
    }

    pub(crate) fn data_type(&self, id: ScalarId) -> &DataType {
        &self.types[id.0 as usize]
    }

    pub(crate) fn nullable(&self, id: ScalarId) -> bool {
        self.nullable[id.0 as usize]
    }

    /// Returns the resolved volatility for a scalar function node.
    ///
    /// Non-function nodes intentionally return `None`; callers that recurse
    /// through an expression can distinguish node-local semantics from a
    /// volatile descendant.
    pub(crate) fn function_volatility(&self, id: ScalarId) -> Option<FunctionVolatility> {
        self.function_volatility[id.0 as usize]
    }

    fn remember_column_display(
        &mut self,
        column_id: ColumnId,
        qualifier: Option<String>,
        column: String,
    ) {
        self.remember_column_display_with_priority(
            column_id,
            ColumnDisplay { qualifier, column },
            ColumnDisplayPriority::Expression,
        );
    }

    pub(crate) fn remember_source_column_display(
        &mut self,
        column_id: ColumnId,
        qualifier: Option<String>,
        column: String,
    ) {
        if column_id == ColumnId::UNSET {
            return;
        }
        self.remember_column_display(column_id, qualifier, column);
    }

    pub(crate) fn remember_project_output_display(
        &mut self,
        column_id: ColumnId,
        qualifier: Option<String>,
        column: String,
    ) {
        if column_id == ColumnId::UNSET {
            return;
        }
        self.remember_column_display_with_priority(
            column_id,
            ColumnDisplay { qualifier, column },
            ColumnDisplayPriority::ProjectOutput,
        );
    }

    pub(crate) fn remember_column_display_from_scalar(
        &mut self,
        column_id: ColumnId,
        source: ScalarId,
    ) {
        if column_id == ColumnId::UNSET {
            return;
        }
        let Some(display) = self
            .source_column_display(source)
            .filter(|display| !display.is_fallback_for(column_id))
            .cloned()
        else {
            return;
        };
        self.remember_column_display_with_priority(
            column_id,
            display,
            ColumnDisplayPriority::Expression,
        );
    }

    fn remember_column_display_with_priority(
        &mut self,
        column_id: ColumnId,
        incoming: ColumnDisplay,
        priority: ColumnDisplayPriority,
    ) {
        match self.column_displays.get_mut(&column_id) {
            Some(existing)
                if should_replace_column_display(column_id, existing, &incoming, priority) =>
            {
                *existing = StoredColumnDisplay {
                    display: incoming,
                    priority,
                };
            }
            Some(_) => {}
            None => {
                self.column_displays.insert(
                    column_id,
                    StoredColumnDisplay {
                        display: incoming,
                        priority,
                    },
                );
            }
        }
    }

    pub(crate) fn column_display(&self, column_id: ColumnId) -> Option<&ColumnDisplay> {
        self.column_displays
            .get(&column_id)
            .map(|stored| &stored.display)
    }

    fn source_column_display(&self, scalar_id: ScalarId) -> Option<&ColumnDisplay> {
        match self.node(scalar_id) {
            ScalarNode::ColumnRef(column_id) => self.column_display(*column_id),
            _ => None,
        }
    }
}

pub(crate) fn resolve_function_binding(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    arena: &ScalarArena,
    name: &str,
    args: &[ScalarId],
) -> Result<crate::binding::SqlFunctionBinding, String> {
    let arguments = args
        .iter()
        .map(|arg| function_argument(arena, *arg))
        .collect::<Vec<_>>();
    catalog
        .resolve_scalar_binding(name, &arguments)
        .map(crate::binding::SqlFunctionBinding::new)
        .map_err(|error| error.to_string())
}

fn function_argument(arena: &ScalarArena, arg: ScalarId) -> novarocks_functions::FunctionArgument {
    use novarocks_functions::{FunctionArgument, FunctionLiteral, FunctionValueType};

    match arena.node(arg) {
        ScalarNode::LambdaFunction { params, body } => FunctionArgument::Lambda {
            parameter_types: params
                .iter()
                .map(|param| FunctionValueType::new(param.data_type.clone(), param.nullable))
                .collect(),
            result_type: FunctionValueType::new(
                arena.data_type(*body).clone(),
                arena.nullable(*body),
            ),
        },
        ScalarNode::Literal(HashableLiteral(value)) => {
            let constant = match value {
                LiteralValue::Null => Some(FunctionLiteral::Null),
                LiteralValue::Bool(value) => Some(FunctionLiteral::Boolean(*value)),
                LiteralValue::Int(value) => Some(FunctionLiteral::Int64(*value)),
                LiteralValue::LargeInt(value) => Some(FunctionLiteral::LargeInt(*value)),
                LiteralValue::Float(value) => {
                    Some(FunctionLiteral::Float64Bits(value.to_bits()))
                }
                LiteralValue::Decimal(value) => match arena.data_type(arg) {
                    DataType::Decimal128(_, scale) => Some(FunctionLiteral::Decimal128(
                        crate::analysis::decimal128_literal_unscaled(value, *scale)
                            .unwrap_or_else(|message| {
                                panic!(
                                    "interned Decimal128 literal must have an exact value: {message}"
                                )
                            }),
                    )),
                    _ => None,
                },
                LiteralValue::String(value) => {
                    Some(FunctionLiteral::Utf8(value.clone().into_boxed_str()))
                }
                LiteralValue::Binary(value) => {
                    Some(FunctionLiteral::Binary(value.clone().into_boxed_slice()))
                }
            };
            FunctionArgument::Value {
                value_type: FunctionValueType::new(
                    arena.data_type(arg).clone(),
                    arena.nullable(arg),
                ),
                constant,
            }
        }
        _ => FunctionArgument::Value {
            value_type: FunctionValueType::new(arena.data_type(arg).clone(), arena.nullable(arg)),
            constant: None,
        },
    }
}

pub(crate) fn resolve_aggregate_binding(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    arena: &ScalarArena,
    name: &str,
    args: &[ScalarId],
    order_by: &[SortKey],
    trusted: bool,
) -> Result<crate::binding::SqlFunctionBinding, String> {
    let arguments = args
        .iter()
        .copied()
        .chain(order_by.iter().map(|item| item.expr))
        .map(|argument| function_argument(arena, argument))
        .collect::<Vec<_>>();
    let exact = if trusted {
        catalog.resolve_aggregate_binding_trusted(name, args.len(), &arguments)
    } else {
        catalog.resolve_aggregate_binding(name, args.len(), &arguments)
    }
    .map_err(|error| error.to_string())?;
    Ok(crate::binding::SqlFunctionBinding::new(exact))
}

#[cfg(test)]
pub(crate) fn test_function_binding(
    arena: &ScalarArena,
    name: &str,
    args: &[ScalarId],
    result_type: DataType,
    result_nullable: bool,
    volatility: novarocks_functions::FunctionVolatility,
) -> crate::binding::SqlFunctionBinding {
    use novarocks_functions::{
        FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId, FunctionKind,
        FunctionOverloadId, FunctionResultType, FunctionSemantics, FunctionValueType,
        ResolvedFunctionBinding,
    };

    let selected_arguments = args
        .iter()
        .map(|argument| match arena.node(*argument) {
            ScalarNode::LambdaFunction { params, body } => {
                novarocks_functions::FunctionArgumentType::Lambda {
                    parameter_types: params
                        .iter()
                        .map(|param| {
                            FunctionValueType::new(param.data_type.clone(), param.nullable)
                        })
                        .collect(),
                    result_type: FunctionValueType::new(
                        arena.data_type(*body).clone(),
                        arena.nullable(*body),
                    ),
                }
            }
            _ => novarocks_functions::FunctionArgumentType::Value(FunctionValueType::new(
                arena.data_type(*argument).clone(),
                arena.nullable(*argument),
            )),
        })
        .collect();
    crate::binding::SqlFunctionBinding::new(ResolvedFunctionBinding {
        function_id: FunctionId::try_new(format!("test.scalar/{name}/v1"))
            .expect("test function identity"),
        kind: FunctionKind::Scalar,
        semantics: FunctionSemantics {
            volatility,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
        },
        logical_argument_count: args.len(),
        selected: novarocks_functions::FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(format!("test.scalar/{name}/overload-v1"))
                .expect("test function overload identity"),
            argument_types: selected_arguments,
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                result_type,
                result_nullable,
            )),
            aggregate: None,
        },
    })
}

#[cfg(test)]
pub(crate) fn test_window_binding(
    arena: &ScalarArena,
    name: &str,
    args: &[ScalarId],
    result_type: DataType,
    result_nullable: bool,
) -> crate::binding::SqlFunctionBinding {
    use novarocks_functions::{FunctionId, FunctionKind, FunctionOverloadId};

    let mut binding = test_function_binding(
        arena,
        name,
        args,
        result_type,
        result_nullable,
        novarocks_functions::FunctionVolatility::Immutable,
    )
    .as_ref()
    .clone();
    binding.function_id = FunctionId::try_new(format!("test.window/{name}/v1"))
        .expect("test window function identity");
    binding.kind = FunctionKind::Window;
    binding.semantics.intrinsic_row_error =
        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated;
    binding.selected.overload =
        FunctionOverloadId::try_new(format!("test.window/{name}/overload-v1"))
            .expect("test window function overload identity");
    crate::binding::SqlFunctionBinding::new(binding)
}

#[cfg(test)]
pub(crate) fn test_table_binding(
    arena: &ScalarArena,
    name: &str,
    args: &[ScalarId],
    result_columns: &[novarocks_functions::FunctionValueType],
) -> crate::binding::SqlFunctionBinding {
    use novarocks_functions::{FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType};

    let mut binding = test_function_binding(
        arena,
        name,
        args,
        DataType::Null,
        true,
        novarocks_functions::FunctionVolatility::Immutable,
    )
    .as_ref()
    .clone();
    binding.function_id =
        FunctionId::try_new(format!("test.table/{name}/v1")).expect("test table function identity");
    binding.kind = FunctionKind::Table;
    binding.selected.overload =
        FunctionOverloadId::try_new(format!("test.table/{name}/overload-v1"))
            .expect("test table function overload identity");
    binding.selected.result_type = FunctionResultType::Relation(result_columns.into());
    crate::binding::SqlFunctionBinding::new(binding)
}

fn should_replace_column_display(
    column_id: ColumnId,
    existing: &StoredColumnDisplay,
    incoming: &ColumnDisplay,
    priority: ColumnDisplayPriority,
) -> bool {
    if incoming.is_fallback_for(column_id) {
        return false;
    }
    if existing.display.is_fallback_for(column_id) {
        return true;
    }
    if existing.priority == ColumnDisplayPriority::Expression
        && priority == ColumnDisplayPriority::ProjectOutput
    {
        let existing_full = existing
            .display
            .qualifier
            .as_deref()
            .map(|qualifier| format!("{qualifier}.{}", existing.display.column));
        return incoming.column != existing.display.column
            && existing_full.as_deref() != Some(incoming.column.as_str());
    }
    if priority > existing.priority {
        return existing.display.column != incoming.column
            || (existing.display.qualifier.is_none() && incoming.qualifier.is_some());
    }
    if priority == existing.priority {
        if priority == ColumnDisplayPriority::ProjectOutput
            && existing.display.column != incoming.column
        {
            return true;
        }
        return existing.display.column == incoming.column
            && existing.display.qualifier.is_none()
            && incoming.qualifier.is_some();
    }
    existing.priority == ColumnDisplayPriority::ProjectOutput
        && priority == ColumnDisplayPriority::Expression
        && existing.display.column == incoming.column
        && existing.display.qualifier.is_none()
        && incoming.qualifier.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BinOp, LiteralValue};
    use crate::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn int() -> DataType {
        DataType::Int64
    }

    #[test]
    fn intern_dedups_structurally_equal_nodes() {
        let mut a = ScalarArena::new();
        let c = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        let c2 = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        assert_eq!(c, c2, "same ColumnRef must intern to one id");
        let d = a.intern(ScalarNode::ColumnRef(ColumnId(2)), int(), false);
        assert_ne!(c, d, "different ColumnRef must get different ids");

        let lit = a.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(7))),
            int(),
            false,
        );
        let add1 = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left: c,
                right: lit,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            int(),
            false,
        );
        let add2 = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left: c,
                right: lit,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            int(),
            false,
        );
        assert_eq!(
            add1, add2,
            "same BinaryOp over same child ids must intern to one id"
        );
        assert_eq!(a.data_type(add1), &int());
        assert!(!a.nullable(add1));
    }

    #[test]
    fn sqlx1_function_scalar_arena_carries_catalog_volatility() {
        let mut arena = ScalarArena::new();
        let volatile_binding = test_function_binding(
            &arena,
            "curdate",
            &[],
            DataType::Date32,
            false,
            FunctionVolatility::Volatile,
        );
        let volatile = arena.intern(
            ScalarNode::FunctionCall {
                binding: volatile_binding,
                volatility: FunctionVolatility::Volatile,
                name: "curdate".to_string(),
                args: vec![],
                distinct: false,
            },
            DataType::Date32,
            false,
        );
        let immutable_binding = test_function_binding(
            &arena,
            "lower",
            &[],
            DataType::Utf8,
            false,
            FunctionVolatility::Immutable,
        );
        let immutable = arena.intern(
            ScalarNode::FunctionCall {
                binding: immutable_binding,
                volatility: FunctionVolatility::Immutable,
                name: "lower".to_string(),
                args: vec![],
                distinct: false,
            },
            DataType::Utf8,
            false,
        );

        assert_eq!(
            arena.function_volatility(volatile),
            Some(FunctionVolatility::Volatile)
        );
        assert_eq!(
            arena.function_volatility(immutable),
            Some(FunctionVolatility::Immutable)
        );
    }

    #[test]
    fn commutative_ops_normalize_to_one_id() {
        let mut a = ScalarArena::new();
        let x = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        let y = a.intern(ScalarNode::ColumnRef(ColumnId(2)), int(), false);
        let b = DataType::Boolean;
        let xy = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: x,
                right: y,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            b.clone(),
            false,
        );
        let yx = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: y,
                right: x,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            b.clone(),
            false,
        );
        assert_eq!(xy, yx, "AND must be commutative-normalized to one id");

        let sub_xy = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Sub,
                left: x,
                right: y,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            int(),
            false,
        );
        let sub_yx = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Sub,
                left: y,
                right: x,
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            int(),
            false,
        );
        assert_ne!(sub_xy, sub_yx, "Sub must NOT be normalized");
    }
}

#[cfg(test)]
mod bridge_tests {
    use super::*;
    use crate::analysis::{
        BinOp, ExprKind, LambdaParam, LiteralValue, ProjectItem, SortItem, TypedExpr, UnOp,
        WindowBound, WindowFrame, WindowFrameType,
    };
    use crate::column_id::ColumnId;
    use crate::planner::optimizer_bridge::scalar::{
        intern_project_item, intern_typed, materialize, materialize_project_item,
    };
    use arrow::datatypes::DataType;

    fn col(id: u32, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("col{id}"),
            },
            data_type: ty,
            nullable: false,
        }
    }

    fn lit_int(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn lit_int_as(v: i64, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: ty,
            nullable: false,
        }
    }

    fn eq(l: TypedExpr, r: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(l),
                op: BinOp::Eq,
                right: Box::new(r),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn typed(kind: ExprKind, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind,
            data_type,
            nullable,
        }
    }

    fn lit_bool(v: bool) -> TypedExpr {
        typed(
            ExprKind::Literal(LiteralValue::Bool(v)),
            DataType::Boolean,
            false,
        )
    }

    fn lit_string(v: &str) -> TypedExpr {
        typed(
            ExprKind::Literal(LiteralValue::String(v.to_string())),
            DataType::Utf8,
            false,
        )
    }

    fn sort(expr: TypedExpr, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr,
            asc,
            nulls_first,
        }
    }

    #[test]
    fn intern_typed_dedups_independent_identical_exprs() {
        let mut a = ScalarArena::new();
        // Independently constructed but structurally identical TypedExpr trees
        // must intern to the same ScalarId.
        let id1 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(5)));
        let id2 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(5)));
        assert_eq!(
            id1, id2,
            "structurally-identical TypedExprs must intern to one ScalarId"
        );

        let id3 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(6)));
        assert_ne!(id1, id3);
    }

    #[test]
    fn intern_typed_separates_literals_by_type_metadata() {
        let mut a = ScalarArena::new();
        let int8 = intern_typed(&mut a, &lit_int_as(1, DataType::Int8));
        let int64 = intern_typed(&mut a, &lit_int_as(1, DataType::Int64));

        assert_ne!(
            int8, int64,
            "same literal value with different types must not share one ScalarId"
        );
        assert_eq!(a.data_type(int8), &DataType::Int8);
        assert_eq!(a.data_type(int64), &DataType::Int64);
    }

    #[test]
    #[should_panic(expected = "ColumnId::UNSET cannot be interned")]
    fn intern_typed_rejects_unset_column_ref() {
        let mut a = ScalarArena::new();
        let expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::UNSET,
                qualifier: Some("q".into()),
                column: "name_a".into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };

        intern_typed(&mut a, &expr);
    }

    #[test]
    fn materialize_preserves_column_ref_display_metadata() {
        let mut a = ScalarArena::new();
        let expr = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(7),
                qualifier: Some("t".into()),
                column: "k".into(),
            },
            data_type: DataType::Int64,
            nullable: false,
        };

        let id = intern_typed(&mut a, &expr);
        let back = materialize(&a, id);

        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = back.kind
        else {
            panic!("expected materialized ColumnRef");
        };
        assert_eq!(column_id, ColumnId(7));
        assert_eq!(qualifier.as_deref(), Some("t"));
        assert_eq!(column, "k");
    }

    #[test]
    fn project_item_materialize_uses_item_display_before_output_alias() {
        let mut a = ScalarArena::new();
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(7),
                    qualifier: None,
                    column: "id".into(),
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            output_name: "alias_id".into(),
            output_column_id: ColumnId(7),
        };

        let scalar_item = intern_project_item(&mut a, &item);

        let general = materialize(&a, scalar_item.expr);
        let ExprKind::ColumnRef { column, .. } = general.kind else {
            panic!("expected materialized ColumnRef");
        };
        assert_eq!(column, "alias_id");

        let projected = materialize_project_item(&a, &scalar_item);
        let ExprKind::ColumnRef { column, .. } = projected.expr.kind else {
            panic!("expected materialized project ColumnRef");
        };
        assert_eq!(column, "id");
        assert_eq!(projected.output_name, "alias_id");
    }

    #[test]
    fn project_output_does_not_drop_existing_source_qualifier_for_same_name() {
        let mut a = ScalarArena::new();
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(7),
                    qualifier: Some("a".into()),
                    column: "k".into(),
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            output_name: "k".into(),
            output_column_id: ColumnId(7),
        };

        let scalar_item = intern_project_item(&mut a, &item);
        let general = materialize(&a, scalar_item.expr);
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = general.kind
        else {
            panic!("expected materialized ColumnRef");
        };
        assert_eq!(qualifier.as_deref(), Some("a"));
        assert_eq!(column, "k");
    }

    #[test]
    fn project_output_real_alias_replaces_existing_source_display_for_same_id() {
        let mut a = ScalarArena::new();
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(7),
                    qualifier: Some("a".into()),
                    column: "v".into(),
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            output_name: "av".into(),
            output_column_id: ColumnId(7),
        };

        let scalar_item = intern_project_item(&mut a, &item);
        let general = materialize(&a, scalar_item.expr);
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = general.kind
        else {
            panic!("expected materialized ColumnRef");
        };
        assert_eq!(qualifier, None);
        assert_eq!(column, "av");
    }

    #[test]
    fn project_output_source_display_name_does_not_replace_existing_source_display_for_same_id() {
        let mut a = ScalarArena::new();
        let item = ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId(7),
                    qualifier: Some("a".into()),
                    column: "k".into(),
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            output_name: "a.k".into(),
            output_column_id: ColumnId(7),
        };

        let scalar_item = intern_project_item(&mut a, &item);
        let general = materialize(&a, scalar_item.expr);
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = general.kind
        else {
            panic!("expected materialized ColumnRef");
        };
        assert_eq!(qualifier.as_deref(), Some("a"));
        assert_eq!(column, "k");

        let projected = materialize_project_item(&a, &scalar_item);
        assert_eq!(projected.output_name, "a.k");
    }

    #[test]
    fn intern_distinguishes_nullable_metadata() {
        let mut a = ScalarArena::new();
        let not_nullable = a.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            DataType::Int64,
            false,
        );
        let nullable = a.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(1))),
            DataType::Int64,
            true,
        );

        assert_ne!(
            not_nullable, nullable,
            "same node and type with different nullability must not share one ScalarId"
        );
        assert!(!a.nullable(not_nullable));
        assert!(a.nullable(nullable));
    }

    #[test]
    fn materialize_round_trips_core_variants() {
        let mut a = ScalarArena::new();
        let original = eq(col(1, DataType::Int64), lit_int(5));
        let id = intern_typed(&mut a, &original);
        let back = materialize(&a, id);
        assert_eq!(
            format!("{:?}", back),
            format!("{:?}", original),
            "intern_typed then materialize must reproduce the expression"
        );
    }

    #[test]
    fn all_variants_round_trip_and_dedup() {
        let mut a = ScalarArena::new();
        let lambda_param = LambdaParam {
            name: "x".to_string(),
            slot_id: 10,
            data_type: DataType::Int64,
            nullable: false,
        };
        let lambda_param_ref = typed(
            ExprKind::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 10,
            },
            DataType::Int64,
            false,
        );
        let lambda_function = typed(
            ExprKind::LambdaFunction {
                params: vec![lambda_param],
                body: Box::new(typed(
                    ExprKind::BinaryOp {
                        left: Box::new(lambda_param_ref.clone()),
                        op: BinOp::Add,
                        right: Box::new(lit_int(1)),
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    },
                    DataType::Int64,
                    false,
                )),
            },
            DataType::Int64,
            false,
        );
        let lambda = typed(
            ExprKind::Lambda {
                params: vec!["y".to_string()],
                body: Box::new(typed(
                    ExprKind::UnaryOp {
                        op: UnOp::Negate,
                        expr: Box::new(typed(
                            ExprKind::LambdaParamRef {
                                name: "y".to_string(),
                                slot_id: 11,
                            },
                            DataType::Int64,
                            false,
                        )),
                    },
                    DataType::Int64,
                    false,
                )),
            },
            DataType::Int64,
            false,
        );

        let function_args = vec![
            typed(
                ExprKind::Cast {
                    expr: Box::new(col(1, DataType::Int64)),
                    target: DataType::Utf8,
                    decimal_overflow_policy:
                        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                },
                DataType::Utf8,
                true,
            ),
            typed(
                ExprKind::IsNull {
                    expr: Box::new(col(2, DataType::Utf8)),
                    negated: true,
                },
                DataType::Boolean,
                false,
            ),
            typed(
                ExprKind::InList {
                    expr: Box::new(col(3, DataType::Int64)),
                    list: vec![lit_int(7), lit_int(8)],
                    negated: true,
                },
                DataType::Boolean,
                false,
            ),
            typed(
                ExprKind::Case {
                    operand: Some(Box::new(col(4, DataType::Int64))),
                    when_then: vec![
                        (lit_int(1), lit_string("one")),
                        (lit_int(2), lit_string("two")),
                    ],
                    else_expr: Some(Box::new(lit_string("other"))),
                },
                DataType::Utf8,
                true,
            ),
            typed(
                ExprKind::AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col(5, DataType::Int64)],
                    distinct: true,
                    order_by: vec![sort(col(6, DataType::Int64), false, true)],
                    resolved: crate::functions::test_resolved_aggregate(
                        "sum",
                        &[DataType::Int64],
                        true,
                    ),
                },
                DataType::Int64,
                true,
            ),
            typed(
                ExprKind::Nested(Box::new(typed(
                    ExprKind::UnaryOp {
                        op: UnOp::Not,
                        expr: Box::new(lit_bool(false)),
                    },
                    DataType::Boolean,
                    false,
                ))),
                DataType::Boolean,
                false,
            ),
            typed(
                ExprKind::Between {
                    expr: Box::new(col(7, DataType::Int64)),
                    low: Box::new(lit_int(3)),
                    high: Box::new(lit_int(9)),
                    negated: false,
                },
                DataType::Boolean,
                false,
            ),
            typed(
                ExprKind::Like {
                    expr: Box::new(col(8, DataType::Utf8)),
                    pattern: Box::new(lit_string("ab%")),
                    negated: true,
                },
                DataType::Boolean,
                false,
            ),
            typed(
                ExprKind::IsTruthValue {
                    expr: Box::new(lit_bool(true)),
                    value: true,
                    negated: true,
                },
                DataType::Boolean,
                false,
            ),
            lambda_function,
            lambda,
            lambda_param_ref,
            typed(
                ExprKind::WindowCall {
                    name: "first_value".to_string(),
                    args: vec![col(9, DataType::Int64)],
                    distinct: false,
                    binding: crate::analysis::test_window_binding(
                        "first_value",
                        &[col(9, DataType::Int64)],
                        DataType::Int64,
                        true,
                    ),
                    function_order_by: vec![],
                    aggregate_binding: None,
                    partition_by: vec![col(10, DataType::Utf8)],
                    order_by: vec![sort(col(11, DataType::Int64), true, false)],
                    window_frame: Some(WindowFrame {
                        frame_type: WindowFrameType::Rows,
                        start: WindowBound::Preceding(1),
                        end: WindowBound::CurrentRow,
                    }),
                    ignore_nulls: true,
                },
                DataType::Int64,
                true,
            ),
        ];
        let e = typed(
            ExprKind::FunctionCall {
                binding: crate::analysis::test_function_binding(
                    "combo",
                    &function_args,
                    DataType::Utf8,
                    true,
                    crate::functions::FunctionVolatility::Immutable,
                ),
                name: "combo".to_string(),
                args: function_args,
                distinct: true,
                volatility: crate::functions::FunctionVolatility::Immutable,
            },
            DataType::Utf8,
            true,
        );
        let id1 = intern_typed(&mut a, &e);
        let back = materialize(&a, id1);
        assert_eq!(
            format!("{back:?}"),
            format!("{e:?}"),
            "complex expr must round-trip"
        );
        let id2 = intern_typed(&mut a, &e);
        assert_eq!(id1, id2, "complex expr must dedup to one id");
    }
}

#[cfg(test)]
mod overflow_policy_tests {
    use super::*;
    use crate::analysis::ExprKind;
    use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};

    #[test]
    fn arithmetic_and_cast_interning_do_not_merge_different_error_policies() {
        let mut arena = ScalarArena::new();
        let child = arena.intern(
            ScalarNode::ColumnRef(ColumnId(1)),
            DataType::Decimal128(38, 0),
            true,
        );
        let mut ids = Vec::new();
        for policy in [OutputNull, ReportError] {
            ids.push(arena.intern(
                ScalarNode::BinaryOp {
                    op: BinOp::Add,
                    left: child,
                    right: child,
                    decimal_overflow_policy: policy,
                },
                DataType::Decimal128(38, 0),
                true,
            ));
            ids.push(arena.intern(
                ScalarNode::Cast {
                    child,
                    target: DataType::Decimal128(9, 0),
                    decimal_overflow_policy: policy,
                },
                DataType::Decimal128(9, 0),
                true,
            ));
        }
        assert_ne!(ids[0], ids[2]);
        assert_ne!(ids[1], ids[3]);
        for id in ids {
            let typed = materialize(&arena, id);
            assert_eq!(intern_typed(&mut arena, &typed), id);
            let expected = match arena.node(id) {
                ScalarNode::BinaryOp {
                    decimal_overflow_policy,
                    ..
                }
                | ScalarNode::Cast {
                    decimal_overflow_policy,
                    ..
                } => *decimal_overflow_policy,
                _ => panic!("arithmetic or cast"),
            };
            let actual = match typed.kind {
                ExprKind::BinaryOp {
                    decimal_overflow_policy,
                    ..
                }
                | ExprKind::Cast {
                    decimal_overflow_policy,
                    ..
                } => decimal_overflow_policy,
                _ => panic!("typed arithmetic or cast"),
            };
            assert_eq!(actual, expected);
        }
    }
}

#[cfg(test)]
mod intrinsic_binding_helper_tests {
    use super::*;

    #[test]
    fn structural_window_and_table_bindings_keep_their_row_boundaries() {
        let arena = ScalarArena::new();
        let window = test_window_binding(&arena, "row_number", &[], DataType::Int64, false);
        let table = test_table_binding(&arena, "unnest", &[], &[]);
        assert_eq!(
            window.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated
        );
        assert_eq!(
            table.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
        );
        for binding in [window, table] {
            assert!(
                binding
                    .semantics
                    .intrinsic_row_error
                    .is_valid_for_kind(binding.kind)
            );
        }
    }
}
