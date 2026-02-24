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
pub mod agg;
mod arithmetic;
mod array_expr;
mod case;
mod cast;
mod comparison;
pub mod decimal;
mod dict_decode;
pub mod function;
mod in_pred;
mod literal;
mod slot;
mod struct_expr;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use std::collections::HashMap;
use std::sync::Arc;

use self::function::FunctionKind;
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExprId(pub usize);

#[derive(Clone, Debug)]
pub enum LiteralValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Utf8(String),
    Binary(Vec<u8>),
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

#[derive(Clone, Debug)]
pub enum ExprNode {
    Literal(LiteralValue),
    /// Slot id coming from StarRocks plan/descriptor table.
    SlotId(SlotId),
    /// Array expression: builds a list from element expressions.
    ArrayExpr {
        elements: Vec<ExprId>,
    },
    /// Struct expression: builds a struct from field expressions.
    StructExpr {
        fields: Vec<ExprId>,
    },
    /// Lambda function expression used by higher-order functions (e.g. array_map).
    LambdaFunction {
        body: ExprId,
        arg_slots: Vec<SlotId>,
        common_sub_exprs: Vec<(SlotId, ExprId)>,
        is_nondeterministic: bool,
    },
    /// Decode low-cardinality dictionary codes to original string/binary values.
    DictDecode {
        child: ExprId,
        dict: Arc<HashMap<i32, Vec<u8>>>,
    },
    Cast(ExprId),
    CastTime(ExprId),
    CastTimeFromDatetime(ExprId),
    Add(ExprId, ExprId),
    Sub(ExprId, ExprId),
    Mul(ExprId, ExprId),
    Div(ExprId, ExprId),
    Mod(ExprId, ExprId),
    Eq(ExprId, ExprId),
    EqForNull(ExprId, ExprId),
    Ne(ExprId, ExprId),
    Lt(ExprId, ExprId),
    Le(ExprId, ExprId),
    Gt(ExprId, ExprId),
    Ge(ExprId, ExprId),
    And(ExprId, ExprId),
    Or(ExprId, ExprId),
    Not(ExprId),
    IsNull(ExprId),
    IsNotNull(ExprId),
    In {
        child: ExprId,
        values: Vec<ExprId>,
        is_not_in: bool,
    },
    Case {
        has_case_expr: bool,
        has_else_expr: bool,
        children: Vec<ExprId>,
    },
    FunctionCall {
        kind: FunctionKind,
        args: Vec<ExprId>,
    },
    /// Clone expression: creates a copy of the child column to avoid shared mutations.
    /// Implements Copy-On-Write semantics similar to StarRocks BE's CloneExpr.
    Clone(ExprId),
}

#[derive(Clone, Debug, Default)]
pub struct ExprArena {
    nodes: Vec<ExprNode>,
    types: Vec<DataType>,
    allow_throw_exception: bool,
    query_global_dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>,
}

impl ExprArena {
    pub fn push(&mut self, node: ExprNode) -> ExprId {
        self.push_typed(node, DataType::Null)
    }

    pub fn push_typed(&mut self, node: ExprNode, data_type: DataType) -> ExprId {
        let id = ExprId(self.nodes.len());
        self.nodes.push(node);
        self.types.push(data_type);
        id
    }

    pub fn node(&self, id: ExprId) -> Option<&ExprNode> {
        self.nodes.get(id.0)
    }

    pub fn data_type(&self, id: ExprId) -> Option<&DataType> {
        self.types.get(id.0)
    }

    pub fn set_allow_throw_exception(&mut self, allow: bool) {
        self.allow_throw_exception = allow;
    }

    pub fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }

    pub fn set_query_global_dicts(&mut self, dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>) {
        self.query_global_dicts = dicts;
    }

    pub fn query_global_dict(&self, slot_id: SlotId) -> Option<&Arc<HashMap<i32, Vec<u8>>>> {
        self.query_global_dicts.get(&slot_id)
    }

    pub fn eval(&self, id: ExprId, chunk: &Chunk) -> Result<ArrayRef, String> {
        let node = self
            .nodes
            .get(id.0)
            .ok_or_else(|| "invalid ExprId".to_string())?;
        match node {
            ExprNode::Literal(v) => {
                if matches!(v, LiteralValue::Null) {
                    let target_type = self.data_type(id).cloned().unwrap_or(DataType::Null);
                    if !matches!(target_type, DataType::Null) {
                        // StarRocks plans may materialize `NULL` directly into typed slots (e.g. `slot: NULL`)
                        // without an explicit `cast(NULL as <type>)`. We must preserve the declared slot/expr type
                        // to avoid later `concat_batches` failures on `(Null, Utf8)` / `(Null, Decimal128(..))`.
                        return Ok(new_null_array(&target_type, chunk.len()));
                    }
                }
                let mut out = literal::eval(v, chunk.len())?;
                let target_type = self.data_type(id).cloned().unwrap_or(DataType::Null);
                if !matches!(target_type, DataType::Null) && out.data_type() != &target_type {
                    out = cast::cast_with_special_rules(&out, &target_type).map_err(|e| {
                        format!(
                            "literal cast failed from {:?} to {:?}: {}",
                            out.data_type(),
                            target_type,
                            e
                        )
                    })?;
                }
                Ok(out)
            }
            ExprNode::SlotId(slot_id) => slot::eval_slot_id(*slot_id, chunk),
            ExprNode::ArrayExpr { elements } => array_expr::eval_array_expr(self, id, elements, chunk),
            ExprNode::StructExpr { fields } => {
                struct_expr::eval_struct_expr(self, id, fields, chunk)
            }
            ExprNode::LambdaFunction { .. } => Err(
                "lambda function expression can only be used as an argument to higher-order functions"
                    .to_string(),
            ),
            ExprNode::DictDecode { child, dict } => {
                dict_decode::eval_dict_decode(self, id, *child, dict, chunk)
            }
            ExprNode::Cast(child) => cast::eval(self, id, *child, chunk),
            ExprNode::CastTime(child) => cast::eval_time(self, id, *child, chunk),
            ExprNode::CastTimeFromDatetime(child) => {
                cast::eval_time_from_datetime(self, id, *child, chunk)
            }
            ExprNode::Add(a, b) => arithmetic::eval_add(self, id, *a, *b, chunk),
            ExprNode::Sub(a, b) => arithmetic::eval_sub(self, id, *a, *b, chunk),
            ExprNode::Mul(a, b) => arithmetic::eval_mul(self, id, *a, *b, chunk),
            ExprNode::Div(a, b) => arithmetic::eval_div(self, id, *a, *b, chunk),
            ExprNode::Mod(a, b) => arithmetic::eval_mod(self, id, *a, *b, chunk),
            ExprNode::Eq(a, b) => comparison::eval_eq(self, *a, *b, chunk),
            ExprNode::EqForNull(a, b) => comparison::eval_eq_for_null(self, *a, *b, chunk),
            ExprNode::Ne(a, b) => comparison::eval_ne(self, *a, *b, chunk),
            ExprNode::Lt(a, b) => comparison::eval_lt(self, *a, *b, chunk),
            ExprNode::Le(a, b) => comparison::eval_le(self, *a, *b, chunk),
            ExprNode::Gt(a, b) => comparison::eval_gt(self, *a, *b, chunk),
            ExprNode::Ge(a, b) => comparison::eval_ge(self, *a, *b, chunk),
            ExprNode::And(a, b) => comparison::eval_and(self, *a, *b, chunk),
            ExprNode::Or(a, b) => comparison::eval_or(self, *a, *b, chunk),
            ExprNode::Not(child) => comparison::eval_not(self, *child, chunk),
            ExprNode::IsNull(child) => function::eval_is_null(self, *child, chunk),
            ExprNode::IsNotNull(child) => function::eval_is_not_null(self, *child, chunk),
            ExprNode::In {
                child,
                values,
                is_not_in,
            } => in_pred::eval_in(self, *child, values, *is_not_in, chunk),
            ExprNode::Case {
                has_case_expr,
                has_else_expr,
                children,
            } => case::eval_case(self, *has_case_expr, *has_else_expr, children, chunk),
            ExprNode::FunctionCall { kind, args } => {
                let metadata = function::function_metadata(*kind);
                if args.len() < metadata.min_args || args.len() > metadata.max_args {
                    return Err(format!(
                        "{} expects {} to {} arguments, got {}",
                        metadata.name,
                        metadata.min_args,
                        metadata.max_args,
                        args.len()
                    ));
                }
                match kind {
                    FunctionKind::Abs => function::eval_abs(self, id, args[0], chunk),
                    FunctionKind::ArrayMap => function::eval_array_map(self, id, args, chunk),
                    FunctionKind::Year => function::eval_year(self, id, chunk),
                    FunctionKind::AssertTrue => function::eval_assert_true(self, args, chunk),
                    FunctionKind::Substring => {
                        let length_expr = if args.len() == 3 { Some(args[2]) } else { None };
                        function::eval_substring(self, args[0], args[1], length_expr, chunk)
                    }
                    FunctionKind::Like => function::eval_like(self, args[0], args[1], chunk),
                    FunctionKind::Upper => function::eval_upper(self, args[0], chunk),
                    FunctionKind::Split => function::eval_split(self, args[0], args[1], chunk),
                    FunctionKind::If => {
                        function::eval_if(self, id, args[0], args[1], args[2], chunk)
                    }
                    FunctionKind::IfNull => function::eval_ifnull(self, args[0], args[1], chunk),
                    FunctionKind::Coalesce => function::eval_coalesce(self, id, args, chunk),
                    FunctionKind::IsNull => function::eval_is_null(self, args[0], chunk),
                    FunctionKind::IsNotNull => function::eval_is_not_null(self, args[0], chunk),
                    FunctionKind::Round => {
                        let decimals_expr = if args.len() == 2 { Some(args[1]) } else { None };
                        function::eval_round(self, id, args[0], decimals_expr, chunk)
                    }
                    FunctionKind::Array(name) => {
                        function::eval_array_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Map(name) => {
                        function::eval_map_function(name, self, id, args, chunk)
                    }
                    FunctionKind::StructFn(name) => {
                        function::eval_struct_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Date(name) => {
                        function::eval_date_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Math(name) => {
                        function::eval_math_function(name, self, id, args, chunk)
                    }
                    FunctionKind::String(name) => {
                        function::eval_string_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Bit(name) => {
                        function::eval_bit_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Matching(name) => {
                        function::eval_matching_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Encryption(name) => {
                        function::eval_encryption_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Variant(name) => {
                        function::eval_variant_function(name, self, id, args, chunk)
                    }
                    FunctionKind::Object(name) => {
                        function::eval_object_function(name, self, id, args, chunk)
                    }
                    FunctionKind::NullIf => {
                        function::eval_nullif(self, id, args[0], args[1], chunk)
                    }
                    FunctionKind::IcebergTransformIdentity => {
                        function::eval_iceberg_identity(self, args[0], chunk)
                    }
                    FunctionKind::IcebergTransformVoid => {
                        function::eval_iceberg_void(self, args[0], chunk)
                    }
                    FunctionKind::IcebergTransformYear => {
                        function::eval_iceberg_year(self, args[0], chunk)
                    }
                    FunctionKind::IcebergTransformMonth => {
                        function::eval_iceberg_month(self, args[0], chunk)
                    }
                    FunctionKind::IcebergTransformDay => {
                        function::eval_iceberg_day(self, args[0], chunk)
                    }
                    FunctionKind::IcebergTransformHour => {
                        function::eval_iceberg_hour(self, args[0], chunk)
                    }
                    FunctionKind::IcebergTransformBucket => {
                        function::eval_iceberg_bucket(self, args[0], args[1], chunk)
                    }
                    FunctionKind::IcebergTransformTruncate => {
                        function::eval_iceberg_truncate(self, args[0], args[1], chunk)
                    }
                }
            }
            ExprNode::Clone(child) => {
                // Evaluate the child expression
                let child_array = self.eval(*child, chunk)?;

                // Implement Copy-On-Write semantics:
                // Arrow arrays are already Arc-wrapped, so cloning the ArrayRef
                // gives us a new reference with independent ownership semantics.
                // If the underlying array needs to be mutated, Arrow will perform
                // a deep copy automatically when calling mutable operations.
                //
                // This aligns with StarRocks BE's Column::mutate() behavior:
                // - If use_count == 1: zero-copy (direct return)
                // - If use_count > 1: deep copy (via Arc::make_mut or clone)
                //
                // For our purposes, simply returning a clone of the ArrayRef
                // provides the necessary COW protection.
                Ok(child_array)
            }
        }
    }
}

pub(crate) fn cast_array_to_target(
    array: &ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    cast::cast_with_special_rules(array, target_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn typed_null_literal_uses_declared_type() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);

        let field = field_with_slot_id(Field::new("x", DataType::Int32, true), SlotId(1));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let chunk = Chunk::new(batch);

        let arr = arena.eval(expr, &chunk).unwrap();
        assert_eq!(arr.data_type(), &DataType::Utf8);
        assert_eq!(arr.len(), 3);
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }
}
