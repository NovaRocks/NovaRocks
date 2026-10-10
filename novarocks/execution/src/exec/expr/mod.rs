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
pub mod compiled_program;
mod constant_eval;
pub mod decimal;
mod dict_decode;
pub mod dict_peel;
pub mod function;
mod in_pred;
#[cfg(test)]
mod legacy_calendar_add_baseline_tests;
#[cfg(test)]
mod legacy_calendar_convert_tz_baseline_tests;
#[cfg(test)]
mod legacy_calendar_duration_baseline_tests;
#[cfg(test)]
mod legacy_calendar_month_baseline_tests;
#[cfg(test)]
mod legacy_calendar_unix_baseline_tests;
#[cfg(test)]
mod legacy_coalesce_assembly_baseline_tests;
#[cfg(test)]
mod legacy_conditional_baseline_tests;
#[cfg(test)]
mod legacy_date_carrier_baseline_tests;
#[cfg(test)]
mod legacy_regexp_extract_baseline_tests;
#[cfg(test)]
mod legacy_regexp_replace_baseline_tests;
mod literal;
#[cfg(test)]
pub(crate) mod pure_differential;
mod slot;
pub(crate) mod static_program;
mod struct_expr;
pub use cast::cast_with_special_rules;

use crate::exec::chunk::Chunk;
use crate::exec::chunk::ChunkFieldSchema;
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
pub use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::SlotId;
use std::collections::HashMap;
use std::sync::Arc;

use self::function::FunctionKind;
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExprId(pub usize);

pub use novarocks_functions::legacy_literal::LegacyLiteralValue as LiteralValue;

#[derive(Clone, Debug)]
pub enum ExprNode {
    Literal(LiteralValue),
    /// An exact selected value in an immutable, checked typed constant pool.
    Constant(novarocks_functions::ConstantValue),
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
    Cast(ExprId, DecimalOverflowPolicy),
    CastTime(ExprId, DecimalOverflowPolicy),
    CastTimeFromDatetime(ExprId, DecimalOverflowPolicy),
    Add(ExprId, ExprId, DecimalOverflowPolicy),
    Sub(ExprId, ExprId, DecimalOverflowPolicy),
    Mul(ExprId, ExprId, DecimalOverflowPolicy),
    Div(ExprId, ExprId, DecimalOverflowPolicy),
    Mod(ExprId, ExprId, DecimalOverflowPolicy),
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
    field_schemas: Vec<Option<ChunkFieldSchema>>,
    allow_throw_exception: bool,
    query_global_dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>,
    session_time_zone: Option<String>,
    runtime_error: Option<Arc<crate::runtime::runtime_state::RuntimeErrorState>>,
}

impl ExprArena {
    /// Inspect an immutable expression fact; arithmetic kernels receive it
    /// directly from their node and never consult query-wide throw settings.
    pub fn decimal_overflow_policy(&self, id: ExprId) -> Option<DecimalOverflowPolicy> {
        match self.node(id)? {
            ExprNode::Cast(_, policy)
            | ExprNode::CastTime(_, policy)
            | ExprNode::CastTimeFromDatetime(_, policy)
            | ExprNode::Add(_, _, policy)
            | ExprNode::Sub(_, _, policy)
            | ExprNode::Mul(_, _, policy)
            | ExprNode::Div(_, _, policy)
            | ExprNode::Mod(_, _, policy) => Some(*policy),
            _ => None,
        }
    }

    /// Bind only the runtime arena, after static program materialization.
    pub(crate) fn bind_runtime_error(
        &mut self,
        runtime_error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) {
        self.runtime_error = Some(runtime_error);
    }

    pub(crate) fn runtime_error_binding(
        &self,
    ) -> Result<Arc<crate::runtime::runtime_state::RuntimeErrorState>, String> {
        self.runtime_error
            .clone()
            .ok_or_else(|| "expression arena has no exact fragment runtime binding".to_string())
    }

    pub(crate) fn check_runtime_error(&self) -> Result<(), String> {
        match self.runtime_error.as_ref().and_then(|error| error.error()) {
            Some(error) => Err(error.to_string()),
            None => Ok(()),
        }
    }

    pub(crate) fn wait_interruptibly(&self, duration: std::time::Duration) -> Result<(), String> {
        match &self.runtime_error {
            Some(error) => error
                .wait_interruptibly(duration)
                .map_err(|error| error.to_string()),
            None => Err("SLEEP requires an exact fragment runtime binding".to_string()),
        }
    }

    pub fn push(&mut self, node: ExprNode) -> ExprId {
        self.push_typed(node, DataType::Null)
    }

    pub fn push_typed(&mut self, node: ExprNode, data_type: DataType) -> ExprId {
        let id = ExprId(self.nodes.len());
        self.nodes.push(node);
        self.types.push(data_type);
        self.field_schemas.push(None);
        id
    }

    pub fn node(&self, id: ExprId) -> Option<&ExprNode> {
        self.nodes.get(id.0)
    }

    pub fn replace_literal(&mut self, id: ExprId, value: LiteralValue) -> Result<(), String> {
        let node = self
            .nodes
            .get_mut(id.0)
            .ok_or_else(|| format!("expression id {} is out of bounds", id.0))?;
        if !matches!(node, ExprNode::Literal(_)) {
            return Err(format!("expression id {} is not a literal", id.0));
        }
        *node = ExprNode::Literal(value);
        Ok(())
    }

    pub fn data_type(&self, id: ExprId) -> Option<&DataType> {
        self.types.get(id.0)
    }

    pub fn set_field_schema(&mut self, id: ExprId, field_schema: ChunkFieldSchema) {
        if let Some(slot) = self.field_schemas.get_mut(id.0) {
            *slot = Some(field_schema);
        }
    }

    pub fn field_schema(&self, id: ExprId) -> Option<&ChunkFieldSchema> {
        self.field_schemas
            .get(id.0)
            .and_then(|schema| schema.as_ref())
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

    pub fn set_session_time_zone(&mut self, time_zone: Option<String>) {
        self.session_time_zone = time_zone;
    }

    pub fn session_time_zone(&self) -> Option<&str> {
        self.session_time_zone.as_deref()
    }

    pub fn eval(&self, id: ExprId, chunk: &Chunk) -> Result<ArrayRef, String> {
        let node = self
            .nodes
            .get(id.0)
            .ok_or_else(|| "invalid ExprId".to_string())?;
        match node {
            ExprNode::Constant(value) => {
                if !self.data_type(id).is_some_and(|ty| novarocks_type_contract::arrow_data_types_exact(ty, &value.value_type().data_type)) {
                    return Err("constant pool carrier differs from expression metadata".into());
                }
                constant_eval::broadcast(value, chunk.len())
            }
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
            ExprNode::Cast(child, policy) => cast::eval(self, id, *child, *policy, chunk),
            ExprNode::CastTime(child, _) => cast::eval_time(self, id, *child, chunk),
            ExprNode::CastTimeFromDatetime(child, _) => {
                cast::eval_time_from_datetime(self, id, *child, chunk)
            }
            ExprNode::Add(a, b, policy) => arithmetic::eval_add(self, id, *a, *b, *policy, chunk),
            ExprNode::Sub(a, b, policy) => arithmetic::eval_sub(self, id, *a, *b, *policy, chunk),
            ExprNode::Mul(a, b, policy) => arithmetic::eval_mul(self, id, *a, *b, *policy, chunk),
            ExprNode::Div(a, b, policy) => arithmetic::eval_div(self, id, *a, *b, *policy, chunk),
            ExprNode::Mod(a, b, policy) => arithmetic::eval_mod(self, id, *a, *b, *policy, chunk),
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
                let arity = novarocks_functions::invocation_arity::InvocationArity {
                    minimum: metadata.min_args,
                    maximum: metadata.max_args,
                };
                if let Some(failure) = arity.failure(args.len()) {
                    return Err(failure.with_original_message(metadata.name, std::fmt::format));
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
                    FunctionKind::MvState(name) => {
                        function::eval_mv_state_function(name, self, id, args, chunk)
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

pub fn cast_array_to_target(array: &ArrayRef, target_type: &DataType) -> Result<ArrayRef, String> {
    cast::cast_with_special_rules(array, target_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::Chunk;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use std::sync::Arc;

    struct ConstantTestControl;

    impl novarocks_type_contract::PureCompileControl for ConstantTestControl {
        fn checkpoint(
            &self,
            _phase: novarocks_type_contract::CompilePhase,
            _work_units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }

    fn constant_policy() -> novarocks_functions::ConstantPolicy {
        novarocks_functions::ConstantPolicy {
            max_rows: 64,
            max_array_nodes: 64,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 65536,
            max_type_depth: 16,
            max_type_nodes: 64,
            max_dictionary_depth: 8,
            max_metadata_bytes: 4096,
            max_library_validation_work: 65536,
            // Nested fixtures admit the owner's conservative diagnostic
            // scratch bound before testing output-format expansion.
            max_library_validation_bytes: 1024 * 1024,
        }
    }

    fn checked_constant(
        array: ArrayRef,
        value_type: novarocks_type_contract::FunctionValueType,
        ordinal: u32,
    ) -> novarocks_functions::ConstantValue {
        novarocks_functions::ConstantPool::try_new(
            Arc::new(
                value_type
                    .try_to_field("constant")
                    .expect("exact constant field"),
            ),
            value_type,
            array.to_data(),
            constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            &ConstantTestControl,
        )
        .expect("checked immutable pool")
        .value(ordinal)
        .expect("checked ordinal")
    }

    fn constant_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0; rows]))])
            .expect("batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn evaluate_constant(value: novarocks_functions::ConstantValue, rows: usize) -> ArrayRef {
        let mut arena = ExprArena::default();
        let ty = value.value_type().data_type.clone();
        let id = arena.push_typed(ExprNode::Constant(value), ty);
        arena
            .eval(id, &constant_chunk(rows))
            .expect("exact constant evaluation")
    }

    #[test]
    fn constant_leaf_broadcasts_only_its_checked_pool_ordinal() {
        let pool = Arc::new(Int32Array::from(vec![-91, 42, 700])) as ArrayRef;
        let value = checked_constant(
            pool,
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
            1,
        );
        let source = value.pool().array().clone();
        let output = evaluate_constant(value.clone(), 4);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32")
                .values()
                .as_ref(),
            &[42; 4]
        );
        assert_eq!(
            source
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("source")
                .values()
                .as_ref(),
            &[-91, 42, 700]
        );
        let empty = evaluate_constant(value, 0);
        assert_eq!(empty.data_type(), &DataType::Int32);
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn constant_leaf_preserves_float32_bits() {
        use arrow::array::Float32Array;
        let bits = [0x7fc0_0042, 0x8000_0000, 0x0000_0000];
        for (ordinal, expected) in bits.into_iter().enumerate() {
            let value = checked_constant(
                Arc::new(Float32Array::from(bits.map(f32::from_bits).to_vec())),
                novarocks_type_contract::FunctionValueType::new(DataType::Float32, false),
                ordinal as u32,
            );
            let output = evaluate_constant(value, 3);
            assert!(
                output
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("Float32")
                    .values()
                    .iter()
                    .all(|value| value.to_bits() == expected)
            );
        }
    }

    #[test]
    fn constant_leaf_freeze_thaw_preserves_json_variant_and_shared_pool() {
        use arrow::array::{LargeBinaryArray, StringArray};
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let cases: [(ArrayRef, FunctionValueType); 2] = [
            (
                Arc::new(StringArray::from(vec!["unused", "{\"x\":1}"])),
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    false,
                    ValueLogicalType::Json,
                )
                .expect("Json source"),
            ),
            (
                Arc::new(LargeBinaryArray::from_vec(vec![b"unused", &[1, 2, 3]])),
                FunctionValueType::try_with_logical_type(
                    DataType::LargeBinary,
                    false,
                    ValueLogicalType::Variant,
                )
                .expect("Variant source"),
            ),
        ];
        for (array, ty) in cases {
            let value = checked_constant(array, ty.clone(), 1);
            let mut arena = ExprArena::default();
            let id = arena.push_typed(ExprNode::Constant(value.clone()), ty.data_type.clone());
            let frozen = arena.into_immutable().expect("freeze CV leaf");
            let novarocks_local_program::StaticExprKind::Constant(stored) =
                frozen.nodes()[id.0].kind()
            else {
                panic!("constant must not become a legacy literal");
            };
            assert_eq!(stored.ordinal(), 1);
            assert_eq!(stored.value_type(), &ty);
            assert_eq!(stored.field(), value.field());
            assert!(Arc::ptr_eq(stored.pool().array(), value.pool().array()));
            let thawed =
                ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
            let Some(ExprNode::Constant(restored)) = thawed.node(id) else {
                panic!("thaw must retain the exact constant owner");
            };
            assert_eq!(restored.ordinal(), value.ordinal());
            assert_eq!(restored.value_type(), value.value_type());
            assert_eq!(restored.field(), value.field());
            assert!(Arc::ptr_eq(restored.pool().array(), value.pool().array()));
            let output = thawed
                .eval(id, &constant_chunk(3))
                .expect("broadcast thawed CV");
            assert_eq!(output.data_type(), &ty.data_type);
            match ty.logical_type {
                ValueLogicalType::Json => assert!(
                    output
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Json carrier")
                        .iter()
                        .all(|value| value == Some("{\"x\":1}"))
                ),
                ValueLogicalType::Variant => assert!(
                    output
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .expect("Variant carrier")
                        .iter()
                        .all(|value| value == Some(&[1, 2, 3][..]))
                ),
                _ => panic!("unexpected domain"),
            }
        }
    }

    #[test]
    fn constant_leaf_preserves_typed_null_and_nested_child_metadata() {
        use arrow::array::{ListArray, StringArray};
        use arrow_buffer::OffsetBuffer;
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let child = Arc::new(
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .expect("Json child")
                .try_to_field("payload")
                .expect("tagged child")
                .with_metadata(
                    [
                        (
                            novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_string(),
                            "json".to_string(),
                        ),
                        ("PARQUET:field_id".to_string(), "19".to_string()),
                    ]
                    .into(),
                ),
        );
        let source = Arc::new(
            ListArray::try_new(
                child,
                OffsetBuffer::new(vec![0_i32, 1, 3].into()),
                Arc::new(StringArray::from(vec![
                    Some("unused"),
                    Some("{\"x\":1}"),
                    None,
                ])),
                None,
            )
            .expect("list pool"),
        ) as ArrayRef;
        let ty = FunctionValueType::new(source.data_type().clone(), false);
        let output = evaluate_constant(checked_constant(source, ty.clone(), 1), 3);
        assert_eq!(output.data_type(), &ty.data_type);
        let list = output
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list output");
        for row in 0..3 {
            let items = list.value(row);
            let strings = items
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Json carrier");
            assert_eq!(strings.len(), 2);
            assert_eq!(strings.value(0), "{\"x\":1}");
            assert!(strings.is_null(1));
        }
        let nullable = FunctionValueType {
            nullable: true,
            ..ty
        };
        let null = checked_constant(new_null_array(&nullable.data_type, 2), nullable.clone(), 1);
        let output = evaluate_constant(null.clone(), 3);
        assert_eq!(output.data_type(), &nullable.data_type);
        assert_eq!(output.null_count(), 3);
        let empty = evaluate_constant(null, 0);
        assert_eq!(empty.data_type(), &nullable.data_type);
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn constant_leaf_preserves_dictionary_and_run_end_encoded_values() {
        use arrow::array::{DictionaryArray, Int8Array, RunArray, StringArray};
        use arrow::datatypes::{Int8Type, Int32Type};
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["unused", "selected"])),
            )
            .expect("dictionary"),
        ) as ArrayRef;
        let run = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![2, 5]),
                &StringArray::from(vec!["unused", "selected"]),
            )
            .expect("run encoded pool"),
        ) as ArrayRef;
        for (array, ordinal) in [(dictionary, 1), (run, 3)] {
            let ty =
                novarocks_type_contract::FunctionValueType::new(array.data_type().clone(), false);
            let output = evaluate_constant(checked_constant(array, ty.clone(), ordinal), 4);
            assert_eq!(output.data_type(), &ty.data_type);
            let values = arrow::compute::cast(&output, &DataType::Utf8)
                .expect("decode exact output for oracle");
            assert!(
                values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("selected output")
                    .iter()
                    .all(|value| value == Some("selected"))
            );
        }
    }

    fn small_run() -> ArrayRef {
        use arrow::array::{Int16Array, RunArray};
        use arrow::datatypes::Int16Type;
        Arc::new(
            RunArray::<Int16Type>::try_new(&Int16Array::from(vec![2]), &Int32Array::from(vec![7]))
                .expect("small run"),
        )
    }

    #[test]
    fn constant_broadcast_rejects_root_and_fixed_list_nested_run_end_overflow() {
        use arrow::array::FixedSizeListArray;
        let run = small_run();
        let root = checked_constant(
            run.clone(),
            novarocks_type_contract::FunctionValueType::new(run.data_type().clone(), false),
            1,
        );
        assert!(constant_eval::broadcast(&root, i16::MAX as usize).is_ok());
        assert_eq!(
            constant_eval::broadcast(&root, i16::MAX as usize + 1)
                .expect_err("root run-end extent"),
            "constant broadcast output extent exceeds its Arrow format"
        );
        for null in [false, true] {
            let array = Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("run", run.data_type().clone(), false)),
                    2,
                    run.clone(),
                    null.then(|| arrow_buffer::NullBuffer::new_null(1)),
                )
                .expect("fixed list"),
            ) as ArrayRef;
            let ty =
                novarocks_type_contract::FunctionValueType::new(array.data_type().clone(), null);
            let value = checked_constant(array, ty, 0);
            assert_eq!(
                constant_eval::broadcast(&value, 16384)
                    .expect_err("fixed list copies children even under NULL"),
                "constant broadcast output extent exceeds its Arrow format"
            );
        }
    }

    #[test]
    fn constant_broadcast_distinguishes_take_null_mask_from_mutable_copy() {
        use arrow::array::{ListArray, StructArray};
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        let run = small_run();
        let child = Arc::new(Field::new("run", run.data_type().clone(), false));
        let null_list = Arc::new(
            ListArray::try_new(
                child,
                OffsetBuffer::new(vec![0_i32, 2].into()),
                run.clone(),
                Some(NullBuffer::new_null(1)),
            )
            .expect("NULL list with payload"),
        ) as ArrayRef;
        let direct = checked_constant(
            null_list.clone(),
            novarocks_type_contract::FunctionValueType::new(null_list.data_type().clone(), true),
            0,
        );
        let output =
            constant_eval::broadcast(&direct, 16384).expect("take_list omits NULL parent payload");
        assert_eq!(output.null_count(), 16384);
        let outer = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new(
                    "nullable_list",
                    null_list.data_type().clone(),
                    true,
                )),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                null_list,
                None,
            )
            .expect("outer list"),
        ) as ArrayRef;
        let outer = checked_constant(
            outer.clone(),
            novarocks_type_contract::FunctionValueType::new(outer.data_type().clone(), false),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&outer, 16384)
                .expect_err("MutableArrayData copies nested NULL list payload"),
            "constant broadcast output extent exceeds its Arrow format"
        );
        let structure = Arc::new(
            StructArray::try_new(
                vec![Arc::new(Field::new("run", run.data_type().clone(), false))].into(),
                vec![run.slice(0, 1)],
                Some(NullBuffer::new_null(1)),
            )
            .expect("NULL struct"),
        ) as ArrayRef;
        let value = checked_constant(
            structure.clone(),
            novarocks_type_contract::FunctionValueType::new(structure.data_type().clone(), true),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&value, 32768)
                .expect_err("struct take copies children under NULL"),
            "constant broadcast output extent exceeds its Arrow format"
        );
    }

    #[test]
    fn constant_broadcast_checks_sparse_union_but_retains_dictionary_values() {
        use arrow::array::{DictionaryArray, Int8Array, UnionArray};
        use arrow::datatypes::{Int8Type, UnionFields};
        use arrow_buffer::ScalarBuffer;
        let run = small_run().slice(0, 1);
        let fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Arc::new(Field::new("selected", DataType::Int32, false)),
                Arc::new(Field::new("unused", run.data_type().clone(), false)),
            ],
        )
        .expect("union fields");
        for dense in [false, true] {
            let union = Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    ScalarBuffer::from(vec![0_i8]),
                    dense.then(|| ScalarBuffer::from(vec![0_i32])),
                    vec![Arc::new(Int32Array::from(vec![5])) as ArrayRef, run.clone()],
                )
                .expect("union"),
            ) as ArrayRef;
            let value = checked_constant(
                union.clone(),
                novarocks_type_contract::FunctionValueType::new(union.data_type().clone(), false),
                0,
            );
            let output = constant_eval::broadcast(&value, 32768);
            if dense {
                assert_eq!(
                    output
                        .expect("dense union only expands selected child")
                        .len(),
                    32768
                );
            } else {
                assert_eq!(
                    output.expect_err("sparse union expands all children"),
                    "constant broadcast output extent exceeds its Arrow format"
                );
            }
        }
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), run)
                .expect("dictionary"),
        ) as ArrayRef;
        let value = checked_constant(
            dictionary.clone(),
            novarocks_type_contract::FunctionValueType::new(dictionary.data_type().clone(), false),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&value, 32768)
                .expect("dictionary value backing is retained, not expanded")
                .len(),
            32768
        );
        assert!(
            constant_eval::broadcast(&value, usize::MAX).is_err(),
            "checked index/allocation extent precedes allocation"
        );
    }

    #[test]
    fn constant_broadcast_nested_byte_offsets_fail_before_library_allocation() {
        use arrow::array::{ListArray, StringArray};
        use arrow_buffer::OffsetBuffer;
        let strings = Arc::new(StringArray::from(vec!["abcd"])) as ArrayRef;
        let list = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("text", DataType::Utf8, false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                strings,
                None,
            )
            .expect("list"),
        ) as ArrayRef;
        let value = checked_constant(
            list.clone(),
            novarocks_type_contract::FunctionValueType::new(list.data_type().clone(), false),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&value, (i32::MAX as usize / 4) + 1)
                .expect_err("nested UTF8 offset overflow"),
            "constant broadcast output extent exceeds its Arrow format"
        );
    }

    fn dictionary_list(dictionary_len: usize, null: bool) -> ArrayRef {
        use arrow::array::{DictionaryArray, Int8Array, ListArray};
        use arrow::datatypes::Int8Type;
        use arrow_buffer::{NullBuffer, OffsetBuffer};
        let values = Arc::new(Int32Array::from_iter_values(0..dictionary_len as i32));
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values)
                .expect("unused dictionary entries are legal"),
        ) as ArrayRef;
        Arc::new(
            ListArray::try_new(
                Arc::new(Field::new(
                    "dictionary",
                    dictionary.data_type().clone(),
                    false,
                )),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                dictionary,
                null.then(|| NullBuffer::new_null(1)),
            )
            .expect("dictionary list"),
        )
    }

    #[test]
    fn constant_broadcast_checks_actual_mutable_dictionary_length_before_null_selection() {
        use arrow::array::ListArray;
        for dictionary_len in [127, 128] {
            for null in [false, true] {
                let array = dictionary_list(dictionary_len, null);
                let value = checked_constant(
                    array.clone(),
                    novarocks_type_contract::FunctionValueType::new(
                        array.data_type().clone(),
                        null,
                    ),
                    0,
                );
                let output = constant_eval::broadcast(&value, 3);
                if dictionary_len == 127 {
                    let output = output.expect("dictionary length fits mutable-copy key cast");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.null_count(), if null { 3 } else { 0 });
                } else {
                    assert_eq!(
                        output.expect_err("constructor precedes NULL list selection"),
                        "constant broadcast output extent exceeds its Arrow format"
                    );
                }
            }
        }
        // Direct dictionary take does not use MutableArrayData and must retain
        // its complete legal backing at this same boundary.
        let list = dictionary_list(128, false);
        let list = list.as_any().downcast_ref::<ListArray>().expect("list");
        let dictionary = list.values().clone();
        let value = checked_constant(
            dictionary.clone(),
            novarocks_type_contract::FunctionValueType::new(dictionary.data_type().clone(), false),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&value, 3)
                .expect("direct take")
                .len(),
            3
        );
    }

    #[test]
    fn constant_broadcast_checks_zero_capacity_constructor_children_without_rejecting_empty_take() {
        use arrow::array::{FixedSizeListArray, ListArray};
        use arrow_buffer::OffsetBuffer;
        let empty_list = dictionary_list(128, false).slice(0, 0);
        let fixed = Arc::new(
            FixedSizeListArray::try_new_with_length(
                Arc::new(Field::new("list", empty_list.data_type().clone(), false)),
                0,
                empty_list,
                None,
                1,
            )
            .expect("zero-width list retaining unused dictionary backing"),
        ) as ArrayRef;
        let value = checked_constant(
            fixed.clone(),
            novarocks_type_contract::FunctionValueType::new(fixed.data_type().clone(), false),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&value, 3)
                .expect("fixed-list take empty child bypasses mutable-copy constructor")
                .len(),
            3
        );
        let outer = Arc::new(
            ListArray::try_new(
                Arc::new(Field::new("fixed", fixed.data_type().clone(), false)),
                OffsetBuffer::new(vec![0_i32, 1].into()),
                fixed,
                None,
            )
            .expect("outer list"),
        ) as ArrayRef;
        let value = checked_constant(
            outer.clone(),
            novarocks_type_contract::FunctionValueType::new(outer.data_type().clone(), false),
            0,
        );
        assert_eq!(
            constant_eval::broadcast(&value, 1)
                .expect_err("mutable-copy recurses into zero-capacity constructor children"),
            "constant broadcast output extent exceeds its Arrow format"
        );
    }

    #[test]
    fn constant_leaf_rejects_a_different_arena_carrier() {
        let value = checked_constant(
            Arc::new(Int32Array::from(vec![7])),
            novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
            0,
        );
        let mut arena = ExprArena::default();
        let id = arena.push_typed(ExprNode::Constant(value), DataType::Int64);
        assert_eq!(
            arena
                .eval(id, &constant_chunk(1))
                .expect_err("no implicit constant cast"),
            "constant pool carrier differs from expression metadata"
        );
    }

    #[test]
    fn constant_leaf_rejects_same_carrier_with_changed_dictionary_field_identity() {
        use arrow::array::{DictionaryArray, Int8Array, StringArray, StructArray};
        use arrow::datatypes::Int8Type;
        #[allow(deprecated)]
        let field = |id| {
            Arc::new(Field::new_dict(
                "dictionary",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                id,
                true,
            ))
        };
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["selected"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let source =
            Arc::new(StructArray::try_new(vec![field(19)].into(), vec![dictionary], None).unwrap())
                as ArrayRef;
        let forged = DataType::Struct(vec![field(20)].into());
        assert_eq!(source.data_type(), &forged);
        let value = checked_constant(
            source.clone(),
            novarocks_type_contract::FunctionValueType::new(source.data_type().clone(), false),
            0,
        );
        let mut arena = ExprArena::default();
        let id = arena.push_typed(ExprNode::Constant(value), forged);
        assert_eq!(
            arena.eval(id, &constant_chunk(1)).unwrap_err(),
            "constant pool carrier differs from expression metadata"
        );
    }

    #[test]
    fn typed_null_literal_uses_declared_type() {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Utf8);

        let field = Field::new("x", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };

        let arr = arena.eval(expr, &chunk).unwrap();
        assert_eq!(arr.data_type(), &DataType::Utf8);
        assert_eq!(arr.len(), 3);
        assert!(arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }
}

#[cfg(test)]
mod legacy_calendar_parts_baseline_tests;

#[cfg(test)]
mod legacy_calendar_to_date_baseline_tests;

#[cfg(test)]
mod legacy_calendar_sec_to_time_baseline_tests;

#[cfg(test)]
mod legacy_crc32_shared_baseline_tests;

#[cfg(test)]
mod legacy_regexp_count_provenance_baseline_tests;
#[cfg(test)]
mod legacy_regexp_position_baseline_tests;

#[cfg(test)]
mod legacy_md5_family_baseline_tests;

#[cfg(test)]
mod legacy_decimal_text_baseline_tests;

#[cfg(test)]
mod legacy_decimal_text_arena_baseline_tests;

#[cfg(test)]
mod legacy_split_baseline_tests;

#[cfg(test)]
mod legacy_null_or_empty_baseline_tests;

#[cfg(test)]
mod cast_decimal_text_oracle_tests;

#[cfg(test)]
mod legacy_time_text_baseline_tests;

#[cfg(test)]
mod legacy_largeint_text_baseline_tests;

#[cfg(test)]
mod cast_largeint_text_oracle_tests;

#[cfg(test)]
mod legacy_timestampadd_baseline_tests;

#[cfg(test)]
mod legacy_unixtime_baseline_tests;

#[cfg(test)]
#[path = "legacy_collection_construct_access_baseline_tests.rs"]
mod legacy_collection_construct_access_baseline_tests;

#[cfg(test)]
#[path = "legacy_array_element_access_baseline_tests.rs"]
mod legacy_array_element_access_baseline_tests;

#[cfg(test)]
#[path = "legacy_map_null_result_baseline_tests.rs"]
mod legacy_map_null_result_baseline_tests;

#[cfg(test)]
mod legacy_time_slice_baseline_tests;

#[cfg(test)]
#[path = "legacy_array_append_baseline_tests.rs"]
mod legacy_array_append_baseline_tests;

#[cfg(test)]
mod legacy_mod_pmod_shared_baseline_tests;

#[cfg(test)]
mod legacy_numeric_binary_shared_baseline_tests;

#[cfg(test)]
mod legacy_time_source_errors_baseline_tests;

#[cfg(test)]
#[path = "legacy_array_append_multi_constructor_baseline_tests.rs"]
mod legacy_array_append_multi_constructor_baseline_tests;

#[cfg(test)]
mod legacy_round_additional_baseline_tests;

#[cfg(test)]
#[path = "legacy_map_size_baseline_tests.rs"]
mod legacy_map_size_baseline_tests;

#[cfg(test)]
#[path = "legacy_map_keys_values_baseline_tests.rs"]
mod legacy_map_keys_values_baseline_tests;

#[cfg(test)]
mod legacy_string_measure_additional_baseline_tests;

#[cfg(test)]
mod legacy_regexp_count_extended_baseline_tests;

#[cfg(test)]
mod legacy_regexp_count_long_error_baseline_tests;

#[cfg(test)]
#[path = "legacy_sha2_additional_baseline_tests.rs"]
mod legacy_sha2_additional_baseline_tests;

#[cfg(test)]
#[path = "legacy_sm3_additional_baseline_tests.rs"]
mod legacy_sm3_additional_baseline_tests;

#[cfg(test)]
#[path = "legacy_array_repeat_baseline_tests.rs"]
mod legacy_array_repeat_baseline_tests;

#[cfg(test)]
mod cast_date_float_oracle_tests;
#[cfg(test)]
mod legacy_date_float_cast_baseline_tests;

#[cfg(test)]
mod cast_float_date_oracle_tests;
#[cfg(test)]
mod legacy_float_date_cast_baseline_tests;

#[cfg(test)]
mod legacy_parse_url_baseline_tests;

#[cfg(test)]
#[path = "legacy_cardinality_dedup_baseline_tests.rs"]
mod legacy_cardinality_dedup_baseline_tests;

#[cfg(test)]
#[path = "legacy_numeric_unary_intrinsic_baseline_tests.rs"]
mod legacy_numeric_unary_intrinsic_baseline_tests;

#[cfg(test)]
mod legacy_to_base64_source_baseline_tests;

#[cfg(test)]
mod legacy_to_binary_metadata_baseline_tests;

#[cfg(test)]
mod legacy_binary_text_cast_baseline_tests;
#[cfg(test)]
mod legacy_binary_text_hidden_null_baseline_tests;

#[cfg(test)]
mod cast_binary_text_oracle_tests;

#[cfg(test)]
mod legacy_array_match_baseline_tests;

#[cfg(test)]
mod legacy_array_difference_baseline_tests;

#[cfg(test)]
mod numeric_unary_original_nonnull_sql_baseline_tests;

#[cfg(test)]
#[path = "cast_decimal_float_oracle_tests.rs"]
mod cast_decimal_float_oracle_tests;
#[cfg(test)]
#[path = "legacy_decimal_float_cast_baseline_tests.rs"]
mod legacy_decimal_float_cast_baseline_tests;

#[cfg(test)]
mod numeric_unary_owned_transaction_tests;

#[cfg(test)]
#[path = "numeric_unary_public_transport_tests.rs"]
mod numeric_unary_public_transport_tests;

#[cfg(test)]
mod cast_decimal_float32_oracle_tests;
#[cfg(test)]
mod legacy_decimal_float32_cast_baseline_tests;

#[cfg(test)]
mod numeric_unary_writer_dml_source_tests;

#[cfg(test)]
mod decimal_float32_sql_own_null_tests;

#[cfg(test)]
mod numeric_unary_original_state_union_baseline_tests;

#[cfg(test)]
mod numeric_unary_exact_state_union_baseline_tests;

#[cfg(test)]
mod numeric_unary_state_author_tests;
#[cfg(test)]
mod numeric_unary_writer_source_author_tests;

#[cfg(test)]
mod numeric_unary_own_constant_state_tests;

#[cfg(test)]
mod numeric_unary_state_compile_control_tests;

#[cfg(test)]
mod legacy_reverse_shared_baseline_tests;

#[cfg(test)]
mod legacy_append_trailing_shared_baseline_tests;

#[cfg(test)]
mod append_trailing_actual_sql_source_tests;

#[cfg(test)]
mod append_trailing_actual_compiled_source_tests;

#[cfg(test)]
mod numeric_unary_ordered_sql_source_tests;

#[cfg(test)]
mod numeric_unary_typed_ordered_merge_tests;

#[cfg(test)]
mod legacy_map_entries_baseline_tests;

#[cfg(test)]
mod numeric_unary_narrow_state_source_tests;

#[cfg(test)]
mod map_entries_copy_edge_tests;

#[cfg(test)]
mod sql_fold_dependency_observation_tests;

#[cfg(test)]
mod sql_published_source_observation_tests;

#[cfg(test)]
mod sql_dependency_context_tests;
#[cfg(test)]
mod sql_scalar_presence_candidate_tests;
#[cfg(test)]
mod sql_scalar_presence_original_tests;

#[cfg(test)]
mod sql_dependency_artifact_storage_tests;

#[cfg(test)]
mod sql_dependency_type_host_abort_tests;

#[cfg(test)]
mod sql_dependency_binding_host_abort_tests;

#[cfg(test)]
mod sql_dependency_constant_host_abort_tests;

#[cfg(test)]
mod exact_percentile_actual_sql_rate_source_tests;

#[cfg(test)]
mod legacy_field_baseline_tests;

#[cfg(test)]
mod legacy_text_time_cast_baseline_tests;
#[cfg(test)]
mod cast_text_time_oracle_tests;

#[cfg(test)]
mod ndv_filter_actual_sql_source_tests;

#[cfg(test)]
mod cast_calendar_time_oracle_tests;

#[cfg(test)]
mod filter_conjunction_actual_sql_compiler_tests;

#[cfg(test)]
mod filter_conjunction_frame_tests;

#[cfg(test)]
mod native_bitnot_intrinsic_baseline_tests;

#[cfg(test)]
mod native_bitnot_intrinsic_after_tests;

#[cfg(test)]
mod legacy_decimal128_rescale_baseline_tests;

#[cfg(test)]
mod approx_percentile_actual_sql_source_tests;

#[cfg(test)]
mod legacy_percentile_hash_original_tests;

#[cfg(test)]
mod percentile_hash_original_sql_source_tests;

#[cfg(test)]
mod legacy_observed_list_cast_baseline_tests;

#[cfg(test)]
mod sql_dependency_resolved_binding_source_tests;

#[cfg(test)]
mod legacy_between_observed_baseline_tests;
#[cfg(test)]
mod between_actual_sql_compiler_tests;

#[cfg(test)]
mod legacy_integral_decimal128_baseline_tests;
#[cfg(test)]
mod integral_decimal128_actual_sql_tests;

#[cfg(test)]
mod percentile_hash_original_fold_tests;

#[cfg(test)]
mod between_actual_after_tests;

#[cfg(test)]
mod percentile_hash_native_n1_frame_tests;

#[cfg(test)]
mod percentile_hash_native_n1_availability_tests;

#[cfg(test)]
mod legacy_inlist_required_baseline_tests;
#[cfg(test)]
mod inlist_required_actual_sql_tests;

#[cfg(test)]
mod legacy_inlist_variant_local_baseline_tests;

#[cfg(test)]
mod legacy_bitmap_to_string_original_tests;

#[cfg(test)]
mod bitmap_to_string_actual_sql_source_tests;

#[cfg(test)]
mod inlist_signed_actual_after_tests;

#[cfg(test)]
mod scan_ordered_required_actual_tests;

#[cfg(test)]
mod legacy_hll_hash_original_tests;
#[cfg(test)]
mod hll_hash_actual_sql_source_tests;

#[cfg(test)]
mod bitmap_union_int_actual_sql_source_tests;

#[cfg(test)]
mod legacy_like_required_baseline_tests;
#[cfg(test)]
mod like_required_actual_sql_tests;

#[cfg(test)]
#[path = "map_agg_actual_sql_source_tests.rs"]
mod map_agg_actual_sql_source_tests;

#[cfg(test)]
mod like_shared_actual_after_tests;

#[cfg(test)]
mod hll_payload_aggregate_actual_sql_source_tests;

#[cfg(test)]
mod by_window_required_actual_sql_tests;

#[cfg(test)]
mod hll_insert_literal_original_tests;

#[cfg(test)]
mod legacy_percentile_approx_raw_original_tests;
#[cfg(test)]
mod percentile_approx_raw_actual_sql_source_tests;

#[cfg(test)]
mod legacy_float64_decimal128_baseline_tests;
#[cfg(test)]
mod cast_float64_decimal128_oracle_tests;

#[cfg(test)]
mod join_probe_filter_actual_sql_tests;

#[cfg(test)]
mod bitmap_agg_actual_sql_source_tests;

#[cfg(test)]
mod legacy_struct_subfield_baseline_tests;
#[cfg(test)]
mod array_struct_subfield_actual_sql_source_tests;

#[cfg(test)]
mod original_float_arithmetic_baseline_tests;
#[cfg(test)]
mod float_arithmetic_required_oracle_tests;
#[cfg(test)]
mod float_arithmetic_statistics_actual_sql_tests;

#[cfg(test)]
mod float_arithmetic_conversion_author_tests;

#[cfg(test)]
mod grouping_sets_actual_sql_placement_tests;

#[cfg(test)]
mod cow_source_receipt_codec_tests;

#[cfg(test)]
mod e08s1_original_lifecycle_baseline_tests;

#[cfg(test)]
mod e08s1_owned_lifecycle_tests;

#[cfg(test)]
mod e08s1_late_original_source_tests;

#[cfg(test)]
mod e08s1_late_source_tests;

#[cfg(test)]
mod e08s1_percentile_original_static_tests;

#[cfg(test)]
mod e08s1_percentile_static_tests;

#[cfg(test)]
mod e08s1_mv_original_catalogue_tests;

#[cfg(test)]
mod e08s1_mv_catalogue_retention_tests;

#[cfg(test)]
mod e08s1_environment_original_tests;

#[cfg(test)]
mod e08s1_environment_tests;

#[cfg(test)]
mod e08s1_fold_environment_original_tests;

#[cfg(test)]
mod e08s1_fold_environment_tests;

#[cfg(test)]
mod e08s1_logical_environment_original_tests;

#[cfg(test)]
mod e08s1_logical_environment_tests;

#[cfg(test)]
mod e08s1_table_fold_parent_tests;

#[cfg(test)]
mod e08s1_append_original_static_tests;

#[cfg(test)]
mod e08s1_append_static_tests;

#[cfg(test)]
mod e08s1_string_batch_original_tests;

#[cfg(test)]
mod e08s1_string_batch_static_tests;

#[cfg(test)]
mod e08s1_string_batch_differential_tests;

#[cfg(test)]
mod e08s1_time_original_profile_tests;

#[cfg(test)]
mod e08s1_time_static_profile_tests;

#[cfg(test)]
mod legacy_repeat_pad_shared_baseline_tests;

#[cfg(test)]
mod legacy_left_right_shared_baseline_tests;

#[cfg(test)]
mod left_right_actual_sql_source_tests;

#[cfg(test)]
mod legacy_split_part_shared_baseline_tests;

#[cfg(test)]
mod split_part_actual_sql_source_tests;

#[cfg(test)]
mod approx_top_k_actual_sql_source_tests;

#[cfg(test)]
mod parse_json_original_runtime_baseline_tests;
#[cfg(test)]
mod parse_json_actual_sql_source_tests;
