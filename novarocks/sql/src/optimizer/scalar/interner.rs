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

//! Observed structural interning. Fingerprints are process-local buckets,
//! never equality evidence. Index/vector allocations are not MEM grants.

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use arrow::datatypes::DataType;
use novarocks_functions::{ConstantError, ConstantValue};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
};

use super::{
    ColumnDisplay, FunctionValueType, HashableLiteral, LiteralValue, ScalarArena, ScalarId,
    ScalarNode, SortKey,
};
use crate::binding::SqlFunctionBinding;
use crate::common::{WindowBound, WindowFrame};
use crate::compiler::SqlCompileError;

#[derive(Debug)]
enum InternerError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Constant(ConstantError),
}
impl From<CompileControlError> for InternerError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ValueTypeError> for InternerError {
    fn from(value: ValueTypeError) -> Self {
        Self::Type(value)
    }
}
impl From<ConstantError> for InternerError {
    fn from(value: ConstantError) -> Self {
        match value {
            ConstantError::Control(cause) => Self::Control(cause),
            other => Self::Constant(other),
        }
    }
}
impl From<InternerError> for SqlCompileError {
    fn from(value: InternerError) -> Self {
        match value {
            InternerError::Control(error) => error.into(),
            InternerError::Type(error) => Self::Compilation(error.to_string()),
            InternerError::Constant(error @ ConstantError::Limit(_)) => Self::from(error),
            InternerError::Constant(error) => Self::Compilation(error.to_string()),
        }
    }
}

// Tokens borrow immutable facts rather than cloning the structural key. Their
// bounded traversal scratch remains separate from host allocation admission.
enum Token<'a> {
    Number(u128),
    Bytes(&'a [u8]),
    Type(&'a DataType),
    ValueType(&'a FunctionValueType),
    Constant(&'a ConstantValue),
}
struct Fields<'a, 'w, 'c> {
    tokens: Vec<Token<'a>>,
    work: &'w mut CompileCheckpoints<'c>,
}
impl<'a> Fields<'a, '_, '_> {
    fn push(&mut self, token: Token<'a>) -> Result<(), InternerError> {
        self.work.step()?;
        self.tokens.push(token);
        Ok(())
    }
    fn number(&mut self, value: u128) -> Result<(), InternerError> {
        self.push(Token::Number(value))
    }
    fn boolean(&mut self, value: bool) -> Result<(), InternerError> {
        self.number(u128::from(value))
    }
    fn text(&mut self, value: &'a str) -> Result<(), InternerError> {
        self.push(Token::Bytes(value.as_bytes()))
    }
    fn id(&mut self, id: ScalarId) -> Result<(), InternerError> {
        self.number(id.0 as u128)
    }
    fn ids(&mut self, ids: &[ScalarId]) -> Result<(), InternerError> {
        self.number(ids.len() as u128)?;
        for id in ids {
            self.id(*id)?;
        }
        Ok(())
    }
    fn optional_id(&mut self, id: Option<ScalarId>) -> Result<(), InternerError> {
        self.boolean(id.is_some())?;
        if let Some(id) = id {
            self.id(id)?;
        }
        Ok(())
    }
    fn optional_text(&mut self, text: Option<&'a str>) -> Result<(), InternerError> {
        self.boolean(text.is_some())?;
        if let Some(text) = text {
            self.text(text)?;
        }
        Ok(())
    }
    fn display(&mut self, display: Option<&'a ColumnDisplay>) -> Result<(), InternerError> {
        self.boolean(display.is_some())?;
        if let Some(display) = display {
            self.optional_text(display.qualifier.as_deref())?;
            self.text(&display.column)?;
        }
        Ok(())
    }
    fn sort_keys(&mut self, keys: &'a [SortKey]) -> Result<(), InternerError> {
        self.number(keys.len() as u128)?;
        for key in keys {
            self.id(key.expr)?;
            self.boolean(key.asc)?;
            self.boolean(key.nulls_first)?;
            self.display(key.display.as_ref())?;
        }
        Ok(())
    }
    fn value_type(&mut self, ty: &'a FunctionValueType) -> Result<(), InternerError> {
        self.push(Token::ValueType(ty))
    }
    fn binding(&mut self, binding: &'a SqlFunctionBinding) -> Result<(), InternerError> {
        crate::binding::observed::visit_fields(binding, |field| match field {
            crate::binding::observed::BindingField::Number(value) => self.number(value),
            crate::binding::observed::BindingField::Bytes(value) => self.push(Token::Bytes(value)),
            crate::binding::observed::BindingField::ValueType(value) => self.value_type(value),
        })
    }
    fn bound(&mut self, bound: &WindowBound) -> Result<(), InternerError> {
        match bound {
            WindowBound::UnboundedPreceding => self.number(0),
            WindowBound::Preceding(offset) => {
                self.number(1)?;
                self.number(*offset as u128)
            }
            WindowBound::CurrentRow => self.number(2),
            WindowBound::Following(offset) => {
                self.number(3)?;
                self.number(*offset as u128)
            }
            WindowBound::UnboundedFollowing => self.number(4),
        }
    }
    fn frame(&mut self, frame: Option<&WindowFrame>) -> Result<(), InternerError> {
        self.boolean(frame.is_some())?;
        if let Some(frame) = frame {
            self.number(frame.frame_type as u128)?;
            self.bound(&frame.start)?;
            self.bound(&frame.end)?;
        }
        Ok(())
    }
    fn literal(&mut self, literal: &'a HashableLiteral) -> Result<(), InternerError> {
        match &literal.0 {
            LiteralValue::Null => self.number(0),
            LiteralValue::Bool(v) => {
                self.number(1)?;
                self.boolean(*v)
            }
            LiteralValue::Int(v) => {
                self.number(2)?;
                self.number(*v as u128)
            }
            LiteralValue::LargeInt(v) => {
                self.number(3)?;
                self.number(*v as u128)
            }
            LiteralValue::Float(v) => {
                self.number(4)?;
                self.number(v.to_bits() as u128)
            }
            LiteralValue::Decimal(v) => {
                self.number(5)?;
                self.text(v)
            }
            LiteralValue::String(v) => {
                self.number(6)?;
                self.text(v)
            }
            LiteralValue::Binary(v) => {
                self.number(7)?;
                self.push(Token::Bytes(v))
            }
        }
    }
}

fn node_fields<'a>(
    node: &'a ScalarNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<Token<'a>>, InternerError> {
    let mut f = Fields {
        tokens: Vec::new(),
        work,
    };
    match node {
        ScalarNode::ColumnRef(id) => {
            f.number(0)?;
            f.number(id.0 as u128)?;
        }
        ScalarNode::LambdaParamRef { name, slot_id } => {
            f.number(1)?;
            f.text(name)?;
            f.number(*slot_id as u128)?;
        }
        ScalarNode::Constant(value) => {
            f.number(18)?;
            f.push(Token::Constant(value))?;
        }
        ScalarNode::Literal(literal) => {
            f.number(2)?;
            f.literal(literal)?;
        }
        ScalarNode::BinaryOp {
            op,
            left,
            right,
            decimal_overflow_policy,
        } => {
            f.number(3)?;
            f.number(*op as u128)?;
            f.id(*left)?;
            f.id(*right)?;
            f.number(*decimal_overflow_policy as u128)?;
        }
        ScalarNode::UnaryOp { op, child } => {
            f.number(4)?;
            f.number(*op as u128)?;
            f.id(*child)?;
        }
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            binding,
            volatility,
        } => {
            f.number(5)?;
            f.text(name)?;
            f.ids(args)?;
            f.boolean(*distinct)?;
            f.binding(binding)?;
            f.number(*volatility as u128)?;
        }
        ScalarNode::LambdaFunction { params, body } => {
            f.number(6)?;
            f.number(params.len() as u128)?;
            for param in params {
                f.text(&param.name)?;
                f.number(param.slot_id as u128)?;
                f.value_type(&param.value_type)?;
            }
            f.id(*body)?;
        }
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => {
            f.number(7)?;
            f.text(name)?;
            f.ids(args)?;
            f.boolean(*distinct)?;
            f.sort_keys(order_by)?;
            f.binding(resolved)?;
        }
        ScalarNode::Cast {
            child,
            target,
            decimal_overflow_policy,
        } => {
            f.number(8)?;
            f.id(*child)?;
            f.push(Token::Type(target))?;
            f.number(*decimal_overflow_policy as u128)?;
        }
        ScalarNode::IsNull { child, negated } => {
            f.number(9)?;
            f.id(*child)?;
            f.boolean(*negated)?;
        }
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            f.number(10)?;
            f.id(*child)?;
            f.ids(list)?;
            f.boolean(*negated)?;
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => {
            f.number(11)?;
            f.id(*child)?;
            f.id(*low)?;
            f.id(*high)?;
            f.boolean(*negated)?;
        }
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => {
            f.number(12)?;
            f.id(*child)?;
            f.id(*pattern)?;
            f.boolean(*negated)?;
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            f.number(13)?;
            f.optional_id(*operand)?;
            f.number(when_then.len() as u128)?;
            for (when, then) in when_then {
                f.id(*when)?;
                f.id(*then)?;
            }
            f.optional_id(*else_expr)?;
        }
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => {
            f.number(14)?;
            f.id(*child)?;
            f.boolean(*value)?;
            f.boolean(*negated)?;
        }
        ScalarNode::Nested(child) => {
            f.number(15)?;
            f.id(*child)?;
        }
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            binding,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => {
            f.number(16)?;
            f.text(name)?;
            f.ids(args)?;
            f.boolean(*distinct)?;
            f.binding(binding)?;
            f.sort_keys(function_order_by)?;
            f.boolean(aggregate_binding.is_some())?;
            if let Some(binding) = aggregate_binding {
                f.binding(binding)?;
            }
            f.ids(partition_by)?;
            f.sort_keys(order_by)?;
            f.frame(window_frame.as_ref())?;
            f.boolean(*ignore_nulls)?;
        }
        ScalarNode::Lambda { params, body } => {
            f.number(17)?;
            f.number(params.len() as u128)?;
            for param in params {
                f.text(param)?;
            }
            f.id(*body)?;
        }
    }
    Ok(f.tokens)
}

fn bytes_hash(
    bytes: &[u8],
    hasher: &mut DefaultHasher,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), InternerError> {
    hasher.write_usize(bytes.len());
    for chunk in bytes.chunks(1024) {
        work.step()?;
        hasher.write(chunk);
    }
    Ok(())
}
fn type_hash(ty: &DataType, work: &mut CompileCheckpoints<'_>) -> Result<u64, InternerError> {
    novarocks_type_contract::arrow_data_type_fingerprint_with_profile_observed(
        ty,
        novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
        &mut || work.step().map_err(InternerError::from),
    )
}
fn fingerprint(
    tokens: &[Token<'_>],
    value_type: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, InternerError> {
    let mut hasher = DefaultHasher::new();
    let output_type = Token::ValueType(value_type);
    for token in tokens.iter().chain(std::iter::once(&output_type)) {
        work.step()?;
        match token {
            Token::Number(v) => {
                hasher.write_u8(0);
                hasher.write_u128(*v);
            }
            Token::Bytes(v) => {
                hasher.write_u8(1);
                bytes_hash(v, &mut hasher, work)?;
            }
            Token::Type(v) => {
                hasher.write_u8(2);
                hasher.write_u64(type_hash(v, work)?);
            }
            // Pool layout, raw bytes and dictionary numbering are not value
            // identity. The exact CV owner decides equality within this bucket.
            Token::Constant(_) => hasher.write_u8(4),
            Token::ValueType(v) => {
                hasher.write_u8(3);
                hasher.write_u8(v.logical_type() as u8);
                hasher.write_u8(u8::from(v.nullable()));
                hasher.write_u64(type_hash(v.data_type(), work)?);
            }
        }
    }
    Ok(hasher.finish())
}
fn bytes_equal(
    a: &[u8],
    b: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, InternerError> {
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.chunks(1024).zip(b.chunks(1024)) {
        work.step()?;
        if a != b {
            return Ok(false);
        }
    }
    Ok(true)
}
fn tokens_equal(
    a: &[Token<'_>],
    b: &[Token<'_>],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, InternerError> {
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.iter().zip(b) {
        work.step()?;
        let equal = match (a, b) {
            (Token::Number(a), Token::Number(b)) => a == b,
            (Token::Bytes(a), Token::Bytes(b)) => bytes_equal(a, b, work)?,
            (Token::Type(a), Token::Type(b)) => {
                novarocks_type_contract::arrow_data_types_exact_with_profile_observed(
                    a,
                    b,
                    novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                    || work.step().map_err(InternerError::from),
                )?
            }
            (Token::Constant(a), Token::Constant(b)) => {
                work.flush()?;
                a.equals_observed(b, CompilePhase::Validate, work.control())?
            }
            (Token::ValueType(a), Token::ValueType(b)) => {
                a.exactly_equals_observed(b, || work.step().map_err(InternerError::from))?
            }
            _ => false,
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

fn intern_inner(
    arena: &mut ScalarArena,
    node: ScalarNode,
    value_type: FunctionValueType,
    control: &dyn PureCompileControl,
    forced_fingerprint: Option<u64>,
) -> Result<ScalarId, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let node = arena.normalize(node);
    let result = (|| -> Result<(u64, Option<ScalarId>), InternerError> {
        if let ScalarNode::Constant(value) = &node
            && !value_type.exactly_equals_observed(value.value_type(), || {
                work.step().map_err(InternerError::from)
            })?
        {
            return Err(ConstantError::Invalid(
                "interner constant source differs from its frozen value type",
            )
            .into());
        }
        let fields = node_fields(&node, &mut work)?;
        let actual = fingerprint(&fields, &value_type, &mut work)?;
        let fingerprint = forced_fingerprint.unwrap_or(actual);
        if let Some(candidates) = arena.intern.get(&fingerprint) {
            for id in candidates {
                work.step()?;
                let other_fields = node_fields(&arena.nodes[id.0 as usize], &mut work)?;
                if tokens_equal(&fields, &other_fields, &mut work)?
                    && value_type
                        .exactly_equals_observed(&arena.value_types[id.0 as usize], || {
                            work.step().map_err(InternerError::from)
                        })?
                {
                    return Ok((fingerprint, Some(*id)));
                }
            }
        }
        Ok((fingerprint, None))
    })();
    if let Err(InternerError::Control(error)) = &result {
        return Err(error.clone().into());
    }
    if let Err(InternerError::Constant(error @ ConstantError::Limit(_))) = &result {
        return Err(SqlCompileError::from(error.clone()));
    }
    work.finish()?;
    let (fingerprint, existing) = result.map_err(SqlCompileError::from)?;
    if let Some(id) = existing {
        return Ok(id);
    }
    let id =
        ScalarId(u32::try_from(arena.nodes.len()).map_err(|_| SqlCompileError::ResourceExhausted)?);
    let volatility = match &node {
        ScalarNode::FunctionCall { volatility, .. } => Some(*volatility),
        _ => None,
    };
    // All fallible observed work precedes publication. These legacy index and
    // vector allocations still require a separate host resource admission.
    arena.nodes.push(node);
    arena.value_types.push(value_type);
    arena.function_volatility.push(volatility);
    arena.intern.entry(fingerprint).or_default().push(id);
    Ok(id)
}

pub(super) fn intern(
    arena: &mut ScalarArena,
    node: ScalarNode,
    value_type: FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<ScalarId, SqlCompileError> {
    intern_inner(arena, node, value_type, control, None)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "constant_interner_tests.rs"]
mod constant_interner_tests;
