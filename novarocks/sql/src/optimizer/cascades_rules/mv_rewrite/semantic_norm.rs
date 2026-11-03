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

//! Exact, observed MV expression identity. Bucket keys are not semantic proofs.
use crate::{
    column_id::ColumnId,
    common::{BinOp, UnOp},
    compiler::SqlCompileError,
    optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode},
};
use novarocks_functions::{ConstantError, ConstantValue};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{
    collections::HashMap,
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NormArgumentOrder {
    Ordered,
    Multiset,
    HeadThenMultiset,
}
#[derive(Clone, Debug)]
pub(crate) enum NormExpr {
    Column {
        name: String,
        value_type: FunctionValueType,
    },
    Constant {
        value: ConstantValue,
        value_type: FunctionValueType,
    },
    Call {
        name: String,
        value_type: FunctionValueType,
        distinct: bool,
        args: Vec<NormExpr>,
        binding: Option<crate::binding::SqlFunctionBinding>,
        decimal_overflow_policy: Option<novarocks_type_contract::DecimalOverflowPolicy>,
        order_by: Vec<NormSortKey>,
        argument_order: NormArgumentOrder,
    },
}
#[derive(Clone, Debug)]
pub(crate) struct NormSortKey {
    pub(crate) expr: NormExpr,
    pub(crate) asc: bool,
    pub(crate) nulls_first: bool,
}
impl NormExpr {
    pub(crate) fn value_type(&self) -> &FunctionValueType {
        match self {
            Self::Column { value_type, .. }
            | Self::Constant { value_type, .. }
            | Self::Call { value_type, .. } => value_type,
        }
    }
}
macro_rules! required {
    ($e:expr) => {
        match $e {
            Some(v) => v,
            None => return Ok(None),
        }
    };
}
fn checked<T>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, SqlCompileError>,
) -> Result<T, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = body(&mut work);
    if matches!(
        &result,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}
pub(crate) fn normalize(
    arena: &ScalarArena,
    expr: ScalarId,
    base_names: &HashMap<ColumnId, String>,
    control: &dyn PureCompileControl,
) -> Result<Option<NormExpr>, SqlCompileError> {
    checked(control, |work| {
        normalize_inner(arena, expr, base_names, control, work)
    })
}
fn normalize_args(
    arena: &ScalarArena,
    args: &[ScalarId],
    names: &HashMap<ColumnId, String>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Vec<NormExpr>>, SqlCompileError> {
    let mut out = Vec::new();
    for arg in args {
        out.push(required!(normalize_inner(
            arena, *arg, names, control, work
        )?));
    }
    Ok(Some(out))
}
fn normalize_order(
    arena: &ScalarArena,
    keys: &[crate::optimizer::scalar::SortKey],
    names: &HashMap<ColumnId, String>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<Vec<NormSortKey>>, SqlCompileError> {
    let mut out = Vec::new();
    for key in keys {
        work.step()?;
        out.push(NormSortKey {
            expr: required!(normalize_inner(arena, key.expr, names, control, work)?),
            asc: key.asc,
            nulls_first: key.nulls_first,
        });
    }
    Ok(Some(out))
}
fn normalize_inner(
    arena: &ScalarArena,
    expr: ScalarId,
    base_names: &HashMap<ColumnId, String>,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<NormExpr>, SqlCompileError> {
    work.step()?;
    let value_type = arena.value_type(expr).clone();
    let call = |name: &str, args: Vec<NormExpr>| NormExpr::Call {
        value_type: value_type.clone(),
        argument_order: NormArgumentOrder::Ordered,
        name: name.to_string(),
        distinct: false,
        args,
        binding: None,
        decimal_overflow_policy: None,
        order_by: vec![],
    };
    Ok(Some(match arena.node(expr) {
        ScalarNode::ColumnRef(column_id) => NormExpr::Column {
            name: required!(base_names.get(column_id)).clone(),
            value_type: value_type.clone(),
        },
        ScalarNode::Literal(HashableLiteral(value)) => {
            work.flush()?;
            NormExpr::Constant {
                value: crate::constant::admit_syntax_constant(
                    value,
                    &value_type,
                    arena.constant_policy(),
                    control,
                )?,
                value_type: value_type.clone(),
            }
        }
        ScalarNode::Constant(value) => {
            if !value_type.exactly_equals_observed::<ConstantError>(value.value_type(), || {
                work.step().map_err(Into::into)
            })? {
                return Err(SqlCompileError::InvalidRequest(
                    "MV constant source differs from its frozen type".into(),
                ));
            }
            NormExpr::Constant {
                value: value.clone(),
                value_type: value_type.clone(),
            }
        }
        ScalarNode::BinaryOp {
            left,
            op,
            right,
            decimal_overflow_policy,
        } => {
            let mut l = required!(normalize_inner(arena, *left, base_names, control, work)?);
            let mut r = required!(normalize_inner(arena, *right, base_names, control, work)?);
            // Canonicalize comparisons: Gt/Ge become flipped Lt/Le.
            let (name, commutative) = match op {
                BinOp::Add => ("add", true),
                BinOp::Mul => ("mul", true),
                BinOp::Sub => ("sub", false),
                BinOp::Div => ("div", false),
                BinOp::Mod => ("mod", false),
                BinOp::Eq => ("eq", true),
                BinOp::Ne => ("ne", true),
                BinOp::EqForNull => ("eq_for_null", true),
                BinOp::And => ("and", true),
                BinOp::Or => ("or", true),
                BinOp::Lt => ("lt", false),
                BinOp::Le => ("le", false),
                BinOp::Gt => {
                    std::mem::swap(&mut l, &mut r);
                    ("lt", false)
                }
                BinOp::Ge => {
                    std::mem::swap(&mut l, &mut r);
                    ("le", false)
                }
            };
            let args = vec![l, r];
            NormExpr::Call {
                value_type: value_type.clone(),
                argument_order: if commutative {
                    NormArgumentOrder::Multiset
                } else {
                    NormArgumentOrder::Ordered
                },
                name: name.to_string(),
                distinct: false,
                args,
                binding: None,
                decimal_overflow_policy: Some(*decimal_overflow_policy),
                order_by: vec![],
            }
        }
        ScalarNode::UnaryOp { op, child } => {
            let name = match op {
                UnOp::Not => "not",
                UnOp::Negate => "neg",
                UnOp::BitwiseNot => "bitnot",
            };
            call(
                name,
                vec![required!(normalize_inner(
                    arena, *child, base_names, control, work
                )?)],
            )
        }
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            binding,
            volatility,
        } => NormExpr::Call {
            value_type: value_type.clone(),
            argument_order: NormArgumentOrder::Ordered,
            name: format!("fn:{}", name.to_ascii_lowercase()),
            distinct: *distinct || volatility.is_volatile(),
            binding: Some(binding.clone()),
            decimal_overflow_policy: None,
            order_by: vec![],
            args: required!(normalize_args(arena, args, base_names, control, work)?),
        },
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            resolved,
            order_by,
        } => NormExpr::Call {
            value_type: value_type.clone(),
            argument_order: NormArgumentOrder::Ordered,
            name: format!("agg:{}", name.to_ascii_lowercase()),
            distinct: *distinct,
            binding: Some(resolved.clone()),
            decimal_overflow_policy: None,
            order_by: required!(normalize_order(arena, order_by, base_names, control, work)?),
            args: required!(normalize_args(arena, args, base_names, control, work)?),
        },
        ScalarNode::Cast {
            child,
            target,
            decimal_overflow_policy,
        } => {
            if !novarocks_type_contract::arrow_data_types_exact_with_profile_observed::<
                ConstantError,
            >(
                target,
                value_type.data_type(),
                novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
                || work.step().map_err(Into::into),
            )? {
                return Ok(None);
            }
            NormExpr::Call {
                value_type: value_type.clone(),
                argument_order: NormArgumentOrder::Ordered,
                name: "cast".into(),
                distinct: false,
                args: vec![required!(normalize_inner(
                    arena, *child, base_names, control, work
                )?)],
                binding: None,
                decimal_overflow_policy: Some(*decimal_overflow_policy),
                order_by: vec![],
            }
        }
        ScalarNode::IsNull { child, negated } => call(
            if *negated { "is_not_null" } else { "is_null" },
            vec![required!(normalize_inner(
                arena, *child, base_names, control, work
            )?)],
        ),
        ScalarNode::InList {
            child,
            list,
            negated,
        } => {
            let mut args = vec![required!(normalize_inner(
                arena, *child, base_names, control, work
            )?)];
            let items = required!(normalize_args(arena, list, base_names, control, work)?);

            args.extend(items);
            let mut result = call(if *negated { "not_in" } else { "in" }, args);
            if let NormExpr::Call { argument_order, .. } = &mut result {
                *argument_order = NormArgumentOrder::HeadThenMultiset;
            }
            result
        }
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => call(
            if *negated { "not_between" } else { "between" },
            vec![
                required!(normalize_inner(arena, *child, base_names, control, work)?),
                required!(normalize_inner(arena, *low, base_names, control, work)?),
                required!(normalize_inner(arena, *high, base_names, control, work)?),
            ],
        ),
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => call(
            if *negated { "not_like" } else { "like" },
            vec![
                required!(normalize_inner(arena, *child, base_names, control, work)?),
                required!(normalize_inner(arena, *pattern, base_names, control, work)?),
            ],
        ),
        ScalarNode::Nested(inner) => {
            let normalized = required!(normalize_inner(arena, *inner, base_names, control, work)?);
            if !value_type
                .exactly_equals_observed::<ConstantError>(normalized.value_type(), || {
                    work.step().map_err(Into::into)
                })?
            {
                return Ok(None);
            }
            normalized
        }
        // CASE [operand] WHEN .. THEN .. [ELSE ..] END. WHEN/THEN pair order
        // is semantically significant (first match wins), so args are NOT
        // sorted. Absent operand/else are encoded with distinct zero-arg
        // marker calls so `CASE WHEN c THEN a END` can never collide with
        // `CASE WHEN c THEN a ELSE b END`.
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut args = Vec::with_capacity(when_then.len() * 2 + 2);
            args.push(match operand {
                Some(op) => call(
                    "case_operand",
                    vec![required!(normalize_inner(
                        arena, *op, base_names, control, work
                    )?)],
                ),
                None => call("case_no_operand", vec![]),
            });
            for (when, then) in when_then {
                args.push(required!(normalize_inner(
                    arena, *when, base_names, control, work
                )?));
                args.push(required!(normalize_inner(
                    arena, *then, base_names, control, work
                )?));
            }
            args.push(match else_expr {
                Some(else_expr) => call(
                    "case_else",
                    vec![required!(normalize_inner(
                        arena, *else_expr, base_names, control, work
                    )?)],
                ),
                None => call("case_no_else", vec![]),
            });
            call("case", args)
        }
        // IsTruthValue / WindowCall / Lambda* / LambdaParamRef /
        // SubqueryPlaceholder: not normalizable here -> fail closed.
        _ => return Ok(None),
    }))
}

fn bytes_equal(
    a: &[u8],
    b: &[u8],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.chunks(1024).zip(b.chunks(1024)) {
        let same = a == b;
        work.step()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}
fn fingerprint(
    expr: &NormExpr,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u64, SqlCompileError> {
    work.step()?;
    let mut hash = DefaultHasher::new();
    let ty = expr.value_type();
    hash.write_u8(ty.logical_type() as u8);
    hash.write_u8(u8::from(ty.nullable()));
    hash.write_u64(
        novarocks_type_contract::arrow_data_type_fingerprint_with_profile_observed::<ConstantError>(
            ty.data_type(),
            novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
            &mut || work.step().map_err(Into::into),
        )?,
    );
    match expr {
        NormExpr::Column { name, .. } => {
            hash.write_u8(0);
            for chunk in name.as_bytes().chunks(1024) {
                hash.write(chunk);
                work.step()?;
            }
        }
        NormExpr::Constant { value, value_type } => {
            if !value_type.exactly_equals_observed::<ConstantError>(value.value_type(), || {
                work.step().map_err(Into::into)
            })? {
                return Err(SqlCompileError::InvalidRequest(
                    "MV constant source differs from its frozen type".into(),
                ));
            }
            hash.write_u8(1);
            work.flush()?;
            value
                .semantic_key_observed(CompilePhase::LowerProgram, control)?
                .hash(&mut hash);
        }
        NormExpr::Call {
            name,
            distinct,
            args,
            binding,
            decimal_overflow_policy,
            order_by,
            argument_order,
            ..
        } => {
            hash.write_u8(2);
            for chunk in name.as_bytes().chunks(1024) {
                hash.write(chunk);
                work.step()?;
            }
            hash.write_u8(u8::from(*distinct));
            hash.write_u8(*argument_order as u8);
            hash.write_u8(decimal_overflow_policy.map_or(0, |p| 1 + p as u8));
            hash.write_usize(args.len());
            hash.write_u8(u8::from(binding.is_some()));
            if let Some(binding) = binding {
                work.flush()?;
                hash.write_u64(binding.fingerprint_observed(CompilePhase::LowerProgram, control)?);
            }
            let mut multiset = 0u64;
            for (i, arg) in args.iter().enumerate() {
                let key = fingerprint(arg, control, work)?;
                if *argument_order == NormArgumentOrder::Ordered
                    || (*argument_order == NormArgumentOrder::HeadThenMultiset && i == 0)
                {
                    hash.write_u64(key);
                } else {
                    multiset = multiset.wrapping_add(key);
                }
            }
            if *argument_order != NormArgumentOrder::Ordered {
                hash.write_u64(multiset);
            }
            hash.write_usize(order_by.len());
            for key in order_by {
                work.step()?;
                hash.write_u8(u8::from(key.asc));
                hash.write_u8(u8::from(key.nulls_first));
                hash.write_u64(fingerprint(&key.expr, control, work)?);
            }
        }
    }
    Ok(hash.finish())
}
fn equal(
    a: &NormExpr,
    b: &NormExpr,
    control: &dyn PureCompileControl,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    if !a
        .value_type()
        .exactly_equals_observed::<ConstantError>(b.value_type(), || {
            work.step().map_err(Into::into)
        })?
    {
        return Ok(false);
    }
    match (a, b) {
        (NormExpr::Column { name: a, .. }, NormExpr::Column { name: b, .. }) => {
            bytes_equal(a.as_bytes(), b.as_bytes(), work)
        }
        (
            NormExpr::Constant {
                value: a,
                value_type: at,
            },
            NormExpr::Constant {
                value: b,
                value_type: bt,
            },
        ) => {
            for (value, ty) in [(a, at), (b, bt)] {
                if !ty.exactly_equals_observed::<ConstantError>(value.value_type(), || {
                    work.step().map_err(Into::into)
                })? {
                    return Err(SqlCompileError::InvalidRequest(
                        "MV constant source differs from its frozen type".into(),
                    ));
                }
            }
            work.flush()?;
            Ok(a.equals_observed(b, CompilePhase::LowerProgram, control)?)
        }
        (
            NormExpr::Call {
                name: an,
                distinct: ad,
                args: aa,
                binding: ab,
                decimal_overflow_policy: ap,
                order_by: ao,
                argument_order: ar,
                ..
            },
            NormExpr::Call {
                name: bn,
                distinct: bd,
                args: ba,
                binding: bb,
                decimal_overflow_policy: bp,
                order_by: bo,
                argument_order: br,
                ..
            },
        ) => {
            if ad != bd
                || ap != bp
                || ar != br
                || aa.len() != ba.len()
                || ao.len() != bo.len()
                || !bytes_equal(an.as_bytes(), bn.as_bytes(), work)?
            {
                return Ok(false);
            }
            match (ab, bb) {
                (None, None) => {}
                (Some(a), Some(b)) => {
                    work.flush()?;
                    if !a.equals_observed(b, CompilePhase::LowerProgram, control)? {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
            for (a, b) in ao.iter().zip(bo) {
                work.step()?;
                if a.asc != b.asc
                    || a.nulls_first != b.nulls_first
                    || !equal(&a.expr, &b.expr, control, work)?
                {
                    return Ok(false);
                }
            }
            let head = match ar {
                NormArgumentOrder::Ordered => aa.len(),
                NormArgumentOrder::Multiset => 0,
                NormArgumentOrder::HeadThenMultiset => usize::from(!aa.is_empty()),
            };
            for (a, b) in aa[..head].iter().zip(&ba[..head]) {
                if !equal(a, b, control, work)? {
                    return Ok(false);
                }
            }
            // Consume each exact right-hand match once; duplicate multiplicity matters.
            let mut used = vec![false; ba.len() - head];
            for a in &aa[head..] {
                let mut found = false;
                for (i, b) in ba[head..].iter().enumerate() {
                    work.step()?;
                    if !used[i] && equal(a, b, control, work)? {
                        used[i] = true;
                        found = true;
                        break;
                    }
                }
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}
/// Finite coarse buckets with observed exact collision checks. No derived CV Hash/Eq.
pub(crate) struct NormIndex<V> {
    buckets: HashMap<u64, Vec<(NormExpr, V)>>,
}
impl<V> NormIndex<V> {
    pub(crate) fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }
    pub(crate) fn insert(
        &mut self,
        expr: NormExpr,
        value: V,
        control: &dyn PureCompileControl,
    ) -> Result<(), SqlCompileError> {
        let (key, replace) = checked(control, |work| {
            let key = fingerprint(&expr, control, work)?;
            let mut replace = None;
            if let Some(bucket) = self.buckets.get(&key) {
                for (i, (candidate, _)) in bucket.iter().enumerate() {
                    work.step()?;
                    if equal(candidate, &expr, control, work)? {
                        replace = Some(i);
                        break;
                    }
                }
            }
            Ok((key, replace))
        })?;
        let bucket = self.buckets.entry(key).or_default();
        if let Some(i) = replace {
            bucket[i] = (expr, value);
        } else {
            bucket.push((expr, value));
        }
        Ok(())
    }
    pub(crate) fn get(
        &self,
        expr: &NormExpr,
        control: &dyn PureCompileControl,
    ) -> Result<Option<&V>, SqlCompileError> {
        checked(control, |work| {
            let key = fingerprint(expr, control, work)?;
            if let Some(bucket) = self.buckets.get(&key) {
                for (candidate, value) in bucket {
                    work.step()?;
                    if equal(candidate, expr, control, work)? {
                        return Ok(Some(value));
                    }
                }
            }
            Ok(None)
        })
    }
}
pub(crate) fn norm_contains(
    values: &[NormExpr],
    value: &NormExpr,
    control: &dyn PureCompileControl,
) -> Result<bool, SqlCompileError> {
    checked(control, |work| {
        for candidate in values {
            work.step()?;
            if equal(candidate, value, control, work)? {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

// Test assertions use the same observed equality as production consumers.
#[cfg(test)]
impl PartialEq for NormExpr {
    fn eq(&self, other: &Self) -> bool {
        let control = crate::compiler::SqlCompileControl::unbounded();
        checked(&control, |work| equal(self, other, &control, work)).unwrap()
    }
}

#[cfg(test)]
#[path = "semantic_norm_tests.rs"]
mod tests;
