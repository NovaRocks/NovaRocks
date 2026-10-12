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

//! Borrowed complete binding facts shared by bucketing and exact comparison.
//! Traversal scratch and immutable handles are not a host allocation grant.

use super::SqlFunctionBinding;
use crate::compiler::SqlCompileError;
use novarocks_functions::{ConstantError, FunctionArgumentType, FunctionResultType};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{collections::hash_map::DefaultHasher, hash::Hasher};

pub(crate) enum BindingField<'a> {
    Number(u128),
    Bytes(&'a [u8]),
    ValueType(&'a FunctionValueType),
}

// Keep this order identical to the scalar interner's original token author.
pub(crate) fn visit_fields<'a, E>(
    binding: &'a SqlFunctionBinding,
    mut visit: impl FnMut(BindingField<'a>) -> Result<(), E>,
) -> Result<(), E> {
    use BindingField::{Bytes, Number, ValueType};
    visit(Number(binding.decimal_overflow_policy() as u128))?;
    visit(Bytes(binding.function_id.as_str().as_bytes()))?;
    visit(Number(binding.kind as u128))?;
    visit(Number(binding.semantics.volatility as u128))?;
    visit(Number(binding.semantics.argument_evaluation as u128))?;
    visit(Number(binding.semantics.failure_behavior as u128))?;
    visit(Number(binding.semantics.intrinsic_row_error as u128))?;
    visit(Number(binding.logical_argument_count as u128))?;
    visit(Bytes(binding.selected.overload.as_str().as_bytes()))?;
    visit(Number(binding.selected.argument_types.len() as u128))?;
    for ty in &binding.selected.argument_types {
        match ty {
            FunctionArgumentType::Value(ty) => {
                visit(Number(0))?;
                visit(ValueType(ty))?;
            }
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                visit(Number(1))?;
                visit(Number(parameter_types.len() as u128))?;
                for ty in parameter_types {
                    visit(ValueType(ty))?;
                }
                visit(ValueType(result_type))?;
            }
        }
    }
    match &binding.selected.result_type {
        FunctionResultType::Scalar(ty) => {
            visit(Number(0))?;
            visit(ValueType(ty))?;
        }
        FunctionResultType::Relation(types) => {
            visit(Number(1))?;
            visit(Number(types.len() as u128))?;
            for ty in types {
                visit(ValueType(ty))?;
            }
        }
    }
    visit(Number(u128::from(binding.selected.aggregate.is_some())))?;
    if let Some(aggregate) = &binding.selected.aggregate {
        visit(ValueType(&aggregate.intermediate_type))?;
        visit(Bytes(aggregate.state_format.as_str().as_bytes()))?;
    }
    if binding.declared_scalar_invocation_data {
        // Positive facts append a token; old undeclared bindings retain their
        // exact original traversal and callback sequence.
        visit(Number(u128::MAX))?;
    }
    Ok(())
}

fn checked<T>(
    phase: CompilePhase,
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, SqlCompileError>,
) -> Result<T, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
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

impl SqlFunctionBinding {
    /// Process-local bucket only; callers must use observed exact equality.
    pub(crate) fn fingerprint_observed(
        &self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<u64, SqlCompileError> {
        checked(phase, control, |work| {
            let mut hash = DefaultHasher::new();
            visit_fields(self, |field| {
                match field {
                    BindingField::Number(value) => {
                        hash.write_u8(0);
                        hash.write_u128(value);
                        work.step()?;
                    }
                    BindingField::Bytes(bytes) => {
                        hash.write_u8(1);
                        hash.write_usize(bytes.len());
                        work.step()?;
                        for chunk in bytes.chunks(1024) {
                            hash.write(chunk);
                            work.step()?;
                        }
                    }
                    BindingField::ValueType(ty) => {
                        hash.write_u8(2);
                        hash.write_u8(ty.logical_type as u8);
                        hash.write_u8(u8::from(ty.nullable));
                        work.step()?;
                        let carrier =
                            novarocks_type_contract::arrow_data_type_fingerprint_observed::<
                                ConstantError,
                            >(&ty.data_type, &mut || {
                                work.step().map_err(Into::into)
                            })
                            .map_err(SqlCompileError::from)?;
                        hash.write_u64(carrier);
                        work.step()?;
                    }
                }
                Ok::<_, SqlCompileError>(())
            })?;
            Ok(hash.finish())
        })
    }

    pub(crate) fn equals_observed(
        &self,
        other: &Self,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<bool, SqlCompileError> {
        checked(phase, control, |work| {
            // Borrow references only; no binding/type/backing clone or Debug.
            let mut left = Vec::new();
            let mut right = Vec::new();
            visit_fields(self, |field| {
                left.push(field);
                work.step().map_err(SqlCompileError::from)
            })?;
            visit_fields(other, |field| {
                right.push(field);
                work.step().map_err(SqlCompileError::from)
            })?;
            let same_length = left.len() == right.len();
            work.step()?;
            if !same_length {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(&right) {
                let same = match (left, right) {
                    (BindingField::Number(a), BindingField::Number(b)) => {
                        let same = a == b;
                        work.step()?;
                        same
                    }
                    (BindingField::Bytes(a), BindingField::Bytes(b)) => {
                        let same_length = a.len() == b.len();
                        work.step()?;
                        if !same_length {
                            return Ok(false);
                        }
                        for (a, b) in a.chunks(1024).zip(b.chunks(1024)) {
                            let same = a == b;
                            work.step()?;
                            if !same {
                                return Ok(false);
                            }
                        }
                        true
                    }
                    (BindingField::ValueType(a), BindingField::ValueType(b)) => a
                        .exactly_equals_observed::<ConstantError>(b, || {
                            work.step().map_err(Into::into)
                        })
                        .map_err(SqlCompileError::from)?,
                    _ => {
                        work.step()?;
                        false
                    }
                };
                if !same {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
