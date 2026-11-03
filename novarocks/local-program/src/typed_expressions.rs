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

//! Mandatory complete definition types owned with the same resolved program.
//! This checks carrier/signature correspondence and the primitive type rules
//! authored here, not lexical capture closure or correct physical lowering.
//! Source NULL/logical facts are explicit; legacy optional schemas never fill
//! an absent entry. A TruthOnly use does not change its definition's value type.

use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::{
    MAX_STATIC_EXPRESSIONS, ProgramCallSite, ProgramExprId, ProgramExpressionArena,
    ProgramResolvedCalls, StaticExprKind,
};
use novarocks_functions::{
    FunctionArgumentType, FunctionResultType, FunctionValueType, KernelFailure,
    validate_function_value_type_observed,
};
#[cfg(test)]
use novarocks_type_contract::ValueSchemaCreation;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, FunctionArgumentTypes,
    PureCompileControl, ValueLogicalType, ValueSchemaCreationScope, ValueTypeError,
    ValueTypeStorageLifetime, arrow_data_types_exact_with_profile_observed,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Clone, Debug)]
pub struct ProgramTypedExpressions {
    calls: ProgramResolvedCalls,
    types: Arc<BTreeMap<ProgramExpressionArena, FunctionArgumentTypes>>,
    _storage: ValueTypeStorageLifetime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramExpressionTypeError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Kernel(KernelFailure),
    TooManyDefinitions,
    IncompleteCoverage,
    WrongKind,
    WrongLambda,
    TypeMismatch,
    WrongDemand,
}
impl From<CompileControlError> for ProgramExpressionTypeError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl From<ValueTypeError> for ProgramExpressionTypeError {
    fn from(value: ValueTypeError) -> Self {
        Self::ValueType(value)
    }
}
impl fmt::Display for ProgramExpressionTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid complete local expression types: {self:?}")
    }
}
impl std::error::Error for ProgramExpressionTypeError {}

impl<H> From<ProgramExpressionTypeError> for ProgramCreationFailure<ProgramExpressionTypeError, H> {
    fn from(error: ProgramExpressionTypeError) -> Self {
        match error {
            ProgramExpressionTypeError::Control(error) => Self::Control(error),
            other => Self::Owner(other),
        }
    }
}

fn pure_type_creation_failure(
    error: ProgramCreationFailure<ProgramExpressionTypeError, std::convert::Infallible>,
) -> ProgramExpressionTypeError {
    match error {
        ProgramCreationFailure::Control(error) => ProgramExpressionTypeError::Control(error),
        ProgramCreationFailure::Owner(error) => error,
        ProgramCreationFailure::Resource(_) | ProgramCreationFailure::Capacity(_) => {
            ProgramExpressionTypeError::Control(CompileControlError::ResourceExhausted)
        }
        ProgramCreationFailure::Creator(never) => match never {},
    }
}

impl ProgramTypedExpressions {
    pub fn try_new(
        calls: ProgramResolvedCalls,
        types: BTreeMap<ProgramExpressionArena, Vec<FunctionArgumentType>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramExpressionTypeError> {
        Self::try_new_created::<Vec<FunctionArgumentType>, std::convert::Infallible>(
            calls,
            types,
            None,
            None,
            control,
            |entries| FunctionArgumentTypes::from_pure_boxed(entries.into_boxed_slice()),
        )
        .map_err(pure_type_creation_failure)
    }
    /// The pure compiler may already own independently guarded arena facts.
    /// Share those original owners without copying them into temporary Vecs.
    pub fn try_new_guarded(
        calls: ProgramResolvedCalls,
        types: BTreeMap<ProgramExpressionArena, FunctionArgumentTypes>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramExpressionTypeError> {
        Self::try_new_created::<FunctionArgumentTypes, std::convert::Infallible>(
            calls,
            types,
            None,
            None,
            control,
            |entries| entries,
        )
        .map_err(pure_type_creation_failure)
    }

    /// The original complete definition checks and control sequence are shared
    /// with the pure author. Only actual construction uses these borrowed ports.
    pub fn try_new_with_creator_in<H>(
        calls: ProgramResolvedCalls,
        types: BTreeMap<ProgramExpressionArena, FunctionArgumentTypes>,
        temporary: &mut dyn ValueSchemaCreationScope<H>,
        retained: &mut dyn ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramExpressionTypeError, H>> {
        Self::try_new_created(
            calls,
            types,
            Some(temporary),
            Some(retained),
            control,
            |entries| entries,
        )
    }
    fn try_new_created<T: AsRef<[FunctionArgumentType]>, H>(
        calls: ProgramResolvedCalls,
        types: BTreeMap<ProgramExpressionArena, T>,
        temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        retained: Option<&mut dyn ValueSchemaCreationScope<H>>,
        control: &dyn PureCompileControl,
        publish: impl FnMut(T) -> FunctionArgumentTypes,
    ) -> Result<Self, ProgramCreationFailure<ProgramExpressionTypeError, H>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let mut temporary = ProgramCreation { creator: temporary };
        let mut retained = ProgramCreation { creator: retained };
        let result = Self::try_new_core(
            calls,
            types,
            &mut temporary,
            &mut retained,
            &mut work,
            publish,
        );
        // A primary construction or control refusal performs no later work.
        if matches!(
            result,
            Err(ProgramCreationFailure::Control(_)
                | ProgramCreationFailure::Creator(_)
                | ProgramCreationFailure::Resource(_)
                | ProgramCreationFailure::Capacity(_))
        ) {
            return result;
        }
        work.finish()?;
        result
    }
    fn try_new_core<T: AsRef<[FunctionArgumentType]>, H>(
        calls: ProgramResolvedCalls,
        types: BTreeMap<ProgramExpressionArena, T>,
        temporary: &mut ProgramCreation<'_, H>,
        retained: &mut ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
        mut publish: impl FnMut(T) -> FunctionArgumentTypes,
    ) -> Result<Self, ProgramCreationFailure<ProgramExpressionTypeError, H>> {
        let arenas = calls.snapshot().roots().arenas();
        if types.len() != arenas.len() {
            return Err(ProgramExpressionTypeError::IncompleteCoverage.into());
        }
        // All actual definition positions share one local arena-entry budget.
        // Reusing an Arc under a different scope does not erase that position.
        let mut count = 0usize;
        for (arena, entries) in &types {
            let entries = entries.as_ref();
            work.step()?;
            let definitions = arenas
                .get(arena)
                .ok_or(ProgramExpressionTypeError::IncompleteCoverage)?;
            if definitions.nodes().len() != entries.len() {
                return Err(ProgramExpressionTypeError::IncompleteCoverage.into());
            }
            count = count
                .checked_add(entries.len())
                .filter(|n| *n <= MAX_STATIC_EXPRESSIONS)
                .ok_or(ProgramExpressionTypeError::TooManyDefinitions)?;
        }
        for (arena, entries) in &types {
            let entries = entries.as_ref();
            let definitions = &arenas[arena];
            for (definition, ty) in definitions.nodes().iter().zip(entries) {
                work.step()?;
                match (definition.kind(), ty) {
                    (
                        StaticExprKind::LambdaFunction {
                            body, arg_slots, ..
                        },
                        FunctionArgumentType::Lambda {
                            parameter_types,
                            result_type,
                        },
                    ) => {
                        if arg_slots.len() != parameter_types.len() {
                            return Err(ProgramExpressionTypeError::WrongLambda.into());
                        }
                        validate(result_type, work)?;
                        same_carrier(definition.data_type(), result_type.data_type(), work)?;
                        if let Some(authored) = definition.value_type() {
                            same_value(authored, result_type, false, work)?;
                        }
                        let Some(FunctionArgumentType::Value(body_type)) =
                            entries.get(body.index())
                        else {
                            return Err(ProgramExpressionTypeError::WrongLambda.into());
                        };
                        same_value(body_type, result_type, false, work)?;
                        for parameter in parameter_types {
                            validate(parameter, work)?;
                        }
                    }
                    (StaticExprKind::LambdaFunction { .. }, _)
                    | (_, FunctionArgumentType::Lambda { .. }) => {
                        return Err(ProgramExpressionTypeError::WrongKind.into());
                    }
                    (_, FunctionArgumentType::Value(value)) => {
                        validate(value, work)?;
                        same_carrier(definition.data_type(), value.data_type(), work)?;
                        if let Some(authored) = definition.value_type() {
                            same_value(authored, value, false, work)?;
                        }
                        if let StaticExprKind::Constant(constant) = definition.kind() {
                            same_value(constant.value_type(), value, false, work)?;
                        }
                        if let StaticExprKind::PreparedCast {
                            operation,
                            child,
                            decimal_overflow_policy,
                            allow_throw_exception,
                        } = definition.kind()
                        {
                            let Some(FunctionArgumentType::Value(source)) =
                                entries.get(child.index())
                            else {
                                return Err(ProgramExpressionTypeError::WrongKind.into());
                            };
                            if ((source.nullable() || *source.data_type() == arrow_schema::DataType::Null) && !value.nullable())
                                || !novarocks_type_contract::preserves_value_domains_observed(
                                    source.value_type(), value.value_type(),
                                    |_| work.step().map_err(ProgramExpressionTypeError::Control),
                                )?
                                || !novarocks_type_contract::decimal_error_policy_cast_supported_observed(
                                    source.data_type(), value.data_type(), *decimal_overflow_policy,
                                    || work.step().map_err(ProgramExpressionTypeError::Control),
                                )? {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            // Successful NULLs share the primitive cast semantic author;
                            // this does not require a dead definition to be runtime-installed.
                            if *operation == novarocks_functions::CastOperation::Carrier
                                && novarocks_functions::carrier_cast_can_produce_null_with_policy(
                                    source.data_type(),
                                    value.data_type(),
                                    *decimal_overflow_policy,
                                    *allow_throw_exception,
                                )
                                && !value.nullable()
                            {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            work.step()?;
                        }
                        if let StaticExprKind::PreparedLike {
                            text,
                            pattern,
                            negated,
                        } = definition.kind()
                        {
                            let source = |id: ProgramExprId| match entries.get(id.index()) {
                                Some(FunctionArgumentType::Value(value)) => Ok(value),
                                _ => Err(ProgramExpressionTypeError::WrongKind),
                            };
                            work.flush()?;
                            novarocks_functions::PreparedNativeLikeRecipe::try_new(
                                *negated,
                                source(*text)?,
                                source(*pattern)?,
                                value,
                                work.control(),
                            )
                            .map_err(|error| {
                                match error.control_error() {
                                    Some(cause) => ProgramExpressionTypeError::Control(cause),
                                    None => ProgramExpressionTypeError::TypeMismatch,
                                }
                            })?;
                            work.step()?;
                        }
                        if let StaticExprKind::PreparedInList {
                            child,
                            values,
                            is_not_in,
                        } = definition.kind()
                        {
                            let source = |id: ProgramExprId| match entries.get(id.index()) {
                                Some(FunctionArgumentType::Value(value)) => Ok(value),
                                _ => Err(ProgramExpressionTypeError::WrongKind),
                            };
                            let mut candidates = temporary
                                .reserve::<ProgramExpressionTypeError, &FunctionValueType>(
                                    values.len(),
                                    work,
                                )?;
                            for id in values {
                                candidates.push(source(*id)?);
                                work.step()?;
                            }
                            work.flush()?;
                            if let Some(creation) = temporary.creator.as_deref_mut() {
                                novarocks_functions::PreparedNativeInListRecipe::try_new_with_creator_in(
                                    *is_not_in, source(*child)?, &candidates, value,
                                    creation, work.control(),
                                ).map_err(|error| match error {
                                    novarocks_functions::NativeInListCreationFailure::Control(e) =>
                                        ProgramCreationFailure::Control(e),
                                    novarocks_functions::NativeInListCreationFailure::Creator(e) =>
                                        ProgramCreationFailure::Creator(e),
                                    novarocks_functions::NativeInListCreationFailure::Resource(e) =>
                                        ProgramCreationFailure::Resource(e),
                                    novarocks_functions::NativeInListCreationFailure::Owner(_) =>
                                        ProgramCreationFailure::Owner(ProgramExpressionTypeError::TypeMismatch),
                                })?;
                            } else {
                                novarocks_functions::PreparedNativeInListRecipe::try_new(
                                    *is_not_in,
                                    source(*child)?,
                                    &candidates,
                                    value,
                                    work.control(),
                                )
                                .map_err(|error| {
                                    match error.control_error() {
                                        Some(cause) => ProgramExpressionTypeError::Control(cause),
                                        None => ProgramExpressionTypeError::TypeMismatch,
                                    }
                                })?;
                            }
                            work.step()?;
                        }
                        if let StaticExprKind::PreparedBetween {
                            plan,
                            operand,
                            low,
                            high,
                        } = definition.kind()
                        {
                            let source = |id: ProgramExprId| match entries.get(id.index()) {
                                Some(FunctionArgumentType::Value(value)) => Ok(value),
                                _ => Err(ProgramExpressionTypeError::WrongKind),
                            };
                            work.flush()?;
                            novarocks_functions::PreparedNativeBetweenRecipe::try_new(
                                *plan,
                                source(*operand)?,
                                source(*low)?,
                                source(*high)?,
                                value,
                                work.control(),
                            )
                            .map_err(|error| {
                                match error.control_error() {
                                    Some(cause) => ProgramExpressionTypeError::Control(cause),
                                    None => ProgramExpressionTypeError::TypeMismatch,
                                }
                            })?;
                            work.step()?;
                        }
                        if let StaticExprKind::PreparedNativeNegate(child) = definition.kind() {
                            let source = match entries.get(child.index()) {
                                Some(FunctionArgumentType::Value(value)) => value,
                                _ => return Err(ProgramExpressionTypeError::WrongKind.into()),
                            };
                            work.flush()?;
                            novarocks_functions::PreparedNativeNegateRecipe::try_new(
                                source,
                                value,
                                work.control(),
                            )
                            .map_err(|error| {
                                match error.control_error() {
                                    Some(cause) => ProgramExpressionTypeError::Control(cause),
                                    None => ProgramExpressionTypeError::TypeMismatch,
                                }
                            })?;
                            work.step()?;
                        }
                        if let StaticExprKind::PreparedNativeBitNot(child) = definition.kind() {
                            let source = match entries.get(child.index()) {
                                Some(FunctionArgumentType::Value(value)) => value,
                                _ => return Err(ProgramExpressionTypeError::WrongKind.into()),
                            };
                            work.flush()?;
                            novarocks_functions::PreparedNativeBitNotRecipe::try_new(
                                source,
                                value,
                                work.control(),
                            )
                            .map_err(|error| {
                                match error.control_error() {
                                    Some(cause) => ProgramExpressionTypeError::Control(cause),
                                    None => ProgramExpressionTypeError::TypeMismatch,
                                }
                            })?;
                            work.step()?;
                        }
                        if let StaticExprKind::PreparedArithmetic {
                            operator,
                            left,
                            right,
                            ..
                        } = definition.kind()
                        {
                            let value_at = |id: ProgramExprId| match entries.get(id.index()) {
                                Some(FunctionArgumentType::Value(value)) => Ok(value),
                                _ => Err(ProgramExpressionTypeError::WrongKind),
                            };
                            work.step()?;
                            let (expected_carrier, expected_domain) =
                                novarocks_type_contract::arithmetic_result_value_shape_with_op(
                                    value_at(*left)?,
                                    value_at(*right)?,
                                    *operator,
                                )
                                .ok_or(ProgramExpressionTypeError::TypeMismatch)?;
                            if value.logical_type() != expected_domain || !value.nullable() {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            same_carrier(value.data_type(), &expected_carrier, work)?;
                        }
                        if let Some((_, left, right)) = definition.kind().ordinary_comparison() {
                            validate_comparison_types(left, right, value, entries, work)?;
                        }
                        if let StaticExprKind::EqForNull(left, right)
                        | StaticExprKind::PreparedNullSafeComparison { left, right } =
                            definition.kind()
                        {
                            // This is a generic type obligation for every definition;
                            // the pure recipe separately authors runtime capability.
                            let value_at = |id: ProgramExprId| match entries.get(id.index()) {
                                Some(FunctionArgumentType::Value(value)) => Ok(value),
                                _ => Err(ProgramExpressionTypeError::WrongKind),
                            };
                            let left = value_at(*left)?;
                            let right = value_at(*right)?;
                            if *value.data_type() != arrow_schema::DataType::Boolean
                                || value.logical_type() != ValueLogicalType::Physical
                                || value.nullable()
                            {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            work.step()?;
                            same_binding_ignoring_root_nullability(left, right, work)?;
                        }
                        if let StaticExprKind::Case {
                            has_case_expr,
                            has_else_expr,
                            children,
                        } = definition.kind()
                        {
                            validate_case_types(
                                *has_case_expr,
                                *has_else_expr,
                                children,
                                value,
                                entries,
                                work,
                            )?;
                        }
                        if let StaticExprKind::Not(argument)
                        | StaticExprKind::IsNull(argument)
                        | StaticExprKind::IsNotNull(argument) = definition.kind()
                        {
                            if *value.data_type() != arrow_schema::DataType::Boolean
                                || value.logical_type() != ValueLogicalType::Physical
                            {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            let Some(FunctionArgumentType::Value(operand)) =
                                entries.get(argument.index())
                            else {
                                return Err(ProgramExpressionTypeError::WrongKind.into());
                            };
                            if matches!(definition.kind(), StaticExprKind::Not(_)) {
                                if !novarocks_type_contract::supports_boolean_negation_value_type(
                                    operand,
                                ) || (!value.nullable() && operand.nullable())
                                {
                                    return Err(ProgramExpressionTypeError::TypeMismatch.into());
                                }
                            } else if value.nullable() {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            work.step()?;
                        }
                        if let StaticExprKind::NaryAnd { args } | StaticExprKind::NaryOr { args } =
                            definition.kind()
                        {
                            if *value.data_type() != arrow_schema::DataType::Boolean
                                || value.logical_type() != ValueLogicalType::Physical
                            {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                            let deciding =
                                matches!(definition.kind(), StaticExprKind::NaryOr { .. });
                            let mut nullable = false;
                            let mut constant_decides = false;
                            for argument in args {
                                work.step()?;
                                let Some(FunctionArgumentType::Value(operand)) =
                                    entries.get(argument.index())
                                else {
                                    return Err(ProgramExpressionTypeError::WrongKind.into());
                                };
                                if *operand.data_type() != arrow_schema::DataType::Boolean
                                    || operand.logical_type() != ValueLogicalType::Physical
                                {
                                    return Err(ProgramExpressionTypeError::TypeMismatch.into());
                                }
                                nullable |= operand.nullable();
                                if let StaticExprKind::Constant(constant) = definitions
                                    .node(*argument)
                                    .expect("checked operand reference")
                                    .kind()
                                {
                                    constant_decides |= constant
                                        .try_boolean()
                                        .map_err(|_| ProgramExpressionTypeError::TypeMismatch)?
                                        == Some(deciding);
                                    work.step()?;
                                }
                            }
                            // A conservative nullable declaration is retained.
                            // A narrower one requires this author's actual
                            // constant deciding-value proof, not demand coercion.
                            if !value.nullable() && nullable && !constant_decides {
                                return Err(ProgramExpressionTypeError::TypeMismatch.into());
                            }
                        }
                    }
                }
            }
        }
        for (arena, flow) in calls.snapshot().flows() {
            let entries = types[arena].as_ref();
            for invocation in flow.uses().values() {
                work.step()?;
                let result = match &entries[invocation.definition.index()] {
                    FunctionArgumentType::Value(value) => value,
                    // LambdaBody demand belongs to its final body result;
                    // the wrapper itself is a lexical callable, not a value.
                    FunctionArgumentType::Lambda { result_type, .. } => result_type,
                };
                if invocation.context.demand == EvaluationDemand::TruthOnly
                    && (*result.data_type() != arrow_schema::DataType::Boolean
                        || result.logical_type() != ValueLogicalType::Physical)
                {
                    return Err(ProgramExpressionTypeError::WrongDemand.into());
                }
            }
        }
        for (site, resolved) in calls.calls() {
            work.step()?;
            let ProgramCallSite::Expression(occurrence) = site else {
                continue;
            };
            let flow = &calls.snapshot().flows()[&occurrence.arena];
            let invocation = &flow.uses()[&occurrence.use_id];
            let definitions = &arenas[&occurrence.arena];
            let entries = types[&occurrence.arena].as_ref();
            let definition = definitions
                .node(invocation.definition)
                .expect("resolved occurrence was checked");
            let args = match definition.kind() {
                StaticExprKind::FunctionCall { args, .. } | StaticExprKind::BoundCall { args } => {
                    args
                }
                _ => unreachable!("checked call site"),
            };
            let call = resolved.call_contract();
            let FunctionResultType::Scalar(result) = &call.selected().result_type else {
                return Err(ProgramExpressionTypeError::WrongKind.into());
            };
            let FunctionArgumentType::Value(actual_result) =
                &entries[invocation.definition.index()]
            else {
                return Err(ProgramExpressionTypeError::WrongKind.into());
            };
            same_value(actual_result, result, false, work)?;
            for (argument, expected) in args.iter().zip(&call.selected().argument_types) {
                work.step()?;
                match (&entries[argument.index()], expected) {
                    (
                        FunctionArgumentType::Value(actual),
                        FunctionArgumentType::Value(expected),
                    ) => {
                        same_value(actual, expected, true, work)?;
                    }
                    (
                        FunctionArgumentType::Lambda {
                            parameter_types: actual_parameters,
                            result_type: actual_result,
                        },
                        FunctionArgumentType::Lambda {
                            parameter_types: expected_parameters,
                            result_type: expected_result,
                        },
                    ) => {
                        if actual_parameters.len() != expected_parameters.len() {
                            return Err(ProgramExpressionTypeError::WrongLambda.into());
                        }
                        for (actual, expected) in actual_parameters.iter().zip(expected_parameters)
                        {
                            same_value(actual, expected, false, work)?;
                        }
                        same_value(actual_result, expected_result, false, work)?;
                    }
                    _ => return Err(ProgramExpressionTypeError::WrongKind.into()),
                }
            }
        }
        let mut created = BTreeMap::new();
        for (arena, entries) in types {
            // A strong producer already authored this independent arena
            // collection. Publishing it transfers its original finite hold,
            // never the table's allowance or a re-created subtree.
            let entries = publish(entries);
            retained.before_tree_insert::<ProgramExpressionTypeError, ProgramExpressionArena,
                FunctionArgumentTypes>(created.len(), work)?;
            created.insert(arena, entries);
        }
        let storage = retained.storage();
        let types = retained.arc::<ProgramExpressionTypeError, _>(created, work)?;
        Ok(Self {
            calls,
            types,
            _storage: storage,
        })
    }
    pub const fn resolved_calls(&self) -> &ProgramResolvedCalls {
        &self.calls
    }
    pub fn types(&self) -> &BTreeMap<ProgramExpressionArena, FunctionArgumentTypes> {
        &self.types
    }
    pub fn definition_type(
        &self,
        arena: ProgramExpressionArena,
        definition: ProgramExprId,
    ) -> Option<&FunctionArgumentType> {
        self.types.get(&arena)?.get(definition.index())
    }
}
/// Definition typing applies even without an evaluation occurrence. Runtime
/// recipes independently admit their implemented carriers; that capability
/// must not decide whether a static expression has a coherent value domain.
/// This check covers root SQL NULL. Nested successful-NULL capabilities still
/// require their comparison author before those recipes can be admitted.
fn validate_comparison_types(
    left: ProgramExprId,
    right: ProgramExprId,
    result: &FunctionValueType,
    types: &[FunctionArgumentType],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    let value = |id: ProgramExprId| -> Result<&FunctionValueType, ProgramExpressionTypeError> {
        match types.get(id.index()) {
            Some(FunctionArgumentType::Value(value)) => Ok(value),
            _ => Err(ProgramExpressionTypeError::WrongKind),
        }
    };
    let left = value(left)?;
    let right = value(right)?;
    if *result.data_type() != arrow_schema::DataType::Boolean
        || result.logical_type() != ValueLogicalType::Physical
        || (!result.nullable()
            && (left.nullable()
                || right.nullable()
                || *left.data_type() == arrow_schema::DataType::Null
                || *right.data_type() == arrow_schema::DataType::Null))
    {
        return Err(ProgramExpressionTypeError::TypeMismatch);
    }
    work.step()?;
    same_binding_ignoring_root_nullability(left, right, work)
}

/// CASE consumes only values. Its common result domain is frozen by the
/// source type author; this check neither coerces operands nor admits a runtime
/// carrier implementation. Simple labels ignore only outer NULL admission.
fn validate_case_types(
    simple: bool,
    has_else: bool,
    children: &[ProgramExprId],
    result: &FunctionValueType,
    types: &[FunctionArgumentType],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    let offset = usize::from(simple);
    let pairs = children
        .len()
        .checked_sub(offset + usize::from(has_else))
        .filter(|count| *count >= 2 && count % 2 == 0)
        .ok_or(ProgramExpressionTypeError::TypeMismatch)?;
    work.step()?;
    let value = |id: ProgramExprId| -> Result<&FunctionValueType, ProgramExpressionTypeError> {
        match types.get(id.index()) {
            Some(FunctionArgumentType::Value(value)) => Ok(value),
            _ => Err(ProgramExpressionTypeError::WrongKind),
        }
    };
    let operand = if simple {
        Some(value(children[0])?)
    } else {
        None
    };
    for arm in 0..pairs / 2 {
        let when = value(children[offset + arm * 2])?;
        if let Some(operand) = operand {
            same_binding_ignoring_root_nullability(operand, when, work)?;
        } else if *when.data_type() != arrow_schema::DataType::Boolean
            || when.logical_type() != ValueLogicalType::Physical
        {
            return Err(ProgramExpressionTypeError::TypeMismatch);
        }
        let then = value(children[offset + arm * 2 + 1])?;
        same_value(then, result, true, work)?;
        work.step()?;
    }
    if has_else {
        let otherwise = value(children[offset + pairs])?;
        same_value(otherwise, result, true, work)?;
    } else if !result.nullable() {
        return Err(ProgramExpressionTypeError::TypeMismatch);
    }
    Ok(())
}

fn validate(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    validate_function_value_type_observed(value, work).map_err(|error| match error {
        KernelFailure::CompileControl(cause) => ProgramExpressionTypeError::Control(cause),
        KernelFailure::Cancelled => {
            ProgramExpressionTypeError::Control(CompileControlError::Cancelled)
        }
        KernelFailure::DeadlineExceeded => {
            ProgramExpressionTypeError::Control(CompileControlError::DeadlineExceeded)
        }
        KernelFailure::ResourceExhausted => {
            ProgramExpressionTypeError::Control(CompileControlError::ResourceExhausted)
        }
        error => ProgramExpressionTypeError::Kernel(error),
    })
}
fn same_carrier(
    actual: &arrow_schema::DataType,
    expected: &arrow_schema::DataType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    if arrow_data_types_exact_with_profile_observed::<ProgramExpressionTypeError>(
        actual,
        expected,
        novarocks_type_contract::ArrowTypeProfile::CanonicalValue,
        || work.step().map_err(Into::into),
    )? {
        Ok(())
    } else {
        Err(ProgramExpressionTypeError::TypeMismatch)
    }
}
fn same_value(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
    argument: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    work.step()?;
    let semantic_match = if argument {
        (expected.nullable() || !actual.nullable())
            && actual.same_value_domain_observed(expected, || {
                work.step().map_err(ProgramExpressionTypeError::from)
            })?
    } else {
        actual.same_semantics_observed(expected, || {
            work.step().map_err(ProgramExpressionTypeError::from)
        })?
    };
    if !semantic_match {
        return Err(ProgramExpressionTypeError::TypeMismatch);
    }
    // Physical port identity remains exact after semantic admission. Runtime
    // encoding and output aliases are separate facts, never semantic children.
    same_carrier(actual.data_type(), expected.data_type(), work)
}

fn same_binding_ignoring_root_nullability(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramExpressionTypeError> {
    if actual.same_physical_binding_ignoring_root_nullability_observed(expected, || {
        work.step().map_err(ProgramExpressionTypeError::from)
    })? {
        Ok(())
    } else {
        Err(ProgramExpressionTypeError::TypeMismatch)
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod call_tests;

#[cfg(test)]
mod nary_tests;

#[cfg(test)]
mod case_tests;

#[cfg(test)]
mod equality_tests;
