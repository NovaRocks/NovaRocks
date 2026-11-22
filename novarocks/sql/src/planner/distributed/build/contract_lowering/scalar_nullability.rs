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

//! The original scalar nullable-contract revision author, shared by actual units.
//! Node graph scheduling and publication remain with the lowering visitor.
use super::scalar_emission::ScalarEmission;
use super::*;
use crate::binding::SqlResultConstraintOrigin;

fn invalid(detail: &'static str) -> ContractLoweringError {
    ContractLoweringError::InvalidAggregate { detail }
}

/// This is the original constraint body used by scalar and relational callers.
pub(super) fn current_constraint(
    binding: &SqlFunctionBinding,
    arguments: &[novarocks_functions::FunctionArgument],
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<novarocks_functions::FunctionValueType>, ContractLoweringError> {
    work.step()?;
    match binding.result_constraint_origin() {
        SqlResultConstraintOrigin::Unconstrained => {
            if binding.result_constraint().is_some() {
                return Err(invalid("unconstrained source carries a result constraint"));
            }
            Ok(None)
        }
        SqlResultConstraintOrigin::ArrayCommonCoercion
        | SqlResultConstraintOrigin::OperatorDecimalFloatCoercion => {
            let [novarocks_functions::FunctionArgument::Value { value_type, .. }] = arguments
            else {
                return Err(invalid(
                    "direct conversion constraint lacks its original value source",
                ));
            };
            let target = binding
                .result_constraint()
                .ok_or_else(|| invalid("direct conversion constraint lost its authored target"))?;
            work.flush()?;
            let target = target
                .with_nullable(target.nullable() || value_type.nullable())
                .expect("validated root NULL contract");
            work.step()?;
            Ok(Some(target))
        }
        SqlResultConstraintOrigin::ValueDomainConversion { final_target } => {
            let [novarocks_functions::FunctionArgument::Value { value_type, .. }] = arguments
            else {
                return Err(invalid(
                    "conversion result constraint lacks one actual value channel",
                ));
            };
            work.flush()?;
            let mut target = final_target.clone();
            work.step()?;
            work.flush()?;
            target = target
                .with_nullable(target.nullable() || (value_type.nullable()))
                .expect("validated root NULL contract");
            novarocks_functions::builtin::value_conversion::conversion_intermediate_type(
                value_type, &target,
            )
            .map_err(|error| ContractLoweringError::InvalidFunctionBinding {
                detail: error.to_string(),
            })?
            .ok_or_else(|| invalid("conversion source lacks its admitted intermediate result"))
            .map(Some)
        }
    }
}

impl ScalarEmission<'_, '_> {
    fn nullability_definition(
        &self,
        expression: ExprId,
    ) -> Result<&novarocks_expression_contract::ExprNode, ContractLoweringError> {
        self.storage
            .definitions()
            .definition(expression)
            .ok_or_else(|| invalid("nullable transaction expression is absent"))
    }

    /// Visit every original definition event, including CAST/negate seeds with
    /// no dirty dependency. A true result requires the caller's original dirty
    /// insertion and immediately following checkpoint before its next event.
    pub(super) fn rebind_definition(
        &mut self,
        id: ExprId,
        expression_dirty: impl Fn(ExprId) -> bool,
        value_dirty: impl Fn(ValueId) -> bool,
    ) -> Result<bool, ContractLoweringError> {
        self.work.flush()?;
        let old = self.nullability_definition(id)?.clone();
        self.work.step()?;
        self.work.flush()?;
        let mut operands = Vec::new();
        collect_operand_expressions(&old.kind, &mut operands);
        self.work.flush()?;
        let mut dependency_changed = false;
        for &operand in operands.iter() {
            self.work.step()?;
            dependency_changed |= expression_dirty(operand);
        }
        dependency_changed |= match &old.kind {
            ContractExprKind::Value(value) => value_dirty(*value),
            ContractExprKind::Lambda { body, .. } => expression_dirty(*body),
            ContractExprKind::FunctionCall { args, .. } => {
                let mut dirty = false;
                for &arg in args.iter() {
                    self.work.step()?;
                    dirty |= expression_dirty(arg);
                }
                dirty
            }
            _ => false,
        };
        if matches!(&old.kind, ContractExprKind::FunctionCall { .. }) && dependency_changed {
            return self.rebind_scalar_call_entry(id);
        }
        let revised = match &old.kind {
            ContractExprKind::Cast {
                array_failure_mode,
                expr,
                target,
                allow_throw_exception,
                decimal_overflow_policy,
            } => {
                // The same statement owner authored this occurrence's reference.
                // Read its frozen boolean, never an ambient setting or default.
                if *allow_throw_exception != self.root_allow_throw_reference() {
                    return Err(invalid(
                        "cast seed differs from its statement semantic source",
                    ));
                }
                let input = &self.nullability_definition(*expr)?.ty;
                let conversion_can_produce_null = match array_failure_mode {
                    Some(mode) => {
                        *mode == novarocks_type_contract::ArrayCastFailureMode::OutputNull
                    }
                    None => novarocks_functions::carrier_cast_can_produce_null_with_policy(
                        input.data_type(),
                        target,
                        *decimal_overflow_policy,
                        self.root_allow_throw_exception,
                    ),
                };
                old.ty
                    .clone()
                    .with_nullable(
                        old.ty.nullable() || input.nullable() || conversion_can_produce_null,
                    )
                    .expect("validated root NULL contract")
            }
            ContractExprKind::Value(value) if dependency_changed => published_value_type(
                &old.ty,
                self.storage
                    .current_value_type(*value)
                    .ok_or_else(|| invalid("nullable transaction value is absent"))?,
            ),
            ContractExprKind::Lambda { body, .. } if dependency_changed => {
                published_value_type(&old.ty, &self.nullability_definition(*body)?.ty)
            }
            _ if dependency_changed && kind_follows_operand_nullability(&old.kind) => {
                let mut revised = old.ty.clone();
                for operand in operands {
                    self.work.step()?;
                    revised = revised
                        .with_nullable(
                            revised.nullable()
                                || (self.nullability_definition(operand)?.ty.nullable()),
                        )
                        .expect("validated root NULL contract");
                }
                revised
            }
            _ => old.ty.clone(),
        };
        if revised.nullable() != old.ty.nullable() {
            self.storage
                .type_editor()?
                .expression_type(id, &old.ty, revised, self.work)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn rebind_scalar_call_entry(
        &mut self,
        expression: ExprId,
    ) -> Result<bool, ContractLoweringError> {
        let site = self.storage.source_site();
        self.work.flush()?;
        let mut entry = self
            .call_sources
            .expression_entries
            .remove(&ScalarDefinitionAddress::new(site.clone(), expression))
            .ok_or_else(|| invalid("call rebind lacks its original source entry"))?;
        let old = self.nullability_definition(expression)?.clone();
        self.work.flush()?;
        validate_expression_source_entry_observed(
            &entry,
            self.storage.definitions().owner(),
            &old,
            self.work,
        )?;
        self.work.flush()?;
        let projected = project_emitted_call_arguments_observed(
            EmittedOperationalCall {
                expressions: self.storage.definitions(),
                pools: self.constant_emission.pools(),
                owner: entry.owner.clone(),
                lambda_scope: entry.lambda_scope,
                kind: Some(entry.kind),
                captured: CapturedOperationalSource::call(entry.captured.captured()),
                derived: match &entry.captured {
                    LoweredExpressionLogicalSource::DerivedVariant(source) => Some(source.as_ref()),
                    _ => None,
                },
                arguments: &entry.arguments,
                channels: &entry.channels,
            },
            self.control,
        )
        .map_err(ContractLoweringError::from);
        let arguments = self.completed_specialization_result(projected)?;
        let binding = entry.captured.captured().binding();
        let constraint = current_constraint(binding, &arguments, self.work)?;
        let logical_count = entry.captured.captured().request().logical_argument_count;
        let selected = super::scalar_emission::select_canonical_request_with_constraint(
            self.functions,
            self.control,
            self.work,
            binding,
            logical_count,
            &arguments,
            constraint.as_ref(),
        )?;
        let novarocks_functions::FunctionResultType::Scalar(result) = &selected.result_type else {
            return Err(invalid("expression rebind acquired a relation result"));
        };
        let revised = bound_function_from_selection(binding.resolved(), &selected, result);
        self.work.flush()?;
        let changed = call_binding(&old.kind)
            .ok_or_else(|| invalid("call rebind has no actual signature"))?
            != &revised;
        self.work.step()?;
        self.work.flush()?;
        match &old.kind {
            ContractExprKind::FunctionCall { function, .. } => self
                .storage
                .type_editor()?
                .scalar_signature(expression, function, revised, self.work)?,
            _ => return Err(invalid("call rebind changed its actual lifecycle")),
        }
        self.work.flush()?;
        entry.canonical_operational = Some(Arc::new(CanonicalCallOperationalRequest {
            binding: binding.clone(),
            arguments,
            logical_count,
            selected,
            result_constraint: constraint,
        }));
        self.work.step()?;
        self.call_sources
            .expression_entries
            .insert(ScalarDefinitionAddress::new(site, expression), entry);
        self.work.step()?;
        Ok(changed)
    }
}

fn call_binding(kind: &ContractExprKind) -> Option<&BoundFunction> {
    match kind {
        ContractExprKind::FunctionCall { function, .. } => Some(function),
        _ => None,
    }
}
