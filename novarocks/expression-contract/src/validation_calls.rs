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

//! Exact correspondence of rooted control, call claims and original requests.
use crate::{ExprKind, RootOrdinal, UnitError, UnitParts};
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, ControlShape, EffectContractError,
    FunctionEffectDeclaration, FunctionKind,
};
use std::collections::BTreeSet;

pub(crate) fn validate(
    parts: &UnitParts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    if parts.roots.len() != parts.root_uses.len() {
        return Err(UnitError::invalid(
            None,
            None,
            "root-use count differs from roots",
        ));
    }
    let mut roots = BTreeSet::new();
    for (ordinal, (root, &id)) in parts.roots.iter().zip(&parts.root_uses).enumerate() {
        let root_ordinal = RootOrdinal::new(ordinal as u32);
        let invocation = parts.control.uses().get(&id).ok_or_else(|| {
            UnitError::invalid(
                Some(root.definition),
                Some(root_ordinal),
                "root use is absent",
            )
        })?;
        work.flush()?;
        let fresh = roots.insert(id);
        work.step()?;
        work.flush()?;
        if !fresh
            || parts.control.root_use_ids().binary_search(&id).is_err()
            || invocation.definition != root.definition
            || invocation.context.demand != root.demand
        {
            return Err(UnitError::invalid(
                Some(root.definition),
                Some(root_ordinal),
                "root does not bind one exact independent invocation",
            ));
        }
        let region = &parts.control.regions()[&invocation.context.region];
        if region.parent.is_some() || region.guard.is_some() {
            return Err(UnitError::invalid(
                Some(root.definition),
                Some(root_ordinal),
                "root invocation belongs to a guarded region",
            ));
        }
    }
    if roots.len() != parts.control.root_use_ids().len() {
        return Err(UnitError::invalid(
            None,
            None,
            "control flow contains an unbound root",
        ));
    }
    let mut call_count = 0usize;
    let mut used_regions = BTreeSet::new();
    for (&use_id, invocation) in parts.control.uses() {
        work.step()?;
        let node = parts
            .definitions
            .get(invocation.definition.get() as usize)
            .ok_or_else(|| {
                UnitError::invalid(
                    Some(invocation.definition),
                    None,
                    "invocation definition is absent",
                )
            })?;
        if invocation.context.demand == novarocks_type_contract::EvaluationDemand::TruthOnly
            && (*node.ty.data_type() != arrow_schema::DataType::Boolean
                || node.ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical)
        {
            return Err(UnitError::invalid(
                Some(node.id),
                None,
                "TruthOnly invocation is not a physical Boolean",
            ));
        }
        work.flush()?;
        used_regions.insert(invocation.context.region);
        work.step()?;
        work.flush()?;
        let mut expected = Vec::new();
        if let Some(shape) = node.kind.intrinsic_control_shape()? {
            if shape != invocation.control {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "intrinsic control differs from its definition",
                ));
            }
            node.kind.invocation_references_observed(|id| {
                crate::validation::reserve(&mut expected, 1, work)?;
                expected.push(id);
                work.step()?;
                Ok::<_, UnitError>(())
            })?;
        } else {
            let ExprKind::FunctionCall { function, args } = &node.kind else {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "control names an unknown scalar definition",
                ));
            };
            let claim = parts.claims.get(&use_id).ok_or_else(|| {
                UnitError::invalid(
                    Some(node.id),
                    None,
                    format!("function use {} lacks a call claim", use_id.get()),
                )
            })?;
            call_count += 1;
            work.step()?;
            if claim.context != invocation.context
                || !claim
                    .effects
                    .argument_control
                    .matches_scalar_shape(invocation.control)
            {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "call claim context/control differs from its invocation",
                ));
            }
            if claim.effects.proof_scope != CallProofScope::Unconditional
                && claim.effects.proof_scope != CallProofScope::Region(claim.context.region)
            {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "call proof scope names a foreign region",
                ));
            }
            let mut references = BTreeSet::new();
            let mut keys = BTreeSet::new();
            for reference in &claim.effects.environment {
                work.flush()?;
                if !references.insert(*reference) || !keys.insert(reference.expected_key) {
                    return Err(EffectContractError::InvalidEnvironmentReference.into());
                }
                work.step()?;
                work.flush()?;
            }
            work.flush()?;
            let declaration = FunctionEffectDeclaration {
                value_stability: claim.effects.value_stability,
                own_row_error: claim.effects.own_row_error,
                failure_behavior: claim.effects.failure_behavior,
                null_behavior: claim.effects.null_behavior,
                argument_control: claim.effects.argument_control,
                instance_state: claim.effects.instance_state,
                observable_effects: claim.effects.observable_effects,
                environment_dependencies: keys.into_iter().collect(),
            };
            work.step()?;
            work.flush()?;
            declaration.validate(FunctionKind::Scalar)?;
            work.step()?;
            work.flush()?;
            if function.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
                if invocation.control != ControlShape::Eager
                    || claim.regexp_count_pattern_source
                        != Some(crate::regexp_count_pattern_source_observed(
                            &parts.definitions,
                            args,
                            work,
                        )?)
                {
                    return Err(UnitError::invalid(
                        Some(node.id),
                        None,
                        "regexp_count source claim differs from actual emitted source",
                    ));
                }
            } else if claim.regexp_count_pattern_source.is_some() {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "unrelated call carries regexp_count source",
                ));
            }
            if function.function_id.as_str() == "builtin.scalar/to_base64/v1" {
                if invocation.control != ControlShape::Eager
                    || claim.to_base64_byte_source
                        != Some(crate::to_base64_byte_source_observed(
                            &parts.definitions,
                            args,
                            work,
                        )?)
                {
                    return Err(UnitError::invalid(
                        Some(node.id),
                        None,
                        "to_base64 source claim differs from actual emitted source",
                    ));
                }
            } else if claim.to_base64_byte_source.is_some() {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "unrelated call carries to_base64 source",
                ));
            }
            match (invocation.control, &claim.temporal_source) {
                (ControlShape::TemporalSource(shape), Some(plan)) => {
                    let actual = crate::temporal_source_definitions_observed(
                        shape.kind(),
                        &parts.definitions,
                        args,
                        work,
                    )?;
                    if actual.facts.shape() != shape {
                        return Err(UnitError::invalid(
                            Some(node.id),
                            None,
                            "temporal invocation shape differs from actual emitted source",
                        ));
                    }
                    plan.validate(&actual).map_err(|_| {
                        UnitError::invalid(
                            Some(node.id),
                            None,
                            "temporal source claim differs from actual definitions",
                        )
                    })?;
                    crate::validation::reserve(&mut expected, actual.definitions.len(), work)?;
                    for &definition in &actual.definitions {
                        expected.push(definition);
                        work.step()?;
                    }
                    if plan.sources.len() != invocation.arguments.len() {
                        return Err(UnitError::invalid(
                            Some(node.id),
                            None,
                            "temporal source use count differs from invocation",
                        ));
                    }
                    for (source, &argument) in plan.sources.iter().zip(&invocation.arguments) {
                        if source.use_id != argument {
                            return Err(UnitError::invalid(
                                Some(node.id),
                                None,
                                "temporal source claims a foreign argument use",
                            ));
                        }
                        work.step()?;
                    }
                }
                (ControlShape::TemporalSource(_), None) | (_, Some(_)) => {
                    return Err(UnitError::invalid(
                        Some(node.id),
                        None,
                        "temporal source claim does not match invocation control",
                    ));
                }
                (ControlShape::TypeOnly | ControlShape::NoArguments, None) => {}
                (_, None) => node.kind.invocation_references_observed(|id| {
                    crate::validation::reserve(&mut expected, 1, work)?;
                    expected.push(id);
                    work.step()?;
                    Ok::<_, UnitError>(())
                })?,
            }
        }
        if expected.len() != invocation.arguments.len() {
            return Err(UnitError::invalid(
                Some(node.id),
                None,
                "invocation child count differs from ordered definition occurrences",
            ));
        }
        for (&definition, argument) in expected.iter().zip(&invocation.arguments) {
            let child = parts.control.uses().get(argument).ok_or_else(|| {
                UnitError::invalid(Some(node.id), None, "invocation child use is absent")
            })?;
            if child.definition != definition {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "invocation child names a different definition occurrence",
                ));
            }
            work.step()?;
        }
        if let (ExprKind::FunctionCall { args, .. }, ControlShape::HigherOrder { body_ordinal, .. }) =
            (&node.kind, invocation.control)
            && !args
                .get(body_ordinal as usize)
                .and_then(|id| parts.definitions.get(id.get() as usize))
                .is_some_and(|body| matches!(body.kind, ExprKind::Lambda { .. }))
        {
            return Err(UnitError::invalid(
                Some(node.id),
                None,
                "higher-order control does not name an actual lambda argument",
            ));
        }
        work.step()?;
    }
    if call_count != parts.claims.len() {
        return Err(UnitError::invalid(
            None,
            None,
            "call claims include a foreign or non-call use",
        ));
    }
    if used_regions.len() != parts.control.regions().len() {
        return Err(UnitError::invalid(
            None,
            None,
            "unit control contains an unused region",
        ));
    }
    let mut requests = 0usize;
    for node in &parts.definitions {
        work.step()?;
        let ExprKind::FunctionCall { function, .. } = &node.kind else {
            continue;
        };
        requests += 1;
        let request = parts.requests.get(&node.id).ok_or_else(|| {
            UnitError::invalid(
                Some(node.id),
                None,
                "function definition lacks its original request",
            )
        })?;
        if request.arguments.len() != function.argument_types.len()
            || request.logical_argument_count != function.argument_types.len()
        {
            return Err(UnitError::invalid(
                Some(node.id),
                None,
                "original request argument counts differ from the scalar signature",
            ));
        }
        for (actual, selected) in request.arguments.iter().zip(function.argument_types.iter()) {
            if !actual.matches_type_observed(selected, work)? {
                return Err(UnitError::invalid(
                    Some(node.id),
                    None,
                    "original request argument type differs from the scalar signature",
                ));
            }
            work.step()?;
        }
    }
    if requests != parts.requests.len() {
        return Err(UnitError::invalid(
            None,
            None,
            "original requests include a foreign or non-call definition",
        ));
    }
    Ok(())
}
