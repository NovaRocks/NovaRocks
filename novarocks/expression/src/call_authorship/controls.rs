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

//! Installed-owner control shapes, written into the draft before it is sealed.

use super::error::CallAuthorError;
use super::{CallAuthorInputs, scalar_binding_selection};
use novarocks_expression_contract::{
    BoundExprSetBuilder, DefinitionControl, ExprId, ExprKind, TemporalSourceProjectionError,
};
use novarocks_functions::PureKernelAbi;
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, ControlShape,
};

pub fn author_call_controls(
    draft: &BoundExprSetBuilder,
    inputs: &CallAuthorInputs<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<(ExprId, DefinitionControl)>, CallAuthorError> {
    let mut controls = Vec::new();
    for ordinal in 0..draft.len() {
        let id = ExprId::new(
            u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        let Some(definition) = draft.definition(id) else {
            return Err(CallAuthorError::occurrence(format!(
                "MissingDefinition({id:?})"
            )));
        };
        work.step()?;
        let ExprKind::FunctionCall { function, args } = &definition.kind else {
            continue;
        };
        let declaration = inputs
            .functions
            .metadata()
            .pure_overload_declaration_observed(
                &function.function_id,
                function.kind,
                &function.overload,
                work.control(),
            )
            .map_err(|error| CallAuthorError::specialization(error, false))?;
        let selected_control =
            if declaration.implementation().abi == PureKernelAbi::ScalarInvocationV1 {
                let request = draft.requests().get(&id).ok_or(CallAuthorError::Invariant(
                    super::error::InvariantRefusal::Source(
                        "selected invocation demand has no original request",
                    ),
                ))?;
                let selection = scalar_binding_selection(function);
                declaration
                    .selected_argument_control_observed(
                        &function.function_id,
                        selection.as_ref(),
                        request.logical_argument_count,
                        work.control(),
                    )
                    .map_err(|error| {
                        CallAuthorError::specialization(
                            novarocks_functions::FunctionSpecializationFailure::Binding(error),
                            false,
                        )
                    })?
            } else {
                declaration.effects().argument_control
            };
        let (shape, temporal_sources) = if let ArgumentControl::TemporalSource(kind) =
            selected_control
        {
            work.flush()?;
            let temporal = match novarocks_expression_contract::temporal_source_definitions_from_lookup_observed(
                kind,
                |id| draft.definition(id),
                args,
                work,
            ) {
                Ok(temporal) => temporal,
                Err(TemporalSourceProjectionError::Control(cause)) => {
                    return Err(CallAuthorError::Control(cause));
                }
                Err(TemporalSourceProjectionError::Invalid(message)) => {
                    return Err(CallAuthorError::occurrence(format!(
                        "TemporalSource({message:?})"
                    )));
                }
            };
            (
                ControlShape::TemporalSource(temporal.facts.shape()),
                Some(temporal),
            )
        } else {
            (
                installed_scalar_control_shape(selected_control, args.len()).ok_or_else(|| {
                    CallAuthorError::occurrence(format!("InvalidFunctionControl({id:?})"))
                })?,
                None,
            )
        };
        work.step()?;
        work.flush()?;
        controls
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        controls.push((
            id,
            DefinitionControl {
                shape,
                temporal_sources,
            },
        ));
        work.step()?;
    }
    Ok(controls)
}

/// Map an installed argument control onto the scalar control shape.
/// Relational controls are not scalar shapes.
pub fn installed_scalar_control_shape(
    control: ArgumentControl,
    count: usize,
) -> Option<ControlShape> {
    Some(match control {
        ArgumentControl::Eager => ControlShape::Eager,
        ArgumentControl::TypeOnly => ControlShape::TypeOnly,
        ArgumentControl::NoArguments => ControlShape::NoArguments,
        ArgumentControl::If => ControlShape::If,
        ArgumentControl::Coalesce => ControlShape::Coalesce,
        ArgumentControl::Try => ControlShape::Try,
        ArgumentControl::NullIf => ControlShape::NullIf,
        ArgumentControl::OrderedRegexp => ControlShape::OrderedRegexp,
        ArgumentControl::OrderedNull => ControlShape::OrderedNull,
        ArgumentControl::SimpleCase | ArgumentControl::SearchedCase => {
            let simple = control == ArgumentControl::SimpleCase;
            let tail = count.checked_sub(usize::from(simple))?;
            ControlShape::Case {
                simple,
                arms: u32::try_from(tail / 2).ok()?,
                has_else: tail % 2 == 1,
            }
        }
        ArgumentControl::HigherOrder {
            body_ordinal,
            body_demand,
        } => ControlShape::HigherOrder {
            body_ordinal,
            body_demand,
        },
        ArgumentControl::TemporalSource(_)
        | ArgumentControl::Aggregate
        | ArgumentControl::Window
        | ArgumentControl::Table => return None,
    })
}
