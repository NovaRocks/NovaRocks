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

//! Static requests from actual scalar definitions, before occurrence refinement.

use std::sync::Arc;

use novarocks_functions::{
    ConstantValidation, FunctionArgument, FunctionBindingRequest, FunctionBindingSelection,
    FunctionKind, FunctionResultType, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType,
};
use {
    novarocks_expression_contract::{BoundFunction, ExprKind, ExprNode},
    novarocks_physical_plan::{ConstantPools, ExprId},
};

use super::{
    lowered_draft::{
        CanonicalCallOperationalRequest, CheckedExpressionLogicalSourceEntry, SqlExpressionCallKind,
    },
    physical_call_arguments::{
        PhysicalArgumentError, argument_types_exact_observed, author_physical_argument_observed,
    },
};

#[derive(Debug)]
pub(crate) enum PhysicalScalarRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    InvalidSource(&'static str),
    MissingArgument(ExprId),
    TooManyArguments,
}
impl From<CompileControlError> for PhysicalScalarRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for PhysicalScalarRequestError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        PhysicalArgumentError::from(error).into()
    }
}
impl From<PhysicalArgumentError> for PhysicalScalarRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}

/// The same selected Arc must feed both CallEffectInput and fresh preparation.
/// Signature metadata alone grants no effects or installed implementation.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalScalarRequest<'a> {
    function: &'a BoundFunction,
    selected: Arc<FunctionBindingSelection>,
    source: ScalarRequestSource<'a>,
}
#[derive(Debug)]
enum ScalarRequestSource<'a> {
    OwnedPhysical {
        arguments: Vec<FunctionArgument>,
        result_constraint: &'a FunctionValueType,
    },
    BorrowedCanonical {
        captured: &'a crate::binding::CapturedLogicalCallArguments,
        canonical: &'a CanonicalCallOperationalRequest,
    },
}
impl AuthoredPhysicalScalarRequest<'_> {
    pub const fn function(&self) -> &BoundFunction {
        self.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        match &self.source {
            ScalarRequestSource::OwnedPhysical {
                arguments,
                result_constraint,
                ..
            } => FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: Some(*result_constraint),
            },
            ScalarRequestSource::BorrowedCanonical { canonical, .. } => canonical.request(),
        }
    }
    /// Direct physical components have no captured SQL policy to infer.
    pub fn captured_decimal_overflow_policy(
        &self,
    ) -> Option<novarocks_type_contract::DecimalOverflowPolicy> {
        match &self.source {
            ScalarRequestSource::OwnedPhysical { .. } => None,
            ScalarRequestSource::BorrowedCanonical { captured, .. } => {
                Some(captured.binding().decimal_overflow_policy())
            }
        }
    }
    pub fn captured_constant_policy(&self) -> Option<ConstantValidation> {
        match &self.source {
            ScalarRequestSource::OwnedPhysical { .. } => None,
            ScalarRequestSource::BorrowedCanonical { captured, .. } => {
                Some(captured.constant_policy())
            }
        }
    }
}

/// Borrow the sole same-emission request and selection from the checked SQL
/// journal. Original None/CV/Lambda and the syntax result constraint travel
/// unchanged; no physical literal is re-admitted or projected here. Exact
/// signature checks authenticate data correspondence, not effects or a kernel.
/// The caller owns entry, ordinary/success footer and source/type-walk admission.
/// No new argument buffer or selected Arc is allocated by this adapter.
pub(crate) fn author_physical_scalar_request_from_journal_observed<'source>(
    entry: &CheckedExpressionLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalScalarRequest<'source>, PhysicalScalarRequestError> {
    let accepted = matches!(
        entry.kind(),
        SqlExpressionCallKind::Scalar
            | SqlExpressionCallKind::ValueConversion
            | SqlExpressionCallKind::DerivedVariant
    );
    work.step()?;
    if !accepted {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "scalar journal request has another lifecycle",
        ));
    }
    let source = entry.source();
    let ExprKind::FunctionCall { function, args } = &source.kind else {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "scalar journal source is not its actual function call",
        ));
    };
    let canonical = entry.canonical_operational();
    work.step()?;
    let canonical = canonical.ok_or(PhysicalScalarRequestError::InvalidSource(
        "scalar journal has no same-emission operational request",
    ))?;
    let captured = entry.captured();
    let belongs = canonical.belongs_to(captured);
    work.step()?;
    if !belongs {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "scalar operational request loans another original binding",
        ));
    }
    let request = canonical.request();
    let selected = canonical.selected();
    if args.len() > MAX_CALL_EFFECT_ARGUMENTS
        || request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
        || selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        || function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let resolved = captured.binding().resolved();
    let matching = function.kind == FunctionKind::Scalar
        && resolved.kind == FunctionKind::Scalar
        && function.function_id == resolved.function_id
        && selected.overload == function.overload
        && selected.overload == resolved.selected.overload
        && selected.aggregate.is_none()
        && request.logical_argument_count == args.len()
        && resolved.logical_argument_count == args.len()
        && request.arguments.len() == args.len()
        && function.argument_types.len() == args.len()
        && selected.argument_types.len() == args.len();
    work.step()?;
    if !matching {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "scalar journal identity or ordered argument count differs from its actual binding",
        ));
    }
    for (expected, bound) in selected.argument_types.iter().zip(&function.argument_types) {
        work.flush()?;
        let matching =
            argument_types_exact_observed::<PhysicalScalarRequestError>(expected, bound, work)?;
        work.flush()?;
        if !matching {
            return Err(PhysicalScalarRequestError::InvalidSource(
                "scalar journal selected full argument type differs from its actual binding",
            ));
        }
    }
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "scalar journal selection carries a relation result",
        ));
    };
    for actual in [&function.result_type, &source.ty] {
        work.flush()?;
        let matching = result
            .exactly_equals_observed::<PhysicalScalarRequestError>(actual, || {
                work.step().map_err(PhysicalScalarRequestError::from)
            })?;
        work.flush()?;
        if !matching {
            return Err(PhysicalScalarRequestError::InvalidSource(
                "scalar journal selected result differs from its actual complete type",
            ));
        }
    }
    work.flush()?;
    let selected = Arc::clone(canonical.selected());
    work.step()?;
    work.flush()?;
    Ok(AuthoredPhysicalScalarRequest {
        function,
        selected,
        source: ScalarRequestSource::BorrowedCanonical {
            captured,
            canonical,
        },
    })
}

/// Preserve ordered actual argument definitions and the original physical
/// signature separately. The installed owner validates the original selected
/// overload against these actual source types; nullable covariance remains its
/// responsibility. No legacy effect bits or parameter references are copied.
/// TypeOnly still has a complete static request; runtime uses are a separate
/// control-flow obligation. Only scalar definitions are accepted here, including
/// control intrinsics; relational/window phase contracts use their own owners.
///
/// Caller admission covers the immutable source and opaque nested type clones.
/// Bounds and fallible vector allocation precede argument expansion. The caller
/// owns entry/ordinary footer, exact policy/environment and subsequent fresh
/// preparation; this leaf observes only its work on the original meter.
pub(crate) fn author_physical_scalar_request_observed<'a>(
    source: &'a ExprNode,
    expressions: super::unit_source::UnitDefinitions<'_>,
    pools: &ConstantPools,
    literal_policy: ConstantValidation,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalScalarRequest<'a>, PhysicalScalarRequestError> {
    work.step()?;
    let ExprKind::FunctionCall { function, args } = &source.kind else {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "physical scalar request requires an actual function call",
        ));
    };
    if function.kind != FunctionKind::Scalar {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "physical scalar request carries a non-scalar binding",
        ));
    }
    if args.len() > MAX_CALL_EFFECT_ARGUMENTS {
        return Err(PhysicalScalarRequestError::TooManyArguments);
    }
    if args.len() != function.argument_types.len() {
        return Err(PhysicalScalarRequestError::InvalidSource(
            "physical scalar argument count differs from its selected signature",
        ));
    }
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(args.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    for &id in args {
        let argument = expressions.definition(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalScalarRequestError::MissingArgument(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    let selected = author_scalar_result_selection_observed(function, None, work)?;
    Ok(AuthoredPhysicalScalarRequest {
        function,
        selected,
        source: ScalarRequestSource::OwnedPhysical {
            arguments,
            result_constraint: &source.ty,
        },
    })
}

/// Loan the actual frozen signature; neither resolve a name nor reconstruct
/// effects. Aggregate state identity is the original neutral type, not a
/// string conversion. The caller admits opaque signature/type clones.
pub(super) fn author_scalar_result_selection_observed(
    function: &BoundFunction,
    aggregate: Option<&novarocks_physical_plan::AggregateBinding>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<FunctionBindingSelection>, CompileControlError> {
    work.flush()?;
    let selected = Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Scalar(function.result_type.clone()),
        aggregate: aggregate.map(|binding| novarocks_functions::AggregateBindingSelection {
            state_argument_contract: binding.state_argument_contract,
            intermediate_type: binding.intermediate_type.clone(),
            state_format: binding.state_format.clone(),
        }),
    });
    work.flush()?;
    Ok(selected)
}

#[cfg(test)]
#[path = "physical_scalar_requests_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "physical_scalar_journal_tests.rs"]
mod journal_tests;
