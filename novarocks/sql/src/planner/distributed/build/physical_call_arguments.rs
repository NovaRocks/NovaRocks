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

//! Project one actual physical argument for the installed function owner.
//! This leaf does not resolve names, evaluate expressions or author effects.

use novarocks_functions::{ConstantError, ConstantPolicy, FunctionArgument};
use novarocks_physical_plan::{ConstantPools, ConstantReferenceError, ExprKind, ExprNode};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ValueTypeError,
};

#[derive(Debug)]
pub(crate) enum PhysicalArgumentError {
    Control(CompileControlError),
    Type(ValueTypeError),
    Constant(ConstantError),
    Reference(ConstantReferenceError),
}
impl From<CompileControlError> for PhysicalArgumentError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for PhysicalArgumentError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl From<ConstantError> for PhysicalArgumentError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(cause) => Self::Control(cause),
            other => Self::Constant(other),
        }
    }
}
impl From<ConstantReferenceError> for PhysicalArgumentError {
    fn from(error: ConstantReferenceError) -> Self {
        match error {
            ConstantReferenceError::Control(cause)
            | ConstantReferenceError::Constant(ConstantError::Control(cause)) => {
                Self::Control(cause)
            }
            other => Self::Reference(other),
        }
    }
}

impl PhysicalArgumentError {
    pub(crate) fn is_constant_limit(&self) -> bool {
        matches!(
            self,
            Self::Constant(ConstantError::Limit(_))
                | Self::Reference(ConstantReferenceError::Constant(ConstantError::Limit(_)))
        )
    }

    /// Consume only the exact limit carrier; preserve all other argument errors.
    pub(crate) fn into_constant_limit(self) -> Result<ConstantError, Self> {
        match self {
            Self::Constant(error @ ConstantError::Limit(_))
            | Self::Reference(ConstantReferenceError::Constant(error @ ConstantError::Limit(_))) => {
                Ok(error)
            }
            other => Err(other),
        }
    }
}

/// Borrow the actual source type, preserving nullable covariance for the
/// installed owner's selected-type validation. A checked pool value keeps its
/// original Field, backing and ordinal; a typed NULL remains Some(value).
/// Computed values remain nonconstant, including casts.
/// Lambda types come from the actual definition; body/lexical admission is
/// the original structural owner's responsibility, not scalar evaluation.
///
/// The caller has admitted the source metadata and this request coexistence.
/// Fallible parameter storage is not a grant for opaque nested type clones.
/// The caller owns entry and the ordinary/success footer; originating control
/// failures return directly without another observation.
pub(crate) fn author_physical_argument_observed(
    source: &ExprNode,
    pools: &ConstantPools,
    _literal_policy: ConstantPolicy,
    _phase: CompilePhase,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionArgument, PhysicalArgumentError> {
    work.step()?;
    if let ExprKind::Lambda {
        parameter_types, ..
    } = &source.kind
    {
        work.flush()?;
        let mut parameters = Vec::new();
        parameters
            .try_reserve_exact(parameter_types.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for parameter in parameter_types {
            parameters.push(parameter.clone_observed(|| work.step())?);
        }
        work.flush()?;
        let parameters = parameters.into_boxed_slice();
        work.step()?;
        work.flush()?;
        let parameter_types =
            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(parameters);
        work.step()?;
        work.flush()?;
        let argument = FunctionArgument::Lambda {
            parameter_types,
            result_type: source.ty.clone_observed(|| work.step())?,
        };
        work.flush()?;
        return Ok(argument);
    }
    let constant = match &source.kind {
        ExprKind::Constant(reference) => {
            Some(pools.resolve_observed(*reference, &source.ty, work)?)
        }
        _ => None,
    };
    work.flush()?;
    let argument = FunctionArgument::Value {
        value_type: source.ty.clone(),
        constant,
    };
    work.flush()?;
    Ok(argument)
}

#[cfg(test)]
#[path = "physical_call_arguments_tests.rs"]
mod tests;

/// The sole borrowed exact Value/Lambda signature comparison, shared by
/// original journal consumers. It grants neither source identity nor effects.
pub(crate) fn argument_types_exact_observed<E>(
    left: &novarocks_type_contract::FunctionArgumentType,
    right: &novarocks_type_contract::FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, E>
where
    E: From<CompileControlError> + From<novarocks_type_contract::ValueTypeError>,
{
    use novarocks_type_contract::FunctionArgumentType;
    work.step().map_err(E::from)?;
    match (left, right) {
        (FunctionArgumentType::Value(left), FunctionArgumentType::Value(right)) => {
            left.exactly_equals_observed(right, || work.step().map_err(E::from))
        }
        (
            FunctionArgumentType::Lambda {
                parameter_types: left,
                result_type: left_result,
            },
            FunctionArgumentType::Lambda {
                parameter_types: right,
                result_type: right_result,
            },
        ) => {
            let same_count = left.len() == right.len();
            work.step().map_err(E::from)?;
            if !same_count {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !left.exactly_equals_observed(right, || work.step().map_err(E::from))? {
                    return Ok(false);
                }
            }
            left_result.exactly_equals_observed(right_result, || work.step().map_err(E::from))
        }
        _ => Ok(false),
    }
}

impl From<novarocks_constant_contract::ConstantTableError> for PhysicalArgumentError {
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(ConstantReferenceError::from(error))
    }
}
