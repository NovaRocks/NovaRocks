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

//! Display text matches the SQL package-semantics strings this author replaces.
//! SQL maps each variant back to that category and embeds this text.

use novarocks_constant_contract::ConstantError;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_functions::{
    ArithmeticPrepareError, CastPrepareError, ComparisonPrepareError, FunctionBindingError,
    FunctionSpecializationFailure, KernelFailure,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{CompileControlError, ValueTypeError};
use std::fmt;

#[derive(Debug)]
pub enum CallAuthorError {
    /// Static shape checks and Functions capability refusals, including constant limits.
    Capability(CapabilityRefusal),
    Control(CompileControlError),
    /// Missing overflow policy, a constant policy that disagrees with the inputs,
    /// or a unit that does not match the sealed catalogue.
    Invariant(InvariantRefusal),
}

#[derive(Debug)]
pub enum CapabilityRefusal {
    /// `constant admission limit: {cause}`.
    ConstantLimit(ConstantError),
    /// `package expression occurrences: {detail}`.
    Occurrence(String),
    /// `package call effects: {detail}`.
    Effects(String),
    /// Bare capability text, previously `UnsupportedCapability`.
    Unsupported(String),
    /// Prepared cast, arithmetic or comparison refusal.
    Primitive {
        source: SharedFailureCause,
        cast: Option<(DataType, DataType)>,
    },
}

#[derive(Debug)]
pub enum InvariantRefusal {
    MissingOverflow(novarocks_expression_contract::ExprId),
    ConstantPolicy,
    Source(&'static str),
}

impl CallAuthorError {
    pub(crate) fn occurrence(detail: impl Into<String>) -> Self {
        Self::Capability(CapabilityRefusal::Occurrence(detail.into()))
    }

    pub(crate) fn effects(detail: impl Into<String>) -> Self {
        Self::Capability(CapabilityRefusal::Effects(detail.into()))
    }

    pub(crate) fn effects_invalid(detail: &'static str) -> Self {
        Self::effects(format!("InvalidSource({detail:?})"))
    }

    pub(crate) fn scalar_invalid(detail: &'static str) -> Self {
        Self::effects(format!("Scalar(InvalidSource({detail:?}))"))
    }

    pub(crate) fn specialization(error: FunctionSpecializationFailure, effects: bool) -> Self {
        match control_specialization(error) {
            Ok(cause) => Self::Control(cause),
            Err(error) => {
                let detail = format!("Function({error:?})");
                if effects {
                    Self::effects(format!("Scalar(Occurrence({detail}))"))
                } else {
                    Self::occurrence(detail)
                }
            }
        }
    }
}

fn control_specialization(
    error: FunctionSpecializationFailure,
) -> Result<CompileControlError, FunctionSpecializationFailure> {
    match error {
        FunctionSpecializationFailure::Control(error)
        | FunctionSpecializationFailure::Binding(FunctionBindingError::Control(error))
        | FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(error)) => {
            Ok(error)
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
            Ok(CompileControlError::Cancelled)
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
            Ok(CompileControlError::DeadlineExceeded)
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
            Ok(CompileControlError::ResourceExhausted)
        }
        error => Err(error),
    }
}

pub(crate) fn comparison_author_error(error: ComparisonPrepareError) -> CallAuthorError {
    match error {
        ComparisonPrepareError::Control(cause)
        | ComparisonPrepareError::Kernel(KernelFailure::CompileControl(cause)) => {
            CallAuthorError::Control(cause)
        }
        other => primitive(SharedFailureCause::new(other), None),
    }
}

pub(crate) fn arithmetic_author_error(error: ArithmeticPrepareError) -> CallAuthorError {
    match error {
        ArithmeticPrepareError::Control(cause)
        | ArithmeticPrepareError::Kernel(KernelFailure::CompileControl(cause)) => {
            CallAuthorError::Control(cause)
        }
        other => primitive(SharedFailureCause::new(other), None),
    }
}

pub(crate) fn cast_author_error(
    error: CastPrepareError,
    from: DataType,
    to: DataType,
) -> CallAuthorError {
    match error {
        CastPrepareError::Control(cause)
        | CastPrepareError::Kernel(KernelFailure::CompileControl(cause)) => {
            CallAuthorError::Control(cause)
        }
        CastPrepareError::Unsupported => primitive(SharedFailureCause::new(error), Some((from, to))),
        other => primitive(SharedFailureCause::new(other), None),
    }
}

fn primitive(
    source: SharedFailureCause,
    cast: Option<(DataType, DataType)>,
) -> CallAuthorError {
    CallAuthorError::Capability(CapabilityRefusal::Primitive { source, cast })
}

impl From<ValueTypeError> for CallAuthorError {
    fn from(error: ValueTypeError) -> Self {
        Self::effects(format!("Type({error:?})"))
    }
}

impl From<CompileControlError> for CallAuthorError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}

impl fmt::Display for CallAuthorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(formatter),
            Self::Capability(CapabilityRefusal::ConstantLimit(cause)) => {
                write!(formatter, "constant admission limit: {cause}")
            }
            Self::Capability(CapabilityRefusal::Occurrence(detail)) => {
                write!(formatter, "package expression occurrences: {detail}")
            }
            Self::Capability(CapabilityRefusal::Effects(detail)) => {
                write!(formatter, "package call effects: {detail}")
            }
            Self::Capability(CapabilityRefusal::Unsupported(detail)) => {
                formatter.write_str(detail)
            }
            Self::Capability(CapabilityRefusal::Primitive { source, cast }) => match cast {
                Some((from, to)) => write!(formatter, "package cast {from:?} to {to:?}: {source}"),
                None => source.fmt(formatter),
            },
            Self::Invariant(InvariantRefusal::MissingOverflow(definition)) => write!(
                formatter,
                "call definition {definition:?} has no decimal overflow policy"
            ),
            Self::Invariant(InvariantRefusal::ConstantPolicy) => formatter.write_str(
                "package call effects: InvalidSource(\"SQL expression occurrence changes its original captured policy\")",
            ),
            Self::Invariant(InvariantRefusal::Source(detail)) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for CallAuthorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Capability(CapabilityRefusal::ConstantLimit(cause)) => Some(cause),
            Self::Capability(CapabilityRefusal::Primitive { source, .. }) => Some(source),
            _ => None,
        }
    }
}
