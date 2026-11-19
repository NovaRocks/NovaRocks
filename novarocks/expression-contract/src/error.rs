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

//! Typed unit construction failures preserve originating owner facts.
use crate::{ExprId, RootOrdinal};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use novarocks_type_contract::{CompileControlError, ExpressionControlFlowError};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Debug)]
pub enum UnitError {
    Control(CompileControlError),
    Flow(ExpressionControlFlowError),
    Binding(novarocks_function_contract::FunctionBindingError),
    Effects(novarocks_type_contract::EffectContractError),
    Type(novarocks_type_contract::ValueTypeError),
    Source(crate::TemporalSourceProjectionError),
    Invalid {
        definition: Option<ExprId>,
        root: Option<RootOrdinal>,
        message: String,
    },
    Resource(&'static str),
}
impl UnitError {
    pub(crate) fn association(message: &'static str) -> Self {
        Self::invalid(None, None, message)
    }
    pub(crate) fn at_definition(mut self, id: ExprId) -> Self {
        if let Self::Invalid { definition, .. } = &mut self
            && definition.is_none()
        {
            *definition = Some(id);
        }
        self
    }
    pub(crate) fn invalid(
        definition: Option<ExprId>,
        root: Option<RootOrdinal>,
        message: impl Into<String>,
    ) -> Self {
        Self::Invalid {
            definition,
            root,
            message: message.into(),
        }
    }
}
impl From<CompileControlError> for UnitError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExpressionControlFlowError> for UnitError {
    fn from(error: ExpressionControlFlowError) -> Self {
        match error {
            ExpressionControlFlowError::Control(cause) => Self::Control(cause),
            other => Self::Flow(other),
        }
    }
}
impl From<novarocks_function_contract::FunctionBindingError> for UnitError {
    fn from(error: novarocks_function_contract::FunctionBindingError) -> Self {
        match error {
            novarocks_function_contract::FunctionBindingError::Control(cause) => {
                Self::Control(cause)
            }
            other => Self::Binding(other),
        }
    }
}
impl From<novarocks_type_contract::EffectContractError> for UnitError {
    fn from(error: novarocks_type_contract::EffectContractError) -> Self {
        Self::Effects(error)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for UnitError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl From<crate::TemporalSourceProjectionError> for UnitError {
    fn from(error: crate::TemporalSourceProjectionError) -> Self {
        match error {
            crate::TemporalSourceProjectionError::Control(cause) => Self::Control(cause),
            other => Self::Source(other),
        }
    }
}
impl fmt::Display for UnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Flow(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
            Self::Effects(error) => error.fmt(f),
            Self::Type(error) => error.fmt(f),
            Self::Source(error) => error.fmt(f),
            Self::Invalid {
                definition,
                root,
                message,
            } => write!(
                f,
                "invalid expression unit (root {root:?}, definition {definition:?}): {message}"
            ),
            Self::Resource(message) => f.write_str(message),
        }
    }
}
impl Error for UnitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Flow(error) => Some(error),
            Self::Binding(error) => Some(error),
            Self::Effects(error) => Some(error),
            Self::Type(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::Invalid { .. } | Self::Resource(_) => None,
        }
    }
}
#[derive(Clone, Copy)]
#[repr(u16)]
enum UnitReason {
    Invalid = 1,
    Resource = 2,
    MissingLegacy = 3,
    Materialization = 4,
    Source = 5,
}
fn fact(class: FailureClass, reason: UnitReason) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(
            domain::EXPRESSION_CONTRACT,
            NonZeroU16::new(reason as u16).unwrap(),
        ),
    )
}
impl FailureProjection for UnitError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Flow(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Effects(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Source(source) => source.failure_fact(),
            Self::Invalid { .. } => fact(FailureClass::Invariant, UnitReason::Invalid),
            Self::Resource(_) => fact(FailureClass::Resource, UnitReason::Resource),
        }
    }
}
impl FailureProjection for crate::MissingLegacyBindingMetadata {
    fn failure_fact(&self) -> FailureFact {
        fact(FailureClass::Invariant, UnitReason::MissingLegacy)
    }
}
impl FailureProjection for crate::CallRequestMaterializationError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Reference(source) => source.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, UnitReason::Materialization),
        }
    }
}
impl<H: FailureProjection> FailureProjection for crate::CallRequestMaterializationFailure<H> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Owner(source) => source.failure_fact(),
            Self::Creator(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for crate::TemporalSourceProjectionError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, UnitReason::Source),
        }
    }
}
