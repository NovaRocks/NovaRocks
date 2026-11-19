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

use novarocks_expression_contract::RootOrdinal;
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_functions::{
    InvocationPreparationFailure, KernelDiagnostic, KernelFailure, RowDataError,
    ScalarInvocationData, ScalarInvocationFailure, ScalarResourceError, Selection,
};
use std::{error::Error, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScalarCallFailure<R> {
    Kernel(KernelFailure),
    Data(ScalarInvocationData),
    Source(ScalarResourceError),
    Host(R),
}

impl<R: fmt::Display> fmt::Display for ScalarCallFailure<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(f),
            Self::Data(cause) => cause.fmt(f),
            Self::Source(cause) => cause.fmt(f),
            Self::Host(cause) => cause.fmt(f),
        }
    }
}

impl<R: Error + 'static> Error for ScalarCallFailure<R> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Kernel(cause) => Some(cause),
            Self::Data(cause) => Some(cause),
            Self::Source(cause) => Some(cause),
            Self::Host(cause) => Some(cause),
        }
    }
}

impl<R: FailureProjection> FailureProjection for ScalarCallFailure<R> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Kernel(cause) => cause.failure_fact(),
            Self::Data(cause) => cause.failure_fact(),
            Self::Source(cause) => cause.failure_fact(),
            Self::Host(cause) => cause.failure_fact(),
        }
    }
}

impl<R> From<KernelFailure> for ScalarCallFailure<R> {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExprEvaluationFailure<R> {
    Kernel(KernelFailure),
    Data(ScalarInvocationData),
    Source(ScalarResourceError),
    Host(R),
    RequiredRow(RequiredExpressionRowError),
}

impl<R: fmt::Display> fmt::Display for ExprEvaluationFailure<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(f),
            Self::Data(cause) => cause.fmt(f),
            Self::Source(cause) => cause.fmt(f),
            Self::Host(cause) => cause.fmt(f),
            Self::RequiredRow(cause) => cause.fmt(f),
        }
    }
}

impl<R: Error + 'static> Error for ExprEvaluationFailure<R> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Kernel(cause) => Some(cause),
            Self::Data(cause) => Some(cause),
            Self::Source(cause) => Some(cause),
            Self::Host(cause) => Some(cause),
            Self::RequiredRow(cause) => Some(cause),
        }
    }
}

impl<R: FailureProjection> FailureProjection for ExprEvaluationFailure<R> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Kernel(cause) => cause.failure_fact(),
            Self::Data(cause) => cause.failure_fact(),
            Self::Source(cause) => cause.failure_fact(),
            Self::Host(cause) => cause.failure_fact(),
            Self::RequiredRow(cause) => cause.failure_fact(),
        }
    }
}

impl<R> From<KernelFailure> for ExprEvaluationFailure<R> {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}

impl<R> From<ScalarInvocationFailure> for ScalarCallFailure<R> {
    fn from(cause: ScalarInvocationFailure) -> Self {
        match cause {
            ScalarInvocationFailure::Kernel(cause) => Self::Kernel(cause),
            ScalarInvocationFailure::Data(cause) => Self::Data(cause),
        }
    }
}

impl<R> From<InvocationPreparationFailure> for ScalarCallFailure<R> {
    fn from(cause: InvocationPreparationFailure) -> Self {
        match cause {
            InvocationPreparationFailure::Kernel(cause) => Self::Kernel(cause),
            InvocationPreparationFailure::Source(cause) => Self::Source(cause),
        }
    }
}

impl<R> From<ScalarCallFailure<R>> for ExprEvaluationFailure<R> {
    fn from(cause: ScalarCallFailure<R>) -> Self {
        match cause {
            ScalarCallFailure::Kernel(cause) => Self::Kernel(cause),
            ScalarCallFailure::Data(cause) => Self::Data(cause),
            ScalarCallFailure::Source(cause) => Self::Source(cause),
            ScalarCallFailure::Host(cause) => Self::Host(cause),
        }
    }
}

impl<R> From<RequiredExpressionRowError> for ExprEvaluationFailure<R> {
    fn from(cause: RequiredExpressionRowError) -> Self {
        Self::RequiredRow(cause)
    }
}

/// A required root's remaining journal error, after its legal guards ran.
/// The original selected ordinal and message remain in the original payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequiredExpressionRowError {
    root: RootOrdinal,
    batch_row: usize,
    error: RowDataError,
}

impl RequiredExpressionRowError {
    pub fn try_new(
        root: RootOrdinal,
        selection: Selection<'_>,
        error: RowDataError,
    ) -> Result<Self, KernelFailure> {
        let batch_row = selection.row(error.selected_ordinal()).ok_or_else(|| {
            KernelFailure::Internal(KernelDiagnostic::new(
                "required expression error ordinal is outside its selection",
            ))
        })?;
        Ok(Self {
            root,
            batch_row,
            error,
        })
    }

    pub const fn root(&self) -> RootOrdinal {
        self.root
    }
    pub const fn batch_row(&self) -> usize {
        self.batch_row
    }
    pub const fn error(&self) -> &RowDataError {
        &self.error
    }
}

impl fmt::Display for RequiredExpressionRowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "required expression {:?} failed at batch row {} (selected ordinal {}): {}",
            self.root,
            self.batch_row,
            self.error.selected_ordinal(),
            self.error.message()
        )
    }
}
impl Error for RequiredExpressionRowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

impl novarocks_failure_contract::FailureProjection for RequiredExpressionRowError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.error.failure_fact().with_row_position(
            novarocks_failure_contract::FailureRowPosition {
                selected_ordinal: u64::try_from(self.error.selected_ordinal())
                    .expect("supported pointer widths fit u64"),
                batch_row: u64::try_from(self.batch_row).expect("supported pointer widths fit u64"),
            },
        )
    }
}
