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

//! Execution-owned failure payloads. These facts own no terminal latch.

use novarocks_functions::{KernelDiagnostic, KernelFailure, RowDataError, Selection};
use novarocks_local_program::ProgramExpressionRootSite;
use std::{error::Error, fmt};

pub type ExecutionResult<T> = Result<T, ExecutionFailure>;

/// A required root's remaining journal error, after its legal guards ran.
/// The original selected ordinal and message remain in the original payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequiredExpressionRowError {
    root: ProgramExpressionRootSite,
    batch_row: usize,
    error: RowDataError,
}

impl RequiredExpressionRowError {
    pub fn try_new(
        root: ProgramExpressionRootSite,
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

    pub const fn root(&self) -> ProgramExpressionRootSite {
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
impl Error for RequiredExpressionRowError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionFailureCause {
    /// An explicitly identified existing text-only execution source. Never
    /// infer a resource, deadline, transport or invariant category from text.
    Pipeline(String),
    Kernel(KernelFailure),
    RuntimeScalarSource(novarocks_functions::ScalarResourceError),
    RuntimeScalarMemory(crate::runtime::scalar_memory::RuntimeScalarMemoryRefusal),
    InvocationData(novarocks_functions::InvocationData),
    ScalarInvocationData(novarocks_functions::ScalarInvocationData),
    WindowInvocationData(novarocks_functions::WindowInvocationData),
    RequiredRow(RequiredExpressionRowError),
    PreparationMetadata(crate::runtime::preparation_metadata::PreparationMetadataFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PipelineOperation {
    Push,
    Pull,
    Finishing,
    Admission,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionFailureContext {
    pub operator_ordinal: usize,
    pub operation: PipelineOperation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailure {
    cause: ExecutionFailureCause,
    context: Option<ExecutionFailureContext>,
}

impl ExecutionFailure {
    pub const fn cause(&self) -> &ExecutionFailureCause {
        &self.cause
    }
    pub const fn context(&self) -> Option<ExecutionFailureContext> {
        self.context
    }

    pub fn detail(&self) -> &str {
        match &self.cause {
            ExecutionFailureCause::Pipeline(message) => message,
            ExecutionFailureCause::RequiredRow(error) => error.error().message(),
            ExecutionFailureCause::InvocationData(error) => error.message(),
            ExecutionFailureCause::ScalarInvocationData(error) => error.message(),
            ExecutionFailureCause::WindowInvocationData(error) => error.message(),
            ExecutionFailureCause::PreparationMetadata(error) => match error {
                crate::runtime::preparation_metadata::PreparationMetadataFailure::Request(_) => {
                    "metadata preparation request failed"
                }
                crate::runtime::preparation_metadata::PreparationMetadataFailure::Host(_) => {
                    "metadata preparation host refused"
                }
            },
            ExecutionFailureCause::RuntimeScalarSource(_) => "scalar runtime source request failed",
            ExecutionFailureCause::RuntimeScalarMemory(_) => "scalar runtime memory refused",
            ExecutionFailureCause::Kernel(error) => match error {
                KernelFailure::Cancelled => "kernel evaluation was cancelled",
                KernelFailure::DeadlineExceeded => "kernel evaluation deadline was exceeded",
                KernelFailure::ResourceExhausted => "kernel evaluation resources were exhausted",
                KernelFailure::InstanceFailed => "kernel instance has already failed",
                KernelFailure::InvalidProgram(message)
                | KernelFailure::Internal(message)
                | KernelFailure::Operational(message) => message.message(),
            },
        }
    }

    /// Attach the exact driver-local operation without formatting the cause
    /// into a different failure category or allocating a second diagnostic.
    pub fn at_operator(mut self, operator_ordinal: usize, operation: PipelineOperation) -> Self {
        if self.context.is_none() {
            self.context = Some(ExecutionFailureContext {
                operator_ordinal,
                operation,
            });
        }
        self
    }

    /// The original Direct graph returns only its original owned diagnostics.
    /// A nominal host cause here would violate the explicit Direct mode.
    pub(crate) fn into_pipeline_message(self) -> String {
        match self.cause {
            ExecutionFailureCause::Pipeline(message) => message,
            _ => unreachable!("a Direct graph cannot produce a nominal metadata host failure"),
        }
    }
}

impl From<crate::runtime::preparation_metadata::PreparationMetadataFailure> for ExecutionFailure {
    fn from(error: crate::runtime::preparation_metadata::PreparationMetadataFailure) -> Self {
        Self {
            cause: ExecutionFailureCause::PreparationMetadata(error),
            context: None,
        }
    }
}

impl From<String> for ExecutionFailure {
    fn from(message: String) -> Self {
        Self {
            cause: ExecutionFailureCause::Pipeline(message),
            context: None,
        }
    }
}
impl From<&str> for ExecutionFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl From<KernelFailure> for ExecutionFailure {
    fn from(error: KernelFailure) -> Self {
        Self {
            cause: ExecutionFailureCause::Kernel(error),
            context: None,
        }
    }
}
impl From<novarocks_functions::EvaluationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::EvaluationFailure) -> Self {
        match error {
            novarocks_functions::EvaluationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::EvaluationFailure::InvocationData(cause) => Self {
                cause: ExecutionFailureCause::InvocationData(cause),
                context: None,
            },
        }
    }
}
impl From<novarocks_functions::ScalarInvocationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::ScalarInvocationFailure) -> Self {
        match error {
            novarocks_functions::ScalarInvocationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::ScalarInvocationFailure::Data(cause) => Self {
                cause: ExecutionFailureCause::ScalarInvocationData(cause),
                context: None,
            },
        }
    }
}
impl From<crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure> for ExecutionFailure {
    fn from(error: crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure) -> Self {
        use crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure;
        let cause = match error {
            RuntimeScalarEvaluationFailure::Kernel(cause) => return cause.into(),
            RuntimeScalarEvaluationFailure::Data(cause) => {
                ExecutionFailureCause::ScalarInvocationData(cause)
            }
            RuntimeScalarEvaluationFailure::Source(cause) => {
                ExecutionFailureCause::RuntimeScalarSource(cause)
            }
            RuntimeScalarEvaluationFailure::Host(cause) => {
                ExecutionFailureCause::RuntimeScalarMemory(cause)
            }
        };
        Self {
            cause,
            context: None,
        }
    }
}
impl From<novarocks_functions::WindowEvaluationFailure> for ExecutionFailure {
    fn from(error: novarocks_functions::WindowEvaluationFailure) -> Self {
        match error {
            novarocks_functions::WindowEvaluationFailure::Kernel(cause) => cause.into(),
            novarocks_functions::WindowEvaluationFailure::InvocationData(cause) => Self {
                cause: ExecutionFailureCause::WindowInvocationData(cause),
                context: None,
            },
        }
    }
}
impl From<RequiredExpressionRowError> for ExecutionFailure {
    fn from(error: RequiredExpressionRowError) -> Self {
        Self {
            cause: ExecutionFailureCause::RequiredRow(error),
            context: None,
        }
    }
}
impl fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(context) = self.context {
            write!(
                f,
                "operator[{}] {:?}: ",
                context.operator_ordinal, context.operation
            )?;
        }
        match &self.cause {
            ExecutionFailureCause::Pipeline(message) => f.write_str(message),
            ExecutionFailureCause::Kernel(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeScalarSource(error) => error.fmt(f),
            ExecutionFailureCause::RuntimeScalarMemory(error) => error.fmt(f),
            ExecutionFailureCause::RequiredRow(error) => error.fmt(f),
            ExecutionFailureCause::InvocationData(error) => error.fmt(f),
            ExecutionFailureCause::ScalarInvocationData(error) => error.fmt(f),
            ExecutionFailureCause::WindowInvocationData(error) => error.fmt(f),
            ExecutionFailureCause::PreparationMetadata(error) => error.fmt(f),
        }
    }
}
impl Error for ExecutionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            ExecutionFailureCause::Pipeline(_) => None,
            ExecutionFailureCause::Kernel(error) => Some(error),
            ExecutionFailureCause::RuntimeScalarSource(error) => Some(error),
            ExecutionFailureCause::RuntimeScalarMemory(error) => Some(error),
            ExecutionFailureCause::RequiredRow(error) => Some(error),
            ExecutionFailureCause::InvocationData(error) => Some(error),
            ExecutionFailureCause::ScalarInvocationData(error) => Some(error),
            ExecutionFailureCause::WindowInvocationData(error) => Some(error),
            ExecutionFailureCause::PreparationMetadata(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn typed_failure_text_cannot_impersonate_a_kernel_category_or_replace_context() {
        let failure = ExecutionFailure::from("ResourceExhausted: kernel evaluation was cancelled")
            .at_operator(7, PipelineOperation::Push)
            .at_operator(41, PipelineOperation::Pull);
        assert!(matches!(
            failure.cause(),
            ExecutionFailureCause::Pipeline(_)
        ));
        assert_eq!(
            failure.context(),
            Some(ExecutionFailureContext {
                operator_ordinal: 7,
                operation: PipelineOperation::Push
            })
        );
        assert!(failure.source().is_none());
    }
    #[test]
    fn typed_failure_launch_cleanup_keeps_original_kernel_payload_and_source_chain() {
        use crate::runtime::fragment::{
            FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
        };
        let original = KernelFailure::Internal(KernelDiagnostic::new("original internal cause"));
        let error = FragmentLaunchError::from_failure(
            FragmentLaunchStage::BuildPipelines,
            FragmentLaunchErrorKind::PipelineBuild,
            original.clone().into(),
        )
        .with_cleanup_diagnostics(vec!["secondary cleanup failure".into()]);
        assert_eq!(
            error.cause().cause(),
            &ExecutionFailureCause::Kernel(original.clone())
        );
        assert_eq!(error.cleanup_diagnostics(), &["secondary cleanup failure"]);
        assert_eq!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<KernelFailure>(),
            Some(&original)
        );
    }
}
