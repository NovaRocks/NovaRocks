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

use std::error::Error;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalTaskLaunchStage {
    ValidateSubmission,
    Register,
    BuildRuntimeState,
    Materialize,
    BuildPipelines,
    Start,
    Rollback,
}

impl fmt::Display for LocalTaskLaunchStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ValidateSubmission => f.write_str("submission validation"),
            Self::Register => f.write_str("registration"),
            Self::BuildRuntimeState => f.write_str("runtime state construction"),
            Self::Materialize => f.write_str("materialization"),
            Self::BuildPipelines => f.write_str("pipeline construction"),
            Self::Start => f.write_str("start"),
            Self::Rollback => f.write_str("rollback"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalTaskLaunchErrorKind {
    Binding,
    DuplicateRegistration,
    ResourceUnavailable,
    Materialization,
    PipelineBuild,
    Start,
    Rollback,
}

impl fmt::Display for LocalTaskLaunchErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding => f.write_str("binding"),
            Self::DuplicateRegistration => f.write_str("duplicate registration"),
            Self::ResourceUnavailable => f.write_str("resource unavailable"),
            Self::Materialization => f.write_str("materialization"),
            Self::PipelineBuild => f.write_str("pipeline build"),
            Self::Start => f.write_str("start"),
            Self::Rollback => f.write_str("rollback"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTaskLaunchError {
    stage: LocalTaskLaunchStage,
    kind: LocalTaskLaunchErrorKind,
    cause: super::ExecutionFailure,
    cleanup_diagnostics: Vec<super::ExecutionFailure>,
}

impl LocalTaskLaunchError {
    pub fn from_failure(
        stage: LocalTaskLaunchStage,
        kind: LocalTaskLaunchErrorKind,
        cause: super::ExecutionFailure,
    ) -> Self {
        Self {
            stage,
            kind,
            cause,
            cleanup_diagnostics: Vec::new(),
        }
    }
    pub fn stage(&self) -> LocalTaskLaunchStage {
        self.stage
    }
    pub fn kind(&self) -> LocalTaskLaunchErrorKind {
        self.kind
    }
    pub fn detail(&self) -> &str {
        self.cause.detail()
    }
    pub const fn cause(&self) -> &super::ExecutionFailure {
        &self.cause
    }
    pub fn cleanup_diagnostics(&self) -> &[super::ExecutionFailure] {
        &self.cleanup_diagnostics
    }
    pub fn with_cleanup_diagnostics(mut self, diagnostics: Vec<super::ExecutionFailure>) -> Self {
        self.cleanup_diagnostics.extend(diagnostics);
        self
    }
}
impl fmt::Display for LocalTaskLaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment launch error during {} ({}): {}",
            self.stage, self.kind, self.cause
        )?;
        if !self.cleanup_diagnostics.is_empty() {
            f.write_str("; cleanup diagnostics: ")?;
            for (index, cause) in self.cleanup_diagnostics.iter().enumerate() {
                if index != 0 {
                    f.write_str("; ")?;
                }
                cause.fmt(f)?;
            }
        }
        Ok(())
    }
}
impl novarocks_failure_contract::FailureProjection for LocalTaskLaunchError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.cause.failure_fact()
    }
}

impl Error for LocalTaskLaunchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalTaskExecutionErrorKind {
    Pipeline,
    Sink,
    Exchange,
    RuntimeFilter,
    Cancelled,
    Panic,
}

impl fmt::Display for LocalTaskExecutionErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipeline => f.write_str("pipeline"),
            Self::Sink => f.write_str("sink"),
            Self::Exchange => f.write_str("exchange"),
            Self::RuntimeFilter => f.write_str("runtime filter"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::Panic => f.write_str("panic"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTaskExecutionError {
    kind: LocalTaskExecutionErrorKind,
    cause: super::ExecutionFailure,
}

impl LocalTaskExecutionError {
    pub fn from_failure(kind: LocalTaskExecutionErrorKind, cause: super::ExecutionFailure) -> Self {
        Self { kind, cause }
    }

    pub fn kind(&self) -> LocalTaskExecutionErrorKind {
        self.kind
    }
    pub fn detail(&self) -> &str {
        self.cause.detail()
    }
    pub const fn cause(&self) -> &super::ExecutionFailure {
        &self.cause
    }
}

impl fmt::Display for LocalTaskExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment execution error ({}): {}",
            self.kind, self.cause
        )
    }
}

impl novarocks_failure_contract::FailureProjection for LocalTaskExecutionError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.cause.failure_fact()
    }
}

impl Error for LocalTaskExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_error_keeps_stage_kind_and_detail() {
        let error = LocalTaskLaunchError::from_failure(
            LocalTaskLaunchStage::Rollback,
            LocalTaskLaunchErrorKind::Rollback,
            novarocks_functions::KernelFailure::Internal(
                novarocks_functions::KernelDiagnostic::new("exchange registration cleanup failed"),
            )
            .into(),
        );
        assert_eq!(error.stage(), LocalTaskLaunchStage::Rollback);
        assert_eq!(error.kind(), LocalTaskLaunchErrorKind::Rollback);
        assert_eq!(error.detail(), "exchange registration cleanup failed");
        assert_eq!(
            error.to_string(),
            "fragment launch error during rollback (rollback): kernel internal failure: exchange registration cleanup failed"
        );
    }

    #[test]
    fn execution_envelope_keeps_the_original_control_cause() {
        let cause: super::super::ExecutionFailure =
            novarocks_functions::KernelFailure::Cancelled.into();
        let error = LocalTaskExecutionError::from_failure(
            LocalTaskExecutionErrorKind::Cancelled,
            cause.clone(),
        );
        assert!(error.cause().same_occurrence(&cause));
        assert_eq!(error.kind(), LocalTaskExecutionErrorKind::Cancelled);
        assert_eq!(error.detail(), "kernel evaluation was cancelled");
        assert_eq!(
            error.to_string(),
            "fragment execution error (cancelled): kernel evaluation was cancelled"
        );
    }

    #[test]
    fn envelopes_forward_the_primary_fact_and_source_independent_of_stage_kind_or_cleanup() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for (cause, class) in [
            (
                novarocks_functions::KernelFailure::ResourceExhausted,
                FailureClass::Resource,
            ),
            (
                novarocks_functions::KernelFailure::Cancelled,
                FailureClass::Control,
            ),
            (
                novarocks_functions::KernelFailure::InvalidProgram(
                    novarocks_functions::KernelDiagnostic::new("ResourceExhausted"),
                ),
                FailureClass::Invariant,
            ),
            (
                novarocks_functions::KernelFailure::Operational(
                    novarocks_functions::KernelDiagnostic::new("numeric overflow"),
                ),
                FailureClass::Unexpected,
            ),
        ] {
            let original: super::super::ExecutionFailure = cause.into();
            let fact = original.failure_fact();
            assert_eq!(fact.class(), class);
            let launch = LocalTaskLaunchError::from_failure(
                LocalTaskLaunchStage::Rollback,
                LocalTaskLaunchErrorKind::ResourceUnavailable,
                original.clone(),
            )
            .with_cleanup_diagnostics(vec![
                novarocks_functions::KernelFailure::Internal(
                    novarocks_functions::KernelDiagnostic::new("cleanup"),
                )
                .into(),
            ]);
            assert_eq!(launch.failure_fact(), fact);
            let source = launch
                .source()
                .unwrap()
                .downcast_ref::<super::super::ExecutionFailure>()
                .unwrap();
            assert!(source.same_occurrence(&original));
            let execution = LocalTaskExecutionError::from_failure(
                LocalTaskExecutionErrorKind::Panic,
                original.clone(),
            );
            assert_eq!(execution.failure_fact(), fact);
            let source = execution
                .source()
                .unwrap()
                .downcast_ref::<super::super::ExecutionFailure>()
                .unwrap();
            assert!(source.same_occurrence(&original));
        }
    }

    #[test]
    fn runtime_fragment_vocabulary_labels_are_stable() {
        for (stage, expected) in [
            (
                LocalTaskLaunchStage::ValidateSubmission,
                "submission validation",
            ),
            (LocalTaskLaunchStage::Register, "registration"),
            (
                LocalTaskLaunchStage::BuildRuntimeState,
                "runtime state construction",
            ),
            (LocalTaskLaunchStage::Materialize, "materialization"),
            (
                LocalTaskLaunchStage::BuildPipelines,
                "pipeline construction",
            ),
            (LocalTaskLaunchStage::Start, "start"),
            (LocalTaskLaunchStage::Rollback, "rollback"),
        ] {
            assert_eq!(stage.to_string(), expected);
        }

        for (kind, expected) in [
            (LocalTaskLaunchErrorKind::Binding, "binding"),
            (
                LocalTaskLaunchErrorKind::DuplicateRegistration,
                "duplicate registration",
            ),
            (
                LocalTaskLaunchErrorKind::ResourceUnavailable,
                "resource unavailable",
            ),
            (LocalTaskLaunchErrorKind::Materialization, "materialization"),
            (LocalTaskLaunchErrorKind::PipelineBuild, "pipeline build"),
            (LocalTaskLaunchErrorKind::Start, "start"),
            (LocalTaskLaunchErrorKind::Rollback, "rollback"),
        ] {
            assert_eq!(kind.to_string(), expected);
        }

        for (kind, expected) in [
            (LocalTaskExecutionErrorKind::Pipeline, "pipeline"),
            (LocalTaskExecutionErrorKind::Sink, "sink"),
            (LocalTaskExecutionErrorKind::Exchange, "exchange"),
            (LocalTaskExecutionErrorKind::RuntimeFilter, "runtime filter"),
            (LocalTaskExecutionErrorKind::Cancelled, "cancelled"),
            (LocalTaskExecutionErrorKind::Panic, "panic"),
        ] {
            assert_eq!(kind.to_string(), expected);
        }
    }
}
