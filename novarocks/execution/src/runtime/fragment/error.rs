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
pub enum FragmentLaunchStage {
    ValidateSubmission,
    Register,
    BuildRuntimeState,
    Materialize,
    BuildPipelines,
    Start,
    Rollback,
}

impl fmt::Display for FragmentLaunchStage {
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
pub enum FragmentLaunchErrorKind {
    Binding,
    DuplicateRegistration,
    ResourceUnavailable,
    Materialization,
    PipelineBuild,
    Start,
    Rollback,
}

impl fmt::Display for FragmentLaunchErrorKind {
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
pub struct FragmentLaunchError {
    stage: FragmentLaunchStage,
    kind: FragmentLaunchErrorKind,
    detail: String,
    cleanup_diagnostics: Vec<String>,
}

impl FragmentLaunchError {
    pub fn new(
        stage: FragmentLaunchStage,
        kind: FragmentLaunchErrorKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            stage,
            kind,
            detail: detail.into(),
            cleanup_diagnostics: Vec::new(),
        }
    }

    pub fn stage(&self) -> FragmentLaunchStage {
        self.stage
    }

    pub fn kind(&self) -> FragmentLaunchErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub fn cleanup_diagnostics(&self) -> &[String] {
        &self.cleanup_diagnostics
    }

    pub fn with_cleanup_diagnostics(mut self, diagnostics: Vec<String>) -> Self {
        self.cleanup_diagnostics.extend(diagnostics);
        self
    }
}

impl fmt::Display for FragmentLaunchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment launch error during {} ({}): {}",
            self.stage, self.kind, self.detail
        )?;
        if !self.cleanup_diagnostics.is_empty() {
            write!(
                f,
                "; cleanup diagnostics: {}",
                self.cleanup_diagnostics.join("; ")
            )?;
        }
        Ok(())
    }
}

impl Error for FragmentLaunchError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentExecutionErrorKind {
    Pipeline,
    Sink,
    Exchange,
    RuntimeFilter,
    Cancelled,
    Panic,
}

impl fmt::Display for FragmentExecutionErrorKind {
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
pub struct FragmentExecutionError {
    kind: FragmentExecutionErrorKind,
    detail: String,
}

impl FragmentExecutionError {
    pub fn new(kind: FragmentExecutionErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub fn kind(&self) -> FragmentExecutionErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for FragmentExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "fragment execution error ({}): {}",
            self.kind, self.detail
        )
    }
}

impl Error for FragmentExecutionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_error_keeps_stage_kind_and_detail() {
        let error = FragmentLaunchError::new(
            FragmentLaunchStage::Rollback,
            FragmentLaunchErrorKind::Rollback,
            "exchange registration cleanup failed",
        );
        assert_eq!(error.stage(), FragmentLaunchStage::Rollback);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Rollback);
        assert_eq!(error.detail(), "exchange registration cleanup failed");
        assert_eq!(
            error.to_string(),
            "fragment launch error during rollback (rollback): exchange registration cleanup failed"
        );
    }

    #[test]
    fn execution_cancelled_is_a_typed_terminal_outcome() {
        let error = FragmentExecutionError::new(
            FragmentExecutionErrorKind::Cancelled,
            "query cancelled by coordinator",
        );
        assert_eq!(error.kind(), FragmentExecutionErrorKind::Cancelled);
        assert_eq!(error.detail(), "query cancelled by coordinator");
        assert_eq!(
            error.to_string(),
            "fragment execution error (cancelled): query cancelled by coordinator"
        );
    }

    #[test]
    fn runtime_fragment_vocabulary_labels_are_stable() {
        for (stage, expected) in [
            (
                FragmentLaunchStage::ValidateSubmission,
                "submission validation",
            ),
            (FragmentLaunchStage::Register, "registration"),
            (
                FragmentLaunchStage::BuildRuntimeState,
                "runtime state construction",
            ),
            (FragmentLaunchStage::Materialize, "materialization"),
            (FragmentLaunchStage::BuildPipelines, "pipeline construction"),
            (FragmentLaunchStage::Start, "start"),
            (FragmentLaunchStage::Rollback, "rollback"),
        ] {
            assert_eq!(stage.to_string(), expected);
        }

        for (kind, expected) in [
            (FragmentLaunchErrorKind::Binding, "binding"),
            (
                FragmentLaunchErrorKind::DuplicateRegistration,
                "duplicate registration",
            ),
            (
                FragmentLaunchErrorKind::ResourceUnavailable,
                "resource unavailable",
            ),
            (FragmentLaunchErrorKind::Materialization, "materialization"),
            (FragmentLaunchErrorKind::PipelineBuild, "pipeline build"),
            (FragmentLaunchErrorKind::Start, "start"),
            (FragmentLaunchErrorKind::Rollback, "rollback"),
        ] {
            assert_eq!(kind.to_string(), expected);
        }

        for (kind, expected) in [
            (FragmentExecutionErrorKind::Pipeline, "pipeline"),
            (FragmentExecutionErrorKind::Sink, "sink"),
            (FragmentExecutionErrorKind::Exchange, "exchange"),
            (FragmentExecutionErrorKind::RuntimeFilter, "runtime filter"),
            (FragmentExecutionErrorKind::Cancelled, "cancelled"),
            (FragmentExecutionErrorKind::Panic, "panic"),
        ] {
            assert_eq!(kind.to_string(), expected);
        }
    }
}
