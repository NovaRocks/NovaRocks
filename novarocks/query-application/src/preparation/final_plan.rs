// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query-application ownership of final physical-plan completion.
//!
//! SQL publishes immutable typed needs and consumes exact fact batches. This
//! module owns the asynchronous application loop around that pure protocol,
//! statement cancellation, the admitted `WorkScope`, and publication of the
//! validated immutable plan candidate. Connector handles, native DTOs, task
//! placement, encoders, and runtime objects remain outside this boundary.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use novarocks_physical_plan::{
    ConstantReferenceError, PhysicalPlan, PlanConstructionError, validate_plan_observed,
};
use novarocks_sql::compiler::{
    ExplainRenderBudget, SqlAuthoredPhysicalPlan, SqlCompileProgress, SqlCompiler,
    SqlDisplayAnnotation, SqlDisplayIntent, SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch,
    render_completed_plan, render_completed_plan_tree,
};
use novarocks_workload_control::{CancellationView, WorkScope};

use super::runtime_access::{
    CompletedPlanWithAccess, FinalPlanAccessError, FrozenReadAccess, ReadAccessSink,
};

/// Immutable, fully validated final plan held by query application before any
/// runtime projection is built. There is deliberately no mutable-plan or
/// unchecked-plan constructor.
#[derive(Clone, Debug)]
pub struct CompletedPhysicalPlanCandidate {
    source: CompletedPlanSource,
    display_intent: SqlDisplayIntent,
    display_annotations: Arc<[SqlDisplayAnnotation]>,
}

/// Keep SQL's original immutable source journal alongside its physical view.
/// Structural programs carry no SQL source certification.
#[derive(Clone, Debug)]
enum CompletedPlanSource {
    Sql(Arc<SqlAuthoredPhysicalPlan>),
    Program(Arc<PhysicalPlan>),
}

impl CompletedPhysicalPlanCandidate {
    fn from_completed(
        completed: novarocks_sql::compiler::SqlCompletedPlan,
        control: &novarocks_sql::compiler::SqlCompileControl,
    ) -> Result<Self, FinalPlanCompletionError> {
        let (plan, display_intent, display_annotations) = completed.into_parts();
        Self::try_new_sql(plan, display_intent, display_annotations, control)
    }

    /// Hold an application-authored structural program without a SQL source
    /// journal. SQL statement and DML results use `for_sql_program` so their
    /// complete immutable lowering source remains available to later owners.
    pub fn for_program(
        plan: PhysicalPlan,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, FinalPlanCompletionError> {
        validate_candidate_plan(&plan, control)?;
        Ok(Self {
            source: CompletedPlanSource::Program(Arc::new(plan)),
            display_intent: SqlDisplayIntent::Execute,
            display_annotations: Arc::from([]),
        })
    }

    /// Preserve SQL's complete source owner for an internal statement or DML
    /// program, including its original aggregate logical-source journal.
    pub fn for_sql_program(
        source: SqlAuthoredPhysicalPlan,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, FinalPlanCompletionError> {
        Self::try_new_sql(source, SqlDisplayIntent::Execute, Box::default(), control)
    }

    fn try_new_sql(
        source: SqlAuthoredPhysicalPlan,
        display_intent: SqlDisplayIntent,
        display_annotations: Box<[SqlDisplayAnnotation]>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, FinalPlanCompletionError> {
        validate_candidate_plan(source.plan(), control)?;
        Ok(Self {
            source: CompletedPlanSource::Sql(Arc::new(source)),
            display_intent,
            display_annotations: display_annotations.into(),
        })
    }

    /// Author every fragment's package semantics from SQL's original source
    /// journal. A structural program has no SQL journal; it is refused rather
    /// than given fabricated call scopes.
    pub fn author_package_semantics(
        &self,
        statement_constant_policy: novarocks_physical_plan::ConstantPolicy,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        std::collections::BTreeMap<
            novarocks_physical_plan::FragmentId,
            novarocks_sql::compiler::FragmentPackageSemantics,
        >,
        novarocks_sql::compiler::PackageSemanticsError,
    > {
        match &self.source {
            CompletedPlanSource::Sql(source) => {
                novarocks_sql::compiler::author_fragment_package_semantics(
                    source,
                    statement_constant_policy,
                    control,
                )
            }
            CompletedPlanSource::Program(_) => Err(
                novarocks_sql::compiler::PackageSemanticsError::InvalidSource(
                    "a structural program has no SQL call source journal",
                ),
            ),
        }
    }

    /// Same immutable source's publication-proved public output obligation.
    /// No caller can replace it with another plan or supplied result schema.
    pub fn original_public_result_port(&self) -> Option<&novarocks_physical_plan::ResultPort> {
        match &self.source {
            CompletedPlanSource::Sql(source) => source
                .original_public_result_declaration()
                .map(|loan| loan.original_port()),
            CompletedPlanSource::Program(plan) => plan.result_port(),
        }
    }

    /// Consume the same source before any plan aliases are published.
    pub fn freeze_root_output(
        self,
        output: novarocks_result_contract::FrozenRootOutput,
    ) -> Result<Self, FinalPlanCompletionError> {
        let contract = novarocks_result_contract::RootOutputContract::new(
            novarocks_result_contract::RootProfileId::V1,
            output,
        );
        let source = match self.source {
            CompletedPlanSource::Sql(source) => {
                let source =
                    Arc::try_unwrap(source).map_err(|_| FinalPlanCompletionError::Compiler {
                        message: Arc::from(
                            "root purpose must be frozen before plan aliases are published",
                        ),
                    })?;
                CompletedPlanSource::Sql(Arc::new(source.with_root_output(contract).map_err(
                    |error| FinalPlanCompletionError::InvalidPlan {
                        message: Arc::from(error.to_string()),
                    },
                )?))
            }
            CompletedPlanSource::Program(plan) => {
                let plan =
                    Arc::try_unwrap(plan).map_err(|_| FinalPlanCompletionError::Compiler {
                        message: Arc::from(
                            "root purpose must be frozen before plan aliases are published",
                        ),
                    })?;
                CompletedPlanSource::Program(Arc::new(plan.with_root_output(contract).map_err(
                    |error| FinalPlanCompletionError::InvalidPlan {
                        message: Arc::from(error.to_string()),
                    },
                )?))
            }
        };
        Ok(Self {
            source,
            display_intent: self.display_intent,
            display_annotations: self.display_annotations,
        })
    }

    pub fn plan(&self) -> &Arc<PhysicalPlan> {
        match &self.source {
            CompletedPlanSource::Sql(source) => source.plan_arc(),
            CompletedPlanSource::Program(plan) => plan,
        }
    }

    /// SQL's completed display semantics are part of the same immutable
    /// artifact as the plan. A renderer may inspect them but cannot ask SQL
    /// to analyze or optimize the statement again.
    pub const fn display_intent(&self) -> SqlDisplayIntent {
        self.display_intent
    }

    pub fn display_annotations(&self) -> &[SqlDisplayAnnotation] {
        &self.display_annotations
    }

    /// Render this plan as ordinary EXPLAIN.
    ///
    /// The plan is already complete, so rendering asks nothing of anyone: no
    /// catalog, no provider, and no task. A statement compiled to be executed
    /// has no EXPLAIN text to give, and says so rather than inventing one.
    pub fn render_explain_lines(
        &self,
        budget: ExplainRenderBudget,
        control: &novarocks_sql::compiler::SqlCompileControl,
    ) -> Result<Vec<String>, FinalPlanCompletionError> {
        let SqlDisplayIntent::Explain { level, analyze } = self.display_intent else {
            return Err(FinalPlanCompletionError::Compiler {
                message: Arc::from("completed plan does not carry EXPLAIN display intent"),
            });
        };
        if analyze {
            return Err(FinalPlanCompletionError::Compiler {
                message: Arc::from("EXPLAIN ANALYZE requires a profile bound to this plan version"),
            });
        }
        // What EXPLAIN answers is what the statement will do, which is the
        // operator tree. What the plan states to the backend is a different
        // question, and it has its own level.
        let lines = if matches!(level, novarocks_sql::compiler::ExplainLevel::Contract) {
            render_completed_plan(
                self.plan(),
                &self.display_annotations,
                level,
                None,
                budget,
                control,
            )
        } else {
            render_completed_plan_tree(self.plan(), level, control)
        };
        lines.map_err(compiler_error)
    }
}

fn validate_candidate_plan(
    plan: &PhysicalPlan,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), FinalPlanCompletionError> {
    validate_plan_observed(plan, control).map_err(|error| match error {
        PlanConstructionError::Constants(ConstantReferenceError::Control(error)) => {
            compiler_error(novarocks_sql::compiler::SqlCompileError::from(error))
        }
        error => FinalPlanCompletionError::InvalidPlan {
            message: Arc::from(error.to_string()),
        },
    })
}

/// Role composition supplies the application-owned adapter that resolves the
/// current exact SQL need batch. A fact source cannot retain compiler state,
/// mint a plan, or substitute a different need batch.
///
/// Answering a need can require freezing a provider read, which yields a
/// runtime capability that must not travel with the facts. The sink is where
/// such a capability goes the instant it is taken, so the driver accounts for
/// it whether or not this call goes on to succeed. A source that freezes
/// nothing never touches it.
#[async_trait]
pub trait SqlCompletionFactSource: Send + Sync {
    /// What performing a frozen read requires at runtime. A source that
    /// freezes nothing uses `()`.
    type Access: Send;

    async fn resolve(
        &self,
        needs: &SqlNeedBatch,
        taken: &ReadAccessSink<Self::Access>,
    ) -> Result<SqlFactBatch, String>;
}

/// A business verdict and the original physical-worker health are independent.
pub type SqlCompletionCpuResult<T> =
    Result<Result<T, FinalPlanCompletionError>, crate::api::StatementNativeHealthFailure>;

/// The role explicitly supplies the three pure SQL operations. Metadata stays
/// in the driver's single resolve loop; this port grants no execution capacity.
#[async_trait]
pub trait SqlCompletionCpuPort: Send + Sync {
    async fn start(
        &self,
        request: SqlFinalPlanCompileRequest,
    ) -> SqlCompletionCpuResult<SqlCompileProgress>;
    async fn finish(
        &self,
        compilation: novarocks_sql::compiler::SqlCompilation,
        facts: SqlFactBatch,
        control: novarocks_sql::compiler::SqlCompileControl,
    ) -> SqlCompletionCpuResult<SqlCompileProgress>;
    async fn completed_candidate(
        &self,
        completed: novarocks_sql::compiler::SqlCompletedPlan,
        control: novarocks_sql::compiler::SqlCompileControl,
    ) -> SqlCompletionCpuResult<CompletedPhysicalPlanCandidate>;
}

/// The original pure operations and error projections. Scheduling is supplied
/// by the explicit role port, never selected or defaulted inside SQL.
pub struct SqlCompletionPureSteps;
impl SqlCompletionPureSteps {
    pub fn start(
        request: SqlFinalPlanCompileRequest,
    ) -> Result<SqlCompileProgress, FinalPlanCompletionError> {
        let control = request.control().clone();
        SqlCompiler::start(
            request.try_into_completion().map_err(compiler_error)?,
            &control,
        )
        .map_err(compiler_progress_error)
    }
    pub fn finish(
        compilation: novarocks_sql::compiler::SqlCompilation,
        facts: SqlFactBatch,
        control: &novarocks_sql::compiler::SqlCompileControl,
    ) -> Result<SqlCompileProgress, FinalPlanCompletionError> {
        SqlCompiler::finish(compilation, facts, control).map_err(compiler_progress_error)
    }
    pub fn completed_candidate(
        completed: novarocks_sql::compiler::SqlCompletedPlan,
        control: &novarocks_sql::compiler::SqlCompileControl,
    ) -> Result<CompletedPhysicalPlanCandidate, FinalPlanCompletionError> {
        CompletedPhysicalPlanCandidate::from_completed(completed, control)
    }
}

/// Drives one query's pure SQL completion protocol under its admitted scope.
///
/// The driver carries no resource authority. It interrupts the pending role
/// adapter on statement cancellation or the frozen deadline, and owns every
/// runtime capability the completion takes until it either publishes them with
/// a plan or returns them unpaired. Warehouse concurrency is acquired once by
/// the query owner before this driver starts; preparation has no second queue.
pub struct FinalPlanCompletionDriver<A> {
    facts: Arc<dyn SqlCompletionFactSource<Access = A>>,
}

impl<A: Send> FinalPlanCompletionDriver<A> {
    pub fn new(facts: Arc<dyn SqlCompletionFactSource<Access = A>>) -> Self {
        Self { facts }
    }

    /// Complete one statement into a plan and the capabilities its scans were
    /// frozen with.
    ///
    /// The two are published together or not at all. Every path that does not
    /// publish returns the capabilities taken on the way, because a capability
    /// whose plan never appeared still has an owner waiting to release it.
    pub async fn complete(
        &self,
        request: SqlFinalPlanCompileRequest,
        scope: &WorkScope,
        taken: &ReadAccessSink<A>,
        cpu: &dyn SqlCompletionCpuPort,
    ) -> SqlCompletionCpuResult<CompletedPhysicalPlanCandidate> {
        let control = request.control().clone();
        if let Err(error) = scope.check() {
            return Ok(Err(governance_error(error)));
        }
        let cancellation = match scope.cancellation() {
            Ok(cancellation) => cancellation,
            Err(error) => return Ok(Err(governance_error(error))),
        };
        let mut progress = match cpu.start(request).await? {
            Ok(progress) => progress,
            Err(error) => return Ok(Err(error)),
        };
        loop {
            if let Err(error) = scope.check() {
                return Ok(Err(governance_error(error)));
            }
            progress = match progress {
                SqlCompileProgress::Complete(completed) => {
                    return cpu.completed_candidate(completed, control).await;
                }
                SqlCompileProgress::Incomplete(compilation) => {
                    let facts = match self
                        .resolve(
                            &cancellation,
                            control.deadline(),
                            compilation.needs(),
                            taken,
                        )
                        .await
                    {
                        Ok(facts) => facts,
                        Err(error) => return Ok(Err(error)),
                    };
                    match cpu.finish(compilation, facts, control.clone()).await? {
                        Ok(progress) => progress,
                        Err(error) => return Ok(Err(error)),
                    }
                }
            };
        }
    }

    /// Consume the original Source sink only after the borrowed loop returns.
    pub fn pair_completed(
        candidate: Result<CompletedPhysicalPlanCandidate, FinalPlanCompletionError>,
        taken: ReadAccessSink<A>,
    ) -> Result<CompletedPlanWithAccess<A>, FinalPlanCompletionFailure<A>> {
        let candidate = match candidate {
            Ok(candidate) => candidate,
            Err(error) => return Err(FinalPlanCompletionFailure::new(error, taken.into_taken())),
        };
        let access = match taken.try_into_access() {
            Ok(access) => access,
            Err((error, taken)) => {
                return Err(FinalPlanCompletionFailure::new(access_error(error), taken));
            }
        };
        CompletedPlanWithAccess::try_pair(candidate, access).map_err(|(error, access)| {
            FinalPlanCompletionFailure::new(access_error(error), access.into_taken())
        })
    }

    async fn resolve(
        &self,
        cancellation: &CancellationView,
        deadline: Option<Instant>,
        needs: &SqlNeedBatch,
        taken: &ReadAccessSink<A>,
    ) -> Result<SqlFactBatch, FinalPlanCompletionError> {
        let future = self.facts.resolve(needs, taken);
        match deadline {
            Some(deadline) => {
                tokio::select! {
                    reason = cancellation.cancelled() => Err(FinalPlanCompletionError::Cancelled { reason: Arc::from(format!("{reason:?}")) }),
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Err(FinalPlanCompletionError::DeadlineExceeded),
                    result = future => result.map_err(|message| FinalPlanCompletionError::FactSource { message: Arc::from(message) }),
                }
            }
            None => {
                tokio::select! {
                    reason = cancellation.cancelled() => Err(FinalPlanCompletionError::Cancelled { reason: Arc::from(format!("{reason:?}")) }),
                    result = future => result.map_err(|message| FinalPlanCompletionError::FactSource { message: Arc::from(message) }),
                }
            }
        }
    }
}

#[cfg(test)]
struct PureSqlCompletionCpuFixture;
#[cfg(test)]
#[async_trait]
impl SqlCompletionCpuPort for PureSqlCompletionCpuFixture {
    async fn start(
        &self,
        request: SqlFinalPlanCompileRequest,
    ) -> SqlCompletionCpuResult<SqlCompileProgress> {
        Ok(SqlCompletionPureSteps::start(request))
    }
    async fn finish(
        &self,
        compilation: novarocks_sql::compiler::SqlCompilation,
        facts: SqlFactBatch,
        control: novarocks_sql::compiler::SqlCompileControl,
    ) -> SqlCompletionCpuResult<SqlCompileProgress> {
        Ok(SqlCompletionPureSteps::finish(compilation, facts, &control))
    }
    async fn completed_candidate(
        &self,
        completed: novarocks_sql::compiler::SqlCompletedPlan,
        control: novarocks_sql::compiler::SqlCompileControl,
    ) -> SqlCompletionCpuResult<CompletedPhysicalPlanCandidate> {
        Ok(SqlCompletionPureSteps::completed_candidate(
            completed, &control,
        ))
    }
}
#[cfg(test)]
impl<A: Send> FinalPlanCompletionDriver<A> {
    pub(crate) async fn complete_for_test(
        &self,
        request: SqlFinalPlanCompileRequest,
        scope: &WorkScope,
    ) -> Result<CompletedPlanWithAccess<A>, FinalPlanCompletionFailure<A>> {
        let taken = ReadAccessSink::new();
        let candidate = self
            .complete(request, scope, &taken, &PureSqlCompletionCpuFixture)
            .await
            .expect("pure fixture has no physical worker");
        Self::pair_completed(candidate, taken)
    }
}

/// A completion that published nothing, and the capabilities it took before it
/// stopped.
///
/// The capabilities leave with the failure because the alternative is dropping
/// them here, where their owner cannot see that they were ever taken.
pub struct FinalPlanCompletionFailure<A> {
    error: FinalPlanCompletionError,
    taken: Vec<FrozenReadAccess<A>>,
}

impl<A> FinalPlanCompletionFailure<A> {
    const fn new(error: FinalPlanCompletionError, taken: Vec<FrozenReadAccess<A>>) -> Self {
        Self { error, taken }
    }

    pub const fn error(&self) -> &FinalPlanCompletionError {
        &self.error
    }

    pub fn into_error(self) -> FinalPlanCompletionError {
        self.error
    }

    /// The capabilities taken before the failure, for their owner to release.
    pub fn into_taken(self) -> Vec<FrozenReadAccess<A>> {
        self.taken
    }

    pub fn into_parts(self) -> (FinalPlanCompletionError, Vec<FrozenReadAccess<A>>) {
        (self.error, self.taken)
    }
}

/// Written without asking the capability to be printable: what a reader needs
/// here is why completion stopped and how much is owed back.
impl<A> fmt::Debug for FinalPlanCompletionFailure<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FinalPlanCompletionFailure")
            .field("error", &self.error)
            .field("capabilities_to_release", &self.taken.len())
            .finish()
    }
}

impl<A> fmt::Display for FinalPlanCompletionFailure<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl<A> std::error::Error for FinalPlanCompletionFailure<A> {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalPlanCompletionError {
    Governance {
        message: Arc<str>,
    },
    Cancelled {
        reason: Arc<str>,
    },
    DeadlineExceeded,
    ResourceExhausted,
    Compiler {
        message: Arc<str>,
    },
    /// The original compiler domain cause, without reclassifying its fact.
    Source {
        source: novarocks_execution_contract::failure::SharedFailureCause,
    },
    /// A statement the analyzer rejected, kept as the analyzer stated it.
    ///
    /// Its code, its phase and the place in the text it points at are what a
    /// client is told; flattening it to a message would leave the client with
    /// the words and none of the three.
    Analyze {
        error: novarocks_sql::analyze_error::AnalyzeError,
    },
    FactSource {
        message: Arc<str>,
    },
    InvalidPlan {
        message: Arc<str>,
    },
    /// The plan and the capabilities do not account for each other, so neither
    /// is publishable.
    AccessCoverage {
        message: Arc<str>,
    },
}

impl fmt::Display for FinalPlanCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Governance { message }
            | Self::Compiler { message }
            | Self::FactSource { message }
            | Self::InvalidPlan { message }
            | Self::AccessCoverage { message } => formatter.write_str(message),
            Self::Cancelled { reason } => {
                write!(formatter, "final plan completion cancelled: {reason}")
            }
            Self::DeadlineExceeded => {
                formatter.write_str("final plan completion deadline exceeded")
            }
            Self::ResourceExhausted => {
                formatter.write_str("final plan completion resource budget was exhausted")
            }
            Self::Analyze { error } => error.fmt(formatter),
            Self::Source { source } => source.fmt(formatter),
        }
    }
}

impl std::error::Error for FinalPlanCompletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source { source } => Some(source),
            _ => None,
        }
    }
}

fn access_error(error: FinalPlanAccessError) -> FinalPlanCompletionError {
    FinalPlanCompletionError::AccessCoverage {
        message: Arc::from(error.to_string()),
    }
}

fn governance_error(error: novarocks_workload_control::WorkError) -> FinalPlanCompletionError {
    FinalPlanCompletionError::Governance {
        message: Arc::from(error.to_string()),
    }
}

fn compiler_error(error: novarocks_sql::compiler::SqlCompileError) -> FinalPlanCompletionError {
    match error {
        novarocks_sql::compiler::SqlCompileError::Cancelled => {
            FinalPlanCompletionError::Cancelled {
                reason: Arc::from("SQL compiler control"),
            }
        }
        novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
            FinalPlanCompletionError::DeadlineExceeded
        }
        novarocks_sql::compiler::SqlCompileError::ResourceExhausted => {
            FinalPlanCompletionError::ResourceExhausted
        }
        novarocks_sql::compiler::SqlCompileError::Analyze(error) => {
            FinalPlanCompletionError::Analyze { error }
        }
        novarocks_sql::compiler::SqlCompileError::CompilationSource(source) => {
            FinalPlanCompletionError::Source { source }
        }
        error => FinalPlanCompletionError::Compiler {
            message: Arc::from(error.to_string()),
        },
    }
}

fn compiler_progress_error(
    error: novarocks_sql::compiler::SqlCompileProgressError,
) -> FinalPlanCompletionError {
    match error {
        novarocks_sql::compiler::SqlCompileProgressError::Compile(error) => compiler_error(error),
        error => FinalPlanCompletionError::Compiler {
            message: Arc::from(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_physical_plan::{
        MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
    };
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, ExplainLevel, SessionOptimizerSettings, SqlCompileControl,
        SqlCompileIntent, SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    #[test]
    fn compiler_control_errors_keep_their_completion_category() {
        use novarocks_sql::compiler::SqlCompileError;
        assert!(matches!(
            compiler_error(SqlCompileError::Cancelled),
            FinalPlanCompletionError::Cancelled { .. }
        ));
        assert_eq!(
            compiler_error(SqlCompileError::DeadlineExceeded),
            FinalPlanCompletionError::DeadlineExceeded
        );
        assert_eq!(
            compiler_error(SqlCompileError::ResourceExhausted),
            FinalPlanCompletionError::ResourceExhausted
        );
    }

    #[test]
    fn actual_constant_limit_keeps_the_same_source_through_completion() {
        use arrow::array::{Array, Int64Array};
        use arrow::datatypes::{DataType, Field};
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection, domain};
        use novarocks_physical_plan::{ConstantError, ConstantPolicy, ConstantPool, ValueType};
        use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};
        let source = ConstantPool::try_new(
            Arc::new(Field::new("value", DataType::Int64, false)),
            ValueType::new(DataType::Int64, false),
            Int64Array::from(vec![1, 2]).to_data(),
            ConstantPolicy {
                max_rows: 1,
                max_array_nodes: 8,
                max_logical_elements: 8,
                max_retained_buffer_bytes: 4096,
                max_type_depth: 8,
                max_type_nodes: 8,
                max_dictionary_depth: 8,
                max_metadata_bytes: 32768,
                max_library_validation_work: 1 << 20,
                max_library_validation_bytes: 1 << 20,
            },
            novarocks_type_contract::CompilePhase::Validate,
            &SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        assert!(matches!(source, ConstantError::Limit(_)));
        let expected = source.failure_fact();
        assert_eq!(expected.class(), FailureClass::Resource);
        assert_eq!(expected.reason().domain(), domain::CONSTANT.get());
        assert_eq!(expected.reason().code(), 2);
        let compile = SqlCompileError::from(source);
        let SqlCompileError::CompilationSource(original) = &compile else {
            panic!("the constant author must keep its original source");
        };
        let original = original.clone();
        for completion in [
            compiler_error(compile.clone()),
            compiler_progress_error(SqlCompileProgressError::Compile(compile)),
        ] {
            assert!(std::error::Error::source(&completion).is_some());
            let FinalPlanCompletionError::Source { source } = completion else {
                panic!("completion must preserve the original compiler source");
            };
            assert_eq!(source, original);
            assert_eq!(source.failure_fact(), expected);
            assert!(matches!(
                source.downcast_ref::<ConstantError>(),
                Some(ConstantError::Limit(_))
            ));
        }
    }

    struct NoFactSource {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SqlCompletionFactSource for NoFactSource {
        type Access = ();

        async fn resolve(
            &self,
            _needs: &SqlNeedBatch,
            _taken: &ReadAccessSink<()>,
        ) -> Result<SqlFactBatch, String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err("VALUES completion must not request facts".to_string())
        }
    }

    fn scope() -> (novarocks_workload_control::RootWork, WorkScope) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        control.mark_ready().expect("workload control ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root");
        let scope = root.owner.scope();
        (root, scope)
    }

    fn request(
        sql: &str,
        control: SqlCompileControl,
        intent: SqlCompileIntent,
    ) -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([9; 16]).expect("plan version"),
            SqlStatementInput::sql(sql),
            intent,
            SqlSessionContext {
                sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            crate::completed_plan_fixture::test_constant_policy(),
            control,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }

    fn values_request() -> SqlFinalPlanCompileRequest {
        request(
            "SELECT 1",
            SqlCompileControl::unbounded(),
            SqlCompileIntent::Query,
        )
    }

    #[tokio::test]
    async fn values_completion_publishes_a_valid_candidate_without_fact_io() {
        let source = Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        });
        let driver = FinalPlanCompletionDriver::new(source.clone());
        let (_root, scope) = scope();

        let completed = driver
            .complete_for_test(values_request(), &scope)
            .await
            .expect("VALUES final plan completion");
        // A statement that scans no provider publishes an empty sidecar, not
        // an absent one.
        assert!(completed.access().is_empty());
        let candidate = completed.candidate();

        assert_eq!(
            candidate.plan().version(),
            PlanVersionId::try_new([9; 16]).unwrap()
        );
        assert_eq!(candidate.display_intent(), SqlDisplayIntent::Execute);
        assert!(candidate.display_annotations().is_empty());
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sql_candidate_clone_and_internal_publication_retain_the_original_source_owner() {
        let driver = FinalPlanCompletionDriver::new(Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        }));
        let (_root, scope) = scope();
        let completed = driver
            .complete_for_test(
                request(
                    "SELECT COUNT(*)",
                    SqlCompileControl::unbounded(),
                    SqlCompileIntent::Query,
                ),
                &scope,
            )
            .await
            .unwrap();
        let candidate = completed.candidate();
        let CompletedPlanSource::Sql(source) = &candidate.source else {
            panic!("SQL completion must retain its complete source owner");
        };
        assert!(source.plan().fragments().values().any(|fragment| {
            fragment.nodes().values().any(|node| {
                matches!(
                    node.kind,
                    novarocks_physical_plan::NodeKind::Aggregate { .. }
                )
            })
        }));
        let original_plan = Arc::clone(source.plan_arc());
        let weak_source = Arc::downgrade(source);
        let cloned = candidate.clone();
        let CompletedPlanSource::Sql(cloned_source) = &cloned.source else {
            panic!("candidate clone must keep the SQL source variant");
        };
        assert!(Arc::ptr_eq(source, cloned_source));
        let published = CompletedPhysicalPlanCandidate::for_sql_program(
            source.as_ref().clone(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert!(matches!(&published.source, CompletedPlanSource::Sql(_)));
        assert!(Arc::ptr_eq(published.plan(), &original_plan));
        let structural = CompletedPhysicalPlanCandidate::for_program(
            original_plan.as_ref().clone(),
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert!(matches!(
            &structural.source,
            CompletedPlanSource::Program(_)
        ));
        drop(completed);
        assert!(weak_source.upgrade().is_some());
        drop(cloned);
        assert!(weak_source.upgrade().is_none());
        assert!(Arc::ptr_eq(published.plan(), &original_plan));
    }

    #[tokio::test]
    async fn program_publication_rechecks_selected_sources_under_every_original_refusal() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Control {
            events: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Control {
            fn checkpoint(
                &self,
                phase: CompilePhase,
                units: u32,
            ) -> Result<(), CompileControlError> {
                let mut events = self.events.lock().unwrap();
                let at = events.len();
                events.push((phase, units));
                match self.refusal.clone() {
                    Some((stop, cause)) if stop == at => Err(cause),
                    Some((stop, _)) if at > stop => panic!("callback after originating refusal"),
                    _ => Ok(()),
                }
            }
        }
        let driver = FinalPlanCompletionDriver::new(Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        }));
        let (_root, scope) = scope();
        let completed = driver
            .complete_for_test(values_request(), &scope)
            .await
            .unwrap();
        let plan = completed.candidate().plan();
        assert!(!plan.constants().entries().is_empty());
        assert!(
            plan.fragments()
                .values()
                .flat_map(|fragment| fragment.expressions().iter())
                .any(|(_, expression)| matches!(
                    expression.kind,
                    novarocks_physical_plan::ExprKind::Constant(_)
                ))
        );
        let baseline = Control {
            events: Default::default(),
            refusal: None,
        };
        let published =
            CompletedPhysicalPlanCandidate::for_program(plan.as_ref().clone(), &baseline).unwrap();
        for (key, source) in plan.constants().entries() {
            assert_eq!(
                published.plan().constants().entries()[key].backing_identity(),
                source.backing_identity()
            );
        }
        let trace = baseline.events.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control {
                    events: Default::default(),
                    refusal: Some((at, cause.clone())),
                };
                let error =
                    CompletedPhysicalPlanCandidate::for_program(plan.as_ref().clone(), &control)
                        .unwrap_err();
                assert_eq!(
                    error,
                    compiler_error(novarocks_sql::compiler::SqlCompileError::from(
                        cause.clone()
                    ))
                );
                assert_eq!(*control.events.lock().unwrap(), trace[..=at]);
            }
        }
    }

    /// A completed plan renders its own EXPLAIN. Nothing is asked of a
    /// catalog, a provider or a backend to produce the text - the plan is
    /// already the answer.
    #[tokio::test]
    async fn a_completed_explain_renders_from_the_candidate_alone() {
        let source = Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        });
        let driver = FinalPlanCompletionDriver::new(source.clone());
        let (_root, scope) = scope();
        let completed = driver
            .complete_for_test(
                request(
                    "SELECT 1",
                    SqlCompileControl::unbounded(),
                    SqlCompileIntent::Explain {
                        level: ExplainLevel::Normal,
                        analyze: false,
                    },
                ),
                &scope,
            )
            .await
            .expect("VALUES explain final plan completion");

        let lines = completed
            .candidate()
            .render_explain_lines(
                ExplainRenderBudget::default(),
                &novarocks_sql::compiler::SqlCompileControl::unbounded(),
            )
            .expect("a completed plan renders its own explain");
        // EXPLAIN answers what the statement will do, so what it prints is
        // the operators, read from the plan alone.
        assert!(
            lines.iter().any(|line| line.contains("VALUES")),
            "{lines:?}"
        );
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn root_purpose_freeze_preserves_plan_version_display_and_access() {
        use novarocks_result_contract::FrozenRootOutput;
        let driver = FinalPlanCompletionDriver::new(Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        }));
        let (_root, scope) = scope();
        for client in [false, true] {
            let completed = driver
                .complete_for_test(
                    request(
                        "SELECT 1 AS display",
                        SqlCompileControl::unbounded(),
                        SqlCompileIntent::Explain {
                            level: ExplainLevel::Normal,
                            analyze: false,
                        },
                    ),
                    &scope,
                )
                .await
                .unwrap();
            let version = completed.candidate().plan().version();
            let intent = completed.candidate().display_intent();
            let annotations = completed.candidate().display_annotations().to_vec();
            let expected_lines = completed
                .candidate()
                .render_explain_lines(
                    ExplainRenderBudget::default(),
                    &SqlCompileControl::unbounded(),
                )
                .unwrap();
            let output = if client {
                FrozenRootOutput::ClientRows(
                    novarocks_sql::compiler::client_render_schema(
                        completed.candidate().plan(),
                        novarocks_sql::compiler::RenderFamilyRequest::MysqlText {
                            timezone_offset_seconds: 0,
                        },
                    )
                    .unwrap(),
                )
            } else {
                FrozenRootOutput::CountOnly
            };
            let completed = completed.freeze_root_output(output).unwrap();
            let candidate = completed.candidate();
            assert_eq!(candidate.plan().version(), version);
            assert_eq!(candidate.display_intent(), intent);
            assert_eq!(candidate.display_annotations(), annotations);
            assert_eq!(
                candidate
                    .render_explain_lines(
                        ExplainRenderBudget::default(),
                        &SqlCompileControl::unbounded(),
                    )
                    .unwrap(),
                expected_lines
            );
            assert!(completed.access().is_empty());
            assert!(novarocks_physical_plan::validate_plan(candidate.plan()).is_ok());
            let (_, access) = completed
                .freeze_root_output(FrozenRootOutput::CountOnly)
                .unwrap_err();
            assert!(access.is_empty());
        }
    }

    #[tokio::test]
    async fn root_purpose_freeze_refuses_published_alias_without_cloning_a_plan() {
        let driver = FinalPlanCompletionDriver::new(Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        }));
        let (_root, scope) = scope();
        let completed = driver
            .complete_for_test(values_request(), &scope)
            .await
            .unwrap();
        let alias = completed.candidate().clone();
        let (_, access) = completed
            .freeze_root_output(novarocks_result_contract::FrozenRootOutput::CountOnly)
            .unwrap_err();
        assert!(access.is_empty());
        let result = alias.plan().result_port().unwrap();
        assert!(matches!(
            alias.plan().fragments()[&result.fragment].sink(),
            novarocks_physical_plan::FragmentSink::Result
        ));
        assert_eq!(Arc::strong_count(alias.plan()), 1);
    }

    /// A statement compiled to be executed has no EXPLAIN text to give.
    #[tokio::test]
    async fn an_executable_plan_has_no_explain_text() {
        let driver = FinalPlanCompletionDriver::new(Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        }));
        let (_root, scope) = scope();
        let completed = driver
            .complete_for_test(values_request(), &scope)
            .await
            .expect("VALUES final plan completion");
        assert!(
            completed
                .candidate()
                .render_explain_lines(
                    ExplainRenderBudget::default(),
                    &novarocks_sql::compiler::SqlCompileControl::unbounded()
                )
                .is_err()
        );
    }

    #[tokio::test]
    async fn candidate_retains_completed_explain_semantics() {
        let source = Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        });
        let driver = FinalPlanCompletionDriver::new(source.clone());
        let (_root, scope) = scope();

        let candidate = driver
            .complete_for_test(
                request(
                    "SELECT 1",
                    SqlCompileControl::unbounded(),
                    SqlCompileIntent::Explain {
                        level: ExplainLevel::Verbose,
                        analyze: false,
                    },
                ),
                &scope,
            )
            .await
            .expect("VALUES explain final plan completion");
        let candidate = candidate.candidate();

        assert_eq!(
            candidate.display_intent(),
            SqlDisplayIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            }
        );
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
    }

    struct PendingFactSource;

    #[async_trait]
    impl SqlCompletionFactSource for PendingFactSource {
        type Access = ();

        async fn resolve(
            &self,
            _needs: &SqlNeedBatch,
            _taken: &ReadAccessSink<()>,
        ) -> Result<SqlFactBatch, String> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn deadline_interrupts_a_pending_fact_round() {
        let driver = FinalPlanCompletionDriver::new(Arc::new(PendingFactSource));
        let (_root, scope) = scope();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(10);

        let error = driver
            .complete_for_test(
                request(
                    "SELECT * FROM iceberg.db.orders",
                    SqlCompileControl::new(deadline.into(), Arc::new(NeverCancelled)),
                    SqlCompileIntent::Query,
                ),
                &scope,
            )
            .await
            .expect_err("deadline must interrupt a pending fact round");

        assert_eq!(error.error(), &FinalPlanCompletionError::DeadlineExceeded);
        // The round was interrupted before any read was frozen, so there is
        // nothing owed back.
        assert!(error.into_taken().is_empty());
    }

    struct NeverCancelled;

    impl novarocks_sql::compiler::SqlCancellationObservation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }
}
