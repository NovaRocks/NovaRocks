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
use novarocks_physical_plan::{PhysicalPlan, validate_plan};
use novarocks_sql::compiler::{
    ExplainRenderBudget, SqlCompileProgress, SqlCompiler, SqlDisplayAnnotation, SqlDisplayIntent,
    SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch, render_completed_plan,
    render_completed_plan_tree,
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
    plan: Arc<PhysicalPlan>,
    display_intent: SqlDisplayIntent,
    display_annotations: Arc<[SqlDisplayAnnotation]>,
}

impl CompletedPhysicalPlanCandidate {
    fn from_completed(
        completed: novarocks_sql::compiler::SqlCompletedPlan,
    ) -> Result<Self, FinalPlanCompletionError> {
        let (plan, display_intent, display_annotations) = completed.into_parts();
        Self::try_new(plan, display_intent, display_annotations)
    }

    /// Hold a plan an application built for work no statement described.
    ///
    /// Statistics collection and the other internal programs are not
    /// statements: nothing parsed them, so there is no text to explain and no
    /// display annotation to carry. They are still plans, and they are held to
    /// the same validation as a compiled one -- this constructor takes the
    /// plan and nothing else precisely so that it cannot be used to smuggle in
    /// a statement's display semantics.
    pub fn for_program(plan: PhysicalPlan) -> Result<Self, FinalPlanCompletionError> {
        Self::try_new(plan, SqlDisplayIntent::Execute, Box::default())
    }

    fn try_new(
        plan: PhysicalPlan,
        display_intent: SqlDisplayIntent,
        display_annotations: Box<[SqlDisplayAnnotation]>,
    ) -> Result<Self, FinalPlanCompletionError> {
        validate_plan(&plan).map_err(|error| FinalPlanCompletionError::InvalidPlan {
            message: Arc::from(error.to_string()),
        })?;
        Ok(Self {
            plan: Arc::new(plan),
            display_intent,
            display_annotations: display_annotations.into(),
        })
    }

    pub const fn plan(&self) -> &Arc<PhysicalPlan> {
        &self.plan
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
            render_completed_plan(&self.plan, &self.display_annotations, level, None, budget)
        } else {
            render_completed_plan_tree(&self.plan, level)
        };
        lines.map_err(|error| FinalPlanCompletionError::Compiler {
            message: Arc::from(error.to_string()),
        })
    }
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
    ) -> Result<CompletedPlanWithAccess<A>, FinalPlanCompletionFailure<A>> {
        let taken = ReadAccessSink::new();
        let candidate = match self.complete_plan(request, scope, &taken).await {
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

    async fn complete_plan(
        &self,
        request: SqlFinalPlanCompileRequest,
        scope: &WorkScope,
        taken: &ReadAccessSink<A>,
    ) -> Result<CompletedPhysicalPlanCandidate, FinalPlanCompletionError> {
        let control = request.control().clone();
        scope.check().map_err(governance_error)?;
        let cancellation = scope.cancellation().map_err(governance_error)?;
        let mut progress =
            SqlCompiler::start(request.try_into_completion().map_err(compiler_error)?)
                .map_err(compiler_progress_error)?;

        loop {
            scope.check().map_err(governance_error)?;
            progress = match progress {
                SqlCompileProgress::Complete(completed) => {
                    return CompletedPhysicalPlanCandidate::from_completed(completed);
                }
                SqlCompileProgress::Incomplete(compilation) => {
                    let facts = self
                        .resolve(
                            &cancellation,
                            control.deadline(),
                            compilation.needs(),
                            taken,
                        )
                        .await?;
                    SqlCompiler::finish(compilation, facts, &control)
                        .map_err(compiler_progress_error)?
                }
            };
        }
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
    Compiler {
        message: Arc<str>,
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
            Self::Analyze { error } => error.fmt(formatter),
        }
    }
}

impl std::error::Error for FinalPlanCompletionError {}

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
        novarocks_sql::compiler::SqlCompileError::Analyze(error) => {
            FinalPlanCompletionError::Analyze { error }
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
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
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
            .complete(values_request(), &scope)
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
            .complete(
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
            .render_explain_lines(ExplainRenderBudget::default())
            .expect("a completed plan renders its own explain");
        // EXPLAIN answers what the statement will do, so what it prints is
        // the operators, read from the plan alone.
        assert!(
            lines.iter().any(|line| line.contains("VALUES")),
            "{lines:?}"
        );
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
    }

    /// A statement compiled to be executed has no EXPLAIN text to give.
    #[tokio::test]
    async fn an_executable_plan_has_no_explain_text() {
        let driver = FinalPlanCompletionDriver::new(Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        }));
        let (_root, scope) = scope();
        let completed = driver
            .complete(values_request(), &scope)
            .await
            .expect("VALUES final plan completion");
        assert!(
            completed
                .candidate()
                .render_explain_lines(ExplainRenderBudget::default())
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
            .complete(
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
            .complete(
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
