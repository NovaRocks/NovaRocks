//! Actual SQL completion and actual public DML lowering publication probes.
//! Test receipt storage is explicit; this is not FE host admission or MEM proof.
use arrow::datatypes::DataType;
use novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog;
use novarocks_physical_plan::{NodeKind, PipelineDopDomain, PlanVersionId, ScanReadBudget};
use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    ConnectorWriteFieldToken,
};
use novarocks_sql::binding::SqlTableBindingAllocator;
use novarocks_sql::compiler::{
    DEFAULT_COMPLETION_LIMITS, RootDistributionRequirement, SessionOptimizerSettings,
    SqlAnalyzeRequest, SqlAuthoredPhysicalPlan, SqlCancellationObservation, SqlCompileControl,
    SqlCompileError, SqlCompileIntent, SqlCompileProgress, SqlCompileProgressError, SqlCompiler,
    SqlFinalPlanCompileRequest, SqlFoldDependencyInput, SqlFoldDependencyObserver,
    SqlFoldEvaluationOutcome, SqlOptimizeRequest, SqlPlannerTableSnapshot, SqlPlanningEnvironment,
    SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
};
use novarocks_sql::planning::catalog::PlannerMemoryCatalog;
use novarocks_sql::planning::dml::{
    ConnectorWriteInputBinding, DmlFinalPlanContext, DmlFinalWritePlanContext,
    DmlFinalizedProviderReadSet, DmlFinalizedWriteTarget, DmlFinalizedWriteTargetSet,
    DmlStatisticsSnapshot, DmlWritePlanInput, DmlWriteSinkMode, DmlWriteTarget,
    DmlWriteTargetField, compile_final_connector_write_plan,
};
use novarocks_type_contract::{CompileControlError, DecimalOverflowPolicy, PureCompileControl};
use novarocks_types::schema::ColumnDef;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Probe {
    refuse: Option<CompileControlError>,
    calls: AtomicUsize,
    sources: Mutex<Vec<SqlAuthoredPhysicalPlan>>,
}
impl Probe {
    fn new(refuse: Option<CompileControlError>) -> Arc<Self> {
        Arc::new(Self {
            refuse,
            calls: AtomicUsize::new(0),
            sources: Mutex::new(Vec::new()),
        })
    }
}
impl SqlFoldDependencyObserver for Probe {
    fn before_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        _: SqlFoldEvaluationOutcome<'_>,
    ) {
    }
    fn observe_published_source_observed(
        &self,
        source: &SqlAuthoredPhysicalPlan,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(cause) = self.refuse.clone() {
            return Err(cause);
        }
        let mut sources = self.sources.lock().unwrap();
        sources
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        sources.push(source.clone()); // Same admitted original Arcs, never a second plan.
        Ok(())
    }
}
struct DefaultObserver;
impl SqlFoldDependencyObserver for DefaultObserver {
    fn before_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        _: &dyn PureCompileControl,
    ) -> Result<(), CompileControlError> {
        Ok(())
    }
    fn after_fold_dependency_observed(
        &self,
        _: SqlFoldDependencyInput<'_>,
        _: SqlFoldEvaluationOutcome<'_>,
    ) {
    }
}
struct CountCancellation(AtomicUsize);
impl SqlCancellationObservation for CountCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.fetch_add(1, Ordering::Relaxed);
        false
    }
}
fn settings() -> SessionOptimizerSettings {
    SessionOptimizerSettings {
        enable_materialized_view_rewrite: Some(false),
        ..SessionOptimizerSettings::default()
    }
}
fn session() -> SqlSessionContext {
    SqlSessionContext {
        sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        current_catalog: None,
        current_database: "fixture".into(),
        optimizer_settings: settings(),
    }
}
fn control(probe: &Arc<Probe>) -> SqlCompileControl {
    SqlCompileControl::unbounded().with_fold_dependency_observer(probe.clone())
}
fn query(
    sql: &str,
    control: SqlCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, SqlCompileProgressError> {
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([97; 16]).unwrap(),
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(sql)),
        SqlCompileIntent::Query,
        session(),
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        noop_constant_evaluator(),
        super::test_support::constant_policy(),
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    match SqlCompiler::start(request.try_into_completion()?, &control)? {
        SqlCompileProgress::Complete(completed) => Ok(completed.into_plan()),
        SqlCompileProgress::Incomplete(_) => {
            panic!("table-free source must require no provider facts")
        }
    }
}
// The same public SQL/DML producer used by the permanent Writer source fixture.
// Only explicit context/observer arguments differ; no replacement physical graph.
fn dml(sql: &str, control: SqlCompileControl) -> Result<SqlAuthoredPhysicalPlan, SqlCompileError> {
    let functions = build_builtin_engine_function_catalog().unwrap();
    let catalog = PlannerMemoryCatalog::default();
    let catalog = SqlPlannerTableSnapshot::new(&catalog);
    let settings = settings();
    let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::exec::expr::test_support::parse_query(sql)),
        SqlCompileIntent::IcebergWrite {
            root_distribution: RootDistributionRequirement::Any,
        },
        session(),
        SqlPlanningEnvironment::Distributed,
        &catalog,
        &functions,
        noop_constant_evaluator(),
        None,
        super::test_support::constant_policy(),
        control.clone(),
    ))?
    .into_pending()?;
    let column = ColumnDef {
        name: "order_id".into(),
        data_type: DataType::Int64,
        nullable: false,
        write_default: None,
        logical_type: None,
    };
    let mut allocator = SqlTableBindingAllocator::new_unique().unwrap();
    let sink = DmlWritePlanInput::try_new(
        DmlWriteSinkMode::Data,
        DmlWriteTarget {
            binding: allocator.allocate().unwrap(),
            catalog: "iceberg".into(),
            namespace: "fixture".into(),
            table: "target".into(),
            fields: vec![DmlWriteTargetField {
                token: ConnectorWriteFieldToken::from_bytes([1; 32]),
                column: column.clone(),
                is_hidden: false,
            }],
        },
        vec![column],
        ConnectorWriteInputBinding::RootOutputByOrdinal,
    )
    .unwrap();
    let ordinal = WriteTargetOrdinal::try_new(0).unwrap();
    let handle = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").unwrap(),
                CatalogVersion::from_bytes([9; 32]),
            ),
            ConnectorCodecCategory::WriteHandle,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7].into(),
    );
    let statistics = DmlStatisticsSnapshot::empty();
    compile_final_connector_write_plan(
        SqlOptimizeRequest::new(analyzed, &statistics, control),
        sink,
        ordinal,
        &[],
        &settings,
        DmlFinalWritePlanContext::new(
            DmlFinalPlanContext::new(
                PlanVersionId::try_new([98; 16]).unwrap(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                DmlFinalizedProviderReadSet::empty(),
            ),
            DmlFinalizedWriteTargetSet::try_new([DmlFinalizedWriteTarget { ordinal, handle }])
                .unwrap(),
        ),
        DecimalOverflowPolicy::OutputNull,
    )
}
fn assert_same_source(actual: &SqlAuthoredPhysicalPlan, probe: &Probe) {
    assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
    let sources = probe.sources.lock().unwrap();
    assert_eq!(sources.len(), 1);
    let loaned = &sources[0];
    assert!(std::ptr::eq(actual.plan(), loaned.plan()));
    let actual_port = actual.original_public_result_declaration();
    let loaned_port = loaned.original_public_result_declaration();
    assert_eq!(
        actual_port.as_ref().map(|p| p.fields()),
        loaned_port.as_ref().map(|p| p.fields())
    );
    assert_eq!(format!("{actual:?}"), format!("{loaned:?}")); // Includes original source journal.
}
fn assert_compile_cause(error: &SqlCompileError, cause: CompileControlError) {
    assert!(
        matches!(
            (error, cause),
            (SqlCompileError::Cancelled, CompileControlError::Cancelled)
                | (
                    SqlCompileError::DeadlineExceeded,
                    CompileControlError::DeadlineExceeded
                )
                | (
                    SqlCompileError::ResourceExhausted,
                    CompileControlError::ResourceExhausted
                )
        ),
        "{error:?}"
    );
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
#[test]
fn sql_published_source_query_loans_same_complete_owner_for_compiled() {
    {
        let probe = Probe::new(None);
        let source = query(
            "SELECT upper('publication') AS original_name",
            control(&probe),
        )
        .unwrap();
        assert_same_source(&source, &probe);
        let declaration = source.original_public_result_declaration().unwrap();
        assert_eq!(
            declaration.fields()[0].name.as_ref(),
            "upper('publication')"
        );
        assert_eq!(
            declaration.fields()[0].alias.as_deref(),
            Some("original_name")
        );
        assert!(
            source
                .plan()
                .fragments()
                .values()
                .any(|f| f.expressions().iter().any(|(_, e)| matches!(
                    e.kind,
                    novarocks_physical_plan::ExprKind::FunctionCall { .. }
                )))
        );
    }
}
#[test]
fn sql_published_source_actual_dml_loans_writer_and_finish_same_owner() {
    {
        let probe = Probe::new(None);
        let source = dml("SELECT CAST(7 AS BIGINT) AS order_id", control(&probe)).unwrap();
        assert_same_source(&source, &probe);
        assert!(source.plan().fragments().values().any(|f| {
            f.nodes()
                .values()
                .any(|n| matches!(n.kind, NodeKind::TableWriter { .. }))
        }));
        assert!(source.plan().fragments().values().any(|f| {
            f.nodes()
                .values()
                .any(|n| matches!(n.kind, NodeKind::TableFinish(_)))
        }));
    }
}
#[test]
fn sql_published_source_query_preserves_three_typed_control_causes() {
    for cause in causes() {
        let probe = Probe::new(Some(cause.clone()));
        let error = query("SELECT 7 AS original_name", control(&probe)).unwrap_err();
        let SqlCompileProgressError::Compile(error) = error else {
            panic!("unexpected source error: {error:?}");
        };
        assert_compile_cause(&error, cause);
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
        assert!(probe.sources.lock().unwrap().is_empty());
    }
}
#[test]
fn sql_published_source_actual_dml_preserves_three_typed_control_causes() {
    for cause in causes() {
        let probe = Probe::new(Some(cause.clone()));
        let error = dml("SELECT CAST(7 AS BIGINT) AS order_id", control(&probe)).unwrap_err();
        assert_compile_cause(&error, cause);
        assert_eq!(probe.calls.load(Ordering::Relaxed), 1);
        assert!(probe.sources.lock().unwrap().is_empty());
    }
}
#[test]
fn sql_published_source_default_none_adds_no_compile_checkpoint() {
    {
        let original = Arc::new(CountCancellation(AtomicUsize::new(0)));
        let observed = Arc::new(CountCancellation(AtomicUsize::new(0)));
        let first = query(
            "SELECT 7 AS original_name",
            SqlCompileControl::new(None, original.clone()),
        )
        .unwrap();
        let second = query(
            "SELECT 7 AS original_name",
            SqlCompileControl::new(None, observed.clone())
                .with_fold_dependency_observer(Arc::new(DefaultObserver)),
        )
        .unwrap();
        assert_eq!(
            original.0.load(Ordering::Relaxed),
            observed.0.load(Ordering::Relaxed)
        );
        assert_eq!(
            first.original_public_result_declaration().unwrap().fields(),
            second
                .original_public_result_declaration()
                .unwrap()
                .fields()
        );
    }
}
#[test]
fn sql_published_source_original_query_failure_precedes_refusing_observer() {
    let original = query(
        "SELECT missing_publication_column",
        SqlCompileControl::unbounded(),
    )
    .unwrap_err();
    for cause in causes() {
        let probe = Probe::new(Some(cause.clone()));
        let actual = query("SELECT missing_publication_column", control(&probe)).unwrap_err();
        assert_eq!(actual.to_string(), original.to_string());
        assert_eq!(probe.calls.load(Ordering::Relaxed), 0);
    }
}
#[test]
fn sql_published_source_original_dml_data_error_precedes_refusing_observer() {
    let original = dml(
        "SELECT 7 AS order_id, 8 AS unexpected_column",
        SqlCompileControl::unbounded(),
    )
    .unwrap_err();
    for cause in causes() {
        let probe = Probe::new(Some(cause.clone()));
        let actual = dml(
            "SELECT 7 AS order_id, 8 AS unexpected_column",
            control(&probe),
        )
        .unwrap_err();
        assert_eq!(actual.to_string(), original.to_string());
        assert_eq!(probe.calls.load(Ordering::Relaxed), 0);
    }
}
#[test]
fn sql_published_source_query_owned_observer_drops_without_plan_cycle() {
    for is_dml in [false, true] {
        let probe = Probe::new(None);
        let weak = Arc::downgrade(&probe);
        let source = if is_dml {
            dml("SELECT CAST(7 AS BIGINT) AS order_id", control(&probe)).unwrap()
        } else {
            query("SELECT 7 AS original_name", control(&probe)).unwrap()
        };
        assert_same_source(&source, &probe);
        drop(probe);
        assert!(weak.upgrade().is_none());
        assert!(!source.plan().fragments().is_empty());
    }
}
