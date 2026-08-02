use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use novarocks::mv::application::{
    CreatedMvTarget, MvApplicationErrorKind, MvApplicationService, MvApplicationStatement,
    MvCreateRefreshPolicy, MvCreateStatement, MvEngine, MvEngineError, MvEngineErrorKind,
    MvRequestContext, MvStatementResult, PrepareMvCreateRequest, PreparedMvCreate,
    PreparedMvDefinition,
};
use novarocks::mv::dependency::model::MvDependencyObjectRef;
use novarocks::mv::persistence::definition::{
    CreateMvDefinitionRequest, StoredMvDefinition, StoredMvRefreshPolicy,
    UpdateMvRefreshMetadataRequest,
};
use novarocks::mv::persistence::dependency::{CreateMvDependencyRequest, StoredMvDependency};
use novarocks::mv::persistence::partition::{
    RecordFailedMvPartitionStatesRequest, ReplaceMvPartitionStatesRequest, StoredMvPartitionState,
    UpdateMvPartitionContractRequest,
};
use novarocks::mv::persistence::refresh::{
    BeginIcebergMvRefreshRequest, MvRefreshFinalizeRequest, RecordPublishCommitRequest,
    RecordStagingCommitRequest, RefreshExternalOutcome, StoredMvRefresh,
    UpdateStarRocksMvRefreshSummaryRequest,
};
use novarocks::mv::repository::{
    CreateMvRepositoryRequest, CreateMvRepositoryWithIdRequest,
    FinalizeMvRefreshWithPartitionsRequest, InitialMvRefreshConfiguration, MvRepository,
    MvRepositoryAvailability, MvRepositoryError, MvRepositoryErrorKind, MvTarget,
    RebuildMvRepositoryRequest, RecordExternalCommitAndFinalizeRequest, UnavailableMvRepository,
};
use novarocks_frontend::mv::FrontendMvService;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailurePoint {
    Prepare,
    CreateTarget,
    Inspect,
    Repository(MvRepositoryErrorKind),
    Sync,
    Register,
    Cleanup,
}

struct FakeEngine {
    failures: Vec<FailurePoint>,
    calls: Mutex<Vec<&'static str>>,
    create_operation_ids: Mutex<Vec<Uuid>>,
}

impl FakeEngine {
    fn new(failures: Vec<FailurePoint>) -> Self {
        Self {
            failures,
            calls: Mutex::new(Vec::new()),
            create_operation_ids: Mutex::new(Vec::new()),
        }
    }

    fn call(&self, call: &'static str) -> Result<(), MvEngineError> {
        self.calls.lock().expect("calls").push(call);
        let failed = self.failures.iter().any(|failure| {
            matches!(
                (failure, call),
                (FailurePoint::Prepare, "prepare")
                    | (FailurePoint::CreateTarget, "create")
                    | (FailurePoint::Inspect, "inspect")
                    | (FailurePoint::Sync, "sync")
                    | (FailurePoint::Register, "register")
                    | (FailurePoint::Cleanup, "cleanup")
            )
        });
        if failed {
            return Err(MvEngineError::new(
                MvEngineErrorKind::TargetOperation,
                format!("{call} failed"),
            ));
        }
        Ok(())
    }

    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().expect("calls").clone()
    }

    fn create_operation_ids(&self) -> Vec<Uuid> {
        self.create_operation_ids
            .lock()
            .expect("create operation ids")
            .clone()
    }
}

impl MvEngine for FakeEngine {
    fn prepare_create(
        &self,
        _request: PrepareMvCreateRequest<'_>,
        _repository: &dyn MvRepository,
    ) -> Result<PreparedMvCreate, MvEngineError> {
        self.call("prepare")?;
        Ok(PreparedMvCreate::new(target(), create_request()))
    }

    fn create_target(
        &self,
        _plan: &PreparedMvCreate,
        operation_id: Uuid,
    ) -> Result<CreatedMvTarget, MvEngineError> {
        self.create_operation_ids
            .lock()
            .expect("create operation ids")
            .push(operation_id);
        self.call("create")?;
        Ok(CreatedMvTarget {
            target: target(),
            table_uuid: "target-uuid".to_string(),
        })
    }

    fn inspect_created_target(
        &self,
        _plan: &PreparedMvCreate,
        _target: &CreatedMvTarget,
    ) -> Result<PreparedMvDefinition, MvEngineError> {
        self.call("inspect")?;
        Ok(PreparedMvDefinition {
            repository_request: create_request(),
        })
    }

    fn sync_target_descriptor(
        &self,
        _target: &CreatedMvTarget,
        _definition: &StoredMvDefinition,
    ) -> Result<(), MvEngineError> {
        self.call("sync")
    }

    fn register_target(&self, _target: &CreatedMvTarget) -> Result<(), MvEngineError> {
        self.call("register")
    }

    fn drop_created_target(&self, _target: &CreatedMvTarget) -> Result<(), MvEngineError> {
        self.call("cleanup")
    }
}

struct FakeRepository {
    create_error: Option<MvRepositoryErrorKind>,
    calls: Mutex<Vec<Uuid>>,
}

impl FakeRepository {
    fn new(create_error: Option<MvRepositoryErrorKind>) -> Self {
        Self {
            create_error,
            calls: Mutex::new(Vec::new()),
        }
    }
}

macro_rules! unused_repository_method {
    () => {
        panic!("unexpected MV repository method")
    };
}

impl MvRepository for FakeRepository {
    fn availability(&self) -> MvRepositoryAvailability {
        MvRepositoryAvailability::Available
    }

    fn create(
        &self,
        operation_id: Uuid,
        _request: CreateMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        self.calls.lock().expect("calls").push(operation_id);
        if let Some(kind) = self.create_error {
            return Err(MvRepositoryError::new(kind, "repository create failed"));
        }
        Ok(stored_definition())
    }

    fn create_with_id(
        &self,
        _: Uuid,
        _: CreateMvRepositoryWithIdRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unused_repository_method!()
    }
    fn rebuild(
        &self,
        _: Uuid,
        _: RebuildMvRepositoryRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unused_repository_method!()
    }
    fn reserve_definition_id(&self, _: i64) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn load_by_id(&self, _: i64) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn find_by_target(
        &self,
        _: &MvTarget,
    ) -> Result<Option<StoredMvDefinition>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_definitions(&self) -> Result<Vec<StoredMvDefinition>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn drop_by_id(&self, _: i64) -> Result<bool, MvRepositoryError> {
        unused_repository_method!()
    }
    fn drop_by_target(&self, _: &MvTarget) -> Result<bool, MvRepositoryError> {
        unused_repository_method!()
    }
    fn set_rebuilt_refresh_watermark(
        &self,
        _: i64,
        _: BTreeMap<String, i64>,
        _: BTreeMap<String, String>,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unused_repository_method!()
    }
    fn update_refresh_metadata(
        &self,
        _: UpdateMvRefreshMetadataRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unused_repository_method!()
    }
    fn update_partition_contract(
        &self,
        _: UpdateMvPartitionContractRequest,
    ) -> Result<StoredMvDefinition, MvRepositoryError> {
        unused_repository_method!()
    }
    fn begin_refresh_intent(
        &self,
        _: i64,
        _: BTreeMap<String, i64>,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unused_repository_method!()
    }
    fn begin_iceberg_refresh_intent(
        &self,
        _: BeginIcebergMvRefreshRequest,
    ) -> Result<StoredMvRefresh, MvRepositoryError> {
        unused_repository_method!()
    }
    fn record_staging_commit(
        &self,
        _: RecordStagingCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn record_publish_commit(
        &self,
        _: RecordPublishCommitRequest,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn mark_refresh_commit_unknown(&self, _: i64) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn record_external_commit_outcome(
        &self,
        _: i64,
        _: RefreshExternalOutcome,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn finalize_refresh(&self, _: MvRefreshFinalizeRequest) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn finalize_refresh_with_partitions(
        &self,
        _: FinalizeMvRefreshWithPartitionsRequest,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn record_external_commit_and_finalize(
        &self,
        _: RecordExternalCommitAndFinalizeRequest,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn clear_refresh_progress(&self, _: i64) -> Result<bool, MvRepositoryError> {
        unused_repository_method!()
    }
    fn load_refresh(&self, _: i64) -> Result<Option<StoredMvRefresh>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_unfinished_refreshes(&self) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_unfinished_branch_staged_iceberg_refreshes(
        &self,
    ) -> Result<Vec<StoredMvRefresh>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn update_starrocks_refresh_summary_if_present(
        &self,
        _: UpdateStarRocksMvRefreshSummaryRequest,
    ) -> Result<bool, MvRepositoryError> {
        unused_repository_method!()
    }
    fn replace_partition_states(
        &self,
        _: ReplaceMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn record_failed_partition_states(
        &self,
        _: RecordFailedMvPartitionStatesRequest,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn clear_partition_states(&self, _: i64) -> Result<bool, MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_partition_states(
        &self,
        _: i64,
    ) -> Result<Vec<StoredMvPartitionState>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn adopt_target_compaction_snapshot(
        &self,
        _: &MvTarget,
        _: i64,
        _: i64,
    ) -> Result<bool, MvRepositoryError> {
        unused_repository_method!()
    }
    fn replace_dependencies_for_mv(
        &self,
        _: i64,
        _: Vec<CreateMvDependencyRequest>,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn delete_dependencies_for_mv(&self, _: i64) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn ensure_no_downstream_dependencies(
        &self,
        _: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_dependencies_by_downstream(
        &self,
        _: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        unused_repository_method!()
    }
    fn list_downstream_dependencies(
        &self,
        _: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        unused_repository_method!()
    }
}

fn target() -> MvTarget {
    MvTarget {
        catalog: Some("ice".to_string()),
        database: "db".to_string(),
        name: "mv".to_string(),
    }
}

fn create_request() -> CreateMvRepositoryRequest {
    CreateMvRepositoryRequest {
        definition: CreateMvDefinitionRequest {
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec![],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
            schema_contract: None,
            partition_spec: None,
            created_at_ms: 1,
        },
        refresh: InitialMvRefreshConfiguration::default(),
        dependencies: vec![],
    }
}

fn stored_definition() -> StoredMvDefinition {
    StoredMvDefinition {
        mv_id: 1,
        select_sql: "SELECT 1".to_string(),
        base_table_refs: vec![],
        primary_key_columns: vec![],
        storage_engine: "iceberg".to_string(),
        target_catalog: Some("ice".to_string()),
        target_namespace: Some("db".to_string()),
        target_table: Some("mv".to_string()),
        schema_contract: None,
        partition_spec: None,
        partition_state_complete: false,
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: BTreeMap::new(),
        last_refresh_table_uuids: BTreeMap::new(),
        last_refreshed_iceberg_snapshot_id: None,
        refresh_in_progress: false,
        active_refresh_id: None,
        refresh_target_snapshots: BTreeMap::new(),
        refresh_policy: StoredMvRefreshPolicy::Manual,
        refresh_paused: false,
        refresh_interval_ms: None,
        max_staleness_ms: None,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
        created_at_ms: 1,
    }
}

fn statement() -> MvApplicationStatement {
    let mut statements = Parser::parse_sql(&GenericDialect {}, "SELECT 1").expect("parse query");
    let sqlparser::ast::Statement::Query(select_query) = statements.remove(0) else {
        panic!("query")
    };
    MvApplicationStatement::Create(MvCreateStatement {
        name_parts: vec!["mv".to_string()],
        if_not_exists: false,
        partition_by: None,
        distribution: None,
        refresh_policy: MvCreateRefreshPolicy::Manual,
        select_sql: "SELECT 1".to_string(),
        select_query: *select_query,
        properties: vec![("storage_engine".to_string(), "iceberg".to_string())],
        primary_key: None,
    })
}

fn execute(
    failures: &[FailurePoint],
) -> (
    Result<Option<MvStatementResult>, novarocks::mv::application::MvApplicationError>,
    Arc<FakeEngine>,
    Arc<FakeRepository>,
) {
    let engine = Arc::new(FakeEngine::new(failures.to_vec()));
    let repository = Arc::new(FakeRepository::new(failures.iter().find_map(
        |failure| match failure {
            FailurePoint::Repository(kind) => Some(*kind),
            _ => None,
        },
    )));
    let service = FrontendMvService::new(repository.clone());
    let result = service.try_handle_statement(
        engine.as_ref(),
        &statement(),
        MvRequestContext {
            current_catalog: Some("ice"),
            current_database: "db",
        },
    );
    (result, engine, repository)
}

#[test]
fn create_success_sequences_one_repository_command_before_sync_and_register() {
    let (result, engine, repository) = execute(&[]);
    assert!(matches!(result, Ok(Some(MvStatementResult::Ok))));
    assert_eq!(
        engine.calls(),
        ["prepare", "create", "inspect", "sync", "register"]
    );
    let operation_ids = repository.calls.lock().expect("calls").clone();
    assert_eq!(operation_ids.len(), 1);
    let target_operation_ids = engine.create_operation_ids();
    assert_eq!(target_operation_ids.len(), 1);
    assert_eq!(target_operation_ids, operation_ids);
    assert_eq!(
        operation_ids[0].get_version(),
        Some(uuid::Version::SortRand)
    );
}

#[test]
fn create_known_uncommitted_failures_clean_up_target_and_preserve_primary_error() {
    for failure in [
        FailurePoint::Inspect,
        FailurePoint::Repository(MvRepositoryErrorKind::Conflict),
    ] {
        let (result, engine, _) = execute(&[failure]);
        assert!(result.is_err());
        assert_eq!(engine.calls().last(), Some(&"cleanup"));
    }
}

#[test]
fn create_commit_unknown_retains_target_for_recovery() {
    let (result, engine, _) = execute(&[FailurePoint::Repository(
        MvRepositoryErrorKind::CommitUnknown,
    )]);
    assert_eq!(
        result.expect_err("commit unknown").kind(),
        MvApplicationErrorKind::CommitUnknown
    );
    assert_eq!(engine.calls(), ["prepare", "create", "inspect"]);
}

#[test]
fn create_after_commit_failures_retain_target_and_are_typed_finalize_failures() {
    for failure in [FailurePoint::Sync, FailurePoint::Register] {
        let (result, engine, _) = execute(&[failure]);
        assert_eq!(
            result.expect_err("finalize failure").kind(),
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        );
        assert!(!engine.calls().contains(&"cleanup"));
    }
}

#[test]
fn prepare_and_target_create_fail_before_repository_or_cleanup() {
    for failure in [FailurePoint::Prepare, FailurePoint::CreateTarget] {
        let (result, engine, repository) = execute(&[failure]);
        assert!(result.is_err());
        assert!(!engine.calls().contains(&"cleanup"));
        assert!(repository.calls.lock().expect("calls").is_empty());
    }
}

#[test]
fn cleanup_failure_keeps_primary_error_and_adds_context() {
    let (result, engine, _) = execute(&[FailurePoint::Inspect, FailurePoint::Cleanup]);
    let error = result.expect_err("inspect failure");
    assert_eq!(error.kind(), MvApplicationErrorKind::Engine);
    assert!(
        error
            .message()
            .contains("inspect failed; target cleanup failed: cleanup failed")
    );
    assert_eq!(engine.calls(), ["prepare", "create", "inspect", "cleanup"]);
}

#[test]
fn unhandled_statement_is_left_for_the_core_route() {
    let engine = FakeEngine::new(vec![]);
    let service = FrontendMvService::new(Arc::new(FakeRepository::new(None)));
    assert!(matches!(
        service.try_handle_statement(
            &engine,
            &MvApplicationStatement::Unhandled,
            MvRequestContext {
                current_catalog: Some("ice"),
                current_database: "db"
            }
        ),
        Ok(None)
    ));
}

#[test]
fn refresh_statement_cannot_fall_through_the_generic_statement_handler() {
    let engine = FakeEngine::new(vec![]);
    let service = FrontendMvService::new(Arc::new(FakeRepository::new(None)));
    let error = service
        .try_handle_statement(
            &engine,
            &MvApplicationStatement::Refresh(novarocks::sql::mv_refresh::MvRefreshStatement {
                name_parts: vec!["orders_mv".to_string()],
                full: false,
            }),
            MvRequestContext {
                current_catalog: Some("ice"),
                current_database: "db",
            },
        )
        .expect_err("refresh must use the typed frontend refresh entrypoint");

    assert_eq!(error.kind(), MvApplicationErrorKind::InvalidRequest);
    assert!(error.message().contains("frontend refresh entrypoint"));
    assert!(engine.calls().is_empty());
}

#[test]
fn create_without_state_store_fails_before_engine_work() {
    let engine = FakeEngine::new(vec![]);
    let service = FrontendMvService::new(Arc::new(UnavailableMvRepository));
    let error = service
        .try_handle_statement(
            &engine,
            &statement(),
            MvRequestContext {
                current_catalog: Some("ice"),
                current_database: "db",
            },
        )
        .expect_err("state store is required");
    assert_eq!(error.kind(), MvApplicationErrorKind::Unavailable);
    assert!(engine.calls().is_empty());
}
