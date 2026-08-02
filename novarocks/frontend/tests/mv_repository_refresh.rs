use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::dependency::CreateMvDependencyRequest;
use novarocks::mv::persistence::partition::ReplaceMvPartitionStatesRequest;
use novarocks::mv::persistence::refresh::{
    FrontendMvRefreshAction, FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
    FrontendMvRefreshCommittedVersion, FrontendMvRefreshLedger, MvRefreshFinalizeRequest,
    MvRefreshLifecycleOwner, MvRefreshState, RecordPublishCommitRequest,
    RecordStagingCommitRequest,
};
use novarocks::mv::repository::{
    FinalizeMvRefreshWithPartitionsRequest, MvRepository, MvRepositoryErrorKind,
};
use novarocks_frontend::mv::repository::{
    BeginFrontendMvRefreshIntentRequest, StateStoreMvRepository,
};
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

#[path = "mv_repository_definition.rs"]
mod definition_support;

fn repository() -> (
    tempfile::TempDir,
    tokio::runtime::Runtime,
    StateStoreHost,
    Arc<StateStoreMvRepository>,
) {
    let temp = tempfile::tempdir().expect("temporary StateStore directory");
    let runtime = tokio::runtime::Runtime::new().expect("repository runtime");
    let registry = builtin_state_store_provider_registry().expect("built-in StateStore providers");
    let host = runtime
        .block_on(StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "mv-refresh-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-refresh-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-refresh-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        ))
        .expect("open SQLite StateStore host");
    let repository = runtime
        .block_on(StateStoreMvRepository::open(
            host.state_store().expect("host exposes StateStore"),
            runtime.handle().clone(),
        ))
        .expect("open MV repository");
    (temp, runtime, host, repository)
}

fn limited_repository() -> (
    tempfile::TempDir,
    tokio::runtime::Runtime,
    StateStoreHost,
    Arc<StateStoreMvRepository>,
) {
    let temp = tempfile::tempdir().expect("temporary StateStore directory");
    let runtime = tokio::runtime::Runtime::new().expect("repository runtime");
    let registry = builtin_state_store_provider_registry().expect("built-in StateStore providers");
    let host = runtime
        .block_on(StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "mv-refresh-limit-test".to_string(),
                        limits: StateStoreLimitOverrides {
                            max_page_size: Some(2),
                            ..StateStoreLimitOverrides::default()
                        },
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-refresh-limit-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-refresh-limit-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        ))
        .expect("open SQLite StateStore host");
    let repository = runtime
        .block_on(StateStoreMvRepository::open(
            host.state_store().expect("host exposes StateStore"),
            runtime.handle().clone(),
        ))
        .expect("open MV repository");
    (temp, runtime, host, repository)
}

fn upstream(name: &str) -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some("ice".to_string()),
        database_or_namespace: "sales".to_string(),
        name: name.to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

fn frontend_ledger() -> FrontendMvRefreshLedger {
    FrontendMvRefreshLedger {
        request_id: uuid::Uuid::now_v7().into_bytes().to_vec(),
        provider_id: "iceberg".to_string(),
        instance_id: "rest".to_string(),
        incarnation: uuid::Uuid::now_v7().into_bytes().to_vec(),
        expected_target_version: None,
        staging_create_operation_id: uuid::Uuid::now_v7().into_bytes().to_vec(),
        write_operation_id: uuid::Uuid::now_v7().into_bytes().to_vec(),
        publication_operation_id: uuid::Uuid::now_v7().into_bytes().to_vec(),
        staging_drop_operation_id: uuid::Uuid::now_v7().into_bytes().to_vec(),
        cohort_ids: vec!["primary".to_string()],
        actions: Vec::new(),
        cleanup_pending: false,
    }
}

fn frontend_action(
    phase: FrontendMvRefreshActionPhase,
    state: FrontendMvRefreshActionState,
    operation_id: Vec<u8>,
    committed_version: Option<FrontendMvRefreshCommittedVersion>,
) -> FrontendMvRefreshAction {
    FrontendMvRefreshAction {
        phase,
        state,
        operation_id,
        receipt: None,
        committed_version,
        external_evidence: None,
        provider_finalized: false,
    }
}

#[test]
fn reserved_frontend_refresh_ids_share_the_legacy_high_water_mark() {
    let (_temp, _runtime, _host, repository) = repository();
    assert_eq!(
        repository
            .reserve_frontend_refresh_id()
            .expect("reserve first frontend refresh ID"),
        1
    );
    assert_eq!(
        repository
            .reserve_frontend_refresh_id()
            .expect("reserve second frontend refresh ID"),
        2
    );

    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("reserved_refresh_id"),
        )
        .expect("create definition");
    let legacy = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin legacy refresh");
    assert_eq!(legacy.refresh_id, 3);
}

#[test]
fn frontend_refresh_v3_is_single_journal_and_isolated_from_legacy_recovery() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_frontend_v3"),
        )
        .expect("create definition");
    let ledger = frontend_ledger();
    let refresh = repository
        .begin_frontend_refresh_intent(BeginFrontendMvRefreshIntentRequest {
            refresh_id: 9001,
            mv_id: definition.mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "sales".to_string(),
            target_table: "daily_frontend_v3".to_string(),
            staging_branch: "__nova_mv_1".to_string(),
            expected_main_snapshot_id: Some(7),
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
            marker_token: "marker".to_string(),
            prepare_external_actions: true,
            ledger: ledger.clone(),
        })
        .expect("persist frontend intent");
    assert_eq!(
        refresh.operation_id, None,
        "v3 must not create legacy operation rows"
    );
    assert_eq!(
        refresh.refresh_id, 9001,
        "v3 keeps the preallocated identity"
    );
    assert_eq!(
        refresh.lifecycle_owner,
        MvRefreshLifecycleOwner::FrontendCurrent
    );
    assert_eq!(
        refresh
            .frontend_ledger
            .as_ref()
            .expect("v3 ledger")
            .actions
            .len(),
        4
    );
    assert!(
        repository
            .list_unfinished_refreshes()
            .expect("legacy unfinished scan")
            .is_empty(),
        "legacy recovery must not claim a frontend-owned attempt"
    );
    assert!(
        repository
            .list_unfinished_branch_staged_iceberg_refreshes()
            .expect("legacy staged scan")
            .is_empty(),
        "legacy staged recovery must not claim a frontend-owned attempt"
    );

    let publication_before_write = frontend_action(
        FrontendMvRefreshActionPhase::Publication,
        FrontendMvRefreshActionState::KnownCommitted,
        ledger.publication_operation_id.clone(),
        None,
    );
    assert!(
        repository
            .record_frontend_refresh_action(refresh.refresh_id, publication_before_write)
            .is_err()
    );

    let staging = frontend_action(
        FrontendMvRefreshActionPhase::StagingCreate,
        FrontendMvRefreshActionState::KnownCommitted,
        ledger.staging_create_operation_id.clone(),
        None,
    );
    repository
        .record_frontend_refresh_action(refresh.refresh_id, staging.clone())
        .expect("record staging create");
    repository
        .record_frontend_refresh_action(refresh.refresh_id, staging)
        .expect("idempotent staging create");

    let write_version =
        FrontendMvRefreshCommittedVersion::try_new(b"write-version".to_vec(), Some(10))
            .expect("committed version");
    repository
        .record_frontend_refresh_action(
            refresh.refresh_id,
            frontend_action(
                FrontendMvRefreshActionPhase::Write,
                FrontendMvRefreshActionState::KnownCommitted,
                ledger.write_operation_id.clone(),
                Some(write_version.clone()),
            ),
        )
        .expect("record write");
    repository
        .record_frontend_refresh_action(
            refresh.refresh_id,
            frontend_action(
                FrontendMvRefreshActionPhase::Publication,
                FrontendMvRefreshActionState::KnownCommitted,
                ledger.publication_operation_id.clone(),
                Some(write_version),
            ),
        )
        .expect("record guarded publication");
    let published = repository
        .load_refresh(refresh.refresh_id)
        .expect("load published refresh")
        .expect("published refresh exists");
    assert_eq!(published.state, MvRefreshState::PublishCommitted);
    assert_eq!(published.published_snapshot_id, Some(10));
    assert!(
        published
            .frontend_ledger
            .as_ref()
            .expect("v3 ledger")
            .cleanup_pending
    );

    repository
        .record_frontend_refresh_action(
            refresh.refresh_id,
            frontend_action(
                FrontendMvRefreshActionPhase::StagingDrop,
                FrontendMvRefreshActionState::KnownCommitted,
                ledger.staging_drop_operation_id.clone(),
                None,
            ),
        )
        .expect("record cleanup");
    assert!(
        !repository
            .load_refresh(refresh.refresh_id)
            .expect("load cleaned refresh")
            .expect("cleaned refresh exists")
            .frontend_ledger
            .expect("v3 ledger")
            .cleanup_pending
    );
}

#[test]
fn frontend_noop_refresh_persists_no_synthetic_external_actions() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_frontend_noop_v3"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_frontend_refresh_intent(BeginFrontendMvRefreshIntentRequest {
            refresh_id: 9002,
            mv_id: definition.mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "sales".to_string(),
            target_table: "daily_frontend_noop_v3".to_string(),
            staging_branch: "__nova_mv_2".to_string(),
            expected_main_snapshot_id: Some(7),
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
            marker_token: "marker".to_string(),
            prepare_external_actions: false,
            ledger: frontend_ledger(),
        })
        .expect("persist no-op frontend intent");

    assert!(
        refresh
            .frontend_ledger
            .expect("v3 ledger")
            .actions
            .is_empty(),
        "a no-op attempt must not invent staging or writer phases"
    );
    repository
        .finalize_frontend_refresh_without_external_actions(MvRefreshFinalizeRequest {
            refresh_id: refresh.refresh_id,
            rows: 0,
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid".to_string(),
            )]),
            target_snapshot_id: Some(7),
        })
        .expect("finalize no-op frontend refresh");
    let finalized = repository
        .load_refresh(refresh.refresh_id)
        .expect("load finalized no-op")
        .expect("no-op refresh exists");
    assert_eq!(finalized.state, MvRefreshState::Finalized);
    assert_eq!(
        repository
            .load_by_id(definition.mv_id)
            .expect("load definition")
            .expect("definition exists")
            .active_refresh_id,
        None
    );
}

#[test]
fn refresh_lifecycle_persists_transitions_and_finalizes_definition() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_refresh"),
        )
        .expect("create definition");
    let base_snapshots = BTreeMap::from([("ice.sales.orders".to_string(), 42)]);
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, base_snapshots.clone())
        .expect("begin refresh");
    assert_eq!(refresh.state, MvRefreshState::IntentCreated);
    repository
        .record_staging_commit(RecordStagingCommitRequest {
            refresh_id: refresh.refresh_id,
            staging_snapshot_id: 43,
            rows: 7,
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid-1".to_string(),
            )]),
        })
        .expect("record staging commit");
    repository
        .record_publish_commit(RecordPublishCommitRequest {
            refresh_id: refresh.refresh_id,
            published_snapshot_id: 44,
        })
        .expect("record publish commit");
    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: refresh.refresh_id,
            rows: 7,
            base_snapshots: base_snapshots.clone(),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "uuid-1".to_string(),
            )]),
            target_snapshot_id: Some(44),
        })
        .expect("finalize refresh");
    assert_eq!(
        repository
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .expect("refresh exists")
            .state,
        MvRefreshState::Finalized
    );
    let stored = repository
        .load_by_id(definition.mv_id)
        .expect("load definition")
        .expect("definition exists");
    assert!(!stored.refresh_in_progress);
    assert_eq!(stored.last_refreshed_iceberg_snapshot_id, Some(44));
}

#[test]
fn unfinished_refreshes_preserve_conflict_and_commit_unknown_guards() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_refresh_guard"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin refresh");
    let duplicate = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect_err("second intent conflicts");
    assert_eq!(duplicate.kind(), MvRepositoryErrorKind::Conflict);
    repository
        .mark_refresh_commit_unknown(refresh.refresh_id)
        .expect("mark unknown");
    assert_eq!(
        repository
            .list_unfinished_refreshes()
            .expect("list unfinished"),
        vec![
            repository
                .load_refresh(refresh.refresh_id)
                .expect("load refresh")
                .expect("refresh exists")
        ]
    );
    let clear = repository
        .clear_refresh_progress(definition.mv_id)
        .expect_err("commit unknown cannot be cleared");
    assert_eq!(clear.kind(), MvRepositoryErrorKind::Conflict);
}

#[test]
fn list_refreshes_pages_and_includes_finalized_and_aborted_records() {
    let (_temp, _runtime, _host, repository) = limited_repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_refresh_history"),
        )
        .expect("create definition");

    let finalized = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin finalized refresh");
    repository
        .record_staging_commit(RecordStagingCommitRequest {
            refresh_id: finalized.refresh_id,
            staging_snapshot_id: 1,
            rows: 1,
            base_table_uuids: BTreeMap::new(),
        })
        .expect("stage refresh");
    repository
        .record_publish_commit(RecordPublishCommitRequest {
            refresh_id: finalized.refresh_id,
            published_snapshot_id: 1,
        })
        .expect("publish refresh");
    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: finalized.refresh_id,
            rows: 1,
            base_snapshots: BTreeMap::new(),
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(1),
        })
        .expect("finalize refresh");

    let aborted = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin aborted refresh");
    assert!(
        repository
            .clear_refresh_progress(definition.mv_id)
            .expect("abort active refresh")
    );

    let unfinished = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin unfinished refresh");

    assert_eq!(
        repository
            .list_refreshes()
            .expect("list paged refresh history")
            .into_iter()
            .map(|refresh| (refresh.refresh_id, refresh.state))
            .collect::<Vec<_>>(),
        vec![
            (finalized.refresh_id, MvRefreshState::Finalized),
            (aborted.refresh_id, MvRefreshState::Aborted),
            (unfinished.refresh_id, MvRefreshState::IntentCreated),
        ]
    );
}

#[test]
fn refresh_commands_return_not_found_for_missing_definition_and_refresh() {
    let (_temp, _runtime, _host, repository) = repository();
    assert_eq!(
        repository
            .begin_refresh_intent(99, BTreeMap::new())
            .expect_err("missing definition")
            .kind(),
        MvRepositoryErrorKind::NotFound
    );
    assert_eq!(
        repository
            .record_staging_commit(RecordStagingCommitRequest {
                refresh_id: 99,
                staging_snapshot_id: 1,
                rows: 1,
                base_table_uuids: BTreeMap::new(),
            })
            .expect_err("missing refresh")
            .kind(),
        MvRepositoryErrorKind::NotFound
    );
}

#[test]
fn dropping_a_finalized_mv_removes_refresh_and_partition_records_before_reopen() {
    let (_temp, runtime, host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_drop_cleanup"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin refresh");
    repository
        .record_staging_commit(RecordStagingCommitRequest {
            refresh_id: refresh.refresh_id,
            staging_snapshot_id: 1,
            rows: 1,
            base_table_uuids: BTreeMap::new(),
        })
        .expect("stage refresh");
    repository
        .record_publish_commit(RecordPublishCommitRequest {
            refresh_id: refresh.refresh_id,
            published_snapshot_id: 2,
        })
        .expect("publish refresh");
    repository
        .finalize_refresh(MvRefreshFinalizeRequest {
            refresh_id: refresh.refresh_id,
            rows: 1,
            base_snapshots: BTreeMap::new(),
            base_table_uuids: BTreeMap::new(),
            target_snapshot_id: Some(2),
        })
        .expect("finalize refresh");
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: ["p1".to_string()].into(),
            last_refresh_ms: 1,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: Some(2),
            last_refresh_id: refresh.refresh_id,
            max_entries: 10,
        })
        .expect("persist partition");
    assert!(repository.drop_by_id(definition.mv_id).expect("drop MV"));
    drop(repository);
    let reopened = runtime
        .block_on(StateStoreMvRepository::open(
            host.state_store().expect("StateStore"),
            runtime.handle().clone(),
        ))
        .expect("reopen after cleanup");
    assert!(
        reopened
            .load_refresh(refresh.refresh_id)
            .expect("load refresh")
            .is_none()
    );
    assert!(
        reopened
            .list_partition_states(definition.mv_id)
            .expect("list removed partition states")
            .is_empty()
    );
}

#[test]
fn dropping_paged_mv_records_removes_all_dependencies_and_refresh_history() {
    let (_temp, runtime, host, repository) = limited_repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_drop_paged_cleanup"),
        )
        .expect("create definition");
    let upstreams = [
        upstream("customers"),
        upstream("lineitems"),
        upstream("products"),
    ];
    repository
        .replace_dependencies_for_mv(
            definition.mv_id,
            upstreams
                .iter()
                .cloned()
                .map(|upstream| CreateMvDependencyRequest {
                    upstream,
                    created_at_ms: 1,
                })
                .collect(),
        )
        .expect("create paged dependencies");
    let mut refresh_ids = Vec::new();
    for snapshot_id in 1..=3 {
        let refresh = repository
            .begin_refresh_intent(definition.mv_id, BTreeMap::new())
            .expect("begin refresh");
        repository
            .record_staging_commit(RecordStagingCommitRequest {
                refresh_id: refresh.refresh_id,
                staging_snapshot_id: snapshot_id,
                rows: snapshot_id,
                base_table_uuids: BTreeMap::new(),
            })
            .expect("stage refresh");
        repository
            .record_publish_commit(RecordPublishCommitRequest {
                refresh_id: refresh.refresh_id,
                published_snapshot_id: snapshot_id,
            })
            .expect("publish refresh");
        repository
            .finalize_refresh(MvRefreshFinalizeRequest {
                refresh_id: refresh.refresh_id,
                rows: snapshot_id,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(snapshot_id),
            })
            .expect("finalize refresh");
        refresh_ids.push(refresh.refresh_id);
    }
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: ["p1".to_string(), "p2".to_string(), "p3".to_string()].into(),
            last_refresh_ms: 3,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: Some(3),
            last_refresh_id: *refresh_ids.last().expect("finalized refresh"),
            max_entries: 10,
        })
        .expect("persist paged partition states");
    assert_eq!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list persisted partition states")
            .into_iter()
            .map(|state| state.partition_key)
            .collect::<Vec<_>>(),
        vec!["p1".to_string(), "p2".to_string(), "p3".to_string()]
    );
    assert!(repository.drop_by_id(definition.mv_id).expect("drop MV"));
    drop(repository);
    let reopened = runtime
        .block_on(StateStoreMvRepository::open(
            host.state_store().expect("StateStore"),
            runtime.handle().clone(),
        ))
        .expect("reopen after paged cleanup");
    assert!(
        reopened
            .list_dependencies_by_downstream(definition.mv_id)
            .expect("list removed dependencies")
            .is_empty()
    );
    assert!(
        reopened
            .list_partition_states(definition.mv_id)
            .expect("list removed partition states")
            .is_empty()
    );
    for upstream in &upstreams {
        assert!(
            reopened
                .list_downstream_dependencies(upstream)
                .expect("list removed upstream index")
                .is_empty()
        );
    }
    for refresh_id in refresh_ids {
        assert!(
            reopened
                .load_refresh(refresh_id)
                .expect("load removed refresh")
                .is_none()
        );
    }
}

#[test]
fn finalize_with_partitions_returns_not_found_for_missing_partition_mv() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_finalize_partition_not_found"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_refresh_intent(definition.mv_id, BTreeMap::new())
        .expect("begin refresh");
    let error = repository
        .finalize_refresh_with_partitions(FinalizeMvRefreshWithPartitionsRequest {
            refresh: MvRefreshFinalizeRequest {
                refresh_id: refresh.refresh_id,
                rows: 1,
                base_snapshots: BTreeMap::new(),
                base_table_uuids: BTreeMap::new(),
                target_snapshot_id: Some(1),
            },
            partitions: Some(ReplaceMvPartitionStatesRequest {
                mv_id: 99,
                partition_keys: ["p1".to_string()].into(),
                last_refresh_ms: 1,
                base_snapshots: BTreeMap::new(),
                target_snapshot_id: Some(1),
                last_refresh_id: refresh.refresh_id,
                max_entries: 10,
            }),
        })
        .expect_err("missing partition MV");
    assert_eq!(error.kind(), MvRepositoryErrorKind::NotFound);
}
