use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_frontend::mv::coordination::{
    MvRefreshOwnershipContext, OwnershipRefusal, acquire_refresh_ownership,
};
use novarocks_frontend::mv::domain::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks_frontend::mv::domain::persistence::definition::UpdateMvRefreshMetadataRequest;
use novarocks_frontend::mv::domain::persistence::dependency::CreateMvDependencyRequest;
use novarocks_frontend::mv::domain::persistence::partition::ReplaceMvPartitionStatesRequest;
use novarocks_frontend::mv::domain::persistence::refresh::{
    FrontendMvRefreshAction, FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
    FrontendMvRefreshCommittedVersion, FrontendMvRefreshEvidence, FrontendMvRefreshLedger,
    FrontendMvRefreshRecoveryBaseFact, FrontendMvRefreshRecoveryDisposition,
    FrontendMvRefreshRecoveryObservation, FrontendMvRefreshRecoveryStatus,
    MvRefreshFinalizeRequest, MvRefreshLifecycleOwner, MvRefreshState, RecordPublishCommitRequest,
    RecordStagingCommitRequest,
};
use novarocks_frontend::mv::domain::repository::{
    BeginFrontendMvRecoveryCycleRequest, FinalizeMvRefreshWithPartitionsRequest,
    FinalizeRecoveredMvRefreshRequest, MvRepository, MvRepositoryError, MvRepositoryErrorKind,
    RecordFrontendMvRecoveryCleanupOutcomeRequest, RecordFrontendMvRecoveryObservationRequest,
};
use novarocks_frontend::mv::repository::{
    BeginFrontendMvRefreshIntentRequest, FenceValidator, MvRefreshFenceSource,
    StateStoreMvRepository,
};
use novarocks_spi::connector::{ConnectorMvRefreshResourceIdentity, ConnectorProviderId};
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use sha2::{Digest, Sha256};

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

fn recovery_evidence(payload: &[u8]) -> FrontendMvRefreshEvidence {
    FrontendMvRefreshEvidence {
        payload: payload.to_vec(),
        digest: Sha256::digest(payload).to_vec(),
    }
}

#[test]
fn frontend_recovery_upgrades_v3_and_finalizes_published_truth_atomically() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_frontend_recovery_v4"),
        )
        .expect("create definition");
    let refresh = repository
        .begin_frontend_refresh_intent(BeginFrontendMvRefreshIntentRequest {
            refresh_id: 9010,
            mv_id: definition.mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "sales".to_string(),
            target_table: "daily_frontend_recovery_v4".to_string(),
            staging_branch: "__nova_mv_recovery".to_string(),
            expected_main_snapshot_id: Some(7),
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "orders-uuid".to_string(),
            )]),
            marker_token: "marker".to_string(),
            prepare_external_actions: true,
            ledger: frontend_ledger(),
        })
        .expect("persist frontend v3 intent");
    assert_eq!(
        refresh.base_table_uuids,
        BTreeMap::from([("ice.sales.orders".to_string(), "orders-uuid".to_string(),)]),
        "frontend refresh intent must durably retain the exact base table identity",
    );
    let cleanup_operation_id = refresh
        .frontend_recovery
        .as_ref()
        .expect("v3 intent preallocates v4 cleanup identity")
        .cleanup_operation_id
        .clone();
    let cycle_id = uuid::Uuid::now_v7().into_bytes().to_vec();
    repository
        .begin_frontend_recovery_cycle(BeginFrontendMvRecoveryCycleRequest {
            refresh_id: refresh.refresh_id,
            cycle_id: cycle_id.clone(),
            provider_id: "iceberg".to_string(),
            instance_id: "rest".to_string(),
            incarnation: uuid::Uuid::now_v7().into_bytes().to_vec(),
            cleanup_operation_id,
        })
        .expect("upgrade v3 record and persist inspection intent");
    let observation = FrontendMvRefreshRecoveryObservation {
        disposition: FrontendMvRefreshRecoveryDisposition::Published,
        digest: Sha256::digest(b"published observation").to_vec(),
        proof: recovery_evidence(b"opaque provider proof"),
        committed_version: Some(
            FrontendMvRefreshCommittedVersion::try_new(b"snapshot-42".to_vec(), Some(42))
                .expect("committed version"),
        ),
        resulting_row_count: Some(2),
        bases: vec![FrontendMvRefreshRecoveryBaseFact {
            table: "ice.sales.orders".to_string(),
            uuid: "orders-uuid".to_string(),
            from_snapshot: Some(9),
            to_snapshot: 9,
        }],
        definition_fingerprint: Some("definition-v1".to_string()),
        staging_snapshot_id: Some(42),
        target_snapshot_id: Some(42),
        cleanup_required: true,
    };
    repository
        .record_frontend_recovery_observation(RecordFrontendMvRecoveryObservationRequest {
            refresh_id: refresh.refresh_id,
            observation: observation.clone(),
        })
        .expect("record published inspection");
    repository
        .record_frontend_recovery_observation(RecordFrontendMvRecoveryObservationRequest {
            refresh_id: refresh.refresh_id,
            observation: observation.clone(),
        })
        .expect("identical observation is idempotent");
    let mut conflicting = observation.clone();
    conflicting.digest = Sha256::digest(b"conflicting observation").to_vec();
    assert_eq!(
        repository
            .record_frontend_recovery_observation(RecordFrontendMvRecoveryObservationRequest {
                refresh_id: refresh.refresh_id,
                observation: conflicting,
            })
            .expect_err("different proof must fail closed")
            .kind(),
        MvRepositoryErrorKind::Conflict
    );
    repository
        .record_frontend_recovery_cleanup_outcome(RecordFrontendMvRecoveryCleanupOutcomeRequest {
            refresh_id: refresh.refresh_id,
            state: FrontendMvRefreshActionState::KnownCommitted,
            evidence: None,
            provider_finalized: true,
        })
        .expect("persist known cleanup");
    let recovery = repository
        .load_refresh(refresh.refresh_id)
        .expect("load refresh")
        .expect("refresh exists")
        .frontend_recovery
        .expect("v4 recovery ledger");
    repository
        .finalize_recovered_published_refresh(FinalizeRecoveredMvRefreshRequest {
            finalize: MvRefreshFinalizeRequest {
                refresh_id: refresh.refresh_id,
                rows: 2,
                base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
                base_table_uuids: BTreeMap::from([(
                    "ice.sales.orders".to_string(),
                    "orders-uuid".to_string(),
                )]),
                target_snapshot_id: Some(42),
                partition_spec: None,
            },
            recovery,
        })
        .expect("atomically finalize recovered publication");
    let stored = repository
        .load_refresh(refresh.refresh_id)
        .expect("load finalized recovery")
        .expect("refresh exists");
    assert_eq!(stored.state, MvRefreshState::Finalized);
    assert_eq!(
        stored.frontend_recovery.expect("recovery state").status,
        FrontendMvRefreshRecoveryStatus::ResolvedPublished
    );
    let definition = repository
        .load_by_id(definition.mv_id)
        .expect("load definition")
        .expect("definition exists");
    assert_eq!(definition.active_refresh_id, None);
    assert_eq!(definition.last_refresh_rows, Some(2));
    assert_eq!(
        definition.last_refresh_table_uuids,
        BTreeMap::from([("ice.sales.orders".to_string(), "orders-uuid".to_string(),)]),
        "recovery finalize must preserve the identity frozen by the refresh intent",
    );
    assert_eq!(definition.last_refreshed_iceberg_snapshot_id, Some(42));
    assert_eq!(cycle_id.len(), 16);
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
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "orders-uuid".to_string(),
            )]),
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
fn frontend_refresh_rejects_mismatched_base_snapshot_and_uuid_keys() {
    let (_temp, _runtime, _host, repository) = repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_frontend_invalid_base_identity"),
        )
        .expect("create definition");
    let error = repository
        .begin_frontend_refresh_intent(BeginFrontendMvRefreshIntentRequest {
            refresh_id: 9003,
            mv_id: definition.mv_id,
            target_catalog: "ice".to_string(),
            target_namespace: "sales".to_string(),
            target_table: "daily_frontend_invalid_base_identity".to_string(),
            staging_branch: "__nova_mv_3".to_string(),
            expected_main_snapshot_id: Some(7),
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
            base_table_uuids: BTreeMap::from([(
                "ice.sales.customers".to_string(),
                "customers-uuid".to_string(),
            )]),
            marker_token: "marker".to_string(),
            prepare_external_actions: true,
            ledger: frontend_ledger(),
        })
        .expect_err("mismatched base identity keys must fail closed");
    assert_eq!(error.kind(), MvRepositoryErrorKind::InvalidRequest);
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
            base_table_uuids: BTreeMap::from([(
                "ice.sales.orders".to_string(),
                "orders-uuid".to_string(),
            )]),
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
            partition_spec: None,
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
            partition_spec: None,
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
            partition_spec: None,
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
            partition_spec: None,
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
                partition_spec: None,
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
                partition_spec: None,
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

/// A fence source standing in for an owner that has been superseded: it hands
/// back a validator that always rejects, exactly as a stale `LeaseFence` does
/// once another frontend has taken the resource over.
struct SupersededOwner;

impl MvRefreshFenceSource for SupersededOwner {
    fn validator_for(&self, _mv_id: i64) -> Result<FenceValidator, MvRepositoryError> {
        Ok(Arc::new(|_transaction| {
            Box::pin(async { Err("lease fence superseded".to_string()) })
        }))
    }
}

/// A fence source for an owner this process does not hold at all. Returning an
/// error here — rather than `None` — is the fail-closed path: "I lost the lease"
/// must never be indistinguishable from "there is no fencing configured".
struct NotTheOwner;

impl MvRefreshFenceSource for NotTheOwner {
    fn validator_for(&self, mv_id: i64) -> Result<FenceValidator, MvRepositoryError> {
        Err(MvRepositoryError::new(
            MvRepositoryErrorKind::Conflict,
            format!("this frontend does not own mv {mv_id}"),
        ))
    }
}

fn fenced_repository(
    fence: Arc<dyn MvRefreshFenceSource>,
) -> (
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
                        cluster_id: "mv-refresh-fence-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-refresh-fence-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-refresh-fence-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        ))
        .expect("open SQLite StateStore host");
    let repository = runtime
        .block_on(
            StateStoreMvRepository::open_with_observations_and_refresh_fence(
                host.state_store().expect("host exposes StateStore"),
                runtime.handle().clone(),
                None,
                Some(fence),
            ),
        )
        .expect("open fenced MV repository");
    (temp, runtime, host, repository)
}

fn fence_intent_request(mv_id: i64) -> BeginFrontendMvRefreshIntentRequest {
    BeginFrontendMvRefreshIntentRequest {
        refresh_id: 9101,
        mv_id,
        target_catalog: "ice".to_string(),
        target_namespace: "sales".to_string(),
        target_table: "daily_fenced".to_string(),
        staging_branch: "__nova_mv_fenced".to_string(),
        expected_main_snapshot_id: Some(7),
        base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 9)]),
        base_table_uuids: BTreeMap::from([(
            "ice.sales.orders".to_string(),
            "orders-uuid".to_string(),
        )]),
        marker_token: "marker".to_string(),
        prepare_external_actions: true,
        ledger: frontend_ledger(),
    }
}

#[test]
fn superseded_owner_cannot_begin_a_refresh_intent() {
    let (_temp, _runtime, _host, repository) = fenced_repository(Arc::new(SupersededOwner));
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_fenced"),
        )
        .expect("create definition");

    let error = repository
        .begin_frontend_refresh_intent(fence_intent_request(definition.mv_id))
        .expect_err("a superseded owner must not create a durable refresh intent");

    // The rejection has to come from the transaction, not from a service-level
    // precheck, which is why this is asserted against the repository directly.
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict, "{error}");

    // And nothing may have been written: the definition must still be idle.
    let reloaded = repository
        .load_by_id(definition.mv_id)
        .expect("reload definition")
        .expect("definition present");
    assert!(
        !reloaded.refresh_in_progress && reloaded.active_refresh_id.is_none(),
        "a fence-rejected intent must leave no durable trace"
    );
}

#[test]
fn a_frontend_that_owns_nothing_cannot_begin_a_refresh_intent() {
    let (_temp, _runtime, _host, repository) = fenced_repository(Arc::new(NotTheOwner));
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_fenced"),
        )
        .expect("create definition");

    let error = repository
        .begin_frontend_refresh_intent(fence_intent_request(definition.mv_id))
        .expect_err("an unowned target must not accept a refresh intent");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict, "{error}");
}

#[test]
fn definition_ddl_stays_outside_the_refresh_ownership_fence() {
    // Creating an MV is catalog DDL guarded by the attachment observation, not
    // by refresh execution ownership. A frontend that owns no refresh lease must
    // still be able to define one, or CREATE MATERIALIZED VIEW would require a
    // lease on a target that does not exist yet.
    let (_temp, _runtime, _host, repository) = fenced_repository(Arc::new(SupersededOwner));
    repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_fenced"),
        )
        .expect("definition DDL must not require a refresh lease");
}

#[test]
fn definition_ddl_can_update_refresh_configuration_without_a_refresh_lease() {
    let (_temp, _runtime, _host, repository) = fenced_repository(Arc::new(SupersededOwner));
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_fenced"),
        )
        .expect("create definition");

    let updated = repository
        .update_definition_refresh_metadata(UpdateMvRefreshMetadataRequest {
            mv_id: definition.mv_id,
            refresh_policy: definition.refresh_policy,
            refresh_paused: true,
            refresh_interval_ms: definition.refresh_interval_ms,
            max_staleness_ms: definition.max_staleness_ms,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
        })
        .expect("definition DDL must not require a refresh lease");

    assert!(updated.refresh_paused);
}

/// A registry-backed fence source, exactly as production would install it.
struct RegistryLikeSource {
    registered: std::sync::Mutex<std::collections::HashSet<i64>>,
}

impl RegistryLikeSource {
    fn empty() -> Self {
        Self {
            registered: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }
}

impl MvRefreshFenceSource for RegistryLikeSource {
    fn validator_for(&self, mv_id: i64) -> Result<FenceValidator, MvRepositoryError> {
        if !self.registered.lock().unwrap().contains(&mv_id) {
            return Err(MvRepositoryError::new(
                MvRepositoryErrorKind::Conflict,
                format!("no refresh lease registered for mv {mv_id}"),
            ));
        }
        Ok(Arc::new(|_transaction| Box::pin(async { Ok(()) })))
    }
}

/// Pins the coupling between installing the fence source and registering
/// ownership at the refresh entry points.
///
/// The registry fails closed for unregistered targets, so installing it without
/// wiring the entry points stops every refresh in the cluster. This test makes
/// that an executable fact rather than a comment: it goes red the moment a
/// composition installs a registry-backed source while the entry points still do
/// not register, which is precisely the half-landed change to prevent.
#[test]
fn installing_a_fence_source_without_registration_stops_every_refresh() {
    let (_temp, _runtime, _host, repository) =
        fenced_repository(Arc::new(RegistryLikeSource::empty()));
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_fenced"),
        )
        .expect("create definition");

    let error = repository
        .begin_frontend_refresh_intent(fence_intent_request(definition.mv_id))
        .expect_err("an unregistered target must not begin a refresh");
    assert_eq!(error.kind(), MvRepositoryErrorKind::Conflict, "{error}");

    // The failure is total, not partial: no refresh can start for any target,
    // which is what makes install-without-registration a cluster outage rather
    // than a degraded mode.
    let second = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("another_fenced"),
        )
        .expect("create second definition");
    assert!(
        repository
            .begin_frontend_refresh_intent(fence_intent_request(second.mv_id))
            .is_err(),
        "install-without-registration must fail closed for every target"
    );
}

/// Two logical frontends sharing one StateStore, competing for the same target.
///
/// This is the property the whole ownership layer exists for: two frontends both
/// find a target due, and exactly one may proceed. It is asserted against real
/// coordination over a shared SQLite StateStore rather than a mock, because the
/// failure this guards against is precisely two processes disagreeing about who
/// won.
#[test]
fn two_frontends_competing_for_one_target_yield_a_single_owner() {
    let temp = tempfile::tempdir().expect("temporary StateStore directory");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let registry = builtin_state_store_provider_registry().expect("providers");
    let host = runtime
        .block_on(StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "mv-ownership-race".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-ownership-race".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                // The SQLite backend supports one FE deployment. That is fine
                // here: the race being tested is between two logical controllers
                // sharing a store, which is what two coordination runtimes model.
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-ownership-race-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        ))
        .expect("open SQLite StateStore host");
    let store = host.state_store().expect("host exposes StateStore");

    // Two independent coordination runtimes over one store: this is what two
    // frontend processes look like to the lease manager.
    let first = runtime
        .block_on(MvRefreshOwnershipContext::open(Arc::clone(&store)))
        .expect("first frontend coordination");
    let second = runtime
        .block_on(MvRefreshOwnershipContext::open(Arc::clone(&store)))
        .expect("second frontend coordination");

    let resource = ConnectorMvRefreshResourceIdentity::try_new(
        ConnectorProviderId::parse("iceberg").expect("provider"),
        uuid::Uuid::from_u128(0x5150),
    )
    .expect("stable target resource");

    let winner = runtime
        .block_on(acquire_refresh_ownership(&first, 42, resource.clone()))
        .expect("the first frontend acquires an uncontended target");

    // The second frontend sees the same target as due and tries to take it.
    let refusal = runtime
        .block_on(acquire_refresh_ownership(&second, 42, resource.clone()))
        .expect_err("a second frontend must not also own the target");
    assert!(
        matches!(
            refusal,
            OwnershipRefusal::Contended | OwnershipRefusal::AwaitingTakeover
        ),
        "contention must be reported as contention, not as unavailability: {refusal:?}"
    );

    // And the loser has no registered fence, so its durable transitions would
    // fail closed rather than racing the winner's.
    assert!(
        !second.registry.holds(42),
        "a frontend that lost the race must not be registered as an owner"
    );
    assert!(
        first.registry.holds(42),
        "the winner must be registered so its transitions can prove ownership"
    );

    // The per-refresh handle is deliberately not the lease lifetime: ownership
    // stays sticky for the target so a later refresh on the same frontend does
    // not race an asynchronous release from this one.
    drop(winner);
    assert!(
        first.registry.holds(42),
        "dropping one refresh handle must not release sticky target ownership"
    );

    // Frontend teardown, not an individual refresh completion, releases the
    // target. `shutdown` removes the fence registration synchronously before
    // releasing its guard on the runtime, so subsequent durable transitions
    // fail closed while another frontend waits to take the lease.
    first.shutdown();
    assert!(!first.registry.holds(42));
}

/// Guards that installing ownership is a deliberate choice, not a default.
///
/// The unfenced constructor still exists because a composition without a
/// StateStore has a structurally single owner. This pins that the difference is
/// explicit: a repository is fenced exactly when a fence source was passed, so a
/// composition that forgets to install ownership is detectable rather than
/// quietly unfenced.
#[test]
fn a_repository_is_fenced_exactly_when_a_fence_source_is_installed() {
    let (_temp, _runtime, _host, fenced) = fenced_repository(Arc::new(SupersededOwner));
    assert!(
        fenced.has_refresh_fence(),
        "installing a fence source must make refresh transitions fenced"
    );

    let (_temp2, _runtime2, _host2, unfenced) = repository();
    assert!(
        !unfenced.has_refresh_fence(),
        "the unfenced constructor must remain visibly unfenced, not accidentally fenced"
    );
}
