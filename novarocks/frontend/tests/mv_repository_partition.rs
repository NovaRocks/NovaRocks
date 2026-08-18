use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_frontend::mv::domain::persistence::partition::{
    MvPartitionRefreshStatus, RecordFailedMvPartitionStatesRequest,
    ReplaceMvPartitionStatesRequest, UpdateMvPartitionContractRequest,
};
use novarocks_frontend::mv::domain::persistence::schema::{
    BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind, ExpressionLineage,
    HiddenApplyKeyContract, MvPartitionContract, MvPartitionFieldContract,
    MvPartitionTransformContract, MvSchemaContract, OutputColumnLineage, OutputContract,
    TargetContract, TargetVisibleColumn,
};
use novarocks_frontend::mv::domain::repository::MvRepository;
use novarocks_frontend::mv::repository::StateStoreMvRepository;
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_sql::planning::mv::ApplyKeySource;
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

#[path = "mv_repository_definition.rs"]
mod definition_support;

fn partition_contract(spec_id: i32) -> MvPartitionContract {
    MvPartitionContract {
        target_spec_id: spec_id,
        fields: vec![MvPartitionFieldContract {
            partition_field_id: 1_000 + spec_id,
            partition_field_name: "id".to_string(),
            source_target_field_id: 10,
            source_column_name: "id".to_string(),
            transform: MvPartitionTransformContract::Identity,
        }],
    }
}

fn schema_contract(partition: MvPartitionContract) -> MvSchemaContract {
    MvSchemaContract {
        contract_version: 1,
        base: BaseContract {
            table_fqn: "ice.sales.orders".to_string(),
            table_uuid: "11111111-1111-1111-1111-111111111111".to_string(),
            alias_at_create: Some("orders".to_string()),
            schema_id_at_create: 7,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![BaseFieldRecord {
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    type_signature: "long".to_string(),
                    required: true,
                }],
            },
        },
        bases: vec![],
        output: OutputContract {
            columns: vec![OutputColumnLineage {
                expression: ExpressionLineage {
                    kind: ExpressionKind::Column,
                    referenced_base_field_ids: vec![1],
                    referenced_base_fields: vec![],
                },
            }],
            filter: None,
        },
        join: None,
        aggregate: None,
        branch: None,
        target: TargetContract {
            table_fqn: "ice.analytics.orders_mv".to_string(),
            table_uuid: "22222222-2222-2222-2222-222222222222".to_string(),
            schema_id_at_create: 11,
            visible_columns: vec![TargetVisibleColumn {
                output_name: "id".to_string(),
                target_field_id: 10,
                type_signature: "long".to_string(),
                nullable: false,
            }],
            hidden_apply_key: HiddenApplyKeyContract {
                column_name: "__nova_base_row_id".to_string(),
                target_field_id: 99,
                source: ApplyKeySource::BaseRowId,
            },
            partition: Some(partition),
        },
    }
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
                        cluster_id: "mv-partition-limit-test".to_string(),
                        limits: StateStoreLimitOverrides {
                            max_page_size: Some(2),
                            ..StateStoreLimitOverrides::default()
                        },
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-partition-limit-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-partition-limit-test-r1"),
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

#[test]
fn partition_replacement_is_ordered_and_marks_the_definition_complete() {
    let (_temp, _runtime, _host, repository) = definition_support::repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_partition"),
        )
        .expect("create definition");
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p2".to_string(), "p1".to_string()]),
            last_refresh_ms: 10,
            base_snapshots: BTreeMap::from([("ice.sales.orders".to_string(), 42)]),
            target_snapshot_id: Some(44),
            last_refresh_id: 1,
            max_entries: 2,
        })
        .expect("replace partition states");
    let states = repository
        .list_partition_states(definition.mv_id)
        .expect("list partition states");
    assert_eq!(states.len(), 2);
    assert_eq!(states[0].partition_key, "p1");
    assert_eq!(states[0].status, MvPartitionRefreshStatus::Fresh);
    assert!(
        repository
            .load_by_id(definition.mv_id)
            .expect("load definition")
            .expect("definition exists")
            .partition_state_complete
    );
}

#[test]
fn partition_limits_clear_existing_state_without_staging_partial_records() {
    let (_temp, _runtime, _host, repository) = definition_support::repository();
    let definition = repository
        .create(
            uuid::Uuid::now_v7(),
            definition_support::create_request("daily_partition_limit"),
        )
        .expect("create definition");
    repository
        .record_failed_partition_states(RecordFailedMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p1".to_string()]),
            failure_message: "injected failure".to_string(),
            last_refresh_ms: 11,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 1,
            max_entries: 1,
        })
        .expect("record failed partition");
    assert_eq!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list failed partition")[0]
            .status,
        MvPartitionRefreshStatus::Failed
    );
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p2".to_string(), "p3".to_string()]),
            last_refresh_ms: 12,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 2,
            max_entries: 1,
        })
        .expect("oversized replacement marks incomplete");
    assert!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list cleared states")
            .is_empty()
    );
    assert!(
        !repository
            .load_by_id(definition.mv_id)
            .expect("load definition")
            .expect("definition exists")
            .partition_state_complete
    );
}

#[test]
fn partition_mutations_page_through_the_configured_state_store_limit() {
    let (_temp, _runtime, _host, repository) = limited_repository();
    let initial_partition = partition_contract(1);
    let replacement_partition = partition_contract(2);
    let mut request = definition_support::create_request("daily_partition_paged");
    request.definition.partition_spec = Some(initial_partition.clone());
    request.definition.schema_contract = Some(schema_contract(initial_partition));
    let definition = repository
        .create(uuid::Uuid::now_v7(), request)
        .expect("create definition");
    let keys = BTreeSet::from(["p1".to_string(), "p2".to_string(), "p3".to_string()]);
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: keys.clone(),
            last_refresh_ms: 1,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 1,
            max_entries: 10,
        })
        .expect("replace more than one page");
    repository
        .record_failed_partition_states(RecordFailedMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: keys,
            failure_message: "injected failure".to_string(),
            last_refresh_ms: 2,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 2,
            max_entries: 10,
        })
        .expect("replace failed states through pages");
    assert_eq!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list states")
            .len(),
        3
    );
    repository
        .update_partition_contract(UpdateMvPartitionContractRequest {
            mv_id: definition.mv_id,
            partition_spec: replacement_partition.clone(),
        })
        .expect("replace partition contract through pages");
    assert!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list contract-cleared states")
            .is_empty()
    );
    assert_eq!(
        repository
            .load_by_id(definition.mv_id)
            .expect("load updated definition")
            .expect("definition exists")
            .partition_spec,
        Some(replacement_partition)
    );
    repository
        .replace_partition_states(ReplaceMvPartitionStatesRequest {
            mv_id: definition.mv_id,
            partition_keys: BTreeSet::from(["p1".to_string(), "p2".to_string(), "p3".to_string()]),
            last_refresh_ms: 3,
            base_snapshots: BTreeMap::new(),
            target_snapshot_id: None,
            last_refresh_id: 3,
            max_entries: 10,
        })
        .expect("repopulate states for clear");
    assert!(
        repository
            .clear_partition_states(definition.mv_id)
            .expect("clear states through pages")
    );
    assert!(
        repository
            .list_partition_states(definition.mv_id)
            .expect("list cleared states")
            .is_empty()
    );
}
