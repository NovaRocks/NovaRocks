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

//! Product-owned process lifecycle for materialized-view activity and workers.

use std::time::Instant;

use crate::activity::{
    MvActivityAdmissionError, MvActivityGate, MvActivityLease, MvActivityOwner, MvActivityTicket,
};
use crate::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvDropCatalogRegistrationPort,
    MvDropProjectionPort, MvDropProviderPort, MvProviderFailure,
};
use crate::process_runtime::{
    MvBackgroundRuntimeLifecycleError, MvBackgroundRuntimeOwner, MvBackgroundRuntimeStart,
};
use crate::product::{
    MvCommand, MvCreateCommand, MvOperationContext, MvProductError, MvProductErrorKind,
    MvProductResult, MvRefreshAttemptIdentity, MvTarget,
};
use crate::readiness::MvDropReadiness;

/// The one process-local MV owner for activity admission and background-worker
/// lifecycle. Hosts may inject effect callbacks, but cannot own a parallel
/// gate, shutdown state, or worker supervisor.
#[derive(Default)]
pub struct MvProductService {
    activity_gate: MvActivityGate,
    background: MvBackgroundRuntimeOwner,
}

impl MvProductService {
    /// Reserve the one immutable identity for a product refresh publication.
    /// Query and provider adapters may derive their wire values from it but
    /// cannot mint a second identity for the same product transition.
    pub fn reserve_refresh_attempt(&self) -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity::reserve()
    }

    /// Execute the product-owned CREATE state machine. The outer adapter owns
    /// SQL lowering and provider capabilities, while this product owns the
    /// effect order and the distinction between pre-commit cleanup and a
    /// known-committed finalization failure.
    pub fn create(
        &self,
        operation: MvOperationContext,
        create: MvCreateCommand,
        provider: &dyn MvCreateProviderPort,
        catalog_registration: &dyn MvCreateCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        let command = MvCommand::Create(create);
        let target = match &command {
            MvCommand::Create(create) => create.target.clone(),
            MvCommand::Drop { .. } | MvCommand::Refresh { .. } | MvCommand::Show { .. } => {
                unreachable!("CREATE product path constructed a non-CREATE command")
            }
        };
        let created = provider
            .create_target(operation, &command)
            .map_err(MvProviderFailure::into_product_error)?;
        let definition = match provider.inspect_created_target(operation, &created) {
            Ok(definition) => definition,
            Err(primary) => {
                return Err(cleanup_after_inspection_failure(
                    operation, provider, target, primary,
                ));
            }
        };
        provider
            .sync_target_descriptor(operation, &created, &definition)
            .map_err(MvProviderFailure::into_product_error)?;
        provider
            .project_created_target(operation, &created)
            .map_err(known_committed_finalize_failure)?;
        catalog_registration
            .register_target(operation, &created)
            .map_err(known_committed_finalize_failure)?;
        Ok(MvProductResult::Created(created))
    }

    /// Execute the product-owned DROP state machine. The projection port
    /// supplies only exact durable facts; provider and catalog ports carry the
    /// outer effects without owning their order or terminal interpretation.
    pub fn drop(
        &self,
        operation: MvOperationContext,
        target: MvTarget,
        if_exists: bool,
        projection: &dyn MvDropProjectionPort,
        provider: &dyn MvDropProviderPort,
        catalog_registration: &dyn MvDropCatalogRegistrationPort,
    ) -> Result<MvProductResult, MvProductError> {
        match projection
            .prepare_drop(operation, &target, if_exists)
            .map_err(MvProviderFailure::into_product_error)?
        {
            MvDropReadiness::AlreadyAbsent => Ok(MvProductResult::Acknowledged),
            MvDropReadiness::ReadyToDrop => {
                provider
                    .drop_target(operation, &target)
                    .map_err(MvProviderFailure::into_product_error)?;
                projection
                    .delete_after_provider_drop(operation, &target)
                    .map_err(known_committed_finalize_failure)?;
                catalog_registration
                    .unregister_target(operation, &target)
                    .map_err(known_committed_finalize_failure)?;
                Ok(MvProductResult::Dropped)
            }
        }
    }

    pub fn acquire_foreground(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
        cancelled: impl Fn() -> bool,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        self.activity_gate
            .acquire_foreground(target, owner, cancelled)
    }

    /// Register background work with the shared product FIFO gate. The host
    /// retains only its effect callback and must release the returned ticket or
    /// lease; the product retains the state machine itself.
    pub fn request_activity(
        &self,
        target: MvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, crate::activity::MvActivityGateError> {
        self.activity_gate.request(target, owner)
    }

    pub fn begin_background_start(
        &self,
    ) -> Result<MvBackgroundRuntimeStart<'_>, MvBackgroundRuntimeLifecycleError> {
        self.background.begin_start()
    }

    pub fn begin_stopping(&self) {
        self.activity_gate.begin_stopping();
    }

    pub async fn shutdown_background_workers_until(&self, deadline: Instant) -> Result<(), String> {
        self.background.shutdown_until(deadline).await
    }

    pub fn request_background_stop_for_process_exit(&self) {
        self.background.request_stop_for_process_exit();
    }
}

fn cleanup_after_inspection_failure(
    operation: MvOperationContext,
    provider: &dyn MvCreateProviderPort,
    target: MvTarget,
    primary: MvProviderFailure,
) -> MvProductError {
    let primary = primary.into_product_error();
    match provider.cleanup_created_target(operation, &target) {
        Ok(()) => primary,
        Err(cleanup) => MvProductError::new(
            primary.kind(),
            format!("{}; target cleanup failed: {cleanup}", primary.message()),
        ),
    }
}

fn known_committed_finalize_failure(failure: MvProviderFailure) -> MvProductError {
    MvProductError::new(
        MvProductErrorKind::KnownCommittedFinalizeFailed,
        failure.message(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::MvProductService;
    use crate::activity::{CanonicalMvTarget, MvActivityGateError, MvActivityOwner};
    use crate::persistence::{
        definition::{CreateMvDefinitionRequest, MvDesiredRefreshPolicy},
        descriptor::MvDescriptorV3,
        schema::{
            BaseContract, BaseSchemaSnapshot, HiddenApplyKeyContract, MvSchemaContract,
            OutputContract, TargetContract,
        },
        semantic::MvRefreshDesiredConfiguration,
    };
    use crate::ports::{
        MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvDropCatalogRegistrationPort,
        MvDropProjectionPort, MvDropProviderPort, MvProviderFailure, MvProviderFailureKind,
    };
    use crate::product::{
        MvCommand, MvCreateCommand, MvCreatedTarget, MvOperationContext, MvPreparedDefinition,
        MvProductErrorKind, MvProductResult, MvTarget,
    };
    use crate::readiness::MvDropReadiness;
    use crate::repository::InitialMvRefreshConfiguration;
    use bytes::Bytes;
    use novarocks_query_application::persisted_query_definition::{
        PersistedQueryDefinition, PersistedQueryDialect,
    };
    use novarocks_spi::connector::ConnectorTableObjectId;
    use novarocks_sql::planning::mv::ApplyKeySource;
    use uuid::Uuid;

    #[derive(Default)]
    struct CreateEffects {
        events: Mutex<Vec<&'static str>>,
        fail_inspection: bool,
        fail_projection: bool,
        drop_absent: bool,
    }

    impl CreateEffects {
        fn record(&self, event: &'static str) {
            self.events.lock().expect("event lock").push(event);
        }

        fn events(&self) -> Vec<&'static str> {
            self.events.lock().expect("event lock").clone()
        }
    }

    impl MvCreateProviderPort for CreateEffects {
        fn create_target(
            &self,
            _operation: MvOperationContext,
            command: &MvCommand,
        ) -> Result<MvCreatedTarget, MvProviderFailure> {
            self.record("create");
            let MvCommand::Create(create) = command else {
                return Err(MvProviderFailure::new(
                    MvProviderFailureKind::InvalidRequest,
                    "CREATE adapter received a non-CREATE command",
                ));
            };
            Ok(MvCreatedTarget {
                target: create.target.clone(),
                table_uuid: "created-table".to_string(),
            })
        }

        fn inspect_created_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvCreatedTarget,
        ) -> Result<MvPreparedDefinition, MvProviderFailure> {
            self.record("inspect");
            if self.fail_inspection {
                return Err(MvProviderFailure::new(
                    MvProviderFailureKind::Unavailable,
                    "inspection failed",
                ));
            }
            Ok(MvPreparedDefinition {
                descriptor: test_descriptor(),
            })
        }

        fn sync_target_descriptor(
            &self,
            _operation: MvOperationContext,
            _target: &MvCreatedTarget,
            _definition: &MvPreparedDefinition,
        ) -> Result<(), MvProviderFailure> {
            self.record("sync");
            Ok(())
        }

        fn project_created_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvCreatedTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("project");
            if self.fail_projection {
                return Err(MvProviderFailure::new(
                    MvProviderFailureKind::Unavailable,
                    "projection failed",
                ));
            }
            Ok(())
        }

        fn cleanup_created_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("drop");
            Ok(())
        }
    }

    impl MvCreateCatalogRegistrationPort for CreateEffects {
        fn register_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvCreatedTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("register");
            Ok(())
        }
    }

    impl MvDropProviderPort for CreateEffects {
        fn drop_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("drop");
            Ok(())
        }
    }

    impl MvDropCatalogRegistrationPort for CreateEffects {
        fn unregister_target(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("unregister");
            Ok(())
        }
    }

    impl MvDropProjectionPort for CreateEffects {
        fn prepare_drop(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
            _if_exists: bool,
        ) -> Result<MvDropReadiness, MvProviderFailure> {
            self.record("prepare_drop");
            Ok(if self.drop_absent {
                MvDropReadiness::AlreadyAbsent
            } else {
                MvDropReadiness::ReadyToDrop
            })
        }

        fn delete_after_provider_drop(
            &self,
            _operation: MvOperationContext,
            _target: &MvTarget,
        ) -> Result<(), MvProviderFailure> {
            self.record("delete_projection");
            Ok(())
        }
    }

    fn create_command() -> MvCreateCommand {
        MvCreateCommand {
            target: MvTarget::from_parts(Some("iceberg"), "db", "mv"),
            if_not_exists: false,
            definition: CreateMvDefinitionRequest {
                query_definition: PersistedQueryDefinition::new(
                    "SELECT 1",
                    PersistedQueryDialect::StarRocks,
                    "iceberg",
                    "db",
                )
                .expect("valid persisted query"),
                base_table_refs: Vec::new(),
                primary_key_columns: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: Some("iceberg".to_string()),
                target_namespace: Some("db".to_string()),
                target_table: Some("mv".to_string()),
                schema_contract: None,
                partition_spec: None,
                created_at_ms: 1,
            },
            refresh: InitialMvRefreshConfiguration::default(),
            dependencies: Vec::new(),
        }
    }

    fn operation() -> MvOperationContext {
        MvOperationContext {
            operation_id: Uuid::nil(),
        }
    }

    fn test_descriptor() -> MvDescriptorV3 {
        MvDescriptorV3 {
            descriptor_version: 3,
            package_id: "test.mv".to_string(),
            query_definition: PersistedQueryDefinition::new(
                "SELECT 1",
                PersistedQueryDialect::StarRocks,
                "iceberg",
                "db",
            )
            .expect("valid persisted query"),
            visible_columns: Vec::new(),
            hidden_columns: Vec::new(),
            base_dependencies: Vec::new(),
            primary_key_columns: Vec::new(),
            schema_contract: MvSchemaContract {
                contract_version: 1,
                base: BaseContract {
                    table_fqn: "iceberg.db.base".to_string(),
                    table_object_id: ConnectorTableObjectId::try_new(Bytes::from_static(b"base"))
                        .expect("valid object ID"),
                    alias_at_create: None,
                    schema_id_at_create: 0,
                    schema_at_create: BaseSchemaSnapshot { fields: Vec::new() },
                },
                bases: Vec::new(),
                output: OutputContract {
                    columns: Vec::new(),
                    filter: None,
                },
                join: None,
                aggregate: None,
                branch: None,
                target: TargetContract {
                    table_fqn: "iceberg.db.mv".to_string(),
                    table_uuid: "created-table".to_string(),
                    schema_id_at_create: 0,
                    visible_columns: Vec::new(),
                    hidden_apply_key: HiddenApplyKeyContract {
                        column_name: "__nova_base_row_id".to_string(),
                        target_field_id: 1,
                        source: ApplyKeySource::BaseRowId,
                    },
                    partition: None,
                },
            },
            refresh: MvRefreshDesiredConfiguration::new(
                MvDesiredRefreshPolicy::Manual,
                false,
                None,
                None,
            )
            .expect("manual refresh configuration"),
            created_at_ms: 1,
        }
    }

    #[test]
    fn stopping_product_service_rejects_new_background_activity() {
        let service = MvProductService::default();
        service.begin_stopping();

        assert!(matches!(
            service.request_activity(
                CanonicalMvTarget::from_parts(Some("ice"), "sales", "mv_orders"),
                MvActivityOwner::ScheduledRefresh,
            ),
            Err(MvActivityGateError::Stopping)
        ));
    }

    #[test]
    fn create_runs_product_effects_in_required_order() {
        let service = MvProductService::default();
        let effects = CreateEffects::default();

        let result = service
            .create(operation(), create_command(), &effects, &effects)
            .expect("create succeeds");

        assert!(matches!(result, MvProductResult::Created(_)));
        assert_eq!(
            effects.events(),
            ["create", "inspect", "sync", "project", "register"]
        );
    }

    #[test]
    fn create_cleans_only_after_inspection_failure() {
        let service = MvProductService::default();
        let effects = CreateEffects {
            fail_inspection: true,
            ..Default::default()
        };

        let error = service
            .create(operation(), create_command(), &effects, &effects)
            .expect_err("inspection failure is returned after cleanup");

        assert_eq!(error.kind(), MvProductErrorKind::Unavailable);
        assert_eq!(effects.events(), ["create", "inspect", "drop"]);
    }

    #[test]
    fn create_marks_projection_failure_after_target_commit() {
        let service = MvProductService::default();
        let effects = CreateEffects {
            fail_projection: true,
            ..Default::default()
        };

        let error = service
            .create(operation(), create_command(), &effects, &effects)
            .expect_err("projection failed after target commit");

        assert_eq!(
            error.kind(),
            MvProductErrorKind::KnownCommittedFinalizeFailed
        );
        assert_eq!(effects.events(), ["create", "inspect", "sync", "project"]);
    }

    #[test]
    fn drop_orders_provider_projection_and_catalog_effects() {
        let service = MvProductService::default();
        let effects = CreateEffects::default();
        let target = MvTarget::from_parts(Some("iceberg"), "db", "mv");

        let result = service
            .drop(operation(), target, false, &effects, &effects, &effects)
            .expect("drop succeeds");

        assert!(matches!(result, MvProductResult::Dropped));
        assert_eq!(
            effects.events(),
            ["prepare_drop", "drop", "delete_projection", "unregister"]
        );
    }

    #[test]
    fn drop_if_exists_absence_emits_no_external_effect() {
        let service = MvProductService::default();
        let effects = CreateEffects {
            drop_absent: true,
            ..Default::default()
        };
        let target = MvTarget::from_parts(Some("iceberg"), "db", "missing_mv");

        let result = service
            .drop(operation(), target, true, &effects, &effects, &effects)
            .expect("missing IF EXISTS target is acknowledged");

        assert!(matches!(result, MvProductResult::Acknowledged));
        assert_eq!(effects.events(), ["prepare_drop"]);
    }
}
