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

//! Frontend SQL/provider adapter for product-owned MV CREATE.

use crate::mv::domain::application::{
    CreatedMvTarget, MvApplicationError, MvApplicationErrorKind, MvCreateStatement, MvEngine,
    MvEngineError, MvEngineErrorKind, MvRequestContext, MvStatementResult, PreparedMvCreate,
};
use novarocks_mv_application::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvProviderFailure, MvProviderFailureKind,
};
use novarocks_mv_application::product::{
    MvCommand, MvCreateCommand, MvCreatedTarget, MvOperationContext, MvPreparedDefinition,
    MvProductError, MvProductErrorKind, MvProductResult, MvTarget as ProductMvTarget,
};
use novarocks_mv_application::service::MvProductService;
use novarocks_sql::planning::mv::SqlMvTarget;
use uuid::Uuid;

pub(super) fn handle_create(
    product: &MvProductService,
    engine: &dyn MvEngine,
    statement: &MvCreateStatement,
    context: MvRequestContext<'_>,
) -> Result<MvStatementResult, MvApplicationError> {
    let plan = engine
        .prepare_create(crate::mv::domain::application::PrepareMvCreateRequest {
            statement,
            context,
        })
        .map_err(engine_error)?;
    let command = MvCreateCommand {
        target: product_target(&plan.target)?,
        if_not_exists: statement.if_not_exists,
        definition: plan.projection_seed.definition.clone(),
        refresh: plan.projection_seed.refresh.clone(),
        dependencies: plan.projection_seed.dependencies.clone(),
    };
    let adapter = FrontendCreateAdapter {
        engine,
        plan: &plan,
    };
    match product
        .create(
            MvOperationContext {
                operation_id: Uuid::now_v7(),
            },
            command,
            &adapter,
            &adapter,
        )
        .map_err(product_error)?
    {
        MvProductResult::Created(_) => Ok(MvStatementResult::Ok),
        MvProductResult::Acknowledged | MvProductResult::Dropped | MvProductResult::Listed(_) => {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Engine,
                "MV CREATE product returned a non-CREATE result",
            ))
        }
    }
}

/// This is intentionally per operation: it captures a parser-admitted,
/// provider-specific preparation, but exposes only the product's narrow ports.
struct FrontendCreateAdapter<'a> {
    engine: &'a dyn MvEngine,
    plan: &'a PreparedMvCreate,
}

impl FrontendCreateAdapter<'_> {
    fn expected_target(&self) -> Result<ProductMvTarget, MvProviderFailure> {
        product_target(&self.plan.target).map_err(|error| {
            MvProviderFailure::new(MvProviderFailureKind::InvalidRequest, error.to_string())
        })
    }

    fn require_target(&self, target: &ProductMvTarget) -> Result<(), MvProviderFailure> {
        if self.expected_target()? == *target {
            Ok(())
        } else {
            Err(MvProviderFailure::new(
                MvProviderFailureKind::InvalidRequest,
                "MV product target differs from the frontend-prepared CREATE target",
            ))
        }
    }

    fn frontend_target(
        &self,
        target: &MvCreatedTarget,
    ) -> Result<CreatedMvTarget, MvProviderFailure> {
        self.require_target(&target.target)?;
        Ok(CreatedMvTarget {
            target: self.plan.target.clone(),
            table_uuid: target.table_uuid.clone(),
        })
    }
}

impl MvCreateProviderPort for FrontendCreateAdapter<'_> {
    fn create_target(
        &self,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Result<MvCreatedTarget, MvProviderFailure> {
        let MvCommand::Create(create) = command else {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::InvalidRequest,
                "MV CREATE adapter received a non-CREATE product command",
            ));
        };
        self.require_target(&create.target)?;
        let created = self
            .engine
            .create_target(self.plan, operation.operation_id)
            .map_err(provider_failure)?;
        Ok(MvCreatedTarget {
            target: create.target.clone(),
            table_uuid: created.table_uuid,
        })
    }

    fn inspect_created_target(
        &self,
        _operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<MvPreparedDefinition, MvProviderFailure> {
        let target = self.frontend_target(target)?;
        let definition = self
            .engine
            .inspect_created_target(self.plan, &target)
            .map_err(provider_failure)?;
        Ok(MvPreparedDefinition {
            descriptor: definition.descriptor,
        })
    }

    fn sync_target_descriptor(
        &self,
        _operation: MvOperationContext,
        target: &MvCreatedTarget,
        definition: &MvPreparedDefinition,
    ) -> Result<(), MvProviderFailure> {
        let target = self.frontend_target(target)?;
        self.engine
            .sync_target_descriptor(&target, &definition.descriptor)
            .map_err(provider_failure)
    }

    fn project_created_target(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        let target = self.frontend_target(target)?;
        self.engine
            .project_created_target(&target, operation.operation_id)
            .map_err(provider_failure)
    }

    fn cleanup_created_target(
        &self,
        _operation: MvOperationContext,
        target: &ProductMvTarget,
    ) -> Result<(), MvProviderFailure> {
        self.require_target(target)?;
        self.engine
            .drop_created_target(&CreatedMvTarget {
                target: self.plan.target.clone(),
                table_uuid: String::new(),
            })
            .map_err(provider_failure)
    }
}

impl MvCreateCatalogRegistrationPort for FrontendCreateAdapter<'_> {
    fn register_target(
        &self,
        _operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        let target = self.frontend_target(target)?;
        self.engine
            .register_target(&target)
            .map_err(provider_failure)
    }
}

fn product_target(target: &SqlMvTarget) -> Result<ProductMvTarget, MvApplicationError> {
    ProductMvTarget::try_new(
        target.catalog.clone(),
        target.database.clone(),
        target.name.clone(),
    )
    .map_err(|error| {
        MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, error.to_string())
    })
}

fn provider_failure(error: MvEngineError) -> MvProviderFailure {
    let kind = match error.kind() {
        MvEngineErrorKind::InvalidRequest | MvEngineErrorKind::Analysis => {
            MvProviderFailureKind::InvalidRequest
        }
        MvEngineErrorKind::TargetOperation
        | MvEngineErrorKind::DescriptorSync
        | MvEngineErrorKind::CatalogRegistration => MvProviderFailureKind::Unavailable,
    };
    MvProviderFailure::new(kind, error.to_string())
}

fn engine_error(error: MvEngineError) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
}

fn product_error(error: MvProductError) -> MvApplicationError {
    let kind = match error.kind() {
        MvProductErrorKind::KnownCommittedFinalizeFailed => {
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        }
        MvProductErrorKind::InvalidRequest
        | MvProductErrorKind::Conflict
        | MvProductErrorKind::Unavailable
        | MvProductErrorKind::ProviderKnownUncommitted
        | MvProductErrorKind::CommitUnknown
        | MvProductErrorKind::TargetReplaced
        | MvProductErrorKind::Corruption
        | MvProductErrorKind::ShutdownCancelled => MvApplicationErrorKind::Engine,
    };
    MvApplicationError::new(kind, error.to_string())
}
