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
    CreatedMvTarget, MvApplicationError, MvApplicationErrorKind, MvCreateProviderAdapter,
    MvCreateProviderError, MvCreateProviderErrorKind, MvCreateStatement, MvRequestContext,
    PreparedMvCreate, StagedMvTarget,
};
use novarocks_mv_application::ports::{
    MvCreateCatalogRegistrationPort, MvCreateProviderPort, MvProviderFailure, MvProviderFailureKind,
};
use novarocks_mv_application::product::{
    MvCommand, MvCreateCommand, MvCreatedTarget, MvOperationContext, MvProductError,
    MvProductErrorKind, MvProductResult, MvStagedTarget, MvTarget as ProductMvTarget,
};
use novarocks_mv_application::service::MvProductService;
use novarocks_sql::planning::mv::SqlMvTarget;
use uuid::Uuid;

pub(super) fn handle_create(
    product: &MvProductService,
    engine: &dyn MvCreateProviderAdapter,
    statement: &MvCreateStatement,
    context: MvRequestContext<'_>,
) -> Result<(), MvApplicationError> {
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
        MvProductResult::Created(_) => Ok(()),
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
    engine: &'a dyn MvCreateProviderAdapter,
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
            object_id: target.object_id.clone(),
        })
    }

    fn frontend_staged(
        &self,
        staged: &MvStagedTarget,
    ) -> Result<StagedMvTarget, MvProviderFailure> {
        self.require_target(&staged.target)?;
        Ok(StagedMvTarget {
            target: self.plan.target.clone(),
            staged_operation_id: staged.staged_operation_id,
        })
    }
}

impl MvCreateProviderPort for FrontendCreateAdapter<'_> {
    fn stage_target(
        &self,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Result<MvStagedTarget, MvProviderFailure> {
        let MvCommand::Create(create) = command else {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::InvalidRequest,
                "MV CREATE adapter received a non-CREATE product command",
            ));
        };
        self.require_target(&create.target)?;
        let staged = self
            .engine
            .stage_target(self.plan, operation.operation_id)
            .map_err(provider_failure)?;
        Ok(MvStagedTarget {
            target: create.target.clone(),
            staged_operation_id: staged.staged_operation_id,
        })
    }

    fn publish_staged_target(
        &self,
        _operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Result<MvCreatedTarget, MvProviderFailure> {
        self.require_target(&staged.target)?;
        let published = self
            .engine
            .publish_staged_target(self.plan, &self.frontend_staged(staged)?)
            .map_err(provider_failure)?;
        Ok(MvCreatedTarget {
            target: staged.target.clone(),
            object_id: published.object_id,
        })
    }

    fn abort_staged_target(
        &self,
        _operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Result<(), MvProviderFailure> {
        self.require_target(&staged.target)?;
        self.engine
            .abort_staged_target(self.plan, &self.frontend_staged(staged)?)
            .map_err(provider_failure)
    }

    fn install_created_projection(
        &self,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Result<(), MvProviderFailure> {
        let target = self.frontend_target(target)?;
        self.engine
            .install_created_projection(&target, operation.operation_id)
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

fn provider_failure(error: MvCreateProviderError) -> MvProviderFailure {
    let kind = match error.kind() {
        MvCreateProviderErrorKind::InvalidRequest | MvCreateProviderErrorKind::Analysis => {
            MvProviderFailureKind::InvalidRequest
        }
        MvCreateProviderErrorKind::KnownUncommitted => MvProviderFailureKind::KnownUncommitted,
        MvCreateProviderErrorKind::CommitUnknown => MvProviderFailureKind::CommitUnknown,
        MvCreateProviderErrorKind::KnownCommittedFinalizeFailed => {
            MvProviderFailureKind::KnownCommittedFinalizeFailed
        }
        MvCreateProviderErrorKind::TargetOperation
        | MvCreateProviderErrorKind::DescriptorSync
        | MvCreateProviderErrorKind::CatalogRegistration => MvProviderFailureKind::Unavailable,
    };
    MvProviderFailure::new(kind, error.to_string())
        .with_compile_control(error.compile_control_error())
}

fn engine_error(error: MvCreateProviderError) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
        .with_compile_control(error.compile_control_error())
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
        .with_compile_control(error.compile_control_error())
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn create_preanalysis_control_survives_provider_product_and_frontend_bridges() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let failure = MvCreateProviderError::from_compile(control.into());
            let direct = engine_error(failure.clone());
            assert_eq!(direct.compile_control_error(), Some(control));
            assert_eq!(direct.kind(), MvApplicationErrorKind::Engine);
            let product = provider_failure(failure).into_product_error();
            let terminal = product_error(product);
            assert_eq!(terminal.compile_control_error(), Some(control));
            assert_eq!(terminal.kind(), MvApplicationErrorKind::Engine);
        }
    }
}
