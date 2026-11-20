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

//! The original ordinary CREATE's two closed pure metadata CPU phases.

use super::*;
use crate::catalog::publication_dispatch::PreparationResult;
use crate::catalog_control::statistics::scoped_cpu::{
    IcebergMetadataCpuOriginal, run_provider_metadata,
};
use std::sync::Weak;

pub(in crate::catalog_control) enum Stage {
    Preflight,
    Freeze(Box<CreatePreflightFacts>),
}

pub(in crate::catalog_control) enum Output {
    Preflight(PreparationResult<CreatePreflightFacts>),
    Freeze(PreparationResult<PreparedCreate>),
}

pub(in crate::catalog_control) struct Work {
    original: Weak<async_existing::Original>,
    stage: Stage,
    output: Option<Result<Output, ConnectorError>>,
}
impl Work {
    fn new(original: Weak<async_existing::Original>, stage: Stage) -> Self {
        Self {
            original,
            stage,
            output: None,
        }
    }
    pub(in crate::catalog_control) fn run(&mut self) {
        assert!(self.output.is_none(), "one actual CREATE CPU result");
        let Some(original) = self.original.upgrade() else {
            self.output = Some(Err(internal("CREATE CPU original Source is unavailable")));
            return;
        };
        let ConnectorCatalogMutationOperation::CreateTable {
            table,
            columns,
            comment,
            properties,
            policy,
        } = &original.request.operation
        else {
            self.output = Some(Err(internal("CREATE CPU original operation differs")));
            return;
        };
        let value = match &self.stage {
            Stage::Preflight => Output::Preflight(prepare_create_preflight(
                &original.provider,
                &original.request,
                table,
                columns,
                comment.as_deref(),
                properties,
            )),
            Stage::Freeze(facts) => Output::Freeze(freeze_ordinary_create(
                &original.provider,
                &original.request,
                table,
                columns,
                *policy,
                facts,
            )),
        };
        // The same metadata Source holds input/output across worker unwind and
        // borrowed waits. No CPU port or request context is held by this Work.
        self.output = Some(Ok(value));
    }
    pub(in crate::catalog_control) fn take_output(&mut self) -> Result<Output, ConnectorError> {
        self.output
            .take()
            .ok_or_else(|| internal("CREATE CPU has no actual result"))?
    }
}

async fn run(
    original: &Arc<async_existing::Original>,
    stage: Stage,
) -> Result<Output, ConnectorError> {
    let cpu = original.request.context.metadata_cpu().ok_or_else(|| {
        unsupported("asynchronous CREATE requires its original metadata CPU capability")
    })?;
    let source = IcebergMetadataCpuOriginal::catalog_create(
        original.provider.descriptor().clone(),
        original.provider.incarnation(),
        Work::new(Arc::downgrade(original), stage),
    );
    run_provider_metadata(cpu, &source).await?;
    source.take_catalog_create_output()
}

pub(super) async fn preflight(
    original: &Arc<async_existing::Original>,
) -> Result<CreatePreflightFacts, CreatePreparationFailure> {
    match run(original, Stage::Preflight)
        .await
        .map_err(CreatePreparationFailure::Source)?
    {
        Output::Preflight(result) => result.map_err(CreatePreparationFailure::Provider),
        _ => Err(CreatePreparationFailure::Source(internal(
            "CREATE CPU preflight returned another phase",
        ))),
    }
}

pub(super) async fn freeze(
    original: &Arc<async_existing::Original>,
    facts: CreatePreflightFacts,
) -> Result<PreparedCreate, CreatePreparationFailure> {
    match run(original, Stage::Freeze(Box::new(facts)))
        .await
        .map_err(CreatePreparationFailure::Source)?
    {
        Output::Freeze(result) => result.map_err(CreatePreparationFailure::Provider),
        _ => Err(CreatePreparationFailure::Source(internal(
            "CREATE CPU freeze returned another phase",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_original_cpu_source_is_internal_without_reconstruction() {
        let mut work = Work::new(Weak::new(), Stage::Preflight);
        work.run();
        assert!(
            matches!(work.take_output(), Err(error) if error.kind() == ConnectorErrorKind::Internal)
        );
    }
}
