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

use crate::mv::domain::application::{
    MvApplicationError, MvApplicationErrorKind, MvCreateStatement, MvEngine, MvEngineError,
    MvRequestContext, MvStatementResult, PrepareMvCreateRequest,
};
use crate::mv::domain::repository::{
    MV_REPOSITORY_UNAVAILABLE_MESSAGE, MvRepository, MvRepositoryError, MvRepositoryErrorKind,
};
use uuid::Uuid;

pub(super) fn handle_create(
    repository: &dyn MvRepository,
    engine: &dyn MvEngine,
    statement: &MvCreateStatement,
    context: MvRequestContext<'_>,
) -> Result<MvStatementResult, MvApplicationError> {
    if !repository.availability().is_available() {
        return Err(MvApplicationError::new(
            MvApplicationErrorKind::Unavailable,
            MV_REPOSITORY_UNAVAILABLE_MESSAGE,
        ));
    }

    let plan = engine
        .prepare_create(PrepareMvCreateRequest { statement, context }, repository)
        .map_err(engine_error)?;
    let operation_id = Uuid::now_v7();
    let target = engine
        .create_target(&plan, operation_id)
        .map_err(engine_error)?;
    let definition = match engine.inspect_created_target(&plan, &target) {
        Ok(definition) => definition,
        Err(error) => {
            return Err(cleanup_known_uncommitted(
                engine,
                &target,
                engine_error(error),
            ));
        }
    };
    let definition = match repository.create(operation_id, definition.repository_request) {
        Ok(definition) => definition,
        Err(error) if error.kind() == MvRepositoryErrorKind::CommitUnknown => {
            return Err(repository_error(error));
        }
        Err(error) => {
            return Err(cleanup_known_uncommitted(
                engine,
                &target,
                repository_error(error),
            ));
        }
    };

    engine
        .sync_target_descriptor(&target, &definition)
        .map_err(known_committed_finalize_error)?;
    engine
        .register_target(&target)
        .map_err(known_committed_finalize_error)?;
    Ok(MvStatementResult::Ok)
}

fn engine_error(error: MvEngineError) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
}

fn repository_error(error: MvRepositoryError) -> MvApplicationError {
    let kind = match error.kind() {
        MvRepositoryErrorKind::Unavailable => MvApplicationErrorKind::Unavailable,
        MvRepositoryErrorKind::CommitUnknown => MvApplicationErrorKind::CommitUnknown,
        MvRepositoryErrorKind::KnownCommittedFinalizeFailed => {
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        }
        _ => MvApplicationErrorKind::Repository,
    };
    MvApplicationError::new(kind, error.to_string())
}

fn known_committed_finalize_error(error: MvEngineError) -> MvApplicationError {
    MvApplicationError::new(
        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
        error.to_string(),
    )
}

fn cleanup_known_uncommitted(
    engine: &dyn MvEngine,
    target: &crate::mv::domain::application::CreatedMvTarget,
    primary: MvApplicationError,
) -> MvApplicationError {
    match engine.drop_created_target(target) {
        Ok(()) => primary,
        Err(cleanup) => MvApplicationError::new(
            primary.kind(),
            format!("{}; target cleanup failed: {cleanup}", primary.message()),
        ),
    }
}
