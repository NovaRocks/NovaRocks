// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Single lake-to-Accelerator convergence path.
//!
//! A CAS conflict intentionally discards the complete payload and asks the
//! caller to observe the lake again.  Reusing the old package after conflict
//! would allow a stale finalizer to overwrite a newer lake revision.

use std::sync::Arc;

use uuid::Uuid;

use crate::mv::domain::accelerator::projection_from_lake;
use crate::mv::domain::model::MvTarget;
use crate::mv::domain::repository::{
    MvRepository, MvRepositoryError, MvRepositoryErrorKind, ReplaceMvProjectionRequest,
};
use crate::mv::domain::storage_observation::MvLakePackageObservation;

pub(crate) struct MvAcceleratorProjector {
    repository: Arc<dyn MvRepository>,
}

impl MvAcceleratorProjector {
    pub(crate) fn new(repository: Arc<dyn MvRepository>) -> Self {
        Self { repository }
    }

    /// Converge one freshly observed package.  A conflict is returned to the
    /// outer observer loop; it must acquire a retained generation and rebuild
    /// a new payload before retrying.
    pub(crate) async fn project_once(
        &self,
        operation_id: Uuid,
        package: &MvLakePackageObservation,
    ) -> Result<(), MvRepositoryError> {
        project_observed_repository(self.repository.as_ref(), operation_id, package).await
    }
}

/// The only repository mutation entry for a lake observation. Startup rebuild
/// and explicit resync use this function rather than becoming independent
/// projection writers.
pub(crate) async fn project_observed_repository(
    repository: &dyn MvRepository,
    operation_id: Uuid,
    package: &MvLakePackageObservation,
) -> Result<(), MvRepositoryError> {
    let projection = projection_from_lake(package)
        .map_err(|error| MvRepositoryError::new(MvRepositoryErrorKind::Corruption, error))?;
    let target = MvTarget {
        catalog: Some(package.table.instance_id.as_str().to_string()),
        database: package.table.namespace.to_string(),
        name: package.table.table.to_string(),
    };
    let Some(current) = repository.find_by_target(&target).await? else {
        repository
            .create_projection(operation_id, projection)
            .await?;
        return Ok(());
    };
    if current.definition.source_revision == projection.source_revision {
        return Ok(());
    }
    repository
        .replace_projection(
            operation_id,
            ReplaceMvProjectionRequest {
                mv_id: current.definition.mv_id,
                expected_version: current.version,
                projection,
            },
        )
        .await?;
    Ok(())
}
