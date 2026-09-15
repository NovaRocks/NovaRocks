// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements.  See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.  The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Pure lake-to-Accelerator projection construction.
//!
//! The caller owns observation and CAS retry.  This module deliberately has no
//! StateStore access, so an obsolete observation cannot become durable after a
//! caller has observed a conflict.

use crate::mv::domain::lake_rebuild::dependency_requests_from_descriptor;
use crate::mv::domain::lake_rebuild::{RebuiltMvDefinition, rebuild_mv_definition_from_lake};
use crate::mv::domain::storage_observation::MvLakePackageObservation;
use novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision;
use novarocks_mv_application::repository::MvProjectionRequest;

pub(crate) fn projection_from_lake(
    package: &MvLakePackageObservation,
) -> Result<MvProjectionRequest, String> {
    let RebuiltMvDefinition {
        create_request,
        refresh,
        publication,
    } = rebuild_mv_definition_from_lake(package)?;
    let dependencies = dependency_requests_from_descriptor(
        &package.descriptor.base_dependencies,
        create_request.created_at_ms,
    )?;
    let descriptor_content_hash = package
        .descriptor
        .content_hash()
        .map_err(|error| format!("hash MV lake descriptor: {error}"))?;
    Ok(MvProjectionRequest {
        definition: create_request,
        refresh,
        publication,
        source_revision: MvAcceleratorSourceRevision {
            target_object_id: package.target_object_id.clone(),
            descriptor_content_hash,
            current_target_snapshot_id: package
                .current_target_snapshot
                .map(|snapshot| snapshot.snapshot_id),
        },
        dependencies,
    })
}
