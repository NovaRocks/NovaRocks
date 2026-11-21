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

//! Dictionary cleanup for one authoritative expiration preparation round.

use std::{collections::BTreeSet, num::NonZeroU64};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorOperationControl,
    ConnectorOperationControlView, ConnectorRequestContext, ProviderBindingEpoch,
};

use super::RetentionPlan;
use crate::catalog_control::maintenance_error::invalid_metadata;
use crate::catalog_control::statistics::scoped_cpu::{
    IcebergMetadataCpuOriginal, run_provider_metadata,
};
use crate::iceberg::{Error, TableUpdate, spec::TableMetadataRef, table::Table};

pub(super) fn dictionary_backing_bytes(table: &Table) -> Result<u64, ConnectorError> {
    let metadata = table.metadata();
    // The retained-ID sets, per-list projection, and sorted update/frozen
    // request copies can coexist. They include only known dictionary IDs.
    (metadata.schemas_iter().len() as u64)
        .checked_add(metadata.partition_specs_iter().len() as u64)
        .and_then(|count| count.checked_mul(8 * std::mem::size_of::<i32>() as u64))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Work>() as u64))
        .ok_or_else(backing_overflow)
}

pub(super) async fn unused_dictionary_updates(
    table: &Table,
    retention: &RetentionPlan,
    context: &ConnectorRequestContext,
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    reserved: NonZeroU64,
) -> Result<Vec<TableUpdate>, ConnectorError> {
    let metadata = table.metadata();
    let mut schemas = BTreeSet::from([metadata.current_schema_id()]);
    let mut specs = BTreeSet::from([metadata.default_partition_spec_id()]);
    let spec_count = metadata.partition_specs_iter().len();
    for snapshot in metadata
        .snapshots()
        .filter(|snapshot| retention.retains_snapshot(snapshot.snapshot_id()))
    {
        context.check_active()?;
        schemas.extend(snapshot.schema_id());
        if specs.len() < spec_count {
            let path = snapshot.manifest_list();
            let reader = table
                .file_io()
                .new_input(path)
                .map_err(|error| read_error(error, path))?;
            let size = reader
                .metadata()
                .await
                .map_err(|error| read_error(error, path))?
                .size;
            let peak = reserved
                .get()
                .checked_add(size)
                .and_then(NonZeroU64::new)
                .ok_or_else(backing_overflow)?;
            context.reserve_owned_metadata(peak).await?;
            let bytes = reader
                .read()
                .await
                .map_err(|error| read_error(error, path))?;
            if bytes.len() as u64 != size {
                return Err(invalid_metadata(format!(
                    "Manifest list size changed: {path}"
                )));
            }
            let cpu = context.metadata_cpu().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Snapshot expiration requires its original manifest-list CPU capability",
                )
            })?;
            let source = IcebergMetadataCpuOriginal::retained_partition_specs(
                descriptor.clone(),
                incarnation,
                Work {
                    bytes,
                    metadata: table.metadata_ref(),
                    control: context.original_operation_control(),
                    result: None,
                },
            );
            run_provider_metadata(cpu, &source).await?;
            specs.extend(source.take_retained_partition_specs()?);
        }
    }
    let mut removed_schemas: Vec<_> = metadata
        .schemas_iter()
        .map(|schema| schema.schema_id())
        .filter(|id| !schemas.contains(id))
        .collect();
    removed_schemas.sort_unstable();
    let mut removed_specs: Vec<_> = metadata
        .partition_specs_iter()
        .map(|spec| spec.spec_id())
        .filter(|id| !specs.contains(id))
        .collect();
    removed_specs.sort_unstable();
    let mut updates = Vec::new();
    if !removed_specs.is_empty() {
        updates.push(TableUpdate::RemovePartitionSpecs {
            spec_ids: removed_specs,
        });
    }
    if !removed_schemas.is_empty() {
        updates.push(TableUpdate::RemoveSchemas {
            schema_ids: removed_schemas,
        });
    }
    Ok(updates)
}

fn backing_overflow() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "Snapshot dictionary cleanup backing overflow",
    )
}

fn read_error(error: Error, path: &str) -> ConnectorError {
    let failure = crate::commit::attempt::before_dispatch_failure(&error);
    match failure.connector_source() {
        Some(source) if source.kind() == ConnectorErrorKind::NotFound => {
            invalid_metadata(format!("Manifest file does not exist: {path}"))
        }
        Some(source) => source.clone(),
        None => super::iceberg_error(error),
    }
}

pub(in crate::catalog_control) struct Work {
    bytes: Bytes,
    metadata: TableMetadataRef,
    control: ConnectorOperationControlView,
    result: Option<Result<BTreeSet<i32>, ConnectorError>>,
}

impl Work {
    pub(in crate::catalog_control) fn run(&mut self) {
        assert!(
            self.result.is_none(),
            "one retained manifest-list CPU receipt"
        );
        self.result = Some(self.project());
    }

    fn project(&self) -> Result<BTreeSet<i32>, ConnectorError> {
        self.control.check_active()?;
        let manifest_list = crate::manifest::decode_manifest_list(&self.bytes, &self.metadata)
            .map_err(|error| {
                invalid_metadata(format!(
                    "Cannot decode retained snapshot manifest list: {error}"
                ))
            })?;
        let mut specs = BTreeSet::new();
        for manifest in manifest_list.entries() {
            self.control.check_active()?;
            let id = manifest.partition_spec_id;
            if self.metadata.partition_spec_by_id(id).is_none() {
                return Err(invalid_metadata(format!(
                    "Retained manifest references missing partition spec {id}"
                )));
            }
            specs.insert(id);
        }
        Ok(specs)
    }

    pub(in crate::catalog_control) fn take_output(
        &mut self,
    ) -> Result<BTreeSet<i32>, ConnectorError> {
        self.result.take().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Original retained manifest-list CPU result is absent",
            )
        })?
    }
}
