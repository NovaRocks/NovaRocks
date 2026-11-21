// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Validate the exact base of one registration round before writing manifests.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorError;
use uuid::Uuid;

use crate::catalog_control::add_files::{
    MappingDependency, commit_error, duplicate_path, unsupported_scan_carrier,
};
use crate::commit::RegisteredPaths;
use crate::commit::model::OperationIntent;
use crate::commit::staging::{PreparedChange, Preparer, StagedView};
use crate::iceberg::TableUpdate;
use crate::iceberg::spec::{TableMetadata, TableProperties};
use crate::schema_mapping::{encode_name_mapping, name_mapping_from_schema, parse_name_mapping};
use crate::typed_read::table_handle::validate_name_mapping_carrier;

use super::format_error;

pub(super) struct RegistrationChecks<'a> {
    pub(super) table_uuid: Uuid,
    pub(super) partition_spec_id: i32,
    pub(super) paths: &'a RegisteredPaths,
    pub(super) mapping: Option<&'a MappingDependency>,
    /// The initial execution read verified this value, including an empty ref.
    pub(super) last_verified_main: Mutex<Option<i64>>,
}

#[async_trait]
impl Preparer for RegistrationChecks<'_> {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> crate::iceberg::Result<PreparedChange> {
        let base = view.metadata();
        let artifacts = view.artifacts();
        artifacts.check_active()?;
        if base.uuid() != self.table_uuid {
            return Err(format_error(commit_error("Target table was replaced")));
        }
        if base.partition_spec_by_id(self.partition_spec_id).is_none() {
            return Err(format_error(commit_error(
                "Registered partition spec disappeared",
            )));
        }
        let main = base
            .snapshot_for_ref("main")
            .map(|snapshot| snapshot.snapshot_id());
        let cached = *self
            .last_verified_main
            .lock()
            .map_err(|_| format_error(commit_error("Registration snapshot check lock failed")))?;
        if cached != main {
            if !self.paths.is_empty()
                && let Some(sources) = view.target_sources().await?
                && let Some(index) = sources
                    .registered_path_match(
                        |path| self.paths.index(path),
                        artifacts.file_io(),
                        || artifacts.check_active(),
                    )
                    .await?
            {
                return Err(format_error(duplicate_path(
                    self.paths.get(index).expect("matched registration path"),
                )));
            }
            artifacts.check_active()?;
            *self.last_verified_main.lock().map_err(|_| {
                format_error(commit_error("Registration snapshot check lock failed"))
            })? = main;
        }
        let update = self
            .mapping
            .map(|mapping| mapping.update(base))
            .transpose()
            .map_err(format_error)?
            .flatten();
        Ok(PreparedChange {
            updates: update.into_iter().collect(),
            requirements: Vec::new(),
        })
    }
}

impl MappingDependency {
    fn update(&self, base: &TableMetadata) -> Result<Option<TableUpdate>, ConnectorError> {
        let property = TableProperties::PROPERTY_DEFAULT_NAME_MAPPING;
        let raw = base.properties().get(property);
        let mapping = match raw {
            Some(raw) => {
                validate_name_mapping_carrier(raw).map_err(mapping_carrier_error)?;
                parse_name_mapping(raw).map_err(commit_error)?
            }
            None if self.originally_present => {
                return Err(commit_error(
                    "schema.name-mapping.default was removed after file binding",
                ));
            }
            None => name_mapping_from_schema(base.current_schema()),
        };
        if !self
            .used
            .matches(&mapping)
            .map_err(|error| commit_error(error.message()))?
        {
            return Err(commit_error(
                "schema.name-mapping.default changed a registered field binding",
            ));
        }
        if raw.is_some() {
            return Ok(None);
        }
        let encoded = encode_name_mapping(&mapping).map_err(commit_error)?;
        validate_name_mapping_carrier(&encoded).map_err(mapping_carrier_error)?;
        Ok(Some(TableUpdate::SetProperties {
            updates: HashMap::from([(property.to_owned(), encoded)]),
        }))
    }
}

fn mapping_carrier_error(error: ConnectorError) -> ConnectorError {
    unsupported_scan_carrier(error.message())
}
