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

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use novarocks_connector_iceberg::iceberg::spec::{Struct, TableMetadata};
use parquet::basic::Compression;

use crate::connector::iceberg::commit::EqualityDeleteColumn;
use crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorBinding;
use crate::fs::object_store_credentials::ObjectStoreCredentials;
use novarocks_connector_iceberg::delete_file::IcebergFileFormat;
use novarocks_execution::exec::expr::{ExprArena, ExprId};
use novarocks_fs::ObjectStoreConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergSinkMode {
    Data,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

#[derive(Clone, Debug)]
pub struct PositionDeleteDataFilePartition {
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_values: Struct,
}

#[derive(Clone, Debug)]
pub struct DeferredPositionDeleteDataFilePartitionIndex {
    pub metadata: TableMetadata,
    pub target_snapshot_id: Option<i64>,
    pub table_location: String,
    pub object_store_s3: Option<IcebergSinkObjectStoreConfig>,
}

impl DeferredPositionDeleteDataFilePartitionIndex {
    pub fn new(
        metadata: TableMetadata,
        target_snapshot_id: Option<i64>,
        table_location: String,
        object_store_s3: Option<IcebergSinkObjectStoreConfig>,
    ) -> Self {
        Self {
            metadata,
            target_snapshot_id,
            table_location,
            object_store_s3,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergSinkObjectStoreConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub session_token: Option<String>,
    pub region: Option<String>,
    pub enable_path_style_access: Option<bool>,
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

impl IcebergSinkObjectStoreConfig {
    pub fn from_credentials(bucket: String, credentials: ObjectStoreCredentials) -> Self {
        Self {
            endpoint: credentials.endpoint,
            bucket,
            access_key_id: credentials.access_key_id,
            access_key_secret: credentials.access_key_secret,
            session_token: credentials.session_token,
            region: credentials.region,
            enable_path_style_access: credentials.enable_path_style_access,
            retry_max_times: credentials.retry_max_times,
            retry_min_delay_ms: credentials.retry_min_delay_ms,
            retry_max_delay_ms: credentials.retry_max_delay_ms,
            timeout_ms: credentials.timeout_ms,
            io_timeout_ms: credentials.io_timeout_ms,
        }
    }

    pub fn to_object_store_config(&self) -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
            enable_path_style_access: self.enable_path_style_access,
            region: self.region.clone(),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        }
    }
}

#[derive(Clone)]
pub struct IcebergSinkPlan {
    pub mode: IcebergSinkMode,
    pub table_location: String,
    pub data_location: String,
    pub target_partition_spec_id: i32,
    pub target_table_metadata: Option<TableMetadata>,
    pub target_snapshot_id: Option<i64>,
    pub position_delete_data_file_partitions: HashMap<String, PositionDeleteDataFilePartition>,
    pub position_delete_data_file_partition_index_input:
        Option<DeferredPositionDeleteDataFilePartitionIndex>,
    pub object_store_s3: Option<IcebergSinkObjectStoreConfig>,
    pub file_format: IcebergFileFormat,
    pub report_file_format: String,
    pub compression: Compression,
    pub output_schema: SchemaRef,
    pub target_schema: SchemaRef,
    pub equality_delete_columns: Vec<EqualityDeleteColumn>,
    pub row_lineage_data: bool,
    pub output_exprs: Vec<ExprId>,
    pub partition_exprs: Vec<ExprId>,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub transform_exprs: Vec<String>,
    pub position_delete_binding: Option<PositionDeleteDescriptorBinding>,
}

pub struct IcebergSinkFactoryInput {
    pub name: String,
    pub arena: ExprArena,
    pub plan: IcebergSinkPlan,
}
