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

//! The Trino-aligned typed Iceberg read control model.
//!
//! This module owns the concrete Iceberg types behind the generic read stack
//! in `novarocks-spi`: the worker-visible table handle, the field-ID column
//! handle, the byte-range split with its full delete closure, and the lazy
//! split source that cuts one pinned snapshot's files into splits.
//!
//! Three rules shape everything here:
//!
//! * every fact is frozen at planning time -- a worker never re-resolves the
//!   catalog, re-reads a manifest, or picks a later snapshot;
//! * every identity is an Iceberg field ID -- never an ordinal, a name, or a
//!   digest of a payload;
//! * anything this stack cannot prove fails closed -- there is no fallback
//!   format, no inferred default, and no partially supported read.

pub mod change_window;
pub mod change_window_page_source;
pub mod codec;
pub mod column_handle;
pub mod delete_manager;
pub mod merge;
pub mod page_source;
pub mod page_source_provider;
mod preparation;
pub mod rewrite_position_page_source;
pub mod runtime;
pub mod schema_binding;
pub mod split;
pub mod split_source;
pub mod system_page_source;
pub mod system_table;
pub mod table_execute;
pub mod table_handle;
#[cfg(test)]
mod test_spawners;
#[cfg(test)]
mod test_streams;

pub use change_window::{
    ICEBERG_CHANGE_OP_COLUMN, ICEBERG_CHANGE_OP_FIELD_ID, IcebergAddedRows, IcebergChangeSide,
    IcebergChangeSplit, IcebergChangeWindowHandle, IcebergChangeWindowHandleParams,
    IcebergChangeWindowPlan, IcebergChangeWindowPlanOutcome, IcebergDeletedDataFileRows,
    IcebergEndpointVisibility, IcebergEqualityDeletedRows, IcebergPositionDeletedRows,
    MAX_RESTRICTED_ROW_IDS, TABLE_CHANGES_METADATA_COLUMNS, TableChangesChangeType,
    TableChangesFileChange, TableChangesFunctionHandle, TableChangesFunctionHandleParams,
    TableChangesSplit, TableChangesSplitParams, change_op_column_handle,
};
pub use change_window_page_source::{
    IcebergChangeWindowPageSourceRequest, IcebergChangeWindowPageStream,
    create_iceberg_change_window_page_stream,
};
pub use codec::IcebergConnectorReadWireAdapter;
pub use column_handle::{
    ColumnIdentity, ColumnIdentityCategory, IcebergColumnHandle, IcebergColumnHandleParams,
    decode_tuple_domain, encode_tuple_domain,
};
pub use delete_manager::{
    DeleteEvaluationMode, DeleteManager, RemovedRowSelection, SplitDeleteFilter,
};
pub use merge::{
    IcebergInsertTableHandle, IcebergInsertTableHandleParams, IcebergMergeSourcePlan,
    IcebergMergeSourcePlanParams, IcebergMergeTableHandle,
};
pub use page_source::{
    DynamicFilterCheckpoint, DynamicFilterObservation, DynamicFilterVerdict,
    IcebergPageSourceRequest, IcebergParquetPageSource, IcebergPartitionOnlyPageSource,
    IcebergReadRelation, ParquetFooterCache,
};
pub use page_source_provider::{IcebergPageSourceProvider, IcebergPageSourceProviderOptions};
pub use rewrite_position_page_source::{
    IcebergRewritePositionDeleteFilesPageSourceRequest,
    create_iceberg_rewrite_position_delete_files_page_stream,
};
pub use runtime::{IcebergExecutionReadRuntime, IcebergReadSplit, IcebergRuntimeRelation};
pub use schema_binding::{
    ALWAYS_BOUND_METADATA_COLUMNS, FileFieldIdCoverage, ICEBERG_METADATA_FIELD_ID_IS_DELETED,
    ICEBERG_METADATA_FIELD_ID_PATH, ICEBERG_METADATA_FIELD_ID_ROW_POSITION, IcebergBoundColumn,
    IcebergColumnSource, IcebergMetadataColumn, IcebergPhysicalAdaptation, IcebergSchemaBinding,
    IcebergSchemaBindingRequest, IcebergSplitFacts, IcebergTypePromotion,
    ROW_LINEAGE_METADATA_COLUMNS, bind_scan_columns, file_field_id_coverage, physical_adaptation,
};
pub use split::{
    DEFAULT_MINIMUM_ASSIGNED_SPLIT_WEIGHT, IcebergDeleteFile, IcebergDeleteFileContent,
    IcebergDeleteFileParams, IcebergFileFormat, IcebergSplit, IcebergSplitParams,
    IcebergSplitWeightParameters, ParquetFileDecryptionData, iceberg_split_weight,
};
pub use split_source::{
    DEFAULT_TARGET_SPLIT_SIZE_BYTES, IcebergChangeWindowEndpoints, IcebergChangeWindowSplitSource,
    IcebergDeleteFileFacts, IcebergPlannedDataFile, IcebergSplitSource, IcebergSplitSourceOptions,
    READ_SPLIT_TARGET_SIZE_PROPERTY, plan_change_window_splits,
};
pub use system_page_source::{
    IcebergSystemPageSource, IcebergSystemTableProvider, bounds_row_type, derived_row_type_json,
    files_relation_schema_json, partition_row_type, partitions_view_schema,
    project_system_relation_columns, system_relation_columns, system_relation_schema,
};
pub use system_table::{
    FilesTableSplit, FilesTableSplitParams, FilesTableSplitSource, FilesTableSplitSourceParams,
    IcebergSystemTableExecution, IcebergSystemTableReference, IcebergSystemTableReferenceParams,
    IcebergSystemTableType, MAX_FILES_TABLE_MANIFESTS, TrinoManifestContent, TrinoManifestFile,
    TrinoManifestFileParams,
};
pub use table_execute::{
    IcebergOptimizeHandle, IcebergProcedureExecution, IcebergProcedureId,
    IcebergRewriteArtifactContentId, IcebergRewritePositionDeleteFilesHandle,
    IcebergRewritePositionDeleteFilesSplit, IcebergRewritePositionDeleteFilesSplitParams,
    IcebergTableExecuteHandle, IcebergTableExecuteHandleParams, IcebergTableExecuteProcedureHandle,
    REWRITE_POSITION_DELETE_OUTPUT_COLUMNS,
};
pub use table_handle::{
    HiveTransactionHandle, IcebergPinnedDataFileSet, IcebergTableHandle, IcebergTableHandleParams,
    identity_partition_source_field_ids,
};
