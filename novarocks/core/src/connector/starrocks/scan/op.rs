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
use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorCancellation,
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding, ConnectorExecutionBindingKey,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorOpenReaderRequest,
    ConnectorProviderId, ConnectorReadExecution, ConnectorRequestContext, ConnectorSplit,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
use serde_json::Value;

use crate::common::types::UniqueId;
use crate::connector::MinMaxPredicate;
use crate::connector::runtime::ConnectorReadScanSource;
use crate::connector::starrocks::fe_v2_meta::{
    LakeScanTabletRef, LakeTableIdentity, lake_scan_execution_properties,
};
use crate::connector::starrocks::fs_access::{
    path_requires_object_store_profile, resolve_with_profile,
};
use crate::connector::starrocks::object_store_profile::ObjectStoreProfile;
use crate::connector::starrocks::schema::StarRocksTabletSchema;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::ScanSource;
use crate::novarocks_logging::{info, warn};
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::{QueryOptions, query_expire_durations};
use crate::runtime::starlet_shard_registry;

use super::reader::StarRocksNativeReader;

pub use crate::exec::dict_encode::QueryGlobalDictEncodeMap;

#[derive(Clone, Debug)]
pub struct StarRocksScanRange {
    pub tablet_id: i64,
    pub partition_id: Option<i64>,
    pub version: Option<i64>,
}

impl StarRocksScanRange {
    pub fn new(tablet_id: i64, partition_id: i64, version: i64) -> Self {
        Self {
            tablet_id,
            partition_id: Some(partition_id),
            version: Some(version),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StarRocksSchemaColumnHint {
    pub name: String,
    pub unique_id: i32,
    pub default_value: Option<String>,
}

impl StarRocksSchemaColumnHint {
    pub fn new(name: String, unique_id: i32, default_value: Option<String>) -> Self {
        Self {
            name,
            unique_id,
            default_value,
        }
    }
}

#[derive(Clone)]
pub struct LakeScanSchemaMeta {
    pub db_id: i64,
    pub table_id: i64,
    pub schema_id: i64,
    pub fe_addr: Option<RuntimeEndpoint>,
    pub query_id: Option<UniqueId>,
    pub native_tablet_schema: Option<StarRocksTabletSchema>,
    pub native_column_hints: Option<Vec<StarRocksSchemaColumnHint>>,
    pub table_schema_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::TableSchemaProvider>>,
    pub storage_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StorageMetadataProvider>>,
}

impl std::fmt::Debug for LakeScanSchemaMeta {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LakeScanSchemaMeta")
            .field("db_id", &self.db_id)
            .field("table_id", &self.table_id)
            .field("schema_id", &self.schema_id)
            .field("fe_addr", &self.fe_addr)
            .field("query_id", &self.query_id)
            .field("native_tablet_schema", &self.native_tablet_schema)
            .field("native_column_hints", &self.native_column_hints)
            .field(
                "has_table_schema_provider",
                &self.table_schema_provider.is_some(),
            )
            .field(
                "has_storage_metadata_provider",
                &self.storage_metadata_provider.is_some(),
            )
            .finish()
    }
}

#[derive(Clone)]
pub struct DeferredLakeScanResolution {
    pub(crate) query_id: Option<crate::runtime::query_context::QueryId>,
    pub(crate) table: LakeTableIdentity,
    pub(crate) tablets: Vec<LakeScanTabletRef>,
    pub(crate) starlet_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StarletMetadataProvider>>,
}

impl std::fmt::Debug for DeferredLakeScanResolution {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeferredLakeScanResolution")
            .field("query_id", &self.query_id)
            .field("table", &self.table)
            .field("tablets", &self.tablets)
            .field(
                "has_starlet_metadata_provider",
                &self.starlet_metadata_provider.is_some(),
            )
            .finish()
    }
}

impl DeferredLakeScanResolution {
    pub fn new(
        query_id: Option<crate::runtime::query_context::QueryId>,
        table: LakeTableIdentity,
        tablets: Vec<LakeScanTabletRef>,
        starlet_metadata_provider: Option<
            Arc<dyn crate::connector::starrocks::ports::StarletMetadataProvider>,
        >,
    ) -> Self {
        Self {
            query_id,
            table,
            tablets,
            starlet_metadata_provider,
        }
    }
}

impl LakeScanSchemaMeta {
    pub fn with_embedded_schema(
        db_id: i64,
        table_id: i64,
        schema_id: i64,
        query_id: Option<crate::runtime::query_context::QueryId>,
        tablet_schema: StarRocksTabletSchema,
        column_hints: Vec<StarRocksSchemaColumnHint>,
    ) -> Self {
        Self {
            db_id,
            table_id,
            schema_id,
            fe_addr: None,
            query_id: query_id.map(|query_id| UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
            native_tablet_schema: Some(tablet_schema),
            native_column_hints: Some(column_hints),
            table_schema_provider: None,
            storage_metadata_provider: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StarRocksScanConfig {
    pub db_name: Option<String>,
    pub table_name: Option<String>,
    pub properties: BTreeMap<String, String>,
    pub ranges: Vec<StarRocksScanRange>,
    pub has_more: bool,
    pub required_chunk_schema: ChunkSchemaRef,
    pub output_chunk_schema: ChunkSchemaRef,
    pub query_global_dicts: QueryGlobalDictEncodeMap,
    pub limit: Option<usize>,
    pub batch_size: Option<i32>,
    pub query_timeout: Option<i32>,
    pub mem_limit: Option<i64>,
    pub profile_label: Option<String>,
    pub min_max_predicates: Vec<MinMaxPredicate>,
    pub lake_schema_meta: Option<LakeScanSchemaMeta>,
    pub deferred_lake_resolution: Option<DeferredLakeScanResolution>,
    /// Maps TopN runtime filter_id → scan column name.
    /// Populated during lowering so that execute_iter() can convert
    /// `RuntimeMinMaxFilter` instances into `MinMaxPredicate` values
    /// for storage-level segment pruning.
    pub topn_filter_column_map: HashMap<i32, String>,
}

const STARROCKS_SPI_PROVIDER_ID: &str = "starrocks";

/// Typed read instance for one decoder-planned StarRocks tablet scan. The
/// FE-derived schema and storage configuration stay in this BE-local object;
/// split payloads carry only a checked range index.
struct StarRocksConnectorInstance {
    instance_id: ConnectorInstanceId,
    binding_key: ConnectorExecutionBindingKey,
    config: StarRocksScanConfig,
    ranges: Mutex<Vec<StarRocksScanRange>>,
}

impl StarRocksConnectorInstance {
    fn new(instance_id: ConnectorInstanceId, config: StarRocksScanConfig) -> Self {
        Self {
            binding_key: ConnectorExecutionBindingKey {
                instance_id: instance_id.clone(),
                incarnation: ConnectorInstanceIncarnation::new(),
            },
            instance_id,
            ranges: Mutex::new(config.ranges.clone()),
            config,
        }
    }

    fn split_for_index(&self, index: usize) -> Result<ConnectorSplit, ConnectorError> {
        let range = self.range_for_index(index)?;
        ConnectorSplit::try_new(
            self.instance_id.clone(),
            format!("starrocks-{}", range.tablet_id),
            bytes::Bytes::copy_from_slice(&(index as u64).to_le_bytes()),
            None,
        )
    }

    fn range_for_index(&self, index: usize) -> Result<StarRocksScanRange, ConnectorError> {
        self.ranges
            .lock()
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "StarRocks range lock poisoned",
                )
            })?
            .get(index)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "StarRocks split index is out of bounds",
                )
            })
    }

    fn range_for_split(
        &self,
        split: &ConnectorSplit,
    ) -> Result<StarRocksScanRange, ConnectorError> {
        if split.owner() != &self.instance_id || split.payload().len() != 8 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid StarRocks split payload",
            ));
        }
        let bytes: [u8; 8] = split
            .payload()
            .as_ref()
            .try_into()
            .expect("payload length checked");
        let index = usize::try_from(u64::from_le_bytes(bytes)).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks split index overflows usize",
            )
        })?;
        self.range_for_index(index)
    }

    fn execution_binding(self: Arc<Self>) -> Result<ConnectorExecutionBinding, ConnectorError> {
        ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse(STARROCKS_SPI_PROVIDER_ID)?,
            self.binding_key.clone(),
            self,
        )
    }
}

impl ConnectorReadExecution for StarRocksConnectorInstance {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        let range = self.range_for_split(split)?;
        let mut config = self.config.clone();
        config.ranges = vec![range];
        config.limit = None;
        Ok(Box::new(StarRocksBatchReader {
            iter: open_starrocks_scan_iter(config)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?,
            context: request.context,
            closed: false,
        }))
    }
}

struct StarRocksBatchReader {
    iter: StarRocksScanIter,
    context: ConnectorRequestContext,
    closed: bool,
}

impl ConnectorBatchReader for StarRocksBatchReader {
    fn next_batch(&mut self) -> Result<Option<arrow::record_batch::RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        if self.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        match self.iter.next() {
            Some(Ok(chunk)) => Ok(Some(chunk.batch)),
            Some(Err(error)) => Err(ConnectorError::new(ConnectorErrorKind::Internal, error)),
            None => {
                self.closed = true;
                Ok(None)
            }
        }
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.iter.close_scanner();
        self.closed = true;
        Ok(())
    }
}

pub(crate) fn plan_starrocks_read_source(
    instance_id: ConnectorInstanceId,
    config: StarRocksScanConfig,
    batch: ConnectorBatchBudget,
    context: ConnectorRequestContext,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let provider = Arc::new(StarRocksConnectorInstance::new(instance_id, config));
    let range_count = provider
        .ranges
        .lock()
        .map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "StarRocks range lock poisoned",
            )
        })?
        .len();
    let scheduled = (0..range_count)
        .map(|index| {
            let range = provider.range_for_index(index)?;
            provider.split_for_index(index).map(|split| {
                crate::connector::runtime::ConnectorScheduledSplit::storage_tablet(
                    split,
                    range.tablet_id,
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_schema = provider.config.output_chunk_schema.arrow_schema_ref();
    let chunk_schema = Arc::clone(&provider.config.output_chunk_schema);
    let has_more = provider.config.has_more;
    let binding = Arc::new(Arc::clone(&provider).execution_binding()?);
    Ok(Arc::new(
        ConnectorReadScanSource::new_scheduled_execution_with_incremental(
            binding,
            scheduled,
            ConnectorOpenReaderRequest {
                expected_schema,
                batch,
                context,
            },
            chunk_schema,
            None,
            has_more,
        ),
    ))
}

struct StarRocksQueryCancellation {
    query_id: Option<QueryId>,
}

impl ConnectorCancellation for StarRocksQueryCancellation {
    fn is_cancelled(&self) -> bool {
        self.query_id
            .is_some_and(|query_id| query_context_manager().is_query_canceled(query_id))
    }
}

fn starrocks_read_budget_and_context(
    query_id: Option<QueryId>,
    query_options: &QueryOptions,
) -> Result<(ConnectorBatchBudget, ConnectorRequestContext), ConnectorError> {
    starrocks_read_budget_and_context_with_cancellation(
        query_options,
        Arc::new(StarRocksQueryCancellation { query_id }),
    )
}

fn starrocks_read_budget_and_context_with_cancellation(
    query_options: &QueryOptions,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<(ConnectorBatchBudget, ConnectorRequestContext), ConnectorError> {
    let rows = query_options
        .batch_size
        .and_then(|value| usize::try_from(value).ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(4096).expect("default batch size is nonzero"));
    let batch = ConnectorBatchBudget {
        max_rows: rows,
        max_bytes: NonZeroUsize::new(MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES)
            .expect("SPI handle maximum is nonzero"),
    };
    let (_, query_expire) = query_expire_durations(Some(query_options));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        cancellation,
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )?;
    Ok((batch, context))
}

pub fn plan_compat_starrocks_read_source(
    query_id: QueryId,
    node_id: i32,
    config: StarRocksScanConfig,
    query_options: &QueryOptions,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let (batch, context) = starrocks_read_budget_and_context(Some(query_id), query_options)?;
    plan_starrocks_read_source(
        ConnectorInstanceId::parse(&format!("starrocks.{query_id}.{node_id}"))?,
        config,
        batch,
        context,
    )
}

pub fn plan_native_starrocks_read_source(
    query_id: Option<QueryId>,
    node_id: i32,
    config: StarRocksScanConfig,
    query_options: &QueryOptions,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    plan_native_starrocks_read_source_with_cancellation(
        query_id,
        node_id,
        config,
        query_options,
        Arc::new(StarRocksQueryCancellation { query_id }),
    )
}

/// Build a native scan source using the cancellation capability supplied by
/// the Backend decoder. The helper never consults query runtime state.
pub fn plan_native_starrocks_read_source_with_cancellation(
    query_id: Option<QueryId>,
    node_id: i32,
    config: StarRocksScanConfig,
    query_options: &QueryOptions,
    cancellation: Arc<dyn ConnectorCancellation>,
) -> Result<Arc<dyn ScanSource>, ConnectorError> {
    let (batch, context) =
        starrocks_read_budget_and_context_with_cancellation(query_options, cancellation)?;
    let instance_query_id = query_id
        .map(|query_id| query_id.to_string())
        .unwrap_or_else(|| "unidentified".to_string());
    plan_starrocks_read_source(
        ConnectorInstanceId::parse(&format!("starrocks.native.{instance_query_id}.{node_id}"))?,
        config,
        batch,
        context,
    )
}

#[derive(Clone, Debug)]
struct StarRocksExecutionContext {
    partition_storage_paths: HashMap<i64, String>,
    object_store_profile: Option<ObjectStoreProfile>,
}

impl StarRocksExecutionContext {
    fn from_scan_config(cfg: &StarRocksScanConfig) -> Result<Self, String> {
        let resolved_properties;
        let properties = if parse_partition_storage_paths_optional(&cfg.properties)?.is_some() {
            &cfg.properties
        } else if let Some(deferred) = cfg.deferred_lake_resolution.as_ref() {
            resolved_properties = lake_scan_execution_properties(
                deferred.query_id,
                &deferred.table,
                &deferred.tablets,
                deferred.starlet_metadata_provider.as_deref(),
            )?;
            &resolved_properties
        } else {
            &cfg.properties
        };
        let partition_storage_paths = resolve_partition_storage_paths(properties)?;
        let object_store_profile =
            resolve_object_store_profile(properties, partition_storage_paths.values())?;
        Ok(Self {
            partition_storage_paths,
            object_store_profile,
        })
    }
}

fn open_starrocks_scan_iter(config: StarRocksScanConfig) -> Result<StarRocksScanIter, String> {
    let context = StarRocksExecutionContext::from_scan_config(&config)?;
    Ok(StarRocksScanIter::new(config, context))
}

pub(crate) fn read_starrocks_batches(config: StarRocksScanConfig) -> Result<BoxedExecIter, String> {
    Ok(Box::new(open_starrocks_scan_iter(config)?))
}

struct StarRocksScanIter {
    cfg: StarRocksScanConfig,
    ctx: StarRocksExecutionContext,
    range_idx: usize,
    scanner: Option<StarRocksScanner>,
    finished: bool,
    total_rows: usize,
}

impl StarRocksScanIter {
    fn new(cfg: StarRocksScanConfig, ctx: StarRocksExecutionContext) -> Self {
        Self {
            cfg,
            ctx,
            range_idx: 0,
            scanner: None,
            finished: false,
            total_rows: 0,
        }
    }

    fn close_scanner(&mut self) {
        if let Some(scanner) = self.scanner.as_mut()
            && let Err(e) = scanner.close()
        {
            warn!("failed to close starrocks scanner: {}", e);
        }
        self.scanner = None;
    }

    fn open_next_scanner(&mut self) -> Result<(), String> {
        self.close_scanner();
        if self.range_idx >= self.cfg.ranges.len() {
            return Err("no more starrocks scan ranges".to_string());
        }
        let range = self.cfg.ranges[self.range_idx].clone();
        let scanner = StarRocksScanner::open(&self.cfg, &self.ctx, range)?;
        self.scanner = Some(scanner);
        Ok(())
    }

    fn remaining_limit(&self) -> Option<usize> {
        self.cfg
            .limit
            .map(|limit| limit.saturating_sub(self.total_rows))
    }
}

impl Iterator for StarRocksScanIter {
    type Item = Result<Chunk, String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Some(remaining) = self.remaining_limit()
            && remaining == 0
        {
            self.finished = true;
            self.close_scanner();
            return None;
        }

        loop {
            if self.scanner.is_none() {
                if self.range_idx >= self.cfg.ranges.len() {
                    self.finished = true;
                    return None;
                }
                if let Err(e) = self.open_next_scanner() {
                    self.finished = true;
                    return Some(Err(e));
                }
            }

            let Some(scanner) = self.scanner.as_mut() else {
                self.finished = true;
                return None;
            };

            match scanner.get_next() {
                Ok(ScanBatch::Eos) => {
                    self.close_scanner();
                    self.range_idx += 1;
                    continue;
                }
                Ok(ScanBatch::Chunk(chunk, rows, _bytes)) => {
                    let mut out_chunk = chunk;
                    let chunk_rows = rows;
                    let remaining = self.remaining_limit();
                    if let Some(remaining) = remaining
                        && chunk_rows > remaining
                    {
                        out_chunk = out_chunk.slice(0, remaining);
                        self.total_rows = self.total_rows.saturating_add(remaining);
                        self.finished = true;
                        self.close_scanner();
                        return Some(Ok(out_chunk));
                    }

                    self.total_rows = self.total_rows.saturating_add(chunk_rows);
                    return Some(Ok(out_chunk));
                }
                Err(e) => {
                    self.close_scanner();
                    return Some(Err(e));
                }
            }
        }
    }
}

enum ScanBatch {
    Eos,
    Chunk(Chunk, usize, usize),
}

struct StarRocksScanner {
    reader: StarRocksNativeReader,
    output_chunk_schema: ChunkSchemaRef,
}

impl StarRocksScanner {
    fn open(
        cfg: &StarRocksScanConfig,
        ctx: &StarRocksExecutionContext,
        range: StarRocksScanRange,
    ) -> Result<Self, String> {
        let output_slot_meta = cfg
            .output_chunk_schema
            .slots()
            .iter()
            .map(|slot| (slot.name().to_string(), slot.slot_id()))
            .collect::<Vec<_>>();
        info!(
            "StarRocksScanner::open tablet_id={} output_slot_ids={:?} output_slot_meta={:?} query_global_dict_slots={:?}",
            range.tablet_id,
            cfg.output_chunk_schema.slot_ids(),
            output_slot_meta,
            cfg.query_global_dicts.keys().collect::<Vec<_>>()
        );
        let partition_id = range.partition_id.ok_or_else(|| {
            format!(
                "missing partition_id in starrocks scan range for tablet_id {}",
                range.tablet_id
            )
        })?;
        let storage_path = ctx
            .partition_storage_paths
            .get(&partition_id)
            .ok_or_else(|| {
                format!(
                    "missing partition_storage_path for partition_id {} (tablet_id={}) in partition_storage_paths",
                    partition_id, range.tablet_id
                )
            })?
            .clone();

        let version = range
            .version
            .ok_or_else(|| format!("missing tablet version for tablet_id {}", range.tablet_id))?;
        eprintln!(
            "[DEBUG] StarRocksScanner::open tablet_id={} partition_id={} version={} storage_path={}",
            range.tablet_id, partition_id, version, storage_path
        );

        let reader = StarRocksNativeReader::open(
            range.tablet_id,
            &storage_path,
            version,
            cfg.required_chunk_schema.clone(),
            cfg.output_chunk_schema.clone(),
            cfg.query_global_dicts.clone(),
            cfg.min_max_predicates.clone(),
            ctx.object_store_profile.as_ref(),
            cfg.lake_schema_meta.as_ref(),
        )?;

        Ok(Self {
            reader,
            output_chunk_schema: cfg.output_chunk_schema.clone(),
        })
    }

    fn get_next(&mut self) -> Result<ScanBatch, String> {
        let output_schema = self.output_chunk_schema.arrow_schema_ref();
        let batch = self.reader.get_next(&output_schema)?;
        match batch {
            None => Ok(ScanBatch::Eos),
            Some(batch) => {
                let rows = batch.num_rows();
                let chunk =
                    Chunk::try_new_with_chunk_schema(batch, self.output_chunk_schema.clone())?;
                let bytes = chunk.logical_bytes();
                Ok(ScanBatch::Chunk(chunk, rows, bytes))
            }
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.reader.close()
    }
}

pub(crate) fn build_native_object_store_profile_from_properties(
    props: &BTreeMap<String, String>,
) -> Result<Option<ObjectStoreProfile>, String> {
    ObjectStoreProfile::from_properties_optional(props)
}

fn resolve_partition_storage_paths(
    properties: &BTreeMap<String, String>,
) -> Result<HashMap<i64, String>, String> {
    if let Some(paths) = parse_partition_storage_paths_optional(properties)? {
        return Ok(paths);
    }
    Err(
        "starrocks direct read requires FE to provide partition_storage_paths in execution properties"
            .to_string(),
    )
}

fn parse_partition_storage_paths_optional(
    props: &BTreeMap<String, String>,
) -> Result<Option<HashMap<i64, String>>, String> {
    let Some(raw) = props.get("partition_storage_paths") else {
        return Ok(None);
    };

    let value: Value = serde_json::from_str(raw)
        .map_err(|e| format!("parse partition_storage_paths json failed: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "partition_storage_paths must be a JSON object".to_string())?;

    let mut out = HashMap::with_capacity(obj.len());
    for (key, value) in obj {
        let partition_id = key
            .parse::<i64>()
            .map_err(|_| format!("invalid partition_id in partition_storage_paths: {key}"))?;
        let path = value
            .as_str()
            .ok_or_else(|| format!("partition_storage_paths entry for {key} is not a string"))?;
        if path.is_empty() {
            return Err(format!("partition_storage_paths entry for {key} is empty"));
        }
        out.insert(partition_id, path.to_string());
    }

    Ok(Some(out))
}

fn resolve_object_store_profile<'a>(
    props: &BTreeMap<String, String>,
    storage_paths: impl Iterator<Item = &'a String>,
) -> Result<Option<ObjectStoreProfile>, String> {
    if let Some(profile) = ObjectStoreProfile::from_properties_optional(props)? {
        let paths = storage_paths.cloned().collect::<Vec<_>>();
        for path in &paths {
            resolve_with_profile(path, Some(&profile))?;
        }
        eprintln!(
            "[DEBUG] starrocks direct read explicit object store profile endpoint={}",
            profile.endpoint
        );
        info!(
            "starrocks direct read uses explicit object store profile endpoint={}",
            profile.endpoint
        );
        return Ok(Some(profile));
    }

    let paths = storage_paths.cloned().collect::<Vec<_>>();
    if paths.is_empty() {
        return Ok(None);
    }

    let mut selected: Option<crate::runtime::starlet_shard_registry::S3StoreConfig> = None;
    let mut requires_profile_seen: Option<bool> = None;
    for path in &paths {
        let requires_profile = path_requires_object_store_profile(path)?;
        if let Some(prev) = requires_profile_seen {
            if prev != requires_profile {
                return Err("mixed scan path schemes are not allowed".to_string());
            }
        } else {
            requires_profile_seen = Some(requires_profile);
        }
        if !requires_profile {
            resolve_with_profile(path, None)?;
            continue;
        }
        let s3 = starlet_shard_registry::infer_s3_config_for_path(path).ok_or_else(|| {
            format!(
                "missing object store config for direct-read path={path}; provide aws.s3.* \
                 properties or ensure shard/env credentials can be inferred"
            )
        })?;
        let profile = ObjectStoreProfile::from_s3_store_config(&s3)?;
        resolve_with_profile(path, Some(&profile))?;
        match selected.as_ref() {
            None => selected = Some(s3),
            Some(prev) if prev == &s3 => {}
            Some(prev) => {
                return Err(format!(
                    "inconsistent inferred object store configs across starrocks direct-read paths: \
                     current_bucket={} current_endpoint={} previous_bucket={} previous_endpoint={}",
                    s3.bucket, s3.endpoint, prev.bucket, prev.endpoint
                ));
            }
        }
    }
    selected
        .as_ref()
        .map(|config| {
            eprintln!(
                "[DEBUG] starrocks direct read inferred object store config bucket={} endpoint={}",
                config.bucket, config.endpoint
            );
            info!(
                "starrocks direct read inferred object store config bucket={} endpoint={}",
                config.bucket, config.endpoint
            );
            ObjectStoreProfile::from_s3_store_config(config)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::DeferredLakeScanResolution;
    use crate::connector::starrocks::fe_v2_meta::{LakeScanTabletRef, LakeTableIdentity};
    use crate::runtime::query_context::QueryId;

    #[test]
    fn deferred_lake_scan_resolution_keeps_protocol_neutral_identity_and_tablets() {
        let input = DeferredLakeScanResolution::new(
            Some(QueryId { hi: 1, lo: 2 }),
            LakeTableIdentity {
                catalog: "default_catalog".to_string(),
                db_name: "db".to_string(),
                table_name: "t".to_string(),
                db_id: 10,
                table_id: 20,
                schema_id: 30,
            },
            vec![LakeScanTabletRef {
                tablet_id: 300,
                partition_id: 100,
                version: 7,
            }],
            None,
        );

        assert_eq!(input.query_id, Some(QueryId { hi: 1, lo: 2 }));
        assert_eq!(input.table.cache_key(), "default_catalog:10:20:30");
        assert_eq!(input.tablets.len(), 1);
        assert_eq!(input.tablets[0].tablet_id, 300);
        assert_eq!(input.tablets[0].partition_id, 100);
        assert_eq!(input.tablets[0].version, 7);
    }
}
