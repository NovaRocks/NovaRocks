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

use crate::cache::ExternalDataCacheRangeOptions;
use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind, ScanSourceContract};
use crate::exec::node::scan::{HdfsScanFileFormat, IncrementalHdfsScanRange, IncrementalScanRange};
use crate::protocol::common::error::FieldPath;
use crate::runtime::endpoint::{FragmentDestination, RuntimeEndpoint};
use crate::runtime::fragment::instance::{BackendNum, FragmentInstanceId};
use crate::runtime::query_context::QueryId;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::scan_range::{
    BrokerFileFormat, BrokerFileScanRange, DatacacheOptions, DeletionVectorDescriptor, FileFormat,
    FileScanRange, IcebergDeleteFile, IcebergFileContent, IcebergFileFormat, ScanRangeParams,
};
use crate::thrift::{descriptors, internal_service, plan_nodes, types};

use super::{StarRocksFragmentDecodeError, decode_query_options, decode_runtime_endpoint};

pub(crate) struct DecodedStarRocksInstanceParts {
    pub(crate) query_id: QueryId,
    pub(crate) fragment_instance_id: FragmentInstanceId,
    pub(crate) backend_num: BackendNum,
    pub(crate) query_options: QueryOptions,
    pub(crate) pipeline_dop: NonZeroUsize,
    pub(crate) scan_ranges: BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    pub(crate) per_exchange_sender_counts: BTreeMap<i32, i32>,
    pub(crate) batch_exchange_sender_counts: HashMap<i32, usize>,
    pub(crate) report_endpoint: Option<RuntimeEndpoint>,
    pub(crate) destinations: Vec<FragmentDestination>,
    pub(crate) sender_id: Option<i32>,
    pub(crate) typed_result_sink: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LakeScanProgramFacts {
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct LakeMetaScanRangeFact {
    pub(crate) tablet_id: i64,
    pub(crate) version: i64,
    pub(crate) row_count: Option<i64>,
    pub(crate) partition_id: Option<i64>,
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) empty: bool,
    pub(crate) has_more: bool,
}

pub(crate) fn decode_lake_meta_scan_range_facts(
    nodes: &[plan_nodes::TPlanNode],
    raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    path: FieldPath,
) -> Result<BTreeMap<i32, Vec<LakeMetaScanRangeFact>>, StarRocksFragmentDecodeError> {
    let mut output = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_META_SCAN_NODE)
    {
        let ranges = raw_ranges.get(&node.node_id).ok_or_else(|| {
            StarRocksFragmentDecodeError::missing(
                path.clone().map_key(node.node_id.to_string()),
                "LAKE_META_SCAN_NODE requires per-node ranges",
            )
        })?;
        let mut decoded = Vec::with_capacity(ranges.len());
        for (index, params) in ranges.iter().enumerate() {
            if params.empty.unwrap_or(false) {
                decoded.push(LakeMetaScanRangeFact {
                    tablet_id: 0,
                    version: 0,
                    row_count: None,
                    partition_id: None,
                    db_name: None,
                    table_name: None,
                    empty: true,
                    has_more: params.has_more.unwrap_or(false),
                });
                continue;
            }
            let internal = params
                .scan_range
                .internal_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone()
                            .map_key(node.node_id.to_string())
                            .index(index)
                            .field("scan_range")
                            .field("internal_scan_range"),
                        "LAKE_META_SCAN_NODE requires internal_scan_range",
                    )
                })?;
            let version = internal.version.parse::<i64>().map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(
                    path.clone()
                        .map_key(node.node_id.to_string())
                        .index(index)
                        .field("scan_range")
                        .field("internal_scan_range")
                        .field("version"),
                    format!("invalid tablet version {:?}: {error}", internal.version),
                )
            })?;
            decoded.push(LakeMetaScanRangeFact {
                tablet_id: internal.tablet_id,
                version,
                row_count: internal.row_count,
                partition_id: internal.partition_id,
                db_name: (!internal.db_name.trim().is_empty())
                    .then(|| internal.db_name.trim().to_string()),
                table_name: internal
                    .table_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
                empty: false,
                has_more: params.has_more.unwrap_or(false),
            });
        }
        output.insert(node.node_id, decoded);
    }
    Ok(output)
}

pub(crate) fn decode_lake_scan_program_facts(
    nodes: &[plan_nodes::TPlanNode],
    raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    path: FieldPath,
) -> Result<BTreeMap<i32, LakeScanProgramFacts>, StarRocksFragmentDecodeError> {
    let mut output = BTreeMap::new();
    for node in nodes
        .iter()
        .filter(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
    {
        let mut facts = LakeScanProgramFacts::default();
        for (index, params) in raw_ranges
            .get(&node.node_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
            .iter()
            .enumerate()
        {
            if params.empty.unwrap_or(false) {
                continue;
            }
            let internal = params
                .scan_range
                .internal_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone()
                            .map_key(node.node_id.to_string())
                            .index(index)
                            .field("scan_range")
                            .field("internal_scan_range"),
                        "LAKE_SCAN_NODE requires internal_scan_range",
                    )
                })?;
            let fill_data_cache = internal.fill_data_cache.unwrap_or(true);
            let skip_page_cache = internal.skip_page_cache.unwrap_or(false);
            let skip_disk_cache = internal.skip_disk_cache.unwrap_or(false);
            if !fill_data_cache || skip_page_cache || skip_disk_cache {
                return Err(StarRocksFragmentDecodeError::unsupported(
                    path.clone()
                        .map_key(node.node_id.to_string())
                        .index(index)
                        .field("scan_range")
                        .field("internal_scan_range"),
                    "internal-table cache controls are not supported",
                ));
            }
            if facts.db_name.is_none() {
                facts.db_name = (!internal.db_name.trim().is_empty())
                    .then(|| internal.db_name.trim().to_string());
            }
            if facts.table_name.is_none() {
                facts.table_name = internal
                    .table_name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string);
            }
        }
        output.insert(node.node_id, facts);
    }
    Ok(output)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StarRocksDecodeFacts {
    stream_load_paths: BTreeMap<UniqueId, String>,
    path_rewrite: Option<StarRocksPathRewriteFacts>,
    datacache_available: bool,
    jdbc: Option<StarRocksJdbcFacts>,
    object_store_defaults: StarRocksObjectStoreDefaults,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct StarRocksObjectStoreDefaults {
    retry_max_times: Option<usize>,
    retry_min_delay_ms: Option<u64>,
    retry_max_delay_ms: Option<u64>,
    timeout_ms: Option<u64>,
    io_timeout_ms: Option<u64>,
}

impl StarRocksObjectStoreDefaults {
    pub(crate) fn new(
        retry_max_times: Option<usize>,
        retry_min_delay_ms: Option<u64>,
        retry_max_delay_ms: Option<u64>,
        timeout_ms: Option<u64>,
        io_timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            retry_max_times,
            retry_min_delay_ms,
            retry_max_delay_ms,
            timeout_ms,
            io_timeout_ms,
        }
    }

    pub(crate) fn apply_to(&self, config: &mut crate::fs::object_store::ObjectStoreConfig) {
        config.retry_max_times = config.retry_max_times.or(self.retry_max_times);
        config.retry_min_delay_ms = config.retry_min_delay_ms.or(self.retry_min_delay_ms);
        config.retry_max_delay_ms = config.retry_max_delay_ms.or(self.retry_max_delay_ms);
        config.timeout_ms = config.timeout_ms.or(self.timeout_ms);
        config.io_timeout_ms = config.io_timeout_ms.or(self.io_timeout_ms);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksJdbcFacts {
    url: String,
    user: Option<String>,
    password: Option<String>,
    default_db: Option<String>,
}

impl StarRocksJdbcFacts {
    pub(crate) fn new(
        url: String,
        user: Option<String>,
        password: Option<String>,
        default_db: Option<String>,
    ) -> Self {
        Self {
            url,
            user,
            password,
            default_db,
        }
    }

    pub(crate) fn connection(&self) -> (String, Option<String>, Option<String>) {
        (self.url.clone(), self.user.clone(), self.password.clone())
    }

    pub(crate) fn default_db(&self) -> Option<&str> {
        self.default_db.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksPathRewriteFacts {
    from_prefix: String,
    to_prefix: String,
}

impl StarRocksPathRewriteFacts {
    pub(crate) fn new(from_prefix: String, to_prefix: String) -> Self {
        Self {
            from_prefix,
            to_prefix,
        }
    }

    pub(crate) fn from_prefix(&self) -> &str {
        &self.from_prefix
    }

    pub(crate) fn to_prefix(&self) -> &str {
        &self.to_prefix
    }
}

impl StarRocksDecodeFacts {
    pub(crate) fn new(
        stream_load_paths: BTreeMap<UniqueId, String>,
        path_rewrite: Option<StarRocksPathRewriteFacts>,
        datacache_available: bool,
        jdbc: Option<StarRocksJdbcFacts>,
        object_store_defaults: StarRocksObjectStoreDefaults,
    ) -> Self {
        Self {
            stream_load_paths,
            path_rewrite,
            datacache_available,
            jdbc,
            object_store_defaults,
        }
    }

    pub(crate) fn stream_load_path(&self, id: UniqueId) -> Option<&str> {
        self.stream_load_paths.get(&id).map(String::as_str)
    }

    pub(crate) fn path_rewrite(&self) -> Option<&StarRocksPathRewriteFacts> {
        self.path_rewrite.as_ref()
    }

    pub(crate) const fn datacache_available(&self) -> bool {
        self.datacache_available
    }

    pub(crate) fn jdbc(&self) -> Option<&StarRocksJdbcFacts> {
        self.jdbc.as_ref()
    }

    pub(crate) fn object_store_defaults(&self) -> &StarRocksObjectStoreDefaults {
        &self.object_store_defaults
    }
}

/// Captures process-local values required by the StarRocks decoder.
///
/// This remains with the decoder until RCI-5F moves the protocol owner to
/// `novarocks-compat`; it is deliberately not part of fragment admission state.
pub fn snapshot_decode_facts(
    exec_params: &internal_service::TPlanFragmentExecParams,
) -> Result<StarRocksDecodeFacts, String> {
    let mut stream_load_paths = BTreeMap::new();
    for ranges in exec_params.per_node_scan_ranges.values() {
        for params in ranges {
            let Some(broker) = params.scan_range.broker_scan_range.as_ref() else {
                continue;
            };
            for range in &broker.ranges {
                if range.file_type != types::TFileType::FILE_STREAM {
                    continue;
                }
                let load_id = range
                    .load_id
                    .as_ref()
                    .ok_or_else(|| "FILE_STREAM range is missing load_id".to_string())?;
                let path =
                    crate::service::stream_load_registry::resolve_stream_load_file_path(load_id)
                        .ok_or_else(|| {
                            format!(
                                "no registered local file for FILE_STREAM load_id={}:{}",
                                load_id.hi, load_id.lo
                            )
                        })?;
                stream_load_paths.insert(
                    UniqueId {
                        hi: load_id.hi,
                        lo: load_id.lo,
                    },
                    path,
                );
            }
        }
    }
    let config = crate::common::app_config::config().map_err(|error| error.to_string())?;
    let rewrite = &config.runtime.path_rewrite;
    let path_rewrite = rewrite.enable.then(|| {
        StarRocksPathRewriteFacts::new(rewrite.from_prefix.clone(), rewrite.to_prefix.clone())
    });
    let datacache_available = config.runtime.cache.datacache_enable
        && crate::cache::DataCacheManager::instance()
            .block_cache()
            .is_some();
    let jdbc = config.jdbc_config().map(|jdbc| {
        StarRocksJdbcFacts::new(
            jdbc.url.clone(),
            jdbc.user.clone(),
            jdbc.password.clone(),
            jdbc.default_db.clone(),
        )
    });
    let object_storage = &config.runtime.object_storage;
    let object_store_defaults = StarRocksObjectStoreDefaults::new(
        object_storage.retry_max_times,
        object_storage.retry_min_delay_ms,
        object_storage.retry_max_delay_ms,
        object_storage.timeout_ms,
        object_storage.io_timeout_ms,
    );
    Ok(StarRocksDecodeFacts::new(
        stream_load_paths,
        path_rewrite,
        datacache_available,
        jdbc,
        object_store_defaults,
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_instance_parts(
    params: &internal_service::TPlanFragmentExecParams,
    query_options: Option<&internal_service::TQueryOptions>,
    coord: Option<&types::TNetworkAddress>,
    backend_num: Option<i32>,
    pipeline_dop: i32,
    batch_exchange_sender_counts: &HashMap<i32, usize>,
    typed_result_sink: bool,
    _facts: &StarRocksDecodeFacts,
    root_path: FieldPath,
) -> Result<DecodedStarRocksInstanceParts, StarRocksFragmentDecodeError> {
    let params_path = root_path.clone().field("params");
    let pipeline_dop = usize::try_from(pipeline_dop)
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| {
            StarRocksFragmentDecodeError::out_of_range(
                root_path
                    .clone()
                    .field("query_options")
                    .field("pipeline_dop"),
                format!("pipeline_dop must be positive, got {pipeline_dop}"),
            )
        })?;
    let backend_num = backend_num.unwrap_or(0);
    let backend_num =
        BackendNum::try_new(backend_num).map_err(StarRocksFragmentDecodeError::Binding)?;
    let query_options = decode_query_options(query_options)?;
    let report_endpoint = coord
        .map(|value| decode_runtime_endpoint(value, root_path.clone().field("coord")))
        .transpose()?;
    Ok(DecodedStarRocksInstanceParts {
        query_id: QueryId {
            hi: params.query_id.hi,
            lo: params.query_id.lo,
        },
        fragment_instance_id: FragmentInstanceId::new(UniqueId {
            hi: params.fragment_instance_id.hi,
            lo: params.fragment_instance_id.lo,
        }),
        backend_num,
        query_options,
        pipeline_dop,
        scan_ranges: params.per_node_scan_ranges.clone(),
        per_exchange_sender_counts: params.per_exch_num_senders.clone(),
        batch_exchange_sender_counts: batch_exchange_sender_counts.clone(),
        report_endpoint,
        destinations: params
            .destinations
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .enumerate()
            .map(|(index, destination)| {
                super::decode_fragment_destination(
                    destination,
                    params_path.clone().field("destinations").index(index),
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        sender_id: params.sender_id,
        typed_result_sink,
    })
}

/// Decode the static scan contracts and the transient per-node enrichment
/// carrier. This does NOT build the instance's `ScanAssignments`: it returns the
/// raw `(ScanAssignmentKind, Vec<ScanRangeParams>)` per node that the scan
/// decoders read (kind guard + range enrichment); the instance assignments are
/// assembled afterwards from the enriched `BoundScanRanges` the decoders capture.
pub(crate) fn decode_scan_contracts_and_raw_ranges(
    nodes: &[plan_nodes::TPlanNode],
    raw_ranges: &BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
    descriptors: Option<&descriptors::TDescriptorTable>,
    facts: &StarRocksDecodeFacts,
    path: FieldPath,
) -> Result<
    (
        BTreeMap<FragmentNodeId, ScanSourceContract>,
        // Transient enrichment INPUT per scan node: its assignment kind (for the
        // decoders' kind guards) plus the decoded `ScanRangeParams`. The
        // instance's `ScanAssignments` are assembled after node decode from the
        // enriched `BoundScanRanges` captured by the decoders.
        BTreeMap<FragmentNodeId, (ScanAssignmentKind, Vec<ScanRangeParams>)>,
    ),
    StarRocksFragmentDecodeError,
> {
    let mut kinds = BTreeMap::new();
    let mut known_node_ids = BTreeMap::new();
    let mut schema_requirements = BTreeMap::new();
    let change_op_slots = decode_change_op_slots(nodes, descriptors)?;
    for (index, node) in nodes.iter().enumerate() {
        known_node_ids.insert(node.node_id, index);
        let kind = match node.node_type {
            plan_nodes::TPlanNodeType::FILE_SCAN_NODE => Some(ScanAssignmentKind::BrokerFile),
            plan_nodes::TPlanNodeType::HDFS_SCAN_NODE => Some(ScanAssignmentKind::File),
            plan_nodes::TPlanNodeType::LAKE_SCAN_NODE => Some(ScanAssignmentKind::StarRocksTablet),
            plan_nodes::TPlanNodeType::SCHEMA_SCAN_NODE => {
                super::node::supported_schema_scan_requires_ranges(node).map(|required| {
                    schema_requirements.insert(FragmentNodeId::new(node.node_id), required);
                    ScanAssignmentKind::SchemaSelection
                })
            }
            _ => None,
        };
        if let Some(kind) = kind {
            let id = FragmentNodeId::new(node.node_id);
            if kinds.insert(id, kind).is_some() {
                return Err(StarRocksFragmentDecodeError::invalid_value(
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("plan")
                        .field("nodes")
                        .index(index)
                        .field("node_id"),
                    format!("duplicate scan node id {}", node.node_id),
                ));
            }
        }
    }
    for node_id in raw_ranges.keys() {
        if !known_node_ids.contains_key(node_id) {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                path.clone().map_key(node_id.to_string()),
                format!("scan ranges assigned to unknown scan node {node_id}"),
            ));
        }
    }
    let contracts = kinds
        .iter()
        .map(|(id, kind)| (*id, ScanSourceContract::new(*kind)))
        .collect::<BTreeMap<_, _>>();
    let mut assignments = BTreeMap::new();
    for (id, kind) in kinds {
        let ranges = raw_ranges.get(&id.get()).map(Vec::as_slice).unwrap_or(&[]);
        if kind == ScanAssignmentKind::SchemaSelection {
            if schema_requirements.get(&id).copied().unwrap_or(false)
                && !raw_ranges.contains_key(&id.get())
            {
                return Err(StarRocksFragmentDecodeError::missing(
                    path.clone().map_key(id.get().to_string()),
                    "schema scan requires a per-node selection assignment",
                ));
            }
            if ranges.iter().any(|range| range.has_more.unwrap_or(false)) {
                return Err(StarRocksFragmentDecodeError::unsupported(
                    path.clone().map_key(id.get().to_string()),
                    "incremental schema-scan selections are not supported",
                ));
            }
            let selected =
                ranges.is_empty() || ranges.iter().any(|range| !range.empty.unwrap_or(false));
            assignments.insert(
                id,
                (kind, vec![ScanRangeParams::schema_selection(selected)]),
            );
            continue;
        }
        let mut decoded = Vec::new();
        for (index, params) in ranges.iter().enumerate() {
            if params.empty.unwrap_or(false) {
                continue;
            }
            decoded.extend(decode_scan_range_params(
                kind,
                params,
                change_op_slots.get(&id.get()).copied().flatten(),
                facts,
                path.clone().map_key(id.get().to_string()).index(index),
            )?);
        }
        assignments.insert(id, (kind, decoded));
    }
    Ok((contracts, assignments))
}

fn decode_scan_range_params(
    kind: ScanAssignmentKind,
    params: &internal_service::TScanRangeParams,
    change_op_slot: Option<SlotId>,
    facts: &StarRocksDecodeFacts,
    path: FieldPath,
) -> Result<Vec<ScanRangeParams>, StarRocksFragmentDecodeError> {
    let decoded = match kind {
        ScanAssignmentKind::BrokerFile => {
            let broker = params
                .scan_range
                .broker_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone().field("scan_range").field("broker_scan_range"),
                        "FILE_SCAN_NODE assignment requires broker_scan_range",
                    )
                })?;
            broker
                .ranges
                .iter()
                .enumerate()
                .map(|(range_index, range)| {
                    let range_path = path
                        .clone()
                        .field("scan_range")
                        .field("broker_scan_range")
                        .field("ranges")
                        .index(range_index);
                    let path_value = if range.file_type == types::TFileType::FILE_LOCAL {
                        range.path.clone()
                    } else if range.file_type == types::TFileType::FILE_STREAM {
                        let load_id = range.load_id.as_ref().ok_or_else(|| {
                            StarRocksFragmentDecodeError::missing(
                                range_path.clone().field("load_id"),
                                "FILE_STREAM range requires load_id",
                            )
                        })?;
                        facts
                            .stream_load_path(UniqueId {
                                hi: load_id.hi,
                                lo: load_id.lo,
                            })
                            .ok_or_else(|| {
                                StarRocksFragmentDecodeError::missing(
                                    range_path.clone().field("load_id"),
                                    "FILE_STREAM load_id has no immutable path fact",
                                )
                            })?
                            .to_string()
                    } else {
                        return Err(StarRocksFragmentDecodeError::unsupported(
                            range_path.clone().field("file_type"),
                            format!("unsupported broker file type {:?}", range.file_type),
                        ));
                    };
                    let format =
                        if range.format_type == plan_nodes::TFileFormatType::FORMAT_CSV_PLAIN {
                            BrokerFileFormat::Csv
                        } else if range.format_type == plan_nodes::TFileFormatType::FORMAT_JSON {
                            BrokerFileFormat::Json
                        } else {
                            return Err(StarRocksFragmentDecodeError::unsupported(
                                range_path.clone().field("format_type"),
                                format!("unsupported broker file format {:?}", range.format_type),
                            ));
                        };
                    let mut decoded = ScanRangeParams::broker_file(BrokerFileScanRange {
                        path: path_value,
                        file_size: range.file_size.unwrap_or_default(),
                        offset: range.start_offset,
                        length: range.size,
                        format,
                        strip_outer_array: range.strip_outer_array.unwrap_or(false),
                        jsonpaths: range.jsonpaths.clone(),
                    });
                    decoded.volume_id = params.volume_id;
                    decoded.empty = params.empty;
                    decoded.has_more = params.has_more;
                    Ok(decoded)
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        ScanAssignmentKind::File => {
            let hdfs = params.scan_range.hdfs_scan_range.as_ref().ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    path.clone().field("scan_range").field("hdfs_scan_range"),
                    "HDFS_SCAN_NODE assignment requires hdfs_scan_range",
                )
            })?;
            vec![ScanRangeParams::file(decode_hdfs_scan_range(
                hdfs,
                change_op_slot,
                facts,
                path.clone(),
            )?)]
        }
        ScanAssignmentKind::StarRocksTablet => {
            let internal = params
                .scan_range
                .internal_scan_range
                .as_ref()
                .ok_or_else(|| {
                    StarRocksFragmentDecodeError::missing(
                        path.clone()
                            .field("scan_range")
                            .field("internal_scan_range"),
                        "LAKE_SCAN_NODE assignment requires internal_scan_range",
                    )
                })?;
            let partition_id = internal.partition_id.ok_or_else(|| {
                StarRocksFragmentDecodeError::missing(
                    path.clone()
                        .field("scan_range")
                        .field("internal_scan_range")
                        .field("partition_id"),
                    "LAKE_SCAN_NODE assignment requires partition_id",
                )
            })?;
            let version = internal.version.parse::<i64>().map_err(|error| {
                StarRocksFragmentDecodeError::invalid_value(
                    path.clone()
                        .field("scan_range")
                        .field("internal_scan_range")
                        .field("version"),
                    format!("invalid tablet version {:?}: {error}", internal.version),
                )
            })?;
            vec![
                ScanRangeParams::starrocks_tablet(internal.tablet_id, partition_id, version)
                    .map_err(|detail| {
                        StarRocksFragmentDecodeError::invalid_value(path.clone(), detail)
                    })?,
            ]
        }
        ScanAssignmentKind::SchemaSelection => unreachable!("schema selection is decoded per node"),
    };
    Ok(decoded
        .into_iter()
        .map(|mut range| {
            range.volume_id = params.volume_id;
            range.empty = params.empty;
            range.has_more = params.has_more;
            range
        })
        .collect())
}

fn decode_hdfs_scan_range(
    src: &plan_nodes::THdfsScanRange,
    change_op_slot: Option<SlotId>,
    facts: &StarRocksDecodeFacts,
    path: FieldPath,
) -> Result<FileScanRange, StarRocksFragmentDecodeError> {
    let file_format = match src.file_format.as_ref() {
        Some(value) if *value == descriptors::THdfsFileFormat::PARQUET => FileFormat::Parquet,
        Some(value) if *value == descriptors::THdfsFileFormat::ORC => FileFormat::Orc,
        Some(value) => {
            return Err(StarRocksFragmentDecodeError::unsupported(
                path.clone()
                    .field("scan_range")
                    .field("hdfs_scan_range")
                    .field("file_format"),
                format!("unsupported HDFS file format {value:?}"),
            ));
        }
        None => {
            return Err(StarRocksFragmentDecodeError::missing(
                path.clone()
                    .field("scan_range")
                    .field("hdfs_scan_range")
                    .field("file_format"),
                "HDFS scan range requires file_format",
            ));
        }
    };
    let mut full_path = src.full_path.clone();
    if let (Some(value), Some(rewrite)) = (full_path.as_mut(), facts.path_rewrite()) {
        let from = rewrite.from_prefix().trim();
        let to = rewrite.to_prefix().trim();
        if from.is_empty() || to.is_empty() {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                path.clone()
                    .field("scan_range")
                    .field("hdfs_scan_range")
                    .field("full_path"),
                "path rewrite facts require non-empty prefixes",
            ));
        }
        if value.starts_with(from) {
            *value = format!("{to}{}", &value[from.len()..]);
        }
    }
    let delete_files = src
        .delete_files
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let file_content = match file.file_content {
                Some(types::TIcebergFileContent::POSITION_DELETES) => {
                    IcebergFileContent::PositionDeletes
                }
                Some(types::TIcebergFileContent::EQUALITY_DELETES) => {
                    IcebergFileContent::EqualityDeletes
                }
                value => {
                    return Err(StarRocksFragmentDecodeError::unsupported(
                        path.clone()
                            .field("scan_range")
                            .field("hdfs_scan_range")
                            .field("delete_files")
                            .index(index)
                            .field("file_content"),
                        format!("unsupported Iceberg delete-file content {value:?}"),
                    ));
                }
            };
            Ok(IcebergDeleteFile {
                full_path: file.full_path.clone(),
                file_format: IcebergFileFormat::Parquet,
                file_content,
                length: file.length,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let deletion_vector_descriptor =
        src.deletion_vector_descriptor
            .as_ref()
            .map(|value| DeletionVectorDescriptor {
                storage_type: value.storage_type.clone(),
                path_or_inline_dv: value.path_or_inline_dv.clone(),
                offset: value.offset,
                size_in_bytes: value.size_in_bytes,
                cardinality: value.cardinality,
            });
    let empty_extended_columns = BTreeMap::new();
    let ivm_change_op = decode_change_op_extended_column(
        -1,
        src.extended_columns
            .as_ref()
            .unwrap_or(&empty_extended_columns),
        change_op_slot,
    )
    .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path.clone(), detail))?;
    Ok(FileScanRange {
        file_format,
        full_path,
        relative_path: src.relative_path.clone(),
        table_id: src.table_id,
        offset: src.offset.unwrap_or_default(),
        length: src.length.unwrap_or_default(),
        file_length: src.file_length.unwrap_or_default(),
        delete_files,
        deletion_vector_descriptor,
        first_row_id: src.first_row_id,
        data_sequence_number: src.data_sequence_number,
        modification_time: src.modification_time,
        datacache_options: src
            .datacache_options
            .as_ref()
            .map(|value| DatacacheOptions {
                enable_populate_datacache: value.enable_populate_datacache,
                priority: value.priority,
            }),
        candidate_node: src.candidate_node.clone(),
        included_positions: src.included_positions.clone().unwrap_or_default(),
        serialized_split: src.serialized_split.clone(),
        use_iceberg_jni_metadata_reader: src.use_iceberg_jni_metadata_reader.unwrap_or(false),
        ivm_change_op,
        file_pruning_min_max_values: None,
    })
}

fn decode_change_op_slots(
    nodes: &[plan_nodes::TPlanNode],
    descriptors: Option<&descriptors::TDescriptorTable>,
) -> Result<BTreeMap<i32, Option<SlotId>>, StarRocksFragmentDecodeError> {
    let mut output = BTreeMap::new();
    for (node_index, node) in nodes.iter().enumerate() {
        if node.node_type != plan_nodes::TPlanNodeType::HDFS_SCAN_NODE {
            continue;
        }
        let Some(hdfs) = node.hdfs_scan_node.as_ref() else {
            continue;
        };
        let extended = hdfs.extended_slot_ids.as_deref().unwrap_or(&[]);
        let Some(descriptors) = descriptors else {
            output.insert(node.node_id, None);
            continue;
        };
        let mut selected = None;
        for slot in descriptors.slot_descriptors.as_deref().unwrap_or(&[]) {
            let (Some(raw_slot_id), Some(parent)) = (slot.id, slot.parent) else {
                continue;
            };
            if !extended.contains(&raw_slot_id)
                || hdfs.tuple_id.is_some_and(|tuple_id| tuple_id != parent)
                || !super::layout::slot_name_from_desc(slot)
                    .is_some_and(|name| crate::exec::row_position::is_change_op(&name))
            {
                continue;
            }
            let slot_id = SlotId::try_from(raw_slot_id).map_err(|detail| {
                StarRocksFragmentDecodeError::invalid_value(
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("plan")
                        .field("nodes")
                        .index(node_index)
                        .field("hdfs_scan_node")
                        .field("extended_slot_ids"),
                    detail,
                )
            })?;
            if selected.replace(slot_id).is_some() {
                return Err(StarRocksFragmentDecodeError::invalid_value(
                    FieldPath::root("exec_plan_fragment")
                        .field("fragment")
                        .field("plan")
                        .field("nodes")
                        .index(node_index)
                        .field("hdfs_scan_node")
                        .field("extended_slot_ids"),
                    format!(
                        "HDFS_SCAN_NODE node_id={} has multiple __change_op extended slots",
                        node.node_id
                    ),
                ));
            }
        }
        output.insert(node.node_id, selected);
    }
    Ok(output)
}

pub(crate) fn decode_change_op_extended_column(
    node_id: i32,
    extended_columns: &BTreeMap<i32, crate::thrift::exprs::TExpr>,
    change_op_slot: Option<SlotId>,
) -> Result<Option<i8>, String> {
    let Some(slot) = change_op_slot else {
        return Ok(None);
    };
    let slot_id = i32::try_from(slot.as_u32()).map_err(|_| {
        format!("HDFS_SCAN_NODE node_id={node_id} __change_op slot_id={slot} exceeds i32")
    })?;
    let context = || format!("HDFS_SCAN_NODE node_id={node_id} __change_op slot_id={slot_id}");
    let Some(expr) = extended_columns.get(&slot_id) else {
        return Ok(None);
    };
    if expr.nodes.len() != 1 {
        return Err(format!(
            "{} expects exactly one INT_LITERAL extended column node, got {}",
            context(),
            expr.nodes.len()
        ));
    }
    let node = &expr.nodes[0];
    if node.node_type != crate::thrift::exprs::TExprNodeType::INT_LITERAL {
        return Err(format!(
            "{} expects INT_LITERAL extended column, got {:?}",
            context(),
            node.node_type
        ));
    }
    if node.num_children != 0 {
        return Err(format!(
            "{} INT_LITERAL extended column expects 0 children, got {}",
            context(),
            node.num_children
        ));
    }
    let value = node
        .int_literal
        .as_ref()
        .ok_or_else(|| format!("{} INT_LITERAL missing int payload", context()))?
        .value;
    let value = i8::try_from(value)
        .map_err(|_| format!("{} value {} does not fit in int8", context(), value))?;
    crate::exec::change_op::validate_change_op_value(value)
        .map_err(|error| format!("{} invalid value: {error}", context()))?;
    Ok(Some(value))
}

pub fn decode_incremental_scan_ranges(
    node_id: i32,
    scan_ranges: &[internal_service::TScanRangeParams],
    change_op_slot: Option<SlotId>,
) -> Result<Vec<IncrementalScanRange>, StarRocksFragmentDecodeError> {
    scan_ranges
        .iter()
        .enumerate()
        .map(|(index, params)| {
            let path = FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("per_node_scan_ranges")
                .map_key(node_id.to_string())
                .index(index);
            if params.empty.unwrap_or(false) {
                return Ok(IncrementalScanRange::Empty {
                    has_more: params.has_more,
                });
            }
            let Some(hdfs) = params.scan_range.hdfs_scan_range.as_ref() else {
                return Ok(IncrementalScanRange::Other {
                    has_more: params.has_more,
                });
            };
            let empty_extended_columns = BTreeMap::new();
            let ivm_change_op = decode_change_op_extended_column(
                node_id,
                hdfs.extended_columns
                    .as_ref()
                    .unwrap_or(&empty_extended_columns),
                change_op_slot,
            )
            .map_err(|detail| StarRocksFragmentDecodeError::invalid_value(path.clone(), detail))?;
            let candidate_node = hdfs
                .candidate_node
                .as_ref()
                .map(|node| node.trim())
                .filter(|node| !node.is_empty())
                .map(str::to_string);
            let external_datacache = ExternalDataCacheRangeOptions {
                modification_time: hdfs.modification_time,
                enable_populate_datacache: hdfs
                    .datacache_options
                    .as_ref()
                    .and_then(|options| options.enable_populate_datacache),
                datacache_priority: hdfs
                    .datacache_options
                    .as_ref()
                    .and_then(|options| options.priority),
                candidate_node,
            };
            let external_datacache = (external_datacache.modification_time.is_some()
                || external_datacache.enable_populate_datacache.is_some()
                || external_datacache.datacache_priority.is_some()
                || external_datacache.candidate_node.is_some())
            .then_some(external_datacache);
            Ok(IncrementalScanRange::Hdfs {
                has_more: params.has_more,
                range: IncrementalHdfsScanRange {
                    file_format: hdfs.file_format.as_ref().map(|format| match *format {
                        descriptors::THdfsFileFormat::PARQUET => HdfsScanFileFormat::Parquet,
                        descriptors::THdfsFileFormat::ORC => HdfsScanFileFormat::Orc,
                        _ => HdfsScanFileFormat::Other,
                    }),
                    full_path: hdfs.full_path.clone(),
                    relative_path: hdfs.relative_path.clone(),
                    table_id: hdfs.table_id,
                    file_length: hdfs.file_length.unwrap_or(0),
                    offset: hdfs.offset.unwrap_or(0),
                    length: hdfs.length.unwrap_or(0),
                    first_row_id: hdfs.first_row_id,
                    ivm_change_op,
                    external_datacache,
                },
            })
        })
        .collect()
}

#[cfg(test)]
mod change_op_tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::thrift::exprs::{TExpr, TExprNode, TExprNodeType, TIntLiteral};

    fn expr_node(node_type: TExprNodeType, children: i32, value: Option<i64>) -> TExprNode {
        TExprNode {
            node_type,
            type_: types::TTypeDesc::new(Vec::<types::TTypeNode>::new()),
            opcode: None,
            num_children: children,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: value.map(TIntLiteral::new),
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: -1,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn columns(slot: i32, expr: TExpr) -> BTreeMap<i32, TExpr> {
        BTreeMap::from([(slot, expr)])
    }

    fn decode(slot: i32, columns: BTreeMap<i32, TExpr>) -> Result<Option<i8>, String> {
        decode_change_op_extended_column(41, &columns, Some(SlotId::try_from(slot).expect("slot")))
    }

    #[test]
    fn change_op_accepts_valid_selected_int_literal_and_ignores_unrelated_columns() {
        let mut values = columns(
            7,
            TExpr::new(vec![expr_node(TExprNodeType::INT_LITERAL, 0, Some(1))]),
        );
        values.insert(
            8,
            TExpr::new(vec![expr_node(TExprNodeType::STRING_LITERAL, 3, None)]),
        );

        assert_eq!(decode(7, values).expect("valid change op"), Some(1));
    }

    #[test]
    fn change_op_rejects_wrong_node_kind() {
        let error = decode(
            7,
            columns(
                7,
                TExpr::new(vec![expr_node(TExprNodeType::STRING_LITERAL, 0, None)]),
            ),
        )
        .expect_err("wrong kind must fail");
        assert!(error.contains("expects INT_LITERAL"), "{error}");
    }

    #[test]
    fn change_op_rejects_literal_with_children() {
        let error = decode(
            7,
            columns(
                7,
                TExpr::new(vec![expr_node(TExprNodeType::INT_LITERAL, 1, Some(1))]),
            ),
        )
        .expect_err("children must fail");
        assert!(error.contains("expects 0 children"), "{error}");
    }

    #[test]
    fn change_op_rejects_missing_int_payload() {
        let error = decode(
            7,
            columns(
                7,
                TExpr::new(vec![expr_node(TExprNodeType::INT_LITERAL, 0, None)]),
            ),
        )
        .expect_err("missing payload must fail");
        assert!(error.contains("missing int payload"), "{error}");
    }

    #[test]
    fn change_op_rejects_value_outside_i8() {
        let error = decode(
            7,
            columns(
                7,
                TExpr::new(vec![expr_node(TExprNodeType::INT_LITERAL, 0, Some(128))]),
            ),
        )
        .expect_err("out of i8 must fail");
        assert!(error.contains("does not fit in int8"), "{error}");
    }

    #[test]
    fn change_op_rejects_invalid_semantic_value() {
        let error = decode(
            7,
            columns(
                7,
                TExpr::new(vec![expr_node(TExprNodeType::INT_LITERAL, 0, Some(3))]),
            ),
        )
        .expect_err("invalid semantic value must fail");
        assert!(error.contains("invalid value"), "{error}");
    }

    #[test]
    fn initial_and_incremental_hdfs_decoders_share_selected_change_op_semantics() {
        let selected = SlotId::new(7);
        let mut hdfs = plan_nodes::THdfsScanRange::default();
        hdfs.file_format = Some(descriptors::THdfsFileFormat::PARQUET);
        hdfs.extended_columns = Some(columns(
            7,
            TExpr::new(vec![expr_node(TExprNodeType::INT_LITERAL, 0, Some(-1))]),
        ));
        hdfs.extended_columns.as_mut().expect("columns").insert(
            8,
            TExpr::new(vec![expr_node(TExprNodeType::STRING_LITERAL, 2, None)]),
        );

        let initial = decode_hdfs_scan_range(
            &hdfs,
            Some(selected),
            &StarRocksDecodeFacts::default(),
            FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("per_node_scan_ranges")
                .map_key("41")
                .index(0),
        )
        .expect("initial range");
        let params = internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(None, None, None, None, Some(hdfs), None, None),
            None,
            Some(false),
            Some(false),
        );
        let incremental = decode_incremental_scan_ranges(41, &[params], Some(selected))
            .expect("incremental range");
        let IncrementalScanRange::Hdfs { range, .. } = &incremental[0] else {
            panic!("expected HDFS incremental range");
        };

        assert_eq!(initial.ivm_change_op, Some(-1));
        assert_eq!(range.ivm_change_op, Some(-1));
    }
}
