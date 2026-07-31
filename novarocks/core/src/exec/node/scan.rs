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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::ExternalDataCacheRangeOptions;
use crate::connector::file_execution::FileScanRange;
use crate::connector::starrocks::scan::{LakeScanSchemaMeta, StarRocksScanRange};
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::ExprId;
use crate::exec::node::BoxedExecIter;
use crate::exec::row_position::{LakeRowPositionSpec, RowPositionSpec};
use crate::exec::runtime_filter::{RuntimeInFilter, RuntimeMembershipFilter, RuntimeMinMaxFilter};
use crate::runtime::profile::RuntimeProfile;
use novarocks_spi::connector::{ConnectorExecutionBinding, ConnectorSplit};

#[derive(Clone, Debug)]
pub enum ScanMorsel {
    FileRange {
        path: String,
        file_len: u64,
        offset: u64,
        length: u64,
        scan_range_id: i32,
        external_datacache: Option<ExternalDataCacheRangeOptions>,
    },
    StarRocksRange {
        index: usize,
        tablet_id: i64,
    },
    JdbcSingle,
    IcebergMetadata {
        index: usize,
    },
    /// Provider-neutral scheduled connector split. The generic core adapter
    /// resolves the index to an SPI-owned split; no provider payload appears
    /// in the core morsel contract.
    ConnectorSplit {
        index: usize,
        row_position: Option<ConnectorRowPosition>,
    },
    Schema {
        table_name: String,
    },
    Empty,
}

impl ScanMorsel {
    pub fn describe(&self) -> String {
        match self {
            ScanMorsel::FileRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                external_datacache,
            } => format!(
                "path={} file_len={} offset={} length={} scan_range_id={} external_datacache={:?}",
                path, file_len, offset, length, scan_range_id, external_datacache,
            ),
            ScanMorsel::StarRocksRange { index, tablet_id } => {
                format!("starrocks_range_index={index} tablet_id={tablet_id}")
            }
            ScanMorsel::JdbcSingle => "jdbc_single".to_string(),
            ScanMorsel::IcebergMetadata { index } => {
                format!("iceberg_metadata_index={index}")
            }
            ScanMorsel::ConnectorSplit {
                index,
                row_position,
            } => format!(
                "connector_split_index={index} row_position_range={}",
                row_position
                    .as_ref()
                    .map(|position| position.scan_range_id.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
            ScanMorsel::Schema { table_name } => format!("schema_table={table_name}"),
            ScanMorsel::Empty => "empty".to_string(),
        }
    }

    /// Returns legacy file metadata. Connector splits never expose provider
    /// file details to core execution.
    pub fn file_range(&self) -> Option<FileScanRange> {
        match self {
            Self::FileRange {
                path,
                file_len,
                offset,
                length,
                scan_range_id,
                external_datacache,
            } => Some(FileScanRange {
                path: path.clone(),
                file_len: *file_len,
                offset: *offset,
                length: *length,
                scan_range_id: *scan_range_id,
                external_datacache: external_datacache.clone(),
            }),
            _ => None,
        }
    }

    pub fn connector_row_position(&self) -> Option<&ConnectorRowPosition> {
        match self {
            Self::ConnectorSplit { row_position, .. } => row_position.as_ref(),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorRowPosition {
    pub scan_range_id: i32,
}

/// Query-local core binding for an engine late-materialization lookup. It is
/// deliberately provider-neutral: the split remains opaque and the bound
/// instance owns every table-format read semantic.
#[derive(Clone)]
pub struct ConnectorRowPositionLookup {
    pub(crate) binding: Arc<ConnectorExecutionBinding>,
    pub(crate) splits: HashMap<i32, ConnectorSplit>,
}

impl ConnectorRowPositionLookup {
    pub fn new_execution(
        binding: Arc<ConnectorExecutionBinding>,
        splits: HashMap<i32, ConnectorSplit>,
    ) -> Self {
        Self { binding, splits }
    }

    pub fn splits(&self) -> impl Iterator<Item = (&i32, &ConnectorSplit)> {
        self.splits.iter()
    }
}

#[derive(Clone, Debug, Default)]
pub struct ScanMorsels {
    pub morsels: Vec<ScanMorsel>,
    pub has_more: bool,
}

impl ScanMorsels {
    pub fn new(morsels: Vec<ScanMorsel>, has_more: bool) -> Self {
        Self { morsels, has_more }
    }

    pub fn ensure_non_empty(&mut self, accept_empty_scan_ranges: bool) {
        if accept_empty_scan_ranges {
            return;
        }
        if self.morsels.is_empty() {
            self.morsels.push(ScanMorsel::Empty);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HdfsScanFileFormat {
    Parquet,
    Orc,
    Other,
}

#[derive(Clone, Debug)]
pub struct IncrementalHdfsScanRange {
    pub file_format: Option<HdfsScanFileFormat>,
    pub full_path: Option<String>,
    pub relative_path: Option<String>,
    pub table_id: Option<i64>,
    pub file_length: i64,
    pub offset: i64,
    pub length: i64,
    pub first_row_id: Option<i64>,
    pub ivm_change_op: Option<i8>,
    pub external_datacache: Option<ExternalDataCacheRangeOptions>,
}

#[derive(Clone, Debug)]
pub enum IncrementalScanRange {
    Empty {
        has_more: Option<bool>,
    },
    Hdfs {
        has_more: Option<bool>,
        range: IncrementalHdfsScanRange,
    },
    Other {
        has_more: Option<bool>,
    },
}

impl IncrementalScanRange {
    pub fn has_more(&self) -> Option<bool> {
        match self {
            Self::Empty { has_more } | Self::Hdfs { has_more, .. } | Self::Other { has_more } => {
                *has_more
            }
        }
    }
}

#[derive(Clone)]
pub struct RuntimeFilterContext {
    inner: RuntimeFilterContextInner,
}

#[derive(Clone)]
enum RuntimeFilterContextInner {
    Static {
        in_filters: Vec<RuntimeInFilter>,
        membership_filters: Vec<RuntimeMembershipFilter>,
        min_max_filters: Vec<(i32, Arc<RuntimeMinMaxFilter>)>,
    },
}

impl RuntimeFilterContext {
    pub(crate) fn new(
        in_filters: Vec<RuntimeInFilter>,
        membership_filters: Vec<RuntimeMembershipFilter>,
    ) -> Self {
        Self {
            inner: RuntimeFilterContextInner::Static {
                in_filters,
                membership_filters,
                min_max_filters: Vec::new(),
            },
        }
    }

    pub(crate) fn with_min_max_filters(
        in_filters: Vec<RuntimeInFilter>,
        membership_filters: Vec<RuntimeMembershipFilter>,
        min_max_filters: Vec<(i32, Arc<RuntimeMinMaxFilter>)>,
    ) -> Self {
        Self {
            inner: RuntimeFilterContextInner::Static {
                in_filters,
                membership_filters,
                min_max_filters,
            },
        }
    }

    pub(crate) fn in_filters(&self) -> &[RuntimeInFilter] {
        match &self.inner {
            RuntimeFilterContextInner::Static { in_filters, .. } => in_filters,
        }
    }

    pub(crate) fn membership_filters(&self) -> &[RuntimeMembershipFilter] {
        match &self.inner {
            RuntimeFilterContextInner::Static {
                membership_filters, ..
            } => membership_filters,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.in_filters().is_empty()
            && self.membership_filters().is_empty()
            && self.min_max_filters().is_empty()
    }

    pub(crate) fn min_max_filters(&self) -> Vec<(i32, Arc<RuntimeMinMaxFilter>)> {
        match &self.inner {
            RuntimeFilterContextInner::Static {
                min_max_filters, ..
            } => min_max_filters.clone(),
        }
    }
}

impl Default for RuntimeFilterContext {
    fn default() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

impl std::fmt::Debug for RuntimeFilterContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeFilterContext")
            .field("in_filters", &self.in_filters().len())
            .field("membership_filters", &self.membership_filters().len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScanMorselPruneDecision {
    Keep,
    Skip,
}

pub trait ScanOp: Send + Sync {
    /// Starts terminal cleanup for readers owned by this scan operation. The
    /// default keeps non-connector scan operators source-compatible.
    fn terminate(&self) -> Result<(), String> {
        Ok(())
    }

    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String>;

    fn profile_name(&self) -> Option<String> {
        None
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        false
    }

    fn build_incremental_morsels(
        &self,
        _scan_ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        Err("incremental scan ranges are not supported for this scan node".to_string())
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String>;

    /// Return the storage-tablet identity associated with a provider-neutral
    /// scheduled split, when that split participates in lake row positioning.
    fn storage_tablet_id(&self, _morsel: &ScanMorsel) -> Result<Option<i64>, String> {
        Ok(None)
    }
}

/// Instance-decoded, proto-free connector ranges handed to [`ScanSource::bind`].
///
/// The wire -> connector-range conversion (and its native/compat divergence)
/// lives in the decoders (`protocol/*/decode`); this enum is that conversion's
/// already-enriched output. Keeping it proto-free lets the wire-free connector
/// layer materialize a per-instance [`ScanOp`] from static config plus these
/// ranges.
///
/// Every variant is consumed at execution time: the decoders route the
/// enriched ranges into the instance's `ScanAssignment`, and
/// `materialize_scan_bindings` replays them through `ScanSource::bind` to
/// produce the per-instance `ScanOp`.
#[derive(Clone, Debug)]
pub enum BoundScanRanges {
    /// No ranges; the op emits a single morsel (jdbc/mysql).
    None,
    /// Schema scans carry only the per-instance assignment gate.
    SchemaSelection { should_scan: bool },
    /// File-based (HDFS / Iceberg data) ranges.
    File {
        ranges: Vec<FileScanRange>,
        has_more: bool,
    },
}

/// Static, proto-free description of a scan source that materializes a
/// per-instance [`ScanOp`] from [`BoundScanRanges`].
///
/// `bind` performs only "connector-ranges + static config -> op"; all wire
/// decoding happens earlier in the decoders. This keeps the connector layer
/// uniform and free of proto/thrift types. Later KRN-1 phases store an
/// `Arc<dyn ScanSource>` on `ScanNode` and call `bind` at execution time.
pub trait ScanSource: Send + Sync {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String>;

    fn profile_name(&self) -> Option<String> {
        None
    }
}

// Compile-time object-safety assertion for `ScanSource`.
#[cfg(test)]
const _: fn(&dyn ScanSource) = |_scan_source: &dyn ScanSource| {};

/// Metadata needed to re-scan a lake tablet for late materialization lookups.
#[derive(Clone, Debug)]
pub struct LakeGlmScanInfo {
    pub ranges: Vec<StarRocksScanRange>,
    pub properties: BTreeMap<String, String>,
    pub lake_schema_meta: Option<LakeScanSchemaMeta>,
}

#[derive(Clone)]
pub struct ScanNode {
    source: Arc<dyn ScanSource>,
    node_id: Option<i32>,
    native_runtime_filter_specs:
        Vec<crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding>,
    conjunct_predicate: Option<ExprId>,
    output_chunk_schema: ChunkSchemaRef,
    connector_io_tasks_per_scan_operator: Option<i32>,
    /// Scan-level limit for early termination optimization.
    /// When set, scan operators will stop reading new morsels after outputting this many rows.
    limit: Option<usize>,
    accept_empty_scan_ranges: bool,
    row_position: Option<RowPositionSpec>,
    connector_row_position_lookup: Option<ConnectorRowPositionLookup>,
    lake_row_position: Option<LakeRowPositionSpec>,
    lake_glm_info: Option<LakeGlmScanInfo>,
}

/// Test-only static source that binds to a fixed, pre-built op regardless of
/// ranges. Lets operator/decoder tests keep hand-rolling a `ScanOp` and drop it
/// onto a static `ScanNode` without a real connector source.
#[cfg(test)]
struct FixedOpScanSource(Arc<dyn ScanOp>);

#[cfg(test)]
impl ScanSource for FixedOpScanSource {
    fn bind(&self, _ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        Ok(Arc::clone(&self.0))
    }
}

impl ScanNode {
    pub fn new(source: Arc<dyn ScanSource>) -> Self {
        Self {
            source,
            node_id: None,
            native_runtime_filter_specs: Vec::new(),
            conjunct_predicate: None,
            output_chunk_schema: Arc::new(ChunkSchema::empty()),
            connector_io_tasks_per_scan_operator: None,
            limit: None,
            accept_empty_scan_ranges: false,
            row_position: None,
            connector_row_position_lookup: None,
            lake_row_position: None,
            lake_glm_info: None,
        }
    }

    /// Test-only: build a static node whose source binds to `op` regardless of
    /// ranges. Mirrors the pre-Phase-3 `ScanNode::new(op)` ergonomics for tests.
    #[cfg(test)]
    pub(crate) fn new_for_test(op: Arc<dyn ScanOp>) -> Self {
        Self::new(Arc::new(FixedOpScanSource(op)))
    }

    pub fn with_node_id(mut self, node_id: i32) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub(crate) fn set_native_runtime_filter_specs(
        &mut self,
        specs: Vec<crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding>,
    ) {
        self.native_runtime_filter_specs = specs;
    }

    pub fn with_runtime_filter_consumers(
        mut self,
        specs: Vec<crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding>,
    ) -> Self {
        self.native_runtime_filter_specs = specs;
        self
    }

    pub fn with_output_chunk_schema(mut self, output_chunk_schema: ChunkSchemaRef) -> Self {
        self.output_chunk_schema = output_chunk_schema;
        self
    }

    pub fn with_connector_io_tasks_per_scan_operator(mut self, value: Option<i32>) -> Self {
        self.connector_io_tasks_per_scan_operator = value;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_accept_empty_scan_ranges(mut self, value: bool) -> Self {
        self.accept_empty_scan_ranges = value;
        self
    }

    pub fn with_row_position(mut self, spec: Option<RowPositionSpec>) -> Self {
        self.row_position = spec;
        self
    }

    pub fn with_connector_row_position_lookup(
        mut self,
        lookup: Option<ConnectorRowPositionLookup>,
    ) -> Self {
        self.connector_row_position_lookup = lookup;
        self
    }

    pub fn with_lake_row_position(mut self, spec: Option<LakeRowPositionSpec>) -> Self {
        self.lake_row_position = spec;
        self
    }

    pub fn with_lake_glm_info(mut self, info: Option<LakeGlmScanInfo>) -> Self {
        self.lake_glm_info = info;
        self
    }

    pub fn node_id(&self) -> Option<i32> {
        self.node_id
    }

    /// The static scan source. The per-instance `ScanOp` is materialized from
    /// this plus the instance's `BoundScanRanges` at execution time
    /// (`materialize_scan_bindings`), not stored on the node.
    pub fn source(&self) -> Arc<dyn ScanSource> {
        Arc::clone(&self.source)
    }

    pub(crate) fn native_runtime_filter_specs(
        &self,
    ) -> &[crate::exec::node::runtime_filter::RuntimeFilterConsumerBinding] {
        &self.native_runtime_filter_specs
    }

    pub fn output_chunk_schema(&self) -> ChunkSchemaRef {
        Arc::clone(&self.output_chunk_schema)
    }

    pub fn conjunct_predicate(&self) -> Option<ExprId> {
        self.conjunct_predicate
    }

    pub fn with_conjunct_predicate(mut self, predicate: Option<ExprId>) -> Self {
        self.conjunct_predicate = predicate;
        self
    }

    pub fn set_conjunct_predicate(&mut self, predicate: Option<ExprId>) {
        self.conjunct_predicate = predicate;
    }

    pub fn connector_io_tasks_per_scan_operator(&self) -> Option<i32> {
        self.connector_io_tasks_per_scan_operator
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn accept_empty_scan_ranges(&self) -> bool {
        self.accept_empty_scan_ranges
    }

    pub fn row_position(&self) -> Option<&RowPositionSpec> {
        self.row_position.as_ref()
    }

    pub(crate) fn connector_row_position_lookup(&self) -> Option<&ConnectorRowPositionLookup> {
        self.connector_row_position_lookup.as_ref()
    }

    pub fn lake_row_position(&self) -> Option<&LakeRowPositionSpec> {
        self.lake_row_position.as_ref()
    }

    pub fn lake_glm_info(&self) -> Option<&LakeGlmScanInfo> {
        self.lake_glm_info.as_ref()
    }
}

impl std::fmt::Debug for ScanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanNode")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ConnectorRowPosition, RuntimeFilterContext, ScanMorsel};
    use crate::exec::runtime_filter::{RuntimeFilterType, RuntimeMinMaxFilter};

    #[test]
    fn runtime_filter_context_preserves_min_max_filters() {
        let ctx = RuntimeFilterContext::with_min_max_filters(
            Vec::new(),
            Vec::new(),
            vec![(
                7,
                Arc::new(
                    RuntimeMinMaxFilter::empty_range(RuntimeFilterType::Int32)
                        .expect("empty min/max filter"),
                ),
            )],
        );

        assert_eq!(ctx.min_max_filters().len(), 1);
    }

    #[test]
    fn connector_split_keeps_only_generic_row_position_metadata() {
        let morsel = ScanMorsel::ConnectorSplit {
            index: 3,
            row_position: Some(ConnectorRowPosition { scan_range_id: 5 }),
        };
        assert_eq!(
            morsel.connector_row_position(),
            Some(&ConnectorRowPosition { scan_range_id: 5 })
        );
        assert!(morsel.file_range().is_none());
    }
}
