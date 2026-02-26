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
use std::sync::Arc;

use crate::cache::ExternalDataCacheRangeOptions;
use crate::common::ids::SlotId;
use crate::descriptors;
use crate::exec::expr::ExprId;
use crate::exec::node::{BoxedExecIter, RuntimeFilterProbeSpec};
use crate::exec::row_position::RowPositionSpec;
use crate::exec::runtime_filter::{RuntimeInFilter, RuntimeMembershipFilter};
use crate::fs::scan_context::FileScanRange;
use crate::novarocks_logging::warn;
use crate::runtime::profile::RuntimeProfile;
use crate::runtime::runtime_filter_hub::{RuntimeFilterHandle, RuntimeFilterSnapshot};

#[derive(Clone, Debug)]
pub enum ScanMorsel {
    FileRange {
        path: String,
        file_len: u64,
        offset: u64,
        length: u64,
        scan_range_id: i32,
        first_row_id: Option<i64>,
        external_datacache: Option<ExternalDataCacheRangeOptions>,
    },
    StarRocksRange {
        index: usize,
    },
    JdbcSingle,
    Exchange,
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
                first_row_id,
                external_datacache,
            } => format!(
                "path={} file_len={} offset={} length={} scan_range_id={} first_row_id={:?} external_datacache={:?}",
                path, file_len, offset, length, scan_range_id, first_row_id, external_datacache
            ),
            ScanMorsel::StarRocksRange { index } => format!("starrocks_range_index={index}"),
            ScanMorsel::JdbcSingle => "jdbc_single".to_string(),
            ScanMorsel::Exchange => "exchange".to_string(),
            ScanMorsel::Empty => "empty".to_string(),
        }
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

#[derive(Clone)]
pub struct RuntimeFilterContext {
    inner: RuntimeFilterContextInner,
}

#[derive(Clone)]
enum RuntimeFilterContextInner {
    Static {
        in_filters: Vec<RuntimeInFilter>,
        membership_filters: Vec<RuntimeMembershipFilter>,
    },
    Handle {
        handle: RuntimeFilterHandle,
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
            },
        }
    }

    pub(crate) fn from_handle(handle: RuntimeFilterHandle) -> Self {
        Self {
            inner: RuntimeFilterContextInner::Handle { handle },
        }
    }

    pub(crate) fn snapshot(&self) -> RuntimeFilterSnapshot {
        match &self.inner {
            RuntimeFilterContextInner::Static {
                in_filters,
                membership_filters,
            } => {
                RuntimeFilterSnapshot::from_filters(in_filters.clone(), membership_filters.clone())
            }
            RuntimeFilterContextInner::Handle { handle } => handle.snapshot(),
        }
    }

    pub(crate) fn version(&self) -> u64 {
        match &self.inner {
            RuntimeFilterContextInner::Static { .. } => 0,
            RuntimeFilterContextInner::Handle { handle } => handle.version(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.snapshot().is_empty()
    }
}

impl Default for RuntimeFilterContext {
    fn default() -> Self {
        Self::new(Vec::new(), Vec::new())
    }
}

impl std::fmt::Debug for RuntimeFilterContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("RuntimeFilterContext")
            .field("in_filters", &snapshot.in_filters().len())
            .field("membership_filters", &snapshot.membership_filters().len())
            .finish()
    }
}

pub trait ScanOp: Send + Sync {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String>;

    fn profile_name(&self) -> Option<String> {
        None
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String>;
}

#[derive(Clone, Debug)]
pub struct RowPositionScanConfig {
    pub file_format: descriptors::THdfsFileFormat,
    pub case_sensitive: bool,
    pub batch_size: Option<usize>,
    pub enable_file_metacache: bool,
    pub enable_file_pagecache: bool,
    /// OSS credentials for re-scanning the source file during late-materialisation lookups.
    /// `None` for local / HDFS paths; must be `Some` for `oss://` / `s3://` paths.
    pub oss_config: Option<crate::fs::object_store::ObjectStoreConfig>,
}

#[derive(Clone)]
pub struct ScanNode {
    op: Arc<dyn ScanOp>,
    node_id: Option<i32>,
    runtime_filter_specs: Vec<RuntimeFilterProbeSpec>,
    conjunct_predicate: Option<ExprId>,
    output_slots: Vec<SlotId>,
    connector_io_tasks_per_scan_operator: Option<i32>,
    /// Scan-level limit for early termination optimization.
    /// When set, scan operators will stop reading new morsels after outputting this many rows.
    limit: Option<usize>,
    local_rf_waiting_set: Vec<i32>,
    accept_empty_scan_ranges: bool,
    row_position: Option<RowPositionSpec>,
    row_position_scan: Option<RowPositionScanConfig>,
    row_position_ranges: Option<Vec<FileScanRange>>,
}

impl ScanNode {
    pub fn new(op: Arc<dyn ScanOp>) -> Self {
        Self {
            op,
            node_id: None,
            runtime_filter_specs: Vec::new(),
            conjunct_predicate: None,
            output_slots: Vec::new(),
            connector_io_tasks_per_scan_operator: None,
            limit: None,
            local_rf_waiting_set: Vec::new(),
            accept_empty_scan_ranges: false,
            row_position: None,
            row_position_scan: None,
            row_position_ranges: None,
        }
    }

    pub fn with_node_id(mut self, node_id: i32) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub fn with_runtime_filter_specs(mut self, specs: Vec<RuntimeFilterProbeSpec>) -> Self {
        self.add_runtime_filter_specs(&specs);
        self
    }

    pub fn with_output_slots(mut self, output_slots: Vec<SlotId>) -> Self {
        self.output_slots = output_slots;
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

    pub fn with_local_rf_waiting_set(mut self, waiting_set: Vec<i32>) -> Self {
        if waiting_set.is_empty() {
            return self;
        }
        let mut seen = HashMap::new();
        for id in waiting_set {
            seen.entry(id).or_insert(());
        }
        self.local_rf_waiting_set = seen.keys().copied().collect();
        self.local_rf_waiting_set.sort_unstable();
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

    pub fn with_row_position_scan(mut self, cfg: Option<RowPositionScanConfig>) -> Self {
        self.row_position_scan = cfg;
        self
    }

    pub fn with_row_position_ranges(mut self, ranges: Option<Vec<FileScanRange>>) -> Self {
        self.row_position_ranges = ranges;
        self
    }

    pub fn node_id(&self) -> Option<i32> {
        self.node_id
    }

    pub fn runtime_filter_specs(&self) -> &[RuntimeFilterProbeSpec] {
        &self.runtime_filter_specs
    }

    pub fn output_slots(&self) -> &[SlotId] {
        &self.output_slots
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

    pub fn local_rf_waiting_set(&self) -> &[i32] {
        &self.local_rf_waiting_set
    }

    pub fn accept_empty_scan_ranges(&self) -> bool {
        self.accept_empty_scan_ranges
    }

    pub fn row_position(&self) -> Option<&RowPositionSpec> {
        self.row_position.as_ref()
    }

    pub fn row_position_scan(&self) -> Option<&RowPositionScanConfig> {
        self.row_position_scan.as_ref()
    }

    pub fn row_position_ranges(&self) -> Option<&[FileScanRange]> {
        self.row_position_ranges.as_deref()
    }

    pub fn add_runtime_filter_specs(&mut self, specs: &[RuntimeFilterProbeSpec]) {
        if specs.is_empty() {
            return;
        }
        let mut seen: HashMap<i32, RuntimeFilterProbeSpec> = HashMap::new();
        for spec in &self.runtime_filter_specs {
            seen.insert(spec.filter_id, spec.clone());
        }
        for spec in specs {
            if let Some(existing) = seen.get(&spec.filter_id) {
                if existing.slot_id != spec.slot_id {
                    warn!(
                        "scan runtime filter spec mismatch: filter_id={} existing_slot_id={:?} new_slot_id={:?}",
                        spec.filter_id, existing.slot_id, spec.slot_id
                    );
                }
                continue;
            }
            self.runtime_filter_specs.push(spec.clone());
            seen.insert(spec.filter_id, spec.clone());
        }
    }

    pub fn profile_name(&self) -> Option<String> {
        self.op.profile_name()
    }

    pub fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        if matches!(morsel, ScanMorsel::Empty) {
            return Ok(Box::new(std::iter::empty()));
        }
        self.op.execute_iter(morsel, profile, runtime_filters)
    }

    pub fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let mut morsels = self.op.build_morsels()?;
        morsels.ensure_non_empty(self.accept_empty_scan_ranges);
        Ok(morsels)
    }
}

impl std::fmt::Debug for ScanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanNode")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}
