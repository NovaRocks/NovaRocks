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

//! Fragment runtime inputs for native physical-plan decoding.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::exec::fragment::program::FragmentNodeId;
use novarocks_execution::exec::node::scan::BoundScanRanges;
use novarocks_execution::runtime::exchange::ExchangeKey;
#[cfg(test)]
use novarocks_execution::runtime::fragment::ExchangeInputAssignment;
use novarocks_execution::runtime::fragment::{ExchangeInputAssignments, FragmentInstanceId};
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_functions::EngineFunctionCatalog;
#[cfg(test)]
use novarocks_native_adapter::fragment_instance::decode_scan_range_params;
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_spi::connector::ConnectorCancellation;
use novarocks_types::QueryId;
use novarocks_worker::TypedScanRuntime;

use novarocks_native_adapter::fragment_error::NativeFragmentLeafDecodeError;

/// All non-wire dependencies required while lowering one native fragment.
///
/// The query-scoped cancellation handle is deliberately opaque. This type
/// never accepts a Core query manager or resolves cancellation itself.
#[derive(Clone)]
#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
pub(crate) struct NativePlanDecodeContext {
    exchange_inputs: ExchangeInputAssignments,
    raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    captured_scan_ranges: RefCell<BTreeMap<FragmentNodeId, BoundScanRanges>>,
    query_options: Option<QueryOptions>,
    connector_cancellation: Option<Arc<dyn ConnectorCancellation>>,
    query_id: Option<QueryId>,
    fragment_instance_id: FragmentInstanceId,
    /// Exchange-source wait resolved from `[runtime]` at Backend startup.
    exchange_wait: Duration,
    /// Absent for a fragment with no typed connector scan; a typed scan that
    /// finds it absent fails closed rather than inventing a registry.
    typed_scan_runtime: Option<TypedScanRuntime>,
    function_catalog: Option<Arc<EngineFunctionCatalog>>,
}

impl Default for NativePlanDecodeContext {
    fn default() -> Self {
        Self {
            exchange_inputs: ExchangeInputAssignments::default(),
            raw_scan_ranges: BTreeMap::new(),
            captured_scan_ranges: RefCell::new(BTreeMap::new()),
            query_options: None,
            connector_cancellation: None,
            query_id: None,
            fragment_instance_id: FragmentInstanceId::new(novarocks_types::UniqueId::new(0, 0)),
            exchange_wait: Duration::from_millis(120_000),
            typed_scan_runtime: None,
            function_catalog: None,
        }
    }
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl NativePlanDecodeContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        exchange_inputs: ExchangeInputAssignments,
        raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
        query_options: QueryOptions,
        connector_cancellation: Arc<dyn ConnectorCancellation>,
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
        exchange_wait: Duration,
    ) -> Self {
        Self {
            exchange_inputs,
            raw_scan_ranges,
            captured_scan_ranges: RefCell::new(BTreeMap::new()),
            query_options: Some(query_options),
            connector_cancellation: Some(connector_cancellation),
            query_id: Some(query_id),
            fragment_instance_id,
            exchange_wait,
            typed_scan_runtime: None,
            function_catalog: None,
        }
    }

    pub(crate) fn with_typed_scan_runtime(mut self, runtime: Option<TypedScanRuntime>) -> Self {
        self.typed_scan_runtime = runtime;
        self
    }

    pub(crate) fn with_function_catalog(mut self, catalog: Arc<EngineFunctionCatalog>) -> Self {
        self.function_catalog = Some(catalog);
        self
    }

    pub(crate) fn function_catalog(&self) -> Option<&EngineFunctionCatalog> {
        self.function_catalog.as_deref()
    }

    pub(crate) fn typed_scan_runtime(&self) -> Option<&TypedScanRuntime> {
        self.typed_scan_runtime.as_ref()
    }

    pub(crate) fn capture_scan_ranges(&self, node_id: i32, ranges: BoundScanRanges) {
        self.captured_scan_ranges
            .borrow_mut()
            .insert(FragmentNodeId::new(node_id), ranges);
    }

    pub(crate) fn take_captured_scan_ranges(&self) -> BTreeMap<FragmentNodeId, BoundScanRanges> {
        std::mem::take(&mut self.captured_scan_ranges.borrow_mut())
    }

    #[cfg(test)]
    pub(crate) fn captured_ranges_for_test(&self, node_id: i32) -> Option<BoundScanRanges> {
        self.captured_scan_ranges
            .borrow()
            .get(&FragmentNodeId::new(node_id))
            .cloned()
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[ScanRangeParams], NativeFragmentLeafDecodeError> {
        self.raw_scan_ranges
            .get(&FragmentNodeId::new(node_id))
            .map(Vec::as_slice)
            .ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(
                    novarocks_proto_codec::ProtocolErrorKind::MissingField,
                    "scan_ranges",
                    format!("native ScanNode node_id={node_id} missing scan ranges"),
                )
            })
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }
    pub(crate) fn query_id(&self) -> Option<QueryId> {
        self.query_id
    }

    pub(crate) fn exchange_wait(&self) -> Duration {
        self.exchange_wait
    }

    pub(crate) fn fragment_instance_id(&self) -> FragmentInstanceId {
        self.fragment_instance_id
    }

    pub(crate) fn connector_cancellation(
        &self,
    ) -> Result<Arc<dyn ConnectorCancellation>, NativeFragmentLeafDecodeError> {
        self.connector_cancellation.clone().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                novarocks_proto_codec::ProtocolErrorKind::MissingField,
                "connector_cancellation",
                "native typed connector scan requires an execution cancellation capability",
            )
        })
    }

    pub(crate) fn exchange_input(
        &self,
        node_id: i32,
    ) -> Result<(ExchangeKey, usize), NativeFragmentLeafDecodeError> {
        let assignment = self
            .exchange_inputs
            .get(&FragmentNodeId::new(node_id))
            .ok_or_else(|| {
                NativeFragmentLeafDecodeError::at_field(
                    novarocks_proto_codec::ProtocolErrorKind::MissingField,
                    "exchange_inputs",
                    format!("ExchangeReceiver missing sender count for node_id {node_id}"),
                )
            })?;
        let fragment_instance_id = self.fragment_instance_id.get();
        Ok((
            ExchangeKey {
                finst_id_hi: fragment_instance_id.high(),
                finst_id_lo: fragment_instance_id.low(),
                node_id,
            },
            assignment.sender_count().get(),
        ))
    }

    #[cfg(test)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        let count = std::num::NonZeroUsize::new(count).expect("test sender count must be positive");
        self.fragment_instance_id = FragmentInstanceId::new(novarocks_types::UniqueId::new(
            key.finst_id_hi,
            key.finst_id_lo,
        ));
        self.exchange_inputs = ExchangeInputAssignments::new(BTreeMap::from([(
            FragmentNodeId::new(key.node_id),
            ExchangeInputAssignment::new(count),
        )]));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_connector_cancellation(
        mut self,
        cancellation: Arc<dyn ConnectorCancellation>,
    ) -> Self {
        self.connector_cancellation = Some(cancellation);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_query_id(mut self, query_id: QueryId) -> Self {
        self.query_id = Some(query_id);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_fragment_instance_id(mut self, id: novarocks_types::UniqueId) -> Self {
        self.fragment_instance_id = FragmentInstanceId::new(id);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks_proto_models::novarocks::ScanRangeParams>,
    ) -> Self {
        let ranges = ranges
            .iter()
            .map(decode_scan_range_params)
            .collect::<Result<Vec<_>, _>>()
            .expect("decode test scan ranges");
        self.raw_scan_ranges
            .insert(FragmentNodeId::new(node_id), ranges);
        self
    }
}
