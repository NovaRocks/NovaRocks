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

//! Explicit backend runtime inputs for native physical-plan decoding.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::native::expression::NativeExpressionInputLayout;
use novarocks::connector::ConnectorRegistry;
use novarocks::exec::expr::{ExprArena, ExprId};
use novarocks::exec::fragment::program::FragmentNodeId;
use novarocks::exec::node::scan::BoundScanRanges;
use novarocks::protocol::FieldPath;
use novarocks::runtime::exchange::ExchangeKey;
use novarocks::runtime::fragment::{
    ExchangeInputAssignment, ExchangeInputAssignments, FragmentInstanceId,
};
use novarocks::runtime::query_options::QueryOptions;
use novarocks::runtime::scan_range::ScanRangeParams;
use novarocks_protocol::{common, expr};
use novarocks_spi::connector::{ConnectorCancellation, ConnectorExecutionResolver};
use novarocks_types::QueryId;

use crate::native::plan_decode::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use crate::native::plan_decode::layout::Layout;

/// All non-wire dependencies required while lowering one native fragment.
///
/// The query-scoped cancellation handle is deliberately opaque. This type
/// never accepts a Core query manager or resolves cancellation itself.
#[derive(Clone)]
pub(crate) struct NativePlanDecodeContext {
    exchange_inputs: ExchangeInputAssignments,
    raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    captured_scan_ranges: RefCell<BTreeMap<FragmentNodeId, BoundScanRanges>>,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<ConnectorRegistry>>,
    execution_resolver: Option<Arc<dyn ConnectorExecutionResolver>>,
    connector_cancellation: Option<Arc<dyn ConnectorCancellation>>,
    query_id: Option<QueryId>,
    fragment_instance_id: FragmentInstanceId,
}

impl Default for NativePlanDecodeContext {
    fn default() -> Self {
        Self {
            exchange_inputs: ExchangeInputAssignments::default(),
            raw_scan_ranges: BTreeMap::new(),
            captured_scan_ranges: RefCell::new(BTreeMap::new()),
            query_options: None,
            connectors: None,
            execution_resolver: None,
            connector_cancellation: None,
            query_id: None,
            fragment_instance_id: FragmentInstanceId::new(novarocks_types::UniqueId::new(0, 0)),
        }
    }
}

impl NativePlanDecodeContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        exchange_inputs: ExchangeInputAssignments,
        raw_scan_ranges: BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
        query_options: QueryOptions,
        connectors: Arc<ConnectorRegistry>,
        execution_resolver: Arc<dyn ConnectorExecutionResolver>,
        connector_cancellation: Arc<dyn ConnectorCancellation>,
        query_id: QueryId,
        fragment_instance_id: FragmentInstanceId,
    ) -> Self {
        Self {
            exchange_inputs,
            raw_scan_ranges,
            captured_scan_ranges: RefCell::new(BTreeMap::new()),
            query_options: Some(query_options),
            connectors: Some(connectors),
            execution_resolver: Some(execution_resolver),
            connector_cancellation: Some(connector_cancellation),
            query_id: Some(query_id),
            fragment_instance_id,
        }
    }

    pub(crate) fn decode_output_layout(
        &self,
        columns: &[common::OutputColumn],
        path: FieldPath,
    ) -> Result<crate::native::layout::NativeOutputLayout, NativeFragmentDecodeError> {
        crate::native::layout::decode_output_layout(columns, path)
            .map_err(NativeFragmentDecodeError::from)
    }

    pub(crate) fn decode_expression(
        &self,
        expression: &expr::Expr,
        path: FieldPath,
        arena: &mut ExprArena,
        layout: &Layout,
    ) -> Result<ExprId, NativeFragmentDecodeError> {
        let input = NativeExpressionInputLayout::from_slot_ids(layout.order().iter().copied());
        crate::native::expression::decode_expr_at(expression, path, arena, &input)
            .map_err(|error| NativeFragmentDecodeError::from(error.into_protocol()))
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
                    novarocks::protocol::ProtocolErrorKind::MissingField,
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

    pub(crate) fn fragment_instance_id(&self) -> FragmentInstanceId {
        self.fragment_instance_id
    }

    pub(crate) fn connectors(&self) -> Result<&ConnectorRegistry, NativeFragmentLeafDecodeError> {
        self.connectors.as_deref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                novarocks::protocol::ProtocolErrorKind::MissingField,
                "connector_registry",
                "native ScanNode requires ConnectorRegistry in NativePlanDecodeContext",
            )
        })
    }

    pub(crate) fn execution_resolver(
        &self,
    ) -> Result<&dyn ConnectorExecutionResolver, NativeFragmentLeafDecodeError> {
        self.execution_resolver.as_deref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                novarocks::protocol::ProtocolErrorKind::MissingField,
                "connector_execution_resolver",
                "native ConnectorReadSource requires a query-scoped execution resolver",
            )
        })
    }

    pub(crate) fn connector_cancellation(
        &self,
    ) -> Result<Arc<dyn ConnectorCancellation>, NativeFragmentLeafDecodeError> {
        self.connector_cancellation.clone().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                novarocks::protocol::ProtocolErrorKind::MissingField,
                "connector_cancellation",
                "native ConnectorReadSource requires an execution cancellation capability",
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
                    novarocks::protocol::ProtocolErrorKind::MissingField,
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
    pub(crate) fn with_execution_resolver(
        mut self,
        resolver: Arc<dyn ConnectorExecutionResolver>,
    ) -> Self {
        self.execution_resolver = Some(resolver);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_connector_registry(mut self, connectors: Arc<ConnectorRegistry>) -> Self {
        self.connectors = Some(connectors);
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
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks_protocol::novarocks::ScanRangeParams>,
    ) -> Self {
        let ranges = ranges
            .iter()
            .map(crate::native::plan_decode::instance::decode_scan_range_params)
            .collect::<Result<Vec<_>, _>>()
            .expect("decode test scan ranges");
        self.raw_scan_ranges
            .insert(FragmentNodeId::new(node_id), ranges);
        self
    }
}
