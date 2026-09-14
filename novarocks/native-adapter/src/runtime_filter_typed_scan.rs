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

//! Native typed-scan carrier projection.
//!
//! This adapter is the one place that sees both the validated carrier and the
//! provider-neutral decoded scan. It projects their matching variables into
//! Worker-owned filter bindings; all subscription and bounds semantics live in
//! `novarocks-worker`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use novarocks_execution::runtime_filter::{
    RuntimeFilterConsumerContract, RuntimeFilterContractViolation, RuntimeFilterSessionRef,
};
use novarocks_proto_codec::connector_read::{ConnectorTableScanSource, DecodedConnectorReadScan};
use novarocks_spi::connector::read_stack::{
    CompleteAllDynamicFilter, ConnectorReadColumnHandle, ConnectorReadDynamicFilter,
};
use novarocks_worker::runtime_filter::typed_scan::{
    TypedScanFilterBindings, typed_scan_dynamic_filter,
};
use novarocks_worker::typed_scan_filter::TypedScanLiveDynamicFilterFactory;

struct NativeTypedScanLiveDynamicFilterFactory {
    wire_scan: ConnectorTableScanSource,
    scan: DecodedConnectorReadScan,
}

impl TypedScanLiveDynamicFilterFactory for NativeTypedScanLiveDynamicFilterFactory {
    fn build(
        &self,
        session: Option<&RuntimeFilterSessionRef>,
        contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
    ) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation> {
        scan_dynamic_filter_spi(&self.wire_scan, &self.scan, session, contracts)
    }
}

pub fn typed_scan_live_dynamic_filter_factory(
    wire_scan: ConnectorTableScanSource,
    scan: DecodedConnectorReadScan,
) -> Arc<dyn TypedScanLiveDynamicFilterFactory> {
    Arc::new(NativeTypedScanLiveDynamicFilterFactory { wire_scan, scan })
}

/// Projects the carrier's dynamic-filter variable names onto decoded SPI
/// columns for a scan that receives no live runtime-filter feedback.
pub fn complete_all_scan_dynamic_filter(
    wire_scan: &ConnectorTableScanSource,
    scan: &DecodedConnectorReadScan,
) -> Arc<ConnectorReadDynamicFilter> {
    let filtered_variables: BTreeSet<&str> = wire_scan
        .dynamic_filters()
        .iter()
        .map(|binding| binding.variable())
        .collect();
    let covered: BTreeSet<ConnectorReadColumnHandle> = scan
        .assignments()
        .iter()
        .filter(|assignment| filtered_variables.contains(assignment.variable()))
        .map(|assignment| assignment.column().clone())
        .collect();
    Arc::new(CompleteAllDynamicFilter::new(covered))
}

pub fn scan_dynamic_filter_spi(
    wire_scan: &ConnectorTableScanSource,
    scan: &DecodedConnectorReadScan,
    session: Option<&RuntimeFilterSessionRef>,
    contracts: &BTreeMap<u32, RuntimeFilterConsumerContract>,
) -> Result<Arc<ConnectorReadDynamicFilter>, RuntimeFilterContractViolation> {
    let filter_by_variable: BTreeMap<&str, u32> = wire_scan
        .dynamic_filters()
        .iter()
        .map(|binding| (binding.variable(), binding.filter_id()))
        .collect();
    let bindings = TypedScanFilterBindings::from_filter_columns(
        scan.assignments().iter().filter_map(|assignment| {
            filter_by_variable
                .get(assignment.variable())
                .map(|filter_id| (*filter_id, assignment.column().clone()))
        }),
    );
    typed_scan_dynamic_filter(&bindings, session, contracts)
}
