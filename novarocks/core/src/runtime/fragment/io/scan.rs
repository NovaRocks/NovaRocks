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

use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::exec::node::scan::ConnectorRowPositionLookup;
use crate::exec::node::scan::ScanOp;
use crate::exec::operators::scan::ScanDispatchState;
use crate::runtime::query_context::QueryId;
use novarocks_types::SlotId;

/// Host-owned registration capability needed by scan operators.
///
/// Query-lifecycle registries are deliberately not part of the execution
/// kernel. Backend admission supplies this narrow port for the two scan
/// registrations that must remain visible to the query owner.
pub trait ScanRegistrationPort: Send + Sync + 'static {
    fn register_row_position_lookup(
        &self,
        query_id: QueryId,
        row_source_slot: SlotId,
        lookup: ConnectorRowPositionLookup,
    ) -> Result<(), String>;

    fn register_incremental_scan(
        &self,
        fragment_instance_id: UniqueId,
        node_id: i32,
        op: Arc<dyn ScanOp>,
        dispatch: Arc<ScanDispatchState>,
    ) -> Result<(), String>;
}

#[derive(Debug, Default)]
pub struct UnavailableScanRegistrationPort;

impl ScanRegistrationPort for UnavailableScanRegistrationPort {
    fn register_row_position_lookup(
        &self,
        _query_id: QueryId,
        _row_source_slot: SlotId,
        _lookup: ConnectorRowPositionLookup,
    ) -> Result<(), String> {
        Err("scan registration port is unavailable".to_string())
    }

    fn register_incremental_scan(
        &self,
        _fragment_instance_id: UniqueId,
        _node_id: i32,
        _op: Arc<dyn ScanOp>,
        _dispatch: Arc<ScanDispatchState>,
    ) -> Result<(), String> {
        Err("scan registration port is unavailable".to_string())
    }
}
