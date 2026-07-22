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

use crate::engine::StandaloneState;
use crate::mv::refresh::pin::RefreshSnapshotPin;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) fn capture_refresh_snapshot_pin(
    state: &Arc<StandaloneState>,
    base_refs: &[TableIdentity],
) -> Result<RefreshSnapshotPin, String> {
    let mut entries = Vec::with_capacity(base_refs.len());
    for base_ref in base_refs {
        let loaded =
            crate::connector::starrocks::table::mv_refresh::load_current_iceberg_base_table(
                state, base_ref,
            )?;
        let snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                format!(
                    "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
                    base_ref.fqn()
                )
            })?;
        entries.push((
            base_ref.clone(),
            snapshot_id,
            loaded.table.metadata().uuid().to_string(),
        ));
    }
    Ok(RefreshSnapshotPin::from_captured_entries(entries))
}
