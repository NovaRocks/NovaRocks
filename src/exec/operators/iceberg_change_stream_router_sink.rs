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
//! Typed Iceberg change-stream router sink.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, Int8Array, Int16Array, Int32Array, Int64Array, NullArray, UInt32Array};
use arrow::compute::take;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::lower::layout::Layout;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use crate::sql::common::{
    CHANGE_OP_DELETE, CHANGE_OP_INSERT, ChangeStreamBranchKind, ChangeStreamRouteKey,
    DATA_ROUTE_FRESH, DATA_ROUTE_REUSE, branch_kind_from_thrift,
};
use crate::thrift::{data_sinks, internal_service, types};

use super::DataStreamSinkFactory;

fn int32_value(array: &dyn Array, row: usize, label: &str) -> Result<Option<i32>, String> {
    if row >= array.len() {
        return Err(format!(
            "{label} row index {row} is out of bounds for length {}",
            array.len()
        ));
    }
    if array.as_any().downcast_ref::<NullArray>().is_some() {
        return Ok(None);
    }
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(Some(i32::from(values.value(row))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Some(i32::from(values.value(row))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(values.value(row)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return i32::try_from(values.value(row))
            .map(Some)
            .map_err(|_| format!("{label} value exceeds Int32 range"));
    }
    Err(format!("{label} must be an integer route column"))
}

fn int8_value(array: &dyn Array, row: usize, label: &str) -> Result<Option<i8>, String> {
    if row >= array.len() {
        return Err(format!(
            "{label} row index {row} is out of bounds for length {}",
            array.len()
        ));
    }
    let Some(values) = array.as_any().downcast_ref::<Int8Array>() else {
        return Err(format!("{label} must be Int8"));
    };
    if values.is_null(row) {
        Ok(None)
    } else {
        Ok(Some(values.value(row)))
    }
}

fn route_key_for_row(
    change_op: &dyn Array,
    data_route: Option<&dyn Array>,
    row: usize,
) -> Result<ChangeStreamRouteKey, String> {
    let Some(op) = int8_value(change_op, row, "__change_op").map(|value| value.map(i32::from))?
    else {
        return Err("__change_op must not be NULL".to_string());
    };
    match op {
        CHANGE_OP_DELETE => {
            let route = data_route
                .map(|array| int32_value(array, row, "data_route"))
                .transpose()?
                .flatten();
            if route.is_some() {
                return Err("DELETE_DV route requires NULL data_route".to_string());
            }
            Ok(ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            })
        }
        CHANGE_OP_INSERT => {
            let Some(data_route) = data_route else {
                return Err("+1 data route requires non-NULL data_route".to_string());
            };
            let Some(route) = int32_value(data_route, row, "data_route")? else {
                return Err("+1 data route requires non-NULL data_route".to_string());
            };
            if route != DATA_ROUTE_REUSE && route != DATA_ROUTE_FRESH {
                return Err(format!("unsupported data_route={route} for +1 row"));
            }
            Ok(ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(route),
            })
        }
        other => Err(format!(
            "unsupported __change_op={other}; expected -1 or +1"
        )),
    }
}

fn route_indices(
    change_op: &dyn Array,
    data_route: Option<&dyn Array>,
    branch_map: &BTreeMap<ChangeStreamRouteKey, usize>,
) -> Result<Vec<usize>, String> {
    if let Some(route) = data_route
        && route.len() != change_op.len()
    {
        return Err(format!(
            "data_route length {} != __change_op length {}",
            route.len(),
            change_op.len()
        ));
    }

    let mut routes = Vec::with_capacity(change_op.len());
    for row in 0..change_op.len() {
        let key = route_key_for_row(change_op, data_route, row)?;
        let Some(branch) = branch_map.get(&key) else {
            return Err(format!(
                "undeclared change-stream route key change_op={} data_route={:?}",
                key.change_op, key.data_route
            ));
        };
        routes.push(*branch);
    }
    Ok(routes)
}

pub(crate) fn route_indices_for_test(
    change_op: arrow::array::ArrayRef,
    data_route: Option<arrow::array::ArrayRef>,
    branch_map: &BTreeMap<ChangeStreamRouteKey, usize>,
) -> Result<Vec<usize>, String> {
    route_indices(change_op.as_ref(), data_route.as_deref(), branch_map)
}

/// Factory for typed Iceberg change-stream router sinks.
pub(crate) struct IcebergChangeStreamRouterSinkFactory {
    name: String,
    init_error: Option<String>,
    change_op_slot_id: Option<SlotId>,
    data_route_slot_id: Option<SlotId>,
    branches: Vec<IcebergChangeStreamRouterBranchFactory>,
    route_to_branch: BTreeMap<ChangeStreamRouteKey, usize>,
}

impl IcebergChangeStreamRouterSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        router: data_sinks::TIcebergChangeStreamRouterSink,
        exec_params: internal_service::TPlanFragmentExecParams,
        layout: Layout,
        plan_node_id: i32,
        last_query_id: Option<String>,
        fe_addr: Option<types::TNetworkAddress>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("ICEBERG_CHANGE_STREAM_ROUTER_SINK (id={plan_node_id})")
        } else {
            "ICEBERG_CHANGE_STREAM_ROUTER_SINK".to_string()
        };

        let mut init_error = None;
        let change_op_slot_id = match SlotId::try_from(router.change_op_slot_id) {
            Ok(slot_id) => Some(slot_id),
            Err(err) => {
                init_error = Some(format!(
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK: invalid change_op_slot_id: {err}"
                ));
                None
            }
        };
        let data_route_slot_id = match router.data_route_slot_id {
            Some(raw) => match SlotId::try_from(raw) {
                Ok(slot_id) => Some(slot_id),
                Err(err) => {
                    if init_error.is_none() {
                        init_error = Some(format!(
                            "ICEBERG_CHANGE_STREAM_ROUTER_SINK: invalid data_route_slot_id: {err}"
                        ));
                    }
                    None
                }
            },
            None => None,
        };
        if let (Some(change_op_slot_id), Some(data_route_slot_id)) =
            (change_op_slot_id, data_route_slot_id)
            && change_op_slot_id == data_route_slot_id
            && init_error.is_none()
        {
            init_error = Some(
                "ICEBERG_CHANGE_STREAM_ROUTER_SINK: data_route_slot_id must differ from change_op_slot_id"
                    .to_string(),
            );
        }

        let mut branches = Vec::new();
        let mut route_to_branch = BTreeMap::new();
        let mut seen_branch_ids = BTreeSet::new();
        let mut seen_branch_kinds = BTreeSet::new();

        if router.branches.is_empty() && init_error.is_none() {
            init_error =
                Some("ICEBERG_CHANGE_STREAM_ROUTER_SINK requires at least one branch".to_string());
        }

        let raw_branches = router.branches;
        let data_branch_count = raw_branches
            .iter()
            .filter(|branch| {
                matches!(
                    branch_kind_from_thrift(branch.branch_kind),
                    Ok(ChangeStreamBranchKind::ReuseData) | Ok(ChangeStreamBranchKind::FreshData)
                )
            })
            .count();

        for branch in raw_branches {
            let branch_kind = match branch_kind_from_thrift(branch.branch_kind) {
                Ok(kind) => kind,
                Err(err) => {
                    if init_error.is_none() {
                        init_error = Some(err);
                    }
                    continue;
                }
            };
            if !seen_branch_ids.insert(branch.branch_id) && init_error.is_none() {
                init_error = Some(format!(
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK: duplicate branch_id {}",
                    branch.branch_id
                ));
            }
            if !seen_branch_kinds.insert(branch_kind) && init_error.is_none() {
                init_error = Some(format!(
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK: duplicate change-stream branch kind {:?}",
                    branch_kind
                ));
            }

            let branch_index = branches.len();
            if route_to_branch
                .insert(branch_kind.route_key(), branch_index)
                .is_some()
                && init_error.is_none()
            {
                init_error = Some(format!(
                    "ICEBERG_CHANGE_STREAM_ROUTER_SINK: duplicate route for branch kind {:?}",
                    branch_kind
                ));
            }
            let mut branch_exec_params = exec_params.clone();
            branch_exec_params.destinations = Some(branch.destinations);
            branches.push(IcebergChangeStreamRouterBranchFactory {
                branch_id: branch.branch_id,
                branch_kind,
                data_stream: DataStreamSinkFactory::new(
                    branch.stream_sink,
                    branch_exec_params,
                    layout.clone(),
                    plan_node_id,
                    last_query_id.clone(),
                    fe_addr.clone(),
                ),
            });
        }

        if data_route_slot_id.is_none() && data_branch_count > 0 && init_error.is_none() {
            init_error = Some(
                "ICEBERG_CHANGE_STREAM_ROUTER_SINK: data_route_slot_id is required when data branches are declared"
                    .to_string(),
            );
        }

        Self {
            name,
            init_error,
            change_op_slot_id,
            data_route_slot_id,
            branches,
            route_to_branch,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        router: data_sinks::TIcebergChangeStreamRouterSink,
        exec_params: internal_service::TPlanFragmentExecParams,
        layout: Layout,
        plan_node_id: i32,
        last_query_id: Option<String>,
        fe_addr: Option<types::TNetworkAddress>,
    ) -> Result<Self, String> {
        let factory = Self::new(
            router,
            exec_params,
            layout,
            plan_node_id,
            last_query_id,
            fe_addr,
        );
        if let Some(err) = factory.init_error.as_ref() {
            return Err(err.clone());
        }
        Ok(factory)
    }
}

impl OperatorFactory for IcebergChangeStreamRouterSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let mut branches = Vec::with_capacity(self.branches.len());
        for branch in &self.branches {
            branches.push(IcebergChangeStreamRouterBranchRuntime {
                branch_id: branch.branch_id,
                branch_kind: branch.branch_kind,
                op: branch.data_stream.create(dop, driver_id),
            });
        }

        Box::new(IcebergChangeStreamRouterSinkOperator {
            name: self.name.clone(),
            init_error: self.init_error.clone(),
            change_op_slot_id: self.change_op_slot_id,
            data_route_slot_id: self.data_route_slot_id,
            route_to_branch: self.route_to_branch.clone(),
            branches,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergChangeStreamRouterBranchFactory {
    branch_id: i32,
    branch_kind: ChangeStreamBranchKind,
    data_stream: DataStreamSinkFactory,
}

struct IcebergChangeStreamRouterBranchRuntime {
    branch_id: i32,
    branch_kind: ChangeStreamBranchKind,
    op: Box<dyn Operator>,
}

struct IcebergChangeStreamRouterSinkOperator {
    name: String,
    init_error: Option<String>,
    change_op_slot_id: Option<SlotId>,
    data_route_slot_id: Option<SlotId>,
    route_to_branch: BTreeMap<ChangeStreamRouteKey, usize>,
    branches: Vec<IcebergChangeStreamRouterBranchRuntime>,
    finished: bool,
}

impl Operator for IcebergChangeStreamRouterSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        for branch in &mut self.branches {
            branch.op.set_mem_tracker(Arc::clone(&tracker));
        }
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        for branch in &mut self.branches {
            branch.op.set_profiles(profiles.clone());
        }
    }

    fn prepare(&mut self) -> Result<(), String> {
        for branch in &mut self.branches {
            branch.op.prepare()?;
        }
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        for branch in &mut self.branches {
            branch.op.bind_runtime_state(state)?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        for branch in &mut self.branches {
            branch.op.close()?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        for branch in &mut self.branches {
            branch.op.cancel();
        }
    }

    fn is_finished(&self) -> bool {
        self.finished
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for IcebergChangeStreamRouterSinkOperator {
    fn need_input(&self) -> bool {
        if self.finished {
            return false;
        }
        if self.init_error.is_some() {
            return true;
        }
        for branch in &self.branches {
            let Some(inner) = branch.op.as_processor_ref() else {
                return false;
            };
            if !inner.need_input() {
                return false;
            }
        }
        true
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finished || chunk.is_empty() {
            return Ok(());
        }

        let change_op_slot_id = self.change_op_slot_id.ok_or_else(|| {
            "ICEBERG_CHANGE_STREAM_ROUTER_SINK missing change_op_slot_id".to_string()
        })?;
        let branch_chunks = route_chunk_by_typed_key(
            &chunk,
            change_op_slot_id,
            self.data_route_slot_id,
            &self.route_to_branch,
            self.branches.len(),
        )?;
        if branch_chunks.len() != self.branches.len() {
            return Err(format!(
                "Iceberg change-stream router produced {} branch chunks for {} branches",
                branch_chunks.len(),
                self.branches.len()
            ));
        }

        for (branch, part) in self.branches.iter_mut().zip(branch_chunks.into_iter()) {
            let Some(part) = part else {
                continue;
            };
            let inner = branch.op.as_processor_mut().ok_or_else(|| {
                format!(
                    "Iceberg change-stream branch {} ({:?}) data stream op missing processor operator",
                    branch.branch_id, branch.branch_kind
                )
            })?;
            inner.push_chunk(state, part)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finished {
            return Ok(());
        }
        for branch in &mut self.branches {
            let inner = branch.op.as_processor_mut().ok_or_else(|| {
                format!(
                    "Iceberg change-stream branch {} ({:?}) data stream op missing processor operator",
                    branch.branch_id, branch.branch_kind
                )
            })?;
            inner.set_finishing(state)?;
        }
        self.finished = true;
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        if self.finished {
            return None;
        }
        for branch in &self.branches {
            let Some(inner) = branch.op.as_processor_ref() else {
                continue;
            };
            if let Some(obs) = inner.sink_observable() {
                return Some(obs);
            }
        }
        None
    }
}

fn route_chunk_by_typed_key(
    chunk: &Chunk,
    change_op_slot_id: SlotId,
    data_route_slot_id: Option<SlotId>,
    branch_map: &BTreeMap<ChangeStreamRouteKey, usize>,
    branch_count: usize,
) -> Result<Vec<Option<Chunk>>, String> {
    let change_op = chunk.column_by_slot_id(change_op_slot_id)?;
    let data_route = data_route_slot_id
        .map(|slot_id| chunk.column_by_slot_id(slot_id))
        .transpose()?;
    let routes = route_indices(change_op.as_ref(), data_route.as_deref(), branch_map)?;
    if routes.len() != chunk.len() {
        return Err(format!(
            "route count {} != chunk length {}",
            routes.len(),
            chunk.len()
        ));
    }

    let mut row_indices = vec![Vec::new(); branch_count];
    for (row, branch_index) in routes.into_iter().enumerate() {
        let rows = row_indices.get_mut(branch_index).ok_or_else(|| {
            format!(
                "route branch index {} is out of bounds for {} branches",
                branch_index, branch_count
            )
        })?;
        let row = u32::try_from(row)
            .map_err(|_| format!("row index {row} exceeds UInt32 routing index range"))?;
        rows.push(row);
    }

    let mut out = Vec::with_capacity(branch_count);
    for rows in row_indices {
        out.push(take_chunk_rows(chunk, rows)?);
    }
    Ok(out)
}

fn take_chunk_rows(chunk: &Chunk, rows: Vec<u32>) -> Result<Option<Chunk>, String> {
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() == chunk.len()
        && rows
            .iter()
            .enumerate()
            .all(|(idx, row)| *row as usize == idx)
    {
        return Ok(Some(chunk.clone()));
    }

    let indices = Arc::new(UInt32Array::from(rows)) as arrow::array::ArrayRef;
    let mut columns = Vec::with_capacity(chunk.batch.num_columns());
    for column in chunk.batch.columns() {
        columns.push(
            take(column.as_ref(), &indices, None)
                .map_err(|e| format!("Iceberg change-stream router take failed: {e}"))?,
        );
    }
    Chunk::try_new_with_columns(chunk.chunk_schema_ref(), columns)
        .map(Some)
        .map_err(|e| format!("Iceberg change-stream router chunk build failed: {e}"))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int8Array, Int32Array, Int64Array};

    use super::*;

    fn int8(values: Vec<Option<i8>>) -> ArrayRef {
        Arc::new(Int8Array::from(values))
    }

    fn int32(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    fn int64(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }

    #[test]
    fn route_rows_by_typed_key() {
        let branch_map = BTreeMap::from([
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_DELETE,
                    data_route: None,
                },
                0usize,
            ),
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_INSERT,
                    data_route: Some(DATA_ROUTE_REUSE),
                },
                1usize,
            ),
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_INSERT,
                    data_route: Some(DATA_ROUTE_FRESH),
                },
                2usize,
            ),
        ]);

        let routes = route_indices_for_test(
            int8(vec![Some(-1), Some(1), Some(1), Some(-1)]),
            Some(int32(vec![
                None,
                Some(DATA_ROUTE_REUSE),
                Some(DATA_ROUTE_FRESH),
                None,
            ])),
            &branch_map,
        )
        .expect("routes");

        assert_eq!(routes, vec![0, 1, 2, 0]);
    }

    #[test]
    fn route_rows_accepts_integral_data_route_literals() {
        let branch_map = BTreeMap::from([
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_INSERT,
                    data_route: Some(DATA_ROUTE_REUSE),
                },
                0usize,
            ),
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_INSERT,
                    data_route: Some(DATA_ROUTE_FRESH),
                },
                1usize,
            ),
        ]);

        let routes = route_indices_for_test(
            int8(vec![Some(1), Some(1)]),
            Some(int64(vec![
                Some(i64::from(DATA_ROUTE_REUSE)),
                Some(i64::from(DATA_ROUTE_FRESH)),
            ])),
            &branch_map,
        )
        .expect("routes");

        assert_eq!(routes, vec![0, 1]);
    }

    #[test]
    fn route_rows_without_data_route_rejects_data_branch() {
        let branch_map = BTreeMap::from([
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_DELETE,
                    data_route: None,
                },
                0usize,
            ),
            (
                ChangeStreamRouteKey {
                    change_op: CHANGE_OP_INSERT,
                    data_route: Some(DATA_ROUTE_REUSE),
                },
                1usize,
            ),
        ]);

        let err = route_indices_for_test(
            int8(vec![Some(-1), Some(1), Some(1), Some(-1)]),
            None,
            &branch_map,
        )
        .expect_err("missing data_route");

        assert!(err.contains("+1 data route requires non-NULL data_route"));
    }

    #[test]
    fn unknown_route_key_fails_without_fallback() {
        let branch_map = BTreeMap::from([(
            ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            },
            0usize,
        )]);

        let err = route_indices_for_test(
            int8(vec![Some(1)]),
            Some(int32(vec![Some(DATA_ROUTE_REUSE)])),
            &branch_map,
        )
        .expect_err("undeclared reuse branch");

        assert!(err.contains("undeclared change-stream route key"));
        assert!(err.contains("change_op=1"));
    }

    #[test]
    fn delete_route_rejects_non_null_data_route() {
        let branch_map = BTreeMap::from([(
            ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            },
            0usize,
        )]);

        let err = route_indices_for_test(
            int8(vec![Some(-1)]),
            Some(int32(vec![Some(DATA_ROUTE_REUSE)])),
            &branch_map,
        )
        .expect_err("delete data route must be null");

        assert!(err.contains("DELETE_DV route requires NULL data_route"));
    }

    #[test]
    fn factory_rejects_same_change_op_and_data_route_slot() {
        let factory = IcebergChangeStreamRouterSinkFactory::new(
            data_sinks::TIcebergChangeStreamRouterSink::new(7, Some(7), Vec::new()),
            internal_service::TPlanFragmentExecParams::new(
                types::TUniqueId::new(0, 1),
                types::TUniqueId::new(0, 2),
                BTreeMap::new(),
                BTreeMap::new(),
                None::<Vec<data_sinks::TPlanFragmentDestination>>,
                None::<i32>,
                None::<i32>,
                None::<bool>,
                None::<bool>,
                None::<crate::thrift::runtime_filter::TRuntimeFilterParams>,
                None::<i32>,
                None::<bool>,
                None::<
                    BTreeMap<
                        types::TPlanNodeId,
                        BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
                    >,
                >,
                None::<bool>,
                None::<i32>,
                None::<bool>,
                None::<Vec<internal_service::TExecDebugOption>>,
            ),
            Layout {
                order: Vec::new(),
                index: HashMap::new(),
            },
            -1,
            None,
            None,
        );

        let err = factory.init_error.expect("same slot must fail");
        assert!(err.contains("data_route_slot_id must differ from change_op_slot_id"));
    }
}
