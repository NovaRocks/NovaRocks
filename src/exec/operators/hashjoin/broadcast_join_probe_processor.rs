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
//! Broadcast hash-join probe processor.
//!
//! Responsibilities:
//! - Probes a fully replicated build-side hash table with probe-side chunks.
//! - Implements join-type specific match/miss output shaping for broadcast joins.
//!
//! Key exported interfaces:
//! - Types: `BroadcastJoinProbeProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::broadcast_join_shared::BroadcastJoinSharedState;
use super::hash_join_probe_core::{HashJoinProbeCore, join_type_str};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinType;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;
use crate::novarocks_logging::debug;

fn parse_join_node_id_from_dep_key(dep_key: &str) -> i32 {
    // Expected format: "broadcast_join_build:<node_id>"
    dep_key
        .strip_prefix("broadcast_join_build:")
        .unwrap_or(dep_key)
        .parse::<i32>()
        .unwrap_or(-1)
}

/// Factory for broadcast hash-join probe operators that read a replicated build artifact.
pub struct BroadcastJoinProbeProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_scope_schema: SchemaRef,
    state: Arc<BroadcastJoinSharedState>,
}

impl BroadcastJoinProbeProcessorFactory {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_scope_schema: SchemaRef,
        state: Arc<BroadcastJoinSharedState>,
    ) -> Self {
        let node_id = parse_join_node_id_from_dep_key(state.dep_name());
        Self {
            name: format!("HASH_JOIN (id={})", node_id),
            arena,
            join_type,
            probe_keys,
            residual_predicate,
            probe_is_left,
            left_schema,
            right_schema,
            join_scope_schema,
            state,
        }
    }
}

impl OperatorFactory for BroadcastJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(BroadcastJoinProbeProcessorOperator {
            name: self.name.clone(),
            state: Arc::clone(&self.state),
            driver_id,
            dep: self.state.dep(),
            buffered: Vec::new(),
            pending_output: None,
            finishing: false,
            finishing_done: false,
            finished: false,
            core: HashJoinProbeCore::new(
                Arc::clone(&self.arena),
                self.join_type,
                self.probe_keys.clone(),
                self.residual_predicate,
                self.probe_is_left,
                Arc::clone(&self.left_schema),
                Arc::clone(&self.right_schema),
                Arc::clone(&self.join_scope_schema),
            ),
            input_rows: 0,
            input_chunks: 0,
            buffered_rows: 0,
            profile_initialized: false,
            profiles: None,
        })
    }
}

struct BroadcastJoinProbeProcessorOperator {
    name: String,
    state: Arc<BroadcastJoinSharedState>,
    driver_id: i32,
    dep: DependencyHandle,
    buffered: Vec<Chunk>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finishing_done: bool,
    finished: bool,
    core: HashJoinProbeCore,
    input_rows: usize,
    input_chunks: usize,
    buffered_rows: usize,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
}

impl Operator for BroadcastJoinProbeProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: crate::runtime::profile::OperatorProfiles) {
        self.profiles = Some(profiles);
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

impl ProcessorOperator for BroadcastJoinProbeProcessorOperator {
    fn need_input(&self) -> bool {
        if self.finishing || self.finished || self.pending_output.is_some() {
            return false;
        }
        self.core.is_build_loaded() || self.state.has_build()
    }

    fn has_output(&self) -> bool {
        if self.pending_output.is_some() {
            return true;
        }
        self.finishing
            && !self.finishing_done
            && (self.core.is_build_loaded() || self.state.has_build())
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        if self.finishing {
            return Err("broadcast join probe received input after set_finishing".to_string());
        }
        if self.pending_output.is_some() {
            return Err(
                "broadcast join probe received input while output buffer is full".to_string(),
            );
        }
        if !self.core.is_build_loaded() {
            self.load_build_side()?;
        }
        let out = self.process_one(chunk)?;
        self.pending_output = out;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.pending_output.is_none() && self.finishing && !self.finishing_done {
            let out = self.finish_one()?;
            self.pending_output = out;
            self.finishing_done = true;
        }
        let out = self.pending_output.take();
        if self.finishing && self.finishing_done && self.pending_output.is_none() {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        self.finishing = true;
        Ok(())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        if self.core.is_build_loaded() || self.state.has_build() {
            None
        } else {
            Some(self.dep.clone())
        }
    }
}

impl BroadcastJoinProbeProcessorOperator {
    fn init_profile_if_needed(&mut self) {
        if self.profile_initialized {
            return;
        }
        self.profile_initialized = true;
        if let Some(profile) = self.profiles.as_ref() {
            profile
                .common
                .add_info_string("JoinType", join_type_str(self.core.join_type()));
            profile
                .common
                .add_info_string("DistributionMode", "BROADCAST");
        }
    }

    fn process_one(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        if !chunk.is_empty() {
            self.input_rows = self.input_rows.saturating_add(chunk.len());
            self.input_chunks = self.input_chunks.saturating_add(1);
        }
        let probe_chunks = self.collect_probe_chunks(Some(chunk));
        self.core.join_probe_chunks(probe_chunks)
    }

    fn finish_one(&mut self) -> Result<Option<Chunk>, String> {
        if !self.core.is_build_loaded() {
            self.load_build_side()?;
        }
        let probe_chunks = self.collect_probe_chunks(None);
        let mut out = self.core.join_probe_chunks(probe_chunks)?;

        match self.core.join_type() {
            JoinType::RightAnti if self.core.probe_is_left() => {
                let build_out = self.core.build_right_semi_anti_output(false)?;
                out = self.core.merge_join_outputs(None, build_out, true)?;
            }
            JoinType::FullOuter | JoinType::RightOuter => {
                let build_unmatched = self.core.build_full_outer_unmatched_build()?;
                out = self.core.merge_join_outputs(out, build_unmatched, false)?;
            }
            _ => {}
        }

        self.log_stats();
        Ok(out)
    }

    fn log_stats(&self) {
        debug!(
            "BroadcastJoinProbe finished: dep_key={} driver_id={} input_rows={} input_chunks={} buffered_rows={} output_rows={} build_batches={} build_table={} probe_keys={}",
            self.dep.name(),
            self.driver_id,
            self.input_rows,
            self.input_chunks,
            self.buffered_rows,
            self.core.output_rows(),
            self.core.build_batches_len(),
            self.core.build_table_present(),
            self.core.probe_keys_len()
        );
    }

    fn load_build_side(&mut self) -> Result<(), String> {
        if self.core.is_build_loaded() {
            return Ok(());
        }
        let Some(artifact) = self.state.get_build() else {
            return Err("broadcast join build not ready".to_string());
        };
        let global_rows = artifact.build_row_count;
        let global_has_null = artifact.build_has_null_key;
        self.core
            .set_build_artifact(artifact, global_rows, global_has_null)?;
        Ok(())
    }

    fn collect_probe_chunks(&mut self, chunk: Option<Chunk>) -> Vec<Chunk> {
        let mut probe_chunks = Vec::new();
        for buffered in self.buffered.drain(..) {
            if !buffered.is_empty() {
                probe_chunks.push(buffered);
            }
        }
        if let Some(chunk) = chunk {
            if !chunk.is_empty() {
                probe_chunks.push(chunk);
            }
        }
        probe_chunks
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::join::JoinDistributionMode;
    use crate::exec::node::join::JoinType;
    use crate::exec::operators::hashjoin::HashJoinBuildSinkFactory;
    use crate::exec::operators::hashjoin::broadcast_join_shared::BroadcastJoinSharedState;
    use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
    use crate::runtime::runtime_state::RuntimeState;

    use super::BroadcastJoinProbeProcessorFactory;

    fn chunk_of(slot_id: SlotId, values: &[i32]) -> Chunk {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("k", DataType::Int32, false),
            slot_id,
        )]));
        let array = Arc::new(Int32Array::from(values.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        Chunk::new(batch)
    }

    fn chunk_of_two(
        k_slot_id: SlotId,
        v_slot_id: SlotId,
        k: &[i32],
        v: &[i32],
        v_name: &str,
    ) -> Chunk {
        assert_eq!(k.len(), v.len());
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), k_slot_id),
            field_with_slot_id(Field::new(v_name, DataType::Int32, false), v_slot_id),
        ]));
        let k_arr = Arc::new(Int32Array::from(k.to_vec())) as arrow::array::ArrayRef;
        let v_arr = Arc::new(Int32Array::from(v.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![k_arr, v_arr]).expect("record batch");
        Chunk::new(batch)
    }

    fn append_pairs(chunk: Chunk, pairs: &mut Vec<(i32, i32)>) {
        let left_arr = chunk
            .columns()
            .get(0)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let right_arr = chunk
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            pairs.push((left_arr.value(i), right_arr.value(i)));
        }
    }

    #[test]
    fn broadcast_inner_join_shares_build_across_probers() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let arena = Arc::new(arena);

        let left_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("k", DataType::Int32, false),
            SlotId::new(1),
        )]));
        let right_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("k", DataType::Int32, false),
            SlotId::new(2),
        )]));
        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(2)),
        ]));

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(BroadcastJoinSharedState::new(1, dep_manager));

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Broadcast,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = BroadcastJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            None,
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of(SlotId::new(2), &[1, 2, 4]);
        let probe_left = chunk_of(SlotId::new(1), &[2, 3]);
        let probe_right = chunk_of(SlotId::new(1), &[4]);

        let mut probe_op_left = probe_factory.create(2, 0);
        let Some(proc_left) = probe_op_left.as_processor_mut() else {
            panic!("probe operator missing processor");
        };
        assert!(!proc_left.need_input());
        let dep = proc_left
            .precondition_dependency()
            .expect("build dependency");

        let mut build_op = build_factory.create(1, 0);
        let Some(sink) = build_op.as_processor_mut() else {
            panic!("build operator missing processor");
        };
        sink.push_chunk(&rt, build).expect("build push");
        sink.set_finishing(&rt).expect("build finish");
        assert!(dep.is_ready());

        let mut pairs = Vec::new();
        assert!(proc_left.need_input());
        proc_left.push_chunk(&rt, probe_left).expect("probe push");
        while proc_left.has_output() {
            if let Some(out) = proc_left.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut pairs);
            }
        }
        proc_left.set_finishing(&rt).expect("probe finish");
        while proc_left.has_output() {
            if let Some(out) = proc_left.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut pairs);
            }
        }

        let mut probe_op_right = probe_factory.create(2, 1);
        let Some(proc_right) = probe_op_right.as_processor_mut() else {
            panic!("probe operator missing processor");
        };
        proc_right
            .push_chunk(&rt, probe_right)
            .expect("probe process");
        while proc_right.has_output() {
            if let Some(out) = proc_right.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut pairs);
            }
        }
        proc_right.set_finishing(&rt).expect("probe finish");
        while proc_right.has_output() {
            if let Some(out) = proc_right.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut pairs);
            }
        }

        pairs.sort();
        assert_eq!(pairs, vec![(2, 2), (4, 4)]);
    }

    #[test]
    fn broadcast_left_semi_join_outputs_probe_side() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let arena = Arc::new(arena);

        let left_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let right_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("k", DataType::Int32, false),
            SlotId::new(3),
        )]));
        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
        ]));

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(BroadcastJoinSharedState::new(1, dep_manager));

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Broadcast,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = BroadcastJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            None,
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        // Left semi: probe is left, build is right.
        let build = chunk_of(SlotId::new(3), &[1, 3]);
        let probe = chunk_of_two(
            SlotId::new(1),
            SlotId::new(2),
            &[1, 2, 3],
            &[100, 200, 300],
            "v",
        );

        let mut probe_op = probe_factory.create(1, 0);
        let Some(proc) = probe_op.as_processor_mut() else {
            panic!("probe operator missing processor");
        };
        assert!(!proc.need_input());
        let dep = proc.precondition_dependency().expect("build dependency");

        let mut build_op = build_factory.create(1, 0);
        let Some(sink) = build_op.as_processor_mut() else {
            panic!("build operator missing processor");
        };
        sink.push_chunk(&rt, build).expect("build push");
        sink.set_finishing(&rt).expect("build finish");
        assert!(dep.is_ready());

        let mut rows = Vec::new();
        assert!(proc.need_input());
        proc.push_chunk(&rt, probe).expect("probe push");
        while proc.has_output() {
            if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut rows);
            }
        }
        proc.set_finishing(&rt).expect("probe finish");
        while proc.has_output() {
            if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut rows);
            }
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 100), (3, 300)]);
    }

    #[test]
    fn broadcast_right_semi_join_with_probe_left_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let probe_key = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let build_key = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let left_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));
        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(BroadcastJoinSharedState::new(1, dep_manager));

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Broadcast,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = BroadcastJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            Some(residual),
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(SlotId::new(3), SlotId::new(4), &[1, 1, 2], &[5, 20, 7], "w");
        let probe = chunk_of_two(
            SlotId::new(1),
            SlotId::new(2),
            &[1, 1, 2],
            &[10, 15, 100],
            "v",
        );

        let mut probe_op = probe_factory.create(1, 0);
        let Some(proc) = probe_op.as_processor_mut() else {
            panic!("probe operator missing processor");
        };
        assert!(!proc.need_input());
        let dep = proc.precondition_dependency().expect("build dependency");

        let mut build_op = build_factory.create(1, 0);
        let Some(sink) = build_op.as_processor_mut() else {
            panic!("build operator missing processor");
        };
        sink.push_chunk(&rt, build).expect("build push");
        sink.set_finishing(&rt).expect("build finish");
        assert!(dep.is_ready());

        let mut rows = Vec::new();
        assert!(proc.need_input());
        proc.push_chunk(&rt, probe).expect("probe push");
        while proc.has_output() {
            if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut rows);
            }
        }
        proc.set_finishing(&rt).expect("probe finish");
        while proc.has_output() {
            if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                append_pairs(out, &mut rows);
            }
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 20)]);
    }
}
