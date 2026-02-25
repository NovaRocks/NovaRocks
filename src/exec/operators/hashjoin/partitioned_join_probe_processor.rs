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
//! Partitioned hash-join probe processor.
//!
//! Responsibilities:
//! - Probes partition-scoped build artifacts and joins probe rows with matching partitions.
//! - Coordinates partition readiness and emits join-type specific output batches.
//!
//! Key exported interfaces:
//! - Types: `PartitionedJoinProbeProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use super::hash_join_probe_core::{HashJoinProbeCore, join_type_str};
use super::partitioned_join_shared::PartitionedJoinSharedState;
use crate::common::config::operator_buffer_chunks;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::join::JoinType;
use crate::exec::pipeline::dependency::DependencyHandle;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::novarocks_logging::debug;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for partitioned hash-join probe operators over per-partition build artifacts.
pub struct PartitionedJoinProbeProcessorFactory {
    name: String,
    node_id: i32,
    arena: Arc<ExprArena>,
    join_type: JoinType,
    probe_keys: Vec<ExprId>,
    residual_predicate: Option<ExprId>,
    probe_is_left: bool,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_scope_schema: SchemaRef,
    state: Arc<PartitionedJoinSharedState>,
}

impl PartitionedJoinProbeProcessorFactory {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        join_type: JoinType,
        probe_keys: Vec<ExprId>,
        residual_predicate: Option<ExprId>,
        probe_is_left: bool,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_scope_schema: SchemaRef,
        state: Arc<PartitionedJoinSharedState>,
    ) -> Self {
        let node_id = parse_join_node_id_from_dep_key(state.dep_name(0));
        Self {
            name: format!("HASH_JOIN (id={})", node_id),
            node_id,
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

impl OperatorFactory for PartitionedJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let partition = driver_id.max(0) as usize;
        debug!(
            "PartitionedJoinProbe create: node_id={} driver_id={} partition={} join_type={} probe_keys={}",
            self.node_id,
            driver_id,
            partition,
            join_type_str(self.join_type),
            self.probe_keys.len()
        );
        Box::new(PartitionedJoinProbeProcessorOperator {
            name: self.name.clone(),
            node_id: self.node_id,
            driver_id,
            state: Arc::clone(&self.state),
            partition,
            dep: self.state.dep_handle(partition),
            max_buffered_probe_chunks: operator_buffer_chunks().max(1),
            buffered: VecDeque::new(),
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
            buffered_rows: 0,
            input_rows: 0,
            input_chunks: 0,
            profile_initialized: false,
            profiles: None,
        })
    }
}

struct PartitionedJoinProbeProcessorOperator {
    name: String,
    node_id: i32,
    driver_id: i32,
    state: Arc<PartitionedJoinSharedState>,
    partition: usize,
    dep: DependencyHandle,
    max_buffered_probe_chunks: usize,
    buffered: VecDeque<Chunk>,
    pending_output: Option<Chunk>,
    finishing: bool,
    finishing_done: bool,
    finished: bool,
    core: HashJoinProbeCore,
    buffered_rows: u64,
    input_rows: u64,
    input_chunks: u64,
    profile_initialized: bool,
    profiles: Option<crate::runtime::profile::OperatorProfiles>,
}

impl Operator for PartitionedJoinProbeProcessorOperator {
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

impl ProcessorOperator for PartitionedJoinProbeProcessorOperator {
    fn need_input(&self) -> bool {
        if self.finishing || self.finished || self.pending_output.is_some() {
            return false;
        }
        if self.core.is_build_loaded() || self.state.is_partition_ready(self.partition) {
            return true;
        }
        self.buffered.len() < self.max_buffered_probe_chunks
    }

    fn has_output(&self) -> bool {
        if self.pending_output.is_some() {
            return true;
        }
        if !self.finishing
            && !self.buffered.is_empty()
            && (self.core.is_build_loaded() || self.state.is_partition_ready(self.partition))
        {
            return true;
        }
        self.finishing
            && !self.finishing_done
            && (self.core.is_build_loaded() || self.state.is_partition_ready(self.partition))
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.init_profile_if_needed();
        if self.finishing {
            return Err("partitioned join probe received input after set_finishing".to_string());
        }
        if self.pending_output.is_some() {
            return Err(
                "partitioned join probe received input while output buffer is full".to_string(),
            );
        }
        if !self.core.is_build_loaded() && !self.state.is_partition_ready(self.partition) {
            if self.buffered.len() >= self.max_buffered_probe_chunks {
                return Err("partitioned join probe buffer is full".to_string());
            }
            if !chunk.is_empty() {
                self.buffered_rows = self.buffered_rows.saturating_add(chunk.len() as u64);
            }
            self.buffered.push_back(chunk);
            return Ok(());
        }
        if !self.core.is_build_loaded() {
            self.load_build_side()?;
        }
        if !chunk.is_empty() {
            self.input_rows = self.input_rows.saturating_add(chunk.len() as u64);
            self.input_chunks = self.input_chunks.saturating_add(1);
        }
        let out = self.process_one(chunk)?;
        self.pending_output = out;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.pending_output.is_none() {
            if !self.finishing
                && !self.buffered.is_empty()
                && (self.core.is_build_loaded() || self.state.is_partition_ready(self.partition))
            {
                if !self.core.is_build_loaded() {
                    self.load_build_side()?;
                }
                let out = self.join_buffered(false)?;
                self.pending_output = out;
            } else if self.finishing && !self.finishing_done {
                let out = self.finish_one()?;
                self.pending_output = out;
                self.finishing_done = true;
            }
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
        if self.core.is_build_loaded() || self.state.is_partition_ready(self.partition) {
            None
        } else {
            Some(self.dep.clone())
        }
    }
}

impl PartitionedJoinProbeProcessorOperator {
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
                .add_info_string("DistributionMode", "PARTITIONED");
        }
    }
}

fn parse_join_node_id_from_dep_key(dep_key: &str) -> i32 {
    let after_prefix = dep_key.strip_prefix("join_build:").unwrap_or(dep_key);
    let node_id_str = after_prefix.split(':').next().unwrap_or("");
    node_id_str.parse::<i32>().unwrap_or(-1)
}

impl PartitionedJoinProbeProcessorOperator {
    fn process_one(&mut self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let probe_chunks = self.collect_probe_chunks(Some(chunk), false);
        self.core.join_probe_chunks(probe_chunks)
    }

    fn finish_one(&mut self) -> Result<Option<Chunk>, String> {
        if !self.core.is_build_loaded() {
            self.load_build_side()?;
        }
        let probe_chunks = self.collect_probe_chunks(None, true);
        let mut out = self.core.join_probe_chunks(probe_chunks)?;
        match self.core.join_type() {
            JoinType::RightAnti if self.core.probe_is_left() => {
                let build_out = self.core.build_right_semi_anti_output(false)?;
                out = self.core.merge_join_outputs(None, build_out, true)?;
            }
            JoinType::FullOuter | JoinType::RightOuter => {
                let build_unmatched = self.core.build_full_outer_unmatched_build()?;
                out = self.core.merge_join_outputs(out, build_unmatched, true)?;
            }
            _ => {}
        }
        self.log_stats();
        Ok(out)
    }

    fn load_build_side(&mut self) -> Result<(), String> {
        if self.core.is_build_loaded() {
            return Ok(());
        }
        if self.partition >= self.state.partition_count() {
            return Err("join probe partition out of bounds".to_string());
        }

        let Some(artifact) = self
            .state
            .take_build_partition(self.partition)
            .map_err(|e| e.to_string())?
        else {
            return Err("partitioned join build not ready".to_string());
        };

        let global_rows = self.state.global_build_row_count();
        let global_has_null = self.state.global_build_has_null_key();
        self.core
            .set_build_artifact(artifact, global_rows, global_has_null)?;
        Ok(())
    }

    fn log_stats(&self) {
        debug!(
            "PartitionedJoinProbe finished: dep_key={} driver_id={} partition={} node_id={} join_type={} input_rows={} input_chunks={} buffered_rows={} output_rows={} build_partition_rows={} build_partition_has_null_key={} build_batches={} build_table={} probe_keys={} residual_predicate={} lookup_hit_rows={} lookup_miss_rows={} residual_rows_checked={} residual_eval_batches={} residual_eval_pairs={} residual_matched_rows={} residual_group_rows_total={}",
            self.dep.name(),
            self.driver_id,
            self.partition,
            self.node_id,
            join_type_str(self.core.join_type()),
            self.input_rows,
            self.input_chunks,
            self.buffered_rows,
            self.core.output_rows(),
            self.core.build_partition_row_count(),
            self.core.build_partition_has_null_key(),
            self.core.build_batches_len(),
            self.core.build_table_present(),
            self.core.probe_keys_len(),
            self.core.has_residual_predicate(),
            self.core.lookup_hit_rows(),
            self.core.lookup_miss_rows(),
            self.core.residual_rows_checked(),
            self.core.residual_eval_batches(),
            self.core.residual_eval_pairs(),
            self.core.residual_matched_rows(),
            self.core.residual_group_rows_total()
        );
    }

    fn join_buffered(&mut self, drain_all: bool) -> Result<Option<Chunk>, String> {
        let probe_chunks = self.collect_probe_chunks(None, drain_all);
        self.core.join_probe_chunks(probe_chunks)
    }

    fn collect_probe_chunks(&mut self, chunk: Option<Chunk>, drain_all: bool) -> Vec<Chunk> {
        let mut probe_chunks = Vec::new();
        if let Some(chunk) = chunk {
            if !chunk.is_empty() {
                self.buffered_rows = self.buffered_rows.saturating_add(chunk.len() as u64);
                self.buffered.push_back(chunk);
            }
            if let Some(buffered) = self.buffered.pop_front() {
                if !buffered.is_empty() {
                    self.buffered_rows = self.buffered_rows.saturating_sub(buffered.len() as u64);
                    probe_chunks.push(buffered);
                }
            }
        } else if drain_all {
            while let Some(buffered) = self.buffered.pop_front() {
                if !buffered.is_empty() {
                    self.buffered_rows = self.buffered_rows.saturating_sub(buffered.len() as u64);
                    probe_chunks.push(buffered);
                }
            }
        } else if let Some(buffered) = self.buffered.pop_front() {
            if !buffered.is_empty() {
                self.buffered_rows = self.buffered_rows.saturating_sub(buffered.len() as u64);
                probe_chunks.push(buffered);
            }
        }
        probe_chunks
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::join::JoinDistributionMode;
    use crate::exec::node::join::JoinType;
    use crate::exec::operators::hashjoin::HashJoinBuildSinkFactory;
    use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
    use crate::exec::operators::hashjoin::partitioned_join_shared::PartitionedJoinSharedState;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::exec::pipeline::operator::ProcessorOperator;
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
    use crate::runtime::runtime_state::RuntimeState;

    use super::PartitionedJoinProbeProcessorFactory;

    const LEFT_K_SLOT_ID: SlotId = SlotId::new(1);
    const LEFT_V_SLOT_ID: SlotId = SlotId::new(2);
    const RIGHT_K_SLOT_ID: SlotId = SlotId::new(3);
    const RIGHT_W_SLOT_ID: SlotId = SlotId::new(4);

    fn schema_k(slot_id: SlotId, nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("k", DataType::Int32, nullable),
            slot_id,
        )]))
    }

    fn schema_kv(k_slot_id: SlotId, v_slot_id: SlotId, v_name: &str, nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, nullable), k_slot_id),
            field_with_slot_id(Field::new(v_name, DataType::Int32, nullable), v_slot_id),
        ]))
    }

    fn join_schema(left: &SchemaRef, right: &SchemaRef) -> SchemaRef {
        let mut fields = left.fields().to_vec();
        fields.extend(right.fields().to_vec());
        Arc::new(Schema::new(fields))
    }

    fn push_expect_consumed(op: &mut dyn ProcessorOperator, rt: &RuntimeState, chunk: Chunk) {
        match op.push_chunk(rt, chunk) {
            Ok(()) => {}
            Err(err) => panic!("unexpected error: {:?}", err),
        }
    }

    fn chunk_of(values: &[i32], k_slot_id: SlotId) -> Chunk {
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("k", DataType::Int32, false),
            k_slot_id,
        )]));
        let array = Arc::new(Int32Array::from(values.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        Chunk::new(batch)
    }

    fn chunk_of_two(
        k: &[i32],
        k_slot_id: SlotId,
        v: &[i32],
        v_slot_id: SlotId,
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

    fn chunk_of_two_nullable(
        k: &[Option<i32>],
        k_slot_id: SlotId,
        v: &[Option<i32>],
        v_slot_id: SlotId,
        v_name: &str,
    ) -> Chunk {
        assert_eq!(k.len(), v.len());
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, true), k_slot_id),
            field_with_slot_id(Field::new(v_name, DataType::Int32, true), v_slot_id),
        ]));
        let k_arr = Arc::new(Int32Array::from(k.to_vec())) as arrow::array::ArrayRef;
        let v_arr = Arc::new(Int32Array::from(v.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![k_arr, v_arr]).expect("record batch");
        Chunk::new(batch)
    }

    fn append_quads(chunk: Chunk, out: &mut Vec<(i32, i32, i32, i32)>) {
        let c0 = chunk
            .columns()
            .get(0)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c1 = chunk
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c2 = chunk
            .columns()
            .get(2)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c3 = chunk
            .columns()
            .get(3)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            out.push((c0.value(i), c1.value(i), c2.value(i), c3.value(i)));
        }
    }

    fn append_pairs(chunk: Chunk, out: &mut Vec<(i32, i32)>) {
        assert_eq!(chunk.columns().len(), 2);
        let c0 = chunk
            .columns()
            .get(0)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c1 = chunk
            .columns()
            .get(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..chunk.len() {
            out.push((c0.value(i), c1.value(i)));
        }
    }

    #[test]
    fn partitioned_join_buffers_probe_chunks_until_build_ready() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            1,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_k(LEFT_K_SLOT_ID, false);
        let right_schema = schema_k(RIGHT_K_SLOT_ID, false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
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

        let mut build = build_factory.create(1, 0);
        let mut probe = probe_factory.create(1, 0);

        let probe_proc = probe.as_processor_mut().expect("probe processor");
        assert!(probe_proc.need_input());
        let dep = probe_proc
            .precondition_dependency()
            .expect("build dependency");
        let probe_chunk_1 = chunk_of(&[1, 2], LEFT_K_SLOT_ID);
        let probe_chunk_2 = chunk_of(&[3, 4], LEFT_K_SLOT_ID);

        assert!(!dep.is_ready());
        assert!(probe_proc.need_input());
        push_expect_consumed(probe_proc, &rt, probe_chunk_1);
        push_expect_consumed(probe_proc, &rt, probe_chunk_2);

        push_expect_consumed(
            build.as_processor_mut().expect("build processor"),
            &rt,
            chunk_of(&[1, 2, 3, 4], RIGHT_K_SLOT_ID),
        );
        build
            .as_processor_mut()
            .expect("build processor")
            .set_finishing(&rt)
            .expect("build finishing");

        assert!(dep.is_ready());
        probe_proc.set_finishing(&rt).expect("probe finishing");

        let mut out_rows = 0usize;
        loop {
            match probe_proc.pull_chunk(&rt) {
                Ok(Some(chunk)) => out_rows = out_rows.saturating_add(chunk.len()),
                Ok(None) => break,
                Err(e) => panic!("unexpected probe pull error: {e:?}"),
            }
        }

        assert_eq!(out_rows, 4);
    }

    #[test]
    fn partitioned_inner_join_matches_keys_with_dop_2() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_k(LEFT_K_SLOT_ID, false);
        let right_schema = schema_k(RIGHT_K_SLOT_ID, false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
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

        let build = chunk_of(&[1, 2, 4], RIGHT_K_SLOT_ID);
        let probe = chunk_of(&[2, 3, 4], LEFT_K_SLOT_ID);

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        // Build per partition.
        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            if let Some(sink) = op.as_processor_mut() {
                let c = build_parts[part].clone();
                if !c.is_empty() {
                    push_expect_consumed(sink, &rt, c);
                }
                sink.set_finishing(&rt).expect("build finish");
            } else {
                panic!("build operator missing processor");
            }
        }

        // Probe per partition.
        let mut pairs = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();

            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };
            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
            while proc.has_output() {
                if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                    let left_arr = out
                        .columns()
                        .get(0)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let right_arr = out
                        .columns()
                        .get(1)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    for i in 0..out.len() {
                        pairs.push((left_arr.value(i), right_arr.value(i)));
                    }
                }
            }
            proc.set_finishing(&rt).expect("probe finish");
            while proc.has_output() {
                if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                    let left_arr = out
                        .columns()
                        .get(0)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let right_arr = out
                        .columns()
                        .get(1)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    for i in 0..out.len() {
                        pairs.push((left_arr.value(i), right_arr.value(i)));
                    }
                }
            }
        }

        pairs.sort();
        assert_eq!(pairs, vec![(2, 2), (4, 4)]);
    }

    #[test]
    fn partitioned_inner_join_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::Inner,
            vec![probe_key],
            Some(residual),
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(
            &[1, 1, 2],
            RIGHT_K_SLOT_ID,
            &[5, 1, 10],
            RIGHT_W_SLOT_ID,
            "w",
        );
        let probe = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[3, 9], LEFT_V_SLOT_ID, "v");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
            while proc.has_output() {
                if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                    append_quads(out, &mut rows);
                }
            }
            proc.set_finishing(&rt).expect("probe finish");
            while proc.has_output() {
                if let Some(out) = proc.pull_chunk(&rt).expect("probe pull") {
                    append_quads(out, &mut rows);
                }
            }
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 3, 1, 5), (2, 9, 2, 10)]);
    }

    #[test]
    fn partitioned_left_semi_join_outputs_probe_side() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Left semi: probe is left, build is right.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
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

        let build = chunk_of_two(
            &[1, 1, 3],
            RIGHT_K_SLOT_ID,
            &[10, 11, 30],
            RIGHT_W_SLOT_ID,
            "w",
        );
        let probe = chunk_of_two(
            &[1, 2, 3],
            LEFT_K_SLOT_ID,
            &[100, 200, 300],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 100), (3, 300)]);
    }

    #[test]
    fn partitioned_left_anti_join_outputs_nonmatching_probe_rows() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Left anti: probe is left, build is right.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![probe_key],
            None,
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 3], RIGHT_K_SLOT_ID, &[10, 30], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two(
            &[1, 2, 3],
            LEFT_K_SLOT_ID,
            &[100, 200, 300],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(2, 200)]);
    }

    #[test]
    fn partitioned_right_semi_join_outputs_probe_side() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "l", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "r", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Right semi: probe is right, build is left.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![probe_key],
            None,
            false,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 20], LEFT_V_SLOT_ID, "l");
        let probe = chunk_of_two(&[2, 3], RIGHT_K_SLOT_ID, &[200, 300], RIGHT_W_SLOT_ID, "r");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(2, 200)]);
    }

    #[test]
    fn partitioned_right_semi_join_with_probe_left_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Keep probe on left to match real pipeline topology.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::RightSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
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

        let build = chunk_of_two(
            &[1, 1, 2],
            RIGHT_K_SLOT_ID,
            &[5, 20, 7],
            RIGHT_W_SLOT_ID,
            "w",
        );
        let probe = chunk_of_two(
            &[1, 1, 2],
            LEFT_K_SLOT_ID,
            &[10, 15, 100],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 20)]);
    }

    #[test]
    fn partitioned_left_semi_join_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            Some(residual),
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 1], RIGHT_K_SLOT_ID, &[5, 20], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 7], LEFT_V_SLOT_ID, "v");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 10)]);
    }

    #[test]
    fn partitioned_left_anti_join_applies_other_join_conjuncts() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::LeftAnti,
            vec![probe_key],
            Some(residual),
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 1], RIGHT_K_SLOT_ID, &[5, 20], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two(
            &[1, 1, 2],
            LEFT_K_SLOT_ID,
            &[30, 7, 999],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(1, 30), (2, 999)]);
    }

    #[test]
    fn partitioned_right_anti_join_outputs_nonmatching_probe_rows() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "l", false);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "r", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        // Right anti: probe is right, build is left.
        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::RightAnti,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::RightAnti,
            vec![probe_key],
            None,
            false,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two(&[1, 2], LEFT_K_SLOT_ID, &[10, 20], LEFT_V_SLOT_ID, "l");
        let probe = chunk_of_two(&[2, 3], RIGHT_K_SLOT_ID, &[200, 300], RIGHT_W_SLOT_ID, "r");

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        rows.sort();
        assert_eq!(rows, vec![(3, 300)]);
    }

    #[test]
    fn partitioned_left_semi_join_skips_null_keys() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);
        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", true);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", false);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
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

        let build = chunk_of_two(&[1], RIGHT_K_SLOT_ID, &[10], RIGHT_W_SLOT_ID, "w");
        let probe = chunk_of_two_nullable(
            &[Some(1), None, Some(2)],
            LEFT_K_SLOT_ID,
            &[Some(100), Some(999), Some(200)],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        // Only k=1 matches; NULL key should never match.
        rows.sort();
        assert_eq!(rows, vec![(1, 100)]);
    }

    #[test]
    fn partitioned_left_semi_join_treats_null_residual_as_false() {
        let rt = RuntimeState::default();
        let mut arena = ExprArena::default();
        let build_key = arena.push_typed(ExprNode::SlotId(RIGHT_K_SLOT_ID), DataType::Int32);
        let probe_key = arena.push_typed(ExprNode::SlotId(LEFT_K_SLOT_ID), DataType::Int32);

        let left_v = arena.push_typed(ExprNode::SlotId(LEFT_V_SLOT_ID), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(RIGHT_W_SLOT_ID), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let arena = Arc::new(arena);

        let dep_manager = DependencyManager::new();
        let runtime_filter_hub = Arc::new(RuntimeFilterHub::new(dep_manager.clone()));
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            2,
            dep_manager.clone(),
            false,
        ));

        let left_schema = schema_kv(LEFT_K_SLOT_ID, LEFT_V_SLOT_ID, "v", true);
        let right_schema = schema_kv(RIGHT_K_SLOT_ID, RIGHT_W_SLOT_ID, "w", true);
        let join_scope_schema = join_schema(&left_schema, &right_schema);

        let build_state: Arc<dyn JoinBuildSinkState> = join_state.clone();
        let build_factory = HashJoinBuildSinkFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![build_key],
            vec![false],
            Vec::new(),
            JoinDistributionMode::Partitioned,
            build_state,
            Arc::clone(&runtime_filter_hub),
            None,
        );
        let probe_factory = PartitionedJoinProbeProcessorFactory::new(
            Arc::clone(&arena),
            JoinType::LeftSemi,
            vec![probe_key],
            Some(residual),
            true,
            left_schema,
            right_schema,
            join_scope_schema,
            Arc::clone(&join_state),
        );

        let build = chunk_of_two_nullable(
            &[Some(1)],
            RIGHT_K_SLOT_ID,
            &[Some(5)],
            RIGHT_W_SLOT_ID,
            "w",
        );
        // v=NULL makes residual evaluate to NULL => treated as false.
        let probe = chunk_of_two_nullable(
            &[Some(1), Some(1)],
            LEFT_K_SLOT_ID,
            &[None, Some(3)],
            LEFT_V_SLOT_ID,
            "v",
        );

        let build_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &build,
            &[build_key],
            &arena,
            2,
            false,
        )
        .expect("partition build");
        let probe_parts = crate::exec::operators::data_stream_sink::partition_chunk_by_hash(
            &probe,
            &[probe_key],
            &arena,
            2,
            false,
        )
        .expect("partition probe");

        for part in 0..2 {
            let mut op = build_factory.create(2, part as i32);
            let Some(sink) = op.as_processor_mut() else {
                panic!("build operator missing processor");
            };
            let c = build_parts[part].clone();
            if !c.is_empty() {
                push_expect_consumed(sink, &rt, c);
            }
            sink.set_finishing(&rt).expect("build finish");
        }

        let mut rows = Vec::new();
        for part in 0..2 {
            let mut op = probe_factory.create(2, part as i32);
            let c = probe_parts[part].clone();
            let Some(proc) = op.as_processor_mut() else {
                panic!("probe operator missing processor");
            };

            assert!(proc.need_input());
            push_expect_consumed(proc, &rt, c);
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
        }

        // Only the v=3 row satisfies v<w (3<5). NULL residual row is filtered out.
        rows.sort();
        assert_eq!(rows, vec![(1, 3)]);
    }
}
