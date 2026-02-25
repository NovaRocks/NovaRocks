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
//! Nested-loop join probe processor.
//!
//! Responsibilities:
//! - Executes row-by-row nested-loop matching between probe and materialized build chunks.
//! - Builds output according to NL-join type semantics for matched and unmatched rows.
//!
//! Key exported interfaces:
//! - Types: `NlJoinProbeProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use arrow::array::Array;
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::nljoin_shared::NlJoinSharedState;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::nljoin::NestedLoopJoinType;
use crate::exec::operators::hashjoin::join_probe_utils::{
    build_join_batch, build_left_with_null_right, build_null_left_with_right,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::runtime_state::RuntimeState;

/// Factory for nested-loop join probe operators that execute row-wise nested-loop matching.
pub struct NlJoinProbeProcessorFactory {
    name: String,
    arena: Arc<ExprArena>,
    join_type: NestedLoopJoinType,
    join_conjunct: Option<ExprId>,
    probe_is_left: bool,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_scope_schema: SchemaRef,
    state: Arc<NlJoinSharedState>,
}

impl NlJoinProbeProcessorFactory {
    pub(crate) fn new(
        arena: Arc<ExprArena>,
        join_type: NestedLoopJoinType,
        join_conjunct: Option<ExprId>,
        probe_is_left: bool,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        join_scope_schema: SchemaRef,
        state: Arc<NlJoinSharedState>,
    ) -> Self {
        let node_id = state.node_id();
        let name = if node_id >= 0 {
            format!("NlJoinProbe (id={node_id})")
        } else {
            "NlJoinProbe".to_string()
        };
        Self {
            name,
            arena,
            join_type,
            join_conjunct,
            probe_is_left,
            left_schema,
            right_schema,
            join_scope_schema,
            state,
        }
    }
}

impl OperatorFactory for NlJoinProbeProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(NlJoinProbeProcessorOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            join_type: self.join_type,
            join_conjunct: self.join_conjunct,
            probe_is_left: self.probe_is_left,
            left_schema: Arc::clone(&self.left_schema),
            right_schema: Arc::clone(&self.right_schema),
            join_scope_schema: Arc::clone(&self.join_scope_schema),
            state: Arc::clone(&self.state),
            build_loaded: false,
            build_batches: Arc::new(Vec::new()),
            probe_chunk: None,
            outer_phase: OuterPhase::ProbeBuild,
            probe_matched: Vec::new(),
            emit_unmatched_probe_row: 0,
            build_matched: None,
            emit_unmatched_build_batch: 0,
            emit_unmatched_build_row: 0,
            build_batch_idx: 0,
            probe_row: 0,
            build_row: 0,
            input_finished: false,
            post_probe_finalized: false,
            finished: false,
        })
    }
}

struct NlJoinProbeProcessorOperator {
    name: String,
    arena: Arc<ExprArena>,
    join_type: NestedLoopJoinType,
    join_conjunct: Option<ExprId>,
    probe_is_left: bool,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_scope_schema: SchemaRef,
    state: Arc<NlJoinSharedState>,

    build_loaded: bool,
    build_batches: Arc<Vec<Chunk>>,

    probe_chunk: Option<Chunk>,
    outer_phase: OuterPhase,
    probe_matched: Vec<bool>,
    emit_unmatched_probe_row: usize,
    build_matched: Option<Vec<Vec<bool>>>,
    emit_unmatched_build_batch: usize,
    emit_unmatched_build_row: usize,
    build_batch_idx: usize,
    probe_row: usize,
    build_row: usize,
    input_finished: bool,
    post_probe_finalized: bool,
    finished: bool,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum OuterPhase {
    ProbeBuild,
    EmitUnmatchedProbe,
    EmitUnmatchedBuild,
    Finished,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ConjunctState {
    True,
    False,
    Null,
}

impl Operator for NlJoinProbeProcessorOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for NlJoinProbeProcessorOperator {
    fn need_input(&self) -> bool {
        if self.finished || self.input_finished {
            return false;
        }
        if self.probe_chunk.is_some() {
            return false;
        }
        if !self.build_loaded && !self.state.has_build() {
            return false;
        }
        if self.state.is_build_empty() && self.should_skip_probe() {
            return false;
        }
        true
    }

    fn has_output(&self) -> bool {
        if self.finished {
            return false;
        }
        if !self.build_loaded && !self.state.has_build() {
            return false;
        }
        if self.state.is_build_empty() && self.should_skip_probe() {
            return true;
        }
        if self.probe_chunk.is_some() {
            return true;
        }
        if self.input_finished {
            if self.join_type == NestedLoopJoinType::FullOuter {
                if !self.post_probe_finalized {
                    return true;
                }
                return self.outer_phase != OuterPhase::Finished;
            }
            return true;
        }
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("nljoin probe received input when it does not need input".to_string());
        }
        if !self.build_loaded {
            self.ensure_build_loaded()?;
        }
        self.probe_chunk = Some(chunk);
        self.build_batch_idx = 0;
        self.probe_row = 0;
        self.build_row = 0;
        if matches!(
            self.join_type,
            NestedLoopJoinType::LeftOuter
                | NestedLoopJoinType::RightOuter
                | NestedLoopJoinType::FullOuter
        ) {
            let probe_len = self.probe_chunk.as_ref().map(|c| c.len()).unwrap_or(0);
            self.probe_matched = vec![false; probe_len];
            self.emit_unmatched_probe_row = 0;
            self.outer_phase = OuterPhase::ProbeBuild;
            if self.join_type == NestedLoopJoinType::FullOuter {
                self.init_build_matched_if_needed();
            }
        }
        Ok(())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }
        if self.state.is_build_empty() && self.should_skip_probe() {
            self.finished = true;
            return Ok(Some(self.empty_progress_chunk()));
        }
        if !self.build_loaded {
            self.ensure_build_loaded()?;
        }
        if self.input_finished && self.probe_chunk.is_none() {
            self.finalize_post_probe()?;
        }

        let had_probe = self.probe_chunk.is_some();
        let chunk_size = state.chunk_size();
        let out = match self.join_type {
            NestedLoopJoinType::Inner | NestedLoopJoinType::Cross => {
                self.read_inner_cross(chunk_size)
            }
            NestedLoopJoinType::LeftOuter
            | NestedLoopJoinType::RightOuter
            | NestedLoopJoinType::FullOuter => self.read_outer(chunk_size),
            NestedLoopJoinType::LeftSemi
            | NestedLoopJoinType::LeftAnti
            | NestedLoopJoinType::NullAwareLeftAnti => self.read_left_semi_anti(),
        }?;

        if let Some(chunk) = out {
            if self.is_done() {
                self.finished = true;
            }
            return Ok(Some(chunk));
        }

        if self.is_done() {
            self.finished = true;
            return Ok(Some(self.empty_progress_chunk()));
        }

        if had_probe && self.probe_chunk.is_none() {
            // Signal progress when input is consumed but no rows are produced.
            return Ok(Some(self.empty_progress_chunk()));
        }
        if self.input_finished && !self.post_probe_finalized {
            // Allow one final pull to advance the finishing state.
            return Ok(Some(self.empty_progress_chunk()));
        }
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.input_finished = true;
        Ok(())
    }

    fn precondition_dependency(
        &self,
    ) -> Option<crate::exec::pipeline::dependency::DependencyHandle> {
        if self.build_loaded || self.state.has_build() {
            None
        } else {
            Some(self.state.build_dep())
        }
    }
}

impl NlJoinProbeProcessorOperator {
    fn is_done(&self) -> bool {
        match self.join_type {
            NestedLoopJoinType::LeftOuter
            | NestedLoopJoinType::RightOuter
            | NestedLoopJoinType::FullOuter => self.outer_phase == OuterPhase::Finished,
            _ => self.probe_drained(),
        }
    }

    fn probe_drained(&self) -> bool {
        self.input_finished && self.probe_chunk.is_none()
    }

    fn should_skip_probe(&self) -> bool {
        matches!(
            self.join_type,
            NestedLoopJoinType::Inner | NestedLoopJoinType::Cross | NestedLoopJoinType::LeftSemi
        )
    }

    fn empty_progress_chunk(&self) -> Chunk {
        let schema = match self.join_type {
            NestedLoopJoinType::LeftSemi
            | NestedLoopJoinType::LeftAnti
            | NestedLoopJoinType::NullAwareLeftAnti => {
                if self.probe_is_left {
                    Arc::clone(&self.left_schema)
                } else {
                    Arc::clone(&self.right_schema)
                }
            }
            _ => Arc::clone(&self.join_scope_schema),
        };
        Chunk::new(RecordBatch::new_empty(schema))
    }

    fn ensure_build_loaded(&mut self) -> Result<(), String> {
        if self.build_loaded {
            return Ok(());
        }
        let Some(artifact) = self.state.get_build() else {
            return Err("nljoin build not ready".to_string());
        };
        self.build_batches = Arc::clone(&artifact.build_batches);
        self.build_loaded = true;
        Ok(())
    }

    fn finalize_post_probe(&mut self) -> Result<(), String> {
        if self.post_probe_finalized {
            return Ok(());
        }
        self.post_probe_finalized = true;
        if self.join_type != NestedLoopJoinType::FullOuter {
            return Ok(());
        }

        // FULL OUTER needs a global view of build matches; the last prober emits unmatched build rows.
        let local_flags = if self.build_matched.is_some() {
            self.build_matched.take()
        } else if !self.build_batches.is_empty() {
            let mut flags = Vec::with_capacity(self.build_batches.len());
            for batch in self.build_batches.iter() {
                flags.push(vec![false; batch.len()]);
            }
            Some(flags)
        } else {
            None
        };
        let is_last = self.state.finish_probe(local_flags);
        if is_last {
            self.build_matched = self.state.shared_build_matched();
            if self.build_batches.is_empty() || self.build_matched.is_none() {
                self.outer_phase = OuterPhase::Finished;
            } else {
                self.emit_unmatched_build_batch = 0;
                self.emit_unmatched_build_row = 0;
                self.outer_phase = OuterPhase::EmitUnmatchedBuild;
            }
        } else {
            self.outer_phase = OuterPhase::Finished;
            self.finished = true;
        }
        Ok(())
    }

    fn read_inner_cross(&mut self, chunk_size: usize) -> Result<Option<Chunk>, String> {
        loop {
            if self.build_batches.is_empty() {
                if self.probe_chunk.is_some() {
                    self.probe_chunk = None;
                }
                return Ok(None);
            }

            if self.probe_chunk.is_none() {
                return Ok(None);
            }

            let Some(probe_chunk) = self.probe_chunk.clone() else {
                continue;
            };
            if probe_chunk.is_empty() {
                self.probe_chunk = None;
                continue;
            }

            if self.build_batch_idx >= self.build_batches.len() {
                self.build_batch_idx = 0;
                self.probe_chunk = None;
                continue;
            }

            let right = self
                .build_batches
                .get(self.build_batch_idx)
                .ok_or_else(|| "nljoin build batch missing".to_string())?
                .clone();
            if right.is_empty() {
                self.build_batch_idx += 1;
                self.probe_row = 0;
                self.build_row = 0;
                continue;
            }

            let output_schema = Arc::clone(&self.join_scope_schema);
            let out = self.permute_pairs_one_build_batch(
                &probe_chunk,
                &right,
                &output_schema,
                chunk_size,
            )?;
            let Some(batch) = out else {
                continue;
            };

            let batch = self.apply_join_conjunct(batch)?;
            if batch.num_rows() == 0 {
                continue;
            }
            return Ok(Some(Chunk::new(batch)));
        }
    }

    fn permute_pairs_one_build_batch(
        &mut self,
        probe_chunk: &Chunk,
        right: &Chunk,
        output_schema: &SchemaRef,
        chunk_size: usize,
    ) -> Result<Option<arrow::record_batch::RecordBatch>, String> {
        let left_len = probe_chunk.len();
        let right_len = right.len();
        if left_len == 0 || right_len == 0 {
            return Ok(None);
        }

        if self.probe_row >= left_len {
            self.probe_row = 0;
            self.build_row = 0;
            self.build_batch_idx += 1;
            return Ok(None);
        }

        let desired = chunk_size;
        let mut left_indices = Vec::with_capacity(desired);
        let mut right_indices = Vec::with_capacity(desired);

        while left_indices.len() < desired && self.probe_row < left_len {
            while left_indices.len() < desired && self.build_row < right_len {
                left_indices.push(self.probe_row as u32);
                right_indices.push(self.build_row as u32);
                self.build_row += 1;
            }
            if self.build_row >= right_len {
                self.build_row = 0;
                self.probe_row += 1;
            }
        }

        if self.probe_row >= left_len {
            self.probe_row = 0;
            self.build_row = 0;
            self.build_batch_idx += 1;
            if self.build_batch_idx >= self.build_batches.len() {
                self.build_batch_idx = 0;
                self.probe_chunk = None;
            }
        }

        build_join_batch(
            probe_chunk,
            right,
            &left_indices,
            &right_indices,
            output_schema,
        )
    }

    fn read_left_semi_anti(&mut self) -> Result<Option<Chunk>, String> {
        let Some(probe_chunk) = self.probe_chunk.take() else {
            return Ok(None);
        };
        if probe_chunk.is_empty() {
            return Ok(None);
        }

        if self.build_batches.is_empty() {
            if matches!(
                self.join_type,
                NestedLoopJoinType::LeftAnti | NestedLoopJoinType::NullAwareLeftAnti
            ) {
                return Ok(Some(probe_chunk));
            }
            return Ok(None);
        }

        let join_scope_schema = Arc::clone(&self.join_scope_schema);

        let keep = self.eval_probe_keep_flags(&probe_chunk, &join_scope_schema)?;
        let mask = BooleanArray::from(keep);
        let filtered = filter_record_batch(&probe_chunk.batch, &mask)
            .map_err(|e| format!("nljoin semi/anti filter failed: {e}"))?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        Ok(Some(Chunk::new(filtered)))
    }

    fn read_outer(&mut self, chunk_size: usize) -> Result<Option<Chunk>, String> {
        let preserve_probe = matches!(
            self.join_type,
            NestedLoopJoinType::LeftOuter
                | NestedLoopJoinType::RightOuter
                | NestedLoopJoinType::FullOuter
        );
        let preserve_build = self.join_type == NestedLoopJoinType::FullOuter;

        loop {
            match self.outer_phase {
                OuterPhase::Finished => return Ok(None),
                OuterPhase::EmitUnmatchedBuild => {
                    let Some(batch) = self.emit_unmatched_build_chunk(chunk_size)? else {
                        self.outer_phase = OuterPhase::Finished;
                        return Ok(None);
                    };
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    return Ok(Some(Chunk::new(batch)));
                }
                OuterPhase::EmitUnmatchedProbe => {
                    let Some(batch) = self.emit_unmatched_probe_chunk(chunk_size)? else {
                        self.outer_phase = OuterPhase::ProbeBuild;
                        self.probe_chunk = None;
                        self.probe_matched.clear();
                        self.emit_unmatched_probe_row = 0;
                        continue;
                    };
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    return Ok(Some(Chunk::new(batch)));
                }
                OuterPhase::ProbeBuild => {}
            }

            if self.probe_chunk.is_none() {
                if !self.input_finished {
                    return Ok(None);
                }
                if preserve_build {
                    if self.join_type == NestedLoopJoinType::FullOuter {
                        return Ok(None);
                    }
                    self.init_build_matched_if_needed();
                    self.outer_phase = OuterPhase::EmitUnmatchedBuild;
                    continue;
                }
                self.outer_phase = OuterPhase::Finished;
                return Ok(None);
            }

            let Some(probe_chunk) = self.probe_chunk.clone() else {
                continue;
            };
            if probe_chunk.is_empty() {
                self.probe_chunk = None;
                self.probe_matched.clear();
                self.emit_unmatched_probe_row = 0;
                continue;
            }

            if self.build_batches.is_empty() {
                if preserve_probe {
                    self.outer_phase = OuterPhase::EmitUnmatchedProbe;
                    continue;
                }
                self.probe_chunk = None;
                self.probe_matched.clear();
                self.emit_unmatched_probe_row = 0;
                continue;
            }

            if self.build_batch_idx >= self.build_batches.len() {
                if preserve_probe {
                    self.outer_phase = OuterPhase::EmitUnmatchedProbe;
                    continue;
                }
                self.probe_chunk = None;
                self.probe_matched.clear();
                self.emit_unmatched_probe_row = 0;
                continue;
            }

            let build_batch = self
                .build_batches
                .get(self.build_batch_idx)
                .ok_or_else(|| "nljoin build batch missing".to_string())?
                .clone();
            let build_batch_idx = self.build_batch_idx;
            if build_batch.is_empty() {
                self.build_batch_idx += 1;
                self.probe_row = 0;
                self.build_row = 0;
                continue;
            }

            let (candidate_batch, probe_indices, build_indices) =
                self.permute_pairs_one_build_batch_outer(&probe_chunk, &build_batch, chunk_size)?;
            let Some(candidate_batch) = candidate_batch else {
                continue;
            };

            let filtered = self.apply_join_conjunct_and_mark(
                candidate_batch,
                &probe_indices,
                &build_indices,
                preserve_build,
                build_batch_idx,
            )?;
            let Some(filtered) = filtered else {
                continue;
            };
            if filtered.num_rows() == 0 {
                continue;
            }
            return Ok(Some(Chunk::new(filtered)));
        }
    }

    fn init_build_matched_if_needed(&mut self) {
        if self.build_matched.is_some() {
            return;
        }
        let mut flags = Vec::with_capacity(self.build_batches.len());
        for b in self.build_batches.iter() {
            flags.push(vec![false; b.len()]);
        }
        self.build_matched = Some(flags);
        self.emit_unmatched_build_batch = 0;
        self.emit_unmatched_build_row = 0;
    }

    fn emit_unmatched_probe_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Option<arrow::record_batch::RecordBatch>, String> {
        let Some(probe_chunk) = self.probe_chunk.as_ref() else {
            return Ok(None);
        };
        if self.emit_unmatched_probe_row >= self.probe_matched.len() {
            return Ok(None);
        }

        let desired = chunk_size;
        let mut indices = Vec::with_capacity(desired);
        while indices.len() < desired && self.emit_unmatched_probe_row < self.probe_matched.len() {
            let row = self.emit_unmatched_probe_row;
            self.emit_unmatched_probe_row += 1;
            if self.probe_matched.get(row).copied().unwrap_or(false) {
                continue;
            }
            indices.push(row as u32);
        }
        if indices.is_empty() {
            return Ok(None);
        }

        if self.probe_is_left {
            build_left_with_null_right(
                probe_chunk,
                &indices,
                &self.right_schema,
                &self.join_scope_schema,
            )
        } else {
            build_null_left_with_right(
                probe_chunk,
                &indices,
                &self.left_schema,
                &self.join_scope_schema,
            )
        }
    }

    fn emit_unmatched_build_chunk(
        &mut self,
        chunk_size: usize,
    ) -> Result<Option<arrow::record_batch::RecordBatch>, String> {
        let Some(flags) = self.build_matched.as_ref() else {
            return Ok(None);
        };
        if self.emit_unmatched_build_batch >= self.build_batches.len() {
            return Ok(None);
        }

        let desired = chunk_size;
        while self.emit_unmatched_build_batch < self.build_batches.len() {
            let batch_idx = self.emit_unmatched_build_batch;
            let build_batch = self
                .build_batches
                .get(batch_idx)
                .ok_or_else(|| "nljoin build batch missing".to_string())?;
            let matched = flags
                .get(batch_idx)
                .ok_or_else(|| "nljoin build match flags missing".to_string())?;

            let mut indices = Vec::with_capacity(desired);
            while indices.len() < desired && self.emit_unmatched_build_row < matched.len() {
                let row = self.emit_unmatched_build_row;
                self.emit_unmatched_build_row += 1;
                if matched.get(row).copied().unwrap_or(false) {
                    continue;
                }
                indices.push(row as u32);
            }

            if !indices.is_empty() {
                return build_null_left_with_right(
                    build_batch,
                    &indices,
                    &self.left_schema,
                    &self.join_scope_schema,
                );
            }

            self.emit_unmatched_build_batch += 1;
            self.emit_unmatched_build_row = 0;
        }

        Ok(None)
    }

    fn permute_pairs_one_build_batch_outer(
        &mut self,
        probe_chunk: &Chunk,
        build_batch: &Chunk,
        chunk_size: usize,
    ) -> Result<(Option<arrow::record_batch::RecordBatch>, Vec<u32>, Vec<u32>), String> {
        let probe_len = probe_chunk.len();
        let build_len = build_batch.len();
        if probe_len == 0 || build_len == 0 {
            return Ok((None, Vec::new(), Vec::new()));
        }
        if self.probe_row >= probe_len {
            self.probe_row = 0;
            self.build_row = 0;
            self.build_batch_idx += 1;
            if self.build_batch_idx >= self.build_batches.len() {
                self.build_batch_idx = 0;
                self.outer_phase = OuterPhase::EmitUnmatchedProbe;
            }
            return Ok((None, Vec::new(), Vec::new()));
        }

        let desired = chunk_size;
        let mut probe_indices = Vec::with_capacity(desired);
        let mut build_indices = Vec::with_capacity(desired);

        while probe_indices.len() < desired && self.probe_row < probe_len {
            while probe_indices.len() < desired && self.build_row < build_len {
                probe_indices.push(self.probe_row as u32);
                build_indices.push(self.build_row as u32);
                self.build_row += 1;
            }
            if self.build_row >= build_len {
                self.build_row = 0;
                self.probe_row += 1;
            }
        }

        if self.probe_row >= probe_len {
            self.probe_row = 0;
            self.build_row = 0;
            self.build_batch_idx += 1;
            if self.build_batch_idx >= self.build_batches.len() {
                self.build_batch_idx = 0;
                self.outer_phase = OuterPhase::EmitUnmatchedProbe;
            }
        }

        let join_scope_schema = Arc::clone(&self.join_scope_schema);
        if self.probe_is_left {
            let batch = build_join_batch(
                probe_chunk,
                build_batch,
                &probe_indices,
                &build_indices,
                &join_scope_schema,
            )?;
            Ok((batch, probe_indices, build_indices))
        } else {
            let batch = build_join_batch(
                build_batch,
                probe_chunk,
                &build_indices,
                &probe_indices,
                &join_scope_schema,
            )?;
            Ok((batch, probe_indices, build_indices))
        }
    }

    fn apply_join_conjunct_and_mark(
        &mut self,
        batch: arrow::record_batch::RecordBatch,
        probe_indices: &[u32],
        build_indices: &[u32],
        preserve_build: bool,
        build_batch_idx: usize,
    ) -> Result<Option<arrow::record_batch::RecordBatch>, String> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let matched_mask = if let Some(pred) = self.join_conjunct {
            let chunk = Chunk::new(batch.clone());
            let mask_arr = self.arena.eval(pred, &chunk).map_err(|e| e.to_string())?;
            mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "nljoin join conjunct must return boolean array".to_string())?
                .clone()
        } else {
            BooleanArray::from(vec![true; batch.num_rows()])
        };

        for i in 0..matched_mask.len() {
            if !matched_mask.is_valid(i) || !matched_mask.value(i) {
                continue;
            }
            let probe_row = probe_indices
                .get(i)
                .copied()
                .ok_or_else(|| "nljoin probe index out of bounds".to_string())?
                as usize;
            if probe_row < self.probe_matched.len() {
                self.probe_matched[probe_row] = true;
            }
            if preserve_build {
                let build_row = build_indices
                    .get(i)
                    .copied()
                    .ok_or_else(|| "nljoin build index out of bounds".to_string())?
                    as usize;
                if let Some(flags) = self.build_matched.as_mut() {
                    if let Some(batch_flags) = flags.get_mut(build_batch_idx) {
                        if build_row < batch_flags.len() {
                            batch_flags[build_row] = true;
                        }
                    }
                }
            }
        }

        if self.join_conjunct.is_none() {
            return Ok(Some(batch));
        }

        filter_record_batch(&batch, &matched_mask)
            .map(Some)
            .map_err(|e| format!("nljoin join conjunct filter failed: {e}"))
    }

    fn eval_probe_keep_flags(
        &self,
        probe_chunk: &Chunk,
        join_scope_schema: &SchemaRef,
    ) -> Result<Vec<bool>, String> {
        let is_semi = self.join_type == NestedLoopJoinType::LeftSemi;
        let is_anti = self.join_type == NestedLoopJoinType::LeftAnti;
        let is_null_aware_anti = self.join_type == NestedLoopJoinType::NullAwareLeftAnti;
        if !is_semi && !is_anti && !is_null_aware_anti {
            return Err("nljoin semi/anti keep flags called with invalid join type".to_string());
        }

        let null_aware_split = if is_null_aware_anti {
            self.join_conjunct
                .map(|pred| Self::split_null_aware_conjuncts(self.arena.as_ref(), pred))
        } else {
            None
        };

        let mut keep = Vec::with_capacity(probe_chunk.len());

        for probe_row in 0..probe_chunk.len() {
            let (has_true, has_null) = if is_null_aware_anti {
                match (self.join_conjunct, null_aware_split.as_ref()) {
                    (Some(pred), Some((key_conjuncts, residual_conjuncts)))
                        if !key_conjuncts.is_empty() =>
                    {
                        self.probe_row_match_flags_null_aware(
                            probe_chunk,
                            probe_row,
                            key_conjuncts,
                            residual_conjuncts,
                            join_scope_schema,
                        )?
                    }
                    (Some(pred), _) => {
                        self.probe_row_match_flags(probe_chunk, probe_row, pred, join_scope_schema)?
                    }
                    (None, _) => (true, false),
                }
            } else if let Some(pred) = self.join_conjunct {
                self.probe_row_match_flags(probe_chunk, probe_row, pred, join_scope_schema)?
            } else {
                (true, false)
            };
            keep.push(if is_semi {
                has_true
            } else if is_anti {
                !has_true
            } else {
                !has_true && !has_null
            });
        }

        Ok(keep)
    }

    fn split_null_aware_conjuncts(arena: &ExprArena, pred: ExprId) -> (Vec<ExprId>, Vec<ExprId>) {
        fn flatten_and(arena: &ExprArena, expr: ExprId, out: &mut Vec<ExprId>) {
            match arena.node(expr) {
                Some(ExprNode::And(left, right)) => {
                    flatten_and(arena, *left, out);
                    flatten_and(arena, *right, out);
                }
                _ => out.push(expr),
            }
        }

        let mut conjuncts = Vec::new();
        flatten_and(arena, pred, &mut conjuncts);

        let mut key_conjuncts = Vec::new();
        let mut residual_conjuncts = Vec::new();
        let mut reading_key = true;
        for expr in conjuncts {
            let is_key = matches!(
                arena.node(expr),
                Some(ExprNode::Eq(_, _) | ExprNode::EqForNull(_, _))
            );
            if reading_key && is_key {
                key_conjuncts.push(expr);
            } else {
                reading_key = false;
                residual_conjuncts.push(expr);
            }
        }
        (key_conjuncts, residual_conjuncts)
    }

    fn eval_conjunct_masks(
        &self,
        chunk: &Chunk,
        conjuncts: &[ExprId],
    ) -> Result<Vec<BooleanArray>, String> {
        let mut out = Vec::with_capacity(conjuncts.len());
        for expr in conjuncts {
            let mask_arr = self.arena.eval(*expr, chunk).map_err(|e| e.to_string())?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "nljoin join conjunct must return boolean array".to_string())?
                .clone();
            out.push(mask);
        }
        Ok(out)
    }

    fn eval_conjunct_row_state(
        masks: &[BooleanArray],
        row: usize,
    ) -> Result<ConjunctState, String> {
        if masks.is_empty() {
            return Ok(ConjunctState::True);
        }
        let mut has_null = false;
        for mask in masks {
            if row >= mask.len() {
                return Err(format!(
                    "nljoin conjunct row out of bounds: row={} mask_len={}",
                    row,
                    mask.len()
                ));
            }
            if !mask.is_valid(row) {
                has_null = true;
                continue;
            }
            if !mask.value(row) {
                return Ok(ConjunctState::False);
            }
        }
        if has_null {
            Ok(ConjunctState::Null)
        } else {
            Ok(ConjunctState::True)
        }
    }

    fn probe_row_match_flags_null_aware(
        &self,
        probe_chunk: &Chunk,
        probe_row: usize,
        key_conjuncts: &[ExprId],
        residual_conjuncts: &[ExprId],
        join_scope_schema: &SchemaRef,
    ) -> Result<(bool, bool), String> {
        let mut has_true = false;
        let mut has_null = false;
        for right in self.build_batches.iter() {
            let right_len = right.len();
            if right_len == 0 {
                continue;
            }
            let left_indices = vec![probe_row as u32; right_len];
            let right_indices = (0..right_len).map(|i| i as u32).collect::<Vec<_>>();
            let Some(batch) = build_join_batch(
                probe_chunk,
                right,
                &left_indices,
                &right_indices,
                join_scope_schema,
            )?
            else {
                continue;
            };

            let joined_chunk = Chunk::new(batch);
            let key_masks = self.eval_conjunct_masks(&joined_chunk, key_conjuncts)?;
            let residual_masks = self.eval_conjunct_masks(&joined_chunk, residual_conjuncts)?;

            for i in 0..joined_chunk.len() {
                if Self::eval_conjunct_row_state(&residual_masks, i)? != ConjunctState::True {
                    continue;
                }
                match Self::eval_conjunct_row_state(&key_masks, i)? {
                    ConjunctState::True => {
                        has_true = true;
                        break;
                    }
                    ConjunctState::Null => has_null = true,
                    ConjunctState::False => {}
                }
            }
            if has_true {
                return Ok((true, has_null));
            }
        }
        Ok((false, has_null))
    }

    fn probe_row_match_flags(
        &self,
        probe_chunk: &Chunk,
        probe_row: usize,
        pred: ExprId,
        join_scope_schema: &SchemaRef,
    ) -> Result<(bool, bool), String> {
        let mut has_true = false;
        let mut has_null = false;
        for right in self.build_batches.iter() {
            let right_len = right.len();
            if right_len == 0 {
                continue;
            }
            let left_indices = vec![probe_row as u32; right_len];
            let right_indices = (0..right_len).map(|i| i as u32).collect::<Vec<_>>();
            let Some(batch) = build_join_batch(
                probe_chunk,
                right,
                &left_indices,
                &right_indices,
                join_scope_schema,
            )?
            else {
                continue;
            };

            let mask_arr = self
                .arena
                .eval(pred, &Chunk::new(batch))
                .map_err(|e| e.to_string())?;
            let mask = mask_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "nljoin join conjunct must return boolean array".to_string())?;
            for i in 0..mask.len() {
                if mask.is_valid(i) {
                    if mask.value(i) {
                        has_true = true;
                        break;
                    }
                } else {
                    has_null = true;
                }
            }
            if has_true {
                return Ok((true, has_null));
            }
        }
        Ok((false, has_null))
    }

    fn apply_join_conjunct(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        let Some(pred) = self.join_conjunct else {
            return Ok(batch);
        };

        let chunk = Chunk::new(batch);
        let mask_arr = self.arena.eval(pred, &chunk).map_err(|e| e.to_string())?;
        let mask = mask_arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "nljoin join conjunct must return boolean array".to_string())?;
        filter_record_batch(&chunk.batch, mask)
            .map_err(|e| format!("nljoin join conjunct filter failed: {e}"))
    }
}
