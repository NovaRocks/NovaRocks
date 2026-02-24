//! Generic source side for DISTINCT set-operation result emission.
//!
//! Responsibilities:
//! - Reads shared state and emits rows selected by operation-specific set semantics.
//! - Handles completion ordering and shared error propagation before producing output.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSourceFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatchOptions, UInt32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;

use super::distinct_set_shared::{DistinctSetSemantics, DistinctSetSharedState};

pub struct DistinctSetSourceFactory<S: DistinctSetSemantics> {
    name: String,
    state: DistinctSetSharedState<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSourceFactory<S> {
    pub(crate) fn new(state: DistinctSetSharedState<S>, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("{} (id={node_id})", S::SOURCE_OPERATOR_NAME)
        } else {
            S::SOURCE_OPERATOR_NAME.to_string()
        };
        Self { name, state }
    }
}

impl<S: DistinctSetSemantics> OperatorFactory for DistinctSetSourceFactory<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(DistinctSetSourceOperator::<S> {
            name: self.name.clone(),
            state: self.state.clone(),
            output_slots: self.state.output_slots().to_vec(),
            prepared: false,
            finished: false,
            arrays: Vec::new(),
            indices: Vec::new(),
            offset: 0,
            _semantics: PhantomData,
        })
    }

    fn is_source(&self) -> bool {
        true
    }
}

struct DistinctSetSourceOperator<S: DistinctSetSemantics> {
    name: String,
    state: DistinctSetSharedState<S>,
    output_slots: Vec<SlotId>,
    prepared: bool,
    finished: bool,
    arrays: Vec<ArrayRef>,
    indices: Vec<u32>,
    offset: usize,
    _semantics: PhantomData<S>,
}

impl<S: DistinctSetSemantics> Operator for DistinctSetSourceOperator<S> {
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

impl<S: DistinctSetSemantics> ProcessorOperator for DistinctSetSourceOperator<S> {
    fn need_input(&self) -> bool {
        false
    }

    fn has_output(&self) -> bool {
        if self.is_finished() {
            return false;
        }
        let output_stage = self.state.controller().stage_total();
        self.state.controller().is_stage_ready(output_stage)
    }

    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err(S::SOURCE_REJECT_INPUT_ERROR.to_string())
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if self.finished {
            return Ok(None);
        }

        let output_stage = self.state.controller().stage_total();
        if !self.state.controller().is_stage_ready(output_stage) {
            return Ok(None);
        }

        if !self.prepared {
            let (arrays, indices) = self.state.snapshot_output()?;
            self.arrays = arrays;
            self.indices = indices;
            self.prepared = true;
        }

        if self.indices.is_empty() {
            self.finished = true;
            return Ok(None);
        }
        if self.offset >= self.indices.len() {
            self.finished = true;
            return Ok(None);
        }

        let batch_size = state.chunk_size();
        let end = (self.offset + batch_size).min(self.indices.len());
        let slice = self.indices[self.offset..end].to_vec();
        self.offset = end;

        let indices = UInt32Array::from(slice);
        let indices_ref = Arc::new(indices) as ArrayRef;

        let mut out_arrays = Vec::with_capacity(self.arrays.len());
        for col in &self.arrays {
            let taken = arrow::compute::take(col.as_ref(), &indices_ref, None)
                .map_err(|e| format!("Arrow take failed: {}", e))?;
            out_arrays.push(taken);
        }

        if self.output_slots.len() != out_arrays.len() {
            return Err(format!(
                "{}: slots={} cols={}",
                S::SOURCE_SLOT_MISMATCH_PREFIX,
                self.output_slots.len(),
                out_arrays.len()
            ));
        }

        let schema = Arc::new(Schema::new(
            out_arrays
                .iter()
                .zip(self.output_slots.iter())
                .enumerate()
                .map(|(idx, (arr, slot))| {
                    let field = Field::new(format!("col_{}", idx), arr.data_type().clone(), true);
                    Arc::new(field_with_slot_id(field, *slot))
                })
                .collect::<Vec<_>>(),
        ));

        let batch = if out_arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(indices_ref.len()));
            RecordBatch::try_new_with_options(schema, out_arrays, &options)
        } else {
            RecordBatch::try_new(schema, out_arrays)
        }
        .map_err(|e| e.to_string())?;

        Ok(Some(Chunk::new(batch)))
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        if self.finished {
            return None;
        }
        Some(self.state.controller().observable())
    }
}
