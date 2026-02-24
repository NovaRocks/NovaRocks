//! Generic sink side for DISTINCT set-operation stage ingestion.
//!
//! Responsibilities:
//! - Feeds stage-specific rows into shared distinct set-operation membership tracking.
//! - Signals stage completion for downstream result materialization.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::runtime_state::RuntimeState;

use super::distinct_set_shared::{DistinctSetSemantics, DistinctSetSharedState};

pub struct DistinctSetSinkFactory<S: DistinctSetSemantics> {
    name: String,
    stage: usize,
    state: DistinctSetSharedState<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSinkFactory<S> {
    pub(crate) fn new(stage: usize, state: DistinctSetSharedState<S>, node_id: i32) -> Self {
        let name = if node_id >= 0 {
            format!("{} (id={node_id})", S::SINK_OPERATOR_NAME)
        } else {
            S::SINK_OPERATOR_NAME.to_string()
        };
        Self { name, stage, state }
    }
}

impl<S: DistinctSetSemantics> OperatorFactory for DistinctSetSinkFactory<S> {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(DistinctSetSinkOperator::<S> {
            name: self.name.clone(),
            stage: self.stage,
            state: self.state.clone(),
            finished: false,
            _semantics: PhantomData,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct DistinctSetSinkOperator<S: DistinctSetSemantics> {
    name: String,
    stage: usize,
    state: DistinctSetSharedState<S>,
    finished: bool,
    _semantics: PhantomData<S>,
}

impl<S: DistinctSetSemantics> Operator for DistinctSetSinkOperator<S> {
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

impl<S: DistinctSetSemantics> ProcessorOperator for DistinctSetSinkOperator<S> {
    fn need_input(&self) -> bool {
        !self.is_finished() && self.state.controller().is_stage_ready(self.stage)
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished || chunk.is_empty() {
            return Ok(());
        }
        if self.stage == 0 {
            self.state.ingest_build(&chunk)?;
        } else {
            self.state.ingest_probe(self.stage, &chunk)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.state.finish_stage(self.stage)?;
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        if self.finished {
            return None;
        }
        Some(self.state.controller().observable())
    }
}
