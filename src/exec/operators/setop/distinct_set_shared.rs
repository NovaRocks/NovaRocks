//! Shared state foundation for DISTINCT set-operation execution.
//!
//! Responsibilities:
//! - Maintains hash-based row membership and stage coordination for multi-input set operations.
//! - Applies operation-specific probe/update/output semantics through `DistinctSetSemantics`.
//!
//! Key exported interfaces:
//! - Types: `DistinctSetSharedState`, `DistinctSetSemantics`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use arrow::array::ArrayRef;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::hash_table::key_builder::build_group_key_views;
use crate::exec::hash_table::key_table::KeyTable;

use super::set_op_stage::SetOpStageController;

pub trait DistinctSetSemantics: Clone + Send + Sync + 'static {
    type Marker: Clone + Send + Sync + 'static;

    const NODE_NAME: &'static str;
    const GROUP_ID_SEQUENCE_MISMATCH: &'static str;
    const GROUP_ID_OUT_OF_BOUNDS: &'static str;
    const SINK_OPERATOR_NAME: &'static str;
    const SOURCE_OPERATOR_NAME: &'static str;
    const SOURCE_REJECT_INPUT_ERROR: &'static str;
    const SOURCE_SLOT_MISMATCH_PREFIX: &'static str;

    fn new_marker() -> Self::Marker;
    fn apply_probe(marker: &mut Self::Marker, stage: usize) -> Result<(), String>;
    fn marker_selected(marker: &Self::Marker, stage_total: usize) -> Result<bool, String>;
}

struct DistinctSetInner<M> {
    key_table: Option<KeyTable>,
    markers: Vec<M>,
}

#[derive(Clone)]
pub(crate) struct DistinctSetSharedState<S: DistinctSetSemantics> {
    controller: SetOpStageController,
    output_slots: Vec<SlotId>,
    inner: Arc<Mutex<DistinctSetInner<S::Marker>>>,
    _semantics: PhantomData<S>,
}

impl<S: DistinctSetSemantics> DistinctSetSharedState<S> {
    pub(crate) fn new(controller: SetOpStageController, output_slots: Vec<SlotId>) -> Self {
        Self {
            controller,
            output_slots,
            inner: Arc::new(Mutex::new(DistinctSetInner {
                key_table: None,
                markers: Vec::new(),
            })),
            _semantics: PhantomData,
        }
    }

    pub(crate) fn controller(&self) -> &SetOpStageController {
        &self.controller
    }

    pub(crate) fn output_slots(&self) -> &[SlotId] {
        &self.output_slots
    }

    pub(crate) fn ingest_build(&self, chunk: &Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        let arrays = chunk.columns().to_vec();
        if arrays.is_empty() {
            return Err(format!("{} with zero columns is unsupported", S::NODE_NAME));
        }
        let key_views = build_group_key_views(&arrays)?;
        let num_rows = chunk.len();

        let mut guard = self.inner.lock().expect("distinct set state lock");
        if guard.key_table.is_none() {
            let key_types = arrays
                .iter()
                .map(|a| a.data_type().clone())
                .collect::<Vec<_>>();
            guard.key_table = Some(KeyTable::new(key_types, false)?);
        }
        let (rows, hashes) = {
            let table = guard.key_table.as_mut().expect("key table");
            let rows = table.build_rows(&arrays)?;
            let hashes = table.build_group_hashes(&key_views, num_rows)?;
            (rows, hashes)
        };

        for row in 0..num_rows {
            let row_bytes = rows.row(row).data();
            let lookup = {
                let table = guard.key_table.as_mut().expect("key table");
                table.find_or_insert_from_row(&key_views, row, row_bytes, hashes[row])?
            };
            if lookup.is_new {
                if lookup.group_id != guard.markers.len() {
                    return Err(S::GROUP_ID_SEQUENCE_MISMATCH.to_string());
                }
                guard.markers.push(S::new_marker());
            }
        }
        Ok(())
    }

    pub(crate) fn ingest_probe(&self, stage: usize, chunk: &Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        let arrays = chunk.columns().to_vec();
        if arrays.is_empty() {
            return Err(format!("{} with zero columns is unsupported", S::NODE_NAME));
        }
        let key_views = build_group_key_views(&arrays)?;
        let num_rows = chunk.len();

        let mut guard = self.inner.lock().expect("distinct set state lock");
        if guard.key_table.is_none() {
            return Ok(());
        };
        let (rows, hashes) = {
            let table = guard.key_table.as_ref().expect("key table");
            let rows = table.build_rows(&arrays)?;
            let hashes = table.build_group_hashes(&key_views, num_rows)?;
            (rows, hashes)
        };

        for row in 0..num_rows {
            let row_bytes = rows.row(row).data();
            let group_id_opt = {
                let table = guard.key_table.as_ref().expect("key table");
                table.lookup_serialized(row_bytes, hashes[row])?
            };
            let Some(group_id) = group_id_opt else {
                continue;
            };
            let marker = guard
                .markers
                .get_mut(group_id)
                .ok_or_else(|| S::GROUP_ID_OUT_OF_BOUNDS.to_string())?;
            S::apply_probe(marker, stage)?;
        }
        Ok(())
    }

    pub(crate) fn finish_stage(&self, stage: usize) -> Result<(), String> {
        self.controller.producer_finished(stage)
    }

    pub(crate) fn snapshot_output(&self) -> Result<(Vec<ArrayRef>, Vec<u32>), String> {
        let stage_total = self.controller.stage_total();
        let guard = self.inner.lock().expect("distinct set state lock");
        let Some(table) = guard.key_table.as_ref() else {
            return Ok((Vec::new(), Vec::new()));
        };
        if guard.markers.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut indices = Vec::new();
        for (group_id, marker) in guard.markers.iter().enumerate() {
            if S::marker_selected(marker, stage_total)? {
                indices.push(group_id as u32);
            }
        }
        let arrays = table
            .key_columns()
            .iter()
            .map(|c| c.to_array())
            .collect::<Result<Vec<_>, _>>()?;
        Ok((arrays, indices))
    }
}
