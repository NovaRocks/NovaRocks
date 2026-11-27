// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_functions::native_inlist::{
    InRows, JsonConstantKeyCache, JsonProbeKeys, PreparedMembership,
};

pub(super) struct GroupRows {
    equality: InRows,
    deferred: Vec<Child>,
    _deferred_storage: Option<OpaqueReservation>,
}

impl GroupRows {
    fn retain_candidate(
        &mut self,
        child: Child,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        if self.deferred.len() == self.deferred.capacity() {
            let capacity = self
                .deferred
                .capacity()
                .checked_mul(2)
                .map(|capacity| capacity.max(1))
                .ok_or(KernelFailure::ResourceExhausted)?;
            work.flush()?;
            let storage =
                super::mapped::reserve(allocator, super::mapped::bytes::<Child>(capacity)?)?;
            let mut values = super::mapped::buffer(capacity)?;
            for value in self.deferred.drain(..) {
                values.push(value);
                work.step()?;
            }
            self.deferred = values;
            self._deferred_storage = storage;
            work.flush()?;
        }
        self.deferred.push(child);
        Ok(())
    }
}

impl<K> Frame<K> {
    pub(super) fn attach_candidate_group(
        &mut self,
        child: Child,
        prepared: &PreparedMembership,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        for error in child.value.row_errors() {
            let parent = *child
                .ordinals
                .get(error.selected_ordinal())
                .ok_or_else(|| invalid("IN source error has no original row"))?;
            self.errors
                .entry(parent)
                .or_insert_with(|| error.with_selected_ordinal(parent));
            work.step()?;
        }
        if self.next == 1 {
            work.flush()?;
            if let Some(host) = work.scalar_host.as_deref() {
                host.validate_frame_allocator(allocator)?;
            }
            let mut equality = in_observed(InRows::begin_in_observed(
                self.rows.len(),
                allocator,
                &mut |event| in_event(event, work),
            ))?;
            self.remaining.clear();
            for (local, &parent) in child.ordinals.iter().enumerate() {
                if !self.errors.contains_key(&parent) {
                    let probe = row_value(&child, local, self.rows[parent])?;
                    let is_null = match probe {
                        Some((argument, row)) => novarocks_functions::inspect_logical_null(
                            argument.array().as_ref(),
                            row,
                            || work.step(),
                        )?,
                        None => true,
                    };
                    if !is_null {
                        self.remaining.push(parent);
                    } else {
                        equality.record_equality(parent, None)?;
                    }
                }
                work.step()?;
            }
            self.candidate_group = Some(GroupRows {
                equality,
                deferred: Vec::new(),
                _deferred_storage: None,
            });
            self.children.push(child);
            return Ok(());
        }

        let state = self
            .candidate_group
            .as_mut()
            .ok_or_else(|| internal("IN candidate completed without its probe"))?;
        let mut live = 0;
        for index in 0..self.remaining.len() {
            let parent = self.remaining[index];
            if !self.errors.contains_key(&parent) {
                self.remaining[live] = parent;
                live += 1;
            }
            work.step()?;
        }
        self.remaining.truncate(live);
        let Some(recipe) = prepared.comparison().streaming_equality() else {
            if !self.remaining.is_empty() {
                state.retain_candidate(child, allocator, work)?;
            } else {
                work.flush()?;
                state.deferred = Vec::new();
                state._deferred_storage = None;
                work.flush()?;
            }
            return Ok(());
        };

        let probe = self
            .children
            .first()
            .ok_or_else(|| internal("IN lost its probe"))?;
        for (local, &parent) in child.ordinals.iter().enumerate() {
            if !self.errors.contains_key(&parent) && state.equality.needs_comparison(parent)? {
                let batch_row = self.rows[parent];
                let probe_local = probe
                    .ordinals
                    .binary_search(&parent)
                    .map_err(|_| invalid("IN candidate row is absent from its probe"))?;
                let (left, left_row) = row_value(probe, probe_local, batch_row)?
                    .ok_or_else(|| invalid("IN comparison has an undemanded probe"))?;
                let equality =
                    if let Some((right, right_row)) = row_value(&child, local, batch_row)? {
                        work.flush()?;
                        recipe.compare_rows(
                            left,
                            left_row,
                            left_row,
                            right,
                            right_row,
                            right_row,
                            work.control,
                        )?
                    } else {
                        None
                    };
                state.equality.record_equality(parent, equality)?;
            }
            work.step()?;
        }
        work.flush()?;
        drop(child);
        work.flush()?;
        Ok(())
    }

    pub(super) fn complete_candidate_group(
        &mut self,
        prepared: &PreparedMembership,
        original_rows: &[usize],
        selection: Selection<'_>,
        demand: EvaluationDemand,
        json_constants: &mut JsonConstantKeyCache,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<OwnedValue, KernelFailure> {
        let Some(mut state) = self.candidate_group.take() else {
            if selection.is_empty() {
                return complete_undemanded_leaf(&DataType::Boolean, work);
            }
            return Err(internal("IN completed without its probe phase"));
        };
        if !state.deferred.is_empty() && !self.remaining.is_empty() {
            let probe = self
                .children
                .pop()
                .ok_or_else(|| internal("IN lost its deferred probe"))?;
            self.compare_deferred_candidates(
                &mut state,
                probe,
                prepared,
                original_rows,
                selection,
                json_constants,
                allocator,
                work,
            )?;
        }
        work.flush()?;
        let _failed_storage =
            super::mapped::reserve(allocator, super::mapped::bytes::<usize>(self.errors.len())?)?;
        let mut failed = super::mapped::buffer(self.errors.len())?;
        for &parent in self.errors.keys() {
            failed.push(parent);
            work.step()?;
        }
        let array = Arc::new(in_observed(state.equality.finish_group_observed(
            demand == EvaluationDemand::TruthOnly,
            &failed,
            &mut |event| in_event(event, work),
        ))?);
        let mut errors = super::mapped::buffer(self.errors.len())?;
        for error in std::mem::take(&mut self.errors).into_values() {
            errors.push(error);
            work.step()?;
        }
        Ok(OwnedValue::from_selected(SelectedValues::try_new_observed(
            selection,
            &DataType::Boolean,
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )?))
    }
}

impl<K> Frame<K> {
    fn compare_deferred_candidates(
        &mut self,
        state: &mut GroupRows,
        probe: Child,
        prepared: &PreparedMembership,
        original_rows: &[usize],
        selection: Selection<'_>,
        json_constants: &mut JsonConstantKeyCache,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        let comparison = prepared.comparison();
        work.flush()?;
        let scratch_rows = self
            .remaining
            .len()
            .checked_mul(2)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let _scratch =
            super::mapped::reserve(allocator, super::mapped::bytes::<usize>(scratch_rows)?)?;
        let mut parents = super::mapped::buffer(self.remaining.len())?;
        let mut rows = super::mapped::buffer(self.remaining.len())?;
        let mut json_probe = None;
        for candidate in state.deferred.drain(..) {
            parents.clear();
            rows.clear();
            for &parent in &self.remaining {
                if !self.errors.contains_key(&parent) && state.equality.needs_comparison(parent)? {
                    parents.push(parent);
                    rows.push(original_rows[parent]);
                }
                work.step()?;
            }
            if parents.is_empty() {
                work.flush()?;
                break;
            }
            let batch_rows = selection.batch_rows();
            let selected = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
            let constant = match candidate.value.uncontinued() {
                OwnedValue::Constant(value) => Some(value.clone()),
                OwnedValue::ConstantView(value) => Some(value.original.clone()),
                _ => None,
            };
            let ty = comparison.source_type().data_type();
            let right = if comparison.uses_json_keys() {
                constant
                    .as_ref()
                    .map(|value| value.pool().array().slice(value.ordinal() as usize, 1))
            } else {
                None
            };
            let right_value;
            let right = if let Some(array) = &right {
                array
            } else {
                let value = project_comparison_operand(
                    candidate,
                    &parents,
                    original_rows,
                    selected,
                    batch_rows,
                    work,
                )?;
                right_value = value.materialize(selected, ty, work)?;
                right_value.values()
            };
            work.flush()?;
            let control = work.control;
            let equalities = if comparison.uses_json_keys() {
                let probe_keys = match &mut json_probe {
                    Some(keys) => keys,
                    None => {
                        // The key cache uses original probe ordinals and normalizes
                        // only rows that actually participate in a comparison.
                        let probe_value = duplicate_child(&probe, work)?
                            .value
                            .into_value(selection, work)?;
                        let probe_values = probe_value.materialize(selection, ty, work)?;
                        let keys = JsonProbeKeys::prepare(
                            probe_values.values(),
                            allocator.cloned(),
                            control,
                        )?;
                        json_probe.insert(keys)
                    }
                };
                in_observed(
                    comparison.json_equalities_observed(
                        probe_keys,
                        right,
                        &parents,
                        constant
                            .as_ref()
                            .map(|constant| (constant, &mut *json_constants)),
                        allocator.cloned(),
                        control,
                        &mut |event| in_event(event, work),
                    ),
                )?
            } else {
                let left = project_comparison_operand(
                    duplicate_child(&probe, work)?,
                    &parents,
                    original_rows,
                    selected,
                    batch_rows,
                    work,
                )?;
                let left = left.materialize(selected, ty, work)?;
                work.flush()?;
                in_observed(comparison.membership_equalities_observed(
                    left.values(),
                    right,
                    allocator.cloned(),
                    control,
                    &mut |event| in_event(event, work),
                ))?
            };
            in_observed(equalities.apply_to_selected_observed(
                &mut state.equality,
                &parents,
                &mut |event| in_event(event, work),
            ))?;
            for error in equalities.errors() {
                let parent = *parents
                    .get(error.selected_ordinal())
                    .ok_or_else(|| internal("IN comparison error has no original row"))?;
                self.errors
                    .insert(parent, error.with_selected_ordinal(parent));
                work.step()?;
            }
            work.flush()?;
        }
        Ok(())
    }
}

fn row_value(
    child: &Child,
    local: usize,
    batch_row: usize,
) -> Result<Option<(EvaluatedArgument<'_>, usize)>, KernelFailure> {
    let pair = match child.value.uncontinued() {
        OwnedValue::Constant(value) => {
            (EvaluatedArgument::Constant(value), value.ordinal() as usize)
        }
        OwnedValue::ConstantView(value) => (value.argument(), value.original.ordinal() as usize),
        OwnedValue::Column(array) => (EvaluatedArgument::Column(array), batch_row),
        OwnedValue::Source(array, domain) => {
            let physical = domain
                .physical_ordinal(batch_row)
                .ok_or_else(|| invalid("IN source row is outside its actual invocation"))?;
            (EvaluatedArgument::Column(array), physical)
        }
        OwnedValue::Selected(array, _) => (EvaluatedArgument::Column(array), local),
        OwnedValue::Mapped(values) => {
            let Some((array, row)) = values.value(local) else {
                return Ok(None);
            };
            (EvaluatedArgument::Column(array), row)
        }
        OwnedValue::Continued(..) => unreachable!("continuation was unwrapped"),
    };
    if pair.1 >= pair.0.array().len() {
        return Err(invalid("IN row is outside its evaluated value"));
    }
    Ok(Some(pair))
}
