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

//! Ordered row demand for a scalar leaf with ordinary evaluated arguments.

use super::*;
use novarocks_functions::PreparedScalarKernel;

impl<K> Frame<K> {
    pub(super) fn attach_ordered_null(
        &mut self,
        child: Child,
        rejects_null: bool,
        batch_rows: usize,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        let mut rows = Vec::new();
        rows.try_reserve_exact(child.ordinals.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for &parent in &child.ordinals {
            rows.push(self.rows[parent]);
            work.step()?;
        }
        let selection = Selection::try_sparse_observed(batch_rows, &rows, || work.step())?;
        let value = child.value.into_value_mapped(selection, work)?;
        if let Value::Mapped(value) = value.uncontinued()
            && value.len() != selection.len()
        {
            return Err(internal("mapped argument differs from its selected rows"));
        }
        let mut errors = value.errors().iter().peekable();
        self.remaining.clear();
        let mut apply = |ordinal: usize, is_null: bool| {
            let parent = child.ordinals[ordinal];
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == ordinal)
            {
                let error = errors
                    .next()
                    .ok_or_else(|| internal("missing ordered argument row error"))?;
                self.errors
                    .insert(parent, error.with_selected_ordinal(parent));
            } else if !is_null {
                self.remaining.push(parent);
            }
            Ok::<_, KernelFailure>(())
        };
        if !rejects_null {
            for ordinal in 0..selection.len() {
                apply(ordinal, false)?;
                work.step()?;
            }
        } else if let Value::Mapped(value) = value.uncontinued() {
            for ordinal in 0..selection.len() {
                apply(ordinal, value.is_null(ordinal, work)?)?;
                work.step()?;
            }
        } else {
            work.flush()?;
            visit_selected_nulls(
                value.argument()?,
                selection,
                work.control,
                |ordinal, _, is_null| apply(ordinal, is_null),
            )?;
        }
        if self.remaining.is_empty() {
            work.flush()?;
            self.children.clear();
            return work.flush();
        }
        self.children.push(Child {
            ordinals: child.ordinals,
            ordinal_storage: child.ordinal_storage,
            value: retain_argument(value),
        });
        Ok(())
    }
}

impl<K: Ord + Clone> Frame<K> {
    pub(super) fn complete_ordered_null<R>(
        &mut self,
        prepared: &Arc<dyn PreparedScalarKernel>,
        selection: Selection<'_>,
        parent_row: Option<super::super::scalar_groups::ParentRow<'_>>,
        instances: &mut BTreeMap<K, scalar_invocation::CallInstance>,
        allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
        work: &mut Work<'_, '_, R>,
    ) -> Result<OwnedValue, ExprEvaluationFailure<R>> {
        let prepared = scalar_invocation::PreparedCall::Kernel(prepared);
        let ty = prepared.contract().result_type().data_type();
        // Only an original invocation ABI has a demanded empty-call
        // lifecycle. Ordinary scalar kernels are skipped on empty selection.
        let result = if self.remaining.is_empty() {
            work.flush()?;
            new_null_array(ty, selection.len())
        } else {
            if self.children.len() != prepared.contract().call().logical_argument_count() {
                return Err(internal("active ordered scalar rows require every argument").into());
            }
            let mut active_rows = Vec::new();
            let active = if self.remaining.len() == selection.len() {
                selection
            } else {
                active_rows
                    .try_reserve_exact(self.remaining.len())
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                for &ordinal in &self.remaining {
                    active_rows.push(selection.row(ordinal).ok_or_else(|| {
                        internal("ordered argument row is outside its original selection")
                    })?);
                    work.step()?;
                }
                Selection::try_sparse_observed(selection.batch_rows(), &active_rows, || {
                    work.step()
                })?
            };
            let mut arguments = Vec::new();
            arguments
                .try_reserve_exact(self.children.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for child in std::mem::take(&mut self.children) {
                let value = if child.ordinals.len() == active.len() {
                    child.value
                } else {
                    project_argument(
                        child.value,
                        &child.ordinals,
                        &self.remaining,
                        selection,
                        &active_rows,
                        work,
                    )?
                };
                arguments.push(value.into_value_mapped(active, work)?);
                work.step()?;
            }
            let output = evaluate_scalar(
                self.occurrence.clone(),
                prepared,
                &arguments,
                active,
                self.activation,
                parent_row,
                instances,
                allocator,
                work,
            )?;
            if active.len() == selection.len() {
                return Ok(OwnedValue::from_selected(output));
            }
            let (_, array, leaf_errors) = output.into_parts();
            for error in leaf_errors {
                let parent = self.remaining[error.selected_ordinal()];
                self.errors
                    .insert(parent, error.with_selected_ordinal(parent));
                work.step()?;
            }
            let mut indices = Vec::new();
            indices
                .try_reserve_exact(selection.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for _ in 0..selection.len() {
                indices.push(None);
                work.step()?;
            }
            for (compact, parent) in self.remaining.iter().copied().enumerate() {
                indices[parent] =
                    Some(u64::try_from(compact).map_err(|_| KernelFailure::ResourceExhausted)?);
                work.step()?;
            }
            gather(&array, &indices, work)?
        };
        let mut errors = Vec::new();
        errors
            .try_reserve_exact(self.errors.len())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        for error in std::mem::take(&mut self.errors).into_values() {
            errors.push(error);
            work.step()?;
        }
        Ok(OwnedValue::from_selected(SelectedValues::try_new_observed(
            selection,
            ty,
            result,
            errors.into_boxed_slice(),
            || work.step(),
        )?))
    }
}

fn project_argument(
    value: OwnedValue,
    previous_ordinals: &[usize],
    active_ordinals: &[usize],
    selection: Selection<'_>,
    active_rows: &[usize],
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<OwnedValue, KernelFailure> {
    match value {
        OwnedValue::Mapped(mut value) => {
            let positions = active_ordinals.iter().map(|parent| {
                previous_ordinals
                    .binary_search(parent)
                    .map_err(|_| internal("active ordered argument row was never evaluated"))
            });
            value.retain_successful_positions(positions, work)?;
            Ok(OwnedValue::Mapped(value))
        }
        OwnedValue::Continued(value, owner) => {
            let value = project_argument(
                *value,
                previous_ordinals,
                active_ordinals,
                selection,
                active_rows,
                work,
            )?;
            Ok(value.with_continuation(Some(owner)))
        }
        OwnedValue::Selected(..) => {
            let mut previous_rows = Vec::new();
            previous_rows
                .try_reserve_exact(previous_ordinals.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            for &ordinal in previous_ordinals {
                previous_rows.push(selection.row(ordinal).ok_or_else(|| {
                    internal("ordered argument row is outside its original selection")
                })?);
                work.step()?;
            }
            project_owned(
                value,
                &previous_rows,
                active_rows,
                selection.batch_rows(),
                work,
            )
        }
        _ => Ok(value),
    }
}

fn retain_argument(value: Value<'_>) -> OwnedValue {
    match value {
        Value::Constant(value) => OwnedValue::Constant(value),
        Value::ConstantView(value) => OwnedValue::ConstantView(value),
        Value::Column(array) => OwnedValue::Column(array),
        Value::Selected(selected) => OwnedValue::from_selected(selected),
        Value::Mapped(value) => OwnedValue::Mapped(value),
        Value::Continued(value, owner) => retain_argument(*value).with_continuation(Some(owner)),
    }
}
