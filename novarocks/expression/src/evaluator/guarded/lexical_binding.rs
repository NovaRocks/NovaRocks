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
use crate::evaluator::mapped::{MappedValues, buffer};

impl<K> Frame<K> {
    pub(super) fn attach_lexical_binding(
        &mut self,
        child: Child,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(), KernelFailure> {
        if self.children.is_empty() {
            let mut errors = child.value.row_errors().iter().peekable();
            self.remaining.clear();
            for (position, &parent) in child.ordinals.iter().enumerate() {
                if errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == position)
                {
                    let error = errors.next().expect("the binding error was observed");
                    self.errors
                        .insert(parent, error.with_selected_ordinal(parent));
                } else {
                    self.remaining.push(parent);
                }
                work.step()?;
            }
        }
        self.children.push(child);
        Ok(())
    }

    pub(super) fn read_lexical_binding(
        &self,
        rows: &[usize],
        ty: &DataType,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<OwnedValue, KernelFailure> {
        if self.next != 2 {
            return Err(invalid("local value was demanded outside the lexical body"));
        }
        let initializer = self
            .children
            .first()
            .ok_or_else(|| internal("lexical body has no evaluated initializer"))?;
        let continuation = initializer.value.continuation();
        let value = match initializer.value.uncontinued() {
            OwnedValue::Constant(value) => OwnedValue::Constant(value.clone()),
            OwnedValue::ConstantView(value) => OwnedValue::ConstantView(value.clone()),
            OwnedValue::Column(array) => OwnedValue::Column(Arc::clone(array)),
            OwnedValue::Source(array, domain) => {
                OwnedValue::Source(Arc::clone(array), Arc::clone(domain))
            }
            OwnedValue::Selected(array, _) => {
                let mut selected = MappedValues::new(ty, 1, rows.len(), allocator)?;
                let source = selected.push_source(Arc::clone(array))?;
                for &row in rows {
                    selected.push(Some((source, self.binding_ordinal(initializer, row)?)))?;
                    work.step()?;
                }
                OwnedValue::Mapped(selected)
            }
            OwnedValue::Mapped(value) => {
                let mut selected =
                    MappedValues::new(ty, value.sources().len(), rows.len(), allocator)?;
                for source in value.sources() {
                    selected.push_source(Arc::clone(source))?;
                    work.step()?;
                }
                for &row in rows {
                    selected.push(value.address(self.binding_ordinal(initializer, row)?))?;
                    work.step()?;
                }
                OwnedValue::Mapped(selected)
            }
            OwnedValue::Continued(..) => unreachable!("the binding is unwrapped above"),
        };
        Ok(value.with_continuation(continuation))
    }

    fn binding_ordinal(&self, initializer: &Child, row: usize) -> Result<usize, KernelFailure> {
        let parent = self
            .rows
            .binary_search(&row)
            .map_err(|_| invalid("local value row is outside its lexical binding"))?;
        if self.errors.contains_key(&parent) {
            return Err(invalid("local value row has a failed initializer"));
        }
        initializer
            .ordinals
            .binary_search(&parent)
            .map_err(|_| invalid("local value row was not evaluated by its initializer"))
    }

    pub(super) fn complete_lexical_binding(
        &mut self,
        selection: Selection<'_>,
        ty: &DataType,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<OwnedValue, KernelFailure> {
        let (array, continuation) = if self.remaining.is_empty() {
            work.flush()?;
            let value = new_null_array(ty, selection.len());
            work.flush()?;
            (value, None)
        } else {
            if self.children.len() != 2 {
                return Err(internal(
                    "successful binding rows did not enter the lexical body",
                ));
            }
            let body = self.children.pop().expect("the lexical body was checked");
            if body.ordinals.len() == selection.len() {
                return Ok(body.value);
            }
            let mut rows = buffer(body.ordinals.len())?;
            for &parent in &body.ordinals {
                rows.push(selection.row(parent).ok_or_else(|| {
                    internal("lexical body row is outside its binding selection")
                })?);
                work.step()?;
            }
            let body_domain =
                Selection::try_sparse_observed(selection.batch_rows(), &rows, || work.step())?;
            let value = body.value.into_value_mapped(body_domain, work)?;
            let output = value.materialize(body_domain, ty, work)?;
            for error in output.errors() {
                let parent = body.ordinals[error.selected_ordinal()];
                self.errors
                    .entry(parent)
                    .or_insert_with(|| error.with_selected_ordinal(parent));
                work.step()?;
            }
            let mut indices = buffer(selection.len())?;
            let mut active = body.ordinals.iter().copied().enumerate().peekable();
            for parent in 0..selection.len() {
                let index = if let Some(&(compact, active_parent)) = active.peek()
                    && active_parent == parent
                {
                    active.next();
                    Some(u64::try_from(compact).map_err(|_| KernelFailure::ResourceExhausted)?)
                } else {
                    None
                };
                indices.push(index);
                work.step()?;
            }
            let continuation = output.continuation.clone();
            let array = gather_continued(output.values(), &indices, continuation.as_ref(), work)?;
            (array, continuation)
        };
        let mut errors = buffer(self.errors.len())?;
        for error in std::mem::take(&mut self.errors).into_values() {
            errors.push(error);
            work.step()?;
        }
        Ok(OwnedValue::Selected(array, errors.into_boxed_slice()).with_continuation(continuation))
    }
}
