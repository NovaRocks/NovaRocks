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

//! Actual ordered Boolean regions. Only row data errors can be pending;
//! outer failures never enter this state or become a successful NULL.
use super::*;
use arrow::array::{Array, BooleanArray};
use novarocks_type_contract::EvaluationDemand;

pub(super) struct BooleanRows {
    decided: Vec<Option<bool>>,
    saw_null: Vec<bool>,
    pending: BTreeMap<usize, RowDataError>,
}
impl BooleanRows {
    pub(super) fn new(rows: usize, work: &mut Work<'_, '_>) -> Result<Self, KernelFailure> {
        let mut decided = Vec::with_capacity(rows);
        let mut saw_null = Vec::with_capacity(rows);
        for _ in 0..rows {
            decided.push(None);
            saw_null.push(false);
            work.step()?;
        }
        Ok(Self {
            decided,
            saw_null,
            pending: BTreeMap::new(),
        })
    }
    pub(super) fn boundary(
        &mut self,
        terminal: &mut BTreeMap<usize, RowDataError>,
        remaining: &mut Vec<usize>,
        work: &mut Work<'_, '_>,
    ) -> Result<(), KernelFailure> {
        for (ordinal, error) in std::mem::take(&mut self.pending) {
            terminal.insert(ordinal, error);
            work.step()?;
        }
        let mut next = Vec::with_capacity(remaining.len());
        for ordinal in std::mem::take(remaining) {
            if !terminal.contains_key(&ordinal) {
                next.push(ordinal);
            }
            work.step()?;
        }
        *remaining = next;
        Ok(())
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep the exact operand mapping, demand, scoped effect verdict and caller-owned error/control domains explicit"
    )]
    pub(super) fn consume(
        &mut self,
        output: &SelectedValues<'_>,
        ordinals: &[usize],
        shape: ControlShape,
        demand: EvaluationDemand,
        pure: bool,
        terminal: &mut BTreeMap<usize, RowDataError>,
        work: &mut Work<'_, '_>,
    ) -> Result<Vec<usize>, KernelFailure> {
        let values = output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("Boolean operand differs from its frozen carrier"))?;
        let mut errors = output.errors().iter().peekable();
        let mut remaining = Vec::with_capacity(ordinals.len());
        let deciding = shape == ControlShape::Disjunction;
        for (ordinal, &parent) in ordinals.iter().enumerate() {
            if errors
                .peek()
                .is_some_and(|error| error.selected_ordinal() == ordinal)
            {
                let error = errors
                    .next()
                    .ok_or_else(|| internal("missing actual Boolean operand error"))?;
                if pure {
                    self.pending
                        .entry(parent)
                        .or_insert_with(|| error.with_selected_ordinal(parent));
                    remaining.push(parent);
                } else {
                    terminal.insert(parent, error.with_selected_ordinal(parent));
                }
            } else if values.is_null(ordinal) {
                if shape == ControlShape::Conjunction && demand == EvaluationDemand::TruthOnly {
                    self.decided[parent] = Some(false);
                    self.pending.remove(&parent);
                } else {
                    self.saw_null[parent] = true;
                    remaining.push(parent);
                }
            } else if values.value(ordinal) == deciding {
                self.decided[parent] = Some(deciding);
                self.pending.remove(&parent);
            } else {
                remaining.push(parent);
            }
            work.step()?;
        }
        Ok(remaining)
    }
    pub(super) fn finish(
        self,
        shape: ControlShape,
        demand: EvaluationDemand,
        mut terminal: BTreeMap<usize, RowDataError>,
        work: &mut Work<'_, '_>,
    ) -> Result<(ArrayRef, Box<[RowDataError]>), KernelFailure> {
        for (ordinal, error) in self.pending {
            terminal.insert(ordinal, error);
            work.step()?;
        }
        let mut output = Vec::with_capacity(self.decided.len());
        for (ordinal, decided) in self.decided.into_iter().enumerate() {
            output.push(if terminal.contains_key(&ordinal) {
                None
            } else if let Some(value) = decided {
                Some(value)
            } else if self.saw_null[ordinal] {
                if demand == EvaluationDemand::TruthOnly {
                    Some(false)
                } else {
                    None
                }
            } else {
                Some(shape == ControlShape::Conjunction)
            });
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(BooleanArray::from(output)) as ArrayRef;
        work.flush()?;
        let mut errors = Vec::with_capacity(terminal.len());
        for error in terminal.into_values() {
            errors.push(error);
            work.step()?;
        }
        Ok((array, errors.into_boxed_slice()))
    }
}

#[cfg(test)]
mod tests;
