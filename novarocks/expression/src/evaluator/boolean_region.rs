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

//! Ordered Boolean regions settle an actual row error as soon as its operand
//! completes. Successful rows continue according to the consumer's demand.
use super::*;
use arrow::array::{Array, BooleanArray};
use novarocks_type_contract::EvaluationDemand;

pub(super) struct BooleanRows {
    decided: Vec<Option<bool>>,
    saw_null: Vec<bool>,
}
impl BooleanRows {
    pub(super) fn new(
        rows: usize,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Self, KernelFailure> {
        let mut decided = Vec::with_capacity(rows);
        let mut saw_null = Vec::with_capacity(rows);
        for _ in 0..rows {
            decided.push(None);
            saw_null.push(false);
            work.step()?;
        }
        Ok(Self { decided, saw_null })
    }

    pub(super) fn consume(
        &mut self,
        output: &SelectedValues<'_>,
        ordinals: &[usize],
        shape: ControlShape,
        demand: EvaluationDemand,
        terminal: &mut BTreeMap<usize, RowDataError>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Vec<usize>, KernelFailure> {
        let values = output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| invalid("Boolean operand differs from its frozen carrier"))?;
        self.consume_rows(
            output.errors(),
            ordinals,
            shape,
            demand,
            terminal,
            |ordinal| Ok((!values.is_null(ordinal)).then(|| values.value(ordinal))),
            work,
        )
    }

    pub(super) fn consume_mapped(
        &mut self,
        value: &super::mapped::MappedValues,
        ordinals: &[usize],
        shape: ControlShape,
        demand: EvaluationDemand,
        terminal: &mut BTreeMap<usize, RowDataError>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Vec<usize>, KernelFailure> {
        self.consume_rows(
            value.errors(),
            ordinals,
            shape,
            demand,
            terminal,
            |ordinal| value.boolean(ordinal),
            work,
        )
    }

    fn consume_rows(
        &mut self,
        errors: &[RowDataError],
        ordinals: &[usize],
        shape: ControlShape,
        demand: EvaluationDemand,
        terminal: &mut BTreeMap<usize, RowDataError>,
        mut value_at: impl FnMut(usize) -> Result<Option<bool>, KernelFailure>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Vec<usize>, KernelFailure> {
        let mut errors = errors.iter().peekable();
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
                terminal.insert(parent, error.with_selected_ordinal(parent));
            } else {
                match value_at(ordinal)? {
                    None if shape == ControlShape::Conjunction
                        && demand == EvaluationDemand::TruthOnly =>
                    {
                        self.decided[parent] = Some(false);
                    }
                    None => {
                        self.saw_null[parent] = true;
                        remaining.push(parent);
                    }
                    Some(value) if value == deciding => {
                        self.decided[parent] = Some(deciding);
                    }
                    Some(_) => remaining.push(parent),
                }
            }
            work.step()?;
        }
        Ok(remaining)
    }
    pub(super) fn finish(
        self,
        shape: ControlShape,
        demand: EvaluationDemand,
        terminal: BTreeMap<usize, RowDataError>,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<(ArrayRef, Box<[RowDataError]>), KernelFailure> {
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
