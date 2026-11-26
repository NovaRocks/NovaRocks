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

//! Ordered arguments and original-row attribution for F10.
use super::*;
use arrow::array::{Int32Array, Int64Array, StringArray};
use novarocks_functions::{PreparedRegexp, RegexpArgument, ScalarInvocationActivation};

pub(super) use novarocks_type_contract::ORDERED_REGEXP_PATTERN_ORDINAL as PATTERN_ORDINAL;

enum Cell<'a> {
    Null,
    Value(&'a ArrayRef, usize),
    Error(&'a RowDataError),
}

impl OwnedValue {
    fn regexp_cell(&self, ordinal: usize, batch_row: usize) -> Result<Cell<'_>, KernelFailure> {
        let (array, row) = match self.uncontinued() {
            Self::Constant(value) => (value.pool().array(), value.ordinal() as usize),
            Self::ConstantView(value) => (&value.values, value.original.ordinal() as usize),
            Self::Column(value) => (value, batch_row),
            Self::Source(value, domain) => {
                let row = domain.physical_ordinal(batch_row).ok_or_else(|| {
                    invalid("ordered regexp source row is outside its checked domain")
                })?;
                (value, row)
            }
            Self::Selected(value, errors) => {
                if let Ok(error) =
                    errors.binary_search_by_key(&ordinal, RowDataError::selected_ordinal)
                {
                    return Ok(Cell::Error(&errors[error]));
                }
                (value, ordinal)
            }
            Self::Mapped(value) => {
                if let Ok(error) = value
                    .errors()
                    .binary_search_by_key(&ordinal, RowDataError::selected_ordinal)
                {
                    return Ok(Cell::Error(&value.errors()[error]));
                }
                let Some(cell) = value.value(ordinal) else {
                    return Ok(Cell::Null);
                };
                cell
            }
            Self::Continued(_, _) => unreachable!("continuation was unwrapped"),
        };
        if row >= array.len() {
            return Err(invalid(
                "ordered regexp argument is outside its checked carrier",
            ));
        }
        if array.is_null(row) || array.data_type() == &DataType::Null {
            Ok(Cell::Null)
        } else {
            Ok(Cell::Value(array, row))
        }
    }
}

fn text(array: &ArrayRef, row: usize) -> Result<&str, KernelFailure> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|values| values.value(row))
        .ok_or_else(|| invalid("ordered regexp text argument differs from its checked carrier"))
}

fn operand(array: &ArrayRef, row: usize) -> Result<RegexpArgument<'_>, KernelFailure> {
    match array.data_type() {
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|values| RegexpArgument::Integer(values.value(row))),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|values| RegexpArgument::Bigint(values.value(row))),
        DataType::Utf8 => return text(array, row).map(RegexpArgument::Text),
        _ => None,
    }
    .ok_or_else(|| invalid("ordered regexp late argument differs from its checked carrier"))
}

pub(super) fn pattern_ordinals<K>(
    frame: &Frame<K>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<Option<Vec<usize>>, KernelFailure> {
    let source = frame
        .children
        .first()
        .ok_or_else(|| internal("missing ordered regexp source"))?;
    let mut active = if frame._body_memory.is_some() {
        super::super::mapped::buffer(frame.rows.len())?
    } else {
        Vec::new()
    };
    for (ordinal, &row) in frame.rows.iter().enumerate() {
        work.step()?;
        if matches!(source.value.regexp_cell(ordinal, row)?, Cell::Value(..)) {
            active.push(ordinal);
        }
    }
    Ok((!active.is_empty()).then_some(active))
}

impl TreeDriver<'_, '_> {
    pub(super) fn evaluate_regexp<R>(
        &mut self,
        prepared: &PreparedRegexp,
        arguments: &[ExpressionUseId],
        frame: &mut Frame,
        rows: &[usize],
        lambda: Option<&LambdaFrame<'_>>,
        instances: &mut BTreeMap<ExpressionUseId, scalar_invocation::CallInstance>,
        work: &mut Work<'_, '_, R>,
    ) -> Result<OwnedValue, ExprEvaluationFailure<R>> {
        if rows.is_empty() {
            let ty = prepared.result_type();
            return complete_undemanded_leaf(ty, work).map_err(Into::into);
        }
        let [source, rest @ ..] = frame.children.as_slice() else {
            return Err(internal("missing ordered regexp source").into());
        };
        let pattern = rest.first();
        if let Some(host) = work.scalar_host.as_deref() {
            host.validate_frame_allocator(self.allocator)?;
        }
        let allocator = self
            .allocator
            .ok_or_else(|| invalid("ordered regexp requires its actual allocation host"))?;
        work.flush()?;
        let mut batch = prepared.start(Arc::clone(allocator), work.control)?;
        let mut pattern_index = 0;
        let mut errors = Vec::new();
        for (ordinal, &row) in rows.iter().enumerate() {
            work.flush()?;
            let source = match source.value.regexp_cell(ordinal, row)? {
                Cell::Value(array, index) => text(array, index)?,
                cell => {
                    if let Cell::Error(error) = cell {
                        errors.push(error.with_selected_ordinal(ordinal));
                    }
                    batch.null(work.control)?;
                    continue;
                }
            };
            let pattern = pattern
                .filter(|pattern| pattern.ordinals.get(pattern_index) == Some(&ordinal))
                .ok_or_else(|| internal("missing ordered regexp pattern row"))?;
            let cell = pattern.value.regexp_cell(pattern_index, row)?;
            pattern_index += 1;
            let pattern = match cell {
                Cell::Value(array, index) => text(array, index)?,
                cell => {
                    if let Cell::Error(error) = cell {
                        errors.push(error.with_selected_ordinal(ordinal));
                    }
                    batch.null(work.control)?;
                    continue;
                }
            };
            let compiled = match batch.pattern(pattern, ordinal, work.control)? {
                Ok(compiled) => compiled,
                Err(error) => {
                    errors.push(error);
                    batch.null(work.control)?;
                    continue;
                }
            };
            // The selected profile has at most two late arguments. Keep
            // their actual values alive until the same row's search is done.
            let mut late = [None, None];
            let mut len = 0;
            let mut terminal = false;
            for &child in &arguments[PATTERN_ORDINAL + 1..] {
                let value = self.regexp_argument(child, frame, row, lambda, instances, work)?;
                match value.regexp_cell(0, row)? {
                    Cell::Null => {
                        terminal = true;
                        break;
                    }
                    Cell::Error(error) => {
                        errors.push(error.with_selected_ordinal(ordinal));
                        terminal = true;
                        break;
                    }
                    Cell::Value(..) => {
                        let slot = late
                            .get_mut(len)
                            .ok_or_else(|| invalid("ordered regexp exceeds its selected arity"))?;
                        *slot = Some(value);
                        len += 1;
                    }
                }
            }
            if terminal {
                batch.null(work.control)?;
                continue;
            }
            let mut values = [RegexpArgument::Integer(0); 2];
            for (slot, value) in values.iter_mut().zip(late.iter().flatten()) {
                let Cell::Value(array, index) = value.regexp_cell(0, row)? else {
                    return Err(internal("ordered regexp lost a successful late argument").into());
                };
                *slot = operand(array, index)?;
            }
            work.flush()?;
            if let Some(error) = compiled.evaluate(ordinal, source, &values[..len], work.control)? {
                errors.push(error);
            }
        }
        work.flush()?;
        let output = batch.finish(work.control)?;
        Ok(OwnedValue::Selected(output, errors.into_boxed_slice()))
    }

    fn regexp_argument<R>(
        &mut self,
        child: ExpressionUseId,
        parent: &Frame,
        row: usize,
        lambda: Option<&LambdaFrame<'_>>,
        instances: &mut BTreeMap<ExpressionUseId, scalar_invocation::CallInstance>,
        work: &mut Work<'_, '_, R>,
    ) -> Result<OwnedValue, ExprEvaluationFailure<R>> {
        let invocation = &self.program.control_flow().uses()[&child];
        let ty = self
            .program
            .definitions()
            .node(invocation.definition)
            .ok_or_else(|| invalid("missing ordered regexp argument definition"))?
            .data_type();
        let original = self.needs_original.contains(&child);
        let domain = if original {
            parent.domain.filtered(&[row], work.control)?
        } else {
            Arc::clone(&parent.domain)
        };
        // Only these re-entries nest; the compiled use graph already bounds
        // call depth. The same driver holds the exact input and instance state.
        let body_memory = parent
            ._body_memory
            .as_ref()
            .map(|memory| {
                let input = super::super::mapped::reserve(Some(&memory.host), size_of::<usize>())?;
                Ok::<_, KernelFailure>(BodyFrameMemory::new(&memory.host, input, None))
            })
            .transpose()?;
        let frame = Frame::new(
            child,
            invocation.control,
            ty,
            vec![row],
            domain,
            original,
            ScalarInvocationActivation::Activated,
            Vec::new(),
            body_memory,
            work,
        )?;
        self.drive(frame, lambda, None, instances, work)
    }
}
