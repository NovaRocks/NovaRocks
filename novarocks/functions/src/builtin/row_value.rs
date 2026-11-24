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

//! The installed TRUNCATE bindings, effects and preparation share each exact owner.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray, UInt64Array};
use arrow_schema::DataType;

use super::collection_arrow::{ArrowSite, arrow_failure};
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    Selection,
};

#[derive(Clone, Copy, Debug)]
pub(super) enum RowOperation {
    Construct,
    Project(usize),
}

pub(super) fn evaluate<'a>(
    operation: RowOperation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let selection = input.selection();
    let target = input.contract().result_type().data_type();
    let values = match operation {
        RowOperation::Construct => {
            let DataType::Struct(fields) = target else {
                return Err(invalid("ROW constructor requires its frozen Struct result"));
            };
            let mut children = Vec::with_capacity(input.arguments().len());
            for &argument in input.arguments() {
                work.step()?;
                children.push(select(
                    argument,
                    argument.array(),
                    None,
                    selection,
                    &mut work,
                )?);
            }
            work.flush()?;
            let result =
                StructArray::try_new_with_length(fields.clone(), children, None, selection.len())
                    .map_err(|source| arrow_failure("row", ArrowSite::Construct, source))?;
            Arc::new(result) as ArrayRef
        }
        RowOperation::Project(ordinal) => {
            let [argument, _] = input.arguments() else {
                return Err(invalid(
                    "ROW projection requires the source and frozen ordinal",
                ));
            };
            let row = argument
                .array()
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("ROW projection requires a Struct carrier"))?;
            let child = row
                .columns()
                .get(ordinal)
                .ok_or_else(|| invalid("ROW carrier lacks its frozen field ordinal"))?;
            select(*argument, child, Some(row), selection, &mut work)?
        }
    };
    let output = SelectedValues::try_new_observed::<KernelFailure>(
        selection,
        target,
        values,
        Box::default(),
        || work.step(),
    )?;
    work.flush()?;
    Ok(output)
}

fn select(
    argument: EvaluatedArgument<'_>,
    values: &ArrayRef,
    parent: Option<&StructArray>,
    selection: Selection<'_>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    // Compact children and contiguous full inputs already use the exact
    // wrapper-selected domain. They retain their source through the same Arc.
    let parent_valid = parent.is_none_or(|row| row.null_count() == 0);
    if parent_valid {
        if matches!(argument, EvaluatedArgument::SelectedColumn(_))
            || selection.is_all() && matches!(argument, EvaluatedArgument::Column(_))
        {
            return Ok(Arc::clone(values));
        }
        if let Some(first_row) = selection.row(0)
            && (selection.len() == 1
                || matches!(argument, EvaluatedArgument::Column(_))
                    && selection.row(selection.len() - 1) == Some(first_row + selection.len() - 1))
        {
            let offset = argument.value_row(0, first_row);
            return Ok(values.slice(offset, selection.len()));
        }
    }

    let mut indices = Vec::with_capacity(selection.len());
    for (ordinal, row) in selection.iter().enumerate() {
        work.step()?;
        let source = argument.value_row(ordinal, row);
        if source >= values.len() {
            return Err(invalid("ROW selection is outside its source"));
        }
        indices.push(if parent.is_some_and(|row| row.is_null(source)) {
            None
        } else {
            Some(u64::try_from(source).map_err(|_| invalid("ROW source index exceeds UInt64"))?)
        });
    }
    crate::selected_copy::preflight_take(values.as_ref(), &indices, |boundary| {
        if boundary { work.flush() } else { work.step() }
    })
    .map_err(crate::selected_copy::CopyError::into_kernel)?;
    work.flush()?;
    arrow_select::take::take(values.as_ref(), &UInt64Array::from(indices), None)
        .map_err(|source| arrow_failure("row selection", ArrowSite::Take, source))
}
