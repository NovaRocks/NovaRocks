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

//! Selected ROW conversion with positional children and one outer row error.

use super::*;
use crate::Selection;
use arrow_array::{ArrayRef, StructArray};
use std::sync::Arc;

// Design: ADR-0270 (docs/adr/ADR-0270-recursive-casts-and-composite-sql-predicates.md)
pub(super) fn evaluate_row(
    recipe: &PreparedCastRecipe,
    input: &ArrayRef,
    row: usize,
    ordinal: usize,
    control: &dyn KernelEvaluationControl,
    allocator: Option<&Arc<dyn crate::AggregateStateAllocator>>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<ArrayRef, RowDataError>, KernelFailure> {
    let source = input
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid("ROW CAST has a foreign concrete carrier"))?;
    let DataType::Struct(fields) = recipe.result.data_type() else {
        return Err(internal("ROW CAST lost its frozen target layout"));
    };
    let CastChildren::Row(children) = &recipe.children else {
        return Err(internal("ROW CAST lost its prepared child recipes"));
    };
    if source.num_columns() != children.len() || children.len() != fields.len() {
        return Err(invalid(
            "ROW CAST field count differs from its prepared recipes",
        ));
    }
    let selection =
        Selection::try_sparse_observed(source.len(), std::slice::from_ref(&row), || work.step())?;
    let mut values = array_mode::reserve::<ArrayRef>(children.len(), work)?;
    for (child, source) in children.iter().zip(source.columns()) {
        work.step()?;
        work.flush()?;
        let selected = child.evaluate_selected_scoped(
            EvaluatedArgument::Column(source),
            selection,
            &[],
            control,
            allocator,
        )?;
        if let Some(error) = selected.errors().first() {
            return Ok(Err(error.with_selected_ordinal(ordinal)));
        }
        values.push(Arc::clone(selected.values()));
    }
    work.flush()?;
    let result = StructArray::try_new(fields.clone(), values, None).map_err(|cause| {
        crate::control_values::AssemblyFailure::arrow("ROW CAST child assembly", cause)
            .into_kernel()
    })?;
    work.flush()?;
    Ok(Ok(Arc::new(result)))
}
