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
//! Exact unary intrinsic evaluation over the already evaluated Value domain.
//! Required row errors remain errors even when their value is a NULL placeholder.
use super::*;
use arrow::array::BooleanArray;

#[derive(Clone, Copy)]
pub(super) enum UnaryIntrinsic {
    IsNull,
    IsNotNull,
}

pub(super) fn evaluate<'a>(
    kind: UnaryIntrinsic,
    child: &Value<'a>,
    selection: Selection<'a>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let argument = child.argument()?;
    let mut required = child.errors().iter().peekable();
    let mut values = Vec::with_capacity(selection.len());
    let mut errors = Vec::new();
    work.flush()?;
    let control = work.control;
    visit_selected_nulls(argument, selection, control, |ordinal, _row, is_null| {
        if required
            .peek()
            .is_some_and(|error| error.selected_ordinal() == ordinal)
        {
            let error = required
                .next()
                .ok_or_else(|| internal("missing required unary error"))?;
            errors.push(error.clone());
            values.push(None);
        } else {
            values.push(Some(is_null != matches!(kind, UnaryIntrinsic::IsNotNull)));
        }
        // The NULL visitor charges this bounded projection in its own row
        // scope, together with the logical address traversal.
        Ok(())
    })?;
    work.flush()?;
    let array = Arc::new(BooleanArray::from(values)) as ArrayRef;
    work.flush()?;
    SelectedValues::try_new_observed(
        selection,
        &DataType::Boolean,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )
}
