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

//! Pure value assembly for already-evaluated conditional arguments.
//! The caller owns all expression demand. Full legacy arrays retain their
//! original Arrow zip representation; compact branches retain interleave.
use crate::{
    KernelEvaluationControl, KernelFailure,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_arith::boolean::is_not_null;
use arrow_array::{Array, ArrayRef, Datum, new_null_array};
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_select::{interleave::interleave, zip::zip};
use std::{alloc::Layout, sync::Arc};

/// Only the already-evaluated representation is described here; neither plan
/// carries an expression, host arena, or callback that may evaluate a child.
pub enum AssemblyPlan<'a> {
    FillNulls {
        left: &'a ArrayRef,
        right: &'a ArrayRef,
    },
    Indexed {
        result_type: &'a DataType,
        /// One slot per already-evaluated child. Non-result children are None.
        sources: &'a [Option<&'a ArrayRef>],
        /// Source slots and compact source ordinals; None is a typed SQL NULL.
        choices: &'a [Option<(usize, usize)>],
    },
}
/// Complete Arrow diagnostics cross the v1 boundary before any bounded kernel
/// projection. Exact host control failures have a separate typed channel.
#[derive(Debug)]
pub enum AssemblyFailure {
    Arrow(String),
    Kernel(KernelFailure),
}
impl From<KernelFailure> for AssemblyFailure {
    fn from(error: KernelFailure) -> Self {
        Self::Kernel(error)
    }
}
impl std::fmt::Display for AssemblyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Arrow(message) => f.write_str(message),
            Self::Kernel(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for AssemblyFailure {}

fn supports_flat_indexed_result(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Boolean
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
    ) || ty.primitive_width().is_some()
}
/// Closed carriers handled by the same actual Arrow indexed interleave.
/// Nested fields retain their complete names, metadata and NULL semantics.
pub fn supports_indexed_result(ty: &DataType) -> bool {
    crate::selected_copy::supports_guarded_result(ty)
}
fn reserve<T>(len: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
/// One pure assembly entrypoint for full arrays and compact selected domains.
/// Both use the original representation operation and preserve its errors.
pub fn assemble_values(
    plan: AssemblyPlan<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, AssemblyFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| match plan {
        AssemblyPlan::FillNulls { left, right } => {
            // Legacy callers retain Arrow's own errors and all carrier support;
            // a compiled indexed contract is separately admitted below.
            work.flush()?;
            let mask =
                is_not_null(left.as_ref()).map_err(|e| AssemblyFailure::Arrow(e.to_string()))?;
            work.flush()?;
            if supports_flat_indexed_result(left.data_type())
                && left.data_type() == right.data_type()
                && left.len() == right.len()
            {
                crate::selected_copy::preflight_zip(
                    &mask,
                    left.as_ref(),
                    right.as_ref(),
                    |boundary| if boundary { work.flush() } else { work.step() },
                )
                .map_err(|e| match e {
                    crate::selected_copy::CopyError::Control(error) => {
                        AssemblyFailure::Kernel(error)
                    }
                    crate::selected_copy::CopyError::Extent => {
                        AssemblyFailure::Kernel(KernelFailure::ResourceExhausted)
                    }
                    _ => AssemblyFailure::Kernel(internal(
                        "checked full conditional values could not be assembled",
                    )),
                })?;
            }
            work.flush()?;
            let values = zip(
                &mask,
                &left.as_ref() as &dyn Datum,
                &right.as_ref() as &dyn Datum,
            )
            .map_err(|e| AssemblyFailure::Arrow(e.to_string()))?;
            work.flush()?;
            Ok(values)
        }
        AssemblyPlan::Indexed {
            result_type,
            sources: children,
            choices,
        } => {
            if !supports_indexed_result(result_type) {
                return Err(
                    invalid("guarded result requires its dedicated carrier protocol").into(),
                );
            }
            crate::selected_copy::guarded_result_extent(result_type, choices.len())
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            let mut sources = reserve::<ArrayRef>(
                children
                    .len()
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?,
                &mut work,
            )?;
            work.flush()?;
            sources.push(new_null_array(result_type, 1));
            work.flush()?;
            let mut source_ids = reserve::<Option<usize>>(children.len(), &mut work)?;
            for child in children {
                let id = match child {
                    Some(array) if array.data_type() == result_type => {
                        sources.push(Arc::clone(array));
                        Some(sources.len() - 1)
                    }
                    _ => None,
                };
                source_ids.push(id);
                work.step()?;
            }
            let mut indices = reserve::<(usize, usize)>(choices.len(), &mut work)?;
            for &choice in choices {
                indices.push(match choice {
                    Some((child, ordinal)) => (
                        source_ids[child]
                            .ok_or_else(|| internal("missing actual guarded result source"))?,
                        ordinal,
                    ),
                    None => (0, 0),
                });
                work.step()?;
            }
            crate::selected_copy::preflight_guarded_interleave(
                result_type,
                &sources,
                &indices,
                |boundary| if boundary { work.flush() } else { work.step() },
            )
            .map_err(|error| match error {
                crate::selected_copy::CopyError::Control(error) => error,
                crate::selected_copy::CopyError::Extent => KernelFailure::ResourceExhausted,
                _ => invalid("guarded result requires its dedicated carrier protocol"),
            })?;
            let mut arrays = reserve::<&dyn Array>(sources.len(), &mut work)?;
            for source in &sources {
                arrays.push(source.as_ref());
                work.step()?;
            }
            work.flush()?;
            let array = interleave(&arrays, &indices)
                .map_err(|_| internal("checked guarded result could not be assembled"))?;
            work.flush()?;
            Ok(array)
        }
    })();
    // Ordinary Arrow errors still observe the completion tail. A refused
    // original control wins unchanged and is never projected into a String.
    match result {
        Ok(value) => work
            .finish_result(Ok(value))
            .map_err(AssemblyFailure::Kernel),
        Err(AssemblyFailure::Kernel(error)) => work
            .finish_result::<ArrayRef>(Err(error))
            .map_err(AssemblyFailure::Kernel),
        Err(AssemblyFailure::Arrow(message)) => {
            work.finish()?;
            Err(AssemblyFailure::Arrow(message))
        }
    }
}
struct LegacyControl;
impl KernelEvaluationControl for LegacyControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("legacy conditional computation must not wait"))
    }
}

/// The IFNULL target is the original left carrier, with Null-left inference.
/// The explicit root result type was never an input to this v1 calculation.
pub fn ifnull_legacy(mut left: ArrayRef, mut right: ArrayRef) -> Result<ArrayRef, String> {
    let mut target = left.data_type().clone();
    if matches!(target, DataType::Null) && !matches!(right.data_type(), DataType::Null) {
        target = right.data_type().clone();
    }
    if left.data_type() != &target {
        left = cast(left.as_ref(), &target).map_err(|e| e.to_string())?;
    }
    if right.data_type() != &target {
        right = cast(right.as_ref(), &target).map_err(|e| e.to_string())?;
    }
    assemble_values(
        AssemblyPlan::FillNulls {
            left: &left,
            right: &right,
        },
        &LegacyControl,
    )
    .map_err(|e| e.to_string())
}
/// This action describes a calculation result, not authority to evaluate any
/// child. The legacy shell alone preserves its original recursive demand.
pub enum CoalesceStep {
    Complete(ArrayRef),
    ReevaluateTail(CoalesceKeepAlive),
}
/// Original first-round backings stay alive until the legacy recursion exits.
/// Field order preserves first-mask Drop before the typed argument arrays.
pub struct CoalesceKeepAlive {
    _mask: arrow_array::BooleanArray,
    _arrays: Vec<ArrayRef>,
}
/// The caller already evaluated every argument and looked up its output type.
/// Re-inference, eager casts, direct identity return and the all-Null Int64
/// fallback are preserved exactly before the original tail recursion action.
pub fn coalesce_legacy(
    arrays: Vec<ArrayRef>,
    output_type: &DataType,
) -> Result<CoalesceStep, String> {
    if arrays.is_empty() {
        return Err("coalesce: requires at least one argument".to_string());
    }
    let target = if matches!(output_type, DataType::Null) {
        arrays
            .iter()
            .find(|arr| !matches!(arr.data_type(), DataType::Null))
            .map(|arr| arr.data_type().clone())
            .unwrap_or(DataType::Null)
    } else {
        output_type.clone()
    };
    if matches!(target, DataType::Null) {
        let values: Vec<Option<i64>> = (0..arrays[0].len()).map(|_| None).collect();
        return Ok(CoalesceStep::Complete(Arc::new(
            arrow_array::Int64Array::from(values),
        )));
    }
    let mut typed = Vec::with_capacity(arrays.len());
    for arr in arrays {
        let value = if arr.data_type() != &target {
            cast(arr.as_ref(), &target).map_err(|e| {
                format!(
                    "coalesce: failed to cast array from {:?} to {:?}: {}",
                    arr.data_type(),
                    target,
                    e
                )
            })?
        } else {
            arr
        };
        typed.push(value);
    }
    let len = typed[0].len();
    let mask = is_not_null(typed[0].as_ref()).map_err(|e| e.to_string())?;
    let first_mask = mask
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .ok_or_else(|| "coalesce: failed to downcast mask to BooleanArray".to_string())?;
    let count = (0..len)
        .map(|i| usize::from(!first_mask.is_null(i) && first_mask.value(i)))
        .sum::<usize>();
    if count == len {
        return Ok(CoalesceStep::Complete(Arc::clone(&typed[0])));
    }
    if count == 0 && typed.len() > 1 {
        return Ok(CoalesceStep::ReevaluateTail(CoalesceKeepAlive {
            _mask: mask,
            _arrays: typed,
        }));
    }
    let mut result = Arc::clone(&typed[0]);
    for next in typed.iter().skip(1) {
        result = assemble_values(
            AssemblyPlan::FillNulls {
                left: &result,
                right: next,
            },
            &LegacyControl,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(CoalesceStep::Complete(result))
}
#[cfg(test)]
#[path = "control_values_tests.rs"]
mod tests;
