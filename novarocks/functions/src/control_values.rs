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
/// Assembly errors retain the actual source; runtime controls keep their
/// original identity. No diagnostic text is an error-classification input.
#[derive(Debug)]
pub struct AssemblyFailure(Box<AssemblyCause>);
#[derive(Debug)]
enum AssemblyCause {
    Arrow {
        context: novarocks_failure_contract::BoundedDetail,
        source: arrow_schema::ArrowError,
    },
    Contract(novarocks_failure_contract::BoundedDetail),
    TextExtent {
        total: u128,
        largest: u128,
    },
    Kernel(KernelFailure),
}
impl AssemblyFailure {
    pub fn arrow(context: impl std::fmt::Display, source: arrow_schema::ArrowError) -> Self {
        Self(Box::new(AssemblyCause::Arrow {
            context: novarocks_failure_contract::BoundedDetail::truncating_display(context),
            source,
        }))
    }
    pub fn contract(context: impl std::fmt::Display) -> Self {
        Self(Box::new(AssemblyCause::Contract(
            novarocks_failure_contract::BoundedDetail::truncating_display(context),
        )))
    }
    pub fn into_kernel(self) -> KernelFailure {
        match *self.0 {
            AssemblyCause::Kernel(source) => source,
            cause => KernelFailure::from_domain(Self(Box::new(cause))),
        }
    }
}
impl From<KernelFailure> for AssemblyFailure {
    fn from(source: KernelFailure) -> Self {
        Self(Box::new(AssemblyCause::Kernel(source)))
    }
}
impl std::fmt::Display for AssemblyFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            AssemblyCause::Arrow { context, source } if context.as_str().is_empty() => {
                source.fmt(f)
            }
            AssemblyCause::Arrow { context, source } => write!(f, "{}: {source}", context.as_str()),
            AssemblyCause::Contract(detail) => f.write_str(detail.as_str()),
            AssemblyCause::TextExtent { total, largest } => write!(
                f,
                "conditional UTF8 output extent {total} (largest value {largest}) exceeds Int32 offsets"
            ),
            AssemblyCause::Kernel(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for AssemblyFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            AssemblyCause::Arrow { source, .. } => Some(source),
            AssemblyCause::Kernel(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for AssemblyFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use arrow_schema::ArrowError;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, code) = match self.0.as_ref() {
            AssemblyCause::Kernel(source) => return source.failure_fact(),
            AssemblyCause::Contract(_) => (FailureClass::Invariant, 255),
            AssemblyCause::TextExtent { total, largest } => {
                return FailureFact::new(
                    FailureClass::Representation,
                    FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(256).unwrap()),
                )
                .with_representation(RepresentationFacts {
                    boundary: if *largest > i32::MAX as u128 {
                        RepresentationBoundary::MinimumUnit
                    } else {
                        RepresentationBoundary::SingleOutput
                    },
                    layout: RepresentationLayout {
                        domain: domain::KERNEL,
                        code: std::num::NonZeroU16::new(43).unwrap(),
                    },
                    required: u64::try_from(if *largest > i32::MAX as u128 {
                        *largest
                    } else {
                        *total
                    })
                    .ok(),
                    extent_limit: Some(i32::MAX as u64),
                    representable_prefix: None,
                });
            }
            // Masking, safe frozen-type conversion and result assembly have no
            // lawful row-data channel. Arithmetic here describes copy geometry.
            AssemblyCause::Arrow {
                source: ArrowError::MemoryError(_) | ArrowError::ArithmeticOverflow(_),
                ..
            } => (FailureClass::Resource, 251),
            AssemblyCause::Arrow {
                source: ArrowError::OffsetOverflowError(_),
                ..
            } => (FailureClass::Representation, 252),
            AssemblyCause::Arrow {
                source: ArrowError::IoError(..),
                ..
            } => (FailureClass::Unexpected, 253),
            AssemblyCause::Arrow { .. } => (FailureClass::Invariant, 254),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        );
        if class == FailureClass::Representation {
            fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(42).unwrap(),
                },
                required: None,
                extent_limit: None,
                representable_prefix: None,
            })
        } else {
            fact
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for AssemblyFailure {}
#[derive(Default)]
pub(crate) struct ConditionalTextExtent {
    total: u128,
    largest: u128,
}
impl ConditionalTextExtent {
    pub(crate) fn append(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        self.total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            KernelFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        self.largest = self.largest.max(bytes as u128);
        if self.total > i32::MAX as u128 {
            Err(AssemblyFailure(Box::new(AssemblyCause::TextExtent {
                total: self.total,
                largest: self.largest,
            }))
            .into_kernel())
        } else {
            Ok(())
        }
    }
}

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
/// Physical Null carries a length without a payload or validity buffer.
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
    let result: Result<ArrayRef, AssemblyFailure> = (|| match plan {
        AssemblyPlan::FillNulls { left, right } => {
            // Legacy callers retain Arrow's own errors and all carrier support;
            // a compiled indexed contract is separately admitted below.
            work.flush()?;
            let mask = is_not_null(left.as_ref()).map_err(|e| AssemblyFailure::arrow("", e))?;
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
                .map_err(crate::selected_copy::CopyError::into_kernel)?;
            }
            work.flush()?;
            let values = zip(
                &mask,
                &left.as_ref() as &dyn Datum,
                &right.as_ref() as &dyn Datum,
            )
            .map_err(|e| AssemblyFailure::arrow("", e))?;
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
            .map_err(crate::selected_copy::CopyError::into_kernel)?;
            let mut arrays = reserve::<&dyn Array>(sources.len(), &mut work)?;
            for source in &sources {
                arrays.push(source.as_ref());
                work.step()?;
            }
            work.flush()?;
            let array = interleave(&arrays, &indices).map_err(|source| {
                AssemblyFailure::arrow("checked guarded result could not be assembled", source)
            })?;
            work.flush()?;
            Ok(array)
        }
    })();
    match result {
        Ok(value) => work.finish_result(Ok(value)).map_err(AssemblyFailure::from),
        Err(source) => work
            .finish_result::<ArrayRef>(Err(source.into_kernel()))
            .map_err(AssemblyFailure::from),
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

/// The caller supplies the frozen result carrier before evaluating children.
pub fn ifnull_legacy(
    mut left: ArrayRef,
    mut right: ArrayRef,
    output_type: &DataType,
) -> Result<ArrayRef, KernelFailure> {
    let target = output_type.clone();
    if left.data_type() != &target {
        left = cast(left.as_ref(), &target)
            .map_err(|source| AssemblyFailure::arrow("", source).into_kernel())?;
    }
    if right.data_type() != &target {
        right = cast(right.as_ref(), &target)
            .map_err(|source| AssemblyFailure::arrow("", source).into_kernel())?;
    }
    assemble_values(
        AssemblyPlan::FillNulls {
            left: &left,
            right: &right,
        },
        &LegacyControl,
    )
    .map_err(AssemblyFailure::into_kernel)
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
/// Already-evaluated children are converted to the frozen result carrier.
/// The legacy shell owns child demand; this helper cannot infer a replacement.
pub fn coalesce_legacy(
    arrays: Vec<ArrayRef>,
    output_type: &DataType,
) -> Result<CoalesceStep, KernelFailure> {
    if arrays.is_empty() {
        return Err(
            AssemblyFailure::contract("coalesce: requires at least one argument").into_kernel(),
        );
    }
    let target = output_type.clone();
    let mut typed = Vec::with_capacity(arrays.len());
    for arr in arrays {
        let value = if arr.data_type() != &target {
            cast(arr.as_ref(), &target).map_err(|source| {
                AssemblyFailure::arrow(
                    format_args!(
                        "coalesce: failed to cast array from {:?} to {:?}",
                        arr.data_type(),
                        target
                    ),
                    source,
                )
                .into_kernel()
            })?
        } else {
            arr
        };
        typed.push(value);
    }
    if target == DataType::Null {
        return Ok(CoalesceStep::Complete(new_null_array(
            &DataType::Null,
            typed[0].len(),
        )));
    }
    let len = typed[0].len();
    let mask = is_not_null(typed[0].as_ref())
        .map_err(|source| AssemblyFailure::arrow("", source).into_kernel())?;
    let first_mask = mask
        .as_any()
        .downcast_ref::<arrow_array::BooleanArray>()
        .ok_or_else(|| {
            AssemblyFailure::contract("coalesce: failed to downcast mask to BooleanArray")
                .into_kernel()
        })?;
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
        .map_err(AssemblyFailure::into_kernel)?;
    }
    Ok(CoalesceStep::Complete(result))
}
#[cfg(test)]
#[path = "control_values_tests.rs"]
mod tests;
