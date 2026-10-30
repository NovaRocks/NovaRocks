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

//! ONE original List-to-List CAST body. Scheduling and child semantic schemas
//! remain owned by the caller; callbacks only consume an already evaluated array.
use arrow_array::{Array, ArrayRef, ListArray, new_empty_array, new_null_array};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, FieldRef};
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum ListCastObservation {
    Step,
    OpaqueBoundary,
}
#[derive(Debug)]
pub enum ListCastError<E> {
    Domain(ListCastFailure),
    Host(E),
}

/// Child-cast callback used by the shared List CAST implementation.
pub type ListChildCaster<'call, E> =
    dyn FnMut(&ArrayRef, &DataType) -> Result<ArrayRef, ListCastError<E>> + 'call;

/// Original Null-source lifting; this does not infer a child logical identity.
pub fn null_source_observed<E>(
    len: usize,
    target: &DataType,
    observe: &mut dyn FnMut(ListCastObservation) -> Result<(), E>,
) -> Result<ArrayRef, ListCastError<E>> {
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    let out = new_null_array(target, len);
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    Ok(out)
}
pub fn null_source(len: usize, target: &DataType) -> ArrayRef {
    null_source_observed::<std::convert::Infallible>(len, target, &mut |_| Ok(())).unwrap_or_else(
        |error| match error {
            ListCastError::Host(never) => match never {},
            ListCastError::Domain(_) => unreachable!("original Null lifting has no data error"),
        },
    )
}

/// Preserve the original physical null-count test, target Field, offsets,
/// original child failures and checked ListArray construction. No decoder is introduced.
pub fn cast_observed<E>(
    array: &ArrayRef,
    target_field: &FieldRef,
    cast_child: &mut ListChildCaster<'_, E>,
    observe: &mut dyn FnMut(ListCastObservation) -> Result<(), E>,
) -> Result<ArrayRef, ListCastError<E>> {
    observe(ListCastObservation::Step).map_err(ListCastError::Host)?;
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| ListCastError::Domain(ListCastFailure::Carrier))?;
    // This is the original child carrier equality, not an identity of Fields.
    if list.values().data_type() == target_field.data_type() {
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        let out: ArrayRef = Arc::new(
            ListArray::try_new(
                target_field.clone(),
                list.offsets().clone(),
                list.values().clone(),
                list.nulls().cloned(),
            )
            .map_err(|cause| ListCastError::Domain(ListCastFailure::Arrow(cause)))?,
        );
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        return Ok(out);
    }
    let cast_values = if list.values().is_empty() {
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        let out = new_empty_array(target_field.data_type());
        observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
        out
    } else if list.values().null_count() == list.values().len() {
        null_source_observed(list.values().len(), target_field.data_type(), observe)?
    } else {
        // NullArray still has physical null_count()==0 in Arrow 60.1.0. Its nonempty
        // values therefore reach the caller's original Null-source branch.
        cast_child(list.values(), target_field.data_type())?
    };
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    let mut offsets = Vec::with_capacity(list.value_offsets().len());
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    for offset in list.value_offsets() {
        offsets.push(*offset);
        observe(ListCastObservation::Step).map_err(ListCastError::Host)?;
    }
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    let out: ArrayRef = Arc::new(
        ListArray::try_new(
            target_field.clone(),
            OffsetBuffer::new(offsets.into()),
            cast_values,
            list.nulls().cloned(),
        )
        .map_err(|cause| ListCastError::Domain(ListCastFailure::Arrow(cause)))?,
    );
    observe(ListCastObservation::OpaqueBoundary).map_err(ListCastError::Host)?;
    Ok(out)
}

pub fn cast<E: From<ListCastFailure>>(
    array: &ArrayRef,
    target_field: &FieldRef,
    cast_child: &mut dyn FnMut(&ArrayRef, &DataType) -> Result<ArrayRef, E>,
) -> Result<ArrayRef, E> {
    cast_observed(
        array,
        target_field,
        &mut |child, target| cast_child(child, target).map_err(ListCastError::Host),
        &mut |_| Ok(()),
    )
    .map_err(|cause| match cause {
        ListCastError::Domain(cause) => E::from(cause),
        ListCastError::Host(cause) => cause,
    })
}
#[derive(Debug)]
pub enum ListCastFailure {
    Carrier,
    Arrow(arrow_schema::ArrowError),
}
impl std::fmt::Display for ListCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Carrier => f.write_str("failed to downcast to ListArray"),
            Self::Arrow(cause) => write!(f, "List CAST construction: {cause}"),
        }
    }
}
impl std::error::Error for ListCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(cause) => Some(cause),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ListCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::Arrow(arrow_schema::ArrowError::MemoryError(_)) => (FailureClass::Resource, 223),
            _ => (FailureClass::Invariant, 224),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ListCastFailure {}
impl From<ListCastFailure> for crate::KernelFailure {
    fn from(cause: ListCastFailure) -> Self {
        Self::from_domain(cause)
    }
}
