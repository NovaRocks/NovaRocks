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

//! Borrowed flat, non-dictionary constant RecordBatch geometry.
//!
//! This component allocates no arrays and invokes no Arrow reader. Its measured
//! buffer and repeated-view extents are inputs to admission, not a complete
//! first-allocation/resource model or a formal memory grant. The stream owner
//! must separately check schema identity, framing/EOS, Field/FVT correspondence,
//! and reader/schema/header/alignment/validation allocation and work bounds.

use crate::ipc_flat_stream_v2::progress::{Admission, ReaderGeometryFailure};
use crate::{
    ipc_schema_v2::verified_message_observed,
    physical_type_v2::{TypeCodecError, validate_field, validate_type},
};
use arrow::datatypes::{DataType, Field};
use novarocks_arrow_ipc_frame::{VerifierOptions, nonnegative_length};
use novarocks_type_contract::CompileControlError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

pub(crate) use neutral::Layout;
pub use neutral::{FlatBatchGeometry, FlatBatchProjectionLimits};
use novarocks_arrow_batch_codec::record_geometry::{self as neutral, GeometryWork};

fn geometry_error(cause: neutral::GeometryError) -> TypeCodecError {
    match cause {
        neutral::GeometryError::InvalidShape(message) => TypeCodecError::InvalidShape(message),
        neutral::GeometryError::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
    }
}
pub(crate) fn layout(ty: &DataType) -> Result<Layout, TypeCodecError> {
    neutral::layout(ty).map_err(geometry_error)
}
pub(crate) struct CallerGeometryFailure<E>(pub(crate) E);
impl<E: From<TypeCodecError>> From<neutral::GeometryError> for CallerGeometryFailure<E> {
    fn from(cause: neutral::GeometryError) -> Self {
        Self(E::from(geometry_error(cause)))
    }
}
impl<E: From<CompileControlError>> From<CompileControlError> for CallerGeometryFailure<E> {
    fn from(cause: CompileControlError) -> Self {
        Self(E::from(cause))
    }
}
pub(crate) struct GeometryAdapter<'a, 'b, 'c, 'd, 'e, E> {
    pub(crate) admission: Option<&'a mut Admission<'b, 'c, E>>,
    pub(crate) work: &'d mut CompileCheckpoints<'e>,
}
impl<E: ReaderGeometryFailure> GeometryWork for GeometryAdapter<'_, '_, '_, '_, '_, E> {
    type Error = CallerGeometryFailure<E>;
    fn has_admission(&self) -> bool {
        self.admission.is_some()
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        parent_step(&mut self.admission, self.work).map_err(CallerGeometryFailure)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.work
            .flush()
            .map_err(|cause| CallerGeometryFailure(E::from(cause)))
    }
    fn geometry_request(
        &mut self,
        layout: std::alloc::Layout,
        construction: bool,
    ) -> Result<(), Self::Error> {
        if let Some(a) = self.admission.as_deref_mut() {
            a.requests(0, layout.size(), usize::from(layout.size() != 0))
                .map_err(CallerGeometryFailure)?;
            if construction {
                if layout.size() != 0 {
                    a.construction_request(layout)
                        .map_err(CallerGeometryFailure)?;
                }
                a.batch_work_add(layout.size())
                    .map_err(CallerGeometryFailure)?;
            }
        }
        Ok(())
    }
    fn source_work(&mut self) -> Result<(), Self::Error> {
        if let Some(a) = self.admission.as_deref_mut() {
            let amount = a
                .source()
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?;
            a.batch_work_add(amount).map_err(CallerGeometryFailure)?;
        }
        Ok(())
    }
    fn validate_root(&mut self, expected: &Field) -> Result<(), Self::Error> {
        validate_field(expected, self.work)
            .map_err(|cause| CallerGeometryFailure(E::from(cause)))?;
        if let Some(a) = self.admission.as_deref_mut() {
            let shared = std::cell::RefCell::new(a);
            let scratch =
                |layout: std::alloc::Layout| shared.borrow_mut().batch_work_add(layout.size());
            let capture = |visit| {
                let mut a = shared.borrow_mut();
                let upper = if matches!(visit, novarocks_type_contract::ValueTypeVisit::Field(_)) {
                    a.source()
                        .checked_add(1)
                        .ok_or(CompileControlError::ResourceExhausted)?
                } else {
                    1
                };
                a.batch_work_add(upper)
            };
            crate::physical_type_v2::validate_type_with_scratch_host_observed(
                expected.data_type(),
                &mut |layout| {
                    scratch(layout).map_err(crate::host_projection_v2::ProjectionFailure::Host)
                },
                &mut |visit| {
                    capture(visit).map_err(crate::host_projection_v2::ProjectionFailure::Host)
                },
                self.work,
            )
            .map_err(|error| {
                CallerGeometryFailure(match error {
                    crate::host_projection_v2::ProjectionFailure::Host(error) => error,
                    crate::host_projection_v2::ProjectionFailure::Codec(error) => E::from(error),
                })
            })?;
        } else {
            validate_type(expected.data_type(), self.work)
                .map_err(|cause| CallerGeometryFailure(E::from(cause)))?;
        }
        Ok(())
    }
}
fn add(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_add(right)
        .ok_or(TypeCodecError::InvalidShape("IPC batch extent overflow"))
}
fn length(value: i64) -> Result<usize, TypeCodecError> {
    nonnegative_length(value)
        .map_err(|_| TypeCodecError::InvalidShape("negative or unrepresentable IPC batch extent"))
}
fn require(
    condition: bool,
    message: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    work.step()?;
    if condition {
        Ok(())
    } else {
        Err(TypeCodecError::InvalidShape(message))
    }
}

fn numeric_parent<T>(parent: bool, result: Result<T, TypeCodecError>) -> Result<T, TypeCodecError> {
    if parent {
        result.map_err(|_| CompileControlError::ResourceExhausted.into())
    } else {
        result
    }
}
fn parent_step<E: ReaderGeometryFailure>(
    admission: &mut Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(a) = admission.as_deref_mut() {
        a.batch_step(work)?;
    } else {
        work.step()?;
    }
    Ok(())
}
fn require_parent<E: ReaderGeometryFailure>(
    condition: bool,
    message: &'static str,
    mut admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    parent_step(&mut admission, work)?;
    if condition {
        Ok(())
    } else {
        Err((TypeCodecError::InvalidShape(message)).into())
    }
}
fn limit_parent<E: ReaderGeometryFailure>(
    condition: bool,
    message: &'static str,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if admission.is_some() && !condition {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    require_parent(condition, message, admission, work)
}
/// Checks one V5 uncompressed metadata/body pair without copying body bytes.
/// Overlapping, nonsequential and unaligned legal descriptors remain allowed;
/// this is independent of the NRX1 result writer's alignment profile. Exact
/// values, validity popcount, offsets and UTF8 are still Arrow's later verdict.
pub fn preflight_flat_record_batch(
    metadata: &[u8],
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight(metadata, body, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn preflight(
    metadata: &[u8],
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    require(
        metadata.len() <= limits.max_metadata_bytes,
        "IPC batch metadata envelope exceeded",
        work,
    )?;
    require(
        body.len() <= limits.max_body_bytes,
        "IPC batch body envelope exceeded",
        work,
    )?;
    let layout = source_layout(expected, work)?;
    let message = verified_message_observed(metadata, verifier, work)?;
    preflight_message(message, body, limits, layout, work)
}

fn source_layout(
    expected: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Layout, TypeCodecError> {
    source_layout_core(expected, None, work)
}
fn source_layout_core<E: ReaderGeometryFailure>(
    expected: &Field,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Layout, E> {
    let mut adapter = GeometryAdapter { admission, work };
    adapter.source_work().map_err(|cause| cause.0)?;
    adapter.validate_root(expected).map_err(|cause| cause.0)?;
    let layout = layout(expected.data_type())?;
    adapter.step().map_err(|cause| cause.0)?;
    Ok(layout)
}

/// The stream owner has already verified this borrowed Message and checked
/// metadata admission. Reuse that exact parse, without a second opaque call.
pub(crate) fn preflight_verified_flat_record_batch(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    preflight_verified_flat_record_batch_core(message, body, expected, limits, None, work)
}
fn preflight_verified_flat_record_batch_core<E: ReaderGeometryFailure>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &Field,
    limits: FlatBatchProjectionLimits,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, E> {
    preflight_verified_flat_record_batch_profile(
        message,
        body,
        expected,
        Some(limits),
        admission,
        work,
    )
}
/// Same geometry body; None derives this input's finite checked extents.
pub(crate) fn preflight_verified_flat_record_batch_profile<E: ReaderGeometryFailure>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &Field,
    limits: Option<FlatBatchProjectionLimits>,
    mut admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, E> {
    limit_parent(
        limits.is_none_or(|limits| body.len() <= limits.max_body_bytes),
        "IPC batch body envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    let layout = source_layout_core(expected, admission.as_deref_mut(), work)?;
    preflight_message_core(message, body, limits, layout, admission, work)
}

fn preflight_message(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    limits: FlatBatchProjectionLimits,
    layout: Layout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, TypeCodecError> {
    preflight_message_core(message, body, Some(limits), layout, None, work)
}
fn preflight_message_core<E: ReaderGeometryFailure>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    limits: Option<FlatBatchProjectionLimits>,
    layout: Layout,
    mut admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, E> {
    let parent = admission.is_some();
    require_parent(
        message.version() == arrow::ipc::MetadataVersion::V5
            && message.header_type() == arrow::ipc::MessageHeader::RecordBatch
            && message
                .custom_metadata()
                .is_none_or(|entries| entries.is_empty()),
        "unsupported constant IPC RecordBatch message profile",
        admission.as_deref_mut(),
        work,
    )?;
    require_parent(
        length(message.bodyLength())? == body.len(),
        "IPC batch body length mismatch",
        admission.as_deref_mut(),
        work,
    )?;
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "IPC RecordBatch payload is missing",
        ))?;
    require_parent(
        batch.compression().is_none(),
        "compressed constant IPC batch is unsupported",
        admission.as_deref_mut(),
        work,
    )?;
    let rows = length(batch.length())?;
    limit_parent(
        limits.is_none_or(|limits| rows <= limits.max_rows),
        "IPC batch row envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    let nodes = batch
        .nodes()
        .ok_or(TypeCodecError::InvalidShape("IPC batch nodes are missing"))?;
    require_parent(
        nodes.len() == 1,
        "flat constant IPC batch must have exactly one node",
        admission.as_deref_mut(),
        work,
    )?;
    let node = nodes.get(0);
    require_parent(
        length(node.length())? == rows,
        "IPC batch node length differs from rows",
        admission.as_deref_mut(),
        work,
    )?;
    let null_count = length(node.null_count())?;
    require_parent(
        null_count <= rows,
        "IPC batch null count exceeds rows",
        admission.as_deref_mut(),
        work,
    )?;
    let buffers = batch.buffers().ok_or(TypeCodecError::InvalidShape(
        "IPC batch buffer descriptors are missing",
    ))?;
    limit_parent(
        limits.is_none_or(|limits| buffers.len() <= limits.max_buffer_descriptors),
        "IPC batch buffer envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    let variadic = batch.variadicBufferCounts();
    let variadic_buffers = if matches!(layout, Layout::Views) {
        require_parent(
            variadic.is_some_and(|counts| counts.len() == 1),
            "IPC view batch requires exactly one variadic count",
            admission.as_deref_mut(),
            work,
        )?;
        let count = variadic
            .ok_or(TypeCodecError::InvalidShape(
                "IPC view variadic count is missing",
            ))?
            .get(0);
        // The locked reader adds two in i64 before collecting Buffer headers.
        numeric_parent(
            parent,
            count.checked_add(2).ok_or(TypeCodecError::InvalidShape(
                "IPC view buffer count overflow",
            )),
        )?;
        length(count)?
    } else {
        require_parent(
            variadic.is_none_or(|counts| counts.is_empty()),
            "non-view IPC batch has extra variadic counts",
            admission.as_deref_mut(),
            work,
        )?;
        0
    };
    let count = match layout {
        Layout::Null => 0,
        Layout::Bits | Layout::Fixed(_) => 2,
        Layout::Offsets(_) => 3,
        Layout::Views => numeric_parent(parent, add(2, variadic_buffers))?,
    };
    require_parent(
        buffers.len() == count,
        "IPC batch descriptor count differs from carrier",
        admission.as_deref_mut(),
        work,
    )?;
    inspect_leaf_at_core(
        batch,
        body,
        layout,
        rows,
        null_count,
        0,
        variadic_buffers,
        limits,
        admission,
        work,
    )
}

/// Constant stream boundary adapter to the shared neutral leaf geometry.
pub(crate) fn inspect_leaf_at_core<E: ReaderGeometryFailure>(
    batch: arrow::ipc::RecordBatch<'_>,
    body: &[u8],
    layout: Layout,
    rows: usize,
    null_count: usize,
    buffer_start: usize,
    variadic_buffers: usize,
    limits: Option<FlatBatchProjectionLimits>,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatBatchGeometry, E> {
    neutral::inspect_leaf(
        batch,
        body,
        layout,
        rows,
        null_count,
        buffer_start,
        variadic_buffers,
        limits,
        &mut GeometryAdapter { admission, work },
    )
    .map_err(|cause| cause.0)
}

#[cfg(test)]
mod tests;
