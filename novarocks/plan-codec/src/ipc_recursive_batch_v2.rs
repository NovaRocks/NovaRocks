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

//! Borrowed recursive IPC geometry. Resource admission precedes owned geometry.

use crate::ipc_flat_stream_v2::progress::{Admission, ReaderGeometryFailure};
use arrow::datatypes::Field;

#[cfg(test)]
use crate::ipc_flat_batch_v2::FlatBatchProjectionLimits;
use crate::ipc_flat_batch_v2::GeometryAdapter;
#[cfg(test)]
use crate::ipc_schema_v2::verified_message_observed;
use crate::physical_type_v2::TypeCodecError;
#[cfg(test)]
use arrow::datatypes::DataType;
pub use novarocks_arrow_batch_codec::record_geometry::RecursiveBatchProjectionLimits;
use novarocks_arrow_batch_codec::record_geometry::{self as neutral, GeometryProfile};
pub(crate) use novarocks_arrow_batch_codec::record_geometry::{
    CheckedRecursiveBatch, RecursiveBatchGeometry, RecursiveNodeGeometry,
};
#[cfg(test)]
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_type_contract::CompileCheckpoints;
#[cfg(test)]
use novarocks_type_contract::CompileControlError;
#[cfg(test)]
use novarocks_type_contract::{CompilePhase, PureCompileControl};
fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn length(n: i64) -> Result<usize, TypeCodecError> {
    novarocks_arrow_ipc_frame::nonnegative_length(n)
        .map_err(|_| shape("recursive IPC extent is negative or unrepresentable"))
}
#[cfg(test)]
fn require(
    condition: bool,
    message: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    work.step()?;
    if condition {
        Ok(())
    } else {
        Err(shape(message))
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
        Err(shape(message).into())
    }
}
#[cfg(test)]
pub(crate) fn preflight_recursive_record_batch<'a>(
    metadata: &[u8],
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        require(
            metadata.len() <= limits.flat.max_metadata_bytes,
            "recursive IPC metadata envelope exceeded",
            &mut work,
        )?;
        let message = verified_message_observed(metadata, verifier, &mut work)?;
        preflight_verified_recursive_record_batch(message, body, expected, limits, &mut work)
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
pub(crate) fn preflight_verified_recursive_record_batch_in<'a, E: ReaderGeometryFailure>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    admission: &mut Admission<'_, '_, E>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, E> {
    preflight_core(message, body, expected, limits, Some(admission), work)
}

pub(crate) fn preflight_verified_recursive_record_batch<'a>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, TypeCodecError> {
    preflight_core(message, body, expected, limits, None, work)
}
fn preflight_core<'a, E: ReaderGeometryFailure>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: RecursiveBatchProjectionLimits,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, E> {
    preflight_verified_recursive_record_batch_profile(
        message,
        body,
        expected,
        Some(limits),
        admission,
        work,
    )
}
/// Input-derived constant admission uses the same neutral recursive body.
pub(crate) fn preflight_verified_recursive_record_batch_profile<'a, E: ReaderGeometryFailure>(
    message: arrow::ipc::Message<'_>,
    body: &[u8],
    expected: &'a Field,
    limits: Option<RecursiveBatchProjectionLimits>,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedRecursiveBatch<'a>, E> {
    let batch = message
        .header_as_record_batch()
        .ok_or(shape("recursive IPC RecordBatch is missing"))?;
    neutral::preflight_record_batch_with_profile(
        batch,
        body,
        &[expected],
        limits,
        GeometryProfile::Constant,
        &mut GeometryAdapter { admission, work },
        |adapter| {
            let result = (|| {
                require_parent(
                    message.version() == arrow::ipc::MetadataVersion::V5
                        && message.header_type() == arrow::ipc::MessageHeader::RecordBatch
                        && message.custom_metadata().is_none_or(|m| m.is_empty()),
                    "unsupported recursive IPC message profile",
                    adapter.admission.as_deref_mut(),
                    adapter.work,
                )?;
                require_parent(
                    length(message.bodyLength())? == body.len(),
                    "recursive IPC body length mismatch",
                    adapter.admission.as_deref_mut(),
                    adapter.work,
                )
            })();
            result.map_err(crate::ipc_flat_batch_v2::CallerGeometryFailure)
        },
    )
    .map_err(|cause| cause.0)
}

#[cfg(test)]
#[path = "ipc_recursive_batch_v2_tests.rs"]
mod tests;
