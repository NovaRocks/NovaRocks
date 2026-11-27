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

//! Borrowed framing and geometry for a flat non-dictionary constant pool.
//! The exact Field comes from the original package type table.
//! No array/schema materialization or reader entry is performed here. This
//! projection does not authorize those allocations or replace the subsequent
//! complete resource model, Field/FVT pairing and ConstantPool value admission.

use crate::{
    ipc_flat_batch_v2::{
        FlatBatchGeometry, FlatBatchProjectionLimits, preflight_verified_flat_record_batch,
    },
    physical_type_v2::TypeCodecError,
};
use arrow::datatypes::Field;
use novarocks_arrow_ipc_frame::{
    ContinuationPrefix, VerifierOptions, checked_range, continuation_prefix, nonnegative_length,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

pub(crate) mod progress;
pub(crate) mod resource_work;
pub(crate) use progress::IpcReaderProgressFacts;
use progress::ReaderGeometryFailure;
pub(crate) mod pool_resources;
pub use pool_resources::{FlatPoolResourceError, FlatPoolResourceProjection};
mod reader;
pub(crate) mod reader_allocations;
pub(crate) mod reader_creation;
pub(crate) mod reader_diagnostics;
mod reader_work;
pub use reader::{FlatReaderError, FlatReaderProjectionLimits, FlatReaderResourceFacts};
pub(crate) use reader::{PreparedFlatReader, ReaderFailure, ReaderResourceFailure};

/// All limits originate with the admitted caller, not the stream or defaults.
#[derive(Clone, Copy, Debug)]
pub struct FlatStreamProjectionLimits {
    pub max_input_bytes: usize,
    pub batch: FlatBatchProjectionLimits,
}

/// This owner can only be constructed by successful complete stream preflight.
/// The input and exact original Field remain borrowed; no values are retagged.
#[derive(Debug)]
pub struct FlatConstantStream<'a, 'f> {
    input: &'a [u8],
    field: &'f Field,
    batch_metadata: &'a [u8],
    batch_body: &'a [u8],
    batch: arrow::ipc::RecordBatch<'a>,
    version: arrow::ipc::MetadataVersion,
    geometry: FlatBatchGeometry,
    progress: Option<IpcReaderProgressFacts>,
}
impl<'a, 'f> FlatConstantStream<'a, 'f> {
    pub fn input(&self) -> &'a [u8] {
        self.input
    }
    pub fn field(&self) -> &'f Field {
        self.field
    }
    pub fn batch_metadata(&self) -> &'a [u8] {
        self.batch_metadata
    }
    pub fn batch_body(&self) -> &'a [u8] {
        self.batch_body
    }
    pub fn geometry(&self) -> FlatBatchGeometry {
        self.geometry
    }
    /// Reuses the safe header from the sole official verification. This does
    /// not admit reader allocations, work or ConstantPool construction.
    pub fn record_batch(&self) -> arrow::ipc::RecordBatch<'a> {
        self.batch
    }
    pub fn metadata_version(&self) -> arrow::ipc::MetadataVersion {
        self.version
    }
}

pub(crate) fn require(
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

pub(crate) fn framing_step<E: ReaderGeometryFailure>(
    admission: &mut Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if let Some(a) = admission.as_deref_mut() {
        a.batch_step(work)?;
    } else {
        work.step()?;
    }
    Ok(())
}
pub(crate) fn framing_require<E: ReaderGeometryFailure>(
    condition: bool,
    message: &'static str,
    mut admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    framing_step(&mut admission, work)?;
    if condition {
        Ok(())
    } else {
        Err((TypeCodecError::InvalidShape(message)).into())
    }
}
pub(crate) fn framing_limit<E: ReaderGeometryFailure>(
    condition: bool,
    message: &'static str,
    admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), E> {
    if admission.is_some() && !condition {
        return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
    }
    framing_require(condition, message, admission, work)
}
#[cfg(test)]
pub(crate) fn metadata<'a>(
    input: &'a [u8],
    offset: usize,
    limit: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), TypeCodecError> {
    metadata_in(input, offset, limit, None, None, work)
}
#[cfg(test)]
pub(crate) fn metadata_in<'a, E: ReaderGeometryFailure>(
    input: &'a [u8],
    offset: usize,
    limit: usize,
    verifier_apparent: Option<usize>,
    admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), E> {
    metadata_core(
        input,
        offset,
        Some(limit),
        verifier_apparent,
        admission,
        work,
    )
}
pub(crate) fn metadata_core<'a, E: ReaderGeometryFailure>(
    input: &'a [u8],
    offset: usize,
    limit: Option<usize>,
    verifier_apparent: Option<usize>,
    mut admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a [u8], usize), E> {
    let prefix = continuation_prefix(input, offset);
    if let (Some(a), Some(apparent), Ok(ContinuationPrefix::Metadata { len, .. })) =
        (admission.as_deref_mut(), verifier_apparent, &prefix)
    {
        let upper = apparent
            .checked_add(*len)
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        a.batch_work_add(upper)?;
    }
    framing_step(&mut admission, work)?;
    let prefix = prefix
        .map_err(|_| TypeCodecError::InvalidShape("invalid constant IPC continuation prefix"))?;
    let ContinuationPrefix::Metadata { start, len } = prefix else {
        return Err((TypeCodecError::InvalidShape(
            "constant IPC stream ended before its required message",
        ))
        .into());
    };
    // MessageReader interprets this wire length as signed i32, even though
    // the neutral prefix primitive can project every u32 without allocating.
    framing_limit(
        len <= i32::MAX as usize && limit.is_none_or(|limit| len <= limit),
        "constant IPC metadata length exceeds envelope",
        admission.as_deref_mut(),
        work,
    )?;
    // Standard writer alignments 8/16/32/64 all include metadata padding in
    // this length. Keep the minimum standard alignment, not NRX1's fixed64.
    framing_require(
        len % 8 == 0,
        "constant IPC metadata framing is not eight-byte aligned",
        admission.as_deref_mut(),
        work,
    )?;
    let range = checked_range(input.len(), start, len)
        .map_err(|_| TypeCodecError::InvalidShape("constant IPC metadata is truncated"))?;
    framing_step(&mut admission, work)?;
    let end = range.end;
    Ok((&input[range], end))
}

/// Requires exactly one continuation-prefixed V5 RecordBatch. The verified
/// header is reused for geometry and recovery on the original Decode scope.
pub fn preflight_flat_constant_stream<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight(input, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
pub(crate) fn preflight_flat_constant_stream_in<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    source_retained_bytes: usize,
    admit: &mut progress::ReaderAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    preflight_flat_constant_stream_observed(
        input,
        expected,
        limits,
        verifier,
        source_retained_bytes,
        &mut |facts| admit(facts).map_err(TypeCodecError::from),
        None,
        work,
    )
}

pub(crate) fn preflight_flat_constant_stream_observed<'a, 'f, 'port, E: ReaderGeometryFailure>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    source_retained_bytes: usize,
    admit: &'port mut progress::ReaderAdmit<'port, E>,
    construction: Option<&'port mut (dyn FnMut(std::alloc::Layout) -> Result<(), E> + 'port)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, E> {
    if input.len() > source_retained_bytes {
        return Err((TypeCodecError::InvalidShape(
            "flat stream source retention is below visible input",
        ))
        .into());
    }
    let mut admission =
        progress::Admission::new(source_retained_bytes, admit).with_construction(construction);
    admission.check()?;
    preflight_core(
        input,
        expected,
        limits,
        verifier,
        Some(&mut admission),
        work,
    )
}

fn preflight<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, TypeCodecError> {
    preflight_core(input, expected, limits, verifier, None, work)
}
fn preflight_core<'a, 'f, E: ReaderGeometryFailure>(
    input: &'a [u8],
    expected: &'f Field,
    limits: FlatStreamProjectionLimits,
    verifier: &VerifierOptions,
    admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, E> {
    preflight_profile_core(
        input,
        expected,
        Some(limits),
        Some(verifier),
        admission,
        work,
    )
}
pub(crate) fn preflight_flat_constant_stream_input_derived_observed<
    'a,
    'f,
    'port,
    E: ReaderGeometryFailure,
>(
    input: &'a [u8],
    expected: &'f Field,
    source: usize,
    admit: &'port mut progress::ReaderAdmit<'port, E>,
    construction: Option<&'port mut (dyn FnMut(std::alloc::Layout) -> Result<(), E> + 'port)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, E> {
    if input.len() > source {
        return Err(TypeCodecError::InvalidShape(
            "flat stream source retention is below visible input",
        )
        .into());
    }
    let mut admission = progress::Admission::new(source, admit).with_construction(construction);
    admission.check()?;
    preflight_profile_core(input, expected, None, None, Some(&mut admission), work)
}
/// The supported RecordBatch has exactly Message/RecordBatch tables and three
/// inline vectors (FieldNodes, Buffer descriptors, variadic counts). Cover each
/// apparent visit from the actual metadata extent, with checked arithmetic.
pub(crate) fn input_derived_batch_verifier(
    metadata_bytes: usize,
) -> Result<VerifierOptions, TypeCodecError> {
    Ok(VerifierOptions {
        max_depth: 3,
        max_tables: 2,
        max_apparent_size: metadata_bytes
            .checked_mul(2 + 3)
            .ok_or(TypeCodecError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            ))?,
        ignore_missing_null_terminator: false,
    })
}
/// Extract the RecordBatch from a trusted Arrow StreamWriter test fixture.
#[cfg(test)]
pub(crate) fn standard_test_record_frame(stream: &[u8]) -> &[u8] {
    let ContinuationPrefix::Metadata { start, len } = continuation_prefix(stream, 0).unwrap()
    else {
        panic!("test fixture must contain an Arrow Schema");
    };
    let batch_start = start + len;
    let ContinuationPrefix::Metadata { start, len } =
        continuation_prefix(stream, batch_start).unwrap()
    else {
        panic!("test fixture must contain a RecordBatch");
    };
    let end = start + len;
    let message = arrow::ipc::root_as_message(&stream[start..end]).unwrap();
    assert_eq!(
        message.header_type(),
        arrow::ipc::MessageHeader::RecordBatch
    );
    let body_end = end + usize::try_from(message.bodyLength()).unwrap();
    assert_eq!(stream[body_end..], [255, 255, 255, 255, 0, 0, 0, 0]);
    &stream[batch_start..body_end]
}

pub(crate) struct RecordFrame<'a> {
    pub metadata: &'a [u8],
    pub body: &'a [u8],
    pub message: arrow::ipc::Message<'a>,
}

/// Project one closed frame before interpreting its geometry. The declared
/// body extent includes padding, with no EOS or subsequent bytes.
pub(crate) fn record_frame<'a, E: ReaderGeometryFailure>(
    input: &'a [u8],
    max_metadata_bytes: Option<usize>,
    max_body_bytes: Option<usize>,
    verifier: Option<&VerifierOptions>,
    mut admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecordFrame<'a>, E> {
    let (metadata, body_start) = metadata_core(
        input,
        0,
        max_metadata_bytes,
        verifier.map(|options| options.max_apparent_size),
        admission.as_deref_mut(),
        work,
    )?;
    let derived;
    let verifier = if let Some(verifier) = verifier {
        verifier
    } else {
        derived = input_derived_batch_verifier(metadata.len())?;
        if let Some(admission) = admission.as_deref_mut() {
            admission.batch_work_add(
                derived
                    .max_apparent_size
                    .checked_add(metadata.len())
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
            )?;
        }
        &derived
    };
    let message = verified_record_message_observed(metadata, verifier, work)?;
    framing_require(
        message.version() == arrow::ipc::MetadataVersion::V5
            && message.header_type() == arrow::ipc::MessageHeader::RecordBatch,
        "constant IPC frame must be a V5 RecordBatch",
        admission.as_deref_mut(),
        work,
    )?;
    let body_length = nonnegative_length(message.bodyLength()).map_err(|_| {
        TypeCodecError::InvalidShape("constant IPC body length is negative or unrepresentable")
    })?;
    framing_limit(
        max_body_bytes.is_none_or(|maximum| body_length <= maximum),
        "constant IPC body envelope exceeded",
        admission.as_deref_mut(),
        work,
    )?;
    framing_require(
        body_length % 8 == 0,
        "constant IPC body framing is not eight-byte aligned",
        admission.as_deref_mut(),
        work,
    )?;
    let range = checked_range(input.len(), body_start, body_length)
        .map_err(|_| TypeCodecError::InvalidShape("constant IPC body is truncated"))?;
    framing_require(
        range.end == input.len(),
        "constant IPC frame has trailing bytes",
        admission.as_deref_mut(),
        work,
    )?;
    Ok(RecordFrame {
        metadata,
        body: &input[range],
        message,
    })
}

pub(crate) fn verified_record_message_observed<'a>(
    metadata: &'a [u8],
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<arrow::ipc::Message<'a>, TypeCodecError> {
    // Bound opaque verification even when the message names a different
    // union member. RecordBatch's vectors of structs need no deep recursion.
    let mut verifier = verifier.clone();
    verifier.max_depth = verifier.max_depth.min(3);
    verifier.max_tables = verifier.max_tables.min(2);
    verifier
        .max_apparent_size
        .checked_add(metadata.len())
        .ok_or(TypeCodecError::InvalidShape("IPC verifier work overflows"))?;
    work.flush()?;
    let parsed = novarocks_arrow_ipc_frame::verified_message(metadata, &verifier);
    work.flush()?;
    parsed.map_err(|_| TypeCodecError::InvalidShape("invalid Arrow IPC message metadata"))
}

fn preflight_profile_core<'a, 'f, E: ReaderGeometryFailure>(
    input: &'a [u8],
    expected: &'f Field,
    limits: Option<FlatStreamProjectionLimits>,
    verifier: Option<&VerifierOptions>,
    mut admission: Option<&mut progress::Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FlatConstantStream<'a, 'f>, E> {
    if admission.is_some() {
        if limits.is_some_and(|limits| input.len() > limits.max_input_bytes) {
            return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
        }
    } else {
        require(
            limits.is_none_or(|limits| input.len() <= limits.max_input_bytes),
            "constant IPC input envelope exceeded",
            work,
        )?;
    }
    let RecordFrame {
        metadata: batch_metadata,
        body: batch_body,
        message,
    } = record_frame(
        input,
        limits.map(|limits| limits.batch.max_metadata_bytes),
        limits.map(|limits| limits.batch.max_body_bytes),
        verifier,
        admission.as_deref_mut(),
        work,
    )?;
    let geometry = if let Some(a) = admission.as_deref_mut() {
        crate::ipc_flat_batch_v2::preflight_verified_flat_record_batch_profile(
            message,
            batch_body,
            expected,
            limits.map(|limits| limits.batch),
            Some(a),
            work,
        )?
    } else {
        preflight_verified_flat_record_batch(
            message,
            batch_body,
            expected,
            limits.expect("fixed plain geometry profile").batch,
            work,
        )?
    };
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "constant IPC batch header is missing",
        ))?;
    Ok(FlatConstantStream {
        input,
        field: expected,
        batch_metadata,
        batch_body,
        batch,
        version: message.version(),
        geometry,
        progress: admission.as_deref().map(progress::Admission::facts),
    })
}

#[cfg(test)]
mod reader_tests;
#[cfg(test)]
mod tests;
