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

//! Complete borrowed framing for a flat non-dictionary constant pool stream.
//! Schema identity and batch geometry are checked by their existing authors.
//! No array/schema materialization or reader entry is performed here. This
//! projection does not authorize those allocations or replace the subsequent
//! complete resource model, Field/FVT pairing and ConstantPool value admission.

use crate::{
    ipc_flat_batch_v2::{
        FlatBatchGeometry, FlatBatchProjectionLimits, preflight_verified_flat_record_batch,
    },
    ipc_schema_v2::{
        IpcSchemaProjectionLimits, verified_message_observed, verify_single_field_schema_with_work,
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
    pub schema: IpcSchemaProjectionLimits,
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

/// Requires continuation framing, V5/Little schema, one uncompressed flat
/// RecordBatch, and an exact EOS with no trailing data. Every metadata Message
/// is officially verified once, on the same original Decode work scope.
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
    let (schema, schema_end) = metadata_core(
        input,
        0,
        limits.map(|limits| limits.schema.max_flatbuffer_bytes),
        verifier.map(|verifier| verifier.max_apparent_size),
        admission.as_deref_mut(),
        work,
    )?;
    if admission.is_some() {
        framing_step(&mut admission, work)?;
    }
    // The schema author also proves bodyLength==0, so the next frame starts
    // exactly at metadata_end. No speculative body skip or schema conversion.
    if let Some(a) = admission.as_deref_mut() {
        let source = a.source();
        let mut capture = |f: &crate::ipc_schema_v2::SchemaWriterRequestFacts| {
            a.requests(4, f.request_bytes, f.request_count)?;
            a.schema_work(f.work_upper_bound)
        };
        let mut host = |facts: &crate::ipc_schema_v2::SchemaWriterRequestFacts| {
            capture(facts).map_err(crate::host_projection_v2::ProjectionFailure::Host)
        };
        let verified = if let (Some(limits), Some(verifier)) = (limits, verifier) {
            crate::ipc_schema_v2::verify_single_field_schema_with_host_in(
                schema,
                expected,
                limits.schema,
                verifier,
                source,
                usize::MAX,
                &mut host,
                work,
            )
        } else {
            crate::ipc_schema_v2::verify_single_field_schema_input_derived_with_host_in(
                schema, expected, &mut host, work,
            )
            .map(|_| ())
        };
        verified.map_err(|error| match error {
            crate::host_projection_v2::ProjectionFailure::Host(error) => error,
            crate::host_projection_v2::ProjectionFailure::Codec(error) => E::from(error),
        })?;
    } else {
        verify_single_field_schema_with_work(
            schema,
            expected,
            limits.expect("fixed plain schema profile").schema,
            verifier.expect("fixed plain verifier"),
            work,
        )?;
    }
    let (batch_metadata, body_start) = metadata_core(
        input,
        schema_end,
        limits.map(|limits| limits.batch.max_metadata_bytes),
        verifier.map(|verifier| verifier.max_apparent_size),
        admission.as_deref_mut(),
        work,
    )?;
    let derived;
    let batch_verifier = if let Some(verifier) = verifier {
        verifier
    } else {
        derived = input_derived_batch_verifier(batch_metadata.len())?;
        if let Some(a) = admission.as_deref_mut() {
            a.batch_work_add(
                derived
                    .max_apparent_size
                    .checked_add(batch_metadata.len())
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?,
            )?;
        }
        &derived
    };
    let message = verified_message_observed(batch_metadata, batch_verifier, work)?;
    framing_require(
        message.header_type() == arrow::ipc::MessageHeader::RecordBatch,
        "constant IPC schema must precede exactly one RecordBatch",
        admission.as_deref_mut(),
        work,
    )?;
    let body_length = nonnegative_length(message.bodyLength()).map_err(|_| {
        TypeCodecError::InvalidShape("negative or unrepresentable constant IPC body length")
    })?;
    framing_limit(
        limits.is_none_or(|limits| body_length <= limits.batch.max_body_bytes),
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
    framing_step(&mut admission, work)?;
    // bodyLength already includes the writer's body padding. Rounding again
    // could skip hostile bytes or a genuine EOS and change the format verdict.
    let body_end = range.end;
    let batch_body = &input[range];
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
    let end = continuation_prefix(input, body_end);
    framing_step(&mut admission, work)?;
    let end =
        end.map_err(|_| TypeCodecError::InvalidShape("constant IPC stream is missing EOS"))?;
    framing_require(
        matches!(end, ContinuationPrefix::End { next_offset } if next_offset == input.len()),
        "constant IPC EOS has extra messages or trailing data",
        admission.as_deref_mut(),
        work,
    )?;
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
