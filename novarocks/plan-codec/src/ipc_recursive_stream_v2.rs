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

//! Checked recursive constant IPC framing and array-output materialization.
//! The numerical request/work projection precedes the public safe reader;
//! preparation hosts retain grant, allocation-origin and free ownership.

use crate::ipc_flat_stream_v2::ReaderFailure;
use crate::ipc_flat_stream_v2::progress::{
    Admission, IpcReaderProgressFacts, ReaderGeometryFailure,
};
use crate::{
    ipc_flat_stream_v2::{FlatPoolResourceError, require},
    ipc_recursive_batch_v2::{CheckedRecursiveBatch, preflight_verified_recursive_record_batch},
    physical_type_v2::TypeCodecError,
};
use arrow::{
    datatypes::{Field, Schema},
    ipc::reader::read_record_batch,
};
use arrow_buffer::Buffer;
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

mod reader_allocations;
mod reader_diagnostics;
mod reader_resources;
mod reader_work;
pub use crate::ipc_flat_stream_v2::FlatReaderError as RecursiveReaderError;
pub use crate::ipc_recursive_batch_v2::RecursiveBatchProjectionLimits;
pub use reader_resources::{RecursiveReaderProjectionLimits, RecursiveReaderResourceFacts};

#[derive(Clone, Copy, Debug)]
pub struct RecursiveStreamProjectionLimits {
    pub max_input_bytes: usize,
    pub batch: RecursiveBatchProjectionLimits,
}

/// Immutable borrowed framing and admitted geometry for the exact source Field.
/// Geometry scratch is owned here; values and the input body remain borrowed.
pub struct RecursiveConstantStream<'a, 'f> {
    input: &'a [u8],
    field: &'f Field,
    body: &'a [u8],
    batch: arrow::ipc::RecordBatch<'a>,
    version: arrow::ipc::MetadataVersion,
    checked: CheckedRecursiveBatch<'f>,
    progress: Option<IpcReaderProgressFacts>,
}

struct PreparedRecursiveParts<'v> {
    field: Arc<Field>,
    value_type: Cow<'v, FunctionValueType>,
    policy: ConstantPolicy,
    facts: RecursiveReaderResourceFacts,
    progress: Option<IpcReaderProgressFacts>,
}
/// Owns the original checked stream and its already admitted geometry scratch.
/// No caller can construct a receipt detached from that immutable snapshot.
pub(crate) struct PreparedRecursiveReader<'a, 'f, 'v, 'c> {
    original_control: Option<&'c dyn PureCompileControl>,
    stream: RecursiveConstantStream<'a, 'f>,
    parts: PreparedRecursiveParts<'v>,
}
impl PreparedRecursiveReader<'_, '_, '_, '_> {
    pub(crate) fn body_bytes(&self) -> usize {
        self.stream.body.len()
    }

    pub(crate) fn facts(&self) -> &RecursiveReaderResourceFacts {
        &self.parts.facts
    }
    pub(crate) fn geometry_scratch_request_bytes(&self) -> usize {
        self.stream.checked.scratch_request_bytes
    }
    pub(crate) fn geometry_scratch_request_count(&self) -> usize {
        self.stream.checked.scratch_request_count
    }
    #[cfg(test)]
    pub(crate) fn materialize_in(
        self,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        let Some(original) = self.original_control else {
            return Err(FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                "recursive reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                "recursive reader belongs to another original control",
            ))
            .into());
        }
        let mut adapter =
            |facts: &IpcReaderProgressFacts| admit(facts).map_err(RecursiveReaderError::from);
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, &mut adapter);
        admission.seed(self.parts.progress.ok_or_else(|| {
            FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                "recursive reader is missing caller-owned facts",
            ))
        })?)?;
        self.stream
            .materialize_parts_core(self.parts, Some(&mut admission), None, None, work)
    }

    pub(crate) fn materialize_pure_observed<E: ReaderFailure>(
        self,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        let Some(original) = self.original_control else {
            return Err(RecursiveReaderError::from(reader_resources::invalid(
                "recursive reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(RecursiveReaderError::from(reader_resources::invalid(
                "recursive reader belongs to another original control",
            ))
            .into());
        }
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, admit);
        admission.seed(self.parts.progress.ok_or_else(|| {
            reader_resources::invalid("recursive reader is missing caller-owned facts")
        })?)?;
        self.stream
            .materialize_parts_core(self.parts, Some(&mut admission), None, None, work)
    }

    pub(crate) fn materialize_with_creator_observed<H>(
        self,
        source: novarocks_constant_contract::ConstantSourceOwner,
        creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<
            '_,
            crate::host_projection_v2::ProjectionFailure<RecursiveReaderError, H>,
        >,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, crate::host_projection_v2::ProjectionFailure<RecursiveReaderError, H>>
    {
        creation.with_error_mapping(
            crate::host_projection_v2::ProjectionFailure::Host,
            |mapped| self.materialize_with_scope_observed(source, mapped, admit, work),
        )
    }
    pub(crate) fn materialize_with_scope_observed<E: ReaderFailure>(
        self,
        source: novarocks_constant_contract::ConstantSourceOwner,
        creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<E>,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        let Some(original) = self.original_control else {
            return Err(RecursiveReaderError::from(reader_resources::invalid(
                "recursive reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(RecursiveReaderError::from(reader_resources::invalid(
                "recursive reader belongs to another original control",
            ))
            .into());
        }
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, admit);
        admission.seed(self.parts.progress.ok_or_else(|| {
            reader_resources::invalid("recursive reader is missing caller-owned facts")
        })?)?;
        self.stream.materialize_parts_core(
            self.parts,
            Some(&mut admission),
            Some(source),
            Some(crate::ipc_flat_stream_v2::reader_creation::ReaderCreation::Single(creation)),
            work,
        )
    }
    pub(crate) fn materialize_with_paired_creator_observed<H>(
        self,
        source: novarocks_constant_contract::ConstantSourceOwner,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        pool: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<
            '_,
            crate::host_projection_v2::ProjectionFailure<
                crate::ipc_flat_stream_v2::FlatReaderError,
                H,
            >,
        >,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<
        ConstantPool,
        crate::host_projection_v2::ProjectionFailure<crate::ipc_flat_stream_v2::FlatReaderError, H>,
    > {
        let Some(original) = self.original_control else {
            return Err(RecursiveReaderError::from(reader_resources::invalid(
                "recursive reader requires caller-owned preparation",
            ))
            .into());
        };
        if !std::ptr::addr_eq(original, work.control()) {
            return Err(RecursiveReaderError::from(reader_resources::invalid(
                "recursive reader belongs to another original control",
            ))
            .into());
        }
        let mut temporary = crate::ipc_flat_stream_v2::reader_creation::MappedScope(temporary);
        let mut retained = crate::ipc_flat_stream_v2::reader_creation::MappedScope(retained);
        let mut pool = crate::ipc_flat_stream_v2::reader_creation::MappedScope(pool);
        let mut admission = Admission::new(self.parts.facts.source_retained_bytes, admit);
        admission.seed(self.parts.progress.ok_or_else(|| {
            reader_resources::invalid("recursive reader is missing caller-owned facts")
        })?)?;
        self.stream.materialize_parts_core(
            self.parts,
            Some(&mut admission),
            Some(source),
            Some(
                crate::ipc_flat_stream_v2::reader_creation::ReaderCreation::Paired {
                    temporary: &mut temporary,
                    retained: &mut retained,
                    pool: &mut pool,
                },
            ),
            work,
        )
    }

    pub(crate) fn materialize(
        self,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = self.stream.materialize_parts(self.parts, &mut work);
        finish_prepared_reader(work, result)
    }
}
fn finish_prepared_reader<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, RecursiveReaderError>,
) -> Result<T, RecursiveReaderError> {
    if matches!(
        &result,
        Err(RecursiveReaderError::Projection(
            FlatPoolResourceError::Control(_)
        ))
    ) {
        return result;
    }
    work.finish()?;
    result
}

pub fn preflight_recursive_constant_stream<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<RecursiveConstantStream<'a, 'f>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = preflight(input, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
pub(crate) fn preflight_recursive_constant_stream_in<'a, 'f>(
    input: &'a [u8],
    expected: &'f Field,
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    source_retained_bytes: usize,
    admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, TypeCodecError> {
    preflight_recursive_constant_stream_observed(
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

pub(crate) fn preflight_recursive_constant_stream_observed<
    'a,
    'f,
    'port,
    E: ReaderGeometryFailure,
>(
    input: &'a [u8],
    expected: &'f Field,
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    source_retained_bytes: usize,
    admit: &'port mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'port, E>,
    construction: Option<&'port mut (dyn FnMut(std::alloc::Layout) -> Result<(), E> + 'port)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, E> {
    if input.len() > source_retained_bytes {
        return Err((TypeCodecError::InvalidShape(
            "recursive stream source retention is below visible input",
        ))
        .into());
    }
    let mut admission =
        Admission::new(source_retained_bytes, admit).with_construction(construction);
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
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, TypeCodecError> {
    preflight_core(input, expected, limits, verifier, None, work)
}
fn preflight_core<'a, 'f, E: ReaderGeometryFailure>(
    input: &'a [u8],
    expected: &'f Field,
    limits: RecursiveStreamProjectionLimits,
    verifier: &VerifierOptions,
    admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, E> {
    preflight_profile_core(
        input,
        expected,
        Some(limits),
        Some(verifier),
        admission,
        work,
    )
}
pub(crate) fn preflight_recursive_constant_stream_input_derived_observed<
    'a,
    'f,
    'port,
    E: ReaderGeometryFailure,
>(
    input: &'a [u8],
    expected: &'f Field,
    source: usize,
    admit: &'port mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'port, E>,
    construction: Option<&'port mut (dyn FnMut(std::alloc::Layout) -> Result<(), E> + 'port)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, E> {
    if input.len() > source {
        return Err(TypeCodecError::InvalidShape(
            "recursive stream source retention is below visible input",
        )
        .into());
    }
    let mut admission = Admission::new(source, admit).with_construction(construction);
    admission.check()?;
    preflight_profile_core(input, expected, None, None, Some(&mut admission), work)
}
fn preflight_profile_core<'a, 'f, E: ReaderGeometryFailure>(
    input: &'a [u8],
    expected: &'f Field,
    limits: Option<RecursiveStreamProjectionLimits>,
    verifier: Option<&VerifierOptions>,
    mut admission: Option<&mut Admission<'_, '_, E>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<RecursiveConstantStream<'a, 'f>, E> {
    if admission.is_some() {
        if limits.is_some_and(|limits| input.len() > limits.max_input_bytes) {
            return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
        }
    } else {
        require(
            limits.is_none_or(|limits| input.len() <= limits.max_input_bytes),
            "recursive IPC input envelope exceeded",
            work,
        )?;
    }
    let crate::ipc_flat_stream_v2::RecordFrame { body, message, .. } =
        crate::ipc_flat_stream_v2::record_frame(
            input,
            limits.map(|limits| limits.batch.flat.max_metadata_bytes),
            limits.map(|limits| limits.batch.flat.max_body_bytes),
            verifier,
            admission.as_deref_mut(),
            work,
        )?;
    let checked = if let Some(a) = admission.as_deref_mut() {
        crate::ipc_recursive_batch_v2::preflight_verified_recursive_record_batch_profile(
            message,
            body,
            expected,
            limits.map(|limits| limits.batch),
            Some(a),
            work,
        )?
    } else {
        preflight_verified_recursive_record_batch(
            message,
            body,
            expected,
            limits.expect("fixed plain geometry profile").batch,
            work,
        )?
    };
    let batch = message
        .header_as_record_batch()
        .ok_or(TypeCodecError::InvalidShape(
            "recursive IPC RecordBatch is missing",
        ))?;
    Ok(RecursiveConstantStream {
        input,
        field: expected,
        body,
        batch,
        version: message.version(),
        checked,
        progress: admission.as_deref().map(Admission::facts),
    })
}

impl<'a, 'f> RecursiveConstantStream<'a, 'f> {
    fn reader_input(&self) -> reader_resources::ReaderInput<'_, '_> {
        reader_resources::ReaderInput {
            field: self.field,
            batch: self.batch,
            body: self.body,
            nodes: &self.checked.nodes,
            geometry: self.checked.geometry,
            geometry_scratch_request_bytes: self.checked.scratch_request_bytes,
            geometry_scratch_request_count: self.checked.scratch_request_count,
        }
    }
    fn reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<RecursiveReaderResourceFacts, FlatPoolResourceError> {
        require(
            self.input.len() <= source_retained_bytes,
            "recursive reader source retention below visible input",
            work,
        )?;
        reader_resources::preflight(
            &self.reader_input(),
            value_type,
            source_retained_bytes,
            policy,
            limits,
            work,
        )
    }
    /// Source retention includes the complete input allocation, original Field
    /// and both original Field and supplied value-type backing, including
    /// their names/metadata and HashMap removed-entry capacity.
    /// Earlier framing and geometry retain their separate admission boundary.
    /// Numerical grouping uses fixed stack indices under the admitted type-node
    /// bound; stack-scratch admission remains the caller's separate obligation.
    pub fn preflight_reader_resources(
        &self,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<RecursiveReaderResourceFacts, FlatPoolResourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result =
            self.reader_resources(value_type, source_retained_bytes, policy, limits, &mut work);
        if matches!(&result, Err(FlatPoolResourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    /// The original Field Arc and admitted full value type are retained. No
    /// array, Schema or body backing is created before the complete output gate.
    pub fn materialize_pool(
        &self,
        field: Arc<Field>,
        value_type: FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        self.materialize_pool_core(
            field,
            Cow::Owned(value_type),
            source_retained_bytes,
            policy,
            limits,
            control,
        )
    }

    /// Borrow the admitted type-table value until the original complete gate
    /// and reader succeed. The permitted carrier profile clones only Arc
    /// owners or inline parameters; no recursive Field or metadata is copied.
    pub fn materialize_pool_borrowed(
        &self,
        field: Arc<Field>,
        value_type: &FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        self.materialize_pool_core(
            field,
            Cow::Borrowed(value_type),
            source_retained_bytes,
            policy,
            limits,
            control,
        )
    }

    fn materialize_pool_core(
        &self,
        field: Arc<Field>,
        value_type: Cow<'_, FunctionValueType>,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            let parts = self.prepare_parts(
                field,
                value_type,
                source_retained_bytes,
                policy,
                limits,
                &mut work,
            )?;
            self.materialize_parts(parts, &mut work)
        })();
        finish_prepared_reader(work, result)
    }

    /// Freeze the single numerical/model pass while retaining the original
    /// geometry. A later namespace gate cannot retro-authorize its allocation.
    pub(crate) fn prepare_pool_borrowed<'v>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedRecursiveReader<'a, 'f, 'v, 'static>, RecursiveReaderError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let parts = self.prepare_parts(
            field,
            Cow::Borrowed(value_type),
            source_retained_bytes,
            policy,
            limits,
            &mut work,
        );
        let parts = finish_prepared_reader(work, parts)?;
        Ok(PreparedRecursiveReader {
            original_control: None,
            stream: self,
            parts,
        })
    }

    #[cfg(test)]
    pub(crate) fn prepare_pool_borrowed_in<'v, 'c>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_>,
        work: &mut CompileCheckpoints<'c>,
    ) -> Result<PreparedRecursiveReader<'a, 'f, 'v, 'c>, RecursiveReaderError> {
        self.prepare_pool_borrowed_observed(
            field,
            value_type,
            source_retained_bytes,
            policy,
            limits,
            &mut |facts| admit(facts).map_err(RecursiveReaderError::from),
            work,
        )
    }

    pub(crate) fn prepare_pool_borrowed_observed<'v, 'c, E: ReaderFailure>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'c>,
    ) -> Result<PreparedRecursiveReader<'a, 'f, 'v, 'c>, E> {
        let mut admission = Admission::new(source_retained_bytes, admit);
        admission.limits(
            limits.max_new_allocation_request_bytes,
            limits.max_coexisting_source_and_request_bytes,
            limits.max_cumulative_library_work,
        )?;
        if let Some(prefix) = self.progress {
            admission.seed_prefix(prefix)?;
        }
        let parts = self.prepare_parts_core(
            field,
            Cow::Borrowed(value_type),
            source_retained_bytes,
            policy,
            limits,
            Some(&mut admission),
            work,
        )?;
        Ok(PreparedRecursiveReader {
            original_control: Some(work.control()),
            stream: self,
            parts,
        })
    }

    fn prepare_parts<'v>(
        &self,
        field: Arc<Field>,
        value_type: Cow<'v, FunctionValueType>,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedRecursiveParts<'v>, RecursiveReaderError> {
        self.prepare_parts_core(
            field,
            value_type,
            source_retained_bytes,
            policy,
            limits,
            None,
            work,
        )
    }
    fn prepare_parts_core<'v, E: ReaderFailure>(
        &self,
        field: Arc<Field>,
        value_type: Cow<'v, FunctionValueType>,
        source_retained_bytes: usize,
        policy: ConstantPolicy,
        limits: RecursiveReaderProjectionLimits,
        admission: Option<&mut Admission<'_, '_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedRecursiveParts<'v>, E> {
        self.prepare_parts_profile_core(
            field,
            value_type,
            source_retained_bytes,
            Some(policy),
            Some(limits),
            admission,
            work,
        )
    }
    pub(crate) fn prepare_pool_borrowed_input_derived_observed<'v, 'c, E: ReaderFailure>(
        self,
        field: Arc<Field>,
        value_type: &'v FunctionValueType,
        source: usize,
        admit: &mut crate::ipc_flat_stream_v2::progress::ReaderAdmit<'_, E>,
        work: &mut CompileCheckpoints<'c>,
    ) -> Result<PreparedRecursiveReader<'a, 'f, 'v, 'c>, E> {
        let mut admission = Admission::new(source, admit);
        if let Some(prefix) = self.progress {
            admission.seed_prefix(prefix)?;
        }
        let parts = self.prepare_parts_profile_core(
            field,
            Cow::Borrowed(value_type),
            source,
            None,
            None,
            Some(&mut admission),
            work,
        )?;
        Ok(PreparedRecursiveReader {
            original_control: Some(work.control()),
            stream: self,
            parts,
        })
    }
    fn prepare_parts_profile_core<'v, E: ReaderFailure>(
        &self,
        field: Arc<Field>,
        value_type: Cow<'v, FunctionValueType>,
        source_retained_bytes: usize,
        policy: Option<ConstantPolicy>,
        limits: Option<RecursiveReaderProjectionLimits>,
        mut admission: Option<&mut Admission<'_, '_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PreparedRecursiveParts<'v>, E> {
        if admission.is_some() {
            if !std::ptr::eq(self.field, field.as_ref()) {
                return Err(FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                    "recursive reader requires the original source Field Arc",
                ))
                .into());
            }
            if let Some(a) = admission.as_deref_mut() {
                let header = reader_allocations::initial_header(&field)?;
                a.requests(
                    2,
                    header.structural_request_bytes_upper_bound,
                    header.allocation_requests_upper_bound,
                )?;
                let body_len = self.body.len();
                let rounded = a.numeric(reader_resources::capacity(body_len))?;
                a.requests(1, rounded, usize::from(rounded != 0))?;
                a.clone_work(
                    crate::physical_type_v2::value_type_clone_preflight_work_upper_bound(),
                )?;
                crate::physical_type_v2::preflight_value_type_clone_admitted::<E>(
                    value_type.as_ref(),
                    &mut |f, _| {
                        a.requests(
                            5,
                            f.allocation_request_bytes_upper_bound(),
                            f.allocation_requests_upper_bound(),
                        )?;
                        a.clone_work(f.work_upper_bound())?;
                        Ok(())
                    },
                    work,
                )?;
            }
            work.step()?;
        } else {
            require(
                std::ptr::eq(self.field, field.as_ref()),
                "recursive reader requires the original source Field Arc",
                work,
            )
            .map_err(FlatPoolResourceError::from)?;
        }
        let (mut facts, sealed) = if let Some(a) = admission.as_deref_mut() {
            if self.input.len() > source_retained_bytes {
                return Err(FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                    "recursive reader source retention below visible input",
                ))
                .into());
            }
            reader_resources::preflight_profile_core(
                &self.reader_input(),
                value_type.as_ref(),
                source_retained_bytes,
                policy,
                limits,
                Some(a),
                work,
            )?
        } else {
            let policy = policy.ok_or_else(|| {
                FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                    "input-derived recursive reader requires original observation",
                ))
            })?;
            let limits = limits.ok_or_else(|| {
                FlatPoolResourceError::from(TypeCodecError::InvalidShape(
                    "plain recursive reader requires explicit profile",
                ))
            })?;
            (
                self.reader_resources(
                    value_type.as_ref(),
                    source_retained_bytes,
                    policy,
                    limits,
                    work,
                )?,
                policy,
            )
        };
        let progress = admission.as_deref().map(Admission::facts);
        if let Some(p) = progress {
            facts.new_allocation_request_bytes_upper_bound =
                p.new_allocation_request_bytes_upper_bound;
            facts.allocation_request_count_upper_bound = p.allocation_request_count_upper_bound;
            facts.coexisting_source_and_request_bytes_upper_bound = source_retained_bytes
                .checked_add(p.new_allocation_request_bytes_upper_bound)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            facts.cumulative_library_work_upper_bound = p.cumulative_library_work_upper_bound;
            facts.structural_request_bytes_upper_bound = p
                .new_allocation_request_bytes_upper_bound
                .checked_sub(facts.payload_request_bytes_upper_bound)
                .and_then(|n| n.checked_sub(facts.diagnostic_request_bytes_upper_bound))
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        }
        Ok(PreparedRecursiveParts {
            field,
            value_type,
            policy: sealed,
            facts,
            progress,
        })
    }

    fn materialize_parts(
        &self,
        parts: PreparedRecursiveParts<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, RecursiveReaderError> {
        self.materialize_parts_core(parts, None, None, None, work)
    }
    fn materialize_parts_core<E: ReaderFailure>(
        &self,
        parts: PreparedRecursiveParts<'_>,
        admission: Option<&mut Admission<'_, '_, E>>,
        source: Option<novarocks_constant_contract::ConstantSourceOwner>,
        mut creation: Option<crate::ipc_flat_stream_v2::reader_creation::ReaderCreation<'_, E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        let PreparedRecursiveParts {
            field,
            value_type,
            policy,
            facts,
            progress,
        } = parts;
        if let Some(creation) = creation.as_mut() {
            let operation = facts
                .new_allocation_request_bytes_upper_bound
                .checked_sub(facts.constant_owner_request_bytes_upper_bound)
                .and_then(|bytes| {
                    bytes.checked_sub(progress.map_or(0, |p| p.geometry_scratch_request_bytes))
                })
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            creation.operation(
                operation,
                facts.retained_payload_request_bytes_upper_bound,
                facts.retained_reader_request_bytes_upper_bound,
                facts.retained_pool_data_request_bytes_upper_bound,
                work,
            )?;
        }
        work.flush()?;
        let schema = Arc::new(Schema::new([Arc::clone(&field)]));
        if admission.is_some() {
            work.step()?;
        }
        work.flush()?;
        let body = Buffer::from_slice_ref(self.body);
        if admission.is_some() {
            work.step()?;
        }
        work.flush()?;
        let decoded = read_record_batch(
            &body,
            self.batch,
            schema,
            &HashMap::new(),
            None,
            &self.version,
        )
        .map_err(RecursiveReaderError::Arrow);
        work.flush()?;
        let decoded = decoded?;
        if admission.is_some() {
            work.step()?;
            work.flush()?;
        }
        let data = decoded.column(0).to_data();
        if admission.is_some() {
            work.step()?;
        }
        work.flush()?;
        let value_type = if admission.is_some() {
            crate::physical_type_v2::clone_value_type_observed(value_type.as_ref(), work)
                .map_err(FlatPoolResourceError::from)?
        } else {
            value_type.into_owned()
        };
        let pool = if let Some(a) = admission {
            let mut capture = |f: &novarocks_constant_contract::ConstantOwnerResourceFacts| {
                a.constant(
                    f.allocation_request_bytes_upper_bound,
                    f.allocation_requests_upper_bound,
                    f.cumulative_work_upper_bound,
                )
            };
            if let Some(creation) = creation {
                let source = source.ok_or_else(|| {
                    reader_resources::invalid(
                        "recursive creator reader is missing original source custody",
                    )
                })?;
                creation
                    .pool(
                        field,
                        value_type,
                        data,
                        policy,
                        source,
                        &mut |facts| {
                            capture(facts).map_err(
                                novarocks_constant_contract::ConstantCreationFailure::Creator,
                            )
                        },
                        work,
                    )
                    .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)?
            } else {
                ConstantPool::try_new_pure_observed_in(
                    field,
                    value_type,
                    data,
                    policy,
                    &mut capture,
                    work,
                )
                .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)?
            }
        } else {
            ConstantPool::try_new(
                field,
                value_type,
                data,
                policy,
                CompilePhase::Decode,
                work.control(),
            )
            .map_err(FlatPoolResourceError::from)?
        };
        Ok(pool)
    }
}

#[cfg(test)]
#[path = "ipc_recursive_stream_v2_tests.rs"]
mod tests;
