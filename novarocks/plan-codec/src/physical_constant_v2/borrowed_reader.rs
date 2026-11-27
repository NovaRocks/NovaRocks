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

//! Shared borrowed constant sources, with the caller's real construction port.
use super::{ConstantDecodeProjectionLimits, PhysicalConstantCodecError as Error, recursive};
use crate::{
    host_projection_v2::ProjectionFailure,
    ipc_flat_stream_v2::{
        FlatReaderError, PreparedFlatReader, ReaderFailure,
        preflight_flat_constant_stream_observed, progress::IpcReaderProgressFacts,
    },
    ipc_recursive_stream_v2::{
        PreparedRecursiveReader, preflight_recursive_constant_stream_observed,
    },
};
use arrow::datatypes::Field;
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantPolicy, ConstantPool, ConstantSourceOwner};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, PureCompileControl,
    ValueSchemaCreation, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, sync::Arc};

type CreatorReaderAdmit<'a, H> =
    dyn FnMut(&IpcReaderProgressFacts) -> Result<(), ProjectionFailure<FlatReaderError, H>> + 'a;
pub(crate) enum PreparedReader<'raw, 'table, 'control> {
    Flat(PreparedFlatReader<'raw, 'table, 'table, 'control>),
    Recursive(PreparedRecursiveReader<'raw, 'table, 'table, 'control>),
}
impl PreparedReader<'_, '_, '_> {
    pub(super) fn body_bytes(&self) -> usize {
        match self {
            Self::Flat(value) => value.body_bytes(),
            Self::Recursive(value) => value.body_bytes(),
        }
    }

    pub(crate) fn geometry_requests(&self) -> (usize, usize) {
        match self {
            Self::Flat(value) => (
                value.geometry_scratch_request_bytes(),
                value.geometry_scratch_request_count(),
            ),
            Self::Recursive(value) => (
                value.geometry_scratch_request_bytes(),
                value.geometry_scratch_request_count(),
            ),
        }
    }
    pub(crate) fn reader_requests(&self) -> (usize, usize, usize) {
        match self {
            Self::Flat(value) => {
                let facts = value.facts();
                (
                    facts.new_allocation_request_bytes_upper_bound,
                    facts.allocation_request_count_upper_bound,
                    facts.cumulative_library_work_upper_bound,
                )
            }
            Self::Recursive(value) => {
                let facts = value.facts();
                (
                    facts.new_allocation_request_bytes_upper_bound,
                    facts.allocation_request_count_upper_bound,
                    facts.cumulative_library_work_upper_bound,
                )
            }
        }
    }
    pub(crate) fn retained_reader_requests(&self) -> Result<usize, CompileControlError> {
        let (headers, payload) = match self {
            Self::Flat(reader) => (
                reader
                    .facts()
                    .retained_reader_request_bytes_upper_bound
                    .checked_add(reader.facts().retained_pool_data_request_bytes_upper_bound)
                    .ok_or(CompileControlError::ResourceExhausted)?,
                reader.facts().retained_payload_request_bytes_upper_bound,
            ),
            Self::Recursive(reader) => (
                reader
                    .facts()
                    .retained_reader_request_bytes_upper_bound
                    .checked_add(reader.facts().retained_pool_data_request_bytes_upper_bound)
                    .ok_or(CompileControlError::ResourceExhausted)?,
                reader.facts().retained_payload_request_bytes_upper_bound,
            ),
        };
        headers
            .checked_add(payload)
            .ok_or(CompileControlError::ResourceExhausted)
    }
    pub(crate) fn materialize(
        self,
        control: &dyn PureCompileControl,
    ) -> Result<ConstantPool, Error> {
        Ok(match self {
            Self::Flat(value) => value.materialize(control)?,
            Self::Recursive(value) => value.materialize(control)?,
        })
    }
    pub(crate) fn materialize_pure_observed<E: ReaderFailure>(
        self,
        admit: &mut dyn FnMut(&IpcReaderProgressFacts) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, E> {
        match self {
            Self::Flat(reader) => reader.materialize_pure_observed(admit, work),
            Self::Recursive(reader) => reader.materialize_pure_observed(admit, work),
        }
    }
    pub(crate) fn materialize_with_creator_observed<H>(
        self,
        source: ConstantSourceOwner,
        creation: &mut ValueSchemaCreation<'_, H>,
        admit: &mut CreatorReaderAdmit<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, ProjectionFailure<FlatReaderError, H>> {
        match self {
            Self::Flat(reader) => {
                reader.materialize_with_creator_observed(source, creation, admit, work)
            }
            Self::Recursive(reader) => {
                reader.materialize_with_creator_observed(source, creation, admit, work)
            }
        }
    }
    pub(crate) fn materialize_with_paired_creator_observed<H>(
        self,
        source: ConstantSourceOwner,
        temporary: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        retained: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        pool: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
        admit: &mut CreatorReaderAdmit<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, ProjectionFailure<FlatReaderError, H>> {
        match self {
            Self::Flat(reader) => reader.materialize_with_paired_creator_observed(
                source, temporary, retained, pool, admit, work,
            ),
            Self::Recursive(reader) => reader.materialize_with_paired_creator_observed(
                source, temporary, retained, pool, admit, work,
            ),
        }
    }
}

pub(crate) fn prepare_reader_from_sources_profile_observed<
    'raw,
    'source,
    'control,
    E: ReaderFailure,
>(
    record: &'raw wire::IpcConstantPool,
    value_type: &'source FunctionValueType,
    field: &'source Arc<Field>,
    live_source: usize,
    profile: Option<(
        ConstantPolicy,
        ConstantDecodeProjectionLimits,
        &VerifierOptions,
    )>,
    admit: &mut dyn FnMut(&IpcReaderProgressFacts) -> Result<(), E>,
    mut construction: Option<&mut dyn FnMut(Layout) -> Result<(), E>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedReader<'raw, 'source, 'control>, E> {
    let observed = construction.is_some();
    let mut request = |layout| {
        construction
            .as_deref_mut()
            .expect("original construction port is present")(layout)
    };
    let construction = if observed {
        Some(&mut request as &mut dyn FnMut(Layout) -> Result<(), E>)
    } else {
        None
    };
    Ok(if recursive(field.data_type()) {
        if let Some((policy, limits, verifier)) = profile {
            let stream = preflight_recursive_constant_stream_observed(
                &record.arrow_ipc,
                field,
                limits.recursive_stream,
                verifier,
                live_source,
                admit,
                construction,
                work,
            )?;
            PreparedReader::Recursive(stream.prepare_pool_borrowed_observed(
                field.clone(),
                value_type,
                live_source,
                policy,
                limits.recursive_reader,
                admit,
                work,
            )?)
        } else {
            let stream = crate::ipc_recursive_stream_v2::preflight_recursive_constant_stream_input_derived_observed(
                &record.arrow_ipc, field, live_source, admit, construction, work,
            )?;
            PreparedReader::Recursive(stream.prepare_pool_borrowed_input_derived_observed(
                field.clone(),
                value_type,
                live_source,
                admit,
                work,
            )?)
        }
    } else if let Some((policy, limits, verifier)) = profile {
        let stream = preflight_flat_constant_stream_observed(
            &record.arrow_ipc,
            field,
            limits.flat_stream,
            verifier,
            live_source,
            admit,
            construction,
            work,
        )?;
        PreparedReader::Flat(stream.prepare_pool_borrowed_observed(
            field.clone(),
            value_type,
            live_source,
            policy,
            limits.flat_reader,
            admit,
            work,
        )?)
    } else {
        let stream =
            crate::ipc_flat_stream_v2::preflight_flat_constant_stream_input_derived_observed(
                &record.arrow_ipc,
                field,
                live_source,
                admit,
                construction,
                work,
            )?;
        PreparedReader::Flat(stream.prepare_pool_borrowed_input_derived_observed(
            field.clone(),
            value_type,
            live_source,
            admit,
            work,
        )?)
    })
}

/// Prepared same-source Reader. It retains finite source custody through its
/// actual materialization, then transfers that custody to the output pool.
pub struct PreparedConstantRecord<'raw, 'source, 'control> {
    reader: PreparedReader<'raw, 'source, 'control>,
    source: ConstantSourceOwner,
}
impl PreparedConstantRecord<'_, '_, '_> {
    /// The original numeric future model is unchanged. It is not an invoice.
    pub fn reader_request_upper_bounds(&self) -> (usize, usize, usize) {
        self.reader.reader_requests()
    }
    pub fn materialize_in<H>(
        self,
        creation: &mut ValueSchemaCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, ProjectionFailure<Error, H>> {
        let mut admit = |_: &IpcReaderProgressFacts| Ok(());
        let result = match self.reader {
            PreparedReader::Flat(reader) => {
                reader.materialize_with_creator_observed(self.source, creation, &mut admit, work)
            }
            PreparedReader::Recursive(reader) => {
                reader.materialize_with_creator_observed(self.source, creation, &mut admit, work)
            }
        };
        result.map_err(reader_failure)
    }
}
fn reader_failure<H>(error: ProjectionFailure<FlatReaderError, H>) -> ProjectionFailure<Error, H> {
    match error {
        ProjectionFailure::Host(error) => ProjectionFailure::Host(error),
        ProjectionFailure::Codec(error) => ProjectionFailure::Codec(error.into()),
    }
}

/// Explicit source authors resolve both IDs before this borrowed entry. The
/// value namespace supplies meaning; the independent source namespace supplies
/// the exact original Field and metadata creation receipt. No table is rebuilt.
#[expect(
    clippy::too_many_arguments,
    reason = "The record boundary keeps original owners, source invoices, decode policy, verifier, creator, and progress ports explicit."
)]
pub fn prepare_constant_record_from_sources_with_creator_in<'raw, 'source, 'control, H>(
    record: &'raw wire::IpcConstantPool,
    value_type: &'source FunctionValueType,
    field: &'source Arc<Field>,
    source_storage: ValueTypeStorageLifetime,
    source_metadata: &MaterializedFieldNamespace,
    original_source_bytes: usize,
    live_source_bytes: usize,
    policy: ConstantPolicy,
    limits: ConstantDecodeProjectionLimits,
    verifier: &VerifierOptions,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantRecord<'raw, 'source, 'control>, ProjectionFailure<Error, H>> {
    prepare_constant_record_from_sources_profile_with_creator_in(
        record,
        value_type,
        field,
        source_storage,
        source_metadata,
        original_source_bytes,
        live_source_bytes,
        Some((policy, limits, verifier)),
        creation,
        work,
    )
}
/// Input-specific receiving profile sealed by the sole verified framing,
/// geometry and constant numerical authors. The real caller still owns every
/// construction request and cancellation; this entry mints no quota or grant.
pub fn prepare_constant_record_from_sources_input_derived_with_creator_in<
    'raw,
    'source,
    'control,
    H,
>(
    record: &'raw wire::IpcConstantPool,
    value_type: &'source FunctionValueType,
    field: &'source Arc<Field>,
    source_storage: ValueTypeStorageLifetime,
    source_metadata: &MaterializedFieldNamespace,
    original_source_bytes: usize,
    live_source_bytes: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantRecord<'raw, 'source, 'control>, ProjectionFailure<Error, H>> {
    prepare_constant_record_from_sources_profile_with_creator_in(
        record,
        value_type,
        field,
        source_storage,
        source_metadata,
        original_source_bytes,
        live_source_bytes,
        None,
        creation,
        work,
    )
}
fn prepare_constant_record_from_sources_profile_with_creator_in<'raw, 'source, 'control, H>(
    record: &'raw wire::IpcConstantPool,
    value_type: &'source FunctionValueType,
    field: &'source Arc<Field>,
    source_storage: ValueTypeStorageLifetime,
    source_metadata: &MaterializedFieldNamespace,
    original_source_bytes: usize,
    live_source_bytes: usize,
    profile: Option<(
        ConstantPolicy,
        ConstantDecodeProjectionLimits,
        &VerifierOptions,
    )>,
    creation: &mut ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantRecord<'raw, 'source, 'control>, ProjectionFailure<Error, H>> {
    checked_record_source_header(record, original_source_bytes, work)?;
    creation
        .with_error_mapping(ProjectionFailure::<FlatReaderError, H>::Host, |mapped| {
            let mut admit = |_: &IpcReaderProgressFacts| Ok(());
            let reader = prepare_reader_from_sources_profile_observed(
                record,
                value_type,
                field,
                live_source_bytes,
                profile,
                &mut admit,
                Some(&mut |layout| mapped.request_layout(layout)),
                work,
            )?;
            let source = ConstantSourceOwner::try_new_with_creator_in(
                field,
                source_metadata,
                original_source_bytes,
                source_storage,
                mapped,
                work,
            )
            .map_err(crate::ipc_flat_stream_v2::pool_resources::flatten_constant_failure)?;
            Ok(PreparedConstantRecord { reader, source })
        })
        .map_err(reader_failure)
}

pub(crate) fn checked_record_source_header(
    record: &wire::IpcConstantPool,
    original_source_bytes: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    work.step().map_err(Error::from)?;
    if record.compression != wire::IpcCompression::Uncompressed as i32 {
        return Err(Error::InvalidShape(
            "constant IPC compression is not supported",
        ));
    }
    super::required(
        record.value_type_id,
        "constant pool lacks value type id",
        work,
    )?;
    super::required(
        record.source_field_id,
        "constant pool lacks source field id",
        work,
    )?;
    if original_source_bytes < record.arrow_ipc.capacity() {
        return Err(Error::InvalidShape(
            "constant source invoice is below original IPC backing",
        ));
    }
    Ok(())
}
