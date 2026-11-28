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

//! Projection of one admitted v2 constant-pool record. Whole-package admission
//! owns the raw DTO/type-table allocations and namespace/reference closure.
//! These explicit component envelopes are not formal MEM allocation grants.

use crate::{
    ipc_flat_pool_v2::{
        FlatPoolWriteFacts, FlatPoolWriteLimits, PreparedFlatPoolWriter, prepare_flat_pool_write,
    },
    ipc_flat_stream_v2::{
        FlatPoolResourceError, FlatReaderError, FlatReaderProjectionLimits,
        FlatStreamProjectionLimits, preflight_flat_constant_stream,
    },
    ipc_recursive_pool_v2::{
        PreparedRecursivePoolWriter, RecursivePoolWriteLimits, prepare_recursive_pool_write,
    },
    ipc_recursive_stream_v2::{
        RecursiveReaderProjectionLimits, RecursiveStreamProjectionLimits,
        preflight_recursive_constant_stream,
    },
    physical_type_v2::{DecodedTypeTable, TypeCodecError},
};
use arrow::datatypes::{DataType, Field};
use novarocks_arrow_ipc_frame::VerifierOptions;
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_physical_plan::ConstantPoolId;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};
use std::{fmt, sync::Arc};

/// Each carrier uses exactly its named profile; neither failure nor a refusal
/// selects another profile. The caller supplies every envelope explicitly.
#[derive(Clone, Copy, Debug)]
pub struct ConstantDecodeProjectionLimits {
    pub flat_stream: FlatStreamProjectionLimits,
    pub flat_reader: FlatReaderProjectionLimits,
    pub recursive_stream: RecursiveStreamProjectionLimits,
    pub recursive_reader: RecursiveReaderProjectionLimits,
}
#[derive(Clone, Copy, Debug)]
pub struct ConstantWriteProjectionLimits {
    pub flat: FlatPoolWriteLimits,
    pub recursive: RecursivePoolWriteLimits,
}
#[derive(Debug)]
pub enum PhysicalConstantCodecError {
    InvalidShape(&'static str),
    JsonValue(novarocks_functions::json_value::JsonValueError),
    Control(CompileControlError),
    Type(TypeCodecError),
    Reader(FlatReaderError),
    Reference(novarocks_physical_plan::ConstantReferenceError),
}
impl fmt::Display for PhysicalConstantCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidShape(message) => f.write_str(message),
            Self::JsonValue(error) => write!(f, "invalid JSON constant: {error}"),
            Self::Control(error) => error.fmt(f),
            Self::Type(error) => error.fmt(f),
            Self::Reader(error) => error.fmt(f),
            Self::Reference(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for PhysicalConstantCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::JsonValue(source) => Some(source),
            Self::Control(source) => Some(source),
            Self::Type(source) => Some(source),
            Self::Reader(source) => Some(source),
            Self::Reference(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for PhysicalConstantCodecError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<TypeCodecError> for PhysicalConstantCodecError {
    fn from(error: TypeCodecError) -> Self {
        match error {
            TypeCodecError::Control(error) => Self::Control(error),
            error => Self::Type(error),
        }
    }
}
impl From<novarocks_type_contract::ValueTypeError> for PhysicalConstantCodecError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Type(TypeCodecError::ValueType(error))
    }
}
impl From<FlatReaderError> for PhysicalConstantCodecError {
    fn from(error: FlatReaderError) -> Self {
        match error {
            FlatReaderError::Projection(FlatPoolResourceError::Control(error)) => {
                Self::Control(error)
            }
            error => Self::Reader(error),
        }
    }
}
impl From<novarocks_constant_contract::ConstantError> for PhysicalConstantCodecError {
    fn from(error: novarocks_constant_contract::ConstantError) -> Self {
        Self::from(FlatReaderError::from(error))
    }
}
impl From<FlatPoolResourceError> for PhysicalConstantCodecError {
    fn from(error: FlatPoolResourceError) -> Self {
        Self::from(FlatReaderError::from(error))
    }
}
impl From<novarocks_type_contract::CarrierParameterError> for PhysicalConstantCodecError {
    fn from(error: novarocks_type_contract::CarrierParameterError) -> Self {
        Self::from(FlatReaderError::from(error))
    }
}
impl<H> From<FlatReaderError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: FlatReaderError) -> Self {
        Self::Codec(PhysicalConstantCodecError::from(error))
    }
}
impl<H> From<FlatPoolResourceError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: FlatPoolResourceError) -> Self {
        Self::Codec(PhysicalConstantCodecError::from(error))
    }
}
impl<H> From<novarocks_constant_contract::ConstantError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: novarocks_constant_contract::ConstantError) -> Self {
        Self::Codec(PhysicalConstantCodecError::from(error))
    }
}
impl<H> From<novarocks_type_contract::CarrierParameterError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: novarocks_type_contract::CarrierParameterError) -> Self {
        Self::Codec(PhysicalConstantCodecError::from(error))
    }
}
impl<H> From<novarocks_type_contract::ValueTypeError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Codec(PhysicalConstantCodecError::from(error))
    }
}
impl<H> From<novarocks_physical_plan::ConstantReferenceError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: novarocks_physical_plan::ConstantReferenceError) -> Self {
        Self::Codec(PhysicalConstantCodecError::from(error))
    }
}
fn recursive(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Struct(_) | DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _)
    )
}
fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, PhysicalConstantCodecError>,
) -> Result<T, PhysicalConstantCodecError> {
    if matches!(&result, Err(PhysicalConstantCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn required(
    value: Option<u32>,
    message: &'static str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u32, PhysicalConstantCodecError> {
    work.step()?;
    value.ok_or(PhysicalConstantCodecError::InvalidShape(message))
}

fn record_sources<'t>(
    record: &wire::IpcConstantPool,
    types: &'t DecodedTypeTable,
    source_retained_bytes: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'t FunctionValueType, &'t Arc<Field>), PhysicalConstantCodecError> {
    record_sources_captured(
        record,
        types,
        source_retained_bytes,
        &mut |_, _, _| Ok(()),
        work,
    )
}

/// Retain the actual two source loans through the captured consumer's known
/// resource gate, before the matched Field lookup's completed observation.
fn record_sources_captured<'t, 'control>(
    record: &wire::IpcConstantPool,
    types: &'t DecodedTypeTable,
    source_retained_bytes: usize,
    capture: &mut impl FnMut(
        &'t FunctionValueType,
        &'t Arc<Field>,
        &mut CompileCheckpoints<'control>,
    ) -> Result<(), PhysicalConstantCodecError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<(&'t FunctionValueType, &'t Arc<Field>), PhysicalConstantCodecError> {
    record_sources_captured_observed(
        record,
        types,
        Some(source_retained_bytes),
        &mut |value, source, work| capture(value, source.field(), work),
        work,
    )
}

fn record_sources_captured_observed<'t, 'control, E: From<PhysicalConstantCodecError>>(
    record: &wire::IpcConstantPool,
    types: &'t DecodedTypeTable,
    source_retained_bytes: Option<usize>,
    capture: &mut impl FnMut(
        &'t FunctionValueType,
        crate::physical_type_v2::SourceFieldBorrow<'t>,
        &mut CompileCheckpoints<'control>,
    ) -> Result<(), E>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<(&'t FunctionValueType, &'t Arc<Field>), E> {
    work.step().map_err(PhysicalConstantCodecError::from)?;
    if record.compression != wire::IpcCompression::Uncompressed as i32 {
        return Err((PhysicalConstantCodecError::InvalidShape(
            "constant record requires uncompressed IPC",
        ))
        .into());
    }
    let type_id = required(
        record.value_type_id,
        "constant record is missing its value type ID",
        work,
    )?;
    let field_id = required(
        record.source_field_id,
        "constant record is missing its Field ID",
        work,
    )?;
    let value_type = types.value_type(type_id);
    work.step().map_err(PhysicalConstantCodecError::from)?;
    let value_type = value_type.ok_or(PhysicalConstantCodecError::InvalidShape(
        "constant record references an unknown value type",
    ))?;
    let field = types.source_field_borrow(field_id);
    if source_retained_bytes.is_none_or(|source| source >= record.arrow_ipc.capacity())
        && let Some(field) = field
    {
        capture(value_type, field, work)?;
    }
    work.step().map_err(PhysicalConstantCodecError::from)?;
    let field = field.ok_or(PhysicalConstantCodecError::InvalidShape(
        "constant record references an unknown Field",
    ))?;
    let source_covers_ipc =
        source_retained_bytes.is_none_or(|source| source >= record.arrow_ipc.capacity());
    work.step().map_err(PhysicalConstantCodecError::from)?;
    if !source_covers_ipc {
        return Err((PhysicalConstantCodecError::InvalidShape(
            "constant record source invoice omits original IPC backing",
        ))
        .into());
    }
    work.flush().map_err(PhysicalConstantCodecError::from)?;
    Ok((value_type, field.field()))
}

/// The invoice covers every still-live DTO/input and type-table owner, including
/// original Field/FVT names, metadata, spare capacity and removed hash buckets.
/// Never substitute IPC visible length for that trusted source invoice.
/// The pool ID is a non-optional proto scalar; zero and MAX are valid IDs.
/// Duplicate IDs, unused definitions and ordinal closure belong to the package.
pub fn decode_constant_record(
    record: &wire::IpcConstantPool,
    types: &DecodedTypeTable,
    source_retained_bytes: usize,
    policy: ConstantPolicy,
    limits: ConstantDecodeProjectionLimits,
    verifier: &VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<(ConstantPoolId, ConstantPool), PhysicalConstantCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let (value_type, field) = record_sources(record, types, source_retained_bytes, &mut work)?;
        let pool = if recursive(field.data_type()) {
            let stream = preflight_recursive_constant_stream(
                &record.arrow_ipc,
                field,
                limits.recursive_stream,
                verifier,
                work.control(),
            )?;
            work.flush()?;
            stream.materialize_pool_borrowed(
                Arc::clone(field),
                value_type,
                source_retained_bytes,
                policy,
                limits.recursive_reader,
                work.control(),
            )?
        } else {
            let stream = preflight_flat_constant_stream(
                &record.arrow_ipc,
                field,
                limits.flat_stream,
                verifier,
                work.control(),
            )?;
            work.flush()?;
            stream.materialize_pool_borrowed(
                Arc::clone(field),
                value_type,
                source_retained_bytes,
                policy,
                limits.flat_reader,
                work.control(),
            )?
        };
        work.step()?;
        Ok((ConstantPoolId::new(record.id), pool))
    })();
    finish(work, result)
}

/// Encode the checked pool's original Field/FVT/backing and all ordinals. The
/// whole-package author must bind the explicit IDs to those exact type-table
/// facts and validate namespace/reference closure; this record owns no table.
/// The invoice includes the whole original pool and all other retained owners.
pub fn encode_constant_record(
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    pool: &ConstantPool,
    source_retained_bytes: usize,
    limits: ConstantWriteProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<wire::IpcConstantPool, PhysicalConstantCodecError> {
    prepare_constant_record_write(
        id,
        value_type_id,
        field_id,
        pool,
        source_retained_bytes,
        limits,
        control,
    )?
    .emit()
}

use crate::host_projection_v2::{AdmissionRefusal, ProjectionFailure};
type HostError<H> = ProjectionFailure<PhysicalConstantCodecError, H>;
type HostTypeError<H> = ProjectionFailure<TypeCodecError, H>;
type HostWriterAdmit<'a, H> = dyn FnMut(&FlatPoolWriteFacts) -> Result<(), HostTypeError<H>> + 'a;
fn writer_refusal<H>(error: AdmissionRefusal<H>) -> HostTypeError<H> {
    match error {
        AdmissionRefusal::Control(cause) => cause.into(),
        AdmissionRefusal::Host(error) => ProjectionFailure::Host(error),
    }
}
impl<H> From<CompileControlError> for HostError<H> {
    fn from(cause: CompileControlError) -> Self {
        Self::Codec(cause.into())
    }
}
impl<H> From<TypeCodecError> for HostError<H> {
    fn from(error: TypeCodecError) -> Self {
        Self::Codec(error.into())
    }
}
impl<H> From<HostTypeError<H>> for HostError<H> {
    fn from(error: HostTypeError<H>) -> Self {
        match error {
            ProjectionFailure::Codec(error) => Self::Codec(error.into()),
            ProjectionFailure::Host(error) => Self::Host(error),
        }
    }
}

enum PreparedWriter<'pool, 'control> {
    Flat(PreparedFlatPoolWriter<'pool, 'control>),
    Recursive(PreparedRecursivePoolWriter<'pool, 'control>),
    PureFlat(crate::ipc_flat_pool_v2::PreparedPureFlatPoolWriter<'pool, 'control>),
    PureRecursive(crate::ipc_recursive_pool_v2::PreparedPureRecursivePoolWriter<'pool, 'control>),
}
/// One original checked pool and its sealed writer model. Table-ID binding
/// still belongs to the sole whole-package type author, as for record encode.
pub struct PreparedConstantRecordWrite<'pool, 'control> {
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    writer: PreparedWriter<'pool, 'control>,
    control: &'control dyn PureCompileControl,
}
impl PreparedConstantRecordWrite<'_, '_> {
    /// The common full request/work envelope. Recursive geometry is included
    /// by its original writer author; it is not re-counted by this wrapper.
    pub fn facts(&self) -> &FlatPoolWriteFacts {
        match &self.writer {
            PreparedWriter::Flat(writer) => writer.facts(),
            PreparedWriter::Recursive(writer) => &writer.facts().flat,
            PreparedWriter::PureFlat(_) | PreparedWriter::PureRecursive(_) => {
                unreachable!("pure writer has no receiving facts")
            }
        }
    }
    pub fn emit(self) -> Result<wire::IpcConstantPool, PhysicalConstantCodecError> {
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        let result = self
            .emit_core::<std::convert::Infallible>(None, &mut work)
            .map_err(ProjectionFailure::without_host);
        finish(work, result)
    }
    pub fn emit_in(
        self,
        admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::IpcConstantPool, PhysicalConstantCodecError> {
        self.emit_with_host_in(
            &mut |facts| {
                admit(facts).map_err(AdmissionRefusal::<std::convert::Infallible>::Control)
            },
            work,
        )
        .map_err(ProjectionFailure::without_host)
    }

    pub fn emit_with_host_in<H>(
        self,
        admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::IpcConstantPool, HostError<H>> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(PhysicalConstantCodecError::InvalidShape(
                "constant record caller work has a different original control",
            )
            .into());
        }
        admit(self.facts()).map_err(writer_refusal)?;
        self.emit_core(
            Some(&mut |facts| admit(facts).map_err(writer_refusal)),
            work,
        )
    }
    fn emit_core<H>(
        self,
        mut admit: Option<&mut HostWriterAdmit<'_, H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::IpcConstantPool, HostError<H>> {
        (|| {
            work.flush()?;
            let arrow_ipc = match self.writer {
                PreparedWriter::Flat(writer) => match &mut admit {
                    Some(parent) => writer.emit_with_host_in(*parent, work)?,
                    None => writer.emit(work.control())?,
                },
                PreparedWriter::Recursive(writer) => match &mut admit {
                    Some(parent) => {
                        writer.emit_with_host_in(&mut |facts| parent(&facts.flat), work)?
                    }
                    None => writer.emit(work.control())?,
                },
                PreparedWriter::PureFlat(writer) => writer.emit_in(work)?,
                PreparedWriter::PureRecursive(writer) => writer.emit_in(work)?,
            };
            work.step()?;
            Ok(wire::IpcConstantPool {
                id: self.id.get(),
                value_type_id: Some(self.value_type_id),
                source_field_id: Some(self.field_id),
                compression: wire::IpcCompression::Uncompressed as i32,
                arrow_ipc,
            })
        })()
    }
}

/// A pure FE preparation owns no receiving invoice or memory grant. Its writer
/// is the same IPC body, with actual borrowed source custody and original control.
pub struct PreparedPureConstantRecordWrite<'pool, 'control> {
    prepared: PreparedConstantRecordWrite<'pool, 'control>,
}
impl PreparedPureConstantRecordWrite<'_, '_> {
    pub fn emit_in(
        self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<wire::IpcConstantPool, PhysicalConstantCodecError> {
        if !std::ptr::addr_eq(self.prepared.control, work.control()) {
            return Err(PhysicalConstantCodecError::InvalidShape(
                "pure constant record uses a different original control",
            ));
        }
        self.prepared
            .emit_core::<std::convert::Infallible>(None, work)
            .map_err(ProjectionFailure::without_host)
    }
}
pub fn prepare_constant_record_write_pure_in<'pool, 'control>(
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    pool: &'pool ConstantPool,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedPureConstantRecordWrite<'pool, 'control>, PhysicalConstantCodecError> {
    let writer = if recursive(pool.field().data_type()) {
        PreparedWriter::PureRecursive(
            crate::ipc_recursive_pool_v2::prepare_recursive_pool_write_pure_in(pool, work)?,
        )
    } else {
        PreparedWriter::PureFlat(crate::ipc_flat_pool_v2::prepare_flat_pool_write_pure_in(
            pool, work,
        )?)
    };
    Ok(PreparedPureConstantRecordWrite {
        prepared: PreparedConstantRecordWrite {
            id,
            value_type_id,
            field_id,
            writer,
            control: work.control(),
        },
    })
}

/// Admit the exact original writer snapshot once. The caller may accumulate
/// receiving/output requests before consuming it; no output stream exists yet.
pub fn prepare_constant_record_write<'pool, 'control>(
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    pool: &'pool ConstantPool,
    source_retained_bytes: usize,
    limits: ConstantWriteProjectionLimits,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedConstantRecordWrite<'pool, 'control>, PhysicalConstantCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = prepare_record_core::<std::convert::Infallible>(
        id,
        value_type_id,
        field_id,
        pool,
        source_retained_bytes,
        limits,
        None,
        &mut work,
    );
    finish(work, result.map_err(ProjectionFailure::without_host))
}

pub fn prepare_constant_record_write_in<'pool, 'control>(
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    pool: &'pool ConstantPool,
    source_retained_bytes: usize,
    limits: ConstantWriteProjectionLimits,
    admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantRecordWrite<'pool, 'control>, PhysicalConstantCodecError> {
    prepare_constant_record_write_with_host_in(
        id,
        value_type_id,
        field_id,
        pool,
        source_retained_bytes,
        limits,
        &mut |facts| admit(facts).map_err(AdmissionRefusal::<std::convert::Infallible>::Control),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub fn prepare_constant_record_write_with_host_in<'pool, 'control, H>(
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    pool: &'pool ConstantPool,
    source_retained_bytes: usize,
    limits: ConstantWriteProjectionLimits,
    admit: &mut dyn FnMut(&FlatPoolWriteFacts) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantRecordWrite<'pool, 'control>, HostError<H>> {
    prepare_record_core(
        id,
        value_type_id,
        field_id,
        pool,
        source_retained_bytes,
        limits,
        Some(&mut |facts| admit(facts).map_err(writer_refusal)),
        work,
    )
}

fn prepare_record_core<'pool, 'control, H>(
    id: ConstantPoolId,
    value_type_id: u32,
    field_id: u32,
    pool: &'pool ConstantPool,
    source_retained_bytes: usize,
    limits: ConstantWriteProjectionLimits,
    mut admit: Option<&mut HostWriterAdmit<'_, H>>,
    work: &mut CompileCheckpoints<'control>,
) -> Result<PreparedConstantRecordWrite<'pool, 'control>, HostError<H>> {
    (|| {
        if admit.is_none() {
            work.flush()?;
        }
        let writer = if recursive(pool.field().data_type()) {
            PreparedWriter::Recursive(match &mut admit {
                Some(parent) => {
                    crate::ipc_recursive_pool_v2::prepare_recursive_pool_write_with_host_in(
                        pool,
                        source_retained_bytes,
                        limits.recursive,
                        &mut |facts| parent(&facts.flat),
                        work,
                    )?
                }
                None => prepare_recursive_pool_write(
                    pool,
                    source_retained_bytes,
                    limits.recursive,
                    work.control(),
                )?,
            })
        } else {
            PreparedWriter::Flat(match &mut admit {
                Some(parent) => crate::ipc_flat_pool_v2::prepare_flat_pool_write_with_host_in(
                    pool,
                    source_retained_bytes,
                    limits.flat,
                    *parent,
                    work,
                )?,
                None => prepare_flat_pool_write(
                    pool,
                    source_retained_bytes,
                    limits.flat,
                    work.control(),
                )?,
            })
        };
        Ok(PreparedConstantRecordWrite {
            id,
            value_type_id,
            field_id,
            writer,
            control: work.control(),
        })
    })()
}

mod binding_resources;
mod namespace;
mod write_namespace;
#[cfg(test)]
pub(crate) use namespace::prepare_constant_namespace_in;
pub use namespace::{
    ConstantNamespaceProjectionLimits, ConstantNamespaceResourceFacts, PreparedConstantNamespace,
    decode_constant_namespace, prepare_constant_namespace,
    prepare_constant_namespace_input_derived_in,
    prepare_constant_namespace_input_derived_with_creator_in,
    prepare_constant_namespace_input_derived_with_factory_in,
    prepare_constant_namespace_with_creator_in, prepare_constant_namespace_with_factory_in,
};

#[cfg(test)]
pub(crate) use write_namespace::prepare_constant_namespace_write_in;

pub use crate::ipc_flat_pool_v2::ConstantWriteRequest;
pub use write_namespace::{
    ConstantNamespaceWriteFacts, ConstantRecordTypeIds, PreparedConstantNamespaceWrite,
    encode_constant_namespace, encode_constant_namespace_input_derived_in,
    prepare_constant_namespace_write,
};

#[cfg(test)]
mod input_derived_tests;
#[cfg(test)]
mod tests;

mod borrowed_reader;
mod record_scopes;
pub use borrowed_reader::{
    PreparedConstantRecord, prepare_constant_record_from_sources_input_derived_with_creator_in,
    prepare_constant_record_from_sources_with_creator_in,
};
pub use record_scopes::{
    ConstantRecordConstructionFacts, ScopedPreparedConstantRecord,
    prepare_constant_record_from_sources_input_derived_with_factory_in,
};

impl<H> From<novarocks_constant_contract::ConstantTableError>
    for crate::host_projection_v2::ProjectionFailure<PhysicalConstantCodecError, H>
{
    fn from(error: novarocks_constant_contract::ConstantTableError) -> Self {
        Self::from(novarocks_physical_plan::ConstantReferenceError::from(error))
    }
}
