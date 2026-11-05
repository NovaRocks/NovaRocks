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

//! Exact one-field standard Arrow IPC schema messages for constant pools.
//!
//! This schema author preserves the actual Field; it does not fabricate it
//! from a value type. It does not encode value buffers or admit a complete pool
//! stream. The explicit envelope bounds primary FlatBuffer backing before its
//! allocation; auxiliary builder/sorting storage is structurally bounded, not
//! a formal MEM grant or a total-heap/RSS model.

use crate::host_projection_v2::ProjectionFailure;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;
use crate::physical_type_v2::{TypeCodecError, validate_field, validate_type};
use arrow::datatypes::{DataType, Field};
use flatbuffers::FlatBufferBuilder;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueTypeVisit,
    validate_arrow_layout_structure_observed,
};

mod emit;
mod verify;

// A supported Field chain has at most 64 value-type levels. The standard
// Message -> Schema -> Field framing adds three verifier table levels;
// dictionary/type payloads fit within the same source-derived cover.
const MAX_SCHEMA_VERIFIER_DEPTH: usize = novarocks_type_contract::MAX_VALUE_TYPE_DEPTH + 3;

/// Caller-resolved admission, with no implicit application policy.
#[derive(Clone, Copy, Debug)]
pub struct IpcSchemaProjectionLimits {
    pub max_field_occurrences: usize,
    pub max_type_occurrences: usize,
    pub max_string_bytes: usize,
    pub max_flatbuffer_bytes: usize,
}

pub(crate) mod owner_admission;
mod writer_resources;
#[cfg(test)]
pub(crate) use owner_admission::SchemaAdmit;
use owner_admission::{Admission, Policy};
pub(crate) use owner_admission::{HostSchemaAdmit, SchemaWriterRequestFacts};
pub(crate) use writer_resources::{
    preflight_schema_writer_resources, schema_writer_prefix_resources,
    schema_writer_prefix_resources_with_host_in,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SchemaPreflight {
    pub backing: usize,
    pub tables: usize,
    pub metadata_entries: usize,
    pub string_bytes: usize,
    pub field_occurrences: usize,
    pub type_occurrences: usize,
    pub metadata_nonempty_fields: usize,
    pub child_offset_items: usize,
    pub child_offset_requests: usize,
    pub union_id_items: usize,
    pub union_id_requests: usize,
}

#[derive(Default)]
struct Counts {
    fields: usize,
    types: usize,
    metadata_entries: usize,
    string_count: usize,
    string_bytes: usize,
    metadata_nonempty_fields: usize,
    child_offset_items: usize,
    child_offset_requests: usize,
    union_id_items: usize,
    union_id_requests: usize,
}

fn checked_add(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_add(b)
        .ok_or(TypeCodecError::InvalidShape("IPC schema extent overflow"))
}
fn checked_mul(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_mul(b)
        .ok_or(TypeCodecError::InvalidShape("IPC schema extent overflow"))
}

impl Counts {
    fn string(
        &mut self,
        length: usize,
        limits: Option<IpcSchemaProjectionLimits>,
        policy: Policy,
    ) -> Result<(), TypeCodecError> {
        let checked_add = |a, b| policy.numeric(checked_add(a, b));
        self.string_count = checked_add(self.string_count, 1)?;
        self.string_bytes = checked_add(self.string_bytes, length)?;
        if let Some(limits) = limits {
            policy.cap(
                self.string_bytes,
                limits.max_string_bytes,
                "IPC schema string envelope exceeded",
            )?;
        }
        Ok(())
    }
    fn field<H>(
        &mut self,
        field: &Field,
        limits: Option<IpcSchemaProjectionLimits>,
        admission: &mut Option<(
            writer_resources::SchemaWriterPrefixFacts,
            &mut Admission<'_, '_, H>,
        )>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), HostError<H>> {
        let policy = Policy(admission.is_some());
        let checked_add = |a, b| policy.numeric(checked_add(a, b));
        if let Some((_, admission)) = admission.as_ref() {
            source_floor(field, admission.source)?;
        }
        self.fields = checked_add(self.fields, 1)?;
        if let Some(limits) = limits {
            policy.cap(
                self.fields,
                limits.max_field_occurrences,
                "IPC schema field envelope exceeded",
            )?;
        }
        self.string(field.name().len(), limits, policy)?;
        if !field.metadata().is_empty() {
            self.metadata_nonempty_fields = checked_add(self.metadata_nonempty_fields, 1)?;
        }
        self.publish(admission)?;
        for (key, value) in field.metadata() {
            if admission.is_none() {
                work.step()?;
            }
            self.metadata_entries = checked_add(self.metadata_entries, 1)?;
            self.string(key.len(), limits, policy)?;
            self.string(value.len(), limits, policy)?;
            self.publish(admission)?;
            if admission.is_some() {
                work.step()?;
            }
        }
        Ok(())
    }
    fn type_header(
        &mut self,
        ty: &DataType,
        limits: Option<IpcSchemaProjectionLimits>,
        policy: Policy,
    ) -> Result<usize, TypeCodecError> {
        let checked_add = |a, b| policy.numeric(checked_add(a, b));
        self.types = checked_add(self.types, 1)?;
        if let Some(limits) = limits {
            policy.cap(
                self.types,
                limits.max_type_occurrences,
                "IPC schema type envelope exceeded",
            )?;
        }
        // These counters describe allocations of the existing emit_type
        // branches. They neither add carriers nor define another schema.
        let geometry = novarocks_functions::arrow_ipc_construction::type_header_geometry(ty);
        let children = geometry.children;
        if geometry.union_ids != 0 {
            self.union_id_items = checked_add(self.union_id_items, geometry.union_ids)?;
            self.union_id_requests = checked_add(self.union_id_requests, 1)?;
        }
        if children != 0 {
            self.child_offset_items = checked_add(self.child_offset_items, children)?;
            self.child_offset_requests = checked_add(self.child_offset_requests, 1)?;
        }
        if let Some(bytes) = geometry.timezone_bytes {
            self.string(bytes, limits, policy)?;
        }
        Ok(children)
    }
    fn schema(&self) -> Result<SchemaPreflight, TypeCodecError> {
        Ok(SchemaPreflight {
            backing: self.backing_upper()?,
            tables: checked_add(
                checked_add(self.fields, self.types)?,
                checked_add(self.metadata_entries, 2)?,
            )?,
            metadata_entries: self.metadata_entries,
            string_bytes: self.string_bytes,
            field_occurrences: self.fields,
            type_occurrences: self.types,
            metadata_nonempty_fields: self.metadata_nonempty_fields,
            child_offset_items: self.child_offset_items,
            child_offset_requests: self.child_offset_requests,
            union_id_items: self.union_id_items,
            union_id_requests: self.union_id_requests,
        })
    }
    fn publish<H>(
        &self,
        admission: &mut Option<(
            writer_resources::SchemaWriterPrefixFacts,
            &mut Admission<'_, '_, H>,
        )>,
    ) -> Result<(), HostError<H>> {
        if let Some((prefix, admission)) = admission.as_mut() {
            let schema = admission.policy().numeric(self.schema())?;
            writer_resources::counts_prefix(schema, *prefix, admission)?;
        }
        Ok(())
    }
    fn backing_upper(&self) -> Result<usize, TypeCodecError> {
        novarocks_functions::arrow_ipc_construction::schema_backing_upper_bound(
            self.fields,
            self.types,
            self.metadata_entries,
            self.string_count,
            self.string_bytes,
        )
        .map_err(TypeCodecError::from)
    }
}

fn control_cause(error: TypeCodecError) -> novarocks_type_contract::CompileControlError {
    match error {
        TypeCodecError::Control(cause) => cause,
        _ => novarocks_type_contract::CompileControlError::ResourceExhausted,
    }
}
fn control_cause_host<H>(error: HostError<H>) -> HostError<H> {
    match error {
        ProjectionFailure::Codec(error) => control_cause(error).into(),
        ProjectionFailure::Host(error) => ProjectionFailure::Host(error),
    }
}

fn source_floor(field: &Field, source: usize) -> Result<(), TypeCodecError> {
    let policy = Policy(true);
    let occupied = policy.numeric(checked_mul(
        field.metadata().len(),
        std::mem::size_of::<(String, String)>(),
    ))?;
    let floor = policy.numeric(checked_add(
        std::mem::size_of::<Field>(),
        checked_add(field.name().len(), occupied)?,
    ))?;
    if source < floor {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema source invoice is understated",
        ));
    }
    Ok(())
}
/// O(1) captured root headers, before the original source-walker callbacks.
pub(crate) fn initial_writer_request_facts(
    field: &Field,
    source: usize,
    limits: IpcSchemaProjectionLimits,
) -> Result<SchemaWriterRequestFacts, TypeCodecError> {
    source_floor(field, source)?;
    let policy = Policy(true);
    policy.cap(
        1,
        limits.max_field_occurrences,
        "IPC schema field envelope exceeded",
    )?;
    policy.cap(
        1,
        limits.max_type_occurrences,
        "IPC schema type envelope exceeded",
    )?;
    policy.cap(
        field.name().len(),
        limits.max_string_bytes,
        "IPC schema string envelope exceeded",
    )?;
    let prefix = policy.numeric(writer_resources::prefix_initial(field, source, 1))?;
    let mut initial = Counts {
        fields: 1,
        metadata_entries: field.metadata().len(),
        metadata_nonempty_fields: usize::from(!field.metadata().is_empty()),
        string_count: 1,
        string_bytes: field.name().len(),
        ..Counts::default()
    };
    let children = initial.type_header(field.data_type(), Some(limits), policy)?;
    initial.fields = policy.numeric(checked_add(initial.fields, children))?;
    policy.cap(
        initial.fields,
        limits.max_field_occurrences,
        "IPC schema field envelope exceeded",
    )?;
    let resource = policy.numeric(writer_resources::allocation_facts(
        initial.schema()?,
        prefix,
        None,
    ))?;
    Ok(SchemaWriterRequestFacts {
        request_bytes: resource.request_bytes,
        request_count: resource.request_count,
        work_upper_bound: resource.work_upper_bound.max(prefix.work_upper_bound),
    })
}

fn preflight_source(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Counts, TypeCodecError> {
    preflight_source_core::<std::convert::Infallible>(field, limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn preflight_source_core<H>(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    admission: Option<(
        writer_resources::SchemaWriterPrefixFacts,
        &mut Admission<'_, '_, H>,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Counts, HostError<H>> {
    preflight_source_policy_core(field, Some(limits), admission, work)
}
fn preflight_source_policy_core<H>(
    field: &Field,
    limits: Option<IpcSchemaProjectionLimits>,
    mut admission: Option<(
        writer_resources::SchemaWriterPrefixFacts,
        &mut Admission<'_, '_, H>,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Counts, HostError<H>> {
    let policy = Policy(admission.is_some());
    validate_field(field, work)?;
    if let Some((prefix, admission)) = admission.as_mut().filter(|(_, a)| a.reader) {
        let mut headers = Counts {
            fields: 1,
            metadata_entries: field.metadata().len(),
            metadata_nonempty_fields: usize::from(!field.metadata().is_empty()),
            string_count: 1,
            string_bytes: field.name().len(),
            ..Counts::default()
        };
        let mut reader_prefix = *prefix;
        let current = std::cell::RefCell::new(&mut **admission);
        let mut scratch_gate = |layout: std::alloc::Layout| {
            let mut admission = current.borrow_mut();
            let mut facts = admission.facts;
            let scratch_work = layout
                .size()
                .checked_mul(4)
                .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            facts.work_upper_bound = facts.work_upper_bound.max(scratch_work);
            admission.update(facts).map_err(control_cause_host)
        };
        let mut capture = |visit: ValueTypeVisit<'_>| {
            if let ValueTypeVisit::TypeNode(_) = visit {
                headers.types = headers
                    .types
                    .checked_add(1)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                reader_prefix = reader_prefix.with_reader_walker_node_bound(headers.types);
            }
            if let ValueTypeVisit::Field(field) = visit {
                headers.fields = headers
                    .fields
                    .checked_add(1)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                headers.metadata_entries = headers
                    .metadata_entries
                    .checked_add(field.metadata().len())
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                headers.string_count = headers
                    .string_count
                    .checked_add(1)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                headers.string_bytes = headers
                    .string_bytes
                    .checked_add(field.name().len())
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
            }
            let facts = Policy(true)
                .numeric(headers.schema())
                .and_then(|schema| {
                    Policy(true).numeric(writer_resources::reader_facts(schema, reader_prefix))
                })
                .map_err(control_cause)?;
            current
                .borrow_mut()
                .update(facts)
                .map_err(control_cause_host)
        };
        crate::physical_type_v2::validate_type_with_scratch_host_observed(
            field.data_type(),
            &mut scratch_gate,
            &mut capture,
            work,
        )?;
        // This same source's complete scratch count now pre-admits every
        // allocation of the following structural Vec walk.
        *prefix = reader_prefix;
    } else {
        validate_type(field.data_type(), work)?;
    }
    let mut counts = Counts::default();
    counts.field(field, limits, &mut admission, work)?;
    validate_arrow_layout_structure_observed(
        field.data_type(),
        |visit| -> Result<(), HostError<H>> {
            if admission.is_none() {
                work.step()?;
            }
            match visit {
                ValueTypeVisit::TypeNode(ty) => {
                    counts.type_header(ty, limits, policy)?;
                    match ty {
                        DataType::Dictionary(_, value)
                            if matches!(value.as_ref(), DataType::Dictionary(_, _)) =>
                        {
                            return Err((TypeCodecError::InvalidShape(
                                "direct nested dictionary has no inner IPC Field identity",
                            ))
                            .into());
                        }
                        _ => {}
                    }
                }
                ValueTypeVisit::Field(field) => {
                    counts.field(field, limits, &mut admission, work)?
                }
                ValueTypeVisit::ChildEdge(_) => {}
            }
            counts.publish(&mut admission)?;
            if admission.is_some() {
                work.step()?;
            }
            Ok(())
        },
    )?;
    Ok(counts)
}

pub(crate) fn preflight_writer(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaPreflight, TypeCodecError> {
    preflight_writer_core::<std::convert::Infallible>(field, limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

fn preflight_writer_core<H>(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    admission: Option<(
        writer_resources::SchemaWriterPrefixFacts,
        &mut Admission<'_, '_, H>,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaPreflight, HostError<H>> {
    preflight_writer_policy_core(field, Some(limits), admission, work)
}
pub(crate) fn preflight_writer_pure_in(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaPreflight, TypeCodecError> {
    preflight_writer_policy_core::<std::convert::Infallible>(field, None, None, work)
        .map_err(ProjectionFailure::without_host)
}
fn preflight_writer_policy_core<H>(
    field: &Field,
    limits: Option<IpcSchemaProjectionLimits>,
    admission: Option<(
        writer_resources::SchemaWriterPrefixFacts,
        &mut Admission<'_, '_, H>,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaPreflight, HostError<H>> {
    let policy = Policy(admission.is_some());
    let counts = preflight_source_policy_core(field, limits, admission, work)?;
    let schema = policy.numeric(counts.schema())?;
    if let Some(limits) = limits {
        policy.cap(
            schema.backing,
            limits.max_flatbuffer_bytes,
            "IPC schema FlatBuffer envelope exceeded",
        )?;
    }
    if schema.backing >= (1usize << 31) {
        return Err((if policy.0 {
            novarocks_type_contract::CompileControlError::ResourceExhausted.into()
        } else {
            TypeCodecError::InvalidShape("IPC schema FlatBuffer envelope exceeded")
        })
        .into());
    }
    Ok(schema)
}

/// Emits a V5 schema message, without stream framing or array-buffer encoding.
/// Success and ordinary errors observe the completed tail; control refusals
/// remain the original typed cause and never replay a later callback.
pub fn encode_single_field_schema(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_schema_core::<std::convert::Infallible>(field, limits, None, &mut work)
        .map_err(ProjectionFailure::without_host);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn encode_schema_core<H>(
    field: &Field,
    limits: IpcSchemaProjectionLimits,
    admission: Option<(
        writer_resources::SchemaWriterPrefixFacts,
        &mut Admission<'_, '_, H>,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    encode_schema_policy_core(field, Some(limits), admission, work)
}
pub(crate) fn encode_single_field_schema_pure_in(
    field: &Field,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, TypeCodecError> {
    encode_schema_policy_core::<std::convert::Infallible>(field, None, None, work)
        .map_err(ProjectionFailure::without_host)
}
fn encode_schema_policy_core<H>(
    field: &Field,
    limits: Option<IpcSchemaProjectionLimits>,
    mut admission: Option<(
        writer_resources::SchemaWriterPrefixFacts,
        &mut Admission<'_, '_, H>,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<u8>, HostError<H>> {
    (|| {
        let facts = if let Some((prefix, admission)) = admission.as_mut() {
            preflight_writer_policy_core(field, limits, Some((*prefix, &mut **admission)), work)?
        } else {
            preflight_writer_policy_core(field, limits, None, work)?
        };
        let capacity = facts.backing;
        work.flush()?;
        let mut backing = Vec::new();
        backing.try_reserve_exact(capacity).map_err(|_| {
            TypeCodecError::Control(novarocks_type_contract::CompileControlError::ResourceExhausted)
        })?;
        work.flush()?;
        // Reserved initialization is our own bounded loop, not an opaque
        // capacity-sized zero-fill hidden inside the library constructor.
        while backing.len() < capacity {
            let end = backing.len().saturating_add(1024).min(capacity);
            backing.resize(end, 0);
            work.step()?;
        }
        work.flush()?;
        let mut builder = FlatBufferBuilder::from_vec(backing);
        work.flush()?;
        let field_offset = emit::emit_field(field, &mut builder, work)?;
        work.flush()?;
        let fields = builder.create_vector(&[field_offset]);
        let schema = arrow::ipc::Schema::create(
            &mut builder,
            &arrow::ipc::SchemaArgs {
                endianness: arrow::ipc::Endianness::Little,
                fields: Some(fields),
                custom_metadata: None,
                features: None,
            },
        );
        let message = arrow::ipc::Message::create(
            &mut builder,
            &arrow::ipc::MessageArgs {
                version: arrow::ipc::MetadataVersion::V5,
                header_type: arrow::ipc::MessageHeader::Schema,
                header: Some(schema.as_union_value()),
                bodyLength: 0,
                custom_metadata: None,
            },
        );
        arrow::ipc::finish_message_buffer(&mut builder, message);
        work.flush()?;
        // The proven primary backing cover must prevent builder growth. This
        // postcondition is a source-model witness, not allocation admission.
        if builder.mut_finished_buffer().0.len() > capacity {
            return Err((TypeCodecError::InvalidShape(
                "IPC schema exceeded its proven backing cover",
            ))
            .into());
        }
        // These are source-derived bounds for this exact writer: every
        // emitted table is counted, and the backing cover also bounds the
        // official verifier's apparent visits, including reused vtables.
        let verifier = novarocks_arrow_ipc_frame::VerifierOptions {
            max_depth: MAX_SCHEMA_VERIFIER_DEPTH,
            max_tables: facts.tables,
            max_apparent_size: capacity,
            ignore_missing_null_terminator: false,
        };
        verify_message(builder.finished_data(), field, &verifier, work)?;
        work.flush()?;
        let output = if admission.is_some() {
            let bytes = builder.finished_data();
            let mut output = Vec::new();
            output.try_reserve_exact(bytes.len()).map_err(|_| {
                TypeCodecError::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                )
            })?;
            for chunk in bytes.chunks(1024) {
                output.extend_from_slice(chunk);
                work.step()?;
            }
            output
        } else {
            builder.finished_data().to_vec()
        };
        work.flush()?;
        Ok(output)
    })()
}

pub(crate) struct PreparedSchemaWriter<'field> {
    field: &'field Field,
    limits: IpcSchemaProjectionLimits,
    source: usize,
    prefix: writer_resources::SchemaWriterPrefixFacts,
    resources: writer_resources::SchemaWriterAllocationFacts,
}
impl PreparedSchemaWriter<'_> {
    pub(crate) fn schema(&self) -> SchemaPreflight {
        self.resources.schema
    }
    pub(crate) fn facts(&self) -> SchemaWriterRequestFacts {
        SchemaWriterRequestFacts {
            request_bytes: self.resources.request_bytes,
            request_count: self.resources.request_count,
            work_upper_bound: self.resources.work_upper_bound,
        }
    }
    #[cfg(test)]
    pub(crate) fn emit_in(
        &self,
        admit: &mut SchemaAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, TypeCodecError> {
        self.emit_with_host_in(
            &mut |facts| admit(facts).map_err(HostError::<std::convert::Infallible>::from),
            work,
        )
        .map_err(ProjectionFailure::without_host)
    }

    pub(crate) fn emit_with_host_in<H>(
        &self,
        admit: &mut HostSchemaAdmit<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<u8>, HostError<H>> {
        let mut admission = Admission {
            parent: Some(admit),
            source: self.source,
            reader: false,
            max_work: Some(self.resources.work_upper_bound),
            facts: self.facts(),
        };
        admission.update(self.facts())?;
        encode_schema_core(
            self.field,
            self.limits,
            Some((self.prefix, &mut admission)),
            work,
        )
    }
}
#[cfg(test)]
pub(crate) fn prepare_schema_writer_in<'field>(
    field: &'field Field,
    source: usize,
    limits: IpcSchemaProjectionLimits,
    max_work: usize,
    admit: &mut SchemaAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedSchemaWriter<'field>, TypeCodecError> {
    prepare_schema_writer_with_host_in(
        field,
        source,
        limits,
        max_work,
        &mut |facts| admit(facts).map_err(HostError::<std::convert::Infallible>::from),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub(crate) fn prepare_schema_writer_with_host_in<'field, H>(
    field: &'field Field,
    source: usize,
    limits: IpcSchemaProjectionLimits,
    max_work: usize,
    admit: &mut HostSchemaAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedSchemaWriter<'field>, HostError<H>> {
    let prefix = writer_resources::schema_writer_prefix_resources_with_host_in(
        field, source, limits, max_work, admit, work,
    )?;
    prepare_schema_writer_with_prefix_host_in(field, source, limits, max_work, prefix, admit, work)
}

pub(crate) fn prepare_schema_writer_with_prefix_host_in<'field, H>(
    field: &'field Field,
    source: usize,
    limits: IpcSchemaProjectionLimits,
    max_work: usize,
    prefix: writer_resources::SchemaWriterPrefixFacts,
    admit: &mut HostSchemaAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedSchemaWriter<'field>, HostError<H>> {
    let mut admission = Admission {
        parent: Some(admit),
        source,
        reader: false,
        max_work: Some(max_work),
        facts: SchemaWriterRequestFacts {
            request_bytes: prefix.request_bytes,
            request_count: 0,
            work_upper_bound: prefix.work_upper_bound,
        },
    };
    let mut resources = writer_resources::preflight_schema_writer_resources_in(
        field,
        source,
        limits,
        prefix,
        &mut admission,
        work,
    )?;
    resources.request_bytes = resources.request_bytes.max(admission.facts.request_bytes);
    resources.request_count = resources.request_count.max(admission.facts.request_count);
    resources.work_upper_bound = resources
        .work_upper_bound
        .max(admission.facts.work_upper_bound);
    Ok(PreparedSchemaWriter {
        field,
        limits,
        source,
        prefix,
        resources,
    })
}
#[cfg(test)]
mod tests;

/// Validates the borrowed schema before any Arrow schema-object conversion.
/// The official verifier's envelope is mandatory and caller-authored. This
/// entry checks one V5 single-field schema message, not an IPC value stream.
/// A depth envelope wider than the supported schema grammar is refused;
/// smaller caller limits remain unchanged. Apparent-size arithmetic must
/// also cover one attempted in-buffer visit without overflowing usize.
pub fn verify_single_field_schema_message(
    metadata: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    control: &dyn PureCompileControl,
) -> Result<(), TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result =
        verify_single_field_schema_with_work(metadata, expected, limits, verifier, &mut work);
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// Same borrowed schema law and verifier, with a parent-owned numerical gate.
/// Reader facts contain source-walker requests and fixed-scratch work only;
/// no FlatBuffer builder or writer output allocation is charged here.
#[cfg(test)]
pub(crate) fn verify_single_field_schema_in(
    metadata: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    source: usize,
    max_work: usize,
    admit: &mut SchemaAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    verify_single_field_schema_with_host_in(
        metadata,
        expected,
        limits,
        verifier,
        source,
        max_work,
        &mut |facts| admit(facts).map_err(HostError::<std::convert::Infallible>::from),
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

pub(crate) fn verify_single_field_schema_with_host_in<H>(
    metadata: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    source: usize,
    max_work: usize,
    admit: &mut HostSchemaAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProjectionFailure<TypeCodecError, H>> {
    verify_single_field_schema_profile_with_host_in(
        metadata,
        expected,
        Some(limits),
        Some(verifier),
        source,
        Some(max_work),
        admit,
        work,
    )
    .map(|_| ())
}
/// Same caller-observed source/count author seals this input's verifier before
/// the sole official parse. No plain DFS or unobserved temporary is introduced.
pub(crate) fn verify_single_field_schema_input_derived_with_host_in<H>(
    metadata: &[u8],
    expected: &Field,
    source: usize,
    admit: &mut HostSchemaAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_arrow_ipc_frame::VerifierOptions, ProjectionFailure<TypeCodecError, H>> {
    verify_single_field_schema_profile_with_host_in(
        metadata, expected, None, None, source, None, admit, work,
    )
}
fn verify_single_field_schema_profile_with_host_in<H>(
    metadata: &[u8],
    expected: &Field,
    limits: Option<IpcSchemaProjectionLimits>,
    verifier: Option<&novarocks_arrow_ipc_frame::VerifierOptions>,
    source: usize,
    max_work: Option<usize>,
    admit: &mut HostSchemaAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_arrow_ipc_frame::VerifierOptions, ProjectionFailure<TypeCodecError, H>> {
    let policy = Policy(true);
    // This original verifier arithmetic is known before any parent admission
    // or completed observation. Plain verification retains its own error order.
    if let Some(verifier) = verifier {
        policy.numeric(checked_add(verifier.max_apparent_size, metadata.len()))?;
    }
    if let Some(limits) = limits {
        policy.cap(
            metadata.len(),
            limits.max_flatbuffer_bytes,
            "IPC schema metadata envelope exceeded",
        )?;
    }
    source_floor(expected, source)?;
    // Before the existing fixed-scratch pass only the root is known. That
    // pass counts all actual nodes and admits the full Vec request bound
    // before the subsequent structural source walk allocates anything.
    let prefix = policy.numeric(writer_resources::prefix_initial(expected, source, 1))?;
    let mut admission = Admission {
        parent: Some(admit),
        source,
        reader: true,
        max_work,
        facts: SchemaWriterRequestFacts::default(),
    };
    let initial = Counts {
        fields: 1,
        types: 0,
        metadata_entries: expected.metadata().len(),
        string_count: 1,
        string_bytes: expected.name().len(),
        metadata_nonempty_fields: usize::from(!expected.metadata().is_empty()),
        ..Counts::default()
    };
    if let Some(limits) = limits {
        policy.cap(
            initial.string_bytes,
            limits.max_string_bytes,
            "IPC schema string envelope exceeded",
        )?;
        policy.cap(
            1,
            limits.max_field_occurrences,
            "IPC schema field envelope exceeded",
        )?;
    }
    let mut initial = initial;
    let children = initial.type_header(expected.data_type(), limits, policy)?;
    initial.fields = policy.numeric(checked_add(initial.fields, children))?;
    if let Some(limits) = limits {
        policy.cap(
            initial.fields,
            limits.max_field_occurrences,
            "IPC schema field envelope exceeded",
        )?;
    }
    writer_resources::counts_prefix(policy.numeric(initial.schema())?, prefix, &mut admission)?;
    (|| {
        let counts =
            preflight_source_policy_core(expected, limits, Some((prefix, &mut admission)), work)?;
        let sealed = match verifier {
            Some(verifier) => novarocks_arrow_ipc_frame::VerifierOptions {
                max_depth: verifier.max_depth,
                max_tables: verifier.max_tables,
                max_apparent_size: verifier.max_apparent_size,
                ignore_missing_null_terminator: verifier.ignore_missing_null_terminator,
            },
            None => {
                let facts = policy.numeric(counts.schema())?;
                novarocks_arrow_ipc_frame::VerifierOptions {
                    max_depth: MAX_SCHEMA_VERIFIER_DEPTH,
                    max_tables: facts.tables,
                    // The original source grammar cover includes repeated
                    // strings/vtables; actual metadata covers wire padding.
                    max_apparent_size: policy
                        .numeric(checked_add(facts.backing, metadata.len()))?,
                    ignore_missing_null_terminator: false,
                }
            }
        };
        // The real host observes the opaque official verifier before it runs.
        if verifier.is_none() {
            let mut facts = admission.facts;
            facts.work_upper_bound = policy.numeric(checked_add(
                facts.work_upper_bound,
                policy.numeric(checked_add(sealed.max_apparent_size, metadata.len()))?,
            ))?;
            admission.update(facts)?;
        }
        verify_message(metadata, expected, &sealed, work)?;
        Ok(sealed)
    })()
}

pub(crate) fn verify_single_field_schema_with_work(
    metadata: &[u8],
    expected: &Field,
    limits: IpcSchemaProjectionLimits,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if metadata.len() > limits.max_flatbuffer_bytes {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema metadata envelope exceeded",
        ));
    }
    // Decode owns no FlatBufferBuilder. Its byte cap applies to the actual
    // borrowed metadata, independently of the encoder's conservative backing
    // cover. Reuse only common source grammar/count/string admission here.
    preflight_source(expected, limits, work)?;
    verify_message(metadata, expected, verifier, work)
}

#[cfg(test)]
mod verify_tests;

fn verify_message(
    metadata: &[u8],
    expected: &Field,
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    let message = verified_message_observed(metadata, verifier, work)?;
    work.step()?;
    if message.version() != arrow::ipc::MetadataVersion::V5
        || message.header_type() != arrow::ipc::MessageHeader::Schema
        || message.bodyLength() != 0
        || message
            .custom_metadata()
            .is_some_and(|entries| !entries.is_empty())
    {
        return Err(TypeCodecError::InvalidShape(
            "unsupported Arrow IPC schema message profile",
        ));
    }
    let schema = message
        .header_as_schema()
        .ok_or(TypeCodecError::InvalidShape(
            "Arrow IPC schema payload is missing",
        ))?;
    work.step()?;
    if schema.endianness() != arrow::ipc::Endianness::Little
        || schema.features().is_some_and(|entries| !entries.is_empty())
        || schema
            .custom_metadata()
            .is_some_and(|entries| !entries.is_empty())
    {
        return Err(TypeCodecError::InvalidShape(
            "unsupported Arrow IPC schema root profile",
        ));
    }
    let fields = schema.fields().ok_or(TypeCodecError::InvalidShape(
        "Arrow IPC schema fields are missing",
    ))?;
    work.step()?;
    if fields.len() != 1 {
        return Err(TypeCodecError::InvalidShape(
            "constant IPC schema must contain exactly one field",
        ));
    }
    verify::verify_field(expected, fields.get(0), work)
}

/// Common bounded, borrowed Message verification. The outer owner retains
/// its exact message profile and completes ordinary-error tails.
pub(crate) fn verified_message_observed<'a>(
    metadata: &'a [u8],
    verifier: &novarocks_arrow_ipc_frame::VerifierOptions,
    work: &mut CompileCheckpoints<'_>,
) -> Result<arrow::ipc::Message<'a>, TypeCodecError> {
    if verifier.max_depth > MAX_SCHEMA_VERIFIER_DEPTH {
        return Err(TypeCodecError::InvalidShape(
            "IPC schema verifier depth envelope exceeds supported grammar",
        ));
    }
    // The official verifier adds each in-buffer visit before comparing its
    // apparent limit. A visit cannot exceed the metadata slice, so this sum
    // proves that even the first over-limit visit cannot overflow usize.
    checked_add(verifier.max_apparent_size, metadata.len())?;
    work.flush()?;
    let parsed = novarocks_arrow_ipc_frame::verified_message(metadata, verifier);
    // Observe the opaque verifier even on its ordinary malformed outcome.
    work.flush()?;
    parsed.map_err(|_| TypeCodecError::InvalidShape("invalid Arrow IPC message metadata"))
}
