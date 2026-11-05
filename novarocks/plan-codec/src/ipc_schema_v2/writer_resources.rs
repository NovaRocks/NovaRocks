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

//! Source-derived schema-only allocation requests and normalized work.
//! Prefix facts must be checked against the caller's real request/work/source
//! coexistence limits before the full helper begins its two type walks.
//! These numbers neither admit memory nor cover batch metadata/body/output.

use super::owner_admission::{Admission, HostSchemaAdmit, SchemaWriterRequestFacts};
use crate::host_projection_v2::ProjectionFailure;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;
use super::{IpcSchemaProjectionLimits, SchemaPreflight, checked_add as add, checked_mul as mul};
use crate::{
    ipc_flat_batch_v2,
    ipc_flat_pool_v2::allocations::field_location_layout,
    physical_type_v2::TypeCodecError,
    resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN},
};
#[cfg(test)]
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use novarocks_type_contract::{
    CompileCheckpoints, MAX_VALUE_TYPE_NODES, ValueTypeVisit,
    validate_arrow_layout_structure_with_scratch_observed,
};
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SchemaWriterPrefixFacts {
    walker_node_bound: usize,
    pub request_bytes: usize,
    pub work_upper_bound: usize,
    field_occurrences: usize,
    metadata_entries: usize,
    source_retained_bytes: usize,
    scratch_work: usize,
    is_flat: bool,
}
impl SchemaWriterPrefixFacts {
    pub(super) fn with_reader_walker_node_bound(mut self, nodes: usize) -> Self {
        self.walker_node_bound = nodes;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SchemaWriterAllocationFacts {
    pub schema: SchemaPreflight,
    pub request_bytes: usize,
    pub request_count: usize,
    pub work_upper_bound: usize,
}
#[derive(Default)]
struct Requests(novarocks_functions::arrow_ipc_construction::SchemaRequestAccumulator);
impl std::ops::Deref for Requests {
    type Target = novarocks_functions::arrow_ipc_construction::SchemaRequestAccumulator;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Requests {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

type WalkerEntry<'a> = novarocks_type_contract::TypeTraversalFrame<'a>;

fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("schema writer resource extent is not representable")
}
fn environment(work: &mut CompileCheckpoints<'_>) -> Result<(), TypeCodecError> {
    let loc = field_location_layout();
    let valid = LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == crate::resource_source_model::LOCKED_ARROW_VERSION
        && cfg!(target_endian = "little")
        && loc.size() == 8
        && loc.align() == 4;
    work.step()?;
    if valid {
        Ok(())
    } else {
        Err(TypeCodecError::InvalidShape(
            "schema writer resource source model changed",
        ))
    }
}
impl Requests {
    #[cfg(test)]
    fn exact<T>(
        &mut self,
        capacity: usize,
        copies: usize,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<(), TypeCodecError> {
        self.0
            .exact::<T, TypeCodecError>(capacity, copies, &mut || {
                if let Some(work) = work.as_deref_mut() {
                    work.step()?;
                }
                Ok(())
            })
    }
    fn geometric<T>(
        &mut self,
        target: usize,
        initial: usize,
        copies: usize,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<(), TypeCodecError> {
        self.0
            .geometric::<T, TypeCodecError>(target, initial, copies, &mut || {
                if let Some(work) = work.as_deref_mut() {
                    work.step()?;
                }
                Ok(())
            })
    }
}

fn flat(field: &Field, work: &mut CompileCheckpoints<'_>) -> Result<bool, TypeCodecError> {
    // Reuse the existing flat carrier author, without another primitive table.
    let result = ipc_flat_batch_v2::layout(field.data_type()).is_ok();
    work.step()?;
    Ok(result)
}
fn walkers(
    requests: &mut Requests,
    node_bound: usize,
    copies: usize,
    work: Option<&mut CompileCheckpoints<'_>>,
) -> Result<(), TypeCodecError> {
    // The original Vec starts at one, grows to four, then doubles. A source's
    // total node occurrences bound its pending entries without a second grammar.
    requests.geometric::<WalkerEntry<'_>>(node_bound, 1, copies, work)
}
fn source_work(source: usize, fields: usize, entries: usize) -> Result<usize, TypeCodecError> {
    // Four preflight metadata scans, one emission scan, and up to K source
    // association scans during verification. Source attributes do not run a
    // logical tag probe; their original backing bounds only actual map work.
    mul(source, add(entries, mul(5, fields)?)?)
}
fn prefix_source_work(source: usize, fields: usize) -> Result<usize, TypeCodecError> {
    // The two subsequent metadata walks use actual source tables. The fixed
    // structural scratch walk only reads lengths and never interprets a tag.
    mul(source, mul(2, fields)?)
}
fn admit_prefix_work(actual: usize, admitted: usize) -> Result<(), TypeCodecError> {
    if actual > admitted {
        Err(TypeCodecError::InvalidShape(
            "schema writer prefix work envelope exceeded",
        ))
    } else {
        Ok(())
    }
}

/// Uses the sole type grammar with fixed scratch to count actual occurrences
/// before the first two Vec walks. The explicit caller work ceiling is checked
/// before initialization and before each actual source metadata walk.
/// Caller request/source-coexistence admission and flush remain mandatory.
pub(crate) fn schema_writer_prefix_resources(
    field: &Field,
    source_retained_bytes: usize,
    _limits: IpcSchemaProjectionLimits,
    max_preflight_library_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaWriterPrefixFacts, TypeCodecError> {
    schema_writer_prefix_core::<std::convert::Infallible>(
        field,
        source_retained_bytes,
        _limits,
        max_preflight_library_work,
        None,
        work,
    )
    .map_err(ProjectionFailure::without_host)
}

fn schema_writer_prefix_core<H>(
    field: &Field,
    source_retained_bytes: usize,
    _limits: IpcSchemaProjectionLimits,
    max_preflight_library_work: usize,
    mut admission: Option<&mut Admission<'_, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaWriterPrefixFacts, HostError<H>> {
    let result = (|| {
        let policy = super::owner_admission::Policy(admission.is_some());
        let add = |a, b| policy.numeric(add(a, b));
        let mul = |a, b| policy.numeric(mul(a, b));
        if let Some(admission) = admission.as_deref_mut() {
            let initial = policy.numeric(prefix_initial(field, source_retained_bytes, 1))?;
            let mut requests = Requests::default();
            walkers(&mut requests, initial.walker_node_bound, 2, None)?;
            admission.update(SchemaWriterRequestFacts {
                request_bytes: initial.request_bytes,
                request_count: requests.count,
                work_upper_bound: initial.work_upper_bound,
            })?;
            admission.update(super::initial_writer_request_facts(
                field,
                source_retained_bytes,
                _limits,
            )?)?;
        }
        environment(work)?;
        let is_flat = flat(field, work)?;
        let mut requests = Requests::default();
        walkers(&mut requests, 1, 2, Some(work))?;
        let mut fields = 1usize;
        let mut type_occurrences = 1usize;
        let mut entries = field.metadata().len();
        let mut storage = add(mul(4, requests.bytes)?, requests.count)?;
        let mut scratch_work = 0usize;
        let work_upper_bound = if is_flat {
            let bound = add(
                source_work(source_retained_bytes, fields, entries)?,
                storage,
            )?;
            if let Some(admission) = admission.as_deref_mut() {
                policy.cap(
                    bound,
                    max_preflight_library_work,
                    "schema writer prefix work envelope exceeded",
                )?;
                admission.update(SchemaWriterRequestFacts {
                    request_bytes: requests.bytes,
                    request_count: requests.count,
                    work_upper_bound: bound,
                })?;
            }
            work.step()?;
            admit_prefix_work(bound, max_preflight_library_work)?;
            bound
        } else {
            // Inline borrowed scratch is not a heap allocation request. Its
            // finite initialization is opaque and bracketed, not internally
            // cooperative or a claim of formal stack memory admission.
            let bytes = Layout::array::<Option<WalkerEntry<'_>>>(MAX_VALUE_TYPE_NODES)
                .map_err(|_| invalid())?
                .size();
            scratch_work = mul(4, bytes)?;
            let initial = add(
                add(prefix_source_work(source_retained_bytes, fields)?, storage)?,
                scratch_work,
            )?;
            if let Some(admission) = admission.as_deref_mut() {
                policy.cap(
                    initial,
                    max_preflight_library_work,
                    "schema writer prefix work envelope exceeded",
                )?;
                admission.update(SchemaWriterRequestFacts {
                    request_bytes: requests.bytes,
                    request_count: requests.count,
                    work_upper_bound: initial,
                })?;
            }
            work.step()?;
            admit_prefix_work(initial, max_preflight_library_work)?;
            work.flush()?;
            let mut scratch = [None; MAX_VALUE_TYPE_NODES];
            work.flush()?;
            validate_arrow_layout_structure_with_scratch_observed(
                field.data_type(),
                &mut scratch,
                |visit| {
                    match visit {
                        ValueTypeVisit::Field(child) => {
                            fields = add(fields, 1)?;
                            entries = add(entries, child.metadata().len())?;
                        }
                        ValueTypeVisit::ChildEdge(_) => {
                            type_occurrences = add(type_occurrences, 1)?;
                            // Admit each known increase before any later Vec walk.
                            let mut next = Requests::default();
                            walkers(&mut next, type_occurrences, 2, None)?;
                            requests = next;
                            storage = add(mul(4, requests.bytes)?, requests.count)?;
                        }
                        ValueTypeVisit::TypeNode(_) => {}
                    }
                    scratch_work = add(scratch_work, 2)?;
                    let bound = add(
                        add(prefix_source_work(source_retained_bytes, fields)?, storage)?,
                        scratch_work,
                    )?;
                    if let Some(admission) = admission.as_deref_mut() {
                        policy.cap(
                            bound,
                            max_preflight_library_work,
                            "schema writer prefix work envelope exceeded",
                        )?;
                        admission.update(SchemaWriterRequestFacts {
                            request_bytes: requests.bytes,
                            request_count: requests.count,
                            work_upper_bound: bound,
                        })?;
                    }
                    work.step()?;
                    admit_prefix_work(bound, max_preflight_library_work)?;
                    // Keep the original opaque-operation boundaries around
                    // source Fields and structural edges; no tag is decoded.
                    if matches!(
                        visit,
                        ValueTypeVisit::Field(_) | ValueTypeVisit::ChildEdge(_)
                    ) {
                        work.flush()?;
                    }
                    Ok::<_, HostError<H>>(())
                },
            )?;
            add(
                add(prefix_source_work(source_retained_bytes, fields)?, storage)?,
                scratch_work,
            )?
        };
        Ok(SchemaWriterPrefixFacts {
            walker_node_bound: type_occurrences,
            request_bytes: requests.bytes,
            work_upper_bound,
            field_occurrences: fields,
            metadata_entries: entries,
            source_retained_bytes,
            scratch_work,
            is_flat,
        })
    })();
    if matches!(
        &result,
        Err(ProjectionFailure::Codec(TypeCodecError::Control(_)) | ProjectionFailure::Host(_))
    ) {
        return result;
    }
    work.flush()?;
    result
}

/// Performs the sole schema preflight (two walks). The actual encoder performs
/// two more, already included below. One earlier scratch walk is also counted.
/// Callers must use `facts.schema` rather
/// than performing another schema preflight, which would add two more walks.
/// The caller has already checked the prefix, and must check the full facts
/// before any schema builder or encoded metadata allocation.
pub(crate) fn preflight_schema_writer_resources(
    field: &Field,
    source_retained_bytes: usize,
    limits: IpcSchemaProjectionLimits,
    prefix: SchemaWriterPrefixFacts,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaWriterAllocationFacts, TypeCodecError> {
    let result = (|| {
        let schema = super::preflight_writer(field, limits, work)?;
        let actual_flat = flat(field, work)?;
        let matches_prefix = prefix.walker_node_bound == schema.type_occurrences
            && prefix.field_occurrences == schema.field_occurrences
            && prefix.metadata_entries == schema.metadata_entries
            && prefix.source_retained_bytes == source_retained_bytes
            && prefix.is_flat == actual_flat;
        work.step()?;
        if !matches_prefix {
            return Err(TypeCodecError::InvalidShape(
                "schema writer prefix differs from source",
            ));
        }
        let facts = allocation_facts(schema, prefix, Some(work))?;
        work.step()?;
        Ok(facts)
    })();
    if matches!(&result, Err(TypeCodecError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

impl From<novarocks_functions::arrow_ipc_construction::IpcGeometryError> for TypeCodecError {
    fn from(_: novarocks_functions::arrow_ipc_construction::IpcGeometryError) -> Self {
        invalid()
    }
}

pub(super) fn allocation_facts(
    schema: SchemaPreflight,
    prefix: SchemaWriterPrefixFacts,
    mut work: Option<&mut CompileCheckpoints<'_>>,
) -> Result<SchemaWriterAllocationFacts, TypeCodecError> {
    use novarocks_functions::arrow_ipc_construction::{
        SchemaGeometry, SchemaResourceSource, schema_writer_allocation_metrics,
    };
    let geometry = SchemaGeometry {
        backing: schema.backing,
        tables: schema.tables,
        metadata_entries: schema.metadata_entries,
        string_bytes: schema.string_bytes,
        field_occurrences: schema.field_occurrences,
        type_occurrences: schema.type_occurrences,
        metadata_nonempty_fields: schema.metadata_nonempty_fields,
        child_offset_items: schema.child_offset_items,
        child_offset_requests: schema.child_offset_requests,
        union_id_items: schema.union_id_items,
        union_id_requests: schema.union_id_requests,
    };
    let source = SchemaResourceSource {
        walker_node_bound: prefix.walker_node_bound,
        source_retained_bytes: prefix.source_retained_bytes,
        scratch_work: prefix.scratch_work,
    };
    // Receiving policy, source custody and the concrete locked layout guard
    // remain at this original codec boundary. Only numerical geometry moved.
    let metrics = schema_writer_allocation_metrics::<TypeCodecError>(
        geometry,
        source,
        field_location_layout(),
        &mut || {
            if let Some(work) = work.as_deref_mut() {
                work.step()?;
            }
            Ok(())
        },
    )?;
    Ok(SchemaWriterAllocationFacts {
        schema,
        request_bytes: metrics.request_bytes,
        request_count: metrics.request_count,
        work_upper_bound: metrics.work_upper_bound,
    })
}

pub(super) fn prefix_initial(
    field: &Field,
    source: usize,
    node_bound: usize,
) -> Result<SchemaWriterPrefixFacts, TypeCodecError> {
    let is_flat = ipc_flat_batch_v2::layout(field.data_type()).is_ok();
    let mut requests = Requests::default();
    walkers(&mut requests, node_bound, 2, None)?;
    let scratch_work = if is_flat {
        0
    } else {
        mul(
            4,
            Layout::array::<Option<WalkerEntry<'_>>>(MAX_VALUE_TYPE_NODES)
                .map_err(|_| invalid())?
                .size(),
        )?
    };
    let fields = 1;
    let entries = field.metadata().len();
    let storage = add(mul(4, requests.bytes)?, requests.count)?;
    let work_upper_bound = if is_flat {
        add(source_work(source, fields, entries)?, storage)?
    } else {
        add(
            add(prefix_source_work(source, fields)?, storage)?,
            scratch_work,
        )?
    };
    Ok(SchemaWriterPrefixFacts {
        walker_node_bound: node_bound,
        request_bytes: requests.bytes,
        work_upper_bound,
        field_occurrences: fields,
        metadata_entries: entries,
        source_retained_bytes: source,
        scratch_work,
        is_flat,
    })
}
pub(super) fn counts_prefix<H>(
    schema: SchemaPreflight,
    prefix: SchemaWriterPrefixFacts,
    admission: &mut Admission<'_, '_, H>,
) -> Result<(), HostError<H>> {
    if admission.reader {
        let facts = admission.policy().numeric(reader_facts(schema, prefix))?;
        return admission.update(facts);
    }
    let facts = admission
        .policy()
        .numeric(allocation_facts(schema, prefix, None))?;
    admission.update(SchemaWriterRequestFacts {
        request_bytes: facts.request_bytes,
        request_count: facts.request_count,
        work_upper_bound: facts.work_upper_bound,
    })
}
pub(super) fn reader_facts(
    schema: SchemaPreflight,
    prefix: SchemaWriterPrefixFacts,
) -> Result<SchemaWriterRequestFacts, TypeCodecError> {
    let mut requests = Requests::default();
    // The observed strict pass uses fixed scratch and counts this source's
    // actual nodes before the sole structural Vec walker starts. Every prefix
    // admits its known requests; the complete bound precedes the first reserve.
    walkers(&mut requests, prefix.walker_node_bound, 1, None)?;
    let scratch = Layout::new::<[Option<WalkerEntry<'_>>; MAX_VALUE_TYPE_NODES]>().size();
    let associations = mul(
        mul(4, mul(schema.metadata_entries, schema.metadata_entries)?)?,
        add(schema.string_bytes, 1)?,
    )?;
    let storage = add(mul(4, requests.bytes)?, requests.count)?;
    let structural = mul(32, add(schema.field_occurrences, schema.type_occurrences)?)?;
    let work = add(
        add(
            source_work(
                prefix.source_retained_bytes,
                schema.field_occurrences,
                schema.metadata_entries,
            )?,
            associations,
        )?,
        add(mul(4, scratch)?, add(storage, structural)?)?,
    )?;
    Ok(SchemaWriterRequestFacts {
        request_bytes: requests.bytes,
        request_count: requests.count,
        work_upper_bound: work,
    })
}

pub(crate) fn schema_writer_prefix_resources_with_host_in<H>(
    field: &Field,
    source: usize,
    limits: IpcSchemaProjectionLimits,
    max_work: usize,
    admit: &mut HostSchemaAdmit<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaWriterPrefixFacts, HostError<H>> {
    let mut admission = Admission {
        parent: Some(admit),
        source,
        reader: false,
        max_work: Some(max_work),
        facts: SchemaWriterRequestFacts::default(),
    };
    schema_writer_prefix_core(field, source, limits, max_work, Some(&mut admission), work)
}
pub(super) fn preflight_schema_writer_resources_in<H>(
    field: &Field,
    source: usize,
    limits: IpcSchemaProjectionLimits,
    prefix: SchemaWriterPrefixFacts,
    admission: &mut Admission<'_, '_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SchemaWriterAllocationFacts, HostError<H>> {
    let schema =
        super::preflight_writer_core(field, limits, Some((prefix, &mut *admission)), work)?;
    let actual_flat = ipc_flat_batch_v2::layout(field.data_type()).is_ok();
    let same = prefix.walker_node_bound == schema.type_occurrences
        && prefix.field_occurrences == schema.field_occurrences
        && prefix.metadata_entries == schema.metadata_entries
        && prefix.source_retained_bytes == source
        && prefix.is_flat == actual_flat;
    work.step()?; // original flat classification
    work.step()?; // original source-prefix comparison
    if !same {
        return Err(
            (TypeCodecError::InvalidShape("schema writer prefix differs from source")).into(),
        );
    }
    let facts = admission
        .policy()
        .numeric(allocation_facts(schema, prefix, None))?;
    admission.update(SchemaWriterRequestFacts {
        request_bytes: facts.request_bytes,
        request_count: facts.request_count,
        work_upper_bound: facts.work_upper_bound,
    })?;
    let result = allocation_facts(schema, prefix, Some(work))?;
    work.step()?;
    work.flush()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::UnionFields;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    const PHASE: CompilePhase = CompilePhase::Encode;
    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];
    struct Control {
        trace: Mutex<Vec<(CompilePhase, u32)>>,
        failure: Option<(usize, CompileControlError)>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                trace: Mutex::new(Vec::new()),
                failure: None,
            }
        }
        fn refusing(at: usize, cause: CompileControlError) -> Self {
            Self {
                trace: Mutex::new(Vec::new()),
                failure: Some((at, cause)),
            }
        }
        fn trace(&self) -> Vec<(CompilePhase, u32)> {
            self.trace.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, PHASE);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            if let Some((refusal, _)) = self.failure {
                assert!(at <= refusal, "callback after primary refusal");
            }
            trace.push((phase, units));
            match self.failure.clone() {
                Some((refusal, cause)) if at == refusal => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn limits() -> IpcSchemaProjectionLimits {
        IpcSchemaProjectionLimits {
            max_field_occurrences: 4096,
            max_type_occurrences: 4096,
            max_string_bytes: 64 * 1024 * 1024,
            max_flatbuffer_bytes: 128 * 1024 * 1024,
        }
    }
    fn full(
        field: &Field,
        source: usize,
        limits: IpcSchemaProjectionLimits,
        control: &Control,
    ) -> Result<SchemaWriterAllocationFacts, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, PHASE)?;
        let prefix =
            schema_writer_prefix_resources(field, source, limits, 1024 * 1024 * 1024, &mut work)?;
        preflight_schema_writer_resources(field, source, limits, prefix, &mut work)
    }
    fn prefixes(field: &Field, source: usize, limits: IpcSchemaProjectionLimits) {
        let control = Control::good();
        let _ = full(field, source, limits, &control);
        let trace = control.trace();
        assert!(trace.len() >= 2);
        assert!(trace.iter().all(|(_, n)| *n <= 256));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control::refusing(at, cause.clone());
                assert!(
                    matches!(full(field,source,limits,&control),Err(TypeCodecError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
    fn prefix_request_bytes(field: &Field) -> usize {
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        schema_writer_prefix_resources(field, 1024, limits(), 1024 * 1024 * 1024, &mut work)
            .unwrap()
            .request_bytes
    }
    fn field(name: &str, ty: DataType, k: usize) -> Field {
        Field::new(name, ty, true).with_metadata(
            (0..k)
                .map(|i| (format!("k{i}"), format!("v{i}")))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn flat_schema_keeps_precise_four_walkers_and_exact_empty_auxiliary_requests() {
        let f = field("source", DataType::Int64, 0);
        let facts = full(&f, 1024, limits(), &Control::good()).unwrap();
        assert_eq!(facts.schema.field_occurrences, 1);
        assert_eq!(facts.schema.type_occurrences, 1);
        assert_eq!(facts.schema.tables, 4);
        assert_eq!(facts.request_count, 9);
        // Two primary/copy requests, four one-entry walkers, FieldLoc4+8,
        // and first u32 vtable capacity4. Empty metadata/children allocate zero.
        assert_eq!(
            facts.request_bytes,
            2 * facts.schema.backing + 4 * Layout::new::<WalkerEntry<'_>>().size() + 12 * 8 + 4 * 4
        );
        assert_eq!(
            prefix_request_bytes(&f),
            2 * Layout::new::<WalkerEntry<'_>>().size()
        );
        prefixes(&f, 1024, limits());
    }
    #[test]
    fn nested_prefixes_capture_actual_node_bounds_and_vec_growth() {
        let list = field(
            "list",
            DataType::List(Arc::new(field("item", DataType::Int32, 0))),
            0,
        );
        let map = field(
            "map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            0,
        );
        let dictionary = field(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            0,
        );
        let wide = field(
            "four_children",
            DataType::Struct(
                (0..4)
                    .map(|i| Arc::new(Field::new(format!("c{i}"), DataType::Int32, true)))
                    .collect::<Vec<_>>()
                    .into(),
            ),
            0,
        );
        for (field, expected_nodes, capacities) in [
            (&list, 2, 5),
            (&map, 4, 5),
            (&dictionary, 3, 5),
            (&wide, 5, 13),
        ] {
            let c = Control::good();
            let mut work = CompileCheckpoints::try_new(&c, PHASE).unwrap();
            let prefix =
                schema_writer_prefix_resources(field, 1024, limits(), usize::MAX / 4, &mut work)
                    .unwrap();
            assert_eq!(prefix.walker_node_bound, expected_nodes);
            assert_eq!(
                prefix.request_bytes,
                2 * capacities * Layout::new::<WalkerEntry<'_>>().size()
            );
            let facts = preflight_schema_writer_resources(field, 1024, limits(), prefix, &mut work)
                .unwrap();
            assert_eq!(facts.schema.type_occurrences, expected_nodes);
        }
    }

    #[test]
    fn nested_schema_counts_each_metadata_owner_and_container_request_without_a_second_walk() {
        let f = field(
            "root",
            DataType::Struct(
                vec![
                    Arc::new(field("left", DataType::Int64, 2)),
                    Arc::new(field(
                        "right",
                        DataType::List(Arc::new(field("item", DataType::Utf8, 2))),
                        2,
                    )),
                ]
                .into(),
            ),
            2,
        );
        let facts = full(&f, 16 * 1024, limits(), &Control::good()).unwrap();
        assert_eq!(facts.schema.field_occurrences, 4);
        assert_eq!(facts.schema.type_occurrences, 4);
        assert_eq!(facts.schema.metadata_entries, 8);
        assert_eq!(facts.schema.metadata_nonempty_fields, 4);
        assert_eq!(facts.schema.child_offset_items, 3);
        assert_eq!(facts.schema.child_offset_requests, 2);
        assert_eq!(facts.schema.tables, 18);
        let walk = Layout::new::<WalkerEntry<'_>>().size();
        // Four source nodes bound pending growth to capacities one and four.
        assert_eq!(prefix_request_bytes(&f), 2 * 5 * walk);
        assert_eq!(
            facts.request_bytes,
            2 * facts.schema.backing
                + 4 * 5 * walk
                + 8 * (Layout::new::<(&str, &str)>().size()
                    + Layout::new::<flatbuffers::WIPOffset<arrow::ipc::KeyValue<'static>>>()
                        .size())
                + 3 * Layout::new::<flatbuffers::WIPOffset<arrow::ipc::Field<'static>>>().size()
                + 12 * 8
                + (4 + 8 + 16 + 32) * 4
        );
        assert_eq!(facts.request_count, 2 + 4 * 2 + 8 + 2 + 2 + 4);
        assert!(facts.work_upper_bound > facts.request_bytes);
        // Eighteen actual source tables are reusable by the caller; it must
        // not run preflight_writer again to recover this same information.
        prefixes(&f, 16 * 1024, limits());
    }
    #[test]
    fn schema_struct320_and_existing_union_resources_preserve_actual_auxiliary_shapes() {
        let fields = (0..320)
            .map(|i| Arc::new(field(&format!("f{i}"), DataType::Int64, 1)))
            .collect::<Vec<_>>();
        let f = field("root", DataType::Struct(fields.into()), 1);
        let control = Control::good();
        let facts = full(&f, 128 * 1024, limits(), &control).unwrap();
        assert_eq!(facts.schema.field_occurrences, 321);
        assert_eq!(facts.schema.metadata_entries, 321);
        assert_eq!(facts.schema.metadata_nonempty_fields, 321);
        assert_eq!(facts.schema.child_offset_items, 320);
        assert_eq!(facts.schema.child_offset_requests, 1);
        assert!(control.trace().iter().any(|(_, units)| *units == 256));
        prefixes(&f, 128 * 1024, limits());
        // This is the existing schema-only Union author, not a new pool IPC
        // encoding permission or a pending Union NULL semantic decision.
        let f = field(
            "union",
            DataType::Union(
                UnionFields::try_new(
                    vec![2, 7],
                    vec![
                        Arc::new(field("a", DataType::Int64, 0)),
                        Arc::new(field("b", DataType::Boolean, 0)),
                    ],
                )
                .unwrap(),
                arrow::datatypes::UnionMode::Sparse,
            ),
            0,
        );
        let facts = full(&f, 1024, limits(), &Control::good()).unwrap();
        assert_eq!(facts.schema.union_id_items, 2);
        assert_eq!(facts.schema.union_id_requests, 1);
        assert_eq!(facts.schema.child_offset_items, 2);
    }
    #[test]
    fn nested_prefix_admits_actual_topology_before_each_logical_probe_under_the_original_cap() {
        let f = field(
            "root",
            DataType::Struct(
                vec![
                    Arc::new(field("left", DataType::Int64, 2)),
                    Arc::new(field(
                        "right",
                        DataType::List(Arc::new(field("item", DataType::Utf8, 2))),
                        2,
                    )),
                ]
                .into(),
            ),
            2,
        );
        let source = 1024 * 1024;
        let original_cap = 1024 * 1024 * 1024;
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        let prefix =
            schema_writer_prefix_resources(&f, source, limits(), original_cap, &mut work).unwrap();
        assert_eq!(prefix.field_occurrences, 4);
        assert_eq!(prefix.metadata_entries, 8);
        // The earlier MAX_FIELDS * MAX_METADATA association prefix rejected
        // this legitimate 1MiB source under its unchanged 1GiB work ceiling.
        // Actual borrowed topology now keeps the prefix below 32MiB.
        assert!(prefix.work_upper_bound < 32 * 1024 * 1024);
        let run = |cap, control: &Control| {
            let mut work = CompileCheckpoints::try_new(control, PHASE)?;
            schema_writer_prefix_resources(&f, source, limits(), cap, &mut work)
        };
        assert!(run(prefix.work_upper_bound, &Control::good()).is_ok());
        let cap = prefix.work_upper_bound - 1;
        let control = Control::good();
        assert!(matches!(
            run(cap, &control),
            Err(TypeCodecError::InvalidShape(_))
        ));
        let trace = control.trace();
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control::refusing(at, cause.clone());
                assert!(
                    matches!(run(cap, &control), Err(TypeCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
    #[test]
    fn schema_resource_overflow_and_ordinary_source_refusals_observe_the_completed_tail() {
        let f = field(
            "root",
            DataType::List(Arc::new(field("item", DataType::Int64, 0))),
            0,
        );
        assert!(matches!(
            full(&f, usize::MAX, limits(), &Control::good()),
            Err(TypeCodecError::InvalidShape(_))
        ));
        prefixes(&f, usize::MAX, limits());
        let mut tight = limits();
        tight.max_field_occurrences = 1;
        assert!(matches!(
            full(&f, 1024, tight, &Control::good()),
            Err(TypeCodecError::InvalidShape(_))
        ));
        prefixes(&f, 1024, tight);
        let wrong = field(
            &"x".repeat(novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES + 1),
            DataType::Int64,
            0,
        );
        prefixes(&wrong, 4096, limits());
        let empty = field(
            "empty",
            DataType::Struct(Vec::<Arc<Field>>::new().into()),
            0,
        );
        let facts = full(&empty, 1024, limits(), &Control::good()).unwrap();
        assert_eq!(facts.schema.child_offset_requests, 0);
        assert_eq!(facts.schema.metadata_nonempty_fields, 0);
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        let mut requests = Requests::default();
        assert!(
            requests
                .exact::<u64>(usize::MAX, 1, Some(&mut work))
                .is_err()
        );
        work.finish().unwrap();
        assert!(control.trace().last().unwrap().1 > 0);
    }
}
