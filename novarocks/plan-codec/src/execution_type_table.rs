// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Complete execution bindings in an explicit owner-local namespace.
//! No metadata, runtime encoding or executable capability is inferred here.

use crate::{
    host_projection_v2::{AdmissionRefusal, ProjectionFailure},
    value_type_table::{
        self, DecodedLogicalTypeTable, LogicalTypeCodecError, LogicalTypeRoot,
        LogicalTypeTableBuilder, LogicalTypeTableLimits,
        resources::{self, Context, find, invalid, sort_by_id},
    },
};
use novarocks_proto_models::{common, plan as wire, value_type as logical_wire};
#[cfg(test)]
use novarocks_type_contract::ValueLayout;
use novarocks_type_contract::{
    ArrowTypeDepth, CompileCheckpoints, CompileControlError, FunctionValueType, TypedValueBinding,
    ValueTypeStorageLifetime,
};
use std::alloc::Layout;
mod layout;
mod scalars;
#[cfg(test)]
mod tests;
type Failure<H> = ProjectionFailure<LogicalTypeCodecError, H>;
impl<H> From<novarocks_type_contract::ExecutionValueTypeError> for Failure<H> {
    fn from(error: novarocks_type_contract::ExecutionValueTypeError) -> Self {
        Self::Codec(error.into())
    }
}

/// Limits are caller policy, not an allocation receipt or execution capability.
#[derive(Clone, Copy, Debug)]
pub struct ExecutionTypeTableLimits {
    pub logical: LogicalTypeTableLimits,
    pub max_bindings: usize,
    /// Counts actual physical types, including Map entries wrappers, across bindings.
    pub max_layout_nodes: usize,
    pub max_layout_string_bytes: usize,
}
impl ExecutionTypeTableLimits {
    /// Finite grammar envelope for an actual owner with this many validated
    /// roots. This is neither a memory quota nor an allocation/grant receipt.
    /// Every definition contributes its unfolded subtree: summed across a
    /// root's occurrences this is at most nodes * ancestor-depth, not nodes².
    pub fn for_validated_root_count(roots: usize) -> Result<Self, CompileControlError> {
        use novarocks_type_contract::{
            MAX_ARROW_FIELD_NAME_BYTES as NAME, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES as ZONE,
            MAX_VALUE_TYPE_DEPTH as D, MAX_VALUE_TYPE_NODES as N,
        };
        let arithmetic = || CompileControlError::ResourceExhausted;
        let definitions = roots.checked_mul(N).ok_or_else(arithmetic)?;
        let expanded = definitions.checked_mul(D).ok_or_else(arithmetic)?;
        let strings_per_root = N
            .checked_mul(NAME.checked_add(ZONE).ok_or_else(arithmetic)?)
            .ok_or_else(arithmetic)?;
        let logical_strings = roots.checked_mul(strings_per_root).ok_or_else(arithmetic)?;
        // Independently decoded schemas also copy their root column names.
        let layout_strings = roots
            .checked_mul(strings_per_root.checked_add(NAME).ok_or_else(arithmetic)?)
            .ok_or_else(arithmetic)?;
        Ok(Self {
            logical: crate::value_type_table::LogicalTypeTableLimits {
                max_definitions: definitions,
                max_expanded_nodes: expanded,
                max_string_bytes: logical_strings,
            },
            max_bindings: roots,
            max_layout_nodes: definitions,
            max_layout_string_bytes: layout_strings,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ExecutionTypeRoot<'a> {
    pub id: u32,
    pub value: &'a FunctionValueType,
}

/// Retains the fully constructed wire table; no partially encoded owner escapes.
pub struct EncodedExecutionTypeTable {
    table: wire::ExecutionTypeTable,
}
impl EncodedExecutionTypeTable {
    pub fn wire(&self) -> &wire::ExecutionTypeTable {
        &self.table
    }
    pub fn into_wire(self) -> wire::ExecutionTypeTable {
        self.table
    }
    pub fn type_reference_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<common::TypeDesc, LogicalTypeCodecError> {
        if find(
            &self.table.value_bindings,
            id,
            |v| v.id.expect("validated binding ID"),
            work,
        )?
        .is_none()
        {
            return Err(LogicalTypeCodecError::InvalidShape(
                "dangling execution binding reference",
            ));
        }
        Ok(common::TypeDesc {
            value_binding_id: Some(id),
        })
    }
}
pub struct ExecutionTypeTableBuilder<'a> {
    roots: &'a [ExecutionTypeRoot<'a>],
}
impl<'a> ExecutionTypeTableBuilder<'a> {
    pub fn new(roots: &'a [ExecutionTypeRoot<'a>]) -> Self {
        Self { roots }
    }
    pub fn encode_observed<H>(
        &self,
        limits: ExecutionTypeTableLimits,
        request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<EncodedExecutionTypeTable, Failure<H>> {
        let mut context = Context {
            observe_scope: None,
            request,
            work,
        };
        context.step()?;
        if self.roots.len() > limits.max_bindings {
            return Err(invalid("execution binding count exceeds caller profile"));
        }
        let mut logical_roots = context.vector::<LogicalTypeRoot>(self.roots.len())?;
        let mut ordered = context.vector::<(u32, usize)>(self.roots.len())?;
        for (ordinal, root) in self.roots.iter().enumerate() {
            context.step()?;
            ordered.push((root.id, ordinal));
        }
        sort_by_id(&mut ordered, |v| v.0, &mut context)?;
        for pair in ordered.windows(2) {
            context.step()?;
            if pair[0].0 == pair[1].0 {
                return Err(invalid("duplicate execution binding ID"));
            }
        }
        for (logical_ordinal, (_, source_ordinal)) in ordered.iter().enumerate() {
            context.step()?;
            let id = u32::try_from(logical_ordinal)
                .map_err(|_| invalid("execution logical root IDs exhausted"))?;
            logical_roots.push(LogicalTypeRoot {
                id,
                value: self.roots[*source_ordinal].value.value_type(),
            });
        }
        let logical_types = LogicalTypeTableBuilder::new(&logical_roots).encode_observed(
            limits.logical,
            &mut |layout| (context.request)(layout),
            context.work,
        )?;
        let definitions = DefinitionIndex::new(&logical_types, &mut context)?;
        let mut counts = layout::Counts::default();
        for (_, ordinal) in &ordered {
            layout::count_source(
                self.roots[*ordinal].value.data_type(),
                ArrowTypeDepth::ROOT,
                &mut counts,
                limits,
                &mut context,
            )?;
        }
        let mut value_bindings = context.vector(ordered.len())?;
        for (logical_ordinal, (id, ordinal)) in ordered.into_iter().enumerate() {
            context.step()?;
            let logical_id = logical_ordinal as u32;
            let value_layout = layout::encode(
                self.roots[ordinal].value.data_type(),
                logical_id,
                &definitions,
                &mut context,
            )?;
            value_bindings.push(wire::ExecutionValueBinding {
                id: Some(id),
                logical_type_id: Some(logical_id),
                value_layout: Some(value_layout),
            });
        }
        drop(definitions);
        Ok(EncodedExecutionTypeTable {
            table: wire::ExecutionTypeTable {
                logical_types,
                value_bindings,
            },
        })
    }
}

/// Immutable values retain the supplied storage owner independently of this map.
pub struct DecodedExecutionTypeTable {
    values: Vec<(u32, FunctionValueType)>,
    _storage: ValueTypeStorageLifetime,
    retained_source_bytes: usize,
}
impl DecodedExecutionTypeTable {
    /// Sole-constructor upper for final heap backing reachable from this table.
    /// Shared logical nodes contribute once. Scratch, unused definitions and
    /// intermediate Map caches are absent. This is numeric source provenance,
    /// not a request/grant or exact allocator-retained ledger. The caller counts
    /// this inline table value in its own source container once.
    pub fn retained_source_bytes_upper_bound(&self) -> usize {
        self.retained_source_bytes
    }
    pub fn len(&self) -> usize {
        self.values.len()
    }
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (u32, &FunctionValueType)> {
        self.values.iter().map(|(id, v)| (*id, v))
    }
    pub fn get_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&FunctionValueType>, LogicalTypeCodecError> {
        Ok(find(&self.values, id, |v| v.0, work)?.map(|i| &self.values[i].1))
    }
    /// TypeDesc is solely a reference into this exact explicit owner.
    pub fn type_reference_observed(
        &self,
        reference: &common::TypeDesc,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&FunctionValueType, LogicalTypeCodecError> {
        work.step()?;
        let id = reference
            .value_binding_id
            .ok_or(LogicalTypeCodecError::InvalidShape(
                "missing execution binding reference",
            ))?;
        self.get_observed(id, work)?
            .ok_or(LogicalTypeCodecError::InvalidShape(
                "dangling execution binding reference",
            ))
    }
}

pub fn decode_execution_type_table_observed<H>(
    table: &wire::ExecutionTypeTable,
    limits: ExecutionTypeTableLimits,
    storage: ValueTypeStorageLifetime,
    request: &mut impl FnMut(Layout) -> Result<(), AdmissionRefusal<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedExecutionTypeTable, Failure<H>> {
    decode_execution_type_table_core::<H, resources::NoFactory>(
        BindingSource::Table(table),
        limits,
        storage,
        None,
        &mut Context {
            observe_scope: None,
            request,
            work,
        },
    )
}

/// Decode on independent lifetimes supplied by the actual BE caller. No source
/// metadata, process context or transported grant participates in this owner.
pub fn decode_execution_type_table_with_factory_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    table: &wire::ExecutionTypeTable,
    limits: ExecutionTypeTableLimits,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedExecutionTypeTable, Failure<H>> {
    let mut unreachable_request = |_: Layout| -> Result<(), AdmissionRefusal<H>> {
        unreachable!("scoped execution decoder lost its explicit factory")
    };
    decode_execution_type_table_core(
        BindingSource::Table(table),
        limits,
        ValueTypeStorageLifetime::pure(),
        Some(factory),
        &mut Context {
            observe_scope: None,
            request: &mut unreachable_request,
            work,
        },
    )
}

/// Borrow one real schema-local logical namespace and its original roots.
/// No synthetic ExecutionTypeTable or cloned physical DTO is constructed.
pub fn decode_execution_schema_roots_with_factory_in<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    columns: &[wire::ArrowPhysicalColumn],
    logical_types: &[logical_wire::LogicalTypeDefinition],
    limits: ExecutionTypeTableLimits,
    factory: &F,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedExecutionTypeTable, Failure<H>> {
    let mut unreachable_request = |_: Layout| -> Result<(), AdmissionRefusal<H>> {
        unreachable!("scoped execution schema decoder lost its explicit factory")
    };
    decode_execution_type_table_core(
        BindingSource::Schema {
            columns,
            logical_types,
        },
        limits,
        ValueTypeStorageLifetime::pure(),
        Some(factory),
        &mut Context {
            observe_scope: None,
            request: &mut unreachable_request,
            work,
        },
    )
}

#[derive(Clone, Copy)]
enum BindingSource<'a> {
    Table(&'a wire::ExecutionTypeTable),
    Schema {
        columns: &'a [wire::ArrowPhysicalColumn],
        logical_types: &'a [logical_wire::LogicalTypeDefinition],
    },
}
impl<'a> BindingSource<'a> {
    fn logical_types(self) -> &'a [logical_wire::LogicalTypeDefinition] {
        match self {
            Self::Table(table) => &table.logical_types,
            Self::Schema { logical_types, .. } => logical_types,
        }
    }
    fn len(self) -> usize {
        match self {
            Self::Table(table) => table.value_bindings.len(),
            Self::Schema { columns, .. } => columns.len(),
        }
    }
    fn root(self, ordinal: usize) -> BorrowedBinding<'a> {
        match self {
            Self::Table(table) => BorrowedBinding::Value(&table.value_bindings[ordinal]),
            Self::Schema { columns, .. } => BorrowedBinding::Field(&columns[ordinal]),
        }
    }
}
#[derive(Clone, Copy)]
enum BorrowedBinding<'a> {
    Value(&'a wire::ExecutionValueBinding),
    Field(&'a wire::ArrowPhysicalColumn),
}
impl<'a> BorrowedBinding<'a> {
    fn id(self) -> Option<u32> {
        match self {
            Self::Value(value) => value.id,
            Self::Field(column) => Some(column.slot_id),
        }
    }
    fn logical_id(self) -> Option<u32> {
        match self {
            Self::Value(value) => value.logical_type_id,
            Self::Field(column) => column
                .field
                .as_ref()
                .and_then(|field| field.logical_type_id),
        }
    }
    fn layout(self) -> Option<&'a wire::ArrowPhysicalType> {
        match self {
            Self::Value(value) => value.value_layout.as_ref(),
            Self::Field(column) => column
                .field
                .as_ref()
                .and_then(|field| field.r#type.as_deref()),
        }
    }
}

fn decode_execution_type_table_core<
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    source: BindingSource<'_>,
    limits: ExecutionTypeTableLimits,
    storage: ValueTypeStorageLifetime,
    factory: Option<&F>,
    context: &mut Context<'_, '_, H>,
) -> Result<DecodedExecutionTypeTable, Failure<H>> {
    resources::with_entity(factory, &storage, context, |storage, context| {
        context.step()?;
        if source.len() > limits.max_bindings {
            return Err(invalid("execution binding count exceeds caller profile"));
        }
        let logical = value_type_table::decode::decode_with_factory(
            source.logical_types(),
            limits.logical,
            storage.clone(),
            factory,
            context,
        )?;
        let mut definitions = DefinitionIndex::new(source.logical_types(), context)?;
        let mut ordered = context.vector::<(u32, BorrowedBinding<'_>)>(source.len())?;
        for ordinal in 0..source.len() {
            context.step()?;
            let binding = source.root(ordinal);
            ordered.push((
                binding
                    .id()
                    .ok_or_else(|| invalid("missing execution binding ID"))?,
                binding,
            ));
        }
        sort_by_id(&mut ordered, |v| v.0, context)?;
        for pair in ordered.windows(2) {
            context.step()?;
            if pair[0].0 == pair[1].0 {
                return Err(invalid("duplicate execution binding ID"));
            }
        }
        // Every binding is validated before constructing any physical target tree.
        let mut counts = layout::Counts::default();
        for (_, binding) in &ordered {
            context.step()?;
            let id = binding
                .logical_id()
                .ok_or_else(|| invalid("missing execution logical reference"))?;
            let value = binding
                .layout()
                .ok_or_else(|| invalid("missing execution value layout"))?;
            match binding {
                BorrowedBinding::Value(_) => layout::validate(
                    value,
                    id,
                    &mut definitions,
                    &logical,
                    ArrowTypeDepth::ROOT,
                    &mut counts,
                    limits,
                    context,
                )?,
                BorrowedBinding::Field(column) => layout::validate_schema_root(
                    column
                        .field
                        .as_ref()
                        .ok_or_else(|| invalid("missing execution schema root field"))?,
                    &mut definitions,
                    &logical,
                    &mut counts,
                    limits,
                    context,
                )?,
            }
        }
        let mut logical_source_bytes = 0usize;
        for &(id, _, retained) in &definitions.entries {
            if retained {
                let bytes = logical
                    .retained_node_source_bytes_observed(id, context.work)?
                    .ok_or_else(|| invalid("validated logical source receipt is absent"))?;
                resources::source_add(&mut logical_source_bytes, bytes)?;
            }
        }
        let ((values, mut retained_source_bytes, container_storage), scope_bytes) =
            resources::with_entity_source_facts(
                factory,
                &storage,
                context,
                |container_storage, context| {
                    let mut values = context.vector(ordered.len())?;
                    let mut retained_source_bytes = logical_source_bytes;
                    for (id, binding) in ordered {
                        context.step()?;
                        let logical_id = binding.logical_id().expect("validated logical reference");
                        let value = resources::with_entity(
                            factory,
                            &container_storage,
                            context,
                            |storage, context| {
                                let created_layout = layout::decode(
                                    binding.layout().expect("validated layout"),
                                    factory,
                                    &storage,
                                    context,
                                )?;
                                context.step()?;
                                context.work.flush()?;
                                let value_type = logical
                                    .get_observed(logical_id, context.work)?
                                    .expect("validated logical reference")
                                    .clone();
                                context.work.flush()?;
                                let source_bytes = created_layout.retained_source_bytes()?;
                                let typed = TypedValueBinding::try_new_observed(
                                    value_type,
                                    created_layout.value,
                                    |units| {
                                        for _ in 0..units {
                                            context.step()?;
                                        }
                                        Ok::<_, Failure<H>>(())
                                    },
                                )?;
                                context.step()?;
                                context.work.flush()?;
                                let value = FunctionValueType::try_from_typed_binding(typed)
                                    .map_err(|error| {
                                        Failure::Codec(LogicalTypeCodecError::ValueType(
                                    novarocks_type_contract::ExecutionValueTypeError::Logical(
                                        error,
                                    ),
                                ))
                                    })?;
                                Ok((value, source_bytes))
                            },
                        )?;
                        resources::source_add(&mut retained_source_bytes, value.1)?;
                        values.push((id, value.0));
                    }
                    resources::source_add(
                        &mut retained_source_bytes,
                        resources::array::<(u32, FunctionValueType), H>(values.capacity())?.size(),
                    )?;
                    Ok((values, retained_source_bytes, container_storage))
                },
            )?;
        resources::source_add(&mut retained_source_bytes, scope_bytes)?;
        Ok(DecodedExecutionTypeTable {
            values,
            _storage: container_storage,
            retained_source_bytes,
        })
    })
}

/// Borrowed definitions, sorted using observed O(n log n) work and O(n) storage.
struct DefinitionIndex<'a> {
    entries: Vec<(u32, &'a logical_wire::LogicalTypeDefinition, bool)>,
}
impl<'a> DefinitionIndex<'a> {
    fn new<H>(
        defs: &'a [logical_wire::LogicalTypeDefinition],
        context: &mut Context<'_, '_, H>,
    ) -> Result<Self, Failure<H>> {
        let mut entries = context.vector(defs.len())?;
        for def in defs {
            context.step()?;
            entries.push((
                def.id
                    .ok_or_else(|| invalid("missing logical definition ID"))?,
                def,
                false,
            ));
        }
        sort_by_id(&mut entries, |v| v.0, context)?;
        Ok(Self { entries })
    }
    fn get_retained<H>(
        &mut self,
        id: u32,
        context: &mut Context<'_, '_, H>,
    ) -> Result<&'a logical_wire::LogicalTypeDefinition, Failure<H>> {
        let index = find(&self.entries, id, |v| v.0, context.work)?
            .ok_or_else(|| invalid("dangling layout logical reference"))?;
        self.entries[index].2 = true;
        Ok(self.entries[index].1)
    }
    fn get<H>(
        &self,
        id: u32,
        context: &mut Context<'_, '_, H>,
    ) -> Result<&'a logical_wire::LogicalTypeDefinition, Failure<H>> {
        let index = find(&self.entries, id, |v| v.0, context.work)?
            .ok_or_else(|| invalid("dangling layout logical reference"))?;
        Ok(self.entries[index].1)
    }
}
