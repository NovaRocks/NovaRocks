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

//! Borrowed package root-source enumeration, not a domain grant or decoder.
//! No graph lookup, root legality, reference proof, writer recipe validation,
//! allocation request or MEM admission is supplied by this component.

use super::TypeCodecError;
use novarocks_proto_models::{physical_package_v2 as raw, physical_type_v2 as types};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriterTypeRootRole {
    Data,
    RowLineageData,
    RowLineageIdentity,
    PositionDeleteIdentity,
    PositionDeletePartition,
    DeletionVectorIdentity,
    DeletionVectorPartition,
    EqualityDelete,
}

#[derive(Clone, Copy, Debug)]
pub enum PackageTypeRootSource<'source> {
    Value(&'source types::ValueTypeDefinition),
    SchemaField {
        schema: &'source raw::SchemaDefinition,
        ordinal: usize,
        field_id: u32,
    },
    IpcField {
        pool: &'source raw::IpcConstantPool,
        field_id: u32,
    },
    WriterField {
        recipe: &'source raw::FrozenWriterRecipe,
        role: WriterTypeRootRole,
        ordinal: usize,
        binding: &'source raw::ConnectorWriteFieldBinding,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackageTypeRootSourceFacts {
    pub value_root_count: usize,
    pub strict_field_root_count: usize,
    pub writer_recipe_count: usize,
    pub writer_field_root_count: usize,
    /// Necessary original inline/visited Vec backing only, never complete B.
    pub source_floor: usize,
    /// Covers this enumerator's one preparation and one full visit only.
    /// Visitor graph lookups, validation and other work belong to the caller.
    /// A repeated visit requires another caller-admitted contribution.
    pub cumulative_work_upper_bound: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TypeNamespace {
    Execution,
    Source,
}
impl TypeNamespace {
    pub(super) fn table(
        self,
        package: &raw::FragmentPackage,
    ) -> Result<&types::TypeTable, TypeCodecError> {
        match self {
            Self::Execution => package.types.as_ref().ok_or(TypeCodecError::InvalidShape(
                "package execution types are absent",
            )),
            Self::Source => package
                .source_fields
                .as_ref()
                .and_then(|owner| owner.shape.as_ref())
                .ok_or(TypeCodecError::InvalidShape(
                    "package source field shape is absent",
                )),
        }
    }
}
pub struct PackageTypeRootSources<'source> {
    namespace: TypeNamespace,
    package: &'source raw::FragmentPackage,
    facts: PackageTypeRootSourceFacts,
}

type RoleGroups<'a> = [Option<(WriterTypeRootRole, &'a Vec<raw::ConnectorWriteFieldBinding>)>; 2];

// One closed role decomposition is reused by preparation and visitation. It
// preserves original optional IDs/tokens rather than authenticating them.
fn writer_roles(recipe: &raw::FrozenWriterRecipe) -> Result<RoleGroups<'_>, TypeCodecError> {
    use WriterTypeRootRole::*;
    use raw::connector_write_input_shape::Kind;
    let input = recipe.input.as_ref().ok_or(TypeCodecError::InvalidShape(
        "writer type roots input is absent",
    ))?;
    Ok(
        match input.kind.as_ref().ok_or(TypeCodecError::InvalidShape(
            "writer type roots input kind is absent",
        ))? {
            Kind::Data(input) => [Some((Data, &input.fields)), None],
            Kind::RowLineage(input) => [
                Some((RowLineageData, &input.data_fields)),
                Some((RowLineageIdentity, &input.row_identity_fields)),
            ],
            Kind::PositionDelete(input) => [
                Some((PositionDeleteIdentity, &input.identity_fields)),
                Some((PositionDeletePartition, &input.partition_source_fields)),
            ],
            Kind::DeletionVector(input) => [
                Some((DeletionVectorIdentity, &input.identity_fields)),
                Some((DeletionVectorPartition, &input.partition_source_fields)),
            ],
            Kind::EqualityDelete(input) => [Some((EqualityDelete, &input.equality_fields)), None],
        },
    )
}

fn add(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_add(right)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
fn mul(left: usize, right: usize) -> Result<usize, TypeCodecError> {
    left.checked_mul(right)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}
fn backing<T>(values: &Vec<T>) -> Result<usize, TypeCodecError> {
    mul(values.capacity(), size_of::<T>())
}
fn gate<H>(
    facts: &mut PackageTypeRootSourceFacts,
    package: &raw::FragmentPackage,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(
        &PackageTypeRootSourceFacts,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
) -> Result<(), crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>> {
    // Closed scalar/header bookkeeping, preparation plus one full visit.
    // This is not fabricated graph work or a cooperative library-operation bound.
    let mut units = 128;
    for (count, multiplier) in [
        (facts.value_root_count, 16),
        (facts.strict_field_root_count, 16),
        (package.schemas.len(), 128),
        (package.constants.len(), 128),
        (facts.writer_recipe_count, 256),
        (facts.writer_field_root_count, 32),
    ] {
        units = add(units, mul(count, multiplier)?)?;
    }
    facts.cumulative_work_upper_bound = units;
    admit(facts)?;
    if facts.source_floor > source_retained_bytes {
        return Err(TypeCodecError::InvalidShape(
            "package type roots source invoice is understated",
        )
        .into());
    }
    Ok(())
}

/// Borrow the original raw owner and enumerate source classifications only.
/// The caller supplies its whole original-source invoice and owns its scope.
/// Known arithmetic/work refusals return before any later checkpoint. No type
/// tree walk, scratch initialization, allocation, copy, or control is created.
pub(super) fn prepare_package_type_root_sources_with_host<'source, H>(
    package: &'source raw::FragmentPackage,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(
        &PackageTypeRootSourceFacts,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    PackageTypeRootSources<'source>,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
> {
    prepare_package_type_root_sources_namespace_with_host(
        package,
        TypeNamespace::Execution,
        source_retained_bytes,
        admit,
        work,
    )
}

pub(super) fn prepare_package_type_root_sources_namespace_with_host<'source, H>(
    package: &'source raw::FragmentPackage,
    namespace: TypeNamespace,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(
        &PackageTypeRootSourceFacts,
    ) -> Result<
        (),
        crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
    >,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    PackageTypeRootSources<'source>,
    crate::host_projection_v2::ProjectionFailure<TypeCodecError, H>,
> {
    let mut facts = PackageTypeRootSourceFacts {
        value_root_count: namespace
            .table(package)
            .map_or(0, |table| table.value_types.len()),
        writer_recipe_count: package.writes.len(),
        source_floor: size_of::<raw::FragmentPackage>(),
        ..Default::default()
    };
    for bytes in [
        backing(&package.schemas)?,
        backing(&package.constants)?,
        backing(&package.writes)?,
    ] {
        facts.source_floor = add(facts.source_floor, bytes)?;
    }
    if let Ok(table) = namespace.table(package) {
        facts.source_floor = add(facts.source_floor, backing(&table.value_types)?)?;
    }
    gate(&mut facts, package, source_retained_bytes, admit)?;
    let table = namespace.table(package);
    work.step()?;
    table?;
    for schema in if namespace == TypeNamespace::Execution {
        package.schemas.as_slice()
    } else {
        &[]
    } {
        facts.strict_field_root_count = add(facts.strict_field_root_count, schema.field_ids.len())?;
        facts.source_floor = add(facts.source_floor, backing(&schema.field_ids)?)?;
        gate(&mut facts, package, source_retained_bytes, admit)?;
        work.step()?;
    }
    for pool in if namespace == TypeNamespace::Source {
        package.constants.as_slice()
    } else {
        &[]
    } {
        if pool.source_field_id.is_some() {
            facts.strict_field_root_count = add(facts.strict_field_root_count, 1)?;
        }
        gate(&mut facts, package, source_retained_bytes, admit)?;
        work.step()?;
    }
    for recipe in if namespace == TypeNamespace::Source {
        package.writes.as_slice()
    } else {
        &[]
    } {
        let groups = writer_roles(recipe);
        // A successful closed shape lookup already exposes both role headers.
        // Admit all their known counts/backing before observing that lookup,
        // so a pending quantum cannot replace its originating resource refusal.
        if let Ok(groups) = &groups {
            for (_, fields) in groups.iter().flatten() {
                facts.writer_field_root_count = add(facts.writer_field_root_count, fields.len())?;
                facts.source_floor = add(facts.source_floor, backing(fields)?)?;
            }
        }
        gate(&mut facts, package, source_retained_bytes, admit)?;
        work.step()?;
        // An ordinary shape error retains its completed lookup observation.
        for _ in groups?.into_iter().flatten() {
            work.step()?;
        }
    }
    Ok(PackageTypeRootSources {
        namespace,
        package,
        facts,
    })
}

pub fn prepare_package_type_root_sources<'source>(
    package: &'source raw::FragmentPackage,
    source_retained_bytes: usize,
    admit: &mut impl FnMut(&PackageTypeRootSourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PackageTypeRootSources<'source>, TypeCodecError> {
    prepare_package_type_root_sources_with_host::<std::convert::Infallible>(
        package,
        source_retained_bytes,
        &mut |facts| admit(facts).map_err(Into::into),
        work,
    )
    .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
}

impl<'source> PackageTypeRootSources<'source> {
    pub fn facts(&self) -> PackageTypeRootSourceFacts {
        self.facts
    }

    /// The actual raw owner, never equality of a foreign clone, binds this loan.
    pub fn table_for(
        &self,
        package: &raw::FragmentPackage,
    ) -> Result<&'source types::TypeTable, TypeCodecError> {
        if !std::ptr::eq(self.package, package) {
            return Err(TypeCodecError::InvalidShape(
                "package type roots belong to another raw package",
            ));
        }
        self.namespace.table(self.package)
    }

    /// No own entry/footer. One admitted visit emits source order and repeated
    /// occurrences; the visitor owns its own additional work and contributions.
    pub fn visit<E: From<TypeCodecError>>(
        &self,
        visitor: &mut impl FnMut(
            PackageTypeRootSource<'source>,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        fn emit<'a, E: From<TypeCodecError>>(
            source: PackageTypeRootSource<'a>,
            visitor: &mut impl FnMut(
                PackageTypeRootSource<'a>,
                &mut CompileCheckpoints<'_>,
            ) -> Result<(), E>,
            work: &mut CompileCheckpoints<'_>,
        ) -> Result<(), E> {
            work.step().map_err(TypeCodecError::from).map_err(E::from)?;
            visitor(source, work)
        }
        let table = self.table_for(self.package).map_err(E::from)?;
        for value in &table.value_types {
            emit(PackageTypeRootSource::Value(value), visitor, work)?;
        }
        for schema in if self.namespace == TypeNamespace::Execution {
            self.package.schemas.as_slice()
        } else {
            &[]
        } {
            work.step().map_err(TypeCodecError::from).map_err(E::from)?;
            for (ordinal, &field_id) in schema.field_ids.iter().enumerate() {
                emit(
                    PackageTypeRootSource::SchemaField {
                        schema,
                        ordinal,
                        field_id,
                    },
                    visitor,
                    work,
                )?;
            }
        }
        for pool in if self.namespace == TypeNamespace::Source {
            self.package.constants.as_slice()
        } else {
            &[]
        } {
            let field_id = pool.source_field_id;
            work.step().map_err(TypeCodecError::from).map_err(E::from)?;
            if let Some(field_id) = field_id {
                emit(
                    PackageTypeRootSource::IpcField { pool, field_id },
                    visitor,
                    work,
                )?;
            }
        }
        for recipe in if self.namespace == TypeNamespace::Source {
            self.package.writes.as_slice()
        } else {
            &[]
        } {
            let groups = writer_roles(recipe);
            work.step().map_err(TypeCodecError::from).map_err(E::from)?;
            for (role, fields) in groups.map_err(E::from)?.into_iter().flatten() {
                work.step().map_err(TypeCodecError::from).map_err(E::from)?;
                for (ordinal, binding) in fields.iter().enumerate() {
                    emit(
                        PackageTypeRootSource::WriterField {
                            recipe,
                            role,
                            ordinal,
                            binding,
                        },
                        visitor,
                        work,
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];
    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl Control {
        fn refusing(at: usize, cause: CompileControlError) -> Self {
            Self {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause)),
            }
        }
        fn trace(&self) -> Vec<u32> {
            self.trace.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Decode);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push(units);
            if let Some((refusal, cause)) = self.refusal.clone()
                && refusal == at
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn field(id: Option<u32>) -> raw::ConnectorWriteFieldBinding {
        raw::ConnectorWriteFieldBinding {
            storage_value_type_id: None,
            field_token: vec![],
            source_field_id: id,
            value_type_id: None,
        }
    }
    fn recipe(kind: raw::connector_write_input_shape::Kind) -> raw::FrozenWriterRecipe {
        raw::FrozenWriterRecipe {
            node_id: None,
            provider_binding_id: None,
            handle_payload_id: None,
            input: Some(raw::ConnectorWriteInputShape { kind: Some(kind) }),
        }
    }
    fn prepare_source_roots<'s>(
        package: &'s raw::FragmentPackage,
        source: usize,
        admit: &mut impl FnMut(&PackageTypeRootSourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<PackageTypeRootSources<'s>, TypeCodecError> {
        prepare_package_type_root_sources_namespace_with_host::<std::convert::Infallible>(
            package,
            TypeNamespace::Source,
            source,
            &mut |facts| admit(facts).map_err(Into::into),
            work,
        )
        .map_err(crate::host_projection_v2::ProjectionFailure::without_host)
    }
    fn fixture() -> raw::FragmentPackage {
        use raw::connector_write_input_shape::Kind;
        raw::FragmentPackage {
            source_fields: Some(types::SourceFieldTable {
                shape: Some(types::TypeTable::default()),
                field_facts: vec![],
            }),
            types: Some(types::TypeTable {
                value_types: vec![
                    types::ValueTypeDefinition {
                        id: 0,
                        ..Default::default()
                    },
                    types::ValueTypeDefinition {
                        id: u32::MAX,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }),
            schemas: vec![raw::SchemaDefinition {
                id: 0,
                field_ids: vec![0, u32::MAX, 0],
            }],
            constants: vec![
                raw::IpcConstantPool {
                    id: 0,
                    source_field_id: Some(0),
                    ..Default::default()
                },
                raw::IpcConstantPool {
                    id: 7,
                    source_field_id: None,
                    ..Default::default()
                },
                raw::IpcConstantPool {
                    id: u32::MAX,
                    source_field_id: Some(u32::MAX),
                    ..Default::default()
                },
            ],
            writes: vec![
                recipe(Kind::Data(raw::ConnectorWriteDataInput {
                    fields: vec![field(Some(0)), field(Some(u32::MAX))],
                })),
                recipe(Kind::RowLineage(raw::ConnectorWriteRowLineageInput {
                    data_fields: vec![field(Some(11))],
                    row_identity_fields: vec![field(None)],
                })),
                recipe(Kind::PositionDelete(
                    raw::ConnectorWritePositionDeleteInput {
                        identity_fields: vec![field(Some(12))],
                        partition_source_fields: vec![field(Some(13))],
                    },
                )),
                recipe(Kind::DeletionVector(
                    raw::ConnectorWriteDeletionVectorInput {
                        identity_fields: vec![field(Some(14))],
                        partition_source_fields: vec![field(Some(15))],
                    },
                )),
                recipe(Kind::EqualityDelete(
                    raw::ConnectorWriteEqualityDeleteInput {
                        equality_fields: vec![field(Some(16))],
                    },
                )),
            ],
            ..Default::default()
        }
    }
    fn invoke(
        package: &raw::FragmentPackage,
        control: &Control,
    ) -> Result<PackageTypeRootSourceFacts, TypeCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let result = (|| {
            let token = prepare_source_roots(package, usize::MAX, &mut |_| Ok(()), &mut work)?;
            token.visit::<TypeCodecError>(&mut |_, _| Ok(()), &mut work)?;
            Ok(token.facts())
        })();
        if matches!(result, Err(TypeCodecError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn assert_control(error: TypeCodecError, cause: CompileControlError) {
        assert!(matches!(error, TypeCodecError::Control(actual) if actual == cause));
    }

    #[test]
    fn ordered_sources_preserve_all_five_writer_roles_strict_roots_and_raw_presence() {
        let package = fixture();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let token = prepare_source_roots(&package, usize::MAX, &mut |_| Ok(()), &mut work).unwrap();
        assert!(std::ptr::eq(
            token.table_for(&package).unwrap(),
            package
                .source_fields
                .as_ref()
                .unwrap()
                .shape
                .as_ref()
                .unwrap()
        ));
        let foreign = package.clone();
        assert!(matches!(
            token.table_for(&foreign),
            Err(TypeCodecError::InvalidShape(
                "package type roots belong to another raw package"
            ))
        ));
        let mut events = Vec::new();
        token
            .visit::<TypeCodecError>(
                &mut |source, _| {
                    let key = match source {
                        PackageTypeRootSource::Value(value) => {
                            assert!(
                                package
                                    .types
                                    .as_ref()
                                    .unwrap()
                                    .value_types
                                    .iter()
                                    .any(|original| std::ptr::eq(original, value))
                            );
                            ("value", value.id as u64, 0, None)
                        }
                        PackageTypeRootSource::SchemaField {
                            schema,
                            ordinal,
                            field_id,
                        } => {
                            assert!(std::ptr::eq(schema, &package.schemas[0]));
                            ("schema", schema.id as u64, ordinal, Some(field_id))
                        }
                        PackageTypeRootSource::IpcField { pool, field_id } => {
                            assert!(
                                package
                                    .constants
                                    .iter()
                                    .any(|original| std::ptr::eq(original, pool))
                            );
                            ("ipc", pool.id as u64, 0, Some(field_id))
                        }
                        PackageTypeRootSource::WriterField {
                            recipe,
                            role,
                            ordinal,
                            binding,
                        } => {
                            let recipe_id = package
                                .writes
                                .iter()
                                .position(|original| std::ptr::eq(original, recipe))
                                .unwrap();
                            assert!(
                                writer_roles(recipe)
                                    .unwrap()
                                    .into_iter()
                                    .flatten()
                                    .any(|(actual, fields)| actual == role
                                        && std::ptr::eq(&fields[ordinal], binding))
                            );
                            (
                                match role {
                                    WriterTypeRootRole::Data => "data",
                                    WriterTypeRootRole::RowLineageData => "row-data",
                                    WriterTypeRootRole::RowLineageIdentity => "row-id",
                                    WriterTypeRootRole::PositionDeleteIdentity => "position-id",
                                    WriterTypeRootRole::PositionDeletePartition => {
                                        "position-partition"
                                    }
                                    WriterTypeRootRole::DeletionVectorIdentity => "dv-id",
                                    WriterTypeRootRole::DeletionVectorPartition => "dv-partition",
                                    WriterTypeRootRole::EqualityDelete => "equality",
                                },
                                recipe_id as u64,
                                ordinal,
                                binding.source_field_id,
                            )
                        }
                    };
                    events.push(key);
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        assert_eq!(
            events,
            [
                ("ipc", 0, 0, Some(0)),
                ("ipc", u32::MAX as u64, 0, Some(u32::MAX)),
                ("data", 0, 0, Some(0)),
                ("data", 0, 1, Some(u32::MAX)),
                ("row-data", 1, 0, Some(11)),
                ("row-id", 1, 0, None),
                ("position-id", 2, 0, Some(12)),
                ("position-partition", 2, 0, Some(13)),
                ("dv-id", 3, 0, Some(14)),
                ("dv-partition", 3, 0, Some(15)),
                ("equality", 4, 0, Some(16)),
            ]
        );
        let execution = super::prepare_package_type_root_sources(
            &package,
            usize::MAX,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        let mut execution_events = Vec::new();
        execution
            .visit::<TypeCodecError>(
                &mut |root, _| {
                    execution_events.push(match root {
                        PackageTypeRootSource::Value(value) => ("value", value.id),
                        PackageTypeRootSource::SchemaField { field_id, .. } => ("schema", field_id),
                        _ => panic!("Source occurrence escaped into Execution namespace"),
                    });
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        assert_eq!(
            execution_events,
            [
                ("value", 0),
                ("value", u32::MAX),
                ("schema", 0),
                ("schema", u32::MAX),
                ("schema", 0)
            ]
        );
        assert_eq!(execution.facts().writer_field_root_count, 0);
        let facts = token.facts();
        assert_eq!(facts.value_root_count, 0);
        assert_eq!(facts.strict_field_root_count, 2);
        assert_eq!(facts.writer_recipe_count, 5);
        assert_eq!(facts.writer_field_root_count, 9);
        work.finish().unwrap();
    }

    #[test]
    fn missing_table_input_and_kind_are_ordinary_and_not_domain_validation() {
        let control = Control::default();
        let mut package = raw::FragmentPackage::default();
        assert!(matches!(
            invoke(&package, &control),
            Err(TypeCodecError::InvalidShape(
                "package source field shape is absent"
            ))
        ));
        package.types = Some(types::TypeTable::default());
        package.source_fields = Some(types::SourceFieldTable {
            shape: Some(types::TypeTable::default()),
            field_facts: vec![],
        });
        package.writes = vec![raw::FrozenWriterRecipe::default()];
        assert!(matches!(
            invoke(&package, &Control::default()),
            Err(TypeCodecError::InvalidShape(
                "writer type roots input is absent"
            ))
        ));
        package.writes[0].input = Some(raw::ConnectorWriteInputShape::default());
        assert!(matches!(
            invoke(&package, &Control::default()),
            Err(TypeCodecError::InvalidShape(
                "writer type roots input kind is absent"
            ))
        ));
        // Unknown references/absent Writer field IDs/tokens are retained sources,
        // deliberately not type-root legality or a recipe admission proof.
        assert!(invoke(&fixture(), &Control::default()).is_ok());
    }

    #[test]
    fn source_floor_is_visited_inline_and_capacity_only_with_exact_and_one_under() {
        let mut package = fixture();
        package.schemas.reserve_exact(7);
        package.schemas[0].field_ids.reserve_exact(19);
        let expected = size_of::<raw::FragmentPackage>()
            + package.source_fields.as_ref().unwrap().shape.as_ref().unwrap().value_types.capacity()
                * size_of::<types::ValueTypeDefinition>()
            + package.schemas.capacity() * size_of::<raw::SchemaDefinition>()
            // Schema field backing belongs to the separate Execution enumeration.
            + package.constants.capacity() * size_of::<raw::IpcConstantPool>()
            + package.writes.capacity() * size_of::<raw::FrozenWriterRecipe>()
            + package
                .writes
                .iter()
                .flat_map(|recipe| writer_roles(recipe).unwrap().into_iter().flatten())
                .map(|(_, fields)| fields.capacity() * size_of::<raw::ConnectorWriteFieldBinding>())
                .sum::<usize>();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let token = prepare_source_roots(&package, expected, &mut |_| Ok(()), &mut work).unwrap();
        assert_eq!(token.facts().source_floor, expected);
        let bound = token.facts().cumulative_work_upper_bound;
        for (cap, accepted) in [(bound, true), (bound - 1, false)] {
            let mut replay = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let result = prepare_source_roots(
                &package,
                expected,
                &mut |facts| {
                    if facts.cumulative_work_upper_bound > cap {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut replay,
            );
            if accepted {
                assert_eq!(result.unwrap().facts(), token.facts());
            } else {
                assert_control(
                    result.err().unwrap(),
                    CompileControlError::ResourceExhausted,
                );
            }
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert!(matches!(
            prepare_source_roots(&package, expected - 1, &mut |_| Ok(()), &mut work),
            Err(TypeCodecError::InvalidShape(
                "package type roots source invoice is understated"
            ))
        ));
    }

    #[test]
    fn known_initial_work_refusal_wins_before_pending_255_late_control() {
        let package = raw::FragmentPackage {
            source_fields: Some(types::SourceFieldTable {
                shape: Some(types::TypeTable::default()),
                field_facts: vec![],
            }),
            types: Some(types::TypeTable::default()),
            ..Default::default()
        };
        for cause in CAUSES {
            let control = Control::refusing(1, cause);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let source = [3u8; 255];
            let mut copied = [0u8; 255];
            for (source, dest) in source.iter().zip(&mut copied) {
                *dest = *source;
                work.step().unwrap();
            }
            assert_eq!(copied, [3u8; 255]);
            let result = prepare_source_roots(
                &package,
                usize::MAX,
                &mut |facts| {
                    if facts.cumulative_work_upper_bound > 127 {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            );
            assert_control(
                result.err().unwrap(),
                CompileControlError::ResourceExhausted,
            );
            assert_eq!(control.trace(), [0]);
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        assert_eq!(
            prepare_source_roots(
                &package,
                usize::MAX,
                &mut |facts| if facts.cumulative_work_upper_bound > 128 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                },
                &mut work
            )
            .unwrap()
            .facts()
            .cumulative_work_upper_bound,
            128
        );
        work.finish().unwrap();
    }

    #[test]
    fn known_writer_role_headers_win_before_pending_254_shape_completion() {
        use raw::connector_write_input_shape::Kind;
        for kind in [
            Kind::Data(raw::ConnectorWriteDataInput {
                fields: (0..320).map(|_| field(Some(0))).collect(),
            }),
            Kind::RowLineage(raw::ConnectorWriteRowLineageInput {
                data_fields: vec![field(Some(0))],
                row_identity_fields: (0..320).map(|_| field(Some(u32::MAX))).collect(),
            }),
        ] {
            let package = raw::FragmentPackage {
                source_fields: Some(types::SourceFieldTable {
                    shape: Some(types::TypeTable::default()),
                    field_facts: vec![],
                }),
                types: Some(types::TypeTable::default()),
                writes: vec![recipe(kind)],
                ..Default::default()
            };
            for cause in CAUSES {
                let control = Control::refusing(1, cause);
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
                let source = [7_u8; 254];
                let mut copied = [0_u8; 254];
                for (source, target) in source.iter().zip(&mut copied) {
                    *target = *source;
                    work.step().unwrap();
                }
                assert_eq!(copied, source);
                // Initial header bound is 128 + 256 = 384. Once the shape
                // reveals at least 320 fields it is >=384 + 320*32 = 10624,
                // including the second role's contribution, before the
                // shape-completion step would observe the 256th actual unit.
                let result = prepare_source_roots(
                    &package,
                    usize::MAX,
                    &mut |facts| {
                        if facts.cumulative_work_upper_bound > 1000 {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    },
                    &mut work,
                );
                assert_control(
                    result.err().unwrap(),
                    CompileControlError::ResourceExhausted,
                );
                assert_eq!(control.trace(), [0]);
            }
        }
    }

    #[test]
    fn every_actual_small_success_and_ordinary_callback_keeps_primary_causes() {
        for package in [fixture(), raw::FragmentPackage::default()] {
            let baseline = Control::default();
            let _ = invoke(&package, &baseline);
            let trace = baseline.trace();
            assert!(!trace.is_empty());
            for at in 0..trace.len() {
                for cause in CAUSES {
                    let control = Control::refusing(at, cause.clone());
                    assert_control(invoke(&package, &control).unwrap_err(), cause);
                    assert_eq!(control.trace(), trace[..=at]);
                }
            }
        }
    }

    #[test]
    fn actual_320_writer_bindings_visit_has_bounded_quantum_and_exact_repeated_ordinals() {
        use raw::connector_write_input_shape::Kind;
        let package = raw::FragmentPackage {
            source_fields: Some(types::SourceFieldTable {
                shape: Some(types::TypeTable::default()),
                field_facts: vec![],
            }),
            types: Some(types::TypeTable::default()),
            writes: vec![recipe(Kind::Data(raw::ConnectorWriteDataInput {
                fields: (0..320).map(|_| field(Some(u32::MAX))).collect(),
            }))],
            ..Default::default()
        };
        let baseline = Control::default();
        let facts = invoke(&package, &baseline).unwrap();
        assert_eq!(facts.writer_field_root_count, 320);
        let trace = baseline.trace();
        assert!(trace.contains(&256));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control::refusing(at, cause.clone());
                assert_control(invoke(&package, &control).unwrap_err(), cause);
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let token = prepare_source_roots(&package, usize::MAX, &mut |_| Ok(()), &mut work).unwrap();
        let mut visited = 0;
        token
            .visit::<TypeCodecError>(
                &mut |source, _| {
                    if let PackageTypeRootSource::WriterField {
                        role,
                        ordinal,
                        binding,
                        ..
                    } = source
                    {
                        assert_eq!(role, WriterTypeRootRole::Data);
                        assert_eq!(ordinal, visited);
                        assert_eq!(binding.source_field_id, Some(u32::MAX));
                        visited += 1;
                    } else {
                        panic!("unexpected source class");
                    }
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        assert_eq!(visited, 320);
    }

    #[test]
    fn visitor_ordinary_error_is_preserved_without_an_enumerator_footer_or_second_event() {
        enum VisitorError {
            Type(TypeCodecError),
            Provider(u32),
        }
        impl From<TypeCodecError> for VisitorError {
            fn from(error: TypeCodecError) -> Self {
                Self::Type(error)
            }
        }
        let package = fixture();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let token = prepare_source_roots(&package, usize::MAX, &mut |_| Ok(()), &mut work).unwrap();
        let before = control.trace();
        let mut visits = 0;
        let result = token.visit::<VisitorError>(
            &mut |_, _| {
                visits += 1;
                Err(VisitorError::Provider(711))
            },
            &mut work,
        );
        assert!(matches!(result, Err(VisitorError::Provider(711))));
        assert_eq!(visits, 1);
        assert_eq!(control.trace(), before);
        // A separate caller admits another visit, then performs 255 real
        // stack byte copies. The first event step is the next actual quantum.
        let control = Control::refusing(1, CompileControlError::Cancelled);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let source = [9_u8; 255];
        let mut copied = [0_u8; 255];
        for (source, target) in source.iter().zip(&mut copied) {
            *target = *source;
            work.step().unwrap();
        }
        assert_eq!(copied, [9_u8; 255]);
        assert!(token.facts().cumulative_work_upper_bound <= 1 << 20);
        let result = token.visit::<VisitorError>(
            &mut |_, _| {
                visits += 1;
                Ok(())
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(VisitorError::Type(TypeCodecError::Control(
                CompileControlError::Cancelled
            )))
        ));
        assert_eq!(visits, 1);
        assert_eq!(control.trace(), [0, 256]);
    }
}
