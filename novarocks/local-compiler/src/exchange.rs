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

//! Lower one admitted stream receiver into the existing ExchangeSource owner.
//! The receiver's layout ordinal `i` is the cut's import `i`; the address is
//! taken from the exact inbound cut, never from a legacy native node ID.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use novarocks_local_program::{CompiledExchangeInput, ProgramNodeKind, StaticLayout};
use novarocks_physical_plan::{Distribution, EdgeKind, FragmentPackage, NodeKind, PhysicalNode};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use novarocks_types::SlotId;

pub(crate) struct LoweredExchangeSource {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub input: CompiledExchangeInput,
}

pub(crate) fn lower_exchange_source_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &[SlotId],
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredExchangeSource, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(package, node, slots, &mut work, creation);
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H>(
    package: &FragmentPackage,
    node: &PhysicalNode,
    slots: &[SlotId],
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredExchangeSource, crate::FragmentCreationError<H>> {
    let NodeKind::ExchangeSource { edge, imports } = &node.kind else {
        return Err((FragmentCompileError::Invalid("ExchangeSource kind differs")).into());
    };
    if !node.inputs.is_empty()
        || slots.len() != node.output.columns.len()
        || imports.len() != node.output.columns.len()
    {
        return Err((FragmentCompileError::Invalid(
            "ExchangeSource input, import or channel width differs",
        ))
        .into());
    }
    let mut found = None;
    for cut in package.cuts().inbound.iter() {
        let addressed = cut.destination_node == node.id;
        work.step()?;
        if addressed && found.replace(cut).is_some() {
            return Err((FragmentCompileError::Invalid(
                "ExchangeSource has more than one inbound cut",
            ))
            .into());
        }
    }
    let cut = found.ok_or(FragmentCompileError::Invalid(
        "ExchangeSource has no inbound cut",
    ))?;
    if cut.edge != *edge {
        return Err((FragmentCompileError::Invalid(
            "ExchangeSource edge differs from its inbound cut",
        ))
        .into());
    }
    if cut.kind != EdgeKind::Stream && cut.kind != EdgeKind::ChangeStreamRouter {
        return Err((FragmentCompileError::Unsupported {
            node: Some(node.id),
            feature: "inbound CTE edge",
        })
        .into());
    }
    // A writer-result receiver presents the writer relation positionally,
    // named by its frozen relation fields; the finish admission proved that
    // it feeds exactly one finish.
    let relation = cut.writer_result().map(|relation| &relation.fields);
    if relation.is_some_and(|fields| fields.len() != node.output.columns.len()) {
        return Err((FragmentCompileError::Invalid(
            "writer-result cut width differs from its receiver",
        ))
        .into());
    }
    // Receive binding is positional: wire column `i` is the cut's import `i`,
    // and the received occurrence `i` is that import's destination value.
    if cut.imports.len() != imports.len() {
        return Err((FragmentCompileError::Invalid(
            "ExchangeSource imports differ from its inbound cut",
        ))
        .into());
    }
    for ((import, (source, destination)), output) in cut
        .imports
        .iter()
        .zip(imports.iter())
        .zip(node.output.columns.iter())
    {
        let exact = import.source.value == *source
            && import.destination == *destination
            && destination == output;
        work.step()?;
        if !exact {
            return Err((FragmentCompileError::Invalid(
                "ExchangeSource occurrence differs from its inbound cut import",
            ))
            .into());
        }
    }
    // Repeated imports retain independent output channels. Routing reads the
    // first occurrence of each destination value, as channel planning does,
    // preserving the declared key order. Applicability follows the same rule.
    let keys = match &cut.partitioning.destination {
        Distribution::Hash { keys, .. }
        | Distribution::ConditionalKey { keys, .. }
        | Distribution::BucketShuffle { keys, .. } => {
            if keys.is_empty() {
                return Err((FragmentCompileError::Invalid(
                    "ExchangeSource keyed cut has no keys",
                ))
                .into());
            }
            keys.as_ref()
        }
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => &[],
    };
    let mut hash_partition_slots = Vec::new();
    reserve_vec(&mut hash_partition_slots, keys.len(), work)?;
    for key in keys {
        let mut found = None;
        for (ordinal, value) in node.output.columns.iter().enumerate() {
            work.step()?;
            if value == key && found.is_none() {
                found = Some(slots[ordinal]);
            }
        }
        hash_partition_slots.push(found.ok_or(FragmentCompileError::Invalid(
            "ExchangeSource hash key is absent from its output",
        ))?);
    }
    let result = package
        .result()
        .filter(|result| result.output.columns == node.output.columns);
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(slots.len(), work, creation)?;
    let mut wire_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(slots.len(), work, creation)?;
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        let ty = &package
            .fragment()
            .values()
            .get(value)
            .ok_or(FragmentCompileError::Invalid(
                "missing ExchangeSource output type",
            ))?
            .ty;
        // Full result labels are authoritative only when this receiver's
        // entire ordered output is the result port's: a Sort, TopN, Filter or
        // Limit root publishes the layout it reads unchanged.
        let name = match (relation, result) {
            (Some(fields), _) => {
                let field = &fields[ordinal];
                let same = field.destination == *value && field.ty == *ty;
                work.step()?;
                if !same {
                    return Err((FragmentCompileError::Invalid(
                        "writer-result cut field differs from its received occurrence",
                    ))
                    .into());
                }
                crate::typed_layout::copy_name_with_creation(&field.name, work, creation)?
            }
            (None, Some(result)) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "missing ExchangeSource result label",
                    ))?;
                crate::typed_layout::copy_name_with_creation(
                    field.alias.as_deref().unwrap_or(&field.name),
                    work,
                    creation,
                )?
            }
            (None, None)
                if matches!(
                    cut.payload,
                    novarocks_physical_plan::ExchangePayloadContract::Summary(_)
                ) =>
            {
                let names = ["inserted", "replaced", "deleted"];
                let name = names.get(ordinal).ok_or(FragmentCompileError::Invalid(
                    "summary receiver width differs",
                ))?;
                crate::typed_layout::copy_name_with_creation(name, work, creation)?
            }
            (None, None) if cut.change_stream_writer().is_some() => {
                let root = package
                    .fragment()
                    .nodes()
                    .get(&package.fragment().root())
                    .ok_or(FragmentCompileError::Invalid("missing route writer root"))?;
                let NodeKind::TableWriter { target } = &root.kind else {
                    return Err(FragmentCompileError::Invalid(
                        "provider route receiver has no writer",
                    )
                    .into());
                };
                let field =
                    target
                        .target_fields
                        .get(ordinal)
                        .ok_or(FragmentCompileError::Invalid(
                            "provider route field is absent",
                        ))?;
                if field.input != *value {
                    return Err(FragmentCompileError::Invalid(
                        "provider route field occurrence differs",
                    )
                    .into());
                }
                crate::typed_layout::copy_name_with_creation(&field.provider_name, work, creation)?
            }
            (None, None) => crate::typed_layout::local_name_with_creation(
                "local_exchange",
                node.id.get(),
                ordinal,
                work,
                creation,
            )?,
        };
        work.flush()?;
        typed_layout.push_with_creation(ty, name, work, creation)?;
        // The cut's source owns the wire declaration. Its ordinal maps to
        // this receiver's slot, independently of source-fragment slot IDs
        // and any local root nullability or display-name differences.
        wire_layout.push_with_creation(
            &cut.imports[ordinal].source.ty,
            String::new(),
            work,
            creation,
        )?;
        work.step()?;
    }
    work.flush()?;
    let layout = typed_layout.finish_with_creation(slots, work, creation)?;
    let wire_layout = wire_layout.finish_with_creation(slots, work, creation)?;
    work.flush()?;
    Ok(LoweredExchangeSource {
        kind: ProgramNodeKind::ExchangeSource {
            runtime_filters: Vec::new().into(),
            hash_partition_exprs: Vec::new(),
        },
        layout,
        input: CompiledExchangeInput {
            wire_layout,
            role: crate::stream_sink::cut_role(cut.payload.role())?,
            delivery: crate::stream_sink::cut_delivery(cut.delivery),
            routing: match &cut.partitioning.destination {
                Distribution::Hash { scheme, .. } => {
                    novarocks_local_program::CompiledExchangeRouting::Hash {
                        algorithm: scheme.definition.algorithm,
                    }
                }
                Distribution::BucketShuffle { scheme, .. } => {
                    novarocks_local_program::CompiledExchangeRouting::BucketShuffle {
                        algorithm: scheme.hash,
                    }
                }
                Distribution::ConditionalKey {
                    applies, scheme, ..
                } => {
                    let mut mask = None;
                    for (ordinal, value) in node.output.columns.iter().enumerate() {
                        if value == applies && mask.is_none() {
                            mask = Some(slots[ordinal]);
                        }
                        work.step()?;
                    }
                    novarocks_local_program::CompiledExchangeRouting::ConditionalKey {
                        applies: mask.ok_or(FragmentCompileError::Invalid(
                            "conditional mask is absent from receiver",
                        ))?,
                        algorithm: scheme.definition.algorithm,
                    }
                }
                Distribution::RoundRobin => {
                    novarocks_local_program::CompiledExchangeRouting::RoundRobin
                }
                _ => novarocks_local_program::CompiledExchangeRouting::Unpartitioned,
            },
            receiver_node: node.id.get(),
            edge: edge.get(),
            source_fragment: cut.source_fragment.get(),
            hash_partition_slots: hash_partition_slots.into_boxed_slice(),
        },
    })
}
