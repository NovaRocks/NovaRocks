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

//! Pure CREATE-time projection of frozen SQL and provider facts into MV documents.
//!
//! This module has no connector calls.  In particular, the target facts come
//! only from the staged-create handle, never from a legacy post-create reload.

use std::collections::BTreeMap;

use crate::persistence::codec::{
    ApplyKeyKind, BoundOutput, ConfigurationDocument, DefinitionDocument, InterpretationDocument,
    PhysicalFieldLogicalIdentity, QuerySource, SourceFieldReference, build_definition,
};
use crate::persistence::identity::{
    ApplyKeyIdentity, FieldIdentity, ObjectIdentity, PartitionSpecVersion, SchemaVersion,
};
use crate::persistence::validation::runtime::{
    RuntimeApplyKeyComponentFacts, RuntimeApplyKeyFacts, RuntimeBranchFacts,
    RuntimeInterpretationFacts, RuntimeOutputBindingFacts, RuntimePhysicalFieldFacts,
    RuntimeTargetFacts,
};
use bytes::Bytes;
use novarocks_spi::connector::ConnectorPreparedCreateDocumentTarget;
use novarocks_sql::planning::mv::{SqlMvCreatePersistenceFacts, TargetIdentity};
use novarocks_types::mv_aggregate_layout::MvAggregateRuntimeLayout;
use sha2::{Digest, Sha256};

use super::aggregate_bindings::{
    MvAggregateCreateBindingInput, MvCreateRelationObservation, MvCreateTargetFieldObservation,
    build_mv_aggregate_create_bindings, source_field_map, target_field_map,
};

const APPLY_KEY_IDENTITY_DOMAIN: &[u8] = b"novarocks.mv.apply-key.v1";

pub struct MvCreateDocumentFacts<'a> {
    pub created_at_ms: u64,
    pub query_definition:
        novarocks_query_application::persisted_query_definition::PersistedQueryDefinition,
    pub sql_facts: &'a SqlMvCreatePersistenceFacts,
    pub runtime_layout: &'a MvAggregateRuntimeLayout,
    pub source_observations: &'a [MvCreateRelationObservation],
    pub prepared_target: &'a ConnectorPreparedCreateDocumentTarget,
    pub target_identity: &'a TargetIdentity,
    pub apply_key_column_name: Option<&'a str>,
    pub branch_column_name: Option<&'a str>,
    pub configuration: ConfigurationDocument,
}

pub struct MvCreateDocuments {
    pub definition: DefinitionDocument,
    pub interpretation: InterpretationDocument,
    pub configuration: ConfigurationDocument,
}

pub struct MvInterpretationFacts<'a> {
    pub definition: &'a DefinitionDocument,
    pub sql_facts: &'a SqlMvCreatePersistenceFacts,
    pub runtime_layout: &'a MvAggregateRuntimeLayout,
    pub source_observations: &'a [MvCreateRelationObservation],
    pub target_identity: &'a TargetIdentity,
    pub apply_key_column_name: Option<&'a str>,
    pub branch_column_name: Option<&'a str>,
}

pub fn build_mv_create_documents(
    input: MvCreateDocumentFacts<'_>,
) -> Result<MvCreateDocuments, String> {
    let target_observations = target_observations(input.prepared_target)?;
    let targets = target_field_map(&target_observations, input.prepared_target)?;
    let definition = build_definition(
        input.created_at_ms,
        QuerySource::try_from(input.query_definition)?,
    )
    .map_err(|error| format!("build MV definition document: {error}"))?;
    let interpretation = build_interpretation(
        MvInterpretationFacts {
            definition: &definition,
            sql_facts: input.sql_facts,
            runtime_layout: input.runtime_layout,
            source_observations: input.source_observations,
            target_identity: input.target_identity,
            apply_key_column_name: input.apply_key_column_name,
            branch_column_name: input.branch_column_name,
        },
        &targets,
        RuntimeTargetFacts {
            object_id: ObjectIdentity::try_new(
                input.prepared_target.object_id().as_bytes().to_vec(),
            )
            .map_err(|error| error.to_string())?,
            schema_version: SchemaVersion::try_new(input.prepared_target.schema_version().to_vec())
                .map_err(|error| error.to_string())?,
            partition_spec_version: PartitionSpecVersion::try_new(
                input.prepared_target.partition_spec_version().to_vec(),
            )
            .map_err(|error| error.to_string())?,
            fields: Vec::new(),
            partition_fields: input
                .prepared_target
                .partition_fields()
                .iter()
                .map(crate::persistence::codec::TargetPartitionFieldBinding::try_from)
                .collect::<Result<_, _>>()?,
        },
    )?;
    Ok(MvCreateDocuments {
        definition,
        interpretation,
        configuration: input.configuration,
    })
}

/// Reconstruct L for newly analyzed work using the exact currently observed S.
pub fn build_mv_refresh_interpretation(
    input: MvInterpretationFacts<'_>,
    target: &super::runtime_bindings::MvExactTargetSchemaFacts,
) -> Result<InterpretationDocument, String> {
    let fields = target
        .fields
        .iter()
        .map(|field| MvCreateTargetFieldObservation {
            request_ordinal: field.ordinal,
            physical_name: field.name.clone(),
            provider_field_id: Bytes::copy_from_slice(field.field_id.as_bytes()),
            source_type: field.source_type.clone(),
            nullable: field.nullable,
        })
        .collect::<Vec<_>>();
    let targets = target_by_name(&fields)?;
    build_interpretation(
        input,
        &targets,
        RuntimeTargetFacts {
            object_id: ObjectIdentity::try_new(target.object_id.as_bytes().to_vec())
                .map_err(|error| error.to_string())?,
            schema_version: target.schema_version.clone(),
            partition_spec_version: target.partition_spec_version.clone(),
            fields: Vec::new(),
            partition_fields: target.partition_fields.clone(),
        },
    )
}

fn build_interpretation(
    input: MvInterpretationFacts<'_>,
    targets: &BTreeMap<&str, &MvCreateTargetFieldObservation>,
    mut target_facts: RuntimeTargetFacts,
) -> Result<InterpretationDocument, String> {
    let aggregate = build_mv_aggregate_create_bindings(MvAggregateCreateBindingInput {
        sql_facts: input.sql_facts,
        runtime_layout: input.runtime_layout,
        source_observations: input.source_observations,
        targets,
    })?;
    let output_bindings = output_bindings(input.sql_facts, &aggregate.output_identities, targets)?;
    let (apply_key, apply_fields, definition_branch_identities, branches) =
        if let Some(column_name) = input.apply_key_column_name {
            let (apply_key, apply_fields) = apply_key_bindings(
                input.target_identity,
                column_name,
                input.sql_facts,
                &aggregate.output_identities,
                targets,
            )?;
            let branches = branch_bindings(
                input.sql_facts,
                &aggregate.output_identities,
                &aggregate.branch_identities,
            )?;
            let definitions = aggregate.branch_identities.values().cloned().collect();
            (Some(apply_key), apply_fields, definitions, branches)
        } else {
            if !aggregate.aggregate_layout.aggregates.is_empty()
                || input.branch_column_name.is_some()
            {
                return Err(
                    "content interpretation cannot declare aggregate or branch layout".into(),
                );
            }
            (None, Vec::new(), Vec::new(), Vec::new())
        };
    let mut target_fields = aggregate.target_fields;
    target_fields.extend(
        output_bindings
            .iter()
            .map(|output| RuntimePhysicalFieldFacts {
                logical_identity: PhysicalFieldLogicalIdentity::Output(output.output_id.clone()),
                target_field_id: output.target_field_id.clone(),
                source_type: output.source_type.clone(),
                nullable: output.nullable,
            }),
    );
    target_fields.extend(apply_fields);
    if !branches.is_empty() {
        let branch_name = input.branch_column_name.ok_or_else(|| {
            "UNION definition has branches but no staged branch target field".to_string()
        })?;
        let target = target(targets, branch_name)?;
        let id = field_identity(target.provider_field_id.clone())?;
        target_fields.extend(branches.iter().map(|branch| RuntimePhysicalFieldFacts {
            logical_identity: PhysicalFieldLogicalIdentity::Branch(branch.branch_id.clone()),
            target_field_id: id.clone(),
            source_type: target.source_type.clone(),
            nullable: target.nullable,
        }));
    }
    target_facts.fields = target_fields;
    let interpretation = InterpretationDocument::try_from(RuntimeInterpretationFacts {
        definition_revision: super::keyed_documents::definition_revision(input.definition)
            .map_err(|error| format!("build MV definition semantic revision: {error}"))?,
        output_bindings,
        aggregate_layout: aggregate.aggregate_layout,
        apply_key,
        definition_branch_identities,
        branches,
        target: target_facts,
    })
    .map_err(|error| format!("build MV interpretation document: {error}"))?;
    // The old planner still authors keyed non-aggregate layouts until the
    // content layout becomes its only non-aggregate output.
    if input.apply_key_column_name.is_none() || interpretation.is_aggregate() {
        super::validation::validate_new_interpretation(&interpretation)
            .map_err(|error| error.to_string())?;
    }
    Ok(interpretation)
}

fn target_observations(
    target: &ConnectorPreparedCreateDocumentTarget,
) -> Result<Vec<MvCreateTargetFieldObservation>, String> {
    target
        .fields()
        .iter()
        .map(|field| {
            Ok(MvCreateTargetFieldObservation {
                request_ordinal: field.request_ordinal(),
                physical_name: field.name().to_string(),
                provider_field_id: field.provider_field_id().clone(),
                source_type: field.source_type().clone(),
                nullable: field.nullable(),
            })
        })
        .collect()
}

fn target_by_name(
    fields: &[MvCreateTargetFieldObservation],
) -> Result<BTreeMap<&str, &MvCreateTargetFieldObservation>, String> {
    let mut result = BTreeMap::new();
    for field in fields {
        if result.insert(field.physical_name.as_str(), field).is_some() {
            return Err("prepared target has duplicate physical field names".to_string());
        }
    }
    Ok(result)
}

fn target<'a>(
    targets: &'a BTreeMap<&str, &'a MvCreateTargetFieldObservation>,
    name: &str,
) -> Result<&'a MvCreateTargetFieldObservation, String> {
    targets
        .get(name)
        .copied()
        .ok_or_else(|| format!("prepared target lacks required physical field {name}"))
}

fn field_identity(value: Bytes) -> Result<FieldIdentity, String> {
    FieldIdentity::try_new(value.to_vec()).map_err(|error| error.to_string())
}

/// Project B5 and the exact root fields whose values supply the output.
pub fn build_binding_outputs(
    sql: &SqlMvCreatePersistenceFacts,
    source_observations: &[MvCreateRelationObservation],
) -> Result<Vec<BoundOutput>, String> {
    let source = source_field_map(sql, source_observations)?;
    sql.outputs()
        .iter()
        .map(|output| {
            let source_fields = output
                .source_fields()
                .iter()
                .map(|reference| {
                    let occurrence_id = reference.occurrence_id().get();
                    let field_id = source
                        .get(&(occurrence_id, reference.field_ordinal()))
                        .ok_or_else(|| {
                            "SQL output source has no frozen provider field".to_string()
                        })?;
                    Ok(SourceFieldReference {
                        occurrence_id,
                        field_id: field_id.clone(),
                    })
                })
                .collect::<Result<std::collections::BTreeSet<_>, String>>()?;
            Ok(BoundOutput {
                ordinal: output.output_ordinal(),
                name: output.name().to_string(),
                logical_type_identity: Bytes::copy_from_slice(output.canonical_bytes()),
                source_fields: source_fields.into_iter().collect(),
            })
        })
        .collect()
}

fn output_bindings(
    facts: &SqlMvCreatePersistenceFacts,
    ids: &BTreeMap<u32, crate::persistence::identity::OutputIdentity>,
    targets: &BTreeMap<&str, &MvCreateTargetFieldObservation>,
) -> Result<Vec<RuntimeOutputBindingFacts>, String> {
    facts
        .outputs()
        .iter()
        .map(|output| {
            let target = target(targets, output.name())?;
            Ok(RuntimeOutputBindingFacts {
                output_id: ids
                    .get(&output.output_ordinal())
                    .cloned()
                    .ok_or_else(|| "SQL output has no semantic identity".to_string())?,
                target_field_id: field_identity(target.provider_field_id.clone())?,
                source_type: target.source_type.clone(),
                nullable: target.nullable,
            })
        })
        .collect()
}

fn branch_bindings(
    facts: &SqlMvCreatePersistenceFacts,
    outputs: &BTreeMap<u32, crate::persistence::identity::OutputIdentity>,
    branches: &BTreeMap<u32, crate::persistence::identity::BranchIdentity>,
) -> Result<Vec<RuntimeBranchFacts>, String> {
    facts
        .union_branches()
        .iter()
        .map(|branch| {
            Ok(RuntimeBranchFacts {
                branch_id: branches
                    .get(&branch.branch_ordinal())
                    .cloned()
                    .ok_or_else(|| "SQL branch has no semantic identity".to_string())?,
                relation_occurrence_ids: branch
                    .relation_occurrence_ids()
                    .iter()
                    .map(|id| id.get())
                    .collect(),
                output_ids: branch
                    .output_ordinals()
                    .iter()
                    .map(|ordinal| {
                        outputs
                            .get(ordinal)
                            .cloned()
                            .ok_or_else(|| "SQL branch references unknown output".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .collect()
}

fn apply_key_bindings(
    identity: &TargetIdentity,
    column: &str,
    facts: &SqlMvCreatePersistenceFacts,
    outputs: &BTreeMap<u32, crate::persistence::identity::OutputIdentity>,
    targets: &BTreeMap<&str, &MvCreateTargetFieldObservation>,
) -> Result<(RuntimeApplyKeyFacts, Vec<RuntimePhysicalFieldFacts>), String> {
    let inner = match identity {
        TargetIdentity::BranchScoped(inner) => inner.as_ref(),
        value => value,
    };
    let (kind, semantic_outputs, label) = match inner {
        TargetIdentity::BaseRowId => (ApplyKeyKind::BaseRowId, Vec::new(), "base-row-id"),
        TargetIdentity::JoinRowKey(_, _) => (ApplyKeyKind::JoinRowKey, Vec::new(), "join-row-key"),
        TargetIdentity::GroupRowId(names) => (
            ApplyKeyKind::GroupRowId,
            names
                .iter()
                .map(|name| {
                    facts
                        .outputs()
                        .iter()
                        .find(|output| output.name().eq_ignore_ascii_case(name))
                        .and_then(|output| outputs.get(&output.output_ordinal()))
                        .cloned()
                        .ok_or_else(|| format!("group apply key references unknown output {name}"))
                })
                .collect::<Result<Vec<_>, _>>()?,
            "group-row-id",
        ),
        TargetIdentity::BranchScoped(_) => unreachable!("branch wrapper was removed"),
    };
    let mut bytes = Vec::from(APPLY_KEY_IDENTITY_DOMAIN);
    bytes.extend_from_slice(label.as_bytes());
    for output in &semantic_outputs {
        bytes.extend_from_slice(output.as_bytes());
    }
    let logical_id = ApplyKeyIdentity::try_new(Sha256::digest(bytes).to_vec())
        .map_err(|error| error.to_string())?;
    let target = target(targets, column)?;
    let target_field_id = field_identity(target.provider_field_id.clone())?;
    Ok((
        RuntimeApplyKeyFacts {
            kind,
            ordered_components: vec![RuntimeApplyKeyComponentFacts {
                logical_id: logical_id.clone(),
                target_field_id: target_field_id.clone(),
            }],
        },
        vec![RuntimePhysicalFieldFacts {
            logical_identity: PhysicalFieldLogicalIdentity::ApplyKey(logical_id),
            target_field_id,
            source_type: target.source_type.clone(),
            nullable: target.nullable,
        }],
    ))
}
