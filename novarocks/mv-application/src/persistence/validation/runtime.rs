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

//! Exhaustive pure mapping between application runtime facts and D/L.
//!
//! MV owns the durable interpretation, while SQL and providers supply typed
//! semantic and physical facts. Every stable fact required by D/L is a
//! mandatory typed field here, so the adapter cannot fall back to persisting
//! the old contract, an AST, a plan, an owner, or a registry handle.

use novarocks_query_application::persisted_query_definition::PersistedQueryDefinition;

use crate::persistence::codec::{
    AggregateInterpretation, ApplyKey, ApplyKeyComponent, ApplyKeyKind, BranchInterpretation,
    DefinitionDocument, ExpressionKind, ExpressionShape, InterpretationDocument, OutputBinding,
    OutputDefinition, PhysicalFieldBinding, PhysicalFieldLogicalIdentity, QuerySource,
    RelationOccurrence, SourceFieldBinding, SourceFieldReference, StateEncoding, StateRole,
    StateSlot, TargetBinding, TargetPartitionFieldBinding, build_definition_with_scalar_contracts,
};
use crate::persistence::identity::{
    AggregateIdentity, ApplyKeyIdentity, BranchIdentity, ComputationIdentity, DocumentRevision,
    FieldIdentity, ObjectIdentity, OutputIdentity, PartitionSpecVersion, SchemaVersion,
    StateSlotIdentity,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeDefinitionFacts {
    pub created_at_ms: u64,
    pub query_definition: PersistedQueryDefinition,
    pub relation_occurrences: Vec<RuntimeRelationOccurrenceFacts>,
    pub outputs: Vec<RuntimeOutputFacts>,
    pub scalar_contracts: Vec<novarocks_functions::PersistentScalarContract>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeRelationOccurrenceFacts {
    pub occurrence_id: u32,
    pub catalog_at_binding: String,
    pub namespace_at_binding: String,
    pub relation_at_binding: String,
    pub qualifier_at_binding: String,
    pub object_id: ObjectIdentity,
    pub schema_version: SchemaVersion,
    pub referenced_fields: Vec<RuntimeSourceFieldFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeSourceFieldFacts {
    pub field_id: FieldIdentity,
    pub name_at_binding: String,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeOutputFacts {
    pub output_id: OutputIdentity,
    pub name: String,
    pub type_signature: String,
    pub nullable: bool,
    pub expression_kind: ExpressionKind,
    pub function_identity: Option<String>,
    pub source_fields: Vec<RuntimeSourceFieldReference>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RuntimeSourceFieldReference {
    pub occurrence_id: u32,
    pub field_id: FieldIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeInterpretationFacts {
    pub definition_revision: DocumentRevision,
    pub computation_identity: ComputationIdentity,
    pub output_bindings: Vec<RuntimeOutputBindingFacts>,
    pub aggregate_layout: RuntimeAggregateLayoutFacts,
    pub apply_key: RuntimeApplyKeyFacts,
    /// The complete ordered branch identity set derived while compiling D.
    /// It is checked against `branches` so an adapter cannot silently omit a
    /// UNION arm while constructing L.
    pub definition_branch_identities: Vec<BranchIdentity>,
    pub branches: Vec<RuntimeBranchFacts>,
    pub target: RuntimeTargetFacts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeOutputBindingFacts {
    pub output_id: OutputIdentity,
    pub target_field_id: FieldIdentity,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeAggregateLayoutFacts {
    pub state_slots: Vec<RuntimeStateSlotFacts>,
    pub aggregates: Vec<RuntimeAggregateFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeStateSlotFacts {
    pub slot_id: StateSlotIdentity,
    pub target_field_id: FieldIdentity,
    pub type_signature: String,
    pub nullable: bool,
    pub role: StateRole,
    pub encoding: StateEncoding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeAggregateFacts {
    pub aggregate_id: AggregateIdentity,
    pub function_identity: String,
    pub semantic_contract: Option<novarocks_functions::PersistentAggregateContract>,
    pub source_fields: Vec<RuntimeSourceFieldReference>,
    /// Ordered algorithm state; AVG is sum followed by count.
    pub state_slot_ids: Vec<StateSlotIdentity>,
    /// The UNION branch this aggregate computes, when the view has branches.
    pub branch_id: Option<BranchIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeApplyKeyFacts {
    pub kind: ApplyKeyKind,
    pub ordered_components: Vec<RuntimeApplyKeyComponentFacts>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeApplyKeyComponentFacts {
    pub logical_id: ApplyKeyIdentity,
    pub target_field_id: FieldIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeBranchFacts {
    pub branch_id: BranchIdentity,
    pub relation_occurrence_ids: Vec<u32>,
    pub output_ids: Vec<OutputIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeTargetFacts {
    pub object_id: ObjectIdentity,
    pub schema_version: SchemaVersion,
    pub partition_spec_version: PartitionSpecVersion,
    pub fields: Vec<RuntimePhysicalFieldFacts>,
    pub partition_fields: Vec<TargetPartitionFieldBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimePhysicalFieldFacts {
    pub logical_identity: PhysicalFieldLogicalIdentity,
    pub target_field_id: FieldIdentity,
    pub type_signature: String,
    pub nullable: bool,
}

impl TryFrom<RuntimeDefinitionFacts> for DefinitionDocument {
    type Error = String;

    fn try_from(value: RuntimeDefinitionFacts) -> Result<Self, Self::Error> {
        let query = QuerySource::try_from(value.query_definition)?;
        build_definition_with_scalar_contracts(
            value.created_at_ms,
            query,
            value
                .relation_occurrences
                .into_iter()
                .map(|relation| RelationOccurrence {
                    occurrence_id: relation.occurrence_id,
                    catalog_at_binding: relation.catalog_at_binding,
                    namespace_at_binding: relation.namespace_at_binding,
                    relation_at_binding: relation.relation_at_binding,
                    qualifier_at_binding: relation.qualifier_at_binding,
                    object_id: relation.object_id,
                    schema_version: relation.schema_version,
                    fields: relation
                        .referenced_fields
                        .into_iter()
                        .map(|field| SourceFieldBinding {
                            field_id: field.field_id,
                            name_at_binding: field.name_at_binding,
                            type_signature: field.type_signature,
                            nullable: field.nullable,
                        })
                        .collect(),
                })
                .collect(),
            value
                .outputs
                .into_iter()
                .map(|output| OutputDefinition {
                    output_id: output.output_id,
                    name: output.name,
                    type_signature: output.type_signature,
                    nullable: output.nullable,
                    expression: ExpressionShape {
                        kind: output.expression_kind,
                        function_identity: output.function_identity,
                        source_fields: output
                            .source_fields
                            .into_iter()
                            .map(|reference| SourceFieldReference {
                                occurrence_id: reference.occurrence_id,
                                field_id: reference.field_id,
                            })
                            .collect(),
                    },
                })
                .collect(),
            value.scalar_contracts,
        )
        .map_err(|error| error.to_string())
    }
}

impl TryFrom<&DefinitionDocument> for RuntimeDefinitionFacts {
    type Error = String;

    fn try_from(value: &DefinitionDocument) -> Result<Self, Self::Error> {
        Ok(Self {
            created_at_ms: value.created_at_ms,
            scalar_contracts: value.scalar_contracts.clone(),
            query_definition: PersistedQueryDefinition::try_from(value.query.clone())?,
            relation_occurrences: value
                .relation_occurrences
                .iter()
                .map(|relation| RuntimeRelationOccurrenceFacts {
                    occurrence_id: relation.occurrence_id,
                    catalog_at_binding: relation.catalog_at_binding.clone(),
                    namespace_at_binding: relation.namespace_at_binding.clone(),
                    relation_at_binding: relation.relation_at_binding.clone(),
                    qualifier_at_binding: relation.qualifier_at_binding.clone(),
                    object_id: relation.object_id.clone(),
                    schema_version: relation.schema_version.clone(),
                    referenced_fields: relation
                        .fields
                        .iter()
                        .map(|field| RuntimeSourceFieldFacts {
                            field_id: field.field_id.clone(),
                            name_at_binding: field.name_at_binding.clone(),
                            type_signature: field.type_signature.clone(),
                            nullable: field.nullable,
                        })
                        .collect(),
                })
                .collect(),
            outputs: value
                .outputs
                .iter()
                .map(|output| RuntimeOutputFacts {
                    output_id: output.output_id.clone(),
                    name: output.name.clone(),
                    type_signature: output.type_signature.clone(),
                    nullable: output.nullable,
                    expression_kind: output.expression.kind,
                    function_identity: output.expression.function_identity.clone(),
                    source_fields: output
                        .expression
                        .source_fields
                        .iter()
                        .map(|reference| RuntimeSourceFieldReference {
                            occurrence_id: reference.occurrence_id,
                            field_id: reference.field_id.clone(),
                        })
                        .collect(),
                })
                .collect(),
        })
    }
}

impl TryFrom<RuntimeInterpretationFacts> for InterpretationDocument {
    type Error = String;

    fn try_from(value: RuntimeInterpretationFacts) -> Result<Self, Self::Error> {
        let actual_branch_identities = value
            .branches
            .iter()
            .map(|branch| branch.branch_id.clone())
            .collect::<Vec<_>>();
        if value.definition_branch_identities != actual_branch_identities {
            return Err(
                "runtime interpretation branches do not match the complete ordered definition branch identity set"
                    .to_string(),
            );
        }
        Ok(Self {
            definition_revision: value.definition_revision,
            computation_identity: value.computation_identity,
            outputs: value
                .output_bindings
                .into_iter()
                .map(|output| OutputBinding {
                    output_id: output.output_id,
                    target_field_id: output.target_field_id,
                    type_signature: output.type_signature,
                    nullable: output.nullable,
                })
                .collect(),
            state_slots: value
                .aggregate_layout
                .state_slots
                .into_iter()
                .map(|slot| StateSlot {
                    slot_id: slot.slot_id,
                    target_field_id: slot.target_field_id,
                    type_signature: slot.type_signature,
                    nullable: slot.nullable,
                    role: slot.role,
                    encoding: slot.encoding,
                })
                .collect(),
            apply_key: ApplyKey {
                kind: value.apply_key.kind,
                components: value
                    .apply_key
                    .ordered_components
                    .into_iter()
                    .map(|component| ApplyKeyComponent {
                        logical_id: component.logical_id,
                        target_field_id: component.target_field_id,
                    })
                    .collect(),
            },
            aggregates: value
                .aggregate_layout
                .aggregates
                .into_iter()
                .map(|aggregate| AggregateInterpretation {
                    aggregate_id: aggregate.aggregate_id,
                    branch_id: aggregate.branch_id,
                    function_identity: aggregate.function_identity,
                    semantic_contract: aggregate.semantic_contract,
                    source_fields: aggregate
                        .source_fields
                        .into_iter()
                        .map(|reference| SourceFieldReference {
                            occurrence_id: reference.occurrence_id,
                            field_id: reference.field_id,
                        })
                        .collect(),
                    state_slot_ids: aggregate.state_slot_ids,
                })
                .collect(),
            branches: value
                .branches
                .into_iter()
                .map(|branch| BranchInterpretation {
                    branch_id: branch.branch_id,
                    relation_occurrence_ids: branch.relation_occurrence_ids,
                    output_ids: branch.output_ids,
                })
                .collect(),
            target: TargetBinding {
                object_id: value.target.object_id,
                schema_version: value.target.schema_version,
                partition_spec_version: value.target.partition_spec_version,
                fields: value
                    .target
                    .fields
                    .into_iter()
                    .map(|field| PhysicalFieldBinding {
                        logical_identity: field.logical_identity,
                        target_field_id: field.target_field_id,
                        type_signature: field.type_signature,
                        nullable: field.nullable,
                    })
                    .collect(),
                partition_fields: value.target.partition_fields,
            },
        })
    }
}

impl From<&InterpretationDocument> for RuntimeInterpretationFacts {
    fn from(value: &InterpretationDocument) -> Self {
        Self {
            definition_revision: value.definition_revision,
            computation_identity: value.computation_identity,
            output_bindings: value
                .outputs
                .iter()
                .map(|output| RuntimeOutputBindingFacts {
                    output_id: output.output_id.clone(),
                    target_field_id: output.target_field_id.clone(),
                    type_signature: output.type_signature.clone(),
                    nullable: output.nullable,
                })
                .collect(),
            aggregate_layout: RuntimeAggregateLayoutFacts {
                state_slots: value
                    .state_slots
                    .iter()
                    .map(|slot| RuntimeStateSlotFacts {
                        slot_id: slot.slot_id.clone(),
                        target_field_id: slot.target_field_id.clone(),
                        type_signature: slot.type_signature.clone(),
                        nullable: slot.nullable,
                        role: slot.role,
                        encoding: slot.encoding,
                    })
                    .collect(),
                aggregates: value
                    .aggregates
                    .iter()
                    .map(|aggregate| RuntimeAggregateFacts {
                        aggregate_id: aggregate.aggregate_id.clone(),
                        branch_id: aggregate.branch_id.clone(),
                        function_identity: aggregate.function_identity.clone(),
                        semantic_contract: aggregate.semantic_contract.clone(),
                        source_fields: aggregate
                            .source_fields
                            .iter()
                            .map(|reference| RuntimeSourceFieldReference {
                                occurrence_id: reference.occurrence_id,
                                field_id: reference.field_id.clone(),
                            })
                            .collect(),
                        state_slot_ids: aggregate.state_slot_ids.clone(),
                    })
                    .collect(),
            },
            apply_key: RuntimeApplyKeyFacts {
                kind: value.apply_key.kind,
                ordered_components: value
                    .apply_key
                    .components
                    .iter()
                    .map(|component| RuntimeApplyKeyComponentFacts {
                        logical_id: component.logical_id.clone(),
                        target_field_id: component.target_field_id.clone(),
                    })
                    .collect(),
            },
            definition_branch_identities: value
                .branches
                .iter()
                .map(|branch| branch.branch_id.clone())
                .collect(),
            branches: value
                .branches
                .iter()
                .map(|branch| RuntimeBranchFacts {
                    branch_id: branch.branch_id.clone(),
                    relation_occurrence_ids: branch.relation_occurrence_ids.clone(),
                    output_ids: branch.output_ids.clone(),
                })
                .collect(),
            target: RuntimeTargetFacts {
                object_id: value.target.object_id.clone(),
                schema_version: value.target.schema_version.clone(),
                partition_spec_version: value.target.partition_spec_version.clone(),
                fields: value
                    .target
                    .fields
                    .iter()
                    .map(|field| RuntimePhysicalFieldFacts {
                        logical_identity: field.logical_identity.clone(),
                        target_field_id: field.target_field_id.clone(),
                        type_signature: field.type_signature.clone(),
                        nullable: field.nullable,
                    })
                    .collect(),
                partition_fields: value.target.partition_fields.clone(),
            },
        }
    }
}
