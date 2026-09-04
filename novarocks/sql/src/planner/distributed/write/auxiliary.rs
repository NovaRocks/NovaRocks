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

//! Provider-neutral plan facts for aggregates embedded in write operators.
//!
//! Connector metadata is consumed only while constructing these facts. The
//! resulting plan contains ordinary resolved aggregate identities, typed slot
//! bindings, target grouping, and generic literal mappings. Neither the wire
//! contract nor Execution receives a Connector session or artifact descriptor.
//!
//! Design: ADR-0135 (docs/adr/ADR-0135-ordinary-aggregate-statistics-dataflow.md)

use std::collections::{BTreeMap, BTreeSet, HashMap};

use arrow::datatypes::{DataType, Schema};
use novarocks_functions::ResolvedAggregateSignature;
use novarocks_spi::connector::StatisticsRequiredAggregation;
use novarocks_spi::connector::write_stack::{
    ROOT_WRITE_RESULT_BODY_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
    ROOT_WRITE_RESULT_PROPERTIES_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootWriteResultSchema,
    WRITE_RELATION_TARGET_INDEX, WriteTargetOrdinal, WriterAuxiliaryChannel, WriterMultiplexSchema,
    root_write_result_column_id, write_relation_column_id,
};

use crate::analysis::{ExprKind, LiteralValue, TypedExpr, UnpivotConstant};
use crate::compiler::SqlFunctionCatalog;

const MAX_UNPIVOT_OUTPUT_ROWS: usize = 4_096;
const MAX_UNPIVOT_OUTPUT_BYTES: usize =
    novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES;

#[derive(Clone, Debug)]
pub struct WriterPartialAggregateCall {
    pub(crate) input_slot_id: u32,
    pub(crate) function_name: String,
    pub(crate) resolved: ResolvedAggregateSignature,
    pub(crate) intermediate_slot_id: u32,
}

impl WriterPartialAggregateCall {
    pub const fn input_slot_id(&self) -> u32 {
        self.input_slot_id
    }
    pub fn function_name(&self) -> &str {
        &self.function_name
    }
    pub const fn resolved(&self) -> &ResolvedAggregateSignature {
        &self.resolved
    }
    pub const fn intermediate_slot_id(&self) -> u32 {
        self.intermediate_slot_id
    }
}

#[derive(Clone, Debug, Default)]
pub struct WriterPartialAggregatePlan {
    pub(crate) calls: Vec<WriterPartialAggregateCall>,
}

impl WriterPartialAggregatePlan {
    pub fn calls(&self) -> &[WriterPartialAggregateCall] {
        &self.calls
    }
}

#[derive(Clone, Debug)]
pub struct WriterFinalAggregateCall {
    pub(crate) function_name: String,
    pub(crate) resolved: ResolvedAggregateSignature,
    pub(crate) intermediate_input_slot_id: u32,
    pub(crate) final_output_slot_id: u32,
}

impl WriterFinalAggregateCall {
    pub fn function_name(&self) -> &str {
        &self.function_name
    }
    pub const fn resolved(&self) -> &ResolvedAggregateSignature {
        &self.resolved
    }
    pub const fn intermediate_input_slot_id(&self) -> u32 {
        self.intermediate_input_slot_id
    }
    pub const fn final_output_slot_id(&self) -> u32 {
        self.final_output_slot_id
    }
}

#[derive(Clone, Debug)]
pub struct WriteUnpivotMapping {
    pub(crate) target: WriteTargetOrdinal,
    pub(crate) input_value_slot_id: u32,
    pub(crate) literals: Vec<UnpivotConstant>,
}

impl WriteUnpivotMapping {
    pub const fn target(&self) -> WriteTargetOrdinal {
        self.target
    }
    pub const fn input_value_slot_id(&self) -> u32 {
        self.input_value_slot_id
    }
    pub fn constants(&self) -> &[UnpivotConstant] {
        &self.literals
    }
}

#[derive(Clone, Debug)]
pub struct WriterUnpivotPlan {
    pub(crate) grouping_input_slot_id: u32,
    pub(crate) grouping_output_slot_id: u32,
    pub(crate) passthrough_output_slot_id: u32,
    pub(crate) value_output_slot_id: u32,
    pub(crate) literal_output_slot_ids: Vec<u32>,
    pub(crate) mappings: Vec<WriteUnpivotMapping>,
    pub(crate) max_output_rows: usize,
    pub(crate) max_output_bytes: usize,
}

impl WriterUnpivotPlan {
    pub const fn grouping_input_slot_id(&self) -> u32 {
        self.grouping_input_slot_id
    }
    pub const fn grouping_output_slot_id(&self) -> u32 {
        self.grouping_output_slot_id
    }
    pub const fn passthrough_output_slot_id(&self) -> u32 {
        self.passthrough_output_slot_id
    }
    pub const fn value_output_slot_id(&self) -> u32 {
        self.value_output_slot_id
    }
    pub fn literal_output_slot_ids(&self) -> &[u32] {
        &self.literal_output_slot_ids
    }
    pub fn mappings(&self) -> &[WriteUnpivotMapping] {
        &self.mappings
    }
    pub const fn max_output_rows(&self) -> usize {
        self.max_output_rows
    }
    pub const fn max_output_bytes(&self) -> usize {
        self.max_output_bytes
    }
}

#[derive(Clone, Debug)]
pub struct WriterFinalAggregatePlan {
    pub(crate) calls: Vec<WriterFinalAggregateCall>,
    pub(crate) unpivot: Option<WriterUnpivotPlan>,
}

impl WriterFinalAggregatePlan {
    pub const fn empty() -> Self {
        Self {
            calls: Vec::new(),
            unpivot: None,
        }
    }

    pub fn calls(&self) -> &[WriterFinalAggregateCall] {
        &self.calls
    }
    pub const fn unpivot(&self) -> Option<&WriterUnpivotPlan> {
        self.unpivot.as_ref()
    }
}

#[derive(Clone, Debug)]
pub struct WriterAuxiliaryPlan {
    schema: WriterMultiplexSchema,
    partial_by_target: BTreeMap<WriteTargetOrdinal, WriterPartialAggregatePlan>,
    final_plan: WriterFinalAggregatePlan,
}

impl WriterAuxiliaryPlan {
    pub fn empty() -> Self {
        Self {
            schema: WriterMultiplexSchema::empty(),
            partial_by_target: BTreeMap::new(),
            final_plan: WriterFinalAggregatePlan::empty(),
        }
    }

    pub(crate) fn without_requirements(
        targets: impl IntoIterator<Item = WriteTargetOrdinal>,
    ) -> Result<Self, String> {
        let mut partial_by_target = BTreeMap::new();
        let mut previous = None;
        for target in targets {
            if previous.is_some_and(|previous: WriteTargetOrdinal| previous >= target) {
                return Err(
                    "write auxiliary targets must be listed in strictly ascending ordinal order"
                        .to_string(),
                );
            }
            previous = Some(target);
            if partial_by_target
                .insert(target, WriterPartialAggregatePlan::default())
                .is_some()
            {
                return Err(format!(
                    "write auxiliary plan contains duplicate target {}",
                    target.get()
                ));
            }
        }
        Ok(Self {
            schema: WriterMultiplexSchema::empty(),
            partial_by_target,
            final_plan: WriterFinalAggregatePlan::empty(),
        })
    }

    pub fn schema(&self) -> &WriterMultiplexSchema {
        &self.schema
    }

    pub fn partial_for(
        &self,
        target: WriteTargetOrdinal,
    ) -> Result<WriterPartialAggregatePlan, String> {
        self.partial_by_target.get(&target).cloned().ok_or_else(|| {
            format!(
                "write auxiliary plan has no partial aggregate entry for target {}",
                target.get()
            )
        })
    }

    pub fn final_plan(&self) -> &WriterFinalAggregatePlan {
        &self.final_plan
    }
}

/// Target-local input facts used only while lowering Connector requirements.
pub struct WriterStatisticsTargetInput<'a> {
    pub target: WriteTargetOrdinal,
    /// Exact Arrow relation produced by the terminal writer projection. Slot
    /// IDs are its one-based ordinals, matching the frozen target schema.
    pub input_schema: &'a Schema,
    pub requirements: &'a [StatisticsRequiredAggregation],
}

/// Resolve Connector-selected names once and erase the Connector descriptors
/// into ordinary engine plan facts.
pub fn plan_writer_statistics(
    targets: &[WriterStatisticsTargetInput<'_>],
    functions: &dyn SqlFunctionCatalog,
) -> Result<WriterAuxiliaryPlan, String> {
    if targets.is_empty() {
        return Ok(WriterAuxiliaryPlan::empty());
    }

    let mut seen_targets = BTreeSet::new();
    let mut previous_target = None;
    for target in targets {
        if previous_target.is_some_and(|previous: WriteTargetOrdinal| previous >= target.target) {
            return Err(
                "write aggregate targets must be listed in strictly ascending ordinal order"
                    .to_string(),
            );
        }
        previous_target = Some(target.target);
        if !seen_targets.insert(target.target) {
            return Err(format!(
                "write aggregate plan contains duplicate target {}",
                target.target.get()
            ));
        }
    }
    if targets.iter().all(|target| target.requirements.is_empty()) {
        return WriterAuxiliaryPlan::without_requirements(
            targets.iter().map(|target| target.target),
        );
    }

    let mut next_slot = first_free_internal_slot(targets)?;
    let grouping_output_slot_id = allocate_slot(&mut next_slot)?;
    let mut channels = Vec::new();
    let mut partial_by_target = BTreeMap::new();
    let mut final_calls = Vec::new();
    let mut mappings = Vec::new();
    let mut shared_channels = HashMap::<(ResolvedAggregateSignature, usize), (u32, u32)>::new();

    for target in targets {
        let mut partial_calls = Vec::with_capacity(target.requirements.len());
        let mut occurrence_by_signature = HashMap::<ResolvedAggregateSignature, usize>::new();
        for requirement in target.requirements {
            let input = target
                .input_schema
                .fields()
                .get(requirement.input().ordinal())
                .ok_or_else(|| {
                    format!(
                        "write aggregate input ordinal {} is absent for target {}",
                        requirement.input().ordinal(),
                        target.target.get()
                    )
                })?;
            if input.name() != requirement.input().name()
                || input.data_type() != requirement.input().data_type()
                || input.is_nullable() != requirement.input().nullable()
            {
                return Err(format!(
                    "write aggregate input ordinal {} does not match the pinned column for target {}",
                    requirement.input().ordinal(),
                    target.target.get()
                ));
            }
            let resolved = functions
                .resolve_aggregate_trusted(
                    requirement.function_name(),
                    std::slice::from_ref(requirement.input().data_type()),
                )
                .map_err(|error| {
                    format!(
                        "resolve trusted write aggregate `{}` for {:?}: {error}",
                        requirement.function_name(),
                        requirement.input().data_type()
                    )
                })?;
            if resolved.output_type != DataType::Binary {
                return Err(format!(
                    "write aggregate `{}` output {:?} cannot feed the binary Root value slot",
                    requirement.function_name(),
                    resolved.output_type
                ));
            }
            let occurrence = occurrence_by_signature.entry(resolved.clone()).or_default();
            let shared_key = (resolved.clone(), *occurrence);
            *occurrence = occurrence
                .checked_add(1)
                .ok_or_else(|| "write aggregate occurrence overflowed".to_string())?;
            let (intermediate_slot_id, final_output_slot_id) = if let Some(slots) =
                shared_channels.get(&shared_key)
            {
                *slots
            } else {
                let intermediate_slot_id = allocate_slot(&mut next_slot)?;
                let final_output_slot_id = allocate_slot(&mut next_slot)?;
                let ordinal = channels.len();
                channels.push(
                    WriterAuxiliaryChannel::try_new(
                        intermediate_slot_id,
                        format!("auxiliary_channel_{ordinal}"),
                        resolved.intermediate_type.clone(),
                    )
                    .map_err(|error| error.to_string())?,
                );
                final_calls.push(WriterFinalAggregateCall {
                    function_name: requirement.function_name().to_string(),
                    resolved: resolved.clone(),
                    intermediate_input_slot_id: intermediate_slot_id,
                    final_output_slot_id,
                });
                shared_channels.insert(shared_key, (intermediate_slot_id, final_output_slot_id));
                (intermediate_slot_id, final_output_slot_id)
            };
            partial_calls.push(WriterPartialAggregateCall {
                input_slot_id: target_input_slot_id(requirement.input().ordinal())?,
                function_name: requirement.function_name().to_string(),
                resolved: resolved.clone(),
                intermediate_slot_id,
            });
            mappings.push(WriteUnpivotMapping {
                target: target.target,
                input_value_slot_id: final_output_slot_id,
                literals: vec![
                    UnpivotConstant::Int32List(requirement.artifact().input_fields().to_vec()),
                    UnpivotConstant::Scalar(TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::String(
                            requirement.artifact().blob_type().to_string(),
                        )),
                        data_type: DataType::Utf8,
                        nullable: false,
                    }),
                    UnpivotConstant::Utf8Map(Vec::new()),
                ],
            });
        }
        partial_by_target.insert(
            target.target,
            WriterPartialAggregatePlan {
                calls: partial_calls,
            },
        );
    }

    let schema = WriterMultiplexSchema::try_new(channels).map_err(|error| error.to_string())?;
    let unpivot = WriterUnpivotPlan {
        grouping_input_slot_id: write_relation_column_id(WRITE_RELATION_TARGET_INDEX),
        grouping_output_slot_id,
        passthrough_output_slot_id: root_write_result_column_id(ROOT_WRITE_RESULT_TARGET_INDEX),
        value_output_slot_id: root_write_result_column_id(ROOT_WRITE_RESULT_BODY_INDEX),
        literal_output_slot_ids: vec![
            root_write_result_column_id(ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX),
            root_write_result_column_id(
                novarocks_spi::connector::write_stack::ROOT_WRITE_RESULT_BLOB_TYPE_INDEX,
            ),
            root_write_result_column_id(ROOT_WRITE_RESULT_PROPERTIES_INDEX),
        ],
        mappings,
        max_output_rows: MAX_UNPIVOT_OUTPUT_ROWS,
        max_output_bytes: MAX_UNPIVOT_OUTPUT_BYTES,
    };
    validate_plan(&schema, &partial_by_target, &final_calls, &unpivot)?;
    Ok(WriterAuxiliaryPlan {
        schema,
        partial_by_target,
        final_plan: WriterFinalAggregatePlan {
            calls: final_calls,
            unpivot: Some(unpivot),
        },
    })
}

fn first_free_internal_slot(targets: &[WriterStatisticsTargetInput<'_>]) -> Result<u32, String> {
    let maximum = targets
        .iter()
        .map(|target| u32::try_from(target.input_schema.fields().len()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "write target column count does not fit u32".to_string())?
        .into_iter()
        .max()
        .unwrap_or(0);
    let first_reserved = novarocks_spi::connector::write_stack::ROOT_WRITE_RESULT_FIRST_COLUMN_ID;
    if maximum >= first_reserved {
        return Err(
            "write input slot collides with the reserved write relation ranges".to_string(),
        );
    }
    maximum
        .checked_add(1)
        .filter(|slot| *slot < first_reserved)
        .ok_or_else(|| "write aggregate plan has no free internal slot IDs".to_string())
}

fn target_input_slot_id(ordinal: usize) -> Result<u32, String> {
    ordinal
        .checked_add(1)
        .and_then(|slot| u32::try_from(slot).ok())
        .filter(|slot| {
            *slot < novarocks_spi::connector::write_stack::ROOT_WRITE_RESULT_FIRST_COLUMN_ID
        })
        .ok_or_else(|| "write aggregate target input slot ID overflowed".to_string())
}

fn allocate_slot(next: &mut u32) -> Result<u32, String> {
    let slot = *next;
    if slot >= novarocks_spi::connector::write_stack::ROOT_WRITE_RESULT_FIRST_COLUMN_ID {
        return Err("write aggregate plan exhausted internal slot IDs".to_string());
    }
    *next = slot
        .checked_add(1)
        .ok_or_else(|| "write aggregate internal slot ID overflowed".to_string())?;
    Ok(slot)
}

fn validate_plan(
    schema: &WriterMultiplexSchema,
    partial_by_target: &BTreeMap<WriteTargetOrdinal, WriterPartialAggregatePlan>,
    final_calls: &[WriterFinalAggregateCall],
    unpivot: &WriterUnpivotPlan,
) -> Result<(), String> {
    let channels = schema
        .auxiliary_channels()
        .iter()
        .map(|channel| (channel.slot_id(), channel.data_type()))
        .collect::<BTreeMap<_, _>>();
    let partial_slots = partial_by_target
        .values()
        .flat_map(|plan| plan.calls.iter())
        .map(|call| call.intermediate_slot_id)
        .collect::<BTreeSet<_>>();
    if partial_slots.len() != final_calls.len() || partial_slots.len() != channels.len() {
        return Err(
            "write aggregate calls do not exactly cover the typed auxiliary tail".to_string(),
        );
    }
    for call in final_calls {
        let Some(data_type) = channels.get(&call.intermediate_input_slot_id) else {
            return Err("write final aggregate reads an unknown auxiliary slot".to_string());
        };
        if **data_type != call.resolved.intermediate_type {
            return Err(
                "write aggregate intermediate type differs from its typed tail".to_string(),
            );
        }
    }
    let final_slots = final_calls
        .iter()
        .map(|call| call.final_output_slot_id)
        .collect::<BTreeSet<_>>();
    let mapping_slots = unpivot
        .mappings
        .iter()
        .map(|mapping| mapping.input_value_slot_id)
        .collect::<BTreeSet<_>>();
    if final_slots.len() != final_calls.len() || mapping_slots != final_slots {
        return Err(
            "write Unpivot mappings do not reference only final aggregate outputs".to_string(),
        );
    }
    let root = RootWriteResultSchema::new().arrow_schema();
    for mapping in &unpivot.mappings {
        if mapping.literals.len() != unpivot.literal_output_slot_ids.len() {
            return Err("write Unpivot literal arity mismatch".to_string());
        }
        for (literal, output_slot) in mapping
            .literals
            .iter()
            .zip(&unpivot.literal_output_slot_ids)
        {
            let index = RootWriteResultSchema::new()
                .slot_ids()
                .iter()
                .position(|slot| slot == output_slot)
                .ok_or_else(|| "write Unpivot literal output is not a Root slot".to_string())?;
            if literal.data_type() != *root.field(index).data_type() {
                return Err(
                    "write Unpivot literal type differs from its Root output slot".to_string(),
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::build_builtin_engine_function_catalog;
    use novarocks_functions::{
        AggregateOverloadMetadata, EngineFunctionCatalog, EngineFunctionCatalogBuilder,
        FunctionDefinition, FunctionVisibility, FunctionVolatility,
    };
    use novarocks_spi::connector::{
        StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
    };

    fn binary_catalog() -> EngineFunctionCatalog {
        let overload = AggregateOverloadMetadata::try_new(
            "test.binary_stat.int64.v1",
            [DataType::Int64],
            DataType::Binary,
            DataType::Binary,
            "test.binary_stat.state.v1",
        )
        .expect("overload");
        let definition = FunctionDefinition::try_new_exact_aggregate(
            "binary_stat",
            FunctionVisibility::Hidden,
            FunctionVolatility::Immutable,
            [overload],
        )
        .expect("definition");
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(definition).expect("register");
        builder.seal().expect("catalog")
    }

    fn requirement(field_id: i32) -> StatisticsRequiredAggregation {
        StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(0, "k", DataType::Int64, false).expect("input"),
            "binary_stat",
            StatisticsArtifactIdentity::try_new(vec![field_id], "generic-binary")
                .expect("identity"),
        )
        .expect("requirement")
    }

    fn input_schema() -> Schema {
        Schema::new(vec![arrow::datatypes::Field::new(
            "k",
            DataType::Int64,
            false,
        )])
    }

    #[test]
    fn requirements_lower_to_resolved_plan_facts_and_a_nonempty_typed_tail() {
        let input_schema = input_schema();
        let required = vec![
            StatisticsRequiredAggregation::try_new(
                StatisticsScanColumn::try_new(0, "k", DataType::Int64, false).expect("input"),
                "count",
                StatisticsArtifactIdentity::try_new(vec![11], "generic-binary").expect("identity"),
            )
            .expect("requirement"),
        ];
        let target = WriteTargetOrdinal::try_new(0).expect("target");
        let plan = plan_writer_statistics(
            &[WriterStatisticsTargetInput {
                target,
                input_schema: &input_schema,
                requirements: &required,
            }],
            &build_builtin_engine_function_catalog().expect("builtin function catalog"),
        )
        .expect_err("count does not produce a binary artifact body");
        assert!(plan.contains("binary Root value slot"));
    }

    #[test]
    fn identical_multi_target_aggregates_share_one_typed_channel() {
        let schema = input_schema();
        let first = vec![requirement(11)];
        let second = vec![requirement(12)];
        let target = |value| WriteTargetOrdinal::try_new(value).expect("target");
        let plan = plan_writer_statistics(
            &[
                WriterStatisticsTargetInput {
                    target: target(0),
                    input_schema: &schema,
                    requirements: &first,
                },
                WriterStatisticsTargetInput {
                    target: target(1),
                    input_schema: &schema,
                    requirements: &second,
                },
            ],
            &binary_catalog(),
        )
        .expect("plan");
        assert_eq!(
            plan.partial_for(target(0)).expect("known target").calls()[0].input_slot_id(),
            1,
            "aggregate input is the one-based target-schema ordinal, not a child-plan column id"
        );
        assert_eq!(plan.schema().auxiliary_channels().len(), 1);
        assert_eq!(plan.final_plan().calls().len(), 1);
        assert_eq!(plan.final_plan().unpivot().unwrap().mappings().len(), 2);
        assert_eq!(
            plan.final_plan().unpivot().unwrap().max_output_bytes(),
            novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
        );
        let unknown = plan
            .partial_for(target(2))
            .expect_err("unknown target must not inherit an empty partial plan");
        assert!(unknown.contains("no partial aggregate entry for target 2"));
    }

    #[test]
    fn repeated_same_signature_within_one_target_keeps_distinct_channels() {
        let schema = input_schema();
        let requirements = vec![requirement(11), requirement(12)];
        let plan = plan_writer_statistics(
            &[WriterStatisticsTargetInput {
                target: WriteTargetOrdinal::try_new(0).expect("target"),
                input_schema: &schema,
                requirements: &requirements,
            }],
            &binary_catalog(),
        )
        .expect("plan");
        assert_eq!(plan.schema().auxiliary_channels().len(), 2);
        assert_eq!(plan.final_plan().calls().len(), 2);
    }

    #[test]
    fn multi_target_width_is_the_max_occurrence_count_not_the_sum() {
        let schema = input_schema();
        let first = vec![requirement(11), requirement(12)];
        let second = vec![requirement(13)];
        let target = |value| WriteTargetOrdinal::try_new(value).expect("target");
        let plan = plan_writer_statistics(
            &[
                WriterStatisticsTargetInput {
                    target: target(0),
                    input_schema: &schema,
                    requirements: &first,
                },
                WriterStatisticsTargetInput {
                    target: target(1),
                    input_schema: &schema,
                    requirements: &second,
                },
            ],
            &binary_catalog(),
        )
        .expect("plan");
        assert_eq!(plan.schema().auxiliary_channels().len(), 2);
        assert_eq!(plan.final_plan().calls().len(), 2);
        let unpivot = plan.final_plan().unpivot().expect("unpivot");
        let target_zero_first = unpivot
            .mappings()
            .iter()
            .find(|mapping| mapping.target() == target(0))
            .expect("target zero occurrence zero");
        let target_one_first = unpivot
            .mappings()
            .iter()
            .find(|mapping| mapping.target() == target(1))
            .expect("target one occurrence zero");
        assert_eq!(
            target_zero_first.input_value_slot_id(),
            target_one_first.input_value_slot_id()
        );
    }

    #[test]
    fn target_order_is_part_of_the_canonical_plan_contract() {
        let schema = input_schema();
        let first = vec![requirement(11)];
        let second = vec![requirement(12)];
        let target = |value| WriteTargetOrdinal::try_new(value).expect("target");
        let error = plan_writer_statistics(
            &[
                WriterStatisticsTargetInput {
                    target: target(1),
                    input_schema: &schema,
                    requirements: &second,
                },
                WriterStatisticsTargetInput {
                    target: target(0),
                    input_schema: &schema,
                    requirements: &first,
                },
            ],
            &binary_catalog(),
        )
        .expect_err("unordered targets");
        assert!(error.contains("strictly ascending"));
    }
}
