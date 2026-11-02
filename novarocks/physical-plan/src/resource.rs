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

use arrow_schema::{
    DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE, DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE,
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, TimeUnit,
};
use novarocks_connector_contract::ConnectorEncodedPayload;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::collections::BTreeSet;

use crate::validation::ValidationContext;
use crate::{
    AggregateBinding, BoundFunction, BoundTableFunction, ExprKind, Fragment, FragmentCuts,
    FragmentSink, FunctionArgumentType, NodeKind, PhysicalPlan, ProviderReadReference, Relation,
    RuntimeFilter, RuntimeFilterCoverage, RuntimeFilterCoverageNode, RuntimeFilterDomain, SortMode,
    UnpivotConstant, ValidationError, ValueType, WriterFinishSpec, WriterRelationSchema,
};

pub const MAX_ANNOTATIONS: usize = 4_096;
pub const MAX_ANNOTATION_KEY_BYTES: usize = 256;
pub const MAX_ANNOTATION_VALUE_BYTES: usize = 16 * 1024;
pub const MAX_ANNOTATION_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_FRAGMENT_DYNAMIC_ITEMS: usize = 4 * 1024 * 1024;
pub const MAX_FRAGMENT_DYNAMIC_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_PLAN_DYNAMIC_ITEMS: usize = 16 * 1024 * 1024;
pub const MAX_PLAN_DYNAMIC_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_PLAN_DERIVED_CUT_ITEMS: usize = 32 * 1024 * 1024;
pub const MAX_PLAN_DERIVED_CUT_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_DATA_TYPE_DEPTH: usize = novarocks_type_contract::MAX_VALUE_TYPE_DEPTH;
pub const MAX_DATA_TYPE_NODES: usize = novarocks_type_contract::MAX_VALUE_TYPE_NODES;
pub const MAX_DATA_TYPE_FIELD_NAME_BYTES: usize =
    novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES;
pub const MAX_DATA_TYPE_FIELD_METADATA_ENTRIES: usize =
    novarocks_type_contract::MAX_ARROW_FIELD_METADATA_ENTRIES;
pub const MAX_DATA_TYPE_FIELD_METADATA_KEY_BYTES: usize =
    novarocks_type_contract::MAX_ARROW_FIELD_METADATA_KEY_BYTES;
pub const MAX_DATA_TYPE_FIELD_METADATA_VALUE_BYTES: usize =
    novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES;
pub const MAX_DATA_TYPE_FIELD_METADATA_BYTES: usize =
    novarocks_type_contract::MAX_ARROW_FIELD_METADATA_BYTES;
pub const MAX_TIMESTAMP_TIMEZONE_BYTES: usize =
    novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES;
pub const MAX_FIXED_SIZE_LENGTH: i32 = 1 << 20;

#[derive(Clone, Copy, Debug)]
struct ResourceUsage {
    items: usize,
    bytes: usize,
    max_items: usize,
    max_bytes: usize,
}

pub(crate) struct CutResourcePreflight {
    usage: ResourceUsage,
}

#[derive(Clone, Copy)]
pub(crate) struct CutResourceUsage {
    pub(crate) items: usize,
    pub(crate) bytes: usize,
}

impl CutResourcePreflight {
    pub(crate) fn new() -> Self {
        Self {
            usage: ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES),
        }
    }

    pub(crate) fn add_fragment(&mut self, fragment: &Fragment, errors: &mut ValidationContext) {
        self.usage.merge(fragment_usage(fragment, errors));
    }

    pub(crate) fn add_cuts(
        &mut self,
        fragment: &Fragment,
        cuts: &FragmentCuts,
        errors: &mut ValidationContext,
    ) {
        self.usage.merge(fragment_cut_usage(fragment, cuts, errors));
    }

    /// Each original fragment type occurrence uses the caller's same two-law author.
    /// Paths, non-type diagnostics, and mandatory call requests retain Plain authors.
    pub(crate) fn add_fragment_in(
        &mut self,
        fragment: &Fragment,
        errors: &mut ValidationContext,
        source_retained_bytes: usize,
        resources: &mut ControlResourceCounter,
        admit: &mut TypeAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ControlResourceError> {
        self.usage.merge(fragment_usage_core(
            fragment,
            errors,
            &mut CallerTypeValidation {
                source: source_retained_bytes,
                resources,
                admit,
                work,
            },
        )?);
        Ok(())
    }
    pub(crate) fn add_cuts_in(
        &mut self,
        fragment: &Fragment,
        cuts: &FragmentCuts,
        errors: &mut ValidationContext,
        source_retained_bytes: usize,
        resources: &mut ControlResourceCounter,
        admit: &mut TypeAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ControlResourceError> {
        self.usage.merge(fragment_cut_usage_core(
            fragment,
            cuts,
            errors,
            &mut CallerTypeValidation {
                source: source_retained_bytes,
                resources,
                admit,
                work,
            },
        )?);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn add_filter_in(
        &mut self,
        filter: &RuntimeFilter,
        path: &str,
        errors: &mut ValidationContext,
        source_retained_bytes: usize,
        resources: &mut ControlResourceCounter,
        admit: &mut TypeAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ControlResourceError> {
        add_runtime_filter_usage_core(
            filter,
            path,
            &mut self.usage,
            errors,
            &mut CallerTypeValidation {
                source: source_retained_bytes,
                resources,
                admit,
                work,
            },
        )
    }

    /// Count sparse addresses individually and immutable backings once in the
    /// same package envelope. These are source facts, not allocation grants.
    pub(crate) fn add_constants_observed(
        &mut self,
        pools: &crate::ConstantPools,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), CompileControlError> {
        add_constant_pool_usage(pools, &mut self.usage, work)
    }
    pub(crate) fn add_unpivot_sources_observed(
        &mut self,
        fragment: &Fragment,
        pools: &crate::ConstantPools,
        limits: crate::PlanLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), crate::ConstantReferenceError> {
        add_unpivot_source_usage(
            fragment,
            pools,
            limits,
            &mut self.usage,
            crate::constants::ConstantValidationMode::Plain,
            work,
        )
    }

    pub(crate) fn add_unpivot_sources_in(
        &mut self,
        fragment: &Fragment,
        pools: &crate::ConstantPools,
        limits: crate::PlanLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), crate::ConstantReferenceError> {
        add_unpivot_source_usage(
            fragment,
            pools,
            limits,
            &mut self.usage,
            crate::constants::ConstantValidationMode::Caller,
            work,
        )
    }

    pub(crate) fn add_items(&mut self, count: usize) {
        self.usage.add_items(count);
    }

    pub(crate) fn add_bytes(&mut self, count: usize) {
        self.usage.add_bytes(count);
    }

    pub(crate) fn add_distribution(&mut self, distribution: &crate::Distribution) {
        add_distribution_usage(distribution, &mut self.usage);
    }

    pub(crate) fn add_filter(
        &mut self,
        filter: &RuntimeFilter,
        path: &str,
        errors: &mut ValidationContext,
    ) {
        add_runtime_filter_usage(filter, path, &mut self.usage, errors);
    }

    pub(crate) fn add_value_type(
        &mut self,
        ty: &ValueType,
        path: &str,
        errors: &mut ValidationContext,
    ) {
        validate_value_type(ty, path, &mut self.usage, errors);
    }

    /// The same original two type laws on caller-owned resource/work loans.
    /// This port owns no entry/footer or whole-Package source certificate.
    pub(crate) fn add_value_type_in(
        &mut self,
        ty: &ValueType,
        path: &str,
        errors: &mut ValidationContext,
        source_retained_bytes: usize,
        resources: &mut ControlResourceCounter,
        admit: &mut TypeAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ControlResourceError> {
        validate_value_type_in(
            ty,
            path,
            &mut self.usage,
            errors,
            &mut CallerTypeValidation {
                source: source_retained_bytes,
                resources,
                admit,
                work,
            },
        )
    }

    pub(crate) fn validate(self, path: &str, errors: &mut ValidationContext) -> CutResourceUsage {
        let result = CutResourceUsage {
            items: self.usage.items,
            bytes: self.usage.bytes,
        };
        validate_usage(
            path,
            self.usage,
            MAX_FRAGMENT_DYNAMIC_ITEMS,
            MAX_FRAGMENT_DYNAMIC_BYTES,
            errors,
        );
        result
    }
}

impl ResourceUsage {
    const fn limited(max_items: usize, max_bytes: usize) -> Self {
        Self {
            items: 0,
            bytes: 0,
            max_items,
            max_bytes,
        }
    }

    fn add_items(&mut self, count: usize) {
        self.items = self
            .items
            .saturating_add(count)
            .min(self.max_items.saturating_add(1));
    }

    fn add_bytes(&mut self, count: usize) {
        self.bytes = self
            .bytes
            .saturating_add(count)
            .min(self.max_bytes.saturating_add(1));
    }

    fn add_item_counts<const N: usize>(&mut self, counts: [usize; N]) {
        for count in counts {
            self.add_items(count);
            if self.exhausted() {
                return;
            }
        }
    }

    fn add_byte_counts<const N: usize>(&mut self, counts: [usize; N]) {
        for count in counts {
            self.add_bytes(count);
            if self.exhausted() {
                return;
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.add_items(other.items);
        self.add_bytes(other.bytes);
    }

    const fn exhausted(self) -> bool {
        self.items > self.max_items || self.bytes > self.max_bytes
    }
}

pub(crate) fn validate_fragment_resources(fragment: &Fragment, errors: &mut ValidationContext) {
    let path = format!("fragments[{}].resources", fragment.id().get());
    let usage = fragment_usage(fragment, errors);
    validate_usage(
        &path,
        usage,
        MAX_FRAGMENT_DYNAMIC_ITEMS,
        MAX_FRAGMENT_DYNAMIC_BYTES,
        errors,
    );
}

pub(crate) fn validate_fragment_cut_resources(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    errors: &mut ValidationContext,
) {
    let usage = fragment_cut_usage(fragment, cuts, errors);
    validate_usage(
        "fragment.cuts.resources",
        usage,
        MAX_FRAGMENT_DYNAMIC_ITEMS,
        MAX_FRAGMENT_DYNAMIC_BYTES,
        errors,
    );
}

// Plain entry points keep their original scopes and diagnostic order.
fn fragment_usage(fragment: &Fragment, errors: &mut ValidationContext) -> ResourceUsage {
    match fragment_usage_core(fragment, errors, &mut PlainTypeValidation) {
        Ok(usage) => usage,
        Err(never) => match never {},
    }
}
fn fragment_cut_usage(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    errors: &mut ValidationContext,
) -> ResourceUsage {
    match fragment_cut_usage_core(fragment, cuts, errors, &mut PlainTypeValidation) {
        Ok(usage) => usage,
        Err(never) => match never {},
    }
}
fn plan_usage(plan: &PhysicalPlan, errors: &mut ValidationContext) -> ResourceUsage {
    match plan_usage_core(plan, errors, &mut PlainTypeValidation) {
        Ok(usage) => usage,
        Err(never) => match never {},
    }
}
fn add_runtime_filter_usage(
    filter: &RuntimeFilter,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    match add_runtime_filter_usage_core(filter, path, usage, errors, &mut PlainTypeValidation) {
        Ok(()) => (),
        Err(never) => match never {},
    }
}
#[cfg(test)]
fn add_aggregate_binding_usage(
    binding: &AggregateBinding,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    match add_aggregate_binding_usage_core(binding, path, usage, errors, &mut PlainTypeValidation) {
        Ok(()) => (),
        Err(never) => match never {},
    }
}

fn fragment_cut_usage_core<P: TypeValidationPolicy>(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<ResourceUsage, P::Error> {
    let prefix = format!("fragments[{}].cuts", fragment.id().get());
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    usage.add_item_counts([cuts.inbound.len(), cuts.outbound.len()]);
    for (index, cut) in cuts.inbound.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        usage.add_items(cut.imports.len());
        match &cut.payload {
            crate::ExchangePayloadContract::Relational => {}
            crate::ExchangePayloadContract::ProviderRoute(writer) => {
                usage.add_items(writer.fields.len())
            }
            crate::ExchangePayloadContract::WriterResults(writer) => {
                add_writer_result_cut_usage_core(
                    writer,
                    &format!("{prefix}.inbound[{index}].writer_result"),
                    &mut usage,
                    errors,
                    policy,
                )?
            }
            crate::ExchangePayloadContract::LogicalSummary(_) => usage.add_items(6),
        }
        add_distribution_usage(&cut.partitioning.source, &mut usage);
        add_distribution_usage(&cut.partitioning.destination, &mut usage);
        for (ordinal, import) in cut.imports.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            validate_value_type_core(
                &import.source.ty,
                &format!("{prefix}.inbound[{index}].imports[{ordinal}].type"),
                &mut usage,
                errors,
                policy,
            )?;
        }
    }
    for (index, cut) in cuts.outbound.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        usage.add_item_counts([cut.projection.len(), cut.destination_imports.len()]);
        match &cut.payload {
            crate::ExchangePayloadContract::Relational => {}
            crate::ExchangePayloadContract::ProviderRoute(writer) => {
                usage.add_items(writer.fields.len())
            }
            crate::ExchangePayloadContract::WriterResults(writer) => {
                add_writer_result_cut_usage_core(
                    writer,
                    &format!("{prefix}.outbound[{index}].writer_result"),
                    &mut usage,
                    errors,
                    policy,
                )?
            }
            crate::ExchangePayloadContract::LogicalSummary(_) => usage.add_items(6),
        }
        add_distribution_usage(&cut.partitioning.source, &mut usage);
        add_distribution_usage(&cut.partitioning.destination, &mut usage);
        for (ordinal, value) in cut.projection.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            validate_value_type_core(
                &value.ty,
                &format!("{prefix}.outbound[{index}].projection[{ordinal}].type"),
                &mut usage,
                errors,
                policy,
            )?;
        }
        for (ordinal, import) in cut.destination_imports.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            validate_value_type_core(
                &import.source.ty,
                &format!("{prefix}.outbound[{index}].destination_imports[{ordinal}].type"),
                &mut usage,
                errors,
                policy,
            )?;
        }
    }
    usage.add_item_counts([
        cuts.runtime_filters.len(),
        cuts.runtime_filter_bindings.len(),
    ]);
    for (index, filter) in cuts.runtime_filters.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        add_runtime_filter_usage_core(
            filter,
            &format!("{prefix}.runtime_filters[{index}]"),
            &mut usage,
            errors,
            policy,
        )?;
    }
    Ok(usage)
}

fn add_writer_result_cut_usage_core<P: TypeValidationPolicy>(
    writer: &crate::WriterResultCut,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    usage.add_items(writer.fields.len());
    for (ordinal, field) in writer.fields.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        usage.add_bytes(field.name.len());
        validate_value_type_core(
            &field.ty,
            &format!("{path}.fields[{ordinal}].type"),
            usage,
            errors,
            policy,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_plan_resources(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let usage = plan_usage(plan, errors);
    validate_usage(
        "resources",
        usage,
        MAX_PLAN_DYNAMIC_ITEMS,
        MAX_PLAN_DYNAMIC_BYTES,
        errors,
    );
}

/// The caller owns entry/completion and keeps the original control. The old
/// structure collector is shared; only this explicit port admits pool facts.
pub(crate) fn validate_plan_resources_observed(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::ConstantReferenceError> {
    validate_plan_resources_core(
        plan,
        errors,
        crate::constants::ConstantValidationMode::Plain,
        work,
    )
}

pub(crate) fn validate_plan_resources_in(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::ConstantReferenceError> {
    validate_plan_resources_core(
        plan,
        errors,
        crate::constants::ConstantValidationMode::Caller,
        work,
    )
}

fn validate_plan_resources_core(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
    mode: crate::constants::ConstantValidationMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::ConstantReferenceError> {
    let mut usage = plan_usage(plan, errors);
    add_constant_pool_usage(plan.constants(), &mut usage, work)?;
    for fragment in plan.fragments().values() {
        // Reuse the original fragment author for its baseline. Only selected
        // consumer expansion is added to the already counted plan baseline;
        // checked retained backing remains counted once by the pool author.
        work.flush()?;
        let mut fragment_usage = fragment_usage(fragment, errors);
        work.flush()?;
        let before = fragment_usage;
        add_unpivot_source_usage(
            fragment,
            plan.constants(),
            crate::PlanLimits::FROZEN,
            &mut fragment_usage,
            mode,
            work,
        )?;
        validate_usage(
            "constants.fragment.resources",
            fragment_usage,
            MAX_FRAGMENT_DYNAMIC_ITEMS,
            MAX_FRAGMENT_DYNAMIC_BYTES,
            errors,
        );
        usage.add_items(fragment_usage.items.saturating_sub(before.items));
        usage.add_bytes(fragment_usage.bytes.saturating_sub(before.bytes));
        work.step()?;
        if usage.exhausted() {
            break;
        }
    }
    validate_usage(
        "resources",
        usage,
        MAX_PLAN_DYNAMIC_ITEMS,
        MAX_PLAN_DYNAMIC_BYTES,
        errors,
    );
    Ok(())
}

fn add_unpivot_source_usage(
    fragment: &Fragment,
    pools: &crate::ConstantPools,
    limits: crate::PlanLimits,
    usage: &mut ResourceUsage,
    mode: crate::constants::ConstantValidationMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::ConstantReferenceError> {
    if usage.exhausted() {
        return Ok(());
    }
    let resource =
        || crate::ConstantReferenceError::Control(CompileControlError::ResourceExhausted);
    let mut previous = None;
    let mut node_items = 0_usize;
    let mut node_bytes = 0_u64;
    crate::constants::visit_unpivot_constants_observed(
        fragment,
        work,
        |node, constant, output, work| {
            if crate::constants::collection_reference(constant).is_none() {
                return Ok(());
            }
            if previous != Some(node) {
                previous = Some(node);
                node_items = 0;
                node_bytes = 0;
            }
            let item_bound = limits
                .unpivot_collection_items
                .checked_sub(node_items)
                .ok_or_else(resource)?
                .min(
                    usage
                        .max_items
                        .checked_sub(usage.items)
                        .ok_or_else(resource)?,
                );
            let byte_bound = (crate::MAX_UNPIVOT_LITERAL_BYTES as u64)
                .checked_sub(node_bytes)
                .ok_or_else(resource)?
                .min(
                    u64::try_from(
                        usage
                            .max_bytes
                            .checked_sub(usage.bytes)
                            .ok_or_else(resource)?,
                    )
                    .map_err(|_| resource())?,
                );
            let selected = match mode {
                crate::constants::ConstantValidationMode::Plain => {
                    crate::constants::unpivot_collection_usage_observed(
                        pools, constant, output, item_bound, byte_bound, work,
                    )?
                }
                crate::constants::ConstantValidationMode::Caller => {
                    crate::constants::unpivot_collection_usage_in(
                        pools, constant, output, item_bound, byte_bound, work,
                    )?
                }
            };
            node_items = node_items
                .checked_add(selected.items)
                .ok_or_else(resource)?;
            node_bytes = node_bytes
                .checked_add(selected.payload_bytes)
                .ok_or_else(resource)?;
            // Preserve the original per-occurrence expansion invoice, even for
            // repeated addresses or alias pool keys. This conservative request
            // bound is separate from once-per-backing retained pool storage.
            usage.add_items(selected.items);
            usage.add_bytes(usize::try_from(selected.payload_bytes).map_err(|_| resource())?);
            work.step()?;
            Ok(())
        },
    )
}

fn plan_usage_core<P: TypeValidationPolicy>(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<ResourceUsage, P::Error> {
    let mut usage = ResourceUsage::limited(MAX_PLAN_DYNAMIC_ITEMS, MAX_PLAN_DYNAMIC_BYTES);
    usage.add_item_counts([
        plan.fragments().len(),
        plan.edges().len(),
        plan.runtime_filters().len(),
        plan.annotations().len(),
    ]);
    for fragment in plan.fragments().values() {
        if usage.exhausted() {
            break;
        }
        usage.merge(fragment_usage_core(fragment, errors, policy)?);
    }
    for edge in plan.edges().values() {
        if usage.exhausted() {
            break;
        }
        usage.add_item_counts([
            edge.source.projection.len(),
            edge.destination.receive_mapping.len(),
            distribution_items(&edge.partitioning.source),
            distribution_items(&edge.partitioning.destination),
        ]);
    }
    if let Some(result) = plan.result_port() {
        if let Some(schema) = &result.scalar_schema {
            usage.add_item_counts([schema.type_nodes()]);
            usage.add_byte_counts([schema.backing_bytes()]);
        }
        usage.add_item_counts([result.output.columns.len(), result.fields.len()]);
        for (index, field) in result.fields.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            usage.add_byte_counts([field.name.len(), field.alias.as_deref().map_or(0, str::len)]);
            validate_value_type_core(
                &field.ty,
                &format!("result.fields[{index}].type"),
                &mut usage,
                errors,
                policy,
            )?;
        }
    }
    for (id, filter) in plan.runtime_filters() {
        if usage.exhausted() {
            break;
        }
        add_runtime_filter_usage_core(
            filter,
            &format!("runtime_filters[{}]", id.get()),
            &mut usage,
            errors,
            policy,
        )?;
    }
    for annotation in plan.annotations() {
        if usage.exhausted() {
            break;
        }
        usage.add_byte_counts([annotation.key.len(), annotation.value.len()]);
    }
    Ok(usage)
}

fn add_constant_pool_usage(
    pools: &crate::ConstantPools,
    usage: &mut ResourceUsage,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), CompileControlError> {
    // This O(1) count occurs before any deduplication scratch allocation. It
    // shares the existing plan/package item limit with all earlier facts.
    usage.add_items(pools.entries().len());
    work.step()?;
    if usage.exhausted() {
        return Ok(());
    }
    let mut retained = BTreeSet::new();
    for pool in pools.entries().values() {
        let first = retained.insert(pool.backing_identity());
        work.step()?;
        if first {
            let facts = pool.resource_facts();
            // Rows describe addressable source items; nodes and descriptors
            // describe backing structure. The expanded validation work bound
            // is not retained data and is not charged as another item table.
            usage.add_item_counts([
                resource_count(facts.rows),
                resource_count(facts.array_nodes),
                resource_count(facts.buffer_count),
            ]);
            usage.add_byte_counts([
                resource_count(facts.retained_buffer_capacity_bytes),
                resource_count(facts.metadata_bytes),
            ]);
            work.step()?;
        }
        if usage.exhausted() {
            break;
        }
    }
    // The scratch tree is bounded by the admitted sparse table and stores
    // opaque owner retention identities, never max-ID-indexed slots or CV Eq.
    // Its allocation is not a MEM-accounted grant/free protocol.
    Ok(())
}

fn resource_count(count: u64) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}

fn validate_usage(
    path: &str,
    usage: ResourceUsage,
    max_items: usize,
    max_bytes: usize,
    errors: &mut ValidationContext,
) {
    if usage.items > max_items {
        errors.push(ValidationError::resource_limit(
            path,
            format!(
                "contains {} dynamic items, exceeding {max_items}",
                usage.items
            ),
        ));
    }
    if usage.bytes > max_bytes {
        errors.push(ValidationError::resource_limit(
            path,
            format!(
                "contains {} dynamic bytes, exceeding {max_bytes}",
                usage.bytes
            ),
        ));
    }
}

fn fragment_usage_core<P: TypeValidationPolicy>(
    fragment: &Fragment,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<ResourceUsage, P::Error> {
    let prefix = format!("fragments[{}]", fragment.id().get());
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    usage.add_item_counts([
        fragment.values().len(),
        fragment.expressions().len(),
        fragment.nodes().len(),
        fragment.runtime_filters().len(),
    ]);
    if let crate::StagePlacementPolicy::FollowSources { sources } = fragment.stage_placement() {
        usage.add_item_counts([sources.len()]);
        usage.add_byte_counts([sources
            .len()
            .saturating_mul(std::mem::size_of::<crate::FragmentId>())]);
    }
    // Mandatory request type validation remains the original Plain author.
    add_call_request_table_usage(fragment.call_requests(), &mut usage, errors);
    for (id, value) in fragment.values() {
        if usage.exhausted() {
            return Ok(usage);
        }
        if let crate::ValueOrigin::ProviderField { field, .. } = &value.origin {
            add_encoded_payload_usage(&field.column_payload, &mut usage);
        }
        validate_value_type_core(
            &value.ty,
            &format!("{prefix}.values[{}].type", id.get()),
            &mut usage,
            errors,
            policy,
        )?;
    }
    for (id, expression) in fragment.expressions().iter() {
        if usage.exhausted() {
            return Ok(usage);
        }
        validate_value_type_core(
            &expression.ty,
            &format!("{prefix}.expressions[{}].type", id.get()),
            &mut usage,
            errors,
            policy,
        )?;
        add_expression_usage_core(
            &expression.kind,
            &format!("{prefix}.expressions[{}]", id.get()),
            &mut usage,
            errors,
            policy,
        )?;
    }
    for (id, node) in fragment.nodes() {
        if usage.exhausted() {
            return Ok(usage);
        }
        usage.add_item_counts([
            node.inputs.len(),
            node.required_inputs.len(),
            node.output.columns.len(),
        ]);
        add_properties_usage(&node.output_properties, &mut usage);
        for properties in &node.required_inputs {
            if usage.exhausted() {
                return Ok(usage);
            }
            add_properties_usage(properties, &mut usage);
        }
        add_node_usage_core(
            &node.kind,
            &format!("{prefix}.nodes[{}]", id.get()),
            &mut usage,
            errors,
            policy,
        )?;
    }
    add_sink_usage(fragment.sink(), &mut usage);
    Ok(usage)
}

fn add_expression_usage_core<P: TypeValidationPolicy>(
    kind: &ExprKind,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    if usage.exhausted() {
        return Ok(());
    }
    match kind {
        // The sparse reference is a leaf. Its selected payload is retained
        // by the enclosing constant table, where backing aliases are deduped.
        ExprKind::Constant(_) => {}
        ExprKind::FunctionCall { function, args } => {
            usage.add_items(args.len());
            add_function_usage_core(function, &format!("{path}.function"), usage, errors, policy)?;
        }
        ExprKind::Lambda {
            parameter_types, ..
        } => {
            usage.add_items(parameter_types.len());
            for (index, ty) in parameter_types.iter().enumerate() {
                if usage.exhausted() {
                    return Ok(());
                }
                validate_value_type_core(
                    ty,
                    &format!("{path}.parameter_types[{index}]"),
                    usage,
                    errors,
                    policy,
                )?;
            }
        }
        ExprKind::Cast { target, .. } => {
            validate_data_type_core(target, &format!("{path}.target"), usage, errors, policy)?;
        }
        ExprKind::InList { list, .. } => usage.add_items(list.len()),
        ExprKind::Case { when_then, .. } => usage.add_items(when_then.len().saturating_mul(2)),
        ExprKind::WindowCall {
            function,
            args,
            function_order_by,
            aggregate_binding,
            ..
        } => {
            usage.add_item_counts([args.len(), function_order_by.len()]);
            add_function_usage_core(function, &format!("{path}.function"), usage, errors, policy)?;
            if let Some(binding) = aggregate_binding {
                add_aggregate_binding_usage_core(
                    binding,
                    &format!("{path}.aggregate_binding"),
                    usage,
                    errors,
                    policy,
                )?;
            }
        }
        _ => {}
    };
    Ok(())
}

fn add_function_usage_core<P: TypeValidationPolicy>(
    function: &BoundFunction,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    usage.add_item_counts([
        function.argument_types.len(),
        function
            .legacy_metadata
            .as_ref()
            .map_or(0, |metadata| metadata.semantic_parameters.len()),
    ]);
    usage.add_byte_counts([
        function.function_id.as_str().len(),
        function.overload.as_str().len(),
    ]);
    add_argument_types_usage_core(&function.argument_types, path, usage, errors, policy)?;
    validate_value_type_core(
        &function.result_type,
        &format!("{path}.result_type"),
        usage,
        errors,
        policy,
    )?;
    Ok(())
}

fn add_table_function_usage_core<P: TypeValidationPolicy>(
    function: &BoundTableFunction,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    usage.add_item_counts([
        function.argument_types.len(),
        function.result_types.len(),
        function
            .legacy_metadata
            .as_ref()
            .map_or(0, |metadata| metadata.semantic_parameters.len()),
    ]);
    usage.add_byte_counts([
        function.function_id.as_str().len(),
        function.overload.as_str().len(),
    ]);
    add_argument_types_usage_core(&function.argument_types, path, usage, errors, policy)?;
    for (index, ty) in function.result_types.iter().enumerate() {
        if usage.exhausted() {
            return Ok(());
        }
        validate_value_type_core(
            ty,
            &format!("{path}.result_types[{index}]"),
            usage,
            errors,
            policy,
        )?;
    }
    Ok(())
}

fn add_argument_types_usage_core<P: TypeValidationPolicy>(
    argument_types: &[FunctionArgumentType],
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    for (index, argument) in argument_types.iter().enumerate() {
        if usage.exhausted() {
            return Ok(());
        }
        match argument {
            FunctionArgumentType::Value(ty) => validate_value_type_core(
                ty,
                &format!("{path}.argument_types[{index}]"),
                usage,
                errors,
                policy,
            )?,
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                usage.add_items(parameter_types.len());
                for (parameter, ty) in parameter_types.iter().enumerate() {
                    if usage.exhausted() {
                        return Ok(());
                    }
                    validate_value_type_core(
                        ty,
                        &format!("{path}.argument_types[{index}].parameter_types[{parameter}]"),
                        usage,
                        errors,
                        policy,
                    )?;
                }
                validate_value_type_core(
                    result_type,
                    &format!("{path}.argument_types[{index}].result_type"),
                    usage,
                    errors,
                    policy,
                )?;
            }
        }
    }
    Ok(())
}

fn add_aggregate_binding_usage_core<P: TypeValidationPolicy>(
    binding: &AggregateBinding,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    usage.add_bytes(binding.state_format.as_str().len());
    if let Some(receipt) = &binding.state_interpretation {
        usage.add_items(receipt.order_keys.len());
        usage.add_bytes(receipt.order_keys.len().saturating_mul(std::mem::size_of::<
            novarocks_type_contract::AggregateStateOrderKey,
        >()));
    }
    add_function_usage_core(
        &binding.function,
        &format!("{path}.function"),
        usage,
        errors,
        policy,
    )?;
    validate_value_type_core(
        &binding.intermediate_type,
        &format!("{path}.intermediate_type"),
        usage,
        errors,
        policy,
    )?;
    Ok(())
}

fn add_node_usage_core<P: TypeValidationPolicy>(
    kind: &NodeKind,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    if usage.exhausted() {
        return Ok(());
    }
    if let Some((group_by, calls)) = kind.aggregate_contract() {
        usage.add_item_counts([group_by.len(), calls.len()]);
        for (index, call) in calls.iter().enumerate() {
            if usage.exhausted() {
                return Ok(());
            }
            usage.add_item_counts([call.arguments.len(), call.order_by.len()]);
            add_aggregate_binding_usage_core(
                &call.binding,
                &format!("{path}.calls[{index}].binding"),
                usage,
                errors,
                policy,
            )?;
        }
    }
    match kind {
        NodeKind::Scan {
            relation,
            provider_outputs,
            residuals,
            derived_values,
            ..
        } => {
            usage.add_item_counts([
                provider_outputs.len(),
                residuals.len(),
                derived_values.len(),
            ]);
            for (column, _) in provider_outputs {
                if usage.exhausted() {
                    return Ok(());
                }
                add_encoded_payload_usage(&column.column_payload, usage);
            }
            add_relation_usage_core(relation, &format!("{path}.relation"), usage, errors, policy)?;
        }
        NodeKind::Project { expressions } => usage.add_items(expressions.len()),
        NodeKind::Aggregate { .. } => {}
        NodeKind::HashJoin {
            keys,
            null_extended,
            ..
        } => usage.add_item_counts([keys.len(), null_extended.len()]),
        NodeKind::NestLoopJoin { null_extended, .. } => usage.add_items(null_extended.len()),
        NodeKind::Sort { order_by, mode } => {
            usage.add_items(order_by.len());
            match mode {
                SortMode::Analytic { partition_by }
                | SortMode::PartitionTopN { partition_by, .. } => {
                    usage.add_items(partition_by.len());
                }
                SortMode::Global => {}
            }
        }
        NodeKind::TopN { order_by, .. } => usage.add_items(order_by.len()),
        NodeKind::Window(spec) => {
            usage.add_item_counts([
                spec.partition_by.len(),
                spec.order_by.len(),
                spec.expressions.len(),
            ]);
        }
        NodeKind::SetOp { input_mappings, .. } => {
            usage.add_items(input_mappings.len());
            for mapping in input_mappings {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_items(mapping.len());
            }
        }
        NodeKind::Values { rows } => {
            usage.add_items(rows.len());
            for row in rows {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_items(row.len());
            }
        }
        NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        } => {
            usage.add_item_counts([
                rollup_keys.len(),
                grouping_sets.len(),
                grouping_values.len(),
                grouping_outputs.len(),
            ]);
            for set in grouping_sets {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_items(set.len());
            }
            for output in grouping_outputs {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_items(output.arguments.len());
            }
        }
        NodeKind::Unpivot { spec } => {
            usage.add_item_counts([
                spec.passthrough.len(),
                spec.literal_outputs.len(),
                spec.mappings.len(),
            ]);
            for mapping in &spec.mappings {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_items(mapping.constants.len());
                for constant in &mapping.constants {
                    if usage.exhausted() {
                        return Ok(());
                    }
                    add_unpivot_constant_usage(constant, usage);
                }
            }
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            ..
        } => {
            usage.add_item_counts([arguments.len(), outputs.len()]);
            add_table_function_usage_core(
                function,
                &format!("{path}.function"),
                usage,
                errors,
                policy,
            )?;
        }
        NodeKind::AssertOneRow(spec) => match spec {
            crate::RowCountAssertionSpec::Global { subject, .. } => usage.add_bytes(subject.len()),
            crate::RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                labels,
                message,
                applicability,
            } => {
                usage.add_item_counts([
                    keys.len(),
                    labels.len(),
                    usize::from(matches!(
                        applicability,
                        crate::AssertionApplicability::WhenTrue(_)
                    )),
                ]);
                usage.add_bytes(message.len());
                for label in labels {
                    if usage.exhausted() {
                        return Ok(());
                    }
                    usage.add_bytes(label.len());
                }
            }
        },
        NodeKind::ChangeEventExpand { events, .. } => {
            usage.add_items(events.len());
            for event in events {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_items(event.assignments.len());
            }
        }
        NodeKind::ExchangeSource { imports, .. } => usage.add_items(imports.len()),
        NodeKind::TableWriter { target } => {
            usage.add_item_counts([
                target.input.len(),
                target.target_fields.len(),
                target.partial_aggregates.len(),
            ]);
            add_distribution_usage(&target.required_distribution, usage);
            add_encoded_payload_usage(&target.handle, usage);
            for (index, field) in target.target_fields.iter().enumerate() {
                if usage.exhausted() {
                    return Ok(());
                }
                usage.add_bytes(field.provider_name.len());
                if field.provider_name.len() > MAX_DATA_TYPE_FIELD_NAME_BYTES {
                    errors.push(ValidationError::resource_limit(
                        format!("{path}.target_fields[{index}].provider_name"),
                        "writer provider field name exceeds the field-name byte limit",
                    ));
                }
                validate_value_type_core(
                    &field.ty,
                    &format!("{path}.target_fields[{index}].type"),
                    usage,
                    errors,
                    policy,
                )?;
            }
            add_writer_schema_usage_core(
                &target.output_schema,
                &format!("{path}.output_schema"),
                usage,
                errors,
                policy,
            )?;
            for (index, aggregate) in target.partial_aggregates.iter().enumerate() {
                if usage.exhausted() {
                    return Ok(());
                }
                add_aggregate_binding_usage_core(
                    &aggregate.binding,
                    &format!("{path}.partial_aggregates[{index}].binding"),
                    usage,
                    errors,
                    policy,
                )?;
            }
        }
        NodeKind::TableFinish(spec) => {
            add_writer_finish_usage_core(spec, path, usage, errors, policy)?
        }
        NodeKind::Filter { .. } | NodeKind::Limit { .. } | NodeKind::GenerateSeries { .. } => {}
    };
    Ok(())
}

fn add_writer_finish_usage_core<P: TypeValidationPolicy>(
    spec: &WriterFinishSpec,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    usage.add_item_counts([
        spec.expected_target_ordinals.len(),
        spec.final_aggregates.len(),
        if spec.logical_summary.is_some() { 3 } else { 0 },
    ]);
    add_writer_schema_usage_core(
        &spec.input_schema,
        &format!("{path}.input_schema"),
        usage,
        errors,
        policy,
    )?;
    add_writer_schema_usage_core(
        &spec.output_schema,
        &format!("{path}.output_schema"),
        usage,
        errors,
        policy,
    )?;
    for (index, aggregate) in spec.final_aggregates.iter().enumerate() {
        if usage.exhausted() {
            return Ok(());
        }
        add_aggregate_binding_usage_core(
            &aggregate.binding,
            &format!("{path}.final_aggregates[{index}].binding"),
            usage,
            errors,
            policy,
        )?;
    }
    if let Some(grouped) = &spec.grouped_unpivot {
        usage.add_item_counts([
            grouped.statistics_target_ordinals.len(),
            grouped.literal_outputs.len(),
            grouped.mappings.len(),
        ]);
        for mapping in &grouped.mappings {
            if usage.exhausted() {
                return Ok(());
            }
            usage.add_items(mapping.constants.len());
            for constant in &mapping.constants {
                if usage.exhausted() {
                    return Ok(());
                }
                add_unpivot_constant_usage(constant, usage);
            }
        }
    }
    Ok(())
}

fn add_writer_schema_usage_core<P: TypeValidationPolicy>(
    schema: &WriterRelationSchema,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    usage.add_items(schema.fields.len());
    for (index, field) in schema.fields.iter().enumerate() {
        if usage.exhausted() {
            return Ok(());
        }
        usage.add_bytes(field.name.len());
        validate_value_type_core(
            &field.ty,
            &format!("{path}.fields[{index}].type"),
            usage,
            errors,
            policy,
        )?;
    }
    Ok(())
}

fn add_unpivot_constant_usage(_constant: &UnpivotConstant, _usage: &mut ResourceUsage) {
    // All payloads are selected addresses or expression IDs. The sole pool
    // resource author accounts retained checked backing once by identity;
    // observed constant publication accounts repeated consumer work/limits.
}

fn add_relation_usage_core<P: TypeValidationPolicy>(
    relation: &Relation,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    let (schema, guarantees, evidence_bytes, metadata_kind_bytes) = match relation {
        Relation::Data(relation) => (
            relation.schema.as_ref(),
            relation.predicate_guarantees.as_ref(),
            0,
            0,
        ),
        Relation::Metadata(relation) => (
            relation.schema.as_ref(),
            relation.predicate_guarantees.as_ref(),
            relation.coverage_evidence.len(),
            relation.kind.as_str().len(),
        ),
    };
    usage.add_item_counts([schema.len(), guarantees.len()]);
    usage.add_byte_counts([evidence_bytes, metadata_kind_bytes]);
    add_properties_usage(relation.provided_properties(), usage);
    add_read_reference_usage(relation.read(), usage);
    for (index, field) in schema.iter().enumerate() {
        if usage.exhausted() {
            return Ok(());
        }
        add_encoded_payload_usage(&field.column.column_payload, usage);
        validate_value_type_core(
            &field.ty,
            &format!("{path}.schema[{index}].type"),
            usage,
            errors,
            policy,
        )?;
    }
    Ok(())
}

fn add_encoded_payload_usage(payload: &ConnectorEncodedPayload, usage: &mut ResourceUsage) {
    usage.add_byte_counts([
        payload.payload().len(),
        payload.header().provider_id().as_str().len(),
        payload.header().catalog().catalog_name().as_str().len(),
    ]);
}

fn add_read_reference_usage(source: &ProviderReadReference, usage: &mut ResourceUsage) {
    usage.add_byte_counts([
        source.binding.descriptor().provider_id.as_str().len(),
        source.binding.descriptor().instance_id.as_str().len(),
        source
            .binding
            .catalog_handle()
            .catalog_name()
            .as_str()
            .len(),
        source.input_version.as_bytes().len(),
    ]);
    add_encoded_payload_usage(source.relation.table(), usage);
    add_encoded_payload_usage(source.relation.view(), usage);
}

fn add_sink_usage(sink: &FragmentSink, usage: &mut ResourceUsage) {
    match sink {
        FragmentSink::Multicast { edges } => usage.add_items(edges.len()),
        FragmentSink::Router { routes, .. } => {
            usage.add_items(3);
            usage.add_items(routes.len());
            for route in routes {
                if usage.exhausted() {
                    return;
                }
                usage.add_item_counts([
                    route.accepted_effects.len(),
                    route.input_mapping.len(),
                    route.requirements.colocation_keys().len(),
                    route.requirements.layout_keys().len(),
                ]);
            }
        }
        FragmentSink::RootResult(contract) => match contract.output() {
            novarocks_result_contract::FrozenRootOutput::ClientRows(schema) => {
                usage.add_items(schema.columns().len());
                usage.add_bytes(schema.backing_bytes());
            }
            novarocks_result_contract::FrozenRootOutput::ScalarValue(schema) => {
                usage.add_items(schema.type_nodes());
                usage.add_bytes(schema.backing_bytes());
            }
            _ => {}
        },
        FragmentSink::Result | FragmentSink::Stream { .. } | FragmentSink::Noop => {}
    }
}

fn add_runtime_filter_usage_core<P: TypeValidationPolicy>(
    filter: &RuntimeFilter,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    if usage.exhausted() {
        return Ok(());
    }
    match &filter.domain {
        RuntimeFilterDomain::Membership { ty, .. } => {
            validate_value_type_core(ty, &format!("{path}.domain.type"), usage, errors, policy)?;
        }
        RuntimeFilterDomain::Ordered { key, .. } => {
            usage.add_items(1);
            validate_value_type_core(
                &key.ty,
                &format!("{path}.domain.key.type"),
                usage,
                errors,
                policy,
            )?;
        }
    }
    if usage.exhausted() {
        return Ok(());
    }
    add_runtime_filter_coverage_usage(
        &filter.availability_coverage,
        &format!("{path}.availability_coverage"),
        usage,
        errors,
    );
    if usage.exhausted() {
        return Ok(());
    }
    add_runtime_filter_coverage_usage(
        &filter.terminal_coverage,
        &format!("{path}.terminal_coverage"),
        usage,
        errors,
    );
    usage.add_item_counts([
        filter.equality_witnesses.len(),
        filter.producers.len(),
        filter.consumers.len(),
    ]);
    for producer in &filter.producers {
        if usage.exhausted() {
            return Ok(());
        }
        usage.add_item_counts([
            producer.endpoint.values.len(),
            producer.contribution_kinds.len(),
            producer.progress.build_edges.len(),
            producer.progress.non_build_edges.len(),
        ]);
    }
    for consumer in &filter.consumers {
        if usage.exhausted() {
            return Ok(());
        }
        usage.add_item_counts([consumer.endpoint.values.len(), consumer.capabilities.len()]);
        match &consumer.target {
            crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. }
            | crate::RuntimeFilterConsumerTarget::AggregateTopNScanField { lineage, .. } => {
                usage.add_items(lineage.len());
            }
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => {}
        }
    }
    Ok(())
}

fn add_runtime_filter_coverage_usage(
    coverage: &RuntimeFilterCoverage,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    if usage.exhausted() {
        return;
    }
    usage.add_items(coverage.nodes.len());
    if coverage.nodes.len() > errors.limits().runtime_filter_coverage_nodes {
        errors.push(ValidationError::resource_limit(
            path,
            format!(
                "contains more than {} arena nodes",
                errors.limits().runtime_filter_coverage_nodes
            ),
        ));
        return;
    }
    let mut child_references = 0_usize;
    for node in &coverage.nodes {
        if usage.exhausted() {
            return;
        }
        if let RuntimeFilterCoverageNode::AllOf { children }
        | RuntimeFilterCoverageNode::AnyOf { children } = node
        {
            child_references = child_references.saturating_add(children.len());
            if child_references > errors.limits().runtime_filter_coverage_nodes {
                errors.push(ValidationError::resource_limit(
                    path,
                    format!(
                        "contains more than {} child references",
                        errors.limits().runtime_filter_coverage_nodes
                    ),
                ));
                return;
            }
        }
    }
    usage.add_items(child_references);
}

fn add_properties_usage(properties: &crate::PhysicalProperties, usage: &mut ResourceUsage) {
    usage.add_item_counts([
        properties.ordering.len(),
        distribution_items(&properties.distribution),
    ]);
}

fn add_distribution_usage(distribution: &crate::Distribution, usage: &mut ResourceUsage) {
    usage.add_items(distribution_items(distribution));
}

fn distribution_items(distribution: &crate::Distribution) -> usize {
    distribution.value_references().count()
}

// The same type laws have two storage/observation policies. ResourceUsage
// remains the original semantic quota; it is not an owned-resource wallet.
use crate::ValidationErrorCategory;
use novarocks_type_contract::{
    ControlOwnedResourceFacts, ControlResourceCounter, ControlResourceError,
    owned_resources::{copy::reserve_exit, hashmap, vec::reserve_for_push_in},
};

type TypeAdmission<'a> =
    dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError> + 'a;
const TYPE_USIZE_WIDTH: usize = usize::MAX.ilog10() as usize + 1;

// Only these closed reports use this contribution. Rust 1.92 core/fmt/num.rs
// emits four/two/one decimal digits; 16 source operations per digit covers
// its division, pair indexing and writes, and 32 per argument covers the
// buffer/sign/no-width pad_integral and fmt dispatch. The 2*n+1 fragments
// include empty literal writes. This is not arbitrary Display or a CPU grant.
struct TypeMessageBound {
    bytes: usize,
    work: usize,
}
const fn type_message_bound(
    bytes: usize,
    arguments: usize,
    decimal_digits: usize,
) -> TypeMessageBound {
    TypeMessageBound {
        bytes,
        work: bytes + 2 * arguments + 1 + 32 * arguments + 16 * decimal_digits,
    }
}

trait TypeValidationPolicy {
    type Error;
    const CALLER: bool;
    fn initial<'source>(
        &mut self,
        root: &'source DataType,
    ) -> Result<Vec<(&'source DataType, usize)>, Self::Error>;
    fn push<'source>(
        &mut self,
        pending: &mut Vec<(&'source DataType, usize)>,
        entry: (&'source DataType, usize),
    ) -> Result<(), Self::Error>;
    fn children(
        &mut self,
        pending: &mut Vec<(&DataType, usize)>,
        count: usize,
        allowed: bool,
    ) -> Result<(), Self::Error>;
    fn node(&mut self) -> Result<(), Self::Error>;
    fn field_header(&mut self, field: &Field) -> Result<(), Self::Error>;
    fn metadata_entry(&mut self) -> Result<(), Self::Error>;
    fn metadata_end(&mut self) -> Result<(), Self::Error>;
    fn message(
        &mut self,
        category: ValidationErrorCategory,
        path: &str,
        message: &'static str,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error>;
    fn formatted(
        &mut self,
        category: ValidationErrorCategory,
        path: &str,
        args: std::fmt::Arguments<'_>,
        bound: TypeMessageBound,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error>;
    fn logical(
        &mut self,
        ty: &ValueType,
        path: &str,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error>;
}

struct PlainTypeValidation;
impl TypeValidationPolicy for PlainTypeValidation {
    type Error = std::convert::Infallible;
    const CALLER: bool = false;
    fn initial<'source>(
        &mut self,
        root: &'source DataType,
    ) -> Result<Vec<(&'source DataType, usize)>, Self::Error> {
        Ok(vec![(root, 1)])
    }
    fn push<'source>(
        &mut self,
        pending: &mut Vec<(&'source DataType, usize)>,
        entry: (&'source DataType, usize),
    ) -> Result<(), Self::Error> {
        pending.push(entry);
        Ok(())
    }
    fn children(
        &mut self,
        _: &mut Vec<(&DataType, usize)>,
        _: usize,
        _: bool,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn node(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn field_header(&mut self, _: &Field) -> Result<(), Self::Error> {
        Ok(())
    }
    fn metadata_entry(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn metadata_end(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn message(
        &mut self,
        category: ValidationErrorCategory,
        path: &str,
        message: &'static str,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        errors.push(ValidationError::categorized(category, path, message));
        Ok(())
    }
    fn formatted(
        &mut self,
        category: ValidationErrorCategory,
        path: &str,
        args: std::fmt::Arguments<'_>,
        _: TypeMessageBound,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        errors.push(ValidationError::categorized(
            category,
            path,
            args.to_string(),
        ));
        Ok(())
    }
    fn logical(
        &mut self,
        ty: &ValueType,
        path: &str,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        if let Err(error) = ty.validate() {
            errors.push(ValidationError::new(path, error.to_string()));
        }
        Ok(())
    }
}

struct CallerTypeValidation<'a, 'control> {
    source: usize,
    resources: &'a mut ControlResourceCounter,
    admit: &'a mut TypeAdmission<'a>,
    work: &'a mut CompileCheckpoints<'control>,
}
impl CallerTypeValidation<'_, '_> {
    fn gate(&mut self) -> Result<(), ControlResourceError> {
        (self.admit)(&self.resources.facts()).map_err(Into::into)
    }
    fn hash_work(
        &mut self,
        result: Result<usize, hashmap::HashMapResourceError>,
    ) -> Result<(), ControlResourceError> {
        let work = result.map_err(|error| match error {
            hashmap::HashMapResourceError::SourceModel(message) => {
                ControlResourceError::SourceModel(message)
            }
            hashmap::HashMapResourceError::Arithmetic(_) => {
                CompileControlError::ResourceExhausted.into()
            }
        })?;
        self.resources.work(work)?;
        self.gate()
    }
}

impl TypeValidationPolicy for CallerTypeValidation<'_, '_> {
    type Error = ControlResourceError;
    const CALLER: bool = true;
    fn initial<'source>(
        &mut self,
        root: &'source DataType,
    ) -> Result<Vec<(&'source DataType, usize)>, Self::Error> {
        self.resources.buffer::<(&DataType, usize)>(1, 1)?;
        self.resources.work(1)?; // The actual initial borrowed tuple move.
        self.gate()?;
        self.work.flush()?;
        let mut pending = Vec::new();
        let reserved = pending.try_reserve_exact(1);
        if reserved.is_ok() {
            self.work.step()?;
        }
        reserve_exit::<ControlResourceError>(reserved, self.work)?;
        pending.push((root, 1));
        self.work.step()?;
        Ok(pending)
    }
    fn push<'source>(
        &mut self,
        pending: &mut Vec<(&'source DataType, usize)>,
        entry: (&'source DataType, usize),
    ) -> Result<(), Self::Error> {
        reserve_for_push_in::<_, ControlResourceError>(
            pending,
            &mut |facts| {
                if let Some(layout) = facts.requested_backing {
                    self.resources.layout(layout, 1)?;
                }
                (self.admit)(&self.resources.facts()).map_err(Into::into)
            },
            self.work,
        )?;
        pending.push(entry);
        self.work.step()?;
        Ok(())
    }
    fn children(
        &mut self,
        pending: &mut Vec<(&DataType, usize)>,
        count: usize,
        allowed: bool,
    ) -> Result<(), Self::Error> {
        if !allowed {
            return Ok(());
        }
        let needed = novarocks_type_contract::control_resource_add(pending.len(), count)?;
        let grows = needed > pending.capacity();
        if grows {
            let layout = std::alloc::Layout::array::<(&DataType, usize)>(needed)
                .map_err(|_| ControlResourceError::from(CompileControlError::ResourceExhausted))?;
            // Rust 1.92 RawVec::try_reserve_exact requests len+additional.
            // No amortized geometry or source backing is reconstructed here.
            self.resources.layout(layout, 1)?;
        }
        self.resources.work(count)?;
        self.gate()?;
        if grows {
            self.work.flush()?;
            let reserved = pending.try_reserve_exact(count);
            if reserved.is_ok() {
                self.work.step()?;
            }
            reserve_exit::<ControlResourceError>(reserved, self.work)?;
        }
        Ok(())
    }
    fn node(&mut self) -> Result<(), Self::Error> {
        self.resources.work(1)?;
        self.gate()?;
        self.work.step().map_err(Into::into)
    }
    fn field_header(&mut self, field: &Field) -> Result<(), Self::Error> {
        // The real take(257) remains below, including the original over-limit
        // diagnostics. Deleted backing is bounded by the true source invoice.
        self.hash_work(hashmap::source_iterator_work_upper_bound(
            self.source,
            field
                .metadata()
                .len()
                .min(MAX_DATA_TYPE_FIELD_METADATA_ENTRIES + 1),
        ))?;
        self.work.flush().map_err(Into::into)
    }
    fn metadata_entry(&mut self) -> Result<(), Self::Error> {
        self.work.step().map_err(Into::into)
    }
    fn metadata_end(&mut self) -> Result<(), Self::Error> {
        self.work.flush().map_err(Into::into)
    }
    fn message(
        &mut self,
        category: ValidationErrorCategory,
        path: &str,
        message: &'static str,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        self.formatted(
            category,
            path,
            format_args!("{message}"),
            type_message_bound(message.len(), 1, 0),
            errors,
        )
    }
    fn formatted(
        &mut self,
        category: ValidationErrorCategory,
        path: &str,
        args: std::fmt::Arguments<'_>,
        bound: TypeMessageBound,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        self.resources.work(bound.work)?;
        // Combine formatter work with the helper's complete known invoice;
        // no callback or formatter runs before that single parent gate.
        errors.report_formatted_in(
            category,
            path,
            args,
            bound.bytes,
            self.resources,
            self.admit,
            self.work,
        )
    }
    fn logical(
        &mut self,
        ty: &ValueType,
        _path: &str,
        _errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        // The immutable semantic author has already validated shape and
        // domains. Observe its actual borrowed tree; metadata grants no type.
        novarocks_type_contract::visit_value_type_observed(ty.value_type(), |visit| {
            self.resources.work(1)?;
            self.gate()?;
            self.work.step()?;
            if let novarocks_type_contract::ValueSemanticVisit::NamedField(field) = visit {
                for _ in field.name().bytes() {
                    self.resources.work(1)?;
                    self.gate()?;
                    self.work.step()?;
                }
            }
            Ok::<_, ControlResourceError>(())
        })
    }
}

fn validate_value_type(
    ty: &ValueType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    match validate_value_type_core(ty, path, usage, errors, &mut PlainTypeValidation) {
        Ok(()) => {}
        Err(never) => match never {},
    }
}
fn validate_value_type_in(
    ty: &ValueType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut CallerTypeValidation<'_, '_>,
) -> Result<(), ControlResourceError> {
    validate_value_type_core(ty, path, usage, errors, policy)
}
fn validate_value_type_core<P: TypeValidationPolicy>(
    ty: &ValueType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    validate_data_type_core(ty.data_type(), path, usage, errors, policy)?;
    if !usage.exhausted() {
        policy.logical(ty, path, errors)?;
    }
    Ok(())
}
fn validate_data_type_core<P: TypeValidationPolicy>(
    root: &DataType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    if usage.exhausted() {
        return Ok(());
    }
    let mut pending = policy.initial(root)?;
    let mut nodes = 0_usize;
    while let Some((data_type, depth)) = pending.pop() {
        if usage.exhausted() {
            return Ok(());
        }
        nodes = nodes.saturating_add(1);
        usage.add_items(1);
        if depth > MAX_DATA_TYPE_DEPTH {
            policy.formatted(
                ValidationErrorCategory::ResourceLimit,
                path,
                format_args!("Arrow data type depth exceeds {MAX_DATA_TYPE_DEPTH}"),
                type_message_bound(
                    "Arrow data type depth exceeds ".len() + TYPE_USIZE_WIDTH,
                    1,
                    TYPE_USIZE_WIDTH,
                ),
                errors,
            )?;
            return Ok(());
        }
        if nodes > MAX_DATA_TYPE_NODES {
            report_type_nodes(path, errors, policy)?;
            return Ok(());
        }
        match data_type {
            DataType::Timestamp(_, Some(timezone)) => {
                usage.add_bytes(timezone.len());
                if timezone.len() > MAX_TIMESTAMP_TIMEZONE_BYTES {
                    policy.formatted(
                        ValidationErrorCategory::ResourceLimit,
                        path,
                        format_args!(
                            "Arrow timestamp timezone exceeds {MAX_TIMESTAMP_TIMEZONE_BYTES} bytes"
                        ),
                        type_message_bound(
                            "Arrow timestamp timezone exceeds  bytes".len() + TYPE_USIZE_WIDTH,
                            1,
                            TYPE_USIZE_WIDTH,
                        ),
                        errors,
                    )?;
                }
            }
            DataType::FixedSizeBinary(size) if *size < 0 || *size > MAX_FIXED_SIZE_LENGTH => {
                invalid_fixed_size_core(path, *size, errors, policy)?;
            }
            DataType::Time32(unit) if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) => {
                policy.message(
                    ValidationErrorCategory::StructuralInvariant,
                    path,
                    "Arrow Time32 must use second or millisecond units",
                    errors,
                )?;
            }
            DataType::Time64(unit)
                if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) =>
            {
                policy.message(
                    ValidationErrorCategory::StructuralInvariant,
                    path,
                    "Arrow Time64 must use microsecond or nanosecond units",
                    errors,
                )?;
            }
            DataType::FixedSizeList(field, size) => {
                policy.children(&mut pending, 1, true)?;
                if *size < 0 || *size > MAX_FIXED_SIZE_LENGTH {
                    invalid_fixed_size_core(path, *size, errors, policy)?;
                }
                validate_field_core(field, path, usage, errors, policy)?;
                policy.push(&mut pending, (field.data_type(), depth.saturating_add(1)))?;
            }
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => {
                policy.children(&mut pending, 1, true)?;
                validate_field_core(field, path, usage, errors, policy)?;
                policy.push(&mut pending, (field.data_type(), depth.saturating_add(1)))?;
            }
            DataType::Struct(fields) => {
                if data_type_children_exceed_budget_core(
                    nodes,
                    pending.len(),
                    fields.len(),
                    path,
                    errors,
                    policy,
                )? {
                    return Ok(());
                }
                policy.children(&mut pending, fields.len(), true)?;
                usage.add_items(fields.len());
                for field in fields {
                    validate_field_core(field, path, usage, errors, policy)?;
                    policy.push(&mut pending, (field.data_type(), depth.saturating_add(1)))?;
                }
            }
            DataType::Union(fields, _) => {
                if data_type_children_exceed_budget_core(
                    nodes,
                    pending.len(),
                    fields.len(),
                    path,
                    errors,
                    policy,
                )? {
                    return Ok(());
                }
                policy.children(&mut pending, fields.len(), true)?;
                usage.add_items(fields.len());
                for (_, field) in fields.iter() {
                    validate_field_core(field, path, usage, errors, policy)?;
                    policy.push(&mut pending, (field.data_type(), depth.saturating_add(1)))?;
                }
            }
            DataType::Dictionary(key, value) => {
                // Capture the numerical possibility without changing the old
                // key-error-before-quota report order below.
                let allowed = P::CALLER.then(|| data_type_children_fit(nodes, pending.len(), 2));
                if let Some(allowed) = allowed {
                    policy.children(&mut pending, 2, allowed)?;
                }
                if !matches!(
                    key.as_ref(),
                    DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                ) {
                    policy.message(
                        ValidationErrorCategory::StructuralInvariant,
                        path,
                        "Arrow dictionary key must be an integer type",
                        errors,
                    )?;
                }
                if !allowed.unwrap_or_else(|| data_type_children_fit(nodes, pending.len(), 2)) {
                    report_type_nodes(path, errors, policy)?;
                    return Ok(());
                }
                policy.push(&mut pending, (key, depth.saturating_add(1)))?;
                policy.push(&mut pending, (value, depth.saturating_add(1)))?;
            }
            DataType::RunEndEncoded(run_ends, values) => {
                let allowed = P::CALLER.then(|| data_type_children_fit(nodes, pending.len(), 2));
                if let Some(allowed) = allowed {
                    policy.children(&mut pending, 2, allowed)?;
                }
                if !matches!(
                    run_ends.data_type(),
                    DataType::Int16 | DataType::Int32 | DataType::Int64
                ) {
                    policy.message(
                        ValidationErrorCategory::StructuralInvariant,
                        path,
                        "Arrow run-end type must be Int16, Int32 or Int64",
                        errors,
                    )?;
                }
                if !allowed.unwrap_or_else(|| data_type_children_fit(nodes, pending.len(), 2)) {
                    report_type_nodes(path, errors, policy)?;
                    return Ok(());
                }
                validate_field_core(run_ends, path, usage, errors, policy)?;
                validate_field_core(values, path, usage, errors, policy)?;
                policy.push(
                    &mut pending,
                    (run_ends.data_type(), depth.saturating_add(1)),
                )?;
                policy.push(&mut pending, (values.data_type(), depth.saturating_add(1)))?;
            }
            DataType::Decimal32(precision, scale) => validate_decimal_core(
                path,
                *precision,
                *scale,
                DECIMAL32_MAX_PRECISION,
                DECIMAL32_MAX_SCALE,
                errors,
                policy,
            )?,
            DataType::Decimal64(precision, scale) => validate_decimal_core(
                path,
                *precision,
                *scale,
                DECIMAL64_MAX_PRECISION,
                DECIMAL64_MAX_SCALE,
                errors,
                policy,
            )?,
            DataType::Decimal128(precision, scale) => validate_decimal_core(
                path,
                *precision,
                *scale,
                DECIMAL128_MAX_PRECISION,
                DECIMAL128_MAX_SCALE,
                errors,
                policy,
            )?,
            DataType::Decimal256(precision, scale) => validate_decimal_core(
                path,
                *precision,
                *scale,
                DECIMAL256_MAX_PRECISION,
                DECIMAL256_MAX_SCALE,
                errors,
                policy,
            )?,
            _ => {}
        }
        policy.node()?;
    }
    Ok(())
}
fn report_type_nodes<P: TypeValidationPolicy>(
    path: &str,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    policy.formatted(
        ValidationErrorCategory::ResourceLimit,
        path,
        format_args!("Arrow data type contains more than {MAX_DATA_TYPE_NODES} nodes"),
        type_message_bound(
            "Arrow data type contains more than  nodes".len() + TYPE_USIZE_WIDTH,
            1,
            TYPE_USIZE_WIDTH,
        ),
        errors,
    )
}
fn data_type_children_fit(visited: usize, pending: usize, children: usize) -> bool {
    visited.saturating_add(pending).saturating_add(children) <= MAX_DATA_TYPE_NODES
}
fn data_type_children_exceed_budget_core<P: TypeValidationPolicy>(
    visited: usize,
    pending: usize,
    children: usize,
    path: &str,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<bool, P::Error> {
    if data_type_children_fit(visited, pending, children) {
        return Ok(false);
    }
    report_type_nodes(path, errors, policy)?;
    Ok(true)
}
fn validate_field_core<P: TypeValidationPolicy>(
    field: &Field,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    policy.field_header(field)?;
    usage.add_bytes(field.name().len());
    if field.name().len() > MAX_DATA_TYPE_FIELD_NAME_BYTES {
        policy.formatted(
            ValidationErrorCategory::ResourceLimit,
            path,
            format_args!("Arrow field name exceeds {MAX_DATA_TYPE_FIELD_NAME_BYTES} bytes"),
            type_message_bound(
                "Arrow field name exceeds  bytes".len() + TYPE_USIZE_WIDTH,
                1,
                TYPE_USIZE_WIDTH,
            ),
            errors,
        )?;
    }
    if field.metadata().len() > MAX_DATA_TYPE_FIELD_METADATA_ENTRIES {
        policy.formatted(ValidationErrorCategory::ResourceLimit, path,
            format_args!("Arrow field metadata contains more than {MAX_DATA_TYPE_FIELD_METADATA_ENTRIES} entries"),
            type_message_bound("Arrow field metadata contains more than  entries".len() + TYPE_USIZE_WIDTH, 1, TYPE_USIZE_WIDTH), errors)?;
    }
    usage.add_items(field.metadata().len());
    let mut metadata_bytes = 0_usize;
    for (key, value) in field
        .metadata()
        .iter()
        .take(MAX_DATA_TYPE_FIELD_METADATA_ENTRIES.saturating_add(1))
    {
        metadata_bytes = metadata_bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        if key.len() > MAX_DATA_TYPE_FIELD_METADATA_KEY_BYTES
            || value.len() > MAX_DATA_TYPE_FIELD_METADATA_VALUE_BYTES
        {
            policy.message(
                ValidationErrorCategory::ResourceLimit,
                path,
                "Arrow field metadata key or value exceeds its byte limit",
                errors,
            )?;
        }
        policy.metadata_entry()?;
    }
    policy.metadata_end()?;
    usage.add_bytes(metadata_bytes);
    if metadata_bytes > MAX_DATA_TYPE_FIELD_METADATA_BYTES {
        policy.formatted(
            ValidationErrorCategory::ResourceLimit,
            path,
            format_args!("Arrow field metadata exceeds {MAX_DATA_TYPE_FIELD_METADATA_BYTES} bytes"),
            type_message_bound(
                "Arrow field metadata exceeds  bytes".len() + TYPE_USIZE_WIDTH,
                1,
                TYPE_USIZE_WIDTH,
            ),
            errors,
        )?;
    }
    Ok(())
}
fn validate_decimal_core<P: TypeValidationPolicy>(
    path: &str,
    precision: u8,
    scale: i8,
    max_precision: u8,
    max_scale: i8,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    if precision == 0 || precision > max_precision || scale < -max_scale || scale > max_scale {
        policy.formatted(ValidationErrorCategory::StructuralInvariant, path,
            format_args!("Arrow decimal precision/scale ({precision}, {scale}) is outside 1..={max_precision} and -{max_scale}..={max_scale}"),
            // Two u8 and three i8 formatted occurrences, including signs.
            type_message_bound("Arrow decimal precision/scale (, ) is outside 1..= and -..=".len() + 2 * 3 + 3 * 4, 5, 2 * 3 + 3 * 4), errors)?;
    }
    Ok(())
}
fn invalid_fixed_size_core<P: TypeValidationPolicy>(
    path: &str,
    size: i32,
    errors: &mut ValidationContext,
    policy: &mut P,
) -> Result<(), P::Error> {
    // Preserve the original negative-vs-upper-limit error categories.
    if size < 0 {
        return policy.formatted(
            ValidationErrorCategory::StructuralInvariant,
            path,
            format_args!("Arrow fixed-size length {size} is negative"),
            type_message_bound("Arrow fixed-size length  is negative".len() + 11, 1, 11),
            errors,
        );
    }
    policy.formatted(
        ValidationErrorCategory::ResourceLimit,
        path,
        format_args!("Arrow fixed-size length {size} exceeds {MAX_FIXED_SIZE_LENGTH}"),
        type_message_bound(
            "Arrow fixed-size length  exceeds ".len() + 2 * 11,
            2,
            2 * 11,
        ),
        errors,
    )
}

#[cfg(test)]
#[path = "resource/usage_type_owned_tests.rs"]
mod usage_type_owned_tests;

#[cfg(test)]
#[path = "resource/type_owned_tests.rs"]
mod type_owned_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_and_aggregate_identity_bytes_are_accounted() {
        let function = BoundFunction {
            legacy_metadata: Some(crate::LegacyBindingMetadata {
                semantic_parameters: Box::default(),
                volatility: novarocks_type_contract::FunctionVolatility::Immutable,
                argument_evaluation: novarocks_type_contract::FunctionArgumentEvaluation::Eager,
                failure_behavior: novarocks_type_contract::FunctionFailureBehavior::Propagate,
                intrinsic_row_error:
                    novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
            }),
            function_id: novarocks_type_contract::FunctionId::try_new("f".repeat(1024)).unwrap(),
            overload: novarocks_type_contract::FunctionOverloadId::try_new("o".repeat(1024))
                .unwrap(),
            kind: crate::FunctionKind::Aggregate,
            argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::default()),
            result_type: ValueType::new(DataType::Int64, false),
        };
        let binding = AggregateBinding {
            state_interpretation: None,
            state_argument_contract:
                novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
            function,
            phase: crate::AggregatePhase::Single,
            logical_argument_count: 0,
            intermediate_type: ValueType::new(DataType::Int64, false),
            state_format: novarocks_type_contract::AggregateStateFormatId::try_new(
                "s".repeat(1024),
            )
            .unwrap(),
        };
        let mut usage =
            ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
        let mut errors = ValidationContext::new();

        add_aggregate_binding_usage(&binding, "binding", &mut usage, &mut errors);

        assert!(errors.is_empty());
        assert_eq!(usage.bytes, 3 * 1024);
    }

    #[test]
    fn cumulative_resource_limits_fail_closed_without_large_allocations() {
        let mut errors = ValidationContext::new();
        validate_usage(
            "fragment.resources",
            ResourceUsage {
                items: MAX_FRAGMENT_DYNAMIC_ITEMS + 1,
                bytes: MAX_FRAGMENT_DYNAMIC_BYTES + 1,
                max_items: MAX_FRAGMENT_DYNAMIC_ITEMS,
                max_bytes: MAX_FRAGMENT_DYNAMIC_BYTES,
            },
            MAX_FRAGMENT_DYNAMIC_ITEMS,
            MAX_FRAGMENT_DYNAMIC_BYTES,
            &mut errors,
        );
        assert_eq!(errors.len(), 2);
        assert!(errors[0].message().contains("dynamic items"));
        assert!(errors[1].message().contains("dynamic bytes"));
    }
}

#[cfg(test)]
mod constant_resource_tests {
    use super::*;
    use arrow_array::{Array, Int64Array};
    use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
    use novarocks_type_contract::{CompilePhase, FunctionValueType, PureCompileControl};
    use std::sync::{Arc, Mutex};

    struct Control {
        refusal: Option<(usize, CompileControlError)>,
        calls: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl Control {
        fn good() -> Self {
            Self {
                refusal: None,
                calls: Mutex::new(Vec::new()),
            }
        }
        fn trace(&self) -> Vec<(CompilePhase, u32)> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            let position = calls.len();
            calls.push((phase, units));
            match self.refusal.clone() {
                Some((at, cause)) if at == position => Err(cause),
                _ => Ok(()),
            }
        }
    }
    fn pool() -> ConstantPool {
        let array = Int64Array::from(vec![11, 42, 71]);
        let field = Field::new("actual_source", DataType::Int64, false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider.id".to_owned(),
                "source-42".to_owned(),
            )]),
        );
        let policy = ConstantPolicy {
            max_rows: 1024,
            max_array_nodes: 4096,
            max_logical_elements: 4096,
            max_retained_buffer_bytes: 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 16,
            max_metadata_bytes: 1024 * 1024,
            max_library_validation_work: 1024 * 1024,
            max_library_validation_bytes: 1024 * 1024,
        };
        ConstantPool::try_new(
            Arc::new(field),
            FunctionValueType::new(DataType::Int64, false),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
    }
    fn measured(
        pools: &crate::ConstantPools,
        control: &dyn PureCompileControl,
    ) -> Result<CutResourceUsage, CompileControlError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let mut usage = CutResourcePreflight::new();
        usage.add_constants_observed(pools, &mut work)?;
        work.finish()?;
        Ok(CutResourceUsage {
            items: usage.usage.items,
            bytes: usage.usage.bytes,
        })
    }

    #[test]
    fn constant_backing_aliases_charge_sparse_entries_and_one_actual_owner() {
        let original = pool();
        let facts = original.resource_facts();
        let mut aliases = crate::ConstantPools::empty();
        aliases
            .insert(crate::ConstantPoolId::new(0), original.clone())
            .unwrap();
        aliases
            .insert(crate::ConstantPoolId::new(u32::MAX), original.clone())
            .unwrap();
        let usage = measured(&aliases, &Control::good()).unwrap();
        let owner_items = facts.rows + facts.array_nodes + facts.buffer_count;
        let owner_bytes = facts.retained_buffer_capacity_bytes + facts.metadata_bytes;
        assert_eq!(usage.items, 2 + usize::try_from(owner_items).unwrap());
        assert_eq!(usage.bytes, usize::try_from(owner_bytes).unwrap());
        let separate = pool();
        assert_ne!(original.backing_identity(), separate.backing_identity());
        assert!(
            original
                .value(1)
                .unwrap()
                .equals_observed(
                    &separate.value(1).unwrap(),
                    CompilePhase::Validate,
                    &Control::good()
                )
                .unwrap()
        );
        aliases
            .insert(crate::ConstantPoolId::new(7), separate)
            .unwrap();
        let usage = measured(&aliases, &Control::good()).unwrap();
        assert_eq!(usage.items, 3 + 2 * usize::try_from(owner_items).unwrap());
        assert_eq!(usage.bytes, 2 * usize::try_from(owner_bytes).unwrap());
    }

    #[test]
    fn constant_resources_share_existing_fragment_items_and_bytes_exact_boundary() {
        let p = pool();
        let mut pools = crate::ConstantPools::empty();
        pools.insert(crate::ConstantPoolId::new(0), p).unwrap();
        let facts = measured(&pools, &Control::good()).unwrap();
        // Each axis reaches/refuses its own envelope. The first refusal is
        // allowed to stop accounting the other axis before visiting backings.
        for item_axis in [true, false] {
            for extra in [0, 1] {
                let control = Control::good();
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
                let mut usage = CutResourcePreflight::new();
                if item_axis {
                    usage.add_items(MAX_FRAGMENT_DYNAMIC_ITEMS - facts.items + extra);
                } else {
                    usage.add_bytes(MAX_FRAGMENT_DYNAMIC_BYTES - facts.bytes + extra);
                }
                usage.add_constants_observed(&pools, &mut work).unwrap();
                let mut errors = ValidationContext::new();
                let counted = usage.validate("package.resources", &mut errors);
                work.finish().unwrap();
                if item_axis {
                    assert_eq!(counted.items, MAX_FRAGMENT_DYNAMIC_ITEMS + extra);
                } else {
                    assert_eq!(counted.bytes, MAX_FRAGMENT_DYNAMIC_BYTES + extra);
                }
                assert_eq!(errors.len(), extra);
                if extra == 1 {
                    assert!(errors[0].message().contains(if item_axis {
                        "dynamic items"
                    } else {
                        "dynamic bytes"
                    }));
                }
            }
        }
    }

    #[test]
    fn constant_table_count_refuses_before_backing_deduplication_scratch() {
        let original = pool();
        let mut pools = crate::ConstantPools::empty();
        pools
            .insert(crate::ConstantPoolId::new(0), original.clone())
            .unwrap();
        pools
            .insert(crate::ConstantPoolId::new(u32::MAX), original)
            .unwrap();
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let mut usage = ResourceUsage::limited(MAX_PLAN_DYNAMIC_ITEMS, MAX_PLAN_DYNAMIC_BYTES);
        usage.add_items(MAX_PLAN_DYNAMIC_ITEMS - 1);
        add_constant_pool_usage(&pools, &mut usage, &mut work).unwrap();
        work.finish().unwrap();
        assert_eq!(usage.items, MAX_PLAN_DYNAMIC_ITEMS + 1);
        assert_eq!(usage.bytes, 0);
        // Only the completed O(1) table-length addition was performed. No
        // backing deduplication visit (and therefore no tree insertion) ran.
        assert_eq!(
            control.trace(),
            vec![(CompilePhase::Validate, 0), (CompilePhase::Validate, 1)]
        );
    }

    #[test]
    fn constant_resource_deduplication_preserves_original_quantum_and_three_causes() {
        let original = pool();
        let mut pools = crate::ConstantPools::empty();
        for id in 0..320 {
            pools
                .insert(crate::ConstantPoolId::new(id), original.clone())
                .unwrap();
        }
        let baseline = Control::good();
        let usage = measured(&pools, &baseline).unwrap();
        let facts = original.resource_facts();
        assert_eq!(
            usage.items,
            320 + usize::try_from(facts.rows + facts.array_nodes + facts.buffer_count).unwrap()
        );
        let trace = baseline.trace();
        assert!(trace.iter().any(|(_, units)| *units == 256));
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    calls: Mutex::new(Vec::new()),
                };
                assert!(matches!(measured(&pools, &control), Err(actual) if actual == cause));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

// Source and immutable request storage are counted separately. These public
// structural ceilings are not allocator/MEM grants or a BTree node peak model.
fn validate_call_request_type_with<E>(
    ty: &ValueType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    observe: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<(), E> {
    if usage.exhausted() {
        return Ok(());
    }
    // This is the original bounded type author, including its own scratch.
    // Bracketing it does not claim cooperation inside that opaque delegate.
    observe(true)?;
    validate_value_type(ty, path, usage, errors);
    if usage.exhausted() {
        return Ok(());
    }
    observe(false)?;
    observe(true)
}

fn add_call_request_usage<E>(
    request: &crate::PhysicalCallRequest,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    observe: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<(), E> {
    use novarocks_function_contract::FunctionArgument;
    usage.add_items(request.arguments.len());
    usage.add_bytes(request.arguments.len().saturating_mul(std::mem::size_of::<
        FunctionArgument<crate::ConstantReference>,
    >()));
    for argument in &request.arguments {
        if usage.exhausted() {
            return Ok(());
        }
        observe(false)?;
        match argument {
            FunctionArgument::Value { value_type, .. } => {
                validate_call_request_type_with(
                    value_type,
                    "requests.argument.type",
                    usage,
                    errors,
                    observe,
                )?;
            }
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                usage.add_items(parameter_types.len());
                usage.add_bytes(
                    parameter_types
                        .len()
                        .saturating_mul(std::mem::size_of::<ValueType>()),
                );
                for ty in parameter_types {
                    if usage.exhausted() {
                        return Ok(());
                    }
                    observe(false)?;
                    validate_call_request_type_with(
                        ty,
                        "requests.lambda.parameter",
                        usage,
                        errors,
                        observe,
                    )?;
                }
                validate_call_request_type_with(
                    result_type,
                    "requests.lambda.result",
                    usage,
                    errors,
                    observe,
                )?;
            }
        }
    }
    if let Some(ty) = &request.expected_result_type {
        validate_call_request_type_with(ty, "requests.constraint.type", usage, errors, observe)?;
    }
    Ok(())
}

fn add_call_request_table_usage(
    requests: &crate::FragmentCallRequests,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    let result = add_call_request_table_usage_with(requests, usage, errors, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    });
    match result {
        Ok(()) => {}
        Err(never) => match never {},
    }
}

fn add_call_request_table_usage_with<E>(
    requests: &crate::FragmentCallRequests,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
    observe: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<(), E> {
    usage.add_items(requests.entries().len());
    usage.add_bytes(
        requests
            .entries()
            .len()
            .saturating_mul(std::mem::size_of::<(
                crate::PhysicalCallDefinition,
                crate::PhysicalCallRequest,
            )>()),
    );
    for request in requests.entries().values() {
        if usage.exhausted() {
            return Ok(());
        }
        observe(false)?;
        add_call_request_usage(request, usage, errors, observe)?;
    }
    Ok(())
}
pub(crate) fn validate_call_request_source_observed(
    requests: &[(crate::PhysicalCallDefinition, crate::PhysicalCallRequest)],
    capacity: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::CallRequestError> {
    let mut errors = ValidationContext::new();
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    usage.add_items(requests.len());
    // The caller's original Vec capacity coexists with the destination table.
    usage.add_bytes(
        capacity
            .saturating_add(requests.len())
            .saturating_mul(std::mem::size_of::<(
                crate::PhysicalCallDefinition,
                crate::PhysicalCallRequest,
            )>()),
    );
    for (_, request) in requests {
        if usage.exhausted() {
            break;
        }
        work.step()?;
        add_call_request_usage(request, &mut usage, &mut errors, &mut |opaque| {
            if opaque { work.flush() } else { work.step() }
        })?;
    }
    if usage.exhausted() {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    if !errors.is_empty() {
        return Err(crate::CallRequestError::Structure(
            crate::ValidationErrors::from_collector(errors),
        ));
    }
    Ok(())
}
pub(crate) fn validate_call_request_table_observed(
    requests: &crate::FragmentCallRequests,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), crate::CallRequestError> {
    let mut errors = ValidationContext::new();
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    add_call_request_table_usage_with(requests, &mut usage, &mut errors, &mut |opaque| {
        if opaque { work.flush() } else { work.step() }
    })?;
    if usage.exhausted() {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    if !errors.is_empty() {
        return Err(crate::CallRequestError::Structure(
            crate::ValidationErrors::from_collector(errors),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "resource/call_request_tests.rs"]
mod call_request_tests;
