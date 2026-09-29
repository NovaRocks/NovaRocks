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

use crate::validation::ValidationContext;
use crate::{
    AggregateBinding, ArtifactInputRequirement, ArtifactSourceBinding, BoundFunction,
    BoundTableFunction, CoverageSet, ExprKind, Fragment, FragmentCuts, FragmentSink,
    FunctionArgumentType, NodeKind, PhysicalPlan, ProviderReadReference, Relation, RuntimeFilter,
    RuntimeFilterCoverage, RuntimeFilterCoverageNode, RuntimeFilterDomain, SealedArtifactRef,
    SealedArtifactSinkSpec, SortMode, UnpivotConstant, ValidationError, ValueType,
    WriterFinishSpec, WriterRelationSchema,
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
pub const MAX_DATA_TYPE_DEPTH: usize = 64;
pub const MAX_DATA_TYPE_NODES: usize = 4_096;
pub const MAX_DATA_TYPE_FIELD_NAME_BYTES: usize = 1_024;
pub const MAX_DATA_TYPE_FIELD_METADATA_ENTRIES: usize = 256;
pub const MAX_DATA_TYPE_FIELD_METADATA_KEY_BYTES: usize = 1_024;
pub const MAX_DATA_TYPE_FIELD_METADATA_VALUE_BYTES: usize = 16 * 1024;
pub const MAX_DATA_TYPE_FIELD_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_TIMESTAMP_TIMEZONE_BYTES: usize = 1_024;
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

    pub(crate) fn add_items(&mut self, count: usize) {
        self.usage.add_items(count);
    }

    pub(crate) fn add_bytes(&mut self, count: usize) {
        self.usage.add_bytes(count);
    }

    pub(crate) fn add_distribution(&mut self, distribution: &crate::Distribution) {
        add_distribution_usage(distribution, &mut self.usage);
    }

    pub(crate) fn add_source(&mut self, source: &ArtifactSourceBinding, path: &str) {
        add_artifact_source_usage(source, path, &mut self.usage);
    }

    pub(crate) fn add_artifact(
        &mut self,
        artifact: &SealedArtifactRef,
        path: &str,
        errors: &mut ValidationContext,
    ) {
        add_artifact_ref_usage(artifact, path, &mut self.usage, errors);
    }

    pub(crate) fn add_filter(
        &mut self,
        filter: &RuntimeFilter,
        path: &str,
        errors: &mut ValidationContext,
    ) {
        add_runtime_filter_usage(filter, path, &mut self.usage, errors);
    }

    pub(crate) fn add_fragment(&mut self, fragment: &Fragment, errors: &mut ValidationContext) {
        self.usage.merge(fragment_usage(fragment, errors));
    }

    pub(crate) fn add_edge(&mut self, edge: &crate::Edge) {
        self.usage.add_item_counts([
            edge.source.projection.len(),
            edge.destination.receive_mapping.len(),
            distribution_items(&edge.partitioning.source),
            distribution_items(&edge.partitioning.destination),
        ]);
    }

    pub(crate) fn add_value_type(
        &mut self,
        ty: &ValueType,
        path: &str,
        errors: &mut ValidationContext,
    ) {
        validate_value_type(ty, path, &mut self.usage, errors);
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
    let prefix = format!("fragments[{}].cuts", fragment.id().get());
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    usage.add_item_counts([
        cuts.inbound.len(),
        cuts.outbound.len(),
        cuts.artifact_refs.len(),
    ]);
    for (index, cut) in cuts.inbound.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        usage.add_item_counts([cut.imports.len(), cut.source_bindings.len()]);
        if let Some(writer) = &cut.change_stream_writer {
            usage.add_items(writer.fields.len());
        }
        if let Some(writer) = &cut.writer_result {
            add_writer_result_cut_usage(
                writer,
                &format!("{prefix}.inbound[{index}].writer_result"),
                &mut usage,
                errors,
            );
        }
        add_distribution_usage(&cut.partitioning.source, &mut usage);
        add_distribution_usage(&cut.partitioning.destination, &mut usage);
        for (binding, source) in cut.source_bindings.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            add_artifact_source_usage(
                source,
                &format!("{prefix}.inbound[{index}].source_bindings[{binding}]"),
                &mut usage,
            );
        }
        for (ordinal, import) in cut.imports.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            validate_value_type(
                &import.source.ty,
                &format!("{prefix}.inbound[{index}].imports[{ordinal}].type"),
                &mut usage,
                errors,
            );
        }
    }
    for (index, cut) in cuts.outbound.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        usage.add_item_counts([
            cut.projection.len(),
            cut.destination_imports.len(),
            cut.source_bindings.len(),
        ]);
        if let Some(writer) = &cut.change_stream_writer {
            usage.add_items(writer.fields.len());
        }
        if let Some(writer) = &cut.writer_result {
            add_writer_result_cut_usage(
                writer,
                &format!("{prefix}.outbound[{index}].writer_result"),
                &mut usage,
                errors,
            );
        }
        add_distribution_usage(&cut.partitioning.source, &mut usage);
        add_distribution_usage(&cut.partitioning.destination, &mut usage);
        for (binding, source) in cut.source_bindings.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            add_artifact_source_usage(
                source,
                &format!("{prefix}.outbound[{index}].source_bindings[{binding}]"),
                &mut usage,
            );
        }
        for (ordinal, value) in cut.projection.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            validate_value_type(
                &value.ty,
                &format!("{prefix}.outbound[{index}].projection[{ordinal}].type"),
                &mut usage,
                errors,
            );
        }
        for (ordinal, import) in cut.destination_imports.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            validate_value_type(
                &import.source.ty,
                &format!("{prefix}.outbound[{index}].destination_imports[{ordinal}].type"),
                &mut usage,
                errors,
            );
        }
    }
    for (index, artifact) in cuts.artifact_refs.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        add_artifact_ref_usage(
            artifact,
            &format!("{prefix}.artifact_refs[{index}]"),
            &mut usage,
            errors,
        );
    }
    usage.add_items(cuts.runtime_filters.len());
    for (index, filter) in cuts.runtime_filters.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        add_runtime_filter_usage(
            filter,
            &format!("{prefix}.runtime_filters[{index}]"),
            &mut usage,
            errors,
        );
    }
    usage.add_item_counts([
        cuts.runtime_filter_proof.fragments.len(),
        cuts.runtime_filter_proof.edges.len(),
        cuts.runtime_filter_proof.filters.len(),
    ]);
    for fragment in &cuts.runtime_filter_proof.fragments {
        if usage.exhausted() {
            break;
        }
        usage.merge(fragment_usage(fragment, errors));
    }
    for edge in &cuts.runtime_filter_proof.edges {
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
    for (index, filter) in cuts.runtime_filter_proof.filters.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        add_runtime_filter_usage(
            filter,
            &format!("{prefix}.runtime_filter_proof.filters[{index}]"),
            &mut usage,
            errors,
        );
    }
    validate_usage(
        &format!("{prefix}.resources"),
        usage,
        MAX_FRAGMENT_DYNAMIC_ITEMS,
        MAX_FRAGMENT_DYNAMIC_BYTES,
        errors,
    );
}

fn add_writer_result_cut_usage(
    writer: &crate::WriterResultCut,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_items(writer.fields.len());
    for (ordinal, field) in writer.fields.iter().enumerate() {
        if usage.exhausted() {
            break;
        }
        usage.add_bytes(field.name.len());
        validate_value_type(
            &field.ty,
            &format!("{path}.fields[{ordinal}].type"),
            usage,
            errors,
        );
    }
}

pub(crate) fn validate_plan_resources(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut usage = ResourceUsage::limited(MAX_PLAN_DYNAMIC_ITEMS, MAX_PLAN_DYNAMIC_BYTES);
    usage.add_item_counts([
        plan.fragments().len(),
        plan.edges().len(),
        plan.runtime_filters().len(),
        plan.artifact_refs().len(),
        plan.annotations().len(),
    ]);
    for fragment in plan.fragments().values() {
        if usage.exhausted() {
            break;
        }
        usage.merge(fragment_usage(fragment, errors));
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
        usage.add_item_counts([result.output.columns.len(), result.fields.len()]);
        for (index, field) in result.fields.iter().enumerate() {
            if usage.exhausted() {
                break;
            }
            usage.add_byte_counts([field.name.len(), field.alias.as_deref().map_or(0, str::len)]);
            validate_value_type(
                &field.ty,
                &format!("result.fields[{index}].type"),
                &mut usage,
                errors,
            );
        }
    }
    for (id, artifact) in plan.artifact_refs() {
        if usage.exhausted() {
            break;
        }
        add_artifact_ref_usage(
            artifact,
            &format!("artifact_refs[{}]", id.get()),
            &mut usage,
            errors,
        );
    }
    for (id, filter) in plan.runtime_filters() {
        if usage.exhausted() {
            break;
        }
        add_runtime_filter_usage(
            filter,
            &format!("runtime_filters[{}]", id.get()),
            &mut usage,
            errors,
        );
    }
    for annotation in plan.annotations() {
        if usage.exhausted() {
            break;
        }
        usage.add_byte_counts([annotation.key.len(), annotation.value.len()]);
    }
    validate_usage(
        "resources",
        usage,
        MAX_PLAN_DYNAMIC_ITEMS,
        MAX_PLAN_DYNAMIC_BYTES,
        errors,
    );
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

fn fragment_usage(fragment: &Fragment, errors: &mut ValidationContext) -> ResourceUsage {
    let prefix = format!("fragments[{}]", fragment.id().get());
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    usage.add_item_counts([
        fragment.values().len(),
        fragment.expressions().len(),
        fragment.nodes().len(),
        fragment.runtime_filters().len(),
    ]);
    for (id, value) in fragment.values() {
        if usage.exhausted() {
            return usage;
        }
        if let crate::ValueOrigin::ProviderField { field, .. } = &value.origin {
            add_encoded_payload_usage(&field.column_payload, &mut usage);
        }
        validate_value_type(
            &value.ty,
            &format!("{prefix}.values[{}].type", id.get()),
            &mut usage,
            errors,
        );
    }
    for (id, expression) in fragment.expressions().iter() {
        if usage.exhausted() {
            return usage;
        }
        validate_value_type(
            &expression.ty,
            &format!("{prefix}.expressions[{}].type", id.get()),
            &mut usage,
            errors,
        );
        add_expression_usage(
            &expression.kind,
            &format!("{prefix}.expressions[{}]", id.get()),
            &mut usage,
            errors,
        );
    }
    for (id, node) in fragment.nodes() {
        if usage.exhausted() {
            return usage;
        }
        usage.add_item_counts([
            node.inputs.len(),
            node.required_inputs.len(),
            node.output.columns.len(),
        ]);
        add_properties_usage(&node.output_properties, &mut usage);
        for properties in &node.required_inputs {
            if usage.exhausted() {
                return usage;
            }
            add_properties_usage(properties, &mut usage);
        }
        add_node_usage(
            &node.kind,
            &format!("{prefix}.nodes[{}]", id.get()),
            &mut usage,
            errors,
        );
    }
    add_sink_usage(
        fragment.sink(),
        &format!("{prefix}.sink"),
        &mut usage,
        errors,
    );
    usage
}

fn add_expression_usage(
    kind: &ExprKind,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    if usage.exhausted() {
        return;
    }
    match kind {
        ExprKind::Literal(crate::LiteralValue::Utf8(value)) => usage.add_bytes(value.len()),
        ExprKind::Literal(crate::LiteralValue::Binary(value)) => usage.add_bytes(value.len()),
        ExprKind::Literal(crate::LiteralValue::LargeInt(_)) => {
            usage.add_bytes(std::mem::size_of::<i128>())
        }
        ExprKind::FunctionCall { function, args } => {
            usage.add_items(args.len());
            add_function_usage(function, &format!("{path}.function"), usage, errors);
        }
        ExprKind::Lambda {
            parameter_types, ..
        } => {
            usage.add_items(parameter_types.len());
            for (index, ty) in parameter_types.iter().enumerate() {
                if usage.exhausted() {
                    return;
                }
                validate_value_type(
                    ty,
                    &format!("{path}.parameter_types[{index}]"),
                    usage,
                    errors,
                );
            }
        }
        ExprKind::Cast { target, .. } => {
            validate_data_type(target, &format!("{path}.target"), usage, errors);
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
            add_function_usage(function, &format!("{path}.function"), usage, errors);
            if let Some(binding) = aggregate_binding {
                add_aggregate_binding_usage(
                    binding,
                    &format!("{path}.aggregate_binding"),
                    usage,
                    errors,
                );
            }
        }
        _ => {}
    }
}

fn add_function_usage(
    function: &BoundFunction,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_items(function.argument_types.len());
    usage.add_byte_counts([
        function.function_id.as_str().len(),
        function.overload.as_str().len(),
    ]);
    add_argument_types_usage(&function.argument_types, path, usage, errors);
    validate_value_type(
        &function.result_type,
        &format!("{path}.result_type"),
        usage,
        errors,
    );
}

fn add_table_function_usage(
    function: &BoundTableFunction,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_item_counts([function.argument_types.len(), function.result_types.len()]);
    usage.add_byte_counts([
        function.function_id.as_str().len(),
        function.overload.as_str().len(),
    ]);
    add_argument_types_usage(&function.argument_types, path, usage, errors);
    for (index, ty) in function.result_types.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        validate_value_type(ty, &format!("{path}.result_types[{index}]"), usage, errors);
    }
}

fn add_argument_types_usage(
    argument_types: &[FunctionArgumentType],
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    for (index, argument) in argument_types.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        match argument {
            FunctionArgumentType::Value(ty) => validate_value_type(
                ty,
                &format!("{path}.argument_types[{index}]"),
                usage,
                errors,
            ),
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                usage.add_items(parameter_types.len());
                for (parameter, ty) in parameter_types.iter().enumerate() {
                    if usage.exhausted() {
                        return;
                    }
                    validate_value_type(
                        ty,
                        &format!("{path}.argument_types[{index}].parameter_types[{parameter}]"),
                        usage,
                        errors,
                    );
                }
                validate_value_type(
                    result_type,
                    &format!("{path}.argument_types[{index}].result_type"),
                    usage,
                    errors,
                );
            }
        }
    }
}

fn add_aggregate_binding_usage(
    binding: &AggregateBinding,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_bytes(binding.state_format.as_str().len());
    add_function_usage(
        &binding.function,
        &format!("{path}.function"),
        usage,
        errors,
    );
    validate_value_type(
        &binding.intermediate_type,
        &format!("{path}.intermediate_type"),
        usage,
        errors,
    );
}

fn add_node_usage(
    kind: &NodeKind,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    if usage.exhausted() {
        return;
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
                    return;
                }
                add_encoded_payload_usage(&column.column_payload, usage);
            }
            add_relation_usage(relation, &format!("{path}.relation"), usage, errors);
        }
        NodeKind::Project { expressions } => usage.add_items(expressions.len()),
        NodeKind::Aggregate {
            group_by, calls, ..
        } => {
            usage.add_item_counts([group_by.len(), calls.len()]);
            for (index, call) in calls.iter().enumerate() {
                if usage.exhausted() {
                    return;
                }
                usage.add_item_counts([call.arguments.len(), call.order_by.len()]);
                add_aggregate_binding_usage(
                    &call.binding,
                    &format!("{path}.calls[{index}].binding"),
                    usage,
                    errors,
                );
            }
        }
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
                    return;
                }
                usage.add_items(mapping.len());
            }
        }
        NodeKind::Values { rows } => {
            usage.add_items(rows.len());
            for row in rows {
                if usage.exhausted() {
                    return;
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
                    return;
                }
                usage.add_items(set.len());
            }
            for output in grouping_outputs {
                if usage.exhausted() {
                    return;
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
                    return;
                }
                usage.add_items(mapping.constants.len());
                for constant in &mapping.constants {
                    if usage.exhausted() {
                        return;
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
            add_table_function_usage(function, &format!("{path}.function"), usage, errors);
        }
        NodeKind::AssertOneRow(spec) => match spec {
            crate::RowCountAssertionSpec::Global { subject, .. } => usage.add_bytes(subject.len()),
            crate::RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                labels,
                message,
            } => {
                usage.add_item_counts([keys.len(), labels.len()]);
                usage.add_bytes(message.len());
                for label in labels {
                    if usage.exhausted() {
                        return;
                    }
                    usage.add_bytes(label.len());
                }
            }
        },
        NodeKind::ChangeEventExpand { events, .. } => {
            usage.add_items(events.len());
            for event in events {
                if usage.exhausted() {
                    return;
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
                    return;
                }
                validate_value_type(
                    &field.ty,
                    &format!("{path}.target_fields[{index}].type"),
                    usage,
                    errors,
                );
            }
            add_writer_schema_usage(
                &target.output_schema,
                &format!("{path}.output_schema"),
                usage,
                errors,
            );
            for (index, aggregate) in target.partial_aggregates.iter().enumerate() {
                if usage.exhausted() {
                    return;
                }
                add_aggregate_binding_usage(
                    &aggregate.binding,
                    &format!("{path}.partial_aggregates[{index}].binding"),
                    usage,
                    errors,
                );
            }
        }
        NodeKind::TableFinish(spec) => add_writer_finish_usage(spec, path, usage, errors),
        NodeKind::Filter { .. } | NodeKind::Limit { .. } | NodeKind::GenerateSeries { .. } => {}
    }
}

fn add_writer_finish_usage(
    spec: &WriterFinishSpec,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_item_counts([
        spec.expected_target_ordinals.len(),
        spec.final_aggregates.len(),
    ]);
    add_writer_schema_usage(
        &spec.input_schema,
        &format!("{path}.input_schema"),
        usage,
        errors,
    );
    add_writer_schema_usage(
        &spec.output_schema,
        &format!("{path}.output_schema"),
        usage,
        errors,
    );
    for (index, aggregate) in spec.final_aggregates.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        add_aggregate_binding_usage(
            &aggregate.binding,
            &format!("{path}.final_aggregates[{index}].binding"),
            usage,
            errors,
        );
    }
    if let Some(grouped) = &spec.grouped_unpivot {
        usage.add_item_counts([
            grouped.statistics_target_ordinals.len(),
            grouped.literal_outputs.len(),
            grouped.mappings.len(),
        ]);
        for mapping in &grouped.mappings {
            if usage.exhausted() {
                return;
            }
            usage.add_items(mapping.constants.len());
            for constant in &mapping.constants {
                if usage.exhausted() {
                    return;
                }
                add_unpivot_constant_usage(constant, usage);
            }
        }
    }
}

fn add_writer_schema_usage(
    schema: &WriterRelationSchema,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_items(schema.fields.len());
    for (index, field) in schema.fields.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        usage.add_bytes(field.name.len());
        validate_value_type(
            &field.ty,
            &format!("{path}.fields[{index}].type"),
            usage,
            errors,
        );
    }
}

fn add_unpivot_constant_usage(constant: &UnpivotConstant, usage: &mut ResourceUsage) {
    match constant {
        UnpivotConstant::Scalar(_) => {}
        UnpivotConstant::Int32List(values) => usage.add_items(values.len()),
        UnpivotConstant::Utf8Map(entries) => {
            usage.add_items(entries.len());
            for (key, value) in entries {
                if usage.exhausted() {
                    return;
                }
                usage.add_byte_counts([key.len(), value.len()]);
            }
        }
    }
}

fn add_relation_usage(
    relation: &Relation,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    let (schema, guarantees, artifacts, evidence_bytes, metadata_kind_bytes) = match relation {
        Relation::Data(relation) => (
            relation.schema.as_ref(),
            relation.predicate_guarantees.as_ref(),
            relation.artifact_inputs.as_ref(),
            0,
            0,
        ),
        Relation::Metadata(relation) => (
            relation.schema.as_ref(),
            relation.predicate_guarantees.as_ref(),
            relation.artifact_inputs.as_ref(),
            relation.coverage_evidence.len(),
            relation.kind.as_str().len(),
        ),
    };
    usage.add_item_counts([schema.len(), guarantees.len(), artifacts.len()]);
    usage.add_byte_counts([evidence_bytes, metadata_kind_bytes]);
    add_properties_usage(relation.provided_properties(), usage);
    add_read_reference_usage(relation.read(), usage);
    for (index, field) in schema.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        add_encoded_payload_usage(&field.column.column_payload, usage);
        validate_value_type(
            &field.ty,
            &format!("{path}.schema[{index}].type"),
            usage,
            errors,
        );
    }
    for (index, artifact) in artifacts.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        add_artifact_requirement_usage(
            artifact,
            &format!("{path}.artifact_inputs[{index}]"),
            usage,
            errors,
        );
    }
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

fn add_artifact_source_usage(
    source: &ArtifactSourceBinding,
    _path: &str,
    usage: &mut ResourceUsage,
) {
    add_read_reference_usage(&source.source, usage);
}

fn add_artifact_requirement_usage(
    artifact: &ArtifactInputRequirement,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_items(artifact.schema.len());
    usage.add_byte_counts([
        artifact.kind.as_str().len(),
        artifact.format.id.as_str().len(),
    ]);
    add_artifact_source_usage(&artifact.source, &format!("{path}.source"), usage);
    add_coverage_usage(&artifact.required_coverage, usage);
    for (ordinal, ty) in artifact.schema.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        validate_value_type(ty, &format!("{path}.schema[{ordinal}]"), usage, errors);
    }
}

fn add_artifact_ref_usage(
    artifact: &SealedArtifactRef,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_items(artifact.schema.len());
    usage.add_byte_counts([
        artifact.kind.as_str().len(),
        artifact.format.id.as_str().len(),
        artifact.location.len(),
    ]);
    add_artifact_source_usage(&artifact.source, &format!("{path}.source"), usage);
    add_coverage_usage(&artifact.coverage, usage);
    for (ordinal, ty) in artifact.schema.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        validate_value_type(ty, &format!("{path}.schema[{ordinal}]"), usage, errors);
    }
}

fn add_sink_usage(
    sink: &FragmentSink,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    match sink {
        FragmentSink::Multicast { edges } => usage.add_items(edges.len()),
        FragmentSink::Router { routes, .. } => {
            usage.add_items(routes.len());
            for route in routes {
                if usage.exhausted() {
                    return;
                }
                usage.add_item_counts([
                    route.accepted_effects.len(),
                    route.input_mapping.len(),
                    route.partition_by.len(),
                ]);
            }
        }
        FragmentSink::SealedArtifact(spec) => {
            add_artifact_sink_usage(spec, path, usage, errors);
        }
        FragmentSink::Result | FragmentSink::Stream { .. } | FragmentSink::Noop => {}
    }
}

fn add_artifact_sink_usage(
    spec: &SealedArtifactSinkSpec,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_item_counts([
        spec.input.len(),
        spec.partition_by.len(),
        spec.order_by.len(),
        spec.group_boundaries.len(),
    ]);
    usage.add_byte_counts([spec.kind.as_str().len(), spec.format.id.as_str().len()]);
    add_artifact_source_usage(&spec.source, &format!("{path}.source"), usage);
    add_coverage_usage(&spec.required_coverage, usage);
    for (index, field) in spec.input.iter().enumerate() {
        if usage.exhausted() {
            return;
        }
        validate_value_type(
            &field.ty,
            &format!("{path}.input[{index}].type"),
            usage,
            errors,
        );
    }
}

fn add_coverage_usage(coverage: &CoverageSet, usage: &mut ResourceUsage) {
    usage.add_items(coverage.ranges.len());
    usage.add_bytes(coverage.domain.len());
    for range in &coverage.ranges {
        if usage.exhausted() {
            return;
        }
        usage.add_byte_counts([
            range.start.as_deref().map_or(0, <[u8]>::len),
            range.end.as_deref().map_or(0, <[u8]>::len),
        ]);
    }
}

fn add_runtime_filter_usage(
    filter: &RuntimeFilter,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    if usage.exhausted() {
        return;
    }
    match &filter.domain {
        RuntimeFilterDomain::Membership { ty, .. } => {
            validate_value_type(ty, &format!("{path}.domain.type"), usage, errors);
        }
        RuntimeFilterDomain::Ordered { key, .. } => {
            usage.add_items(1);
            validate_value_type(&key.ty, &format!("{path}.domain.key.type"), usage, errors);
        }
    }
    if usage.exhausted() {
        return;
    }
    add_runtime_filter_coverage_usage(
        &filter.availability_coverage,
        &format!("{path}.availability_coverage"),
        usage,
        errors,
    );
    if usage.exhausted() {
        return;
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
            return;
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
            return;
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
    match distribution {
        crate::Distribution::Hash { keys, .. }
        | crate::Distribution::BucketShuffle { keys, .. } => keys.len(),
        _ => 0,
    }
}

fn validate_value_type(
    ty: &ValueType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    validate_data_type(&ty.data_type, path, usage, errors);
}

fn validate_data_type(
    root: &DataType,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    if usage.exhausted() {
        return;
    }
    let mut pending = vec![(root, 1_usize)];
    let mut nodes = 0_usize;
    while let Some((data_type, depth)) = pending.pop() {
        if usage.exhausted() {
            return;
        }
        nodes = nodes.saturating_add(1);
        usage.add_items(1);
        if depth > MAX_DATA_TYPE_DEPTH {
            errors.push(ValidationError::resource_limit(
                path,
                format!("Arrow data type depth exceeds {MAX_DATA_TYPE_DEPTH}"),
            ));
            return;
        }
        if nodes > MAX_DATA_TYPE_NODES {
            errors.push(ValidationError::resource_limit(
                path,
                format!("Arrow data type contains more than {MAX_DATA_TYPE_NODES} nodes"),
            ));
            return;
        }
        match data_type {
            DataType::Timestamp(_, Some(timezone)) => {
                usage.add_bytes(timezone.len());
                if timezone.len() > MAX_TIMESTAMP_TIMEZONE_BYTES {
                    errors.push(ValidationError::resource_limit(
                        path,
                        format!(
                            "Arrow timestamp timezone exceeds {MAX_TIMESTAMP_TIMEZONE_BYTES} bytes"
                        ),
                    ));
                }
            }
            DataType::FixedSizeBinary(size) if *size < 0 || *size > MAX_FIXED_SIZE_LENGTH => {
                invalid_fixed_size(path, *size, errors);
            }
            DataType::Time32(unit) if !matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) => {
                errors.push(ValidationError::new(
                    path,
                    "Arrow Time32 must use second or millisecond units",
                ));
            }
            DataType::Time64(unit)
                if !matches!(unit, TimeUnit::Microsecond | TimeUnit::Nanosecond) =>
            {
                errors.push(ValidationError::new(
                    path,
                    "Arrow Time64 must use microsecond or nanosecond units",
                ));
            }
            DataType::FixedSizeList(field, size) => {
                if *size < 0 || *size > MAX_FIXED_SIZE_LENGTH {
                    invalid_fixed_size(path, *size, errors);
                }
                validate_field(field, path, usage, errors);
                pending.push((field.data_type(), depth.saturating_add(1)));
            }
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field)
            | DataType::Map(field, _) => {
                validate_field(field, path, usage, errors);
                pending.push((field.data_type(), depth.saturating_add(1)));
            }
            DataType::Struct(fields) => {
                if data_type_children_exceed_budget(
                    nodes,
                    pending.len(),
                    fields.len(),
                    path,
                    errors,
                ) {
                    return;
                }
                usage.add_items(fields.len());
                for field in fields {
                    validate_field(field, path, usage, errors);
                    pending.push((field.data_type(), depth.saturating_add(1)));
                }
            }
            DataType::Union(fields, _) => {
                if data_type_children_exceed_budget(
                    nodes,
                    pending.len(),
                    fields.len(),
                    path,
                    errors,
                ) {
                    return;
                }
                usage.add_items(fields.len());
                for (_, field) in fields.iter() {
                    validate_field(field, path, usage, errors);
                    pending.push((field.data_type(), depth.saturating_add(1)));
                }
            }
            DataType::Dictionary(key, value) => {
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
                    errors.push(ValidationError::new(
                        path,
                        "Arrow dictionary key must be an integer type",
                    ));
                }
                if data_type_children_exceed_budget(nodes, pending.len(), 2, path, errors) {
                    return;
                }
                pending.push((key, depth.saturating_add(1)));
                pending.push((value, depth.saturating_add(1)));
            }
            DataType::RunEndEncoded(run_ends, values) => {
                if !matches!(
                    run_ends.data_type(),
                    DataType::Int16 | DataType::Int32 | DataType::Int64
                ) {
                    errors.push(ValidationError::new(
                        path,
                        "Arrow run-end type must be Int16, Int32 or Int64",
                    ));
                }
                if data_type_children_exceed_budget(nodes, pending.len(), 2, path, errors) {
                    return;
                }
                validate_field(run_ends, path, usage, errors);
                validate_field(values, path, usage, errors);
                pending.push((run_ends.data_type(), depth.saturating_add(1)));
                pending.push((values.data_type(), depth.saturating_add(1)));
            }
            DataType::Decimal32(precision, scale) => validate_decimal(
                path,
                *precision,
                *scale,
                DECIMAL32_MAX_PRECISION,
                DECIMAL32_MAX_SCALE,
                errors,
            ),
            DataType::Decimal64(precision, scale) => validate_decimal(
                path,
                *precision,
                *scale,
                DECIMAL64_MAX_PRECISION,
                DECIMAL64_MAX_SCALE,
                errors,
            ),
            DataType::Decimal128(precision, scale) => validate_decimal(
                path,
                *precision,
                *scale,
                DECIMAL128_MAX_PRECISION,
                DECIMAL128_MAX_SCALE,
                errors,
            ),
            DataType::Decimal256(precision, scale) => validate_decimal(
                path,
                *precision,
                *scale,
                DECIMAL256_MAX_PRECISION,
                DECIMAL256_MAX_SCALE,
                errors,
            ),
            _ => {}
        }
    }
}

fn data_type_children_exceed_budget(
    visited: usize,
    pending: usize,
    children: usize,
    path: &str,
    errors: &mut ValidationContext,
) -> bool {
    if visited.saturating_add(pending).saturating_add(children) <= MAX_DATA_TYPE_NODES {
        return false;
    }
    errors.push(ValidationError::resource_limit(
        path,
        format!("Arrow data type contains more than {MAX_DATA_TYPE_NODES} nodes"),
    ));
    true
}

fn validate_field(
    field: &Field,
    path: &str,
    usage: &mut ResourceUsage,
    errors: &mut ValidationContext,
) {
    usage.add_bytes(field.name().len());
    if field.name().len() > MAX_DATA_TYPE_FIELD_NAME_BYTES {
        errors.push(ValidationError::resource_limit(
            path,
            format!("Arrow field name exceeds {MAX_DATA_TYPE_FIELD_NAME_BYTES} bytes"),
        ));
    }
    if field.metadata().len() > MAX_DATA_TYPE_FIELD_METADATA_ENTRIES {
        errors.push(ValidationError::resource_limit(path, format!( "Arrow field metadata contains more than {MAX_DATA_TYPE_FIELD_METADATA_ENTRIES} entries" )));
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
            errors.push(ValidationError::resource_limit(
                path,
                "Arrow field metadata key or value exceeds its byte limit",
            ));
        }
    }
    usage.add_bytes(metadata_bytes);
    if metadata_bytes > MAX_DATA_TYPE_FIELD_METADATA_BYTES {
        errors.push(ValidationError::resource_limit(
            path,
            format!("Arrow field metadata exceeds {MAX_DATA_TYPE_FIELD_METADATA_BYTES} bytes"),
        ));
    }
}

fn validate_decimal(
    path: &str,
    precision: u8,
    scale: i8,
    max_precision: u8,
    max_scale: i8,
    errors: &mut ValidationContext,
) {
    if precision == 0 || precision > max_precision || scale < -max_scale || scale > max_scale {
        errors.push(ValidationError::new(path, format!( "Arrow decimal precision/scale ({precision}, {scale}) is outside 1..={max_precision} and -{max_scale}..={max_scale}" )));
    }
}

fn invalid_fixed_size(path: &str, size: i32, errors: &mut ValidationContext) {
    // A negative length is not a plan that is too large; it is a type that
    // cannot exist. Only the upper bound is a limit an operator could raise.
    if size < 0 {
        errors.push(ValidationError::new(
            path,
            format!("Arrow fixed-size length {size} is negative"),
        ));
        return;
    }
    errors.push(ValidationError::resource_limit(
        path,
        format!("Arrow fixed-size length {size} exceeds {MAX_FIXED_SIZE_LENGTH}"),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn function_and_aggregate_identity_bytes_are_accounted() {
        let function = BoundFunction {
            function_id: novarocks_type_contract::FunctionId::try_new("f".repeat(1024)).unwrap(),
            overload: novarocks_type_contract::FunctionOverloadId::try_new("o".repeat(1024))
                .unwrap(),
            kind: crate::FunctionKind::Aggregate,
            argument_types: Box::default(),
            result_type: ValueType {
                data_type: DataType::Int64,
                nullable: false,
            },
            volatility: novarocks_type_contract::FunctionVolatility::Immutable,
            argument_evaluation: novarocks_type_contract::FunctionArgumentEvaluation::Eager,
            failure_behavior: novarocks_type_contract::FunctionFailureBehavior::Propagate,
            intrinsic_row_error:
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
        };
        let binding = AggregateBinding {
            function,
            phase: crate::AggregatePhase::Single,
            logical_argument_count: 0,
            intermediate_type: ValueType {
                data_type: DataType::Int64,
                nullable: false,
            },
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
