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

use super::*;
use crate::resource::CutResourcePreflight;
use crate::{FragmentPackageInput, FragmentSink, RuntimeFilterApplyPoint};

// Extraction must bound the externally supplied call references before
// materializing them. This is a necessary subset of the same package profile;
// the final package constructor still checks cuts and all other owned facts.
pub(crate) fn validate_fragment_parameter_resource_usage(
    fragment: &Fragment,
    limits: PlanLimits,
    semantic_items: usize,
) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::for_construction(limits);
    let mut usage = CutResourcePreflight::new();
    usage.add_fragment(fragment, &mut errors);
    usage.add_items(semantic_items);
    usage.validate("package.resources", &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

pub(crate) fn validate_package(
    input: &FragmentPackageInput,
    limits: PlanLimits,
    semantic_items: usize,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), crate::FragmentPackageError> {
    validate_package_core(
        input,
        limits,
        semantic_items,
        crate::constants::ConstantValidationMode::Plain,
        None,
        work,
    )
}

pub(crate) fn validate_package_in(
    input: &FragmentPackageInput,
    limits: PlanLimits,
    semantic_items: usize,
    source_retained_bytes: usize,
    resources: &mut novarocks_type_contract::ControlResourceCounter,
    admit: &mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), novarocks_type_contract::CompileControlError>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), crate::FragmentPackageError> {
    validate_package_core(
        input,
        limits,
        semantic_items,
        crate::constants::ConstantValidationMode::Caller,
        Some((resources, admit, source_retained_bytes)),
        work,
    )
}

type PackageScratch<'a> = (
    &'a mut novarocks_type_contract::ControlResourceCounter,
    &'a mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), novarocks_type_contract::CompileControlError>,
    usize,
);

fn validate_package_core(
    input: &FragmentPackageInput,
    limits: PlanLimits,
    semantic_items: usize,
    mode: crate::constants::ConstantValidationMode,
    mut scratch: Option<PackageScratch<'_>>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), crate::FragmentPackageError> {
    let mut errors = ValidationContext::for_construction(limits);
    let fragment = &input.fragment;
    let mut usage = CutResourcePreflight::new();
    if let Some((resources, admit, source_retained_bytes)) = scratch.as_mut() {
        usage
            .add_fragment_in(
                fragment,
                &mut errors,
                *source_retained_bytes,
                resources,
                *admit,
                work,
            )
            .map_err(crate::package::package_resource_error)?;
        usage
            .add_cuts_in(
                fragment,
                &input.cuts,
                &mut errors,
                *source_retained_bytes,
                resources,
                *admit,
                work,
            )
            .map_err(crate::package::package_resource_error)?;
    } else {
        usage.add_fragment(fragment, &mut errors);
        usage.add_cuts(fragment, &input.cuts, &mut errors);
    }
    usage
        .add_constants_observed(&input.constants, work)
        .map_err(crate::FragmentPackageError::Control)?;
    let unpivot = match mode {
        crate::constants::ConstantValidationMode::Plain => {
            usage.add_unpivot_sources_observed(fragment, &input.constants, limits, work)
        }
        crate::constants::ConstantValidationMode::Caller => {
            usage.add_unpivot_sources_in(fragment, &input.constants, limits, work)
        }
    };
    unpivot.map_err(|error| match error {
        crate::ConstantReferenceError::Control(cause) => {
            crate::FragmentPackageError::Control(cause)
        }
        error => crate::FragmentPackageError::Constant(error),
    })?;
    // Count the immutable control representation in the same package dynamic
    // item bound. These counts are not a decoded-allocation or peak-byte model.
    let control = &input.expression_uses;
    usage.add_items(control.flow().regions().len());
    usage.add_items(control.flow().use_reference_count());
    usage.add_items(control.bindings().len());
    usage.add_items(control.roots().sites().len());
    usage.add_items(semantic_items);
    usage.add_items(input.scans.len());
    for scan in input.scans.values() {
        usage.add_bytes(scan.retained_bytes());
        usage.add_items(scan.scan().assignments().len());
    }
    usage.add_items(input.layouts.len());
    for layout in input.layouts.values() {
        usage.add_items(layout.inputs().len());
        usage.add_bytes(layout.inputs().len().saturating_mul(std::mem::size_of::<
            novarocks_connector_contract::ConnectorWriteFieldToken,
        >()));
        usage.add_bytes(layout.payload().payload().len());
    }
    usage.add_items(input.writes.len());
    for write in input.writes.values() {
        usage.add_bytes(write.charged_bytes());
        usage.add_items(write.input().field_count());
    }
    if let Some(result) = &input.result {
        usage.add_items(result.fields.len());
        for field in &result.fields {
            usage.add_bytes(field.name.len());
            usage.add_bytes(field.alias.as_ref().map_or(0, |alias| alias.len()));
            if let Some((resources, admit, source_retained_bytes)) = scratch.as_mut() {
                usage
                    .add_value_type_in(
                        &field.ty,
                        "package.result.type",
                        &mut errors,
                        *source_retained_bytes,
                        resources,
                        *admit,
                        work,
                    )
                    .map_err(crate::package::package_resource_error)?;
            } else {
                usage.add_value_type(&field.ty, "package.result.type", &mut errors);
            }
        }
    }
    usage.add_items(input.parameters.entries().len());
    for value in input.parameters.entries().values() {
        if let novarocks_type_contract::SemanticParameterValue::TimeZone(zone) = value {
            usage.add_bytes(zone.len());
        }
    }
    usage.add_items(input.annotations.len());
    for annotation in &input.annotations {
        usage.add_bytes(annotation.key.len());
        usage.add_bytes(annotation.value.len());
    }
    usage.validate("package.resources", &mut errors);
    if !errors.is_empty() {
        return Err(crate::FragmentPackageError::Structure(
            ValidationErrors::from_collector(errors),
        ));
    }
    if input.required.plan_contract_revision != PLAN_CONTRACT_REVISION {
        errors.push(ValidationError::new(
            "package.required",
            "fragment package plan contract revision differs",
        ));
    }
    if let Some((resources, admit, _)) = scratch {
        validate_fragment_structure_into_in(fragment, &mut errors, resources, admit, work)
            .map_err(crate::package::package_resource_error)?;
    } else {
        validate_fragment_structure_into(fragment, &mut errors);
    }
    if !errors.is_empty() {
        return Err(crate::FragmentPackageError::Structure(
            ValidationErrors::from_collector(errors),
        ));
    }
    validate_fragment_cuts_into(fragment, &input.cuts, &mut errors);
    validate_fragment_partition_identities(fragment, &input.cuts, &mut errors);
    match (fragment.sink(), &input.result) {
        (FragmentSink::Result | FragmentSink::RootResult(_), Some(result)) => {
            validate_result_port_fields(fragment, result, &mut errors)
        }
        (FragmentSink::Result | FragmentSink::RootResult(_), None) => errors.push(
            ValidationError::new("package.result", "result sink has no result port"),
        ),
        (_, Some(_)) => errors.push(ValidationError::new(
            "package.result",
            "result port has no result sink",
        )),
        (_, None) => {}
    }
    validate_annotation_table(
        &input.annotations,
        |subject| match subject {
            AnnotationSubject::Plan => false,
            AnnotationSubject::Fragment(id) => id == fragment.id(),
            AnnotationSubject::Node(id, node) => {
                id == fragment.id() && fragment.nodes().contains_key(&node)
            }
            AnnotationSubject::Value(id, value) => {
                id == fragment.id() && fragment.values().contains_key(&value)
            }
        },
        &mut errors,
    );
    validate_package_scans(input, &mut errors);
    validate_package_writes(input, &mut errors);
    super::graph::validate_write_layout_recipes(
        &input.fragment,
        input.layouts.iter().map(|(node, recipe)| (*node, recipe)),
        &mut errors,
    );
    if errors.is_empty() {
        Ok(())
    } else {
        Err(crate::FragmentPackageError::Structure(
            ValidationErrors::from_collector(errors),
        ))
    }
}

fn validate_package_scans(input: &FragmentPackageInput, errors: &mut ValidationContext) {
    let fragment = &input.fragment;
    let mut consumers_by_scan: BTreeMap<NodeId, BTreeMap<crate::RuntimeFilterId, Vec<_>>> =
        BTreeMap::new();
    for filter in &input.cuts.runtime_filters {
        for consumer in &filter.consumers {
            if consumer.endpoint.fragment == fragment.id()
                && consumer.apply_point == RuntimeFilterApplyPoint::ScanSource
            {
                consumers_by_scan
                    .entry(consumer.endpoint.node)
                    .or_default()
                    .entry(filter.id)
                    .or_default()
                    .push(consumer);
            }
        }
    }
    for node in fragment.nodes().values() {
        let NodeKind::Scan {
            relation,
            read_budget,
            provider_outputs,
            ..
        } = &node.kind
        else {
            continue;
        };
        let path = format!("package.scans[{}]", node.id.get());
        let Some(scan) = input.scans.get(&node.id) else {
            errors.push(ValidationError::new(
                &path,
                "physical scan has no complete frozen public facts",
            ));
            continue;
        };
        let frozen = scan;
        let scan = frozen.scan();
        let public = frozen.public_facts();
        let metadata_matches = match relation.as_ref() {
            crate::Relation::Data(_) => public.metadata_kind().is_none(),
            crate::Relation::Metadata(metadata) => {
                public
                    .metadata_kind()
                    .is_some_and(|kind| kind.as_str() == metadata.kind.as_str())
                    && public.source().coverage_evidence() == metadata.coverage_evidence.as_ref()
            }
        };
        // The path names the first differing part, so a refusal says which
        // public fact disagrees with the physical relation.
        let differing = if public.source().input_version() != &relation.read().input_version {
            Some(".input_version".to_owned())
        } else if public.source().selection_digest() != relation.selection_digest() {
            Some(".selection_digest".to_owned())
        } else if !metadata_matches {
            Some(".metadata".to_owned())
        } else if public.schema().fields().len() != relation.schema().len() {
            Some(".schema".to_owned())
        } else {
            relation
                .schema()
                .iter()
                .enumerate()
                .position(|(ordinal, field)| !public.matches_value_type(ordinal, &field.ty))
                .map(|ordinal| format!(".schema[{ordinal}]"))
        };
        if let Some(part) = differing {
            errors.push(ValidationError::new(format!("{path}{part}"),
                "frozen read public version, selection, metadata or exact schema differs from its physical relation"));
        }
        validate_public_read_properties(frozen, relation, provider_outputs, &path, errors);
        let recipe = scan.recipe();
        if recipe.binding() != &relation.read().binding
            || recipe.relation() != &relation.read().relation
            || recipe.columns().len() != relation.schema().len()
            || !recipe
                .columns()
                .iter()
                .zip(relation.schema())
                .all(|(column, field)| column == &field.column.column_payload)
        {
            errors.push(ValidationError::new(
                &path,
                "frozen scan recipe differs from its exact physical relation or column occurrence",
            ));
        }
        if scan.assignments().len() != relation.schema().len()
            || !scan
                .assignments()
                .iter()
                .zip(relation.schema())
                .all(|(assignment, field)| {
                    novarocks_connector_contract::connector_type_accepts_arrow(
                        assignment.value_type(),
                        field.ty.data_type(),
                    )
                })
        {
            errors.push(ValidationError::new(
                &path,
                "frozen scan assignment type differs from its exact physical schema ordinal",
            ));
        }
        if scan.max_batch_rows().get() != read_budget.max_batch_rows
            || scan.max_batch_bytes().get() != read_budget.max_batch_bytes
        {
            errors.push(ValidationError::new(
                &path,
                "frozen scan execution bounds or work source differ from the physical scan",
            ));
        }
        let assignment_columns = scan
            .assignments()
            .iter()
            .enumerate()
            .map(|(index, assignment)| (assignment.variable(), index))
            .collect::<BTreeMap<_, _>>();
        let mut actual_filters = BTreeSet::new();
        for dynamic in scan.dynamic_filters() {
            let filter_id = crate::RuntimeFilterId::new(dynamic.filter_id());
            let value = assignment_columns
                .get(dynamic.variable())
                .and_then(|ordinal| provider_outputs.get(*ordinal))
                .map(|(_, value)| value);
            let valid = consumers_by_scan
                .get(&node.id)
                .and_then(|filters| filters.get(&filter_id))
                .is_some_and(|consumers| {
                    !consumers.is_empty()
                        && consumers.iter().all(|consumer| {
                            consumer.endpoint.values.len() == 1
                                && consumer.endpoint.values.first() == value
                        })
                });
            if !valid {
                errors.push(ValidationError::new(
                    &path,
                    "dynamic filter does not bind the exact local scan consumer column",
                ));
            }
            actual_filters.insert(filter_id);
        }
        let expected_filters = consumers_by_scan
            .get(&node.id)
            .map(|filters| filters.keys().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        if actual_filters != expected_filters {
            errors.push(ValidationError::new(
                &path,
                "frozen scan dynamic filters do not cover its exact local consumers",
            ));
        }
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    for id in input.scans.keys() {
        if !fragment
            .nodes()
            .get(id)
            .is_some_and(|node| matches!(node.kind, NodeKind::Scan { .. }))
        {
            errors.push(ValidationError::new(
                "package.scans",
                "frozen scan facts name a missing or non-scan node",
            ));
        }
    }
}

fn validate_package_writes(input: &FragmentPackageInput, errors: &mut ValidationContext) {
    for node in input.fragment.nodes().values() {
        let NodeKind::TableWriter { target } = &node.kind else {
            continue;
        };
        let path = format!("package.writes[{}]", node.id.get());
        let Some(write) = input.writes.get(&node.id) else {
            errors.push(ValidationError::new(
                &path,
                "physical writer has no complete frozen public facts",
            ));
            continue;
        };
        // The path names the first differing part, so a refusal says which
        // side of the recipe disagrees with the plan.
        let differing = if write.payload() != &target.handle {
            Some(".handle".to_owned())
        } else if write.input().field_count() != target.target_fields.len() {
            Some(".fields".to_owned())
        } else {
            write
                .input()
                .fields_iter()
                .zip(&target.target_fields)
                .enumerate()
                .find_map(|(ordinal, (field, target))| {
                    let part = if field.token() != target.token {
                        "token"
                    } else if field.field().name() != target.provider_name.as_ref() {
                        "name"
                    } else if field.storage_type() != target.storage_type.as_ref() {
                        "storage_type"
                    } else if let Some(actual) = field.execution_type() {
                        if actual.nullable() != target.ty.nullable() {
                            "nullable"
                        } else if !actual.same_semantics(&target.ty) {
                            "value_type"
                        } else if actual.typed_binding().layout()
                            != target.ty.typed_binding().layout()
                        {
                            "value_layout"
                        } else {
                            return None;
                        }
                    } else {
                        legacy_writer_field_difference(field.field(), &target.ty)?
                    };
                    Some(format!(".fields[{ordinal}].{part}"))
                })
        };
        if let Some(part) = differing {
            errors.push(ValidationError::new(
                format!("{path}{part}"),
                "frozen writer recipe differs from its exact physical handle or input field occurrence",
            ));
        }
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    for id in input.writes.keys() {
        if !input
            .fragment
            .nodes()
            .get(id)
            .is_some_and(|node| matches!(node.kind, NodeKind::TableWriter { .. }))
        {
            errors.push(ValidationError::new(
                "package.writes",
                "frozen write facts name a missing or non-writer node",
            ));
        }
    }
}

fn validate_public_read_properties(
    frozen: &novarocks_connector_contract::FrozenConnectorRead,
    relation: &crate::Relation,
    outputs: &[(crate::ProviderColumnReference, crate::ValueId)],
    path: &str,
    errors: &mut ValidationContext,
) {
    use novarocks_connector_contract::{
        ConnectorReadDistribution, ConnectorReadNullOrdering, ConnectorReadSortDirection,
    };
    let public = frozen.public_facts().source().properties();
    let physical = relation.provided_properties();
    let distribution_matches = matches!(
        (public.distribution(), &physical.distribution),
        (
            ConnectorReadDistribution::Unconstrained,
            crate::Distribution::Unconstrained
        ) | (
            ConnectorReadDistribution::Singleton,
            crate::Distribution::Singleton
        ) | (
            ConnectorReadDistribution::RoundRobin,
            crate::Distribution::RoundRobin
        )
    );
    let ordering_matches = public.ordering().len() == physical.ordering.len()
        && public
            .ordering()
            .iter()
            .zip(&physical.ordering)
            .all(|(public, physical)| {
                outputs
                    .get(public.column().index())
                    .is_some_and(|(_, value)| *value == physical.value)
                    && matches!(
                        (public.direction(), physical.direction),
                        (
                            ConnectorReadSortDirection::Ascending,
                            crate::SortDirection::Ascending
                        ) | (
                            ConnectorReadSortDirection::Descending,
                            crate::SortDirection::Descending
                        )
                    )
                    && matches!(
                        (public.null_ordering(), physical.null_ordering),
                        (ConnectorReadNullOrdering::First, crate::NullOrdering::First)
                            | (ConnectorReadNullOrdering::Last, crate::NullOrdering::Last)
                    )
            });
    if !distribution_matches || !ordering_matches {
        errors.push(ValidationError::new(
            path,
            "frozen read source properties differ from their exact physical column projection",
        ));
    }
}

/// Expansion-only adapter for existing provider drafts. P05 supplies complete
/// execution projections; P07 removes this None branch and its metadata law.
/// This is the prior field comparison, never the author of a new typed binding.
fn legacy_writer_field_difference(
    field: &arrow_schema::Field,
    target: &crate::ValueType,
) -> Option<&'static str> {
    if novarocks_type_contract::field_logical_type(field) != Ok(target.logical_type()) {
        Some("logical_type")
    } else if !novarocks_connector_contract::arrow_data_types_exact(
        field.data_type(),
        target.data_type(),
    ) {
        Some("data_type")
    } else if field.is_nullable() != target.nullable() {
        Some("nullable")
    } else {
        None
    }
}
