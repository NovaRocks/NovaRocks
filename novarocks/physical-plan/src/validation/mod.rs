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

mod aggregate;
mod cuts;
mod error;
mod expr;
mod graph;
mod guarantee;
mod index;
mod io_cuts;
mod limits;
mod node;
mod package;
mod properties;
mod runtime_filter;
mod summary;

#[cfg(test)]
mod tests;

pub(crate) use aggregate::*;
pub use cuts::*;
pub use error::*;
pub(crate) use expr::*;
pub(crate) use graph::*;
pub(crate) use index::*;
pub use io_cuts::*;
pub use limits::*;
pub(crate) use node::*;
pub(crate) use package::*;
pub(crate) use properties::*;
pub use properties::{
    FragmentPropertyError, derive_fragment_output_properties_in,
    derive_fragment_output_properties_observed,
    derive_replica_sensitive_output_properties_observed, validate_fragment_output_properties_in,
    validate_fragment_output_properties_observed,
};
pub(crate) use runtime_filter::*;
pub(crate) use summary::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::resource::{
    MAX_ANNOTATION_BYTES, MAX_ANNOTATION_KEY_BYTES, MAX_ANNOTATION_VALUE_BYTES, MAX_ANNOTATIONS,
    MAX_PLAN_DERIVED_CUT_BYTES, MAX_PLAN_DERIVED_CUT_ITEMS, validate_fragment_cut_resources,
    validate_fragment_resources, validate_plan_resources,
};
use crate::{
    AnnotationSubject, ExprId, Fragment, FragmentCuts, FragmentId, NodeId, NodeKind,
    PLAN_CONTRACT_REVISION, PhysicalPlan, RequiredContracts, ValueId,
};

// Guard rails, not operational bounds: these fence off values no
// legitimate plan produces. Counting them as structural protection would
// overstate what is actually bounded. Real bounds live in `PlanLimits`.
pub const MAX_RUNTIME_FILTER_ARTIFACT_BYTES: u64 = 1 << 30;
pub const MAX_RUNTIME_FILTER_DEADLINE_MS: u64 = 86_400_000;
pub const MAX_RUNTIME_FILTER_RETRIES: u32 = 100;
pub const MAX_PROVIDER_PRIVATE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_METADATA_COVERAGE_EVIDENCE_BYTES: usize = 1024 * 1024;
pub const MAX_ARTIFACT_REFERENCE_BYTES: u32 = 16 * 1024 * 1024;
pub const MAX_UNPIVOT_LITERAL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_UNPIVOT_OUTPUT_ROWS: u64 = 1 << 30;
pub const MAX_UNPIVOT_OUTPUT_BYTES: u64 = 1 << 30;
pub const MAX_PARTITION_COUNT: u32 = 1 << 20;
pub const MAX_SCAN_BATCH_ROWS: u64 = 1 << 30;
pub const MAX_SCAN_BATCH_BYTES: u64 = 1 << 30;
pub const MAX_PIPELINE_DOP: u32 = 1 << 20;

pub fn validate_fragment(fragment: &Fragment, cuts: &FragmentCuts) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::new();
    validate_fragment_into(fragment, &mut errors);
    validate_fragment_cut_resources(fragment, cuts, &mut errors);
    if !errors.is_empty() {
        return Err(ValidationErrors::from_collector(errors));
    }
    validate_fragment_cuts_into(fragment, cuts, &mut errors);
    validate_fragment_partition_identities(fragment, cuts, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

pub(crate) fn validate_fragment_definition(fragment: &Fragment) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::new();
    validate_fragment_into(fragment, &mut errors);
    validate_fragment_partition_identities(fragment, &FragmentCuts::default(), &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

/// The sole caller admits this same owned source and its exact pending
/// runtime-filter count before creating the reference array. Do not repeat
/// that resource walk after the array has been installed.
pub(crate) fn validate_fragment_construction_after_admission(
    fragment: &Fragment,
    limits: PlanLimits,
) -> Result<(), ValidationErrors> {
    let mut errors = ValidationContext::for_construction(limits);
    validate_fragment_structure_into(fragment, &mut errors);
    validate_fragment_partition_identities(fragment, &FragmentCuts::default(), &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

/// The same complete construction laws with the caller's original scratch
/// counter and meter. Other expression/type/diagnostic owners remain separate.
pub(crate) fn validate_fragment_construction_after_admission_in(
    fragment: &Fragment,
    limits: PlanLimits,
    resources: &mut novarocks_type_contract::ControlResourceCounter,
    admit: &mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), novarocks_type_contract::CompileControlError>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), FragmentPropertyError> {
    let mut errors = ValidationContext::for_construction(limits);
    validate_fragment_structure_into_in(fragment, &mut errors, resources, admit, work)?;
    validate_fragment_partition_identities(fragment, &FragmentCuts::default(), &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(FragmentPropertyError::Structure(
            ValidationErrors::from_collector(errors),
        ))
    }
}

pub fn validate_plan(plan: &PhysicalPlan) -> Result<(), ValidationErrors> {
    validate_plan_with_limits(plan, PlanLimits::FROZEN)
}

/// Validates against caller-supplied bounds.
///
/// A refused plan is a user-visible refusal, so the bounds that produce one
/// are a deployment decision rather than a property of this build.
pub fn validate_plan_with_limits(
    plan: &PhysicalPlan,
    limits: PlanLimits,
) -> Result<(), ValidationErrors> {
    validate_plan_with_limits_core(plan, limits, &mut PlainPlanWaitValidation)
}

/// Only the shared wait stage is cooperative inside the legacy structural
/// traversal. The original caller retains its control and work scope.
pub(crate) fn validate_plan_with_limits_in(
    plan: &PhysicalPlan,
    limits: PlanLimits,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), crate::PlanConstructionError> {
    validate_plan_with_limits_core(plan, limits, &mut CallerPlanWaitValidation { work })
}

trait PlanWaitValidation {
    type Error: From<ValidationErrors>;
    fn validate(
        &mut self,
        plan: &PhysicalPlan,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error>;
}

// The public plain validator has no caller control. This closed adapter is
// never selected by caller-observed publication and grants no resources.
struct PlainValidationControl;
impl novarocks_type_contract::PureCompileControl for PlainValidationControl {
    fn checkpoint(
        &self,
        _phase: novarocks_type_contract::CompilePhase,
        _work: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
struct PlainPlanWaitValidation;
impl PlanWaitValidation for PlainPlanWaitValidation {
    type Error = ValidationErrors;
    fn validate(
        &mut self,
        plan: &PhysicalPlan,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            &PlainValidationControl,
            novarocks_type_contract::CompilePhase::Validate,
        )
        .expect("plain validation control cannot refuse");
        validate_runtime_filter_wait_graph_in(plan, errors, &mut work)
            .expect("plain validation control cannot refuse");
        work.finish()
            .expect("plain validation control cannot refuse");
        Ok(())
    }
}

struct CallerPlanWaitValidation<'work, 'control> {
    work: &'work mut novarocks_type_contract::CompileCheckpoints<'control>,
}
impl PlanWaitValidation for CallerPlanWaitValidation<'_, '_> {
    type Error = crate::PlanConstructionError;
    fn validate(
        &mut self,
        plan: &PhysicalPlan,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        validate_runtime_filter_wait_graph_in(plan, errors, self.work)
            .map_err(crate::ConstantReferenceError::from)?;
        Ok(())
    }
}

fn validate_plan_with_limits_core<V: PlanWaitValidation>(
    plan: &PhysicalPlan,
    limits: PlanLimits,
    waits: &mut V,
) -> Result<(), V::Error> {
    let mut errors = ValidationContext::with_limits(limits);
    validate_plan_resources(plan, &mut errors);
    if !errors.is_empty() {
        return Err(ValidationErrors::from_collector(errors).into());
    }
    if plan.required().plan_contract_revision != PLAN_CONTRACT_REVISION {
        errors.push(ValidationError::new(
            "required.plan_contract_revision",
            format!(
                "expected {PLAN_CONTRACT_REVISION}, got {}",
                plan.required().plan_contract_revision
            ),
        ));
    }
    bounded_count(
        &mut errors,
        "fragments",
        plan.fragments().len(),
        limits.plan_fragments,
    );
    bounded_count(&mut errors, "edges", plan.edges().len(), limits.plan_edges);
    bounded_count(
        &mut errors,
        "runtime_filters",
        plan.runtime_filters().len(),
        limits.plan_runtime_filters,
    );
    if plan.fragments().is_empty() {
        errors.push(ValidationError::new("fragments", "plan has no fragments"));
    }

    for fragment in plan.fragments().values() {
        validate_fragment_into(fragment, &mut errors);
        // The complete-plan table is the sole intrinsic source. Fragment-only
        // validation checks reference shape; publication resolves exact values.
        for (id, expression) in fragment.expressions().iter() {
            for reference in expression.kind.intrinsic_parameter_references() {
                if let Err(error) = plan.parameters().require(*reference) {
                    errors.push(ValidationError::new(
                        format!(
                            "fragments[{}].expressions[{}].parameters",
                            fragment.id().get(),
                            id.get()
                        ),
                        error.to_string(),
                    ));
                }
            }
            if errors.is_saturated() {
                break;
            }
        }
        if errors.is_saturated() {
            errors.mark_truncated();
            return Err(ValidationErrors::from_collector(errors).into());
        }
    }
    let mut root_port_indexes = BTreeMap::new();
    for edge in plan.edges().values() {
        validate_edge(plan, edge, &mut root_port_indexes, &mut errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Err(ValidationErrors::from_collector(errors).into());
        }
    }
    macro_rules! run_validation_stage {
        ($stage:expr) => {{
            $stage;
            if errors.is_saturated() {
                errors.mark_truncated();
                return Err(ValidationErrors::from_collector(errors).into());
            }
        }};
    }
    run_validation_stage!(validate_fragment_graph(plan, &mut errors));
    run_validation_stage!(validate_sinks(plan, &mut errors));
    run_validation_stage!(validate_writer_flows(plan, &mut errors));
    run_validation_stage!(validate_result(plan, &mut errors));
    run_validation_stage!(validate_runtime_filters(plan, &mut errors));
    if errors.is_empty() {
        run_validation_stage!(waits.validate(plan, &mut errors)?);
    }
    run_validation_stage!(validate_annotations(plan, &mut errors));
    run_validation_stage!(validate_cross_fragment_value_origins(plan, &mut errors));
    run_validation_stage!(validate_provider_read_occurrences(plan, &mut errors));
    run_validation_stage!(validate_partition_identities(plan, &mut errors));
    run_validation_stage!(validate_aggregate_sequences(plan, &mut errors));
    run_validation_stage!(validate_topn_reductions(plan, &mut errors));

    if errors.is_empty() {
        let Some(derivation) = FragmentCutDerivation::new(plan, errors.limits()) else {
            errors.push(ValidationError::new(
                "fragments.cuts",
                "cannot index the complete fragment cut graph",
            ));
            return Err(ValidationErrors::from_collector(errors).into());
        };
        let mut total_cut_items = 0usize;
        let mut total_cut_bytes = 0usize;
        for fragment in plan.fragments().values() {
            let Some(usage) =
                preflight_fragment_cut_resources(plan, fragment.id(), &derivation, &mut errors)
            else {
                errors.push(ValidationError::new(
                    format!("fragments[{}].cuts", fragment.id().get()),
                    "cannot preflight the complete fragment cuts",
                ));
                return Err(ValidationErrors::from_collector(errors).into());
            };
            total_cut_items = total_cut_items.saturating_add(usage.items);
            total_cut_bytes = total_cut_bytes.saturating_add(usage.bytes);
            if total_cut_items > MAX_PLAN_DERIVED_CUT_ITEMS
                || total_cut_bytes > MAX_PLAN_DERIVED_CUT_BYTES
            {
                errors.push(ValidationError::resource_limit(
                    "fragments.cuts.resources",
                    "aggregate derived fragment cuts exceed the plan publication budget",
                ));
            }
            if !errors.is_empty() {
                return Err(ValidationErrors::from_collector(errors).into());
            }
            let Some(cuts) = derivation.derive_preflighted(plan, fragment.id()) else {
                errors.push(ValidationError::new(
                    format!("fragments[{}].cuts", fragment.id().get()),
                    "cannot derive complete fragment cuts",
                ));
                return Err(ValidationErrors::from_collector(errors).into());
            };
            validate_fragment_cuts_into(fragment, &cuts, &mut errors);
            validate_fragment_partition_identities(fragment, &cuts, &mut errors);
            if !errors.is_empty() {
                return Err(ValidationErrors::from_collector(errors).into());
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors).into())
    }
}

pub(crate) fn validate_provider_read_occurrences(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
) {
    let mut occurrences = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Scan { occurrence, .. } = &node.kind else {
                continue;
            };
            if let Some((first_fragment, first_node)) =
                occurrences.insert(*occurrence, (fragment.id(), node.id))
            {
                errors.push(ValidationError::new(
                    format!(
                        "fragments[{}].nodes[{}].occurrence",
                        fragment.id().get(),
                        node.id.get()
                    ),
                    format!(
                        "provider read occurrence {} is already owned by fragment {} node {}",
                        occurrence.get(),
                        first_fragment.get(),
                        first_node.get()
                    ),
                ));
            }
        }
    }
}

pub(crate) fn validate_fragment_into(fragment: &Fragment, errors: &mut ValidationContext) {
    let previous_errors = errors.len();
    validate_fragment_resources(fragment, errors);
    if errors.len() != previous_errors {
        return;
    }
    validate_fragment_structure_into(fragment, errors);
}

// The original structure body chooses only owned scratch operations. All
// reference, expression, node and error-order laws remain in that one body.
trait FragmentStructureScratch {
    type Error;
    fn indexes(&mut self, fragment: &Fragment) -> Result<FragmentValidationIndexes, Self::Error>;
    fn node_graph(
        &mut self,
        fragment: &Fragment,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error>;
}
struct PlainFragmentScratch;
impl FragmentStructureScratch for PlainFragmentScratch {
    type Error = std::convert::Infallible;
    fn indexes(&mut self, fragment: &Fragment) -> Result<FragmentValidationIndexes, Self::Error> {
        Ok(FragmentValidationIndexes::new(fragment))
    }
    fn node_graph(
        &mut self,
        fragment: &Fragment,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        validate_node_graph(fragment, errors);
        Ok(())
    }
}
struct CallerFragmentScratch<'a, 'control> {
    resources: &'a mut novarocks_type_contract::ControlResourceCounter,
    admit: &'a mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), novarocks_type_contract::CompileControlError>,
    work: &'a mut novarocks_type_contract::CompileCheckpoints<'control>,
}
impl FragmentStructureScratch for CallerFragmentScratch<'_, '_> {
    type Error = novarocks_type_contract::ControlResourceError;
    fn indexes(&mut self, fragment: &Fragment) -> Result<FragmentValidationIndexes, Self::Error> {
        FragmentValidationIndexes::new_in(fragment, self.resources, self.admit, self.work)
    }
    fn node_graph(
        &mut self,
        fragment: &Fragment,
        errors: &mut ValidationContext,
    ) -> Result<(), Self::Error> {
        graph::validate_node_graph_in(fragment, errors, self.resources, self.admit, self.work)
    }
}

pub(crate) fn validate_fragment_structure_into(
    fragment: &Fragment,
    errors: &mut ValidationContext,
) {
    validate_fragment_structure_into_core(fragment, errors, &mut PlainFragmentScratch)
        .unwrap_or_else(|never| match never {});
}

pub(crate) fn validate_fragment_structure_into_in(
    fragment: &Fragment,
    errors: &mut ValidationContext,
    resources: &mut novarocks_type_contract::ControlResourceCounter,
    admit: &mut dyn FnMut(
        &novarocks_type_contract::ControlOwnedResourceFacts,
    ) -> Result<(), novarocks_type_contract::CompileControlError>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<(), novarocks_type_contract::ControlResourceError> {
    validate_fragment_structure_into_core(
        fragment,
        errors,
        &mut CallerFragmentScratch {
            resources,
            admit,
            work,
        },
    )
}

fn validate_fragment_structure_into_core<S: FragmentStructureScratch>(
    fragment: &Fragment,
    errors: &mut ValidationContext,
    scratch: &mut S,
) -> Result<(), S::Error> {
    let prefix = format!("fragments[{}]", fragment.id().get());
    bounded_count(
        errors,
        &format!("{prefix}.nodes"),
        fragment.nodes().len(),
        errors.limits().fragment_nodes,
    );
    bounded_count(
        errors,
        &format!("{prefix}.values"),
        fragment.values().len(),
        errors.limits().fragment_values,
    );
    bounded_count(
        errors,
        &format!("{prefix}.expressions"),
        fragment.expressions().len(),
        errors.limits().fragment_expressions,
    );
    if !fragment.nodes().contains_key(&fragment.root()) {
        errors.push(ValidationError::new(
            format!("{prefix}.root"),
            format!("node {} is not defined", fragment.root().get()),
        ));
    }
    let dop = fragment.dop_domain();
    match fragment.stage_placement() {
        crate::StagePlacementPolicy::FollowSources { sources } => {
            if sources.is_empty()
                || sources.windows(2).any(|pair| pair[0] >= pair[1])
                || sources.contains(&fragment.id())
            {
                errors.push(ValidationError::new(
                    format!("{prefix}.stage_placement"),
                    "placement sources must be nonempty, strictly ordered and exclude self",
                ));
            }
        }
        crate::StagePlacementPolicy::ScanWork => {
            if !fragment
                .nodes()
                .values()
                .any(|node| matches!(node.kind, crate::NodeKind::Scan { .. }))
            {
                errors.push(ValidationError::new(
                    format!("{prefix}.stage_placement"),
                    "scan-work placement requires an actual scan",
                ));
            }
        }
        crate::StagePlacementPolicy::Singleton | crate::StagePlacementPolicy::Spread { .. } => {}
    }
    if dop.min == 0 || dop.min > dop.max || dop.max > MAX_PIPELINE_DOP {
        errors.push(ValidationError::new(
            format!("{prefix}.dop_domain"),
            "DOP bounds must be non-zero, ordered and bounded",
        ));
    }
    if dop.requires_power_of_two
        && dop
            .min
            .checked_next_power_of_two()
            .is_none_or(|first| first > dop.max)
    {
        errors.push(ValidationError::new(
            format!("{prefix}.dop_domain"),
            "power-of-two DOP domain has no admissible member",
        ));
    }
    let mut aggregate_calls = BTreeMap::new();
    for node in fragment.nodes().values() {
        let Some((_, calls)) = node.kind.aggregate_contract() else {
            continue;
        };
        for call in calls {
            if aggregate_calls
                .insert(call.id, call.binding.phase)
                .is_some()
            {
                errors.push(ValidationError::new(
                    format!("{prefix}.aggregate_calls[{}]", call.id.get()),
                    "aggregate call identity must be unique within its fragment",
                ));
            }
            if errors.is_saturated() {
                errors.mark_truncated();
                return Ok(());
            }
        }
    }
    for value in fragment.values().values() {
        validate_value(fragment, value, &aggregate_calls, errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Ok(());
        }
    }
    let indexes = scratch.indexes(fragment)?;
    let window_roots = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Window(spec) => Some(spec.expressions.iter().map(|item| item.expression)),
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let operator_roots = fragment
        .nodes()
        .values()
        .flat_map(|node| {
            let mut roots = Vec::new();
            node.kind.expression_references(&mut roots);
            roots
        })
        .collect::<BTreeSet<_>>();
    let mut expression_parents: BTreeMap<ExprId, Vec<ExpressionParentReference>> = BTreeMap::new();
    for (_, parent) in fragment.expressions().iter() {
        let mut references = Vec::new();
        expression_parent_references(parent, &mut references);
        for (child, role) in references {
            expression_parents
                .entry(child)
                .or_default()
                .push(ExpressionParentReference {
                    parent: parent.id,
                    role,
                });
        }
    }
    for (_, expression) in fragment.expressions().iter() {
        validate_expression(
            fragment,
            expression,
            &indexes.visible_inputs,
            &window_roots,
            &operator_roots,
            expression_parents
                .get(&expression.id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            errors,
        );
        if errors.is_saturated() {
            errors.mark_truncated();
            return Ok(());
        }
    }
    validate_expression_acyclic(fragment, errors);
    validate_lambda_scope_acyclic(fragment, errors);
    validate_expression_reachability(fragment, errors);
    for node in fragment.nodes().values() {
        validate_node(fragment, node, &indexes, errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Ok(());
        }
    }
    scratch.node_graph(fragment, errors)?;
    validate_fragment_sink(fragment, errors);
    Ok(())
}

pub(crate) fn validate_annotations(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    validate_annotation_table(
        plan.annotations(),
        |subject| match subject {
            AnnotationSubject::Plan => true,
            AnnotationSubject::Fragment(fragment) => plan.fragments().contains_key(&fragment),
            AnnotationSubject::Node(fragment, node) => plan
                .fragments()
                .get(&fragment)
                .is_some_and(|fragment| fragment.nodes().contains_key(&node)),
            AnnotationSubject::Value(fragment, value) => plan
                .fragments()
                .get(&fragment)
                .is_some_and(|fragment| fragment.values().contains_key(&value)),
        },
        errors,
    );
}

pub(crate) fn validate_annotation_table(
    annotations: &[crate::PlanAnnotation],
    valid_subject: impl Fn(AnnotationSubject) -> bool,
    errors: &mut ValidationContext,
) {
    bounded_count(errors, "annotations", annotations.len(), MAX_ANNOTATIONS);
    let total_bytes = annotations.iter().fold(0_usize, |total, annotation| {
        total
            .saturating_add(annotation.key.len())
            .saturating_add(annotation.value.len())
    });
    if total_bytes > MAX_ANNOTATION_BYTES {
        errors.push(ValidationError::resource_limit(
            "annotations",
            format!("contains {total_bytes} bytes, exceeding {MAX_ANNOTATION_BYTES}"),
        ));
    }
    for (index, annotation) in annotations.iter().enumerate() {
        let valid = valid_subject(annotation.subject);
        // Four different faults read alike once they are one message, and an
        // annotation names a subject the reader has to go and find.
        let fault = if !valid {
            Some(format!(
                "names a subject this plan does not have: {:?}",
                annotation.subject
            ))
        } else if annotation.key.is_empty() {
            Some("has an empty key".to_string())
        } else if annotation.key.len() > MAX_ANNOTATION_KEY_BYTES {
            Some(format!(
                "key is {} bytes, exceeding {MAX_ANNOTATION_KEY_BYTES}",
                annotation.key.len()
            ))
        } else if annotation.value.len() > MAX_ANNOTATION_VALUE_BYTES {
            Some(format!(
                "value is {} bytes, exceeding {MAX_ANNOTATION_VALUE_BYTES}",
                annotation.value.len()
            ))
        } else {
            None
        };
        if let Some(fault) = fault {
            errors.push(ValidationError::new(
                format!("annotations[{index}]"),
                format!("annotation `{}` {fault}", annotation.key),
            ));
        }
    }
}

pub(crate) fn require_node(
    fragment: &Fragment,
    node: NodeId,
    path: &str,
    errors: &mut ValidationContext,
) {
    if !fragment.nodes().contains_key(&node) {
        errors.push(ValidationError::new(
            path,
            format!("node {} is not defined", node.get()),
        ));
    }
}

pub(crate) fn require_value(
    fragment: &Fragment,
    value: ValueId,
    path: &str,
    errors: &mut ValidationContext,
) {
    if !fragment.values().contains_key(&value) {
        errors.push(ValidationError::new(
            path,
            format!("value {} is not defined", value.get()),
        ));
    }
}

#[allow(dead_code)]
pub(crate) fn _assert_required_contract_is_copy(_: RequiredContracts) {}

#[allow(dead_code)]
pub(crate) fn _assert_maps_are_deterministic(_: BTreeMap<FragmentId, Fragment>) {}

pub use aggregate::{
    aggregate_bindings_match_observed, aggregate_topn_grouped_parent_matches_observed,
};

/// Frozen executable transport/projection identity, with root NULL admission
/// delegated to the association owner. Semantic overload identity is separate.
fn value_bindings_match_ignoring_root_nullability(
    left: &crate::ValueType,
    right: &crate::ValueType,
) -> bool {
    left.same_value_domain(right) && left.typed_binding().layout() == right.typed_binding().layout()
}

/// Borrow a canonical primitive requirement without allocating a fake source
/// FunctionValueType, semantic holder or layout projection for comparison.
fn value_binding_is_primitive(
    value: &crate::ValueType,
    expected: &arrow_schema::DataType,
    nullable: bool,
) -> bool {
    use arrow_schema::DataType;
    use novarocks_type_contract::ValueKind;
    value.nullable() == nullable
        && value.data_type() == expected
        && match value.value_type().kind() {
            ValueKind::Scalar(actual) => actual == expected,
            ValueKind::String => matches!(
                expected,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ),
            ValueKind::Binary => matches!(
                expected,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ),
            _ => false,
        }
}

fn value_has_plain_string_map_semantics(value: &crate::ValueType) -> bool {
    use novarocks_type_contract::ValueKind;
    matches!(value.value_type().kind(),ValueKind::Map{key,value}
        if matches!(key.kind(),ValueKind::String) && matches!(value.kind(),ValueKind::String))
}
