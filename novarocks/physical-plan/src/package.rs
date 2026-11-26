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

//! One carrier-neutral fragment input. Its constructor proves local structure;
//! exact installed function and provider capabilities are checked separately.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_connector_contract::{
    ConnectorWriteRecipeDraft, FrozenConnectorRead, WriteTargetOrdinal,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
    SemanticParameterError, SemanticParameterProjectionError, SemanticParameterRef,
    SemanticParameters,
};

use crate::{
    AnnotationSubject, Fragment, FragmentCuts, FragmentId, FrozenCallError, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPruningError, NodeId, PhysicalPlan, PlanAnnotation, PlanVersionId,
    ProviderReadOccurrenceId, RequiredContracts, ResultPort, ValidationErrors,
    derive_fragment_cuts,
};

/// An owned input for the same checked constructor on FE and BE. It contains
/// neither generated DTOs nor runtime handles. Sparse IDs remain map keys.
#[derive(Clone, Debug)]
pub struct FragmentPackageInput {
    pub constants: crate::ConstantPools,

    pub version: PlanVersionId,
    pub required: RequiredContracts,
    pub fragment: Fragment,
    /// Explicit derived-domain declarations. Structure never grants pruning
    /// authority; the FE still proves semantics and complete consumer coverage.
    pub pruning: FrozenFragmentPruning,
    pub cuts: FragmentCuts,
    pub result: Option<ResultPort>,
    pub parameters: SemanticParameters,
    /// Additional provider public facts paired with exact physical scan nodes.
    /// The recipe is a structurally checked draft, not a capability proof.
    pub scans: BTreeMap<NodeId, FrozenConnectorRead>,
    pub writes: BTreeMap<NodeId, ConnectorWriteRecipeDraft>,
    pub layouts: BTreeMap<NodeId, novarocks_connector_contract::ConnectorWriteLayoutRecipeDraft>,
    pub annotations: Box<[PlanAnnotation]>,
}

/// Explicit host-authored admission for this exact coexisting source.
/// PlanLimits are structural counts; the independent property ceilings admit
/// its occurrence projection. This is not a complete validator scratch model
/// or a MEM grant. Source bytes cover structural backing. System-relation batch data is
/// separately reported by SystemRelationResourceFacts to its original owner.
#[derive(Clone, Copy, Debug)]
pub struct FragmentPackageAdmission {
    pub plan_limits: crate::PlanLimits,
    pub source_retained_bytes: usize,
    pub property_projection_limits: crate::PropertyProofProjectionLimits,
}

#[derive(Clone)]
pub struct FragmentPackage(
    FragmentPackageInput,
    Option<novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>,
);
impl fmt::Debug for FragmentPackage {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_tuple("FragmentPackage").field(&self.0).finish()
    }
}

type PackageResourceAdmission<'a> = dyn FnMut(&novarocks_type_contract::ControlOwnedResourceFacts) -> Result<(), CompileControlError>
    + 'a;

#[derive(Clone, Copy)]
struct PackageValidationProfile {
    plan_limits: crate::PlanLimits,
    property_admission: Option<(usize, crate::PropertyProofProjectionLimits)>,
}

impl From<FragmentPackageAdmission> for PackageValidationProfile {
    fn from(admission: FragmentPackageAdmission) -> Self {
        Self {
            plan_limits: admission.plan_limits,
            property_admission: Some((
                admission.source_retained_bytes,
                admission.property_projection_limits,
            )),
        }
    }
}

impl PackageValidationProfile {
    fn input_derived(plan_limits: crate::PlanLimits) -> Self {
        Self {
            plan_limits,
            property_admission: None,
        }
    }
}

enum PackageValidation<'borrow, 'control, 'admit> {
    Plain(&'control dyn PureCompileControl),
    Caller {
        source_retained_bytes: usize,
        admit: &'borrow mut PackageResourceAdmission<'admit>,
        work: &'borrow mut CompileCheckpoints<'control>,
    },
}

impl FragmentPackage {
    /// Attach only positive loans from the same decoded Field namespace.
    /// No semantic plan facts, runtime capability or capacity grant changes.
    pub fn with_original_metadata_namespace(
        mut self,
        namespace: novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace,
    ) -> Self {
        self.1 = Some(namespace);
        self
    }
    pub fn original_metadata_namespace(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::MaterializedFieldNamespace>{
        self.1.as_ref()
    }
    pub fn try_new(
        input: FragmentPackageInput,
        admission: FragmentPackageAdmission,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentPackageError> {
        Self::try_new_observed(input, admission.into(), control)
    }

    /// Check the complete package from its original source and structural limits.
    /// The pure planning caller retains the original backing.
    pub fn try_new_input_derived(
        input: FragmentPackageInput,
        limits: crate::PlanLimits,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentPackageError> {
        Self::try_new_observed(
            input,
            PackageValidationProfile::input_derived(limits),
            control,
        )
    }

    fn try_new_observed(
        input: FragmentPackageInput,
        profile: PackageValidationProfile,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentPackageError> {
        control
            .checkpoint(CompilePhase::Validate, 0)
            .map_err(FragmentPackageError::Control)?;
        Self::try_new_core(input, profile, PackageValidation::Plain(control))
    }

    /// Run all original package laws in the caller's existing scope.
    /// The hook covers the existing call/control author and parameter scratch.
    /// Other structural, type and diagnostic scratch still requires its owner;
    /// this entry does not claim a complete allocation grant.
    pub fn try_new_in(
        input: FragmentPackageInput,
        admission: FragmentPackageAdmission,
        admit: &mut PackageResourceAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FragmentPackageError> {
        Self::try_new_core(
            input,
            admission.into(),
            PackageValidation::Caller {
                source_retained_bytes: admission.source_retained_bytes,
                admit,
                work,
            },
        )
    }

    /// Check the retained input in its caller's original scope.
    /// `source_retained_bytes` bounds the actual backing of the metadata tables
    /// visited by the opaque iterator. The caller retains this source and
    /// admits the local scratch requests through its existing hook.
    pub fn try_new_input_derived_in(
        input: FragmentPackageInput,
        limits: crate::PlanLimits,
        source_retained_bytes: usize,
        admit: &mut PackageResourceAdmission<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, FragmentPackageError> {
        Self::try_new_core(
            input,
            PackageValidationProfile::input_derived(limits),
            PackageValidation::Caller {
                source_retained_bytes,
                admit,
                work,
            },
        )
    }

    fn try_new_core(
        input: FragmentPackageInput,
        profile: PackageValidationProfile,
        mut observation: PackageValidation<'_, '_, '_>,
    ) -> Result<Self, FragmentPackageError> {
        let mut resources = novarocks_type_contract::ControlResourceCounter::default();
        match &mut observation {
            PackageValidation::Plain(control) => {
                crate::validation::validate_fragment_output_properties_observed_with_admission(
                    &input.fragment,
                    profile.plan_limits,
                    profile.property_admission,
                    *control,
                )
                .map_err(property_error)?;
            }
            PackageValidation::Caller { admit, work, .. } => {
                // One original call/control contribution emits cumulative
                // snapshots. Replace that contribution, never sum snapshots.
                let mut child = novarocks_type_contract::ControlOwnedResourceFacts::default();
                crate::validation::validate_fragment_output_properties_with_admission_in(
                    &input.fragment,
                    profile.plan_limits,
                    profile.property_admission,
                    &mut |next| {
                        child = *next;
                        admit(next)
                    },
                    work,
                )
                .map_err(property_error)?;
                resources.merge(child).map_err(package_resource_error)?;
            }
        }
        let requests = match &mut observation {
            PackageValidation::Plain(control) => input
                .fragment
                .call_requests()
                .validate_fragment(&input.fragment, *control),
            PackageValidation::Caller { work, .. } => input
                .fragment
                .call_requests()
                .validate_fragment_in(&input.fragment, work),
        };
        requests.map_err(|error| match error {
            crate::CallRequestError::Control(cause) => FragmentPackageError::Control(cause),
            other => FragmentPackageError::Requests(other),
        })?;
        let constants = match &mut observation {
            PackageValidation::Plain(control) => {
                crate::constants::validate_fragment_constants_observed(
                    &input.fragment,
                    &input.constants,
                    true,
                    profile.plan_limits,
                    *control,
                )
            }
            PackageValidation::Caller { work, .. } => {
                crate::constants::validate_fragment_constants_in(
                    &input.fragment,
                    &input.constants,
                    true,
                    profile.plan_limits,
                    work,
                )
            }
        };
        constants.map_err(|error| match error {
            crate::ConstantReferenceError::Control(cause) => FragmentPackageError::Control(cause),
            error => FragmentPackageError::Constant(error),
        })?;
        let call_items = match &mut observation {
            PackageValidation::Plain(control) => {
                input.fragment.calls().dynamic_items_observed(*control)
            }
            PackageValidation::Caller { work, .. } => input.fragment.calls().dynamic_items_in(work),
        }
        .map_err(|error| match error {
            FrozenCallError::Control(cause) => FragmentPackageError::Control(cause),
            error => FragmentPackageError::Calls(error),
        })?;
        let pruning_items = match &mut observation {
            PackageValidation::Plain(control) => input.pruning.dynamic_items_observed(*control),
            PackageValidation::Caller { work, .. } => input.pruning.dynamic_items_in(work),
        }
        .map_err(pruning_error)?;
        let counts = match &mut observation {
            PackageValidation::Plain(control) => {
                visit_fragment_parameter_references(&input.fragment, *control, |_| {})
                    .map_err(parameter_error)?
            }
            PackageValidation::Caller { admit, work, .. } => {
                parameter_walk_resources(&mut resources, &input.fragment, call_items, work)?;
                admit(&resources.facts()).map_err(FragmentPackageError::Control)?;
                visit_fragment_parameter_references_core(&input.fragment, true, work, |_| {})
                    .map_err(parameter_error)?
            }
        };
        let semantic_items = match &observation {
            PackageValidation::Plain(_) => call_items
                .saturating_add(pruning_items)
                .saturating_add(counts.intrinsic),
            PackageValidation::Caller { .. } => call_items
                .checked_add(pruning_items)
                .and_then(|n| n.checked_add(counts.intrinsic))
                .ok_or(FragmentPackageError::Control(
                    CompileControlError::ResourceExhausted,
                ))?,
        };
        match &mut observation {
            PackageValidation::Plain(control) => {
                let mut resource_work =
                    CompileCheckpoints::try_new(*control, CompilePhase::Validate)
                        .map_err(FragmentPackageError::Control)?;
                let result = crate::validation::validate_package(
                    &input,
                    profile.plan_limits,
                    semantic_items,
                    &mut resource_work,
                );
                if matches!(result, Err(FragmentPackageError::Control(_))) {
                    return result.map(|_| Self(input, None));
                }
                resource_work
                    .finish()
                    .map_err(FragmentPackageError::Control)?;
                result?;
            }
            PackageValidation::Caller {
                source_retained_bytes,
                admit,
                work,
            } => crate::validation::validate_package_in(
                &input,
                profile.plan_limits,
                semantic_items,
                *source_retained_bytes,
                &mut resources,
                *admit,
                work,
            )?,
        }
        // Preserve the second actual reference walk after the complete original
        // package profile, before immutable parameter subset publication.
        let closure = match &mut observation {
            PackageValidation::Plain(control) => {
                let references = fragment_parameter_references(&input.fragment, *control)
                    .map_err(parameter_error)?;
                input
                    .parameters
                    .project_observed(references, CompilePhase::Validate, *control)
                    .map_err(parameter_error)?
            }
            PackageValidation::Caller { admit, work, .. } => {
                parameter_walk_resources(&mut resources, &input.fragment, call_items, work)?;
                resources
                    .buffer::<SemanticParameterRef>(counts.total, 1)
                    .map_err(package_resource_error)?;
                admit(&resources.facts()).map_err(FragmentPackageError::Control)?;
                let mut references = Vec::new();
                references.try_reserve_exact(counts.total).map_err(|_| {
                    FragmentPackageError::Control(CompileControlError::ResourceExhausted)
                })?;
                visit_fragment_parameter_references_core(
                    &input.fragment,
                    true,
                    work,
                    |reference| references.push(reference),
                )
                .map_err(parameter_error)?;
                let mut tree = novarocks_type_contract::ControlOwnedResourceFacts::default();
                input.parameters.project_in::<FragmentPackageError>(
                    references,
                    &mut |visit| {
                        parameter_projection_resources(&mut resources, &mut tree, visit)?;
                        let mut combined =
                            novarocks_type_contract::ControlResourceCounter::default();
                        combined
                            .merge(resources.facts())
                            .map_err(package_resource_error)?;
                        combined.merge(tree).map_err(package_resource_error)?;
                        admit(&combined.facts()).map_err(FragmentPackageError::Control)
                    },
                    work,
                )?
            }
        };
        if closure.entries().len() != input.parameters.entries().len() {
            return Err(FragmentPackageError::UnusedParameters);
        }
        let package = Self(input, None);
        match &mut observation {
            PackageValidation::Plain(control) => {
                package.0.pruning.validate_package(&package, *control)
            }
            PackageValidation::Caller { work, .. } => {
                package.0.pruning.validate_package_in(&package, work)
            }
        }
        .map_err(pruning_error)?;
        Ok(package)
    }

    /// Derive and apply all output candidates before mandatory full admission.
    /// The derivation proof borrows the original source only and is dropped
    /// before its fields are moved. Roots and calls are rebuilt against the
    /// new immutable snapshot; no old proof is promoted to its authority.
    ///
    /// This changes output claims only. Definitions, invocation flow, exact
    /// binding selections, requirements, cuts and application facts retain
    /// their original authors. Their installed capabilities remain separately
    /// mandatory. Coexisting root/call copies, maps and delegated scratch must
    /// be caller-admitted; this is not a complete allocation model or MEM grant.
    pub fn try_new_with_derived_properties(
        input: FragmentPackageInput,
        admission: FragmentPackageAdmission,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentPackageError> {
        Self::try_new_derived(input, admission.into(), control)
    }

    /// Derive properties from the immutable input, then validate the rebuilt
    /// package using the original structural limits.
    pub fn try_new_with_input_derived_properties(
        input: FragmentPackageInput,
        limits: crate::PlanLimits,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentPackageError> {
        Self::try_new_derived(
            input,
            PackageValidationProfile::input_derived(limits),
            control,
        )
    }

    fn try_new_derived(
        mut input: FragmentPackageInput,
        profile: PackageValidationProfile,
        control: &dyn PureCompileControl,
    ) -> Result<Self, FragmentPackageError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
            .map_err(FragmentPackageError::Control)?;
        let result = (|| {
            work.flush().map_err(FragmentPackageError::Control)?;
            let (candidates, _) =
                crate::validation::derive_fragment_output_properties_observed_with_admission(
                    &input.fragment,
                    &input.cuts,
                    profile.plan_limits,
                    profile.property_admission,
                    control,
                )
                .map_err(property_error)?;
            work.step().map_err(FragmentPackageError::Control)?;
            work.flush().map_err(FragmentPackageError::Control)?;
            // No occurrence proof or other source loan survives the preceding
            // call. Consume one construction source; do not clone its nodes.
            let mut parts = input.fragment.into_parts();
            for (id, properties) in candidates {
                let node = parts
                    .nodes
                    .get_mut(&id)
                    .ok_or(FragmentPackageError::Calls(FrozenCallError::InvalidSite))?;
                node.output_properties = properties;
                work.step().map_err(FragmentPackageError::Control)?;
            }
            input.fragment = Fragment::from(parts);
            work.flush().map_err(FragmentPackageError::Control)?;

            // Units and relation declarations stay with the original Fragment.
            // Re-admit the new property snapshot; no duplicate root/call table
            // or regenerated invocation identity exists after this transform.
            Self::try_new_observed(input, profile, control)
        })();
        if matches!(&result, Err(FragmentPackageError::Control(_))) {
            return result;
        }
        work.finish().map_err(FragmentPackageError::Control)?;
        result
    }

    pub const fn version(&self) -> PlanVersionId {
        self.0.version
    }

    pub const fn required(&self) -> RequiredContracts {
        self.0.required
    }

    pub fn constants(&self) -> &crate::ConstantPools {
        &self.0.constants
    }

    pub const fn fragment(&self) -> &Fragment {
        &self.0.fragment
    }

    pub const fn calls(&self) -> &FrozenFragmentCalls {
        self.0.fragment.calls()
    }

    pub const fn pruning(&self) -> &FrozenFragmentPruning {
        &self.0.pruning
    }

    pub const fn cuts(&self) -> &FragmentCuts {
        &self.0.cuts
    }

    pub const fn result(&self) -> Option<&ResultPort> {
        self.0.result.as_ref()
    }

    pub const fn parameters(&self) -> &SemanticParameters {
        &self.0.parameters
    }

    pub fn scans(&self) -> &BTreeMap<NodeId, FrozenConnectorRead> {
        &self.0.scans
    }

    pub fn writes(&self) -> &BTreeMap<NodeId, ConnectorWriteRecipeDraft> {
        &self.0.writes
    }

    pub fn layouts(
        &self,
    ) -> &BTreeMap<NodeId, novarocks_connector_contract::ConnectorWriteLayoutRecipeDraft> {
        &self.0.layouts
    }

    pub fn annotations(&self) -> &[PlanAnnotation] {
        &self.0.annotations
    }

    pub fn into_input(self) -> FragmentPackageInput {
        self.0
    }
}

fn property_error(error: crate::FragmentPropertyError) -> FragmentPackageError {
    match error {
        crate::FragmentPropertyError::Control(cause) => FragmentPackageError::Control(cause),
        crate::FragmentPropertyError::Calls(cause) => call_error(cause),
        crate::FragmentPropertyError::Structure(cause) => FragmentPackageError::Structure(cause),
    }
}
fn call_error(error: FrozenCallError) -> FragmentPackageError {
    match error {
        FrozenCallError::Control(cause) => FragmentPackageError::Control(cause),
        error => FragmentPackageError::Calls(error),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentPackageError {
    ResourceSource(&'static str),
    Requests(crate::CallRequestError),
    Control(CompileControlError),
    Constant(crate::ConstantReferenceError),
    Structure(ValidationErrors),
    Calls(FrozenCallError),
    Pruning(FrozenPruningError),
    Parameter(SemanticParameterError),
    UnusedParameters,
}
impl fmt::Display for FragmentPackageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceSource(message) => f.write_str(message),
            Self::Requests(error) => error.fmt(f),
            Self::Control(error) => error.fmt(f),
            Self::Constant(error) => error.fmt(f),
            Self::Structure(error) => error.fmt(f),
            Self::Calls(error) => error.fmt(f),
            Self::Pruning(error) => error.fmt(f),
            Self::Parameter(error) => error.fmt(f),
            Self::UnusedParameters => {
                f.write_str("semantic parameter table contains unused definitions")
            }
        }
    }
}
impl std::error::Error for FragmentPackageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Requests(source) => Some(source),
            Self::Control(source) => Some(source),
            Self::Constant(source) => Some(source),
            Self::Structure(source) => Some(source),
            Self::Calls(source) => Some(source),
            Self::Pruning(source) => Some(source),
            Self::Parameter(source) => Some(source),
            _ => None,
        }
    }
}

fn parameter_error(error: SemanticParameterProjectionError) -> FragmentPackageError {
    match error {
        SemanticParameterProjectionError::Control(error) => FragmentPackageError::Control(error),
        SemanticParameterProjectionError::Parameter(error) => {
            FragmentPackageError::Parameter(error)
        }
    }
}

impl From<SemanticParameterProjectionError> for FragmentPackageError {
    fn from(error: SemanticParameterProjectionError) -> Self {
        parameter_error(error)
    }
}

pub(crate) fn package_resource_error(
    error: novarocks_type_contract::ControlResourceError,
) -> FragmentPackageError {
    match error {
        novarocks_type_contract::ControlResourceError::Control(cause) => {
            FragmentPackageError::Control(cause)
        }
        novarocks_type_contract::ControlResourceError::SourceModel(message) => {
            FragmentPackageError::ResourceSource(message)
        }
    }
}

fn parameter_walk_resources(
    resources: &mut novarocks_type_contract::ControlResourceCounter,
    fragment: &Fragment,
    relation_items: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentPackageError> {
    use novarocks_type_contract::{control_resource_add, control_resource_mul};
    let mut expressions = 0usize;
    let mut scalar_items = 0usize;
    let mut scalar_claims = 0usize;
    for node in fragment.nodes().values() {
        let unit = node.expressions.as_ref();
        work.step().map_err(FragmentPackageError::Control)?;
        let Some(unit) = unit else { continue };
        expressions = control_resource_add(expressions, unit.definitions().len())
            .map_err(package_resource_error)?;
        scalar_claims = control_resource_add(scalar_claims, unit.claims().len())
            .map_err(package_resource_error)?;
        scalar_items = control_resource_add(scalar_items, unit.claims().len())
            .map_err(package_resource_error)?;
        for claim in unit.claims().values() {
            scalar_items = control_resource_add(scalar_items, claim.effects.environment.len())
                .map_err(package_resource_error)?;
            work.step().map_err(FragmentPackageError::Control)?;
        }
    }
    let expression_work = control_resource_mul(expressions, 2).map_err(package_resource_error)?;
    let calls =
        control_resource_add(relation_items, scalar_items).map_err(package_resource_error)?;
    let source_nodes =
        control_resource_mul(fragment.nodes().len(), 2).map_err(package_resource_error)?;
    let total = control_resource_add(
        control_resource_add(calls, expression_work).map_err(package_resource_error)?,
        source_nodes,
    )
    .map_err(package_resource_error)?;
    let prepass = control_resource_add(fragment.nodes().len(), scalar_claims)
        .map_err(package_resource_error)?;
    resources
        .work(control_resource_add(total, prepass).map_err(package_resource_error)?)
        .map_err(package_resource_error)
}

fn parameter_projection_resources(
    resources: &mut novarocks_type_contract::ControlResourceCounter,
    tree: &mut novarocks_type_contract::ControlOwnedResourceFacts,
    visit: novarocks_type_contract::SemanticParameterProjectionVisit<'_>,
) -> Result<(), FragmentPackageError> {
    use novarocks_type_contract::{
        ControlResourceCounter, SemanticParameterId, SemanticParameterProjectionVisit,
        SemanticParameterValue,
    };
    match visit {
        SemanticParameterProjectionVisit::BeforeLookup {
            source_definition_count,
            output_definition_count,
            ..
        } => {
            resources
                .work(
                    ControlResourceCounter::lookup_work(source_definition_count)
                        .map_err(package_resource_error)?,
                )
                .map_err(package_resource_error)?;
            resources
                .work(
                    ControlResourceCounter::lookup_work(output_definition_count)
                        .map_err(package_resource_error)?,
                )
                .map_err(package_resource_error)?;
        }
        SemanticParameterProjectionVisit::CapturedValue {
            is_new,
            output_definition_count,
            ..
        } => {
            if is_new {
                let count =
                    novarocks_type_contract::control_resource_add(output_definition_count, 1)
                        .map_err(package_resource_error)?;
                let mut next_tree = ControlResourceCounter::default();
                next_tree
                    .tree::<SemanticParameterId, SemanticParameterValue>(count)
                    .map_err(package_resource_error)?;
                *tree = next_tree.facts();
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct ParameterReferenceCounts {
    total: usize,
    relation: usize,
    intrinsic: usize,
}

// Both the allocation-free counting pass and the admitted collection pass
// observe every actual definition, including definitions with no consumer.
// A filtering iterator must not hide an arbitrarily long source walk.
fn visit_fragment_parameter_references(
    fragment: &Fragment,
    control: &dyn PureCompileControl,
    visit: impl FnMut(SemanticParameterRef),
) -> Result<ParameterReferenceCounts, SemanticParameterProjectionError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(SemanticParameterProjectionError::Control)?;
    let counts = visit_fragment_parameter_references_core(fragment, false, &mut work, visit)?;
    work.finish()
        .map_err(SemanticParameterProjectionError::Control)?;
    Ok(counts)
}

fn visit_fragment_parameter_references_core(
    fragment: &Fragment,
    checked: bool,
    work: &mut CompileCheckpoints<'_>,
    mut visit: impl FnMut(SemanticParameterRef),
) -> Result<ParameterReferenceCounts, SemanticParameterProjectionError> {
    let mut counts = ParameterReferenceCounts::default();
    {
        let mut environment = |references: &[SemanticParameterRef],
                               relation: bool,
                               work: &mut CompileCheckpoints<'_>|
         -> Result<(), SemanticParameterProjectionError> {
            for reference in references {
                visit(*reference);
                counts.total = if checked {
                    counts
                        .total
                        .checked_add(1)
                        .ok_or(SemanticParameterProjectionError::Control(
                            CompileControlError::ResourceExhausted,
                        ))?
                } else {
                    counts.total.saturating_add(1)
                };
                if relation {
                    counts.relation = if checked {
                        counts.relation.checked_add(1).ok_or(
                            SemanticParameterProjectionError::Control(
                                CompileControlError::ResourceExhausted,
                            ),
                        )?
                    } else {
                        counts.relation.saturating_add(1)
                    };
                }
                work.step()
                    .map_err(SemanticParameterProjectionError::Control)?;
            }
            work.step()
                .map_err(SemanticParameterProjectionError::Control)
        };
        for call in fragment.calls().entries().values() {
            environment(&call.effects.environment, true, work)?;
        }
        for node in fragment.nodes().values() {
            let unit = node.expressions.as_ref();
            work.step()
                .map_err(SemanticParameterProjectionError::Control)?;
            if let Some(unit) = unit {
                for claim in unit.claims().values() {
                    environment(&claim.effects.environment, false, work)?;
                }
            }
        }
    }
    for node in fragment.nodes().values() {
        let unit = node.expressions.as_ref();
        work.step()
            .map_err(SemanticParameterProjectionError::Control)?;
        if let Some(unit) = unit {
            for definition in unit.definitions() {
                for reference in definition.kind.intrinsic_parameter_references() {
                    visit(*reference);
                    counts.total = if checked {
                        counts.total.checked_add(1).ok_or(
                            SemanticParameterProjectionError::Control(
                                CompileControlError::ResourceExhausted,
                            ),
                        )?
                    } else {
                        counts.total.saturating_add(1)
                    };
                    counts.intrinsic = if checked {
                        counts.intrinsic.checked_add(1).ok_or(
                            SemanticParameterProjectionError::Control(
                                CompileControlError::ResourceExhausted,
                            ),
                        )?
                    } else {
                        counts.intrinsic.saturating_add(1)
                    };
                }
                work.step()
                    .map_err(SemanticParameterProjectionError::Control)?;
            }
        }
    }
    Ok(counts)
}

// Callers must first admit the allocation-free count using the existing
// package resource profile. This second observed walk then copies references.
fn fragment_parameter_references(
    fragment: &Fragment,
    control: &dyn PureCompileControl,
) -> Result<Vec<SemanticParameterRef>, SemanticParameterProjectionError> {
    let mut references = Vec::new();
    visit_fragment_parameter_references(fragment, control, |reference| {
        references.push(reference);
    })?;
    Ok(references)
}

fn pruning_error(error: FrozenPruningError) -> FragmentPackageError {
    match error {
        FrozenPruningError::Control(error) => FragmentPackageError::Control(error),
        error => FragmentPackageError::Pruning(error),
    }
}

/// Extract from the immutable complete-plan authority. No second global plan
/// validation or executable peer graph is performed here. Boundary derivation
/// is indexed once for all fragments; each output uses the BE constructor.
pub fn extract_fragment_packages(
    plan: &PhysicalPlan,
    scans: &BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    pruning: &BTreeMap<FragmentId, FrozenFragmentPruning>,
    admissions: &BTreeMap<FragmentId, FragmentPackageAdmission>,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, FragmentPackageExtractionError> {
    control
        .checkpoint(CompilePhase::Validate, 0)
        .map_err(FragmentPackageExtractionError::Control)?;
    let mut admission_work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
        .map_err(FragmentPackageExtractionError::Control)?;
    for fragment in plan.fragments().values() {
        let present = admissions.contains_key(&fragment.id());
        admission_work
            .step()
            .map_err(FragmentPackageExtractionError::Control)?;
        if !present {
            admission_work
                .finish()
                .map_err(FragmentPackageExtractionError::Control)?;
            return Err(FragmentPackageExtractionError::MissingAdmission(
                fragment.id(),
            ));
        }
    }
    let exact_count = admissions.len() == plan.fragments().len();
    admission_work
        .step()
        .map_err(FragmentPackageExtractionError::Control)?;
    admission_work
        .finish()
        .map_err(FragmentPackageExtractionError::Control)?;
    if !exact_count {
        return Err(FragmentPackageExtractionError::UnusedAdmission);
    }
    extract_fragment_packages_observed(
        plan,
        scans,
        writes,
        pruning,
        |id| admissions.get(&id).copied().map(Into::into),
        control,
    )
}

/// Extract the checked local fragments from the original plan. The pure
/// source stays with its author. Freezing owns the subsequent requests.
pub fn extract_fragment_packages_input_derived(
    plan: &PhysicalPlan,
    scans: &BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    pruning: &BTreeMap<FragmentId, FrozenFragmentPruning>,
    limits: crate::PlanLimits,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, FragmentPackageExtractionError> {
    control
        .checkpoint(CompilePhase::Validate, 0)
        .map_err(FragmentPackageExtractionError::Control)?;
    let profile = PackageValidationProfile::input_derived(limits);
    extract_fragment_packages_observed(plan, scans, writes, pruning, |_| Some(profile), control)
}

fn extract_fragment_packages_observed(
    plan: &PhysicalPlan,
    scans: &BTreeMap<ProviderReadOccurrenceId, FrozenConnectorRead>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    pruning: &BTreeMap<FragmentId, FrozenFragmentPruning>,
    mut validation: impl FnMut(FragmentId) -> Option<PackageValidationProfile>,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, FragmentPackageExtractionError> {
    crate::constants::validate_plan_constants_observed(plan, control).map_err(
        |error| match error {
            crate::ConstantReferenceError::Control(error) => {
                FragmentPackageExtractionError::Control(error)
            }
            error => FragmentPackageExtractionError::Local(FragmentPackageError::Constant(error)),
        },
    )?;
    let mut cuts =
        derive_fragment_cuts(plan).ok_or(FragmentPackageExtractionError::BoundaryDerivation)?;
    // Display/statistics annotations for the whole plan remain FE-owned. Index
    // local subjects once so unrelated peer statistics cannot grow a package.
    let mut annotations_by_fragment: BTreeMap<FragmentId, Vec<PlanAnnotation>> = BTreeMap::new();
    for annotation in plan.annotations() {
        let id = match annotation.subject {
            AnnotationSubject::Plan => continue,
            AnnotationSubject::Fragment(id)
            | AnnotationSubject::Node(id, _)
            | AnnotationSubject::Value(id, _) => id,
        };
        annotations_by_fragment
            .entry(id)
            .or_default()
            .push(annotation.clone());
    }
    let mut outputs = BTreeMap::new();
    let mut consumed = std::collections::BTreeSet::new();
    let mut consumed_writes = std::collections::BTreeSet::new();
    let mut consumed_parameters = std::collections::BTreeSet::new();
    for fragment in plan.fragments().values() {
        let profile = validation(fragment.id()).ok_or(
            FragmentPackageExtractionError::MissingAdmission(fragment.id()),
        )?;
        let mut local_scans = BTreeMap::new();
        let mut local_writes = BTreeMap::new();
        for node in fragment.nodes().values() {
            if let crate::NodeKind::Scan { occurrence, .. } = &node.kind {
                let fact = scans
                    .get(occurrence)
                    .ok_or(FragmentPackageExtractionError::MissingScan(*occurrence))?;
                consumed.insert(*occurrence);
                local_scans.insert(node.id, fact.clone());
            }
        }
        for node in fragment.nodes().values() {
            if let crate::NodeKind::TableWriter { target } = &node.kind {
                let fact = writes.get(&target.write_target_ordinal).ok_or(
                    FragmentPackageExtractionError::MissingWrite(target.write_target_ordinal),
                )?;
                consumed_writes.insert(target.write_target_ordinal);
                local_writes.insert(node.id, fact.clone());
            }
        }
        let annotations = annotations_by_fragment
            .remove(&fragment.id())
            .unwrap_or_default()
            .into_boxed_slice();
        let counts = visit_fragment_parameter_references(fragment, control, |_| {})
            .map_err(extraction_parameter_error)?;
        // The complete plan has no ownership of the external frozen call
        // table. Admit this necessary subset of the eventual package profile
        // before copying its potentially repeated environment references.
        crate::validation::validate_fragment_parameter_resource_usage(
            fragment,
            profile.plan_limits,
            parameter_preallocation_items(fragment, counts),
        )
        .map_err(|error| {
            FragmentPackageExtractionError::Local(FragmentPackageError::Structure(error))
        })?;
        let references =
            fragment_parameter_references(fragment, control).map_err(extraction_parameter_error)?;
        let parameters = plan
            .parameters()
            .project_observed(references, CompilePhase::Validate, control)
            .map_err(extraction_parameter_error)?;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
            .map_err(FragmentPackageExtractionError::Control)?;
        for id in parameters.entries().keys() {
            consumed_parameters.insert(*id);
            work.step()
                .map_err(FragmentPackageExtractionError::Control)?;
        }
        work.finish()
            .map_err(FragmentPackageExtractionError::Control)?;
        let mut constant_work = CompileCheckpoints::try_new(control, CompilePhase::Validate)
            .map_err(FragmentPackageExtractionError::Control)?;
        let projected = crate::project_fragment_constants_observed(
            plan.constants(),
            fragment,
            &mut constant_work,
        );
        let projected = match projected {
            Err(crate::ConstantReferenceError::Control(error)) => {
                return Err(FragmentPackageExtractionError::Control(error));
            }
            other => {
                constant_work
                    .finish()
                    .map_err(FragmentPackageExtractionError::Control)?;
                other.map_err(|error| {
                    FragmentPackageExtractionError::Local(FragmentPackageError::Constant(error))
                })?
            }
        };
        let package = FragmentPackage::try_new_observed(
            FragmentPackageInput {
                constants: projected,

                version: plan.version(),
                required: plan.required(),
                fragment: fragment.clone(),
                pruning: pruning
                    .get(&fragment.id())
                    .ok_or(FragmentPackageExtractionError::MissingPruning(
                        fragment.id(),
                    ))?
                    .clone(),
                cuts: cuts
                    .remove(&fragment.id())
                    .ok_or(FragmentPackageExtractionError::BoundaryDerivation)?,
                result: plan
                    .result_port()
                    .filter(|result| result.fragment == fragment.id())
                    .cloned(),
                parameters,
                scans: local_scans,
                writes: local_writes,
                layouts: plan
                    .write_layouts()
                    .range((fragment.id(), NodeId::new(0))..=(fragment.id(), NodeId::new(u32::MAX)))
                    .map(|((_, node), recipe)| (*node, recipe.clone()))
                    .collect(),
                annotations,
            },
            profile,
            control,
        )
        .map_err(|error| match error {
            FragmentPackageError::Control(error) => FragmentPackageExtractionError::Control(error),
            error => FragmentPackageExtractionError::Local(error),
        })?;
        outputs.insert(fragment.id(), package);
    }
    if consumed_parameters.len() != plan.parameters().entries().len() {
        return Err(FragmentPackageExtractionError::UnusedParameters);
    }
    if consumed.len() != scans.len() {
        return Err(FragmentPackageExtractionError::UnusedScan);
    }
    if consumed_writes.len() != writes.len() {
        return Err(FragmentPackageExtractionError::UnusedWrite);
    }
    if pruning.len() != outputs.len() || pruning.keys().any(|id| !outputs.contains_key(id)) {
        return Err(FragmentPackageExtractionError::UnusedPruning);
    }
    Ok(outputs)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentPackageExtractionError {
    BoundaryDerivation,
    MissingScan(ProviderReadOccurrenceId),
    UnusedScan,
    MissingWrite(WriteTargetOrdinal),
    UnusedWrite,
    Local(FragmentPackageError),
    Parameter(SemanticParameterError),
    Control(CompileControlError),
    MissingPruning(FragmentId),
    UnusedPruning,
    MissingAdmission(FragmentId),
    UnusedAdmission,
    UnusedParameters,
}

impl fmt::Display for FragmentPackageExtractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BoundaryDerivation => f.write_str("complete plan boundary derivation failed"),
            Self::MissingScan(id) => write!(
                f,
                "frozen scan facts are missing for occurrence {}",
                id.get()
            ),
            Self::UnusedScan => f.write_str("frozen scan facts contain an unused occurrence"),
            Self::MissingWrite(id) => {
                write!(f, "frozen write facts are missing for target {}", id.get())
            }
            Self::UnusedWrite => f.write_str("frozen write facts contain an unused target"),
            Self::Local(errors) => errors.fmt(f),
            Self::Parameter(error) => error.fmt(f),
            Self::Control(error) => error.fmt(f),
            Self::MissingPruning(id) => write!(
                f,
                "frozen pruning declarations are missing for fragment {}",
                id.get()
            ),
            Self::UnusedPruning => {
                f.write_str("frozen pruning declarations name an unused fragment")
            }
            Self::MissingAdmission(id) => {
                write!(f, "resource admission is missing for fragment {}", id.get())
            }
            Self::UnusedAdmission => f.write_str("resource admission names an unused fragment"),
            Self::UnusedParameters => {
                f.write_str("complete plan contains unused semantic parameters")
            }
        }
    }
}

impl std::error::Error for FragmentPackageExtractionError {}

fn extraction_parameter_error(
    error: SemanticParameterProjectionError,
) -> FragmentPackageExtractionError {
    match error {
        SemanticParameterProjectionError::Control(error) => {
            FragmentPackageExtractionError::Control(error)
        }
        SemanticParameterProjectionError::Parameter(error) => {
            FragmentPackageExtractionError::Parameter(error)
        }
    }
}

fn parameter_preallocation_items(fragment: &Fragment, counts: ParameterReferenceCounts) -> usize {
    fragment
        .calls()
        .entries()
        .len()
        .saturating_add(counts.relation)
        .saturating_add(counts.intrinsic)
}

#[cfg(test)]
mod parameter_preallocation_review_tests {
    use super::*;
    use crate::*;
    use novarocks_expression_contract::{
        BoundFunction, DefinitionControl, ExprKind, ExpressionCallClaim, RootScope,
    };
    use novarocks_type_contract::*;
    use std::sync::Arc;

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    #[test]
    fn extraction_additional_items_do_not_count_unit_owned_environment_again() {
        let reference = SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        };
        let mut builder = FragmentBuilder::new(FragmentId::new(991));
        let node = builder.reserve_node_id().unwrap();
        let ty = ValueType::new(arrow_schema::DataType::Int64, false);
        let draft = builder.unit(node).unwrap();
        let definition = draft
            .add(
                None,
                ty.clone(),
                ExprKind::FunctionCall {
                    function: BoundFunction::from_exact_signature(
                        FunctionId::try_new("fixture/preallocation/timezone-length").unwrap(),
                        FunctionOverloadId::try_new("fixture/preallocation/zero-to-i64").unwrap(),
                        FunctionKind::Scalar,
                        TypedFactSlice::from_pure_boxed(Box::default()),
                        ty.clone(),
                    ),
                    args: Box::default(),
                },
            )
            .unwrap();
        draft
            .definition_control(
                definition,
                DefinitionControl {
                    shape: ControlShape::Eager,
                    temporal_sources: None,
                },
            )
            .unwrap();
        draft
            .request(
                definition,
                PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type: None,
                },
            )
            .unwrap();
        let root = draft
            .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        let value = builder
            .add_value(
                ty,
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node_unchecked(PhysicalNode {
                expressions: None,
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([root])]),
                },
            })
            .unwrap();
        let draft = builder.take_unit_builder(node).unwrap();
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
        let sealed = draft.seal(vec![], &mut work).unwrap();
        let invocation = sealed.uses()[&sealed.root_use_ids()[0]].context;
        let claims = BTreeMap::from([(
            invocation.use_id,
            ExpressionCallClaim {
                context: invocation,
                effects: CallEffects {
                    value_stability: FunctionVolatility::Stable,
                    own_row_error: FunctionIntrinsicRowError::NoRowError,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    null_behavior: FunctionNullBehavior::CalledOnNull,
                    argument_control: ArgumentControl::Eager,
                    instance_state: FunctionInstanceState::None,
                    observable_effects: ObservableEffects::NONE,
                    environment: Box::from([reference]),
                    proof_scope: CallProofScope::Region(invocation.region),
                },
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                temporal_source: None,
            },
        )]);
        let unit = sealed.finish(claims, &mut work).unwrap();
        work.finish().unwrap();
        builder.install_unit(node, Arc::new(unit)).unwrap();
        let fragment = builder
            .finish_definition(
                node,
                FragmentSink::Noop,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let parameters = SemanticParameters::try_new([(
            reference.id,
            SemanticParameterValue::TimeZone(
                novarocks_type_contract::TimeZoneKey::parse("UTC").unwrap(),
            ),
        )])
        .unwrap();
        let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &Control).unwrap();
        let admission = FragmentPackageAdmission {
            plan_limits: PlanLimits::FROZEN,
            source_retained_bytes: 64 * 1024 * 1024,
            property_projection_limits: PropertyProofProjectionLimits {
                max_request_bytes: 16 * 1024 * 1024,
                max_coexisting_bytes: 256 * 1024 * 1024,
                max_projection_work: 16 * 1024 * 1024,
            },
        };
        let package = FragmentPackage::try_new(
            FragmentPackageInput {
                constants: ConstantPools::empty(),

                version: PlanVersionId::try_new([29; 16]).unwrap(),
                required: RequiredContracts::default(),
                fragment: fragment.clone(),
                pruning: pruning.clone(),
                cuts: FragmentCuts::default(),
                result: None,
                parameters: parameters.clone(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                layouts: BTreeMap::new(),
                annotations: Box::default(),
            },
            admission,
            &Control,
        )
        .unwrap();
        let mut plan = PlanBuilder::new(package.version()).with_semantic_parameters(parameters);
        plan.add_fragment(fragment.clone()).unwrap();
        let plan = plan.finish_observed(&Control).unwrap();
        let extracted = extract_fragment_packages(
            &plan,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::from([(fragment.id(), pruning)]),
            &BTreeMap::from([(fragment.id(), admission)]),
            &Control,
        )
        .unwrap();
        assert_eq!(extracted[&fragment.id()].fragment(), package.fragment());
        let counts = visit_fragment_parameter_references(&fragment, &Control, |_| {}).unwrap();
        assert_eq!(counts.total, 1);
        assert_eq!(counts.intrinsic, 0);
        assert!(fragment.calls().entries().is_empty());
        assert_eq!(
            fragment
                .unit(node)
                .unwrap()
                .claims()
                .values()
                .map(|claim| claim.effects.environment.len())
                .sum::<usize>(),
            1
        );
        // The full source already owns this one scalar environment reference.
        // No external relation/intrinsic item remains to add to that source.
        // Red on the extracted current production expression: left=1/right=0.
        assert_eq!(parameter_preallocation_items(&fragment, counts), 0);
    }
}
