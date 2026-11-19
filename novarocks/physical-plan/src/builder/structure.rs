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
use crate::validation::{
    ValidationContext, bounded_count, validate_fragment_construction_after_admission,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentStructureError {
    Control(CompileControlError),
    Structure(ValidationErrors),
}
impl From<CompileControlError> for FragmentStructureError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValidationErrors> for FragmentStructureError {
    fn from(error: ValidationErrors) -> Self {
        Self::Structure(error)
    }
}
impl fmt::Display for FragmentStructureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Structure(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for FragmentStructureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Structure(source) => Some(source),
        }
    }
}

impl FragmentBuilder {
    /// Finish an unpublished definition's resource and structural stage.
    ///
    /// The caller then authors actual roots, control and complete call facts
    /// for this same fragment before proving output properties. Final package
    /// publication still runs the complete validator; this entry cannot turn
    /// pending effects, predicate guarantees or properties into proof.
    ///
    /// Source ownership and the structural validator's internal allocations
    /// remain the caller's admission responsibility. This port observes their
    /// opaque boundary, not internal cooperative work or a MEM allocation
    /// grant. Counts precede the new runtime-filter array request; the request
    /// is fallible and its actual refusal retains the first control cause.
    pub fn finish_structure(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop_domain: PipelineDopDomain,
        limits: crate::PlanLimits,
        control: &dyn PureCompileControl,
        stage_placement: crate::StagePlacementPolicy,
    ) -> Result<Fragment, FragmentStructureError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = (|| {
            self.ensure_units_finished()?;
            let mut errors = ValidationContext::for_construction(limits);
            admit_structure_counts(
                self.nodes.len(),
                self.values.len(),
                self.nodes
                    .values()
                    .filter_map(|n| n.expressions.as_ref())
                    .map(|u| u.definitions().len())
                    .sum(),
                self.runtime_filters.len(),
                limits,
                &mut errors,
                &mut work,
            )?;
            if !errors.is_empty() {
                return Err(FragmentStructureError::Structure(
                    ValidationErrors::from_collector(errors),
                ));
            }
            let (parts, runtime_filters) =
                self.into_fragment_parts(root, sink, dop_domain, stage_placement);
            let source = Fragment::from(parts);
            // Account the exact source before the new reference array exists.
            // The pending set contributes its actual reference count once;
            // the temporary construction input owns no replacement array.
            work.flush()?;
            let mut usage = crate::resource::CutResourcePreflight::new();
            usage.add_fragment(&source, &mut errors);
            usage.add_items(runtime_filters.len());
            usage.validate("fragment.structure.resources", &mut errors);
            work.step()?;
            work.flush()?;
            if !errors.is_empty() {
                return Err(FragmentStructureError::Structure(
                    ValidationErrors::from_collector(errors),
                ));
            }
            let mut parts = source.into_parts();
            let mut references = Vec::new();
            work.flush()?;
            if references.try_reserve_exact(runtime_filters.len()).is_err() {
                return Err(FragmentStructureError::Control(
                    CompileControlError::ResourceExhausted,
                ));
            }
            work.flush()?;
            for reference in runtime_filters {
                work.step()?;
                references.push(reference);
            }
            work.flush()?;
            // This stock conversion can shrink allocator surplus. Its opaque
            // boundary is observed; formal allocation accounting is separate.
            parts.runtime_filters = references.into_boxed_slice();
            work.flush()?;
            let fragment = Fragment::from(parts);
            let validation = validate_fragment_construction_after_admission(&fragment, limits);
            work.step()?;
            validation?;
            Ok(fragment)
        })();
        if matches!(result, Err(FragmentStructureError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
}

/// The same count gate for mutable construction and owned receiving inputs.
pub(crate) fn admit_structure_counts(
    nodes: usize,
    values: usize,
    expressions: usize,
    runtime_filters: usize,
    limits: crate::PlanLimits,
    errors: &mut ValidationContext,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentStructureError> {
    for (path, actual, limit) in [
        ("nodes", nodes, limits.fragment_nodes),
        ("values", values, limits.fragment_values),
        ("expressions", expressions, limits.fragment_expressions),
        (
            "runtime_filters",
            runtime_filters,
            limits.plan_runtime_filters,
        ),
    ] {
        work.step()?;
        bounded_count(errors, path, actual, limit);
    }
    Ok(())
}
