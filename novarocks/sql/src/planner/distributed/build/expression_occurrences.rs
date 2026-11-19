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

//! Same-emission scalar control facts and actual relational call identities.
//! Units own scalar flow derivation; relations never allocate scalar uses or regions.
use super::contract_lowering::SqlUnitSourceLoan;
use super::lowered_draft::SqlSourceJournalError;
use super::physical_temporal_sources::SourceError;
use crate::compiler::SqlFunctionCatalog;
use novarocks_expression_contract::{DefinitionControl, ExprId, ExprKind};
use novarocks_functions::{FunctionSpecializationFailure, PureKernelAbi};
use novarocks_physical_plan::{
    Fragment, FrozenCallError, NodeKind, PhysicalCallBinding, PhysicalCallSite, ValueDef,
    visit_relational_calls_observed,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    MAX_CONTROL_USE_REFERENCES, PureCompileControl, RelationCallId, RelationEffectContext,
};

#[derive(Debug)]
pub(crate) enum ExpressionOccurrenceError {
    Control(CompileControlError),
    Function(FunctionSpecializationFailure),
    MissingDefinition(ExprId),
    InvalidFunctionControl(ExprId),
    TooManyItems,
    Calls(FrozenCallError),
    InvalidRelationalControl(PhysicalCallSite),
    TemporalSource(&'static str),
    Journal(SqlSourceJournalError),
}
impl From<CompileControlError> for ExpressionOccurrenceError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<FrozenCallError> for ExpressionOccurrenceError {
    fn from(error: FrozenCallError) -> Self {
        match error {
            FrozenCallError::Control(cause) => Self::Control(cause),
            error => Self::Calls(error),
        }
    }
}
impl From<SqlSourceJournalError> for ExpressionOccurrenceError {
    fn from(error: SqlSourceJournalError) -> Self {
        match error {
            SqlSourceJournalError::Control(cause) => Self::Control(cause),
            error => Self::Journal(error),
        }
    }
}
impl From<SourceError> for ExpressionOccurrenceError {
    fn from(error: SourceError) -> Self {
        match error {
            SourceError::Control(cause) => Self::Control(cause),
            SourceError::Invalid(message) => Self::TemporalSource(message),
        }
    }
}
impl ExpressionOccurrenceError {
    pub(super) fn function(error: FunctionSpecializationFailure) -> Self {
        match error {
            FunctionSpecializationFailure::Control(error) => Self::Control(error),
            FunctionSpecializationFailure::Binding(
                novarocks_functions::FunctionBindingError::Control(error),
            ) => Self::Control(error),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::CompileControl(error),
            ) => Self::Control(error),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::Cancelled,
            ) => Self::Control(CompileControlError::Cancelled),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::DeadlineExceeded,
            ) => Self::Control(CompileControlError::DeadlineExceeded),
            FunctionSpecializationFailure::Kernel(
                novarocks_functions::KernelFailure::ResourceExhausted,
            ) => Self::Control(CompileControlError::ResourceExhausted),
            error => Self::Function(error),
        }
    }
}

/// Select original controls before sealing. These records are consumed by the
/// unit builder, which alone expands roots into ordered uses and guards. The
/// caller admits the actual temporary records; this loan grants no capacity.
pub(crate) fn author_original_unit_controls_observed(
    source: &SqlUnitSourceLoan<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<(ExprId, DefinitionControl)>, ExpressionOccurrenceError> {
    let definitions = source.definitions();
    let functions = source.functions();
    let mut controls = Vec::new();
    for ordinal in 0..definitions.len() {
        let id = ExprId::new(
            u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?,
        );
        let definition = definitions
            .definition(id)
            .ok_or(ExpressionOccurrenceError::MissingDefinition(id))?;
        work.step()?;
        let ExprKind::FunctionCall { function, args } = &definition.kind else {
            continue;
        };
        let original = source.checked_expression_source_observed(definition, work)?;
        work.flush()?;
        let declaration = functions
            .pure_overload_declaration_observed(
                &function.function_id,
                function.kind,
                &function.overload,
                work.control(),
            )
            .map_err(ExpressionOccurrenceError::function)?;
        if function.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
            work.flush()?;
            novarocks_expression_contract::regexp_count_pattern_source_from_lookup_observed(
                |id| definitions.definition(id),
                args,
                work,
            )?;
        }
        let selected_control =
            if declaration.implementation().abi == PureKernelAbi::ScalarInvocationV1 {
                let canonical = original.canonical_operational().ok_or(
                    ExpressionOccurrenceError::TemporalSource(
                        "selected invocation demand has no same-emission canonical request",
                    ),
                )?;
                declaration
                    .selected_argument_control_observed(
                        &function.function_id,
                        canonical.selected().as_ref(),
                        canonical.request().logical_argument_count,
                        work.control(),
                    )
                    .map_err(|error| ExpressionOccurrenceError::function(error.into()))?
            } else {
                declaration.effects().argument_control
            };
        let (shape, temporal_sources) =
            if let ArgumentControl::TemporalSource(kind) = selected_control {
                work.flush()?;
                let temporal =
                novarocks_expression_contract::temporal_source_definitions_from_lookup_observed(
                    kind,
                    |id| definitions.definition(id),
                    args,
                    work,
                )?;
                (
                    ControlShape::TemporalSource(temporal.facts.shape()),
                    Some(temporal),
                )
            } else {
                (
                    scalar_shape(selected_control, args.len())
                        .ok_or(ExpressionOccurrenceError::InvalidFunctionControl(id))?,
                    None,
                )
            };
        work.step()?;
        work.flush()?;
        controls
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        controls.push((
            id,
            DefinitionControl {
                shape,
                temporal_sources,
            },
        ));
        work.step()?;
    }
    Ok(controls)
}

#[derive(Clone, Debug)]
pub(crate) struct AuthoredPhysicalOccurrences<'a> {
    pub relational_contexts: Vec<(PhysicalCallSite, RelationEffectContext)>,
    fragment: &'a Fragment,
    writer_values: Vec<WriterValueUse<'a>>,
}
#[derive(Clone, Copy, Debug)]
struct WriterValueUse<'a> {
    site: PhysicalCallSite,
    value: &'a ValueDef,
    context: RelationEffectContext,
}
impl<'a> AuthoredPhysicalOccurrences<'a> {
    pub(crate) fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) fn writer_value_uses(
        &self,
    ) -> impl Iterator<Item = (PhysicalCallSite, &'a ValueDef, RelationEffectContext)> + '_ {
        self.writer_values
            .iter()
            .map(|entry| (entry.site, entry.value, entry.context))
    }
    pub(crate) fn writer_contexts(&self) -> WriterOccurrenceContexts<'_, 'a> {
        WriterOccurrenceContexts {
            fragment: self.fragment,
            relational_contexts: &self.relational_contexts,
            writer_values: &self.writer_values,
        }
    }
}
/// Original call sites issue one relation ID apiece. Materialized Writer input
/// shares that ID in its distinct role; neither role belongs to a scalar flow.
pub(crate) fn author_physical_occurrences_observed<'a>(
    fragment: &'a Fragment,
    functions: &dyn SqlFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<AuthoredPhysicalOccurrences<'a>, ExpressionOccurrenceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let mut relational_contexts = Vec::new();
        let mut writer_values = Vec::new();
        visit_relational_calls_observed(fragment, &mut work, |site, binding, work| {
            if relational_contexts.len() == MAX_CONTROL_USE_REFERENCES {
                return Err(ExpressionOccurrenceError::TooManyItems);
            }
            let (function_id, kind, overload, expected) = match binding {
                PhysicalCallBinding::Aggregate(binding) => (
                    &binding.function.function_id,
                    binding.function.kind,
                    &binding.function.overload,
                    ArgumentControl::Aggregate,
                ),
                PhysicalCallBinding::Window { function, .. } => (
                    &function.function_id,
                    function.kind,
                    &function.overload,
                    if function.kind == novarocks_type_contract::FunctionKind::Aggregate {
                        ArgumentControl::Aggregate
                    } else {
                        ArgumentControl::Window
                    },
                ),
                PhysicalCallBinding::Table(function) => (
                    &function.function_id,
                    novarocks_type_contract::FunctionKind::Table,
                    &function.overload,
                    ArgumentControl::Table,
                ),
            };
            work.flush()?;
            let declaration = functions
                .pure_overload_declaration_observed(function_id, kind, overload, control)
                .map_err(ExpressionOccurrenceError::function)?;
            let matching = declaration.effects().argument_control == expected;
            work.step()?;
            if !matching {
                return Err(ExpressionOccurrenceError::InvalidRelationalControl(site));
            }
            let id = RelationCallId::new(
                u32::try_from(relational_contexts.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            );
            work.flush()?;
            relational_contexts
                .try_reserve(1)
                .map_err(|_| CompileControlError::ResourceExhausted)?;
            relational_contexts.push((site, RelationEffectContext::call(id)));
            work.step()?;
            let channel = match (
                site,
                fragment.nodes().get(&site.node()).map(|node| &node.kind),
            ) {
                (
                    PhysicalCallSite::WriterPartial { call, .. },
                    Some(NodeKind::TableWriter { target }),
                ) => target.partial_aggregates.get(call as usize),
                (PhysicalCallSite::WriterFinal { call, .. }, Some(NodeKind::TableFinish(spec))) => {
                    spec.final_aggregates.get(call as usize)
                }
                _ => None,
            };
            work.step()?;
            if let Some(channel) = channel {
                let value = fragment.values().get(&channel.input);
                work.step()?;
                let value =
                    value.ok_or(ExpressionOccurrenceError::InvalidRelationalControl(site))?;
                if writer_values.len() == MAX_CONTROL_USE_REFERENCES {
                    return Err(ExpressionOccurrenceError::TooManyItems);
                }
                work.flush()?;
                writer_values
                    .try_reserve(1)
                    .map_err(|_| CompileControlError::ResourceExhausted)?;
                writer_values.push(WriterValueUse {
                    site,
                    value,
                    context: RelationEffectContext::materialized_input(id),
                });
                work.step()?;
            } else if matches!(
                site,
                PhysicalCallSite::WriterPartial { .. } | PhysicalCallSite::WriterFinal { .. }
            ) {
                return Err(ExpressionOccurrenceError::InvalidRelationalControl(site));
            }
            Ok(())
        })?;
        Ok(AuthoredPhysicalOccurrences {
            relational_contexts,
            fragment,
            writer_values,
        })
    })();
    work.finish_result(result, ExpressionOccurrenceError::Control)
}

pub(crate) struct WriterOccurrenceContexts<'a, 'source> {
    fragment: &'source Fragment,
    pub relational_contexts: &'a [(PhysicalCallSite, RelationEffectContext)],
    writer_values: &'a [WriterValueUse<'source>],
}
impl WriterOccurrenceContexts<'_, '_> {
    pub(crate) fn fragment(&self) -> &Fragment {
        self.fragment
    }
    pub(crate) fn root_fragment(&self) -> novarocks_physical_plan::FragmentId {
        self.fragment.id()
    }
    pub(crate) fn writer_value_uses(
        &self,
    ) -> impl Iterator<Item = (PhysicalCallSite, &ValueDef, RelationEffectContext)> {
        self.writer_values
            .iter()
            .map(|entry| (entry.site, entry.value, entry.context))
    }
}
pub(super) use crate::compiler::selected_scalar_call::scalar_shape;

#[cfg(test)]
#[path = "relational_occurrences_tests.rs"]
mod relational_tests;
#[cfg(test)]
#[path = "expression_occurrences_tests.rs"]
mod tests;
