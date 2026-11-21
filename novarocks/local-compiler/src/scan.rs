// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Admit and lower one provider read into the compiled Scan owner. The layout
//! is the provider's own public schema with compiled slots, the source is the
//! complete-input provider seal, and the physical scan NodeId is the explicit
//! runtime split address. Splits themselves are task facts, never program facts.

use crate::lowering::FragmentCompileError;
use novarocks_connector_contract::{ConnectorReadProgramRecipe, ConnectorReadRelationKind};
use novarocks_local_program::{
    BindingRequirement, CompiledScanInput, ProgramNodeId, ProgramNodeKind, ProgramScanSource,
    ScanSourceKind, StaticLayout,
};
use novarocks_physical_plan::{NodeKind, PhysicalNode, PredicateGuaranteeKind, Relation};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use novarocks_types::SlotId;

pub(crate) struct LoweredScan {
    pub kind: ProgramNodeKind,
    pub layout: StaticLayout,
    pub requirement: BindingRequirement,
    pub input: CompiledScanInput,
}

/// The admitted scan shape: a source-tree leaf over one provider work-unit
/// read whose output is exactly its provider outputs in order, with its original
/// ordered residuals and guarantees admitted by the checked package. Exact
/// guarantees remain in the provider recipe; pruning-only guarantees retain
/// their local residual. The sole FragmentPackage guarantee proof gate rejects
/// function calls without their guarantee's own proof before this compiler.
/// Every other shape is refused here, before channels or expressions exist.
pub(crate) fn admit_scan(
    node: &PhysicalNode,
    recipe: Option<&ConnectorReadProgramRecipe>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let NodeKind::Scan {
        relation,
        provider_outputs,
        residuals,
        derived_values,
        ..
    } = &node.kind
    else {
        return Err(FragmentCompileError::Invalid("Scan kind differs"));
    };
    let unsupported = |feature| FragmentCompileError::Unsupported {
        node: Some(node.id),
        feature,
    };
    work.step()?;
    match relation.as_ref() {
        Relation::Data(data) => {
            if !matches!(
                data.read.relation.kind(),
                ConnectorReadRelationKind::Table | ConnectorReadRelationKind::ChangeWindow
            ) {
                return Err(unsupported("provider relation other than a table"));
            }
        }
        Relation::Metadata(metadata) => {
            if metadata.read.relation.kind() != ConnectorReadRelationKind::SystemTable {
                return Err(FragmentCompileError::Invalid(
                    "metadata relation is not a system table",
                ));
            }
        }
    }
    if !derived_values.is_empty() {
        return Err(unsupported("derived or VARIANT scan value"));
    }
    for guarantee in relation.predicate_guarantees().iter() {
        let evaluated = residuals.contains(&guarantee.predicate);
        work.step()?;
        match guarantee.kind {
            // The checked package owns the guarantee proof and the validated
            // provider recipe owns its row filtering. Do not invent a local
            // expression use or repeat the provider's exact predicate here.
            PredicateGuaranteeKind::Exact => {}
            // The checked package already binds a pruning-only predicate to a
            // residual; this compiler never evaluates a guarantee on its own.
            PredicateGuaranteeKind::PruningOnly if !evaluated => {
                return Err(FragmentCompileError::Invalid(
                    "pruning-only guarantee has no scan residual",
                ));
            }
            PredicateGuaranteeKind::PruningOnly => {}
        }
    }
    // The compiled layout is the public schema itself, so the output port
    // must be the provider outputs in their exact order, with nothing derived.
    if node.output.columns.len() != provider_outputs.len() {
        return Err(unsupported(
            "scan output differs from its ordered provider outputs",
        ));
    }
    for (actual, (_, expected)) in node.output.columns.iter().zip(provider_outputs.iter()) {
        let same = actual == expected;
        work.step()?;
        if !same {
            return Err(unsupported(
                "scan output differs from its ordered provider outputs",
            ));
        }
    }
    let _recipe = recipe.ok_or(FragmentCompileError::Invalid(
        "scan has no provider read recipe",
    ))?;
    // The checked package and provider validation retain the exact recipe.
    work.step()?;
    Ok(())
}

/// `runtime_filters` are the scan's admitted blocking membership consumers in
/// binding order; consumer `i` is keyed by its `RuntimeFilter { binding: i }`
/// root over this scan's own output.
pub(crate) fn lower_scan_with_creation<H>(
    node: &PhysicalNode,
    id: ProgramNodeId,
    recipe: ConnectorReadProgramRecipe,
    runtime_filters: novarocks_local_program::ScanFilterConsumers,
    slots: &[SlotId],
    expressions: &crate::expressions::RootMap,
    control: &dyn PureCompileControl,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredScan, crate::FragmentCreationError<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core_with_creation(
        node,
        id,
        recipe,
        runtime_filters,
        slots,
        expressions,
        &mut work,
        creation,
    );
    crate::creation::finish_attempt(result, work)
}

fn lower_core_with_creation<H>(
    node: &PhysicalNode,
    id: ProgramNodeId,
    recipe: ConnectorReadProgramRecipe,
    runtime_filters: novarocks_local_program::ScanFilterConsumers,
    slots: &[SlotId],
    expressions: &crate::expressions::RootMap,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<LoweredScan, crate::FragmentCreationError<H>> {
    let NodeKind::Scan {
        provider_outputs,
        residuals,
        ..
    } = &node.kind
    else {
        return Err((FragmentCompileError::Invalid("Scan kind differs")).into());
    };
    let public = recipe.frozen().public_facts();
    let width = public.schema().fields().len();
    work.step()?;
    if !node.inputs.is_empty()
        || width != slots.len()
        || width != node.output.columns.len()
        || width != provider_outputs.len()
    {
        return Err((FragmentCompileError::Invalid(
            "Scan input, public schema, channel or output width differs",
        ))
        .into());
    }
    // Every original residual keeps its own ordered TruthOnly root.
    let mut lowered_residuals = Vec::with_capacity(residuals.len());
    for residual in residuals.iter() {
        lowered_residuals.push(
            *expressions
                .get(residual)
                .ok_or(FragmentCompileError::Invalid(
                    "missing scan residual definition",
                ))?,
        );
        work.step()?;
    }
    // Source attributes remain on the provider facts. Execution fields and
    // bindings are borrowed from its independently authored typed projection.
    let execution = public
        .execution_schema()
        .ok_or(FragmentCompileError::Invalid(
            "scan has no complete execution schema",
        ))?;
    let mut typed_layout =
        crate::typed_layout::TypedLayoutBuilder::new_with_creation(width, work, creation)?;
    for (field, ty) in execution.schema().fields().iter().zip(execution.bindings()) {
        work.step()?;
        typed_layout.push_with_creation(
            ty,
            crate::typed_layout::copy_name_with_creation(field.name(), work, creation)?,
            work,
            creation,
        )?;
    }
    let layout = typed_layout.finish_with_creation(slots, work, creation)?;
    work.flush()?;
    let source = ProgramScanSource::from(recipe);
    let relation = source.relation_header().clone();
    work.step()?;
    Ok(LoweredScan {
        kind: ProgramNodeKind::Scan {
            source,
            runtime_filters,
            residuals: lowered_residuals,
            limit: None,
        },
        requirement: BindingRequirement::Scan {
            node: id,
            kind: ScanSourceKind::TypedConnector { relation },
            layout: layout.clone(),
        },
        layout,
        input: CompiledScanInput {
            scan_node: node.id.get(),
        },
    })
}
