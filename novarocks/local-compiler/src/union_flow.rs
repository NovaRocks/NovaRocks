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

//! Compiler-authored reads use the same checked unit and pure compiler as node roots.
use crate::{FragmentCompileError, FragmentCreationError, expressions::LoweredExpressions};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::{ConstantValidation, PureEngineFunctionCatalog};
use novarocks_local_program::{ProgramRootRef, ProgramUnitId, StaticLayout};
use novarocks_type_contract::{
    CompileCheckpoints, EvaluationDemand, FunctionValueType, ValueSchemaCreation,
    ValueSchemaCreationFactory, ValueSchemaCreationScope,
};
use std::collections::{BTreeMap, BTreeSet};

#[expect(
    clippy::too_many_arguments,
    reason = "Actual source layout, selected roots and original creator ports remain explicit"
)]
pub(crate) fn compile_reads<H, F: ValueSchemaCreationFactory<H>>(
    package: &novarocks_physical_plan::FragmentPackage,
    expressions: &mut LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    policy: ConstantValidation,
    layout: &StaticLayout,
    columns: &[(ValueId, FunctionValueType)],
    reads: &[(usize, EvaluationDemand)],
    creation: &mut ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ProgramRootRef>, FragmentCreationError<H>> {
    if columns.len() != layout.schema().fields().len() {
        return Err(FragmentCompileError::Invalid(
            "synthesized read source differs from actual runtime width",
        )
        .into());
    }
    let mut seen = BTreeSet::new();
    let mut declarations = Vec::new();
    let mut values = Vec::new();
    crate::creation::reserve_vec(&mut values, columns.len(), Some(creation), work)?;
    crate::creation::reserve_vec(&mut declarations, columns.len(), Some(creation), work)?;
    for (value, ty) in columns {
        work.step()?;
        values.push(*value);
        if !seen.contains(value) {
            crate::creation::before_tree_insert::<ValueId, (), H>(
                seen.len(),
                crate::creation::borrow_scope(&mut temporary),
                work,
            )?;
            seen.insert(*value);
            declarations.push((*value, ty.clone()));
        }
    }
    let scope = crate::expressions::scope_layout(
        &values,
        layout,
        &declarations,
        creation,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    let mut roots = Vec::new();
    crate::creation::reserve_vec(
        &mut roots,
        reads.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    for &(ordinal, demand) in reads {
        work.step()?;
        let (value, ty) = columns.get(ordinal).ok_or(FragmentCompileError::Invalid(
            "synthesized read ordinal is outside actual source",
        ))?;
        let root = if factory.is_some() {
            let definition = builder.add_with_creator_in(
                None,
                ty.clone(),
                ExprKind::input(*value),
                creation,
                work,
            )?;
            builder.root_with_creator_in(definition, demand, RootScope::Port(0), creation, work)?
        } else {
            let definition = builder
                .add(None, ty.clone(), ExprKind::input(*value))
                .map_err(unit_error)?;
            builder
                .root(definition, demand, RootScope::Port(0))
                .map_err(unit_error)?
        };
        roots.push(root);
    }
    // These public source containers belong to the enclosing caller, while
    // the builder's private definition/root Vecs are funded by its own API.
    if declarations.capacity() != declarations.len() && !declarations.is_empty() {
        crate::creation::request(std::alloc::Layout::array::<(ValueId,FunctionValueType)>(declarations.len()).map_err(|_|novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)?,creation,work)?;
    }
    let port = ExprPort {
        values: declarations.into_boxed_slice(),
    };
    let mut ports = Vec::new();
    crate::creation::reserve_vec(&mut ports, 1, Some(creation), work)?;
    ports.push(port);
    let unit = if factory.is_some() {
        builder
            .seal_with_creator_in(
                ports,
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies its original temporary scope"),
                creation,
                work,
            )?
            .finish_with_creator_in(
                BTreeMap::new(),
                crate::creation::borrow_scope(&mut temporary)
                    .expect("strong entry supplies its original temporary scope"),
                creation,
                work,
            )?
    } else {
        builder
            .with_storage_lifetime(creation.storage_lifetime())
            .map_err(unit_error)?
            .seal(ports, work)
            .map_err(unit_error)?
            .finish(BTreeMap::new(), work)
            .map_err(unit_error)?
    };
    let compiled = crate::expressions::compile_unit(
        &unit,
        package,
        &[scope],
        None,
        functions,
        policy,
        factory,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    let id = ProgramUnitId::new(expressions.units.len());
    crate::creation::reserve_vec(
        &mut expressions.units,
        1,
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    expressions.units.push(Some(compiled));
    let mut references = Vec::new();
    crate::creation::reserve_vec(&mut references, roots.len(), Some(creation), work)?;
    for root in roots {
        work.step()?;
        references.push(ProgramRootRef { unit: id, root });
    }
    Ok(references)
}

fn unit_error(error: novarocks_expression_contract::UnitError) -> FragmentCompileError {
    match error {
        novarocks_expression_contract::UnitError::Control(c) => FragmentCompileError::Control(c),
        error => FragmentCompileError::Owner {
            phase: "synthetic expression unit",
            error: novarocks_failure_contract::SharedFailureCause::new(error),
        },
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "The same original owners fund one actual normalized input"
)]
pub(crate) fn compile_branch_reads<H, F: ValueSchemaCreationFactory<H>>(
    package: &novarocks_physical_plan::FragmentPackage,
    expressions: &mut LoweredExpressions,
    functions: &PureEngineFunctionCatalog,
    policy: ConstantValidation,
    layout: &StaticLayout,
    source: novarocks_physical_plan::NodeId,
    branch: &crate::channels::UnionChannelBranch,
    creation: &mut ValueSchemaCreation<'_, H>,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ProgramRootRef>, FragmentCreationError<H>> {
    let values = &package.fragment().nodes()[&source].output.columns;
    let mut columns = Vec::new();
    crate::creation::reserve_vec(&mut columns, values.len(), Some(creation), work)?;
    for value in values {
        work.step()?;
        columns.push((*value, package.fragment().values()[value].ty.clone()));
    }
    let mut reads = Vec::new();
    crate::creation::reserve_vec(
        &mut reads,
        branch.sources.len(),
        crate::creation::borrow_scope(&mut temporary),
        work,
    )?;
    for source in &branch.sources {
        let novarocks_local_program::ProgramChannelSite::Layout { ordinal, .. } =
            source.input.source
        else {
            return Err(FragmentCompileError::Invalid(
                "synthesized branch reads a nonlayout channel",
            )
            .into());
        };
        reads.push((ordinal as usize, EvaluationDemand::Value));
        work.step()?;
    }
    compile_reads(
        package,
        expressions,
        functions,
        policy,
        layout,
        &columns,
        &reads,
        creation,
        temporary,
        factory,
        work,
    )
}
