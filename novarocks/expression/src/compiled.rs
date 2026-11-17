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

//! One immutable compiled unit and its original per-use facts.
use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::primitives::{ComparisonSite, CreatedPrimitiveMap, ProgramPrimitiveError};
use crate::{
    CompiledExprId, ExprCompileError, ExprCompileFailure, ImmutableExpressions, InputBinding,
    ScopeLayout, ScopeLayouts,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPools};
use novarocks_expression_contract::{BoundExprSet, ExprId, ExprKind, RootOrdinal, RootScope};
use novarocks_functions::{
    PureCallSpecialization, PureEngineFunctionCatalog, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    CompileCheckpoints, ExpressionControlFlow, ExpressionInvocation, ExpressionUseId,
    FunctionArgumentTypes, SemanticParameters, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueTypeStorageLifetime,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
pub struct EvaluationEnvironment<'a> {
    pub constants: &'a ConstantPools,
    pub parameters: &'a SemanticParameters,
}
pub struct ExprCreation<'a, H, F: ValueSchemaCreationFactory<H>> {
    pub temporary: &'a mut dyn ValueSchemaCreationScope<H>,
    pub retained: &'a mut dyn ValueSchemaCreationScope<H>,
    pub factory: &'a F,
}
#[derive(Clone, Copy, Debug)]
pub struct CompiledRoot {
    pub definition: CompiledExprId,
    pub demand: novarocks_type_contract::EvaluationDemand,
    pub scope: RootScope,
    pub use_id: ExpressionUseId,
}
#[derive(Debug)]
pub(crate) struct CompiledParts {
    pub(crate) definitions: Arc<ImmutableExpressions>,
    pub(crate) flow: ExpressionControlFlow<CompiledExprId>,
    pub(crate) types: FunctionArgumentTypes,
}
#[derive(Debug)]
pub struct UnitRecipes {
    native_like:
        CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedNativeLikeRecipe>,
    native_inlist:
        CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedNativeInListRecipe>,
    native_negate:
        CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedNativeNegateRecipe>,
    native_bitnot:
        CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedNativeBitNotRecipe>,
    arithmetic: CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedArithmeticRecipe>,
    casts: CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedCastRecipe>,
    comparisons: CreatedPrimitiveMap<ComparisonSite, novarocks_functions::PreparedComparisonRecipe>,
    null_safe:
        CreatedPrimitiveMap<ExpressionUseId, novarocks_functions::PreparedNullSafeComparisonRecipe>,
}
#[derive(Debug)]
pub struct CompiledExprSet {
    pub(crate) parts: CompiledParts,
    calls: BTreeMap<ExpressionUseId, PureCallSpecialization>,
    recipes: UnitRecipes,
    inputs: BTreeMap<ExpressionUseId, InputBinding>,
    owners: BTreeMap<ExpressionUseId, RootOrdinal>,
    scopes: Box<[ScopeLayout]>,
    all_ports: Option<ScopeLayout>,
    roots: Box<[CompiledRoot]>,
    effects: BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    ids: BTreeMap<ExprId, CompiledExprId>,
    storage: ValueTypeStorageLifetime,
}
#[path = "evaluator_view.rs"]
pub mod evaluator_view;

fn publication<H>(e: ProgramCreationFailure<ProgramPrimitiveError, H>) -> ExprCompileFailure<H> {
    match e {
        ProgramCreationFailure::Control(e) => ExprCompileFailure::Control(e),
        ProgramCreationFailure::Creator(e) => ExprCompileFailure::Creator(e),
        ProgramCreationFailure::Resource(e) => ExprCompileFailure::Resource(e),
        ProgramCreationFailure::Capacity(e) => ExprCompileFailure::Owner(
            ExprCompileError::Publication(ProgramCreationFailure::Capacity(e)),
        ),
        ProgramCreationFailure::Owner(e) => {
            ExprCompileFailure::Owner(ExprCompileError::Primitive(e))
        }
    }
}
/// Borrow actual caller construction owners. No new root scope or grant is minted here.
pub fn compile_expr_set<H, F: ValueSchemaCreationFactory<H>>(
    unit: &BoundExprSet,
    environment: EvaluationEnvironment<'_>,
    scopes: ScopeLayouts<'_>,
    policy: ConstantPolicy,
    catalog: &PureEngineFunctionCatalog,
    creation: ExprCreation<'_, H, F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<CompiledExprSet>, ExprCompileFailure<H>> {
    compile_core(
        unit,
        environment,
        scopes,
        policy,
        catalog,
        Some(creation.temporary),
        Some(creation.retained),
        Some(creation.factory),
        work,
    )
}
/// Explicit pure preparation. Absence of a creator is not a successful no-op factory.
pub fn compile_expr_set_pure(
    unit: &BoundExprSet,
    environment: EvaluationEnvironment<'_>,
    scopes: ScopeLayouts<'_>,
    policy: ConstantPolicy,
    catalog: &PureEngineFunctionCatalog,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<CompiledExprSet>, ExprCompileError> {
    compile_core::<std::convert::Infallible, crate::creation::NoFactory>(
        unit,
        environment,
        scopes,
        policy,
        catalog,
        None,
        None,
        None,
        work,
    )
    .map_err(ExprCompileFailure::into_pure)
}
#[allow(clippy::too_many_arguments)]
fn compile_core<H, F: ValueSchemaCreationFactory<H>>(
    unit: &BoundExprSet,
    environment: EvaluationEnvironment<'_>,
    scopes: ScopeLayouts<'_>,
    policy: ConstantPolicy,
    catalog: &PureEngineFunctionCatalog,
    mut temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
    mut retained: Option<&mut dyn ValueSchemaCreationScope<H>>,
    factory: Option<&F>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<CompiledExprSet>, ExprCompileFailure<H>> {
    let control = work.control();
    if scopes.ports.len() != unit.ports().len() {
        return Err(ExprCompileError::Invalid("scope layout count differs from unit ports").into());
    }
    for (port, layout) in unit.ports().iter().zip(scopes.ports) {
        if port.values.len() != layout.columns().len() {
            return Err(ExprCompileError::Invalid("scope width differs from unit port").into());
        }
        for ((value, ty), column) in port.values.iter().zip(layout.columns()) {
            work.step()?;
            if *value != column.value
                || !ty.exactly_equals_observed(&column.ty, || {
                    work.step().map_err(ExprCompileError::from)
                })?
            {
                return Err(
                    ExprCompileError::Invalid("scope input differs from exact unit port").into(),
                );
            }
        }
    }
    if unit.ports().is_empty() && scopes.all_ports.is_some_and(|l| !l.columns().is_empty()) {
        return Err(
            ExprCompileError::Invalid("zero-port unit has a nonempty combined layout").into(),
        );
    }
    if scopes.all_ports.is_none()
        && !unit.ports().is_empty()
        && unit.roots().iter().any(|r| r.scope == RootScope::AllPorts)
    {
        return Err(
            ExprCompileError::Invalid("combined root requires its actual scope layout").into(),
        );
    }
    // Index each actual scope once under the caller's original temporary
    // owner. The ordered source columns remain shared immutable facts.
    let indexes = index_scopes(scopes, &mut temporary, work)?;
    if let Some(combined) = scopes.all_ports {
        let total: usize = unit.ports().iter().map(|p| p.values.len()).sum();
        if total != combined.columns().len() {
            return Err(ExprCompileError::Invalid(
                "combined scope width differs from actual ports",
            )
            .into());
        }
        for port in unit.ports() {
            for (value, ty) in &port.values {
                work.step()?;
                work.flush()?;
                let (_, actual) =
                    indexes[scopes.ports.len()]
                        .get(value)
                        .ok_or(ExprCompileError::Invalid(
                            "combined scope lacks a declared value",
                        ))?;
                if !ty.exactly_equals_observed(actual, || {
                    work.step().map_err(ExprCompileError::from)
                })? {
                    return Err(
                        ExprCompileError::Invalid("combined scope complete type differs").into(),
                    );
                }
            }
        }
    }
    // Admit newly constructed headers and maps against the actual borrowed
    // owners. Checked source identity never authorizes these new containers.
    charge_structure(unit, scopes, &mut temporary, &mut retained, work)?;
    let lowered = crate::compiler::lower_core(
        unit,
        environment,
        control,
        work,
        crate::creation::borrow_scope(&mut retained),
        factory,
    )?;
    let creation = factory.map(|f| (f, &policy));
    let (calls, effects) = crate::compiler::prepare_core(
        unit,
        environment,
        &lowered,
        catalog,
        control,
        work,
        creation,
    )?;
    let mut inputs = BTreeMap::new();
    let mut roots = Vec::new();
    let mut owners = BTreeMap::new();
    for (ordinal, root) in unit.roots().iter().enumerate() {
        let first = unit
            .root_use(RootOrdinal::new(ordinal as u32))
            .ok_or(ExprCompileError::Invalid("unit root use is absent"))?;
        let mut stack = vec![first];
        while let Some(id) = stack.pop() {
            work.step()?;
            if owners
                .insert(id, RootOrdinal::new(ordinal as u32))
                .is_some()
            {
                return Err(
                    ExprCompileError::Invalid("unit use belongs to multiple input scopes").into(),
                );
            }
            let use_ = unit
                .control_flow()
                .uses()
                .get(&id)
                .ok_or(ExprCompileError::Invalid("unit input use is absent"))?;
            if let ExprKind::Value(value) = unit
                .definition(use_.definition)
                .ok_or(ExprCompileError::Invalid("unit input definition is absent"))?
                .kind
            {
                let index = match root.scope {
                    RootScope::Port(i) => indexes.get(i as usize),
                    RootScope::AllPorts => indexes.get(scopes.ports.len()),
                }
                .ok_or(ExprCompileError::Invalid(
                    "actual input scope layout is absent",
                ))?;
                work.flush()?;
                let (position, ty) = index.get(&value).ok_or(ExprCompileError::Invalid(
                    "input value is absent from its actual scope",
                ))?;
                work.step()?;
                let source = &unit.definition(use_.definition).unwrap().ty;
                if !source
                    .exactly_equals_observed(ty, || work.step().map_err(ExprCompileError::from))?
                {
                    return Err(ExprCompileError::Invalid(
                        "input value full type differs from scope",
                    )
                    .into());
                }
                inputs.insert(
                    id,
                    InputBinding {
                        scope: root.scope,
                        ordinal: *position,
                    },
                );
            }
            stack.extend(use_.arguments.iter().rev().copied());
        }
        roots.push(CompiledRoot {
            definition: lowered.ids[&root.definition],
            demand: root.demand,
            scope: root.scope,
            use_id: first,
        });
    }
    let uses = unit
        .control_flow()
        .uses()
        .values()
        .map(|u| ExpressionInvocation {
            context: u.context,
            definition: lowered.ids[&u.definition],
            control: u.control,
            arguments: u.arguments.clone(),
        })
        .collect();
    struct Definitions(usize);
    impl novarocks_type_contract::ExpressionDefinitionMembership<CompiledExprId> for Definitions {
        fn definition_count(&self) -> usize {
            self.0
        }
        fn contains_definition(&self, id: CompiledExprId) -> bool {
            id.index() < self.0
        }
    }
    let regions = unit.control_flow().regions().values().cloned().collect();
    let flow = match (
        crate::creation::borrow_scope(&mut temporary),
        crate::creation::borrow_scope(&mut retained),
    ) {
        (Some(temporary), Some(retained)) => ExpressionControlFlow::try_new_with_creator_in(
            regions,
            uses,
            &Definitions(lowered.arena.nodes().len()),
            temporary,
            retained,
            work,
        )
        .map_err(|e| match e {
            novarocks_type_contract::ExpressionControlCreationFailure::Control(e) => {
                ExprCompileFailure::Control(e)
            }
            novarocks_type_contract::ExpressionControlCreationFailure::Creator(e) => {
                ExprCompileFailure::Creator(e)
            }
            novarocks_type_contract::ExpressionControlCreationFailure::Resource(e) => {
                ExprCompileFailure::Resource(e)
            }
            novarocks_type_contract::ExpressionControlCreationFailure::Owner(e) => {
                ExprCompileError::from(e).into()
            }
        })?,
        (None, None) => ExpressionControlFlow::try_new_in(
            regions,
            uses,
            &Definitions(lowered.arena.nodes().len()),
            &mut |_| Ok(()),
            work,
        )
        .map_err(ExprCompileError::from)?,
        _ => {
            return Err(ExprCompileError::Invalid(
                "unit construction owners must be supplied together",
            )
            .into());
        }
    };
    let parts = CompiledParts {
        definitions: lowered.arena,
        flow,
        types: lowered.types,
    };
    let native_like =
        crate::primitives::compile_native_like_with_factory(&parts, None, factory, control)
            .map_err(publication)?;
    let native_inlist = crate::primitives::compile_native_inlist_with_factory(
        &parts,
        crate::creation::borrow_scope(&mut temporary),
        factory,
        control,
    )
    .map_err(publication)?;
    let native_negate =
        crate::primitives::compile_native_negate_with_factory(&parts, None, factory, control)
            .map_err(publication)?;
    let native_bitnot =
        crate::primitives::compile_native_bitnot_with_factory(&parts, None, factory, control)
            .map_err(publication)?;
    let arithmetic =
        crate::primitives::compile_arithmetic_with_factory(&parts, None, factory, control)
            .map_err(publication)?;
    let casts = crate::primitives::compile_casts_with_factory(&parts, None, factory, control)
        .map_err(publication)?;
    let comparisons =
        crate::primitives::compile_comparisons_with_factory(&parts, None, factory, control)
            .map_err(publication)?;
    let null_safe = crate::primitives::compile_null_safe_comparisons_with_factory(
        &parts, None, factory, control,
    )
    .map_err(publication)?;
    let mut owner = ProgramCreation { creator: retained };
    owner
        .before_arc::<ProgramPrimitiveError, CompiledExprSet>(work)
        .map_err(publication)?;
    let storage = owner.storage();
    let all_ports = match scopes.all_ports {
        Some(layout) => Some(layout.clone()),
        None if unit.ports().is_empty() => Some(ScopeLayout::try_new(
            Arc::new(arrow::datatypes::Schema::empty()),
            Arc::from([]),
            storage.clone(),
            work,
        )?),
        None => None,
    };
    Ok(Arc::new(CompiledExprSet {
        parts,
        calls: calls.into_iter().collect(),
        recipes: UnitRecipes {
            native_like,
            native_inlist,
            native_negate,
            native_bitnot,
            arithmetic,
            casts,
            comparisons,
            null_safe,
        },
        inputs,
        owners,
        scopes: scopes.ports.into(),
        all_ports,
        roots: roots.into_boxed_slice(),
        effects,
        ids: lowered.ids,
        storage,
    }))
}

fn charge_structure<H>(
    unit: &BoundExprSet,
    scopes: ScopeLayouts<'_>,
    temporary: &mut Option<&mut dyn ValueSchemaCreationScope<H>>,
    retained: &mut Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExprCompileFailure<H>> {
    use novarocks_type_contract::owned_resources::{
        btree,
        layout::{LayoutResourceError, arc_layout},
    };
    use std::alloc::Layout;
    let resource = |_| ExprCompileFailure::Resource(LayoutResourceError::Arithmetic);
    let n = unit.definitions().len();
    let m = unit.control_flow().uses().len();
    let mut out = ProgramCreation {
        creator: crate::creation::borrow_scope(retained),
    };
    for layout in [
        Layout::array::<crate::StaticExprNode>(n).map_err(resource)?,
        arc_layout(Layout::array::<crate::StaticExprNode>(n).map_err(resource)?)
            .map_err(ExprCompileFailure::Resource)?,
        arc_layout(Layout::new::<ImmutableExpressions>()).map_err(ExprCompileFailure::Resource)?,
        Layout::array::<CompiledRoot>(unit.roots().len()).map_err(resource)?,
        Layout::array::<ScopeLayout>(scopes.ports.len()).map_err(resource)?,
    ] {
        if layout.size() != 0 {
            out.request::<ProgramPrimitiveError>(layout, work)
                .map_err(publication)?;
        }
    }
    if unit.ports().is_empty() && scopes.all_ports.is_none() {
        out.request::<ProgramPrimitiveError>(
            arc_layout(Layout::new::<arrow::datatypes::Schema>())
                .map_err(ExprCompileFailure::Resource)?,
            work,
        )
        .map_err(publication)?;
    }
    macro_rules! map {
        ($key:ty,$value:ty,$count:expr) => {{
            let facts = btree::insertion_only::<$key, $value>($count)
                .map_err(|e| ExprCompileError::BTree(e))?;
            if facts.request_bytes_upper_bound != 0 {
                out.opaque::<ProgramPrimitiveError>(facts.request_bytes_upper_bound, work)
                    .map_err(publication)?;
            }
        }};
    }
    map!(ExprId, CompiledExprId, n);
    map!(ExpressionUseId, InputBinding, m);
    map!(ExpressionUseId, RootOrdinal, m);
    map!(ExpressionUseId, ScopedExpressionEffects, m);
    map!(ExpressionUseId, PureCallSpecialization, unit.claims().len());
    let mut scratch = ProgramCreation {
        creator: crate::creation::borrow_scope(temporary),
    };
    for layout in [
        Layout::array::<(ExprId, usize)>(n).map_err(resource)?,
        Layout::array::<ExpressionUseId>(m).map_err(resource)?,
    ] {
        if layout.size() != 0 {
            scratch
                .request::<ProgramPrimitiveError>(layout, work)
                .map_err(publication)?;
        }
    }
    Ok(())
}

type ScopeIndex<'a> = BTreeMap<
    novarocks_expression_contract::ValueId,
    (u32, &'a novarocks_type_contract::FunctionValueType),
>;
fn index_scopes<'a, H>(
    scopes: ScopeLayouts<'a>,
    temporary: &mut Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<ScopeIndex<'a>>, ExprCompileFailure<H>> {
    let mut owner = ProgramCreation {
        creator: crate::creation::borrow_scope(temporary),
    };
    let count = scopes
        .ports
        .len()
        .checked_add(usize::from(scopes.all_ports.is_some()))
        .ok_or(ExprCompileFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ))?;
    let mut indexes = owner
        .reserve::<ProgramPrimitiveError, ScopeIndex<'a>>(count, work)
        .map_err(publication)?;
    for scope in scopes.ports.iter().chain(scopes.all_ports) {
        let mut index = ScopeIndex::new();
        for (ordinal, column) in scope.columns().iter().enumerate() {
            work.step()?;
            owner.before_tree_insert::<ProgramPrimitiveError,novarocks_expression_contract::ValueId,(u32,&'a novarocks_type_contract::FunctionValueType)>(index.len(),work).map_err(publication)?;
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                ExprCompileError::Invalid("scope input ordinal exceeds its address domain")
            })?;
            if index.insert(column.value, (ordinal, &column.ty)).is_some() {
                return Err(ExprCompileError::Invalid("scope has ambiguous input values").into());
            }
        }
        indexes.push(index);
    }
    Ok(indexes)
}
