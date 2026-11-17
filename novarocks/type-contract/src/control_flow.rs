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

//! Flat expression use/control regions. A shared definition is not a shared
//! invocation: every use retains its own demand and strong branch region.
use crate::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, PureCompileControl,
};
mod creation;
mod resources;
pub use creation::ExpressionControlCreationFailure;
pub use resources::*;

use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
};

/// Borrowed membership view of one validated immutable definition arena.
/// Count and membership must describe the same backing. Both operations are
/// bounded and perform no I/O; the view is never retained in a control graph.
pub trait ExpressionDefinitionMembership<D> {
    fn definition_count(&self) -> usize;
    fn contains_definition(&self, definition: D) -> bool;
}

/// Existing expression-arena and invocation limits, shared by both projections.
pub const MAX_CONTROL_DEFINITIONS: usize = 262_144;
pub const MAX_CONTROL_DEPTH: usize = 96;
pub const MAX_CONTROL_USE_REFERENCES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlShape {
    Eager,
    /// Full call definitions are retained; this invocation has zero child
    /// demand and still invokes its exact original owner.
    NoArguments,
    TypeOnly,
    /// The exact higher-order call has already established the invocation
    /// guard. Ordered local definitions require Value, followed by the body
    /// which inherits the wrapper's demand. A lambda with no locals has one
    /// argument: its body.
    LambdaBody,
    Conjunction,
    Disjunction,
    If,
    Coalesce,
    /// Four ordered source occurrences and two intervening comparisons.
    Between {
        negated: bool,
    },
    /// One source followed by ordered candidate comparisons. Matching never
    /// suppresses a later candidate; an empty invocation skips candidates.
    Membership {
        negated: bool,
    },
    TemporalSource(crate::TemporalSourceShape),
    Case {
        simple: bool,
        arms: u32,
        has_else: bool,
    },
    HigherOrder {
        body_ordinal: u32,
        body_demand: crate::EvaluationDemand,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardKind {
    IfThen,
    IfElse,
    CoalesceAfterNull {
        ordinal: u32,
    },
    CaseWhen {
        arm: u32,
    },
    CaseThen {
        arm: u32,
    },
    CaseElse,
    LambdaInvocation,
    /// The previous source phase completed successfully for this row.
    TemporalAfterSource {
        ordinal: u32,
    },
    /// The preceding source (and completed bound comparison) succeeded.
    BetweenAfterSource {
        ordinal: u32,
    },
    /// Source/comparison completed and current invocation is nonempty.
    MembershipAfterSource {
        ordinal: u32,
    },
    /// Whole current invocation predicate, never a per-row NULL branch.
    TemporalInvocationNull,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionGuard {
    pub owner: ExpressionUseId,
    pub kind: GuardKind,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpressionEvaluationRegion {
    pub id: EvaluationRegionId,
    pub parent: Option<EvaluationRegionId>,
    pub guard: Option<RegionGuard>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpressionInvocation<D> {
    pub context: ExpressionEffectContext,
    pub definition: D,
    pub control: ControlShape,
    pub arguments: Box<[ExpressionUseId]>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpressionControlFlow<D> {
    regions: Arc<ControlFlowOwned<BTreeMap<EvaluationRegionId, ExpressionEvaluationRegion>>>,
    uses: Arc<ControlFlowOwned<BTreeMap<ExpressionUseId, ExpressionInvocation<D>>>>,
    roots: Arc<ControlFlowOwned<Box<[ExpressionUseId]>>>,
    use_references: usize,
}
/// Private immutable backing; an independently cloned graph retains only its
/// own maps/root array and their original creation responsibility.
struct ControlFlowOwned<T> {
    value: T,
    _storage: crate::ValueTypeStorageLifetime,
}
impl<T> std::ops::Deref for ControlFlowOwned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
impl<T: fmt::Debug> fmt::Debug for ControlFlowOwned<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}
impl<T: PartialEq> PartialEq for ControlFlowOwned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}
impl<T: Eq> Eq for ControlFlowOwned<T> {}
impl<T> ControlFlowOwned<T> {
    fn pure(value: T) -> Self {
        Self {
            value,
            _storage: crate::ValueTypeStorageLifetime::pure(),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpressionControlFlowError {
    Control(CompileControlError),
    TooManyItems,
    DuplicateIdentity,
    InvalidReference,
    InvalidGuard,
    InvalidControlShape,
    InvalidDemand,
    SharedUse,
    Cycle,
    TooDeep,
    SourceModel(&'static str),
}
impl fmt::Display for ExpressionControlFlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid expression control flow: {self:?}")
    }
}
impl std::error::Error for ExpressionControlFlowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for ExpressionControlFlowError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl<D: Copy> ExpressionControlFlow<D> {
    /// The caller supplies the count and exact membership of an already
    /// validated immutable definition arena. Sparse maps use their actual
    /// keys; dense arenas check ordinals without allocating an ID set. This
    /// borrowed O(1)/O(log n) lookup is called only during validation and is
    /// never retained by the graph. It performs no I/O or runtime lookup.
    /// The caller also checks the exact definition/control correspondence.
    /// No instance Selection or mutable state is stored here.
    pub fn try_new(
        regions: Vec<ExpressionEvaluationRegion>,
        uses: Vec<ExpressionInvocation<D>>,
        definitions: &impl ExpressionDefinitionMembership<D>,
        phase: CompilePhase,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExpressionControlFlowError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let parts = Self::parts_in(
            regions,
            uses,
            definitions,
            &mut |_| Ok(()),
            false,
            &mut work,
        );
        if let Err(ExpressionControlFlowError::Control(cause)) = &parts {
            return Err(cause.clone().into());
        }
        work.finish()?;
        let (regions, uses, roots, references) = parts?;
        Ok(Self {
            regions: Arc::new(ControlFlowOwned::pure(regions)),
            uses: Arc::new(ControlFlowOwned::pure(uses)),
            roots: Arc::new(ControlFlowOwned::pure(roots.into_boxed_slice())),
            use_references: references,
        })
    }

    /// Construct using the caller's original scope and synchronous parent
    /// admission. Facts are cumulative for this call; no entry or footer is
    /// emitted here. Definition membership remains the original borrowed view.
    pub fn try_new_in(
        regions: Vec<ExpressionEvaluationRegion>,
        uses: Vec<ExpressionInvocation<D>>,
        definitions: &impl ExpressionDefinitionMembership<D>,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExpressionControlFlowError> {
        let (regions, uses, roots, references) =
            Self::parts_in(regions, uses, definitions, admit, true, work)?;
        work.flush()?;
        let regions = Arc::new(ControlFlowOwned::pure(regions));
        work.step()?;
        work.flush()?;
        let uses = Arc::new(ControlFlowOwned::pure(uses));
        work.step()?;
        work.flush()?;
        let roots = Arc::new(ControlFlowOwned::pure(roots.into_boxed_slice()));
        work.step()?;
        work.flush()?;
        Ok(Self {
            regions,
            uses,
            roots,
            use_references: references,
        })
    }
    /// Actual creation of the same validated graph, with temporary validation
    /// structures and immutable output backing in independent caller scopes.
    pub fn try_new_with_creator_in<H>(
        regions: Vec<ExpressionEvaluationRegion>,
        uses: Vec<ExpressionInvocation<D>>,
        definitions: &impl ExpressionDefinitionMembership<D>,
        temporary: &mut dyn crate::ValueSchemaCreationScope<H>,
        retained: &mut dyn crate::ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExpressionControlCreationFailure<H>> {
        use ExpressionControlCreationFailure as Failure;
        // Preserve the existing first header law before any admission/traversal.
        if regions.len() > MAX_CONTROL_DEFINITIONS
            || uses.len() > MAX_CONTROL_DEFINITIONS
            || definitions.definition_count() > MAX_CONTROL_DEFINITIONS
        {
            return Err(Failure::Owner(ExpressionControlFlowError::TooManyItems));
        }
        // The sole constructor geometry visitor also supplies the existing
        // cumulative pure model. It quotes no input payload or source bytes.
        creation::observe::<D, H>(regions.len(), uses.len(), temporary, retained, work)?;
        let (regions, uses, roots, references) =
            Self::parts_in(regions, uses, definitions, &mut |_| Ok(()), true, work)
                .map_err(Failure::from)?;
        work.flush().map_err(Failure::Control)?;
        let storage = retained.storage_lifetime();
        let regions = Arc::new(ControlFlowOwned {
            value: regions,
            _storage: storage.clone(),
        });
        work.step().map_err(Failure::Control)?;
        work.flush().map_err(Failure::Control)?;
        let uses = Arc::new(ControlFlowOwned {
            value: uses,
            _storage: storage.clone(),
        });
        work.step().map_err(Failure::Control)?;
        work.flush().map_err(Failure::Control)?;
        if roots.capacity() != roots.len() && !roots.is_empty() {
            retained
                .request_layout(
                    std::alloc::Layout::array::<ExpressionUseId>(roots.len()).map_err(|_| {
                        Failure::Resource(
                            crate::owned_resources::layout::LayoutResourceError::Arithmetic,
                        )
                    })?,
                )
                .map_err(Failure::Creator)?;
        }
        let roots = Arc::new(ControlFlowOwned {
            value: roots.into_boxed_slice(),
            _storage: storage,
        });
        work.step().map_err(Failure::Control)?;
        work.flush().map_err(Failure::Control)?;
        Ok(Self {
            regions,
            uses,
            roots,
            use_references: references,
        })
    }
    // Keep the original facade's loop observations; the caller-owned port
    // additionally admits geometry and brackets completed opaque operations.
    fn parts_in(
        regions: Vec<ExpressionEvaluationRegion>,
        uses: Vec<ExpressionInvocation<D>>,
        definitions: &impl ExpressionDefinitionMembership<D>,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        observed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FlowParts<D>, ExpressionControlFlowError> {
        let definition_count = definitions.definition_count();
        // Keep the original first header law ahead of source traversal.
        if regions.len() > MAX_CONTROL_DEFINITIONS
            || uses.len() > MAX_CONTROL_DEFINITIONS
            || definition_count > MAX_CONTROL_DEFINITIONS
        {
            return Err(ExpressionControlFlowError::TooManyItems);
        }
        if observed {
            preflight_flow(&regions, &uses, admit, work)?;
        }
        (|| {
            let mut region_index = BTreeMap::new();
            for region in regions {
                if observed {
                    work.flush()?;
                }
                let duplicate = region_index.insert(region.id, region).is_some();
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                if duplicate {
                    return Err(ExpressionControlFlowError::DuplicateIdentity);
                }
                if !observed {
                    work.step()?;
                }
            }
            let mut use_index = BTreeMap::new();
            let mut references = uses.len();
            if references > MAX_CONTROL_USE_REFERENCES {
                return Err(ExpressionControlFlowError::TooManyItems);
            }
            for value in uses {
                if observed {
                    work.flush()?;
                }
                references = references
                    .checked_add(value.arguments.len())
                    .ok_or(ExpressionControlFlowError::TooManyItems)?;
                if references > MAX_CONTROL_USE_REFERENCES {
                    return Err(ExpressionControlFlowError::TooManyItems);
                }
                if !definitions.contains_definition(value.definition)
                    || !region_index.contains_key(&value.context.region)
                {
                    return Err(ExpressionControlFlowError::InvalidReference);
                }
                if observed {
                    work.flush()?;
                }
                let duplicate = use_index.insert(value.context.use_id, value).is_some();
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                if duplicate {
                    return Err(ExpressionControlFlowError::DuplicateIdentity);
                }
                if !observed {
                    work.step()?;
                }
            }
            let mut children: BTreeMap<EvaluationRegionId, Vec<_>> = BTreeMap::new();
            let mut ready = VecDeque::new();
            reserve_deque(&mut ready, region_index.len(), observed, work)?;
            let mut depths = BTreeMap::new();
            for region in region_index.values() {
                if observed {
                    work.flush()?;
                }
                match (region.parent, region.guard) {
                    (None, None) => {
                        depths.insert(region.id, 1usize);
                        ready.push_back(region.id);
                    }
                    (Some(parent), Some(guard)) => {
                        let owner = use_index
                            .get(&guard.owner)
                            .ok_or(ExpressionControlFlowError::InvalidReference)?;
                        if parent != owner.context.region || !region_index.contains_key(&parent) {
                            return Err(ExpressionControlFlowError::InvalidGuard);
                        }
                        if observed {
                            work.flush()?;
                        }
                        let children = children.entry(parent).or_default();
                        children
                            .try_reserve(1)
                            .map_err(|_| CompileControlError::ResourceExhausted)?;
                        children.push(region.id);
                        if observed {
                            work.step()?;
                            work.flush()?;
                        }
                    }
                    _ => return Err(ExpressionControlFlowError::InvalidGuard),
                }
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            let mut visited = 0usize;
            loop {
                if observed {
                    work.flush()?;
                }
                let next = ready.pop_front();
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                let Some(parent) = next else { break };
                visited += 1;
                let depth = depths[&parent];
                if depth > MAX_CONTROL_DEPTH {
                    return Err(ExpressionControlFlowError::TooDeep);
                }
                for child in children.get(&parent).into_iter().flatten() {
                    if observed {
                        work.flush()?;
                    }
                    depths.insert(*child, depth + 1);
                    ready.push_back(*child);
                    work.step()?;
                    if observed {
                        work.flush()?;
                    }
                }
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            if visited != region_index.len() {
                return Err(ExpressionControlFlowError::Cycle);
            }
            let mut indegrees: BTreeMap<ExpressionUseId, usize> = BTreeMap::new();
            for id in use_index.keys() {
                if observed {
                    work.flush()?;
                }
                indegrees.insert(*id, 0);
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            let mut used_guards = BTreeMap::new();
            for owner in use_index.values() {
                if observed {
                    work.flush()?;
                }
                validate_arity(owner.control, owner.arguments.len())?;
                for (ordinal, argument) in owner.arguments.iter().enumerate() {
                    if observed {
                        work.flush()?;
                    }
                    let child = use_index
                        .get(argument)
                        .ok_or(ExpressionControlFlowError::InvalidReference)?;
                    if child.context.demand
                        != control_argument_demand(
                            owner.control,
                            owner.arguments.len(),
                            ordinal,
                            owner.context.demand,
                        )
                    {
                        return Err(ExpressionControlFlowError::InvalidDemand);
                    }
                    let expected = control_argument_guard(owner.control, ordinal);
                    let region = &region_index[&child.context.region];
                    match expected {
                        None if child.context.region == owner.context.region => {}
                        Some(kind)
                            if region.parent == Some(owner.context.region)
                                && region.guard
                                    == Some(RegionGuard {
                                        owner: owner.context.use_id,
                                        kind,
                                    }) =>
                        {
                            used_guards.insert(region.id, ());
                        }
                        _ => return Err(ExpressionControlFlowError::InvalidGuard),
                    }
                    let degree = indegrees.get_mut(argument).unwrap();
                    *degree += 1;
                    if *degree > 1 {
                        return Err(ExpressionControlFlowError::SharedUse);
                    }
                    work.step()?;
                    if observed {
                        work.flush()?;
                    }
                }
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            for region in region_index.values() {
                if observed {
                    work.flush()?;
                }
                if region.guard.is_some() && !used_guards.contains_key(&region.id) {
                    return Err(ExpressionControlFlowError::InvalidGuard);
                }
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            let mut ready = VecDeque::new();
            reserve_deque(&mut ready, use_index.len(), observed, work)?;
            let mut use_depths = BTreeMap::new();
            let mut roots = Vec::new();
            reserve_vec(&mut roots, use_index.len(), observed, work)?;
            for (id, degree) in &indegrees {
                if observed {
                    work.flush()?;
                }
                if *degree == 0 {
                    roots.push(*id);
                    ready.push_back(*id);
                    use_depths.insert(*id, 1usize);
                }
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            let mut visited = 0usize;
            loop {
                if observed {
                    work.flush()?;
                }
                let next = ready.pop_front();
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                let Some(id) = next else { break };
                visited += 1;
                let depth = use_depths[&id];
                if depth > MAX_CONTROL_DEPTH {
                    return Err(ExpressionControlFlowError::TooDeep);
                }
                for child in &use_index[&id].arguments {
                    if observed {
                        work.flush()?;
                    }
                    let degree = indegrees.get_mut(child).unwrap();
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push_back(*child);
                        use_depths.insert(*child, depth + 1);
                    }
                    work.step()?;
                    if observed {
                        work.flush()?;
                    }
                }
                work.step()?;
                if observed {
                    work.flush()?;
                }
            }
            if visited != use_index.len() {
                return Err(ExpressionControlFlowError::Cycle);
            }
            Ok((region_index, use_index, roots, references))
        })()
    }
    /// Every invocation with no parent argument edge, in stable ID order.
    /// A physical projection must bind these roots to exact operator sites.
    pub fn root_use_ids(&self) -> &[ExpressionUseId] {
        &self.roots
    }
    pub fn regions(&self) -> &BTreeMap<EvaluationRegionId, ExpressionEvaluationRegion> {
        &self.regions
    }
    pub fn uses(&self) -> &BTreeMap<ExpressionUseId, ExpressionInvocation<D>> {
        &self.uses
    }
    /// Invocation entries plus their ordered argument references, counted by
    /// the same bounded constructor. No unobserved edge scan is needed later.
    pub const fn use_reference_count(&self) -> usize {
        self.use_references
    }
}
type FlowParts<D> = (
    BTreeMap<EvaluationRegionId, ExpressionEvaluationRegion>,
    BTreeMap<ExpressionUseId, ExpressionInvocation<D>>,
    Vec<ExpressionUseId>,
    usize,
);
impl From<ControlResourceError> for ExpressionControlFlowError {
    fn from(e: ControlResourceError) -> Self {
        match e {
            ControlResourceError::Control(c) => Self::Control(c),
            ControlResourceError::SourceModel(m) => Self::SourceModel(m),
        }
    }
}
fn reserve_vec<T>(
    v: &mut Vec<T>,
    n: usize,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionControlFlowError> {
    if observed {
        w.flush()?;
    }
    v.try_reserve_exact(n)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if observed {
        w.step()?;
    }
    if observed {
        w.flush()?;
    }
    Ok(())
}
fn reserve_deque<T>(
    v: &mut VecDeque<T>,
    n: usize,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionControlFlowError> {
    if observed {
        w.flush()?;
    }
    v.try_reserve_exact(n)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    if observed {
        w.step()?;
    }
    if observed {
        w.flush()?;
    }
    Ok(())
}
/// Pure geometry of the original constructor's known containers. No graph
/// validation, traversal, allocation or observer; count-prefix work follows in
/// the same constructor before any source edge observation.
pub fn expression_control_flow_header_resource_facts<D>(
    n: usize,
    u: usize,
) -> Result<ControlOwnedResourceFacts, ControlResourceError> {
    let mut r = creation::resource_facts::<D>(n, u)?;
    let lookup = ControlResourceCounter::lookup_work(n.max(u))?;
    r.work(control_resource_mul(
        control_resource_mul(control_resource_add(n, u)?, 64)?,
        lookup,
    )?)?;
    Ok(r.facts())
}
/// Work for the actual known ordered edge prefix, shared with parent count.
pub fn expression_control_flow_edge_resource_facts(
    edges: usize,
    n: usize,
    u: usize,
) -> Result<ControlOwnedResourceFacts, ControlResourceError> {
    let lookup = ControlResourceCounter::lookup_work(n.max(u))?;
    Ok(ControlOwnedResourceFacts {
        cumulative_work_upper_bound: control_resource_mul(
            edges,
            control_resource_add(
                control_resource_mul(lookup, 64)?,
                std::mem::size_of::<ExpressionUseId>(),
            )?,
        )?,
        ..Default::default()
    })
}
fn preflight_flow<D>(
    regions: &[ExpressionEvaluationRegion],
    uses: &[ExpressionInvocation<D>],
    admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionControlFlowError> {
    let n = regions.len();
    let u = uses.len();
    let mut r = ControlResourceCounter::default();
    r.merge(expression_control_flow_header_resource_facts::<D>(n, u)?)?;
    admit(&r.facts())?;
    for invocation in uses {
        // Actual captured edge-header prefix, before its next observer.
        r.merge(expression_control_flow_edge_resource_facts(
            invocation.arguments.len(),
            n,
            u,
        )?)?;
        admit(&r.facts())?;
        w.step()?;
    }
    Ok(())
}

fn validate_arity(shape: ControlShape, count: usize) -> Result<(), ExpressionControlFlowError> {
    let valid = match shape {
        ControlShape::Eager => true,
        ControlShape::TypeOnly | ControlShape::NoArguments => count == 0,
        ControlShape::LambdaBody => count > 0,
        ControlShape::Conjunction | ControlShape::Disjunction => count > 0,
        ControlShape::If => count == 3,
        ControlShape::Coalesce => count > 0,
        ControlShape::Between { .. } => count == 4,
        ControlShape::Membership { .. } => count > 0,
        ControlShape::TemporalSource(shape) => count == shape.source_count(),
        ControlShape::HigherOrder { body_ordinal, .. } => (body_ordinal as usize) < count,
        ControlShape::Case {
            simple,
            arms,
            has_else,
        } => {
            arms > 0
                && (arms as usize).checked_mul(2).and_then(|count| {
                    count.checked_add(usize::from(simple) + usize::from(has_else))
                }) == Some(count)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ExpressionControlFlowError::InvalidControlShape)
    }
}
fn control_argument_guard(shape: ControlShape, ordinal: usize) -> Option<GuardKind> {
    match shape {
        ControlShape::Eager
        | ControlShape::TypeOnly
        | ControlShape::NoArguments
        | ControlShape::LambdaBody
        | ControlShape::Conjunction
        | ControlShape::Disjunction => None,
        ControlShape::If => match ordinal {
            0 => None,
            1 => Some(GuardKind::IfThen),
            _ => Some(GuardKind::IfElse),
        },
        ControlShape::Coalesce => (ordinal > 0).then_some(GuardKind::CoalesceAfterNull {
            ordinal: ordinal as u32,
        }),
        ControlShape::Between { .. } => (ordinal > 0).then(|| GuardKind::BetweenAfterSource {
            ordinal: (ordinal - 1) as u32,
        }),
        ControlShape::Membership { .. } => {
            (ordinal > 0).then(|| GuardKind::MembershipAfterSource {
                ordinal: (ordinal - 1) as u32,
            })
        }
        ControlShape::TemporalSource(source) => {
            if source == crate::TemporalSourceShape::SecondsCastOther && ordinal == 2 {
                Some(GuardKind::TemporalInvocationNull)
            } else if ordinal > 0 {
                Some(GuardKind::TemporalAfterSource {
                    ordinal: (ordinal - 1) as u32,
                })
            } else {
                None
            }
        }
        ControlShape::HigherOrder { body_ordinal, .. } => {
            (ordinal == body_ordinal as usize).then_some(GuardKind::LambdaInvocation)
        }
        ControlShape::Case { simple, arms, .. } => {
            if simple && ordinal == 0 {
                return None;
            }
            let ordinal = ordinal - usize::from(simple);
            if ordinal == arms as usize * 2 {
                Some(GuardKind::CaseElse)
            } else if ordinal.is_multiple_of(2) {
                Some(GuardKind::CaseWhen {
                    arm: (ordinal / 2) as u32,
                })
            } else {
                Some(GuardKind::CaseThen {
                    arm: (ordinal / 2) as u32,
                })
            }
        }
    }
}

fn control_argument_demand(
    shape: ControlShape,
    count: usize,
    ordinal: usize,
    owner: crate::EvaluationDemand,
) -> crate::EvaluationDemand {
    use crate::EvaluationDemand::{TruthOnly, Value};
    match shape {
        ControlShape::Eager
        | ControlShape::TypeOnly
        | ControlShape::NoArguments
        | ControlShape::Coalesce
        | ControlShape::TemporalSource(_)
        | ControlShape::Between { .. }
        | ControlShape::Membership { .. } => Value,
        ControlShape::Conjunction | ControlShape::Disjunction => owner,
        ControlShape::LambdaBody => {
            if ordinal + 1 == count {
                owner
            } else {
                Value
            }
        }
        ControlShape::If => {
            if ordinal == 0 {
                TruthOnly
            } else {
                owner
            }
        }
        ControlShape::HigherOrder {
            body_ordinal,
            body_demand,
        } => {
            if ordinal == body_ordinal as usize {
                body_demand
            } else {
                Value
            }
        }
        ControlShape::Case { simple, arms, .. } => {
            if simple && ordinal == 0 {
                return Value;
            }
            let ordinal = ordinal - usize::from(simple);
            if ordinal == arms as usize * 2 || ordinal % 2 == 1 {
                owner
            } else if simple {
                Value
            } else {
                TruthOnly
            }
        }
    }
}

/// Exact control semantics of one argument occurrence. Validate arity and
/// ordinal before deriving guards; callers cannot manufacture an else arm or
/// an out-of-range lambda/body use from an unchecked control shape.
pub fn control_argument_semantics(
    shape: ControlShape,
    argument_count: usize,
    ordinal: usize,
    owner: crate::EvaluationDemand,
) -> Result<(crate::EvaluationDemand, Option<GuardKind>), ExpressionControlFlowError> {
    if argument_count > MAX_CONTROL_USE_REFERENCES {
        return Err(ExpressionControlFlowError::TooManyItems);
    }
    validate_arity(shape, argument_count)?;
    if ordinal >= argument_count {
        return Err(ExpressionControlFlowError::InvalidReference);
    }
    Ok((
        control_argument_demand(shape, argument_count, ordinal, owner),
        control_argument_guard(shape, ordinal),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvaluationDemand, MAX_UNOBSERVED_COMPILE_WORK};
    use std::{cell::Cell, sync::Mutex};

    #[test]
    fn lambda_wrapper_preserves_body_demand_without_another_guard() {
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            assert_eq!(
                control_argument_semantics(ControlShape::LambdaBody, 1, 0, demand),
                Ok((demand, None))
            );
        }
        assert_eq!(
            control_argument_semantics(ControlShape::LambdaBody, 0, 0, EvaluationDemand::TruthOnly),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
        for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
            for ordinal in 0..3 {
                assert_eq!(
                    control_argument_semantics(ControlShape::LambdaBody, 3, ordinal, demand),
                    Ok((
                        if ordinal == 2 {
                            demand
                        } else {
                            EvaluationDemand::Value
                        },
                        None
                    ))
                );
            }
        }
        assert_eq!(
            control_argument_semantics(ControlShape::LambdaBody, 1, 1, EvaluationDemand::TruthOnly),
            Err(ExpressionControlFlowError::InvalidReference)
        );
    }

    struct Definitions {
        count: usize,
        lookups: Cell<usize>,
    }
    impl ExpressionDefinitionMembership<u32> for Definitions {
        fn definition_count(&self) -> usize {
            self.count
        }
        fn contains_definition(&self, id: u32) -> bool {
            self.lookups.set(self.lookups.get() + 1);
            matches!(id, 1000 | u32::MAX)
        }
    }
    struct Control {
        observations: Mutex<Vec<(CompilePhase, u32)>>,
        fail: Option<CompileControlError>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.observations.lock().unwrap().push((phase, units));
            if units > 0
                && let Some(failure) = self.fail.clone()
            {
                return Err(failure);
            }
            Ok(())
        }
    }
    fn control(fail: Option<CompileControlError>) -> Control {
        Control {
            observations: Mutex::default(),
            fail,
        }
    }
    fn definitions(count: usize) -> Definitions {
        Definitions {
            count,
            lookups: Cell::new(0),
        }
    }
    fn root() -> ExpressionEvaluationRegion {
        ExpressionEvaluationRegion {
            id: EvaluationRegionId::new(u32::MAX),
            parent: None,
            guard: None,
        }
    }
    fn invocation(id: u32, definition: u32) -> ExpressionInvocation<u32> {
        ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(id),
                region: root().id,
                demand: EvaluationDemand::Value,
            },
            definition,
            control: ControlShape::Eager,
            arguments: Box::default(),
        }
    }
    #[test]
    fn sparse_definition_membership_and_exact_roots_do_not_use_maximum_id() {
        let mut parent = invocation(50, u32::MAX);
        parent.arguments = vec![ExpressionUseId::new(3)].into_boxed_slice();
        let graph = ExpressionControlFlow::try_new(
            vec![root()],
            vec![parent, invocation(3, 1000), invocation(u32::MAX, 1000)],
            &definitions(2),
            CompilePhase::Validate,
            &control(None),
        )
        .unwrap();
        assert_eq!(
            graph.root_use_ids(),
            &[ExpressionUseId::new(50), ExpressionUseId::new(u32::MAX)]
        );
        assert_eq!(graph.uses().len(), 3);
        assert_eq!(graph.use_reference_count(), 4);
        assert_eq!(
            ExpressionControlFlow::try_new(
                vec![root()],
                vec![invocation(0, 1)],
                &definitions(2),
                CompilePhase::Validate,
                &control(None)
            )
            .unwrap_err(),
            ExpressionControlFlowError::InvalidReference
        );
    }
    #[test]
    fn excessive_definition_count_is_rejected_before_membership_lookup() {
        let definitions = definitions(MAX_CONTROL_DEFINITIONS + 1);
        assert_eq!(
            ExpressionControlFlow::try_new(
                vec![root()],
                vec![invocation(0, 1000)],
                &definitions,
                CompilePhase::Validate,
                &control(None)
            )
            .unwrap_err(),
            ExpressionControlFlowError::TooManyItems
        );
        assert_eq!(definitions.lookups.get(), 0);
    }
    #[test]
    fn uses_and_argument_edges_share_one_bounded_expansion_budget() {
        let count = MAX_CONTROL_USE_REFERENCES / 2;
        let mut parent = invocation(u32::MAX, 1000);
        parent.arguments = (0..count as u32).map(ExpressionUseId::new).collect();
        let mut uses = (0..count as u32)
            .map(|id| invocation(id, 1000))
            .collect::<Vec<_>>();
        uses.push(parent);
        let definitions = definitions(2);
        assert_eq!(
            ExpressionControlFlow::try_new(
                vec![root()],
                uses,
                &definitions,
                CompilePhase::Validate,
                &control(None)
            )
            .unwrap_err(),
            ExpressionControlFlowError::TooManyItems
        );
        // The oversized parent was rejected without walking its child edges.
        assert_eq!(definitions.lookups.get(), count);
    }
    #[test]
    fn control_failures_preserve_phase_and_bounded_work_classification() {
        for phase in [CompilePhase::Validate, CompilePhase::LowerProgram] {
            for failure in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let owner = control(Some(failure.clone()));
                let uses = (0..1000).map(|id| invocation(id, 1000)).collect();
                assert_eq!(
                    ExpressionControlFlow::try_new(
                        vec![root()],
                        uses,
                        &definitions(2),
                        phase,
                        &owner
                    )
                    .unwrap_err(),
                    ExpressionControlFlowError::Control(failure)
                );
                assert_eq!(
                    *owner.observations.lock().unwrap(),
                    vec![(phase, 0), (phase, MAX_UNOBSERVED_COMPILE_WORK)]
                );
            }
        }
    }
    #[test]
    fn unchecked_argument_shapes_and_ordinals_cannot_create_guard_regions() {
        assert_eq!(
            control_argument_semantics(ControlShape::If, 3, 3, EvaluationDemand::Value),
            Err(ExpressionControlFlowError::InvalidReference)
        );
        assert_eq!(
            control_argument_semantics(ControlShape::If, 4, 3, EvaluationDemand::Value),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
        assert_eq!(
            control_argument_semantics(
                ControlShape::Case {
                    simple: true,
                    arms: 1,
                    has_else: false
                },
                3,
                2,
                EvaluationDemand::TruthOnly
            ),
            Ok((
                EvaluationDemand::TruthOnly,
                Some(GuardKind::CaseThen { arm: 0 })
            ))
        );
        assert_eq!(
            control_argument_semantics(
                ControlShape::HigherOrder {
                    body_ordinal: 3,
                    body_demand: EvaluationDemand::Value
                },
                3,
                0,
                EvaluationDemand::Value
            ),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
        assert_eq!(
            control_argument_semantics(ControlShape::TypeOnly, 1, 0, EvaluationDemand::Value),
            Err(ExpressionControlFlowError::InvalidControlShape)
        );
    }

    #[path = "failure_tail_tests.rs"]
    mod failure_tail_tests;
}

#[cfg(test)]
#[path = "control_flow/owned_tests.rs"]
mod owned_tests;
