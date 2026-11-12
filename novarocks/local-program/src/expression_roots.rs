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

//! Actual local operator, writer and sink expression entry sites. A site is a
//! static source-field occurrence, not a definition or permission to reorder
//! evaluation. Pipeline expansion and DOP still require separate actual
//! operator/driver instances; a shared node/site cannot own one global state.

use crate::creation::{ProgramCreation, ProgramCreationFailure};
use crate::{
    ImmutableExpressions, LocalProgramGraph, NestedLoopJoinType, ProgramControlFlow, ProgramExprId,
    ProgramNodeId, ProgramNodeKind, StaticExprKind, UnpivotConstant,
};
#[cfg(test)]
use novarocks_type_contract::ValueSchemaCreation;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    ExpressionUseId, MAX_CONTROL_USE_REFERENCES, PureCompileControl, ValueSchemaCreationScope,
    ValueTypeStorageLifetime,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramExpressionArena {
    Main,
    WriterProjection(ProgramNodeId),
    Sink,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramNodeExpressionRole {
    ProjectOutput {
        expression: u32,
    },
    UnpivotConstant {
        mapping: u32,
        constant: u32,
    },
    FilterPredicate {
        predicate: u32,
    },
    ScanResidual {
        predicate: u32,
    },
    RuntimeFilter {
        binding: u32,
    },
    ExchangePartition {
        key: u32,
    },
    AggregateGroup {
        group: u32,
    },
    /// Current logical arguments followed by exact function ORDER channels.
    AggregateInput {
        call: u32,
        argument: u32,
    },
    TopNGroup {
        group: u32,
    },
    TopNOrder {
        key: u32,
    },
    TopNStateArgument {
        call: u32,
        argument: u32,
    },
    JoinProbeKey {
        key: u32,
    },
    JoinBuildKey {
        key: u32,
    },
    JoinResidual,
    NaaValueProbeKey {
        key: u32,
    },
    NaaValueBuildKey {
        key: u32,
    },
    NaaScopeProbeKey {
        key: u32,
    },
    NaaScopeBuildKey {
        key: u32,
    },
    NaaEligibility,
    NestedLoopPredicate,
    WindowPartition {
        key: u32,
    },
    WindowOrder {
        key: u32,
    },
    WindowInput {
        call: u32,
        argument: u32,
    },
    FinishUnpivotConstant {
        mapping: u32,
        constant: u32,
    },
    ChangePredicate {
        event: u32,
    },
    ChangeAssignment {
        event: u32,
        assignment: u32,
    },
    SortOrder {
        key: u32,
    },
    SortPartition {
        key: u32,
    },
    /// One dynamic Values cell. It reads no input layout: the evaluator runs
    /// it once over an explicit empty port of one row.
    ValuesCell {
        row: u32,
        column: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ProgramExpressionRootSite {
    Node {
        node: ProgramNodeId,
        role: ProgramNodeExpressionRole,
    },
    WriterProjection {
        node: ProgramNodeId,
        expression: u32,
    },
    SinkPartition {
        branch: u32,
        key: u32,
    },
    SinkApplicability {
        branch: u32,
    },
    SinkSplitPredicate {
        branch: u32,
    },
}
impl ProgramExpressionRootSite {
    pub const fn arena(self) -> ProgramExpressionArena {
        match self {
            Self::Node { .. } => ProgramExpressionArena::Main,
            Self::WriterProjection { node, .. } => ProgramExpressionArena::WriterProjection(node),
            Self::SinkPartition { .. }
            | Self::SinkSplitPredicate { .. }
            | Self::SinkApplicability { .. } => ProgramExpressionArena::Sink,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramExpressionRoot {
    pub definition: ProgramExprId,
    pub demand: EvaluationDemand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramExpressionRootError {
    Control(CompileControlError),
    TooManyRoots,
    InvalidDefinition,
    InvalidArena,
    DuplicateSite,
}
impl fmt::Display for ProgramExpressionRootError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid local expression root: {self:?}")
    }
}
impl std::error::Error for ProgramExpressionRootError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}

/// One bounded complete projection of the real program fields. All scopes
/// share one root-reference budget. Immutable backing may be shared between
/// scopes, but their entry occurrences remain distinct. This checks source
/// identity and demand, not control domains, prepared types or runtime order.

#[derive(Debug)]
struct RootFacts<T> {
    value: T,
    _storage: ValueTypeStorageLifetime,
}
impl<T> std::ops::Deref for RootFacts<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<H> From<ProgramExpressionRootError> for ProgramCreationFailure<ProgramExpressionRootError, H> {
    fn from(error: ProgramExpressionRootError) -> Self {
        match error {
            ProgramExpressionRootError::Control(error) => Self::Control(error),
            error => Self::Owner(error),
        }
    }
}
fn pure_programexpressionrooterror(
    error: ProgramCreationFailure<ProgramExpressionRootError, std::convert::Infallible>,
) -> ProgramExpressionRootError {
    match error {
        ProgramCreationFailure::Control(error) => ProgramExpressionRootError::Control(error),
        ProgramCreationFailure::Owner(error) => error,
        ProgramCreationFailure::Resource(_) | ProgramCreationFailure::Capacity(_) => {
            ProgramExpressionRootError::Control(CompileControlError::ResourceExhausted)
        }
        ProgramCreationFailure::Creator(never) => match never {},
    }
}

impl<H> From<ProgramRootBindingError> for ProgramCreationFailure<ProgramRootBindingError, H> {
    fn from(error: ProgramRootBindingError) -> Self {
        match error {
            ProgramRootBindingError::Control(error) => Self::Control(error),
            error => Self::Owner(error),
        }
    }
}
fn pure_programrootbindingerror(
    error: ProgramCreationFailure<ProgramRootBindingError, std::convert::Infallible>,
) -> ProgramRootBindingError {
    match error {
        ProgramCreationFailure::Control(error) => ProgramRootBindingError::Control(error),
        ProgramCreationFailure::Owner(error) => error,
        ProgramCreationFailure::Resource(_) | ProgramCreationFailure::Capacity(_) => {
            ProgramRootBindingError::Control(CompileControlError::ResourceExhausted)
        }
        ProgramCreationFailure::Creator(never) => match never {},
    }
}
#[derive(Clone, Debug)]
pub struct ProgramExpressionRoots {
    arenas: Arc<RootFacts<BTreeMap<ProgramExpressionArena, Arc<ImmutableExpressions>>>>,
    sites: Arc<RootFacts<BTreeMap<ProgramExpressionRootSite, ProgramExpressionRoot>>>,
}
impl ProgramExpressionRoots {
    /// Pure collection delegates the same validation and construction body.
    pub fn collect(
        program: &LocalProgramGraph,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramExpressionRootError> {
        Self::collect_core::<std::convert::Infallible>(program, None, control)
            .map_err(pure_programexpressionrooterror)
    }
    pub fn collect_with_creator_in<H>(
        program: &LocalProgramGraph,
        creation: &mut dyn ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramExpressionRootError, H>> {
        Self::collect_core(program, Some(creation), control)
    }
    fn collect_core<H>(
        program: &LocalProgramGraph,
        creator: Option<&mut dyn ValueSchemaCreationScope<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramExpressionRootError, H>> {
        let mut builder = RootCollector {
            work: CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
                .map_err(ProgramExpressionRootError::Control)?,
            creation: ProgramCreation { creator },
            arena_insertions: 0,
            site_insertions: 0,
            arenas: BTreeMap::new(),
            sites: BTreeMap::new(),
        };
        builder.arena(ProgramExpressionArena::Main, program.expressions().clone())?;
        for (index, node) in program.nodes().iter().enumerate() {
            builder.step()?;
            builder.node(ProgramNodeId::new(index), node.kind())?;
        }
        if let Some(sink) = program.sink() {
            if let Some(arena) = sink.arena() {
                builder.arena(ProgramExpressionArena::Sink, arena.clone())?;
            }
            for (branch, value) in sink.branches().iter().enumerate() {
                builder.step()?;
                let branch = ordinal(branch)?;
                if let crate::StaticStreamRouting::ConditionalKeyV1 { applies } =
                    value.contract().routing
                {
                    builder.add(
                        ProgramExpressionRootSite::SinkApplicability { branch },
                        applies,
                        EvaluationDemand::TruthOnly,
                    )?;
                }
                for (key, definition) in value.partition_exprs().iter().enumerate() {
                    builder.add(
                        ProgramExpressionRootSite::SinkPartition {
                            branch,
                            key: ordinal(key)?,
                        },
                        *definition,
                        EvaluationDemand::Value,
                    )?;
                }
            }
            for (branch, definition) in sink.split_exprs().iter().enumerate() {
                builder.add(
                    ProgramExpressionRootSite::SinkSplitPredicate {
                        branch: ordinal(branch)?,
                    },
                    *definition,
                    EvaluationDemand::TruthOnly,
                )?;
            }
        }
        // Preserve the original tail control before publishing either map.
        builder.work.flush()?;
        let has_creator = builder.creation.creator.is_some();
        let storage = builder.creation.storage();
        let arenas = builder.creation.arc::<ProgramExpressionRootError, _>(
            RootFacts {
                value: builder.arenas,
                _storage: storage.clone(),
            },
            &mut builder.work,
        )?;
        let sites = builder.creation.arc::<ProgramExpressionRootError, _>(
            RootFacts {
                value: builder.sites,
                _storage: storage,
            },
            &mut builder.work,
        )?;
        if has_creator {
            builder.work.finish()?;
        }
        Ok(Self { arenas, sites })
    }
    pub fn arenas(&self) -> &BTreeMap<ProgramExpressionArena, Arc<ImmutableExpressions>> {
        &self.arenas
    }
    pub fn sites(&self) -> &BTreeMap<ProgramExpressionRootSite, ProgramExpressionRoot> {
        &self.sites
    }
}

fn ordinal(value: usize) -> Result<u32, ProgramExpressionRootError> {
    u32::try_from(value).map_err(|_| ProgramExpressionRootError::TooManyRoots)
}
struct RootCollector<'control, 'creation, H> {
    work: CompileCheckpoints<'control>,
    creation: ProgramCreation<'creation, H>,
    arena_insertions: usize,
    site_insertions: usize,
    arenas: BTreeMap<ProgramExpressionArena, Arc<ImmutableExpressions>>,
    sites: BTreeMap<ProgramExpressionRootSite, ProgramExpressionRoot>,
}
impl<H> RootCollector<'_, '_, H> {
    fn step(&mut self) -> Result<(), ProgramCreationFailure<ProgramExpressionRootError, H>> {
        self.work.step().map_err(ProgramCreationFailure::Control)
    }
    fn arena(
        &mut self,
        scope: ProgramExpressionArena,
        arena: Arc<ImmutableExpressions>,
    ) -> Result<(), ProgramCreationFailure<ProgramExpressionRootError, H>> {
        self.creation.before_tree_insert::<ProgramExpressionRootError, ProgramExpressionArena, Arc<ImmutableExpressions>>(self.arena_insertions, &mut self.work)?;
        self.arena_insertions += 1;
        self.arenas.insert(scope, arena);
        Ok(())
    }
    fn add(
        &mut self,
        site: ProgramExpressionRootSite,
        definition: ProgramExprId,
        demand: EvaluationDemand,
    ) -> Result<(), ProgramCreationFailure<ProgramExpressionRootError, H>> {
        self.step()?;
        if self.sites.len() >= MAX_CONTROL_USE_REFERENCES {
            return Err(ProgramExpressionRootError::TooManyRoots.into());
        }
        let arena = self
            .arenas
            .get(&site.arena())
            .ok_or(ProgramExpressionRootError::InvalidArena)?;
        if arena.node(definition).is_none() {
            return Err(ProgramExpressionRootError::InvalidDefinition.into());
        }
        self.creation.before_tree_insert::<ProgramExpressionRootError, ProgramExpressionRootSite, ProgramExpressionRoot>(self.site_insertions, &mut self.work)?;
        self.site_insertions += 1;
        if self
            .sites
            .insert(site, ProgramExpressionRoot { definition, demand })
            .is_some()
        {
            return Err(ProgramExpressionRootError::DuplicateSite.into());
        }
        Ok(())
    }
    fn node_root(
        &mut self,
        node: ProgramNodeId,
        role: ProgramNodeExpressionRole,
        definition: ProgramExprId,
        demand: EvaluationDemand,
    ) -> Result<(), ProgramCreationFailure<ProgramExpressionRootError, H>> {
        self.add(
            ProgramExpressionRootSite::Node { node, role },
            definition,
            demand,
        )
    }
    fn node(
        &mut self,
        node: ProgramNodeId,
        kind: &ProgramNodeKind,
    ) -> Result<(), ProgramCreationFailure<ProgramExpressionRootError, H>> {
        use EvaluationDemand::{TruthOnly, Value};
        use ProgramNodeExpressionRole as Role;
        match kind {
            ProgramNodeKind::SystemRelation { .. } => {}
            ProgramNodeKind::Values { values } => {
                // Constant cells are materialized backing; only dynamic cells
                // are runtime roots, in row-major order.
                for cell in values.dynamic_cells() {
                    self.node_root(
                        node,
                        Role::ValuesCell {
                            row: cell.row,
                            column: cell.column,
                        },
                        cell.definition,
                        Value,
                    )?;
                }
            }
            ProgramNodeKind::AssertNumRows { .. }
            | ProgramNodeKind::Repeat { .. }
            | ProgramNodeKind::UnionAll { .. }
            | ProgramNodeKind::PositionShard { .. }
            | ProgramNodeKind::Limit { .. }
            | ProgramNodeKind::TableFunction { .. }
            | ProgramNodeKind::GenerateSeries { .. }
            | ProgramNodeKind::SetOp { .. } => {}
            ProgramNodeKind::Project { exprs, .. } => {
                for (expression, definition) in exprs.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::ProjectOutput {
                            expression: ordinal(expression)?,
                        },
                        *definition,
                        Value,
                    )?;
                }
            }
            ProgramNodeKind::Unpivot { value_mappings, .. } => {
                for (mapping, value) in value_mappings.iter().enumerate() {
                    self.step()?;
                    for (constant, value) in value.constants.iter().enumerate() {
                        self.step()?;
                        if let UnpivotConstant::Scalar { expr_id, .. } = value {
                            self.node_root(
                                node,
                                Role::UnpivotConstant {
                                    mapping: ordinal(mapping)?,
                                    constant: ordinal(constant)?,
                                },
                                *expr_id,
                                Value,
                            )?;
                        }
                    }
                }
            }
            ProgramNodeKind::Filter { predicates, .. } => {
                if predicates.is_empty() {
                    return Err(ProgramExpressionRootError::InvalidDefinition.into());
                }
                for (predicate, definition) in predicates.iter().enumerate() {
                    self.step()?;
                    self.node_root(
                        node,
                        Role::FilterPredicate {
                            predicate: ordinal(predicate)?,
                        },
                        *definition,
                        TruthOnly,
                    )?;
                }
            }
            ProgramNodeKind::Scan {
                residuals,
                runtime_filters,
                ..
            } => {
                for (predicate, definition) in residuals.iter().enumerate() {
                    self.step()?;
                    self.node_root(
                        node,
                        Role::ScanResidual {
                            predicate: ordinal(predicate)?,
                        },
                        *definition,
                        TruthOnly,
                    )?;
                }
                for (binding, value) in runtime_filters.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::RuntimeFilter {
                            binding: ordinal(binding)?,
                        },
                        value.expr_id,
                        Value,
                    )?;
                }
            }
            ProgramNodeKind::ExchangeSource {
                hash_partition_exprs,
                runtime_filters,
                ..
            } => {
                for (key, definition) in hash_partition_exprs.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::ExchangePartition { key: ordinal(key)? },
                        *definition,
                        Value,
                    )?;
                }
                for (binding, value) in runtime_filters.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::RuntimeFilter {
                            binding: ordinal(binding)?,
                        },
                        value.expr_id,
                        Value,
                    )?;
                }
            }
            ProgramNodeKind::Aggregate {
                group_by,
                functions,
                ..
            } => {
                for (group, definition) in group_by.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::AggregateGroup {
                            group: ordinal(group)?,
                        },
                        *definition,
                        Value,
                    )?;
                }
                for (call, function) in functions.iter().enumerate() {
                    self.step()?;
                    for (argument, definition) in function.inputs.iter().enumerate() {
                        self.node_root(
                            node,
                            Role::AggregateInput {
                                call: ordinal(call)?,
                                argument: ordinal(argument)?,
                            },
                            *definition,
                            Value,
                        )?;
                    }
                }
                // TopN producers observe the already evaluated group array.
                // Their source expression is lineage, not another invocation.
            }
            ProgramNodeKind::GroupedTopN {
                group_by,
                order_by,
                functions,
                ..
            } => {
                for (group, id) in group_by.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::TopNGroup {
                            group: ordinal(group)?,
                        },
                        *id,
                        Value,
                    )?;
                }
                for (key, order) in order_by.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::TopNOrder { key: ordinal(key)? },
                        order.expr,
                        Value,
                    )?;
                }
                for (call, function) in functions.iter().enumerate() {
                    self.step()?;
                    for (argument, id) in function.inputs.iter().enumerate() {
                        self.node_root(
                            node,
                            Role::TopNStateArgument {
                                call: ordinal(call)?,
                                argument: ordinal(argument)?,
                            },
                            *id,
                            Value,
                        )?;
                    }
                }
            }
            ProgramNodeKind::Join {
                condition,
                runtime_filter_consumers,
                ..
            } => {
                for (key, definition) in condition.keys().iter().enumerate() {
                    let key = ordinal(key)?;
                    let role = if condition.is_null_aware() {
                        Role::NaaValueProbeKey { key }
                    } else {
                        Role::JoinProbeKey { key }
                    };
                    self.node_root(node, role, definition.probe, Value)?;
                }
                for (key, definition) in condition.keys().iter().enumerate() {
                    let key = ordinal(key)?;
                    let role = if condition.is_null_aware() {
                        Role::NaaValueBuildKey { key }
                    } else {
                        Role::JoinBuildKey { key }
                    };
                    self.node_root(node, role, definition.build, Value)?;
                }
                for (key, definition) in condition.scope_keys().iter().enumerate() {
                    let key = ordinal(key)?;
                    self.node_root(
                        node,
                        Role::NaaScopeProbeKey { key },
                        definition.probe,
                        Value,
                    )?;
                }
                for (key, definition) in condition.scope_keys().iter().enumerate() {
                    let key = ordinal(key)?;
                    self.node_root(
                        node,
                        Role::NaaScopeBuildKey { key },
                        definition.build,
                        Value,
                    )?;
                }
                if let Some(definition) = condition.residual_predicate() {
                    let (role, demand) = if condition.is_null_aware() {
                        (Role::NaaEligibility, Value)
                    } else {
                        (Role::JoinResidual, TruthOnly)
                    };
                    self.node_root(node, role, definition, demand)?;
                }
                for (binding, consumer) in runtime_filter_consumers.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::RuntimeFilter {
                            binding: ordinal(binding)?,
                        },
                        consumer.expr_id,
                        Value,
                    )?;
                }
                // Build-side RF producers observe the evaluated key arrays.
            }
            ProgramNodeKind::NestedLoopJoin {
                join_conjunct,
                join_type,
                ..
            } => {
                if let Some(definition) = join_conjunct {
                    self.node_root(
                        node,
                        Role::NestedLoopPredicate,
                        *definition,
                        if matches!(join_type, NestedLoopJoinType::NullAwareLeftAnti) {
                            Value
                        } else {
                            TruthOnly
                        },
                    )?;
                }
            }
            ProgramNodeKind::Analytic {
                partition_exprs,
                order_by_exprs,
                functions,
                ..
            } => {
                for (key, definition) in partition_exprs.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::WindowPartition { key: ordinal(key)? },
                        *definition,
                        Value,
                    )?;
                }
                for (key, definition) in order_by_exprs.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::WindowOrder { key: ordinal(key)? },
                        *definition,
                        Value,
                    )?;
                }
                for (call, function) in functions.iter().enumerate() {
                    self.step()?;
                    for (argument, definition) in function.args.iter().enumerate() {
                        self.node_root(
                            node,
                            Role::WindowInput {
                                call: ordinal(call)?,
                                argument: ordinal(argument)?,
                            },
                            *definition,
                            Value,
                        )?;
                    }
                }
            }
            ProgramNodeKind::RuntimeFilterConsumer { bindings, .. } => {
                for (binding, value) in bindings.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::RuntimeFilter {
                            binding: ordinal(binding)?,
                        },
                        value.expr_id,
                        Value,
                    )?;
                }
            }
            ProgramNodeKind::TableWriter { projection, .. } => {
                self.arena(
                    ProgramExpressionArena::WriterProjection(node),
                    projection.arena.clone(),
                )?;
                for (expression, definition) in projection.expressions.iter().enumerate() {
                    self.add(
                        ProgramExpressionRootSite::WriterProjection {
                            node,
                            expression: ordinal(expression)?,
                        },
                        *definition,
                        Value,
                    )?;
                }
            }
            ProgramNodeKind::TableFinish {
                final_aggregates, ..
            } => {
                if let Some(unpivot) = &final_aggregates.unpivot {
                    for (mapping, value) in unpivot.mappings.iter().enumerate() {
                        self.step()?;
                        for (constant, value) in value.constants.iter().enumerate() {
                            self.step()?;
                            if let UnpivotConstant::Scalar { expr_id, .. } = value {
                                self.node_root(
                                    node,
                                    Role::FinishUnpivotConstant {
                                        mapping: ordinal(mapping)?,
                                        constant: ordinal(constant)?,
                                    },
                                    *expr_id,
                                    Value,
                                )?;
                            }
                        }
                    }
                }
            }
            ProgramNodeKind::ChangeEventExpand { events, .. } => {
                for (event, value) in events.iter().enumerate() {
                    self.step()?;
                    if let Some(definition) = value.predicate {
                        self.node_root(
                            node,
                            Role::ChangePredicate {
                                event: ordinal(event)?,
                            },
                            definition,
                            TruthOnly,
                        )?;
                    }
                    for (assignment, value) in value.assignments.iter().enumerate() {
                        self.step()?;
                        if let Some(definition) = value.expr {
                            self.node_root(
                                node,
                                Role::ChangeAssignment {
                                    event: ordinal(event)?,
                                    assignment: ordinal(assignment)?,
                                },
                                definition,
                                Value,
                            )?;
                        }
                    }
                }
            }
            ProgramNodeKind::Sort {
                order_by,
                partition_exprs,
                ..
            } => {
                for (key, value) in order_by.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::SortOrder { key: ordinal(key)? },
                        value.expr,
                        Value,
                    )?;
                }
                for (key, value) in partition_exprs.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::SortPartition { key: ordinal(key)? },
                        value.expr,
                        Value,
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramRootUseBinding {
    pub site: ProgramExpressionRootSite,
    pub use_id: ExpressionUseId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgramRootBindingError {
    Roots(ProgramExpressionRootError),
    Control(CompileControlError),
    TooManyItems,
    InvalidArena,
    InvalidDefinition,
    WrongControl,
    WrongArguments,
    InvalidRoot,
    DuplicateSite,
    SharedRootUse,
    IncompleteCoverage,
}
impl fmt::Display for ProgramRootBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid local root control binding: {self:?}")
    }
}
impl std::error::Error for ProgramRootBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Roots(source) => Some(source),
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}

/// Root/control correspondence owned with the same actual program snapshot.
/// A caller cannot attach a receipt from another equally sized program or
/// supply an independent root table. This is a prerequisite for complete
/// resolved expression construction. Actual intrinsic shape and ordered
/// children are checked; types and exact function-owner effects are not. The
/// common invocation-entry plus ordered-argument budget applies across all
/// arena scopes; root-field references have their own common bounded table.
#[derive(Clone, Debug)]
pub struct ProgramRootControlBindings {
    program: LocalProgramGraph,
    roots: ProgramExpressionRoots,
    flows: Arc<RootFacts<BTreeMap<ProgramExpressionArena, ProgramControlFlow>>>,
    bindings: Arc<RootFacts<BTreeMap<ProgramExpressionRootSite, ExpressionUseId>>>,
}
impl ProgramRootControlBindings {
    pub fn try_new(
        program: LocalProgramGraph,
        flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
        bindings: Vec<ProgramRootUseBinding>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramRootBindingError> {
        Self::try_new_core::<std::convert::Infallible>(
            program, flows, bindings, None, None, control,
        )
        .map_err(pure_programrootbindingerror)
    }
    pub fn try_new_with_creator_in<H>(
        program: LocalProgramGraph,
        flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
        bindings: Vec<ProgramRootUseBinding>,
        temporary: &mut dyn ValueSchemaCreationScope<H>,
        retained: &mut dyn ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramRootBindingError, H>> {
        Self::try_new_core(
            program,
            flows,
            bindings,
            Some(temporary),
            Some(retained),
            control,
        )
    }
    fn try_new_core<H>(
        program: LocalProgramGraph,
        flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
        bindings: Vec<ProgramRootUseBinding>,
        temporary: Option<&mut dyn ValueSchemaCreationScope<H>>,
        mut creator: Option<&mut dyn ValueSchemaCreationScope<H>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCreationFailure<ProgramRootBindingError, H>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
            .map_err(ProgramRootBindingError::Control)?;
        let has_creator = creator.is_some();
        let roots = match creator.as_mut() {
            Some(creation) => {
                ProgramExpressionRoots::collect_with_creator_in(&program, &mut **creation, control)
            }
            None => ProgramExpressionRoots::collect_core(&program, None, control),
        }
        .map_err(|error| match error {
            ProgramCreationFailure::Owner(error) => {
                ProgramCreationFailure::Owner(ProgramRootBindingError::Roots(error))
            }
            ProgramCreationFailure::Control(error) if !has_creator => {
                ProgramCreationFailure::Owner(ProgramRootBindingError::Roots(
                    ProgramExpressionRootError::Control(error),
                ))
            }
            ProgramCreationFailure::Control(error) => ProgramCreationFailure::Control(error),
            ProgramCreationFailure::Creator(error) => ProgramCreationFailure::Creator(error),
            ProgramCreationFailure::Resource(error) => ProgramCreationFailure::Resource(error),
            ProgramCreationFailure::Capacity(error) => ProgramCreationFailure::Capacity(error),
        })?;
        let mut creation = ProgramCreation { creator };
        let (all_roots, sites) = {
            let mut temporary = ProgramCreation { creator: temporary };
            if flows.len() != roots.arenas.len() || bindings.len() != roots.sites.len() {
                return Err(ProgramRootBindingError::IncompleteCoverage.into());
            }
            let mut all_roots = BTreeSet::new();
            let mut root_insertions = 0usize;
            let mut reference_count = 0usize;
            for (scope, flow) in &flows {
                work.step().map_err(ProgramRootBindingError::Control)?;
                let arena = roots
                    .arenas
                    .get(scope)
                    .ok_or(ProgramRootBindingError::InvalidArena)?;
                reference_count = reference_count
                    .checked_add(flow.use_reference_count())
                    .filter(|value| *value <= MAX_CONTROL_USE_REFERENCES)
                    .ok_or(ProgramRootBindingError::TooManyItems)?;
                for value in flow.uses().values() {
                    work.step().map_err(ProgramRootBindingError::Control)?;
                    if arena.node(value.definition).is_none() {
                        return Err(ProgramRootBindingError::InvalidDefinition.into());
                    }
                }
                validate_intrinsic_correspondence(arena, flow, &mut work)?;
                for use_id in flow.root_use_ids() {
                    work.step().map_err(ProgramRootBindingError::Control)?;
                    temporary.before_tree_insert::<ProgramRootBindingError, (ProgramExpressionArena, ExpressionUseId), ()>(root_insertions, &mut work)?;
                    root_insertions += 1;
                    all_roots.insert((*scope, *use_id));
                }
            }
            let mut sites = BTreeMap::new();
            for (site_insertions, binding) in bindings.into_iter().enumerate() {
                work.step().map_err(ProgramRootBindingError::Control)?;
                let expected = roots
                    .sites
                    .get(&binding.site)
                    .ok_or(ProgramRootBindingError::InvalidRoot)?;
                let scope = binding.site.arena();
                let flow = flows
                    .get(&scope)
                    .ok_or(ProgramRootBindingError::InvalidArena)?;
                let invocation = flow
                    .uses()
                    .get(&binding.use_id)
                    .ok_or(ProgramRootBindingError::InvalidRoot)?;
                if invocation.definition != expected.definition
                    || invocation.context.demand != expected.demand
                {
                    return Err(ProgramRootBindingError::InvalidRoot.into());
                }
                creation.before_tree_insert::<ProgramRootBindingError, ProgramExpressionRootSite, ExpressionUseId>(site_insertions, &mut work)?;
                if sites.insert(binding.site, binding.use_id).is_some() {
                    return Err(ProgramRootBindingError::DuplicateSite.into());
                }
                if !all_roots.remove(&(scope, binding.use_id)) {
                    return Err(ProgramRootBindingError::SharedRootUse.into());
                }
            }
            if sites.len() != roots.sites.len() || !all_roots.is_empty() {
                return Err(ProgramRootBindingError::IncompleteCoverage.into());
            }
            (all_roots, sites)
        };
        // Scratch is released independently of the published maps.
        drop(all_roots);
        work.flush()?;
        let storage = creation.storage();
        let flows = creation.arc::<ProgramRootBindingError, _>(
            RootFacts {
                value: flows,
                _storage: storage.clone(),
            },
            &mut work,
        )?;
        let bindings = creation.arc::<ProgramRootBindingError, _>(
            RootFacts {
                value: sites,
                _storage: storage,
            },
            &mut work,
        )?;
        if has_creator {
            work.finish()?;
        }
        Ok(Self {
            program,
            roots,
            flows,
            bindings,
        })
    }
    pub const fn program(&self) -> &LocalProgramGraph {
        &self.program
    }
    pub const fn roots(&self) -> &ProgramExpressionRoots {
        &self.roots
    }
    pub fn flows(&self) -> &BTreeMap<ProgramExpressionArena, ProgramControlFlow> {
        &self.flows
    }
    pub fn bindings(&self) -> &BTreeMap<ProgramExpressionRootSite, ExpressionUseId> {
        &self.bindings
    }
}

/// Match actual ordered source children without creating a second expression
/// table. Function-call control belongs to the exact frozen implementation;
/// no legacy family name can establish it here.
fn validate_intrinsic_correspondence(
    arena: &ImmutableExpressions,
    flow: &ProgramControlFlow,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ProgramRootBindingError> {
    for invocation in flow.uses().values() {
        work.step().map_err(ProgramRootBindingError::Control)?;
        let definition = arena
            .node(invocation.definition)
            .ok_or(ProgramRootBindingError::InvalidDefinition)?;
        let expected = match definition.kind() {
            StaticExprKind::FunctionCall { .. } | StaticExprKind::BoundCall { .. } => continue,
            StaticExprKind::And(..) | StaticExprKind::NaryAnd { .. } => ControlShape::Conjunction,
            StaticExprKind::Or(..) | StaticExprKind::NaryOr { .. } => ControlShape::Disjunction,
            StaticExprKind::PreparedInList { is_not_in, .. } => ControlShape::Membership {
                negated: *is_not_in,
            },
            StaticExprKind::PreparedBetween { plan, .. } => ControlShape::Between {
                negated: plan.negated(),
            },
            StaticExprKind::LambdaFunction { .. } => ControlShape::LambdaBody,
            StaticExprKind::Case {
                has_case_expr,
                has_else_expr,
                children,
            } => {
                let pairs = children
                    .len()
                    .checked_sub(usize::from(*has_case_expr) + usize::from(*has_else_expr))
                    .filter(|count| *count >= 2 && count % 2 == 0)
                    .ok_or(ProgramRootBindingError::WrongArguments)?;
                ControlShape::Case {
                    simple: *has_case_expr,
                    arms: u32::try_from(pairs / 2)
                        .map_err(|_| ProgramRootBindingError::WrongArguments)?,
                    has_else: *has_else_expr,
                }
            }
            StaticExprKind::Constant(_)
            | StaticExprKind::Literal(_)
            | StaticExprKind::SlotId(_)
            | StaticExprKind::ArrayExpr { .. }
            | StaticExprKind::StructExpr { .. }
            | StaticExprKind::DictDecode { .. }
            | StaticExprKind::Cast(..)
            | StaticExprKind::CastTime(..)
            | StaticExprKind::CastTimeFromDatetime(..)
            | StaticExprKind::PreparedCast { .. }
            | StaticExprKind::PreparedNativeNegate(..)
            | StaticExprKind::PreparedNativeBitNot(..)
            | StaticExprKind::PreparedLike { .. }
            | StaticExprKind::PreparedArithmetic { .. }
            | StaticExprKind::Add(..)
            | StaticExprKind::Sub(..)
            | StaticExprKind::Mul(..)
            | StaticExprKind::Div(..)
            | StaticExprKind::Mod(..)
            | StaticExprKind::Eq(..)
            | StaticExprKind::EqForNull(..)
            | StaticExprKind::PreparedNullSafeComparison { .. }
            | StaticExprKind::Ne(..)
            | StaticExprKind::Lt(..)
            | StaticExprKind::Le(..)
            | StaticExprKind::Gt(..)
            | StaticExprKind::Ge(..)
            | StaticExprKind::Not(..)
            | StaticExprKind::IsNull(..)
            | StaticExprKind::IsNotNull(..)
            | StaticExprKind::In { .. }
            | StaticExprKind::Clone(..) => ControlShape::Eager,
        };
        if invocation.control != expected {
            return Err(ProgramRootBindingError::WrongControl);
        }
        let mut ordinal = 0usize;
        let mut child = |expected| -> Result<(), ProgramRootBindingError> {
            work.step().map_err(ProgramRootBindingError::Control)?;
            let use_id = invocation
                .arguments
                .get(ordinal)
                .ok_or(ProgramRootBindingError::WrongArguments)?;
            if flow.uses()[use_id].definition != expected {
                return Err(ProgramRootBindingError::WrongArguments);
            }
            ordinal += 1;
            Ok(())
        };
        match definition.kind() {
            StaticExprKind::Constant(_)
            | StaticExprKind::Literal(_)
            | StaticExprKind::SlotId(_) => {}
            StaticExprKind::ArrayExpr { elements }
            | StaticExprKind::NaryAnd { args: elements }
            | StaticExprKind::NaryOr { args: elements }
            | StaticExprKind::StructExpr { fields: elements }
            | StaticExprKind::Case {
                children: elements, ..
            } => {
                for definition in elements {
                    child(*definition)?;
                }
            }
            StaticExprKind::LambdaFunction {
                body,
                common_sub_exprs,
                ..
            } => {
                // Local computations execute in declaration order, then body.
                for (_, definition) in common_sub_exprs {
                    child(*definition)?;
                }
                child(*body)?;
            }
            StaticExprKind::PreparedCast {
                child: definition, ..
            }
            | StaticExprKind::DictDecode {
                child: definition, ..
            }
            | StaticExprKind::Cast(definition, _)
            | StaticExprKind::CastTime(definition, _)
            | StaticExprKind::CastTimeFromDatetime(definition, _)
            | StaticExprKind::PreparedNativeNegate(definition)
            | StaticExprKind::PreparedNativeBitNot(definition)
            | StaticExprKind::Not(definition)
            | StaticExprKind::IsNull(definition)
            | StaticExprKind::IsNotNull(definition)
            | StaticExprKind::Clone(definition) => child(*definition)?,
            StaticExprKind::PreparedLike {
                text: a,
                pattern: b,
                ..
            }
            | StaticExprKind::PreparedArithmetic {
                left: a, right: b, ..
            }
            | StaticExprKind::Add(a, b, _)
            | StaticExprKind::Sub(a, b, _)
            | StaticExprKind::Mul(a, b, _)
            | StaticExprKind::Div(a, b, _)
            | StaticExprKind::Mod(a, b, _)
            | StaticExprKind::Eq(a, b)
            | StaticExprKind::EqForNull(a, b)
            | StaticExprKind::PreparedNullSafeComparison { left: a, right: b }
            | StaticExprKind::Ne(a, b)
            | StaticExprKind::Lt(a, b)
            | StaticExprKind::Le(a, b)
            | StaticExprKind::Gt(a, b)
            | StaticExprKind::Ge(a, b)
            | StaticExprKind::And(a, b)
            | StaticExprKind::Or(a, b) => {
                child(*a)?;
                child(*b)?;
            }
            StaticExprKind::PreparedBetween {
                operand,
                low,
                high,
                plan,
            } => {
                for role in plan.sources() {
                    child(match role {
                        novarocks_type_contract::BetweenSourceRole::Operand => *operand,
                        novarocks_type_contract::BetweenSourceRole::Lower => *low,
                        novarocks_type_contract::BetweenSourceRole::Upper => *high,
                    })?;
                }
            }
            StaticExprKind::In {
                child: definition,
                values,
                ..
            }
            | StaticExprKind::PreparedInList {
                child: definition,
                values,
                ..
            } => {
                child(*definition)?;
                for definition in values {
                    child(*definition)?;
                }
            }
            StaticExprKind::FunctionCall { .. } | StaticExprKind::BoundCall { .. } => {
                unreachable!("frozen owner checks calls")
            }
        }
        if ordinal != invocation.arguments.len() {
            return Err(ProgramRootBindingError::WrongArguments);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod control_tests;

#[cfg(test)]
mod nary_tests;

#[cfg(test)]
mod values_cell_tests;

/// The frozen program site's index width is a representation limit, not an
/// allocator budget or a component invariant. The original standard refusal
/// remains available to every caller, including kernel and driver shells.
#[derive(Debug)]
pub struct ProgramOrdinalFailure {
    value: usize,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for ProgramOrdinalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "program site ordinal {} exceeds UInt32: {}",
            self.value, self.source
        )
    }
}
impl std::error::Error for ProgramOrdinalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ProgramOrdinalFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let code = std::num::NonZeroU16::new(30).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::LOCAL_PROGRAM, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::LOCAL_PROGRAM,
                code,
            },
            required: u64::try_from(self.value).ok(),
            extent_limit: Some(u32::MAX as u64),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ProgramOrdinalFailure {}
pub fn checked_program_ordinal(value: usize) -> Result<u32, ProgramOrdinalFailure> {
    u32::try_from(value).map_err(|source| ProgramOrdinalFailure { value, source })
}
#[cfg(test)]
mod ordinal_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn ordinal_format_boundary_retains_the_real_standard_source_without_memory_facts() {
        assert_eq!(checked_program_ordinal(0).unwrap(), 0);
        assert_eq!(
            checked_program_ordinal(u32::MAX as usize).unwrap(),
            u32::MAX
        );
        if usize::BITS > 32 {
            let value = u32::MAX as usize + 1;
            let error = checked_program_ordinal(value).unwrap_err();
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            let representation = fact.representation().unwrap();
            assert_eq!(representation.required, Some(value as u64));
            assert_eq!(representation.extent_limit, Some(u32::MAX as u64));
            assert_eq!(representation.representable_prefix, None);
            assert!(fact.resource().is_none());
            assert!(
                std::error::Error::source(&error)
                    .unwrap()
                    .is::<std::num::TryFromIntError>()
            );
        }
    }
}
