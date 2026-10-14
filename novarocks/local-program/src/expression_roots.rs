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

use crate::{
    ImmutableExpressions, JoinType, LocalProgramGraph, NestedLoopJoinType, ProgramControlFlow,
    ProgramExprId, ProgramNodeId, ProgramNodeKind, StaticExprKind, UnpivotConstant,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    ExpressionUseId, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
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
    SinkSplitPredicate {
        branch: u32,
    },
}
impl ProgramExpressionRootSite {
    pub const fn arena(self) -> ProgramExpressionArena {
        match self {
            Self::Node { .. } => ProgramExpressionArena::Main,
            Self::WriterProjection { node, .. } => ProgramExpressionArena::WriterProjection(node),
            Self::SinkPartition { .. } | Self::SinkSplitPredicate { .. } => {
                ProgramExpressionArena::Sink
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgramExpressionRoot {
    pub definition: ProgramExprId,
    pub demand: EvaluationDemand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
impl std::error::Error for ProgramExpressionRootError {}

/// One bounded complete projection of the real program fields. All scopes
/// share one root-reference budget. Immutable backing may be shared between
/// scopes, but their entry occurrences remain distinct. This checks source
/// identity and demand, not control domains, prepared types or runtime order.
#[derive(Clone, Debug)]
pub struct ProgramExpressionRoots {
    arenas: Arc<BTreeMap<ProgramExpressionArena, Arc<ImmutableExpressions>>>,
    sites: Arc<BTreeMap<ProgramExpressionRootSite, ProgramExpressionRoot>>,
}
impl ProgramExpressionRoots {
    pub fn collect(
        program: &LocalProgramGraph,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramExpressionRootError> {
        let mut builder = RootCollector {
            work: CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
                .map_err(ProgramExpressionRootError::Control)?,
            arenas: BTreeMap::from([(ProgramExpressionArena::Main, program.expressions().clone())]),
            sites: BTreeMap::new(),
        };
        for (index, node) in program.nodes().iter().enumerate() {
            builder.step()?;
            builder.node(ProgramNodeId::new(index), node.kind())?;
        }
        if let Some(sink) = program.sink() {
            if let Some(arena) = sink.arena() {
                builder
                    .arenas
                    .insert(ProgramExpressionArena::Sink, arena.clone());
            }
            for (branch, value) in sink.branches().iter().enumerate() {
                builder.step()?;
                let branch = ordinal(branch)?;
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
        builder
            .work
            .finish()
            .map_err(ProgramExpressionRootError::Control)?;
        Ok(Self {
            arenas: Arc::new(builder.arenas),
            sites: Arc::new(builder.sites),
        })
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
struct RootCollector<'control> {
    work: CompileCheckpoints<'control>,
    arenas: BTreeMap<ProgramExpressionArena, Arc<ImmutableExpressions>>,
    sites: BTreeMap<ProgramExpressionRootSite, ProgramExpressionRoot>,
}
impl RootCollector<'_> {
    fn step(&mut self) -> Result<(), ProgramExpressionRootError> {
        self.work
            .step()
            .map_err(ProgramExpressionRootError::Control)
    }
    fn add(
        &mut self,
        site: ProgramExpressionRootSite,
        definition: ProgramExprId,
        demand: EvaluationDemand,
    ) -> Result<(), ProgramExpressionRootError> {
        self.step()?;
        if self.sites.len() >= MAX_CONTROL_USE_REFERENCES {
            return Err(ProgramExpressionRootError::TooManyRoots);
        }
        let arena = self
            .arenas
            .get(&site.arena())
            .ok_or(ProgramExpressionRootError::InvalidArena)?;
        if arena.node(definition).is_none() {
            return Err(ProgramExpressionRootError::InvalidDefinition);
        }
        if self
            .sites
            .insert(site, ProgramExpressionRoot { definition, demand })
            .is_some()
        {
            return Err(ProgramExpressionRootError::DuplicateSite);
        }
        Ok(())
    }
    fn node_root(
        &mut self,
        node: ProgramNodeId,
        role: ProgramNodeExpressionRole,
        definition: ProgramExprId,
        demand: EvaluationDemand,
    ) -> Result<(), ProgramExpressionRootError> {
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
    ) -> Result<(), ProgramExpressionRootError> {
        use EvaluationDemand::{TruthOnly, Value};
        use ProgramNodeExpressionRole as Role;
        match kind {
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
                    return Err(ProgramExpressionRootError::InvalidDefinition);
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
                probe_keys,
                build_keys,
                residual_predicate,
                join_type,
                runtime_filter_consumers,
                ..
            } => {
                for (key, definition) in probe_keys.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::JoinProbeKey { key: ordinal(key)? },
                        *definition,
                        Value,
                    )?;
                }
                for (key, definition) in build_keys.iter().enumerate() {
                    self.node_root(
                        node,
                        Role::JoinBuildKey { key: ordinal(key)? },
                        *definition,
                        Value,
                    )?;
                }
                if let Some(definition) = residual_predicate {
                    self.node_root(
                        node,
                        Role::JoinResidual,
                        *definition,
                        if *join_type == JoinType::NullAwareLeftAnti {
                            Value
                        } else {
                            TruthOnly
                        },
                    )?;
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
                        if *join_type == NestedLoopJoinType::NullAwareLeftAnti {
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
                self.arenas.insert(
                    ProgramExpressionArena::WriterProjection(node),
                    projection.arena.clone(),
                );
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
impl std::error::Error for ProgramRootBindingError {}

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
    flows: Arc<BTreeMap<ProgramExpressionArena, ProgramControlFlow>>,
    bindings: Arc<BTreeMap<ProgramExpressionRootSite, ExpressionUseId>>,
}
impl ProgramRootControlBindings {
    pub fn try_new(
        program: LocalProgramGraph,
        flows: BTreeMap<ProgramExpressionArena, ProgramControlFlow>,
        bindings: Vec<ProgramRootUseBinding>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramRootBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
            .map_err(ProgramRootBindingError::Control)?;
        let roots = ProgramExpressionRoots::collect(&program, control)
            .map_err(ProgramRootBindingError::Roots)?;
        if flows.len() != roots.arenas.len() || bindings.len() != roots.sites.len() {
            return Err(ProgramRootBindingError::IncompleteCoverage);
        }
        let mut all_roots = BTreeSet::new();
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
                    return Err(ProgramRootBindingError::InvalidDefinition);
                }
            }
            validate_intrinsic_correspondence(arena, flow, &mut work)?;
            for use_id in flow.root_use_ids() {
                work.step().map_err(ProgramRootBindingError::Control)?;
                all_roots.insert((*scope, *use_id));
            }
        }
        let mut sites = BTreeMap::new();
        for binding in bindings {
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
                return Err(ProgramRootBindingError::InvalidRoot);
            }
            if sites.insert(binding.site, binding.use_id).is_some() {
                return Err(ProgramRootBindingError::DuplicateSite);
            }
            if !all_roots.remove(&(scope, binding.use_id)) {
                return Err(ProgramRootBindingError::SharedRootUse);
            }
        }
        if sites.len() != roots.sites.len() || !all_roots.is_empty() {
            return Err(ProgramRootBindingError::IncompleteCoverage);
        }
        work.finish().map_err(ProgramRootBindingError::Control)?;
        Ok(Self {
            program,
            roots,
            flows: Arc::new(flows),
            bindings: Arc::new(sites),
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
