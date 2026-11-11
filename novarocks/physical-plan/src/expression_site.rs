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

//! Exact runtime expression root fields in one immutable fragment.
//!
//! Field indices are frozen-plan positions, never ordinals in a flattened
//! expression list. Proof references do not create invocation sites.

use crate::{
    AggregateCall, ExprUse, Fragment, FragmentId, JoinKind, JoinSide, NodeId, NodeKind, SortMode,
    TopNReduction, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlOwnedResourceFacts,
    ControlResourceCounter, ControlResourceError, EvaluationDemand, MAX_CONTROL_USE_REFERENCES,
    PureCompileControl, control_resource_add, control_resource_mul,
};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExpressionRootRole {
    ScanResidual {
        predicate: u32,
    },
    ScanDerived {
        derived: u32,
    },
    FilterPredicate {
        predicate: u32,
    },
    ProjectOutput {
        expression: u32,
    },
    AggregateGroup {
        group: u32,
    },
    AggregateArgument {
        call: u32,
        argument: u32,
    },
    AggregateOrder {
        call: u32,
        key: u32,
    },
    JoinKey {
        key: u32,
        side: JoinSide,
    },
    NaaValueEqualKey {
        key: u32,
        side: JoinSide,
    },
    NaaScopeKey {
        key: u32,
        side: JoinSide,
    },
    NaaEligibility,
    HashJoinResidual,
    NestLoopPredicate,
    SortOrder {
        key: u32,
    },
    SortPartition {
        key: u32,
    },
    TopNOrder {
        key: u32,
    },
    TopNGroup {
        group: u32,
    },
    TopNStateArgument {
        call: u32,
        argument: u32,
    },
    TopNStateOrder {
        call: u32,
        key: u32,
    },
    WindowPartition {
        key: u32,
    },
    WindowOrder {
        key: u32,
    },
    WindowCall {
        call: u32,
    },
    ValuesCell {
        row: u32,
        column: u32,
    },
    SeriesStart,
    SeriesStop,
    SeriesStep,
    TableFunctionArgument {
        argument: u32,
    },
    UnpivotConstant {
        mapping: u32,
        constant: u32,
    },
    ChangePredicate {
        event: u32,
    },
    /// Evaluated only for this event's exact selected rows, after its predicate.
    ChangeAssignment {
        event: u32,
        assignment: u32,
    },
    /// Evaluated only for the mapping selected by its exact write-target group.
    FinishUnpivotConstant {
        mapping: u32,
        constant: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExpressionRootSite {
    pub node: NodeId,
    pub role: ExpressionRootRole,
}

/// Complete root-field projection, including repeated uses of one definition.
/// This checks root ownership/demand; control and function-owner correspondence
/// remain separate compilation obligations. It owns no Selection or instances.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalExpressionRoots {
    fragment: FragmentId,
    sites: Arc<BTreeMap<ExpressionRootSite, ExprUse>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpressionRootError {
    Control(CompileControlError),
    TooManyRoots,
    InvalidDerivedValue,
    InvalidExpressionOwner,
    InvalidExpressionScope,
    DuplicateSite,
    SourceModel(&'static str),
}
impl fmt::Display for ExpressionRootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid physical expression root: {self:?}")
    }
}
impl std::error::Error for ExpressionRootError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for ExpressionRootError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl From<ControlResourceError> for ExpressionRootError {
    fn from(e: ControlResourceError) -> Self {
        match e {
            ControlResourceError::Control(c) => Self::Control(c),
            ControlResourceError::SourceModel(m) => Self::SourceModel(m),
        }
    }
}

// One collector grammar serves both entry policies. Original facades retain
// their loop-only observations; caller-owned ports also admit and bracket.
struct RootCollector<'a, 'w, 'c> {
    fragment: &'a Fragment,
    sites: BTreeMap<ExpressionRootSite, ExprUse>,
    work: &'w mut CompileCheckpoints<'c>,
    resources: ControlResourceCounter,
    observed: bool,
    admit: &'w mut dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
}
impl RootCollector<'_, '_, '_> {
    fn before(&mut self) -> Result<(), ExpressionRootError> {
        if !self.observed {
            return Ok(());
        }
        self.resources.work(control_resource_mul(
            ControlResourceCounter::lookup_work(
                self.fragment
                    .expressions()
                    .len()
                    .max(self.fragment.nodes().len()),
            )?,
            32,
        )?)?;
        (self.admit)(&self.resources.facts())?;
        self.work.flush()?;
        Ok(())
    }
    fn step(&mut self) -> Result<(), ExpressionRootError> {
        if !self.observed {
            return Ok(self.work.step()?);
        }
        let lookups = ControlResourceCounter::lookup_work(
            self.fragment
                .expressions()
                .len()
                .max(self.fragment.nodes().len()),
        )?;
        self.resources.work(control_resource_mul(lookups, 32)?)?;
        (self.admit)(&self.resources.facts())?;
        self.work.step()?;
        self.work.flush()?;
        Ok(())
    }
    fn add(
        &mut self,
        node: NodeId,
        role: ExpressionRootRole,
        expr: crate::ExprId,
        demand: EvaluationDemand,
    ) -> Result<(), ExpressionRootError> {
        if self.sites.len() == MAX_CONTROL_USE_REFERENCES {
            return Err(ExpressionRootError::TooManyRoots);
        }
        if self.observed {
            self.resources
                .tree_entry::<ExpressionRootSite, ExprUse>(control_resource_add(
                    self.sites.len(),
                    1,
                )?)?;
            self.resources.work(control_resource_mul(
                ControlResourceCounter::lookup_work(self.fragment.expressions().len())?,
                32,
            )?)?;
            (self.admit)(&self.resources.facts())?;
            self.work.flush()?;
        }
        let definition = self
            .fragment
            .expressions()
            .get(expr)
            .filter(|definition| definition.owner == node)
            .ok_or(ExpressionRootError::InvalidExpressionOwner)?;
        if definition.lambda_scope.is_some() {
            return Err(ExpressionRootError::InvalidExpressionScope);
        }
        let duplicate = self
            .sites
            .insert(ExpressionRootSite { node, role }, ExprUse { expr, demand })
            .is_some();
        if self.observed {
            self.work.step()?;
            self.work.flush()?;
        }
        if duplicate {
            return Err(ExpressionRootError::DuplicateSite);
        }
        if !self.observed {
            self.work.step()?;
        }
        Ok(())
    }
    fn calls(
        &mut self,
        node: NodeId,
        calls: &[AggregateCall],
        topn: bool,
    ) -> Result<(), ExpressionRootError> {
        use ExpressionRootRole::*;
        for (call, definition) in calls.iter().enumerate() {
            self.before()?;
            let call = ordinal(call)?;
            for (argument, expr) in definition.arguments.iter().enumerate() {
                self.before()?;
                let argument = ordinal(argument)?;
                self.add(
                    node,
                    if topn {
                        TopNStateArgument { call, argument }
                    } else {
                        AggregateArgument { call, argument }
                    },
                    *expr,
                    EvaluationDemand::Value,
                )?;
            }
            for (key, item) in definition.order_by.iter().enumerate() {
                self.before()?;
                let key = ordinal(key)?;
                self.add(
                    node,
                    if topn {
                        TopNStateOrder { call, key }
                    } else {
                        AggregateOrder { call, key }
                    },
                    item.expr,
                    EvaluationDemand::Value,
                )?;
            }
            self.step()?;
        }
        Ok(())
    }
}
fn ordinal(value: usize) -> Result<u32, ExpressionRootError> {
    u32::try_from(value).map_err(|_| ExpressionRootError::TooManyRoots)
}
fn join_demand(kind: JoinKind) -> EvaluationDemand {
    if matches!(kind, JoinKind::NullAwareLeftAnti | JoinKind::LeftMark) {
        EvaluationDemand::Value
    } else {
        EvaluationDemand::TruthOnly
    }
}

impl PhysicalExpressionRoots {
    pub fn try_new(
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExpressionRootError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let sites = Self::parts_in(fragment, &mut |_| Ok(()), false, &mut work);
        if let Err(ExpressionRootError::Control(c)) = &sites {
            return Err(c.clone().into());
        }
        work.finish()?;
        Ok(Self {
            fragment: fragment.id(),
            sites: Arc::new(sites?),
        })
    }
    /// The original root collector with caller-owned work and growing facts.
    /// No entry/footer; facts belong to this call only.
    pub fn try_new_in(
        fragment: &Fragment,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExpressionRootError> {
        let sites = Self::parts_in(fragment, admit, true, work)?;
        work.flush()?;
        let sites = Arc::new(sites);
        work.step()?;
        work.flush()?;
        Ok(Self {
            fragment: fragment.id(),
            sites,
        })
    }
    fn parts_in(
        fragment: &Fragment,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        observed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<BTreeMap<ExpressionRootSite, ExprUse>, ExpressionRootError> {
        use EvaluationDemand::{TruthOnly, Value};
        use ExpressionRootRole::*;
        let mut resources = ControlResourceCounter::default();
        if observed {
            resources.merge(physical_expression_roots_header_resource_facts(fragment)?)?;
            admit(&resources.facts())?;
        }
        let mut collector = RootCollector {
            fragment,
            sites: BTreeMap::new(),
            work,
            resources,
            observed,
            admit,
        };
        let collected = (|| {
            for node in fragment.nodes().values() {
                collector.before()?;
                let id = node.id;
                match &node.kind {
                    NodeKind::Scan {
                        residuals,
                        derived_values,
                        ..
                    } => {
                        for (predicate, expr) in residuals.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                ScanResidual {
                                    predicate: ordinal(predicate)?,
                                },
                                *expr,
                                TruthOnly,
                            )?;
                        }
                        for (derived, value) in derived_values.iter().enumerate() {
                            collector.before()?;
                            let Some(crate::ValueDef {
                                origin: ValueOrigin::Expr { node: owner, expr },
                                ..
                            }) = fragment.values().get(value)
                            else {
                                return Err(ExpressionRootError::InvalidDerivedValue);
                            };
                            if *owner != id {
                                return Err(ExpressionRootError::InvalidDerivedValue);
                            }
                            collector.add(
                                id,
                                ScanDerived {
                                    derived: ordinal(derived)?,
                                },
                                *expr,
                                Value,
                            )?;
                        }
                    }
                    NodeKind::Filter { predicates } => {
                        for (predicate, expr) in predicates.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                FilterPredicate {
                                    predicate: ordinal(predicate)?,
                                },
                                *expr,
                                TruthOnly,
                            )?;
                        }
                    }
                    NodeKind::Project { expressions } => {
                        for (expression, (expr, _)) in expressions.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                ProjectOutput {
                                    expression: ordinal(expression)?,
                                },
                                *expr,
                                Value,
                            )?;
                        }
                    }
                    NodeKind::Aggregate {
                        group_by, calls, ..
                    } => {
                        for (group, (expr, _)) in group_by.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                AggregateGroup {
                                    group: ordinal(group)?,
                                },
                                *expr,
                                Value,
                            )?;
                        }
                        collector.calls(id, calls, false)?;
                    }
                    NodeKind::HashJoin { condition, .. } => {
                        for (key, definition) in condition.keys().iter().enumerate() {
                            collector.before()?;
                            let key = ordinal(key)?;
                            for (side, expr) in [
                                (JoinSide::Left, definition.left),
                                (JoinSide::Right, definition.right),
                            ] {
                                let role = if condition.is_null_aware() {
                                    NaaValueEqualKey { key, side }
                                } else {
                                    JoinKey { key, side }
                                };
                                collector.add(id, role, expr, Value)?;
                            }
                        }
                        if let Some(keys) = condition.scope_keys() {
                            for (key, definition) in keys.iter().enumerate() {
                                collector.before()?;
                                let key = ordinal(key)?;
                                collector.add(
                                    id,
                                    NaaScopeKey {
                                        key,
                                        side: JoinSide::Left,
                                    },
                                    definition.left,
                                    Value,
                                )?;
                                collector.add(
                                    id,
                                    NaaScopeKey {
                                        key,
                                        side: JoinSide::Right,
                                    },
                                    definition.right,
                                    Value,
                                )?;
                            }
                        }
                        if let Some(expr) = condition.residual() {
                            let (role, demand) = if condition.is_null_aware() {
                                (NaaEligibility, Value)
                            } else {
                                (HashJoinResidual, TruthOnly)
                            };
                            collector.add(id, role, *expr, demand)?;
                        }
                    }
                    NodeKind::NestLoopJoin {
                        kind, predicate, ..
                    } => {
                        if let Some(expr) = predicate {
                            collector.add(id, NestLoopPredicate, *expr, join_demand(*kind))?;
                        }
                    }
                    NodeKind::Sort { order_by, mode } => {
                        for (key, item) in order_by.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                SortOrder { key: ordinal(key)? },
                                item.expr,
                                Value,
                            )?;
                        }
                        match mode {
                            SortMode::Global => {}
                            SortMode::Analytic { partition_by }
                            | SortMode::PartitionTopN { partition_by, .. } => {
                                for (key, item) in partition_by.iter().enumerate() {
                                    collector.before()?;
                                    collector.add(
                                        id,
                                        SortPartition { key: ordinal(key)? },
                                        item.expr,
                                        Value,
                                    )?;
                                }
                            }
                        }
                    }
                    NodeKind::TopN {
                        order_by,
                        reduction,
                        ..
                    } => {
                        for (key, item) in order_by.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                TopNOrder { key: ordinal(key)? },
                                item.expr,
                                Value,
                            )?;
                        }
                        if let TopNReduction::GroupedStates {
                            group_by, calls, ..
                        } = reduction
                        {
                            for (group, (expr, _)) in group_by.iter().enumerate() {
                                collector.before()?;
                                collector.add(
                                    id,
                                    TopNGroup {
                                        group: ordinal(group)?,
                                    },
                                    *expr,
                                    Value,
                                )?;
                            }
                            collector.calls(id, calls, true)?;
                        }
                    }
                    NodeKind::Window(spec) => {
                        for (key, item) in spec.partition_by.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                WindowPartition { key: ordinal(key)? },
                                item.expr,
                                Value,
                            )?;
                        }
                        for (key, item) in spec.order_by.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                WindowOrder { key: ordinal(key)? },
                                item.expr,
                                Value,
                            )?;
                        }
                        for (call, item) in spec.expressions.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                WindowCall {
                                    call: ordinal(call)?,
                                },
                                item.expression,
                                Value,
                            )?;
                        }
                    }
                    NodeKind::Values { rows } => {
                        for (row, cells) in rows.iter().enumerate() {
                            collector.before()?;
                            for (column, expr) in cells.iter().enumerate() {
                                collector.before()?;
                                collector.add(
                                    id,
                                    ValuesCell {
                                        row: ordinal(row)?,
                                        column: ordinal(column)?,
                                    },
                                    *expr,
                                    Value,
                                )?;
                            }
                            collector.step()?;
                        }
                    }
                    NodeKind::GenerateSeries { start, stop, step } => {
                        collector.add(id, SeriesStart, *start, Value)?;
                        collector.add(id, SeriesStop, *stop, Value)?;
                        if let Some(expr) = step {
                            collector.add(id, SeriesStep, *expr, Value)?;
                        }
                    }
                    NodeKind::TableFunction { arguments, .. } => {
                        for (argument, expr) in arguments.iter().enumerate() {
                            collector.before()?;
                            collector.add(
                                id,
                                TableFunctionArgument {
                                    argument: ordinal(argument)?,
                                },
                                *expr,
                                Value,
                            )?;
                        }
                    }
                    NodeKind::Unpivot { spec } => {
                        for (mapping, item) in spec.mappings.iter().enumerate() {
                            collector.before()?;
                            for (constant, item) in item.constants.iter().enumerate() {
                                collector.before()?;
                                if let crate::UnpivotConstant::Scalar(expr) = item {
                                    collector.add(
                                        id,
                                        ExpressionRootRole::UnpivotConstant {
                                            mapping: ordinal(mapping)?,
                                            constant: ordinal(constant)?,
                                        },
                                        *expr,
                                        Value,
                                    )?;
                                }
                                collector.step()?;
                            }
                            collector.step()?;
                        }
                    }
                    NodeKind::ChangeEventExpand { events, .. } => {
                        for (event, item) in events.iter().enumerate() {
                            collector.before()?;
                            let event = ordinal(event)?;
                            if let Some(expr) = item.predicate {
                                collector.add(id, ChangePredicate { event }, expr, TruthOnly)?;
                            }
                            for (assignment, (_, expr)) in item.assignments.iter().enumerate() {
                                collector.before()?;
                                if let Some(expr) = expr {
                                    collector.add(
                                        id,
                                        ChangeAssignment {
                                            event,
                                            assignment: ordinal(assignment)?,
                                        },
                                        *expr,
                                        Value,
                                    )?;
                                }
                                collector.step()?;
                            }
                            collector.step()?;
                        }
                    }
                    NodeKind::TableFinish(spec) => {
                        if let Some(unpivot) = &spec.grouped_unpivot {
                            for (mapping, item) in unpivot.mappings.iter().enumerate() {
                                collector.before()?;
                                for (constant, item) in item.constants.iter().enumerate() {
                                    collector.before()?;
                                    if let crate::UnpivotConstant::Scalar(expr) = item {
                                        collector.add(
                                            id,
                                            FinishUnpivotConstant {
                                                mapping: ordinal(mapping)?,
                                                constant: ordinal(constant)?,
                                            },
                                            *expr,
                                            Value,
                                        )?;
                                    }
                                    collector.step()?;
                                }
                                collector.step()?;
                            }
                        }
                    }
                    NodeKind::Limit { .. }
                    | NodeKind::SetOp { .. }
                    | NodeKind::Repeat { .. }
                    | NodeKind::AssertOneRow(_)
                    | NodeKind::ExchangeSource { .. }
                    | NodeKind::PositionShard(_)
                    | NodeKind::TableWriter { .. } => {}
                }
                collector.step()?;
            }
            Ok(())
        })();
        collected?;
        Ok(collector.sites)
    }
    pub const fn fragment(&self) -> FragmentId {
        self.fragment
    }
    pub fn sites(&self) -> &BTreeMap<ExpressionRootSite, ExprUse> {
        &self.sites
    }
}

/// Root-use binding and intrinsic definition correspondence in one fragment.
/// The pure compiler separately validates exact installed function controls
/// and operator/lifecycle row domains; an intrinsic shape proves neither.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalRootUses {
    roots: PhysicalExpressionRoots,
    flow: novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
    bindings: Arc<BTreeMap<ExpressionRootSite, novarocks_type_contract::ExpressionUseId>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootUseBindingError {
    Control(CompileControlError),
    IncompleteCoverage,
    InvalidSite,
    InvalidUse,
    DuplicateSite,
    SharedUse,
    WrongDefinition,
    WrongDemand,
    GuardedRoot,
    Roots(ExpressionRootError),
    WrongControl,
    WrongArguments,
    WrongFragment,
    ChangedRoots,
    SourceModel(&'static str),
}
impl fmt::Display for RootUseBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid physical root-use binding: {self:?}")
    }
}
impl std::error::Error for RootUseBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Roots(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for RootUseBindingError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl PhysicalRootUses {
    /// Recheck control correspondence against the current package fragment.
    /// Numeric IDs do not prove correspondence after a rewrite. The immutable
    /// graph's own guards and edges were checked by its constructor; roots,
    /// demand and ordered definitions are checked again here. This is not a
    /// content identity or effect proof: changing a literal's value or an eager
    /// operator while retaining these facts can remain valid. Accurate owner
    /// capabilities and effects must be checked for the current definitions.
    pub fn validate_fragment(
        &self,
        fragment: &Fragment,
        control: &dyn PureCompileControl,
    ) -> Result<(), RootUseBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let checked = self.validate_fragment_parts(fragment, &mut |_| Ok(()), false, &mut work);
        if let Err(RootUseBindingError::Control(c)) = &checked {
            return Err(c.clone().into());
        }
        work.finish()?;
        checked
    }
    pub fn validate_fragment_in(
        &self,
        fragment: &Fragment,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), RootUseBindingError> {
        self.validate_fragment_parts(fragment, admit, true, work)
    }
    fn validate_fragment_parts(
        &self,
        fragment: &Fragment,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        observed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), RootUseBindingError> {
        let mut resources = if observed {
            root_binding_resources(fragment, &self.flow, 0, false)?
        } else {
            ControlResourceCounter::default()
        };
        if observed {
            admit_root_header(&resources, fragment, admit)?;
        }
        let mut child = ControlOwnedResourceFacts::default();
        (|| {
            if self.roots.fragment != fragment.id() {
                return Err(RootUseBindingError::WrongFragment);
            }
            let roots = if observed {
                PhysicalExpressionRoots::try_new_in(
                    fragment,
                    &mut |facts| {
                        let mut total = ControlResourceCounter::default();
                        total
                            .merge(resources.facts())
                            .map_err(root_resource_control)?;
                        total.merge(*facts).map_err(root_resource_control)?;
                        child = *facts;
                        admit(&total.facts())
                    },
                    work,
                )
            } else {
                PhysicalExpressionRoots::try_new(fragment, work.control())
            }
            .map_err(|error| match error {
                ExpressionRootError::Control(error) => RootUseBindingError::Control(error),
                error => RootUseBindingError::Roots(error),
            })?;
            if observed {
                resources.merge(child)?;
                admit(&resources.facts())?;
            }
            if roots.sites.len() != self.roots.sites.len() {
                return Err(RootUseBindingError::ChangedRoots);
            }
            for (actual, checked) in roots.sites.iter().zip(self.roots.sites.iter()) {
                if observed {
                    work.flush()?;
                }
                if actual != checked {
                    return Err(RootUseBindingError::ChangedRoots);
                }
                work.step()?;
            }
            validate_definition_correspondence(fragment, &self.flow, observed, work)?;
            Ok(())
        })()
    }
    pub fn try_new(
        fragment: &Fragment,
        flow: novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
        bindings: Vec<(ExpressionRootSite, novarocks_type_contract::ExpressionUseId)>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, RootUseBindingError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let parts = Self::parts_in(fragment, flow, bindings, &mut |_| Ok(()), false, &mut work);
        if let Err(RootUseBindingError::Control(c)) = &parts {
            return Err(c.clone().into());
        }
        work.finish()?;
        let (roots, flow, sites) = parts?;
        Ok(Self {
            roots,
            flow,
            bindings: Arc::new(sites),
        })
    }
    pub fn try_new_in(
        fragment: &Fragment,
        flow: novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
        bindings: Vec<(ExpressionRootSite, novarocks_type_contract::ExpressionUseId)>,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, RootUseBindingError> {
        let (roots, flow, sites) = Self::parts_in(fragment, flow, bindings, admit, true, work)?;
        work.flush()?;
        let bindings = Arc::new(sites);
        work.step()?;
        work.flush()?;
        Ok(Self {
            roots,
            flow,
            bindings,
        })
    }
    fn parts_in(
        fragment: &Fragment,
        flow: novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
        bindings: Vec<(ExpressionRootSite, novarocks_type_contract::ExpressionUseId)>,
        admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        observed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<RootBindingParts, RootUseBindingError> {
        let mut resources = if observed {
            root_binding_resources(fragment, &flow, bindings.len(), true)?
        } else {
            ControlResourceCounter::default()
        };
        if observed {
            admit_root_header(&resources, fragment, admit)?;
        }
        let mut child = ControlOwnedResourceFacts::default();
        let checked = (|| {
            let roots = if observed {
                PhysicalExpressionRoots::try_new_in(
                    fragment,
                    &mut |facts| {
                        let mut total = ControlResourceCounter::default();
                        total
                            .merge(resources.facts())
                            .map_err(root_resource_control)?;
                        total.merge(*facts).map_err(root_resource_control)?;
                        child = *facts;
                        admit(&total.facts())
                    },
                    work,
                )
            } else {
                PhysicalExpressionRoots::try_new(fragment, work.control())
            }
            .map_err(|error| match error {
                ExpressionRootError::Control(error) => RootUseBindingError::Control(error),
                error => RootUseBindingError::Roots(error),
            })?;
            if observed {
                resources.merge(child)?;
                admit(&resources.facts())?;
            }
            if bindings.len() != roots.sites.len() || bindings.len() != flow.root_use_ids().len() {
                return Err(RootUseBindingError::IncompleteCoverage);
            }
            let mut sites = BTreeMap::new();
            let mut use_ids = std::collections::BTreeSet::new();
            for (site, id) in bindings {
                if observed {
                    work.flush()?;
                }
                let root = roots
                    .sites
                    .get(&site)
                    .ok_or(RootUseBindingError::InvalidSite)?;
                if flow.root_use_ids().binary_search(&id).is_err() {
                    return Err(RootUseBindingError::InvalidUse);
                }
                let invocation = &flow.uses()[&id];
                if root.expr != invocation.definition {
                    return Err(RootUseBindingError::WrongDefinition);
                }
                if root.demand != invocation.context.demand {
                    return Err(RootUseBindingError::WrongDemand);
                }
                let domain = &flow.domains()[&invocation.context.domain];
                if domain.parent.is_some() || domain.guard.is_some() {
                    return Err(RootUseBindingError::GuardedRoot);
                }
                let duplicate = sites.insert(site, id).is_some();
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                if duplicate {
                    return Err(RootUseBindingError::DuplicateSite);
                }
                let unique = use_ids.insert(id);
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                if !unique {
                    return Err(RootUseBindingError::SharedUse);
                }
                if !observed {
                    work.step()?;
                }
            }
            validate_definition_correspondence(fragment, &flow, observed, work)?;
            Ok((roots, sites))
        })();
        let (roots, sites) = checked?;
        Ok((roots, flow, sites))
    }
    /// Necessary occupied storage only; private tree node upper bounds are not
    /// retained backing. The caller supplies the true union source invoice.
    pub fn source_retained_floor(&self) -> Result<usize, ControlResourceError> {
        use std::mem::size_of;
        let flow = &self.flow;
        let mut n = size_of::<Self>();
        for (count, bytes) in [
            (
                flow.domains().len(),
                size_of::<novarocks_type_contract::EvaluationDomainId>()
                    + size_of::<novarocks_type_contract::ExpressionEvaluationDomain>(),
            ),
            (
                flow.uses().len(),
                size_of::<novarocks_type_contract::ExpressionUseId>()
                    + size_of::<novarocks_type_contract::ExpressionInvocation<crate::ExprId>>(),
            ),
            (
                flow.use_reference_count() - flow.uses().len(),
                size_of::<novarocks_type_contract::ExpressionUseId>(),
            ),
            (
                flow.root_use_ids().len(),
                size_of::<novarocks_type_contract::ExpressionUseId>(),
            ),
            (
                self.roots.sites.len(),
                size_of::<ExpressionRootSite>() + size_of::<ExprUse>(),
            ),
            (
                self.bindings.len(),
                size_of::<ExpressionRootSite>()
                    + size_of::<novarocks_type_contract::ExpressionUseId>(),
            ),
        ] {
            n = control_resource_add(n, control_resource_mul(count, bytes)?)?;
        }
        Ok(n)
    }
    pub const fn roots(&self) -> &PhysicalExpressionRoots {
        &self.roots
    }
    pub const fn flow(&self) -> &novarocks_type_contract::ExpressionControlFlow<crate::ExprId> {
        &self.flow
    }
    pub fn bindings(
        &self,
    ) -> &BTreeMap<ExpressionRootSite, novarocks_type_contract::ExpressionUseId> {
        &self.bindings
    }
}

/// Known root collector Arc/source-header geometry. The original 21-kind
/// collector owns the later actual map-entry and source-occurrence prefixes.
pub fn physical_expression_roots_header_resource_facts(
    fragment: &Fragment,
) -> Result<ControlOwnedResourceFacts, ControlResourceError> {
    let mut resources = ControlResourceCounter::default();
    resources.arc::<BTreeMap<ExpressionRootSite, ExprUse>>(1)?;
    resources.work(control_resource_mul(
        control_resource_add(fragment.nodes().len(), 1)?,
        control_resource_mul(
            ControlResourceCounter::lookup_work(fragment.expressions().len())?,
            64,
        )?,
    )?)?;
    Ok(resources.facts())
}
fn admit_root_header(
    resources: &ControlResourceCounter,
    fragment: &Fragment,
    admit: &mut impl FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
) -> Result<(), RootUseBindingError> {
    let mut total = ControlResourceCounter::default();
    total.merge(resources.facts())?;
    total.merge(physical_expression_roots_header_resource_facts(fragment)?)?;
    admit(&total.facts())?;
    Ok(())
}
type RootBindingParts = (
    PhysicalExpressionRoots,
    novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
    BTreeMap<ExpressionRootSite, novarocks_type_contract::ExpressionUseId>,
);
impl From<ControlResourceError> for RootUseBindingError {
    fn from(e: ControlResourceError) -> Self {
        match e {
            ControlResourceError::Control(c) => Self::Control(c),
            ControlResourceError::SourceModel(m) => Self::SourceModel(m),
        }
    }
}
fn root_resource_control(e: ControlResourceError) -> CompileControlError {
    match e {
        ControlResourceError::Control(c) => c,
        ControlResourceError::SourceModel(_) => {
            unreachable!("merging numerical facts does not inspect a source model")
        }
    }
}
/// Known retained binding map/set/Arc geometry, without root enumeration.
/// The actual root collector remains a separate mandatory original author.
pub fn root_use_binding_header_resource_facts(
    bindings: usize,
) -> Result<ControlOwnedResourceFacts, ControlResourceError> {
    let mut r = ControlResourceCounter::default();
    r.tree::<ExpressionRootSite, novarocks_type_contract::ExpressionUseId>(bindings)?;
    r.tree::<novarocks_type_contract::ExpressionUseId, ()>(bindings)?;
    r.arc::<BTreeMap<ExpressionRootSite, novarocks_type_contract::ExpressionUseId>>(1)?;
    Ok(r.facts())
}
fn root_binding_resources(
    fragment: &Fragment,
    flow: &novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
    bindings: usize,
    owned: bool,
) -> Result<ControlResourceCounter, RootUseBindingError> {
    let mut r = ControlResourceCounter::default();
    if owned {
        r.merge(root_use_binding_header_resource_facts(bindings)?)?;
    }
    let lookup = ControlResourceCounter::lookup_work(
        fragment
            .expressions()
            .len()
            .max(flow.uses().len())
            .max(flow.domains().len())
            .max(bindings),
    )?;
    r.work(control_resource_mul(
        control_resource_mul(
            control_resource_add(
                control_resource_add(flow.use_reference_count(), bindings)?,
                1,
            )?,
            64,
        )?,
        lookup,
    )?)?;
    Ok(r)
}

fn validate_definition_correspondence(
    fragment: &Fragment,
    flow: &novarocks_type_contract::ExpressionControlFlow<crate::ExprId>,
    observed: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), RootUseBindingError> {
    use crate::ExprKind;
    use novarocks_type_contract::ControlShape;
    for invocation in flow.uses().values() {
        if observed {
            work.flush()?;
        }
        let definition = fragment
            .expressions()
            .get(invocation.definition)
            .ok_or(RootUseBindingError::WrongDefinition)?;
        if invocation.context.demand == novarocks_type_contract::EvaluationDemand::TruthOnly
            && (*definition.ty.data_type() != arrow_schema::DataType::Boolean
                || definition.ty.logical_type()
                    != novarocks_type_contract::ValueLogicalType::Physical)
        {
            return Err(RootUseBindingError::WrongDemand);
        }
        let intrinsic = definition
            .kind
            .intrinsic_control_shape()
            .map_err(|_| RootUseBindingError::WrongArguments)?;
        if intrinsic.is_some_and(|expected| invocation.control != expected)
            || (intrinsic.is_none()
                && matches!(
                    invocation.control,
                    ControlShape::Conjunction
                        | ControlShape::Disjunction
                        | ControlShape::LambdaBody
                ))
        {
            return Err(RootUseBindingError::WrongControl);
        }
        let temporal =
            if let (ExprKind::FunctionCall { args, .. }, ControlShape::TemporalSource(shape)) =
                (&definition.kind, invocation.control)
            {
                let source = crate::temporal_source_definitions_observed(
                    shape.kind(),
                    fragment.expressions(),
                    args,
                    work,
                )
                .map_err(|error| match error {
                    crate::TemporalSourceProjectionError::Control(cause) => {
                        RootUseBindingError::Control(cause)
                    }
                    crate::TemporalSourceProjectionError::Invalid(_) => {
                        RootUseBindingError::WrongArguments
                    }
                })?;
                if source.facts.shape() != shape {
                    return Err(RootUseBindingError::WrongControl);
                }
                Some(source)
            } else {
                None
            };
        let mut ordinal = 0usize;
        if let Some(source) = &temporal {
            for definition in &source.definitions {
                let argument = invocation
                    .arguments
                    .get(ordinal)
                    .ok_or(RootUseBindingError::WrongArguments)?;
                if flow.uses()[argument].definition != *definition {
                    return Err(RootUseBindingError::WrongArguments);
                }
                ordinal += 1;
                work.step()?;
            }
        }
        // TypeOnly static arguments still belong to the checked definition,
        // but they are not runtime argument invocations.
        if temporal.is_none()
            && !matches!(
                (&definition.kind, invocation.control),
                (
                    ExprKind::FunctionCall { .. },
                    ControlShape::TypeOnly | ControlShape::NoArguments
                )
            )
        {
            definition
                .kind
                .invocation_references_observed(|definition| {
                    if observed {
                        work.flush()?;
                    } else {
                        work.step()?;
                    }
                    let argument = invocation
                        .arguments
                        .get(ordinal)
                        .ok_or(RootUseBindingError::WrongArguments)?;
                    if flow.uses()[argument].definition != definition {
                        return Err(RootUseBindingError::WrongArguments);
                    }
                    ordinal += 1;
                    if observed {
                        work.step()?;
                        work.flush()?;
                    }
                    Ok(())
                })?;
        }
        if ordinal != invocation.arguments.len() {
            return Err(RootUseBindingError::WrongArguments);
        }
        if let (ExprKind::FunctionCall { args, .. }, ControlShape::HigherOrder { body_ordinal, .. }) =
            (&definition.kind, invocation.control)
            && !args
                .get(body_ordinal as usize)
                .and_then(|id| fragment.expressions().get(*id))
                .is_some_and(|body| matches!(body.kind, ExprKind::Lambda { .. }))
        {
            return Err(RootUseBindingError::WrongArguments);
        }
        work.step()?;
        if observed {
            work.flush()?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "expression_site/owned_tests.rs"]
mod owned_tests;
