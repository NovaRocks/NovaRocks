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

//! Public scan semantics paired by ordinal with a provider-validated relation
//! recipe. Dynamic splits are Task assignments and are never stored here.

use crate::owned_copy::{ObservedCopy, OwnedCopy, PlainCopy};
use crate::{
    ConnectorError, ConnectorErrorKind, PureProviderCompileError, WriterOwnedResourceFacts,
};
use novarocks_type_contract::owned_resources::btree;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::sync::Arc;

use crate::{
    ConnectorExpression, ConnectorReadRelationRecipe, ConnectorReadRelationRecipeDraft,
    ConnectorValueType, TupleDomain,
};

const MAX_SCAN_NAME_BYTES: usize = 256;
/// Total retained static scan material, including the provider recipe and all
/// public predicate/value backing. Individual algebra limits are insufficient:
/// thousands of individually valid ranges can otherwise retain gigabytes.
pub const MAX_STATIC_SCAN_RETAINED_BYTES: usize = 16 * 1024 * 1024;

/// Ordinal into both `assignments` and the recipe's canonical column payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScanColumnId(usize);

impl ScanColumnId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticScanAssignment {
    variable: Arc<str>,
    value_type: ConnectorValueType,
}

impl StaticScanAssignment {
    pub fn new(variable: Arc<str>, value_type: ConnectorValueType) -> Self {
        Self {
            variable,
            value_type,
        }
    }

    pub fn variable(&self) -> &str {
        &self.variable
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.value_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticScanDynamicFilter {
    filter_id: u32,
    variable: Arc<str>,
}

impl StaticScanDynamicFilter {
    pub fn new(filter_id: u32, variable: Arc<str>) -> Self {
        Self {
            filter_id,
            variable,
        }
    }

    pub const fn filter_id(&self) -> u32 {
        self.filter_id
    }

    pub fn variable(&self) -> &str {
        &self.variable
    }
}

/// Shared public scan facts. The recipe type states whether private provider
/// validation has run; neither form owns a reader or other live capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorScan<R> {
    recipe: R,
    facts: Arc<ConnectorScanFacts>,
    retained_bytes: usize,
}

// Validated once. Provider canonicalization shares these immutable public
// facts instead of cloning/revalidating predicate and expression trees.
#[derive(Debug, Eq, PartialEq)]
struct ConnectorScanFacts {
    assignments: Arc<[StaticScanAssignment]>,
    enforced_predicate: TupleDomain<ScanColumnId>,
    unenforced_predicate: TupleDomain<ScanColumnId>,
    remaining_expression: Option<ConnectorExpression>,
    dynamic_filters: Arc<[StaticScanDynamicFilter]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticConnectorScanError {
    EmptyAssignments,
    RecipeColumnMismatch,
    InvalidVariable,
    DuplicateVariable,
    InvalidPredicateColumn,
    PredicateTypeMismatch,
    InvalidExpression,
    UnknownExpressionVariable,
    ExpressionTypeMismatch,
    DuplicateDynamicFilter,
    UnknownDynamicFilterVariable,
    TooManyRetainedBytes,
}

impl fmt::Display for StaticConnectorScanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static connector scan: {self:?}")
    }
}

impl std::error::Error for StaticConnectorScanError {}

/// Structurally checked frozen input carried by a physical FragmentPackage.
pub type FrozenConnectorScan = ConnectorScan<ConnectorReadRelationRecipeDraft>;
/// Provider-validated input consumed by a local program.
pub type StaticConnectorScan = ConnectorScan<ConnectorReadRelationRecipe>;

impl<R: AsRef<ConnectorReadRelationRecipeDraft>> ConnectorScan<R> {
    pub fn try_new(
        recipe: R,
        assignments: Vec<StaticScanAssignment>,
        enforced_predicate: TupleDomain<ScanColumnId>,
        unenforced_predicate: TupleDomain<ScanColumnId>,
        remaining_expression: Option<ConnectorExpression>,
        dynamic_filters: Vec<StaticScanDynamicFilter>,
    ) -> Result<Self, StaticConnectorScanError> {
        let input = ScanConstructionInput {
            recipe,
            assignments,
            enforced_predicate,
            unenforced_predicate,
            remaining_expression,
            dynamic_filters,
        };
        let retained = match validate_scan(&input, &mut PlainScan) {
            Ok(bytes) => bytes,
            Err(ScanValidationError::Contract(error)) => return Err(error),
            Err(ScanValidationError::Observer(never)) => match never {},
        };
        let remaining_expression = input.remaining_expression.as_ref().map(owned_expression);
        Ok(Self {
            recipe: input.recipe,
            facts: Arc::new(ConnectorScanFacts {
                assignments: input.assignments.into_boxed_slice().into(),
                enforced_predicate: input.enforced_predicate,
                unenforced_predicate: input.unenforced_predicate,
                remaining_expression,
                dynamic_filters: input.dynamic_filters.into_boxed_slice().into(),
            }),
            retained_bytes: retained,
        })
    }

    /// Original scan law and detached expression copy on the caller's scope.
    /// The source invoice covers all input/private recipe backing once. The
    /// necessary floor and numerical request bounds are not allocator grants.
    pub fn try_new_observed(
        input: ScanConstructionInput<R>,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ReadScanOwnedError>> {
        let mut context =
            ObservedCopy::new(source_retained_bytes, admit, work).map_err(lift_scan_resources)?;
        let retained = match validate_scan(&input, &mut ObservedScan(&mut context)) {
            Ok(bytes) => bytes,
            Err(ScanValidationError::Contract(error)) => {
                return Err(PureProviderCompileError::Provider(
                    ReadScanOwnedError::Contract(error),
                ));
            }
            Err(ScanValidationError::Observer(error)) => return Err(lift_scan_resources(error)),
        };
        if let Some(expression) = &input.remaining_expression {
            owned_expression_core(expression, &mut context, true).map_err(lift_scan_resources)?;
        }
        context.begin_copy().map_err(lift_scan_resources)?;
        let remaining_expression = input
            .remaining_expression
            .as_ref()
            .map(|expression| owned_expression_core(expression, &mut context, true))
            .transpose()
            .map_err(lift_scan_resources)?
            .flatten();
        context.flush().map_err(lift_scan_resources)?;
        let assignments: Box<[_]> = input.assignments.into_boxed_slice();
        context.step().map_err(lift_scan_resources)?;
        context.flush().map_err(lift_scan_resources)?;
        let assignments = Arc::from(assignments);
        context.step().map_err(lift_scan_resources)?;
        context.flush().map_err(lift_scan_resources)?;
        let dynamic_filters: Box<[_]> = input.dynamic_filters.into_boxed_slice();
        context.step().map_err(lift_scan_resources)?;
        context.flush().map_err(lift_scan_resources)?;
        let dynamic_filters = Arc::from(dynamic_filters);
        context.step().map_err(lift_scan_resources)?;
        context.flush().map_err(lift_scan_resources)?;
        let facts = Arc::new(ConnectorScanFacts {
            assignments,
            enforced_predicate: input.enforced_predicate,
            unenforced_predicate: input.unenforced_predicate,
            remaining_expression,
            dynamic_filters,
        });
        context.step().map_err(lift_scan_resources)?;
        context.flush().map_err(lift_scan_resources)?;
        Ok(Self {
            recipe: input.recipe,
            facts,
            retained_bytes: retained,
        })
    }

    pub const fn recipe(&self) -> &R {
        &self.recipe
    }

    pub fn assignments(&self) -> &[StaticScanAssignment] {
        &self.facts.assignments
    }

    pub fn enforced_predicate(&self) -> &TupleDomain<ScanColumnId> {
        &self.facts.enforced_predicate
    }

    pub fn unenforced_predicate(&self) -> &TupleDomain<ScanColumnId> {
        &self.facts.unenforced_predicate
    }

    pub fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.facts.remaining_expression.as_ref()
    }

    pub fn dynamic_filters(&self) -> &[StaticScanDynamicFilter] {
        &self.facts.dynamic_filters
    }

    /// The checked conservative charge computed from these frozen facts.
    /// A containing package must still enforce its cumulative budget.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

impl FrozenConnectorScan {
    /// The caller first checks exact canonical public headers. Only private
    /// recipe bytes change; validated public backing remains shared.
    pub(crate) fn try_replace_private_recipe(
        &self,
        recipe: ConnectorReadRelationRecipeDraft,
    ) -> Result<Self, StaticConnectorScanError> {
        let retained_bytes = self
            .retained_bytes
            .checked_sub(self.recipe.charged_bytes())
            .and_then(|bytes| bytes.checked_add(recipe.charged_bytes()))
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        if retained_bytes > MAX_STATIC_SCAN_RETAINED_BYTES {
            return Err(StaticConnectorScanError::TooManyRetainedBytes);
        }
        Ok(Self {
            recipe,
            facts: self.facts.clone(),
            retained_bytes,
        })
    }
}

/// The nine original scan inputs, moved without recreating provider facts.
pub struct ScanConstructionInput<R> {
    pub recipe: R,
    pub assignments: Vec<StaticScanAssignment>,
    pub enforced_predicate: TupleDomain<ScanColumnId>,
    pub unenforced_predicate: TupleDomain<ScanColumnId>,
    pub remaining_expression: Option<ConnectorExpression>,
    pub dynamic_filters: Vec<StaticScanDynamicFilter>,
}

#[derive(Debug)]
pub enum ReadScanOwnedError {
    Contract(StaticConnectorScanError),
    Resources(ConnectorError),
}
impl fmt::Display for ReadScanOwnedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(e) => e.fmt(f),
            Self::Resources(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for ReadScanOwnedError {}
fn lift_scan_resources(
    e: PureProviderCompileError<ConnectorError>,
) -> PureProviderCompileError<ReadScanOwnedError> {
    match e {
        PureProviderCompileError::Control(e) => PureProviderCompileError::Control(e),
        PureProviderCompileError::Provider(e) => {
            PureProviderCompileError::Provider(ReadScanOwnedError::Resources(e))
        }
    }
}
enum ScanValidationError<E> {
    Contract(StaticConnectorScanError),
    Observer(E),
}
impl<E> From<StaticConnectorScanError> for ScanValidationError<E> {
    fn from(e: StaticConnectorScanError) -> Self {
        Self::Contract(e)
    }
}
enum ExpressionValidationError<E> {
    Law,
    Observer(E),
}
impl<E> From<ConnectorError> for ExpressionValidationError<E> {
    fn from(_: ConnectorError) -> Self {
        Self::Law
    }
}
trait ScanOperations {
    type Error;
    fn prepare<R>(&mut self, _: &ScanConstructionInput<R>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn expression_node(&mut self, _: &ConnectorExpression) -> Result<(), Self::Error> {
        self.step()
    }
    fn step(&mut self) -> Result<(), Self::Error>;
    fn boundary(&mut self) -> Result<(), Self::Error> {
        self.step()
    }
    fn scratch<T>(&mut self, _: usize) -> Result<Vec<T>, Self::Error>;
}
struct PlainScan;
impl ScanOperations for PlainScan {
    type Error = Infallible;
    fn step(&mut self) -> Result<(), Infallible> {
        Ok(())
    }
    fn scratch<T>(&mut self, _: usize) -> Result<Vec<T>, Infallible> {
        Ok(Vec::new())
    }
}
struct ObservedScan<'a, O>(&'a mut O);
impl<O: OwnedCopy> ScanOperations for ObservedScan<'_, O> {
    type Error = O::Error;
    fn prepare<R>(&mut self, input: &ScanConstructionInput<R>) -> Result<(), Self::Error> {
        let c = &mut self.0;
        c.array::<StaticScanAssignment>(input.assignments.len(), 1)?;
        c.arc_slice::<StaticScanAssignment>(input.assignments.len())?;
        c.array::<StaticScanDynamicFilter>(input.dynamic_filters.len(), 1)?;
        c.arc_slice::<StaticScanDynamicFilter>(input.dynamic_filters.len())?;
        c.arc::<ConnectorScanFacts>()?;

        let base = c.add(
            size_of::<ScanConstructionInput<R>>(),
            c.add(
                c.mul(
                    input.assignments.capacity(),
                    size_of::<StaticScanAssignment>(),
                )?,
                c.mul(
                    input.dynamic_filters.capacity(),
                    size_of::<StaticScanDynamicFilter>(),
                )?,
            )?,
        )?;
        c.source_floor(base)?;
        for (facts, node) in [
            (
                btree::insertion_only::<&str, ConnectorValueType>(input.assignments.len()),
                btree::node_layout_typed::<&str, ConnectorValueType>(),
            ),
            (
                btree::insertion_only::<u32, ()>(input.dynamic_filters.len()),
                btree::node_layout_typed::<u32, ()>(),
            ),
        ] {
            let facts = facts.map_err(|e| match e {
                btree::BTreeResourceError::Arithmetic(_) => c.arithmetic(),
                btree::BTreeResourceError::SourceModel(message) => {
                    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
                }
            })?;
            if facts.allocation_requests_upper_bound != 0 {
                // Original locked BTree node layout, no private repr mirror.
                let layout = node.map_err(|e| match e {
                    btree::BTreeResourceError::Arithmetic(_) => c.arithmetic(),
                    btree::BTreeResourceError::SourceModel(message) => {
                        ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
                    }
                })?;
                c.request(layout, facts.allocation_requests_upper_bound)?;
            }
            c.work(facts.cumulative_work_upper_bound)?;
        }
        let lookup = btree::lookup_work_typed(input.assignments.len()).map_err(|e| match e {
            btree::BTreeResourceError::Arithmetic(_) => c.arithmetic(),
            btree::BTreeResourceError::SourceModel(message) => {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, message).into()
            }
        })?;
        let source = c.source_invoice().unwrap_or(0);
        // These moved Vecs may trim backing before their actual Arc copy.
        // Their whole-copy work is already known before any observation.
        let copy_work = c.add(c.mul(source, 2)?, 256)?;
        c.work(copy_work)?;
        // Closed &str comparisons cost at most the validated name extent.
        // source covers opaque original predicate iteration/destruction; the
        // bounded expression maximum covers validation before its count is known.
        let lookups = c.add(
            input.assignments.len(),
            c.add(
                input.dynamic_filters.len(),
                crate::predicate::MAX_CONNECTOR_EXPRESSION_NODES,
            )?,
        )?;
        c.work(c.add(
            c.mul(c.mul(lookup, 256)?, lookups)?,
            c.add(
                c.mul(source, 4)?,
                128 * crate::predicate::MAX_CONNECTOR_EXPRESSION_NODES,
            )?,
        )?)?;
        c.flush()
    }
    fn expression_node(&mut self, node: &ConnectorExpression) -> Result<(), Self::Error> {
        if let ConnectorExpression::Call { arguments, .. } = node {
            self.0.source_floor(
                self.0
                    .mul(arguments.capacity(), size_of::<ConnectorExpression>())?,
            )?;
        }
        owned_expression_header(node, self.0)?;
        self.0.step()
    }

    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.step()
    }
    fn boundary(&mut self) -> Result<(), Self::Error> {
        self.0.step()?;
        self.0.flush()
    }
    fn scratch<T>(&mut self, count: usize) -> Result<Vec<T>, Self::Error> {
        self.0.array::<T>(count, 1)?;
        self.0
            .work(self.0.add(self.0.mul(count, size_of::<T>())?, 64)?)?;
        reserve_scan(count, self.0)
    }
}
fn reserve_scan<T, O: OwnedCopy>(count: usize, c: &mut O) -> Result<Vec<T>, O::Error> {
    c.flush()?;
    let mut out = Vec::new();
    let result = out.try_reserve_exact(count);
    c.reserve_exit(result)?;
    Ok(out)
}

/// The sole original law, including its ordered ordinary failures. Plain
/// retains its original infallible scratch allocation; Observed prefunds it.
fn validate_scan<R: AsRef<ConnectorReadRelationRecipeDraft>, S: ScanOperations>(
    input: &ScanConstructionInput<R>,
    ops: &mut S,
) -> Result<usize, ScanValidationError<S::Error>> {
    use StaticConnectorScanError as E;
    let assignments = &input.assignments;
    if assignments.is_empty() {
        return Err(E::EmptyAssignments.into());
    }
    if assignments.len() != input.recipe.as_ref().columns().len() {
        return Err(E::RecipeColumnMismatch.into());
    }
    ops.prepare(input).map_err(ScanValidationError::Observer)?;
    let mut variables = BTreeMap::new();
    for assignment in assignments {
        if assignment.variable.is_empty() || assignment.variable.len() > MAX_SCAN_NAME_BYTES {
            return Err(E::InvalidVariable.into());
        }
        let previous = variables.insert(assignment.variable.as_ref(), assignment.value_type);
        ops.boundary().map_err(ScanValidationError::Observer)?;
        if previous.is_some() {
            return Err(E::DuplicateVariable.into());
        }
    }
    for (predicate, enforced) in [
        (&input.enforced_predicate, true),
        (&input.unenforced_predicate, false),
    ] {
        for column in predicate.columns() {
            ops.boundary().map_err(ScanValidationError::Observer)?;
            if column.index() >= assignments.len() {
                return Err(E::InvalidPredicateColumn.into());
            }
        }
        if let Some(domains) = predicate.domains() {
            for (column, domain) in domains {
                ops.boundary().map_err(ScanValidationError::Observer)?;
                let source = assignments[column.index()].value_type;
                let target = domain.value_type();
                let compatible = source == target
                    || (!enforced
                        && matches!(
                            (source, target),
                            (ConnectorValueType::Temporal(source), ConnectorValueType::Temporal(target))
                                if source.is_lossless_widening_to(target)
                        ));
                if !compatible {
                    return Err(E::PredicateTypeMismatch.into());
                }
            }
        }
    }
    let mut node_count = 0;
    if let Some(expression) = &input.remaining_expression {
        let result = expression.validate_observed(|node| {
            node_count += 1;
            ops.expression_node(node)
                .map_err(ExpressionValidationError::Observer)
        });
        match result {
            Ok(()) => {}
            Err(ExpressionValidationError::Law) => return Err(E::InvalidExpression.into()),
            Err(ExpressionValidationError::Observer(e)) => {
                return Err(ScanValidationError::Observer(e));
            }
        }
        let mut names = ops
            .scratch::<Arc<str>>(node_count)
            .map_err(ScanValidationError::Observer)?;
        expression
            .visit_variable_names_observed(&mut |name| {
                names.push(name.clone());
                ops.step()
            })
            .map_err(ScanValidationError::Observer)?;
        for name in &names {
            let present = variables.contains_key(name.as_ref());
            ops.boundary().map_err(ScanValidationError::Observer)?;
            if !present {
                return Err(E::UnknownExpressionVariable.into());
            }
        }
        let mut pending = ops
            .scratch::<&ConnectorExpression>(node_count)
            .map_err(ScanValidationError::Observer)?;
        pending.push(expression);
        while let Some(node) = pending.pop() {
            let mismatch = match node {
                ConnectorExpression::Variable { name, value_type } => {
                    variables.get(name.as_ref()) != Some(value_type)
                }
                ConnectorExpression::FieldDereference { target, .. } => {
                    pending.push(target);
                    false
                }
                ConnectorExpression::Call { arguments, .. } => {
                    pending.extend(arguments);
                    false
                }
                _ => false,
            };
            ops.boundary().map_err(ScanValidationError::Observer)?;
            if mismatch {
                return Err(E::ExpressionTypeMismatch.into());
            }
        }
    }
    let mut filter_ids = BTreeSet::new();
    for filter in &input.dynamic_filters {
        let inserted = filter_ids.insert(filter.filter_id);
        ops.boundary().map_err(ScanValidationError::Observer)?;
        if !inserted {
            return Err(E::DuplicateDynamicFilter.into());
        }
        let present = variables.contains_key(filter.variable.as_ref());
        ops.boundary().map_err(ScanValidationError::Observer)?;
        if !present {
            return Err(E::UnknownDynamicFilterVariable.into());
        }
    }
    let mut retained = input
        .recipe
        .as_ref()
        .charged_bytes()
        .checked_add(size_of::<ConnectorScanFacts>() + 2 * size_of::<usize>())
        .ok_or(E::TooManyRetainedBytes)?;
    for assignment in assignments {
        retained = retained
            .checked_add(size_of::<StaticScanAssignment>() + assignment.variable.len())
            .ok_or(E::TooManyRetainedBytes)?;
        ops.step().map_err(ScanValidationError::Observer)?;
    }
    for filter in &input.dynamic_filters {
        retained = retained
            .checked_add(size_of::<StaticScanDynamicFilter>() + filter.variable.len())
            .ok_or(E::TooManyRetainedBytes)?;
        ops.step().map_err(ScanValidationError::Observer)?;
    }
    retained = retained
        .checked_add(tuple_domain_bytes(&input.enforced_predicate, ops)?)
        .ok_or(E::TooManyRetainedBytes)?;
    retained = retained
        .checked_add(tuple_domain_bytes(&input.unenforced_predicate, ops)?)
        .ok_or(E::TooManyRetainedBytes)?;
    if let Some(expression) = &input.remaining_expression {
        retained = retained
            .checked_add(expression_bytes(expression, node_count, ops)?)
            .ok_or(E::TooManyRetainedBytes)?;
    }
    if retained > MAX_STATIC_SCAN_RETAINED_BYTES {
        return Err(E::TooManyRetainedBytes.into());
    }
    Ok(retained)
}
fn tuple_domain_bytes<S: ScanOperations>(
    domain: &TupleDomain<ScanColumnId>,
    ops: &mut S,
) -> Result<usize, ScanValidationError<S::Error>> {
    let mut total = size_of::<TupleDomain<ScanColumnId>>();
    if let Some(domains) = domain.domains() {
        for values in domains.values() {
            total = total
                .checked_add(size_of::<ScanColumnId>() + size_of_val(values))
                .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
            ops.boundary().map_err(ScanValidationError::Observer)?;
            for range in values.values().ranges() {
                total = total
                    .checked_add(size_of_val(range))
                    .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
                for bound in [range.low(), range.high()] {
                    total = total
                        .checked_add(bound.value().map_or(0, |v| v.payload_bytes()))
                        .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
                    ops.step().map_err(ScanValidationError::Observer)?;
                }
            }
        }
    }
    Ok(total)
}
fn expression_bytes<S: ScanOperations>(
    expression: &ConnectorExpression,
    nodes: usize,
    ops: &mut S,
) -> Result<usize, ScanValidationError<S::Error>> {
    let mut total = 0usize;
    let mut pending = ops
        .scratch::<&ConnectorExpression>(nodes)
        .map_err(ScanValidationError::Observer)?;
    pending.push(expression);
    while let Some(node) = pending.pop() {
        total = total
            .checked_add(size_of::<ConnectorExpression>())
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        let payload = match node {
            ConnectorExpression::Constant { value, .. } => {
                value.as_ref().map_or(0, |v| v.payload_bytes())
            }
            ConnectorExpression::Variable { name, .. } => name.len(),
            ConnectorExpression::FieldDereference { target, .. } => {
                pending.push(target);
                0
            }
            ConnectorExpression::Call {
                function,
                arguments,
                ..
            } => {
                pending.extend(arguments);
                function.as_str().len()
            }
        };
        total = total
            .checked_add(payload)
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        ops.step().map_err(ScanValidationError::Observer)?;
    }
    Ok(total)
}

// One bounded expression copy grammar for Count, Copy and the old Plain path.
fn owned_expression(expression: &ConnectorExpression) -> ConnectorExpression {
    // Plain context cannot fail on this already checked finite source: no
    // request model or fallible allocation is enabled for the old entry.
    owned_expression_core(expression, &mut PlainCopy, false)
        .expect("plain checked scan expression copy")
        .expect("plain scan expression materializes")
}
// This is the sole copy-header request grammar. The actual validator loans
// each source node to it before the next callback. Count/Copy then consume
// the already admitted header contributions without charging them again.
fn owned_expression_header<O: OwnedCopy>(
    expression: &ConnectorExpression,
    c: &mut O,
) -> Result<(), O::Error> {
    match expression {
        ConnectorExpression::FieldDereference { .. } => c.array::<ConnectorExpression>(1, 1)?,
        ConnectorExpression::Call { arguments, .. } => {
            c.array::<ConnectorExpression>(arguments.len(), 2)?
        }
        _ => {}
    }
    c.work(128)
}
fn owned_expression_core<O: OwnedCopy>(
    expression: &ConnectorExpression,
    c: &mut O,
    precharged: bool,
) -> Result<Option<ConnectorExpression>, O::Error> {
    if !precharged {
        owned_expression_header(expression, c)?;
    }
    let owned = match expression {
        ConnectorExpression::Constant { value, value_type } => {
            Some(ConnectorExpression::Constant {
                value: value.clone(),
                value_type: *value_type,
            })
        }
        ConnectorExpression::Variable { name, value_type } => Some(ConnectorExpression::Variable {
            name: name.clone(),
            value_type: *value_type,
        }),
        ConnectorExpression::FieldDereference {
            target,
            field_index,
            value_type,
        } => {
            let target = owned_expression_core(target, c, precharged)?;
            if c.materializes() {
                c.flush()?;
                let target = Box::new(target.ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "scan expression copy did not materialize",
                    )
                })?);
                c.step()?;
                c.flush()?;
                Some(ConnectorExpression::FieldDereference {
                    target,
                    field_index: *field_index,
                    value_type: *value_type,
                })
            } else {
                None
            }
        }
        ConnectorExpression::Call {
            function,
            value_type,
            arguments,
        } => {
            let mut out = if c.materializes() {
                Some(if c.source_invoice().is_some() {
                    reserve_scan(arguments.len(), c)?
                } else {
                    Vec::with_capacity(arguments.len())
                })
            } else {
                None
            };
            for argument in arguments {
                let argument = owned_expression_core(argument, c, precharged)?;
                if let Some(out) = &mut out {
                    out.push(argument.ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "scan expression copy did not materialize",
                        )
                    })?)
                }
                c.step()?;
            }
            if let Some(out) = out {
                c.flush()?;
                let out = out.into_boxed_slice().into_vec();
                c.step()?;
                c.flush()?;
                Some(ConnectorExpression::Call {
                    function: function.clone(),
                    value_type: *value_type,
                    arguments: out,
                })
            } else {
                None
            }
        }
    };
    c.step()?;
    if c.materializes() {
        Ok(owned)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
