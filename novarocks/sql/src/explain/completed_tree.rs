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

//! EXPLAIN over a completed plan, as the operator tree a reader expects.
//!
//! What EXPLAIN is for is seeing the operators: which relations are read, in
//! what order they are joined, where the rows are exchanged. That is what the
//! sealed planner printed and what plan-shape assertions are written against,
//! so it is what this prints -- from the completed plan, which is now where
//! those facts live. The plan contract itself -- value and expression
//! definitions, partition spaces, filter attachments, cut evidence -- is a
//! different question with its own level, in [`super::completed`].
//!
//! Names come from the plan's own display annotations. A value the statement
//! named is printed by that name; one it did not is printed by its identity,
//! which is the only honest thing to call it.

use std::fmt::{self, Display, Write as _};

use super::completed::{ExplainRenderBudget, ExplainRenderOutput, RenderDiagnostic};

use arrow::datatypes::DataType;
use novarocks_physical_plan::{
    AnnotationSubject, Distribution, ExprId, ExprKind, Fragment, FragmentId, FragmentSink,
    LiteralValue, NodeId, PhysicalNode, PhysicalPlan, PlanAnnotation, Relation, SortExpr, ValueId,
};

use crate::compiler::SqlCompileError;
use crate::explain::ExplainLevel;

/// One completed plan, printed as its operator tree.
pub fn render_completed_plan_tree(
    plan: &PhysicalPlan,
    level: ExplainLevel,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    render_tree_with_budget_observed(plan, level, ExplainRenderBudget::default(), control)
}

fn render_tree_with_budget_observed(
    plan: &PhysicalPlan,
    level: ExplainLevel,
    budget: ExplainRenderBudget,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Vec<String>, SqlCompileError> {
    let mut out = ExplainRenderOutput::new_observed(budget, control)?;
    let context = TreeContext::new_observed(plan, level, &mut out, Some(control))?;
    let result: Result<(), SqlCompileError> = (|| {
        if context.costs() {
            for annotation in plan.annotations() {
                if annotation.subject == AnnotationSubject::Plan
                    && annotation.key.as_ref()
                        == crate::optimizer::stats_input::TABLE_STATISTICS_ANNOTATION_KEY
                {
                    out.push(format_args!("{}", annotation.value))?;
                }
            }
        }
        context.render_runtime_filters(&mut out)?;
        for (display_id, fragment_id) in context.fragment_order().enumerate() {
            let Some(fragment) = plan.fragments().get(&fragment_id) else {
                continue;
            };
            if context.detailed() {
                out.push(format_args!("PLAN FRAGMENT {display_id}"))?;
                out.push(format_args!("  OUTPUT EXPRS: *"))?;
                out.push(format_args!(
                    "  STAGE PLACEMENT: {}",
                    super::completed::StagePlacementDisplay(fragment.stage_placement())
                ))?;
                let dop = fragment.dop_domain();
                out.push(format_args!(
                    "  DRIVER DOMAIN: min={}, max={}, power-of-two={}",
                    dop.min, dop.max, dop.requires_power_of_two
                ))?;
                let distribution = fragment
                    .nodes()
                    .get(&fragment.root())
                    .map_or(&Distribution::Unconstrained, |node| {
                        &node.output_properties.distribution
                    });
                out.push(format_args!(
                    "  PARTITION: {}",
                    context.distribution_label(fragment_id, distribution)
                ))?;
                context.render_sink(fragment, &mut out)?;
            }
            context.render_node(fragment, fragment.root(), 0, &mut out)?;
        }
        Ok(())
    })();
    if let Some(error) = context
        .diagnostic
        .as_ref()
        .and_then(RenderDiagnostic::take_error)
    {
        return Err(error);
    }
    result?;
    out.finish_observed()
}

#[cfg(test)]
fn render_tree_with_budget(
    plan: &PhysicalPlan,
    level: ExplainLevel,
    budget: ExplainRenderBudget,
) -> Result<Vec<String>, SqlCompileError> {
    render_tree_with_budget_observed(
        plan,
        level,
        budget,
        &crate::compiler::SqlCompileControl::unbounded(),
    )
}

fn fragment_order(plan: &PhysicalPlan) -> impl Iterator<Item = FragmentId> + '_ {
    let root = plan
        .result_port()
        .map(|port| port.fragment)
        .or_else(|| plan.fragments().keys().next().copied());
    root.into_iter().chain(
        plan.fragments()
            .keys()
            .copied()
            .filter(move |fragment| Some(*fragment) != root),
    )
}

struct Text<F>(F);
fn text<F>(render: F) -> Text<F>
where
    F: Fn(&mut fmt::Formatter<'_>) -> fmt::Result,
{
    Text(render)
}
impl<F> Display for Text<F>
where
    F: Fn(&mut fmt::Formatter<'_>) -> fmt::Result,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (self.0)(f)
    }
}
struct Joined<'a, T, F> {
    items: &'a [T],
    separator: &'static str,
    render: F,
}
fn joined<'a, T, F>(items: &'a [T], separator: &'static str, render: F) -> Joined<'a, T, F>
where
    F: Fn(&T, &mut fmt::Formatter<'_>) -> fmt::Result,
{
    Joined {
        items,
        separator,
        render,
    }
}
impl<T, F> Display for Joined<'_, T, F>
where
    F: Fn(&T, &mut fmt::Formatter<'_>) -> fmt::Result,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, item) in self.items.iter().enumerate() {
            if index != 0 {
                f.write_str(self.separator)?;
            }
            (self.render)(item, f)?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct ValueName<'a> {
    name: Option<&'a str>,
    value: ValueId,
}
impl ValueName<'_> {
    fn column(mut self) -> Self {
        self.name = self
            .name
            .map(|name| name.rsplit_once('.').map_or(name, |(_, column)| column));
        self
    }
}
impl Display for ValueName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name {
            Some(name) => f.write_str(name),
            None => write!(f, "v{}", self.value.get()),
        }
    }
}
#[derive(Clone, Copy)]
struct Indent(usize);
impl Display for Indent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.0 {
            f.write_str("  ")?;
        }
        Ok(())
    }
}
struct Uppercase<'a>(&'a str);
impl Display for Uppercase<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for character in self.0.chars().flat_map(char::to_uppercase) {
            f.write_char(character)?;
        }
        Ok(())
    }
}

/// Emit an expression once while comparing its emitted bytes with the alias.
/// The bounded output owns the only formatting pass; a refused write stops
/// comparison and expression traversal together.
struct ProjectExpressionDisplay<'a, D> {
    expression: D,
    name: ValueName<'a>,
}
impl<D: Display> Display for ProjectExpressionDisplay<'_, D> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fallback = [0u8; 16];
        let expected = match self.name.name {
            Some(name) => name,
            None => {
                // A value ID is u32, so its name is at most 11 ASCII bytes.
                struct Fixed<'a> {
                    bytes: &'a mut [u8],
                    len: usize,
                }
                impl fmt::Write for Fixed<'_> {
                    fn write_str(&mut self, value: &str) -> fmt::Result {
                        let end = self.len + value.len();
                        self.bytes
                            .get_mut(self.len..end)
                            .ok_or(fmt::Error)?
                            .copy_from_slice(value.as_bytes());
                        self.len = end;
                        Ok(())
                    }
                }
                let mut output = Fixed {
                    bytes: &mut fallback,
                    len: 0,
                };
                write!(&mut output, "{}", self.name)?;
                let len = output.len;
                std::str::from_utf8(&fallback[..len]).map_err(|_| fmt::Error)?
            }
        };
        struct ComparingWrite<'a, 'b, 'c> {
            formatter: &'a mut fmt::Formatter<'b>,
            remaining: Option<&'c str>,
        }
        impl fmt::Write for ComparingWrite<'_, '_, '_> {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                self.formatter.write_str(value)?;
                self.remaining = self
                    .remaining
                    .and_then(|remaining| remaining.strip_prefix(value));
                Ok(())
            }
        }
        let equal = {
            let mut output = ComparingWrite {
                formatter,
                remaining: Some(expected),
            };
            write!(&mut output, "{}", self.expression)?;
            output.remaining == Some("")
        };
        if !equal {
            write!(formatter, " AS {}", self.name)?;
        }
        Ok(())
    }
}

const MAX_TREE_DEPTH: usize = 128;
const SOURCE_INDEX_BYTES: usize = 4 * 1024 * 1024;

fn source_refusal() -> SqlCompileError {
    SqlCompileError::InvalidRequest(
        "EXPLAIN tree exceeds its source workspace or depth bound".into(),
    )
}

// One key-sorted slot per node. The traversal state also detects a back edge
// without allocating a pending input queue or an ancestor set.
struct DisplayNode {
    key: (FragmentId, NodeId),
    display: usize,
    active: bool,
}

struct TreeContext<'a> {
    plan: &'a PhysicalPlan,
    level: ExplainLevel,
    // Stable sorting preserves the old first node annotation / last value
    // annotation rules. The strings remain borrowed from the frozen plan.
    node_annotations: Vec<&'a PlanAnnotation>,
    value_names: Vec<&'a PlanAnnotation>,
    display_ids: Vec<DisplayNode>,
    diagnostic: Option<RenderDiagnostic<'a>>,
}

fn annotation_key(annotation: &PlanAnnotation) -> (FragmentId, u32) {
    match annotation.subject {
        AnnotationSubject::Node(fragment, node) => (fragment, node.get()),
        AnnotationSubject::Value(fragment, value) => (fragment, value.get()),
        _ => unreachable!("only node/value annotations enter their indexes"),
    }
}

fn reserve_exact<T>(count: usize) -> Result<Vec<T>, SqlCompileError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| source_refusal())?;
    if values.capacity() != count {
        return Err(source_refusal());
    }
    Ok(values)
}

fn assign_display_ids<'a>(
    entries: &mut [DisplayNode],
    fragment: FragmentId,
    node: NodeId,
    depth: usize,
    next: &mut usize,
    inputs: &impl Fn(NodeId) -> Option<&'a [NodeId]>,
) -> Result<(), SqlCompileError> {
    if depth > MAX_TREE_DEPTH {
        return Err(source_refusal());
    }
    let Ok(index) = entries.binary_search_by_key(&(fragment, node), |entry| entry.key) else {
        return Ok(());
    };
    if entries[index].active {
        return Err(source_refusal());
    }
    if entries[index].display != usize::MAX {
        // Preserve the original DFS numbering of a shared input: its latest
        // encounter names it with the current next ID, without expanding it
        // again. Cycle detection remains independent of this display rule.
        entries[index].display = *next;
        return Ok(());
    }
    entries[index].display = *next;
    entries[index].active = true;
    *next += 1;
    if let Some(inputs_for_node) = inputs(node) {
        for input in inputs_for_node {
            assign_display_ids(entries, fragment, *input, depth + 1, next, inputs)?;
        }
    }
    entries[index].active = false;
    Ok(())
}

impl<'a> TreeContext<'a> {
    #[cfg(test)]
    fn new(plan: &'a PhysicalPlan, level: ExplainLevel) -> Result<Self, SqlCompileError> {
        let mut out = ExplainRenderOutput::new(ExplainRenderBudget::default());
        Self::new_observed(plan, level, &mut out, None)
    }

    fn new_observed(
        plan: &'a PhysicalPlan,
        level: ExplainLevel,
        out: &mut ExplainRenderOutput<'_>,
        diagnostic_control: Option<&'a dyn novarocks_type_contract::PureCompileControl>,
    ) -> Result<Self, SqlCompileError> {
        let mut node_count = 0usize;
        for fragment in plan.fragments().values() {
            out.observe()?;
            node_count = node_count
                .checked_add(fragment.nodes().len())
                .ok_or_else(source_refusal)?;
        }
        let mut nodes = 0usize;
        let mut values = 0usize;
        for annotation in plan.annotations() {
            out.observe()?;
            match annotation.subject {
                AnnotationSubject::Node(..) => nodes += 1,
                AnnotationSubject::Value(..) if annotation.key.as_ref() == "sql.display_name" => {
                    values += 1
                }
                _ => {}
            }
        }
        // Two reference capacities include stable-sort scratch. All indexing
        // is admitted before allocating. Together with the output collector's
        // <=16 MiB payload/copy peak, <=3 MiB old/new line headers and <=4 MiB
        // per-line allocation allowance, this 4 MiB index leaves the 32 MiB
        // source workspace room for fixed depth stacks and formatter adapters.
        let bytes = nodes
            .checked_add(values)
            .and_then(|n| n.checked_mul(2 * size_of::<&PlanAnnotation>()))
            .and_then(|n| {
                node_count
                    .checked_mul(size_of::<DisplayNode>())
                    .and_then(|m| n.checked_add(m))
            })
            .ok_or_else(source_refusal)?;
        if bytes > SOURCE_INDEX_BYTES || node_count > 65_536 {
            return Err(source_refusal());
        }
        let mut node_annotations = reserve_exact(nodes)?;
        let mut value_names = reserve_exact(values)?;
        for annotation in plan.annotations() {
            out.observe()?;
            match annotation.subject {
                AnnotationSubject::Node(..) => node_annotations.push(annotation),
                AnnotationSubject::Value(..) if annotation.key.as_ref() == "sql.display_name" => {
                    value_names.push(annotation)
                }
                _ => {}
            }
        }
        out.observe()?;
        node_annotations.sort_by_key(|annotation| annotation_key(annotation));
        value_names.sort_by_key(|annotation| annotation_key(annotation));
        out.observe()?;
        let mut display_ids = reserve_exact(node_count)?;
        for (fragment, definition) in plan.fragments() {
            for node in definition.nodes().keys() {
                out.observe()?;
                display_ids.push(DisplayNode {
                    key: (*fragment, *node),
                    display: usize::MAX,
                    active: false,
                });
            }
        }
        let mut context = Self {
            plan,
            level,
            node_annotations,
            value_names,
            display_ids,
            diagnostic: diagnostic_control.map(RenderDiagnostic::new),
        };
        let mut next = 0;
        // Iterate the borrowed plan so the context remains mutable while IDs
        // are assigned. No fragment-order Vec or distribution clone exists.
        for fragment_id in fragment_order(plan) {
            out.observe()?;
            if let Some(fragment) = plan.fragments().get(&fragment_id) {
                assign_display_ids(
                    &mut context.display_ids,
                    fragment.id(),
                    fragment.root(),
                    0,
                    &mut next,
                    &|node| fragment.nodes().get(&node).map(|node| node.inputs.as_ref()),
                )?;
            }
        }
        Ok(context)
    }

    fn display_id(&self, fragment: FragmentId, node: NodeId) -> usize {
        self.display_ids
            .binary_search_by_key(&(fragment, node), |entry| entry.key)
            .ok()
            .map(|index| self.display_ids[index].display)
            .filter(|id| *id != usize::MAX)
            .unwrap_or(node.get() as usize)
    }

    const fn detailed(&self) -> bool {
        !matches!(self.level, ExplainLevel::Normal)
    }

    const fn costs(&self) -> bool {
        matches!(self.level, ExplainLevel::Costs | ExplainLevel::Analyze)
    }

    /// Levels that describe how a node will be executed rather than what it
    /// costs. `COSTS` deliberately is not one: it answers a narrower question
    /// and says only what bears on the number it prints.
    const fn verbose(&self) -> bool {
        matches!(self.level, ExplainLevel::Verbose | ExplainLevel::Analyze)
    }

    /// The fragment that delivers the statement's rows first, then the rest.
    ///
    /// A reader follows the plan from what it answers back to what it reads,
    /// and the numbering is the reader's, not the plan's.
    fn fragment_order(&self) -> impl Iterator<Item = FragmentId> + '_ {
        fragment_order(self.plan)
    }

    fn node_annotation(&self, fragment: FragmentId, node: NodeId, key: &str) -> Option<&'a str> {
        let index = self
            .node_annotations
            .partition_point(|a| annotation_key(a) < (fragment, node.get()));
        self.node_annotations[index..]
            .iter()
            .take_while(|a| annotation_key(a) == (fragment, node.get()))
            .find(|a| a.key.as_ref() == key)
            .map(|a| a.value.as_ref())
    }

    fn value_name(&self, fragment: FragmentId, value: ValueId) -> ValueName<'a> {
        let end = self
            .value_names
            .partition_point(|a| annotation_key(a) <= (fragment, value.get()));
        let name = end
            .checked_sub(1)
            .and_then(|index| self.value_names.get(index))
            .filter(|a| annotation_key(a) == (fragment, value.get()))
            .map(|a| a.value.as_ref());
        ValueName { name, value }
    }

    /// The filters one side of a join builds for another side to read.
    ///
    /// A reader asks two things of a runtime filter: what it carries, and
    /// where each end of it sits. Both ends name the expression they are
    /// bound to, because that is what makes a filter checkable against the
    /// join it came from.
    fn render_runtime_filters(&self, out: &mut ExplainRenderOutput) -> Result<(), SqlCompileError> {
        use novarocks_physical_plan::RuntimeFilterDomain;

        if !self.detailed() || self.plan.runtime_filters().is_empty() {
            return Ok(());
        }
        out.push(format_args!("RUNTIME FILTER GRAPH"))?;
        for filter in self.plan.runtime_filters().values() {
            match &filter.domain {
                RuntimeFilterDomain::Membership { .. } => {
                    out.push(format_args!("  runtime filter channel {}", filter.id.get()))?;
                }
                RuntimeFilterDomain::Ordered {
                    key,
                    inclusive,
                    comparator: _,
                } => {
                    out.push(format_args!("  runtime filter"))?;
                    out.push(format_args!(
                        "    domain = OrderedBound(key={} {} NULLS {}, inclusive={inclusive})",
                        key.ty.data_type(),
                        match key.direction {
                            novarocks_physical_plan::SortDirection::Ascending => "ASC",
                            novarocks_physical_plan::SortDirection::Descending => "DESC",
                        },
                        match key.null_ordering {
                            novarocks_physical_plan::NullOrdering::First => "FIRST",
                            novarocks_physical_plan::NullOrdering::Last => "LAST",
                        }
                    ))?;
                }
            }
            for producer in filter.producers.iter() {
                out.push(format_args!(
                    "    producer binding {}, fragment = {}, node = {}, expr = ({})",
                    filter.id.get(),
                    producer.endpoint.fragment.get(),
                    self.display_id(producer.endpoint.fragment, producer.endpoint.node),
                    self.endpoint_text(&producer.endpoint)
                ))?;
            }
            for consumer in filter.consumers.iter() {
                out.push(format_args!(
                    "    consumer binding {}, fragment = {}, node = {}, expr = ({}), activation = {}",
                    filter.id.get(),
                    consumer.endpoint.fragment.get(),
                    self.display_id(consumer.endpoint.fragment, consumer.endpoint.node),
                    self.endpoint_text(&consumer.endpoint),
                    activation_text(&consumer.activation)
                ))?;
            }
        }
        Ok(())
    }

    /// The values one end of a filter is bound to, streamed into its line.
    fn endpoint_text<'b>(
        &'b self,
        endpoint: &'b novarocks_physical_plan::RuntimeFilterEndpoint,
    ) -> impl Display + 'b {
        joined(&endpoint.values, ", ", move |value, f| {
            self.value_name(endpoint.fragment, *value).fmt(f)
        })
    }

    fn render_sink(
        &self,
        fragment: &Fragment,
        out: &mut ExplainRenderOutput,
    ) -> Result<(), SqlCompileError> {
        match fragment.sink() {
            FragmentSink::Stream { edge } => self.render_edge_sink(*edge, out)?,
            FragmentSink::Multicast { edges } => {
                for edge in edges.iter() {
                    self.render_edge_sink(*edge, out)?;
                }
            }
            FragmentSink::Router { routes, .. } => {
                for route in routes.iter() {
                    self.render_edge_sink(route.edge, out)?;
                }
            }
            FragmentSink::Result | FragmentSink::RootResult(_) | FragmentSink::Noop => {}
        }
        for summary in fragment.summaries() {
            self.render_edge_sink(summary.edge, out)?;
        }
        Ok(())
    }

    fn render_edge_sink(
        &self,
        edge: novarocks_physical_plan::EdgeId,
        out: &mut ExplainRenderOutput,
    ) -> Result<(), SqlCompileError> {
        let Some(edge) = self.plan.edges().get(&edge) else {
            return Ok(());
        };
        out.push(format_args!("  STREAM DATA SINK"))?;
        match edge.role {
            novarocks_physical_plan::OutputRole::Relational => {}
            novarocks_physical_plan::OutputRole::ProviderRoute { target } => {
                out.push(format_args!(
                    "    ROLE: provider-route(target={})",
                    target.get()
                ))?;
            }
            novarocks_physical_plan::OutputRole::Summary(kind) => {
                out.push(format_args!(
                    "    ROLE: summary({}: {})",
                    kind,
                    joined(kind.fields(), ", ", |field, f| f.write_str(field.name()))
                ))?;
            }
            novarocks_physical_plan::OutputRole::WriterResults => {
                out.push(format_args!(
                    "    ROLE: writer-results(physical-received-rows, commit-fragments)"
                ))?;
            }
        }
        out.push(format_args!(
            "    EXCHANGE ID: {}",
            self.display_id(edge.destination.fragment, edge.destination.node)
        ))?;
        out.push(format_args!(
            "    PARTITION: {}",
            self.distribution_label(edge.destination.fragment, &edge.partitioning.destination)
        ))?;
        Ok(())
    }

    /// Input recursion has a fixed bound, checked before any indentation or
    /// node output. The ID prepass refuses cycles before publication starts.
    fn render_node(
        &self,
        fragment: &Fragment,
        node_id: NodeId,
        indent: usize,
        out: &mut ExplainRenderOutput,
    ) -> Result<(), SqlCompileError> {
        if indent > MAX_TREE_DEPTH {
            return Err(source_refusal());
        }
        out.ensure_prefix_fits(indent * 2)?;
        let pad = Indent(indent);
        let Some(node) = fragment.nodes().get(&node_id) else {
            return out.push(format_args!("{pad}{}:UNKNOWN", node_id.get()));
        };
        self.render_node_lines(fragment, node, pad, out)?;
        for input in &node.inputs {
            self.render_node(fragment, *input, indent + 1, out)?;
        }
        Ok(())
    }

    fn stats_suffix(&self, fragment: FragmentId, node: NodeId) -> impl Display + '_ {
        let statistics = self
            .detailed()
            .then(|| self.node_annotation(fragment, node, "optimizer.statistics"))
            .flatten();
        text(move |f| {
            let Some(statistics) = statistics else {
                return Ok(());
            };
            let mut rows = "?";
            let mut confidence = None;
            for field in statistics.split(", ") {
                if let Some(value) = field.strip_prefix("rows=") {
                    rows = value;
                } else if let Some(value) = field.strip_prefix("conf=")
                    && self.costs()
                {
                    confidence = Some(value);
                }
            }
            write!(f, " stats={{rows={}", RowCount(rows))?;
            if let Some(confidence) = confidence {
                write!(f, " conf={confidence}")?;
            }
            f.write_str("}")
        })
    }

    fn broadcast_suffix(&self, fragment: FragmentId, node: NodeId) -> impl Display + '_ {
        let decision = self
            .detailed()
            .then(|| self.node_annotation(fragment, node, "optimizer.broadcast"))
            .flatten();
        text(move |f| {
            let Some(decision) = decision else {
                return Ok(());
            };
            let verdict = decision
                .split(", ")
                .find_map(|field| field.strip_prefix("verdict="))
                .unwrap_or("unknown");
            write!(f, " bcast_verdict={verdict}")?;
            if self.costs() {
                write!(f, " bcast[{decision}]")?;
            }
            Ok(())
        })
    }
}

/// One estimated row count, as a count.
///
/// The estimate is arithmetic over fractions and arrives as one; a reader
/// counts rows. An estimate of none and an estimate too large to mean
/// anything both say so rather than printing a number.
struct RowCount<'a>(&'a str);
impl Display for RowCount<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.parse::<f64>() {
            Err(_) => f.write_str(self.0),
            Ok(rows) if rows.is_nan() || rows <= 0.0 => f.write_str("?"),
            Ok(rows) if rows.is_infinite() || rows >= 1e15 => f.write_str(">=1e15"),
            Ok(rows) => write!(f, "{}", rows.round() as i64),
        }
    }
}

/// The relation a scan reads, without the alias the statement gave it.
///
/// The header prints the name as the statement wrote it, alias and all; the
/// line below names the relation itself, which is what a reader checks
/// against the catalog.
/// Whether this scan's shape admits min/max pruning.
///
/// Two things have to hold: the scan reads data, not a metadata relation --
/// a metadata relation's rows describe the table rather than being it -- and
/// every column it projects is one whose type a reader can state bounds for.
/// This is a property of the shape, not evidence that bounds exist.
fn scan_admits_min_max_stats(
    fragment: &Fragment,
    relation: &Relation,
    node: &PhysicalNode,
) -> bool {
    if !matches!(relation, Relation::Data(_)) {
        return false;
    }
    node.output.columns.iter().all(|value| {
        fragment
            .values()
            .get(value)
            .is_some_and(|definition| type_states_bounds(definition.ty.data_type()))
    })
}

/// Whether a reader can state a min and a max for this type.
fn type_states_bounds(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

fn relation_table(relation: &str) -> &str {
    relation
        .split_once(" (alias=")
        .map_or(relation, |(table, _)| table)
}

impl TreeContext<'_> {
    fn distribution_label<'b>(
        &'b self,
        fragment: FragmentId,
        distribution: &'b Distribution,
    ) -> impl Display + 'b {
        text(move |f| match distribution {
            Distribution::Singleton | Distribution::Unconstrained => f.write_str("UNPARTITIONED"),
            Distribution::RoundRobin => f.write_str("RANDOM"),
            Distribution::Broadcast => f.write_str("BROADCAST"),
            Distribution::ConditionalKey { applies, keys, .. } => {
                write!(
                    f,
                    "CONDITIONAL_KEY (WHEN {} IS TRUE HASH (",
                    self.value_name(fragment, *applies)
                )?;
                joined(keys, ", ", |value, f| {
                    self.value_name(fragment, *value).fmt(f)
                })
                .fmt(f)?;
                f.write_str(") ELSE ROUND_ROBIN)")
            }
            Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => {
                f.write_str(if matches!(distribution, Distribution::Hash { .. }) {
                    "HASH_PARTITIONED ("
                } else {
                    "BUCKET_SHUFFLE_HASH_PARTITIONED ("
                })?;
                joined(keys, ", ", |value, f| {
                    self.value_name(fragment, *value).fmt(f)
                })
                .fmt(f)?;
                f.write_str(")")
            }
        })
    }
}

/// One expression, written out the way the statement wrote it.
///
/// The contract dump names each expression and refers to it by that name;
/// here every operand is written where it stands, because an operator tree is
/// read top to bottom and nothing above defines what `e12` was.
struct ExprText<'a> {
    context: &'a TreeContext<'a>,
    fragment: &'a Fragment,
    expr: ExprId,
}

impl std::fmt::Display for ExprText<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.write(formatter, self.expr, 0)
    }
}

impl ExprText<'_> {
    /// Maximum expression depth; malformed input is refused, never elided.
    ///
    /// This is the renderer's own source boundary. A completed plan's broader
    /// validation limits do not authorize unbounded formatting recursion.
    const MAX_DEPTH: usize = 64;

    fn nested(&self, expr: ExprId) -> Self {
        Self {
            context: self.context,
            fragment: self.fragment,
            expr,
        }
    }

    fn write(
        &self,
        formatter: &mut std::fmt::Formatter<'_>,
        expr: ExprId,
        depth: usize,
    ) -> std::fmt::Result {
        if depth > Self::MAX_DEPTH {
            return Err(fmt::Error);
        }
        let Some(node) = self.fragment.expressions().get(expr) else {
            return write!(formatter, "e{}", expr.get());
        };
        let inner = |expr: ExprId| ExprTextAt {
            text: self.nested(expr),
            depth: depth.saturating_add(1),
        };
        match &node.kind {
            ExprKind::Value(value) => self
                .context
                .value_name(self.fragment.id(), *value)
                .fmt(formatter),
            ExprKind::Literal(value) => write!(formatter, "{}", literal_text(value)),
            ExprKind::Constant(reference) => match &self.context.diagnostic {
                Some(diagnostic) => {
                    diagnostic.plan_constant(self.context.plan, *reference, &node.ty, formatter)
                }
                None => write!(
                    formatter,
                    "constant-reference(pool={}, ordinal={})",
                    reference.pool.get(),
                    reference.ordinal
                ),
            },
            ExprKind::LambdaParameter { ordinal, .. } => write!(formatter, "arg{ordinal}"),
            ExprKind::Lambda { body, .. } => write!(formatter, "-> {}", inner(*body)),
            ExprKind::Unary { op, expr } => {
                use novarocks_physical_plan::UnaryOperator;
                match op {
                    UnaryOperator::Plus => write!(formatter, "+{}", inner(*expr)),
                    UnaryOperator::Minus => write!(formatter, "-{}", inner(*expr)),
                    UnaryOperator::Not => write!(formatter, "NOT {}", inner(*expr)),
                    UnaryOperator::BitwiseNot => write!(formatter, "~{}", inner(*expr)),
                }
            }
            ExprKind::Binary {
                left, op, right, ..
            } => write!(
                formatter,
                "{} {} {}",
                inner(*left),
                binary_operator_text(*op),
                inner(*right)
            ),
            ExprKind::Conjunction { args } => {
                write_joined(formatter, args, " AND ", |arg| inner(*arg))
            }
            // An OR is parenthesized where something binding tighter is
            // reading it, and written plainly where it is the whole thing.
            ExprKind::Disjunction { args } => {
                if depth > 0 {
                    write!(formatter, "(")?;
                }
                write_joined(formatter, args, " OR ", |arg| inner(*arg))?;
                if depth > 0 {
                    write!(formatter, ")")?;
                }
                Ok(())
            }
            ExprKind::IsNull { expr, negated } => write!(
                formatter,
                "{} IS {}NULL",
                inner(*expr),
                if *negated { "NOT " } else { "" }
            ),
            ExprKind::IsTruthValue {
                expr,
                value,
                negated,
            } => write!(
                formatter,
                "{} IS {}{}",
                inner(*expr),
                if *negated { "NOT " } else { "" },
                if *value { "TRUE" } else { "FALSE" }
            ),
            ExprKind::Cast { expr, target, .. } => {
                write!(formatter, "CAST({} AS {target})", inner(*expr))
            }
            ExprKind::InList {
                expr,
                list,
                negated,
            } => {
                write!(
                    formatter,
                    "{} {}IN (",
                    inner(*expr),
                    if *negated { "NOT " } else { "" }
                )?;
                write_joined(formatter, list, ", ", |item| inner(*item))?;
                write!(formatter, ")")
            }
            ExprKind::Between {
                expr,
                low,
                high,
                negated,
            } => write!(
                formatter,
                "{} {}BETWEEN {} AND {}",
                inner(*expr),
                if *negated { "NOT " } else { "" },
                inner(*low),
                inner(*high)
            ),
            ExprKind::Like {
                expr,
                pattern,
                negated,
            } => write!(
                formatter,
                "{} {}LIKE {}",
                inner(*expr),
                if *negated { "NOT " } else { "" },
                inner(*pattern)
            ),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                formatter.write_str("CASE")?;
                if let Some(operand) = operand {
                    write!(formatter, " {}", inner(*operand))?;
                }
                for (when, then) in when_then.iter() {
                    write!(formatter, " WHEN {} THEN {}", inner(*when), inner(*then))?;
                }
                if let Some(otherwise) = else_expr {
                    write!(formatter, " ELSE {}", inner(*otherwise))?;
                }
                formatter.write_str(" END")
            }
            ExprKind::FunctionCall { function, args } => {
                write!(formatter, "{}(", function_text(&function.function_id))?;
                write_joined(formatter, args, ", ", |arg| inner(*arg))?;
                write!(formatter, ")")
            }
            ExprKind::WindowCall {
                function,
                distinct,
                args,
                ..
            } => {
                write!(
                    formatter,
                    "{}({}",
                    function_text(&function.function_id),
                    if *distinct { "DISTINCT " } else { "" }
                )?;
                write_joined(formatter, args, ", ", |arg| inner(*arg))?;
                write!(formatter, ")")
            }
        }
    }
}

/// One expression written at a known depth, so nesting stays bounded.
struct ExprTextAt<'a> {
    text: ExprText<'a>,
    depth: usize,
}

impl std::fmt::Display for ExprTextAt<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.text.write(formatter, self.text.expr, self.depth)
    }
}

fn write_joined<T, D: std::fmt::Display>(
    formatter: &mut std::fmt::Formatter<'_>,
    items: &[T],
    separator: &str,
    display: impl Fn(&T) -> D,
) -> std::fmt::Result {
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            formatter.write_str(separator)?;
        }
        display(item).fmt(formatter)?;
    }
    Ok(())
}

fn function_text(function: &novarocks_physical_plan::FunctionId) -> &str {
    // A function is registered as `builtin.scalar/coalesce/v1`; a statement
    // wrote the middle part and that is what names it here.
    let id = function.as_str();
    id.split('/').nth(1).unwrap_or(id)
}

fn binary_operator_text(op: novarocks_physical_plan::BinaryOperator) -> &'static str {
    use novarocks_physical_plan::BinaryOperator;
    match op {
        BinaryOperator::Eq => "=",
        BinaryOperator::EqForNull => "<=>",
        BinaryOperator::NotEq => "!=",
        BinaryOperator::Lt => "<",
        BinaryOperator::LtEq => "<=",
        BinaryOperator::Gt => ">",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Add => "+",
        BinaryOperator::Subtract => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        BinaryOperator::BitAnd => "&",
        BinaryOperator::BitOr => "|",
        BinaryOperator::BitXor => "^",
    }
}

struct LiteralText<'a>(&'a LiteralValue);

fn literal_text(value: &LiteralValue) -> LiteralText<'_> {
    LiteralText(value)
}

impl std::fmt::Display for LiteralText<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            LiteralValue::Null => formatter.write_str("NULL"),
            LiteralValue::Boolean(value) => write!(formatter, "{value}"),
            LiteralValue::Int64(value) => write!(formatter, "{value}"),
            LiteralValue::UInt64(value) => write!(formatter, "{value}"),
            LiteralValue::Float64Bits(bits) => write!(formatter, "{}", f64::from_bits(*bits)),
            LiteralValue::LargeInt(value) | LiteralValue::Decimal128(value) => {
                write!(formatter, "{value}")
            }
            LiteralValue::Decimal256(bytes) => {
                write!(
                    formatter,
                    "{}",
                    arrow::datatypes::i256::from_be_bytes(*bytes)
                )
            }
            LiteralValue::Utf8(value) => write!(formatter, "'{value}'"),
            LiteralValue::Binary(value) => {
                formatter.write_str("X'")?;
                for byte in value.iter() {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str("'")
            }
            LiteralValue::Date32(value) => write!(formatter, "{value}"),
            LiteralValue::Time64(value) | LiteralValue::Timestamp(value) => {
                write!(formatter, "{value}")
            }
            LiteralValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } => write!(
                formatter,
                "interval(months={months}, days={days}, nanoseconds={nanoseconds})"
            ),
        }
    }
}

fn sort_items_text<'a>(
    context: &'a TreeContext<'a>,
    fragment: &'a Fragment,
    items: &'a [SortExpr],
) -> impl Display + 'a {
    sort_items_pair(context, fragment, items, &[])
}
fn sort_items_pair<'a>(
    context: &'a TreeContext<'a>,
    fragment: &'a Fragment,
    first: &'a [SortExpr],
    second: &'a [SortExpr],
) -> impl Display + 'a {
    text(move |f| {
        for (index, item) in first.iter().chain(second).enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(
                f,
                "{} {} NULLS {}",
                context.expr(fragment, item.expr),
                match item.direction {
                    novarocks_physical_plan::SortDirection::Ascending => "ASC",
                    novarocks_physical_plan::SortDirection::Descending => "DESC",
                },
                match item.null_ordering {
                    novarocks_physical_plan::NullOrdering::First => "FIRST",
                    novarocks_physical_plan::NullOrdering::Last => "LAST",
                }
            )?;
        }
        Ok(())
    })
}

impl TreeContext<'_> {
    fn expr<'a>(&'a self, fragment: &'a Fragment, expr: ExprId) -> ExprText<'a> {
        ExprText {
            context: self,
            fragment,
            expr,
        }
    }

    fn expressions<'a>(
        &'a self,
        fragment: &'a Fragment,
        expressions: &'a [ExprId],
        separator: &'static str,
    ) -> impl Display + 'a {
        joined(expressions, separator, move |expr, f| {
            self.expr(fragment, *expr).fmt(f)
        })
    }

    #[allow(clippy::too_many_lines)]
    fn render_node_lines(
        &self,
        fragment: &Fragment,
        node: &PhysicalNode,
        pad: Indent,
        out: &mut ExplainRenderOutput,
    ) -> Result<(), SqlCompileError> {
        use novarocks_physical_plan::NodeKind;

        let prefix = text(|f| write!(f, "{pad}{}:", self.display_id(fragment.id(), node.id)));
        let stats = self.stats_suffix(fragment.id(), node.id);
        match &node.kind {
            NodeKind::Scan {
                relation: frozen,
                residuals,
                derived_values,
                ..
            } => {
                let relation = self
                    .node_annotation(fragment.id(), node.id, "sql.relation")
                    .unwrap_or("relation");
                out.push(format_args!("{prefix}SCAN {relation}{stats}"))?;
                out.push(format_args!(
                    "{pad}     TABLE: {}",
                    relation_table(relation)
                ))?;
                if let Some(mv) =
                    self.node_annotation(fragment.id(), node.id, "sql.mv_rewritten_from")
                {
                    out.push(format_args!("{pad}     rewritten with mv: {mv}"))?;
                }
                if let Some(provenance) =
                    self.node_annotation(fragment.id(), node.id, "sql.mv_rewrite_provenance")
                {
                    out.push(format_args!(
                        "{pad}     mv rewrite provenance: {provenance}"
                    ))?;
                }
                if self.detailed() && !node.output.columns.is_empty() {
                    let columns = joined(&node.output.columns, ", ", |value, f| {
                        self.value_name(fragment.id(), *value).column().fmt(f)
                    });
                    out.push(format_args!("{pad}     columns: {columns}"))?;
                }
                if self.detailed() && !derived_values.is_empty() {
                    let derived = joined(derived_values, ", ", |value, f| {
                        self.value_name(fragment.id(), *value).fmt(f)?;
                        if let Some(novarocks_physical_plan::ValueOrigin::Expr { expr, .. }) =
                            fragment.values().get(value).map(|def| &def.origin)
                        {
                            write!(f, " := {}", self.expr(fragment, *expr))?;
                        }
                        Ok(())
                    });
                    out.push(format_args!("{pad}     variant columns: {derived}"))?;
                }
                // Whether this scan's shape admits min/max pruning at all:
                // it reads data rather than metadata, and every column it
                // projects is one a reader can state bounds for. It says
                // nothing about whether bounds have been collected.
                if self.verbose() && scan_admits_min_max_stats(fragment, frozen, node) {
                    out.push(format_args!("{pad}     min-max stats"))?;
                }
                if !residuals.is_empty() {
                    out.push(format_args!(
                        "{pad}     predicates: {}",
                        self.expressions(fragment, residuals, " AND ")
                    ))?;
                }
            }
            NodeKind::Filter { predicates } => {
                out.push(format_args!("{prefix}FILTER{stats}"))?;
                out.push(format_args!(
                    "{pad}  predicate: {}",
                    self.expressions(fragment, predicates, " AND ")
                ))?;
            }
            NodeKind::Project { expressions } => {
                let items = joined(expressions, ", ", |(expression, value), f| {
                    let expression = self.expr(fragment, *expression);
                    let name = self.value_name(fragment.id(), *value);
                    ProjectExpressionDisplay { expression, name }.fmt(f)
                });
                out.push(format_args!("{prefix}PROJECT [{items}]{stats}"))?;
            }
            NodeKind::Aggregate {
                group_by,
                calls,
                grouping,
            } => {
                let groups = text(|f| {
                    if !group_by.is_empty() {
                        write!(
                            f,
                            ", group by: [{}]",
                            joined(group_by, ", ", |(expression, _), f| self
                                .expr(fragment, *expression)
                                .fmt(f))
                        )?;
                    }
                    Ok(())
                });
                out.push(format_args!(
                    "{prefix}HASH AGGREGATE ({}{groups}){stats}",
                    aggregate_mode(calls, *grouping)
                ))?;
                if !calls.is_empty() {
                    let aggregates = joined(calls, ", ", |call, f| {
                        if !call.binding.phase.consumes_logical_arguments() {
                            return self.value_name(fragment.id(), call.output).fmt(f);
                        }
                        write!(
                            f,
                            "{}({}{})",
                            function_text(&call.binding.function.function_id),
                            if call.distinct { "DISTINCT " } else { "" },
                            self.expressions(fragment, &call.arguments, ", ")
                        )
                    });
                    out.push(format_args!("{pad}  aggregations: {aggregates}"))?;
                }
            }
            NodeKind::HashJoin {
                kind,
                condition,
                distribution,
                ..
            } => {
                let keys = condition.keys();
                let residual = condition.residual();
                let equalities = joined(keys, ", ", |key, f| {
                    write!(
                        f,
                        "{} {} {}",
                        self.expr(fragment, key.left),
                        if key.null_safe { "<=>" } else { "=" },
                        self.expr(fragment, key.right)
                    )
                });
                out.push(format_args!(
                    "{prefix}HASH JOIN ({}, {}, eq: [{}]){}{stats}",
                    join_distribution_text(*distribution),
                    join_kind_text(*kind),
                    equalities,
                    self.broadcast_suffix(fragment.id(), node.id)
                ))?;
                if let Some(residual) = residual {
                    out.push(format_args!(
                        "{pad}  other: {}",
                        self.expr(fragment, *residual)
                    ))?;
                }
                if let Some(scope) = condition.scope_keys() {
                    let equalities = joined(scope, ", ", |key, f| {
                        write!(
                            f,
                            "{} {} {}",
                            self.expr(fragment, key.left),
                            if key.null_safe { "<=>" } else { "=" },
                            self.expr(fragment, key.right)
                        )
                    });
                    out.push(format_args!("{pad}  scope: [{equalities}]"))?;
                }
            }
            NodeKind::NestLoopJoin {
                kind, predicate, ..
            } => {
                out.push(format_args!(
                    "{prefix}NEST LOOP JOIN ({}){stats}",
                    join_kind_text(*kind)
                ))?;
                if let Some(predicate) = predicate {
                    out.push(format_args!(
                        "{pad}  on: {}",
                        self.expr(fragment, *predicate)
                    ))?;
                }
            }
            NodeKind::Sort { order_by, mode } => {
                let partitions = match mode {
                    novarocks_physical_plan::SortMode::Global => &[][..],
                    novarocks_physical_plan::SortMode::Analytic { partition_by }
                    | novarocks_physical_plan::SortMode::PartitionTopN { partition_by, .. } => {
                        partition_by.as_ref()
                    }
                };
                let suffix = text(|f| {
                    if let novarocks_physical_plan::SortMode::PartitionTopN {
                        limit, kind, ..
                    } = mode
                    {
                        write!(
                            f,
                            " partition_limit={limit} topn_type={}",
                            partition_topn_text(*kind)
                        )?;
                    }
                    Ok(())
                });
                out.push(format_args!(
                    "{prefix}SORT BY [{}]{suffix}{stats}",
                    sort_items_pair(self, fragment, partitions, order_by)
                ))?;
            }
            NodeKind::TopN {
                order_by,
                limit,
                offset,
                phase,
                ..
            } => {
                let label = match phase {
                    novarocks_physical_plan::TopNPhase::Partial { .. } => "LOCAL TOP-N",
                    _ => "TOP-N",
                };
                // Both bounds, always: a top-N that skips nothing says so
                // rather than leaving a reader to infer it.
                out.push(format_args!(
                    "{prefix}{label} (limit={limit}, offset={offset}) [{}]{stats}",
                    sort_items_text(self, fragment, order_by)
                ))?;
            }
            NodeKind::Limit { limit, offset } => {
                let parts = text(|f| {
                    if let Some(limit) = limit {
                        write!(f, "limit={limit}")?;
                    }
                    if *offset > 0 {
                        if limit.is_some() {
                            f.write_str(", ")?;
                        }
                        write!(f, "offset={offset}")?;
                    }
                    Ok(())
                });
                out.push(format_args!("{prefix}LIMIT ({parts}){stats}"))?;
            }
            NodeKind::Window(spec) => {
                let functions = joined(&spec.expressions, "; ", |expression, f| {
                    self.expr(fragment, expression.expression).fmt(f)
                });
                out.push(format_args!("{prefix}WINDOW [{functions}]{stats}"))?;
                if self.detailed() && !spec.partition_by.is_empty() {
                    out.push(format_args!(
                        "{pad}  partition by: [{}]",
                        sort_items_text(self, fragment, &spec.partition_by)
                    ))?;
                }
                if self.detailed() && !spec.order_by.is_empty() {
                    out.push(format_args!(
                        "{pad}  order by: [{}]",
                        sort_items_text(self, fragment, &spec.order_by)
                    ))?;
                }
            }
            NodeKind::SetOp { kind, .. } => {
                out.push(format_args!(
                    "{prefix}{}{stats}",
                    match kind {
                        novarocks_physical_plan::SetOperationKind::UnionAll => "UNION ALL",
                        novarocks_physical_plan::SetOperationKind::Intersect => "INTERSECT",
                        novarocks_physical_plan::SetOperationKind::Except => "EXCEPT",
                    }
                ))?;
            }
            NodeKind::Values { rows } => {
                out.push(format_args!("{prefix}VALUES ({} rows){stats}", rows.len()))?;
            }
            NodeKind::Repeat { grouping_sets, .. } => {
                out.push(format_args!(
                    "{prefix}REPEAT ({} grouping sets){stats}",
                    grouping_sets.len()
                ))?;
            }
            NodeKind::Unpivot { spec } => {
                out.push(format_args!(
                    "{prefix}UNPIVOT (mappings={}){stats}",
                    spec.mappings.len()
                ))?;
            }
            NodeKind::GenerateSeries { start, stop, step } => {
                let step = text(|f| match step {
                    None => f.write_str("1"),
                    Some(expression) => self.expr(fragment, *expression).fmt(f),
                });
                out.push(format_args!(
                    "{prefix}GENERATE_SERIES({}, {}, {step}){stats}",
                    self.expr(fragment, *start),
                    self.expr(fragment, *stop)
                ))?;
            }
            NodeKind::TableFunction {
                function,
                left_outer,
                ..
            } => {
                out.push(format_args!(
                    "{prefix}TABLE_FUNCTION [{} {}]{stats}",
                    if *left_outer { "LEFT" } else { "CROSS" },
                    Uppercase(function_text(&function.function_id))
                ))?;
            }
            NodeKind::AssertOneRow(_) => {
                out.push(format_args!("{prefix}ASSERT ONE ROW{stats}"))?;
            }
            NodeKind::ChangeEventExpand { events, .. } => {
                out.push(format_args!(
                    "{prefix}CHANGE_EVENT_EXPAND(events={}){stats}",
                    events.len()
                ))?;
            }
            NodeKind::ExchangeSource { edge, .. } => {
                // A gather that keeps an ordering is merging its senders
                // rather than concatenating them, and that is the difference
                // a reader is looking for.
                let ordered = !node.output_properties.ordering.is_empty();
                let label = self.plan.edges().get(edge).map_or("EXCHANGE", |edge| {
                    match edge.partitioning.destination {
                        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
                            "HASH EXCHANGE"
                        }
                        Distribution::ConditionalKey { .. } => "CONDITIONAL KEY EXCHANGE",
                        Distribution::Broadcast => "BROADCAST EXCHANGE",
                        Distribution::RoundRobin => "RANDOM EXCHANGE",
                        Distribution::Singleton | Distribution::Unconstrained if ordered => {
                            "MERGING-EXCHANGE"
                        }
                        Distribution::Singleton | Distribution::Unconstrained => "GATHER",
                    }
                });
                out.push(format_args!("{prefix}{label}{stats}"))?;
            }
            NodeKind::TableWriter { .. } => {
                out.push(format_args!("{prefix}TABLE WRITER{stats}"))?;
            }
            NodeKind::TableFinish(spec) => {
                out.push(format_args!("{prefix}TABLE FINISH{stats}"))?;
                out.push(format_args!(
                    "{pad}  count mode: {}",
                    super::completed::finish_count_mode_name(spec.count_mode)
                ))?;
            }
        }
        Ok(())
    }
}

/// The name the wire and the reader both know this aggregate phase by.
fn aggregate_mode(
    calls: &[novarocks_physical_plan::AggregateCall],
    grouping: novarocks_physical_plan::AggregateGrouping,
) -> &'static str {
    use novarocks_physical_plan::AggregateGrouping;

    let finalizes = calls
        .iter()
        .any(|call| call.binding.phase.produces_final_result());
    let merges = calls
        .iter()
        .any(|call| call.binding.phase.sequence().is_some());
    match (grouping, finalizes) {
        (_, true) if merges => "GLOBAL",
        (_, true) => "SINGLE",
        (AggregateGrouping::Partial, false) => "LOCAL",
        (AggregateGrouping::Complete, false) => "DISTINCT_GLOBAL",
    }
}

/// When a consumer starts reading through a filter.
fn activation_text(
    activation: &novarocks_physical_plan::RuntimeFilterConsumerActivation,
) -> impl Display + '_ {
    use novarocks_physical_plan::{LateApplyGranularity, RuntimeFilterConsumerActivation};
    text(move |f| match activation {
        RuntimeFilterConsumerActivation::BlockingSnapshot => f.write_str("BlockingSnapshot"),
        RuntimeFilterConsumerActivation::NonBlockingLive { late_apply }
        | RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply } => {
            let granularity = match late_apply {
                LateApplyGranularity::Row => "Row",
                LateApplyGranularity::Batch => "Batch",
                LateApplyGranularity::RowGroup => "RowGroup",
                LateApplyGranularity::Split => "Split",
                LateApplyGranularity::File => "File",
            };
            write!(f, "NonBlockingLive({granularity})")
        }
    })
}

fn join_kind_text(kind: novarocks_physical_plan::JoinKind) -> &'static str {
    use novarocks_physical_plan::JoinKind;
    match kind {
        JoinKind::Inner => "INNER",
        JoinKind::LeftOuter => "LEFT OUTER",
        JoinKind::RightOuter => "RIGHT OUTER",
        JoinKind::FullOuter => "FULL OUTER",
        JoinKind::LeftSemi => "LEFT SEMI",
        JoinKind::RightSemi => "RIGHT SEMI",
        JoinKind::LeftAnti => "LEFT ANTI",
        JoinKind::RightAnti => "RIGHT ANTI",
        JoinKind::LeftMark => "NULL AWARE LEFT MARK",
        JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI",
        JoinKind::Cross => "CROSS",
    }
}

fn join_distribution_text(distribution: novarocks_physical_plan::JoinDistribution) -> &'static str {
    use novarocks_physical_plan::JoinDistribution;
    match distribution {
        JoinDistribution::BroadcastBuild => "BROADCAST",
        JoinDistribution::Partitioned => "PARTITIONED",
        JoinDistribution::Colocated => "COLOCATE",
        JoinDistribution::Singleton => "SINGLETON",
    }
}

fn partition_topn_text(kind: novarocks_physical_plan::PartitionTopNType) -> &'static str {
    use novarocks_physical_plan::PartitionTopNType;
    match kind {
        PartitionTopNType::RowNumber => "ROW_NUMBER",
        PartitionTopNType::Rank => "RANK",
        PartitionTopNType::DenseRank => "DENSE_RANK",
    }
}

#[cfg(test)]
#[path = "completed_tree_tests.rs"]
mod tests;
