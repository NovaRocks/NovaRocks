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

use std::collections::BTreeMap;
use std::fmt::Write as _;

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
) -> Result<Vec<String>, SqlCompileError> {
    let context = TreeContext::new(plan, level);
    let mut out = Vec::new();
    if context.costs() {
        out.extend(plan.annotations().iter().filter_map(|annotation| {
            (annotation.subject == AnnotationSubject::Plan
                && annotation.key.as_ref()
                    == crate::optimizer::stats_input::TABLE_STATISTICS_ANNOTATION_KEY)
                .then(|| annotation.value.to_string())
        }));
    }
    context.render_runtime_filters(&mut out);
    for (display_id, fragment_id) in context.fragment_order().into_iter().enumerate() {
        let Some(fragment) = plan.fragments().get(&fragment_id) else {
            continue;
        };
        if context.detailed() {
            out.push(format!("PLAN FRAGMENT {display_id}"));
            out.push("  OUTPUT EXPRS: *".to_string());
            out.push(format!(
                "  PARTITION: {}",
                context.distribution_label(fragment_id, &fragment.root_output_distribution())
            ));
            context.render_sink(fragment, &mut out);
        }
        context.render_node(fragment, fragment.root(), 0, &mut out);
    }
    Ok(out)
}

/// What a fragment's own rows are laid out as, read at its root.
trait RootDistribution {
    fn root_output_distribution(&self) -> Distribution;
}

impl RootDistribution for Fragment {
    fn root_output_distribution(&self) -> Distribution {
        self.nodes()
            .get(&self.root())
            .map_or(Distribution::Unconstrained, |node| {
                node.output_properties.distribution.clone()
            })
    }
}

struct TreeContext<'a> {
    plan: &'a PhysicalPlan,
    level: ExplainLevel,
    node_annotations: BTreeMap<(FragmentId, NodeId), Vec<&'a PlanAnnotation>>,
    value_names: BTreeMap<(FragmentId, ValueId), &'a str>,
    /// What a reader calls each node.
    ///
    /// A node's identity is unique within its fragment, which is all the plan
    /// needs; a reader looking at every fragment at once needs a name that is
    /// unique across them, so each node is numbered where it is met.
    display_ids: BTreeMap<(FragmentId, NodeId), usize>,
}

impl<'a> TreeContext<'a> {
    fn new(plan: &'a PhysicalPlan, level: ExplainLevel) -> Self {
        let mut node_annotations: BTreeMap<_, Vec<&PlanAnnotation>> = BTreeMap::new();
        let mut value_names = BTreeMap::new();
        for annotation in plan.annotations() {
            match annotation.subject {
                AnnotationSubject::Node(fragment, node) => {
                    node_annotations
                        .entry((fragment, node))
                        .or_default()
                        .push(annotation);
                }
                AnnotationSubject::Value(fragment, value)
                    if annotation.key.as_ref() == "sql.display_name" =>
                {
                    value_names.insert((fragment, value), annotation.value.as_ref());
                }
                _ => {}
            }
        }
        let mut context = Self {
            plan,
            level,
            node_annotations,
            value_names,
            display_ids: BTreeMap::new(),
        };
        let mut next = 0_usize;
        for fragment_id in context.fragment_order() {
            let Some(fragment) = plan.fragments().get(&fragment_id) else {
                continue;
            };
            let mut pending = vec![fragment.root()];
            while let Some(node_id) = pending.pop() {
                if context
                    .display_ids
                    .insert((fragment_id, node_id), next)
                    .is_some()
                {
                    continue;
                }
                next = next.saturating_add(1);
                if let Some(node) = fragment.nodes().get(&node_id) {
                    pending.extend(node.inputs.iter().rev().copied());
                }
            }
        }
        context
    }

    fn display_id(&self, fragment: FragmentId, node: NodeId) -> usize {
        self.display_ids
            .get(&(fragment, node))
            .copied()
            .unwrap_or_else(|| node.get() as usize)
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
    fn fragment_order(&self) -> Vec<FragmentId> {
        let root = self
            .plan
            .result_port()
            .map_or_else(
                || self.plan.fragments().keys().next().copied(),
                |port| Some(port.fragment),
            )
            .unwrap_or(FragmentId::new(0));
        let mut order = vec![root];
        order.extend(
            self.plan
                .fragments()
                .keys()
                .copied()
                .filter(|fragment| *fragment != root),
        );
        order
    }

    fn node_annotation(&self, fragment: FragmentId, node: NodeId, key: &str) -> Option<&'a str> {
        self.node_annotations
            .get(&(fragment, node))?
            .iter()
            .find(|annotation| annotation.key.as_ref() == key)
            .map(|annotation| annotation.value.as_ref())
    }

    /// What a value is called, which is what the statement called it.
    fn value_name(&self, fragment: FragmentId, value: ValueId) -> String {
        self.value_names
            .get(&(fragment, value))
            .map_or_else(|| format!("v{}", value.get()), ToString::to_string)
    }

    /// The filters one side of a join builds for another side to read.
    ///
    /// A reader asks two things of a runtime filter: what it carries, and
    /// where each end of it sits. Both ends name the expression they are
    /// bound to, because that is what makes a filter checkable against the
    /// join it came from.
    fn render_runtime_filters(&self, out: &mut Vec<String>) {
        use novarocks_physical_plan::RuntimeFilterDomain;

        if !self.detailed() || self.plan.runtime_filters().is_empty() {
            return;
        }
        out.push("RUNTIME FILTER GRAPH".to_string());
        for filter in self.plan.runtime_filters().values() {
            match &filter.domain {
                RuntimeFilterDomain::Membership { .. } => {
                    out.push(format!("  runtime filter channel {}", filter.id.get()));
                }
                RuntimeFilterDomain::Ordered {
                    key,
                    inclusive,
                    comparator: _,
                } => {
                    out.push("  runtime filter".to_string());
                    out.push(format!(
                        "    domain = OrderedBound(key={} {} NULLS {}, inclusive={inclusive})",
                        key.ty.data_type,
                        match key.direction {
                            novarocks_physical_plan::SortDirection::Ascending => "ASC",
                            novarocks_physical_plan::SortDirection::Descending => "DESC",
                        },
                        match key.null_ordering {
                            novarocks_physical_plan::NullOrdering::First => "FIRST",
                            novarocks_physical_plan::NullOrdering::Last => "LAST",
                        }
                    ));
                }
            }
            for producer in filter.producers.iter() {
                out.push(format!(
                    "    producer binding {}, fragment = {}, node = {}, expr = ({})",
                    filter.id.get(),
                    producer.endpoint.fragment.get(),
                    self.display_id(producer.endpoint.fragment, producer.endpoint.node),
                    self.endpoint_text(&producer.endpoint)
                ));
            }
            for consumer in filter.consumers.iter() {
                out.push(format!(
                    "    consumer binding {}, fragment = {}, node = {}, expr = ({}), activation = {}",
                    filter.id.get(),
                    consumer.endpoint.fragment.get(),
                    self.display_id(consumer.endpoint.fragment, consumer.endpoint.node),
                    self.endpoint_text(&consumer.endpoint),
                    activation_text(&consumer.activation)
                ));
            }
        }
    }

    /// The values one end of a filter is bound to.
    fn endpoint_text(&self, endpoint: &novarocks_physical_plan::RuntimeFilterEndpoint) -> String {
        endpoint
            .values
            .iter()
            .map(|value| self.value_name(endpoint.fragment, *value))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn render_sink(&self, fragment: &Fragment, out: &mut Vec<String>) {
        match fragment.sink() {
            FragmentSink::Stream { edge } => self.render_edge_sink(*edge, out),
            FragmentSink::Multicast { edges } => {
                for edge in edges.iter() {
                    self.render_edge_sink(*edge, out);
                }
            }
            FragmentSink::Router { routes, .. } => {
                for route in routes.iter() {
                    self.render_edge_sink(route.edge, out);
                }
            }
            FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => {}
        }
    }

    fn render_edge_sink(&self, edge: novarocks_physical_plan::EdgeId, out: &mut Vec<String>) {
        let Some(edge) = self.plan.edges().get(&edge) else {
            return;
        };
        out.push("  STREAM DATA SINK".to_string());
        out.push(format!(
            "    EXCHANGE ID: {}",
            self.display_id(edge.destination.fragment, edge.destination.node)
        ));
        out.push(format!(
            "    PARTITION: {}",
            self.distribution_label(edge.destination.fragment, &edge.partitioning.destination)
        ));
    }

    /// One node and everything under it.
    ///
    /// Recursion is bounded by the tree-depth the contract already validates,
    /// so the traversal reads the way the output does.
    fn render_node(
        &self,
        fragment: &Fragment,
        node_id: NodeId,
        indent: usize,
        out: &mut Vec<String>,
    ) {
        let pad = "  ".repeat(indent);
        let Some(node) = fragment.nodes().get(&node_id) else {
            out.push(format!("{pad}{}:UNKNOWN", node_id.get()));
            return;
        };
        self.render_node_lines(fragment, node, &pad, out);
        for input in node.inputs.iter() {
            self.render_node(fragment, *input, indent.saturating_add(1), out);
        }
    }

    fn stats_suffix(&self, fragment: FragmentId, node: NodeId) -> String {
        if !self.detailed() {
            return String::new();
        }
        let Some(statistics) = self.node_annotation(fragment, node, "optimizer.statistics") else {
            return String::new();
        };
        let mut rows = "?".to_string();
        let mut confidence = String::new();
        for field in statistics.split(", ") {
            if let Some(value) = field.strip_prefix("rows=") {
                rows = row_count_text(value);
            } else if let Some(value) = field.strip_prefix("conf=")
                && self.costs()
            {
                confidence = format!(" conf={value}");
            }
        }
        format!(" stats={{rows={rows}{confidence}}}")
    }

    /// What the cost model decided about broadcasting this join.
    ///
    /// The verdict is the part a reader acts on, so it shows from Verbose; the
    /// numbers behind it belong with the other costs.
    fn broadcast_suffix(&self, fragment: FragmentId, node: NodeId) -> String {
        if !self.detailed() {
            return String::new();
        }
        let Some(decision) = self.node_annotation(fragment, node, "optimizer.broadcast") else {
            return String::new();
        };
        let verdict = decision
            .split(", ")
            .find_map(|field| field.strip_prefix("verdict="))
            .unwrap_or("unknown");
        let mut suffix = format!(" bcast_verdict={verdict}");
        if self.costs() {
            let _ = write!(suffix, " bcast[{decision}]");
        }
        suffix
    }
}

/// One estimated row count, as a count.
///
/// The estimate is arithmetic over fractions and arrives as one; a reader
/// counts rows. An estimate of none and an estimate too large to mean
/// anything both say so rather than printing a number.
fn row_count_text(value: &str) -> String {
    /// Above this the estimate has stopped being a number a reader can use.
    const UNBOUNDED: f64 = 1e15;

    let Ok(rows) = value.parse::<f64>() else {
        return value.to_string();
    };
    if rows.is_nan() || rows <= 0.0 {
        "?".to_string()
    } else if rows.is_infinite() || rows >= UNBOUNDED {
        ">=1e15".to_string()
    } else {
        format!("{}", rows.round() as i64)
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
            .is_some_and(|definition| type_states_bounds(&definition.ty.data_type))
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
    /// How a layout places its rows, and what it places them by.
    fn distribution_label(&self, fragment: FragmentId, distribution: &Distribution) -> String {
        let keys = |keys: &[ValueId]| {
            keys.iter()
                .map(|value| self.value_name(fragment, *value))
                .collect::<Vec<_>>()
                .join(", ")
        };
        match distribution {
            Distribution::Singleton | Distribution::Unconstrained => "UNPARTITIONED".to_string(),
            Distribution::RoundRobin => "RANDOM".to_string(),
            Distribution::Broadcast => "BROADCAST".to_string(),
            Distribution::Hash { keys: values, .. } => {
                format!("HASH_PARTITIONED ({})", keys(values))
            }
            Distribution::BucketShuffle { keys: values, .. } => {
                format!("BUCKET_SHUFFLE_HASH_PARTITIONED ({})", keys(values))
            }
        }
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
    /// How deep an expression may be written out before it is elided.
    ///
    /// The contract bounds an expression's depth, and this bound is above it;
    /// it exists so a malformed plan cannot turn rendering into recursion
    /// without end.
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
            return formatter.write_str("...");
        }
        let Some(node) = self.fragment.expressions().get(expr) else {
            return write!(formatter, "e{}", expr.get());
        };
        let inner = |expr: ExprId| ExprTextAt {
            text: self.nested(expr),
            depth: depth.saturating_add(1),
        };
        match &node.kind {
            ExprKind::Value(value) => {
                formatter.write_str(&self.context.value_name(self.fragment.id(), *value))
            }
            ExprKind::Literal(value) => write!(formatter, "{}", literal_text(value)),
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
            ExprKind::Binary { left, op, right } => write!(
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
            ExprKind::Cast { expr, target } => {
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
            LiteralValue::IntervalMonthDayNano(value) => write!(formatter, "{value}"),
        }
    }
}

/// One sort key, with the direction and null placement it establishes.
fn sort_item_text(context: &TreeContext<'_>, fragment: &Fragment, item: &SortExpr) -> String {
    format!(
        "{} {} NULLS {}",
        ExprText {
            context,
            fragment,
            expr: item.expr,
        },
        match item.direction {
            novarocks_physical_plan::SortDirection::Ascending => "ASC",
            novarocks_physical_plan::SortDirection::Descending => "DESC",
        },
        match item.null_ordering {
            novarocks_physical_plan::NullOrdering::First => "FIRST",
            novarocks_physical_plan::NullOrdering::Last => "LAST",
        }
    )
}

fn sort_items_text(context: &TreeContext<'_>, fragment: &Fragment, items: &[SortExpr]) -> String {
    items
        .iter()
        .map(|item| sort_item_text(context, fragment, item))
        .collect::<Vec<_>>()
        .join(", ")
}

impl TreeContext<'_> {
    fn expr(&self, fragment: &Fragment, expr: ExprId) -> String {
        ExprText {
            context: self,
            fragment,
            expr,
        }
        .to_string()
    }

    #[allow(clippy::too_many_lines)]
    fn render_node_lines(
        &self,
        fragment: &Fragment,
        node: &PhysicalNode,
        pad: &str,
        out: &mut Vec<String>,
    ) {
        use novarocks_physical_plan::NodeKind;

        let prefix = format!("{pad}{}:", self.display_id(fragment.id(), node.id));
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
                out.push(format!("{prefix}SCAN {relation}{stats}"));
                out.push(format!("{pad}     TABLE: {}", relation_table(relation)));
                if let Some(mv) =
                    self.node_annotation(fragment.id(), node.id, "sql.mv_rewritten_from")
                {
                    out.push(format!("{pad}     rewritten with mv: {mv}"));
                }
                if let Some(provenance) =
                    self.node_annotation(fragment.id(), node.id, "sql.mv_rewrite_provenance")
                {
                    out.push(format!("{pad}     mv rewrite provenance: {provenance}"));
                }
                if self.detailed() {
                    // The relation's own columns, by the relation's own names:
                    // which of them this scan reads is the question here, and
                    // the name the statement reaches them by is not part of it.
                    let columns = node
                        .output
                        .columns
                        .iter()
                        .map(|value| {
                            let name = self.value_name(fragment.id(), *value);
                            name.rsplit_once('.')
                                .map_or(name.clone(), |(_, column)| column.to_string())
                        })
                        .collect::<Vec<_>>();
                    if !columns.is_empty() {
                        out.push(format!("{pad}     columns: {}", columns.join(", ")));
                    }
                }
                // A column the scan derives while it reads, and the call it
                // derives it with: the reader applies that call to the bytes
                // it is already reading rather than to a column handed on.
                if self.detailed() && !derived_values.is_empty() {
                    let derived = derived_values
                        .iter()
                        .map(|value| {
                            let name = self.value_name(fragment.id(), *value);
                            match fragment.values().get(value).map(|def| &def.origin) {
                                Some(novarocks_physical_plan::ValueOrigin::Expr {
                                    expr, ..
                                }) => {
                                    format!("{name} := {}", self.expr(fragment, *expr))
                                }
                                _ => name,
                            }
                        })
                        .collect::<Vec<_>>();
                    out.push(format!("{pad}     variant columns: {}", derived.join(", ")));
                }
                // Whether this scan's shape admits min/max pruning at all:
                // it reads data rather than metadata, and every column it
                // projects is one a reader can state bounds for. It says
                // nothing about whether bounds have been collected.
                if self.verbose() && scan_admits_min_max_stats(fragment, frozen, node) {
                    out.push(format!("{pad}     min-max stats"));
                }
                if !residuals.is_empty() {
                    let predicates = residuals
                        .iter()
                        .map(|expression| self.expr(fragment, *expression))
                        .collect::<Vec<_>>();
                    out.push(format!(
                        "{pad}     predicates: {}",
                        predicates.join(" AND ")
                    ));
                }
            }
            NodeKind::Filter { predicates } => {
                out.push(format!("{prefix}FILTER{stats}"));
                let text = predicates
                    .iter()
                    .map(|expression| self.expr(fragment, *expression))
                    .collect::<Vec<_>>();
                out.push(format!("{pad}  predicate: {}", text.join(" AND ")));
            }
            NodeKind::Project { expressions } => {
                let items = expressions
                    .iter()
                    .map(|(expression, value)| {
                        let text = self.expr(fragment, *expression);
                        let name = self.value_name(fragment.id(), *value);
                        if text == name {
                            name
                        } else {
                            format!("{text} AS {name}")
                        }
                    })
                    .collect::<Vec<_>>();
                out.push(format!("{prefix}PROJECT [{}]{stats}", items.join(", ")));
            }
            NodeKind::Aggregate {
                group_by,
                calls,
                grouping,
            } => {
                let mut header = format!(
                    "{prefix}HASH AGGREGATE ({}",
                    aggregate_mode(calls, *grouping)
                );
                if !group_by.is_empty() {
                    let keys = group_by
                        .iter()
                        .map(|(expression, _)| self.expr(fragment, *expression))
                        .collect::<Vec<_>>();
                    let _ = write!(header, ", group by: [{}]", keys.join(", "));
                }
                let _ = write!(header, "){stats}");
                out.push(header);
                if !calls.is_empty() {
                    let aggregates = calls
                        .iter()
                        .map(|call| -> String {
                            // A phase that merges reads the state the phase
                            // below it produced, and that state is already
                            // named after the call it belongs to. Printing the
                            // call around it would say the call twice.
                            if !call.binding.phase.consumes_logical_arguments() {
                                return self.value_name(fragment.id(), call.output);
                            }
                            let args = call
                                .arguments
                                .iter()
                                .map(|argument| self.expr(fragment, *argument))
                                .collect::<Vec<_>>();
                            format!(
                                "{}({}{})",
                                function_text(&call.binding.function.function_id),
                                if call.distinct { "DISTINCT " } else { "" },
                                args.join(", ")
                            )
                        })
                        .collect::<Vec<_>>();
                    out.push(format!("{pad}  aggregations: {}", aggregates.join(", ")));
                }
            }
            NodeKind::HashJoin {
                kind,
                keys,
                distribution,
                residual,
                ..
            } => {
                let equalities = keys
                    .iter()
                    .map(|key| {
                        format!(
                            "{} {} {}",
                            self.expr(fragment, key.left),
                            if key.null_safe { "<=>" } else { "=" },
                            self.expr(fragment, key.right)
                        )
                    })
                    .collect::<Vec<_>>();
                out.push(format!(
                    "{prefix}HASH JOIN ({}, {}, eq: [{}]){}{stats}",
                    join_distribution_text(*distribution),
                    join_kind_text(*kind),
                    equalities.join(", "),
                    self.broadcast_suffix(fragment.id(), node.id)
                ));
                if let Some(residual) = residual {
                    out.push(format!("{pad}  other: {}", self.expr(fragment, *residual)));
                }
            }
            NodeKind::NestLoopJoin {
                kind, predicate, ..
            } => {
                out.push(format!(
                    "{prefix}NEST LOOP JOIN ({}){stats}",
                    join_kind_text(*kind)
                ));
                if let Some(predicate) = predicate {
                    out.push(format!("{pad}  on: {}", self.expr(fragment, *predicate)));
                }
            }
            NodeKind::Sort { order_by, mode } => {
                let mut keys = match mode {
                    novarocks_physical_plan::SortMode::Global => Vec::new(),
                    novarocks_physical_plan::SortMode::Analytic { partition_by }
                    | novarocks_physical_plan::SortMode::PartitionTopN { partition_by, .. } => {
                        partition_by.to_vec()
                    }
                };
                keys.extend(order_by.iter().cloned());
                let mut suffix = String::new();
                if let novarocks_physical_plan::SortMode::PartitionTopN { limit, kind, .. } = mode {
                    let _ = write!(
                        suffix,
                        " partition_limit={limit} topn_type={}",
                        partition_topn_text(*kind)
                    );
                }
                out.push(format!(
                    "{prefix}SORT BY [{}]{suffix}{stats}",
                    sort_items_text(self, fragment, &keys)
                ));
            }
            NodeKind::TopN {
                order_by,
                limit,
                offset,
                phase,
            } => {
                let label = match phase {
                    novarocks_physical_plan::TopNPhase::Partial { .. } => "LOCAL TOP-N",
                    _ => "TOP-N",
                };
                // Both bounds, always: a top-N that skips nothing says so
                // rather than leaving a reader to infer it.
                let parts = [format!("limit={limit}"), format!("offset={offset}")];
                out.push(format!(
                    "{prefix}{label} ({}) [{}]{stats}",
                    parts.join(", "),
                    sort_items_text(self, fragment, order_by)
                ));
            }
            NodeKind::Limit { limit, offset } => {
                let mut parts = Vec::new();
                if let Some(limit) = limit {
                    parts.push(format!("limit={limit}"));
                }
                if *offset > 0 {
                    parts.push(format!("offset={offset}"));
                }
                out.push(format!("{prefix}LIMIT ({}){stats}", parts.join(", ")));
            }
            NodeKind::Window(spec) => {
                let functions = spec
                    .expressions
                    .iter()
                    .map(|expression| self.expr(fragment, expression.expression))
                    .collect::<Vec<_>>();
                out.push(format!("{prefix}WINDOW [{}]{stats}", functions.join("; ")));
                if self.detailed() && !spec.partition_by.is_empty() {
                    out.push(format!(
                        "{pad}  partition by: [{}]",
                        sort_items_text(self, fragment, &spec.partition_by)
                    ));
                }
                if self.detailed() && !spec.order_by.is_empty() {
                    out.push(format!(
                        "{pad}  order by: [{}]",
                        sort_items_text(self, fragment, &spec.order_by)
                    ));
                }
            }
            NodeKind::SetOp { kind, .. } => {
                out.push(format!(
                    "{prefix}{}{stats}",
                    match kind {
                        novarocks_physical_plan::SetOperationKind::UnionAll => "UNION ALL",
                        novarocks_physical_plan::SetOperationKind::Intersect => "INTERSECT",
                        novarocks_physical_plan::SetOperationKind::Except => "EXCEPT",
                    }
                ));
            }
            NodeKind::Values { rows } => {
                out.push(format!("{prefix}VALUES ({} rows){stats}", rows.len()));
            }
            NodeKind::Repeat { grouping_sets, .. } => {
                out.push(format!(
                    "{prefix}REPEAT ({} grouping sets){stats}",
                    grouping_sets.len()
                ));
            }
            NodeKind::Unpivot { spec } => {
                out.push(format!(
                    "{prefix}UNPIVOT (mappings={}){stats}",
                    spec.mappings.len()
                ));
            }
            NodeKind::GenerateSeries { start, stop, step } => {
                let step = step.map_or_else(
                    || "1".to_string(),
                    |expression| self.expr(fragment, expression),
                );
                out.push(format!(
                    "{prefix}GENERATE_SERIES({}, {}, {step}){stats}",
                    self.expr(fragment, *start),
                    self.expr(fragment, *stop)
                ));
            }
            NodeKind::TableFunction {
                function,
                left_outer,
                ..
            } => {
                out.push(format!(
                    "{prefix}TABLE_FUNCTION [{} {}]{stats}",
                    if *left_outer { "LEFT" } else { "CROSS" },
                    function_text(&function.function_id).to_uppercase()
                ));
            }
            NodeKind::AssertOneRow(_) => {
                out.push(format!("{prefix}ASSERT ONE ROW{stats}"));
            }
            NodeKind::ChangeEventExpand { events, .. } => {
                out.push(format!(
                    "{prefix}CHANGE_EVENT_EXPAND(events={}){stats}",
                    events.len()
                ));
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
                        Distribution::Broadcast => "BROADCAST EXCHANGE",
                        Distribution::RoundRobin => "RANDOM EXCHANGE",
                        Distribution::Singleton | Distribution::Unconstrained if ordered => {
                            "MERGING-EXCHANGE"
                        }
                        Distribution::Singleton | Distribution::Unconstrained => "GATHER",
                    }
                });
                out.push(format!("{prefix}{label}{stats}"));
            }
            NodeKind::TableWriter { .. } => {
                out.push(format!("{prefix}TABLE WRITER{stats}"));
            }
            NodeKind::TableFinish(_) => {
                out.push(format!("{prefix}TABLE FINISH{stats}"));
            }
        }
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
) -> String {
    use novarocks_physical_plan::{LateApplyGranularity, RuntimeFilterConsumerActivation};

    let granularity = |late_apply: LateApplyGranularity| match late_apply {
        LateApplyGranularity::Row => "Row",
        LateApplyGranularity::Batch => "Batch",
        LateApplyGranularity::RowGroup => "RowGroup",
        LateApplyGranularity::Split => "Split",
        LateApplyGranularity::File => "File",
    };
    match activation {
        RuntimeFilterConsumerActivation::BlockingSnapshot => "BlockingSnapshot".to_string(),
        RuntimeFilterConsumerActivation::NonBlockingLive { late_apply }
        | RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply } => {
            format!("NonBlockingLive({})", granularity(*late_apply))
        }
    }
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
