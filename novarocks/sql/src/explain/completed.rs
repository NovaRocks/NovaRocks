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

//! EXPLAIN rendering over the immutable final physical-plan contract.

use std::fmt::Display as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Write},
};

use novarocks_physical_plan::{
    AnnotationSubject, Edge, ExprId, Fragment, FragmentCuts, FragmentId, FragmentSink, NodeId,
    NodeKind, PhysicalNode, PhysicalPlan, PlanAnnotation, PlanVersionId, SortExpr, ValueId,
    WindowExpression, WriteTargetOrdinal, WriterGroupedUnpivotSpec,
};
use sha2::{Digest, Sha256};

use crate::compiler::{SqlCompileError, SqlCompletedPlan, SqlDisplayAnnotation, SqlDisplayIntent};
use crate::explain::ExplainLevel;

struct Joined<'a, T, F> {
    items: &'a [T],
    separator: &'static str,
    render: F,
}

fn joined<'a, T, F>(items: &'a [T], separator: &'static str, render: F) -> Joined<'a, T, F> {
    Joined {
        items,
        separator,
        render,
    }
}

impl<T, F> fmt::Display for Joined<'_, T, F>
where
    F: Fn(&T, &mut fmt::Formatter<'_>) -> fmt::Result,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, item) in self.items.iter().enumerate() {
            if index != 0 {
                formatter.write_str(self.separator)?;
            }
            (self.render)(item, formatter)?;
        }
        Ok(())
    }
}

struct Quoted<'a>(&'a str);

impl fmt::Display for Quoted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_char('"')?;
        for character in self.0.chars() {
            for escaped in character.escape_default() {
                formatter.write_char(escaped)?;
            }
        }
        formatter.write_char('"')
    }
}

#[derive(Clone, Copy)]
struct Hex<'a>(&'a [u8]);

impl fmt::Display for Hex<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

struct OptionalQuoted<'a>(Option<&'a str>);

impl fmt::Display for OptionalQuoted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => Quoted(value).fmt(formatter),
            None => formatter.write_str("none"),
        }
    }
}

struct CoverageBound<'a> {
    value: Option<&'a [u8]>,
    infinity: &'static str,
}

impl fmt::Display for CoverageBound<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value {
            Some(value) => Hex(value).fmt(formatter),
            None => formatter.write_str(self.infinity),
        }
    }
}

struct DigestBytes([u8; 32]);

impl fmt::Display for DigestBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Hex(&self.0).fmt(formatter)
    }
}

impl SqlCompletedPlan {
    /// Render ordinary EXPLAIN only after the final physical plan is complete.
    pub fn render_explain_lines(&self) -> Result<Vec<String>, SqlCompileError> {
        self.render_explain_lines_with_budget(ExplainRenderBudget::default())
    }

    /// Render ordinary EXPLAIN within one explicit output budget.
    pub fn render_explain_lines_with_budget(
        &self,
        budget: ExplainRenderBudget,
    ) -> Result<Vec<String>, SqlCompileError> {
        explain_completed_plan(self, budget)
    }

    /// Render EXPLAIN ANALYZE with observations bound to this exact plan.
    pub fn render_explain_analyze_lines(
        &self,
        profile: &SqlCompletedExplainProfile,
    ) -> Result<Vec<String>, SqlCompileError> {
        self.render_explain_analyze_lines_with_budget(profile, ExplainRenderBudget::default())
    }

    /// Render EXPLAIN ANALYZE within one explicit output budget.
    pub fn render_explain_analyze_lines_with_budget(
        &self,
        profile: &SqlCompletedExplainProfile,
        budget: ExplainRenderBudget,
    ) -> Result<Vec<String>, SqlCompileError> {
        explain_completed_plan_analyze(self, profile, budget)
    }
}

/// Hard output bounds applied while EXPLAIN text is formatted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExplainRenderBudget {
    max_lines: usize,
    max_bytes: usize,
}

impl ExplainRenderBudget {
    pub fn try_new(max_lines: usize, max_bytes: usize) -> Result<Self, SqlCompileError> {
        if max_lines == 0 || max_bytes == 0 {
            return Err(invalid_request(
                "EXPLAIN render budget requires non-zero line and byte limits",
            ));
        }
        Ok(Self {
            max_lines,
            max_bytes,
        })
    }

    pub const fn max_lines(self) -> usize {
        self.max_lines
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }
}

impl Default for ExplainRenderBudget {
    fn default() -> Self {
        Self {
            max_lines: 65_536,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SqlExplainNodeKey {
    fragment: FragmentId,
    node: NodeId,
}

impl SqlExplainNodeKey {
    pub const fn new(fragment: FragmentId, node: NodeId) -> Self {
        Self { fragment, node }
    }

    pub const fn fragment(self) -> FragmentId {
        self.fragment
    }

    pub const fn node(self) -> NodeId {
        self.node
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlExplainOperatorMetrics {
    output_rows: i64,
    total_time_ns: i64,
    peak_mem_bytes: i64,
    total_time_max_ns: i64,
    total_time_min_ns: i64,
    build_ht_ns: i64,
    search_ns: i64,
    out_build_ns: i64,
    out_probe_ns: i64,
    dict_input_rows: i64,
    dict_input_columns: i64,
    dict_kept_rows: i64,
    dict_kept_columns: i64,
    dict_hydrated_rows: i64,
    dict_hydrated_columns: i64,
    dict_unsupported_columns: i64,
}

impl SqlExplainOperatorMetrics {
    pub const fn new(output_rows: i64, total_time_ns: i64, peak_mem_bytes: i64) -> Self {
        Self {
            output_rows,
            total_time_ns,
            peak_mem_bytes,
            total_time_max_ns: 0,
            total_time_min_ns: 0,
            build_ht_ns: 0,
            search_ns: 0,
            out_build_ns: 0,
            out_probe_ns: 0,
            dict_input_rows: 0,
            dict_input_columns: 0,
            dict_kept_rows: 0,
            dict_kept_columns: 0,
            dict_hydrated_rows: 0,
            dict_hydrated_columns: 0,
            dict_unsupported_columns: 0,
        }
    }

    pub const fn with_time_range(mut self, min_ns: i64, max_ns: i64) -> Self {
        self.total_time_min_ns = min_ns;
        self.total_time_max_ns = max_ns;
        self
    }

    pub const fn with_hash_join_times(
        mut self,
        build_ht_ns: i64,
        search_ns: i64,
        out_build_ns: i64,
        out_probe_ns: i64,
    ) -> Self {
        self.build_ht_ns = build_ht_ns;
        self.search_ns = search_ns;
        self.out_build_ns = out_build_ns;
        self.out_probe_ns = out_probe_ns;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub const fn with_dictionary_counts(
        mut self,
        input_rows: i64,
        input_columns: i64,
        kept_rows: i64,
        kept_columns: i64,
        hydrated_rows: i64,
        hydrated_columns: i64,
        unsupported_columns: i64,
    ) -> Self {
        self.dict_input_rows = input_rows;
        self.dict_input_columns = input_columns;
        self.dict_kept_rows = kept_rows;
        self.dict_kept_columns = kept_columns;
        self.dict_hydrated_rows = hydrated_rows;
        self.dict_hydrated_columns = hydrated_columns;
        self.dict_unsupported_columns = unsupported_columns;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqlExplainFragmentMetrics {
    operator_active_time_ns: i64,
    driver_blocked_time_ns: i64,
    dependency_wait_time_ns: i64,
    exchange_wait_time_ns: i64,
    network_time_ns: i64,
    scan_io_time_ns: i64,
}

impl SqlExplainFragmentMetrics {
    pub const fn new(operator_active_time_ns: i64, driver_blocked_time_ns: i64) -> Self {
        Self {
            operator_active_time_ns,
            driver_blocked_time_ns,
            dependency_wait_time_ns: 0,
            exchange_wait_time_ns: 0,
            network_time_ns: 0,
            scan_io_time_ns: 0,
        }
    }

    pub const fn with_wait_times(
        mut self,
        dependency_wait_time_ns: i64,
        exchange_wait_time_ns: i64,
        network_time_ns: i64,
        scan_io_time_ns: i64,
    ) -> Self {
        self.dependency_wait_time_ns = dependency_wait_time_ns;
        self.exchange_wait_time_ns = exchange_wait_time_ns;
        self.network_time_ns = network_time_ns;
        self.scan_io_time_ns = scan_io_time_ns;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlExplainUnavailableReason {
    NotScheduled,
    Cancelled,
    Failed,
    RuntimeDidNotReport,
}

impl SqlExplainUnavailableReason {
    const fn stable_name(self) -> &'static str {
        match self {
            Self::NotScheduled => "not-scheduled",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::RuntimeDidNotReport => "runtime-did-not-report",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlExplainObservation<T> {
    Available(T),
    Unavailable(SqlExplainUnavailableReason),
}

impl<T> From<T> for SqlExplainObservation<T> {
    fn from(value: T) -> Self {
        Self::Available(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlCompletedExplainProfile {
    // UEA-5/T23 must add the authoritative query-attempt identity here once the
    // execution owner publishes it. PlanVersionId alone deliberately does not
    // claim that two executions of one immutable plan are the same attempt.
    plan_version: PlanVersionId,
    operators: BTreeMap<SqlExplainNodeKey, SqlExplainObservation<SqlExplainOperatorMetrics>>,
    fragments: BTreeMap<FragmentId, SqlExplainObservation<SqlExplainFragmentMetrics>>,
}

impl SqlCompletedExplainProfile {
    pub fn try_new(
        plan_version: PlanVersionId,
        operator_facts: Vec<(
            SqlExplainNodeKey,
            SqlExplainObservation<SqlExplainOperatorMetrics>,
        )>,
        fragment_facts: Vec<(FragmentId, SqlExplainObservation<SqlExplainFragmentMetrics>)>,
    ) -> Result<Self, SqlCompileError> {
        let mut operators = BTreeMap::new();
        for (key, metrics) in operator_facts {
            if operators.insert(key, metrics).is_some() {
                return Err(invalid_request(
                    "EXPLAIN ANALYZE profile repeats a fragment/node key",
                ));
            }
        }
        let mut fragments = BTreeMap::new();
        for (fragment, metrics) in fragment_facts {
            if fragments.insert(fragment, metrics).is_some() {
                return Err(invalid_request(
                    "EXPLAIN ANALYZE profile repeats a fragment key",
                ));
            }
        }
        Ok(Self {
            plan_version,
            operators,
            fragments,
        })
    }
}

pub(crate) fn explain_completed_plan(
    completed: &SqlCompletedPlan,
    budget: ExplainRenderBudget,
) -> Result<Vec<String>, SqlCompileError> {
    let SqlDisplayIntent::Explain { level, analyze } = completed.display_intent() else {
        return Err(invalid_request(
            "completed plan does not carry EXPLAIN display intent",
        ));
    };
    if analyze {
        return Err(invalid_request(
            "EXPLAIN ANALYZE requires a profile bound to the completed plan version",
        ));
    }
    render_plan(completed, completed.plan(), level, None, budget)
}

pub(crate) fn explain_completed_plan_analyze(
    completed: &SqlCompletedPlan,
    profile: &SqlCompletedExplainProfile,
    budget: ExplainRenderBudget,
) -> Result<Vec<String>, SqlCompileError> {
    let SqlDisplayIntent::Explain { level, analyze } = completed.display_intent() else {
        return Err(invalid_request(
            "completed plan does not carry EXPLAIN display intent",
        ));
    };
    if !analyze {
        return Err(invalid_request(
            "runtime profile supplied for a non-ANALYZE EXPLAIN plan",
        ));
    }
    if profile.plan_version != completed.plan().version() {
        return Err(invalid_request(
            "EXPLAIN ANALYZE profile plan version does not match the completed plan",
        ));
    }
    validate_profile(completed.plan(), profile).map_err(SqlCompileError::InvalidRequest)?;
    render_plan(completed, completed.plan(), level, Some(profile), budget)
}

fn invalid_request(message: &str) -> SqlCompileError {
    SqlCompileError::InvalidRequest(message.to_string())
}

fn validate_profile(
    plan: &PhysicalPlan,
    profile: &SqlCompletedExplainProfile,
) -> Result<(), String> {
    let expected_fragments = plan.fragments().keys().copied().collect::<BTreeSet<_>>();
    let actual_fragments = profile.fragments.keys().copied().collect::<BTreeSet<_>>();
    if expected_fragments != actual_fragments {
        let missing = expected_fragments.difference(&actual_fragments).count();
        let extra = actual_fragments.difference(&expected_fragments).count();
        return Err(format!(
            "EXPLAIN ANALYZE fragment coverage mismatch: missing=[count={missing}], extra=[count={extra}]"
        ));
    }
    let expected_operators = plan
        .fragments()
        .iter()
        .flat_map(|(fragment, plan)| {
            plan.nodes()
                .keys()
                .map(|node| SqlExplainNodeKey::new(*fragment, *node))
        })
        .collect::<BTreeSet<_>>();
    let actual_operators = profile.operators.keys().copied().collect::<BTreeSet<_>>();
    if expected_operators != actual_operators {
        let missing = expected_operators.difference(&actual_operators).count();
        let extra = actual_operators.difference(&expected_operators).count();
        return Err(format!(
            "EXPLAIN ANALYZE operator coverage mismatch: missing=[count={missing}], extra=[count={extra}]"
        ));
    }
    Ok(())
}

struct ExplainRenderOutput {
    budget: ExplainRenderBudget,
    bytes: usize,
    lines: Vec<String>,
}

impl ExplainRenderOutput {
    fn new(budget: ExplainRenderBudget) -> Self {
        Self {
            budget,
            bytes: 0,
            lines: Vec::new(),
        }
    }

    fn push(&mut self, arguments: fmt::Arguments<'_>) -> Result<(), SqlCompileError> {
        if self.lines.len() >= self.budget.max_lines {
            return Err(explain_budget_exceeded(self.budget));
        }
        let separator = usize::from(!self.lines.is_empty());
        let used = self
            .bytes
            .checked_add(separator)
            .ok_or_else(|| explain_budget_exceeded(self.budget))?;
        let remaining = self
            .budget
            .max_bytes
            .checked_sub(used)
            .ok_or_else(|| explain_budget_exceeded(self.budget))?;
        let mut line = String::new();
        let mut writer = BoundedStringWriter {
            output: &mut line,
            remaining,
        };
        fmt::write(&mut writer, arguments).map_err(|_| explain_budget_exceeded(self.budget))?;
        self.bytes = used
            .checked_add(line.len())
            .ok_or_else(|| explain_budget_exceeded(self.budget))?;
        self.lines.push(line);
        Ok(())
    }

    fn finish(self) -> Vec<String> {
        self.lines
    }

    fn ensure_prefix_fits(&self, bytes: usize) -> Result<(), SqlCompileError> {
        let separator = usize::from(!self.lines.is_empty());
        let used = self
            .bytes
            .checked_add(separator)
            .and_then(|used| used.checked_add(bytes))
            .ok_or_else(|| explain_budget_exceeded(self.budget))?;
        if used > self.budget.max_bytes {
            return Err(explain_budget_exceeded(self.budget));
        }
        Ok(())
    }
}

struct BoundedStringWriter<'a> {
    output: &'a mut String,
    remaining: usize,
}

impl Write for BoundedStringWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if value.len() > self.remaining {
            return Err(fmt::Error);
        }
        self.output.push_str(value);
        self.remaining -= value.len();
        Ok(())
    }
}

fn explain_budget_exceeded(budget: ExplainRenderBudget) -> SqlCompileError {
    invalid_request(&format!(
        "EXPLAIN render exceeds budget of {} lines or {} bytes",
        budget.max_lines, budget.max_bytes
    ))
}

fn render_plan(
    completed: &SqlCompletedPlan,
    plan: &PhysicalPlan,
    level: ExplainLevel,
    profile: Option<&SqlCompletedExplainProfile>,
    budget: ExplainRenderBudget,
) -> Result<Vec<String>, SqlCompileError> {
    render_completed_plan(
        plan,
        completed.display_annotations(),
        level,
        profile,
        budget,
    )
}

/// Render a completed plan that is no longer held as one `SqlCompletedPlan`.
///
/// An owner that kept the plan and its display annotations separately - a
/// published candidate, say - renders through here. The plan and the
/// annotations are the whole input either way; nothing else about how they
/// were carried reaches the text.
pub fn render_completed_plan(
    plan: &PhysicalPlan,
    display_annotations: &[SqlDisplayAnnotation],
    level: ExplainLevel,
    profile: Option<&SqlCompletedExplainProfile>,
    budget: ExplainRenderBudget,
) -> Result<Vec<String>, SqlCompileError> {
    let context = RenderContext::new(plan, level, profile)?;
    let mut lines = ExplainRenderOutput::new(budget);
    lines.push(format_args!(
        "PHYSICAL PLAN version={}, contract-revision={}",
        format_hex(plan.version().as_bytes()),
        plan.required().plan_contract_revision
    ))?;
    render_result_schema(plan, &mut lines)?;
    render_display_annotations(display_annotations, &mut lines)?;
    if is_detailed(level) {
        render_annotations(&context, AnnotationSubject::Plan, "", &mut lines)?;
        render_artifact_references(&context, &mut lines)?;
        render_edges(&context, &mut lines)?;
        render_runtime_filters(&context, &mut lines)?;
    }
    for (fragment_id, fragment) in plan.fragments() {
        lines.push(format_args!("PLAN FRAGMENT {}", fragment_id.get()))?;
        let dop = fragment.dop_domain();
        lines.push(format_args!(
            "  DOP DOMAIN min={}, max={}, power-of-two={}",
            dop.min, dop.max, dop.requires_power_of_two
        ))?;
        lines.push(format_args!(
            "  RUNTIME FILTER ATTACHMENTS [{}]",
            joined(
                fragment.runtime_filters(),
                ", ",
                |filter: &novarocks_physical_plan::RuntimeFilterId,
                 output: &mut fmt::Formatter<'_>| {
                    write!(output, "rf{}", filter.get())
                }
            )
        ))?;
        render_cut_attachments(&context, *fragment_id, &mut lines)?;
        render_sink(&context, *fragment_id, fragment.sink(), &mut lines)?;
        if is_detailed(level) {
            render_annotations(
                &context,
                AnnotationSubject::Fragment(*fragment_id),
                "  ",
                &mut lines,
            )?;
            for (value, annotations) in context.annotations.values_for_fragment(*fragment_id) {
                for annotation in annotations.iter().filter(|annotation| {
                    !annotation_is_internal_display(&annotation.key)
                        && annotation_visible(level, &annotation.key)
                }) {
                    lines.push(format_args!(
                        "  value {}: {}={}",
                        value.get(),
                        annotation.key,
                        annotation.value
                    ))?;
                }
            }
        }
        if let Some(observation) = profile.and_then(|profile| profile.fragments.get(fragment_id)) {
            render_fragment_profile(observation, &mut lines)?;
        }
        render_value_definitions(&context, *fragment_id, fragment, &mut lines)?;
        render_expression_definitions(&context, *fragment_id, fragment, &mut lines)?;
        render_nodes_iterative(&context, *fragment_id, fragment, &mut lines)?;
    }
    Ok(lines.finish())
}

struct AnnotationIndex<'a> {
    plan: Vec<&'a PlanAnnotation>,
    fragments: BTreeMap<FragmentId, Vec<&'a PlanAnnotation>>,
    nodes: BTreeMap<(FragmentId, NodeId), Vec<&'a PlanAnnotation>>,
    values: BTreeMap<(FragmentId, ValueId), Vec<&'a PlanAnnotation>>,
    plan_values: BTreeMap<&'a str, &'a str>,
    fragment_values: BTreeMap<FragmentId, BTreeMap<&'a str, &'a str>>,
    node_values: BTreeMap<(FragmentId, NodeId), BTreeMap<&'a str, &'a str>>,
    value_values: BTreeMap<(FragmentId, ValueId), BTreeMap<&'a str, &'a str>>,
}

impl<'a> AnnotationIndex<'a> {
    fn new(plan: &'a PhysicalPlan) -> Self {
        let mut index = Self {
            plan: Vec::new(),
            fragments: BTreeMap::new(),
            nodes: BTreeMap::new(),
            values: BTreeMap::new(),
            plan_values: BTreeMap::new(),
            fragment_values: BTreeMap::new(),
            node_values: BTreeMap::new(),
            value_values: BTreeMap::new(),
        };
        for annotation in plan.annotations() {
            match annotation.subject {
                AnnotationSubject::Plan => {
                    index.plan.push(annotation);
                    index
                        .plan_values
                        .entry(&annotation.key)
                        .or_insert(&annotation.value);
                }
                AnnotationSubject::Fragment(fragment) => {
                    index
                        .fragments
                        .entry(fragment)
                        .or_default()
                        .push(annotation);
                    index
                        .fragment_values
                        .entry(fragment)
                        .or_default()
                        .entry(&annotation.key)
                        .or_insert(&annotation.value);
                }
                AnnotationSubject::Node(fragment, node) => {
                    index
                        .nodes
                        .entry((fragment, node))
                        .or_default()
                        .push(annotation);
                    index
                        .node_values
                        .entry((fragment, node))
                        .or_default()
                        .entry(&annotation.key)
                        .or_insert(&annotation.value);
                }
                AnnotationSubject::Value(fragment, value) => {
                    index
                        .values
                        .entry((fragment, value))
                        .or_default()
                        .push(annotation);
                    index
                        .value_values
                        .entry((fragment, value))
                        .or_default()
                        .entry(&annotation.key)
                        .or_insert(&annotation.value);
                }
            }
        }
        index
    }

    fn get(&self, subject: AnnotationSubject) -> &[&'a PlanAnnotation] {
        match subject {
            AnnotationSubject::Plan => &self.plan,
            AnnotationSubject::Fragment(fragment) => self
                .fragments
                .get(&fragment)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            AnnotationSubject::Node(fragment, node) => self
                .nodes
                .get(&(fragment, node))
                .map(Vec::as_slice)
                .unwrap_or_default(),
            AnnotationSubject::Value(fragment, value) => self
                .values
                .get(&(fragment, value))
                .map(Vec::as_slice)
                .unwrap_or_default(),
        }
    }

    fn value(&self, subject: AnnotationSubject, key: &str) -> Option<&'a str> {
        match subject {
            AnnotationSubject::Plan => self.plan_values.get(key).copied(),
            AnnotationSubject::Fragment(fragment) => self
                .fragment_values
                .get(&fragment)
                .and_then(|values| values.get(key))
                .copied(),
            AnnotationSubject::Node(fragment, node) => self
                .node_values
                .get(&(fragment, node))
                .and_then(|values| values.get(key))
                .copied(),
            AnnotationSubject::Value(fragment, value) => self
                .value_values
                .get(&(fragment, value))
                .and_then(|values| values.get(key))
                .copied(),
        }
    }

    fn values_for_fragment(
        &self,
        fragment: FragmentId,
    ) -> impl Iterator<Item = (ValueId, &[&'a PlanAnnotation])> {
        self.values
            .range((fragment, ValueId::new(0))..=(fragment, ValueId::new(u32::MAX)))
            .map(|((_, value), annotations)| (*value, annotations.as_slice()))
    }
}

struct RenderContext<'a> {
    plan: &'a PhysicalPlan,
    level: ExplainLevel,
    profile: Option<&'a SqlCompletedExplainProfile>,
    annotations: AnnotationIndex<'a>,
    inbound_edges: BTreeMap<FragmentId, Vec<&'a Edge>>,
    outbound_edges: BTreeMap<FragmentId, Vec<&'a Edge>>,
    cuts: BTreeMap<FragmentId, FragmentCuts>,
}

impl<'a> RenderContext<'a> {
    fn new(
        plan: &'a PhysicalPlan,
        level: ExplainLevel,
        profile: Option<&'a SqlCompletedExplainProfile>,
    ) -> Result<Self, SqlCompileError> {
        let mut inbound_edges: BTreeMap<FragmentId, Vec<&Edge>> = BTreeMap::new();
        let mut outbound_edges: BTreeMap<FragmentId, Vec<&Edge>> = BTreeMap::new();
        for edge in plan.edges().values() {
            inbound_edges
                .entry(edge.destination.fragment)
                .or_default()
                .push(edge);
            outbound_edges
                .entry(edge.source.fragment)
                .or_default()
                .push(edge);
        }
        let cuts = novarocks_physical_plan::derive_fragment_cuts(plan).ok_or_else(|| {
            invalid_request("completed physical plan cannot derive fragment cut contracts")
        })?;
        Ok(Self {
            plan,
            level,
            profile,
            annotations: AnnotationIndex::new(plan),
            inbound_edges,
            outbound_edges,
            cuts,
        })
    }

    fn value_name(&self, fragment: FragmentId, value: ValueId) -> ValueNameDisplay<'_> {
        ValueNameDisplay {
            context: self,
            fragment,
            value,
        }
    }
}

struct ValueNameDisplay<'a> {
    context: &'a RenderContext<'a>,
    fragment: FragmentId,
    value: ValueId,
}

impl fmt::Display for ValueNameDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "v{}", self.value.get())?;
        if let Some(name) = self.context.annotations.value(
            AnnotationSubject::Value(self.fragment, self.value),
            "sql.display_name",
        ) {
            write!(formatter, "{{{name}}}")?;
        }
        Ok(())
    }
}

fn render_result_schema(
    plan: &PhysicalPlan,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    let Some(result) = plan.result_port() else {
        lines.push(format_args!("RESULT PORT: none"))?;
        return Ok(());
    };
    lines.push(format_args!(
        "RESULT PORT fragment={}, node={}",
        result.fragment.get(),
        result.output.node.get()
    ))?;
    for (ordinal, field) in result.fields.iter().enumerate() {
        lines.push(format_args!(
            "  field{} name={}, alias={}, value=v{}, type={}, {}",
            ordinal,
            quote_text(&field.name),
            OptionalQuoted(field.alias.as_deref()),
            field.value.get(),
            field.ty.data_type,
            if field.ty.nullable {
                "NULL"
            } else {
                "NOT NULL"
            }
        ))?;
    }
    Ok(())
}

fn render_artifact_references(
    context: &RenderContext<'_>,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    if context.plan.artifact_refs().is_empty() {
        return Ok(());
    }
    lines.push(format_args!("SEALED ARTIFACT REFERENCES"))?;
    for artifact in context.plan.artifact_refs().values() {
        lines.push(format_args!(
            "  artifact{} kind={} format={}@{} schema=[{}] source={} coverage={} location={} content-digest={} schema-digest={} objects={} rows={}",
            artifact.id.get(),
            artifact.kind.as_str(),
            artifact.format.id.as_str(),
            artifact.format.revision,
            joined(&artifact.schema, ",", |ty: &novarocks_physical_plan::ValueType, output: &mut fmt::Formatter<'_>| {
                format_value_type(ty).fmt(output)
            }),
            format_artifact_source(&artifact.source),
            format_coverage_set(&artifact.coverage),
            quote_text(&artifact.location),
            format_hex(&artifact.content_digest),
            format_hex(&artifact.schema_digest),
            artifact.object_count,
            artifact.row_count
        ))?;
    }
    Ok(())
}

fn render_edges(
    context: &RenderContext<'_>,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    if context.plan.edges().is_empty() {
        return Ok(());
    }
    lines.push(format_args!("EDGE GRAPH"))?;
    for edge in context.plan.edges().values() {
        render_edge(context, edge, lines)?;
    }
    Ok(())
}

fn render_cut_attachments(
    context: &RenderContext<'_>,
    fragment_id: FragmentId,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    let inbound = context
        .inbound_edges
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let outbound = context
        .outbound_edges
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    lines.push(format_args!(
        "  CUT ATTACHMENTS inbound=[{}], outbound=[{}]",
        joined(
            inbound,
            ", ",
            |edge: &&Edge, output: &mut fmt::Formatter<'_>| {
                write!(output, "edge{}:{}", edge.id.get(), edge_kind(edge.kind))
            }
        ),
        joined(
            outbound,
            ", ",
            |edge: &&Edge, output: &mut fmt::Formatter<'_>| {
                write!(output, "edge{}:{}", edge.id.get(), edge_kind(edge.kind))
            }
        )
    ))?;
    let cuts = context.cuts.get(&fragment_id).ok_or_else(|| {
        invalid_request("completed physical plan omitted a derived fragment cut contract")
    })?;
    for cut in &cuts.inbound {
        lines.push(format_args!(
            "    inbound edge{} kind={} source=f{} destination=n{} imports=[{}] source-distribution={} source-multiplicity={} destination-distribution={} destination-multiplicity={} source-bindings=[{}] source-free={} change-stream-writer={} writer-result={}",
            cut.edge.get(),
            edge_kind(cut.kind),
            cut.source_fragment.get(),
            cut.destination_node.get(),
            joined(&cut.imports, ",", |import: &novarocks_physical_plan::CutImport, output: &mut fmt::Formatter<'_>| write!(output, "v{}:{}->v{}", import.source.value.get(), format_value_type(&import.source.ty), import.destination.get())),
            format_distribution_complete(context, cut.source_fragment, &cut.partitioning.source),
            row_multiplicity(cut.partitioning.source_multiplicity),
            format_distribution_complete(context, fragment_id, &cut.partitioning.destination),
            row_multiplicity(cut.partitioning.destination_multiplicity),
            joined(&cut.source_bindings, ";", |binding: &novarocks_physical_plan::ArtifactSourceBinding, output: &mut fmt::Formatter<'_>| format_artifact_source(binding).fmt(output)),
            cut.has_source_free_rows,
            ChangeStreamWriterCutDisplay(cut.change_stream_writer.as_ref()),
            WriterResultCutDisplay(cut.writer_result.as_ref())
        ))?;
    }
    for cut in &cuts.outbound {
        lines.push(format_args!(
            "    outbound edge{} kind={} destination=f{} projection=[{}] destination-imports=[{}] source-distribution={} source-multiplicity={} destination-distribution={} destination-multiplicity={} source-bindings=[{}] source-free={} change-stream-writer={} writer-result={}",
            cut.edge.get(),
            edge_kind(cut.kind),
            cut.destination_fragment.get(),
            joined(&cut.projection, ",", |value: &novarocks_physical_plan::CutValue, output: &mut fmt::Formatter<'_>| write!(output, "v{}:{}", value.value.get(), format_value_type(&value.ty))),
            joined(&cut.destination_imports, ",", |import: &novarocks_physical_plan::CutImport, output: &mut fmt::Formatter<'_>| write!(output, "v{}:{}->v{}", import.source.value.get(), format_value_type(&import.source.ty), import.destination.get())),
            format_distribution_complete(context, fragment_id, &cut.partitioning.source),
            row_multiplicity(cut.partitioning.source_multiplicity),
            format_distribution_complete(context, cut.destination_fragment, &cut.partitioning.destination),
            row_multiplicity(cut.partitioning.destination_multiplicity),
            joined(&cut.source_bindings, ";", |binding: &novarocks_physical_plan::ArtifactSourceBinding, output: &mut fmt::Formatter<'_>| format_artifact_source(binding).fmt(output)),
            cut.has_source_free_rows,
            ChangeStreamWriterCutDisplay(cut.change_stream_writer.as_ref()),
            WriterResultCutDisplay(cut.writer_result.as_ref())
        ))?;
    }
    Ok(())
}

struct ChangeStreamWriterCutDisplay<'a>(Option<&'a novarocks_physical_plan::ChangeStreamWriterCut>);

impl fmt::Display for ChangeStreamWriterCutDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(proof) = self.0 else {
            return formatter.write_str("none");
        };
        write!(
            formatter,
            "{{route={}, target={}, fields=[{}]}}",
            format_hex(&proof.route_id.to_bytes()),
            proof.write_target_ordinal.get(),
            joined(
                &proof.fields,
                ",",
                |field: &novarocks_physical_plan::ChangeStreamWriterCutField,
                 output: &mut fmt::Formatter<'_>| write!(
                    output,
                    "{}:v{}->v{}",
                    format_hex(&field.token.to_bytes()),
                    field.source.get(),
                    field.destination.get()
                )
            )
        )
    }
}

struct WriterResultCutDisplay<'a>(Option<&'a novarocks_physical_plan::WriterResultCut>);

impl fmt::Display for WriterResultCutDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(proof) = self.0 else {
            return formatter.write_str("none");
        };
        write!(
            formatter,
            "{{target={}, schema-revision={}, fields=[{}]}}",
            proof.write_target_ordinal.get(),
            proof.schema_revision,
            joined(
                &proof.fields,
                ";",
                |field: &novarocks_physical_plan::WriterResultCutField,
                 output: &mut fmt::Formatter<'_>| write!(
                    output,
                    "v{}->v{}:{}:{}:{}",
                    field.source.get(),
                    field.destination.get(),
                    quote_text(&field.name),
                    format_value_type(&field.ty),
                    writer_relation_field_role(field.role)
                )
            )
        )
    }
}

fn render_sink(
    context: &RenderContext<'_>,
    fragment_id: FragmentId,
    sink: &FragmentSink,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    match sink {
        FragmentSink::Result => lines.push(format_args!("  SINK result"))?,
        FragmentSink::Stream { edge } => {
            lines.push(format_args!("  SINK stream edge=edge{}", edge.get()))?
        }
        FragmentSink::Multicast { edges } => lines.push(format_args!(
            "  SINK multicast edges=[{}]",
            format_edge_ids(edges)
        ))?,
        FragmentSink::Router { effect, routes } => {
            lines.push(format_args!(
                "  SINK router effect={} routes={}",
                context.value_name(fragment_id, *effect),
                routes.len()
            ))?;
            for route in routes {
                lines.push(format_args!(
                    "    route id={} target={} effects=[{}] input=[{}] partition-by=[{}] edge=edge{}",
                    format_hex(&route.route_id.to_bytes()),
                    route.write_target_ordinal.get(),
                    joined(&route.accepted_effects, ",", |effect: &novarocks_spi::connector::ConnectorRowMutationEffect, output: &mut fmt::Formatter<'_>| output.write_str(connector_row_mutation_effect(*effect))),
                    joined(&route.input_mapping, ",", |(token, value): &(novarocks_spi::connector::ConnectorWriteFieldToken, ValueId), output: &mut fmt::Formatter<'_>| write!(output, "{}->{}", format_hex(&token.to_bytes()), context.value_name(fragment_id, *value))),
                    joined(&route.partition_by, ",", |value: &ValueId, output: &mut fmt::Formatter<'_>| context.value_name(fragment_id, *value).fmt(output)),
                    route.edge.get()
                ))?;
            }
        }
        FragmentSink::SealedArtifact(spec) => lines.push(format_args!(
            "  SINK sealed-artifact kind={} format={}@{} input=[{}] partition-by=[{}] order-by=[{}] group-boundaries=[{}] source={} coverage={} max-reference-bytes={}",
            spec.kind.as_str(),
            spec.format.id.as_str(),
            spec.format.revision,
            joined(&spec.input, ",", |field: &novarocks_physical_plan::ArtifactInputField, output: &mut fmt::Formatter<'_>| write!(output, "{}:{}", context.value_name(fragment_id, field.value), format_value_type(&field.ty))),
            joined(&spec.partition_by, ",", |value: &ValueId, output: &mut fmt::Formatter<'_>| context.value_name(fragment_id, *value).fmt(output)),
            joined(&spec.order_by, ",", |key: &novarocks_physical_plan::ArtifactSortKey, output: &mut fmt::Formatter<'_>| write!(output, "{} {} NULLS {}", context.value_name(fragment_id, key.value), sort_direction(key.direction), null_ordering(key.null_ordering))),
            joined(&spec.group_boundaries, ",", |value: &ValueId, output: &mut fmt::Formatter<'_>| context.value_name(fragment_id, *value).fmt(output)),
            format_artifact_source(&spec.source),
            format_coverage_set(&spec.required_coverage),
            spec.max_reference_bytes
        ))?,
        FragmentSink::Noop => lines.push(format_args!("  SINK noop"))?,
    }
    Ok(())
}

fn render_expression_definitions(
    context: &RenderContext<'_>,
    fragment_id: FragmentId,
    fragment: &Fragment,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    if fragment.expressions().is_empty() {
        return Ok(());
    }
    lines.push(format_args!("  EXPRESSION DEFINITIONS"))?;
    for (id, expression) in fragment.expressions().iter() {
        lines.push(format_args!(
            "    e{} = {} : {}",
            id.get(),
            format_expr_definition(context, fragment_id, expression),
            format_value_type(&expression.ty)
        ))?;
    }
    Ok(())
}

fn render_value_definitions(
    context: &RenderContext<'_>,
    fragment_id: FragmentId,
    fragment: &Fragment,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    if fragment.values().is_empty() {
        return Ok(());
    }
    lines.push(format_args!("  VALUE DEFINITIONS"))?;
    for (id, value) in fragment.values() {
        lines.push(format_args!(
            "    {} : {} origin={}",
            context.value_name(fragment_id, *id),
            format_value_type(&value.ty),
            ValueOriginDisplay(&value.origin)
        ))?;
    }
    Ok(())
}

struct ValueOriginDisplay<'a>(&'a novarocks_physical_plan::ValueOrigin);

impl fmt::Display for ValueOriginDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        use novarocks_physical_plan::ValueOrigin;
        match self.0 {
            ValueOrigin::ProviderField { scan_node, field } => {
                let payload = &field.column_payload;
                let header = payload.header();
                write!(
                    formatter,
                    "provider-field(scan=n{}, column={{provider={}, catalog={}, catalog-version={}, category={}, revision={}, bytes={}, digest={}}})",
                    scan_node.get(),
                    header.provider_id().as_str(),
                    header.catalog().catalog_name().as_str(),
                    header.catalog().version().short_hex(),
                    connector_codec_category(header.category()),
                    header.codec_revision().get(),
                    payload.payload().len(),
                    digest_bytes(payload.payload())
                )
            }
            ValueOrigin::Expr { node, expr } => {
                write!(
                    formatter,
                    "expr(node=n{}, expr=e{})",
                    node.get(),
                    expr.get()
                )
            }
            ValueOrigin::NullExtended { node, of } => write!(
                formatter,
                "null-extended(node=n{}, of=v{})",
                node.get(),
                of.get()
            ),
            ValueOrigin::AggregateState { call, phase } => write!(
                formatter,
                "aggregate-state(call=a{}, phase={})",
                call.get(),
                aggregate_phase(*phase)
            ),
            ValueOrigin::AggregateResult { call } => {
                write!(formatter, "aggregate-result(call=a{})", call.get())
            }
            ValueOrigin::NodeOutput {
                node,
                output_ordinal,
            } => write!(
                formatter,
                "node-output(node=n{}, ordinal={output_ordinal})",
                node.get()
            ),
            ValueOrigin::ExchangeImport { edge, source_value } => write!(
                formatter,
                "exchange-import(edge=edge{}, source=v{})",
                edge.get(),
                source_value.get()
            ),
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value,
            } => write!(
                formatter,
                "cte-import(edge=edge{}, producer=f{}:v{})",
                edge.get(),
                producer_fragment.get(),
                producer_value.get()
            ),
            ValueOrigin::WriterDerived { writer_node, kind } => write!(
                formatter,
                "writer-derived(node=n{}, kind={})",
                writer_node.get(),
                writer_derived_kind(*kind)
            ),
        }
    }
}

fn writer_derived_kind(kind: novarocks_physical_plan::WriterDerivedKind) -> &'static str {
    use novarocks_physical_plan::WriterDerivedKind;
    match kind {
        WriterDerivedKind::RelationKind => "relation-kind",
        WriterDerivedKind::AffectedRows => "affected-rows",
        WriterDerivedKind::CommitFragment => "commit-fragment",
        WriterDerivedKind::ChangeEvent => "change-event",
        WriterDerivedKind::ArtifactReference => "artifact-reference",
        WriterDerivedKind::RelationAuxiliary => "relation-auxiliary",
        WriterDerivedKind::WriteTargetOrdinal => "write-target-ordinal",
        WriterDerivedKind::GroupingKey => "grouping-key",
    }
}

fn render_edge(
    context: &RenderContext<'_>,
    edge: &Edge,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    lines.push(format_args!(
        "  edge{} kind={} source={{fragment=f{}, projection=[{}], distribution={}, multiplicity={}}} destination={{fragment=f{}, node=n{}, mapping=[{}], distribution={}, multiplicity={}}}",
        edge.id.get(),
        edge_kind(edge.kind),
        edge.source.fragment.get(),
        joined(&edge.source.projection, ",", |value: &ValueId, output: &mut fmt::Formatter<'_>| context.value_name(edge.source.fragment, *value).fmt(output)),
        format_distribution_complete(
            context,
            edge.source.fragment,
            &edge.partitioning.source
        ),
        row_multiplicity(edge.partitioning.source_multiplicity),
        edge.destination.fragment.get(),
        edge.destination.node.get(),
        joined(&edge.destination.receive_mapping, ",", |(source, destination): &(ValueId, ValueId), output: &mut fmt::Formatter<'_>| write!(
            output,
            "f{}.{}->f{}.{}",
            edge.source.fragment.get(),
            context.value_name(edge.source.fragment, *source),
            edge.destination.fragment.get(),
            context.value_name(edge.destination.fragment, *destination)
        )),
        format_distribution_complete(
            context,
            edge.destination.fragment,
            &edge.partitioning.destination
        ),
        row_multiplicity(edge.partitioning.destination_multiplicity)
    ))
}

fn edge_kind(kind: novarocks_physical_plan::EdgeKind) -> &'static str {
    match kind {
        novarocks_physical_plan::EdgeKind::Stream => "stream",
        novarocks_physical_plan::EdgeKind::CteMulticast => "cte-multicast",
        novarocks_physical_plan::EdgeKind::ChangeStreamRouter => "change-stream-router",
    }
}

fn row_multiplicity(multiplicity: novarocks_physical_plan::RowMultiplicity) -> &'static str {
    match multiplicity {
        novarocks_physical_plan::RowMultiplicity::SingleCopy => "single-copy",
        novarocks_physical_plan::RowMultiplicity::Replicated => "replicated",
    }
}

struct DistributionDisplay<'a> {
    context: &'a RenderContext<'a>,
    fragment: FragmentId,
    distribution: &'a novarocks_physical_plan::Distribution,
}

impl fmt::Display for DistributionDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        use novarocks_physical_plan::Distribution;
        match self.distribution {
            Distribution::Unconstrained => formatter.write_str("unconstrained"),
            Distribution::Singleton => formatter.write_str("singleton"),
            Distribution::RoundRobin => formatter.write_str("round-robin"),
            Distribution::Broadcast => formatter.write_str("broadcast"),
            Distribution::Hash { keys, scheme } => write!(
                formatter,
                "hash(keys=[{}], space={}, count={{id={}, min={}, max={}, power-of-two={}}}, algorithm={})",
                joined(
                    keys,
                    ",",
                    |value: &ValueId, output: &mut fmt::Formatter<'_>| self
                        .context
                        .value_name(self.fragment, *value)
                        .fmt(output)
                ),
                format_hex(&scheme.space.as_bytes()),
                format_hex(&scheme.count.id.as_bytes()),
                scheme.count.admissible.min,
                scheme.count.admissible.max,
                scheme.count.admissible.requires_power_of_two,
                scheme.definition.algorithm.stable_name()
            ),
            Distribution::BucketShuffle { keys, scheme } => write!(
                formatter,
                "bucket-shuffle(keys=[{}], space={}, buckets={}, hash={}, layout={}, ordinal-domain={{first={}, count={}, digest={}}})",
                joined(
                    keys,
                    ",",
                    |value: &ValueId, output: &mut fmt::Formatter<'_>| self
                        .context
                        .value_name(self.fragment, *value)
                        .fmt(output)
                ),
                format_hex(&scheme.space.as_bytes()),
                scheme.bucket_count,
                scheme.hash.stable_name(),
                scheme.layout.stable_name(),
                scheme.ordinal_domain.first_ordinal,
                scheme.ordinal_domain.ordinal_count,
                format_hex(&scheme.ordinal_domain.evidence_digest)
            ),
        }
    }
}

fn format_distribution_complete<'a>(
    context: &'a RenderContext<'a>,
    fragment: FragmentId,
    distribution: &'a novarocks_physical_plan::Distribution,
) -> DistributionDisplay<'a> {
    DistributionDisplay {
        context,
        fragment,
        distribution,
    }
}

struct ArtifactSourceDisplay<'a>(&'a novarocks_physical_plan::ArtifactSourceBinding);

impl fmt::Display for ArtifactSourceDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{{read={}, selection-digest={}}}",
            format_provider_read(&self.0.source),
            format_hex(&self.0.selection_digest)
        )
    }
}

fn format_artifact_source(
    source: &novarocks_physical_plan::ArtifactSourceBinding,
) -> ArtifactSourceDisplay<'_> {
    ArtifactSourceDisplay(source)
}

struct ArtifactRequirementDisplay<'a>(&'a novarocks_physical_plan::ArtifactInputRequirement);

impl fmt::Display for ArtifactRequirementDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let requirement = self.0;
        write!(
            formatter,
            "artifact{} kind={} format={}@{} schema=[{}] source={} coverage={}",
            requirement.artifact.get(),
            requirement.kind.as_str(),
            requirement.format.id.as_str(),
            requirement.format.revision,
            joined(
                &requirement.schema,
                ",",
                |ty: &novarocks_physical_plan::ValueType, output: &mut fmt::Formatter<'_>| {
                    format_value_type(ty).fmt(output)
                }
            ),
            format_artifact_source(&requirement.source),
            format_coverage_set(&requirement.required_coverage)
        )
    }
}

fn format_artifact_requirement(
    requirement: &novarocks_physical_plan::ArtifactInputRequirement,
) -> ArtifactRequirementDisplay<'_> {
    ArtifactRequirementDisplay(requirement)
}

macro_rules! format_connector_payload {
    ($payload:expr) => {{ ConnectorPayloadDisplay($payload) }};
}

struct ConnectorPayloadDisplay<'a>(&'a novarocks_spi::connector::ConnectorEncodedPayload);

impl fmt::Display for ConnectorPayloadDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header = self.0.header();
        write!(
            formatter,
            "{{provider={}, catalog={}, catalog-version={}, category={}, revision={}, bytes={}, digest={}}}",
            header.provider_id().as_str(),
            header.catalog().catalog_name().as_str(),
            header.catalog().version().short_hex(),
            connector_codec_category(header.category()),
            header.codec_revision().get(),
            self.0.payload().len(),
            digest_bytes(self.0.payload())
        )
    }
}

struct ProviderReadDisplay<'a>(&'a novarocks_physical_plan::ProviderReadReference);

impl fmt::Display for ProviderReadDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let read = self.0;
        let descriptor = read.binding.descriptor();
        let catalog = read.binding.catalog_handle();
        write!(
            formatter,
            "{{provider={}, instance={}, catalog={}, catalog-version={}, input-version={{bytes={}, digest={}}}, relation-kind={}, table={}, view={}}}",
            descriptor.provider_id.as_str(),
            descriptor.instance_id.as_str(),
            catalog.catalog_name().as_str(),
            catalog.version().short_hex(),
            read.input_version.as_bytes().len(),
            digest_bytes(read.input_version.as_bytes()),
            connector_relation_kind(read.relation.kind()),
            format_connector_payload!(read.relation.table()),
            format_connector_payload!(read.relation.view())
        )
    }
}

fn format_provider_read(
    read: &novarocks_physical_plan::ProviderReadReference,
) -> ProviderReadDisplay<'_> {
    ProviderReadDisplay(read)
}

struct CoverageDisplay<'a>(&'a novarocks_physical_plan::CoverageSet);

impl fmt::Display for CoverageDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let coverage = self.0;
        write!(
            formatter,
            "{{domain={}, selection-digest={}, complete={}, ranges=[{}]}}",
            quote_text(&coverage.domain),
            format_hex(&coverage.selection_digest),
            coverage.complete_input,
            joined(
                &coverage.ranges,
                ",",
                |range: &novarocks_physical_plan::CoverageRange,
                 output: &mut fmt::Formatter<'_>| write!(
                    output,
                    "{}..{}",
                    CoverageBound {
                        value: range.start.as_deref(),
                        infinity: "-inf"
                    },
                    CoverageBound {
                        value: range.end.as_deref(),
                        infinity: "+inf"
                    }
                )
            )
        )
    }
}

fn format_coverage_set(coverage: &novarocks_physical_plan::CoverageSet) -> CoverageDisplay<'_> {
    CoverageDisplay(coverage)
}

fn connector_codec_category(
    category: novarocks_spi::connector::ConnectorCodecCategory,
) -> &'static str {
    match category {
        novarocks_spi::connector::ConnectorCodecCategory::ReadTable => "read-table",
        novarocks_spi::connector::ConnectorCodecCategory::ReadView => "read-view",
        novarocks_spi::connector::ConnectorCodecCategory::ReadColumn => "read-column",
        novarocks_spi::connector::ConnectorCodecCategory::ReadSplit => "read-split",
        novarocks_spi::connector::ConnectorCodecCategory::WriteHandle => "write-handle",
        novarocks_spi::connector::ConnectorCodecCategory::CommitFragment => "commit-fragment",
    }
}

fn connector_relation_kind(
    kind: novarocks_spi::connector::read_stack::ConnectorReadRelationKind,
) -> &'static str {
    match kind {
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table => "table",
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::TableFunction => {
            "table-function"
        }
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::ChangeWindow => {
            "change-window"
        }
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::SystemTable => {
            "system-table"
        }
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::TableExecute => {
            "table-execute"
        }
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::MergeTable => {
            "merge-table"
        }
    }
}

fn connector_row_mutation_effect(
    effect: novarocks_spi::connector::ConnectorRowMutationEffect,
) -> &'static str {
    match effect {
        novarocks_spi::connector::ConnectorRowMutationEffect::Delete => "delete",
        novarocks_spi::connector::ConnectorRowMutationEffect::Replace => "replace",
        novarocks_spi::connector::ConnectorRowMutationEffect::Insert => "insert",
    }
}

fn quote_text(value: &str) -> Quoted<'_> {
    Quoted(value)
}

struct ValueTypeDisplay<'a>(&'a novarocks_physical_plan::ValueType);

impl fmt::Display for ValueTypeDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/{}",
            self.0.data_type,
            if self.0.nullable {
                "nullable"
            } else {
                "required"
            }
        )
    }
}

fn format_value_type(value: &novarocks_physical_plan::ValueType) -> ValueTypeDisplay<'_> {
    ValueTypeDisplay(value)
}

fn digest_bytes(bytes: &[u8]) -> DigestBytes {
    DigestBytes(Sha256::digest(bytes).into())
}

struct ExprDefinitionDisplay<'a> {
    context: &'a RenderContext<'a>,
    fragment: FragmentId,
    expression: &'a novarocks_physical_plan::ExprNode,
}

impl fmt::Display for ExprDefinitionDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        use novarocks_physical_plan::ExprKind;
        let expr = |id: ExprId| id.get();
        match &self.expression.kind {
            ExprKind::Value(value) => self
                .context
                .value_name(self.fragment, *value)
                .fmt(formatter),
            ExprKind::LambdaParameter { lambda, ordinal } => {
                write!(
                    formatter,
                    "lambda-parameter(lambda=e{}, ordinal={ordinal})",
                    expr(*lambda)
                )
            }
            ExprKind::Literal(value) => format_literal(value).fmt(formatter),
            ExprKind::Unary { op, expr: inner } => write!(
                formatter,
                "({}e{})",
                match op {
                    novarocks_physical_plan::UnaryOperator::Plus => "+",
                    novarocks_physical_plan::UnaryOperator::Minus => "-",
                    novarocks_physical_plan::UnaryOperator::Not => "NOT ",
                    novarocks_physical_plan::UnaryOperator::BitwiseNot => "~",
                },
                expr(*inner)
            ),
            ExprKind::Conjunction { args } => write!(
                formatter,
                "({})",
                args.iter()
                    .map(|arg| format!("e{}", expr(*arg)))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            ExprKind::Disjunction { args } => write!(
                formatter,
                "({})",
                args.iter()
                    .map(|arg| format!("e{}", expr(*arg)))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            ExprKind::Binary { left, op, right } => write!(
                formatter,
                "(e{} {} e{})",
                expr(*left),
                binary_operator(*op),
                expr(*right)
            ),
            ExprKind::FunctionCall { function, args } => write!(
                formatter,
                "{}@{}({})",
                function.function_id.as_str(),
                function.overload.as_str(),
                joined(
                    args,
                    ", ",
                    |argument: &ExprId, output: &mut fmt::Formatter<'_>| write!(
                        output,
                        "e{}",
                        argument.get()
                    )
                )
            ),
            ExprKind::Lambda {
                parameter_types,
                body,
            } => write!(
                formatter,
                "lambda<{}> -> e{}",
                joined(
                    parameter_types,
                    ",",
                    |ty: &novarocks_physical_plan::ValueType, output: &mut fmt::Formatter<'_>| {
                        format_value_type(ty).fmt(output)
                    }
                ),
                expr(*body)
            ),
            ExprKind::Cast {
                expr: inner,
                target,
            } => {
                write!(formatter, "CAST(e{} AS {target})", expr(*inner))
            }
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => write!(
                formatter,
                "(e{} IS {}NULL)",
                expr(*inner),
                if *negated { "NOT " } else { "" }
            ),
            ExprKind::InList {
                expr: inner,
                list,
                negated,
            } => write!(
                formatter,
                "(e{} {}IN ({}))",
                expr(*inner),
                if *negated { "NOT " } else { "" },
                joined(
                    list,
                    ", ",
                    |item: &ExprId, output: &mut fmt::Formatter<'_>| write!(
                        output,
                        "e{}",
                        item.get()
                    )
                )
            ),
            ExprKind::Between {
                expr: inner,
                low,
                high,
                negated,
            } => write!(
                formatter,
                "(e{} {}BETWEEN e{} AND e{})",
                expr(*inner),
                if *negated { "NOT " } else { "" },
                expr(*low),
                expr(*high)
            ),
            ExprKind::Like {
                expr: inner,
                pattern,
                negated,
            } => write!(
                formatter,
                "(e{} {}LIKE e{})",
                expr(*inner),
                if *negated { "NOT " } else { "" },
                expr(*pattern)
            ),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                formatter.write_str("CASE")?;
                if let Some(operand) = operand {
                    write!(formatter, " e{}", expr(*operand))?;
                }
                for (when, then) in when_then {
                    write!(formatter, " WHEN e{} THEN e{}", expr(*when), expr(*then))?;
                }
                if let Some(otherwise) = else_expr {
                    write!(formatter, " ELSE e{}", expr(*otherwise))?;
                }
                formatter.write_str(" END")
            }
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => write!(
                formatter,
                "(e{} IS {}{})",
                expr(*inner),
                if *negated { "NOT " } else { "" },
                if *value { "TRUE" } else { "FALSE" }
            ),
            ExprKind::WindowCall {
                function,
                distinct,
                args,
                function_order_by,
                frame,
                ignore_nulls,
                aggregate_binding,
            } => {
                write!(
                    formatter,
                    "{}@{}({}{}) order-by=[{}] frame=",
                    function.function_id.as_str(),
                    function.overload.as_str(),
                    if *distinct { "DISTINCT " } else { "" },
                    joined(
                        args,
                        ", ",
                        |argument: &ExprId, output: &mut fmt::Formatter<'_>| write!(
                            output,
                            "e{}",
                            argument.get()
                        )
                    ),
                    format_sort_expr_refs(function_order_by)
                )?;
                match frame {
                    Some(frame) => write!(formatter, "{}", format_window_frame(frame))?,
                    None => formatter.write_str("none")?,
                }
                write!(formatter, " ignore-nulls={ignore_nulls} aggregate-binding=")?;
                match aggregate_binding {
                    Some(binding) => write!(
                        formatter,
                        "{}@{}:{}",
                        binding.function.function_id.as_str(),
                        binding.function.overload.as_str(),
                        aggregate_phase(binding.phase)
                    ),
                    None => formatter.write_str("none"),
                }
            }
        }
    }
}

fn format_expr_definition<'a>(
    context: &'a RenderContext<'a>,
    fragment_id: FragmentId,
    expression: &'a novarocks_physical_plan::ExprNode,
) -> ExprDefinitionDisplay<'a> {
    ExprDefinitionDisplay {
        context,
        fragment: fragment_id,
        expression,
    }
}

fn binary_operator(operator: novarocks_physical_plan::BinaryOperator) -> &'static str {
    match operator {
        novarocks_physical_plan::BinaryOperator::Add => "+",
        novarocks_physical_plan::BinaryOperator::Subtract => "-",
        novarocks_physical_plan::BinaryOperator::Multiply => "*",
        novarocks_physical_plan::BinaryOperator::Divide => "/",
        novarocks_physical_plan::BinaryOperator::Modulo => "%",
        novarocks_physical_plan::BinaryOperator::Eq => "=",
        novarocks_physical_plan::BinaryOperator::EqForNull => "<=>",
        novarocks_physical_plan::BinaryOperator::NotEq => "!=",
        novarocks_physical_plan::BinaryOperator::Lt => "<",
        novarocks_physical_plan::BinaryOperator::LtEq => "<=",
        novarocks_physical_plan::BinaryOperator::Gt => ">",
        novarocks_physical_plan::BinaryOperator::GtEq => ">=",
        novarocks_physical_plan::BinaryOperator::BitAnd => "&",
        novarocks_physical_plan::BinaryOperator::BitOr => "|",
        novarocks_physical_plan::BinaryOperator::BitXor => "^",
    }
}

fn format_sort_expr_refs(expressions: &[SortExpr]) -> SortExprsDisplay<'_> {
    SortExprsDisplay(expressions)
}

fn format_window_frame(frame: &novarocks_physical_plan::WindowFrame) -> String {
    format!(
        "{} BETWEEN {} AND {} EXCLUDE {}",
        match frame.units {
            novarocks_physical_plan::WindowFrameUnits::Rows => "ROWS",
            novarocks_physical_plan::WindowFrameUnits::Range => "RANGE",
            novarocks_physical_plan::WindowFrameUnits::Groups => "GROUPS",
        },
        format_window_bound(&frame.start),
        format_window_bound(&frame.end),
        match frame.exclusion {
            novarocks_physical_plan::WindowFrameExclusion::NoOthers => "NO OTHERS",
            novarocks_physical_plan::WindowFrameExclusion::CurrentRow => "CURRENT ROW",
            novarocks_physical_plan::WindowFrameExclusion::Group => "GROUP",
            novarocks_physical_plan::WindowFrameExclusion::Ties => "TIES",
        }
    )
}

fn format_window_bound(bound: &novarocks_physical_plan::WindowBound) -> String {
    match bound {
        novarocks_physical_plan::WindowBound::UnboundedPreceding => {
            "UNBOUNDED PRECEDING".to_string()
        }
        novarocks_physical_plan::WindowBound::Preceding(expression) => {
            format!("e{} PRECEDING", expression.get())
        }
        novarocks_physical_plan::WindowBound::CurrentRow => "CURRENT ROW".to_string(),
        novarocks_physical_plan::WindowBound::Following(expression) => {
            format!("e{} FOLLOWING", expression.get())
        }
        novarocks_physical_plan::WindowBound::UnboundedFollowing => {
            "UNBOUNDED FOLLOWING".to_string()
        }
    }
}

fn aggregate_phase(phase: novarocks_physical_plan::AggregatePhase) -> String {
    match phase {
        novarocks_physical_plan::AggregatePhase::Single => "single".to_string(),
        novarocks_physical_plan::AggregatePhase::Partial { sequence } => {
            format!("partial(sequence={})", sequence.get())
        }
        novarocks_physical_plan::AggregatePhase::Intermediate { sequence } => {
            format!("intermediate(sequence={})", sequence.get())
        }
        novarocks_physical_plan::AggregatePhase::Final { sequence } => {
            format!("final(sequence={})", sequence.get())
        }
    }
}

fn render_fragment_profile(
    observation: &SqlExplainObservation<SqlExplainFragmentMetrics>,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    match observation {
        SqlExplainObservation::Available(metrics) => lines.push(format_args!(
            "  Profile: active={}ns, blocked={}ns, dependency-wait={}ns, exchange-wait={}ns, network={}ns, scan-io={}ns",
            metrics.operator_active_time_ns,
            metrics.driver_blocked_time_ns,
            metrics.dependency_wait_time_ns,
            metrics.exchange_wait_time_ns,
            metrics.network_time_ns,
            metrics.scan_io_time_ns
        )),
        SqlExplainObservation::Unavailable(reason) => lines.push(format_args!(
            "  Profile: unavailable={}",
            reason.stable_name()
        )),
    }
}

fn render_runtime_filters(
    context: &RenderContext<'_>,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    if context.plan.runtime_filters().is_empty() {
        return Ok(());
    }
    lines.push(format_args!("RUNTIME FILTER GRAPH"))?;
    for filter in context.plan.runtime_filters().values() {
        render_runtime_filter(context, filter, lines)?;
    }
    Ok(())
}

fn render_runtime_filter(
    context: &RenderContext<'_>,
    filter: &novarocks_physical_plan::RuntimeFilter,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    lines.push(format_args!(
        "  RF{} kind={}, lifecycle={}, reduction={}",
        filter.id.get(),
        runtime_filter_kind(filter.kind),
        runtime_filter_lifecycle(filter.lifecycle),
        runtime_filter_reduction(filter.reduction)
    ))?;
    lines.push(format_args!(
        "    domain = {}",
        runtime_filter_domain(&filter.domain)
    ))?;
    lines.push(format_args!(
        "    availability-coverage={}, terminal-coverage={}, policy={{contribution-bytes={}, artifact-bytes={}, deadline-ms={}, retries={}}}",
        runtime_filter_coverage(&filter.availability_coverage),
        runtime_filter_coverage(&filter.terminal_coverage),
        filter.policy.max_contribution_bytes,
        filter.policy.max_artifact_bytes,
        filter.policy.deadline_ms,
        filter.policy.max_retries
    ))?;
    for witness in &filter.equality_witnesses {
        lines.push(format_args!(
            "    equality-witness ew{} fragment={} join=n{} key-ordinal={} domain-side={}",
            witness.id.get(),
            witness.fragment.get(),
            witness.join.get(),
            witness.key_ordinal,
            join_side(witness.domain_side)
        ))?;
    }
    for producer in &filter.producers {
        lines.push(format_args!(
            "    producer binding witness=w{} fragment={} node={} apply={} values=[{}]",
            producer.witness.get(),
            producer.endpoint.fragment.get(),
            producer.endpoint.node.get(),
            runtime_filter_apply_point(producer.apply_point),
            joined(
                &producer.endpoint.values,
                ", ",
                |value: &ValueId, output: &mut fmt::Formatter<'_>| context
                    .value_name(producer.endpoint.fragment, *value)
                    .fmt(output)
            )
        ))?;
        lines.push(format_args!(
            "      target = {}",
            runtime_filter_producer_target(producer.target)
        ))?;
        lines.push(format_args!(
            "      contribution=[{}], completion={}, progress={{build-edges=[{}], non-build-edges=[{}]}}",
            joined(&producer.contribution_kinds, ",", |kind: &novarocks_physical_plan::RuntimeFilterContributionKind, output: &mut fmt::Formatter<'_>| output.write_str(runtime_filter_contribution(*kind))),
            runtime_filter_completion(producer.completion),
            format_edge_ids(&producer.progress.build_edges),
            format_edge_ids(&producer.progress.non_build_edges)
        ))?;
    }
    for consumer in &filter.consumers {
        lines.push(format_args!(
            "    consumer binding fragment={} node={} apply={} values=[{}]",
            consumer.endpoint.fragment.get(),
            consumer.endpoint.node.get(),
            runtime_filter_apply_point(consumer.apply_point),
            joined(
                &consumer.endpoint.values,
                ", ",
                |value: &ValueId, output: &mut fmt::Formatter<'_>| context
                    .value_name(consumer.endpoint.fragment, *value)
                    .fmt(output)
            )
        ))?;
        lines.push(format_args!(
            "      target = {}, activation = {}, capabilities=[{}]",
            runtime_filter_consumer_target(&consumer.target),
            runtime_filter_activation(consumer.activation),
            joined(
                &consumer.capabilities,
                ",",
                |capability: &novarocks_physical_plan::RuntimeFilterArtifactCapability,
                 output: &mut fmt::Formatter<'_>| output
                    .write_str(runtime_filter_capability(*capability))
            )
        ))?;
    }
    Ok(())
}

struct RuntimeFilterDomainDisplay<'a>(&'a novarocks_physical_plan::RuntimeFilterDomain);

impl fmt::Display for RuntimeFilterDomainDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            novarocks_physical_plan::RuntimeFilterDomain::Membership { ty, null_semantics } => {
                write!(
                    formatter,
                    "Membership(key={}{}, nulls={})",
                    ty.data_type,
                    if ty.nullable { " NULLABLE" } else { "" },
                    runtime_filter_null_semantics(*null_semantics)
                )
            }
            novarocks_physical_plan::RuntimeFilterDomain::Ordered {
                key,
                inclusive,
                comparator,
            } => write!(
                formatter,
                "OrderedBound(key={} {} NULLS {}, inclusive={inclusive}, comparator={})",
                key.ty.data_type,
                sort_direction(key.direction),
                null_ordering(key.null_ordering),
                comparator.stable_name()
            ),
        }
    }
}

fn runtime_filter_domain(
    domain: &novarocks_physical_plan::RuntimeFilterDomain,
) -> RuntimeFilterDomainDisplay<'_> {
    RuntimeFilterDomainDisplay(domain)
}

fn runtime_filter_kind(kind: novarocks_physical_plan::RuntimeFilterKind) -> &'static str {
    match kind {
        novarocks_physical_plan::RuntimeFilterKind::Bloom => "bloom",
        novarocks_physical_plan::RuntimeFilterKind::MinMax => "min-max",
        novarocks_physical_plan::RuntimeFilterKind::InList => "in-list",
    }
}

fn runtime_filter_lifecycle(
    lifecycle: novarocks_physical_plan::RuntimeFilterLifecycle,
) -> &'static str {
    match lifecycle {
        novarocks_physical_plan::RuntimeFilterLifecycle::CompleteOnce => "complete-once",
        novarocks_physical_plan::RuntimeFilterLifecycle::MonotonicUpdates => "monotonic-updates",
    }
}

fn runtime_filter_reduction(
    reduction: novarocks_physical_plan::RuntimeFilterReduction,
) -> &'static str {
    match reduction {
        novarocks_physical_plan::RuntimeFilterReduction::SetUnion => "set-union",
        novarocks_physical_plan::RuntimeFilterReduction::TightenOrderedBound => {
            "tighten-ordered-bound"
        }
        novarocks_physical_plan::RuntimeFilterReduction::UnionOrderedHull => "union-ordered-hull",
    }
}

fn runtime_filter_null_semantics(
    semantics: novarocks_physical_plan::RuntimeFilterNullSemantics,
) -> &'static str {
    match semantics {
        novarocks_physical_plan::RuntimeFilterNullSemantics::NeverMatches => "never-matches",
        novarocks_physical_plan::RuntimeFilterNullSemantics::NullSafeEqual => "null-safe-equal",
    }
}

struct RuntimeFilterCoverageDisplay<'a>(&'a novarocks_physical_plan::RuntimeFilterCoverage);

impl fmt::Display for RuntimeFilterCoverageDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{{root=c{}, nodes=[", self.0.root)?;
        for (index, node) in self.0.nodes.iter().enumerate() {
            if index != 0 {
                formatter.write_str(";")?;
            }
            match node {
                novarocks_physical_plan::RuntimeFilterCoverageNode::Witness(witness) => {
                    write!(formatter, "c{index}:w{}", witness.get())?;
                }
                novarocks_physical_plan::RuntimeFilterCoverageNode::AllOf { children }
                | novarocks_physical_plan::RuntimeFilterCoverageNode::AnyOf { children } => {
                    let operation = if matches!(
                        node,
                        novarocks_physical_plan::RuntimeFilterCoverageNode::AllOf { .. }
                    ) {
                        "all"
                    } else {
                        "any"
                    };
                    write!(
                        formatter,
                        "c{index}:{operation}({})",
                        joined(
                            children,
                            ",",
                            |child: &u32, output: &mut fmt::Formatter<'_>| write!(
                                output,
                                "c{child}"
                            )
                        )
                    )?;
                }
            }
        }
        formatter.write_str("]}")
    }
}

fn runtime_filter_coverage(
    coverage: &novarocks_physical_plan::RuntimeFilterCoverage,
) -> RuntimeFilterCoverageDisplay<'_> {
    RuntimeFilterCoverageDisplay(coverage)
}

fn runtime_filter_producer_target(
    target: novarocks_physical_plan::RuntimeFilterProducerTarget,
) -> String {
    match target {
        novarocks_physical_plan::RuntimeFilterProducerTarget::JoinBuildKey { equality } => {
            format!("JoinBuildKey(equality={})", equality.get())
        }
        novarocks_physical_plan::RuntimeFilterProducerTarget::AggregateTopNKey {
            group_key_ordinal,
            topn,
            phase,
            order_key_ordinal,
            limit,
            offset,
            direction,
            null_ordering: nulls,
        } => format!(
            "AggregateTopNKey(group_key_ordinal={group_key_ordinal}, topn=n{}, phase={}, order_key_ordinal={order_key_ordinal}, limit={limit}, offset={offset}, direction={}, null_ordering={})",
            topn.get(),
            topn_phase(phase),
            sort_direction(direction),
            null_ordering(nulls)
        ),
    }
}

struct RuntimeFilterConsumerTargetDisplay<'a>(
    &'a novarocks_physical_plan::RuntimeFilterConsumerTarget,
);

impl fmt::Display for RuntimeFilterConsumerTargetDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            novarocks_physical_plan::RuntimeFilterConsumerTarget::JoinProbeKey { equality } => {
                write!(formatter, "JoinProbeKey(equality={})", equality.get())
            }
            novarocks_physical_plan::RuntimeFilterConsumerTarget::ScanField {
                equality,
                lineage,
            } => write!(
                formatter,
                "ScanField(equality={}, lineage=[{}])",
                equality.get(),
                format_runtime_filter_lineage(lineage)
            ),
            novarocks_physical_plan::RuntimeFilterConsumerTarget::AggregateTopNScanField {
                producer,
                lineage,
            } => write!(
                formatter,
                "AggregateTopNScanField(producer={}, lineage=[{}])",
                producer.get(),
                format_runtime_filter_lineage(lineage)
            ),
        }
    }
}

fn runtime_filter_consumer_target(
    target: &novarocks_physical_plan::RuntimeFilterConsumerTarget,
) -> RuntimeFilterConsumerTargetDisplay<'_> {
    RuntimeFilterConsumerTargetDisplay(target)
}

fn runtime_filter_activation(
    activation: novarocks_physical_plan::RuntimeFilterConsumerActivation,
) -> String {
    match activation {
        novarocks_physical_plan::RuntimeFilterConsumerActivation::BlockingSnapshot => {
            "BlockingSnapshot".to_string()
        }
        novarocks_physical_plan::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
            late_apply,
        } => format!(
            "StartUnfilteredThenApplyComplete({})",
            late_apply_granularity(late_apply)
        ),
        novarocks_physical_plan::RuntimeFilterConsumerActivation::NonBlockingLive {
            late_apply,
        } => format!("NonBlockingLive({})", late_apply_granularity(late_apply)),
    }
}

fn runtime_filter_apply_point(point: novarocks_physical_plan::RuntimeFilterApplyPoint) -> String {
    match point {
        novarocks_physical_plan::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
            format!("node-input({input_ordinal})")
        }
        novarocks_physical_plan::RuntimeFilterApplyPoint::NodeOutput => "node-output".to_string(),
        novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource => "scan-source".to_string(),
    }
}

fn runtime_filter_contribution(
    kind: novarocks_physical_plan::RuntimeFilterContributionKind,
) -> &'static str {
    match kind {
        novarocks_physical_plan::RuntimeFilterContributionKind::ValueDomainDelta => {
            "value-domain-delta"
        }
        novarocks_physical_plan::RuntimeFilterContributionKind::FinalDomainShard => {
            "final-domain-shard"
        }
        novarocks_physical_plan::RuntimeFilterContributionKind::OrderedBoundUpdate => {
            "ordered-bound-update"
        }
        novarocks_physical_plan::RuntimeFilterContributionKind::FinalOrderedHullShard => {
            "final-ordered-hull-shard"
        }
        novarocks_physical_plan::RuntimeFilterContributionKind::ProducerClosed => "producer-closed",
    }
}

fn runtime_filter_completion(
    completion: novarocks_physical_plan::RuntimeFilterCompletion,
) -> &'static str {
    match completion {
        novarocks_physical_plan::RuntimeFilterCompletion::ProducerClosed => "producer-closed",
        novarocks_physical_plan::RuntimeFilterCompletion::FencedCommittedDomain => {
            "fenced-committed-domain"
        }
    }
}

fn runtime_filter_capability(
    capability: novarocks_physical_plan::RuntimeFilterArtifactCapability,
) -> &'static str {
    match capability {
        novarocks_physical_plan::RuntimeFilterArtifactCapability::Membership => "membership",
        novarocks_physical_plan::RuntimeFilterArtifactCapability::OrderedRange => "ordered-range",
        novarocks_physical_plan::RuntimeFilterArtifactCapability::EmptyDomain => "empty-domain",
    }
}

fn late_apply_granularity(
    granularity: novarocks_physical_plan::LateApplyGranularity,
) -> &'static str {
    match granularity {
        novarocks_physical_plan::LateApplyGranularity::Row => "row",
        novarocks_physical_plan::LateApplyGranularity::Batch => "batch",
        novarocks_physical_plan::LateApplyGranularity::RowGroup => "row-group",
        novarocks_physical_plan::LateApplyGranularity::Split => "split",
        novarocks_physical_plan::LateApplyGranularity::File => "file",
    }
}

fn sort_direction(direction: novarocks_physical_plan::SortDirection) -> &'static str {
    match direction {
        novarocks_physical_plan::SortDirection::Ascending => "ASC",
        novarocks_physical_plan::SortDirection::Descending => "DESC",
    }
}

fn null_ordering(ordering: novarocks_physical_plan::NullOrdering) -> &'static str {
    match ordering {
        novarocks_physical_plan::NullOrdering::First => "FIRST",
        novarocks_physical_plan::NullOrdering::Last => "LAST",
    }
}

fn topn_phase(phase: novarocks_physical_plan::TopNPhase) -> String {
    match phase {
        novarocks_physical_plan::TopNPhase::Single => "single".to_string(),
        novarocks_physical_plan::TopNPhase::Partial { sequence } => {
            format!("partial(sequence={})", sequence.get())
        }
        novarocks_physical_plan::TopNPhase::Final { sequence } => {
            format!("final(sequence={})", sequence.get())
        }
    }
}

fn join_side(side: novarocks_physical_plan::JoinSide) -> &'static str {
    match side {
        novarocks_physical_plan::JoinSide::Left => "left",
        novarocks_physical_plan::JoinSide::Right => "right",
    }
}

fn connector_read_work_source(
    source: novarocks_spi::connector::read_stack::ConnectorReadWorkSource,
) -> &'static str {
    match source {
        novarocks_spi::connector::read_stack::ConnectorReadWorkSource::RuntimeSplits => {
            "runtime-splits"
        }
        novarocks_spi::connector::read_stack::ConnectorReadWorkSource::WholeRelation => {
            "whole-relation"
        }
    }
}

fn predicate_guarantee_kind(kind: novarocks_physical_plan::PredicateGuaranteeKind) -> &'static str {
    match kind {
        novarocks_physical_plan::PredicateGuaranteeKind::Exact => "exact",
        novarocks_physical_plan::PredicateGuaranteeKind::PruningOnly => "pruning-only",
    }
}

fn nest_loop_distribution(
    distribution: novarocks_physical_plan::NestLoopJoinDistribution,
) -> &'static str {
    match distribution {
        novarocks_physical_plan::NestLoopJoinDistribution::Singleton => "singleton",
        novarocks_physical_plan::NestLoopJoinDistribution::BroadcastRight => "broadcast-right",
    }
}

fn set_operation_kind(kind: novarocks_physical_plan::SetOperationKind) -> &'static str {
    match kind {
        novarocks_physical_plan::SetOperationKind::UnionAll => "union-all",
        novarocks_physical_plan::SetOperationKind::Intersect => "intersect",
        novarocks_physical_plan::SetOperationKind::Except => "except",
    }
}

struct TableFunctionOutputsDisplay<'a>(&'a [novarocks_physical_plan::TableFunctionOutput]);

impl fmt::Display for TableFunctionOutputsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, output) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(",")?;
            }
            match output {
                novarocks_physical_plan::TableFunctionOutput::PassThrough(value) => {
                    write!(formatter, "pass-through(v{})", value.get())?
                }
                novarocks_physical_plan::TableFunctionOutput::FunctionResult {
                    result_ordinal,
                    value,
                } => write!(
                    formatter,
                    "result(ordinal={result_ordinal}, value=v{})",
                    value.get()
                )?,
            }
        }
        Ok(())
    }
}

fn format_table_function_outputs(
    outputs: &[novarocks_physical_plan::TableFunctionOutput],
) -> TableFunctionOutputsDisplay<'_> {
    TableFunctionOutputsDisplay(outputs)
}

struct RowCountAssertionDisplay<'a>(&'a novarocks_physical_plan::RowCountAssertionSpec);

impl fmt::Display for RowCountAssertionDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            novarocks_physical_plan::RowCountAssertionSpec::Global {
                subject,
                desired_rows,
                comparison,
            } => write!(
                formatter,
                "global(subject={}, comparison={}, rows={desired_rows})",
                quote_text(subject),
                row_count_comparison(*comparison)
            ),
            novarocks_physical_plan::RowCountAssertionSpec::PerKeyAtMostOne {
                keys,
                labels,
                message,
            } => {
                write!(
                    formatter,
                    "per-key-at-most-one(keys=[{}], labels=[",
                    format_value_ids(keys)
                )?;
                for (index, label) in labels.iter().enumerate() {
                    if index != 0 {
                        formatter.write_str(",")?;
                    }
                    quote_text(label).fmt(formatter)?;
                }
                write!(formatter, "], message={})", quote_text(message))
            }
        }
    }
}

fn format_row_count_assertion(
    assertion: &novarocks_physical_plan::RowCountAssertionSpec,
) -> RowCountAssertionDisplay<'_> {
    RowCountAssertionDisplay(assertion)
}

fn row_count_comparison(comparison: novarocks_physical_plan::RowCountAssertion) -> &'static str {
    match comparison {
        novarocks_physical_plan::RowCountAssertion::Eq => "eq",
        novarocks_physical_plan::RowCountAssertion::Ne => "ne",
        novarocks_physical_plan::RowCountAssertion::Lt => "lt",
        novarocks_physical_plan::RowCountAssertion::Le => "le",
        novarocks_physical_plan::RowCountAssertion::Gt => "gt",
        novarocks_physical_plan::RowCountAssertion::Ge => "ge",
    }
}

fn writer_relation_field_role(
    role: novarocks_physical_plan::WriterRelationFieldRole,
) -> &'static str {
    match role {
        novarocks_physical_plan::WriterRelationFieldRole::Kind => "kind",
        novarocks_physical_plan::WriterRelationFieldRole::TargetOrdinal => "target-ordinal",
        novarocks_physical_plan::WriterRelationFieldRole::RowCount => "row-count",
        novarocks_physical_plan::WriterRelationFieldRole::CommitFragment => "commit-fragment",
        novarocks_physical_plan::WriterRelationFieldRole::Auxiliary => "auxiliary",
    }
}

struct EdgeIdsDisplay<'a>(&'a [novarocks_physical_plan::EdgeId]);

impl fmt::Display for EdgeIdsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        joined(
            self.0,
            ",",
            |edge: &novarocks_physical_plan::EdgeId, output: &mut fmt::Formatter<'_>| {
                write!(output, "edge{}", edge.get())
            },
        )
        .fmt(formatter)
    }
}

fn format_edge_ids(edges: &[novarocks_physical_plan::EdgeId]) -> EdgeIdsDisplay<'_> {
    EdgeIdsDisplay(edges)
}

struct RuntimeFilterLineageDisplay<'a>(&'a [novarocks_physical_plan::RuntimeFilterLineageStep]);

impl fmt::Display for RuntimeFilterLineageDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, step) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(" -> ")?;
            }
            match step {
                novarocks_physical_plan::RuntimeFilterLineageStep::FilterPassThrough {
                    fragment,
                    node,
                    input_ordinal,
                } => write!(
                    formatter,
                    "filter-pass(f{},n{},input={input_ordinal})",
                    fragment.get(),
                    node.get()
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::SortPassThrough {
                    fragment,
                    node,
                    input_ordinal,
                } => write!(
                    formatter,
                    "sort-pass(f{},n{},input={input_ordinal})",
                    fragment.get(),
                    node.get()
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::ProjectIdentity {
                    fragment,
                    node,
                    output_ordinal,
                } => write!(
                    formatter,
                    "project-identity(f{},n{},output={output_ordinal})",
                    fragment.get(),
                    node.get()
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::JoinOutputPassThrough {
                    fragment,
                    node,
                    input_ordinal,
                } => write!(
                    formatter,
                    "join-pass(f{},n{},input={input_ordinal})",
                    fragment.get(),
                    node.get()
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::JoinEquality {
                    fragment,
                    node,
                    key_ordinal,
                    source_side,
                    target_side,
                } => write!(
                    formatter,
                    "join-equality(f{},n{},key={key_ordinal},source={},target={})",
                    fragment.get(),
                    node.get(),
                    join_side(*source_side),
                    join_side(*target_side)
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::AggregateGroupKey {
                    fragment,
                    node,
                    group_key_ordinal,
                } => write!(
                    formatter,
                    "aggregate-group(f{},n{},key={group_key_ordinal})",
                    fragment.get(),
                    node.get()
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::UnionAllBranch {
                    fragment,
                    node,
                    input_ordinal,
                    output_ordinal,
                } => write!(
                    formatter,
                    "union-all(f{},n{},input={input_ordinal},output={output_ordinal})",
                    fragment.get(),
                    node.get()
                )?,
                novarocks_physical_plan::RuntimeFilterLineageStep::ExchangeMapping {
                    edge,
                    mapping_ordinal,
                } => write!(
                    formatter,
                    "exchange(edge{},mapping={mapping_ordinal})",
                    edge.get()
                )?,
            }
        }
        Ok(())
    }
}

fn format_runtime_filter_lineage(
    lineage: &[novarocks_physical_plan::RuntimeFilterLineageStep],
) -> RuntimeFilterLineageDisplay<'_> {
    RuntimeFilterLineageDisplay(lineage)
}

fn render_nodes_iterative(
    context: &RenderContext<'_>,
    fragment_id: FragmentId,
    fragment: &Fragment,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    const MAX_INDENT_BYTES: usize = 64;
    let mut rendered = BTreeSet::new();
    let mut pending = vec![(fragment.root(), 1_usize)];
    while let Some((node_id, depth)) = pending.pop() {
        let pad_width = depth.saturating_mul(2).min(MAX_INDENT_BYTES);
        lines.ensure_prefix_fits(pad_width)?;
        let pad = " ".repeat(pad_width);
        if !rendered.insert(node_id) {
            lines.push(format_args!("{pad}NODE {} (shared)", node_id.get()))?;
            continue;
        }
        let Some(node) = fragment.nodes().get(&node_id) else {
            lines.push(format_args!("{pad}UNKNOWN NODE {}", node_id.get()))?;
            continue;
        };
        lines.push(format_args!(
            "{pad}{} [fragment={}, node={}, depth={depth}]",
            node_header(context, fragment_id, node),
            fragment_id.get(),
            node.id.get()
        ))?;
        let mut contract = NodeContractLines::new(lines);
        render_node_contract(context, fragment_id, fragment, node, &pad, &mut contract);
        contract.finish()?;
        if is_detailed(context.level) {
            render_annotations(
                context,
                AnnotationSubject::Node(fragment_id, node.id),
                &format!("{pad}  "),
                lines,
            )?;
        }
        if let Some(observation) = context.profile.and_then(|profile| {
            profile
                .operators
                .get(&SqlExplainNodeKey::new(fragment_id, node.id))
        }) {
            render_operator_profile(observation, &pad, lines)?;
        }
        pending.extend(
            node.inputs
                .iter()
                .rev()
                .map(|input| (*input, depth.saturating_add(1))),
        );
    }
    Ok(())
}

fn render_operator_profile(
    observation: &SqlExplainObservation<SqlExplainOperatorMetrics>,
    pad: &str,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    match observation {
        SqlExplainObservation::Available(metrics) => lines.push(format_args!(
            "{pad}  act={{rows={}, time={}ns, min={}ns, max={}ns, peak-mem={}B, build-ht={}ns, search={}ns, out-build={}ns, out-probe={}ns, dict={{in_rows={}, kept_rows={}, hydrated_rows={}, in_cols={}, kept_cols={}, hydrated_cols={}, unsupported_cols={}}}}}",
            metrics.output_rows,
            metrics.total_time_ns,
            metrics.total_time_min_ns,
            metrics.total_time_max_ns,
            metrics.peak_mem_bytes,
            metrics.build_ht_ns,
            metrics.search_ns,
            metrics.out_build_ns,
            metrics.out_probe_ns,
            metrics.dict_input_rows,
            metrics.dict_kept_rows,
            metrics.dict_hydrated_rows,
            metrics.dict_input_columns,
            metrics.dict_kept_columns,
            metrics.dict_hydrated_columns,
            metrics.dict_unsupported_columns
        )),
        SqlExplainObservation::Unavailable(reason) => lines.push(format_args!(
            "{pad}  act={{unavailable={}}}",
            reason.stable_name()
        )),
    }
}

struct NodeContractLines<'a> {
    output: &'a mut ExplainRenderOutput,
    error: Option<SqlCompileError>,
}

impl<'a> NodeContractLines<'a> {
    fn new(output: &'a mut ExplainRenderOutput) -> Self {
        Self {
            output,
            error: None,
        }
    }

    fn push(&mut self, arguments: impl fmt::Display) {
        if self.error.is_none()
            && let Err(error) = self.output.push(format_args!("{arguments}"))
        {
            self.error = Some(error);
        }
    }

    fn finish(self) -> Result<(), SqlCompileError> {
        match self.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn render_node_contract(
    context: &RenderContext<'_>,
    fragment_id: FragmentId,
    fragment: &Fragment,
    node: &PhysicalNode,
    pad: &str,
    lines: &mut NodeContractLines<'_>,
) {
    let plan = context.plan;
    let level = context.level;
    if matches!(
        level,
        ExplainLevel::Verbose | ExplainLevel::Costs | ExplainLevel::Analyze
    ) {
        lines.push(format_args!(
            "{pad}  output values: [{}]",
            joined(
                &node.output.columns,
                ", ",
                |value: &ValueId, output: &mut fmt::Formatter<'_>| {
                    write!(output, "v{}", value.get())
                }
            )
        ));
        for (ordinal, properties) in node.required_inputs.iter().enumerate() {
            lines.push(format_args!(
                "{pad}  required-input[{ordinal}] distribution={}, multiplicity={}, ordering=[{}]",
                format_distribution_complete(context, fragment_id, &properties.distribution),
                row_multiplicity(properties.row_multiplicity),
                joined(
                    &properties.ordering,
                    ", ",
                    |key: &novarocks_physical_plan::OrderingKey,
                     output: &mut fmt::Formatter<'_>| {
                        write!(
                            output,
                            "{} {} NULLS {}",
                            context.value_name(fragment_id, key.value),
                            sort_direction(key.direction),
                            null_ordering(key.null_ordering)
                        )
                    }
                )
            ));
        }
        lines.push(format_args!(
            "{pad}  PARTITION: {}",
            format_distribution(context, fragment_id, &node.output_properties.distribution)
        ));
        lines.push(format_args!(
            "{pad}  multiplicity={}, ordering=[{}]",
            row_multiplicity(node.output_properties.row_multiplicity),
            joined(
                &node.output_properties.ordering,
                ", ",
                |key: &novarocks_physical_plan::OrderingKey, output: &mut fmt::Formatter<'_>| {
                    write!(
                        output,
                        "{} {} NULLS {}",
                        context.value_name(fragment_id, key.value),
                        sort_direction(key.direction),
                        null_ordering(key.null_ordering)
                    )
                }
            )
        ));
    }
    match &node.kind {
        NodeKind::Scan {
            occurrence,
            relation,
            read_budget,
            provider_outputs,
            residuals,
            derived_values,
            ..
        } => {
            let relation_kind = match relation.as_ref() {
                novarocks_physical_plan::Relation::Data(_) => "data".to_string(),
                novarocks_physical_plan::Relation::Metadata(metadata) => {
                    format!("metadata({})", metadata.kind.as_str())
                }
            };
            lines.push(format_args!(
                "{pad}  occurrence=pr{}, relation={relation_kind}, work-source={}, schema-fields={}, read-budget={{rows={}, bytes={}}}",
                occurrence.get(),
                connector_read_work_source(relation.work_source()),
                relation.schema().len(),
                read_budget.max_batch_rows,
                read_budget.max_batch_bytes
            ));
            lines.push(format_args!(
                "{pad}  provider-read={}",
                format_provider_read(relation.read())
            ));
            match relation.as_ref() {
                novarocks_physical_plan::Relation::Data(data) => lines.push(format_args!(
                    "{pad}  selection-digest={}, artifact-inputs=[{}]",
                    format_hex(&data.selection_digest),
                    joined(&data.artifact_inputs, "; ", |requirement: &novarocks_physical_plan::ArtifactInputRequirement, output: &mut fmt::Formatter<'_>| format_artifact_requirement(requirement).fmt(output))
                )),
                novarocks_physical_plan::Relation::Metadata(metadata) => lines.push(format_args!(
                    "{pad}  selection-digest={}, coverage-evidence={{bytes={}, digest={}}}, artifact-inputs=[{}]",
                    format_hex(&metadata.selection_digest),
                    metadata.coverage_evidence.len(),
                    digest_bytes(&metadata.coverage_evidence),
                    joined(&metadata.artifact_inputs, "; ", |requirement: &novarocks_physical_plan::ArtifactInputRequirement, output: &mut fmt::Formatter<'_>| format_artifact_requirement(requirement).fmt(output))
                )),
            }
            for (ordinal, field) in relation.schema().iter().enumerate() {
                lines.push(format_args!(
                    "{pad}  relation-field[{ordinal}] column={} type={}",
                    format_connector_payload!(&field.column.column_payload),
                    format_value_type(&field.ty)
                ));
            }
            for (ordinal, (column, value)) in provider_outputs.iter().enumerate() {
                if let Some(ty) = fragment.values().get(value).map(|value| &value.ty) {
                    lines.push(format_args!(
                        "{pad}  provider-output[{ordinal}] column={} -> {} type={}",
                        format_connector_payload!(&column.column_payload),
                        context.value_name(fragment_id, *value),
                        format_value_type(ty)
                    ));
                } else {
                    lines.push(format_args!(
                        "{pad}  provider-output[{ordinal}] column={} -> {} type=unknown",
                        format_connector_payload!(&column.column_payload),
                        context.value_name(fragment_id, *value)
                    ));
                }
            }
            for guarantee in relation.predicate_guarantees() {
                lines.push(format_args!(
                    "{pad}  provider-guarantee expr=e{} kind={}",
                    guarantee.predicate.get(),
                    predicate_guarantee_kind(guarantee.kind)
                ));
            }
            for residual in residuals {
                lines.push(format_args!("{pad}  residual expr=e{}", residual.get()));
            }
            if !derived_values.is_empty() {
                lines.push(format_args!(
                    "{pad}  variant columns: {}",
                    joined(derived_values, ", ", |value: &ValueId, output: &mut fmt::Formatter<'_>| context.value_name(fragment_id, *value).fmt(output))
                ));
            }
            if let Some(materialized_view) = context.annotations.value(
                AnnotationSubject::Node(fragment_id, node.id),
                "sql.mv_rewritten_from",
            ) {
                lines.push(format_args!("{pad}  rewritten with mv: {materialized_view}"));
            }
            if let Some(provenance) = context.annotations.value(
                AnnotationSubject::Node(fragment_id, node.id),
                "sql.mv_rewrite_provenance",
            ) {
                lines.push(format_args!("{pad}  mv rewrite provenance: {provenance}"));
            }
            lines.push(format_args!(
                "{pad}  provider-outputs=[{}], guarantees=[{}], residuals=[{}], derived-values=[{}]",
                joined(provider_outputs, ", ", |(_, value): &(novarocks_physical_plan::ProviderColumnReference, ValueId), output: &mut fmt::Formatter<'_>| write!(output, "v{}", value.get())),
                joined(relation.predicate_guarantees(), ", ", |guarantee: &novarocks_physical_plan::PredicateGuarantee, output: &mut fmt::Formatter<'_>| write!(output, "e{}:{}", guarantee.predicate.get(), predicate_guarantee_kind(guarantee.kind))),
                format_expr_ids(residuals),
                format_value_ids(derived_values)
            ));
        }
        NodeKind::Filter { predicates } => {
            for (ordinal, predicate) in predicates.iter().enumerate() {
                lines.push(format_args!(
                    "{pad}  predicates[{ordinal}]: {}",
                    format_expr(plan, fragment_id, fragment, *predicate)
                ));
            }
        }
        NodeKind::Project { expressions } => lines.push(format_args!(
            "{pad}  expressions=[{}]",
            joined(expressions, ", ", |(expression, output): &(ExprId, ValueId), formatter: &mut fmt::Formatter<'_>| write!(
                    formatter,
                    "{} AS {}",
                    format_expr(plan, fragment_id, fragment, *expression),
                    context.value_name(fragment_id, *output)
                ))
        )),
        NodeKind::Aggregate { group_by, calls, .. } => {
            lines.push(format_args!(
                "{pad}  group-by=[{}]",
                joined(group_by, ", ", |(expression, output): &(ExprId, ValueId), formatter: &mut fmt::Formatter<'_>| write!(
                            formatter,
                            "{} AS {}",
                            format_expr(plan, fragment_id, fragment, *expression),
                            context.value_name(fragment_id, *output)
                        ))
            ));
            lines.push(format_args!(
                "{pad}  calls=[{}]",
                joined(calls, "; ", |call: &novarocks_physical_plan::AggregateCall, formatter: &mut fmt::Formatter<'_>| write!(
                        formatter,
                        "a{}:{}@{} phase={} args=[{}] distinct={} order-by=[{}] ->v{}",
                        call.id.get(),
                        call.binding.function.function_id.as_str(),
                        call.binding.function.overload.as_str(),
                        aggregate_phase(call.binding.phase),
                        joined(&call.arguments, ", ", |expression: &ExprId, output: &mut fmt::Formatter<'_>| format_expr(plan, fragment_id, fragment, *expression).fmt(output)),
                        call.distinct,
                        format_sort_exprs(plan, fragment_id, fragment, &call.order_by),
                        context.value_name(fragment_id, call.output)
                    ))
            ));
        }
        NodeKind::ExchangeSource { edge, imports } => lines.push(format_args!(
            "{pad}  edge={}, imports=[{}]",
            edge.get(),
            joined(imports, ", ", |(source, destination): &(ValueId, ValueId), formatter: &mut fmt::Formatter<'_>| write!(formatter, "v{}->v{}", source.get(), destination.get()))
        )),
        NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            distribution,
            residual,
            null_extended,
        } => {
            lines.push(format_args!("{pad}  join: {} JOIN", join_kind_label(*kind)));
            lines.push(format_args!(
                "{pad}  kind={}, build-side={}, distribution={}, keys=[{}], residual={}, null-extended=[{}]",
                join_kind_label(*kind),
                join_side(*build_side),
                join_distribution_label(*distribution),
                joined(keys, ", ", |key: &novarocks_physical_plan::JoinKey, formatter: &mut fmt::Formatter<'_>| write!(
                        formatter,
                        "{} {} {}",
                        format_expr(plan, fragment_id, fragment, key.left),
                        if key.null_safe { "<=>" } else { "=" },
                        format_expr(plan, fragment_id, fragment, key.right)
                    )),
                format_optional_readable_expr(plan, fragment_id, fragment, *residual),
                format_value_ids(null_extended)
            ));
        }
        NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            null_extended,
        } => {
            lines.push(format_args!("{pad}  join: {} JOIN", join_kind_label(*kind)));
            lines.push(format_args!(
                "{pad}  kind={}, distribution={}, on: {}, null-extended=[{}]",
                join_kind_label(*kind),
                nest_loop_distribution(*distribution),
                format_optional_readable_expr(plan, fragment_id, fragment, *predicate),
                format_value_ids(null_extended)
            ));
        }
        NodeKind::Sort { order_by, mode } => match mode {
                novarocks_physical_plan::SortMode::Global => lines.push(format_args!(
                    "{pad}  mode=global, order-by=[{}]",
                    format_sort_exprs(plan, fragment_id, fragment, order_by)
                )),
                novarocks_physical_plan::SortMode::Analytic { partition_by } => lines.push(format_args!(
                    "{pad}  mode=analytic partition-by=[{}], order-by=[{}]",
                    format_sort_exprs(plan, fragment_id, fragment, partition_by),
                    format_sort_exprs(plan, fragment_id, fragment, order_by)
                )),
                novarocks_physical_plan::SortMode::PartitionTopN {
                    partition_by,
                    limit,
                    kind,
                } => lines.push(format_args!(
                    "{pad}  mode=analytic, partition_limit={limit} topn_type={} partition-by=[{}], order-by=[{}]",
                    partition_topn_label(*kind),
                    format_sort_exprs(plan, fragment_id, fragment, partition_by),
                    format_sort_exprs(plan, fragment_id, fragment, order_by)
                )),
            },
        NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase,
        } => lines.push(format_args!(
            "{pad}  phase={}, limit={limit}, offset={offset}, order-by=[{}]",
            topn_phase(*phase),
            format_sort_exprs(plan, fragment_id, fragment, order_by)
        )),
        NodeKind::Limit { limit, offset } => match limit {
            Some(limit) => lines.push(format_args!("{pad}  limit={limit}, offset={offset}")),
            None => lines.push(format_args!("{pad}  limit=none, offset={offset}")),
        },
        NodeKind::Window(spec) => lines.push(format_args!(
            "{pad}  partition-by=[{}], order-by=[{}], expressions=[{}]",
            format_sort_exprs(plan, fragment_id, fragment, &spec.partition_by),
            format_sort_exprs(plan, fragment_id, fragment, &spec.order_by),
            joined(&spec.expressions, ", ", |expression: &WindowExpression, formatter: &mut fmt::Formatter<'_>| write!(
                    formatter,
                    "{}->{}",
                    format_expr(plan, fragment_id, fragment, expression.expression),
                    context.value_name(fragment_id, expression.output)
                ))
        )),
        NodeKind::SetOp {
            kind,
            input_mappings,
        } => lines.push(format_args!(
            "{pad}  kind={}, input-mappings=[{}]",
            set_operation_kind(*kind),
            SetOpMappingsDisplay(input_mappings)
        )),
        NodeKind::Values { rows } => lines.push(format_args!(
            "{pad}  rows=[{}]",
            ValuesRowsDisplay { plan, fragment_id, fragment, rows }
        )),
        NodeKind::Repeat {
            grouping_sets,
            grouping_values,
            grouping_outputs,
            ..
        } => lines.push(format_args!(
            "{pad}  grouping-sets=[{}], grouping-values=[{}], grouping-outputs=[{}]",
            GroupingSetsDisplay(grouping_sets),
            joined(grouping_values, ", ", |(input, output): &(ValueId, ValueId), formatter: &mut fmt::Formatter<'_>| write!(formatter, "v{}->v{}", input.get(), output.get())),
            joined(grouping_outputs, ", ", |output: &novarocks_physical_plan::GroupingOutput, formatter: &mut fmt::Formatter<'_>| write!(
                    formatter,
                    "[{}]->v{}",
                    format_value_ids(&output.arguments),
                    output.output.get()
                ))
        )),
        NodeKind::Unpivot { spec } => lines.push(format_args!(
            "{pad}  passthrough=[{}], value-output=v{}, literal-outputs=[{}], mappings=[{}], limits={{rows={}, bytes={}}}",
            joined(&spec.passthrough, ", ", |(input, output): &(ValueId, ValueId), formatter: &mut fmt::Formatter<'_>| write!(formatter, "v{}->v{}", input.get(), output.get())),
            spec.value_output.get(),
            format_value_ids(&spec.literal_outputs),
            joined(&spec.mappings, ", ", |mapping: &novarocks_physical_plan::UnpivotValueMapping, formatter: &mut fmt::Formatter<'_>| write!(
                    formatter,
                    "v{}:[{}]",
                    mapping.input.get(),
                    format_unpivot_constants(plan, fragment_id, fragment, &mapping.constants)
                )),
            spec.max_output_rows,
            spec.max_output_bytes
        )),
        NodeKind::GenerateSeries { start, stop, step } => lines.push(format_args!(
            "{pad}  start={}, stop={}, step={}",
            format_expr(plan, fragment_id, fragment, *start),
            format_expr(plan, fragment_id, fragment, *stop),
            format_optional_readable_expr(plan, fragment_id, fragment, *step)
        )),
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            left_outer,
        } => lines.push(format_args!(
            "{pad}  function={}@{}, arguments=[{}], outputs=[{}], left-outer={left_outer}",
            function.function_id.as_str(),
            function.overload.as_str(),
            joined(arguments, ", ", |argument: &ExprId, formatter: &mut fmt::Formatter<'_>| format_expr(plan, fragment_id, fragment, *argument).fmt(formatter)),
            format_table_function_outputs(outputs)
        )),
        NodeKind::AssertOneRow(assertion) => {
            lines.push(format_args!(
                "{pad}  assertion={}",
                format_row_count_assertion(assertion)
            ));
        }
        NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } => lines.push(format_args!(
            "{pad}  effect-output=v{}, events=[{}]",
            effect_output.get(),
            joined(events, "; ", |event: &novarocks_physical_plan::ChangeEventSpec, formatter: &mut fmt::Formatter<'_>| write!(
                    formatter,
                    "effect={} predicate={} assignments=[{}]",
                    connector_row_mutation_effect(event.effect),
                    format_optional_readable_expr(plan, fragment_id, fragment, event.predicate),
                    joined(&event.assignments, ", ", |(value, expression): &(ValueId, Option<ExprId>), output: &mut fmt::Formatter<'_>| write!(
                            output,
                            "v{}->{}",
                            value.get(),
                            format_optional_readable_expr(
                                plan,
                                fragment_id,
                                fragment,
                                *expression
                            )
                        ))
                ))
        )),
        NodeKind::TableWriter { target } => lines.push(format_args!(
            "{pad}  target={}, input=[{}], required-distribution={}, fields=[{}], output-schema={}, partial-aggregates=[{}]",
            target.write_target_ordinal.get(),
            format_value_ids(&target.input),
            format_distribution_complete(context, fragment_id, &target.required_distribution),
            joined(&target.target_fields, "; ", |field: &novarocks_physical_plan::WriterTargetField, formatter: &mut fmt::Formatter<'_>| write!(
                    formatter,
                    "token={} input=v{} type={} hidden={}",
                    format_hex(&field.token.to_bytes()),
                    field.input.get(),
                    format_value_type(&field.ty),
                    field.hidden
                )),
            format_writer_schema(&target.output_schema),
            format_writer_aggregates(&target.partial_aggregates)
        )),
        NodeKind::TableFinish(spec) => lines.push(format_args!(
            "{pad}  targets=[{}], input-schema={}, output-schema={}, final-aggregates=[{}], grouped-unpivot={}",
            joined(&spec.expected_target_ordinals, ", ", |ordinal: &WriteTargetOrdinal, formatter: &mut fmt::Formatter<'_>| ordinal.get().fmt(formatter)),
            format_writer_schema(&spec.input_schema),
            format_writer_schema(&spec.output_schema),
            format_writer_aggregates(&spec.final_aggregates),
            GroupedUnpivotDisplay { plan, fragment_id, fragment, value: spec.grouped_unpivot.as_ref() }
        )),
    }
}

struct ExprIdsDisplay<'a>(&'a [ExprId]);

impl fmt::Display for ExprIdsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        joined(
            self.0,
            ", ",
            |expression: &ExprId, output: &mut fmt::Formatter<'_>| {
                write!(output, "e{}", expression.get())
            },
        )
        .fmt(formatter)
    }
}

fn format_expr_ids(expressions: &[ExprId]) -> ExprIdsDisplay<'_> {
    ExprIdsDisplay(expressions)
}

struct SetOpMappingsDisplay<'a>(&'a [Box<[ValueId]>]);

impl fmt::Display for SetOpMappingsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, mapping) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "[{}]", format_value_ids(mapping))?;
        }
        Ok(())
    }
}

struct GroupingSetsDisplay<'a>(&'a [Box<[ValueId]>]);

impl fmt::Display for GroupingSetsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, set) in self.0.iter().enumerate() {
            if index != 0 {
                formatter.write_str(", ")?;
            }
            write!(formatter, "[{}]", format_value_ids(set))?;
        }
        Ok(())
    }
}

struct ValuesRowsDisplay<'a> {
    plan: &'a PhysicalPlan,
    fragment_id: FragmentId,
    fragment: &'a Fragment,
    rows: &'a [Box<[ExprId]>],
}

impl fmt::Display for ValuesRowsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (row_index, row) in self.rows.iter().enumerate() {
            if row_index != 0 {
                formatter.write_str(", ")?;
            }
            formatter.write_str("[")?;
            for (column_index, expression) in row.iter().enumerate() {
                if column_index != 0 {
                    formatter.write_str(", ")?;
                }
                format_expr(self.plan, self.fragment_id, self.fragment, *expression)
                    .fmt(formatter)?;
            }
            formatter.write_str("]")?;
        }
        Ok(())
    }
}

fn format_optional_readable_expr(
    _plan: &PhysicalPlan,
    _fragment_id: FragmentId,
    _fragment: &Fragment,
    expression: Option<ExprId>,
) -> String {
    expression.map_or_else(|| "none".to_string(), format_expr_id)
}

fn format_expr(
    _plan: &PhysicalPlan,
    _fragment_id: FragmentId,
    _fragment: &Fragment,
    expression: ExprId,
) -> String {
    format_expr_id(expression)
}

fn format_expr_id(expression: ExprId) -> String {
    format!("e{}", expression.get())
}

struct LiteralDisplay<'a>(&'a novarocks_physical_plan::LiteralValue);

impl fmt::Display for LiteralDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            novarocks_physical_plan::LiteralValue::Null => formatter.write_str("NULL"),
            novarocks_physical_plan::LiteralValue::Boolean(value) => value.fmt(formatter),
            novarocks_physical_plan::LiteralValue::Int64(value) => value.fmt(formatter),
            novarocks_physical_plan::LiteralValue::UInt64(value) => value.fmt(formatter),
            novarocks_physical_plan::LiteralValue::Float64Bits(value) => {
                write!(formatter, "{}[bits=0x{value:016x}]", f64::from_bits(*value))
            }
            novarocks_physical_plan::LiteralValue::LargeInt(value)
            | novarocks_physical_plan::LiteralValue::Decimal128(value)
            | novarocks_physical_plan::LiteralValue::IntervalMonthDayNano(value) => {
                value.fmt(formatter)
            }
            novarocks_physical_plan::LiteralValue::Decimal256(value) => {
                arrow::datatypes::i256::from_be_bytes(*value).fmt(formatter)
            }
            novarocks_physical_plan::LiteralValue::Utf8(value) => quote_text(value).fmt(formatter),
            novarocks_physical_plan::LiteralValue::Binary(value) => {
                write!(formatter, "X'{}'", format_hex(value))
            }
            novarocks_physical_plan::LiteralValue::Date32(value) => value.fmt(formatter),
            novarocks_physical_plan::LiteralValue::Time64(value)
            | novarocks_physical_plan::LiteralValue::Timestamp(value) => value.fmt(formatter),
        }
    }
}

fn format_literal(value: &novarocks_physical_plan::LiteralValue) -> LiteralDisplay<'_> {
    LiteralDisplay(value)
}

fn function_display_name(identity: &novarocks_physical_plan::FunctionId) -> &str {
    identity
        .as_str()
        .split('/')
        .rev()
        .nth(1)
        .unwrap_or_else(|| identity.as_str())
}

struct ValueIdsDisplay<'a>(&'a [ValueId]);

impl fmt::Display for ValueIdsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        joined(
            self.0,
            ", ",
            |value: &ValueId, output: &mut fmt::Formatter<'_>| write!(output, "v{}", value.get()),
        )
        .fmt(formatter)
    }
}

fn format_value_ids(values: &[ValueId]) -> ValueIdsDisplay<'_> {
    ValueIdsDisplay(values)
}

struct SortExprsDisplay<'a>(&'a [SortExpr]);

impl fmt::Display for SortExprsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        joined(
            self.0,
            ", ",
            |expression: &SortExpr, output: &mut fmt::Formatter<'_>| {
                write!(
                    output,
                    "e{} {} NULLS {}",
                    expression.expr.get(),
                    sort_direction(expression.direction),
                    null_ordering(expression.null_ordering)
                )
            },
        )
        .fmt(formatter)
    }
}

fn format_sort_exprs<'a>(
    _plan: &PhysicalPlan,
    _fragment_id: FragmentId,
    _fragment: &Fragment,
    expressions: &'a [SortExpr],
) -> SortExprsDisplay<'a> {
    SortExprsDisplay(expressions)
}

fn format_distribution<'a>(
    context: &'a RenderContext<'a>,
    fragment: FragmentId,
    distribution: &'a novarocks_physical_plan::Distribution,
) -> DistributionDisplay<'a> {
    format_distribution_complete(context, fragment, distribution)
}

fn join_distribution_label(
    distribution: novarocks_physical_plan::JoinDistribution,
) -> &'static str {
    match distribution {
        novarocks_physical_plan::JoinDistribution::Colocated => "COLOCATED",
        novarocks_physical_plan::JoinDistribution::Partitioned => "PARTITIONED",
        novarocks_physical_plan::JoinDistribution::BroadcastBuild => "BROADCAST",
        novarocks_physical_plan::JoinDistribution::Singleton => "SINGLETON",
    }
}

fn join_kind_label(kind: novarocks_physical_plan::JoinKind) -> &'static str {
    match kind {
        novarocks_physical_plan::JoinKind::Cross => "CROSS",
        novarocks_physical_plan::JoinKind::Inner => "INNER",
        novarocks_physical_plan::JoinKind::LeftOuter => "LEFT OUTER",
        novarocks_physical_plan::JoinKind::RightOuter => "RIGHT OUTER",
        novarocks_physical_plan::JoinKind::FullOuter => "FULL OUTER",
        novarocks_physical_plan::JoinKind::LeftSemi => "LEFT SEMI",
        novarocks_physical_plan::JoinKind::RightSemi => "RIGHT SEMI",
        novarocks_physical_plan::JoinKind::LeftAnti => "LEFT ANTI",
        novarocks_physical_plan::JoinKind::RightAnti => "RIGHT ANTI",
        novarocks_physical_plan::JoinKind::NullAwareLeftAnti => "NULL AWARE LEFT ANTI",
    }
}

fn partition_topn_label(kind: novarocks_physical_plan::PartitionTopNType) -> &'static str {
    match kind {
        novarocks_physical_plan::PartitionTopNType::RowNumber => "ROW_NUMBER",
        novarocks_physical_plan::PartitionTopNType::Rank => "RANK",
        novarocks_physical_plan::PartitionTopNType::DenseRank => "DENSE_RANK",
    }
}

fn aggregate_phase_label(calls: &[novarocks_physical_plan::AggregateCall]) -> &'static str {
    let Some(call) = calls.first() else {
        return "SINGLE";
    };
    match call.binding.phase {
        novarocks_physical_plan::AggregatePhase::Single => "SINGLE",
        novarocks_physical_plan::AggregatePhase::Partial { .. } => "LOCAL",
        novarocks_physical_plan::AggregatePhase::Intermediate { .. } => "INTERMEDIATE",
        novarocks_physical_plan::AggregatePhase::Final { .. } if call.distinct => "DISTINCT_GLOBAL",
        novarocks_physical_plan::AggregatePhase::Final { .. } => "GLOBAL",
    }
}

struct WriterAggregatesDisplay<'a>(&'a [novarocks_physical_plan::WriterAggregateCall]);

impl fmt::Display for WriterAggregatesDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        joined(
            self.0,
            "; ",
            |call: &novarocks_physical_plan::WriterAggregateCall,
             output: &mut fmt::Formatter<'_>| {
                write!(
                    output,
                    "{}@{} phase={} v{}->v{}",
                    call.binding.function.function_id.as_str(),
                    call.binding.function.overload.as_str(),
                    aggregate_phase(call.binding.phase),
                    call.input.get(),
                    call.output.get()
                )
            },
        )
        .fmt(formatter)
    }
}

fn format_writer_aggregates(
    calls: &[novarocks_physical_plan::WriterAggregateCall],
) -> WriterAggregatesDisplay<'_> {
    WriterAggregatesDisplay(calls)
}

struct WriterSchemaDisplay<'a>(&'a novarocks_physical_plan::WriterRelationSchema);

impl fmt::Display for WriterSchemaDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{{revision={}, fields=[{}]}}",
            self.0.revision,
            joined(
                &self.0.fields,
                "; ",
                |field: &novarocks_physical_plan::WriterRelationField,
                 output: &mut fmt::Formatter<'_>| write!(
                    output,
                    "v{}:{}:{}:{}",
                    field.value.get(),
                    quote_text(&field.name),
                    format_value_type(&field.ty),
                    writer_relation_field_role(field.role)
                )
            )
        )
    }
}

fn format_writer_schema(
    schema: &novarocks_physical_plan::WriterRelationSchema,
) -> WriterSchemaDisplay<'_> {
    WriterSchemaDisplay(schema)
}

struct UnpivotConstantsDisplay<'a> {
    plan: &'a PhysicalPlan,
    fragment_id: FragmentId,
    fragment: &'a Fragment,
    constants: &'a [novarocks_physical_plan::UnpivotConstant],
}

impl fmt::Display for UnpivotConstantsDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        joined(
            self.constants,
            ", ",
            |constant: &novarocks_physical_plan::UnpivotConstant,
             output: &mut fmt::Formatter<'_>| {
                match constant {
                    novarocks_physical_plan::UnpivotConstant::Scalar(expression) => {
                        format_expr(self.plan, self.fragment_id, self.fragment, *expression)
                            .fmt(output)
                    }
                    novarocks_physical_plan::UnpivotConstant::Int32List(values) => write!(
                        output,
                        "int32[{}]",
                        joined(
                            values,
                            ",",
                            |value: &i32, nested: &mut fmt::Formatter<'_>| value.fmt(nested)
                        )
                    ),
                    novarocks_physical_plan::UnpivotConstant::Utf8Map(entries) => write!(
                        output,
                        "utf8[{}]",
                        joined(
                            entries,
                            ",",
                            |entry: &(Box<str>, Box<str>), nested: &mut fmt::Formatter<'_>| write!(
                                nested,
                                "{}->{}",
                                quote_text(&entry.0),
                                quote_text(&entry.1)
                            )
                        )
                    ),
                }
            },
        )
        .fmt(formatter)
    }
}

fn format_unpivot_constants<'a>(
    plan: &'a PhysicalPlan,
    fragment_id: FragmentId,
    fragment: &'a Fragment,
    constants: &'a [novarocks_physical_plan::UnpivotConstant],
) -> UnpivotConstantsDisplay<'a> {
    UnpivotConstantsDisplay {
        plan,
        fragment_id,
        fragment,
        constants,
    }
}

struct GroupedUnpivotDisplay<'a> {
    plan: &'a PhysicalPlan,
    fragment_id: FragmentId,
    fragment: &'a Fragment,
    value: Option<&'a WriterGroupedUnpivotSpec>,
}

impl fmt::Display for GroupedUnpivotDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(unpivot) = self.value else {
            return formatter.write_str("none");
        };
        write!(
            formatter,
            "targets=[{}] group=v{}->v{} passthrough=v{} value=v{} literal-outputs=[{}] mappings={} limits={{rows={}, bytes={}}}",
            joined(
                &unpivot.statistics_target_ordinals,
                ", ",
                |ordinal: &WriteTargetOrdinal, output: &mut fmt::Formatter<'_>| ordinal
                    .get()
                    .fmt(output)
            ),
            unpivot.grouping_input.get(),
            unpivot.grouping_output.get(),
            unpivot.passthrough_output.get(),
            unpivot.value_output.get(),
            format_value_ids(&unpivot.literal_outputs),
            joined(
                &unpivot.mappings,
                "; ",
                |mapping: &novarocks_physical_plan::WriterGroupedUnpivotMapping,
                 output: &mut fmt::Formatter<'_>| write!(
                    output,
                    "target={} v{}:[{}]",
                    mapping.write_target_ordinal.get(),
                    mapping.input.get(),
                    format_unpivot_constants(
                        self.plan,
                        self.fragment_id,
                        self.fragment,
                        &mapping.constants
                    )
                )
            ),
            unpivot.max_output_rows,
            unpivot.max_output_bytes
        )
    }
}

fn format_hex(bytes: &[u8]) -> Hex<'_> {
    Hex(bytes)
}

fn render_annotations(
    context: &RenderContext<'_>,
    subject: AnnotationSubject,
    pad: &str,
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    for annotation in context
        .annotations
        .get(subject)
        .iter()
        .filter(|annotation| {
            !annotation_is_internal_display(&annotation.key)
                && annotation_visible(context.level, &annotation.key)
        })
    {
        if matches!(subject, AnnotationSubject::Node(_, _))
            && matches!(
                annotation.key.as_ref(),
                "optimizer.statistics" | "optimizer.broadcast"
            )
        {
            continue;
        }
        lines.push(format_args!("{pad}{}={}", annotation.key, annotation.value))?;
    }
    Ok(())
}

fn render_display_annotations(
    annotations: &[SqlDisplayAnnotation],
    lines: &mut ExplainRenderOutput,
) -> Result<(), SqlCompileError> {
    if annotations.is_empty() {
        return Ok(());
    }
    lines.push(format_args!("DISPLAY ANNOTATIONS"))?;
    for annotation in annotations {
        lines.push(format_args!(
            "  {}={}",
            annotation.key(),
            annotation.value()
        ))?;
    }
    Ok(())
}

const fn is_detailed(level: ExplainLevel) -> bool {
    matches!(
        level,
        ExplainLevel::Verbose
            | ExplainLevel::Costs
            | ExplainLevel::Analyze
            | ExplainLevel::Contract
    )
}

fn annotation_visible(level: ExplainLevel, key: &str) -> bool {
    match level {
        ExplainLevel::Normal => false,
        ExplainLevel::Verbose => {
            matches!(key, "optimizer.statistics" | "optimizer.broadcast")
        }
        ExplainLevel::Costs | ExplainLevel::Analyze | ExplainLevel::Contract => true,
    }
}

fn annotation_is_internal_display(key: &str) -> bool {
    matches!(
        key,
        "sql.display_name" | "sql.relation" | "sql.mv_rewritten_from"
    )
}

struct NodeHeaderDisplay<'a> {
    context: &'a RenderContext<'a>,
    fragment_id: FragmentId,
    node: &'a PhysicalNode,
}

impl fmt::Display for NodeHeaderDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let context = self.context;
        let node = self.node;
        match &node.kind {
            NodeKind::Scan { .. } => {
                formatter.write_str("SCAN")?;
                if let Some(relation) = context.annotations.value(
                    AnnotationSubject::Node(self.fragment_id, node.id),
                    "sql.relation",
                ) {
                    write!(formatter, " {relation}")?;
                }
            }
            NodeKind::Filter { .. } => formatter.write_str("FILTER")?,
            NodeKind::Project { .. } => formatter.write_str("PROJECT")?,
            NodeKind::Aggregate { calls, .. } => write!(
                formatter,
                "HASH AGGREGATE ({})",
                aggregate_phase_label(calls)
            )?,
            NodeKind::HashJoin {
                kind, distribution, ..
            } => write!(
                formatter,
                "HASH JOIN ({}, {})",
                join_distribution_label(*distribution),
                join_kind_label(*kind)
            )?,
            NodeKind::NestLoopJoin { kind, .. } => {
                write!(formatter, "NEST LOOP JOIN ({})", join_kind_label(*kind))?
            }
            NodeKind::Sort { .. } => formatter.write_str("SORT")?,
            NodeKind::TopN { .. } => formatter.write_str("TOP-N")?,
            NodeKind::Limit { .. } => formatter.write_str("LIMIT")?,
            NodeKind::Window(_) => formatter.write_str("WINDOW")?,
            NodeKind::SetOp { kind, .. } => formatter.write_str(match kind {
                novarocks_physical_plan::SetOperationKind::UnionAll => "UNION ALL",
                novarocks_physical_plan::SetOperationKind::Intersect => "INTERSECT",
                novarocks_physical_plan::SetOperationKind::Except => "EXCEPT",
            })?,
            NodeKind::Values { .. } => formatter.write_str("VALUES")?,
            NodeKind::Repeat { .. } => formatter.write_str("REPEAT")?,
            NodeKind::Unpivot { .. } => formatter.write_str("UNPIVOT")?,
            NodeKind::GenerateSeries { .. } => formatter.write_str("GENERATE SERIES")?,
            NodeKind::TableFunction { function, .. } => write!(
                formatter,
                "TABLE FUNCTION {}",
                function_display_name(&function.function_id)
            )?,
            NodeKind::AssertOneRow(_) => formatter.write_str("ASSERT ONE ROW")?,
            NodeKind::ChangeEventExpand { .. } => formatter.write_str("CHANGE EVENT EXPAND")?,
            NodeKind::ExchangeSource { edge, .. } => {
                let label = context.plan.edges().get(edge).map_or("EXCHANGE", |edge| {
                    if matches!(
                        edge.partitioning.destination,
                        novarocks_physical_plan::Distribution::Hash { .. }
                            | novarocks_physical_plan::Distribution::BucketShuffle { .. }
                    ) {
                        "HASH EXCHANGE"
                    } else {
                        "EXCHANGE"
                    }
                });
                write!(formatter, "{label} (EXCHANGE ID: {})", edge.get())?;
            }
            NodeKind::TableWriter { .. } => formatter.write_str("TABLE WRITER")?,
            NodeKind::TableFinish(_) => formatter.write_str("TABLE FINISH")?,
        }
        if is_detailed(context.level) {
            let subject = AnnotationSubject::Node(self.fragment_id, node.id);
            if let Some(verdict) = context
                .annotations
                .value(subject, "optimizer.broadcast")
                .and_then(|value| value.split(',').next())
            {
                write!(formatter, " bcast_{verdict}")?;
            }
            if let Some(statistics) = context.annotations.value(subject, "optimizer.statistics") {
                write!(formatter, " stats={{{statistics}}}")?;
            }
        }
        Ok(())
    }
}

fn node_header<'a>(
    context: &'a RenderContext<'a>,
    fragment_id: FragmentId,
    node: &'a PhysicalNode,
) -> NodeHeaderDisplay<'a> {
    NodeHeaderDisplay {
        context,
        fragment_id,
        node,
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::{
        ExactInputVersion, PipelineDopDomain, PlanVersionId, PredicateGuaranteeKind,
        ProviderColumnReference, ProviderReadReference, ScanReadBudget,
    };
    use novarocks_spi::connector::read_stack::{ConnectorReadBinding, ConnectorReadWorkSource};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
    };

    use super::*;
    use crate::binding::SqlTableBindingId;
    use crate::catalog::ResolvedAnalyzerTable;
    use crate::compiler::{
        CatalogRelationFact, CatalogRelationNeed, DEFAULT_COMPLETION_LIMITS,
        ProviderReadColumnFact, ProviderReadFact, ProviderReadLimitFact, ProviderReadPredicateFact,
        ProviderReadProperties, ProviderReadRequestBinding, SessionOptimizerSettings,
        SqlCompilation, SqlCompileControl, SqlCompileIntent, SqlCompileProgress, SqlCompiler,
        SqlFactBatch, SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput, StatisticsFact, builtin_sql_function_catalog, noop_constant_evaluator,
    };
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use crate::planning::dml::DmlStatisticsEvidence;

    fn request(
        sql: &str,
        intent: SqlCompileIntent,
        version_byte: u8,
    ) -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([version_byte; 16]).expect("plan version"),
            SqlStatementInput::sql(sql),
            intent,
            SqlSessionContext {
                sql_semantics: crate::sql_mode::SqlSemanticSettings::default(),
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings {
                    enable_materialized_view_rewrite: Some(false),
                    ..SessionOptimizerSettings::default()
                },
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            SqlCompileControl::unbounded(),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: 1024,
                max_batch_bytes: 1024 * 1024,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }

    fn completed(intent: SqlCompileIntent, version_byte: u8) -> SqlCompletedPlan {
        completed_sql("select 1", intent, version_byte)
    }

    fn completed_sql(sql: &str, intent: SqlCompileIntent, version_byte: u8) -> SqlCompletedPlan {
        let request = request(sql, intent, version_byte)
            .try_into_completion()
            .expect("completion request");
        let SqlCompileProgress::Complete(completed) =
            SqlCompiler::start(request).expect("completed values plan")
        else {
            panic!("VALUES must not require external facts");
        };
        completed
    }

    /// Shapes that generated SQL actually produces at scale.
    ///
    /// Every structural bound that can refuse a plan needs the symmetric
    /// evidence: not only that it rejects what it should, but that no
    /// legitimate query trips it. A dashboard filter panel, an ORM `IN`
    /// expansion and a wide `UNION ALL` are ordinary queries, and a bound that
    /// refuses one is a user-visible loss of function rather than a slow path.
    fn generated_sql_corpus() -> Vec<(&'static str, String)> {
        let conjuncts = (0..512)
            .map(|i| format!("c0 <> {i}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let disjuncts = (0..512)
            .map(|i| format!("(c0 = {i} AND c1 = {i})"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let in_list = (0..4096)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let union_all = (0..256)
            .map(|i| format!("SELECT {i} AS c0"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let wide_projection = (0..1024)
            .map(|i| format!("c0 + {i} AS p{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut nested = "SELECT c0 FROM (VALUES (1)) AS t(c0)".to_string();
        for _ in 0..64 {
            nested = format!("SELECT c0 FROM ({nested}) AS s");
        }
        vec![
            (
                "wide filter panel",
                format!("SELECT c0 FROM (VALUES (1, 2)) AS t(c0, c1) WHERE {conjuncts}"),
            ),
            (
                "generated disjunction",
                format!("SELECT c0 FROM (VALUES (1, 2)) AS t(c0, c1) WHERE {disjuncts}"),
            ),
            (
                "orm in expansion",
                format!("SELECT c0 FROM (VALUES (1, 2)) AS t(c0, c1) WHERE c0 IN ({in_list})"),
            ),
            ("wide union all", union_all),
            (
                "wide projection",
                format!("SELECT {wide_projection} FROM (VALUES (1, 2)) AS t(c0, c1)"),
            ),
            ("deeply nested subqueries", nested),
        ]
    }

    /// A chain wide enough to exhaust the stack is refused, not aborted on.
    ///
    /// The engine cannot report a stack overflow: it aborts the process rather
    /// than unwinding. So the width that would get there has to be an error a
    /// client can see, raised before the walks that would consume the stack.
    #[test]
    fn an_unboundedly_wide_boolean_chain_is_refused_rather_than_aborted_on() {
        // Parsing and analysis now keep this chain shallow enough to reject it
        // on a bounded planning stack.
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(check_wide_boolean_chain_is_refused)
            .expect("spawn")
            .join()
            .expect("refusal thread");
    }

    fn check_wide_boolean_chain_is_refused() {
        let conjuncts = (0..crate::analyzer::MAX_BOOLEAN_CHAIN_OPERANDS + 1)
            .map(|i| format!("c0 <> {i}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("SELECT c0 FROM (VALUES (1, 2)) AS t(c0, c1) WHERE {conjuncts}");
        let error = match request(&sql, SqlCompileIntent::Query, 7).try_into_completion() {
            Ok(_) => panic!("a chain this wide must be refused"),
            Err(error) => error,
        };
        assert!(
            format!("{error:?}").contains("exceeds the supported maximum"),
            "{error:?}"
        );
    }

    #[test]
    fn generated_sql_shapes_are_not_refused_by_a_structural_bound() {
        // Match a bounded production planning stack instead of masking deep
        // recursive walks with the former 64 MiB test-only stack.
        std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(check_generated_sql_shapes)
            .expect("spawn")
            .join()
            .expect("corpus thread");
    }

    fn check_generated_sql_shapes() {
        for (name, sql) in generated_sql_corpus() {
            let plan = completed_sql(&sql, SqlCompileIntent::Query, 7);
            let physical = plan.plan();
            // Record the headroom, so a future bound change shows which shape
            // is closest to it rather than only that something broke.
            let nodes = physical
                .fragments()
                .values()
                .map(|fragment| fragment.nodes().len())
                .max()
                .unwrap_or(0);
            let expressions = physical
                .fragments()
                .values()
                .map(|fragment| fragment.expressions().len())
                .max()
                .unwrap_or(0);
            let values = physical
                .fragments()
                .values()
                .map(|fragment| fragment.values().len())
                .max()
                .unwrap_or(0);
            println!(
                "{name}: fragments={}, max_nodes={nodes}, max_values={values}, max_expressions={expressions}",
                physical.fragments().len(),
            );
            assert!(
                nodes <= novarocks_physical_plan::PlanLimits::FROZEN.fragment_nodes,
                "{name}: {nodes} nodes"
            );
            assert!(
                values <= novarocks_physical_plan::PlanLimits::FROZEN.fragment_values,
                "{name}: {values} values"
            );
            assert!(
                expressions <= novarocks_physical_plan::PlanLimits::FROZEN.fragment_expressions,
                "{name}: {expressions} expressions"
            );
        }
    }

    fn incomplete(progress: SqlCompileProgress) -> SqlCompilation {
        match progress {
            SqlCompileProgress::Incomplete(compilation) => compilation,
            SqlCompileProgress::Complete(_) => panic!("expected an external fact need"),
        }
    }

    fn resolved_table(need: &CatalogRelationNeed) -> ResolvedAnalyzerTable {
        let relation = need.relation();
        ResolvedAnalyzerTable::from_planner(
            Some(&relation.catalog),
            &relation.namespace,
            TableDef {
                name: relation.table.clone(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "order_key".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    SqlTableBindingId::new_for_test(41),
                    SqlTableIdentity::try_new(
                        relation.catalog.clone(),
                        relation.namespace.clone(),
                        relation.table.clone(),
                    )
                    .expect("table identity"),
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            },
        )
    }

    fn connector_binding() -> ConnectorReadBinding {
        let provider_id = ConnectorProviderId::parse("iceberg").expect("provider id");
        let instance_id = ConnectorInstanceId::parse("lakehouse").expect("instance id");
        ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        )
    }

    fn encoded(
        binding: &ConnectorReadBinding,
        category: ConnectorCodecCategory,
    ) -> ConnectorEncodedPayload {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).expect("codec revision"),
            ),
            vec![category as u8 + 1].into(),
        )
    }

    fn provider_contract(
        need: &crate::compiler::ProviderReadNeed,
        guarantee: PredicateGuaranteeKind,
    ) -> crate::compiler::ProviderReadStaticContract {
        let binding = connector_binding();
        crate::compiler::ProviderReadStaticContract {
            sql_binding: need.binding(),
            request: ProviderReadRequestBinding::from_need(need),
            read: ProviderReadReference {
                binding: binding.clone(),
                input_version: ExactInputVersion::try_new([9]).expect("input version"),
                relation: ConnectorReadRelationPayload::new(
                    need.relation().relation_kind(),
                    encoded(&binding, ConnectorCodecCategory::ReadTable),
                    encoded(&binding, ConnectorCodecCategory::ReadView),
                ),
            },
            work_source: ConnectorReadWorkSource::RuntimeSplits,
            selection_digest: [8; 32],
            schema: need
                .columns()
                .iter()
                .map(|column| {
                    ProviderReadColumnFact::new(
                        column.ordinal(),
                        ProviderColumnReference {
                            column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn),
                        },
                        column.engine_type().clone(),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            predicates: need
                .predicates()
                .iter()
                .map(|predicate| ProviderReadPredicateFact::new(predicate.occurrence(), guarantee))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            limit: need.limit().map_or(
                ProviderReadLimitFact::NotRequested,
                ProviderReadLimitFact::Exact,
            ),
            provided_properties: ProviderReadProperties::unconstrained(),
            artifact_inputs: Box::default(),
            artifact_refs: Box::default(),
            coverage_evidence: Box::default(),
        }
    }

    fn completed_external_sql(
        sql: &str,
        version_byte: u8,
        guarantee: PredicateGuaranteeKind,
    ) -> SqlCompletedPlan {
        let seed = request(
            sql,
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            version_byte,
        )
        .try_into_completion()
        .expect("completion request");
        let catalog = incomplete(SqlCompiler::start(seed).expect("catalog need"));
        let catalog_needs = match catalog.needs() {
            crate::compiler::SqlNeedBatch::CatalogRelations(needs) => needs,
            other => panic!("expected catalog needs, got {other:?}"),
        };
        let catalog_facts = catalog_needs
            .iter()
            .map(|need| {
                CatalogRelationFact::resolved(need, resolved_table(need))
                    .expect("catalog relation fact")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let statistics = incomplete(
            SqlCompiler::finish(
                catalog,
                SqlFactBatch::CatalogRelations(catalog_facts),
                &SqlCompileControl::unbounded(),
            )
            .expect("catalog round"),
        );
        let statistics_needs = match statistics.needs() {
            crate::compiler::SqlNeedBatch::Statistics(needs) => needs,
            other => panic!("expected statistics needs, got {other:?}"),
        };
        let statistics_facts = statistics_needs
            .iter()
            .map(|need| {
                StatisticsFact::try_new(
                    need,
                    need.metrics().to_vec(),
                    DmlStatisticsEvidence::Missing {
                        binding: need.binding(),
                        label: "iceberg.db.orders".to_string(),
                        reason: "test fixture has no statistics".to_string(),
                    },
                )
                .expect("statistics fact")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let provider = incomplete(
            SqlCompiler::finish(
                statistics,
                SqlFactBatch::Statistics(statistics_facts),
                &SqlCompileControl::unbounded(),
            )
            .expect("statistics round"),
        );
        let provider_needs = match provider.needs() {
            crate::compiler::SqlNeedBatch::ProviderReads(needs) => needs,
            other => panic!("expected provider needs, got {other:?}"),
        };
        let provider_facts = provider_needs
            .iter()
            .map(|need| {
                ProviderReadFact::negotiated(need, provider_contract(need, guarantee))
                    .expect("provider read fact")
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let SqlCompileProgress::Complete(completed) = SqlCompiler::finish(
            provider,
            SqlFactBatch::ProviderReads(provider_facts),
            &SqlCompileControl::unbounded(),
        )
        .expect("provider round") else {
            panic!("provider facts must complete the plan");
        };
        completed
    }

    fn exact_profile(
        completed: &SqlCompletedPlan,
        operator_metrics: SqlExplainObservation<SqlExplainOperatorMetrics>,
        fragment_metrics: SqlExplainObservation<SqlExplainFragmentMetrics>,
    ) -> SqlCompletedExplainProfile {
        let operators = completed
            .plan()
            .fragments()
            .iter()
            .flat_map(|(fragment_id, fragment)| {
                fragment.nodes().keys().map(|node_id| {
                    (
                        SqlExplainNodeKey::new(*fragment_id, *node_id),
                        operator_metrics,
                    )
                })
            })
            .collect();
        let fragments = completed
            .plan()
            .fragments()
            .keys()
            .map(|fragment_id| (*fragment_id, fragment_metrics))
            .collect();
        SqlCompletedExplainProfile::try_new(completed.plan().version(), operators, fragments)
            .expect("exact profile")
    }

    #[test]
    fn relation_explain_publishes_completion_needs_before_rendering() {
        let request = request(
            "select order_key from orders",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Normal,
                analyze: false,
            },
            30,
        )
        .try_into_completion()
        .expect("completion request");

        assert!(matches!(
            SqlCompiler::start(request).expect("catalog need"),
            SqlCompileProgress::Incomplete(_)
        ));
    }

    #[test]
    fn ordinary_explain_renders_only_after_completion() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            31,
        );

        let rendered = completed
            .render_explain_lines()
            .expect("complete EXPLAIN")
            .join("\n");

        assert!(rendered.contains("RESULT PORT"), "{rendered}");
        assert!(rendered.contains("PHYSICAL PLAN version="), "{rendered}");
        assert!(rendered.contains("contract-revision="), "{rendered}");
        assert!(rendered.contains("type=Int64, NOT NULL"), "{rendered}");
        assert!(rendered.contains("VALUE DEFINITIONS"), "{rendered}");
        assert!(rendered.contains("origin="), "{rendered}");
        assert!(rendered.contains("required-input["), "{rendered}");
        assert!(rendered.contains("multiplicity="), "{rendered}");
        assert!(rendered.contains("ordering=["), "{rendered}");
        assert!(rendered.contains("VALUES"), "{rendered}");
        assert!(rendered.contains("stats={rows="), "{rendered}");
    }

    #[test]
    fn completed_display_annotations_are_rendered_explicitly() {
        let annotation = SqlDisplayAnnotation::try_new("optimizer", "stable").expect("annotation");
        let mut lines = ExplainRenderOutput::new(ExplainRenderBudget::default());

        render_display_annotations(&[annotation], &mut lines).expect("display annotations");
        let rendered = lines.finish().join("\n");

        assert!(rendered.contains("DISPLAY ANNOTATIONS"), "{rendered}");
        assert!(rendered.contains("optimizer=stable"), "{rendered}");
    }

    #[test]
    fn completed_renderer_uses_final_node_contract_semantics() {
        let completed = completed_sql(
            "select sum(x) as total from (values (1), (2)) t(x) where x > 0 order by total limit 1",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            37,
        );

        let rendered = completed
            .render_explain_lines()
            .expect("completed explain")
            .join("\n");

        assert!(rendered.contains("EXPRESSION DEFINITIONS"), "{rendered}");
        assert!(rendered.contains("predicates[0]: e"), "{rendered}");
        assert!(rendered.contains("HASH AGGREGATE"), "{rendered}");
        assert!(rendered.contains("builtin.aggregate/sum/v1"), "{rendered}");
        assert!(rendered.contains("order-by=[e"), "{rendered}");
        assert!(rendered.contains("limit=1"), "{rendered}");
    }

    #[test]
    fn completed_renderer_exposes_final_runtime_filter_bindings() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            38,
        );
        let (&fragment_id, fragment) = completed
            .plan()
            .fragments()
            .first_key_value()
            .expect("fragment");
        let value = fragment.nodes()[&fragment.root()].output.columns[0];
        let witness = novarocks_physical_plan::RuntimeFilterWitnessId::new(1);
        let coverage = novarocks_physical_plan::RuntimeFilterCoverage {
            nodes: Box::from(
                [novarocks_physical_plan::RuntimeFilterCoverageNode::Witness(
                    witness,
                )],
            ),
            root: 0,
        };
        let endpoint = novarocks_physical_plan::RuntimeFilterEndpoint {
            fragment: fragment_id,
            node: fragment.root(),
            values: Box::from([value]),
        };
        let filter = novarocks_physical_plan::RuntimeFilter {
            id: novarocks_physical_plan::RuntimeFilterId::new(7),
            kind: novarocks_physical_plan::RuntimeFilterKind::MinMax,
            domain: novarocks_physical_plan::RuntimeFilterDomain::Ordered {
                key: novarocks_physical_plan::RuntimeFilterOrderKey {
                    ty: novarocks_physical_plan::ValueType::new(DataType::Int64, false),
                    direction: novarocks_physical_plan::SortDirection::Ascending,
                    null_ordering: novarocks_physical_plan::NullOrdering::Last,
                },
                inclusive: true,
                comparator:
                    novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
            },
            lifecycle: novarocks_physical_plan::RuntimeFilterLifecycle::MonotonicUpdates,
            reduction: novarocks_physical_plan::RuntimeFilterReduction::TightenOrderedBound,
            availability_coverage: coverage.clone(),
            terminal_coverage: coverage,
            equality_witnesses: Box::default(),
            producers: Box::from([novarocks_physical_plan::RuntimeFilterProducer {
                witness,
                endpoint: endpoint.clone(),
                apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::NodeOutput,
                contribution_kinds: Box::from([
                    novarocks_physical_plan::RuntimeFilterContributionKind::OrderedBoundUpdate,
                    novarocks_physical_plan::RuntimeFilterContributionKind::ProducerClosed,
                ]),
                completion: novarocks_physical_plan::RuntimeFilterCompletion::ProducerClosed,
                progress: novarocks_physical_plan::RuntimeFilterProducerProgress {
                    build_edges: Box::default(),
                    non_build_edges: Box::default(),
                },
                target: novarocks_physical_plan::RuntimeFilterProducerTarget::AggregateTopNKey {
                    group_key_ordinal: 0,
                    topn: fragment.root(),
                    phase: novarocks_physical_plan::TopNPhase::Single,
                    order_key_ordinal: 0,
                    limit: 2,
                    offset: 0,
                    direction: novarocks_physical_plan::SortDirection::Ascending,
                    null_ordering: novarocks_physical_plan::NullOrdering::Last,
                },
            }]),
            consumers: Box::from([novarocks_physical_plan::RuntimeFilterConsumer {
                endpoint,
                apply_point: novarocks_physical_plan::RuntimeFilterApplyPoint::ScanSource,
                capabilities: Box::from([
                    novarocks_physical_plan::RuntimeFilterArtifactCapability::OrderedRange,
                ]),
                activation:
                    novarocks_physical_plan::RuntimeFilterConsumerActivation::NonBlockingLive {
                        late_apply: novarocks_physical_plan::LateApplyGranularity::Batch,
                    },
                target:
                    novarocks_physical_plan::RuntimeFilterConsumerTarget::AggregateTopNScanField {
                        producer: witness,
                        lineage: Box::default(),
                    },
            }]),
            policy: novarocks_physical_plan::RuntimeFilterPolicy {
                max_contribution_bytes: 1024,
                max_artifact_bytes: 4096,
                deadline_ms: 100,
                max_retries: 1,
            },
        };
        let context = RenderContext::new(completed.plan(), ExplainLevel::Verbose, None)
            .expect("render context");
        let mut lines = ExplainRenderOutput::new(ExplainRenderBudget::default());

        render_runtime_filter(&context, &filter, &mut lines).expect("runtime filter");
        let rendered = lines.finish().join("\n");

        assert!(
            rendered.contains("domain = OrderedBound(key=Int64 ASC NULLS LAST, inclusive=true, comparator=novarocks.native-scalar-order.v1)"),
            "{rendered}"
        );
        assert!(rendered.contains("producer binding"), "{rendered}");
        assert!(rendered.contains("consumer binding"), "{rendered}");
        assert!(
            rendered.contains("target = AggregateTopNKey(group_key_ordinal=0, topn=n"),
            "{rendered}"
        );
        assert!(
            rendered.contains("activation = NonBlockingLive(batch)"),
            "{rendered}"
        );
    }

    #[test]
    fn completed_renderer_preserves_hash_join_shape_tokens() {
        let completed = completed_sql(
            "select l.k from (values (1), (2)) l(k) join (values (1), (3)) r(k) on l.k = r.k",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            39,
        );

        let rendered = completed
            .render_explain_lines()
            .expect("completed explain")
            .join("\n");

        assert!(rendered.contains("HASH JOIN ("), "{rendered}");
        assert!(rendered.contains("kind=INNER"), "{rendered}");
        assert!(rendered.contains("keys=["), "{rendered}");
        assert!(rendered.contains(" = "), "{rendered}");
        assert!(rendered.contains("stats={rows="), "{rendered}");

        assert!(!completed.plan().edges().is_empty(), "{rendered}");
        assert!(rendered.contains("EDGE GRAPH"), "{rendered}");
        assert!(rendered.contains("source={fragment=f"), "{rendered}");
        assert!(rendered.contains("mapping=["), "{rendered}");
        assert!(rendered.contains("CUT ATTACHMENTS"), "{rendered}");
        assert!(
            rendered.contains("source-bindings=[] source-free=true"),
            "{rendered}"
        );
        assert!(rendered.contains("change-stream-writer=none"), "{rendered}");
        assert!(rendered.contains("writer-result=none"), "{rendered}");
        assert!(rendered.contains("SINK stream edge=edge"), "{rendered}");
        assert!(!completed.plan().runtime_filters().is_empty(), "{rendered}");
        assert!(rendered.contains("RUNTIME FILTER GRAPH"), "{rendered}");
        assert!(rendered.contains("availability-coverage="), "{rendered}");
        assert!(rendered.contains("producer binding witness="), "{rendered}");
        assert!(
            rendered.contains("consumer binding fragment="),
            "{rendered}"
        );
    }

    #[test]
    fn completed_renderer_defines_expression_dag_once_with_explicit_grouping() {
        let completed = completed_sql(
            "select x * (y + z) as answer from (values (1, 2, 3), (4, 5, 6)) t(x, y, z)",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            40,
        );
        let rendered = completed
            .render_explain_lines()
            .expect("completed explain")
            .join("\n");
        let expression_count = completed
            .plan()
            .fragments()
            .values()
            .map(|fragment| fragment.expressions().len())
            .sum::<usize>();
        let definition_count = rendered
            .lines()
            .filter(|line| {
                let line = line.trim_start();
                line.starts_with('e') && line.contains(" = ") && line.contains(" : ")
            })
            .count();
        assert_eq!(definition_count, expression_count, "{rendered}");

        let multiply = completed
            .plan()
            .fragments()
            .values()
            .find_map(|fragment| {
                fragment.expressions().iter().find_map(|(id, expression)| {
                    let novarocks_physical_plan::ExprKind::Binary {
                        left,
                        op: novarocks_physical_plan::BinaryOperator::Multiply,
                        right,
                    } = expression.kind
                    else {
                        return None;
                    };
                    Some((*id, left, right))
                })
            })
            .expect("multiply expression");
        assert!(
            rendered.contains(&format!(
                "e{} = (e{} * e{})",
                multiply.0.get(),
                multiply.1.get(),
                multiply.2.get()
            )),
            "{rendered}"
        );
    }

    #[test]
    fn result_aliases_are_bound_only_to_result_port_occurrences() {
        let completed = completed_sql(
            "select x as left_alias, x as right_alias from (values (1), (2)) t(x)",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            41,
        );
        let rendered = completed
            .render_explain_lines()
            .expect("completed explain")
            .join("\n");

        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("alias=\"left_alias\""))
                .count(),
            1,
            "{rendered}"
        );
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("alias=\"right_alias\""))
                .count(),
            1,
            "{rendered}"
        );
        // An alias names a result field, not the value behind it: one value
        // read twice under two aliases is still one value with one name.
        assert!(!rendered.contains("{left_alias}"), "{rendered}");
        assert!(!rendered.contains("{right_alias}"), "{rendered}");
        assert!(rendered.contains("value=v"), "{rendered}");
        assert!(rendered.contains("EXPRESSION DEFINITIONS"), "{rendered}");
    }

    #[test]
    fn result_alias_does_not_hide_internal_value_rendering_with_the_same_name() {
        let completed = completed_sql(
            "select y as x from (values (1, 2), (3, 4)) t(x, y) where x > 0",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            45,
        );
        // The value the result field aliases `x`: the alias belongs to the
        // field, and the value keeps whatever the statement calls it.
        let aliased = completed
            .plan()
            .result_port()
            .and_then(|port| port.fields.first())
            .map(|field| field.value)
            .expect("a result field");
        let rendered = completed
            .render_explain_lines()
            .expect("completed explain")
            .join("\n");

        assert!(
            rendered.contains(&format!("v{}{{", aliased.get())),
            "the aliased value keeps a display name of its own: {rendered}"
        );
        assert_eq!(
            rendered
                .lines()
                .filter(|line| line.contains("alias=\"x\""))
                .count(),
            1,
            "{rendered}"
        );
    }

    #[test]
    fn completed_renderer_enforces_checked_line_and_byte_budgets() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            42,
        );
        assert!(matches!(
            ExplainRenderBudget::try_new(0, 1),
            Err(SqlCompileError::InvalidRequest(_))
        ));
        assert!(matches!(
            completed.render_explain_lines_with_budget(
                ExplainRenderBudget::try_new(1, 1024).expect("line budget")
            ),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("exceeds budget")
        ));
        assert!(matches!(
            completed.render_explain_lines_with_budget(
                ExplainRenderBudget::try_new(1024, 8).expect("byte budget")
            ),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("exceeds budget")
        ));
    }

    #[test]
    fn bounded_writer_rejects_large_single_fields_wide_hex_and_coverage() {
        let huge = "x".repeat(1024 * 1024);
        let mut text =
            ExplainRenderOutput::new(ExplainRenderBudget::try_new(4, 128).expect("small budget"));
        assert!(text.push(format_args!("{}", quote_text(&huge))).is_err());

        let bytes = vec![0xab; 128 * 1024];
        let mut hex =
            ExplainRenderOutput::new(ExplainRenderBudget::try_new(4, 128).expect("small budget"));
        assert!(hex.push(format_args!("{}", format_hex(&bytes))).is_err());

        let coverage = novarocks_physical_plan::RuntimeFilterCoverage {
            nodes: (0..32_768)
                .map(|index| {
                    novarocks_physical_plan::RuntimeFilterCoverageNode::Witness(
                        novarocks_physical_plan::RuntimeFilterWitnessId::new(index),
                    )
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            root: 32_767,
        };
        let mut coverage_output =
            ExplainRenderOutput::new(ExplainRenderBudget::try_new(4, 128).expect("small budget"));
        assert!(
            coverage_output
                .push(format_args!("{}", runtime_filter_coverage(&coverage)))
                .is_err()
        );
    }

    #[test]
    fn wide_values_and_deep_join_chains_remain_budgeted_and_iterative() {
        let values = (0..256).map(|_| "1").collect::<Vec<_>>().join(",");
        let completed = completed_sql(
            &format!("select * from (values ({values})) t"),
            SqlCompileIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            },
            46,
        );
        assert!(matches!(
            completed.render_explain_lines_with_budget(
                ExplainRenderBudget::try_new(4096, 512).expect("small budget")
            ),
            Err(SqlCompileError::InvalidRequest(message)) if message.contains("exceeds budget")
        ));

        let seed = completed_sql(
            "select 1 limit 1",
            SqlCompileIntent::Explain {
                level: ExplainLevel::Normal,
                analyze: false,
            },
            47,
        );
        let fragment_id = FragmentId::new(0);
        let mut builder = novarocks_physical_plan::FragmentBuilder::new(fragment_id);
        let root = builder.reserve_node_id().expect("values node id");
        let ty = novarocks_physical_plan::ValueType::new(DataType::Int64, false);
        let expression = builder
            .add_expression(
                root,
                ty.clone(),
                novarocks_physical_plan::ExprKind::Literal(
                    novarocks_physical_plan::LiteralValue::Int64(1),
                ),
            )
            .expect("literal expression");
        let value = builder
            .add_value(
                ty.clone(),
                novarocks_physical_plan::ValueOrigin::NodeOutput {
                    node: root,
                    output_ordinal: 0,
                },
            )
            .expect("values output");
        let properties = novarocks_physical_plan::PhysicalProperties {
            distribution: novarocks_physical_plan::Distribution::Singleton,
            row_multiplicity: novarocks_physical_plan::RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        builder
            .insert_node_unchecked(PhysicalNode {
                id: root,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: properties.clone(),
                output: novarocks_physical_plan::OutputPort {
                    node: root,
                    columns: Box::from([value]),
                },
                kind: NodeKind::Values {
                    rows: Box::from([Box::from([expression])]),
                },
            })
            .expect("values node");
        let mut root = root;
        let mut output = novarocks_physical_plan::OutputPort {
            node: root,
            columns: Box::from([value]),
        };
        for _ in 0..4_000 {
            let node = builder.reserve_node_id().expect("deep node id");
            builder
                .insert_node_unchecked(PhysicalNode {
                    id: node,
                    inputs: Box::from([root]),
                    required_inputs: Box::from([properties.clone()]),
                    output_properties: properties.clone(),
                    output: novarocks_physical_plan::OutputPort {
                        node,
                        columns: output.columns.clone(),
                    },
                    kind: NodeKind::Limit {
                        limit: Some(1),
                        offset: 0,
                    },
                })
                .expect("deep limit node");
            root = node;
            output.node = node;
        }
        let deep_fragment = builder
            .finish_definition(
                root,
                FragmentSink::Result,
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .expect("deep fragment");
        let mut plan = novarocks_physical_plan::PlanBuilder::new(seed.plan().version())
            .with_required_contracts(seed.plan().required());
        plan.add_fragment(deep_fragment)
            .expect("deep fragment in plan");
        plan.set_result_port(novarocks_physical_plan::ResultPort {
            fragment: fragment_id,
            output,
            fields: Box::from([novarocks_physical_plan::ResultField {
                name: "one".into(),
                alias: None,
                value,
                ty,
            }]),
        })
        .expect("deep result port");
        let deep = plan.finish().expect("deep complete plan");
        let rendered = render_plan(
            &seed,
            &deep,
            ExplainLevel::Normal,
            None,
            ExplainRenderBudget::default(),
        )
        .expect("iterative deep render");
        assert!(rendered.len() > 4_000, "deep chain must be fully rendered");
    }

    #[test]
    fn cte_outer_join_and_set_operation_publish_distinct_complete_structures() {
        let cases = [
            (
                "with c(x) as (values (1), (2)) select a.x from c a join c b on a.x = b.x",
                "cte-multicast",
            ),
            (
                "select l.x from (values (1)) l(x) left join (values (2)) r(x) on l.x = r.x",
                "kind=LEFT OUTER",
            ),
            ("select 1 union all select 2", "kind=union-all"),
        ];
        let mut outputs = Vec::new();
        for (ordinal, (sql, token)) in cases.into_iter().enumerate() {
            let rendered = completed_sql(
                sql,
                SqlCompileIntent::Explain {
                    level: ExplainLevel::Verbose,
                    analyze: false,
                },
                50 + ordinal as u8,
            )
            .render_explain_lines()
            .expect("completed structural explain")
            .join("\n");
            assert!(rendered.contains(token), "missing {token}: {rendered}");
            outputs.push(rendered);
        }
        assert_ne!(outputs[0], outputs[1]);
        assert_ne!(outputs[1], outputs[2]);
    }

    #[test]
    fn provider_guarantee_and_residual_facts_change_the_complete_explain() {
        let sql = "select order_key from orders where order_key > 10";
        let exact = completed_external_sql(sql, 60, PredicateGuaranteeKind::Exact)
            .render_explain_lines()
            .expect("exact provider explain")
            .join("\n");
        let pruning = completed_external_sql(sql, 60, PredicateGuaranteeKind::PruningOnly)
            .render_explain_lines()
            .expect("pruning provider explain")
            .join("\n");

        assert_ne!(exact, pruning);
        assert!(exact.contains("provider-guarantee expr=e"), "{exact}");
        assert!(exact.contains("kind=exact"), "{exact}");
        assert!(pruning.contains("kind=pruning-only"), "{pruning}");
        assert!(pruning.contains("residual expr=e"), "{pruning}");
        assert!(pruning.contains("relation-field[0] column="), "{pruning}");
        assert!(pruning.contains("provider-output[0] column="), "{pruning}");
        assert!(pruning.contains("source-bindings=[{read="), "{pruning}");
        assert!(pruning.contains("source-free=false"), "{pruning}");
    }

    #[test]
    fn execute_intent_cannot_enter_the_explain_renderer() {
        let completed = completed(SqlCompileIntent::Query, 32);

        assert!(matches!(
            completed.render_explain_lines(),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }

    #[test]
    fn costs_are_read_from_completed_plan_annotations() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Costs,
                analyze: false,
            },
            35,
        );

        let rendered = completed
            .render_explain_lines()
            .expect("completed cost explain")
            .join("\n");

        assert!(rendered.contains("stats={rows="), "{rendered}");
    }

    #[test]
    fn analyze_profile_is_bound_to_plan_version_and_exact_node_key() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Analyze,
                analyze: true,
            },
            33,
        );
        let profile = exact_profile(
            &completed,
            SqlExplainOperatorMetrics::new(1, 7, 8).into(),
            SqlExplainFragmentMetrics::new(7, 2).into(),
        );
        let rendered =
            explain_completed_plan_analyze(&completed, &profile, ExplainRenderBudget::default())
                .expect("version-bound profile")
                .join("\n");
        assert!(rendered.contains("act={rows=1"), "{rendered}");
        assert!(rendered.contains("Profile: active=7ns"), "{rendered}");

        let mismatched = SqlCompletedExplainProfile::try_new(
            PlanVersionId::try_new([34; 16]).expect("other version"),
            profile
                .operators
                .iter()
                .map(|(key, metrics)| (*key, *metrics))
                .collect(),
            profile
                .fragments
                .iter()
                .map(|(fragment, metrics)| (*fragment, *metrics))
                .collect(),
        )
        .expect("mismatched profile");
        assert!(matches!(
            explain_completed_plan_analyze(&completed, &mismatched, ExplainRenderBudget::default()),
            Err(SqlCompileError::InvalidRequest(_))
        ));
    }

    #[test]
    fn analyze_profile_requires_exact_fragment_and_operator_coverage() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Analyze,
                analyze: true,
            },
            43,
        );
        let mut missing_operator = exact_profile(
            &completed,
            SqlExplainOperatorMetrics::new(1, 2, 3).into(),
            SqlExplainFragmentMetrics::new(4, 5).into(),
        );
        missing_operator
            .operators
            .pop_first()
            .expect("operator fact");
        assert!(matches!(
            completed.render_explain_analyze_lines(&missing_operator),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("operator coverage mismatch") && message.contains("missing=[")
        ));

        let mut extra_fragment = exact_profile(
            &completed,
            SqlExplainOperatorMetrics::new(1, 2, 3).into(),
            SqlExplainFragmentMetrics::new(4, 5).into(),
        );
        extra_fragment.fragments.insert(
            FragmentId::new(u32::MAX),
            SqlExplainFragmentMetrics::new(0, 0).into(),
        );
        assert!(matches!(
            completed.render_explain_analyze_lines(&extra_fragment),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("fragment coverage mismatch") && message.contains("extra=[")
        ));

        let mut missing_fragment = exact_profile(
            &completed,
            SqlExplainOperatorMetrics::new(1, 2, 3).into(),
            SqlExplainFragmentMetrics::new(4, 5).into(),
        );
        missing_fragment
            .fragments
            .pop_first()
            .expect("fragment fact");
        assert!(matches!(
            completed.render_explain_analyze_lines(&missing_fragment),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("fragment coverage mismatch") && message.contains("missing=[")
        ));

        let mut extra_operator = exact_profile(
            &completed,
            SqlExplainOperatorMetrics::new(1, 2, 3).into(),
            SqlExplainFragmentMetrics::new(4, 5).into(),
        );
        let fragment = *completed
            .plan()
            .fragments()
            .keys()
            .next()
            .expect("fragment");
        extra_operator.operators.insert(
            SqlExplainNodeKey::new(fragment, NodeId::new(u32::MAX)),
            SqlExplainOperatorMetrics::new(0, 0, 0).into(),
        );
        assert!(matches!(
            completed.render_explain_analyze_lines(&extra_operator),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("operator coverage mismatch") && message.contains("extra=[")
        ));

        let key = *extra_operator
            .operators
            .keys()
            .next()
            .expect("operator key");
        assert!(matches!(
            SqlCompletedExplainProfile::try_new(
                completed.plan().version(),
                vec![
                    (key, SqlExplainOperatorMetrics::new(0, 0, 0).into()),
                    (key, SqlExplainOperatorMetrics::new(0, 0, 0).into())
                ],
                Vec::new()
            ),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("repeats a fragment/node key")
        ));
        assert!(matches!(
            SqlCompletedExplainProfile::try_new(
                completed.plan().version(),
                Vec::new(),
                vec![
                    (fragment, SqlExplainFragmentMetrics::new(0, 0).into()),
                    (fragment, SqlExplainFragmentMetrics::new(0, 0).into())
                ]
            ),
            Err(SqlCompileError::InvalidRequest(message))
                if message.contains("repeats a fragment key")
        ));
    }

    #[test]
    fn analyze_profile_preserves_unavailable_and_legacy_metric_dimensions() {
        let completed = completed(
            SqlCompileIntent::Explain {
                level: ExplainLevel::Analyze,
                analyze: true,
            },
            44,
        );
        let unavailable = exact_profile(
            &completed,
            SqlExplainObservation::Unavailable(SqlExplainUnavailableReason::RuntimeDidNotReport),
            SqlExplainObservation::Unavailable(SqlExplainUnavailableReason::NotScheduled),
        );
        let rendered = completed
            .render_explain_analyze_lines(&unavailable)
            .expect("typed unavailable profile")
            .join("\n");
        assert!(
            rendered.contains("act={unavailable=runtime-did-not-report}"),
            "{rendered}"
        );
        assert!(
            rendered.contains("Profile: unavailable=not-scheduled"),
            "{rendered}"
        );

        let metrics = SqlExplainOperatorMetrics::new(11, 12, 13)
            .with_time_range(14, 15)
            .with_hash_join_times(16, 17, 18, 19)
            .with_dictionary_counts(20, 21, 22, 23, 24, 25, 26);
        let fragment_metrics =
            SqlExplainFragmentMetrics::new(27, 28).with_wait_times(29, 30, 31, 32);
        let available = exact_profile(&completed, metrics.into(), fragment_metrics.into());
        let rendered = completed
            .render_explain_analyze_lines(&available)
            .expect("full metrics profile")
            .join("\n");
        assert!(
            rendered.contains("rows=11, time=12ns, min=14ns, max=15ns, peak-mem=13B"),
            "{rendered}"
        );
        assert!(
            rendered.contains("build-ht=16ns, search=17ns, out-build=18ns, out-probe=19ns"),
            "{rendered}"
        );
        assert!(rendered.contains("in_rows=20, kept_rows=22, hydrated_rows=24, in_cols=21, kept_cols=23, hydrated_cols=25, unsupported_cols=26"), "{rendered}");
        assert!(rendered.contains("active=27ns, blocked=28ns, dependency-wait=29ns, exchange-wait=30ns, network=31ns, scan-io=32ns"), "{rendered}");
    }
}
