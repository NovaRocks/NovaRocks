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

//! Frontend lowering of one SQL scan into a typed connector scan node.
//!
//! Preparation freezes the relation, resolves every output column to a
//! connector column, offers filter/projection/limit pushdown, and builds the
//! protocol-validated scan source. It deliberately enumerates no split:
//! enumeration is lazy and owned by the execution round, so a prepared scan
//! holds only the split manager it will later be driven through.
//!
//! Ordered assignments are the sole output-order authority. A connector may
//! narrow its own handle in response to a projection, but it never reorders,
//! adds, or removes an output column.
// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::ConnectorControlReadBinding;
use novarocks_spi::connector::ConnectorPinnedFileSet;
use novarocks_spi::connector::read_stack::{
    Assignment, ConnectorReadChangeWindow, ConnectorReadColumnBinding, ConnectorReadConstraint,
    ConnectorReadRelationKind, ConnectorReadRelationVersion, ConnectorReadSplitManager,
    ConnectorReadTableExecuteProcedure, ConnectorReadWorkSource, ConnectorSession,
    ConnectorValueType, SchemaTableName, SystemTableDistribution, TupleDomain,
};
use novarocks_sql::plan_read::PlanScanNode;

use crate::query_execution::connector_domain::{
    CatalogHandle, DynamicFilterBinding, TableHandle, TableScanNode,
};

use super::scan::ResolvedScanColumn;
use super::typed_predicate::{lower_scan_predicates, scan_output_value_type};

/// Reader batch budgets for a typed scan.
///
/// The typed scan source carries its own budgets and the protocol rejects a
/// zero, so preparation names bounded defaults here rather than inventing one
/// at the wire encoder. `4096` rows matches every other frontend read budget.
const DEFAULT_MAX_BATCH_ROWS: u64 = 4096;
const DEFAULT_MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;

/// Prefix of the per-scan assignment variable. The scan output ordinal makes
/// the name unique within one scan without depending on the column's SQL name,
/// which may be an alias.
const SCAN_VARIABLE_PREFIX: &str = "v";

/// Which relation family preparation asks the connector to freeze, and the
/// exact pin that names it.
///
/// The variant decides which control entry point is called, so a lane can never
/// reach a family it did not ask for.
#[derive(Clone, Debug)]
pub(crate) enum TypedRelationFreeze<'a> {
    /// One relation as of a point in time, optionally reached through a
    /// connector-resolved branch or tag name.
    Table {
        version: ConnectorReadRelationVersion,
        reference: Option<&'a str>,
    },
    /// One relation restricted to exactly the files a provider froze for one
    /// mutation or rewrite cohort.
    ///
    /// The set is the whole definition of the read. It is a freeze of its own
    /// rather than a `Table` with a pushdown, because a pushdown may be
    /// declined and a declined file set would silently widen the read to the
    /// whole snapshot -- which a cohort's commit then contradicts.
    PinnedFileSet(&'a ConnectorPinnedFileSet),
    /// The set difference between the rows visible at two snapshots.
    ///
    /// Both endpoints are pinned by the frozen handle. A row written and
    /// deleted inside the window is invisible at both endpoints and is
    /// therefore not part of the window: this is a difference of two visible
    /// row sets, never a replay of the manifests between them.
    ChangeWindow(ConnectorReadChangeWindow),
    /// The relation one distributed `ALTER TABLE ... EXECUTE` procedure
    /// instance reads.
    ///
    /// The procedure names the exact frozen group it rewrites, and the
    /// connector resolves that group back to its artifacts itself. It is a
    /// freeze of its own because what such a read produces is not the
    /// relation's rows: rewriting delete artifacts produces the rows those
    /// artifacts remove, which no `Table` or `PinnedFileSet` read can describe.
    TableExecute(ConnectorReadTableExecuteProcedure),
    /// One system relation of a table.
    ///
    /// It carries no pin of its own: the relation name already spells the
    /// connector's `<table>$<SUFFIX>` vocabulary, and the connector resolves
    /// that suffix and pins the immutable metadata file behind it. How the
    /// relation is executed is the connector's answer too, not the engine's.
    SystemTable,
}

impl TypedRelationFreeze<'_> {
    /// The relation family this freeze must produce.
    pub(crate) const fn relation_kind(&self) -> ConnectorReadRelationKind {
        match self {
            Self::Table { .. } | Self::PinnedFileSet(_) => ConnectorReadRelationKind::Table,
            Self::ChangeWindow(_) => ConnectorReadRelationKind::ChangeWindow,
            Self::TableExecute(_) => ConnectorReadRelationKind::TableExecute,
            Self::SystemTable => ConnectorReadRelationKind::SystemTable,
        }
    }
}

/// One SQL scan lowered against one installed typed connector control.
pub(crate) struct PreparedTypedScan {
    /// The fragment-plan scan node, carrying the frozen relation handle and
    /// the ordered assignments.
    pub(crate) table_scan: TableScanNode,
    /// The connector's lazy split enumerator entry point. Preparation never
    /// calls it; the execution round does.
    pub(crate) split_manager: Arc<dyn ConnectorReadSplitManager>,
    /// The only conversion authority for this exact binding.  It is retained
    /// solely for fragment and TaskUpdate egress; planning never calls it.
    pub(crate) encoder: Arc<dyn novarocks_spi::connector::ConnectorReadWireEncoder>,
    /// The constraint that was offered to the connector, kept so the round
    /// driver enumerates splits under exactly what planning pushed down.
    pub(crate) constraint: ConnectorReadConstraint,
    /// Ordinals into `PlanScanNode::predicates` that have no exact domain
    /// representation at all, so the SQL side must still evaluate them.
    ///
    /// A conjunct the connector declined is not listed here: it was
    /// representable, and it travels as the scan source's
    /// `unenforced_predicate`, which the backend reader applies.
    pub(crate) residual_ordinals: Vec<usize>,
    /// Whether the connector guarantees the pushed-down limit. Only the
    /// connector's own answer sets this; an absent or declined limit leaves it
    /// false so the engine keeps its own limit operator.
    pub(crate) limit_guaranteed: bool,
    /// The scan output column each dynamic filter was bound to, keyed by the
    /// runtime filter's id.
    ///
    /// It records the binding this scan actually made, so a later resolution
    /// proves the filter reaches the reader through that column's assignment
    /// instead of assuming it did.
    pub(crate) dynamic_filter_outputs: BTreeMap<u32, String>,
}

impl PreparedTypedScan {
    /// The scan output column one runtime filter constrains on this scan.
    pub(crate) fn dynamic_filter_output(&self, filter_id: u32) -> Option<&str> {
        self.dynamic_filter_outputs
            .get(&filter_id)
            .map(String::as_str)
    }
}

impl std::fmt::Debug for PreparedTypedScan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The split manager is a connector-owned trait object with no debug
        // surface; everything the engine itself decided is shown.
        formatter
            .debug_struct("PreparedTypedScan")
            .field("table_scan", &self.table_scan)
            .field("constraint", &self.constraint)
            .field("residual_ordinals", &self.residual_ordinals)
            .field("limit_guaranteed", &self.limit_guaranteed)
            .field("dynamic_filter_outputs", &self.dynamic_filter_outputs)
            .finish_non_exhaustive()
    }
}

/// Lower one SQL scan into a typed connector scan node.
///
/// `physical_columns` is the scan's ordered *physical* output: the columns the
/// connector itself produces. A synthetic output — a VARIANT path column, for
/// instance — is deliberately absent, because the connector has no column for
/// it; the backend materializes those on top of the physical read slots.
///
/// `dynamic_filters` pairs a runtime-filter id with the scan output column
/// name it constrains. A name this scan does not output is an error: silently
/// dropping the binding would leave the filter's producer waiting on a
/// consumer that never applies it.
#[expect(
    clippy::too_many_arguments,
    reason = "Every argument is a distinct frozen fact of one scan; grouping them would hide which of them the connector sees."
)]
pub(crate) fn prepare_typed_scan(
    session: &ConnectorSession,
    catalog: CatalogHandle,
    control: &ConnectorControlReadBinding,
    request_control: &novarocks_spi::connector::read_stack::ConnectorReadRequestControl,
    plan_node_id: i32,
    scan: &PlanScanNode,
    physical_columns: &[ResolvedScanColumn],
    relation: &SchemaTableName,
    freeze: TypedRelationFreeze<'_>,
    limit: Option<u64>,
    dynamic_filters: &[(u32, String)],
) -> Result<PreparedTypedScan, String> {
    let relation_kind = freeze.relation_kind();
    let metadata = request_control.metadata();
    let relation_name = format!("{}.{}", relation.schema_name(), relation.table_name());

    // 1. Freeze the relation the lane asked for. Admission already resolved
    //    this name, so a connector that now reports nothing means the pin is
    //    gone rather than that the query referenced an unknown table.
    //
    //    How this scan's work reaches a backend is decided here too, because
    //    only the connector knows it: a system relation it resolves to one
    //    task has no split at all.
    let (mut handle, work_source) = match freeze {
        TypedRelationFreeze::Table { version, reference } => {
            let handle = metadata
                .get_table_handle(session, relation, version, reference)
                .map_err(|error| {
                    format!("typed scan cannot freeze relation {relation_name}: {error}")
                })?
                .ok_or_else(|| {
                    format!(
                        "typed scan relation {relation_name} is no longer resolvable after admission pinned it"
                    )
                })?;
            (handle, ConnectorReadWorkSource::RuntimeSplits)
        }
        TypedRelationFreeze::PinnedFileSet(pinned) => {
            let handle = metadata
                .get_pinned_file_set_handle(session, relation, pinned)
                .map_err(|error| {
                    format!(
                        "typed scan cannot freeze relation {relation_name} restricted to the {} data files pinned at version {}: {error}",
                        pinned.files().len(),
                        pinned.version_ordinal()
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "typed scan relation {relation_name} does not expose a pinned file set read"
                    )
                })?;
            (handle, ConnectorReadWorkSource::RuntimeSplits)
        }
        TypedRelationFreeze::ChangeWindow(window) => {
            let handle = metadata
                .get_change_window_plan(session, relation, window)
                .map_err(|error| {
                    format!(
                        "typed scan cannot freeze the change window of relation {relation_name} from snapshot {} to snapshot {}: {error}",
                        window.from_snapshot_id(),
                        window.to_snapshot_id()
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "typed scan relation {relation_name} exposes no change window from snapshot {} to snapshot {}",
                        window.from_snapshot_id(),
                        window.to_snapshot_id()
                    )
                })?;
            (handle, ConnectorReadWorkSource::RuntimeSplits)
        }
        TypedRelationFreeze::TableExecute(procedure) => {
            let handle = metadata
                .get_table_execute_plan(session, relation, procedure)
                .map_err(|error| {
                    format!(
                        "typed scan cannot freeze the table-execute relation of {relation_name}: {error}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "typed scan relation {relation_name} exposes no table-execute relation"
                    )
                })?;
            (handle, ConnectorReadWorkSource::RuntimeSplits)
        }
        TypedRelationFreeze::SystemTable => {
            let plan = metadata
                .get_system_table_plan(session, relation)
                .map_err(|error| {
                    format!(
                        "typed scan cannot freeze system relation {relation_name}: {error}"
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "typed scan relation {relation_name} is not a system relation of this connector"
                    )
                })?;
            // `SingleCoordinator` means one backend reads one immutable
            // metadata file with no split; spreading it would duplicate every
            // row. `AllNodes` is real distributable I/O and enumerates.
            let work_source = match plan.distribution() {
                SystemTableDistribution::AllNodes => ConnectorReadWorkSource::RuntimeSplits,
                SystemTableDistribution::SingleCoordinator => {
                    ConnectorReadWorkSource::WholeRelation
                }
            };
            (plan.into_handle(), work_source)
        }
    };

    // 2. Resolve the relation's columns.
    let column_bindings = metadata
        .get_column_bindings(session, &handle)
        .map_err(|error| {
            format!("typed scan cannot read the columns of relation {relation_name}: {error}")
        })?;

    // 3. Build the ordered assignments. `scan.columns` order is the output
    //    authority, and the connector produces exactly its physical subset, so
    //    the scan's own order is walked once and never sorted or deduplicated.
    //    Taking `physical_columns` order instead would reorder the assignments
    //    whenever a refresh lane resolved its physical columns in some other
    //    order than the plan node outputs them.
    let physical_by_column_id = physical_columns
        .iter()
        .map(|column| (column.planner.column_id, column))
        .collect::<BTreeMap<_, _>>();
    let ordered_physical = scan
        .columns
        .iter()
        .filter_map(|column| physical_by_column_id.get(&column.column_id).copied())
        .collect::<Vec<_>>();
    // A physical column the scan does not output has no output slot to fill,
    // and dropping it would silently shift every later assignment.
    if ordered_physical.len() != physical_columns.len() {
        return Err(format!(
            "typed scan of relation {relation_name} resolved {} physical columns but the scan outputs only {} of them",
            physical_columns.len(),
            ordered_physical.len()
        ));
    }
    let mut assignments = Vec::with_capacity(ordered_physical.len());
    let mut columns_by_name = BTreeMap::new();
    let mut value_types_by_name: BTreeMap<String, ConnectorValueType> = BTreeMap::new();
    let mut variables_by_name: BTreeMap<String, String> = BTreeMap::new();
    for (ordinal, output) in ordered_physical.iter().enumerate() {
        // The connector is asked for the column by its own schema spelling;
        // the planner name beside it may be an alias and is never provider
        // identity.
        let binding = unique_binding(&column_bindings, &output.source.name, &relation_name)?;
        // `ConnectorReadColumnBinding` carries column identity, not column type, so
        // the assignment's exact type is the engine's own declared type. A
        // type with no exact typed counterpart is rejected rather than
        // approximated: the connector would otherwise filter and decode
        // against a type the query never stated.
        let value_type = scan_output_value_type(&output.planner.data_type).ok_or_else(|| {
            format!(
                "typed scan output column '{}' of relation {relation_name} has engine type {:?}, which has no exact typed connector counterpart",
                output.source.name, output.planner.data_type
            )
        })?;
        let variable = format!("{SCAN_VARIABLE_PREFIX}{ordinal}");
        let assignment = Assignment::try_new(variable.clone(), binding.column().clone(), value_type)
        .map_err(|error| {
            let column_name = &output.source.name;
            format!(
                "typed scan assignment for output column '{column_name}' of relation {relation_name}: {error}"
            )
        })?;
        // Predicate and dynamic-filter lookups are by planner output column
        // name, because that is the name the plan's own column references
        // resolve to. Two outputs sharing a name would make those lookups
        // ambiguous, and picking either one would push down a predicate about
        // the other.
        if columns_by_name
            .insert(output.planner.name.clone(), binding.column().clone())
            .is_some()
        {
            return Err(format!(
                "typed scan of relation {relation_name} outputs column '{}' more than once",
                output.planner.name
            ));
        }
        value_types_by_name.insert(output.planner.name.clone(), value_type);
        variables_by_name.insert(output.planner.name.clone(), variable);
        assignments.push(assignment);
    }

    // 4. Lower the scan's own conjuncts into the offered summary.
    let mut lowered = lower_scan_predicates(scan, &columns_by_name, &value_types_by_name);
    let constraint = ConnectorReadConstraint::of_summary(lowered.summary.clone());

    // 5. Offer the filter. Whatever the connector hands back stays the
    //    reader's own work: `unenforced_predicate` is applied by the backend
    //    scan, `enforced_predicate` records only what the connector took.
    let (enforced_predicate, unenforced_predicate, remaining_expression) = match metadata
        .apply_filter(session, &handle, &constraint)
        .map_err(|error| {
            format!("typed scan filter pushdown on relation {relation_name} failed: {error}")
        })? {
        // Nothing was accepted, so the engine keeps the whole predicate — and
        // keeping it means evaluating it, not handing it to the reader.
        //
        // An unenforced predicate is the reader's own work by contract. A
        // relation the connector declines outright may have no reader that
        // applies one: a system relation read whole by a single backend opens
        // its page source with no constraint at all, so a predicate parked
        // there is applied by nobody and the query silently returns rows it
        // must not see.
        None => {
            lowered.residual_ordinals = (0..scan.predicates.len()).collect();
            (TupleDomain::all(), TupleDomain::all(), None)
        }
        Some(application) => {
            let unenforced = application.remaining_constraint().summary().clone();
            let remaining_expression = application
                .remaining_expression()
                .filter(|expression| !expression.is_constant_true())
                .cloned();
            // Enforcement is claimed only for a column the connector kept
            // whole. A column it handed back partially is covered by its own
            // guarantee for the complement, and by the reader for the rest.
            let enforced = lowered
                .summary
                .filter_columns(|column| unenforced.domain_for(column).is_none());
            handle = application.into_handle();
            (enforced, unenforced, remaining_expression)
        }
    };

    // 6. Offer the projection. A narrowed handle is a pushdown fact; the
    //    ordered assignments above remain the output authority either way.
    if let Some(narrowed) = metadata
        .apply_projection(session, &handle, &assignments)
        .map_err(|error| {
            format!("typed scan projection pushdown on relation {relation_name} failed: {error}")
        })?
    {
        handle = narrowed;
    }

    // 7. Offer the limit. Only the connector's own answer may drop the
    //    engine's limit operator.
    let mut limit_guaranteed = false;
    if let Some(limit) = limit
        && let Some(application) =
            metadata
                .apply_limit(session, &handle, limit)
                .map_err(|error| {
                    format!("typed scan limit pushdown on relation {relation_name} failed: {error}")
                })?
    {
        limit_guaranteed = application.limit_guaranteed();
        handle = application.into_handle();
    }

    // 8. Bind dynamic filters and freeze the exact relation.  The metadata
    // service alone can pair the opaque table with its installed transaction;
    // frontend code never sees or constructs the transaction payload.
    let (dynamic_filter_bindings, dynamic_filter_outputs) =
        bind_dynamic_filters(dynamic_filters, &variables_by_name, &relation_name)?;
    let relation = metadata
        .relation(relation_kind, handle)
        .map_err(|error| format!("typed scan cannot freeze relation {relation_name}: {error}"))?;
    let table_scan = TableScanNode::new(
        plan_node_id,
        TableHandle::new(catalog, relation),
        assignments,
        enforced_predicate,
        unenforced_predicate,
        remaining_expression,
        dynamic_filter_bindings,
        DEFAULT_MAX_BATCH_ROWS,
        DEFAULT_MAX_BATCH_BYTES,
        work_source,
    )
    .map_err(|error| format!("typed scan node {plan_node_id}: {error}"))?;

    // 9. Take the enumerator entry point without enumerating anything.
    Ok(PreparedTypedScan {
        table_scan,
        split_manager: request_control.splits(),
        encoder: control.encoder(),
        constraint,
        residual_ordinals: lowered.residual_ordinals,
        limit_guaranteed,
        dynamic_filter_outputs,
    })
}

/// The one connector column an output column names.
///
/// Matching is case-insensitive because the SQL and connector spellings of one
/// column may differ, but it must be unambiguous: two connector columns whose
/// names differ only in case give no basis for choosing between them.
fn unique_binding<'a>(
    bindings: &'a [ConnectorReadColumnBinding],
    name: &str,
    relation_name: &str,
) -> Result<&'a ConnectorReadColumnBinding, String> {
    let mut matched = bindings
        .iter()
        .filter(|binding| binding.name().eq_ignore_ascii_case(name));
    let binding = matched.next().ok_or_else(|| {
        format!(
            "typed scan output column '{name}' has no column binding in relation {relation_name}"
        )
    })?;
    if matched.next().is_some() {
        return Err(format!(
            "typed scan output column '{name}' matches more than one column binding in relation {relation_name}"
        ));
    }
    Ok(binding)
}

/// The carrier bindings and the output column each filter id was bound to.
///
/// One filter id may name only one column of one scan: two bindings would ask
/// the reader to constrain two columns from a single artifact, and choosing
/// either of them would apply the filter to a column it was not built for.
fn bind_dynamic_filters(
    dynamic_filters: &[(u32, String)],
    variables_by_name: &BTreeMap<String, String>,
    relation_name: &str,
) -> Result<(Vec<DynamicFilterBinding>, BTreeMap<u32, String>), String> {
    let mut bindings = Vec::with_capacity(dynamic_filters.len());
    let mut outputs = BTreeMap::new();
    for (filter_id, column_name) in dynamic_filters {
        let variable = variables_by_name.get(column_name).ok_or_else(|| {
            format!(
                "typed scan dynamic filter {filter_id} names column '{column_name}', which the scan of relation {relation_name} does not output"
            )
        })?;
        if outputs.insert(*filter_id, column_name.clone()).is_some() {
            return Err(format!(
                "typed scan of relation {relation_name} binds dynamic filter {filter_id} more than once"
            ));
        }
        bindings.push(DynamicFilterBinding::new(*filter_id, variable.clone()));
    }
    Ok((bindings, outputs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_filter_bindings_preserve_declared_order_and_reject_duplicates() {
        let variables = BTreeMap::from([(String::from("id"), String::from("v0"))]);
        let (bindings, outputs) =
            bind_dynamic_filters(&[(7, String::from("id"))], &variables, "db.t").expect("binding");
        assert_eq!(bindings[0].filter_id(), 7);
        assert_eq!(bindings[0].variable(), "v0");
        assert_eq!(outputs.get(&7).map(String::as_str), Some("id"));
        assert!(
            bind_dynamic_filters(
                &[(7, String::from("id")), (7, String::from("id"))],
                &variables,
                "db.t"
            )
            .is_err()
        );
    }

    #[test]
    fn system_table_freeze_keeps_its_spi_relation_kind() {
        assert_eq!(
            TypedRelationFreeze::SystemTable.relation_kind(),
            ConnectorReadRelationKind::SystemTable
        );
    }
}
