//! Immutable refresh-time context for Iceberg MV refresh.
//!
//! Two layers:
//! - `IcebergMvRewriteContext` — pure metadata that future optimizer rewrite
//!   rules (TODO list tasks 2 / 3 / 4) consume.
//! - `IcebergMvRefreshContext` — wraps the rewrite layer and adds the
//!   execution handles only the current refresh path needs.
//!
//! Constructed once per refresh attempt, after pin capture and schema-contract
//! rebind. See `docs/design/specs/2026-05-26-iceberg-mv-rewrite-context-design.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use iceberg::spec::Schema;

use crate::connector::iceberg::catalog::registry::{IcebergCatalogEntry, IcebergCatalogRegistry};
use crate::connector::starrocks::table::model::IcebergTableRef;
use crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin;
use crate::meta::repository::mv::StoredMvDefinition;
use crate::meta::repository::mv_contract::MvSchemaContract;
use crate::sql::catalog::{
    IcebergDataFileInfo, IcebergMvTargetStateScan, IcebergPartitionFieldValue,
    IcebergPartitionValue, IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo, ScanSource,
};

use super::iceberg_refresh::IcebergMvTarget;

/// Read-only metadata that drives Iceberg MV refresh rewrite.
///
/// Future optimizer rewrite rules consume `Arc<IcebergMvRewriteContext>` and
/// MUST NOT depend on `iceberg::table::Table`, `iceberg::Catalog`, or
/// `IcebergCatalogEntry` — those live in `IcebergMvRefreshContext`.
#[derive(Debug)]
pub(crate) struct IcebergMvRewriteContext {
    // ---- Identity ----
    pub target: IcebergMvTarget,
    pub mv_id: i64,

    // ---- Session ----
    pub current_catalog: Option<String>,
    pub current_database: String,

    // ---- MV definition (post schema-contract rebind) ----
    pub mv_definition: Arc<StoredMvDefinition>,
    pub canonical_select_query: Arc<sqlparser::ast::Query>,

    // ---- Base table inputs ----
    pub base_refs: Arc<[IcebergTableRef]>,
    pub pin: Arc<RefreshSnapshotPin>,
    pub previous_snapshot_ids: BTreeMap<String, i64>,
    pub previous_table_uuids: BTreeMap<String, String>,

    // ---- Target table inputs (extracted from target_table.metadata()) ----
    pub target_snapshot_id: Option<i64>,
    pub target_table_uuid: String,
    pub target_schema: Arc<Schema>,

    // ---- Contracts ----
    pub schema_contract: Arc<MvSchemaContract>,
}

/// Refresh-time context. Wraps `IcebergMvRewriteContext` and adds execution
/// handles only the refresh path needs.
pub(crate) struct IcebergMvRefreshContext {
    pub rewrite: Arc<IcebergMvRewriteContext>,
    pub target_entry: Arc<IcebergCatalogEntry>,
    pub base_catalog_entries: BTreeMap<String, IcebergCatalogEntry>,
    pub iceberg_catalog: Arc<dyn iceberg::Catalog>,
    pub target_table: iceberg::table::Table,
    pub affected_partitions: crate::engine::mv::partition::AffectedTargetPartitions,
    pub pruning_limits: MvRefreshPruningLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MvRefreshPruningLimits {
    pub max_touched_groups: usize,
    pub max_affected_partitions: usize,
}

impl Default for MvRefreshPruningLimits {
    fn default() -> Self {
        Self {
            max_touched_groups: 100_000,
            max_affected_partitions: 4_096,
        }
    }
}

impl MvRefreshPruningLimits {
    pub(crate) fn from_standalone_config(
        config: &crate::common::app_config::StandaloneServerConfig,
    ) -> Self {
        Self {
            max_touched_groups: config.mv_refresh_max_touched_groups,
            max_affected_partitions: config.mv_refresh_max_affected_partitions,
        }
    }

    pub(crate) fn affected_partition_count_exceeds_limit(&self, partition_count: usize) -> bool {
        partition_count > self.max_affected_partitions
    }

    pub(crate) fn touched_group_count_exceeds_limit(&self, touched_group_count: usize) -> bool {
        touched_group_count > self.max_touched_groups
    }
}

/// Debug-only view of an `IcebergMvRewriteContext`. No `Display` impl — log
/// via `tracing::info!(summary = ?ctx.rewrite.summary(), ...)`.
#[derive(Debug)]
pub(crate) struct CtxSummary<'a> {
    pub target: &'a IcebergMvTarget,
    pub mv_id: i64,
    pub base_count: usize,
    pub base_fqns: Vec<String>,
    pub pinned_snapshots: Vec<(String, i64)>,
    pub previous_snapshots: Vec<(String, Option<i64>)>,
    pub target_snapshot_id: Option<i64>,
    pub schema_contract_version: u16,
    pub partition_contract_present: bool,
    pub visible_output_column_count: usize,
    pub hidden_apply_key_column: &'a str,
}

fn err(msg: impl Into<String>) -> String {
    format!("IcebergMvRewriteContext::new: {}", msg.into())
}

impl IcebergMvRewriteContext {
    /// Build the rewrite layer from already-derived primitive inputs.
    ///
    /// `IcebergMvRefreshContext::new` uses this internally after pulling
    /// `target_snapshot_id` / `target_table_uuid` / `target_schema` out of
    /// `target_table.metadata()`. Unit tests construct the rewrite layer
    /// directly via this helper without needing a real `iceberg::table::Table`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<String>,
        current_database: String,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        target_schema: Arc<Schema>,
        schema_contract: Option<Arc<MvSchemaContract>>,
    ) -> Result<Self, String> {
        let target_fqn = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
        let schema_contract = schema_contract.ok_or_else(|| {
            err(format!(
                "missing schema contract on target {target_fqn}; rebuild or recreate the MV"
            ))
        })?;

        if base_refs.is_empty() {
            return Err(err("mv definition has no base table refs"));
        }

        let pin_count = pin.len();
        if pin_count != base_refs.len() {
            return Err(err(format!(
                "refresh pin covers {} bases but definition has {}",
                pin_count,
                base_refs.len()
            )));
        }

        for base_ref in base_refs.iter() {
            if pin.uuid(base_ref).is_none() {
                return Err(err(format!(
                    "refresh pin missing uuid for base {}",
                    base_ref.fqn()
                )));
            }
        }

        let previous_snapshot_ids = mv_definition.last_refresh_snapshots.clone();
        let previous_table_uuids = mv_definition.last_refresh_table_uuids.clone();

        for base_ref in base_refs.iter() {
            let fqn = base_ref.fqn();
            if let Some(previous_uuid) = previous_table_uuids.get(&fqn) {
                let current_uuid = pin
                    .uuid(base_ref)
                    .expect("uuid presence verified above")
                    .to_string();
                if previous_uuid != &current_uuid {
                    return Err(err(format!(
                        "base table identity changed for {fqn}; incremental refresh unsafe, rebuild the MV"
                    )));
                }
            }
        }

        // The target schema is the union of visible columns (those listed in
        // `schema_contract.target.visible_columns`), the hidden apply-key
        // column (`schema_contract.target.hidden_apply_key.target_field_id`),
        // and — for aggregate MVs — the hidden aggregate-state columns listed
        // in `schema_contract.aggregate.state_columns`. The hidden apply-key
        // field id can coincide with a visible column (when the apply-key
        // aliases an existing user column) or be a distinct field (the
        // common case, e.g. `__nova_base_row_id` / `__row_id__`).
        let schema_field_ids: BTreeSet<i32> = target_schema
            .as_ref()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.id)
            .collect();
        let mut contract_field_ids: BTreeSet<i32> = schema_contract
            .target
            .visible_columns
            .iter()
            .map(|c| c.target_field_id)
            .collect();
        contract_field_ids.insert(schema_contract.target.hidden_apply_key.target_field_id);
        if let Some(aggregate) = &schema_contract.aggregate {
            for state_col in &aggregate.state_columns {
                contract_field_ids.insert(state_col.target_field_id);
            }
        }
        if let Some(branch) = &schema_contract.branch {
            contract_field_ids.insert(branch.branch_id_column.target_field_id);
        }
        if schema_field_ids != contract_field_ids {
            return Err(err(format!(
                "target schema/contract field id mismatch: schema has {:?}, contract has {:?}",
                schema_field_ids, contract_field_ids
            )));
        }

        let apply_key_name = &schema_contract.target.hidden_apply_key.column_name;
        let apply_key_in_schema = target_schema
            .as_ref()
            .as_struct()
            .fields()
            .iter()
            .any(|f| &f.name == apply_key_name);
        if !apply_key_in_schema {
            return Err(err(format!(
                "target apply-key column {apply_key_name} not present in target schema"
            )));
        }

        Ok(Self {
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            previous_snapshot_ids,
            previous_table_uuids,
            target_snapshot_id,
            target_table_uuid,
            target_schema,
            schema_contract,
        })
    }

    pub(crate) fn summary(&self) -> CtxSummary<'_> {
        let n = self.base_refs.len();
        let mut base_fqns: Vec<String> = Vec::with_capacity(n);
        let mut pinned_snapshots: Vec<(String, i64)> = Vec::with_capacity(n);
        let mut previous_snapshots: Vec<(String, Option<i64>)> = Vec::with_capacity(n);
        for r in self.base_refs.iter() {
            let fqn = r.fqn();
            let snap = self
                .pin
                .get(r)
                .expect("pin coverage verified in constructor");
            let prev = self.previous_snapshot_ids.get(&fqn).copied();
            pinned_snapshots.push((fqn.clone(), snap));
            previous_snapshots.push((fqn.clone(), prev));
            base_fqns.push(fqn);
        }

        CtxSummary {
            target: &self.target,
            mv_id: self.mv_id,
            base_count: self.base_refs.len(),
            base_fqns,
            pinned_snapshots,
            previous_snapshots,
            target_snapshot_id: self.target_snapshot_id,
            schema_contract_version: self.schema_contract.contract_version,
            partition_contract_present: self.schema_contract.target.partition.is_some(),
            visible_output_column_count: self.schema_contract.target.visible_columns.len(),
            hidden_apply_key_column: &self.schema_contract.target.hidden_apply_key.column_name,
        }
    }

    pub(crate) fn aggregate_shape_and_layout_for_execution(
        &self,
    ) -> Result<
        (
            crate::connector::starrocks::table::aggregate_sql_calls::AggregateSqlCalls,
            crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
        ),
        String,
    > {
        // Source the aggregate-call surface from the focused extractor (not the
        // legacy union classifier), so a composed branch (`Agg(a JOIN b)` /
        // `Agg(fan-in)`) is supported. For a branch UNION ALL, every branch shares
        // the same output schema (the UNION ALL requirement) and — under the
        // CREATE-time homogeneity gate — the same aggregate layout, so the
        // aggregate-state physical layout is derived from the FIRST branch's
        // aggregate calls, exactly the surface the CREATE path used to build the
        // target schema + contract. For a single aggregate / join aggregate, the
        // whole query is the aggregate.
        let query = self.canonical_select_query.as_ref();
        let aggregate_query = if is_union_all_query(query) {
            first_union_branch_query(query)?
        } else {
            query.clone()
        };
        let aggregate_calls =
            crate::connector::starrocks::table::aggregate_sql_calls::extract_aggregate_sql_calls(
                &aggregate_query,
            )
            .map_err(|e| format!("extract aggregate calls for execution layout: {e}"))?;

        let arrow_schema = iceberg::arrow::schema_to_arrow_schema(self.target_schema.as_ref())
            .map_err(|e| format!("convert target iceberg schema to arrow schema: {e}"))?;
        let iceberg_fields = self.target_schema.as_ref().as_struct().fields();
        let mut output_columns =
            Vec::with_capacity(self.schema_contract.target.visible_columns.len());
        for visible in &self.schema_contract.target.visible_columns {
            let field_idx = iceberg_fields
                .iter()
                .position(|field| field.id == visible.target_field_id)
                .ok_or_else(|| {
                    format!(
                        "target visible column {} field id {} is missing from target schema",
                        visible.output_name, visible.target_field_id
                    )
                })?;
            let arrow_field = arrow_schema.field(field_idx);
            output_columns.push(crate::sql::analysis::OutputColumn {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                name: visible.output_name.clone(),
                data_type: arrow_field.data_type().clone(),
                nullable: visible.nullable,
                is_internal: false,
            });
        }

        let aggregate_input_types =
            aggregate_input_types_from_schema_contract(&aggregate_calls, &self.schema_contract)?;
        let layout =
            crate::connector::starrocks::table::mv_agg_state::build_aggregate_mv_layout_with_input_types(
            &aggregate_calls,
            &output_columns,
            &aggregate_input_types,
        )?;
        Ok((aggregate_calls, layout))
    }
}

/// Whether `query`'s body is a UNION ALL set operation (possibly nested), used
/// to decide whether to source aggregate calls from the first branch.
fn is_union_all_query(query: &sqlparser::ast::Query) -> bool {
    matches!(
        query.body.as_ref(),
        sqlparser::ast::SetExpr::SetOperation {
            op: sqlparser::ast::SetOperator::Union,
            set_quantifier: sqlparser::ast::SetQuantifier::All,
            ..
        }
    )
}

/// The first UNION ALL branch as a standalone `Query` (keeps the branch's own
/// FROM — a scan, a join, or a fan-in union). Works off the AST so a composed
/// branch is not classified.
fn first_union_branch_query(
    query: &sqlparser::ast::Query,
) -> Result<sqlparser::ast::Query, String> {
    fn first_branch_body(
        body: &sqlparser::ast::SetExpr,
    ) -> Result<&sqlparser::ast::SetExpr, String> {
        match body {
            sqlparser::ast::SetExpr::SetOperation { left, .. } => first_branch_body(left),
            sqlparser::ast::SetExpr::Query(inner) => first_branch_body(inner.body.as_ref()),
            other => Ok(other),
        }
    }
    let mut branch = query.clone();
    branch.body = Box::new(first_branch_body(query.body.as_ref())?.clone());
    Ok(branch)
}

fn aggregate_input_types_from_schema_contract(
    calls: &crate::connector::starrocks::table::aggregate_sql_calls::AggregateSqlCalls,
    contract: &MvSchemaContract,
) -> Result<Vec<Option<DataType>>, String> {
    use crate::connector::starrocks::table::mv_shape::{AggregateInput, VisibleAggregateOutput};

    let mut input_types = vec![None; calls.aggregates.len()];
    for (aggregate_index, aggregate) in calls.aggregates.iter().enumerate() {
        if matches!(aggregate.input, AggregateInput::Star) {
            continue;
        }
        if let Some(cast_type) = aggregate_input_cast_type(&aggregate.input)? {
            input_types[aggregate_index] = Some(cast_type);
            continue;
        }

        let visible_index = calls
            .visible_outputs
            .iter()
            .position(|output| {
                matches!(output, VisibleAggregateOutput::Aggregate(index) if *index == aggregate_index)
            })
            .ok_or_else(|| {
                format!(
                    "aggregate MV aggregate output is not visible: aggregate_index={aggregate_index}"
                )
            })?;
        let lineage = contract.output.columns.get(visible_index).ok_or_else(|| {
            format!(
                "aggregate MV contract output lineage missing for visible index {visible_index}"
            )
        })?;
        input_types[aggregate_index] =
            aggregate_input_type_from_lineage(contract, &lineage.expression)?;
    }
    Ok(input_types)
}

fn aggregate_input_cast_type(
    input: &crate::connector::starrocks::table::mv_shape::AggregateInput,
) -> Result<Option<DataType>, String> {
    let crate::connector::starrocks::table::mv_shape::AggregateInput::Expr(expr) = input else {
        return Ok(None);
    };
    explicit_cast_type(expr)
}

fn explicit_cast_type(expr: &sqlparser::ast::Expr) -> Result<Option<DataType>, String> {
    match expr {
        sqlparser::ast::Expr::Cast { data_type, .. } => sql_data_type_to_arrow(data_type).map(Some),
        sqlparser::ast::Expr::Nested(inner) => explicit_cast_type(inner),
        _ => Ok(None),
    }
}

fn aggregate_input_type_from_lineage(
    contract: &MvSchemaContract,
    lineage: &crate::meta::repository::mv_contract::ExpressionLineage,
) -> Result<Option<DataType>, String> {
    if let [qualified] = lineage.referenced_base_fields.as_slice() {
        let Some(field) = base_contracts(contract)
            .into_iter()
            .find(|base| base.table_fqn.eq_ignore_ascii_case(&qualified.table_fqn))
            .and_then(|base| {
                base.schema_at_create
                    .fields
                    .iter()
                    .find(|field| field.field_id == qualified.field_id)
            })
        else {
            return Err(format!(
                "aggregate MV contract references unknown base field {}#{}",
                qualified.table_fqn, qualified.field_id
            ));
        };
        return arrow_type_from_contract_signature(&field.type_signature).map(Some);
    }

    if let [field_id] = lineage.referenced_base_field_ids.as_slice() {
        let mut matches = base_contracts(contract)
            .into_iter()
            .filter_map(|base| {
                base.schema_at_create
                    .fields
                    .iter()
                    .find(|field| field.field_id == *field_id)
                    .map(|field| field.type_signature.as_str())
            })
            .collect::<Vec<_>>();
        matches.sort_unstable();
        matches.dedup();
        match matches.as_slice() {
            [type_signature] => {
                return arrow_type_from_contract_signature(type_signature).map(Some);
            }
            [] => {
                return Err(format!(
                    "aggregate MV contract references unknown base field id {field_id}"
                ));
            }
            _ => {
                return Err(format!(
                    "aggregate MV contract base field id {field_id} is ambiguous across join inputs"
                ));
            }
        }
    }

    Ok(None)
}

fn base_contracts(
    contract: &MvSchemaContract,
) -> Vec<&crate::meta::repository::mv_contract::BaseContract> {
    if contract.bases.is_empty() {
        vec![&contract.base]
    } else {
        contract.bases.iter().collect()
    }
}

fn sql_data_type_to_arrow(data_type: &sqlparser::ast::DataType) -> Result<DataType, String> {
    use sqlparser::ast as sqlast;

    Ok(match data_type {
        sqlast::DataType::TinyInt(_) => DataType::Int8,
        sqlast::DataType::SmallInt(_) => DataType::Int16,
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => DataType::Int32,
        sqlast::DataType::BigInt(_) => DataType::Int64,
        sqlast::DataType::Float(_) => DataType::Float32,
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => DataType::Float64,
        sqlast::DataType::Boolean => DataType::Boolean,
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text
        | sqlast::DataType::Char(_)
        | sqlast::DataType::Character(_)
        | sqlast::DataType::String(_) => DataType::Utf8,
        sqlast::DataType::Varbinary(_) | sqlast::DataType::Binary(_) => DataType::Binary,
        sqlast::DataType::Date => DataType::Date32,
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                DataType::Decimal128(*p as u8, *s as i8)
            }
            sqlast::ExactNumberInfo::Precision(p) => DataType::Decimal128(*p as u8, 0),
            sqlast::ExactNumberInfo::None => DataType::Decimal128(38, 0),
        },
        other => {
            return Err(format!(
                "aggregate MV explicit cast input type is unsupported: {other}"
            ));
        }
    })
}

fn arrow_type_from_contract_signature(type_signature: &str) -> Result<DataType, String> {
    let trimmed = type_signature.trim();
    let lower = trimmed.to_ascii_lowercase();
    Ok(match lower.as_str() {
        "boolean" | "bool" => DataType::Boolean,
        "tinyint" => DataType::Int8,
        "smallint" => DataType::Int16,
        "int" | "integer" => DataType::Int32,
        "long" | "bigint" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "date" => DataType::Date32,
        "timestamp" | "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "string" | "varchar" | "char" => DataType::Utf8,
        "binary" | "varbinary" => DataType::Binary,
        _ if lower.starts_with("decimal(") => {
            let inner = trimmed
                .strip_prefix("decimal(")
                .or_else(|| trimmed.strip_prefix("DECIMAL("))
                .and_then(|value| value.strip_suffix(')'))
                .ok_or_else(|| format!("invalid decimal type signature `{type_signature}`"))?;
            let mut parts = inner.split(',').map(str::trim);
            let precision = parts
                .next()
                .and_then(|value| value.parse::<u8>().ok())
                .ok_or_else(|| format!("invalid decimal precision in `{type_signature}`"))?;
            let scale = parts
                .next()
                .and_then(|value| value.parse::<i8>().ok())
                .ok_or_else(|| format!("invalid decimal scale in `{type_signature}`"))?;
            DataType::Decimal128(precision, scale)
        }
        _ => {
            return Err(format!(
                "aggregate MV contract input type is unsupported: {type_signature}"
            ));
        }
    })
}

impl IcebergMvRefreshContext {
    /// Build the full refresh context from raw inputs. Extracts target
    /// snapshot id / uuid / schema from `target_table.metadata()` and forwards
    /// the rest to `IcebergMvRewriteContext::from_parts`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
    ) -> Result<Self, String> {
        Self::new_with_pruning_limits(
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            iceberg_catalogs,
            target_entry,
            iceberg_catalog,
            target_table,
            MvRefreshPruningLimits::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_pruning_limits(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
        pruning_limits: MvRefreshPruningLimits,
    ) -> Result<Self, String> {
        Self::new_with_affected_partitions_and_pruning_limits(
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            iceberg_catalogs,
            target_entry,
            iceberg_catalog,
            target_table,
            crate::engine::mv::partition::AffectedTargetPartitions::not_derived(
                "refresh context was constructed without planned affected partitions",
            ),
            pruning_limits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_affected_partitions(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
        affected_partitions: crate::engine::mv::partition::AffectedTargetPartitions,
    ) -> Result<Self, String> {
        Self::new_with_affected_partitions_and_pruning_limits(
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            iceberg_catalogs,
            target_entry,
            iceberg_catalog,
            target_table,
            affected_partitions,
            MvRefreshPruningLimits::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_affected_partitions_and_pruning_limits(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[IcebergTableRef]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
        affected_partitions: crate::engine::mv::partition::AffectedTargetPartitions,
        pruning_limits: MvRefreshPruningLimits,
    ) -> Result<Self, String> {
        let metadata = target_table.metadata();
        let target_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id());
        let target_table_uuid = metadata.uuid().to_string();
        let target_schema = metadata.current_schema().clone();
        let schema_contract = mv_definition.schema_contract.clone().map(Arc::new);

        let rewrite = IcebergMvRewriteContext::from_parts(
            target,
            mv_id,
            current_catalog.map(str::to_string),
            current_database.to_string(),
            mv_definition,
            canonical_select_query,
            base_refs.clone(),
            pin,
            target_snapshot_id,
            target_table_uuid,
            target_schema,
            schema_contract,
        )?;
        let base_catalog_entries = collect_base_catalog_entries(iceberg_catalogs, &base_refs)?;

        Ok(Self {
            rewrite: Arc::new(rewrite),
            target_entry,
            base_catalog_entries,
            iceberg_catalog,
            target_table,
            affected_partitions,
            pruning_limits,
        })
    }

    pub(crate) fn affected_partitions_to_target_partition_filter(
        &self,
    ) -> crate::engine::mv::partition::TargetPartitionFilter {
        match &self.affected_partitions {
            crate::engine::mv::partition::AffectedTargetPartitions::Known { partitions } => {
                if self
                    .pruning_limits
                    .affected_partition_count_exceeds_limit(partitions.len())
                {
                    tracing::warn!(
                        target = ?self.rewrite.target,
                        affected_partition_count = partitions.len(),
                        max_affected_partitions = self.pruning_limits.max_affected_partitions,
                        fallback_reason = "affected_partition_threshold",
                        "falling back to unpartitioned target scan because affected partition allow-list exceeds configured threshold"
                    );
                    crate::engine::mv::partition::TargetPartitionFilter::None
                } else {
                    crate::engine::mv::partition::TargetPartitionFilter::AllowList(
                        partitions.clone(),
                    )
                }
            }
            crate::engine::mv::partition::AffectedTargetPartitions::Unpartitioned
            | crate::engine::mv::partition::AffectedTargetPartitions::NotDerived { .. } => {
                crate::engine::mv::partition::TargetPartitionFilter::None
            }
        }
    }

    pub(crate) fn version_scan_source(
        &self,
        table: &IcebergTableInfo,
        snapshot_id: i64,
    ) -> Result<ScanSource, String> {
        let entry = self.base_catalog_entry_for_version_scan(&table.catalog)?;
        let ident =
            iceberg::TableIdent::from_strs([table.namespace.as_str(), table.table.as_str()])
                .map_err(|e| {
                    format!(
                        "build iceberg table ident for version scan {}.{}.{}: {e}",
                        table.catalog, table.namespace, table.table
                    )
                })?;
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(entry)
            .map_err(|e| {
                format!(
                    "build iceberg catalog for version scan {}.{}.{}: {e}",
                    table.catalog, table.namespace, table.table
                )
            })?;
        let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            catalog.load_table(&ident).await
        })
        .map_err(|e| format!("load iceberg table for version scan runtime failed: {e}"))?
        .map_err(|e| {
            format!(
                "load iceberg table for version scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
        let files = data_files_at_snapshot(&loaded, snapshot_id)?;
        Ok(ScanSource::IcebergDataFiles {
            table: table.clone(),
            files,
            cloud_properties: entry.cloud_properties_map(),
            binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
        })
    }

    fn base_catalog_entry_for_version_scan(
        &self,
        catalog: &str,
    ) -> Result<&IcebergCatalogEntry, String> {
        let key = crate::engine::catalog::normalize_identifier(catalog)?;
        self.base_catalog_entries.get(&key).ok_or_else(|| {
            format!("Iceberg version scan requires base catalog {catalog} in MV refresh context")
        })
    }

    pub(crate) fn target_state_scan_source(
        &self,
        scan: &IcebergMvTargetStateScan,
    ) -> Result<ScanSource, String> {
        let target = &self.rewrite.target;
        if !scan.catalog.eq_ignore_ascii_case(&target.catalog)
            || !scan.database.eq_ignore_ascii_case(&target.namespace)
            || !scan.table.eq_ignore_ascii_case(&target.table)
        {
            return Err(format!(
                "Iceberg target-state scan {} does not match MV refresh target {}.{}.{}",
                scan.fqn(),
                target.catalog,
                target.namespace,
                target.table
            ));
        }
        if scan.target_table_uuid != self.rewrite.target_table_uuid {
            return Err(format!(
                "Iceberg target-state scan {} target uuid mismatch: scan={} context={}",
                scan.fqn(),
                scan.target_table_uuid,
                self.rewrite.target_table_uuid
            ));
        }
        if scan.target_snapshot_id != self.rewrite.target_snapshot_id {
            return Err(format!(
                "Iceberg target-state scan {} target snapshot mismatch: scan={:?} context={:?}",
                scan.fqn(),
                scan.target_snapshot_id,
                self.rewrite.target_snapshot_id
            ));
        }
        let target_partition_allow_list = self.target_state_partition_allow_list(scan)?;
        let aggregate_contract =
            self.rewrite
                .schema_contract
                .aggregate
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "Iceberg target-state scan {} requires aggregate state contract",
                        scan.fqn()
                    )
                })?;
        if scan.aggregate_state_layout_version != aggregate_contract.state_layout_version {
            return Err(format!(
                "Iceberg target-state scan {} aggregate layout version mismatch: scan={} contract={}",
                scan.fqn(),
                scan.aggregate_state_layout_version,
                aggregate_contract.state_layout_version
            ));
        }
        match &scan.row_filter {
            crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope,
            } if row_id_column_name.eq_ignore_ascii_case(&scan.row_id_column_name) => {
                validate_target_state_branch_scope(
                    scan,
                    branch_scope.as_ref(),
                    &self.rewrite.schema_contract,
                )?;
            }
            crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                ..
            } => {
                return Err(format!(
                    "Iceberg target-state scan {} row filter column mismatch: filter={} scan={}",
                    scan.fqn(),
                    row_id_column_name,
                    scan.row_id_column_name
                ));
            }
        }
        let (_, layout) = self.rewrite.aggregate_shape_and_layout_for_execution()?;
        let expected_physical_columns = layout
            .physical_columns
            .iter()
            .map(|column| column.column.name.clone())
            .collect::<Vec<_>>();
        if scan.physical_column_names != expected_physical_columns {
            return Err(format!(
                "Iceberg target-state scan {} physical column mismatch: scan={:?} expected={:?}",
                scan.fqn(),
                scan.physical_column_names,
                expected_physical_columns
            ));
        }

        let files = match self.rewrite.target_snapshot_id {
            Some(snapshot_id) => data_files_at_snapshot(&self.target_table, snapshot_id)?,
            None => Vec::new(),
        };
        let files = if let Some(allow_list) = target_partition_allow_list {
            filter_target_state_files_by_partition(
                self.rewrite.schema_contract.as_ref(),
                &allow_list,
                files,
                scan,
            )?
        } else {
            files
        };
        Ok(ScanSource::IcebergDataFiles {
            table: target_table_info(self, scan)?,
            files,
            cloud_properties: self.target_entry.cloud_properties_map(),
            binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
        })
    }

    fn target_state_partition_allow_list(
        &self,
        scan: &IcebergMvTargetStateScan,
    ) -> Result<Option<BTreeSet<crate::engine::mv::partition::MvPartitionKey>>, String> {
        match scan.partition_constraint {
            crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned => {
                Ok(None)
            }
            crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                match &self.affected_partitions {
                    crate::engine::mv::partition::AffectedTargetPartitions::Unpartitioned => {
                        Ok(None)
                    }
                    crate::engine::mv::partition::AffectedTargetPartitions::Known {
                        partitions,
                    } => {
                        if self
                            .pruning_limits
                            .affected_partition_count_exceeds_limit(partitions.len())
                        {
                            tracing::warn!(
                                target = %scan.fqn(),
                                affected_partition_count = partitions.len(),
                                max_affected_partitions =
                                    self.pruning_limits.max_affected_partitions,
                                fallback_reason = "affected_partition_threshold",
                                "falling back to full target-state scan because affected partition allow-list exceeds configured threshold"
                            );
                            Ok(None)
                        } else {
                            Ok(Some(partitions.clone()))
                        }
                    }
                    crate::engine::mv::partition::AffectedTargetPartitions::NotDerived {
                        reason,
                    } => {
                        tracing::warn!(
                            target = %scan.fqn(),
                            reason = %reason,
                            "falling back to full target-state scan because affected partition planning is unknown"
                        );
                        Ok(None)
                    }
                }
            }
        }
    }
}

pub(crate) fn bind_target_state_file_positions(
    mut source: ScanSource,
    matched_positions: &[crate::engine::mv::iceberg_target_apply::TargetRowPositionSet],
    target: &str,
) -> Result<ScanSource, String> {
    let ScanSource::IcebergDataFiles { files, .. } = &mut source else {
        return Err(format!(
            "Iceberg target-state position binding for {target} requires IcebergDataFiles source"
        ));
    };

    if matched_positions.is_empty() {
        files.clear();
        return Ok(source);
    }

    let mut by_file = BTreeMap::<String, Vec<i64>>::new();
    for set in matched_positions {
        if set.positions.is_empty() {
            continue;
        }
        by_file
            .entry(set.referenced_data_file.clone())
            .or_default()
            .extend(set.positions.iter().copied());
    }
    for positions in by_file.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    if by_file.is_empty() {
        files.clear();
        return Ok(source);
    }

    let mut bound_files = Vec::new();
    for mut file in std::mem::take(files) {
        if let Some(positions) = by_file.remove(&file.path) {
            file.included_positions = Some(positions);
            bound_files.push(file);
        }
    }
    if !by_file.is_empty() {
        let missing = by_file.keys().cloned().collect::<Vec<_>>().join(", ");
        return Err(format!(
            "Iceberg target-state scan {target} locator returned positions for files not present in scan source: [{missing}]"
        ));
    }
    *files = bound_files;
    Ok(source)
}

fn validate_target_state_branch_scope(
    scan: &IcebergMvTargetStateScan,
    scope: Option<&crate::sql::catalog::BranchScope>,
    contract: &MvSchemaContract,
) -> Result<(), String> {
    let Some(scope) = scope else {
        return Ok(());
    };
    let branch = contract.branch.as_ref().ok_or_else(|| {
        format!(
            "Iceberg target-state scan {} has branch scope but schema contract has no branch contract",
            scan.fqn()
        )
    })?;
    if !scope
        .branch_id_column_name
        .eq_ignore_ascii_case(&branch.branch_id_column.column_name)
    {
        return Err(format!(
            "Iceberg target-state scan {} branch column mismatch: scope={} contract={}",
            scan.fqn(),
            scope.branch_id_column_name,
            branch.branch_id_column.column_name
        ));
    }
    if scope.branch_id < 0 || scope.branch_id as u32 >= branch.branch_count {
        return Err(format!(
            "Iceberg target-state scan {} branch id {} out of range 0..{}",
            scan.fqn(),
            scope.branch_id,
            branch.branch_count
        ));
    }
    Ok(())
}

fn data_files_at_snapshot(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
        table,
        snapshot_id,
    )
    .map(|files| {
        files
            .into_iter()
            .map(data_file_with_stats_to_info)
            .collect()
    })
}

fn filter_target_state_files_by_partition(
    contract: &MvSchemaContract,
    allow_list: &BTreeSet<crate::engine::mv::partition::MvPartitionKey>,
    files: Vec<IcebergDataFileInfo>,
    scan: &IcebergMvTargetStateScan,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    if allow_list.is_empty() {
        return Ok(Vec::new());
    }
    files
        .into_iter()
        .filter_map(|file| match target_file_partition_key(contract, &file) {
            Ok(Some(key)) if allow_list.contains(&key) => Some(Ok(file)),
            Ok(Some(_)) => None,
            Ok(None) => Some(Err(format!(
                "Iceberg target-state scan {} requires partition keys for target files",
                scan.fqn()
            ))),
            Err(err) => Some(Err(format!(
                "Iceberg target-state scan {} cannot map target file {} partition: {}",
                scan.fqn(),
                file.path,
                err
            ))),
        })
        .collect()
}

fn target_file_partition_key(
    contract: &MvSchemaContract,
    file: &IcebergDataFileInfo,
) -> Result<Option<crate::engine::mv::partition::MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };
    let Some(spec_id) = file.partition_spec_id else {
        return Err(format!(
            "target file {} is missing partition spec id",
            file.path
        ));
    };
    let mut fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let expected_transform = target_contract_transform_text(&partition_field.transform)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} uses unsupported void transform",
                    partition_field.partition_field_name
                )
            })?;
        let value = file
            .partition_values
            .iter()
            .find(|value| {
                value
                    .source_column
                    .eq_ignore_ascii_case(&partition_field.source_column_name)
                    && value.transform.eq_ignore_ascii_case(&expected_transform)
            })
            .or_else(|| {
                file.partition_values.iter().find(|value| {
                    value
                        .field_name
                        .eq_ignore_ascii_case(&partition_field.partition_field_name)
                        && value.transform.eq_ignore_ascii_case(&expected_transform)
                })
            })
            .ok_or_else(|| {
                format!(
                    "target file {} has no partition value for {} with transform {}",
                    file.path, partition_field.partition_field_name, expected_transform
                )
            })?;
        fields.push(crate::engine::mv::partition::MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            target_partition_value_to_mv_value(value)?,
        ));
    }

    Ok(Some(crate::engine::mv::partition::MvPartitionKey::new(
        spec_id, fields,
    )))
}

fn target_partition_value_to_mv_value(
    value: &IcebergPartitionFieldValue,
) -> Result<crate::engine::mv::partition::MvPartitionValue, String> {
    match &value.value {
        None => Ok(crate::engine::mv::partition::MvPartitionValue::Null),
        Some(IcebergPartitionValue::Boolean(v)) => Ok(
            crate::engine::mv::partition::MvPartitionValue::String(v.to_string()),
        ),
        Some(IcebergPartitionValue::Int32(v)) => Ok(
            crate::engine::mv::partition::MvPartitionValue::String(v.to_string()),
        ),
        Some(IcebergPartitionValue::Int64(v)) => Ok(
            crate::engine::mv::partition::MvPartitionValue::String(v.to_string()),
        ),
        Some(IcebergPartitionValue::Float(v)) => Ok(
            crate::engine::mv::partition::MvPartitionValue::String(v.to_string()),
        ),
        Some(IcebergPartitionValue::Double(v)) => Ok(
            crate::engine::mv::partition::MvPartitionValue::String(v.to_string()),
        ),
        Some(IcebergPartitionValue::String(v)) => Ok(
            crate::engine::mv::partition::MvPartitionValue::String(v.clone()),
        ),
        Some(IcebergPartitionValue::Binary(_)) => Err(format!(
            "target partition field {} has unsupported binary value",
            value.field_name
        )),
    }
}

fn target_contract_transform_text(
    transform: &crate::meta::repository::mv_contract::MvPartitionTransformContract,
) -> Option<String> {
    match transform {
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Identity => {
            Some("identity".to_string())
        }
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Year => {
            Some("year".to_string())
        }
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Month => {
            Some("month".to_string())
        }
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Day => {
            Some("day".to_string())
        }
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Hour => {
            Some("hour".to_string())
        }
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Bucket {
            num_buckets,
        } => Some(format!("bucket({num_buckets})")),
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Truncate { width } => {
            Some(format!("truncate({width})"))
        }
        crate::meta::repository::mv_contract::MvPartitionTransformContract::Void => None,
    }
}

fn collect_base_catalog_entries(
    iceberg_catalogs: &IcebergCatalogRegistry,
    base_refs: &[IcebergTableRef],
) -> Result<BTreeMap<String, IcebergCatalogEntry>, String> {
    let mut entries = BTreeMap::new();
    for base_ref in base_refs {
        let key = crate::engine::catalog::normalize_identifier(&base_ref.catalog)?;
        if entries.contains_key(&key) {
            continue;
        }
        let entry = iceberg_catalogs.get(&base_ref.catalog).map_err(|e| {
            format!(
                "collect iceberg MV refresh base catalog {} for {}: {e}",
                base_ref.catalog,
                base_ref.fqn()
            )
        })?;
        entries.insert(key, entry);
    }
    Ok(entries)
}

fn data_file_with_stats_to_info(
    file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        included_positions: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn target_table_info(
    ctx: &IcebergMvRefreshContext,
    scan: &IcebergMvTargetStateScan,
) -> Result<IcebergTableInfo, String> {
    let metadata = ctx.target_table.metadata();
    Ok(IcebergTableInfo {
        catalog: scan.catalog.clone(),
        namespace: scan.database.clone(),
        table: scan.table.clone(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema_def(metadata.current_schema()),
        serialized_metadata: Some(
            serde_json::to_string(metadata)
                .map_err(|err| format!("serialize iceberg target table metadata failed: {err}"))?,
        ),
        serialized_metadata_rows: None,
    })
}

fn iceberg_schema_def(schema: &iceberg::spec::Schema) -> IcebergSchemaDef {
    IcebergSchemaDef {
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
    }
}

fn iceberg_field_def(field: &iceberg::spec::NestedField) -> IcebergSchemaFieldDef {
    let initial_default_json = field.initial_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        initial_default_json,
        children: iceberg_type_children(field.field_type.as_ref()),
    }
}

fn iceberg_type_children(ty: &iceberg::spec::Type) -> Vec<IcebergSchemaFieldDef> {
    match ty {
        iceberg::spec::Type::Struct(struct_ty) => struct_ty
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
        iceberg::spec::Type::List(list_ty) => {
            vec![iceberg_field_def(list_ty.element_field.as_ref())]
        }
        iceberg::spec::Type::Map(map_ty) => vec![
            iceberg_field_def(map_ty.key_field.as_ref()),
            iceberg_field_def(map_ty.value_field.as_ref()),
        ],
        iceberg::spec::Type::Primitive(_) => vec![],
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::connector::starrocks::table::model::IcebergTableRef;
    use crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin;
    use crate::meta::repository::mv::StoredMvDefinition;
    use crate::meta::repository::mv_contract::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, MvSchemaContract, OutputColumnLineage,
        OutputContract, TargetContract, TargetVisibleColumn,
    };

    use super::*;

    pub(crate) fn make_ref(c: &str, n: &str, t: &str) -> IcebergTableRef {
        IcebergTableRef {
            catalog: c.to_string(),
            namespace: n.to_string(),
            table: t.to_string(),
        }
    }

    pub(crate) fn make_pin(entries: &[(&str, i64, &str)]) -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(entries)
    }

    pub(crate) fn make_target_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        )
    }

    pub(crate) fn make_schema_contract() -> MvSchemaContract {
        MvSchemaContract {
            contract_version: 3,
            base: BaseContract {
                table_fqn: "ice.db.b".to_string(),
                table_uuid: "uuid-b".to_string(),
                alias_at_create: None,
                schema_id_at_create: 0,
                schema_at_create: BaseSchemaSnapshot {
                    fields: vec![
                        BaseFieldRecord {
                            field_id: 1,
                            name_at_create: "k".to_string(),
                            type_signature: "long".to_string(),
                            required: true,
                        },
                        BaseFieldRecord {
                            field_id: 2,
                            name_at_create: "v".to_string(),
                            type_signature: "long".to_string(),
                            required: false,
                        },
                    ],
                },
            },
            bases: Vec::new(),
            output: OutputContract {
                columns: vec![
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Column,
                            referenced_base_field_ids: vec![1],
                            referenced_base_fields: Vec::new(),
                        },
                    },
                    OutputColumnLineage {
                        expression: ExpressionLineage {
                            kind: ExpressionKind::Column,
                            referenced_base_field_ids: vec![2],
                            referenced_base_fields: Vec::new(),
                        },
                    },
                ],
                filter: None,
            },
            join: None,
            aggregate: None,
            branch: None,
            target: TargetContract {
                table_fqn: "tgt.db.mv".to_string(),
                table_uuid: "uuid-tgt".to_string(),
                schema_id_at_create: 7,
                visible_columns: vec![
                    TargetVisibleColumn {
                        output_name: "k".to_string(),
                        target_field_id: 100,
                        type_signature: "long".to_string(),
                        nullable: false,
                    },
                    TargetVisibleColumn {
                        output_name: "v".to_string(),
                        target_field_id: 101,
                        type_signature: "long".to_string(),
                        nullable: true,
                    },
                ],
                hidden_apply_key: HiddenApplyKeyContract {
                    column_name: "k".to_string(),
                    target_field_id: 100,
                    source: ApplyKeySource::BaseRowId,
                },
                partition: None,
            },
        }
    }

    pub(crate) fn make_mv_definition() -> StoredMvDefinition {
        StoredMvDefinition {
            mv_id: 42,
            select_sql: "SELECT k, v FROM ice.db.b".to_string(),
            base_table_refs: vec!["ice.db.b".to_string()],
            primary_key_columns: vec!["k".to_string()],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("tgt".to_string()),
            target_namespace: Some("db".to_string()),
            target_table: Some("mv".to_string()),
            schema_contract: Some(make_schema_contract()),
            partition_spec: None,
            partition_state_complete: false,
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: [("ice.db.b".to_string(), 11i64)].into_iter().collect(),
            last_refresh_table_uuids: [("ice.db.b".to_string(), "uuid-b".to_string())]
                .into_iter()
                .collect(),
            last_refreshed_iceberg_snapshot_id: Some(99),
            refresh_in_progress: false,
            active_refresh_id: None,
            refresh_target_snapshots: Default::default(),
            refresh_policy: Default::default(),
            refresh_paused: false,
            refresh_interval_ms: None,
            max_staleness_ms: None,
            last_scheduler_error: None,
            next_refresh_after_ms: None,
            created_at_ms: 0,
        }
    }

    pub(crate) fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let dialect = sqlparser::dialect::GenericDialect {};
        let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql).expect("parse_sql");
        match statements.into_iter().next().expect("one statement") {
            sqlparser::ast::Statement::Query(q) => *q,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    pub(crate) fn make_target() -> IcebergMvTarget {
        IcebergMvTarget {
            catalog: "tgt".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        }
    }

    /// Returns a minimally-valid `Arc<IcebergMvRewriteContext>` for use in
    /// unit tests outside this module (e.g. `imv/entrypoint.rs`).
    pub(crate) fn dummy_rewrite_context() -> Arc<IcebergMvRewriteContext> {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        Arc::new(
            IcebergMvRewriteContext::from_parts(
                target,
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                mv_def,
                query,
                base_refs,
                pin,
                Some(99),
                "uuid-tgt".to_string(),
                schema,
                Some(contract),
            )
            .expect("dummy_rewrite_context: from_parts must succeed on canonical fixture"),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::connector::starrocks::table::model::IcebergTableRef;
    use crate::connector::starrocks::table::refresh_pin::RefreshSnapshotPin;
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BRANCH_ID_COLUMN_NAME, BranchIdColumnContract, BranchUnionContract,
    };

    use super::tests_support::*;
    use super::*;

    #[test]
    fn from_parts_happy_path_derives_all_fields() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_parts(
            target.clone(),
            42,
            Some("sess_cat".to_string()),
            "sess_db".to_string(),
            mv_def.clone(),
            query.clone(),
            base_refs.clone(),
            pin.clone(),
            Some(99),
            "uuid-tgt".to_string(),
            schema.clone(),
            Some(contract.clone()),
        )
        .expect("constructor should succeed on happy path");

        assert_eq!(ctx.target.table, "mv");
        assert_eq!(ctx.mv_id, 42);
        assert_eq!(ctx.current_catalog.as_deref(), Some("sess_cat"));
        assert_eq!(ctx.current_database, "sess_db");
        assert!(Arc::ptr_eq(&ctx.mv_definition, &mv_def));
        assert!(Arc::ptr_eq(&ctx.canonical_select_query, &query));
        assert_eq!(ctx.base_refs.len(), 1);
        assert!(Arc::ptr_eq(&ctx.base_refs, &base_refs));
        assert!(Arc::ptr_eq(&ctx.pin, &pin));
        assert_eq!(ctx.previous_snapshot_ids.get("ice.db.b"), Some(&11));
        assert_eq!(
            ctx.previous_table_uuids.get("ice.db.b").map(String::as_str),
            Some("uuid-b")
        );
        assert_eq!(ctx.target_snapshot_id, Some(99));
        assert_eq!(ctx.target_table_uuid, "uuid-tgt");
        assert!(Arc::ptr_eq(&ctx.target_schema, &schema));
        assert!(Arc::ptr_eq(&ctx.schema_contract, &contract));
    }

    #[test]
    fn from_parts_rejects_missing_schema_contract() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();

        let err_msg = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            None,
        )
        .expect_err("missing schema contract must fail");
        assert!(
            err_msg.contains("missing schema contract on target tgt.db.mv"),
            "got: {err_msg}"
        );
    }

    #[test]
    fn from_parts_rejects_empty_base_refs() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(Vec::<IcebergTableRef>::new());
        let pin = Arc::new(RefreshSnapshotPin::default());
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("empty base_refs must fail");
        assert!(err.contains("no base table refs"), "got: {err}");
    }

    #[test]
    fn from_parts_rejects_pin_coverage_mismatch() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> =
            Arc::from(vec![make_ref("ice", "db", "b"), make_ref("ice", "db", "c")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("pin coverage mismatch must fail");
        assert!(err.contains("refresh pin covers"), "got: {err}");
    }

    #[test]
    fn from_parts_rejects_pin_missing_uuid() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        // Pin has the right count but the entry is for a different fqn.
        let pin = Arc::new(make_pin(&[("ice.db.OTHER", 22, "uuid-x")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("missing pin uuid must fail");
        assert!(
            err.contains("refresh pin missing uuid for base"),
            "got: {err}"
        );
    }

    #[test]
    fn from_parts_rejects_base_identity_drift() {
        let target = make_target();
        let mut def = make_mv_definition();
        def.last_refresh_table_uuids
            .insert("ice.db.b".to_string(), "uuid-OLD".to_string());
        let mv_def = Arc::new(def);
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-NEW")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("identity drift must fail");
        assert!(err.contains("base table identity changed"), "got: {err}");
    }

    #[test]
    fn from_parts_first_refresh_passes_with_empty_previous() {
        let target = make_target();
        let mut def = make_mv_definition();
        def.last_refresh_snapshots.clear();
        def.last_refresh_table_uuids.clear();
        let mv_def = Arc::new(def);
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("first refresh must succeed");
        assert!(ctx.previous_snapshot_ids.is_empty());
        assert!(ctx.previous_table_uuids.is_empty());
    }

    #[test]
    fn from_parts_rejects_target_schema_contract_field_mismatch() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let mut contract = make_schema_contract();
        contract.target.visible_columns.pop();
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("schema/contract mismatch must fail");
        assert!(
            err.contains("target schema/contract field id mismatch"),
            "got: {err}"
        );
    }

    #[test]
    fn from_parts_rejects_target_schema_contract_field_ids_differ_same_count() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        // Contract has two columns (matching schema count) but one has a wrong
        // target_field_id (schema has 100/101; contract claims 100/999).
        let mut contract = make_schema_contract();
        contract.target.visible_columns[1].target_field_id = 999;
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("field-id set mismatch must fail even when counts match");
        assert!(
            err.contains("target schema/contract field id mismatch"),
            "got: {err}"
        );
    }

    #[test]
    fn summary_orders_by_base_refs_declared_order() {
        let target = make_target();
        let query = Arc::new(parse_query("SELECT k FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![
            make_ref("ice", "db", "b"),
            make_ref("ice", "db", "a"),
            make_ref("ice", "db", "c"),
        ]);
        let pin = Arc::new(make_pin(&[
            // Insert in NON-declared order to confirm summary reorders.
            ("ice.db.a", 30, "uuid-a"),
            ("ice.db.c", 50, "uuid-c"),
            ("ice.db.b", 20, "uuid-b"),
        ]));
        let schema = make_target_schema();
        let mut def_for_three_bases = make_mv_definition();
        def_for_three_bases.last_refresh_snapshots.clear();
        def_for_three_bases
            .last_refresh_snapshots
            .insert("ice.db.b".to_string(), 11);
        def_for_three_bases.last_refresh_table_uuids.clear();
        def_for_three_bases
            .last_refresh_table_uuids
            .insert("ice.db.b".to_string(), "uuid-b".to_string());
        def_for_three_bases
            .last_refresh_table_uuids
            .insert("ice.db.a".to_string(), "uuid-a".to_string());
        def_for_three_bases
            .last_refresh_table_uuids
            .insert("ice.db.c".to_string(), "uuid-c".to_string());
        let mv_def = Arc::new(def_for_three_bases);
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx happy path");

        let summary = ctx.summary();
        assert_eq!(
            summary.base_fqns,
            vec![
                "ice.db.b".to_string(),
                "ice.db.a".to_string(),
                "ice.db.c".to_string()
            ],
            "base_fqns must use base_refs declared order"
        );
        assert_eq!(
            summary.pinned_snapshots,
            vec![
                ("ice.db.b".to_string(), 20),
                ("ice.db.a".to_string(), 30),
                ("ice.db.c".to_string(), 50),
            ],
            "summary must use base_refs declared order, not BTreeMap key order"
        );
        assert_eq!(
            summary.previous_snapshots,
            vec![
                ("ice.db.b".to_string(), Some(11)),
                ("ice.db.a".to_string(), None),
                ("ice.db.c".to_string(), None),
            ]
        );
    }

    #[test]
    fn from_parts_rejects_apply_key_not_in_target_schema() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "nonexistent".to_string();
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect_err("apply-key absence must fail");
        assert!(err.contains("apply-key column"), "got: {err}");
    }

    #[test]
    fn from_parts_succeeds_with_distinct_hidden_apply_key_field() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));

        // Three-field target schema: 100=k (visible), 101=v (visible),
        // 999=__nova_apply_key (hidden — present in schema but NOT in
        // visible_columns).
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__nova_apply_key",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );

        // Contract: visible columns are 100/101; hidden apply key is 999.
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__nova_apply_key".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        let contract = Arc::new(contract);

        IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx must succeed when apply-key is a distinct hidden schema field");
    }

    #[test]
    fn from_parts_succeeds_with_branch_id_field_in_schema() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));

        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__nova_apply_key",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        4242,
                        BRANCH_ID_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::Int),
                    )),
                ])
                .build()
                .expect("build schema"),
        );

        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__nova_apply_key".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.branch = Some(BranchUnionContract {
            branch_id_column: BranchIdColumnContract {
                column_name: BRANCH_ID_COLUMN_NAME.to_string(),
                target_field_id: 4242,
            },
            branch_count: 2,
            inner_apply_key_source: ApplyKeySource::BaseRowId,
        });
        let contract = Arc::new(contract);

        IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx must accept branch id field in target schema");
    }

    #[test]
    fn from_parts_succeeds_with_aggregate_state_columns_in_schema() {
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[IcebergTableRef]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));

        // Aggregate target schema: visible columns 100=k and 101=v, hidden
        // apply key 999=__row_id__, and aggregate-state columns
        // 200=__agg_state_c, 201=__agg_state_s. All must be accepted by
        // from_parts.
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_c",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        201,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()
                .expect("build schema"),
        );

        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![
                AggregateStateColumnContract {
                    column_name: "__agg_state_c".to_string(),
                    target_field_id: 200,
                    type_signature: "long".to_string(),
                    nullable: true,
                    role: AggregateStateRoleContract::Single,
                },
                AggregateStateColumnContract {
                    column_name: "__agg_state_s".to_string(),
                    target_field_id: 201,
                    type_signature: "long".to_string(),
                    nullable: true,
                    role: AggregateStateRoleContract::Single,
                },
            ],
        });
        let contract = Arc::new(contract);

        IcebergMvRewriteContext::from_parts(
            target,
            42,
            None,
            "db".to_string(),
            mv_def,
            query,
            base_refs,
            pin,
            Some(99),
            "uuid-tgt".to_string(),
            schema,
            Some(contract),
        )
        .expect("ctx must accept aggregate state columns in target schema");
    }

    #[test]
    fn version_scan_source_does_not_reject_base_catalog_that_differs_from_target() {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::{CatalogBuilder, NamespaceIdent, TableIdent};

        let warehouse = format!(
            "memory://novarocks-version-scan-base-catalog-test-{}",
            uuid::Uuid::new_v4()
        );
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            runtime
                .block_on(MemoryCatalogBuilder::default().load(
                    "memory",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.clone(),
                    )]),
                ))
                .expect("memory catalog"),
        );

        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "memory".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("catalog entry"),
        );
        let base_warehouse = std::env::temp_dir()
            .join(format!(
                "novarocks-version-scan-base-catalog-test-{}",
                uuid::Uuid::new_v4()
            ))
            .to_string_lossy()
            .into_owned();
        let base_entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), base_warehouse),
            ],
        )
        .expect("base catalog entry");
        let base_catalog_entries = [("ice".to_string(), base_entry)].into_iter().collect();
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    100,
                    "k",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    101,
                    "v",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("schema");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            "memory://target/table".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("target table");
        let ctx = IcebergMvRefreshContext {
            rewrite: dummy_rewrite_context(),
            target_entry,
            base_catalog_entries,
            iceberg_catalog,
            target_table,
            affected_partitions:
                crate::engine::mv::partition::AffectedTargetPartitions::not_derived("test context"),
            pruning_limits: MvRefreshPruningLimits::default(),
        };
        let table = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "missing_base".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: String::new(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };

        let err = ctx
            .version_scan_source(&table, 123)
            .expect_err("missing base table should fail after catalog resolution");
        assert!(
            !err.contains("requires catalog ice in MV refresh context, got tgt"),
            "version scan must resolve by base catalog, got: {err}"
        );
    }

    #[test]
    fn collect_base_catalog_entries_preserves_base_catalog_cloud_properties() {
        let mut registry = IcebergCatalogRegistry::default();
        let target_warehouse = std::env::temp_dir()
            .join(format!(
                "novarocks-version-scan-target-catalog-test-{}",
                uuid::Uuid::new_v4()
            ))
            .to_string_lossy()
            .into_owned();
        let base_warehouse = std::env::temp_dir()
            .join(format!(
                "novarocks-version-scan-base-entry-test-{}",
                uuid::Uuid::new_v4()
            ))
            .to_string_lossy()
            .into_owned();
        registry
            .create_catalog(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), target_warehouse),
                    ("aws.s3.endpoint".to_string(), "target-endpoint".to_string()),
                ],
            )
            .expect("target catalog");
        registry
            .create_catalog(
                "ice",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), base_warehouse),
                    ("aws.s3.endpoint".to_string(), "base-endpoint".to_string()),
                ],
            )
            .expect("base catalog");
        let base_refs = vec![make_ref("ice", "db", "b")];

        let entries = collect_base_catalog_entries(&registry, &base_refs).expect("entries");
        let cloud = entries
            .get("ice")
            .expect("base entry")
            .cloud_properties_map();

        assert_eq!(
            cloud.get("aws.s3.endpoint").map(String::as_str),
            Some("base-endpoint")
        );
    }

    fn target_state_source_for_binding_test() -> ScanSource {
        ScanSource::IcebergDataFiles {
            table: IcebergTableInfo {
                catalog: "tgt".to_string(),
                namespace: "db".to_string(),
                table: "mv".to_string(),
                table_uuid: Some("uuid-tgt".to_string()),
                current_snapshot_id: Some(99),
                schema_id: 1,
                location: "s3://bucket/mv".to_string(),
                schema: IcebergSchemaDef { fields: Vec::new() },
                serialized_metadata: None,
                serialized_metadata_rows: None,
            },
            files: vec![
                IcebergDataFileInfo {
                    path: "s3://bucket/mv/data-a.parquet".to_string(),
                    size: 10,
                    row_count: Some(10),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files: Vec::new(),
                    manifest_path: None,
                    partition_values: Vec::new(),
                },
                IcebergDataFileInfo {
                    path: "s3://bucket/mv/data-b.parquet".to_string(),
                    size: 20,
                    row_count: Some(20),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files: Vec::new(),
                    manifest_path: None,
                    partition_values: Vec::new(),
                },
            ],
            cloud_properties: BTreeMap::new(),
            binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
        }
    }

    #[test]
    fn bind_target_state_file_positions_keeps_only_matched_files() {
        let positions = vec![
            crate::engine::mv::iceberg_target_apply::TargetRowPositionSet {
                referenced_data_file: "s3://bucket/mv/data-b.parquet".to_string(),
                positions: vec![2, 8, 13],
            },
        ];

        let source = bind_target_state_file_positions(
            target_state_source_for_binding_test(),
            &positions,
            "tgt.db.mv",
        )
        .expect("bind positions");

        let ScanSource::IcebergDataFiles { files, .. } = source else {
            panic!("expected IcebergDataFiles");
        };
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "s3://bucket/mv/data-b.parquet");
        assert_eq!(files[0].included_positions, Some(vec![2, 8, 13]));
    }

    #[test]
    fn bind_target_state_file_positions_empty_matches_returns_empty_source() {
        let source = bind_target_state_file_positions(
            target_state_source_for_binding_test(),
            &[],
            "tgt.db.mv",
        )
        .expect("bind empty positions");

        let ScanSource::IcebergDataFiles { files, .. } = source else {
            panic!("expected IcebergDataFiles");
        };
        assert!(files.is_empty());
    }

    #[test]
    fn bind_target_state_file_positions_rejects_missing_files() {
        let positions = vec![
            crate::engine::mv::iceberg_target_apply::TargetRowPositionSet {
                referenced_data_file: "s3://bucket/mv/missing.parquet".to_string(),
                positions: vec![1],
            },
        ];

        let err = bind_target_state_file_positions(
            target_state_source_for_binding_test(),
            &positions,
            "tgt.db.mv",
        )
        .expect_err("missing target file should fail");

        assert!(err.contains("locator returned positions for files not present"));
        assert!(err.contains("s3://bucket/mv/missing.parquet"));
    }

    #[test]
    fn target_state_scan_falls_back_without_partition_allow_list() {
        use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
        use iceberg::{CatalogBuilder, NamespaceIdent, TableIdent};

        let warehouse = format!(
            "memory://novarocks-target-state-partition-contract-test-{}",
            uuid::Uuid::new_v4()
        );
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            runtime
                .block_on(MemoryCatalogBuilder::default().load(
                    "memory",
                    std::collections::HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.clone(),
                    )]),
                ))
                .expect("memory catalog"),
        );
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "memory".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("target entry"),
        );
        let schema = make_target_schema();
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            "memory://target/table".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_memory())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("target table");
        let mut ctx = IcebergMvRefreshContext {
            rewrite: dummy_rewrite_context(),
            target_entry,
            base_catalog_entries: BTreeMap::new(),
            iceberg_catalog,
            target_table,
            affected_partitions:
                crate::engine::mv::partition::AffectedTargetPartitions::not_derived("test context"),
            pruning_limits: MvRefreshPruningLimits {
                max_touched_groups: 100_000,
                max_affected_partitions: 2,
            },
        };
        let scan = IcebergMvTargetStateScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: "uuid-tgt".to_string(),
            target_snapshot_id: Some(99),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: Vec::new(),
            aggregate_state_names: Vec::new(),
            physical_column_names: Vec::new(),
            row_id_column_name: "__row_id__".to_string(),
            row_filter: crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint:
                crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired,
        };

        let unknown_filter = ctx
            .target_state_partition_allow_list(&scan)
            .expect("unknown affected partitions should fall back to full target scan");
        assert!(
            unknown_filter.is_none(),
            "unknown affected partitions should disable pruning"
        );

        let new_key = crate::engine::mv::partition::MvPartitionKey::new(1, Vec::new());
        let old_key = crate::engine::mv::partition::MvPartitionKey::new(2, Vec::new());
        ctx.affected_partitions = crate::engine::mv::partition::AffectedTargetPartitions::known([
            new_key.clone(),
            old_key.clone(),
        ]);
        let allow_list = ctx
            .target_state_partition_allow_list(&scan)
            .expect("known affected partitions should satisfy partition contract")
            .expect("partitioned scan should return an allow-list");
        assert!(allow_list.contains(&new_key));
        assert!(allow_list.contains(&old_key));

        ctx.pruning_limits.max_affected_partitions = 1;
        let threshold_filter = ctx
            .target_state_partition_allow_list(&scan)
            .expect("over-threshold affected partitions should fall back to full target scan");
        assert!(
            threshold_filter.is_none(),
            "over-threshold affected partitions should disable pruning"
        );
        assert_eq!(
            ctx.affected_partitions_to_target_partition_filter(),
            crate::engine::mv::partition::TargetPartitionFilter::None,
            "over-threshold affected partitions should disable merge-sink plan-time pruning"
        );
    }
}
