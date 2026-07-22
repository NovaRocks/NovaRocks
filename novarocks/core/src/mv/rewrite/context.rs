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

//! Immutable metadata for Iceberg MV rewrite planning.
//!
//! This canonical context owns only identities, persisted contracts, snapshot
//! pins, schemas, and derived aggregate layout. Concrete catalogs, tables,
//! scan binding, and refresh execution state remain in the engine adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use iceberg::spec::Schema;

use crate::mv::persistence::definition::StoredMvDefinition;
use crate::mv::persistence::schema as mv_schema;
use crate::mv::refresh::pin::RefreshSnapshotPin;
use mv_schema::MvSchemaContract;
use novarocks_catalog::identifier::TableIdentity;

/// Read-only metadata that drives Iceberg MV refresh rewrite.
///
/// Optimizer rewrite rules consume `Arc<IcebergMvRewriteContext>` without
/// depending on concrete execution handles owned by the engine adapter.
#[derive(Debug)]
pub(crate) struct IcebergMvRewriteContext {
    // ---- Identity ----
    pub target: TableIdentity,
    pub mv_id: i64,

    // ---- Session ----
    pub current_catalog: Option<String>,
    pub current_database: String,

    // ---- MV definition (post schema-contract rebind) ----
    pub mv_definition: Arc<StoredMvDefinition>,
    pub canonical_select_query: Arc<sqlparser::ast::Query>,

    // ---- Base table inputs ----
    pub base_refs: Arc<[TableIdentity]>,
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

/// Debug-only view of an `IcebergMvRewriteContext`. No `Display` impl — log
/// via `tracing::info!(summary = ?ctx.rewrite.summary(), ...)`.
#[derive(Debug)]
pub(crate) struct CtxSummary<'a> {
    pub target: &'a TableIdentity,
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
    /// Execution adapters use this internally after pulling
    /// `target_snapshot_id` / `target_table_uuid` / `target_schema` out of
    /// `target_table.metadata()`. Unit tests construct the rewrite layer
    /// directly via this helper without concrete execution handles.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        target: TableIdentity,
        mv_id: i64,
        current_catalog: Option<String>,
        current_database: String,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[TableIdentity]>,
        pin: Arc<RefreshSnapshotPin>,
        previous_snapshot_ids: BTreeMap<String, i64>,
        previous_table_uuids: BTreeMap<String, String>,
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

    /// Compatibility constructor for tests, EXPLAIN, and repartition paths
    /// that do not execute a validated refresh plan. Production refresh
    /// execution must call `from_parts` with its contract baseline instead.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_definition_parts(
        target: TableIdentity,
        mv_id: i64,
        current_catalog: Option<String>,
        current_database: String,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[TableIdentity]>,
        pin: Arc<RefreshSnapshotPin>,
        target_snapshot_id: Option<i64>,
        target_table_uuid: String,
        target_schema: Arc<Schema>,
        schema_contract: Option<Arc<MvSchemaContract>>,
    ) -> Result<Self, String> {
        let previous_snapshot_ids = mv_definition.last_refresh_snapshots.clone();
        let previous_table_uuids = mv_definition.last_refresh_table_uuids.clone();
        Self::from_parts(
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
        )
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
            crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
            crate::mv::aggregate_state::mv_agg_state::AggregateMvLayout,
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
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(
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
            crate::mv::aggregate_state::mv_agg_state::build_aggregate_mv_layout_with_input_types(
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
    calls: &crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls,
    contract: &MvSchemaContract,
) -> Result<Vec<Option<DataType>>, String> {
    use crate::mv::aggregate_state::mv_shape::AggregateInput;
    use crate::mv::model::VisibleAggregateOutput;

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
    input: &crate::mv::aggregate_state::mv_shape::AggregateInput,
) -> Result<Option<DataType>, String> {
    let crate::mv::aggregate_state::mv_shape::AggregateInput::Expr(expr) = input else {
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
    lineage: &mv_schema::ExpressionLineage,
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

fn base_contracts(contract: &MvSchemaContract) -> Vec<&mv_schema::BaseContract> {
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

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::mv::persistence::definition::StoredMvDefinition;
    use crate::mv::refresh::pin::RefreshSnapshotPin;
    use mv_schema::{
        ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot, ExpressionKind,
        ExpressionLineage, HiddenApplyKeyContract, JOIN_APPLY_KEY_COLUMN_NAME, JoinContract,
        JoinContractKind, JoinPredicateLineage, MvSchemaContract, OutputColumnLineage,
        OutputContract, QualifiedFieldLineage, TargetContract, TargetVisibleColumn,
    };
    use novarocks_catalog::identifier::TableIdentity;

    use super::*;

    pub(crate) fn make_ref(c: &str, n: &str, t: &str) -> TableIdentity {
        TableIdentity {
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

    pub(crate) fn make_target() -> TableIdentity {
        TableIdentity {
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        Arc::new(
            IcebergMvRewriteContext::from_definition_parts(
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

    pub(crate) fn join_projection_rewrite_context() -> Arc<IcebergMvRewriteContext> {
        let mut mv_def = make_mv_definition();
        mv_def.base_table_refs = vec!["ice.db.l".to_string(), "ice.db.r".to_string()];
        mv_def.last_refresh_snapshots = [
            ("ice.db.l".to_string(), 11i64),
            ("ice.db.r".to_string(), 33i64),
        ]
        .into_iter()
        .collect();
        mv_def.last_refresh_table_uuids = [
            ("ice.db.l".to_string(), "uuid-l".to_string()),
            ("ice.db.r".to_string(), "uuid-r".to_string()),
        ]
        .into_iter()
        .collect();
        let mut contract = make_schema_contract();
        contract.target.visible_columns[0].output_name = "k".to_string();
        contract.target.visible_columns[1].output_name = "v".to_string();
        contract.target.hidden_apply_key.column_name = JOIN_APPLY_KEY_COLUMN_NAME.to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::JoinRowKey;
        contract.bases = vec![
            join_base_contract("ice.db.l", "uuid-l", "l"),
            join_base_contract("ice.db.r", "uuid-r", "r"),
        ];
        contract.output.columns[0]
            .expression
            .referenced_base_field_ids
            .clear();
        contract.output.columns[0].expression.referenced_base_fields =
            vec![qualified_field("ice.db.l", "l", 1)];
        contract.output.columns[1]
            .expression
            .referenced_base_field_ids
            .clear();
        contract.output.columns[1].expression.referenced_base_fields =
            vec![qualified_field("ice.db.r", "r", 2)];
        contract.join = Some(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![JoinPredicateLineage {
                left: qualified_field("ice.db.l", "l", 1),
                right: qualified_field("ice.db.r", "r", 1),
            }],
        });
        contract.aggregate = None;
        mv_def.schema_contract = Some(contract.clone());
        let target_schema = Arc::new(
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
                        JOIN_APPLY_KEY_COLUMN_NAME,
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .expect("build join projection target schema"),
        );
        Arc::new(
            IcebergMvRewriteContext::from_definition_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT l.k, r.v FROM ice.db.l JOIN ice.db.r ON l.k = r.k",
                )),
                Arc::from(vec![make_ref("ice", "db", "l"), make_ref("ice", "db", "r")]),
                Arc::new(make_pin(&[
                    ("ice.db.l", 22, "uuid-l"),
                    ("ice.db.r", 44, "uuid-r"),
                ])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("join projection mv context must build"),
        )
    }

    fn join_base_contract(table_fqn: &str, table_uuid: &str, alias: &str) -> BaseContract {
        BaseContract {
            table_fqn: table_fqn.to_string(),
            table_uuid: table_uuid.to_string(),
            alias_at_create: Some(alias.to_string()),
            schema_id_at_create: 7,
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
        }
    }

    fn qualified_field(table_fqn: &str, qualifier: &str, field_id: i32) -> QualifiedFieldLineage {
        QualifiedFieldLineage {
            table_fqn: table_fqn.to_string(),
            qualifier_at_create: qualifier.to_string(),
            field_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::mv::refresh::pin::RefreshSnapshotPin;
    use mv_schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BRANCH_ID_COLUMN_NAME, BranchIdColumnContract, BranchUnionContract,
    };
    use novarocks_catalog::identifier::TableIdentity;

    use super::tests_support::*;
    use super::*;

    #[test]
    fn from_parts_happy_path_derives_all_fields() {
        let target = make_ref("TargetCase", "NameSpace", "MvTable");
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_definition_parts(
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

        assert_eq!(ctx.target.catalog, "TargetCase");
        assert_eq!(ctx.target.namespace, "NameSpace");
        assert_eq!(ctx.target.table, "MvTable");
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();

        let err_msg = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(Vec::<TableIdentity>::new());
        let pin = Arc::new(RefreshSnapshotPin::default());
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> =
            Arc::from(vec![make_ref("ice", "db", "b"), make_ref("ice", "db", "c")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        // Pin has the right count but the entry is for a different fqn.
        let pin = Arc::new(make_pin(&[("ice.db.OTHER", 22, "uuid-x")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-NEW")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let contract = Arc::new(make_schema_contract());

        let ctx = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let mut contract = make_schema_contract();
        contract.target.visible_columns.pop();
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        // Contract has two columns (matching schema count) but one has a wrong
        // target_field_id (schema has 100/101; contract claims 100/999).
        let mut contract = make_schema_contract();
        contract.target.visible_columns[1].target_field_id = 999;
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![
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

        let ctx = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));
        let schema = make_target_schema();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "nonexistent".to_string();
        let contract = Arc::new(contract);

        let err = IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
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

        IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
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

        IcebergMvRewriteContext::from_definition_parts(
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
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
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

        IcebergMvRewriteContext::from_definition_parts(
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
}
