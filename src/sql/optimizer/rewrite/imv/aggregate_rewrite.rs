use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use iceberg::spec::{NestedField, PrimitiveType, Type};

use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
use crate::sql::catalog::{ColumnDef, TableDef};
use crate::sql::codegen::helpers::typed_expr_display_name;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::imv::action_column::ImvActionColumn;
use crate::sql::optimizer::rewrite::imv::annotation::ImvExtension;
use crate::sql::optimizer::rewrite::imv::marker::{ImvDeltaNode, plan_contains_imv_marker};
use crate::sql::optimizer::rewrite::imv::target_state::build_target_state_scan_source;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::sql::planner::plan::{
    AggregateCall, AggregateNode, AggregateStateMergeNode, LogicalPlan, ScanNode,
};

pub(crate) struct RewriteAggregateStateRule;

pub(crate) fn signed_state_function(name: &str) -> Result<&'static str, String> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Ok("count_state_signed"),
        "sum" => Ok("sum_state_signed"),
        "min" => Ok("min_state_signed"),
        "max" => Ok("max_state_signed"),
        other => Err(format!("unsupported IMV aggregate function {other}")),
    }
}

impl LogicalRewriteRule for RewriteAggregateStateRule {
    fn name(&self) -> &'static str {
        "RewriteAggregateState"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(
            plan,
            LogicalPlan::ImvDelta(delta)
                if delta.is_root && matches!(delta.input.as_ref(), LogicalPlan::Aggregate(_))
        )
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::ImvDelta(delta) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        if !delta.is_root {
            return Ok(RewriteResult::Unchanged);
        }
        let LogicalPlan::Aggregate(aggregate) = *delta.input else {
            return Ok(RewriteResult::Unchanged);
        };

        if aggregate.group_by.is_empty() {
            return Err(
                "Iceberg IMV aggregate rewrite requires at least one GROUP BY key".to_string(),
            );
        }
        if aggregate.aggregates.iter().any(|call| call.distinct) {
            return Err(
                "Iceberg IMV aggregate rewrite does not support SELECT DISTINCT".to_string(),
            );
        }

        let ext = ctx.extension::<ImvExtension>().ok_or_else(|| {
            "RewriteAggregateState requires ImvExtension in RewriteContext".to_string()
        })?;
        let group_key_names = group_key_names(&aggregate)?;
        let aggregate_state_names = aggregate_state_names(ext, &aggregate)?;
        let row_id_column_name = aggregate_row_id_column_name(ext)?;
        let target_columns = target_columns(ext)?;
        let target = &ext.mv_ctx.target;

        let old_source = build_target_state_scan_source(
            target.catalog.clone(),
            target.namespace.clone(),
            target.table.clone(),
            target_columns.clone(),
            group_key_names.clone(),
            aggregate_state_names.clone(),
            row_id_column_name.clone(),
        );
        let old_columns = target_columns
            .iter()
            .map(|column| crate::sql::analysis::OutputColumn {
                column_id: ext.allocate_column_id(),
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                is_internal: aggregate_state_names
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&column.name))
                    || column.name.eq_ignore_ascii_case(&row_id_column_name),
            })
            .collect();
        let old_input = LogicalPlan::Scan(ScanNode {
            database: target.namespace.clone(),
            table: TableDef {
                name: target.table.clone(),
                columns: target_columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: old_source,
            },
            alias: None,
            columns: old_columns,
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        });

        let action_column = delta
            .action_column
            .unwrap_or_else(|| ext.allocate_column_id());
        let output_columns = aggregate.output_columns.clone();
        let signed_aggregate = signed_aggregate(aggregate, action_column)?;

        Ok(RewriteResult::Changed(LogicalPlan::AggregateStateMerge(
            AggregateStateMergeNode {
                old_input: Box::new(old_input),
                delta_input: Box::new(signed_aggregate),
                group_key_names,
                aggregate_state_names,
                change_op_column: ImvActionColumn::NAME.to_string(),
                output_columns,
            },
        )))
    }
}

fn group_key_names(aggregate: &AggregateNode) -> Result<Vec<String>, String> {
    aggregate
        .group_by
        .iter()
        .map(|expr| group_key_output_name(expr, &aggregate.output_columns))
        .collect()
}

fn group_key_output_name(
    expr: &TypedExpr,
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<String, String> {
    if let ExprKind::ColumnRef { column_id, .. } = &expr.kind {
        let matches = output_columns
            .iter()
            .filter(|column| column.column_id == *column_id)
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [column] => return Ok(column.name.clone()),
            [] => {}
            _ => {
                return Err(format!(
                    "Iceberg IMV aggregate rewrite found ambiguous GROUP BY output column id {column_id:?}"
                ));
            }
        }
    }

    let display_name = typed_expr_display_name(expr);
    let matches = output_columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(&display_name))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [column] => Ok(column.name.clone()),
        [] => Err(format!(
            "Iceberg IMV aggregate rewrite cannot map GROUP BY expression {display_name} to aggregate output column"
        )),
        _ => Err(format!(
            "Iceberg IMV aggregate rewrite found ambiguous GROUP BY output column name {display_name}"
        )),
    }
}

fn aggregate_state_names(
    ext: &ImvExtension,
    aggregate_node: &AggregateNode,
) -> Result<Vec<String>, String> {
    let aggregate = ext
        .mv_ctx
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate schema contract".to_string()
        })?;
    if aggregate.state_columns.is_empty() {
        return Err(
            "Iceberg IMV aggregate rewrite requires aggregate state columns in schema contract"
                .to_string(),
        );
    }
    if aggregate.state_columns.len() != aggregate_node.aggregates.len() {
        return Err(format!(
            "Iceberg IMV aggregate rewrite aggregate state column count {} does not match aggregate call count {}",
            aggregate.state_columns.len(),
            aggregate_node.aggregates.len()
        ));
    }
    for column in &aggregate.state_columns {
        if !column.type_signature.eq_ignore_ascii_case("binary") {
            return Err(format!(
                "Iceberg IMV aggregate rewrite aggregate state column {} must have binary type signature, got {}",
                column.column_name, column.type_signature
            ));
        }
    }
    Ok(aggregate
        .state_columns
        .iter()
        .map(|column| column.column_name.clone())
        .collect())
}

fn aggregate_row_id_column_name(ext: &ImvExtension) -> Result<String, String> {
    let aggregate = ext
        .mv_ctx
        .schema_contract
        .aggregate
        .as_ref()
        .ok_or_else(|| {
            "Iceberg IMV aggregate rewrite requires aggregate schema contract".to_string()
        })?;
    if aggregate.row_id_column_name.trim().is_empty() {
        return Err(
            "Iceberg IMV aggregate rewrite requires aggregate row-id column in schema contract"
                .to_string(),
        );
    }
    Ok(aggregate.row_id_column_name.clone())
}

fn target_columns(ext: &ImvExtension) -> Result<Vec<ColumnDef>, String> {
    ext.mv_ctx
        .target_schema
        .as_ref()
        .as_struct()
        .fields()
        .iter()
        .map(|field| target_column_from_field(field.as_ref()))
        .collect()
}

fn target_column_from_field(field: &NestedField) -> Result<ColumnDef, String> {
    Ok(ColumnDef {
        name: field.name.clone(),
        data_type: iceberg_type_to_arrow(field.field_type.as_ref(), &field.name)?,
        nullable: !field.required,
        write_default: field.write_default.clone(),
        logical_type: None,
    })
}

fn iceberg_type_to_arrow(ty: &Type, column_name: &str) -> Result<DataType, String> {
    Ok(match ty {
        Type::Primitive(primitive) => primitive_type_to_arrow(primitive, column_name)?,
        Type::Struct(struct_ty) => {
            let fields = struct_ty
                .fields()
                .iter()
                .map(|field| {
                    Ok(Arc::new(Field::new(
                        field.name.clone(),
                        iceberg_type_to_arrow(field.field_type.as_ref(), &field.name)?,
                        !field.required,
                    )))
                })
                .collect::<Result<Vec<_>, String>>()?;
            DataType::Struct(fields.into())
        }
        Type::List(list_ty) => {
            let element = list_ty.element_field.as_ref();
            DataType::List(Arc::new(Field::new(
                element.name.clone(),
                iceberg_type_to_arrow(element.field_type.as_ref(), &element.name)?,
                !element.required,
            )))
        }
        Type::Map(map_ty) => {
            let key = map_ty.key_field.as_ref();
            let value = map_ty.value_field.as_ref();
            let entries = DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        key.name.clone(),
                        iceberg_type_to_arrow(key.field_type.as_ref(), &key.name)?,
                        !key.required,
                    )),
                    Arc::new(Field::new(
                        value.name.clone(),
                        iceberg_type_to_arrow(value.field_type.as_ref(), &value.name)?,
                        !value.required,
                    )),
                ]
                .into(),
            );
            DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
        }
    })
}

fn primitive_type_to_arrow(
    primitive: &PrimitiveType,
    column_name: &str,
) -> Result<DataType, String> {
    Ok(match primitive {
        PrimitiveType::Boolean => DataType::Boolean,
        PrimitiveType::Int => DataType::Int32,
        PrimitiveType::Long => DataType::Int64,
        PrimitiveType::Float => DataType::Float32,
        PrimitiveType::Double => DataType::Float64,
        PrimitiveType::Decimal { precision, scale } => {
            let precision = u8::try_from(*precision).map_err(|_| {
                format!(
                    "Iceberg IMV aggregate rewrite target column {column_name} has out-of-range decimal precision {precision}"
                )
            })?;
            let scale = i8::try_from(*scale).map_err(|_| {
                format!(
                    "Iceberg IMV aggregate rewrite target column {column_name} has out-of-range decimal scale {scale}"
                )
            })?;
            DataType::Decimal128(precision, scale)
        }
        PrimitiveType::Date => DataType::Date32,
        PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
        PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        PrimitiveType::String => DataType::Utf8,
        PrimitiveType::Binary => DataType::Binary,
        other => {
            return Err(format!(
                "Iceberg IMV aggregate rewrite does not support target column {column_name} with Iceberg type {other:?}"
            ));
        }
    })
}

fn signed_aggregate(
    aggregate: AggregateNode,
    action_column: ColumnId,
) -> Result<LogicalPlan, String> {
    let signed_calls = aggregate
        .aggregates
        .iter()
        .map(|call| signed_aggregate_call(call, action_column))
        .collect::<Result<Vec<_>, String>>()?;
    let input = if plan_contains_imv_marker(&aggregate.input) {
        aggregate.input
    } else {
        Box::new(LogicalPlan::ImvDelta(ImvDeltaNode {
            input: aggregate.input,
            is_root: false,
            action_column: Some(action_column),
        }))
    };
    Ok(LogicalPlan::Aggregate(AggregateNode {
        input,
        group_by: aggregate.group_by,
        aggregates: signed_calls,
        output_columns: aggregate.output_columns,
        already_pushed: aggregate.already_pushed,
        required_output_columns: aggregate.required_output_columns,
    }))
}

fn signed_aggregate_call(
    call: &AggregateCall,
    action_column: ColumnId,
) -> Result<AggregateCall, String> {
    let signed_name = signed_state_function(&call.name)?;
    let value = signed_value_arg(call)?;
    Ok(AggregateCall {
        name: signed_name.to_string(),
        args: vec![signed_state_input(value, action_column)],
        distinct: false,
        result_type: DataType::Binary,
        order_by: call.order_by.clone(),
    })
}

fn signed_value_arg(call: &AggregateCall) -> Result<TypedExpr, String> {
    match call.args.as_slice() {
        [] if call.name.eq_ignore_ascii_case("count") => Ok(TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1)),
            data_type: DataType::Int64,
            nullable: false,
        }),
        [arg] => Ok(arg.clone()),
        [] => Err(format!(
            "Iceberg IMV aggregate rewrite requires an input for aggregate function {}",
            call.name
        )),
        _ => Err(format!(
            "Iceberg IMV aggregate rewrite supports only single-argument aggregate state inputs for {}",
            call.name
        )),
    }
}

fn signed_state_input(value: TypedExpr, action_column: ColumnId) -> TypedExpr {
    let value_type = value.data_type.clone();
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "named_struct".to_string(),
            args: vec![
                string_literal("value"),
                value,
                string_literal("change_op"),
                TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: action_column,
                        qualifier: None,
                        column: ImvActionColumn::NAME.to_string(),
                    },
                    data_type: DataType::Int8,
                    nullable: false,
                },
            ],
            distinct: false,
        },
        data_type: DataType::Struct(
            vec![
                Arc::new(Field::new("value", value_type, true)),
                Arc::new(Field::new("change_op", DataType::Int8, false)),
            ]
            .into(),
        ),
        nullable: true,
    }
}

fn string_literal(value: &str) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        data_type: DataType::Utf8,
        nullable: false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use arrow::datatypes::DataType;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use super::*;
    use crate::engine::mv::refresh_context::IcebergMvRewriteContext;
    use crate::engine::mv::refresh_context::tests_support::{
        make_mv_definition, make_pin, make_ref, make_schema_contract, make_target, parse_query,
    };
    use crate::meta::repository::mv_contract::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource,
    };
    use crate::sql::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::RewriteContext;
    use crate::sql::optimizer::rewrite::imv::annotation::{ImvExtension, ImvPlanAnnotation};
    use crate::sql::optimizer::rewrite::imv::marker::{
        ImvDeltaNode, ImvVersionNode, ImvVersionRef,
    };
    use crate::sql::planner::plan::{AggregateCall, AggregateNode, ScanNode, UnionNode};

    #[test]
    fn signed_state_function_maps_supported_aggregates() {
        assert_eq!(
            signed_state_function("count").unwrap(),
            "count_state_signed"
        );
        assert_eq!(signed_state_function("sum").unwrap(), "sum_state_signed");
        assert_eq!(signed_state_function("min").unwrap(), "min_state_signed");
        assert_eq!(signed_state_function("max").unwrap(), "max_state_signed");
    }

    #[test]
    fn signed_state_function_rejects_unsupported_aggregate() {
        let err = signed_state_function("median").expect_err("median must be unsupported");
        assert!(
            err.contains("unsupported IMV aggregate function median"),
            "{err}"
        );
        let err = signed_state_function("avg").expect_err("avg must be unsupported in Task 5");
        assert!(
            err.contains("unsupported IMV aggregate function avg"),
            "{err}"
        );
    }

    fn state_column(type_signature: &str) -> AggregateStateColumnContract {
        AggregateStateColumnContract {
            column_name: "__agg_state_s".to_string(),
            target_field_id: 200,
            type_signature: type_signature.to_string(),
            nullable: true,
            role: AggregateStateRoleContract::Single,
        }
    }

    fn build_ctx() -> RewriteContext {
        build_ctx_with_state_columns(vec![state_column("binary")])
    }

    fn build_ctx_with_state_columns(
        state_columns: Vec<AggregateStateColumnContract>,
    ) -> RewriteContext {
        let mut mv_def = make_mv_definition();
        let mut contract = make_schema_contract();
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns,
        });
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
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        200,
                        "__agg_state_s",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                ])
                .build()
                .expect("build schema"),
        );
        let mv_ctx = Arc::new(
            IcebergMvRewriteContext::from_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(mv_def),
                Arc::new(parse_query(
                    "SELECT k, sum(v) AS s FROM ice.db.b GROUP BY k",
                )),
                Arc::from(vec![make_ref("ice", "db", "b")]),
                Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
                Some(99),
                "uuid-tgt".to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("aggregate rewrite context must build"),
        );

        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            mv_ctx,
            annotation: ImvPlanAnnotation::default(),
            next_column_id: Arc::new(AtomicU32::new(100)),
        });
        ctx
    }

    fn col_expr(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn leaf_scan() -> LogicalPlan {
        let columns = vec![
            ColumnDef {
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "v".to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        LogicalPlan::Scan(ScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                    },
                    files: Vec::new(),
                    cloud_properties: BTreeMap::new(),
                },
            },
            alias: None,
            columns: vec![
                OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::new_for_test(2),
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            predicates: Vec::new(),
            required_columns: None,
            dict_columns: Vec::new(),
            required_output_columns: None,
        })
    }

    fn aggregate_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![col_expr(1, "k")],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_expr(2, "v")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::new_for_test(3),
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn aggregate_first_output_over(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(input),
            group_by: vec![col_expr(1, "k")],
            aggregates: vec![AggregateCall {
                name: "sum".to_string(),
                args: vec![col_expr(2, "v")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: Vec::new(),
            }],
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId::new_for_test(3),
                    name: "s".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
            ],
            already_pushed: false,
            required_output_columns: None,
        })
    }

    fn aggregate_with_two_calls(input: LogicalPlan) -> LogicalPlan {
        let mut plan = match aggregate_over(input) {
            LogicalPlan::Aggregate(node) => node,
            _ => unreachable!(),
        };
        plan.aggregates.push(AggregateCall {
            name: "count".to_string(),
            args: Vec::new(),
            distinct: false,
            result_type: DataType::Int64,
            order_by: Vec::new(),
        });
        plan.output_columns.push(OutputColumn {
            column_id: ColumnId::new_for_test(4),
            name: "c".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            is_internal: false,
        });
        LogicalPlan::Aggregate(plan)
    }

    fn delta(input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(input),
            is_root: true,
            action_column: None,
        })
    }

    fn join_expanded_input() -> LogicalPlan {
        LogicalPlan::Union(UnionNode {
            inputs: vec![
                LogicalPlan::ImvDelta(ImvDeltaNode {
                    input: Box::new(leaf_scan()),
                    is_root: false,
                    action_column: Some(ColumnId::new_for_test(100)),
                }),
                LogicalPlan::ImvVersion(ImvVersionNode {
                    input: Box::new(leaf_scan()),
                    version_ref: ImvVersionRef::from_snapshot(),
                }),
            ],
            all: true,
            output_columns: vec![
                OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: ColumnId::new_for_test(2),
                    name: "v".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    is_internal: false,
                },
            ],
            required_output_columns: None,
        })
    }

    #[test]
    fn rewrite_aggregate_state_matches_only_root_delta_over_aggregate() {
        let rule = RewriteAggregateStateRule;
        let ctx = build_ctx();
        assert!(rule.matches(&delta(aggregate_over(leaf_scan())), &ctx));
        assert!(!rule.matches(&aggregate_over(leaf_scan()), &ctx));
        let nested_delta = LogicalPlan::ImvDelta(ImvDeltaNode {
            input: Box::new(aggregate_over(leaf_scan())),
            is_root: false,
            action_column: None,
        });
        assert!(!rule.matches(&nested_delta, &ctx));
    }

    #[test]
    fn rewrite_aggregate_state_rejects_empty_group_by() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let mut aggregate = match aggregate_over(leaf_scan()) {
            LogicalPlan::Aggregate(aggregate) => aggregate,
            _ => unreachable!(),
        };
        aggregate.group_by.clear();
        let err = rule
            .apply(delta(LogicalPlan::Aggregate(aggregate)), &mut ctx)
            .expect_err("empty GROUP BY must fail");
        assert_eq!(
            err,
            "Iceberg IMV aggregate rewrite requires at least one GROUP BY key"
        );
    }

    #[test]
    fn rewrite_aggregate_state_rejects_distinct_aggregate() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let mut aggregate = match aggregate_over(leaf_scan()) {
            LogicalPlan::Aggregate(aggregate) => aggregate,
            _ => unreachable!(),
        };
        aggregate.aggregates[0].distinct = true;
        let err = rule
            .apply(delta(LogicalPlan::Aggregate(aggregate)), &mut ctx)
            .expect_err("distinct aggregate must fail");
        assert_eq!(
            err,
            "Iceberg IMV aggregate rewrite does not support SELECT DISTINCT"
        );
    }

    #[test]
    fn rewrite_aggregate_state_builds_state_merge_with_signed_delta() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let result = rule
            .apply(delta(aggregate_over(leaf_scan())), &mut ctx)
            .expect("aggregate rewrite must succeed");
        let RewriteResult::Changed(LogicalPlan::AggregateStateMerge(merge)) = result else {
            panic!("expected Changed(AggregateStateMerge)");
        };

        assert_eq!(merge.group_key_names, vec!["k"]);
        assert_eq!(merge.aggregate_state_names, vec!["__agg_state_s"]);
        assert_eq!(merge.change_op_column, "__change_op");
        assert_eq!(merge.output_columns[1].name, "s");

        let LogicalPlan::Scan(old_scan) = merge.old_input.as_ref() else {
            panic!("expected target-state scan");
        };
        let ScanSource::IcebergMvTargetState(target_state) = &old_scan.table.source else {
            panic!("expected IcebergMvTargetState source");
        };
        assert_eq!(target_state.fqn(), "tgt.db.mv");
        assert_eq!(target_state.group_key_names, vec!["k"]);
        assert_eq!(target_state.aggregate_state_names, vec!["__agg_state_s"]);

        let LogicalPlan::Aggregate(signed_aggregate) = merge.delta_input.as_ref() else {
            panic!("expected signed aggregate delta input");
        };
        assert!(matches!(
            signed_aggregate.input.as_ref(),
            LogicalPlan::ImvDelta(ImvDeltaNode { is_root: false, .. })
        ));
        assert_eq!(signed_aggregate.aggregates[0].name, "sum_state_signed");
        assert_eq!(signed_aggregate.output_columns[1].name, "s");
        assert_eq!(
            signed_aggregate.output_columns[1].data_type,
            DataType::Int64
        );
        let args = &signed_aggregate.aggregates[0].args;
        assert_eq!(args.len(), 1);
        let ExprKind::FunctionCall {
            name,
            args: struct_args,
            ..
        } = &args[0].kind
        else {
            panic!("expected named_struct signed input");
        };
        assert_eq!(name, "named_struct");
        assert!(matches!(
            &struct_args[0].kind,
            ExprKind::Literal(LiteralValue::String(name)) if name == "value"
        ));
        assert!(matches!(
            &struct_args[2].kind,
            ExprKind::Literal(LiteralValue::String(name)) if name == "change_op"
        ));
        assert!(matches!(
            &struct_args[3].kind,
            ExprKind::ColumnRef { column, .. } if column == "__change_op"
        ));
    }

    #[test]
    fn rewrite_aggregate_state_preserves_pre_expanded_join_delta_input() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let result = rule
            .apply(delta(aggregate_over(join_expanded_input())), &mut ctx)
            .expect("aggregate rewrite must succeed");
        let RewriteResult::Changed(LogicalPlan::AggregateStateMerge(merge)) = result else {
            panic!("expected Changed(AggregateStateMerge)");
        };

        let LogicalPlan::Aggregate(signed_aggregate) = merge.delta_input.as_ref() else {
            panic!("expected signed aggregate delta input");
        };
        assert!(
            matches!(signed_aggregate.input.as_ref(), LogicalPlan::Union(_)),
            "pre-expanded join delta input must not be wrapped as ImvDelta(Union)"
        );
    }

    #[test]
    fn rewrite_aggregate_state_maps_group_key_by_column_id_when_output_is_aggregate_first() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let result = rule
            .apply(delta(aggregate_first_output_over(leaf_scan())), &mut ctx)
            .expect("aggregate rewrite must succeed");
        let RewriteResult::Changed(LogicalPlan::AggregateStateMerge(merge)) = result else {
            panic!("expected Changed(AggregateStateMerge)");
        };

        assert_eq!(merge.group_key_names, vec!["k"]);
    }

    #[test]
    fn rewrite_aggregate_state_rejects_state_column_count_mismatch() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx();
        let err = rule
            .apply(delta(aggregate_with_two_calls(leaf_scan())), &mut ctx)
            .expect_err("state column count mismatch must fail");
        assert!(
            err.contains("aggregate state column count"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rewrite_aggregate_state_rejects_non_binary_state_column() {
        let rule = RewriteAggregateStateRule;
        let mut ctx = build_ctx_with_state_columns(vec![state_column("string")]);
        let err = rule
            .apply(delta(aggregate_over(leaf_scan())), &mut ctx)
            .expect_err("non-binary state column must fail");
        assert!(
            err.contains("must have binary type signature"),
            "unexpected error: {err}"
        );
    }
}
