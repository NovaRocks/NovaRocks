pub(crate) mod expr;
pub(crate) mod instance;
pub(crate) mod plan;
pub(crate) mod types;

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use prost::Message;

    use super::expr::encode_expr;
    use super::types::{decode_field_type, decode_type, encode_field_type, encode_type};
    use super::{instance, plan};
    use crate::proto::{common, expr};
    use crate::sql::analysis::{ExprKind, SortItem, SubqueryKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::{
        BinOp, LambdaParam, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
    };
    use crate::types::logical::{LogicalType, field_with_logical_type, logical_type_of_field};

    fn roundtrip_message<M>(value: &M) -> M
    where
        M: Message + Default,
    {
        M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
    }

    fn scalar_primitive(desc: &common::TypeDesc) -> common::PrimitiveType {
        let common::type_desc::Kind::Scalar(scalar) = desc.kind.as_ref().expect("type kind") else {
            panic!("expected scalar TypeDesc");
        };
        common::PrimitiveType::try_from(scalar.r#type).expect("known primitive")
    }

    fn literal_expr(value: LiteralValue, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(value),
            data_type,
            nullable: false,
        }
    }

    fn int_expr(value: i64) -> TypedExpr {
        literal_expr(LiteralValue::Int(value), DataType::Int64)
    }

    fn bool_expr(value: bool) -> TypedExpr {
        literal_expr(LiteralValue::Bool(value), DataType::Boolean)
    }

    fn string_expr(value: &str) -> TypedExpr {
        literal_expr(LiteralValue::String(value.to_string()), DataType::Utf8)
    }

    fn column_expr(id: u32, name: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type,
            nullable: true,
        }
    }

    fn assert_expr_roundtrip(encoded: expr::Expr) -> expr::Expr {
        let decoded = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded);
        decoded
    }

    #[test]
    fn recursive_arrow_type_round_trips_through_type_desc() {
        let data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("amount", DataType::Decimal128(18, 2), true)),
                Arc::new(Field::new(
                    "ids",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                )),
            ])),
            true,
        )));

        let encoded = encode_type(&data_type).expect("encode recursive type");
        let decoded_proto: common::TypeDesc = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded_proto);
        assert_eq!(
            decode_type(&decoded_proto).expect("decode recursive type"),
            data_type
        );
    }

    #[test]
    fn map_type_round_trips_through_type_desc() {
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                    Arc::new(Field::new("value", DataType::Decimal128(12, 4), true)),
                ])),
                false,
            )),
            false,
        );

        let encoded = encode_type(&data_type).expect("encode map type");
        let decoded_proto: common::TypeDesc = roundtrip_message(&encoded);
        assert_eq!(encoded, decoded_proto);
        assert_eq!(
            decode_type(&decoded_proto).expect("decode map type"),
            data_type
        );
    }

    #[test]
    fn metadata_logical_fields_encode_to_logical_primitives() {
        let cases = [
            (
                field_with_logical_type(
                    Field::new("json_payload", DataType::Utf8, true),
                    LogicalType::Json,
                ),
                common::PrimitiveType::Json,
                DataType::Utf8,
                Some(LogicalType::Json),
            ),
            (
                field_with_logical_type(
                    Field::new("hll_state", DataType::Binary, true),
                    LogicalType::Hll,
                ),
                common::PrimitiveType::Hll,
                DataType::Binary,
                Some(LogicalType::Hll),
            ),
            (
                field_with_logical_type(
                    Field::new("bitmap_state", DataType::Binary, true),
                    LogicalType::Bitmap,
                ),
                common::PrimitiveType::Bitmap,
                DataType::Binary,
                Some(LogicalType::Bitmap),
            ),
            (
                field_with_logical_type(
                    Field::new("object_state", DataType::Binary, true),
                    LogicalType::Object,
                ),
                common::PrimitiveType::Object,
                DataType::Binary,
                Some(LogicalType::Object),
            ),
            (
                field_with_logical_type(
                    Field::new("percentile_state", DataType::Binary, true),
                    LogicalType::Percentile,
                ),
                common::PrimitiveType::Percentile,
                DataType::Binary,
                Some(LogicalType::Percentile),
            ),
            (
                Field::new("variant_payload", DataType::LargeBinary, true),
                common::PrimitiveType::Variant,
                DataType::LargeBinary,
                None,
            ),
            (
                Field::new("large_int", DataType::FixedSizeBinary(16), true),
                common::PrimitiveType::Largeint,
                DataType::FixedSizeBinary(16),
                None,
            ),
        ];

        for (field, expected_primitive, expected_type, expected_logical) in cases {
            let encoded = encode_field_type(&field).expect("encode logical field");
            assert_eq!(scalar_primitive(&encoded), expected_primitive);

            let decoded = decode_field_type(field.name(), field.is_nullable(), &encoded)
                .expect("decode field");
            assert_eq!(decoded.data_type(), &expected_type);
            assert_eq!(logical_type_of_field(&decoded), expected_logical);
        }
    }

    #[test]
    fn nested_logical_field_metadata_survives_decode_type() {
        let data_type = DataType::Struct(Fields::from(vec![Arc::new(field_with_logical_type(
            Field::new("payload", DataType::Utf8, true),
            LogicalType::Json,
        ))]));

        let encoded = encode_type(&data_type).expect("encode struct with logical child");
        let decoded = decode_type(&roundtrip_message(&encoded)).expect("decode logical child");

        let DataType::Struct(fields) = decoded else {
            panic!("expected struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(
            logical_type_of_field(fields[0].as_ref()),
            Some(LogicalType::Json)
        );
    }

    #[test]
    fn decimal_date_and_largeint_literals_are_structured() {
        let decimal = literal_expr(
            LiteralValue::Decimal("-123.45".to_string()),
            DataType::Decimal128(10, 2),
        );
        let encoded = encode_expr(&decimal).expect("encode decimal literal");
        let decoded = assert_expr_roundtrip(encoded);
        let Some(expr::expr::Kind::Literal(lit)) = decoded.kind else {
            panic!("expected literal");
        };
        let Some(common::literal_value::Value::DecimalValue(value)) =
            lit.value.and_then(|v| v.value)
        else {
            panic!("expected decimal literal");
        };
        assert_eq!(value.precision, 10);
        assert_eq!(value.scale, 2);
        assert_eq!(value.value, (-12_345i128).to_be_bytes().to_vec());

        let date = literal_expr(LiteralValue::Int(19_000), DataType::Date32);
        let encoded = encode_expr(&date).expect("encode date32 literal");
        let Some(expr::expr::Kind::Literal(lit)) = assert_expr_roundtrip(encoded).kind else {
            panic!("expected literal");
        };
        assert_eq!(
            lit.value.and_then(|v| v.value),
            Some(common::literal_value::Value::Date32Value(19_000))
        );

        let largeint = literal_expr(
            LiteralValue::LargeInt(-170_141_183_460_469_231_731_687_303_715_884_105_728i128),
            DataType::FixedSizeBinary(16),
        );
        let encoded = encode_expr(&largeint).expect("encode largeint literal");
        let Some(expr::expr::Kind::Literal(lit)) = assert_expr_roundtrip(encoded).kind else {
            panic!("expected literal");
        };
        assert_eq!(
            lit.value.and_then(|v| v.value),
            Some(common::literal_value::Value::LargeintValue(
                i128::MIN.to_be_bytes().to_vec()
            ))
        );
    }

    #[test]
    fn decimal256_literal_encodes_full_32_byte_range() {
        let decimal = literal_expr(
            LiteralValue::Decimal("170141183460469231731687303715884105728.99".to_string()),
            DataType::Decimal256(41, 2),
        );

        let encoded = encode_expr(&decimal).expect("encode decimal256 literal");
        let Some(expr::expr::Kind::Literal(lit)) = assert_expr_roundtrip(encoded).kind else {
            panic!("expected literal");
        };
        let Some(common::literal_value::Value::DecimalValue(value)) =
            lit.value.and_then(|v| v.value)
        else {
            panic!("expected decimal literal");
        };
        assert_eq!(value.precision, 41);
        assert_eq!(value.scale, 2);
        assert_eq!(value.value.len(), 32);
        assert_eq!(
            hex::encode(value.value),
            "0000000000000000000000000000003200000000000000000000000000000063"
        );
    }

    #[test]
    fn invalid_decimal_type_widths_are_rejected() {
        let err = encode_type(&DataType::Decimal128(39, 0))
            .expect_err("Decimal128 precision above 38 must fail");
        assert!(err.contains("Decimal128"));
        assert!(err.contains("precision"));

        let err = encode_type(&DataType::Decimal256(77, 0))
            .expect_err("Decimal256 precision above 76 must fail");
        assert!(err.contains("Decimal256"));
        assert!(err.contains("precision"));
    }

    #[test]
    fn invalid_decimal_literal_widths_are_rejected() {
        let decimal128 = literal_expr(
            LiteralValue::Decimal("1".to_string()),
            DataType::Decimal128(39, 0),
        );
        let err = encode_expr(&decimal128).expect_err("Decimal128 literal width must fail");
        assert!(err.contains("Decimal128"));
        assert!(err.contains("precision"));

        let decimal256 = literal_expr(
            LiteralValue::Decimal("1".to_string()),
            DataType::Decimal256(77, 0),
        );
        let err = encode_expr(&decimal256).expect_err("Decimal256 literal width must fail");
        assert!(err.contains("Decimal256"));
        assert!(err.contains("precision"));
    }

    #[test]
    fn typed_expr_variants_encode_to_expected_oneof_arms() {
        let col = column_expr(7, "a", DataType::Int64);
        let lit = int_expr(2);
        let lambda_body = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::LambdaParamRef {
                        name: "x".to_string(),
                        slot_id: 3,
                    },
                    data_type: DataType::Int64,
                    nullable: true,
                }),
                op: BinOp::Add,
                right: Box::new(int_expr(1)),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let sort_item = SortItem {
            expr: col.clone(),
            asc: false,
            nulls_first: true,
        };
        let lambda_expr = TypedExpr {
            kind: ExprKind::LambdaFunction {
                params: vec![LambdaParam {
                    name: "x".to_string(),
                    slot_id: 3,
                    data_type: DataType::Int64,
                    nullable: true,
                }],
                body: Box::new(lambda_body.clone()),
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let Some(expr::expr::Kind::Lambda(lambda)) =
            encode_expr(&lambda_expr).expect("encode lambda").kind
        else {
            panic!("expected lambda");
        };
        assert_eq!(lambda.params[0].name.as_deref(), Some("x"));
        assert_eq!(lambda.params[0].slot_id, 3);
        assert_eq!(
            scalar_primitive(lambda.params[0].r#type.as_ref().expect("type")),
            common::PrimitiveType::Bigint
        );
        let variants = vec![
            column_expr(1, "c1", DataType::Int64),
            TypedExpr {
                kind: ExprKind::LambdaParamRef {
                    name: "x".to_string(),
                    slot_id: 3,
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            lit.clone(),
            TypedExpr {
                kind: ExprKind::BinaryOp {
                    left: Box::new(col.clone()),
                    op: BinOp::Gt,
                    right: Box::new(lit.clone()),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::UnaryOp {
                    op: UnOp::Not,
                    expr: Box::new(bool_expr(true)),
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: "abs".to_string(),
                    args: vec![col.clone()],
                    distinct: false,
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::LambdaFunction {
                    params: vec![LambdaParam {
                        name: "x".to_string(),
                        slot_id: 3,
                        data_type: DataType::Int64,
                        nullable: true,
                    }],
                    body: Box::new(lambda_body.clone()),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::AggregateCall {
                    name: "sum".to_string(),
                    args: vec![col.clone()],
                    distinct: true,
                    order_by: vec![sort_item.clone()],
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::Cast {
                    expr: Box::new(col.clone()),
                    target: DataType::Float64,
                },
                data_type: DataType::Float64,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::IsNull {
                    expr: Box::new(col.clone()),
                    negated: true,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::InList {
                    expr: Box::new(string_expr("x")),
                    list: vec![string_expr("a"), string_expr("b")],
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::Between {
                    expr: Box::new(col.clone()),
                    low: Box::new(int_expr(1)),
                    high: Box::new(int_expr(9)),
                    negated: true,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::Like {
                    expr: Box::new(string_expr("abc")),
                    pattern: Box::new(string_expr("a%")),
                    negated: true,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::Case {
                    operand: None,
                    when_then: vec![(bool_expr(true), int_expr(1))],
                    else_expr: Some(Box::new(int_expr(0))),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::IsTruthValue {
                    expr: Box::new(bool_expr(false)),
                    value: false,
                    negated: true,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::Nested(Box::new(col.clone())),
                data_type: DataType::Int64,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::WindowCall {
                    name: "rank".to_string(),
                    args: vec![],
                    distinct: false,
                    partition_by: vec![col.clone()],
                    order_by: vec![sort_item],
                    window_frame: Some(WindowFrame {
                        frame_type: WindowFrameType::Rows,
                        start: WindowBound::UnboundedPreceding,
                        end: WindowBound::CurrentRow,
                    }),
                    ignore_nulls: false,
                },
                data_type: DataType::Int64,
                nullable: false,
            },
            TypedExpr {
                kind: ExprKind::Lambda {
                    params: vec!["x".to_string()],
                    body: Box::new(lambda_body),
                },
                data_type: DataType::Int64,
                nullable: true,
            },
        ];

        let names = variants
            .iter()
            .map(|expr| {
                let encoded = encode_expr(expr).expect("encode variant");
                let decoded = assert_expr_roundtrip(encoded);
                match decoded.kind.expect("expr kind") {
                    expr::expr::Kind::ColumnRef(_) => "column_ref",
                    expr::expr::Kind::LambdaParamRef(_) => "lambda_param_ref",
                    expr::expr::Kind::Literal(_) => "literal",
                    expr::expr::Kind::BinaryOp(_) => "binary_op",
                    expr::expr::Kind::UnaryOp(_) => "unary_op",
                    expr::expr::Kind::FunctionCall(_) => "function_call",
                    expr::expr::Kind::Lambda(_) => "lambda",
                    expr::expr::Kind::AggregateCall(_) => "aggregate_call",
                    expr::expr::Kind::Cast(_) => "cast",
                    expr::expr::Kind::IsNull(_) => "is_null",
                    expr::expr::Kind::InList(_) => "in_list",
                    expr::expr::Kind::Between(_) => "between",
                    expr::expr::Kind::Like(_) => "like",
                    expr::expr::Kind::CaseExpr(_) => "case",
                    expr::expr::Kind::IsTruth(_) => "is_truth",
                    expr::expr::Kind::Nested(_) => "nested",
                    expr::expr::Kind::WindowCall(_) => "window_call",
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(names.len(), 18);
        assert_eq!(names.iter().filter(|&&name| name == "lambda").count(), 2);
        assert!(names.contains(&"window_call"));
        assert!(names.contains(&"aggregate_call"));
        assert!(names.contains(&"case"));
    }

    #[test]
    fn representative_nested_expr_fields_are_preserved() {
        let col = column_expr(7, "amount", DataType::Int64);

        let cast = TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(col.clone()),
                target: DataType::Float64,
            },
            data_type: DataType::Float64,
            nullable: true,
        };
        let Some(expr::expr::Kind::Cast(cast)) = encode_expr(&cast).expect("encode cast").kind
        else {
            panic!("expected cast");
        };
        assert_eq!(
            scalar_primitive(cast.target.as_ref().expect("target type")),
            common::PrimitiveType::Double
        );

        let lambda_param = TypedExpr {
            kind: ExprKind::LambdaParamRef {
                name: "x".to_string(),
                slot_id: 3,
            },
            data_type: DataType::Int64,
            nullable: true,
        };
        let Some(expr::expr::Kind::LambdaParamRef(param)) = encode_expr(&lambda_param)
            .expect("encode lambda param")
            .kind
        else {
            panic!("expected lambda param");
        };
        assert_eq!(param.slot_id, 3);
        assert_eq!(param.name.as_deref(), Some("x"));

        let sort_item = SortItem {
            expr: col.clone(),
            asc: false,
            nulls_first: true,
        };
        let window = TypedExpr {
            kind: ExprKind::WindowCall {
                name: "rank".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![col],
                order_by: vec![sort_item],
                window_frame: Some(WindowFrame {
                    frame_type: WindowFrameType::Rows,
                    start: WindowBound::UnboundedPreceding,
                    end: WindowBound::CurrentRow,
                }),
                ignore_nulls: false,
            },
            data_type: DataType::Int64,
            nullable: false,
        };
        let Some(expr::expr::Kind::WindowCall(window)) =
            encode_expr(&window).expect("encode window").kind
        else {
            panic!("expected window call");
        };
        assert_eq!(window.function_name, "rank");
        assert_eq!(window.order_by.len(), 1);
        assert!(!window.order_by[0].asc);
        assert!(window.order_by[0].nulls_first);
        assert_eq!(
            window.frame.as_ref().expect("window frame").frame_type,
            expr::WindowFrameType::Rows as i32
        );
    }

    #[test]
    fn subquery_placeholder_is_rejected_by_expr_encoder() {
        let subquery = TypedExpr {
            kind: ExprKind::SubqueryPlaceholder {
                id: 42,
                kind: SubqueryKind::Scalar,
                data_type: DataType::Int64,
            },
            data_type: DataType::Int64,
            nullable: true,
        };

        let err = encode_expr(&subquery).expect_err("subquery must fail fast");
        assert!(err.contains("SubqueryPlaceholder"));
        assert!(err.contains("42"));
    }

    #[test]
    fn unsupported_decimal_literal_reports_clear_error() {
        let malformed = literal_expr(
            LiteralValue::Decimal("12.345".to_string()),
            DataType::Decimal128(10, 2),
        );

        let err = encode_expr(&malformed).expect_err("scale mismatch");
        assert!(err.contains("scale"));
    }

    #[test]
    fn unsupported_timestamp_unit_reports_clear_error() {
        let err = encode_type(&DataType::Timestamp(TimeUnit::Second, None))
            .expect_err("second timestamp rejected");

        assert!(err.contains("unsupported timestamp unit"));
    }

    #[test]
    fn unsupported_time64_unit_reports_clear_error() {
        let err = encode_type(&DataType::Time64(TimeUnit::Nanosecond))
            .expect_err("nanosecond Time64 rejected");

        assert!(err.contains("unsupported Time64 unit"));
    }

    fn planner_output_column(
        id: u32,
        name: &str,
        data_type: DataType,
    ) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal: false,
        }
    }

    fn physical_stats() -> crate::sql::planner::PhysicalPlanStats {
        crate::sql::planner::PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: crate::sql::planner::PlannerConfidence::Fallback,
            column_statistics: std::collections::HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn values_distributed_node(
        fragment_id: crate::sql::codegen::FragmentId,
        node_id: i32,
        output: Vec<crate::sql::analysis::OutputColumn>,
    ) -> crate::sql::planner::DistributedNode {
        crate::sql::planner::DistributedNode {
            node_id,
            fragment_id,
            tuple_ids: vec![node_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::Values(
                    crate::sql::planner::plan::PlanValuesNode {
                        rows: vec![vec![int_expr(7)]],
                        columns: output,
                    },
                ),
            ),
        }
    }

    fn iceberg_scan_table_for_columns(names: &[&str]) -> crate::sql::catalog::TableDef {
        let columns = names
            .iter()
            .map(|name| crate::sql::catalog::ColumnDef {
                name: (*name).to_string(),
                data_type: DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            })
            .collect::<Vec<_>>();
        let schema = crate::sql::catalog::IcebergSchemaDef {
            fields: names
                .iter()
                .enumerate()
                .map(|(idx, name)| crate::sql::catalog::IcebergSchemaFieldDef {
                    field_id: i32::try_from(idx + 1).expect("field id"),
                    name: (*name).to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                })
                .collect(),
        };
        let iceberg_table = crate::sql::catalog::IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "sc2".to_string(),
            table_uuid: Some("uuid-sc2".to_string()),
            current_snapshot_id: Some(10),
            schema_id: 1,
            location: "s3://warehouse/db/sc2".to_string(),
            schema,
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        crate::sql::catalog::TableDef {
            name: "sc2".to_string(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::sql::catalog::ScanSource::IcebergDataFiles {
                table: iceberg_table,
                files: Vec::new(),
                cloud_properties: std::collections::BTreeMap::new(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    #[test]
    fn distributed_plan_encoder_round_trips_fragments_edges_partitions_and_exchange() {
        let output = vec![planner_output_column(10, "v", DataType::Int64)];
        let source = crate::sql::planner::PlanFragment {
            fragment_id: 0,
            root: values_distributed_node(0, 11, output.clone()),
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition {
                kind: crate::sql::planner::PartitionKind::Hash,
                exprs: vec![column_expr(10, "v", DataType::Int64)],
            },
            sink: crate::sql::planner::DataSink::Noop,
            output_exprs: None,
            output_columns: output.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let receiver = crate::sql::planner::DistributedNode {
            node_id: 42,
            fragment_id: 1,
            tuple_ids: vec![42],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Exchange(
                crate::sql::planner::ExchangeReceiver {
                    partition: crate::sql::planner::DataPartition::hash(vec![column_expr(
                        10,
                        "v",
                        DataType::Int64,
                    )]),
                    source_fragment_id: 0,
                    output_columns: output.clone(),
                    output_qualifier: Some("recv".to_string()),
                    flavor: crate::sql::planner::plan::ExchangeFlavor::Distribution,
                },
            ),
        };
        let target = crate::sql::planner::PlanFragment {
            fragment_id: 1,
            root: receiver,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Result,
            output_exprs: Some(vec![column_expr(10, "v", DataType::Int64)]),
            output_columns: output,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 1,
            edges: vec![crate::sql::codegen::FragmentEdge {
                source_fragment_id: 0,
                target_fragment_id: 1,
                target_exchange_node_id: 42,
                output_partition: crate::sql::planner::DataPartition {
                    kind: crate::sql::planner::PartitionKind::Hash,
                    exprs: vec![column_expr(10, "v", DataType::Int64)],
                },
                stream_kind: crate::sql::codegen::FragmentStreamKind::Partitioned,
                edge_kind: crate::sql::codegen::FragmentEdgeKind::Stream,
                output_slot_ids: vec![10],
            }],
        };

        let encoded = plan::encode_distributed_plan(&plan).expect("encode distributed plan");
        let decoded: crate::proto::plan::DistributedPlan = roundtrip_message(&encoded);

        assert_eq!(decoded.root_fragment_id, 1);
        assert_eq!(decoded.fragments.len(), 2);
        assert_eq!(decoded.edges.len(), 1);
        assert_eq!(decoded.edges[0].target_exchange_node_id, 42);
        assert_eq!(
            decoded.edges[0].output_partition,
            crate::proto::plan::PartitionType::Hash as i32
        );
        assert_eq!(
            decoded.edges[0]
                .edge_kind
                .as_ref()
                .and_then(|kind| kind.kind.as_ref()),
            Some(&crate::proto::plan::fragment_edge_kind::Kind::Stream(true))
        );

        let root_fragment = decoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("root fragment");
        assert_eq!(root_fragment.output_exprs.len(), 1);
        let root = root_fragment.root.as_ref().expect("root node");
        let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
            root.payload.as_ref()
        else {
            panic!("expected exchange receiver payload");
        };
        assert_eq!(exchange.source_fragment_id, 0);
        assert_eq!(exchange.output_qualifier.as_deref(), Some("recv"));
        assert_eq!(
            exchange.partition_type,
            crate::proto::plan::PartitionType::Hash as i32
        );
        assert_eq!(exchange.output_columns.len(), 1);
        assert_eq!(exchange.output_columns[0].column_id, 10);
        assert_eq!(exchange.output_columns[0].name, "v");
    }

    #[test]
    fn stream_edge_projects_pruned_scan_columns_by_column_id() {
        let all_scan_columns = vec![
            planner_output_column(1, "v1", DataType::Int64),
            planner_output_column(2, "s2", DataType::Utf8),
            planner_output_column(3, "array1", DataType::Int64),
        ];
        let stream_columns = vec![all_scan_columns[1].clone(), all_scan_columns[2].clone()];
        let source_root = crate::sql::planner::DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: vec![11],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::Scan(
                    crate::sql::planner::plan::PlanScanNode {
                        database: "db".to_string(),
                        table: iceberg_scan_table_for_columns(&["v1", "s2", "array1"]),
                        alias: None,
                        columns: all_scan_columns,
                        predicates: Vec::new(),
                        required_columns: Some(vec!["s2".to_string(), "array1".to_string()]),
                        variant_columns: Vec::new(),
                        mv_rewritten_from: None,
                    },
                ),
            ),
        };
        let source = crate::sql::planner::PlanFragment {
            fragment_id: 0,
            root: source_root,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Noop,
            output_exprs: None,
            output_columns: stream_columns.clone(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let receiver = crate::sql::planner::DistributedNode {
            node_id: 42,
            fragment_id: 1,
            tuple_ids: vec![42],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Exchange(
                crate::sql::planner::ExchangeReceiver {
                    partition: crate::sql::planner::DataPartition::unpartitioned(),
                    source_fragment_id: 0,
                    output_columns: Vec::new(),
                    output_qualifier: None,
                    flavor: crate::sql::planner::plan::ExchangeFlavor::Distribution,
                },
            ),
        };
        let target = crate::sql::planner::PlanFragment {
            fragment_id: 1,
            root: receiver,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 1,
            edges: vec![crate::sql::codegen::FragmentEdge {
                source_fragment_id: 0,
                target_fragment_id: 1,
                target_exchange_node_id: 42,
                output_partition: crate::sql::planner::DataPartition::unpartitioned(),
                stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
                edge_kind: crate::sql::codegen::FragmentEdgeKind::Stream,
                output_slot_ids: vec![2, 3],
            }],
        };

        let encoded = plan::encode_distributed_plan(&plan).expect("encode distributed plan");
        let target_fragment = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("target fragment");
        let root = target_fragment.root.as_ref().expect("target root");
        let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
            root.payload.as_ref()
        else {
            panic!("expected exchange receiver payload");
        };

        let patched = exchange
            .output_columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(patched, vec![(2, "s2"), (3, "array1")]);
    }

    #[test]
    fn stream_edge_patches_exchange_columns_from_aggregate_layout_when_fragment_output_is_empty() {
        let group_column = planner_output_column(2, "c1", DataType::Utf8);
        let source_root = crate::sql::planner::DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: vec![11],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![values_distributed_node(0, 10, vec![group_column.clone()])],
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::HashAggregate(Box::new(
                    crate::sql::planner::plan::PhysicalHashAggregateNode {
                        mode: crate::sql::planner::AggMode::Local,
                        group_by: vec![column_expr(2, "c1", DataType::Utf8)],
                        aggregates: Vec::new(),
                        is_merge: Vec::new(),
                        output_layout: crate::sql::planner::AggregateOutputLayout::new(
                            vec![group_column.clone()],
                            Vec::new(),
                        ),
                        output_columns: Vec::new(),
                    },
                )),
            ),
        };
        let source = crate::sql::planner::PlanFragment {
            fragment_id: 0,
            root: source_root,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Noop,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let receiver = crate::sql::planner::DistributedNode {
            node_id: 42,
            fragment_id: 1,
            tuple_ids: vec![42],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Exchange(
                crate::sql::planner::ExchangeReceiver {
                    partition: crate::sql::planner::DataPartition::unpartitioned(),
                    source_fragment_id: 0,
                    output_columns: Vec::new(),
                    output_qualifier: None,
                    flavor: crate::sql::planner::plan::ExchangeFlavor::Distribution,
                },
            ),
        };
        let target = crate::sql::planner::PlanFragment {
            fragment_id: 1,
            root: receiver,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 1,
            edges: vec![crate::sql::codegen::FragmentEdge {
                source_fragment_id: 0,
                target_fragment_id: 1,
                target_exchange_node_id: 42,
                output_partition: crate::sql::planner::DataPartition::unpartitioned(),
                stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
                edge_kind: crate::sql::codegen::FragmentEdgeKind::Stream,
                output_slot_ids: vec![2],
            }],
        };

        let encoded = plan::encode_distributed_plan(&plan).expect("encode distributed plan");
        let target_fragment = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("target fragment");
        let root = target_fragment.root.as_ref().expect("target root");
        let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
            root.payload.as_ref()
        else {
            panic!("expected exchange receiver");
        };
        assert_eq!(exchange.output_columns.len(), 1);
        assert_eq!(exchange.output_columns[0].column_id, 2);
        assert_eq!(exchange.output_columns[0].name, "c1");
    }

    #[test]
    fn stream_edge_patches_local_avg_exchange_schema_to_intermediate_type() {
        let group_column = planner_output_column(2, "c0", DataType::Int64);
        let value_column = planner_output_column(3, "c1", DataType::Int64);
        let avg_column = planner_output_column(15, "avg(c1)", DataType::Float64);
        let source_root = crate::sql::planner::DistributedNode {
            node_id: 11,
            fragment_id: 0,
            tuple_ids: vec![11],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![values_distributed_node(
                0,
                10,
                vec![group_column.clone(), value_column.clone()],
            )],
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::HashAggregate(Box::new(
                    crate::sql::planner::plan::PhysicalHashAggregateNode {
                        mode: crate::sql::planner::AggMode::Local,
                        group_by: vec![column_expr(2, "c0", DataType::Int64)],
                        aggregates: vec![crate::sql::planner::plan::AggregateCall {
                            name: "avg".to_string(),
                            args: vec![column_expr(3, "c1", DataType::Int64)],
                            distinct: false,
                            result_type: DataType::Float64,
                            order_by: Vec::new(),
                            output_column_id: crate::sql::column_id::ColumnId::new_for_test(15),
                        }],
                        is_merge: vec![false],
                        output_layout: crate::sql::planner::AggregateOutputLayout::new(
                            vec![group_column.clone()],
                            vec![avg_column.clone()],
                        ),
                        output_columns: Vec::new(),
                    },
                )),
            ),
        };
        let source = crate::sql::planner::PlanFragment {
            fragment_id: 0,
            root: source_root,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Noop,
            output_exprs: None,
            output_columns: vec![group_column.clone(), avg_column],
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let receiver = crate::sql::planner::DistributedNode {
            node_id: 42,
            fragment_id: 1,
            tuple_ids: vec![42],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Exchange(
                crate::sql::planner::ExchangeReceiver {
                    partition: crate::sql::planner::DataPartition::unpartitioned(),
                    source_fragment_id: 0,
                    output_columns: Vec::new(),
                    output_qualifier: None,
                    flavor: crate::sql::planner::plan::ExchangeFlavor::Distribution,
                },
            ),
        };
        let target = crate::sql::planner::PlanFragment {
            fragment_id: 1,
            root: receiver,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 1,
            edges: vec![crate::sql::codegen::FragmentEdge {
                source_fragment_id: 0,
                target_fragment_id: 1,
                target_exchange_node_id: 42,
                output_partition: crate::sql::planner::DataPartition::unpartitioned(),
                stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
                edge_kind: crate::sql::codegen::FragmentEdgeKind::Stream,
                output_slot_ids: vec![2, 15],
            }],
        };

        let encoded = plan::encode_distributed_plan(&plan).expect("encode distributed plan");
        let target_fragment = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("target fragment");
        let root = target_fragment.root.as_ref().expect("target root");
        let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
            root.payload.as_ref()
        else {
            panic!("expected exchange receiver");
        };
        assert_eq!(exchange.output_columns.len(), 2);
        assert_eq!(exchange.output_columns[1].column_id, 15);
        let avg_type = decode_type(
            exchange.output_columns[1]
                .r#type
                .as_ref()
                .expect("avg column type"),
        )
        .expect("decode avg column type");
        assert_eq!(avg_type, DataType::Utf8);
    }

    #[test]
    fn stream_edge_allows_zero_column_source_when_no_slots_are_requested() {
        let source = crate::sql::planner::PlanFragment {
            fragment_id: 0,
            root: values_distributed_node(0, 10, Vec::new()),
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Noop,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let receiver = crate::sql::planner::DistributedNode {
            node_id: 42,
            fragment_id: 1,
            tuple_ids: vec![42],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Exchange(
                crate::sql::planner::ExchangeReceiver {
                    partition: crate::sql::planner::DataPartition::unpartitioned(),
                    source_fragment_id: 0,
                    output_columns: Vec::new(),
                    output_qualifier: None,
                    flavor: crate::sql::planner::plan::ExchangeFlavor::Distribution,
                },
            ),
        };
        let target = crate::sql::planner::PlanFragment {
            fragment_id: 1,
            root: receiver,
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::Result,
            output_exprs: None,
            output_columns: Vec::new(),
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };
        let plan = crate::sql::planner::DistributedPlan {
            fragments: vec![source, target],
            root_fragment_id: 1,
            edges: vec![crate::sql::codegen::FragmentEdge {
                source_fragment_id: 0,
                target_fragment_id: 1,
                target_exchange_node_id: 42,
                output_partition: crate::sql::planner::DataPartition::unpartitioned(),
                stream_kind: crate::sql::codegen::FragmentStreamKind::Gather,
                edge_kind: crate::sql::codegen::FragmentEdgeKind::Stream,
                output_slot_ids: Vec::new(),
            }],
        };

        let encoded = plan::encode_distributed_plan(&plan).expect("encode distributed plan");
        let target_fragment = encoded
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == 1)
            .expect("target fragment");
        let root = target_fragment.root.as_ref().expect("target root");
        let Some(crate::proto::plan::distributed_node::Payload::Exchange(exchange)) =
            root.payload.as_ref()
        else {
            panic!("expected exchange receiver");
        };
        assert!(exchange.output_columns.is_empty());
    }

    #[test]
    fn iceberg_write_fragment_uses_sink_output_contract_for_duplicate_input_columns() {
        let mut sink_spec = crate::sql::planner::write_sink::test_support::simple_sink_spec();
        sink_spec.target_columns = vec![
            crate::sql::catalog::ColumnDef {
                name: "c0".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            crate::sql::catalog::ColumnDef {
                name: "c1".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
        ];
        sink_spec.target_table.columns = sink_spec.target_columns.clone();

        let repeated_input = vec![
            planner_output_column(7, "g0", DataType::Int64),
            planner_output_column(7, "g1", DataType::Int64),
        ];
        let fragment = crate::sql::planner::PlanFragment {
            fragment_id: 0,
            root: values_distributed_node(0, 11, repeated_input.clone()),
            data_partition: crate::sql::planner::DataPartition::unpartitioned(),
            output_partition: crate::sql::planner::DataPartition::unpartitioned(),
            sink: crate::sql::planner::DataSink::IcebergWrite(
                crate::sql::planner::IcebergWriteFragmentSink {
                    descriptor_database: "db".to_string(),
                    spec: sink_spec,
                    input: crate::sql::planner::IcebergWriteInputBinding::RootOutputByOrdinal,
                },
            ),
            output_exprs: None,
            output_columns: repeated_input,
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        };

        let encoded = plan::encode_plan_fragment(&fragment).expect("encode fragment");

        assert_eq!(encoded.output_exprs.len(), 2);
        let encoded_ids = encoded
            .output_exprs
            .iter()
            .map(|expr| {
                let Some(crate::proto::expr::expr::Kind::ColumnRef(column)) = expr.kind.as_ref()
                else {
                    panic!("expected column ref");
                };
                column.column_id
            })
            .collect::<Vec<_>>();
        assert_eq!(encoded_ids, vec![7, 7]);

        let output_ids = encoded
            .output_columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        assert_eq!(output_ids, vec![1, 2]);
        assert_eq!(
            encoded
                .output_columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c0", "c1"]
        );
    }

    #[test]
    fn native_scan_encoder_preserves_iceberg_write_defaults() {
        let schema = crate::sql::catalog::IcebergSchemaDef {
            fields: vec![crate::sql::catalog::IcebergSchemaFieldDef {
                field_id: 1,
                name: "amount".to_string(),
                initial_default: Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(5),
                )),
                write_default: Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int(7),
                )),
                initial_default_json: Some("5".to_string()),
                write_default_json: Some("7".to_string()),
                children: vec![],
            }],
        };
        let iceberg_table = crate::sql::catalog::IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("uuid-orders".to_string()),
            current_snapshot_id: Some(10),
            schema_id: 1,
            location: "s3://warehouse/db/orders".to_string(),
            schema,
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let table = crate::sql::catalog::TableDef {
            name: "orders".to_string(),
            columns: vec![crate::sql::catalog::ColumnDef {
                name: "amount".to_string(),
                data_type: DataType::Decimal128(10, 2),
                nullable: true,
                write_default: Some(iceberg::spec::Literal::Primitive(
                    iceberg::spec::PrimitiveLiteral::Int128(999),
                )),
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::sql::catalog::ScanSource::IcebergDataFiles {
                table: iceberg_table,
                files: vec![],
                cloud_properties: std::collections::BTreeMap::new(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let scan = crate::sql::planner::DistributedNode {
            node_id: 7,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::Scan(
                    crate::sql::planner::plan::PlanScanNode {
                        database: "db".to_string(),
                        table,
                        alias: None,
                        columns: vec![planner_output_column(
                            10,
                            "amount",
                            DataType::Decimal128(10, 2),
                        )],
                        predicates: Vec::new(),
                        required_columns: None,
                        variant_columns: Vec::new(),
                        mv_rewritten_from: None,
                    },
                ),
            ),
        };

        let encoded = plan::encode_node(&scan).expect("encode scan node");
        let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
            encoded.payload.as_ref()
        else {
            panic!("expected physical node");
        };
        let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan node");
        };
        let table = scan.table.as_ref().expect("table");

        assert_eq!(
            table.columns[0].write_default_json.as_deref(),
            Some("\"9.99\"")
        );
        let source = table.source.as_ref().expect("scan source");
        let Some(crate::proto::plan::scan_source::Kind::IcebergDataFiles(iceberg)) =
            source.kind.as_ref()
        else {
            panic!("expected Iceberg data-files source");
        };
        let field = &iceberg
            .table
            .as_ref()
            .expect("iceberg table")
            .schema
            .as_ref()
            .expect("iceberg schema")
            .fields[0];
        assert_eq!(field.initial_default_json.as_deref(), Some("5"));
        assert_eq!(field.write_default_json.as_deref(), Some("7"));
    }

    #[test]
    fn native_scan_encoder_preserves_iceberg_list_write_defaults_from_arrow_metadata() {
        let list_type = DataType::List(Arc::new(
            Field::new("element", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "4".to_string(),
            )])),
        ));
        let table = crate::sql::catalog::TableDef {
            name: "orders".to_string(),
            columns: vec![crate::sql::catalog::ColumnDef {
                name: "tags".to_string(),
                data_type: list_type.clone(),
                nullable: true,
                write_default: Some(iceberg::spec::Literal::List(vec![])),
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::sql::catalog::ScanSource::IcebergDataFiles {
                table: crate::sql::catalog::IcebergTableInfo {
                    catalog: "ice".to_string(),
                    namespace: "db".to_string(),
                    table: "orders".to_string(),
                    table_uuid: Some("uuid-orders".to_string()),
                    current_snapshot_id: Some(10),
                    schema_id: 1,
                    location: "s3://warehouse/db/orders".to_string(),
                    schema: crate::sql::catalog::IcebergSchemaDef { fields: vec![] },
                    serialized_metadata: None,
                    serialized_metadata_rows: None,
                },
                files: vec![],
                cloud_properties: std::collections::BTreeMap::new(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        };
        let scan = crate::sql::planner::DistributedNode {
            node_id: 7,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::Scan(
                    crate::sql::planner::plan::PlanScanNode {
                        database: "db".to_string(),
                        table,
                        alias: None,
                        columns: vec![planner_output_column(10, "tags", list_type)],
                        predicates: Vec::new(),
                        required_columns: None,
                        variant_columns: Vec::new(),
                        mv_rewritten_from: None,
                    },
                ),
            ),
        };

        let encoded = plan::encode_node(&scan).expect("encode scan node");
        let Some(crate::proto::plan::distributed_node::Payload::Physical(physical)) =
            encoded.payload.as_ref()
        else {
            panic!("expected physical node");
        };
        let Some(crate::proto::plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref() else {
            panic!("expected scan node");
        };
        let table = scan.table.as_ref().expect("table");

        assert_eq!(table.columns[0].write_default_json.as_deref(), Some("[]"));
    }

    #[test]
    fn native_plan_encoder_rejects_starrocks_scan_source() {
        let scan = crate::sql::planner::DistributedNode {
            node_id: 7,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: physical_stats(),
            payload: crate::sql::planner::DistributedPayload::Physical(
                crate::sql::planner::plan::PhysicalPlanKind::Scan(
                    crate::sql::planner::plan::PlanScanNode {
                        database: "db".to_string(),
                        table: crate::sql::catalog::TableDef {
                            name: "sr_table".to_string(),
                            columns: Vec::new(),
                            iceberg_row_lineage_metadata_columns: Vec::new(),
                            source: crate::sql::catalog::ScanSource::StarRocks {
                                db_id: 1,
                                table_id: 2,
                            },
                        },
                        alias: None,
                        columns: Vec::new(),
                        predicates: Vec::new(),
                        required_columns: None,
                        variant_columns: Vec::new(),
                        mv_rewritten_from: None,
                    },
                ),
            ),
        };

        let err = plan::encode_node(&scan).expect_err("StarRocks native scan must fail fast");

        assert!(err.contains("StarRocks"), "{err}");
        assert!(err.contains("native"), "{err}");
    }

    #[test]
    fn physical_plan_encoder_variant_guard_tracks_rust_enum_not_proto_arms() {
        assert_eq!(
            plan::encoded_physical_variant_names_for_test(),
            crate::sql::planner::plan::PhysicalPlanKind::variant_names_for_test()
        );
        assert!(
            !plan::encoded_physical_variant_names_for_test().contains(&"Decode"),
            "Decode exists only as a proto arm; Rust PhysicalPlanKind is the source of truth"
        );
    }

    #[test]
    fn instance_params_encoder_maps_scan_ranges_destinations_rf_and_query_options() {
        use std::collections::BTreeMap;

        let mut scan_range = crate::runtime::scan_range::ScanRangeParams::file(
            crate::runtime::scan_range::FileScanRange {
                file_format: crate::runtime::scan_range::FileFormat::Parquet,
                full_path: Some("s3://bucket/data.parquet".to_string()),
                relative_path: Some("data.parquet".to_string()),
                table_id: Some(99),
                offset: 8,
                length: 16,
                file_length: 128,
                delete_files: Vec::new(),
                deletion_vector_descriptor: None,
                first_row_id: Some(1_000),
                data_sequence_number: Some(44),
                modification_time: None,
                datacache_options: None,
                included_positions: vec![3, 5, 8],
                serialized_split: Some("{\"split\":1}".to_string()),
                use_iceberg_jni_metadata_reader: true,
                ivm_change_op: Some(crate::exec::change_op::CHANGE_OP_DELETE),
                file_pruning_min_max_values: Some(BTreeMap::from([(
                    0,
                    crate::runtime::scan_range::FilePruningMinMaxValue {
                        value_kind: crate::runtime::scan_range::FilePruningValueKind::Int,
                        has_null: false,
                        all_null: false,
                        min_int_value: Some(10),
                        max_int_value: Some(20),
                        min_float_value: None,
                        max_float_value: None,
                    },
                )])),
                extended_columns: None,
            },
        );
        scan_range.volume_id = Some(13);
        scan_range.empty = Some(true);
        scan_range.has_more = Some(false);
        let mut scan_ranges = BTreeMap::new();
        scan_ranges.insert(11, vec![scan_range]);
        let destination = crate::runtime::endpoint::FragmentDestination::new(
            crate::thrift::types::TUniqueId::new(3, 4),
            crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.9", 8060)
                .expect("destination endpoint"),
        );
        let mut per_exch_num_senders = BTreeMap::new();
        per_exch_num_senders.insert(42, 2);
        let placement = crate::runtime::scheduler::FragmentInstancePlacement {
            fragment_id: 0,
            instance_index: 5,
            finst_id: crate::thrift::types::TUniqueId::new(1, 2),
            backend_idx: 7,
            endpoint: crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.7", 8060)
                .expect("placement endpoint"),
            scan_ranges,
            destinations: vec![destination],
            runtime_filter_prober_params: BTreeMap::new(),
            per_exch_num_senders,
        };
        let query_options = crate::runtime::query_options::QueryOptions {
            batch_size: Some(4096),
            query_timeout: Some(60),
            query_delivery_timeout: Some(30),
            enable_profile: true,
            runtime_profile_report_interval: Some(7),
            pipeline_dop: Some(8),
            exec_mem_limit: Some(1 << 20),
            connector_io_tasks_per_scan_operator: Some(12),
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(5_000),
            allow_throw_exception: true,
            group_concat_max_len: Some(65_535),
            enable_join_runtime_bitset_filter: Some(false),
            global_runtime_filter_build_max_size: Some(1 << 19),
            cache: crate::runtime::query_options::QueryCacheOptions {
                enable_scan_datacache: true,
                enable_populate_datacache: true,
                enable_datacache_async_populate_mode: true,
                enable_datacache_io_adaptor: true,
                enable_cache_select: true,
                datacache_evict_probability: Some(75),
                datacache_priority: Some(2),
                datacache_ttl_seconds: Some(3600),
                datacache_sharing_work_period: Some(10),
            },
            ..Default::default()
        };
        let runtime_filter_prober_params = BTreeMap::from([(
            9,
            vec![
                crate::runtime::endpoint::RuntimeFilterProberDestination::new(
                    crate::thrift::types::TUniqueId::new(30, 40),
                    crate::runtime::endpoint::RuntimeEndpoint::new("10.0.0.30", 8060)
                        .expect("prober endpoint"),
                ),
            ],
        )]);
        let runtime_filter_builder_number = BTreeMap::from([(9, 3)]);

        let encoded = instance::encode_instance_params(
            &crate::thrift::types::TUniqueId::new(100, 200),
            &placement,
            Some(&query_options),
            &runtime_filter_prober_params,
            &runtime_filter_builder_number,
            1 << 18,
            5,
            Some(
                &crate::runtime::endpoint::RuntimeEndpoint::new("127.0.0.1", 9030)
                    .expect("report endpoint"),
            ),
            true,
        )
        .expect("encode instance params");

        assert_eq!(encoded.query_id.as_ref().expect("query id").hi, 100);
        assert_eq!(
            encoded
                .fragment_instance_id
                .as_ref()
                .expect("fragment instance id")
                .lo,
            2
        );
        assert_eq!(encoded.backend_num, 5);
        assert_eq!(encoded.per_exch_num_senders.get(&42), Some(&2));
        assert_eq!(encoded.destinations[0].endpoint, "10.0.0.9:8060");
        assert_eq!(encoded.report_endpoint.as_deref(), Some("127.0.0.1:9030"));
        assert!(encoded.typed_result_sink);
        let encoded_range = &encoded.per_node_scan_ranges[&11].ranges[0];
        assert_eq!(encoded_range.volume_id, Some(13));
        assert_eq!(encoded_range.empty, Some(true));
        assert_eq!(encoded_range.has_more, Some(false));
        let crate::proto::novarocks::scan_range::Kind::File(file) = encoded_range
            .range
            .as_ref()
            .and_then(|range| range.kind.as_ref())
            .expect("scan range kind");
        assert_eq!(file.file_format, "PARQUET");
        assert_eq!(file.full_path.as_deref(), Some("s3://bucket/data.parquet"));
        assert_eq!(file.included_positions, vec![3, 5, 8]);
        assert!(file.use_iceberg_jni_metadata_reader);
        assert_eq!(
            file.change_op,
            Some(i32::from(crate::exec::change_op::CHANGE_OP_DELETE))
        );
        let pruning = file
            .file_pruning_min_max_values
            .get(&0)
            .expect("file pruning stats");
        assert_eq!(pruning.value_kind, 2);
        assert_eq!(pruning.min_int_value, Some(10));
        assert_eq!(pruning.max_int_value, Some(20));
        let rf = encoded
            .runtime_filter_params
            .as_ref()
            .expect("runtime filter params");
        assert_eq!(rf.runtime_filter_builder_number.get(&9), Some(&3));
        assert_eq!(
            rf.id_to_prober_params[&9].params[0].endpoint,
            "10.0.0.30:8060"
        );
        let opts = encoded.query_options.as_ref().expect("query options");
        assert_eq!(opts.batch_size, 4096);
        assert_eq!(opts.query_timeout, 60);
        assert_eq!(opts.query_delivery_timeout, 30);
        assert_eq!(opts.runtime_profile_report_interval, 7);
        assert_eq!(opts.pipeline_dop, 8);
        assert_eq!(opts.query_mem_limit, 1 << 20);
        assert_eq!(opts.runtime_filter_wait_timeout_ms, Some(5_000));
        assert!(opts.enable_scan_datacache);
        assert_eq!(opts.datacache_evict_probability, Some(75));
        assert_eq!(opts.datacache_sharing_work_period, 10);
        assert_eq!(opts.enable_join_runtime_bitset_filter, Some(false));
        assert_eq!(opts.global_runtime_filter_build_max_size, 1 << 19);
    }
}
