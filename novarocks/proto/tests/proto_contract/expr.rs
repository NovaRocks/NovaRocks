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

use prost::Message;

use novarocks_proto::{common, expr as expr_proto};

#[derive(Clone, Debug, PartialEq, Eq)]
struct IType {
    prim: i32,
}

#[derive(Clone, Debug, PartialEq)]
struct IExpr {
    ty: IType,
    nullable: bool,
    kind: IExprKind,
}

#[derive(Clone, Debug, PartialEq)]
enum IExprKind {
    ColumnRef {
        column_id: u32,
        qualifier: Option<String>,
        column: Option<String>,
    },
    Literal(ILiteral),
    BinaryOp {
        op: i32,
        left: Box<IExpr>,
        right: Box<IExpr>,
    },
    FunctionCall {
        function_name: String,
        args: Vec<IExpr>,
        distinct: bool,
    },
    Cast {
        operand: Box<IExpr>,
        target: IType,
    },
    Case {
        operand: Option<Box<IExpr>>,
        when_then: Vec<(IExpr, IExpr)>,
        else_expr: Option<Box<IExpr>>,
    },
    InList {
        operand: Box<IExpr>,
        list: Vec<IExpr>,
        negated: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum ILiteral {
    Null,
    Bool(bool),
    Int(i64),
    String(String),
}

fn roundtrip_message<M>(value: &M) -> M
where
    M: Message + Default,
{
    M::decode(value.encode_to_vec().as_slice()).expect("decode proto message")
}

fn scalar_type(prim: common::PrimitiveType) -> IType {
    IType { prim: prim as i32 }
}

fn type_to_proto(ty: &IType) -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: ty.prim,
            len: None,
            precision: None,
            scale: None,
            time_unit: None,
        })),
    }
}

fn type_from_proto(proto: &common::TypeDesc) -> Result<IType, String> {
    let kind = proto.kind.as_ref().ok_or("Expr.type.kind missing")?;
    match kind {
        common::type_desc::Kind::Scalar(scalar) => Ok(IType {
            prim: scalar.r#type,
        }),
        _ => Err("expr test analogue only accepts scalar TypeDesc".to_string()),
    }
}

fn literal_to_proto(literal: &ILiteral) -> common::LiteralValue {
    use common::literal_value::Value;

    common::LiteralValue {
        value: Some(match literal {
            ILiteral::Null => Value::NullValue(true),
            ILiteral::Bool(value) => Value::BoolValue(*value),
            ILiteral::Int(value) => Value::IntValue(*value),
            ILiteral::String(value) => Value::StringValue(value.clone()),
        }),
    }
}

fn literal_from_proto(proto: &common::LiteralValue) -> Result<ILiteral, String> {
    use common::literal_value::Value;

    match proto.value.as_ref().ok_or("LiteralValue.value missing")? {
        Value::NullValue(true) => Ok(ILiteral::Null),
        Value::NullValue(false) => Err("LiteralValue.null_value false is invalid".to_string()),
        Value::BoolValue(value) => Ok(ILiteral::Bool(*value)),
        Value::IntValue(value) => Ok(ILiteral::Int(*value)),
        Value::StringValue(value) => Ok(ILiteral::String(value.clone())),
        other => Err(format!(
            "literal arm not covered by expr analogue: {other:?}"
        )),
    }
}

fn expr_to_proto(expr: &IExpr) -> expr_proto::Expr {
    use expr_proto::expr::Kind;

    let kind = match &expr.kind {
        IExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => Kind::ColumnRef(expr_proto::ColumnRef {
            column_id: *column_id,
            qualifier: qualifier.clone(),
            column: column.clone(),
        }),
        IExprKind::Literal(value) => Kind::Literal(expr_proto::LiteralExpr {
            value: Some(literal_to_proto(value)),
        }),
        IExprKind::BinaryOp { op, left, right } => {
            Kind::BinaryOp(Box::new(expr_proto::BinaryOpExpr {
                op: *op,
                left: Some(Box::new(expr_to_proto(left))),
                right: Some(Box::new(expr_to_proto(right))),
            }))
        }
        IExprKind::FunctionCall {
            function_name,
            args,
            distinct,
        } => Kind::FunctionCall(expr_proto::FunctionCall {
            function_name: function_name.clone(),
            args: args.iter().map(expr_to_proto).collect(),
            distinct: *distinct,
        }),
        IExprKind::Cast { operand, target } => Kind::Cast(Box::new(expr_proto::CastExpr {
            operand: Some(Box::new(expr_to_proto(operand))),
            target: Some(type_to_proto(target)),
        })),
        IExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => Kind::CaseExpr(Box::new(expr_proto::CaseExpr {
            operand: operand
                .as_ref()
                .map(|operand| Box::new(expr_to_proto(operand))),
            when_then: when_then
                .iter()
                .map(|(when, then)| expr_proto::WhenThen {
                    when: Some(expr_to_proto(when)),
                    then: Some(expr_to_proto(then)),
                })
                .collect(),
            else_expr: else_expr
                .as_ref()
                .map(|else_expr| Box::new(expr_to_proto(else_expr))),
        })),
        IExprKind::InList {
            operand,
            list,
            negated,
        } => Kind::InList(Box::new(expr_proto::InListExpr {
            operand: Some(Box::new(expr_to_proto(operand))),
            list: list.iter().map(expr_to_proto).collect(),
            negated: *negated,
        })),
    };

    expr_proto::Expr {
        r#type: Some(type_to_proto(&expr.ty)),
        nullable: expr.nullable,
        kind: Some(kind),
    }
}

fn expr_from_proto(proto: &expr_proto::Expr) -> Result<IExpr, String> {
    use expr_proto::expr::Kind;

    let ty = type_from_proto(proto.r#type.as_ref().ok_or("Expr.type missing")?)?;
    let kind = match proto.kind.as_ref().ok_or("Expr.kind missing")? {
        Kind::ColumnRef(column_ref) => IExprKind::ColumnRef {
            column_id: column_ref.column_id,
            qualifier: column_ref.qualifier.clone(),
            column: column_ref.column.clone(),
        },
        Kind::Literal(literal) => IExprKind::Literal(literal_from_proto(
            literal.value.as_ref().ok_or("LiteralExpr.value missing")?,
        )?),
        Kind::BinaryOp(binary) => IExprKind::BinaryOp {
            op: binary.op,
            left: Box::new(expr_from_proto(
                binary.left.as_ref().ok_or("BinaryOpExpr.left missing")?,
            )?),
            right: Box::new(expr_from_proto(
                binary.right.as_ref().ok_or("BinaryOpExpr.right missing")?,
            )?),
        },
        Kind::FunctionCall(call) => IExprKind::FunctionCall {
            function_name: call.function_name.clone(),
            args: call
                .args
                .iter()
                .map(expr_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
            distinct: call.distinct,
        },
        Kind::Cast(cast) => IExprKind::Cast {
            operand: Box::new(expr_from_proto(
                cast.operand.as_ref().ok_or("CastExpr.operand missing")?,
            )?),
            target: type_from_proto(cast.target.as_ref().ok_or("CastExpr.target missing")?)?,
        },
        Kind::CaseExpr(case_expr) => IExprKind::Case {
            operand: case_expr
                .operand
                .as_ref()
                .map(|operand| expr_from_proto(operand).map(Box::new))
                .transpose()?,
            when_then: case_expr
                .when_then
                .iter()
                .map(|branch| {
                    let when =
                        expr_from_proto(branch.when.as_ref().ok_or("WhenThen.when missing")?)?;
                    let then =
                        expr_from_proto(branch.then.as_ref().ok_or("WhenThen.then missing")?)?;
                    Ok((when, then))
                })
                .collect::<Result<Vec<_>, String>>()?,
            else_expr: case_expr
                .else_expr
                .as_ref()
                .map(|else_expr| expr_from_proto(else_expr).map(Box::new))
                .transpose()?,
        },
        Kind::InList(in_list) => IExprKind::InList {
            operand: Box::new(expr_from_proto(
                in_list
                    .operand
                    .as_ref()
                    .ok_or("InListExpr.operand missing")?,
            )?),
            list: in_list
                .list
                .iter()
                .map(expr_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
            negated: in_list.negated,
        },
        other => {
            return Err(format!(
                "expr analogue intentionally does not cover {other:?}"
            ));
        }
    };

    Ok(IExpr {
        ty,
        nullable: proto.nullable,
        kind,
    })
}

fn int_expr(value: i64) -> IExpr {
    IExpr {
        ty: scalar_type(common::PrimitiveType::Bigint),
        nullable: false,
        kind: IExprKind::Literal(ILiteral::Int(value)),
    }
}

fn string_expr(value: &str) -> IExpr {
    IExpr {
        ty: scalar_type(common::PrimitiveType::Varchar),
        nullable: false,
        kind: IExprKind::Literal(ILiteral::String(value.to_string())),
    }
}

fn column_expr(column_id: u32, name: &str) -> IExpr {
    IExpr {
        ty: scalar_type(common::PrimitiveType::Bigint),
        nullable: true,
        kind: IExprKind::ColumnRef {
            column_id,
            qualifier: Some("lineitem".to_string()),
            column: Some(name.to_string()),
        },
    }
}

fn sample_internal_expr() -> IExpr {
    let quantity = column_expr(7, "l_quantity");
    let discount = column_expr(8, "l_discount");

    IExpr {
        ty: scalar_type(common::PrimitiveType::Bigint),
        nullable: true,
        kind: IExprKind::Case {
            operand: None,
            when_then: vec![(
                IExpr {
                    ty: scalar_type(common::PrimitiveType::Boolean),
                    nullable: false,
                    kind: IExprKind::BinaryOp {
                        op: expr_proto::BinaryOp::Gt as i32,
                        left: Box::new(quantity.clone()),
                        right: Box::new(int_expr(10)),
                    },
                },
                IExpr {
                    ty: scalar_type(common::PrimitiveType::Bigint),
                    nullable: true,
                    kind: IExprKind::Cast {
                        operand: Box::new(IExpr {
                            ty: scalar_type(common::PrimitiveType::Double),
                            nullable: true,
                            kind: IExprKind::FunctionCall {
                                function_name: "multiply".to_string(),
                                args: vec![quantity.clone(), discount],
                                distinct: false,
                            },
                        }),
                        target: scalar_type(common::PrimitiveType::Bigint),
                    },
                },
            )],
            else_expr: Some(Box::new(IExpr {
                ty: scalar_type(common::PrimitiveType::Boolean),
                nullable: false,
                kind: IExprKind::InList {
                    operand: Box::new(string_expr("returnflag")),
                    list: vec![string_expr("R"), string_expr("A")],
                    negated: true,
                },
            })),
        },
    }
}

fn sample_proto_expr() -> expr_proto::Expr {
    expr_to_proto(&column_expr(1, "l_orderkey"))
}

fn classify_kind(kind: expr_proto::expr::Kind) -> &'static str {
    match kind {
        expr_proto::expr::Kind::ColumnRef(_) => "column_ref",
        expr_proto::expr::Kind::Literal(_) => "literal",
        expr_proto::expr::Kind::BinaryOp(_) => "binary_op",
        expr_proto::expr::Kind::UnaryOp(_) => "unary_op",
        expr_proto::expr::Kind::FunctionCall(_) => "function_call",
        expr_proto::expr::Kind::AggregateCall(_) => "aggregate_call",
        expr_proto::expr::Kind::WindowCall(_) => "window_call",
        expr_proto::expr::Kind::Cast(_) => "cast",
        expr_proto::expr::Kind::IsNull(_) => "is_null",
        expr_proto::expr::Kind::InList(_) => "in_list",
        expr_proto::expr::Kind::Between(_) => "between",
        expr_proto::expr::Kind::Like(_) => "like",
        expr_proto::expr::Kind::CaseExpr(_) => "case_expr",
        expr_proto::expr::Kind::IsTruth(_) => "is_truth",
        expr_proto::expr::Kind::LambdaParamRef(_) => "lambda_param_ref",
        expr_proto::expr::Kind::Lambda(_) => "lambda",
        expr_proto::expr::Kind::Nested(_) => "nested",
    }
}

#[test]
fn recursive_expr_survives_proto_roundtrip() {
    let original = sample_internal_expr();
    let proto = expr_to_proto(&original);

    let decoded: expr_proto::Expr = roundtrip_message(&proto);
    assert_eq!(proto, decoded);

    let back = expr_from_proto(&decoded).expect("convert Expr back");
    assert_eq!(original, back);
}

#[test]
fn missing_expr_fields_report_boundary_errors() {
    let missing_type = expr_proto::Expr {
        r#type: None,
        nullable: false,
        kind: Some(expr_proto::expr::Kind::ColumnRef(expr_proto::ColumnRef {
            column_id: 1,
            qualifier: None,
            column: None,
        })),
    };
    assert_eq!(
        expr_from_proto(&missing_type).expect_err("missing type"),
        "Expr.type missing"
    );

    let missing_kind = expr_proto::Expr {
        r#type: Some(type_to_proto(&scalar_type(common::PrimitiveType::Boolean))),
        nullable: false,
        kind: None,
    };
    assert_eq!(
        expr_from_proto(&missing_kind).expect_err("missing kind"),
        "Expr.kind missing"
    );
}

#[test]
fn expr_kind_match_is_exhaustive_over_current_oneof() {
    use expr_proto::expr::Kind;

    let expr = sample_proto_expr();
    let ty = Some(type_to_proto(&scalar_type(common::PrimitiveType::Boolean)));
    let sort_item = expr_proto::SortItem {
        expr: Some(expr.clone()),
        asc: true,
        nulls_first: false,
    };
    let window_bound = expr_proto::WindowBound {
        bound: Some(expr_proto::window_bound::Bound::CurrentRow(true)),
    };

    let kinds = vec![
        Kind::ColumnRef(expr_proto::ColumnRef {
            column_id: 1,
            qualifier: None,
            column: Some("c1".to_string()),
        }),
        Kind::Literal(expr_proto::LiteralExpr {
            value: Some(literal_to_proto(&ILiteral::Null)),
        }),
        Kind::BinaryOp(Box::new(expr_proto::BinaryOpExpr {
            op: expr_proto::BinaryOp::Add as i32,
            left: Some(Box::new(expr.clone())),
            right: Some(Box::new(expr.clone())),
        })),
        Kind::UnaryOp(Box::new(expr_proto::UnaryOpExpr {
            op: expr_proto::UnaryOp::Not as i32,
            operand: Some(Box::new(expr.clone())),
        })),
        Kind::FunctionCall(expr_proto::FunctionCall {
            function_name: "abs".to_string(),
            args: vec![expr.clone()],
            distinct: false,
        }),
        Kind::AggregateCall(expr_proto::AggregateCall {
            function_name: "sum".to_string(),
            args: vec![expr.clone()],
            distinct: true,
            order_by: vec![sort_item.clone()],
        }),
        Kind::WindowCall(expr_proto::WindowCall {
            function_name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by: vec![expr.clone()],
            order_by: vec![sort_item],
            frame: Some(expr_proto::WindowFrame {
                frame_type: expr_proto::WindowFrameType::Rows as i32,
                start: Some(window_bound),
                end: Some(expr_proto::WindowBound {
                    bound: Some(expr_proto::window_bound::Bound::UnboundedFollowing(true)),
                }),
            }),
            ignore_nulls: false,
        }),
        Kind::Cast(Box::new(expr_proto::CastExpr {
            operand: Some(Box::new(expr.clone())),
            target: ty.clone(),
        })),
        Kind::IsNull(Box::new(expr_proto::IsNullExpr {
            operand: Some(Box::new(expr.clone())),
            negated: true,
        })),
        Kind::InList(Box::new(expr_proto::InListExpr {
            operand: Some(Box::new(expr.clone())),
            list: vec![expr.clone()],
            negated: false,
        })),
        Kind::Between(Box::new(expr_proto::BetweenExpr {
            operand: Some(Box::new(expr.clone())),
            low: Some(Box::new(expr.clone())),
            high: Some(Box::new(expr.clone())),
            negated: false,
        })),
        Kind::Like(Box::new(expr_proto::LikeExpr {
            operand: Some(Box::new(expr.clone())),
            pattern: Some(Box::new(expr.clone())),
            negated: false,
        })),
        Kind::CaseExpr(Box::new(expr_proto::CaseExpr {
            operand: Some(Box::new(expr.clone())),
            when_then: vec![expr_proto::WhenThen {
                when: Some(expr.clone()),
                then: Some(expr.clone()),
            }],
            else_expr: Some(Box::new(expr.clone())),
        })),
        Kind::IsTruth(Box::new(expr_proto::IsTruthExpr {
            operand: Some(Box::new(expr.clone())),
            value: true,
            negated: false,
        })),
        Kind::LambdaParamRef(expr_proto::LambdaParamRef {
            slot_id: 3,
            name: Some("x".to_string()),
        }),
        Kind::Lambda(Box::new(expr_proto::LambdaExpr {
            params: vec![expr_proto::LambdaParam {
                slot_id: 3,
                name: Some("x".to_string()),
                r#type: Some(type_to_proto(&scalar_type(common::PrimitiveType::Bigint))),
                nullable: true,
            }],
            body: Some(Box::new(expr.clone())),
        })),
        Kind::Nested(Box::new(expr_proto::NestedExpr {
            inner: Some(Box::new(expr)),
        })),
    ];

    let names = kinds.into_iter().map(classify_kind).collect::<Vec<_>>();
    assert_eq!(names.len(), 17);
    assert!(names.contains(&"column_ref"));
    assert!(names.contains(&"nested"));
}
