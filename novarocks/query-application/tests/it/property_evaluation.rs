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

//! QA projection tests. The port supplies already checked constants; it does
//! not stand in for the FE compiler, execution, or connector property policy.

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, ListBuilder,
        StringArray, StringBuilder,
    },
    datatypes::Field,
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool, ConstantValue};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_parser::{SYNTHETIC, Span, ast::Expr, trino_parser::parse_expression};
use novarocks_query_application::{
    session_error::{QueryServiceError, QueryServiceErrorKind},
    sql::{property_admit::DdlPropertyScope, property_evaluation::*},
};
use novarocks_spi::connector::{
    DdlPropertyDeclaration, DdlPropertyType as Type, DdlPropertyValue as Value,
};
use novarocks_sql::compiler::RequiredConstantTarget as Target;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use novarocks_user_error::{ErrorPhase, RetryClass};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Default)]
struct Control {
    stop: AtomicBool,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        if self.stop.load(Ordering::SeqCst) {
            Err(CompileControlError::Cancelled)
        } else {
            Ok(())
        }
    }
}
fn constant(array: ArrayRef, logical: ValueLogicalType) -> ConstantValue {
    assert_eq!(array.len(), 2);
    let mut field = Arc::new(
        Field::new("source_property", array.data_type().clone(), true).with_metadata(
            HashMap::from([("original-provider".into(), "retained".into())]),
        ),
    );
    let ty = if logical == ValueLogicalType::Physical {
        FunctionValueType::try_from_field(&field).unwrap()
    } else {
        FunctionValueType::try_with_logical_type(array.data_type().clone(), true, logical).unwrap()
    };
    if logical != ValueLogicalType::Physical {
        let declared = ty.try_to_field("source_property").unwrap();
        let mut metadata = declared.metadata().clone();
        metadata.insert("original-provider", "retained");
        field = Arc::new(declared.with_metadata(metadata));
    }
    let policy = ConstantPolicy {
        max_rows: 2,
        max_array_nodes: 32,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 1 << 16,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    };
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy,
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), &field));
    assert_eq!(pool.value_type(), &ty);
    let value = pool.value(1).unwrap();
    assert_eq!(value.ordinal(), 1);
    assert_eq!(value.pool().backing_identity(), pool.backing_identity());
    assert_eq!(value.field().metadata()["original-provider"], "retained");
    value
}
fn text(value: Option<&str>) -> ConstantValue {
    constant(
        Arc::new(StringArray::from(vec![Some("unused first row"), value])),
        ValueLogicalType::Physical,
    )
}
fn strings() -> ConstantValue {
    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_value("unused row");
    builder.append(true);
    builder.values().append_value("bucket(id, 8)");
    builder.values().append_null();
    builder.values().append_value("中");
    builder.append(true);
    constant(Arc::new(builder.finish()), ValueLogicalType::Physical)
}
struct Call<'a> {
    expression: &'a Expr,
    target: Target,
    result: Result<ConstantValue, DdlPropertyConstantError>,
}
struct Port<'a> {
    calls: VecDeque<Call<'a>>,
    seen: Vec<*const Expr>,
    stop_after: Option<&'a Control>,
}
impl<'a> Port<'a> {
    fn new(calls: Vec<Call<'a>>) -> Self {
        Self {
            calls: calls.into(),
            seen: vec![],
            stop_after: None,
        }
    }
    fn exhausted(&self) {
        assert!(self.calls.is_empty());
    }
}
impl DdlPropertyConstantPort for Port<'_> {
    fn evaluate(
        &mut self,
        expression: &Expr,
        target: Target,
    ) -> Result<ConstantValue, DdlPropertyConstantError> {
        let call = self.calls.pop_front().expect("no unexpected computation");
        assert!(
            std::ptr::eq(expression, call.expression),
            "actual original borrowed expression, not a clone or equal occurrence"
        );
        assert_eq!(target, call.target);
        self.seen.push(expression);
        if let Ok(value) = &call.result {
            assert_eq!(value.ordinal(), 1);
            assert_eq!(value.field().metadata()["original-provider"], "retained");
            let alias = value.clone();
            assert!(Arc::ptr_eq(
                alias.pool().field_ref(),
                value.pool().field_ref()
            ));
            assert_eq!(
                alias.pool().backing_identity(),
                value.pool().backing_identity()
            );
            assert_eq!(alias.value_type(), value.value_type());
        }
        if let Some(control) = self.stop_after {
            control.stop.store(true, Ordering::SeqCst);
        }
        call.result
    }
}
fn declaration(name: &str, ty: Type, default: Value) -> DdlPropertyDeclaration {
    DdlPropertyDeclaration::new(name, ty, default, false).unwrap()
}
fn assignment<'a>(name: &'a str, expression: &'a Expr) -> DdlPropertyAssignment<'a> {
    // These are programmatic borrowed assignments, not a DDL parser fixture.
    // Only the expression range is claimed as parsed source custody here.
    DdlPropertyAssignment {
        name,
        name_span: SYNTHETIC,
        span: expression.span(),
        value: DdlPropertyAssignmentValue::Expression(expression),
    }
}
fn reset(name: &str) -> DdlPropertyAssignment<'_> {
    DdlPropertyAssignment {
        name,
        name_span: SYNTHETIC,
        span: SYNTHETIC,
        value: DdlPropertyAssignmentValue::Default { span: SYNTHETIC },
    }
}
fn evaluate(
    declarations: &[DdlPropertyDeclaration],
    assignments: &[DdlPropertyAssignment<'_>],
    port: &mut Port<'_>,
    mode: DdlPropertyEvaluationMode,
) -> Result<EvaluatedDdlProperties, QueryServiceError> {
    evaluate_ddl_properties(
        DdlPropertyScope::Table,
        mode,
        "source",
        declarations,
        assignments,
        port,
        &Control::default(),
    )
}

#[test]
fn ddl_property_defaults_resets_absence_and_declaration_order_are_distinct() {
    let expression = parse_expression("'selected'").unwrap();
    let declarations = [
        declaration("first", Type::Varchar, Value::Varchar("default".into())),
        declaration("nullable", Type::Varchar, Value::Null),
        declaration("last", Type::Boolean, Value::Boolean(true)),
    ];
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Ok(text(Some("selected"))),
    }]);
    let result = evaluate(
        &declarations,
        &[assignment("FIRST", &expression)],
        &mut port,
        DdlPropertyEvaluationMode::Create,
    )
    .unwrap();
    port.exhausted();
    assert_eq!(
        result.into_properties(),
        vec![
            EvaluatedDdlProperty {
                name: "first".into(),
                change: DdlPropertyChange::Set(Value::Varchar("selected".into()))
            },
            EvaluatedDdlProperty {
                name: "last".into(),
                change: DdlPropertyChange::Set(Value::Boolean(true))
            }
        ]
    );
    let mut port = Port::new(vec![]);
    let result = evaluate(
        &declarations,
        &[reset("last"), reset("nullable")],
        &mut port,
        DdlPropertyEvaluationMode::Alter,
    )
    .unwrap();
    assert_eq!(
        result.into_properties(),
        vec![
            EvaluatedDdlProperty {
                name: "nullable".into(),
                change: DdlPropertyChange::Reset(Value::Null)
            },
            EvaluatedDdlProperty {
                name: "last".into(),
                change: DdlPropertyChange::Reset(Value::Boolean(true))
            }
        ]
    );
    assert!(
        evaluate(
            &declarations,
            &[],
            &mut port,
            DdlPropertyEvaluationMode::Alter
        )
        .unwrap()
        .properties()
        .is_empty()
    );
    let result = evaluate(
        &declarations,
        &[reset("nullable")],
        &mut port,
        DdlPropertyEvaluationMode::Create,
    )
    .unwrap();
    assert_eq!(result.properties().len(), 2);
    assert!(
        result
            .properties()
            .iter()
            .all(|value| value.name != "nullable")
    );
    port.exhausted();
    let declarations = [declaration(
        "map_default",
        Type::MapVarcharVarchar,
        Value::MapVarcharVarchar(vec![
            ("key".into(), None),
            ("key".into(), Some("not overwritten".into())),
        ]),
    )];
    let error = evaluate(
        &declarations,
        &[],
        &mut port,
        DdlPropertyEvaluationMode::Create,
    )
    .unwrap_err();
    assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
    assert!(error.message().contains("duplicate map keys"));
}

#[test]
fn ddl_property_selected_values_preserve_full_targets_and_nullable_children() {
    let cases: Vec<(Type, Target, ConstantValue, Value)> = vec![
        (
            Type::Boolean,
            Target::Boolean,
            constant(
                Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
                ValueLogicalType::Physical,
            ),
            Value::Boolean(true),
        ),
        (
            Type::Integer,
            Target::Integer,
            constant(
                Arc::new(Int32Array::from(vec![Some(8), Some(i32::MIN)])),
                ValueLogicalType::Physical,
            ),
            Value::Integer(i32::MIN),
        ),
        (
            Type::Bigint,
            Target::Bigint,
            constant(
                Arc::new(Int64Array::from(vec![Some(8), Some(i64::MAX)])),
                ValueLogicalType::Physical,
            ),
            Value::Bigint(i64::MAX),
        ),
        (
            Type::Double,
            Target::Double,
            constant(
                Arc::new(Float64Array::from(vec![Some(8.0), Some(-0.0)])),
                ValueLogicalType::Physical,
            ),
            Value::Double(-0.0),
        ),
        (
            Type::Varchar,
            Target::Varchar,
            text(Some("selected中")),
            Value::Varchar("selected中".into()),
        ),
        (
            Type::ArrayVarchar,
            Target::ArrayVarchar,
            strings(),
            Value::ArrayVarchar(vec![Some("bucket(id, 8)".into()), None, Some("中".into())]),
        ),
    ];
    for (ty, target, value, expected) in cases {
        let expression = parse_expression("original_value").unwrap();
        let original_type = value.value_type().clone();
        let original_field = value.pool().field_ref().clone();
        let original_backing = value.pool().backing_identity();
        let retained = value.clone();
        let mut port = Port::new(vec![Call {
            expression: &expression,
            target,
            result: Ok(value),
        }]);
        let result = evaluate(
            &[declaration("p", ty, Value::Null)],
            &[assignment("p", &expression)],
            &mut port,
            DdlPropertyEvaluationMode::Alter,
        )
        .unwrap();
        port.exhausted();
        assert_eq!(
            result.properties()[0].change,
            DdlPropertyChange::Set(expected)
        );
        if let DdlPropertyChange::Set(Value::Double(value)) = &result.properties()[0].change {
            assert_eq!(value.to_bits(), (-0.0f64).to_bits());
        }
        assert!(Arc::ptr_eq(retained.pool().field_ref(), &original_field));
        assert_eq!(retained.value_type(), &original_type);
        assert_eq!(retained.pool().backing_identity(), original_backing);
        assert_eq!(retained.ordinal(), 1);
    }
}

#[test]
fn ddl_catalog_keys_are_unmodified_and_default_and_whole_null_are_rejected() {
    let expression = parse_expression("'payload'").unwrap();
    let mut port = Port::new(vec![
        Call {
            expression: &expression,
            target: Target::Varchar,
            result: Ok(text(Some("payload"))),
        },
        Call {
            expression: &expression,
            target: Target::Varchar,
            result: Ok(text(Some("different"))),
        },
    ]);
    let result = evaluate_ddl_properties(
        DdlPropertyScope::Catalog,
        DdlPropertyEvaluationMode::Create,
        "'payload'",
        &[],
        &[
            assignment("MixedKey", &expression),
            assignment("mixedkey", &expression),
        ],
        &mut port,
        &Control::default(),
    )
    .unwrap();
    port.exhausted();
    assert_eq!(result.properties()[0].name, "MixedKey");
    assert_eq!(result.properties()[1].name, "mixedkey");
    let mut port = Port::new(vec![]);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Catalog,
        DdlPropertyEvaluationMode::Alter,
        "",
        &[],
        &[reset("MixedKey")],
        &mut port,
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(
        error.user_error().unwrap(),
        &DdlPropertyScope::Catalog.to_user_error(
            "",
            SYNTHETIC,
            "Catalog property DEFAULT is not supported"
        )
    );
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Ok(text(None)),
    }]);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Catalog,
        DdlPropertyEvaluationMode::Create,
        "'payload'",
        &[],
        &[assignment("MixedKey", &expression)],
        &mut port,
        &Control::default(),
    )
    .unwrap_err();
    assert!(error.message().contains("cannot be NULL"));
    port.exhausted();
}

#[test]
fn ddl_map_uses_original_key_then_value_children_and_preserves_null_values() {
    let expression = parse_expression("\"MaP\"(ARRAY['a', 'b'], ARRAY['x', NULL])").unwrap();
    let Expr::FunctionCall(call) = &expression else {
        panic!("real MAP call")
    };
    let (Expr::Array(keys), Expr::Array(values)) =
        (&call.arguments[0].value, &call.arguments[1].value)
    else {
        panic!("actual arrays")
    };
    let mut port = Port::new(vec![
        Call {
            expression: &keys.values[0],
            target: Target::Varchar,
            result: Ok(text(Some("a"))),
        },
        Call {
            expression: &keys.values[1],
            target: Target::Varchar,
            result: Ok(text(Some("b"))),
        },
        Call {
            expression: &values.values[0],
            target: Target::Varchar,
            result: Ok(text(Some("x"))),
        },
        Call {
            expression: &values.values[1],
            target: Target::Varchar,
            result: Ok(text(None)),
        },
    ]);
    let declarations = [declaration(
        "extra_properties",
        Type::MapVarcharVarchar,
        Value::Null,
    )];
    let result = evaluate(
        &declarations,
        &[assignment("extra_properties", &expression)],
        &mut port,
        DdlPropertyEvaluationMode::Alter,
    )
    .unwrap();
    port.exhausted();
    assert_eq!(
        result.properties()[0].change,
        DdlPropertyChange::Set(Value::MapVarcharVarchar(vec![
            ("a".into(), Some("x".into())),
            ("b".into(), None)
        ]))
    );
    // Key validation follows evaluation of both original arrays.
    for nullable_key in [false, true] {
        let mut port = Port::new(vec![
            Call {
                expression: &keys.values[0],
                target: Target::Varchar,
                result: Ok(text(if nullable_key { None } else { Some("a") })),
            },
            Call {
                expression: &keys.values[1],
                target: Target::Varchar,
                result: Ok(text(Some("a"))),
            },
            Call {
                expression: &values.values[0],
                target: Target::Varchar,
                result: Ok(text(Some("x"))),
            },
            Call {
                expression: &values.values[1],
                target: Target::Varchar,
                result: Ok(text(None)),
            },
        ]);
        let error = evaluate(
            &declarations,
            &[assignment("extra_properties", &expression)],
            &mut port,
            DdlPropertyEvaluationMode::Alter,
        )
        .unwrap_err();
        assert!(error.message().contains(if nullable_key {
            "key cannot be NULL"
        } else {
            "Duplicate map"
        }));
        port.exhausted();
    }
    let original = QueryServiceError::new(
        QueryServiceErrorKind::Unsupported,
        "original value computation unavailable",
    );
    let mut port = Port::new(vec![
        Call {
            expression: &keys.values[0],
            target: Target::Varchar,
            result: Ok(text(Some("a"))),
        },
        Call {
            expression: &keys.values[1],
            target: Target::Varchar,
            result: Ok(text(Some("a"))),
        },
        Call {
            expression: &values.values[0],
            target: Target::Varchar,
            result: Err(DdlPropertyConstantError::Failure(original.clone())),
        },
    ]);
    assert_eq!(
        evaluate(
            &declarations,
            &[assignment("extra_properties", &expression)],
            &mut port,
            DdlPropertyEvaluationMode::Alter
        )
        .unwrap_err(),
        original
    );
    port.exhausted();
    for sql in [
        "MAP(ARRAY['a'], ARRAY['x','y'])",
        "cat.MAP(ARRAY['a'],ARRAY['x'])",
        "MAP(ARRAY['a'], ARRAY['x']) FILTER (WHERE TRUE)",
    ] {
        let expression = parse_expression(sql).unwrap();
        let mut port = Port::new(vec![]);
        assert!(
            evaluate(
                &declarations,
                &[assignment("extra_properties", &expression)],
                &mut port,
                DdlPropertyEvaluationMode::Alter
            )
            .is_err()
        );
        port.exhausted();
    }
}

#[test]
fn ddl_property_user_errors_do_not_consume_later_or_duplicate_expressions() {
    let first = parse_expression("first_value").unwrap();
    let second = parse_expression("second_value").unwrap();
    let declarations = [declaration("known", Type::Varchar, Value::Null)];
    let mut port = Port::new(vec![]);
    assert!(
        evaluate(
            &declarations,
            &[reset("unknown")],
            &mut port,
            DdlPropertyEvaluationMode::Alter
        )
        .unwrap_err()
        .message()
        .contains("Unknown property")
    );
    port.exhausted();
    let mut port = Port::new(vec![Call {
        expression: &first,
        target: Target::Varchar,
        result: Ok(text(Some("first"))),
    }]);
    assert!(
        evaluate(
            &declarations,
            &[assignment("KNOWN", &first), assignment("known", &second)],
            &mut port,
            DdlPropertyEvaluationMode::Alter
        )
        .unwrap_err()
        .message()
        .contains("Duplicate property")
    );
    port.exhausted();
    let mut port = Port::new(vec![Call {
        expression: &first,
        target: Target::Varchar,
        result: Ok(text(None)),
    }]);
    assert!(
        evaluate(
            &declarations,
            &[assignment("known", &first)],
            &mut port,
            DdlPropertyEvaluationMode::Alter
        )
        .unwrap_err()
        .message()
        .contains("cannot be NULL")
    );
    port.exhausted();
}

#[test]
fn ddl_property_port_contract_and_original_failures_are_not_scope_data_errors() {
    let expression = parse_expression("original_value").unwrap();
    let declarations = [declaration("p", Type::Varchar, Value::Null)];
    // A real JSON constant has the same UTF8 carrier, but is not VARCHAR.
    let json = constant(
        Arc::new(StringArray::from(vec![Some("0"), Some("1")])),
        ValueLogicalType::Json,
    );
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Ok(json),
    }]);
    let error = evaluate(
        &declarations,
        &[assignment("p", &expression)],
        &mut port,
        DdlPropertyEvaluationMode::Alter,
    )
    .unwrap_err();
    assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
    assert!(error.user_error().is_none());
    port.exhausted();
    let json_child = FunctionValueType::try_with_logical_type(
        arrow::datatypes::DataType::Utf8,
        true,
        ValueLogicalType::Json,
    )
    .unwrap()
    .try_to_field("item")
    .unwrap();
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(json_child);
    builder.values().append_value("0");
    builder.append(true);
    builder.values().append_value("1");
    builder.append(true);
    let json_list = constant(Arc::new(builder.finish()), ValueLogicalType::Physical);
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::ArrayVarchar,
        result: Ok(json_list),
    }]);
    let error = evaluate(
        &[declaration("p", Type::ArrayVarchar, Value::Null)],
        &[assignment("p", &expression)],
        &mut port,
        DdlPropertyEvaluationMode::Alter,
    )
    .unwrap_err();
    assert_eq!(error.kind(), QueryServiceErrorKind::Internal);
    assert!(error.user_error().is_none());
    port.exhausted();
    let cause = SharedFailureCause::new(novarocks_spi::connector::ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
        "original property computation data failure",
    ));
    for original in [
        QueryServiceError::from_compile_control(CompileControlError::Cancelled),
        QueryServiceError::from_failure_cause(cause.clone()),
        QueryServiceError::new(
            QueryServiceErrorKind::Unsupported,
            "original selected capability refusal",
        ),
    ] {
        let mut port = Port::new(vec![Call {
            expression: &expression,
            target: Target::Varchar,
            result: Err(DdlPropertyConstantError::Failure(original.clone())),
        }]);
        assert_eq!(
            evaluate(
                &declarations,
                &[assignment("p", &expression)],
                &mut port,
                DdlPropertyEvaluationMode::Alter
            )
            .unwrap_err(),
            original
        );
        port.exhausted();
    }
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Err(DdlPropertyConstantError::invalid_value_with_cause(
            "original invalid expression",
            cause.clone(),
        )),
    }]);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        "original_value",
        &declarations,
        &[assignment("p", &expression)],
        &mut port,
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(
        error.user_error().unwrap(),
        &DdlPropertyScope::Table.to_user_error(
            "original_value",
            expression.span(),
            "original invalid expression"
        )
    );
    assert_eq!(error.failure_cause(), Some(&cause));
    port.exhausted();
}

#[test]
fn ddl_property_control_refuses_before_port_and_after_actual_port_return() {
    let expression = parse_expression("original_value").unwrap();
    let declarations = [declaration("p", Type::Varchar, Value::Null)];
    let control = Control {
        stop: AtomicBool::new(true),
    };
    let mut port = Port::new(vec![]);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        "original_value",
        &declarations,
        &[assignment("p", &expression)],
        &mut port,
        &control,
    )
    .unwrap_err();
    assert_eq!(
        error.compile_control_error(),
        Some(CompileControlError::Cancelled)
    );
    port.exhausted();
    let control = Control::default();
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Ok(text(Some("selected"))),
    }]);
    port.stop_after = Some(&control);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        "original_value",
        &declarations,
        &[assignment("p", &expression)],
        &mut port,
        &control,
    )
    .unwrap_err();
    assert_eq!(
        error.compile_control_error(),
        Some(CompileControlError::Cancelled)
    );
    assert_eq!(port.seen.len(), 1);
    port.exhausted();
}

#[test]
fn legacy_catalog_preparation_uses_original_literal_facts_and_evaluated_strings() {
    use novarocks_parser::ast::{
        CatalogStatement, LiteralKind, Statement, legacy::CatalogProperty,
    };
    use novarocks_query_application::sql::{lower_product_sql_command, parse_single_statement};

    struct LegacyPort<'a> {
        properties: &'a [CatalogProperty],
        results: VecDeque<ConstantValue>,
        cursor: usize,
    }
    impl DdlPropertyConstantPort for LegacyPort<'_> {
        fn evaluate(
            &mut self,
            expression: &Expr,
            target: Target,
        ) -> Result<ConstantValue, DdlPropertyConstantError> {
            assert_eq!(target, Target::Varchar);
            let Expr::Literal(literal) = expression else {
                panic!("original literal bridge");
            };
            // The old tree has a Literal slot, not an Expr address. Compare its
            // actual parsed value/range; do not claim a fabricated old Expr loan.
            assert_eq!(literal, &self.properties[self.cursor].value);
            self.cursor += 1;
            let result = self
                .results
                .pop_front()
                .expect("one result per requested literal");
            assert_eq!(result.ordinal(), 1);
            assert_eq!(result.field().metadata()["original-provider"], "retained");
            Ok(result)
        }
    }
    let source = "CREATE EXTERNAL CATALOG IF NOT EXISTS Warehouse COMMENT 'old admitted comment' PROPERTIES ('MixedCase'='iceberg', 'location'='s3://original/中')";
    let statement = parse_single_statement(source).unwrap();
    let Statement::Catalog(CatalogStatement::CreateCatalog(original)) = &statement else {
        panic!("actual legacy CREATE CATALOG");
    };
    assert!(original.external);
    assert!(
        matches!(&original.comment.as_ref().unwrap().kind, LiteralKind::String(value) if value == "old admitted comment")
    );
    assert_eq!(
        lower_product_sql_command(&statement).unwrap_err(),
        "CREATE CATALOG requires evaluated property preparation"
    );
    let mut port = LegacyPort {
        properties: &original.properties,
        cursor: 0,
        results: vec![
            text(Some("evaluated-iceberg")),
            text(Some("s3://evaluated/中")),
        ]
        .into(),
    };
    let command =
        prepare_legacy_create_catalog(source, original, &mut port, &Control::default()).unwrap();
    assert_eq!(port.cursor, 2);
    assert!(port.results.is_empty());
    assert_eq!(command.name, "Warehouse");
    assert!(command.if_not_exists);
    assert_eq!(
        command.properties,
        vec![
            ("MixedCase".into(), "evaluated-iceberg".into()),
            ("location".into(), "s3://evaluated/中".into())
        ]
    );
    assert!(
        matches!(&original.properties[0].value.kind, LiteralKind::String(value) if value == "iceberg")
    );
    assert_eq!(
        &source[original.properties[1].value.span.start()..original.properties[1].value.span.end()],
        "'s3://original/中'"
    );

    let source = "CREATE CATALOG Warehouse PROPERTIES (12='iceberg')";
    let statement = parse_single_statement(source).unwrap();
    let Statement::Catalog(CatalogStatement::CreateCatalog(original)) = &statement else {
        panic!("actual legacy CREATE CATALOG");
    };
    let mut port = LegacyPort {
        properties: &original.properties,
        cursor: 0,
        results: VecDeque::new(),
    };
    let error = prepare_legacy_create_catalog(source, original, &mut port, &Control::default())
        .unwrap_err();
    assert_eq!(port.cursor, 0);
    assert_eq!(
        error.user_error().unwrap(),
        &DdlPropertyScope::Catalog.to_user_error(
            source,
            original.properties[0].key.span,
            "Catalog property keys must be string values"
        )
    );

    let source = "CREATE CATALOG Warehouse PROPERTIES ('same'='first', 'same'='second')";
    let statement = parse_single_statement(source).unwrap();
    let Statement::Catalog(CatalogStatement::CreateCatalog(original)) = &statement else {
        panic!("actual legacy CREATE CATALOG");
    };
    let mut port = LegacyPort {
        properties: &original.properties,
        cursor: 0,
        results: vec![text(Some("first evaluated"))].into(),
    };
    let error = prepare_legacy_create_catalog(source, original, &mut port, &Control::default())
        .unwrap_err();
    assert_eq!(port.cursor, 1);
    assert!(port.results.is_empty());
    assert_eq!(
        error.user_error().unwrap(),
        &DdlPropertyScope::Catalog.to_user_error(
            source,
            original.properties[1].key.span,
            "Duplicate property 'same'"
        )
    );
}

struct NamePolicy {
    checked: Vec<String>,
    reject: Option<(&'static str, QueryServiceError)>,
}
impl DdlPropertyNamePolicy for NamePolicy {
    fn check_unknown_property(&mut self, canonical: &str) -> Result<(), QueryServiceError> {
        self.checked.push(canonical.to_owned());
        match &self.reject {
            Some((name, error)) if *name == canonical => Err(error.clone()),
            _ => Ok(()),
        }
    }
}

#[test]
fn ddl_property_name_policy_does_not_preempt_an_earlier_original_port_failure() {
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    let first = parse_expression("first_value").unwrap();
    let later = parse_expression("later_value").unwrap();
    let original =
        QueryServiceError::from_failure_cause(SharedFailureCause::new(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "original property source failure",
        )));
    let recognized =
        QueryServiceError::from_failure_cause(SharedFailureCause::new(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "recognized property has no writer",
        )));
    let mut policy = NamePolicy {
        checked: vec![],
        reject: Some(("sorted_by", recognized)),
    };
    let mut port = Port::new(vec![Call {
        expression: &first,
        target: Target::Varchar,
        result: Err(DdlPropertyConstantError::Failure(original.clone())),
    }]);
    let actual = evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        "source",
        &[declaration("known", Type::Varchar, Value::Null)],
        &[assignment("known", &first), assignment("SORTED_BY", &later)],
        &mut port,
        &Control::default(),
        &mut policy,
    )
    .unwrap_err();
    assert_eq!(actual, original);
    assert!(policy.checked.is_empty());
    assert_eq!(port.seen, vec![&first as *const Expr]);
    port.exhausted();
}

#[test]
fn ddl_property_name_policy_returns_the_original_recognized_capability_cause() {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    let expression = parse_expression("not_computed").unwrap();
    let cause = SharedFailureCause::new(ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "recognized property has no writer",
    ));
    let original = QueryServiceError::from_failure_cause(cause.clone());
    let mut policy = NamePolicy {
        checked: vec![],
        reject: Some(("sorted_by", original.clone())),
    };
    let mut port = Port::new(vec![]);
    let actual = evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Create,
        "source",
        &[],
        &[assignment("SORTED_BY", &expression)],
        &mut port,
        &Control::default(),
        &mut policy,
    )
    .unwrap_err();
    assert_eq!(actual, original);
    assert_eq!(policy.checked, ["sorted_by"]);
    let retained = actual.failure_cause().unwrap();
    assert_eq!(retained.failure_fact(), cause.failure_fact());
    assert_eq!(
        retained.downcast_ref::<ConnectorError>().unwrap().kind(),
        ConnectorErrorKind::Unsupported
    );
    assert!(std::ptr::eq(
        retained.downcast_ref::<ConnectorError>().unwrap(),
        cause.downcast_ref::<ConnectorError>().unwrap(),
    ));
    assert!(port.seen.is_empty());
    port.exhausted();
}

#[test]
fn ddl_property_name_policy_ok_still_rejects_unknown_at_the_original_scope_and_site() {
    let expression = parse_expression("not_computed").unwrap();
    let source = "  UNKNOWN";
    let span = novarocks_parser::Span::new(2, source.len());
    // This input explicitly authors its name extent; no DDL parser custody is
    // asserted for the separately parsed, unevaluated value expression.
    let input = DdlPropertyAssignment {
        name: "UNKNOWN",
        name_span: span,
        span,
        value: DdlPropertyAssignmentValue::Expression(&expression),
    };
    for scope in [
        DdlPropertyScope::Table,
        DdlPropertyScope::Schema,
        DdlPropertyScope::MaterializedView,
        DdlPropertyScope::Analyze,
        DdlPropertyScope::Branch,
    ] {
        let mut policy = NamePolicy {
            checked: vec![],
            reject: None,
        };
        let mut port = Port::new(vec![]);
        let actual = evaluate_ddl_properties_with_name_policy(
            scope,
            DdlPropertyEvaluationMode::Alter,
            source,
            &[],
            &[input],
            &mut port,
            &Control::default(),
            &mut policy,
        )
        .unwrap_err();
        assert_eq!(
            actual.user_error().unwrap(),
            &scope.to_user_error(source, span, "Unknown property 'unknown'")
        );
        assert_eq!(policy.checked, ["unknown"]);
        assert!(port.seen.is_empty());
        let original = evaluate_ddl_properties(
            scope,
            DdlPropertyEvaluationMode::Alter,
            source,
            &[],
            &[input],
            &mut port,
            &Control::default(),
        )
        .unwrap_err();
        assert_eq!(actual, original);
        port.exhausted();
    }
}

#[test]
fn ddl_property_name_policy_bypasses_known_duplicate_and_dynamic_catalog_names() {
    let expression = parse_expression("'retained'").unwrap();
    let rejected = QueryServiceError::new(QueryServiceErrorKind::Unsupported, "must not be called");
    let mut policy = NamePolicy {
        checked: vec![],
        reject: Some(("known", rejected)),
    };
    let declarations = [declaration("known", Type::Varchar, Value::Null)];
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Ok(text(Some("retained"))),
    }]);
    let result = evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        "source",
        &declarations,
        &[assignment("KNOWN", &expression)],
        &mut port,
        &Control::default(),
        &mut policy,
    )
    .unwrap();
    assert_eq!(
        result.properties(),
        &[EvaluatedDdlProperty {
            name: "known".into(),
            change: DdlPropertyChange::Set(Value::Varchar("retained".into())),
        }]
    );
    assert!(policy.checked.is_empty());
    port.exhausted();
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Ok(text(Some("retained"))),
    }]);
    let error = evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        "source",
        &declarations,
        &[
            assignment("KNOWN", &expression),
            assignment("known", &expression),
        ],
        &mut port,
        &Control::default(),
        &mut policy,
    )
    .unwrap_err();
    assert_eq!(
        error.user_error().unwrap(),
        &DdlPropertyScope::Table.to_user_error("source", SYNTHETIC, "Duplicate property 'known'")
    );
    assert!(policy.checked.is_empty());
    assert_eq!(port.seen.len(), 1);
    port.exhausted();
    let mut policy = NamePolicy {
        checked: vec![],
        reject: Some((
            "sorted_by",
            QueryServiceError::new(
                QueryServiceErrorKind::Unsupported,
                "not a Catalog declaration",
            ),
        )),
    };
    let mut port = Port::new(vec![
        Call {
            expression: &expression,
            target: Target::Varchar,
            result: Ok(text(Some("first"))),
        },
        Call {
            expression: &expression,
            target: Target::Varchar,
            result: Ok(text(Some("second"))),
        },
    ]);
    let result = evaluate_ddl_properties_with_name_policy(
        DdlPropertyScope::Catalog,
        DdlPropertyEvaluationMode::Create,
        "source",
        &[],
        &[
            assignment("sorted_by", &expression),
            assignment("SORTED_BY", &expression),
        ],
        &mut port,
        &Control::default(),
        &mut policy,
    )
    .unwrap();
    assert_eq!(
        result
            .properties()
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["sorted_by", "SORTED_BY"]
    );
    assert!(policy.checked.is_empty());
    assert_eq!(port.seen.len(), 2);
    port.exhausted();
}

#[test]
fn authored_property_inputs_share_expression_results_defaults_and_order_without_sql_casts() {
    let cases = vec![
        (
            Type::Integer,
            Target::Integer,
            Value::Integer(i32::MAX),
            constant(
                Arc::new(Int32Array::from(vec![Some(0), Some(i32::MAX)])),
                ValueLogicalType::Physical,
            ),
        ),
        (
            Type::Varchar,
            Target::Varchar,
            Value::Varchar("actual中".into()),
            text(Some("actual中")),
        ),
        (
            Type::ArrayVarchar,
            Target::ArrayVarchar,
            Value::ArrayVarchar(vec![Some("bucket(id, 8)".into()), None, Some("中".into())]),
            strings(),
        ),
    ];
    for (value_type, target, authored, checked) in cases {
        let source = "actual_value";
        let expression = parse_expression(source).unwrap();
        let declarations = [
            declaration("before", Type::Boolean, Value::Boolean(true)),
            declaration("chosen", value_type, Value::Null),
        ];
        let mut port = Port::new(vec![Call {
            expression: &expression,
            target,
            result: Ok(checked),
        }]);
        let evaluated_expression = evaluate_ddl_properties(
            DdlPropertyScope::Table,
            DdlPropertyEvaluationMode::Create,
            source,
            &declarations,
            &[assignment("CHOSEN", &expression)],
            &mut port,
            &Control::default(),
        )
        .unwrap();
        port.exhausted();
        let mut port = Port::new(vec![]);
        let evaluated_authored = evaluate_ddl_properties(
            DdlPropertyScope::Table,
            DdlPropertyEvaluationMode::Create,
            source,
            &declarations,
            &[DdlPropertyAssignment {
                name: "CHOSEN",
                name_span: SYNTHETIC,
                span: expression.span(),
                value: DdlPropertyAssignmentValue::Authored {
                    value: &authored,
                    span: expression.span(),
                },
            }],
            &mut port,
            &Control::default(),
        )
        .unwrap();
        port.exhausted();
        assert_eq!(evaluated_expression, evaluated_authored);
        assert_eq!(
            evaluated_authored.properties(),
            &[
                EvaluatedDdlProperty {
                    name: "before".into(),
                    change: DdlPropertyChange::Set(Value::Boolean(true))
                },
                EvaluatedDdlProperty {
                    name: "chosen".into(),
                    change: DdlPropertyChange::Set(authored.clone())
                },
            ]
        );
    }
    let source = "actual_map_value";
    let expression = parse_expression(source).unwrap();
    let map = Value::MapVarcharVarchar(vec![
        ("first".into(), None),
        ("second".into(), Some("kept".into())),
    ]);
    let mut port = Port::new(vec![]);
    let result = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        source,
        &[declaration(
            "extra_properties",
            Type::MapVarcharVarchar,
            Value::Null,
        )],
        &[DdlPropertyAssignment {
            name: "extra_properties",
            name_span: SYNTHETIC,
            span: expression.span(),
            value: DdlPropertyAssignmentValue::Authored {
                value: &map,
                span: expression.span(),
            },
        }],
        &mut port,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.properties()[0].change, DdlPropertyChange::Set(map));
    port.exhausted();
}

#[test]
fn authored_property_input_errors_keep_scope_span_and_original_name_duplicate_precedence() {
    let source = "actual_property_value";
    let expression = parse_expression(source).unwrap();
    let value_span = expression.span();
    let null = Value::Null;
    let wrong = Value::Varchar("2".into());
    let duplicate_map = Value::MapVarcharVarchar(vec![
        ("key".into(), None),
        ("key".into(), Some("not overwritten".into())),
    ]);
    for (value_type, value, expected) in [
        (Type::Integer, &null, "cannot be NULL"),
        (Type::Integer, &wrong, "requires Integer, got Varchar"),
        (
            Type::MapVarcharVarchar,
            &duplicate_map,
            "Duplicate map property key",
        ),
    ] {
        let mut port = Port::new(vec![]);
        let error = evaluate_ddl_properties(
            DdlPropertyScope::Table,
            DdlPropertyEvaluationMode::Alter,
            source,
            &[declaration("known", value_type, Value::Null)],
            &[DdlPropertyAssignment {
                name: "known",
                name_span: SYNTHETIC,
                span: value_span,
                value: DdlPropertyAssignmentValue::Authored {
                    value,
                    span: value_span,
                },
            }],
            &mut port,
            &Control::default(),
        )
        .unwrap_err();
        let user = error.user_error().expect("typed property data error");
        assert_eq!(user.code().as_str(), "sql.admit.invalid_table_property");
        assert_eq!(
            user.location(),
            Some(&value_span.to_user_error_location(source))
        );
        assert!(user.message().contains(expected));
        port.exhausted();
    }
    let mut port = Port::new(vec![]);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        source,
        &[declaration("known", Type::Integer, Value::Null)],
        &[DdlPropertyAssignment {
            name: "unknown",
            name_span: SYNTHETIC,
            span: value_span,
            value: DdlPropertyAssignmentValue::Authored {
                value: &wrong,
                span: value_span,
            },
        }],
        &mut port,
        &Control::default(),
    )
    .unwrap_err();
    assert!(
        error
            .user_error()
            .unwrap()
            .message()
            .contains("Unknown property 'unknown'")
    );
    let first = Value::Integer(7);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        source,
        &[declaration("known", Type::Integer, Value::Null)],
        &[
            DdlPropertyAssignment {
                name: "known",
                name_span: SYNTHETIC,
                span: value_span,
                value: DdlPropertyAssignmentValue::Authored {
                    value: &first,
                    span: value_span,
                },
            },
            DdlPropertyAssignment {
                name: "KNOWN",
                name_span: SYNTHETIC,
                span: value_span,
                value: DdlPropertyAssignmentValue::Authored {
                    value: &null,
                    span: value_span,
                },
            },
        ],
        &mut port,
        &Control::default(),
    )
    .unwrap_err();
    assert!(
        error
            .user_error()
            .unwrap()
            .message()
            .contains("Duplicate property 'known'")
    );
    port.exhausted();
}

#[test]
fn authored_property_copy_preserves_control_source_and_does_not_reorder_expression_failure() {
    struct StopCopy {
        cause: SharedFailureCause,
        stopped: AtomicBool,
    }
    impl PureCompileControl for StopCopy {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Validate);
            assert!(
                !self.stopped.load(Ordering::SeqCst),
                "no callback after original refusal"
            );
            if units == 256 {
                self.stopped.store(true, Ordering::SeqCst);
                return Err(CompileControlError::Source(self.cause.clone()));
            }
            Ok(())
        }
    }
    let cause = SharedFailureCause::new(novarocks_spi::connector::ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
        "original property preparation control source",
    ));
    let control = StopCopy {
        cause: cause.clone(),
        stopped: AtomicBool::new(false),
    };
    let source = "actual_value";
    let expression = parse_expression(source).unwrap();
    let large = Value::Varchar("x".repeat(600));
    let declarations = [
        declaration("first", Type::Varchar, Value::Null),
        declaration("later", Type::Integer, Value::Null),
    ];
    let mut port = Port::new(vec![]);
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        source,
        &declarations,
        &[
            DdlPropertyAssignment {
                name: "first",
                name_span: SYNTHETIC,
                span: expression.span(),
                value: DdlPropertyAssignmentValue::Authored {
                    value: &large,
                    span: expression.span(),
                },
            },
            assignment("later", &expression),
        ],
        &mut port,
        &control,
    )
    .unwrap_err();
    assert!(control.stopped.load(Ordering::SeqCst));
    assert_eq!(
        error.compile_control_error(),
        Some(CompileControlError::Source(cause))
    );
    assert!(
        error
            .user_error()
            .is_none_or(|user| user.code().as_str() != "sql.admit.invalid_table_property")
    );
    port.exhausted();
    let original = QueryServiceError::new(
        QueryServiceErrorKind::Unsupported,
        "original required expression unavailable",
    );
    let mut port = Port::new(vec![Call {
        expression: &expression,
        target: Target::Varchar,
        result: Err(DdlPropertyConstantError::Failure(original.clone())),
    }]);
    let wrong = Value::Varchar("not an integer".into());
    let error = evaluate_ddl_properties(
        DdlPropertyScope::Table,
        DdlPropertyEvaluationMode::Alter,
        source,
        &declarations,
        &[
            assignment("first", &expression),
            DdlPropertyAssignment {
                name: "later",
                name_span: SYNTHETIC,
                span: expression.span(),
                value: DdlPropertyAssignmentValue::Authored {
                    value: &wrong,
                    span: expression.span(),
                },
            },
        ],
        &mut port,
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(error, original);
    port.exhausted();
}

#[test]
fn trino_catalog_using_is_independent_of_original_with_arguments() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};

    let source = "CREATE CATALOG IF NOT EXISTS Warehouse USING iceberg COMMENT 'kept by parser' \
                  WITH (\"Mixed Key\" = coalesce('source', 'other'), \"Value\" = 'source')";
    let TrinoStatement::CreateCatalog(statement) = parse_statement(source).unwrap() else {
        panic!("real CREATE CATALOG");
    };
    let mut port = Port::new(vec![
        Call {
            expression: statement.properties[0].value.as_ref().unwrap(),
            target: Target::Varchar,
            result: Ok(text(Some(" value with both spaces "))),
        },
        Call {
            expression: statement.properties[1].value.as_ref().unwrap(),
            target: Target::Varchar,
            result: Ok(text(Some("selected中"))),
        },
    ]);
    let actual = prepare_trino_create_catalog(source, &statement, &mut port, &Control::default())
        .expect("prepare owned configuration facts");
    port.exhausted();
    assert!(actual.if_not_exists);
    assert_eq!(actual.name, "Warehouse");
    assert_eq!(actual.connector_name.as_deref(), Some("iceberg"));
    assert_eq!(
        actual.properties,
        vec![
            ("Mixed Key".into(), " value with both spaces ".into()),
            ("Value".into(), "selected中".into()),
        ]
    );
}

#[test]
fn trino_catalog_rejects_type_at_the_original_name_before_its_value() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};

    let source =
        "CREATE CATALOG warehouse USING iceberg WITH (setting = 'good', \"TyPe\" = DEFAULT)";
    let TrinoStatement::CreateCatalog(statement) = parse_statement(source).unwrap() else {
        panic!("real CREATE CATALOG");
    };
    let original = statement.properties[0].value.as_ref().unwrap();
    let expected = DdlPropertyScope::Catalog.to_user_error(
        source,
        statement.properties[1].name.span,
        "Catalog property 'type' cannot override the USING connector",
    );
    let mut port = Port::new(vec![Call {
        expression: original,
        target: Target::Varchar,
        result: Ok(text(Some("good"))),
    }]);
    let actual = prepare_trino_create_catalog(source, &statement, &mut port, &Control::default())
        .unwrap_err();
    assert_eq!(actual.user_error(), Some(&expected));
    port.exhausted();
    assert_eq!(port.seen, [std::ptr::from_ref(original)]);

    let original_failure =
        QueryServiceError::new(QueryServiceErrorKind::Unavailable, "original source");
    let mut port = Port::new(vec![Call {
        expression: original,
        target: Target::Varchar,
        result: Err(original_failure.clone().into()),
    }]);
    let actual = prepare_trino_create_catalog(source, &statement, &mut port, &Control::default())
        .unwrap_err();
    assert_eq!(actual, original_failure);
    port.exhausted();
}

fn assert_create_authorization_refusal(
    error: &QueryServiceError,
    source: &str,
    principal_span: Span,
    message: &str,
) {
    let user = error.user_error().expect("typed authorization refusal");
    assert_eq!(user.code().as_str(), "sql.admit.unsupported_statement");
    assert_eq!(user.phase(), ErrorPhase::Admit);
    assert_eq!(user.retry(), RetryClass::Never);
    assert_eq!(user.message(), message);
    assert_eq!(
        user.location(),
        Some(&principal_span.to_user_error_location(source)),
    );
}

#[test]
fn trino_catalog_refuses_authorization_and_delimited_provider_names() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};

    let source =
        "CREATE CATALOG warehouse USING iceberg AUTHORIZATION USER user1 WITH (setting = 'value')";
    let TrinoStatement::CreateCatalog(statement) = parse_statement(source).unwrap() else {
        panic!("real CREATE CATALOG");
    };
    let mut port = Port::new(vec![]);
    let error = prepare_trino_create_catalog(source, &statement, &mut port, &Control::default())
        .unwrap_err();
    assert_create_authorization_refusal(
        &error,
        source,
        statement.principal.as_ref().unwrap().name.span,
        "CREATE CATALOG AUTHORIZATION is not supported",
    );
    assert!(port.seen.is_empty());

    let source = "CREATE CATALOG warehouse USING \"iceberg\"";
    let TrinoStatement::CreateCatalog(statement) = parse_statement(source).unwrap() else {
        panic!("real CREATE CATALOG");
    };
    let error = prepare_trino_create_catalog(source, &statement, &mut port, &Control::default())
        .unwrap_err();
    assert_eq!(error.kind(), QueryServiceErrorKind::InvalidValue);
    assert_eq!(
        error.message(),
        "CREATE CATALOG USING requires an undelimited connector name",
    );
    assert!(port.seen.is_empty());
}

#[test]
fn trino_schema_properties_use_the_scope_declaration_and_original_value_slot() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
    use novarocks_sql::semantic::CatalogSqlCommand;

    // Independent declaration fixture of the Iceberg provider's current
    // location profile. The provider's own REST/Hadoop tests own publication.
    let declarations = [declaration("location", Type::Varchar, Value::Null)];
    for (property, selected_location) in [
        (
            "\"LOCATION\" = coalesce('s3://warehouse/path', '')",
            Some("s3://warehouse/path"),
        ),
        ("location = DEFAULT", None),
    ] {
        let source = format!("CREATE SCHEMA IF NOT EXISTS cat.\"MixedDb\" WITH ({property})");
        let TrinoStatement::CreateSchema(statement) = parse_statement(&source).unwrap() else {
            panic!("real CREATE SCHEMA");
        };
        let mut port = Port::new(
            statement.properties[0]
                .value
                .as_ref()
                .map(|expression| Call {
                    expression,
                    target: Target::Varchar,
                    result: Ok(text(selected_location)),
                })
                .into_iter()
                .collect(),
        );
        let CatalogSqlCommand::CreateDatabase {
            name,
            if_not_exists,
            properties,
        } = prepare_trino_create_schema(
            &source,
            &statement,
            &declarations,
            &mut port,
            &Control::default(),
        )
        .unwrap()
        else {
            panic!("original semantic schema command");
        };
        port.exhausted();
        assert!(if_not_exists);
        assert_eq!(name.parts, ["cat", "MixedDb"]);
        let expected =
            selected_location.map(|value| ("location".into(), Value::Varchar(value.into())));
        assert_eq!(properties, expected.into_iter().collect::<Vec<_>>());
    }

    let source = "CREATE SCHEMA cat.db WITH (unclaimed = 'value')";
    let TrinoStatement::CreateSchema(statement) = parse_statement(source).unwrap() else {
        panic!("real CREATE SCHEMA");
    };
    let error = prepare_trino_create_schema(
        source,
        &statement,
        &declarations,
        &mut Port::new(vec![]),
        &Control::default(),
    )
    .unwrap_err();
    assert_eq!(
        error.user_error(),
        Some(&DdlPropertyScope::Schema.to_user_error(
            source,
            statement.properties[0].name.span,
            "Unknown property 'unclaimed'",
        ))
    );

    let source = "CREATE SCHEMA cat.db AUTHORIZATION ROLE future_role WITH (location = 'value')";
    let TrinoStatement::CreateSchema(statement) = parse_statement(source).unwrap() else {
        panic!("real CREATE SCHEMA");
    };
    let error = prepare_trino_create_schema(
        source,
        &statement,
        &declarations,
        &mut Port::new(vec![]),
        &Control::default(),
    )
    .unwrap_err();
    assert_create_authorization_refusal(
        &error,
        source,
        statement.principal.as_ref().unwrap().name.span,
        "CREATE SCHEMA AUTHORIZATION is not supported",
    );
}

#[test]
fn trino_column_and_comment_commands_keep_parser_name_segments() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
    use novarocks_query_application::sql::trino_table_command::*;
    use novarocks_sql::semantic::{
        CatalogSqlCommand,
        command::{
            ColumnPositionSql, CommandLiteral, IcebergColumnSqlAction, IcebergSchemaSqlChange,
            IcebergTableSqlAction,
        },
    };
    use novarocks_types::schema::SqlType;

    let source =
        r#"ALTER TABLE lake.ns.t ADD COLUMN "parent.segment".added bigint DEFAULT 7 AFTER anchor"#;
    let TrinoStatement::AddColumn(statement) = parse_statement(source).unwrap() else {
        panic!("expected an ADD COLUMN statement");
    };
    let CatalogSqlCommand::AlterIcebergTable(command) =
        lower_trino_add_column(source, &statement).unwrap()
    else {
        panic!("expected the original Iceberg table command");
    };
    assert_eq!(command.table.parts, ["lake", "ns", "t"]);
    assert_eq!(
        command.action,
        IcebergTableSqlAction::Schema(IcebergSchemaSqlChange::AddColumn {
            path: vec!["parent.segment".into(), "added".into()],
            data_type: SqlType::BigInt,
            nullable: Some(true),
            default: Some(CommandLiteral::Number("7".into())),
            position: ColumnPositionSql::After(vec!["anchor".into()]),
        })
    );

    let source = r#"COMMENT ON COLUMN lake.ns.t."x.y" IS ''"#;
    let TrinoStatement::Comment(statement) = parse_statement(source).unwrap() else {
        panic!("expected a COMMENT ON COLUMN statement");
    };
    let CatalogSqlCommand::AlterIcebergTable(command) =
        lower_trino_table_comment(source, &statement).unwrap()
    else {
        panic!("expected the original Iceberg table command");
    };
    assert_eq!(command.table.parts, ["lake", "ns", "t"]);
    assert_eq!(
        command.action,
        IcebergTableSqlAction::Schema(IcebergSchemaSqlChange::AlterColumn {
            path: vec!["x.y".into()],
            action: IcebergColumnSqlAction::Comment(CommandLiteral::String(String::new())),
        })
    );

    let source = "COMMENT ON TABLE lake.ns.t IS NULL";
    let TrinoStatement::Comment(statement) = parse_statement(source).unwrap() else {
        panic!("expected a COMMENT ON TABLE statement");
    };
    let error = lower_trino_table_comment(source, &statement).unwrap_err();
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "sql.admit.unsupported_statement"
    );

    let source = "ALTER TABLE lake.ns.t DROP COLUMN IF EXISTS col";
    let TrinoStatement::DropColumn(statement) = parse_statement(source).unwrap() else {
        panic!("expected a DROP COLUMN statement");
    };
    let error = lower_trino_drop_column(source, &statement).unwrap_err();
    assert_eq!(
        error.user_error().unwrap().code().as_str(),
        "sql.admit.unsupported_statement"
    );
}

#[test]
fn trino_analyze_properties_preserve_omission_and_array_custody() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
    use novarocks_query_application::sql::trino_table_command::prepare_trino_analyze;
    use novarocks_sql::semantic::StatisticsSqlCommand;

    let declared = [declaration("columns", Type::ArrayVarchar, Value::Null)];
    let control = Control::default();
    for source in [
        "ANALYZE lake.ns.t",
        "ANALYZE lake.ns.t WITH (columns = DEFAULT)",
    ] {
        let TrinoStatement::Analyze(statement) = parse_statement(source).unwrap() else {
            panic!("expected an ANALYZE statement");
        };
        let mut port = Port::new(vec![]);
        let command =
            prepare_trino_analyze(source, &statement, &declared, &mut port, &control).unwrap();
        let StatisticsSqlCommand::AnalyzeTable { table, columns } = command else {
            panic!("expected the original ANALYZE command");
        };
        assert_eq!(table.parts, ["lake", "ns", "t"]);
        assert_eq!(columns, None);
        port.exhausted();
    }
    for (source, selected) in [
        (
            "ANALYZE lake.ns.t WITH (columns = ARRAY['Z', 'a'])",
            &[Some("Z"), Some("a")][..],
        ),
        ("ANALYZE lake.ns.t WITH (columns = ARRAY[])", &[][..]),
        (
            "ANALYZE lake.ns.t WITH (columns = ARRAY['A', NULL])",
            &[Some("A"), None][..],
        ),
    ] {
        let TrinoStatement::Analyze(statement) = parse_statement(source).unwrap() else {
            panic!("expected an ANALYZE statement");
        };
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a different ordinal");
        builder.append(true);
        for &column in selected {
            builder.values().append_option(column);
        }
        builder.append(true);
        let expression = statement.properties[0].value.as_ref().unwrap();
        let mut port = Port::new(vec![Call {
            expression,
            target: Target::ArrayVarchar,
            result: Ok(constant(
                Arc::new(builder.finish()),
                ValueLogicalType::Physical,
            )),
        }]);
        let result = prepare_trino_analyze(source, &statement, &declared, &mut port, &control);
        if selected.contains(&None) {
            let error = result.unwrap_err();
            assert_eq!(
                error.user_error().unwrap().code().as_str(),
                "sql.admit.invalid_analyze_property"
            );
            assert!(error.message().contains("Invalid null value"));
        } else {
            let StatisticsSqlCommand::AnalyzeTable { table, columns } = result.unwrap() else {
                panic!("expected the original ANALYZE command");
            };
            assert_eq!(table.parts, ["lake", "ns", "t"]);
            assert_eq!(
                columns.unwrap(),
                selected
                    .iter()
                    .map(|value| value.unwrap())
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(port.seen, [expression as *const Expr]);
        port.exhausted();
    }
}

#[test]
fn trino_branch_uses_the_authentic_bigint_expression_and_original_ref_command() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
    use novarocks_query_application::sql::trino_table_command::*;
    use novarocks_sql::semantic::{
        CatalogSqlCommand,
        command::{IcebergReferenceKindSql, IcebergReferenceSqlAction, IcebergTableSqlAction},
    };

    let source =
        r#"CREATE BRANCH IF NOT EXISTS "b.next" WITH (snapshot_id = 100 + 23) IN TABLE lake.ns.t"#;
    let TrinoStatement::CreateBranch(statement) = parse_statement(source).unwrap() else {
        panic!("expected a CREATE BRANCH statement");
    };
    let declared = [declaration("snapshot_id", Type::Bigint, Value::Null)];
    let expression = statement.properties[0].value.as_ref().unwrap();
    let mut port = Port::new(vec![Call {
        expression,
        target: Target::Bigint,
        result: Ok(constant(
            Arc::new(Int64Array::from(vec![1, 123])),
            ValueLogicalType::Physical,
        )),
    }]);
    let control = Control::default();
    let CatalogSqlCommand::AlterIcebergTable(command) =
        prepare_trino_create_branch(source, &statement, &declared, &mut port, &control).unwrap()
    else {
        panic!("expected the original Iceberg table command");
    };
    assert_eq!(command.table.parts, ["lake", "ns", "t"]);
    assert_eq!(
        command.action,
        IcebergTableSqlAction::Reference(IcebergReferenceSqlAction::Create {
            kind: IcebergReferenceKindSql::Branch,
            name: "b.next".into(),
            if_not_exists: true,
            or_replace: false,
            snapshot_id: Some(123),
        })
    );
    assert_eq!(port.seen, [expression as *const Expr]);
    port.exhausted();

    let source = r#"DROP BRANCH IF EXISTS "b.next" IN TABLE lake.ns.t"#;
    let TrinoStatement::DropBranch(statement) = parse_statement(source).unwrap() else {
        panic!("expected a DROP BRANCH statement");
    };
    let CatalogSqlCommand::AlterIcebergTable(command) = lower_trino_drop_branch(&statement) else {
        panic!("expected the original Iceberg table command");
    };
    assert_eq!(
        command.action,
        IcebergTableSqlAction::Reference(IcebergReferenceSqlAction::Drop {
            kind: IcebergReferenceKindSql::Branch,
            name: "b.next".into(),
            if_exists: true,
        })
    );
}

#[test]
fn trino_table_execute_resolves_provider_and_where_before_evaluating_arguments() {
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
    use novarocks_query_application::sql::{
        table_procedure::{
            BoundProcedureArgument, TableProcedureAbsoluteTimePort, TableProcedureKind,
        },
        trino_table_command::bind_trino_table_execute,
    };
    use novarocks_spi::connector::ConnectorProviderId;

    struct NoAbsoluteTime;
    impl TableProcedureAbsoluteTimePort for NoAbsoluteTime {
        fn legacy_timestamp_literal_millis(
            &mut self,
            _: &str,
            _: novarocks_parser::Span,
        ) -> Result<i64, QueryServiceError> {
            panic!("this procedure does not request an absolute time")
        }
    }

    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let control = Control::default();
    for (source, code) in [
        (
            "ALTER TABLE lake.ns.t EXECUTE not_registered(bad => missing) WHERE id > 0",
            "sql.admit.table_procedure_not_found",
        ),
        (
            "ALTER TABLE lake.ns.t EXECUTE optimize(bad => missing) WHERE id > 0",
            "sql.admit.unsupported_statement",
        ),
        (
            "ALTER TABLE lake.ns.t EXECUTE optimize(bad => missing)",
            "sql.admit.invalid_table_procedure_argument",
        ),
    ] {
        let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
            panic!("expected an ALTER TABLE EXECUTE statement");
        };
        let error = bind_trino_table_execute(
            source,
            &statement,
            &provider,
            &mut Port::new(vec![]),
            &mut NoAbsoluteTime,
            &control,
        )
        .unwrap_err();
        assert_eq!(error.user_error().unwrap().code().as_str(), code);
    }

    let source = "ALTER TABLE lake.ns.t EXECUTE optimize";
    let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
        panic!("expected an ALTER TABLE EXECUTE statement");
    };
    let bound = bind_trino_table_execute(
        source,
        &statement,
        &provider,
        &mut Port::new(vec![]),
        &mut NoAbsoluteTime,
        &control,
    )
    .unwrap();
    assert_eq!(bound.declaration().kind(), TableProcedureKind::Optimize);
    assert!(
        bound
            .arguments()
            .all(|(_, value)| matches!(value, BoundProcedureArgument::Omitted))
    );
}

#[test]
fn table_procedure_timestamp_projection_uses_original_selected_ticks_and_declared_unit() {
    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use novarocks_parser::{ast::TrinoStatement, trino_parser::parse_statement};
    use novarocks_query_application::sql::{
        table_procedure::{
            BoundProcedureArgument, BoundProcedureValue, TableProcedureAbsoluteTimePort,
        },
        trino_table_command::bind_trino_table_execute,
    };
    use novarocks_spi::connector::ConnectorProviderId;

    struct NoLegacyTimestamp;
    impl TableProcedureAbsoluteTimePort for NoLegacyTimestamp {
        fn legacy_timestamp_literal_millis(
            &mut self,
            _: &str,
            _: novarocks_parser::Span,
        ) -> Result<i64, QueryServiceError> {
            panic!("a typed expression does not use the legacy literal parser")
        }
    }

    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let control = Control::default();
    let source = "ALTER TABLE lake.ns.t EXECUTE expire_snapshots(older_than => CAST('1970-01-01' AS TIMESTAMP))";
    let TrinoStatement::TableExecute(statement) = parse_statement(source).unwrap() else {
        panic!("expected ALTER TABLE EXECUTE");
    };
    let cases: [(ArrayRef, Option<i64>); 7] = [
        (
            Arc::new(TimestampSecondArray::from(vec![10, 7])),
            Some(7000),
        ),
        (
            Arc::new(TimestampMillisecondArray::from(vec![10, 7123])),
            Some(7123),
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![10, 7_123_456]).with_timezone("+02:00")),
            Some(7123),
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![10, -1])),
            Some(-1),
        ),
        (
            Arc::new(TimestampSecondArray::from(vec![10, i64::MAX])),
            None,
        ),
        (
            Arc::new(TimestampNanosecondArray::from(vec![Some(10), None])),
            None,
        ),
        (Arc::new(arrow::array::NullArray::new(2)), None),
    ];
    for (array, expected) in cases {
        let mut port = Port::new(vec![Call {
            expression: &statement.arguments[0].value,
            target: Target::Timestamp,
            result: Ok(constant(array, ValueLogicalType::Physical)),
        }]);
        let result = bind_trino_table_execute(
            source,
            &statement,
            &provider,
            &mut port,
            &mut NoLegacyTimestamp,
            &control,
        );
        match expected {
            Some(millis) => {
                let bound = result.unwrap();
                let (_, value) = bound
                    .arguments()
                    .find(|(slot, _)| slot.name == "older_than")
                    .unwrap();
                assert!(matches!(value, BoundProcedureArgument::Present {
                    value: BoundProcedureValue::Value(Value::Bigint(actual)), ..
                } if *actual == millis));
            }
            None => assert_eq!(
                result.unwrap_err().user_error().unwrap().code().as_str(),
                "sql.admit.invalid_table_procedure_argument",
            ),
        }
        port.exhausted();
    }
}
