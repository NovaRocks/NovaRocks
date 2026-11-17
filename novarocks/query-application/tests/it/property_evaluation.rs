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
use novarocks_parser::{SYNTHETIC, ast::Expr, trino_parser::parse_expression};
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
