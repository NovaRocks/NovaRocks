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

use arrow::datatypes::DataType;
use novarocks_expression::{
    testkit::{HarnessControl, constant_policy},
    typing::{
        ChildForm, ChildTyping, Conversion, LiteralEmission, LiteralSyntax, NodeShape, NodeTyping,
        ScalarTypingRoute, TypedLiteral, TypingContext, TypingError, finish_call,
        scalar_typing_route, type_node,
    },
};
use novarocks_functions::{
    FunctionBindingError, FunctionResultType, testkit::installed_builtin_owner_catalogue,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    FunctionIntrinsicRowError, FunctionValueType, OperatorFunction, PureCompileControl,
    ValueLogicalType,
};

fn input(ty: DataType, nullable: bool) -> ChildTyping {
    ChildTyping {
        value_type: FunctionValueType::new(ty, nullable),
        constant: None,
        form: ChildForm::Other,
    }
}

fn literal_child(literal: TypedLiteral, form: ChildForm) -> ChildTyping {
    ChildTyping {
        value_type: literal.value_type,
        constant: Some(literal.constant),
        form,
    }
}

fn context(catalog: &dyn novarocks_expression::typing::ScalarBindingCatalog) -> TypingContext<'_> {
    TypingContext {
        catalog,
        constant_policy: constant_policy(),
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
    }
}

#[test]
fn coalesce_types_null_then_finishes_the_selected_overload_on_converted_children() {
    let catalogue = installed_builtin_owner_catalogue();
    let context = context(catalogue.metadata());
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let mut unknown = input(DataType::Null, true);
    unknown.form = ChildForm::NullLiteral;
    let value = input(DataType::Int32, false);
    let NodeTyping::Call {
        selected,
        mut conversions,
    } = type_node(
        &context,
        NodeShape::CatalogCall {
            name: "coalesce",
            expected_result: None,
        },
        &[unknown, value.clone()],
        &mut work,
    )
    .unwrap()
    else {
        panic!("expected bound COALESCE");
    };
    let Conversion::RetypeLiteral(typed_null) = conversions.remove(0) else {
        panic!("expected typed NULL");
    };
    assert_eq!(
        typed_null.value_type,
        FunctionValueType::new(DataType::Int32, true)
    );
    assert!(matches!(conversions.as_slice(), [Conversion::Keep]));
    let converted = [literal_child(typed_null, ChildForm::NullLiteral), value];
    let exact = finish_call(&context, &selected, &converted, &mut work).unwrap();
    assert_eq!(exact.function_id.as_str(), "builtin.scalar/coalesce/v1");
    assert!(matches!(
        exact.selected.result_type,
        FunctionResultType::Scalar(result) if result == FunctionValueType::new(DataType::Int32, false)
    ));
}

#[test]
fn negation_requires_an_exact_numeric_operand_and_keeps_the_original_overload() {
    let catalogue = installed_builtin_owner_catalogue();
    let context = context(catalogue.metadata());
    let operator = NodeShape::Operator(OperatorFunction::Negation);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    for ty in [DataType::Null, DataType::Utf8, DataType::UInt32] {
        assert!(matches!(
            type_node(&context, operator, &[input(ty, true)], &mut work),
            Err(TypingError::TypeMismatch(_))
        ));
    }
    let value = input(DataType::Int64, true);
    let NodeTyping::Call {
        selected,
        conversions,
    } = type_node(&context, operator, std::slice::from_ref(&value), &mut work).unwrap()
    else {
        panic!("expected a bound operator");
    };
    assert!(matches!(conversions.as_slice(), [Conversion::Keep]));
    assert!(matches!(
        finish_call(
            &context,
            &selected,
            &[input(DataType::Float64, true)],
            &mut work
        ),
        Err(TypingError::Binding(
            FunctionBindingError::NoMatchingOverload | FunctionBindingError::InvalidBinding(_)
        ))
    ));
    let bound = finish_call(&context, &selected, std::slice::from_ref(&value), &mut work).unwrap();
    assert_eq!(bound.function_id, OperatorFunction::Negation.function_id());
    assert_eq!(
        bound.semantics.intrinsic_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    assert!(
        matches!(bound.selected.result_type, FunctionResultType::Scalar(result) if result == value.value_type)
    );
}

#[test]
fn branch_and_boolean_typing_own_the_full_conversion() {
    let catalogue = installed_builtin_owner_catalogue();
    let context = context(catalogue.metadata());
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let mut unknown = input(DataType::Null, true);
    unknown.form = ChildForm::NullLiteral;
    let value = input(DataType::Int32, false);
    let NodeTyping::Value {
        result,
        conversions,
    } = type_node(
        &context,
        NodeShape::SearchedCase {
            when_count: 1,
            has_else: true,
        },
        &[unknown.clone(), value.clone(), unknown.clone()],
        &mut work,
    )
    .unwrap()
    else {
        panic!("expected a CASE value");
    };
    assert_eq!(result, FunctionValueType::new(DataType::Int32, true));
    let [
        Conversion::RetypeLiteral(condition),
        Conversion::Cast(branch),
        Conversion::RetypeLiteral(otherwise),
    ] = conversions.as_slice()
    else {
        panic!("expected explicit BOOL NULL and the two branch conversions");
    };
    assert_eq!(
        condition.value_type,
        FunctionValueType::new(DataType::Boolean, true)
    );
    assert_eq!(branch.target, result);
    assert_eq!(otherwise.value_type, result);

    let NodeTyping::Value { result, .. } = type_node(
        &context,
        NodeShape::And,
        &[unknown, input(DataType::Boolean, false)],
        &mut work,
    )
    .unwrap() else {
        panic!("expected AND value");
    };
    assert_eq!(result, FunctionValueType::new(DataType::Boolean, true));

    assert!(matches!(
        type_node(
            &context,
            NodeShape::Or,
            &[value.clone(), input(DataType::Boolean, false)],
            &mut work
        ),
        Err(TypingError::TypeMismatch(_))
    ));
    let NodeTyping::Value {
        result,
        conversions,
    } = type_node(&context, NodeShape::IsNull, &[value], &mut work).unwrap()
    else {
        panic!("expected IS NULL value");
    };
    assert_eq!(result, FunctionValueType::new(DataType::Boolean, false));
    assert!(matches!(conversions.as_slice(), [Conversion::Keep]));
}

#[test]
fn literal_typing_preserves_date_json_and_exact_temporal_values() {
    let catalogue = installed_builtin_owner_catalogue();
    let context = context(catalogue.metadata());
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let literal = |syntax, work: &mut CompileCheckpoints<'_>| {
        type_node(&context, NodeShape::Literal(syntax), &[], work)
    };

    let NodeTyping::Literal(LiteralEmission::Direct(date)) = literal(
        LiteralSyntax::TypedString {
            name: "DATE",
            value: "1970-01-02",
        },
        &mut work,
    )
    .unwrap() else {
        panic!("expected direct DATE");
    };
    assert_eq!(
        date.value,
        novarocks_expression::typing::LiteralValue::Int(1)
    );
    assert_eq!(date.value_type.data_type(), &DataType::Date32);
    let NodeTyping::Literal(LiteralEmission::Direct(json)) = literal(
        LiteralSyntax::TypedString {
            name: "json",
            value: r#"{"b":1, "a":2}"#,
        },
        &mut work,
    )
    .unwrap() else {
        panic!("expected direct JSON");
    };
    assert_eq!(json.value_type.logical_type(), ValueLogicalType::Json);
    assert_eq!(
        json.value,
        novarocks_expression::typing::LiteralValue::String(r#"{"b":1, "a":2}"#.into())
    );
    let NodeTyping::Literal(LiteralEmission::Constant { value_type, value }) = literal(
        LiteralSyntax::TypedString {
            name: "TIME",
            value: "01:02:03.000000000001",
        },
        &mut work,
    )
    .unwrap() else {
        panic!("expected exact temporal constant");
    };
    use novarocks_type_contract::{TemporalPrecision, TemporalType};
    assert_eq!(
        value_type.value_type().temporal(),
        Some(TemporalType::Time(TemporalPrecision::PICOSECONDS))
    );
    assert!(!value_type.nullable());
    assert_eq!(value.pool().array().data_type(), &DataType::Int64);
}

#[test]
fn shared_route_reserves_special_handling_and_typing_propagates_original_control() {
    assert_eq!(scalar_typing_route("trim"), ScalarTypingRoute::Legacy);
    assert_eq!(scalar_typing_route("btrim"), ScalarTypingRoute::Legacy);
    assert_eq!(scalar_typing_route("coalesce"), ScalarTypingRoute::Shared);
    struct Stop;
    impl PureCompileControl for Stop {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Err(CompileControlError::Cancelled)
        }
    }
    let catalogue = installed_builtin_owner_catalogue();
    struct AllowOnce(std::sync::atomic::AtomicBool);
    impl PureCompileControl for AllowOnce {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            if self.0.swap(false, std::sync::atomic::Ordering::Relaxed) {
                Ok(())
            } else {
                Stop.checkpoint(phase, units)
            }
        }
    }
    let control = AllowOnce(std::sync::atomic::AtomicBool::new(true));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        type_node(
            &context(catalogue.metadata()),
            NodeShape::IsNull,
            &[input(DataType::Int32, true)],
            &mut work
        ),
        Err(TypingError::Control(CompileControlError::Cancelled))
    ));
}
