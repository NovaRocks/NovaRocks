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

use super::*;
use crate::{
    physical_aggregate_binding_v2::prepare_aggregate_binding_headers,
    physical_binding_v2::{BindingProjectionLimits, prepare_function_binding_headers},
    physical_connector_payload_v2::{ConnectorPayloadProjectionLimits, decode_connector_payloads},
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, decode_values},
};
use arrow::{
    array::{Array, Int32Array, Int64Array, StructArray},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_physical_plan::ConstantReferenceError;
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{
    CompileControlError, SemanticParameterId, SemanticParameterValue, ValueLogicalType,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

const SOURCE: usize = 256 * 1024;
const PREVIOUS: usize = 64 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
#[derive(Default)]
struct Control {
    active: AtomicBool,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn activate(&self) {
        self.trace.lock().unwrap().clear();
        self.active.store(true, Ordering::SeqCst);
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        if !self.active.load(Ordering::SeqCst) {
            return Ok(());
        }
        assert_eq!(phase, CompilePhase::Decode);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after originating refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> ExpressionProjectionLimits {
    ExpressionProjectionLimits {
        max_definitions: 1024,
        max_type_references: 4096,
        max_expression_references: 8192,
        max_new_allocation_requests: 1,
        max_new_allocation_request_bytes: 128 * 1024,
        max_coexisting_source_and_request_bytes: 2 * SOURCE,
        max_cumulative_work: 128 * 1024 * 1024,
    }
}
fn binding_limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 32,
        max_type_references: 128,
        max_request_bytes: 8192,
        max_allocation_requests: 1,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 16 * 1024 * 1024,
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 0,
        max_payload_bytes: 0,
        max_allocation_requests: 0,
        max_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1024 * 1024,
    }
}
fn value_limits() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 8,
        max_origin_references: 16,
        max_allocation_requests: 64,
        max_allocation_request_bytes: PREVIOUS,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 16 * 1024 * 1024,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 8,
            max_allocation_request_bytes: 8192,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 4 * 1024 * 1024,
        },
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 3,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 8,
        max_type_nodes: 32,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 16 << 20,
        max_library_validation_bytes: 16 << 20,
    }
}
fn arg(id: u32) -> wire::FunctionArgumentType {
    wire::FunctionArgumentType {
        kind: Some(wire::function_argument_type::Kind::ValueTypeId(id)),
    }
}
fn function(
    id: u32,
    kind: wire::FunctionKind,
    arguments: Vec<wire::FunctionArgumentType>,
) -> wire::FunctionBindingDefinition {
    wire::FunctionBindingDefinition {
        id,
        function_id: "actual/selected".into(),
        overload_id: "actual/overload".into(),
        kind: kind as i32,
        arguments,
        result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(1)),
    }
}
fn expression(
    id: u32,
    ty: u32,
    kind: wire::expression_definition::Kind,
) -> wire::ExpressionDefinition {
    wire::ExpressionDefinition {
        id,
        owner_node_id: Some(u32::MAX),
        lambda_scope_expr_id: None,
        value_type_id: Some(ty),
        kind: Some(kind),
    }
}
fn allow() -> semantics::SemanticParameterRef {
    semantics::SemanticParameterRef {
        id: Some(0),
        expected_key: semantics::SemanticParameterKey::AllowThrowException as i32,
    }
}
struct Fixture {
    types: DecodedTypeTable,
    carrier_i64: u32,
    functions: Vec<wire::FunctionBindingDefinition>,
    aggregates: Vec<wire::AggregateBindingDefinition>,
    values: Vec<wire::ValueDefinition>,
    parameters: SemanticParameters,
    pools: ConstantPools,
}
impl Fixture {
    fn new() -> Self {
        let child = Arc::new(
            Field::new("original_child", DataType::Int32, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "unknown.child".into(),
                    "unchanged".into(),
                )]),
            ),
        );
        let nested = DataType::Struct(vec![child].into());
        let source_types = [
            (0, FunctionValueType::new(DataType::Int64, false)),
            (1, FunctionValueType::new(DataType::Int64, true)),
            (2, FunctionValueType::new(DataType::Boolean, false)),
            (3, FunctionValueType::new(DataType::Boolean, true)),
            (
                4,
                FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    true,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ),
            (7, FunctionValueType::new(nested.clone(), true)),
            (
                8,
                FunctionValueType::new(
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("".into())),
                    true,
                ),
            ),
            (
                9,
                FunctionValueType::new(
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    true,
                ),
            ),
            (10, FunctionValueType::new(DataType::Utf8, true)),
            (u32::MAX, FunctionValueType::new(DataType::Float64, true)),
        ];
        let bounds = TypeProjectionLimits {
            max_definitions: 128,
            max_expanded_nodes: 512,
            max_string_bytes: 8192,
        };
        let wire = encode_type_table(&source_types, bounds, &Setup).unwrap();
        let types = decode_type_table(&wire, bounds, &Setup).unwrap();
        let carrier_i64 = types
            .carriers
            .iter()
            .find(|(_, value)| value.value == DataType::Int64)
            .map(|(id, _)| *id)
            .unwrap();
        let lambda_arg = wire::FunctionArgumentType {
            kind: Some(wire::function_argument_type::Kind::Lambda(
                wire::LambdaArgumentType {
                    parameter_value_type_ids: vec![1],
                    result_value_type_id: Some(1),
                },
            )),
        };
        let functions = vec![
            function(
                u32::MAX,
                wire::FunctionKind::Scalar,
                vec![arg(1), lambda_arg],
            ),
            function(0, wire::FunctionKind::Window, vec![arg(1)]),
            function(9, wire::FunctionKind::Aggregate, vec![arg(1)]),
        ];
        let aggregates = vec![wire::AggregateBindingDefinition {
            state_interpretation: None,
            id: u32::MAX,
            function_binding_id: Some(9),
            phase: Some(wire::AggregatePhase {
                kind: Some(wire::aggregate_phase::Kind::Single(Empty {})),
            }),
            logical_argument_count: 1,
            state_format: "actual-state/v1".into(),
            state_argument_contract:
                novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature
                    as i32,
            intermediate_value_type_id: Some(1),
        }];
        let values = [(0, 0), (1, 7), (2, 10)]
            .into_iter()
            .map(|(id, ty)| wire::ValueDefinition {
                id,
                value_type_id: Some(ty),
                origin: Some(wire::ValueOrigin {
                    kind: Some(wire::value_origin::Kind::NodeOutput(
                        wire::NodeOutputOrigin {
                            node_id: Some(u32::MAX),
                            output_ordinal: id,
                        },
                    )),
                }),
            })
            .collect();
        let parameters = SemanticParameters::try_new([
            (
                SemanticParameterId::new(0),
                SemanticParameterValue::AllowThrowException(false),
            ),
            (
                SemanticParameterId::new(u32::MAX),
                SemanticParameterValue::AllowThrowException(true),
            ),
            (
                SemanticParameterId::new(5),
                SemanticParameterValue::TimeZone("UTC".into()),
            ),
        ])
        .unwrap();
        let mut pools = ConstantPools::empty();
        let i64type = types.value_type(1).unwrap().clone();
        let field = Arc::new(
            Field::new("original_i64", DataType::Int64, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "source.field".into(),
                    "kept".into(),
                )]),
            ),
        );
        let pool = ConstantPool::try_new(
            field,
            i64type,
            Arc::new(Int64Array::from(vec![Some(999), Some(-7), None])).to_data(),
            policy(),
            CompilePhase::Decode,
            &Setup,
        )
        .unwrap();
        pools.insert(ConstantPoolId::new(u32::MAX), pool).unwrap();
        let nestedtype = types.value_type(7).unwrap().clone();
        let DataType::Struct(fields) = &nested else {
            unreachable!()
        };
        let array = StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(17), None, Some(99)]))],
            None,
        );
        let field = Arc::new(Field::new("original_nested", nested, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source.field".into(),
                "nested".into(),
            )]),
        ));
        let pool = ConstantPool::try_new(
            field,
            nestedtype,
            array.to_data(),
            policy(),
            CompilePhase::Decode,
            &Setup,
        )
        .unwrap();
        pools.insert(ConstantPoolId::new(7), pool).unwrap();
        Self {
            types,
            carrier_i64,
            functions,
            aggregates,
            values,
            parameters,
            pools,
        }
    }
    fn with_receivers<T>(
        &self,
        control: &Control,
        run: impl FnOnce(
            &DecodedValues<'_, '_, '_>,
            &PreparedFunctionBindingHeaders<'_>,
            &PreparedAggregateBindingHeaders<'_, '_>,
        ) -> T,
    ) -> T {
        let empty = [];
        let payloads = decode_connector_payloads(&empty, 4096, payload_limits(), control).unwrap();
        let values = decode_values(
            &self.values,
            &payloads,
            &self.types,
            PREVIOUS,
            value_limits(),
        )
        .unwrap();
        let functions = prepare_function_binding_headers(
            &self.functions,
            &self.types,
            PREVIOUS,
            binding_limits(),
            control,
        )
        .unwrap();
        let aggregates = prepare_aggregate_binding_headers(
            &self.aggregates,
            &functions,
            2 * PREVIOUS,
            binding_limits(),
        )
        .unwrap();
        run(&values, &functions, &aggregates)
    }
    fn all_kinds(&self) -> Vec<wire::ExpressionDefinition> {
        use wire::expression_definition::Kind as K;
        let mut parameter = expression(
            7,
            1,
            K::LambdaParameter(wire::LambdaParameter {
                lambda_expr_id: Some(13),
                ordinal: 0,
            }),
        );
        parameter.lambda_scope_expr_id = Some(13);
        let mut defs = vec![
            expression(
                6,
                1,
                K::FunctionCall(wire::FunctionCall {
                    function_binding_id: Some(u32::MAX),
                    argument_expr_ids: vec![u32::MAX, 13],
                }),
            ),
            expression(u32::MAX, 0, K::ValueId(0)),
            expression(
                0,
                1,
                K::Literal(wire::ConstantReference {
                    pool_id: Some(u32::MAX),
                    row_ordinal: 1,
                }),
            ),
            expression(
                1,
                1,
                K::Unary(wire::UnaryExpression {
                    op: wire::UnaryOperator::Plus as i32,
                    expr_id: Some(0),
                }),
            ),
            expression(
                2,
                1,
                K::Binary(wire::BinaryExpression {
                    left_expr_id: Some(0),
                    right_expr_id: Some(u32::MAX),
                    op: wire::BinaryOperator::Add as i32,
                    decimal_overflow_policy: semantics::DecimalOverflowPolicy::OutputNull as i32,
                    allow_throw_exception: Some(allow()),
                }),
            ),
            expression(
                3,
                3,
                K::Conjunction(wire::ExpressionIds {
                    expr_ids: vec![4, 4],
                }),
            ),
            expression(
                4,
                2,
                K::IsNull(wire::IsNullExpression {
                    expr_id: Some(0),
                    negated: false,
                }),
            ),
            expression(
                5,
                3,
                K::Disjunction(wire::ExpressionIds {
                    expr_ids: vec![4, 4],
                }),
            ),
            parameter,
            expression(
                8,
                1,
                K::Cast(wire::CastExpression {
                    expr_id: Some(u32::MAX),
                    target_carrier_type_id: Some(self.carrier_i64),
                    decimal_overflow_policy: semantics::DecimalOverflowPolicy::ReportError as i32,
                    allow_throw_exception: Some(allow()),
                }),
            ),
            expression(
                9,
                3,
                K::InList(wire::InListExpression {
                    expr_id: Some(u32::MAX),
                    list_expr_ids: vec![0, u32::MAX, 0],
                    negated: true,
                }),
            ),
            expression(
                10,
                3,
                K::Between(wire::BetweenExpression {
                    expr_id: Some(0),
                    low_expr_id: Some(0),
                    high_expr_id: Some(u32::MAX),
                    negated: false,
                }),
            ),
            expression(
                11,
                3,
                K::Like(wire::LikeExpression {
                    expr_id: Some(19),
                    pattern_expr_id: Some(19),
                    negated: false,
                }),
            ),
            expression(
                12,
                1,
                K::CaseExpression(wire::CaseExpression {
                    operand_expr_id: None,
                    arms: vec![wire::WhenThen {
                        when_expr_id: Some(4),
                        then_expr_id: Some(0),
                    }],
                    else_expr_id: Some(u32::MAX),
                }),
            ),
            expression(
                13,
                1,
                K::Lambda(wire::LambdaExpression {
                    parameter_value_type_ids: vec![1],
                    body_expr_id: Some(7),
                }),
            ),
            expression(
                14,
                2,
                K::IsTruthValue(wire::TruthValueExpression {
                    expr_id: Some(4),
                    value: true,
                    negated: false,
                }),
            ),
            expression(
                15,
                1,
                K::WindowCall(wire::WindowCall {
                    function_binding_id: Some(0),
                    argument_expr_ids: vec![u32::MAX],
                    frame: None,
                    ..Default::default()
                }),
            ),
            expression(
                16,
                1,
                K::WindowCall(wire::WindowCall {
                    function_binding_id: Some(9),
                    argument_expr_ids: vec![u32::MAX, 0],
                    function_order_by: vec![wire::SortExpression {
                        expr_id: Some(0),
                        direction: wire::SortDirection::Ascending as i32,
                        null_ordering: wire::NullOrdering::First as i32,
                    }],
                    frame: Some(wire::WindowFrame {
                        units: wire::WindowFrameUnits::Rows as i32,
                        exclusion: wire::WindowFrameExclusion::NoOthers as i32,
                        start: Some(wire::WindowBound {
                            kind: Some(wire::window_bound::Kind::PrecedingExprId(0)),
                        }),
                        end: Some(wire::WindowBound {
                            kind: Some(wire::window_bound::Kind::CurrentRow(Empty {})),
                        }),
                    }),
                    aggregate_binding_id: Some(u32::MAX),
                    ..Default::default()
                }),
            ),
            expression(
                17,
                7,
                K::Literal(wire::ConstantReference {
                    pool_id: Some(7),
                    row_ordinal: 1,
                }),
            ),
            expression(19, 10, K::ValueId(2)),
        ];
        // Inputs deliberately retain author order; IDs never drive capacity.
        defs.reverse();
        defs
    }
}
fn small() -> Vec<wire::ExpressionDefinition> {
    vec![
        expression(
            0,
            0,
            wire::expression_definition::Kind::Unary(wire::UnaryExpression {
                op: wire::UnaryOperator::Plus as i32,
                expr_id: Some(u32::MAX),
            }),
        ),
        expression(u32::MAX, 0, wire::expression_definition::Kind::ValueId(0)),
    ]
}
fn run(
    fixture: &Fixture,
    defs: &[wire::ExpressionDefinition],
    source: usize,
    bounds: ExpressionProjectionLimits,
    control: &Control,
) -> Result<ExpressionNamespaceReadFacts, Error> {
    fixture.with_receivers(control, |values, functions, aggregates| {
        control.activate();
        decode_expression_definitions(
            defs,
            values,
            functions,
            aggregates,
            &fixture.parameters,
            &fixture.pools,
            source,
            bounds,
        )
        .map(|token| *token.facts())
    })
}
fn prefixes(
    fixture: &Fixture,
    defs: &[wire::ExpressionDefinition],
    source: usize,
    bounds: ExpressionProjectionLimits,
    success: bool,
    all: bool,
) {
    let good = Control::default();
    assert_eq!(run(fixture, defs, source, bounds, &good).is_ok(), success);
    let trace = good.trace();
    assert!(!trace.is_empty());
    // An ordinary exit can flush its completed opaque work before the
    // enclosing finish, whose actual tail then contains zero units. Replay
    // both positive and zero tails; never invent an extra completed unit.
    assert!(trace.len() >= 2);
    for (at, (_, units)) in trace.iter().enumerate() {
        if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = Control {
                refusal: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(run(fixture, defs, source, bounds, &control), Err(Error::Control(actual)) if actual == cause),
                "{at}/{cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn receiving_expressions_cover_all_seventeen_kinds_and_original_sparse_correspondence() {
    use wire::expression_definition::Kind as K;
    let fixture = Fixture::new();
    let definitions = fixture.all_kinds();
    let control = Control::default();
    fixture.with_receivers(&control, |values, functions, aggregates| {
        control.activate();
        let token = decode_expression_definitions(
            &definitions,
            values,
            functions,
            aggregates,
            &fixture.parameters,
            &fixture.pools,
            SOURCE,
            limits(),
        )
        .unwrap();
        assert!(std::ptr::eq(token.as_wire(), definitions.as_slice()));
        assert!(std::ptr::eq(token.values(), values));
        assert!(std::ptr::eq(token.types(), &fixture.types));
        assert!(std::ptr::eq(token.functions(), functions));
        assert!(std::ptr::eq(token.aggregates(), aggregates));
        assert!(std::ptr::eq(token.parameters(), &fixture.parameters));
        assert!(std::ptr::eq(token.pools(), &fixture.pools));
        assert_eq!(token.source_count(), definitions.len());
        assert_eq!(token.facts().new_allocation_requests_upper_bound, 1);
        assert_eq!(
            token.facts().new_allocation_request_bytes_upper_bound,
            definitions.len() * size_of::<usize>()
        );
        assert_eq!(
            token
                .facts()
                .coexisting_source_and_request_bytes_upper_bound,
            SOURCE + definitions.len() * size_of::<usize>()
        );
        let mut kinds = Vec::new();
        for definition in &definitions {
            let actual = token.definition(definition.id).unwrap().unwrap();
            assert!(std::ptr::eq(actual, definition));
            assert_eq!(token.source_id(actual).unwrap(), definition.id);
            assert!(std::ptr::eq(
                token.value_type(actual.id).unwrap().unwrap(),
                fixture
                    .types
                    .value_type(actual.value_type_id.unwrap())
                    .unwrap()
            ));
            let kind = std::mem::discriminant(actual.kind.as_ref().unwrap());
            if !kinds.contains(&kind) {
                kinds.push(kind);
            }
        }
        assert_eq!(kinds.len(), 17);
        assert!(token.definition(123456).unwrap().is_none());
        let zero = token.definition(0).unwrap().unwrap();
        let Some(K::Literal(reference)) = zero.kind.as_ref() else {
            panic!("actual selected constant");
        };
        assert_eq!(reference.pool_id, Some(u32::MAX));
        assert_eq!(reference.row_ordinal, 1);
        assert_eq!(
            fixture
                .pools
                .entries()
                .get(&ConstantPoolId::new(u32::MAX))
                .unwrap()
                .value(1)
                .unwrap()
                .try_i64()
                .unwrap(),
            Some(-7)
        );
        let Some(K::InList(list)) = token.definition(9).unwrap().unwrap().kind.as_ref() else {
            panic!("actual in-list");
        };
        assert_eq!(list.list_expr_ids, [0, u32::MAX, 0]);
        let Some(K::WindowCall(window)) = token.definition(15).unwrap().unwrap().kind.as_ref()
        else {
            panic!("actual window");
        };
        assert!(window.frame.is_none());
        // Frozen expected nullable=true accepts this actual nonnullable Value
        // shape. Domain/fits proof remains with the original Fragment author.
        assert_eq!(functions.as_wire()[0].arguments[0], arg(1));
        assert!(!fixture.types.value_type(0).unwrap().nullable());
        let foreign = zero.clone();
        assert!(matches!(
            token.source_id(&foreign),
            Err(Error::InvalidShape(_))
        ));
        assert!(
            token.retained_invoice_floor().unwrap()
                >= SOURCE + definitions.len() * size_of::<usize>()
        );
    });
}

#[test]
fn receiving_expressions_reject_missing_closed_payloads_and_forward_reference_failures() {
    use wire::expression_definition::Kind as K;
    let fixture = Fixture::new();
    let valid = small();
    let mut cases = Vec::new();
    let mut bad = valid.clone();
    bad[0].kind = None;
    cases.push(bad);
    let mut bad = valid.clone();
    bad[0].owner_node_id = None;
    cases.push(bad);
    let mut bad = valid.clone();
    bad[0].value_type_id = None;
    cases.push(bad);
    let mut bad = valid.clone();
    bad[0].value_type_id = Some(404);
    cases.push(bad);
    let mut bad = valid.clone();
    bad[0].id = u32::MAX;
    cases.push(bad);
    let mut bad = valid.clone();
    bad[0].lambda_scope_expr_id = Some(404);
    cases.push(bad);
    for (op, expr_id) in [
        (0, Some(u32::MAX)),
        (99, Some(u32::MAX)),
        (wire::UnaryOperator::Plus as i32, None),
        (wire::UnaryOperator::Plus as i32, Some(404)),
    ] {
        let mut bad = valid.clone();
        bad[0].kind = Some(K::Unary(wire::UnaryExpression { op, expr_id }));
        cases.push(bad);
    }
    let mut bad = valid.clone();
    bad[0].kind = Some(K::CaseExpression(wire::CaseExpression {
        arms: vec![wire::WhenThen {
            when_expr_id: None,
            then_expr_id: Some(u32::MAX),
        }],
        ..Default::default()
    }));
    cases.push(bad);
    for case in cases {
        assert!(run(&fixture, &case, SOURCE, limits(), &Control::default()).is_err());
    }
    let mut forward = valid;
    forward[0].kind = Some(K::Unary(wire::UnaryExpression {
        op: wire::UnaryOperator::Minus as i32,
        expr_id: Some(u32::MAX),
    }));
    assert!(run(&fixture, &forward, SOURCE, limits(), &Control::default()).is_ok());
}

#[test]
fn receiving_constants_preserve_selected_ordinal_and_require_exact_nested_source_type() {
    use wire::expression_definition::Kind as K;
    let fixture = Fixture::new();
    let source = expression(
        0,
        7,
        K::Literal(wire::ConstantReference {
            pool_id: Some(7),
            row_ordinal: 1,
        }),
    );
    let facts = run(
        &fixture,
        std::slice::from_ref(&source),
        SOURCE,
        limits(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(facts.new_allocation_requests_upper_bound, 1);
    assert_eq!(
        facts.new_allocation_request_bytes_upper_bound,
        size_of::<usize>()
    );
    let original = fixture
        .pools
        .entries()
        .get(&ConstantPoolId::new(7))
        .unwrap();
    let selected = original.value(1).unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert!(std::ptr::eq(selected.field(), original.field()));
    for (pool_id, row_ordinal) in [(None, 1), (Some(404), 1), (Some(7), 3)] {
        let mut bad = source.clone();
        bad.kind = Some(K::Literal(wire::ConstantReference {
            pool_id,
            row_ordinal,
        }));
        assert!(run(&fixture, &[bad], SOURCE, limits(), &Control::default()).is_err());
    }
    // A separate, valid received root differs in actual child nullability.
    let mut changed = Fixture::new();
    let DataType::Struct(fields) = &changed.types.value_type(7).unwrap().data_type() else {
        panic!("nested source");
    };
    let wrong = FunctionValueType::new(
        DataType::Struct(
            vec![Arc::new(
                fields[0]
                    .as_ref()
                    .clone()
                    .with_nullable(!fields[0].is_nullable()),
            )]
            .into(),
        ),
        true,
    );
    let bounds = TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 512,
        max_string_bytes: 8192,
    };
    let mut roots = changed
        .types
        .value_types()
        .map(|(id, value)| (id, value.clone()))
        .collect::<Vec<_>>();
    roots.push((42, wrong));
    changed.types = decode_type_table(
        &encode_type_table(&roots, bounds, &Setup).unwrap(),
        bounds,
        &Setup,
    )
    .unwrap();
    let mut bad = source;
    bad.value_type_id = Some(42);
    assert!(matches!(
        run(&changed, &[bad], SOURCE, limits(), &Control::default()),
        Err(Error::Constant(ConstantReferenceError::SourceTypeMismatch(
            _
        )))
    ));
    let value = expression(0, 1, K::ValueId(0));
    assert!(
        run(&fixture, &[value], SOURCE, limits(), &Control::default()).is_err(),
        "Value-ref header must exactly match its definition, including root nullable"
    );
    let mut bad = expression(
        0,
        0,
        K::Literal(wire::ConstantReference {
            pool_id: Some(u32::MAX),
            row_ordinal: 1,
        }),
    );
    assert!(matches!(
        run(
            &fixture,
            std::slice::from_ref(&bad),
            SOURCE,
            limits(),
            &Control::default()
        ),
        Err(Error::Constant(ConstantReferenceError::SourceTypeMismatch(
            _
        )))
    ));
    bad.value_type_id = Some(4);
    assert!(run(&fixture, &[bad], SOURCE, limits(), &Control::default()).is_err());
}

#[test]
fn receiving_calls_lambdas_and_window_payloads_use_exact_original_namespaces() {
    use wire::expression_definition::Kind as K;
    let fixture = Fixture::new();
    let valid = fixture.all_kinds();
    let mut cases = Vec::new();
    for id in [404, 0, 9] {
        let mut bad = valid.clone();
        let call = bad.iter_mut().find(|def| def.id == 6).unwrap();
        call.kind = Some(K::FunctionCall(wire::FunctionCall {
            function_binding_id: Some(id),
            argument_expr_ids: vec![u32::MAX, 13],
        }));
        cases.push(bad);
    }
    for args in [vec![u32::MAX], vec![u32::MAX, 0], vec![13, 13]] {
        let mut bad = valid.clone();
        let call = bad.iter_mut().find(|def| def.id == 6).unwrap();
        call.kind = Some(K::FunctionCall(wire::FunctionCall {
            function_binding_id: Some(u32::MAX),
            argument_expr_ids: args,
        }));
        cases.push(bad);
    }
    let mut bad = valid.clone();
    bad.iter_mut()
        .find(|def| def.id == 6)
        .unwrap()
        .value_type_id = Some(0);
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::Lambda(lambda)) = bad
        .iter_mut()
        .find(|def| def.id == 13)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    lambda.parameter_value_type_ids = vec![0];
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::LambdaParameter(parameter)) = bad
        .iter_mut()
        .find(|def| def.id == 7)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    parameter.ordinal = 1;
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::WindowCall(window)) = bad
        .iter_mut()
        .find(|def| def.id == 15)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    window.distinct = true;
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::WindowCall(window)) = bad
        .iter_mut()
        .find(|def| def.id == 16)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    window.aggregate_binding_id = None;
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::WindowCall(window)) = bad
        .iter_mut()
        .find(|def| def.id == 16)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    window.function_order_by[0].direction = 0;
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::WindowCall(window)) = bad
        .iter_mut()
        .find(|def| def.id == 16)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    window.frame.as_mut().unwrap().end = None;
    cases.push(bad);
    let mut bad = valid.clone();
    let Some(K::WindowCall(window)) = bad
        .iter_mut()
        .find(|def| def.id == 16)
        .unwrap()
        .kind
        .as_mut()
    else {
        unreachable!()
    };
    window.frame.as_mut().unwrap().units = 0;
    cases.push(bad);
    for case in cases {
        assert!(run(&fixture, &case, SOURCE, limits(), &Control::default()).is_err());
    }
    let control = Control::default();
    fixture.with_receivers(&control, |values, functions, aggregates| {
        let foreign = prepare_function_binding_headers(
            &fixture.functions,
            &fixture.types,
            PREVIOUS,
            binding_limits(),
            &control,
        )
        .unwrap();
        control.activate();
        assert!(matches!(
            decode_expression_definitions(
                &valid,
                values,
                &foreign,
                aggregates,
                &fixture.parameters,
                &fixture.pools,
                SOURCE,
                limits()
            ),
            Err(Error::InvalidShape(_))
        ));
        assert!(std::ptr::eq(functions.type_table(), foreign.type_table()));
    });
    let control = Control::default();
    fixture.with_receivers(&control, |values, _, _| {
        let foreign_control = Control::default();
        let foreign = prepare_function_binding_headers(
            &fixture.functions,
            &fixture.types,
            PREVIOUS,
            binding_limits(),
            &foreign_control,
        )
        .unwrap();
        let aggregate = prepare_aggregate_binding_headers(
            &fixture.aggregates,
            &foreign,
            2 * PREVIOUS,
            binding_limits(),
        )
        .unwrap();
        control.activate();
        assert!(matches!(
            decode_expression_definitions(
                &valid,
                values,
                &foreign,
                &aggregate,
                &fixture.parameters,
                &fixture.pools,
                SOURCE,
                limits()
            ),
            Err(Error::InvalidShape(_))
        ));
    });
}

#[test]
fn receiving_cast_and_all_arithmetic_parameter_references_are_not_defaults() {
    use wire::expression_definition::Kind as K;
    let fixture = Fixture::new();
    for op in [
        wire::BinaryOperator::Add,
        wire::BinaryOperator::Subtract,
        wire::BinaryOperator::Multiply,
        wire::BinaryOperator::Divide,
        wire::BinaryOperator::Modulo,
    ] {
        for id in [0, u32::MAX] {
            let mut defs = small();
            defs[0].value_type_id = Some(1);
            defs[0].kind = Some(K::Binary(wire::BinaryExpression {
                left_expr_id: Some(u32::MAX),
                right_expr_id: Some(u32::MAX),
                op: op as i32,
                decimal_overflow_policy: semantics::DecimalOverflowPolicy::ReportError as i32,
                allow_throw_exception: Some(semantics::SemanticParameterRef {
                    id: Some(id),
                    ..allow()
                }),
            }));
            assert!(run(&fixture, &defs, SOURCE, limits(), &Control::default()).is_ok());
            let Some(K::Binary(binary)) = defs[0].kind.as_mut() else {
                unreachable!()
            };
            binary.allow_throw_exception = None;
            assert!(run(&fixture, &defs, SOURCE, limits(), &Control::default()).is_err());
        }
    }
    let cast = expression(
        0,
        1,
        K::Cast(wire::CastExpression {
            expr_id: Some(u32::MAX),
            target_carrier_type_id: Some(fixture.carrier_i64),
            decimal_overflow_policy: semantics::DecimalOverflowPolicy::OutputNull as i32,
            allow_throw_exception: Some(allow()),
        }),
    );
    let mut defs = vec![cast, small().pop().unwrap()];
    for reference in [
        semantics::SemanticParameterRef {
            id: None,
            ..allow()
        },
        semantics::SemanticParameterRef {
            id: Some(404),
            ..allow()
        },
        semantics::SemanticParameterRef {
            id: Some(5),
            ..allow()
        },
        semantics::SemanticParameterRef {
            expected_key: semantics::SemanticParameterKey::TimeZone as i32,
            ..allow()
        },
    ] {
        let Some(K::Cast(value)) = defs[0].kind.as_mut() else {
            unreachable!()
        };
        value.allow_throw_exception = Some(reference);
        assert!(run(&fixture, &defs, SOURCE, limits(), &Control::default()).is_err());
    }
    let Some(K::Cast(value)) = defs[0].kind.as_mut() else {
        unreachable!()
    };
    value.allow_throw_exception = Some(allow());
    value.target_carrier_type_id = Some(404);
    assert!(run(&fixture, &defs, SOURCE, limits(), &Control::default()).is_err());
    // None and present-empty timezone are separate same-unit full carriers.
    let none_zone = fixture
        .types
        .carriers
        .iter()
        .find(|(_, carrier)| {
            carrier.value == DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        })
        .map(|(id, _)| *id)
        .unwrap();
    let Some(K::Cast(value)) = defs[0].kind.as_mut() else {
        unreachable!()
    };
    value.target_carrier_type_id = Some(none_zone);
    defs[0].value_type_id = Some(8);
    assert!(run(&fixture, &defs, SOURCE, limits(), &Control::default()).is_err());
    let mut binary = small();
    binary[0].kind = Some(K::Binary(wire::BinaryExpression {
        left_expr_id: Some(u32::MAX),
        right_expr_id: Some(u32::MAX),
        op: wire::BinaryOperator::Eq as i32,
        decimal_overflow_policy: semantics::DecimalOverflowPolicy::OutputNull as i32,
        allow_throw_exception: Some(allow()),
    }));
    assert!(run(&fixture, &binary, SOURCE, limits(), &Control::default()).is_err());
}

#[test]
fn receiving_expression_caps_and_source_capacity_are_admitted_before_index_requests() {
    let fixture = Fixture::new();
    let defs = small();
    let facts = run(&fixture, &defs, SOURCE, limits(), &Control::default()).unwrap();
    let exact = ExpressionProjectionLimits {
        max_definitions: facts.definition_count,
        max_type_references: facts.type_reference_count,
        max_expression_references: facts.expression_reference_count,
        max_new_allocation_requests: facts.new_allocation_requests_upper_bound,
        max_new_allocation_request_bytes: facts.new_allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_cumulative_work: facts.cumulative_work_upper_bound,
    };
    assert!(run(&fixture, &defs, SOURCE, exact, &Control::default()).is_ok());
    for field in 0..7 {
        let mut under = exact;
        match field {
            0 => under.max_definitions -= 1,
            1 => under.max_type_references -= 1,
            2 => under.max_expression_references -= 1,
            3 => under.max_new_allocation_requests -= 1,
            4 => under.max_new_allocation_request_bytes -= 1,
            5 => under.max_coexisting_source_and_request_bytes -= 1,
            _ => under.max_cumulative_work -= 1,
        }
        assert!(
            run(&fixture, &defs, SOURCE, under, &Control::default()).is_err(),
            "limit {field}"
        );
    }
    assert!(run(&fixture, &defs, 0, limits(), &Control::default()).is_err());
    let mut sparse = defs.clone();
    let mut refs = Vec::with_capacity(SOURCE / 2);
    refs.extend([u32::MAX, u32::MAX]);
    sparse[0].kind = Some(wire::expression_definition::Kind::Conjunction(
        wire::ExpressionIds { expr_ids: refs },
    ));
    assert!(
        run(&fixture, &sparse, SOURCE, limits(), &Control::default()).is_err(),
        "source must cover actual capacity, not just two live references"
    );
    let empty = run(&fixture, &[], SOURCE, limits(), &Control::default()).unwrap();
    assert_eq!(empty.new_allocation_requests_upper_bound, 0);
    assert_eq!(empty.new_allocation_request_bytes_upper_bound, 0);
    assert_eq!(empty.definition_count, 0);
}

#[test]
fn receiving_expression_success_ordinary_tails_and_real_quantum_preserve_all_control_prefixes() {
    let fixture = Fixture::new();
    let defs = small();
    prefixes(&fixture, &defs, SOURCE, limits(), true, true);
    let mut bad = defs.clone();
    bad[0].kind = Some(wire::expression_definition::Kind::Unary(
        wire::UnaryExpression {
            op: wire::UnaryOperator::Plus as i32,
            expr_id: Some(404),
        },
    ));
    prefixes(&fixture, &bad, SOURCE, limits(), false, true);
    prefixes(&fixture, &[], SOURCE, limits(), true, true);
    // Exercise the actual nested borrowed comparison followed by selected
    // address resolution at every callback, including both ordinary exits.
    let mut literal = expression(
        0,
        7,
        wire::expression_definition::Kind::Literal(wire::ConstantReference {
            pool_id: Some(7),
            row_ordinal: 1,
        }),
    );
    prefixes(
        &fixture,
        std::slice::from_ref(&literal),
        SOURCE,
        limits(),
        true,
        true,
    );
    literal.kind = Some(wire::expression_definition::Kind::Literal(
        wire::ConstantReference {
            pool_id: Some(7),
            row_ordinal: 3,
        },
    ));
    let ordinal_exit = Control::default();
    assert!(matches!(
        run(
            &fixture,
            std::slice::from_ref(&literal),
            SOURCE,
            limits(),
            &ordinal_exit
        ),
        Err(Error::Constant(ConstantReferenceError::Constant(
            novarocks_constant_contract::ConstantError::Invalid(_)
        )))
    ));
    let trace = ordinal_exit.trace();
    // Source resolution completes the pool and ordinal visits (two units),
    // the opaque exit flush observes them, then enclosing ordinary finish
    // observes its zero tail before returning the original ordinal error.
    assert_eq!(
        &trace[trace.len() - 2..],
        &[(CompilePhase::Decode, 2), (CompilePhase::Decode, 0)]
    );
    prefixes(
        &fixture,
        std::slice::from_ref(&literal),
        SOURCE,
        limits(),
        false,
        true,
    );
    literal.value_type_id = Some(1);
    prefixes(
        &fixture,
        std::slice::from_ref(&literal),
        SOURCE,
        limits(),
        false,
        true,
    );
    let mut wide = (0..320)
        .map(|id| {
            expression(
                id,
                0,
                wire::expression_definition::Kind::Unary(wire::UnaryExpression {
                    op: wire::UnaryOperator::Plus as i32,
                    expr_id: Some(u32::MAX),
                }),
            )
        })
        .collect::<Vec<_>>();
    wide.push(expression(
        u32::MAX,
        0,
        wire::expression_definition::Kind::ValueId(0),
    ));
    wide.reverse();
    let good = Control::default();
    assert!(run(&fixture, &wide, SOURCE, limits(), &good).is_ok());
    assert!(good.trace().iter().any(|(_, units)| *units == 256));
    prefixes(&fixture, &wide, SOURCE, limits(), true, false);
    let mut ordinary = wide;
    ordinary[159].owner_node_id = None;
    prefixes(&fixture, &ordinary, SOURCE, limits(), false, false);
}
