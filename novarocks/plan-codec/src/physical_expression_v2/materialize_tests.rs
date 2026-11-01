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
    physical_aggregate_binding_v2::{
        materialize_aggregate_bindings, prepare_aggregate_binding_headers,
        prepare_aggregate_bindings_materialization,
    },
    physical_binding_v2::{
        materialize_function_bindings, prepare_function_binding_headers,
        prepare_function_bindings_materialization,
    },
    physical_connector_payload_v2::{ConnectorPayloadProjectionLimits, decode_connector_payloads},
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, decode_values},
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_proto_models::{physical_control_v2::Empty, physical_semantics_v2 as semantics};
use novarocks_type_contract::{
    DecimalOverflowPolicy, FunctionArgumentType, SemanticParameterId, SemanticParameterKey,
    SemanticParameterValue, SemanticParameters,
};
use std::{
    alloc::Layout,
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

const EXPR_SOURCE: usize = 2 * 1024 * 1024;
const SOURCE: usize = 4 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control(Mutex<State>);
#[derive(Default)]
struct State {
    active: bool,
    trace: Vec<u32>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        *self.0.lock().unwrap() = State {
            active: true,
            trace: vec![],
            stop,
        };
    }
    fn disarm(&self) {
        self.0.lock().unwrap().active = false;
    }
    fn trace(&self) -> Vec<u32> {
        self.0.lock().unwrap().trace.clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut s = self.0.lock().unwrap();
        if !s.active {
            return Ok(());
        }
        assert_eq!(phase, CompilePhase::Decode);
        let at = s.trace.len();
        if let Some((stop, _)) = s.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        s.trace.push(units);
        match s.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn limits() -> Limits {
    Limits {
        max_definitions: 1024,
        max_type_references: 8192,
        max_expression_references: 16384,
        max_new_allocation_requests: 8192,
        max_new_allocation_request_bytes: 16 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 32 * 1024 * 1024,
        // The original borrowed comparator charges both primitive roots
        // against the whole 2 MiB source invoice: 320 * 2 * 2 MiB already
        // exceeds 1 GiB, before its fixed headers and namespace work.
        max_cumulative_work: 2_000_000_000,
    }
}
fn blimits() -> BindingProjectionLimits {
    binding_limits(limits())
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 128,
        max_expanded_nodes: 512,
        max_string_bytes: 8192,
    }
}
fn pool_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 3,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 8,
        max_type_nodes: 64,
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
    args: Vec<wire::FunctionArgumentType>,
) -> wire::FunctionBindingDefinition {
    wire::FunctionBindingDefinition {
        id,
        function_id: "original/function".into(),
        overload_id: "original/overload".into(),
        kind: kind as i32,
        arguments: args,
        result: Some(wire::function_binding_definition::Result::ScalarValueTypeId(1)),
    }
}
fn definition(
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
    pools: p::ConstantPools,
}
impl Fixture {
    fn new(control: &Control) -> Self {
        let field = Arc::new(
            Field::new("original-child", DataType::Int32, true)
                .with_metadata(HashMap::from([("unknown".into(), "preserved".into())])),
        );
        let roots = [
            (0, FunctionValueType::new(DataType::Int64, false)),
            (1, FunctionValueType::new(DataType::Int64, true)),
            (2, FunctionValueType::new(DataType::Boolean, false)),
            (3, FunctionValueType::new(DataType::Boolean, true)),
            (
                4,
                FunctionValueType::new(DataType::Struct(vec![field].into()), true),
            ),
            (
                5,
                FunctionValueType::new(
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ),
            (6, FunctionValueType::new(DataType::Utf8, true)),
        ];
        let encoded = encode_type_table(&roots, type_limits(), control).unwrap();
        let types = decode_type_table(&encoded, type_limits(), control).unwrap();
        let carrier_i64 = types
            .carriers
            .iter()
            .find(|(_, t)| t.value == DataType::Int64)
            .map(|(id, _)| *id)
            .unwrap();
        let lambda = wire::FunctionArgumentType {
            kind: Some(wire::function_argument_type::Kind::Lambda(
                wire::LambdaArgumentType {
                    parameter_value_type_ids: vec![1],
                    result_value_type_id: Some(1),
                },
            )),
        };
        let functions = vec![
            function(u32::MAX, wire::FunctionKind::Scalar, vec![arg(1), lambda]),
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
            state_format: "original-state/v1".into(),
            state_argument_contract:
                novarocks_proto_models::common::AggregateStateArgumentContract::ExactSignature
                    as i32,
            intermediate_value_type_id: Some(5),
        }];
        let values = [(0, 0), (1, 6), (2, 4)]
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
        let parameters = SemanticParameters::try_new([(
            SemanticParameterId::new(0),
            SemanticParameterValue::AllowThrowException(false),
        )])
        .unwrap();
        let mut pools = p::ConstantPools::empty();
        let pool = ConstantPool::try_new(
            Arc::new(
                Field::new("original-pool", DataType::Int64, true)
                    .with_metadata(HashMap::from([("unknown.pool".into(), "kept".into())])),
            ),
            types.value_type(1).unwrap().clone(),
            Arc::new(Int64Array::from(vec![Some(999), Some(-7), None])).to_data(),
            pool_policy(),
            CompilePhase::Decode,
            control,
        )
        .unwrap();
        pools
            .insert(p::ConstantPoolId::new(u32::MAX), pool)
            .unwrap();
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
    fn with_tokens<T>(
        &self,
        defs: &[wire::ExpressionDefinition],
        control: &Control,
        action: impl FnOnce(
            &DecodedExpressions<'_, '_, '_>,
            &MaterializedFunctionBindings<'_, '_>,
            &MaterializedAggregateBindings<'_, '_, '_>,
        ) -> T,
    ) -> T {
        let raw_payloads = [];
        let payloads = decode_connector_payloads(
            &raw_payloads,
            4096,
            ConnectorPayloadProjectionLimits {
                max_definitions: 0,
                max_payload_bytes: 0,
                max_allocation_requests: 0,
                max_allocation_request_bytes: 0,
                max_coexisting_source_and_request_bytes: EXPR_SOURCE,
                max_work: 1024 * 1024,
            },
            control,
        )
        .unwrap();
        let vl = ValueProjectionLimits {
            max_definitions: 8,
            max_origin_references: 16,
            max_allocation_requests: 64,
            max_allocation_request_bytes: 128 * 1024,
            max_coexisting_source_and_request_bytes: EXPR_SOURCE,
            max_work: 16 * 1024 * 1024,
            origins: ValueOriginProjectionLimits {
                max_allocation_requests: 64,
                max_allocation_request_bytes: 128 * 1024,
                max_coexisting_source_and_request_bytes: EXPR_SOURCE,
                max_work: 16 * 1024 * 1024,
            },
        };
        let values = decode_values(&self.values, &payloads, &self.types, 256 * 1024, vl).unwrap();
        let fh = prepare_function_binding_headers(
            &self.functions,
            &self.types,
            128 * 1024,
            blimits(),
            control,
        )
        .unwrap();
        let ah = prepare_aggregate_binding_headers(&self.aggregates, &fh, 256 * 1024, blimits())
            .unwrap();
        let functions = materialize_function_bindings(
            prepare_function_bindings_materialization(&fh, 512 * 1024, blimits()).unwrap(),
        )
        .unwrap();
        let aggregates = materialize_aggregate_bindings(
            prepare_aggregate_bindings_materialization(&ah, &functions, 1024 * 1024, blimits())
                .unwrap(),
        )
        .unwrap();
        let expressions = super::super::decode_expression_definitions(
            defs,
            &values,
            &fh,
            &ah,
            &self.parameters,
            &self.pools,
            EXPR_SOURCE,
            limits(),
        )
        .unwrap();
        action(&expressions, &functions, &aggregates)
    }
    fn all(&self) -> Vec<wire::ExpressionDefinition> {
        use wire::expression_definition::Kind as K;
        let mut parameter = definition(
            7,
            1,
            K::LambdaParameter(wire::LambdaParameter {
                lambda_expr_id: Some(13),
                ordinal: 0,
            }),
        );
        parameter.lambda_scope_expr_id = Some(13);
        vec![
            definition(
                6,
                1,
                K::FunctionCall(wire::FunctionCall {
                    function_binding_id: Some(u32::MAX),
                    argument_expr_ids: vec![u32::MAX, 13],
                }),
            ),
            definition(u32::MAX, 0, K::ValueId(0)),
            definition(
                0,
                1,
                K::Literal(wire::ConstantReference {
                    pool_id: Some(u32::MAX),
                    row_ordinal: 1,
                }),
            ),
            definition(
                1,
                1,
                K::Unary(wire::UnaryExpression {
                    op: wire::UnaryOperator::Plus as i32,
                    expr_id: Some(0),
                }),
            ),
            definition(
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
            definition(
                3,
                3,
                K::Conjunction(wire::ExpressionIds {
                    expr_ids: vec![4, 4],
                }),
            ),
            definition(
                4,
                2,
                K::IsNull(wire::IsNullExpression {
                    expr_id: Some(0),
                    negated: false,
                }),
            ),
            definition(
                5,
                3,
                K::Disjunction(wire::ExpressionIds {
                    expr_ids: vec![4, 4],
                }),
            ),
            parameter,
            definition(
                8,
                1,
                K::Cast(wire::CastExpression {
                    expr_id: Some(u32::MAX),
                    target_carrier_type_id: Some(self.carrier_i64),
                    decimal_overflow_policy: semantics::DecimalOverflowPolicy::ReportError as i32,
                    allow_throw_exception: Some(allow()),
                }),
            ),
            definition(
                9,
                3,
                K::InList(wire::InListExpression {
                    expr_id: Some(u32::MAX),
                    list_expr_ids: vec![0, u32::MAX, 0],
                    negated: true,
                }),
            ),
            definition(
                10,
                3,
                K::Between(wire::BetweenExpression {
                    expr_id: Some(0),
                    low_expr_id: Some(0),
                    high_expr_id: Some(u32::MAX),
                    negated: false,
                }),
            ),
            definition(
                11,
                3,
                K::Like(wire::LikeExpression {
                    expr_id: Some(19),
                    pattern_expr_id: Some(19),
                    negated: true,
                }),
            ),
            definition(
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
            definition(
                13,
                1,
                K::Lambda(wire::LambdaExpression {
                    parameter_value_type_ids: vec![1],
                    body_expr_id: Some(7),
                }),
            ),
            definition(
                14,
                2,
                K::IsTruthValue(wire::TruthValueExpression {
                    expr_id: Some(4),
                    value: true,
                    negated: false,
                }),
            ),
            definition(
                15,
                1,
                K::WindowCall(wire::WindowCall {
                    function_binding_id: Some(0),
                    argument_expr_ids: vec![u32::MAX],
                    frame: None,
                    ..Default::default()
                }),
            ),
            definition(
                16,
                1,
                K::WindowCall(wire::WindowCall {
                    function_binding_id: Some(9),
                    argument_expr_ids: vec![u32::MAX, 0],
                    function_order_by: vec![wire::SortExpression {
                        expr_id: Some(0),
                        direction: wire::SortDirection::Descending as i32,
                        null_ordering: wire::NullOrdering::Last as i32,
                    }],
                    frame: Some(wire::WindowFrame {
                        units: wire::WindowFrameUnits::Groups as i32,
                        exclusion: wire::WindowFrameExclusion::Ties as i32,
                        start: Some(wire::WindowBound {
                            kind: Some(wire::window_bound::Kind::PrecedingExprId(0)),
                        }),
                        end: Some(wire::WindowBound {
                            kind: Some(wire::window_bound::Kind::FollowingExprId(u32::MAX)),
                        }),
                    }),
                    ignore_nulls: true,
                    aggregate_binding_id: Some(u32::MAX),
                    ..Default::default()
                }),
            ),
            definition(19, 6, K::ValueId(1)),
        ]
    }
}
fn small() -> Vec<wire::ExpressionDefinition> {
    vec![
        definition(
            0,
            0,
            wire::expression_definition::Kind::Unary(wire::UnaryExpression {
                op: wire::UnaryOperator::Plus as i32,
                expr_id: Some(u32::MAX),
            }),
        ),
        definition(u32::MAX, 0, wire::expression_definition::Kind::ValueId(0)),
    ]
}
fn golden() -> Vec<wire::ExpressionDefinition> {
    use wire::expression_definition::Kind as K;
    vec![
        definition(
            0,
            1,
            K::Literal(wire::ConstantReference {
                pool_id: Some(u32::MAX),
                row_ordinal: 1,
            }),
        ),
        definition(u32::MAX, 0, K::ValueId(0)),
        definition(
            3,
            3,
            K::Conjunction(wire::ExpressionIds {
                expr_ids: vec![0, u32::MAX],
            }),
        ),
        definition(
            13,
            1,
            K::Lambda(wire::LambdaExpression {
                parameter_value_type_ids: vec![5, 4],
                body_expr_id: Some(0),
            }),
        ),
    ]
}
fn owned(f: &Fixture, defs: &[wire::ExpressionDefinition]) -> p::ExprArena {
    let control = Control::default();
    f.with_tokens(defs, &control, |e, fs, ags| {
        materialize_expressions(
            prepare_expression_materialization(
                e,
                fs,
                ags,
                &p::PlanLimits::FROZEN,
                SOURCE,
                limits(),
            )
            .unwrap(),
        )
        .unwrap()
        .into_arena()
    })
}

#[test]
fn owned_expressions_materialize_all_seventeen_kinds_with_independent_payload_oracles() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = f.all();
    let arena = owned(&f, &defs);
    assert_eq!(arena.len(), 19);
    let id = p::ExprId::new;
    let get = |i| &arena.get(id(i)).unwrap().kind;
    let allow = novarocks_type_contract::SemanticParameterRef {
        id: SemanticParameterId::new(0),
        expected_key: SemanticParameterKey::AllowThrowException,
    };
    for (i, expected) in [
        (u32::MAX, p::ExprKind::Value(p::ValueId::new(0))),
        (
            0,
            p::ExprKind::Constant(p::ConstantReference {
                pool: p::ConstantPoolId::new(u32::MAX),
                ordinal: 1,
            }),
        ),
        (
            1,
            p::ExprKind::Unary {
                op: p::UnaryOperator::Plus,
                expr: id(0),
            },
        ),
        (
            2,
            p::ExprKind::Binary {
                left: id(0),
                op: p::BinaryOperator::Add,
                right: id(u32::MAX),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: Some(allow),
            },
        ),
        (
            3,
            p::ExprKind::Conjunction {
                args: Box::from([id(4), id(4)]),
            },
        ),
        (
            4,
            p::ExprKind::IsNull {
                expr: id(0),
                negated: false,
            },
        ),
        (
            5,
            p::ExprKind::Disjunction {
                args: Box::from([id(4), id(4)]),
            },
        ),
        (
            7,
            p::ExprKind::LambdaParameter {
                lambda: id(13),
                ordinal: 0,
            },
        ),
        (
            8,
            p::ExprKind::Cast {
                expr: id(u32::MAX),
                target: DataType::Int64,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: allow,
            },
        ),
        (
            9,
            p::ExprKind::InList {
                expr: id(u32::MAX),
                list: Box::from([id(0), id(u32::MAX), id(0)]),
                negated: true,
            },
        ),
        (
            10,
            p::ExprKind::Between {
                expr: id(0),
                low: id(0),
                high: id(u32::MAX),
                negated: false,
            },
        ),
        (
            11,
            p::ExprKind::Like {
                expr: id(19),
                pattern: id(19),
                negated: true,
            },
        ),
        (
            12,
            p::ExprKind::Case {
                operand: None,
                when_then: Box::from([(id(4), id(0))]),
                else_expr: Some(id(u32::MAX)),
            },
        ),
        (
            13,
            p::ExprKind::Lambda {
                parameter_types: Box::from([FunctionValueType::new(DataType::Int64, true)]),
                body: id(7),
            },
        ),
        (
            14,
            p::ExprKind::IsTruthValue {
                expr: id(4),
                value: true,
                negated: false,
            },
        ),
    ] {
        assert_eq!(*get(i), expected);
    }
    let p::ExprKind::FunctionCall { function, args } = get(6) else {
        panic!("FunctionCall")
    };
    assert_eq!(&**args, &[id(u32::MAX), id(13)]);
    assert_eq!(function.function_id.as_str(), "original/function");
    assert!(function.legacy_metadata.is_none());
    assert_eq!(
        function.result_type,
        FunctionValueType::new(DataType::Int64, true)
    );
    let FunctionArgumentType::Lambda {
        parameter_types,
        result_type,
    } = &function.argument_types[1]
    else {
        panic!("signature Lambda")
    };
    assert_eq!(
        &**parameter_types,
        &[FunctionValueType::new(DataType::Int64, true)]
    );
    assert_eq!(*result_type, FunctionValueType::new(DataType::Int64, true));
    let p::ExprKind::WindowCall {
        frame,
        aggregate_binding,
        ignore_nulls,
        ..
    } = get(15)
    else {
        panic!("Window")
    };
    assert!(frame.is_none());
    assert!(aggregate_binding.is_none());
    assert!(!ignore_nulls);
    let p::ExprKind::WindowCall {
        function,
        args,
        function_order_by,
        frame,
        aggregate_binding,
        ignore_nulls,
        distinct,
    } = get(16)
    else {
        panic!("Aggregate Window")
    };
    assert!(function.legacy_metadata.is_none());
    assert!(!distinct);
    assert!(ignore_nulls);
    assert_eq!(&**args, &[id(u32::MAX), id(0)]);
    assert_eq!(
        &**function_order_by,
        &[p::SortExpr {
            expr: id(0),
            direction: p::SortDirection::Descending,
            null_ordering: p::NullOrdering::Last
        }]
    );
    assert_eq!(
        *frame,
        Some(p::WindowFrame {
            units: p::WindowFrameUnits::Groups,
            exclusion: p::WindowFrameExclusion::Ties,
            start: p::WindowBound::Preceding(id(0)),
            end: p::WindowBound::Following(id(u32::MAX))
        })
    );
    let a = aggregate_binding.as_ref().unwrap();
    assert_eq!(a.phase, p::AggregatePhase::Single);
    assert_eq!(a.state_format.as_str(), "original-state/v1");
    assert_eq!(a.logical_argument_count, 1);
    assert_eq!(
        a.state_argument_contract,
        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature
    );
    assert!(a.function.legacy_metadata.is_none());
    assert_eq!(a.intermediate_type, *f.types.value_type(5).unwrap());
    for raw in &defs {
        let node = arena.get(id(raw.id)).unwrap();
        assert_eq!(node.owner, p::NodeId::new(u32::MAX));
        assert_eq!(node.lambda_scope, raw.lambda_scope_expr_id.map(id));
        assert_eq!(
            node.ty,
            *f.types.value_type(raw.value_type_id.unwrap()).unwrap()
        );
    }
    assert_eq!(arena.get(id(7)).unwrap().lambda_scope, Some(id(13)));
    let pool = f
        .pools
        .entries()
        .get(&p::ConstantPoolId::new(u32::MAX))
        .unwrap();
    assert_eq!(pool.field().name(), "original-pool");
}

fn independent_tree_layout() -> Layout {
    let key = Layout::new::<p::ExprId>();
    let value = Layout::new::<p::ExprNode>();
    let ptr = Layout::new::<usize>();
    let align = key.align().max(value.align()).max(ptr.align());
    // Locked source five leaf fields plus the internal edge-array padding.
    let bytes = ptr.size()
        + 4
        + 11 * (key.size() + value.size())
        + 5 * (align - 1)
        + 12 * ptr.size()
        + (align - 1);
    Layout::from_size_align(bytes, align)
        .unwrap()
        .pad_to_align()
}
#[test]
fn owned_expression_requests_have_independent_stage_tree_dictionary_and_list_layout_oracle() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = golden();
    f.with_tokens(&defs, &control, |e, fs, ags| {
        let prepared = prepare_expression_materialization(
            e,
            fs,
            ags,
            &p::PlanLimits::FROZEN,
            SOURCE,
            limits(),
        )
        .unwrap();
        let facts = *prepared.facts();
        let node = independent_tree_layout();
        let bytes = Layout::array::<p::ExprNode>(4).unwrap().size()
            + 4 * node.size()
            + 2 * Layout::array::<p::ExprId>(2).unwrap().size()
            + 2 * Layout::array::<FunctionValueType>(2).unwrap().size();
        assert_eq!(facts.definition_count, 4);
        assert_eq!(facts.type_reference_count, 6);
        assert_eq!(facts.expression_reference_count, 3);
        assert_eq!(facts.new_allocation_requests_upper_bound, 9);
        assert_eq!(facts.new_allocation_request_bytes_upper_bound, bytes);
        assert_eq!(
            facts.coexisting_source_and_request_bytes_upper_bound,
            SOURCE + bytes
        );
        let out = materialize_expressions(prepared).unwrap();
        assert!(std::ptr::eq(out.expressions(), e));
        assert!(std::ptr::eq(out.functions(), fs));
        assert!(std::ptr::eq(out.aggregates(), ags));
        let payload = Layout::array::<p::ExprId>(2).unwrap().size()
            + Layout::array::<FunctionValueType>(2).unwrap().size();
        assert_eq!(
            out.retained_bytes,
            4 * (size_of::<p::ExprId>() + size_of::<p::ExprNode>()) + payload
        );
        assert!(out.retained_bytes < bytes);
        let p::ExprKind::Lambda {
            parameter_types, ..
        } = &out.arena().get(p::ExprId::new(13)).unwrap().kind
        else {
            panic!("Lambda")
        };
        let DataType::Struct(fields) = &parameter_types[1].data_type() else {
            panic!("Struct")
        };
        let DataType::Struct(original) = &f.types.value_type(4).unwrap().data_type() else {
            panic!("source Struct")
        };
        assert!(Arc::ptr_eq(&fields[0], &original[0]));
        assert!(fields[0].metadata().as_arc().is_none());
        assert_eq!(parameter_types[0].data_type(), &DataType::Utf8);
        assert!(std::ptr::eq(
            parameter_types[0].data_type(),
            f.types.value_type(5).unwrap().data_type()
        ));
    });
}

fn caps(f: Facts) -> Limits {
    Limits {
        max_definitions: f.definition_count,
        max_type_references: f.type_reference_count,
        max_expression_references: f.expression_reference_count,
        max_new_allocation_requests: f.new_allocation_requests_upper_bound,
        max_new_allocation_request_bytes: f.new_allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_cumulative_work: f.cumulative_work_upper_bound,
    }
}
#[test]
fn owned_expression_seven_exact_and_under_axes_keep_known_numeric_primary() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = golden();
    f.with_tokens(&defs, &control, |e, fs, ags| {
        let tight = caps(
            *prepare_expression_materialization(
                e,
                fs,
                ags,
                &p::PlanLimits::FROZEN,
                SOURCE,
                limits(),
            )
            .unwrap()
            .facts(),
        );
        let prepared =
            prepare_expression_materialization(e, fs, ags, &p::PlanLimits::FROZEN, SOURCE, tight)
                .unwrap();
        assert_eq!(materialize_expressions(prepared).unwrap().arena().len(), 4);
        for axis in 0..7 {
            let mut under = tight;
            match axis {
                0 => under.max_definitions -= 1,
                1 => under.max_type_references -= 1,
                2 => under.max_expression_references -= 1,
                3 => under.max_new_allocation_requests -= 1,
                4 => under.max_new_allocation_request_bytes -= 1,
                5 => under.max_coexisting_source_and_request_bytes -= 1,
                6 => under.max_cumulative_work -= 1,
                _ => unreachable!(),
            };
            control.arm(None);
            assert!(matches!(
                prepare_expression_materialization(
                    e,
                    fs,
                    ags,
                    &p::PlanLimits::FROZEN,
                    SOURCE,
                    under
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            control.disarm();
        }
        for late in CAUSES {
            let mut under = limits();
            under.max_definitions = 0;
            control.arm(Some((1, late)));
            assert!(matches!(
                prepare_expression_materialization(
                    e,
                    fs,
                    ags,
                    &p::PlanLimits::FROZEN,
                    SOURCE,
                    under
                ),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), [0]);
            control.disarm();
        }
    });
}

#[test]
fn owned_expression_foreign_equal_namespaces_and_source_floor_remain_ordinary() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = small();
    f.with_tokens(&defs, &control, |e, fs, ags| {
        let fh = prepare_function_binding_headers(
            &f.functions,
            &f.types,
            128 * 1024,
            blimits(),
            &control,
        )
        .unwrap();
        let foreign = materialize_function_bindings(
            prepare_function_bindings_materialization(&fh, 512 * 1024, blimits()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            prepare_expression_materialization(
                e,
                &foreign,
                ags,
                &p::PlanLimits::FROZEN,
                SOURCE,
                limits()
            ),
            Err(Error::InvalidShape(
                "expression materialization has different original namespaces or control"
            ))
        ));
        let known = e
            .retained_floor_observed(
                &mut CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap(),
            )
            .unwrap()
            + fs.retained_output_floor().unwrap()
            + ags.retained_output_floor().unwrap();
        assert!(matches!(
            prepare_expression_materialization(
                e,
                fs,
                ags,
                &p::PlanLimits::FROZEN,
                known - 1,
                limits()
            ),
            Err(Error::Binding(
                crate::physical_binding_v2::BindingCodecError::InvalidShape(
                    "binding materialization source invoice omits original namespace"
                )
            ))
        ));
        let out = materialize_expressions(
            prepare_expression_materialization(
                e,
                fs,
                ags,
                &p::PlanLimits::FROZEN,
                SOURCE,
                limits(),
            )
            .unwrap(),
        )
        .unwrap();
        let foreign_control = Control::default();
        let mut work = CompileCheckpoints::try_new(&foreign_control, CompilePhase::Decode).unwrap();
        assert!(matches!(
            out.definition_observed(0, &mut work),
            Err(Error::InvalidShape(
                "owned expression lookup has a different original control"
            ))
        ));
    });
}

#[test]
fn owned_expression_actual_prepare_emit_and_ordinary_tails_keep_all_small_control_prefixes() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = small();
    f.with_tokens(&defs,&control,|e,fs,ags|{
        for ordinary in [false,true]{
            control.arm(None);let result=prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,if ordinary{1}else{SOURCE},limits());assert_eq!(result.is_ok(),!ordinary);let trace=control.trace();control.disarm();
            for at in 0..trace.len(){for cause in CAUSES{control.arm(Some((at,cause.clone())));assert!(matches!(prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,if ordinary{1}else{SOURCE},limits()),Err(Error::Control(actual)) if actual==cause));assert_eq!(control.trace(),trace[..=at]);control.disarm();}}
        }
        let prepared=prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,SOURCE,limits()).unwrap();control.arm(None);materialize_expressions(prepared).unwrap();let trace=control.trace();control.disarm();
        for at in 0..trace.len(){for cause in CAUSES{let prepared=prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,SOURCE,limits()).unwrap();control.arm(Some((at,cause.clone())));assert!(matches!(materialize_expressions(prepared),Err(Error::Control(actual)) if actual==cause));assert_eq!(control.trace(),trace[..=at]);control.disarm();}}
    });
}

#[test]
fn owned_expression_wide_actual_source_loops_and_arena_preserve_quantum_prefixes() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = (0..320)
        .map(|i| {
            definition(
                if i == 319 { u32::MAX } else { i },
                0,
                wire::expression_definition::Kind::ValueId(0),
            )
        })
        .collect::<Vec<_>>();
    f.with_tokens(&defs,&control,|e,fs,ags|{
        control.arm(None);let prepared=prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,SOURCE,limits()).unwrap();let trace=control.trace();control.disarm();assert!(trace.contains(&256));
        for at in [0,trace.iter().position(|n|*n==256).unwrap(),trace.len()-1]{for cause in CAUSES{control.arm(Some((at,cause.clone())));assert!(matches!(prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,SOURCE,limits()),Err(Error::Control(actual)) if actual==cause));assert_eq!(control.trace(),trace[..=at]);control.disarm();}}
        control.arm(None);let out=materialize_expressions(prepared).unwrap();assert_eq!(out.arena().len(),320);assert_eq!(out.arena().get(p::ExprId::new(u32::MAX)).unwrap().ty,FunctionValueType::new(DataType::Int64,false));let trace=control.trace();control.disarm();
        for at in [0,trace.len()/2,trace.len()-1]{for cause in CAUSES{let prepared=prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,SOURCE,limits()).unwrap();control.arm(Some((at,cause.clone())));assert!(matches!(materialize_expressions(prepared),Err(Error::Control(actual)) if actual==cause));assert_eq!(control.trace(),trace[..=at]);control.disarm();}}
        for late in CAUSES{let mut under=limits();under.max_type_references=255;control.arm(Some((1,late)));assert!(matches!(prepare_expression_materialization(e,fs,ags,&p::PlanLimits::FROZEN,SOURCE,under),Err(Error::Control(CompileControlError::ResourceExhausted))));assert_eq!(control.trace(),[0]);control.disarm();}
    });
}

#[test]
fn owned_expression_arena_enters_actual_sparse_fragment_construction() {
    let control = Control::default();
    let f = Fixture::new(&control);
    let defs = vec![definition(
        0,
        1,
        wire::expression_definition::Kind::Literal(wire::ConstantReference {
            pool_id: Some(u32::MAX),
            row_ordinal: 1,
        }),
    )];
    let arena = owned(&f, &defs);
    let node = p::NodeId::new(u32::MAX);
    let value = p::ValueId::new(u32::MAX);
    let properties = p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::new([]),
    };
    let physical = p::PhysicalNode {
        id: node,
        inputs: Box::new([]),
        required_inputs: Box::new([]),
        output_properties: properties,
        output: p::OutputPort {
            node,
            columns: Box::from([value]),
        },
        kind: p::NodeKind::Values {
            rows: Box::from([Box::from([p::ExprId::new(0)])]),
        },
    };
    let input = p::FragmentStructureInput {
        stage_placement: novarocks_physical_plan::StagePlacementPolicy::Singleton,
        id: p::FragmentId::new(0),
        root: node,
        values: BTreeMap::from([(
            value,
            p::ValueDef {
                id: value,
                ty: FunctionValueType::new(DataType::Int64, true),
                origin: p::ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: 0,
                },
            },
        )]),
        expressions: arena,
        nodes: BTreeMap::from([(node, physical)]),
        sink: p::FragmentSink::Noop,
        dop_domain: p::PipelineDopDomain {
            min: 2,
            max: 4,
            requires_power_of_two: true,
        },
        runtime_filters: Box::new([]),
    };
    let fragment =
        p::Fragment::try_from_structure_observed(input, p::PlanLimits::FROZEN, &control).unwrap();
    assert_eq!(fragment.root(), node);
    assert_eq!(
        fragment.expressions().get(p::ExprId::new(0)).unwrap().kind,
        p::ExprKind::Constant(p::ConstantReference {
            pool: p::ConstantPoolId::new(u32::MAX),
            ordinal: 1
        })
    );
}

#[path = "materialize_owned_tests.rs"]
mod caller_owned_tests;
