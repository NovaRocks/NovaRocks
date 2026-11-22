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
    physical_aggregate_binding_v2::{AggregateBindingInput, encode_aggregate_bindings},
    physical_binding_v2::{
        ArgumentTypeIds, BindingProjectionLimits, BindingSource, FunctionBindingInput,
        ResultTypeIds, encode_function_bindings,
    },
    physical_type_v2::{TypeProjectionLimits, encode_type_table_sources},
};
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field},
};
use novarocks_constant_contract::{ConstantPolicy, ConstantPool};
use novarocks_expression_contract::{BoundFunction, UnaryOperator};
use novarocks_physical_plan::{
    AggregateBinding, AggregatePhase, AggregateSequenceId, ConstantPoolId, ConstantReference,
    ValueId,
};
use novarocks_type_contract::{
    AggregateStateFormatId, CompileControlError, DecimalOverflowPolicy, FunctionArgumentEvaluation,
    FunctionArgumentType, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionOverloadId, FunctionValueType, FunctionVolatility, SemanticParameterId,
    SemanticParameterRef, SemanticParameterValue, ValueLogicalType,
};
use std::{
    collections::HashMap,
    mem::{size_of, size_of_val},
    sync::{Arc, Mutex},
};

const SOURCE: usize = 1 << 20;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Encode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn limits() -> ExpressionProjectionLimits {
    ExpressionProjectionLimits {
        max_definitions: 1024,
        max_type_references: 4096,
        max_expression_references: 8192,
        max_new_allocation_requests: 4096,
        max_new_allocation_request_bytes: 1 << 20,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_cumulative_work: 2_000_000_000,
    }
}
fn binding_limits() -> BindingProjectionLimits {
    BindingProjectionLimits {
        max_definitions: 16,
        max_type_references: 64,
        max_request_bytes: 1 << 20,
        max_allocation_requests: 128,
        max_coexisting_source_and_request_bytes: 8 << 20,
        max_work: 128 << 20,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 1 << 20,
    }
}
fn int() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn boolean() -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, true)
}
fn allow(id: u32) -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(id),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
fn function(kind: FunctionKind) -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new("test.expression/signature").unwrap(),
        overload: FunctionOverloadId::try_new("test.expression/signature/exact").unwrap(),
        kind,
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::from(
            [FunctionArgumentType::Value(int())],
        )),
        result_type: int(),

        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::new([]),
        }),
    }
}
fn aggregate(function: &BoundFunction) -> AggregateBinding {
    AggregateBinding {
        state_interpretation: None,
        function: function.clone(),
        phase: AggregatePhase::Partial {
            sequence: AggregateSequenceId::new(u32::MAX),
        },
        logical_argument_count: 1,
        intermediate_type: int(),
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
        state_format: AggregateStateFormatId::try_new("test.expression/state-v1").unwrap(),
    }
}
fn definitions(nodes: Vec<ExprNode>) -> Box<[ExprNode]> {
    nodes.into_boxed_slice()
}
fn node(id: u32, kind: ExprKind, ty: FunctionValueType) -> ExprNode {
    ExprNode {
        id: ExprId::new(id),
        lambda_scope: None,
        ty,
        kind,
    }
}
#[derive(Clone)]
struct Input {
    id: u32,
    type_id: u32,
    parameters: Vec<u32>,
    function: Option<u32>,
}
struct Fixture {
    definitions: Box<[ExprNode]>,
    roots: Vec<(u32, FunctionValueType)>,
    input: Vec<Input>,
    functions: [BoundFunction; 3],
    aggregate: AggregateBinding,
    parameters: SemanticParameters,
    pools: ConstantPools,
    other_type_token: bool,
    other_function_token: bool,
}
impl Fixture {
    fn new(nodes: Vec<ExprNode>) -> Self {
        let functions = [
            function(FunctionKind::Scalar),
            function(FunctionKind::Window),
            function(FunctionKind::Aggregate),
        ];
        let aggregate = aggregate(&functions[2]);
        let input = nodes
            .iter()
            .map(|n| Input {
                id: n.id.get(),
                type_id: if n.ty.data_type() == &DataType::Boolean {
                    u32::MAX
                } else {
                    0
                },
                parameters: if matches!(n.kind, ExprKind::Lambda { .. }) {
                    vec![0, 7]
                } else {
                    vec![]
                },
                function: match n.kind {
                    ExprKind::FunctionCall { .. } => Some(0),
                    _ => None,
                },
            })
            .collect();
        let array = Int64Array::from(vec![Some(99), None, Some(71)]);
        let pool = ConstantPool::try_new(
            Arc::new(Field::new("original", DataType::Int64, true)),
            int(),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Setup,
        )
        .unwrap();
        let mut pools = ConstantPools::empty();
        pools.insert(ConstantPoolId::new(u32::MAX), pool).unwrap();
        Self {
            definitions: definitions(nodes),
            roots: vec![
                (0, int()),
                (7, FunctionValueType::new(DataType::Int64, false)),
                (u32::MAX, boolean()),
            ],
            input,
            functions,
            aggregate,
            parameters: SemanticParameters::try_new([
                (
                    SemanticParameterId::new(0),
                    SemanticParameterValue::AllowThrowException(false),
                ),
                (
                    SemanticParameterId::new(u32::MAX),
                    SemanticParameterValue::AllowThrowException(true),
                ),
            ])
            .unwrap(),
            pools,
            other_type_token: false,
            other_function_token: false,
        }
    }
    fn replace(&mut self, index: usize, edit: impl FnOnce(&mut ExprNode)) {
        let mut nodes = self.definitions.to_vec();
        edit(&mut nodes[index]);
        self.definitions = definitions(nodes);
    }
    fn single(kind: ExprKind) -> Self {
        Self::new(vec![node(0, kind, int())])
    }
    fn run(
        &self,
        control: &Control,
        source: usize,
        caps: ExpressionProjectionLimits,
        emit: bool,
    ) -> Result<
        (
            ExpressionNamespaceWriteFacts,
            Vec<wire::ExpressionDefinition>,
        ),
        Error,
    > {
        self.with_prepared(control, source, caps, |prepared| {
            let facts = *prepared.facts();
            Ok((
                facts,
                if emit {
                    prepared.emit()?.into_wire()
                } else {
                    vec![]
                },
            ))
        })
    }
    fn with_prepared<T>(
        &self,
        control: &Control,
        source: usize,
        caps: ExpressionProjectionLimits,
        consume: impl FnOnce(PreparedExpressionNamespaceWrite<'_, '_, '_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.with_sources(|types, functions, aggregates, input| {
            let prepared = prepare_expression_definitions(
                Some(ExpressionDefinitionSource::new(&self.definitions, &[])),
                input,
                types,
                functions,
                aggregates,
                &self.parameters,
                &self.pools,
                source,
                caps,
                control,
            )?;
            consume(prepared)
        })
    }
    fn with_sources<T>(
        &self,
        consume: impl FnOnce(
            &EncodedTypeTable<'_>,
            &EncodedFunctionBindings<'_, '_>,
            &EncodedAggregateBindings<'_, '_>,
            &[ExpressionTypeIds<'_>],
        ) -> T,
    ) -> T {
        // These original namespace owners are constructed on a separate setup
        // control. Only the actual expression prepare/emit trace is replayed.
        let types = encode_type_table_sources(&self.roots, &[], type_limits(), &Setup).unwrap();
        let other_types = self
            .other_type_token
            .then(|| encode_type_table_sources(&self.roots, &[], type_limits(), &Setup).unwrap());
        let args = [ArgumentTypeIds::Value(0)];
        let functions_input = [0, 1, u32::MAX]
            .into_iter()
            .zip(&self.functions)
            .map(|(id, source)| FunctionBindingInput {
                id,
                source: BindingSource::Scalar(source),
                arguments: &args,
                result: ResultTypeIds::Scalar(0),
            })
            .collect::<Vec<_>>();
        let functions =
            encode_function_bindings(&types, &functions_input, SOURCE, binding_limits(), &Setup)
                .unwrap();
        let aggregate_input = [AggregateBindingInput {
            id: u32::MAX,
            source: &self.aggregate,
            function_binding_id: u32::MAX,
            intermediate_value_type_id: 0,
        }];
        let aggregates = encode_aggregate_bindings(
            &types,
            &functions,
            &aggregate_input,
            SOURCE,
            binding_limits(),
            &Setup,
        )
        .unwrap();
        let other_function_input = [7, 8, u32::MAX]
            .into_iter()
            .zip(&self.functions)
            .map(|(id, source)| FunctionBindingInput {
                id,
                source: BindingSource::Scalar(source),
                arguments: &args,
                result: ResultTypeIds::Scalar(0),
            })
            .collect::<Vec<_>>();
        let other_functions = self.other_function_token.then(|| {
            encode_function_bindings(
                &types,
                &other_function_input,
                SOURCE,
                binding_limits(),
                &Setup,
            )
            .unwrap()
        });
        let input = self
            .input
            .iter()
            .map(|x| ExpressionTypeIds {
                expr: ExprId::new(x.id),
                value_type_id: x.type_id,
                lambda_parameter_type_ids: &x.parameters,
                function_binding_id: x.function,
            })
            .collect::<Vec<_>>();
        consume(
            other_types.as_ref().unwrap_or(&types),
            other_functions.as_ref().unwrap_or(&functions),
            &aggregates,
            &input,
        )
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 65536,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
fn all_kinds() -> Fixture {
    let scalar = function(FunctionKind::Scalar);
    let a = ExprId::new(0);
    let z = ExprId::new(15);
    let kinds = vec![
        ExprKind::Value(ValueId::new(u32::MAX)),
        ExprKind::LambdaParameter {
            lambda: ExprId::new(8),
            ordinal: 1,
        },
        ExprKind::Constant(ConstantReference {
            pool: ConstantPoolId::new(u32::MAX),
            ordinal: 2,
        }),
        ExprKind::Unary {
            op: UnaryOperator::Not,
            expr: z,
        },
        ExprKind::Binary {
            left: z,
            op: BinaryOperator::Add,
            right: a,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            allow_throw_exception: Some(allow(0)),
        },
        ExprKind::Conjunction {
            args: Box::from([z, a, z]),
        },
        ExprKind::Disjunction {
            args: Box::from([a, z, a]),
        },
        ExprKind::FunctionCall {
            function: scalar,
            args: Box::from([z]),
        },
        ExprKind::Lambda {
            parameter_types: Box::from([int(), FunctionValueType::new(DataType::Int64, false)]),
            body: a,
        },
        ExprKind::Cast {
            expr: a,
            target: DataType::Int64,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: allow(u32::MAX),
            array_failure_mode: None,
        },
        ExprKind::IsNull {
            expr: a,
            negated: true,
        },
        ExprKind::InList {
            expr: a,
            list: Box::from([z, a, z]),
            negated: true,
        },
        ExprKind::Between {
            expr: z,
            low: a,
            high: z,
            negated: false,
        },
        ExprKind::Like {
            expr: a,
            pattern: z,
            negated: true,
        },
        ExprKind::Case {
            ieee_float_equality: false,
            operand: Some(z),
            when_then: Box::from([(a, z), (a, a)]),
            else_expr: Some(a),
        },
        ExprKind::IsTruthValue {
            expr: z,
            value: false,
            negated: true,
        },
    ];
    let nodes = kinds
        .into_iter()
        .enumerate()
        .map(|(i, kind)| {
            let id = i as u32;
            let ty = if matches!(i, 3 | 5 | 6 | 10 | 11 | 12 | 13 | 15) {
                boolean()
            } else {
                int()
            };
            let mut n = node(id, kind, ty);
            if i == 1 {
                n.lambda_scope = Some(ExprId::new(8));
            }
            n
        })
        .collect();
    Fixture::new(nodes)
}
fn prefixes(
    f: impl Fn(
        &Control,
    ) -> Result<
        (
            ExpressionNamespaceWriteFacts,
            Vec<wire::ExpressionDefinition>,
        ),
        Error,
    >,
    success: bool,
) {
    let good = Control::default();
    assert_eq!(f(&good).is_ok(), success);
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(f(&c),Err(Error::Control(actual)) if actual==cause),
                "callback {at}: {cause:?}"
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn namespace_all_sixteen_scalar_kinds_preserve_dense_headers_and_ordered_occurrences() {
    use wire::expression_definition::Kind;
    let f = all_kinds();
    let (facts, out) = f.run(&Control::default(), SOURCE, limits(), true).unwrap();
    assert_eq!(out.len(), 16);
    assert_eq!(facts.definition_count, 16);
    assert_eq!(facts.type_reference_count, 19);
    assert_eq!(facts.expression_reference_count, 31);
    for (index, def) in out.iter().enumerate() {
        let id = index as u32;
        assert_eq!(def.id, id);
        assert_eq!(
            def.lambda_scope_expr_id,
            if index == 1 { Some(8) } else { None }
        );
        assert_eq!(def.value_type_id, Some(f.input[index].type_id));
    }
    assert_eq!(out[0].kind, Some(Kind::ValueId(u32::MAX)));
    assert_eq!(
        out[1].kind,
        Some(Kind::LambdaParameter(wire::LambdaParameter {
            lambda_expr_id: Some(8),
            ordinal: 1
        }))
    );
    assert_eq!(
        out[2].kind,
        Some(Kind::Constant(wire::ConstantReference {
            pool_id: Some(u32::MAX),
            row_ordinal: 2
        }))
    );
    assert_eq!(
        out[3].kind,
        Some(Kind::Unary(wire::UnaryExpression {
            op: wire::UnaryOperator::Not as i32,
            expr_id: Some(15)
        }))
    );
    let Some(Kind::Binary(binary)) = &out[4].kind else {
        panic!("binary")
    };
    assert_eq!(
        (binary.left_expr_id, binary.right_expr_id, binary.op),
        (Some(15), Some(0), wire::BinaryOperator::Add as i32)
    );
    assert_eq!(binary.allow_throw_exception.as_ref().unwrap().id, Some(0));
    assert_eq!(
        out[5].kind,
        Some(Kind::Conjunction(wire::ExpressionIds {
            expr_ids: vec![15, 0, 15]
        }))
    );
    assert_eq!(
        out[6].kind,
        Some(Kind::Disjunction(wire::ExpressionIds {
            expr_ids: vec![0, 15, 0]
        }))
    );
    assert_eq!(
        out[7].kind,
        Some(Kind::FunctionCall(wire::FunctionCall {
            function_binding_id: Some(0),
            argument_expr_ids: vec![15]
        }))
    );
    assert_eq!(
        out[8].kind,
        Some(Kind::Lambda(wire::LambdaExpression {
            parameter_value_type_ids: vec![0, 7],
            body_expr_id: Some(0)
        }))
    );
    let Some(Kind::Cast(cast)) = &out[9].kind else {
        panic!("cast")
    };
    let original_types = encode_type_table_sources(&f.roots, &[], type_limits(), &Setup).unwrap();
    let original_carrier = original_types
        .as_wire()
        .value_types
        .iter()
        .find(|definition| definition.id == 0)
        .unwrap()
        .carrier_type_id;
    assert_eq!(cast.target_carrier_type_id, original_carrier);
    assert_eq!(
        cast.allow_throw_exception.as_ref().unwrap().id,
        Some(u32::MAX)
    );
    assert_eq!(cast.expr_id, Some(0));
    assert_eq!(
        out[10].kind,
        Some(Kind::IsNull(wire::IsNullExpression {
            expr_id: Some(0),
            negated: true
        }))
    );
    assert_eq!(
        out[11].kind,
        Some(Kind::InList(wire::InListExpression {
            expr_id: Some(0),
            list_expr_ids: vec![15, 0, 15],
            negated: true
        }))
    );
    assert_eq!(
        out[12].kind,
        Some(Kind::Between(wire::BetweenExpression {
            expr_id: Some(15),
            low_expr_id: Some(0),
            high_expr_id: Some(15),
            negated: false
        }))
    );
    assert_eq!(
        out[13].kind,
        Some(Kind::Like(wire::LikeExpression {
            expr_id: Some(0),
            pattern_expr_id: Some(15),
            negated: true
        }))
    );
    assert_eq!(
        out[14].kind,
        Some(Kind::CaseExpression(wire::CaseExpression {
            ieee_float_equality: Some(false),
            operand_expr_id: Some(15),
            arms: vec![
                wire::WhenThen {
                    when_expr_id: Some(0),
                    then_expr_id: Some(15)
                },
                wire::WhenThen {
                    when_expr_id: Some(0),
                    then_expr_id: Some(0)
                }
            ],
            else_expr_id: Some(0)
        }))
    );
    assert_eq!(
        out[15].kind,
        Some(Kind::IsTruthValue(wire::TruthValueExpression {
            expr_id: Some(15),
            value: false,
            negated: true
        }))
    );
    // This is definition projection, not proof of a complete executable graph.
}

#[test]
fn namespace_full_type_and_original_binding_tokens_refuse_substitution() {
    let rejected = |fixture: &Fixture| {
        assert!(
            fixture
                .run(&Control::default(), SOURCE, limits(), true)
                .is_err()
        )
    };
    let mut f = all_kinds();
    f.input[0].type_id = 18;
    rejected(&f);
    let mut f = all_kinds();
    f.input[0].type_id = 7;
    rejected(&f); // root nullable differs
    let mut f = all_kinds();
    f.input[7].function = Some(17);
    rejected(&f);
    let mut f = all_kinds();
    f.input[8].parameters.swap(0, 1);
    rejected(&f);
    let mut f = all_kinds();
    f.other_type_token = true;
    rejected(&f);
    // Equal type roots do not prove the same function namespace emission.
    // No expression function lookup is needed to expose this mismatch.
    let mut distinct = Fixture::single(ExprKind::Value(ValueId::new(0)));
    distinct.other_function_token = true;
    rejected(&distinct);

    let mut f = all_kinds();
    f.replace(9, |n| {
        if let ExprKind::Cast { target, .. } = &mut n.kind {
            *target = DataType::Float64
        }
    });
    rejected(&f);
    let mut f = all_kinds();
    f.replace(7, |n| {
        if let ExprKind::FunctionCall { function, .. } = &mut n.kind {
            function.overload = FunctionOverloadId::try_new("test.expression/foreign").unwrap()
        }
    });
    rejected(&f);
    let mut f = all_kinds();
    f.replace(7, |n| {
        if let ExprKind::FunctionCall { function, .. } = &mut n.kind {
            function.argument_types =
                novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::from([
                    FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, false)),
                ]));
        }
    });
    rejected(&f);
    let mut f = all_kinds();
    f.replace(7, |n| {
        if let ExprKind::FunctionCall { function, .. } = &mut n.kind {
            function.result_type = FunctionValueType::new(DataType::Int64, false);
        }
    });
    rejected(&f);
    let mut f = all_kinds();
    f.input[0].id = 17;
    rejected(&f);
    let mut f = all_kinds();
    f.input[0].parameters = vec![0];
    rejected(&f);
    for (left, right) in [
        (
            FunctionValueType::try_from_legacy_declared_projection(
                DataType::Utf8,
                true,
                ValueLogicalType::Json,
            )
            .expect("valid temporary legacy value projection"),
            FunctionValueType::new(DataType::Utf8, true),
        ),
        (
            FunctionValueType::new(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("a", DataType::Int64, true)
                            .with_metadata(HashMap::from([("origin".into(), "left".into())])),
                    )]
                    .into(),
                ),
                true,
            ),
            FunctionValueType::new(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("a", DataType::Int64, false)
                            .with_metadata(HashMap::from([("origin".into(), "right".into())])),
                    )]
                    .into(),
                ),
                true,
            ),
        ),
    ] {
        let mut f = Fixture::new(vec![node(0, ExprKind::Value(ValueId::new(0)), left)]);
        f.roots.push((17, right));
        f.input[0].type_id = 17;
        rejected(&f);
    }
}

#[test]
fn namespace_intrinsics_require_original_boolean_parameters_and_exact_presence() {
    use wire::expression_definition::Kind;
    for op in [
        BinaryOperator::Add,
        BinaryOperator::Subtract,
        BinaryOperator::Multiply,
        BinaryOperator::Divide,
        BinaryOperator::Modulo,
    ] {
        for id in [0, u32::MAX] {
            let f = Fixture::single(ExprKind::Binary {
                left: ExprId::new(0),
                op,
                right: ExprId::new(u32::MAX),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: Some(allow(id)),
            });
            let out = f
                .run(&Control::default(), SOURCE, limits(), true)
                .unwrap()
                .1;
            let Some(Kind::Binary(binary)) = &out[0].kind else {
                panic!("binary")
            };
            assert_eq!(binary.allow_throw_exception.as_ref().unwrap().id, Some(id));
            assert_eq!(
                f.parameters.require(allow(id)).unwrap(),
                &SemanticParameterValue::AllowThrowException(id == u32::MAX)
            );
            let mut missing = f;
            missing.parameters = SemanticParameters::default();
            assert!(
                missing
                    .run(&Control::default(), SOURCE, limits(), false)
                    .is_err()
            );
        }
        let missing = Fixture::single(ExprKind::Binary {
            left: ExprId::new(0),
            op,
            right: ExprId::new(0),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: None,
        });
        assert!(
            missing
                .run(&Control::default(), SOURCE, limits(), false)
                .is_err()
        );
    }
    let mut wrong = Fixture::single(ExprKind::Cast {
        expr: ExprId::new(0),
        target: DataType::Int64,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        allow_throw_exception: SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        },
        array_failure_mode: None,
    });
    assert!(
        wrong
            .run(&Control::default(), SOURCE, limits(), false)
            .is_err()
    );
    wrong.replace(0, |n| {
        if let ExprKind::Cast {
            allow_throw_exception,
            ..
        } = &mut n.kind
        {
            *allow_throw_exception = allow(0)
        }
    });
    wrong.parameters = SemanticParameters::try_new([(
        SemanticParameterId::new(0),
        SemanticParameterValue::GroupConcatLegacy(false),
    )])
    .unwrap();
    assert!(
        wrong
            .run(&Control::default(), SOURCE, limits(), false)
            .is_err()
    );
    for present in [false, true] {
        let f = Fixture::single(ExprKind::Binary {
            left: ExprId::new(0),
            op: BinaryOperator::Eq,
            right: ExprId::new(0),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: present.then(|| allow(0)),
        });
        assert_eq!(
            f.run(&Control::default(), SOURCE, limits(), true).is_ok(),
            !present
        );
    }
}

#[test]
fn namespace_constant_selected_ordinal_and_retained_floor_are_checked_before_emission() {
    use wire::expression_definition::Kind;
    let mut f = Fixture::single(ExprKind::Constant(ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 2,
    }));
    let identity = f
        .pools
        .entries()
        .get(&ConstantPoolId::new(u32::MAX))
        .unwrap()
        .backing_identity();
    let out = f
        .run(&Control::default(), SOURCE, limits(), true)
        .unwrap()
        .1;
    assert_eq!(
        out[0].kind,
        Some(Kind::Constant(wire::ConstantReference {
            pool_id: Some(u32::MAX),
            row_ordinal: 2
        }))
    );
    assert_eq!(
        f.pools
            .entries()
            .get(&ConstantPoolId::new(u32::MAX))
            .unwrap()
            .value(2)
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(71)
    );
    assert_eq!(
        f.pools
            .entries()
            .get(&ConstantPoolId::new(u32::MAX))
            .unwrap()
            .backing_identity(),
        identity
    );
    f.replace(0, |n| {
        if let ExprKind::Constant(reference) = &mut n.kind {
            reference.ordinal = 1;
        }
    });
    let null_output = f
        .run(&Control::default(), SOURCE, limits(), true)
        .unwrap()
        .1;
    assert_eq!(
        null_output[0].kind,
        Some(Kind::Constant(wire::ConstantReference {
            pool_id: Some(u32::MAX),
            row_ordinal: 1,
        }))
    );
    assert_eq!(
        f.pools
            .entries()
            .get(&ConstantPoolId::new(u32::MAX))
            .unwrap()
            .value(1)
            .unwrap()
            .try_i64()
            .unwrap(),
        None
    );
    f.replace(0, |n| {
        if let ExprKind::Constant(reference) = &mut n.kind {
            reference.ordinal = 3
        }
    });
    assert!(f.run(&Control::default(), SOURCE, limits(), false).is_err());
    f.replace(0, |n| {
        if let ExprKind::Constant(reference) = &mut n.kind {
            reference.pool = ConstantPoolId::new(0)
        }
    });
    assert!(f.run(&Control::default(), SOURCE, limits(), false).is_err());
    let mut mismatch = Fixture::single(ExprKind::Constant(ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 1,
    }));
    mismatch.replace(0, |n| n.ty = FunctionValueType::new(DataType::Int64, false));
    mismatch.input[0].type_id = 7;
    assert!(
        mismatch
            .run(&Control::default(), SOURCE, limits(), false)
            .is_err()
    );
    // An admitted large source backing remains live even though this selected
    // constant addresses only one row. A small invoice cannot hide it.
    let array = Int64Array::from(vec![71; 16384]);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("large", DataType::Int64, true)),
        int(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Setup,
    )
    .unwrap();
    let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes).unwrap();
    let mut large = Fixture::single(ExprKind::Constant(ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 0,
    }));
    large.pools = ConstantPools::empty();
    large
        .pools
        .insert(ConstantPoolId::new(u32::MAX), pool)
        .unwrap();
    let c = Control::default();
    let result = large.run(&c, retained - 1, limits(), false);
    assert!(matches!(
        result,
        Err(Error::InvalidShape("expression writer envelope exceeded"))
    ));
    assert!(
        large
            .run(&Control::default(), SOURCE, limits(), true)
            .is_ok()
    );
}

#[test]
fn namespace_seven_projection_limits_admit_exact_facts_and_refuse_one_over() {
    let f = all_kinds();
    let facts = f
        .run(&Control::default(), SOURCE, limits(), false)
        .unwrap()
        .0;
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
    assert!(f.run(&Control::default(), SOURCE, exact, true).is_ok());
    for field in 0..7 {
        let mut cap = exact;
        match field {
            0 => cap.max_definitions -= 1,
            1 => cap.max_type_references -= 1,
            2 => cap.max_expression_references -= 1,
            3 => cap.max_new_allocation_requests -= 1,
            4 => cap.max_new_allocation_request_bytes -= 1,
            5 => cap.max_coexisting_source_and_request_bytes -= 1,
            _ => cap.max_cumulative_work -= 1,
        };
        assert!(
            f.run(&Control::default(), SOURCE, cap, true).is_err(),
            "limit {field}"
        );
    }
    assert!(f.run(&Control::default(), 0, limits(), false).is_err());
    let mut no_count_walk = limits();
    no_count_walk.max_cumulative_work = 95 * facts.definition_count;
    let c = Control::default();
    assert!(f.run(&c, SOURCE, no_count_walk, false).is_err());
    assert!(!c.trace.lock().unwrap().contains(&256));
    // Empty source namespaces genuinely require no root backing or requests.
    let types = encode_type_table_sources(&[], &[], type_limits(), &Setup).unwrap();
    let functions = encode_function_bindings(&types, &[], 0, binding_limits(), &Setup).unwrap();
    let aggregates =
        encode_aggregate_bindings(&types, &functions, &[], 0, binding_limits(), &Setup).unwrap();
    let empty_definitions = definitions(vec![]);
    let parameters = SemanticParameters::default();
    let pools = ConstantPools::empty();
    let c = Control::default();
    let zero = ExpressionProjectionLimits {
        max_definitions: 0,
        max_type_references: 0,
        max_expression_references: 0,
        max_new_allocation_requests: 0,
        max_new_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: 0,
        max_cumulative_work: limits().max_cumulative_work,
    };
    let prepared = prepare_expression_definitions(
        Some(ExpressionDefinitionSource::new(&empty_definitions, &[])),
        &[],
        &types,
        &functions,
        &aggregates,
        &parameters,
        &pools,
        0,
        zero,
        &c,
    )
    .unwrap();
    assert_eq!(prepared.facts().new_allocation_requests_upper_bound, 0);
    let empty_work = prepared.facts().cumulative_work_upper_bound;
    assert!(empty_work > 0);
    assert!(prepared.emit().unwrap().as_wire().is_empty());
    let mut under = zero;
    under.max_cumulative_work = empty_work - 1;
    assert!(
        prepare_expression_definitions(
            Some(ExpressionDefinitionSource::new(&empty_definitions, &[])),
            &[],
            &types,
            &functions,
            &aggregates,
            &parameters,
            &pools,
            0,
            under,
            &Control::default(),
        )
        .is_err()
    );
}

#[test]
fn namespace_original_owned_boxes_cannot_be_hidden_by_inline_source_invoice() {
    let f = Fixture::single(ExprKind::Conjunction {
        args: vec![ExprId::new(0); 320].into_boxed_slice(),
    });
    // This is the same numerical inline/namespace floor used by the owner,
    // independently computed before adding the original owned argument Box.
    let types = encode_type_table_sources(&f.roots, &[], type_limits(), &Setup).unwrap();
    let args = [ArgumentTypeIds::Value(0)];
    let input = [0, 1, u32::MAX]
        .into_iter()
        .zip(&f.functions)
        .map(|(id, source)| FunctionBindingInput {
            id,
            source: BindingSource::Scalar(source),
            arguments: &args,
            result: ResultTypeIds::Scalar(0),
        })
        .collect::<Vec<_>>();
    let functions =
        encode_function_bindings(&types, &input, SOURCE, binding_limits(), &Setup).unwrap();
    let aggregate_input = [AggregateBindingInput {
        id: u32::MAX,
        source: &f.aggregate,
        function_binding_id: u32::MAX,
        intermediate_value_type_id: 0,
    }];
    let aggregates = encode_aggregate_bindings(
        &types,
        &functions,
        &aggregate_input,
        SOURCE,
        binding_limits(),
        &Setup,
    )
    .unwrap();
    let table = types.as_wire();
    let floor = size_of::<ExprNode>()
        + size_of::<ExpressionTypeIds<'_>>()
        + std::mem::size_of_val(functions.as_wire())
        + std::mem::size_of_val(aggregates.as_wire())
        + size_of::<novarocks_proto_models::physical_type_v2::CarrierTypeDefinition>()
            * table.carriers.capacity()
        + size_of::<novarocks_proto_models::physical_type_v2::ValueTypeDefinition>()
            * table.value_types.capacity()
        + size_of::<novarocks_proto_models::physical_type_v2::FieldDefinition>()
            * table.fields.capacity();
    assert!(f.run(&Control::default(), floor, limits(), false).is_err());
    assert!(f.run(&Control::default(), SOURCE, limits(), true).is_ok());
}

#[test]
fn namespace_prepare_and_consuming_emit_preserve_every_small_callback_and_ordinary_tail() {
    let f = Fixture::single(ExprKind::Cast {
        expr: ExprId::new(u32::MAX),
        target: DataType::Int64,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        allow_throw_exception: allow(0),
        array_failure_mode: None,
    });
    prefixes(|c| f.run(c, SOURCE, limits(), false), true);
    prefixes(|c| f.run(c, SOURCE, limits(), true), true);
    let mut bad = f;
    bad.replace(0, |n| {
        if let ExprKind::Cast { target, .. } = &mut n.kind {
            *target = DataType::Utf8
        }
    });
    prefixes(|c| bad.run(c, SOURCE, limits(), true), false);
    let mut no_key = all_kinds();
    no_key.parameters = SemanticParameters::default();
    prefixes(|c| no_key.run(c, SOURCE, limits(), true), false);
}

#[test]
fn sealed_namespace_preserves_original_loans_dense_sources_and_selected_constant() {
    let f = Fixture::new(vec![
        node(0, ExprKind::Value(ValueId::new(u32::MAX)), int()),
        node(
            1,
            ExprKind::Constant(ConstantReference {
                pool: ConstantPoolId::new(u32::MAX),
                ordinal: 2,
            }),
            int(),
        ),
    ]);
    let c = Control::default();
    f.with_prepared(&c, SOURCE, limits(), |prepared| {
        let definitions = prepared.unit.unwrap().definitions();
        let types = prepared.types;
        let functions = prepared._functions;
        let aggregates = prepared._aggregates;
        let facts = prepared.facts;
        let encoded = prepared.emit()?;
        assert!(std::ptr::eq(
            encoded.source().unwrap().definitions(),
            definitions
        ));
        assert!(std::ptr::eq(encoded.types(), types));
        assert!(std::ptr::eq(encoded.functions(), functions));
        assert!(std::ptr::eq(encoded.aggregates(), aggregates));
        assert!(std::ptr::eq(encoded.parameters(), &f.parameters));
        assert!(std::ptr::eq(encoded.pools(), &f.pools));
        assert!(std::ptr::eq(
            encoded.original_control(),
            &c as &dyn PureCompileControl
        ));
        assert_eq!(encoded.facts().definition_count, facts.definition_count);
        assert_eq!(encoded.source_count(), 2);
        assert_eq!(
            encoded.as_wire().iter().map(|n| n.id).collect::<Vec<_>>(),
            [0, 1]
        );
        for id in [0, 1] {
            let original = f.definitions.get(id as usize).unwrap();
            assert!(std::ptr::eq(encoded.expression(id)?.unwrap(), original));
            assert_eq!(encoded.source_id(original)?, id);
        }
        assert!(encoded.expression(2)?.is_none());
        let mut foreign = f.definitions.first().unwrap().clone();
        assert!(matches!(
            encoded.source_id(&foreign),
            Err(Error::InvalidShape(_))
        ));
        foreign.ty = FunctionValueType::new(DataType::Float64, false);
        assert!(matches!(
            encoded.source_id(&foreign),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(encoded.lookup_work_upper_bound()?, 48);
        let Some(wire::expression_definition::Kind::Constant(reference)) =
            &encoded.as_wire()[1].kind
        else {
            panic!("selected constant reference")
        };
        assert_eq!(reference.pool_id, Some(u32::MAX));
        assert_eq!(reference.row_ordinal, 2);
        assert_eq!(
            encoded.pools().entries()[&ConstantPoolId::new(u32::MAX)]
                .value(2)
                .unwrap()
                .try_i64()
                .unwrap(),
            Some(71)
        );
        let output = encoded.into_wire();
        assert_eq!(output.len(), 2);
        Ok(())
    })
    .unwrap();
}

#[test]
fn sealed_namespace_live_floor_uses_original_invoice_and_actual_root_capacity() {
    let f = Fixture::single(ExprKind::Conjunction {
        args: vec![ExprId::new(0); 320].into_boxed_slice(),
    });
    let inspect = |source| {
        f.with_prepared(&Control::default(), source, limits(), |prepared| {
            let encoded = prepared.emit()?;
            let expected = source
                + size_of_val(&encoded)
                + size_of::<wire::ExpressionDefinition>() * encoded.wire.capacity();
            let floor = encoded.retained_invoice_floor()?;
            assert_eq!(floor, expected);
            // Root DTO capacity is a necessary floor, not the nested argument
            // Vec's retained backing or an allocator/grant measurement.
            assert!(
                encoded.facts().new_allocation_request_bytes_upper_bound
                    > std::mem::size_of_val(encoded.wire.as_slice())
            );
            Ok((
                floor,
                encoded
                    .facts()
                    .coexisting_source_and_request_bytes_upper_bound,
            ))
        })
        .unwrap()
    };
    let first = inspect(SOURCE);
    let second = inspect(SOURCE + 4096);
    assert_eq!(second.0 - first.0, 4096);
    assert_eq!(second.1 - first.1, 4096);
}

#[test]
fn sealed_namespace_empty_public_encoder_keeps_zero_requests_and_real_lookup_tails() {
    let types = encode_type_table_sources(&[], &[], type_limits(), &Setup).unwrap();
    let functions = encode_function_bindings(&types, &[], 0, binding_limits(), &Setup).unwrap();
    let aggregates =
        encode_aggregate_bindings(&types, &functions, &[], 0, binding_limits(), &Setup).unwrap();
    let empty_definitions = definitions(vec![]);
    let parameters = SemanticParameters::default();
    let pools = ConstantPools::empty();
    let c = Control::default();
    let caps = ExpressionProjectionLimits {
        max_definitions: 0,
        max_type_references: 0,
        max_expression_references: 0,
        max_new_allocation_requests: 0,
        max_new_allocation_request_bytes: 0,
        max_coexisting_source_and_request_bytes: 0,
        max_cumulative_work: limits().max_cumulative_work,
    };
    let encoded = encode_expression_definitions(
        Some(ExpressionDefinitionSource::new(&empty_definitions, &[])),
        &[],
        &types,
        &functions,
        &aggregates,
        &parameters,
        &pools,
        0,
        caps,
        &c,
    )
    .unwrap();
    assert!(encoded.as_wire().is_empty());
    assert_eq!(encoded.source_count(), 0);
    assert_eq!(encoded.facts().new_allocation_requests_upper_bound, 0);
    assert_eq!(
        encoded.retained_invoice_floor().unwrap(),
        size_of_val(&encoded)
    );
    assert!(encoded.expression(0).unwrap().is_none());
    assert!(encoded.expression(u32::MAX).unwrap().is_none());
    let foreign = node(0, ExprKind::Value(ValueId::new(0)), int());
    assert!(matches!(
        encoded.source_id(&foreign),
        Err(Error::InvalidShape(_))
    ));
    assert!(encoded.into_wire().is_empty());
}

#[test]
fn sealed_namespace_queries_keep_every_original_control_prefix_and_ordinary_tail() {
    let f = Fixture::single(ExprKind::Value(ValueId::new(0)));
    let foreign = f.definitions.first().unwrap().clone();
    for ordinary in [false, true] {
        let invoke = |control: &Control| {
            f.with_prepared(control, SOURCE, limits(), |prepared| {
                let encoded = prepared.emit()?;
                assert!(std::ptr::eq(
                    encoded.expression(0)?.unwrap(),
                    f.definitions.first().unwrap()
                ));
                assert!(encoded.expression(u32::MAX)?.is_none());
                encoded.retained_invoice_floor()?;
                if ordinary {
                    encoded.source_id(&foreign)?;
                } else {
                    assert_eq!(encoded.source_id(f.definitions.first().unwrap())?, 0);
                }
                Ok(())
            })
        };
        let good = Control::default();
        let result = invoke(&good);
        assert_eq!(result.is_err(), ordinary);
        if ordinary {
            assert!(matches!(
                result,
                Err(Error::InvalidShape(
                    "expression source owner is not in this namespace"
                ))
            ));
        }
        let trace = good.trace.lock().unwrap().clone();
        assert_eq!(trace[0], 0);
        // The final source identity comparison is actual completed work,
        // including the ordinary mismatch, before its publication tail.
        assert_eq!(*trace.last().unwrap(), 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    stop: Some((at, cause.clone())),
                };
                assert!(matches!(invoke(&control), Err(Error::Control(actual)) if actual == cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn namespace_real_wide_lists_and_definitions_reject_original_quantum_without_replay() {
    let fixtures = [
        Fixture::single(ExprKind::Conjunction {
            args: vec![ExprId::new(u32::MAX); 320].into_boxed_slice(),
        }),
        Fixture::new(
            (0..320)
                .map(|id| node(id, ExprKind::Value(ValueId::new(u32::MAX)), int()))
                .collect(),
        ),
    ];
    for f in &fixtures {
        let good = Control::default();
        let (facts, out) = f.run(&good, SOURCE, limits(), true).unwrap();
        assert_eq!(out.len(), facts.definition_count);
        let trace = good.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        let positions = trace
            .iter()
            .enumerate()
            .filter_map(|(at, units)| {
                if *units == 256 || at == 0 || at + 1 == trace.len() {
                    Some(at)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for at in positions {
            for cause in CAUSES {
                let c = Control {
                    trace: Mutex::new(vec![]),
                    stop: Some((at, cause.clone())),
                };
                assert!(
                    matches!(f.run(&c,SOURCE,limits(),true),Err(Error::Control(actual)) if actual==cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[path = "namespace_owned_tests.rs"]
mod caller_owned_tests;

#[test]
fn explicit_array_cast_modes_encode_separately_and_reject_foreign_source_shapes() {
    use novarocks_type_contract::ArrayCastFailureMode as Mode;
    let list = |item| {
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("declared-item", item, true))),
            true,
        )
    };
    let source = list(DataType::Int32);
    let target = list(DataType::Int64);
    for (mode, expected) in [
        (None, None),
        (Some(Mode::ReportError), Some(1)),
        (Some(Mode::OutputNull), Some(2)),
    ] {
        let mut fixture = Fixture::new(vec![
            node(0, ExprKind::Value(ValueId::new(44)), source.clone()),
            node(
                1,
                ExprKind::Cast {
                    expr: ExprId::new(0),
                    target: target.data_type().clone(),
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    allow_throw_exception: allow(0),
                    array_failure_mode: mode,
                },
                target.clone(),
            ),
        ]);
        fixture.roots.push((11, source.clone()));
        fixture.roots.push((12, target.clone()));
        fixture.input[0].type_id = 11;
        fixture.input[1].type_id = 12;
        let (_, wire) = fixture
            .run(&Control::default(), SOURCE, limits(), true)
            .unwrap();
        let Some(wire::expression_definition::Kind::Cast(cast)) = &wire[1].kind else {
            panic!("encoded CAST")
        };
        assert_eq!(cast.array_failure_mode, expected);
        assert_eq!(cast.expr_id, Some(0));
        if mode.is_some() {
            // Keep the original namespace's source identity honest while changing its shape.
            fixture.replace(0, |node| node.ty = int());
            fixture.input[0].type_id = 0;
            assert!(
                fixture
                    .run(&Control::default(), SOURCE, limits(), true)
                    .is_err()
            );
        }
    }
}

#[test]
fn namespace_rejects_sparse_or_duplicate_local_definition_ids_without_reminting() {
    for ids in [[0, 7], [0, u32::MAX], [0, 0]] {
        let fixture = Fixture::new(
            ids.into_iter()
                .map(|id| node(id, ExprKind::Value(ValueId::new(u32::MAX)), int()))
                .collect(),
        );
        assert!(matches!(
            fixture.run(&Control::default(), SOURCE, limits(), true),
            Err(Error::InvalidShape(
                "expression binding order differs from original definitions"
            ))
        ));
    }
}
