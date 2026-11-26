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

//! Public unit compilation contracts use genuine checked constants and installed owners.
use arrow::array::Array;
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
};
use novarocks_constant_contract::*;
use novarocks_expression::*;
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_functions::FunctionResultType;
use novarocks_functions::{
    EngineFunctionCatalogBuilder, InstalledPureKernel, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};
use novarocks_type_contract::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::default(),
            stop: None,
        }
    }
    fn at(i: usize, c: CompileControlError) -> Self {
        Self {
            trace: Mutex::default(),
            stop: Some((i, c)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, p: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        let i = t.len();
        t.push((p, n));
        if let Some((at, c)) = &self.stop
            && *at == i
        {
            return Err(c.clone());
        }
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 16_777_216,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 67_108_864,
        max_library_validation_bytes: 67_108_864,
    }
}
fn limits() -> UnitLimits {
    UnitLimits {
        expression_semantic_depth: MAX_CONTROL_DEPTH,
    }
}
fn int(nullable: bool) -> ValueType {
    ValueType::new(DataType::Int64, nullable)
}
fn boolean(nullable: bool) -> ValueType {
    ValueType::new(DataType::Boolean, nullable)
}
fn builder() -> BoundExprSetBuilder {
    BoundExprSetBuilder::new(limits())
}
fn finish(b: BoundExprSetBuilder, ports: Vec<ExprPort>) -> BoundExprSet {
    let c = Control::good();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let u = b
        .seal(ports, &mut w)
        .unwrap()
        .finish(BTreeMap::new(), &mut w)
        .unwrap();
    w.finish().unwrap();
    u
}
fn catalogue() -> PureEngineFunctionCatalog {
    scalar_catalogue()
}
fn layouts(values: &[(ValueId, ValueType)]) -> ScopeLayout {
    let c = Control::good();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let s = Arc::new(Schema::new(
        values
            .iter()
            .enumerate()
            .map(|(i, (_, t))| Field::new(format!("input{i}"), t.data_type().clone(), t.nullable()))
            .collect::<Vec<_>>(),
    ));
    ScopeLayout::try_new(
        s,
        values.iter().map(|(_, ty)| ty.clone()).collect(),
        values
            .iter()
            .enumerate()
            .map(|(ordinal, (value, ty))| ScopeColumn {
                value: *value,
                ordinal: u32::try_from(ordinal).unwrap(),
                ty: ty.clone(),
            })
            .collect(),
        ValueTypeStorageLifetime::pure(),
        &mut w,
    )
    .unwrap()
}
fn compile(
    unit: &BoundExprSet,
    pools: &ConstantPools,
    parameters: &SemanticParameters,
    scopes: ScopeLayouts<'_>,
    control: &Control,
) -> Result<Arc<CompiledExprSet>, ExprCompileError> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let out = compile_expr_set_pure(
        unit,
        EvaluationEnvironment {
            constants: pools,
            parameters,
        },
        scopes,
        policy(),
        &catalogue(),
        &mut w,
    );
    if matches!(&out, Err(ExprCompileError::Control(_))) {
        return out;
    }
    w.finish()?;
    out
}
fn checked_pool() -> (ConstantPools, ConstantPool) {
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("original", DataType::Int64, true)),
        int(true),
        Arc::new(Int64Array::from(vec![Some(7), None, Some(42)])).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let mut p = ConstantPools::empty();
    p.insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    (p, pool)
}
fn constant(b: &mut BoundExprSetBuilder, ordinal: u32) -> ExprId {
    b.add(
        None,
        int(true),
        ExprKind::Constant(ConstantReference {
            pool: ConstantPoolId::new(u32::MAX),
            ordinal,
        }),
    )
    .unwrap()
}
#[test]
fn zero_port_constants_keep_sparse_original_backing_ordinal_null_and_complete_type() {
    let (pools, original) = checked_pool();
    let mut b = builder();
    for ordinal in [2, 1, 0] {
        let id = constant(&mut b, ordinal);
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let u = finish(b, vec![]);
    let p = compile(
        &u,
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    assert_eq!(p.roots().len(), 3);
    assert!(
        p.scope(RootScope::AllPorts)
            .unwrap()
            .schema()
            .fields()
            .is_empty()
    );
    for (root, (ordinal, payload)) in p
        .roots()
        .iter()
        .zip([(2, Some(42)), (1, None), (0, Some(7))])
    {
        let StaticExprKind::Constant(v) = p.definitions().node(root.definition).unwrap().kind()
        else {
            panic!("constant")
        };
        assert_eq!(v.ordinal(), ordinal);
        assert_eq!(v.try_i64().unwrap(), payload);
        assert_eq!(v.value_type(), &int(true));
        assert_eq!(v.pool().backing_identity(), original.backing_identity());
        assert!(Arc::ptr_eq(v.pool().field_ref(), original.field_ref()));
    }
}
#[test]
fn two_ports_combined_scope_and_repeated_value_definition_bind_each_real_use() {
    let left = [(ValueId::new(10), int(false))];
    let right = [(ValueId::new(20), int(false))];
    let mut b = builder();
    let a = b.add(None, int(false), ExprKind::Value(left[0].0)).unwrap();
    let z = b
        .add(None, int(false), ExprKind::Value(right[0].0))
        .unwrap();
    let eq = b
        .add(
            None,
            boolean(false),
            ExprKind::Binary {
                left: a,
                op: BinaryOperator::Eq,
                right: z,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    b.root(a, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    b.root(eq, EvaluationDemand::TruthOnly, RootScope::AllPorts)
        .unwrap();
    let u = finish(
        b,
        vec![
            ExprPort {
                values: left.clone().into(),
            },
            ExprPort {
                values: right.clone().into(),
            },
        ],
    );
    let ports = [layouts(&left), layouts(&right)];
    let combined = layouts(&[right[0].clone(), left[0].clone()]);
    let p = compile(
        &u,
        &ConstantPools::empty(),
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &ports,
            all_ports: Some(&combined),
        },
        &Control::good(),
    )
    .unwrap();
    let first = p.input(p.roots()[0].use_id).unwrap();
    assert_eq!(
        first,
        InputBinding {
            scope: RootScope::Port(0),
            ordinal: 0
        }
    );
    let eq_use = &p.control_flow().uses()[&p.roots()[1].use_id];
    assert_eq!(
        p.input(eq_use.arguments[0]),
        Some(InputBinding {
            scope: RootScope::AllPorts,
            ordinal: 1
        })
    );
    assert_eq!(
        p.input(eq_use.arguments[1]),
        Some(InputBinding {
            scope: RootScope::AllPorts,
            ordinal: 0
        })
    );
    assert!(
        p.recipes()
            .comparison(ComparisonSite::Binary(p.roots()[1].use_id))
            .is_some()
    );
    assert_eq!(p.roots()[1].demand, EvaluationDemand::TruthOnly);
}
#[test]
fn scope_full_type_width_and_missing_combined_layout_are_rejected() {
    let values = [(ValueId::new(3), int(false))];
    let mut b = builder();
    let id = b
        .add(None, int(false), ExprKind::Value(values[0].0))
        .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let u = finish(
        b,
        vec![ExprPort {
            values: values.clone().into(),
        }],
    );
    let actual = [layouts(&values)];
    assert!(matches!(
        compile(
            &u,
            &ConstantPools::empty(),
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &actual,
                all_ports: None
            },
            &Control::good()
        ),
        Err(ExprCompileError::Invalid(_))
    ));
    let wrong = [layouts(&[(ValueId::new(3), int(true))])];
    assert!(
        compile(
            &u,
            &ConstantPools::empty(),
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &wrong,
                all_ports: Some(&wrong[0])
            },
            &Control::good()
        )
        .is_err()
    );
    let nominal =
        ValueType::try_with_logical_type(DataType::LargeBinary, true, ValueLogicalType::Json)
            .unwrap();
    let schema = Arc::new(Schema::new(vec![nominal.try_to_field("nominal").unwrap()]));
    let canonical: Arc<[ScopeColumn]> = Arc::from([ScopeColumn {
        value: values[0].0,
        ordinal: 0,
        ty: nominal.clone(),
    }]);
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    assert!(matches!(
        ScopeLayout::try_new(
            schema.clone(),
            Arc::from([]),
            canonical,
            ValueTypeStorageLifetime::pure(),
            &mut work,
        ),
        Err(ExprCompileError::Invalid(
            "scope schema and complete columns differ"
        ))
    ));
    assert!(matches!(
        ScopeLayout::try_new(
            schema,
            Arc::from([nominal]),
            Arc::from([ScopeColumn {
                value: values[0].0,
                ordinal: 0,
                ty: ValueType::new(DataType::LargeBinary, true),
            }]),
            ValueTypeStorageLifetime::pure(),
            &mut work,
        ),
        Err(ExprCompileError::Invalid(
            "scope field differs from its complete input declaration"
        ))
    ));
    work.finish().unwrap();
}
#[test]
fn higher_order_compilation_keeps_exact_captures_and_ancestor_reuse() {
    let (binding, request, catalog) = resolved::lambda_compilation_fixture();
    let c = Control::good();
    let pools = ConstantPools::empty();
    let parameters = SemanticParameters::default();
    let mut b = builder();
    let outer = b.reserve().unwrap();
    let middle = b.reserve().unwrap();
    let inner = b.reserve().unwrap();
    let column = ValueId::new(63);
    let root_input = b.add(None, int(false), ExprKind::Value(column)).unwrap();
    let capture = b
        .add(Some(inner), int(false), ExprKind::Value(column))
        .unwrap();
    let x = [outer, middle, inner].map(|lambda| {
        b.add(
            Some(inner),
            int(false),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap()
    });
    let mut compare = |left, right| {
        b.add(
            Some(inner),
            boolean(true),
            ExprKind::Binary {
                left,
                right,
                op: BinaryOperator::Eq,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap()
    };
    let top_only = compare(capture, capture);
    let outer_used = compare(x[0], capture);
    let middle_used = compare(x[1], x[0]);
    let current_used = compare(x[2], x[1]);
    let conjunction = b
        .add(
            Some(inner),
            boolean(true),
            ExprKind::Conjunction {
                args: Box::new([top_only, outer_used, middle_used, current_used]),
            },
        )
        .unwrap();

    let mut overflow = BTreeMap::new();
    let rand = catalog
        .metadata()
        .resolve_bound_user(
            "rand",
            FunctionKind::Scalar,
            novarocks_function_contract::FunctionBindingRequest {
                arguments: &[],
                logical_argument_count: 0,
                expected_result_type: None,
            },
            &c,
        )
        .unwrap();
    let FunctionResultType::Scalar(rand_type) = &rand.selected.result_type else {
        panic!("rand returns a scalar");
    };
    let stateful = b
        .add(
            Some(middle),
            rand_type.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction::from_exact_signature(
                    rand.function_id,
                    rand.selected.overload,
                    FunctionKind::Scalar,
                    rand.selected.argument_types,
                    rand_type.clone(),
                ),
                args: Box::new([]),
            },
        )
        .unwrap();
    b.request(
        stateful,
        PhysicalCallRequest {
            arguments: Box::new([]),
            logical_argument_count: 0,
            expected_result_type: None,
            constant_policy: policy(),
        },
    )
    .unwrap();
    b.definition_control(
        stateful,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )
    .unwrap();
    overflow.insert(stateful, DecimalOverflowPolicy::OutputNull);
    let rand_predicate = b
        .add(
            Some(middle),
            boolean(false),
            ExprKind::IsNull {
                expr: stateful,
                negated: true,
            },
        )
        .unwrap();
    let middle_body = b.reserve().unwrap();
    let mut add_call = |scope, wrapper, body| {
        b.insert(
            wrapper,
            scope,
            boolean(true),
            ExprKind::Lambda {
                body,
                parameter_types: Box::new([int(false)]),
            },
        )
        .unwrap();
        let collection = if scope.is_some() {
            b.add(scope, int(false), ExprKind::Value(column)).unwrap()
        } else {
            root_input
        };
        let call = b
            .add(
                scope,
                boolean(true),
                ExprKind::FunctionCall {
                    function: binding.clone(),
                    args: Box::new([collection, wrapper]),
                },
            )
            .unwrap();
        b.request(call, request.clone()).unwrap();
        b.definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::HigherOrder {
                    body_ordinal: 1,
                    body_demand: EvaluationDemand::Value,
                },
                temporal_sources: None,
            },
        )
        .unwrap();
        overflow.insert(call, DecimalOverflowPolicy::OutputNull);
        call
    };
    let inner_call = add_call(Some(middle), inner, conjunction);
    let middle_call = add_call(Some(outer), middle, middle_body);
    let outer_call = add_call(None, outer, middle_call);
    b.insert(
        middle_body,
        Some(middle),
        boolean(true),
        ExprKind::Conjunction {
            args: Box::new([inner_call, rand_predicate]),
        },
    )
    .unwrap();
    b.root(outer_call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let values = [(column, int(false))];
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new(values.clone()),
            }],
            &mut work,
        )
        .unwrap();
    let environment = EvaluationEnvironment {
        constants: &pools,
        parameters: &parameters,
    };
    let authored = author_unit_calls(
        &sealed,
        &CallAuthorInputs {
            environment,
            constant_policy: policy(),
            functions: &catalog,
            decimal_overflow: &overflow,
        },
        &mut work,
    )
    .unwrap();
    let u = sealed.finish(authored.claims, &mut work).unwrap();
    work.finish().unwrap();
    let scope = layouts(&values);
    let scopes = ScopeLayouts {
        ports: std::slice::from_ref(&scope),
        all_ports: None,
    };
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let program =
        compile_expr_set_pure(&u, environment, scopes, policy(), &catalog, &mut work).unwrap();
    work.finish().unwrap();

    let only_use = |id| {
        let mut uses = u
            .control_flow()
            .uses()
            .values()
            .filter(|use_| use_.definition == id);
        let selected = uses.next().unwrap().context.use_id;
        assert!(uses.next().is_none());
        selected
    };
    assert!(matches!(
        program.types().get(program.compiled_definition(inner).unwrap().index()),
        Some(FunctionArgumentType::Lambda { parameter_types, result_type })
            if parameter_types.as_slice() == [int(false)] && result_type == &boolean(true)
    ));
    assert!(matches!(
        program.definitions().node(program.compiled_definition(x[0]).unwrap()).unwrap().kind(),
        StaticExprKind::LambdaParameter { lambda, ordinal: 0 } if *lambda == outer
    ));

    let expected_sources = [
        LambdaCaptureSource::Input(column),
        LambdaCaptureSource::Parameter {
            lambda: outer,
            ordinal: 0,
        },
        LambdaCaptureSource::Parameter {
            lambda: middle,
            ordinal: 0,
        },
    ];
    let mut last = None;
    for (index, (call, wrapper, body)) in [
        (outer_call, outer, middle_call),
        (middle_call, middle, middle_body),
        (inner_call, inner, conjunction),
    ]
    .into_iter()
    .enumerate()
    {
        let call = only_use(call);
        let body_use = only_use(body);
        let actual = program.higher_order_body(call).unwrap();
        assert_eq!(actual.lambda_definition, wrapper);
        assert_eq!(actual.body_use, body_use);
        assert_eq!(actual.contract.call().body_edge_use(), only_use(wrapper));
        assert_eq!(
            actual.contract.context(),
            u.control_flow().uses()[&body_use].context
        );
        assert_ne!(
            actual.contract.context().region,
            u.control_flow().uses()[&call].context.region
        );
        assert_eq!(
            actual
                .captures
                .iter()
                .map(|capture| capture.source)
                .collect::<Vec<_>>(),
            expected_sources[..=index]
        );
        assert_eq!(actual.contract.capture_types(), vec![int(false); index + 1]);
        for capture in actual.captures.iter().take(1) {
            assert_eq!(
                program.input(capture.use_id),
                Some(InputBinding {
                    scope: RootScope::Port(0),
                    ordinal: 0,
                })
            );
        }
        if let Some(previous) = last {
            assert!(!Arc::ptr_eq(previous, &actual.contract));
        }
        last = Some(&actual.contract);
    }
    assert_eq!(
        program.reuse_scope(only_use(top_only)),
        Some(LambdaReuseScope::TopLevel)
    );
    assert_eq!(
        program.reuse_scope(only_use(outer_used)),
        Some(LambdaReuseScope::Ancestor(outer))
    );
    assert_eq!(
        program.reuse_scope(only_use(middle_used)),
        Some(LambdaReuseScope::Ancestor(middle))
    );
    assert_eq!(program.reuse_scope(only_use(current_used)), None);
    // The call's ordinary source is a root column, but its child lambda
    // closes over its actual current binder through one of its descendants.
    assert_eq!(program.reuse_scope(only_use(inner_call)), None);
    let stateful_use = only_use(stateful);
    let stateful_effects = program.effects()[&stateful_use]
        .for_use(u.control_flow().uses()[&stateful_use].context)
        .unwrap();
    assert!(stateful_effects.has_instance_state);
    assert_eq!(program.reuse_scope(stateful_use), None);
    assert_eq!(program.reuse_scope(only_use(rand_predicate)), None);
}

#[test]
fn lambda_is_rejected_as_an_independent_value_root() {
    let mut b = builder();
    let lambda = b.reserve().unwrap();
    let body = b
        .add(
            Some(lambda),
            int(false),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    b.insert(
        lambda,
        None,
        int(false),
        ExprKind::Lambda {
            parameter_types: Box::new([int(false)]),
            body,
        },
    )
    .unwrap();
    b.root(lambda, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    assert!(b.seal(vec![], &mut work).is_err());
}

#[test]
fn all_original_control_causes_stop_at_every_actual_compile_callback() {
    let (pools, _) = checked_pool();
    let mut b = builder();
    let id = constant(&mut b, 2);
    for _ in 0..320 {
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let u = finish(b, vec![]);
    let good = Control::good();
    compile(
        &u,
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &good,
    )
    .unwrap();
    let trace = good.trace();
    assert!(trace.iter().any(|(_, n)| *n == 256));
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control::at(at, cause.clone());
            assert!(
                matches!(compile(&u,&pools,&SemanticParameters::default(),ScopeLayouts{ports:&[],all_ports:None},&c),Err(ExprCompileError::Control(actual))if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
#[test]
fn intrinsic_recipes_are_keyed_by_each_use_and_between_keeps_four_edges() {
    let (pools, _) = checked_pool();
    let mut b = builder();
    let a = constant(&mut b, 0);
    let z = constant(&mut b, 2);
    let between = b
        .add(
            None,
            boolean(true),
            ExprKind::Between {
                expr: a,
                low: a,
                high: z,
                negated: false,
            },
        )
        .unwrap();
    let eq = b
        .add(
            None,
            boolean(false),
            ExprKind::Binary {
                left: a,
                right: z,
                op: BinaryOperator::EqForNull,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    for id in [between, eq, between] {
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let u = finish(b, vec![]);
    let p = compile(
        &u,
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    for i in [0, 2] {
        let id = p.roots()[i].use_id;
        let args = &p.control_flow().uses()[&id].arguments;
        assert_eq!(args.len(), 4);
        assert_ne!(args[0], args[2]);
        assert!(
            p.recipes()
                .comparison(ComparisonSite::BetweenLower(id))
                .is_some()
        );
        assert!(
            p.recipes()
                .comparison(ComparisonSite::BetweenUpper(id))
                .is_some()
        );
    }
    assert_ne!(p.roots()[0].use_id, p.roots()[2].use_id);
    assert!(
        p.recipes()
            .null_safe_comparison(p.roots()[1].use_id)
            .is_some()
    );
}
#[test]
fn non_topological_dense_definitions_compile_to_exact_source_mapping() {
    let (pools, _) = checked_pool();
    let mut b = builder();
    let root = b.reserve().unwrap();
    let child = constant(&mut b, 2);
    b.insert(
        root,
        None,
        boolean(false),
        ExprKind::IsNull {
            expr: child,
            negated: false,
        },
    )
    .unwrap();
    b.root(root, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let u = finish(b, vec![]);
    let p = compile(
        &u,
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    assert!(
        p.compiled_definition(child).unwrap().index()
            < p.compiled_definition(root).unwrap().index()
    );
    let StaticExprKind::IsNull(actual) = p
        .definitions()
        .node(p.roots()[0].definition)
        .unwrap()
        .kind()
    else {
        panic!("IS NULL")
    };
    assert_eq!(Some(*actual), p.compiled_definition(child));
}

fn scalar_catalogue() -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut b = EngineFunctionCatalogBuilder::new();
    b.register(
        actual
            .definition("rand", FunctionKind::Scalar)
            .unwrap()
            .clone(),
    )
    .unwrap();
    b.seal_pure(
        [
            "()->f64;strict;legacy",
            "(i8)->i8;strict;coerce",
            "(i16)->i16;strict;coerce",
            "(i32)->i32;strict;coerce",
            "(i64)->i64;strict;coerce",
        ]
        .map(|overload| InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(format!("builtin.scalar/rand/{overload}"))
                    .unwrap(),
                implementation: PureImplementationId::try_new("builtin.scalar/rand/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::ScalarV1,
            },
            aggregate_state_format: None,
        }),
    )
    .unwrap()
}
fn rand_unit() -> (BoundExprSet, PureEngineFunctionCatalog, SemanticParameters) {
    use novarocks_functions::{
        CallArgumentUses, CallEffectInput, PureCallPreparation, ScopedExpressionEffects,
    };
    let c = Control::good();
    let catalog = scalar_catalogue();
    let args: Vec<novarocks_function_contract::FunctionArgument> = vec![];
    let request = novarocks_function_contract::FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let bound = catalog
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &c)
        .unwrap();
    let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
        panic!("scalar")
    };
    let mut b = builder();
    let id = b
        .add(
            None,
            result.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction::from_exact_signature(
                    bound.function_id.clone(),
                    bound.selected.overload.clone(),
                    bound.kind,
                    bound.selected.argument_types.clone(),
                    result.clone(),
                ),
                args: Box::default(),
            },
        )
        .unwrap();
    b.request(
        id,
        PhysicalCallRequest {
            arguments: Box::default(),
            logical_argument_count: 0,
            expected_result_type: None,
            constant_policy: policy(),
        },
    )
    .unwrap();
    b.definition_control(
        id,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )
    .unwrap();
    for _ in 0..2 {
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b.seal(vec![], &mut w).unwrap();
    let parameters = SemanticParameters::default();
    let mut claims = BTreeMap::new();
    let selected = Arc::new(bound.selected.clone());
    for (&use_id, u) in sealed.uses() {
        let token = catalog
            .prepare_fresh(
                CallEffectInput {
                    context: u.context.into(),
                    argument_uses: CallArgumentUses::SelectedChannels(&[]),
                    function_id: &bound.function_id,
                    kind: bound.kind,
                    selected: selected.as_ref(),
                    request,
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Region(u.context.region),
                },
                Arc::clone(&selected),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(u.context),
                },
                &c,
            )
            .unwrap();
        claims.insert(
            use_id,
            ExpressionCallClaim {
                context: u.context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                temporal_source: None,
            },
        );
    }
    let unit = sealed.finish(claims, &mut w).unwrap();
    w.finish().unwrap();
    (unit, catalog, parameters)
}
#[test]
fn installed_scalar_requests_and_state_tokens_are_independent_per_unit_use() {
    let (unit, catalog, parameters) = rand_unit();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let p = compile_expr_set_pure(
        &unit,
        EvaluationEnvironment {
            constants: &ConstantPools::empty(),
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut work,
    )
    .unwrap();
    assert_eq!(p.calls().len(), 2);
    for root in p.roots() {
        let token = &p.calls()[&root.use_id];
        assert_eq!(
            token.call_contract().context(),
            CallEffectContext::Expression(unit.control_flow().uses()[&root.use_id].context)
        );
        assert!(
            p.effects()[&root.use_id]
                .for_use(unit.control_flow().uses()[&root.use_id].context)
                .unwrap()
                .has_instance_state
        );
    }
    assert_ne!(p.roots()[0].use_id, p.roots()[1].use_id);
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct HostRefusal;
impl std::fmt::Display for HostRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original fixture construction refusal")
    }
}
impl std::error::Error for HostRefusal {}
impl novarocks_failure_contract::FailureProjection for HostRefusal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::MEMORY, std::num::NonZeroU16::new(31).unwrap()),
        )
    }
}
struct Hold;
impl ValueTypeStorageHold for Hold {}
struct Scope {
    hold: Arc<Hold>,
    events: Arc<Mutex<Vec<usize>>>,
    refuse: Option<usize>,
    control: Arc<Control>,
    stopped: Arc<Mutex<Option<usize>>>,
}
impl ValueSchemaCreationScope<HostRefusal> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.hold.clone())
    }
    fn request_layout(&mut self, l: std::alloc::Layout) -> Result<(), HostRefusal> {
        self.observe_opaque_upper_bound(l.size())
    }
    fn observe_opaque_upper_bound(&mut self, n: usize) -> Result<(), HostRefusal> {
        let mut e = self.events.lock().unwrap();
        let at = e.len();
        e.push(n);
        if self.refuse == Some(at) {
            *self.stopped.lock().unwrap() = Some(self.control.trace().len());
            return Err(HostRefusal);
        }
        Ok(())
    }
}
struct Factory {
    events: Arc<Mutex<Vec<usize>>>,
    holds: Mutex<Vec<std::sync::Weak<Hold>>>,
    refuse: Option<usize>,
    control: Arc<Control>,
    stopped: Arc<Mutex<Option<usize>>>,
}
impl Factory {
    fn scope(&self) -> Scope {
        let hold = Arc::new(Hold);
        self.holds.lock().unwrap().push(Arc::downgrade(&hold));
        Scope {
            hold,
            events: self.events.clone(),
            refuse: self.refuse,
            control: self.control.clone(),
            stopped: self.stopped.clone(),
        }
    }
}
impl ValueSchemaCreationFactory<HostRefusal> for Factory {
    type Scope<'a> = Scope;
    fn begin_scope(&self) -> Result<Scope, HostRefusal> {
        Ok(self.scope())
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(std::alloc::Layout) -> Result<(), O>,
    ) -> Result<Scope, ValueSchemaScopeBeginFailure<O, HostRefusal>> {
        observe(std::alloc::Layout::new::<Hold>())
            .map_err(ValueSchemaScopeBeginFailure::Observer)?;
        Ok(self.scope())
    }
}
#[test]
fn caller_creation_refusals_remain_typed_without_footer_and_final_holds_survive() {
    let (pools, _) = checked_pool();
    let mut b = builder();
    let id = constant(&mut b, 0);
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let value = b
        .add(None, int(false), ExprKind::Value(ValueId::new(99)))
        .unwrap();
    b.root(value, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = finish(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(99), int(false))]),
        }],
    );
    let scopes = [layouts(&[(ValueId::new(99), int(false))])];
    let run = |refuse| {
        let c = Arc::new(Control::good());
        let f = Factory {
            events: Arc::new(Mutex::default()),
            holds: Mutex::default(),
            refuse,
            control: c.clone(),
            stopped: Arc::new(Mutex::default()),
        };
        let mut temporary = f.scope();
        let mut retained = f.scope();
        let mut work = CompileCheckpoints::try_new(c.as_ref(), CompilePhase::LowerProgram).unwrap();
        let outcome = compile_expr_set(
            &unit,
            EvaluationEnvironment {
                constants: &pools,
                parameters: &SemanticParameters::default(),
            },
            ScopeLayouts {
                ports: &scopes,
                all_ports: Some(&scopes[0]),
            },
            policy(),
            &catalogue(),
            ExprCreation {
                temporary: &mut temporary,
                retained: &mut retained,
                factory: &f,
            },
            &mut work,
        );
        let count = f.events.lock().unwrap().len();
        if refuse.is_some() {
            assert!(matches!(
                outcome,
                Err(ExprCompileFailure::Creator(HostRefusal))
            ));
            assert_eq!(f.stopped.lock().unwrap().unwrap(), c.trace().len());
            drop(temporary);
            drop(retained);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|h| h.upgrade().is_none())
            );
        } else {
            let result = outcome.unwrap();
            drop(temporary);
            drop(retained);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|h| h.upgrade().is_some())
            );
            drop(result);
            assert!(
                f.holds
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|h| h.upgrade().is_none())
            );
        }
        count
    };
    let requests = run(None);
    assert!(requests > 0);
    for at in 0..requests {
        run(Some(at));
    }
}
#[test]
fn checked_sources_are_not_readmitted_under_a_different_caller_policy() {
    let (pools, original) = checked_pool();
    let mut b = builder();
    let id = constant(&mut b, 2);
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let u = finish(b, vec![]);
    let c = Control::good();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let low = ConstantPolicy {
        max_rows: 0,
        max_retained_buffer_bytes: 0,
        ..policy()
    };
    let p = compile_expr_set_pure(
        &u,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &SemanticParameters::default(),
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        low,
        &catalogue(),
        &mut w,
    )
    .unwrap();
    let StaticExprKind::Constant(v) = p
        .definitions()
        .node(p.roots()[0].definition)
        .unwrap()
        .kind()
    else {
        panic!("checked source")
    };
    assert_eq!(v.pool().backing_identity(), original.backing_identity());
    assert_eq!(v.ordinal(), 2);
    assert_eq!(v.try_i64().unwrap(), Some(42));
}
#[test]
fn missing_pool_bad_ordinal_and_full_source_mismatch_keep_original_owner() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let (pools, _) = checked_pool();
    for (reference, ty, missing) in [
        (
            ConstantReference {
                pool: ConstantPoolId::new(0),
                ordinal: 0,
            },
            int(true),
            true,
        ),
        (
            ConstantReference {
                pool: ConstantPoolId::new(u32::MAX),
                ordinal: 3,
            },
            int(true),
            false,
        ),
        (
            ConstantReference {
                pool: ConstantPoolId::new(u32::MAX),
                ordinal: 0,
            },
            int(false),
            false,
        ),
    ] {
        let mut b = builder();
        let id = b.add(None, ty, ExprKind::Constant(reference)).unwrap();
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        let u = finish(b, vec![]);
        let e = compile(
            &u,
            &pools,
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &[],
                all_ports: None,
            },
            &Control::good(),
        )
        .unwrap_err();
        assert_eq!(e.failure_fact().class(), FailureClass::Invariant);
        assert!(matches!(&e, ExprCompileError::Reference(_)));
        if missing {
            assert!(
                matches!(e,ExprCompileError::Reference(ConstantTableError::MissingPool(actual))if actual==reference.pool)
            );
        } else if reference.ordinal == 3 {
            assert!(matches!(
                e,
                ExprCompileError::Reference(ConstantTableError::Constant(ConstantError::Invalid(
                    _
                )))
            ));
        } else {
            assert!(
                matches!(e,ExprCompileError::Reference(ConstantTableError::SourceTypeMismatch(actual))if actual==reference)
            );
        }
    }
}

struct Definitions(Vec<ExprId>);
impl ExpressionDefinitionMembership<ExprId> for Definitions {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.0.contains(&id)
    }
}
#[test]
fn neutral_parts_preserve_sparse_non_dfs_use_ids_and_root_specific_port_bindings() {
    let ty = boolean(true);
    let definition = ExprId::new(0);
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let flow = ExpressionControlFlow::try_new_in(
        vec![
            ExpressionEvaluationRegion {
                id: EvaluationRegionId::new(7),
                parent: None,
                guard: None,
            },
            ExpressionEvaluationRegion {
                id: EvaluationRegionId::new(81),
                parent: None,
                guard: None,
            },
        ],
        vec![
            ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(10),
                    region: EvaluationRegionId::new(7),
                    demand: EvaluationDemand::Value,
                },
                definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(95),
                    region: EvaluationRegionId::new(81),
                    demand: EvaluationDemand::TruthOnly,
                },
                definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        &Definitions(vec![definition]),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let parts = UnitParts::new(
        Box::new([
            ExprPort {
                values: Box::new([(ValueId::new(9), ty.clone())]),
            },
            ExprPort {
                values: Box::new([(ValueId::new(8), int(false)), (ValueId::new(9), ty.clone())]),
            },
        ]),
        Box::new([ExprNode {
            id: definition,
            lambda_scope: None,
            ty,
            kind: ExprKind::Value(ValueId::new(9)),
        }]),
        Box::new([
            ExprRoot {
                definition,
                demand: EvaluationDemand::TruthOnly,
                scope: RootScope::Port(1),
            },
            ExprRoot {
                definition,
                demand: EvaluationDemand::Value,
                scope: RootScope::Port(0),
            },
        ]),
        Box::new([ExpressionUseId::new(95), ExpressionUseId::new(10)]),
        flow,
        BTreeMap::new(),
        BTreeMap::new(),
    );
    let unit = BoundExprSet::try_from_parts_in(parts, limits(), &mut work).unwrap();
    work.finish().unwrap();
    assert_eq!(
        unit.root_use_ids(),
        [ExpressionUseId::new(95), ExpressionUseId::new(10)]
    );
    assert_eq!(unit.roots()[0].scope, RootScope::Port(1));
    let ports = [
        layouts(&[(ValueId::new(9), boolean(true))]),
        layouts(&[
            (ValueId::new(8), int(false)),
            (ValueId::new(9), boolean(true)),
        ]),
    ];
    let p = compile(
        &unit,
        &ConstantPools::empty(),
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &ports,
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        p.roots().iter().map(|r| r.use_id).collect::<Vec<_>>(),
        [ExpressionUseId::new(95), ExpressionUseId::new(10)]
    );
    assert_eq!(
        p.input(ExpressionUseId::new(95)),
        Some(InputBinding {
            scope: RootScope::Port(1),
            ordinal: 1
        })
    );
    assert_eq!(
        p.input(ExpressionUseId::new(10)),
        Some(InputBinding {
            scope: RootScope::Port(0),
            ordinal: 0
        })
    );
    assert_eq!(
        p.control_flow().uses()[&ExpressionUseId::new(95)]
            .context
            .region,
        EvaluationRegionId::new(81)
    );
    assert_eq!(
        p.control_flow().uses()[&ExpressionUseId::new(10)]
            .context
            .region,
        EvaluationRegionId::new(7)
    );
    assert_eq!(p.types().as_slice().len(), 1);
}

#[test]
fn each_intrinsic_recipe_keeps_its_actual_full_types_and_use_site() {
    let (mut pools, _) = checked_pool();
    let mut b = builder();
    let a = constant(&mut b, 0);
    let z = constant(&mut b, 2);
    let parameter = SemanticParameterRef {
        id: SemanticParameterId::new(71),
        expected_key: SemanticParameterKey::AllowThrowException,
    };
    let parameters = SemanticParameters::try_new([(
        parameter.id,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    let arithmetic = b
        .add(
            None,
            int(true),
            ExprKind::Binary {
                left: a,
                right: z,
                op: BinaryOperator::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: Some(parameter),
            },
        )
        .unwrap();
    let cast = b
        .add(
            None,
            int(true),
            ExprKind::Cast {
                cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                expr: a,
                target: DataType::Int64,
            },
        )
        .unwrap();
    let inlist = b
        .add(
            None,
            boolean(true),
            ExprKind::InList {
                expr: a,
                list: Box::new([z, a]),
                negated: true,
            },
        )
        .unwrap();
    let mut strings = Vec::new();
    for (pool_id, payload) in [(8, "hello"), (9, "h%")] {
        let ty = ValueType::new(DataType::Utf8, false);
        let value = ConstantValue::from_utf8(
            Arc::new(Field::new("text", DataType::Utf8, false)),
            ty.clone(),
            payload,
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap();
        pools
            .insert(ConstantPoolId::new(pool_id), value.pool().clone())
            .unwrap();
        strings.push(
            b.add(
                None,
                ty,
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(pool_id),
                    ordinal: value.ordinal(),
                }),
            )
            .unwrap(),
        );
    }
    let like = b
        .add(
            None,
            boolean(false),
            ExprKind::Like {
                expr: strings[0],
                pattern: strings[1],
                negated: true,
            },
        )
        .unwrap();
    let case = b
        .add(
            None,
            int(true),
            ExprKind::Case {
                ieee_float_equality: false,
                operand: Some(a),
                when_then: Box::new([(z, a)]),
                else_expr: Some(z),
            },
        )
        .unwrap();
    for id in [arithmetic, cast, inlist, like, case] {
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let unit = finish(b, vec![]);
    let compiled = compile(
        &unit,
        &pools,
        &parameters,
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    let uses: Vec<_> = compiled.roots().iter().map(|r| r.use_id).collect();
    let recipes = compiled.recipes();
    assert!(recipes.arithmetic(uses[0]).is_some());
    assert!(recipes.cast(uses[1]).is_some());
    assert!(recipes.native_inlist(uses[2]).is_some());
    let like = recipes.native_like(uses[3]).unwrap();
    assert!(like.negated());
    assert_eq!(like.text_type(), &ValueType::new(DataType::Utf8, false));
    assert_eq!(like.pattern_type(), &ValueType::new(DataType::Utf8, false));
    assert!(
        recipes
            .comparison(ComparisonSite::CaseWhen {
                occurrence: uses[4],
                arm: 0
            })
            .is_some()
    );
    for root in compiled.roots() {
        let source = compiled.definitions().node(root.definition).unwrap();
        assert_eq!(
            source.value_type().unwrap(),
            &unit
                .definition(
                    unit.roots()[compiled
                        .roots()
                        .iter()
                        .position(|r| r.use_id == root.use_id)
                        .unwrap()]
                    .definition
                )
                .unwrap()
                .ty
        );
    }
}

#[test]
fn full_types_and_shared_float_bits_keep_original_backing_and_reject_retagging() {
    use arrow::array::{Float32Array, LargeBinaryArray};
    let floats = [0x8000_0000u32, 0x7fc0_0042];
    let ty = ValueType::new(DataType::Float32, false);
    let source = ConstantPool::try_new(
        Arc::new(Field::new("selected", DataType::Float32, false)),
        ty.clone(),
        Arc::new(Float32Array::from(floats.map(f32::from_bits).to_vec())).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let json_type =
        ValueType::try_with_logical_type(DataType::LargeBinary, true, ValueLogicalType::Json)
            .unwrap();
    let json = ConstantPool::try_new(
        Arc::new(json_type.try_to_field("json").unwrap()),
        json_type.clone(),
        Arc::new(LargeBinaryArray::from(vec![None, Some(&[0x01, 0x20][..])])).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(5), source.clone())
        .unwrap();
    pools.insert(ConstantPoolId::new(6), json.clone()).unwrap();
    let make = |float_type: ValueType, json_type: ValueType| {
        let mut b = builder();
        let f = b
            .add(
                None,
                float_type,
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(5),
                    ordinal: 1,
                }),
            )
            .unwrap();
        let j = b
            .add(
                None,
                json_type,
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(6),
                    ordinal: 0,
                }),
            )
            .unwrap();
        b.root(j, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        b.root(f, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        finish(b, vec![])
    };
    let unit = make(ty.clone(), json_type.clone());
    let compiled = compile(
        &unit,
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    assert_eq!(compiled.types().len(), 2);
    for (i, exact) in [ty.clone(), json_type.clone()].iter().enumerate() {
        let local = compiled.compiled_definition(ExprId::new(i as u32)).unwrap();
        assert_eq!(
            compiled.types()[local.index()],
            FunctionArgumentType::Value(exact.clone())
        );
    }
    let StaticExprKind::Constant(stored) = compiled
        .definitions()
        .node(compiled.compiled_definition(ExprId::new(0)).unwrap())
        .unwrap()
        .kind()
    else {
        panic!("constant")
    };
    assert_eq!(stored.ordinal(), 1);
    assert!(Arc::ptr_eq(stored.pool().array(), source.array()));
    assert_eq!(
        stored
            .pool()
            .array()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(1)
            .to_bits(),
        floats[1]
    );
    let local = compiled.compiled_definition(ExprId::new(1)).unwrap();
    let StaticExprKind::Constant(stored_json) = compiled.definitions().node(local).unwrap().kind()
    else {
        panic!("json")
    };
    assert!(Arc::ptr_eq(stored_json.pool().array(), json.array()));
    assert!(
        stored_json
            .is_null_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    for wrong in [
        ValueType::new(DataType::LargeBinary, true),
        ValueType::try_with_logical_type(DataType::LargeBinary, false, ValueLogicalType::Json)
            .unwrap(),
    ] {
        let e = compile(
            &make(ty.clone(), wrong),
            &pools,
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &[],
                all_ports: None,
            },
            &Control::good(),
        )
        .unwrap_err();
        assert!(matches!(
            e,
            ExprCompileError::Reference(ConstantTableError::SourceTypeMismatch(_))
        ));
        assert_eq!(e.failure_fact().class(), FailureClass::Invariant);
    }
    let e = compile(
        &make(ValueType::new(DataType::Float64, false), json_type),
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap_err();
    assert!(matches!(
        e,
        ExprCompileError::Reference(ConstantTableError::SourceTypeMismatch(_))
    ));
}

#[test]
fn shared_constant_backing_is_bounded_once_and_independent_backings_are_charged() {
    use arrow::array::BinaryArray;
    let payload = vec![7u8; 9 * 1024 * 1024];
    let make = || {
        ConstantPool::try_new(
            Arc::new(Field::new("large", DataType::Binary, false)),
            ValueType::new(DataType::Binary, false),
            Arc::new(BinaryArray::from(vec![payload.as_slice()])).to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap()
    };
    let a = make();
    let b = make();
    let mut pools = ConstantPools::empty();
    pools.insert(ConstantPoolId::new(1), a.clone()).unwrap();
    pools.insert(ConstantPoolId::new(2), b).unwrap();
    let unit = |pool_ids: &[u32]| {
        let mut builder = builder();
        for &pool in pool_ids {
            let id = builder
                .add(
                    None,
                    ValueType::new(DataType::Binary, false),
                    ExprKind::Constant(ConstantReference {
                        pool: ConstantPoolId::new(pool),
                        ordinal: 0,
                    }),
                )
                .unwrap();
            builder
                .root(id, EvaluationDemand::Value, RootScope::AllPorts)
                .unwrap();
        }
        finish(builder, vec![])
    };
    let compiled = compile(
        &unit(&[1, 1, 1]),
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap();
    for node in compiled.definitions().nodes() {
        let StaticExprKind::Constant(value) = node.kind() else {
            panic!("constant")
        };
        assert_eq!(value.pool().backing_identity(), a.backing_identity());
    }
    let e = compile(
        &unit(&[1, 2]),
        &pools,
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &Control::good(),
    )
    .unwrap_err();
    assert!(matches!(
        e,
        ExprCompileError::Expressions(ExpressionsCompileError::Expression(
            StaticExpressionError::TooManyBytes
        ))
    ));
    assert_eq!(e.failure_fact().class(), FailureClass::Resource);
}

#[test]
fn complete_map64_scope_preserves_exact_carrier_and_all_control_causes() {
    let map = |mut ty| {
        for _ in 1..64 {
            ty = DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Int32, false)),
                            Arc::new(Field::new("value", ty, false)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
        }
        ty
    };
    let ty = ValueType::new(map(DataType::Utf8), true);
    let value = ValueId::new(91);
    let mut b = builder();
    let id = b.add(None, ty.clone(), ExprKind::Value(value)).unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = finish(
        b,
        vec![ExprPort {
            values: Box::new([(value, ty.clone())]),
        }],
    );
    let correct = [layouts(&[(value, ty.clone())])];
    let good = Control::good();
    let compiled = compile(
        &unit,
        &ConstantPools::empty(),
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &correct,
            all_ports: None,
        },
        &good,
    )
    .unwrap();
    assert_eq!(compiled.types()[0], FunctionArgumentType::Value(ty));
    let wrong = [layouts(&[(
        value,
        ValueType::new(map(DataType::LargeUtf8), true),
    )])];
    assert!(matches!(
        compile(
            &unit,
            &ConstantPools::empty(),
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &wrong,
                all_ports: None
            },
            &Control::good()
        ),
        Err(ExprCompileError::Invalid(_))
    ));
    let at = good
        .trace()
        .iter()
        .position(|(_, n)| *n == 256)
        .expect("real canonical type work");
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let c = Control::at(at, cause.clone());
        assert!(
            matches!(compile(&unit, &ConstantPools::empty(), &SemanticParameters::default(), ScopeLayouts { ports: &correct, all_ports: None }, &c),Err(ExprCompileError::Control(actual)) if actual == cause)
        );
        assert_eq!(c.trace(), good.trace()[..=at]);
    }
}

#[path = "resolved.rs"]
mod resolved;

#[test]
fn nested_nominal_types_keep_original_fields_and_ignore_only_nonsemantic_source_attributes() {
    use arrow::array::{LargeBinaryArray, StructArray};
    use std::collections::HashMap;
    let field = Arc::new(
        Field::new("json", DataType::LargeBinary, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("provider".to_owned(), "source".to_owned()),
        ])),
    );
    let empty_object = &[0x01, 0x80, 0x01, 0x00][..];
    let array = Arc::new(StructArray::new(
        vec![field.clone()].into(),
        vec![Arc::new(LargeBinaryArray::from(vec![
            &[0x01, 0x20][..],
            empty_object,
        ]))],
        None,
    ));
    let declare = |layout: &DataType, json| {
        let storage = ValueTypeStorageLifetime::pure();
        let kind = if json {
            ValueKind::Nominal(ValueLogicalType::Json)
        } else {
            ValueKind::Binary
        };
        let child = ValueField::new(
            "json",
            ValueTypeRef::try_new(kind, true, storage.clone()).unwrap(),
            storage.clone(),
        );
        ValueType::try_from_value(
            ValueTypeRef::try_new(
                ValueKind::Struct(vec![child].into()),
                false,
                storage.clone(),
            )
            .unwrap(),
            ValueLayout::try_new(layout, storage).unwrap(),
        )
        .unwrap()
    };
    let exact = declare(array.data_type(), true);
    let source = ConstantPool::try_new(
        Arc::new(Field::new("nested", array.data_type().clone(), false)),
        exact.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(13), source.clone())
        .unwrap();
    let compile_type = |ty| {
        let mut b = builder();
        let id = b
            .add(
                None,
                ty,
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(13),
                    ordinal: 1,
                }),
            )
            .unwrap();
        b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        compile(
            &finish(b, vec![]),
            &pools,
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &[],
                all_ports: None,
            },
            &Control::good(),
        )
    };
    let p = compile_type(exact.clone()).unwrap();
    let StaticExprKind::Constant(stored) = p
        .definitions()
        .node(p.roots()[0].definition)
        .unwrap()
        .kind()
    else {
        panic!("constant")
    };
    assert_eq!(stored.ordinal(), 1);
    assert!(Arc::ptr_eq(stored.pool().array(), source.array()));
    let DataType::Struct(fields) = stored.pool().array().data_type() else {
        panic!("struct")
    };
    assert!(Arc::ptr_eq(&fields[0], &field));
    assert_eq!(
        stored
            .pool()
            .array()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(1),
        empty_object
    );
    let attributes_only = DataType::Struct(
        vec![Arc::new(field.as_ref().clone().with_metadata(
            HashMap::from([
                (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
                ("provider".to_owned(), "different".to_owned()),
            ]),
        ))]
        .into(),
    );
    compile_type(declare(&attributes_only, true)).unwrap();
    let e = compile_type(declare(array.data_type(), false)).unwrap_err();
    assert!(matches!(
        e,
        ExprCompileError::Reference(ConstantTableError::SourceTypeMismatch(_))
    ));
    assert_eq!(e.failure_fact().class(), FailureClass::Invariant);
}

#[test]
fn original_no_arguments_invocation_keeps_full_static_request_without_input_uses() {
    use novarocks_functions::{
        CallArgumentUses, CallEffectInput, FunctionArgument, FunctionBindingRequest,
        PureCallPreparation, ScopedExpressionEffects,
    };
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut registry = EngineFunctionCatalogBuilder::new();
    registry
        .register(
            actual
                .definition("__array_struct_subfield", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let catalog = registry
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.scalar/__array_struct_subfield/v1").unwrap(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(
                    "builtin.scalar/__array_struct_subfield/dynamic-v1",
                )
                .unwrap(),
                implementation: PureImplementationId::try_new(
                    "builtin.scalar/__array_struct_subfield/selected-v1",
                )
                .unwrap(),
                abi: PureKernelAbi::ScalarInvocationV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap();
    let source_type = ValueType::new(
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Arc::new(Field::new("item", DataType::Int32, true))].into()),
            true,
        ))),
        true,
    );
    let field_type = ValueType::new(DataType::Utf8, true);
    let extra_type = ValueType::new(DataType::Int32, false);
    let field = ConstantValue::from_utf8(
        Arc::new(Field::new("field", DataType::Utf8, true)),
        field_type.clone(),
        "item",
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(12), field.pool().clone())
        .unwrap();
    let arguments = [
        FunctionArgument::Value {
            value_type: source_type.clone(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: field_type.clone(),
            constant: Some(field.clone()),
        },
        FunctionArgument::Value {
            value_type: extra_type.clone(),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 3,
        expected_result_type: None,
    };
    let c = Control::good();
    let bound = catalog
        .metadata()
        .resolve_bound_user("__array_struct_subfield", FunctionKind::Scalar, request, &c)
        .unwrap();
    let selected = Arc::new(bound.selected);
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("scalar")
    };
    let mut b = builder();
    let ids = [
        b.add(None, source_type.clone(), ExprKind::Value(ValueId::new(1)))
            .unwrap(),
        b.add(
            None,
            field_type.clone(),
            ExprKind::Constant(ConstantReference {
                pool: ConstantPoolId::new(12),
                ordinal: field.ordinal(),
            }),
        )
        .unwrap(),
        b.add(None, extra_type.clone(), ExprKind::Value(ValueId::new(3)))
            .unwrap(),
    ];
    let call = b
        .add(
            None,
            result.clone(),
            ExprKind::FunctionCall {
                function: BoundFunction::from_exact_signature(
                    bound.function_id.clone(),
                    selected.overload.clone(),
                    FunctionKind::Scalar,
                    selected.argument_types.clone(),
                    result.clone(),
                ),
                args: Box::new(ids),
            },
        )
        .unwrap();
    b.request(
        call,
        PhysicalCallRequest {
            arguments: Box::new([
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: source_type.clone(),
                    constant: None,
                },
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: field_type,
                    constant: Some(ConstantReference {
                        pool: ConstantPoolId::new(12),
                        ordinal: field.ordinal(),
                    }),
                },
                novarocks_function_contract::FunctionArgument::Value {
                    value_type: extra_type.clone(),
                    constant: None,
                },
            ]),
            logical_argument_count: 3,
            expected_result_type: None,
            constant_policy: policy(),
        },
    )
    .unwrap();
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::NoArguments,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([
                    (ValueId::new(1), source_type.clone()),
                    (ValueId::new(3), extra_type.clone()),
                ]),
            }],
            &mut w,
        )
        .unwrap();
    let use_id = sealed.root_use_ids()[0];
    let context = sealed.uses()[&use_id].context;
    let parameters = SemanticParameters::default();
    let token = catalog
        .prepare_fresh(
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&[None, None, None]),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Region(context.region),
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &c,
        )
        .unwrap();
    assert_eq!(
        token.call_contract().effects().argument_control,
        ArgumentControl::NoArguments
    );
    let u = sealed
        .finish(
            BTreeMap::from([(
                use_id,
                ExpressionCallClaim {
                    context,
                    effects: token.call_contract().effects().clone(),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                },
            )]),
            &mut w,
        )
        .unwrap();
    w.finish().unwrap();
    let scopes = [layouts(&[
        (ValueId::new(1), source_type),
        (ValueId::new(3), extra_type),
    ])];
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let p = compile_expr_set_pure(
        &u,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: &scopes,
            all_ports: None,
        },
        policy(),
        &catalog,
        &mut w,
    )
    .unwrap();
    w.finish().unwrap();
    assert_eq!(p.definitions().nodes().len(), 4);
    assert_eq!(p.types().len(), 4);
    assert_eq!(p.control_flow().uses().len(), 1);
    assert!(p.control_flow().uses()[&use_id].arguments.is_empty());
    assert!(p.input(use_id).is_none());
    let compiled = &p.calls()[&use_id];
    assert_eq!(compiled.call_contract().logical_argument_count(), 3);
    assert_eq!(
        compiled.call_contract().selected().argument_types,
        selected.argument_types
    );
    let evaluator_view::UnitStateTemplate::ScalarInvocation { scope, kernel } =
        evaluator_view::state_template(&p, use_id).unwrap()
    else {
        panic!("original invocation lifecycle")
    };
    assert_eq!(scope.root, RootOrdinal::new(0));
    assert_eq!(scope.occurrence, use_id);
    let novarocks_functions::PreparedPureKernel::ScalarInvocation(original) = compiled.prepared()
    else {
        panic!("invocation")
    };
    assert!(Arc::ptr_eq(kernel, original));
}

#[test]
fn late_definitions_clone_alone_retains_its_actual_construction_hold() {
    let (pools, _) = checked_pool();
    let mut b = builder();
    let id = constant(&mut b, 0);
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let unit = finish(b, vec![]);
    let c = Arc::new(Control::good());
    let factory = Factory {
        events: Arc::new(Mutex::default()),
        holds: Mutex::default(),
        refuse: None,
        control: c.clone(),
        stopped: Arc::new(Mutex::default()),
    };
    let mut temporary = factory.scope();
    let mut retained = factory.scope();
    let mut work = CompileCheckpoints::try_new(c.as_ref(), CompilePhase::LowerProgram).unwrap();
    let compiled = compile_expr_set(
        &unit,
        EvaluationEnvironment {
            constants: &pools,
            parameters: &SemanticParameters::default(),
        },
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        policy(),
        &catalogue(),
        ExprCreation {
            temporary: &mut temporary,
            retained: &mut retained,
            factory: &factory,
        },
        &mut work,
    )
    .unwrap();
    let late = compiled.definitions().clone();
    drop(compiled);
    drop(temporary);
    drop(retained);
    assert!(
        factory
            .holds
            .lock()
            .unwrap()
            .iter()
            .any(|hold| hold.upgrade().is_some()),
        "published definition backing retains its own original construction hold"
    );
    let StaticExprKind::Constant(value) = late.nodes()[0].kind() else {
        panic!("constant")
    };
    assert_eq!(
        value
            .int64_observed(CompilePhase::Validate, c.as_ref())
            .unwrap(),
        Some(7)
    );
    drop(late);
    assert!(
        factory
            .holds
            .lock()
            .unwrap()
            .iter()
            .all(|hold| hold.upgrade().is_none())
    );
}

#[test]
fn wide_combined_scope_is_indexed_once_and_keeps_original_control_prefix() {
    let columns = (0..512)
        .map(|i| (ValueId::new(i), int(false)))
        .collect::<Vec<_>>();
    let mut reversed = columns.clone();
    reversed.reverse();
    let ports = [layouts(&columns)];
    let combined = layouts(&reversed);
    let mut b = builder();
    let value = b
        .add(None, int(false), ExprKind::Value(ValueId::new(511)))
        .unwrap();
    b.root(value, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    b.root(value, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = finish(
        b,
        vec![ExprPort {
            values: columns.into_boxed_slice(),
        }],
    );
    let good = Control::good();
    let p = compile(
        &unit,
        &ConstantPools::empty(),
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &ports,
            all_ports: Some(&combined),
        },
        &good,
    )
    .unwrap();
    assert_eq!(
        p.input(p.roots()[0].use_id),
        Some(InputBinding {
            scope: RootScope::AllPorts,
            ordinal: 0
        })
    );
    assert_eq!(
        p.input(p.roots()[1].use_id),
        Some(InputBinding {
            scope: RootScope::Port(0),
            ordinal: 511
        })
    );
    let trace = good.trace();
    let units: u64 = trace.iter().map(|(_, n)| u64::from(*n)).sum();
    assert!(
        units >= 1024,
        "both real source scopes contribute observed indexing work"
    );
    assert!(
        units < 512 * 512 / 2,
        "scope verification does not repeatedly scan the full width"
    );
    let first_quantum = trace.iter().position(|(_, n)| *n == 256).unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let c = Control::at(first_quantum, cause.clone());
        assert!(
            matches!(compile(&unit,&ConstantPools::empty(),&SemanticParameters::default(),ScopeLayouts {ports:&ports,all_ports:Some(&combined)},&c),Err(ExprCompileError::Control(actual)) if actual==cause)
        );
        assert_eq!(c.trace(), trace[..=first_quantum]);
    }
}

#[test]
fn late_scope_clone_shares_source_columns_and_retains_only_their_original_owner() {
    let hold = Arc::new(Hold);
    let weak = Arc::downgrade(&hold);
    let columns: Arc<[ScopeColumn]> = Arc::from([ScopeColumn {
        value: ValueId::new(1),
        ordinal: 0,
        ty: int(false),
    }]);
    let source_pointer = columns.as_ptr();
    let nominal =
        ValueType::try_with_logical_type(DataType::LargeBinary, true, ValueLogicalType::Json)
            .unwrap();
    let ordinal_types: Arc<[ValueType]> = Arc::from([int(false), nominal.clone()]);
    let ordinal_pointer = ordinal_types.as_ptr();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::LowerProgram).unwrap();
    let scope = ScopeLayout::try_new(
        Arc::new(Schema::new(vec![
            Field::new("source", DataType::Int64, false),
            nominal.try_to_field("unmapped_nominal").unwrap(),
        ])),
        ordinal_types,
        columns,
        ValueTypeStorageLifetime::held(hold.clone()),
        &mut work,
    )
    .unwrap();
    let mut b = builder();
    let input = b
        .add(None, int(false), ExprKind::Value(ValueId::new(1)))
        .unwrap();
    b.root(input, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = finish(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(1), int(false))]),
        }],
    );
    let p = compile(
        &unit,
        &ConstantPools::empty(),
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: None,
        },
        &c,
    )
    .unwrap();
    let late = p.scope(RootScope::Port(0)).unwrap().clone();
    assert_eq!(late.columns().as_ptr(), source_pointer);
    assert_eq!(late.ordinal_types().as_ptr(), ordinal_pointer);
    assert_eq!(late.ordinal_types().len(), 2);
    assert_eq!(late.ordinal_type(1), Some(&nominal));
    assert_eq!(late.columns().len(), 1);
    assert!(Arc::ptr_eq(late.schema(), scope.schema()));
    {
        let storage = late.storage_lifetime();
        assert!(!storage.is_pure());
        storage
            .retaining_observed::<novarocks_type_contract::owned_resources::layout::LayoutResourceError>(
                &scope.storage_lifetime(),
                |_| panic!("scope clone must retain the same original storage"),
            )
            .unwrap();
    }
    drop(p);
    drop(scope);
    drop(hold);
    assert!(weak.upgrade().is_some());
    assert_eq!(late.columns()[0].value, ValueId::new(1));
    drop(late);
    assert!(weak.upgrade().is_none());
}

#[test]
fn source_dictionary_ids_do_not_change_complete_static_binding_and_invalid_nominals_fail_at_author()
{
    #[allow(deprecated)]
    let field = |id| {
        Arc::new(Field::new_dict(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
            id,
            true,
        ))
    };
    let left = DataType::Struct(vec![field(1)].into());
    let right = DataType::Struct(vec![field(2)].into());
    assert_eq!(left, right);
    let ty = ValueType::new(left, true);
    let mut b = builder();
    let input = b
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(1)))
        .unwrap();
    b.root(input, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let unit = finish(
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(1), ty.clone())]),
        }],
    );
    for carrier in [ty, ValueType::new(right, true)] {
        let scopes = [layouts(&[(ValueId::new(1), carrier.clone())])];
        let p = compile(
            &unit,
            &ConstantPools::empty(),
            &SemanticParameters::default(),
            ScopeLayouts {
                ports: &scopes,
                all_ports: None,
            },
            &Control::good(),
        )
        .unwrap();
        assert_eq!(p.types()[0], FunctionArgumentType::Value(carrier));
    }
    assert!(
        ValueType::try_with_logical_type(DataType::Boolean, true, ValueLogicalType::Json).is_err()
    );
}

#[test]
fn scope_layout_creator_funds_only_actual_temporary_duplicate_checks() {
    let columns: Arc<[ScopeColumn]> = (0..32)
        .rev()
        .enumerate()
        .map(|(ordinal, value)| ScopeColumn {
            value: ValueId::new(value),
            ordinal: u32::try_from(ordinal).unwrap(),
            ty: int(false),
        })
        .collect();
    let schema = Arc::new(Schema::new(
        (0..32)
            .map(|ordinal| Field::new(format!("input{ordinal}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let source_hold = Arc::new(Hold);
    let source_weak = Arc::downgrade(&source_hold);
    let run = |refuse| {
        let control = Arc::new(Control::good());
        let factory = Factory {
            events: Arc::new(Mutex::default()),
            holds: Mutex::default(),
            refuse,
            control: control.clone(),
            stopped: Arc::new(Mutex::default()),
        };
        let mut temporary = factory.scope();
        let mut work = CompileCheckpoints::try_new(control.as_ref(), CompilePhase::Decode).unwrap();
        let result = ScopeLayout::try_new_with_creator_in(
            schema.clone(),
            columns.iter().map(|column| column.ty.clone()).collect(),
            columns.clone(),
            ValueTypeStorageLifetime::held(source_hold.clone()),
            &mut temporary,
            &mut work,
        );
        let count = factory.events.lock().unwrap().len();
        match refuse {
            Some(_) => {
                assert!(matches!(
                    result,
                    Err(ExprCompileFailure::Creator(HostRefusal))
                ));
                assert_eq!(
                    factory.stopped.lock().unwrap().unwrap(),
                    control.trace().len()
                );
            }
            None => {
                let layout = result.unwrap();
                work.finish().unwrap();
                assert_eq!(layout.columns().as_ptr(), columns.as_ptr());
                drop(temporary);
                assert!(
                    factory
                        .holds
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|hold| hold.upgrade().is_none()),
                    "temporary index responsibility must end before the returned layout"
                );
                assert!(source_weak.upgrade().is_some());
                assert_eq!(layout.columns()[0].value, ValueId::new(31));
                drop(layout);
                return count;
            }
        }
        drop(temporary);
        assert!(
            factory
                .holds
                .lock()
                .unwrap()
                .iter()
                .all(|hold| hold.upgrade().is_none())
        );
        count
    };
    let requests = run(None);
    assert!(requests > 0);
    for at in 0..requests {
        run(Some(at));
    }
}
#[test]
fn scope_layout_creator_keeps_original_phase_and_every_first_control_cause() {
    let columns: Arc<[ScopeColumn]> = (0..32)
        .map(|value| ScopeColumn {
            value: ValueId::new(value),
            ordinal: value,
            ty: int(false),
        })
        .collect();
    let schema = Arc::new(Schema::new(
        (0..32)
            .map(|ordinal| Field::new(format!("input{ordinal}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let run = |control: Arc<Control>| -> Result<ScopeLayout, ExprCompileFailure<HostRefusal>> {
        let factory = Factory {
            events: Arc::new(Mutex::default()),
            holds: Mutex::default(),
            refuse: None,
            control: control.clone(),
            stopped: Arc::new(Mutex::default()),
        };
        let mut temporary = factory.scope();
        let mut work = CompileCheckpoints::try_new(control.as_ref(), CompilePhase::Decode)?;
        let result = ScopeLayout::try_new_with_creator_in(
            schema.clone(),
            columns.iter().map(|column| column.ty.clone()).collect(),
            columns.clone(),
            ValueTypeStorageLifetime::pure(),
            &mut temporary,
            &mut work,
        )?;
        work.finish()?;
        Ok(result)
    };
    let control = Arc::new(Control::good());
    run(control.clone()).unwrap();
    let original = control.trace();
    assert!(original.len() > 2);
    assert!(
        original
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Decode)
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..original.len() {
            let control = Arc::new(Control::at(at, cause.clone()));
            assert!(
                matches!(run(control.clone()),Err(ExprCompileFailure::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), original[..=at]);
        }
    }
}

#[test]
fn runtime_columns_keep_canonical_value_reads_after_repeated_publications() {
    // The enclosing channel author proves the repeated publication is the
    // same value. Its two physical columns remain distinct; the next value
    // keeps ordinal 2, and the right port starts after all three left columns.
    let left_value = ValueId::new(10);
    let other = ValueId::new(30);
    let right_value = ValueId::new(20);
    let left = [(left_value, int(false)), (other, int(false))];
    let right = [(right_value, int(false))];
    let mut b = builder();
    for (value, scope) in [
        (other, RootScope::Port(0)),
        (right_value, RootScope::Port(1)),
        (right_value, RootScope::AllPorts),
    ] {
        let definition = b.add(None, int(false), ExprKind::Value(value)).unwrap();
        b.root(definition, EvaluationDemand::Value, scope).unwrap();
    }
    let unit = finish(
        b,
        vec![
            ExprPort {
                values: left.clone().into(),
            },
            ExprPort {
                values: right.clone().into(),
            },
        ],
    );
    let mapped = |actual: &[(ValueId, ValueType)], representatives: &[u32]| {
        let schema = Arc::new(Schema::new(
            actual
                .iter()
                .enumerate()
                .map(|(ordinal, (_, ty))| {
                    Field::new(
                        format!("actual{ordinal}"),
                        ty.data_type().clone(),
                        ty.nullable(),
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let columns: Arc<[ScopeColumn]> = representatives
            .iter()
            .map(|&ordinal| ScopeColumn {
                value: actual[ordinal as usize].0,
                ordinal,
                ty: actual[ordinal as usize].1.clone(),
            })
            .collect();
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let layout = ScopeLayout::try_new(
            schema.clone(),
            actual.iter().map(|(_, ty)| ty.clone()).collect(),
            columns,
            ValueTypeStorageLifetime::pure(),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        assert!(Arc::ptr_eq(layout.schema(), &schema));
        layout
    };
    let ports = [
        mapped(
            &[left[0].clone(), left[0].clone(), left[1].clone()],
            &[0, 2],
        ),
        layouts(&right),
    ];
    let combined = mapped(
        &[
            left[0].clone(),
            left[0].clone(),
            left[1].clone(),
            right[0].clone(),
        ],
        &[0, 2, 3],
    );
    let mut wrong = combined.columns().to_vec();
    wrong[2].ordinal = 4;
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    assert!(matches!(
        ScopeLayout::try_new(
            combined.schema().clone(),
            combined.ordinal_types().to_vec().into(),
            wrong.into(),
            ValueTypeStorageLifetime::pure(),
            &mut work
        ),
        Err(ExprCompileError::Invalid(
            "scope input ordinal is outside its runtime schema"
        ))
    ));
    let program = compile(
        &unit,
        &ConstantPools::empty(),
        &SemanticParameters::default(),
        ScopeLayouts {
            ports: &ports,
            all_ports: Some(&combined),
        },
        &Control::good(),
    )
    .unwrap();
    for (root, expected) in program.roots().iter().zip([
        InputBinding {
            scope: RootScope::Port(0),
            ordinal: 2,
        },
        InputBinding {
            scope: RootScope::Port(1),
            ordinal: 0,
        },
        InputBinding {
            scope: RootScope::AllPorts,
            ordinal: 3,
        },
    ]) {
        assert_eq!(program.input(root.use_id), Some(expected));
    }
    assert_eq!(
        program
            .scope(RootScope::Port(0))
            .unwrap()
            .schema()
            .fields()
            .len(),
        3
    );
    assert_eq!(
        program
            .scope(RootScope::AllPorts)
            .unwrap()
            .schema()
            .fields()
            .len(),
        4
    );
}

#[test]
fn nullable_boolean_connectives_require_actual_deciding_cv_under_both_root_demands() {
    for or in [false, true] {
        for constant in [Some(false), Some(true), None] {
            for demand in [EvaluationDemand::Value, EvaluationDemand::TruthOnly] {
                for result_nullable in [false, true] {
                    let source_type = boolean(true);
                    let field = Arc::new(
                        Field::new("actual_boolean", DataType::Boolean, true).with_metadata(
                            std::collections::HashMap::from([(
                                "fixture_annotation".to_owned(),
                                "preserved".to_owned(),
                            )]),
                        ),
                    );
                    let value = match constant {
                        Some(value) => ConstantValue::from_boolean(
                            field.clone(),
                            source_type.clone(),
                            value,
                            policy(),
                            CompilePhase::Validate,
                            &Control::good(),
                        ),
                        None => ConstantValue::null(
                            field.clone(),
                            source_type.clone(),
                            policy(),
                            CompilePhase::Validate,
                            &Control::good(),
                        ),
                    }
                    .unwrap();
                    let reference = ConstantReference {
                        pool: ConstantPoolId::new(41),
                        ordinal: value.ordinal(),
                    };
                    let mut pools = ConstantPools::empty();
                    pools.insert(reference.pool, value.pool().clone()).unwrap();
                    let mut b = builder();
                    let input = b
                        .add(None, source_type.clone(), ExprKind::Value(ValueId::new(7)))
                        .unwrap();
                    let literal = b
                        .add(None, source_type.clone(), ExprKind::Constant(reference))
                        .unwrap();
                    let args = Box::from([input, literal]);
                    let root = b
                        .add(
                            None,
                            boolean(result_nullable),
                            if or {
                                ExprKind::Disjunction { args }
                            } else {
                                ExprKind::Conjunction { args }
                            },
                        )
                        .unwrap();
                    b.root(root, demand, RootScope::Port(0)).unwrap();
                    let unit = finish(
                        b,
                        vec![ExprPort {
                            values: Box::from([(ValueId::new(7), source_type.clone())]),
                        }],
                    );
                    let scope = layouts(&[(ValueId::new(7), source_type.clone())]);
                    let parameters = SemanticParameters::try_new([]).unwrap();
                    let result = compile(
                        &unit,
                        &pools,
                        &parameters,
                        ScopeLayouts {
                            ports: &[scope],
                            all_ports: None,
                        },
                        &Control::good(),
                    );
                    if !result_nullable && constant != Some(or) {
                        assert!(matches!(
                            result,
                            Err(ExprCompileError::Invalid(
                                "Boolean connective result discards nullable operands without a deciding constant"
                            ))
                        ));
                        continue;
                    }
                    let compiled = result.unwrap();
                    let root = &compiled.roots()[0];
                    assert_eq!(root.demand, demand);
                    assert_eq!(
                        compiled.types().get(root.definition.index()),
                        Some(&FunctionArgumentType::Value(boolean(result_nullable)))
                    );
                    let actual = compiled
                        .definitions()
                        .nodes()
                        .iter()
                        .find_map(|node| match node.kind() {
                            StaticExprKind::Constant(value) => Some(value),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(actual.try_boolean().unwrap(), constant);
                    assert_eq!(actual.value_type(), &source_type);
                    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
                    assert_eq!(
                        actual
                            .field()
                            .metadata()
                            .get("fixture_annotation")
                            .map(String::as_str),
                        Some("preserved")
                    );
                }
            }
        }
    }
    // Required inputs do not need a deciding-value exception.
    for or in [false, true] {
        let ty = boolean(false);
        let mut b = builder();
        let left = b
            .add(None, ty.clone(), ExprKind::Value(ValueId::new(7)))
            .unwrap();
        let right = b
            .add(None, ty.clone(), ExprKind::Value(ValueId::new(19)))
            .unwrap();
        let args = Box::new([left, right]);
        let root = b
            .add(
                None,
                ty.clone(),
                if or {
                    ExprKind::Disjunction { args }
                } else {
                    ExprKind::Conjunction { args }
                },
            )
            .unwrap();
        b.root(root, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap();
        let values = [
            (ValueId::new(7), ty.clone()),
            (ValueId::new(19), ty.clone()),
        ];
        let unit = finish(
            b,
            vec![ExprPort {
                values: Box::from(values.clone()),
            }],
        );
        let compiled = compile(
            &unit,
            &ConstantPools::empty(),
            &SemanticParameters::try_new([]).unwrap(),
            ScopeLayouts {
                ports: &[layouts(&values)],
                all_ports: None,
            },
            &Control::good(),
        )
        .unwrap();
        assert_eq!(
            compiled.types()[compiled.roots()[0].definition.index()],
            FunctionArgumentType::Value(ty)
        );
    }
}

#[test]
fn sparse_non_dfs_nested_shared_calls_keep_actual_edges_claims_and_compile_prefixes() {
    use novarocks_functions::{
        CallArgumentUses, CallEffectInput, PureCallPreparation, ScopedExpressionEffects,
    };
    // Reuse the installed RAND fixture's real metadata, request and control.
    // Only use coordinates are projected below; the SDK remains the sole
    // author of control shape, guards, child edges and ordered roots.
    let (template, catalog, parameters) = rand_unit();
    let source = template
        .definitions()
        .iter()
        .find(|node| matches!(node.kind, ExprKind::FunctionCall { .. }))
        .unwrap();
    let mut b = builder();
    let rand = b.add(None, source.ty.clone(), source.kind.clone()).unwrap();
    b.request(rand, template.requests()[&source.id].clone())
        .unwrap();
    let original = template
        .control_flow()
        .uses()
        .values()
        .find(|item| item.definition == source.id)
        .unwrap();
    b.definition_control(
        rand,
        DefinitionControl {
            shape: original.control,
            temporal_sources: None,
        },
    )
    .unwrap();
    let shared = b
        .add(
            None,
            boolean(false),
            ExprKind::IsNull {
                expr: rand,
                negated: false,
            },
        )
        .unwrap();
    let root = b
        .add(
            None,
            boolean(false),
            ExprKind::Conjunction {
                args: Box::new([shared, shared]),
            },
        )
        .unwrap();
    let value_root = b
        .root(root, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let truth_root = b
        .root(root, EvaluationDemand::TruthOnly, RootScope::AllPorts)
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let sealed = b.seal(vec![], &mut work).unwrap();
    assert_eq!(sealed.uses().len(), 10);
    let permutation = [7u32, 2, 9, 0, 5, 1, 8, 3, 6, 4];
    let ids: BTreeMap<_, _> = sealed
        .uses()
        .keys()
        .enumerate()
        .map(|(ordinal, &old)| (old, ExpressionUseId::new(1009 + 17 * permutation[ordinal])))
        .collect();
    let regions = sealed
        .control_flow()
        .regions()
        .values()
        .map(|region| ExpressionEvaluationRegion {
            guard: region.guard.map(|guard| RegionGuard {
                owner: ids[&guard.owner],
                kind: guard.kind,
            }),
            ..*region
        })
        .collect();
    let invocations = sealed
        .uses()
        .values()
        .map(|item| ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ids[&item.context.use_id],
                ..item.context
            },
            definition: item.definition,
            control: item.control,
            arguments: item.arguments.iter().map(|id| ids[id]).collect(),
        })
        .collect();
    let definitions = Definitions(sealed.definitions().iter().map(|node| node.id).collect());
    let flow = ExpressionControlFlow::try_new_in(
        regions,
        invocations,
        &definitions,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    // Both numerical directions occur. Neither a sorted use map nor definition
    // order is an evaluation schedule for this fixed graph.
    assert!(
        flow.uses()
            .iter()
            .any(|(&id, item)| item.arguments.iter().any(|child| id < *child))
    );
    assert!(
        flow.uses()
            .iter()
            .any(|(&id, item)| item.arguments.iter().any(|child| id > *child))
    );
    let pools = ConstantPools::empty();
    let request =
        materialize_call_request_observed(&sealed.requests()[&rand], &pools, &mut work, 0).unwrap();
    let bound = catalog
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request.request(), &c)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let mut claims = BTreeMap::new();
    for (&id, item) in flow
        .uses()
        .iter()
        .filter(|(_, item)| item.definition == rand)
    {
        let fresh = catalog
            .prepare_fresh(
                CallEffectInput {
                    context: item.context.into(),
                    argument_uses: CallArgumentUses::SelectedChannels(&[]),
                    function_id: &bound.function_id,
                    kind: bound.kind,
                    selected: selected.as_ref(),
                    request: request.request(),
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Region(item.context.region),
                },
                Arc::clone(&selected),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(item.context),
                },
                &c,
            )
            .unwrap();
        claims.insert(
            id,
            ExpressionCallClaim {
                context: item.context,
                effects: fresh.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                temporal_source: None,
            },
        );
    }
    assert_eq!(claims.len(), 4);
    let roots: Vec<_> = sealed.root_use_ids().iter().map(|id| ids[id]).collect();
    let unit = BoundExprSet::try_from_parts_in(
        UnitParts::new(
            sealed.ports().to_vec().into_boxed_slice(),
            sealed.definitions().to_vec().into_boxed_slice(),
            sealed.roots().to_vec().into_boxed_slice(),
            roots.clone().into_boxed_slice(),
            flow,
            claims,
            sealed.requests().clone(),
        ),
        limits(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(
        unit.root_use_ids()[value_root.get() as usize],
        roots[value_root.get() as usize]
    );
    assert_eq!(
        unit.root_use_ids()[truth_root.get() as usize],
        roots[truth_root.get() as usize]
    );
    assert_eq!(
        unit.roots()[value_root.get() as usize].demand,
        EvaluationDemand::Value
    );
    assert_eq!(
        unit.roots()[truth_root.get() as usize].demand,
        EvaluationDemand::TruthOnly
    );
    assert_eq!(unit.definition(rand).unwrap().ty, source.ty);
    // Exercise the neutral receiver's original validation and all control
    // prefixes independently from installed compilation.
    let receive = |control: &Control| -> Result<BoundExprSet, UnitError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
        let out = BoundExprSet::try_from_parts_in(
            UnitParts::new(
                unit.ports().to_vec().into_boxed_slice(),
                unit.definitions().to_vec().into_boxed_slice(),
                unit.roots().to_vec().into_boxed_slice(),
                unit.root_use_ids().to_vec().into_boxed_slice(),
                unit.control_flow().clone(),
                unit.claims().clone(),
                unit.requests().clone(),
            ),
            limits(),
            &mut work,
        );
        if matches!(&out, Err(UnitError::Control(_))) {
            return out;
        }
        work.finish()?;
        out
    };
    let control = Control::good();
    let checked = receive(&control).unwrap();
    assert_eq!(checked.root_use_ids(), unit.root_use_ids());
    assert_eq!(checked.control_flow(), unit.control_flow());
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::at(at, cause.clone());
            assert!(
                matches!(receive(&control), Err(UnitError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    let invoke = |control: &Control| -> Result<Arc<CompiledExprSet>, ExprCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let out = compile_expr_set_pure(
            &unit,
            EvaluationEnvironment {
                constants: &pools,
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: &[],
                all_ports: None,
            },
            policy(),
            &catalog,
            &mut work,
        );
        if matches!(&out, Err(ExprCompileError::Control(_))) {
            return out;
        }
        work.finish()?;
        out
    };
    let control = Control::good();
    let program = invoke(&control).unwrap();
    assert_eq!(program.definitions().nodes().len(), 3);
    assert_eq!(program.calls().len(), 4);
    assert_eq!(program.effects().len(), 10);
    assert_eq!(
        program
            .roots()
            .iter()
            .map(|root| root.use_id)
            .collect::<Vec<_>>(),
        roots
    );
    for (&id, item) in unit.control_flow().uses() {
        let actual = &program.control_flow().uses()[&id];
        assert_eq!(actual.context, item.context);
        assert_eq!(actual.control, item.control);
        assert_eq!(actual.arguments, item.arguments);
        assert_eq!(
            actual.definition,
            program.compiled_definition(item.definition).unwrap()
        );
        assert_eq!(
            program.types()[actual.definition.index()],
            FunctionArgumentType::Value(unit.definition(item.definition).unwrap().ty.clone())
        );
        assert_eq!(program.effects()[&id].context(), item.context);
        if item.definition == rand {
            assert_eq!(
                program.calls()[&id].call_contract().context(),
                CallEffectContext::Expression(item.context)
            );
        }
    }
    for root in program.roots() {
        let context = program.control_flow().uses()[&root.use_id].context;
        let effects = program.effects()[&root.use_id].for_use(context).unwrap();
        assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
        assert!(effects.has_instance_state);
        assert!(effects.observable_effects.rng_sampling);
    }
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::at(at, cause.clone());
            assert!(
                matches!(invoke(&control), Err(ExprCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn zero_port_compiler_keeps_unsupported_primitive_capabilities() {
    use crate::zero_port;
    use arrow::array::{
        ArrayRef, BooleanArray, Date32Array, FixedSizeBinaryArray, Float64Array,
        TimestampSecondArray, new_null_array,
    };
    use arrow::datatypes::TimeUnit;
    use novarocks_functions::{ArithmeticPrepareError, CastPrepareError, ComparisonPrepareError};
    use novarocks_type_contract::TrinoCastMode;

    let allow_id = SemanticParameterId::new(0);
    let parameters = SemanticParameters::try_new([(
        allow_id,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    let allow = SemanticParameterRef {
        id: allow_id,
        expected_key: SemanticParameterKey::AllowThrowException,
    };
    let add = |left, right| ExprKind::Binary {
        left,
        op: BinaryOperator::Add,
        right,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        allow_throw_exception: Some(allow),
    };

    let mut draft = builder();
    let mut pools = ConstantPools::empty();
    let flag = zero_port::add_constant(
        &mut draft,
        &mut pools,
        Arc::new(BooleanArray::from(vec![true])),
        boolean(false),
    );
    let root = draft.add(None, int(true), add(flag, flag)).unwrap();
    draft
        .root(root, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        draft.seal(Vec::new(), &mut work),
        Err(UnitError::Invalid { definition: Some(id), root: None, message })
            if id == root && message.ends_with("binary expression types are inconsistent with its operator")
    ));
    work.finish().unwrap();

    // LARGEINT + DOUBLE has a frozen DOUBLE result. The arithmetic recipe
    // declines the mixed carrier when the compiler prepares the sealed unit.
    let mut draft = builder();
    let largeint = zero_port::add_constant(
        &mut draft,
        &mut pools,
        Arc::new(FixedSizeBinaryArray::try_from_iter([1_i128.to_be_bytes()].into_iter()).unwrap()),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
    );
    let double = zero_port::add_constant(
        &mut draft,
        &mut pools,
        Arc::new(Float64Array::from(vec![2.0])),
        FunctionValueType::new(DataType::Float64, false),
    );
    let root = draft
        .add(
            None,
            FunctionValueType::new(DataType::Float64, true),
            add(largeint, double),
        )
        .unwrap();
    draft
        .root(root, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let unit = finish(draft, Vec::new());
    let result = compile(
        &unit,
        &pools,
        &parameters,
        ScopeLayouts {
            ports: &[],
            all_ports: None,
        },
        &control,
    );
    assert!(matches!(
        result,
        Err(ExprCompileError::Arithmetic(
            ArithmeticPrepareError::Unsupported
        ))
    ));

    let result = zero_port::primitive(
        [Arc::new(Date32Array::from(vec![1])) as ArrayRef],
        FunctionValueType::new(DataType::Int8, true),
        |args| ExprKind::Cast {
            expr: args[0],
            target: DataType::Int8,
            cast_mode: TrinoCastMode::Explicit,
        },
        &parameters,
    );
    assert!(matches!(
        result,
        Err(ExprCompileError::Cast(CastPrepareError::Illegal))
    ));

    let target = DataType::Timestamp(TimeUnit::Microsecond, None);
    let result = zero_port::primitive(
        [Arc::new(TimestampSecondArray::from(vec![1])) as ArrayRef],
        FunctionValueType::new(target.clone(), false),
        |args| ExprKind::Cast {
            expr: args[0],
            target,
            cast_mode: TrinoCastMode::Explicit,
        },
        &parameters,
    );
    assert!(matches!(
        result,
        Err(ExprCompileError::Cast(CastPrepareError::Unsupported))
    ));

    // Signed integer ARRAY equality has an installed recipe. VARCHAR items do not.
    let ty = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    for op in [BinaryOperator::Eq, BinaryOperator::EqForNull] {
        let result = zero_port::primitive(
            [new_null_array(&ty, 1), new_null_array(&ty, 1)],
            boolean(op == BinaryOperator::Eq),
            |args| ExprKind::Binary {
                left: args[0],
                op,
                right: args[1],
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
            &parameters,
        );
        assert!(matches!(
            result,
            Err(ExprCompileError::Comparison(
                ComparisonPrepareError::Unsupported
            ))
        ));
    }
}

#[test]
fn null_list_json_carrier_compiles_live_and_unused_with_the_implicit_mode() {
    let source = ValueType::new(
        DataType::List(Arc::new(Field::new("source-item", DataType::Null, true))),
        false,
    );
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        true,
        storage.clone(),
    )
    .unwrap();
    let result = ValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap(),
        ValueLayout::try_new(
            &DataType::List(Arc::new(Field::new(
                "result-item",
                DataType::LargeBinary,
                true,
            ))),
            storage,
        )
        .unwrap(),
    )
    .unwrap();
    let input = ValueId::new(0);
    let values = [(input, source.clone())];
    let layout = layouts(&values);
    for unused in [false, true] {
        let mut builder = builder();
        let child = builder
            .add(None, source.clone(), ExprKind::Value(input))
            .unwrap();
        let cast = builder
            .add(
                None,
                result.clone(),
                ExprKind::Cast {
                    expr: child,
                    target: result.data_type().clone(),
                    cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
                },
            )
            .unwrap();
        let ports = vec![ExprPort {
            values: values.clone().into(),
        }];
        let (unit, catalogue) = if unused {
            resolved::type_only_fixture(
                builder,
                [(cast, result.clone()), (child, source.clone())],
                ports,
            )
        } else {
            builder
                .root(cast, EvaluationDemand::Value, RootScope::Port(0))
                .unwrap();
            (finish(builder, ports), catalogue())
        };
        let parameters = SemanticParameters::default();
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let compiled = compile_expr_set_pure(
            &unit,
            EvaluationEnvironment {
                constants: &ConstantPools::empty(),
                parameters: &parameters,
            },
            ScopeLayouts {
                ports: std::slice::from_ref(&layout),
                all_ports: None,
            },
            policy(),
            &catalogue,
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let local_child = compiled.compiled_definition(child).unwrap();
        let local_cast = compiled.compiled_definition(cast).unwrap();
        let retained = compiled.definitions().node(local_cast).unwrap();
        assert_eq!(retained.value_type(), Some(&result));
        assert_eq!(
            compiled
                .definitions()
                .node(local_child)
                .unwrap()
                .value_type(),
            Some(&source)
        );
        assert!(matches!(
            retained.kind(),
            StaticExprKind::PreparedCast {
                operation: novarocks_functions::CastOperation::Carrier,
                child,
                cast_mode: novarocks_type_contract::TrinoCastMode::Implicit,
            } if *child == local_child
        ));
        let root = &compiled.roots()[0];
        assert_eq!(
            compiled.control_flow().uses().len(),
            if unused { 1 } else { 2 }
        );
        let recipe = compiled.recipes().cast(root.use_id);
        if unused {
            let uses = compiled.control_flow().uses();
            assert_eq!(uses[&root.use_id].control, ControlShape::TypeOnly);
            assert!(uses[&root.use_id].arguments.is_empty());
            assert!(
                uses.values()
                    .all(|usage| usage.definition != local_cast && usage.definition != local_child)
            );
            assert!(recipe.is_none());
        } else {
            let recipe = recipe.unwrap();
            assert_eq!(recipe.source_type(), &source);
            assert_eq!(recipe.result_type(), &result);
            assert_eq!(
                recipe.cast_mode(),
                Some(novarocks_type_contract::TrinoCastMode::Implicit)
            );
        }
    }
}
