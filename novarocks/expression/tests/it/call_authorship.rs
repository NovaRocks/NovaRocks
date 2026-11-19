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

//! Public call-author contract: installed control shapes, claims and effect summaries.

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use novarocks_constant_contract::{ConstantPolicy, ConstantPools};
use novarocks_expression::{
    CallAuthorInputs, EvaluationEnvironment, author_call_controls, author_unit_calls,
    testkit::{ClaimAuthor, HarnessControl, add_call, register_call_requests},
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprKind, ExprPort, RootScope, UnitLimits, ValueId,
};
use novarocks_functions::FunctionArgument;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    FunctionValueType, SemanticParameters,
};

const POLICY: ConstantPolicy = ConstantPolicy {
    max_rows: 16,
    max_array_nodes: 128,
    max_logical_elements: 1024,
    max_retained_buffer_bytes: 1 << 20,
    max_type_depth: 64,
    max_type_nodes: 4096,
    max_dictionary_depth: 64,
    max_metadata_bytes: 1 << 20,
    max_library_validation_work: 1 << 20,
    max_library_validation_bytes: 1 << 20,
};

fn utf8() -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, true)
}

#[test]
fn upper_call_is_eager_and_carries_no_source_fact() {
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut constants = ConstantPools::empty();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 64,
    });
    let input = ValueId::new(1);
    let ty = utf8();
    let argument = draft.add(None, ty.clone(), ExprKind::Value(input)).unwrap();
    let owner = ClaimAuthor::with_exact_eager_binding(
        &functions,
        "upper",
        vec![FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        }],
        POLICY,
        &HarnessControl,
    );
    let mut authors = BTreeMap::new();
    let call = add_call(&mut draft, &mut authors, owner, vec![argument]);
    draft
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let decimal = BTreeMap::from([(call, DecimalOverflowPolicy::ReportError)]);
    let inputs = CallAuthorInputs {
        environment: EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        constant_policy: POLICY,
        functions: &functions,
        decimal_overflow: &decimal,
    };
    let controls = author_call_controls(&draft, &inputs, &mut work).unwrap();
    assert_eq!(controls.len(), 1);
    assert_eq!(controls[0].0, call);
    assert_eq!(controls[0].1.shape, ControlShape::Eager);
    assert!(controls[0].1.temporal_sources.is_none());
    for (id, control) in controls {
        draft.definition_control(id, control).unwrap();
    }
    let sealed = draft
        .seal(
            vec![ExprPort {
                values: vec![(input, ty)].into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    let authored = author_unit_calls(&sealed, &inputs, &mut work).unwrap();
    assert_eq!(authored.claims.len(), 1);
    let claim = authored.claims.values().next().unwrap();
    assert_eq!(
        claim.decimal_overflow_policy,
        DecimalOverflowPolicy::ReportError
    );
    assert!(claim.regexp_count_pattern_source.is_none());
    assert!(claim.to_base64_byte_source.is_none());
    assert!(claim.temporal_source.is_none());
    assert_eq!(authored.summaries.len(), sealed.uses().len());
}

#[test]
fn missing_overflow_policy_is_an_invariant() {
    let functions = novarocks_functions::testkit::installed_builtin_owner_catalogue();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let mut constants = ConstantPools::empty();
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 64,
    });
    let ty = utf8();
    let argument = draft
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(1)))
        .unwrap();
    let owner = ClaimAuthor::with_exact_eager_binding(
        &functions,
        "upper",
        vec![FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        }],
        POLICY,
        &HarnessControl,
    );
    let mut authors = BTreeMap::new();
    let call = add_call(&mut draft, &mut authors, owner, vec![argument]);
    draft
        .root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    register_call_requests(&mut draft, &authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    let empty = BTreeMap::new();
    let inputs = CallAuthorInputs {
        environment: EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        constant_policy: POLICY,
        functions: &functions,
        decimal_overflow: &empty,
    };
    for (id, control) in author_call_controls(&draft, &inputs, &mut work).unwrap() {
        draft.definition_control(id, control).unwrap();
    }
    let sealed = draft
        .seal(
            vec![ExprPort {
                values: vec![(ValueId::new(1), ty)].into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    let error = match author_unit_calls(&sealed, &inputs, &mut work) {
        Err(error) => error,
        Ok(_) => panic!("missing overflow policy was accepted"),
    };
    let text = error.to_string();
    assert!(
        text.contains("decimal overflow policy"),
        "unexpected refusal text: {text}"
    );
}

struct AuthorFixture {
    draft: BoundExprSetBuilder,
    constants: ConstantPools,
    functions: std::sync::Arc<novarocks_functions::PureEngineFunctionCatalog>,
    authors: BTreeMap<novarocks_expression_contract::ExprId, ClaimAuthor>,
    decimal: BTreeMap<novarocks_expression_contract::ExprId, DecimalOverflowPolicy>,
}

impl AuthorFixture {
    fn new() -> Self {
        Self {
            draft: BoundExprSetBuilder::new(UnitLimits {
                expression_semantic_depth: 64,
            }),
            constants: ConstantPools::empty(),
            functions: novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            authors: BTreeMap::new(),
            decimal: BTreeMap::new(),
        }
    }

    fn add(
        &mut self,
        ty: FunctionValueType,
        kind: ExprKind,
    ) -> novarocks_expression_contract::ExprId {
        self.draft.add(None, ty, kind).unwrap()
    }

    fn call(
        &mut self,
        name: &str,
        args: &[novarocks_expression_contract::ExprId],
        policy: DecimalOverflowPolicy,
    ) -> novarocks_expression_contract::ExprId {
        let arguments = args
            .iter()
            .map(|arg| FunctionArgument::Value {
                value_type: self.ty(*arg),
                constant: None,
            })
            .collect();
        // The production author projects control from the installed overload.
        let owner = ClaimAuthor::with_legacy_binding(
            &self.functions,
            name,
            arguments,
            ControlShape::Eager,
            None,
            POLICY,
            &HarnessControl,
        )
        .with_decimal_policy(policy);
        let definition = add_call(&mut self.draft, &mut self.authors, owner, args.to_vec());
        self.decimal.insert(definition, policy);
        definition
    }

    fn ty(&self, id: novarocks_expression_contract::ExprId) -> FunctionValueType {
        self.draft.definition(id).unwrap().ty.clone()
    }

    fn constant(
        &mut self,
        array: arrow::array::ArrayRef,
        ty: FunctionValueType,
    ) -> novarocks_expression_contract::ExprId {
        super::zero_port::add_constant(&mut self.draft, &mut self.constants, array, ty)
    }

    fn root(
        &mut self,
        definition: novarocks_expression_contract::ExprId,
        demand: EvaluationDemand,
    ) {
        self.draft
            .root(definition, demand, RootScope::AllPorts)
            .unwrap();
    }

    fn seal(&mut self) -> novarocks_expression_contract::SealedUnit {
        register_call_requests(&mut self.draft, &self.authors, &mut self.constants);
        let parameters = SemanticParameters::default();
        let inputs = CallAuthorInputs {
            environment: EvaluationEnvironment {
                constants: &self.constants,
                parameters: &parameters,
            },
            constant_policy: POLICY,
            functions: &self.functions,
            decimal_overflow: &self.decimal,
        };
        let mut work =
            CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
        for (definition, shape) in author_call_controls(&self.draft, &inputs, &mut work).unwrap() {
            self.draft.definition_control(definition, shape).unwrap();
        }
        let draft = std::mem::replace(
            &mut self.draft,
            BoundExprSetBuilder::new(UnitLimits {
                expression_semantic_depth: 64,
            }),
        );
        let sealed = draft.seal(Vec::new(), &mut work).unwrap();
        work.finish().unwrap();
        sealed
    }

    fn author(
        &self,
        unit: &novarocks_expression_contract::SealedUnit,
        parameters: &SemanticParameters,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<novarocks_expression::UnitCallAuthorship, novarocks_expression::CallAuthorError>
    {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let input = CallAuthorInputs {
            environment: EvaluationEnvironment {
                constants: &self.constants,
                parameters,
            },
            constant_policy: POLICY,
            functions: &self.functions,
            decimal_overflow: &self.decimal,
        };
        let result = author_unit_calls(unit, &input, &mut work);
        work.finish_result(result, novarocks_expression::CallAuthorError::from)
    }
}

#[test]
fn shared_if_authors_exact_guarded_claims_for_value_and_truth_demands() {
    use arrow::array::BooleanArray;
    use novarocks_type_contract::{CallProofScope, ExpressionEffects, GuardKind, RegionGuard};
    use std::sync::Arc;

    let mut fixture = AuthorFixture::new();
    let ty = FunctionValueType::new(DataType::Boolean, false);
    let yes = fixture.constant(Arc::new(BooleanArray::from(vec![true])), ty.clone());
    let no = fixture.constant(Arc::new(BooleanArray::from(vec![false])), ty);
    let root = fixture.call("if", &[yes, no, yes], DecimalOverflowPolicy::OutputNull);
    fixture.root(root, EvaluationDemand::TruthOnly);
    fixture.root(root, EvaluationDemand::Value);
    let sealed = fixture.seal();
    let parameters = SemanticParameters::try_new([(
        novarocks_type_contract::SemanticParameterId::new(91),
        novarocks_type_contract::SemanticParameterValue::TimeZone("UTC".into()),
    )])
    .unwrap();
    let authored = fixture
        .author(&sealed, &parameters, &HarnessControl)
        .unwrap();
    let flow = sealed.control_flow();

    assert_eq!(authored.claims.len(), 2);
    assert_eq!(authored.summaries.len(), flow.uses().len());
    assert_ne!(sealed.root_use_ids()[0], sealed.root_use_ids()[1]);
    for (&id, demand) in sealed
        .root_use_ids()
        .iter()
        .zip([EvaluationDemand::TruthOnly, EvaluationDemand::Value])
    {
        let invocation = &flow.uses()[&id];
        assert_eq!(invocation.definition, root);
        assert_eq!(invocation.control, ControlShape::If);
        assert_eq!(invocation.context.demand, demand);
        let claim = &authored.claims[&id];
        assert_eq!(claim.context, invocation.context);
        assert_eq!(
            claim.effects.proof_scope,
            CallProofScope::Region(invocation.context.region)
        );
        assert!(claim.effects.environment.is_empty());
        assert_eq!(
            claim.decimal_overflow_policy,
            DecimalOverflowPolicy::OutputNull
        );
        assert_eq!(
            authored.summaries[&id].for_use(invocation.context).unwrap(),
            ExpressionEffects::PURE_VALUE
        );
        for (ordinal, kind) in [(1, GuardKind::IfThen), (2, GuardKind::IfElse)] {
            let child = &flow.uses()[&invocation.arguments[ordinal]];
            let domain = &flow.regions()[&child.context.region];
            assert_eq!(domain.parent, Some(invocation.context.region));
            assert_eq!(domain.guard, Some(RegionGuard { owner: id, kind }));
            assert_eq!(child.context.demand, demand);
        }
    }
}

#[test]
fn guarded_parents_preserve_shared_definition_state_and_per_definition_decimal_effects() {
    use arrow::array::Decimal128Array;
    use novarocks_type_contract::{FunctionIntrinsicRowError, FunctionVolatility};
    use std::sync::Arc;

    let mut fixture = AuthorFixture::new();
    let rand = fixture.call("rand", &[], DecimalOverflowPolicy::OutputNull);
    let rng_root = fixture.call("coalesce", &[rand, rand], DecimalOverflowPolicy::OutputNull);

    let decimal = fixture.constant(
        Arc::new(
            Decimal128Array::from(vec![12345])
                .with_precision_and_scale(38, 3)
                .unwrap(),
        ),
        FunctionValueType::new(DataType::Decimal128(38, 3), true),
    );
    let mut decimal_roots = Vec::new();
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        let round = fixture.call("round", &[decimal], policy);
        let root = fixture.call(
            "coalesce",
            &[round, round],
            DecimalOverflowPolicy::OutputNull,
        );
        decimal_roots.push((round, root, policy));
    }
    fixture.root(rng_root, EvaluationDemand::Value);
    for (_, root, _) in &decimal_roots {
        fixture.root(*root, EvaluationDemand::Value);
    }
    let sealed = fixture.seal();
    let authored = fixture
        .author(&sealed, &SemanticParameters::default(), &HarnessControl)
        .unwrap();
    let flow = sealed.control_flow();
    let rng_use = sealed.root_use_ids()[0];
    let effects = authored.summaries[&rng_use]
        .for_use(flow.uses()[&rng_use].context)
        .unwrap();
    assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
    assert!(effects.has_instance_state);
    assert!(effects.observable_effects.rng_sampling);
    assert_eq!(
        authored.claims[&rng_use].effects.own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
    assert_eq!(authored.claims.len(), 9);
    assert_eq!(authored.summaries.len(), flow.uses().len());
    let mut random_contexts = flow.uses().values().filter(|item| item.definition == rand);
    let regions = [
        random_contexts.next().unwrap().context.region,
        random_contexts.next().unwrap().context.region,
    ];
    assert_ne!(regions[0], regions[1]);
    assert!(random_contexts.next().is_none());
    for (&id, (round, _, policy)) in sealed.root_use_ids()[1..].iter().zip(decimal_roots) {
        let invocation = &flow.uses()[&id];
        let summary = authored.summaries[&id].for_use(invocation.context).unwrap();
        assert_eq!(
            summary.may_raise_row_error,
            policy == DecimalOverflowPolicy::ReportError
        );
        for child in &invocation.arguments {
            let child_use = &flow.uses()[child];
            assert_eq!(child_use.definition, round);
            assert_eq!(authored.claims[child].decimal_overflow_policy, policy);
            assert_eq!(authored.summaries[child].context(), child_use.context);
        }
    }
}

#[derive(Default)]
struct RecordingAuthorControl {
    trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, novarocks_type_contract::CompileControlError)>,
}

impl novarocks_type_contract::PureCompileControl for RecordingAuthorControl {
    fn checkpoint(
        &self,
        phase: CompilePhase,
        units: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let position = trace.len();
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        trace.push((phase, units));
        if let Some((stop, cause)) = &self.stop {
            assert!(position <= *stop, "callback after the original refusal");
            if position == *stop {
                return Err(cause.clone());
            }
        }
        Ok(())
    }
}

#[test]
fn call_author_preserves_first_control_and_actual_wide_edge_order() {
    use arrow::array::{BooleanArray, StringArray};
    use novarocks_expression::CallAuthorError;
    use novarocks_type_contract::{
        CompileControlError, ExpressionEffects, MAX_UNOBSERVED_COMPILE_WORK,
    };
    use std::sync::Arc;

    for width in [2, 320] {
        let mut fixture = AuthorFixture::new();
        let args: Vec<_> = (0..width)
            .map(|ordinal| {
                fixture.constant(
                    Arc::new(StringArray::from(vec![Some(ordinal.to_string())])),
                    utf8(),
                )
            })
            .collect();
        let root = fixture.call("coalesce", &args, DecimalOverflowPolicy::OutputNull);
        fixture.root(root, EvaluationDemand::Value);
        let boolean = FunctionValueType::new(DataType::Boolean, false);
        let flag = fixture.constant(Arc::new(BooleanArray::from(vec![true])), boolean.clone());
        let conjunction = fixture.add(
            boolean,
            ExprKind::Conjunction {
                args: vec![flag; width].into_boxed_slice(),
            },
        );
        fixture.root(conjunction, EvaluationDemand::Value);
        let sealed = fixture.seal();
        let parameters = SemanticParameters::default();
        let initial = RecordingAuthorControl::default();
        let authored = fixture.author(&sealed, &parameters, &initial).unwrap();
        let id = sealed.root_use_ids()[0];
        let flow = sealed.control_flow();
        let invocation = &flow.uses()[&id];
        assert_eq!(invocation.control, ControlShape::Coalesce);
        assert_eq!(invocation.arguments.len(), width);
        for (&id, definition) in invocation.arguments.iter().zip(args) {
            assert_eq!(flow.uses()[&id].definition, definition);
        }
        assert_eq!(authored.claims.len(), 1);
        assert_eq!(authored.summaries.len(), flow.uses().len());
        let conjunction = &flow.uses()[&sealed.root_use_ids()[1]];
        assert_eq!(
            authored.summaries[&conjunction.context.use_id]
                .for_use(conjunction.context)
                .unwrap(),
            ExpressionEffects::PURE_VALUE
        );
        let trace = initial.trace.into_inner().unwrap();
        let sampled = if width == 2 {
            (0..trace.len()).collect()
        } else {
            vec![
                0,
                trace
                    .iter()
                    .position(|&(phase, count)| {
                        phase == CompilePhase::FunctionSpecialization
                            && count == MAX_UNOBSERVED_COMPILE_WORK
                    })
                    .expect("wide effect author crosses a real checkpoint quantum"),
                trace.len() - 1,
            ]
        };
        for at in sampled {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = RecordingAuthorControl {
                    stop: Some((at, cause.clone())),
                    ..RecordingAuthorControl::default()
                };
                assert!(matches!(
                    fixture.author(&sealed, &parameters, &control),
                    Err(CallAuthorError::Control(actual)) if actual == cause
                ));
                assert_eq!(control.trace.into_inner().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn primitive_cast_summaries_preserve_null_and_error_semantics_and_typed_refusals() {
    use arrow::{
        array::{TimestampMicrosecondArray, TimestampSecondArray},
        datatypes::TimeUnit,
    };
    use novarocks_expression::{CallAuthorError, CapabilityRefusal};
    use novarocks_functions::CastPrepareError;
    use novarocks_type_contract::{
        ExpressionEffects, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
        SemanticParameterValue,
    };
    use std::sync::Arc;

    let allow = SemanticParameterRef {
        id: SemanticParameterId::new(71),
        expected_key: SemanticParameterKey::AllowThrowException,
    };
    let parameters = SemanticParameters::try_new([(
        allow.id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap();
    let mut fixture = AuthorFixture::new();
    let seconds = fixture.constant(
        Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Second, None), false),
    );
    let micros = fixture.constant(
        Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX])),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), false),
    );
    for (expr, target, nullable) in [
        (
            seconds,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        (
            micros,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ] {
        let cast = fixture.add(
            FunctionValueType::new(target.clone(), nullable),
            ExprKind::Cast {
                expr,
                target,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: allow,
            },
        );
        fixture.root(cast, EvaluationDemand::Value);
    }
    let sealed = fixture.seal();
    let authored = fixture
        .author(&sealed, &parameters, &HarnessControl)
        .unwrap();
    assert!(authored.claims.is_empty());
    let contexts = sealed.control_flow().uses();
    let root = sealed.root_use_ids()[0];
    let summary = authored.summaries[&root]
        .for_use(contexts[&root].context)
        .unwrap();
    assert_eq!(summary, ExpressionEffects::PURE_VALUE);
    let root = sealed.root_use_ids()[1];
    let summary = authored.summaries[&root]
        .for_use(contexts[&root].context)
        .unwrap();
    assert!(summary.may_raise_row_error);

    for invalid in [
        SemanticParameters::default(),
        SemanticParameters::try_new([(allow.id, SemanticParameterValue::TimeZone("UTC".into()))])
            .unwrap(),
    ] {
        let Err(error @ CallAuthorError::Capability(CapabilityRefusal::Effects(_))) =
            fixture.author(&sealed, &invalid, &HarnessControl)
        else {
            panic!("missing or foreign semantic parameter must fail through the effects author");
        };
        assert!(
            error
                .to_string()
                .starts_with("package call effects: Parameter(")
        );
    }

    let mut fixture = AuthorFixture::new();
    let input = fixture.constant(
        Arc::new(TimestampSecondArray::from(vec![i64::MAX])),
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Second, None), false),
    );
    let target = DataType::Timestamp(TimeUnit::Microsecond, None);
    let cast = fixture.add(
        FunctionValueType::new(target.clone(), false),
        ExprKind::Cast {
            expr: input,
            target,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            allow_throw_exception: allow,
        },
    );
    fixture.root(cast, EvaluationDemand::Value);
    let sealed = fixture.seal();
    let Err(CallAuthorError::Capability(CapabilityRefusal::Primitive { source, cast: None })) =
        fixture.author(&sealed, &parameters, &HarnessControl)
    else {
        panic!("nonnullable overflowing carrier must fail through the cast owner");
    };
    assert_eq!(
        source.downcast_ref::<CastPrepareError>(),
        Some(&CastPrepareError::TypeMismatch),
    );
}

#[test]
fn call_author_keeps_the_installed_regexp_base64_and_temporal_sources() {
    use arrow::array::StringArray;
    use novarocks_type_contract::{
        RegexpCountPatternSource, TemporalSourceShape, ToBase64ByteSource,
    };
    use std::sync::Arc;

    let mut fixture = AuthorFixture::new();
    let text = fixture.constant(Arc::new(StringArray::from(vec!["x"])), utf8());
    let pattern = fixture.constant(Arc::new(StringArray::from(vec!["."])), utf8());
    let dynamic = fixture.call("lower", &[pattern], DecimalOverflowPolicy::OutputNull);
    let literal_regex = fixture.call(
        "regexp_count",
        &[text, pattern],
        DecimalOverflowPolicy::OutputNull,
    );
    let dynamic_regex = fixture.call(
        "regexp_count",
        &[text, dynamic],
        DecimalOverflowPolicy::OutputNull,
    );
    let ordinary = fixture.call("to_base64", &[text], DecimalOverflowPolicy::OutputNull);
    // FROM_BASE64 is an installed source with the same immediate Latin-1
    // receipt used by native encryption byte producers.
    let bytes = fixture.call("from_base64", &[text], DecimalOverflowPolicy::OutputNull);
    let latin1 = fixture.call("to_base64", &[bytes], DecimalOverflowPolicy::OutputNull);
    let temporal = fixture.call("time_to_sec", &[text], DecimalOverflowPolicy::OutputNull);
    for root in [literal_regex, dynamic_regex, ordinary, latin1, temporal] {
        fixture.root(root, EvaluationDemand::Value);
    }
    let sealed = fixture.seal();
    let authored = fixture
        .author(&sealed, &SemanticParameters::default(), &HarnessControl)
        .unwrap();
    let roots = sealed.root_use_ids();
    assert_eq!(
        authored.claims[&roots[0]].regexp_count_pattern_source,
        Some(RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent)
    );
    assert_eq!(
        authored.claims[&roots[1]].regexp_count_pattern_source,
        Some(RegexpCountPatternSource::Dynamic)
    );
    assert_eq!(
        authored.claims[&roots[2]].to_base64_byte_source,
        Some(ToBase64ByteSource::Ordinary)
    );
    assert_eq!(
        authored.claims[&roots[3]].to_base64_byte_source,
        Some(ToBase64ByteSource::NativeV1EncryptionLatin1)
    );
    let temporal = authored.claims[&roots[4]].temporal_source.as_ref().unwrap();
    assert_eq!(temporal.facts.shape(), TemporalSourceShape::SecondsDirect);
    assert_eq!(temporal.sources.len(), 1);
    assert_eq!(temporal.sources[0].definition, text);
    assert_eq!(
        sealed.control_flow().uses()[&roots[4]].control,
        ControlShape::TemporalSource(TemporalSourceShape::SecondsDirect)
    );
}

#[test]
fn case_inside_boolean_guard_keeps_inactive_error_and_random_state_summaries() {
    use arrow::array::{BooleanArray, Decimal128Array, Float64Array};
    use novarocks_expression_contract::BinaryOperator;
    use novarocks_type_contract::{FunctionVolatility, GuardKind, RegionGuard};
    use std::sync::Arc;

    let mut fixture = AuthorFixture::new();
    let required = FunctionValueType::new(DataType::Boolean, false);
    let condition = fixture.constant(Arc::new(BooleanArray::from(vec![true])), required.clone());
    let deciding = fixture.constant(Arc::new(BooleanArray::from(vec![false])), required.clone());
    let decimal = fixture.constant(
        Arc::new(
            Decimal128Array::from(vec![10_i128.pow(38) - 1])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
        FunctionValueType::new(DataType::Decimal128(38, 0), true),
    );
    let round = fixture.call("round", &[decimal], DecimalOverflowPolicy::ReportError);
    let then_value = fixture.add(
        required,
        ExprKind::IsNull {
            expr: round,
            negated: false,
        },
    );
    let rand = fixture.call("rand", &[], DecimalOverflowPolicy::OutputNull);
    let zero = fixture.constant(Arc::new(Float64Array::from(vec![0.0])), fixture.ty(rand));
    let ty = FunctionValueType::new(DataType::Boolean, fixture.ty(rand).nullable());
    let else_value = fixture.add(
        ty.clone(),
        ExprKind::Binary {
            left: rand,
            op: BinaryOperator::Gt,
            right: zero,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: None,
        },
    );
    let case = fixture.add(
        ty.clone(),
        ExprKind::Case {
            operand: None,
            when_then: Box::from([(condition, then_value)]),
            else_expr: Some(else_value),
        },
    );
    let root = fixture.add(
        ty,
        ExprKind::Conjunction {
            args: Box::from([deciding, case]),
        },
    );
    fixture.root(root, EvaluationDemand::Value);
    let sealed = fixture.seal();
    let authored = fixture
        .author(&sealed, &SemanticParameters::default(), &HarnessControl)
        .unwrap();
    let flow = sealed.control_flow();
    let id = sealed.root_use_ids()[0];
    let summary = authored.summaries[&id]
        .for_use(flow.uses()[&id].context)
        .unwrap();
    assert_eq!(summary.value_stability, FunctionVolatility::Volatile);
    assert!(summary.has_instance_state);
    assert!(summary.observable_effects.rng_sampling);
    assert!(summary.may_raise_row_error);
    assert_eq!(authored.claims.len(), 2);
    let case_id = flow.uses()[&id].arguments[1];
    let case_use = &flow.uses()[&case_id];
    assert_eq!(case_use.definition, case);
    assert_eq!(
        case_use.control,
        ControlShape::Case {
            simple: false,
            arms: 1,
            has_else: true
        },
    );
    for (ordinal, kind) in [
        (1, GuardKind::CaseThen { arm: 0 }),
        (2, GuardKind::CaseElse),
    ] {
        let child = &flow.uses()[&case_use.arguments[ordinal]];
        let region = &flow.regions()[&child.context.region];
        assert_eq!(region.parent, Some(case_use.context.region));
        assert_eq!(
            region.guard,
            Some(RegionGuard {
                owner: case_id,
                kind
            })
        );
    }
}

#[test]
fn absent_installed_overload_retains_the_typed_author_refusal_and_text() {
    use arrow::array::StringArray;
    use novarocks_expression::{CallAuthorError, CapabilityRefusal};
    use std::sync::Arc;

    let mut fixture = AuthorFixture::new();
    let text = fixture.constant(Arc::new(StringArray::from(vec!["abc"])), utf8());
    let root = fixture.call("upper", &[text], DecimalOverflowPolicy::ReportError);
    fixture.root(root, EvaluationDemand::Value);
    let sealed = fixture.seal();
    fixture.functions = Arc::new(novarocks_functions::testkit::rng_subset());
    let Err(error @ CallAuthorError::Capability(CapabilityRefusal::Effects(_))) =
        fixture.author(&sealed, &SemanticParameters::default(), &HarnessControl)
    else {
        panic!("a sealed catalogue without UPPER must refuse its authored call");
    };
    assert_eq!(
        error.to_string(),
        "package call effects: Scalar(Occurrence(Function(Binding(UnknownFunction))))",
    );
}
