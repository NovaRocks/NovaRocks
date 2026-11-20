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
//! Tests the actual emitted definition author, not an earlier SQL syntax tree.
use arrow_schema::{DataType, TimeUnit};
use novarocks_expression_contract::{
    BoundFunction, ExprId, ExprKind, ExprNode, ValueId, ValueType,
};
use novarocks_expression_contract::{
    TemporalSourceProjectionError as SourceError, temporal_source_definitions_observed as author,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    PureCompileControl, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    TemporalCastKind, TemporalSourceDefinitions, TemporalSourceFacts, TemporalSourceKind as Kind,
};
use novarocks_type_contract::{FunctionId, FunctionKind, FunctionOverloadId};
use std::sync::Mutex;
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refuse {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        match self.refuse.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn node(id: u32, ty: DataType, kind: ExprKind) -> ExprNode {
    ExprNode {
        id: ExprId::new(id - 1),
        lambda_scope: None,
        ty: ValueType::new(ty, true),
        kind,
    }
}
fn leaf(id: u32, ty: DataType) -> ExprNode {
    node(id, ty, ExprKind::Value(ValueId::new(id)))
}
fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}
fn cast(id: u32, child: u32, target: DataType) -> ExprNode {
    node(
        id,
        target.clone(),
        ExprKind::Cast {
            array_failure_mode: None,
            expr: ExprId::new(child - 1),
            target,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: SemanticParameterRef {
                id: SemanticParameterId::new(0),
                expected_key: SemanticParameterKey::AllowThrowException,
            },
        },
    )
}
fn call(id: u32, identity: &str, child: u32, ty: DataType) -> ExprNode {
    node(
        id,
        ty.clone(),
        ExprKind::FunctionCall {
            function: BoundFunction {
                function_id: FunctionId::try_new(identity).unwrap(),
                overload: FunctionOverloadId::try_new("fixture/exact-v1").unwrap(),
                kind: FunctionKind::Scalar,
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    Box::from([novarocks_type_contract::FunctionArgumentType::Value(
                        ValueType::new(DataType::Int64, true),
                    )]),
                ),
                result_type: ValueType::new(ty, true),
                legacy_metadata: None,
            },
            args: Box::from([ExprId::new(child - 1)]),
        },
    )
}
fn definitions(nodes: Vec<ExprNode>) -> Vec<ExprNode> {
    for (ordinal, node) in nodes.iter().enumerate() {
        assert_eq!(node.id.get() as usize, ordinal);
    }
    nodes
}

fn run(
    a: &[ExprNode],
    kind: Kind,
    args: &[u32],
    control: &Control,
) -> Result<TemporalSourceDefinitions<ExprId>, SourceError> {
    let ids = args
        .iter()
        .map(|id| ExprId::new(*id - 1))
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = author(kind, a, &ids, &mut work);
    if matches!(result, Err(SourceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
#[test]
fn physical_format_source_tracks_only_actual_immediate_ordinary_cast() {
    let a = definitions(vec![
        leaf(1, DataType::Utf8),
        cast(2, 1, ts()),
        leaf(3, DataType::Utf8),
        cast(4, 1, DataType::Time64(TimeUnit::Microsecond)),
        cast(5, 2, ts()),
    ]);
    let c = Control::default();
    let override_source = run(&a, Kind::TimeFormat, &[2, 3], &c).unwrap();
    assert_eq!(
        override_source.facts,
        TemporalSourceFacts::FormatUtf8Override
    );
    assert_eq!(
        override_source.definitions.as_ref(),
        [ExprId::new(0), ExprId::new(1), ExprId::new(2)]
    );
    for normal in [4, 5] {
        let plain = run(&a, Kind::TimeFormat, &[normal, 3], &c).unwrap();
        assert_eq!(plain.facts, TemporalSourceFacts::FormatOrdinary);
        assert_eq!(
            plain.definitions.as_ref(),
            [ExprId::new(normal - 1), ExprId::new(2)]
        );
    }
}
#[test]
fn physical_seconds_source_prunes_actual_unreachable_fallback_definitions() {
    let a = definitions(vec![
        leaf(1, DataType::Utf8),
        cast(2, 1, ts()),
        cast(3, 2, DataType::Utf8),
        leaf(4, DataType::Date32),
        cast(5, 4, ts()),
    ]);
    let direct = run(&a, Kind::TimeToSec, &[3], &Control::default()).unwrap();
    assert_eq!(
        direct.facts.shape(),
        novarocks_type_contract::TemporalSourceShape::SecondsDirect
    );
    assert_eq!(direct.definitions.as_ref(), [ExprId::new(2)]);
    assert_eq!(
        direct.facts.cast_chain(),
        [TemporalCastKind::Ordinary, TemporalCastKind::Ordinary]
    );
    let string = run(&a, Kind::TimeToSec, &[2], &Control::default()).unwrap();
    assert_eq!(
        string.definitions.as_ref(),
        [ExprId::new(1), ExprId::new(0)]
    );
    assert!(matches!(
        string.facts,
        TemporalSourceFacts::SecondsCastString { .. }
    ));
    let other = run(&a, Kind::TimeToSec, &[5], &Control::default()).unwrap();
    assert!(matches!(
        other.facts,
        TemporalSourceFacts::SecondsCastOther { .. }
    ));
    // Same definition is demanded twice; an occurrence author must mint fresh IDs.
    assert_eq!(
        other.definitions.as_ref(),
        [ExprId::new(4), ExprId::new(3), ExprId::new(3)]
    );
}
#[test]
fn physical_seconds_roundtrip_uses_emitted_original_argument_not_transformed_child() {
    for identity in [
        "builtin.scalar/sec_to_time/v1",
        "parametric.scalar/sec_to_time/v1",
    ] {
        let a = definitions(vec![
            leaf(1, DataType::Int64),
            call(2, identity, 1, DataType::Utf8),
            cast(3, 2, ts()),
            cast(4, 3, DataType::Time64(TimeUnit::Microsecond)),
        ]);
        let source = run(&a, Kind::TimeToSec, &[4], &Control::default()).unwrap();
        assert_eq!(source.definitions.as_ref(), [ExprId::new(0)]);
        assert_eq!(
            source.facts.cast_chain(),
            [
                TemporalCastKind::TimeFromDatetime,
                TemporalCastKind::Ordinary
            ]
        );
        assert!(matches!(
            source.facts,
            TemporalSourceFacts::SecondsRoundtrip { .. }
        ));
    }
}
#[test]
fn physical_value_conversion_call_is_not_an_ordinary_cast_or_sec_roundtrip() {
    let a = definitions(vec![
        leaf(1, DataType::Int64),
        call(2, "builtin.scalar/cast_text/v1", 1, DataType::Utf8),
    ]);
    let source = run(&a, Kind::TimeToSec, &[2], &Control::default()).unwrap();
    assert!(matches!(
        source.facts,
        TemporalSourceFacts::SecondsDirect { .. }
    ));
    assert!(source.facts.cast_chain().is_empty());
    assert_eq!(source.definitions.as_ref(), [ExprId::new(1)]);
}
#[test]
fn physical_source_author_refuses_wrong_arity_and_missing_definitions_without_defaults() {
    let a = definitions(vec![leaf(1, ts())]);
    for args in [vec![], vec![1, 1], vec![999]] {
        assert!(run(&a, Kind::TimeToSec, &args, &Control::default()).is_err());
    }
}
#[test]
fn physical_source_author_preserves_three_typed_compile_causes_at_every_callback() {
    let a = definitions(vec![
        leaf(1, DataType::Int64),
        call(2, "builtin.scalar/sec_to_time/v1", 1, DataType::Utf8),
        cast(3, 2, ts()),
    ]);
    let c = Control::default();
    run(&a, Kind::TimeToSec, &[3], &c).unwrap();
    let trace = c.trace.lock().unwrap().clone();
    assert!(trace.len() > 3);
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control {
                trace: Mutex::default(),
                refuse: Some((at, cause.clone())),
            };
            assert!(
                matches!(run(&a,Kind::TimeToSec,&[3],&c),Err(SourceError::Control(actual)) if actual==cause)
            );
            assert_eq!(*c.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn original_unsealed_unit_loans_temporal_sources_before_control_construction() {
    use novarocks_expression_contract::{
        BoundExprSetBuilder, UnitLimits, temporal_source_definitions_from_lookup_observed,
    };
    let source = definitions(vec![
        leaf(1, DataType::Int64),
        call(2, "builtin.scalar/sec_to_time/v1", 1, DataType::Utf8),
        cast(3, 2, ts()),
    ]);
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: novarocks_type_contract::MAX_CONTROL_DEPTH,
    });
    for node in &source {
        let id = draft
            .add(node.lambda_scope, node.ty.clone(), node.kind.clone())
            .unwrap();
        assert_eq!(id, node.id);
    }
    assert!(draft.roots().is_empty());
    let run_draft = |control: &Control| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = temporal_source_definitions_from_lookup_observed(
            Kind::TimeToSec,
            |id| draft.definition(id),
            &[ExprId::new(2)],
            &mut work,
        );
        if matches!(result, Err(SourceError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let original = Control::default();
    let expected = run(&source, Kind::TimeToSec, &[3], &original).unwrap();
    let baseline = Control::default();
    let actual = run_draft(&baseline).unwrap();
    assert_eq!(actual.facts, expected.facts);
    assert_eq!(actual.definitions, expected.definitions);
    assert_eq!(actual.definitions.as_ref(), &[ExprId::new(0)]);
    assert_eq!(
        *baseline.trace.lock().unwrap(),
        *original.trace.lock().unwrap()
    );
    let trace = baseline.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::default(),
                refuse: Some((at, cause.clone())),
            };
            assert!(
                matches!(run_draft(&control), Err(SourceError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

fn assert_immediate_source_trace<T: std::fmt::Debug + PartialEq>(
    original: impl Fn(&Control) -> Result<T, SourceError>,
    borrowed: impl Fn(&Control) -> Result<T, SourceError>,
    expected: T,
) {
    let control = Control::default();
    assert_eq!(original(&control).unwrap(), expected);
    let trace = control.trace.lock().unwrap().clone();
    let control = Control::default();
    assert_eq!(borrowed(&control).unwrap(), expected);
    assert_eq!(*control.trace.lock().unwrap(), trace);
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for run in [
                &original as &dyn Fn(&Control) -> Result<T, SourceError>,
                &borrowed,
            ] {
                let control = Control {
                    trace: Mutex::default(),
                    refuse: Some((at, cause.clone())),
                };
                assert!(
                    matches!(run(&control), Err(SourceError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn unsealed_regexp_source_keeps_checked_literal_dynamic_and_original_control() {
    use novarocks_expression_contract::*;
    use novarocks_type_contract::RegexpCountPatternSource;
    let ty = ValueType::new(DataType::Utf8, true);
    let (_pools, reference) = super::support::null_source(&ty);
    let mut draft = BoundExprSetBuilder::new(super::support::limits());
    let text = draft
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(0)))
        .unwrap();
    let literal = draft
        .add(None, ty.clone(), ExprKind::Constant(reference))
        .unwrap();
    let dynamic = draft
        .add(None, ty, ExprKind::Value(ValueId::new(1)))
        .unwrap();
    let stored: Vec<_> = draft.definitions().cloned().collect();
    for (pattern, expected) in [
        (
            literal,
            RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent,
        ),
        (dynamic, RegexpCountPatternSource::Dynamic),
    ] {
        let args = [text, pattern];
        let run = |borrowed: bool, control: &Control| {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
            let result = if borrowed {
                regexp_count_pattern_source_from_lookup_observed(
                    |id| draft.definition(id),
                    &args,
                    &mut work,
                )
            } else {
                regexp_count_pattern_source_observed(&stored, &args, &mut work)
            };
            work.finish_result(result, SourceError::Control)
        };
        assert_immediate_source_trace(
            |control| run(false, control),
            |control| run(true, control),
            expected,
        );
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        regexp_count_pattern_source_from_lookup_observed(
            |_| panic!("invalid arity cannot borrow a definition"),
            &[],
            &mut work,
        ),
        Err(SourceError::Invalid(
            "regexp_count source requires two arguments"
        ))
    ));
    assert!(matches!(
        regexp_count_pattern_source_from_lookup_observed(|_| None, &[text, literal], &mut work,),
        Err(SourceError::Invalid(
            "regexp_count pattern definition is absent"
        ))
    ));
}

#[test]
fn unsealed_base64_source_keeps_actual_immediate_identity_and_original_control() {
    use novarocks_expression_contract::*;
    use novarocks_type_contract::{FunctionArgumentType, ToBase64ByteSource};
    let ty = ValueType::new(DataType::Utf8, true);
    let mut draft = BoundExprSetBuilder::new(super::support::limits());
    let value = draft
        .add(None, ty.clone(), ExprKind::Value(ValueId::new(0)))
        .unwrap();
    let decoded = draft
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: super::support::function(
                    "builtin.scalar/from_base64/v1",
                    vec![FunctionArgumentType::Value(ty.clone())],
                    ty,
                ),
                args: Box::from([value]),
            },
        )
        .unwrap();
    let stored: Vec<_> = draft.definitions().cloned().collect();
    for (argument, expected) in [
        (value, ToBase64ByteSource::Ordinary),
        (decoded, ToBase64ByteSource::NativeV1EncryptionLatin1),
    ] {
        let args = [argument];
        let run = |borrowed: bool, control: &Control| {
            let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
            let result = if borrowed {
                to_base64_byte_source_from_lookup_observed(
                    |id| draft.definition(id),
                    &args,
                    &mut work,
                )
            } else {
                to_base64_byte_source_observed(&stored, &args, &mut work)
            };
            work.finish_result(result, SourceError::Control)
        };
        assert_immediate_source_trace(
            |control| run(false, control),
            |control| run(true, control),
            expected,
        );
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        to_base64_byte_source_from_lookup_observed(
            |_| panic!("invalid arity cannot borrow a definition"),
            &[],
            &mut work,
        ),
        Err(SourceError::Invalid(
            "to_base64 source requires one argument"
        ))
    ));
    assert!(matches!(
        to_base64_byte_source_from_lookup_observed(|_| None, &[decoded], &mut work,),
        Err(SourceError::Invalid(
            "to_base64 source definition is absent"
        ))
    ));
}

#[test]
fn regexp_count_shared_native_projection_distinguishes_constant_from_value_without_cv() {
    let constant = ExprKind::Constant(novarocks_constant_contract::ConstantReference {
        pool: novarocks_constant_contract::ConstantPoolId::new(71),
        ordinal: 2,
    });
    assert_eq!(
        novarocks_expression_contract::native_v1_emitted_constant_reference(&constant),
        Some(novarocks_constant_contract::ConstantReference {
            pool: novarocks_constant_contract::ConstantPoolId::new(71),
            ordinal: 2
        })
    );

    assert_eq!(
        novarocks_expression_contract::native_v1_emitted_constant_reference(&ExprKind::Value(
            ValueId::new(91)
        )),
        None
    );
    use novarocks_type_contract::RegexpCountPatternSource;
    assert_ne!(
        RegexpCountPatternSource::Dynamic,
        RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent,
    );
}

#[test]
fn to_base64_actual_native_identity_projection_matches_independent_source_table() {
    use novarocks_type_contract::ToBase64ByteSource as Source;

    // The frozen native-v1 source contract canonicalizes wire names and
    // aliases case-insensitively before testing the remaining encryption tags.
    // Expected facts are pinned independently; do not call a registry lookup
    // (or the classifier being tested) to calculate their own expected value.
    for (name, expected) in [
        ("aes_encrypt", Source::Ordinary),
        ("AES_ENCRYPT", Source::Ordinary),
        ("from_base64", Source::NativeV1EncryptionLatin1),
        ("FROM_BASE64", Source::NativeV1EncryptionLatin1),
        ("base64_decode_binary", Source::NativeV1EncryptionLatin1),
        ("BASE64_DECODE_STRING", Source::NativeV1EncryptionLatin1),
        ("to_binary", Source::NativeV1EncryptionLatin1),
        ("TO_BINARY", Source::NativeV1EncryptionLatin1),
        ("concat", Source::Ordinary),
        ("md5", Source::Ordinary),
        ("upper", Source::Ordinary),
    ] {
        let id = FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap();
        let emitted = novarocks_expression_contract::native_v1_function_name(&id).unwrap();
        assert_eq!(emitted, name);
        assert_eq!(
            Source::from_immediate_native_function_name(Some(emitted)),
            expected,
            "the complete source fact follows the actual emitted native name"
        );
        assert_eq!(
            Source::from_immediate_native_function_name(Some(emitted)).prefers_latin1(),
            matches!(expected, Source::NativeV1EncryptionLatin1),
            "the source fact also retains its byte interpretation"
        );
    }
    for raw_tag in [
        "AES_ENCRYPT",
        "base64_decode_binary",
        "base64_decode_string",
    ] {
        assert_eq!(
            Source::from_immediate_encryption_identity(Some(raw_tag)),
            Source::Ordinary,
            "a raw encryption identity does not resolve a wire alias"
        );
    }
}

/// Exercise native-v1 immediate-source projection from already emitted definitions.
#[test]
fn to_base64_physical_author_exact_declared_carriers_and_immediate_definitions() {
    use novarocks_type_contract::{FunctionArgumentType, ToBase64ByteSource as Source};

    let utf8 = ValueType::new(DataType::Utf8, true);
    let leaf = |id, kind, ty| novarocks_expression_contract::ExprNode {
        id: ExprId::new(id),
        lambda_scope: None,
        ty,
        kind,
    };
    let classify = |nodes: Vec<novarocks_expression_contract::ExprNode>, args: &[ExprId]| {
        let mut draft =
            novarocks_expression_contract::BoundExprSetBuilder::new(super::support::limits());
        for node in nodes {
            let id = draft.add(node.lambda_scope, node.ty, node.kind).unwrap();
            assert_eq!(id, node.id);
        }
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let result = novarocks_expression_contract::to_base64_byte_source_from_lookup_observed(
            |id| draft.definition(id),
            args,
            &mut work,
        );
        work.finish().unwrap();
        result
    };
    for kind in [
        ExprKind::Value(ValueId::new(91)),
        ExprKind::Constant(novarocks_constant_contract::ConstantReference {
            pool: novarocks_constant_contract::ConstantPoolId::new(2),
            ordinal: 7,
        }),
    ] {
        assert_eq!(
            classify(vec![leaf(0, kind, utf8.clone())], &[ExprId::new(0)]).unwrap(),
            Source::Ordinary
        );
    }

    assert!(matches!(
        classify(
            vec![leaf(0, ExprKind::Value(ValueId::new(91)), utf8.clone())],
            &[]
        ),
        Err(
            novarocks_expression_contract::TemporalSourceProjectionError::Invalid(
                "to_base64 source requires one argument"
            )
        )
    ));
    assert!(matches!(
        classify(
            vec![leaf(0, ExprKind::Value(ValueId::new(91)), utf8.clone())],
            &[ExprId::new(8)]
        ),
        Err(
            novarocks_expression_contract::TemporalSourceProjectionError::Invalid(
                "to_base64 source definition is absent"
            )
        )
    ));
    // Pin the emitted carrier and exact native owner ID for each source.
    // The immediate-source projection consumes these facts from the emitted nodes.
    for (name, dtype, expected) in [
        (
            "from_base64",
            DataType::Utf8,
            Source::NativeV1EncryptionLatin1,
        ),
        (
            "to_binary",
            DataType::Binary,
            Source::NativeV1EncryptionLatin1,
        ),
        ("concat", DataType::Utf8, Source::Ordinary),
    ] {
        let result = ValueType::new(dtype, true);
        let function = super::support::function(
            &format!("builtin.scalar/{name}/v1"),
            vec![FunctionArgumentType::Value(utf8.clone())],
            result.clone(),
        );
        let input = ExprId::new(0);
        let call = ExprId::new(1);
        let mut nodes = vec![
            leaf(input.get(), ExprKind::Value(ValueId::new(91)), utf8.clone()),
            leaf(
                call.get(),
                ExprKind::FunctionCall {
                    function,
                    args: vec![input],
                },
                result.clone(),
            ),
        ];
        let direct = classify(nodes.clone(), &[call]);
        if name == "to_binary" {
            assert!(
                matches!(
                    direct,
                    Err(
                        novarocks_expression_contract::TemporalSourceProjectionError::Invalid(
                            "to_base64 emitted source is not exact Physical Utf8"
                        )
                    )
                ),
                "the actual Binary declaration is rejected before runtime NULL handling"
            );
        } else {
            assert_eq!(direct.unwrap(), expected);
        }
        let cast = ExprId::new(2);
        nodes.push(leaf(
            cast.get(),
            ExprKind::Cast {
                array_failure_mode: None,
                expr: call,
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                },
            },
            utf8.clone(),
        ));
        assert_eq!(
            classify(nodes, &[cast]).unwrap(),
            Source::Ordinary,
            "CAST is the immediate emitted node, regardless of child identity"
        );
    }
}
