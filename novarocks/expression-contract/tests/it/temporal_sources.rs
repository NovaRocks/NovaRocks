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
