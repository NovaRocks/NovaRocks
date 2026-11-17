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
//! Original FE emitter/journal authors precise temporal source occurrences.
use super::contract_lowering::lowered_scalar_source_tests::{authored, policy, text};
use super::expression_occurrences::{
    ExpressionOccurrenceError, author_physical_occurrences_from_journal_observed,
    author_physical_occurrences_observed,
};
use crate::{
    analysis::{ExprKind, LiteralValue, TypedExpr},
    binding::SqlFunctionBinding,
    compiler::SqlAuthoredPhysicalPlan,
};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::FunctionResultType;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
    PureCompileControl, TemporalSourceRole as R, TemporalSourceShape as S,
};
use std::sync::Mutex;
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        match self.refuse.clone() {
            Some((at, cause)) if at == trace.len() => Err(cause),
            _ => Ok(()),
        }
    }
}
fn call(name: &str, args: Vec<TypedExpr>) -> TypedExpr {
    let control = Control::default();
    let arguments = args
        .iter()
        .map(|arg| crate::analysis::function_argument(arg, policy(), &control).unwrap())
        .collect::<Vec<_>>();
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_scalar_binding(name, &arguments, &control)
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("scalar result")
    };
    let value_type = result.clone();
    let volatility = resolved.semantics.volatility;
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: name.into(),
            args: (args).into(),
            distinct: false,
            binding: SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::OutputNull),
            volatility,
        },
        value_type,
    }
}
fn cast(input: TypedExpr, target: DataType) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Cast {
            array_failure_mode: None,
            expr: novarocks_parser::ast::AstBox::new(input),
            target: target.clone(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        value_type: FunctionValueType::new(target, true),
    }
}
fn author_shapes(owner: &SqlAuthoredPhysicalPlan) -> Vec<S> {
    let mut shapes = vec![];
    for fragment in owner.plan().fragments().values() {
        let authored =
            author_physical_occurrences_from_journal_observed(owner, fragment, &Control::default())
                .unwrap();
        for source in authored.temporal_sources.values() {
            source.validate_structure().unwrap();
            shapes.push(source.facts.shape());
            let definition = source.sources[0].definition;
            for occurrence in &source.sources {
                let invocation = authored
                    .root_uses
                    .flow()
                    .uses()
                    .get(&occurrence.use_id)
                    .unwrap();
                assert_eq!(invocation.definition, occurrence.definition);
            }
            if source.facts.shape() == S::SecondsCastOther {
                assert_eq!(source.sources[1].role, R::ImmediateCastSource);
                assert_eq!(source.sources[2].role, R::DeepestCastSource);
                if source.sources[1].definition == source.sources[2].definition {
                    assert_ne!(source.sources[1].use_id, source.sources[2].use_id);
                }
            }
            assert!(fragment.expressions().get(definition).is_some());
        }
    }
    shapes
}
#[test]
fn emitted_utf8_cast_override_retains_raw_before_normal_and_format() {
    let owner = authored(call(
        "time_format",
        vec![
            cast(
                text("12:34:56"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            text("%f"),
        ],
    ));
    assert_eq!(author_shapes(&owner), vec![S::FormatUtf8Override]);
    for fragment in owner.plan().fragments().values() {
        let authored = author_physical_occurrences_from_journal_observed(
            &owner,
            fragment,
            &Control::default(),
        )
        .unwrap();
        for source in authored.temporal_sources.values() {
            assert_eq!(
                source.sources.iter().map(|s| s.role).collect::<Vec<_>>(),
                vec![R::RawOverride, R::Normal, R::Format]
            );
            assert_ne!(source.sources[0].use_id, source.sources[1].use_id);
        }
    }
}
#[test]
fn canonicalized_equal_type_cast_does_not_resurrect_source_provenance() {
    let owner = authored(call(
        "time_format",
        vec![
            {
                let source = text("12:34:56");
                let source_type = source.value_type.clone();
                let mut equal_cast = cast(source, DataType::Utf8);
                equal_cast.value_type = source_type;
                equal_cast
            },
            text("%f"),
        ],
    ));
    assert_eq!(author_shapes(&owner), vec![S::FormatOrdinary]);
}
#[test]
fn original_seconds_roundtrip_authors_only_original_integer_use() {
    let original = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(-12)),
        value_type: FunctionValueType::new(DataType::Int64, false),
    };
    let transformed = call("sec_to_time", vec![original]);
    let owner = authored(call(
        "time_to_sec",
        vec![cast(
            transformed,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        )],
    ));
    assert_eq!(author_shapes(&owner), vec![S::SecondsRoundtrip]);
    for fragment in owner.plan().fragments().values() {
        let occurrences = author_physical_occurrences_from_journal_observed(
            &owner,
            fragment,
            &Control::default(),
        )
        .unwrap();
        for source in occurrences.temporal_sources.values() {
            assert_eq!(source.sources.len(), 1);
            assert_eq!(source.sources[0].role, R::OriginalSeconds);
            assert_eq!(
                fragment
                    .expressions()
                    .get(source.sources[0].definition)
                    .unwrap()
                    .ty
                    .data_type()
                    .clone(),
                DataType::Int64
            );
        }
    }
}
#[test]
fn ordinary_occurrence_entry_without_original_journal_cannot_guess_temporal_facts() {
    let owner = authored(call("time_to_sec", vec![text("12:34:56")]));
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        if fragment.expressions().iter().any(|(_,node)|matches!(&node.kind,novarocks_physical_plan::ExprKind::FunctionCall{function,..} if function.function_id.as_str().contains("time_to_sec"))){
   assert!(author_physical_occurrences_observed(fragment,owner.function_catalog().as_ref(),&Control::default()).is_err());count+=1;
  }
    }
    assert_eq!(count, 1);
}
#[test]
fn actual_emitted_journal_author_preserves_each_compile_cause_at_every_checkpoint() {
    let owner = authored(call(
        "time_format",
        vec![
            cast(
                text("12:34:56"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            text("%f"),
        ],
    ));
    for fragment in owner.plan().fragments().values() {
        let recorder = Control::default();
        author_physical_occurrences_from_journal_observed(&owner, fragment, &recorder).unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        for at in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refuse: Some((at, cause.clone())),
                };
                assert!(
                    matches!(author_physical_occurrences_from_journal_observed(&owner,fragment,&control),Err(ExpressionOccurrenceError::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
}
