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

//! Nominal source wire facts are never inferred from missing fields.
use super::*;
use novarocks_type_contract::{
    CompilePhase, TemporalCastKind as C, TemporalSourceFacts as F, TemporalSourceOccurrence,
    TemporalSourcePlan,
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
fn plan(facts: F) -> TemporalSourcePlan<novarocks_expression_contract::ExprId> {
    let sources = facts
        .shape()
        .roles()
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(ordinal, role)| TemporalSourceOccurrence {
            role,
            use_id: ExpressionUseId::new(ordinal as u32 + 1),
            // Repeated definitions are legal; repeated occurrences are not.
            definition: novarocks_expression_contract::ExprId::new(97),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    TemporalSourcePlan { facts, sources }
}
fn encode(
    plan: &TemporalSourcePlan<novarocks_expression_contract::ExprId>,
    control: &Control,
) -> Result<wire::TemporalSourcePlan, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_temporal_source(plan, &mut Projection::plain(), &mut work)?;
    work.finish()?;
    Ok(result)
}
fn decode(
    plan: &wire::TemporalSourcePlan,
    control: &Control,
) -> Result<TemporalSourcePlan<novarocks_expression_contract::ExprId>, E> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_temporal_source(plan, &mut Projection::plain(), &mut work)?;
    work.finish()?;
    Ok(result)
}
#[test]
fn all_six_exact_source_facts_roundtrip_with_distinct_use_occurrences() {
    for facts in [
        F::FormatOrdinary,
        F::FormatUtf8Override,
        F::SecondsDirect {
            cast_chain: Box::default(),
        },
        F::SecondsCastString {
            cast_chain: Box::from([C::Ordinary]),
        },
        F::SecondsCastOther {
            cast_chain: Box::from([C::Time, C::TimeFromDatetime, C::Ordinary]),
        },
        F::SecondsRoundtrip {
            cast_chain: Box::default(),
        },
    ] {
        let original = plan(facts);
        let wire = encode(&original, &Control::default()).unwrap();
        assert_eq!(decode(&wire, &Control::default()).unwrap(), original);
    }
}
#[test]
fn missing_unknown_reordered_duplicate_and_overdepth_wire_facts_are_rejected() {
    let original = encode(
        &plan(F::SecondsCastOther {
            cast_chain: Box::from([C::Ordinary]),
        }),
        &Control::default(),
    )
    .unwrap();
    let mut cases = vec![];
    let mut dto = original.clone();
    dto.shape = 0;
    cases.push(dto);
    let mut dto = original.clone();
    dto.shape = i32::MAX;
    cases.push(dto);
    let mut dto = original.clone();
    dto.sources.clear();
    cases.push(dto);
    let mut dto = original.clone();
    dto.sources[0].role = 0;
    cases.push(dto);
    let mut dto = original.clone();
    dto.sources[0].use_id = None;
    cases.push(dto);
    let mut dto = original.clone();
    dto.sources[0].definition_id = None;
    cases.push(dto);
    let mut dto = original.clone();
    dto.sources[1].use_id = dto.sources[0].use_id;
    cases.push(dto);
    let mut dto = original.clone();
    dto.sources.swap(0, 1);
    cases.push(dto);
    let mut dto = original.clone();
    dto.cast_chain.clear();
    cases.push(dto);
    let mut dto = original.clone();
    dto.cast_chain[0] = 0;
    cases.push(dto);
    let mut dto = original.clone();
    dto.cast_chain = vec![
        wire::TemporalCastKind::Ordinary as i32;
        novarocks_type_contract::MAX_CONTROL_DEPTH + 1
    ];
    cases.push(dto);
    for dto in cases {
        assert!(decode(&dto, &Control::default()).is_err());
    }
}
#[test]
fn encode_and_decode_preserve_each_compile_cause_without_checkpoint_after_refusal() {
    let original = plan(F::SecondsCastOther {
        cast_chain: Box::from([C::Ordinary]),
    });
    let dto = encode(&original, &Control::default()).unwrap();
    for decoding in [false, true] {
        let recorder = Control::default();
        if decoding {
            decode(&dto, &recorder).unwrap();
        } else {
            encode(&original, &recorder).unwrap();
        }
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
                if decoding {
                    assert!(
                        matches!(decode(&dto,&control),Err(E::Control(actual)) if actual==cause)
                    );
                } else {
                    assert!(
                        matches!(encode(&original,&control),Err(E::Control(actual)) if actual==cause)
                    );
                }
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
            }
        }
    }
}
