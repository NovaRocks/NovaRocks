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

fn invoke(
    f: &Fixture,
    control: &Control,
    caps: impl Into<Option<ExpressionProjectionLimits>>,
) -> Result<
    (
        ExpressionNamespaceWriteFacts,
        Vec<wire::ExpressionDefinition>,
    ),
    Error,
> {
    f.with_sources(|types, functions, aggregates, inputs| {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let mut observed = ExpressionNamespaceWriteFacts::default();
        let result = (|| {
            let token = prepare_expression_definitions_in(
                Some(ExpressionDefinitionSource::new(&f.definitions, &[])),
                inputs,
                types,
                functions,
                aggregates,
                &f.parameters,
                &f.pools,
                SOURCE,
                caps,
                &mut |facts| {
                    observed = *facts;
                    Ok(())
                },
                &mut work,
            )?;
            assert_eq!(*token.facts(), observed);
            let output = token.emit_in(
                &mut |facts| {
                    assert_eq!(*facts, observed);
                    Ok(())
                },
                &mut work,
            )?;
            Ok((observed, output.into_wire()))
        })();
        finish(work, result)
    })
}
#[test]
fn caller_owned_sender_preserves_all_scalar_payload_kinds_and_facts() {
    let f = all_kinds();
    let legacy = f.run(&Control::default(), SOURCE, limits(), true).unwrap();
    let actual = invoke(&f, &Control::default(), limits()).unwrap();
    assert_eq!(actual, legacy);
    assert_eq!(actual.1.len(), 16);
    assert_eq!(actual.1.last().unwrap().id, 15);
}
#[test]
fn input_derived_expression_sender_preserves_wire_and_admitted_geometry() {
    let f = all_kinds();
    let explicit = invoke(&f, &Control::default(), limits()).unwrap();
    assert_eq!(invoke(&f, &Control::default(), None).unwrap(), explicit);
}

#[test]
fn caller_owned_sender_seven_exact_axes_and_one_under_replay_actual_source() {
    let f = Fixture::single(ExprKind::input(ValueId::new(0)));
    let (facts, _) = invoke(&f, &Control::default(), limits()).unwrap();
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
    assert_eq!(invoke(&f, &Control::default(), exact).unwrap().0, facts);
    for axis in 0..7 {
        let mut under = exact;
        let v = match axis {
            0 => &mut under.max_definitions,
            1 => &mut under.max_type_references,
            2 => &mut under.max_expression_references,
            3 => &mut under.max_new_allocation_requests,
            4 => &mut under.max_new_allocation_request_bytes,
            5 => &mut under.max_coexisting_source_and_request_bytes,
            _ => &mut under.max_cumulative_work,
        };
        if *v == 0 {
            continue;
        }
        *v -= 1;
        assert!(
            matches!(
                invoke(&f, &Control::default(), under),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ),
            "axis {axis}"
        );
    }
}
#[test]
fn caller_owned_sender_known_root_request_refuses_before_pending_quantum_and_emit() {
    let f = Fixture::single(ExprKind::input(ValueId::new(0)));
    f.with_sources(|types, functions, aggregates, inputs| {
        for cause in CAUSES {
            let c = Control {
                trace: Mutex::new(vec![]),
                stop: Some((1, cause)),
            };
            let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
            for _ in 0..255 {
                w.step().unwrap();
            }
            let mut l = limits();
            l.max_new_allocation_request_bytes = 0;
            let result = prepare_expression_definitions_in(
                Some(ExpressionDefinitionSource::new(&f.definitions, &[])),
                inputs,
                types,
                functions,
                aggregates,
                &f.parameters,
                &f.pools,
                SOURCE,
                l,
                &mut |_| Ok(()),
                &mut w,
            );
            assert!(matches!(
                result,
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(*c.trace.lock().unwrap(), [0]);
        }
        let c = Control::default();
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        let token = prepare_expression_definitions_in(
            Some(ExpressionDefinitionSource::new(&f.definitions, &[])),
            inputs,
            types,
            functions,
            aggregates,
            &f.parameters,
            &f.pools,
            SOURCE,
            limits(),
            &mut |_| Ok(()),
            &mut w,
        )
        .unwrap();
        w.flush().unwrap();
        let before = c.trace.lock().unwrap().clone();
        let result = token.emit_in(&mut |_| Err(CompileControlError::ResourceExhausted), &mut w);
        assert!(matches!(
            result,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(*c.trace.lock().unwrap(), before);
    });
}
#[test]
fn caller_owned_sender_actual_success_and_ordinary_refusal_have_all_three_primary_prefixes() {
    let good = Fixture::single(ExprKind::input(ValueId::new(0)));
    prefixes(|c| invoke(&good, c, limits()), true);
    let mut bad = Fixture::single(ExprKind::input(ValueId::new(0)));
    bad.input[0].id = 7;
    prefixes(|c| invoke(&bad, c, limits()), false);
}

#[test]
fn caller_owned_literal_captured_comparer_preserves_original_facts_and_primary_refusals() {
    let good = Fixture::single(ExprKind::Constant(ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 1,
    }));
    let legacy = good
        .run(&Control::default(), SOURCE, limits(), true)
        .unwrap();
    assert_eq!(
        invoke(&good, &Control::default(), limits()).unwrap(),
        legacy
    );
    prefixes(|c| invoke(&good, c, limits()), true);
    let mut bad = Fixture::single(ExprKind::Constant(ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 1,
    }));
    bad.replace(0, |node| node.ty = node.ty.with_nullable(false).unwrap());
    bad.input[0].type_id = 7;
    prefixes(|c| invoke(&bad, c, limits()), false);

    // This private caller seam uses the actual admitted pool and expression
    // roots. It proves the sole comparer numerical gate before a pending
    // callback; the paired complete sender paths above prove source capture.
    let pool = good
        .pools
        .entries()
        .get(&ConstantPoolId::new(u32::MAX))
        .unwrap();
    let node = good.definitions.first().unwrap();
    for cause in CAUSES {
        let c = Control {
            trace: Mutex::new(vec![]),
            stop: Some((1, cause)),
        };
        let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Encode).unwrap();
        for _ in 0..255 {
            w.step().unwrap();
        }
        let mut caps = limits();
        caps.max_cumulative_work = 0;
        let mut admit = |_: &ExpressionNamespaceWriteFacts| Ok(());
        let mut admission = Admission {
            parent: Some(&mut admit),
            source: SOURCE,
            limits: Some(caps),
        };
        let result = compare_types(
            &node.ty,
            pool.value_type(),
            &mut ExpressionNamespaceWriteFacts::default(),
            &mut admission,
            &mut w,
        );
        assert!(matches!(
            result,
            Err(Error::Control(CompileControlError::ResourceExhausted))
        ));
        assert_eq!(*c.trace.lock().unwrap(), [0]);
    }
}
