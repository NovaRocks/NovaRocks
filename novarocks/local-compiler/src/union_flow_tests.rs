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

//! Synthesized reads share the checked unit compiler with actual node units.
use crate::test_units::{compiled_root, input_ordinal};
use crate::{
    FragmentCompileError,
    union_lowering_tests::{self as fixture, Case},
};
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramRootRef};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationDemand, MAX_CONTROL_USE_REFERENCES,
    PureCompileControl,
};
use std::{collections::BTreeSet, sync::Mutex};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
fn synthetic(program: &LocalProgram) -> Vec<Vec<ProgramRootRef>> {
    program
        .graph()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            ProgramNodeKind::Project {
                is_subordinate: true,
                exprs,
                ..
            } => Some(exprs.to_vec()),
            _ => None,
        })
        .collect()
}
#[test]
fn union_flow_sparse_max_ids_author_fresh_exact_root_contexts_and_channels() {
    let source = fixture::fixture(Case::ordinary());
    let program = fixture::compile(&source, &fixture::functions(), &Control::default()).unwrap();
    let roots = synthetic(&program);
    assert_eq!(roots.len(), 2);
    let mut contexts = BTreeSet::new();
    for branch in &roots {
        for (ordinal, &reference) in branch.iter().enumerate() {
            let (unit, root) = compiled_root(&program, reference);
            let invocation = &unit.control_flow().uses()[&root.use_id];
            assert_eq!(invocation.definition, root.definition);
            assert_eq!(root.demand, EvaluationDemand::Value);
            assert!(invocation.arguments.is_empty());
            let region = &unit.control_flow().regions()[&invocation.context.region];
            assert!(region.parent.is_none() && region.guard.is_none());
            assert!(contexts.insert((reference.unit, root.use_id)));
            assert_eq!(
                input_ordinal(&program, reference),
                if ordinal % 3 == 1 { 0 } else { 1 }
            );
        }
    }
    assert_eq!(contexts.len(), 6);
    assert_ne!(roots[0][0].unit, roots[1][0].unit);
    assert!(
        program
            .graph()
            .nodes()
            .iter()
            .any(|n| n.physical_sources().iter().any(|id| id.get() == u32::MAX))
    );
}
#[test]
fn union_flow_fresh_uses_skip_every_package_minted_identity() {
    // Package-wide use-ID minting retired. The full unit address now provides
    // separation even when both actual unit authors choose local occurrence 0.
    let source = fixture::fixture(Case::ordinary());
    let program = fixture::compile(&source, &fixture::functions(), &Control::default()).unwrap();
    let roots = synthetic(&program);
    let (left, a) = compiled_root(&program, roots[0][0]);
    let (right, b) = compiled_root(&program, roots[1][0]);
    assert_eq!(a.use_id, b.use_id);
    assert_eq!(a.use_id.get(), 0);
    assert_ne!(roots[0][0].unit, roots[1][0].unit);
    assert!(!std::ptr::eq(left, right));
    assert_eq!(
        source
            .package
            .fragment()
            .nodes()
            .values()
            .filter(|n| n.expressions.is_some())
            .count(),
        2
    );
    assert_eq!(program.units().len(), 4);
}
#[test]
fn union_flow_duplicate_project_sources_keep_ordered_independent_occurrences() {
    let source = fixture::fixture(Case::ordinary());
    let program = fixture::compile(&source, &fixture::functions(), &Control::default()).unwrap();
    for branch in synthetic(&program) {
        let (_, first) = compiled_root(&program, branch[0]);
        let (_, repeated) = compiled_root(&program, branch[2]);
        assert_ne!(first.use_id, repeated.use_id);
        assert_eq!(
            input_ordinal(&program, branch[0]),
            input_ordinal(&program, branch[2])
        );
        assert_eq!(input_ordinal(&program, branch[1]), 0);
        assert_eq!(
            branch.iter().map(|r| r.root.get()).collect::<Vec<_>>(),
            [0, 1, 2]
        );
    }
}
#[test]
fn union_flow_every_actual_small_control_prefix_preserves_all_three_first_causes() {
    let source = fixture::fixture(Case::ordinary());
    let functions = fixture::functions();
    let control = Control::default();
    fixture::compile(&source, &functions, &control).unwrap();
    let baseline = control.trace.into_inner().unwrap();
    assert_eq!(baseline[0], (CompilePhase::LowerProgram, 0));
    assert!(baseline.last().unwrap().1 > 0);
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(fixture::compile(&source,&functions,&control),Err(FragmentCompileError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), baseline[..=stop]);
        }
    }
}
#[test]
fn union_flow_reference_ceiling_refuses_before_reserve_or_output_publication() {
    // Four real source roots coexist with both normalization branches. No raw
    // malformed flow or duplicate numeric use is needed to exceed the sum.
    let width = MAX_CONTROL_USE_REFERENCES / 2;
    let source = fixture::fixture_width(width);
    let original = source.package.clone();
    let control = Control::default();
    assert!(matches!(
        fixture::compile(&source, &fixture::functions(), &control),
        Err(FragmentCompileError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert!(std::sync::Arc::ptr_eq(&source.package, &original));
    assert_eq!(source.package.fragment().nodes().len(), 3);
}
