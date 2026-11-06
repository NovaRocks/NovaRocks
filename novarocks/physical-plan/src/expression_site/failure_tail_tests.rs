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
use std::fmt::Debug;

struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Trace {
    fn new(refusal: Option<(usize, CompileControlError)>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn calls(&self) -> Vec<(CompilePhase, u32)> {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push((phase, work));
        match self.refusal.clone() {
            Some((at, cause)) if at == index => Err(cause),
            _ => Ok(()),
        }
    }
}

fn every_callback<T: Debug, E: Debug + PartialEq>(
    mut run: impl FnMut(&Trace) -> Result<T, E>,
    expected_error: Option<E>,
    control_error: impl Fn(CompileControlError) -> E,
    quantum: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Trace::new(None);
    let result = run(&baseline);
    match expected_error {
        Some(expected) => assert_eq!(result.unwrap_err(), expected),
        None => {
            result.unwrap();
        }
    }
    let calls = baseline.calls();
    assert_eq!(calls.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(calls.len() >= 2, "entry alone is not a completed tail");
    assert!(calls.iter().all(|(phase, work)| {
        *phase == CompilePhase::Validate
            && *work <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK
    }));
    if quantum {
        assert!(calls.contains(&(
            CompilePhase::Validate,
            novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK,
        )));
    }
    for at in 0..calls.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusing = Trace::new(Some((at, cause.clone())));
            assert_eq!(
                run(&refusing).unwrap_err(),
                control_error(cause.clone()),
                "callback {at}, cause {cause:?}"
            );
            assert_eq!(
                refusing.calls(),
                calls[..=at],
                "the original refusal must return without tail replay"
            );
        }
    }
    calls
}

fn checked_values(rows: usize) -> Fragment {
    let mut builder = FragmentBuilder::new(FragmentId::new(27));
    let owner = builder.reserve_node_id().unwrap();
    let mut cells = Vec::new();
    let mut output = Vec::new();
    for (column, value) in [true, false].into_iter().enumerate() {
        cells.push(
            builder
                .add_expression(
                    owner,
                    ty(DataType::Boolean, false),
                    ExprKind::Literal(LiteralValue::Boolean(value)),
                )
                .unwrap(),
        );
        output.push(
            builder
                .add_value(
                    ty(DataType::Boolean, false),
                    ValueOrigin::NodeOutput {
                        node: owner,
                        output_ordinal: u32::try_from(column).unwrap(),
                    },
                )
                .unwrap(),
        );
    }
    builder
        .add_values(
            owner,
            (0..rows)
                .map(|_| cells.clone().into_boxed_slice())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            output.into_boxed_slice(),
        )
        .unwrap();
    builder
        .finish_definition(
            owner,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
}

fn snapshot(
    source: &Fragment,
    expressions: ExprArena,
    nodes: BTreeMap<NodeId, PhysicalNode>,
) -> Fragment {
    Fragment::from(FragmentParts {
        summaries: Box::default(),
        stage_placement: crate::StagePlacementPolicy::Singleton,
        id: source.id(),
        root: source.root(),
        values: source.values().clone(),
        expressions,
        nodes,
        sink: source.sink().clone(),
        dop_domain: source.dop_domain(),
        runtime_filters: source.runtime_filters().into(),
        call_requests: source.call_requests().clone(),
    })
}

// Negative snapshots deliberately change one public field after the genuine
// builder has validated the baseline. They claim no checked root/use authority.
fn missing_last_cell(source: &Fragment) -> Fragment {
    let mut nodes = source.nodes().clone();
    let NodeKind::Values { rows } = &mut nodes.get_mut(&source.root()).unwrap().kind else {
        panic!("the checked fixture must be Values");
    };
    rows.last_mut().unwrap()[1] = ExprId::new(u32::MAX);
    snapshot(source, source.expressions().clone(), nodes)
}
fn scoped_second_cell(source: &Fragment) -> Fragment {
    let mut expressions = source.expressions().clone();
    let mut second = expressions.get(ExprId::new(1)).unwrap().clone();
    second.lambda_scope = Some(ExprId::new(0));
    expressions.insert(second);
    snapshot(source, expressions, source.nodes().clone())
}
fn second_cell_becomes_is_null(source: &Fragment) -> Fragment {
    let mut expressions = source.expressions().clone();
    let mut second = expressions.get(ExprId::new(1)).unwrap().clone();
    second.kind = ExprKind::IsNull {
        expr: ExprId::new(0),
        negated: false,
    };
    expressions.insert(second);
    let changed = snapshot(source, expressions, source.nodes().clone());
    // This is a valid new definition graph, but an old flow cannot be reused
    // merely because its fragment, root, definition and demand IDs still match.
    crate::validation::validate_fragment_definition(&changed).unwrap();
    changed
}

type Bindings = Vec<(ExpressionRootSite, ExpressionUseId)>;
fn checked_flow(source: &Fragment) -> (ExpressionControlFlow<ExprId>, Bindings) {
    let roots = PhysicalExpressionRoots::try_new(source, &control()).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut bindings = Vec::new();
    let mut invocations = Vec::new();
    for (index, (site, root)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(u32::try_from(index).unwrap());
        bindings.push((*site, use_id));
        invocations.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        source.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    (flow, bindings)
}

fn forged_second_literal_argument(source: &Fragment) -> ExpressionControlFlow<ExprId> {
    let domain = EvaluationDomainId::new(0);
    let invocation = |id, definition, arguments| ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            domain,
            demand: EvaluationDemand::Value,
        },
        definition,
        control: ControlShape::Eager,
        arguments,
    };
    // The flow itself is a valid acyclic tree. Its second root's child is
    // nevertheless absent from the actual literal definition.
    ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            invocation(0, ExprId::new(0), Box::default()),
            invocation(1, ExprId::new(1), Box::from([ExpressionUseId::new(2)])),
            invocation(2, ExprId::new(0), Box::default()),
        ],
        source.expressions(),
        CompilePhase::Validate,
        &control(),
    )
    .unwrap()
}

#[test]
fn actual_values_roots_observe_success_and_missing_scope_failure_tails() {
    let valid = checked_values(1);
    assert_eq!(
        PhysicalExpressionRoots::try_new(&valid, &control())
            .unwrap()
            .sites()
            .len(),
        2
    );
    every_callback(
        |control| PhysicalExpressionRoots::try_new(&valid, control),
        None,
        ExpressionRootError::Control,
        false,
    );
    for (invalid, expected) in [
        (
            missing_last_cell(&valid),
            ExpressionRootError::InvalidExpressionOwner,
        ),
        (
            scoped_second_cell(&valid),
            ExpressionRootError::InvalidExpressionScope,
        ),
    ] {
        let calls = every_callback(
            |control| PhysicalExpressionRoots::try_new(&invalid, control),
            Some(expected),
            ExpressionRootError::Control,
            false,
        );
        assert!(
            calls.last().unwrap().1 > 0,
            "the first valid cell leaves actual pending work"
        );
    }
}

#[test]
fn actual_root_bindings_observe_second_site_and_correspondence_failure_tails() {
    let source = checked_values(1);
    let (flow, bindings) = checked_flow(&source);
    every_callback(
        |control| PhysicalRootUses::try_new(&source, flow.clone(), bindings.clone(), control),
        None,
        RootUseBindingError::Control,
        false,
    );
    let mut bad_bindings = bindings.clone();
    bad_bindings[1].0.role = ExpressionRootRole::ProjectOutput { expression: 1 };
    let calls = every_callback(
        |control| PhysicalRootUses::try_new(&source, flow.clone(), bad_bindings.clone(), control),
        Some(RootUseBindingError::InvalidSite),
        RootUseBindingError::Control,
        false,
    );
    assert!(calls.last().unwrap().1 > 0);
    let forged = forged_second_literal_argument(&source);
    every_callback(
        |control| PhysicalRootUses::try_new(&source, forged.clone(), bindings.clone(), control),
        Some(RootUseBindingError::WrongArguments),
        RootUseBindingError::Control,
        false,
    );
}

#[test]
fn actual_fragment_recheck_observes_changed_roots_and_wrong_arguments_tails() {
    let source = checked_values(1);
    let (flow, bindings) = checked_flow(&source);
    let bound = PhysicalRootUses::try_new(&source, flow, bindings, &control()).unwrap();
    every_callback(
        |control| bound.validate_fragment(&source, control),
        None,
        RootUseBindingError::Control,
        false,
    );
    let changed = checked_values(2);
    every_callback(
        |control| bound.validate_fragment(&changed, control),
        Some(RootUseBindingError::ChangedRoots),
        RootUseBindingError::Control,
        false,
    );
    let changed = second_cell_becomes_is_null(&source);
    every_callback(
        |control| bound.validate_fragment(&changed, control),
        Some(RootUseBindingError::WrongArguments),
        RootUseBindingError::Control,
        false,
    );
    let scoped = scoped_second_cell(&source);
    every_callback(
        |control| bound.validate_fragment(&scoped, control),
        Some(RootUseBindingError::Roots(
            ExpressionRootError::InvalidExpressionScope,
        )),
        RootUseBindingError::Control,
        false,
    );
}

#[test]
fn actual_wide_values_cover_all_owner_quantums_and_ordinary_refusal_prefixes() {
    let source = checked_values(320);
    let (flow, bindings) = checked_flow(&source);
    assert_eq!(bindings.len(), 640);
    let bound =
        PhysicalRootUses::try_new(&source, flow.clone(), bindings.clone(), &control()).unwrap();
    every_callback(
        |control| PhysicalExpressionRoots::try_new(&source, control),
        None,
        ExpressionRootError::Control,
        true,
    );
    every_callback(
        |control| PhysicalRootUses::try_new(&source, flow.clone(), bindings.clone(), control),
        None,
        RootUseBindingError::Control,
        true,
    );
    every_callback(
        |control| bound.validate_fragment(&source, control),
        None,
        RootUseBindingError::Control,
        true,
    );
    let missing = missing_last_cell(&source);
    every_callback(
        |control| PhysicalExpressionRoots::try_new(&missing, control),
        Some(ExpressionRootError::InvalidExpressionOwner),
        ExpressionRootError::Control,
        true,
    );
    let mut bad_bindings = bindings.clone();
    bad_bindings.last_mut().unwrap().0.role = ExpressionRootRole::ProjectOutput { expression: 639 };
    every_callback(
        |control| PhysicalRootUses::try_new(&source, flow.clone(), bad_bindings.clone(), control),
        Some(RootUseBindingError::InvalidSite),
        RootUseBindingError::Control,
        true,
    );
}
