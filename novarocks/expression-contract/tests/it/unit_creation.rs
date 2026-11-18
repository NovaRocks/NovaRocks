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

//! Public receiving validation funds its actual scratch and escaping errors.
//! Fixed graph, lexical, call and temporal examples protect the original unit
//! owner. The recording caller refuses each actual slot; it grants no product
//! capacity and neither estimates SDK internals nor supplies another budget.
use super::support::*;
use arrow_schema::{DataType, Field};
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureFact, FailureProjection};
use novarocks_type_contract::*;
use std::{
    alloc::Layout,
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScopeKind {
    Temporary,
    Retained,
    Single,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Request {
    Exact(usize, usize),
    Opaque(usize),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Event {
    scope: ScopeKind,
    request: Request,
    completed_checkpoints: usize,
}
#[derive(Debug, Eq, PartialEq)]
struct HostRefusal(usize);
impl fmt::Display for HostRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "original creator refusal at {}", self.0)
    }
}
impl std::error::Error for HostRefusal {}
impl FailureProjection for HostRefusal {
    fn failure_fact(&self) -> FailureFact {
        CompileControlError::DeadlineExceeded.failure_fact()
    }
}
struct Hold;
impl ValueTypeStorageHold for Hold {}
struct Scope {
    kind: ScopeKind,
    events: Arc<Mutex<Vec<Event>>>,
    refuse: Option<usize>,
    storage: ValueTypeStorageLifetime,
    control: Option<Arc<Control>>,
}
impl Scope {
    fn new(
        kind: ScopeKind,
        events: Arc<Mutex<Vec<Event>>>,
        refuse: Option<usize>,
    ) -> (Self, Weak<Hold>) {
        let hold = Arc::new(Hold);
        let weak = Arc::downgrade(&hold);
        (
            Self {
                kind,
                events,
                refuse,
                storage: ValueTypeStorageLifetime::held(hold),
                control: None,
            },
            weak,
        )
    }
    fn observe(&mut self, request: Request) -> Result<(), HostRefusal> {
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        if let Some(stop) = self.refuse {
            assert!(index <= stop, "creator callback after original refusal");
        }
        events.push(Event {
            scope: self.kind,
            request,
            completed_checkpoints: self
                .control
                .as_ref()
                .map_or(0, |control| control.trace().len()),
        });
        if self.refuse == Some(index) {
            Err(HostRefusal(index))
        } else {
            Ok(())
        }
    }
}
impl ValueSchemaCreationScope<HostRefusal> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), HostRefusal> {
        assert_ne!(layout.size(), 0, "no zero-sized allocation fact");
        self.observe(Request::Exact(layout.size(), layout.align()))
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), HostRefusal> {
        assert_ne!(bytes, 0, "no zero-sized opaque fact");
        self.observe(Request::Opaque(bytes))
    }
}
fn parts(unit: &BoundExprSet) -> UnitParts {
    // These are explicit pure fixture input headers, constructed before the
    // receiving operation. Existing immutable types/control keep their loans.
    UnitParts::new(
        unit.ports().into(),
        unit.definitions().into(),
        unit.roots().into(),
        unit.root_use_ids().into(),
        unit.control_flow().clone(),
        unit.claims().clone(),
        unit.requests().clone(),
    )
}
fn read(
    parts: UnitParts,
    temporary: &mut Scope,
    retained: &mut Scope,
    control: &Control,
) -> Result<BoundExprSet, UnitConstructionFailure<HostRefusal>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)
        .map_err(UnitConstructionFailure::Control)?;
    let unit = BoundExprSet::try_from_parts_with_creator_in(
        parts,
        limits(),
        temporary,
        retained,
        &mut work,
    )?;
    work.finish().map_err(UnitConstructionFailure::Control)?;
    Ok(unit)
}
fn graph_input() -> (BoundExprSetBuilder, Vec<ExprPort>) {
    let mut b = BoundExprSetBuilder::new(limits());
    let mut ports = Vec::new();
    let mut leaves = Vec::new();
    for side in 0..2 {
        let mut values = Vec::new();
        for ordinal in 0..12 {
            let value = ValueId::new(side * 12 + ordinal);
            values.push((value, integer(false)));
            leaves.push(b.add(None, integer(false), ExprKind::Value(value)).unwrap());
        }
        ports.push(ExprPort {
            values: values.into_boxed_slice(),
        });
    }
    let mut root = leaves[0];
    for leaf in leaves.into_iter().skip(1) {
        root = b
            .add(
                None,
                integer(false),
                ExprKind::Binary {
                    left: root,
                    right: leaf,
                    op: BinaryOperator::Add,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: Some(SemanticParameterRef {
                        id: SemanticParameterId::new(0),
                        expected_key: SemanticParameterKey::AllowThrowException,
                    }),
                },
            )
            .unwrap();
    }
    for _ in 0..3 {
        b.root(root, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    (b, ports)
}
fn graph() -> BoundExprSet {
    let (builder, ports) = graph_input();
    no_calls(builder, ports, &Control::good()).unwrap()
}
fn call_unit(environment: bool) -> BoundExprSet {
    let mut b = BoundExprSetBuilder::new(limits());
    let input = b
        .add(None, integer(false), ExprKind::Value(ValueId::new(4)))
        .unwrap();
    let signature = function(
        "fixture/unit/creator",
        vec![FunctionArgumentType::Value(integer(false))],
        integer(false),
    );
    let request = request(&signature);
    let call = b
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: signature,
                args: Box::new([input]),
            },
        )
        .unwrap();
    b.request(call, request).unwrap();
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(4), integer(false))]),
            }],
            &mut work,
        )
        .unwrap();
    let id = sealed.root_use_ids()[0];
    let mut claim = claim(sealed.uses()[&id].context, ArgumentControl::Eager);
    if environment {
        claim.effects.environment = [
            SemanticParameterKey::StatementStartUtc,
            SemanticParameterKey::TimeZone,
            SemanticParameterKey::AllowThrowException,
            SemanticParameterKey::DecimalOverflowToDouble,
            SemanticParameterKey::GroupConcatLegacy,
            SemanticParameterKey::GroupConcatMaxLen,
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, expected_key)| SemanticParameterRef {
            id: SemanticParameterId::new(ordinal as u32 * 19),
            expected_key,
        })
        .collect();
    }
    let unit = sealed
        .finish(BTreeMap::from([(id, claim)]), &mut work)
        .unwrap();
    work.finish().unwrap();
    unit
}
fn temporal_input() -> (BoundExprSetBuilder, Vec<ExprPort>) {
    let mut b = BoundExprSetBuilder::new(limits());
    let leaf = b
        .add(None, integer(false), ExprKind::Value(ValueId::new(4)))
        .unwrap();
    let mut normal = leaf;
    for _ in 0..3 {
        normal = b
            .add(
                None,
                integer(false),
                ExprKind::Cast {
                    expr: normal,
                    target: DataType::Int64,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    allow_throw_exception: SemanticParameterRef {
                        id: SemanticParameterId::new(0),
                        expected_key: SemanticParameterKey::AllowThrowException,
                    },
                },
            )
            .unwrap();
    }
    let signature = function(
        "fixture/unit/temporal-creator",
        vec![FunctionArgumentType::Value(integer(false))],
        integer(false),
    );
    let request = request(&signature);
    let call = b
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: signature,
                args: Box::new([normal]),
            },
        )
        .unwrap();
    b.request(call, request).unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let source = temporal_source_definitions_from_lookup_observed(
        TemporalSourceKind::TimeToSec,
        |id| b.definition(id),
        &[normal],
        &mut work,
    )
    .unwrap();
    assert_eq!(source.facts.cast_chain().len(), 3);
    b.definition_control(
        call,
        DefinitionControl {
            shape: ControlShape::TemporalSource(source.facts.shape()),
            temporal_sources: Some(source),
        },
    )
    .unwrap();
    b.root(call, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    work.finish().unwrap();
    (
        b,
        vec![ExprPort {
            values: Box::new([(ValueId::new(4), integer(false))]),
        }],
    )
}
fn temporal_unit() -> BoundExprSet {
    let (builder, ports) = temporal_input();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(ports, &mut work).unwrap();
    let id = sealed.root_use_ids()[0];
    let mut claim = claim(
        sealed.uses()[&id].context,
        ArgumentControl::TemporalSource(TemporalSourceKind::TimeToSec),
    );
    claim.temporal_source = Some(sealed.temporal_source(id).unwrap().clone());
    let unit = sealed
        .finish(BTreeMap::from([(id, claim)]), &mut work)
        .unwrap();
    work.finish().unwrap();
    unit
}
fn lexical_unit() -> BoundExprSet {
    let mut b = BoundExprSetBuilder::new(limits());
    let outer = b.reserve().unwrap();
    let outer_parameter = b
        .add(
            Some(outer),
            integer(false),
            ExprKind::LambdaParameter {
                lambda: outer,
                ordinal: 0,
            },
        )
        .unwrap();
    let inner = b.reserve().unwrap();
    let inner_parameter = b
        .add(
            Some(inner),
            integer(false),
            ExprKind::LambdaParameter {
                lambda: inner,
                ordinal: 0,
            },
        )
        .unwrap();
    let body = b
        .add(
            Some(inner),
            integer(false),
            ExprKind::Binary {
                left: outer_parameter,
                right: inner_parameter,
                op: BinaryOperator::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: Some(SemanticParameterRef {
                    id: SemanticParameterId::new(0),
                    expected_key: SemanticParameterKey::AllowThrowException,
                }),
            },
        )
        .unwrap();
    b.insert(
        inner,
        Some(outer),
        integer(false),
        ExprKind::Lambda {
            parameter_types: Box::new([integer(false)]),
            body,
        },
    )
    .unwrap();
    let signature = function(
        "fixture/unit/lexical-creator",
        vec![FunctionArgumentType::Lambda {
            parameter_types: TypedFactSlice::from_pure_boxed(Box::new([integer(false)])),
            result_type: integer(false),
        }],
        integer(false),
    );
    let request = request(&signature);
    let inner_call = b
        .add(
            Some(outer),
            integer(false),
            ExprKind::FunctionCall {
                function: signature.clone(),
                args: Box::new([inner]),
            },
        )
        .unwrap();
    b.request(inner_call, request.clone()).unwrap();
    b.insert(
        outer,
        None,
        integer(false),
        ExprKind::Lambda {
            parameter_types: Box::new([integer(false)]),
            body: inner_call,
        },
    )
    .unwrap();
    let root = b
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: signature,
                args: Box::new([outer]),
            },
        )
        .unwrap();
    b.request(root, request).unwrap();
    let shape = ControlShape::HigherOrder {
        body_ordinal: 0,
        body_demand: EvaluationDemand::Value,
    };
    for call in [inner_call, root] {
        b.definition_control(
            call,
            DefinitionControl {
                shape,
                temporal_sources: None,
            },
        )
        .unwrap();
    }
    b.root(root, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let unit = with_call_claims(
        b.seal(vec![], &mut work).unwrap(),
        ArgumentControl::HigherOrder {
            body_ordinal: 0,
            body_demand: EvaluationDemand::Value,
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    unit
}
#[test]
fn each_actual_scratch_request_preserves_the_first_host_refusal_and_input_identity() {
    for (fixture, unit) in [graph(), lexical_unit(), call_unit(true), temporal_unit()]
        .into_iter()
        .enumerate()
    {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut temporary, temporary_weak) =
            Scope::new(ScopeKind::Temporary, events.clone(), None);
        let (mut retained, retained_weak) = Scope::new(ScopeKind::Retained, events.clone(), None);
        let input = parts(&unit);
        let definitions = input.definitions.as_ptr();
        let regions = input.control.regions() as *const _;
        let output = read(input, &mut temporary, &mut retained, &Control::good()).unwrap();
        assert_eq!(output.definitions().as_ptr(), definitions);
        assert_eq!(
            output.control_flow().regions() as *const _,
            regions,
            "checked control is borrowed, never rebuilt"
        );
        assert_eq!(output, unit);
        let trace = events.lock().unwrap().clone();
        assert!(
            trace
                .iter()
                .all(|event| event.scope == ScopeKind::Temporary)
        );
        assert!(
            trace
                .iter()
                .any(|event| matches!(event.request, Request::Exact(..)))
        );
        assert!(
            trace
                .iter()
                .any(|event| matches!(event.request, Request::Opaque(_)))
        );
        // These two independent vocabulary extents belong to the hidden
        // environment and temporal authors, rather than the graph Vec headers.
        let expected = match fixture {
            2 => Some(Layout::array::<SemanticParameterKey>(6).unwrap()),
            3 => Some(Layout::array::<TemporalCastKind>(3).unwrap()),
            _ => None,
        };
        if let Some(layout) = expected {
            assert!(
                trace
                    .iter()
                    .any(|event| event.request == Request::Exact(layout.size(), layout.align())),
                "actual owned semantic source container is funded"
            );
        }

        drop(temporary);
        drop(retained);
        assert!(
            temporary_weak.upgrade().is_none(),
            "success must not retain validation scratch"
        );
        assert!(
            retained_weak.upgrade().is_none(),
            "moving existing parts creates no new retained owner"
        );
        for stop in 0..trace.len() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), Some(stop));
            let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), Some(stop));
            let error = read(
                parts(&unit),
                &mut temporary,
                &mut retained,
                &Control::good(),
            )
            .unwrap_err();
            assert!(
                matches!(error, UnitConstructionFailure::Creator(HostRefusal(actual)) if actual == stop)
            );
            assert_eq!(error.failure_fact(), HostRefusal(stop).failure_fact());
            assert_eq!(*events.lock().unwrap(), trace[..=stop]);
        }
    }
}
#[test]
fn every_control_prefix_stops_without_later_creator_callbacks() {
    let unit = graph();
    let control = Arc::new(Control::good());
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), None);
    let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), None);
    temporary.control = Some(control.clone());
    retained.control = Some(control.clone());
    read(parts(&unit), &mut temporary, &mut retained, &control).unwrap();
    let creator_trace = events.lock().unwrap().clone();
    let trace = control.trace();
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256)
    );
    for at in 1..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Arc::new(Control::at(at, cause.clone()));
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), None);
            let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), None);
            temporary.control = Some(control.clone());
            retained.control = Some(control.clone());
            assert!(
                matches!(read(parts(&unit), &mut temporary, &mut retained, &control), Err(UnitConstructionFailure::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
            let expected: Vec<_> = creator_trace
                .iter()
                .filter(|event| event.completed_checkpoints <= at)
                .cloned()
                .collect();
            assert_eq!(
                *events.lock().unwrap(),
                expected,
                "no creator callback after the first control refusal"
            );
        }
    }
}
fn wrong_argument(unit: &BoundExprSet) -> UnitParts {
    let mut input = parts(unit);
    let ExprKind::FunctionCall { function, .. } = &mut input.definitions[1].kind else {
        panic!("original call")
    };
    let fields = (0..24)
        .map(|i| Field::new(format!("original-field-{i}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    function.argument_types =
        TypedFactSlice::from_pure_boxed(Box::new([FunctionArgumentType::Value(ValueType::new(
            DataType::Struct(fields.into()),
            false,
        ))]));
    input
}
#[test]
fn escaping_static_and_wide_diagnostics_keep_only_the_original_retained_storage() {
    let unit = call_unit(false);
    for wide in [false, true] {
        let mut input = if wide {
            wrong_argument(&unit)
        } else {
            parts(&unit)
        };
        if !wide {
            input.roots[0].scope = RootScope::Port(9);
        }
        let input_hold = Arc::new(Hold);
        let input_weak = Arc::downgrade(&input_hold);
        input = input
            .with_storage_lifetime(ValueTypeStorageLifetime::held(input_hold))
            .unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut temporary, temporary_weak) =
            Scope::new(ScopeKind::Temporary, events.clone(), None);
        let (mut retained, retained_weak) = Scope::new(ScopeKind::Retained, events.clone(), None);
        let error = read(input, &mut temporary, &mut retained, &Control::good()).unwrap_err();
        assert!(
            input_weak.upgrade().is_none(),
            "owned diagnosis does not capture the original parts"
        );
        let UnitConstructionFailure::Owner(diagnosis) = &error else {
            panic!("{error:?}")
        };
        if wide {
            assert!(diagnosis.to_string().contains("original-field-23"));
        } else {
            assert!(diagnosis.to_string().contains("root input port is absent"));
        }
        assert_eq!(error.failure_fact(), diagnosis.cause().failure_fact());
        let trace = events.lock().unwrap().clone();
        assert!(trace.iter().any(|event| event.scope == ScopeKind::Retained));
        drop(temporary);
        drop(retained);
        assert!(temporary_weak.upgrade().is_none());
        assert!(retained_weak.upgrade().is_some());
        drop(error);
        assert!(retained_weak.upgrade().is_none());
        for stop in trace
            .iter()
            .enumerate()
            .filter_map(|(i, event)| (event.scope == ScopeKind::Retained).then_some(i))
        {
            let mut input = if wide {
                wrong_argument(&unit)
            } else {
                parts(&unit)
            };
            if !wide {
                input.roots[0].scope = RootScope::Port(9);
            }
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), Some(stop));
            let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), Some(stop));
            assert!(
                matches!(read(input, &mut temporary, &mut retained, &Control::good()), Err(UnitConstructionFailure::Creator(HostRefusal(actual))) if actual == stop)
            );
            assert_eq!(*events.lock().unwrap(), trace[..=stop]);
        }
    }
}
#[test]
fn source_header_refusal_precedes_every_creator_callback() {
    let unit = call_unit(false);
    let mut input = parts(&unit);
    input.definitions =
        vec![input.definitions[0].clone(); MAX_CONTROL_DEFINITIONS + 1].into_boxed_slice();
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), Some(0));
    let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), Some(0));
    let error = read(input, &mut temporary, &mut retained, &Control::good()).unwrap_err();
    assert!(
        matches!(&error, UnitConstructionFailure::Owner(e) if matches!(e.cause(), UnitError::Flow(ExpressionControlFlowError::TooManyItems)))
    );
    assert!(events.lock().unwrap().is_empty());
}
#[test]
fn single_original_scope_serves_scratch_and_diagnostic_without_a_second_identity() {
    let unit = call_unit(false);
    for invalid in [false, true] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut scope, weak) = Scope::new(ScopeKind::Single, events.clone(), None);
        let mut input = parts(&unit);
        if invalid {
            input.roots[0].scope = RootScope::Port(9);
        }
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let result = BoundExprSet::try_from_parts_with_single_creator_in(
            input,
            limits(),
            &mut scope,
            &mut work,
        );
        work.finish().unwrap();
        assert_eq!(result.is_err(), invalid);
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .all(|event| event.scope == ScopeKind::Single)
        );
        drop(scope);
        assert_eq!(weak.upgrade().is_some(), invalid);
        drop(result);
        assert!(weak.upgrade().is_none());
    }
}

#[test]
fn structural_recheck_and_sealed_finish_reborrow_the_original_scopes() {
    let unit = graph();
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut temporary, temporary_weak) = Scope::new(ScopeKind::Temporary, events.clone(), None);
    let (mut retained, retained_weak) = Scope::new(ScopeKind::Retained, events.clone(), None);
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    unit.validate_structure_with_creator_in(limits(), &mut temporary, &mut retained, &mut work)
        .unwrap();
    work.finish().unwrap();
    assert!(!events.lock().unwrap().is_empty());
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .all(|event| event.scope == ScopeKind::Temporary)
    );
    drop(temporary);
    drop(retained);
    assert!(temporary_weak.upgrade().is_none());
    assert!(retained_weak.upgrade().is_none());

    for refuse in [None, Some(0)] {
        let mut b = BoundExprSetBuilder::new(limits());
        let signature = function("fixture/unit/creator-finish", vec![], integer(false));
        let request = request(&signature);
        let call = b
            .add(
                None,
                integer(false),
                ExprKind::FunctionCall {
                    function: signature,
                    args: Box::default(),
                },
            )
            .unwrap();
        b.request(call, request).unwrap();
        b.definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
        b.root(call, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let sealed = b.seal(vec![], &mut work).unwrap();
        let id = sealed.root_use_ids()[0];
        let context = sealed.uses()[&id].context;
        let regions = sealed.control_flow().regions() as *const _;
        let definitions = sealed.definitions().as_ptr();
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), refuse);
        let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), refuse);
        let result = sealed.finish_with_creator_in(
            BTreeMap::from([(id, claim(context, ArgumentControl::Eager))]),
            &mut temporary,
            &mut retained,
            &mut work,
        );
        match refuse {
            None => {
                let output = result.unwrap();
                assert_eq!(output.definitions().as_ptr(), definitions);
                assert_eq!(output.control_flow().regions() as *const _, regions);
                work.finish().unwrap();
                assert!(
                    events
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|event| event.scope == ScopeKind::Temporary)
                );
            }
            Some(stop) => assert!(
                matches!(result, Err(UnitConstructionFailure::Creator(HostRefusal(actual))) if actual == stop)
            ),
        }
    }
}

#[test]
fn original_numeric_observer_precedes_each_creator_and_preserves_its_own_refusal() {
    for (unit, invalid) in [
        (graph(), false),
        (temporal_unit(), false),
        (call_unit(false), true),
    ] {
        let input = || {
            if invalid {
                wrong_argument(&unit)
            } else {
                parts(&unit)
            }
        };
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), None);
        let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), None);
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut requests = Vec::new();
        let result = BoundExprSet::try_from_parts_with_creator_observed_in(
            input(),
            limits(),
            &mut temporary,
            &mut retained,
            &mut |request| {
                requests.push(request);
                Ok(())
            },
            &mut work,
        );
        assert_eq!(result.is_err(), invalid);
        if invalid {
            assert!(matches!(result, Err(UnitConstructionFailure::Owner(_))));
        }
        work.finish().unwrap();
        let creator_trace = events.lock().unwrap().clone();
        let projected: Vec<_> = requests
            .iter()
            .map(|request| {
                let (storage, request) = match request {
                    UnitConstructionRequest::Exact { storage, layout } => {
                        (*storage, Request::Exact(layout.size(), layout.align()))
                    }
                    UnitConstructionRequest::Opaque { storage, facts } => {
                        assert_eq!(
                            facts.allocation_requests_upper_bound, 1,
                            "the original insertion-only cumulative model advances one unique entry"
                        );
                        assert_ne!(facts.cumulative_work_upper_bound, 0);
                        (*storage, Request::Opaque(facts.request_bytes_upper_bound))
                    }
                };
                Event {
                    scope: match storage {
                        UnitConstructionStorage::Temporary => ScopeKind::Temporary,
                        UnitConstructionStorage::Diagnostic | UnitConstructionStorage::Retained => {
                            ScopeKind::Retained
                        }
                    },
                    request,
                    completed_checkpoints: 0,
                }
            })
            .collect();
        assert_eq!(
            creator_trace, projected,
            "observation describes only the corresponding real creation request"
        );
        let control = Control::good();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut pure_requests = Vec::new();
        let pure = BoundExprSet::try_from_parts_observed_in(
            input(),
            limits(),
            &mut |request| {
                pure_requests.push(request);
                Ok::<_, HostRefusal>(())
            },
            &mut work,
        );
        assert_eq!(pure.is_err(), invalid);
        work.finish().unwrap();
        assert_eq!(
            pure_requests, requests,
            "pure receiving observes the same actual numerical facts without a creator"
        );

        for stop in 0..requests.len() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), None);
            let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), None);
            let control = Control::good();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut observed = Vec::new();
            let error = BoundExprSet::try_from_parts_with_creator_observed_in(
                input(),
                limits(),
                &mut temporary,
                &mut retained,
                &mut |request| {
                    let at = observed.len();
                    assert!(at <= stop, "observer callback after its original refusal");
                    observed.push(request);
                    if at == stop {
                        Err(HostRefusal(at))
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            )
            .unwrap_err();
            assert!(
                matches!(error, UnitConstructionFailure::Observer(HostRefusal(actual)) if actual == stop)
            );
            assert_eq!(error.failure_fact(), HostRefusal(stop).failure_fact());
            assert_eq!(observed, requests[..=stop]);
            assert_eq!(
                *events.lock().unwrap(),
                creator_trace[..stop],
                "observer refusal must precede the matching creator"
            );
        }
    }
    let unit = call_unit(false);
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, _) = Scope::new(ScopeKind::Single, events.clone(), None);
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut observed = 0;
    BoundExprSet::try_from_parts_with_single_creator_observed_in(
        parts(&unit),
        limits(),
        &mut scope,
        &mut |_| {
            observed += 1;
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(observed, events.lock().unwrap().len());
}

// The source builder and port buffers are constructed before this operation.
// Recording actual new requests does not acquire product capacity.
fn produce(
    builder: BoundExprSetBuilder,
    ports: Vec<ExprPort>,
    temporary: &mut Scope,
    retained: &mut Scope,
    control: &Control,
) -> Result<SealedUnit, UnitConstructionFailure<HostRefusal>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let sealed = builder.seal_with_creator_in(ports, temporary, retained, &mut work)?;
    work.finish()?;
    Ok(sealed)
}

#[test]
fn producer_funds_new_control_and_temporal_storage_and_preserves_every_host_refusal() {
    for input in [graph_input, temporal_input] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut temporary, scratch) = Scope::new(ScopeKind::Temporary, events.clone(), None);
        let (mut retained, output) = Scope::new(ScopeKind::Retained, events.clone(), None);
        let (builder, ports) = input();
        let sealed = produce(
            builder,
            ports,
            &mut temporary,
            &mut retained,
            &Control::good(),
        )
        .unwrap();
        let trace = events.lock().unwrap().clone();
        for kind in [ScopeKind::Temporary, ScopeKind::Retained] {
            for exact in [false, true] {
                assert!(
                    trace.iter().any(|event| event.scope == kind
                        && matches!(event.request, Request::Exact(..)) == exact),
                    "both actual scopes fund their own explicit and opaque backing"
                );
            }
        }
        assert!(
            sealed
                .uses()
                .values()
                .any(|use_| !use_.arguments.is_empty())
        );
        let flow = sealed.control_flow().clone();
        let root = sealed.root_use_ids()[0];
        let actual_arguments = flow.uses()[&root].arguments.as_ptr();
        drop(temporary);
        drop(retained);
        assert!(
            scratch.upgrade().is_none(),
            "published success releases actual scratch"
        );
        assert!(output.upgrade().is_some());
        drop(sealed);
        assert!(
            output.upgrade().is_some(),
            "independent Flow keeps original argument Boxes and maps funded"
        );
        assert_eq!(flow.uses()[&root].arguments.as_ptr(), actual_arguments);
        let alias = flow.clone();
        drop(flow);
        assert!(output.upgrade().is_some());
        drop(alias);
        assert!(
            output.upgrade().is_none(),
            "refund follows the last actual alias exit"
        );
        for stop in 0..trace.len() {
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), Some(stop));
            let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), Some(stop));
            let (builder, ports) = input();
            let error = produce(
                builder,
                ports,
                &mut temporary,
                &mut retained,
                &Control::good(),
            )
            .err()
            .expect("the actual creator must refuse");
            assert!(
                matches!(error, UnitConstructionFailure::Creator(HostRefusal(actual)) if actual == stop)
            );
            assert_eq!(error.failure_fact(), HostRefusal(stop).failure_fact());
            assert_eq!(*events.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn producer_keeps_every_original_control_prefix_and_no_later_creator_callback() {
    let control = Arc::new(Control::good());
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), None);
    let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), None);
    temporary.control = Some(control.clone());
    retained.control = Some(control.clone());
    let (builder, ports) = temporal_input();
    produce(builder, ports, &mut temporary, &mut retained, &control).unwrap();
    let checkpoints = control.trace();
    let creator = events.lock().unwrap().clone();
    assert!(
        checkpoints
            .iter()
            .all(|&(phase, units)| phase == CompilePhase::LowerProgram && units <= 256)
    );
    for at in 0..checkpoints.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Arc::new(Control::at(at, cause.clone()));
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut temporary, _) = Scope::new(ScopeKind::Temporary, events.clone(), None);
            let (mut retained, _) = Scope::new(ScopeKind::Retained, events.clone(), None);
            temporary.control = Some(control.clone());
            retained.control = Some(control.clone());
            let (builder, ports) = temporal_input();
            assert!(
                matches!(produce(builder,ports,&mut temporary,&mut retained,&control),Err(UnitConstructionFailure::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), checkpoints[..=at]);
            let expected: Vec<_> = creator
                .iter()
                .filter(|event| event.completed_checkpoints <= at)
                .cloned()
                .collect();
            assert_eq!(*events.lock().unwrap(), expected);
        }
    }
}

#[test]
fn producer_single_scope_retains_real_output_and_original_source_without_a_new_scope() {
    let source = Arc::new(Hold);
    let source_weak = Arc::downgrade(&source);
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, output) = Scope::new(ScopeKind::Single, events.clone(), None);
    let (builder, ports) = graph_input();
    let builder = builder
        .with_storage_lifetime(ValueTypeStorageLifetime::held(source))
        .unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
    let sealed = builder
        .seal_with_single_creator_in(ports, &mut scope, &mut work)
        .unwrap();
    let unit = sealed.finish(BTreeMap::new(), &mut work).unwrap();
    work.finish().unwrap();
    let flow = unit.control_flow().clone();
    let parts = unit.into_parts();
    drop(scope);
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.scope == ScopeKind::Single)
    );
    assert!(source_weak.upgrade().is_some());
    assert!(output.upgrade().is_some());
    drop(parts);
    assert!(
        source_weak.upgrade().is_none(),
        "the moved source belongs to original unit data"
    );
    assert!(
        output.upgrade().is_some(),
        "the separately escaping Flow owns its newly built backing"
    );
    drop(flow);
    assert!(output.upgrade().is_none());
}

#[test]
fn producer_rejection_retains_its_actual_diagnostic_and_not_scratch() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut temporary, scratch) = Scope::new(ScopeKind::Temporary, events.clone(), None);
    let (mut retained, diagnostic) = Scope::new(ScopeKind::Retained, events, None);
    let mut builder = BoundExprSetBuilder::new(limits());
    builder.reserve().unwrap();
    let error = produce(
        builder,
        vec![],
        &mut temporary,
        &mut retained,
        &Control::good(),
    )
    .err()
    .expect("unfilled original reservation");
    assert!(
        matches!(&error,UnitConstructionFailure::Owner(cause) if matches!(cause.cause(),UnitError::Invalid{definition:Some(id),message,..} if id.get()==0 && message=="reserved definition was not filled"))
    );
    drop(temporary);
    drop(retained);
    assert!(scratch.upgrade().is_none());
    assert!(diagnostic.upgrade().is_some());
    drop(error);
    assert!(diagnostic.upgrade().is_none());
}

fn authored_source(
    scope: &mut Scope,
    control: &Control,
) -> Result<BoundExprSetBuilder, UnitConstructionFailure<HostRefusal>> {
    let mut builder = BoundExprSetBuilder::new(limits());
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    for ordinal in 0..12 {
        let kind = ExprKind::Value(ValueId::new(ordinal * 19));
        let id = if ordinal == 0 {
            let id = builder.reserve_with_creator_in(scope, &mut work)?;
            builder.insert(id, None, integer(false), kind).unwrap();
            id
        } else {
            builder.add_with_creator_in(None, integer(false), kind, scope, &mut work)?
        };
        let root = builder.root_with_creator_in(
            id,
            EvaluationDemand::Value,
            RootScope::Port(0),
            scope,
            &mut work,
        )?;
        assert_eq!(id.get(), ordinal);
        assert_eq!(root.get(), ordinal);
    }
    work.finish()?;
    Ok(builder)
}

#[test]
fn original_source_buffers_borrow_actual_growth_and_keep_each_creator_and_control_refusal() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, source) = Scope::new(ScopeKind::Single, events.clone(), None);
    let control = Arc::new(Control::good());
    scope.control = Some(control.clone());
    let builder = authored_source(&mut scope, &control).unwrap();
    assert_eq!(builder.len(), 12);
    assert_eq!(builder.roots().len(), 12);
    for ordinal in 0..12 {
        assert!(
            matches!(builder.definition(ExprId::new(ordinal)).unwrap().kind,ExprKind::Value(id) if id.get()==ordinal*19)
        );
    }
    let trace = events.lock().unwrap().clone();
    let checkpoints = control.trace();
    // Twelve pushes cross several real amortized growths in each original
    // buffer. Spare capacity is reused; one push is not one allocation.
    assert!(trace.len() > 2 && trace.len() < 24);
    assert!(trace.iter().all(
        |event| event.scope == ScopeKind::Single && matches!(event.request, Request::Exact(..))
    ));
    drop(scope);
    assert!(source.upgrade().is_some());
    drop(builder);
    assert!(source.upgrade().is_none());
    for stop in 0..trace.len() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (mut scope, source) = Scope::new(ScopeKind::Single, events.clone(), Some(stop));
        let control = Arc::new(Control::good());
        scope.control = Some(control.clone());
        let error = authored_source(&mut scope, &control)
            .err()
            .expect("original creator refusal");
        assert!(
            matches!(error,UnitConstructionFailure::Creator(HostRefusal(actual)) if actual==stop)
        );
        assert_eq!(*events.lock().unwrap(), trace[..=stop]);
        drop(scope);
        assert!(source.upgrade().is_none());
    }
    for at in 0..checkpoints.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let (mut scope, source) = Scope::new(ScopeKind::Single, events.clone(), None);
            let control = Arc::new(Control::at(at, cause.clone()));
            scope.control = Some(control.clone());
            assert!(
                matches!(authored_source(&mut scope,&control),Err(UnitConstructionFailure::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), checkpoints[..=at]);
            let expected: Vec<_> = trace
                .iter()
                .filter(|event| event.completed_checkpoints <= at)
                .cloned()
                .collect();
            assert_eq!(*events.lock().unwrap(), expected);
            drop(scope);
            assert!(source.upgrade().is_none());
        }
    }
}
