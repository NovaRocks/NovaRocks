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
use crate::{
    BindingRequirements, CompileProfile, KernelAbiVersion, LocalProgramGraph, ProgramControlFlow,
    ProgramEvaluationRegion, ProgramExpressionArena, ProgramExpressionUse, ProgramNode,
    ProgramRootControlBindings, ProgramRootUseBinding, ProgramTypedExpressions, StaticExprNode,
    StaticLayout, StaticValues,
};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::{FunctionArgumentType, FunctionValueType};
use novarocks_type_contract::{
    EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionUseId,
};
use std::{num::NonZeroUsize, sync::Mutex};
const ROOT_DOMAIN: EvaluationRegionId = EvaluationRegionId::new(7);
#[derive(Default)]
struct Control {
    fail: Option<(u32, CompileControlError)>,
    units: Mutex<usize>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        *self.units.lock().unwrap() += units as usize;
        if let Some((at, failure)) = self.fail.clone()
            && at == units
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
fn int() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn occurrence(id: u32) -> ProgramUseRef {
    ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: ExpressionUseId::new(id),
    }
}
fn input() -> ProgramLexicalSource {
    ProgramLexicalSource::Input(ProgramChannelSite::Layout {
        node: ProgramNodeId::new(0),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal: 0,
    })
}
fn slot(use_id: u32, source: ProgramLexicalSource) -> ProgramSlotBinding {
    ProgramSlotBinding {
        occurrence: occurrence(use_id),
        source,
    }
}
fn use_(
    id: u32,
    definition: usize,
    domain: EvaluationRegionId,
    control: ControlShape,
    args: &[u32],
) -> ProgramExpressionUse {
    ProgramExpressionUse {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region: domain,
            demand: EvaluationDemand::Value,
        },
        definition: ProgramExprId::new(definition),
        control,
        arguments: args.iter().map(|id| ExpressionUseId::new(*id)).collect(),
    }
}
fn eager(id: u32, definition: usize, domain: EvaluationRegionId) -> ProgramExpressionUse {
    use_(id, definition, domain, ControlShape::Eager, &[])
}
fn lit() -> StaticExprKind {
    crate::test_support::integer(4)
}
fn ref_(slot: u32) -> StaticExprKind {
    StaticExprKind::SlotId(SlotId::new(slot))
}
struct Fixture {
    channels: ProgramTypedChannels,
    lambdas: Vec<ProgramLambdaBinding>,
    slots: Vec<ProgramSlotBinding>,
}
fn build(
    definitions: Vec<StaticExprKind>,
    uses: Vec<ProgramExpressionUse>,
    roots: &[(usize, u32)],
    domains: Vec<ProgramEvaluationRegion>,
    lambdas: Vec<ProgramLambdaBinding>,
    slots: Vec<ProgramSlotBinding>,
) -> Fixture {
    build_project(
        definitions,
        uses,
        roots,
        domains,
        lambdas,
        slots,
        None,
        None,
    )
}
// Keep independent source, control, closure and output axes explicit in fixtures.
fn build_project(
    definitions: Vec<StaticExprKind>,
    uses: Vec<ProgramExpressionUse>,
    roots: &[(usize, u32)],
    domains: Vec<ProgramEvaluationRegion>,
    lambdas: Vec<ProgramLambdaBinding>,
    slots: Vec<ProgramSlotBinding>,
    projected_slots: Option<Vec<SlotId>>,
    output_indices: Option<Vec<usize>>,
) -> Fixture {
    let types = definitions
        .iter()
        .map(|_| FunctionArgumentType::Value(int()))
        .collect::<Vec<_>>();
    let expressions = Arc::new(
        crate::test_support::arena(
            definitions
                .into_iter()
                .map(|definition| {
                    StaticExprNode::new_value_typed(
                        definition,
                        novarocks_functions::FunctionValueType::new(DataType::Int64, false),
                    )
                })
                .collect(),
        )
        .unwrap(),
    );
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Int64,
        false,
    )]));
    let source_layout =
        StaticLayout::try_new(source_schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(source_schema, vec![Arc::new(Int64Array::from(vec![4, 2]))]).unwrap(),
        source_layout.clone(),
    )
    .unwrap();
    let expr_slots = projected_slots.unwrap_or_else(|| {
        (0..roots.len())
            .map(|i| SlotId::new(10 + i as u32))
            .collect()
    });
    let visible_slots = output_indices
        .as_ref()
        .map(|indices| indices.iter().map(|i| expr_slots[*i]).collect::<Vec<_>>())
        .unwrap_or_else(|| expr_slots.clone());
    let output_count = visible_slots.len();
    let output = StaticLayout::try_new(
        Arc::new(Schema::new(
            (0..output_count)
                .map(|i| Field::new(format!("output{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        )),
        visible_slots.into(),
    )
    .unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        output.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let program = LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(1, ProgramNodeKind::Values { values }, source_layout),
            ProgramNode::new(
                2,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: roots
                        .iter()
                        .map(|(definition, _)| ProgramExprId::new(*definition))
                        .collect(),
                    expr_slot_ids: expr_slots,
                    expr_slot_schemas: None,
                    output_indices,
                },
                output,
            ),
        ],
        ProgramNodeId::new(1),
        expressions,
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let flow =
        ProgramControlFlow::try_new(domains, uses, types.len(), &Control::default()).unwrap();
    let snapshot = ProgramRootControlBindings::try_new(
        program,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        roots
            .iter()
            .enumerate()
            .map(|(index, (_, id))| ProgramRootUseBinding {
                site: ProgramExpressionRootSite::Node {
                    node: ProgramNodeId::new(1),
                    role: ProgramNodeExpressionRole::ProjectOutput {
                        expression: index as u32,
                    },
                },
                use_id: ExpressionUseId::new(*id),
            })
            .collect(),
        &Control::default(),
    )
    .unwrap();
    let calls =
        crate::ProgramResolvedCalls::try_new(snapshot, vec![], &Control::default()).unwrap();
    let expressions = ProgramTypedExpressions::try_new(
        calls,
        BTreeMap::from([(ProgramExpressionArena::Main, types)]),
        &Control::default(),
    )
    .unwrap();
    let mut channels = vec![(
        ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        },
        int(),
    )];
    for ordinal in 0..output_count {
        channels.push((
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(1),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: ordinal as u32,
            },
            int(),
        ));
    }
    Fixture {
        channels: ProgramTypedChannels::try_new(expressions, channels, &Control::default())
            .unwrap(),
        lambdas,
        slots,
    }
}
fn construct(fixture: Fixture) -> Result<ProgramLexicalBindings, ProgramLexicalBindingError> {
    ProgramLexicalBindings::try_new(
        fixture.channels,
        fixture.lambdas,
        fixture.slots,
        &Control::default(),
    )
}
#[test]
fn ordinary_input_slot_is_not_an_unbound_capture() {
    let fixture = build(
        vec![ref_(1)],
        vec![eager(0, 0, ROOT_DOMAIN)],
        &[(0, 0)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(0, input())],
    );
    let checked = construct(fixture).unwrap();
    assert!(checked.lambdas().is_empty());
    assert_eq!(checked.slots()[&occurrence(0)], input());
}
fn simple_fixture() -> Fixture {
    build(
        vec![ref_(1)],
        vec![eager(0, 0, ROOT_DOMAIN)],
        &[(0, 0)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(0, input())],
    )
}
fn projected(expression: u32) -> ProgramLexicalSource {
    ProgramLexicalSource::Projected {
        node: ProgramNodeId::new(1),
        expression,
    }
}
#[test]
fn prior_unprojected_computation_and_same_slot_replacement_have_exact_sources() {
    for produced in [1, 20] {
        let fixture = build_project(
            vec![lit(), ref_(produced)],
            vec![eager(0, 0, ROOT_DOMAIN), eager(1, 1, ROOT_DOMAIN)],
            &[(0, 0), (1, 1)],
            vec![ProgramEvaluationRegion {
                id: ROOT_DOMAIN,
                parent: None,
                guard: None,
            }],
            vec![],
            vec![slot(1, projected(0))],
            Some(vec![SlotId::new(produced), SlotId::new(10)]),
            Some(vec![1]),
        );
        assert!(
            fixture
                .channels
                .slot_type(
                    ProgramNodeId::new(1),
                    ProgramChannelLayoutRole::NodeOutput,
                    SlotId::new(produced)
                )
                .is_none()
        );
        construct(fixture).unwrap();
    }
    let stale = build_project(
        vec![lit(), ref_(1)],
        vec![eager(0, 0, ROOT_DOMAIN), eager(1, 1, ROOT_DOMAIN)],
        &[(0, 0), (1, 1)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(1, input())],
        Some(vec![SlotId::new(1), SlotId::new(10)]),
        Some(vec![1]),
    );
    assert_eq!(
        construct(stale).unwrap_err(),
        ProgramLexicalBindingError::ShadowedSource
    );
    let future = build_project(
        vec![ref_(1), lit()],
        vec![eager(0, 0, ROOT_DOMAIN), eager(1, 1, ROOT_DOMAIN)],
        &[(0, 0), (1, 1)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(0, projected(1))],
        Some(vec![SlotId::new(20), SlotId::new(1)]),
        Some(vec![0]),
    );
    assert_eq!(
        construct(future).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
}
// These neutral-constructor probes observe actual request ports and finite
// holders. They do not stand in for BE tracker or Native capture publication.
struct LexicalCreationHold(Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for LexicalCreationHold {}
impl Drop for LexicalCreationHold {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
fn lexical_creation_storage(
    exits: &Arc<std::sync::atomic::AtomicUsize>,
) -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(LexicalCreationHold(exits.clone())))
}
#[test]
fn lexical_created_maps_keep_retained_owner_after_temporary_and_creator_exit() {
    let fixture = simple_fixture();
    let expected = construct(simple_fixture()).unwrap();
    let temporary_exits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retained_exits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let temporary_events = std::cell::RefCell::new(Vec::new());
    let retained_events = std::cell::RefCell::new(Vec::new());
    let result = {
        let mut tr = |layout: std::alloc::Layout| -> Result<(), u64> {
            temporary_events.borrow_mut().push(layout.size());
            Ok(())
        };
        let mut to = |bytes: usize| -> Result<(), u64> {
            temporary_events.borrow_mut().push(bytes);
            Ok(())
        };
        let mut rr = |layout: std::alloc::Layout| -> Result<(), u64> {
            retained_events.borrow_mut().push(layout.size());
            Ok(())
        };
        let mut ro = |bytes: usize| -> Result<(), u64> {
            retained_events.borrow_mut().push(bytes);
            Ok(())
        };
        let mut temporary = novarocks_type_contract::ValueSchemaCreation::new(
            lexical_creation_storage(&temporary_exits),
            &mut tr,
            &mut to,
        );
        let mut retained = novarocks_type_contract::ValueSchemaCreation::new(
            lexical_creation_storage(&retained_exits),
            &mut rr,
            &mut ro,
        );
        ProgramLexicalBindings::try_new_with_creator_in(
            fixture.channels,
            fixture.lambdas,
            fixture.slots,
            ValueTypeStorageLifetime::pure(),
            &mut temporary,
            &mut retained,
            &Control::default(),
        )
        .unwrap()
    };
    assert_eq!(result.lambdas(), expected.lambdas());
    assert_eq!(result.slots(), expected.slots());
    assert!(!temporary_events.borrow().is_empty());
    assert!(!retained_events.borrow().is_empty());
    assert_eq!(temporary_exits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(retained_exits.load(std::sync::atomic::Ordering::SeqCst), 0);
    let late = result.clone();
    drop(result);
    assert_eq!(retained_exits.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(late);
    assert_eq!(retained_exits.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[test]
fn lexical_first_creator_cause_is_preserved_without_temporary_or_callback_tail() {
    let fixture = simple_fixture();
    let calls = std::cell::Cell::new(0usize);
    let mut reject = |_: std::alloc::Layout| -> Result<(), u64> {
        panic!("first insertion uses opaque geometry")
    };
    let mut opaque = |_: usize| -> Result<(), u64> {
        assert_eq!(calls.replace(calls.get() + 1), 0);
        Err(91)
    };
    let mut temporary_request = |_: std::alloc::Layout| -> Result<(), u64> {
        panic!("temporary callback after original refusal")
    };
    let mut temporary_opaque =
        |_: usize| -> Result<(), u64> { panic!("temporary callback after original refusal") };
    let mut temporary = novarocks_type_contract::ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut temporary_request,
        &mut temporary_opaque,
    );
    let mut retained = novarocks_type_contract::ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut reject,
        &mut opaque,
    );
    let error = ProgramLexicalBindings::try_new_with_creator_in(
        fixture.channels,
        fixture.lambdas,
        fixture.slots,
        ValueTypeStorageLifetime::pure(),
        &mut temporary,
        &mut retained,
        &Control::default(),
    )
    .unwrap_err();
    assert!(matches!(error, ProgramCreationFailure::Creator(91)));
    assert_eq!(calls.get(), 1);
}

#[test]
fn missing_extra_duplicate_or_foreign_occurrences_never_reuse_bindings() {
    let mut fixture = simple_fixture();
    fixture.slots.clear();
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::IncompleteCoverage
    );
    let mut fixture = simple_fixture();
    fixture.slots.push(fixture.slots[0]);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::DuplicateBinding
    );
    let mut fixture = simple_fixture();
    fixture.slots[0].occurrence = occurrence(u32::MAX);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::IncompleteCoverage
    );
    let mut fixture = simple_fixture();
    fixture.slots.push(slot(1, input()));
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::IncompleteCoverage
    );
}

#[test]
fn slot_records_share_one_bound_and_observe_typed_outer_control() {
    let fixture = simple_fixture();
    assert_eq!(
        ProgramLexicalBindings::try_new(
            fixture.channels,
            vec![],
            vec![slot(0, input()); MAX_CONTROL_USE_REFERENCES + 1],
            &Control::default()
        )
        .unwrap_err(),
        ProgramLexicalBindingError::TooManyItems
    );
    let fixture = || {
        let uses = (0..320).map(|id| eager(id, 0, ROOT_DOMAIN)).collect();
        let roots = (0..320).map(|id| (0, id)).collect::<Vec<_>>();
        let slots = (0..320).map(|id| slot(id, input())).collect();
        build(
            vec![ref_(1)],
            uses,
            &roots,
            vec![ProgramEvaluationRegion {
                id: ROOT_DOMAIN,
                parent: None,
                guard: None,
            }],
            vec![],
            slots,
        )
    };
    construct(fixture()).unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let fixture = fixture();
        let control = Control {
            fail: Some((256, cause.clone())),
            ..Default::default()
        };
        assert_eq!(
            ProgramLexicalBindings::try_new(
                fixture.channels,
                fixture.lambdas,
                fixture.slots,
                &control
            )
            .unwrap_err(),
            ProgramLexicalBindingError::Control(cause)
        );
    }
}
