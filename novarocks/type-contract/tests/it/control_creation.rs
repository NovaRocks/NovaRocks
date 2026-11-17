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

//! An independently cloned graph keeps its original creation responsibility.
//! This public SDK layer exercises the existing constructor rather than the
//! enclosing unit envelope, with fixed actual source definitions and no task.
use novarocks_type_contract::*;
use std::{
    alloc::Layout,
    sync::{Arc, Mutex, Weak},
};

struct Definitions(Vec<u32>);
impl ExpressionDefinitionMembership<u32> for Definitions {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, definition: u32) -> bool {
        self.0.get(definition as usize) == Some(&definition)
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        assert!(units <= 256);
        if let Some((stop, _)) = &self.stop {
            assert!(at <= *stop, "checkpoint after original refusal");
        }
        trace.push((phase, units));
        match &self.stop {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
struct Hold;
impl ValueTypeStorageHold for Hold {}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Request {
    Layout(usize, usize),
    Opaque(usize),
}
struct Scope {
    storage: ValueTypeStorageLifetime,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Option<usize>,
}
impl Scope {
    fn new(requests: Arc<Mutex<Vec<Request>>>, stop: Option<usize>) -> (Self, Weak<Hold>) {
        let hold = Arc::new(Hold);
        let weak = Arc::downgrade(&hold);
        (
            Self {
                storage: ValueTypeStorageLifetime::held(hold),
                requests,
                stop,
            },
            weak,
        )
    }
    fn record(&mut self, request: Request) -> Result<(), usize> {
        let mut requests = self.requests.lock().unwrap();
        let at = requests.len();
        if let Some(stop) = self.stop {
            assert!(at <= stop, "creator after original refusal");
        }
        requests.push(request);
        if self.stop == Some(at) {
            Err(at)
        } else {
            Ok(())
        }
    }
}
impl ValueSchemaCreationScope<usize> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), usize> {
        self.record(Request::Layout(layout.size(), layout.align()))
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), usize> {
        self.record(Request::Opaque(bytes))
    }
}
fn inputs() -> (
    Vec<ExpressionEvaluationRegion>,
    Vec<ExpressionInvocation<u32>>,
    Definitions,
) {
    let region = EvaluationRegionId::new(0);
    let uses = (0..2)
        .map(|id| ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(id),
                region,
                demand: EvaluationDemand::Value,
            },
            definition: id,
            control: ControlShape::Eager,
            arguments: if id == 0 {
                Box::new([ExpressionUseId::new(1)])
            } else {
                Box::default()
            },
        })
        .collect();
    (
        vec![ExpressionEvaluationRegion {
            id: region,
            parent: None,
            guard: None,
        }],
        uses,
        Definitions(vec![0, 1]),
    )
}
fn single(
    scope: &mut Scope,
    control: &Control,
) -> Result<ExpressionControlFlow<u32>, ExpressionControlCreationFailure<usize>> {
    let (regions, uses, definitions) = inputs();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)
        .map_err(ExpressionControlCreationFailure::Control)?;
    let graph = ExpressionControlFlow::try_new_with_single_creator_in(
        regions,
        uses,
        &definitions,
        scope,
        &mut work,
    )?;
    work.finish()
        .map_err(ExpressionControlCreationFailure::Control)?;
    Ok(graph)
}
#[test]
fn single_and_separate_scopes_share_geometry_and_clone_retains_actual_backing() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, weak) = Scope::new(requests.clone(), None);
    let control = Control::default();
    let graph = single(&mut scope, &control).unwrap();
    assert_eq!(graph.root_use_ids(), [ExpressionUseId::new(0)]);
    let clone = graph.clone();
    assert!(std::ptr::eq(graph.regions(), clone.regions()));
    assert!(std::ptr::eq(graph.uses(), clone.uses()));
    drop(scope);
    assert!(weak.upgrade().is_some());
    drop(graph);
    assert!(
        weak.upgrade().is_some(),
        "cloned graph outlives its enclosing unit/creator"
    );
    drop(clone);
    assert!(weak.upgrade().is_none());
    let single_requests = requests.lock().unwrap().clone();
    // The same root Vec starts with two use slots and retains one real root.
    assert_eq!(
        single_requests.last(),
        Some(&Request::Layout(
            std::mem::size_of::<ExpressionUseId>(),
            std::mem::align_of::<ExpressionUseId>()
        )),
        "the actual roots trim uses the retained creator at its late conversion slot"
    );

    let requests = Arc::new(Mutex::new(Vec::new()));
    let (mut temporary, temporary_weak) = Scope::new(requests.clone(), None);
    let (mut retained, retained_weak) = Scope::new(requests.clone(), None);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (regions, uses, definitions) = inputs();
    let graph = ExpressionControlFlow::try_new_with_creator_in(
        regions,
        uses,
        &definitions,
        &mut temporary,
        &mut retained,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(
        *requests.lock().unwrap(),
        single_requests,
        "one original container author, independent of caller scope arrangement"
    );
    drop(temporary);
    drop(retained);
    assert!(temporary_weak.upgrade().is_none());
    assert!(retained_weak.upgrade().is_some());
    drop(graph);
    assert!(retained_weak.upgrade().is_none());
}
#[test]
fn each_single_scope_request_preserves_the_original_host_refusal() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, _) = Scope::new(requests.clone(), None);
    single(&mut scope, &Control::default()).unwrap();
    let trace = requests.lock().unwrap().clone();
    for stop in 0..trace.len() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (mut scope, weak) = Scope::new(requests.clone(), Some(stop));
        assert!(
            matches!(single(&mut scope,&Control::default()),Err(ExpressionControlCreationFailure::Creator(actual)) if actual==stop)
        );
        assert_eq!(*requests.lock().unwrap(), trace[..=stop]);
        drop(scope);
        assert!(weak.upgrade().is_none());
    }
}
#[test]
fn original_control_prefix_and_source_header_still_precede_creation() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, _) = Scope::new(requests.clone(), None);
    let control = Control::default();
    single(&mut scope, &control).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let (mut scope, _) = Scope::new(requests, None);
            let control = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(single(&mut scope,&control),Err(ExpressionControlCreationFailure::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let (mut scope, _) = Scope::new(requests.clone(), Some(0));
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let (regions, uses, _) = inputs();
    let definitions = Definitions((0..=MAX_CONTROL_DEFINITIONS as u32).collect());
    assert!(matches!(
        ExpressionControlFlow::try_new_with_single_creator_in(
            regions,
            uses,
            &definitions,
            &mut scope,
            &mut work
        ),
        Err(ExpressionControlCreationFailure::Owner(
            ExpressionControlFlowError::TooManyItems
        ))
    ));
    assert!(requests.lock().unwrap().is_empty());
}
