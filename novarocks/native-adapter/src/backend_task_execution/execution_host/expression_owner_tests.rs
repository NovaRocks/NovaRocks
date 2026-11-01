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

// Real FE package, Native creator and independently retained expression owners.
// This leaf is registered inside execution_host::tests::compiled_package_host.
use super::*;
use crate::backend_task_execution::compiled_package::{CompiledTypeScope, CompiledTypeScopes};
use crate::native_type_construction::{
    NativeTypeConstruction, NativeTypeConstructionError, NativeTypeScope,
};
use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_local_program::{
    ExpressionProgram, LocalProgram, ProgramCreationFailure, ProgramLexicalBindings, ProgramUseRef,
    StaticExprKind,
};
use novarocks_type_contract::{
    ValueSchemaCreation, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
};
use std::{
    alloc::Layout,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

const ONE_IN: &str = "SELECT 1 IN (1, NULL) AS original_in";
const TWO_IN: &str = "SELECT 1 IN (1, NULL) AS original_in, 1 IN (2, NULL) AS unrelated_in";

fn compiled_in_source(sql: &str, scratch_len: usize) -> (CompiledTaskProgram, Arc<MemTracker>) {
    let (bytes, _) = select_sql_producer(sql);
    let tracker = MemTracker::new_root("actual expression source");
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
    if scratch_len != 0 {
        actual
            .request_layout(Layout::array::<u64>(scratch_len).unwrap())
            .unwrap();
    }
    let mut scratch = Vec::<u64>::with_capacity(scratch_len);
    scratch.resize(scratch_len, 37);
    let scopes = CompiledTypeScopes::new(&actual);
    let mut exact = |layout| actual.request_layout(layout);
    let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
    let mut creation = ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
    let compiled = interpreter()
        .compile(
            &bytes,
            CompiledTaskOptions {
                pipeline_dop: NonZeroUsize::MIN,
            },
            &mut creation,
            &scopes,
            &Unbounded,
        )
        .expect("real FE IN package compiles on its Native creator");
    drop(creation);
    {
        let _scopes = scopes;
    }
    drop(scratch);
    drop(actual);
    (compiled, tracker)
}

fn actual_in_sites(program: &LocalProgram) -> Vec<ProgramUseRef> {
    let typed = program.checked().channels().expressions();
    let snapshot = typed.resolved_calls().snapshot();
    let mut sites = Vec::new();
    for (&arena, flow) in snapshot.flows() {
        let definitions = &snapshot.roots().arenas()[&arena];
        for (&use_id, invocation) in flow.uses() {
            let Some(node) = definitions.node(invocation.definition) else {
                panic!("real definition");
            };
            let StaticExprKind::PreparedInList { child, values, .. } = node.kind() else {
                continue;
            };
            let site = ProgramUseRef { arena, use_id };
            let recipe = program
                .native_inlist_recipe(site)
                .expect("actual IN occurrence has its prepared owner");
            let novarocks_functions::FunctionArgumentType::Value(source) =
                typed.definition_type(arena, *child).unwrap()
            else {
                panic!("complete IN source");
            };
            assert_eq!(recipe.source_type(), source);
            assert_eq!(recipe.candidate_types().len(), values.len());
            for (candidate, definition) in recipe.candidate_types().iter().zip(values) {
                let novarocks_functions::FunctionArgumentType::Value(expected) =
                    typed.definition_type(arena, *definition).unwrap()
                else {
                    panic!("complete IN candidate");
                };
                assert_eq!(candidate, expected);
            }
            let novarocks_functions::FunctionArgumentType::Value(result) =
                typed.definition_type(arena, invocation.definition).unwrap()
            else {
                panic!("complete IN result");
            };
            assert_eq!(recipe.result_type(), result);
            sites.push(site);
        }
    }
    sites
}
fn late_in(
    sql: &str,
    scratch: usize,
    expected_calls: usize,
) -> (
    novarocks_functions::PreparedNativeInListRecipe,
    Arc<MemTracker>,
) {
    let (compiled, tracker) = compiled_in_source(sql, scratch);
    let sites = actual_in_sites(compiled.program());
    assert_eq!(
        sites.len(),
        expected_calls,
        "SQL must actually retain PreparedInList, not fold it away"
    );
    let late = compiled
        .program()
        .native_inlist_recipe(sites[0])
        .unwrap()
        .clone();
    drop(compiled);
    assert!(
        tracker.current() > 0,
        "the independently owned parameter/type holders remain live"
    );
    (late, tracker)
}
#[test]
fn actual_host_late_in_recipe_keeps_no_unrelated_main_or_other_call_charge() {
    let (minimal, minimal_tracker) = late_in(ONE_IN, 0, 1);
    let (wide, wide_tracker) = late_in(TWO_IN, 512, 2);
    assert_eq!(minimal.source_type(), wide.source_type());
    assert_eq!(minimal.candidate_types(), wide.candidate_types());
    assert_eq!(minimal.result_type(), wide.result_type());
    assert_eq!(
        wide_tracker.current(),
        minimal_tracker.current(),
        "late parameters retain neither the actual unrelated Vec nor the other IN call/map/program"
    );
    let independent = wide.clone();
    drop(wide);
    assert_eq!(wide_tracker.current(), minimal_tracker.current());
    drop(independent);
    assert_eq!(wide_tracker.current(), 0);
    drop(minimal);
    assert_eq!(minimal_tracker.current(), 0);
}

#[derive(Clone, Debug)]
struct ActualScopeEvent {
    scope: usize,
    bytes: usize,
    kind: &'static str,
}
#[derive(Default)]
struct TraceState {
    events: Mutex<Vec<ActualScopeEvent>>,
    scopes: AtomicUsize,
    controls: AtomicUsize,
    refused: AtomicBool,
    original: Mutex<Option<NativeTypeConstructionError>>,
    refused_at: Mutex<Option<(usize, usize)>>,
}
impl PureCompileControl for TraceState {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        assert!(
            !self.refused.load(Ordering::SeqCst),
            "control callback after original tracker refusal"
        );
        self.controls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl TraceState {
    fn before(
        &self,
        scope: usize,
        bytes: usize,
        kind: &'static str,
        tracker: &MemTracker,
        fail_at: Option<usize>,
    ) {
        assert!(
            !self.refused.load(Ordering::SeqCst),
            "request/factory callback after original tracker refusal"
        );
        let mut events = self.events.lock().unwrap();
        if fail_at == Some(events.len()) {
            tracker.install_limit_once(tracker.current()).unwrap();
        }
        events.push(ActualScopeEvent { scope, bytes, kind });
    }
    fn result<T>(
        &self,
        result: Result<T, NativeTypeConstructionError>,
    ) -> Result<T, NativeTypeConstructionError> {
        if let Err(cause) = &result {
            assert!(
                !self.refused.swap(true, Ordering::SeqCst),
                "a second creator refusal was observed"
            );
            *self.original.lock().unwrap() = Some(cause.clone());
            *self.refused_at.lock().unwrap() = Some((
                self.controls.load(Ordering::SeqCst),
                self.events.lock().unwrap().len(),
            ));
        }
        result
    }
}
// These stack wrappers only trace/delegate the real Native factory. Their
// storage getter returns the original scope's finite reservation unchanged.
struct TracedFactory<'borrow, 'control> {
    inner: CompiledTypeScopes<'borrow, 'control>,
    trace: &'borrow TraceState,
    tracker: &'borrow MemTracker,
    fail_at: Option<usize>,
}
struct TracedScope<'borrow> {
    inner: CompiledTypeScope<'borrow>,
    trace: &'borrow TraceState,
    tracker: &'borrow MemTracker,
    scope: usize,
    fail_at: Option<usize>,
}
impl ValueSchemaCreationScope<NativeTypeConstructionError> for TracedScope<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.inner.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        self.trace.before(
            self.scope,
            layout.size(),
            "layout",
            self.tracker,
            self.fail_at,
        );
        self.trace.result(self.inner.request_layout(layout))
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        self.trace
            .before(self.scope, bytes, "opaque", self.tracker, self.fail_at);
        self.trace
            .result(self.inner.observe_opaque_upper_bound(bytes))
    }
}
struct TracedTemporary<'borrow> {
    inner: NativeTypeScope<'borrow>,
    trace: &'borrow TraceState,
    tracker: &'borrow MemTracker,
    fail_at: Option<usize>,
}
impl ValueSchemaCreationScope<NativeTypeConstructionError> for TracedTemporary<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.inner.storage_lifetime()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), NativeTypeConstructionError> {
        self.trace.before(
            usize::MAX,
            layout.size(),
            "temporary",
            self.tracker,
            self.fail_at,
        );
        self.trace.result(self.inner.request_layout(layout))
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        self.trace
            .before(usize::MAX, bytes, "temporary", self.tracker, self.fail_at);
        self.trace
            .result(self.inner.observe_opaque_upper_bound(bytes))
    }
}
impl ValueSchemaCreationFactory<NativeTypeConstructionError> for TracedFactory<'_, '_> {
    type Scope<'a>
        = TracedScope<'a>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, NativeTypeConstructionError> {
        let scope = self.trace.scopes.fetch_add(1, Ordering::SeqCst);
        self.trace.before(
            scope,
            NativeTypeScope::reservation_allocation_layout()?.size(),
            "scope",
            self.tracker,
            self.fail_at,
        );
        let inner = self.trace.result(self.inner.begin_scope())?;
        Ok(TracedScope {
            inner,
            trace: self.trace,
            tracker: self.tracker,
            scope,
            fail_at: self.fail_at,
        })
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>> {
        let scope = self.trace.scopes.fetch_add(1, Ordering::SeqCst);
        self.trace.before(
            scope,
            NativeTypeScope::reservation_allocation_layout()
                .map_err(ValueSchemaScopeBeginFailure::Creator)?
                .size(),
            "scope",
            self.tracker,
            self.fail_at,
        );
        let inner = match self.inner.begin_scope_observed(observe) {
            Ok(inner) => inner,
            Err(ValueSchemaScopeBeginFailure::Creator(cause)) => {
                let cause = self.trace.result::<()>(Err(cause)).unwrap_err();
                return Err(ValueSchemaScopeBeginFailure::Creator(cause));
            }
            Err(ValueSchemaScopeBeginFailure::Observer(cause)) => {
                return Err(ValueSchemaScopeBeginFailure::Observer(cause));
            }
        };
        Ok(TracedScope {
            inner,
            trace: self.trace,
            tracker: self.tracker,
            scope,
            fail_at: self.fail_at,
        })
    }
}

#[test]
fn actual_host_expression_all_family_and_final_arc_scopes_exit_independently() {
    let (compiled, source_tracker) = compiled_in_source(ONE_IN, 0);
    let sites = actual_in_sites(compiled.program());
    assert_eq!(sites.len(), 1);
    let checked = compiled.program().checked().clone();
    let tracker = MemTracker::new_root("actual expression entity scopes");
    let trace = TraceState::default();
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &trace).unwrap();
    let mut temporary = TracedTemporary {
        inner: actual.begin_scope().unwrap(),
        trace: &trace,
        tracker: &tracker,
        fail_at: None,
    };
    let factory = TracedFactory {
        inner: CompiledTypeScopes::new(&actual),
        trace: &trace,
        tracker: &tracker,
        fail_at: None,
    };
    let program =
        ExpressionProgram::try_new_with_creator_in(checked, &mut temporary, &factory, &trace)
            .unwrap();
    let late_recipe = program.native_inlist_recipe(sites[0]).unwrap().clone();
    let events = trace.events.lock().unwrap().clone();
    // The original ordered eight families, one independent actual IN parameter
    // collection and one final entity Arc all have real Native scopes.
    assert_eq!(trace.scopes.load(Ordering::SeqCst), 10);
    assert_eq!(
        events.iter().filter(|event| event.kind == "scope").count(),
        10
    );
    assert!(
        events.iter().any(|event| event.kind == "temporary"),
        "the actual IN candidate reference Vec is separately temporary"
    );
    let recipe_scope = events
        .iter()
        .find(|event| event.scope == 2 && event.kind == "layout")
        .expect("the actual IN parameter scope has constructor requests")
        .scope;
    let recipe_charge = events
        .iter()
        .filter(|event| event.scope == recipe_scope)
        .map(|event| i64::try_from(event.bytes).unwrap())
        .sum::<i64>();
    assert!(recipe_charge > 0);
    assert!(
        events
            .iter()
            .any(|event| event.scope == 1 && event.kind == "opaque"),
        "IN map insertion uses its separate family scope"
    );
    assert!(
        events
            .iter()
            .any(|event| event.scope == 9 && event.kind == "layout"),
        "final Arc uses its own exact constructor request"
    );
    let late_program = Arc::clone(&program);
    drop(program);
    drop(compiled);
    drop(temporary);
    {
        let _factory = factory;
    }
    drop(actual);
    assert!(
        tracker.current() > recipe_charge,
        "all eight family/Arc owners remain with the late entity"
    );
    drop(late_program);
    assert_eq!(
        tracker.current(),
        recipe_charge,
        "only the independent IN parameter collection remains"
    );
    drop(late_recipe);
    assert_eq!(tracker.current(), 0);
    assert_eq!(source_tracker.current(), 0);
}

fn actual_constructor_events(checked: ProgramLexicalBindings) -> Vec<ActualScopeEvent> {
    let tracker = MemTracker::new_root("expression request prefix discovery");
    let trace = TraceState::default();
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &trace).unwrap();
    let mut temporary = TracedTemporary {
        inner: actual.begin_scope().unwrap(),
        trace: &trace,
        tracker: &tracker,
        fail_at: None,
    };
    let factory = TracedFactory {
        inner: CompiledTypeScopes::new(&actual),
        trace: &trace,
        tracker: &tracker,
        fail_at: None,
    };
    let program =
        ExpressionProgram::try_new_with_creator_in(checked, &mut temporary, &factory, &trace)
            .unwrap();
    let events = trace.events.lock().unwrap().clone();
    drop(program);
    drop(temporary);
    {
        let _factory = factory;
    }
    drop(actual);
    assert_eq!(tracker.current(), 0);
    events
}
#[test]
fn actual_host_expression_each_first_tracker_refusal_preserves_cause_and_has_no_tail() {
    let (compiled, source_tracker) = compiled_in_source(ONE_IN, 0);
    assert_eq!(actual_in_sites(compiled.program()).len(), 1);
    let checked = compiled.program().checked().clone();
    let discovered = actual_constructor_events(checked.clone());
    assert!(!discovered.is_empty());
    // Every real nonzero scope/header/map request in this actual constructor
    // gets a fresh immutable tracker limit at the original prospective event.
    for (fail_at, event) in discovered.iter().enumerate() {
        if event.bytes == 0 {
            continue;
        }
        let tracker = MemTracker::new_root("expression original tracker refusal");
        let trace = TraceState::default();
        let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &trace).unwrap();
        let mut temporary = TracedTemporary {
            inner: actual.begin_scope().unwrap(),
            trace: &trace,
            tracker: &tracker,
            fail_at: Some(fail_at),
        };
        let prior = tracker.current();
        let factory = TracedFactory {
            inner: CompiledTypeScopes::new(&actual),
            trace: &trace,
            tracker: &tracker,
            fail_at: Some(fail_at),
        };
        let error = ExpressionProgram::try_new_with_creator_in(
            checked.clone(),
            &mut temporary,
            &factory,
            &trace,
        )
        .unwrap_err();
        let ProgramCreationFailure::Creator(original) = error else {
            panic!("original Native tracker cause must remain typed");
        };
        assert!(matches!(original, NativeTypeConstructionError::Capacity(_)));
        assert_eq!(Some(original), *trace.original.lock().unwrap());
        assert_eq!(
            Some((
                trace.controls.load(Ordering::SeqCst),
                trace.events.lock().unwrap().len()
            )),
            *trace.refused_at.lock().unwrap(),
            "no control or request tail after the first actual refusal"
        );
        let actual_prefix = trace.events.lock().unwrap().clone();
        assert_eq!(actual_prefix.len(), fail_at + 1);
        // The rejected last event acquired no charge. Successful temporary
        // requests belong to the caller's still-borrowed operation scope;
        // failed retained family/recipe/Arc scopes must already have exited.
        let temporary_prefix = actual_prefix[..fail_at]
            .iter()
            .filter(|event| event.kind == "temporary")
            .try_fold(0_i64, |total, event| {
                total.checked_add(i64::try_from(event.bytes).unwrap())
            })
            .unwrap();
        assert_eq!(
            tracker.current(),
            prior.checked_add(temporary_prefix).unwrap(),
            "only the caller's actual successful temporary prefix remains"
        );
        drop(temporary);
        {
            let _factory = factory;
        }
        drop(actual);
        assert_eq!(tracker.current(), 0);
    }
    drop(checked);
    drop(compiled);
    assert_eq!(source_tracker.current(), 0);
}
