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
use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ExprCompileFailure, ExprCreation, ScopeLayouts,
    StaticExprKind, compile_expr_set,
};
use novarocks_expression_contract::RootScope;
use novarocks_local_program::{LocalProgram, ProgramNodeKind, ProgramUnitId};
use novarocks_type_contract::{
    CompileCheckpoints, ExpressionUseId, ValueSchemaCreation, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueSchemaScopeBeginFailure, ValueTypeStorageLifetime,
    owned_resources::layout::arc_layout,
};
use std::{
    alloc::Layout,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

const ONE_IN: &str = "SELECT 1 IN (1, NULL) AS original_in";
const TWO_IN: &str = "SELECT 1 IN (1, NULL) AS original_in, 1 IN (2, NULL) AS unrelated_in";

struct CompiledInSource {
    package: FragmentPackage,
    compiled: CompiledTaskProgram,
    functions: Arc<PureEngineFunctionCatalog>,
    tracker: Arc<MemTracker>,
}

fn compiled_in_source(sql: &str, scratch_len: usize) -> CompiledInSource {
    let (package, _) = sql_fragment_package(sql, false, false);
    let bytes = encode_sql_fixture_package(&package);
    let tracker = MemTracker::new_root("actual expression source");
    let actual = NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
    let scratch_owner = (scratch_len != 0).then(|| {
        let owner = NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
        owner
            .request_layout(Layout::array::<u64>(scratch_len).unwrap())
            .unwrap();
        owner
    });
    let mut scratch = Vec::<u64>::with_capacity(scratch_len);
    scratch.resize(scratch_len, 37);
    let scopes = CompiledTypeScopes::new(&actual);
    let mut exact = |layout| actual.request_layout(layout);
    let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
    let mut creation = ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
    let functions = novarocks_functions::testkit::shared_installed_builtin_owner_catalogue();
    let compiled = interpreter_with_functions(Arc::clone(&functions))
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
    drop(scratch_owner);
    drop(actual);
    CompiledInSource {
        package,
        compiled,
        functions,
        tracker,
    }
}

#[derive(Clone, Copy)]
struct InSite {
    unit: ProgramUnitId,
    use_id: ExpressionUseId,
}
impl InSite {
    fn recipe(
        self,
        program: &LocalProgram,
    ) -> &novarocks_functions::native_inlist::PreparedMembership {
        let unit = program.unit(self.unit).unwrap();
        let novarocks_functions::PreparedPureKernel::Membership(prepared) =
            unit.calls()[&self.use_id].prepared()
        else {
            panic!("selected native membership owner")
        };
        prepared
    }
}

fn actual_in_sites(program: &LocalProgram) -> Vec<InSite> {
    let mut sites = Vec::new();
    for (index, unit) in program.units().iter().enumerate() {
        for (&use_id, invocation) in unit.control_flow().uses() {
            let Some(node) = unit.definitions().node(invocation.definition) else {
                panic!("real definition");
            };
            if invocation.control != novarocks_type_contract::ControlShape::CandidateGroup {
                continue;
            }
            let StaticExprKind::BoundCall { args, .. } = node.kind() else {
                panic!("exact membership call")
            };
            let (probe, members) = args.split_first().expect("probe and ordered candidates");
            assert!(!members.is_empty());
            let site = InSite {
                unit: ProgramUnitId::new(index),
                use_id,
            };
            let selected = site.recipe(program).call().selected();
            let [
                novarocks_functions::FunctionArgumentType::Value(source),
                novarocks_functions::FunctionArgumentType::CandidateGroup { element_type },
            ] = selected.argument_types.as_ref()
            else {
                panic!("exact IN two-slot signature")
            };
            assert_eq!(
                unit.types().get(probe.index()),
                Some(&novarocks_functions::FunctionArgumentType::Value(
                    source.clone()
                ))
            );
            for member in members {
                let novarocks_functions::FunctionArgumentType::Value(actual) = unit
                    .types()
                    .get(member.index())
                    .expect("complete candidate declaration")
                else {
                    panic!("candidate is a value")
                };
                assert!(
                    actual
                        .same_physical_binding_ignoring_root_nullability_observed(
                            element_type,
                            || Ok::<_, std::convert::Infallible>(()),
                        )
                        .unwrap()
                );
            }
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &site.recipe(program).call().selected().result_type
            else {
                panic!("selected membership is scalar");
            };
            assert_eq!(node.value_type(), Some(result));
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
    novarocks_functions::native_inlist::PreparedMembership,
    Arc<MemTracker>,
) {
    let CompiledInSource {
        compiled, tracker, ..
    } = compiled_in_source(sql, scratch);
    let sites = actual_in_sites(compiled.program());
    assert_eq!(
        sites.len(),
        expected_calls,
        "SQL retains the actual installed membership call"
    );
    let late = sites[0].recipe(compiled.program()).clone();
    let active = tracker.current();
    drop(compiled);
    assert!(
        tracker.current() > 0,
        "the independently owned parameter/type holders remain live"
    );
    assert!(
        tracker.current() < active,
        "the retired program releases its independent construction scopes"
    );
    (late, tracker)
}
#[test]
fn actual_host_membership_retains_its_selected_source_and_releases_last_alias() {
    let (minimal, minimal_tracker) = late_in(ONE_IN, 0, 1);
    let (scratch, scratch_tracker) = late_in(ONE_IN, 512, 1);
    let (wide, wide_tracker) = late_in(TWO_IN, 512, 2);
    assert_eq!(
        scratch_tracker.current(),
        minimal_tracker.current(),
        "an independently charged scratch allocation is retired"
    );
    drop(scratch);
    assert_eq!(scratch_tracker.current(), 0);
    assert_eq!(minimal.call().selected(), wide.call().selected());
    // These bindings borrow their exact, potentially shared package type
    // source. Its charge remains while a selected call can still reach it.
    let retained = wide_tracker.current();
    let independent = wide.clone();
    drop(wide);
    assert_eq!(wide_tracker.current(), retained);
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

fn compile_traced_in(
    source: &CompiledInSource,
    site: InSite,
    temporary: &mut TracedTemporary<'_>,
    factory: &TracedFactory<'_, '_>,
) -> Result<Arc<CompiledExprSet>, ExprCompileFailure<NativeTypeConstructionError>> {
    let program = source.compiled.program();
    let compiled = program.unit(site.unit).unwrap();
    let root = compiled.root_of_use(site.use_id).unwrap();
    let physical = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            ProgramNodeKind::Project { exprs, .. }
                if exprs
                    .iter()
                    .any(|candidate| candidate.unit == site.unit && candidate.root == root) =>
            {
                let [physical_source] = node.physical_sources() else {
                    panic!("the actual IN Project has one physical source");
                };
                source
                    .package
                    .fragment()
                    .nodes()
                    .get(&novarocks_physical_plan::NodeId::new(physical_source.get()))
            }
            _ => None,
        })
        .expect("the selected compiled root retains its original SQL Project");
    let bound = physical
        .expressions
        .as_deref()
        .expect("the original SQL IN unit");
    assert_eq!(
        bound.roots()[root.get() as usize].scope,
        compiled.root(root).unwrap().scope
    );
    let original = &bound.control_flow().uses()[&site.use_id];
    let lowered = &compiled.control_flow().uses()[&site.use_id];
    assert_eq!(original.context, lowered.context);
    assert_eq!(
        compiled.compiled_definition(original.definition),
        Some(lowered.definition)
    );
    let mut work = CompileCheckpoints::try_new(factory.trace, CompilePhase::LowerProgram)?;
    let mut retained = factory.begin_scope().map_err(ExprCompileFailure::Creator)?;
    let result = compile_expr_set(
        bound,
        EvaluationEnvironment {
            constants: source.package.constants(),
            parameters: source.package.parameters(),
        },
        ScopeLayouts {
            ports: compiled.scopes(),
            all_ports: if bound.ports().is_empty() {
                None
            } else {
                compiled.scope(RootScope::AllPorts)
            },
        },
        novarocks_functions::ConstantValidation::INPUT_DERIVED,
        &source.functions,
        ExprCreation {
            temporary,
            retained: &mut retained,
            factory,
        },
        &mut work,
    )?;
    work.finish()?;
    Ok(result)
}

#[test]
fn actual_host_expression_all_family_and_final_arc_scopes_exit_independently() {
    let source = compiled_in_source(ONE_IN, 0);
    let sites = actual_in_sites(source.compiled.program());
    assert_eq!(sites.len(), 1);
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
    let unit = compile_traced_in(&source, sites[0], &mut temporary, &factory).unwrap();
    let original_root = source
        .compiled
        .program()
        .unit(sites[0].unit)
        .unwrap()
        .root_of_use(sites[0].use_id)
        .unwrap();
    assert_eq!(unit.root_of_use(sites[0].use_id), Some(original_root));
    let novarocks_functions::PreparedPureKernel::Membership(late_recipe) =
        unit.calls()[&sites[0].use_id].prepared()
    else {
        panic!("selected membership")
    };
    let late_recipe = late_recipe.clone();
    let events = trace.events.lock().unwrap().clone();
    assert_eq!(
        trace.scopes.load(Ordering::SeqCst),
        events.iter().filter(|event| event.kind == "scope").count(),
    );
    assert!(
        events.iter().any(|event| event.kind == "temporary"),
        "the actual checked call and occurrence work uses the original temporary scope"
    );
    let final_arc_bytes = arc_layout(Layout::new::<CompiledExprSet>()).unwrap().size();
    assert!(
        events.last().is_some_and(|event| {
            event.scope == 0 && event.kind == "layout" && event.bytes == final_arc_bytes
        }),
        "the unit's own scope pays for its final Arc"
    );
    let late_unit = Arc::clone(&unit);
    drop(unit);
    let source_tracker = Arc::clone(&source.tracker);
    drop(source);
    drop(temporary);
    {
        let _factory = factory;
    }
    drop(actual);
    let unit_charge = tracker.current();
    assert!(unit_charge > 0);
    drop(late_unit);
    assert!(tracker.current() < unit_charge);
    drop(late_recipe);
    assert_eq!(tracker.current(), 0);
    assert_eq!(source_tracker.current(), 0);
}

fn actual_constructor_events(source: &CompiledInSource, site: InSite) -> Vec<ActualScopeEvent> {
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
    let unit = compile_traced_in(source, site, &mut temporary, &factory).unwrap();
    let events = trace.events.lock().unwrap().clone();
    drop(unit);
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
    let source = compiled_in_source(ONE_IN, 0);
    let sites = actual_in_sites(source.compiled.program());
    assert_eq!(sites.len(), 1);
    let discovered = actual_constructor_events(&source, sites[0]);
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
        let error = compile_traced_in(&source, sites[0], &mut temporary, &factory).unwrap_err();
        let ExprCompileFailure::Creator(original) = error else {
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
    let source_tracker = Arc::clone(&source.tracker);
    drop(source);
    assert_eq!(source_tracker.current(), 0);
}
