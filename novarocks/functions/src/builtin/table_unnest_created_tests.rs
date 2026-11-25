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

//! Installed UNNEST binding, effects and selected table lifecycle owner.
use crate::{
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionKind,
    FunctionResultType,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueKind,
    ValueLayout, ValueLogicalType, ValueSchemaCreationScope, ValueTypeRef, ValueTypeStorageHold,
    ValueTypeStorageLifetime,
};
use std::{
    alloc::Layout,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push((phase, units));
        match self.stop.clone() {
            Some((at, cause)) if calls.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Cause;
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Exact(usize, usize),
    Opaque(usize),
}
#[derive(Default)]
struct Facts {
    events: Mutex<Vec<Event>>,
    held: AtomicUsize,
    exits: AtomicUsize,
}
struct Hold(Arc<Facts>);
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.held.store(0, Ordering::SeqCst);
        self.0.exits.fetch_add(1, Ordering::SeqCst);
    }
}
struct Scope {
    hold: Arc<Hold>,
    refuse: Option<usize>,
    cause: Arc<Cause>,
}
impl Scope {
    fn new(refuse: Option<usize>) -> (Arc<Facts>, Self) {
        let facts = Arc::new(Facts::default());
        (
            facts.clone(),
            Self {
                hold: Arc::new(Hold(facts)),
                refuse,
                cause: Arc::new(Cause),
            },
        )
    }
    fn admit(&mut self, event: Event, bytes: usize) -> Result<(), Arc<Cause>> {
        let mut events = self.hold.0.events.lock().unwrap();
        events.push(event);
        if self.refuse == Some(events.len()) {
            return Err(self.cause.clone());
        }
        let prior = self.hold.0.held.load(Ordering::SeqCst);
        let next = prior
            .checked_add(bytes)
            .filter(|n| *n <= 65536)
            .ok_or_else(|| self.cause.clone())?;
        self.hold.0.held.store(next, Ordering::SeqCst);
        Ok(())
    }
}
impl ValueSchemaCreationScope<Arc<Cause>> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.hold.clone())
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), Arc<Cause>> {
        self.admit(Event::Exact(layout.size(), layout.align()), layout.size())
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), Arc<Cause>> {
        self.admit(Event::Opaque(bytes), bytes)
    }
}
fn arguments() -> [FunctionArgument; 1] {
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        ValueKind::Nominal(ValueLogicalType::Json),
        false,
        storage.clone(),
    )
    .unwrap();
    let semantic = ValueTypeRef::try_new(ValueKind::List(item), true, storage.clone()).unwrap();
    let layout = ValueLayout::try_new(
        &DataType::List(Arc::new(Field::new("item", DataType::LargeBinary, false))),
        storage,
    )
    .unwrap();
    [FunctionArgument::Value {
        value_type: FunctionValueType::try_from_value(semantic, layout).unwrap(),
        constant: None,
    }]
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}
#[test]
fn unnest_created_validation_retains_original_json_child_and_temporary_exit() {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = arguments();
    let control = Control::default();
    let bound = catalog
        .resolve_bound_user("unnest", FunctionKind::Table, request(&args), &control)
        .unwrap();
    let FunctionResultType::Relation(result) = &bound.selected.result_type else {
        panic!("relation")
    };
    assert_eq!(result[0].logical_type(), ValueLogicalType::Json);
    assert!(result[0].nullable());
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &control,
        )
        .unwrap();
    let (facts, mut scope) = Scope::new(None);
    catalog
        .validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &mut scope,
            &control,
        )
        .unwrap();
    let events = facts.events.lock().unwrap().clone();
    assert!(
        matches!(events.first(),Some(Event::Exact(bytes,_)) if *bytes==std::mem::size_of::<crate::FunctionArgumentType>())
    );
    assert!(
        matches!(events.get(1),Some(Event::Exact(bytes,_)) if *bytes==std::mem::size_of::<FunctionValueType>())
    );
    assert!(events.iter().any(|event| matches!(event,Event::Exact(bytes,_) if *bytes==novarocks_type_contract::FunctionArgumentTypes::storage_allocation_layout().unwrap().size())));
    assert!(events.iter().any(|event| matches!(event,Event::Exact(bytes,_) if *bytes==novarocks_type_contract::FunctionParameterTypes::storage_allocation_layout().unwrap().size())));
    assert!(facts.held.load(Ordering::SeqCst) > 0);
    drop(scope);
    assert_eq!(facts.held.load(Ordering::SeqCst), 0);
    assert_eq!(facts.exits.load(Ordering::SeqCst), 1);
    assert_eq!(result[0].logical_type(), ValueLogicalType::Json);
    assert!(result[0].nullable());
}
#[test]
fn unnest_created_validation_preserves_every_first_h_and_control_prefix() {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = arguments();
    let bound = catalog
        .resolve_bound_user(
            "unnest",
            FunctionKind::Table,
            request(&args),
            &Control::default(),
        )
        .unwrap();
    let (facts, mut scope) = Scope::new(None);
    let control = Control::default();
    catalog
        .validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &mut scope,
            &control,
        )
        .unwrap();
    let events = facts.events.lock().unwrap().clone();
    let calls = control.calls.lock().unwrap().clone();
    drop(scope);
    assert!(events.len() > 2);
    for at in 1..=events.len() {
        let (facts, mut scope) = Scope::new(Some(at));
        let expected = scope.cause.clone();
        let control = Control::default();
        match catalog.validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request(&args),
            &mut scope,
            &control,
        ) {
            Err(crate::FunctionBindingCreationFailure::Creator(actual)) => {
                assert!(Arc::ptr_eq(&actual, &expected))
            }
            other => panic!("first H lost: {other:?}"),
        }
        assert_eq!(*facts.events.lock().unwrap(), events[..at]);
        let observed = control.calls.lock().unwrap().clone();
        assert_eq!(observed, calls[..observed.len()]);
        assert!(observed.len() < calls.len());
        drop(scope);
        assert_eq!(facts.held.load(Ordering::SeqCst), 0);
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=calls.len() {
            let (facts, mut scope) = Scope::new(None);
            let control = Control {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(catalog.validate_frozen_selection_with_creator_in(&bound.function_id,bound.kind,&bound.selected,request(&args),&mut scope,&control),Err(crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(actual))) if actual==cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
            let observed = facts.events.lock().unwrap().clone();
            assert_eq!(observed, events[..observed.len()]);
            drop(scope);
            assert_eq!(facts.held.load(Ordering::SeqCst), 0);
        }
    }
}
#[test]
fn unnest_created_validation_rejects_foreign_identity_arity_and_same_carrier_domain() {
    let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
    let args = arguments();
    let bound = catalog
        .resolve_bound_user(
            "unnest",
            FunctionKind::Table,
            request(&args),
            &Control::default(),
        )
        .unwrap();
    let mut wrong = bound.selected.clone();
    wrong.overload = crate::FunctionOverloadId::try_new("foreign/unnest/v1").unwrap();
    let (facts, mut scope) = Scope::new(Some(1));
    assert!(matches!(
        catalog.validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &wrong,
            request(&args),
            &mut scope,
            &Control::default()
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::UnknownOverload(_)
        ))
    ));
    assert!(facts.events.lock().unwrap().is_empty());
    let (facts, mut scope) = Scope::new(Some(1));
    let bad = FunctionBindingRequest {
        logical_argument_count: 0,
        ..request(&args)
    };
    assert!(matches!(
        catalog.validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            bad,
            &mut scope,
            &Control::default()
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(_))
    ));
    assert!(facts.events.lock().unwrap().is_empty());
    let mut wrong = bound.selected.clone();
    wrong.result_type = FunctionResultType::Relation(
        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::from([
            FunctionValueType::new(DataType::LargeBinary, true),
        ])),
    );
    let (_, mut scope) = Scope::new(None);
    assert!(matches!(
        catalog.validate_frozen_selection_with_creator_in(
            &bound.function_id,
            bound.kind,
            &wrong,
            request(&args),
            &mut scope,
            &Control::default()
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
}
