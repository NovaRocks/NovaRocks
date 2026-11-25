// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{ConnectorSourceFieldFacts, ConnectorSourceInitialDefault, ConnectorSourceSchemaShape};
use novarocks_type_contract::{
    CompilePhase, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::{collections::HashMap, sync::Mutex};
const SOURCE: usize = 32 * 1024 * 1024;
fn pure() -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::pure()
}
fn fresh_attributes(entries: &[(&str, &str)]) -> (HashMap<String, String>, usize) {
    // This test owns the actual one-reserve map creation and every String backing.
    let table = novarocks_type_contract::owned_resources::hashmap::fresh_table_layout::<
        String,
        String,
    >(entries.len())
    .unwrap();
    let mut attributes = HashMap::with_capacity(entries.len());
    let mut source_invoice = table.request_bytes_upper_bound;
    for (key, value) in entries {
        assert!(!attributes.contains_key(*key));
        source_invoice += key.len() + value.len();
        attributes.insert((*key).to_owned(), (*value).to_owned());
    }
    (attributes, source_invoice)
}
fn parts(
    id: i32,
) -> (
    ConnectorReadExecutionSchema,
    ConnectorSourceSchemaFacts,
    FunctionValueType,
) {
    let child = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let carrier = DataType::LargeList(Arc::new(Field::new(
        "source-element",
        DataType::LargeBinary,
        true,
    )));
    let ty = FunctionValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::List(child.value_type().clone()), false, pure()).unwrap(),
        ValueLayout::try_new(&carrier, pure()).unwrap(),
    )
    .unwrap();
    let execution = ConnectorReadExecutionSchema::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "scan-value",
            ty.data_type().clone(),
            false,
        )])),
        vec![ty.clone()].into(),
        pure(),
    )
    .unwrap();
    let shape = ConnectorSourceSchemaShape::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "provider-value",
            carrier,
            false,
        )])),
        pure(),
    )
    .unwrap();
    let default = ConnectorSourceInitialDefault::json(Arc::from("[null]"), pure());
    let text = "原始\0".repeat(300);
    let (field_attributes, field_invoice) = fresh_attributes(&[("provider.opaque", text.as_str())]);
    let (schema_attributes, schema_invoice) = fresh_attributes(&[("source-version", "exact")]);
    let fields = vec![
        ConnectorSourceFieldFacts::try_new_observed(
            vec![0].into(),
            Some(id),
            Some(default),
            field_attributes,
            &["provider.id", "provider.default"],
            pure(),
            field_invoice,
            |_| Ok::<_, ConnectorError>(()),
            |_| Ok(()),
        )
        .unwrap(),
    ]
    .into();
    let source = ConnectorSourceSchemaFacts::try_new_observed_with_source_invoice(
        shape,
        schema_attributes,
        fields,
        pure(),
        schema_invoice,
        |_| Ok::<_, ConnectorError>(()),
        |_| Ok(()),
    )
    .unwrap();
    (execution, source, ty)
}
#[test]
fn public_typed_read_separates_source_identity_from_complete_execution_binding() {
    let (execution, source, ty) = parts(17);
    let first = ConnectorReadPublicFacts::try_new_typed(
        super::super::tests::source(),
        None,
        execution.clone(),
        source,
        pure(),
    )
    .unwrap();
    let (_, other_source, _) = parts(18);
    let second = ConnectorReadPublicFacts::try_new_typed(
        super::super::tests::source(),
        None,
        execution,
        other_source,
        pure(),
    )
    .unwrap();
    assert_ne!(first, second);
    assert_eq!(first.execution_schema(), second.execution_schema());
    assert_eq!(
        first
            .source_schema_facts()
            .unwrap()
            .shape()
            .schema()
            .field(0)
            .name(),
        "provider-value"
    );
    assert_eq!(first.schema().field(0).name(), "scan-value");
    assert!(first.matches_value_type(0, &ty));
    let plain = FunctionValueType::new(ty.data_type().clone(), false);
    assert_eq!(ty.data_type(), plain.data_type());
    assert!(!first.matches_value_type(0, &plain));
    assert!(first.schema().metadata().is_empty());
    assert!(first.schema().field(0).metadata().is_empty());
    assert_eq!(
        first.source_schema_facts().unwrap().field_facts()[0]
            .initial_default()
            .unwrap()
            .as_json(),
        "[null]"
    );
}
#[test]
fn typed_public_facts_allow_zero_columns_without_guessing_reader_business_laws() {
    let execution =
        ConnectorReadExecutionSchema::try_new(Arc::new(Schema::empty()), Box::default(), pure())
            .unwrap();
    let shape = ConnectorSourceSchemaShape::try_new(Arc::new(Schema::empty()), pure()).unwrap();
    let source =
        ConnectorSourceSchemaFacts::try_new(shape, HashMap::new(), Box::default(), pure()).unwrap();
    let facts = ConnectorReadPublicFacts::try_new_typed(
        super::super::tests::source(),
        None,
        execution,
        source,
        pure(),
    )
    .unwrap();
    assert_eq!(facts.execution_schema().unwrap().bindings().len(), 0);
    assert_eq!(
        facts
            .source_schema_facts()
            .unwrap()
            .shape()
            .schema()
            .fields()
            .len(),
        0
    );
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl novarocks_type_contract::PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop);
        }
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn invoke(
    control: &Control,
    admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
) -> Result<ConnectorReadPublicFacts, PureProviderCompileError<ConnectorError>> {
    let (execution, source, _) = parts(17);
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let result = ConnectorReadPublicFacts::try_new_typed_observed(
        super::super::tests::source(),
        None,
        &execution,
        &source,
        pure(),
        SOURCE,
        admit,
        &mut work,
    );
    match result {
        Err(PureProviderCompileError::Control(cause)) => {
            Err(PureProviderCompileError::Control(cause))
        }
        other => {
            work.finish()?;
            other
        }
    }
}
#[test]
fn prepared_source_retained_and_real_new_projection_requests_preserve_all_refusal_prefixes() {
    let control = Control::default();
    let mut last = WriterOwnedResourceFacts::default();
    invoke(&control, &mut |facts| {
        last = *facts;
        Ok(())
    })
    .unwrap();
    assert_eq!(last.source_retained_bytes, SOURCE);
    assert_eq!(last.coexistence_bytes, SOURCE + last.requested_bytes);
    assert!(last.requested_bytes > 0);
    assert!(last.source_floor > 0);
    assert!(last.source_floor <= SOURCE);
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.iter().any(|units| *units > 0));
    for stop in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                stop: Some((stop, cause.clone())),
            };
            assert!(
                matches!(invoke(&control,&mut |_|Ok(())),Err(PureProviderCompileError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    for axis in 0..4 {
        let bound = match axis {
            0 => last.allocation_requests,
            1 => last.requested_bytes,
            2 => last.coexistence_bytes,
            _ => last.work_units,
        };
        for (cap, pass) in [(bound, true), (bound - 1, false)] {
            let result = invoke(&Control::default(), &mut |facts| {
                let actual = match axis {
                    0 => facts.allocation_requests,
                    1 => facts.requested_bytes,
                    2 => facts.coexistence_bytes,
                    _ => facts.work_units,
                };
                if actual > cap {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            });
            assert_eq!(result.is_ok(), pass);
            if !pass {
                assert!(matches!(
                    result,
                    Err(PureProviderCompileError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
            }
        }
    }
}

#[test]
fn pure_typed_publication_keeps_complete_semantics_and_every_original_control_prefix() {
    let (execution, source, ty) = parts(17);
    let invoke = |control: &Control| {
        ConnectorReadPublicFacts::try_new_typed_with_control(
            super::super::tests::source(),
            None,
            &execution,
            &source,
            control,
        )
    };
    let control = Control::default();
    let value = invoke(&control).unwrap();
    assert!(value.matches_value_type(0, &ty));
    assert_eq!(value.source_schema_facts(), Some(&source));
    let expected = control.trace.into_inner().unwrap();
    assert!(expected.iter().any(|units| *units > 0));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for stop in 0..expected.len() {
            let control = Control {
                stop: Some((stop, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(invoke(&control),Err(PureProviderCompileError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
        }
    }
}

/// These fixture creators retain only finite construction responsibility.
/// They contain no query/provider service and introduce no FE quota.
struct CreatorHold(Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for CreatorHold {}
impl Drop for CreatorHold {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn typed_public_creator_first_refusal_keeps_borrowed_host_cause_and_control_prefix() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct PrefixControl {
        refused: AtomicBool,
        before: AtomicUsize,
        after: AtomicUsize,
    }
    impl novarocks_type_contract::PureCompileControl for PrefixControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            assert!(units <= 256);
            if self.refused.load(Ordering::SeqCst) {
                self.after.fetch_add(1, Ordering::SeqCst);
                Err(CompileControlError::Cancelled)
            } else {
                self.before.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }
    #[derive(Debug)]
    struct HostRefusal<'a>(&'a str);
    let control = PrefixControl {
        refused: AtomicBool::new(false),
        before: AtomicUsize::new(0),
        after: AtomicUsize::new(0),
    };
    // A borrowed nominal cause cannot be hidden behind a 'static boxed error
    // or reconstructed from diagnostic text without breaking this identity.
    let diagnostic = String::from("original host creator refusal");
    let mut requests = 0usize;
    let mut exact = |layout: std::alloc::Layout| {
        assert!(layout.size() > 0);
        requests += 1;
        control.refused.store(true, Ordering::SeqCst);
        Err(HostRefusal(diagnostic.as_str()))
    };
    let mut opaque = |_: usize| Ok::<_, HostRefusal<'_>>(());
    let mut creation =
        novarocks_type_contract::ValueSchemaCreation::new(pure(), &mut exact, &mut opaque);
    let (execution, source, _) = parts(17);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let result = ConnectorReadPublicFacts::try_new_typed_with_creator(
        super::super::tests::source(),
        None,
        &execution,
        &source,
        SOURCE,
        &mut |_| Ok(()),
        &mut creation,
        &mut work,
    );
    let Err(TypedReadCreationFailure::Creator(HostRefusal(actual))) = result else {
        panic!("the first host refusal must remain its original nominal cause");
    };
    assert_eq!(actual.as_ptr(), diagnostic.as_ptr());
    assert_eq!(actual.len(), diagnostic.len());
    assert!(control.before.load(Ordering::SeqCst) > 0);
    assert_eq!(control.after.load(Ordering::SeqCst), 0);
    drop(creation);
    assert_eq!(requests, 1);
}

#[test]
fn last_typed_public_reference_keeps_created_labels_and_original_schema_independently() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let original_drops = Arc::new(AtomicUsize::new(0));
    let created_drops = Arc::new(AtomicUsize::new(0));
    let (input, source, ty) = parts(17);
    let execution = ConnectorReadExecutionSchema::try_new(
        Arc::clone(input.schema_ref()),
        input.bindings().to_vec().into_boxed_slice(),
        ValueTypeStorageLifetime::held(Arc::new(CreatorHold(original_drops.clone()))),
    )
    .unwrap();
    drop(input);
    let control = Control::default();
    let mut layouts = Vec::new();
    let mut exact = |layout: std::alloc::Layout| {
        layouts.push(layout);
        Ok::<_, std::convert::Infallible>(())
    };
    let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(Arc::new(CreatorHold(created_drops.clone()))),
        &mut exact,
        &mut opaque,
    );
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
    let facts = ConnectorReadPublicFacts::try_new_typed_with_creator(
        super::super::tests::source(),
        None,
        &execution,
        &source,
        SOURCE,
        &mut |_| Ok(()),
        &mut creation,
        &mut work,
    )
    .unwrap();
    let retained = facts.clone();
    // This is the actual immutable label allocation, shared by public clones;
    // it is a purpose projection, while the full binding keeps nested JSON.
    assert!(Arc::ptr_eq(&facts.logical_types, &retained.logical_types));
    assert!(retained.matches_value_type(0, &ty));
    assert_eq!(retained.logical_types(), &[ValueLogicalType::Physical]);
    let independent_execution = retained.execution_schema().unwrap().clone();
    drop(execution);
    drop(source);
    drop(creation);
    assert!(layouts.iter().any(|layout| layout.size() > 0));
    drop(facts);
    assert_eq!(created_drops.load(Ordering::SeqCst), 0);
    assert_eq!(original_drops.load(Ordering::SeqCst), 0);
    assert_eq!(retained.schema().field(0).name(), "scan-value");
    assert_eq!(
        retained
            .source_schema_facts()
            .unwrap()
            .shape()
            .schema()
            .field(0)
            .name(),
        "provider-value"
    );
    assert!(retained.matches_value_type(0, &ty));
    drop(retained);
    // Releasing labels must not depend on a different, independently retained
    // source execution projection. That projection keeps only its own creator.
    assert_eq!(created_drops.load(Ordering::SeqCst), 1);
    assert_eq!(original_drops.load(Ordering::SeqCst), 0);
    assert_eq!(independent_execution.schema().field(0).name(), "scan-value");
    assert_eq!(independent_execution.binding(0), Some(&ty));
    drop(independent_execution);
    assert_eq!(original_drops.load(Ordering::SeqCst), 1);
}
