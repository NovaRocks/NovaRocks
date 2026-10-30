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
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::{alloc::Layout, collections::HashMap, sync::Mutex};

const SOURCE: usize = 2 * 1024 * 1024;
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if let Some((index, cause)) = self.stop.clone()
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn invoke(
    schema: &Schema,
    logical: Vec<ValueLogicalType>,
    source_bytes: usize,
    control: &Control,
    admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
) -> Result<ConnectorReadPublicFacts, PureProviderCompileError<ConnectorError>> {
    let source = super::tests::source();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let result = ConnectorReadPublicFacts::try_new_from_borrowed_schema_observed(
        source,
        None,
        schema,
        logical,
        source_bytes,
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
fn primitive() -> Schema {
    Schema::new(vec![Field::new("v", DataType::Int64, false)])
}
fn facts(schema: &Schema) -> (ConnectorReadPublicFacts, WriterOwnedResourceFacts) {
    let mut facts = WriterOwnedResourceFacts::default();
    let output = invoke(
        schema,
        vec![ValueLogicalType::Physical; schema.fields().len()],
        SOURCE,
        &Control::default(),
        &mut |known| {
            facts = *known;
            Ok(())
        },
    )
    .unwrap();
    (output, facts)
}
#[test]
fn borrowed_read_schema_detaches_root_nested_dictionary_and_metadata_backing() {
    let mut child_metadata = HashMap::with_capacity(4096);
    child_metadata.insert("opaque".to_owned(), "中国\0".repeat(160));
    #[allow(deprecated)]
    let child = Arc::new(
        Field::new_dict(
            "dict",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            true,
            777,
            true,
        )
        .with_metadata(child_metadata),
    );
    let root = Arc::new(Field::new(
        "root",
        DataType::Struct(vec![child.clone()].into()),
        true,
    ));
    let mut metadata = HashMap::with_capacity(4096);
    metadata.insert("schema".to_owned(), "x".repeat(16 * 1024));
    let schema = Schema::new_with_metadata(vec![root.clone()], metadata);
    let expected = ConnectorReadPublicFacts::try_new(
        super::tests::source(),
        None,
        schema.clone(),
        vec![ValueLogicalType::Physical],
    )
    .unwrap();
    let (actual, invoice) = facts(&schema);
    assert_eq!(actual, expected);
    assert_eq!(actual.charged_bytes(), expected.charged_bytes());
    assert!(!Arc::ptr_eq(&actual.schema().fields()[0], &root));
    let DataType::Struct(fields) = actual.schema().fields()[0].data_type() else {
        panic!()
    };
    assert!(!Arc::ptr_eq(&fields[0], &child));
    assert!(crate::arrow_fields_exact(&fields[0], &child));
    #[allow(deprecated)]
    {
        assert_eq!(fields[0].dict_id(), Some(777));
        assert_eq!(fields[0].dict_is_ordered(), Some(true));
    }
    assert_eq!(actual.schema().metadata()["schema"], "x".repeat(16 * 1024));
    assert!(!Arc::ptr_eq(
        actual.schema().metadata().as_arc().unwrap(),
        schema.metadata().as_arc().unwrap()
    ));
    assert!(!Arc::ptr_eq(
        fields[0].metadata().as_arc().unwrap(),
        child.metadata().as_arc().unwrap()
    ));
    assert_eq!(invoice.coexistence_bytes, SOURCE + invoice.requested_bytes);
}
#[test]
fn borrowed_read_preserves_original_logical_and_structural_error_categories() {
    let bad_label = Field::new("v", DataType::Int64, false).with_metadata(HashMap::from([(
        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
        "unknown-logical-domain".to_owned(),
    )]));
    let schemas = [
        Schema::empty(),
        Schema::new(vec![bad_label]),
        Schema::new_with_metadata(
            vec![Field::new("v", DataType::Int64, false)],
            HashMap::from([("large".to_owned(), "x".repeat(16 * 1024 + 1))]),
        ),
    ];
    for schema in schemas {
        let logical = vec![ValueLogicalType::Physical; schema.fields().len()];
        let expected = ConnectorReadPublicFacts::try_new(
            super::tests::source(),
            None,
            schema.clone(),
            logical.clone(),
        )
        .unwrap_err();
        let actual = invoke(&schema, logical, SOURCE, &Control::default(), &mut |_| {
            Ok(())
        })
        .unwrap_err();
        let PureProviderCompileError::Provider(actual) = actual else {
            panic!()
        };
        assert_eq!(actual.kind(), expected.kind());
        assert_eq!(actual.to_string(), expected.to_string());
    }
    let schema = Schema::new(vec![Field::new("j", DataType::Utf8, true)]);
    let logical = vec![ValueLogicalType::Json];
    let actual = invoke(
        &schema,
        logical.clone(),
        SOURCE,
        &Control::default(),
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(actual.logical_types(), logical);
    assert!(
        actual.matches_value_type(
            0,
            &FunctionValueType::try_from_legacy_declared_projection(
                DataType::Utf8,
                true,
                ValueLogicalType::Json
            )
            .expect("valid temporary legacy value projection")
        )
    );
}
#[test]
fn borrowed_read_primitive_requests_match_independent_layout_inventory() {
    let (_, invoice) = facts(&primitive());
    let arc = |payload: Layout| {
        novarocks_type_contract::owned_resources::layout::arc_layout(payload)
            .unwrap()
            .size()
    };
    let bytes = 16 * 1024 // original fixed diagnostic requests
        + 2 * size_of::<Arc<Field>>() // root Vec and conservative trim
        + arc(Layout::array::<Arc<Field>>(1).unwrap())
        + arc(Layout::new::<Schema>())
        + size_of::<ValueLogicalType>() // original logical Vec trim
        + arc(Layout::array::<ValueLogicalType>(1).unwrap())
        + size_of::<&DataType>() // actual one-node SchemaBudget scratch
        + arc(Layout::new::<Field>())
        + 1; // name "v"
    assert_eq!(invoice.allocation_requests, 25);
    assert_eq!(invoice.requested_bytes, bytes);
    assert_eq!(invoice.coexistence_bytes, SOURCE + bytes);
    assert!(invoice.source_floor > 0);
    assert!(invoice.source_floor <= SOURCE);
}
#[test]
fn borrowed_read_each_request_work_and_coexistence_gate_accepts_exact_and_refuses_under() {
    let schema = primitive();
    let (_, upper) = facts(&schema);
    for axis in 0..4 {
        let amount = match axis {
            0 => upper.allocation_requests,
            1 => upper.requested_bytes,
            2 => upper.coexistence_bytes,
            _ => upper.work_units,
        };
        for (cap, success) in [(amount, true), (amount - 1, false)] {
            let result = invoke(
                &schema,
                vec![ValueLogicalType::Physical],
                SOURCE,
                &Control::default(),
                &mut |known| {
                    let actual = match axis {
                        0 => known.allocation_requests,
                        1 => known.requested_bytes,
                        2 => known.coexistence_bytes,
                        _ => known.work_units,
                    };
                    if actual > cap {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
            );
            if success {
                assert!(result.is_ok());
            } else {
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
fn borrowed_read_small_success_and_ordinary_failures_preserve_every_control_prefix() {
    for schema in [
        primitive(),
        Schema::new_with_metadata(
            vec![Field::new("v", DataType::Int64, false)],
            HashMap::from([("invalid".to_owned(), "x".repeat(16 * 1024 + 1))]),
        ),
    ] {
        let baseline = Control::default();
        let result = invoke(
            &schema,
            vec![ValueLogicalType::Physical],
            SOURCE,
            &baseline,
            &mut |_| Ok(()),
        );
        if schema.metadata().is_empty() {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(PureProviderCompileError::Provider(_))));
        }
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    stop: Some((at, cause.clone())),
                };
                let result = invoke(
                    &schema,
                    vec![ValueLogicalType::Physical],
                    SOURCE,
                    &control,
                    &mut |_| Ok(()),
                );
                assert!(
                    matches!(result, Err(PureProviderCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn borrowed_read_understated_source_invoice_is_not_an_allocation_grant() {
    let control = Control::default();
    let result = invoke(
        &primitive(),
        vec![ValueLogicalType::Physical],
        0,
        &control,
        &mut |_| Ok(()),
    );
    let Err(PureProviderCompileError::Provider(error)) = result else {
        panic!()
    };
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.to_string(),
        invalid("writer retained source invoice is understated").to_string()
    );
    assert_eq!(*control.trace.lock().unwrap(), [0, 0]);
}

#[test]
fn borrowed_read_known_request_refusal_precedes_pending_255_late_control() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            trace: Mutex::new(vec![]),
            stop: Some((1, cause)),
        };
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let result = ConnectorReadPublicFacts::try_new_from_borrowed_schema_observed(
            super::tests::source(),
            None,
            &primitive(),
            vec![ValueLogicalType::Physical],
            SOURCE,
            &mut |known| {
                if known.allocation_requests > 16 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(PureProviderCompileError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(*control.trace.lock().unwrap(), [0]);
    }
}
#[test]
fn borrowed_read_wide_real_string_copy_observes_quantum_without_schema_retagging() {
    let mut value = "中国\0".repeat(100);
    value.push('x');
    let schema = Schema::new_with_metadata(
        vec![Field::new("v", DataType::Int64, true)],
        HashMap::from([("opaque".to_owned(), value.clone())]),
    );
    let control = Control::default();
    let actual = invoke(
        &schema,
        vec![ValueLogicalType::Physical],
        SOURCE,
        &control,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(actual.schema().metadata()["opaque"], value);
    let trace = control.trace.lock().unwrap().clone();
    let quantum = trace.iter().position(|units| *units == 256).unwrap();
    for at in [0, quantum, trace.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(vec![]),
                stop: Some((at, cause.clone())),
            };
            let result = invoke(
                &schema,
                vec![ValueLogicalType::Physical],
                SOURCE,
                &control,
                &mut |_| Ok(()),
            );
            assert!(
                matches!(result, Err(PureProviderCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
