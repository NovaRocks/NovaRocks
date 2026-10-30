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

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use novarocks_types::schema::{ColumnDef, SqlType};

use super::{helpers, scope::AnalyzerScope};
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::column_id::ColumnRefFactory;

type Trace = Vec<(CompilePhase, u32)>;

#[derive(Default)]
struct Control {
    calls: Mutex<Trace>,
    fail_at: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Trace {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let index = calls.len();
        calls.push((phase, units));
        if let Some((at, cause)) = self.fail_at.clone()
            && at == index
        {
            return Err(cause);
        }
        Ok(())
    }
}

fn column(data_type: DataType, declaration: Option<SqlType>, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: "authored_source".into(),
        data_type,
        nullable,
        write_default: None,
        logical_type: declaration,
    }
}

fn check_prefixes<T>(trace: &Trace, operation: impl Fn(&Control) -> Result<T, AnalyzeError>) {
    assert_eq!(trace.first(), Some(&(CompilePhase::Validate, 0)));
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Validate && *units <= 256)
    );
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                fail_at: Some((at, cause.clone())),
                ..Control::default()
            };
            let error = operation(&control)
                .err()
                .expect("original refusal must win");
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(error.span(), None);
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn column_type_source_domains_and_scope_keep_complete_authored_fields() {
    for (carrier, declaration, logical) in [
        (DataType::Utf8, Some(SqlType::Json), ValueLogicalType::Json),
        (
            DataType::FixedSizeBinary(16),
            Some(SqlType::LargeInt),
            ValueLogicalType::LargeInt,
        ),
        (
            DataType::FixedSizeBinary(16),
            None,
            ValueLogicalType::Physical,
        ),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
            None,
            ValueLogicalType::Physical,
        ),
    ] {
        for nullable in [false, true] {
            let source = column(carrier.clone(), declaration.clone(), nullable);
            let control = Control::default();
            let result = helpers::column_value_type(&source, &control).unwrap();
            assert_eq!(result.data_type().clone(), carrier);
            assert_eq!(result.logical_type(), logical);
            assert_eq!(result.nullable(), nullable);
            check_prefixes(&control.trace(), |control| {
                helpers::column_value_type(&source, control)
            });

            let mut scope = AnalyzerScope::new(Rc::new(RefCell::new(ColumnRefFactory::new())));
            let id = scope
                .add_table_column(Some("source_alias"), &source, &Control::default())
                .unwrap();
            let (resolved_id, resolved) = scope
                .resolve_value_type(Some("SOURCE_ALIAS"), "AUTHORED_SOURCE")
                .unwrap();
            assert_eq!(resolved_id, id);
            assert_eq!(resolved, result);
        }
    }

    let item = Field::new("provider_item", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "provider".into(),
            "kept-exactly".into(),
        )]),
    );
    let source = column(
        DataType::LargeList(Arc::new(item)),
        Some(SqlType::Array(Box::new(SqlType::Json))),
        true,
    );
    let result = helpers::column_value_type(&source, &Control::default()).unwrap();
    let DataType::LargeList(item) = result.data_type() else {
        panic!("preserve LargeList carrier");
    };
    assert_eq!(item.name(), "item");
    assert!(!item.is_nullable());
    assert!(item.metadata().as_arc().is_none());
    assert_eq!(
        result.child_at(0).unwrap().logical_type(),
        ValueLogicalType::Json
    );
    let DataType::LargeList(source_item) = &source.data_type else {
        panic!("source List")
    };
    assert_eq!(source_item.name(), "provider_item");
    assert_eq!(source_item.metadata()["provider"], "kept-exactly");
    assert_eq!(result.logical_type(), ValueLogicalType::Physical);
    assert!(result.nullable());
}

#[test]
fn column_type_invalid_catalog_declaration_observes_ordinary_tail_without_publication() {
    let source = column(DataType::FixedSizeBinary(16), Some(SqlType::Json), false);
    let control = Control::default();
    let error = helpers::column_value_type(&source, &control).unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::Internal);
    assert_eq!(
        error.message(),
        "invalid catalog column `authored_source`: invalid Arrow carrier for Json"
    );
    assert_eq!(error.span(), None);
    assert_eq!(error.control_error(), None);
    let trace = control.trace();
    assert_eq!(trace.len(), 2);
    assert!(trace.last().unwrap().1 > 0);
    check_prefixes(&trace, |control| {
        helpers::column_value_type(&source, control)
    });

    let mut scope = AnalyzerScope::new(Rc::new(RefCell::new(ColumnRefFactory::new())));
    let error = scope
        .add_table_column(Some("source_alias"), &source, &Control::default())
        .unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::Internal);
    assert!(
        scope
            .resolve_value_type(Some("source_alias"), "authored_source")
            .is_err()
    );
}

#[test]
fn column_type_full_value_validator_preserves_early_and_completed_ordinary_tails() {
    let valid = FunctionValueType::try_from_legacy_declared_projection(
        DataType::Utf8,
        false,
        ValueLogicalType::Json,
    )
    .expect("valid temporary legacy value projection");
    let control = Control::default();
    helpers::validate_value_type(&valid, &control).unwrap();
    assert!(control.trace().last().unwrap().1 > 0);
    check_prefixes(&control.trace(), |control| {
        helpers::validate_value_type(&valid, control)
    });

    // An invalid binding can no longer exist behind the checked public
    // wrapper. Refuse it at its actual author, before helper publication.
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Json,
        )
        .is_err()
    );
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::Decimal128(0, 0),
            false,
            ValueLogicalType::Physical,
        )
        .is_err()
    );
}

#[test]
fn column_type_wide_real_fields_observe_quantum_and_original_control_prefixes() {
    let fields = Fields::from(
        (0..320)
            .map(|index| {
                Field::new(format!("source_{index}"), DataType::Int64, index % 2 == 0)
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        "provider".into(),
                        format!("field-{index}"),
                    )]))
            })
            .collect::<Vec<_>>(),
    );
    let source = column(DataType::Struct(fields.clone()), None, true);
    let control = Control::default();
    let value = helpers::column_value_type(&source, &control).unwrap();
    let DataType::Struct(actual) = value.data_type() else {
        panic!("execution Struct")
    };
    assert_eq!(actual.len(), fields.len());
    for (actual, source) in actual.iter().zip(fields.iter()) {
        assert_eq!(actual.name(), source.name());
        assert_eq!(actual.data_type(), source.data_type());
        assert_eq!(actual.is_nullable(), source.is_nullable());
        assert!(actual.metadata().as_arc().is_none());
        assert!(source.metadata().contains_key("provider"));
    }
    assert_eq!(value.logical_type(), ValueLogicalType::Physical);
    assert!(value.nullable());
    assert!(control.trace().iter().any(|(_, units)| *units == 256));
    check_prefixes(&control.trace(), |control| {
        helpers::column_value_type(&source, control)
    });

    let control = Control::default();
    helpers::validate_value_type(&value, &control).unwrap();
    assert!(control.trace().iter().any(|(_, units)| *units == 256));
    check_prefixes(&control.trace(), |control| {
        helpers::validate_value_type(&value, control)
    });

    // The incompatible Json root is checked only after the actual wide
    // source walk. No guessed carrier or new schema is published on failure.
    let invalid = column(source.data_type, Some(SqlType::Json), true);
    let control = Control::default();
    let error = helpers::column_value_type(&invalid, &control).unwrap_err();
    assert_eq!(error.kind(), AnalyzeErrorKind::Internal);
    assert_eq!(error.control_error(), None);
    assert!(control.trace().iter().any(|(_, units)| *units == 256));
    check_prefixes(&control.trace(), |control| {
        helpers::column_value_type(&invalid, control)
    });
}
