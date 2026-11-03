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

//! One schema-binding algorithm for legacy equality writers and pure preparation.

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use super::domain::IcebergEqualityDeleteRecipe;
use crate::iceberg::arrow::schema_to_arrow_schema;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct EqualityDeleteFileTarget {
    pub arrow_schema: SchemaRef,
}

#[derive(Debug)]
pub(crate) enum EqualitySchemaError {
    Source(ConnectorError),
    Control(CompileControlError),
}
impl std::fmt::Display for EqualitySchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => std::fmt::Display::fmt(error, f),
            Self::Control(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for EqualitySchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}
impl From<CompileControlError> for EqualitySchemaError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

struct Work<'a>(Option<CompileCheckpoints<'a>>);
impl Work<'_> {
    fn step(&mut self) -> Result<(), EqualitySchemaError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), EqualitySchemaError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, EqualitySchemaError> {
        self.flush()?;
        let result = operation();
        self.flush()?;
        Ok(result)
    }
    fn equal(&mut self, left: &str, right: &str) -> Result<bool, EqualitySchemaError> {
        let same_length = left.len() == right.len();
        self.step()?;
        if !same_length {
            return Ok(false);
        }
        for (left, right) in left
            .as_bytes()
            .chunks(1024)
            .zip(right.as_bytes().chunks(1024))
        {
            let equal = left == right;
            self.step()?;
            if !equal {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn append(&mut self, output: &mut String, text: &str) -> Result<(), EqualitySchemaError> {
        let mut position = 0;
        while position < text.len() {
            let mut end = text.len().min(position.saturating_add(1024));
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            output.push_str(&text[position..end]);
            position = end;
            self.step()?;
        }
        Ok(())
    }
}

/// Bind the complete frozen target projection before opening storage.
pub(crate) fn equality_delete_target_from_recipe(
    recipe: &IcebergEqualityDeleteRecipe,
    expected_schema: &SchemaRef,
) -> Result<EqualityDeleteFileTarget, ConnectorError> {
    match resolve_core(recipe, expected_schema, &mut Work(None)) {
        Ok(target) => Ok(target),
        Err(EqualitySchemaError::Source(error)) => Err(error),
        Err(EqualitySchemaError::Control(_)) => {
            unreachable!("runtime binding has no compile control")
        }
    }
}
pub(crate) fn equality_delete_target_for_compile(
    recipe: &IcebergEqualityDeleteRecipe,
    expected_schema: &SchemaRef,
    control: &dyn PureCompileControl,
) -> Result<EqualityDeleteFileTarget, EqualitySchemaError> {
    let mut work = Work(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::ProviderValidation,
    )?));
    let result = resolve_core(recipe, expected_schema, &mut work);
    if matches!(&result, Err(EqualitySchemaError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}
fn invalid(message: impl Into<String>) -> EqualitySchemaError {
    EqualitySchemaError::Source(ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        message,
    ))
}
fn resolve_core(
    recipe: &IcebergEqualityDeleteRecipe,
    expected_schema: &SchemaRef,
    work: &mut Work<'_>,
) -> Result<EqualityDeleteFileTarget, EqualitySchemaError> {
    let same_count = recipe.columns().len() == expected_schema.fields().len();
    work.step()?;
    if !same_count {
        return Err(invalid(work.opaque(|| {
            format!(
                "Iceberg equality-delete handle names {} columns but its fragment input carries {}",
                recipe.columns().len(),
                expected_schema.fields().len()
            )
        })?));
    }
    // Observe JSON as one finite library call, preserving the original control.
    let target: crate::iceberg::spec::Schema = work
        .opaque(|| crate::metadata_json::from_str(recipe.field_schema_json()))?
        .map_err(|error| {
            invalid(format!(
                "decode frozen Iceberg equality-delete schema: {error}"
            ))
        })?;
    let fields = target.as_struct().fields();
    if fields.len() != recipe.columns().len() {
        return Err(invalid(
            "Iceberg equality-delete schema projection has a different column count",
        ));
    }
    for ((frozen, actual), target) in recipe
        .columns()
        .iter()
        .zip(expected_schema.fields())
        .zip(fields)
    {
        if !work.equal(frozen.name(), actual.name())?
            || !work.opaque(|| frozen.name().eq_ignore_ascii_case(&target.name))?
            || frozen.field_id() != target.id
        {
            let mut message = String::new();
            work.append(&mut message, "Iceberg equality-delete column `")?;
            work.append(&mut message, frozen.name())?;
            work.append(&mut message, "` does not match fragment input `")?;
            work.append(&mut message, actual.name())?;
            work.append(&mut message, "`")?;
            return Err(invalid(message));
        }
        work.opaque(|| crate::commit::frozen_write::validate_writer_type(&target.field_type))?
            .map_err(invalid)?;
        work.step()?;
    }
    let arrow_schema = Arc::new(work.opaque(|| schema_to_arrow_schema(&target))?.map_err(
        |error| {
            invalid(format!(
                "convert frozen Iceberg equality-delete schema: {error}"
            ))
        },
    )?);
    work.opaque(|| {
        crate::commit::write_adaptation::validate_batch_schema(expected_schema, &arrow_schema)
    })?
    .map_err(invalid)?;
    Ok(EqualityDeleteFileTarget { arrow_schema })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::write_stack::domain::IcebergEqualityDeleteColumnFacts;
    use arrow::datatypes::{DataType, Schema};
    use std::sync::Mutex;
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((at, error)) = self.refusal.clone()
                && calls.len() == at
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn control() -> Control {
        Control {
            calls: Mutex::new(vec![]),
            refusal: None,
        }
    }
    fn fixture(ty: serde_json::Value) -> (IcebergEqualityDeleteRecipe, SchemaRef) {
        let json = serde_json::json!({"type":"struct", "schema-id":42, "fields":[{"id":77,"name":"key","required":false,"type":ty}]}).to_string();
        (
            IcebergEqualityDeleteRecipe::try_new(
                vec![IcebergEqualityDeleteColumnFacts::try_new("key".into(), 77).unwrap()],
                json.clone(),
            )
            .unwrap(),
            {
                let frozen: crate::iceberg::spec::Schema = serde_json::from_str(&json).unwrap();
                Arc::new(schema_to_arrow_schema(&frozen).unwrap())
            },
        )
    }
    #[test]
    fn equality_target_uses_frozen_type_and_field_id() {
        let (recipe, input) = fixture(serde_json::json!("timestamptz"));
        let target = equality_delete_target_for_compile(&recipe, &input, &control()).unwrap();
        assert_eq!(
            target,
            equality_delete_target_from_recipe(&recipe, &input).unwrap()
        );
        assert_eq!(
            target
                .arrow_schema
                .field(0)
                .metadata()
                .get("PARQUET:field_id")
                .unwrap(),
            "77"
        );
        assert_eq!(
            target.arrow_schema.field(0).data_type(),
            &DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("+00:00".into())
            )
        );
    }
    #[test]
    fn equality_target_rejects_variant_uuid_and_missing_projection() {
        for ty in [serde_json::json!("variant"), serde_json::json!("uuid")] {
            let (recipe, input) = fixture(ty);
            assert!(
                equality_delete_target_from_recipe(&recipe, &input)
                    .unwrap_err()
                    .message()
                    .contains("does not support writing")
            );
        }
        let (recipe, _) = fixture(serde_json::json!("long"));
        assert!(equality_delete_target_from_recipe(&recipe, &Arc::new(Schema::empty())).is_err());
    }
    #[test]
    fn equality_target_original_control_refuses_entry_and_every_later_checkpoint() {
        let (recipe, input) = fixture(serde_json::json!("long"));
        let accepted = control();
        equality_delete_target_for_compile(&recipe, &input, &accepted).unwrap();
        let count = accepted.calls.lock().unwrap().len();
        for at in 1..=count {
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((at, error.clone())),
                    ..control()
                };
                assert!(
                    matches!(equality_delete_target_for_compile(&recipe,&input,&control),Err(EqualitySchemaError::Control(actual)) if actual==error)
                );
                assert_eq!(control.calls.lock().unwrap().len(), at);
            }
        }
    }
}
