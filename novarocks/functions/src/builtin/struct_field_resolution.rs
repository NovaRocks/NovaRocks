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
//! One identifier resolution author for binding, SQL and unprepared projection.
//! A successful ordinal selects one field; it grants no runtime capability.
use crate::FunctionBindingError;
use arrow_schema::Fields;

pub fn resolve_struct_field_ordinal_observed<E: From<FunctionBindingError>>(
    fields: &Fields,
    name: &str,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<Option<usize>, E> {
    let mut selected = None;
    for (ordinal, field) in fields.iter().enumerate() {
        observe()?;
        if field.name().eq_ignore_ascii_case(name) {
            if selected.is_some() {
                return Err(FunctionBindingError::InvalidBinding(
                    format!("struct field '{name}' is ambiguous").into(),
                )
                .into());
            }
            selected = Some(ordinal);
        }
    }
    Ok(selected)
}

// Only an unprepared direct library projection needs this nominal adapter.
// Public binding/SQL retain FunctionBindingError itself; runtime observer
// refusals bypass this source wrapper and keep their original identity.
#[derive(Debug)]
struct ResolutionSource(FunctionBindingError);
impl std::fmt::Display for ResolutionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for ResolutionSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for ResolutionSource {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(&self.0)
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ResolutionSource {}
pub(super) fn into_kernel(source: FunctionBindingError) -> crate::KernelFailure {
    crate::KernelFailure::from_domain(ResolutionSource(source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;
    #[test]
    fn unique_ascii_case_resolution_has_no_exact_spelling_priority() {
        let fields: Fields = vec![
            Field::new("first", DataType::Int64, false),
            Field::new("Payload", DataType::Utf8, true),
        ]
        .into();
        for spelling in ["payload", "Payload", "PAYLOAD"] {
            assert_eq!(
                resolve_struct_field_ordinal_observed::<FunctionBindingError>(
                    &fields,
                    spelling,
                    || Ok(())
                )
                .unwrap(),
                Some(1)
            );
        }
        assert_eq!(
            resolve_struct_field_ordinal_observed::<FunctionBindingError>(
                &fields,
                "absent",
                || Ok(())
            )
            .unwrap(),
            None
        );
        let colliding: Fields = vec![
            Field::new("ID", DataType::Int32, false),
            Field::new("id", DataType::Int64, true),
        ]
        .into();
        for spelling in ["id", "ID", "Id"] {
            assert!(
                matches!(resolve_struct_field_ordinal_observed::<FunctionBindingError>(&colliding, spelling, || Ok(())),
                Err(FunctionBindingError::InvalidBinding(message)) if message.contains("ambiguous"))
            );
        }
    }
    #[derive(Debug)]
    enum Stop {
        Binding(FunctionBindingError),
        Original(Arc<u32>),
    }
    impl From<FunctionBindingError> for Stop {
        fn from(source: FunctionBindingError) -> Self {
            Self::Binding(source)
        }
    }
    #[test]
    fn unique_resolution_visits_full_candidates_and_preserves_each_original_observer_refusal() {
        let fields: Fields = vec![
            Field::new("first", DataType::Int64, false),
            Field::new("Payload", DataType::Utf8, true),
            Field::new("last", DataType::Int64, false),
        ]
        .into();
        let cause = Arc::new(83);
        for at in 0..fields.len() {
            let mut visits = 0;
            let result = resolve_struct_field_ordinal_observed::<Stop>(&fields, "payload", || {
                visits += 1;
                if visits == at + 1 {
                    Err(Stop::Original(cause.clone()))
                } else {
                    Ok(())
                }
            });
            match result {
                Err(Stop::Original(actual)) => assert!(Arc::ptr_eq(&actual, &cause)),
                Err(Stop::Binding(source)) => panic!("observer source lost: {source:?}"),
                Ok(_) => panic!("observer refusal lost"),
            }
            assert_eq!(visits, at + 1);
        }
    }
}
