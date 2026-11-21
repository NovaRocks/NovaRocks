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

//! A complete frozen read and its sealed pure provider compilation result.
use crate::{
    ConnectorError, ConnectorErrorKind, ConnectorReadPublicFacts, ConnectorReadRelationKind,
    ConnectorReadRelationRecipe, ConnectorReadRelationRecipeDraft, FrozenConnectorScan,
    MAX_STATIC_SCAN_RETAINED_BYTES, PureProviderCompileError, StaticConnectorScanError,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::{error::Error, fmt};

/// Reuses the scan's assignments, predicate/RF algebra and work-source facts.
/// The source facts supply the exact public schema, version and selection;
/// there is no second scan language or runtime handle in this type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenConnectorRead {
    scan: FrozenConnectorScan,
    public: ConnectorReadPublicFacts,
    retained_bytes: usize,
}
impl FrozenConnectorRead {
    pub fn try_new(
        scan: FrozenConnectorScan,
        public: ConnectorReadPublicFacts,
    ) -> Result<Self, ConnectorError> {
        if scan.assignments().len() != public.schema().fields().len()
            || !scan
                .assignments()
                .iter()
                .zip(public.schema().fields())
                .zip(public.logical_types())
                .all(|((assignment, field), logical)| {
                    crate::connector_type_accepts_arrow(assignment.value_type(), field.data_type())
                        && (assignment.value_type() == crate::ConnectorValueType::Uuid)
                            == (*logical == ValueLogicalType::Uuid)
                })
        {
            return Err(invalid(
                "read assignment differs from its complete public schema ordinal",
            ));
        }
        if scan.recipe().relation().kind() == ConnectorReadRelationKind::SystemTable
            && public.metadata_kind().is_none()
        {
            return Err(invalid("system-table read has no frozen metadata kind"));
        }
        let retained_bytes = scan
            .retained_bytes()
            .checked_add(public.charged_bytes())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or_else(exhausted)?;
        if retained_bytes > MAX_STATIC_SCAN_RETAINED_BYTES {
            return Err(exhausted());
        }
        Ok(Self {
            scan,
            public,
            retained_bytes,
        })
    }
    pub const fn scan(&self) -> &FrozenConnectorScan {
        &self.scan
    }
    pub const fn public_facts(&self) -> &ConnectorReadPublicFacts {
        &self.public
    }
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// The exact installed pure provider definition validates private facts against
/// the entire frozen input. It may canonicalize private bytes only; the public
/// source and scan facts are borrowed and cannot be replaced by this result.
/// Decode, canonicalization and copying loops must observe bounded work through
/// control; interruption returns Control unchanged, never a Provider diagnostic.
pub trait ConnectorReadProgramCompiler: Send + Sync {
    type Error: Error;
    fn compile_private(
        &self,
        frozen: &FrozenConnectorRead,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<Self::Error>>;
}

#[derive(Debug)]
pub enum ConnectorReadProgramCompileError<E: Error> {
    Contract(ConnectorError),
    Provider(E),
    Control(CompileControlError),
}
impl<E: Error> fmt::Display for ConnectorReadProgramCompileError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(e) => fmt::Display::fmt(e, f),
            Self::Provider(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
        }
    }
}
impl<E: Error + 'static> Error for ConnectorReadProgramCompileError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            Self::Provider(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadProgramRecipe(FrozenConnectorRead);
impl ConnectorReadProgramRecipe {
    /// The only sealing path requires complete-input provider validation. The
    /// legacy payload-only recipe seal cannot construct this compiled result.
    pub fn try_compile_with_provider<C: ConnectorReadProgramCompiler + ?Sized>(
        frozen: &FrozenConnectorRead,
        compiler: &C,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ConnectorReadProgramCompileError<C::Error>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)
            .map_err(ConnectorReadProgramCompileError::Control)?;
        let result = (|| {
            work.flush()
                .map_err(ConnectorReadProgramCompileError::Control)?;
            let canonical = compiler.compile_private(frozen, work.control()).map_err(
                |failure| match failure {
                    PureProviderCompileError::Provider(error) => {
                        ConnectorReadProgramCompileError::Provider(error)
                    }
                    PureProviderCompileError::Control(error) => {
                        ConnectorReadProgramCompileError::Control(error)
                    }
                },
            )?;
            ConnectorReadRelationRecipe::validate_canonical_header_shape(
                frozen.scan.recipe(),
                &canonical,
            )
            .map_err(|error| {
                ConnectorReadProgramCompileError::Contract(ConnectorError::from_domain(
                    ConnectorErrorKind::InvalidRequest,
                    error,
                ))
            })?;
            work.step()
                .map_err(ConnectorReadProgramCompileError::Control)?;
            for (original, canonical) in frozen
                .scan
                .recipe()
                .columns()
                .iter()
                .zip(canonical.columns())
            {
                let same_header = original.header() == canonical.header();
                work.step()
                    .map_err(ConnectorReadProgramCompileError::Control)?;
                if !same_header {
                    return Err(ConnectorReadProgramCompileError::Contract(invalid(
                        "canonical read column header differs from its frozen ordinal",
                    )));
                }
            }
            let scan = frozen
                .scan
                .try_replace_private_recipe(canonical)
                .map_err(|error| {
                    ConnectorReadProgramCompileError::Contract(match error {
                        StaticConnectorScanError::TooManyRetainedBytes => exhausted(),
                        _ => invalid(error.to_string()),
                    })
                })?;
            // Public facts were validated before provider compilation. Sharing them
            // cannot alter any assignment, predicate, schema or source identity.
            let retained_bytes = scan
                .retained_bytes()
                .checked_add(frozen.public.charged_bytes())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<FrozenConnectorRead>()))
                .ok_or_else(|| ConnectorReadProgramCompileError::Contract(exhausted()))?;
            if retained_bytes > MAX_STATIC_SCAN_RETAINED_BYTES {
                return Err(ConnectorReadProgramCompileError::Contract(exhausted()));
            }
            let canonical = FrozenConnectorRead {
                scan,
                public: frozen.public.clone(),
                retained_bytes,
            };
            work.step()
                .map_err(ConnectorReadProgramCompileError::Control)?;
            Ok(Self(canonical))
        })();
        work.finish_result(result, ConnectorReadProgramCompileError::Control)
    }

    pub const fn frozen(&self) -> &FrozenConnectorRead {
        &self.0
    }
}
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "complete frozen read exceeds the retained structural byte limit",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use bytes::Bytes;
    use std::sync::Arc;
    struct Control(bool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.0 {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    fn scan() -> FrozenConnectorScan {
        let provider = ConnectorProviderId::parse("fixture").unwrap();
        let instance = ConnectorInstanceId::parse("fixture").unwrap();
        let catalog = CatalogHandle::new(instance.clone(), CatalogVersion::from_bytes([1; 32]));
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let payload = |category| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    provider.clone(),
                    catalog.clone(),
                    category,
                    ConnectorCodecRevision::try_new(1).unwrap(),
                ),
                Bytes::from_static(b"private"),
            )
        };
        let recipe = ConnectorReadRelationRecipeDraft::try_new(
            binding,
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(ConnectorCodecCategory::ReadTable),
                payload(ConnectorCodecCategory::ReadView),
            ),
            vec![payload(ConnectorCodecCategory::ReadColumn)],
        )
        .unwrap();
        FrozenConnectorScan::try_new(
            recipe,
            vec![StaticScanAssignment::new(
                Arc::from("v"),
                ConnectorValueType::BigInt,
            )],
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
        )
        .unwrap()
    }
    struct Compiler(bool);
    impl ConnectorReadProgramCompiler for Compiler {
        type Error = ConnectorError;
        fn compile_private(
            &self,
            frozen: &FrozenConnectorRead,
            _: &dyn PureCompileControl,
        ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<ConnectorError>>
        {
            if frozen.public_facts().source().input_version().as_bytes() != [9]
                || frozen.public_facts().source().selection_digest() != [7; 32]
            {
                return Err(PureProviderCompileError::Provider(invalid(
                    "fixture private source identity differs from public facts",
                )));
            }
            let draft = frozen.scan().recipe();
            let revision = if self.0 { 2 } else { 1 };
            let canonical = |payload: &ConnectorEncodedPayload| {
                ConnectorEncodedPayload::new(
                    ConnectorEnvelopeHeader::new(
                        payload.header().provider_id().clone(),
                        payload.header().catalog().clone(),
                        payload.header().category(),
                        ConnectorCodecRevision::try_new(revision).unwrap(),
                    ),
                    Bytes::from_static(b"canonical"),
                )
            };
            ConnectorReadRelationRecipeDraft::try_new(
                draft.binding().clone(),
                ConnectorReadRelationPayload::new(
                    draft.relation().kind(),
                    canonical(draft.relation().table()),
                    canonical(draft.relation().view()),
                ),
                draft.columns().iter().map(canonical).collect(),
            )
            .map_err(|error| PureProviderCompileError::Provider(invalid(error.to_string())))
        }
    }
    fn frozen_with_columns(
        count: usize,
        ty: arrow_schema::DataType,
        logical: ValueLogicalType,
        assignment: ConnectorValueType,
    ) -> FrozenConnectorRead {
        let original = scan();
        let draft = original.recipe();
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            draft.binding().clone(),
            draft.relation().clone(),
            vec![draft.columns()[0].clone(); count],
        )
        .unwrap();
        let scan = FrozenConnectorScan::try_new(
            draft,
            (0..count)
                .map(|i| StaticScanAssignment::new(Arc::from(format!("v{i}")), assignment))
                .collect(),
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![],
        )
        .unwrap();
        let public = ConnectorReadPublicFacts::try_new(
            crate::read_public::tests::public().source().clone(),
            None,
            arrow_schema::Schema::new(
                (0..count)
                    .map(|i| arrow_schema::Field::new(format!("v{i}"), ty.clone(), true))
                    .collect::<Vec<_>>(),
            ),
            vec![logical; count],
        )
        .unwrap();
        FrozenConnectorRead::try_new(scan, public).unwrap()
    }
    #[test]
    fn wrapper_accounts_header_work_and_cancels_before_sealing() {
        struct Budget(std::sync::Mutex<Vec<u32>>);
        impl PureCompileControl for Budget {
            fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
                self.0.lock().unwrap().push(work);
                if work > 0 {
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        let frozen = frozen_with_columns(
            300,
            arrow_schema::DataType::Int64,
            ValueLogicalType::Physical,
            ConnectorValueType::BigInt,
        );
        let budget = Budget(std::sync::Mutex::default());
        assert!(matches!(
            ConnectorReadProgramRecipe::try_compile_with_provider(
                &frozen,
                &Compiler(false),
                &budget
            ),
            Err(ConnectorReadProgramCompileError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(
            budget.0.lock().unwrap().iter().sum::<u32>(),
            novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK
        );
    }
    #[test]
    fn canonical_private_expansion_keeps_resource_error_class() {
        struct Expanding;
        impl ConnectorReadProgramCompiler for Expanding {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                frozen: &FrozenConnectorRead,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                let original = frozen.scan().recipe();
                ConnectorReadRelationRecipeDraft::try_new(
                    original.binding().clone(),
                    original.relation().clone(),
                    vec![ConnectorEncodedPayload::new(
                        original.columns()[0].header().clone(),
                        Bytes::from(vec![0; MAX_STATIC_SCAN_RETAINED_BYTES]),
                    )],
                )
                .map_err(|e| PureProviderCompileError::Provider(invalid(e.to_string())))
            }
        }
        let frozen =
            FrozenConnectorRead::try_new(scan(), crate::read_public::tests::public()).unwrap();
        assert!(
            matches!(ConnectorReadProgramRecipe::try_compile_with_provider(&frozen, &Expanding, &Control(false)),
            Err(ConnectorReadProgramCompileError::Contract(error)) if error.kind() == ConnectorErrorKind::ResourceExhausted)
        );
    }
    #[test]
    fn explicit_uuid_requires_uuid_assignment_in_both_directions() {
        let valid = frozen_with_columns(
            1,
            arrow_schema::DataType::FixedSizeBinary(16),
            ValueLogicalType::Uuid,
            ConnectorValueType::Uuid,
        );
        let physical = frozen_with_columns(
            1,
            arrow_schema::DataType::FixedSizeBinary(16),
            ValueLogicalType::Physical,
            ConnectorValueType::Fixed { length: 16 },
        );
        assert!(
            FrozenConnectorRead::try_new(valid.scan().clone(), physical.public_facts().clone())
                .is_err()
        );
        assert!(
            FrozenConnectorRead::try_new(physical.scan().clone(), valid.public_facts().clone())
                .is_err()
        );
    }

    #[test]
    fn complete_provider_compile_preserves_public_facts_and_refuses_header_drift() {
        let frozen =
            FrozenConnectorRead::try_new(scan(), crate::read_public::tests::public()).unwrap();
        let recipe = ConnectorReadProgramRecipe::try_compile_with_provider(
            &frozen,
            &Compiler(false),
            &Control(false),
        )
        .unwrap();
        assert_eq!(recipe.frozen().public_facts(), frozen.public_facts());
        assert!(std::ptr::eq(
            recipe.frozen().scan().enforced_predicate(),
            frozen.scan().enforced_predicate()
        ));
        assert!(std::ptr::eq(
            recipe.frozen().public_facts().schema(),
            frozen.public_facts().schema()
        ));
        assert_eq!(
            recipe.frozen().scan().recipe().columns()[0]
                .payload()
                .as_ref(),
            b"canonical"
        );
        assert!(matches!(
            ConnectorReadProgramRecipe::try_compile_with_provider(
                &frozen,
                &Compiler(true),
                &Control(false)
            ),
            Err(ConnectorReadProgramCompileError::Contract(_))
        ));
        assert!(matches!(
            ConnectorReadProgramRecipe::try_compile_with_provider(
                &frozen,
                &Compiler(false),
                &Control(true)
            ),
            Err(ConnectorReadProgramCompileError::Control(
                CompileControlError::Cancelled
            ))
        ));
    }
    #[test]
    fn ordinary_provider_and_contract_failures_keep_their_first_error_and_actual_control_prefix() {
        struct Refusing;
        impl ConnectorReadProgramCompiler for Refusing {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                _: &FrozenConnectorRead,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                Err(PureProviderCompileError::Provider(invalid(
                    "provider refusal",
                )))
            }
        }
        struct Tail {
            trace: std::sync::Mutex<Vec<u32>>,
            refusal: Option<(usize, CompileControlError)>,
        }
        impl PureCompileControl for Tail {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                let mut trace = self.trace.lock().unwrap();
                trace.push(units);
                match &self.refusal {
                    Some((at, cause)) if trace.len() == *at => Err(cause.clone()),
                    _ => Ok(()),
                }
            }
        }
        let frozen =
            FrozenConnectorRead::try_new(scan(), crate::read_public::tests::public()).unwrap();
        for provider in [true, false] {
            let run = |control: &Tail| {
                if provider {
                    ConnectorReadProgramRecipe::try_compile_with_provider(
                        &frozen, &Refusing, control,
                    )
                } else {
                    ConnectorReadProgramRecipe::try_compile_with_provider(
                        &frozen,
                        &Compiler(true),
                        control,
                    )
                }
            };
            let baseline = Tail {
                trace: Default::default(),
                refusal: None,
            };
            let expected = run(&baseline).unwrap_err();
            let original = match &expected {
                ConnectorReadProgramCompileError::Provider(cause) if provider => cause,
                ConnectorReadProgramCompileError::Contract(cause) if !provider => cause,
                other => panic!("original refusal changed: {other:?}"),
            };
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace, [0, 0]);
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                for at in 1..=trace.len() {
                    let control = Tail {
                        trace: Default::default(),
                        refusal: Some((at, cause.clone())),
                    };
                    assert!(
                        matches!(run(&control), Err(ConnectorReadProgramCompileError::Control(actual)) if actual == cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
                }
                let control = Tail {
                    trace: Default::default(),
                    refusal: Some((trace.len() + 1, cause)),
                };
                let actual = run(&control).unwrap_err();
                let returned = match &actual {
                    ConnectorReadProgramCompileError::Provider(cause) if provider => cause,
                    ConnectorReadProgramCompileError::Contract(cause) if !provider => cause,
                    other => panic!("late control replaced original refusal: {other:?}"),
                };
                use novarocks_failure_contract::FailureProjection;
                assert_eq!(returned.failure_fact(), original.failure_fact());
                assert_eq!(returned.kind(), original.kind());
                assert_eq!(returned.message(), original.message());
                if !provider {
                    let shared = returned
                        .source()
                        .unwrap()
                        .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
                        .unwrap();
                    assert_eq!(
                        shared.downcast_ref::<crate::ConnectorReadRelationRecipeError>(),
                        Some(&crate::ConnectorReadRelationRecipeError::PublicFactsMismatch)
                    );
                }
                assert_eq!(*control.trace.lock().unwrap(), trace);
            }
        }
    }

    #[test]
    fn primary_provider_control_returns_without_another_callback_and_preserves_source() {
        struct Interrupted;
        impl ConnectorReadProgramCompiler for Interrupted {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                _: &FrozenConnectorRead,
                _: &dyn PureCompileControl,
            ) -> Result<ConnectorReadRelationRecipeDraft, PureProviderCompileError<Self::Error>>
            {
                Err(PureProviderCompileError::Control(
                    CompileControlError::Cancelled,
                ))
            }
        }
        struct Count(std::sync::Mutex<usize>);
        impl PureCompileControl for Count {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                *self.0.lock().unwrap() += 1;
                Ok(())
            }
        }
        let frozen =
            FrozenConnectorRead::try_new(scan(), crate::read_public::tests::public()).unwrap();
        let count = Count(std::sync::Mutex::new(0));
        let error =
            ConnectorReadProgramRecipe::try_compile_with_provider(&frozen, &Interrupted, &count)
                .unwrap_err();
        assert!(matches!(
            &error,
            ConnectorReadProgramCompileError::Control(CompileControlError::Cancelled)
        ));
        assert_eq!(*count.0.lock().unwrap(), 2);
        assert!(error.source().unwrap().is::<CompileControlError>());
        let provider: ConnectorReadProgramCompileError<ConnectorError> =
            ConnectorReadProgramCompileError::Provider(invalid("source"));
        assert!(provider.source().unwrap().is::<ConnectorError>());
    }
}
