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

use std::sync::Mutex;

use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::AggregateStateArgumentContract;

use super::*;
use crate::{AggregateOverloadIdentity, AggregateSignatureResolver, ResolvedAggregateSignature};

const NAME: &str = "parametric_exact_fixture";
const LEFT: &str = "test/parametric/left/v1";
const RIGHT: &str = "test/parametric/right/v1";

#[derive(Default)]
struct Family {
    elections: Mutex<Vec<Vec<DataType>>>,
    updates: Mutex<Vec<(String, Vec<DataType>)>>,
}
impl AggregateSignatureResolver for Family {
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        self.elections.lock().unwrap().push(types.to_vec());
        let selected = match types {
            [DataType::Utf8] => RIGHT,
            [DataType::Int64 | DataType::Struct(_)] => LEFT,
            _ => {
                return Err(crate::FunctionResolutionError::BadSignature(
                    "no candidate".into(),
                ));
            }
        };
        self.resolve_update_signature(
            &AggregateOverloadIdentity::try_new(selected).unwrap(),
            types,
        )
    }
    fn produces_null(&self) -> bool {
        false
    }
    fn resolve_update_signature(
        &self,
        selected: &AggregateOverloadIdentity,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        self.updates
            .lock()
            .unwrap()
            .push((selected.as_str().to_owned(), types.to_vec()));
        let accepted = matches!(
            (selected.as_str(), types),
            (LEFT, [DataType::Int64 | DataType::Struct(_)]) | (RIGHT, [DataType::Utf8])
        );
        if !accepted {
            return Err(crate::FunctionResolutionError::BadSignature(
                "fixed overload does not accept these channels".into(),
            ));
        }
        Ok(ResolvedAggregateSignature {
            overload: selected.clone(),
            argument_types: types.to_vec(),
            intermediate_type: DataType::Binary,
            output_type: DataType::Int64,
            state_format: AggregateStateFormatIdentity::try_new(if selected.as_str() == LEFT {
                "test/left/state-v1"
            } else {
                "test/right/state-v1"
            })
            .unwrap(),
        })
    }
}

fn catalog_from(family: Arc<dyn AggregateSignatureResolver>) -> EngineFunctionCatalog {
    let definition = FunctionDefinition::try_new_parametric_aggregate(
        NAME,
        FunctionVisibility::Public,
        FunctionVolatility::Immutable,
        [
            AggregateOverloadDeclaration::try_new(
                LEFT,
                "(int64|struct)",
                "binary",
                "int64",
                "test/left/state-v1",
            )
            .unwrap(),
            AggregateOverloadDeclaration::try_new(
                RIGHT,
                "(utf8)",
                "binary",
                "int64",
                "test/right/state-v1",
            )
            .unwrap(),
        ],
        family.clone(),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    builder.seal_bound().unwrap()
}
fn catalog() -> (EngineFunctionCatalog, Arc<Family>) {
    let family = Arc::new(Family::default());
    (catalog_from(family.clone()), family)
}

#[test]
fn parametric_binding_loan_retains_the_original_fixed_author_and_control() {
    let (catalog, family) = catalog();
    let definition = catalog.definition_by_id(&id()).unwrap();
    let cloned = definition.clone();
    let author = definition.binding_resolver().unwrap();
    assert!(same_resolver_owner(
        author,
        cloned.binding_resolver().unwrap()
    ));
    let args = [argument(DataType::Int64, true)];
    let overload = FunctionOverloadId::try_new(LEFT).unwrap();
    let control = Trace::default();
    let selected = author
        .select_at_overload_observed(&overload, request(&args), &control)
        .unwrap();
    author
        .validate_selected(&selected, request(&args), &control)
        .unwrap();
    assert!(family.elections.lock().unwrap().is_empty());
    assert_eq!(family.updates.lock().unwrap().len(), 2);

    let successful = Trace::default();
    author
        .select_at_overload_observed(&overload, request(&args), &successful)
        .unwrap();
    let prefix = successful.calls.lock().unwrap().clone();
    assert!(!prefix.is_empty());
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=prefix.len() {
            let stop = Trace {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                author.select_at_overload_observed(&overload, request(&args), &stop),
                Err(FunctionBindingError::Control(cause.clone())),
            );
            assert_eq!(*stop.calls.lock().unwrap(), prefix[..at]);
        }
    }
    assert!(family.elections.lock().unwrap().is_empty());
}
fn id() -> FunctionId {
    FunctionId::try_new("parametric.aggregate/parametric_exact_fixture/v1").unwrap()
}
fn argument(ty: DataType, nullable: bool) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::from_legacy_declared_projection(ty, nullable),
        constant: None,
    }
}
fn request(args: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments: args,
        logical_argument_count: args.len(),
    }
}
fn select(
    catalog: &EngineFunctionCatalog,
    overload: &str,
    args: &[FunctionArgument],
    control: &dyn PureCompileControl,
) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
    catalog.select_exact_overload_observed(
        &id(),
        FunctionKind::Aggregate,
        &FunctionOverloadId::try_new(overload).unwrap(),
        request(args),
        control,
    )
}

#[derive(Default)]
struct Trace {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        calls.push((phase, units));
        let at = calls.len();
        if let Some((stop, cause)) = self.stop.clone() {
            assert!(at <= stop, "observation after the primary refusal");
            if at == stop {
                return Err(cause);
            }
        }
        Ok(())
    }
}
fn every_prefix<T>(call: impl Fn(&Trace) -> Result<T, FunctionBindingError>) {
    let baseline = Trace::default();
    let baseline_error = call(&baseline).err();
    let trace = baseline.calls.lock().unwrap().clone();
    assert_eq!(
        trace.first(),
        Some(&(CompilePhase::FunctionSpecialization, 0))
    );
    if baseline_error.is_none() {
        // Successful work still observes its original positive completion tail.
        assert!(trace.len() >= 2);
        assert!(trace.iter().any(|(_, units)| *units > 0));
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
    ] {
        for at in 1..=trace.len() {
            let control = Trace {
                calls: Mutex::new(Vec::new()),
                stop: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FunctionBindingError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..at]);
        }
    }
    if let Some(original) = baseline_error {
        assert!(
            !matches!(&original, FunctionBindingError::Control(_)),
            "an unarmed baseline must supply the real semantic refusal"
        );
        let late = Trace {
            calls: Mutex::default(),
            stop: Some((trace.len() + 1, CompileControlError::Cancelled)),
        };
        let actual = call(&late).err().expect("the original semantic refusal");
        assert_eq!(actual, original);
        assert_eq!(
            *late.calls.lock().unwrap(),
            trace,
            "a real semantic error cannot trigger a new completion callback"
        );
    }
}

#[test]
fn parametric_exact_identity_uses_only_the_original_fixed_overload_author() {
    let (catalog, family) = catalog();
    for overload in catalog
        .definition(NAME, FunctionKind::Aggregate)
        .unwrap()
        .binding_declaration()
        .unwrap()
        .overloads()
    {
        assert_eq!(
            overload.aggregate.as_ref().unwrap().state_argument_contract,
            AggregateStateArgumentContract::ExactSignature
        );
    }
    let integer = [argument(DataType::Int64, true)];
    let text = [argument(DataType::Utf8, false)];
    let resolved = catalog
        .resolve_bound_user(
            NAME,
            FunctionKind::Aggregate,
            request(&integer),
            &Trace::default(),
        )
        .unwrap();
    assert_eq!(resolved.function_id, id());
    assert_eq!(resolved.selected.overload.as_str(), LEFT);
    assert_eq!(family.elections.lock().unwrap().len(), 1);
    family.elections.lock().unwrap().clear();
    family.updates.lock().unwrap().clear();
    for (overload, args) in [(LEFT, integer.as_slice()), (RIGHT, text.as_slice())] {
        let selected = select(&catalog, overload, args, &Trace::default()).unwrap();
        assert_eq!(selected.overload.as_str(), overload);
        assert_eq!(selected.argument_types.as_ref(), &[args[0].argument_type()]);
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false))
        );
        let state = selected.aggregate.as_ref().unwrap();
        assert_eq!(
            state.intermediate_type,
            FunctionValueType::new(DataType::Binary, false)
        );
        assert_eq!(
            state.state_argument_contract,
            AggregateStateArgumentContract::ExactSignature
        );
        assert_eq!(
            state.state_format.as_str(),
            if overload == LEFT {
                "test/left/state-v1"
            } else {
                "test/right/state-v1"
            }
        );
    }
    assert!(family.elections.lock().unwrap().is_empty());
    assert!(matches!(
        select(&catalog, LEFT, &text, &Trace::default()),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    assert!(
        select(&catalog, RIGHT, &text, &Trace::default()).is_ok(),
        "the alternate candidate really accepts the channels"
    );
    assert!(family.elections.lock().unwrap().is_empty());
    let before = family.updates.lock().unwrap().len();
    let absent = FunctionOverloadId::try_new("test/parametric/absent/v1").unwrap();
    assert!(matches!(
        catalog.select_exact_overload_observed(
            &id(),
            FunctionKind::Aggregate,
            &absent,
            request(&integer),
            &Trace::default()
        ),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    assert!(matches!(
        catalog.select_exact_overload_observed(
            &id(),
            FunctionKind::Scalar,
            &FunctionOverloadId::try_new(LEFT).unwrap(),
            request(&integer),
            &Trace::default()
        ),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    assert_eq!(
        family.updates.lock().unwrap().len(),
        before,
        "outer identity gates run before the family"
    );
    assert!(
        family
            .updates
            .lock()
            .unwrap()
            .iter()
            .all(|(overload, _)| overload == LEFT || overload == RIGHT)
    );
}

#[test]
fn parametric_exact_request_preserves_none_and_the_original_selected_constant() {
    let (catalog, family) = catalog();
    let ty = FunctionValueType::new(DataType::Int64, true);
    let field = Arc::new(ty.try_to_field("original-source").unwrap());
    let array = Int64Array::from(vec![Some(-900), Some(42), None]);
    let policy = crate::ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 64,
        max_logical_elements: 16384,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 << 20,
        max_library_validation_bytes: 4 << 20,
    };
    let pool = crate::ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy,
        CompilePhase::FunctionSpecialization,
        &Trace::default(),
    )
    .unwrap();
    let original = pool.value(1).unwrap();
    let args = [FunctionArgument::Value {
        value_type: ty.clone(),
        constant: Some(original.clone()),
    }];
    every_prefix(|control| select(&catalog, LEFT, &args, control));
    let selected = select(&catalog, LEFT, &args, &Trace::default()).unwrap();
    let FunctionArgument::Value {
        constant: Some(actual),
        ..
    } = &args[0]
    else {
        panic!("original constant remains present")
    };
    assert_eq!(actual.ordinal(), 1);
    assert!(Arc::ptr_eq(actual.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
    assert_eq!(
        actual
            .int64_observed(CompilePhase::FunctionSpecialization, &Trace::default())
            .unwrap(),
        Some(42)
    );
    assert_eq!(
        selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(ty.clone())]
    );
    let nonconstant = [argument(DataType::Int64, true)];
    assert!(matches!(
        &nonconstant[0],
        FunctionArgument::Value { constant: None, .. }
    ));
    assert_eq!(
        *select(&catalog, LEFT, &nonconstant, &Trace::default()).unwrap(),
        *selected
    );
    assert!(family.elections.lock().unwrap().is_empty());
    // Equal metadata is not a proof that the original request had a constant.
}

#[test]
fn parametric_exact_validation_rejects_full_selected_contract_drift() {
    let (catalog, family) = catalog();
    let args = [argument(DataType::Int64, true)];
    let selected = select(&catalog, LEFT, &args, &Trace::default()).unwrap();
    let mut variants = Vec::new();
    let mut changed = (*selected).clone();
    let mut arguments = changed.argument_types.as_slice().to_vec();
    let FunctionArgumentType::Value(ty) = &mut arguments[0] else {
        unreachable!()
    };
    *ty = ty
        .with_nullable(false)
        .expect("valid function nullability declaration");
    changed.argument_types = novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
        arguments.into_boxed_slice(),
    );
    variants.push(changed);
    let mut changed = (*selected).clone();
    changed.result_type = FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, true));
    variants.push(changed);
    let mut changed = (*selected).clone();
    {
        let intermediate = &mut changed.aggregate.as_mut().unwrap().intermediate_type;
        *intermediate = intermediate
            .with_nullable(true)
            .expect("valid function nullability declaration");
    }
    variants.push(changed);
    let mut changed = (*selected).clone();
    changed.aggregate.as_mut().unwrap().state_format =
        AggregateStateFormatIdentity::try_new("test/wrong/state-v1").unwrap();
    variants.push(changed);
    let mut changed = (*selected).clone();
    changed.aggregate.as_mut().unwrap().state_argument_contract =
        AggregateStateArgumentContract::ValueRootNullabilityIndependent;
    variants.push(changed);
    for changed in variants {
        every_prefix(|control| {
            catalog.validate_frozen_selection(
                &id(),
                FunctionKind::Aggregate,
                &changed,
                request(&args),
                control,
            )
        });
        assert!(matches!(
            catalog.validate_frozen_selection(
                &id(),
                FunctionKind::Aggregate,
                &changed,
                request(&args),
                &Trace::default()
            ),
            Err(FunctionBindingError::InvalidBinding(_))
        ));
    }
    assert!(family.elections.lock().unwrap().is_empty());
}

#[test]
fn parametric_exact_actual_control_prefixes_cover_success_failure_and_nested_metadata() {
    let (catalog, family) = catalog();
    let args = [argument(DataType::Int64, true)];
    every_prefix(|control| select(&catalog, LEFT, &args, control));
    let rejected = [argument(DataType::Utf8, false)];
    assert!(matches!(
        select(&catalog, LEFT, &rejected, &Trace::default()),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    every_prefix(|control| select(&catalog, LEFT, &rejected, control));
    let wide = [argument(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("field_{i}"), DataType::Int64, i % 2 == 0)
                            .with_metadata(std::collections::HashMap::<String, String>::from([(
                                "source".into(),
                                format!("metadata_{i}"),
                            )])),
                    )
                })
                .collect(),
        ),
        true,
    )];
    let selected = select(&catalog, LEFT, &wide, &Trace::default()).unwrap();
    let control = Trace::default();
    catalog
        .validate_frozen_selection(
            &id(),
            FunctionKind::Aggregate,
            &selected,
            request(&wide),
            &control,
        )
        .unwrap();
    let trace = control.calls.lock().unwrap().clone();
    assert!(
        trace.iter().any(|(_, units)| *units == 256),
        "the original complete nested-type walker crosses its real quantum"
    );
    // This is a bounded metadata author fixture, not an installed runtime kernel.
    for at in [
        1,
        trace.iter().position(|(_, n)| *n == 256).unwrap() + 1,
        trace.len(),
    ] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace {
                calls: Mutex::new(Vec::new()),
                stop: Some((at, cause.clone())),
            };
            assert_eq!(
                catalog.validate_frozen_selection(
                    &id(),
                    FunctionKind::Aggregate,
                    &selected,
                    request(&wide),
                    &control
                ),
                Err(FunctionBindingError::Control(cause))
            );
            assert_eq!(*control.calls.lock().unwrap(), trace[..at]);
        }
    }
    assert!(family.elections.lock().unwrap().is_empty());
}

#[derive(Default)]
struct ElectionOnly {
    elections: std::sync::atomic::AtomicUsize,
}
impl AggregateSignatureResolver for ElectionOnly {
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        self.elections
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(ResolvedAggregateSignature {
            overload: AggregateOverloadIdentity::try_new(LEFT).unwrap(),
            argument_types: types.to_vec(),
            intermediate_type: DataType::Binary,
            output_type: DataType::Int64,
            state_format: AggregateStateFormatIdentity::try_new("test/left/state-v1").unwrap(),
        })
    }
}
#[test]
fn parametric_default_fixed_capability_refuses_without_candidate_election() {
    let family = Arc::new(ElectionOnly::default());
    let definition = FunctionDefinition::try_new_parametric_aggregate(
        "election_only",
        FunctionVisibility::Public,
        FunctionVolatility::Immutable,
        [AggregateOverloadDeclaration::try_new(
            LEFT,
            "(int64)",
            "binary",
            "int64",
            "test/left/state-v1",
        )
        .unwrap()],
        family.clone(),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    let catalog = builder.seal_bound().unwrap();
    let args = [argument(DataType::Int64, true)];
    let legacy = catalog
        .resolve_aggregate_user("election_only", &[DataType::Int64])
        .unwrap();
    assert_eq!(legacy.overload.as_str(), LEFT);
    assert_eq!(
        family.elections.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    family
        .elections
        .store(0, std::sync::atomic::Ordering::SeqCst);
    let exact_id = FunctionId::try_new("parametric.aggregate/election_only/v1").unwrap();
    let overload = FunctionOverloadId::try_new(LEFT).unwrap();
    let call = |control: &Trace| {
        catalog.select_exact_overload_observed(
            &exact_id,
            FunctionKind::Aggregate,
            &overload,
            request(&args),
            control,
        )
    };
    assert!(matches!(
        call(&Trace::default()),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    every_prefix(call);
    assert_eq!(
        family.elections.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the unsupported fixed port must not invoke its still-working election author"
    );
}

#[derive(Clone, Copy)]
enum ReturnedChannelFault {
    Arity,
    Carrier,
    NestedMetadata,
}
struct FaultyChannels {
    family: Family,
    fault: ReturnedChannelFault,
}
impl AggregateSignatureResolver for FaultyChannels {
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        self.family.resolve_aggregate(types)
    }
    fn produces_null(&self) -> bool {
        false
    }
    fn resolve_update_signature(
        &self,
        selected: &AggregateOverloadIdentity,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        let mut resolved = self.family.resolve_update_signature(selected, types)?;
        match self.fault {
            ReturnedChannelFault::Arity => resolved.argument_types.clear(),
            ReturnedChannelFault::Carrier => resolved.argument_types[0] = DataType::Utf8,
            ReturnedChannelFault::NestedMetadata => {
                let DataType::Struct(fields) = &resolved.argument_types[0] else {
                    panic!("nested fixture")
                };
                assert_eq!(fields.len(), 1);
                let original = &fields[0];
                let mut metadata = original.metadata().clone();
                metadata.insert("source-label".to_owned(), "forged".to_owned());
                resolved.argument_types[0] = DataType::Struct(
                    vec![Arc::new(original.as_ref().clone().with_metadata(metadata))].into(),
                );
            }
        }
        assert_eq!(
            resolved.overload, *selected,
            "the malicious author keeps the supplied identity"
        );
        Ok(resolved)
    }
}

#[test]
fn parametric_returned_channels_cannot_be_hidden_by_the_supplied_overload_identity() {
    let (correct, _) = catalog();
    for fault in [
        ReturnedChannelFault::Arity,
        ReturnedChannelFault::Carrier,
        ReturnedChannelFault::NestedMetadata,
    ] {
        let args = match fault {
            ReturnedChannelFault::Arity | ReturnedChannelFault::Carrier => {
                [argument(DataType::Int64, true)]
            }
            ReturnedChannelFault::NestedMetadata => [argument(
                DataType::Struct(
                    vec![Arc::new(
                        Field::new("original-field", DataType::Int64, true).with_metadata(
                            std::collections::HashMap::<String, String>::from([(
                                "source-label".into(),
                                "original".into(),
                            )]),
                        ),
                    )]
                    .into(),
                ),
                true,
            )],
        };
        // An independently valid selected payload lets the validator reach
        // its own faulty installed resolver, rather than failing shape first.
        let selected = select(&correct, LEFT, &args, &Trace::default()).unwrap();
        let family = Arc::new(FaultyChannels {
            family: Family::default(),
            fault,
        });
        let faulty = catalog_from(family.clone());
        let exact = |control: &Trace| select(&faulty, LEFT, &args, control);
        assert!(matches!(
            exact(&Trace::default()),
            Err(FunctionBindingError::InvalidBinding(_))
        ));
        every_prefix(exact);
        let validate = |control: &Trace| {
            faulty.validate_frozen_selection(
                &id(),
                FunctionKind::Aggregate,
                &selected,
                request(&args),
                control,
            )
        };
        assert!(matches!(
            validate(&Trace::default()),
            Err(FunctionBindingError::InvalidBinding(_))
        ));
        every_prefix(validate);
        assert!(
            family.family.elections.lock().unwrap().is_empty(),
            "neither path may elect a replacement candidate"
        );
        let updates = family.family.updates.lock().unwrap();
        assert!(!updates.is_empty());
        assert!(updates.iter().all(|(overload, _)| overload == LEFT));
    }
}

#[derive(Default)]
struct StateLawFamily {
    family: Family,
    mode: std::sync::atomic::AtomicU8,
    laws: Mutex<Vec<String>>,
    observation: Mutex<Option<Arc<Trace>>>,
    last_author_entry: std::sync::atomic::AtomicUsize,
}
impl AggregateSignatureResolver for StateLawFamily {
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        self.family.resolve_aggregate(types)
    }
    fn resolve_update_signature(
        &self,
        overload: &AggregateOverloadIdentity,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        self.family.resolve_update_signature(overload, types)
    }
    fn produces_null(&self) -> bool {
        false
    }
    fn state_argument_contract(
        &self,
        overload: &AggregateOverloadIdentity,
    ) -> Result<AggregateStateArgumentContract, crate::FunctionResolutionError> {
        self.laws.lock().unwrap().push(overload.as_str().to_owned());
        if let Some(trace) = self.observation.lock().unwrap().as_ref() {
            self.last_author_entry.store(
                trace.calls.lock().unwrap().len(),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        match self.mode.load(std::sync::atomic::Ordering::SeqCst) {
            1 => return Ok(AggregateStateArgumentContract::ExactSignature),
            2 => {
                return Err(crate::FunctionResolutionError::BadSignature(
                    "state-law author rejected selected overload".into(),
                ));
            }
            3 => {
                return Err(crate::FunctionResolutionError::Control(
                    CompileControlError::Cancelled,
                ));
            }
            4 => {
                return Err(crate::FunctionResolutionError::Control(
                    CompileControlError::DeadlineExceeded,
                ));
            }
            5 => {
                return Err(crate::FunctionResolutionError::Control(
                    CompileControlError::ResourceExhausted,
                ));
            }
            _ => {}
        }
        match overload.as_str() {
            LEFT => Ok(AggregateStateArgumentContract::ValueRootNullabilityIndependent),
            RIGHT => Ok(AggregateStateArgumentContract::ExactSignature),
            _ => Err(crate::FunctionResolutionError::BadSignature(
                "foreign state-law overload".into(),
            )),
        }
    }
}
#[test]
fn parametric_state_law_is_authored_per_overload_in_declaration_and_every_selection() {
    let family = Arc::new(StateLawFamily::default());
    let catalog = catalog_from(family.clone());
    assert_eq!(*family.laws.lock().unwrap(), [LEFT, RIGHT]);
    let declaration = catalog
        .definition(NAME, FunctionKind::Aggregate)
        .unwrap()
        .binding_declaration()
        .unwrap();
    for (overload, ty, expected) in [
        (
            LEFT,
            DataType::Int64,
            AggregateStateArgumentContract::ValueRootNullabilityIndependent,
        ),
        (
            RIGHT,
            DataType::Utf8,
            AggregateStateArgumentContract::ExactSignature,
        ),
    ] {
        assert_eq!(
            declaration
                .overload(&FunctionOverloadId::try_new(overload).unwrap())
                .unwrap()
                .aggregate
                .as_ref()
                .unwrap()
                .state_argument_contract,
            expected
        );
        for nullable in [false, true] {
            let args = [argument(ty.clone(), nullable)];
            let initial = catalog
                .resolve_bound_user(
                    NAME,
                    FunctionKind::Aggregate,
                    request(&args),
                    &Trace::default(),
                )
                .unwrap();
            let exact = select(&catalog, overload, &args, &Trace::default()).unwrap();
            assert_eq!(initial.selected, *exact);
            assert_eq!(exact.argument_types.as_ref(), &[args[0].argument_type()]);
            let state = exact.aggregate.as_ref().unwrap();
            assert_eq!(state.state_argument_contract, expected);
            assert_eq!(
                state.intermediate_type,
                FunctionValueType::new(DataType::Binary, false)
            );
            every_prefix(|control| select(&catalog, overload, &args, control));
            every_prefix(|control| {
                catalog.validate_frozen_selection(
                    &id(),
                    FunctionKind::Aggregate,
                    &exact,
                    request(&args),
                    control,
                )
            });
            let mut forged = exact.as_ref().clone();
            forged.aggregate.as_mut().unwrap().state_argument_contract =
                if expected == AggregateStateArgumentContract::ExactSignature {
                    AggregateStateArgumentContract::ValueRootNullabilityIndependent
                } else {
                    AggregateStateArgumentContract::ExactSignature
                };
            assert!(
                catalog
                    .validate_frozen_selection(
                        &id(),
                        FunctionKind::Aggregate,
                        &forged,
                        request(&args),
                        &Trace::default()
                    )
                    .is_err()
            );
        }
    }
    let args = [argument(DataType::Int64, true)];
    family.family.elections.lock().unwrap().clear();
    family.mode.store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        select(&catalog, LEFT, &args, &Trace::default()),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    every_prefix(|control| select(&catalog, LEFT, &args, control));
    assert!(
        family.family.elections.lock().unwrap().is_empty(),
        "a changed state-law author must not re-elect a candidate"
    );
}

#[test]
fn parametric_state_law_keeps_its_semantic_failure_and_original_author_control_without_after() {
    let family = Arc::new(StateLawFamily::default());
    let catalog = catalog_from(family.clone());
    let args = [argument(DataType::Int64, true)];
    family.mode.store(2, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(
        select(&catalog, LEFT, &args, &Trace::default()),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    every_prefix(|control| select(&catalog, LEFT, &args, control));
    for (mode, cause) in [
        (3, CompileControlError::Cancelled),
        (4, CompileControlError::DeadlineExceeded),
        (5, CompileControlError::ResourceExhausted),
    ] {
        family.mode.store(mode, std::sync::atomic::Ordering::SeqCst);
        let trace = Arc::new(Trace::default());
        *family.observation.lock().unwrap() = Some(trace.clone());
        assert!(
            matches!(select(&catalog, LEFT, &args, trace.as_ref()), Err(FunctionBindingError::Control(actual)) if actual == cause)
        );
        assert_eq!(
            trace.calls.lock().unwrap().len(),
            family
                .last_author_entry
                .load(std::sync::atomic::Ordering::SeqCst),
            "no completed observation or footer may follow the owner's primary Control"
        );
        *family.observation.lock().unwrap() = None;
    }
    // A declaration constructor has no compile-control scope to impersonate.
    // Its original catalogue error owns a failed declaration author.
    let invalid = FunctionDefinition::try_new_parametric_aggregate(
        NAME,
        FunctionVisibility::Public,
        FunctionVolatility::Immutable,
        [AggregateOverloadDeclaration::try_new(
            LEFT,
            "int64",
            "binary",
            "int64",
            "test/left/state-v1",
        )
        .unwrap()],
        family,
    );
    assert!(matches!(
        invalid,
        Err(FunctionCatalogError::InvalidStableIdentity { .. })
    ));
}

#[test]
fn fixed_binding_carrier_shares_actual_author_and_keeps_field_prefix() {
    let catalogue = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let cloned = catalogue.clone();
    let rebuilt = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    for name in ["abs", "field"] {
        let original = catalogue
            .definition(name, FunctionKind::Scalar)
            .unwrap()
            .binding_resolver()
            .unwrap();
        let loan = cloned
            .definition(name, FunctionKind::Scalar)
            .unwrap()
            .binding_resolver()
            .unwrap();
        let independent = rebuilt
            .definition(name, FunctionKind::Scalar)
            .unwrap()
            .binding_resolver()
            .unwrap();
        assert!(same_resolver_owner(original, loan));
        assert!(!same_resolver_owner(original, independent));
    }
    let args = [
        argument(DataType::Int64, false),
        argument(DataType::Int64, false),
    ];
    let selected = catalogue
        .resolve_bound_user(
            "field",
            FunctionKind::Scalar,
            request(&args),
            &Trace::default(),
        )
        .unwrap();
    let original = catalogue
        .definition("field", FunctionKind::Scalar)
        .unwrap()
        .binding_resolver()
        .unwrap();
    // The real FIELD wrapper checks its logical arity before entering its
    // fixed scalar delegate. Merely loaning the nested core would lose this.
    let wrong = FunctionBindingRequest {
        logical_argument_count: 1,
        ..request(&args)
    };
    let baseline = Trace::default();
    let failure = original
        .validate_selected(&selected.selected, wrong, &baseline)
        .unwrap_err();
    assert!(
        matches!(&failure, FunctionBindingError::InvalidBinding(message) if message.contains("field unsupported argument shape (1 total)"))
    );
    assert_eq!(
        *baseline.calls.lock().unwrap(),
        [(CompilePhase::FunctionSpecialization, 0)]
    );
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let stop = Trace {
            calls: Mutex::new(Vec::new()),
            stop: Some((1, cause.clone())),
        };
        assert!(
            matches!(original.validate_selected(&selected.selected,wrong,&stop), Err(FunctionBindingError::Control(actual)) if actual==cause)
        );
        assert_eq!(*stop.calls.lock().unwrap(), *baseline.calls.lock().unwrap());
    }
    original
        .validate_selected(&selected.selected, request(&args), &Trace::default())
        .unwrap();
}

fn same_resolver_owner(
    left: &FunctionBindingResolverOwner,
    right: &FunctionBindingResolverOwner,
) -> bool {
    std::ptr::addr_eq(left.as_ref(), right.as_ref())
}
