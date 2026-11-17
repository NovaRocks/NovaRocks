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

//! Selected Theta lifecycle through the original family, canonical bytes and CPU.
//! Resource facts describe requests; host scopes remain the allocation authority.

use super::*;
use arrow_buffer::{Buffer, OffsetBuffer};
use novarocks_functions::*;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompileControlError,
    CompilePhase, FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior,
    ObservableEffects, PureCompileControl,
};
use std::alloc::Layout;

fn invalid(message: &str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}
fn internal(message: &str) -> KernelFailure {
    KernelFailure::Internal(KernelDiagnostic::new(message))
}
fn compile_failure(cause: CompileControlError) -> KernelFailure {
    KernelFailure::CompileControl(cause)
}
fn author_failure(error: IcebergThetaError) -> KernelFailure {
    match error {
        IcebergThetaError::InvalidCompact(_)
        | IcebergThetaError::CompactTooLarge { .. }
        | IcebergThetaError::Sketch(_) => {
            KernelFailure::Operational(KernelDiagnostic::new(&error.to_string()))
        }
        IcebergThetaError::MixedUpdateAndMerge => {
            KernelFailure::Operational(KernelDiagnostic::new(&error.to_string()))
        }
        IcebergThetaError::OutputResourceExhausted => KernelFailure::ResourceExhausted,
        _ => internal(&error.to_string()),
    }
}
fn observed<T>(
    control: &dyn KernelEvaluationControl,
    body: impl FnOnce(&mut EvaluationCheckpoints<'_>) -> Result<T, KernelFailure>,
) -> Result<T, KernelFailure> {
    let observation = KernelControlObservation::new(control);
    observation.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(&observation);
    let result = body(&mut work);
    observation.finish(work.finish_result(result))
}
fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Aggregate,
        instance_state: FunctionInstanceState::AggregateInstance,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

struct ThetaPureOwner {
    resolver: novarocks_functions::FunctionBindingResolverOwner,
    family: Arc<IcebergThetaAggregateFamily>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ThetaPureOwner {
    fn from_registration(
        registration: &TypedAggregateRegistration<IcebergThetaAggregateFamily>,
    ) -> Result<Self, FunctionCatalogError> {
        let original = registration.definition();
        let missing = || FunctionCatalogError::InvalidStableIdentity {
            subject: "Iceberg Theta original binding owner",
            value: ICEBERG_THETA_AGGREGATE_NAME.into(),
        };
        let resolver = original.binding_resolver().ok_or_else(missing)?.clone();
        let original_declaration = original.binding_declaration().ok_or_else(missing)?;
        // Only effects are authored here. Identity/pattern/state law and format
        // stay with the same original parametric binding declaration.
        let declaration = FunctionBindingDeclaration::try_new_complete(
            original_declaration.function_id().clone(),
            original_declaration.kind(),
            original_declaration.overloads().iter().map(|overload| {
                FunctionOverloadDeclaration::from_effects(
                    overload.identity.clone(),
                    overload.argument_pattern.clone(),
                    overload.result_pattern.clone(),
                    overload.aggregate.clone(),
                    effects(),
                )
            }),
        )
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "Iceberg Theta pure binding declaration",
            value: error.to_string().into(),
        })?;
        let implementation = PureImplementationId::try_new(ICEBERG_THETA_IMPLEMENTATION_IDENTITY)?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::AggregateV1,
            })
            .collect();
        Ok(Self {
            resolver,
            family: Arc::clone(registration.family()),
            declaration,
            implementations,
        })
    }
    fn call_effects(&self, proof_scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope,
        }
    }
}
impl FunctionBindingResolver for ThetaPureOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.resolve(request, control)
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver
            .select_at_overload_observed(overload, request, control)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver.validate_selected(selected, request, control)
    }
}
impl AggregateSignatureResolver for ThetaPureOwner {
    fn borrow_update_signature<'a>(
        &'a self,
        selected_overload: &str,
        arguments: &'a [FunctionValueType],
    ) -> Result<Option<AggregateUpdateSignatureRecipe<'a>>, FunctionResolutionError> {
        self.family
            .borrow_update_signature(selected_overload, arguments)
    }
    fn validate_value_arguments(
        &self,
        arguments: &[FunctionValueType],
    ) -> Result<(), FunctionResolutionError> {
        self.family.validate_value_arguments(arguments)
    }
    fn resolve_aggregate(
        &self,
        arguments: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.family.resolve_signature(arguments)
    }
    fn resolve_update_signature(
        &self,
        overload: &AggregateOverloadIdentity,
        arguments: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.family.resolve_update_signature(overload, arguments)
    }
    fn produces_null(&self) -> bool {
        self.family.produces_null()
    }
    fn supports_ordered_update_channels(&self) -> bool {
        self.family.supports_ordered_update_channels()
    }
    fn state_argument_contract(
        &self,
        overload: &AggregateOverloadIdentity,
    ) -> Result<AggregateStateArgumentContract, FunctionResolutionError> {
        self.family.state_argument_contract(overload)
    }
}
impl PureFunctionMetadataOwner for ThetaPureOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        // Keep the exact registration's original parametric author. The pure
        // effects wrapper neither rebuilds it nor chooses a new family.
        self.resolver.clone()
    }
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}
impl FunctionEffectOwner for ThetaPureOwner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refine_core(input, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>> {
        self.refine_core(input, control, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(CheckedOwnerFailure::Original)
    }
}
impl PureAggregateImplementation for ThetaPureOwner {
    type Kernel = PreparedThetaKernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<PreparedThetaKernel>, KernelFailure> {
        self.prepare_core(input, contract, control, false, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_aggregate_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<PreparedThetaKernel>, CheckedOwnerFailure<KernelFailure>> {
        self.prepare_core(input, contract, control, true, || {
            checked.require_binding_author(self.resolver.as_ref(), input)
        })
        .map_err(CheckedOwnerFailure::Original)
    }
}

impl ThetaPureOwner {
    fn refine_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Aggregate;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let scope = input.environment.is_empty()
                && (input.proof_scope == CallProofScope::Unconditional
                    || input.proof_scope == CallProofScope::Region(input.context.region));
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "Iceberg Theta requires its original scope and no environment".into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(cause) => FunctionEffectOwnerError::Control(cause),
                error => FunctionEffectOwnerError::Owner(error),
            })?;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            Ok(self.call_effects(input.proof_scope))
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
    }
    fn prepare_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
        borrow_signature: bool,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<PreparedThetaKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            let call = contract.call();
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Aggregate
                && call.function_id() == input.function_id
                && std::ptr::eq(call.selected(), input.selected)
                && call.context() == input.context
                && call.decimal_overflow_policy() == input.decimal_overflow_policy
                && call.effects() == &self.call_effects(input.proof_scope)
                && call.logical_argument_count() == input.request.logical_argument_count;
            work.step().map_err(compile_failure)?;
            if !exact {
                return Err(invalid(
                    "Iceberg Theta preparation differs from its checked call",
                ));
            }
            let supported = !contract.distinct()
                && contract.order_keys().is_empty()
                && input.request.logical_argument_count == 1
                && input.request.arguments.len() == 1;
            work.step().map_err(compile_failure)?;
            if !supported {
                return Err(invalid(
                    "Iceberg Theta requires one logical argument and rejects DISTINCT or function ORDER BY",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(cause) => compile_failure(cause),
                _ => invalid("Iceberg Theta preparation has a stale selected binding"),
            })?;
            let (
                FunctionArgument::Value {
                    value_type: actual, ..
                },
                FunctionArgumentType::Value(selected),
            ) = (
                &input.request.arguments[0],
                &input.selected.argument_types[0],
            )
            else {
                work.step().map_err(compile_failure)?;
                return Err(invalid(
                    "Iceberg Theta requires its canonical value argument",
                ));
            };
            if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                work.step().map_err(compile_failure)
            })? {
                return Err(invalid(
                    "Iceberg Theta request differs from its selected source",
                ));
            }
            let physical_binary = |ty: &FunctionValueType| {
                ty.logical_type() == ValueLogicalType::Physical
                    && ty.data_type() == &DataType::Binary
                    && !ty.nullable()
            };
            let valid = physical_binary(contract.final_type())
                && physical_binary(contract.intermediate_type())
                && contract.state_format().as_str() == ICEBERG_THETA_STATE_FORMAT_IDENTITY;
            work.step().map_err(compile_failure)?;
            if !valid {
                return Err(invalid(
                    "Iceberg Theta requires its original Binary state, result and format",
                ));
            }
            // The original family is still the sole fixed signature/options author.
            work.flush().map_err(compile_failure)?;
            let signature = if borrow_signature {
                // The checked path borrows the already-validated source and
                // exact contract. It never builds an owned signature first.
                None
            } else {
                let mut carriers = Vec::new();
                carriers
                    .try_reserve_exact(1)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                work.step().map_err(compile_failure)?;
                carriers.push(actual.data_type().clone());
                work.step().map_err(compile_failure)?;
                work.flush().map_err(compile_failure)?;
                let signature = ResolvedAggregateSignature {
                    overload: AggregateOverloadIdentity::try_new(input.selected.overload.as_str())
                        .map_err(|_| {
                            invalid("Iceberg Theta selected overload identity is invalid")
                        })?,
                    argument_types: carriers,
                    intermediate_type: contract.intermediate_type().data_type().clone(),
                    output_type: contract.final_type().data_type().clone(),
                    state_format: contract.state_format().clone(),
                };
                Some(signature)
            };
            let options = AggregateBindOptions::try_new(false, &[], &[], None)
                .map_err(|_| invalid("Iceberg Theta canonical options are invalid"))?;
            let original = match &signature {
                Some(signature) => self.family.prepare(signature, &options),
                None => self.family.prepare_borrowed(
                    input.selected.overload.as_str(),
                    std::slice::from_ref(actual.data_type()),
                    contract.intermediate_type().data_type(),
                    contract.final_type().data_type(),
                    contract.state_format(),
                    &options,
                ),
            }
            .map_err(author_failure);
            work.step().map_err(compile_failure)?;
            let original = original?;
            work.flush().map_err(compile_failure)?;
            let prepared = Arc::new(PreparedThetaKernel { contract, original });
            work.step().map_err(compile_failure)?;
            work.flush().map_err(compile_failure)?;
            Ok(prepared)
        })();
        // A nested author may have already refused its original callback.
        // Finish this same scope without starting a callback after that result.
        work.finish_result(result, compile_failure)
    }
}

pub(super) fn attach_registration(
    registration: TypedAggregateRegistration<IcebergThetaAggregateFamily>,
) -> Result<TypedAggregateRegistration<IcebergThetaAggregateFamily>, FunctionCatalogError> {
    let owner = Arc::new(ThetaPureOwner::from_registration(&registration)?);
    registration
        .try_attach_pure_aggregate(owner)
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "Iceberg Theta installed pure owner",
            value: error.to_string().into(),
        })
}

/// The process registration and pure metadata use the same family and CPU.
pub fn iceberg_theta_pure_definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    Ok(iceberg_theta_registration()?.definition().clone())
}

pub struct PreparedThetaKernel {
    contract: Arc<AggregateCallContract>,
    original: IcebergThetaKernel,
}
impl fmt::Debug for PreparedThetaKernel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedThetaKernel")
            .field("contract", &self.contract)
            .finish_non_exhaustive()
    }
}
pub struct PreparedThetaUpdate<'a> {
    input: SelectedAggregateUpdateInput<'a, 'a>,
    canonical: PreparedCanonicalBatch<'a>,
}
pub struct PreparedThetaMerge<'a> {
    input: SelectedAggregateMergeInput<'a, 'a>,
    binary: &'a BinaryArray,
}
impl PreparedAggregateKernel for PreparedThetaKernel {
    type State = IcebergThetaState;
    type PreparedUpdateBatch<'batch> = PreparedThetaUpdate<'batch>;
    type PreparedMergeBatch<'batch> = PreparedThetaMerge<'batch>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        self.original.memory_policy()
    }
    fn retained_bytes(&self, state: &Self::State) -> usize {
        self.original.retained_bytes(state)
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::State, KernelFailure> {
        observed(control, |work| {
            work.flush()?;
            let state = self.original.create_state().map_err(author_failure);
            work.step()?;
            work.flush()?;
            state
        })
    }
    fn prepare_update<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<PreparedThetaUpdate<'a>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && self.contract.phase().consumes_logical_arguments()
                && input.logical_arguments().len() == 1
                && input.order_arguments().is_empty();
            work.step()?;
            if !valid {
                return Err(invalid(
                    "Iceberg Theta update differs from its exact phase or channels",
                ));
            }
            work.flush()?;
            let canonical =
                PreparedCanonicalBatch::try_new(input.logical_arguments()[0].array().as_ref())
                    .map_err(author_failure);
            work.step()?;
            work.flush()?;
            Ok(PreparedThetaUpdate {
                input,
                canonical: canonical?,
            })
        })
    }
    fn update_row<'a>(
        &self,
        state: &mut Self::State,
        prepared: &PreparedThetaUpdate<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.input.selection().row(ordinal);
            work.step()?;
            let row = row
                .ok_or_else(|| invalid("Iceberg Theta selected update ordinal is out of bounds"))?;
            let argument = prepared.input.logical_arguments()[0];
            let address = argument.value_row(ordinal, row);
            let valid = address < argument.array().len();
            work.step()?;
            if !valid {
                return Err(internal(
                    "Iceberg Theta selected update address is outside its carrier",
                ));
            }
            work.flush()?;
            let result = self
                .original
                .update_row(state, &prepared.canonical, address)
                .map_err(author_failure);
            work.step()?;
            work.flush()?;
            result
        })
    }
    fn prepare_merge<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<PreparedThetaMerge<'a>, KernelFailure> {
        observed(control, |work| {
            let valid = std::ptr::eq(input.contract(), self.contract.as_ref())
                && !self.contract.phase().consumes_logical_arguments();
            work.step()?;
            if !valid {
                return Err(invalid("Iceberg Theta merge differs from its exact phase"));
            }
            let binary = input.state().array().as_any().downcast_ref::<BinaryArray>();
            work.step()?;
            let binary =
                binary.ok_or_else(|| internal("Iceberg Theta merge carrier is not BinaryArray"))?;
            Ok(PreparedThetaMerge { input, binary })
        })
    }
    fn merge_row<'a>(
        &self,
        state: &mut Self::State,
        prepared: &PreparedThetaMerge<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        observed(control, |work| {
            let row = prepared.input.selection().row(ordinal);
            work.step()?;
            let row = row
                .ok_or_else(|| invalid("Iceberg Theta selected merge ordinal is out of bounds"))?;
            let address = prepared.input.state().value_row(ordinal, row);
            let valid = address < prepared.binary.len();
            work.step()?;
            if !valid {
                return Err(internal(
                    "Iceberg Theta selected merge address is outside its carrier",
                ));
            }
            work.flush()?;
            let result = self
                .original
                .merge_row(state, &prepared.binary, address)
                .map_err(author_failure);
            work.step()?;
            work.flush()?;
            result
        })
    }
    fn build_intermediate<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'a,
        I: ExactSizeIterator<Item = &'a Self::State>,
    {
        build_output_observed(states, Some(control))
    }
    fn build_final<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        Self::State: 'a,
        I: ExactSizeIterator<Item = &'a Self::State>,
    {
        build_output_observed(states, Some(control))
    }
}

/// Pinned lg_k=12 compaction may coexist with its mapped entry Vec; during
/// serialization the compact entries coexist with the 64+n*8 byte request.
/// These are library request extents, not a complete allocator/host grant.
pub const THETA_EMISSION_LIBRARY_REQUEST_BYTES: usize = 2 * MAX_TABLE_RETAINED_BYTES + 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThetaOutputResourceFacts {
    pub state_reference_request_bytes: usize,
    pub offsets_request_bytes: usize,
    pub payload_request_bytes: usize,
    pub library_temporary_request_bytes: usize,
    pub array_owner_request_bytes: usize,
    pub combined_request_bytes: usize,
}
/// Actual rows and measured serialized payload are required. No per-state
/// worst-case output invoice is substituted for empty or sparse sketches.
pub fn theta_output_resource_facts(
    rows: usize,
    payload: usize,
) -> Result<ThetaOutputResourceFacts, KernelFailure> {
    let offsets = rows
        .checked_add(1)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let refs = Layout::array::<&IcebergThetaState>(rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    let offset_bytes = Layout::array::<i32>(offsets)
        .map_err(|_| KernelFailure::ResourceExhausted)?
        .size();
    Layout::array::<u8>(payload).map_err(|_| KernelFailure::ResourceExhausted)?;
    i32::try_from(payload).map_err(|_| KernelFailure::ResourceExhausted)?;
    novarocks_functions::selected_copy::guarded_interleave_extent(&DataType::Binary, rows)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(THETA_EMISSION_LIBRARY_REQUEST_BYTES)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    // ArcInner has its two counters followed by the concrete Array body.
    // This is the original Arc allocation request geometry, not an arena grant.
    let (array_owner, _) = Layout::new::<[usize; 2]>()
        .extend(Layout::new::<BinaryArray>())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let array_owner = array_owner.pad_to_align().size();
    let combined = refs
        .checked_add(offset_bytes)
        .and_then(|n| n.checked_add(payload))
        .and_then(|n| n.checked_add(THETA_EMISSION_LIBRARY_REQUEST_BYTES))
        .and_then(|n| n.checked_add(array_owner))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(ThetaOutputResourceFacts {
        state_reference_request_bytes: refs,
        offsets_request_bytes: offset_bytes,
        payload_request_bytes: payload,
        library_temporary_request_bytes: THETA_EMISSION_LIBRARY_REQUEST_BYTES,
        array_owner_request_bytes: array_owner,
        combined_request_bytes: combined,
    })
}
fn output_step(work: &mut Option<EvaluationCheckpoints<'_>>) -> Result<(), KernelFailure> {
    if let Some(work) = work {
        work.step()?;
    }
    Ok(())
}
fn output_boundary(work: &mut Option<EvaluationCheckpoints<'_>>) -> Result<(), KernelFailure> {
    if let Some(work) = work {
        work.flush()?;
    }
    Ok(())
}
fn output_reserve<T>(
    count: usize,
    work: &mut Option<EvaluationCheckpoints<'_>>,
) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(count).map_err(|_| KernelFailure::ResourceExhausted)?;
    output_boundary(work)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    output_step(work)?;
    output_boundary(work)?;
    Ok(values)
}
enum SerializedThetaBytes {
    Empty,
    Owned(Vec<u8>),
}
impl SerializedThetaBytes {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Empty => &EMPTY_ORDERED_COMPACT_V3,
            Self::Owned(bytes) => bytes,
        }
    }
}
fn state_bytes(
    state: &IcebergThetaState,
    work: &mut Option<EvaluationCheckpoints<'_>>,
) -> Result<SerializedThetaBytes, KernelFailure> {
    let untouched = matches!(&state.mode, ThetaStateMode::Update(_)) && !state.has_updates;
    output_step(work)?;
    if untouched {
        return Ok(SerializedThetaBytes::Empty);
    }
    // Validate the pinned opaque request before either library allocation.
    Layout::array::<u8>(THETA_EMISSION_LIBRARY_REQUEST_BYTES)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    output_boundary(work)?;
    let compact = match &state.mode {
        ThetaStateMode::Update(sketch) => sketch.compact(true),
        ThetaStateMode::Merge(union) => union.to_sketch(true),
    };
    output_step(work)?;
    output_boundary(work)?;
    let empty = compact.is_empty();
    output_step(work)?;
    if empty {
        return Ok(SerializedThetaBytes::Empty);
    }
    output_boundary(work)?;
    let bytes = serialize_canonical_compact(&compact);
    output_step(work)?;
    output_boundary(work)?;
    if bytes.len() > ICEBERG_THETA_MAX_COMPACT_BYTES {
        return Err(internal(
            "Iceberg Theta original serialization exceeded its compact extent",
        ));
    }
    Ok(SerializedThetaBytes::Owned(bytes))
}
/// The sole emission body serves selected and legacy typed lifecycles. The
/// legacy path has no observation scope rather than a fabricated host control.
pub(super) fn build_output_observed<'a, I>(
    mut states: I,
    control: Option<&dyn KernelEvaluationControl>,
) -> Result<ArrayRef, KernelFailure>
where
    I: ExactSizeIterator<Item = &'a IcebergThetaState>,
{
    let observation = control.map(KernelControlObservation::new);
    let borrowed_control = observation
        .as_ref()
        .map(|observer| observer as &dyn KernelEvaluationControl);
    if let Some(control) = borrowed_control {
        control.checkpoint(0)?;
    }
    let mut work = borrowed_control.map(EvaluationCheckpoints::new);
    let result = (|| {
        let rows = states.len();
        // Both borrowed scratch and output header geometry precede reserve.
        theta_output_resource_facts(rows, 0)?;
        let mut sources = output_reserve::<&IcebergThetaState>(rows, &mut work)?;
        for _ in 0..rows {
            output_boundary(&mut work)?;
            let state = states.next();
            output_step(&mut work)?;
            output_boundary(&mut work)?;
            let state = state.ok_or_else(|| {
                invalid("Iceberg Theta state iterator ended before its exact length")
            })?;
            sources.push(state);
            output_step(&mut work)?;
        }
        output_boundary(&mut work)?;
        let extra = states.next();
        output_step(&mut work)?;
        output_boundary(&mut work)?;
        if extra.is_some() {
            return Err(invalid(
                "Iceberg Theta state iterator exceeds its exact length",
            ));
        }
        let mut payload = 0usize;
        for state in &sources {
            let bytes = state_bytes(state, &mut work)?;
            payload = payload
                .checked_add(bytes.bytes().len())
                .ok_or(KernelFailure::ResourceExhausted)?;
            output_step(&mut work)?;
            i32::try_from(payload).map_err(|_| KernelFailure::ResourceExhausted)?;
        }
        theta_output_resource_facts(rows, payload)?;
        let count = rows
            .checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let mut offsets = output_reserve::<i32>(count, &mut work)?;
        let mut values = output_reserve::<u8>(payload, &mut work)?;
        offsets.push(0);
        output_step(&mut work)?;
        for state in sources {
            let bytes = state_bytes(state, &mut work)?;
            let end = values
                .len()
                .checked_add(bytes.bytes().len())
                .ok_or(KernelFailure::ResourceExhausted)?;
            if end > payload {
                return Err(internal(
                    "Iceberg Theta state emission exceeds its measured payload",
                ));
            }
            for byte in bytes.bytes() {
                values.push(*byte);
                output_step(&mut work)?;
            }
            offsets
                .push(i32::try_from(values.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            output_step(&mut work)?;
        }
        if values.len() != payload {
            return Err(internal(
                "Iceberg Theta state emission differs from its measured payload",
            ));
        }
        output_boundary(&mut work)?;
        let output = BinaryArray::try_new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(values),
            None,
        );
        output_step(&mut work)?;
        output_boundary(&mut work)?;
        let output = output
            .map_err(|_| internal("Iceberg Theta output violates its original Binary format"))?;
        output_boundary(&mut work)?;
        let output = Arc::new(output) as ArrayRef;
        output_step(&mut work)?;
        output_boundary(&mut work)?;
        Ok(output)
    })();
    let result = match work {
        Some(work) => work.finish_result(result),
        None => result,
    };
    match observation {
        Some(observation) => observation.finish(result),
        None => result,
    }
}

#[cfg(test)]
#[path = "theta_pure_tests.rs"]
mod tests;
