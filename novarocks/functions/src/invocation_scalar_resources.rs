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

//! Closed request facts for the original whole-invocation wrapper.
//! Only the audited prepared owner supplies a positive source; facts grant
//! no allocation authority.
//! Body, validation and concrete host callbacks retain their own authors.

use crate::json_object_resources::AllocationGeometry;
use crate::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, ScalarInvocationActivation,
    ScalarResourceError, Selection,
};
use arrow_array::StringArray;
use arrow_buffer::Buffer;
use arrow_data::BufferSpec;
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use std::alloc::Layout;
use std::sync::atomic::{AtomicBool, Ordering};

/// Numerical paths only. Ordinary contract failure is still published by the
/// original evaluator, with its original argument order, message and latch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InvocationRequestPath {
    EarlyFailure,
    ArgumentFailure,
    ValidateOnlyEmpty,
    Activated,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InvocationInputResources {
    pub(crate) path: InvocationRequestPath,
    pub(crate) metadata: AllocationGeometry,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationPreparationFailure {
    Source(ScalarResourceError),
    Kernel(KernelFailure),
}

/// Complete Functions-owned cumulative requests for ONE original evaluate.
/// A qualified concrete host composes its callback requests separately, only
/// when this original path can reach that host. These facts carry no grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WholeInvocationRequestFacts {
    geometry: AllocationGeometry,
    requires_host_callback: bool,
}
impl WholeInvocationRequestFacts {
    pub const fn bytes(self) -> usize {
        self.geometry.requested_bytes
    }
    pub const fn requests(self) -> usize {
        self.geometry.requests
    }
    pub const fn peak_bytes(self) -> usize {
        self.geometry.peak_bytes
    }
    pub const fn requires_host_callback(self) -> bool {
        self.requires_host_callback
    }
}

/// Source selected by the SAME audited prepared whole-invocation owner.
/// This pure profile does not qualify arbitrary controllers or Arrow carriers.
#[derive(Clone, Debug)]
pub struct WholeInvocationResourceProfile {
    source: WholeInvocationSource,
    prepared: std::sync::Weak<dyn crate::PreparedInvocationScalarKernel>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WholeInvocationSource {
    JsonObject,
}
impl WholeInvocationResourceProfile {
    pub(crate) fn json_object(
        prepared: std::sync::Weak<dyn crate::PreparedInvocationScalarKernel>,
    ) -> Self {
        Self {
            source: WholeInvocationSource::JsonObject,
            prepared,
        }
    }
    /// The original bounded diagnostic potentially created during preparation.
    /// The qualified Runtime controller's refusals themselves are bare enums.
    pub fn preparation_prefix(&self) -> WholeInvocationRequestFacts {
        WholeInvocationRequestFacts {
            geometry: preparation_prefix_geometry(),
            requires_host_callback: false,
        }
    }
    /// Borrow the original instance contract, arguments and explicit demand.
    /// Preparation neither calls nor latches the instance. Ordinary argument
    /// errors remain with its ONE original evaluate and original validator.
    pub fn requests_for(
        &self,
        instance: &crate::InvocationScalarEvaluationInstance,
        selection: Selection<'_>,
        arguments: &[EvaluatedArgument<'_>],
        activation: ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
    ) -> Result<WholeInvocationRequestFacts, InvocationPreparationFailure> {
        if !self
            .prepared
            .upgrade()
            .is_some_and(|owner| instance.matches_resource_owner(&owner))
        {
            return Err(InvocationPreparationFailure::Source(
                ScalarResourceError::Invariant(
                    "whole-invocation profile differs from its exact prepared instance",
                ),
            ));
        }
        let resources = match self.source {
            WholeInvocationSource::JsonObject => json_object_invocation_resources(
                instance.is_failed(),
                activation,
                instance.contract().value_argument_types(),
                arguments,
                selection,
                control,
            )?,
        };
        Ok(WholeInvocationRequestFacts {
            geometry: resources.geometry,
            requires_host_callback: resources.path == InvocationRequestPath::Activated,
        })
    }
}
impl From<KernelFailure> for InvocationPreparationFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<novarocks_type_contract::ValueTypeError> for InvocationPreparationFailure {
    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Source(ScalarResourceError::Invariant(
            "whole-invocation canonical type violates its prepared grammar",
        ))
    }
}
impl From<novarocks_type_contract::owned_resources::hashmap::HashMapResourceError>
    for InvocationPreparationFailure
{
    fn from(
        cause: novarocks_type_contract::owned_resources::hashmap::HashMapResourceError,
    ) -> Self {
        use novarocks_type_contract::owned_resources::hashmap::HashMapResourceError;
        Self::Source(match cause {
            HashMapResourceError::SourceModel(detail) => ScalarResourceError::SourceModel(detail),
            HashMapResourceError::Arithmetic(_) => ScalarResourceError::Arithmetic,
        })
    }
}
impl From<novarocks_type_contract::ControlResourceError> for InvocationPreparationFailure {
    fn from(cause: novarocks_type_contract::ControlResourceError) -> Self {
        use novarocks_type_contract::ControlResourceError;
        Self::Source(match cause {
            ControlResourceError::SourceModel(detail) => ScalarResourceError::SourceModel(detail),
            ControlResourceError::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            ) => ScalarResourceError::Arithmetic,
            ControlResourceError::Control(
                novarocks_type_contract::CompileControlError::Cancelled
                | novarocks_type_contract::CompileControlError::DeadlineExceeded,
            ) => ScalarResourceError::Invariant(
                "whole-invocation numerical Vec author returned a control refusal",
            ),
        })
    }
}

fn source_sum(
    left: AllocationGeometry,
    right: AllocationGeometry,
) -> Result<AllocationGeometry, InvocationPreparationFailure> {
    left.plus(right)
        .map_err(|_| InvocationPreparationFailure::Source(ScalarResourceError::Arithmetic))
}

/// The original Bucketed comparator builds one fresh table per reached Field
/// occurrence, even when several FieldRefs share an Arc. Its right-hand
/// canonical metadata bounds insertions; collision Vecs use the original
/// partitioned-push author. This walk allocates no table and never compares
/// expected with itself through the allocating semantic port.
pub(crate) fn argument_metadata_geometry(
    ty: &arrow_schema::DataType,
    control: &dyn KernelEvaluationControl,
) -> Result<AllocationGeometry, InvocationPreparationFailure> {
    use novarocks_type_contract::{
        MAX_VALUE_TYPE_NODES, ValueTypeVisit,
        owned_resources::{hashmap, vec},
        validate_value_type_structure_with_scratch_observed,
    };
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    let mut work = crate::kernel_input::EvaluationCheckpoints::new(control);
    let mut geometry = AllocationGeometry::default();
    validate_value_type_structure_with_scratch_observed(ty, &mut scratch, |event| {
        work.step()?;
        if let ValueTypeVisit::Field(field) = event {
            let entries = field.metadata().len();
            hashmap::original_fresh_insertion_allocation_requests_observed::<
                u64,
                Vec<(&str, &str)>,
                InvocationPreparationFailure,
            >(entries, &mut |layout| {
                work.step()?;
                geometry = source_sum(geometry, AllocationGeometry::exact(layout.size()))?;
                Ok(())
            })?;
            let collisions =
                vec::original_partitioned_fresh_push_request_bound::<(&str, &str)>(entries)?;
            geometry = source_sum(
                geometry,
                AllocationGeometry {
                    requests: collisions.allocation_requests_upper_bound,
                    requested_bytes: collisions.request_bytes_upper_bound,
                    peak_bytes: collisions.request_bytes_upper_bound,
                },
            )?;
        }
        Ok::<_, InvocationPreparationFailure>(())
    })?;
    work.finish()?;
    Ok(geometry)
}

enum ReachabilityFailure {
    Ordinary,
    Control(KernelFailure),
}
impl From<crate::EvaluationContractError> for ReachabilityFailure {
    fn from(_: crate::EvaluationContractError) -> Self {
        Self::Ordinary
    }
}
impl From<novarocks_type_contract::ValueTypeError> for ReachabilityFailure {
    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Ordinary
    }
}

fn argument_body_reachable(
    argument: EvaluatedArgument<'_>,
    selection: Selection<'_>,
    expected: &novarocks_type_contract::FunctionValueType,
    control: &dyn KernelEvaluationControl,
) -> Result<bool, KernelFailure> {
    use crate::kernel_input::{EvaluationCheckpoints, constant_type_matches, logical_is_null};
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        argument.validate_shape_observed::<ReachabilityFailure>(selection, || {
            work.step().map_err(ReachabilityFailure::Control)
        })?;
        if let EvaluatedArgument::Constant(value) = argument {
            work.step().map_err(ReachabilityFailure::Control)?;
            if !constant_type_matches(value.value_type(), expected) {
                return Ok(false);
            }
        }
        novarocks_type_contract::arrow_data_types_exact_borrowed_observed::<ReachabilityFailure>(
            argument.array().data_type(),
            &expected.data_type,
            || work.step().map_err(ReachabilityFailure::Control),
        )
    })();
    match result {
        Err(ReachabilityFailure::Control(cause)) => return Err(cause),
        Err(ReachabilityFailure::Ordinary) | Ok(false) => {
            work.finish()?;
            return Ok(false);
        }
        Ok(true) => {}
    }
    work.step()?;
    if !expected.nullable {
        for (ordinal, row) in selection.iter().enumerate() {
            match logical_is_null(
                argument.array().as_ref(),
                argument.value_row(ordinal, row),
                1,
                &mut work,
            ) {
                Ok(false) => {}
                Ok(true) | Err(_) => {
                    // finish consumes the original observation latch: a real
                    // callback refusal remains distinct from ordinary NULL
                    // or carrier addressing failure, regardless of category.
                    work.finish()?;
                    return Ok(false);
                }
            }
        }
    }
    work.finish()?;
    Ok(true)
}

/// Request preparation does not invoke, fail, or otherwise mutate an instance.
/// Failed/activation/arity paths read no argument array or row extent. A false
/// original argument fact stops later actual reads; only a fully reachable
/// Activated path may subsequently borrow JSON body geometry.
pub(crate) fn invocation_input_resources<'types>(
    failed: bool,
    activation: ScalarInvocationActivation,
    expected: impl IntoIterator<
        Item = &'types novarocks_type_contract::FunctionValueType,
        IntoIter: ExactSizeIterator,
    >,
    arguments: &[EvaluatedArgument<'_>],
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<InvocationInputResources, InvocationPreparationFailure> {
    let expected = expected.into_iter();
    let mut resources = InvocationInputResources {
        path: InvocationRequestPath::EarlyFailure,
        metadata: AllocationGeometry::default(),
    };
    if failed
        || (activation == ScalarInvocationActivation::ValidateOnly && !selection.is_empty())
        || expected.len() != arguments.len()
    {
        return Ok(resources);
    }
    control.checkpoint(0)?;
    for (argument, ty) in arguments.iter().zip(expected) {
        resources.metadata = source_sum(
            resources.metadata,
            argument_metadata_geometry(&ty.data_type, control)?,
        )?;
        if !argument_body_reachable(*argument, selection, ty, control)? {
            resources.path = InvocationRequestPath::ArgumentFailure;
            return Ok(resources);
        }
    }
    resources.path = match activation {
        ScalarInvocationActivation::ValidateOnly => InvocationRequestPath::ValidateOnlyEmpty,
        ScalarInvocationActivation::Activated => InvocationRequestPath::Activated,
    };
    Ok(resources)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JsonObjectInvocationResources {
    pub(crate) path: InvocationRequestPath,
    pub(crate) geometry: AllocationGeometry,
}

struct BorrowedArguments<'a>(&'a [EvaluatedArgument<'a>]);
impl crate::json_object_core::JsonObjectArguments for BorrowedArguments<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }
    fn array(&self, index: usize) -> &arrow_array::ArrayRef {
        self.0[index].array()
    }
    fn value_row(&self, index: usize, ordinal: usize, row: usize) -> usize {
        self.0[index].value_row(ordinal, row)
    }
}

/// Forward the original controller without creating an authority, meter or
/// diagnostic. The flag distinguishes an original refusal from the numerical
/// author's checked arithmetic/source failure, including diagnostic causes.
struct GeometryControl<'a> {
    original: &'a dyn KernelEvaluationControl,
    refused: AtomicBool,
}
impl KernelEvaluationControl for GeometryControl<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let result = self.original.checkpoint(units);
        if result.is_err() {
            self.refused.store(true, Ordering::Relaxed);
        }
        result
    }
    fn wait(&self, duration: std::time::Duration) -> Result<(), KernelFailure> {
        let result = self.original.wait(duration);
        if result.is_err() {
            self.refused.store(true, Ordering::Relaxed);
        }
        result
    }
}

/// One bounded original diagnostic can be constructed by NULL addressing in
/// preparation. The actual qualified Runtime controller returns bare enum
/// refusals; arbitrary diagnostic-producing controls are not qualified here.
pub(crate) fn preparation_prefix_geometry() -> AllocationGeometry {
    AllocationGeometry::exact(crate::MAX_ROW_ERROR_MESSAGE_BYTES)
}

/// All Functions-owned requests of ONE original evaluate, except the concrete
/// host's own callback requests. Runtime composes that same host's numerical
/// author separately. This private component grants nothing and does not
/// install a source profile. Body/Data already contains the two bounded
/// diagnostic slots and output group; neither is charged a second time here.
pub(crate) fn json_object_invocation_resources<'types>(
    failed: bool,
    activation: ScalarInvocationActivation,
    expected: impl IntoIterator<
        Item = &'types novarocks_type_contract::FunctionValueType,
        IntoIter: ExactSizeIterator,
    >,
    arguments: &[EvaluatedArgument<'_>],
    selection: Selection<'_>,
    control: &dyn KernelEvaluationControl,
) -> Result<JsonObjectInvocationResources, InvocationPreparationFailure> {
    let input =
        invocation_input_resources(failed, activation, expected, arguments, selection, control)?;
    let diagnostic = AllocationGeometry::exact(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    let geometry = match input.path {
        InvocationRequestPath::EarlyFailure if failed => AllocationGeometry::default(),
        InvocationRequestPath::EarlyFailure => diagnostic,
        InvocationRequestPath::ArgumentFailure => source_sum(input.metadata, diagnostic)?,
        InvocationRequestPath::ValidateOnlyEmpty => {
            let empty =
                validate_only_utf8_geometry().map_err(InvocationPreparationFailure::Source)?;
            source_sum(source_sum(input.metadata, empty)?, diagnostic)?
        }
        InvocationRequestPath::Activated => {
            crate::scalar_resources::require_arrow_model()
                .map_err(InvocationPreparationFailure::Source)?;
            let observed = GeometryControl {
                original: control,
                refused: AtomicBool::new(false),
            };
            let mut body = crate::json_object_resources::JsonObjectBatchResources::default();
            let arguments = BorrowedArguments(arguments);
            let geometry = (|| {
                for (ordinal, row) in selection.iter().enumerate() {
                    observed.checkpoint(1)?;
                    body.add_row(&arguments, ordinal, row, &observed)?;
                }
                body.body_and_data_geometry(selection.len())?
                    .plus(direct_data_slot_geometry(selection)?)
            })()
            .map_err(|cause| {
                if observed.refused.load(Ordering::Relaxed) {
                    InvocationPreparationFailure::Kernel(cause)
                } else {
                    InvocationPreparationFailure::Source(match cause {
                        KernelFailure::ResourceExhausted => ScalarResourceError::Arithmetic,
                        _ => ScalarResourceError::Invariant(
                            "whole-invocation JSONObject source geometry is invalid",
                        ),
                    })
                }
            })?;
            source_sum(input.metadata, geometry)?
        }
    };
    Ok(JsonObjectInvocationResources {
        path: input.path,
        geometry,
    })
}

pub(crate) fn direct_data_slot_geometry(
    selection: Selection<'_>,
) -> Result<AllocationGeometry, KernelFailure> {
    let (header, rows) =
        crate::builtin::scalar_invocation_data::direct_slot_request_layouts(selection)?;
    AllocationGeometry::exact(header.size()).plus(AllocationGeometry::exact(
        rows.map_or(0, |layout| layout.size()),
    ))
}

/// The original new_empty_array(Utf8), after successful argument validation.
/// This is the already-built instance's ValidateOnly constructor, not the
/// activated empty JSON builder or the earlier Frame bypass's funding.
pub(crate) fn validate_only_utf8_geometry() -> Result<AllocationGeometry, ScalarResourceError> {
    crate::scalar_resources::require_arrow_model()?;
    let array = arc_layout(Layout::new::<StringArray>()).map_err(|cause| match cause {
        LayoutResourceError::SourceModel => ScalarResourceError::SourceModel(
            "whole-invocation Arc request compiler source model drift",
        ),
        _ => ScalarResourceError::Arithmetic,
    })?;
    // The same audited non-pool Arrow Bytes envelope as ScalarV1: pointer
    // and length, tagged/niche Deallocation, and the two Arc counters.
    let bytes_arcs = 3 * 8 * size_of::<usize>();
    let bytes = [
        Layout::new::<i32>().size(),
        2 * size_of::<Buffer>(),
        bytes_arcs,
        array.size(),
        // One force_validate superset request. Zero-sized value/validity
        // payloads do not request blocks, but their original Bytes Arcs do.
        2 * size_of::<BufferSpec>(),
    ]
    .into_iter()
    .try_fold(0usize, |sum, bytes| {
        sum.checked_add(bytes)
            .ok_or(ScalarResourceError::Arithmetic)
    })?;
    Ok(AllocationGeometry {
        requests: 7,
        requested_bytes: bytes,
        // Sum of all original requests also bounds any simultaneous prefix;
        // this does not claim that transient tables remain in the result.
        peak_bytes: bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source_custody_resources_tests::measured;
    use arrow_array::{Array, ArrayRef, Int64Array, LargeListArray, new_empty_array};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct ProbeControl {
        calls: AtomicUsize,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for ProbeControl {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let index = self.calls.fetch_add(1, Ordering::Relaxed);
            if let Some((at, cause)) = &self.refusal
                && index == *at
            {
                return Err(cause.clone());
            }
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("request preparation must not wait");
        }
    }

    #[derive(Debug)]
    struct UnreadArray(StringArray);
    // SAFETY: Every buffer/lifetime operation delegates to the immutable
    // canonical array. Panicking observations witness forbidden later reads.
    unsafe impl Array for UnreadArray {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn to_data(&self) -> arrow_data::ArrayData {
            self.0.to_data()
        }
        fn into_data(self) -> arrow_data::ArrayData {
            self.0.into_data()
        }
        fn data_type(&self) -> &DataType {
            panic!("unreachable argument type read")
        }
        fn slice(&self, offset: usize, length: usize) -> ArrayRef {
            Arc::new(self.0.slice(offset, length))
        }
        fn len(&self) -> usize {
            panic!("unreachable argument length read")
        }
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
        fn offset(&self) -> usize {
            self.0.offset()
        }
        fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
            self.0.nulls()
        }
        fn get_buffer_memory_size(&self) -> usize {
            self.0.get_buffer_memory_size()
        }
        fn get_array_memory_size(&self) -> usize {
            self.0.get_array_memory_size()
        }
    }
    fn ty(data_type: DataType, nullable: bool) -> novarocks_type_contract::FunctionValueType {
        novarocks_type_contract::FunctionValueType::new(data_type, nullable)
    }

    struct RequestHost {
        typed: AtomicUsize,
        opaque: AtomicUsize,
    }
    impl crate::opaque_memory::OpaqueAllocationHost for RequestHost {
        fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
            self.opaque.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        }
        fn release_opaque(&self, bytes: usize) {
            assert!(self.opaque.fetch_sub(bytes, Ordering::Relaxed) >= bytes);
        }
    }
    impl crate::AggregateStateAllocator for RequestHost {
        fn allocate(&self, layout: Layout) -> Result<std::ptr::NonNull<u8>, KernelFailure> {
            let pointer = std::ptr::NonNull::new(unsafe { std::alloc::alloc(layout) })
                .ok_or(KernelFailure::ResourceExhausted)?;
            self.typed.fetch_add(layout.size(), Ordering::Relaxed);
            Ok(pointer)
        }
        unsafe fn release(&self, pointer: std::ptr::NonNull<u8>, layout: Layout) {
            unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
            assert!(self.typed.fetch_sub(layout.size(), Ordering::Relaxed) >= layout.size());
        }
        fn opaque_allocation_host(
            &self,
        ) -> Option<&dyn crate::opaque_memory::OpaqueAllocationHost> {
            Some(self)
        }
    }

    fn prepare_original_json(types: &[DataType]) -> Arc<dyn crate::PreparedInvocationScalarKernel> {
        use crate::*;
        use novarocks_type_contract::{
            CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
            ExpressionEffectContext, ExpressionUseId, SemanticParameters,
        };
        let catalog = crate::builtin::catalogue::builtin_engine_function_catalog();
        let definition = catalog
            .definition("json_object", FunctionKind::Scalar)
            .unwrap();
        let arguments: Vec<_> = types
            .iter()
            .map(|t| FunctionArgument::Value {
                value_type: FunctionValueType::new(t.clone(), true),
                constant: None,
            })
            .collect();
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: arguments.len(),
        };
        let selected = Arc::new(
            definition
                .binding_resolver()
                .unwrap()
                .resolve(request, crate::binding_test_control())
                .unwrap(),
        );
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            domain: EvaluationDomainId::new(7),
            demand: EvaluationDemand::Value,
        };
        let uses: Vec<_> = (0..types.len())
            .map(|i| Some(ExpressionUseId::new(42 + i as u32)))
            .collect();
        let parameters = SemanticParameters::try_new([]).unwrap();
        let prepared = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    context,
                    argument_uses: CallArgumentUses::SelectedChannels(&uses),
                    function_id: definition.binding_declaration().unwrap().function_id(),
                    kind: FunctionKind::Scalar,
                    selected: &selected,
                    request,
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    proof_scope: CallProofScope::Unconditional,
                },
                selected.clone(),
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(context),
                },
                crate::binding_test_control(),
            )
            .unwrap()
            .into_prepared();
        let PreparedPureKernel::ScalarInvocation(prepared) = prepared else {
            panic!("original installed JSONObject must retain its whole-invocation ABI");
        };
        prepared
    }

    #[test]
    fn invocation_scalar_resources_public_ticket_cannot_author_a_different_prepared_owner() {
        #[derive(Debug)]
        struct ForeignPrepared {
            original: Arc<dyn crate::PreparedInvocationScalarKernel>,
            profile: WholeInvocationResourceProfile,
        }
        impl crate::PreparedInvocationScalarKernel for ForeignPrepared {
            fn contract(&self) -> &Arc<crate::ScalarCallContract> {
                self.original.contract()
            }
            fn instance_retained_upper_bound(&self) -> usize {
                self.original.instance_retained_upper_bound()
            }
            fn instance_inline_allocation_bytes(&self) -> usize {
                self.original.instance_inline_allocation_bytes()
            }
            fn invocation_resource_profile(&self) -> Option<WholeInvocationResourceProfile> {
                Some(self.profile.clone())
            }
            fn create_instance_with_allocator(
                &self,
                host: Arc<dyn crate::AggregateStateAllocator>,
            ) -> Result<Box<dyn crate::InvocationScalarKernelInstance>, KernelFailure> {
                self.original.create_instance_with_allocator(host)
            }
        }
        let original = prepare_original_json(&[DataType::Utf8]);
        let profile = original.invocation_resource_profile().unwrap();
        let foreign: Arc<dyn crate::PreparedInvocationScalarKernel> = Arc::new(ForeignPrepared {
            original,
            profile: profile.clone(),
        });
        let host = Arc::new(RequestHost {
            typed: AtomicUsize::new(0),
            opaque: AtomicUsize::new(0),
        });
        let instance = crate::InvocationScalarEvaluationInstance::instantiate_with_allocator(
            foreign,
            Some(host.clone()),
        )
        .unwrap();
        let poison: ArrayRef = Arc::new(UnreadArray(StringArray::from(vec!["unread"])));
        let control = ProbeControl::default();
        let (result, requests) = measured(|| {
            instance
                .invocation_resource_profile()
                .unwrap()
                .requests_for(
                    &instance,
                    Selection::all(usize::MAX),
                    &[EvaluatedArgument::Column(&poison)],
                    ScalarInvocationActivation::Activated,
                    &control,
                )
        });
        assert_eq!(
            result,
            Err(InvocationPreparationFailure::Source(
                ScalarResourceError::Invariant(
                    "whole-invocation profile differs from its exact prepared instance",
                )
            ))
        );
        assert_eq!(requests, (0, 0, 0, 0));
        assert_eq!(control.calls.load(Ordering::Relaxed), 0);
        assert!(!instance.is_failed());
        drop(instance);
        assert_eq!(host.typed.load(Ordering::Relaxed), 0);
        assert_eq!(host.opaque.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn invocation_scalar_resources_public_profile_preserves_early_failure_and_latch() {
        let prepared = prepare_original_json(&[DataType::Utf8]);
        let profile = prepared.invocation_resource_profile().unwrap();
        let prefix = profile.preparation_prefix();
        assert_eq!(
            (prefix.bytes(), prefix.requests(), prefix.peak_bytes()),
            (512, 1, 512)
        );
        assert!(!prefix.requires_host_callback());
        let host = Arc::new(RequestHost {
            typed: AtomicUsize::new(0),
            opaque: AtomicUsize::new(0),
        });
        let mut instance = crate::InvocationScalarEvaluationInstance::instantiate_with_allocator(
            prepared,
            Some(host.clone()),
        )
        .unwrap();
        let poison: ArrayRef = Arc::new(UnreadArray(StringArray::from(vec!["unread"])));
        let arguments = [EvaluatedArgument::Column(&poison)];
        let control = ProbeControl::default();
        let selection = Selection::all(usize::MAX);
        let (facts, requests) = measured(|| {
            profile.requests_for(
                &instance,
                selection,
                &arguments,
                ScalarInvocationActivation::ValidateOnly,
                &control,
            )
        });
        let facts = facts.unwrap();
        assert_eq!(requests, (0, 0, 0, 0));
        assert_eq!((facts.bytes(), facts.requests()), (512, 1));
        assert!(!facts.requires_host_callback());
        assert!(!instance.is_failed());
        assert_eq!(control.calls.load(Ordering::Relaxed), 0);
        assert!(matches!(instance.evaluate(selection, &arguments,
            ScalarInvocationActivation::ValidateOnly, &control),
            Err(crate::ScalarInvocationFailure::Kernel(KernelFailure::InvalidProgram(ref cause)))
            if cause.message() == "nonempty scalar demand cannot be validation-only"));
        assert!(instance.is_failed());
        let facts = profile
            .requests_for(
                &instance,
                selection,
                &arguments,
                ScalarInvocationActivation::Activated,
                &control,
            )
            .unwrap();
        assert_eq!(
            (facts.bytes(), facts.requests(), facts.peak_bytes()),
            (0, 0, 0)
        );
        assert!(!facts.requires_host_callback());
        assert_eq!(control.calls.load(Ordering::Relaxed), 0);
        drop(instance);
        assert_eq!(host.typed.load(Ordering::Relaxed), 0);
        assert_eq!(host.opaque.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn invocation_scalar_resources_public_profile_refusal_does_not_call_or_fail_instance() {
        let prepared = prepare_original_json(&[DataType::Utf8]);
        let host = Arc::new(RequestHost {
            typed: AtomicUsize::new(0),
            opaque: AtomicUsize::new(0),
        });
        let mut instance = crate::InvocationScalarEvaluationInstance::instantiate_with_allocator(
            prepared,
            Some(host.clone()),
        )
        .unwrap();
        let profile = instance.invocation_resource_profile().unwrap();
        let key: ArrayRef = Arc::new(StringArray::from(vec!["key"]));
        let arguments = [EvaluatedArgument::Column(&key)];
        let control = ProbeControl {
            refusal: Some((0, KernelFailure::DeadlineExceeded)),
            ..Default::default()
        };
        assert_eq!(
            profile.requests_for(
                &instance,
                Selection::all(1),
                &arguments,
                ScalarInvocationActivation::Activated,
                &control
            ),
            Err(InvocationPreparationFailure::Kernel(
                KernelFailure::DeadlineExceeded
            ))
        );
        assert!(!instance.is_failed());
        let output = instance
            .evaluate(
                Selection::all(1),
                &arguments,
                ScalarInvocationActivation::Activated,
                &ProbeControl::default(),
            )
            .unwrap();
        let text = output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "{\"key\": null}");
        drop(output);
        drop(instance);
        assert_eq!(host.typed.load(Ordering::Relaxed), 0);
        assert_eq!(host.opaque.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn invocation_scalar_resources_complete_functions_geometry_covers_original_whole_evaluate() {
        let rows = [2, 8];
        let key: ArrayRef = Arc::new(StringArray::from(vec!["key"]));
        let value: ArrayRef = Arc::new(StringArray::from(vec!["{\"z\":[2,1],\"a\":\"µ\"}"; 9]));
        let empty: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
        let unsupported: ArrayRef = Arc::new(LargeListArray::new(
            Arc::new(Field::new("field\0µ".repeat(100), DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 0])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            None,
        ));
        for (arguments, selection, activation, data) in [
            (
                vec![
                    EvaluatedArgument::Scalar(&key),
                    EvaluatedArgument::Column(&value),
                ],
                Selection::try_sparse(9, &rows).unwrap(),
                ScalarInvocationActivation::Activated,
                false,
            ),
            (
                vec![
                    EvaluatedArgument::Scalar(&key),
                    EvaluatedArgument::Column(&empty),
                ],
                Selection::all(0),
                ScalarInvocationActivation::Activated,
                false,
            ),
            (
                vec![
                    EvaluatedArgument::Scalar(&key),
                    EvaluatedArgument::Column(&empty),
                ],
                Selection::all(0),
                ScalarInvocationActivation::ValidateOnly,
                false,
            ),
            (
                vec![EvaluatedArgument::Scalar(&unsupported)],
                Selection::try_sparse(9, &rows).unwrap(),
                ScalarInvocationActivation::Activated,
                true,
            ),
        ] {
            let types: Vec<_> = arguments
                .iter()
                .map(|a| a.array().data_type().clone())
                .collect();
            let prepared = prepare_original_json(&types);
            let host = Arc::new(RequestHost {
                typed: AtomicUsize::new(0),
                opaque: AtomicUsize::new(0),
            });
            let mut instance =
                crate::InvocationScalarEvaluationInstance::instantiate_with_allocator(
                    prepared,
                    Some(host.clone()),
                )
                .unwrap();
            let control = ProbeControl::default();
            let (bound, preparation) = measured(|| {
                instance
                    .invocation_resource_profile()
                    .expect("installed positive source")
                    .requests_for(&instance, selection, &arguments, activation, &control)
            });
            assert_eq!(preparation, (0, 0, 0, 0));
            let facts = bound.unwrap();
            assert_eq!(
                facts.requires_host_callback(),
                activation == ScalarInvocationActivation::Activated
            );
            let bound = facts.geometry;
            let (result, actual) =
                measured(|| instance.evaluate(selection, &arguments, activation, &control));
            assert!(actual.0 > 0 && actual.1 > 0);
            assert!(actual.0 <= bound.requested_bytes, "{actual:?} > {bound:?}");
            assert!(actual.1 <= bound.requests, "{actual:?} > {bound:?}");
            assert!(actual.2 <= bound.peak_bytes, "{actual:?} > {bound:?}");
            if data {
                assert!(
                    matches!(&result, Err(crate::ScalarInvocationFailure::Data(cause))
                    if cause.message().len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES)
                );
            } else {
                assert!(result.is_ok(), "{result:?}");
            }
            drop(result);
            drop(instance);
            assert_eq!(host.typed.load(Ordering::Relaxed), 0);
            assert_eq!(host.opaque.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn invocation_scalar_resources_source_arithmetic_is_distinct_from_same_kernel_callback() {
        let full = AllocationGeometry {
            requested_bytes: usize::MAX,
            requests: 1,
            peak_bytes: 0,
        };
        assert!(matches!(
            source_sum(full, AllocationGeometry::exact(1)),
            Err(InvocationPreparationFailure::Source(
                ScalarResourceError::Arithmetic
            ))
        ));
        assert_eq!(preparation_prefix_geometry().requests, 1);
        assert_eq!(
            preparation_prefix_geometry().requested_bytes,
            crate::MAX_ROW_ERROR_MESSAGE_BYTES
        );
        let poison: ArrayRef = Arc::new(UnreadArray(StringArray::from(vec!["unread"])));
        assert_eq!(
            json_object_invocation_resources(
                true,
                ScalarInvocationActivation::Activated,
                &[ty(DataType::Utf8, true)],
                &[EvaluatedArgument::Column(&poison)],
                Selection::all(usize::MAX),
                &ProbeControl::default()
            )
            .unwrap()
            .geometry,
            AllocationGeometry::default()
        );
        let input: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let control = ProbeControl {
            refusal: Some((0, KernelFailure::ResourceExhausted)),
            ..Default::default()
        };
        assert!(matches!(
            json_object_invocation_resources(
                false,
                ScalarInvocationActivation::Activated,
                &[ty(DataType::Int64, true)],
                &[EvaluatedArgument::Column(&input)],
                Selection::all(1),
                &control
            ),
            Err(InvocationPreparationFailure::Kernel(
                KernelFailure::ResourceExhausted
            ))
        ));
    }

    #[test]
    fn invocation_scalar_resources_early_paths_read_no_arrays_or_giant_selection_rows() {
        let poison: ArrayRef = Arc::new(UnreadArray(StringArray::from(vec!["unread"])));
        let expected = [ty(DataType::Utf8, true)];
        let args = [EvaluatedArgument::Column(&poison)];
        for (failed, activation, types, arguments) in [
            (
                true,
                ScalarInvocationActivation::Activated,
                expected.as_slice(),
                args.as_slice(),
            ),
            (
                false,
                ScalarInvocationActivation::ValidateOnly,
                expected.as_slice(),
                args.as_slice(),
            ),
            (
                false,
                ScalarInvocationActivation::Activated,
                expected.as_slice(),
                &[],
            ),
        ] {
            let control = ProbeControl::default();
            let (result, actual) = measured(|| {
                invocation_input_resources(
                    failed,
                    activation,
                    types,
                    arguments,
                    Selection::all(usize::MAX),
                    &control,
                )
            });
            let result = result.unwrap();
            assert_eq!(result.path, InvocationRequestPath::EarlyFailure);
            assert_eq!(result.metadata, AllocationGeometry::default());
            assert_eq!(control.calls.load(Ordering::Relaxed), 0);
            assert_eq!(actual, (0, 0, 0, 0));
        }
    }

    #[test]
    fn invocation_scalar_resources_first_bad_argument_stops_later_reads_and_preserves_error() {
        let poison: ArrayRef = Arc::new(UnreadArray(StringArray::from(vec!["unread"])));
        let short: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let wrong: ArrayRef = Arc::new(StringArray::from(vec![
            "huge".repeat(32_768),
            "type".into(),
        ]));
        let null: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(7)]));
        let expected = [ty(DataType::Int64, false), ty(DataType::Utf8, true)];
        for (first, message) in [
            (
                &short,
                "evaluated argument violates its exact selected carrier",
            ),
            (
                &wrong,
                "evaluated argument differs from its exact selected type",
            ),
            (&null, "non-null argument contains a selected SQL NULL"),
        ] {
            let args = [
                EvaluatedArgument::Column(first),
                EvaluatedArgument::Column(&poison),
            ];
            let control = ProbeControl::default();
            let (result, actual) = measured(|| {
                invocation_input_resources(
                    false,
                    ScalarInvocationActivation::Activated,
                    &expected,
                    &args,
                    Selection::all(2),
                    &control,
                )
            });
            assert_eq!(result.unwrap().path, InvocationRequestPath::ArgumentFailure);
            assert_eq!(actual, (0, 0, 0, 0));
            // Execute the original validation owner after numerical selection.
            // The later poisoned argument remains unreachable there too.
            let original: Result<(), KernelFailure> =
                args.iter().zip(&expected).try_for_each(|(arg, expected)| {
                    crate::kernel_input::validate_argument_observed(
                        *arg,
                        Selection::all(2),
                        expected,
                        &ProbeControl::default(),
                    )
                });
            assert!(
                matches!(original, Err(KernelFailure::InvalidProgram(ref cause))
                if cause.message() == message)
            );
        }
    }

    #[test]
    fn invocation_scalar_resources_metadata_counts_shared_field_occurrences_and_original_requests()
    {
        let control = ProbeControl::default();
        let metadata = (0..129)
            .map(|i| (format!("key{i}"), format!("value{i}")))
            .collect();
        let field = Arc::new(Field::new("shared", DataType::Int64, true).with_metadata(metadata));
        let once = DataType::Struct(vec![field.clone()].into());
        let twice = DataType::Struct(vec![field.clone(), field].into());
        let (bound, preparation) = measured(|| argument_metadata_geometry(&twice, &control));
        assert_eq!(preparation, (0, 0, 0, 0));
        let bound = bound.unwrap();
        let single = argument_metadata_geometry(&once, &control).unwrap();
        assert_eq!(bound, single.plus(single).unwrap());
        let (equal, actual) = measured(|| {
            novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
                &twice,
                &twice,
                || control.checkpoint(1),
            )
        });
        assert!(equal.unwrap());
        assert!(actual.0 > 0 && actual.1 > 0);
        assert!(actual.0 <= bound.requested_bytes, "{actual:?} > {bound:?}");
        assert!(actual.1 <= bound.requests, "{actual:?} > {bound:?}");
        assert!(actual.2 <= bound.peak_bytes, "{actual:?} > {bound:?}");
        assert_eq!(actual.3, 0);
    }

    #[test]
    fn invocation_scalar_resources_selected_domain_errors_and_empty_activation_remain_distinct() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(8)]));
        let left = Selection::try_sparse(10, &[2, 8]).unwrap();
        let right = Selection::try_sparse(10, &[3, 8]).unwrap();
        let errors = vec![crate::RowDataError::new(0, "unresolved")].into_boxed_slice();
        let selected =
            crate::SelectedValues::try_new(left, &DataType::Int64, array.clone(), errors).unwrap();
        let expected = [ty(DataType::Int64, true)];
        for selection in [left, right] {
            let result = invocation_input_resources(
                false,
                ScalarInvocationActivation::Activated,
                &expected,
                &[EvaluatedArgument::SelectedColumn(&selected)],
                selection,
                &ProbeControl::default(),
            )
            .unwrap();
            assert_eq!(result.path, InvocationRequestPath::ArgumentFailure);
        }
        let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let args = [EvaluatedArgument::Column(&empty)];
        for (activation, path) in [
            (
                ScalarInvocationActivation::Activated,
                InvocationRequestPath::Activated,
            ),
            (
                ScalarInvocationActivation::ValidateOnly,
                InvocationRequestPath::ValidateOnlyEmpty,
            ),
        ] {
            assert_eq!(
                invocation_input_resources(
                    false,
                    activation,
                    &expected,
                    &args,
                    Selection::all(0),
                    &ProbeControl::default()
                )
                .unwrap()
                .path,
                path
            );
        }
    }

    #[test]
    fn invocation_scalar_resources_callback_causes_are_not_ordinary_argument_failures() {
        let mut values = vec![Some(7); 512];
        values.push(None);
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let expected = [ty(DataType::Int64, false)];
        let args = [EvaluatedArgument::Column(&array)];
        let success = ProbeControl::default();
        assert_eq!(
            invocation_input_resources(
                false,
                ScalarInvocationActivation::Activated,
                &expected,
                &args,
                Selection::all(513),
                &success
            )
            .unwrap()
            .path,
            InvocationRequestPath::ArgumentFailure
        );
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::ResourceExhausted,
            KernelFailure::Internal(crate::KernelDiagnostic::new("original callback")),
            KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("original callback")),
        ] {
            for at in 0..success.calls.load(Ordering::Relaxed) {
                let control = ProbeControl {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert!(matches!(invocation_input_resources(false,
                    ScalarInvocationActivation::Activated, &expected, &args,
                    Selection::all(513), &control),
                    Err(InvocationPreparationFailure::Kernel(ref error)) if error == &cause));
                assert_eq!(control.calls.load(Ordering::Relaxed), at + 1);
            }
        }
    }

    #[test]
    fn invocation_scalar_resources_constant_reuses_root_identity_nullability_and_pool_ordinal() {
        use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
        let array: ArrayRef = Arc::new(StringArray::from(vec![None, Some("{}")]));
        let actual =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let field = Arc::new(actual.try_to_field("checked-json").unwrap());
        let pool = crate::ConstantPool::try_new(
            field,
            actual.clone(),
            array.to_data(),
            crate::ConstantPolicy {
                max_rows: 2,
                max_array_nodes: 8,
                max_logical_elements: 16,
                max_retained_buffer_bytes: 4096,
                max_type_depth: 8,
                max_type_nodes: 16,
                max_dictionary_depth: 2,
                max_metadata_bytes: 4096,
                max_library_validation_work: 65536,
                max_library_validation_bytes: 65536,
            },
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let value = pool.value(1).unwrap();
        let selection = Selection::try_sparse(1000, &[3, 67, 999]).unwrap();
        let mut nonnullable = actual.clone();
        nonnullable.nullable = false;
        for expected in [ty(DataType::Utf8, true), nonnullable] {
            let resources = invocation_input_resources(
                false,
                ScalarInvocationActivation::Activated,
                std::slice::from_ref(&expected),
                &[EvaluatedArgument::Constant(&value)],
                selection,
                &ProbeControl::default(),
            )
            .unwrap();
            assert_eq!(resources.path, InvocationRequestPath::ArgumentFailure);
            assert!(matches!(crate::kernel_input::validate_argument_observed(
                EvaluatedArgument::Constant(&value), selection, &expected, &ProbeControl::default()),
                Err(KernelFailure::InvalidProgram(ref message)) if message.message() ==
                "constant argument differs from its exact logical type or nullability"));
        }
        assert_eq!(
            invocation_input_resources(
                false,
                ScalarInvocationActivation::Activated,
                &[actual],
                &[EvaluatedArgument::Constant(&value)],
                selection,
                &ProbeControl::default()
            )
            .unwrap()
            .path,
            InvocationRequestPath::Activated
        );
    }

    #[test]
    fn invocation_scalar_resources_empty_constructor_covers_original_global_requests() {
        let (bound, preparation) = measured(validate_only_utf8_geometry);
        assert_eq!(preparation, (0, 0, 0, 0));
        let bound = bound.unwrap();
        let (array, actual) = measured(|| new_empty_array(&DataType::Utf8));
        assert_eq!(array.data_type(), &DataType::Utf8);
        assert_eq!(array.len(), 0);
        assert!(actual.0 > 0 && actual.1 > 0);
        assert!(actual.0 <= bound.requested_bytes, "{actual:?} > {bound:?}");
        assert!(actual.1 <= bound.requests, "{actual:?} > {bound:?}");
        assert!(actual.2 <= bound.peak_bytes, "{actual:?} > {bound:?}");
        drop(array);
    }

    #[test]
    fn invocation_scalar_resources_body_and_long_data_count_original_requests_once() {
        let unsupported: ArrayRef = Arc::new(LargeListArray::new(
            Arc::new(Field::new("field\0µ".repeat(1024), DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i64, 0])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            None,
        ));
        let inputs = [
            vec![
                Arc::new(StringArray::from(vec!["key"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["{\"z\":[2,1],\"a\":\"µ\"}"])),
            ],
            vec![unsupported],
        ];
        for arguments in inputs {
            let selection = Selection::all(1);
            let control = crate::source_custody_direct_tests::Control::default();
            let mut resources = crate::json_object_resources::JsonObjectBatchResources::default();
            let (bound, preparation) = measured(|| {
                resources.add_row(arguments.as_slice(), 0, 0, &control)?;
                resources.body_and_data_geometry(1)
            });
            assert_eq!(preparation, (0, 0, 0, 0));
            let bound = bound.unwrap();
            let (result, actual) =
                measured(|| crate::json_object_core::evaluate_original(&arguments, selection));
            assert!(actual.0 > 0 && actual.1 > 0);
            assert!(actual.0 <= bound.requested_bytes, "{actual:?} > {bound:?}");
            assert!(actual.1 <= bound.requests, "{actual:?} > {bound:?}");
            assert!(actual.2 <= bound.peak_bytes, "{actual:?} > {bound:?}");
            if let Err(message) = &result {
                assert!(message.len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES);
                assert!(message.starts_with("unsupported scalar type: LargeList"));
            }
            drop(result);
        }
    }
}
