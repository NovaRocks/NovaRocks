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

//! Actual construction and result custody from the statement's own window.

use novarocks_expression::{
    ExprEvaluationFailure, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts,
};
use novarocks_failure_contract::{SharedFailureCause, SourceRetentionCapability};
use novarocks_functions::opaque_memory::OpaqueAllocationHost;
use novarocks_functions::{
    AggregateStateAllocator, KernelDiagnostic, KernelFailure, ScalarResourceError,
};
use novarocks_sql::compiler::SqlConstantEvaluationError;
use novarocks_type_contract::owned_resources::layout::{LayoutResourceError, arc_layout};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueSchemaCreationFactory,
    ValueSchemaCreationScope, ValueSchemaScopeBeginFailure, ValueTypeStorageHold,
    ValueTypeStorageLifetime,
};
use novarocks_workload_control::{ResultPayloadBytes, ResultWindowAlias, WorkError, WorkScope};
use std::{
    alloc::Layout,
    convert::Infallible,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

fn source(error: WorkError) -> CompileControlError {
    CompileControlError::Source(SharedFailureCause::new(error))
}
fn extent(bytes: usize) -> Result<u64, CompileControlError> {
    u64::try_from(bytes).map_err(|_| source(WorkError::ArithmeticOverflow))
}
fn layout_error(error: LayoutResourceError) -> CompileControlError {
    CompileControlError::Source(SharedFailureCause::new(error))
}

struct Backing {
    bytes: Mutex<ResultPayloadBytes>,
}
impl ValueTypeStorageHold for Backing {}
impl SourceRetentionCapability for BackingGuard {}
struct BackingGuard {
    _backing: Arc<Backing>,
}
impl std::fmt::Debug for BackingGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RequiredConstantBacking")
    }
}
impl Backing {
    fn new(window: &ResultWindowAlias, initial: usize) -> Result<Arc<Self>, CompileControlError> {
        let holder = arc_layout(Layout::new::<Self>())
            .map_err(layout_error)?
            .size();
        let bytes = initial
            .checked_add(holder)
            .ok_or_else(|| source(WorkError::ArithmeticOverflow))?;
        let bytes = window
            .reserve_payload_bytes(extent(bytes)?)
            .map_err(source)?;
        Ok(Arc::new(Self {
            bytes: Mutex::new(bytes),
        }))
    }
    fn reserve(&self, bytes: usize) -> Result<(), CompileControlError> {
        self.bytes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .try_grow(extent(bytes)?)
            .map_err(source)
    }
    fn destroyed(&self, bytes: usize) {
        self.bytes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .release_destroyed(u64::try_from(bytes).expect("previously admitted request width"))
            .expect("original required constant backing receipt");
    }
    fn storage(self: &Arc<Self>) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.clone())
    }
}

pub(super) struct RootAllocator {
    backing: Arc<Backing>,
    _holder: Arc<Backing>,
}
impl RootAllocator {
    fn new(window: &ResultWindowAlias) -> Result<Arc<Self>, CompileControlError> {
        let holder = Backing::new(
            window,
            arc_layout(Layout::new::<Self>())
                .map_err(layout_error)?
                .size(),
        )?;
        Ok(Arc::new(Self {
            backing: Backing::new(window, 0)?,
            _holder: holder,
        }))
    }
}
impl OpaqueAllocationHost for RootAllocator {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.backing
            .reserve(bytes)
            .map_err(KernelFailure::CompileControl)
    }
    fn release_opaque(&self, bytes: usize) {
        self.backing.destroyed(bytes)
    }
}
impl AggregateStateAllocator for RootAllocator {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if layout.size() == 0 {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "required constant host received an empty block",
            )));
        }
        self.backing
            .reserve(layout.size())
            .map_err(KernelFailure::CompileControl)?;
        // SAFETY: the original host contract supplies a valid nonzero layout.
        match NonNull::new(unsafe { std::alloc::alloc(layout) }) {
            Some(block) => Ok(block),
            None => {
                self.backing.destroyed(layout.size());
                Err(KernelFailure::ResourceExhausted)
            }
        }
    }
    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the caller returns this host's block with its original layout.
        unsafe { std::alloc::dealloc(block.as_ptr(), layout) };
        self.backing.destroyed(layout.size());
    }
}

pub(super) struct RootFactory<'a> {
    window: &'a ResultWindowAlias,
    control: &'a dyn PureCompileControl,
}
pub(super) struct RootScope<'a> {
    backing: Arc<Backing>,
    window: &'a ResultWindowAlias,
    control: &'a dyn PureCompileControl,
}
impl RootScope<'_> {
    fn reserve(&self, bytes: usize) -> Result<(), CompileControlError> {
        self.backing.reserve(bytes).inspect_err(|_| {
            tracing::debug!(
                requested_bytes = bytes,
                retained_bytes = self.window.retained_payload_bytes(),
                capacity_bytes = self.window.payload_capacity_bytes(),
                "Statement value layout request refused by the original window"
            );
        })
    }

    pub(super) fn with_creation<T>(
        &self,
        body: impl FnOnce(
            &mut novarocks_type_contract::ValueSchemaCreation<'_, CompileControlError>,
        ) -> T,
    ) -> T {
        let mut request = |layout: Layout| {
            self.control.checkpoint(CompilePhase::LowerProgram, 0)?;
            self.reserve(layout.size())
        };
        let mut opaque = |bytes: usize| {
            self.control.checkpoint(CompilePhase::LowerProgram, 0)?;
            self.reserve(bytes)
        };
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            self.backing.storage(),
            &mut request,
            &mut opaque,
        );
        body(&mut creation)
    }
}
impl ValueSchemaCreationScope<CompileControlError> for RootScope<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.backing.storage()
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), CompileControlError> {
        self.control.checkpoint(CompilePhase::LowerProgram, 0)?;
        self.reserve(layout.size())
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), CompileControlError> {
        self.control.checkpoint(CompilePhase::LowerProgram, 0)?;
        self.reserve(bytes)
    }
}
/// Reuse the original statement-window author for pure planning construction.
/// This creates no interpreter, allocator policy, window, or Work owner.
pub(crate) fn with_root_value_creation<T>(
    root: &WorkScope,
    window: &ResultWindowAlias,
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut novarocks_type_contract::ValueSchemaCreation<'_, CompileControlError>) -> T,
) -> Result<T, CompileControlError> {
    let factory = root_factory(root, window, control)?;
    let scope = factory.begin_scope()?;
    Ok(scope.with_creation(body))
}
fn root_factory<'a>(
    root: &WorkScope,
    window: &'a ResultWindowAlias,
    control: &'a dyn PureCompileControl,
) -> Result<RootFactory<'a>, CompileControlError> {
    if !window.is_for_scope(root) {
        return Err(source(WorkError::ForeignAuthority));
    }
    root.check().map_err(source)?;
    control.checkpoint(CompilePhase::Validate, 0)?;
    Ok(RootFactory { window, control })
}

impl ValueSchemaCreationFactory<CompileControlError> for RootFactory<'_> {
    type Scope<'a>
        = RootScope<'a>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, CompileControlError> {
        self.control.checkpoint(CompilePhase::LowerProgram, 0)?;
        Ok(RootScope {
            backing: Backing::new(self.window, 0)?,
            window: self.window,
            control: self.control,
        })
    }
    fn visit_scope_allocation_layouts<E>(
        &self,
        visit: &mut dyn FnMut(Layout) -> Result<(), E>,
    ) -> Result<(), E>
    where
        E: From<LayoutResourceError>,
    {
        visit(arc_layout(Layout::new::<Backing>())?)
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, ValueSchemaScopeBeginFailure<O, CompileControlError>> {
        observe(
            arc_layout(Layout::new::<Backing>())
                .map_err(|cause| ValueSchemaScopeBeginFailure::Creator(layout_error(cause)))?,
        )
        .map_err(ValueSchemaScopeBeginFailure::Observer)?;
        self.begin_scope()
            .map_err(ValueSchemaScopeBeginFailure::Creator)
    }
}

/// Exact positive operation envelopes remain paid until the same computation's
/// resulting CV or error actually drops. This conservative custody is not an
/// estimate of retained payload or a claim that uncovered kernels are funded.
pub(super) struct RootResources<'a> {
    pub(super) allocator: Arc<RootAllocator>,
    pub(super) factory: RootFactory<'a>,
    operations: Arc<Backing>,
    failure_guard: Option<Box<BackingGuard>>,
}
impl<'a> RootResources<'a> {
    pub(super) fn new(
        root: &WorkScope,
        window: &'a ResultWindowAlias,
        control: &'a dyn PureCompileControl,
    ) -> Result<Self, CompileControlError> {
        let factory = root_factory(root, window, control)?;
        let operations = Backing::new(window, Layout::new::<BackingGuard>().size())?;
        let failure_guard = Some(Box::new(BackingGuard {
            _backing: operations.clone(),
        }));
        Ok(Self {
            allocator: RootAllocator::new(window)?,
            factory,
            operations,
            failure_guard,
        })
    }
    pub(super) fn operation_storage(&self) -> ValueTypeStorageLifetime {
        self.operations.storage()
    }
    pub(super) fn retain_error(
        &mut self,
        error: SqlConstantEvaluationError,
    ) -> SqlConstantEvaluationError {
        // This guard was admitted and allocated before entering the body. An
        // error does not start another fallible guard-construction operation.
        let guard: Box<dyn SourceRetentionCapability> = self
            .failure_guard
            .take()
            .expect("one original required result");
        match error {
            SqlConstantEvaluationError::Source(ref source)
            | SqlConstantEvaluationError::Outer(ref source) => {
                // Forward custody to a typed Kernel owner where it can keep
                // the receipt. The shared source owns custody in all other cases.
                let kernel = match source.downcast_ref::<ExprEvaluationFailure<Infallible>>() {
                    Some(ExprEvaluationFailure::Kernel(kernel)) => Some(kernel),
                    _ => source.downcast_ref::<KernelFailure>(),
                };
                if let Some(kernel) = kernel {
                    if let Err(custody) = kernel.try_retain_source_lifetime(guard) {
                        source.retain_source_lifetime(custody);
                    }
                } else {
                    source.retain_source_lifetime(guard);
                }
            }
            SqlConstantEvaluationError::Control(CompileControlError::Source(ref source)) => {
                source.retain_source_lifetime(guard)
            }
            _ => {}
        }
        error
    }
    fn operation(&self, bytes: usize) -> Result<(), ScalarCallFailure<Infallible>> {
        self.operations
            .reserve(bytes)
            .map_err(|cause| KernelFailure::CompileControl(cause).into())
    }
}
impl ScalarCallHost for RootResources<'_> {
    type Refusal = Infallible;

    fn begin_call(&mut self) {}

    fn run_admitted(
        &mut self,
        _phase: ScalarCallPhase,
        facts: ScalarCallRequestFacts,
        body: &mut dyn FnMut() -> Result<(), ScalarCallFailure<Self::Refusal>>,
    ) -> Result<(), ScalarCallFailure<Self::Refusal>> {
        self.operation(facts.bytes())?;
        body()
    }

    fn callback_request_facts(
        &mut self,
        _allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    ) -> Result<ScalarCallRequestFacts, ScalarCallFailure<Self::Refusal>> {
        Err(ScalarCallFailure::Source(ScalarResourceError::SourceModel(
            "required constant host has no original whole-invocation callback request source",
        )))
    }
}
