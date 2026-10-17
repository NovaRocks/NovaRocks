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

//! The actual prepared lifecycle chooses an instance; the original Frame
//! remains the sole child/gather/NULL/row-journal author. No name or syntax
//! classifies an invocation failure, and no alternate allocation host exists.
use novarocks_functions::{
    AggregateStateAllocator, EvaluatedArgument, InvocationScalarEvaluationInstance,
    KernelEvaluationControl, KernelFailure, PreparedInvocationScalarKernel, PreparedScalarKernel,
    ScalarCallContract, ScalarEvaluationInstance, ScalarInvocationActivation,
    ScalarInvocationFailure, SelectedValues, Selection,
};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) enum PreparedCall<'a> {
    Kernel(&'a Arc<dyn PreparedScalarKernel>),
    Invocation(&'a Arc<dyn PreparedInvocationScalarKernel>),
}
impl PreparedCall<'_> {
    pub(super) fn contract(&self) -> &Arc<ScalarCallContract> {
        match self {
            Self::Kernel(prepared) => prepared.contract(),
            Self::Invocation(prepared) => prepared.contract(),
        }
    }
    pub(super) fn input_domain_policy(self) -> novarocks_functions::InvocationInputDomainPolicy {
        match self {
            Self::Kernel(_) => novarocks_functions::InvocationInputDomainPolicy::SelectedDemand,
            Self::Invocation(prepared) => prepared.input_domain_policy(),
        }
    }
    pub(super) fn is_invocation(self) -> bool {
        matches!(self, Self::Invocation(_))
    }
    pub(super) fn instantiate(
        self,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
    ) -> Result<CallInstance, KernelFailure> {
        match self {
            Self::Kernel(prepared) => ScalarEvaluationInstance::instantiate_with_allocator(
                Arc::clone(prepared),
                allocator,
            )
            .map(CallInstance::Kernel),
            Self::Invocation(prepared) => {
                InvocationScalarEvaluationInstance::instantiate_with_allocator(
                    Arc::clone(prepared),
                    allocator,
                )
                .map(CallInstance::Invocation)
            }
        }
    }
}

pub(super) enum CallInstance {
    Kernel(ScalarEvaluationInstance),
    Invocation(InvocationScalarEvaluationInstance),
}
impl CallInstance {
    pub(super) fn contract(&self) -> &ScalarCallContract {
        match self {
            Self::Kernel(instance) => instance.contract(),
            Self::Invocation(instance) => instance.contract(),
        }
    }
    pub(super) fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, ScalarInvocationFailure> {
        match self {
            Self::Kernel(instance) => instance
                .evaluate(selection, arguments, control)
                .map_err(ScalarInvocationFailure::from),
            Self::Invocation(instance) => {
                instance.evaluate(selection, arguments, activation, control)
            }
        }
    }
}

/// The generic Frame shares every scheduling/validation operation. Its error
/// transport is chosen at an explicit public entry, not by names or payloads.
/// The old adapter cannot invoke a whole-Data leaf at all.
pub(super) trait FrameFailure: From<KernelFailure> {
    const ACTIVATE_EMPTY: bool;
    fn invoke<'a>(
        instance: &mut CallInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self>;
    fn invoke_scoped<'a>(
        instance: &mut CallInstance,
        scope: novarocks_functions::ScalarInvocationScope<'a>,
        sources: &'a [novarocks_functions::ScalarInvocationSource<'a>],
        activation: ScalarInvocationActivation,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self>;
    fn required(
        root: novarocks_local_program::ProgramExpressionRootSite,
        selection: Selection<'_>,
        error: novarocks_functions::RowDataError,
    ) -> Self;
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self>;
}
impl FrameFailure for KernelFailure {
    const ACTIVATE_EMPTY: bool = false;
    fn invoke<'a>(
        instance: &mut CallInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        _activation: ScalarInvocationActivation,
        _allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        match instance {
            CallInstance::Kernel(instance) => instance.evaluate(selection, arguments, work.control),
            CallInstance::Invocation(_) => Err(super::invalid(
                "whole-invocation scalar requires the lossless evaluation entry",
            )),
        }
    }
    fn invoke_scoped<'a>(
        _instance: &mut CallInstance,
        _scope: novarocks_functions::ScalarInvocationScope<'a>,
        _sources: &'a [novarocks_functions::ScalarInvocationSource<'a>],
        _activation: ScalarInvocationActivation,
        _allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        _work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        unreachable!("Kernel-only root entry rejects every invocation lifecycle before Frame")
    }
    fn required(
        _root: novarocks_local_program::ProgramExpressionRootSite,
        _selection: Selection<'_>,
        _error: novarocks_functions::RowDataError,
    ) -> Self {
        unreachable!("Kernel-only root entry rejects every invocation lifecycle before Frame")
    }
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self> {
        work.finish(result)
    }
}
impl FrameFailure for crate::runtime::fragment::ExecutionFailure {
    const ACTIVATE_EMPTY: bool = true;
    fn invoke<'a>(
        instance: &mut CallInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        if let Some(scope) = work.scalar_scope.as_mut() {
            match instance {
                CallInstance::Kernel(instance) => scope
                    .evaluate(instance, selection, arguments, work.control)
                    .map_err(Into::into),
                CallInstance::Invocation(instance) => scope
                    .evaluate_invocation(
                        instance,
                        selection,
                        arguments,
                        activation,
                        allocator,
                        work.control,
                    )
                    .map_err(Into::into),
            }
        } else {
            instance
                .evaluate(selection, arguments, activation, work.control)
                .map_err(Into::into)
        }
    }
    fn invoke_scoped<'a>(
        instance: &mut CallInstance,
        scope: novarocks_functions::ScalarInvocationScope<'a>,
        sources: &'a [novarocks_functions::ScalarInvocationSource<'a>],
        activation: ScalarInvocationActivation,
        allocator: Option<&Arc<dyn AggregateStateAllocator>>,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        let CallInstance::Invocation(instance) = instance else {
            return Err(super::internal(
                "scoped Frame call differs from its prepared invocation lifecycle",
            )
            .into());
        };
        if let Some(runtime) = work.scalar_scope.as_mut() {
            runtime
                .evaluate_scoped_invocation(
                    instance,
                    scope,
                    sources,
                    activation,
                    allocator,
                    work.control,
                )
                .map_err(Into::into)
        } else {
            instance
                .evaluate_scoped(scope, sources, activation, work.control)
                .map_err(Into::into)
        }
    }
    fn required(
        root: novarocks_local_program::ProgramExpressionRootSite,
        selection: Selection<'_>,
        error: novarocks_functions::RowDataError,
    ) -> Self {
        match crate::runtime::fragment::RequiredExpressionRowError::try_new(root, selection, error)
        {
            Ok(error) => error.into(),
            Err(cause) => cause.into(),
        }
    }
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self> {
        match result {
            Err(error) => {
                if let crate::runtime::fragment::ExecutionFailureCause::Kernel(cause) =
                    error.cause()
                {
                    // Keep the original Kernel footer policy; the existing
                    // Work author decides whether completion may run.
                    match work.finish::<()>(Err(cause.clone())) {
                        Err(final_cause) if &final_cause != cause => Err(final_cause.into()),
                        _ => Err(error),
                    }
                } else {
                    Err(error)
                }
            }
            Ok(value) => work.finish(Ok(value)).map_err(Into::into),
        }
    }
}
