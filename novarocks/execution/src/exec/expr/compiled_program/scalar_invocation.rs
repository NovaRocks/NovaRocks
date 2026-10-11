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
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self>;
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self>;
}
impl FrameFailure for KernelFailure {
    const ACTIVATE_EMPTY: bool = false;
    fn invoke<'a>(
        instance: &mut CallInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        _activation: ScalarInvocationActivation,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        match instance {
            CallInstance::Kernel(instance) => instance.evaluate(selection, arguments, work.control),
            CallInstance::Invocation(_) => Err(super::invalid(
                "whole-invocation scalar requires the lossless evaluation entry",
            )),
        }
    }
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self> {
        work.finish(result)
    }
}
impl FrameFailure for ScalarInvocationFailure {
    const ACTIVATE_EMPTY: bool = true;
    fn invoke<'a>(
        instance: &mut CallInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        instance.evaluate(selection, arguments, activation, work.control)
    }
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self> {
        match result {
            Err(Self::Data(data)) => Err(Self::Data(data)),
            Err(Self::Kernel(cause)) => work.finish(Err(cause)).map_err(Self::Kernel),
            Ok(value) => work.finish(Ok(value)).map_err(Self::Kernel),
        }
    }
}

impl FrameFailure for crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure {
    const ACTIVATE_EMPTY: bool = true;
    fn invoke<'a>(
        instance: &mut CallInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        activation: ScalarInvocationActivation,
        work: &mut super::Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, Self> {
        match instance {
            CallInstance::Kernel(instance) => {
                let scope = work.scalar_scope.as_mut().ok_or_else(|| {
                    super::internal("runtime scalar Frame has no explicit operation scope")
                })?;
                scope.evaluate(instance, selection, arguments, work.control)
            }
            CallInstance::Invocation(instance) => instance
                .evaluate(selection, arguments, activation, work.control)
                .map_err(Into::into),
        }
    }
    fn finish<T>(work: &mut super::Work<'_, '_>, result: Result<T, Self>) -> Result<T, Self> {
        match result {
            Err(Self::Data(data)) => Err(Self::Data(data)),
            Err(Self::Source(error)) => Err(Self::Source(error)),
            Err(Self::Host(cause)) => Err(Self::Host(cause)),
            Err(Self::Kernel(cause)) => work.finish(Err(cause)).map_err(Self::Kernel),
            Ok(value) => work.finish(Ok(value)).map_err(Self::Kernel),
        }
    }
}
