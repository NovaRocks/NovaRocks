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

// Design: ADR-0239 (docs/adr/ADR-0239-expression-evaluation-and-host-admission.md)
use novarocks_functions::{
    AggregateStateAllocator, EvaluatedArgument, InvocationScalarEvaluationInstance,
    KernelEvaluationControl, KernelFailure, PreparedInvocationScalarKernel, PreparedScalarKernel,
    ScalarCallContract, ScalarEvaluationInstance, ScalarInvocationActivation, SelectedValues,
    Selection,
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
    #[cfg(test)]
    pub(super) fn contract(&self) -> &ScalarCallContract {
        match self {
            Self::Kernel(instance) => instance.contract(),
            Self::Invocation(instance) => instance.contract(),
        }
    }
}

use crate::{
    ExprEvaluationFailure, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts,
};
use novarocks_functions::{ScalarInvocationScope, ScalarInvocationSource, ScalarResourceError};

/// Carry the typed phase output outside the object's admission closure.
fn run_phase<R, T>(
    host: &mut dyn ScalarCallHost<Refusal = R>,
    phase: ScalarCallPhase,
    facts: ScalarCallRequestFacts,
    body: impl FnOnce() -> Result<T, ScalarCallFailure<R>>,
) -> Result<T, ScalarCallFailure<R>> {
    #[cfg(test)]
    observe(|| LeafCallObservation::Admission(phase, facts));

    let mut body = Some(body);
    let mut output = None;
    host.run_admitted(phase, facts, &mut || {
        let body = body
            .take()
            .ok_or_else(|| super::internal("scalar host invoked an admitted phase twice"))?;
        output = Some(body()?);
        Ok(())
    })?;
    output.ok_or_else(|| super::internal("scalar host did not run an admitted phase").into())
}

pub fn call_scalar<'a, R>(
    host: &mut dyn ScalarCallHost<Refusal = R>,
    instance: &mut ScalarEvaluationInstance,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, ScalarCallFailure<R>> {
    host.begin_call();
    let body = |instance: &mut ScalarEvaluationInstance| {
        #[cfg(test)]
        observe(|| LeafCallObservation::Rows {
            explicit_actual_domain: false,
            actual: selection.iter().collect(),
            demand: selection.iter().collect(),
        });
        instance
            .evaluate(selection, arguments, control)
            .map_err(Into::into)
    };
    let Some(profile) = instance.invocation_resource_profile() else {
        return body(instance);
    };
    let facts = profile
        .requests_for(selection, arguments)
        .map_err(ScalarCallFailure::Source)?;
    run_phase(
        host,
        ScalarCallPhase::Body,
        ScalarCallRequestFacts::new(facts.bytes(), facts.requests()),
        || body(instance),
    )
}

pub fn call_invocation<'a, R>(
    host: &mut dyn ScalarCallHost<Refusal = R>,
    instance: &mut InvocationScalarEvaluationInstance,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
    activation: ScalarInvocationActivation,
    allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, ScalarCallFailure<R>> {
    host.begin_call();
    let body = |instance: &mut InvocationScalarEvaluationInstance| {
        #[cfg(test)]
        observe(|| LeafCallObservation::Rows {
            explicit_actual_domain: false,
            actual: selection.iter().collect(),
            demand: selection.iter().collect(),
        });
        instance
            .evaluate(selection, arguments, activation, control)
            .map_err(Into::into)
    };
    if instance.is_failed() {
        return body(instance);
    }
    let Some(profile) = instance.invocation_resource_profile() else {
        return body(instance);
    };
    let prefix = profile.preparation_prefix();
    let requests = run_phase(
        host,
        ScalarCallPhase::Preparation,
        ScalarCallRequestFacts::new(prefix.bytes(), prefix.requests()),
        || {
            profile
                .requests_for(instance, selection, arguments, activation, control)
                .map_err(Into::into)
        },
    )?;
    let mut facts = ScalarCallRequestFacts::new(requests.bytes(), requests.requests());
    if requests.requires_host_callback() {
        facts = facts
            .checked_add(host.callback_request_facts(allocator)?)
            .ok_or(ScalarCallFailure::Source(ScalarResourceError::Arithmetic))?;
    }
    run_phase(host, ScalarCallPhase::Body, facts, || body(instance))
}

pub fn call_scoped_invocation<'a, R>(
    host: &mut dyn ScalarCallHost<Refusal = R>,
    instance: &mut InvocationScalarEvaluationInstance,
    scope: ScalarInvocationScope<'a>,
    sources: &'a [ScalarInvocationSource<'a>],
    activation: ScalarInvocationActivation,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, ScalarCallFailure<R>> {
    host.begin_call();
    if !instance.is_failed() && instance.invocation_resource_profile().is_some() {
        return Err(super::invalid(
            "scoped invocation has no installed measurement source transport",
        )
        .into());
    }
    #[cfg(test)]
    observe(|| LeafCallObservation::Rows {
        explicit_actual_domain: true,
        actual: scope.actual_domain().iter().collect(),
        demand: scope.demand().iter().collect(),
    });
    instance
        .evaluate_scoped(scope, sources, activation, control)
        .map_err(Into::into)
}

pub(super) fn invoke<'a, R>(
    instance: &mut CallInstance,
    selection: Selection<'a>,
    arguments: &'a [EvaluatedArgument<'a>],
    activation: ScalarInvocationActivation,
    allocator: Option<&Arc<dyn AggregateStateAllocator>>,
    work: &mut super::Work<'_, '_, R>,
) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
    // Every evaluating root lends its actual host.
    match (instance, work.scalar_host.as_deref_mut()) {
        (CallInstance::Kernel(instance), Some(host)) => {
            call_scalar(host, instance, selection, arguments, work.control).map_err(Into::into)
        }
        (CallInstance::Invocation(instance), Some(host)) => call_invocation(
            host,
            instance,
            selection,
            arguments,
            activation,
            allocator,
            work.control,
        )
        .map_err(Into::into),
        (CallInstance::Kernel(instance), None) => instance
            .evaluate(selection, arguments, work.control)
            .map_err(Into::into),
        (CallInstance::Invocation(_), None) => Err(super::invalid(
            "whole-invocation scalar requires the lossless evaluation entry",
        )
        .into()),
    }
}

pub(super) fn invoke_scoped<'a, R>(
    instance: &mut CallInstance,
    scope: ScalarInvocationScope<'a>,
    sources: &'a [ScalarInvocationSource<'a>],
    activation: ScalarInvocationActivation,
    work: &mut super::Work<'_, '_, R>,
) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
    let CallInstance::Invocation(instance) = instance else {
        return Err(super::internal(
            "scoped Frame call differs from its prepared invocation lifecycle",
        )
        .into());
    };
    let host = work
        .scalar_host
        .as_deref_mut()
        .expect("root evaluation lends its scalar host");
    call_scoped_invocation(host, instance, scope, sources, activation, work.control)
        .map_err(Into::into)
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LeafCallObservation {
    Rows {
        explicit_actual_domain: bool,
        actual: Vec<usize>,
        demand: Vec<usize>,
    },
    Admission(ScalarCallPhase, ScalarCallRequestFacts),
}

#[cfg(test)]
thread_local! {
    static OBSERVATIONS: std::cell::RefCell<Option<Vec<LeafCallObservation>>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
fn observe(observation: impl FnOnce() -> LeafCallObservation) {
    OBSERVATIONS.with_borrow_mut(|slot| {
        if let Some(observations) = slot {
            observations.push(observation());
        }
    });
}

#[cfg(test)]
pub(super) fn observe_calls<T>(run: impl FnOnce() -> T) -> (T, Vec<LeafCallObservation>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVATIONS.with_borrow_mut(|slot| *slot = None);
        }
    }
    OBSERVATIONS.with_borrow_mut(|slot| {
        assert!(
            slot.replace(Vec::new()).is_none(),
            "nested scalar call observation"
        );
    });
    let _reset = Reset;
    let result = run();
    let observations = OBSERVATIONS.with_borrow_mut(|slot| slot.take().unwrap());
    (result, observations)
}
