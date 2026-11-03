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

//! The original flow constructor's numerical author and paired actual ports.
use super::*;
use crate::{
    ValueSchemaCreationScope,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use std::alloc::Layout;
#[derive(Debug)]
pub enum ExpressionControlCreationFailure<H> {
    Control(CompileControlError),
    Creator(H),
    Resource(LayoutResourceError),
    Owner(ExpressionControlFlowError),
}
impl<H> From<ExpressionControlFlowError> for ExpressionControlCreationFailure<H> {
    fn from(e: ExpressionControlFlowError) -> Self {
        match e {
            ExpressionControlFlowError::Control(c) => Self::Control(c),
            e => Self::Owner(e),
        }
    }
}
impl<H> From<ControlResourceError> for ExpressionControlCreationFailure<H> {
    fn from(e: ControlResourceError) -> Self {
        match e {
            ControlResourceError::Control(c) => Self::Control(c),
            ControlResourceError::SourceModel(_) => {
                Self::Resource(LayoutResourceError::SourceModel)
            }
        }
    }
}
impl<H: fmt::Display> fmt::Display for ExpressionControlCreationFailure<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Resource(e) => write!(f, "control construction resource: {e:?}"),
            Self::Owner(e) => e.fmt(f),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for ExpressionControlCreationFailure<H> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Creator(e) => Some(e),
            Self::Resource(_) => None,
            Self::Owner(e) => Some(e),
        }
    }
}
#[derive(Clone, Copy)]
enum CreationRetentionKind {
    Temporary,
    Retained,
}
enum Geometry {
    Exact(Layout),
    Trim(Layout),
    Opaque(ControlOwnedResourceFacts),
}
fn layout<T>(n: usize) -> Result<Layout, ControlResourceError> {
    Layout::array::<T>(n)
        .map_err(|_| ControlResourceError::Control(CompileControlError::ResourceExhausted))
}
fn arc<T>() -> Result<Layout, ControlResourceError> {
    arc_layout(Layout::new::<T>()).map_err(|e| match e {
        LayoutResourceError::SourceModel => {
            ControlResourceError::SourceModel("Control Arc source model drift")
        }
        _ => ControlResourceError::Control(CompileControlError::ResourceExhausted),
    })
}
fn tree<K, V>(n: usize) -> Result<ControlOwnedResourceFacts, ControlResourceError> {
    let mut r = ControlResourceCounter::default();
    r.tree::<K, V>(n)?;
    Ok(r.facts())
}
/// Sole constructor container list; no invocation or type parser is duplicated.
fn visit<D, E: From<ControlResourceError>>(
    n: usize,
    u: usize,
    mut emit: impl FnMut(CreationRetentionKind, Geometry) -> Result<(), E>,
) -> Result<(), E> {
    emit(
        CreationRetentionKind::Retained,
        Geometry::Opaque(tree::<EvaluationDomainId, ExpressionEvaluationDomain>(n)?),
    )?;
    emit(
        CreationRetentionKind::Retained,
        Geometry::Opaque(tree::<ExpressionUseId, ExpressionInvocation<D>>(u)?),
    )?;
    emit(
        CreationRetentionKind::Temporary,
        Geometry::Opaque(tree::<EvaluationDomainId, Vec<EvaluationDomainId>>(n)?),
    )?;
    emit(
        CreationRetentionKind::Temporary,
        Geometry::Opaque(tree::<EvaluationDomainId, usize>(n)?),
    )?;
    emit(
        CreationRetentionKind::Temporary,
        Geometry::Opaque(tree::<ExpressionUseId, usize>(u)?),
    )?;
    emit(
        CreationRetentionKind::Temporary,
        Geometry::Opaque(tree::<EvaluationDomainId, ()>(n)?),
    )?;
    emit(
        CreationRetentionKind::Temporary,
        Geometry::Opaque(tree::<ExpressionUseId, usize>(u)?),
    )?;
    // Same locked original child-Vec growth allowance; not a guessed public
    // Layout or exact retained capacity for a private library operation.
    let mut children = ControlResourceCounter::default();
    children.buffer::<EvaluationDomainId>(control_resource_mul(n, 16)?, 1)?;
    if n > 0 {
        children.merge(ControlOwnedResourceFacts {
            allocation_requests_upper_bound: n - 1,
            ..Default::default()
        })?;
    }
    emit(
        CreationRetentionKind::Temporary,
        Geometry::Opaque(children.facts()),
    )?;
    if n > 0 {
        emit(
            CreationRetentionKind::Temporary,
            Geometry::Exact(layout::<EvaluationDomainId>(n)?),
        )?;
    }
    if u > 0 {
        emit(
            CreationRetentionKind::Temporary,
            Geometry::Exact(layout::<ExpressionUseId>(u)?),
        )?;
        emit(
            CreationRetentionKind::Retained,
            Geometry::Exact(layout::<ExpressionUseId>(u)?),
        )?;
        // Pure model keeps its original maximum trim; actual request uses the
        // real final length immediately before Vec::into_boxed_slice.
        emit(
            CreationRetentionKind::Retained,
            Geometry::Trim(layout::<ExpressionUseId>(u)?),
        )?;
    }
    emit(
        CreationRetentionKind::Retained,
        Geometry::Exact(arc::<
            ControlFlowOwned<BTreeMap<EvaluationDomainId, ExpressionEvaluationDomain>>,
        >()?),
    )?;
    emit(
        CreationRetentionKind::Retained,
        Geometry::Exact(arc::<
            ControlFlowOwned<BTreeMap<ExpressionUseId, ExpressionInvocation<D>>>,
        >()?),
    )?;
    emit(
        CreationRetentionKind::Retained,
        Geometry::Exact(arc::<ControlFlowOwned<Box<[ExpressionUseId]>>>()?),
    )?;
    Ok(())
}
pub(super) fn resource_facts<D>(
    n: usize,
    u: usize,
) -> Result<ControlResourceCounter, ControlResourceError> {
    let mut r = ControlResourceCounter::default();
    visit::<D, ControlResourceError>(n, u, |_, g| match g {
        Geometry::Exact(l) | Geometry::Trim(l) => r.layout(l, 1),
        Geometry::Opaque(f) => r.merge(f),
    })?;
    Ok(r)
}
pub(super) fn observe<D, H>(
    n: usize,
    u: usize,
    temporary: &mut dyn ValueSchemaCreationScope<H>,
    retained: &mut dyn ValueSchemaCreationScope<H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ExpressionControlCreationFailure<H>> {
    // Same complete numeric header before any creator callback.
    resource_facts::<D>(n, u)?;
    visit::<D, ExpressionControlCreationFailure<H>>(n, u, |role, g| {
        if let Geometry::Trim(_) = g {
            return Ok(());
        }
        if matches!(&g, Geometry::Opaque(f) if f.allocation_request_bytes_upper_bound == 0) {
            return Ok(());
        }
        work.flush()
            .map_err(ExpressionControlCreationFailure::Control)?;
        fn request<H>(
            creation: &mut dyn ValueSchemaCreationScope<H>,
            geometry: Geometry,
        ) -> Result<(), H> {
            match geometry {
                Geometry::Exact(layout) => creation.request_layout(layout),
                Geometry::Opaque(facts) => {
                    creation.observe_opaque_upper_bound(facts.allocation_request_bytes_upper_bound)
                }
                Geometry::Trim(_) => unreachable!(),
            }
        }
        match role {
            CreationRetentionKind::Temporary => request(temporary, g),
            CreationRetentionKind::Retained => request(retained, g),
        }
        .map_err(ExpressionControlCreationFailure::Creator)
    })
}
