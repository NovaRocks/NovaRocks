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

//! Exact lambda body occurrences and free references in their original flow.

use super::{ExprCompileError, ExprCompileFailure};
use crate::creation::ProgramCreation;
use novarocks_expression_contract::{ExprId, ExprKind, ExprNode, ValueId};
use novarocks_functions::{
    HigherOrderPreparationOptions, LambdaBodyContract, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlShape, ExpressionControlFlow,
    ExpressionInvocation, ExpressionUseId, FunctionValueType,
};
use std::{alloc::Layout, collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LambdaCaptureSource {
    Input(ValueId),
    Parameter { lambda: ExprId, ordinal: u32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledLambdaCapture {
    pub source: LambdaCaptureSource,
    /// Witness in the actual body tree. An input witness has an InputBinding
    /// in the enclosing statement root, even when nested in a shadowed scope.
    pub use_id: ExpressionUseId,
}

#[derive(Debug)]
pub struct CompiledLambdaBody {
    pub body_use: ExpressionUseId,
    pub lambda_definition: ExprId,
    pub contract: Arc<LambdaBodyContract>,
    pub captures: Box<[CompiledLambdaCapture]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LambdaReuseScope {
    TopLevel,
    Ancestor(ExprId),
}

#[derive(Debug, Default)]
pub(crate) struct CompiledLambdas {
    pub(crate) bodies: BTreeMap<ExpressionUseId, CompiledLambdaBody>,
    pub(crate) reuse: BTreeMap<ExpressionUseId, LambdaReuseScope>,
}

pub(crate) struct BodyEdge<'a> {
    pub(crate) lambda_definition: ExprId,
    pub(crate) wrapper_use: ExpressionUseId,
    pub(crate) root: &'a ExpressionInvocation<ExprId>,
}

pub(crate) fn body_edge<'a>(
    definition: impl Fn(ExprId) -> Option<&'a ExprNode>,
    flow: &'a ExpressionControlFlow<ExprId>,
    call: &ExpressionInvocation<ExprId>,
    ordinal: usize,
) -> Result<BodyEdge<'a>, &'static str> {
    let wrapper = call
        .arguments
        .get(ordinal)
        .and_then(|id| flow.uses().get(id))
        .ok_or("higher-order call has no actual lambda edge")?;
    let Some(ExprNode {
        id,
        kind: ExprKind::Lambda { body, .. },
        ..
    }) = definition(wrapper.definition)
    else {
        return Err("higher-order call does not own the named lambda wrapper");
    };
    let [body_use] = wrapper.arguments.as_ref() else {
        return Err("lambda wrapper does not have one actual body edge");
    };
    let root = flow
        .uses()
        .get(body_use)
        .ok_or("lambda body use is absent")?;
    if wrapper.control != ControlShape::LambdaBody
        || root.definition != *body
        || definition(*body).is_none_or(|body| body.lambda_scope != Some(*id))
        || root.context.region != wrapper.context.region
        || root.context.demand != wrapper.context.demand
    {
        return Err("lambda wrapper and body differ from the original guarded use");
    }
    Ok(BodyEdge {
        lambda_definition: *id,
        wrapper_use: wrapper.context.use_id,
        root,
    })
}

#[derive(Default)]
struct FreeReferences {
    // One actual witness for each caller-owned origin suffices. The key
    // names its identity, never an approximate carrier or a parameter name.
    sources: BTreeMap<LambdaCaptureSource, ExpressionUseId>,
    insertions: usize,
}
impl FreeReferences {
    fn insert<H>(
        &mut self,
        source: LambdaCaptureSource,
        use_id: ExpressionUseId,
        owner: &mut ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ExprCompileFailure<H>> {
        work.step()?;
        if !self.sources.contains_key(&source) {
            owner.before_tree_insert::<ExprCompileError, LambdaCaptureSource, ExpressionUseId>(
                self.insertions,
                work,
            )?;
            self.sources.insert(source, use_id);
            self.insertions += 1;
        }
        Ok(())
    }

    fn merge<H>(
        &mut self,
        mut child: Self,
        owner: &mut ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ExprCompileFailure<H>> {
        if self.sources.len() < child.sources.len() {
            std::mem::swap(self, &mut child);
        }
        for (source, use_id) in child.sources {
            self.insert(source, use_id, owner, work)?;
        }
        Ok(())
    }

    fn remove_binder(
        &mut self,
        lambda: ExprId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ExprCompileError> {
        let start = LambdaCaptureSource::Parameter { lambda, ordinal: 0 };
        let end = LambdaCaptureSource::Parameter {
            lambda,
            ordinal: u32::MAX,
        };
        while let Some(source) = self
            .sources
            .range(start..=end)
            .next()
            .map(|(source, _)| *source)
        {
            work.step()?;
            self.sources.remove(&source);
        }
        Ok(())
    }

    fn uses_binder(&self, lambda: ExprId) -> bool {
        let start = LambdaCaptureSource::Parameter { lambda, ordinal: 0 };
        let end = LambdaCaptureSource::Parameter {
            lambda,
            ordinal: u32::MAX,
        };
        self.sources.range(start..=end).next().is_some()
    }
}

/// Postorder folding consumes each reference set once per actual parent. The
/// checked flow prohibits a shared use, even when several uses share a source
/// definition. A nested wrapper subtracts only the binder it introduces.
#[derive(Default)]
pub(crate) struct LexicalReferences {
    pending: BTreeMap<ExpressionUseId, FreeReferences>,
    insertions: usize,
}

impl LexicalReferences {
    pub(crate) fn record<H>(
        &mut self,
        source: &ExprNode,
        invocation: &ExpressionInvocation<ExprId>,
        owner: &mut ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ExprCompileFailure<H>> {
        let wrapper = matches!(source.kind, ExprKind::Lambda { .. });
        let track_references = source.lambda_scope.is_some() || wrapper;
        let mut references = FreeReferences::default();
        for child in &invocation.arguments {
            if let Some(child) = self.pending.remove(child) {
                if track_references {
                    references.merge(child, owner, work)?;
                }
            }
            if track_references {
                work.step()?;
            }
        }
        if !track_references {
            return Ok(());
        }
        let use_id = invocation.context.use_id;
        match &source.kind {
            ExprKind::Lambda { .. } => references.remove_binder(source.id, work)?,
            ExprKind::Value(value) => {
                references.insert(LambdaCaptureSource::Input(*value), use_id, owner, work)?
            }
            ExprKind::LambdaParameter { lambda, ordinal } => references.insert(
                LambdaCaptureSource::Parameter {
                    lambda: *lambda,
                    ordinal: *ordinal,
                },
                use_id,
                owner,
                work,
            )?,
            _ => {}
        }
        if !references.sources.is_empty() {
            owner.before_tree_insert::<ExprCompileError, ExpressionUseId, FreeReferences>(
                self.insertions,
                work,
            )?;
            self.pending.insert(use_id, references);
            self.insertions += 1;
        }
        Ok(())
    }

    pub(crate) fn reuse_scope<'a>(
        &self,
        source: &ExprNode,
        use_id: ExpressionUseId,
        effects: ScopedExpressionEffects,
        definition: impl Fn(ExprId) -> Option<&'a ExprNode>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<LambdaReuseScope>, ExprCompileError> {
        let Some(current) = source.lambda_scope else {
            return Ok(None);
        };
        // Direct sources already borrow their backing. A body with an atomic
        // invocation domain, state or observable effects must keep its scope.
        if matches!(
            source.kind,
            ExprKind::Lambda { .. }
                | ExprKind::LambdaParameter { .. }
                | ExprKind::Value(_)
                | ExprKind::Constant(_)
        ) || !effects
            .for_use(effects.context())?
            .permits_boolean_reordering()
        {
            return Ok(None);
        }
        let references = self.pending.get(&use_id);
        let mut scope = Some(current);
        while let Some(lambda) = scope {
            work.step()?;
            if references.is_some_and(|refs| refs.uses_binder(lambda)) {
                return Ok((lambda != current).then_some(LambdaReuseScope::Ancestor(lambda)));
            }
            scope = definition(lambda)
                .ok_or(ExprCompileError::Invalid(
                    "actual lexical ancestor is absent",
                ))?
                .lambda_scope;
        }
        Ok(Some(LambdaReuseScope::TopLevel))
    }

    pub(crate) fn captures(
        &self,
        edge: &BodyEdge<'_>,
    ) -> (usize, impl Iterator<Item = CompiledLambdaCapture> + '_) {
        let captures = self.pending.get(&edge.wrapper_use);
        (
            captures.map_or(0, |refs| refs.sources.len()),
            captures
                .into_iter()
                .flat_map(|refs| refs.sources.iter())
                .map(|(&source, &use_id)| CompiledLambdaCapture { source, use_id }),
        )
    }

    pub(crate) fn preparation<'a, H>(
        &self,
        edge: &BodyEdge<'a>,
        flow: &ExpressionControlFlow<ExprId>,
        definition: impl Fn(ExprId) -> Option<&'a ExprNode>,
        effects: &BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
        arguments: ScopedExpressionEffects,
        owner: &mut ProgramCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<HigherOrderPreparationOptions, ExprCompileFailure<H>> {
        let body_context = edge.root.context;
        let body_effects = *effects
            .get(&body_context.use_id)
            .ok_or(ExprCompileError::Invalid(
                "actual lambda body effects are absent",
            ))?;
        let (capture_count, captures) = self.captures(edge);
        let mut types =
            owner.reserve::<ExprCompileError, FunctionValueType>(capture_count, work)?;
        for capture in captures {
            let source = flow
                .uses()
                .get(&capture.use_id)
                .and_then(|invocation| definition(invocation.definition))
                .ok_or(ExprCompileError::Invalid("actual lambda capture is absent"))?;
            types.push(source.ty.clone_observed(|| work.step())?);
        }
        if !types.is_empty() && types.len() != types.capacity() {
            owner.request::<ExprCompileError>(
                Layout::array::<FunctionValueType>(types.len())
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                work,
            )?;
        }
        Ok(HigherOrderPreparationOptions {
            arguments,
            body_context,
            body_effects,
            capture_types: types.into_boxed_slice(),
        })
    }
}
