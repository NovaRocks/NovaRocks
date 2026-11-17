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

//! Exact search-predicate responsibilities and positive-conjunct sources.
//!
//! These facts identify the source of a necessary condition. They do not
//! authorize relocation, copying, provider pruning or removal of the source.
//! Those actions additionally require relational and accurate owner effects.
//! Facts are local to the owner's one frozen snapshot. Numeric IDs and a root
//! field check do not establish snapshot identity. Rebuild these facts while
//! validating a witness against that same snapshot; never cache or transfer
//! them as evidence for another plan with reused IDs.

use crate::{
    ExprId, ExprKind, ExpressionRootRole, ExpressionRootSite, Fragment, FragmentId,
    PhysicalRootUses,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    ExpressionEffectContext, ExpressionUseId, MAX_CONTROL_DEPTH, PureCompileControl,
    ValueLogicalType,
};
use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PredicateResponsibilityRef {
    pub fragment: FragmentId,
    pub site: ExpressionRootSite,
    pub use_id: ExpressionUseId,
}

/// One real relation-row search condition at its original exact site.
/// TruthOnly alone is insufficient: a branch condition may select an ELSE
/// result and a mutation condition may select a different event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateResponsibility {
    anchor: PredicateResponsibilityRef,
    definition: ExprId,
    context: ExpressionEffectContext,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PredicateSourceError {
    Control(CompileControlError),
    InvalidFragment,
    InvalidSite,
    NotSearchPredicate,
    InvalidUse,
    InvalidScope,
    WrongDemand,
    NotBoolean,
    TooDeep,
    NotPositiveConjunction,
    WrongControl,
    WrongArguments,
    InvalidArgument,
}
impl fmt::Display for PredicateSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid predicate responsibility source: {self:?}")
    }
}
impl std::error::Error for PredicateSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for PredicateSourceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
fn definition(
    fragment: &Fragment,
    id: ExprId,
    site: ExpressionRootSite,
) -> Result<&crate::ExprNode, PredicateSourceError> {
    let definition = fragment
        .expressions()
        .get(id)
        .ok_or(PredicateSourceError::InvalidUse)?;
    if definition.owner != site.node || definition.lambda_scope.is_some() {
        return Err(PredicateSourceError::InvalidScope);
    }
    if *definition.ty.data_type() != arrow_schema::DataType::Boolean
        || definition.ty.logical_type() != ValueLogicalType::Physical
    {
        return Err(PredicateSourceError::NotBoolean);
    }
    Ok(definition)
}
impl ExactPredicateResponsibility {
    pub fn try_new(
        fragment: &Fragment,
        roots: &PhysicalRootUses,
        site: ExpressionRootSite,
        control: &dyn PureCompileControl,
    ) -> Result<Self, PredicateSourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let value = Self::try_new_in(fragment, roots, site, &mut work)?;
        work.finish()?;
        Ok(value)
    }

    /// The same original responsibility author on the caller's scope.
    pub fn try_new_in(
        fragment: &Fragment,
        roots: &PhysicalRootUses,
        site: ExpressionRootSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PredicateSourceError> {
        if roots.roots().fragment() != fragment.id() {
            return Err(PredicateSourceError::InvalidFragment);
        }
        // Join matching predicates require a dedicated relation/multiplicity
        // proof. No implicit search privilege is inferred from their demand.
        if !matches!(
            site.role,
            ExpressionRootRole::FilterPredicate { .. } | ExpressionRootRole::ScanResidual { .. }
        ) {
            return Err(PredicateSourceError::NotSearchPredicate);
        }
        let id = *roots
            .bindings()
            .get(&site)
            .ok_or(PredicateSourceError::InvalidSite)?;
        let invocation = roots
            .flow()
            .uses()
            .get(&id)
            .ok_or(PredicateSourceError::InvalidUse)?;
        if invocation.context.demand != EvaluationDemand::TruthOnly {
            return Err(PredicateSourceError::WrongDemand);
        }
        let root = roots
            .roots()
            .sites()
            .get(&site)
            .ok_or(PredicateSourceError::InvalidSite)?;
        if invocation.definition != root.expr {
            return Err(PredicateSourceError::InvalidUse);
        }
        definition(fragment, invocation.definition, site)?;
        // Resolve the actual immutable node field again. This catches a changed
        // root field; the owner must still enforce the frozen snapshot scope.
        let actual = fragment
            .nodes()
            .get(&site.node)
            .and_then(|node| match (&node.kind, site.role) {
                (
                    crate::NodeKind::Filter { predicates },
                    ExpressionRootRole::FilterPredicate { predicate },
                ) => predicates.get(predicate as usize),
                (
                    crate::NodeKind::Scan { residuals, .. },
                    ExpressionRootRole::ScanResidual { predicate },
                ) => residuals.get(predicate as usize),
                _ => None,
            })
            .ok_or(PredicateSourceError::InvalidSite)?;
        if *actual != invocation.definition {
            return Err(PredicateSourceError::InvalidUse);
        }
        work.step()?;
        Ok(Self {
            anchor: PredicateResponsibilityRef {
                fragment: fragment.id(),
                site,
                use_id: id,
            },
            definition: invocation.definition,
            context: invocation.context,
        })
    }
    pub const fn anchor(&self) -> PredicateResponsibilityRef {
        self.anchor
    }
    pub const fn definition(&self) -> ExprId {
        self.definition
    }
    pub const fn context(&self) -> ExpressionEffectContext {
        self.context
    }
}

/// A checked positive-conjunction path from p to an existing source expression.
/// Empty path is identity. It crosses no NOT, IS NULL, CASE or strong domain.
/// q still needs exact value transport/equality, relational placement, pure
/// owner facts and keep-candidate row-error semantics before it can prune.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PredicateConjunctSource {
    responsibility: ExactPredicateResponsibility,
    path: Arc<[u32]>,
    definition: ExprId,
    context: ExpressionEffectContext,
}
impl PredicateConjunctSource {
    pub fn try_new(
        fragment: &Fragment,
        roots: &PhysicalRootUses,
        site: ExpressionRootSite,
        path: Vec<u32>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, PredicateSourceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let (responsibility, definition, context) =
            Self::checked_parts(fragment, roots, site, &path, false, &mut work)?;
        work.finish()?;
        Ok(Self {
            responsibility,
            path: path.into(),
            definition,
            context,
        })
    }

    /// Validate the same full path and actual root using the caller's scope.
    /// The final Arc remains an opaque host boundary, not an allocation grant.
    pub fn try_new_in(
        fragment: &Fragment,
        roots: &PhysicalRootUses,
        site: ExpressionRootSite,
        path: Vec<u32>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PredicateSourceError> {
        let (responsibility, definition, context) =
            Self::checked_parts(fragment, roots, site, &path, true, work)?;
        work.flush()?;
        let path = path.into();
        work.step()?;
        work.flush()?;
        Ok(Self {
            responsibility,
            path,
            definition,
            context,
        })
    }
    fn checked_parts(
        fragment: &Fragment,
        roots: &PhysicalRootUses,
        site: ExpressionRootSite,
        path: &[u32],
        borrowed: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<
        (
            ExactPredicateResponsibility,
            ExprId,
            ExpressionEffectContext,
        ),
        PredicateSourceError,
    > {
        if path.len() >= MAX_CONTROL_DEPTH {
            return Err(PredicateSourceError::TooDeep);
        }
        let responsibility = if borrowed {
            ExactPredicateResponsibility::try_new_in(fragment, roots, site, work)?
        } else {
            ExactPredicateResponsibility::try_new(fragment, roots, site, work.control())?
        };
        let mut current = roots
            .flow()
            .uses()
            .get(&responsibility.anchor.use_id)
            .ok_or(PredicateSourceError::InvalidUse)?;
        for ordinal in path {
            let source = definition(fragment, current.definition, site)?;
            let ExprKind::Conjunction { args } = &source.kind else {
                return Err(PredicateSourceError::NotPositiveConjunction);
            };
            if current.control != ControlShape::Conjunction {
                return Err(PredicateSourceError::WrongControl);
            }
            if args.len() != current.arguments.len() {
                return Err(PredicateSourceError::WrongArguments);
            }
            let ordinal = *ordinal as usize;
            let child_definition = args
                .get(ordinal)
                .ok_or(PredicateSourceError::InvalidArgument)?;
            let child_id = current
                .arguments
                .get(ordinal)
                .ok_or(PredicateSourceError::InvalidArgument)?;
            let child = roots
                .flow()
                .uses()
                .get(child_id)
                .ok_or(PredicateSourceError::InvalidUse)?;
            if *child_definition != child.definition {
                return Err(PredicateSourceError::WrongArguments);
            }
            if child.context.region != responsibility.context.region {
                return Err(PredicateSourceError::InvalidScope);
            }
            if child.context.demand != EvaluationDemand::TruthOnly {
                return Err(PredicateSourceError::WrongDemand);
            }
            current = child;
            work.step()?;
        }
        definition(fragment, current.definition, site)?;
        work.step()?;
        Ok((responsibility, current.definition, current.context))
    }
    pub const fn responsibility(&self) -> &ExactPredicateResponsibility {
        &self.responsibility
    }
    pub fn argument_ordinals(&self) -> &[u32] {
        &self.path
    }
    pub const fn definition(&self) -> ExprId {
        self.definition
    }
    pub const fn context(&self) -> ExpressionEffectContext {
        self.context
    }
}
