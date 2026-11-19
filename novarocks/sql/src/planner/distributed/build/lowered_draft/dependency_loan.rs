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

//! Read-only loans from the sole completed SQL source journal.
//! Validation stays with its original site author and caller checkpoints.
use super::*;
use crate::binding::SqlFunctionBinding;
use novarocks_functions::{
    ConstantPolicy, FunctionArgument, FunctionBindingRequest, FunctionBindingSelection,
};

/// A consumer asks about an actual immutable source; IDs alone do not authorize it.
pub enum SqlCallDependencySite<'a> {
    Expression {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        source: &'a novarocks_expression_contract::ExprNode,
    },
    Window {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a novarocks_physical_plan::WindowCallSpec,
    },
    Table {
        fragment: &'a Fragment,
        source: &'a PhysicalNode,
    },
    Aggregate {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a AggregateCall,
    },
    WriterAggregate {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a WriterAggregateCall,
    },
}

enum AuthenticatedSource<'a> {
    Expression {
        entry: CheckedExpressionLogicalSourceEntry<'a>,
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
    },
    Window(CheckedWindowLogicalSourceEntry<'a>),
    Table(CheckedTableLogicalSourceEntry<'a>),
    Aggregate(CheckedAggregateLogicalSourceEntry<'a>),
    WriterAggregate(CheckedWriterAggregateLogicalSourceEntry<'a>),
}

/// Construction requires the original journal's authentication, not type equality.
pub struct SqlCallDependencyLoan<'a> {
    source: AuthenticatedSource<'a>,
}

/// Physical channels and lifecycle remain distinct from the original logical request.
pub enum SqlCallDependencyProvenance<'a> {
    Expression {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        source: &'a novarocks_expression_contract::ExprNode,
        kind: SqlExpressionCallKind,
        channels: &'a [ExprId],
    },
    Window {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a novarocks_physical_plan::WindowCallSpec,
    },
    Table {
        fragment: &'a Fragment,
        source: &'a PhysicalNode,
        channels: &'a [ExprId],
    },
    Aggregate {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a AggregateCall,
        phase: AggregatePhase,
        runtime: AggregateRuntimeDemand,
    },
    WriterAggregate {
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a WriterAggregateCall,
        phase: AggregatePhase,
        runtime: AggregateRuntimeDemand,
    },
}

#[derive(Clone, Copy)]
enum CanonicalSource<'a> {
    Call(&'a CanonicalCallOperationalRequest),
    Aggregate(&'a CanonicalAggregateOperationalRequest),
}
/// Same-emission metadata from the original canonical author, if one exists.
/// It is not a prepared kernel, effect, phase, or coverage certificate.
pub struct SqlCanonicalDependencyLoan<'a> {
    source: CanonicalSource<'a>,
}
impl<'a> SqlCanonicalDependencyLoan<'a> {
    pub fn binding(&self) -> &'a SqlFunctionBinding {
        match self.source {
            CanonicalSource::Call(source) => &source.binding,
            CanonicalSource::Aggregate(source) => &source.binding,
        }
    }
    pub fn request(&self) -> FunctionBindingRequest<'a> {
        match self.source {
            CanonicalSource::Call(source) => source.request(),
            CanonicalSource::Aggregate(source) => source.request(),
        }
    }
    pub fn selected(&self) -> &'a FunctionBindingSelection {
        match self.source {
            CanonicalSource::Call(source) => source.selected().as_ref(),
            CanonicalSource::Aggregate(source) => source.selected().as_ref(),
        }
    }
}
impl<'a> SqlCallDependencyLoan<'a> {
    pub fn original_binding(&self) -> &'a SqlFunctionBinding {
        match &self.source {
            AuthenticatedSource::Expression { entry: source, .. } => source.captured().binding(),
            AuthenticatedSource::Window(source) => source.captured().binding(),
            AuthenticatedSource::Table(source) => source.captured().binding(),
            AuthenticatedSource::Aggregate(source) => source.captured().binding(),
            AuthenticatedSource::WriterAggregate(source) => source.captured().binding(),
        }
    }
    /// Borrow only actual admitted argument data. The capture's selected-result
    /// constraint is intentionally not exposed as an original request constraint.
    pub fn original_arguments(&self) -> &'a [FunctionArgument] {
        match &self.source {
            AuthenticatedSource::Expression { entry: source, .. } => {
                source.captured().request().arguments
            }
            AuthenticatedSource::Window(source) => source.captured().request().arguments,
            AuthenticatedSource::Table(source) => source.captured().request().arguments,
            AuthenticatedSource::Aggregate(source) => source.captured().request().arguments,
            AuthenticatedSource::WriterAggregate(source) => source.captured().request().arguments,
        }
    }
    pub fn original_logical_argument_count(&self) -> usize {
        match &self.source {
            AuthenticatedSource::Expression { entry: source, .. } => {
                source.captured().request().logical_argument_count
            }
            AuthenticatedSource::Window(source) => {
                source.captured().request().logical_argument_count
            }
            AuthenticatedSource::Table(source) => {
                source.captured().request().logical_argument_count
            }
            AuthenticatedSource::Aggregate(source) => {
                source.captured().request().logical_argument_count
            }
            AuthenticatedSource::WriterAggregate(source) => {
                source.captured().request().logical_argument_count
            }
        }
    }
    pub fn original_constant_policy(&self) -> ConstantPolicy {
        match &self.source {
            AuthenticatedSource::Expression { entry: source, .. } => {
                source.captured().constant_policy()
            }
            AuthenticatedSource::Window(source) => source.captured().constant_policy(),
            AuthenticatedSource::Table(source) => source.captured().constant_policy(),
            AuthenticatedSource::Aggregate(source) => source.captured().constant_policy(),
            AuthenticatedSource::WriterAggregate(source) => source.captured().constant_policy(),
        }
    }
    pub fn canonical(&self) -> Option<SqlCanonicalDependencyLoan<'a>> {
        let source = match &self.source {
            AuthenticatedSource::Expression { entry: source, .. } => {
                CanonicalSource::Call(source.canonical_operational()?.as_ref())
            }
            AuthenticatedSource::Window(source) => {
                CanonicalSource::Call(source.canonical_operational().as_ref())
            }
            AuthenticatedSource::Table(source) => {
                CanonicalSource::Call(source.canonical_operational().as_ref())
            }
            AuthenticatedSource::Aggregate(source) => {
                CanonicalSource::Aggregate(source.canonical()?.as_ref())
            }
            AuthenticatedSource::WriterAggregate(source) => {
                CanonicalSource::Aggregate(source.canonical()?.as_ref())
            }
        };
        Some(SqlCanonicalDependencyLoan { source })
    }
    pub fn provenance(&self) -> SqlCallDependencyProvenance<'a> {
        match &self.source {
            AuthenticatedSource::Expression {
                entry: source,
                fragment,
                node,
            } => SqlCallDependencyProvenance::Expression {
                fragment,
                node,
                source: source.source(),
                kind: source.kind(),
                channels: source.arguments(),
            },
            AuthenticatedSource::Window(source) => SqlCallDependencyProvenance::Window {
                fragment: source.fragment(),
                node: source.node(),
                site: source.site(),
                source: source.source(),
            },
            AuthenticatedSource::Table(source) => SqlCallDependencyProvenance::Table {
                fragment: source.fragment(),
                source: source.source(),
                channels: source.arguments(),
            },
            AuthenticatedSource::Aggregate(source) => SqlCallDependencyProvenance::Aggregate {
                fragment: source.fragment(),
                node: source.node(),
                site: source.site(),
                source: source.source(),
                phase: source.phase(),
                runtime: source.runtime(),
            },
            AuthenticatedSource::WriterAggregate(source) => {
                SqlCallDependencyProvenance::WriterAggregate {
                    fragment: source.fragment(),
                    node: source.node(),
                    site: source.site(),
                    source: source.source(),
                    phase: source.phase(),
                    runtime: source.runtime(),
                }
            }
        }
    }
}
impl SqlAuthoredPhysicalPlan {
    /// Delegate to one original authentication author without another observer,
    /// walker, source fallback, control scope, or success/failure footer.
    pub fn borrow_call_dependency_observed<'a>(
        &'a self,
        site: SqlCallDependencySite<'a>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<SqlCallDependencyLoan<'a>, SqlSourceJournalError> {
        let source = match site {
            SqlCallDependencySite::Expression {
                fragment,
                node,
                source,
            } => AuthenticatedSource::Expression {
                entry: self
                    .checked_expression_call_source_observed(fragment, node, source, work)?,
                fragment,
                node,
            },
            SqlCallDependencySite::Window {
                fragment,
                node,
                site,
                source,
            } => AuthenticatedSource::Window(
                self.checked_window_source_observed(fragment, node, site, source, work)?,
            ),
            SqlCallDependencySite::Table { fragment, source } => AuthenticatedSource::Table(
                self.checked_table_source_observed(fragment, source, work)?,
            ),
            SqlCallDependencySite::Aggregate {
                fragment,
                node,
                site,
                source,
            } => AuthenticatedSource::Aggregate(
                self.checked_aggregate_source_observed(fragment, node, site, source, work)?,
            ),
            SqlCallDependencySite::WriterAggregate {
                fragment,
                node,
                site,
                source,
            } => AuthenticatedSource::WriterAggregate(
                self.checked_writer_aggregate_source_observed(fragment, node, site, source, work)?,
            ),
        };
        Ok(SqlCallDependencyLoan { source })
    }
}
