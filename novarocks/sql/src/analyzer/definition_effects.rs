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

//! Effect facts belong to the original binding round, before optimizer rewrites.

use std::{cell::RefCell, collections::HashSet, rc::Rc, sync::Arc};

use novarocks_functions::{
    FunctionBindingError, FunctionSpecializationFailure, FunctionVolatility,
    ResolvedFunctionBinding,
};
use novarocks_parser::Span;
use novarocks_type_contract::{
    DecimalOverflowPolicy, FunctionId, FunctionOverloadId, FunctionValueType, PureCompileControl,
    SemanticParameterKey, StatementTimeEnvironment,
};

use crate::{
    analyze_error::{AnalyzeError, AnalyzeErrorKind},
    binding::{SqlFunctionBinding, SqlResultConstraintOrigin},
    compiler::SqlFunctionCatalog,
};

#[derive(Clone, Copy)]
enum Dependency {
    Stable,
    Volatile,
    StatementStart,
    TimeZone,
    CallerSession,
}

impl Dependency {
    fn from_environment_key(key: &SemanticParameterKey) -> Option<Self> {
        match key {
            SemanticParameterKey::StatementStartUtc => Some(Self::StatementStart),
            SemanticParameterKey::TimeZone => Some(Self::TimeZone),
            SemanticParameterKey::AllowThrowException => None,
        }
    }

    fn requires_environment(self) -> bool {
        matches!(
            self,
            Self::StatementStart | Self::TimeZone | Self::CallerSession
        )
    }

    fn rejection(self, name: &str, span: Span) -> AnalyzeError {
        let reason = match self {
            Self::Stable => format!("cannot use stable function '{name}'"),
            Self::Volatile => format!("cannot use volatile function '{name}'"),
            Self::StatementStart => format!("cannot depend on the statement instant ('{name}')"),
            Self::TimeZone => format!(
                "cannot depend on the session time zone ('{name}'). Specify an explicit time zone"
            ),
            Self::CallerSession => "cannot depend on the caller's session".into(),
        };
        AnalyzeError::at(
            AnalyzeErrorKind::MvDefinitionEffect,
            format!("Materialized view definition {reason}"),
            span,
        )
    }
}

#[derive(Clone)]
struct Occurrence {
    diagnostic: AnalyzeError,
    source: Option<Rc<ViewOrigin>>,
}

impl Occurrence {
    fn in_caller(&self, caller: Option<&ViewOrigin>) -> AnalyzeError {
        let mut error = self.diagnostic.clone();
        let mut source = self.source.as_deref();
        while let Some(view) = source {
            if caller.is_some_and(|caller| std::ptr::eq(caller, view)) {
                break;
            }
            error = error.at_view_reference(view.raw_sql.clone(), view.reference);
            source = view.outer.as_deref();
        }
        error
    }
}

#[derive(Clone, Default)]
struct Summary {
    first: [Option<Occurrence>; 5],
    first_rejection: Option<Dependency>,
    first_missing_environment: Option<Dependency>,
    deferred_environment: HashSet<(FunctionId, FunctionOverloadId)>,
    speculative: bool,
}

impl Summary {
    fn record(
        &mut self,
        dependency: Dependency,
        missing: bool,
        occurrence: impl FnOnce() -> Occurrence,
    ) {
        self.first[dependency as usize].get_or_insert_with(occurrence);
        self.first_rejection.get_or_insert(dependency);
        if missing {
            self.first_missing_environment.get_or_insert(dependency);
        }
    }

    fn diagnostic(
        &self,
        dependency: Option<Dependency>,
        caller: Option<&ViewOrigin>,
    ) -> Option<AnalyzeError> {
        dependency
            .and_then(|dependency| self.first[dependency as usize].as_ref())
            .map(|occurrence| occurrence.in_caller(caller))
    }
}

#[derive(Clone, Default)]
pub(super) struct DefinitionEffects {
    // Ordinary queries have no definition effects to collect. Analysis
    // descendants of a definition share its summary and scope transactions.
    summary: Option<Rc<RefCell<Summary>>>,
    source: Option<Rc<ViewOrigin>>,
}

struct ViewOrigin {
    outer: Option<Rc<ViewOrigin>>,
    raw_sql: Arc<str>,
    reference: Span,
}

impl DefinitionEffects {
    pub(super) fn materialized_view() -> Self {
        Self {
            summary: Some(Rc::new(RefCell::new(Summary::default()))),
            source: None,
        }
    }

    pub(super) fn in_view(&self, raw_sql: Arc<str>, reference: Span) -> Self {
        Self {
            summary: self.summary.clone(),
            source: self.summary.as_ref().map(|_| {
                Rc::new(ViewOrigin {
                    outer: self.source.clone(),
                    raw_sql,
                    reference,
                })
            }),
        }
    }

    fn record(&self, dependency: Dependency, name: &str, span: Span, environment_missing: bool) {
        if let Some(summary) = &self.summary {
            summary.borrow_mut().record(
                dependency,
                environment_missing && dependency.requires_environment(),
                || Occurrence {
                    diagnostic: dependency.rejection(name, span),
                    source: self.source.clone(),
                },
            );
        }
    }

    pub(super) fn admit_definition(&self) -> Result<(), AnalyzeError> {
        if let Some(summary) = &self.summary {
            let summary = summary.borrow();
            if let Some(error) = summary.diagnostic(summary.first_rejection, self.source.as_deref())
            {
                return Err(error);
            }
        }
        Ok(())
    }

    /// Scope selection retains an effect only when the corresponding syntax
    /// successfully binds. An unavailable MV environment is left deferred until
    /// the selected attempt becomes known.
    pub(super) fn try_scope<T>(
        &self,
        analyze: impl FnOnce() -> Result<T, AnalyzeError>,
    ) -> Result<T, AnalyzeError> {
        let Some(summary) = &self.summary else {
            return analyze();
        };
        let before = summary.borrow().clone();
        summary.borrow_mut().speculative = true;
        let result = analyze();
        let mut summary = summary.borrow_mut();
        if result.is_err() {
            *summary = before;
        } else {
            summary.speculative = before.speculative;
        }
        result
    }

    pub(super) fn admit_selected_scope(&self) -> Result<(), AnalyzeError> {
        if let Some(summary) = &self.summary {
            let summary = summary.borrow();
            if !summary.speculative
                && let Some(error) =
                    summary.diagnostic(summary.first_missing_environment, self.source.as_deref())
            {
                return Err(error);
            }
        }
        Ok(())
    }

    pub(super) fn has_deferred_environment(&self, binding: &ResolvedFunctionBinding) -> bool {
        self.summary.as_ref().is_some_and(|summary| {
            let summary = summary.borrow();
            summary.speculative
                && summary.deferred_environment.contains(&(
                    binding.function_id.clone(),
                    binding.selected.overload.clone(),
                ))
        })
    }

    fn defers_environment(&self) -> bool {
        self.summary
            .as_ref()
            .is_some_and(|summary| summary.borrow().speculative)
    }

    /// Returns true only for an unchosen analysis attempt that can proceed
    /// using the syntax's type without reading the caller session.
    pub(super) fn observe_caller_session(&self, span: Span) -> Result<bool, AnalyzeError> {
        if self.summary.is_none() {
            return Ok(false);
        }
        self.record(Dependency::CallerSession, "", span, true);
        if !self.defers_environment() {
            return Err(Dependency::CallerSession.rejection("", span));
        }
        Ok(true)
    }
}

pub(super) struct BindingFinalizer<'a> {
    pub(super) catalog: &'a dyn SqlFunctionCatalog,
    effects: DefinitionEffects,
    statement_time: Option<StatementTimeEnvironment>,
    pub(super) decimal_overflow_policy: DecimalOverflowPolicy,
    pub(super) control: &'a dyn PureCompileControl,
}

impl<'a> BindingFinalizer<'a> {
    pub(super) fn new(
        catalog: &'a dyn SqlFunctionCatalog,
        effects: &DefinitionEffects,
        statement_time: Option<StatementTimeEnvironment>,
        decimal_overflow_policy: DecimalOverflowPolicy,
        control: &'a dyn PureCompileControl,
    ) -> Self {
        Self {
            catalog,
            effects: effects.clone(),
            statement_time,
            decimal_overflow_policy,
            control,
        }
    }

    pub(super) fn ordinary(
        catalog: &'a dyn SqlFunctionCatalog,
        decimal_overflow_policy: DecimalOverflowPolicy,
        control: &'a dyn PureCompileControl,
    ) -> Self {
        Self::new(
            catalog,
            &DefinitionEffects::default(),
            None,
            decimal_overflow_policy,
            control,
        )
    }

    pub(super) fn finish(
        &self,
        resolved: ResolvedFunctionBinding,
        name: &str,
        span: Span,
    ) -> Result<SqlFunctionBinding, AnalyzeError> {
        self.finish_with(
            resolved,
            name,
            span,
            SqlResultConstraintOrigin::Unconstrained,
            None,
        )
    }

    pub(super) fn finish_with(
        &self,
        resolved: ResolvedFunctionBinding,
        name: &str,
        span: Span,
        origin: SqlResultConstraintOrigin,
        target: Option<FunctionValueType>,
    ) -> Result<SqlFunctionBinding, AnalyzeError> {
        let declaration = self
            .effects
            .summary
            .is_some()
            .then(|| {
                self.catalog.pure_overload_declaration_observed(
                    &resolved.function_id,
                    resolved.kind,
                    &resolved.selected.overload,
                    self.control,
                )
            })
            .transpose()
            .map_err(|error| match error {
                FunctionSpecializationFailure::Control(cause) => AnalyzeError::control(cause),
                FunctionSpecializationFailure::Binding(cause) => {
                    AnalyzeError::function_binding(cause)
                }
                FunctionSpecializationFailure::MissingPureImplementation(overload) => {
                    AnalyzeError::function_binding(FunctionBindingError::UnavailableImplementation(
                        overload,
                    ))
                }
                other => AnalyzeError::internal(other.to_string()),
            })?;
        let dependencies = if let Some(declaration) = &declaration {
            let effects = declaration.effects();
            let missing = self.statement_time.is_none();
            for dependency in effects
                .environment_dependencies
                .iter()
                .filter_map(Dependency::from_environment_key)
            {
                self.effects.record(dependency, name, span, missing);
            }
            let stability = match effects.value_stability {
                FunctionVolatility::Immutable => None,
                FunctionVolatility::Stable => Some(Dependency::Stable),
                FunctionVolatility::Volatile => Some(Dependency::Volatile),
            };
            if let Some(stability) = stability {
                self.effects.record(stability, name, span, false);
            }
            &effects.environment_dependencies
        } else {
            self.catalog.scalar_time_dependencies(&resolved)
        };
        let mut defer_environment = false;
        if self.statement_time.is_none()
            && let Some(summary) = &self.effects.summary
            && let Some(dependency) = dependencies
                .iter()
                .find_map(Dependency::from_environment_key)
        {
            let mut summary = summary.borrow_mut();
            if !summary.speculative {
                // The view boundary attaches its saved source and reference
                // span to this local diagnostic.
                return Err(dependency.rejection(name, span));
            }
            summary.deferred_environment.insert((
                resolved.function_id.clone(),
                resolved.selected.overload.clone(),
            ));
            defer_environment = true;
        }
        let binding = SqlFunctionBinding::from_analyzed(FinalizedAnalysisCall {
            resolved,
            decimal_overflow_policy: self.decimal_overflow_policy,
            origin,
            target,
        });
        if defer_environment {
            Ok(binding)
        } else {
            binding
                .with_statement_time(self.statement_time, dependencies)
                .map_err(|error| AnalyzeError::function_binding(error).at_type_mismatch(span))
        }
    }
}

/// Only the analysis finalizer can mint this receipt. The IR constructor
/// consumes it without retaining its diagnostic state or lexical source.
pub(crate) struct FinalizedAnalysisCall {
    resolved: ResolvedFunctionBinding,
    decimal_overflow_policy: DecimalOverflowPolicy,
    origin: SqlResultConstraintOrigin,
    target: Option<FunctionValueType>,
}

impl FinalizedAnalysisCall {
    pub(crate) fn into_parts(
        self,
    ) -> (
        ResolvedFunctionBinding,
        DecimalOverflowPolicy,
        SqlResultConstraintOrigin,
        Option<FunctionValueType>,
    ) {
        (
            self.resolved,
            self.decimal_overflow_policy,
            self.origin,
            self.target,
        )
    }
}
