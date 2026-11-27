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

//! SQL-owned immutable binding handles.
//!
//! These values deliberately have no serialization implementation. Table
//! bindings are meaningful only in the application-owned store that allocated
//! their scope. Function bindings share one immutable selection across SQL IR
//! layers and are copied into the final physical-plan contract only at its
//! lowering boundary.

mod call_arguments;
pub(crate) use call_arguments::{
    CapturedLogicalCallArguments, LogicalCallArgumentCaptureError, capture_logical_call_arguments,
    move_authored_call_arguments_observed,
};

mod aggregate_request;
mod aggregate_source;
pub(crate) use aggregate_request::{
    AggregateRequestCaptureError, CapturedAggregateLogicalRequest,
    capture_aggregate_logical_request,
};
pub(crate) mod observed;
pub(crate) use aggregate_source::{AggregateArgumentSource, AggregateLogicalSourceIdentity};

use std::{
    num::{NonZeroU32, NonZeroU64},
    ops::Deref,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

/// Shared, immutable function selection carried through SQL-owned IR layers.
///
/// Function bindings include complete identity, type and semantic metadata.
/// Sharing keeps expression nodes compact while preserving one exact binding
/// from analysis through optimization and physical lowering.
#[derive(Clone)]
pub struct SqlFunctionBinding(
    Arc<SqlFunctionCallFacts>,
    Option<novarocks_functions::StaticScalarCallEffects>,
);

// Derived proof availability is independent of the bound expression's identity.
impl std::fmt::Debug for SqlFunctionBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("SqlFunctionBinding")
            .field(&self.0)
            .finish()
    }
}
impl PartialEq for SqlFunctionBinding {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for SqlFunctionBinding {}
impl std::hash::Hash for SqlFunctionBinding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.0, state);
    }
}

/// Positive evidence of the constructor that supplied an exact result target.
/// It is not inferred from a function name, selected result or source absence.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum SqlResultConstraintOrigin {
    Unconstrained,
    /// A common target authored from the original ARRAY element declarations.
    ArrayCommonCoercion,
    /// The original field names and positions of an explicitly constructed ROW.
    RowConstructor,
    TemporalSelected {
        own_nullable: bool,
    },
    /// An operand target selected by an installed numeric operator overload.
    OperatorDecimalFloatCoercion,
    ValueDomainConversion {
        /// The original full final assignment/CAST target, before the one
        /// conversion owner chose its explicit intermediate contract.
        final_target: novarocks_functions::FunctionValueType,
    },
}

/// One call's exact selection and authored semantic policy. The selected
/// overload remains catalog-owned; a SQL scope does not redefine its identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SqlFunctionCallFacts {
    resolved: novarocks_functions::ResolvedFunctionBinding,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    /// A real producer-supplied result target, separate from inferred selection.
    result_constraint: Option<novarocks_functions::FunctionValueType>,
    result_constraint_origin: SqlResultConstraintOrigin,

    statement_start: Option<novarocks_type_contract::StatementStartUtc>,
    time_zone: Option<novarocks_type_contract::TimeZoneKey>,
    aggregate_state_source: Option<Arc<novarocks_type_contract::AggregateStateInterpretation>>,
}

/// Capabilities minted by the stages that may author additional plan calls.
pub(crate) enum PlanBindingAuthority {
    Optimizer(crate::optimizer::BindingAuthority),
    Planner(crate::planner::BindingAuthority),
    Planning(crate::planning::BindingAuthority),
    #[cfg(test)]
    Test,
}

impl SqlFunctionBinding {
    pub(crate) fn from_analyzed(call: crate::analyzer::FinalizedAnalysisCall) -> Self {
        let (resolved, policy, origin, target, static_effects) = call.into_parts();
        let mut binding = Self::with_constraint(resolved, policy, origin, target);
        binding.1 = static_effects;
        binding
    }

    pub(crate) fn static_scalar_effects(
        &self,
    ) -> Option<novarocks_functions::StaticScalarCallEffects> {
        self.1
    }

    fn with_constraint(
        resolved: novarocks_functions::ResolvedFunctionBinding,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
        result_constraint_origin: SqlResultConstraintOrigin,
        result_constraint: Option<novarocks_functions::FunctionValueType>,
    ) -> Self {
        Self(
            Arc::new(SqlFunctionCallFacts {
                resolved,
                decimal_overflow_policy,
                result_constraint,
                result_constraint_origin,
                statement_start: None,
                time_zone: None,
                aggregate_state_source: None,
            }),
            None,
        )
    }

    pub(crate) fn from_plan(
        authority: PlanBindingAuthority,
        resolved: novarocks_functions::ResolvedFunctionBinding,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> Self {
        Self::from_plan_with_constraint(
            authority,
            resolved,
            policy,
            SqlResultConstraintOrigin::Unconstrained,
            None,
        )
    }

    pub(crate) fn from_plan_with_constraint(
        _authority: PlanBindingAuthority,
        resolved: novarocks_functions::ResolvedFunctionBinding,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
        origin: SqlResultConstraintOrigin,
        target: Option<novarocks_functions::FunctionValueType>,
    ) -> Self {
        Self::with_constraint(resolved, policy, origin, target)
    }

    pub(crate) fn resolve_for_plan(
        authority: PlanBindingAuthority,
        catalog: &dyn crate::compiler::SqlFunctionCatalog,
        name: &str,
        args: &[crate::analysis::TypedExpr],
        policy: novarocks_type_contract::DecimalOverflowPolicy,
        constant_policy: novarocks_functions::ConstantValidation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<Self, novarocks_functions::FunctionBindingError> {
        let resolved =
            crate::analysis::select_scalar_function(catalog, name, args, constant_policy, control)?;
        Ok(Self::from_plan(authority, resolved, policy))
    }

    #[cfg(test)]
    pub(crate) fn from_test(
        resolved: novarocks_functions::ResolvedFunctionBinding,
        policy: novarocks_type_contract::DecimalOverflowPolicy,
    ) -> Self {
        Self::from_plan(PlanBindingAuthority::Test, resolved, policy)
    }

    /// Retain the actual lexical aggregate producer's DISTINCT and ORDER facts.
    /// This receipt is independent of a consuming merge call's execution flags.
    pub(crate) fn with_aggregate_state_source(
        mut self,
        facts: novarocks_type_contract::AggregateStateInterpretation,
    ) -> Self {
        let old = &self.0;
        self.0 = Arc::new(SqlFunctionCallFacts {
            resolved: old.resolved.clone(),
            decimal_overflow_policy: old.decimal_overflow_policy,
            result_constraint: old.result_constraint.clone(),
            result_constraint_origin: old.result_constraint_origin.clone(),

            statement_start: old.statement_start,
            time_zone: old.time_zone,
            aggregate_state_source: Some(Arc::new(facts)),
        });
        self
    }
    pub(crate) fn aggregate_state_source(
        &self,
    ) -> Option<&novarocks_type_contract::AggregateStateInterpretation> {
        self.0.aggregate_state_source.as_deref()
    }

    /// Attach only the time facts declared by the actual selected overload.
    /// Other semantic keys remain with their original binding authors.
    pub(crate) fn with_statement_time(
        mut self,
        environment: Option<novarocks_type_contract::StatementTimeEnvironment>,
        dependencies: &[novarocks_type_contract::SemanticParameterKey],
    ) -> Result<Self, novarocks_functions::FunctionBindingError> {
        use novarocks_type_contract::SemanticParameterKey as K;
        let start = dependencies.contains(&K::StatementStartUtc);
        let zone = dependencies.contains(&K::TimeZone);
        if start || zone {
            let environment = environment.ok_or_else(|| {
                novarocks_functions::FunctionBindingError::UnavailableImplementation(
                    self.selected.overload.clone(),
                )
            })?;
            let facts = Arc::make_mut(&mut self.0);
            facts.statement_start = start.then_some(environment.start);
            facts.time_zone = zone.then_some(environment.zone);
        }
        Ok(self)
    }

    /// IDs 0–3 belong to AllowThrowException and GROUP_CONCAT. A statement
    /// body has one original instant and one effective time zone; required
    /// property constants use their own environment and parameter table.
    pub(crate) fn statement_time_parameters(
        &self,
    ) -> impl Iterator<
        Item = (
            novarocks_type_contract::SemanticParameterRef,
            novarocks_type_contract::SemanticParameterValue,
        ),
    > {
        use novarocks_type_contract::{
            SemanticParameterId as I, SemanticParameterRef as R, SemanticParameterValue as V,
        };
        [
            self.0
                .statement_start
                .map(|start| (4, V::StatementStartUtc(start))),
            self.0.time_zone.map(|zone| (5, V::TimeZone(zone))),
        ]
        .into_iter()
        .flatten()
        .map(|(id, value)| {
            (
                R {
                    id: I::new(id),
                    expected_key: value.key(),
                },
                value,
            )
        })
    }

    pub fn result_constraint(&self) -> Option<&novarocks_functions::FunctionValueType> {
        self.0.result_constraint.as_ref()
    }

    pub fn result_constraint_origin(&self) -> &SqlResultConstraintOrigin {
        &self.0.result_constraint_origin
    }

    pub fn resolved(&self) -> &novarocks_functions::ResolvedFunctionBinding {
        &self.0.resolved
    }

    pub fn decimal_overflow_policy(&self) -> novarocks_type_contract::DecimalOverflowPolicy {
        self.0.decimal_overflow_policy
    }
}

impl AsRef<novarocks_functions::ResolvedFunctionBinding> for SqlFunctionBinding {
    fn as_ref(&self) -> &novarocks_functions::ResolvedFunctionBinding {
        self.resolved()
    }
}

impl Deref for SqlFunctionBinding {
    type Target = novarocks_functions::ResolvedFunctionBinding;

    fn deref(&self) -> &Self::Target {
        self.resolved()
    }
}

/// Process-local identity of one application binding store.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SqlTableBindingScopeId(NonZeroU64);

impl SqlTableBindingScopeId {
    pub(crate) fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub fn get(self) -> NonZeroU64 {
        self.0
    }
}

/// One table fact allocated by a query-local binding store.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SqlTableBindingId {
    scope: SqlTableBindingScopeId,
    ordinal: NonZeroU32,
}

impl SqlTableBindingId {
    pub(crate) fn new(scope: SqlTableBindingScopeId, ordinal: NonZeroU32) -> Self {
        Self { scope, ordinal }
    }

    pub fn scope(self) -> SqlTableBindingScopeId {
        self.scope
    }

    pub fn ordinal(self) -> NonZeroU32 {
        self.ordinal
    }

    pub fn belongs_to(self, scope: SqlTableBindingScopeId) -> bool {
        self.scope == scope
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(ordinal: u32) -> Self {
        let ordinal = NonZeroU32::new(ordinal).expect("test binding ordinal is nonzero");
        let mut allocator = SqlTableBindingAllocator::try_new_for_test(
            NonZeroU64::new(1).expect("test binding scope is nonzero"),
        )
        .expect("test binding allocator must be valid");
        for _ in 1..ordinal.get() {
            allocator
                .allocate()
                .expect("test binding ordinal must be valid");
        }
        allocator
            .allocate()
            .expect("test binding ordinal must be valid")
    }
}

/// Opaque request-local token allocator.
///
/// The application owns the globally unique nonzero seed. SQL owns conversion
/// of that seed into binding tokens, so consumers cannot construct a token
/// from a scope and ordinal independently. Tokens retain no provider, plan,
/// wire, or lifecycle data.
pub struct SqlTableBindingAllocator {
    scope: SqlTableBindingScopeId,
    next_ordinal: u32,
}

impl SqlTableBindingAllocator {
    /// Mint one process-unique request-local scope.
    ///
    /// Callers cannot choose the scope value, so observing a binding token
    /// does not provide a way to recreate its mint authority.
    pub fn new_unique() -> Result<Self, String> {
        static NEXT_SCOPE: AtomicU64 = AtomicU64::new(1);
        let scope = NEXT_SCOPE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| "SQL table binding scope space is exhausted".to_string())?;
        let scope = NonZeroU64::new(scope)
            .ok_or_else(|| "SQL table binding scope space is exhausted".to_string())?;
        Ok(Self {
            scope: SqlTableBindingScopeId::new(scope),
            next_ordinal: 0,
        })
    }

    /// Construct a named scope only for cross-crate fixtures whose sealed SQL
    /// plan already contains deterministic binding tokens.
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn try_new_for_test(scope_seed: NonZeroU64) -> Result<Self, String> {
        Ok(Self {
            scope: SqlTableBindingScopeId::new(scope_seed),
            next_ordinal: 0,
        })
    }

    pub fn scope(&self) -> SqlTableBindingScopeId {
        self.scope
    }

    /// Mint exactly one next token in this request-local scope.
    pub fn allocate(&mut self) -> Result<SqlTableBindingId, String> {
        self.next_ordinal = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| "SQL table binding ordinal space is exhausted".to_string())?;
        let ordinal = NonZeroU32::new(self.next_ordinal)
            .ok_or_else(|| "SQL table binding ordinal space is exhausted".to_string())?;
        Ok(SqlTableBindingId::new(self.scope, ordinal))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use arrow::datatypes::DataType;

    use super::SqlTableBindingAllocator;

    #[test]
    fn function_binding_clones_share_one_immutable_selection() {
        let binding = crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false);
        let clone = binding.clone();

        assert!(std::ptr::eq(binding.resolved(), clone.resolved()));
    }

    #[test]
    fn sqlx2_binding_token_is_scoped_and_nonzero() {
        let mut first = SqlTableBindingAllocator::try_new_for_test(NonZeroU64::new(17).unwrap())
            .expect("first allocator");
        let second = SqlTableBindingAllocator::try_new_for_test(NonZeroU64::new(18).unwrap())
            .expect("second allocator");
        let first_scope = first.scope();
        let second_scope = second.scope();
        let binding = first.allocate().expect("first binding");

        assert_eq!(binding.scope(), first_scope);
        assert_eq!(binding.ordinal().get(), 1);
        assert!(binding.belongs_to(first_scope));
        assert!(!binding.belongs_to(second_scope));
        assert_eq!(first_scope.get(), NonZeroU64::new(17).unwrap());
    }
}

#[cfg(test)]
mod aggregate_identity_tests;
