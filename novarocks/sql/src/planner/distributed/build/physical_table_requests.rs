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

//! Static relation requests from the original table-function node.

use std::sync::Arc;

use novarocks_functions::{
    ConstantPolicy, FunctionArgument, FunctionBindingRequest, FunctionBindingSelection,
    FunctionResultType, MAX_CALL_EFFECT_ARGUMENTS,
};
use novarocks_physical_plan::{
    BoundTableFunction, ConstantPools, ExprId, Fragment, NodeKind, PhysicalNode,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionKind,
    ValueTypeError,
};

use super::{
    lowered_draft::{
        CanonicalCallOperationalRequest, CheckedTableLogicalSourceEntry, SqlSourceJournalError,
    },
    physical_call_arguments::{
        PhysicalArgumentError, argument_types_exact_observed, author_physical_argument_observed,
    },
};
use crate::binding::CapturedLogicalCallArguments;

#[cfg(test)]
#[path = "physical_table_journal_tests.rs"]
mod journal_tests;

#[derive(Debug)]
pub(crate) enum PhysicalTableRequestError {
    Control(CompileControlError),
    Argument(PhysicalArgumentError),
    Journal(SqlSourceJournalError),
    Type(ValueTypeError),
    InvalidSource(&'static str),
    MissingArgument(ExprId),
    MissingRoot(novarocks_expression_contract::RootOrdinal),
}
impl From<ValueTypeError> for PhysicalTableRequestError {
    fn from(error: ValueTypeError) -> Self {
        Self::Type(error)
    }
}
impl From<SqlSourceJournalError> for PhysicalTableRequestError {
    fn from(error: SqlSourceJournalError) -> Self {
        match error {
            SqlSourceJournalError::Control(cause) => Self::Control(cause),
            other => Self::Journal(other),
        }
    }
}

#[derive(Debug)]
enum TableRequestArguments<'source> {
    OwnedPhysical(Vec<FunctionArgument>),
    BorrowedCanonical {
        captured: &'source CapturedLogicalCallArguments,
        canonical: &'source CanonicalCallOperationalRequest,
    },
}
impl From<CompileControlError> for PhysicalTableRequestError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<PhysicalArgumentError> for PhysicalTableRequestError {
    fn from(error: PhysicalArgumentError) -> Self {
        match error {
            PhysicalArgumentError::Control(cause) => Self::Control(cause),
            other => Self::Argument(other),
        }
    }
}

/// One original node loan and the complete selected relation signature.
/// The selected Arc feeds both CallEffectInput and actual fresh preparation.
/// It authenticates neither installed capability nor complete call effects.
#[derive(Debug)]
pub(crate) struct AuthoredPhysicalTableRequest<'a> {
    source: &'a PhysicalNode,
    function: &'a BoundTableFunction,
    selected: Arc<FunctionBindingSelection>,
    arguments: TableRequestArguments<'a>,
}
impl AuthoredPhysicalTableRequest<'_> {
    pub const fn source(&self) -> &PhysicalNode {
        self.source
    }
    pub const fn function(&self) -> &BoundTableFunction {
        self.function
    }
    pub const fn selected(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn request(&self) -> FunctionBindingRequest<'_> {
        if let TableRequestArguments::BorrowedCanonical { canonical, .. } = &self.arguments {
            return canonical.request();
        }
        let TableRequestArguments::OwnedPhysical(arguments) = &self.arguments else {
            unreachable!()
        };
        FunctionBindingRequest {
            arguments,
            logical_argument_count: arguments.len(),
            // A scalar result constraint cannot represent the selected relation.
            // The exact installed table owner validates all result columns.
            expected_result_type: None,
        }
    }
    pub fn captured_decimal_overflow_policy(&self) -> Option<DecimalOverflowPolicy> {
        match &self.arguments {
            TableRequestArguments::OwnedPhysical(_) => None,
            TableRequestArguments::BorrowedCanonical { captured, .. } => {
                Some(captured.binding().decimal_overflow_policy())
            }
        }
    }
    pub fn captured_constant_policy(&self) -> Option<ConstantPolicy> {
        match &self.arguments {
            TableRequestArguments::OwnedPhysical(_) => None,
            TableRequestArguments::BorrowedCanonical { captured, .. } => {
                Some(captured.constant_policy())
            }
        }
    }
}

/// Preserve ordered actual argument occurrences and the original full relation
/// signature separately. Outer pass-through columns, LEFT assembly and node
/// output roles remain with the mandatory physical node validator. No scalar
/// result projection, name resolution or legacy effect metadata is used here.
/// The actual installed table owner validates these selected/source types,
/// including argument shape and its precise UNNEST specialization domain.
///
/// Caller admission covers the source and opaque nested signature/type clones.
/// Existing call bounds precede expansion and fallible parameter reservation.
/// The caller owns entry, the ordinary/success footer, exact per-use policy and
/// environment, and subsequent fresh preparation. Originating control/resource
/// refusals return directly on the same meter without a later observation.
pub(crate) fn author_physical_table_request_observed<'a>(
    source: &'a PhysicalNode,
    fragment: &Fragment,
    pools: &ConstantPools,
    literal_policy: ConstantPolicy,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalTableRequest<'a>, PhysicalTableRequestError> {
    work.flush()?;
    let original = fragment.nodes().get(&source.id);
    work.step()?;
    work.flush()?;
    if !original.is_some_and(|node| std::ptr::eq(node, source)) {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table request source is not the original fragment node",
        ));
    }
    let NodeKind::TableFunction {
        function,
        arguments: ids,
        ..
    } = &source.kind
    else {
        return Err(PhysicalTableRequestError::InvalidSource(
            "physical table request requires an actual table-function node",
        ));
    };
    let bounded = ids.len() <= MAX_CALL_EFFECT_ARGUMENTS
        && function.argument_types.len() <= MAX_CALL_EFFECT_ARGUMENTS
        && function.result_types.len() <= MAX_CALL_EFFECT_ARGUMENTS;
    work.step()?;
    if !bounded {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let exact_count = ids.len() == function.argument_types.len();
    work.step()?;
    if !exact_count {
        return Err(PhysicalTableRequestError::InvalidSource(
            "physical table argument count differs from its selected signature",
        ));
    }
    let has_results = !function.result_types.is_empty();
    work.step()?;
    if !has_results {
        return Err(PhysicalTableRequestError::InvalidSource(
            "physical table request has no selected relation columns",
        ));
    }
    work.flush()?;
    let mut arguments = Vec::new();
    arguments
        .try_reserve_exact(ids.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.flush()?;
    for &id in ids {
        let argument = super::unit_source::UnitDefinitions::from_node(source).root_definition(id);
        work.step()?;
        let argument = argument.ok_or(PhysicalTableRequestError::MissingRoot(id))?;
        arguments.push(author_physical_argument_observed(
            argument,
            pools,
            literal_policy,
            CompilePhase::FunctionSpecialization,
            work,
        )?);
        work.step()?;
    }
    work.flush()?;
    let selected = Arc::new(FunctionBindingSelection {
        overload: function.overload.clone(),
        argument_types: function.argument_types.clone(),
        result_type: FunctionResultType::Relation(function.result_types.clone()),
        aggregate: None,
    });
    work.flush()?;
    Ok(AuthoredPhysicalTableRequest {
        source,
        function,
        selected,
        arguments: TableRequestArguments::OwnedPhysical(arguments),
    })
}

/// Borrow the actual emitter's retained operational arguments and exact selected
/// Arc. Source membership is supplied only by the checked SQL journal loan;
/// this comparison cannot reconstruct it from equal physical metadata. In
/// particular, a computed physical constant stays the original nonconstant.
///
/// The caller owns entry, ordinary/success footer and source/opaque comparison
/// admission. This path creates no parameter vector, constant backing, type
/// projection, overload selection or fallback to the independent direct path.
pub(crate) fn author_physical_table_request_from_journal_observed<'source>(
    entry: &CheckedTableLogicalSourceEntry<'source>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AuthoredPhysicalTableRequest<'source>, PhysicalTableRequestError> {
    let source = entry.source();
    work.flush()?;
    let actual = entry.fragment().nodes().get(&source.id);
    work.step()?;
    work.flush()?;
    if !actual.is_some_and(|node| std::ptr::eq(node, source)) {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table journal request loans a foreign physical node",
        ));
    }
    let shape = match &source.kind {
        NodeKind::TableFunction {
            function,
            arguments,
            ..
        } => Ok((function, arguments)),
        _ => Err(PhysicalTableRequestError::InvalidSource(
            "table journal request requires its original table-function node",
        )),
    };
    work.step()?;
    let (function, ids) = shape?;
    let captured = entry.captured();
    let canonical = entry.canonical_operational();
    let request = canonical.request();
    let selected = canonical.selected();
    if ids.len() > MAX_CALL_EFFECT_ARGUMENTS
        || function.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        || function.result_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        || selected.argument_types.len() > MAX_CALL_EFFECT_ARGUMENTS
        || request.arguments.len() > MAX_CALL_EFFECT_ARGUMENTS
    {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let belongs = canonical.belongs_to(captured);
    work.step()?;
    if !belongs {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table operational request has a different original binding loan",
        ));
    }
    let resolved = captured.binding().resolved();
    work.flush()?;
    let identity = resolved.kind == FunctionKind::Table
        && resolved.function_id == function.function_id
        && resolved.selected.overload == function.overload
        && selected.overload == function.overload
        && selected.aggregate.is_none();
    work.step()?;
    work.flush()?;
    if !identity {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table operational selection differs from its original exact identity",
        ));
    }
    let same_count = ids.len() == entry.arguments().len()
        && ids.len() == request.arguments.len()
        && ids.len() == request.logical_argument_count
        && ids.len() == captured.request().logical_argument_count
        && ids.len() == function.argument_types.len()
        && ids.len() == selected.argument_types.len()
        && request.expected_result_type.is_none();
    work.step()?;
    if !same_count {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table operational request differs from its ordered logical channels",
        ));
    }
    for (&actual, &original) in ids.iter().zip(entry.arguments()) {
        let definitions = super::unit_source::UnitDefinitions::from_node(entry.source());
        let matching = definitions
            .root(actual)
            .is_some_and(|root| root.definition == original);
        work.step()?;
        if !matching {
            return Err(PhysicalTableRequestError::InvalidSource(
                "table operational request changes an original argument expression",
            ));
        }
        work.flush()?;
        let expression = definitions.definition(original);
        work.step()?;
        work.flush()?;
        if expression.is_none() {
            return Err(PhysicalTableRequestError::MissingArgument(original));
        }
    }
    for (expected, actual) in selected.argument_types.iter().zip(&function.argument_types) {
        work.flush()?;
        let matching =
            argument_types_exact_observed::<PhysicalTableRequestError>(expected, actual, work)?;
        work.flush()?;
        if !matching {
            return Err(PhysicalTableRequestError::InvalidSource(
                "table operational selection differs from the full argument signature",
            ));
        }
    }
    let relation = match &selected.result_type {
        FunctionResultType::Relation(results) => Ok(results),
        _ => Err(PhysicalTableRequestError::InvalidSource(
            "table operational selection has no whole relation result",
        )),
    };
    work.step()?;
    let results = relation?;
    if results.len() > MAX_CALL_EFFECT_ARGUMENTS {
        return Err(CompileControlError::ResourceExhausted.into());
    }
    let same_results = !results.is_empty() && results.len() == function.result_types.len();
    work.step()?;
    if !same_results {
        return Err(PhysicalTableRequestError::InvalidSource(
            "table operational selection differs from its complete result arity",
        ));
    }
    for (expected, actual) in results.iter().zip(&function.result_types) {
        work.flush()?;
        let matching = expected
            .exactly_equals_observed::<PhysicalTableRequestError>(actual, || {
                work.step().map_err(PhysicalTableRequestError::from)
            })?;
        work.flush()?;
        if !matching {
            return Err(PhysicalTableRequestError::InvalidSource(
                "table operational selection differs from its full relation signature",
            ));
        }
    }
    work.flush()?;
    let selected = Arc::clone(selected);
    work.step()?;
    work.flush()?;
    Ok(AuthoredPhysicalTableRequest {
        source,
        function,
        selected,
        arguments: TableRequestArguments::BorrowedCanonical {
            captured,
            canonical,
        },
    })
}
