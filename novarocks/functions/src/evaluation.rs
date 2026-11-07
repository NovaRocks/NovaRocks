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

//! Carrier-neutral scalar evaluation inputs and per-row data errors.
//! Control flow and argument evaluation belong to the expression runtime.

use std::fmt;

use arrow_array::ArrayRef;
use arrow_schema::DataType;

/// A checked, ordered set of original batch rows. The dense representation
/// allocates nothing; sparse storage belongs to the evaluation instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Selection<'a> {
    batch_rows: usize,
    rows: Option<&'a [usize]>,
}

impl<'a> Selection<'a> {
    pub const fn all(batch_rows: usize) -> Self {
        Self {
            batch_rows,
            rows: None,
        }
    }

    pub fn try_sparse(
        batch_rows: usize,
        rows: &'a [usize],
    ) -> Result<Self, EvaluationContractError> {
        Self::try_sparse_observed(batch_rows, rows, || Ok::<_, EvaluationContractError>(()))
    }

    /// The same ordered-row author with caller-owned work observations. No
    /// allocation, limit, runtime control or unchecked selection is retained.
    pub fn try_sparse_observed<E: From<EvaluationContractError>>(
        batch_rows: usize,
        rows: &'a [usize],
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Self, E> {
        if let Some(row) = rows.last() {
            let outside = *row >= batch_rows;
            observe()?;
            if outside {
                return Err(EvaluationContractError::InvalidSelection.into());
            }
        }
        for pair in rows.windows(2) {
            let unordered = pair[0] >= pair[1];
            observe()?;
            if unordered {
                return Err(EvaluationContractError::InvalidSelection.into());
            }
        }
        if rows.len() == batch_rows {
            return Ok(Self::all(batch_rows));
        }
        Ok(Self {
            batch_rows,
            rows: Some(rows),
        })
    }

    pub const fn batch_rows(self) -> usize {
        self.batch_rows
    }

    pub fn len(self) -> usize {
        self.rows.map_or(self.batch_rows, <[usize]>::len)
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub const fn is_all(self) -> bool {
        self.rows.is_none()
    }

    /// Map a compact result ordinal back to its original input row. An error
    /// ordinal is never interpreted as a row from an unrelated Selection.
    pub fn row(self, selected_ordinal: usize) -> Option<usize> {
        self.rows.map_or_else(
            || (selected_ordinal < self.batch_rows).then_some(selected_ordinal),
            |rows| rows.get(selected_ordinal).copied(),
        )
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = usize> + 'a {
        (0..self.len()).map(move |ordinal| self.row(ordinal).expect("ordinal is in selection"))
    }
    /// Shared backing compares in O(1); distinct sparse backing observes each row.
    pub fn same_rows_observed<E>(
        self,
        other: Selection<'_>,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if self.batch_rows != other.batch_rows || self.len() != other.len() {
            return Ok(false);
        }
        match (self.rows, other.rows) {
            (None, None) => Ok(true),
            (Some(left), Some(right)) if std::ptr::eq(left, right) => Ok(true),
            (Some(left), Some(right)) => {
                for (left, right) in left.iter().zip(right) {
                    observe()?;
                    if left != right {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Borrowed execution representation of ONE original checked constant.
///
/// The actual producer owns the canonical same-shape array and retains the SAME
/// source CV. This loan grants neither a semantic declaration nor capacity.
/// Validation still checks original full semantics and the exact actual carrier.
#[derive(Clone, Copy, Debug)]
pub struct CanonicalConstantView<'a> {
    original: &'a crate::ConstantValue,
    values: &'a ArrayRef,
}
impl<'a> CanonicalConstantView<'a> {
    /// Facts supplied by the actual projection author, like EvaluatedSourceLoan.
    /// The producer must obtain values from this selected CV through the shared
    /// representation author, retaining its real source and construction owner.
    /// This constructor does not prove payload equality or encoding capability;
    /// those are the pairing obligations of that actual producer, not metadata.
    pub const fn from_authored_projection(
        original: &'a crate::ConstantValue,
        values: &'a ArrayRef,
    ) -> Self {
        Self { original, values }
    }
    pub const fn original(self) -> &'a crate::ConstantValue {
        self.original
    }
    pub fn has_original_shape(self) -> bool {
        self.values.len() == self.original.pool().array().len()
            && (self.original.ordinal() as usize) < self.values.len()
    }
    pub const fn values(self) -> &'a ArrayRef {
        self.values
    }
}

/// Broadcasting is explicit. A one-row column is not silently converted to
/// a scalar, and scalar NULL remains distinct from an unevaluated argument.
#[derive(Clone, Copy, Debug)]
pub enum EvaluatedArgument<'a> {
    Scalar(&'a ArrayRef),
    /// One checked SQL value. Other rows in its retained pool are not inputs.
    Constant(&'a crate::ConstantValue),
    /// Canonical same-shape broadcast retaining its original checked CV.
    ConstantView(CanonicalConstantView<'a>),
    Column(&'a ArrayRef),
    SelectedColumn(&'a SelectedValues<'a>),
}

impl<'a> EvaluatedArgument<'a> {
    /// The identical semantic/source author for raw and canonical CV loans.
    pub fn constant_value(self) -> Option<&'a crate::ConstantValue> {
        match self {
            Self::Constant(value) => Some(value),
            Self::ConstantView(value) => Some(value.original()),
            _ => None,
        }
    }

    pub fn array(self) -> &'a ArrayRef {
        match self {
            Self::Scalar(array) | Self::Column(array) => array,
            Self::Constant(value) => value.pool().array(),
            Self::ConstantView(value) => value.values(),
            Self::SelectedColumn(output) => output.values(),
        }
    }

    /// After validation, use ordinals and rows from the same Selection walk.
    /// Compact arguments retain original row identity without a full scatter.
    pub fn value_row(self, selected_ordinal: usize, batch_row: usize) -> usize {
        match self {
            Self::Scalar(_) => 0,
            Self::ConstantView(value) => value.original().ordinal() as usize,
            Self::Constant(value) => value.ordinal() as usize,
            Self::Column(_) => batch_row,
            Self::SelectedColumn(_) => selected_ordinal,
        }
    }

    pub fn validate(
        self,
        selection: Selection<'_>,
        exact_type: &DataType,
    ) -> Result<(), EvaluationContractError> {
        self.validate_shape(selection)?;
        if !novarocks_type_contract::arrow_data_types_exact(self.array().data_type(), exact_type) {
            return Err(EvaluationContractError::ArgumentType);
        }
        Ok(())
    }

    pub(crate) fn validate_shape(
        self,
        selection: Selection<'_>,
    ) -> Result<(), EvaluationContractError> {
        self.validate_shape_observed(selection, || Ok(()))
    }
    pub(crate) fn validate_shape_observed<E: From<EvaluationContractError>>(
        self,
        selection: Selection<'_>,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        let expected_rows = match self {
            // The immutable owner has already checked this ordinal. A pool
            // may contain many rows; the call broadcasts only this value.
            Self::Constant(_) => return Ok(()),
            Self::ConstantView(value) => {
                return if value.has_original_shape() {
                    Ok(())
                } else {
                    Err(EvaluationContractError::ArgumentLength.into())
                };
            }
            Self::Scalar(_) => 1,
            Self::Column(_) => selection.batch_rows(),
            Self::SelectedColumn(output) => {
                if !output.selection.same_rows_observed(selection, observe)? {
                    return Err(EvaluationContractError::ArgumentSelection.into());
                }
                if !output.errors.is_empty() {
                    return Err(EvaluationContractError::ArgumentRowErrors.into());
                }
                selection.len()
            }
        };
        if self.array().len() != expected_rows {
            return Err(EvaluationContractError::ArgumentLength.into());
        }
        Ok(())
    }
}

/// Only errors explicitly classified by the implementation as row data errors
/// enter this channel. Resource, cancellation, deadline, plan and internal
/// failures remain outer failures and cannot be masked by Boolean decisions.
#[derive(Clone, Debug)]
enum RowDiagnostic {
    Bounded(Box<str>),
    Host(crate::aggregate_invocation_backing::HostDiagnostic),
    Domain(novarocks_failure_contract::SharedFailureCause),
}
/// Semantic data failure retained independently of its diagnostic and row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataFailureReason {
    InvalidValue,
    NumericOutOfRange,
    DivisionByZero,
    InvalidEncoding,
    /// Stable Data reason authored by a more specific value domain.
    Domain(novarocks_failure_contract::FailureReason),
}

#[derive(Clone, Debug)]
pub struct RowDataError {
    selected_ordinal: usize,
    reason: DataFailureReason,
    message: RowDiagnostic,
}
impl PartialEq for RowDataError {
    fn eq(&self, other: &Self) -> bool {
        self.selected_ordinal == other.selected_ordinal
            && self.reason == other.reason
            && self.message() == other.message()
    }
}
impl Eq for RowDataError {}

impl fmt::Display for RowDataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for RowDataError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.message {
            RowDiagnostic::Domain(source) => Some(source),
            _ => None,
        }
    }
}

pub const MAX_ROW_ERROR_MESSAGE_BYTES: usize = 512;

impl RowDataError {
    /// Admit only a domain's actual Data failure into the maskable journal.
    /// Other facts return on the outer channel with the SAME original source.
    /// No caller-provided diagnostic can change this decision.
    pub(crate) fn from_domain<E>(
        selected_ordinal: usize,
        cause: E,
    ) -> Result<Self, crate::KernelFailure>
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        let source = SharedFailureCause::new(cause);
        let fact = source.failure_fact();
        if fact.class() != FailureClass::Data {
            return Err(crate::KernelFailure::rejected_row_source(source));
        }
        Ok(Self {
            selected_ordinal,
            reason: DataFailureReason::Domain(fact.reason()),
            message: RowDiagnostic::Domain(source),
        })
    }
    pub(crate) fn projected_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::FailureProjection;
        match &self.message {
            RowDiagnostic::Domain(source) => source.failure_fact(),
            _ => self.reason.failure_fact(),
        }
    }
    pub fn new(selected_ordinal: usize, message: &str) -> Self {
        Self::with_reason(selected_ordinal, DataFailureReason::InvalidValue, message)
    }
    pub fn with_reason(selected_ordinal: usize, reason: DataFailureReason, message: &str) -> Self {
        let mut end = message.len().min(MAX_ROW_ERROR_MESSAGE_BYTES);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            selected_ordinal,
            reason,
            message: RowDiagnostic::Bounded(message[..end].into()),
        }
    }

    pub const fn reason(&self) -> DataFailureReason {
        self.reason
    }
    pub const fn selected_ordinal(&self) -> usize {
        self.selected_ordinal
    }

    pub fn message(&self) -> &str {
        match &self.message {
            RowDiagnostic::Bounded(message) => message,
            RowDiagnostic::Host(message) => message.message(),
            RowDiagnostic::Domain(source) => source.detail(),
        }
    }
    /// Remap the true selected ordinal without clipping or reformatting the
    /// diagnostic. A host-backed diagnostic loans its actual shared block.
    pub fn with_selected_ordinal(&self, selected_ordinal: usize) -> Self {
        Self {
            selected_ordinal,
            reason: self.reason,
            message: self.message.clone(),
        }
    }
    pub(crate) fn prepare_host_with_reason(
        selected_ordinal: usize,
        reason: DataFailureReason,
        allocator: &crate::aggregate_host_allocator::HostAggregateAllocator,
        work: &mut crate::kernel_input::EvaluationCheckpoints<'_>,
        formatter: impl FnOnce(&mut dyn fmt::Write) -> fmt::Result,
    ) -> Result<Self, crate::KernelFailure> {
        let diagnostic = crate::aggregate_invocation_backing::HostDiagnostic::prepare(
            allocator, work, formatter,
        )?;
        Ok(Self {
            selected_ordinal,
            reason,
            message: RowDiagnostic::Host(diagnostic),
        })
    }
}

/// Compact values retain their Selection. An error ordinal has an outer
/// validity NULL (or Null-typed value), so validation needs no dictionary
/// expansion or logical-null bitmap allocation. A logical NULL with no error
/// is a successful SQL NULL. No unselected row appears in values or errors.
#[derive(Clone, Debug)]
pub struct SelectedValues<'a> {
    selection: Selection<'a>,
    values: ArrayRef,
    errors: Box<[RowDataError]>,
}

impl<'a> SelectedValues<'a> {
    pub fn try_new(
        selection: Selection<'a>,
        exact_type: &DataType,
        values: ArrayRef,
        errors: Box<[RowDataError]>,
    ) -> Result<Self, EvaluationContractError> {
        Self::try_new_with(
            selection,
            exact_type,
            values,
            errors,
            || Ok(()),
            |left, right, _| Ok(novarocks_type_contract::arrow_data_types_exact(left, right)),
        )
    }

    /// The same result author with caller-owned observations and a bounded
    /// exact type walk. Frozen result domains must already have been admitted.
    pub fn try_new_observed<
        E: From<EvaluationContractError> + From<novarocks_type_contract::ValueTypeError>,
    >(
        selection: Selection<'a>,
        exact_type: &DataType,
        values: ArrayRef,
        errors: Box<[RowDataError]>,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Self, E> {
        Self::try_new_with(
            selection,
            exact_type,
            values,
            errors,
            observe,
            |left, right, observe| {
                novarocks_type_contract::arrow_data_types_exact_observed(left, right, observe)
            },
        )
    }

    fn try_new_with<E: From<EvaluationContractError>, F: FnMut() -> Result<(), E>>(
        selection: Selection<'a>,
        exact_type: &DataType,
        values: ArrayRef,
        errors: Box<[RowDataError]>,
        mut observe: F,
        compare: impl FnOnce(&DataType, &DataType, &mut F) -> Result<bool, E>,
    ) -> Result<Self, E> {
        if values.len() != selection.len() {
            return Err(EvaluationContractError::ResultLength.into());
        }
        if !compare(values.data_type(), exact_type, &mut observe)? {
            return Err(EvaluationContractError::ResultType.into());
        }
        if errors.len() > selection.len() {
            return Err(EvaluationContractError::InvalidRowErrors.into());
        }
        for pair in errors.windows(2) {
            let unordered = pair[0].selected_ordinal >= pair[1].selected_ordinal;
            observe()?;
            if unordered {
                return Err(EvaluationContractError::InvalidRowErrors.into());
            }
        }
        for error in &errors {
            let invalid = error.selected_ordinal >= selection.len()
                || (values.data_type() != &DataType::Null
                    && !values.is_null(error.selected_ordinal));
            observe()?;
            if invalid {
                return Err(EvaluationContractError::InvalidRowErrors.into());
            }
        }
        Ok(Self {
            selection,
            values,
            errors,
        })
    }

    pub const fn selection(&self) -> Selection<'a> {
        self.selection
    }

    pub const fn values(&self) -> &ArrayRef {
        &self.values
    }

    pub fn errors(&self) -> &[RowDataError] {
        &self.errors
    }

    pub fn into_parts(self) -> (Selection<'a>, ArrayRef, Box<[RowDataError]>) {
        (self.selection, self.values, self.errors)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationContractError {
    InvalidSelection,
    ArgumentLength,
    ArgumentType,
    ArgumentSelection,
    ArgumentRowErrors,
    ResultLength,
    ResultType,
    InvalidRowErrors,
}

impl fmt::Display for EvaluationContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidSelection => "selection rows must be ordered, unique and in the batch",
            Self::ArgumentLength => "evaluated argument length differs from its explicit shape",
            Self::ArgumentType => "evaluated argument type differs from its frozen binding",
            Self::ArgumentSelection => {
                "compact argument selection differs from the evaluation domain"
            }
            Self::ArgumentRowErrors => {
                "row errors must be resolved before calling an ordinary kernel"
            }
            Self::ResultLength => "kernel output length differs from the selected row count",
            Self::ResultType => "kernel output type differs from its frozen binding",
            Self::InvalidRowErrors => {
                "kernel row errors must be ordered, unique NULL result ordinals"
            }
        })
    }
}

impl std::error::Error for EvaluationContractError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, NullArray};

    use super::*;

    #[test]
    fn observed_sparse_author_keeps_bare_results_and_preserves_each_primary_refusal() {
        use crate::KernelFailure;
        let rows = (0..320).map(|i| i * 2).collect::<Vec<_>>();
        let bare = Selection::try_sparse(640, &rows).unwrap();
        let mut visits = 0;
        let observed = Selection::try_sparse_observed(640, &rows, || {
            visits += 1;
            Ok::<_, KernelFailure>(())
        })
        .unwrap();
        assert_eq!(observed, bare);
        assert_eq!(visits, 320);
        for stop in [1, 256, 320] {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let mut visits = 0;
                let result = Selection::try_sparse_observed(640, &rows, || {
                    visits += 1;
                    if visits == stop {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(cause));
                assert_eq!(visits, stop);
            }
        }
    }

    #[test]
    fn observed_sparse_author_preserves_range_order_errors_and_empty_dense_shapes() {
        for (rows, count) in [(&[8][..], 1), (&[3, 3][..], 2), (&[4, 2][..], 2)] {
            let mut visits = 0;
            assert_eq!(
                Selection::try_sparse_observed(8, rows, || {
                    visits += 1;
                    Ok::<_, EvaluationContractError>(())
                }),
                Err(EvaluationContractError::InvalidSelection)
            );
            assert_eq!(visits, count);
        }
        let mut visits = 0;
        assert_eq!(
            Selection::try_sparse_observed(0, &[], || {
                visits += 1;
                Ok::<_, EvaluationContractError>(())
            })
            .unwrap(),
            Selection::all(0)
        );
        assert_eq!(visits, 0);
        assert_eq!(
            Selection::try_sparse_observed(3, &[0, 1, 2], || Ok::<_, EvaluationContractError>(())),
            Ok(Selection::all(3))
        );
    }

    #[test]
    fn selection_preserves_original_rows_without_dense_storage() {
        let dense = Selection::all(usize::MAX);
        assert_eq!(dense.len(), usize::MAX);
        assert_eq!(dense.row(usize::MAX - 1), Some(usize::MAX - 1));
        assert_eq!(dense.row(usize::MAX), None);
        let sparse = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        assert_eq!(sparse.iter().collect::<Vec<_>>(), [1, 4, 9]);
        for invalid in [&[1, 1][..], &[4, 1], &[10]] {
            assert_eq!(
                Selection::try_sparse(10, invalid),
                Err(EvaluationContractError::InvalidSelection)
            );
        }
        assert!(Selection::try_sparse(0, &[]).unwrap().is_empty());
        assert!(Selection::all(0).is_empty());
    }

    #[test]
    fn scalar_broadcast_is_explicit_and_does_not_rebind_types() {
        let scalar = Arc::new(Int32Array::from(vec![Some(7)])) as ArrayRef;
        let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        EvaluatedArgument::Scalar(&scalar)
            .validate(selection, &DataType::Int32)
            .unwrap();
        assert_eq!(EvaluatedArgument::Scalar(&scalar).value_row(2, 9), 0);
        assert_eq!(
            EvaluatedArgument::Column(&scalar).validate(selection, &DataType::Int32),
            Err(EvaluationContractError::ArgumentLength)
        );
        assert_eq!(
            EvaluatedArgument::Scalar(&scalar).validate(selection, &DataType::Int64),
            Err(EvaluationContractError::ArgumentType)
        );
    }

    #[test]
    fn compact_children_keep_their_domain_at_the_parent_call() {
        let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        let other = Selection::try_sparse(10, &[0, 3, 8]).unwrap();
        let child = SelectedValues::try_new(
            selection,
            &DataType::Int32,
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Box::default(),
        )
        .unwrap();
        let argument = EvaluatedArgument::SelectedColumn(&child);
        argument.validate(selection, &DataType::Int32).unwrap();
        assert_eq!(argument.value_row(2, 9), 2);
        assert_eq!(
            argument.validate(other, &DataType::Int32),
            Err(EvaluationContractError::ArgumentSelection)
        );
        let (preserved, _, _) = child.clone().into_parts();
        assert_eq!(preserved, selection);
        assert_ne!(preserved, other);
        assert_eq!(
            Selection::try_sparse(3, &[0, 1, 2]).unwrap(),
            Selection::all(3)
        );
    }

    #[test]
    fn sparse_row_errors_are_distinct_from_successful_nulls() {
        let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        let values = Arc::new(Int32Array::from(vec![None, Some(7), None])) as ArrayRef;
        let output = SelectedValues::try_new(
            selection,
            &DataType::Int32,
            values.clone(),
            Box::from([RowDataError::new(2, "invalid input")]),
        )
        .unwrap();
        assert_eq!(
            selection.row(output.errors()[0].selected_ordinal()),
            Some(9)
        );
        assert!(output.values().is_null(0)); // Successful SQL NULL.
        assert_eq!(output.selection(), selection);
        assert_eq!(
            EvaluatedArgument::SelectedColumn(&output).validate(selection, &DataType::Int32),
            Err(EvaluationContractError::ArgumentRowErrors)
        );
        for ordinal in [1, 3] {
            assert!(
                SelectedValues::try_new(
                    selection,
                    &DataType::Int32,
                    values.clone(),
                    Box::from([RowDataError::new(ordinal, "invalid input")])
                )
                .is_err()
            );
        }
        assert!(
            SelectedValues::try_new(
                selection,
                &DataType::Int32,
                values.clone(),
                Box::from([
                    RowDataError::new(2, "first"),
                    RowDataError::new(2, "duplicate")
                ])
            )
            .is_err()
        );
        let nulls = Arc::new(NullArray::new(3)) as ArrayRef;
        assert!(
            SelectedValues::try_new(selection, &DataType::Int32, nulls.clone(), Box::default())
                .is_err()
        );
        SelectedValues::try_new(
            selection,
            &DataType::Null,
            nulls,
            Box::from([RowDataError::new(1, "invalid input")]),
        )
        .unwrap();
    }

    #[test]
    fn row_error_diagnostics_are_bounded_at_utf8_boundaries() {
        let error = RowDataError::new(0, &"界".repeat(512));
        assert!(error.message().len() <= MAX_ROW_ERROR_MESSAGE_BYTES);
        assert_eq!(error.message().len() % 3, 0);
    }

    #[test]
    fn observed_result_author_matches_bare_ordered_unique_null_error_contract() {
        use crate::KernelFailure;
        let rows = [1, 4, 9];
        let selection = Selection::try_sparse(10, &rows).unwrap();
        let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(7), None]));
        let cases: Vec<Box<[RowDataError]>> = vec![
            Box::default(),
            Box::from([RowDataError::new(0, "first"), RowDataError::new(2, "last")]),
            Box::from([RowDataError::new(2, "last"), RowDataError::new(0, "first")]),
            Box::from([
                RowDataError::new(0, "first"),
                RowDataError::new(0, "duplicate"),
            ]),
            Box::from([RowDataError::new(3, "outside selected domain")]),
            Box::from([RowDataError::new(
                1,
                "successful nonnull value cannot be an error",
            )]),
        ];
        for errors in cases {
            let bare = SelectedValues::try_new(
                selection,
                &DataType::Int32,
                values.clone(),
                errors.clone(),
            );
            let mut observations = 0;
            let observed = SelectedValues::try_new_observed::<KernelFailure>(
                selection,
                &DataType::Int32,
                values.clone(),
                errors.clone(),
                || {
                    observations += 1;
                    Ok(())
                },
            );
            assert!(observations > 0);
            match (bare, observed) {
                (Ok(bare), Ok(observed)) => {
                    assert_eq!(bare.selection(), selection);
                    assert_eq!(observed.selection(), selection);
                    assert_eq!(bare.errors(), errors.as_ref());
                    assert_eq!(observed.errors(), errors.as_ref());
                    assert!(Arc::ptr_eq(bare.values(), &values));
                    assert!(Arc::ptr_eq(observed.values(), &values));
                    for error in observed.errors() {
                        assert_eq!(
                            selection.row(error.selected_ordinal()),
                            rows.get(error.selected_ordinal()).copied()
                        );
                    }
                }
                (Err(bare), Err(observed)) => {
                    crate::kernel_control::test_contract::exact(&observed, bare)
                }
                _ => panic!("observed result author changed the bare acceptance contract"),
            }
        }
        for (selection, exact_type, expected) in [
            (
                Selection::all(4),
                DataType::Int32,
                EvaluationContractError::ResultLength,
            ),
            (
                selection,
                DataType::Int64,
                EvaluationContractError::ResultType,
            ),
        ] {
            assert_eq!(
                SelectedValues::try_new(selection, &exact_type, values.clone(), Box::default())
                    .unwrap_err(),
                expected,
            );
            crate::kernel_control::test_contract::exact(
                &SelectedValues::try_new_observed::<KernelFailure>(
                    selection,
                    &exact_type,
                    values.clone(),
                    Box::default(),
                    || Ok(()),
                )
                .unwrap_err(),
                expected,
            );
        }
    }

    #[test]
    fn observed_null_result_keeps_row_errors_and_successful_nulls_separate() {
        use crate::KernelFailure;
        let rows = [2, 7, 11];
        let selection = Selection::try_sparse(12, &rows).unwrap();
        let values: ArrayRef = Arc::new(NullArray::new(3));
        let result = SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Null,
            values.clone(),
            Box::from([RowDataError::new(1, "required child error")]),
            || Ok(()),
        )
        .unwrap();
        assert!(Arc::ptr_eq(result.values(), &values));
        assert_eq!(result.errors().len(), 1);
        assert_eq!(result.errors()[0].selected_ordinal(), 1);
        assert_eq!(
            result
                .selection()
                .row(result.errors()[0].selected_ordinal()),
            Some(7)
        );
        assert_eq!(result.values().logical_null_count(), 3);
        assert!(
            result
                .errors()
                .iter()
                .all(|error| error.selected_ordinal() != 0 && error.selected_ordinal() != 2)
        );
    }

    #[test]
    fn observed_result_author_propagates_all_seven_failure_categories_at_each_real_callback() {
        use crate::{KernelDiagnostic, KernelFailure};
        let rows = (0..320).map(|row| row * 2 + 1).collect::<Vec<_>>();
        let selection = Selection::try_sparse(640, &rows).unwrap();
        let values: ArrayRef = Arc::new(Int32Array::from(vec![None; 320]));
        let errors = (0..320)
            .map(|ordinal| RowDataError::new(ordinal, "required row error"))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut successful_observations = 0;
        let result = SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Int32,
            values.clone(),
            errors.clone(),
            || {
                successful_observations += 1;
                Ok(())
            },
        )
        .unwrap();
        assert!(successful_observations > 256);
        assert_eq!(result.errors(), errors.as_ref());
        assert_eq!(
            result
                .selection()
                .row(result.errors()[319].selected_ordinal()),
            Some(639)
        );
        let causes = [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid program")),
            KernelFailure::Internal(KernelDiagnostic::new("original internal failure")),
            KernelFailure::Operational(KernelDiagnostic::new("original operational failure")),
            KernelFailure::InstanceFailed,
        ];
        // This author emits observations; quantum accounting belongs to the
        // caller. Reject every real observation, including those beyond 256.
        for stop_at in 1..=successful_observations {
            for cause in &causes {
                let mut observed = 0;
                let mut refused = false;
                let result = SelectedValues::try_new_observed::<KernelFailure>(
                    selection,
                    &DataType::Int32,
                    values.clone(),
                    errors.clone(),
                    || {
                        assert!(!refused, "result author retried an originating failure");
                        observed += 1;
                        if observed == stop_at {
                            refused = true;
                            Err(cause.clone())
                        } else {
                            Ok(())
                        }
                    },
                );
                assert_eq!(result.unwrap_err(), *cause);
                assert!(refused);
                assert_eq!(observed, stop_at);
            }
        }
    }
}

#[cfg(test)]
mod row_domain_source_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    #[test]
    fn actual_map_data_keeps_its_reason_source_and_remapped_ordinal() {
        let cause = crate::builtin::map_lookup_core::LookupRowFailure::MissingKey;
        let expected = cause.failure_fact();
        let error = RowDataError::from_domain(7, cause).unwrap();
        let moved = error.with_selected_ordinal(2);
        assert_eq!(moved.selected_ordinal(), 2);
        assert_eq!(moved.failure_fact(), expected);
        assert_eq!(moved.reason().failure_fact(), expected);
        let original = error.source().unwrap().source().unwrap();
        let remapped = moved.source().unwrap().source().unwrap();
        assert!(std::ptr::eq(original, remapped));
        assert!(original.is::<crate::builtin::map_lookup_core::LookupRowFailure>());
    }
    #[test]
    fn resource_control_and_contract_sources_cannot_become_maskable_data() {
        for cause in [
            crate::KernelFailure::ResourceExhausted,
            crate::KernelFailure::Cancelled,
            crate::KernelFailure::Internal(crate::KernelDiagnostic::new("invalid value")),
        ] {
            let expected = cause.failure_fact();
            assert_ne!(expected.class(), FailureClass::Data);
            let error = RowDataError::from_domain(0, cause).unwrap_err();
            assert_eq!(error.failure_fact(), expected);
            assert!(
                error
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<crate::KernelFailure>()
            );
        }
    }
}
