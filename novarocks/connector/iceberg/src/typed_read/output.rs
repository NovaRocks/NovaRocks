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

use novarocks_fs::FileReadContext;
use novarocks_spi::connector::read_stack::SourcePage;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ENGINE_OUTPUT_POLICY, ScanOutputPolicy,
};
use novarocks_value_arrays::{
    ByteLayout, ValueError, ValueFailure, ValueViolation,
    access::ByteSource,
    measure::{OutputColumn, OutputRows, PrefixPlanner},
};

use crate::file_reader::map_file_error;

#[derive(Clone)]
pub(crate) enum OutputSlot {
    Base,
    Constant(arrow::array::ArrayRef),
}

#[derive(Clone)]
pub(crate) struct OutputProjection(pub(crate) Vec<OutputSlot>);

impl OutputProjection {
    pub(super) fn project<'a>(
        &'a self,
        base: &[OutputColumn<'a>],
    ) -> Result<Vec<OutputColumn<'a>>, ConnectorError> {
        let inconsistent = || {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "Iceberg output projection does not consume the exact base columns",
            )
        };
        let mut remaining = base.iter().copied();
        let columns = self
            .0
            .iter()
            .map(|slot| match slot {
                OutputSlot::Base => remaining.next().ok_or_else(inconsistent),
                OutputSlot::Constant(array) => Ok(OutputColumn::constant(array, array.data_type())),
            })
            .collect::<Result<Vec<_>, _>>()?;
        if remaining.next().is_some() {
            return Err(inconsistent());
        }
        Ok(columns)
    }
}

pub(crate) fn byte_source(
    input: &arrow::array::ArrayRef,
    utf8: bool,
) -> Result<ByteSource<'_>, ValueError> {
    use arrow::datatypes::DataType;
    let layout_type = match input.data_type() {
        DataType::Dictionary(_, values) => values.as_ref(),
        other => other,
    };
    let layout = ByteLayout::from_data_type(layout_type)
        .filter(|layout| layout.is_utf8() == utf8)
        .ok_or(ValueError::Invariant(ValueViolation::Layout))?;
    ByteSource::parse(input, layout, Some(layout))
}

pub(super) struct OutputCursor {
    planner: PrefixPlanner<'static>,
    policy: ScanOutputPolicy,
}

impl OutputCursor {
    pub(super) fn new(
        columns: &[OutputColumn<'_>],
        rows: OutputRows<'static>,
        context: &FileReadContext,
    ) -> Result<Self, ConnectorError> {
        Self::with_policy(columns, rows, context, ENGINE_OUTPUT_POLICY)
    }

    pub(super) fn with_policy(
        columns: &[OutputColumn<'_>],
        rows: OutputRows<'static>,
        context: &FileReadContext,
        policy: ScanOutputPolicy,
    ) -> Result<Self, ConnectorError> {
        let planner = PrefixPlanner::new(
            columns,
            rows,
            &mut |_| context.check_active().map_err(map_file_error),
            &mut |_| context.check_active().map_err(map_file_error),
        )
        .map_err(value_failure)?;
        Ok(Self { planner, policy })
    }

    pub(super) const fn is_exhausted(&self) -> bool {
        self.planner.remaining_rows() == 0
    }

    pub(super) fn next_page(
        &mut self,
        context: &FileReadContext,
    ) -> Result<Option<SourcePage>, ConnectorError> {
        if self.is_exhausted() {
            return Ok(None);
        }
        let mut work = |_| context.check_active().map_err(map_file_error);
        let prefix = self
            .planner
            .plan_prefix(
                self.policy.max_rows,
                self.policy.soft_target_new_column_bytes,
                &mut work,
            )
            .map_err(value_failure)?;
        let rows = prefix.rows();
        let columns = prefix
            .build_observed(
                &mut |_| context.check_active().map_err(map_file_error),
                &mut work,
            )
            .map_err(value_failure)?;
        let page = SourcePage::try_new(rows, columns)?;
        let logical_output_bytes = prefix.logical_bytes();
        let constructed_column_bytes = prefix.new_column_bytes();
        self.planner.advance(rows).map_err(value_error)?;
        tracing::trace!(rows, %logical_output_bytes, %constructed_column_bytes, "Iceberg output page constructed");
        Ok(Some(page))
    }
}

fn value_failure(failure: ValueFailure<ConnectorError>) -> ConnectorError {
    match failure {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(error) => value_error(error),
    }
}

fn value_error(error: ValueError) -> ConnectorError {
    let kind = match error {
        ValueError::Allocation { .. } => ConnectorErrorKind::ResourceExhausted,
        _ => ConnectorErrorKind::CorruptData,
    };
    ConnectorError::from_domain(kind, error)
}
