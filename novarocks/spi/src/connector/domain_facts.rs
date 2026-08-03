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

//! Immutable, bounded physical domain evidence attached to one prepared scan unit.
//!
//! Design: ADR-0039 (docs/adr/ADR-0039-immutable-scan-unit-domain-facts.md)

use std::cmp::Ordering;

use super::{ConnectorError, ConnectorErrorKind, ConnectorScalarType, ConnectorScalarValue};

pub const MAX_CONNECTOR_SCAN_UNIT_FACT_COLUMNS: usize = 1024;
pub const MAX_CONNECTOR_SCAN_UNIT_FACT_VARIABLE_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_SCAN_UNIT_FACT_PAYLOAD_BYTES: usize = 1024 * 1024;

const FACTS_UNIT_FIXED_BYTES: usize = 16;
const FACTS_COLUMN_FIXED_BYTES: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorScanUnitFactsEvidence {
    Exact,
    Conservative,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorScanUnitFactsMissingReason {
    ProviderUnsupported,
    NoPinnedStatistics,
    PhysicalStatisticsAbsent,
    DataTypeUnsupported,
    ValueUnavailable,
    BudgetExceeded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorScanUnitColumn {
    field_ordinal: u32,
    data_type: ConnectorScalarType,
    nullable: bool,
}

impl ConnectorScanUnitColumn {
    pub const fn new(field_ordinal: u32, data_type: ConnectorScalarType, nullable: bool) -> Self {
        Self {
            field_ordinal,
            data_type,
            nullable,
        }
    }

    pub const fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub const fn data_type(&self) -> ConnectorScalarType {
        self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorScanUnitColumnDomain {
    Range {
        inclusive_min: ConnectorScalarValue,
        inclusive_max: ConnectorScalarValue,
        null_count: u64,
    },
    AllNull {
        null_count: u64,
    },
}

impl ConnectorScanUnitColumnDomain {
    pub fn try_range(
        column: ConnectorScanUnitColumn,
        inclusive_min: ConnectorScalarValue,
        inclusive_max: ConnectorScalarValue,
        null_count: u64,
        physical_row_count: u64,
    ) -> Result<ConnectorScanUnitColumnFacts, ConnectorError> {
        if inclusive_min.data_type() != column.data_type
            || inclusive_max.data_type() != column.data_type
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector scan-unit range bounds differ from the frozen column type",
            ));
        }
        if inclusive_min.compare_same_type(&inclusive_max) != Some(Ordering::Less)
            && inclusive_min.compare_same_type(&inclusive_max) != Some(Ordering::Equal)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector scan-unit range lower bound exceeds upper bound",
            ));
        }
        if !column.nullable && null_count != 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "non-nullable connector scan-unit column reports nulls",
            ));
        }
        if null_count >= physical_row_count {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector scan-unit range requires at least one non-null physical row",
            ));
        }
        if exceeds_variable_bound(&inclusive_min) || exceeds_variable_bound(&inclusive_max) {
            return Ok(ConnectorScanUnitColumnFacts::Missing {
                column,
                reason: ConnectorScanUnitFactsMissingReason::BudgetExceeded,
            });
        }
        Ok(ConnectorScanUnitColumnFacts::Available {
            column,
            domain: Self::Range {
                inclusive_min,
                inclusive_max,
                null_count,
            },
        })
    }

    pub fn try_all_null(
        column: ConnectorScanUnitColumn,
        null_count: u64,
        physical_row_count: u64,
    ) -> Result<ConnectorScanUnitColumnFacts, ConnectorError> {
        if !column.nullable || physical_row_count == 0 || null_count != physical_row_count {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector scan-unit all-null domain violates the frozen column contract",
            ));
        }
        Ok(ConnectorScanUnitColumnFacts::Available {
            column,
            domain: Self::AllNull { null_count },
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorScanUnitColumnFacts {
    Available {
        column: ConnectorScanUnitColumn,
        domain: ConnectorScanUnitColumnDomain,
    },
    Missing {
        column: ConnectorScanUnitColumn,
        reason: ConnectorScanUnitFactsMissingReason,
    },
}

impl ConnectorScanUnitColumnFacts {
    pub fn missing(
        column: ConnectorScanUnitColumn,
        reason: ConnectorScanUnitFactsMissingReason,
    ) -> Self {
        Self::Missing { column, reason }
    }

    pub fn column(&self) -> &ConnectorScanUnitColumn {
        match self {
            Self::Available { column, .. } | Self::Missing { column, .. } => column,
        }
    }

    pub fn domain(&self) -> Option<&ConnectorScanUnitColumnDomain> {
        match self {
            Self::Available { domain, .. } => Some(domain),
            Self::Missing { .. } => None,
        }
    }

    pub fn missing_reason(&self) -> Option<ConnectorScanUnitFactsMissingReason> {
        match self {
            Self::Available { .. } => None,
            Self::Missing { reason, .. } => Some(*reason),
        }
    }

    fn canonical_bytes(&self) -> usize {
        let payload = match self {
            Self::Available {
                domain:
                    ConnectorScanUnitColumnDomain::Range {
                        inclusive_min,
                        inclusive_max,
                        ..
                    },
                ..
            } => inclusive_min
                .payload_bytes()
                .saturating_add(inclusive_max.payload_bytes())
                .saturating_add(8),
            Self::Available {
                domain: ConnectorScanUnitColumnDomain::AllNull { .. },
                ..
            } => 8,
            Self::Missing { .. } => 1,
        };
        FACTS_COLUMN_FIXED_BYTES.saturating_add(payload)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorAvailableScanUnitDomainFacts {
    physical_row_count: u64,
    evidence: ConnectorScanUnitFactsEvidence,
    columns: Vec<ConnectorScanUnitColumnFacts>,
}

impl ConnectorAvailableScanUnitDomainFacts {
    pub fn physical_row_count(&self) -> u64 {
        self.physical_row_count
    }

    pub const fn evidence(&self) -> ConnectorScanUnitFactsEvidence {
        self.evidence
    }

    pub fn columns(&self) -> &[ConnectorScanUnitColumnFacts] {
        &self.columns
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorScanUnitDomainFacts {
    Available(ConnectorAvailableScanUnitDomainFacts),
    Missing(ConnectorScanUnitFactsMissingReason),
}

impl ConnectorScanUnitDomainFacts {
    pub fn available(
        physical_row_count: u64,
        evidence: ConnectorScanUnitFactsEvidence,
        columns: Vec<ConnectorScanUnitColumnFacts>,
    ) -> Result<Self, ConnectorError> {
        if columns.len() > MAX_CONNECTOR_SCAN_UNIT_FACT_COLUMNS {
            return Ok(Self::Missing(
                ConnectorScanUnitFactsMissingReason::BudgetExceeded,
            ));
        }
        let mut previous = None;
        let mut bytes = FACTS_UNIT_FIXED_BYTES;
        for facts in &columns {
            let ordinal = facts.column().field_ordinal();
            if previous.is_some_and(|previous| previous >= ordinal) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector scan-unit fact columns must be strictly ordered and unique",
                ));
            }
            previous = Some(ordinal);
            bytes = bytes.saturating_add(facts.canonical_bytes());
            if bytes > MAX_CONNECTOR_SCAN_UNIT_FACT_PAYLOAD_BYTES {
                return Ok(Self::Missing(
                    ConnectorScanUnitFactsMissingReason::BudgetExceeded,
                ));
            }
        }
        Ok(Self::Available(ConnectorAvailableScanUnitDomainFacts {
            physical_row_count,
            evidence,
            columns,
        }))
    }

    pub const fn missing(reason: ConnectorScanUnitFactsMissingReason) -> Self {
        Self::Missing(reason)
    }

    pub fn available_facts(&self) -> Option<&ConnectorAvailableScanUnitDomainFacts> {
        match self {
            Self::Available(facts) => Some(facts),
            Self::Missing(_) => None,
        }
    }

    pub fn missing_reason(&self) -> Option<ConnectorScanUnitFactsMissingReason> {
        match self {
            Self::Available(_) => None,
            Self::Missing(reason) => Some(*reason),
        }
    }

    pub fn summary(&self) -> ConnectorScanUnitFactsSummary {
        match self {
            Self::Missing(_) => ConnectorScanUnitFactsSummary {
                missing_units: 1,
                ..ConnectorScanUnitFactsSummary::default()
            },
            Self::Available(facts) => {
                let mut summary = ConnectorScanUnitFactsSummary::default();
                match facts.evidence {
                    ConnectorScanUnitFactsEvidence::Exact => summary.exact_units = 1,
                    ConnectorScanUnitFactsEvidence::Conservative => summary.conservative_units = 1,
                }
                for column in &facts.columns {
                    match column {
                        ConnectorScanUnitColumnFacts::Available { .. } => {
                            summary.available_columns += 1;
                        }
                        ConnectorScanUnitColumnFacts::Missing { .. } => {
                            summary.missing_columns += 1;
                        }
                    }
                }
                summary
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectorScanUnitFactsSummary {
    exact_units: u64,
    conservative_units: u64,
    missing_units: u64,
    available_columns: u64,
    missing_columns: u64,
}

impl ConnectorScanUnitFactsSummary {
    pub const fn exact_units(&self) -> u64 {
        self.exact_units
    }

    pub const fn conservative_units(&self) -> u64 {
        self.conservative_units
    }

    pub const fn missing_units(&self) -> u64 {
        self.missing_units
    }

    pub const fn available_columns(&self) -> u64 {
        self.available_columns
    }

    pub const fn missing_columns(&self) -> u64 {
        self.missing_columns
    }

    pub fn combine(&mut self, other: Self) {
        self.exact_units = self.exact_units.saturating_add(other.exact_units);
        self.conservative_units = self
            .conservative_units
            .saturating_add(other.conservative_units);
        self.missing_units = self.missing_units.saturating_add(other.missing_units);
        self.available_columns = self
            .available_columns
            .saturating_add(other.available_columns);
        self.missing_columns = self.missing_columns.saturating_add(other.missing_columns);
    }
}

fn exceeds_variable_bound(value: &ConnectorScalarValue) -> bool {
    value
        .variable_payload_bytes()
        .is_some_and(|bytes| bytes > MAX_CONNECTOR_SCAN_UNIT_FACT_VARIABLE_VALUE_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(
        ordinal: u32,
        data_type: ConnectorScalarType,
        nullable: bool,
    ) -> ConnectorScanUnitColumn {
        ConnectorScanUnitColumn::new(ordinal, data_type, nullable)
    }

    #[test]
    fn available_range_all_null_and_summary_are_immutable() {
        let range = ConnectorScanUnitColumnDomain::try_range(
            column(3, ConnectorScalarType::Int32, true),
            ConnectorScalarValue::Int32(4),
            ConnectorScalarValue::Int32(9),
            2,
            10,
        )
        .expect("valid range");
        let all_null = ConnectorScanUnitColumnDomain::try_all_null(
            column(7, ConnectorScalarType::Utf8, true),
            10,
            10,
        )
        .expect("valid all-null domain");
        let facts = ConnectorScanUnitDomainFacts::available(
            10,
            ConnectorScanUnitFactsEvidence::Exact,
            vec![range, all_null],
        )
        .expect("available facts");

        let available = facts.available_facts().expect("available");
        assert_eq!(available.physical_row_count(), 10);
        assert_eq!(available.columns().len(), 2);
        assert_eq!(facts.summary().exact_units(), 1);
        assert_eq!(facts.summary().available_columns(), 2);
        assert_eq!(facts.summary().missing_columns(), 0);
    }

    #[test]
    fn invalid_range_null_and_column_order_are_errors() {
        let error = ConnectorScanUnitColumnDomain::try_range(
            column(1, ConnectorScalarType::Int32, false),
            ConnectorScalarValue::Int32(9),
            ConnectorScalarValue::Int32(4),
            0,
            10,
        )
        .expect_err("reversed range");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);

        let error = ConnectorScanUnitColumnDomain::try_all_null(
            column(1, ConnectorScalarType::Int32, false),
            10,
            10,
        )
        .expect_err("non-nullable all-null");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);

        let one = ConnectorScanUnitColumnFacts::missing(
            column(2, ConnectorScalarType::Int32, true),
            ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
        );
        let two = ConnectorScanUnitColumnFacts::missing(
            column(1, ConnectorScalarType::Int32, true),
            ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
        );
        let error = ConnectorScanUnitDomainFacts::available(
            1,
            ConnectorScanUnitFactsEvidence::Exact,
            vec![one, two],
        )
        .expect_err("unordered columns");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn legal_budget_overflow_becomes_typed_missing() {
        let oversized = ConnectorScanUnitColumnDomain::try_range(
            column(0, ConnectorScalarType::Binary, true),
            ConnectorScalarValue::Binary(vec![
                0;
                MAX_CONNECTOR_SCAN_UNIT_FACT_VARIABLE_VALUE_BYTES + 1
            ]),
            ConnectorScalarValue::Binary(vec![
                1;
                MAX_CONNECTOR_SCAN_UNIT_FACT_VARIABLE_VALUE_BYTES + 1
            ]),
            0,
            1,
        )
        .expect("legal oversized values become missing");
        assert_eq!(
            oversized.missing_reason(),
            Some(ConnectorScanUnitFactsMissingReason::BudgetExceeded)
        );

        let columns = (0..MAX_CONNECTOR_SCAN_UNIT_FACT_COLUMNS)
            .map(|ordinal| {
                ConnectorScanUnitColumnDomain::try_range(
                    column(ordinal as u32, ConnectorScalarType::Utf8, false),
                    ConnectorScalarValue::Utf8("a".repeat(600)),
                    ConnectorScalarValue::Utf8("z".repeat(600)),
                    0,
                    1,
                )
                .expect("bounded column")
            })
            .collect();
        let facts = ConnectorScanUnitDomainFacts::available(
            1,
            ConnectorScanUnitFactsEvidence::Conservative,
            columns,
        )
        .expect("aggregate budget maps to missing");
        assert_eq!(
            facts.missing_reason(),
            Some(ConnectorScanUnitFactsMissingReason::BudgetExceeded)
        );
    }
}
