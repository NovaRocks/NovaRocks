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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};

use super::{DataFileFact, DeleteFact, DeleteKind};

pub const POSITION_FILE_PATH_FIELD_ID: i32 = i32::MAX - 101;

/// Missing counts remain missing. Bounds must already be decoded using the
/// manifest field ID/type. Unknown or incompatible bounds are kept as unknown,
/// never reinterpreted as an unrelated primitive with the same byte width.
#[derive(Clone, Debug, PartialEq)]
pub struct FieldMetrics {
    pub resolved_type: PrimitiveType,
    pub value_count: Option<u64>,
    pub null_count: Option<u64>,
    pub nan_count: Option<u64>,
    pub lower_bound: Option<Datum>,
    pub upper_bound: Option<Datum>,
}

impl FieldMetrics {
    pub fn unknown(resolved_type: PrimitiveType) -> Self {
        Self {
            resolved_type,
            value_count: None,
            null_count: None,
            nan_count: None,
            lower_bound: None,
            upper_bound: None,
        }
    }

    /// Only format-legal widening is allowed. Bounds may be outward-truncated:
    /// comparing the inclusive outer intervals is conservative for that case.
    pub fn bind_bounds(mut self) -> Self {
        self.lower_bound = self
            .lower_bound
            .and_then(|bound| promote_bound(bound, &self.resolved_type));
        self.upper_bound = self
            .upper_bound
            .and_then(|bound| promote_bound(bound, &self.resolved_type));
        self
    }

    fn coherent_counts(&self) -> bool {
        if let Some(value_count) = self.value_count {
            if self.null_count.is_some_and(|n| n > value_count)
                || self.nan_count.is_some_and(|n| n > value_count)
            {
                return false;
            }
            if let (Some(null), Some(nan)) = (self.null_count, self.nan_count) {
                if null.checked_add(nan).is_none_or(|sum| sum > value_count) {
                    return false;
                }
            }
        }
        true
    }

    fn float_type(&self) -> bool {
        matches!(
            self.resolved_type,
            PrimitiveType::Float | PrimitiveType::Double
        )
    }
    fn can_have_null(&self) -> bool {
        self.value_count != Some(0) && self.null_count != Some(0)
    }
    fn can_have_nan(&self) -> bool {
        self.float_type()
            && self.value_count != Some(0)
            && self.nan_count != Some(0)
            && !matches!((self.value_count, self.null_count), (Some(v), Some(n)) if v == n)
    }
    fn can_have_ordinary_value(&self) -> bool {
        let Some(values) = self.value_count else {
            return true;
        };
        let nulls = self.null_count.unwrap_or(0);
        let nans = if self.float_type() {
            self.nan_count.unwrap_or(0)
        } else {
            0
        };
        values > nulls.saturating_add(nans)
    }

    fn interval(&self) -> Option<(Datum, Datum)> {
        let lower = promote_bound(self.lower_bound.clone()?, &self.resolved_type)?;
        let upper = promote_bound(self.upper_bound.clone()?, &self.resolved_type)?;
        if lower.is_nan()
            || upper.is_nan()
            || !matches!(
                lower.partial_cmp(&upper),
                Some(Ordering::Less | Ordering::Equal)
            )
        {
            return None;
        }
        Some((lower, upper))
    }

    /// False is a proof that neither NULL, NaN nor an ordinary value can match.
    /// True includes unknown. Incorrect writer statistics are outside this
    /// proof's premise; no remote content reads are performed to validate them.
    pub fn may_overlap(&self, other: &Self) -> bool {
        if self.resolved_type != other.resolved_type
            || !self.coherent_counts()
            || !other.coherent_counts()
        {
            return true;
        }
        if self.can_have_null() && other.can_have_null() {
            return true;
        }
        if self.can_have_nan() && other.can_have_nan() {
            return true;
        }
        if !self.can_have_ordinary_value() || !other.can_have_ordinary_value() {
            return false;
        }
        let (Some((lower, upper)), Some((other_lower, other_upper))) =
            (self.interval(), other.interval())
        else {
            return true;
        };
        !matches!(upper.partial_cmp(&other_lower), Some(Ordering::Less))
            && !matches!(other_upper.partial_cmp(&lower), Some(Ordering::Less))
    }
}

fn promote_bound(bound: Datum, target: &PrimitiveType) -> Option<Datum> {
    let source = bound.data_type();
    let legal = source == target
        || matches!(
            (source, target),
            (PrimitiveType::Int, PrimitiveType::Long)
                | (PrimitiveType::Float, PrimitiveType::Double)
        )
        || matches!((source, target),
            (PrimitiveType::Decimal { precision: p, scale: s }, PrimitiveType::Decimal { precision: q, scale: t }) if p <= q && s == t);
    if !legal {
        return None;
    }
    Datum::try_from_bytes(&bound.to_bytes().ok()?, target.clone()).ok()
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FileMetrics {
    fields: BTreeMap<i32, FieldMetrics>,
}

impl FileMetrics {
    pub fn new(fields: BTreeMap<i32, FieldMetrics>) -> Self {
        Self {
            fields: fields
                .into_iter()
                .map(|(id, metrics)| (id, metrics.bind_bounds()))
                .collect(),
        }
    }
    pub fn fields(&self) -> &BTreeMap<i32, FieldMetrics> {
        &self.fields
    }
    pub fn get(&self, field_id: i32) -> Option<&FieldMetrics> {
        self.fields.get(&field_id)
    }

    /// file_path is a required format field. Equal reliable outer bounds prove
    /// a unique target even when optional null/value counts are absent.
    pub fn exact_position_target(&self) -> Option<Arc<str>> {
        let metrics = self.get(POSITION_FILE_PATH_FIELD_ID)?;
        if metrics.resolved_type != PrimitiveType::String || !metrics.coherent_counts() {
            return None;
        }
        let (lower, upper) = metrics.interval()?;
        match (lower.literal(), upper.literal()) {
            (PrimitiveLiteral::String(a), PrimitiveLiteral::String(b))
                if a == b && !a.is_empty() =>
            {
                Some(Arc::from(a.as_str()))
            }
            _ => None,
        }
    }

    fn may_contain_path(&self, path: &str) -> bool {
        let Some(metrics) = self.get(POSITION_FILE_PATH_FIELD_ID) else {
            return true;
        };
        if metrics.resolved_type != PrimitiveType::String || !metrics.coherent_counts() {
            return true;
        }
        let Some((lower, upper)) = metrics.interval() else {
            return true;
        };
        let value = Datum::string(path);
        !matches!(value.partial_cmp(&lower), Some(Ordering::Less))
            && !matches!(value.partial_cmp(&upper), Some(Ordering::Greater))
    }
}

/// The number of actual field comparisons is returned for cost receipts.
pub(crate) fn delete_may_match(data: &DataFileFact, delete: &DeleteFact) -> (bool, usize) {
    match delete.kind() {
        DeleteKind::DeletionVector { .. } => (true, 0),
        DeleteKind::Position {
            exact_target: Some(_),
        } => (true, 0),
        DeleteKind::Position { exact_target: None } => {
            (delete.metrics().may_contain_path(data.path()), 1)
        }
        DeleteKind::Equality(group) => {
            let mut comparisons = 0;
            for (id, resolved_type) in group.fields() {
                let (Some(data_metrics), Some(delete_metrics)) =
                    (data.metrics.get(*id), delete.metrics().get(*id))
                else {
                    continue;
                };
                if &data_metrics.resolved_type != resolved_type
                    || &delete_metrics.resolved_type != resolved_type
                {
                    continue;
                }
                comparisons += 1;
                if !data_metrics.may_overlap(delete_metrics) {
                    return (false, comparisons);
                }
            }
            (true, comparisons)
        }
    }
}
