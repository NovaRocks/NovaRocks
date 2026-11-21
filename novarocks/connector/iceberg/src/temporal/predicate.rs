// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Domain, Range, ValueSet,
};

use crate::iceberg::spec::PrimitiveType;

/// Convert only comparisons whose storage-unit predicate is proved from the
/// original physical declaration. The caller retains the original residual.
pub(crate) fn project_domain(primitive: &PrimitiveType, domain: &Domain) -> Option<Domain> {
    let ConnectorValueType::Temporal(input) = domain.value_type() else {
        return None;
    };
    let stored = super::declaration(primitive)?;
    if !stored.is_lossless_widening_to(input) {
        return None;
    }
    let scalar_type = storage_type(primitive)?;
    let step = i128::from(stored.precision().picosecond_step());
    let exact = |value: &ConnectorValue| match value {
        ConnectorValue::Temporal { data_type, value } if input == *data_type => {
            super::observable_picos(input, *value).ok()
        }
        _ => None,
    };
    let value = |units| storage_value(scalar_type, i64::try_from(units).ok()?);
    let values = domain.values();
    let projected = if values.is_all() {
        ValueSet::all(scalar_type)
    } else if values.is_none() {
        ValueSet::none(scalar_type)
    } else if let Some(points) = values.discrete_values() {
        let mut projected = Vec::with_capacity(points.len());
        for point in points {
            let picos = exact(&point)?;
            if picos.rem_euclid(step) == 0 {
                projected.push(value(picos.div_euclid(step))?);
            }
        }
        ValueSet::of_values(scalar_type, projected).ok()?
    } else {
        let bound = |bound: &Bound, lower| {
            let (point, inclusive) = match bound {
                Bound::Unbounded => return Some(Bound::Unbounded),
                Bound::Inclusive(value) => (value, true),
                Bound::Exclusive(value) => (value, false),
            };
            let picos = exact(point)?;
            let floor = picos.div_euclid(step);
            let units = if lower == inclusive {
                floor + i128::from(picos.rem_euclid(step) != 0)
            } else {
                floor
            };
            let point = value(units)?;
            Some(if inclusive {
                Bound::Inclusive(point)
            } else {
                Bound::Exclusive(point)
            })
        };
        let ranges = values
            .ranges()
            .iter()
            .map(|range| {
                Range::try_new(
                    scalar_type,
                    bound(range.low(), true)?,
                    bound(range.high(), false)?,
                )
                .ok()
            })
            .collect::<Option<Vec<_>>>()?;
        ValueSet::of_ranges(scalar_type, ranges).ok()?
    };
    Some(Domain::new(projected, domain.null_allowed()))
}

fn storage_type(primitive: &PrimitiveType) -> Option<ConnectorValueType> {
    Some(match primitive {
        PrimitiveType::Time => ConnectorValueType::TimeMicros,
        PrimitiveType::Timestamp => ConnectorValueType::TimestampMicros,
        PrimitiveType::Timestamptz => ConnectorValueType::TimestampTzMicros,
        PrimitiveType::TimestampNs => ConnectorValueType::TimestampNanos,
        PrimitiveType::TimestamptzNs => ConnectorValueType::TimestampTzNanos,
        _ => return None,
    })
}

fn storage_value(kind: ConnectorValueType, units: i64) -> Option<ConnectorValue> {
    Some(match kind {
        ConnectorValueType::TimeMicros => ConnectorValue::TimeMicros(units),
        ConnectorValueType::TimestampMicros => ConnectorValue::TimestampMicros(units),
        ConnectorValueType::TimestampTzMicros => ConnectorValue::TimestampTzMicros(units),
        ConnectorValueType::TimestampNanos => ConnectorValue::TimestampNanos(units),
        ConnectorValueType::TimestampTzNanos => ConnectorValue::TimestampTzNanos(units),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{TemporalMicros, TemporalPrecision, TemporalType, TemporalValue};

    #[test]
    fn temporal_projection_preserves_open_closed_bounds_across_zero() {
        let exact = TemporalType::Timestamp(TemporalPrecision::PICOSECONDS);
        let value_type = ConnectorValueType::Temporal(exact);
        for picos in [-1_000_000, -400_000, 0, 400_000, 1_000_000] {
            let value = ConnectorValue::Temporal {
                data_type: exact,
                value: TemporalValue::Timestamp(TemporalMicros::from_picos(picos).unwrap()),
            };
            for lower in [false, true] {
                for inclusive in [false, true] {
                    let bound = if inclusive {
                        Bound::Inclusive(value.clone())
                    } else {
                        Bound::Exclusive(value.clone())
                    };
                    let (low, high) = if lower {
                        (bound, Bound::Unbounded)
                    } else {
                        (Bound::Unbounded, bound)
                    };
                    let domain = Domain::new(
                        ValueSet::of_ranges(
                            value_type,
                            vec![Range::try_new(value_type, low, high).unwrap()],
                        )
                        .unwrap(),
                        false,
                    );
                    let projected = project_domain(&PrimitiveType::Timestamp, &domain).unwrap();
                    for micros in -2..=2 {
                        let row = i128::from(micros) * 1_000_000;
                        let expected = match (lower, inclusive) {
                            (false, false) => row < picos,
                            (false, true) => row <= picos,
                            (true, false) => row > picos,
                            (true, true) => row >= picos,
                        };
                        assert_eq!(
                            projected
                                .values()
                                .contains_value(&ConnectorValue::TimestampMicros(micros))
                                .unwrap(),
                            expected
                        );
                    }
                }
            }
        }
    }
}
