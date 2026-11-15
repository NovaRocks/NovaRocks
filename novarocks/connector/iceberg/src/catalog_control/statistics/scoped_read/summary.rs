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

//! Raw metadata facts for SHOW STATS; optimizer statistics remain independent.
use super::*;
use crate::iceberg::spec::{Literal, PrimitiveLiteral, PrimitiveType, TableMetadata, Transform};
use novarocks_spi::connector::{
    StatisticsSummaryColumn, StatisticsSummaryColumnType as Kind, TableStatisticsSummary,
};

pub(super) fn project<F: StatisticsFileFacts>(
    metadata: &TableMetadata,
    declaration: &ConnectorTableMetadata,
    files: &[F],
    scope: &CaptureScope<'_>,
) -> Result<TableStatisticsSummary, ConnectorError> {
    let row_count = physical_row_count(files, || scope.active().map_err(checked_read))?;
    let schema = metadata.current_schema();
    let wanted = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();
    let ndvs = match metadata.current_snapshot_id() {
        Some(snapshot) => crate::statistics_ancestry::resolve_summary_ndv_from_metadata(
            metadata,
            snapshot,
            &wanted,
            || scope.active().map_err(checked_read),
        )?,
        None => HashMap::new(),
    };
    let mut columns = Vec::new();
    for (ordinal, field) in schema.as_struct().fields().iter().enumerate() {
        scope.active().map_err(checked_read)?;
        let declared = declaration
            .schema()
            .fields()
            .get(ordinal)
            .ok_or_else(|| corrupt("statistics summary schema arity differs"))?;
        if declared
            .metadata()
            .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            continue;
        }
        let value_type = declaration
            .execution_schema()
            .bindings()
            .get(ordinal)
            .ok_or_else(|| corrupt("statistics summary lacks its exact value declaration"))?
            .clone();
        let column_type = column_type(&field.field_type);
        let (null_count, column_size, lower_bound, upper_bound) = if column_type == Kind::Complex {
            (None, None, None, None)
        } else {
            column_facts(metadata, field, files, || {
                scope.active().map_err(checked_read)
            })?
        };
        columns.push(StatisticsSummaryColumn {
            name: Arc::from(field.name.as_str()),
            value_type,
            column_type,
            null_count,
            column_size,
            lower_bound,
            upper_bound,
            ndv: if column_type == Kind::Complex {
                None
            } else {
                ndvs.get(&field.id).copied()
            },
        });
    }
    Ok(TableStatisticsSummary { row_count, columns })
}

pub(super) fn column_type(ty: &Type) -> Kind {
    match ty {
        Type::Primitive(primitive) => match primitive {
            PrimitiveType::Boolean => Kind::Boolean,
            PrimitiveType::Int | PrimitiveType::Long => Kind::Integer,
            PrimitiveType::Float => Kind::Real,
            PrimitiveType::Double => Kind::Double,
            PrimitiveType::Decimal { scale, .. } => Kind::Decimal { scale: *scale },
            PrimitiveType::Date => Kind::Date,
            PrimitiveType::Timestamp => Kind::Timestamp {
                precision: 6,
                with_time_zone: false,
            },
            PrimitiveType::TimestampNs => Kind::Timestamp {
                precision: 9,
                with_time_zone: false,
            },
            PrimitiveType::Timestamptz => Kind::Timestamp {
                precision: 6,
                with_time_zone: true,
            },
            PrimitiveType::TimestamptzNs => Kind::Timestamp {
                precision: 9,
                with_time_zone: true,
            },
            PrimitiveType::String => Kind::Varchar,
            PrimitiveType::Binary => Kind::Varbinary,
            PrimitiveType::Fixed(width) => Kind::FixedBinary { width: *width },
            PrimitiveType::Time => Kind::Time,
            PrimitiveType::Uuid => Kind::Uuid,
        },
        _ => Kind::Complex,
    }
}

pub(super) fn physical_row_count<F: StatisticsFileFacts>(
    files: &[F],
    mut active: impl FnMut() -> Result<(), ConnectorError>,
) -> Result<u64, ConnectorError> {
    files.iter().try_fold(0u64, |sum, file| {
        active()?;
        let count = count(file.record_count())?
            .ok_or_else(|| corrupt("Iceberg data file is missing its record count"))?;
        sum.checked_add(count)
            .ok_or_else(|| corrupt("Iceberg statistics row count overflow"))
    })
}

fn count(value: Option<i64>) -> Result<Option<u64>, ConnectorError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| corrupt("Iceberg statistics contain a negative count"))
        })
        .transpose()
}
fn add(sum: &mut Option<u64>, value: Option<u64>) -> Result<(), ConnectorError> {
    if let Some(value) = value {
        *sum = Some(
            sum.unwrap_or(0)
                .checked_add(value)
                .ok_or_else(|| corrupt("Iceberg column statistics count overflow"))?,
        );
    }
    Ok(())
}
pub(super) type ColumnFacts = (Option<u64>, Option<u64>, Option<f64>, Option<f64>);
pub(super) fn column_facts<F: StatisticsFileFacts>(
    metadata: &TableMetadata,
    field: &crate::iceberg::spec::NestedField,
    files: &[F],
    mut active: impl FnMut() -> Result<(), ConnectorError>,
) -> Result<ColumnFacts, ConnectorError> {
    let Type::Primitive(primitive) = field.field_type.as_ref() else {
        return Ok((None, None, None, None));
    };
    let mut nulls = None;
    let mut size = None;
    let mut lower = None::<f64>;
    let mut upper = None::<f64>;
    let mut missing_lower = false;
    let mut missing_upper = false;
    for file in files {
        active()?;
        let rows = count(file.record_count())?
            .ok_or_else(|| corrupt("Iceberg data file is missing its record count"))?;
        let stats = column_stats(file, &field.name);
        let identity = identity_value(metadata, file, field.id)?;
        let file_nulls = count(stats.and_then(|stats| stats.null_count))?.or_else(|| {
            identity
                .is_some_and(|value| value.is_none())
                .then_some(rows)
        });
        add(&mut nulls, file_nulls)?;
        add(&mut size, count(stats.and_then(|stats| stats.column_size))?)?;
        if file_nulls == Some(rows) {
            continue;
        }
        let identity_bound = identity.flatten().and_then(|literal| match literal {
            Literal::Primitive(value) => primitive_bound(
                &crate::manifest::partition_types::resolved_partition_primitive(value, primitive),
                primitive,
            ),
            _ => None,
        });
        let file_lower = bound(
            stats.and_then(|stats| stats.lower_bound.as_deref()),
            primitive,
        )?
        .or(identity_bound);
        let file_upper = bound(
            stats.and_then(|stats| stats.upper_bound.as_deref()),
            primitive,
        )?
        .or(identity_bound);
        match file_lower {
            Some(value) => lower = Some(lower.map_or(value, |current| current.min(value))),
            None => missing_lower = true,
        }
        match file_upper {
            Some(value) => upper = Some(upper.map_or(value, |current| current.max(value))),
            None => missing_upper = true,
        }
    }
    Ok((
        nulls,
        size,
        (!missing_lower).then_some(lower).flatten(),
        (!missing_upper).then_some(upper).flatten(),
    ))
}

fn identity_value<'a, F: StatisticsFileFacts>(
    metadata: &TableMetadata,
    file: &'a F,
    field_id: i32,
) -> Result<Option<Option<&'a Literal>>, ConnectorError> {
    let Some(spec_id) = file.partition_spec_id() else {
        return Ok(None);
    };
    let spec = metadata
        .partition_spec_by_id(spec_id)
        .ok_or_else(|| corrupt("statistics file references an absent partition spec"))?;
    let Some(ordinal) = spec
        .fields()
        .iter()
        .position(|field| field.source_id == field_id && field.transform == Transform::Identity)
    else {
        return Ok(None);
    };
    let partition = file
        .partition_values()
        .ok_or_else(|| corrupt("statistics file is missing its declared partition values"))?;
    partition
        .fields()
        .get(ordinal)
        .map(|value| Some(value.as_ref()))
        .ok_or_else(|| corrupt("statistics partition tuple arity differs from its spec"))
}

pub(super) fn bound(
    bytes: Option<&[u8]>,
    primitive: &PrimitiveType,
) -> Result<Option<f64>, ConnectorError> {
    if matches!(
        primitive,
        PrimitiveType::String
            | PrimitiveType::Binary
            | PrimitiveType::Fixed(_)
            | PrimitiveType::Uuid
            | PrimitiveType::Time
    ) {
        return Ok(None);
    }
    bytes
        .map(|bytes| {
            let value = crate::iceberg::spec::Datum::try_from_bytes(bytes, primitive.clone())
                .map_err(|error| corrupt(error.to_string()))?;
            Ok(primitive_bound(value.literal(), primitive))
        })
        .transpose()
        .map(Option::flatten)
}
pub(super) fn primitive_bound(value: &PrimitiveLiteral, primitive: &PrimitiveType) -> Option<f64> {
    let value = match (value, primitive) {
        (PrimitiveLiteral::Boolean(value), PrimitiveType::Boolean) => f64::from(u8::from(*value)),
        (PrimitiveLiteral::Int(value), PrimitiveType::Int | PrimitiveType::Date) => {
            f64::from(*value)
        }
        (PrimitiveLiteral::Long(value), PrimitiveType::Long | PrimitiveType::Timestamp) => {
            *value as f64
        }
        (PrimitiveLiteral::Long(value), PrimitiveType::TimestampNs) => {
            value.div_euclid(1_000) as f64
        }
        (PrimitiveLiteral::Long(value), PrimitiveType::Timestamptz) => {
            value.div_euclid(1_000) as f64
        }
        (PrimitiveLiteral::Long(value), PrimitiveType::TimestamptzNs) => {
            value.div_euclid(1_000_000) as f64
        }
        (PrimitiveLiteral::Float(value), PrimitiveType::Float) => f64::from(value.0),
        (PrimitiveLiteral::Double(value), PrimitiveType::Double) => value.0,
        (PrimitiveLiteral::Int128(value), PrimitiveType::Decimal { scale, .. }) => {
            format!("{value}e-{scale}").parse().ok()?
        }
        _ => return None,
    };
    value.is_finite().then_some(value)
}
