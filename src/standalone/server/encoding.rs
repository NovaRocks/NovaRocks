//! Arrow → MySQL wire value conversion for the standalone MySQL server.

use std::io::{self, Write};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use mysql_common::value::Value as MySqlValue;
use opensrv_mysql::{Column, ColumnFlags, ColumnType, QueryResultWriter, ToMysqlValue};
use tokio::io::AsyncWrite;

use crate::common::util::format_mysql_container_value_with_schema;
use crate::exec::chunk::{Chunk, ChunkFieldSchema};

use super::super::engine::{QueryResult, QueryResultColumn};

#[derive(Clone, Debug, PartialEq)]
pub(super) enum StandaloneMysqlValue {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

impl ToMysqlValue for StandaloneMysqlValue {
    fn to_mysql_text<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Self::Null => None::<u8>.to_mysql_text(w),
            Self::Bytes(bytes) => bytes.to_mysql_text(w),
            Self::Int(value) => value.to_mysql_text(w),
            Self::UInt(value) => value.to_mysql_text(w),
            Self::Float(value) => value.to_mysql_text(w),
            Self::Double(value) => value.to_mysql_text(w),
            Self::Date(value) => value.to_mysql_text(w),
            Self::DateTime(value) => value.to_mysql_text(w),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_text(w),
        }
    }

    fn to_mysql_bin<W: Write>(&self, w: &mut W, c: &Column) -> io::Result<()> {
        match self {
            Self::Null => unreachable!("NULL payloads are handled by the row null bitmap"),
            Self::Bytes(bytes) => bytes.to_mysql_bin(w, c),
            Self::Int(value) => value.to_mysql_bin(w, c),
            Self::UInt(value) => value.to_mysql_bin(w, c),
            Self::Float(value) => value.to_mysql_bin(w, c),
            Self::Double(value) => value.to_mysql_bin(w, c),
            Self::Date(value) => value.to_mysql_bin(w, c),
            Self::DateTime(value) => value.to_mysql_bin(w, c),
            Self::Time {
                negative,
                days,
                hours,
                minutes,
                seconds,
                micros,
            } => MySqlValue::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
                .to_mysql_bin(w, c),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

pub(super) async fn write_query_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    let columns = result
        .columns
        .iter()
        .map(query_result_column_to_mysql_column)
        .collect::<Result<Vec<_>, _>>()
        .map_err(invalid_data_error)?;

    let mut writer = results.start(columns.as_slice()).await?;
    for chunk in &result.chunks {
        for row_idx in 0..chunk.len() {
            let row =
                build_mysql_row(chunk, &result.columns, row_idx).map_err(invalid_data_error)?;
            writer.write_row(row).await?;
        }
    }
    writer.finish().await
}

pub(super) fn query_result_column_to_mysql_column(
    column: &QueryResultColumn,
) -> Result<Column, String> {
    let mut colflags = ColumnFlags::empty();
    if !column.nullable {
        colflags.insert(ColumnFlags::NOT_NULL_FLAG);
    }
    if matches!(
        column.logical_type,
        Some(crate::sql::SqlType::Decimal { .. })
    ) {
        return Ok(Column {
            table: String::new(),
            column: column.name.clone(),
            coltype: ColumnType::MYSQL_TYPE_NEWDECIMAL,
            colflags,
        });
    }
    let coltype = match column.data_type {
        DataType::Boolean => ColumnType::MYSQL_TYPE_TINY,
        DataType::Int8 | DataType::Int16 | DataType::Int32 => ColumnType::MYSQL_TYPE_LONG,
        DataType::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            colflags.insert(ColumnFlags::UNSIGNED_FLAG);
            ColumnType::MYSQL_TYPE_LONGLONG
        }
        DataType::Float32 => ColumnType::MYSQL_TYPE_FLOAT,
        DataType::Float64 => ColumnType::MYSQL_TYPE_DOUBLE,
        DataType::FixedSizeBinary(width)
            if width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            ColumnType::MYSQL_TYPE_STRING
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::List(_)
        | DataType::Map(_, _)
        | DataType::Struct(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
        DataType::Decimal128(_, _) => ColumnType::MYSQL_TYPE_NEWDECIMAL,
        DataType::Date32 => ColumnType::MYSQL_TYPE_DATE,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::MYSQL_TYPE_TIME,
        DataType::Timestamp(_, _) => ColumnType::MYSQL_TYPE_DATETIME,
        DataType::Null => ColumnType::MYSQL_TYPE_NULL,
        ref other => {
            return Err(format!(
                "standalone mysql server does not support output column type {:?}",
                other
            ));
        }
    };

    Ok(Column {
        table: String::new(),
        column: column.name.clone(),
        coltype,
        colflags,
    })
}

pub(super) fn build_mysql_row(
    chunk: &Chunk,
    columns: &[QueryResultColumn],
    row_idx: usize,
) -> Result<Vec<StandaloneMysqlValue>, String> {
    if chunk.columns().len() != columns.len() {
        return Err(format!(
            "query result column count mismatch: metadata has {}, chunk has {}",
            columns.len(),
            chunk.columns().len()
        ));
    }
    if chunk.chunk_schema().slots().len() != columns.len() {
        return Err(format!(
            "query result slot count mismatch: schema has {}, metadata has {}",
            chunk.chunk_schema().slots().len(),
            columns.len()
        ));
    }
    chunk
        .columns()
        .iter()
        .zip(chunk.chunk_schema().slots().iter())
        .zip(columns.iter())
        .map(|((column, slot), declared)| {
            array_value_to_mysql_value(column, declared, row_idx, Some(slot.field_schema()))
        })
        .collect()
}

pub(super) fn array_value_to_mysql_value(
    column: &ArrayRef,
    declared: &QueryResultColumn,
    row_idx: usize,
    field_schema: Option<&ChunkFieldSchema>,
) -> Result<StandaloneMysqlValue, String> {
    if column.is_null(row_idx) {
        return Ok(StandaloneMysqlValue::Null);
    }

    if let Some(crate::sql::SqlType::Decimal { scale, .. }) = declared.logical_type.as_ref() {
        return decimal_to_mysql_value(column, row_idx, *scale);
    }

    if matches!(declared.data_type, DataType::Date32)
        && matches!(column.data_type(), DataType::Timestamp(_, _))
    {
        return timestamp_to_date_mysql_value(column, timestamp_unit(column.data_type())?, row_idx);
    }

    let name_lower = declared.name.to_lowercase();
    if matches!(column.data_type(), DataType::Binary | DataType::LargeBinary)
        && (name_lower.starts_with("bitmap_agg(")
            || name_lower.starts_with("bitmap_union(")
            || name_lower.starts_with("hll_union(")
            || name_lower.starts_with("hll_raw_agg("))
    {
        return Ok(StandaloneMysqlValue::Null);
    }

    match column.data_type() {
        DataType::Boolean => downcast_array::<BooleanArray>(column, "BooleanArray")
            .map(|arr| StandaloneMysqlValue::Int(if arr.value(row_idx) { 1 } else { 0 })),
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| StandaloneMysqlValue::Int(i64::from(arr.value(row_idx)))),
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| StandaloneMysqlValue::Int(arr.value(row_idx))),
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| StandaloneMysqlValue::UInt(u64::from(arr.value(row_idx)))),
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| StandaloneMysqlValue::UInt(arr.value(row_idx))),
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| StandaloneMysqlValue::Float(arr.value(row_idx))),
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| StandaloneMysqlValue::Double(arr.value(row_idx))),
        DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let arr = downcast_array::<FixedSizeBinaryArray>(column, "FixedSizeBinaryArray")?;
            let value = crate::common::largeint::i128_from_be_bytes(arr.value(row_idx))?;
            Ok(StandaloneMysqlValue::Bytes(value.to_string().into_bytes()))
        }
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).as_bytes().to_vec())),
        DataType::Binary => downcast_array::<BinaryArray>(column, "BinaryArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::LargeBinary => downcast_array::<LargeBinaryArray>(column, "LargeBinaryArray")
            .map(|arr| StandaloneMysqlValue::Bytes(arr.value(row_idx).to_vec())),
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(column, "Date32Array")?;
            date32_to_mysql_value(arr.value(row_idx))
        }
        DataType::Decimal128(_, scale) => decimal128_to_mysql_value(column, row_idx, *scale),
        DataType::Time32(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Time64(unit) => time_to_mysql_value(column, *unit, row_idx),
        DataType::Timestamp(unit, _) => timestamp_to_mysql_value(column, *unit, row_idx),
        DataType::Null => Ok(StandaloneMysqlValue::Null),
        DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
            Ok(StandaloneMysqlValue::Bytes(
                format_mysql_container_value_with_schema(column, row_idx, field_schema)?
                    .into_bytes(),
            ))
        }
        other => Err(format!(
            "standalone mysql server does not support output column type {:?}",
            other
        )),
    }
}

fn decimal128_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<StandaloneMysqlValue, String> {
    let arr = downcast_array::<Decimal128Array>(column, "Decimal128Array")?;
    Ok(StandaloneMysqlValue::Bytes(
        format_decimal128_string(arr.value(row_idx), scale)?.into_bytes(),
    ))
}

fn format_decimal128_string(value: i128, scale: i8) -> Result<String, String> {
    if scale < 0 {
        return Err(format!("unsupported decimal scale: {scale}"));
    }
    let scale = u32::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let factor = 10_u128
        .checked_pow(scale)
        .ok_or_else(|| format!("unsupported decimal scale: {scale}"))?;
    let negative = value.is_negative();
    let abs = value.unsigned_abs();
    let whole = abs / factor;
    let fraction = abs % factor;
    Ok(format!(
        "{}{}.{:0width$}",
        if negative { "-" } else { "" },
        whole,
        fraction,
        width = scale as usize
    ))
}

fn decimal_to_mysql_value(
    column: &ArrayRef,
    row_idx: usize,
    scale: i8,
) -> Result<StandaloneMysqlValue, String> {
    let scale =
        usize::try_from(scale).map_err(|_| format!("unsupported decimal scale: {scale}"))?;
    let formatted = match column.data_type() {
        DataType::Int8 => downcast_array::<Int8Array>(column, "Int8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int16 => downcast_array::<Int16Array>(column, "Int16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int32 => downcast_array::<Int32Array>(column, "Int32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Int64 => downcast_array::<Int64Array>(column, "Int64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::UInt8 => downcast_array::<UInt8Array>(column, "UInt8Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt16 => downcast_array::<UInt16Array>(column, "UInt16Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt32 => downcast_array::<UInt32Array>(column, "UInt32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::UInt64 => downcast_array::<UInt64Array>(column, "UInt64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx) as f64))?,
        DataType::Float32 => downcast_array::<Float32Array>(column, "Float32Array")
            .map(|arr| format!("{:.*}", scale, f64::from(arr.value(row_idx))))?,
        DataType::Float64 => downcast_array::<Float64Array>(column, "Float64Array")
            .map(|arr| format!("{:.*}", scale, arr.value(row_idx)))?,
        DataType::Utf8 => downcast_array::<StringArray>(column, "StringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        DataType::LargeUtf8 => downcast_array::<LargeStringArray>(column, "LargeStringArray")
            .map(|arr| arr.value(row_idx).to_string())?,
        other => {
            return Err(format!(
                "standalone mysql server does not support decimal output column type {:?}",
                other
            ));
        }
    };
    Ok(StandaloneMysqlValue::Bytes(formatted.into_bytes()))
}

fn timestamp_unit(data_type: &DataType) -> Result<TimeUnit, String> {
    match data_type {
        DataType::Timestamp(unit, _) => Ok(*unit),
        other => Err(format!("expected timestamp data type, got {:?}", other)),
    }
}

fn downcast_array<'a, T: 'static>(column: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast output column to {}", expected))
}

fn date32_to_mysql_value(days: i32) -> Result<StandaloneMysqlValue, String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| format!("date32 value out of range: {days}"))?;
    Ok(StandaloneMysqlValue::Date(date))
}

fn timestamp_to_naive_datetime(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<NaiveDateTime, String> {
    let raw = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<TimestampSecondArray>(column, "TimestampSecondArray")?
                    .value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<TimestampMillisecondArray>(column, "TimestampMillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<TimestampMicrosecondArray>(column, "TimestampMicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<TimestampNanosecondArray>(column, "TimestampNanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };
    let secs = raw.div_euclid(1_000_000);
    let micros = raw.rem_euclid(1_000_000);
    let secs = i64::try_from(secs).map_err(|_| format!("timestamp value out of range: {raw}"))?;
    let micros =
        u32::try_from(micros).map_err(|_| format!("timestamp micros out of range: {raw}"))?;
    let dt = chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1_000)
        .ok_or_else(|| format!("timestamp value out of range: {raw}"))?;
    Ok(dt.naive_utc())
}

fn timestamp_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    Ok(StandaloneMysqlValue::DateTime(timestamp_to_naive_datetime(
        column, unit, row_idx,
    )?))
}

fn timestamp_to_date_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    Ok(StandaloneMysqlValue::Date(
        timestamp_to_naive_datetime(column, unit, row_idx)?.date(),
    ))
}

fn time_to_mysql_value(
    column: &ArrayRef,
    unit: TimeUnit,
    row_idx: usize,
) -> Result<StandaloneMysqlValue, String> {
    let micros = match unit {
        TimeUnit::Second => {
            i128::from(
                downcast_array::<Time32SecondArray>(column, "Time32SecondArray")?.value(row_idx),
            ) * 1_000_000
        }
        TimeUnit::Millisecond => {
            i128::from(
                downcast_array::<Time32MillisecondArray>(column, "Time32MillisecondArray")?
                    .value(row_idx),
            ) * 1_000
        }
        TimeUnit::Microsecond => i128::from(
            downcast_array::<Time64MicrosecondArray>(column, "Time64MicrosecondArray")?
                .value(row_idx),
        ),
        TimeUnit::Nanosecond => {
            i128::from(
                downcast_array::<Time64NanosecondArray>(column, "Time64NanosecondArray")?
                    .value(row_idx),
            ) / 1_000
        }
    };

    let total_seconds = micros.div_euclid(1_000_000);
    let microseconds = micros.rem_euclid(1_000_000) as u32;
    let hours = total_seconds.div_euclid(3_600);
    let minutes = total_seconds.rem_euclid(3_600).div_euclid(60);
    let seconds = total_seconds.rem_euclid(60);
    let days = hours.div_euclid(24);
    let hour_of_day = hours.rem_euclid(24);

    Ok(StandaloneMysqlValue::Time {
        negative: micros.is_negative(),
        days: u32::try_from(days.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        hours: u8::try_from(hour_of_day.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        minutes: u8::try_from(minutes.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        seconds: u8::try_from(seconds.unsigned_abs())
            .map_err(|_| format!("time value out of range: {micros}"))?,
        micros: microseconds,
    })
}

pub(super) fn invalid_data_error(err: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ListBuilder, StringBuilder, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
    use crate::types;

    fn array_json_type_desc() -> types::TTypeDesc {
        types::TTypeDesc::new(vec![
            types::TTypeNode::new(types::TTypeNodeType::ARRAY, None, None, None),
            types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                Some(types::TScalarType::new(
                    types::TPrimitiveType::JSON,
                    None,
                    None,
                    None,
                )),
                None,
                None,
            ),
        ])
    }

    #[test]
    fn declared_date_timestamp_value_serializes_without_time_component() {
        let declared = QueryResultColumn {
            name: "d".to_string(),
            data_type: DataType::Date32,
            nullable: false,
            logical_type: None,
        };
        let value = array_value_to_mysql_value(
            &(Arc::new(TimestampMicrosecondArray::from(vec![
                1_580_601_600_000_000i64,
            ])) as ArrayRef),
            &declared,
            0,
            None,
        )
        .expect("convert timestamp to DATE");

        assert_eq!(
            value,
            StandaloneMysqlValue::Date(NaiveDate::from_ymd_opt(2020, 2, 2).expect("valid date"))
        );

        let mut encoded = Vec::new();
        value
            .to_mysql_text(&mut encoded)
            .expect("encode DATE text payload");
        assert_eq!(encoded[0], 10);
        assert_eq!(&encoded[1..], b"2020-02-02");
    }

    #[test]
    fn build_mysql_row_uses_chunk_field_schema_for_array_json() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value(r#"{"2:3": null}"#);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                array.data_type().clone(),
                true,
            )])),
            vec![Arc::clone(&array)],
        )
        .expect("batch");
        let chunk = Chunk::new_with_chunk_schema(
            batch,
            Arc::new(
                ChunkSchema::try_new(vec![
                    ChunkSlotSchema::try_from_type_desc(
                        SlotId::new(1),
                        "payload",
                        true,
                        array_json_type_desc(),
                        None,
                    )
                    .expect("slot schema"),
                ])
                .expect("chunk schema"),
            ),
        );
        let columns = vec![QueryResultColumn {
            name: "payload".to_string(),
            data_type: array.data_type().clone(),
            nullable: true,
            logical_type: None,
        }];

        let row = build_mysql_row(&chunk, &columns, 0).expect("mysql row");

        assert_eq!(
            row,
            vec![StandaloneMysqlValue::Bytes(
                br#"['{"2:3": null}']"#.to_vec()
            )]
        );
    }
}
