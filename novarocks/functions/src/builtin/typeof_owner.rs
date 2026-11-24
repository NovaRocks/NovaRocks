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

use std::{fmt::Write, sync::Arc};

use arrow_array::builder::StringBuilder;
use arrow_schema::DataType;
use novarocks_type_contract::{CharacterKind, TemporalType, ValueKind, ValueTypeRef};

use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    aggregate_scalar::checked_utf8_output_offset, kernel_control::invalid,
    kernel_input::EvaluationCheckpoints,
};

pub(super) fn evaluate<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let [FunctionArgumentType::Value(value)] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("typeof requires one declared value argument"));
    };
    let selection = input.selection();
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let mut name = String::new();
        append_type_name(value.value_type(), &mut name, &mut work)?;
        let bytes = name
            .len()
            .checked_mul(selection.len())
            .ok_or(KernelFailure::ResourceExhausted)?;
        checked_utf8_output_offset(0, bytes)?;
        work.flush()?;
        let mut values = StringBuilder::with_capacity(selection.len(), bytes);
        for _ in selection.iter() {
            work.step()?;
            values.append_value(&name);
        }
        work.flush()?;
        SelectedValues::try_new_observed(
            selection,
            &DataType::Utf8,
            Arc::new(values.finish()),
            Box::default(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

fn append_type_name(
    declaration: &ValueTypeRef,
    output: &mut String,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step()?;
    match declaration.kind() {
        ValueKind::Scalar(carrier) => match carrier {
            DataType::Null => output.push_str("unknown"),
            DataType::Boolean => output.push_str("boolean"),
            DataType::Int8 => output.push_str("tinyint"),
            DataType::Int16 => output.push_str("smallint"),
            DataType::Int32 => output.push_str("integer"),
            DataType::Int64 => output.push_str("bigint"),
            DataType::Float32 => output.push_str("real"),
            DataType::Float64 => output.push_str("double"),
            DataType::Date32 => output.push_str("date"),
            DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                write!(output, "decimal({precision},{scale})").expect("writing to String");
            }
            other => output.push_str(&other.to_string().to_ascii_lowercase()),
        },
        ValueKind::String => output.push_str("varchar"),
        ValueKind::BoundedCharacter(character) => {
            let kind = match character.kind() {
                CharacterKind::Char => "char",
                CharacterKind::Varchar => "varchar",
            };
            write!(output, "{kind}({})", character.length()).expect("writing to String");
        }
        ValueKind::Binary => output.push_str("varbinary"),
        ValueKind::Nominal(logical) => output.push_str(
            logical
                .metadata_value()
                .ok_or_else(|| invalid("typeof received a nominal Physical type"))?,
        ),
        ValueKind::Temporal(temporal) => output.push_str(&temporal_typeof_name(*temporal)),
        ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => {
            output.push_str("array(");
            append_type_name(item, output, work)?;
            output.push(')');
        }
        ValueKind::Struct(fields) => {
            output.push_str("row(");
            for (ordinal, field) in fields.iter().enumerate() {
                if ordinal > 0 {
                    output.push_str(", ");
                }
                if !field.name().is_empty() {
                    output.push('"');
                    for character in field.name().chars() {
                        work.step()?;
                        output.push(character);
                        if character == '"' {
                            output.push('"');
                        }
                    }
                    output.push_str("\" ");
                }
                append_type_name(field.value_type(), output, work)?;
            }
            output.push(')');
        }
        ValueKind::Map { key, value } => {
            output.push_str("map(");
            append_type_name(key, output, work)?;
            output.push_str(", ");
            append_type_name(value, output, work)?;
            output.push(')');
        }
        ValueKind::Union(fields) => {
            output.push_str("union(");
            for (ordinal, field) in fields.iter().enumerate() {
                if ordinal > 0 {
                    output.push_str(", ");
                }
                append_type_name(field.field.value_type(), output, work)?;
            }
            output.push(')');
        }
    }
    Ok(())
}

fn temporal_typeof_name(temporal: TemporalType) -> String {
    let precision = temporal.precision().digits();
    match temporal {
        TemporalType::Time(_) => format!("time({precision})"),
        TemporalType::TimeWithTimeZone(_) => format!("time({precision}) with time zone"),
        TemporalType::Timestamp(_) => format!("timestamp({precision})"),
        TemporalType::TimestampWithTimeZone(_) => {
            format!("timestamp({precision}) with time zone")
        }
        TemporalType::Interval(qualifier) => {
            use novarocks_type_contract::IntervalField;
            let field = |kind| match kind {
                IntervalField::Year => "year",
                IntervalField::Month => "month",
                IntervalField::Day => "day",
                IntervalField::Hour => "hour",
                IntervalField::Minute => "minute",
                IntervalField::Second => "second",
            };
            let start = qualifier.start();
            let end = qualifier.end();
            let leading = qualifier.leading_precision();
            let fractional = qualifier.fractional_precision().digits();
            let start_name = field(start);
            let end_name = field(end);
            match (start, end) {
                (IntervalField::Second, IntervalField::Second) => {
                    format!("interval second({leading}, {fractional})")
                }
                (start, end) if start == end => format!("interval {start_name}({leading})"),
                (_, IntervalField::Second) => {
                    format!("interval {start_name}({leading}) to second({fractional})")
                }
                _ => format!("interval {start_name}({leading}) to {end_name}"),
            }
        }
    }
}
