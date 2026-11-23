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

use novarocks_type_contract::{IntervalField, IntervalQualifier, TemporalPrecision, TemporalType};

use novarocks_proto_models::value_type as wire;
use wire::temporal_type::Kind;

pub fn decode(value: &wire::TemporalType) -> Result<TemporalType, &'static str> {
    Ok(
        match value.kind.as_ref().ok_or("missing temporal type kind")? {
            Kind::TimePrecision(value) => TemporalType::Time(precision(*value)?),
            Kind::TimeWithTimeZonePrecision(value) => {
                TemporalType::TimeWithTimeZone(precision(*value)?)
            }
            Kind::TimestampPrecision(value) => TemporalType::Timestamp(precision(*value)?),
            Kind::TimestampWithTimeZonePrecision(value) => {
                TemporalType::TimestampWithTimeZone(precision(*value)?)
            }
            Kind::Interval(qualifier) => {
                let leading = qualifier
                    .leading_precision
                    .and_then(|value| u8::try_from(value).ok())
                    .ok_or("invalid interval leading precision")?;
                let fractional = qualifier
                    .fractional_precision
                    .ok_or("missing interval fractional precision")?;
                TemporalType::Interval(
                    IntervalQualifier::new(
                        field(qualifier.start_field)?,
                        field(qualifier.end_field)?,
                        leading,
                        precision(fractional)?,
                    )
                    .map_err(|_| "invalid interval qualifier")?,
                )
            }
        },
    )
}

pub fn encode(value: TemporalType) -> wire::TemporalType {
    let kind = match value {
        TemporalType::Time(value) => Kind::TimePrecision(value.digits().into()),
        TemporalType::TimeWithTimeZone(value) => {
            Kind::TimeWithTimeZonePrecision(value.digits().into())
        }
        TemporalType::Timestamp(value) => Kind::TimestampPrecision(value.digits().into()),
        TemporalType::TimestampWithTimeZone(value) => {
            Kind::TimestampWithTimeZonePrecision(value.digits().into())
        }
        TemporalType::Interval(value) => Kind::Interval(wire::IntervalQualifier {
            start_field: wire_field(value.start()) as i32,
            end_field: wire_field(value.end()) as i32,
            leading_precision: Some(value.leading_precision().into()),
            fractional_precision: Some(value.fractional_precision().digits().into()),
        }),
    };
    wire::TemporalType { kind: Some(kind) }
}

fn precision(value: u32) -> Result<TemporalPrecision, &'static str> {
    u8::try_from(value)
        .ok()
        .and_then(|value| TemporalPrecision::new(value).ok())
        .ok_or("invalid temporal fractional precision")
}

fn field(value: i32) -> Result<IntervalField, &'static str> {
    Ok(match wire::IntervalField::try_from(value) {
        Ok(wire::IntervalField::Year) => IntervalField::Year,
        Ok(wire::IntervalField::Month) => IntervalField::Month,
        Ok(wire::IntervalField::Day) => IntervalField::Day,
        Ok(wire::IntervalField::Hour) => IntervalField::Hour,
        Ok(wire::IntervalField::Minute) => IntervalField::Minute,
        Ok(wire::IntervalField::Second) => IntervalField::Second,
        _ => return Err("invalid interval field"),
    })
}

fn wire_field(value: IntervalField) -> wire::IntervalField {
    match value {
        IntervalField::Year => wire::IntervalField::Year,
        IntervalField::Month => wire::IntervalField::Month,
        IntervalField::Day => wire::IntervalField::Day,
        IntervalField::Hour => wire::IntervalField::Hour,
        IntervalField::Minute => wire::IntervalField::Minute,
        IntervalField::Second => wire::IntervalField::Second,
    }
}
