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

//! Legacy text is retained only until G10c. Conversion accepts the old oracle.
use super::Block;
use crate::types::ResultSet;
use anyhow::{Context, Result, bail, ensure};
use chrono::{Datelike, LocalResult, NaiveDateTime, TimeZone, Timelike, Utc};
use novarocks_test_trino_client::{StatementResult, TrinoValue, TypeArgument, TypeSignature};

#[derive(Clone, Copy, Debug)]
pub struct OldRenderContext {
    pub variant_offset_seconds: i32,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct OldComparePolicy {
    pub order_sensitive: bool,
    pub float_epsilon: Option<f64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowMapping {
    pub actual_rows: Vec<usize>,
}

pub fn old_render_cell(
    ctx: &OldRenderContext,
    sig: &TypeSignature,
    value: &TrinoValue,
) -> Result<String> {
    old_value(ctx, sig, value, false)
}
fn old_value(
    ctx: &OldRenderContext,
    sig: &TypeSignature,
    value: &TrinoValue,
    nested: bool,
) -> Result<String> {
    use TrinoValue as V;
    if matches!(value, V::Null) {
        return Ok(if nested { "null" } else { "NULL" }.into());
    }
    let scalar = match (sig.raw_type.as_str(), value) {
        ("boolean", V::Boolean(v)) => if *v { "1" } else { "0" }.into(), // encoder.rs:1595
        ("tinyint" | "smallint" | "integer" | "bigint", V::Integer(v)) => v.to_string(),
        ("real", V::Real(v)) => v.to_string(), // encoder.rs:1604-1613
        ("double", V::Double(v)) => v.to_string(),
        ("varchar" | "char", V::Text(v)) => {
            return Ok(if nested { old_quote(v, '"') } else { v.clone() });
        }
        ("varbinary", V::Binary(v)) => {
            let text = String::from_utf8_lossy(v);
            return Ok(if nested {
                old_quote(&text, '"')
            } else {
                text.into_owned()
            });
        }
        ("json", V::Text(v)) => {
            return Ok(if nested {
                old_quote(v, '\'')
            } else {
                v.clone()
            });
        }
        ("decimal", V::Text(v)) => v.clone(),
        ("date", V::Text(v)) => {
            let date = if v == "-0001-11-30" {
                "0000-00-00".to_string()
            } else if nested {
                v.clone()
            } else {
                mysql_top_year(v)?
            };
            return Ok(if nested { old_quote(&date, '"') } else { date }); // encoder.rs:1484-1501
        }
        ("time" | "time with time zone" | "timestamp" | "timestamp with time zone", V::Text(v)) => {
            let text = old_temporal(sig, v, nested)?;
            return Ok(if nested { old_quote(&text, '"') } else { text }); // encoder.rs:1652-1709
        }
        ("variant", V::Variant(_)) => {
            // Inline Trino JSON loses the original Variant primitive tags and
            // serialized metadata. The old encoder used those tags for float
            // widening, decimal text, Latin-1 strings and temporal offsets;
            // reconstructing any of them from a JSON token would guess facts.
            bail!(
                "original MySQL Variant presentation cannot be recovered from Trino JSON (baseline offset {})",
                ctx.variant_offset_seconds
            );
        }
        ("array", V::Array(values)) => {
            let child = sig.type_argument(0)?;
            return Ok(format!(
                "[{}]",
                values
                    .iter()
                    .map(|v| old_value(ctx, child, v, true))
                    .collect::<Result<Vec<_>>>()?
                    .join(",")
            ));
        }
        ("map", V::Map(values)) => {
            let key = sig.type_argument(0)?;
            let child = sig.type_argument(1)?;
            let entries = values
                .iter()
                .map(|(k, v)| {
                    Ok(format!(
                        "{}:{}",
                        old_value(ctx, key, k, true)?,
                        old_value(ctx, child, v, true)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(format!("{{{}}}", entries.join(",")));
        }
        ("row", V::Row(values)) => {
            ensure!(
                values.len() == sig.arguments.len(),
                "legacy row width differs from type"
            );
            let mut fields = Vec::new();
            for (arg, v) in sig.arguments.iter().zip(values) {
                let TypeArgument::NamedType(field) = arg else {
                    bail!("legacy row field is not named");
                };
                let name = field
                    .field_name
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("legacy row field has no name"))?;
                fields.push(format!(
                    "{}:{}",
                    old_quote(&name.name, '"'),
                    old_value(ctx, &field.type_signature, v, true)?
                ));
            }
            return Ok(format!("{{{}}}", fields.join(",")));
        }
        _ => bail!(
            "legacy value does not match supported type {}",
            sig.raw_type
        ),
    };
    Ok(scalar)
}
fn old_quote(text: &str, quote: char) -> String {
    let mut out = String::new();
    out.push(quote);
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}
/// MySQL's top-level year formatter counts the sign in its minimum width,
/// unlike chrono's ISO formatter used by Trino and nested MySQL values.
fn mysql_top_year(text: &str) -> Result<String> {
    let search_from = usize::from(text.starts_with(['-', '+']));
    let end = text[search_from..]
        .find('-')
        .map(|index| search_from + index)
        .context("legacy temporal lacks a Gregorian year")?;
    let year: i32 = text[..end]
        .parse()
        .context("invalid legacy Gregorian year")?;
    Ok(format!("{year:04}{}", &text[end..]))
}
fn old_temporal(sig: &TypeSignature, text: &str, nested: bool) -> Result<String> {
    let timestamp = sig.raw_type.starts_with("timestamp");
    let zoned = sig.raw_type.ends_with("with time zone");
    let (body, zone) = if zoned {
        text.rsplit_once(' ')
            .context("zoned legacy temporal lacks zone")?
    } else {
        (text, "")
    };
    let precision = match sig.arguments.first() {
        Some(TypeArgument::Long(p)) => usize::try_from(*p)?,
        _ => 3,
    };
    ensure!(
        matches!(precision, 0 | 3 | 6 | 9),
        "legacy temporal precision is not emitted by the original encoder"
    );
    if !timestamp {
        // TIME uses micros(raw, unit), truncating nanoseconds toward zero. All
        // Trino TIME cells are nonnegative, so fraction truncation is exact.
        let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
        ensure!(
            fraction.bytes().all(|b| b.is_ascii_digit()),
            "legacy TIME fraction is invalid"
        );
        let mut micro = fraction.chars().take(6).collect::<String>();
        micro.extend(std::iter::repeat_n('0', 6 - micro.len()));
        return Ok(if micro.bytes().any(|b| b != b'0') {
            format!("{whole}.{micro}")
        } else {
            whole.to_owned()
        });
    }
    let local = NaiveDateTime::parse_from_str(body, "%Y-%m-%d %H:%M:%S%.f")
        .context("invalid legacy timestamp value")?;
    let utc = if !zoned || zone == "UTC" {
        local.and_utc()
    } else if zone.starts_with(['+', '-']) {
        let offset = zone
            .parse::<chrono::FixedOffset>()
            .context("invalid legacy timestamp zone offset")?;
        local
            .and_local_timezone(offset)
            .single()
            .context("invalid fixed-offset timestamp")?
            .with_timezone(&Utc)
    } else {
        let tz = zone
            .parse::<chrono_tz::Tz>()
            .context("unknown legacy timestamp zone")?;
        match tz.from_local_datetime(&local) {
            LocalResult::Single(dt) => dt.with_timezone(&Utc),
            LocalResult::Ambiguous(_, _) => {
                bail!("ambiguous local Trino timestamp cannot recover its original MySQL instant")
            }
            LocalResult::None => {
                bail!("local Trino timestamp is outside its zone's supported instants")
            }
        }
    };
    // TimestampUtcMicros deliberately truncates the original raw nanosecond
    // count toward zero, even immediately before epoch; flooring the textual
    // fraction would instead produce the preceding microsecond.
    let utc = if !nested {
        let nanos = i128::from(utc.timestamp()) * 1_000_000_000 + i128::from(utc.nanosecond());
        let micros = nanos / 1000;
        chrono::DateTime::from_timestamp(
            i64::try_from(micros.div_euclid(1_000_000))?,
            u32::try_from(micros.rem_euclid(1_000_000))? * 1000,
        )
        .context("legacy timestamp lies outside the original calendar range")?
    } else {
        utc
    };
    let year = if nested {
        if (0..=9999).contains(&utc.year()) {
            format!("{:04}", utc.year())
        } else if utc.year() < 0 {
            format!("-{:04}", utc.year().unsigned_abs())
        } else {
            format!("+{}", utc.year())
        }
    } else {
        format!("{:04}", utc.year())
    };
    let mut out = format!(
        "{year}-{:02}-{:02} {:02}:{:02}:{:02}",
        utc.month(),
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second()
    );
    let width = if nested { precision } else { 6 };
    if width != 0 && (nested && width != 6 || utc.nanosecond() != 0) {
        let fraction = utc.nanosecond() / 10u32.pow(u32::try_from(9 - width)?);
        out.push_str(&format!(".{fraction:0width$}"));
    }
    if zoned && nested {
        out.push(' ');
        out.push_str(zone);
    }
    Ok(out)
}

pub fn old_render_step(ctx: &OldRenderContext, results: &[StatementResult]) -> Result<ResultSet> {
    let mut output = ResultSet::default();
    for result in results.iter().filter(|r| r.is_result_set()) {
        let names = result
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        if output.header.is_empty() {
            output.header = names;
        } else {
            output.rows.push(names);
        }
        for row in &result.rows {
            ensure!(
                row.len() == result.columns.len(),
                "legacy result width differs from columns"
            );
            output.rows.push(
                row.iter()
                    .zip(&result.columns)
                    .map(|(v, c)| old_render_cell(ctx, &c.type_signature, v))
                    .collect::<Result<Vec<_>>>()?,
            );
        }
    }
    Ok(output)
}
pub fn read_legacy(block: &Block) -> Result<ResultSet> {
    let Block::Legacy { names, rows } = block else {
        bail!("expected legacy golden block");
    };
    Ok(ResultSet {
        golden: None,
        header: names
            .iter()
            .map(|s| crate::results::unescape_cell(s))
            .collect(),
        rows: rows
            .iter()
            .map(|r| r.iter().map(|s| crate::results::unescape_cell(s)).collect())
            .collect(),
    })
}
pub fn old_equivalent(
    ctx: &OldRenderContext,
    policy: &OldComparePolicy,
    legacy: &ResultSet,
    actual: &[StatementResult],
) -> Result<RowMapping> {
    let actual = old_render_step(ctx, actual)?;
    let (ok, detail) = crate::results::compare_result_sets(
        &legacy.header,
        &legacy.rows,
        &actual.header,
        &actual.rows,
        policy.order_sensitive,
        policy.float_epsilon,
    );
    ensure!(ok, "legacy rendering mismatch: {detail}");
    if policy.order_sensitive {
        return Ok(RowMapping {
            actual_rows: (0..actual.rows.len()).collect(),
        });
    }
    let mut expected_order = (0..legacy.rows.len()).collect::<Vec<_>>();
    let mut actual_order = (0..actual.rows.len()).collect::<Vec<_>>();
    let key = |row: &[String]| {
        row.iter()
            .map(|s| crate::results::normalized_cell_for_sort(s, policy.float_epsilon))
            .collect::<Vec<_>>()
    };
    expected_order.sort_by_key(|i| key(&legacy.rows[*i]));
    actual_order.sort_by_key(|i| key(&actual.rows[*i]));
    let mut mapping = vec![0; legacy.rows.len()];
    for (expected, actual) in expected_order.into_iter().zip(actual_order) {
        mapping[expected] = actual;
    }
    Ok(RowMapping {
        actual_rows: mapping,
    })
}
pub fn parse_old_scalar(sig: &TypeSignature, text: &str) -> Option<TrinoValue> {
    use TrinoValue as V;
    if text == "NULL" {
        return Some(V::Null);
    }
    Some(match sig.raw_type.as_str() {
        "boolean" => match text {
            "1" => V::Boolean(true),
            "0" => V::Boolean(false),
            _ => return None,
        },
        "tinyint" | "smallint" | "integer" | "bigint" => V::Integer(text.parse().ok()?),
        "real" => V::Real(text.parse().ok()?),
        "double" => V::Double(text.parse().ok()?),
        "varchar" | "char" | "decimal" | "json" => V::Text(text.into()),
        "date" => V::Text(
            if text == "0000-00-00" {
                "-0001-11-30"
            } else {
                text
            }
            .into(),
        ),
        "varbinary" if !text.contains('\u{FFFD}') => V::Binary(text.as_bytes().to_vec()),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::golden::parse_type;
    fn render(ty: &str, v: TrinoValue) -> String {
        old_render_cell(
            &OldRenderContext {
                variant_offset_seconds: 28800,
            },
            &parse_type(ty).unwrap(),
            &v,
        )
        .unwrap()
    }
    #[test]
    fn mysql_scalar_expectations_are_preserved() {
        // result-render/src/encoder.rs:1595-1613 and :1484-1501.
        assert_eq!(render("boolean", TrinoValue::Boolean(true)), "1");
        assert_eq!(render("double", TrinoValue::Double(2.0)), "2");
        assert_eq!(render("double", TrinoValue::Double(f64::INFINITY)), "inf");
        assert_eq!(
            render("date", TrinoValue::Text("-0001-11-30".into())),
            "0000-00-00"
        );
        assert_eq!(
            render(
                "timestamp(6)",
                TrinoValue::Text("2024-01-01 00:00:00.000000".into())
            ),
            "2024-01-01 00:00:00"
        );
        assert_eq!(
            render(
                "timestamp(6)",
                TrinoValue::Text("2024-01-01 00:00:00.250000".into())
            ),
            "2024-01-01 00:00:00.250000"
        );
        assert_eq!(
            render("varbinary", TrinoValue::Binary(vec![0xFF])),
            "\u{FFFD}"
        );
    }
    #[test]
    fn mysql_containers_keep_wire_map_order_and_named_struct() {
        // result-render/src/encoder.rs:760-1000, golden complex-type map rows.
        use TrinoValue as V;
        assert_eq!(
            render(
                "map(integer, varchar)",
                V::Map(vec![
                    (V::Integer(2), V::Text("y".into())),
                    (V::Integer(1), V::Text("x".into()))
                ])
            ),
            "{2:\"y\",1:\"x\"}"
        );
        assert_eq!(
            render(
                "row(col1 integer, col2 json)",
                V::Row(vec![V::Integer(1), V::Text("{\"k\": 2}".into())])
            ),
            "{\"col1\":1,\"col2\":'{\"k\": 2}'}"
        );
        assert_eq!(
            render("array(boolean)", V::Array(vec![V::Boolean(true), V::Null])),
            "[1,null]"
        );
    }
    #[test]
    fn scalar_transcription_does_not_invent_lost_binary_bytes() {
        assert!(parse_old_scalar(&parse_type("varbinary").unwrap(), "\u{FFFD}").is_none());
        assert!(parse_old_scalar(&parse_type("array(integer)").unwrap(), "[1]").is_none());
    }
    #[test]
    fn nested_text_escapes_only_quote_and_backslash_before_tsv_encoding() {
        // encoder.rs:425-435 ModeDouble/Single; control bytes are not JSON
        // escaped by this owner. results.rs::escape_cell handles TSV later.
        use TrinoValue as V;
        assert_eq!(
            render(
                "array(varchar)",
                V::Array(vec![V::Text("a\n\r\t\"\\中".into())])
            ),
            "[\"a\n\r\t\\\"\\\\中\"]"
        );
        assert_eq!(
            render(
                "array(varbinary)",
                V::Array(vec![V::Binary(vec![b'a', b'\n', 0xff, b'"', b'\\'])])
            ),
            "[\"a\n�\\\"\\\\\"]"
        );
        assert_eq!(
            render(
                "array(json)",
                V::Array(vec![V::Text("{\"s\":\"a'b\\\\c\"}".into())])
            ),
            "['{\"s\":\"a\\'b\\\\\\\\c\"}']"
        );
    }
    #[test]
    fn timestamp_precision_and_negative_epoch_follow_original_encoders() {
        // result-render/tests/encoder.rs:886-940 and :971-1000; top-level
        // nanos use encoder.rs:1570-1575 signed division toward zero.
        use TrinoValue as V;
        assert_eq!(
            render(
                "timestamp(9)",
                V::Text("1969-12-31 23:59:59.999999999".into())
            ),
            "1970-01-01 00:00:00"
        );
        assert_eq!(
            render(
                "array(timestamp(9))",
                V::Array(vec![V::Text("1969-12-31 23:59:59.999999999".into())])
            ),
            "[\"1969-12-31 23:59:59.999999999\"]"
        );
        assert_eq!(
            render(
                "array(timestamp(3))",
                V::Array(vec![V::Text("1970-01-01 00:00:00.000".into())])
            ),
            "[\"1970-01-01 00:00:00.000\"]"
        );
        assert_eq!(
            render(
                "array(timestamp(6))",
                V::Array(vec![V::Text("1970-01-01 00:00:00.000000".into())])
            ),
            "[\"1970-01-01 00:00:00\"]"
        );
        assert_eq!(
            render(
                "array(timestamp(0))",
                V::Array(vec![V::Text("+10000-01-01 00:00:00".into())])
            ),
            "[\"+10000-01-01 00:00:00\"]"
        );
        assert_eq!(
            render("timestamp(0)", V::Text("+10000-01-01 00:00:00".into())),
            "10000-01-01 00:00:00"
        );
        assert_eq!(render("date", V::Text("-0001-11-29".into())), "-001-11-29");
    }
    #[test]
    fn zoned_timestamp_recovers_utc_and_refuses_ambiguous_local_instants() {
        // encoder.rs:1686-1720 formats UTC for both presentations; nested
        // TimestampContainerText additionally appends the original zone.
        use TrinoValue as V;
        assert_eq!(
            render(
                "timestamp(6) with time zone",
                V::Text("1970-01-01 08:00:00.000000 +08:00".into())
            ),
            "1970-01-01 00:00:00"
        );
        assert_eq!(
            render(
                "array(timestamp(9) with time zone)",
                V::Array(vec![V::Text("1970-01-01 00:00:00.000000001 UTC".into())])
            ),
            "[\"1970-01-01 00:00:00.000000001 UTC\"]"
        );
        assert_eq!(
            render(
                "timestamp(6) with time zone",
                V::Text("1970-01-01 08:00:00.000000 Asia/Shanghai".into())
            ),
            "1970-01-01 00:00:00"
        );
        assert!(
            old_render_cell(
                &OldRenderContext {
                    variant_offset_seconds: 0
                },
                &parse_type("timestamp(6) with time zone").unwrap(),
                &V::Text("2024-11-03 01:30:00.000000 America/New_York".into())
            )
            .unwrap_err()
            .to_string()
            .contains("ambiguous")
        );
    }
    fn result(ty: &str, name: &str, values: Vec<TrinoValue>) -> StatementResult {
        StatementResult {
            columns: vec![novarocks_test_trino_client::Column {
                name: name.into(),
                type_name: ty.into(),
                type_signature: parse_type(ty).unwrap(),
            }],
            rows: values.into_iter().map(|v| vec![v]).collect(),
            query_id: "q".into(),
            pages: 1,
            update_type: None,
            update_count: None,
        }
    }
    #[test]
    fn old_multi_result_concatenation_keeps_subsequent_headers_as_rows() {
        // Baseline MysqlSession::execute session.rs:381-399.
        let actual = old_render_step(
            &OldRenderContext {
                variant_offset_seconds: 0,
            },
            &[
                result("integer", "a", vec![TrinoValue::Integer(1)]),
                result("varchar", "b", vec![TrinoValue::Text("x".into())]),
            ],
        )
        .unwrap();
        assert_eq!(
            actual,
            ResultSet {
                golden: None,
                header: vec!["a".into()],
                rows: vec![vec!["1".into()], vec!["b".into()], vec!["x".into()]]
            }
        );
    }
    #[test]
    fn row_mapping_preserves_original_unordered_epsilon_and_duplicates() {
        let expected = ResultSet {
            golden: None,
            header: vec!["v".into()],
            rows: vec![vec!["2".into()], vec!["1".into()], vec!["1".into()]],
        };
        let actual = [result(
            "double",
            "v",
            vec![
                TrinoValue::Double(1.00001),
                TrinoValue::Double(2.00001),
                TrinoValue::Double(1.00001),
            ],
        )];
        let mapping = old_equivalent(
            &OldRenderContext {
                variant_offset_seconds: 0,
            },
            &OldComparePolicy {
                order_sensitive: false,
                float_epsilon: Some(0.001),
            },
            &expected,
            &actual,
        )
        .unwrap();
        assert_eq!(mapping.actual_rows, vec![1, 0, 2]);
        assert!(
            old_equivalent(
                &OldRenderContext {
                    variant_offset_seconds: 0
                },
                &OldComparePolicy {
                    order_sensitive: true,
                    float_epsilon: Some(0.001)
                },
                &expected,
                &actual
            )
            .is_err()
        );
    }
    #[test]
    fn variant_reconstruction_refuses_the_lost_original_primitive_tags() {
        // encoder.rs:1249-1275 distinguishes f32 widening, f64, decimal and
        // non-finite tags; inline JSON cannot retain that distinction.
        for ty in ["variant", "array(variant)"] {
            let v = TrinoValue::Variant("1.2".into());
            let value = if ty.starts_with("array") {
                TrinoValue::Array(vec![v])
            } else {
                v
            };
            assert!(
                old_render_cell(
                    &OldRenderContext {
                        variant_offset_seconds: 0
                    },
                    &parse_type(ty).unwrap(),
                    &value
                )
                .unwrap_err()
                .to_string()
                .contains("cannot be recovered")
            );
        }
    }
}
