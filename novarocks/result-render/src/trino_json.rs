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

//! Trino JSON presentation over the shared bounded row machine.
use super::*;
use novarocks_result_contract::ClientPresentationFamily;

pub struct ArrowTrinoJsonEncoder(ArrowClientRowEncoder);
impl ArrowTrinoJsonEncoder {
    pub fn try_new(
        schema: Arc<ClientRenderSchema>,
        batch: RecordBatch,
    ) -> Result<Self, RenderError> {
        if !matches!(schema.family(), ClientPresentationFamily::TrinoJson(_)) {
            return Err(error(E::UnsupportedPresentation));
        }
        ArrowClientRowEncoder::try_new(schema, batch).map(Self)
    }
    pub fn try_new_root(
        contract: Arc<RootOutputContract>,
        batch: RecordBatch,
    ) -> Result<Self, RenderError> {
        let FrozenRootOutput::ClientRows(schema) = contract.output() else {
            return Err(error(E::SchemaMismatch));
        };
        if !matches!(schema.family(), ClientPresentationFamily::TrinoJson(_)) {
            return Err(error(E::UnsupportedPresentation));
        }
        ArrowClientRowEncoder::try_new_root(contract, batch).map(Self)
    }
    pub fn try_new_typed_root(
        input: ValidatedClientInput,
        batch: RecordBatch,
    ) -> Result<Self, RenderError> {
        let FrozenRootOutput::ClientRows(schema) = input.contract().output() else {
            return Err(error(E::SchemaMismatch));
        };
        if !matches!(schema.family(), ClientPresentationFamily::TrinoJson(_)) {
            return Err(error(E::UnsupportedPresentation));
        }
        ArrowClientRowEncoder::try_new_typed_root(input, batch).map(Self)
    }
}
impl BoundedClientRowEncoder for ArrowTrinoJsonEncoder {
    fn step(&mut self, out: &mut [u8]) -> Result<RenderTurn, RenderError> {
        self.0.step(out)
    }
    fn cancel(&mut self) {
        self.0.cancel();
    }
    fn scratch_capacity_bytes(&self) -> usize {
        self.0.scratch_capacity_bytes()
    }
}

pub(super) fn escape_byte(byte: u8, out: &mut [u8; 6]) -> usize {
    let escaped: Option<&[u8]> = match byte {
        b'"' => Some(b"\\\""),
        b'\\' => Some(b"\\\\"),
        b'\n' => Some(b"\\n"),
        b'\r' => Some(b"\\r"),
        b'\t' => Some(b"\\t"),
        8 => Some(b"\\b"),
        12 => Some(b"\\f"),
        _ => None,
    };
    if let Some(text) = escaped {
        out[..text.len()].copy_from_slice(text);
        text.len()
    } else if byte < 0x20 {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        *out = [
            b'\\',
            b'u',
            b'0',
            b'0',
            HEX[usize::from(byte >> 4)],
            HEX[usize::from(byte & 15)],
        ];
        6
    } else {
        out[0] = byte;
        1
    }
}

impl CellCursor {
    pub(super) fn trino_value(
        &mut self,
        array: ArrayRef,
        row: usize,
        path: Path,
        schema: &ClientRenderSchema,
        key: bool,
        budget: &mut Budget,
    ) -> Result<(), RenderError> {
        // Fixed descriptor work uses the same reserved operation quantum as
        // the MySQL cursor; variable source bytes are scanned by Stream.
        budget.scan(32);
        let ClientPresentationFamily::TrinoJson(facts) = *schema.family() else {
            return Err(invalid());
        };
        let field = path.field(schema)?;
        let (array, row) = resolve_dictionary(array, row)?;
        if row >= array.len() {
            return Err(error(E::SchemaMismatch));
        }
        if array.is_null(row) || matches!(field.native_type, N::Null) {
            if key {
                return Err(invalid_value());
            }
            if !field.nullable && !matches!(field.native_type, N::Null) {
                return Err(error(E::SchemaMismatch));
            }
            return self.literal(b"null");
        }
        match &field.native_type {
            N::List(_) if !key => {
                let (values, start, end) = list_values(&array, row)?;
                self.literal(b"]")?;
                self.push(Task::List {
                    array: values,
                    next: start,
                    end,
                    path,
                    first: true,
                })?;
                self.literal(b"[")?;
            }
            N::Map { .. } if !key => {
                let map = down::<MapArray>(&array)?;
                let start = usize::try_from(map.value_offsets()[row]).map_err(|_| invalid())?;
                let end = usize::try_from(map.value_offsets()[row + 1]).map_err(|_| invalid())?;
                if start > end || end > map.keys().len() || end > map.values().len() {
                    return Err(invalid());
                }
                self.literal(b"}")?;
                self.push(Task::Map {
                    array,
                    next: start,
                    end,
                    path,
                    phase: 0,
                    first: true,
                })?;
                self.literal(b"{")?;
            }
            N::Struct(_) if !key => {
                self.literal(b"]")?;
                self.push(Task::Struct {
                    array,
                    row,
                    path,
                    next: 0,
                    phase: 0,
                })?;
                self.literal(b"[")?;
            }
            N::String | N::Json | N::Binary => {
                let mut stream = Stream::new(
                    Source::array(array, row),
                    if matches!(field.native_type, N::Binary) {
                        Mode::Base64
                    } else {
                        Mode::Json
                    },
                );
                if matches!(field.native_type, N::Json) {
                    stream.json = Some(JsonCursor::new(path.depth as usize));
                }
                if !matches!(field.native_type, N::Binary) {
                    stream.utf8 = Some(Utf8::default());
                }
                self.literal(b"\"")?;
                self.push(Task::Stream(stream))?;
                self.literal(b"\"")?;
            }
            N::Variant if key => {
                let source = Source::array(array, row);
                let data = source.data(schema)?;
                let view = VariantView::parse(data)?;
                let head = data[view.metadata_end];
                let basic = match head & 3 {
                    0 => "PRIMITIVE",
                    1 => "SHORT_STRING",
                    2 => "OBJECT",
                    _ => "ARRAY",
                };
                let primitive = if head & 3 == 0 {
                    [
                        "NULL",
                        "BOOLEAN_TRUE",
                        "BOOLEAN_FALSE",
                        "INT8",
                        "INT16",
                        "INT32",
                        "INT64",
                        "DOUBLE",
                        "DECIMAL4",
                        "DECIMAL8",
                        "DECIMAL16",
                        "DATE",
                        "TIMESTAMP_UTC_MICROS",
                        "TIMESTAMP_NTZ_MICROS",
                        "FLOAT",
                        "BINARY",
                        "STRING",
                        "TIME_NTZ_MICROS",
                        "TIMESTAMP_UTC_NANOS",
                        "TIMESTAMP_NTZ_NANOS",
                        "UUID",
                    ]
                    .get(usize::from(head >> 2))
                    .copied()
                    .ok_or_else(|| error(E::InvalidVariant))?
                } else {
                    "null"
                };
                self.push(Task::Atom(atom!(
                    "\"Variant[basicType={basic}, primitiveType={primitive}]\""
                )))?;
            }
            N::Variant if !key => {
                let source = Source::array(array, row);
                let data = source.data(schema)?;
                let view = VariantView::parse(data)?;
                if facts.variant_binary {
                    self.literal(b"\"}")?;
                    self.push(Task::Stream(Stream::new(
                        source.slice(view.metadata_end..data.len())?,
                        Mode::Base64,
                    )))?;
                    self.literal(b"\",\"value\":\"")?;
                    self.push(Task::Stream(Stream::new(
                        source.slice(4..view.metadata_end)?,
                        Mode::Base64,
                    )))?;
                    self.literal(b"{\"metadata\":\"")?;
                } else {
                    if !facts.variant {
                        self.literal(b"\"")?;
                        self.push(Task::JsonEscape(false))?;
                    }
                    self.push(Task::Variant(Variant {
                        source: source.clone(),
                        view,
                        range: view.metadata_end..data.len(),
                        depth: path.depth as usize,
                        offset: 0,
                    }))?;
                    self.push(Task::Metadata {
                        source,
                        view,
                        next: 0,
                        at: 0,
                        utf8: Utf8::default(),
                    })?;
                    if !facts.variant {
                        self.push(Task::JsonEscape(true))?;
                        self.literal(b"\"")?;
                    }
                }
            }
            N::Boolean
            | N::SignedInteger(_)
            | N::Float32
            | N::Float64
            | N::Decimal {
                bits: 128 | 256, ..
            }
            | N::Date
            | N::Time { .. }
            | N::Timestamp { .. } => {
                let atom = scalar(&array, row, field, facts.parametric_datetime, key, budget)?;
                budget.scan(atom.len * 2 + 32);
                self.push(Task::Atom(atom))?;
            }
            _ => return Err(error(E::UnsupportedPresentation)),
        }
        Ok(())
    }
}

fn quoted(atom: Atom) -> Result<Atom, RenderError> {
    Ok(atom!("\"{}\"", atom))
}

pub(super) fn scalar(
    array: &ArrayRef,
    row: usize,
    field: &RenderField,
    parametric: bool,
    key: bool,
    budget: &mut Budget,
) -> Result<Atom, RenderError> {
    let atom = match &field.native_type {
        N::Boolean => Atom::literal(if down::<BooleanArray>(array)?.value(row) {
            b"true"
        } else {
            b"false"
        }),
        N::Float32 => float32(down::<Float32Array>(array)?.value(row), budget)?,
        N::Float64 => float64(down::<Float64Array>(array)?.value(row), budget)?,
        N::Decimal {
            bits: 128,
            precision,
            scale,
        } => {
            return quoted(decimal128(
                down::<Decimal128Array>(array)?.value(row),
                *scale as usize,
                Some(*precision),
            )?);
        }
        N::Decimal {
            bits: 256,
            precision,
            scale,
        } => {
            return quoted(decimal256(
                down::<Decimal256Array>(array)?.value(row).to_le_bytes(),
                *scale as usize,
                *precision,
            )?);
        }
        N::Date => {
            return quoted(date_atom(
                down::<Date32Array>(array)?.value(row),
                true,
                false,
            )?);
        }
        N::Time { unit } | N::Timestamp { unit, .. } => {
            return quoted(temporal(
                time_raw(array, row)?,
                *unit,
                &field.native_type,
                parametric || key,
            )?);
        }
        N::SignedInteger(_) => scalar_atom(array, row, field, true)?,
        _ => return Err(error(E::UnsupportedPresentation)),
    };
    if key {
        // Non-finite floating values are already JSON strings.
        if atom.data[0] == b'"' {
            Ok(atom)
        } else {
            quoted(atom)
        }
    } else {
        Ok(atom)
    }
}

pub(super) fn float32(n: f32, budget: &mut Budget) -> Result<Atom, RenderError> {
    if !n.is_finite() {
        return Ok(Atom::literal(if n.is_nan() {
            b"\"NaN\""
        } else if n.is_sign_positive() {
            b"\"Infinity\""
        } else {
            b"\"-Infinity\""
        }));
    }
    let decimal = atom!("{n}");
    budget.scan(decimal.len * 3);
    let scientific = n.abs() != 0.0 && (n.abs() < 1e-3 || n.abs() >= 1e7);
    if scientific && one_significant_digit(&decimal) {
        let mut rounded = atom!("{n:.1e}");
        for byte in &mut rounded.data[..rounded.len] {
            if *byte == b'e' {
                *byte = b'E';
            }
        }
        return Ok(rounded);
    }
    java_float(decimal, scientific)
}
pub(super) fn float64(n: f64, budget: &mut Budget) -> Result<Atom, RenderError> {
    if !n.is_finite() {
        return Ok(Atom::literal(if n.is_nan() {
            b"\"NaN\""
        } else if n.is_sign_positive() {
            b"\"Infinity\""
        } else {
            b"\"-Infinity\""
        }));
    }
    let decimal = atom!("{n}");
    budget.scan(decimal.len * 3);
    let scientific = n.abs() != 0.0 && (n.abs() < 1e-3 || n.abs() >= 1e7);
    if scientific && one_significant_digit(&decimal) {
        let mut rounded = atom!("{n:.1e}");
        for byte in &mut rounded.data[..rounded.len] {
            if *byte == b'e' {
                *byte = b'E';
            }
        }
        return Ok(rounded);
    }
    java_float(decimal, scientific)
}
// Java selects the nearest one- or two-digit decimal when the shortest
// round-trip has one significant digit, including MIN_VALUE subnormals.
// https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/Double.html#toString(double)
fn one_significant_digit(decimal: &Atom) -> bool {
    let start = usize::from(decimal.data[0] == b'-');
    let Some(first) =
        (start..decimal.len).find(|&i| decimal.data[i] != b'0' && decimal.data[i] != b'.')
    else {
        return false;
    };
    let mut last = decimal.len;
    while last > first + 1 && matches!(decimal.data[last - 1], b'0' | b'.') {
        last -= 1;
    }
    first + 1 == last
}
fn java_float(mut decimal: Atom, scientific: bool) -> Result<Atom, RenderError> {
    if !scientific {
        if !decimal.data[..decimal.len].contains(&b'.') {
            decimal.write_str(".0").map_err(|_| invalid())?;
        }
        return Ok(decimal);
    }
    let negative = decimal.data[0] == b'-';
    let begin = usize::from(negative);
    let dot = decimal.data[begin..decimal.len]
        .iter()
        .position(|c| *c == b'.')
        .map_or(decimal.len, |n| begin + n);
    let first = (begin..decimal.len)
        .find(|&i| decimal.data[i] != b'0' && decimal.data[i] != b'.')
        .ok_or_else(invalid)?;
    let exponent = if first < dot {
        dot as i32 - first as i32 - 1
    } else {
        dot as i32 - first as i32
    };
    let mut last = decimal.len;
    while last > first + 1 && (decimal.data[last - 1] == b'0' || decimal.data[last - 1] == b'.') {
        last -= 1;
    }
    let mut out = Atom::new();
    if negative {
        out.write_str("-").map_err(|_| invalid())?;
    }
    write!(&mut out, "{}.", decimal.data[first] as char).map_err(|_| invalid())?;
    let mut fraction = false;
    for &byte in &decimal.data[first + 1..last] {
        if byte != b'.' {
            write!(&mut out, "{}", byte as char).map_err(|_| invalid())?;
            fraction = true;
        }
    }
    if !fraction {
        out.write_str("0").map_err(|_| invalid())?;
    }
    write!(&mut out, "E{exponent}").map_err(|_| invalid())?;
    Ok(out)
}

pub(super) fn temporal(
    raw: i64,
    unit: RenderTimeUnit,
    ty: &N,
    parametric: bool,
) -> Result<Atom, RenderError> {
    let (factor, precision) = match unit {
        RenderTimeUnit::Second => (1_000_000_000i128, 0),
        RenderTimeUnit::Millisecond => (1_000_000, 3),
        RenderTimeUnit::Microsecond => (1000, 6),
        RenderTimeUnit::Nanosecond => (1, 9),
    };
    let mut nanos = i128::from(raw) * factor;
    let precision = if parametric {
        precision
    } else {
        nanos = (nanos + 500_000).div_euclid(1_000_000) * 1_000_000;
        3
    };
    if matches!(ty, N::Time { .. }) {
        let original = i128::from(raw) * factor;
        const DAY: i128 = 86_400_000_000_000;
        if !(0..DAY).contains(&original) {
            return Err(invalid_value());
        }
        nanos = nanos.rem_euclid(DAY);
        let seconds = nanos / 1_000_000_000;
        let mut out = atom!(
            "{:02}:{:02}:{:02}",
            seconds / 3600,
            seconds / 60 % 60,
            seconds % 60
        );
        fraction(&mut out, (nanos % 1_000_000_000) as u32, precision)?;
        return Ok(out);
    }
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(
        i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| invalid_value())?,
        nanos.rem_euclid(1_000_000_000) as u32,
    )
    .ok_or_else(invalid_value)?;
    let (local, zone) = if let N::Timestamp {
        timezone: Some(zone),
        ..
    } = ty
    {
        timezone(dt, zone)?
    } else {
        (dt.fixed_offset(), None)
    };
    let mut out = atom!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        year_atom(local.year(), true)?,
        local.month(),
        local.day(),
        local.hour(),
        local.minute(),
        local.second()
    );
    fraction(&mut out, local.nanosecond(), precision)?;
    if let Some(zone) = zone {
        write!(&mut out, " {zone}").map_err(|_| invalid_value())?;
    }
    Ok(out)
}
fn fraction(out: &mut Atom, nanos: u32, precision: u32) -> Result<(), RenderError> {
    if precision != 0 {
        write!(
            out,
            ".{:0width$}",
            nanos / 10u32.pow(9 - precision),
            width = precision as usize
        )
        .map_err(|_| invalid_value())?;
    }
    Ok(())
}
fn timezone(
    dt: chrono::DateTime<chrono::Utc>,
    zone: &str,
) -> Result<(chrono::DateTime<chrono::FixedOffset>, Option<Atom>), RenderError> {
    // Trino 483 TimeZoneKey normalizes these zero-offset aliases to UTC.
    let utc = matches!(
        zone,
        "GMT"
            | "GMT0"
            | "GMT+0"
            | "GMT-0"
            | "Etc/GMT"
            | "Etc/GMT0"
            | "Etc/GMT+0"
            | "Etc/GMT-0"
            | "UT"
            | "UT+0"
            | "UT-0"
            | "Etc/UT"
            | "Etc/UT+0"
            | "Etc/UT-0"
            | "UTC"
            | "UTC+0"
            | "UTC-0"
            | "Etc/UTC"
            | "Etc/UTC+0"
            | "Etc/UTC-0"
            | "+0000"
            | "+00:00"
            | "-0000"
            | "-00:00"
            | "Z"
            | "Zulu"
            | "UCT"
            | "Greenwich"
            | "Universal"
            | "Etc/Universal"
            | "Etc/UCT"
    );
    if utc {
        return Ok((dt.fixed_offset(), Some(Atom::literal(b"UTC"))));
    }
    let prefixed = ["Etc/GMT", "Etc/UTC", "Etc/UT", "GMT", "UTC", "UT"]
        .iter()
        .find_map(|prefix| {
            zone.strip_prefix(prefix)
                .filter(|suffix| suffix.starts_with(['+', '-']))
                .map(|suffix| (suffix, *prefix == "Etc/GMT"))
        });
    let offset = if let Some((text, invert)) = prefixed {
        Some((text, invert))
    } else if zone.starts_with(['+', '-']) {
        Some((zone, false))
    } else {
        None
    };
    if let Some((text, invert)) = offset {
        if !text.is_ascii() {
            return Err(invalid_value());
        }
        let negative = text.as_bytes()[0] == b'-';
        let body = &text[1..];
        let (hours, minutes) = if let Some((h, m)) = body.split_once(':') {
            (
                h.parse::<i32>().map_err(|_| invalid_value())?,
                m.parse::<i32>().map_err(|_| invalid_value())?,
            )
        } else if body.len() <= 2 {
            (body.parse::<i32>().map_err(|_| invalid_value())?, 0)
        } else if body.len() == 4 {
            (
                body[..2].parse::<i32>().map_err(|_| invalid_value())?,
                body[2..].parse::<i32>().map_err(|_| invalid_value())?,
            )
        } else {
            return Err(invalid_value());
        };
        if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
            return Err(invalid_value());
        }
        let mut seconds = hours * 3600 + minutes * 60;
        if negative ^ invert {
            seconds = -seconds;
        }
        let offset = chrono::FixedOffset::east_opt(seconds).ok_or_else(invalid_value)?;
        let text = if seconds == 0 {
            Atom::literal(b"UTC")
        } else {
            atom!(
                "{}{:02}:{:02}",
                if seconds < 0 { '-' } else { '+' },
                seconds.unsigned_abs() / 3600,
                seconds.unsigned_abs() / 60 % 60
            )
        };
        return Ok((dt.with_timezone(&offset), Some(text)));
    }
    let tz: chrono_tz::Tz = zone.parse().map_err(|_| invalid_value())?;
    Ok((dt.with_timezone(&tz).fixed_offset(), Some(atom!("{zone}"))))
}

pub(super) fn validate_schema(schema: &ClientRenderSchema) -> Result<(), RenderError> {
    fn visit(field: &RenderField) -> Result<(), RenderError> {
        match &field.native_type {
            N::Timestamp {
                timezone: Some(zone),
                ..
            } => {
                timezone(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH, zone)?;
            }
            N::List(child) => visit(child)?,
            N::Map { key, value } => {
                visit(key)?;
                visit(value)?;
            }
            N::Struct(fields) => {
                for child in fields {
                    visit(&child.field)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    for (ordinal, column) in schema.columns().iter().enumerate() {
        visit(&column.field).map_err(|mut e| {
            e.output_ordinal = Some(ordinal as u32);
            e
        })?;
    }
    Ok(())
}

// Java BigDecimal.toString keeps scale and uses an exponent below 1E-6.
pub(super) fn variant_decimal(decimal: Atom, scale: u8) -> Result<Atom, RenderError> {
    let start = usize::from(decimal.data[0] == b'-');
    let first = (start..decimal.len).find(|&i| decimal.data[i] != b'0' && decimal.data[i] != b'.');
    let Some(first) = first else {
        if scale > 6 {
            return Ok(atom!("0E-{scale}"));
        }
        return Ok(decimal);
    };
    let dot = decimal.data[start..decimal.len]
        .iter()
        .position(|c| *c == b'.')
        .map_or(decimal.len, |i| start + i);
    let exponent = if first < dot {
        dot as i32 - first as i32 - 1
    } else {
        dot as i32 - first as i32
    };
    if exponent >= -6 {
        return Ok(decimal);
    }
    let mut out = Atom::new();
    if start != 0 {
        out.write_str("-").map_err(|_| invalid())?;
    }
    write!(&mut out, "{}", decimal.data[first] as char).map_err(|_| invalid())?;
    if first + 1 < decimal.len {
        out.write_str(".").map_err(|_| invalid())?;
    }
    for &byte in &decimal.data[first + 1..decimal.len] {
        if byte != b'.' {
            write!(&mut out, "{}", byte as char).map_err(|_| invalid())?;
        }
    }
    write!(&mut out, "E{exponent}").map_err(|_| invalid())?;
    Ok(out)
}

#[cfg(test)]
#[path = "trino_json_tests.rs"]
mod tests;
