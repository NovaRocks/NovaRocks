// SPDX-License-Identifier: Apache-2.0

//! Prepared statement header codec.

use axum::http::HeaderMap;
use base64::{
    Engine,
    alphabet::URL_SAFE,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
use novarocks_parser::parser::validate_statement_syntax;
use novarocks_query_application::session_value::{
    PreparedStatementDefinition, PreparedStatementSnapshot,
};
use std::{collections::BTreeMap, sync::Arc};
use zstd_safe::{DCtx, InBuffer, OutBuffer};

pub(crate) const HEADER: &str = "x-trino-prepared-statement";
const COMPRESSED_PREFIX: &str = "$zstd:";
const COMPRESSION_THRESHOLD_UTF16: usize = 2 * 1024;
const COMPRESSION_MIN_GAIN_UTF16: usize = 512;
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Clone, Copy, Debug)]
pub(crate) enum IngressError {
    Frame,
    Form,
    Compression,
    Utf8,
    Syntax,
    Duplicate,
}

impl std::fmt::Display for IngressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Frame => "Invalid X-Trino-Prepared-Statement header",
            Self::Form => "Invalid X-Trino-Prepared-Statement form encoding",
            Self::Compression => "Invalid compressed prepared statement",
            Self::Utf8 => "Prepared statement header must encode UTF-8",
            Self::Syntax => "Prepared statement header must contain one valid statement",
            Self::Duplicate => "Duplicate decoded prepared statement name",
        })
    }
}

pub(crate) fn decode(headers: &HeaderMap) -> Result<PreparedStatementSnapshot, IngressError> {
    let mut pairs = BTreeMap::new();
    for pair in assignments(headers) {
        let (name, value) = pair?;
        // Reference parseProperty checks form syntax before applying last-wins
        // to assignments with the same raw name.
        if !super::request::valid_form_value(value) {
            return Err(IngressError::Form);
        }
        pairs.insert(name, value);
    }
    if pairs.is_empty() {
        return Ok(PreparedStatementSnapshot::default());
    }
    let mut definitions = Vec::with_capacity(pairs.len());
    for (name, sql) in pairs {
        let name = super::request::decode_form_value(name).ok_or(IngressError::Form)?;
        definitions.push((name, decode_sql(sql)?));
    }
    PreparedStatementSnapshot::new(definitions).map_err(|_| IngressError::Duplicate)
}

fn decode_sql(encoded: &str) -> Result<PreparedStatementDefinition, IngressError> {
    let form = super::request::decode_form_value(encoded).ok_or(IngressError::Form)?;
    let sql = if let Some(compressed) = form.strip_prefix(COMPRESSED_PREFIX) {
        String::from_utf8(decompress(compressed)?).map_err(|_| IngressError::Utf8)?
    } else {
        form
    };
    validate_statement_syntax(&sql).map_err(|_| IngressError::Syntax)?;
    Ok(PreparedStatementDefinition::new(Arc::from(sql)))
}

fn decompress(encoded: &str) -> Result<Vec<u8>, IngressError> {
    let compressed = BASE64
        .decode(encoded)
        .map_err(|_| IngressError::Compression)?;
    let mut decoder = DCtx::create();
    // Accept the library's supported frame window range.
    // SAFETY: This query takes the library's own parameter identity.
    let bounds = unsafe {
        zstd_safe::zstd_sys::ZSTD_dParam_getBounds(
            zstd_safe::zstd_sys::ZSTD_dParameter::ZSTD_d_windowLogMax,
        )
    };
    // SAFETY: The value is the status returned by the bounds query.
    if unsafe { zstd_safe::zstd_sys::ZSTD_isError(bounds.error) } != 0 {
        return Err(IngressError::Compression);
    }
    decoder
        .set_parameter(zstd_safe::DParameter::WindowLogMax(
            u32::try_from(bounds.upperBound).map_err(|_| IngressError::Compression)?,
        ))
        .map_err(|_| IngressError::Compression)?;
    let mut input = InBuffer {
        src: &compressed,
        pos: 0,
    };
    let mut decoded = Vec::new();
    let mut chunk = [0; 16 * 1024];
    let declared =
        zstd_safe::get_frame_content_size(&compressed).map_err(|_| IngressError::Compression)?;
    loop {
        let position = input.pos;
        let mut output = OutBuffer::around(&mut chunk[..]);
        let remaining = decoder
            .decompress_stream(&mut output, &mut input)
            .map_err(|_| IngressError::Compression)?;
        let produced = output.pos();
        decoded.extend_from_slice(&chunk[..produced]);
        if remaining == 0 {
            if input.pos != compressed.len()
                || declared.is_some_and(|size| size != decoded.len() as u64)
            {
                return Err(IngressError::Compression);
            }
            return Ok(decoded);
        }
        if input.pos == position && produced == 0 {
            return Err(IngressError::Compression);
        }
    }
}

fn assignments(headers: &HeaderMap) -> impl Iterator<Item = Result<(&str, &str), IngressError>> {
    headers.get_all(HEADER).into_iter().flat_map(|value| {
        let text = value.to_str();
        text.as_ref()
            .ok()
            .copied()
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(|part| {
                let (name, value) = part.split_once('=').ok_or(IngressError::Frame)?;
                if value.contains('=') {
                    return Err(IngressError::Frame);
                }
                Ok((name.trim(), value.trim()))
            })
            .chain(text.err().map(|_| Err(IngressError::Frame)))
    })
}

pub(crate) fn header_sql(sql: &str) -> Result<std::borrow::Cow<'_, str>, &'static str> {
    use std::borrow::Cow;
    let units = sql.encode_utf16().count();
    if units < COMPRESSION_THRESHOLD_UTF16 {
        return Ok(Cow::Borrowed(sql));
    }
    let mut buffer = vec![0; zstd_safe::compress_bound(sql.len())];
    let compressed = zstd_safe::compress(&mut buffer[..], sql.as_bytes(), 3)
        .map_err(zstd_safe::get_error_name)?;
    let encoded = BASE64.encode(&buffer[..compressed]);
    if encoded.len() + COMPRESSED_PREFIX.len() + COMPRESSION_MIN_GAIN_UTF16 > units {
        Ok(Cow::Borrowed(sql))
    } else {
        Ok(Cow::Owned(format!("{COMPRESSED_PREFIX}{encoded}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn prepared_compression_requires_one_complete_frame() {
        let sql = b"SELECT 1";
        let mut buffer = vec![0; zstd_safe::compress_bound(sql.len())];
        let written = zstd_safe::compress(&mut buffer[..], sql, 3).unwrap();
        let frame = &buffer[..written];
        assert_eq!(decompress(&BASE64.encode(frame)).unwrap().as_slice(), sql);

        let concatenated = [frame, frame].concat();
        for invalid in [&frame[..written - 1], concatenated.as_slice()] {
            assert!(matches!(
                decompress(&BASE64.encode(invalid)),
                Err(IngressError::Compression)
            ));
        }
    }

    #[test]
    fn upstream_and_local_prepared_compression_decode_the_exact_sql() {
        // Captured from Trino 484's Added-Prepare response to a long
        // SELECT length('<2048 a characters>'); its formatter adds LF twice.
        let upstream = "p=%24zstd%3AKLUv_WATB90AAKBTRUxFQ1QgbGVuZ3RoKCdhJykKCgEA-P_BEg%3D%3D";
        let text = format!("SELECT length('{}')", "a".repeat(2048));
        let mut headers = HeaderMap::new();
        headers.insert(HEADER, HeaderValue::from_static(upstream));
        let snapshot = decode(&headers).unwrap();
        assert_eq!(
            snapshot.get("p").unwrap().sql().as_ref(),
            format!("{text}\n\n")
        );
        drop(snapshot);

        let name = "中+,=";
        let effects = novarocks_query_application::session_effects::SessionEffects::prepare(
            name.into(),
            Arc::from(text.as_str()),
        );
        let result = crate::request::session_effect_headers(&effects).unwrap();
        let added = &result["x-trino-added-prepare"];
        assert!(
            added
                .to_str()
                .unwrap()
                .starts_with("%E4%B8%AD%2B%2C%3D=%24zstd%3A")
        );
        headers.insert(HEADER, added.clone());
        let snapshot = decode(&headers).unwrap();
        assert_eq!(snapshot.get(name).unwrap().sql().as_ref(), text);
        assert_eq!(header_sql("SELECT 1").unwrap(), "SELECT 1");
    }
}
