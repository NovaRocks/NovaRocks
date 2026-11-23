// SPDX-License-Identifier: Apache-2.0

//! Prepared statement header codec and statement-owned ingress capacity.

use axum::http::HeaderMap;
use base64::{
    Engine,
    alphabet::URL_SAFE,
    engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
};
use novarocks_memory::{Account, ExplicitGrant, FactToken};
use novarocks_query_application::session_value::{
    PreparedStatementDefinition, PreparedStatementSnapshot,
};
use std::{mem::size_of, sync::Arc};
use zstd_safe::{DCtx, InBuffer, OutBuffer};

pub(crate) const HEADER: &str = "x-trino-prepared-statement";
const COMPRESSED_PREFIX: &str = "$zstd:";
const COMPRESSION_THRESHOLD_UTF16: usize = 2 * 1024;
const COMPRESSION_MIN_GAIN_UTF16: usize = 512;
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// The single listener's ordinary FE service account. No control floor or
/// separate ingress quota participates in prepared statement admission.
#[derive(Clone, Debug)]
pub struct PreparedStatementIngress {
    account: Account,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum IngressError {
    Frame,
    Form,
    Compression,
    Utf8,
    Syntax,
    Duplicate,
    Capacity,
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
            Self::Capacity => "Prepared statement ingress capacity exhausted",
        })
    }
}

// Frozen graphs keep this witness after the explicit grant closes. Owners put
// the governed storage before its token so the actual release is first.
struct Allocation {
    token: FactToken,
    bytes: u64,
}

impl Allocation {
    fn settle(mut grant: ExplicitGrant, bytes: usize) -> Result<Self, IngressError> {
        let bytes = u64::try_from(bytes).map_err(|_| IngressError::Capacity)?;
        let token = grant
            .record_success(bytes)
            .map_err(|_| IngressError::Capacity)?;
        Ok(Self { token, bytes })
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        // SAFETY: This owner is non-Clone, records one exact storage receipt
        // and is dropped only after the receipt's data has been released.
        unsafe { self.token.record_deallocation(self.bytes) }
    }
}

#[derive(Default)]
struct DecodeBuffer {
    bytes: Vec<u8>,
    allocation: Option<Allocation>,
}

impl DecodeBuffer {
    fn extend(
        &mut self,
        bytes: &[u8],
        ingress: &PreparedStatementIngress,
    ) -> Result<(), IngressError> {
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(IngressError::Capacity)?;
        if needed > self.bytes.capacity() {
            let capacity = self.bytes.capacity().saturating_mul(2).max(needed);
            let grant = ingress.reserve(capacity)?;
            let mut replacement = Vec::new();
            replacement
                .try_reserve_exact(capacity)
                .map_err(|_| IngressError::Capacity)?;
            replacement.extend_from_slice(&self.bytes);
            replacement.extend_from_slice(bytes);
            let allocation = Allocation::settle(grant, replacement.capacity())?;
            // The previous Vec is freed while its charge is still live.
            self.bytes = replacement;
            self.allocation = Some(allocation);
        } else {
            self.bytes.extend_from_slice(bytes);
        }
        Ok(())
    }
}

impl PreparedStatementIngress {
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    fn reserve(&self, bytes: usize) -> Result<ExplicitGrant, IngressError> {
        let bytes = u64::try_from(bytes).map_err(|_| IngressError::Capacity)?;
        self.account
            .request_explicit(bytes)
            .map_err(|_| IngressError::Capacity)
    }

    pub(crate) fn decode(
        &self,
        headers: &HeaderMap,
    ) -> Result<PreparedStatementSnapshot, IngressError> {
        // Count and validate framing without allocating. Transport owns the
        // raw HTTP buffer. The temporary framed views keep their own grant.
        let mut count = 0;
        for pair in assignments(headers) {
            let (_, value) = pair?;
            // Reference parseProperty validates each assignment's form before
            // applying last-wins to the exact raw name.
            if !super::request::valid_form_value(value) {
                return Err(IngressError::Form);
            }
            count += 1;
        }
        if count == 0 {
            return Ok(PreparedStatementSnapshot::default());
        }
        let _framing_budget = self.reserve(vector_bytes::<(&str, &str, usize)>(count)?)?;
        let mut pairs = Vec::new();
        pairs
            .try_reserve_exact(count)
            .map_err(|_| IngressError::Capacity)?;
        for (ordinal, pair) in assignments(headers).enumerate() {
            let (name, value) = pair?;
            pairs.push((name, value, ordinal));
        }
        pairs.sort_unstable_by(|left, right| left.0.cmp(right.0).then(right.2.cmp(&left.2)));
        pairs.dedup_by(|left, right| left.0 == right.0);
        let key_bytes = pairs.iter().try_fold(0usize, |bytes, (name, _, _)| {
            bytes.checked_add(name.len()).ok_or(IngressError::Capacity)
        })?;
        let metadata = PreparedStatementSnapshot::retained_metadata_bytes::<Allocation>()
            .ok_or(IngressError::Capacity)?;
        let mapping_bytes = vector_bytes::<(String, PreparedStatementDefinition)>(pairs.len())?;
        let mapping_grant = self.reserve(
            mapping_bytes
                .checked_add(key_bytes)
                .and_then(|bytes| bytes.checked_add(metadata))
                .ok_or(IngressError::Capacity)?,
        )?;
        let mut definitions = Vec::new();
        definitions
            .try_reserve_exact(pairs.len())
            .map_err(|_| IngressError::Capacity)?;
        for &(name, sql, _) in &pairs {
            let name = super::request::decode_form_value(name).ok_or(IngressError::Form)?;
            let definition = self.decode_sql(sql)?;
            definitions.push((name, definition));
        }
        let capacity =
            vector_bytes::<(String, PreparedStatementDefinition)>(definitions.capacity())?;
        let capacity = definitions.iter().try_fold(capacity, |bytes, (name, _)| {
            bytes
                .checked_add(name.capacity())
                .ok_or(IngressError::Capacity)
        })?;
        let mapping = Allocation::settle(
            mapping_grant,
            capacity
                .checked_add(metadata)
                .ok_or(IngressError::Capacity)?,
        )?;
        PreparedStatementSnapshot::new(definitions, mapping).map_err(|_| IngressError::Duplicate)
    }

    fn decode_sql(&self, encoded: &str) -> Result<PreparedStatementDefinition, IngressError> {
        let _form_budget = self.reserve(encoded.len())?;
        let form = super::request::decode_form_value(encoded).ok_or(IngressError::Form)?;
        let expanded;
        let sql = if let Some(compressed) = form.strip_prefix(COMPRESSED_PREFIX) {
            expanded = self.decompress(compressed)?;
            std::str::from_utf8(&expanded.bytes).map_err(|_| IngressError::Utf8)?
        } else {
            &form
        };
        // The decoded source and the parser's token storage overlap until the
        // original grammar has validated the statement.
        use novarocks_parser::parser::{StatementValidationError, validate_statement_syntax};
        validate_statement_syntax(sql, |bytes| self.reserve(bytes).ok()).map_err(|error| {
            match error {
                StatementValidationError::Syntax(_) => IngressError::Syntax,
                StatementValidationError::TokenCapacity => IngressError::Capacity,
            }
        })?;
        let retained_bytes =
            PreparedStatementDefinition::retained_content_bytes::<Allocation>(sql.len())
                .ok_or(IngressError::Capacity)?;
        let retention_grant = self.reserve(retained_bytes)?;
        let sql: Arc<str> = Arc::from(sql);
        let retention = Allocation::settle(retention_grant, retained_bytes)?;
        Ok(PreparedStatementDefinition::new(sql, retention))
    }

    fn decompress(&self, encoded: &str) -> Result<DecodeBuffer, IngressError> {
        let input_capacity = base64::decoded_len_estimate(encoded.len());
        let _input_budget = self.reserve(input_capacity)?;
        let mut compressed = Vec::new();
        compressed
            .try_reserve_exact(input_capacity)
            .map_err(|_| IngressError::Capacity)?;
        compressed.resize(input_capacity, 0);
        let length = BASE64
            .decode_slice(encoded, &mut compressed)
            .map_err(|_| IngressError::Compression)?;
        compressed.truncate(length);
        // The upstream estimator inspects only the frame header. Admission
        // precedes initialization of any dynamic decompressor workspace.
        // SAFETY: Zstd reads exactly the supplied borrowed frame prefix.
        let workspace = unsafe {
            zstd_safe::zstd_sys::ZSTD_estimateDStreamSize_fromFrame(
                compressed.as_ptr().cast(),
                compressed.len(),
            )
        };
        // SAFETY: The value is the return code from the estimator above.
        if unsafe { zstd_safe::zstd_sys::ZSTD_isError(workspace) } != 0 {
            return Err(IngressError::Compression);
        }
        let _decoder_budget = self.reserve(workspace)?;
        let mut decoder = DCtx::try_create().ok_or(IngressError::Capacity)?;
        // The process grant has already qualified the frame's workspace.
        // Keep the library's representable range instead of its default,
        // independent streaming-window policy.
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
        let mut decoded = DecodeBuffer::default();
        let mut chunk = [0; 16 * 1024];
        let declared = zstd_safe::get_frame_content_size(&compressed)
            .map_err(|_| IngressError::Compression)?;
        loop {
            let position = input.pos;
            let mut output = OutBuffer::around(&mut chunk[..]);
            let remaining = decoder
                .decompress_stream(&mut output, &mut input)
                .map_err(|_| IngressError::Compression)?;
            let produced = output.pos();
            decoded.extend(&chunk[..produced], self)?;
            if remaining == 0 {
                if input.pos != compressed.len()
                    || declared.is_some_and(|size| size != decoded.bytes.len() as u64)
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
}

fn vector_bytes<T>(count: usize) -> Result<usize, IngressError> {
    count
        .checked_mul(size_of::<T>())
        .ok_or(IngressError::Capacity)
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
pub(crate) fn test_ingress() -> (novarocks_memory::MemoryAuthority, PreparedStatementIngress) {
    use novarocks_memory::{
        AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TopUpPolicy,
    };
    let cap = 64 * 1024 * 1024;
    let mut config = AuthorityConfig::new(cap, cap, 0);
    config.top_up = TopUpPolicy::uniform(1);
    let authority = MemoryAuthority::new(config).unwrap();
    let account = authority
        .create_account(AccountKind::Service, ExternalRef::NONE)
        .unwrap();
    (authority, PreparedStatementIngress::new(account))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn trino_prepared_ingress_applies_process_capacity_to_parser_tokens() {
        let (authority, ingress) = test_ingress();
        let capacity = authority.snapshot().capacity_bytes;
        let mut writer = authority.take_capacity_writer().unwrap();
        let baseline = authority.root().committed_bytes();
        let form = format!("SELECT+{}1", "1%2C+".repeat(32));
        writer.set_capacity(1024).unwrap();

        assert!(matches!(
            ingress.decode_sql(&form),
            Err(IngressError::Capacity)
        ));
        while !authority.maintain(64).complete {}
        assert_eq!(authority.root().committed_bytes(), baseline);

        writer.set_capacity(capacity).unwrap();
        let definition = ingress.decode_sql(&form).unwrap();
        assert_eq!(definition.sql().matches(',').count(), 32);
        drop(definition);
        while !authority.maintain(64).complete {}
        assert_eq!(authority.root().committed_bytes(), baseline);
    }

    #[test]
    fn upstream_and_local_prepared_compression_decode_the_exact_sql() {
        // Captured from Trino 484's Added-Prepare response to a long
        // SELECT length('<2048 a characters>'); its formatter adds LF twice.
        let upstream = "p=%24zstd%3AKLUv_WATB90AAKBTRUxFQ1QgbGVuZ3RoKCdhJykKCgEA-P_BEg%3D%3D";
        let text = format!("SELECT length('{}')", "a".repeat(2048));
        let (_authority, ingress) = test_ingress();
        let mut headers = HeaderMap::new();
        headers.insert(HEADER, HeaderValue::from_static(upstream));
        let snapshot = ingress.decode(&headers).unwrap();
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
        let snapshot = ingress.decode(&headers).unwrap();
        assert_eq!(snapshot.get(name).unwrap().sql().as_ref(), text);
        assert_eq!(header_sql("SELECT 1").unwrap(), "SELECT 1");
    }
}
