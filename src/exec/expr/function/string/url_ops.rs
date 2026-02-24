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
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};

fn url_encode_impl(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len().saturating_mul(3));
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            escaped.push(char::from(b));
            continue;
        }
        escaped.push('%');
        escaped.push(char::from(b"0123456789ABCDEF"[(b >> 4) as usize]));
        escaped.push(char::from(b"0123456789ABCDEF"[(b & 0x0F) as usize]));
    }
    escaped
}

fn decode_hex_char(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

fn url_decode_impl(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err("decode string contains illegal hex chars: %".to_string());
            }
            let l = bytes[i + 1];
            let r = bytes[i + 2];
            let lh = decode_hex_char(l).ok_or_else(|| {
                format!(
                    "decode string contains illegal hex chars: {}{}",
                    char::from(l),
                    char::from(r)
                )
            })?;
            let rh = decode_hex_char(r).ok_or_else(|| {
                format!(
                    "decode string contains illegal hex chars: {}{}",
                    char::from(l),
                    char::from(r)
                )
            })?;
            out.push((lh << 4) | rh);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| format!("url_decode utf8 decode failed: {e}"))
}

fn extract_host_impl(url: &str) -> Option<String> {
    let scheme_pos = url.find("://")?;
    let mut authority = &url[(scheme_pos + 3)..];
    let end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    authority = &authority[..end];
    if authority.is_empty() {
        return None;
    }
    if let Some(at_idx) = authority.rfind('@') {
        authority = &authority[(at_idx + 1)..];
    }
    if authority.is_empty() {
        return None;
    }

    if authority.starts_with('[') {
        let closing = authority.find(']')?;
        return Some(authority[1..closing].to_string());
    }

    let host = if let Some(colon_idx) = authority.rfind(':') {
        let port = &authority[(colon_idx + 1)..];
        if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
            &authority[..colon_idx]
        } else {
            authority
        }
    } else {
        authority
    };
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

fn eval_url_unary<F>(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
    f: F,
) -> Result<ArrayRef, String>
where
    F: Fn(&str) -> Result<String, String>,
{
    let arr = arena.eval(args[0], chunk)?;
    let arr = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{fn_name} expects string"))?;

    let rows = arr.len().max(chunk.len());
    if arr.len() != 1 && arr.len() != rows {
        return Err(format!(
            "{fn_name} input length mismatch: {} vs {rows}",
            arr.len()
        ));
    }

    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let idx = if arr.len() == 1 { 0 } else { row };
        if arr.is_null(idx) {
            out.push(None);
            continue;
        }
        let value = arr.value(idx);
        out.push(Some(f(value)?));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_url_encode(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_url_unary(arena, args, chunk, "url_encode", |v| Ok(url_encode_impl(v)))
}

pub fn eval_url_decode(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_url_unary(arena, args, chunk, "url_decode", url_decode_impl)
}

pub fn eval_url_extract_host(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let arr = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "url_extract_host expects string".to_string())?;

    let rows = arr.len().max(chunk.len());
    if arr.len() != 1 && arr.len() != rows {
        return Err(format!(
            "url_extract_host input length mismatch: {} vs {rows}",
            arr.len()
        ));
    }

    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let idx = if arr.len() == 1 { 0 } else { row };
        if arr.is_null(idx) {
            out.push(None);
            continue;
        }
        out.push(extract_host_impl(arr.value(idx)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::{extract_host_impl, url_decode_impl, url_encode_impl};

    #[test]
    fn test_url_encode_decode_logic() {
        let encoded = url_encode_impl("https://docs.starrocks.io/en-us/latest/quick_start/Deploy");
        assert_eq!(
            encoded,
            "https%3A%2F%2Fdocs.starrocks.io%2Fen-us%2Flatest%2Fquick_start%2FDeploy"
        );
        let decoded = url_decode_impl(&encoded).unwrap();
        assert_eq!(
            decoded,
            "https://docs.starrocks.io/en-us/latest/quick_start/Deploy"
        );
    }

    #[test]
    fn test_url_extract_host_logic() {
        assert_eq!(
            extract_host_impl("https://starrocks.com/test/api/v1").as_deref(),
            Some("starrocks.com")
        );
        assert_eq!(
            extract_host_impl("https://starrocks.快速.com/test/api/v1").as_deref(),
            Some("starrocks.快速.com")
        );
        assert_eq!(extract_host_impl("bad-url"), None);
    }
}
