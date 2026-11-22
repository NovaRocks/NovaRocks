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
//! The original v1 PARSE_URL computation. The key supplier is evaluated only for QUERY.
//! The parser, uppercase conversion, query iterator and owned text are opaque library work.
use url::Url;
/// Outer None means the original two-argument call. Some(None) means its NULL query key.
/// This supplier preserves the original conditional key access without depending on an arena.
pub fn parse_value<'a>(
    url_str: &str,
    part_str: &str,
    key_value: &mut dyn FnMut() -> Option<Option<&'a str>>,
) -> Option<String> {
    let part = part_str.to_uppercase();
    let url = Url::parse(url_str).ok();
    match (url, part.as_str()) {
        (Some(u), "HOST") => u.host_str().map(|s| s.to_string()),
        (Some(u), "PATH") => Some(u.path().to_string()),
        (Some(u), "PROTOCOL") => Some(u.scheme().to_string()),
        (Some(u), "REF") => u.fragment().map(|s| s.to_string()),
        (Some(u), "QUERY") => {
            if let Some(key) = key_value() {
                if let Some(key) = key {
                    u.query_pairs()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.to_string())
                } else {
                    None
                }
            } else {
                u.query().map(|s| s.to_string())
            }
        }
        _ => None,
    }
}

use crate::{KernelFailure, kernel_input::EvaluationCheckpoints};

/// Borrowed Java URI components. Authority recognition supplies a server
/// host and port only when the authority has the server grammar.
pub(super) struct TrinoUri<'a> {
    scheme: &'a str,
    host: &'a str,
    pub(super) port: Option<i64>,
    path: &'a str,
    query: Option<&'a str>,
    fragment: &'a str,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum TrinoUrlPart {
    Protocol,
    Host,
    Port,
    Path,
    Query,
    Fragment,
    Parameter,
    Decode,
}

impl TrinoUrlPart {
    pub(super) fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "url_extract_protocol" => Self::Protocol,
            "url_extract_host" => Self::Host,
            "url_extract_port" => Self::Port,
            "url_extract_path" => Self::Path,
            "url_extract_query" => Self::Query,
            "url_extract_fragment" => Self::Fragment,
            "url_extract_parameter" => Self::Parameter,
            "url_decode" => Self::Decode,
            _ => return None,
        })
    }

    pub(super) fn is_port(self) -> bool {
        matches!(self, Self::Port)
    }

    pub(super) fn arity(self) -> usize {
        if matches!(self, Self::Parameter) {
            2
        } else {
            1
        }
    }
}

impl<'a> TrinoUri<'a> {
    pub(super) fn parse(
        source: &'a str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<Self>, KernelFailure> {
        let delimiter = find(source, b":/?#", work)?;
        let (scheme, remainder) = match delimiter {
            Some(index) if source.as_bytes()[index] == b':' => {
                let scheme = &source[..index];
                if !valid_scheme(scheme, work)? {
                    return Ok(None);
                }
                (scheme, &source[index + 1..])
            }
            _ => ("", source),
        };
        let (body, fragment) = split(remainder, b'#', work)?;
        let fragment_text = fragment.unwrap_or_default();
        if !valid_component(fragment_text, Component::Uric, work)? {
            return Ok(None);
        }
        let mut uri = Self {
            scheme,
            host: "",
            port: None,
            path: "",
            query: None,
            fragment: fragment_text,
        };
        if !scheme.is_empty() && !body.starts_with('/') {
            // Opaque scheme-specific parts have no hierarchical path or query.
            return Ok(
                (!body.is_empty() && valid_component(body, Component::Uric, work)?).then_some(uri),
            );
        }

        let (hierarchy, query) = split(body, b'?', work)?;
        if !valid_component(query.unwrap_or_default(), Component::Uric, work)? {
            return Ok(None);
        }
        uri.query = query;
        if let Some(after_slashes) = hierarchy.strip_prefix("//") {
            let end = find(after_slashes, b"/", work)?.unwrap_or(after_slashes.len());
            let authority = &after_slashes[..end];
            uri.path = &after_slashes[end..];
            if authority.is_empty() {
                if uri.path.is_empty() && query.is_none() && fragment.is_none() {
                    return Ok(None);
                }
            } else if let Some((host, port)) = server_authority(authority, work)? {
                uri.host = host;
                uri.port = port;
            } else if !valid_component(authority, Component::Registry, work)? {
                return Ok(None);
            }
        } else {
            uri.path = hierarchy;
        }
        Ok(valid_component(uri.path, Component::Path, work)?.then_some(uri))
    }

    pub(super) fn text(&self, part: TrinoUrlPart) -> &'a str {
        match part {
            TrinoUrlPart::Protocol => self.scheme,
            TrinoUrlPart::Host => self.host,
            TrinoUrlPart::Path => self.path,
            TrinoUrlPart::Query => self.query.unwrap_or_default(),
            TrinoUrlPart::Fragment => self.fragment,
            _ => unreachable!("URI text part is selected by its function overload"),
        }
    }

    pub(super) fn raw_parameter(
        self,
        key: &str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<&'a str>, KernelFailure> {
        let Some(mut query) = self.query else {
            return Ok(None);
        };
        loop {
            let (entry, next) = split(query, b'&', work)?;
            let (candidate, value) = split(entry, b'=', work)?;
            if same_bytes(candidate, key, work)? {
                return Ok(Some(value.unwrap_or_default()));
            }
            let Some(next) = next else {
                return Ok(None);
            };
            query = next;
        }
    }
}

fn find(
    text: &str,
    delimiters: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<usize>, KernelFailure> {
    for (index, byte) in text.bytes().enumerate() {
        work.step()?;
        if delimiters.contains(&byte) {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn split<'a>(
    text: &'a str,
    delimiter: u8,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(&'a str, Option<&'a str>), KernelFailure> {
    Ok(match find(text, &[delimiter], work)? {
        Some(index) => (&text[..index], Some(&text[index + 1..])),
        None => (text, None),
    })
}

fn same_bytes(
    left: &str,
    right: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (l, r) in left.bytes().zip(right.bytes()) {
        work.step()?;
        if l != r {
            return Ok(false);
        }
    }
    Ok(true)
}

fn valid_scheme(scheme: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<bool, KernelFailure> {
    let mut bytes = scheme.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic()) {
        return Ok(false);
    }
    for byte in bytes {
        work.step()?;
        if !byte.is_ascii_alphanumeric() && !matches!(byte, b'+' | b'-' | b'.') {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy)]
enum Component {
    Uric,
    Path,
    Registry,
    UserInfo,
}

fn valid_component(
    component: &str,
    grammar: Component,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    let mut chars = component.chars();
    while let Some(ch) = chars.next() {
        work.step()?;
        if ch == '%' {
            for _ in 0..2 {
                work.step()?;
                if !chars.next().is_some_and(|digit| digit.is_ascii_hexdigit()) {
                    return Ok(false);
                }
            }
        } else if ch.is_ascii_alphanumeric()
            || matches!(ch, '-' | '_' | '.' | '!' | '~' | '*' | '\'' | '(' | ')')
            || matches!(ch, ';' | ':' | '&' | '=' | '+' | '$' | ',')
            || (!ch.is_ascii() && !ch.is_whitespace() && !ch.is_control())
        {
            continue;
        } else if !(matches!(
            grammar,
            Component::Uric | Component::Path | Component::Registry
        ) && ch == '@'
            || matches!(grammar, Component::Uric | Component::Path) && ch == '/'
            || matches!(grammar, Component::Uric) && matches!(ch, '?' | '[' | ']'))
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn server_authority<'a>(
    authority: &'a str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<(&'a str, Option<i64>)>, KernelFailure> {
    let (first, after_user) = split(authority, b'@', work)?;
    let server = if let Some(server) = after_user {
        if !valid_component(first, Component::UserInfo, work)? {
            return Ok(None);
        }
        server
    } else {
        first
    };
    let (host, port) = if let Some(bracketed) = server.strip_prefix('[') {
        let Some(end) = find(bracketed, b"]", work)? else {
            return Ok(None);
        };
        let (address, scope) = split(&bracketed[..end], b'%', work)?;
        if !valid_ipv6(address, work)? {
            return Ok(None);
        }
        if let Some(scope) = scope {
            if scope.is_empty() {
                return Ok(None);
            }
            for byte in scope.bytes() {
                work.step()?;
                if !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'.') {
                    return Ok(None);
                }
            }
        }
        let host_end = end + 2;
        let suffix = &server[host_end..];
        if suffix.is_empty() {
            (&server[..host_end], None)
        } else if let Some(port) = suffix.strip_prefix(':') {
            (&server[..host_end], Some(port))
        } else {
            return Ok(None);
        }
    } else {
        let (host, port) = split(server, b':', work)?;
        if !valid_host(host, work)? {
            return Ok(None);
        }
        (host, port)
    };
    let mut value = None;
    if let Some(port) = port {
        for byte in port.bytes() {
            work.step()?;
            if !byte.is_ascii_digit() {
                return Ok(None);
            }
            let current: i32 = value.unwrap_or(0);
            let Some(next) = current
                .checked_mul(10)
                .and_then(|x| x.checked_add(i32::from(byte - b'0')))
            else {
                return Ok(None);
            };
            value = Some(next);
        }
    }
    Ok(Some((host, value.map(i64::from))))
}

fn valid_host(host: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<bool, KernelFailure> {
    let mut labels = host.strip_suffix('.').unwrap_or(host);
    let mut label_count = 0;
    let mut ipv4 = !host.ends_with('.');
    loop {
        let (label, next) = split(labels, b'.', work)?;
        let bytes = label.as_bytes();
        if !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
            || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        {
            return Ok(false);
        }
        let mut octet = Some(0u8);
        for byte in label.bytes() {
            work.step()?;
            if !byte.is_ascii_alphanumeric() && byte != b'-' {
                return Ok(false);
            }
            octet = octet
                .filter(|_| byte.is_ascii_digit())
                .and_then(|value| value.checked_mul(10))
                .and_then(|value| value.checked_add(byte - b'0'));
        }
        label_count += 1;
        ipv4 &= octet.is_some();
        let Some(next) = next else {
            return Ok(label_count == 1
                || bytes[0].is_ascii_alphabetic()
                || ipv4 && label_count == 4);
        };
        labels = next;
    }
}

fn valid_ipv6(address: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<bool, KernelFailure> {
    let mut colon = None;
    for (index, byte) in address.bytes().enumerate() {
        work.step()?;
        if byte == b':' {
            colon = Some(index);
        }
    }
    let Some(colon) = colon else {
        return Ok(false);
    };
    if colon >= 38 {
        return Ok(false);
    }
    let tail = &address[colon + 1..];
    if find(tail, b".", work)?.is_none() {
        return Ok(address.len() <= 39 && address.parse::<std::net::Ipv6Addr>().is_ok());
    }
    // Java accepts zero-padded IPv4 octets in the final 32 bits of an IPv6
    // literal. Validate the octets, then give Rust's IPv6 parser their hex words.
    let mut octets = [0; 4];
    let mut remaining = Some(tail);
    for octet in &mut octets {
        let Some(part) = remaining else {
            return Ok(false);
        };
        let (digits, next) = split(part, b'.', work)?;
        let Some(value) = ipv4_octet(digits, work)? else {
            return Ok(false);
        };
        *octet = value;
        remaining = next;
    }
    if remaining.is_some() {
        return Ok(false);
    }

    use std::io::Write;
    let mut normalized = [0; 48];
    let (prefix, words) = normalized.split_at_mut(colon + 1);
    prefix.copy_from_slice(&address.as_bytes()[..colon + 1]);
    let mut writer = words;
    write!(
        writer,
        "{:x}:{:x}",
        u16::from_be_bytes([octets[0], octets[1]]),
        u16::from_be_bytes([octets[2], octets[3]]),
    )
    .expect("two hexadecimal words fit the fixed buffer");
    let length = 48 - writer.len();
    let address = std::str::from_utf8(&normalized[..length])
        .expect("hexadecimal address and IPv6 prefix are valid UTF-8");
    Ok(address.parse::<std::net::Ipv6Addr>().is_ok())
}

fn ipv4_octet(
    digits: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<u8>, KernelFailure> {
    if digits.is_empty() {
        return Ok(None);
    }
    let mut number = 0u8;
    for byte in digits.bytes() {
        work.step()?;
        if !byte.is_ascii_digit() {
            return Ok(None);
        }
        let Some(value) = number
            .checked_mul(10)
            .and_then(|x| x.checked_add(byte - b'0'))
        else {
            return Ok(None);
        };
        number = value;
    }
    Ok(Some(number))
}
