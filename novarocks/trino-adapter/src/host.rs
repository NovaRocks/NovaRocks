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

use axum::{
    extract::Request,
    http::{StatusCode, header::HOST},
    middleware::Next,
    response::Response,
};

/// The accepted authority yields a bounded URI after canonicalizing its numeric port.
pub(crate) fn base_uri(value: &str) -> Option<String> {
    let (name, port) = if let Some(rest) = value.strip_prefix('[') {
        let end = rest.find(']')?;
        let name = &value[..end + 2];
        let suffix = &rest[end + 1..];
        if suffix.is_empty() {
            (name, None)
        } else {
            (name, Some(suffix.strip_prefix(':')?))
        }
    } else if let Some((name, port)) = value.split_once(':') {
        (name, Some(port))
    } else {
        (value, None)
    };
    if !(name.eq_ignore_ascii_case("localhost") || name == "127.0.0.1" || name == "[::1]") {
        return None;
    }
    match port {
        Some(port) if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) => {
            Some(format!("http://{name}:{}", port.parse::<u16>().ok()?))
        }
        Some(_) => None,
        None => Some(format!("http://{name}")),
    }
}

pub(crate) async fn require_local_host(
    request: Request,
    next: Next,
) -> Result<Response, (StatusCode, &'static str)> {
    let mut hosts = request.headers().get_all(HOST).iter();
    if !hosts
        .next()
        .and_then(|h| h.to_str().ok())
        .and_then(base_uri)
        .is_some()
        || hosts.next().is_some()
    {
        return Err((
            StatusCode::FORBIDDEN,
            "Host must name localhost, 127.0.0.1, or [::1]",
        ));
    }
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepted_port_text_is_normalized_without_a_new_length_limit() {
        assert_eq!(
            base_uri("LOCALHOST:0000008080").as_deref(),
            Some("http://LOCALHOST:8080")
        );
        let host = format!("127.0.0.1:{}65535", "0".repeat(128));
        assert_eq!(base_uri(&host).unwrap(), "http://127.0.0.1:65535");
        assert_eq!(base_uri("[::1]:00080").as_deref(), Some("http://[::1]:80"));
    }
    #[test]
    fn accepts_only_local_authorities() {
        for host in [
            "localhost",
            "LOCALHOST:8080",
            "127.0.0.1:65535",
            "[::1]",
            "[::1]:8080",
        ] {
            assert!(base_uri(host).is_some(), "{host}");
        }
        for host in [
            "",
            "evil.example",
            "evil.example:8080",
            "localhost.evil",
            "localhost.",
            "user@localhost",
            "localhost:",
            "localhost:65536",
            "127.0.0.1:abc",
            "[::1]evil",
            " localhost",
            "::1",
        ] {
            assert!(base_uri(host).is_none(), "{host}");
        }
    }
}
