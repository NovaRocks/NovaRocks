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

//! A deliberately small, loopback-only S3 compatibility fixture for system tests.
//!
//! This is not an S3 implementation and must never be used outside tests.  It is
//! intentionally bounded and authenticates only the public access-key identifier:
//! the fixture extracts the key id from a SigV4-style request, but neither verifies
//! nor records signatures, headers, query parameters, or request bodies.

use anyhow::{Context, Result, ensure};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_HEADER_BYTES: usize = 64 * 1024;
// Keep the request framing buffer bounded even when a client uses HTTP/1.1
// chunked transfer encoding. The decoded object remains subject to the
// per-fixture object-size limit below.
const MAX_CHUNKED_FRAMING_OVERHEAD_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_OBJECTS: usize = 128;
const DEFAULT_MAX_OBJECT_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_REQUESTS: usize = 4_096;
const DEFAULT_MAX_REQUEST_LOG_ENTRIES: usize = 1_024;
const DEFAULT_MAX_CONNECTIONS: usize = 16;

/// Hard limits for one [`LoopbackS3Fixture`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoopbackS3Config {
    /// The only credential fact this fixture checks. Secret material is never
    /// accepted by the fixture, preventing it from becoming an identity input or
    /// from appearing in request logs.
    pub access_key_id: String,
    pub max_objects: usize,
    pub max_object_bytes: usize,
    pub max_requests: usize,
    pub max_request_log_entries: usize,
    pub max_connections: usize,
}

impl LoopbackS3Config {
    pub fn for_access_key(access_key_id: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            max_objects: DEFAULT_MAX_OBJECTS,
            max_object_bytes: DEFAULT_MAX_OBJECT_BYTES,
            max_requests: DEFAULT_MAX_REQUESTS,
            max_request_log_entries: DEFAULT_MAX_REQUEST_LOG_ENTRIES,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.access_key_id.trim().is_empty(),
            "loopback S3 fixture access key id must not be empty"
        );
        ensure!(
            self.max_objects > 0,
            "loopback S3 fixture max_objects must be positive"
        );
        ensure!(
            self.max_object_bytes > 0,
            "loopback S3 fixture max_object_bytes must be positive"
        );
        ensure!(
            self.max_requests > 0,
            "loopback S3 fixture max_requests must be positive"
        );
        ensure!(
            self.max_request_log_entries > 0,
            "loopback S3 fixture max_request_log_entries must be positive"
        );
        ensure!(
            self.max_connections > 0,
            "loopback S3 fixture max_connections must be positive"
        );
        Ok(())
    }
}

/// A non-secret audit record. It deliberately omits raw headers, query strings,
/// request bodies, and credential secret/signature fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoopbackS3Request {
    pub method: String,
    pub path: String,
    pub credential_key_id: Option<String>,
    pub status: u16,
}

/// A bounded fixture object snapshot used only to prepare a cross-endpoint
/// cache-isolation corpus before the system scenario starts reading it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoopbackS3Object {
    pub bucket: String,
    pub key: String,
    pub bytes: Vec<u8>,
}

/// A bounded in-memory, HTTP-only S3 fixture bound exclusively to `127.0.0.1`.
pub struct LoopbackS3Fixture {
    endpoint: String,
    state: Arc<SharedState>,
    shutdown: Arc<AtomicBool>,
    server_thread: Option<JoinHandle<()>>,
}

impl LoopbackS3Fixture {
    pub fn start(config: LoopbackS3Config) -> Result<Self> {
        config.validate()?;
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .context("bind loopback S3 fixture listener")?;
        listener
            .set_nonblocking(true)
            .context("set loopback S3 fixture listener nonblocking")?;
        let address = listener
            .local_addr()
            .context("read loopback S3 fixture listener address")?;
        ensure!(
            address.ip().is_loopback(),
            "loopback S3 fixture bound a non-loopback address"
        );

        let state = Arc::new(SharedState {
            config,
            objects: Mutex::new(BTreeMap::new()),
            requests: Mutex::new(Vec::new()),
            accepted_requests: AtomicU64::new(0),
            active_connections: AtomicUsize::new(0),
        });
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_state = Arc::clone(&state);
        let server_shutdown = Arc::clone(&shutdown);
        let server_thread = thread::Builder::new()
            .name("loopback-s3-fixture".to_string())
            .spawn(move || serve(listener, server_state, server_shutdown))
            .context("start loopback S3 fixture listener")?;

        Ok(Self {
            endpoint: format!("http://{address}"),
            state,
            shutdown,
            server_thread: Some(server_thread),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn request_log(&self) -> Vec<LoopbackS3Request> {
        self.state
            .requests
            .lock()
            .expect("loopback S3 fixture request log lock poisoned")
            .clone()
    }

    pub fn request_count(&self) -> u64 {
        self.state.accepted_requests.load(Ordering::Relaxed)
    }

    pub fn object_snapshot_for_test(&self) -> Vec<LoopbackS3Object> {
        self.state
            .objects
            .lock()
            .expect("loopback S3 fixture object lock poisoned")
            .iter()
            .filter_map(|(id, object)| {
                let (bucket, key) = id.split_once('/')?;
                Some(LoopbackS3Object {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    bytes: object.bytes.clone(),
                })
            })
            .collect()
    }

    pub fn replace_object_for_test(&self, object: LoopbackS3Object) -> Result<()> {
        ensure!(
            !object.bucket.trim().is_empty() && !object.key.trim().is_empty(),
            "loopback S3 test object bucket and key must not be empty"
        );
        ensure!(
            object.bytes.len() <= self.state.config.max_object_bytes,
            "loopback S3 test object exceeds configured object limit"
        );
        let mut objects = self
            .state
            .objects
            .lock()
            .expect("loopback S3 fixture object lock poisoned");
        let id = object_id(&object.bucket, &object.key);
        ensure!(
            objects.contains_key(&id) || objects.len() < self.state.config.max_objects,
            "loopback S3 test object exceeds configured object count"
        );
        let etag = format!("loopback-test-{:016x}", object.bytes.len());
        objects.insert(
            id,
            StoredObject {
                bytes: object.bytes,
                etag,
            },
        );
        Ok(())
    }
}

impl Drop for LoopbackS3Fixture {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(server_thread) = self.server_thread.take() {
            let _ = server_thread.join();
        }
    }
}

struct SharedState {
    config: LoopbackS3Config,
    objects: Mutex<BTreeMap<String, StoredObject>>,
    requests: Mutex<Vec<LoopbackS3Request>>,
    accepted_requests: AtomicU64,
    active_connections: AtomicUsize,
}

struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
}

fn serve(listener: TcpListener, state: Arc<SharedState>, shutdown: Arc<AtomicBool>) {
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, address)) => {
                if !address.ip().is_loopback() {
                    let _ = write_response(stream, 403, &[], &[]);
                    continue;
                }
                let active = state.active_connections.fetch_add(1, Ordering::AcqRel);
                if active >= state.config.max_connections {
                    state.active_connections.fetch_sub(1, Ordering::AcqRel);
                    let _ = write_response(stream, 503, &[], &[]);
                    continue;
                }
                let request_state = Arc::clone(&state);
                thread::spawn(move || {
                    let _ = handle_connection(stream, Arc::clone(&request_state));
                    request_state
                        .active_connections
                        .fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => break,
        }
    }
}

fn handle_connection(mut stream: TcpStream, state: Arc<SharedState>) -> Result<()> {
    // Accepted sockets inherit the nonblocking listener mode on the supported
    // platforms. This fixture performs bounded blocking reads below, so make
    // the per-connection mode explicit before installing timeouts.
    stream
        .set_nonblocking(false)
        .context("set loopback S3 connection blocking mode")?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("set loopback S3 fixture read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("set loopback S3 fixture write timeout")?;
    let request = match read_request(&mut stream, state.config.max_object_bytes) {
        Ok(request) => request,
        Err(error) => {
            // The parser never includes raw headers, bodies, query strings, or
            // signatures in its errors. Keep this test-only marker so a system
            // scenario can distinguish fixture framing rejection from an S3
            // authorization or storage failure without exposing credentials.
            eprintln!(
                "{} reason={error:#}",
                novarocks_failpoint::observation_marker::LOOPBACK_S3_REJECTED_REQUEST
            );
            write_response(stream, 400, &[], &[])?;
            return Ok(());
        }
    };
    let ordinal = state.accepted_requests.fetch_add(1, Ordering::AcqRel) + 1;
    if ordinal > state.config.max_requests as u64 {
        log_request(&state, &request, 429);
        write_response(stream, 429, &[], &[])?;
        return Ok(());
    }
    let credential_key_id = request.credential_key_id();
    if credential_key_id.as_deref() != Some(state.config.access_key_id.as_str()) {
        log_request_with_key(&state, &request, credential_key_id, 403);
        write_response(stream, 403, &[], &[])?;
        return Ok(());
    }

    let response = dispatch_request(&state, &request, ordinal)?;
    log_request_with_key(&state, &request, credential_key_id, response.status);
    write_response(stream, response.status, &response.headers, &response.body)
}

fn log_request(state: &SharedState, request: &ParsedRequest, status: u16) {
    log_request_with_key(state, request, request.credential_key_id(), status);
}

fn log_request_with_key(
    state: &SharedState,
    request: &ParsedRequest,
    credential_key_id: Option<String>,
    status: u16,
) {
    let mut requests = state
        .requests
        .lock()
        .expect("loopback S3 fixture request log lock poisoned");
    if requests.len() == state.config.max_request_log_entries {
        requests.remove(0);
    }
    requests.push(LoopbackS3Request {
        method: request.method.clone(),
        path: request.path.clone(),
        credential_key_id,
        status,
    });
}

struct ParsedRequest {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl ParsedRequest {
    fn credential_key_id(&self) -> Option<String> {
        self.headers
            .get("authorization")
            .and_then(|value| credential_key_from_authorization(value))
            .or_else(|| {
                self.headers
                    .get("x-amz-credential")
                    .and_then(|value| credential_key_from_value(value))
            })
            .or_else(|| {
                self.query
                    .get("X-Amz-Credential")
                    .or_else(|| self.query.get("x-amz-credential"))
                    .and_then(|value| credential_key_from_value(value))
            })
    }
}

fn read_request(stream: &mut TcpStream, max_object_bytes: usize) -> Result<ParsedRequest> {
    let mut bytes = Vec::with_capacity(4096);
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream
            .read(&mut buffer)
            .context("read loopback S3 request")?;
        ensure!(read > 0, "loopback S3 request closed before headers");
        bytes.extend_from_slice(&buffer[..read]);
        ensure!(
            bytes.len() <= MAX_HEADER_BYTES,
            "loopback S3 request headers exceed limit"
        );
        if let Some(end) = find_header_end(&bytes) {
            break end;
        }
    };
    let header = std::str::from_utf8(&bytes[..header_end]).context("decode loopback S3 headers")?;
    let mut lines = header.split("\r\n");
    let request_line = lines.next().context("loopback S3 request line missing")?;
    let mut request_parts = request_line.split_ascii_whitespace();
    let method = request_parts
        .next()
        .context("loopback S3 request method missing")?
        .to_ascii_uppercase();
    let request_target = request_parts
        .next()
        .context("loopback S3 request target missing")?;
    let version = request_parts
        .next()
        .context("loopback S3 request HTTP version missing")?;
    ensure!(
        matches!(version, "HTTP/1.0" | "HTTP/1.1"),
        "loopback S3 unsupported HTTP version"
    );
    ensure!(
        request_parts.next().is_none(),
        "loopback S3 malformed request line"
    );
    let (path, query) = split_target(request_target)?;
    let mut headers = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .context("loopback S3 malformed request header")?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    let mut request_bytes = bytes[(header_end + 4)..].to_vec();
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|value| transfer_encoding_is_chunked(value))
    {
        read_chunked_body(stream, &mut request_bytes, max_object_bytes)?
    } else {
        let content_length = headers
            .get("content-length")
            .map(|value| {
                value
                    .parse::<usize>()
                    .context("loopback S3 invalid content length")
            })
            .transpose()?
            .unwrap_or(0);
        ensure!(
            content_length <= max_object_bytes,
            "loopback S3 request body exceeds configured object limit"
        );
        read_content_length_body(stream, &mut request_bytes, content_length)?
    };
    Ok(ParsedRequest {
        method,
        path,
        query,
        headers,
        body,
    })
}

fn transfer_encoding_is_chunked(value: &str) -> bool {
    value
        .split(',')
        .map(str::trim)
        .any(|encoding| encoding.eq_ignore_ascii_case("chunked"))
}

fn read_content_length_body(
    stream: &mut TcpStream,
    request_bytes: &mut Vec<u8>,
    content_length: usize,
) -> Result<Vec<u8>> {
    while request_bytes.len() < content_length {
        let remaining = content_length - request_bytes.len();
        read_request_bytes(stream, request_bytes, remaining)?;
    }
    ensure!(
        request_bytes.len() == content_length,
        "loopback S3 request contains pipelined body bytes"
    );
    Ok(std::mem::take(request_bytes))
}

fn read_chunked_body(
    stream: &mut TcpStream,
    request_bytes: &mut Vec<u8>,
    max_object_bytes: usize,
) -> Result<Vec<u8>> {
    let max_framed_bytes = max_object_bytes.saturating_add(MAX_CHUNKED_FRAMING_OVERHEAD_BYTES);
    let mut offset = 0;
    let mut body = Vec::new();
    loop {
        let chunk_size_line = read_crlf_line(stream, request_bytes, &mut offset, max_framed_bytes)?;
        let chunk_size = std::str::from_utf8(chunk_size_line)
            .context("decode loopback S3 chunk size")?
            .split(';')
            .next()
            .expect("split always yields at least one segment")
            .trim();
        let chunk_size =
            usize::from_str_radix(chunk_size, 16).context("loopback S3 invalid chunk size")?;
        if chunk_size == 0 {
            loop {
                let trailer = read_crlf_line(stream, request_bytes, &mut offset, max_framed_bytes)?;
                if trailer.is_empty() {
                    ensure!(
                        offset == request_bytes.len(),
                        "loopback S3 request contains pipelined body bytes"
                    );
                    return Ok(body);
                }
                let trailer =
                    std::str::from_utf8(trailer).context("decode loopback S3 chunk trailer")?;
                ensure!(trailer.contains(':'), "loopback S3 malformed chunk trailer");
            }
        }
        ensure!(
            chunk_size <= max_object_bytes.saturating_sub(body.len()),
            "loopback S3 request body exceeds configured object limit"
        );
        let required = offset
            .checked_add(chunk_size)
            .and_then(|value| value.checked_add(2))
            .context("loopback S3 chunk size overflows request framing")?;
        while request_bytes.len() < required {
            read_request_bytes(stream, request_bytes, required - request_bytes.len())?;
            ensure!(
                request_bytes.len() <= max_framed_bytes,
                "loopback S3 chunked request framing exceeds limit"
            );
        }
        body.extend_from_slice(&request_bytes[offset..offset + chunk_size]);
        offset += chunk_size;
        ensure!(
            request_bytes[offset..offset + 2] == *b"\r\n",
            "loopback S3 chunk data is missing its terminator"
        );
        offset += 2;
    }
}

fn read_crlf_line<'a>(
    stream: &mut TcpStream,
    request_bytes: &'a mut Vec<u8>,
    offset: &mut usize,
    max_framed_bytes: usize,
) -> Result<&'a [u8]> {
    loop {
        if let Some(line_end) = request_bytes[*offset..]
            .windows(2)
            .position(|window| window == b"\r\n")
        {
            let start = *offset;
            let end = start + line_end;
            *offset = end + 2;
            return Ok(&request_bytes[start..end]);
        }
        ensure!(
            request_bytes.len() < max_framed_bytes,
            "loopback S3 chunked request framing exceeds limit"
        );
        let remaining = max_framed_bytes - request_bytes.len();
        read_request_bytes(stream, request_bytes, remaining)?;
    }
}

fn read_request_bytes(
    stream: &mut TcpStream,
    request_bytes: &mut Vec<u8>,
    remaining: usize,
) -> Result<()> {
    let mut buffer = [0_u8; 4096];
    let read_len = remaining.min(buffer.len());
    let read = stream
        .read(&mut buffer[..read_len])
        .context("read loopback S3 request body")?;
    ensure!(read > 0, "loopback S3 request closed before body completed");
    request_bytes.extend_from_slice(&buffer[..read]);
    Ok(())
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn split_target(target: &str) -> Result<(String, BTreeMap<String, String>)> {
    ensure!(
        target.starts_with('/'),
        "loopback S3 request target must be absolute path"
    );
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut parsed = BTreeMap::new();
    for segment in query.split('&').filter(|segment| !segment.is_empty()) {
        let (key, value) = segment.split_once('=').unwrap_or((segment, ""));
        parsed.insert(percent_decode(key)?, percent_decode(value)?);
    }
    Ok((percent_decode(path)?, parsed))
}

fn percent_decode(input: &str) -> Result<String> {
    let mut decoded = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let high = hex_nibble(bytes[index + 1]).context("invalid percent escape")?;
                let low = hex_nibble(bytes[index + 2]).context("invalid percent escape")?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).context("loopback S3 request target is not UTF-8")
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn credential_key_from_authorization(value: &str) -> Option<String> {
    if let Some((_, credential)) = value.split_once("Credential=") {
        return credential_key_from_value(credential);
    }
    value
        .strip_prefix("AWS ")
        .and_then(|credential| credential.split_once(':').map(|(key, _)| key.to_string()))
}

fn credential_key_from_value(value: &str) -> Option<String> {
    let key = value
        .trim()
        .split(['/', ',', ' '])
        .next()
        .filter(|key| !key.is_empty())?;
    Some(key.to_string())
}

struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

fn dispatch_request(
    state: &SharedState,
    request: &ParsedRequest,
    ordinal: u64,
) -> Result<Response> {
    let (bucket, key) = parse_s3_path(&request.path)?;
    if request.method == "GET"
        && (request.query.contains_key("list-type") || request.query.contains_key("prefix"))
    {
        return list_response(state, bucket, request);
    }
    match (request.method.as_str(), key) {
        ("PUT", Some(key)) => put_response(state, bucket, key, request, ordinal),
        ("GET", Some(key)) => get_response(state, bucket, key, request),
        ("HEAD", Some(key)) => head_response(state, bucket, key),
        ("DELETE", Some(key)) => delete_response(state, bucket, key),
        ("HEAD", None) => Ok(empty_response(200)),
        ("GET", None) if request.query.contains_key("location") => Ok(xml_response(
            200,
            "<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">us-east-1</LocationConstraint>"
                .to_string(),
        )),
        ("PUT", None) => Ok(empty_response(200)),
        _ => Ok(empty_response(405)),
    }
}

fn parse_s3_path(path: &str) -> Result<(&str, Option<&str>)> {
    let path = path.trim_start_matches('/');
    ensure!(!path.is_empty(), "loopback S3 request missing bucket");
    let (bucket, key) = path
        .split_once('/')
        .map_or((path, None), |(bucket, key)| (bucket, Some(key)));
    ensure!(!bucket.is_empty(), "loopback S3 request missing bucket");
    Ok((bucket, key.filter(|key| !key.is_empty())))
}

fn object_id(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

fn put_response(
    state: &SharedState,
    bucket: &str,
    key: &str,
    request: &ParsedRequest,
    ordinal: u64,
) -> Result<Response> {
    ensure!(
        request.body.len() <= state.config.max_object_bytes,
        "loopback S3 object exceeds configured object limit"
    );
    let mut objects = state
        .objects
        .lock()
        .expect("loopback S3 fixture object lock poisoned");
    let id = object_id(bucket, key);
    if !objects.contains_key(&id) && objects.len() >= state.config.max_objects {
        return Ok(empty_response(507));
    }
    let etag = format!("loopback-{ordinal:016x}-{:016x}", request.body.len());
    objects.insert(
        id,
        StoredObject {
            bytes: request.body.clone(),
            etag: etag.clone(),
        },
    );
    Ok(Response {
        status: 200,
        headers: vec![("ETag".to_string(), format!("\"{etag}\""))],
        body: Vec::new(),
    })
}

fn get_response(
    state: &SharedState,
    bucket: &str,
    key: &str,
    request: &ParsedRequest,
) -> Result<Response> {
    let objects = state
        .objects
        .lock()
        .expect("loopback S3 fixture object lock poisoned");
    let Some(object) = objects.get(&object_id(bucket, key)) else {
        return Ok(empty_response(404));
    };
    let total_length = object.bytes.len();
    let range = request
        .headers
        .get("range")
        .map(|value| parse_byte_range(value, total_length))
        .transpose()?;
    let (status, start, end) = match range {
        Some((start, end)) => (206, start, end),
        None => (200, 0, total_length),
    };
    let mut headers = object_headers(object);
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case("content-length"));
    headers.push(("Content-Length".to_string(), (end - start).to_string()));
    if status == 206 {
        headers.push((
            "Content-Range".to_string(),
            format!("bytes {}-{}/{}", start, end.saturating_sub(1), total_length),
        ));
    }
    Ok(Response {
        status,
        headers,
        body: object.bytes[start..end].to_vec(),
    })
}

fn head_response(state: &SharedState, bucket: &str, key: &str) -> Result<Response> {
    let objects = state
        .objects
        .lock()
        .expect("loopback S3 fixture object lock poisoned");
    Ok(objects
        .get(&object_id(bucket, key))
        .map(|object| Response {
            status: 200,
            headers: object_headers(object),
            body: Vec::new(),
        })
        .unwrap_or_else(|| empty_response(404)))
}

fn delete_response(state: &SharedState, bucket: &str, key: &str) -> Result<Response> {
    state
        .objects
        .lock()
        .expect("loopback S3 fixture object lock poisoned")
        .remove(&object_id(bucket, key));
    Ok(empty_response(204))
}

fn object_headers(object: &StoredObject) -> Vec<(String, String)> {
    vec![
        ("Content-Length".to_string(), object.bytes.len().to_string()),
        ("ETag".to_string(), format!("\"{}\"", object.etag)),
        (
            "Last-Modified".to_string(),
            "Thu, 01 Jan 1970 00:00:00 GMT".to_string(),
        ),
    ]
}

fn parse_byte_range(value: &str, total_length: usize) -> Result<(usize, usize)> {
    let value = value
        .strip_prefix("bytes=")
        .context("loopback S3 only supports bytes ranges")?;
    let (start, end) = value
        .split_once('-')
        .context("loopback S3 malformed byte range")?;
    ensure!(
        !start.is_empty(),
        "loopback S3 suffix byte ranges are unsupported"
    );
    let start = start
        .parse::<usize>()
        .context("loopback S3 invalid byte range start")?;
    ensure!(
        start < total_length,
        "loopback S3 byte range start is past object end"
    );
    let end = if end.is_empty() {
        total_length
    } else {
        let inclusive_end = end
            .parse::<usize>()
            .context("loopback S3 invalid byte range end")?;
        ensure!(
            inclusive_end >= start,
            "loopback S3 byte range end precedes start"
        );
        inclusive_end.saturating_add(1).min(total_length)
    };
    Ok((start, end))
}

fn list_response(state: &SharedState, bucket: &str, request: &ParsedRequest) -> Result<Response> {
    let prefix = request
        .query
        .get("prefix")
        .map(String::as_str)
        .unwrap_or_default();
    let start_after = request
        .query
        .get("start-after")
        .or_else(|| request.query.get("marker"))
        .map(String::as_str)
        .unwrap_or_default();
    let prefix_id = format!("{bucket}/{prefix}");
    let start_after_id = format!("{bucket}/{start_after}");
    let bucket_prefix = format!("{bucket}/");
    let objects = state
        .objects
        .lock()
        .expect("loopback S3 fixture object lock poisoned");
    let contents = objects
        .range(prefix_id..)
        .take_while(|(id, _)| id.starts_with(&bucket_prefix))
        .filter(|(id, _)| id.starts_with(&format!("{bucket}/{prefix}")) && id.as_str() > start_after_id.as_str())
        .map(|(id, object)| {
            let key = id.strip_prefix(&bucket_prefix).unwrap_or(id);
            format!(
                "<Contents><Key>{}</Key><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                xml_escape(key),
                xml_escape(&object.etag),
                object.bytes.len()
            )
        })
        .collect::<String>();
    let key_count = contents.matches("<Contents>").count();
    let list_type_v2 = request
        .query
        .get("list-type")
        .is_some_and(|value| value == "2");
    let body = if list_type_v2 {
        format!(
            "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{key_count}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>",
            xml_escape(bucket),
            xml_escape(prefix),
        )
    } else {
        format!(
            "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><Marker>{}</Marker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>",
            xml_escape(bucket),
            xml_escape(prefix),
            xml_escape(start_after),
        )
    };
    Ok(xml_response(200, body))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_response(status: u16, body: String) -> Response {
    Response {
        status,
        headers: vec![("Content-Type".to_string(), "application/xml".to_string())],
        body: body.into_bytes(),
    }
}

fn empty_response(status: u16) -> Response {
    Response {
        status,
        headers: Vec::new(),
        body: Vec::new(),
    }
}

fn write_response(
    mut stream: TcpStream,
    status: u16,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        206 => "Partial Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        507 => "Insufficient Storage",
        _ => "Internal Server Error",
    };
    let mut response = format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\n");
    let has_content_length = headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    if !has_content_length {
        response.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    response.push_str("\r\n");
    stream
        .write_all(response.as_bytes())
        .context("write loopback S3 response headers")?;
    stream
        .write_all(body)
        .context("write loopback S3 response body")?;
    let _ = stream.shutdown(Shutdown::Both);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpStream;

    fn fixture() -> LoopbackS3Fixture {
        LoopbackS3Fixture::start(LoopbackS3Config::for_access_key("test-key"))
            .expect("start loopback fixture")
    }

    fn request(fixture: &LoopbackS3Fixture, raw: &str) -> (u16, BTreeMap<String, String>, Vec<u8>) {
        let address = fixture
            .endpoint()
            .strip_prefix("http://")
            .expect("http endpoint");
        let mut stream = TcpStream::connect(address).expect("connect fixture");
        stream.write_all(raw.as_bytes()).expect("write request");
        stream.shutdown(Shutdown::Write).expect("finish request");
        let mut reader = BufReader::new(stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).expect("read status");
        let status = status_line
            .split_ascii_whitespace()
            .nth(1)
            .expect("status value")
            .parse()
            .expect("numeric status");
        let mut headers = BTreeMap::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).expect("read response header");
            if line == "\r\n" {
                break;
            }
            let (name, value) = line.trim_end().split_once(':').expect("response header");
            headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
        }
        let content_length = headers
            .get("content-length")
            .expect("content length")
            .parse::<usize>()
            .expect("content length number");
        let mut body = vec![
            0;
            if raw.starts_with("HEAD ") {
                0
            } else {
                content_length
            }
        ];
        reader.read_exact(&mut body).expect("read response body");
        (status, headers, body)
    }

    fn signed(method: &str, target: &str, body: &str) -> String {
        format!(
            "{method} {target} HTTP/1.1\r\nHost: loopback\r\nAuthorization: AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    #[test]
    fn performs_bounded_path_style_s3_object_operations() {
        let fixture = fixture();
        let (status, _, _) = request(&fixture, &signed("PUT", "/bucket/data/file", "abcdef"));
        assert_eq!(status, 200);
        let (status, headers, body) = request(
            &fixture,
            "GET /bucket/data/file HTTP/1.1\r\nHost: loopback\r\nAuthorization: AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, Signature=must-not-log\r\nRange: bytes=2-4\r\n\r\n",
        );
        assert_eq!(status, 206);
        assert_eq!(
            headers.get("content-range").map(String::as_str),
            Some("bytes 2-4/6")
        );
        assert_eq!(body, b"cde");
        let (status, _, _) = request(&fixture, &signed("HEAD", "/bucket/data/file", ""));
        assert_eq!(status, 200);
        let (status, _, body) = request(
            &fixture,
            &signed("GET", "/bucket?list-type=2&prefix=data/", ""),
        );
        assert_eq!(status, 200);
        assert!(
            String::from_utf8(body)
                .expect("list XML")
                .contains("data/file")
        );
        let (status, _, _) = request(&fixture, &signed("DELETE", "/bucket/data/file", ""));
        assert_eq!(status, 204);
        let (status, _, _) = request(&fixture, &signed("GET", "/bucket/data/file", ""));
        assert_eq!(status, 404);
    }

    #[test]
    fn accepts_chunked_puts_sent_with_headers_and_body_in_one_write() {
        let fixture = fixture();
        let request_text = "PUT /bucket/data/chunked HTTP/1.1\r\nHost: loopback\r\nAuthorization: AWS4-HMAC-SHA256 Credential=test-key/20260829/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=must-not-log\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n3;extension=value\r\ndef\r\n0\r\n\r\n";
        let (status, _, _) = request(&fixture, request_text);
        assert_eq!(status, 200);
        let (status, _, body) = request(&fixture, &signed("GET", "/bucket/data/chunked", ""));
        assert_eq!(status, 200);
        assert_eq!(body, b"abcdef");
    }

    #[test]
    fn only_accepts_expected_key_id_and_log_omits_secret_material() {
        let fixture = fixture();
        let request_text = "GET /bucket/object?X-Amz-Signature=secret-signature HTTP/1.1\r\nHost: loopback\r\nAuthorization: AWS4-HMAC-SHA256 Credential=wrong-key/20260829/us-east-1/s3/aws4_request, Signature=must-not-log\r\n\r\n";
        let (status, _, _) = request(&fixture, request_text);
        assert_eq!(status, 403);
        let log = fixture.request_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].credential_key_id.as_deref(), Some("wrong-key"));
        assert_eq!(log[0].path, "/bucket/object");
        let rendered = format!("{log:?}");
        assert!(!rendered.contains("secret-signature"));
        assert!(!rendered.contains("must-not-log"));
    }

    #[test]
    fn rejects_configurations_that_remove_fixture_bounds() {
        let mut config = LoopbackS3Config::for_access_key("test-key");
        config.max_objects = 0;
        assert!(LoopbackS3Fixture::start(config).is_err());
    }
}
