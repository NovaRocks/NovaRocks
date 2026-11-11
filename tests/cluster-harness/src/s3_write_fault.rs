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

//! Loopback transport fault for the actual MinIO signed write. The selected
//! successful operation exits upstream before its downstream connection is
//! dropped, so any retry or HTTP refusal still belongs to the real clients.

use anyhow::{Context, Result, ensure};
use axum::body::Body;
use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, header};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use reqwest::{Client, Url};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, TcpListener};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context as PollContext, Poll};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinSet;
use tokio_stream::Stream;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum S3Operation {
    Put,
    Initiate,
    Part,
    Complete,
    Abort,
    Delete,
    Head,
    Get,
    Other,
}

#[derive(Clone, Debug)]
pub enum AckTarget {
    Object(String),
    /// Select one path from a private table's data prefix on first receipt.
    /// All subsequent matching is against that exact path.
    NextObjectUnder(String),
}

struct AckLoss {
    operation: S3Operation,
    target: AckTarget,
    dropped: bool,
    pause: Option<(PauseAfter, Arc<PausedResponse>)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseAfter {
    SuccessfulCreate,
    RetryPreconditionFailure,
}

#[derive(Default)]
struct PausedResponse {
    reached: Mutex<Option<String>>,
    observed: Condvar,
    resumed: Notify,
}

pub struct S3WritePause(Arc<PausedResponse>);

impl S3WritePause {
    pub fn wait_for_object(&self, timeout: Duration) -> Result<String> {
        let reached = self.0.reached.lock().expect("S3 response pause lock");
        let (reached, _) = self
            .0
            .observed
            .wait_timeout_while(reached, timeout, |path| path.is_none())
            .expect("S3 response pause lock");
        reached
            .clone()
            .context("exact MinIO write did not reach its selected response pause")
    }
}

impl Drop for S3WritePause {
    fn drop(&mut self) {
        self.0.resumed.notify_one();
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct S3WriteEvent {
    pub object_path: String,
    pub operation: S3Operation,
    pub upload_id_sha256: Option<String>,
    pub part_number: Option<u32>,
    pub condition_if_none_match_star: bool,
    pub create_marker: Option<CreateMarker>,
    pub readback_sha256: Option<String>,
    pub attempt: u64,
    pub status: Option<u16>,
    pub xml_code: Option<String>,
    pub upstream_response_complete: bool,
    pub downstream_response_dropped: bool,
    pub response_pause: Option<PauseAfter>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CreateMarker {
    pub id: String,
    pub identity: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct S3WriteProxyExit {
    pub accepted_connections: u64,
    pub exited_connections: u64,
    pub events: Vec<S3WriteEvent>,
}

type AttemptKey = (S3Operation, String, Option<String>, Option<u32>);

struct RecordedRequest {
    shared: Arc<Shared>,
    event: Option<S3WriteEvent>,
}

impl Drop for RecordedRequest {
    fn drop(&mut self) {
        if let Some(event) = self.event.take() {
            self.shared
                .observations
                .lock()
                .expect("S3 write observation lock")
                .events
                .push(event);
        }
    }
}

/// Observe the body while forwarding it. Hyper may stop polling after a
/// successful fixed-length response, so count that framing boundary too.
struct ObservedReadBody {
    upstream: Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + Send>>,
    recorded: RecordedRequest,
    length: Option<u64>,
    size: u64,
    digest: Sha256,
}

impl ObservedReadBody {
    fn record_complete(&mut self) {
        if let Some(event) = self.recorded.event.as_mut() {
            event.upstream_response_complete = true;
            event.readback_sha256 = Some(format!("{:x}", self.digest.clone().finalize()));
        }
    }
}

impl Stream for ObservedReadBody {
    type Item = std::result::Result<Bytes, reqwest::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut PollContext<'_>) -> Poll<Option<Self::Item>> {
        let result = self.upstream.as_mut().poll_next(cx);
        match &result {
            Poll::Ready(Some(Ok(chunk))) => {
                self.size += chunk.len() as u64;
                self.digest.update(chunk);
                if self.length == Some(self.size) {
                    self.record_complete();
                }
            }
            Poll::Ready(None) if self.length.is_none_or(|length| length == self.size) => {
                self.record_complete();
            }
            Poll::Ready(Some(Err(_))) => {
                if let Some(event) = self.recorded.event.as_mut() {
                    event.upstream_response_complete = false;
                    event.readback_sha256 = None;
                }
            }
            _ => {}
        }
        result
    }
}

#[derive(Default)]
struct Observations {
    ack_loss: Option<AckLoss>,
    attempts: BTreeMap<AttemptKey, u64>,
    events: Vec<S3WriteEvent>,
}

struct Shared {
    upstream: Url,
    client: Client,
    observations: Mutex<Observations>,
}

pub struct S3WriteFault {
    endpoint: String,
    shared: Arc<Shared>,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<Result<(u64, u64)>>>,
}

impl S3WriteFault {
    pub fn start(upstream: &str, timeout: Duration) -> Result<Self> {
        let upstream =
            Url::parse(upstream).map_err(|_| anyhow::anyhow!("invalid MinIO endpoint"))?;
        ensure!(
            upstream.scheme() == "http"
                && matches!(upstream.host_str(), Some("127.0.0.1" | "localhost"))
                && upstream.username().is_empty()
                && upstream.password().is_none()
                && upstream.path() == "/"
                && upstream.query().is_none()
                && upstream.fragment().is_none(),
            "S3 write proxy requires the original loopback HTTP endpoint"
        );
        ensure!(
            !timeout.is_zero(),
            "S3 write proxy requires a positive timeout"
        );
        let shared = Arc::new(Shared {
            upstream,
            client: Client::builder()
                .http1_only()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .timeout(timeout)
                .build()
                .context("build S3 write proxy HTTP client")?,
            observations: Mutex::new(Observations::default()),
        });
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (stop, stopped) = oneshot::channel();
        let owner = shared.clone();
        let worker = thread::Builder::new()
            .name("minio-write-fault-proxy".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                runtime.block_on(run(listener, owner, stopped))
            })
            .context("start MinIO write proxy listener owner")?;
        Ok(Self {
            endpoint,
            shared,
            stop: Some(stop),
            worker: Some(worker),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn arm_ack_loss(&self, operation: S3Operation, target: AckTarget) -> Result<()> {
        self.arm(operation, target, None)
    }

    pub fn arm_ack_loss_with_pause(
        &self,
        operation: S3Operation,
        target: AckTarget,
        phase: PauseAfter,
    ) -> Result<S3WritePause> {
        let pause = Arc::new(PausedResponse::default());
        self.arm(operation, target, Some((phase, pause.clone())))?;
        Ok(S3WritePause(pause))
    }

    fn arm(
        &self,
        operation: S3Operation,
        target: AckTarget,
        pause: Option<(PauseAfter, Arc<PausedResponse>)>,
    ) -> Result<()> {
        ensure!(
            matches!(operation, S3Operation::Put | S3Operation::Complete),
            "only single PUT and multipart Complete acknowledge creation"
        );
        let mut observed = self
            .shared
            .observations
            .lock()
            .expect("S3 write observation lock");
        ensure!(
            observed.ack_loss.as_ref().is_none_or(|fault| fault.dropped),
            "the prior S3 ACK fault is still armed"
        );
        observed.ack_loss = Some(AckLoss {
            operation,
            target,
            dropped: false,
            pause,
        });
        Ok(())
    }

    pub fn events(&self) -> Vec<S3WriteEvent> {
        self.shared
            .observations
            .lock()
            .expect("S3 write observation lock")
            .events
            .clone()
    }

    pub fn shutdown(&mut self) -> Result<S3WriteProxyExit> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let worker = self
            .worker
            .take()
            .context("S3 write proxy already exited")?;
        let (accepted_connections, exited_connections) = worker
            .join()
            .map_err(|_| anyhow::anyhow!("S3 write proxy worker panicked"))??;
        ensure!(
            accepted_connections == exited_connections,
            "S3 write proxy retained a connection after shutdown"
        );
        Ok(S3WriteProxyExit {
            accepted_connections,
            exited_connections,
            events: self.events(),
        })
    }
}

impl Drop for S3WriteFault {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self.shutdown();
        }
    }
}

async fn run(
    listener: TcpListener,
    shared: Arc<Shared>,
    mut stop: oneshot::Receiver<()>,
) -> Result<(u64, u64)> {
    let listener = tokio::net::TcpListener::from_std(listener)?;
    let (closing, _) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut accepted = 0;
    let mut exited = 0;
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => break,
            result = connections.join_next(), if !connections.is_empty() => {
                result.expect("nonempty connection set").context("S3 write proxy connection panicked")?;
                exited += 1;
            }
            result = listener.accept() => {
                let (stream, _) = result?;
                accepted += 1;
                let shared = shared.clone();
                let mut shutdown = closing.subscribe();
                connections.spawn(async move {
                    let service = service_fn(move |request| forward(shared.clone(), request));
                    let connection = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service);
                    tokio::pin!(connection);
                    tokio::select! {
                        _ = &mut connection => {}
                        _ = shutdown.changed() => {}
                    }
                });
            }
        }
    }
    drop(listener);
    let _ = closing.send(true);
    while let Some(result) = connections.join_next().await {
        result.context("S3 write proxy connection panicked during shutdown")?;
        exited += 1;
    }
    Ok((accepted, exited))
}

fn classify(method: &Method, url: &Url) -> (S3Operation, Option<String>, Option<u32>) {
    let mut uploads = false;
    let mut upload = None;
    let mut part = None;
    for (name, value) in url.query_pairs() {
        match name.as_ref() {
            "uploads" => uploads = true,
            "uploadId" => upload = Some(hash_upload(&value)),
            "partNumber" => part = value.parse().ok(),
            _ => {}
        }
    }
    let operation = match (method, uploads, upload.is_some(), part.is_some()) {
        (&Method::PUT, _, true, true) => S3Operation::Part,
        (&Method::PUT, _, _, _) => S3Operation::Put,
        (&Method::POST, true, _, _) => S3Operation::Initiate,
        (&Method::POST, _, true, _) => S3Operation::Complete,
        (&Method::DELETE, _, true, _) => S3Operation::Abort,
        (&Method::DELETE, _, _, _) => S3Operation::Delete,
        (&Method::HEAD, _, _, _) => S3Operation::Head,
        (&Method::GET, _, _, _) => S3Operation::Get,
        _ => S3Operation::Other,
    };
    (operation, upload, part)
}

fn hash_upload(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn xml_value<'a>(bytes: &'a [u8], name: &str) -> Option<&'a str> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (_, tail) = text.split_once(&format!("<{name}>"))?;
    let (value, _) = tail.split_once(&format!("</{name}>"))?;
    Some(value)
}

fn remove_hop_headers(headers: &mut HeaderMap) {
    for name in [
        "connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn transport_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "S3 write proxy response unavailable",
    )
}

fn create_marker(headers: &HeaderMap) -> Option<CreateMarker> {
    let id = headers
        .get("x-amz-meta-novarocks-create-id")?
        .to_str()
        .ok()?;
    let identity = headers
        .get("x-amz-meta-novarocks-create-identity")?
        .to_str()
        .ok()?;
    if id.len() != 36
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        || identity.len() != 64
        || !identity.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(CreateMarker {
        id: id.to_owned(),
        identity: identity.to_owned(),
    })
}

async fn forward(shared: Arc<Shared>, request: Request<Incoming>) -> io::Result<Response<Body>> {
    let (mut parts, incoming) = request.into_parts();
    let path = parts.uri.path().to_owned();
    let origin = shared.upstream.as_str().trim_end_matches('/');
    let url = Url::parse(&format!(
        "{origin}{}",
        parts.uri.path_and_query().ok_or_else(transport_error)?
    ))
    .map_err(|_| transport_error())?;
    let (operation, upload, part) = classify(&parts.method, &url);
    let conditional = parts
        .headers
        .get(header::IF_NONE_MATCH)
        .is_some_and(|value| value == "*");
    let attempt = {
        let mut observed = shared
            .observations
            .lock()
            .expect("S3 write observation lock");
        if let Some(fault) = &mut observed.ack_loss
            && fault.operation == operation
            && conditional
            && matches!(&fault.target, AckTarget::NextObjectUnder(prefix) if path.starts_with(prefix))
        {
            fault.target = AckTarget::Object(path.clone());
        }
        let attempt = observed
            .attempts
            .entry((operation, path.clone(), upload.clone(), part))
            .or_default();
        *attempt += 1;
        *attempt
    };
    let recorded = RecordedRequest {
        shared: Arc::clone(&shared),
        event: Some(S3WriteEvent {
            object_path: path,
            operation,
            upload_id_sha256: upload,
            part_number: part,
            condition_if_none_match_star: conditional,
            create_marker: conditional.then(|| create_marker(&parts.headers)).flatten(),
            readback_sha256: None,
            attempt,
            status: None,
            xml_code: None,
            upstream_response_complete: false,
            downstream_response_dropped: false,
            response_pause: None,
        }),
    };
    let has_body = parts.headers.contains_key(header::TRANSFER_ENCODING)
        || parts
            .headers
            .get(header::CONTENT_LENGTH)
            .is_some_and(|length| length != "0");
    remove_hop_headers(&mut parts.headers);
    let mut upstream = shared
        .client
        .request(parts.method, url)
        .headers(parts.headers);
    if has_body {
        upstream = upstream.body(reqwest::Body::wrap_stream(
            Body::new(incoming).into_data_stream(),
        ));
    }
    let sent = upstream.send().await;
    match sent {
        Ok(response) => finish_upstream(recorded, response).await,
        Err(_) => Err(transport_error()),
    }
}

async fn finish_upstream(
    mut recorded: RecordedRequest,
    upstream: reqwest::Response,
) -> io::Result<Response<Body>> {
    let event = recorded.event.as_mut().expect("S3 request event is owned");
    let status = upstream.status();
    event.status = Some(status.as_u16());
    let mut headers = upstream.headers().clone();
    remove_hop_headers(&mut headers);
    let body = if status.is_success() && matches!(event.operation, S3Operation::Get) {
        let length = upstream.content_length();
        Body::from_stream(ObservedReadBody {
            upstream: Box::pin(upstream.bytes_stream()),
            recorded,
            length,
            size: 0,
            digest: Sha256::new(),
        })
    } else {
        let bytes = upstream.bytes().await.map_err(|_| transport_error())?;
        event.upstream_response_complete = true;
        if !status.is_success() || event.operation == S3Operation::Complete {
            event.xml_code = xml_value(&bytes, "Code")
                .filter(|code| code.bytes().all(|byte| byte.is_ascii_alphanumeric()))
                .map(str::to_owned);
        }
        if status.is_success() && event.operation == S3Operation::Initiate {
            event.upload_id_sha256 = xml_value(&bytes, "UploadId").map(hash_upload);
        }
        let successful_create = event.condition_if_none_match_star
            && status.is_success()
            && match event.operation {
                S3Operation::Put => true,
                S3Operation::Complete => {
                    event.xml_code.is_none()
                        && bytes
                            .windows(b"<CompleteMultipartUploadResult".len())
                            .any(|prefix| prefix == b"<CompleteMultipartUploadResult")
                }
                _ => false,
            };
        let (drop_response, pause) = {
            let mut observed = recorded
                .shared
                .observations
                .lock()
                .expect("S3 write observation lock");
            let mut drop_response = false;
            let mut pause = None;
            if let Some(fault) = &mut observed.ack_loss
                && fault.operation == event.operation
                && event.condition_if_none_match_star
                && matches!(&fault.target, AckTarget::Object(path) if *path == event.object_path)
            {
                let phase = if successful_create && !fault.dropped {
                    drop_response = true;
                    fault.dropped = true;
                    Some(PauseAfter::SuccessfulCreate)
                } else if fault.dropped
                    && status == StatusCode::PRECONDITION_FAILED
                    && event.xml_code.as_deref() == Some("PreconditionFailed")
                {
                    Some(PauseAfter::RetryPreconditionFailure)
                } else {
                    None
                };
                if fault
                    .pause
                    .as_ref()
                    .is_some_and(|(configured, _)| Some(*configured) == phase)
                {
                    pause = fault.pause.take();
                }
            }
            (drop_response, pause)
        };
        if let Some((phase, pause)) = pause {
            event.response_pause = Some(phase);
            *pause.reached.lock().expect("S3 response pause lock") =
                Some(event.object_path.clone());
            pause.observed.notify_all();
            pause.resumed.notified().await;
        }
        if drop_response {
            event.downstream_response_dropped = true;
            // Hyper closes the HTTP connection on service error before it
            // sends headers or body to the original production caller.
            return Err(transport_error());
        }
        Body::from(bytes)
    };
    let mut response = Response::new(body);
    *response.status_mut() =
        StatusCode::from_u16(status.as_u16()).map_err(|_| transport_error())?;
    *response.headers_mut() = headers;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    #[test]
    fn reports_real_upstream_success_before_dropping_the_exact_response() {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let origin = format!("http://{}", upstream.local_addr().unwrap());
        let upstream_thread = thread::spawn(move || {
            for status in [200, 412] {
                let (stream, _) = upstream.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut stream = BufReader::new(stream);
                let mut request = String::new();
                loop {
                    let mut line = String::new();
                    assert_ne!(stream.read_line(&mut line).unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    request.push_str(&line);
                }
                let request = request.to_ascii_lowercase();
                assert!(request.starts_with("put /bucket/object%20name?fixture=%2f http/1.1\r\n"));
                assert!(request.contains("\r\nhost: signed-host.test\r\n"));
                assert!(request.contains("\r\nif-none-match: *\r\n"));
                assert!(request.contains("\r\ncontent-length: 7\r\n"));
                let mut body = [0; 7];
                stream.read_exact(&mut body).unwrap();
                assert_eq!(&body, b"content");
                let body = if status == 200 {
                    ""
                } else {
                    "<Error><Code>PreconditionFailed</Code></Error>"
                };
                write!(stream.get_mut(), "HTTP/1.1 {status} Fixture\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let mut proxy = S3WriteFault::start(&origin, Duration::from_secs(5)).unwrap();
        proxy
            .arm_ack_loss(
                S3Operation::Put,
                AckTarget::Object("/bucket/object%20name".into()),
            )
            .unwrap();
        let client = reqwest::blocking::Client::builder()
            .retry(reqwest::retry::never())
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let send = || {
            client
                .put(format!(
                    "{}/bucket/object%20name?fixture=%2F",
                    proxy.endpoint()
                ))
                .header(header::HOST, "signed-host.test")
                .header(header::IF_NONE_MATCH, "*")
                .body("content")
                .send()
        };
        assert!(send().is_err());
        let response = send().unwrap();
        assert_eq!(response.status(), 412);
        assert!(response.text().unwrap().contains("PreconditionFailed"));
        upstream_thread.join().unwrap();
        let exit = proxy.shutdown().unwrap();
        assert_eq!(exit.accepted_connections, 2);
        assert_eq!(exit.accepted_connections, exit.exited_connections);
        assert_eq!(exit.events.len(), 2);
        assert_eq!(exit.events[0].status, Some(200));
        assert!(exit.events[0].upstream_response_complete);
        assert!(exit.events[0].downstream_response_dropped);
        assert_eq!(exit.events[1].status, Some(412));
        assert_eq!(
            exit.events[1].xml_code.as_deref(),
            Some("PreconditionFailed")
        );
        assert_eq!(exit.events[1].attempt, 2);
        assert!(!exit.events[1].downstream_response_dropped);
    }
}
