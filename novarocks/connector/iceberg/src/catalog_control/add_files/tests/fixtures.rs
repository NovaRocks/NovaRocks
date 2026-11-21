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

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct SourceRequest {
    pub method: String,
    pub target: String,
    pub range: Option<String>,
}

/// A real S3 accessor reads immutable external Parquet through this HTTP
/// endpoint. An armed stat HEAD or footer-tail GET is held; no production hook is used.
pub(crate) struct SourceHttpFixture {
    address: std::net::SocketAddr,
    bytes: Arc<Vec<u8>>,
    requests: Arc<Mutex<Vec<SourceRequest>>>,
    armed: Arc<AtomicBool>,
    stat_armed: Arc<AtomicBool>,
    tail_started: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
    stopped: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SourceHttpFixture {
    pub(crate) fn new(bytes: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let bytes = Arc::new(bytes);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let armed = Arc::new(AtomicBool::new(false));
        let stat_armed = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (tail_tx, tail_started) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let state = (
            bytes.clone(),
            requests.clone(),
            armed.clone(),
            stat_armed.clone(),
            stopped.clone(),
        );
        let thread = std::thread::spawn(move || {
            let (bytes, requests, armed, stat_armed, stopped) = state;
            while !stopped.load(Ordering::SeqCst) {
                let (stream, _) = match listener.accept() {
                    Ok(accepted) => accepted,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("source HTTP accept: {error}"),
                };
                // Accepted sockets may inherit the listener's nonblocking
                // flag on BSD/macOS; the bounded HTTP reader is blocking.
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line.is_empty() {
                    continue;
                }
                let mut parts = line.split_whitespace();
                let method = parts.next().unwrap().to_string();
                let target = parts.next().unwrap().to_string();
                let mut range = None;
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("range")
                    {
                        range = Some(value.trim().to_string());
                    }
                }
                requests.lock().unwrap().push(SourceRequest {
                    method: method.clone(),
                    target: target.clone(),
                    range: range.clone(),
                });
                let mut stream = reader.into_inner();
                let is_object = target.split('?').next().unwrap().ends_with(".parquet");
                let mut status = "200 OK";
                let mut extra = String::new();
                let mut content_length = bytes.len();
                let body = if method == "GET" && !is_object {
                    if target.contains("a.parquet") {
                        let listing = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>source-bucket</Name><Prefix>incoming/a.parquet/</Prefix><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>";
                        content_length = listing.len();
                        listing.to_vec()
                    } else {
                        let listing = format!(
                            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>source-bucket</Name><Prefix>incoming/</Prefix><KeyCount>2</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>incoming/a.parquet</Key><LastModified>2026-10-10T00:00:00.000Z</LastModified><ETag>\"external-stable\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents><Contents><Key>incoming/b.parquet</Key><LastModified>2026-10-10T00:00:00.000Z</LastModified><ETag>\"external-stable\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>",
                            bytes.len(),
                            bytes.len()
                        );
                        content_length = listing.len();
                        listing.into_bytes()
                    }
                } else if method == "HEAD" && is_object {
                    if stat_armed.swap(false, Ordering::SeqCst) {
                        tail_tx.send(()).unwrap();
                        release_rx
                            .recv_timeout(Duration::from_secs(10))
                            .expect("release issued source stat");
                    }
                    Vec::new()
                } else if method == "GET" && is_object {
                    let (start, end) = match range.as_deref() {
                        Some(range) => {
                            let (start, end) = range
                                .strip_prefix("bytes=")
                                .unwrap()
                                .split_once('-')
                                .unwrap();
                            (
                                start.parse::<usize>().unwrap(),
                                if end.is_empty() {
                                    bytes.len() - 1
                                } else {
                                    end.parse().unwrap()
                                },
                            )
                        }
                        None => (0, bytes.len() - 1),
                    };
                    if start == bytes.len() - 8 && armed.swap(false, Ordering::SeqCst) {
                        tail_tx.send(()).unwrap();
                        release_rx
                            .recv_timeout(Duration::from_secs(10))
                            .expect("release issued source tail");
                    }
                    status = "206 Partial Content";
                    extra = format!("Content-Range: bytes {start}-{end}/{}\r\n", bytes.len());
                    content_length = end - start + 1;
                    bytes[start..=end].to_vec()
                } else {
                    status = "405 Method Not Allowed";
                    content_length = 0;
                    Vec::new()
                };
                let reply = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {content_length}\r\nETag: \"external-stable\"\r\nLast-Modified: Sat, 10 Oct 2026 00:00:00 GMT\r\nAccept-Ranges: bytes\r\n{extra}Connection: close\r\n\r\n"
                );
                if let Err(error) = reply
                    .and_then(|()| stream.write_all(&body))
                    .and_then(|()| stream.flush())
                {
                    assert!(
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                        ),
                        "source fixture response: {error}"
                    );
                }
            }
        });
        Self {
            address,
            bytes,
            requests,
            armed,
            stat_armed,
            tail_started,
            release,
            stopped,
            thread: Some(thread),
        }
    }

    pub(crate) fn config(&self) -> novarocks_fs::ObjectStoreConfig {
        novarocks_fs::ObjectStoreConfig {
            endpoint: format!("http://{}", self.address),
            access_key_id: novarocks_fs::SecretValue::new("source-test-key"),
            access_key_secret: novarocks_fs::SecretValue::new("source-test-secret"),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".into()),
            retry_max_times: Some(0),
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: Some(10000),
            io_timeout_ms: Some(10000),
        }
    }
    pub(crate) fn source(&self) -> &'static str {
        "s3://source-bucket/incoming/"
    }
    pub(crate) fn arm_tail(&self) {
        self.requests.lock().unwrap().clear();
        self.armed.store(true, Ordering::SeqCst);
    }
    pub(crate) fn arm_stat(&self) {
        self.requests.lock().unwrap().clear();
        self.stat_armed.store(true, Ordering::SeqCst);
    }
    pub(crate) fn wait_for_tail(&self) {
        self.tail_started
            .recv_timeout(Duration::from_secs(10))
            .expect("actual tail GET started");
    }
    pub(crate) fn release_tail(&self) {
        self.release.send(()).unwrap();
    }
    pub(crate) fn requests(&self) -> Vec<SourceRequest> {
        self.requests.lock().unwrap().clone()
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }
    pub(crate) fn tail_range(&self) -> String {
        format!("bytes={}-{}", self.bytes.len() - 8, self.bytes.len() - 1)
    }
}
impl Drop for SourceHttpFixture {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
        let _ = self.release.send(());
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
