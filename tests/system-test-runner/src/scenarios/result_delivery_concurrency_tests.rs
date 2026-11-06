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

use super::*;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::atomic::{AtomicBool, Ordering},
    thread::JoinHandle,
};

struct WireDelete {
    client: usize,
    observed_us: u128,
    path: String,
}
struct Peer {
    port: u16,
    stop: Arc<AtomicBool>,
    owner: Option<JoinHandle<Result<()>>>,
}
impl Peer {
    fn start(client: usize, epoch: Instant, deletes: Arc<Mutex<Vec<WireDelete>>>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let owner = std::thread::spawn(move || -> Result<()> {
            let id = format!("controlled-client-{client}");
            let base = format!("/v1/statement/executing/{id}/fixture");
            let next = format!("http://127.0.0.1:{port}{base}/1");
            let mut cancelled = false;
            while !stopping.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(socket) => socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                };
                stream.set_nonblocking(false)?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                let (method, path) = read_request(&mut stream).with_context(|| {
                    format!("controlled HTTP peer {client} read original request")
                })?;
                let mut status = 200;
                let columns = json!([{"name":"payload","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647u64}]}}]);
                let value = match method.as_str() {
                    "POST" => {
                        json!({"id":id,"stats":{"state":"QUEUED"},"nextUri":format!("http://127.0.0.1:{port}{base}/0")})
                    }
                    "DELETE" => {
                        ensure!(
                            path == format!("{base}/1"),
                            "peer DELETE used another original continuation"
                        );
                        deletes
                            .lock()
                            .map_err(|_| anyhow::anyhow!("controlled delete observation poisoned"))?
                            .push(WireDelete {
                                client,
                                observed_us: epoch.elapsed().as_micros(),
                                path,
                            });
                        cancelled = true;
                        status = 204;
                        Value::Null
                    }
                    "GET" if path == format!("{base}/0") => {
                        json!({"id":id,"stats":{"state":"RUNNING"},"columns":columns,
                        "data":[[format!("client-{client}:1:{}","x".repeat(512))]],"nextUri":next})
                    }
                    "GET" if path == format!("{base}/1") && client == 0 => {
                        json!({"id":id,"stats":{"state":"RUNNING"},"columns":columns,
                        "data":[[format!("client-{client}:999:{}","x".repeat(512))]],"nextUri":next})
                    }
                    "GET" if path == format!("{base}/1") && cancelled => {
                        json!({"id":id,"stats":{"state":"FAILED"},
                        "error":{"message":"controlled original peer cancellation","errorCode":3,"errorName":"USER_CANCELED","errorType":"USER_ERROR"}})
                    }
                    "GET" if path == format!("{base}/1") => {
                        json!({"id":id,"stats":{"state":"RUNNING"},"columns":columns,"nextUri":next})
                    }
                    _ => anyhow::bail!("controlled peer received a foreign request"),
                };
                let bytes = if status == 204 {
                    Vec::new()
                } else {
                    serde_json::to_vec(&value)?
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    if status == 204 { "No Content" } else { "OK" },
                    bytes.len()
                )
                .with_context(|| {
                    format!("controlled HTTP peer {client} write {method} response headers")
                })?;
                stream.write_all(&bytes).with_context(|| {
                    format!("controlled HTTP peer {client} write {method} response body")
                })?;
                stream.flush().with_context(|| {
                    format!("controlled HTTP peer {client} flush {method} response")
                })?;
            }
            Ok(())
        });
        Ok(Self {
            port,
            stop,
            owner: Some(owner),
        })
    }
    fn finish(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        let actual = self
            .owner
            .take()
            .context("controlled original peer was joined twice")?
            .join();
        match actual {
            Ok(result) => result,
            Err(payload) => Err(anyhow::Error::new(PeerPanic {
                _payload: Mutex::new(payload),
            })),
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if self.owner.is_some() {
            let _ = self.finish();
        }
    }
}
struct PeerPanic {
    _payload: Mutex<Box<dyn std::any::Any + Send>>,
}
impl std::fmt::Debug for PeerPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerPanic(actual payload retained)")
    }
}
impl std::fmt::Display for PeerPanic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("controlled original HTTP peer panicked")
    }
}
impl std::error::Error for PeerPanic {}
fn read_request(stream: &mut TcpStream) -> Result<(String, String)> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        ensure!(
            header.len() < 8192,
            "controlled request header exceeded its fixed fixture bound"
        );
        let mut byte = [0];
        stream.read_exact(&mut byte)?;
        header.push(byte[0]);
    }
    let header = std::str::from_utf8(&header)?;
    let mut lines = header.split("\r\n");
    let mut first = lines
        .next()
        .context("controlled request line missing")?
        .split_whitespace();
    let method = first
        .next()
        .context("controlled method missing")?
        .to_owned();
    let path = first.next().context("controlled path missing")?.to_owned();
    let length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map(|(_, length)| length.trim().parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    ensure!(
        length <= 8192,
        "controlled request body exceeded its fixed fixture bound"
    );
    let mut body = vec![0; length];
    stream.read_exact(&mut body)?;
    Ok((method, path))
}

#[test]
fn first_original_reader_failure_sends_exact_peer_deletes_before_remaining_actual_joins()
-> Result<()> {
    let root = tempfile::tempdir()?;
    let epoch = Instant::now();
    let deadline = epoch + Duration::from_secs(10);
    let deletes = Arc::new(Mutex::new(Vec::new()));
    let mut peers = Vec::new();
    for client in 0..4 {
        peers.push(Peer::start(client, epoch, deletes.clone())?);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(4)
        .enable_all()
        .build()?;
    let mut leases = Vec::new();
    for (client, peer) in peers.iter().enumerate() {
        let actor = TrinoClient::new(
            peer.port,
            "root",
            deadline,
            &root.path().join(format!("client-{client}")),
        )?;
        let query = format!(
            "SELECT CONCAT('client-{client}:', CAST(generate_series AS VARCHAR), ':', REPEAT('x', 512)) AS payload FROM generate_series(1, 4096) ORDER BY generate_series"
        );
        let post = actor.submit(&query)?;
        let id = post.json()?["id"]
            .as_str()
            .context("controlled POST omitted id")?
            .to_owned();
        leases.push(OriginalQuery {
            client,
            actor,
            id,
            current_uri: Arc::new(Mutex::new(post.next_uri()?)),
        });
    }
    // This invokes the same owner as the real Native scene. Only the peer's
    // second data row is wrong; the other peers cannot exit until real DELETE.
    let group = drive_original_readers(
        &runtime,
        &leases,
        Vec::new(),
        deadline,
        epoch,
        epoch.elapsed().as_micros(),
    );
    let mut server_errors = Vec::new();
    for (client, peer) in peers.iter_mut().enumerate() {
        if let Err(error) = peer.finish() {
            server_errors.push(error.context(format!("controlled HTTP peer {client} actual Join")));
        }
    }
    if !server_errors.is_empty() {
        // Keep every original error object after joining every peer. The test
        // failure report also exposes each chain, rather than only a boolean.
        for error in &server_errors {
            eprintln!("controlled original HTTP peer Join failed: {error:#}");
        }
        return Err(anyhow::Error::new(ConcurrentFailures(server_errors))
            .context("controlled original HTTP owners failed their actual joins"));
    }
    assert_eq!(group.actors_spawned, 4);
    assert_eq!(group.unjoined_original_handles, 0);
    assert_eq!(group.joins.len(), 4);
    assert_eq!(group.first_data.len(), 4);
    assert!(group.barrier_completed_us.is_some());
    assert_eq!(group.failures.len(), 4);
    assert_eq!(group.outcomes.len(), 4);
    assert_eq!(group.joins[0]["client"], 0);
    assert!(
        group
            .joins
            .iter()
            .all(|join| join["actor_failed"] == true && join["panicked"] == false)
    );
    assert_eq!(group.cancellation.len(), 4);
    assert!(
        group
            .cancellation
            .iter()
            .all(|request| request["actual_status"] == 204)
    );
    let wire = deletes
        .lock()
        .map_err(|_| anyhow::anyhow!("controlled delete observation poisoned"))?;
    assert_eq!(wire.len(), 4);
    assert_eq!(
        wire.iter()
            .map(|event| event.client)
            .collect::<BTreeSet<_>>(),
        (0..4).collect()
    );
    for client in 1..4 {
        let actual = wire.iter().find(|event| event.client == client).unwrap();
        assert_eq!(
            actual.path,
            format!("/v1/statement/executing/controlled-client-{client}/fixture/1")
        );
        let joined = group
            .joins
            .iter()
            .find(|join| join["client"] == client)
            .unwrap();
        assert!(actual.observed_us <= joined["joined_us"].as_u64().unwrap() as u128);
        let report = group
            .outcomes
            .iter()
            .find(|report| report.client == client)
            .unwrap();
        assert_eq!(report.rows, 1);
        assert_eq!(report.actual_public_error.as_ref().unwrap()["errorCode"], 3);
    }
    assert_eq!(group.failures.iter().filter(|failure|matches!(failure.downcast_ref::<StatementFailure>(),Some(StatementFailure::Trino(error)) if error.error_code==3)).count(),3);
    let original = group
        .outcomes
        .iter()
        .find(|report| report.client == 0)
        .unwrap();
    assert_eq!(original.rows, 1);
    assert!(original.actual_public_error.is_none());
    Ok(())
}
