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

//! HTTP/1 transport owns actual write deadlines independently of body polling.
use crate::{
    body::{WRITE_TIMEOUT, WriteDeadlines},
    observation::{ResponseWriteFailure, TrinoProtocolObservation},
};
use axum::{Router, body::Body};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinSet,
};
use tower::ServiceExt;

pub(crate) async fn serve(
    listener: TcpListener,
    router: Router,
    mut shutdown: watch::Receiver<bool>,
    observation: TrinoProtocolObservation,
) -> io::Result<()> {
    let (stop, stopped) = watch::channel(false);
    let mut connections = JoinSet::new();
    let mut result = Ok(());
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((socket, peer)) => {
                        let router = router.clone();
                        let stopped = stopped.clone();
                        let observation = observation.clone();
                        connections.spawn(async move {
                            let exit = serve_connection(socket, router, stopped, WRITE_TIMEOUT).await;
                            match exit {
                                ConnectionExit::WriteTimedOut { response } => {
                                    observation.write_failed(ResponseWriteFailure::Timeout);
                                    tracing::warn!(%peer, response, "Trino response write deadline elapsed; connection closed");
                                },
                                ConnectionExit::ShutdownTimedOut => tracing::warn!(%peer, "Trino connection did not finish before listener shutdown deadline"),
                                ConnectionExit::HttpError { error, pending_write } => {
                                    if pending_write {
                                        observation.write_failed(ResponseWriteFailure::Interrupted);
                                    }
                                    tracing::debug!(%peer, %error, pending_write, "Trino HTTP connection ended");
                                },
                                ConnectionExit::Complete => {},
                            }
                        });
                    }
                    Err(error) => { result = Err(error); break; }
                }
            }
            _ = shutdown.changed() => { break; }
            joined = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = joined { result = Err(io::Error::other(error)); break; }
            }
        }
    }
    drop(listener);
    let _ = stop.send(true);
    // Every connection observes stop. The connection's own shutdown bound also
    // covers clients that stopped uploading before a response body existed.
    while let Some(joined) = connections.join_next().await {
        if let Err(error) = joined {
            result = Err(io::Error::other(error));
        }
    }
    result
}

#[derive(Debug)]
enum ConnectionExit {
    Complete,
    WriteTimedOut {
        response: u64,
    },
    ShutdownTimedOut,
    HttpError {
        error: hyper::Error,
        pending_write: bool,
    },
}
async fn serve_connection(
    socket: TcpStream,
    router: Router,
    mut stop: watch::Receiver<bool>,
    timeout: Duration,
) -> ConnectionExit {
    let deadlines = WriteDeadlines::new(timeout);
    let writes = deadlines.clone();
    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        let router = router.clone();
        let writes = writes.clone();
        async move {
            let response = router
                .oneshot(request.map(Body::new))
                .await
                .expect("Trino router is infallible");
            Ok::<_, Infallible>(writes.wrap(response))
        }
    });
    let io = FlushObservedIo {
        inner: socket,
        writes: deadlines.clone(),
    };
    let mut connection =
        std::pin::pin!(http1::Builder::new().serve_connection(TokioIo::new(io), service));
    let mut shutdown_deadline = None;
    if *stop.borrow() {
        connection.as_mut().graceful_shutdown();
        shutdown_deadline = Some(tokio::time::Instant::now() + timeout);
    }
    loop {
        tokio::select! {
            outcome = &mut connection => return match outcome {
                Ok(()) => ConnectionExit::Complete,
                Err(error) => ConnectionExit::HttpError { error, pending_write: deadlines.has_pending() },
            },
            response = deadlines.expired() => return ConnectionExit::WriteTimedOut { response },
            _ = stop.changed(), if shutdown_deadline.is_none() => {
                connection.as_mut().graceful_shutdown();
                shutdown_deadline = Some(tokio::time::Instant::now() + timeout);
            }
            _ = async {
                match shutdown_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => return ConnectionExit::ShutdownTimedOut,
        }
    }
}

struct FlushObservedIo<T> {
    inner: T,
    writes: WriteDeadlines,
}
impl<T: AsyncRead + Unpin> AsyncRead for FlushObservedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for FlushObservedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buffer)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, buffers)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            self.writes.flushed();
        }
        result
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::StatusCode, response::Response, routing::get};
    use bytes::Bytes;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn blocked_tcp_write_exits_without_waiting_for_another_body_poll() {
        static CHUNK: [u8; 32768] = [b'x'; 32768];
        let router = Router::new().route(
            "/",
            get(|| async {
                Response::new(Body::from_stream(futures::stream::repeat(Ok::<
                    _,
                    Infallible,
                >(
                    Bytes::from_static(&CHUNK),
                ))))
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (_stop, stopped) = watch::channel(false);
        let connection = tokio::spawn(serve_connection(
            server,
            router,
            stopped,
            Duration::from_millis(100),
        ));
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        // The client deliberately never reads. The repeating source retains
        // only one fixed chunk, but eventually fills the real TCP send window.
        let exit = tokio::time::timeout(Duration::from_secs(3), connection)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(exit, ConnectionExit::WriteTimedOut { .. }),
            "{exit:?}"
        );
        // Actual task exit proves the independent watchdog dropped its IO;
        // unread bytes in the client's kernel receive buffer do not postpone it.
    }
    #[tokio::test]
    async fn listener_shutdown_closes_partial_upload_and_joins_actual_exit() {
        let invoked = Arc::new(AtomicBool::new(false));
        let marker = invoked.clone();
        let (started, mut received) = tokio::sync::mpsc::channel(1);
        let router = Router::new()
            .route(
                "/",
                axum::routing::post(move |_: String| {
                    let marker = marker.clone();
                    async move {
                        marker.store(true, Ordering::Release);
                        StatusCode::OK
                    }
                }),
            )
            .layer(axum::middleware::from_fn(
                move |request: axum::extract::Request, next: axum::middleware::Next| {
                    let started = started.clone();
                    async move {
                        started.send(()).await.unwrap();
                        next.run(request).await
                    }
                },
            ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (stop, stopped) = watch::channel(false);
        let connection = tokio::spawn(serve_connection(
            server,
            router,
            stopped,
            Duration::from_millis(100),
        ));
        client
            .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\n\r\na")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), received.recv())
            .await
            .unwrap()
            .unwrap();
        stop.send(true).unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(3), connection)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                exit,
                ConnectionExit::Complete | ConnectionExit::ShutdownTimedOut
            ),
            "{exit:?}"
        );
        assert!(!invoked.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn successful_flush_does_not_close_a_later_idle_keepalive() {
        let router = Router::new().route("/", get(|| async { "ok" }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (stop, stopped) = watch::channel(false);
        let connection = tokio::spawn(serve_connection(
            server,
            router,
            stopped,
            Duration::from_millis(50),
        ));
        for _ in 0..2 {
            client
                .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let response = tokio::time::timeout(Duration::from_secs(3), async {
                let mut response = Vec::new();
                loop {
                    let mut byte = [0];
                    assert_eq!(client.read(&mut byte).await.unwrap(), 1);
                    response.push(byte[0]);
                    if response.ends_with(b"\r\n\r\nok") {
                        break;
                    }
                }
                response
            })
            .await
            .unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .starts_with("HTTP/1.1 200")
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!connection.is_finished());
        }
        stop.send(true).unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(3), connection)
                .await
                .unwrap()
                .unwrap(),
            ConnectionExit::Complete
        ));
    }
}
