// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Physical controls for the SAME accepted finite Root peer connections.
//! Weak socket loans carry no protocol, process, Source or exit authority.
use super::*;
use std::pin::Pin;
use std::sync::{Mutex, Weak};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(super) type OriginalRootConnections = Arc<Mutex<Vec<Weak<OriginalSocketClose>>>>;
pub(super) struct OriginalSocketClose {
    pub(super) remote: std::net::SocketAddr,
    duplicate: Mutex<Option<std::net::TcpStream>>,
}
impl OriginalSocketClose {
    pub(super) fn shutdown_original(&self) -> std::io::Result<bool> {
        let original = self.duplicate.lock().unwrap().take();
        if let Some(original) = original {
            // This is a duplicate descriptor of the SAME accepted TCP socket.
            // No error is returned to RPC/Native by this fixture API.
            original.shutdown(std::net::Shutdown::Both)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
struct OriginalRootIo {
    stream: tokio::net::TcpStream,
    close: Arc<OriginalSocketClose>,
}
impl Drop for OriginalRootIo {
    fn drop(&mut self) {
        // The fixture bank is Weak. Natural Hyper connection Drop also drops
        // this SAME duplicated descriptor; it cannot extend socket lifetime.
        drop(self.close.duplicate.lock().unwrap().take());
    }
}
impl tonic::transport::server::Connected for OriginalRootIo {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        tonic::transport::server::Connected::connect_info(&self.stream)
    }
}
impl AsyncRead for OriginalRootIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for OriginalRootIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

pub(super) async fn serve(
    peer: TaskWirePeer,
    listener: tokio::net::TcpListener,
    connections: OriginalRootConnections,
) -> (
    RuntimeEndpoint,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let address = listener.local_addr().expect("original Root address");
    let incoming =
        futures::stream::unfold((listener, connections), |(listener, connections)| async {
            let item = async {
                let (stream, _) = listener.accept().await?;
                let remote = stream.peer_addr()?;
                let original = stream.into_std()?;
                let duplicate = original.try_clone()?;
                let stream = tokio::net::TcpStream::from_std(original)?;
                let close = Arc::new(OriginalSocketClose {
                    remote,
                    duplicate: Mutex::new(Some(duplicate)),
                });
                connections.lock().unwrap().push(Arc::downgrade(&close));
                Ok::<_, std::io::Error>(OriginalRootIo { stream, close })
            }
            .await;
            Some((item, (listener, connections)))
        });
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    let served = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                NovaRocksGrpcServer::new(peer)
                    .max_decoding_message_size(128 * 1024 * 1024)
                    .max_encoding_message_size(128 * 1024 * 1024),
            )
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stop.await;
            })
            .await
            .expect("serve the same controlled original Root peer");
    });
    (RuntimeEndpoint::from_socket_addr(address), shutdown, served)
}
