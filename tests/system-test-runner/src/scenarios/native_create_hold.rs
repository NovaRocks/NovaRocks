// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Runner-owned exact pre-Worker Create rendezvous shared by Native scenarios.

use anyhow::{Context, Result, ensure};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub(crate) struct HoldListener {
    backend_count: usize,
    path: PathBuf,
    listener: UnixListener,
}
impl HoldListener {
    pub(crate) fn bind(token: &str, backend_count: usize) -> Result<Self> {
        ensure!(
            backend_count > 0,
            "Create rendezvous needs its actual backend count"
        );
        let path = novarocks_failpoint::create_before_worker_rendezvous_socket_path(token)
            .map_err(anyhow::Error::msg)?;
        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            path,
            listener,
            backend_count,
        })
    }
    pub(crate) fn try_receive(&self, token: &str, deadline: Instant) -> Result<Option<HeldCreate>> {
        let (mut stream, _) = match self.listener.accept() {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let payload = crate::scenarios::native_direct_controls::read_marker(
            &mut stream,
            deadline.min(Instant::now() + Duration::from_secs(1)),
        )?;
        let field = |name: &str| -> Result<String> {
            payload
                .split_whitespace()
                .find_map(|value| value.strip_prefix(&format!("{name}=")))
                .map(str::to_owned)
                .with_context(|| format!("missing {name} in held Create {payload:?}"))
        };
        let backend = field("backend_index")?.parse::<usize>()?;
        ensure!(
            field("token")? == token && backend < self.backend_count,
            "held Create rendezvous identity differs from arming: {payload}"
        );
        let create = HeldCreate {
            backend,
            deadline,
            execution: field("execution_id")?,
            stage: field("stage")?,
            process: field("process_id")?,
            task: field("task")?,
            outbound_edges: field("outbound_edges")?.parse()?,
            payload,
            stream: Some(stream),
            released_peer: None,
        };
        Ok(Some(create))
    }
}
impl Drop for HoldListener {
    fn drop(&mut self) {
        // Any connection that the main loop has not yet observed still owns
        // an exact parked Native request. Release it before unlinking.
        while let Ok((mut stream, _)) = self.listener.accept() {
            crate::scenarios::native_direct_controls::close_hold(&mut stream);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
pub(crate) struct HeldCreate {
    deadline: Instant,
    pub(crate) backend: usize,
    pub(crate) execution: String,
    pub(crate) stage: String,
    pub(crate) task: String,
    pub(crate) process: String,
    pub(crate) outbound_edges: usize,
    pub(crate) payload: String,
    stream: Option<UnixStream>,
    released_peer: Option<UnixStream>,
}
impl HeldCreate {
    pub(crate) fn identity_marker(&self) -> String {
        format!(
            "execution_id={} stage={} task={}",
            self.execution, self.stage, self.task
        )
    }
    pub(crate) fn release(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            crate::scenarios::native_direct_controls::release_hold(&mut stream, self.deadline)?;
            // The Native peer may still set its next read timeout before consuming
            // R. Retain this exact socket until the fixture owner retires.
            self.released_peer = Some(stream);
        }
        Ok(())
    }
}
impl Drop for HeldCreate {
    fn drop(&mut self) {
        if let Some(mut stream) = self.stream.take() {
            crate::scenarios::native_direct_controls::close_hold(&mut stream);
        }
        drop(self.released_peer.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::Shutdown;

    #[test]
    fn release_keeps_peer_open_until_fixture_owner_retires() {
        let (stream, mut backend) = UnixStream::pair().unwrap();
        let mut create = HeldCreate {
            deadline: Instant::now() + Duration::from_secs(1),
            backend: 1,
            execution: "10:20:1".into(),
            stage: "2".into(),
            task: "3".into(),
            process: "exact-process".into(),
            outbound_edges: 1,
            payload: "exact held Create".into(),
            stream: Some(stream),
            released_peer: None,
        };
        create.release().unwrap();
        backend
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut release = [0_u8; 1];
        backend.read_exact(&mut release).unwrap();
        assert_eq!(release, [b'R']);
        backend.set_nonblocking(true).unwrap();
        assert_eq!(
            backend.read(&mut release).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        create.release().unwrap();
        assert_eq!(
            backend.read(&mut release).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        drop(create);
        assert_eq!(backend.read(&mut release).unwrap(), 0);
    }

    #[test]
    fn listener_drop_releases_unobserved_parked_create_and_unlinks_socket() {
        let token = format!(
            "cbw-pending-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let listener = HoldListener::bind(&token, 3).unwrap();
        let path = listener.path.clone();
        let mut backend = UnixStream::connect(&path).unwrap();
        backend.write_all(format!("token={token} execution_id=10:20:1 stage=2 task=3 backend_index=1 process_id=exact-process outbound_edges=1\n").as_bytes()).unwrap();
        backend.shutdown(Shutdown::Write).unwrap();
        drop(listener);
        let mut release = [0_u8; 1];
        backend.read_exact(&mut release).unwrap();
        assert_eq!(release, [b'R']);
        assert!(!path.exists());
    }

    #[test]
    fn held_create_rendezvous_preserves_identity_until_explicit_release() {
        let token = format!(
            "cbw-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let listener = HoldListener::bind(&token, 3).unwrap();
        let mut backend = UnixStream::connect(&listener.path).unwrap();
        backend.write_all(format!(
            "token={token} execution_id=10:20:1 stage=2 task=3 backend_index=1 process_id=exact-process outbound_edges=1\n"
        ).as_bytes()).unwrap();
        backend.shutdown(Shutdown::Write).unwrap();
        let mut create = listener
            .try_receive(&token, Instant::now() + Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(
            create.identity_marker(),
            "execution_id=10:20:1 stage=2 task=3"
        );
        assert_eq!(create.process, "exact-process");
        assert_eq!(create.outbound_edges, 1);
        create.release().unwrap();
        let mut release = [0_u8; 1];
        backend.read_exact(&mut release).unwrap();
        assert_eq!(release, [b'R']);
        let path = listener.path.clone();
        drop(listener);
        assert!(!path.exists());
    }
}
