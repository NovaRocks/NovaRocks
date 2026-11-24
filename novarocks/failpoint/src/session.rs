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

//! Bounded role-launch declarations and mechanical liveness messages.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt;
use std::fs::OpenOptions;
#[cfg(novarocks_dev_controls)]
use std::io::Read;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const DECLARATION_VERSION: u32 = 1;
pub const MAX_DECLARATION_BYTES: usize = 64 * 1024;
pub const MAX_LIVENESS_FRAME_BYTES: usize = 4096;
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
pub const HEARTBEAT_LOSS: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionRole {
    Fe,
    Be,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainGrant {
    pub domain: String,
    pub grant_id: String,
    pub parameters: Value,
}

impl fmt::Debug for DomainGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DomainGrant")
            .field("domain", &self.domain)
            .field("grant_id", &self.grant_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDeclaration {
    pub schema_version: u32,
    pub session_id: String,
    pub launch_id: String,
    pub role: SessionRole,
    pub lifetime_millis: u64,
    pub liveness_socket: PathBuf,
    pub handshake_secret: String,
    pub domain_grants: Vec<DomainGrant>,
}

impl fmt::Debug for SessionDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionDeclaration")
            .field("session_id", &self.session_id)
            .field("launch_id", &self.launch_id)
            .field("role", &self.role)
            .field("grant_count", &self.domain_grants.len())
            .finish_non_exhaustive()
    }
}

pub(crate) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

impl SessionDeclaration {
    pub fn validate_envelope(&self) -> io::Result<()> {
        if self.schema_version != DECLARATION_VERSION {
            return Err(invalid("unsupported test-session declaration version"));
        }
        if !identifier(&self.session_id) || !identifier(&self.launch_id) {
            return Err(invalid("invalid test-session or launch identity"));
        }
        if self.lifetime_millis == 0 {
            return Err(invalid("test-session lifetime must be nonzero and finite"));
        }
        if !(32..=256).contains(&self.handshake_secret.len())
            || !self
                .handshake_secret
                .bytes()
                .all(|b| b.is_ascii_alphanumeric())
        {
            return Err(invalid("invalid one-use test-session handshake material"));
        }
        if self.domain_grants.is_empty() {
            return Err(invalid("test-session requires a nonempty grant set"));
        }
        let mut ids = BTreeSet::new();
        for grant in &self.domain_grants {
            if !identifier(&grant.domain)
                || !identifier(&grant.grant_id)
                || !grant.parameters.is_object()
                || !ids.insert((&grant.domain, &grant.grant_id))
            {
                return Err(invalid("invalid or duplicate test-session domain grant"));
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            if !self.liveness_socket.is_absolute()
                || self.liveness_socket.as_os_str().as_bytes().len() > 90
            {
                return Err(invalid(
                    "test-session requires a short absolute private Unix socket path",
                ));
            }
        }
        #[cfg(not(unix))]
        {
            return Err(invalid(
                "test-session transport is unsupported on this platform",
            ));
        }
        Ok(())
    }

    pub fn write_private(&self, path: &Path) -> io::Result<()> {
        self.validate_envelope()?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| invalid("encode test-session declaration"))?;
        if bytes.len() > MAX_DECLARATION_BYTES {
            return Err(invalid("test-session declaration exceeds its byte bound"));
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()
    }

    #[cfg(novarocks_dev_controls)]
    pub fn read_bounded(path: &Path) -> io::Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.is_file() {
            return Err(invalid(
                "test-session declaration must be a regular private file",
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
            if metadata.mode() & 0o077 != 0 {
                return Err(invalid("test-session declaration is not private"));
            }
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_DECLARATION_BYTES as u64 {
            return Err(invalid(
                "test-session declaration must be a bounded regular file",
            ));
        }
        let mut bytes = Vec::new();
        file.take(MAX_DECLARATION_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_DECLARATION_BYTES {
            return Err(invalid("test-session declaration exceeds its byte bound"));
        }
        let declaration: Self = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("invalid test-session declaration"))?;
        declaration.validate_envelope()?;
        Ok(declaration)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum LivenessFrame {
    Hello {
        session_id: String,
        launch_id: String,
        role: SessionRole,
        process_id: String,
        handshake_secret: String,
    },
    Bound {
        remaining_millis: u64,
    },
    Heartbeat,
    End,
}

pub(crate) fn write_frame(writer: &mut impl Write, frame: &LivenessFrame) -> io::Result<()> {
    let bytes =
        serde_json::to_vec(frame).map_err(|_| invalid("encode test-session liveness frame"))?;
    if bytes.is_empty() || bytes.len() > MAX_LIVENESS_FRAME_BYTES {
        return Err(invalid("test-session liveness frame exceeds its bound"));
    }
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()
}

/// The caller supplies an absolute deadline; each partial read preserves it.
pub(crate) fn read_frame_with(
    read_exact: &mut impl FnMut(&mut [u8]) -> io::Result<()>,
) -> io::Result<LivenessFrame> {
    let mut header = [0; 4];
    read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > MAX_LIVENESS_FRAME_BYTES {
        return Err(invalid("invalid test-session liveness frame length"));
    }
    let mut body = vec![0; length];
    read_exact(&mut body)?;
    serde_json::from_slice(&body).map_err(|_| invalid("invalid test-session liveness frame"))
}

pub(crate) fn secret_matches(expected: &str, received: &str) -> bool {
    if expected.len() != received.len() {
        return false;
    }
    expected
        .bytes()
        .zip(received.bytes())
        .fold(0, |diff, (a, b)| diff | (a ^ b))
        == 0
}

#[cfg(all(test, unix))]
mod capacity_tests {
    use super::*;

    fn declaration(grants: usize) -> SessionDeclaration {
        SessionDeclaration {
            schema_version: DECLARATION_VERSION,
            session_id: "capacity-session".into(),
            launch_id: "capacity-launch".into(),
            role: SessionRole::Fe,
            lifetime_millis: 1000,
            liveness_socket: PathBuf::from("/tmp/capacity.sock"),
            handshake_secret: "abcdefghijklmnopqrstuvwxyz0123456789".into(),
            domain_grants: (0..grants)
                .map(|index| DomainGrant {
                    domain: "test-domain".into(),
                    grant_id: format!("exact-{index}"),
                    parameters: serde_json::json!({"request":"exact"}),
                })
                .collect(),
        }
    }

    #[test]
    fn declaration_accepts_more_than_128_grants_within_the_byte_bound() {
        let accepted = declaration(163);
        accepted.validate_envelope().unwrap();
        let encoded = serde_json::to_vec(&accepted).unwrap();
        assert!(encoded.len() <= MAX_DECLARATION_BYTES);
        let decoded: SessionDeclaration = serde_json::from_slice(&encoded).unwrap();
        decoded.validate_envelope().unwrap();
        assert_eq!(decoded.domain_grants.len(), 163);

        let error = declaration(0).validate_envelope().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "test-session requires a nonempty grant set"
        );
    }

    #[test]
    fn declaration_capacity_does_not_relax_the_total_byte_bound() {
        let mut oversized = declaration(1);
        oversized.domain_grants[0].parameters =
            serde_json::json!({"action":"observe","payload":"x".repeat(64 * 1024)});
        oversized.validate_envelope().unwrap();
        assert!(serde_json::to_vec(&oversized).unwrap().len() > MAX_DECLARATION_BYTES);
        // Serialization must reject before opening this path. No role or test
        // transport is created by this envelope boundary check.
        let error = oversized.write_private(Path::new("/")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "test-session declaration exceeds its byte bound"
        );
    }
}
