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

//! Transport-neutral client connection lifecycle contracts.
//!
//! A protocol adapter owns allocation and termination. Query application
//! sessions retain only the exact, opaque identity and request termination
//! through the consumer-supplied port.

use std::fmt;

/// An exact process-local client connection identity.
///
/// The generation fences reuse of a protocol-visible connection ID after its
/// previous protocol task has exited.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientConnectionToken {
    connection_id: u32,
    generation: u64,
}

impl ClientConnectionToken {
    pub fn new(connection_id: u32, generation: u64) -> Result<Self, ClientConnectionTokenError> {
        if connection_id == 0 {
            return Err(ClientConnectionTokenError::ZeroConnectionId);
        }
        if generation == 0 {
            return Err(ClientConnectionTokenError::ZeroGeneration);
        }
        Ok(Self {
            connection_id,
            generation,
        })
    }

    pub const fn connection_id(self) -> u32 {
        self.connection_id
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientConnectionTokenError {
    ZeroConnectionId,
    ZeroGeneration,
}

impl fmt::Display for ClientConnectionTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroConnectionId => formatter.write_str("client connection ID must be non-zero"),
            Self::ZeroGeneration => {
                formatter.write_str("client connection generation must be non-zero")
            }
        }
    }
}

impl std::error::Error for ClientConnectionTokenError {}

/// The protocol-lifecycle reason that has won termination of a connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientConnectionTerminationReason {
    ExplicitKillConnection {
        requester_session: crate::session_control::ClientSessionKey,
    },
    ServerShutdown,
}

/// The synchronous result of attempting to latch connection termination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientConnectionTerminateOutcome {
    Requested,
    AlreadyTerminating,
    Stale,
}

/// A protocol-owned capability for terminating one exact client connection.
// Design: ADR-0102 (docs/adr/ADR-0102-mysql-kill-connection-lifecycle-ownership.md)
pub trait ClientConnectionControlPort: Send + Sync + 'static {
    fn terminate(
        &self,
        target: ClientConnectionToken,
        reason: ClientConnectionTerminationReason,
    ) -> ClientConnectionTerminateOutcome;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_rejects_zero_identity_components() {
        assert_eq!(
            ClientConnectionToken::new(0, 1),
            Err(ClientConnectionTokenError::ZeroConnectionId)
        );
        assert_eq!(
            ClientConnectionToken::new(1, 0),
            Err(ClientConnectionTokenError::ZeroGeneration)
        );
    }

    #[test]
    fn token_preserves_exact_identity() {
        let token = ClientConnectionToken::new(17, 23).expect("valid token");
        assert_eq!(token.connection_id(), 17);
        assert_eq!(token.generation(), 23);
        assert_ne!(
            token,
            ClientConnectionToken::new(17, 24).expect("valid token")
        );
    }
}
