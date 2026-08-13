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

use std::io;
use std::net::TcpListener;

/// Holds a loopback TCP port until a test is ready to hand it to a child.
///
/// Releasing the listener reduces the configuration-to-spawn race, but it is
/// not an atomic handoff. A child bind conflict remains a normal startup
/// failure and must be reported by the child process diagnostics.
#[derive(Debug)]
pub struct ReservedTcpPort {
    listener: TcpListener,
    port: u16,
}

impl ReservedTcpPort {
    pub fn new() -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let port = listener.local_addr()?.port();
        Ok(Self { listener, port })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn release(self) -> u16 {
        let Self { listener, port } = self;
        drop(listener);
        port
    }
}

#[cfg(test)]
mod tests {
    use super::ReservedTcpPort;
    use std::net::TcpListener;

    #[test]
    fn reservation_blocks_rebinding_until_release() {
        let reserved = ReservedTcpPort::new().expect("reserve TCP port");
        let port = reserved.port();

        assert!(TcpListener::bind(("127.0.0.1", port)).is_err());

        assert_eq!(reserved.release(), port);
        let rebound = TcpListener::bind(("127.0.0.1", port)).expect("bind released TCP port");
        drop(rebound);
    }

    #[test]
    fn independent_reservations_hold_distinct_live_ports() {
        let first = ReservedTcpPort::new().expect("reserve first TCP port");
        let second = ReservedTcpPort::new().expect("reserve second TCP port");

        assert_ne!(first.port(), second.port());
        assert!(TcpListener::bind(("127.0.0.1", first.port())).is_err());
        assert!(TcpListener::bind(("127.0.0.1", second.port())).is_err());
    }
}
