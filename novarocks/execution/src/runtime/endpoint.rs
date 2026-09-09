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

pub use novarocks_execution_contract::RuntimeEndpoint;
use novarocks_types::UniqueId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentDestination {
    finst_id: UniqueId,
    endpoint: RuntimeEndpoint,
    source_finst_id: UniqueId,
    sender_ordinal: u32,
    sender_count: u32,
}

impl FragmentDestination {
    pub fn new(
        finst_id: UniqueId,
        endpoint: RuntimeEndpoint,
        source_finst_id: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<Self, String> {
        if finst_id == UniqueId::new(0, 0) || source_finst_id == UniqueId::new(0, 0) {
            return Err(
                "fragment destination requires non-zero source and destination IDs".to_string(),
            );
        }
        if sender_count == 0 || sender_ordinal >= sender_count {
            return Err("fragment destination sender ordinal/count is invalid".to_string());
        }
        Ok(Self {
            finst_id,
            endpoint,
            source_finst_id,
            sender_ordinal,
            sender_count,
        })
    }

    pub fn finst_id(&self) -> &UniqueId {
        &self.finst_id
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub const fn source_finst_id(&self) -> UniqueId {
        self.source_finst_id
    }

    pub const fn sender_ordinal(&self) -> u32 {
        self.sender_ordinal
    }

    pub const fn sender_count(&self) -> u32 {
        self.sender_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProberDestination {
    fragment_instance_id: UniqueId,
    endpoint: RuntimeEndpoint,
}

impl RuntimeFilterProberDestination {
    pub fn new(fragment_instance_id: UniqueId, endpoint: RuntimeEndpoint) -> Self {
        Self {
            fragment_instance_id,
            endpoint,
        }
    }

    pub fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_host_port_endpoint() {
        let endpoint = RuntimeEndpoint::parse("BE-1.Internal:8060").expect("endpoint");

        assert_eq!(endpoint.host(), "be-1.internal");
        assert_eq!(endpoint.port(), 8060);
        assert_eq!(endpoint.as_host_port(), "be-1.internal:8060");
    }

    #[test]
    fn rejects_missing_separator() {
        let err = RuntimeEndpoint::parse("be-1.internal").expect_err("missing separator");

        assert!(err.contains("must be host:port"), "{err}");
    }

    #[test]
    fn rejects_empty_host() {
        let err = RuntimeEndpoint::parse(":8060").expect_err("empty host");

        assert!(err.contains("reference host"), "{err}");
    }

    #[test]
    fn rejects_non_numeric_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:not-a-port").expect_err("invalid port");

        assert!(err.contains("invalid port"), "{err}");
    }

    #[test]
    fn rejects_i32_overflow_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:2147483648").expect_err("overflow port");

        assert!(err.contains("invalid port"), "{err}");
    }

    #[test]
    fn rejects_zero_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:0").expect_err("zero port");

        assert!(err.contains("must be in 1..=65535"), "{err}");
    }

    #[test]
    fn rejects_negative_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:-1").expect_err("negative port");

        assert!(err.contains("invalid port"), "{err}");
    }

    #[test]
    fn rejects_invalid_port() {
        let err = RuntimeEndpoint::parse("be-1.internal:70000").expect_err("invalid port");

        assert!(err.contains("invalid port"), "{err}");
    }
}
