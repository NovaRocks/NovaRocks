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

//! The role-local, immutable session view handed to a connector.
//!
//! A connector session never carries a credential, secret, or storage
//! property: object-store access stays with the Server/Backend binding owner.
//! Cancellation and deadlines stay with the existing query runtime rather than
//! becoming connector method arguments.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use crate::connector::{ConnectorError, ConnectorErrorKind};

use super::value::ConnectorValue;

pub const MAX_SESSION_PROPERTIES: usize = 1024;

/// One typed session property value.
pub type SessionPropertyValue = ConnectorValue;
/// Exact catalog's admitted overrides for one connector request.
pub type ConnectorCatalogSession = BTreeMap<Arc<str>, SessionPropertyValue>;

/// An immutable, role-local view of the session driving connector work.
#[derive(Clone, Debug)]
pub struct ConnectorSession {
    query_id: Arc<str>,
    user: Arc<str>,
    source: Option<Arc<str>>,
    time_zone_id: Arc<str>,
    locale: Arc<str>,
    trace_token: Option<Arc<str>>,
    start_time: SystemTime,
    properties: BTreeMap<Arc<str>, SessionPropertyValue>,
}

impl ConnectorSession {
    pub fn try_new(
        query_id: impl AsRef<str>,
        user: impl AsRef<str>,
        time_zone_id: impl AsRef<str>,
        locale: impl AsRef<str>,
        start_time: SystemTime,
    ) -> Result<Self, ConnectorError> {
        let query_id = query_id.as_ref();
        if query_id.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector session query ID must not be empty",
            ));
        }
        Ok(Self {
            query_id: Arc::from(query_id),
            user: Arc::from(user.as_ref()),
            source: None,
            time_zone_id: Arc::from(time_zone_id.as_ref()),
            locale: Arc::from(locale.as_ref()),
            trace_token: None,
            start_time,
            properties: BTreeMap::new(),
        })
    }

    pub fn with_source(mut self, source: impl AsRef<str>) -> Self {
        self.source = Some(Arc::from(source.as_ref()));
        self
    }

    pub fn with_trace_token(mut self, trace_token: impl AsRef<str>) -> Self {
        self.trace_token = Some(Arc::from(trace_token.as_ref()));
        self
    }

    pub fn try_with_properties(
        mut self,
        properties: BTreeMap<Arc<str>, SessionPropertyValue>,
    ) -> Result<Self, ConnectorError> {
        if properties.len() > MAX_SESSION_PROPERTIES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector session property count exceeds the hard limit",
            ));
        }
        self.properties = properties;
        Ok(self)
    }

    pub fn query_id(&self) -> &str {
        &self.query_id
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn time_zone_id(&self) -> &str {
        &self.time_zone_id
    }

    pub fn locale(&self) -> &str {
        &self.locale
    }

    pub fn trace_token(&self) -> Option<&str> {
        self.trace_token.as_deref()
    }

    pub const fn start_time(&self) -> SystemTime {
        self.start_time
    }

    pub fn property(&self, name: &str) -> Option<&SessionPropertyValue> {
        self.properties.get(name)
    }

    pub const fn properties(&self) -> &BTreeMap<Arc<str>, SessionPropertyValue> {
        &self.properties
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sessions_require_a_query_id_and_expose_no_secret_surface() {
        assert!(
            ConnectorSession::try_new("", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH).is_err()
        );
        let session = ConnectorSession::try_new("q1", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("valid session")
            .with_source("cli");
        assert_eq!(session.query_id(), "q1");
        assert_eq!(session.source(), Some("cli"));
        assert!(session.property("aws.secret").is_none());
    }

    #[test]
    fn session_property_count_is_bounded() {
        let mut properties = BTreeMap::new();
        for index in 0..=MAX_SESSION_PROPERTIES {
            properties.insert(
                Arc::from(format!("p{index}").as_str()),
                ConnectorValue::BigInt(index as i64),
            );
        }
        let session = ConnectorSession::try_new("q", "u", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("valid session");
        assert!(session.try_with_properties(properties).is_err());
    }
}
