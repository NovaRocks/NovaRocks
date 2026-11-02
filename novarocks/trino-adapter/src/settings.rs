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

use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoListenerSettings {
    pub port: u16,
    pub user: String,
    pub client_timeout: Duration,
}
impl Default for TrinoListenerSettings {
    fn default() -> Self {
        Self {
            port: 8080,
            user: "root".into(),
            client_timeout: Duration::from_millis(300_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_match_the_client_entry_contract() {
        let settings = TrinoListenerSettings::default();
        assert_eq!(settings.port, 8080);
        assert_eq!(settings.user, "root");
        assert_eq!(settings.client_timeout, Duration::from_secs(300));
    }
}
