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

pub use novarocks_connector_contract::{
    ConnectorIdentityError, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
};

#[cfg(test)]
mod tests {
    use super::ConnectorInstanceId;

    #[test]
    fn catalog_admission_normalizes_but_wire_ingress_requires_canonical_form() {
        assert_eq!(
            ConnectorInstanceId::parse("MyCatalog.Analytics")
                .unwrap()
                .as_str(),
            "mycatalog.analytics"
        );
        assert!(ConnectorInstanceId::try_from_canonical("MyCatalog.Analytics").is_err());
        assert_eq!(
            ConnectorInstanceId::try_from_canonical("mycatalog.analytics")
                .unwrap()
                .as_str(),
            "mycatalog.analytics"
        );
    }
}
