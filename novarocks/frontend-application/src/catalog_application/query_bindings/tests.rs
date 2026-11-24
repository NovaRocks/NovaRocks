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

use super::*;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

#[test]
fn object_failure_is_memoized_without_publishing_a_token() {
    let store = QueryTableBindingStore::try_new().unwrap();
    let key = QueryTableBindingKey::strict_base("ice", "db", "orders");
    let missing = CatalogResolutionError::missing("missing table");
    let first = store.resolve_catalog_binding(key.clone(), |_| Err(missing.clone()));
    let second = store.resolve_catalog_binding(key, |_| panic!("object outcome loaded twice"));
    assert_eq!(first.unwrap_err(), missing);
    assert_eq!(second.unwrap_err(), missing);
    assert!(store.captured_bindings().is_empty());
}

#[test]
fn control_failure_can_be_retried_with_the_original_kind() {
    let store = QueryTableBindingStore::try_new().unwrap();
    let key = QueryTableBindingKey::strict_base("ice", "db", "orders");
    let interrupted = CatalogResolutionError::from_connector(ConnectorError::new(
        ConnectorErrorKind::Interrupted,
        "statement observation interrupted",
    ));
    let first = store.resolve_catalog_binding(key.clone(), |_| Err(interrupted.clone()));
    assert_eq!(first.unwrap_err(), interrupted);
    assert!(store.resolved_lookup(&key).is_none());
    let missing = CatalogResolutionError::missing("missing table");
    let retry = store.resolve_catalog_binding(key, |_| Err(missing.clone()));
    assert_eq!(retry.unwrap_err(), missing);
}
