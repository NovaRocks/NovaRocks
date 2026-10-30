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

//! Cross-product contract: every active SQL error must have an adapter mapping.

use super::error_kind_for_domain_code;
use std::collections::BTreeSet;

use novarocks_parser::ERROR_CODE_DESCRIPTORS as PARSER_ERROR_CODE_DESCRIPTORS;
use novarocks_query_application::sql::dml_admission::DML_ADMISSION_ERROR_CODE_DESCRIPTORS;
use novarocks_query_application::sql::session_admit::SESSION_ERROR_CODE_DESCRIPTORS;
use novarocks_sql::analyze_error::ERROR_CODE_DESCRIPTORS as ANALYZE_ERROR_CODE_DESCRIPTORS;
use novarocks_user_error::ErrorCodeStatus;

#[test]
fn every_active_manifest_descriptor_has_exactly_one_adapter_wire_mapping() {
    let descriptor_codes = PARSER_ERROR_CODE_DESCRIPTORS
        .iter()
        .chain(ANALYZE_ERROR_CODE_DESCRIPTORS)
        .chain(DML_ADMISSION_ERROR_CODE_DESCRIPTORS)
        .chain(SESSION_ERROR_CODE_DESCRIPTORS)
        .chain(novarocks_query_application::execution_error::EXECUTION_ERROR_CODE_DESCRIPTORS)
        .filter(|descriptor| descriptor.status == ErrorCodeStatus::Active)
        .map(|descriptor| descriptor.code.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        descriptor_codes.len(),
        32 + novarocks_query_application::execution_error::EXECUTION_ERROR_CODE_DESCRIPTORS.len()
    );
    for code in descriptor_codes {
        assert!(
            error_kind_for_domain_code(code).is_some(),
            "active descriptor `{code}` must have one MySQL wire mapping"
        );
    }
}

#[test]
fn execution_registry_preserves_exact_errno_and_sqlstate() {
    use novarocks_query_application::execution_error::{
        EXECUTION_ERROR_CODE_DESCRIPTORS, execution_descriptor_for_code,
    };
    for entry in EXECUTION_ERROR_CODE_DESCRIPTORS {
        let expected = execution_descriptor_for_code(entry.code).unwrap();
        let actual = error_kind_for_domain_code(entry.code.as_str()).unwrap();
        assert_eq!(actual as u16, expected.errno, "{}", entry.code);
        assert_eq!(
            actual.sqlstate().as_slice(),
            expected.sqlstate.as_bytes(),
            "{}",
            entry.code
        );
        assert_eq!(entry.phase, novarocks_user_error::ErrorPhase::Execution);
    }
}
