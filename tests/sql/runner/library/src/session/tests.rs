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

use super::session_tests::{Peer, Reply, generic_user_expectation, trino_error};

/// Observes the runner's private error evaluation against the terminal protocol failure.
#[test]
fn trino_error_message_prefix_must_match_the_same_terminal_protocol_error() {
    let peer = Peer::start(vec![
        Reply::queued("/v1/statement", "/bad"),
        Reply::json("GET", "/bad", &trino_error("original marker")),
    ]);
    let mut session = peer.session();
    let outcome = session.execute_query(9, "SELECT bad", None);
    let mut expectation = generic_user_expectation();
    for (prefix, matches) in [("original", true), ("marker", false), ("wrong", false)] {
        expectation.expect_error = Some(prefix.into());
        let result =
            crate::evaluate_expected_error_outcome(&expectation, false, &outcome.2, Some(&outcome))
                .unwrap();
        assert_eq!(result.is_ok(), matches, "{result:?}");
    }
    assert_eq!(peer.finish().len(), 2);
}
