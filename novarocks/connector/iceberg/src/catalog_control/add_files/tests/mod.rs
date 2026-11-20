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

#[allow(dead_code, reason = "P08 reconnects the source I/O verification")]
pub(crate) mod fixtures;

#[test]
fn location_accepts_at_signs_in_paths_and_rejects_embedded_credentials() {
    use super::validate_registration_location;

    validate_registration_location("s3://bucket/part@2.parquet").expect("ordinary path");
    let credentials = validate_registration_location("s3://user:secret@bucket/file").unwrap_err();
    assert!(credentials.message().contains("must not embed credentials"));
    let token =
        validate_registration_location("s3://bucket/file?session_token=secret").unwrap_err();
    assert!(token.message().contains("must not embed credentials"));
    assert!(!token.message().contains("=secret"));
}
