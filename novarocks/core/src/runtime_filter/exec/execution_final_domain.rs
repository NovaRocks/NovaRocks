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

use novarocks_execution::runtime_filter as execution;

use crate::runtime_filter::port::value_domain::ValueDomainDelta;

/// Converts Core's current value-domain implementation into the opaque
/// Execution completion payload. The Service adapter validates this exact
/// native payload before it freezes a partition; neither callers nor this
/// helper receives an issuance authority.
pub fn final_domain_payload(
    domain: ValueDomainDelta,
) -> Result<execution::RuntimeFilterFinalDomain, String> {
    let mut canonical = Vec::new();
    domain
        .encode_canonical_into(&mut canonical)
        .map_err(|error| error.to_string())?;
    Ok(execution::RuntimeFilterFinalDomain::new(canonical, domain))
}
