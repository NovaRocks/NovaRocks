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

//! Ordered original Scan residuals use the existing conjunction consumer.
//! This isolated required predicate preserves an explicit optimizer variant;
//! it does not claim native whole-case or provider execution acceptance.
use novarocks_sql_testkit::{inspect_ordered_scan_source, ordered_scan_source};

#[test]
fn scan_ordered_required_source_keeps_original_nullable_field_and_residuals() {
    inspect_ordered_scan_source(&ordered_scan_source());
}
