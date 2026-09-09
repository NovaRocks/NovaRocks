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

//! Issued capacity (MEM-1 wave-1 T02).
//!
//! A grant is one already-issued, non-reassignable amount plus the right to
//! fulfil or return it. It is an RAII value: dropping it returns the
//! unfulfilled remainder to its account. Requests that are not yet granted,
//! and the tickets that wait for them, belong to the arbitrator, not here —
//! a waiting request holds no capacity at all.
