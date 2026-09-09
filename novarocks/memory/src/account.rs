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

//! The strict account tree (MEM-1 wave-1 T02).
//!
//! Hard limits and capacity propagate along one tree: a process root with
//! resource-group, work, attempt, task and owner branches, plus service and
//! preparation branches for state that has no attempt. A work belongs to
//! exactly one hard-limit group at a time. Components, resource classes and
//! holder exposure are observation labels, not extra tree edges, so no
//! allocation ever evaluates a general graph.
//!
//! Ordinary requests are fulfilled from an account's own local free slack and
//! only touch the parent chain when that slack is topped up or returned, which
//! is what keeps the root off the hot path.
