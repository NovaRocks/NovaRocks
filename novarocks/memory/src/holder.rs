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

//! Shared retention and pins (MEM-1 wave-1 T03).
//!
//! A retention lease records that a scope actually holds shared backing; a pin
//! protects a specific resident frame and its address. Both are exposure, not
//! new physical charges: the same resource may appear in several scopes and
//! those numbers must never be summed. The last pin leaving means only that a
//! frame became unloadable — the real release and the capacity return are
//! separate events.
