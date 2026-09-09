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

//! Counting global allocator (MEM-1 wave-1 T04).
//!
//! Wraps the selected allocator and counts successful allocations, releases
//! and reallocation deltas. A failure keeps the previous accounting. The
//! callback allocates nothing, waits for nothing and calls no business code.
//! Per-shard accumulation is a contention measure, not thread attribution: it
//! never implies which work owns a byte.
