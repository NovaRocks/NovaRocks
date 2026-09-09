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

//! Coverage and blind spots (MEM-1 wave-1 T04).
//!
//! Every allocator report states its coverage and lists what it cannot see:
//! direct `System` calls, custom allocators, native libraries and direct
//! mappings, plus allocator-internal retention and fragmentation. Where a
//! source can be measured its statistics are added as their own line; where it
//! cannot, the value stays unknown rather than being folded into a total.
