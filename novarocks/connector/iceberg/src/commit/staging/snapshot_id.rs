// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::iceberg::spec::TableMetadata;

/// An attempt owns this ID; refreshed metadata is checked before every allocation.
pub fn new_snapshot_id(metadata: &TableMetadata) -> i64 {
    loop {
        let id = (rand::random::<u64>() & i64::MAX as u64) as i64;
        if id != 0 && metadata.snapshot_by_id(id).is_none() {
            return id;
        }
    }
}
