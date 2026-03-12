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
use arrow::datatypes::Field;

use crate::common::ids::SlotId;

pub const FIELD_META_SLOT_ID: &str = "novarocks.slot_id";
pub const FIELD_META_STARROCKS_COLUMN_ID: &str = "starrocks.format.column.id";

pub fn field_with_slot_id(field: Field, slot_id: SlotId) -> Field {
    let mut meta = field.metadata().clone();
    meta.insert(FIELD_META_SLOT_ID.to_string(), slot_id.to_string());
    field.with_metadata(meta)
}

pub fn field_slot_id(field: &Field) -> Result<Option<SlotId>, String> {
    let Some(v) = field.metadata().get(FIELD_META_SLOT_ID) else {
        return Ok(None);
    };
    Ok(Some(v.parse::<SlotId>()?))
}

pub(super) fn field_unique_id(field: &Field) -> Result<Option<i32>, String> {
    let Some(value) = field.metadata().get(FIELD_META_STARROCKS_COLUMN_ID) else {
        return Ok(None);
    };
    let parsed = value.parse::<i32>().map_err(|e| {
        format!(
            "invalid {}='{}' for field '{}': {}",
            FIELD_META_STARROCKS_COLUMN_ID,
            value,
            field.name(),
            e
        )
    })?;
    Ok(Some(parsed))
}
