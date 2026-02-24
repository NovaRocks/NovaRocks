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
use crate::descriptors;

pub const ROW_SOURCE_ID_COL: &str = "_row_source_id";
pub const SCAN_RANGE_ID_COL: &str = "_scan_range_id";
pub const ROW_ID_COL: &str = "_row_id";

pub fn is_row_source_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ROW_SOURCE_ID_COL)
}

pub fn is_scan_range_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(SCAN_RANGE_ID_COL)
}

pub fn is_row_id(name: &str) -> bool {
    name.eq_ignore_ascii_case(ROW_ID_COL)
}

#[derive(Clone, Debug)]
pub struct RowPositionDescriptor {
    pub row_position_type: descriptors::TRowPositionType,
    pub row_source_slot: SlotId,
    pub fetch_ref_slots: Vec<SlotId>,
    pub lookup_ref_slots: Vec<SlotId>,
}

#[derive(Clone, Debug)]
pub struct RowPositionSpec {
    pub row_source_slot: SlotId,
    pub scan_range_slot: SlotId,
    pub row_id_slot: SlotId,
    pub row_source_field: Field,
    pub scan_range_field: Field,
    pub row_id_field: Field,
}
