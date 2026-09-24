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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use novarocks_types::SlotId;

/// Immutable Arrow schema plus exact execution slot order. No Chunk or lease
/// is retained by this type.
#[derive(Clone, Debug)]
pub struct StaticLayout {
    schema: SchemaRef,
    slots: Arc<[SlotId]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LayoutError {
    ArityMismatch,
    DuplicateSlot,
}

impl fmt::Display for LayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ArityMismatch => "static layout field and slot counts differ",
            Self::DuplicateSlot => "static layout contains a duplicate slot",
        })
    }
}

impl std::error::Error for LayoutError {}

impl StaticLayout {
    pub fn try_new(schema: SchemaRef, slots: Arc<[SlotId]>) -> Result<Self, LayoutError> {
        if schema.fields().len() != slots.len() {
            return Err(LayoutError::ArityMismatch);
        }
        let unique = slots.iter().copied().collect::<BTreeSet<_>>();
        if unique.len() != slots.len() {
            return Err(LayoutError::DuplicateSlot);
        }
        Ok(Self { schema, slots })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn slots(&self) -> &[SlotId] {
        &self.slots
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn rejects_layout_that_cannot_bind_columns_exactly() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        assert_eq!(
            StaticLayout::try_new(Arc::clone(&schema), Arc::from([SlotId::new(1)])).unwrap_err(),
            LayoutError::ArityMismatch
        );
        assert_eq!(
            StaticLayout::try_new(schema, Arc::from([SlotId::new(1), SlotId::new(1)])).unwrap_err(),
            LayoutError::DuplicateSlot
        );
    }
}
