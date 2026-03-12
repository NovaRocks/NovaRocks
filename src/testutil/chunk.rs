use arrow::datatypes::Field;

use crate::common::ids::SlotId;

#[cfg(test)]
pub fn field_with_slot_id(field: Field, slot_id: SlotId) -> Field {
    crate::exec::chunk::field_with_slot_id(field, slot_id)
}
