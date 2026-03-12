use arrow::datatypes::Field;

use crate::common::ids::SlotId;

#[cfg(test)]
pub fn field_with_slot_id(field: Field, slot_id: SlotId) -> Field {
    let slot_id = slot_id
        .to_string()
        .parse::<i64>()
        .expect("test slot id must fit i64");
    #[allow(deprecated)]
    {
        Field::new_dict(
            field.name().to_string(),
            field.data_type().clone(),
            field.is_nullable(),
            slot_id,
            false,
        )
    }
}
