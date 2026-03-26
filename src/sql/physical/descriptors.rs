use arrow::datatypes::DataType;

use crate::descriptors;
use crate::types;

use super::type_infer::arrow_type_to_type_desc;

pub(crate) struct DescriptorTableBuilder {
    slots: Vec<descriptors::TSlotDescriptor>,
    tuples: Vec<descriptors::TTupleDescriptor>,
}

impl DescriptorTableBuilder {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            tuples: Vec::new(),
        }
    }

    pub fn add_slot(
        &mut self,
        slot_id: types::TSlotId,
        tuple_id: types::TTupleId,
        name: &str,
        data_type: &DataType,
        nullable: bool,
        col_pos: i32,
    ) {
        let slot_type = match arrow_type_to_type_desc(data_type) {
            Ok(t) => t,
            Err(_) => return, // skip unsupported types
        };
        self.slots.push(descriptors::TSlotDescriptor::new(
            Some(slot_id),
            Some(tuple_id),
            Some(slot_type),
            Some(col_pos),
            Some(0), // byte_offset
            Some(0), // null_indicator_byte
            Some(0), // null_indicator_bit
            Some(name.to_string()),
            Some(col_pos),
            Some(true), // is_materialized
            Some(true), // is_output_column
            Some(nullable),
            None::<i32>,
            None::<String>,
        ));
    }

    pub fn add_tuple(&mut self, tuple_id: types::TTupleId) {
        self.tuples.push(descriptors::TTupleDescriptor::new(
            Some(tuple_id),
            Some(0), // byte_size
            Some(0), // num_null_bytes
            None::<types::TTableId>,
            Some(0), // num_null_slots
        ));
    }

    pub fn build(self) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            Some(self.slots),
            self.tuples,
            None::<Vec<descriptors::TTableDescriptor>>,
            None::<bool>,
        )
    }
}
