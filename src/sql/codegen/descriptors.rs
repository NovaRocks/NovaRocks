use arrow::datatypes::DataType;
use std::collections::BTreeSet;

use crate::descriptors;
use crate::types;

use super::type_infer::arrow_type_to_type_desc;

pub(crate) struct DescriptorTableBuilder {
    slots: Vec<descriptors::TSlotDescriptor>,
    tuples: Vec<descriptors::TTupleDescriptor>,
    tables: Vec<descriptors::TTableDescriptor>,
    table_ids: BTreeSet<types::TTableId>,
}

impl DescriptorTableBuilder {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            tuples: Vec::new(),
            tables: Vec::new(),
            table_ids: BTreeSet::new(),
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
        self.add_slot_with_type_desc(slot_id, tuple_id, name, slot_type, nullable, col_pos);
    }

    pub fn add_slot_with_type_desc(
        &mut self,
        slot_id: types::TSlotId,
        tuple_id: types::TTupleId,
        name: &str,
        slot_type: types::TTypeDesc,
        nullable: bool,
        col_pos: i32,
    ) {
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
            None::<bool>,
        ));
    }

    pub fn add_tuple(&mut self, tuple_id: types::TTupleId, table_id: Option<types::TTableId>) {
        self.tuples.push(descriptors::TTupleDescriptor::new(
            Some(tuple_id),
            Some(0), // byte_size
            Some(0), // num_null_bytes
            table_id,
            Some(0), // num_null_slots
        ));
    }

    pub fn add_table(
        &mut self,
        table_id: types::TTableId,
        db_name: &str,
        table_name: &str,
        num_cols: i32,
    ) {
        if !self.table_ids.insert(table_id) {
            return;
        }
        self.tables.push(descriptors::TTableDescriptor::new(
            table_id,
            types::TTableType::OLAP_TABLE,
            num_cols,
            0,
            table_name.to_string(),
            db_name.to_string(),
            None::<descriptors::TMySQLTable>,
            None::<descriptors::TOlapTable>,
            None::<descriptors::TSchemaTable>,
            None::<descriptors::TBrokerTable>,
            None::<descriptors::TEsTable>,
            None::<descriptors::TJDBCTable>,
            None::<descriptors::THdfsTable>,
            None::<descriptors::TIcebergTable>,
            None::<descriptors::THudiTable>,
            None::<descriptors::TDeltaLakeTable>,
            None::<descriptors::TFileTable>,
            None::<descriptors::TTableFunctionTable>,
            None::<descriptors::TPaimonTable>,
        ));
    }

    /// Mark all slots belonging to the given tuple as nullable.
    /// Used for outer/anti join nullable side columns.
    pub fn widen_tuple_nullable(&mut self, tuple_id: types::TTupleId) {
        for slot in &mut self.slots {
            if slot.parent == Some(tuple_id) {
                slot.is_nullable = Some(true);
            }
        }
    }

    pub fn build(self) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable::new(
            Some(self.slots),
            self.tuples,
            if self.tables.is_empty() {
                None::<Vec<descriptors::TTableDescriptor>>
            } else {
                Some(self.tables)
            },
            None::<bool>,
        )
    }
}
