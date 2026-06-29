use std::collections::HashMap;

use arrow::datatypes::Field;

use crate::common::ids::SlotId;
use crate::exec::row_position::RowPositionDescriptor;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DescriptorLogicalType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    LargeInt,
    Float32,
    Float64,
    Date,
    Timestamp,
    Time,
    Decimal128 { precision: u8, scale: i8 },
    Decimal256 { precision: u8, scale: i8 },
    Utf8,
    Binary,
    Json,
    Variant,
    Hll,
    Object,
    Percentile,
    Function,
    Unknown,
}

impl DescriptorLogicalType {
    #[allow(dead_code)]
    pub(crate) fn is_int32(&self) -> bool {
        matches!(self, Self::Int32)
    }

    #[allow(dead_code)]
    pub(crate) fn is_int64(&self) -> bool {
        matches!(self, Self::Int64)
    }

    #[allow(dead_code)]
    pub(crate) fn is_int8(&self) -> bool {
        matches!(self, Self::Int8)
    }

    #[allow(dead_code)]
    pub(crate) fn is_variant(&self) -> bool {
        matches!(self, Self::Variant)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DescriptorSlot {
    pub(crate) tuple_id: i32,
    pub(crate) slot_id: SlotId,
    pub(crate) name: String,
    pub(crate) field: Field,
    pub(crate) logical: DescriptorLogicalType,
    pub(crate) unique_id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DescriptorIcebergSchemaField {
    pub(crate) field_id: Option<i32>,
    pub(crate) name: Option<String>,
    pub(crate) initial_default_json: Option<String>,
    pub(crate) children: Option<Vec<DescriptorIcebergSchemaField>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DescriptorIcebergSchema {
    pub(crate) fields: Option<Vec<DescriptorIcebergSchemaField>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DescriptorTableKind {
    Iceberg,
    Paimon,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DescriptorTable {
    pub(crate) id: i64,
    pub(crate) kind: DescriptorTableKind,
    pub(crate) iceberg_schema: Option<DescriptorIcebergSchema>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DescriptorSnapshot {
    slots_by_tuple_slot: HashMap<(i32, SlotId), DescriptorSlot>,
    slots_by_tuple: HashMap<i32, Vec<SlotId>>,
    tuple_to_table: HashMap<i32, i64>,
    tables_by_id: HashMap<i64, DescriptorTable>,
}

impl DescriptorSnapshot {
    pub(crate) fn new(
        slots: Vec<DescriptorSlot>,
        tuple_to_table: HashMap<i32, i64>,
    ) -> Result<Self, String> {
        Self::new_with_tables(slots, tuple_to_table, Vec::new())
    }

    pub(crate) fn new_with_tables(
        slots: Vec<DescriptorSlot>,
        tuple_to_table: HashMap<i32, i64>,
        tables: Vec<DescriptorTable>,
    ) -> Result<Self, String> {
        let mut slots_by_tuple_slot = HashMap::with_capacity(slots.len());
        let mut slots_by_tuple: HashMap<i32, Vec<SlotId>> = HashMap::new();
        let mut tables_by_id = HashMap::with_capacity(tables.len());

        for slot in slots {
            let key = (slot.tuple_id, slot.slot_id);
            if slots_by_tuple_slot.insert(key, slot).is_some() {
                return Err(format!(
                    "duplicate descriptor slot tuple_id={} slot_id={}",
                    key.0, key.1
                ));
            }
            slots_by_tuple.entry(key.0).or_default().push(key.1);
        }

        for tuple_slots in slots_by_tuple.values_mut() {
            tuple_slots.sort_by_key(|slot_id| slot_id.as_u32());
        }

        for table in tables {
            let table_id = table.id;
            if tables_by_id.insert(table_id, table).is_some() {
                return Err(format!("duplicate descriptor table id={table_id}"));
            }
        }

        Ok(Self {
            slots_by_tuple_slot,
            slots_by_tuple,
            tuple_to_table,
            tables_by_id,
        })
    }

    pub(crate) fn slot(&self, tuple_id: i32, slot_id: SlotId) -> Option<&DescriptorSlot> {
        self.slots_by_tuple_slot.get(&(tuple_id, slot_id))
    }

    pub(crate) fn tuple_slots(&self, tuple_id: i32) -> &[SlotId] {
        self.slots_by_tuple
            .get(&tuple_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn table_id_for_tuple(&self, tuple_id: i32) -> Option<i64> {
        self.tuple_to_table.get(&tuple_id).copied()
    }

    pub(crate) fn table_for_tuple(&self, tuple_id: i32) -> Option<&DescriptorTable> {
        let table_id = self.table_id_for_tuple(tuple_id)?;
        self.tables_by_id.get(&table_id)
    }

    pub(crate) fn is_paimon_table_for_tuple(&self, tuple_id: i32) -> bool {
        self.table_for_tuple(tuple_id)
            .is_some_and(|table| matches!(table.kind, DescriptorTableKind::Paimon))
    }

    pub(crate) fn iceberg_schema_for_tuple(
        &self,
        tuple_id: i32,
    ) -> Option<&DescriptorIcebergSchema> {
        let table = self.table_for_tuple(tuple_id)?;
        match table.kind {
            DescriptorTableKind::Iceberg => table.iceberg_schema.as_ref(),
            DescriptorTableKind::Paimon | DescriptorTableKind::Other => None,
        }
    }

    pub(crate) fn is_iceberg_table_for_tuple(&self, tuple_id: i32) -> bool {
        self.table_for_tuple(tuple_id)
            .is_some_and(|table| matches!(table.kind, DescriptorTableKind::Iceberg))
    }

    pub(crate) fn lookup_output_slots(
        &self,
        tuple_id: i32,
        row_pos_desc: &RowPositionDescriptor,
    ) -> Vec<SlotId> {
        let mut out = Vec::new();
        for slot_id in self.tuple_slots(tuple_id) {
            if *slot_id == row_pos_desc.row_source_slot
                || row_pos_desc.fetch_ref_slots.contains(slot_id)
                || row_pos_desc.lookup_ref_slots.contains(slot_id)
            {
                continue;
            }
            out.push(*slot_id);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field};

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::row_position::{RowPositionDescriptor, RowPositionType};

    fn descriptor_slot(tuple_id: i32, slot_id: u32, name: &str) -> DescriptorSlot {
        DescriptorSlot {
            tuple_id,
            slot_id: SlotId::new(slot_id),
            name: name.to_string(),
            field: Field::new(name, DataType::Int32, true),
            logical: DescriptorLogicalType::Int32,
            unique_id: Some(slot_id as i32 + 100),
        }
    }

    #[test]
    fn domain_model_sorts_tuple_slots_and_filters_lookup_outputs() {
        let snapshot = DescriptorSnapshot::new(
            vec![
                descriptor_slot(2, 8, "payload"),
                descriptor_slot(2, 3, "row_source"),
                descriptor_slot(2, 5, "fetch_ref"),
                descriptor_slot(4, 1, "other"),
            ],
            HashMap::from([(2, 10), (4, 40)]),
        )
        .expect("snapshot");

        assert_eq!(
            snapshot.tuple_slots(2),
            &[SlotId::new(3), SlotId::new(5), SlotId::new(8)]
        );
        assert_eq!(snapshot.table_id_for_tuple(2), Some(10));
        assert_eq!(
            snapshot.slot(2, SlotId::new(8)).expect("slot").name,
            "payload"
        );

        let row_pos = RowPositionDescriptor {
            row_position_type: RowPositionType::Iceberg,
            row_source_slot: SlotId::new(3),
            fetch_ref_slots: vec![SlotId::new(5)],
            lookup_ref_slots: vec![SlotId::new(1)],
        };
        assert_eq!(
            snapshot.lookup_output_slots(2, &row_pos),
            vec![SlotId::new(8)]
        );
    }

    #[test]
    fn domain_model_rejects_duplicate_tuple_slot_keys() {
        let err = DescriptorSnapshot::new(
            vec![
                descriptor_slot(2, 8, "payload"),
                descriptor_slot(2, 8, "payload_dup"),
            ],
            HashMap::new(),
        )
        .expect_err("duplicate slot should fail");

        assert!(
            err.contains("duplicate descriptor slot tuple_id=2 slot_id=8"),
            "got: {err}"
        );
    }

    #[test]
    fn domain_model_indexes_table_view_by_tuple() {
        let snapshot = DescriptorSnapshot::new_with_tables(
            vec![descriptor_slot(2, 8, "payload")],
            HashMap::from([(2, 10), (4, 40)]),
            vec![
                DescriptorTable {
                    id: 10,
                    kind: DescriptorTableKind::Iceberg,
                    iceberg_schema: Some(DescriptorIcebergSchema { fields: None }),
                },
                DescriptorTable {
                    id: 40,
                    kind: DescriptorTableKind::Paimon,
                    iceberg_schema: None,
                },
            ],
        )
        .expect("snapshot");

        assert!(snapshot.is_iceberg_table_for_tuple(2));
        assert!(snapshot.iceberg_schema_for_tuple(2).is_some());
        assert!(snapshot.is_paimon_table_for_tuple(4));
        assert!(!snapshot.is_paimon_table_for_tuple(2));
    }

    #[test]
    fn domain_model_rejects_duplicate_table_ids() {
        let err = DescriptorSnapshot::new_with_tables(
            Vec::new(),
            HashMap::new(),
            vec![
                DescriptorTable {
                    id: 10,
                    kind: DescriptorTableKind::Other,
                    iceberg_schema: None,
                },
                DescriptorTable {
                    id: 10,
                    kind: DescriptorTableKind::Paimon,
                    iceberg_schema: None,
                },
            ],
        )
        .expect_err("duplicate table should fail");

        assert!(
            err.contains("duplicate descriptor table id=10"),
            "got: {err}"
        );
    }
}
