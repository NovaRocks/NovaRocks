use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;

pub(crate) type CteId = u32;

#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,
}
