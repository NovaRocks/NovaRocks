use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::dictionary::DictionarySnapshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodeMapping {
    pub source_column_id: ColumnId,
    pub output_column_id: ColumnId,
    pub dict_column: String,
    pub string_column: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

#[derive(Clone, Debug)]
pub(crate) struct ScanDictionaryColumn {
    pub source_column: String,
    pub dict_column: String,
    pub dictionary: Arc<DictionarySnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanVariantColumn {
    pub source_column_id: ColumnId,
    pub source_column: String,
    pub synthetic_column_id: ColumnId,
    pub synthetic_column: String,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
}
