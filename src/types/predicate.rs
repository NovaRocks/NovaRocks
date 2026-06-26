use arrow::datatypes::DataType;

pub(crate) fn is_largeint(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::FixedSizeBinary(width)
            if *width == crate::common::largeint::LARGEINT_BYTE_WIDTH
    )
}

pub(crate) fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}
