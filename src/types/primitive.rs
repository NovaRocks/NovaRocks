#![allow(dead_code)]

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum PrimitiveType {
    Invalid,
    Null,
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Int256,
    Float,
    Double,
    Date,
    DateTime,
    Time,
    Decimal,
    DecimalV2,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Char,
    Varchar,
    Binary,
    Varbinary,
    Json,
    Hll,
    Object,
    Percentile,
    Function,
    Variant,
}

impl PrimitiveType {
    pub(crate) fn is_opaque_binary(self) -> bool {
        matches!(self, Self::Hll | Self::Object | Self::Percentile)
    }

    pub(crate) fn is_json(self) -> bool {
        matches!(self, Self::Json)
    }

    pub(crate) fn is_largeint(self) -> bool {
        matches!(self, Self::LargeInt)
    }

    pub(crate) fn is_time(self) -> bool {
        matches!(self, Self::Time)
    }
}

#[cfg(test)]
mod tests {
    use super::PrimitiveType;

    #[test]
    fn primitive_type_marks_opaque_binary_family() {
        assert!(PrimitiveType::Hll.is_opaque_binary());
        assert!(PrimitiveType::Object.is_opaque_binary());
        assert!(PrimitiveType::Percentile.is_opaque_binary());
        assert!(!PrimitiveType::Varbinary.is_opaque_binary());
    }

    #[test]
    fn primitive_type_classifies_rendering_helpers() {
        assert!(PrimitiveType::Json.is_json());
        assert!(PrimitiveType::LargeInt.is_largeint());
        assert!(PrimitiveType::Time.is_time());
        assert!(!PrimitiveType::Int256.is_largeint());
        assert!(!PrimitiveType::Int256.is_json());
        assert!(!PrimitiveType::Int256.is_time());
    }
}
