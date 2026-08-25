//! Validated native scan-range wire values.
//!
//! These wrappers retain the generated messages unchanged. File-system and
//! execution-specific interpretation remains with the Backend decoder.

use super::error::ContractError;
use crate::novarocks;

/// A validated generated `ScanRangeParams` value.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanRangeParams {
    raw: novarocks::ScanRangeParams,
}

impl ScanRangeParams {
    pub fn parse(raw: novarocks::ScanRangeParams) -> Result<Self, ContractError> {
        let range = raw
            .range
            .clone()
            .ok_or_else(|| ContractError::invalid_value("native ScanRangeParams requires range"))?;
        ScanRange::parse(range)?;
        Ok(Self { raw })
    }

    pub fn new(
        range: ScanRange,
        volume_id: Option<i32>,
        empty: Option<bool>,
        has_more: Option<bool>,
    ) -> Result<Self, ContractError> {
        Self::parse(novarocks::ScanRangeParams {
            range: Some(range.into_proto()),
            volume_id,
            empty,
            has_more,
        })
    }

    pub const fn as_proto(&self) -> &novarocks::ScanRangeParams {
        &self.raw
    }

    pub fn into_proto(self) -> novarocks::ScanRangeParams {
        self.raw
    }

    pub fn range(&self) -> ScanRange {
        ScanRange::parse(
            self.raw
                .range
                .clone()
                .expect("validated ScanRangeParams always has a range"),
        )
        .expect("validated ScanRangeParams always has a valid range")
    }

    pub const fn volume_id(&self) -> Option<i32> {
        self.raw.volume_id
    }

    pub const fn empty(&self) -> Option<bool> {
        self.raw.empty
    }

    pub const fn has_more(&self) -> Option<bool> {
        self.raw.has_more
    }
}

/// A validated generated `ScanRange` oneof.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanRange {
    raw: novarocks::ScanRange,
}

impl ScanRange {
    pub fn parse(raw: novarocks::ScanRange) -> Result<Self, ContractError> {
        match raw.kind.as_ref() {
            Some(novarocks::scan_range::Kind::File(file)) => {
                FileScanRange::parse(file.clone())?;
            }
            None => {
                return Err(ContractError::invalid_value(
                    "native ScanRange requires kind",
                ));
            }
        }
        Ok(Self { raw })
    }

    pub fn file(file: FileScanRange) -> Result<Self, ContractError> {
        Self::parse(novarocks::ScanRange {
            kind: Some(novarocks::scan_range::Kind::File(file.into_proto())),
        })
    }

    pub const fn as_proto(&self) -> &novarocks::ScanRange {
        &self.raw
    }

    pub fn into_proto(self) -> novarocks::ScanRange {
        self.raw
    }

    pub fn file_range(&self) -> FileScanRange {
        let Some(novarocks::scan_range::Kind::File(file)) = self.raw.kind.as_ref() else {
            unreachable!("validated ScanRange always has a file variant");
        };
        FileScanRange::parse(file.clone()).expect("validated ScanRange always has a valid file")
    }
}

/// A validated generated native file scan range.
#[derive(Clone, Debug, PartialEq)]
pub struct FileScanRange {
    raw: novarocks::FileScanRange,
}

impl FileScanRange {
    /// Validates only wire-owned shape. Backend owns the execution-specific
    /// file-format and pruning interpretation.
    pub fn parse(raw: novarocks::FileScanRange) -> Result<Self, ContractError> {
        if raw.file_format.trim().is_empty() {
            return Err(ContractError::invalid_value(
                "native FileScanRange requires file_format",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::FileScanRange {
        &self.raw
    }

    pub fn into_proto(self) -> novarocks::FileScanRange {
        self.raw
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::{FileScanRange, ScanRange, ScanRangeParams};
    use crate::novarocks;

    #[test]
    fn preserves_fixed_file_scan_range_wire_bytes() {
        let range = ScanRangeParams::new(
            ScanRange::file(
                FileScanRange::parse(novarocks::FileScanRange {
                    file_format: "PARQUET".into(),
                    full_path: Some("s3://bucket/data.parquet".into()),
                    offset: 8,
                    length: 16,
                    file_length: 128,
                    ..Default::default()
                })
                .expect("valid file range"),
            )
            .expect("valid scan range"),
            Some(7),
            Some(false),
            Some(false),
        )
        .expect("valid range params");

        assert_eq!(
            range.as_proto().encode_to_vec(),
            [
                10, 44, 10, 42, 10, 7, 80, 65, 82, 81, 85, 69, 84, 18, 24, 115, 51, 58, 47, 47, 98,
                117, 99, 107, 101, 116, 47, 100, 97, 116, 97, 46, 112, 97, 114, 113, 117, 101, 116,
                40, 8, 48, 16, 56, 128, 1, 16, 7, 24, 0, 32, 0,
            ]
        );
    }

    #[test]
    fn rejects_missing_range_and_file_format() {
        assert!(ScanRangeParams::parse(novarocks::ScanRangeParams::default()).is_err());
        assert!(ScanRange::parse(novarocks::ScanRange::default()).is_err());
        assert!(FileScanRange::parse(novarocks::FileScanRange::default()).is_err());
    }
}
