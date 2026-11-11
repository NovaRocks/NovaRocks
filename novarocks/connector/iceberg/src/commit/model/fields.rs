// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Data age, physical file age and inherited row lineage are independent facts.

use crate::iceberg::Result;
use crate::iceberg::spec::{DataFile, Struct};
use crate::manifest::binding::BoundEntry;

use super::{EntryIdentity, invalid};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeqField {
    Inherit,
    Explicit(i64),
}

impl SeqField {
    pub fn explicit(sequence: i64) -> Result<Self> {
        if sequence < 0 {
            return Err(invalid("Explicit sequence number must be nonnegative"));
        }
        Ok(Self::Explicit(sequence))
    }
    /// The format writer's -1 sentinel means inheritance, never a predicted sequence.
    pub fn manifest_writer_value(self) -> i64 {
        match self {
            Self::Inherit => -1,
            Self::Explicit(value) => value,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AddedContent {
    file: DataFile,
    partition_spec_id: i32,
    data_sequence: SeqField,
}

impl AddedContent {
    pub fn new_logical_data(file: DataFile, partition_spec_id: i32) -> Result<Self> {
        EntryIdentity::try_from(&file)?;
        validate_partition_spec_id(partition_spec_id)?;
        Ok(Self {
            file,
            partition_spec_id,
            data_sequence: SeqField::Inherit,
        })
    }
    pub fn rewritten_data(
        file: DataFile,
        partition_spec_id: i32,
        source_sequence: i64,
    ) -> Result<Self> {
        EntryIdentity::try_from(&file)?;
        validate_partition_spec_id(partition_spec_id)?;
        Ok(Self {
            file,
            partition_spec_id,
            data_sequence: SeqField::explicit(source_sequence)?,
        })
    }
    pub fn file(&self) -> &DataFile {
        &self.file
    }
    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }
    pub const fn data_sequence(&self) -> SeqField {
        self.data_sequence
    }
    /// Every newly added physical file inherits the sequence of its actual publication.
    #[cfg(test)]
    pub const fn file_sequence(&self) -> SeqField {
        SeqField::Inherit
    }
}

fn validate_partition_spec_id(id: i32) -> Result<()> {
    if id < 0 {
        return Err(invalid(
            "Added content requires an actual nonnegative partition spec ID",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub struct EntryFacts {
    pub added_snapshot_id: Option<i64>,
    pub data_sequence: Option<i64>,
    pub file_sequence: Option<i64>,
    pub partition_spec_id: i32,
    pub partition: Struct,
    pub record_count: u64,
    /// None also represents historical data that has never received a row ID.
    pub first_row_id: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenEntry {
    identity: EntryIdentity,
    facts: EntryFacts,
}

impl FrozenEntry {
    pub fn new(identity: EntryIdentity, facts: EntryFacts) -> Result<Self> {
        identity.validate()?;
        if facts.data_sequence.is_some_and(|value| value < 0)
            || facts.file_sequence.is_some_and(|value| value < 0)
            || facts.first_row_id.is_some_and(|value| value < 0)
        {
            return Err(invalid(
                "Frozen entry facts must contain actual nonnegative values or inheritance",
            ));
        }
        Ok(Self { identity, facts })
    }
    /// Freeze the resolved facts, including an authoritative absent row ID.
    pub(crate) fn from_bound_entry(entry: &BoundEntry<'_>) -> Result<Self> {
        let file = entry.raw.data_file();
        Self::new(
            EntryIdentity::try_from(file)?,
            EntryFacts {
                added_snapshot_id: entry.sequences.snapshot_id,
                data_sequence: entry.sequences.data_sequence,
                file_sequence: entry.sequences.file_sequence,
                partition_spec_id: entry.source.descriptor.partition_spec_id,
                partition: file.partition().clone(),
                record_count: file.record_count(),
                first_row_id: entry.first_row_id,
            },
        )
    }

    pub fn identity(&self) -> &EntryIdentity {
        &self.identity
    }
    pub fn facts(&self) -> &EntryFacts {
        &self.facts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{DataContentType, DataFileFormat};

    fn file(first_row_id: Option<i64>) -> DataFile {
        crate::iceberg::spec::DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("s3://bucket/data.parquet".into())
            .file_format(DataFileFormat::Parquet)
            .record_count(3)
            .file_size_in_bytes(100)
            .first_row_id(first_row_id)
            .build()
            .unwrap()
    }

    #[test]
    fn new_physical_files_inherit_file_age_and_new_logical_data_inherits_data_age() {
        let added = AddedContent::new_logical_data(file(None), 0).unwrap();
        assert_eq!(added.data_sequence(), SeqField::Inherit);
        assert_eq!(added.file_sequence(), SeqField::Inherit);
        assert_eq!(added.data_sequence().manifest_writer_value(), -1);
        let rewritten = AddedContent::rewritten_data(file(Some(30)), 0, 9).unwrap();
        assert_eq!(rewritten.data_sequence(), SeqField::Explicit(9));
        assert_eq!(rewritten.file_sequence(), SeqField::Inherit);
        assert!(AddedContent::rewritten_data(file(None), 0, -1).is_err());
        assert!(AddedContent::new_logical_data(file(None), -1).is_err());
        let historical = AddedContent::new_logical_data(file(None), 3).unwrap();
        assert_eq!(historical.partition_spec_id(), 3);
    }
}
