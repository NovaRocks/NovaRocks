use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedMvPartitions {
    Unpartitioned,
    Known {
        new_partitions: BTreeSet<MvPartitionKey>,
        old_partitions: BTreeSet<MvPartitionKey>,
    },
    Unknown {
        reason: String,
    },
}

impl AffectedMvPartitions {
    pub(crate) fn known<N, O>(new_partitions: N, old_partitions: O) -> Self
    where
        N: IntoIterator<Item = MvPartitionKey>,
        O: IntoIterator<Item = MvPartitionKey>,
    {
        Self::Known {
            new_partitions: new_partitions.into_iter().collect(),
            old_partitions: old_partitions.into_iter().collect(),
        }
    }

    pub(crate) fn unknown(reason: impl Into<String>) -> Self {
        Self::Unknown {
            reason: reason.into(),
        }
    }

    pub(crate) fn unknown_reason(&self) -> Option<&str> {
        match self {
            Self::Unknown { reason } => Some(reason.as_str()),
            Self::Unpartitioned | Self::Known { .. } => None,
        }
    }

    pub(crate) fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }

    pub(crate) fn partition_count(&self) -> usize {
        match self {
            Self::Unpartitioned | Self::Unknown { .. } => 0,
            Self::Known {
                new_partitions,
                old_partitions,
            } => new_partitions.len() + old_partitions.len(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKey {
    pub spec_id: i32,
    pub fields: Vec<MvPartitionKeyField>,
}

impl MvPartitionKey {
    pub(crate) fn new(spec_id: i32, fields: Vec<MvPartitionKeyField>) -> Self {
        Self { spec_id, fields }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKeyField {
    pub field_name: String,
    pub value: MvPartitionValue,
}

impl MvPartitionKeyField {
    pub(crate) fn new(field_name: String, value: MvPartitionValue) -> Self {
        Self { field_name, value }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MvPartitionValue {
    Null,
    String(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(spec_id: i32, field: &str, value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            spec_id,
            vec![MvPartitionKeyField::new(
                field.to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn known_partitions_are_sorted_and_deduped() {
        let result = AffectedMvPartitions::known(
            [key(2, "id", "2"), key(1, "id", "1"), key(1, "id", "1")],
            [key(3, "id", "3")],
        );

        let AffectedMvPartitions::Known {
            new_partitions,
            old_partitions,
        } = result
        else {
            panic!("expected known affected partitions");
        };
        assert_eq!(
            new_partitions.into_iter().collect::<Vec<_>>(),
            vec![key(1, "id", "1"), key(2, "id", "2")]
        );
        assert_eq!(
            old_partitions.into_iter().collect::<Vec<_>>(),
            vec![key(3, "id", "3")]
        );
    }

    #[test]
    fn unknown_preserves_reason() {
        assert_eq!(
            AffectedMvPartitions::unknown("row evaluation fallback is not implemented")
                .unknown_reason(),
            Some("row evaluation fallback is not implemented")
        );
    }

    #[test]
    fn unpartitioned_is_not_unknown() {
        assert!(!AffectedMvPartitions::Unpartitioned.is_unknown());
        assert_eq!(AffectedMvPartitions::Unpartitioned.partition_count(), 0);
    }
}
