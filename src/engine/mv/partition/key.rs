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

/// Optional partition predicate that the aggregate target state loader and
/// the iceberg MV target locator share. `None` means "do not prune"; an
/// `AllowList` means "drop FileScanTasks whose target partition key is not in
/// this set". The empty allow-list is a legitimate state (no partition is
/// affected); callers MUST NOT silently treat it as "no filter".
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TargetPartitionFilter {
    None,
    AllowList(BTreeSet<MvPartitionKey>),
}

impl TargetPartitionFilter {
    pub(crate) fn matches(&self, key: &MvPartitionKey) -> bool {
        match self {
            Self::None => true,
            Self::AllowList(set) => set.contains(key),
        }
    }

    pub(crate) fn allow_list_len(&self) -> Option<usize> {
        match self {
            Self::None => None,
            Self::AllowList(set) => Some(set.len()),
        }
    }

    pub(crate) fn is_allow_list(&self) -> bool {
        matches!(self, Self::AllowList(_))
    }
}

impl std::fmt::Display for TargetPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::AllowList(set) => write!(f, "AllowList({} keys)", set.len()),
        }
    }
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

    #[test]
    fn target_partition_filter_none_passes_any_key() {
        let filter = TargetPartitionFilter::None;
        assert!(filter.matches(&key(1, "id", "1")));
        assert_eq!(filter.allow_list_len(), None);
    }

    #[test]
    fn target_partition_filter_allow_list_matches_only_listed_keys() {
        let filter = TargetPartitionFilter::AllowList(
            [key(1, "id", "1"), key(1, "id", "2")].into_iter().collect(),
        );
        assert!(filter.matches(&key(1, "id", "1")));
        assert!(filter.matches(&key(1, "id", "2")));
        assert!(!filter.matches(&key(1, "id", "3")));
        assert!(!filter.matches(&key(2, "id", "1")));
        assert_eq!(filter.allow_list_len(), Some(2));
    }

    #[test]
    fn target_partition_filter_empty_allow_list_matches_nothing() {
        let filter = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        assert!(!filter.matches(&key(1, "id", "1")));
        assert_eq!(filter.allow_list_len(), Some(0));
    }

    #[test]
    fn target_partition_filter_display_summarizes_allow_list_size() {
        let none = TargetPartitionFilter::None;
        assert_eq!(format!("{none}"), "None");

        let empty = TargetPartitionFilter::AllowList(std::collections::BTreeSet::new());
        assert_eq!(format!("{empty}"), "AllowList(0 keys)");

        let two = TargetPartitionFilter::AllowList(
            [key(1, "id", "1"), key(1, "id", "2")].into_iter().collect(),
        );
        assert_eq!(format!("{two}"), "AllowList(2 keys)");
    }
}
