use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKey {
    pub spec_id: i32,
    pub fields: Vec<MvPartitionKeyField>,
}

impl MvPartitionKey {
    pub(crate) fn new(spec_id: i32, fields: Vec<MvPartitionKeyField>) -> Self {
        Self { spec_id, fields }
    }

    pub(crate) fn canonical_string(&self) -> String {
        let mut out = format!("spec={}", self.spec_id);
        for field in &self.fields {
            out.push(';');
            out.push_str(&encode_component(&field.field_name));
            out.push('=');
            match &field.value {
                MvPartitionValue::Null => out.push_str("null"),
                MvPartitionValue::String(value) => {
                    out.push_str("s:");
                    out.push_str(&encode_component(value));
                }
            }
        }
        out
    }
}

fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
        }
    }
    out
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
    fn target_partition_filter_none_passes_any_key() {
        let filter = TargetPartitionFilter::None;
        assert!(filter.matches(&key(1, "id", "1")));
        assert_eq!(filter.allow_list_len(), None);
    }

    #[test]
    fn mv_partition_key_canonical_string_is_stable_and_escaped() {
        let key = MvPartitionKey::new(
            7,
            vec![
                MvPartitionKeyField::new(
                    "region/name".to_string(),
                    MvPartitionValue::String("east;1".to_string()),
                ),
                MvPartitionKeyField::new("bucket".to_string(), MvPartitionValue::Null),
            ],
        );
        assert_eq!(
            key.canonical_string(),
            "spec=7;region%2Fname=s:east%3B1;bucket=null"
        );
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
