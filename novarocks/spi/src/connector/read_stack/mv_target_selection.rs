// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact, provider-admitted target partition selection for an MV read.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;

use crate::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorTableObjectId,
    MAX_MV_OBSERVATION_PARTITION_FIELDS, MvExactPartitionField,
};

pub const MAX_MV_TARGET_PARTITION_KEYS: usize = 4096;
pub const MAX_MV_TARGET_PARTITION_VALUE_BYTES: usize = 4096;

/// A value from one ordered target partition key. Its spelling is an input to
/// the provider, which alone decides whether a file value can be compared.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorMvPartitionValue {
    Null,
    String(Arc<str>),
}

/// A Known target partition selection against one frozen target and snapshot.
///
/// The object, spec, and field identities are opaque to the application. An
/// empty key list is a proven empty selection, not an unrestricted scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvTargetPartitionSelection {
    object_id: ConnectorTableObjectId,
    partition_spec_version: Bytes,
    partition_fields: Vec<MvExactPartitionField>,
    keys: Vec<Vec<ConnectorMvPartitionValue>>,
    snapshot_id: i64,
}

impl ConnectorMvTargetPartitionSelection {
    pub fn try_new(
        object_id: ConnectorTableObjectId,
        partition_spec_version: Bytes,
        partition_fields: Vec<MvExactPartitionField>,
        keys: Vec<Vec<ConnectorMvPartitionValue>>,
        snapshot_id: i64,
    ) -> Result<Self, ConnectorError> {
        if keys.len() > MAX_MV_TARGET_PARTITION_KEYS {
            return Err(invalid("MV target selection exceeds the key limit"));
        }
        if keys.iter().flatten().any(|value| {
            matches!(value, ConnectorMvPartitionValue::String(value) if value.len() > MAX_MV_TARGET_PARTITION_VALUE_BYTES)
        }) {
            return Err(invalid("MV target selection value exceeds the byte limit"));
        }
        Self::for_exact_replacement(
            object_id,
            partition_spec_version,
            partition_fields,
            keys,
            snapshot_id,
        )
    }

    /// An exact write predicate retains every admitted affected target key.
    /// The bounded target read optimization uses its separately checked constructor.
    pub fn for_exact_replacement(
        object_id: ConnectorTableObjectId,
        partition_spec_version: Bytes,
        partition_fields: Vec<MvExactPartitionField>,
        keys: Vec<Vec<ConnectorMvPartitionValue>>,
        snapshot_id: i64,
    ) -> Result<Self, ConnectorError> {
        if partition_spec_version.is_empty() || partition_spec_version.len() > 1024 {
            return Err(invalid(
                "MV target selection requires a bounded exact partition spec",
            ));
        }
        if partition_fields.is_empty()
            || partition_fields.len() > MAX_MV_OBSERVATION_PARTITION_FIELDS
        {
            return Err(invalid(
                "MV target selection requires bounded partition fields",
            ));
        }
        let mut ids = HashSet::with_capacity(partition_fields.len());
        for field in &partition_fields {
            if !ids.insert(field.partition_field_id().clone()) {
                return Err(invalid(
                    "MV target selection repeats a partition field identity",
                ));
            }
        }
        let mut unique_keys = HashSet::with_capacity(keys.len());
        for key in &keys {
            if key.len() != partition_fields.len() {
                return Err(invalid(
                    "MV target selection key width differs from its fields",
                ));
            }
            if !unique_keys.insert(key) {
                return Err(invalid("MV target selection repeats a partition key"));
            }
        }
        Ok(Self {
            object_id,
            partition_spec_version,
            partition_fields,
            keys,
            snapshot_id,
        })
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub const fn partition_spec_version(&self) -> &Bytes {
        &self.partition_spec_version
    }

    pub fn partition_fields(&self) -> &[MvExactPartitionField] {
        &self.partition_fields
    }

    pub fn keys(&self) -> &[Vec<ConnectorMvPartitionValue>] {
        &self.keys
    }

    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}

fn invalid(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::MvExactPartitionTransform;

    #[test]
    fn partition_replacement_keeps_keys_beyond_the_read_optimization_bound() {
        let object = ConnectorTableObjectId::try_new(Bytes::from_static(b"target")).unwrap();
        let spec = Bytes::from_static(b"spec");
        let fields = vec![
            MvExactPartitionField::try_new(
                Bytes::from_static(b"partition-field"),
                Bytes::from_static(b"source-field"),
                MvExactPartitionTransform::Identity,
            )
            .unwrap(),
        ];
        let keys = (0..=MAX_MV_TARGET_PARTITION_KEYS)
            .map(|value| vec![ConnectorMvPartitionValue::String(value.to_string().into())])
            .collect::<Vec<_>>();
        assert!(
            ConnectorMvTargetPartitionSelection::try_new(
                object.clone(),
                spec.clone(),
                fields.clone(),
                keys.clone(),
                41
            )
            .is_err()
        );
        let exact = ConnectorMvTargetPartitionSelection::for_exact_replacement(
            object,
            spec,
            fields,
            keys.clone(),
            41,
        )
        .unwrap();
        assert_eq!(exact.keys(), keys);
    }
}
