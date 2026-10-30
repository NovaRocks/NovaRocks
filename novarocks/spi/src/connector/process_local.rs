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

// Design: ADR-0177 (docs/adr/ADR-0177-attempt-local-maintenance-inputs-and-single-rewrite-plan.md)
use std::{any::Any, fmt, sync::Arc};

use super::{ConnectorError, ConnectorErrorKind, ConnectorProviderBindingKey};

/// Provider-owned immutable content carried only by values in one process.
/// Cloning shares ownership; neither Debug nor an identity comparison exposes content.
#[derive(Clone)]
pub struct ConnectorProcessLocalPayload {
    owner: ConnectorProviderBindingKey,
    value: Arc<dyn Any + Send + Sync>,
}

impl ConnectorProcessLocalPayload {
    pub fn new<T: Send + Sync + 'static>(
        owner: ConnectorProviderBindingKey,
        value: Arc<T>,
    ) -> Self {
        Self {
            owner,
            value: Arc::new(value),
        }
    }

    /// Observe the actual erased wrapper allocation through the caller's
    /// existing schema creator. The supplied Arc keeps its original lifetime;
    /// this constructor creates no grant or independent resource authority.
    pub fn try_new_with_creation<T: Send + Sync + 'static>(
        owner: ConnectorProviderBindingKey,
        value: Arc<T>,
        creation: &mut super::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<Self, novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
        Ok(Self {
            owner,
            value: creation.arc(value)?,
        })
    }

    /// Validate the complete provider generation before opening the content.
    /// Providers use their retained binding key here: `value` checks the
    /// instance and private type, but cannot establish the incarnation fence.
    pub fn validate_owner(
        &self,
        expected: &ConnectorProviderBindingKey,
    ) -> Result<(), ConnectorError> {
        if &self.owner != expected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "process-local payload belongs to another connector generation",
            ));
        }
        Ok(())
    }

    pub fn value<T: Send + Sync + 'static>(
        &self,
        expected_instance_id: &str,
    ) -> Result<&Arc<T>, ConnectorError> {
        if self.owner.instance_id() != expected_instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "process-local payload belongs to another connector instance",
            ));
        }
        self.value.downcast_ref::<Arc<T>>().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "process-local payload has an unexpected provider type",
            )
        })
    }
}

impl fmt::Debug for ConnectorProcessLocalPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectorProcessLocalPayload")
            .field("owner", &self.owner)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::ConnectorProviderBinding;

    fn owner() -> ConnectorProviderBindingKey {
        ConnectorProviderBinding::iceberg("catalog", [1; 16], "binding")
            .unwrap()
            .binding_key()
            .clone()
    }

    #[test]
    fn process_local_payload_checks_owner_before_type() {
        let value = Arc::new(String::from("private content"));
        let payload = ConnectorProcessLocalPayload::new(owner(), value.clone());
        assert!(Arc::ptr_eq(
            payload.value::<String>("catalog").unwrap(),
            &value
        ));
        assert_eq!(
            payload.value::<String>("other").unwrap_err().kind(),
            ConnectorErrorKind::Internal
        );
        assert_eq!(
            payload.value::<u64>("catalog").unwrap_err().kind(),
            ConnectorErrorKind::Internal
        );
        let debug = format!("{payload:?}");
        assert!(debug.contains("catalog"));
        assert!(!debug.contains("private content"));
    }

    #[test]
    fn process_local_clones_release_the_shared_content() {
        let value = Arc::new(vec![1u8, 2, 3]);
        let weak = Arc::downgrade(&value);
        let payload = ConnectorProcessLocalPayload::new(owner(), value);
        let clone = payload.clone();
        assert!(Arc::ptr_eq(
            payload.value::<Vec<u8>>("catalog").unwrap(),
            clone.value::<Vec<u8>>("catalog").unwrap()
        ));
        drop(payload);
        assert!(weak.upgrade().is_some());
        drop(clone);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn observed_process_local_creation_retains_original_and_preserves_refusal() {
        use novarocks_connector_contract::PureProviderCompileError;
        use novarocks_type_contract::{CompileControlError, ValueTypeStorageLifetime};
        let original = Arc::new(String::from("original private source"));
        let weak = Arc::downgrade(&original);
        let mut refuse = |_| Err(CompileControlError::ResourceExhausted);
        let mut work = |_| Ok(());
        let mut creation = super::super::read_stack::ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut refuse,
            &mut work,
        );
        let error = ConnectorProcessLocalPayload::try_new_with_creation(
            owner(),
            original.clone(),
            &mut creation,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            PureProviderCompileError::Control(CompileControlError::ResourceExhausted)
        ));
        assert_eq!(Arc::strong_count(&original), 1);
        let mut accept = |_| Ok(());
        let mut creation = super::super::read_stack::ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut accept,
            &mut work,
        );
        let payload =
            ConnectorProcessLocalPayload::try_new_with_creation(owner(), original, &mut creation)
                .unwrap();
        assert_eq!(
            payload.value::<String>("catalog").unwrap().as_str(),
            "original private source"
        );
        let alias = payload.clone();
        drop(payload);
        assert!(weak.upgrade().is_some());
        drop(alias);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn process_local_payload_rejects_same_instance_from_another_incarnation() {
        let current = owner();
        let mut previous = current.clone();
        previous.incarnation = super::super::ProviderBindingEpoch::from_bytes([2; 16]);
        let payload = ConnectorProcessLocalPayload::new(previous.clone(), Arc::new(17u64));
        // A matching private type and instance do not establish generation ownership.
        assert_eq!(**payload.value::<u64>(current.instance_id()).unwrap(), 17);
        payload.validate_owner(&previous).unwrap();
        let error = payload.validate_owner(&current).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(
            error.message(),
            "process-local payload belongs to another connector generation"
        );
    }
}
