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

use std::sync::Arc;

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorNamespaceIdentity,
    ConnectorRequestContext, ConnectorViewDefinition, ConnectorViewIdentity, ProviderBindingEpoch,
};

#[derive(Clone)]
pub struct ConnectorViewRequest {
    pub view: ConnectorViewIdentity,
    pub context: ConnectorRequestContext,
}

/// Enumerate one namespace's views. The provider refuses a listing that would
/// exceed `bound`; it never truncates one.
#[derive(Clone)]
pub struct ConnectorListViewsRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub bound: super::ConnectorListingBound,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
// Design: ADR-0218 (docs/adr/ADR-0218-sql-output-declarations-and-stored-view-analysis.md)
pub struct ConnectorViewMetadataValue {
    pub identity: ConnectorViewIdentity,
    pub definition: ConnectorViewDefinition,
    /// Immutable provider identity for the same observed definition/schema.
    pub version: Arc<str>,
    pub comment: Option<Arc<str>>,
    pub properties: Vec<(Arc<str>, Arc<str>)>,
}

impl ConnectorViewMetadataValue {
    pub fn try_new(
        identity: ConnectorViewIdentity,
        definition: ConnectorViewDefinition,
        version: Arc<str>,
        comment: Option<Arc<str>>,
        mut properties: Vec<(Arc<str>, Arc<str>)>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        definition
            .validate_output_declaration()
            .map_err(|message| ConnectorError::new(ConnectorErrorKind::CorruptData, message))?;
        if version.is_empty()
            || definition.raw_sql.is_empty()
            || definition.default_namespace.is_empty()
            || definition
                .default_catalog
                .as_ref()
                .is_some_and(|catalog| catalog.is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector view metadata contains an empty definition source or context",
            ));
        }
        properties.sort_by(|left, right| left.0.cmp(&right.0));
        if properties
            .windows(2)
            .any(|pair| pair[0].0.as_ref() == pair[1].0.as_ref())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector view metadata contains duplicate property keys",
            ));
        }
        let bytes = [
            identity.namespace.len(),
            identity.view.len(),
            version.len(),
            definition.raw_sql.len(),
            definition
                .default_catalog
                .as_ref()
                .map_or(0, |value| value.len()),
            definition.default_namespace.len(),
            definition
                .columns
                .len()
                .saturating_mul(std::mem::size_of::<super::ConnectorViewColumn>()),
            definition.columns.iter().fold(0usize, |bytes, column| {
                bytes.saturating_add(column.name.len())
            }),
            comment.as_ref().map_or(0, |value| value.len()),
            properties.iter().fold(0usize, |bytes, (key, value)| {
                bytes.saturating_add(key.len()).saturating_add(value.len())
            }),
        ]
        .into_iter()
        .fold(0usize, usize::saturating_add);
        let bytes = definition.columns.iter().try_fold(bytes, |bytes, column| {
            novarocks_type_contract::owned_resources::type_representation::function_value_type_retained_bytes(
                &column.value_type,
            ).and_then(|value| bytes.checked_add(value))
        }).ok_or_else(|| ConnectorError::new(ConnectorErrorKind::ResourceExhausted,
            "connector view type representation byte count overflow"))?;
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector view metadata exceeds request total payload budget",
            ));
        }
        Ok(Self {
            identity,
            definition,
            version,
            comment,
            properties,
        })
    }
}

/// One completed view observation, fenced to its original control generation.
/// No future, schema observer, callback, or capacity author crosses this value.
pub struct ConnectorViewPreparation {
    owner: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    facts: Box<dyn std::any::Any + Send + Sync>,
}
impl ConnectorViewPreparation {
    pub fn new<T: std::any::Any + Send + Sync>(
        owner: &ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        facts: T,
    ) -> Self {
        Self {
            owner: owner.clone(),
            incarnation,
            facts: Box::new(facts),
        }
    }
    pub fn into_facts<T: std::any::Any + Send + Sync>(
        self,
        owner: &ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
    ) -> Result<T, ConnectorError> {
        if &self.owner != owner || self.incarnation != incarnation {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "view preparation belongs to another connector control generation",
            ));
        }
        self.facts.downcast::<T>().map(|facts| *facts).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "view preparation belongs to another provider author",
            )
        })
    }
}
impl std::fmt::Debug for ConnectorViewPreparation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorViewPreparation")
            .field("owner", &self.owner)
            .field("incarnation", &self.incarnation)
            .finish_non_exhaustive()
    }
}
pub type ConnectorViewPreparationFuture<'a> =
    futures::future::BoxFuture<'a, Result<ConnectorViewPreparation, ConnectorError>>;

pub trait ConnectorViewMetadata: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;

    fn incarnation(&self) -> ProviderBindingEpoch;

    fn view_exists(&self, request: ConnectorViewRequest) -> Result<bool, ConnectorError>;

    fn load_view(
        &self,
        request: ConnectorViewRequest,
        creation: &mut super::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorViewMetadataValue,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    >;

    /// Await only the original provider I/O. Synchronous creation observers
    /// are borrowed after this completes, never across an await or sync fallback.
    fn prepare_view(&self, request: ConnectorViewRequest) -> ConnectorViewPreparationFuture<'_> {
        Box::pin(async move {
            let _ = request;
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector view metadata does not support asynchronous preparation",
            ))
        })
    }

    fn create_prepared_view(
        &self,
        preparation: ConnectorViewPreparation,
        creation: &mut super::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorViewMetadataValue,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        let _ = (preparation, creation);
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector view metadata does not support prepared schema creation",
        )
        .into())
    }

    fn list_views(
        &self,
        request: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError>;

    /// Enumerate with the same request facts without a synchronous bridge.
    /// Providers must explicitly support this path; there is no sync fallback.
    fn list_views_async(
        &self,
        request: ConnectorListViewsRequest,
        cpu: Arc<dyn super::ProviderMetadataCpuPort>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorViewIdentity>, ConnectorError>> {
        Box::pin(async move {
            let _ = (request, cpu);
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector view metadata does not support asynchronous listing",
            ))
        })
    }
}

pub(crate) fn validate_view_metadata_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    capability: &dyn ConnectorViewMetadata,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector view metadata capability owner does not match its control binding generation",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::connector::{ConnectorInstanceId, ConnectorProviderId, ConnectorViewDialect};

    struct ViewCapability {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        list_calls: std::sync::atomic::AtomicUsize,
    }

    impl ConnectorViewMetadata for ViewCapability {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn view_exists(&self, _: ConnectorViewRequest) -> Result<bool, ConnectorError> {
            Ok(false)
        }

        fn load_view(
            &self,
            _: ConnectorViewRequest,
            _: &mut crate::connector::read_stack::ConnectorReadSchemaCreation<'_>,
        ) -> Result<
            ConnectorViewMetadataValue,
            novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
        > {
            Err(ConnectorError::new(ConnectorErrorKind::NotFound, "missing view").into())
        }

        fn list_views(
            &self,
            _: ConnectorListViewsRequest,
        ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
            self.list_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Vec::new())
        }
    }

    pub(super) fn descriptor() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
        }
    }

    pub(super) fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            crate::connector::ConnectorStopOwner::new().view(),
            1024,
            1024,
        )
        .expect("valid connector request context")
    }

    pub(super) fn identity() -> ConnectorViewIdentity {
        ConnectorViewIdentity {
            instance_id: ConnectorInstanceId::parse("catalog").expect("instance ID"),
            namespace: Arc::from("db"),
            view: Arc::from("v"),
        }
    }

    pub(super) fn definition() -> ConnectorViewDefinition {
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            raw_sql: Arc::from("SELECT 1 AS c"),
            default_catalog: Some(Arc::from("catalog")),
            default_namespace: Arc::from("db"),
            columns: vec![crate::connector::ConnectorViewColumn {
                name: "c".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            }]
            .into(),
        }
    }

    #[test]
    fn spi5b_view_metadata_canonicalizes_properties() {
        let value = ConnectorViewMetadataValue::try_new(
            identity(),
            definition(),
            "same-observed-version".into(),
            Some(Arc::from("comment")),
            vec![
                (Arc::from("z"), Arc::from("2")),
                (Arc::from("a"), Arc::from("1")),
            ],
            &context(),
        )
        .expect("valid view metadata");

        assert_eq!(value.properties[0].0.as_ref(), "a");
        assert_eq!(value.properties[1].0.as_ref(), "z");
    }

    #[test]
    fn spi5b_view_metadata_rejects_duplicate_properties() {
        let error = ConnectorViewMetadataValue::try_new(
            identity(),
            definition(),
            "same-observed-version".into(),
            None,
            vec![
                (Arc::from("a"), Arc::from("1")),
                (Arc::from("a"), Arc::from("2")),
            ],
            &context(),
        )
        .expect_err("duplicate properties are corrupt provider data");

        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5b_view_metadata_keeps_an_omitted_standard_default_catalog() {
        let mut definition = definition();
        definition.default_catalog = None;
        let value = ConnectorViewMetadataValue::try_new(
            identity(),
            definition,
            "same-observed-version".into(),
            None,
            Vec::new(),
            &context(),
        )
        .expect("standard view metadata may omit its default catalog");

        assert_eq!(value.definition.default_catalog, None);
    }

    #[test]
    fn spi5b_view_metadata_rejects_a_different_generation_owner() {
        let descriptor = descriptor();
        let expected_incarnation = ProviderBindingEpoch::new();
        let capability = ViewCapability {
            descriptor: descriptor.clone(),
            incarnation: ProviderBindingEpoch::new(),
            list_calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let error = validate_view_metadata_owner(&descriptor, expected_incarnation, &capability)
            .expect_err("view capability must belong to the exact control generation");

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    struct NoListingCpu;
    impl crate::connector::ProviderMetadataCpuPort for NoListingCpu {
        fn submit_metadata_in(
            &self,
            _: &mut Option<crate::connector::ProviderMetadataCpuInput>,
        ) -> Result<(), ConnectorError> {
            panic!("the unsupported default cannot submit a CPU phase")
        }
        fn poll_finished(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            panic!("no submitted phase")
        }
        fn poll_closed(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
    }

    #[tokio::test]
    async fn asynchronous_view_listing_requires_explicit_provider_support() {
        let capability = ViewCapability {
            descriptor: descriptor(),
            incarnation: ProviderBindingEpoch::new(),
            list_calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let request = ConnectorListViewsRequest {
            namespace: ConnectorNamespaceIdentity {
                instance_id: capability.descriptor.instance_id.clone(),
                namespace: Arc::from("db"),
            },
            bound: crate::connector::ConnectorListingBound::V1,
            context: context(),
        };
        let error = capability
            .list_views_async(request, Arc::new(NoListingCpu))
            .await
            .expect_err("a synchronous capability is not an async listing implementation");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            capability
                .list_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the default must not call a synchronous provider or fabricate an empty listing"
        );
    }
}

#[cfg(test)]
mod declaration_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_type_contract::FunctionValueType;

    fn metadata(
        definition: ConnectorViewDefinition,
        version: &str,
    ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        ConnectorViewMetadataValue::try_new(
            super::tests::identity(),
            definition,
            version.into(),
            None,
            Vec::new(),
            &super::tests::context(),
        )
    }

    #[test]
    fn public_name_and_complete_type_have_one_declaration_author() {
        let value = metadata(super::tests::definition(), "version-1").unwrap();
        assert_eq!(value.version.as_ref(), "version-1");
        assert_eq!(value.definition.columns[0].name.as_ref(), "c");
        assert_eq!(
            value.definition.columns[0].value_type,
            FunctionValueType::new(DataType::Int64, true)
        );
    }

    #[test]
    fn malformed_version_or_declared_header_is_corrupt_without_a_schema_guess() {
        assert_eq!(
            metadata(super::tests::definition(), "").unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        let mut definition = super::tests::definition();
        definition.columns = Vec::new().into();
        assert_eq!(
            metadata(definition, "v").unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        let mut definition = super::tests::definition();
        let mut column = definition.columns[0].clone();
        column.name = "".into();
        definition.columns = vec![column].into();
        assert_eq!(
            metadata(definition, "v").unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        let mut definition = super::tests::definition();
        let mut second = definition.columns[0].clone();
        second.name = "C".into();
        definition.columns = vec![definition.columns[0].clone(), second].into();
        assert_eq!(
            metadata(definition, "v").unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn original_payload_budget_includes_complete_nested_type_backing() {
        let mut definition = super::tests::definition();
        let value_type = FunctionValueType::new(
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "nested_field".repeat(64),
                    DataType::Int32,
                    true,
                ))]
                .into(),
            ),
            true,
        );
        definition.columns = vec![super::super::ConnectorViewColumn {
            name: "c".into(),
            value_type,
        }]
        .into();
        assert_eq!(
            metadata(definition, "v").unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn prepared_facts_cannot_cross_a_control_generation_or_provider_author() {
        let owner = super::tests::descriptor();
        let epoch = ProviderBindingEpoch::new();
        assert_eq!(
            ConnectorViewPreparation::new(&owner, epoch, 42_u64)
                .into_facts::<u64>(&owner, epoch)
                .unwrap(),
            42
        );
        assert_eq!(
            ConnectorViewPreparation::new(&owner, epoch, 42_u64)
                .into_facts::<u64>(&owner, ProviderBindingEpoch::new())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorViewPreparation::new(&owner, epoch, 42_u64)
                .into_facts::<String>(&owner, epoch)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
