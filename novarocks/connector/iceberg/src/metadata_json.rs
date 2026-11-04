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

//! Bounded decoding of complete Iceberg metadata and schema documents.
//!
//! A logical field nests several JSON containers. The default Serde limit of
//! 128 containers cannot represent the admitted logical depth of 64. Validate
//! the complete input with the provider's iterative, finitely bounded scanner
//! before decoding the original SDK types. This does not change logical type
//! admission, discard history, or retry a failed parse with another policy.

// Design: ADR-0214 (docs/adr/ADR-0214-iceberg-schema-depth-by-decoder-owner.md)
use crate::rest_protocol::json::{ScanError, validate_document_with_depth};
use serde::Deserialize;

/// The physical envelope of the document actually handed to Serde. A further
/// STRUCT level contributes type object + fields array + field object; LIST
/// and MAP (including their permitted defaults) contribute no longer path.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Document {
    /// schema object / fields array / top-level field object.
    Schema,
    /// metadata object / schemas array / schema / fields / field.
    TableMetadata,
    /// response / metadata / schemas / schema / fields / field. Commit and
    /// stage-create responses use the same LoadTableResult wire shape.
    TableResponse,
}

impl Document {
    pub(crate) const fn max_depth(self) -> usize {
        let envelope = match self {
            Self::Schema => 3,
            Self::TableMetadata => 5,
            Self::TableResponse => 6,
        };
        envelope + 3 * (novarocks_type_contract::MAX_VALUE_TYPE_DEPTH - 1)
    }
}

#[derive(Debug)]
pub(crate) enum Error {
    UnsupportedDepth { limit: usize },
    Scan(ScanError),
    Json(serde_json::Error),
}

impl Error {
    pub(crate) fn is_unsupported(&self) -> bool {
        matches!(self, Self::UnsupportedDepth { .. })
    }

    pub(crate) fn connector_kind(
        &self,
        invalid: novarocks_spi::connector::ConnectorErrorKind,
    ) -> novarocks_spi::connector::ConnectorErrorKind {
        if self.is_unsupported() {
            novarocks_spi::connector::ConnectorErrorKind::Unsupported
        } else {
            invalid
        }
    }

    pub(crate) fn iceberg_kind(&self) -> crate::iceberg::ErrorKind {
        if self.is_unsupported() {
            crate::iceberg::ErrorKind::FeatureUnsupported
        } else {
            crate::iceberg::ErrorKind::DataInvalid
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedDepth { limit } => write!(
                f,
                "Iceberg document exceeds {limit} JSON container levels derived from logical type depth {}; deeper schemas are unsupported",
                novarocks_type_contract::MAX_VALUE_TYPE_DEPTH
            ),
            Self::Scan(error) => error.fmt(f),
            Self::Json(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::UnsupportedDepth { .. } => None,
            Self::Scan(error) => Some(error),
            Self::Json(error) => Some(error),
        }
    }
}

pub(crate) fn from_slice_document<'de, T: Deserialize<'de>>(
    bytes: &'de [u8],
    document: Document,
) -> Result<T, Error> {
    let limit = document.max_depth();
    validate_document_with_depth(bytes, limit).map_err(|error| match error {
        ScanError::NestingTooDeep { .. } => Error::UnsupportedDepth { limit },
        other => Error::Scan(other),
    })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    // The iterative scanner authorizes precisely this document's finite
    // container geometry before the original SDK decoder runs once.
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder).map_err(Error::Json)?;
    decoder.end().map_err(Error::Json)?;
    Ok(value)
}

/// Provider-private schema re-parsing, including field/default projections.
/// Outer table documents must select their own envelope explicitly.
pub(crate) fn from_slice<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    from_slice_document(bytes, Document::Schema)
}

pub(crate) fn from_str<'de, T: Deserialize<'de>>(text: &'de str) -> Result<T, Error> {
    from_slice(text.as_bytes())
}

pub(crate) fn from_metadata_str<'de, T: Deserialize<'de>>(text: &'de str) -> Result<T, Error> {
    from_slice_document(text.as_bytes(), Document::TableMetadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, StructType,
        TableMetadata, TableMetadataBuilder, Type,
    };
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn complete_metadata_preserves_depth64_in_current_and_retained_history() {
        let mut ty = Type::Primitive(PrimitiveType::Int);
        for id in (2..=64).rev() {
            ty = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                id,
                format!("n{id}"),
                ty,
            ))]));
        }
        let deep = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(NestedField::optional(1, "deep", ty))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            deep,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///metadata-json-depth64".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let encoded = serde_json::to_vec(&metadata).unwrap();
        // Establish that the regression really crosses Serde's physical
        // container limit, independently of the provider decoder.
        assert!(serde_json::from_slice::<TableMetadata>(&encoded).is_err());
        let decoded: TableMetadata =
            from_slice_document(&encoded, Document::TableMetadata).unwrap();
        assert_eq!(decoded.current_schema(), metadata.current_schema());
        let shallow = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::optional(
                65,
                "id",
                PrimitiveType::Int.into(),
            ))])
            .build()
            .unwrap();
        let historical = metadata
            .into_builder(None)
            .add_current_schema(shallow)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let historical_json = serde_json::to_string(&historical).unwrap();
        let decoded: TableMetadata = from_metadata_str(&historical_json).unwrap();
        assert!(decoded.current_schema().field_by_name("deep").is_none());
        assert!(
            decoded
                .schemas_iter()
                .any(|schema| schema.field_by_name("deep").is_some())
        );
        assert_eq!(decoded, historical);
    }

    #[test]
    fn each_document_envelope_accepts_its_derived_limit_and_refuses_one_more() {
        for document in [
            Document::Schema,
            Document::TableMetadata,
            Document::TableResponse,
        ] {
            let limit = document.max_depth();
            let nested = |depth| format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            from_slice_document::<serde::de::IgnoredAny>(nested(limit).as_bytes(), document)
                .unwrap();
            let error = from_slice_document::<serde::de::IgnoredAny>(
                nested(limit + 1).as_bytes(),
                document,
            )
            .unwrap_err();
            assert!(error.is_unsupported());
            assert_eq!(
                error.connector_kind(novarocks_spi::connector::ConnectorErrorKind::CorruptData),
                novarocks_spi::connector::ConnectorErrorKind::Unsupported
            );
            assert_eq!(
                error.iceberg_kind(),
                crate::iceberg::ErrorKind::FeatureUnsupported
            );
        }
    }

    // Runs on the ordinary Tokio worker stack, without a test-only stack-size
    // override. Decode, SDK traversal, Debug and destruction all run there.
    // The Iceberg owner maintains this correctness contract when SDK/Serde
    // recursion changes; release probes use this same input and entrypoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn deepest_sdk_documents_decode_traverse_debug_and_drop_on_runtime_worker() {
        tokio::spawn(async { exercise_deepest_sdk_documents() })
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn deepest_sdk_documents_decode_traverse_debug_and_drop_on_catalog_bridge() {
        let bridge =
            crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current());
        bridge
            .block_on(async { exercise_deepest_sdk_documents() })
            .unwrap();
    }

    fn exercise_deepest_sdk_documents() {
        let mut ty = Type::Primitive(PrimitiveType::Int);
        for id in (2..=64).rev() {
            ty = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
                id,
                format!("n{id}"),
                ty,
            ))]));
        }
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "deep", ty))])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema.clone(),
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "file:///maximum-json-documents".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let schema_bytes = serde_json::to_vec(&schema).unwrap();
        let metadata_bytes = serde_json::to_vec(&metadata).unwrap();
        let response = crate::rest_protocol::wire::LoadTableResult {
            metadata_location: Some("file:///maximum-json-documents/v1.metadata.json".to_owned()),
            metadata,
            config: HashMap::new(),
            storage_credentials: None,
        };
        let response_bytes = serde_json::to_vec(&response).unwrap();
        for (bytes, document) in [
            (&schema_bytes, Document::Schema),
            (&metadata_bytes, Document::TableMetadata),
            (&response_bytes, Document::TableResponse),
        ] {
            validate_document_with_depth(bytes, document.max_depth()).unwrap();
            assert!(matches!(
                validate_document_with_depth(bytes, document.max_depth() - 1),
                Err(ScanError::NestingTooDeep { .. })
            ));
        }
        let decoded_schema: Schema = from_slice(&schema_bytes).unwrap();
        let decoded_metadata: TableMetadata =
            from_slice_document(&metadata_bytes, Document::TableMetadata).unwrap();
        let decoded_response: crate::rest_protocol::wire::LoadTableResult =
            from_slice_document(&response_bytes, Document::TableResponse).unwrap();
        assert_eq!(decoded_schema, schema);
        assert_eq!(decoded_metadata.current_schema().as_ref(), &schema);
        assert_eq!(decoded_response, response);
        assert!(
            !format!("{decoded_schema:?} {decoded_metadata:?} {decoded_response:?}").is_empty()
        );
        drop((
            decoded_schema,
            decoded_metadata,
            decoded_response,
            response,
            schema,
        ));
    }

    #[test]
    fn malformed_or_overdeep_documents_never_reach_a_permissive_retry() {
        let overdeep = format!(
            "{}0{}",
            "[".repeat(Document::Schema.max_depth() + 1),
            "]".repeat(Document::Schema.max_depth() + 1)
        );
        assert!(from_str::<serde_json::Value>(&overdeep).is_err());
        assert!(from_str::<serde_json::Value>("{} {}").is_err());
        assert!(from_str::<serde_json::Value>("{\"x\":[}").is_err());
        assert!(from_str::<Schema>(r#"{"type":"struct","fields":"wrong"}"#).is_err());
    }
}
