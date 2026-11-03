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

use serde::Deserialize;

pub(crate) type Error = serde_json::Error;

pub(crate) fn from_slice<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> Result<T, Error> {
    crate::rest_protocol::json::validate_document(bytes)
        .map_err(<Error as serde::de::Error>::custom)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    // The original iterative validator has already enforced the finite
    // provider container bound, including ignored fields and quoted text.
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(value)
}

pub(crate) fn from_str<'de, T: Deserialize<'de>>(text: &'de str) -> Result<T, Error> {
    from_slice(text.as_bytes())
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
        let decoded: TableMetadata = from_slice(&encoded).unwrap();
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
        let decoded: TableMetadata = from_str(&historical_json).unwrap();
        assert!(decoded.current_schema().field_by_name("deep").is_none());
        assert!(
            decoded
                .schemas_iter()
                .any(|schema| schema.field_by_name("deep").is_some())
        );
        assert_eq!(decoded, historical);
    }

    #[test]
    fn malformed_or_overdeep_documents_never_reach_a_permissive_retry() {
        let overdeep = format!(
            "{}0{}",
            "[".repeat(crate::rest_protocol::json::MAX_DEPTH + 1),
            "]".repeat(crate::rest_protocol::json::MAX_DEPTH + 1)
        );
        assert!(from_str::<serde_json::Value>(&overdeep).is_err());
        assert!(from_str::<serde_json::Value>("{} {}").is_err());
        assert!(from_str::<serde_json::Value>("{\"x\":[}").is_err());
        assert!(from_str::<Schema>(r#"{"type":"struct","fields":"wrong"}"#).is_err());
    }
}
