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

//! Each codec projects its own errors. Composite receivers delegate the
//! original facts; Native must not inspect nested codec variants.

use crate::{
    ipc_flat_stream_v2::{FlatPoolResourceError, FlatReaderError},
    physical_binding_v2::BindingCodecError,
    physical_type_v2::TypeCodecError,
    value_type_table::LogicalTypeCodecError,
};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::num::NonZeroU16;

#[derive(Clone, Copy)]
#[repr(u16)]
enum CodecReason {
    TypeShape = 1,
    TypeSourceModel = 2,
    LogicalShape = 3,
    LogicalSourceModel = 4,
    BindingShape = 5,
    FlatReaderMemory = 6,
    FlatReaderData = 7,
    FlatReaderUnexpected = 8,
    FlatReaderContract = 9,
    SchemaSourceModel = 11,
    SchemaShape = 10,
    PublicSchemaShape = 12,
    PublicSchemaModel = 13,
    ConstantShape = 14,
    ValueShape = 15,
    RelationShape = 16,
    ExpressionShape = 17,
    NodeShape = 18,
    ControlShape = 19,
    SemanticsShape = 20,
    SemanticsModel = 21,
    ProviderBindingShape = 22,
    ProviderReadShape = 23,
    ConnectorPayloadShape = 24,
    WriterShape = 25,
    WriterModel = 26,
    ReadScanShape = 27,
    ReadScanModel = 28,
    ValueOriginShape = 29,
    PropertyShape = 30,
    CallRequestsShape = 31,
    PackageMetadataShape = 32,
    PackageWireSourceModel = 33,
    PackageInvalid = 38,
    PackageProtobuf = 34,
    ResourceMalformed = 35,
    ResourceLimit = 36,
    ResourceSchema = 37,
    EncodeInvalid = 39,
    EncodeSourceModel = 41,
    EncodeInvalidSource = 42,
}
impl CodecReason {
    fn fact(self, class: FailureClass) -> FailureFact {
        FailureFact::new(
            class,
            FailureReason::new(domain::PLAN_CODEC, NonZeroU16::new(self as u16).unwrap()),
        )
    }
}
impl FailureProjection for TypeCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidShape(_) => CodecReason::TypeShape.fact(FailureClass::Data),
            Self::ResourceSource(_) => CodecReason::TypeSourceModel.fact(FailureClass::Invariant),
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::Carrier(source) => source.failure_fact(),
            Self::Writer(source) => source.failure_fact(),
            Self::Logical(source) => source.failure_fact(),
            Self::ExecutionValue(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for LogicalTypeCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidShape(_) | Self::Arrow(_) => {
                CodecReason::LogicalShape.fact(FailureClass::Data)
            }
            Self::ResourceSource(_) => {
                CodecReason::LogicalSourceModel.fact(FailureClass::Invariant)
            }
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for BindingCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidShape(_) => CodecReason::BindingShape.fact(FailureClass::Data),
            Self::Control(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Resource(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for FlatPoolResourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Shape(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::ConstructionResource(source) => source.failure_fact(),
            Self::SourceMetadata(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for FlatReaderError {
    fn failure_fact(&self) -> FailureFact {
        use arrow::error::ArrowError;
        match self {
            Self::Projection(source) => source.failure_fact(),
            // This owner reads an admitted in-memory IPC constant stream.
            // Row arithmetic/cast semantics cannot originate in that read.
            Self::Arrow(ArrowError::MemoryError(_)) => {
                CodecReason::FlatReaderMemory.fact(FailureClass::Resource)
            }
            Self::Arrow(
                ArrowError::ParseError(_)
                | ArrowError::SchemaError(_)
                | ArrowError::IpcError(_)
                | ArrowError::InvalidArgumentError(_),
            ) => CodecReason::FlatReaderData.fact(FailureClass::Data),
            Self::Arrow(ArrowError::ExternalError(_) | ArrowError::IoError(_, _)) => {
                CodecReason::FlatReaderUnexpected.fact(FailureClass::Unexpected)
            }
            Self::Arrow(_) => CodecReason::FlatReaderContract.fact(FailureClass::Invariant),
        }
    }
}

impl FailureProjection for crate::physical_schema_v2::SchemaCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Node(source) => source.failure_fact(),
            Self::ConstructionResource(source) => source.failure_fact(),
            Self::SourceCustody(source) => source.failure_fact(),
            Self::SourceMetadata(source) => source.failure_fact(),
            Self::SourceModel(_) => CodecReason::SchemaSourceModel.fact(FailureClass::Invariant),
            Self::InvalidShape(_) => CodecReason::SchemaShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_public_source_schema_v2::PublicSourceSchemaCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Schema(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Connector(source) => source.failure_fact(),
            Self::Layout(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::PublicSchemaShape.fact(FailureClass::Data),
            Self::SourceModel(_) => CodecReason::PublicSchemaModel.fact(FailureClass::Invariant),
        }
    }
}
impl FailureProjection for crate::physical_constant_v2::PhysicalConstantCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Reader(source) => source.failure_fact(),
            Self::Reference(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ConstantShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_value_v2::ValueCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Origin(source) => source.failure_fact(),
            Self::Index(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ValueShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_relation_v2::RelationCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Read(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Properties(source) => source.failure_fact(),
            Self::Identity(source) => source.failure_fact(),
            Self::Index(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::RelationShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_expression_v2::ExpressionCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Semantics(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Value(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Arena(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ExpressionShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_node_v2::NodeCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Properties(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Value(source) => source.failure_fact(),
            Self::Expression(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Relation(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Identity(source) => source.failure_fact(),
            Self::Root(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::NodeShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_control_v2::ControlCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Flow(source) => source.failure_fact(),
            Self::Roots(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ControlShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_semantics_v2::SemanticsCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Parameter(source) => source.failure_fact(),
            Self::Calls(source) => source.failure_fact(),
            Self::Pruning(source) => source.failure_fact(),
            Self::ControlComponent(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::SemanticsShape.fact(FailureClass::Data),
            Self::SourceModel(_) => CodecReason::SemanticsModel.fact(FailureClass::Invariant),
        }
    }
}
impl FailureProjection for crate::physical_provider_binding_v2::ProviderBindingCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Identity(source) => source.failure_fact(),
            Self::Index(source) => source.failure_fact(),
            Self::ArcLayout(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ProviderBindingShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_provider_read_v2::ProviderReadCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Index(source) => source.failure_fact(),
            Self::Contract(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ProviderReadShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_connector_payload_v2::ConnectorPayloadCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Identity(source) => source.failure_fact(),
            Self::Contract(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ConnectorPayloadShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_writer_recipe_v2::WriterRecipeCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ConstructionResource(source) => source.failure_fact(),
            Self::Provider(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Index(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::WriterShape.fact(FailureClass::Data),
            Self::SourceModel(_) => CodecReason::WriterModel.fact(FailureClass::Invariant),
        }
    }
}
impl FailureProjection for crate::physical_read_scan_v2::ReadScanCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Provider(source) => source.failure_fact(),
            Self::Scan(source) => source.failure_fact(),
            Self::Recipe(source) => source.failure_fact(),
            Self::Protocol(source) => source.failure_fact(),
            Self::Domain(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Schema(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Index(source) => source.failure_fact(),
            Self::Node(source) => source.failure_fact(),
            Self::ConstructionResource(source) => source.failure_fact(),
            Self::InvalidSourceSchema(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ReadScanShape.fact(FailureClass::Data),
            Self::SourceModel(_) => CodecReason::ReadScanModel.fact(FailureClass::Invariant),
        }
    }
}
impl FailureProjection for crate::physical_value_origin_v2::ValueOriginCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::ValueOriginShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_properties_v2::PhysicalPropertyCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::PropertyShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_call_requests_v2::CallRequestCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Requests(source) => source.failure_fact(),
            Self::Semantics(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::CallRequestsShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_package_metadata_v2::PackageMetadataCodecError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Identity(source) => source.failure_fact(),
            Self::Projection(source) => source.failure_fact(),
            Self::InvalidShape(_) => CodecReason::PackageMetadataShape.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::physical_package_v2::PackageWireError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ResourceModel(source) => source.failure_fact(),
            Self::InvalidSource(_) => {
                CodecReason::PackageWireSourceModel.fact(FailureClass::Invariant)
            }
            Self::Protobuf(_) => CodecReason::PackageProtobuf.fact(FailureClass::Data),
        }
    }
}
impl FailureProjection for crate::resource_preflight_v2::ResourceModelError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Malformed => CodecReason::ResourceMalformed.fact(FailureClass::Data),
            Self::Limit(_) => CodecReason::ResourceLimit.fact(FailureClass::Resource),
            Self::Schema(_) => CodecReason::ResourceSchema.fact(FailureClass::Invariant),
        }
    }
}
impl FailureProjection for crate::physical_package_v2::PackageDecodeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Wire(source) => source.failure_fact(),
            Self::Metadata(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Schema(source) => source.failure_fact(),
            Self::PublicSourceSchema(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::ProviderBinding(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Read(source) => source.failure_fact(),
            Self::Relation(source) => source.failure_fact(),
            Self::Value(source) => source.failure_fact(),
            Self::Expression(source) => source.failure_fact(),
            Self::Node(source) => source.failure_fact(),
            Self::CallRequests(source) => source.failure_fact(),
            Self::ExpressionControl(source) => source.failure_fact(),
            Self::Semantics(source) => source.failure_fact(),
            Self::Scan(source) => source.failure_fact(),
            Self::Writer(source) => source.failure_fact(),
            Self::Structure(source) => source.failure_fact(),
            Self::Requests(source) => source.failure_fact(),
            Self::Package(source) => source.failure_fact(),
            Self::Invalid(_) => CodecReason::PackageInvalid.fact(FailureClass::Data),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::FailureEvidence;
    use novarocks_type_contract::{
        CompileControlError, ValueTypeError, owned_resources::layout::LayoutResourceError,
    };
    use std::error::Error;

    #[test]
    fn complete_package_delegates_scan_bound_and_original_nested_source() {
        use crate::{
            physical_package_v2::PackageDecodeError, physical_read_scan_v2::ReadScanCodecError,
            physical_schema_v2::SchemaCodecError,
        };
        use novarocks_connector_contract::{
            MAX_STATIC_SCAN_RETAINED_BYTES, StaticConnectorScanError,
        };
        let package = PackageDecodeError::Scan(ReadScanCodecError::Scan(
            StaticConnectorScanError::TooManyRetainedBytes,
        ));
        let projected = package.failure_fact();
        assert_eq!(
            projected,
            StaticConnectorScanError::TooManyRetainedBytes.failure_fact()
        );
        assert_eq!(projected.class(), FailureClass::Resource);
        assert_eq!(
            projected.resource().unwrap().limit,
            Some(MAX_STATIC_SCAN_RETAINED_BYTES as u64)
        );
        assert!(
            package
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<StaticConnectorScanError>()
        );
        let model = PackageDecodeError::Schema(SchemaCodecError::SourceModel(
            "resource exhausted cancelled",
        ));
        assert_eq!(model.failure_fact().class(), FailureClass::Invariant);
    }
    #[test]
    fn constant_request_conversion_does_not_relabel_original_limit_as_control() {
        use crate::physical_call_requests_v2::CallRequestCodecError;
        use novarocks_constant_contract::ConstantError;
        use novarocks_physical_plan::ConstantReferenceError;
        let original = ConstantError::Limit("actual constant profile bound");
        let expected = original.failure_fact();
        let error = CallRequestCodecError::from(ConstantReferenceError::Constant(original));
        assert_eq!(error.failure_fact(), expected);
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<ConstantError>()
        );
    }
    #[test]
    fn nested_type_projection_preserves_actual_resource_and_source() {
        let error = BindingCodecError::Type(TypeCodecError::ValueType(ValueTypeError::TooDeep));
        assert_eq!(error.failure_fact(), ValueTypeError::TooDeep.failure_fact());
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<ValueTypeError>()
        );
        let source = BindingCodecError::Resource(LayoutResourceError::SourceModel);
        assert_eq!(
            source.failure_fact(),
            LayoutResourceError::SourceModel.failure_fact()
        );
        assert!(source.source().unwrap().is::<LayoutResourceError>());
        let resource = FlatReaderError::Projection(FlatPoolResourceError::ConstructionResource(
            LayoutResourceError::ArcBacking,
        ));
        assert_eq!(
            resource.failure_fact(),
            LayoutResourceError::ArcBacking.failure_fact()
        );
        assert!(
            resource
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<LayoutResourceError>()
        );
    }
    #[test]
    fn nested_control_is_only_an_observation_and_shape_ignores_text() {
        let error = FlatReaderError::Projection(FlatPoolResourceError::Shape(
            TypeCodecError::Control(CompileControlError::Cancelled),
        ));
        assert_eq!(
            error.failure_fact(),
            CompileControlError::Cancelled.failure_fact()
        );
        assert_eq!(
            error.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<CompileControlError>()
        );
        for detail in ["cancelled", "memory exhausted", "numeric overflow"] {
            let shape = TypeCodecError::InvalidShape(detail);
            assert_eq!(shape.failure_fact().class(), FailureClass::Data);
            assert_eq!(
                shape.failure_fact().reason().domain(),
                domain::PLAN_CODEC.get()
            );
            assert_eq!(
                shape.failure_fact().reason().code(),
                CodecReason::TypeShape as u16
            );
        }
    }
    #[test]
    fn ipc_reader_retains_sdk_error_without_becoming_row_arithmetic() {
        use arrow::error::ArrowError;
        let memory = FlatReaderError::Arrow(ArrowError::MemoryError("local SDK refusal".into()));
        assert_eq!(memory.failure_fact().class(), FailureClass::Resource);
        assert!(memory.source().unwrap().is::<ArrowError>());
        let external = FlatReaderError::Arrow(ArrowError::ExternalError(Box::new(
            std::io::Error::other("SDK-local cause"),
        )));
        assert_eq!(external.failure_fact().class(), FailureClass::Unexpected);
        assert!(
            external
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<std::io::Error>()
        );
        let arithmetic =
            FlatReaderError::Arrow(ArrowError::ArithmeticOverflow("reader contract".into()));
        assert_eq!(arithmetic.failure_fact().class(), FailureClass::Invariant);
    }
}

impl FailureProjection for crate::physical_package_v2::PhysicalEncodeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Source(source) => source.failure_fact(),
            Self::Invalid(_) => CodecReason::EncodeInvalid.fact(FailureClass::Invariant),
            Self::UnsupportedCapability(_) => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_failure_contract::plan_codec_reason::ENCODE_UNSUPPORTED,
            ),
        }
    }
}

#[cfg(test)]
mod compiled_encode_author_tests {
    use super::*;
    use crate::PhysicalEncodeError;
    use novarocks_functions::FunctionBindingError;
    use novarocks_type_contract::CompileControlError;
    use std::error::Error;

    #[test]
    fn compiled_encoder_preserves_binding_and_control_authors() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let expected = cause.failure_fact();
            let encoded = PhysicalEncodeError::from(FunctionBindingError::Control(cause.clone()));
            assert_eq!(encoded.failure_fact(), expected);
            assert_eq!(
                encoded
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
        let source = FunctionBindingError::Constant(novarocks_functions::ConstantError::Limit(
            "original bound",
        ));
        let fact = source.failure_fact();
        let encoded = PhysicalEncodeError::from(source.clone());
        assert_eq!(encoded.failure_fact(), fact);
        assert_eq!(
            encoded
                .source()
                .unwrap()
                .downcast_ref::<FunctionBindingError>(),
            Some(&source)
        );
    }

    #[test]
    fn compiled_capability_class_is_nominal_and_does_not_read_diagnostics() {
        let unsupported = PhysicalEncodeError::UnsupportedCapability("ordinary diagnostic".into());
        let invalid = PhysicalEncodeError::Invalid("unsupported resource exhausted".into());
        assert_eq!(
            unsupported.failure_fact(),
            FailureFact::new(
                FailureClass::Unsupported,
                FailureReason::new(domain::PLAN_CODEC, NonZeroU16::new(40).unwrap()),
            )
        );
        assert_eq!(
            invalid.failure_fact(),
            CodecReason::EncodeInvalid.fact(FailureClass::Invariant)
        );
    }
}

impl FailureProjection for crate::physical_package_v2::TypeViewError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::SourceModel(_) => CodecReason::EncodeSourceModel.fact(FailureClass::Invariant),
            Self::InvalidSource(_) => {
                CodecReason::EncodeInvalidSource.fact(FailureClass::Invariant)
            }
        }
    }
}
impl FailureProjection for crate::physical_package_v2::ProviderSourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Source(source) => source.failure_fact(),
            Self::Connector(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for crate::physical_package_v2::PackageEncodeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Source(source) => source.failure_fact(),
            Self::Provider(source) => source.failure_fact(),
            Self::Metadata(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::ProviderBinding(source) => source.failure_fact(),
            Self::Payload(source) => source.failure_fact(),
            Self::Read(source) => source.failure_fact(),
            Self::Relation(source) => source.failure_fact(),
            Self::Schema(source) => source.failure_fact(),
            Self::PublicSourceSchema(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Semantics(source) => source.failure_fact(),
            Self::Value(source) => source.failure_fact(),
            Self::Expression(source) => source.failure_fact(),
            Self::Request(source) => source.failure_fact(),
            Self::Node(source) => source.failure_fact(),
            Self::ExpressionControl(source) => source.failure_fact(),
            Self::Scan(source) => source.failure_fact(),
            Self::Writer(source) => source.failure_fact(),
        }
    }
}
