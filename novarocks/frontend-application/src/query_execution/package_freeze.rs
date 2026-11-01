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

//! Freeze one completed SQL plan into v2 FragmentPackage wire bytes.
//!
//! The SQL owner authors each fragment's uses, calls and pruning; each scan's
//! frozen read is authored from the plan and the freeze that produced it; each
//! written target's frozen recipe is authored from the write session that
//! sealed it; the original extraction law publishes the checked packages; the
//! v2 sender encodes them.
//!
//! The host provides [`CompiledPackageCarrier`] admission, encode limits and
//! generated receiver resource model once, paired with the compiled backend.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use novarocks_connector_contract::ConnectorWriteRecipeDraft;
use novarocks_physical_plan::{
    ConstantPolicy, FragmentId, FragmentPackage, FragmentPackageAdmission,
    ProviderReadOccurrenceId, WriteTargetOrdinal, extract_fragment_packages,
};
#[cfg(test)]
use novarocks_plan_codec::physical_package_v2::PackageDecodeLimits;
use novarocks_plan_codec::physical_package_v2::{
    PackageEncodeError, PackageEncodeLimits, encode_fragment_package,
};
use novarocks_plan_codec::resource_preflight_v2::{
    DecodeProjectionLimits, FragmentDecodeResourceModel, ResourceModelError,
};
use novarocks_query_application::preparation::CompletedPhysicalPlanCandidate;
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use prost::Message;

use crate::query_execution::package_reads::author_frozen_reads;
use crate::query_execution::provider_read_facts::FrozenReadEncoding;

/// Host-owned configuration of the compiled package carrier.
///
/// Structural ceilings and the immutable generated receiver model are supplied
/// by the one startup composition; none grants runtime capacity or defaults
/// here. The statement constant policy is not part of it: each plan is packaged
/// with the policy its own statement was compiled with.
#[derive(Clone)]
pub struct CompiledPackageCarrier {
    admission: FragmentPackageAdmission,
    limits: PackageEncodeLimits,
    receiver_model: Arc<FragmentDecodeResourceModel>,
    receiver_limits: DecodeProjectionLimits,
}

impl CompiledPackageCarrier {
    pub fn new(
        admission: FragmentPackageAdmission,
        limits: PackageEncodeLimits,
        receiver_model: Arc<FragmentDecodeResourceModel>,
        receiver_limits: DecodeProjectionLimits,
    ) -> Self {
        Self {
            admission,
            limits,
            receiver_model,
            receiver_limits,
        }
    }

    /// The same immutable generated model the host supplied to its receiver.
    pub fn receiver_model(&self) -> &Arc<FragmentDecodeResourceModel> {
        &self.receiver_model
    }

    pub const fn receiver_limits(&self) -> DecodeProjectionLimits {
        self.receiver_limits
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        admission: FragmentPackageAdmission,
        limits: PackageEncodeLimits,
    ) -> Self {
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        Self::new(
            admission,
            limits,
            Arc::new(FragmentDecodeResourceModel::try_new(&control).expect("test package model")),
            PackageDecodeLimits::FROZEN.wire,
        )
    }

    /// The admission every fragment's checked package is constructed under.
    pub const fn admission(&self) -> &FragmentPackageAdmission {
        &self.admission
    }

    /// The limits every package is encoded under.
    pub const fn limits(&self) -> &PackageEncodeLimits {
        &self.limits
    }
}

impl fmt::Debug for CompiledPackageCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledPackageCarrier")
            .field("admission", &self.admission)
            .field("limits", &self.limits)
            .field("receiver_limits", &self.receiver_limits)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) enum PackageFreezeError {
    Control(CompileControlError),
    Semantics(novarocks_sql::compiler::PackageSemanticsError),
    Extraction(String),
    Encode(PackageEncodeError),
    Receiver(ResourceModelError),
    /// A carrier fact the plan states inconsistently.
    Facts(String),
    /// A frozen provider read the package cannot carry as frozen.
    Read(String),
    /// A frozen writer recipe the package cannot carry as frozen.
    Write(String),
}
impl fmt::Display for PackageFreezeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Semantics(error) => error.fmt(f),
            Self::Extraction(detail) => write!(f, "fragment package extraction: {detail}"),
            Self::Encode(error) => error.fmt(f),
            Self::Receiver(error) => error.fmt(f),
            Self::Facts(detail) => write!(f, "package carrier facts: {detail}"),
            Self::Read(detail) => write!(f, "frozen provider read: {detail}"),
            Self::Write(detail) => write!(f, "frozen writer recipe: {detail}"),
        }
    }
}
impl std::error::Error for PackageFreezeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Semantics(source) => Some(source),
            Self::Encode(source) => Some(source),
            Self::Receiver(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for PackageFreezeError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let code = match self {
            Self::Control(cause) => return cause.failure_fact(),
            Self::Semantics(source) => return source.failure_fact(),
            Self::Encode(source) => return source.failure_fact(),
            Self::Receiver(source) => return source.failure_fact(),
            Self::Extraction(_) => 200,
            Self::Facts(_) => 201,
            Self::Read(_) => 202,
            Self::Write(_) => 203,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

/// A completed plan the compiled carrier cannot freeze is refused through the
/// encoder's own error, keeping a caller's control cause primary.
impl From<PackageFreezeError> for novarocks_plan_codec::PhysicalEncodeError {
    fn from(error: PackageFreezeError) -> Self {
        match error {
            PackageFreezeError::Control(cause) => Self::Control(cause),
            other => Self::from_domain(other),
        }
    }
}

impl From<CompileControlError> for PackageFreezeError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}

impl From<ResourceModelError> for PackageFreezeError {
    fn from(error: ResourceModelError) -> Self {
        match error {
            ResourceModelError::Control(cause) => Self::Control(cause),
            other => Self::Receiver(other),
        }
    }
}

/// A receiver refusal already owns its original typed cause; it is not a
/// successful encode whose ordinary tail may replace that cause.
pub(crate) fn is_receiver_refusal(error: &novarocks_plan_codec::PhysicalEncodeError) -> bool {
    match error {
        novarocks_plan_codec::PhysicalEncodeError::Source(source) => matches!(
            source.downcast_ref::<PackageFreezeError>(),
            Some(PackageFreezeError::Receiver(_))
        ),
        _ => false,
    }
}

/// Every fragment's v2 package bytes. The host admission and encode limits
/// are caller-owned configuration; none is defaulted here. `encodings` are
/// what the freeze of each of the plan's provider reads kept; `writes` is the
/// frozen recipe of exactly each target the plan writes.
#[cfg(test)]
pub(crate) fn freeze_fragment_packages(
    candidate: &CompletedPhysicalPlanCandidate,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    statement_constant_policy: ConstantPolicy,
    admission: &FragmentPackageAdmission,
    limits: &PackageEncodeLimits,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, Vec<u8>>, PackageFreezeError> {
    let packages = extract_checked_packages(
        candidate,
        encodings,
        writes,
        statement_constant_policy,
        admission,
        control,
    )?;
    let carrier = CompiledPackageCarrier::new(
        *admission,
        *limits,
        Arc::new(FragmentDecodeResourceModel::try_new(control)?),
        PackageDecodeLimits::FROZEN.wire,
    );
    let mut output = BTreeMap::new();
    for (id, package) in packages {
        output.insert(id, encode_checked_package(&package, &carrier, control)?);
    }
    Ok(output)
}

/// Every fragment's checked v2 package, before encoding. The extraction law
/// routes each writer's recipe by its target ordinal and refuses a missing or
/// unused one.
pub(crate) fn extract_checked_packages(
    candidate: &CompletedPhysicalPlanCandidate,
    encodings: &BTreeMap<ProviderReadOccurrenceId, FrozenReadEncoding>,
    writes: &BTreeMap<WriteTargetOrdinal, ConnectorWriteRecipeDraft>,
    statement_constant_policy: ConstantPolicy,
    admission: &FragmentPackageAdmission,
    control: &dyn PureCompileControl,
) -> Result<BTreeMap<FragmentId, FragmentPackage>, PackageFreezeError> {
    let plan = candidate.plan();
    let scans = author_frozen_reads(plan, encodings, control)?;
    let semantics = candidate
        .author_package_semantics(statement_constant_policy, control)
        .map_err(|error| match error {
            novarocks_sql::compiler::PackageSemanticsError::Control(cause) => {
                PackageFreezeError::Control(cause)
            }
            other => PackageFreezeError::Semantics(other),
        })?;
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, authored) in semantics {
        uses.insert(id, authored.expression_uses);
        calls.insert(id, authored.calls);
        pruning.insert(id, authored.pruning);
        admissions.insert(id, *admission);
    }
    extract_fragment_packages(
        plan,
        &scans,
        writes,
        &uses,
        &calls,
        &pruning,
        &admissions,
        control,
    )
    .map_err(|error| match error {
        novarocks_physical_plan::FragmentPackageExtractionError::Control(cause) => {
            PackageFreezeError::Control(cause)
        }
        other => PackageFreezeError::Extraction(format!("{other:?}")),
    })
}

/// The exact wire bytes of one checked package.
pub(crate) fn encode_checked_package(
    package: &FragmentPackage,
    carrier: &CompiledPackageCarrier,
    control: &dyn PureCompileControl,
) -> Result<Vec<u8>, PackageFreezeError> {
    let wire =
        encode_fragment_package(package, carrier.limits(), control).map_err(
            |error| match error {
                PackageEncodeError::Control(cause) => PackageFreezeError::Control(cause),
                other => PackageFreezeError::Encode(other),
            },
        )?;
    let bytes = wire.encode_to_vec();
    // Qualify these exact final bytes before any Native artifact/Task exists.
    // The original statement owns control and progress; the model is startup
    // metadata and grants neither a resource budget nor a dispatch capability.
    carrier
        .receiver_model
        .preflight(&bytes, carrier.receiver_limits, control)?;
    Ok(bytes)
}

#[cfg(test)]
mod error_projection_tests {
    use super::*;
    use novarocks_plan_codec::PhysicalEncodeError;
    use novarocks_sql::compiler::PackageSemanticsError;

    #[test]
    fn receiver_refusals_keep_original_resource_data_and_schema_authors() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use std::error::Error;
        for (original, class) in [
            (
                ResourceModelError::Limit("protobuf field occurrences exceed caller limit"),
                FailureClass::Resource,
            ),
            (ResourceModelError::Malformed, FailureClass::Data),
            (
                ResourceModelError::Schema("missing resource schema root"),
                FailureClass::Invariant,
            ),
        ] {
            let expected = original.failure_fact();
            let encoded = PhysicalEncodeError::from(PackageFreezeError::from(original.clone()));
            assert!(is_receiver_refusal(&encoded));
            assert_eq!(encoded.failure_fact(), expected);
            assert_eq!(expected.class(), class);
            let PhysicalEncodeError::Source(source) = &encoded else {
                panic!("original freeze author");
            };
            let freeze = source.downcast_ref::<PackageFreezeError>().unwrap();
            assert_eq!(
                freeze
                    .source()
                    .unwrap()
                    .downcast_ref::<ResourceModelError>(),
                Some(&original)
            );
            assert_eq!(encoded.to_string(), original.to_string());
        }
    }

    #[test]
    fn receiver_preflight_keeps_malformed_prefix_separate_from_a_capacity_refusal() {
        let control = novarocks_sql::compiler::SqlCompileControl::unbounded();
        let model = FragmentDecodeResourceModel::try_new(&control).unwrap();
        // Resource preflight retains a malformed prefix's accounted work.
        // It does not replace Prost's later format verdict with a capacity error.
        let projection = model
            .preflight(&[0x80], PackageDecodeLimits::FROZEN.wire, &control)
            .unwrap();
        assert_eq!(
            projection.status,
            novarocks_plan_codec::resource_preflight_v2::ResourceCursorStatus::MalformedPrefix
        );
        let mut receiver = PackageDecodeLimits::FROZEN.wire;
        receiver.max_input_bytes = 0;
        assert!(matches!(
            model.preflight(&[0x80], receiver, &control),
            Err(ResourceModelError::Limit(_))
        ));
    }

    #[test]
    fn package_freeze_preserves_only_typed_semantic_capability_refusals() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        for original in [
            PackageSemanticsError::UnsupportedCapability("cast List<Int8> to List<Int64>".into()),
            PackageSemanticsError::Effects("UnsupportedCast diagnostic".into()),
        ] {
            let expected = original.failure_fact();
            let error = PhysicalEncodeError::from(PackageFreezeError::Semantics(original));
            assert_eq!(error.failure_fact(), expected);
            let PhysicalEncodeError::Source(source) = error else {
                panic!("retain the actual freeze author")
            };
            let Some(PackageFreezeError::Semantics(original)) =
                source.downcast_ref::<PackageFreezeError>()
            else {
                panic!("retain the nested SQL semantics author")
            };
            assert_eq!(original.failure_fact(), expected);
            assert_eq!(
                matches!(original, PackageSemanticsError::UnsupportedCapability(_)),
                expected.class() == FailureClass::Unsupported
            );
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            assert_eq!(
                PhysicalEncodeError::from(PackageFreezeError::Control(cause.clone())),
                PhysicalEncodeError::Control(cause)
            );
        }
    }
    #[test]
    fn package_freeze_keeps_comparison_source_identity_fact_and_detail() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        use novarocks_functions::ComparisonPrepareError;
        use std::error::Error;
        for (original, class) in [
            (
                ComparisonPrepareError::Unsupported,
                FailureClass::Unsupported,
            ),
            (
                ComparisonPrepareError::TypeMismatch,
                FailureClass::Invariant,
            ),
        ] {
            let source = SharedFailureCause::new(original.clone());
            let expected = original.failure_fact();
            let error = PhysicalEncodeError::from(PackageFreezeError::Semantics(
                PackageSemanticsError::PrimitiveSource {
                    source: source.clone(),
                    cast: None,
                },
            ));
            assert_eq!(error.failure_fact(), expected);
            let PhysicalEncodeError::Source(outer) = &error else {
                panic!("freeze must retain its actual author");
            };
            let freeze = outer.downcast_ref::<PackageFreezeError>().unwrap();
            let PackageFreezeError::Semantics(PackageSemanticsError::PrimitiveSource {
                source: actual,
                cast: None,
            }) = freeze
            else {
                panic!("comparison source identity was not retained");
            };
            assert_eq!(actual, &source);
            assert_eq!(actual.detail(), source.detail());
            assert_eq!(
                actual.downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            assert_eq!(
                actual
                    .source()
                    .unwrap()
                    .downcast_ref::<ComparisonPrepareError>(),
                Some(&original)
            );
            assert_eq!(error.to_string(), source.detail());
            assert_eq!(error.failure_fact().class(), class);
        }
    }
}
