// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Package header and local annotation projection. The wire revision is
//! validated before decoding any namespace that uses its representation.
//! These contributions join a caller invoice; they are not allocation grants.

use crate::physical_node_v2::{self as resource, Model, NodeCodecError};
use crate::physical_result_v2::{copy_box, copy_string};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::{fmt, mem::size_of};

pub use crate::physical_node_v2::{
    NodeProjectionFacts as PackageMetadataProjectionFacts,
    NodeProjectionLimits as PackageMetadataProjectionLimits,
};

#[derive(Debug)]
pub enum PackageMetadataCodecError {
    Control(CompileControlError),
    Identity(p::IdentityError),
    Projection(NodeCodecError),
    InvalidShape(&'static str),
    PlanContractRevision { required: u32 },
}
type Error = PackageMetadataCodecError;
impl From<CompileControlError> for Error {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<NodeCodecError> for Error {
    fn from(error: NodeCodecError) -> Self {
        match error {
            NodeCodecError::Control(cause) => Self::Control(cause),
            error => Self::Projection(error),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Identity(error) => error.fmt(f),
            Self::Projection(error) => error.fmt(f),
            Self::InvalidShape(message) => f.write_str(message),
            Self::PlanContractRevision { required } => write!(
                f,
                "fragment package plan contract revision differs: expected {}, got {required}",
                p::PLAN_CONTRACT_REVISION,
            ),
        }
    }
}
impl std::error::Error for PackageMetadataCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Identity(source) => Some(source),
            Self::Projection(source) => Some(source),
            _ => None,
        }
    }
}

pub struct EncodedPackageMetadata {
    pub plan_version: Vec<u8>,
    pub required: wire::RequiredContracts,
    pub annotations: Vec<wire::PlanAnnotation>,
}
pub struct DecodedPackageMetadata {
    pub version: p::PlanVersionId,
    pub required: p::RequiredContracts,
    pub annotations: Box<[p::PlanAnnotation]>,
}
fn required<T>(value: Option<T>, message: &'static str) -> Result<T, Error> {
    value.ok_or(Error::InvalidShape(message))
}
fn encode_subject(subject: p::AnnotationSubject) -> Result<wire::AnnotationSubject, Error> {
    use wire::annotation_subject::Kind;
    let kind = match subject {
        p::AnnotationSubject::Plan => {
            return Err(Error::InvalidShape(
                "local package annotation names the global plan",
            ));
        }
        p::AnnotationSubject::Fragment(fragment) => Kind::FragmentId(fragment.get()),
        p::AnnotationSubject::Node(fragment, node) => Kind::Node(wire::FragmentNodeSubject {
            fragment_id: Some(fragment.get()),
            node_id: Some(node.get()),
        }),
        p::AnnotationSubject::Value(fragment, value) => Kind::Value(wire::FragmentValueSubject {
            fragment_id: Some(fragment.get()),
            value_id: Some(value.get()),
        }),
    };
    Ok(wire::AnnotationSubject { kind: Some(kind) })
}
fn decode_subject(
    subject: Option<&wire::AnnotationSubject>,
) -> Result<p::AnnotationSubject, Error> {
    use wire::annotation_subject::Kind;
    let subject = required(subject, "annotation subject is absent")?;
    Ok(
        match required(subject.kind.as_ref(), "annotation subject kind is absent")? {
            Kind::FragmentId(id) => p::AnnotationSubject::Fragment(p::FragmentId::new(*id)),
            Kind::Node(node) => p::AnnotationSubject::Node(
                p::FragmentId::new(required(
                    node.fragment_id,
                    "annotation fragment ID is absent",
                )?),
                p::NodeId::new(required(node.node_id, "annotation node ID is absent")?),
            ),
            Kind::Value(value) => p::AnnotationSubject::Value(
                p::FragmentId::new(required(
                    value.fragment_id,
                    "annotation fragment ID is absent",
                )?),
                p::ValueId::new(required(value.value_id, "annotation value ID is absent")?),
            ),
        },
    )
}
fn gate(
    model: &Model,
    source: usize,
    limits: Option<PackageMetadataProjectionLimits>,
    admit: &mut impl FnMut(&PackageMetadataProjectionFacts) -> Result<(), CompileControlError>,
) -> Result<PackageMetadataProjectionFacts, Error> {
    // All known numerical axes and the parent's cumulative invoice precede
    // the next completed observation, including after lengths are captured.
    let facts = model.numerical_facts(source, 0, limits)?;
    admit(&facts)?;
    Ok(facts)
}
fn source_floor(source: usize, known: usize) -> Result<(), Error> {
    if source < known {
        Err(Error::InvalidShape(
            "package metadata source invoice is understated",
        ))
    } else {
        Ok(())
    }
}

/// Borrow one actual package and the caller's existing Encode checkpoints.
/// No namespace, allocation scope, entry or footer is minted here.
pub fn encode_package_metadata_observed(
    input: &p::FragmentPackage,
    source_retained_bytes: usize,
    limits: impl Into<Option<PackageMetadataProjectionLimits>>,
    admit: &mut impl FnMut(&PackageMetadataProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(EncodedPackageMetadata, PackageMetadataProjectionFacts), Error> {
    let limits = limits.into();
    let annotations = input.annotations();
    let mut model = Model {
        items: annotations.len(),
        ..Model::default()
    };
    model.request::<u8>(16, 1)?;
    model.request::<wire::PlanAnnotation>(annotations.len(), 1)?;
    gate(&model, source_retained_bytes, limits, admit)?;
    let mut known = resource::add(
        size_of::<p::FragmentPackage>(),
        resource::bytes::<p::PlanAnnotation>(annotations.len())?,
    )?;
    if limits.is_some() {
        source_floor(source_retained_bytes, known)?;
    }
    for annotation in annotations {
        model.request::<u8>(annotation.key.len(), 1)?;
        model.request::<u8>(annotation.value.len(), 1)?;
        known = resource::add(
            known,
            resource::add(annotation.key.len(), annotation.value.len())?,
        )?;
        gate(&model, source_retained_bytes, limits, admit)?;
        if limits.is_some() {
            source_floor(source_retained_bytes, known)?;
        }
        let subject = encode_subject(annotation.subject);
        work.step()?;
        subject?;
    }
    let facts = gate(&model, source_retained_bytes, limits, admit)?;
    work.step()?;
    let mut plan_version = resource::reserve(16, work)?;
    for byte in input.version().as_bytes() {
        plan_version.push(*byte);
        work.step()?;
    }
    let mut output = resource::reserve(annotations.len(), work)?;
    for annotation in annotations {
        output.push(wire::PlanAnnotation {
            subject: Some(encode_subject(annotation.subject)?),
            key: copy_string(&annotation.key, work)?,
            value: copy_string(&annotation.value, work)?,
        });
        work.step()?;
    }
    Ok((
        EncodedPackageMetadata {
            plan_version,
            required: wire::RequiredContracts {
                plan_contract_revision: input.required().plan_contract_revision,
            },
            annotations: output,
        },
        facts,
    ))
}

/// Decode every original local annotation into fresh bounded owned backing.
/// Reject an unknown wire revision before projecting annotations.
pub fn decode_package_metadata_observed(
    input: &wire::FragmentPackage,
    source_retained_bytes: usize,
    limits: impl Into<Option<PackageMetadataProjectionLimits>>,
    admit: &mut impl FnMut(&PackageMetadataProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(DecodedPackageMetadata, PackageMetadataProjectionFacts), Error> {
    let limits = limits.into();
    let mut model = Model {
        items: input.annotations.len(),
        ..Model::default()
    };
    model.request::<p::PlanAnnotation>(input.annotations.len(), 2)?;
    gate(&model, source_retained_bytes, limits, admit)?;
    let bytes: [u8; 16] = input
        .plan_version
        .as_slice()
        .try_into()
        .map_err(|_| Error::InvalidShape("plan version must contain exactly 16 bytes"))?;
    let version = p::PlanVersionId::try_new(bytes).map_err(Error::Identity)?;
    let revision =
        required(input.required.as_ref(), "required contracts are absent")?.plan_contract_revision;
    if revision != p::PLAN_CONTRACT_REVISION {
        return Err(Error::PlanContractRevision { required: revision });
    }
    let required = p::RequiredContracts {
        plan_contract_revision: revision,
    };
    let mut known = resource::add(
        size_of::<wire::FragmentPackage>(),
        resource::add(
            input.plan_version.capacity(),
            resource::bytes::<wire::PlanAnnotation>(input.annotations.capacity())?,
        )?,
    )?;
    if limits.is_some() {
        source_floor(source_retained_bytes, known)?;
    }
    for annotation in &input.annotations {
        model.request::<u8>(annotation.key.len(), 2)?;
        model.request::<u8>(annotation.value.len(), 2)?;
        known = resource::add(
            known,
            resource::add(annotation.key.capacity(), annotation.value.capacity())?,
        )?;
        gate(&model, source_retained_bytes, limits, admit)?;
        if limits.is_some() {
            source_floor(source_retained_bytes, known)?;
        }
        let subject = decode_subject(annotation.subject.as_ref());
        work.step()?;
        subject?;
    }
    let facts = gate(&model, source_retained_bytes, limits, admit)?;
    work.step()?;
    let mut annotations = resource::reserve(input.annotations.len(), work)?;
    for annotation in &input.annotations {
        annotations.push(p::PlanAnnotation {
            subject: decode_subject(annotation.subject.as_ref())?,
            key: copy_box(&annotation.key, work)?,
            value: copy_box(&annotation.value, work)?,
        });
        work.step()?;
    }
    let annotations = resource::boxed(annotations, work)?;
    Ok((
        DecodedPackageMetadata {
            version,
            required,
            annotations,
        },
        facts,
    ))
}

#[cfg(test)]
#[path = "physical_package_metadata_v2/tests.rs"]
mod tests;
