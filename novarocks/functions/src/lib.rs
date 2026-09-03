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

//! Process-wide immutable engine function catalog.
//!
//! The catalog owns function identity, visibility and signature resolution.
//! Execution-specific state erasure is intentionally not part of this crate.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::DataType;
use sha2::{Digest, Sha256};

const FUNCTION_CATALOG_DIGEST_DOMAIN: &[u8] = b"novarocks.engine-function-catalog/v1\0";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
}

impl FunctionKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Scalar => 1,
            Self::Aggregate => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FunctionVisibility {
    Public,
    Hidden,
}

impl FunctionVisibility {
    const fn tag(self) -> u8 {
        match self {
            Self::Public => 1,
            Self::Hidden => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum FunctionVolatility {
    #[default]
    Immutable,
    Volatile,
}

impl FunctionVolatility {
    const fn tag(self) -> u8 {
        match self {
            Self::Immutable => 1,
            Self::Volatile => 2,
        }
    }

    pub const fn is_volatile(self) -> bool {
        matches!(self, Self::Volatile)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFunctionSignature {
    pub return_type: DataType,
    pub argument_types: Vec<DataType>,
    pub enforce_argument_binding: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionResolutionError {
    UnknownFunction,
    HiddenFunction,
    NoMatchingSignature {
        candidates: usize,
        binding_enforced: bool,
    },
    BadSignature(String),
}

impl fmt::Display for FunctionResolutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFunction => formatter.write_str("function not registered"),
            Self::HiddenFunction => formatter.write_str("function is hidden from user SQL"),
            Self::NoMatchingSignature { candidates, .. } => write!(
                formatter,
                "no matching signature among {candidates} registered candidates"
            ),
            Self::BadSignature(message) => write!(formatter, "bad signature: {message}"),
        }
    }
}

impl std::error::Error for FunctionResolutionError {}

/// Safe type-level resolver supplied by a statically linked function bundle.
pub trait FunctionSignatureResolver: Send + Sync {
    fn resolve(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError>;
}

pub struct FunctionDefinition {
    canonical_name: Box<str>,
    kind: FunctionKind,
    visibility: FunctionVisibility,
    volatility: FunctionVolatility,
    canonical_signatures: Box<[Box<str>]>,
    resolver: Arc<dyn FunctionSignatureResolver>,
}

impl fmt::Debug for FunctionDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionDefinition")
            .field("canonical_name", &self.canonical_name)
            .field("kind", &self.kind)
            .field("visibility", &self.visibility)
            .field("volatility", &self.volatility)
            .field("canonical_signatures", &self.canonical_signatures)
            .finish_non_exhaustive()
    }
}

impl FunctionDefinition {
    pub fn try_new(
        canonical_name: impl AsRef<str>,
        kind: FunctionKind,
        visibility: FunctionVisibility,
        volatility: FunctionVolatility,
        canonical_signatures: impl IntoIterator<Item = impl AsRef<str>>,
        resolver: Arc<dyn FunctionSignatureResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        validate_canonical_name(canonical_name)?;
        let mut canonical_signatures = canonical_signatures
            .into_iter()
            .map(|signature| signature.as_ref().to_string().into_boxed_str())
            .collect::<Vec<_>>();
        if canonical_signatures.is_empty() {
            return Err(FunctionCatalogError::EmptySignatureSet {
                name: canonical_name.into(),
            });
        }
        canonical_signatures.sort_unstable();
        for pair in canonical_signatures.windows(2) {
            if pair[0] == pair[1] {
                return Err(FunctionCatalogError::DuplicateSignature {
                    name: canonical_name.into(),
                    signature: pair[0].clone(),
                });
            }
        }
        if let Some(signature) = canonical_signatures.iter().find(|value| value.is_empty()) {
            return Err(FunctionCatalogError::EmptySignature {
                name: canonical_name.into(),
                signature: signature.clone(),
            });
        }
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind,
            visibility,
            volatility,
            canonical_signatures: canonical_signatures.into_boxed_slice(),
            resolver,
        })
    }

    pub fn canonical_name(&self) -> &str {
        &self.canonical_name
    }

    pub const fn kind(&self) -> FunctionKind {
        self.kind
    }

    pub const fn visibility(&self) -> FunctionVisibility {
        self.visibility
    }

    pub const fn volatility(&self) -> FunctionVolatility {
        self.volatility
    }

    pub fn canonical_signatures(&self) -> &[Box<str>] {
        &self.canonical_signatures
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionCatalogError {
    EmptyCatalog,
    InvalidCanonicalName { name: Box<str> },
    EmptySignatureSet { name: Box<str> },
    EmptySignature { name: Box<str>, signature: Box<str> },
    DuplicateSignature { name: Box<str>, signature: Box<str> },
    DuplicateDefinition { name: Box<str>, kind: FunctionKind },
}

impl fmt::Display for FunctionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCatalog => formatter.write_str("engine function catalog is empty"),
            Self::InvalidCanonicalName { name } => {
                write!(formatter, "invalid canonical function name `{name}`")
            }
            Self::EmptySignatureSet { name } => {
                write!(formatter, "function `{name}` has no declared signatures")
            }
            Self::EmptySignature { name, .. } => {
                write!(formatter, "function `{name}` has an empty signature")
            }
            Self::DuplicateSignature { name, signature } => write!(
                formatter,
                "function `{name}` declares duplicate signature `{signature}`"
            ),
            Self::DuplicateDefinition { name, kind } => {
                write!(formatter, "duplicate {kind:?} function definition `{name}`")
            }
        }
    }
}

impl std::error::Error for FunctionCatalogError {}

fn validate_canonical_name(name: &str) -> Result<(), FunctionCatalogError> {
    let valid = !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'$')
        });
    if !valid {
        return Err(FunctionCatalogError::InvalidCanonicalName { name: name.into() });
    }
    Ok(())
}

#[derive(Default)]
pub struct EngineFunctionCatalogBuilder {
    definitions: BTreeMap<(Box<str>, FunctionKind), FunctionDefinition>,
}

impl EngineFunctionCatalogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, definition: FunctionDefinition) -> Result<(), FunctionCatalogError> {
        let key = (
            definition.canonical_name.clone(),
            definition.kind,
        );
        if self.definitions.contains_key(&key) {
            return Err(FunctionCatalogError::DuplicateDefinition {
                name: definition.canonical_name.clone(),
                kind: definition.kind,
            });
        }
        self.definitions.insert(key, definition);
        Ok(())
    }

    pub fn seal(self) -> Result<EngineFunctionCatalog, FunctionCatalogError> {
        if self.definitions.is_empty() {
            return Err(FunctionCatalogError::EmptyCatalog);
        }
        let definitions = self.definitions.into_values().collect::<Vec<_>>();
        let digest = digest_definitions(&definitions);
        Ok(EngineFunctionCatalog {
            definitions: definitions.into_boxed_slice(),
            digest,
        })
    }
}

pub trait FunctionBundleContributor {
    fn contribute(
        &self,
        builder: &mut EngineFunctionCatalogBuilder,
    ) -> Result<(), FunctionCatalogError>;
}

pub struct EngineFunctionCatalog {
    definitions: Box<[FunctionDefinition]>,
    digest: [u8; 32],
}

impl fmt::Debug for EngineFunctionCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EngineFunctionCatalog")
            .field("definitions", &self.definitions)
            .field("digest", &self.digest)
            .finish()
    }
}

impl EngineFunctionCatalog {
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn definitions(&self) -> &[FunctionDefinition] {
        &self.definitions
    }

    pub fn definition(&self, name: &str, kind: FunctionKind) -> Option<&FunctionDefinition> {
        let canonical_name = name.to_ascii_lowercase();
        self.definitions
            .binary_search_by(|candidate| {
                (candidate.canonical_name(), candidate.kind())
                    .cmp(&(canonical_name.as_str(), kind))
            })
            .ok()
            .map(|index| &self.definitions[index])
    }

    pub fn resolve_user(
        &self,
        name: &str,
        kind: FunctionKind,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        let definition = self
            .definition(name, kind)
            .ok_or(FunctionResolutionError::UnknownFunction)?;
        if definition.visibility == FunctionVisibility::Hidden {
            return Err(FunctionResolutionError::HiddenFunction);
        }
        definition.resolver.resolve(argument_types)
    }

    pub fn resolve_trusted(
        &self,
        name: &str,
        kind: FunctionKind,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        self.definition(name, kind)
            .ok_or(FunctionResolutionError::UnknownFunction)?
            .resolver
            .resolve(argument_types)
    }
}

fn digest_definitions(definitions: &[FunctionDefinition]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FUNCTION_CATALOG_DIGEST_DOMAIN);
    hasher.update(
        u32::try_from(definitions.len())
            .expect("function definition count fits u32")
            .to_be_bytes(),
    );
    for definition in definitions {
        let name = definition.canonical_name().as_bytes();
        hasher.update(
            u16::try_from(name.len())
                .expect("validated function name fits u16")
                .to_be_bytes(),
        );
        hasher.update(name);
        hasher.update([definition.kind.tag()]);
        hasher.update([definition.visibility.tag()]);
        hasher.update([definition.volatility.tag()]);
        hasher.update(
            u32::try_from(definition.canonical_signatures.len())
                .expect("function signature count fits u32")
                .to_be_bytes(),
        );
        for signature in definition.canonical_signatures() {
            let signature = signature.as_bytes();
            hasher.update(
                u32::try_from(signature.len())
                    .expect("function signature length fits u32")
                    .to_be_bytes(),
            );
            hasher.update(signature);
        }
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Int64Resolver;

    impl FunctionSignatureResolver for Int64Resolver {
        fn resolve(
            &self,
            argument_types: &[DataType],
        ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
            if argument_types != [DataType::Int64] {
                return Err(FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                });
            }
            Ok(ResolvedFunctionSignature {
                return_type: DataType::Int64,
                argument_types: argument_types.to_vec(),
                enforce_argument_binding: true,
            })
        }
    }

    fn definition(name: &str, visibility: FunctionVisibility) -> FunctionDefinition {
        FunctionDefinition::try_new(
            name,
            FunctionKind::Aggregate,
            visibility,
            FunctionVolatility::Immutable,
            ["(int64)->int64"],
            Arc::new(Int64Resolver),
        )
        .expect("definition")
    }

    #[test]
    fn hidden_functions_require_trusted_resolution() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(definition("$hidden", FunctionVisibility::Hidden))
            .unwrap();
        let catalog = builder.seal().unwrap();
        assert_eq!(
            catalog.resolve_user("$hidden", FunctionKind::Aggregate, &[DataType::Int64]),
            Err(FunctionResolutionError::HiddenFunction)
        );
        assert_eq!(
            catalog
                .resolve_trusted("$hidden", FunctionKind::Aggregate, &[DataType::Int64])
                .unwrap()
                .return_type,
            DataType::Int64
        );
    }

    #[test]
    fn duplicate_name_and_kind_fail_closed() {
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(definition("count", FunctionVisibility::Public))
            .unwrap();
        let error = builder
            .register(definition("count", FunctionVisibility::Public))
            .unwrap_err();
        assert!(matches!(error, FunctionCatalogError::DuplicateDefinition { .. }));
    }

    #[test]
    fn digest_is_independent_of_contribution_order_and_covers_visibility() {
        let build = |reverse: bool, hidden: bool| {
            let mut builder = EngineFunctionCatalogBuilder::new();
            let mut definitions = vec![
                definition("alpha", FunctionVisibility::Public),
                definition(
                    "beta",
                    if hidden {
                        FunctionVisibility::Hidden
                    } else {
                        FunctionVisibility::Public
                    },
                ),
            ];
            if reverse {
                definitions.reverse();
            }
            for definition in definitions {
                builder.register(definition).unwrap();
            }
            builder.seal().unwrap().digest()
        };
        assert_eq!(build(false, false), build(true, false));
        assert_ne!(build(false, false), build(false, true));
    }

    #[test]
    fn definitions_require_canonical_names_and_unique_signatures() {
        assert!(matches!(
            FunctionDefinition::try_new(
                "NotCanonical",
                FunctionKind::Scalar,
                FunctionVisibility::Public,
                FunctionVolatility::Immutable,
                ["()->int64"],
                Arc::new(Int64Resolver),
            ),
            Err(FunctionCatalogError::InvalidCanonicalName { .. })
        ));
        assert!(matches!(
            FunctionDefinition::try_new(
                "duplicate",
                FunctionKind::Scalar,
                FunctionVisibility::Public,
                FunctionVolatility::Immutable,
                ["()->int64", "()->int64"],
                Arc::new(Int64Resolver),
            ),
            Err(FunctionCatalogError::DuplicateSignature { .. })
        ));
    }
}
