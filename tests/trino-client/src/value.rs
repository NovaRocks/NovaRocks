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

use crate::StatementFailure;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

#[derive(Clone, Debug, PartialEq)]
pub enum TrinoValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f32),
    Double(f64),
    Text(String),
    Binary(Vec<u8>),
    Array(Vec<TrinoValue>),
    Row(Vec<TrinoValue>),
    /// Entries retain their order in the original JSON document.
    Map(Vec<(TrinoValue, TrinoValue)>),
    Variant(String),
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TypeSignature {
    pub raw_type: String,
    #[serde(default)]
    pub arguments: Vec<TypeArgument>,
}
// JSON metadata adds several protocol containers per logical type level.
// Capture the original subtree once, then parse shallow borrowed views with an
// explicit postorder stack. RawValue's lexical reader is iterative; children
// borrow that one capture rather than copying each ancestor's entire subtree.
impl<'de> Deserialize<'de> for TypeSignature {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        decode_signature(&raw).map_err(serde::de::Error::custom)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignatureWire<'a> {
    raw_type: String,
    #[serde(default, borrow)]
    arguments: Vec<ArgumentWire<'a>>,
}
#[derive(Deserialize)]
struct ArgumentWire<'a> {
    kind: String,
    #[serde(borrow)]
    value: &'a RawValue,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NamedWire<'a> {
    field_name: Option<RowFieldName>,
    #[serde(borrow)]
    type_signature: &'a RawValue,
}

enum SignatureWork<'a> {
    Type(&'a RawValue),
    Argument(ArgumentWire<'a>),
    FinishType { raw_type: String, arguments: usize },
    FinishArgument,
    FinishNamed(Option<RowFieldName>),
}
enum SignaturePart {
    Type(TypeSignature),
    Argument(TypeArgument),
}
fn decode_signature(raw: &RawValue) -> Result<TypeSignature, serde_json::Error> {
    let SignaturePart::Type(signature) = decode_signature_part(SignatureWork::Type(raw))? else {
        unreachable!("postorder root type");
    };
    Ok(signature)
}
fn decode_signature_part(root: SignatureWork<'_>) -> Result<SignaturePart, serde_json::Error> {
    use SignaturePart as P;
    use SignatureWork as W;
    let mut work = vec![root];
    let mut parts = Vec::new();
    while let Some(next) = work.pop() {
        match next {
            W::Type(raw) => {
                let wire: SignatureWire<'_> = serde_json::from_str(raw.get())?;
                work.push(W::FinishType {
                    raw_type: wire.raw_type,
                    arguments: wire.arguments.len(),
                });
                work.extend(wire.arguments.into_iter().rev().map(W::Argument));
            }
            W::Argument(argument) => match argument.kind.as_str() {
                "LONG" => parts.push(P::Argument(TypeArgument::Long(serde_json::from_str(
                    argument.value.get(),
                )?))),
                "TYPE" => {
                    work.push(W::FinishArgument);
                    work.push(W::Type(argument.value));
                }
                "NAMED_TYPE" => {
                    let wire: NamedWire<'_> = serde_json::from_str(argument.value.get())?;
                    work.push(W::FinishNamed(wire.field_name));
                    work.push(W::Type(wire.type_signature));
                }
                other => {
                    return Err(serde::de::Error::unknown_variant(
                        other,
                        &["LONG", "TYPE", "NAMED_TYPE"],
                    ));
                }
            },
            W::FinishType {
                raw_type,
                arguments,
            } => {
                let start = parts
                    .len()
                    .checked_sub(arguments)
                    .expect("postorder arguments");
                let arguments = parts
                    .drain(start..)
                    .map(|part| match part {
                        P::Argument(argument) => argument,
                        P::Type(_) => unreachable!("postorder argument"),
                    })
                    .collect();
                parts.push(P::Type(TypeSignature {
                    raw_type,
                    arguments,
                }));
            }
            W::FinishArgument | W::FinishNamed(_) => {
                let P::Type(signature) = parts.pop().expect("postorder child") else {
                    unreachable!("postorder child type");
                };
                let argument = match next {
                    W::FinishNamed(field_name) => TypeArgument::NamedType(NamedTypeSignature {
                        field_name,
                        type_signature: signature,
                    }),
                    _ => TypeArgument::Type(signature),
                };
                parts.push(P::Argument(argument));
            }
        }
    }
    Ok(parts.pop().expect("postorder root"))
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value")]
pub enum TypeArgument {
    #[serde(rename = "LONG")]
    Long(u64),
    #[serde(rename = "TYPE")]
    Type(TypeSignature),
    #[serde(rename = "NAMED_TYPE")]
    NamedType(NamedTypeSignature),
}
impl<'de> Deserialize<'de> for TypeArgument {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        let wire = serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        let part = decode_signature_part(SignatureWork::Argument(wire))
            .map_err(serde::de::Error::custom)?;
        let SignaturePart::Argument(argument) = part else {
            unreachable!("postorder root argument");
        };
        Ok(argument)
    }
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct NamedTypeSignature {
    pub field_name: Option<RowFieldName>,
    pub type_signature: TypeSignature,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RowFieldName {
    pub name: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub type_signature: TypeSignature,
}
#[derive(Clone, Debug, PartialEq)]
pub struct StatementResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<TrinoValue>>,
    pub query_id: String,
    pub pages: usize,
    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}
impl StatementResult {
    /// JDBC completion classification follows updateType, including commands
    /// with the static rows bigint column. Column names never classify results.
    pub fn is_completion(&self) -> bool {
        self.update_type.is_some()
    }
    pub fn is_result_set(&self) -> bool {
        !self.is_completion() && self.has_columns()
    }
    pub fn has_columns(&self) -> bool {
        !self.columns.is_empty()
    }
}
impl TypeSignature {
    pub fn simple(raw_type: impl Into<String>) -> Self {
        Self {
            raw_type: raw_type.into(),
            arguments: Vec::new(),
        }
    }
    pub fn validate(&self) -> Result<(), StatementFailure> {
        use TypeArgument as A;
        let mut work = vec![self];
        while let Some(signature) = work.pop() {
            let supported = match signature.raw_type.as_str() {
                "unknown" | "boolean" | "tinyint" | "smallint" | "integer" | "bigint" | "real"
                | "double" | "varbinary" | "date" | "json" | "variant" => {
                    signature.arguments.is_empty()
                }
                "varchar" => matches!(signature.arguments.as_slice(), [] | [A::Long(_)]),
                "char" => matches!(signature.arguments.as_slice(), [A::Long(_)]),
                "decimal" => {
                    matches!(signature.arguments.as_slice(), [A::Long(p), A::Long(s)] if (1..=38).contains(p) && s <= p)
                }
                "time" | "time with time zone" | "timestamp" | "timestamp with time zone" => {
                    matches!(signature.arguments.as_slice(), [] | [A::Long(0..=12)])
                }
                "array" => matches!(signature.arguments.as_slice(), [A::Type(_)]),
                "map" => matches!(signature.arguments.as_slice(), [A::Type(_), A::Type(_)]),
                "row" => signature
                    .arguments
                    .iter()
                    .all(|a| matches!(a, A::NamedType(_))),
                _ => false,
            };
            if !supported {
                return Err(StatementFailure::protocol(format!(
                    "unsupported Trino type signature: {signature:?}"
                )));
            }
            for arg in signature.arguments.iter().rev() {
                match arg {
                    A::Type(child) => work.push(child),
                    A::NamedType(child) => work.push(&child.type_signature),
                    A::Long(_) => {}
                }
            }
        }
        Ok(())
    }
    pub fn type_argument(&self, index: usize) -> Result<&TypeSignature, StatementFailure> {
        match self.arguments.get(index) {
            Some(TypeArgument::Type(child)) => Ok(child),
            _ => Err(StatementFailure::protocol("missing Trino type argument")),
        }
    }
}
