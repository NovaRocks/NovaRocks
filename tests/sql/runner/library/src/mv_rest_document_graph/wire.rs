// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Independent test decoder for the persisted contract. Do not import a product
//! document codec here: the REST assertions must detect a matching writer/reader
//! bug. MV owns this correctness oracle; native suites consume it serially on
//! their private fixture, and local unit tests use manually specified vectors.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const PREFIX: &str = "novarocks.doc.";
const V2: &str = "novarocks.doc.v2.";
pub(super) const RETIREMENT_KEY: &str = "novarocks.doc.v2.record.novarocks.mv~retirement";
pub(super) const PUBLICATION_KEY: &str = "novarocks.doc.v2.record.novarocks.mv~publication";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Format {
    pub owner: String,
    pub name: String,
    pub version: u32,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    rel: String,
    id: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct VersionEnvelope {
    v: u8,
    owner: String,
    name: String,
    format: Format,
    refs: Vec<Reference>,
    content: String,
    id: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SlotEnvelope {
    v: u8,
    owner: String,
    name: String,
    format: Format,
    writer: Option<i64>,
    content: String,
    digest: String,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MarkerEnvelope {
    v: u8,
    owner: String,
    name: String,
    binding: String,
    format: Format,
    content: String,
    digest: String,
}

#[derive(Clone, Debug)]
pub(super) struct Document {
    pub owner: String,
    pub name: String,
    pub format: Format,
    pub refs: BTreeMap<String, String>,
    pub content: Vec<u8>,
}
#[derive(Debug)]
pub(super) struct Slot {
    pub format: Format,
    pub writer: Option<i64>,
    pub content: Vec<u8>,
}
#[derive(Debug, Default)]
pub(super) struct Properties {
    markers: Vec<MarkerEnvelope>,
    pub versions: BTreeMap<String, Document>,
    pub slots: BTreeMap<String, Slot>,
    pub roots: BTreeMap<String, String>,
    pub private_bytes: usize,
}
#[derive(Debug)]
pub(super) struct Record {
    pub binding: Option<String>,
    pub payload: Vec<u8>,
}

fn token(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len() <= 64,
        "invalid document token length"
    );
    ensure!(
        value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b)),
        "invalid document token {value:?}"
    );
    Ok(())
}
pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
pub(super) fn decode_identity(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid document identity"
    );
    (0..64)
        .step_by(2)
        .map(|n| u8::from_str_radix(&value[n..n + 2], 16).context("decode digest"))
        .collect()
}
fn lp32(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
}
fn header(owner: &str, name: &str, format: &Format) -> Result<Vec<u8>> {
    for v in [owner, name, &format.owner, &format.name] {
        token(v)?;
    }
    ensure!(format.version > 0, "zero document format version");
    let mut result = Vec::new();
    for v in [owner, name, &format.owner, &format.name] {
        lp32(&mut result, v.as_bytes());
    }
    result.extend_from_slice(&format.version.to_be_bytes());
    Ok(result)
}
fn content_hash(domain: &[u8], header: &[u8], middle: &[u8], content: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(header);
    hash.update(middle);
    hash.update((content.len() as u64).to_be_bytes());
    hash.update(content);
    hex(&hash.finalize())
}
fn canonical<T: Serialize>(value: &T, raw: &str) -> Result<()> {
    ensure!(
        serde_json::to_string(value)? == raw,
        "noncanonical document JSON"
    );
    Ok(())
}
pub(super) fn base64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let bits = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..(chunk.len() + 1) {
            out.push(ALPHABET[((bits >> (18 - i * 6)) & 63) as usize] as char);
        }
    }
    out
}
pub(super) fn decode_base64url(value: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut bits = 0u32;
    let mut remaining = 0;
    for b in value.bytes() {
        let n = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => bail!("invalid base64url byte"),
        };
        bits = (bits << 6) | u32::from(n);
        remaining += 6;
        if remaining >= 8 {
            remaining -= 8;
            out.push((bits >> remaining) as u8);
        }
    }
    ensure!(base64url(&out) == value, "noncanonical base64url");
    Ok(out)
}
pub(super) fn legacy(key: &str) -> bool {
    matches!(
        key,
        "novarocks.documents.v1"
            | "novarocks.internal.pending-documents.v1"
            | "novarocks.documents.preparation-digest.v1"
            | "novarocks.documents.update.operation-id.v1"
    ) || key.starts_with("novarocks.managed.")
}
pub(super) fn properties(value: &Value) -> Result<Properties> {
    let values = value
        .as_object()
        .context("REST properties must be an object")?;
    let mut parsed = Properties::default();
    for (key, raw) in values {
        ensure!(!legacy(key), "LegacyFormat property {key}");
        if !key.starts_with(PREFIX) {
            continue;
        }
        let raw = raw
            .as_str()
            .with_context(|| format!("non-string property {key}"))?;
        parsed.private_bytes += key.len() + raw.len();
        let (kind, suffix) = key
            .strip_prefix(V2)
            .with_context(|| format!("unknown document key version {key}"))?
            .split_once('.')
            .context("document key has no category")?;
        match kind {
            "rev" => {
                let v: VersionEnvelope = serde_json::from_str(raw)?;
                canonical(&v, raw)?;
                ensure!(v.v == 2, "unknown version envelope");
                let content = decode_base64url(&v.content)?;
                let head = header(&v.owner, &v.name, &v.format)?;
                let mut refs = BTreeMap::new();
                let mut middle = (v.refs.len() as u32).to_be_bytes().to_vec();
                let mut previous: Option<&str> = None;
                for reference in &v.refs {
                    token(&reference.rel)?;
                    ensure!(
                        previous.is_none_or(|p| p < reference.rel.as_str()),
                        "unordered or repeated document relationship"
                    );
                    previous = Some(&reference.rel);
                    lp32(&mut middle, reference.rel.as_bytes());
                    middle.extend_from_slice(&decode_identity(&reference.id)?);
                    refs.insert(reference.rel.clone(), reference.id.clone());
                }
                let expected = content_hash(
                    b"novarocks-document-identity/v2\0",
                    &head,
                    &middle,
                    &content,
                );
                ensure!(
                    v.id == expected && suffix == expected,
                    "version identity mismatch for {key}"
                );
                eprintln!(
                    "MV document version bytes: key={key} envelope={} content={}",
                    raw.len(),
                    content.len()
                );
                parsed.versions.insert(
                    expected,
                    Document {
                        owner: v.owner,
                        name: v.name,
                        format: v.format,
                        refs,
                        content,
                    },
                );
            }
            "slot" => {
                let v: SlotEnvelope = serde_json::from_str(raw)?;
                canonical(&v, raw)?;
                ensure!(
                    v.v == 2 && suffix == format!("{}~{}", v.owner, v.name),
                    "slot key/value mismatch"
                );
                let head = header(&v.owner, &v.name, &v.format)?;
                let content = decode_base64url(&v.content)?;
                let mut writer = vec![u8::from(v.writer.is_some())];
                if let Some(id) = v.writer {
                    writer.extend_from_slice(&id.to_be_bytes());
                }
                ensure!(
                    v.digest
                        == content_hash(b"novarocks-document-slot/v2\0", &head, &writer, &content),
                    "slot integrity mismatch"
                );
                parsed.slots.insert(
                    suffix.into(),
                    Slot {
                        format: v.format,
                        writer: v.writer,
                        content,
                    },
                );
            }
            "mark" => {
                let v: MarkerEnvelope = serde_json::from_str(raw)?;
                canonical(&v, raw)?;
                token(&v.binding)?;
                ensure!(
                    v.v == 2 && suffix == format!("{}~{}~{}", v.owner, v.name, v.binding),
                    "marker key/value mismatch"
                );
                let mut middle = Vec::new();
                lp32(&mut middle, v.binding.as_bytes());
                ensure!(
                    v.digest
                        == content_hash(
                            b"novarocks-document-marker/v2\0",
                            &header(&v.owner, &v.name, &v.format)?,
                            &middle,
                            &decode_base64url(&v.content)?
                        ),
                    "marker integrity mismatch"
                );
                parsed.markers.push(v);
            }
            "root" => {
                token(suffix)?;
                decode_identity(raw)?;
                parsed.roots.insert(suffix.into(), raw.into());
            }
            _ => bail!("unknown or misplaced document key {key}"),
        }
    }
    eprintln!(
        "MV document private property bytes: {}",
        parsed.private_bytes
    );
    Ok(parsed)
}
pub(super) fn records(summary: &Value) -> Result<BTreeMap<String, String>> {
    let values = summary
        .as_object()
        .context("snapshot summary must be an object")?;
    let mut records = BTreeMap::new();
    for (key, value) in values {
        ensure!(!legacy(key), "LegacyFormat snapshot key {key}");
        if !key.starts_with(PREFIX) {
            continue;
        }
        let suffix = key
            .strip_prefix("novarocks.doc.v2.record.")
            .with_context(|| format!("unknown or misplaced snapshot key {key}"))?;
        let (owner, name) = suffix.split_once('~').context("invalid record key")?;
        token(owner)?;
        token(name)?;
        let text = value.as_str().context("record must be string")?;
        let parsed = record(text)?;
        eprintln!(
            "MV document record bytes: key={key} frame={} payload={} key-and-frame={}",
            text.len(),
            parsed.payload.len(),
            key.len() + text.len()
        );
        records.insert(key.clone(), text.into());
    }
    Ok(records)
}
pub(super) fn record(text: &str) -> Result<Record> {
    let fields: Vec<_> = text.split('.').collect();
    ensure!(
        fields.len() == 3 && fields[0] == "2",
        "invalid snapshot record frame"
    );
    let binding = if fields[1] == "-" {
        None
    } else {
        decode_identity(fields[1])?;
        Some(fields[1].into())
    };
    Ok(Record {
        binding,
        payload: decode_base64url(fields[2])?,
    })
}
pub(super) fn closure(properties: &Properties, root: &str) -> Result<BTreeSet<String>> {
    let mut seen = BTreeSet::new();
    let mut pending = vec![root.to_string()];
    while let Some(id) = pending.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        let version = properties.versions.get(&id).with_context(|| {
            format!("record/root references absent current property {V2}rev.{id}")
        })?;
        pending.extend(version.refs.values().cloned());
    }
    Ok(seen)
}

struct Cursor<'a> {
    raw: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, position: 0 }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .context("payload length overflow")?;
        let value = self
            .raw
            .get(self.position..end)
            .context("truncated payload")?;
        self.position = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into()?))
    }
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for i in 0..10 {
            let b = self.byte()?;
            ensure!(i != 9 || b <= 1, "varint overflow");
            value |= u64::from(b & 127) << (7 * i);
            if b < 128 {
                ensure!(i == 0 || b != 0, "noncanonical varint");
                return Ok(value);
            }
        }
        bail!("varint too long")
    }
    fn done(&self) -> Result<()> {
        ensure!(self.position == self.raw.len(), "trailing payload bytes");
        Ok(())
    }
}
#[derive(Debug)]
enum Field<'a> {
    Int(u64),
    Bytes(&'a [u8]),
}
impl<'a> Field<'a> {
    fn int(&self) -> Result<u64> {
        match self {
            Self::Int(v) => Ok(*v),
            _ => bail!("expected protobuf integer"),
        }
    }
    fn bytes(&self) -> Result<&'a [u8]> {
        match self {
            Self::Bytes(v) => Ok(v),
            _ => bail!("expected protobuf bytes"),
        }
    }
}
fn proto<'a>(
    raw: &'a [u8],
    wire_types: &[(u32, u8)],
    repeated: &[u32],
) -> Result<Vec<(u32, Field<'a>)>> {
    let mut c = Cursor::new(raw);
    let mut fields = Vec::new();
    let mut previous = 0;
    let mut seen = BTreeSet::new();
    while c.position < raw.len() {
        let key = c.varint()?;
        let number = u32::try_from(key >> 3)?;
        let wire = (key & 7) as u8;
        ensure!(
            wire_types.contains(&(number, wire)),
            "unknown protobuf field or wire type"
        );
        ensure!(
            number >= previous && (seen.insert(number) || repeated.contains(&number)),
            "duplicate or unordered protobuf field"
        );
        previous = number;
        let value = match wire {
            0 => Field::Int(c.varint()?),
            2 => {
                let n = usize::try_from(c.varint()?)?;
                Field::Bytes(c.take(n)?)
            }
            _ => bail!("unsupported protobuf wire type"),
        };
        fields.push((number, value));
    }
    Ok(fields)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DataVersionFormat {
    pub provider: String,
    pub name: String,
    pub version: u16,
    pub maximum: u16,
}
pub(super) struct Binding<'a> {
    pub formats: Vec<DataVersionFormat>,
    pub occurrences: Vec<Occurrence<'a>>,
    pub outputs: Vec<BoundOutput<'a>>,
}

pub(super) struct Occurrence<'a> {
    pub namespace: &'a str,
    pub table: &'a str,
    pub object: &'a [u8],
}

pub(super) struct BoundOutput<'a> {
    pub name: &'a str,
    pub logical_type: &'a [u8],
}

fn version(fields: &[(u32, Field<'_>)], expected: u64, document: &str) -> Result<()> {
    ensure!(
        fields
            .first()
            .is_some_and(|(n, v)| *n == 1 && v.int().ok() == Some(expected)),
        "{document} format must be {expected}"
    );
    Ok(())
}

fn required<'a>(fields: &[(u32, Field<'a>)], number: u32) -> Result<&'a [u8]> {
    fields
        .iter()
        .find(|(n, _)| *n == number)
        .with_context(|| format!("missing protobuf field {number}"))?
        .1
        .bytes()
}

pub(super) fn binding(raw: &[u8]) -> Result<Binding<'_>> {
    let fields = proto(raw, &[(1, 0), (3, 2), (4, 2), (5, 2), (6, 2)], &[3, 4, 5])?;
    version(&fields, 2, "binding")?;
    let mut formats = Vec::new();
    let mut occurrence_ids = BTreeSet::new();
    let mut occurrences = Vec::new();
    let mut outputs = Vec::new();
    for (number, value) in &fields[1..] {
        match number {
            3 => {
                let occurrence = proto(
                    value.bytes()?,
                    &[
                        (1, 0),
                        (2, 2),
                        (3, 2),
                        (4, 2),
                        (5, 2),
                        (6, 2),
                        (7, 0),
                        (8, 2),
                        (9, 2),
                        (10, 2),
                        (11, 2),
                        (12, 2),
                        (13, 2),
                        (14, 2),
                    ],
                    &[13],
                )?;
                let id = occurrence
                    .first()
                    .filter(|(n, _)| *n == 1)
                    .context("binding occurrence lacks ID")?
                    .1
                    .int()?;
                ensure!(
                    id <= u64::from(u32::MAX) && occurrence_ids.insert(id),
                    "invalid or duplicate binding occurrence ID"
                );
                for n in [2, 3, 4, 5] {
                    ensure!(
                        !std::str::from_utf8(required(&occurrence, n)?)?.is_empty(),
                        "empty binding occurrence locator"
                    );
                }
                for n in [6, 9, 10, 11, 12, 14] {
                    ensure!(
                        !required(&occurrence, n)?.is_empty(),
                        "empty binding occurrence fact"
                    );
                }
                ensure!(
                    occurrence
                        .iter()
                        .any(|(n, value)| *n == 7 && value.int().ok() == Some(1)),
                    "binding occurrence must be a current read"
                );
                formats.push(data_version_format(required(&occurrence, 8)?)?);
                occurrences.push(Occurrence {
                    namespace: std::str::from_utf8(required(&occurrence, 3)?)?,
                    table: std::str::from_utf8(required(&occurrence, 4)?)?,
                    object: required(&occurrence, 6)?,
                });
            }
            4 => {
                let output = proto(value.bytes()?, &[(1, 0), (2, 2), (3, 2), (4, 2)], &[4])?;
                ensure!(
                    output.first().is_some_and(|(n, v)| *n == 1 && v.int().ok() == Some(outputs.len() as u64)),
                    "binding output ordinal is missing or out of order"
                );
                let name = std::str::from_utf8(required(&output, 2)?)?;
                let logical_type = required(&output, 3)?;
                ensure!(
                    !name.is_empty() && !logical_type.is_empty(),
                    "empty binding output fact"
                );
                outputs.push(BoundOutput { name, logical_type });
            }
            _ => {}
        }
    }
    let digests = proto(required(&fields, 6)?, &[(1, 2), (2, 2), (3, 2)], &[])?;
    for n in 1..=3 {
        ensure!(
            required(&digests, n)?.len() == 32,
            "invalid binding fact digest"
        );
    }
    ensure!(
        (1..=usize::from(u16::MAX)).contains(&formats.len()),
        "invalid occurrence count"
    );
    ensure!(!outputs.is_empty(), "binding lacks outputs");
    Ok(Binding {
        formats,
        occurrences,
        outputs,
    })
}

fn data_version_format(raw: &[u8]) -> Result<DataVersionFormat> {
    let descriptor = proto(raw, &[(1, 2), (2, 2), (3, 0), (4, 0)], &[])?;
    ensure!(
        descriptor.len() == 4,
        "binding descriptor missing required field"
    );
    let provider = std::str::from_utf8(descriptor[0].1.bytes()?)?.to_string();
    token(&provider)?;
    let name = std::str::from_utf8(descriptor[1].1.bytes()?)?.to_string();
    ensure!(
        !name.is_empty() && name.len() <= 128 && name.is_ascii(),
        "invalid data-version format name"
    );
    let version = u16::try_from(descriptor[2].1.int()?)?;
    ensure!(version > 0, "zero data-version format version");
    let maximum = u16::try_from(descriptor[3].1.int()?)?;
    ensure!(
        (1..=256).contains(&maximum),
        "invalid declared data-version bound"
    );
    Ok(DataVersionFormat {
        provider,
        name,
        version,
        maximum,
    })
}

pub(super) fn definition(raw: &[u8]) -> Result<()> {
    let fields = proto(raw, &[(1, 0), (2, 2), (6, 0)], &[])?;
    version(&fields, 2, "definition")?;
    ensure!(fields.len() == 3, "definition fields are incomplete");
    let query = proto(required(&fields, 2)?, &[(1, 2), (2, 0), (3, 2)], &[])?;
    let sql = std::str::from_utf8(required(&query, 1)?)?;
    ensure!(!sql.is_empty(), "definition lacks SQL");
    ensure!(
        query
            .get(1)
            .is_some_and(|(n, v)| *n == 2 && v.int().ok() == Some(2)),
        "definition dialect must be Trino"
    );
    let context = proto(required(&query, 3)?, &[(1, 2), (2, 2)], &[])?;
    for n in 1..=2 {
        ensure!(
            !std::str::from_utf8(required(&context, n)?)?.is_empty(),
            "definition lacks resolution context"
        );
    }
    Ok(())
}

pub(super) struct MappingColumn<'a> {
    pub storage_field: &'a [u8],
    pub storage_type: &'a [u8],
    pub storage_nullable: bool,
    pub declaration: &'a [u8],
    pub encode_identity: &'a [u8],
    pub decode_identity: &'a [u8],
    pub content_contract: &'a [u8],
    pub storage_format: u64,
}

pub(super) fn mapping<'a>(
    raw: &'a [u8],
    outputs: &[BoundOutput<'_>],
) -> Result<Vec<MappingColumn<'a>>> {
    let fields = proto(raw, &[(1, 0), (2, 2)], &[2])?;
    version(&fields, 1, "mapping")?;
    ensure!(
        fields.len() == outputs.len() + 1,
        "M/B output count differs"
    );
    let mut field_ids = BTreeSet::new();
    let mut columns = Vec::new();
    for (ordinal, ((_, value), output)) in fields[1..].iter().zip(outputs).enumerate() {
        let column = proto(
            value.bytes()?,
            &[
                (1, 0),
                (2, 2),
                (3, 2),
                (4, 2),
                (5, 2),
                (6, 2),
                (7, 2),
                (8, 0),
                (9, 0),
                (10, 2),
                (11, 0),
            ],
            &[],
        )?;
        ensure!(column.len() == 11, "M column fields are incomplete");
        ensure!(
            column[0].1.int()? == ordinal as u64,
            "M output ordinal is out of order"
        );
        for n in [2, 3, 4, 5, 6, 7, 10] {
            ensure!(
                !required(&column, n)?.is_empty(),
                "M column identity is empty"
            );
        }
        ensure!(
            required(&column, 3)? == output.logical_type,
            "M disagrees with B logical type for {}",
            output.name
        );
        let storage_field = required(&column, 2)?;
        ensure!(
            field_ids.insert(storage_field),
            "M repeats a physical field"
        );
        ensure!(
            (1..=3).contains(&column[7].1.int()?),
            "unknown M pushdown class"
        );
        let nullable = column[8].1.int()?;
        ensure!(nullable <= 1, "invalid M nullability");
        let storage_format = column[10].1.int()?;
        ensure!(storage_format > 0, "missing M storage format version");
        columns.push(MappingColumn {
            storage_field,
            storage_type: required(&column, 4)?,
            storage_nullable: nullable == 1,
            declaration: required(&column, 10)?,
            encode_identity: required(&column, 5)?,
            decode_identity: required(&column, 6)?,
            content_contract: required(&column, 7)?,
            storage_format,
        });
    }
    Ok(columns)
}
pub(super) fn interpretation_definition(raw: &[u8]) -> Result<String> {
    let fields = proto(
        raw,
        &[
            (1, 0),
            (2, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
            (8, 2),
            (9, 2),
        ],
        &[4, 5, 7, 8],
    )?;
    version(&fields, 3, "interpretation")?;
    let revision = fields
        .iter()
        .find(|(n, _)| *n == 2)
        .context("interpretation lacks definition revision")?
        .1
        .bytes()?;
    ensure!(
        revision.len() == 32,
        "interpretation definition revision is not identity"
    );
    Ok(hex(revision))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MarkerScope {
    Publication,
    Binding,
}

pub(super) struct Retirement {
    pub retired: [u8; 16],
    pub operation: [u8; 16],
    pub revision: [u8; 32],
}

pub(super) fn retirement(frame: &str) -> Result<Retirement> {
    let record = record(frame)?;
    ensure!(record.binding.is_none(), "retirement record binds a B");
    let fields = proto(
        &record.payload,
        &[(1, 0), (2, 2), (3, 2), (4, 0), (5, 0), (6, 0), (7, 2)],
        &[],
    )?;
    version(&fields, 1, "retirement")?;
    for tag in 4..=6 {
        let field = fields
            .iter()
            .find(|(n, _)| *n == tag)
            .context("missing retirement count")?;
        ensure!(
            i64::try_from(field.1.int()?).is_ok(),
            "retirement count exceeds Int64"
        );
    }
    if let Some((_, fingerprint)) = fields.iter().find(|(n, _)| *n == 7) {
        ensure!(
            fingerprint.bytes()?.len() == 16,
            "invalid retirement fingerprint"
        );
    }
    Ok(Retirement {
        retired: required(&fields, 2)?
            .try_into()
            .context("invalid retired publication")?,
        operation: required(&fields, 3)?
            .try_into()
            .context("invalid retirement operation")?,
        revision: Sha256::digest(frame.as_bytes()).into(),
    })
}

#[derive(Debug)]
pub(super) struct Marker {
    pub name: String,
    pub scope: MarkerScope,
    pub identity: Vec<u8>,
    pub retirement_revision: Option<[u8; 32]>,
}

fn frame64(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

pub(super) fn markers(properties: &Properties) -> Result<Vec<Marker>> {
    properties
        .markers
        .iter()
        .filter(|item| item.owner == "novarocks.mv")
        .map(|item| {
            let raw = decode_base64url(&item.content)?;
            let fields = proto(
                &raw,
                &[
                    (1, 0),
                    (2, 2),
                    (3, 2),
                    (4, 2),
                    (5, 2),
                    (6, 2),
                    (7, 2),
                    (8, 2),
                ],
                &[],
            )?;
            version(&fields, 1, "marker")?;
            ensure!(fields.len() == 3, "marker must carry a scope and one body");
            let identity = fields[1].1.bytes()?;
            let (scope, tag) = match fields[1].0 {
                2 if identity.len() == 16 => (MarkerScope::Publication, 1u32),
                3 if identity.len() == 32 => (MarkerScope::Binding, 2u32),
                _ => bail!("invalid MV marker scope"),
            };
            let body = fields[2].1.bytes()?;
            let mut attempt = None;
            let mut retirement_revision = None;
            let name = match fields[2].0 {
                4 | 5 => {
                    ensure!(
                        scope == MarkerScope::Publication,
                        "attempt marker must scope to P"
                    );
                    let shape = if fields[2].0 == 4 {
                        &[(1, 2), (2, 2), (3, 2), (4, 2), (5, 0), (6, 2), (7, 2)][..]
                    } else {
                        &[(1, 2)]
                    };
                    let attempt_fields = proto(body, shape, &[3])?;
                    let id = required(&attempt_fields, 1)?;
                    ensure!(id.len() == 16, "invalid marker attempt identity");
                    attempt = Some(id);
                    if fields[2].0 == 4 {
                        "pending"
                    } else {
                        "pending-released"
                    }
                }
                6 => {
                    let changes = proto(body, &[(1, 2)], &[1])?;
                    ensure!(
                        !changes.is_empty(),
                        "needs-rebuild marker lost its change evidence"
                    );
                    let mut prior = 0;
                    for (_, change) in changes {
                        let fields = proto(change.bytes()?, &[(1, 0), (2, 2), (3, 2)], &[])?;
                        ensure!(fields.len() == 3, "incomplete binding change evidence");
                        let kind = fields[0].1.int()?;
                        ensure!(
                            kind > prior && kind <= 5,
                            "invalid rebuild change kind order"
                        );
                        prior = kind;
                        ensure!(
                            required(&fields, 2)?.len() == 32 && required(&fields, 3)?.len() == 32,
                            "rebuild marker revision is not an exact identity"
                        );
                    }
                    "needs-rebuild"
                }
                7 => {
                    let invalid = proto(body, &[(1, 0), (2, 2)], &[])?;
                    ensure!(
                        invalid
                            .first()
                            .is_some_and(|(n, v)| *n == 1 && matches!(v.int(), Ok(1 | 2)))
                            && !std::str::from_utf8(required(&invalid, 2)?)?
                                .trim()
                                .is_empty(),
                        "invalid-definition marker lacks its typed diagnostic"
                    );
                    "invalid-definition"
                }
                8 => {
                    let invalid = proto(body, &[(1, 2)], &[])?;
                    ensure!(scope == MarkerScope::Publication, "Invalid must scope to P");
                    retirement_revision = Some(
                        required(&invalid, 1)?
                            .try_into()
                            .context("invalid marker lacks its retirement revision")?,
                    );
                    "invalid"
                }
                _ => bail!("missing MV marker body"),
            };
            ensure!(
                item.name == name
                    && item.format.owner == item.owner
                    && item.format.name == name
                    && item.format.version == 1,
                "MV marker key does not match its typed body"
            );
            let mut binding = Vec::new();
            frame64(&mut binding, b"novarocks.mv.marker-binding");
            binding.extend_from_slice(&1u32.to_be_bytes());
            binding.extend_from_slice(&tag.to_be_bytes());
            frame64(&mut binding, identity);
            if let Some(attempt) = attempt {
                frame64(&mut binding, attempt);
            }
            ensure!(
                hex(&Sha256::digest(binding)) == item.binding,
                "MV marker key does not match its exact scope"
            );
            Ok(Marker {
                name: name.to_string(),
                scope,
                identity: identity.to_vec(),
                retirement_revision,
            })
        })
        .collect()
}

pub(super) fn iceberg_object_identity(raw: &[u8]) -> Result<&[u8]> {
    let mut c = Cursor::new(raw);
    ensure!(c.take(8)? == b"NRMVFCT1", "unknown object fact envelope");
    let len = usize::from(c.u16()?);
    ensure!(c.take(len)? == b"iceberg", "unexpected object provider");
    let len = usize::from(c.u16()?);
    ensure!(
        c.take(len)? == b"connector-table-object-id",
        "unexpected object declaration"
    );
    ensure!(
        c.u16()? == 1 && c.u16()? == 36,
        "expected canonical Iceberg table UUID text"
    );
    let object = c.take(36)?;
    c.done()?;
    Ok(object)
}

pub(super) fn assert_decimal_mapping(
    column: &MappingColumn<'_>,
    output: &BoundOutput<'_>,
    field: &Value,
    format_version: u64,
) -> Result<()> {
    let field_id = i32::try_from(field["id"].as_i64().context("REST field lacks ID")?)?;
    let required = field["required"]
        .as_bool()
        .context("REST field lacks required")?;
    let decimal = field["type"]
        .as_str()
        .and_then(|value| value.strip_prefix("decimal(")?.strip_suffix(')'))
        .context("REST output storage is not decimal")?;
    let (precision, scale) = decimal
        .split_once(',')
        .context("invalid REST decimal type")?;
    let precision = precision.trim().parse::<u32>()?;
    let scale = scale.trim().parse::<u32>()?;
    let mut ty = Vec::new();
    frame64(&mut ty, b"novarocks.iceberg.source-type");
    ty.extend_from_slice(&1u32.to_be_bytes());
    ty.extend_from_slice(&1u64.to_be_bytes());
    for value in [1u32, 6, precision, scale] {
        ty.extend_from_slice(&value.to_be_bytes());
    }
    ensure!(
        column.storage_field == field_id.to_be_bytes()
            && column.storage_type == ty
            && column.storage_nullable != required
            && column.storage_format == format_version,
        "M named a different Iceberg field, decimal storage type, or table format"
    );
    ensure!(
        column.encode_identity == b"novarocks.iceberg.ordinary-value-write/v1"
            && column.decode_identity == b"novarocks.iceberg.ordinary-value-read/v1"
            && column.content_contract == b"novarocks.iceberg.mv.observable-value-content/v1",
        "M does not describe the ordinary observable value path"
    );
    let mut declaration = Vec::new();
    frame64(&mut declaration, b"novarocks.iceberg.mv.storage-column");
    declaration.extend_from_slice(&1u32.to_be_bytes());
    frame64(&mut declaration, &ty);
    declaration.push(u8::from(required));
    let mut logical = Cursor::new(output.logical_type);
    let domain_len = usize::try_from(u64::from_be_bytes(logical.take(8)?.try_into()?))?;
    ensure!(
        logical.take(domain_len)? == b"novarocks.sql.mv-output-type"
            && logical.take(4)? == 1u32.to_be_bytes(),
        "B5 has an unexpected output type format"
    );
    let value_len = usize::try_from(u64::from_be_bytes(logical.take(8)?.try_into()?))?;
    let value_type = logical.take(value_len)?;
    logical.done()?;
    frame64(&mut declaration, value_type);
    frame64(&mut declaration, value_type);
    ensure!(
        column.declaration == declaration,
        "M disagrees with the ordinary decimal declaration"
    );
    Ok(())
}

pub(super) fn management(raw: &[u8]) -> Result<[String; 3]> {
    let mut c = Cursor::new(raw);
    let mut result = Vec::new();
    for _ in 0..3 {
        let n = u32::from_be_bytes(c.take(4)?.try_into()?) as usize;
        let value = std::str::from_utf8(c.take(n)?)?;
        ensure!(
            !value.is_empty() && value.is_ascii(),
            "invalid management string"
        );
        result.push(value.to_string());
    }
    c.done()?;
    ensure!(result[0] == "materialized-view", "wrong managed kind");
    Ok(result.try_into().expect("three management fields"))
}
#[derive(Debug)]
pub(super) struct Publication {
    pub id: [u8; 16],
    pub rows: Option<u64>,
    pub watermarks: Vec<Vec<u8>>,
}
pub(super) fn publication(raw: &[u8], formats: &[DataVersionFormat]) -> Result<Publication> {
    let mut c = Cursor::new(raw);
    ensure!(c.byte()? == 1, "unknown compact publication layout");
    let id = c.take(16)?.try_into()?;
    let frozen = i64::from_be_bytes(c.take(8)?.try_into()?);
    ensure!(frozen >= 0, "negative publication time");
    let rows = match c.byte()? {
        0 => None,
        1 => Some(u64::from_be_bytes(c.take(8)?.try_into()?)),
        _ => bail!("invalid rows presence flag"),
    };
    ensure!(
        usize::from(c.u16()?) == formats.len(),
        "P occurrence count differs from B"
    );
    let mut watermarks = Vec::new();
    for format in formats {
        let n = c.u16()?;
        ensure!(
            n <= format.maximum && n <= 256,
            "publication value exceeds declared maximum"
        );
        watermarks.push(c.take(usize::from(n))?.to_vec());
    }
    c.done()?;
    Ok(Publication {
        id,
        rows,
        watermarks,
    })
}
