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
#[derive(Deserialize, Serialize)]
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
fn unhex(value: &str) -> Result<Vec<u8>> {
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
                    middle.extend_from_slice(&unhex(&reference.id)?);
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
            }
            "root" => {
                token(suffix)?;
                unhex(raw)?;
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
        unhex(fields[1])?;
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
pub(super) fn binding(raw: &[u8]) -> Result<Vec<DataVersionFormat>> {
    let fields = proto(raw, &[(1, 0), (2, 2)], &[2])?;
    ensure!(
        fields
            .first()
            .is_some_and(|(n, v)| *n == 1 && v.int().ok() == Some(1)),
        "binding format must be 1"
    );
    let mut result = Vec::new();
    for (_, value) in fields.iter().skip(1) {
        let descriptor = proto(value.bytes()?, &[(1, 2), (2, 2), (3, 0), (4, 0)], &[])?;
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
        result.push(DataVersionFormat {
            provider,
            name,
            version,
            maximum,
        });
    }
    ensure!(result.len() <= 65535, "too many binding occurrences");
    Ok(result)
}
pub(super) fn definition_occurrences(raw: &[u8]) -> Result<usize> {
    let fields = proto(
        raw,
        &[(1, 0), (2, 2), (3, 2), (4, 2), (5, 2), (6, 0)],
        &[3, 4],
    )?;
    ensure!(
        fields
            .first()
            .is_some_and(|(n, v)| *n == 1 && v.int().ok() == Some(1)),
        "definition format must be 1"
    );
    Ok(fields.iter().filter(|(n, _)| *n == 3).count())
}
pub(super) fn interpretation_definition(raw: &[u8]) -> Result<String> {
    let fields = proto(
        raw,
        &[
            (1, 0),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
            (8, 2),
            (9, 2),
        ],
        &[4, 5, 7, 8],
    )?;
    ensure!(
        fields
            .first()
            .is_some_and(|(n, v)| *n == 1 && v.int().ok() == Some(2)),
        "interpretation format must be 2"
    );
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
        "P occurrence count differs from B/D"
    );
    for format in formats {
        let n = c.u16()?;
        ensure!(
            n <= format.maximum && n <= 256,
            "publication value exceeds declared maximum"
        );
        c.take(usize::from(n))?;
    }
    c.done()?;
    Ok(Publication { id, rows })
}
