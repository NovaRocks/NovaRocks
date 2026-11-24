// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Independent system-test oracle for the frozen keyed wire contract.
//! This module imports no SPI or product document codec.

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(in crate::scenarios) const ROOT: &str = "novarocks.doc.v2.root.novarocks.mv";
pub(in crate::scenarios) const CONFIG: &str = "novarocks.doc.v2.slot.novarocks.mv~configuration";
pub(in crate::scenarios) const MANAGEMENT: &str =
    "novarocks.doc.v2.slot.novarocks.storage~management";
pub(in crate::scenarios) const RECORD: &str = "novarocks.doc.v2.record.novarocks.mv~publication";
const PREFIX: &str = "novarocks.doc.v2.";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::scenarios) struct Format {
    pub owner: String,
    pub name: String,
    pub version: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::scenarios) struct Reference {
    pub rel: String,
    pub id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::scenarios) struct Version {
    pub v: u8,
    pub owner: String,
    pub name: String,
    pub format: Format,
    pub refs: Vec<Reference>,
    pub content: String,
    pub id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::scenarios) struct Slot {
    pub v: u8,
    pub owner: String,
    pub name: String,
    pub format: Format,
    pub writer: Option<i64>,
    pub content: String,
    pub digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::scenarios) struct Marker {
    pub v: u8,
    pub owner: String,
    pub name: String,
    pub binding: String,
    pub format: Format,
    pub content: String,
    pub digest: String,
}
fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
pub(in crate::scenarios) fn unhex(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid 32-byte lowercase identity"
    );
    (0..32)
        .map(|i| u8::from_str_radix(&value[i * 2..i * 2 + 2], 16).map_err(Into::into))
        .collect()
}
fn legacy_key(key: &str) -> bool {
    key.starts_with("novarocks.managed.")
        || matches!(
            key,
            "novarocks.documents.v1"
                | "novarocks.internal.pending-documents.v1"
                | "novarocks.documents.preparation-digest.v1"
                | "novarocks.documents.update.operation-id.v1"
        )
}
fn token(s: &str) -> Result<()> {
    ensure!(
        !s.is_empty()
            && s.len() <= 64
            && (s.as_bytes()[0].is_ascii_lowercase() || s.as_bytes()[0].is_ascii_digit()),
        "invalid document token {s:?}"
    );
    ensure!(
        s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b)),
        "invalid document token {s:?}"
    );
    Ok(())
}
fn lp(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}
fn header(domain: &[u8], owner: &str, name: &str, f: &Format) -> Result<Vec<u8>> {
    for value in [owner, name, &f.owner, &f.name] {
        token(value)?;
    }
    ensure!(f.version > 0, "zero format version");
    let mut out = domain.to_vec();
    for value in [owner, name, &f.owner, &f.name] {
        lp(&mut out, value.as_bytes());
    }
    out.extend_from_slice(&f.version.to_be_bytes());
    Ok(out)
}
fn content(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}
pub(in crate::scenarios) fn decode64(value: &str) -> Result<Vec<u8>> {
    let bytes = URL_SAFE_NO_PAD.decode(value)?;
    ensure!(
        URL_SAFE_NO_PAD.encode(&bytes) == value,
        "noncanonical base64url"
    );
    Ok(bytes)
}
fn canonical<T: Serialize + for<'de> Deserialize<'de>>(text: &str) -> Result<T> {
    let value = serde_json::from_str::<T>(text)?;
    ensure!(
        serde_json::to_string(&value)? == text,
        "noncanonical envelope"
    );
    Ok(value)
}
impl Version {
    pub fn new(
        owner: &str,
        name: &str,
        format: Format,
        mut refs: Vec<Reference>,
        bytes: &[u8],
    ) -> Result<Self> {
        refs.sort_by(|a, b| a.rel.cmp(&b.rel));
        let mut v = Self {
            v: 2,
            owner: owner.into(),
            name: name.into(),
            format,
            refs,
            content: URL_SAFE_NO_PAD.encode(bytes),
            id: String::new(),
        };
        v.id = v.identity()?;
        Ok(v)
    }
    pub fn key(&self) -> String {
        format!("{PREFIX}rev.{}", self.id)
    }
    pub fn bytes(&self) -> Result<Vec<u8>> {
        decode64(&self.content)
    }
    pub fn identity(&self) -> Result<String> {
        let mut out = header(
            b"novarocks-document-identity/v2\0",
            &self.owner,
            &self.name,
            &self.format,
        )?;
        out.extend_from_slice(&(self.refs.len() as u32).to_be_bytes());
        let mut previous = None;
        for r in &self.refs {
            token(&r.rel)?;
            ensure!(
                previous.is_none_or(|p: &str| p < r.rel.as_str()),
                "duplicate or unsorted reference"
            );
            previous = Some(&r.rel);
            lp(&mut out, r.rel.as_bytes());
            out.extend_from_slice(&unhex(&r.id)?);
        }
        content(&mut out, &self.bytes()?);
        Ok(hex(Sha256::digest(out)))
    }
    pub fn encoded(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}
impl Slot {
    fn digest(&self) -> Result<String> {
        let mut out = header(
            b"novarocks-document-slot/v2\0",
            &self.owner,
            &self.name,
            &self.format,
        )?;
        out.push(u8::from(self.writer.is_some()));
        if let Some(w) = self.writer {
            out.extend_from_slice(&w.to_be_bytes());
        }
        content(&mut out, &decode64(&self.content)?);
        Ok(hex(Sha256::digest(out)))
    }
    pub fn encoded(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
    pub fn replace_content(&mut self, bytes: &[u8]) -> Result<()> {
        self.content = URL_SAFE_NO_PAD.encode(bytes);
        self.digest = self.digest()?;
        Ok(())
    }
    pub fn content_revision(&self) -> Result<Vec<u8>> {
        Ok(Sha256::digest(decode64(&self.content)?).to_vec())
    }
}
impl Marker {
    pub fn late() -> Result<Self> {
        let mut v = Self {
            v: 2,
            owner: "novarocks.test".into(),
            name: "late".into(),
            binding: "third-party".into(),
            format: Format {
                owner: "novarocks.test".into(),
                name: "late".into(),
                version: 1,
            },
            content: URL_SAFE_NO_PAD.encode(b"late marker"),
            digest: String::new(),
        };
        v.digest = v.checksum()?;
        Ok(v)
    }
    pub fn key(&self) -> String {
        format!("{PREFIX}mark.{}~{}~{}", self.owner, self.name, self.binding)
    }
    fn checksum(&self) -> Result<String> {
        token(&self.binding)?;
        let mut out = header(
            b"novarocks-document-marker/v2\0",
            &self.owner,
            &self.name,
            &self.format,
        )?;
        lp(&mut out, self.binding.as_bytes());
        content(&mut out, &decode64(&self.content)?);
        Ok(hex(Sha256::digest(out)))
    }
    pub fn encoded(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}
pub(in crate::scenarios) fn management_content(
    kind: &str,
    owner: &str,
    incarnation: &str,
) -> Vec<u8> {
    let mut out = Vec::new();
    for x in [kind, owner, incarnation] {
        lp(&mut out, x.as_bytes());
    }
    out
}
pub(in crate::scenarios) fn management(slot: &Slot) -> Result<Vec<String>> {
    let mut bytes = decode64(&slot.content)?;
    let mut fields = Vec::new();
    for _ in 0..3 {
        ensure!(bytes.len() >= 4, "truncated management slot");
        let len = u32::from_be_bytes(bytes[..4].try_into()?) as usize;
        bytes.drain(..4);
        ensure!(bytes.len() >= len, "truncated management value");
        fields.push(String::from_utf8(bytes.drain(..len).collect())?);
    }
    ensure!(
        bytes.is_empty() && fields[0] == "materialized-view",
        "invalid management slot"
    );
    Ok(fields)
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::scenarios) struct Record {
    pub binding: Option<String>,
    pub payload: Vec<u8>,
    pub frame: String,
}
pub(in crate::scenarios) fn record(frame: &str) -> Result<Record> {
    let parts: Vec<_> = frame.split('.').collect();
    let ["2", binding, payload] = parts.as_slice() else {
        bail!("invalid record frame")
    };
    let binding = if *binding == "-" {
        None
    } else {
        unhex(binding)?;
        Some((*binding).into())
    };
    Ok(Record {
        binding,
        payload: decode64(payload)?,
        frame: frame.into(),
    })
}
pub(in crate::scenarios) fn record_frame(binding: &str, payload: &[u8]) -> String {
    format!("2.{binding}.{}", URL_SAFE_NO_PAD.encode(payload))
}
#[derive(Clone, Debug)]
pub(in crate::scenarios) struct Graph {
    pub properties: BTreeMap<String, String>,
    pub versions: BTreeMap<String, Version>,
    pub slots: BTreeMap<String, Slot>,
    pub roots: BTreeMap<String, String>,
    pub snapshots: Vec<(i64, Option<Record>)>,
    pub current: Option<i64>,
    pub diagnostics: Vec<String>,
    pub private_bytes: usize,
    pub record_bytes: Vec<usize>,
    pub definition: String,
    pub interpretation: String,
    pub mapping: String,
    pub binding: String,
}
impl Graph {
    pub fn from_metadata(metadata: &Value) -> Result<Self> {
        let mut graph = Self {
            properties: BTreeMap::new(),
            versions: BTreeMap::new(),
            slots: BTreeMap::new(),
            roots: BTreeMap::new(),
            snapshots: Vec::new(),
            current: metadata["current-snapshot-id"]
                .as_i64()
                .filter(|i| *i != -1),
            diagnostics: Vec::new(),
            private_bytes: 0,
            record_bytes: Vec::new(),
            definition: String::new(),
            interpretation: String::new(),
            mapping: String::new(),
            binding: String::new(),
        };
        for (key, value) in metadata["properties"]
            .as_object()
            .context("metadata lacks properties")?
        {
            ensure!(!legacy_key(key), "{key}: unexpected LegacyFormat property");
            if !key.starts_with("novarocks.doc.") {
                continue;
            }
            let value = value
                .as_str()
                .with_context(|| format!("{key}: property is not text"))?;
            graph.private_bytes += key.len() + value.len();
            graph.properties.insert(key.clone(), value.into());
            if let Some(id) = key.strip_prefix(&format!("{PREFIX}rev.")) {
                let version: Version = canonical(value).with_context(|| key.clone())?;
                ensure!(
                    version.v == 2 && version.id == id && version.identity()? == id,
                    "{key}: invalid document identity/envelope"
                );
                graph.versions.insert(id.into(), version);
            } else if let Some(name) = key.strip_prefix(&format!("{PREFIX}slot.")) {
                let slot: Slot = canonical(value).with_context(|| key.clone())?;
                ensure!(
                    slot.v == 2
                        && name == format!("{}~{}", slot.owner, slot.name)
                        && slot.digest == slot.digest()?,
                    "{key}: invalid slot envelope/digest"
                );
                graph.slots.insert(key.clone(), slot);
            } else if let Some(owner) = key.strip_prefix(&format!("{PREFIX}root.")) {
                token(owner)?;
                unhex(value)?;
                graph.roots.insert(key.clone(), value.into());
            } else if let Some(name) = key.strip_prefix(&format!("{PREFIX}mark.")) {
                let marker: Marker = canonical(value).with_context(|| key.clone())?;
                ensure!(
                    marker.v == 2
                        && name == format!("{}~{}~{}", marker.owner, marker.name, marker.binding)
                        && marker.checksum()? == marker.digest,
                    "{key}: invalid marker"
                );
            } else {
                bail!("{key}: unrecognized private property key")
            }
        }
        for snapshot in metadata["snapshots"]
            .as_array()
            .context("metadata lacks snapshots")?
        {
            let id = snapshot["snapshot-id"]
                .as_i64()
                .context("snapshot lacks ID")?;
            let mut publication = None;
            for (key, value) in snapshot["summary"]
                .as_object()
                .context("snapshot lacks summary")?
            {
                if Some(id) == graph.current {
                    ensure!(!legacy_key(key), "{key}: unexpected Current LegacyFormat");
                }
                if !key.starts_with("novarocks.doc.") {
                    continue;
                }
                let name = key
                    .strip_prefix(&format!("{PREFIX}record."))
                    .with_context(|| format!("{key}: wrong snapshot key category"))?;
                let parts: Vec<_> = name.split('~').collect();
                ensure!(parts.len() == 2, "{key}: malformed record key");
                for p in parts {
                    token(p)?;
                }
                let frame = value
                    .as_str()
                    .with_context(|| format!("{key}: record is not text"))?;
                let parsed = record(frame).with_context(|| format!("{key}: snapshot {id}"))?;
                graph.record_bytes.push(frame.len());
                if key == RECORD {
                    publication = Some(parsed);
                }
            }
            graph.snapshots.push((id, publication));
        }
        let active = graph
            .current
            .and_then(|id| graph.snapshots.iter().find(|(i, _)| *i == id))
            .and_then(|(_, r)| r.as_ref());
        if graph.current.is_some() {
            ensure!(
                graph
                    .snapshots
                    .iter()
                    .any(|(id, _)| Some(*id) == graph.current),
                "current snapshot is absent"
            );
        }
        let binding = active
            .and_then(|r| r.binding.clone())
            .or_else(|| graph.roots.get(ROOT).cloned())
            .context("missing MV root/binding")?;
        let (d, l, m) = graph.closure(&binding)?;
        graph.binding = binding;
        graph.definition = d;
        graph.interpretation = l;
        graph.mapping = m;
        if let Some(p) = active {
            let _ = graph.record_logical_rows(p)?;
        } else if let Some(id) = graph.current {
            let snapshot = metadata["snapshots"]
                .as_array()
                .unwrap()
                .iter()
                .find(|s| s["snapshot-id"].as_i64() == Some(id))
                .unwrap();
            ensure!(
                snapshot["summary"]["total-data-files"].as_str() == Some("0")
                    && snapshot["summary"]["total-delete-files"].as_str() == Some("0"),
                "current snapshot has data or unknown file totals without P"
            );
        }
        for (id, p) in &graph.snapshots {
            if let Some(p) = p
                && let Err(e) = graph.record_logical_rows(p)
            {
                if Some(*id) == graph.current {
                    return Err(e);
                }
                graph.diagnostics.push(format!(
                    "history cannot be interpreted: snapshot {id}: {e:#}"
                ));
            }
        }
        ensure!(
            graph.slots.contains_key(CONFIG) && graph.slots.contains_key(MANAGEMENT),
            "missing C or management slot"
        );
        management(&graph.slots[MANAGEMENT])?;
        Ok(graph)
    }
    pub fn closure(&self, b: &str) -> Result<(String, String, String)> {
        let b = self
            .versions
            .get(b)
            .with_context(|| format!("novarocks.doc.v2.rev.{b}: missing binding"))?;
        ensure!(
            b.owner == "novarocks.mv"
                && b.name == "binding"
                && b.format
                    == Format {
                        owner: "novarocks.mv".into(),
                        name: "binding".into(),
                        version: 2
                    },
            "invalid B format"
        );
        ensure!(b.refs.len() == 3, "B must reference D, L and M");
        let find = |name: &str| -> Result<String> {
            Ok(b.refs
                .iter()
                .find(|r| r.rel == name)
                .with_context(|| format!("B missing {name}"))?
                .id
                .clone())
        };
        let d = find("definition")?;
        let l = find("interpretation")?;
        let m = find("mapping")?;
        let dv = self
            .versions
            .get(&d)
            .with_context(|| format!("novarocks.doc.v2.rev.{d}: missing D"))?;
        let lv = self
            .versions
            .get(&l)
            .with_context(|| format!("novarocks.doc.v2.rev.{l}: missing L"))?;
        ensure!(
            dv.owner == "novarocks.mv"
                && dv.name == "definition"
                && dv.refs.is_empty()
                && dv.format
                    == Format {
                        owner: "novarocks.mv".into(),
                        name: "definition".into(),
                        version: 2
                    },
            "invalid D"
        );
        ensure!(
            lv.owner == "novarocks.mv"
                && lv.name == "interpretation"
                && lv.format
                    == Format {
                        owner: "novarocks.mv".into(),
                        name: "interpretation".into(),
                        version: 3
                    }
                && lv.refs
                    == vec![Reference {
                        rel: "definition".into(),
                        id: d.clone()
                    }],
            "invalid L"
        );
        let lbytes = lv.bytes()?;
        idl_shape(
            &lbytes,
            &[
                (1, 0, false),
                (2, 2, false),
                (4, 2, true),
                (5, 2, true),
                (6, 2, false),
                (7, 2, true),
                (8, 2, true),
                (9, 2, false),
            ],
            &[1, 2, 4, 9],
        )?;
        let fields = proto_fields(&lbytes)?;
        ensure!(
            fields
                .iter()
                .find(|(n, _)| *n == 1)
                .map(|(_, b)| b.as_slice())
                == Some(&[3][..]),
            "L content is not format3"
        );
        ensure!(
            fields
                .iter()
                .find(|(n, _)| *n == 2)
                .map(|(_, b)| b.as_slice())
                == Some(unhex(&d)?.as_slice()),
            "L does not embed D semantic identity"
        );
        validate_definition(&dv.bytes()?)?;
        let mv = self
            .versions
            .get(&m)
            .with_context(|| format!("novarocks.doc.v2.rev.{m}: missing M"))?;
        ensure!(
            mv.owner == "novarocks.mv"
                && mv.name == "mapping"
                && mv.refs.is_empty()
                && mv.format
                    == Format {
                        owner: "novarocks.mv".into(),
                        name: "mapping".into(),
                        version: 1,
                    },
            "invalid M"
        );
        let bytes = b.bytes()?;
        binding_bounds(&bytes)?;
        validate_mapping(&mv.bytes()?, &bytes)?;
        Ok((d, l, m))
    }
    /// Independent decoding of the original publication inputs for Native
    /// window tests; this never imports the product document/revision codec.
    pub fn record_snapshot_watermarks(&self, p: &Record) -> Result<Vec<Option<i64>>> {
        let binding = p.binding.as_ref().context("MV publication missing B")?;
        self.closure(binding)?;
        let bytes = self.versions[binding].bytes()?;
        let bounds = binding_bounds(&bytes)?;
        let descriptors = binding_descriptors(&bytes)?;
        for descriptor in &descriptors {
            let fields = proto_fields(descriptor)?;
            ensure!(
                fields.len() == 4
                    && std::str::from_utf8(&fields[0].1)? == "iceberg"
                    && std::str::from_utf8(&fields[1].1)? == "connector-snapshot-id"
                    && scalar(&fields[2].1) == 1
                    && scalar(&fields[3].1) == 9,
                "Native window oracle requires the exact Iceberg snapshot format declaration"
            );
        }
        let (_, values) = publication_payload_parts(&p.payload, bounds.len(), &bounds)?;
        values
            .iter()
            .map(|value| decode_snapshot_watermark(value))
            .collect()
    }

    pub fn record_iceberg_source_uuids(&self, p: &Record) -> Result<Vec<String>> {
        let binding = p.binding.as_ref().context("MV publication missing B")?;
        self.closure(binding)?;
        let mut prefix = b"NRMVFCT1".to_vec();
        for part in ["iceberg", "connector-table-object-id"] {
            prefix.extend_from_slice(&u16::try_from(part.len())?.to_be_bytes());
            prefix.extend_from_slice(part.as_bytes());
        }
        prefix.extend_from_slice(&1u16.to_be_bytes());
        prefix.extend_from_slice(&36u16.to_be_bytes());
        proto_fields(&self.versions[binding].bytes()?)?
            .into_iter()
            .filter(|(tag, _)| *tag == 3)
            .map(|(_, occurrence)| {
                let (_, object) = proto_fields(&occurrence)?
                    .into_iter()
                    .find(|(tag, _)| *tag == 6)
                    .context("B occurrence is missing its exact object fact")?;
                let uuid = object
                    .strip_prefix(prefix.as_slice())
                    .context("not an exact Iceberg table UUID fact")?;
                ensure!(uuid.len() == 36, "invalid Iceberg table UUID length");
                Ok(std::str::from_utf8(uuid)?.to_owned())
            })
            .collect()
    }

    pub fn record_logical_rows(&self, p: &Record) -> Result<Option<u64>> {
        let b = p.binding.as_ref().context("MV publication missing B")?;
        self.closure(b)?;
        let bounds = binding_bounds(&self.versions[b].bytes()?)?;
        validate_payload(&p.payload, bounds.len(), &bounds)
    }
}
/// Parse just protobuf wire fields, retaining scalar's canonical varint bytes.
/// Full product semantics are deliberately not imported into this oracle.
pub(in crate::scenarios) fn proto_fields(bytes: &[u8]) -> Result<Vec<(u64, Vec<u8>)>> {
    fn varint(bytes: &[u8], p: &mut usize) -> Result<(u64, Vec<u8>)> {
        let start = *p;
        let mut n = 0u64;
        for shift in (0..70).step_by(7) {
            let b = *bytes.get(*p).context("truncated protobuf varint")?;
            *p += 1;
            ensure!(shift < 63 || b < 2, "protobuf varint overflow");
            n |= u64::from(b & 127) << shift;
            if b < 128 {
                ensure!(*p - start == 1 || b > 0, "noncanonical protobuf varint");
                return Ok((n, bytes[start..*p].to_vec()));
            }
        }
        bail!("protobuf varint overflow")
    }
    let mut p = 0;
    let mut out = Vec::new();
    while p < bytes.len() {
        let (key, _) = varint(bytes, &mut p)?;
        ensure!(key >> 3 > 0, "zero protobuf field");
        let value = match key & 7 {
            0 => varint(bytes, &mut p)?.1,
            2 => {
                let (len, _) = varint(bytes, &mut p)?;
                let end = p
                    .checked_add(usize::try_from(len)?)
                    .context("protobuf size overflow")?;
                let value = bytes
                    .get(p..end)
                    .context("truncated protobuf bytes")?
                    .to_vec();
                p = end;
                value
            }
            _ => bail!("unexpected protobuf wire kind"),
        };
        out.push((key >> 3, value));
    }
    Ok(out)
}
fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 128 {
        out.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    out.push(value as u8);
}
fn idl_shape(bytes: &[u8], schema: &[(u64, u8, bool)], required: &[u64]) -> Result<()> {
    let fields = proto_fields(bytes)?;
    let mut encoded = Vec::new();
    let mut previous = 0;
    for (number, value) in &fields {
        let (_, kind, repeated) = schema
            .iter()
            .find(|(n, _, _)| n == number)
            .context("unknown IDL field")?;
        ensure!(
            *number > previous || *number == previous && *repeated,
            "duplicate or unsorted IDL field"
        );
        previous = *number;
        encode_varint((number << 3) | u64::from(*kind), &mut encoded);
        if *kind == 2 {
            encode_varint(value.len() as u64, &mut encoded);
        }
        encoded.extend_from_slice(value);
    }
    ensure!(encoded == bytes, "IDL field has the wrong wire kind");
    ensure!(
        required
            .iter()
            .all(|n| fields.iter().any(|(actual, _)| n == actual)),
        "missing required IDL field"
    );
    Ok(())
}
fn scalar(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .enumerate()
        .fold(0, |n, (i, b)| n | (u64::from(b & 127) << (i * 7)))
}
fn validate_definition(d: &[u8]) -> Result<()> {
    idl_shape(
        d,
        &[(1, 0, false), (2, 2, false), (6, 0, false)],
        &[1, 2, 6],
    )?;
    let fields = proto_fields(d)?;
    ensure!(fields.first() == Some(&(1, vec![2])), "D is not format2");
    let query = &fields[1].1;
    idl_shape(
        query,
        &[(1, 2, false), (2, 0, false), (3, 2, false)],
        &[1, 2, 3],
    )?;
    let fields = proto_fields(query)?;
    ensure!(
        !std::str::from_utf8(&fields[0].1)?.is_empty() && fields[1].1 == [2],
        "D does not carry a Trino query"
    );
    let context = &fields[2].1;
    idl_shape(context, &[(1, 2, false), (2, 2, false)], &[1, 2])?;
    ensure!(
        proto_fields(context)?
            .iter()
            .all(|(_, value)| !value.is_empty()),
        "D lacks a resolution context"
    );
    Ok(())
}

fn binding_descriptors(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    idl_shape(
        bytes,
        &[
            (1, 0, false),
            (3, 2, true),
            (4, 2, true),
            (5, 2, true),
            (6, 2, false),
        ],
        &[1, 3, 4, 6],
    )?;
    let fields = proto_fields(bytes)?;
    ensure!(fields.first() == Some(&(1, vec![2])), "B is not format2");
    fields
        .into_iter()
        .filter(|(n, _)| *n == 3)
        .map(|(_, occurrence)| {
            idl_shape(
                &occurrence,
                &[
                    (1, 0, false),
                    (2, 2, false),
                    (3, 2, false),
                    (4, 2, false),
                    (5, 2, false),
                    (6, 2, false),
                    (7, 0, false),
                    (8, 2, false),
                    (9, 2, false),
                    (10, 2, false),
                    (11, 2, false),
                    (12, 2, false),
                    (13, 2, true),
                    (14, 2, false),
                ],
                &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14],
            )?;
            let mut fields = proto_fields(&occurrence)?.into_iter();
            ensure!(
                fields
                    .nth(6)
                    .is_some_and(|(n, value)| n == 7 && value == [1]),
                "B occurrence must be a current read"
            );
            fields
                .next()
                .filter(|(n, _)| *n == 8)
                .map(|(_, value)| value)
                .context("B occurrence lacks a data-version descriptor")
        })
        .collect()
}

fn validate_mapping(bytes: &[u8], binding: &[u8]) -> Result<()> {
    idl_shape(bytes, &[(1, 0, false), (2, 2, true)], &[1, 2])?;
    let columns = proto_fields(bytes)?;
    ensure!(columns.first() == Some(&(1, vec![1])), "M is not format1");
    let outputs = proto_fields(binding)?
        .into_iter()
        .filter_map(|(n, bytes)| (n == 4).then_some(bytes))
        .collect::<Vec<_>>();
    ensure!(
        columns.len() == outputs.len() + 1,
        "M/B output count differs"
    );
    let mut storage_ids = BTreeSet::new();
    for (ordinal, ((_, column), output)) in columns[1..].iter().zip(outputs).enumerate() {
        idl_shape(
            column,
            &[
                (1, 0, false),
                (2, 2, false),
                (3, 2, false),
                (4, 2, false),
                (5, 2, false),
                (6, 2, false),
                (7, 2, false),
                (8, 0, false),
                (9, 0, false),
                (10, 2, false),
                (11, 0, false),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
        )?;
        idl_shape(
            &output,
            &[(1, 0, false), (2, 2, false), (3, 2, false), (4, 2, true)],
            &[1, 2, 3],
        )?;
        let fields = proto_fields(column)?;
        let bound = proto_fields(&output)?;
        ensure!(
            scalar(&fields[0].1) == ordinal as u64
                && fields[0].1 == bound[0].1
                && !fields[2].1.is_empty()
                && fields[2].1 == bound[2].1,
            "M column disagrees with the B output"
        );
        ensure!(
            [1, 3, 4, 5, 6, 9]
                .into_iter()
                .all(|i| !fields[i].1.is_empty())
                && storage_ids.insert(fields[1].1.clone())
                && (1..=3).contains(&scalar(&fields[7].1))
                && scalar(&fields[8].1) <= 1
                && scalar(&fields[10].1) > 0,
            "M column has incomplete or invalid storage facts"
        );
    }
    Ok(())
}

pub(in crate::scenarios) fn binding_bounds(bytes: &[u8]) -> Result<Vec<usize>> {
    let descriptors = binding_descriptors(bytes)?;
    let mut out = Vec::new();
    for descriptor in &descriptors {
        idl_shape(
            descriptor,
            &[(1, 2, false), (2, 2, false), (3, 0, false), (4, 0, false)],
            &[1, 2, 3, 4],
        )?;
        let fields = proto_fields(descriptor)?;
        ensure!(
            fields.len() == 4 && fields.iter().map(|(n, _)| *n).eq(1..=4),
            "B descriptor fields are not exact"
        );
        let provider = std::str::from_utf8(&fields[0].1)?;
        let name = std::str::from_utf8(&fields[1].1)?;
        ensure!(
            !provider.is_empty() && !name.is_empty() && !fields[2].1.is_empty(),
            "invalid B format descriptor"
        );
        ensure!(
            provider.is_ascii()
                && name.is_ascii()
                && name.len() <= 128
                && (1..=u64::from(u16::MAX)).contains(&scalar(&fields[2].1)),
            "invalid B provider/format/version"
        );
        let n = usize::try_from(scalar(&fields[3].1))?;
        ensure!((1..=256).contains(&n), "invalid B maximum");
        out.push(n);
    }
    Ok(out)
}
pub(in crate::scenarios) fn validate_payload(
    bytes: &[u8],
    count: usize,
    bounds: &[usize],
) -> Result<Option<u64>> {
    publication_payload_parts(bytes, count, bounds).map(|(rows, _)| rows)
}

fn decode_snapshot_watermark(value: &[u8]) -> Result<Option<i64>> {
    match value {
        [0] => Ok(None),
        [1, bytes @ ..] if bytes.len() == 8 => {
            let id = i64::from_be_bytes(bytes.try_into()?);
            Ok(Some(id))
        }
        _ => bail!("invalid publication snapshot watermark tag or width"),
    }
}

fn publication_payload_parts(
    bytes: &[u8],
    count: usize,
    bounds: &[usize],
) -> Result<(Option<u64>, Vec<Vec<u8>>)> {
    ensure!(bytes.len() >= 28 && bytes[0] == 1, "invalid P header");
    let frozen = i64::from_be_bytes(bytes[17..25].try_into()?);
    ensure!(frozen >= 0, "negative P frozen time");
    let mut p = 26;
    let logical_rows = match bytes[25] {
        0 => None,
        1 => {
            let value = bytes.get(p..p + 8).context("truncated P logical rows")?;
            p += 8;
            Some(u64::from_be_bytes(value.try_into()?))
        }
        _ => bail!("invalid P row presence"),
    };
    ensure!(bytes.len() >= p + 2, "truncated P count");
    let n = u16::from_be_bytes(bytes[p..p + 2].try_into()?) as usize;
    p += 2;
    ensure!(
        n == count && n == bounds.len(),
        "P occurrence count differs from D/B"
    );
    let mut values = Vec::with_capacity(count);
    for bound in bounds {
        ensure!(bytes.len() >= p + 2, "truncated P value length");
        let len = u16::from_be_bytes(bytes[p..p + 2].try_into()?) as usize;
        p += 2;
        ensure!(
            len <= *bound && len <= 256,
            "P value exceeds declared maximum"
        );
        let end = p.checked_add(len).context("P length overflow")?;
        values.push(bytes.get(p..end).context("truncated P value")?.to_vec());
        p = end;
    }
    ensure!(p == bytes.len(), "trailing P bytes");
    Ok((logical_rows, values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_retained_version_crosses_former_inline_quotas_without_losing_identity() {
        let content = vec![b'x'; 196_609];
        let version = Version::new(
            "novarocks.mv",
            "definition",
            Format {
                owner: "novarocks.mv".into(),
                name: "definition".into(),
                version: 1,
            },
            vec![],
            &content,
        )
        .unwrap();
        let encoded = version.encoded().unwrap();
        assert!(encoded.len() > 65_536);
        let mut metadata = graph_fixture();
        let key = format!("{PREFIX}rev.{}", version.id);
        metadata["properties"][&key] = serde_json::json!(encoded);
        let graph = Graph::from_metadata(&metadata).unwrap();
        assert!(graph.private_bytes > 262_144);
        assert_eq!(graph.versions[&version.id].bytes().unwrap(), content);

        let encoded = metadata["properties"][&key].as_str().unwrap();
        metadata["properties"][&key] = serde_json::json!(encoded.replacen("eHh4", "eHh5", 1));
        assert!(Graph::from_metadata(&metadata).is_err());
    }

    #[test]
    fn large_publication_keeps_all_declared_watermark_bounds() {
        let mut payload = vec![1];
        payload.extend_from_slice(&[0x11; 16]);
        payload.extend_from_slice(&42i64.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&7u64.to_be_bytes());
        payload.extend_from_slice(&370u16.to_be_bytes());
        for _ in 0..370 {
            payload.extend_from_slice(&9u16.to_be_bytes());
            payload.push(1);
            payload.extend_from_slice(&17i64.to_be_bytes());
        }
        assert!(payload.len() > 4_096);
        let binding = "a".repeat(64);
        let framed = record_frame(&binding, &payload);
        let decoded = record(&framed).unwrap();
        assert_eq!(decoded.binding.as_deref(), Some(binding.as_str()));
        assert_eq!(decoded.payload, payload);
        assert_eq!(
            validate_payload(&decoded.payload, 370, &[9; 370]).unwrap(),
            Some(7)
        );
        let mut bounds = [9; 370];
        bounds[0] = 8;
        assert!(validate_payload(&decoded.payload, 370, &bounds).is_err());
    }

    #[test]
    fn publication_snapshot_watermark_has_exact_tag_width_and_none() {
        assert_eq!(decode_snapshot_watermark(&[0]).unwrap(), None);
        let mut value = vec![1];
        value.extend_from_slice(&42i64.to_be_bytes());
        assert_eq!(decode_snapshot_watermark(&value).unwrap(), Some(42));
        let mut signed = vec![1];
        signed.extend_from_slice(&i64::MIN.to_be_bytes());
        assert_eq!(decode_snapshot_watermark(&signed).unwrap(), Some(i64::MIN));
        for invalid in [vec![], vec![0, 0], vec![1], vec![2]] {
            assert!(decode_snapshot_watermark(&invalid).is_err());
        }
        let mut p = vec![1];
        p.extend_from_slice(&[0; 16]);
        p.extend_from_slice(&0i64.to_be_bytes());
        p.push(0);
        p.extend_from_slice(&1u16.to_be_bytes());
        p.extend_from_slice(&9u16.to_be_bytes());
        p.extend_from_slice(&value);
        assert_eq!(
            publication_payload_parts(&p, 1, &[9]).unwrap().1,
            vec![value]
        );
        assert!(publication_payload_parts(&p, 2, &[9, 9]).is_err());
        p.push(0);
        assert!(publication_payload_parts(&p, 1, &[9]).is_err());
    }

    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }
    #[test]
    fn independent_b_and_p_match_frozen_manual_vectors() {
        let b = bytes(concat!(
            "08021a920108011207636174616c6f671a026e732204626173652a046c65667432066f626a656374380142240a076963",
            "65626572671215636f6e6e6563746f722d736e617073686f742d6964180120094a0b6f626a6563742d66616374520970",
            "6174682d666163745a09747970652d6661637462116465636c61726174696f6e732d66616374720f6f6363757272656e",
            "63652d666163741a930108021207636174616c6f671a026e732204626173652a05726967687432066f626a6563743801",
            "42240a07696365626572671215636f6e6e6563746f722d736e617073686f742d6964180120094a0b6f626a6563742d66",
            "6163745209706174682d666163745a09747970652d6661637462116465636c61726174696f6e732d66616374720f6f63",
            "63757272656e63652d6661637422120800120576616c75651a07696e746567657232660a207272727272727272727272",
            "727272727272727272727272727272727272727272122064646464646464646464646464646464646464646464646464",
            "646464646464641a206f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f",
        ));
        let p = bytes(
            "0111111111111111111111111111111111000000000000002a010000000000000007000200090100000000000000110009010000000000000011",
        );
        assert_eq!(binding_bounds(&b).unwrap(), [9, 9]);
        let mut wrong_kind = vec![10, 1, 1];
        wrong_kind.extend_from_slice(&b[2..]);
        assert!(binding_bounds(&wrong_kind).is_err());
        assert_eq!(validate_payload(&p, 2, &[9, 9]).unwrap(), Some(7));
        for end in 0..p.len() {
            assert!(validate_payload(&p[..end], 2, &[9, 9]).is_err());
        }
        let mut trailing = p;
        trailing.push(0);
        assert!(validate_payload(&trailing, 2, &[9, 9]).is_err());
    }
    #[test]
    fn independent_semantic_identity_matches_spi_manual_golden() {
        let v = Version::new(
            "novarocks.mv",
            "definition",
            Format {
                owner: "novarocks.mv".into(),
                name: "definition".into(),
                version: 1,
            },
            vec![],
            b"abc",
        )
        .unwrap();
        assert_eq!(
            v.id,
            "b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e"
        );
        assert_eq!(
            record_frame(&v.id, b"abc"),
            "2.b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e.YWJj"
        );
    }
    #[test]
    fn strict_envelopes_and_management_values_reject_ambiguity() {
        let marker = Marker::late().unwrap();
        let encoded = marker.encoded().unwrap();
        let _: Marker = canonical(&encoded).unwrap();
        assert!(canonical::<Marker>(&format!(" {encoded}")).is_err());
        let mut slot = Slot {
            v: 2,
            owner: "novarocks.storage".into(),
            name: "management".into(),
            format: Format {
                owner: "novarocks.mv".into(),
                name: "management".into(),
                version: 1,
            },
            writer: None,
            content: String::new(),
            digest: String::new(),
        };
        slot.replace_content(&management_content(
            "materialized-view",
            "deployment",
            "process",
        ))
        .unwrap();
        assert_eq!(
            management(&slot).unwrap(),
            ["materialized-view", "deployment", "process"]
        );
        let mut bad = decode64(&slot.content).unwrap();
        bad.push(0);
        slot.replace_content(&bad).unwrap();
        assert!(management(&slot).is_err());
    }
    // Child application payloads are opaque here: this scaffold exercises
    // identity/reference interpretation, not execution or management authority.
    fn graph_fixture() -> Value {
        let format = |name: &str, version| Format {
            owner: "novarocks.mv".into(),
            name: name.into(),
            version,
        };
        let d = Version::new(
            "novarocks.mv",
            "definition",
            format("definition", 2),
            vec![],
            &bytes(
                "080212290a1653454c4543542076616c75652046524f4d206261736510021a0d0a07636174616c6f6712026e73302a",
            ),
        )
        .unwrap();
        let mut l = vec![8, 3, 18, 32];
        l.extend(unhex(&d.id).unwrap());
        l.extend([34, 0, 74, 3, 10, 1, 1]);
        let l = Version::new(
            "novarocks.mv",
            "interpretation",
            format("interpretation", 3),
            vec![Reference {
                rel: "definition".into(),
                id: d.id.clone(),
            }],
            &l,
        )
        .unwrap();
        let m = Version::new(
            "novarocks.mv",
            "mapping",
            format("mapping", 1),
            vec![],
            &bytes(concat!(
                "0801124e080012056669656c641a07696e7465676572220c73746f726167652d747970652a07656e636f646572320764",
                "65636f6465723a07636f6e74656e7440014800520b6465636c61726174696f6e5803",
            )),
        )
        .unwrap();
        let b = Version::new(
            "novarocks.mv",
            "binding",
            format("binding", 2),
            vec![
                Reference {
                    rel: "definition".into(),
                    id: d.id.clone(),
                },
                Reference {
                    rel: "interpretation".into(),
                    id: l.id.clone(),
                },
                Reference {
                    rel: "mapping".into(),
                    id: m.id.clone(),
                },
            ],
            &bytes(concat!(
                "08021a940108011207636174616c6f671a026e732204626173652a06736f7572636532066f626a656374380142240a07",
                "696365626572671215636f6e6e6563746f722d736e617073686f742d6964180120094a0b6f626a6563742d6661637452",
                "09706174682d666163745a09747970652d6661637462116465636c61726174696f6e732d66616374720f6f6363757272",
                "656e63652d6661637422120800120576616c75651a07696e746567657232660a20727272727272727272727272727272",
                "727272727272727272727272727272727212206464646464646464646464646464646464646464646464646464646464",
                "6464641a206f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f",
            )),
        )
        .unwrap();
        let mut c = Slot {
            v: 2,
            owner: "novarocks.mv".into(),
            name: "configuration".into(),
            format: format("configuration", 1),
            writer: None,
            content: String::new(),
            digest: String::new(),
        };
        c.replace_content(&[8, 1, 16, 1, 24, 0]).unwrap();
        let mut management_slot = Slot {
            v: 2,
            owner: "novarocks.storage".into(),
            name: "management".into(),
            format: format("management", 1),
            writer: None,
            content: String::new(),
            digest: String::new(),
        };
        management_slot
            .replace_content(&management_content("materialized-view", "owner", "process"))
            .unwrap();
        let p =
            bytes("0111111111111111111111111111111111000000000000002a0000010009010000000000000011");
        serde_json::json!({"properties":{(d.key()):d.encoded().unwrap(),(l.key()):l.encoded().unwrap(),(m.key()):m.encoded().unwrap(),(b.key()):b.encoded().unwrap(),(ROOT):b.id,(CONFIG):c.encoded().unwrap(),(MANAGEMENT):management_slot.encoded().unwrap()},"current-snapshot-id":1,"snapshots":[{"snapshot-id":1,"summary":{"operation":"append","total-data-files":"1","total-delete-files":"0",(RECORD):record_frame(&b.id,&p)}}]})
    }
    #[test]
    fn current_corruption_names_the_key_and_history_is_only_diagnostic() {
        let mut metadata = graph_fixture();
        let first = Graph::from_metadata(&metadata).unwrap();
        let old_l = &first.versions[&first.interpretation];
        let mut content = old_l.bytes().unwrap();
        *content.last_mut().unwrap() = 2;
        let new_l = Version::new(
            &old_l.owner,
            &old_l.name,
            old_l.format.clone(),
            old_l.refs.clone(),
            &content,
        )
        .unwrap();
        let old_b = &first.versions[&first.binding];
        let new_b = Version::new(
            &old_b.owner,
            &old_b.name,
            old_b.format.clone(),
            vec![
                Reference {
                    rel: "definition".into(),
                    id: first.definition.clone(),
                },
                Reference {
                    rel: "interpretation".into(),
                    id: new_l.id.clone(),
                },
                Reference {
                    rel: "mapping".into(),
                    id: first.mapping.clone(),
                },
            ],
            &old_b.bytes().unwrap(),
        )
        .unwrap();
        metadata["properties"][new_l.key()] = serde_json::json!(new_l.encoded().unwrap());
        metadata["properties"][new_b.key()] = serde_json::json!(new_b.encoded().unwrap());
        let p = first.snapshots[0].1.as_ref().unwrap();
        let mut current = metadata["snapshots"][0].clone();
        current["snapshot-id"] = serde_json::json!(2);
        current["summary"][RECORD] = serde_json::json!(record_frame(&new_b.id, &p.payload));
        metadata["snapshots"].as_array_mut().unwrap().push(current);
        metadata["current-snapshot-id"] = serde_json::json!(2);
        metadata["properties"]
            .as_object_mut()
            .unwrap()
            .remove(&old_l.key());
        let graph = Graph::from_metadata(&metadata).unwrap();
        assert_eq!(graph.binding, new_b.id);
        assert_eq!(graph.diagnostics.len(), 1);
        // A dangling root does not replace the actual current record's binding.
        metadata["properties"]
            .as_object_mut()
            .unwrap()
            .remove(&old_b.key());
        assert!(Graph::from_metadata(&metadata).is_ok());
        let key = new_l.key();
        metadata["properties"].as_object_mut().unwrap().remove(&key);
        assert!(format!("{:#}", Graph::from_metadata(&metadata).unwrap_err()).contains(&key));
    }
    #[test]
    fn no_publication_requires_both_known_zero_file_totals() {
        let mut metadata = graph_fixture();
        metadata["snapshots"][0]["summary"]
            .as_object_mut()
            .unwrap()
            .remove(RECORD);
        assert!(Graph::from_metadata(&metadata).is_err());
        metadata["snapshots"][0]["summary"]["total-data-files"] = serde_json::json!("0");
        assert!(Graph::from_metadata(&metadata).is_ok());
        metadata["snapshots"][0]["summary"]
            .as_object_mut()
            .unwrap()
            .remove("total-delete-files");
        assert!(Graph::from_metadata(&metadata).is_err());
        metadata["current-snapshot-id"] = Value::Null;
        metadata["snapshots"] = serde_json::json!([]);
        assert!(Graph::from_metadata(&metadata).is_ok());
    }
    #[test]
    fn noncanonical_version_and_unknown_private_key_are_rejected() {
        let mut metadata = graph_fixture();
        let graph = Graph::from_metadata(&metadata).unwrap();
        let key = graph.versions[&graph.definition].key();
        let encoded = metadata["properties"][&key].as_str().unwrap().to_string();
        metadata["properties"][&key] = serde_json::json!(format!("{encoded} "));
        assert!(format!("{:#}", Graph::from_metadata(&metadata).unwrap_err()).contains(&key));
        let mut metadata = graph_fixture();
        metadata["properties"]["novarocks.doc.v2.unknown.test"] = serde_json::json!("value");
        assert!(
            format!("{:#}", Graph::from_metadata(&metadata).unwrap_err())
                .contains("novarocks.doc.v2.unknown.test")
        );
    }
}
