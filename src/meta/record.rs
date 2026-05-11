use bytes::Bytes;

use crate::meta::{MetaError, MetaErrorKind};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetaKey {
    namespace: String,
    path: Vec<String>,
}

impl MetaKey {
    pub fn new<I, S>(namespace: impl Into<String>, path: I) -> Result<Self, MetaError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let namespace = validate_namespace(namespace.into())?;
        let path = path
            .into_iter()
            .map(|segment| validate_segment(segment.into()))
            .collect::<Result<Vec<_>, _>>()?;
        if path.is_empty() {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                "metadata key path must not be empty",
            ));
        }
        Ok(Self { namespace, path })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn canonical_path(&self) -> String {
        self.path.join("/")
    }

    pub(crate) fn from_canonical(
        namespace: impl Into<String>,
        canonical_path: impl Into<String>,
    ) -> Result<Self, MetaError> {
        let canonical_path = canonical_path.into();
        Self::new(namespace, canonical_path.split('/'))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetaKeyPrefix {
    namespace: String,
    path: Vec<String>,
}

impl MetaKeyPrefix {
    pub fn new<I, S>(namespace: impl Into<String>, path: I) -> Result<Self, MetaError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Ok(Self {
            namespace: validate_namespace(namespace.into())?,
            path: path
                .into_iter()
                .map(|segment| validate_segment(segment.into()))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn canonical_path(&self) -> String {
        self.path.join("/")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaRecord {
    pub key: MetaKey,
    pub kind: MetaRecordKind,
    pub revision: MetaRevision,
    pub payload: MetaPayload,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaRecordPut {
    pub key: MetaKey,
    pub kind: MetaRecordKind,
    pub expected: ExpectedRevision,
    pub payload: MetaPayload,
}

impl MetaRecordPut {
    pub fn new(
        key: MetaKey,
        kind: MetaRecordKind,
        expected: ExpectedRevision,
        payload: MetaPayload,
    ) -> Self {
        Self {
            key,
            kind,
            expected,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaRecordKind(String);

impl MetaRecordKind {
    pub fn new(value: impl Into<String>) -> Result<Self, MetaError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                "metadata record kind must not be empty",
            ));
        }
        if value.chars().any(|ch| ch.is_control()) {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                format!("metadata record kind `{value}` contains a control character"),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaRevision(Bytes);

impl MetaRevision {
    pub(crate) fn from_sqlite_i64(value: i64) -> Self {
        Self(Bytes::from(value.to_be_bytes().to_vec()))
    }

    pub(crate) fn to_sqlite_i64(&self) -> Result<i64, MetaError> {
        let bytes = self.0.as_ref();
        let array: [u8; 8] = bytes.try_into().map_err(|_| {
            MetaError::new(
                MetaErrorKind::InvalidRequest,
                "metadata revision is not a SQLite revision token",
            )
        })?;
        Ok(i64::from_be_bytes(array))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpectedRevision {
    Any,
    NotExists,
    Exists,
    Exact(MetaRevision),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaPayload {
    pub encoding: MetaPayloadEncoding,
    pub schema_version: i32,
    pub bytes: Bytes,
}

impl MetaPayload {
    pub fn json(schema_version: i32, bytes: Bytes) -> Self {
        Self {
            encoding: MetaPayloadEncoding::Json,
            schema_version,
            bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetaPayloadEncoding {
    Json,
    Protobuf,
    Raw,
}

impl MetaPayloadEncoding {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Protobuf => "protobuf",
            Self::Raw => "raw",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, MetaError> {
        match value {
            "json" => Ok(Self::Json),
            "protobuf" => Ok(Self::Protobuf),
            "raw" => Ok(Self::Raw),
            _ => Err(MetaError::new(
                MetaErrorKind::ProviderCorruption,
                format!("unknown metadata payload encoding `{value}`"),
            )),
        }
    }
}

fn validate_namespace(value: String) -> Result<String, MetaError> {
    if value.is_empty() {
        return Err(MetaError::new(
            MetaErrorKind::InvalidRequest,
            "metadata key namespace must not be empty",
        ));
    }
    if !value.is_ascii() || value.contains('/') || value.chars().any(|ch| ch.is_control()) {
        return Err(MetaError::new(
            MetaErrorKind::InvalidRequest,
            format!("invalid metadata namespace `{value}`"),
        ));
    }
    Ok(value)
}

fn validate_segment(value: String) -> Result<String, MetaError> {
    if value.is_empty() {
        return Err(MetaError::new(
            MetaErrorKind::InvalidRequest,
            "metadata key path segment must not be empty",
        ));
    }
    if !value.is_ascii() || value.contains('/') || value.chars().any(|ch| ch.is_control()) {
        return Err(MetaError::new(
            MetaErrorKind::InvalidRequest,
            format!("invalid metadata key path segment `{value}`"),
        ));
    }
    Ok(value)
}
