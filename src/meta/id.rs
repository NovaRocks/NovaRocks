use crate::meta::{MetaError, MetaErrorKind};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct IdScope(String);

impl IdScope {
    pub fn new(value: impl Into<String>) -> Result<Self, MetaError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                "metadata id scope must not be empty",
            ));
        }
        if value.chars().any(|ch| ch.is_control()) {
            return Err(MetaError::new(
                MetaErrorKind::InvalidRequest,
                format!("metadata id scope `{value}` contains a control character"),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}
