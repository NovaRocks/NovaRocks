#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImvVersionRole {
    From,
    To,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvVersionRef {
    pub(crate) role: ImvVersionRole,
}

impl ImvVersionRef {
    pub(crate) fn from_snapshot() -> Self {
        Self {
            role: ImvVersionRole::From,
        }
    }

    pub(crate) fn to_snapshot() -> Self {
        Self {
            role: ImvVersionRole::To,
        }
    }
}

impl Default for ImvVersionRef {
    fn default() -> Self {
        Self::to_snapshot()
    }
}
