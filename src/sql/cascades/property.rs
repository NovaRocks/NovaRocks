//! Physical properties for Cascades optimizer.

/// A column reference used in distribution/ordering specs.
/// Uses column name (not TypedExpr) for hashability.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct ColumnRef {
    pub qualifier: Option<String>,
    pub column: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct PhysicalPropertySet {
    pub distribution: DistributionSpec,
    pub ordering: OrderingSpec,
}

impl PhysicalPropertySet {
    pub fn any() -> Self {
        Self {
            distribution: DistributionSpec::Any,
            ordering: OrderingSpec::Any,
        }
    }

    pub fn gather() -> Self {
        Self {
            distribution: DistributionSpec::Gather,
            ordering: OrderingSpec::Any,
        }
    }

    pub fn satisfies(&self, required: &PhysicalPropertySet) -> bool {
        self.distribution.satisfies(&required.distribution)
            && self.ordering.satisfies(&required.ordering)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum DistributionSpec {
    Any,
    Gather,
    HashPartitioned(Vec<ColumnRef>),
}

impl DistributionSpec {
    pub fn satisfies(&self, required: &DistributionSpec) -> bool {
        match required {
            DistributionSpec::Any => true,
            DistributionSpec::Gather => matches!(self, DistributionSpec::Gather),
            DistributionSpec::HashPartitioned(req_cols) => {
                if let DistributionSpec::HashPartitioned(my_cols) = self {
                    my_cols == req_cols
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum OrderingSpec {
    Any,
    Required(Vec<SortKey>),
}

impl OrderingSpec {
    pub fn satisfies(&self, required: &OrderingSpec) -> bool {
        match required {
            OrderingSpec::Any => true,
            OrderingSpec::Required(req_keys) => {
                if let OrderingSpec::Required(my_keys) = self {
                    // Provided ordering must be a prefix-or-equal match
                    my_keys.len() >= req_keys.len()
                        && my_keys.iter().zip(req_keys).all(|(m, r)| m == r)
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct SortKey {
    pub column: ColumnRef,
    pub asc: bool,
    pub nulls_first: bool,
}
