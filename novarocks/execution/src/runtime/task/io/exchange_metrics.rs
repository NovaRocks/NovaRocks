use once_cell::sync::Lazy;
use prometheus::{IntCounter, Opts, Registry};

static EXCHANGE_SHUFFLE_BYTES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(Opts::new(
        "novarocks_exchange_shuffle_bytes_total",
        "Total number of exchange shuffle payload bytes sent.",
    ))
    .expect("construct novarocks_exchange_shuffle_bytes_total")
});

pub fn observe_exchange_shuffle_bytes(bytes: usize) {
    Lazy::force(&EXCHANGE_SHUFFLE_BYTES_TOTAL).inc_by(bytes as u64);
}

/// Register execution-owned exchange collectors with the role registry that
/// composes execution.  Execution does not select a process or registry.
pub fn register_exchange_metrics(registry: &Registry) -> Result<(), String> {
    registry
        .register(Box::new(Lazy::force(&EXCHANGE_SHUFFLE_BYTES_TOTAL).clone()))
        .map_err(|error| format!("register exchange metrics: {error}"))
}

#[cfg(test)]
mod tests {
    use prometheus::Registry;

    use super::{observe_exchange_shuffle_bytes, register_exchange_metrics};

    #[test]
    fn registers_only_with_the_explicit_registry() {
        let registry = Registry::new();
        register_exchange_metrics(&registry).expect("register exchange collector");
        observe_exchange_shuffle_bytes(7);

        let families = registry.gather();
        assert!(
            families
                .iter()
                .any(|family| { family.get_name() == "novarocks_exchange_shuffle_bytes_total" })
        );
    }
}
