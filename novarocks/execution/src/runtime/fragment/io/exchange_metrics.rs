use once_cell::sync::Lazy;
use prometheus::{IntCounter, register_int_counter};

static EXCHANGE_SHUFFLE_BYTES_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "novarocks_exchange_shuffle_bytes_total",
        "Total number of exchange shuffle payload bytes sent."
    )
    .expect("register novarocks_exchange_shuffle_bytes_total")
});

pub fn observe_exchange_shuffle_bytes(bytes: usize) {
    Lazy::force(&EXCHANGE_SHUFFLE_BYTES_TOTAL).inc_by(bytes as u64);
}

pub fn ensure_exchange_metrics_registered() {
    Lazy::force(&EXCHANGE_SHUFFLE_BYTES_TOTAL);
}
