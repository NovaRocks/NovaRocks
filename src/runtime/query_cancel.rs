use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    static CLIENT_DISCONNECT_SIGNAL: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

struct ClientDisconnectSignalGuard {
    previous: Option<Arc<AtomicBool>>,
}

impl Drop for ClientDisconnectSignalGuard {
    fn drop(&mut self) {
        CLIENT_DISCONNECT_SIGNAL.with(|cell| {
            cell.replace(self.previous.take());
        });
    }
}

pub(crate) fn with_client_disconnect_signal<T>(
    signal: Arc<AtomicBool>,
    f: impl FnOnce() -> T,
) -> T {
    let _guard = CLIENT_DISCONNECT_SIGNAL.with(|cell| ClientDisconnectSignalGuard {
        previous: cell.replace(Some(signal)),
    });
    f()
}

pub(crate) fn client_disconnected() -> bool {
    CLIENT_DISCONNECT_SIGNAL.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|signal| signal.load(Ordering::SeqCst))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_client_disconnect_signal_restores_state_after_panic() {
        let signal = Arc::new(AtomicBool::new(true));
        let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_client_disconnect_signal(signal, || panic!("boom"));
        }));

        assert!(panic_result.is_err(), "closure should panic");
        assert!(
            !client_disconnected(),
            "disconnect signal must be restored after unwind"
        );
    }
}
