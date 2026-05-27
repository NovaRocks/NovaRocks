use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

thread_local! {
    static CLIENT_DISCONNECT_SIGNAL: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

pub(crate) fn with_client_disconnect_signal<T>(
    signal: Arc<AtomicBool>,
    f: impl FnOnce() -> T,
) -> T {
    CLIENT_DISCONNECT_SIGNAL.with(|cell| {
        let previous = cell.replace(Some(signal));
        let result = f();
        cell.replace(previous);
        result
    })
}

pub(crate) fn client_disconnected() -> bool {
    CLIENT_DISCONNECT_SIGNAL.with(|cell| {
        cell.borrow()
            .as_ref()
            .is_some_and(|signal| signal.load(Ordering::SeqCst))
    })
}
