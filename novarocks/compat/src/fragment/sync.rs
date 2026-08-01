use novarocks_types::UniqueId;

/// Compat-owned bridge for synchronous StarRocks fragment execution.
///
/// The encoded request remains entirely within the Compat application; the
/// fragment kernel accepts only decoded, sealed fragment submissions.
pub(crate) trait SyncFragmentExecutor: Send + Sync + 'static {
    fn execute_encoded(&self, payload: &[u8]) -> Result<UniqueId, String>;
}
