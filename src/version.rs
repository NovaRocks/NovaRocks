const GIT_HASH: &str = env!("NOVAROCKS_GIT_HASH");
const GIT_TIME: &str = env!("NOVAROCKS_GIT_TIME");

/// Short version string reported via heartbeat, e.g. "novarocks-1b9f054a".
/// Matches StarRocks BE convention of "version-commit".
pub fn short_version() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION.get_or_init(|| format!("novarocks-{GIT_HASH}"))
}

/// Full version string including commit time for logging at startup.
pub fn full_version() -> &'static str {
    static VERSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    VERSION.get_or_init(|| format!("novarocks-{GIT_HASH} ({GIT_TIME})"))
}
