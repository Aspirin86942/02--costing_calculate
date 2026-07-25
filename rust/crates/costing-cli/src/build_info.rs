use serde::Serialize;

pub const CONFIG_SCHEMA_VERSION: u32 = 1;
pub const RUN_MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BuildInfo {
    pub name: &'static str,
    pub version: &'static str,
    pub git_commit: &'static str,
    pub build_timestamp: &'static str,
    pub rustc_version: &'static str,
    pub target: &'static str,
    pub config_schema_version: u32,
    pub run_manifest_schema_version: u32,
}

impl BuildInfo {
    #[must_use]
    pub const fn current() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            git_commit: env!("COSTING_GIT_COMMIT"),
            build_timestamp: env!("COSTING_BUILD_TIMESTAMP"),
            rustc_version: env!("COSTING_RUSTC_VERSION"),
            target: env!("COSTING_BUILD_TARGET"),
            config_schema_version: CONFIG_SCHEMA_VERSION,
            run_manifest_schema_version: RUN_MANIFEST_SCHEMA_VERSION,
        }
    }
}
