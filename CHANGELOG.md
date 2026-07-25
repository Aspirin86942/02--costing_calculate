# Changelog

All notable changes to Costing Calculate are documented in this file.

## Unreleased

- Adopt comparison-only borrowed `cell_text` access after the controlled SK
  check-only and normal-run experiments passed all performance and correctness
  gates.
- Complete the controlled ZIP compression experiment before the final v0.2.0
  release.

## 0.2.0-rc.1 - 2026-07-25

### Added

- Windows and Ubuntu public CI with pinned Actions, locked Cargo, frozen uv,
  synthetic end-to-end checks, and protected contract baselines.
- A typed Rust application boundary and deterministic `--version-json` build
  identity.
- Versioned configuration with strict validation, semantic SHA-256, and sealed
  workbook/anomaly contracts.
- Optional success/failure `RunManifestV1`, exact input/output hashes, path
  redaction, and published schema/golden examples.
- Windows release packaging with internal and external SHA-256 files.

### Changed

- Workbook and Manifest outputs now use same-directory temporary files,
  flush/sync, and no-overwrite atomic publication.
- Rust remains the only production implementation; Python remains an
  oracle/legacy regression path.

### Compatibility

- The default three-Sheet workbook, CLI stdout/stderr JSON, exit codes, quality
  metrics, error-log semantics, GB/SK rules, and Decimal behavior remain
  compatible with the frozen v0.1 baseline.

### Known issues

- This is a release candidate. The two required performance experiments are not
  yet complete, so this package must not be relabeled as the final v0.2.0
  artifact.
- Forced process termination or power loss can leave diagnostic temporary
  files. Final output paths still never expose partial workbooks.
