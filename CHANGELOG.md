# Changelog

All notable changes to Costing Calculate are documented in this file.

## Unreleased

## 0.2.0 - 2026-07-25

### Added

- Windows and Ubuntu public CI with pinned Actions, locked Cargo, frozen uv,
  synthetic end-to-end checks, and protected contract baselines.
- A typed Rust application boundary and deterministic `--version-json` build
  identity.
- Versioned strict configuration, a closed schema, semantic SHA-256, effective
  source reporting, and sealed business/engineering fields.
- Optional success/failure `RunManifestV1` with exact input/output identities,
  configuration identity, quality/count/timing evidence, and path redaction.
- Verified self-contained Windows packaging with internal and external
  SHA-256 files.

### Changed

- Workbook and Manifest outputs now use same-directory temporary files,
  flush/sync, and no-overwrite atomic publication.
- Comparison-only normalize paths borrow cell text while period and Decimal
  conversions retain their owned semantics.
- Rust remains the only production implementation; Python remains an
  oracle/legacy regression path.

### Performance

- Adopt comparison-only borrowed `cell_text` access after the controlled SK
  check-only and normal-run experiments passed all performance and correctness
  gates.
- Retain ZIP compression Level 5 after controlled Level 3 and Level 2
  experiments improved save time but exceeded the frozen SK output-size gate.

### Compatibility

- The default three-Sheet workbook, CLI stdout/stderr JSON, exit codes, quality
  metrics, error-log semantics, GB/SK rules, Decimal behavior, and no-overwrite
  policy remain compatible with the frozen v0.1 baseline.

### Known limitations

- Forced process termination or power loss can leave diagnostic temporary
  files. Final output paths still never expose partial workbooks.
- Bit-for-bit ZIP reproducibility, `doctor`, human logs, private scheduled
  runners, additional performance experiments, and Python retirement are not
  part of v0.2.0.

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
