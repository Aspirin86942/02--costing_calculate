# `toml` / `sha2` Production Dependency Review — 2026-07-25

## Decision

**Decision:** the repository owner approved the exact versions and minimal
features below for M3 configuration governance on 2026-07-25.

```toml
toml = { version = "1.1.3", default-features = false, features = ["parse", "serde", "std"] }
sha2 = { version = "0.11.0", default-features = false }
```

The application will use existing `serde`/`serde_json` support, and will not add
a separate hexadecimal-encoding dependency.

## Upstream and license review

| Crate | Exact version | License | MSRV | Published / updated | Upstream |
| --- | --- | --- | ---: | --- | --- |
| `toml` | `1.1.3+spec-1.1.0` | MIT OR Apache-2.0 | 1.85 | 2026-07-14 | [toml-rs/toml](https://github.com/toml-rs/toml) |
| `sha2` | `0.11.0` | MIT OR Apache-2.0 | 1.85 | 2026-03-25 | [RustCrypto/hashes](https://github.com/RustCrypto/hashes) |

Both versions were current, non-yanked crates.io releases when reviewed. The
repositories show ongoing maintenance and use the same permissive dual-license
policy already common in the Rust dependency graph. The project toolchain
(`rustc 1.96.0`) is newer than both declared MSRVs.

Crates.io package checksums:

- `toml 1.1.3+spec-1.1.0`:
  `53c96ecdfa941c8fc4fcaed14f99ada8ebed502eef533015095a07e3301d4c3c`
- `sha2 0.11.0`:
  `446ba717509524cb3f22f17ecc096f10f4822d76ab5c0b9822c5f9c284e825f4`

## Lockfile impact

The candidate dependencies were added only to an isolated detached copy of the
current workspace, preserving the existing `Cargo.lock`, then resolved with
Cargo 1.96.0. Cargo added 10 packages and upgraded or downgraded none:

| Added package | Purpose |
| --- | --- |
| `toml 1.1.3+spec-1.1.0` | Typed TOML deserialization |
| `serde_spanned 1.1.1` | TOML serde support |
| `toml_writer 1.1.2+spec-1.1.0` | Locked optional upstream component |
| `sha2 0.11.0` | SHA-256 implementation |
| `digest 0.11.3` | Hash trait and core implementation support |
| `block-buffer 0.12.1` | Hash block buffering |
| `crypto-common 0.2.2` | Common cryptographic types |
| `hybrid-array 0.4.13` | Fixed-size hash arrays |
| `cpufeatures 0.3.0` | Runtime CPU feature selection |
| `typenum 1.20.1` | Compile-time numeric sizes |

The lockfile grew from 31,826 to 34,206 bytes in that isolated resolution. No
dependency file in the working repository was changed by the review.

## Risk controls

- Versions and transitive checksums remain frozen in `Cargo.lock`; CI uses
  `--locked`.
- `toml` is enabled only for parse/serde/std. Format-preserving editing,
  debug-output and order-preservation features are excluded.
- `sha2` default `alloc` and OID features are excluded; SHA-256 is used only
  for integrity fingerprints, not passwords, signatures or secret material.
- External TOML is decoded as UTF-8 into `#[serde(deny_unknown_fields)]`
  structures and then receives domain validation before any workbook read.
- Effective configuration hashing uses a typed, stable-order semantic JSON
  view rather than raw TOML bytes.
- The release gates rebuild with the locked graph and publish package
  checksums. Any future crate version change requires a new review.

## Residual risk

Adding the two top-level crates expands the locked third-party graph by ten
packages. `toml 1.1.3` was a recent release at review time, so strict golden,
unknown-field, malformed-input and equivalent-config tests are required before
M3 can exit. No vulnerability scanner was installed in the local toolchain;
the review therefore relies on upstream metadata, exact checksums, minimal
features, locked resolution and the repository's full CI gates.
