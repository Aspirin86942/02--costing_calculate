# Costing Calculate for Windows

This package contains the production Rust CLI. It does not require a Rust or
Python installation.

## Verify

From PowerShell, compare the executable hash with `SHA256SUMS`:

```powershell
Get-FileHash .\costing-calculate.exe -Algorithm SHA256
```

The release download also includes a `.zip.sha256` file for verifying the ZIP
before extraction.

## Run

```powershell
.\costing-calculate.exe --help
.\costing-calculate.exe --version-json
.\costing-calculate.exe gb --check-only --input C:\path\to\gb-input.xlsx
.\costing-calculate.exe sk --input C:\path\to\sk-input.xlsx --output C:\path\to\result.xlsx
```

See `examples/run-examples.txt` for configuration, Manifest, and redaction
examples. Existing output workbooks and Manifests are never overwritten.

Keep the executable, default configuration, configuration schema, and Manifest
schema from the same release together when deploying or rolling back.
