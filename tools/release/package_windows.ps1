[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^v[0-9]+\.[0-9]+\.[0-9]+(?:-rc\.[1-9][0-9]*)?$')]
    [string]$ReleaseLabel,

    [string]$OutputDirectory = 'dist',
    [string]$SourceCommit = '',
    [Nullable[long]]$SourceDateEpoch = $null,
    [switch]$SkipBuild,
    [string]$BinaryPath = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($env:OS -ne 'Windows_NT') {
    throw 'Windows release packages must be built on Windows.'
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$toolchainPath = Join-Path $repoRoot 'rust-toolchain.toml'
$toolchainText = Get-Content -LiteralPath $toolchainPath -Raw
if ($toolchainText -notmatch 'channel\s*=\s*"([^"]+)"') {
    throw 'rust-toolchain.toml does not declare a channel.'
}
$toolchainVersion = $Matches[1]
if ($toolchainVersion -ne '1.96.0') {
    throw "Release toolchain must be exactly 1.96.0; found $toolchainVersion."
}

$headCommit = (& git -C $repoRoot rev-parse HEAD).Trim().ToLowerInvariant()
if ($LASTEXITCODE -ne 0 -or $headCommit -notmatch '^[0-9a-f]{40}$') {
    throw 'Unable to resolve the source commit.'
}
if ([string]::IsNullOrWhiteSpace($SourceCommit)) {
    $SourceCommit = $headCommit
}
$SourceCommit = $SourceCommit.Trim().ToLowerInvariant()
if ($SourceCommit -notmatch '^[0-9a-f]{40}$' -or $SourceCommit -ne $headCommit) {
    throw 'SourceCommit must be the full hexadecimal commit currently checked out.'
}

$dirtyPaths = @(& git -C $repoRoot status --porcelain --untracked-files=all)
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to inspect the Git worktree.'
}
if ($dirtyPaths.Count -gt 0) {
    throw "Release packaging requires a clean worktree.`n$($dirtyPaths -join "`n")"
}

$epoch = if ($null -ne $SourceDateEpoch) {
    [long]$SourceDateEpoch
}
else {
    [long]((& git -C $repoRoot show -s --format=%ct $SourceCommit).Trim())
}
if ($LASTEXITCODE -ne 0 -or $epoch -lt 0) {
    throw 'SourceDateEpoch must be non-negative Unix seconds.'
}
$expectedTimestamp = [DateTimeOffset]::FromUnixTimeSeconds($epoch).UtcDateTime.ToString(
    'yyyy-MM-ddTHH:mm:ssZ',
    [Globalization.CultureInfo]::InvariantCulture
)

if (-not $SkipBuild) {
    $previousCommit = [Environment]::GetEnvironmentVariable('COSTING_GIT_COMMIT', 'Process')
    $previousEpoch = [Environment]::GetEnvironmentVariable('SOURCE_DATE_EPOCH', 'Process')
    try {
        $env:COSTING_GIT_COMMIT = $SourceCommit
        $env:SOURCE_DATE_EPOCH = $epoch.ToString([Globalization.CultureInfo]::InvariantCulture)
        Push-Location $repoRoot
        try {
            # Contract command: cargo build --release --locked
            & cargo build --release --locked --manifest-path rust/Cargo.toml -p costing-calculate
            if ($LASTEXITCODE -ne 0) {
                throw "Cargo release build failed with exit code $LASTEXITCODE."
            }
        }
        finally {
            Pop-Location
        }
    }
    finally {
        [Environment]::SetEnvironmentVariable('COSTING_GIT_COMMIT', $previousCommit, 'Process')
        [Environment]::SetEnvironmentVariable('SOURCE_DATE_EPOCH', $previousEpoch, 'Process')
    }
}

$binary = if ([string]::IsNullOrWhiteSpace($BinaryPath)) {
    Join-Path $repoRoot 'rust\target\release\costing-calculate.exe'
}
elseif ([IO.Path]::IsPathRooted($BinaryPath)) {
    [IO.Path]::GetFullPath($BinaryPath)
}
else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $BinaryPath))
}
if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
    throw "Release executable does not exist: $binary"
}

$versionText = (& $binary --version-json) -join "`n"
if ($LASTEXITCODE -ne 0) {
    throw 'Release executable --version-json failed.'
}
$version = $versionText | ConvertFrom-Json
$baseVersion = ($ReleaseLabel.TrimStart('v') -split '-')[0]
$actualBuildTimestamp = if ($version.build_timestamp -is [DateTime]) {
    ([DateTime]$version.build_timestamp).ToUniversalTime().ToString(
        'yyyy-MM-ddTHH:mm:ssZ',
        [Globalization.CultureInfo]::InvariantCulture
    )
}
elseif ($version.build_timestamp -is [DateTimeOffset]) {
    ([DateTimeOffset]$version.build_timestamp).UtcDateTime.ToString(
        'yyyy-MM-ddTHH:mm:ssZ',
        [Globalization.CultureInfo]::InvariantCulture
    )
}
else {
    [DateTimeOffset]::ParseExact(
        [string]$version.build_timestamp,
        'yyyy-MM-ddTHH:mm:ssZ',
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::AssumeUniversal -bor [Globalization.DateTimeStyles]::AdjustToUniversal
    ).UtcDateTime.ToString('yyyy-MM-ddTHH:mm:ssZ', [Globalization.CultureInfo]::InvariantCulture)
}
if (
    $version.version -ne $baseVersion -or
    $version.git_commit -ne $SourceCommit -or
    $actualBuildTimestamp -ne $expectedTimestamp -or
    $version.rustc_version -notmatch '^rustc 1\.96\.0 ' -or
    $version.target -ne 'x86_64-pc-windows-msvc'
) {
    throw "Release executable build identity does not match the requested source.`n$versionText"
}

$outputRoot = if ([IO.Path]::IsPathRooted($OutputDirectory)) {
    [IO.Path]::GetFullPath($OutputDirectory)
}
else {
    [IO.Path]::GetFullPath((Join-Path $repoRoot $OutputDirectory))
}
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

$packageName = "costing-calculate-$ReleaseLabel-windows-x86_64"
$packageRoot = Join-Path $outputRoot $packageName
$archivePath = Join-Path $outputRoot "$packageName.zip"
$archiveChecksumPath = "$archivePath.sha256"
foreach ($target in @($packageRoot, $archivePath, $archiveChecksumPath)) {
    if (Test-Path -LiteralPath $target) {
        throw "Release target already exists and will not be overwritten: $target"
    }
}

New-Item -ItemType Directory -Path $packageRoot | Out-Null
$files = [ordered]@{
    'costing-calculate.exe' = $binary
    'README.md' = (Join-Path $repoRoot 'tools\release\README.md')
    'CHANGELOG.md' = (Join-Path $repoRoot 'CHANGELOG.md')
    'config/costing.default.toml' = (Join-Path $repoRoot 'rust\crates\costing-cli\config\costing.default.toml')
    'config/costing.schema.json' = (Join-Path $repoRoot 'rust\crates\costing-cli\config\costing.schema.json')
    'schemas/run-manifest-v1.schema.json' = (
        Join-Path $repoRoot 'rust\crates\costing-cli\config\run-manifest.schema.json'
    )
    'examples/run-examples.txt' = (Join-Path $repoRoot 'tools\release\run-examples.txt')
}

foreach ($relativePath in $files.Keys) {
    $sourcePath = $files[$relativePath]
    if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
        throw "Required release input is missing: $sourcePath"
    }
    $nativeRelativePath = $relativePath.Replace('/', [IO.Path]::DirectorySeparatorChar)
    $destination = Join-Path $packageRoot $nativeRelativePath
    $destinationParent = Split-Path -Parent $destination
    New-Item -ItemType Directory -Force -Path $destinationParent | Out-Null
    Copy-Item -LiteralPath $sourcePath -Destination $destination
}

$checksumLines = foreach ($relativePath in $files.Keys) {
    $nativeRelativePath = $relativePath.Replace('/', [IO.Path]::DirectorySeparatorChar)
    $packagedPath = Join-Path $packageRoot $nativeRelativePath
    $hash = (Get-FileHash -LiteralPath $packagedPath -Algorithm SHA256).Hash.ToLowerInvariant()
    "$hash  $relativePath"
}
$checksumFile = Join-Path $packageRoot 'SHA256SUMS'
[IO.File]::WriteAllLines($checksumFile, $checksumLines, [Text.UTF8Encoding]::new($false))

Compress-Archive -LiteralPath $packageRoot -DestinationPath $archivePath -CompressionLevel Optimal
$archiveHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
[IO.File]::WriteAllText(
    $archiveChecksumPath,
    "$archiveHash  $([IO.Path]::GetFileName($archivePath))`n",
    [Text.UTF8Encoding]::new($false)
)

[pscustomobject]@{
    status = 'packaged'
    release_label = $ReleaseLabel
    source_commit = $SourceCommit
    source_date_epoch = $epoch
    build_timestamp = $expectedTimestamp
    rustc_version = $version.rustc_version
    target = $version.target
    package_directory = $packageRoot
    archive_path = $archivePath
    archive_sha256 = $archiveHash
    executable_sha256 = (
        Get-FileHash -LiteralPath (Join-Path $packageRoot 'costing-calculate.exe') -Algorithm SHA256
    ).Hash.ToLowerInvariant()
} | ConvertTo-Json -Depth 3
