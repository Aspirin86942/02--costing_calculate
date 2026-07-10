[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('gb', 'sk')]
    [string] $Pipeline,

    [Parameter(Mandatory)]
    [string] $InputPath,

    [Parameter(Mandatory)]
    [ValidatePattern('^[a-z0-9-]+$')]
    [string] $Label,

    [Parameter(Mandatory)]
    [string] $ResultPath
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
if (-not (Test-Path -LiteralPath $InputPath -PathType Leaf)) {
    throw "input workbook does not exist: $InputPath"
}
$input = (Resolve-Path -LiteralPath $InputPath).Path
$result = if ([System.IO.Path]::IsPathRooted($ResultPath)) {
    [System.IO.Path]::GetFullPath($ResultPath)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $ResultPath))
}
$target = Join-Path $RepoRoot "rust\target\perf\profile-$Label"
$oldTarget = $env:CARGO_TARGET_DIR
$oldDebug = $env:CARGO_PROFILE_RELEASE_DEBUG
$traceStarted = $false
try {
    if (-not (Get-Command wpr.exe -ErrorAction SilentlyContinue)) {
        throw 'wpr.exe is not available; install Windows Performance Toolkit or use the documented profiler fallback'
    }
    $env:CARGO_TARGET_DIR = $target
    $env:CARGO_PROFILE_RELEASE_DEBUG = 'true'
    & cargo build --release --manifest-path (Join-Path $RepoRoot 'rust\Cargo.toml') -p costing-calculate
    if ($LASTEXITCODE -ne 0) { throw "profiling build failed with exit code $LASTEXITCODE" }
    $executable = Join-Path $target 'release\costing-calculate.exe'
    if (-not (Test-Path -LiteralPath $executable -PathType Leaf)) {
        throw "profiling executable is missing: $executable"
    }
    [void] (New-Item -ItemType Directory -Force -Path (Split-Path -Parent $result))
    & wpr.exe -start CPU -filemode
    if ($LASTEXITCODE -ne 0) { throw "WPR start failed with exit code $LASTEXITCODE" }
    $traceStarted = $true
    & $executable $Pipeline --input $input --check-only --benchmark | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "profiling run failed with exit code $LASTEXITCODE" }
    & wpr.exe -stop $result
    if ($LASTEXITCODE -ne 0) { throw "WPR stop failed with exit code $LASTEXITCODE" }
    $traceStarted = $false
}
finally {
    if ($traceStarted) { & wpr.exe -cancel | Out-Null }
    if ($null -eq $oldTarget) { Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue }
    else { $env:CARGO_TARGET_DIR = $oldTarget }
    if ($null -eq $oldDebug) { Remove-Item Env:CARGO_PROFILE_RELEASE_DEBUG -ErrorAction SilentlyContinue }
    else { $env:CARGO_PROFILE_RELEASE_DEBUG = $oldDebug }
}
