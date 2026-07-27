[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ArchivePath,
    [Parameter(Mandatory = $true)]
    [string]$ChecksumPath,
    [Parameter(Mandatory = $true)]
    [string]$GbInput,
    [Parameter(Mandatory = $true)]
    [string]$SkInput,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^v[0-9]+\.[0-9]+\.[0-9]+(?:-rc\.[1-9][0-9]*)?$')]
    [string]$ExpectedReleaseLabel,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$ExpectedCommit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($env:OS -ne 'Windows_NT') {
    throw 'Windows package smoke must run on Windows.'
}

function Resolve-RequiredFile {
    param([Parameter(Mandatory = $true)][string]$Path)

    $resolved = Resolve-Path -LiteralPath $Path -ErrorAction Stop
    if (-not (Test-Path -LiteralPath $resolved.Path -PathType Leaf)) {
        throw "Required file does not exist: $Path"
    }
    return $resolved.Path
}

function Invoke-IsolatedExecutable {
    param(
        [Parameter(Mandatory = $true)][string]$Executable,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory
    )

    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Executable
    foreach ($argument in $Arguments) {
        $startInfo.ArgumentList.Add($argument)
    }
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [Text.UTF8Encoding]::new($false)
    $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false)
    $startInfo.Environment.Clear()
    $startInfo.Environment['SystemRoot'] = $env:SystemRoot
    $startInfo.Environment['WINDIR'] = $env:WINDIR
    $startInfo.Environment['TEMP'] = $WorkingDirectory
    $startInfo.Environment['TMP'] = $WorkingDirectory
    $startInfo.Environment['PATH'] = "$env:SystemRoot\System32;$env:SystemRoot"

    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) {
            throw "Failed to start packaged executable: $Executable"
        }
        $stdout = $process.StandardOutput.ReadToEnd()
        $stderr = $process.StandardError.ReadToEnd()
        $process.WaitForExit()
        return [pscustomobject]@{
            exit_code = $process.ExitCode
            stdout = $stdout
            stderr = $stderr
        }
    }
    finally {
        $process.Dispose()
    }
}

$archive = Resolve-RequiredFile $ArchivePath
$externalChecksum = Resolve-RequiredFile $ChecksumPath
$inputs = [ordered]@{
    gb = Resolve-RequiredFile $GbInput
    sk = Resolve-RequiredFile $SkInput
}

$checksumText = (Get-Content -LiteralPath $externalChecksum -Raw).Trim()
if ($checksumText -notmatch '^([0-9a-f]{64})  ([^\\/]+\.zip)$') {
    throw 'External ZIP checksum has an invalid format.'
}
$expectedArchiveHash = $Matches[1]
if ($Matches[2] -ne [IO.Path]::GetFileName($archive)) {
    throw 'External ZIP checksum names a different archive.'
}
$actualArchiveHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualArchiveHash -ne $expectedArchiveHash) {
    throw 'Release ZIP SHA-256 does not match its checksum file.'
}

$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar
)
$smokeRoot = Join-Path $tempRoot "costing-package-smoke-$([Guid]::NewGuid().ToString('N'))"
$smokeRoot = [IO.Path]::GetFullPath($smokeRoot)
$safePrefix = "$tempRoot$([IO.Path]::DirectorySeparatorChar)costing-package-smoke-"
if (-not $smokeRoot.StartsWith($safePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Refusing to create an unsafe smoke-test directory.'
}

New-Item -ItemType Directory -Path $smokeRoot | Out-Null
try {
    Expand-Archive -LiteralPath $archive -DestinationPath $smokeRoot
    $packageName = "costing-calculate-$ExpectedReleaseLabel-windows-x86_64"
    $packageRoot = Join-Path $smokeRoot $packageName
    if (-not (Test-Path -LiteralPath $packageRoot -PathType Container)) {
        throw "Release ZIP does not contain the expected root directory: $packageName"
    }

    $requiredFiles = @(
        'costing-calculate.exe',
        'README.md',
        'CHANGELOG.md',
        'config/costing.default.toml',
        'config/costing.schema.json',
        'schemas/run-manifest-v1.schema.json',
        'examples/run-examples.txt',
        'SHA256SUMS'
    )
    $actualFiles = @(
        Get-ChildItem -LiteralPath $packageRoot -Recurse -File |
            ForEach-Object {
                $_.FullName.Substring($packageRoot.Length).TrimStart('\', '/').Replace('\', '/')
            } |
            Sort-Object
    )
    $expectedFiles = @($requiredFiles | Sort-Object)
    if (Compare-Object -ReferenceObject $expectedFiles -DifferenceObject $actualFiles) {
        throw "Release package file layout differs.`nActual: $($actualFiles -join ', ')"
    }

    $internalChecksums = @{}
    foreach ($line in Get-Content -LiteralPath (Join-Path $packageRoot 'SHA256SUMS')) {
        if ($line -notmatch '^([0-9a-f]{64})  (.+)$') {
            throw "Invalid SHA256SUMS line: $line"
        }
        $relativePath = $Matches[2]
        if ($internalChecksums.ContainsKey($relativePath)) {
            throw "Duplicate SHA256SUMS entry: $relativePath"
        }
        $internalChecksums[$relativePath] = $Matches[1]
    }
    $expectedChecksumPaths = @($requiredFiles | Where-Object { $_ -ne 'SHA256SUMS' } | Sort-Object)
    $actualChecksumPaths = @($internalChecksums.Keys | Sort-Object)
    if (Compare-Object -ReferenceObject $expectedChecksumPaths -DifferenceObject $actualChecksumPaths) {
        throw "SHA256SUMS entries differ.`nActual: $($actualChecksumPaths -join ', ')"
    }
    foreach ($relativePath in $expectedChecksumPaths) {
        if (-not $internalChecksums.ContainsKey($relativePath)) {
            throw "SHA256SUMS is missing $relativePath"
        }
        $nativeRelativePath = $relativePath.Replace('/', [IO.Path]::DirectorySeparatorChar)
        $fullPath = [IO.Path]::GetFullPath((Join-Path $packageRoot $nativeRelativePath))
        $packagePrefix = "$([IO.Path]::GetFullPath($packageRoot))$([IO.Path]::DirectorySeparatorChar)"
        if (-not $fullPath.StartsWith($packagePrefix, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Unsafe SHA256SUMS path: $relativePath"
        }
        $actualHash = (Get-FileHash -LiteralPath $fullPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualHash -ne $internalChecksums[$relativePath]) {
            throw "Packaged file hash differs: $relativePath"
        }
    }

    $executable = Join-Path $packageRoot 'costing-calculate.exe'
    $help = Invoke-IsolatedExecutable $executable @('--help') $packageRoot
    if ($help.exit_code -ne 0 -or $help.stdout -notmatch 'Usage:') {
        throw "Packaged --help failed: $($help.stderr)"
    }

    $versionResult = Invoke-IsolatedExecutable $executable @('--version-json') $packageRoot
    if ($versionResult.exit_code -ne 0) {
        throw "Packaged --version-json failed: $($versionResult.stderr)"
    }
    $version = $versionResult.stdout | ConvertFrom-Json
    $baseVersion = ($ExpectedReleaseLabel.TrimStart('v') -split '-')[0]
    if (
        $version.version -ne $baseVersion -or
        $version.git_commit -ne $ExpectedCommit -or
        $version.rustc_version -notmatch '^rustc 1\.96\.0 ' -or
        $version.target -ne 'x86_64-pc-windows-msvc'
    ) {
        throw "Packaged build identity differs: $($versionResult.stdout)"
    }

    $pipelineResults = @()
    foreach ($pipeline in @('gb', 'sk')) {
        $workDirectory = Join-Path $smokeRoot "work-$pipeline"
        New-Item -ItemType Directory -Path $workDirectory | Out-Null
        $manifestPath = Join-Path $workDirectory "$pipeline-manifest.json"

        $configResult = Invoke-IsolatedExecutable $executable @(
            $pipeline,
            '--validate-config'
        ) $workDirectory
        if ($configResult.exit_code -ne 0) {
            throw "$pipeline packaged config validation failed: $($configResult.stderr)"
        }

        $checkResult = Invoke-IsolatedExecutable $executable @(
            $pipeline,
            '--input',
            $inputs[$pipeline],
            '--check-only',
            '--summary-output',
            $manifestPath,
            '--redact-paths'
        ) $workDirectory
        if ($checkResult.exit_code -ne 0) {
            throw "$pipeline packaged check-only failed: $($checkResult.stderr)"
        }
        $summary = $checkResult.stdout | ConvertFrom-Json
        $manifest = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
        if (
            $summary.status -ne 'succeeded' -or
            $summary.pipeline -ne $pipeline -or
            $summary.output_written -ne $false -or
            $manifest.status -ne 'succeeded' -or
            $manifest.result.output_written -ne $false
        ) {
            throw "$pipeline packaged check-only returned an unexpected contract."
        }

        $outputPath = Join-Path $workDirectory "$pipeline-output.xlsx"
        $normalManifestPath = Join-Path $workDirectory "$pipeline-normal-manifest.json"
        $normalResult = Invoke-IsolatedExecutable $executable @(
            $pipeline,
            '--input',
            $inputs[$pipeline],
            '--output',
            $outputPath,
            '--summary-output',
            $normalManifestPath,
            '--redact-paths'
        ) $workDirectory
        if ($normalResult.exit_code -ne 0) {
            throw "$pipeline packaged workbook run failed: $($normalResult.stderr)"
        }
        $normalSummary = $normalResult.stdout | ConvertFrom-Json
        $normalManifest = Get-Content -LiteralPath $normalManifestPath -Raw | ConvertFrom-Json
        $expectedSheets = @('成本计算单总表', '成本计算单数量聚合维度', '成本分析工单维度')
        $actualSheets = @($normalManifest.result.sheet_names)
        if (
            $normalSummary.status -ne 'succeeded' -or
            $normalSummary.output_written -ne $true -or
            $normalManifest.status -ne 'succeeded' -or
            $normalManifest.result.output_written -ne $true -or
            $normalManifest.result.final_output_valid -ne $true -or
            -not (Test-Path -LiteralPath $outputPath -PathType Leaf) -or
            (Compare-Object -ReferenceObject $expectedSheets -DifferenceObject $actualSheets -SyncWindow 0)
        ) {
            throw "$pipeline packaged workbook run returned an unexpected contract."
        }
        $actualOutputHash = (Get-FileHash -LiteralPath $outputPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualOutputHash -ne $normalManifest.result.output_sha256) {
            throw "$pipeline packaged workbook hash differs from its Manifest."
        }
        $pipelineResults += [pscustomobject]@{
            pipeline = $pipeline
            reader_rows = $manifest.input.reader_rows
            input_sha256 = $manifest.input.sha256
            output_size_bytes = $normalManifest.result.output_size_bytes
            output_sha256 = $normalManifest.result.output_sha256
        }
    }

    [pscustomobject]@{
        status = 'passed'
        release_label = $ExpectedReleaseLabel
        source_commit = $ExpectedCommit
        archive_sha256 = $actualArchiveHash
        executable_sha256 = $internalChecksums['costing-calculate.exe']
        isolated_path = "$env:SystemRoot\System32;$env:SystemRoot"
        pipelines = $pipelineResults
    } | ConvertTo-Json -Depth 5
}
finally {
    if (
        (Test-Path -LiteralPath $smokeRoot) -and
        $smokeRoot.StartsWith($safePrefix, [StringComparison]::OrdinalIgnoreCase)
    ) {
        Remove-Item -LiteralPath $smokeRoot -Recurse -Force
    }
}
