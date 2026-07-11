[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('Normal', 'CheckOnly')]
    [string] $Mode,

    [Parameter(Mandatory)]
    [ValidateSet('gb', 'sk')]
    [string] $Pipeline,

    [Parameter(Mandatory)]
    [string] $InputPath,

    [Parameter(Mandatory)]
    [string] $Executable,

    [Parameter(Mandatory)]
    [ValidateSet('reference', 'candidate')]
    [string] $Role,

    [Parameter(Mandatory)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string] $BatchId,

    [Parameter(Mandatory)]
    [ValidateRange(1, 10)]
    [int] $GlobalRound,

    [string] $OutputPath = '',

    [Parameter(Mandatory)]
    [string] $LocalLogRoot,

    [Parameter(Mandatory)]
    [string] $LocalResultPath
)

$ErrorActionPreference = 'Stop'
$PollMilliseconds = 50
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$TrustedLocalRoot = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot 'rust\target\perf-local'))
$Utf8NoBom = New-Object -TypeName System.Text.UTF8Encoding -ArgumentList $false

function Resolve-RequiredFile([string] $Path, [string] $Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label does not exist or is not a file: $Path"
    }
    return (Resolve-Path -LiteralPath $Path).Path
}

function Assert-TrustedLocalPath([string] $Path, [string] $Label) {
    $full = [System.IO.Path]::GetFullPath($Path)
    $rootWithSeparator = $TrustedLocalRoot.TrimEnd('\') + '\'
    if (-not $full.StartsWith($rootWithSeparator, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "$Label must stay below the trusted ignored local root"
    }
    return $full
}

function ConvertTo-WindowsCommandLineArgument([string] $Value) {
    if ($Value -notmatch '[\s"]') {
        return $Value
    }
    $builder = New-Object System.Text.StringBuilder
    [void] $builder.Append('"')
    $backslashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') {
            $backslashes += 1
            continue
        }
        if ($character -eq '"') {
            [void] $builder.Append(('\' * (($backslashes * 2) + 1)))
            [void] $builder.Append('"')
        }
        else {
            [void] $builder.Append(('\' * $backslashes))
            [void] $builder.Append($character)
        }
        $backslashes = 0
    }
    [void] $builder.Append(('\' * ($backslashes * 2)))
    [void] $builder.Append('"')
    return $builder.ToString()
}

function Get-BytesSha256([byte[]] $Bytes) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return -join ($sha.ComputeHash($Bytes) | ForEach-Object { $_.ToString('x2') })
    }
    finally {
        $sha.Dispose()
    }
}

function Get-FileSha256([string] $Path) {
    $stream = [System.IO.File]::OpenRead($Path)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        return -join ($sha.ComputeHash($stream) | ForEach-Object { $_.ToString('x2') })
    }
    finally {
        $sha.Dispose()
        $stream.Dispose()
    }
}

function Write-CreateNewUtf8([string] $Path, [string] $Content) {
    $parent = Split-Path -Parent $Path
    if ($parent) {
        [void] (New-Item -ItemType Directory -Force -Path $parent)
    }
    $bytes = $Utf8NoBom.GetBytes($Content)
    $stream = New-Object System.IO.FileStream(
        $Path,
        [System.IO.FileMode]::CreateNew,
        [System.IO.FileAccess]::Write,
        [System.IO.FileShare]::None
    )
    try {
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    }
    finally {
        $stream.Dispose()
    }
    return Get-BytesSha256 $bytes
}

$input = Resolve-RequiredFile $InputPath 'input workbook'
$executablePath = Resolve-RequiredFile $Executable 'benchmark executable'
$inputSha256 = Get-FileSha256 $input
$binarySha256 = Get-FileSha256 $executablePath
$localLogRootPath = Assert-TrustedLocalPath $LocalLogRoot 'local log root'
$localResult = Assert-TrustedLocalPath $LocalResultPath 'local result path'

if ($Mode -eq 'Normal') {
    if ([string]::IsNullOrWhiteSpace($OutputPath)) {
        throw 'Normal mode requires OutputPath'
    }
    $commandArguments = @($Pipeline, '--input', $input, '--output', [System.IO.Path]::GetFullPath($OutputPath), '--benchmark')
}
else {
    if (-not [string]::IsNullOrWhiteSpace($OutputPath)) {
        throw 'CheckOnly mode forbids OutputPath'
    }
    $commandArguments = @($Pipeline, '--input', $input, '--check-only', '--benchmark')
}

$logDirectory = Join-Path $localLogRootPath (Join-Path $BatchId $GlobalRound)
$stdoutPath = Join-Path $logDirectory "$Role.stdout.log"
$stderrPath = Join-Path $logDirectory "$Role.stderr.log"
foreach ($createNewPath in @($stdoutPath, $stderrPath, $localResult)) {
    if (Test-Path -LiteralPath $createNewPath) {
        throw "create-new local artifact already exists: $createNewPath"
    }
}

$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $executablePath
$startInfo.Arguments = (($commandArguments | ForEach-Object { ConvertTo-WindowsCommandLineArgument ([string] $_) }) -join ' ')
$startInfo.WorkingDirectory = $RepoRoot
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $true
$startInfo.RedirectStandardOutput = $true
$startInfo.RedirectStandardError = $true

$process = New-Object System.Diagnostics.Process
$process.StartInfo = $startInfo
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
if (-not $process.Start()) {
    throw 'failed to start benchmark process'
}
$stdoutTask = $process.StandardOutput.ReadToEndAsync()
$stderrTask = $process.StandardError.ReadToEndAsync()
$peakBytes = [long] 0
while (-not $process.HasExited) {
    $process.Refresh()
    if ($process.PeakWorkingSet64 -gt $peakBytes) {
        $peakBytes = [long] $process.PeakWorkingSet64
    }
    Start-Sleep -Milliseconds $PollMilliseconds
}
$process.WaitForExit()
$stopwatch.Stop()
try {
    $process.Refresh()
    if ($process.PeakWorkingSet64 -gt $peakBytes) {
        $peakBytes = [long] $process.PeakWorkingSet64
    }
}
catch {
    # PeakWorkingSet64 is a kernel-maintained cumulative peak; the last successful refresh remains authoritative.
}
$stdout = $stdoutTask.Result
$stderr = $stderrTask.Result
$exitCode = $process.ExitCode
$process.Dispose()

$stdoutSha256 = Write-CreateNewUtf8 $stdoutPath $stdout
$stderrSha256 = Write-CreateNewUtf8 $stderrPath $stderr
$combinedLogSha256 = Get-BytesSha256 $Utf8NoBom.GetBytes("$stdoutSha256`n$stderrSha256")

$result = [ordered]@{
    mode = $Mode
    pipeline = $Pipeline
    role = $Role
    batch_id = $BatchId
    global_round = $GlobalRound
    exit_code = $exitCode
    external_wall_seconds = $stopwatch.Elapsed.TotalSeconds.ToString('R', [System.Globalization.CultureInfo]::InvariantCulture)
    peak_working_set_bytes = $peakBytes
    input_sha256 = $inputSha256
    binary_sha256 = $binarySha256
    command_arguments = $commandArguments
    stdout_log_sha256 = $stdoutSha256
    stderr_log_sha256 = $stderrSha256
    local_unversioned_log_sha256 = $combinedLogSha256
}
$resultJson = $result | ConvertTo-Json -Depth 4 -Compress
[void] (Write-CreateNewUtf8 $localResult $resultJson)

if ($exitCode -ne 0) {
    exit $exitCode
}
if ($peakBytes -le 0) {
    throw 'PeakWorkingSet64 must be positive'
}
