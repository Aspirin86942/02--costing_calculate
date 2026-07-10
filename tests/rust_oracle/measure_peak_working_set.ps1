[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('gb', 'sk')]
    [string] $Pipeline,

    [Parameter(Mandatory)]
    [string] $InputPath,

    [Parameter(Mandatory)]
    [string] $BaselineExecutable,

    [string] $CurrentExecutable = '',

    [Parameter(Mandatory)]
    [string] $ResultPath
)

$ErrorActionPreference = 'Stop'
$Warmups = 1
$Rounds = 5
$PollMilliseconds = 50
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

function Resolve-RequiredFile([string] $Path, [string] $Label) {
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "$Label does not exist or is not a file: $Path"
    }
    return (Resolve-Path -LiteralPath $Path).Path
}

function Get-Median([long[]] $Values) {
    if ($Values.Count -ne $Rounds) {
        throw "expected $Rounds values, got $($Values.Count)"
    }
    $sorted = @($Values | Sort-Object)
    return [long] $sorted[[int][Math]::Floor($sorted.Count / 2)]
}

function Get-TextSha256([string] $Value) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
        $hash = $sha.ComputeHash($bytes)
        return -join ($hash | ForEach-Object { $_.ToString('x2') })
    }
    finally {
        $sha.Dispose()
    }
}

function Invoke-PeakSample([string] $Executable, [string] $InputWorkbook) {
    if ($InputWorkbook.Contains('"')) {
        throw "input workbook path cannot contain a quote: $InputWorkbook"
    }
    $arguments = @(
        $Pipeline,
        '--input',
        ('"{0}"' -f $InputWorkbook),
        '--check-only',
        '--benchmark'
    )
    $process = Start-Process `
        -FilePath $Executable `
        -ArgumentList $arguments `
        -WorkingDirectory $RepoRoot `
        -WindowStyle Hidden `
        -PassThru
    $peakBytes = [long] 0
    while (-not $process.HasExited) {
        $process.Refresh()
        if ($process.PeakWorkingSet64 -gt $peakBytes) {
            $peakBytes = [long] $process.PeakWorkingSet64
        }
        Start-Sleep -Milliseconds $PollMilliseconds
    }
    $process.WaitForExit()
    $process.Refresh()
    if ($process.PeakWorkingSet64 -gt $peakBytes) {
        $peakBytes = [long] $process.PeakWorkingSet64
    }
    if ($process.ExitCode -ne 0) {
        throw "benchmark process failed with exit code $($process.ExitCode): $Executable"
    }
    return $peakBytes
}

$input = Resolve-RequiredFile $InputPath 'input workbook'
$baseline = Resolve-RequiredFile $BaselineExecutable 'baseline executable'
$inputSha256 = (Get-FileHash -LiteralPath $input -Algorithm SHA256).Hash.ToLowerInvariant()
$baselineSha256 = (Get-FileHash -LiteralPath $baseline -Algorithm SHA256).Hash.ToLowerInvariant()
$current = if ($CurrentExecutable) {
    Resolve-RequiredFile $CurrentExecutable 'current executable'
}
else {
    $null
}
$currentSha256 = if ($null -ne $current) {
    (Get-FileHash -LiteralPath $current -Algorithm SHA256).Hash.ToLowerInvariant()
}
else {
    $null
}
$commandArguments = @(
    $Pipeline,
    '--input',
    $input,
    '--check-only',
    '--benchmark'
)

for ($index = 0; $index -lt $Warmups; $index++) {
    [void] (Invoke-PeakSample $baseline $input)
    if ($null -ne $current) {
        [void] (Invoke-PeakSample $current $input)
    }
}

$baselineValues = [System.Collections.Generic.List[long]]::new()
$currentValues = [System.Collections.Generic.List[long]]::new()
for ($round = 1; $round -le $Rounds; $round++) {
    if (($round % 2 -eq 1) -or ($null -eq $current)) {
        $baselineValues.Add((Invoke-PeakSample $baseline $input))
        if ($null -ne $current) {
            $currentValues.Add((Invoke-PeakSample $current $input))
        }
    }
    else {
        $currentValues.Add((Invoke-PeakSample $current $input))
        $baselineValues.Add((Invoke-PeakSample $baseline $input))
    }
}

$baselineMedian = Get-Median $baselineValues.ToArray()
if ($baselineMedian -le 0) {
    throw "baseline peak working set median must be positive, got $baselineMedian"
}
$currentMedian = if ($null -ne $current) { Get-Median $currentValues.ToArray() } else { $null }
$ratio = if ($null -ne $currentMedian) { [double] $currentMedian / [double] $baselineMedian } else { $null }
$verdict = if ($null -eq $currentMedian) {
    'BASELINE_RECORDED'
}
elseif ($ratio -le 1.05) {
    'VALIDATED'
}
else {
    'MEMORY_REGRESSION'
}

$inputSha256After = (Get-FileHash -LiteralPath $input -Algorithm SHA256).Hash.ToLowerInvariant()
if ($inputSha256After -ne $inputSha256) {
    throw "input workbook SHA-256 changed during sampling: $input"
}
if ((Get-FileHash -LiteralPath $baseline -Algorithm SHA256).Hash.ToLowerInvariant() -ne $baselineSha256) {
    throw "baseline executable SHA-256 changed during sampling: $baseline"
}
if (
    ($null -ne $current) -and
    ((Get-FileHash -LiteralPath $current -Algorithm SHA256).Hash.ToLowerInvariant() -ne $currentSha256)
) {
    throw "current executable SHA-256 changed during sampling: $current"
}
$workingTreeStatus = (& git -C $RepoRoot status --porcelain=v1 | Out-String).Trim()
$workingTreeDiff = (& git -C $RepoRoot diff --binary HEAD -- | Out-String).Trim()
$workingTreeState = $workingTreeStatus + "`n" + $workingTreeDiff
$result = [ordered]@{
    pipeline = $Pipeline
    input_path = $input
    input_sha256 = $inputSha256
    baseline_executable = $baseline
    baseline_sha256 = $baselineSha256
    current_executable = $current
    current_sha256 = $currentSha256
    git_head = (& git -C $RepoRoot rev-parse HEAD).Trim()
    working_tree_diff_id = Get-TextSha256 $workingTreeState
    working_directory = $RepoRoot
    command_arguments = $commandArguments
    warmups = $Warmups
    rounds = $Rounds
    poll_milliseconds = $PollMilliseconds
    baseline_peak_working_set_bytes = $baselineValues.ToArray()
    current_peak_working_set_bytes = $currentValues.ToArray()
    baseline_median_bytes = $baselineMedian
    current_median_bytes = $currentMedian
    current_to_baseline_ratio = $ratio
    verdict = $verdict
}

$resultParent = Split-Path -Parent $ResultPath
if ($resultParent) {
    [void] (New-Item -ItemType Directory -Force -Path $resultParent)
}
$resultJson = $result | ConvertTo-Json -Depth 6
$utf8NoBom = New-Object -TypeName System.Text.UTF8Encoding -ArgumentList $false
[System.IO.File]::WriteAllText($ResultPath, $resultJson, $utf8NoBom)
