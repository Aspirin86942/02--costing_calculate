[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$BinaryPath,

    [Parameter(Mandatory = $true)]
    [ValidateSet('gb', 'sk')]
    [string]$Pipeline,

    [Parameter(Mandatory = $true)]
    [string]$InputPath,

    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory,

    [ValidateRange(1, 100)]
    [int]$Iterations = 5,

    [string]$ReportPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-RequiredFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,

        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    $resolved = [System.IO.Path]::GetFullPath($Path)
    if (-not [System.IO.File]::Exists($resolved)) {
        throw "$Label not found: $([System.IO.Path]::GetFileName($resolved))"
    }
    return $resolved
}

function Get-Median {
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$Values
    )

    $sorted = @($Values | Sort-Object)
    $middle = [int][Math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) {
        return $sorted[$middle]
    }
    return ($sorted[$middle - 1] + $sorted[$middle]) / 2
}

$binary = Resolve-RequiredFile -Path $BinaryPath -Label 'binary'
$inputWorkbook = Resolve-RequiredFile -Path $InputPath -Label 'input workbook'
$outputRoot = [System.IO.Path]::GetFullPath($OutputDirectory)
if ([System.IO.File]::Exists($outputRoot)) {
    throw "Output directory points to a file: $([System.IO.Path]::GetFileName($outputRoot))"
}
[System.IO.Directory]::CreateDirectory($outputRoot) | Out-Null

$thresholdsByPipeline = @{
    gb = [ordered]@{
        max_wall_median_seconds = 3.2554
        max_pws_median_bytes = 375700685
        max_output_bytes = 4194321
    }
    sk = [ordered]@{
        max_wall_median_seconds = 20.0
        max_pws_median_bytes = 2147483648
        max_output_bytes = 48658823
    }
}
$thresholds = $thresholdsByPipeline[$Pipeline]
$samples = @()

for ($iteration = 1; $iteration -le $Iterations; $iteration++) {
    $outputPath = [System.IO.Path]::Combine(
        $outputRoot,
        ('{0}-normal-{1:D2}.xlsx' -f $Pipeline, $iteration)
    )
    if ([System.IO.File]::Exists($outputPath)) {
        throw "Performance output already exists: $([System.IO.Path]::GetFileName($outputPath))"
    }

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $binary
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [System.Text.Encoding]::UTF8
    [void]$startInfo.ArgumentList.Add($Pipeline)
    [void]$startInfo.ArgumentList.Add('--input')
    [void]$startInfo.ArgumentList.Add($inputWorkbook)
    [void]$startInfo.ArgumentList.Add('--output')
    [void]$startInfo.ArgumentList.Add($outputPath)
    [void]$startInfo.ArgumentList.Add('--benchmark')
    [void]$startInfo.ArgumentList.Add('--redact-paths')

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    if (-not $process.Start()) {
        throw 'Unable to start performance process.'
    }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $peakWorkingSet = [long]0
    do {
        try {
            $process.Refresh()
            $peakWorkingSet = [Math]::Max(
                $peakWorkingSet,
                [long]$process.PeakWorkingSet64
            )
        }
        catch [System.InvalidOperationException] {
            # The process may exit between the wait and the property read.
        }
        $exited = $process.WaitForExit(10)
    } while (-not $exited)
    $process.WaitForExit()
    try {
        $process.Refresh()
        $peakWorkingSet = [Math]::Max(
            $peakWorkingSet,
            [long]$process.PeakWorkingSet64
        )
    }
    catch [System.InvalidOperationException] {
        # The sampled peak above remains valid after the process has exited.
    }
    $stopwatch.Stop()
    $stdout = $stdoutTask.GetAwaiter().GetResult()
    $stderr = $stderrTask.GetAwaiter().GetResult()
    $exitCode = $process.ExitCode
    $process.Dispose()

    if ($exitCode -ne 0) {
        throw "Performance run $iteration failed with exit code ${exitCode}: $($stderr.Trim())"
    }
    if ($peakWorkingSet -le 0) {
        throw "Performance run $iteration did not produce a Peak Working Set measurement."
    }
    $payload = $stdout | ConvertFrom-Json
    if ($payload.status -ne 'succeeded' -or $payload.output_written -ne $true) {
        throw "Performance run $iteration returned an unexpected summary."
    }
    $outputSize = (Get-Item -LiteralPath $outputPath).Length
    $samples += [pscustomobject][ordered]@{
        iteration = $iteration
        wall_seconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 4)
        peak_working_set_bytes = $peakWorkingSet
        output_size_bytes = $outputSize
        compute_total_seconds = [Math]::Round(
            [double]$payload.stage_timings.stages.total,
            4
        )
    }
}

$wallMedian = [Math]::Round(
    (Get-Median -Values @($samples.wall_seconds)),
    4
)
$pwsMedian = [long][Math]::Round(
    (Get-Median -Values @($samples.peak_working_set_bytes)),
    0
)
$maxOutput = [long]($samples.output_size_bytes | Measure-Object -Maximum).Maximum
$passed = (
    $wallMedian -le $thresholds.max_wall_median_seconds -and
    $pwsMedian -le $thresholds.max_pws_median_bytes -and
    $maxOutput -le $thresholds.max_output_bytes
)

$report = [ordered]@{
    schema_version = 1
    status = if ($passed) { 'passed' } else { 'failed' }
    pipeline = $Pipeline
    iterations = $Iterations
    thresholds = $thresholds
    result = [ordered]@{
        wall_median_seconds = $wallMedian
        pws_median_bytes = $pwsMedian
        max_output_bytes = $maxOutput
    }
    samples = $samples
}
$json = $report | ConvertTo-Json -Depth 8

if ($ReportPath) {
    $reportFullPath = [System.IO.Path]::GetFullPath($ReportPath)
    if ([System.IO.File]::Exists($reportFullPath)) {
        throw "Performance report already exists: $([System.IO.Path]::GetFileName($reportFullPath))"
    }
    $reportParent = [System.IO.Path]::GetDirectoryName($reportFullPath)
    [System.IO.Directory]::CreateDirectory($reportParent) | Out-Null
    $utf8WithoutBom = [System.Text.UTF8Encoding]::new($false)
    [System.IO.File]::WriteAllText($reportFullPath, "$json`n", $utf8WithoutBom)
}

$json
if (-not $passed) {
    exit 1
}
