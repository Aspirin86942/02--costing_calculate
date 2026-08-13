[CmdletBinding(DefaultParameterSetName = 'Measure')]
param(
    [Parameter(Mandatory = $true, ParameterSetName = 'Measure')]
    [string]$BaselineBinary,

    [Parameter(Mandatory = $true, ParameterSetName = 'Measure')]
    [string]$CandidateBinary,

    [Parameter(Mandatory = $true, ParameterSetName = 'Measure')]
    [ValidateSet('gb', 'sk')]
    [string]$Pipeline,

    [Parameter(Mandatory = $true, ParameterSetName = 'Measure')]
    [string]$InputPath,

    [Parameter(Mandatory = $true, ParameterSetName = 'Measure')]
    [ValidateSet('normal', 'check-only')]
    [string]$Mode = 'normal',

    [Parameter(Mandatory = $true, ParameterSetName = 'Measure')]
    [string]$OutputDirectory,

    [Parameter(ParameterSetName = 'Measure')]
    [ValidateRange(1, 100)]
    [int]$Pairs = 8,

    [Parameter(ParameterSetName = 'Measure')]
    [ValidateRange(1, 86400)]
    [int]$PerRunTimeoutSeconds = 3600,

    [Parameter(ParameterSetName = 'Measure')]
    [string]$ReportPath,

    [Parameter(Mandatory = $true, ParameterSetName = 'SelfTest')]
    [switch]$SelfTest
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
        Throw-InvalidBatch -Reason 'REQUIRED_FILE_NOT_FOUND' -SafeMessage (
            "$Label not found: $([System.IO.Path]::GetFileName($resolved))"
        )
    }
    return $resolved
}

function Throw-InvalidBatch {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Reason,

        [Parameter(Mandatory = $true)]
        [string]$SafeMessage
    )

    $exception = [System.InvalidOperationException]::new($SafeMessage)
    $exception.Data['invalid_reason'] = $Reason
    throw $exception
}

function Get-Sha256 {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Get-FileIdentity {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Alias,

        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    return [pscustomobject][ordered]@{
        alias = $Alias
        sha256 = Get-Sha256 -Path $Path
        size_bytes = [long](Get-Item -LiteralPath $Path).Length
    }
}

function Test-PathsEqual {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Left,

        [Parameter(Mandatory = $true)]
        [string]$Right
    )

    return [string]::Equals(
        [System.IO.Path]::GetFullPath($Left),
        [System.IO.Path]::GetFullPath($Right),
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Test-PathWithin {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Candidate,

        [Parameter(Mandatory = $true)]
        [string]$Parent
    )

    $candidateFull = [System.IO.Path]::GetFullPath($Candidate)
    $parentFull = [System.IO.Path]::GetFullPath($Parent).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
    if (Test-PathsEqual -Left $candidateFull -Right $parentFull) {
        return $true
    }
    $prefix = "$parentFull$([System.IO.Path]::DirectorySeparatorChar)"
    return $candidateFull.StartsWith(
        $prefix,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Get-Median {
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$Values
    )

    if ($Values.Count -eq 0) {
        Throw-InvalidBatch -Reason 'EMPTY_METRIC_SET' -SafeMessage 'Cannot calculate a median for an empty metric set.'
    }
    $sorted = @($Values | Sort-Object)
    $middle = [int][Math]::Floor($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) {
        return [double]$sorted[$middle]
    }
    return ([double]$sorted[$middle - 1] + [double]$sorted[$middle]) / 2
}

function Get-RelativeDelta {
    param(
        [Parameter(Mandatory = $true)]
        [double]$Baseline,

        [Parameter(Mandatory = $true)]
        [double]$Candidate
    )

    if ($Baseline -eq 0.0) {
        return $null
    }
    return ($Candidate - $Baseline) / $Baseline
}

function Get-PairedMetricSummary {
    param(
        [Parameter(Mandatory = $true)]
        [double[]]$BaselineValues,

        [Parameter(Mandatory = $true)]
        [double[]]$CandidateValues
    )

    if ($BaselineValues.Count -ne $CandidateValues.Count -or $BaselineValues.Count -eq 0) {
        Throw-InvalidBatch -Reason 'INVALID_PAIRED_METRICS' -SafeMessage 'Paired metric arrays must be non-empty and have equal lengths.'
    }
    $relativeDeltas = @()
    $candidateWins = 0
    for ($index = 0; $index -lt $BaselineValues.Count; $index++) {
        $baseline = [double]$BaselineValues[$index]
        $candidate = [double]$CandidateValues[$index]
        $relative = Get-RelativeDelta -Baseline $baseline -Candidate $candidate
        if ($null -ne $relative) {
            $relativeDeltas += [double]$relative
        }
        if ($candidate -lt $baseline) {
            $candidateWins++
        }
    }
    if ($relativeDeltas.Count -eq 0) {
        Throw-InvalidBatch -Reason 'ZERO_BASELINE_METRICS' -SafeMessage 'Every baseline value for a paired metric was zero.'
    }

    return [pscustomobject][ordered]@{
        baseline_median = [Math]::Round((Get-Median -Values $BaselineValues), 6)
        candidate_median = [Math]::Round((Get-Median -Values $CandidateValues), 6)
        paired_median_relative_delta = [Math]::Round(
            (Get-Median -Values @($relativeDeltas)),
            6
        )
        candidate_wins = $candidateWins
        pair_count = $BaselineValues.Count
    }
}

function Get-PairExecutionOrder {
    param(
        [Parameter(Mandatory = $true)]
        [int]$PairNumber
    )

    if ($PairNumber % 2 -eq 1) {
        return @('baseline', 'candidate')
    }
    return @('candidate', 'baseline')
}

function Get-ActivePowerScheme {
    try {
        $output = & powercfg.exe /GetActiveScheme 2>$null
        $match = [regex]::Match(
            [string]::Join(' ', @($output)),
            '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
        )
        if ($match.Success) {
            return $match.Value.ToLowerInvariant()
        }
    }
    catch {
        # The safe "unavailable" marker is enough to detect a before/after change.
    }
    return 'unavailable'
}

function Get-PhysicalMemoryBucket {
    try {
        $total = [long](Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory
        $gib = [long][Math]::Max(1, [Math]::Round($total / 1GB, 0))
        return $gib * 1GB
    }
    catch {
        return [long]0
    }
}

function Get-SafeEnvironmentSnapshot {
    $memoryBucket = Get-PhysicalMemoryBucket
    $components = @(
        [System.Runtime.InteropServices.RuntimeInformation]::OSDescription,
        [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString(),
        [Environment]::ProcessorCount.ToString([System.Globalization.CultureInfo]::InvariantCulture),
        $memoryBucket.ToString([System.Globalization.CultureInfo]::InvariantCulture)
    )
    $fingerprintBytes = [System.Text.Encoding]::UTF8.GetBytes(
        [string]::Join('|', $components)
    )
    $sha = [System.Security.Cryptography.SHA256]::HashData($fingerprintBytes)

    return [pscustomobject][ordered]@{
        machine_fingerprint_sha256 = [Convert]::ToHexString($sha).ToLowerInvariant()
        os_description = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        os_architecture = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
        logical_processor_count = [Environment]::ProcessorCount
        physical_memory_bucket_bytes = $memoryBucket
        active_power_scheme = Get-ActivePowerScheme
    }
}

function Convert-ToSafeCountMap {
    param(
        [Parameter(Mandatory = $false)]
        [AllowNull()]
        [object]$Value
    )

    $result = [ordered]@{}
    if ($null -eq $Value) {
        return [pscustomobject]$result
    }
    foreach ($property in @($Value.PSObject.Properties | Sort-Object Name)) {
        $result[$property.Name] = [long]$property.Value
    }
    return [pscustomobject]$result
}

function Convert-ToStageMap {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Payload
    )

    if (
        $null -eq $Payload.stage_timings -or
        $null -eq $Payload.stage_timings.stages
    ) {
        Throw-InvalidBatch -Reason 'STAGE_TIMINGS_MISSING' -SafeMessage 'The run summary did not contain stage timings.'
    }
    $result = [ordered]@{}
    foreach ($property in @($Payload.stage_timings.stages.PSObject.Properties | Sort-Object Name)) {
        $value = [double]$property.Value
        if ([double]::IsNaN($value) -or [double]::IsInfinity($value) -or $value -lt 0.0) {
            Throw-InvalidBatch -Reason 'STAGE_TIMING_INVALID' -SafeMessage 'The run summary contained an invalid stage timing.'
        }
        $result[$property.Name] = [Math]::Round($value, 6)
    }
    if (-not $result.Contains('total')) {
        Throw-InvalidBatch -Reason 'TOTAL_STAGE_MISSING' -SafeMessage 'The run summary did not contain the total stage timing.'
    }
    return [pscustomobject]$result
}

function Get-StageValue {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Stages,

        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    $property = $Stages.PSObject.Properties[$Name]
    if ($null -eq $property) {
        Throw-InvalidBatch -Reason 'STAGE_SET_CHANGED' -SafeMessage "Stage timing set changed for safe alias: $Name."
    }
    return [double]$property.Value
}

function Get-SafeFailureCode {
    param(
        [Parameter(Mandatory = $false)]
        [AllowEmptyString()]
        [string]$StandardError
    )

    if ([string]::IsNullOrWhiteSpace($StandardError)) {
        return 'UNKNOWN'
    }
    try {
        $payload = $StandardError | ConvertFrom-Json
        foreach ($name in @('code', 'error_code')) {
            $property = $payload.PSObject.Properties[$name]
            if ($null -ne $property -and $property.Value -is [string]) {
                return [string]$property.Value
            }
        }
    }
    catch {
        # Raw stderr can contain sensitive paths, so it is intentionally discarded.
    }
    return 'UNPARSEABLE'
}

function Assert-NoTemporaryResidue {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RunDirectory
    )

    $residue = @(
        Get-ChildItem -LiteralPath $RunDirectory -Force -Recurse |
            Where-Object {
                $_.Name -like '.costing-tmp-*' -or
                $_.Name -like '.costing-publish-*'
            }
    )
    if ($residue.Count -ne 0) {
        Throw-InvalidBatch -Reason 'TEMPORARY_RESIDUE_FOUND' -SafeMessage 'A measurement run left temporary publishing files.'
    }
}

function Invoke-MeasurementRun {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet('baseline', 'candidate')]
        [string]$Role,

        [Parameter(Mandatory = $true)]
        [string]$Alias,

        [Parameter(Mandatory = $true)]
        [string]$Binary,

        [Parameter(Mandatory = $true)]
        [string]$InputWorkbook,

        [Parameter(Mandatory = $true)]
        [ValidateSet('gb', 'sk')]
        [string]$RunPipeline,

        [Parameter(Mandatory = $true)]
        [ValidateSet('normal', 'check-only')]
        [string]$RunMode,

        [Parameter(Mandatory = $true)]
        [string]$OutputRoot,

        [Parameter(Mandatory = $true)]
        [int]$TimeoutSeconds
    )

    $runDirectory = [System.IO.Path]::Combine($OutputRoot, $Alias)
    if ([System.IO.Directory]::Exists($runDirectory) -or [System.IO.File]::Exists($runDirectory)) {
        Throw-InvalidBatch -Reason 'RUN_OUTPUT_REUSED' -SafeMessage "Run output alias already exists: $Alias."
    }
    [System.IO.Directory]::CreateDirectory($runDirectory) | Out-Null
    $outputPath = [System.IO.Path]::Combine($runDirectory, 'output.xlsx')

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Binary
    $startInfo.WorkingDirectory = $runDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = [System.Text.Encoding]::UTF8
    [void]$startInfo.ArgumentList.Add($RunPipeline)
    [void]$startInfo.ArgumentList.Add('--input')
    [void]$startInfo.ArgumentList.Add($InputWorkbook)
    if ($RunMode -eq 'normal') {
        [void]$startInfo.ArgumentList.Add('--output')
        [void]$startInfo.ArgumentList.Add($outputPath)
    }
    else {
        [void]$startInfo.ArgumentList.Add('--check-only')
    }
    [void]$startInfo.ArgumentList.Add('--benchmark')
    [void]$startInfo.ArgumentList.Add('--redact-paths')

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    if (-not $process.Start()) {
        Throw-InvalidBatch -Reason 'PROCESS_START_FAILED' -SafeMessage "Unable to start measurement role: $Role."
    }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $peakWorkingSet = [long]0
    $timedOut = $false
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
        if (-not $exited -and $stopwatch.Elapsed.TotalSeconds -gt $TimeoutSeconds) {
            $timedOut = $true
            try {
                $process.Kill($true)
            }
            catch {
                # The process may have exited while the timeout was handled.
            }
            $process.WaitForExit()
            break
        }
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

    if ($timedOut) {
        Throw-InvalidBatch -Reason 'RUN_TIMEOUT' -SafeMessage "Measurement role timed out: $Role."
    }
    if ($exitCode -ne 0) {
        $failureCode = Get-SafeFailureCode -StandardError $stderr
        Throw-InvalidBatch -Reason 'RUN_FAILED' -SafeMessage (
            "Measurement role failed: role=$Role exit_code=$exitCode error_code=$failureCode."
        )
    }
    if ($peakWorkingSet -le 0) {
        Throw-InvalidBatch -Reason 'PWS_MEASUREMENT_MISSING' -SafeMessage "Measurement role did not produce a Peak Working Set: $Role."
    }
    try {
        $payload = $stdout | ConvertFrom-Json
    }
    catch {
        Throw-InvalidBatch -Reason 'SUMMARY_JSON_INVALID' -SafeMessage "Measurement role returned invalid JSON: $Role."
    }
    $expectedOutputWritten = $RunMode -eq 'normal'
    if (
        $payload.status -ne 'succeeded' -or
        $payload.pipeline -ne $RunPipeline -or
        [bool]$payload.output_written -ne $expectedOutputWritten
    ) {
        Throw-InvalidBatch -Reason 'SUMMARY_CONTRACT_MISMATCH' -SafeMessage "Measurement role returned an unexpected safe summary: $Role."
    }
    $stageTimings = Convert-ToStageMap -Payload $payload

    $outputSize = $null
    $outputSha256 = $null
    if ($RunMode -eq 'normal') {
        if (-not [System.IO.File]::Exists($outputPath)) {
            Throw-InvalidBatch -Reason 'OUTPUT_MISSING' -SafeMessage "Measurement role did not publish its workbook: $Role."
        }
        $outputSize = [long](Get-Item -LiteralPath $outputPath).Length
        $outputSha256 = Get-Sha256 -Path $outputPath
        if ($null -eq $payload.output_size_bytes -or [long]$payload.output_size_bytes -ne $outputSize) {
            Throw-InvalidBatch -Reason 'OUTPUT_SIZE_MISMATCH' -SafeMessage "Measurement role reported a different output size: $Role."
        }
    }
    else {
        $unexpectedWorkbooks = @(Get-ChildItem -LiteralPath $runDirectory -File -Filter '*.xlsx')
        if ($unexpectedWorkbooks.Count -ne 0 -or $null -ne $payload.output_size_bytes) {
            Throw-InvalidBatch -Reason 'CHECK_ONLY_WROTE_OUTPUT' -SafeMessage "Check-only role unexpectedly wrote a workbook: $Role."
        }
    }
    Assert-NoTemporaryResidue -RunDirectory $runDirectory

    return [pscustomobject][ordered]@{
        role = $Role
        run_alias = $Alias
        external_wall_seconds = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 6)
        peak_working_set_bytes = $peakWorkingSet
        output_size_bytes = $outputSize
        output_sha256 = $outputSha256
        stage_timings = $stageTimings
        summary = [pscustomobject][ordered]@{
            status = [string]$payload.status
            pipeline = [string]$payload.pipeline
            output_written = [bool]$payload.output_written
            sheet_count = [long]$payload.sheet_count
            error_log_count = [long]$payload.error_log_count
            issue_type_counts = Convert-ToSafeCountMap -Value $payload.issue_type_counts
            run_counts = Convert-ToSafeCountMap -Value $payload.run_counts
        }
        temporary_residue_count = 0
    }
}

function Get-StageNames {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Runs
    )

    $names = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($run in $Runs) {
        foreach ($property in $run.stage_timings.PSObject.Properties) {
            [void]$names.Add($property.Name)
        }
    }
    return @($names | Sort-Object)
}

function New-PairRecord {
    param(
        [Parameter(Mandatory = $true)]
        [int]$PairNumber,

        [Parameter(Mandatory = $true)]
        [string[]]$ExecutionOrder,

        [Parameter(Mandatory = $true)]
        [object]$BaselineRun,

        [Parameter(Mandatory = $true)]
        [object]$CandidateRun,

        [Parameter(Mandatory = $true)]
        [ValidateSet('normal', 'check-only')]
        [string]$PairMode
    )

    $stageDeltas = [ordered]@{}
    $stageNames = Get-StageNames -Runs @($BaselineRun, $CandidateRun)
    foreach ($stageName in $stageNames) {
        $baselineValue = Get-StageValue -Stages $BaselineRun.stage_timings -Name $stageName
        $candidateValue = Get-StageValue -Stages $CandidateRun.stage_timings -Name $stageName
        $relative = Get-RelativeDelta -Baseline $baselineValue -Candidate $candidateValue
        $stageDeltas[$stageName] = if ($null -eq $relative) {
            $null
        }
        else {
            [Math]::Round([double]$relative, 6)
        }
    }

    $outputRelative = $null
    if ($PairMode -eq 'normal') {
        $outputRelative = Get-RelativeDelta `
            -Baseline ([double]$BaselineRun.output_size_bytes) `
            -Candidate ([double]$CandidateRun.output_size_bytes)
        if ($null -ne $outputRelative) {
            $outputRelative = [Math]::Round([double]$outputRelative, 6)
        }
    }

    return [pscustomobject][ordered]@{
        pair_number = $PairNumber
        execution_order = $ExecutionOrder
        baseline = $BaselineRun
        candidate = $CandidateRun
        relative_deltas = [pscustomobject][ordered]@{
            external_wall = [Math]::Round(
                [double](Get-RelativeDelta `
                    -Baseline $BaselineRun.external_wall_seconds `
                    -Candidate $CandidateRun.external_wall_seconds),
                6
            )
            peak_working_set = [Math]::Round(
                [double](Get-RelativeDelta `
                    -Baseline $BaselineRun.peak_working_set_bytes `
                    -Candidate $CandidateRun.peak_working_set_bytes),
                6
            )
            output_size = $outputRelative
            stages = [pscustomobject]$stageDeltas
        }
    }
}

function New-BatchSummary {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$PairRecords,

        [Parameter(Mandatory = $true)]
        [ValidateSet('normal', 'check-only')]
        [string]$BatchMode
    )

    $baselineRuns = @($PairRecords | ForEach-Object { $_.baseline })
    $candidateRuns = @($PairRecords | ForEach-Object { $_.candidate })
    $allRuns = @($baselineRuns) + @($candidateRuns)

    $stages = [ordered]@{}
    foreach ($stageName in (Get-StageNames -Runs $allRuns)) {
        $baselineValues = @(
            $baselineRuns |
                ForEach-Object { Get-StageValue -Stages $_.stage_timings -Name $stageName }
        )
        $candidateValues = @(
            $candidateRuns |
                ForEach-Object { Get-StageValue -Stages $_.stage_timings -Name $stageName }
        )
        $stages[$stageName] = Get-PairedMetricSummary `
            -BaselineValues $baselineValues `
            -CandidateValues $candidateValues
    }

    $outputMetric = $null
    $candidateMaximumOutput = $null
    if ($BatchMode -eq 'normal') {
        $outputMetric = Get-PairedMetricSummary `
            -BaselineValues @($baselineRuns.output_size_bytes) `
            -CandidateValues @($candidateRuns.output_size_bytes)
        $candidateMaximumOutput = [long](
            $candidateRuns.output_size_bytes |
                Measure-Object -Maximum
        ).Maximum
    }

    return [pscustomobject][ordered]@{
        external_wall_seconds = Get-PairedMetricSummary `
            -BaselineValues @($baselineRuns.external_wall_seconds) `
            -CandidateValues @($candidateRuns.external_wall_seconds)
        peak_working_set_bytes = Get-PairedMetricSummary `
            -BaselineValues @($baselineRuns.peak_working_set_bytes) `
            -CandidateValues @($candidateRuns.peak_working_set_bytes)
        output_size_bytes = $outputMetric
        candidate_max_output_bytes = $candidateMaximumOutput
        stages = [pscustomobject]$stages
    }
}

function Write-JsonReport {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Report,

        [Parameter(Mandatory = $false)]
        [AllowNull()]
        [string]$Path
    )

    $json = $Report | ConvertTo-Json -Depth 20
    if (-not [string]::IsNullOrWhiteSpace($Path)) {
        if ([System.IO.File]::Exists($Path)) {
            Throw-InvalidBatch -Reason 'REPORT_ALREADY_EXISTS' -SafeMessage 'The paired measurement report already exists.'
        }
        $parent = [System.IO.Path]::GetDirectoryName($Path)
        if (-not [string]::IsNullOrWhiteSpace($parent)) {
            [System.IO.Directory]::CreateDirectory($parent) | Out-Null
        }
        $utf8WithoutBom = [System.Text.UTF8Encoding]::new($false)
        [System.IO.File]::WriteAllText($Path, "$json`n", $utf8WithoutBom)
    }
    return $json
}

if ($PSCmdlet.ParameterSetName -eq 'SelfTest') {
    $orders = @(
        1..4 | ForEach-Object {
            , @(Get-PairExecutionOrder -PairNumber $_)
        }
    )
    $summary = Get-PairedMetricSummary `
        -BaselineValues @(10.0, 20.0, 30.0, 40.0) `
        -CandidateValues @(9.0, 22.0, 27.0, 44.0)
    [pscustomobject][ordered]@{
        status = 'passed'
        median_and_relative_delta = $summary
        pair_orders = $orders
    } | ConvertTo-Json -Depth 6
    exit 0
}

$resolvedReport = $null
$startedUtc = [DateTimeOffset]::UtcNow
$initialArtifacts = $null
$initialEnvironment = $null

try {
    $baseline = Resolve-RequiredFile -Path $BaselineBinary -Label 'baseline binary'
    $candidate = Resolve-RequiredFile -Path $CandidateBinary -Label 'candidate binary'
    $inputWorkbook = Resolve-RequiredFile -Path $InputPath -Label 'input workbook'
    $outputRoot = [System.IO.Path]::GetFullPath($OutputDirectory)
    if (
        (Test-PathsEqual -Left $baseline -Right $inputWorkbook) -or
        (Test-PathsEqual -Left $candidate -Right $inputWorkbook)
    ) {
        Throw-InvalidBatch -Reason 'INPUT_BINARY_PATH_CONFLICT' -SafeMessage 'The input workbook conflicts with a binary path.'
    }

    if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
        $candidateReport = [System.IO.Path]::GetFullPath($ReportPath)
        foreach ($protected in @($baseline, $candidate, $inputWorkbook)) {
            if (Test-PathsEqual -Left $candidateReport -Right $protected) {
                Throw-InvalidBatch -Reason 'REPORT_PATH_CONFLICT' -SafeMessage 'The report path conflicts with a protected input.'
            }
        }
        if (Test-PathWithin -Candidate $candidateReport -Parent $outputRoot) {
            Throw-InvalidBatch -Reason 'REPORT_OUTPUT_CONFLICT' -SafeMessage 'The report path must be outside the paired output root.'
        }
        if (
            [System.IO.File]::Exists($candidateReport) -or
            [System.IO.Directory]::Exists($candidateReport)
        ) {
            Throw-InvalidBatch -Reason 'REPORT_ALREADY_EXISTS' -SafeMessage 'The paired measurement report must not exist before the batch.'
        }
        $resolvedReport = $candidateReport
    }
    if (
        [System.IO.Directory]::Exists($outputRoot) -or
        [System.IO.File]::Exists($outputRoot)
    ) {
        Throw-InvalidBatch -Reason 'OUTPUT_ROOT_ALREADY_EXISTS' -SafeMessage 'The paired output root must not exist before the batch.'
    }

    $initialArtifacts = [pscustomobject][ordered]@{
        baseline_binary = Get-FileIdentity -Alias 'baseline_binary' -Path $baseline
        candidate_binary = Get-FileIdentity -Alias 'candidate_binary' -Path $candidate
        input_workbook = Get-FileIdentity -Alias 'input_workbook' -Path $inputWorkbook
    }
    $initialEnvironment = Get-SafeEnvironmentSnapshot
    [System.IO.Directory]::CreateDirectory($outputRoot) | Out-Null

    [void](Invoke-MeasurementRun `
        -Role 'baseline' `
        -Alias 'warmup-baseline' `
        -Binary $baseline `
        -InputWorkbook $inputWorkbook `
        -RunPipeline $Pipeline `
        -RunMode $Mode `
        -OutputRoot $outputRoot `
        -TimeoutSeconds $PerRunTimeoutSeconds)
    [void](Invoke-MeasurementRun `
        -Role 'candidate' `
        -Alias 'warmup-candidate' `
        -Binary $candidate `
        -InputWorkbook $inputWorkbook `
        -RunPipeline $Pipeline `
        -RunMode $Mode `
        -OutputRoot $outputRoot `
        -TimeoutSeconds $PerRunTimeoutSeconds)

    $pairRecords = @()
    for ($pairNumber = 1; $pairNumber -le $Pairs; $pairNumber++) {
        $order = @(Get-PairExecutionOrder -PairNumber $pairNumber)
        $runs = @{}
        foreach ($role in $order) {
            $binaryForRole = if ($role -eq 'baseline') { $baseline } else { $candidate }
            $alias = 'pair-{0:D2}-{1}' -f $pairNumber, $role
            $runs[$role] = Invoke-MeasurementRun `
                -Role $role `
                -Alias $alias `
                -Binary $binaryForRole `
                -InputWorkbook $inputWorkbook `
                -RunPipeline $Pipeline `
                -RunMode $Mode `
                -OutputRoot $outputRoot `
                -TimeoutSeconds $PerRunTimeoutSeconds
        }
        $pairRecords += New-PairRecord `
            -PairNumber $pairNumber `
            -ExecutionOrder $order `
            -BaselineRun $runs['baseline'] `
            -CandidateRun $runs['candidate'] `
            -PairMode $Mode
    }

    $finalArtifacts = [pscustomobject][ordered]@{
        baseline_binary = Get-FileIdentity -Alias 'baseline_binary' -Path $baseline
        candidate_binary = Get-FileIdentity -Alias 'candidate_binary' -Path $candidate
        input_workbook = Get-FileIdentity -Alias 'input_workbook' -Path $inputWorkbook
    }
    foreach ($artifactName in @('baseline_binary', 'candidate_binary', 'input_workbook')) {
        $before = $initialArtifacts.PSObject.Properties[$artifactName].Value
        $after = $finalArtifacts.PSObject.Properties[$artifactName].Value
        if ($before.sha256 -ne $after.sha256 -or $before.size_bytes -ne $after.size_bytes) {
            Throw-InvalidBatch -Reason 'ARTIFACT_IDENTITY_CHANGED' -SafeMessage "Artifact identity changed during the batch: $artifactName."
        }
    }
    $finalEnvironment = Get-SafeEnvironmentSnapshot
    if (
        $initialEnvironment.machine_fingerprint_sha256 -ne
            $finalEnvironment.machine_fingerprint_sha256 -or
        $initialEnvironment.active_power_scheme -ne
            $finalEnvironment.active_power_scheme
    ) {
        Throw-InvalidBatch -Reason 'ENVIRONMENT_CHANGED' -SafeMessage 'The safe environment fingerprint changed during the batch.'
    }

    $report = [pscustomobject][ordered]@{
        schema_version = 1
        status = 'valid'
        measurement_only = $true
        pipeline = $Pipeline
        mode = $Mode
        pair_count = $Pairs
        ordering = [pscustomobject][ordered]@{
            warmup_order = @('baseline', 'candidate')
            odd_pairs = @('baseline', 'candidate')
            even_pairs = @('candidate', 'baseline')
        }
        started_utc = $startedUtc.ToString('O')
        finished_utc = [DateTimeOffset]::UtcNow.ToString('O')
        artifacts = [pscustomobject][ordered]@{
            before = $initialArtifacts
            after = $finalArtifacts
        }
        environment = [pscustomobject][ordered]@{
            before = $initialEnvironment
            after = $finalEnvironment
            limitations = @(
                'Peak Working Set is sampled every 10ms from an external Windows process.',
                'The report records measurement facts and does not decide candidate adoption.'
            )
        }
        samples = $pairRecords
        summary = New-BatchSummary -PairRecords $pairRecords -BatchMode $Mode
        invalid_reason = $null
    }
    Write-JsonReport -Report $report -Path $resolvedReport
    exit 0
}
catch {
    $reason = 'MEASUREMENT_BATCH_INVALID'
    if ($_.Exception.Data.Contains('invalid_reason')) {
        $reason = [string]$_.Exception.Data['invalid_reason']
    }
    $invalidReport = [pscustomobject][ordered]@{
        schema_version = 1
        status = 'invalid'
        measurement_only = $true
        pipeline = $Pipeline
        mode = $Mode
        pair_count = $Pairs
        ordering = [pscustomobject][ordered]@{
            warmup_order = @('baseline', 'candidate')
            odd_pairs = @('baseline', 'candidate')
            even_pairs = @('candidate', 'baseline')
        }
        started_utc = $startedUtc.ToString('O')
        finished_utc = [DateTimeOffset]::UtcNow.ToString('O')
        artifacts = if ($null -eq $initialArtifacts) {
            [pscustomobject]@{}
        }
        else {
            [pscustomobject][ordered]@{ before = $initialArtifacts }
        }
        environment = if ($null -eq $initialEnvironment) {
            [pscustomobject]@{}
        }
        else {
            [pscustomobject][ordered]@{ before = $initialEnvironment }
        }
        samples = @()
        summary = [pscustomobject]@{}
        invalid_reason = $reason
    }
    try {
        Write-JsonReport -Report $invalidReport -Path $resolvedReport
    }
    catch {
        $invalidReport | ConvertTo-Json -Depth 20
    }
    exit 2
}
