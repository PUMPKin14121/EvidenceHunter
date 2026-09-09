[CmdletBinding()]
param(
    [ValidateRange(1, 600)]
    [int]$DurationSeconds = 600,

    [ValidateRange(1, 120)]
    [int]$Phase1Seconds = 60,

    [ValidateRange(60, 90)]
    [int]$NetworkLossSeconds = 90
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot '.venv\Scripts\python.exe'
$collector = Join-Path $repoRoot 'EvidenceHunter_collector.py'

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "PYTHON_NOT_FOUND: $python"
}
if (-not (Test-Path -LiteralPath $collector -PathType Leaf)) {
    throw "COLLECTOR_NOT_FOUND: $collector"
}

$overlap = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match 'EvidenceHunter_collector.py' -and
            $_.ProcessId -ne $PID
        }
)
if ($overlap.Count -gt 0) {
    throw "COLLECTOR_ALREADY_RUNNING: $($overlap.ProcessId -join ',')"
}

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$datasetId = "NETWORK_SWITCH_SMOKE_$stamp"
$runtimeRoot = Join-Path $env:TEMP "EvidenceHunter_network_switch_$stamp"
$logPath = Join-Path $env:TEMP "EvidenceHunter_network_switch_$stamp.log"

New-Item -ItemType File -Path $logPath -Force | Out-Null
$summaryPath = Join-Path $runtimeRoot 'soak_summary.json'
$earlyExit = $false
$routeJson = & $python -B -c (
    "import json; from EvidenceHunter_collector_network import network_routes; " +
    "print(json.dumps(network_routes('https://fapi.binance.com/fapi/v1/time')))"
)
if ($LASTEXITCODE -ne 0) {
    throw 'SYSTEM_PROXY_PREFLIGHT_FAILED'
}
$initialRoutes = $routeJson | ConvertFrom-Json
if (@($initialRoutes).Count -eq 0 -or $initialRoutes[0].mode -ne 'SYSTEM_PROXY') {
    throw 'PHASE1_REQUIRES_SYSTEM_PROXY_ON'
}

function Receive-CollectorOutput {
    param($Job, [string]$Path)
    $items = @(Receive-Job -Job $Job -ErrorAction Continue 2>&1)
    foreach ($item in $items) {
        $line = $item.ToString()
        Add-Content -LiteralPath $Path -Value $line -Encoding utf8
        Write-Host $line
    }
}

function Get-NonEmptyLineCount {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        return 0
    }
    return @(
        Get-Content -LiteralPath $Path | Where-Object { $_.Trim().Length -gt 0 }
    ).Count
}

function Get-NetworkModes {
    param([string[]]$Lines)
    $modes = foreach ($line in $Lines) {
        if ($line -match 'effective_network_mode[^A-Z_]*(SYSTEM_PROXY|DIRECT)') {
            $Matches[1]
        }
        elseif ($line -match 'network_mode=(SYSTEM_PROXY|DIRECT)') {
            $Matches[1]
        }
    }
    return @($modes | Select-Object -Unique)
}

Write-Host 'PHASE_1_START'
Write-Host 'Required Clash state: TUN OFF, System Proxy ON.'
Write-Host "RuntimeRoot: $runtimeRoot"
Write-Host "LogPath: $logPath"

$job = Start-Job -ArgumentList @(
    $repoRoot, $python, $collector, $datasetId, $runtimeRoot, $DurationSeconds
) -ScriptBlock {
    param($RepoRoot, $Python, $Collector, $DatasetId, $RuntimeRoot, $Duration)
    Set-Location -LiteralPath $RepoRoot
    $ErrorActionPreference = 'Continue'
    & $Python -u -B $Collector `
        --dataset-id $DatasetId `
        --runtime-dir $RuntimeRoot `
        --symbol BTCUSDT `
        --duration-seconds $Duration 2>&1
    Write-Output "__COLLECTOR_EXIT_CODE__=$LASTEXITCODE"
}

Start-Sleep -Seconds $Phase1Seconds
Receive-CollectorOutput -Job $job -Path $logPath

$aggPath = Join-Path $runtimeRoot 'raw\aggTrade.jsonl'
$depthPath = Join-Path $runtimeRoot 'raw\depth.jsonl'
$restPath = Join-Path $runtimeRoot 'quality\rest_telemetry.jsonl'
$supervisorPath = Join-Path $runtimeRoot 'supervisor_state.json'
$phase1Agg = Get-NonEmptyLineCount $aggPath
$phase1Depth = Get-NonEmptyLineCount $depthPath
$phase1Rest = Get-NonEmptyLineCount $restPath
$phase1LogLines = @(Get-Content -LiteralPath $logPath)
$initialNetworkMode = @(Get-NetworkModes $phase1LogLines | Select-Object -First 1)
$phase1State = if (Test-Path -LiteralPath $supervisorPath) {
    Get-Content -LiteralPath $supervisorPath -Raw | ConvertFrom-Json
} else {
    $null
}

[pscustomobject]@{
    Phase = 'PHASE_1_SNAPSHOT'
    JobState = $job.State
    AggTradeMessages = $phase1Agg
    DiffDepthMessages = $phase1Depth
    InitialNetworkMode = ($initialNetworkMode -join ',')
    AggTradeState = $phase1State.components.aggTrade.state
    DiffDepthState = $phase1State.components.diff_depth.state
} | Format-List

if ($job.State -ne 'Running') {
    Receive-CollectorOutput -Job $job -Path $logPath
    $earlyExit = $true
    Write-Host 'FAIL: COLLECTOR_EXITED_BEFORE_NETWORK_SWITCH'
}

$lossStartLine = $phase1LogLines.Count
Write-Host 'PHASE 1 COMPLETE'
Write-Host 'Please turn OFF Clash System Proxy, keep TUN OFF, then press ENTER'
$null = Read-Host
Start-Sleep -Seconds $NetworkLossSeconds
Receive-CollectorOutput -Job $job -Path $logPath

$phase2LogLines = @(Get-Content -LiteralPath $logPath)
$lossLines = @($phase2LogLines | Select-Object -Skip $lossStartLine)
$networkLossDetected = [bool](
    $lossLines -match 'CONNECTIVITY_LOST|NETWORK_UNAVAILABLE|RECONNECTING'
)

[pscustomobject]@{
    Phase = 'PHASE_2_SNAPSHOT'
    JobState = $job.State
    NetworkLossDetected = $networkLossDetected
    AggTradeMessages = Get-NonEmptyLineCount $aggPath
    DiffDepthMessages = Get-NonEmptyLineCount $depthPath
} | Format-List

if ($job.State -ne 'Running') {
    Receive-CollectorOutput -Job $job -Path $logPath
    $earlyExit = $true
    Write-Host 'FAIL: COLLECTOR_EXITED_DURING_NETWORK_LOSS'
}

$recoveryStartLine = $phase2LogLines.Count
$recoveryRestStart = Get-NonEmptyLineCount $restPath
Write-Host 'PHASE 2 COMPLETE'
Write-Host 'Please turn ON Clash System Proxy, keep TUN OFF, then press ENTER'
$null = Read-Host

while ($job.State -eq 'Running') {
    Start-Sleep -Seconds 5
    Receive-CollectorOutput -Job $job -Path $logPath
}
Receive-CollectorOutput -Job $job -Path $logPath

$allLogLines = @(Get-Content -LiteralPath $logPath)
$recoveryLines = @($allLogLines | Select-Object -Skip $recoveryStartLine)
$exitMarker = $allLogLines | Where-Object { $_ -match '^__COLLECTOR_EXIT_CODE__=' } |
    Select-Object -Last 1
$exitCode = if ($exitMarker -match '=(-?\d+)$') { [int]$Matches[1] } else { -1 }

$summaryFile = Get-ChildItem -LiteralPath $summaryPath -ErrorAction SilentlyContinue |
    Select-Object -Last 1
$summary = if ($summaryFile) {
    Get-Content -LiteralPath $summaryFile.FullName -Raw | ConvertFrom-Json
} else {
    $null
}

$restRecords = if (Test-Path -LiteralPath $restPath -PathType Leaf) {
    @(Get-Content -LiteralPath $restPath | ForEach-Object { $_ | ConvertFrom-Json })
} else {
    @()
}
$recoveryRestRecords = @($restRecords | Select-Object -Skip $recoveryRestStart)
$recoveryRestSucceeded = @(
    $recoveryRestRecords | Where-Object outcome -eq 'SUCCESS'
).Count -gt 0
$recoveryRestModes = @(
    $recoveryRestRecords |
        ForEach-Object { $_.network_route_events } |
        Where-Object event_type -eq 'CONNECTIVITY_RESTORED' |
        Select-Object -ExpandProperty effective_network_mode -Unique
)
$recoveryLogModes = @(Get-NetworkModes $recoveryLines)
$recoveryNetworkMode = @($recoveryRestModes + $recoveryLogModes | Select-Object -Unique)

$aggTradeMessages = if ($summary) { [int]$summary.streams.aggTrade.message_count } else { 0 }
$diffDepthMessages = if ($summary) { [int]$summary.streams.diff_depth.message_count } else { 0 }
$messagesContinued = (
    $aggTradeMessages -gt $phase1Agg -and
    $diffDepthMessages -gt $phase1Depth
)
$recoveryEventDetected = [bool](
    $recoveryLines -match 'CONNECTIVITY_RESTORED|NETWORK_MODE_CHANGED'
)
$recoveryDetected = $recoveryEventDetected -and $recoveryRestSucceeded -and $messagesContinued

$writerFailures = if ($summary) {
    @(
        $summary.writers.PSObject.Properties |
            Where-Object {
                $_.Value.dropped_count -ne 0 -or
                $_.Value.backlog_at_stop -ne 0
            }
    ).Count
} else {
    -1
}
$hasTraceback = [bool]($allLogLines -match 'Traceback')

$result = [ordered]@{
    ExitCode = $exitCode
    AggTradeMessages = $aggTradeMessages
    DiffDepthMessages = $diffDepthMessages
    InitialNetworkMode = ($initialNetworkMode -join ',')
    NetworkLossDetected = $networkLossDetected
    RecoveryDetected = $recoveryDetected
    RecoveryNetworkMode = ($recoveryNetworkMode -join ',')
    WriterFailures = $writerFailures
    HasTraceback = $hasTraceback
    SummaryPath = $summaryPath
    LogPath = $logPath
}

$pass = (
    -not $earlyExit -and
    $result.ExitCode -eq 0 -and
    $result.AggTradeMessages -gt 0 -and
    $result.DiffDepthMessages -gt 0 -and
    $result.NetworkLossDetected -eq $true -and
    $result.RecoveryDetected -eq $true -and
    $result.RecoveryNetworkMode -match 'SYSTEM_PROXY' -and
    $result.WriterFailures -eq 0 -and
    $result.HasTraceback -eq $false
)

[pscustomobject]$result | Format-List
Write-Host ("SMOKE_RESULT=" + $(if ($pass) { 'PASS' } else { 'FAIL' }))
Remove-Job -Job $job -Force

if (-not $pass) {
    exit 1
}



