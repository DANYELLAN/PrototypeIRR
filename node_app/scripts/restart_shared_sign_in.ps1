param([string]$LiveRoot = "C:\Users\Benoit\Documents\InspectionApp")
$ErrorActionPreference = "Stop"
$sourceRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$statusPath = Join-Path $sourceRoot "work\inspection-shared-signin-status.json"
$timeTaskName = "Benoit CNC Time Entry App"
$status = [ordered]@{state="checking";healthy=$false;time_entry=$false;inspection=$false;error=$null}
$timeStopped = $false
$timeTouched = $false
try {
    $principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "Windows administrator approval is required to restart the app tasks." }
    $manifest = Get-Content -LiteralPath (Join-Path $sourceRoot "inspection-release-manifest.json") -Raw | ConvertFrom-Json
    foreach ($entry in $manifest.time_entry_files.PSObject.Properties) {
        if ((Get-FileHash -LiteralPath (Join-Path $sourceRoot $entry.Name) -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.Value) { throw "The tested Time Entry source changed: $($entry.Name)" }
    }
    $timeTask = Get-ScheduledTask -TaskName $timeTaskName
    $launcher = Join-Path $sourceRoot "time_entry_app\scripts\run_cnc_time_app.ps1"
    if (-not ($timeTask.Actions | Where-Object { $_.Arguments -like "*$launcher*" })) { throw "The Time Entry task points to a different source folder. No task was stopped." }
    Stop-ScheduledTask -TaskName $timeTaskName
    $timeTouched = $true
    $timeStopped = $true
    Start-Sleep -Seconds 2
    foreach ($listener in @(Get-NetTCPConnection -State Listen -LocalPort 3100 -ErrorAction SilentlyContinue)) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
        $serverPath = Join-Path $sourceRoot "time_entry_app\src\cncTimeServer.js"
        if ($process.Name -ne 'node.exe' -or $process.CommandLine -notlike "*$serverPath*") { throw "Port 3100 is used by an unverified process." }
        Stop-Process -Id $listener.OwningProcess
    }
    & (Join-Path $PSScriptRoot "restart_inspection_release.ps1") -LiveRoot $LiveRoot
    $inspectionStatus = Get-Content -LiteralPath (Join-Path $sourceRoot "work\inspection-deploy-status.json") -Raw | ConvertFrom-Json
    if (-not $inspectionStatus.healthy -or $inspectionStatus.build -ne $manifest.build) { throw "Inspection startup failed: $($inspectionStatus.error)" }
    $status.inspection = $true
    Start-ScheduledTask -TaskName $timeTaskName
    $timeStopped = $false
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Seconds 2
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:3100/api/inspection-sign-in" -TimeoutSec 5
            $signInPage = Invoke-WebRequest -Uri "http://127.0.0.1:3100/" -UseBasicParsing -TimeoutSec 15
            $status.time_entry = $health.enabled -eq $true -and $signInPage.StatusCode -eq 200
        } catch { $status.error = $_.Exception.Message }
    } until ($status.time_entry -or (Get-Date) -gt $deadline)
    if (-not $status.time_entry) { throw "The updated Time Entry app did not become healthy." }
    $status.healthy = $true
    $status.state = "live"
    $status.error = $null
} catch {
    $status.state = "failed"
    $status.error = $_.Exception.Message
    $rollback = Join-Path $sourceRoot "work\inspection-shared-signin-rollback.zip"
    if ($timeTouched -and (Test-Path -LiteralPath $rollback)) {
        Stop-ScheduledTask -TaskName $timeTaskName -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        $runtime = Get-Content -LiteralPath (Join-Path $sourceRoot "node_app\config\inspection-runtime.json") -Raw | ConvertFrom-Json
        & $runtime.pythonExecutable (Join-Path $PSScriptRoot "sync_inspection_copies.py") $sourceRoot --restore-archive $rollback
        Start-ScheduledTask -TaskName $timeTaskName -ErrorAction SilentlyContinue
        $status.state = "rolled_back"
    } elseif ($timeStopped) { Start-ScheduledTask -TaskName $timeTaskName -ErrorAction SilentlyContinue }
    Write-Error $_ -ErrorAction Continue
} finally {
    $status | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statusPath -Encoding UTF8
}
