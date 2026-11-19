param([string]$LiveRoot = "C:\Users\Benoit\Documents\InspectionApp")
$ErrorActionPreference = "Stop"
$sourceRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$statusPath = Join-Path $sourceRoot "work\inspection-deploy-status.json"
$taskName = "Benoit Inspection App"
$status = [ordered]@{state="checking";healthy=$false;build=$null;https_listener=$false;error=$null}
$stopped = $false
$rollbackArchive = $null
try {
    $principal = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw "Windows administrator approval is required to restart the SYSTEM inspection task." }
    $manifest = Get-Content -LiteralPath (Join-Path $sourceRoot "inspection-release-manifest.json") -Raw | ConvertFrom-Json
    $status.build = $manifest.build
    $runtime = Get-Content -LiteralPath (Join-Path $sourceRoot "node_app\config\inspection-runtime.json") -Raw | ConvertFrom-Json
    foreach ($entry in $manifest.files.PSObject.Properties) {
        $file = Join-Path $sourceRoot $entry.Name
        if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -ne $entry.Value) { throw "The tested source changed: $($entry.Name)" }
    }
    $task = Get-ScheduledTask -TaskName $taskName
    $launcher = Join-Path $LiveRoot "node_app\scripts\run_inspection_app.ps1"
    if (-not ($task.Actions | Where-Object { $_.Arguments -like "*$launcher*" })) { throw "The startup task points to a different app. No process was stopped." }
    Stop-ScheduledTask -TaskName $taskName
    $stopped = $true
    Start-Sleep -Seconds 2
    $listeners = @(Get-NetTCPConnection -State Listen -LocalPort 3000 -ErrorAction SilentlyContinue)
    foreach ($listener in $listeners) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
        $serverPath = Join-Path $LiveRoot "node_app\src\server.js"
        if ($process.Name -ne 'node.exe' -or $process.CommandLine -notlike "*$serverPath*") { throw "Port 3000 is used by an unverified process. Update stopped." }
        Stop-Process -Id $listener.OwningProcess -ErrorAction Stop
    }
    $syncScript = Join-Path $sourceRoot "node_app\scripts\sync_inspection_copies.py"
    $syncOutput = & $runtime.pythonExecutable $syncScript $LiveRoot | Out-String
    if ($LASTEXITCODE -ne 0) { throw "Code synchronization failed." }
    $syncResults = $syncOutput | ConvertFrom-Json
    $rollbackArchive = @($syncResults)[0].backup
    Start-ScheduledTask -TaskName $taskName
    $stopped = $false
    $deadline = (Get-Date).AddSeconds(45)
    do {
        Start-Sleep -Seconds 2
        try {
            $release = Invoke-RestMethod -Uri "http://127.0.0.1:3000/api/app-version" -TimeoutSec 5
            $homeResponse = Invoke-WebRequest -Uri "http://127.0.0.1:3000/" -UseBasicParsing -TimeoutSec 15
            $status.healthy = $release.build -eq $manifest.build -and $homeResponse.StatusCode -eq 200
        } catch { $status.error = $_.Exception.Message }
    } until ($status.healthy -or (Get-Date) -gt $deadline)
    if (-not $status.healthy) { throw "The new release did not become healthy. Check node_app/data/inspection-host.log." }
    $status.state = "live"
    $status.error = $null
    $status.https_listener = [bool](Get-NetTCPConnection -State Listen -LocalPort 3444 -ErrorAction SilentlyContinue)
    Write-Output "Inspection release is live: $($manifest.build)"
} catch {
    $status.state = "failed"
    $status.error = $_.Exception.Message
    if ($rollbackArchive) {
        Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        & $runtime.pythonExecutable $syncScript $LiveRoot --restore-archive $rollbackArchive
        Start-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        $status.state = "rolled_back"
    } elseif ($stopped) { Start-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue }
    Write-Error $_ -ErrorAction Continue
} finally {
    $status | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $statusPath -Encoding UTF8
}
