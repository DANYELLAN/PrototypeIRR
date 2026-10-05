param(
    [string]$TaskName = "Benoit CNC Time Entry App",
    [int]$Port = 3100
)

$ErrorActionPreference = "Stop"
$appDirectory = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $appDirectory "data"
$statusPath = Join-Path $dataDirectory "host_task_start_status.json"
New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null

Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

$listeners = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
foreach ($listener in $listeners) {
    $process = Get-Process -Id $listener.OwningProcess -ErrorAction Stop
    if ($process.ProcessName -ne "node") {
        throw "Port $Port is owned by unexpected process $($process.ProcessName)."
    }
    Stop-Process -Id $process.Id -Force -ErrorAction Stop
}

Start-Sleep -Seconds 2
Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop

$deadline = (Get-Date).AddSeconds(30)
do {
    Start-Sleep -Seconds 1
    $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
} until ($listener -or (Get-Date) -ge $deadline)

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction Stop
$status = [pscustomobject]@{
    task_name = $task.TaskName
    state = [string]$task.State
    last_task_result = $info.LastTaskResult
    listening = [bool]$listener
    port = $Port
    checked_at = (Get-Date).ToString("o")
}
$status | ConvertTo-Json | Set-Content -LiteralPath $statusPath -Encoding UTF8

if (-not $listener) {
    throw "The scheduled task did not open port $Port within 30 seconds."
}

Write-Host "Scheduled task '$TaskName' is running on port $Port."
