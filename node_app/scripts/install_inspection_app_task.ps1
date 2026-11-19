#Requires -RunAsAdministrator
param(
    [string]$NodePath = "",
    [switch]$StartNow,
    [int]$ReplaceProcessId = 0
)

$ErrorActionPreference = "Stop"
$taskName = "Benoit Inspection App"
$appDirectory = Split-Path -Parent $PSScriptRoot
$launcher = Join-Path $PSScriptRoot "run_inspection_app.ps1"
$dataDirectory = Join-Path $appDirectory "data"
New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null
$statusPath = Join-Path $dataDirectory "inspection-startup-status.json"
$status = [ordered]@{ task_name = $taskName; installed = $false; healthy = $false; checked_at = $null; error = $null }

try {
    if (-not $NodePath) { $NodePath = (Get-Command node.exe -ErrorAction Stop).Source }
    $NodePath = (Resolve-Path -LiteralPath $NodePath).Path
    if (-not (Test-Path -LiteralPath $launcher -PathType Leaf)) { throw "Inspection launcher is missing." }
    if (-not (Test-Path -LiteralPath (Join-Path $appDirectory "src\server.js") -PathType Leaf)) { throw "Inspection server is missing." }
    $existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($existingTask -and -not ($existingTask.Actions | Where-Object { $_.Arguments -like "*$launcher*" })) {
        throw "A different task already uses the name '$taskName'. No task was changed."
    }
    $listeners = @(Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue)
    if ($StartNow -and $listeners.Count) {
        foreach ($listener in $listeners) {
            $process = Get-CimInstance Win32_Process -Filter "ProcessId = $($listener.OwningProcess)"
            if ($ReplaceProcessId -le 0 -or $listener.OwningProcess -ne $ReplaceProcessId -or
                $process.Name -ne "node.exe" -or $process.ExecutablePath -ne $NodePath -or
                $process.CommandLine -notmatch 'src[\\/]server\.js') {
                throw "Port 3000 is in use by an unverified process. The running app was not stopped."
            }
        }
    }
    $powershell = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
    $action = New-ScheduledTaskAction -Execute $powershell -WorkingDirectory $appDirectory `
        -Argument ('-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}" -NodePath "{1}"' -f $launcher, $NodePath)
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $trigger.Delay = "PT30S"
    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew `
        -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal `
        -Settings $settings -Description "Starts the Benoit Inspection app after Windows boots, without requiring sign-in." -Force | Out-Null
    $status.installed = $true
    Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (Join-Path $dataDirectory "inspection-startup-task.xml") -Encoding UTF8

    if ($StartNow) {
        if ($existingTask) { Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue }
        foreach ($processId in @($listeners.OwningProcess | Sort-Object -Unique)) {
            Stop-Process -Id $processId -ErrorAction Stop
        }
        Start-ScheduledTask -TaskName $taskName
        $deadline = (Get-Date).AddSeconds(60)
        do {
            Start-Sleep -Seconds 2
            try {
                $response = Invoke-WebRequest -Uri "http://127.0.0.1:3000/" -UseBasicParsing -TimeoutSec 5
                $status.healthy = $response.StatusCode -eq 200 -and $response.Content -match '<title>Inspection Run Report</title>'
            } catch { $status.healthy = $false }
        } until ($status.healthy -or (Get-Date) -ge $deadline)
        if (-not $status.healthy) { throw "The task was installed, but the sign-in page did not become healthy. Check inspection-host.log." }
    }
    $task = Get-ScheduledTask -TaskName $taskName
    $status.state = [string]$task.State
    $status.principal = $task.Principal.UserId
    $status.startup_delay = $task.Triggers[0].Delay
    $status.restart_count = $task.Settings.RestartCount
    $status.listener_pid = @(Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique)
    Write-Host "Inspection app automatic startup installed. Healthy: $($status.healthy)"
} catch {
    $status.error = $_.Exception.Message
    Write-Error $_ -ErrorAction Continue
    exit 1
} finally {
    $status.checked_at = (Get-Date).ToString("o")
    $status | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $statusPath -Encoding UTF8
}
