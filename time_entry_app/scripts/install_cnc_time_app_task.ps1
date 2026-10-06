param(
    [string]$TaskName = "Benoit CNC Time Entry App"
)

$ErrorActionPreference = "Stop"
$launcher = Join-Path $PSScriptRoot "run_cnc_time_app.ps1"
$powershellCommand = Get-Command powershell.exe -ErrorAction Stop

if (-not (Test-Path -LiteralPath $launcher)) {
    throw "CNC Time Entry launcher was not found at $launcher"
}

$action = New-ScheduledTaskAction `
    -Execute $powershellCommand.Source `
    -Argument ('-NoProfile -ExecutionPolicy Bypass -File "{0}"' -f $launcher)
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" `
    -LogonType ServiceAccount `
    -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "Runs the Benoit CNC Time Entry web app after the host starts." `
    -Force `
    -ErrorAction Stop

Write-Host "Installed startup task '$TaskName'."
