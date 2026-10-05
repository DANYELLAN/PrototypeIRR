param(
    [string]$TaskName = "Benoit CNC Acumatica Labor Sync",
    [string]$RunAt = "06:00"
)

$ErrorActionPreference = "Stop"
$appDirectory = Split-Path -Parent $PSScriptRoot
$syncScript = Join-Path $PSScriptRoot "sync_acumatica.py"
$pythonCommand = Get-Command python -ErrorAction Stop

if (-not (Test-Path -LiteralPath $syncScript)) {
    throw "Acumatica sync script was not found at $syncScript"
}

$action = New-ScheduledTaskAction `
    -Execute $pythonCommand.Source `
    -Argument ('"{0}"' -f $syncScript) `
    -WorkingDirectory $appDirectory
$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 10)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Sends approved CNC labor to the Acumatica Test tenant in on-hold batches." `
    -Force

Write-Host "Installed scheduled task '$TaskName' for $RunAt local server time."
