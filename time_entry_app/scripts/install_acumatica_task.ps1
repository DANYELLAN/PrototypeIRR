param(
    [string]$TaskName = "Benoit CNC Acumatica Labor Sync",
    [string]$RunAt = "06:00"
)

$ErrorActionPreference = "Stop"
$appDirectory = Split-Path -Parent $PSScriptRoot
$projectDirectory = Split-Path -Parent $appDirectory
$syncScript = Join-Path $PSScriptRoot "sync_acumatica.py"
$pythonPath = Join-Path $projectDirectory ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $syncScript)) {
    throw "Acumatica sync script was not found at $syncScript"
}
if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw "Project Python was not found at $pythonPath"
}

$action = New-ScheduledTaskAction `
    -Execute $pythonPath `
    -Argument ('"{0}"' -f $syncScript) `
    -WorkingDirectory $appDirectory
$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt
$principal = New-ScheduledTaskPrincipal `
    -UserId "SYSTEM" `
    -LogonType ServiceAccount `
    -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 10)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "Sends approved CNC labor to the Acumatica Test tenant in on-hold batches." `
    -Force `
    -ErrorAction Stop

Write-Host "Installed scheduled task '$TaskName' for $RunAt local server time."
