$ErrorActionPreference = "Stop"

& (Join-Path $PSScriptRoot "install_cnc_time_app_task.ps1")
& (Join-Path $PSScriptRoot "install_acumatica_task.ps1")

$firewallRuleName = "Benoit CNC Time Entry TCP 3100"
$firewallRule = Get-NetFirewallRule -DisplayName $firewallRuleName -ErrorAction SilentlyContinue
if (-not $firewallRule) {
    New-NetFirewallRule `
        -DisplayName $firewallRuleName `
        -Direction Inbound `
        -Action Allow `
        -Protocol TCP `
        -LocalPort 3100 `
        -Profile Domain,Private `
        -ErrorAction Stop | Out-Null
}
$firewallRule = Get-NetFirewallRule -DisplayName $firewallRuleName -ErrorAction Stop

$appDirectory = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $appDirectory "data"
$statusPath = Join-Path $dataDirectory "host_task_status.json"
New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null

$status = @(
    "Benoit CNC Time Entry App",
    "Benoit CNC Acumatica Labor Sync"
) | ForEach-Object {
    $task = Get-ScheduledTask -TaskName $_ -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $_ -ErrorAction Stop
    [pscustomobject]@{
        task_name = $task.TaskName
        state = [string]$task.State
        user_id = $task.Principal.UserId
        next_run_time = $info.NextRunTime
        execute = $task.Actions.Execute
        arguments = $task.Actions.Arguments
        firewall_rule = $firewallRuleName
        firewall_enabled = [string]$firewallRule.Enabled
    }
}
$status | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $statusPath -Encoding UTF8

Write-Host "Benoit CNC host tasks installed successfully."
Write-Host "Task status written to $statusPath"
