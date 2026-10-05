$ErrorActionPreference = "Stop"

$appDirectory = Split-Path -Parent $PSScriptRoot
$npmPath = "C:\Program Files\nodejs\npm.cmd"
$dataDirectory = Join-Path $appDirectory "data"
$logPath = Join-Path $dataDirectory "cnc-time-host.log"

if (-not (Test-Path -LiteralPath $npmPath)) {
    $npmPath = (Get-Command npm.cmd -ErrorAction Stop).Source
}

New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null
Set-Location -LiteralPath $appDirectory
& $npmPath start *>> $logPath
exit $LASTEXITCODE
