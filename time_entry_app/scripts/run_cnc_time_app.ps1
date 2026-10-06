param(
    [string]$NodePath = ""
)

$ErrorActionPreference = "Stop"

$appDirectory = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $appDirectory "data"
$logPath = Join-Path $dataDirectory "cnc-time-host.log"

New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null
try {
    if (-not $NodePath) {
        $NodePath = "C:\Program Files\nodejs\node.exe"
        if (-not (Test-Path -LiteralPath $NodePath)) {
            $NodePath = (Get-Command node.exe -ErrorAction Stop).Source
        }
    }
    if (-not (Test-Path -LiteralPath $NodePath -PathType Leaf)) {
        throw "Node.js was not found at $NodePath. Reinstall the startup task with a valid -NodePath."
    }
    $envPath = Join-Path (Split-Path -Parent $appDirectory) ".env"
    $serverPath = Join-Path $appDirectory "src\cncTimeServer.js"
    Set-Location -LiteralPath $appDirectory
    "$(Get-Date -Format o) Starting CNC Time Entry with $NodePath" | Add-Content -LiteralPath $logPath -Encoding UTF8
    # Windows PowerShell treats native stderr as errors even when Node keeps running.
    $ErrorActionPreference = "Continue"
    & $NodePath "--env-file=$envPath" $serverPath 2>&1 | Out-File -FilePath $logPath -Append -Encoding UTF8 -ErrorAction Stop
    exit $LASTEXITCODE
} catch {
    "$(Get-Date -Format o) Startup failed: $($_.Exception.Message)" | Add-Content -LiteralPath $logPath -Encoding UTF8
    exit 1
}
