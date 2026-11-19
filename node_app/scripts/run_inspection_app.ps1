param(
    [Parameter(Mandatory = $true)]
    [string]$NodePath
)

$ErrorActionPreference = "Stop"
$appDirectory = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $appDirectory "data"
$logPath = Join-Path $dataDirectory "inspection-host.log"
New-Item -ItemType Directory -Path $dataDirectory -Force | Out-Null

try {
    if (-not (Test-Path -LiteralPath $NodePath -PathType Leaf)) {
        throw "Node.js was not found at $NodePath. Reinstall the startup task with a valid NodePath."
    }
    $projectDirectory = Split-Path -Parent $appDirectory
    $pythonPath = Join-Path $projectDirectory ".venv\Scripts\python.exe"
    $runtimePath = Join-Path $appDirectory "config\inspection-runtime.json"
    if (Test-Path -LiteralPath $runtimePath) {
        $runtime = Get-Content -LiteralPath $runtimePath -Raw | ConvertFrom-Json
        if ($runtime.pythonExecutable) { $pythonPath = $runtime.pythonExecutable }
    }
    if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
        throw "The Inspection app Python environment was not found at $pythonPath."
    }
    $env:PYTHON_EXECUTABLE = $pythonPath
    $env:NODE_APP_PORT = "3000"
    Set-Location -LiteralPath $appDirectory
    "$(Get-Date -Format o) Starting Inspection app with $NodePath" | Add-Content -LiteralPath $logPath -Encoding UTF8
    # Keep native stderr in the log without PowerShell treating it as a fatal error.
    $ErrorActionPreference = "Continue"
    & $NodePath (Join-Path $appDirectory "src\server.js") 2>&1 | Out-File -LiteralPath $logPath -Append -Encoding UTF8
    exit $LASTEXITCODE
} catch {
    "$(Get-Date -Format o) Startup failed: $($_.Exception.Message)" | Add-Content -LiteralPath $logPath -Encoding UTF8
    exit 1
}
