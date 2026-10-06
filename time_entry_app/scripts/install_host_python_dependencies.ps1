param(
    [string]$PythonPath = "C:\Python314\python.exe"
)

$ErrorActionPreference = "Stop"
$projectDirectory = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$requirementsPath = Join-Path $projectDirectory "requirements.txt"
$venvDirectory = Join-Path $projectDirectory ".venv"
$venvPython = Join-Path $venvDirectory "Scripts\python.exe"

if (-not (Test-Path -LiteralPath $PythonPath)) {
    throw "Python was not found at $PythonPath"
}
if (-not (Test-Path -LiteralPath $requirementsPath)) {
    throw "Requirements file was not found at $requirementsPath"
}

if (-not (Test-Path -LiteralPath $venvPython)) {
    & $PythonPath -m venv $venvDirectory
    if ($LASTEXITCODE -ne 0) {
        throw "Virtual environment creation failed with exit code $LASTEXITCODE."
    }
}

& $venvPython -m pip install --disable-pip-version-check -r $requirementsPath
if ($LASTEXITCODE -ne 0) {
    throw "Python dependency installation failed with exit code $LASTEXITCODE."
}

& $venvPython -c "import msal, psycopg2, requests; print('Host Python dependencies verified.')"
if ($LASTEXITCODE -ne 0) {
    throw "Python dependency verification failed with exit code $LASTEXITCODE."
}
