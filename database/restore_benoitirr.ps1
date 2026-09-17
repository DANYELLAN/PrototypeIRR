param(
    [string]$BackupDirectory,
    [switch]$ReplaceExisting
)

$ErrorActionPreference = "Stop"
$databaseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$appRoot = Split-Path -Parent $databaseDir

if (-not $BackupDirectory) {
    $latestBackup = Get-ChildItem -LiteralPath $databaseDir -Directory -Filter "benoitirr_logical_*" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if (-not $latestBackup) {
        throw "No benoitirr_logical_* backup folder was found in $databaseDir."
    }
    $BackupDirectory = $latestBackup.FullName
}

$BackupDirectory = (Resolve-Path -LiteralPath $BackupDirectory).Path
$dataDir = Join-Path $BackupDirectory "data"
$manifestPath = Join-Path $BackupDirectory "table_counts.csv"
if (-not (Test-Path -LiteralPath $dataDir -PathType Container)) {
    throw "Backup data folder not found: $dataDir"
}
if (-not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
    throw "Backup manifest not found: $manifestPath"
}

$envFile = Join-Path $appRoot ".env"
if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    $envFile = Join-Path $appRoot ".env.example"
}
if (-not (Test-Path -LiteralPath $envFile -PathType Leaf)) {
    throw "Neither .env nor .env.example was found in $appRoot."
}

$settings = @{}
Get-Content -LiteralPath $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $key, $value = $line.Split("=", 2)
        $settings[$key.Trim()] = $value.Trim().Trim('"').Trim("'")
    }
}

$pgHost = $settings["POSTGRES_HOST"]
$pgPort = $settings["POSTGRES_PORT"]
$pgDatabase = $settings["POSTGRES_DB"]
$pgUser = $settings["POSTGRES_USER"]
$env:POSTGRES_HOST = $pgHost
$env:POSTGRES_PORT = $pgPort
$env:POSTGRES_DB = $pgDatabase
$env:POSTGRES_USER = $pgUser
$env:POSTGRES_PASSWORD = $settings["POSTGRES_PASSWORD"]
$env:PGPASSWORD = $settings["POSTGRES_PASSWORD"]

$postgresBin = "C:\Program Files\PostgreSQL\18\bin"
$psql = Join-Path $postgresBin "psql.exe"
$createdb = Join-Path $postgresBin "createdb.exe"
if (-not (Test-Path -LiteralPath $psql)) {
    $psqlCommand = Get-Command psql -ErrorAction SilentlyContinue
    if (-not $psqlCommand) { throw "psql was not found. Install PostgreSQL 18 first." }
    $psql = $psqlCommand.Source
    $createdbCommand = Get-Command createdb -ErrorAction SilentlyContinue
    if (-not $createdbCommand) { throw "createdb was not found. Install PostgreSQL 18 first." }
    $createdb = $createdbCommand.Source
}

$tables = @(
    "sharepoint_sites",
    "sharepoint_lists",
    "sharepoint_items",
    "app_locations",
    "inspector_sessions",
    "manager_credentials",
    "recipe_aliases",
    "app_recipe_headers",
    "app_recipe_elements",
    "app_entry_options",
    "pipe_units",
    "inspection_attempts",
    "inspection_measurements",
    "ncr_reports"
)

function Invoke-Psql {
    param(
        [string]$Database,
        [string]$Command,
        [string]$File
    )

    $arguments = @(
        "--host=$pgHost",
        "--port=$pgPort",
        "--username=$pgUser",
        "--dbname=$Database",
        "--no-password",
        "--set=ON_ERROR_STOP=1"
    )
    if ($Command) { $arguments += "--command=$Command" }
    if ($File) { $arguments += "--file=$File" }

    & $psql @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed with exit code $LASTEXITCODE."
    }
}

try {
    $databaseExists = & $psql "--host=$pgHost" "--port=$pgPort" "--username=$pgUser" "--dbname=postgres" "--no-password" "--tuples-only" "--no-align" "--command=SELECT 1 FROM pg_database WHERE datname = '$pgDatabase' LIMIT 1;"
    if ($LASTEXITCODE -ne 0) { throw "Could not check whether the target database exists." }

    if ($databaseExists -eq "1") {
        $existingTableCount = & $psql "--host=$pgHost" "--port=$pgPort" "--username=$pgUser" "--dbname=$pgDatabase" "--no-password" "--tuples-only" "--no-align" "--command=SELECT count(*) FROM pg_stat_user_tables;"
        if ($LASTEXITCODE -ne 0) { throw "Could not inspect the target database." }
        if ([int]$existingTableCount -gt 0 -and -not $ReplaceExisting) {
            throw "The target database already contains tables. Re-run with -ReplaceExisting only when you intend to replace its application data."
        }
    } else {
        & $createdb "--host=$pgHost" "--port=$pgPort" "--username=$pgUser" "--no-password" "--template=template0" "--encoding=UTF8" $pgDatabase
        if ($LASTEXITCODE -ne 0) { throw "Could not create the target database." }
    }

    Invoke-Psql -Database $pgDatabase -File (Join-Path $appRoot "postgres_schema.sql")

    $python = Join-Path $appRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $python)) {
        $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
        if (-not $pythonCommand) { throw "Python was not found." }
        $python = $pythonCommand.Source
    }
    Push-Location $appRoot
    try {
        & $python -c "from workflow_db import initialize_workflow_schema; initialize_workflow_schema()"
        if ($LASTEXITCODE -ne 0) { throw "Workflow schema initialization failed." }
    } finally {
        Pop-Location
    }

    $truncateOrder = [array]$tables.Clone()
    [array]::Reverse($truncateOrder)
    $truncateSql = "TRUNCATE TABLE " + (($truncateOrder | ForEach-Object { "public.$_" }) -join ", ") + " RESTART IDENTITY CASCADE;"
    Invoke-Psql -Database $pgDatabase -Command $truncateSql

    foreach ($table in $tables) {
        $csvPath = Join-Path $dataDir "$table.csv"
        if (-not (Test-Path -LiteralPath $csvPath -PathType Leaf)) {
            throw "Missing table backup: $csvPath"
        }
        $psqlPath = $csvPath.Replace("\", "/").Replace("'", "''")
        Invoke-Psql -Database $pgDatabase -Command "\copy public.$table FROM '$psqlPath' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')"
    }

    $resetSequences = @"
DO `$`$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND column_default LIKE 'nextval(%'
    LOOP
        EXECUTE format(
            'SELECT setval(pg_get_serial_sequence(%L, %L), COALESCE(MAX(%I), 1), MAX(%I) IS NOT NULL) FROM %I.%I',
            item.table_schema || '.' || item.table_name,
            item.column_name,
            item.column_name,
            item.column_name,
            item.table_schema,
            item.table_name
        );
    END LOOP;
END
`$`$;
"@
    Invoke-Psql -Database $pgDatabase -Command $resetSequences

    $expectedCounts = @{}
    Import-Csv -LiteralPath $manifestPath | ForEach-Object {
        $expectedCounts[$_.table_name] = [long]$_.row_count
    }
    foreach ($table in $tables) {
        $actualCount = & $psql "--host=$pgHost" "--port=$pgPort" "--username=$pgUser" "--dbname=$pgDatabase" "--no-password" "--tuples-only" "--no-align" "--command=SELECT count(*) FROM public.$table;"
        if ($LASTEXITCODE -ne 0) { throw "Could not verify $table." }
        if ([long]$actualCount -ne $expectedCounts[$table]) {
            throw "$table verification failed: expected $($expectedCounts[$table]), found $actualCount."
        }
    }

    Write-Host "Restore complete. All $($tables.Count) table counts match the backup." -ForegroundColor Green
} finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:POSTGRES_HOST -ErrorAction SilentlyContinue
    Remove-Item Env:POSTGRES_PORT -ErrorAction SilentlyContinue
    Remove-Item Env:POSTGRES_DB -ErrorAction SilentlyContinue
    Remove-Item Env:POSTGRES_USER -ErrorAction SilentlyContinue
    Remove-Item Env:POSTGRES_PASSWORD -ErrorAction SilentlyContinue
}
