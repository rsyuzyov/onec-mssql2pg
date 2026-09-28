# Пустая база 1С в PostgreSQL в формате новой платформы по структуре и служебным таблицам боевой базы MS SQL.
# Запускать на Windows-хосте, где есть SQL Server для заготовки, ibcmd нужной версии и bcp.
#   1) DDL таблиц и индексов dbo — с боевого SQL (gen-schema-ddl.sql рядом со скриптом);
#   2) заготовка в локальном SQL Server: пустые таблицы и индексы;
#   3) служебные таблицы с данными — bcp с боевого SQL в заготовку;
#   4) ibcmd infobase replicate заготовки в PostgreSQL (служебные таблицы — в формате новой платформы).
# Учетные данные — файл key=value (-EnvFile), в вывод не попадают:
#   LOCAL_SQL=sqlserver://sa:пароль@localhost:1433     (SQL Server заготовки)
#   SOURCE_SQL=sqlserver://sa:пароль@source-host:1433  (боевой SQL Server, только чтение)
#   PG_USER=postgres
#   PG_PASSWORD=пароль
# Весь цикл на базе ~600 ГБ — 1–2 минуты: ibcmd по пустой заготовке работает секунды.
#   .\build-struct.ps1 -SourceDb base -TargetPg base_struct -PgServer pg-host -IbcmdPath 'C:\Program Files\1cv8\8.5.1.1529\bin\ibcmd.exe'
param(
    [Parameter(Mandatory)][string]$SourceDb,
    [Parameter(Mandatory)][string]$TargetPg,
    [Parameter(Mandatory)][string]$PgServer,
    [Parameter(Mandatory)][string]$IbcmdPath,
    [string]$SchemaDb = 'onec_schema',
    [string]$EnvFile = (Join-Path $PSScriptRoot 'migration.env'),
    [string]$WorkDir = $env:TEMP,
    [string]$DataDir = '',
    [string]$LogDir = '',
    [string]$SqlToolsDir = 'C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn'
)
$ErrorActionPreference = 'Stop'
$sqlcmdExe = Join-Path $SqlToolsDir 'SQLCMD.EXE'
$bcpExe = Join-Path $SqlToolsDir 'bcp.exe'
$serviceTables = 'Config','Params','ConfigCAS','Files','IBVersion','_YearOffset','DBSchema','SchemaStorage','v8users',
    'ConfigSave','ConfigCASSave','DepotFiles','V8USERPWDPLCS','V8USERSMATKEYS'

$envValues = @{}
Get-Content $EnvFile | Where-Object { $_ -match '=' } | ForEach-Object { $pair = $_.Split('=', 2); $envValues[$pair[0].Trim()] = $pair[1].Trim() }
function Split-SqlDsn([string]$dsn) {
    $uri = [Uri]$dsn
    $credentials = $uri.UserInfo.Split(':', 2)
    @{ Server = $uri.Host; User = [Uri]::UnescapeDataString($credentials[0]); Password = [Uri]::UnescapeDataString($credentials[1]) }
}
$localSql = Split-SqlDsn $envValues['LOCAL_SQL']
$sourceSql = Split-SqlDsn $envValues['SOURCE_SQL']

$stageDir = Join-Path $WorkDir "struct-$TargetPg"
if (Test-Path $stageDir) { Remove-Item $stageDir -Recurse -Force }
New-Item -ItemType Directory $stageDir | Out-Null
$stopwatch = [Diagnostics.Stopwatch]::StartNew()
function Write-Step([string]$message) { '{0:hh\:mm\:ss} {1}' -f $stopwatch.Elapsed, $message }
function Invoke-Checked([string]$exe, [string[]]$arguments) {
    $ErrorActionPreference = 'Continue'
    $output = & $exe @arguments 2>&1 | ForEach-Object { "$_" }
    if ($LASTEXITCODE -ne 0) { $output | Select-Object -Last 20; throw "${exe}: код $LASTEXITCODE" }
    $output
}

Write-Step "DDL с $($sourceSql.Server)/$SourceDb"
$ddlFile = Join-Path $stageDir 'ddl.sql'
# -y 0 не обрезает nvarchar(max); с -W и -h он несовместим — заголовок убран именем-комментарием в самом запросе
Invoke-Checked $sqlcmdExe @('-S', $sourceSql.Server, '-U', $sourceSql.User, '-P', $sourceSql.Password, '-C', '-d', $SourceDb,
    '-i', (Join-Path $PSScriptRoot 'gen-schema-ddl.sql'), '-o', $ddlFile, '-y', '0', '-f', '65001', '-b') | Out-Null

Write-Step "заготовка $SchemaDb"
$collation = Invoke-Checked $sqlcmdExe @('-S', $sourceSql.Server, '-U', $sourceSql.User, '-P', $sourceSql.Password, '-C', '-h', '-1', '-W', '-b',
    '-Q', "SET NOCOUNT ON; SELECT collation_name FROM sys.databases WHERE name = '$SourceDb'") | Select-Object -First 1
$fileClause = if ($DataDir -and $LogDir) {
    "ON (NAME = [$SchemaDb], FILENAME = '$DataDir\$SchemaDb.mdf', SIZE = 2GB) LOG ON (NAME = [${SchemaDb}_log], FILENAME = '$LogDir\${SchemaDb}_log.ldf', SIZE = 1GB) "
} else { '' }
$createDb = "IF DB_ID('$SchemaDb') IS NOT NULL BEGIN ALTER DATABASE [$SchemaDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [$SchemaDb]; END; " +
    "CREATE DATABASE [$SchemaDb] $fileClause COLLATE $collation; ALTER DATABASE [$SchemaDb] SET RECOVERY SIMPLE;"
Invoke-Checked $sqlcmdExe @('-S', $localSql.Server, '-U', $localSql.User, '-P', $localSql.Password, '-C', '-b', '-Q', $createDb) | Out-Null
Invoke-Checked $sqlcmdExe @('-S', $localSql.Server, '-U', $localSql.User, '-P', $localSql.Password, '-C', '-b', '-d', $SchemaDb, '-i', $ddlFile, '-f', '65001') | Out-Null

Write-Step 'служебные таблицы (bcp)'
foreach ($tableName in $serviceTables) {
    $dataFile = Join-Path $stageDir "$tableName.bcp"
    Invoke-Checked $bcpExe @("SELECT * FROM [$SourceDb].dbo.[$tableName]", 'queryout', $dataFile, '-n',
        '-S', $sourceSql.Server, '-U', $sourceSql.User, '-P', $sourceSql.Password) | Out-Null
    $loaded = Invoke-Checked $bcpExe @("[$SchemaDb].dbo.[$tableName]", 'in', $dataFile, '-n', '-b', '500', '-h', 'TABLOCK',
        '-S', $localSql.Server, '-U', $localSql.User, '-P', $localSql.Password)
    '  {0,-16} {1}' -f $tableName, (($loaded | Select-String 'rows copied') -replace '\s+', ' ')
}

Write-Step "ibcmd infobase replicate $SchemaDb -> $PgServer/$TargetPg"
Invoke-Checked $IbcmdPath @('infobase', 'replicate', "--data=$(Join-Path $stageDir 'ibcmd-data')",
    '--dbms=MSSQLServer', "--database-server=$($localSql.Server)", "--database-name=$SchemaDb",
    "--database-user=$($localSql.User)", "--database-password=$($localSql.Password)",
    '--target-dbms=PostgreSQL', "--target-database-server=$PgServer", "--target-database-name=$TargetPg",
    "--target-database-user=$($envValues['PG_USER'])", "--target-database-password=$($envValues['PG_PASSWORD'])",
    '--target-create-database') | Select-Object -Last 3
Write-Step 'готово'
