# Перевести регистрацию базы в кластере 1С на другую СУБД (rac infobase update). Имя базы, публикации и строки
# соединения клиентов не меняются. Запускать на хосте сервера 1С, rac той же версии, что и агент кластера.
# Откат — тот же вызов с прежними параметрами: они печатаются первой строкой («было: ...»).
# Секреты — JSON-файл (-SecretsFile), удаляется сразу после чтения:
#   {"clusterUser","clusterPassword","ibUser","ibPassword","dbUser","dbPassword"}  (clusterUser пустой — без администратора кластера)
#   .\register-pg.ps1 -Infobase base -DbServer pg-host -DbName base -RacPath 'C:\Program Files\1cv8\8.5.1.1529\bin\rac.exe' [-DryRun]
param(
    [Parameter(Mandatory)][string]$Infobase,
    [Parameter(Mandatory)][string]$DbServer,
    [Parameter(Mandatory)][string]$DbName,
    [Parameter(Mandatory)][string]$RacPath,
    [ValidateSet('PostgreSQL', 'MSSQLServer')][string]$Dbms = 'PostgreSQL',
    [string]$RasAddress = 'localhost:1545',
    [string]$SecretsFile = (Join-Path $PSScriptRoot 'register-pg.secrets.json'),
    [switch]$DryRun
)
$ErrorActionPreference = 'Stop'
$secrets = Get-Content $SecretsFile -Raw -Encoding UTF8 | ConvertFrom-Json
Remove-Item $SecretsFile -Force

function Invoke-Rac([string[]]$arguments) {
    $ErrorActionPreference = 'Continue'
    $output = & $RacPath $RasAddress @arguments 2>&1 | ForEach-Object { "$_" }
    if ($LASTEXITCODE -ne 0) { throw "rac $($arguments[0..1] -join ' '): $($output -join ' ')" }
    $output
}
function Get-RacValue([string[]]$lines, [string]$key) {
    ($lines | Where-Object { $_ -match "^$key\s*:" } | Select-Object -First 1) -replace "^$key\s*:\s*", '' -replace '^"|"$', ''
}

$clusterId = Get-RacValue (Invoke-Rac @('cluster', 'list')) 'cluster'
$clusterArgs = @("--cluster=$clusterId")
if ($secrets.clusterUser) { $clusterArgs += @("--cluster-user=$($secrets.clusterUser)", "--cluster-pwd=$($secrets.clusterPassword)") }
$infobaseId = $null
foreach ($block in ((Invoke-Rac (@('infobase', 'summary', 'list') + $clusterArgs)) -join "`n") -split "`n\s*`n") {
    if ($block -match "(?m)^name\s*:\s*`"?$([regex]::Escape($Infobase))`"?\s*$") { $infobaseId = Get-RacValue ($block -split "`n") 'infobase' }
}
if (-not $infobaseId) { throw "база $Infobase в кластере не найдена" }
$infobaseArgs = $clusterArgs + @("--infobase=$infobaseId", "--infobase-user=$($secrets.ibUser)", "--infobase-pwd=$($secrets.ibPassword)")

function Show-Registration([string]$label) {
    $info = Invoke-Rac (@('infobase', 'info') + $infobaseArgs)
    '{0}: -Dbms {1} -DbServer {2} -DbName {3} (db-user {4}; scheduled-jobs-deny {5}, sessions-deny {6})' -f $label,
        (Get-RacValue $info 'dbms'), (Get-RacValue $info 'db-server'), (Get-RacValue $info 'db-name'),
        (Get-RacValue $info 'db-user'), (Get-RacValue $info 'scheduled-jobs-deny'), (Get-RacValue $info 'sessions-deny')
}

Show-Registration 'было'
if ($DryRun) { "план: -Dbms $Dbms -DbServer $DbServer -DbName $DbName (db-user $($secrets.dbUser))"; return }
Invoke-Rac (@('infobase', 'update') + $infobaseArgs + @("--dbms=$Dbms", "--db-server=$DbServer", "--db-name=$DbName",
    "--db-user=$($secrets.dbUser)", "--db-pwd=$($secrets.dbPassword)")) | Out-Null
Show-Registration 'стало'
