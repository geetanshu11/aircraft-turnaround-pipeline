$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
function Invoke-Docker {
    & docker @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed (exit $LASTEXITCODE). Check Docker Desktop or run 'docker compose ps -a'." }
}
function Assert-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Install Docker Desktop, open it and reopen PowerShell. See README.md for prerequisites.' }
    & docker info --format '{{.OSType}}' 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker engine is not responding. Open Docker Desktop and wait.' }
    $dockerOS = & docker info --format '{{.OSType}}'
    if ($dockerOS -ne 'linux') { throw 'Switch Docker Desktop to Linux containers.' }
    Invoke-Docker compose version
}
function Assert-Environment {
    if (-not (Test-Path '.env')) { throw 'Run ./scripts/setup.ps1 first.' }
    if (Select-String -Path '.env' -Pattern 'GENERATED_BY_SETUP' -Quiet) { throw 'Incomplete .env: remove it if unused and run setup again.' }
}
