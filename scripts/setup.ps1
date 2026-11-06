param([ValidateSet('lite','full')][string]$Profile='lite')
. "$PSScriptRoot/common.ps1"
Assert-Docker
if (-not (Test-Path '.env')) {
    function Random-Bytes([int]$Count) {
        $bytes = New-Object byte[] $Count
        $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        return ,$bytes
    }
    function Random-Hex { return ([BitConverter]::ToString((Random-Bytes 32))).Replace('-','').ToLowerInvariant() }
    $settings = Get-Content '.env.example' -Raw
    foreach ($name in @('POSTGRES_ADMIN_PASSWORD','WAREHOUSE_PASSWORD','AIRFLOW_DB_PASSWORD','AIRFLOW_ADMIN_PASSWORD','AIRFLOW_JWT_SECRET')) {
        $settings = $settings.Replace("$name=GENERATED_BY_SETUP", "$name=$(Random-Hex)")
    }
    $fernet = [Convert]::ToBase64String((Random-Bytes 32)).Replace('+','-').Replace('/','_')
    $settings = $settings.Replace('AIRFLOW_FERNET_KEY=GENERATED_BY_SETUP', "AIRFLOW_FERNET_KEY=$fernet")
    [IO.File]::WriteAllText((Join-Path $RepoRoot '.env'), $settings, (New-Object Text.UTF8Encoding $false))
    Write-Host 'Created private local .env with random passwords.'
}
Assert-Environment
Invoke-Docker compose --profile full --profile tools config --quiet
if ($Profile -eq 'full') { Invoke-Docker compose --profile full build dashboard airflow-init }
else { Invoke-Docker compose build dashboard }
# Start the selected profile and wait for health checks.
if ($Profile -eq 'full') { Invoke-Docker compose --profile full up -d --wait --wait-timeout 300 }
else { Invoke-Docker compose up -d --wait --wait-timeout 180 }
Invoke-Docker compose --profile full ps -a
Write-Host 'Health checks passed. Dashboard: http://localhost:8501 (or your configured port).'
if ($Profile -eq 'full') { Write-Host 'Airflow: http://localhost:8080 . Username: admin. Password: AIRFLOW_ADMIN_PASSWORD in your .env.' }
Invoke-Docker compose --profile tools run --rm pipeline init --mode demo
Invoke-Docker compose --profile tools run --rm pipeline init --mode live
Write-Host 'Database schemas initialized. Next: ./scripts/run-demo.ps1'
