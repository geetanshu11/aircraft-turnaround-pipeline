. "$PSScriptRoot/common.ps1"
Assert-Docker
Assert-Environment
Invoke-Docker compose up -d --wait --wait-timeout 180 postgres volumes-init dashboard
Invoke-Docker compose --profile tools run --rm pipeline run --mode demo
Invoke-Docker compose --profile tools run --rm pipeline health --mode demo
Write-Host 'Demo published. Open http://localhost:8501 and choose demo.'
