# Force rebuild stats-worker so it picks up Kotlin changes (e.g. featureImportanceDetrended).
# Run from repo root: .\scripts\rebuild-stats-worker.ps1

Set-Location $PSScriptRoot\..

Write-Host "Stopping stats-worker..."
docker compose stop stats-worker

Write-Host "Removing stats-worker image so build is not cached..."
docker compose build --no-cache stats-worker

Write-Host "Starting stats-worker..."
docker compose up -d stats-worker

Write-Host "Waiting 5s for startup..."
Start-Sleep -Seconds 5

Write-Host "Check logs - you should see: 'Stats worker started (with birth-year detrending)'"
docker compose logs stats-worker --tail 20
