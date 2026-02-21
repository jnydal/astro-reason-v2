# Pipeline diagnostics - run against the astro_reason PostgreSQL database.
# Usage:
#   ./scripts/diagnose-pipeline.ps1
#   ./scripts/diagnose-pipeline.ps1 -UseDocker    # run via docker compose exec db
#
# Set DATABASE_URL or pass -ConnectionString if connecting directly (not Docker).

param(
    [switch]$UseDocker,
    [string]$ConnectionString = $env:DATABASE_URL
)

$sql = @"
SELECT 'people_total' AS metric, COUNT(*)::text AS value FROM person_raw
UNION ALL SELECT 'with_birth_date', COUNT(*)::text FROM birth
UNION ALL SELECT 'entity_link_qid', COUNT(*)::text FROM entity_link
UNION ALL SELECT 'embeddings_computed', COUNT(DISTINCT person_id)::text FROM (
  SELECT person_id FROM embeddings_384 UNION SELECT person_id FROM embeddings_768
  UNION SELECT person_id FROM embeddings_1024 UNION SELECT person_id FROM embeddings_1536
) emb
UNION ALL SELECT 'astro_features', COUNT(*)::text FROM astro_features
UNION ALL SELECT 'pending_qid_resolution', COUNT(*)::text FROM person_raw pr
  INNER JOIN birth b ON b.person_id = pr.id
  LEFT JOIN entity_link el ON el.person_id = pr.id
  WHERE el.person_id IS NULL;
"@

Write-Host "`n=== Pipeline Diagnostics ===" -ForegroundColor Cyan
Write-Host ""

if ($UseDocker) {
    $result = $sql | docker compose exec -T db psql -U postgres -d astro_reason -t -A -F "|" 2>&1
} else {
    if (-not $ConnectionString) { $ConnectionString = "postgresql://postgres:postgres@localhost:5432/astro_reason" }
    $pgHost = "localhost"; $pgPort = "5432"; $pgUser = "postgres"; $pgPass = "postgres"; $pgDb = "astro_reason"
    if ($ConnectionString -match "postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)") {
        $pgUser,$pgPass,$pgHost,$pgPort,$pgDb = $Matches[1,2,3,4,5]
    }
    $env:PGPASSWORD = $pgPass
    $result = $sql | psql -h $pgHost -p $pgPort -U $pgUser -d $pgDb -t -A -F "|" 2>&1
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

if ($LASTEXITCODE -ne 0 -and -not $UseDocker) {
    Write-Host "psql failed. Try: ./scripts/diagnose-pipeline.ps1 -UseDocker" -ForegroundColor Red
    Write-Host $result
    exit 1
}

$result -split "`n" | Where-Object { $_.Trim() } | ForEach-Object {
    $m, $v = $_ -split '\|', 2
    Write-Host ("  {0,-24} {1}" -f $m, $v)
}

Write-Host ""
Write-Host "Bottleneck: Embeddings require Resolver(QID) -> fetch_bio(Wikipedia) -> embeddings worker." -ForegroundColor Yellow
Write-Host "Resolver processes RESOLVE_LIMIT per minute (default 50). To speed up bulk ingest:" -ForegroundColor Yellow
Write-Host "  RESOLVE_LIMIT=200  (add to .env, restart resolver)" -ForegroundColor Cyan
