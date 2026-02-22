# Diagnostic script for pipeline stagnation (pending QID / pending wiki enrichment stuck)
# Usage:
#   ./scripts/diagnose-pipeline-stagnation.ps1
#   ./scripts/diagnose-pipeline-stagnation.ps1 -UseDocker
#
# Run from project root. For Kafka lag check, use: docker compose exec kafka rpk group describe embeddings-worker

param(
    [switch]$UseDocker,
    [string]$ConnectionString = $env:DATABASE_URL
)

$ErrorActionPreference = "Continue"

function Invoke-Sql {
    param([string]$Sql)
    if ($UseDocker) {
        $Sql | docker compose exec -T db psql -U postgres -d astro_reason -t -A 2>&1
    } else {
        $pgHost = "localhost"; $pgPort = "5433"; $pgUser = "postgres"; $pgPass = "postgres"; $pgDb = "astro_reason"
        if ($ConnectionString -match "postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/([^?]+)") {
            $pgUser,$pgPass,$pgHost,$pgPort,$pgDb = $Matches[1,2,3,4,5]
        }
        $env:PGPASSWORD = $pgPass
        $Sql | psql -h $pgHost -p $pgPort -U $pgUser -d $pgDb -t -A 2>&1
        Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
    }
}

Write-Host ""
Write-Host "=== Pipeline Stagnation Diagnostics ===" -ForegroundColor Cyan
Write-Host ""

# 1. Pipeline counts
Write-Host "1. Pipeline counts (from pipeline_counts view):" -ForegroundColor Yellow
$counts = Invoke-Sql "SELECT pending_qid_resolution, pending_wiki_enrichment, people_total FROM pipeline_counts;"
if ($counts) {
    $parts = $counts.Trim() -split '\|'
    Write-Host "   pending_qid_resolution: $($parts[0])"
    Write-Host "   pending_wiki_enrichment: $($parts[1])"
    Write-Host "   people_total:           $($parts[2])"
}
Write-Host ""

# 2. Ingest jobs (gate for fetch-bio)
Write-Host "2. Ingest job gate (Resolver skips fetch-bio if any QUEUED/STARTED):" -ForegroundColor Yellow
$ingest = Invoke-Sql "SELECT status, function FROM job_status WHERE function = 'worker.ingest.parse_adb_xml' AND status IN ('QUEUED','STARTED');"
if ($ingest -and $ingest.Trim()) {
    Write-Host "   ISSUE: Active ingest job(s) found - fetch-bio is BLOCKED" -ForegroundColor Red
    Write-Host "   $ingest"
} else {
    Write-Host "   OK: No active ingest jobs (fetch-bio gate is open)" -ForegroundColor Green
}
Write-Host ""

# 3. Embeddings lag (requires Kafka check - show command)
Write-Host "3. Embeddings lag (Resolver skips fetch-bio if lag > EMBEDDINGS_LAG_THRESHOLD):" -ForegroundColor Yellow
Write-Host "   Run: docker compose exec kafka rpk group describe embeddings-worker" -ForegroundColor Cyan
Write-Host "   LAG should be 0 for fetch-bio to run. If LAG > 100 (default threshold), fetch-bio is BLOCKED." -ForegroundColor Gray
Write-Host ""

# 4. Sample pending QID - are they resolvable?
Write-Host "4. Sample of pending QID resolution (first 5 names):" -ForegroundColor Yellow
$sampleQid = Invoke-Sql @"
SELECT pr.name, b.date
FROM person_raw pr
INNER JOIN birth b ON b.person_id = pr.id
LEFT JOIN entity_link el ON el.person_id = pr.id
WHERE el.person_id IS NULL
ORDER BY pr.created_at ASC, pr.name ASC
LIMIT 5;
"@
if ($sampleQid -and $sampleQid.Trim()) {
    $sampleQid.Trim() -split "`n" | ForEach-Object { Write-Host "   $_" }
    Write-Host "   (Resolver skips names matching: accident:, disaster, victim, role :, etc.)" -ForegroundColor Gray
} else {
    Write-Host "   (none - all resolved or no birth data)" -ForegroundColor Gray
}
Write-Host ""

# 5. Sample pending wiki enrichment - do they have QIDs?
Write-Host "5. Sample of pending wiki enrichment (entity_link with no fetch_bio source):" -ForegroundColor Yellow
$sampleWiki = Invoke-Sql @"
SELECT pr.name, el.qid
FROM entity_link el
JOIN person_raw pr ON pr.id = el.person_id
WHERE NOT EXISTS (
  SELECT 1 FROM bio_text bt
  WHERE bt.person_id = el.person_id AND bt.source LIKE '%fetch_bio%'
)
LIMIT 5;
"@
if ($sampleWiki -and $sampleWiki.Trim()) {
    $sampleWiki.Trim() -split "`n" | ForEach-Object { Write-Host "   $_" }
    Write-Host "   (These have QIDs but no Wikipedia bio yet. Fetch-bio may skip if no en.wikipedia sitelink.)" -ForegroundColor Gray
} else {
    Write-Host "   (none - all enriched)" -ForegroundColor Gray
}
Write-Host ""

# 6. Resolver / fetch-bio logs
Write-Host "6. Resolver logs (check for Resolved/Fetched/Skipping):" -ForegroundColor Yellow
Write-Host "   Run: docker compose logs resolver --tail=100 | Select-String -Pattern 'Resolved|Fetched|Skipping'" -ForegroundColor Cyan
Write-Host "   - 'Resolved 0 QIDs' every cycle = QID resolution failing (Wikidata match/parse bug)" -ForegroundColor Gray
Write-Host "   - 'Skipping fetch-bio: ingest' = blocked by ingest jobs" -ForegroundColor Gray
Write-Host "   - 'Skipping fetch-bio: embeddings topic lag' = blocked by embeddings backlog" -ForegroundColor Gray
Write-Host "   - 'Fetched X Wikipedia bios' = fetch-bio ran successfully" -ForegroundColor Gray
Write-Host ""

# 7. Services running?
Write-Host "7. Service status:" -ForegroundColor Yellow
$resolverRunning = docker compose ps resolver 2>&1
if ($resolverRunning -match "Up") {
    Write-Host "   Resolver: running" -ForegroundColor Green
} else {
    Write-Host "   Resolver: NOT RUNNING - start with: docker compose up -d resolver" -ForegroundColor Red
}
$fetchBioRunning = docker compose ps fetch-bio 2>&1
if ($fetchBioRunning -match "Up") {
    Write-Host "   Fetch-bio: running (called by Resolver, no standalone polling)" -ForegroundColor Green
} else {
    Write-Host "   Fetch-bio: NOT RUNNING - Resolver cannot call it" -ForegroundColor Red
}
Write-Host ""

# Summary
Write-Host "=== Remediation Summary ===" -ForegroundColor Cyan
Write-Host "Pending QID stuck:"
Write-Host "  - If Resolved 0 every cycle: rebuild resolver (docker compose build resolver && docker compose up -d resolver)" -ForegroundColor White
Write-Host "  - Increase RESOLVE_LIMIT in .env (e.g. 200) and restart resolver" -ForegroundColor White
Write-Host ""
Write-Host "Pending Wiki stuck:"
Write-Host "  - Clear ingest gate: ensure no worker.ingest job is QUEUED/STARTED" -ForegroundColor White
Write-Host "  - Clear lag gate: ensure embeddings-worker LAG is 0 (may need to run embeddings worker)" -ForegroundColor White
Write-Host "  - Manual fetch-bio: curl -X POST http://localhost:8002/fetch-bio -H 'Content-Type: application/json' -d '{\"lang\":\"en\",\"limit\":500}'" -ForegroundColor White
Write-Host "  - Some QIDs have no en.wikipedia sitelink - they will never be wiki-enriched" -ForegroundColor Gray
Write-Host ""
