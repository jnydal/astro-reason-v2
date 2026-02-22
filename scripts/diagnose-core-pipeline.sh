#!/usr/bin/env bash
# Core pipeline diagnostic: QID resolution, wiki enrichment, re-embedded (wiki-based) progress.
# Inspired by services in restart-core-pipeline.sh (db, kafka, grafana, embeddings, resolver, fetch-bio).
#
# Usage: ./scripts/diagnose-core-pipeline.sh
#   -UseDocker   run SQL via docker compose exec (default when no psql in path)
#
# Requires: docker compose, project root as cwd

set -e
cd "$(dirname "$0")/.."

USE_DOCKER=false
for arg in "$@"; do
  case "$arg" in
    -UseDocker) USE_DOCKER=true ;;
  esac
done

# Default to Docker if psql not found
if ! command -v psql &>/dev/null; then
  USE_DOCKER=true
fi

run_sql() {
  local sql="$1"
  if $USE_DOCKER; then
    echo "$sql" | docker compose exec -T db psql -U postgres -d astro_reason -t -A -F "|" 2>/dev/null || true
  else
    local dsn="${DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/astro_reason}"
    echo "$sql" | psql "$dsn" -t -A -F "|" 2>/dev/null || true
  fi
}

echo ""
echo "=== Core Pipeline Diagnostic ==="
echo "Services: db, kafka, grafana, embeddings, resolver, fetch-bio"
echo ""

# --- Service status ---
echo "--- Service status ---"
for svc in db kafka grafana embeddings resolver fetch-bio; do
  status=$(docker compose ps "$svc" 2>/dev/null | tail -1)
  if echo "$status" | grep -qE "Up|running"; then
    printf "  %-12s running\n" "$svc"
  else
    printf "  %-12s NOT RUNNING\n" "$svc"
  fi
done
echo ""

# --- 1. QID resolving progress ---
echo "--- 1. QID resolving ---"
QID_SQL="
SELECT
  (SELECT COUNT(*) FROM person_raw pr INNER JOIN birth b ON b.person_id = pr.id)::text AS eligible,
  (SELECT COUNT(*) FROM entity_link)::text AS resolved,
  (SELECT COUNT(*) FROM person_raw pr INNER JOIN birth b ON b.person_id = pr.id LEFT JOIN entity_link el ON el.person_id = pr.id WHERE el.person_id IS NULL)::text AS pending
"
qid_row=$(run_sql "$QID_SQL")
if [ -n "$qid_row" ]; then
  IFS='|' read -r eligible resolved pending <<< "$qid_row"
  eligible=${eligible:-0}
  resolved=${resolved:-0}
  pending=${pending:-0}
  pct="0"
  if [ "$eligible" -gt 0 ] 2>/dev/null; then
    pct=$((resolved * 100 / eligible))
  fi
  printf "  Eligible (with birth):  %s\n" "$eligible"
  printf "  Resolved (have QID):    %s\n" "$resolved"
  printf "  Pending resolution:     %s\n" "$pending"
  printf "  Progress:               %s%%\n" "$pct"
else
  echo "  (could not query - is db running?)"
fi
echo ""

# --- 2. Wiki enriched bio_texts ---
echo "--- 2. Wiki enriched bio_texts ---"
WIKI_SQL="
SELECT
  (SELECT COUNT(*) FROM entity_link)::text AS with_qid,
  (SELECT COUNT(DISTINCT el.person_id) FROM entity_link el
   WHERE EXISTS (SELECT 1 FROM bio_text bt WHERE bt.person_id = el.person_id AND bt.source LIKE '%fetch_bio%'))::text AS wiki_enriched,
  (SELECT COUNT(*) FROM entity_link el
   WHERE NOT EXISTS (SELECT 1 FROM bio_text bt WHERE bt.person_id = el.person_id AND bt.source LIKE '%fetch_bio%'))::text AS pending_wiki
"
wiki_row=$(run_sql "$WIKI_SQL")
if [ -n "$wiki_row" ]; then
  IFS='|' read -r with_qid wiki_enriched pending_wiki <<< "$wiki_row"
  with_qid=${with_qid:-0}
  wiki_enriched=${wiki_enriched:-0}
  pending_wiki=${pending_wiki:-0}
  pct="0"
  if [ "$with_qid" -gt 0 ] 2>/dev/null; then
    pct=$((wiki_enriched * 100 / with_qid))
  fi
  printf "  With QID:               %s\n" "$with_qid"
  printf "  Wiki enriched:          %s\n" "$wiki_enriched"
  printf "  Pending wiki fetch:     %s\n" "$pending_wiki"
  printf "  Progress:               %s%%\n" "$pct"
else
  echo "  (could not query)"
fi
echo ""

# --- 3. Re-embedded bio_texts (wiki-based) ---
# People whose best bio row is wiki-enriched: current (hash match) vs stale (hash mismatch)
echo "--- 3. Re-embedded bio_texts (wiki-based) ---"
MODEL="${EMBEDDINGS_MODEL:-BAAI/bge-large-en-v1.5}"
REEMBED_SQL="
WITH best_bio AS (
  SELECT DISTINCT ON (bt.person_id) bt.person_id, bt.text_hash,
    (bt.source LIKE '%fetch_bio%') AS is_wiki
  FROM bio_text bt
  WHERE bt.text IS NOT NULL AND LENGTH(TRIM(bt.text)) > 0
  ORDER BY bt.person_id,
    COALESCE(bt.retrieved_at, bt.updated_at) DESC NULLS LAST,
    COALESCE(bt.char_count, LENGTH(bt.text)) DESC NULLS LAST
),
wiki_best AS (SELECT person_id, text_hash FROM best_bio WHERE is_wiki),
with_emb AS (
  SELECT person_id, text_hash AS emb_hash FROM embeddings_384 WHERE model_name = '$MODEL'
  UNION ALL SELECT person_id, text_hash FROM embeddings_768 WHERE model_name = '$MODEL'
  UNION ALL SELECT person_id, text_hash FROM embeddings_1024 WHERE model_name = '$MODEL'
  UNION ALL SELECT person_id, text_hash FROM embeddings_1536 WHERE model_name = '$MODEL'
)
SELECT
  (SELECT COUNT(*) FROM wiki_best)::text AS wiki_with_bio,
  (SELECT COUNT(*) FROM wiki_best wb JOIN with_emb e ON e.person_id = wb.person_id)::text AS wiki_with_embeddings,
  (SELECT COUNT(*) FROM wiki_best wb JOIN with_emb e ON e.person_id = wb.person_id WHERE wb.text_hash IS NOT DISTINCT FROM e.emb_hash)::text AS current,
  (SELECT COUNT(*) FROM wiki_best wb JOIN with_emb e ON e.person_id = wb.person_id WHERE wb.text_hash IS DISTINCT FROM e.emb_hash)::text AS stale
"
reembed_row=$(run_sql "$REEMBED_SQL")
if [ -n "$reembed_row" ]; then
  IFS='|' read -r wiki_with_bio wiki_with_embeddings current stale <<< "$reembed_row"
  wiki_with_bio=${wiki_with_bio:-0}
  wiki_with_embeddings=${wiki_with_embeddings:-0}
  current=${current:-0}
  stale=${stale:-0}
  pending_reembed=$((wiki_with_bio - wiki_with_embeddings))
  printf "  Wiki-enriched (with bio): %s\n" "$wiki_with_bio"
  printf "  With embeddings:          %s\n" "$wiki_with_embeddings"
  printf "  Current (hash match):     %s\n" "$current"
  printf "  Stale (need re-embed):    %s\n" "$stale"
  printf "  Pending (no embeddings):  %s\n" "$pending_reembed"
  if [ "$stale" -gt 0 ] 2>/dev/null; then
    echo ""
    echo "  To fix stale: docker compose run --rm embeddings python -m app.reembed_stale_sync"
  fi
else
  echo "  (could not query - model: $MODEL)"
fi
echo ""

# --- Kafka lag (optional) ---
echo "--- Kafka embeddings lag ---"
if docker compose ps kafka 2>/dev/null | grep -qE "Up|running"; then
  lag=$(docker compose exec -T kafka rpk group describe embeddings-worker 2>/dev/null | grep "TOTAL-LAG" | awk '{print $2}' || echo "?")
  printf "  embeddings-worker TOTAL-LAG: %s\n" "${lag:-N/A}"
  echo "  (Resolver skips fetch-bio when lag > EMBEDDINGS_LAG_THRESHOLD, default 100)"
else
  echo "  (kafka not running)"
fi
echo ""

echo "=== End ==="
