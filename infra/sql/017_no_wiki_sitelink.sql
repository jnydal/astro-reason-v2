-- QIDs known to have no en.wikipedia (or other lang) sitelink in Wikidata.
-- Fetch-bio skips these so we don't waste rate limit re-checking every cycle.
-- For existing DBs: run manually, e.g. docker compose exec -T db psql -U postgres -d astro_reason -f - < infra/sql/017_no_wiki_sitelink.sql
-- To re-check later (e.g. once a year): DELETE FROM no_wiki_sitelink WHERE lang = 'en';

CREATE TABLE IF NOT EXISTS no_wiki_sitelink (
  qid TEXT NOT NULL,
  lang TEXT NOT NULL,
  checked_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (qid, lang)
);

CREATE INDEX IF NOT EXISTS idx_no_wiki_sitelink_lang ON no_wiki_sitelink(lang);
