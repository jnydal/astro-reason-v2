-- Place-match confidence: 1 = fuzzy place match succeeded, 0 = checked but no match, null = not applicable
ALTER TABLE entity_link ADD COLUMN IF NOT EXISTS place_match_confidence SMALLINT;
