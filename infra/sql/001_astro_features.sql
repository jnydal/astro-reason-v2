CREATE TABLE IF NOT EXISTS astro_features (
    person_id UUID PRIMARY KEY
        REFERENCES person_raw(id)
        ON DELETE CASCADE,

    -- Core metadata
    system TEXT NOT NULL,                 -- 'swisseph' or 'skyfield'
    jd_utc DOUBLE PRECISION NOT NULL,     -- Julian day UTC
    unknown_time BOOLEAN NOT NULL DEFAULT FALSE,

    -- Derived components
    longs JSONB NOT NULL,                 -- planetary longitudes (deg)
    houses JSONB,                         -- house cusps & placements (may be null)
    aspects JSONB NOT NULL,               -- list of aspect pairs with strengths
    elem_ratios JSONB NOT NULL,           -- normalized Fire/Earth/Air/Water weights
    modality_ratios JSONB NOT NULL,       -- normalized Cardinal/Fixed/Mutable weights
    feature_vec JSONB NOT NULL,           -- flat numeric features (sin/cos etc.)

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful indexes for analysis and querying
CREATE INDEX IF NOT EXISTS idx_astro_features_system
    ON astro_features (system);

CREATE INDEX IF NOT EXISTS idx_astro_features_elem
    ON astro_features USING GIN (elem_ratios);

CREATE INDEX IF NOT EXISTS idx_astro_features_mod
    ON astro_features USING GIN (modality_ratios);

CREATE INDEX IF NOT EXISTS idx_astro_features_vec
    ON astro_features USING GIN (feature_vec);

-- Optional denormalized numeric columns for common analytic queries
ALTER TABLE astro_features
  ADD COLUMN IF NOT EXISTS elem_fire  DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'fire')::DOUBLE PRECISION) STORED,
  ADD COLUMN IF NOT EXISTS elem_earth DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'earth')::DOUBLE PRECISION) STORED,
  ADD COLUMN IF NOT EXISTS elem_air   DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'air')::DOUBLE PRECISION) STORED,
  ADD COLUMN IF NOT EXISTS elem_water DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'water')::DOUBLE PRECISION) STORED;

CREATE INDEX IF NOT EXISTS idx_astro_features_elem_values
    ON astro_features (elem_fire, elem_earth, elem_air, elem_water);
