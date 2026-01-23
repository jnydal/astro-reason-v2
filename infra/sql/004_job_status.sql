CREATE TABLE IF NOT EXISTS job_status (
    id UUID PRIMARY KEY,
    function TEXT NOT NULL,
    status TEXT NOT NULL,
    args_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    kwargs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    enqueued_at BIGINT NOT NULL,
    started_at BIGINT NULL,
    ended_at BIGINT NULL,
    result TEXT NULL,
    exc_info TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS job_status_status_idx ON job_status (status);
CREATE INDEX IF NOT EXISTS job_status_enqueued_idx ON job_status (enqueued_at DESC);
