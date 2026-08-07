-- this helps ensure permanently failed jobs are stored separately for inspection and recovery
CREATE TABLE IF NOT EXISTS dead_letter_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  original_job_id UUID NOT NULL,
  type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}',
  priority VARCHAR(20) NOT NULL DEFAULT 'normal',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  error TEXT,
  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  recovered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL
);

-- this helps ensure we can filter dead letter jobs by type for bulk recovery
CREATE INDEX IF NOT EXISTS idx_dlq_type ON dead_letter_jobs (type)
  WHERE recovered_at IS NULL;

-- this helps ensure we can track when jobs were moved to the DLQ
CREATE INDEX IF NOT EXISTS idx_dlq_failed ON dead_letter_jobs (failed_at DESC);
