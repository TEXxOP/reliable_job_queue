-- this helps ensure jobs are durably stored even if Redis goes down
CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY,
  type VARCHAR(255) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}',
  priority VARCHAR(20) NOT NULL DEFAULT 'normal',
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 5,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  scheduled_for TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  locked_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  error TEXT,
  worker_id VARCHAR(255)
);

-- this helps ensure workers can quickly find pending jobs sorted by priority
CREATE INDEX IF NOT EXISTS idx_jobs_pending ON jobs (priority, scheduled_for)
  WHERE status = 'pending';

-- this helps ensure we can efficiently look up which jobs a specific worker is handling
CREATE INDEX IF NOT EXISTS idx_jobs_worker ON jobs (worker_id)
  WHERE status = 'processing';

-- this helps ensure dashboard queries for recent jobs are fast
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at DESC);
