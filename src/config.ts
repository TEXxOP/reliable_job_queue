import dotenv from 'dotenv';

dotenv.config();

// this helps ensure all config is in one place so nothing is scattered around
export const config = {
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
  },
  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'jobqueue',
    user: process.env.POSTGRES_USER || 'jobqueue',
    password: process.env.POSTGRES_PASSWORD || 'jobqueue_secret',
  },
  api: {
    port: parseInt(process.env.API_PORT || '3000', 10),
  },
  dashboard: {
    port: parseInt(process.env.DASHBOARD_PORT || '3001', 10),
  },
  worker: {
    concurrency: parseInt(process.env.WORKER_CONCURRENCY || '5', 10),
    pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || '1000', 10),
    visibilityTimeoutSec: parseInt(process.env.VISIBILITY_TIMEOUT_SEC || '30', 10),
  },
  retry: {
    maxAttempts: parseInt(process.env.MAX_RETRY_ATTEMPTS || '5', 10),
    baseDelayMs: parseInt(process.env.RETRY_BASE_DELAY_MS || '1000', 10),
  },
};
