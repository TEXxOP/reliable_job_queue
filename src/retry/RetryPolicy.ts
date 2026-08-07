import { Job, RetryConfig } from '../queue/types';
import { config } from '../config';

export class RetryPolicy {
  private config: RetryConfig;

  constructor(retryConfig?: Partial<RetryConfig>) {
    this.config = {
      maxAttempts: retryConfig?.maxAttempts || config.retry.maxAttempts,
      baseDelayMs: retryConfig?.baseDelayMs || config.retry.baseDelayMs,
      maxDelayMs: retryConfig?.maxDelayMs || 30000,
    };
  }

  // this helps ensure we don't keep retrying jobs that will never succeed
  shouldRetry(job: Job): boolean {
    return job.attempts < job.maxAttempts;
  }

  // this helps ensure retries are spaced out with exponential backoff so we don't overwhelm services
  // formula: baseDelay * 2^attempt + random jitter
  calculateDelay(attempt: number): number {
    const exponentialDelay = this.config.baseDelayMs * Math.pow(2, attempt);

    // this helps ensure we don't wait forever on high attempt counts
    const cappedDelay = Math.min(exponentialDelay, this.config.maxDelayMs);

    // this helps ensure multiple workers don't all retry at the exact same time
    const jitter = Math.random() * this.config.baseDelayMs;

    return Math.floor(cappedDelay + jitter);
  }

  getMaxAttempts(): number {
    return this.config.maxAttempts;
  }
}
